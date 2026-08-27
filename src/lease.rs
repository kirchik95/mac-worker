use std::{
    fs,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    error::WorkerError,
    host_store::{
        CleanupReceipt, HostStore, HostStoreWritePoint, JobDisposition, read_json_strict,
        rename_no_replace, sync_directory,
    },
    job::{JobId, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord},
    protocol::MemoryPressure,
    rooted_fs::RootedDir,
};

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionFacts {
    pub free_disk_bytes: u64,
    pub total_disk_bytes: u64,
    pub memory_pressure: MemoryPressure,
    pub swap_used_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlotState {
    Idle,
    Busy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseSummary {
    pub job_id: JobId,
    pub project_id: String,
    pub worktree_id: String,
    pub created_at_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseOccupancy {
    pub slot_state: SlotState,
    pub active_lease: Option<LeaseSummary>,
}

pub struct LeaseService<'a> {
    store: &'a HostStore,
}

impl<'a> LeaseService<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }

    pub fn acquire(
        &self,
        request: &LeaseAcquireRequest,
        facts: &AdmissionFacts,
        now: u64,
    ) -> Result<LeaseAcquireResponse, WorkerError> {
        request.validate()?;
        if request.material().resource_class() != "heavy" {
            return Err(WorkerError::Protocol(
                "only the heavy resource class is supported".into(),
            ));
        }
        self.store.validate_layout()?;
        let _guard = self.store.admission_lock(request.material().job_id())?;
        if let Some(disposition) = self.store.disposition(request.material().job_id())? {
            return match disposition {
                JobDisposition::Accepted {
                    client_id,
                    project_id,
                    worktree_id,
                    request_fingerprint,
                    status,
                    ..
                } if client_id == request.material().client_id()
                    && project_id == request.material().project_id()
                    && worktree_id == request.material().worktree_id()
                    && request_fingerprint == *request.request_fingerprint() =>
                {
                    Ok(LeaseAcquireResponse::ExistingAccepted { status })
                }
                JobDisposition::Abandoned { .. } => Err(protocol_code(
                    "JOB_ABANDONED",
                    "job ID was permanently abandoned",
                )),
                _ => Err(protocol_code(
                    "JOB_ID_CONFLICT",
                    "job ID belongs to another immutable request",
                )),
            };
        }
        let _capacity = self.store.capacity_lock()?;

        if let Some(existing) = self.load_locked()? {
            if lease_matches_request(&existing, request)? {
                return Ok(LeaseAcquireResponse::Acquired { lease: existing });
            }
            return Err(WorkerError::Capacity {
                code: "CAPACITY_BUSY",
                message: "one heavy job is already active".into(),
            });
        }
        validate_admission(facts)?;

        let expires = now
            .checked_add(request.material().timeout_millis())
            .ok_or_else(|| WorkerError::Protocol("lease expiry overflow".into()))?;
        let lease = LeaseRecord::new(
            request.material(),
            request.request_fingerprint().clone(),
            now,
            expires,
        )?;
        self.publish_lease(&lease)?;
        Ok(LeaseAcquireResponse::Acquired { lease })
    }

    pub fn load(&self) -> Result<Option<LeaseRecord>, WorkerError> {
        self.store.validate_layout()?;
        self.load_locked()
    }

    pub fn occupancy(&self) -> Result<LeaseOccupancy, WorkerError> {
        occupancy_from_lease(self.load()?)
    }

    pub fn load_if_present(root: &Path) -> Result<LeaseOccupancy, WorkerError> {
        match fs::symlink_metadata(root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(idle()),
            Err(error) => Err(error.into()),
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                Err(WorkerError::Protocol("unsafe host data root".into()))
            }
            Ok(_) => {
                let leases = root.join("leases");
                let leases_metadata = match fs::symlink_metadata(&leases) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(idle()),
                    Err(error) => return Err(error.into()),
                    Ok(metadata) => metadata,
                };
                if leases_metadata.file_type().is_symlink() || !leases_metadata.is_dir() {
                    return Err(WorkerError::Protocol("unsafe lease namespace".into()));
                }
                let live_directory = root.join("leases/heavy");
                let live_metadata = match fs::symlink_metadata(&live_directory) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(idle()),
                    Err(error) => return Err(error.into()),
                    Ok(metadata) => metadata,
                };
                if live_metadata.file_type().is_symlink() || !live_metadata.is_dir() {
                    return Err(WorkerError::Protocol("unsafe live lease directory".into()));
                }
                occupancy_from_lease(Some(read_json_strict(&live_directory.join("lease.json"))?))
            }
        }
    }

    #[doc(hidden)]
    pub fn release_after_cleanup(
        &self,
        expected: &LeaseRecord,
        receipt: &CleanupReceipt,
    ) -> Result<(), WorkerError> {
        expected.validate()?;
        self.store.validate_layout()?;
        let _guard = self.store.admission_lock(expected.job_id())?;
        let _capacity = self.store.capacity_lock()?;
        let live = self
            .load_locked()?
            .ok_or_else(|| WorkerError::Protocol("live lease is absent".into()))?;
        if live != *expected {
            return Err(WorkerError::Protocol("live lease identity mismatch".into()));
        }
        receipt.validate_durable(&live)?;
        let live_dir = self.store.live_lease_path();
        let retired = live_dir
            .parent()
            .expect("lease path has parent")
            .join(format!(".released-{}", live.job_id()));
        if retired.exists() {
            remove_owned_directory(&retired, live_dir.parent().expect("lease path has parent"))?;
        }
        rename_no_replace(&live_dir, &retired)?;
        sync_directory(live_dir.parent().expect("lease path has parent"))?;
        remove_owned_directory(&retired, live_dir.parent().expect("lease path has parent"))?;
        sync_directory(live_dir.parent().expect("lease path has parent"))
    }

    fn load_locked(&self) -> Result<Option<LeaseRecord>, WorkerError> {
        let path = self.store.live_lease_path();
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                Err(WorkerError::Protocol("unsafe live lease directory".into()))
            }
            Ok(_) => Ok(Some(read_json_strict(&path.join("lease.json"))?)),
        }
    }

    fn publish_lease(&self, lease: &LeaseRecord) -> Result<(), WorkerError> {
        let operation = self.store.lease_operation_path(lease.job_id());
        if operation.exists() {
            remove_owned_directory(
                &operation,
                operation.parent().expect("operation has parent"),
            )?;
        }
        fs::create_dir(&operation)?;
        fs::set_permissions(&operation, fs::Permissions::from_mode(0o700))?;
        let lease_path = operation.join("lease.json");
        let bytes = serde_json::to_vec(lease).map_err(|error| {
            WorkerError::Protocol(format!("failed to serialize lease: {error}"))
        })?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lease_path)?;
        use std::io::Write;
        file.write_all(&bytes)?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseWrite)
        {
            return Err(injected());
        }
        file.sync_all()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseFileSync)
        {
            return Err(injected());
        }
        sync_directory(&operation)?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseDirectorySync)
        {
            return Err(injected());
        }
        let parent = operation.parent().expect("operation has parent");
        sync_directory(parent)?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseParentSync)
        {
            return Err(injected());
        }
        rename_no_replace(&operation, &self.store.live_lease_path())?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeasePublish)
        {
            return Err(injected());
        }
        sync_directory(parent)?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeasePublishSync)
        {
            return Err(injected());
        }
        Ok(())
    }
}

fn validate_admission(facts: &AdmissionFacts) -> Result<(), WorkerError> {
    let minimum = (facts.total_disk_bytes / 5).max(50 * GIB);
    if facts.free_disk_bytes < minimum {
        return Err(WorkerError::Capacity {
            code: "INSUFFICIENT_DISK",
            message: "worker data filesystem is below its free-space threshold".into(),
        });
    }
    if facts.memory_pressure == MemoryPressure::Critical {
        return Err(WorkerError::Capacity {
            code: "MEMORY_PRESSURE",
            message: "worker memory pressure is critical".into(),
        });
    }
    if facts.swap_used_bytes.is_some_and(|bytes| bytes > 2 * GIB) {
        return Err(WorkerError::Capacity {
            code: "SWAP_LIMIT",
            message: "worker swap usage exceeds 2 GiB".into(),
        });
    }
    Ok(())
}

fn lease_matches_request(
    lease: &LeaseRecord,
    request: &LeaseAcquireRequest,
) -> Result<bool, WorkerError> {
    let material = request.material();
    Ok(lease.job_id() == material.job_id()
        && lease.client_id() == request.material().client_id()
        && lease.lease_token() == material.lease_token()
        && lease.request_fingerprint() == request.request_fingerprint()
        && lease.worker_name() == material.worker_name()
        && lease.project_id() == material.project_id()
        && lease.worktree_id() == material.worktree_id()
        && lease.manifest_digest() == material.manifest_digest()
        && lease.timeout_millis() == material.timeout_millis()
        && lease.resource_class() == material.resource_class()
        && lease.command_summary() == &material.command().summary()?)
}

fn occupancy_from_lease(lease: Option<LeaseRecord>) -> Result<LeaseOccupancy, WorkerError> {
    match lease {
        None => Ok(idle()),
        Some(lease) => {
            lease.validate()?;
            Ok(LeaseOccupancy {
                slot_state: SlotState::Busy,
                active_lease: Some(LeaseSummary {
                    job_id: lease.job_id(),
                    project_id: lease.project_id().into(),
                    worktree_id: lease.worktree_id().into(),
                    created_at_millis: lease.created_at_millis(),
                }),
            })
        }
    }
}

fn idle() -> LeaseOccupancy {
    LeaseOccupancy {
        slot_state: SlotState::Idle,
        active_lease: None,
    }
}

fn remove_owned_directory(path: &Path, namespace: &Path) -> Result<(), WorkerError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(WorkerError::Protocol(
            "unsafe operation-owned directory".into(),
        ));
    }
    let namespace = fs::canonicalize(namespace)?;
    let path = fs::canonicalize(path)?;
    if path.parent() != Some(namespace.as_path()) {
        return Err(WorkerError::Protocol(
            "operation directory escaped its namespace".into(),
        ));
    }
    RootedDir::open(&path)?.remove_owned_tree()?;
    Ok(())
}

fn injected() -> WorkerError {
    WorkerError::Io(std::io::Error::other("injected lease crash boundary"))
}
fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}
