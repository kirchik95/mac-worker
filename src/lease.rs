use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    error::WorkerError,
    host_store::{CleanupReceipt, HostStore, HostStoreWritePoint, JobDisposition},
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
        let guard = self.store.admission_lock(request.material().job_id())?;
        guard.validate()?;
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
        let capacity = self.store.capacity_lock_after(&guard)?;
        guard.validate()?;
        capacity.validate()?;

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
        self.publish_lease(&lease, &guard, &capacity)?;
        Ok(LeaseAcquireResponse::Acquired { lease })
    }

    pub fn load(&self) -> Result<Option<LeaseRecord>, WorkerError> {
        self.store.validate_layout()?;
        self.load_locked()
    }

    pub(crate) fn load_after(
        &self,
        admission: &crate::host_store::AdmissionGuard,
        job: JobId,
    ) -> Result<Option<LeaseRecord>, WorkerError> {
        admission.validate_for(job)?;
        self.load_locked()
    }

    pub fn occupancy(&self) -> Result<LeaseOccupancy, WorkerError> {
        occupancy_from_lease(self.load()?)
    }

    pub fn load_if_present(root: &Path) -> Result<LeaseOccupancy, WorkerError> {
        let Some(store) = HostStore::open_if_present(root)? else {
            return Ok(idle());
        };
        LeaseService::new(&store).occupancy()
    }

    #[allow(dead_code)] // Task 7 lifecycle consumes this internal release boundary.
    pub(crate) fn release_after_cleanup(
        &self,
        expected: &LeaseRecord,
        receipt: &CleanupReceipt,
    ) -> Result<(), WorkerError> {
        expected.validate()?;
        self.store.validate_layout()?;
        let guard = self.store.admission_lock(expected.job_id())?;
        let capacity = self.store.capacity_lock_after(&guard)?;
        guard.validate()?;
        capacity.validate()?;
        let live = self
            .load_locked()?
            .ok_or_else(|| WorkerError::Protocol("live lease is absent".into()))?;
        if live != *expected {
            return Err(WorkerError::Protocol("live lease identity mismatch".into()));
        }
        receipt.validate_durable(self.store, &live)?;
        guard.validate()?;
        capacity.validate()?;
        let leases = self.store.open_directory("leases", false)?;
        let retired = format!(".released-{}", live.job_id());
        if leases.entry_exists(&retired)? {
            guard.validate()?;
            capacity.validate()?;
            leases.remove_owned_child(&retired)?;
        }
        let mut live_dir = leases.open_child_directory(&relative("heavy")?, false)?;
        guard.validate()?;
        capacity.validate()?;
        live_dir.publish_owned_into(&leases, &retired)?;
        leases.sync_root()?;
        leases.remove_owned_child(&retired)?;
        leases.sync_root()?;
        Ok(())
    }

    fn load_locked(&self) -> Result<Option<LeaseRecord>, WorkerError> {
        match self.store.open_directory("leases/heavy", false) {
            Ok(live) => Ok(Some(read_lease(&live)?)),
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn publish_lease(
        &self,
        lease: &LeaseRecord,
        admission: &crate::host_store::AdmissionGuard,
        capacity: &crate::host_store::AdmissionGuard,
    ) -> Result<(), WorkerError> {
        admission.validate()?;
        capacity.validate()?;
        let leases = self.store.open_directory("leases", false)?;
        let operation_name = format!(".acquire-{}", lease.job_id());
        if leases.entry_exists(&operation_name)? {
            leases.remove_owned_child(&operation_name)?;
        }
        let mut operation = leases.create_new_child_directory(&operation_name)?;
        let bytes = serde_json::to_vec(lease).map_err(|error| {
            WorkerError::Protocol(format!("failed to serialize lease: {error}"))
        })?;
        let file = operation.write_new_private_file("lease.json", &bytes)?;
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
        operation.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseDirectorySync)
        {
            return Err(injected());
        }
        leases.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseParentSync)
        {
            return Err(injected());
        }
        admission.validate()?;
        capacity.validate()?;
        operation.publish_owned_into(&leases, "heavy")?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeasePublish)
        {
            return Err(injected());
        }
        leases.sync_root()?;
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

fn injected() -> WorkerError {
    WorkerError::Io(std::io::Error::other("injected lease crash boundary"))
}

fn relative(path: &str) -> Result<crate::inputs::RelativePath, WorkerError> {
    crate::inputs::RelativePath::parse(path.as_bytes())
        .map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn read_lease(directory: &RootedDir) -> Result<LeaseRecord, WorkerError> {
    let bytes = directory.read_private_regular("lease.json", 1024 * 1024)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let lease = LeaseRecord::deserialize(&mut deserializer)
        .map_err(|error| WorkerError::Protocol(format!("invalid host JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| WorkerError::Protocol(format!("trailing host JSON data: {error}")))?;
    if serde_json::to_vec(&lease).map_err(|error| WorkerError::Protocol(error.to_string()))?
        != bytes
    {
        return Err(WorkerError::Protocol("host JSON is not canonical".into()));
    }
    lease.validate()?;
    Ok(lease)
}
fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

#[cfg(test)]
mod lifecycle_tests {
    use std::{
        collections::BTreeSet,
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };

    use super::*;
    use crate::{
        inputs::RelativePath,
        job::{ClientId, CommandSpec, JobStatus, LeaseToken, RequestFingerprintMaterial},
    };
    use tempfile::tempdir;

    fn request(seed: u128) -> LeaseAcquireRequest {
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(seed)),
            ClientId::new(uuid::Uuid::from_u128(seed + 10_000)),
            LeaseToken::new(uuid::Uuid::from_u128(seed + 20_000)),
            "mini-1".into(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        )
        .unwrap();
        LeaseAcquireRequest::new(material)
    }

    fn healthy() -> AdmissionFacts {
        AdmissionFacts {
            free_disk_bytes: 100 * GIB,
            total_disk_bytes: 250 * GIB,
            memory_pressure: MemoryPressure::Normal,
            swap_used_bytes: Some(0),
        }
    }

    fn acquire<'a>(service: &LeaseService<'a>, request: &LeaseAcquireRequest) -> LeaseRecord {
        match service.acquire(request, &healthy(), 1).unwrap() {
            LeaseAcquireResponse::Acquired { lease } => lease,
            _ => unreachable!(),
        }
    }

    fn publish_job(store: &HostStore, lease: &LeaseRecord) {
        let mut staged = store
            .begin_job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let payload = RelativePath::parse(b"payload").unwrap();
        staged
            .create_workspace_tree()
            .unwrap()
            .create_empty_directory(&payload)
            .unwrap();
        let receipt = staged
            .complete_snapshot_materialization(
                &BTreeSet::from([payload]),
                lease.project_id(),
                lease.worktree_id(),
                lease.manifest_digest(),
            )
            .unwrap();
        staged.publish_complete(receipt).unwrap();
    }

    fn private_dir(path: &Path) {
        fs::create_dir_all(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn private_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn targeted_cleanup_removes_exact_mutable_scopes_before_release() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        let acquire_request = request(1);
        let lease = acquire(&service, &acquire_request);
        assert!(store.cleanup_job_owned(&lease).is_err());
        publish_job(&store, &lease);
        store
            .record_terminal_status(&lease, &JobStatus::succeeded(100, 0, 0).unwrap())
            .unwrap();

        let incoming = store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap();
        private_dir(incoming.parent().unwrap());
        private_dir(&incoming);
        private_file(&incoming.join("payload"), b"incoming");
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        for name in ["workspace", "home", "tmp"] {
            let scope = job.join(name);
            private_dir(&scope);
            private_file(&scope.join("sentinel"), name.as_bytes());
        }
        private_file(&job.join("execution.json"), b"mutable");

        let receipt = store.cleanup_job_owned(&lease).unwrap();
        assert!(!incoming.exists());
        assert!(!job.join("workspace").exists());
        assert!(!job.join("home").exists());
        assert!(!job.join("tmp").exists());
        assert!(!job.join("execution.json").exists());

        let wrong_request = request(2);
        let wrong = LeaseRecord::new(
            wrong_request.material(),
            wrong_request.request_fingerprint().clone(),
            1,
            60_001,
        )
        .unwrap();
        assert!(service.release_after_cleanup(&wrong, &receipt).is_err());
        assert_eq!(service.load().unwrap(), Some(lease.clone()));
        service.release_after_cleanup(&lease, &receipt).unwrap();
        assert_eq!(service.load().unwrap(), None);
    }

    #[test]
    fn cleanup_preserves_substituted_scope_and_release_rechecks_absence() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let outside = temp.path().join("outside");
        private_dir(&outside);
        private_file(&outside.join("sentinel"), b"keep");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        let lease = acquire(&service, &request(1));
        publish_job(&store, &lease);
        store
            .record_terminal_status(&lease, &JobStatus::succeeded(100, 0, 0).unwrap())
            .unwrap();
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        fs::remove_dir_all(job.join("workspace")).unwrap();
        symlink(&outside, job.join("workspace")).unwrap();

        assert!(store.cleanup_job_owned(&lease).is_err());
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
        assert!(job.join("workspace").symlink_metadata().is_ok());
        assert_eq!(service.load().unwrap(), Some(lease.clone()));

        fs::remove_file(job.join("workspace")).unwrap();
        let receipt = store.cleanup_job_owned(&lease).unwrap();
        private_dir(&job.join("workspace"));
        assert!(service.release_after_cleanup(&lease, &receipt).is_err());
        assert_eq!(service.load().unwrap(), Some(lease));
    }

    #[test]
    fn exact_abandonment_cleanup_can_release_without_creating_final_job() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        let request = request(1);
        let lease = acquire(&service, &request);
        store.record_abandoned(&request, 2).unwrap();
        let receipt = store.cleanup_job_owned(&lease).unwrap();
        assert!(
            !store
                .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                .unwrap()
                .exists()
        );
        service.release_after_cleanup(&lease, &receipt).unwrap();
        assert_eq!(service.load().unwrap(), None);
    }

    #[test]
    fn cleanup_receipt_from_another_host_root_cannot_release_identical_lease() {
        let temp = tempdir().unwrap();
        let root_a = temp.path().join("host-a");
        let root_b = temp.path().join("host-b");
        let store_a = HostStore::open(&root_a).unwrap();
        let store_b = HostStore::open(&root_b).unwrap();
        let service_a = LeaseService::new(&store_a);
        let service_b = LeaseService::new(&store_b);
        let request = request(1);
        let lease_a = acquire(&service_a, &request);
        let lease_b = acquire(&service_b, &request);
        assert_eq!(lease_a, lease_b);
        store_b.record_abandoned(&request, 2).unwrap();
        let receipt_b = store_b.cleanup_job_owned(&lease_b).unwrap();
        let incoming_a = store_a
            .incoming_job(lease_a.job_id(), lease_a.lease_token())
            .unwrap();
        private_dir(incoming_a.parent().unwrap());
        private_dir(&incoming_a);
        private_file(&incoming_a.join("sentinel"), b"must remain");

        assert!(
            service_a
                .release_after_cleanup(&lease_a, &receipt_b)
                .is_err()
        );
        assert_eq!(service_a.load().unwrap(), Some(lease_a));
        assert_eq!(
            fs::read(incoming_a.join("sentinel")).unwrap(),
            b"must remain"
        );
    }
}
