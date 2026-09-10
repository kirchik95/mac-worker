use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    error::WorkerError,
    host_store::{CleanupReceipt, HostStore, HostStoreWritePoint, JobDisposition},
    job::{ExecutionScope, JobId, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord},
    protocol::MemoryPressure,
    rooted_fs::RootedDir,
    task::TaskId,
};

const GIB: u64 = 1024 * 1024 * 1024;
/// Host and laptop slot counts are this `u8` inclusive maximum.
pub const MAX_HOST_SLOTS: u8 = 8;
const CAPACITY_FILE: &str = "capacity.json";
const SLOTS_DIR: &str = "slots";
const SCOPE_FILE: &str = "scope.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OccupiedSlot {
    pub slot_id: u8,
    pub lease: LeaseRecord,
    pub execution_scope: ExecutionScope,
}

impl OccupiedSlot {
    pub fn owns_task(&self, project_id: &str, task_id: TaskId) -> bool {
        self.lease.project_id() == project_id
            && matches!(
                &self.execution_scope,
                ExecutionScope::Task { task_id: bound } if *bound == task_id
            )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HostSlotCapacity {
    slot_count: u8,
}

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
    #[serde(default)]
    pub configured_slots: u8,
    #[serde(default)]
    pub busy_slots: u8,
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
            return match &disposition {
                JobDisposition::Accepted {
                    client_id,
                    project_id,
                    worktree_id,
                    request_fingerprint,
                    status,
                    ..
                } if *client_id == request.material().client_id()
                    && project_id == request.material().project_id()
                    && worktree_id == request.material().worktree_id()
                    && request_fingerprint == request.request_fingerprint() =>
                {
                    Ok(LeaseAcquireResponse::ExistingAccepted {
                        status: status.clone(),
                    })
                }
                JobDisposition::Abandoned { .. }
                    if disposition.is_exact_abandonment_for(
                        request.material(),
                        request.request_fingerprint(),
                    ) =>
                {
                    Err(protocol_code(
                        "JOB_ABANDONED",
                        "job ID was permanently abandoned",
                    ))
                }
                _ => Err(protocol_code(
                    "JOB_ID_CONFLICT",
                    "job ID belongs to another immutable request",
                )),
            };
        }
        let capacity = self.store.capacity_lock_after(&guard)?;
        guard.validate()?;
        capacity.validate()?;

        let occupied = self.load_all_locked()?;
        if let Some(existing) = occupied
            .iter()
            .find(|slot| slot.lease.job_id() == request.material().job_id())
        {
            if lease_matches_request(&existing.lease, request)? {
                if existing.execution_scope != *request.execution_scope() {
                    return Err(protocol_code(
                        "EXECUTION_SCOPE_CONFLICT",
                        "retry execution scope does not match the live lease binding",
                    ));
                }
                return Ok(LeaseAcquireResponse::Acquired {
                    lease: existing.lease.clone(),
                });
            }
            return Err(protocol_code(
                "JOB_ID_CONFLICT",
                "job ID belongs to another immutable request",
            ));
        }
        let incoming_scope = request.execution_scope();
        refuse_terminal_task_scope(self.store, request.material().project_id(), incoming_scope)?;
        if occupied.iter().any(|slot| {
            scopes_conflict(
                request.material().project_id(),
                incoming_scope,
                slot.lease.project_id(),
                &slot.execution_scope,
            )
        }) {
            return Err(WorkerError::capacity(
                "WORKSPACE_BUSY",
                "another live lease already owns this execution workspace",
            ));
        }
        let slot_count = self.slot_count_locked()?;
        let Some(slot_id) =
            (0..slot_count).find(|id| occupied.iter().all(|slot| slot.slot_id != *id))
        else {
            return Err(WorkerError::capacity(
                "CAPACITY_BUSY",
                "all host execution slots are busy",
            ));
        };
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
        self.publish_lease(slot_id, &lease, incoming_scope, &guard, &capacity)?;
        Ok(LeaseAcquireResponse::Acquired { lease })
    }

    pub fn load(&self) -> Result<Option<LeaseRecord>, WorkerError> {
        self.store.validate_layout()?;
        let occupied = self.load_all_locked()?;
        match occupied.as_slice() {
            [] => Ok(None),
            [slot] => Ok(Some(slot.lease.clone())),
            _ => Err(protocol_code(
                "MULTIPLE_LIVE_LEASES",
                "host has more than one live lease; use load_for_job",
            )),
        }
    }

    pub fn load_all(&self) -> Result<Vec<(u8, LeaseRecord)>, WorkerError> {
        self.store.validate_layout()?;
        Ok(self
            .load_all_locked()?
            .into_iter()
            .map(|slot| (slot.slot_id, slot.lease))
            .collect())
    }

    pub fn load_for_job(&self, job: JobId) -> Result<Option<LeaseRecord>, WorkerError> {
        Ok(self.occupied_slot_for_job(job)?.map(|slot| slot.lease))
    }

    pub fn occupied_slot_for_job(&self, job: JobId) -> Result<Option<OccupiedSlot>, WorkerError> {
        self.store.validate_layout()?;
        Ok(self
            .load_all_locked()?
            .into_iter()
            .find(|slot| slot.lease.job_id() == job))
    }

    pub fn occupied_slots(&self) -> Result<Vec<OccupiedSlot>, WorkerError> {
        self.store.validate_layout()?;
        self.load_all_locked()
    }

    pub fn task_scope_is_live(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<bool, WorkerError> {
        Ok(self
            .occupied_slots()?
            .iter()
            .any(|slot| slot.owns_task(project_id, task_id)))
    }

    pub fn slot_count(&self) -> Result<u8, WorkerError> {
        self.store.validate_layout()?;
        self.slot_count_locked()
    }

    pub fn set_slot_count(&self, slot_count: u8) -> Result<(), WorkerError> {
        validate_slot_count(slot_count)?;
        self.store.validate_layout()?;
        let capacity = self.store.capacity_lock()?;
        capacity.validate()?;
        let occupied = self.load_all_locked()?;
        if occupied.iter().any(|slot| slot.slot_id >= slot_count) {
            return Err(WorkerError::capacity(
                "CAPACITY_BUSY",
                "a live lease occupies a slot that shrink would remove",
            ));
        }
        self.write_slot_count(slot_count)?;
        Ok(())
    }

    pub fn promotion_blocked(&self) -> Result<(), WorkerError> {
        self.store.validate_layout()?;
        if !self.load_all_locked()?.is_empty() {
            return Err(upgrade_drain_required("a live lease is still present"));
        }
        Ok(())
    }

    pub(crate) fn load_after(
        &self,
        admission: &crate::host_store::AdmissionGuard,
        job: JobId,
    ) -> Result<Option<LeaseRecord>, WorkerError> {
        admission.validate_for(job)?;
        self.load_for_job(job)
    }

    pub fn occupancy(&self) -> Result<LeaseOccupancy, WorkerError> {
        let occupied = self.load_all()?;
        occupancy_from_leases(self.slot_count()?, &occupied)
    }

    pub fn load_if_present(root: &Path) -> Result<LeaseOccupancy, WorkerError> {
        let Some(store) = HostStore::open_if_present(root)? else {
            return Ok(idle());
        };
        LeaseService::new(&store).occupancy()
    }

    #[doc(hidden)]
    pub fn release_after_cleanup(
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
        let occupied = self.load_all_locked()?;
        let live = match occupied
            .iter()
            .find(|slot| slot.lease.job_id() == expected.job_id())
        {
            None => {
                receipt.validate_durable(self.store, expected)?;
                return Ok(());
            }
            Some(slot) if slot.lease != *expected => {
                return Err(WorkerError::Protocol("live lease identity mismatch".into()));
            }
            Some(slot) => (slot.slot_id, slot.lease.clone()),
        };
        let leases = self.store.open_directory("leases", false)?;
        let slots = leases.open_child_directory(&relative(SLOTS_DIR)?, false)?;
        let retired = format!(".released-{}", live.1.job_id());
        guard.validate()?;
        capacity.validate()?;
        self.store.remove_owned_child_committed(&slots, &retired)?;
        receipt.validate_durable(self.store, &live.1)?;
        guard.validate()?;
        capacity.validate()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::BeforeJobLeaseRetirement)
        {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected lease retirement failure",
            )));
        }
        let mut live_dir = slots.open_child_directory(&relative(&live.0.to_string())?, false)?;
        guard.validate()?;
        capacity.validate()?;
        live_dir.publish_owned_into(&slots, &retired)?;
        slots.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterJobLeaseRetirement)
        {
            return Ok(());
        }
        slots.remove_owned_child(&retired)?;
        slots.sync_root()?;
        Ok(())
    }

    fn slot_count_locked(&self) -> Result<u8, WorkerError> {
        let leases = self.store.open_directory("leases", false)?;
        read_slot_count(&leases)
    }

    fn write_slot_count(&self, slot_count: u8) -> Result<(), WorkerError> {
        validate_slot_count(slot_count)?;
        let leases = self.store.open_directory("leases", false)?;
        write_slot_count_at(&leases, slot_count)
    }

    fn load_all_locked(&self) -> Result<Vec<OccupiedSlot>, WorkerError> {
        load_all_from_store(self.store)
    }

    fn publish_lease(
        &self,
        slot_id: u8,
        lease: &LeaseRecord,
        execution_scope: &ExecutionScope,
        admission: &crate::host_store::AdmissionGuard,
        capacity: &crate::host_store::AdmissionGuard,
    ) -> Result<(), WorkerError> {
        admission.validate()?;
        capacity.validate()?;
        let leases = self.store.open_directory("leases", false)?;
        let slots = leases.open_child_directory(&relative(SLOTS_DIR)?, false)?;
        let operation_name = format!(".acquire-{}", lease.job_id());
        self.store
            .remove_owned_child_committed(&slots, &operation_name)?;
        let mut operation = slots.create_new_child_directory(&operation_name)?;
        let bytes = serde_json::to_vec(lease).map_err(|error| {
            WorkerError::Protocol(format!("failed to serialize lease: {error}"))
        })?;
        let file = operation.write_new_private_file("lease.json", &bytes)?;
        let scope_bytes = serde_json::to_vec(execution_scope).map_err(|error| {
            WorkerError::Protocol(format!("failed to serialize execution scope: {error}"))
        })?;
        let scope_file = operation.write_new_private_file(SCOPE_FILE, &scope_bytes)?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseWrite)
        {
            return Err(injected());
        }
        file.sync_all()?;
        scope_file.sync_all()?;
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
        slots.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeaseParentSync)
        {
            return Err(injected());
        }
        admission.validate()?;
        capacity.validate()?;
        operation.publish_owned_into(&slots, &slot_id.to_string())?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterLeasePublish)
        {
            return Err(injected());
        }
        slots.sync_root()?;
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
        return Err(WorkerError::capacity(
            "INSUFFICIENT_DISK",
            "worker data filesystem is below its free-space threshold",
        ));
    }
    if facts.memory_pressure == MemoryPressure::Critical {
        return Err(WorkerError::capacity(
            "MEMORY_PRESSURE",
            "worker memory pressure is critical",
        ));
    }
    if facts.swap_used_bytes.is_some_and(|bytes| bytes > 2 * GIB) {
        return Err(WorkerError::capacity(
            "SWAP_LIMIT",
            "worker swap usage exceeds 2 GiB",
        ));
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

fn refuse_terminal_task_scope(
    store: &HostStore,
    project_id: &str,
    scope: &ExecutionScope,
) -> Result<(), WorkerError> {
    let ExecutionScope::Task { task_id } = scope else {
        return Ok(());
    };
    match store.task_status(project_id, *task_id) {
        Ok(status) if status.state().is_terminal() => Err(protocol_code(
            "TASK_CLOSED",
            "closed task cannot acquire an execution slot",
        )),
        Ok(_) => Ok(()),
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn scopes_conflict(
    left_project: &str,
    left: &ExecutionScope,
    right_project: &str,
    right: &ExecutionScope,
) -> bool {
    match (left, right) {
        (
            ExecutionScope::Task { task_id: left_task },
            ExecutionScope::Task {
                task_id: right_task,
            },
        ) => left_project == right_project && left_task == right_task,
        _ => false,
    }
}

fn occupancy_from_leases(
    configured: u8,
    occupied: &[(u8, LeaseRecord)],
) -> Result<LeaseOccupancy, WorkerError> {
    let busy = u8::try_from(occupied.len())
        .map_err(|_| protocol_code("CAPACITY_BUSY", "live lease count exceeds the slot bound"))?;
    let active_lease = occupied.first().map(|(_, lease)| LeaseSummary {
        job_id: lease.job_id(),
        project_id: lease.project_id().into(),
        worktree_id: lease.worktree_id().into(),
        created_at_millis: lease.created_at_millis(),
    });
    Ok(LeaseOccupancy {
        slot_state: if busy < configured {
            SlotState::Idle
        } else {
            SlotState::Busy
        },
        active_lease,
        configured_slots: configured,
        busy_slots: busy,
    })
}

fn idle() -> LeaseOccupancy {
    LeaseOccupancy {
        slot_state: SlotState::Idle,
        active_lease: None,
        configured_slots: 1,
        busy_slots: 0,
    }
}

fn validate_slot_count(slot_count: u8) -> Result<(), WorkerError> {
    if slot_count == 0 || slot_count > MAX_HOST_SLOTS {
        return Err(WorkerError::Protocol(format!(
            "slot_count must be between 1 and {MAX_HOST_SLOTS}"
        )));
    }
    Ok(())
}

fn upgrade_drain_required(message: &str) -> WorkerError {
    WorkerError::Unavailable(format!("HOST_UPGRADE_DRAIN_REQUIRED: {message}"))
}

fn read_slot_count(leases: &RootedDir) -> Result<u8, WorkerError> {
    if !leases.entry_exists(CAPACITY_FILE)? {
        return Err(protocol_code(
            "HOST_SLOT_CAPACITY_INVALID",
            "leases/capacity.json is missing",
        ));
    }
    let bytes = leases.read_private_regular(CAPACITY_FILE, 4096)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let record = HostSlotCapacity::deserialize(&mut deserializer).map_err(|error| {
        protocol_code(
            "HOST_SLOT_CAPACITY_INVALID",
            &format!("leases/capacity.json is invalid: {error}"),
        )
    })?;
    deserializer.end().map_err(|error| {
        protocol_code(
            "HOST_SLOT_CAPACITY_INVALID",
            &format!("leases/capacity.json has trailing data: {error}"),
        )
    })?;
    validate_slot_count(record.slot_count).map_err(|_| {
        protocol_code(
            "HOST_SLOT_CAPACITY_INVALID",
            "leases/capacity.json slot_count is out of range",
        )
    })?;
    Ok(record.slot_count)
}

fn write_slot_count_at(leases: &RootedDir, slot_count: u8) -> Result<(), WorkerError> {
    validate_slot_count(slot_count)?;
    let bytes = serde_json::to_vec(&HostSlotCapacity { slot_count }).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize slot capacity: {error}"))
    })?;
    if leases.entry_exists(CAPACITY_FILE)? {
        let expected = leases.read_private_regular(CAPACITY_FILE, 4096)?;
        leases.rewrite_private_regular_exact(CAPACITY_FILE, &expected, &bytes)?;
    } else {
        let file = leases.write_new_private_file(CAPACITY_FILE, &bytes)?;
        file.sync_all()?;
        leases.sync_root()?;
    }
    Ok(())
}

pub(crate) fn initialize_slot_layout(leases: &RootedDir) -> Result<(), WorkerError> {
    if !leases.entry_exists(SLOTS_DIR)? {
        let created = leases.create_new_child_directory(SLOTS_DIR)?;
        created.sync_root()?;
        leases.sync_root()?;
    }
    if !leases.entry_exists(CAPACITY_FILE)? {
        write_slot_count_at(leases, 1)?;
    }
    Ok(())
}

/// Layout-2 → layout-3 slot directories. FLOW's `complete_protocol_upgrade`
/// must invoke this **after** drain inspect and binary rename, while still
/// holding the construction installation lock. Do not call
/// [`HostStore::migrate_layout`] from that path (second flock fd).
pub fn promote_slot_directories(leases: &RootedDir) -> Result<(), WorkerError> {
    initialize_slot_layout(leases)
}

fn load_all_from_store(store: &HostStore) -> Result<Vec<OccupiedSlot>, WorkerError> {
    let leases = store.open_directory("leases", false)?;
    let slot_count = read_slot_count(&leases)?;
    let slots = match leases.open_child_directory(&relative(SLOTS_DIR)?, false) {
        Ok(slots) => slots,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(WorkerError::Io(error)),
    };
    let mut occupied = Vec::new();
    for raw in slots.list_names()? {
        let name = std::str::from_utf8(&raw)
            .map_err(|_| WorkerError::Protocol("slot directory name is not UTF-8".into()))?;
        if name.starts_with('.') {
            continue;
        }
        let slot_id = parse_canonical_slot_id(name)?;
        let directory = slots.open_child_directory(&relative(name)?, false)?;
        if !directory.entry_exists("lease.json")? {
            if slot_directory_is_idle(&directory)? {
                continue;
            }
            return Err(WorkerError::Protocol(
                "slot directory is missing a canonical lease record".into(),
            ));
        }
        if slot_id >= slot_count {
            return Err(protocol_code(
                "HOST_SLOT_ID_INVALID",
                "a live lease occupies a slot outside stored capacity",
            ));
        }
        occupied.push(OccupiedSlot {
            slot_id,
            lease: read_lease(&directory)?,
            execution_scope: read_scope(&directory)?,
        });
    }
    occupied.sort_by_key(|slot| slot.slot_id);
    Ok(occupied)
}

fn parse_canonical_slot_id(name: &str) -> Result<u8, WorkerError> {
    let slot_id: u8 = name.parse().map_err(|_| {
        protocol_code(
            "HOST_SLOT_ID_INVALID",
            "slot directory name is not a slot id",
        )
    })?;
    if name != slot_id.to_string() {
        return Err(protocol_code(
            "HOST_SLOT_ID_INVALID",
            "slot directory name is not canonical",
        ));
    }
    if slot_id >= MAX_HOST_SLOTS {
        return Err(protocol_code(
            "HOST_SLOT_ID_INVALID",
            "slot directory name exceeds the host slot bound",
        ));
    }
    Ok(slot_id)
}

fn injected() -> WorkerError {
    WorkerError::Io(std::io::Error::other("injected lease crash boundary"))
}

fn relative(path: &str) -> Result<crate::inputs::RelativePath, WorkerError> {
    crate::inputs::RelativePath::parse(path.as_bytes())
        .map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn slot_directory_is_idle(directory: &RootedDir) -> Result<bool, WorkerError> {
    for raw in directory.list_names()? {
        let name = std::str::from_utf8(&raw)
            .map_err(|_| WorkerError::Protocol("slot entry name is not UTF-8".into()))?;
        if name != ".mac-worker-rooted-fs" {
            return Ok(false);
        }
    }
    Ok(true)
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

fn read_scope(directory: &RootedDir) -> Result<ExecutionScope, WorkerError> {
    let bytes = directory.read_private_regular(SCOPE_FILE, 1024 * 1024)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let scope = ExecutionScope::deserialize(&mut deserializer)
        .map_err(|error| WorkerError::Protocol(format!("invalid host JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| WorkerError::Protocol(format!("trailing host JSON data: {error}")))?;
    if serde_json::to_vec(&scope).map_err(|error| WorkerError::Protocol(error.to_string()))?
        != bytes
    {
        return Err(WorkerError::Protocol("host JSON is not canonical".into()));
    }
    Ok(scope)
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
        path::Path,
    };

    use super::*;
    use crate::{
        inputs::RelativePath,
        job::{ClientId, CommandSpec, JobMeta, JobStatus, LeaseToken, RequestFingerprintMaterial},
    };
    use tempfile::tempdir;

    fn request(seed: u128) -> LeaseAcquireRequest {
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(seed)),
            ClientId::new(uuid::Uuid::from_u128(seed + 10_000)),
            LeaseToken::new(uuid::Uuid::from_u128(seed + 20_000)),
            2,
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

    fn publish_job(store: &HostStore, lease: &LeaseRecord, request: &LeaseAcquireRequest) {
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
        let meta = JobMeta::new(request.material(), request.request_fingerprint().clone()).unwrap();
        let meta_file = staged
            .rooted_dir()
            .write_new_private_file("meta.json", &serde_json::to_vec(&meta).unwrap())
            .unwrap();
        meta_file.sync_all().unwrap();
        for name in ["stdout.log", "stderr.log"] {
            staged
                .rooted_dir()
                .write_new_private_file(name, &[])
                .unwrap()
                .sync_all()
                .unwrap();
        }
        staged.rooted_dir().sync_root().unwrap();
        staged.publish_complete(receipt).unwrap();
        store
            .record_accepted(request, &JobStatus::accepted(2).unwrap(), 2)
            .unwrap();
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
        publish_job(&store, &lease, &acquire_request);
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

        assert!(store.cleanup_job_owned(&lease).is_err());
        assert_eq!(service.load().unwrap(), Some(lease.clone()));
        fs::remove_file(job.join("execution.json")).unwrap();
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
        // A later cancel, status, or supervisor finish can retry release after
        // the winner already retired the slot. That must stay a no-op: the
        // live lease is gone, but the exact cleanup receipt still proves this
        // identity owned the cleanup. Treating absence as failure is what
        // stamped LEASE_RELEASE_FAILED on already-idle workers.
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
        let acquire_request = request(1);
        let lease = acquire(&service, &acquire_request);
        publish_job(&store, &lease, &acquire_request);
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
    fn terminal_cleanup_rejects_conflicting_metadata_before_mutating_workspace() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let service = LeaseService::new(&store);
        let acquire_request = request(1);
        let lease = acquire(&service, &acquire_request);
        publish_job(&store, &lease, &acquire_request);
        store
            .record_terminal_status(&lease, &JobStatus::succeeded(100, 0, 0).unwrap())
            .unwrap();
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let conflicting_request = request(2);
        let conflicting_meta = JobMeta::new(
            conflicting_request.material(),
            conflicting_request.request_fingerprint().clone(),
        )
        .unwrap();
        fs::write(
            job.join("meta.json"),
            serde_json::to_vec(&conflicting_meta).unwrap(),
        )
        .unwrap();

        assert!(store.cleanup_job_owned(&lease).is_err());
        assert!(job.join("workspace").is_dir());
        assert_eq!(service.load().unwrap(), Some(lease));
    }

    #[test]
    fn terminal_cleanup_rejects_log_lengths_that_do_not_match_status() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let service = LeaseService::new(&store);
        let acquire_request = request(1);
        let lease = acquire(&service, &acquire_request);
        publish_job(&store, &lease, &acquire_request);
        store
            .record_terminal_status(&lease, &JobStatus::succeeded(100, 9, 8).unwrap())
            .unwrap();
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();

        assert!(store.cleanup_job_owned(&lease).is_err());
        assert!(job.join("workspace").is_dir());
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

    fn task_request(seed: u128, task: u128) -> LeaseAcquireRequest {
        request(seed).with_execution_scope(ExecutionScope::task(TaskId::new(
            uuid::Uuid::from_u128(task),
        )))
    }

    fn assert_capacity(code: &str, result: Result<LeaseAcquireResponse, WorkerError>) {
        match result {
            Err(error) => assert_eq!(error.public_code(), code, "{error}"),
            Ok(value) => panic!("expected {code}, got {value:?}"),
        }
    }

    #[test]
    fn two_task_scopes_overlap_on_two_slots_and_third_is_capacity_busy() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let service = LeaseService::new(&store);
        service.set_slot_count(2).unwrap();
        let first = acquire(&service, &task_request(1, 1));
        let second = acquire(&service, &task_request(2, 2));
        assert_ne!(first.job_id(), second.job_id());
        assert_eq!(service.occupied_slots().unwrap().len(), 2);
        assert_capacity(
            "CAPACITY_BUSY",
            service.acquire(&task_request(3, 3), &healthy(), 1),
        );
        assert_capacity(
            "WORKSPACE_BUSY",
            service.acquire(&task_request(4, 1), &healthy(), 1),
        );
    }

    #[test]
    fn two_job_scopes_from_the_same_origin_overlap() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let service = LeaseService::new(&store);
        service.set_slot_count(2).unwrap();
        let first = acquire(&service, &request(1));
        let second = acquire(&service, &request(2));
        assert_eq!(first.project_id(), second.project_id());
        assert_eq!(first.worktree_id(), second.worktree_id());
        assert_eq!(service.occupied_slots().unwrap().len(), 2);
    }

    #[test]
    fn shrink_refuses_while_a_high_index_slot_is_live() {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let service = LeaseService::new(&store);
        service.set_slot_count(2).unwrap();
        let first_req = request(1);
        let second_req = request(2);
        let first = acquire(&service, &first_req);
        acquire(&service, &second_req);
        store.record_abandoned(&first_req, 2).unwrap();
        let receipt = store.cleanup_job_owned(&first).unwrap();
        service.release_after_cleanup(&first, &receipt).unwrap();
        let occupied = service.occupied_slots().unwrap();
        assert_eq!(occupied.len(), 1);
        assert_eq!(occupied[0].slot_id, 1);
        let error = service.set_slot_count(1).unwrap_err();
        assert_eq!(error.public_code(), "CAPACITY_BUSY");
        service.set_slot_count(2).unwrap();
        store.record_abandoned(&second_req, 3).unwrap();
        let receipt = store.cleanup_job_owned(&occupied[0].lease).unwrap();
        service
            .release_after_cleanup(&occupied[0].lease, &receipt)
            .unwrap();
        service.set_slot_count(1).unwrap();
        assert_eq!(service.slot_count().unwrap(), 1);
    }

    fn occupy_only_slot_one<'a>(
        service: &LeaseService<'a>,
        store: &HostStore,
    ) -> (LeaseAcquireRequest, OccupiedSlot) {
        service.set_slot_count(2).unwrap();
        let first_req = request(1);
        let second_req = request(2);
        let first = acquire(service, &first_req);
        acquire(service, &second_req);
        store.record_abandoned(&first_req, 2).unwrap();
        let receipt = store.cleanup_job_owned(&first).unwrap();
        service.release_after_cleanup(&first, &receipt).unwrap();
        let occupied = service.occupied_slots().unwrap();
        assert_eq!(occupied.len(), 1);
        assert_eq!(occupied[0].slot_id, 1);
        (second_req, occupied.into_iter().next().unwrap())
    }

    fn assert_no_slot_zero_lease(root: &Path) {
        assert!(
            !root.join("leases/slots/0/lease.json").exists(),
            "missing/invalid capacity must not publish a new slot 0 lease"
        );
        assert!(root.join("leases/slots/1/lease.json").is_file());
    }

    #[test]
    fn new_host_defaults_to_one_slot() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        assert_eq!(service.slot_count().unwrap(), 1);
        assert_eq!(
            fs::read(root.join("leases/capacity.json")).unwrap(),
            serde_json::to_vec(&HostSlotCapacity { slot_count: 1 }).unwrap()
        );
        acquire(&service, &request(1));
        assert_eq!(service.occupied_slots().unwrap()[0].slot_id, 0);
    }

    #[test]
    fn missing_capacity_on_installed_layout3_refuses_acquire_and_probe() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        occupy_only_slot_one(&service, &store);
        fs::remove_file(root.join("leases/capacity.json")).unwrap();

        let error = service.occupancy().unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_CAPACITY_INVALID");
        let error = service.slot_count().unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_CAPACITY_INVALID");
        let error = service.acquire(&request(3), &healthy(), 1).unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_CAPACITY_INVALID");
        assert_no_slot_zero_lease(&root);
    }

    #[test]
    fn corrupt_capacity_on_installed_layout3_refuses_acquire_and_probe() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        occupy_only_slot_one(&service, &store);
        private_file(root.join("leases/capacity.json").as_path(), b"{not-json");

        let error = service.occupancy().unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_CAPACITY_INVALID");
        let error = service.acquire(&request(3), &healthy(), 1).unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_CAPACITY_INVALID");
        assert_no_slot_zero_lease(&root);
    }

    #[test]
    fn live_slot_outside_stored_capacity_is_rejected() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        occupy_only_slot_one(&service, &store);
        private_file(
            root.join("leases/capacity.json").as_path(),
            &serde_json::to_vec(&HostSlotCapacity { slot_count: 1 }).unwrap(),
        );

        let error = service.occupied_slots().unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_ID_INVALID");
        let error = service.acquire(&request(3), &healthy(), 1).unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_ID_INVALID");
        assert_no_slot_zero_lease(&root);
    }

    #[test]
    fn noncanonical_slot_directory_name_is_rejected() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        acquire(&service, &request(1));
        fs::rename(root.join("leases/slots/0"), root.join("leases/slots/00")).unwrap();

        let error = service.occupied_slots().unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_ID_INVALID");
        let error = service.acquire(&request(2), &healthy(), 1).unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_ID_INVALID");
        assert!(!root.join("leases/slots/0/lease.json").exists());
        assert!(root.join("leases/slots/00/lease.json").is_file());
    }

    #[test]
    fn noncanonical_leading_zero_alias_for_high_slot_is_rejected() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        occupy_only_slot_one(&service, &store);
        fs::rename(root.join("leases/slots/1"), root.join("leases/slots/08")).unwrap();

        let error = service.occupied_slots().unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_ID_INVALID");
        let error = service.acquire(&request(3), &healthy(), 1).unwrap_err();
        assert_eq!(error.public_code(), "HOST_SLOT_ID_INVALID");
        assert!(!root.join("leases/slots/0/lease.json").exists());
        assert!(root.join("leases/slots/08/lease.json").is_file());
    }

    #[test]
    fn shrink_ignores_idle_leftover_high_slot_directory() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        service.set_slot_count(2).unwrap();
        service.set_slot_count(1).unwrap();
        let leases = store.open_directory("leases", false).unwrap();
        let slots = leases
            .open_child_directory(&relative(SLOTS_DIR).unwrap(), false)
            .unwrap();
        slots.create_new_child_directory("1").unwrap();

        acquire(&service, &request(1));
        let occupied = service.occupied_slots().unwrap();
        assert_eq!(occupied.len(), 1);
        assert_eq!(occupied[0].slot_id, 0);
        assert_eq!(service.slot_count().unwrap(), 1);
        let occupancy = service.occupancy().unwrap();
        assert_eq!(occupancy.configured_slots, 1);
        assert_eq!(occupancy.busy_slots, 1);
    }

    fn plant_layout2(root: &Path, heavy_bytes: Option<&[u8]>) {
        let leases = root.join("leases");
        let slots = leases.join("slots");
        if slots.exists() {
            fs::remove_dir_all(&slots).unwrap();
        }
        let capacity = leases.join("capacity.json");
        if capacity.exists() {
            fs::remove_file(&capacity).unwrap();
        }
        if let Some(bytes) = heavy_bytes {
            let heavy = leases.join("heavy");
            private_dir(&heavy);
            private_file(&heavy.join("lease.json"), bytes);
        }
        let layout = root.join("layout.json");
        let current = format!("\"version\":{}", crate::host_store::HOST_LAYOUT_VERSION);
        let previous = format!(
            "\"version\":{}",
            crate::host_store::PREVIOUS_HOST_LAYOUT_VERSION
        );
        let bytes = fs::read(&layout).unwrap();
        fs::write(
            &layout,
            String::from_utf8(bytes)
                .unwrap()
                .replace(&current, &previous),
        )
        .unwrap();
    }

    #[test]
    fn genuine_layout2_heavy_residue_refuses_migrate_and_keeps_original_bytes() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let stale = store.clone();
        drop(store);
        let leftover = br#"{"layout2":"heavy-lease"}"#;
        plant_layout2(&root, Some(leftover));
        assert_eq!(
            match HostStore::open(&root) {
                Ok(_) => panic!("layout-2 host opened without migrate"),
                Err(error) => error.public_code(),
            },
            "HOST_LAYOUT_OUTDATED"
        );
        assert!(stale.validate_layout().is_err());
        let error = HostStore::migrate_layout(&root).unwrap_err();
        assert_eq!(error.public_code(), "HOST_UPGRADE_DRAIN_REQUIRED");
        assert!(
            error.to_string().contains("live layout-2 lease remains"),
            "{error}"
        );
        assert_eq!(
            fs::read(root.join("leases/heavy/lease.json")).unwrap(),
            leftover
        );
        assert!(!root.join("leases/slots").exists());
        assert!(!root.join("leases/capacity.json").exists());
        assert!(root.join("leases/capacity.lock").is_file());
        let layout: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("layout.json")).unwrap()).unwrap();
        assert_eq!(
            layout["version"],
            crate::host_store::PREVIOUS_HOST_LAYOUT_VERSION
        );
        assert!(stale.validate_layout().is_err());
    }

    #[test]
    fn genuine_layout2_terminal_archive_migrates_and_stale_handle_fails() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open(&root).unwrap();
        let service = LeaseService::new(&store);
        let req = request(1);
        let lease = acquire(&service, &req);
        publish_job(&store, &lease, &req);
        store
            .record_terminal_status(&lease, &JobStatus::succeeded(100, 0, 0).unwrap())
            .unwrap();
        let receipt = store.cleanup_job_owned(&lease).unwrap();
        service.release_after_cleanup(&lease, &receipt).unwrap();
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        let archived_meta = fs::read(job.join("meta.json")).unwrap();
        let archived_status = fs::read(job.join("status.json")).unwrap();
        let index = fs::read(store.job_index(lease.job_id()).unwrap()).unwrap();
        let stale = store.clone();
        drop(store);
        plant_layout2(&root, None);
        assert!(!root.join("leases/slots").exists());
        assert!(!root.join("leases/capacity.json").exists());
        assert!(root.join("leases/capacity.lock").is_file());
        assert_eq!(
            match HostStore::open(&root) {
                Ok(_) => panic!("layout-2 host opened without migrate"),
                Err(error) => error.public_code(),
            },
            "HOST_LAYOUT_OUTDATED"
        );
        assert!(stale.validate_layout().is_err());
        HostStore::migrate_layout(&root).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &fs::read(root.join("layout.json")).unwrap()
            )
            .unwrap()["version"],
            crate::host_store::HOST_LAYOUT_VERSION
        );
        assert!(root.join("leases/slots").is_dir());
        assert!(root.join("leases/capacity.json").is_file());
        assert_eq!(fs::read(job.join("meta.json")).unwrap(), archived_meta);
        assert_eq!(fs::read(job.join("status.json")).unwrap(), archived_status);
        assert_eq!(
            fs::read(
                HostStore::open(&root)
                    .unwrap()
                    .job_index(lease.job_id())
                    .unwrap()
            )
            .unwrap(),
            index
        );
        assert!(HostStore::open(&root).is_ok());
        assert!(
            stale.validate_layout().is_err(),
            "pre-migration handle must fail after layout and installation identity refresh"
        );
        assert_eq!(
            crate::host_store::ROLLBACK_HELPER_LAYOUT_VERSION,
            crate::host_store::PREVIOUS_HOST_LAYOUT_VERSION
        );
    }

    #[test]
    fn genuine_layout2_migrate_refresh_crash_residue_reopens_and_rejects_stale() {
        for point in [
            crate::host_store::HostStoreWritePoint::AfterHostLayoutRefreshUnlink,
            crate::host_store::HostStoreWritePoint::AfterHostLayoutRefreshPublish,
            crate::host_store::HostStoreWritePoint::AfterInstallationIdentityRefreshUnlink,
            crate::host_store::HostStoreWritePoint::AfterInstallationIdentityRefreshPublish,
        ] {
            let temp = tempdir().unwrap();
            let root = temp.path().join("host");
            let store = HostStore::open(&root).unwrap();
            let service = LeaseService::new(&store);
            let req = request(1);
            let lease = acquire(&service, &req);
            publish_job(&store, &lease, &req);
            store
                .record_terminal_status(&lease, &JobStatus::succeeded(100, 0, 0).unwrap())
                .unwrap();
            let receipt = store.cleanup_job_owned(&lease).unwrap();
            service.release_after_cleanup(&lease, &receipt).unwrap();
            let job = store
                .job(lease.project_id(), lease.worktree_id(), lease.job_id())
                .unwrap();
            let archived_meta = fs::read(job.join("meta.json")).unwrap();
            let archived_status = fs::read(job.join("status.json")).unwrap();
            let archived_index = fs::read(store.job_index(lease.job_id()).unwrap()).unwrap();
            let stale = store.clone();
            drop(store);
            plant_layout2(&root, None);
            assert!(
                HostStore::migrate_layout_with_write_fault(&root, point).is_err(),
                "fault at {point:?}"
            );
            match point {
                crate::host_store::HostStoreWritePoint::AfterHostLayoutRefreshUnlink => {
                    assert!(
                        !root.join("layout.json").exists(),
                        "unlink fault must not truncate layout.json in place: {point:?}"
                    );
                    let refresh = fs::read(root.join("layout.refresh.json")).unwrap();
                    assert!(!refresh.is_empty(), "{point:?}");
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&refresh).unwrap()["version"],
                        crate::host_store::PREVIOUS_HOST_LAYOUT_VERSION
                    );
                }
                _ => {
                    let layout = fs::read(root.join("layout.json")).unwrap();
                    assert!(!layout.is_empty(), "{point:?}");
                    assert_eq!(
                        serde_json::from_slice::<serde_json::Value>(&layout).unwrap()["version"],
                        crate::host_store::HOST_LAYOUT_VERSION
                    );
                    assert!(
                        root.join("layout.refresh.json").is_file(),
                        "layout residue must survive until installation identity is bound: {point:?}"
                    );
                }
            }
            assert!(stale.validate_layout().is_err(), "{point:?}");
            match HostStore::open(&root) {
                Ok(_) => {}
                Err(error) if error.public_code() == "HOST_LAYOUT_OUTDATED" => {
                    HostStore::migrate_layout(&root).unwrap();
                    HostStore::open(&root).expect("fresh open after migrate repair");
                }
                Err(error) => panic!("repair after {point:?} failed: {error}"),
            }
            assert!(stale.validate_layout().is_err(), "{point:?}");
            assert_eq!(fs::read(job.join("meta.json")).unwrap(), archived_meta);
            assert_eq!(fs::read(job.join("status.json")).unwrap(), archived_status);
            assert_eq!(
                fs::read(
                    HostStore::open(&root)
                        .unwrap()
                        .job_index(lease.job_id())
                        .unwrap()
                )
                .unwrap(),
                archived_index
            );
            assert!(!root.join("layout.refresh.json").exists(), "{point:?}");
        }
    }
}
