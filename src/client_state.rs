use std::{
    collections::{BTreeSet, HashSet},
    ffi::{CStr, CString, OsStr},
    fs::File,
    io::{self, Read, Write},
    mem::MaybeUninit,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::ffi::OsStrExt,
    },
    path::{Component, Path},
    str::FromStr,
    sync::{
        Arc, Condvar, LazyLock, Mutex, Weak,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use uuid::Uuid;

use crate::{
    config::Config,
    error::WorkerError,
    job::{
        AdmissionObservation, CachedAdmissionObservation, ClientId, JobId, JobState, JobStatus,
        LocalJobRecord, ProcessIdentity, QueueAbandonmentProof, QueueCancel, QueueClaim,
        QueueEntry, QueueEntryKind, QueueSnapshot, QueueState, RemoteUncertainty,
        ResolveOrAbandonRequest,
    },
    scheduler::{
        AffinityHints, CandidateObservation, CandidateRejection, QueueBlockingReason,
        SchedulerPolicy, Selection, WorkerPreference,
    },
    supervisor::{ProcessInspector, ProcessObservation, SystemProcessInspector},
    transfer::PreacceptanceAbandonmentReceipt,
};

const DIRECTORY_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
const READ_FILE_FLAGS: libc::c_int =
    libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;
const MAX_STATE_FILE_BYTES: usize = 1024 * 1024;
const CLIENT_ID_NAME: &CStr = c"client-id";
const JOBS_NAME: &CStr = c"jobs";
const OPERATIONS_NAME: &CStr = c".mac-worker-state";
const LOCK_NAME: &CStr = c"jobs.lock";
const PAYLOAD_NAME: &CStr = c"payload";
const QUEUE_NAME: &CStr = c"queue";
const QUEUE_STATE_NAME: &CStr = c"state.json";
const QUEUE_LOCK_NAME: &CStr = c"lock";
const AFFINITY_NAME: &CStr = c"affinity";
const AFFINITY_PROJECTS_NAME: &CStr = c"projects";
const AFFINITY_WORKTREES_NAME: &CStr = c"worktrees";
const OBSERVATIONS_NAME: &CStr = c"observations";
const OBSERVATION_TTL_MILLIS: u64 = 2_000;

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStateWritePoint {
    BeforePublish = 1,
    AfterPublish = 2,
    SwapOperationPayloadBeforePublish = 3,
    SwapOperationDirectoryBeforeCleanup = 4,
    SwapLiveJobBeforeReplace = 5,
    SwapOperationDirectoryAfterValidationBeforeRemoval = 6,
    SwapOperationChildAfterValidationBeforeRemoval = 7,
    SwapPublishedRollbackAfterValidationBeforeRemoval = 8,
    SwapLiveJobWithSymlinkBeforeReplace = 9,
    SwapLiveJobWithDirectoryBeforeReplace = 10,
    SwapLiveJobWithFifoBeforeReplace = 11,
    SwapLiveJobWithPermissiveFileBeforeReplace = 12,
    CrashCleanupAfterRetirementCreated = 13,
    CrashCleanupAfterOperationMoved = 14,
    CrashCleanupBeforeNestedCleanup = 15,
    CrashRollbackAfterEntryMoved = 16,
    CrashRollbackBeforeNestedCleanup = 17,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStateCreationRacePoint {
    RootComponent = 1,
    OwnedDirectory = 2,
    LockFile = 3,
}

/// Test-only synchronization points around the durable scheduler boundaries.
/// Normal stores have no hook, so production execution remains inert.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStateConcurrencyPoint {
    QueuePublication,
    ClaimRunCapEvaluation,
    ObservationRefreshPublication,
}

/// A deterministic test hook for scheduler persistence races. Implementors
/// must not grant capacity or alter durable state.
#[doc(hidden)]
pub trait ClientStateConcurrencyHook: Send + Sync {
    fn reach(&self, point: ClientStateConcurrencyPoint);
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientStateSyncCounts {
    pub parent_directories: u64,
    pub root: u64,
    pub jobs: u64,
    pub concurrent_loser_parents: u64,
}

#[derive(Clone)]
pub struct ClientStateStore {
    inner: Arc<ClientStateInner>,
}

pub(crate) enum ConditionalStatusUpdate {
    Applied(LocalJobRecord),
    Conflict(LocalJobRecord),
}

pub(crate) enum ObservationRelation {
    RemoteAdvances,
    CurrentAtLeastRemote,
    Conflict,
}

/// One durable queue row plus its advisory, cache-derived blocking reason.
/// A `None` reason means the row is dispatching or is eligible according to
/// the cached policy facts; it is not an admission decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRowWithBlockingReason {
    entry: QueueEntry,
    blocking_reason: Option<QueueBlockingReason>,
}

impl QueueRowWithBlockingReason {
    pub fn entry(&self) -> &QueueEntry {
        &self.entry
    }

    pub fn blocking_reason(&self) -> Option<&QueueBlockingReason> {
        self.blocking_reason.as_ref()
    }
}

/// Durable observation of the row that a public cancellation previously
/// marked while it was dispatching. This is intentionally queue-lock scoped:
/// callers must release the lock before resolving a host or waiting for an
/// owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DispatchCancellationObservation {
    NotRequested,
    Requested,
    WaitingCancelled,
    Gone,
}

struct ClientStateInner {
    root: OwnedFd,
    jobs: OwnedFd,
    operations: OwnedFd,
    queue: OwnedFd,
    affinity_projects: OwnedFd,
    affinity_worktrees: OwnedFd,
    observations: OwnedFd,
    client_id: ClientId,
    owner_inspector: Arc<dyn ProcessInspector>,
    concurrency_hook: Option<Arc<dyn ClientStateConcurrencyHook>>,
    write_fault: Arc<AtomicU8>,
    sync_counts: Arc<SyncCounters>,
    cleanup_pause: Mutex<Option<Arc<CleanupPauseState>>>,
}

struct CleanupPauseState {
    state: Mutex<CleanupPauseFlags>,
    changed: Condvar,
}

#[derive(Default)]
struct CleanupPauseFlags {
    paused: bool,
    resumed: bool,
}

#[doc(hidden)]
pub struct ClientStateCleanupPause {
    inner: Arc<CleanupPauseState>,
}

impl ClientStateCleanupPause {
    pub fn wait_until_paused(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("cleanup pause mutex poisoned");
        if !state.paused {
            let (next, timeout) = self
                .inner
                .changed
                .wait_timeout_while(state, Duration::from_secs(5), |state| !state.paused)
                .expect("cleanup pause mutex poisoned");
            state = next;
            assert!(
                !timeout.timed_out() || state.paused,
                "cleanup did not pause"
            );
        }
    }

    pub fn resume(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("cleanup pause mutex poisoned");
        state.resumed = true;
        self.inner.changed.notify_all();
    }
}

impl Drop for ClientStateCleanupPause {
    fn drop(&mut self) {
        self.resume();
    }
}

fn pause_cleanup(pause: Arc<CleanupPauseState>) {
    let mut state = pause.state.lock().expect("cleanup pause mutex poisoned");
    state.paused = true;
    pause.changed.notify_all();
    if !state.resumed {
        let (next, timeout) = pause
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.resumed)
            .expect("cleanup pause mutex poisoned");
        state = next;
        assert!(
            !timeout.timed_out() || state.resumed,
            "cleanup pause was not resumed"
        );
    }
}

struct LockContentionState {
    root: FileIdentity,
    confirmed: Mutex<bool>,
    changed: Condvar,
}

static LOCK_CONTENTION_PROBES: LazyLock<Mutex<Vec<Weak<LockContentionState>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

#[doc(hidden)]
pub struct ClientStateLockContentionProbe {
    inner: Arc<LockContentionState>,
}

impl ClientStateLockContentionProbe {
    pub fn wait_until_confirmed(&self) {
        assert!(
            self.confirmed_within(Duration::from_secs(5)),
            "authoritative local state lock contention was not confirmed"
        );
    }

    pub fn confirmed_within(&self, timeout: Duration) -> bool {
        let mut confirmed = self
            .inner
            .confirmed
            .lock()
            .expect("lock-contention probe mutex poisoned");
        if !*confirmed {
            let (next, _) = self
                .inner
                .changed
                .wait_timeout_while(confirmed, timeout, |confirmed| !*confirmed)
                .expect("lock-contention probe mutex poisoned");
            confirmed = next;
        }
        *confirmed
    }
}

#[derive(Default)]
struct SyncCounters {
    parent_directories: std::sync::atomic::AtomicU64,
    root: std::sync::atomic::AtomicU64,
    jobs: std::sync::atomic::AtomicU64,
    operations: std::sync::atomic::AtomicU64,
    concurrent_loser_parents: std::sync::atomic::AtomicU64,
}

impl ClientStateStore {
    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        Self::open_inner(
            state_root,
            None,
            None,
            Arc::new(SystemProcessInspector),
            None,
        )
    }

    #[doc(hidden)]
    pub fn open_with_write_fault(
        state_root: &Path,
        point: ClientStateWritePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(
            state_root,
            Some(point),
            None,
            Arc::new(SystemProcessInspector),
            None,
        )
    }

    #[doc(hidden)]
    pub fn open_with_creation_race(
        state_root: &Path,
        point: ClientStateCreationRacePoint,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(
            state_root,
            None,
            Some(point),
            Arc::new(SystemProcessInspector),
            None,
        )
    }

    #[doc(hidden)]
    pub fn open_with_owner_inspector<I>(
        state_root: &Path,
        inspector: I,
    ) -> Result<Self, WorkerError>
    where
        I: ProcessInspector + 'static,
    {
        Self::open_inner(state_root, None, None, Arc::new(inspector), None)
    }

    #[doc(hidden)]
    pub fn open_with_concurrency_hook(
        state_root: &Path,
        hook: Arc<dyn ClientStateConcurrencyHook>,
    ) -> Result<Self, WorkerError> {
        Self::open_inner(
            state_root,
            None,
            None,
            Arc::new(SystemProcessInspector),
            Some(hook),
        )
    }

    fn open_inner(
        state_root: &Path,
        initial_fault: Option<ClientStateWritePoint>,
        initial_creation_race: Option<ClientStateCreationRacePoint>,
        owner_inspector: Arc<dyn ProcessInspector>,
        concurrency_hook: Option<Arc<dyn ClientStateConcurrencyHook>>,
    ) -> Result<Self, WorkerError> {
        let write_fault = Arc::new(AtomicU8::new(initial_fault.map_or(0, |point| point as u8)));
        let sync_counts = Arc::new(SyncCounters::default());
        let creation_race = AtomicU8::new(initial_creation_race.map_or(0, |point| point as u8));
        let root = open_or_create_root(state_root, &sync_counts, &creation_race)?;
        require_owned_directory(root.as_raw_fd())?;
        let jobs = open_or_create_owned_directory(
            root.as_raw_fd(),
            JOBS_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let operations = open_or_create_owned_directory(
            root.as_raw_fd(),
            OPERATIONS_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let queue = open_or_create_owned_directory(
            root.as_raw_fd(),
            QUEUE_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let affinity = open_or_create_owned_directory(
            root.as_raw_fd(),
            AFFINITY_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let affinity_projects = open_or_create_owned_directory(
            affinity.as_raw_fd(),
            AFFINITY_PROJECTS_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let affinity_worktrees = open_or_create_owned_directory(
            affinity.as_raw_fd(),
            AFFINITY_WORKTREES_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let observations = open_or_create_owned_directory(
            root.as_raw_fd(),
            OBSERVATIONS_NAME,
            &sync_counts,
            SyncKind::Root,
            &creation_race,
        )?;
        let _lock =
            StateLock::acquire_with_creation_race(root.as_raw_fd(), &sync_counts, &creation_race)?;
        require_same_device(
            root.as_raw_fd(),
            &[
                jobs.as_raw_fd(),
                operations.as_raw_fd(),
                queue.as_raw_fd(),
                affinity.as_raw_fd(),
                affinity_projects.as_raw_fd(),
                affinity_worktrees.as_raw_fd(),
                observations.as_raw_fd(),
            ],
        )?;
        validate_root_entries(root.as_raw_fd())?;
        validate_operation_entries(operations.as_raw_fd())?;
        let client_id = load_or_create_client_id(
            root.as_raw_fd(),
            operations.as_raw_fd(),
            &write_fault,
            &sync_counts,
        )?;
        open_or_create_queue_lock(queue.as_raw_fd())?;
        let queue_snapshot = load_or_create_queue_snapshot(
            queue.as_raw_fd(),
            operations.as_raw_fd(),
            &write_fault,
            &sync_counts,
        )?;
        require_queue_client(&queue_snapshot, client_id)?;
        validate_affinity_entries(
            affinity.as_raw_fd(),
            affinity_projects.as_raw_fd(),
            affinity_worktrees.as_raw_fd(),
        )?;
        validate_observation_entries(observations.as_raw_fd())?;

        Ok(Self {
            inner: Arc::new(ClientStateInner {
                root,
                jobs,
                operations,
                queue,
                affinity_projects,
                affinity_worktrees,
                observations,
                client_id,
                owner_inspector,
                concurrency_hook,
                write_fault,
                sync_counts,
                cleanup_pause: Mutex::new(None),
            }),
        })
    }

    pub fn client_id(&self) -> ClientId {
        self.inner.client_id
    }

    pub fn enqueue(&self, mut entry: QueueEntry) -> Result<QueueEntry, WorkerError> {
        entry.validate()?;
        if entry.client_id() != self.inner.client_id {
            return Err(queue_error(
                "QUEUE_CLIENT_MISMATCH",
                "queue row belongs to another client identity",
            ));
        }
        if entry.queue_id().value() != 0 {
            return Err(queue_error(
                "QUEUE_ID_ASSIGNED",
                "new queue row already has a sequence ID",
            ));
        }
        self.update_queue(|snapshot| {
            if snapshot
                .entries
                .iter()
                .any(|existing| existing.job_id() == entry.job_id())
            {
                return Err(queue_error(
                    "QUEUE_JOB_CONFLICT",
                    "job ID is already present in the queue",
                ));
            }
            if let Some(previous) = snapshot.entries.last()
                && entry.enqueued_at_millis() < previous.enqueued_at_millis()
            {
                return Err(queue_error(
                    "QUEUE_TIME_REGRESSION",
                    "queue enqueue timestamp moved backwards",
                ));
            }
            let assigned = snapshot.next_id;
            let next = assigned.checked_next()?;
            entry.assign_queue_id(assigned);
            snapshot.next_id = next;
            snapshot.entries.push(entry.clone());
            Ok((entry.clone(), true))
        })
    }

    pub fn queue_snapshot(&self) -> Result<QueueSnapshot, WorkerError> {
        let _lock = QueueLock::acquire(
            self.inner.root.as_raw_fd(),
            self.inner.queue.as_raw_fd(),
            &self.inner.sync_counts,
        )?;
        let snapshot = read_queue_snapshot(self.inner.queue.as_raw_fd())?.0;
        require_queue_client(&snapshot, self.inner.client_id)?;
        Ok(snapshot)
    }

    /// Read the durable queue and last cached admission observations without
    /// refreshing either.  This is a dashboard-compatible advisory view, not
    /// a scheduler admission path.
    pub fn queue_rows_with_blocking_reasons(
        &self,
        config: &Config,
    ) -> Result<Vec<QueueRowWithBlockingReason>, WorkerError> {
        let _lock = QueueLock::acquire(
            self.inner.root.as_raw_fd(),
            self.inner.queue.as_raw_fd(),
            &self.inner.sync_counts,
        )?;
        let snapshot = read_queue_snapshot(self.inner.queue.as_raw_fd())?.0;
        require_queue_client(&snapshot, self.inner.client_id)?;
        let observations = config
            .workers
            .iter()
            .map(|worker| {
                read_observation_optional(self.inner.observations.as_raw_fd(), &worker.name)?
                    .map(|observation| {
                        CandidateObservation::new(
                            observation.worker_name().to_owned(),
                            observation.ready(),
                            observation.slot(),
                            observation.capabilities().to_vec(),
                            observation.available_memory_bytes(),
                            observation.free_disk_bytes(),
                        )
                        .map_err(|_| {
                            WorkerError::Protocol("cached scheduler observation is invalid".into())
                        })
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, WorkerError>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        snapshot
            .entries()
            .iter()
            .cloned()
            .map(|entry| {
                let blocking_reason =
                    self.queue_blocking_reason(&snapshot, &entry, &observations)?;
                Ok(QueueRowWithBlockingReason {
                    entry,
                    blocking_reason,
                })
            })
            .collect()
    }

    pub fn adopt_row(
        &self,
        job_id: JobId,
        new_owner: ProcessIdentity,
    ) -> Result<QueueEntry, WorkerError> {
        new_owner.validate()?;
        self.update_queue(|snapshot| {
            let entry = find_queue_entry_mut(snapshot, job_id)?;
            entry.adopt(new_owner).map_err(|_| {
                queue_error(
                    "QUEUE_ADOPTION_CONFLICT",
                    "queue row cannot be adopted in its current state",
                )
            })?;
            Ok((entry.clone(), true))
        })
    }

    pub fn claim_next(
        &self,
        owner: ProcessIdentity,
        ranked_workers: &[String],
        claimed_at_millis: u64,
    ) -> Result<Option<QueueClaim>, WorkerError> {
        owner.validate()?;
        if claimed_at_millis == 0 {
            return Err(queue_error(
                "QUEUE_CLAIM_INVALID",
                "queue claim timestamp must be positive",
            ));
        }
        let mut names = BTreeSet::new();
        for worker in ranked_workers {
            validate_state_worker_name(worker)?;
            if !names.insert(worker.as_str()) {
                return Err(queue_error(
                    "QUEUE_CANDIDATES_INVALID",
                    "ranked workers contain a duplicate",
                ));
            }
        }

        self.update_queue(|snapshot| {
            self.reach_concurrency_point(ClientStateConcurrencyPoint::ClaimRunCapEvaluation);
            let mut selected = None;
            for worker in ranked_workers {
                if snapshot.entries.iter().any(|entry| {
                    matches!(
                        entry.state(),
                        QueueState::Dispatching { selected_worker, .. }
                            if selected_worker == worker
                    )
                }) {
                    continue;
                }
                let capabilities = read_observation_optional(
                    self.inner.observations.as_raw_fd(),
                    worker,
                )?
                .map(|observation| observation.capabilities().to_vec());
                for index in 0..snapshot.entries.len() {
                    let candidate = &snapshot.entries[index];
                    if !matches!(candidate.state(), QueueState::Waiting { owner: row_owner } if *row_owner == owner)
                        || candidate.is_cancel_requested()
                        || !candidate.eligible_for(worker, capabilities.as_deref())
                        || !self.run_has_capacity(snapshot, candidate)?
                    {
                        continue;
                    }
                    let mut blocked = false;
                    for older in &snapshot.entries[..index] {
                        if matches!(older.state(), QueueState::Waiting { .. })
                            && !older.is_cancel_requested()
                            && older.eligible_for(worker, capabilities.as_deref())
                            && self.run_has_capacity(snapshot, older)?
                            && self.owner_is_live_or_ambiguous(older.owner(), owner)
                        {
                            blocked = true;
                            break;
                        }
                    }
                    if !blocked {
                        selected = Some((index, worker.clone()));
                        break;
                    }
                }
                if selected.is_some() {
                    break;
                }
            }

            let Some((index, worker)) = selected else {
                return Ok((None, false));
            };
            let entry = &mut snapshot.entries[index];
            entry.dispatch(owner, worker, claimed_at_millis)?;
            Ok((Some(QueueClaim::new(entry.clone())), true))
        })
    }

    pub fn revert_dispatch(
        &self,
        job_id: JobId,
        dispatch_owner: ProcessIdentity,
    ) -> Result<QueueEntry, WorkerError> {
        dispatch_owner.validate()?;
        self.update_queue(|snapshot| {
            let entry = find_queue_entry_mut(snapshot, job_id)?;
            entry.revert(dispatch_owner).map_err(|_| {
                queue_error(
                    "QUEUE_OWNER_MISMATCH",
                    "queue dispatch owner does not match",
                )
            })?;
            Ok((entry.clone(), true))
        })
    }

    pub fn record_preacceptance_abandoned(
        &self,
        receipt: &PreacceptanceAbandonmentReceipt,
        dispatch_owner: ProcessIdentity,
    ) -> Result<QueueEntry, WorkerError> {
        dispatch_owner.validate()?;
        self.update_queue(|snapshot| {
            let name = job_file_name(receipt.job_id())?;
            let record = read_job_optional(self.inner.jobs.as_raw_fd(), &name)?
                .ok_or_else(abandonment_conflict)?;
            self.require_local_client(&record)
                .map_err(|_| abandonment_conflict())?;
            if record.last_status().is_some()
                || record.remote_uncertainty() != &RemoteUncertainty::None
            {
                return Err(abandonment_conflict());
            }
            let durable_request = ResolveOrAbandonRequest::from_local_record(&record)
                .map_err(|_| abandonment_conflict())?;
            if !receipt.matches_request(&durable_request) {
                return Err(abandonment_conflict());
            }
            let entry = find_queue_entry_mut(snapshot, receipt.job_id())?;
            let proof =
                QueueAbandonmentProof::from_resolution(entry, &durable_request, dispatch_owner)
                    .map_err(|_| abandonment_conflict())?;
            let changed = entry
                .record_preacceptance_abandoned(proof)
                .map_err(|_| abandonment_conflict())?;
            Ok((entry.clone(), changed))
        })
    }

    pub fn request_queue_cancel(
        &self,
        job_id: JobId,
        requested_at_millis: u64,
    ) -> Result<Option<QueueCancel>, WorkerError> {
        self.update_queue(|snapshot| {
            let Some(index) = snapshot
                .entries
                .iter()
                .position(|entry| entry.job_id() == job_id)
            else {
                return Ok((None, false));
            };
            match snapshot.entries[index].state().clone() {
                QueueState::Waiting { .. } => {
                    snapshot.entries.remove(index);
                    Ok((Some(QueueCancel::RemovedWaiting { job_id }), true))
                }
                QueueState::Dispatching { dispatch_owner, .. } => {
                    snapshot.entries[index].request_cancel(requested_at_millis)?;
                    Ok((
                        Some(QueueCancel::RequestedDispatch {
                            job_id,
                            dispatch_owner,
                        }),
                        true,
                    ))
                }
            }
        })
    }

    pub(crate) fn observe_dispatch_cancellation(
        &self,
        job_id: JobId,
        dispatch_owner: ProcessIdentity,
    ) -> Result<DispatchCancellationObservation, WorkerError> {
        dispatch_owner.validate()?;
        self.update_queue(|snapshot| {
            let Some(entry) = snapshot
                .entries
                .iter()
                .find(|entry| entry.job_id() == job_id)
            else {
                return Ok((DispatchCancellationObservation::Gone, false));
            };
            let observation = match entry.state() {
                QueueState::Dispatching {
                    dispatch_owner: current,
                    ..
                } if *current == dispatch_owner && entry.is_cancel_requested() => {
                    DispatchCancellationObservation::Requested
                }
                QueueState::Dispatching {
                    dispatch_owner: current,
                    ..
                } if *current == dispatch_owner => DispatchCancellationObservation::NotRequested,
                QueueState::Waiting { .. } if entry.is_cancel_requested() => {
                    DispatchCancellationObservation::WaitingCancelled
                }
                QueueState::Dispatching { .. } => {
                    return Err(queue_error(
                        "QUEUE_OWNER_MISMATCH",
                        "queue dispatch owner does not match",
                    ));
                }
                QueueState::Waiting { .. } => DispatchCancellationObservation::NotRequested,
            };
            Ok((observation, false))
        })
    }

    /// Retires a cancellation-requested dispatch before a local immutable job
    /// record exists. At that point Task 3/4 ordering proves no remote
    /// boundary has been reached, so this is the only safe no-SSH claim-race
    /// retirement path.
    pub(crate) fn remove_cancelled_dispatch_before_local_record(
        &self,
        job_id: JobId,
        dispatch_owner: ProcessIdentity,
    ) -> Result<Option<QueueEntry>, WorkerError> {
        dispatch_owner.validate()?;
        self.update_queue(|snapshot| {
            let Some(index) = snapshot
                .entries
                .iter()
                .position(|entry| entry.job_id() == job_id)
            else {
                return Ok((None, false));
            };
            let entry = &snapshot.entries[index];
            if !matches!(
                entry.state(),
                QueueState::Dispatching { dispatch_owner: current, .. } if *current == dispatch_owner
            ) || !entry.is_cancel_requested()
            {
                return Err(queue_error(
                    "QUEUE_CANCEL_CONFLICT",
                    "dispatch row is not the exact cancellation-requested owner",
                ));
            }
            let name = job_file_name(job_id)?;
            if read_job_optional(self.inner.jobs.as_raw_fd(), &name)?.is_some() {
                return Ok((None, false));
            }
            Ok((Some(snapshot.entries.remove(index)), true))
        })
    }

    pub fn remove_after_terminal(
        &self,
        job_id: JobId,
        dispatch_owner: ProcessIdentity,
    ) -> Result<QueueEntry, WorkerError> {
        dispatch_owner.validate()?;
        self.update_queue(|snapshot| {
            let index = snapshot
                .entries
                .iter()
                .position(|entry| entry.job_id() == job_id)
                .ok_or_else(|| queue_error("QUEUE_NOT_FOUND", "queue row was not found"))?;
            if !matches!(
                snapshot.entries[index].state(),
                QueueState::Dispatching { dispatch_owner: current, .. } if *current == dispatch_owner
            ) {
                return Err(queue_error(
                    "QUEUE_OWNER_MISMATCH",
                    "queue dispatch owner does not match",
                ));
            }
            if snapshot.entries[index]
                .preacceptance_abandonment_proof()
                .is_none()
            {
                let name = job_file_name(job_id)?;
                let record = read_job_optional(self.inner.jobs.as_raw_fd(), &name)?
                    .ok_or_else(terminal_unproven)?;
                if !terminal_record_matches(&snapshot.entries[index], &record) {
                    return Err(terminal_unproven());
                }
            }
            Ok((snapshot.entries.remove(index), true))
        })
    }

    pub fn recover_dead_dispatches(&self) -> Result<Vec<JobId>, WorkerError> {
        self.update_queue(|snapshot| {
            let mut recovered = Vec::new();
            let mut index = 0;
            while index < snapshot.entries.len() {
                if snapshot.entries[index].kind() != QueueEntryKind::Batch {
                    index += 1;
                    continue;
                }
                let QueueState::Dispatching { dispatch_owner, .. } =
                    snapshot.entries[index].state()
                else {
                    index += 1;
                    continue;
                };
                let dispatch_owner = *dispatch_owner;
                if matches!(
                    self.inner.owner_inspector.observe(dispatch_owner),
                    ProcessObservation::Absent | ProcessObservation::Reused
                ) {
                    let job_id = snapshot.entries[index].job_id();
                    if snapshot.entries[index]
                        .preacceptance_abandonment_proof()
                        .is_some()
                    {
                        recovered.push(job_id);
                        snapshot.entries.remove(index);
                        continue;
                    }
                    let name = job_file_name(job_id)?;
                    if let Some(record) = read_job_optional(self.inner.jobs.as_raw_fd(), &name)? {
                        self.require_local_client(&record)?;
                        if terminal_record_matches(&snapshot.entries[index], &record) {
                            recovered.push(job_id);
                            snapshot.entries.remove(index);
                            continue;
                        }
                        if !local_record_matches_queue_entry(&snapshot.entries[index], &record) {
                            return Err(queue_error(
                                "QUEUE_JOB_RECORD_MISMATCH",
                                "local job metadata does not match its queue row",
                            ));
                        }
                        index += 1;
                        continue;
                    }
                    recovered.push(job_id);
                    snapshot.entries[index].revert(dispatch_owner)?;
                }
                index += 1;
            }
            let changed = !recovered.is_empty();
            Ok((recovered, changed))
        })
    }

    pub fn remove_queued(&self, job_id: JobId) -> Result<Option<QueueEntry>, WorkerError> {
        self.update_queue(|snapshot| {
            let Some(index) = snapshot
                .entries
                .iter()
                .position(|entry| entry.job_id() == job_id)
            else {
                return Ok((None, false));
            };
            if !matches!(snapshot.entries[index].state(), QueueState::Waiting { .. }) {
                return Err(queue_error(
                    "QUEUE_STATE_CONFLICT",
                    "only a waiting queue row can be removed",
                ));
            }
            Ok((Some(snapshot.entries.remove(index)), true))
        })
    }

    pub fn record_affinity(
        &self,
        project_id: &str,
        worktree_id: &str,
        worker: &str,
        observed_at_millis: u64,
    ) -> Result<(), WorkerError> {
        validate_affinity_key(project_id, "project ID")?;
        validate_affinity_key(worktree_id, "worktree ID")?;
        validate_state_worker_name(worker)?;
        if observed_at_millis == 0 {
            return Err(queue_error(
                "AFFINITY_INVALID",
                "affinity timestamp must be positive",
            ));
        }
        let project = ProjectAffinityRecord {
            project_id: project_id.to_owned(),
            worker: worker.to_owned(),
            observed_at_millis,
        };
        let worktree = WorktreeAffinityRecord {
            project_id: project_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            worker: worker.to_owned(),
            observed_at_millis,
        };
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        read_worktree_affinity_optional(
            self.inner.affinity_worktrees.as_raw_fd(),
            &worktree_affinity_name(project_id, worktree_id)?,
        )?;
        read_project_affinity_optional(
            self.inner.affinity_projects.as_raw_fd(),
            &project_affinity_name(project_id)?,
        )?;
        publish_affinity_record(
            self.inner.affinity_worktrees.as_raw_fd(),
            &worktree_affinity_name(project_id, worktree_id)?,
            &canonical_json_bytes(&worktree, "worktree affinity")?,
            self,
        )?;
        publish_affinity_record(
            self.inner.affinity_projects.as_raw_fd(),
            &project_affinity_name(project_id)?,
            &canonical_json_bytes(&project, "project affinity")?,
            self,
        )
    }

    pub fn affinity_hints(
        &self,
        project_id: &str,
        worktree_id: &str,
    ) -> Result<AffinityHints, WorkerError> {
        validate_affinity_key(project_id, "project ID")?;
        validate_affinity_key(worktree_id, "worktree ID")?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let project = read_project_affinity_optional(
            self.inner.affinity_projects.as_raw_fd(),
            &project_affinity_name(project_id)?,
        )?;
        let worktree = read_worktree_affinity_optional(
            self.inner.affinity_worktrees.as_raw_fd(),
            &worktree_affinity_name(project_id, worktree_id)?,
        )?;
        if project
            .as_ref()
            .is_some_and(|record| record.project_id != project_id)
            || worktree.as_ref().is_some_and(|record| {
                record.project_id != project_id || record.worktree_id != worktree_id
            })
        {
            return Err(invalid_state(
                "affinity filename and record identity differ",
            ));
        }
        Ok(AffinityHints {
            worktree_worker: worktree.map(|record| record.worker),
            project_worker: project.map(|record| record.worker),
        })
    }

    /// Drops only the exact affinity records for this local project/worktree
    /// when they still name `worker`. A successful fresh probe may establish
    /// that a worker is reachable but no longer eligible; that advisory fact
    /// must not keep steering later scheduling attempts to the worker.
    pub fn remove_affinity_if_matches(
        &self,
        project_id: &str,
        worktree_id: &str,
        worker: &str,
    ) -> Result<(), WorkerError> {
        validate_affinity_key(project_id, "project ID")?;
        validate_affinity_key(worktree_id, "worktree ID")?;
        validate_state_worker_name(worker)?;
        let project_name = project_affinity_name(project_id)?;
        let worktree_name = worktree_affinity_name(project_id, worktree_id)?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let project = read_project_affinity_optional(
            self.inner.affinity_projects.as_raw_fd(),
            &project_name,
        )?;
        let worktree = read_worktree_affinity_optional(
            self.inner.affinity_worktrees.as_raw_fd(),
            &worktree_name,
        )?;
        if project
            .as_ref()
            .is_some_and(|record| record.project_id != project_id)
            || worktree.as_ref().is_some_and(|record| {
                record.project_id != project_id || record.worktree_id != worktree_id
            })
        {
            return Err(invalid_state(
                "affinity filename and record identity differ",
            ));
        }
        if project
            .as_ref()
            .is_some_and(|record| record.worker == worker)
        {
            unlink_at(self.inner.affinity_projects.as_raw_fd(), &project_name, 0)
                .map_err(WorkerError::Io)?;
            sync_directory(self.inner.affinity_projects.as_raw_fd())?;
        }
        if worktree
            .as_ref()
            .is_some_and(|record| record.worker == worker)
        {
            unlink_at(self.inner.affinity_worktrees.as_raw_fd(), &worktree_name, 0)
                .map_err(WorkerError::Io)?;
            sync_directory(self.inner.affinity_worktrees.as_raw_fd())?;
        }
        Ok(())
    }

    pub fn admission_observation<F>(
        &self,
        worker: &str,
        now_millis: u64,
        refresh: F,
    ) -> Result<CachedAdmissionObservation, WorkerError>
    where
        F: FnOnce() -> Result<AdmissionObservation, WorkerError>,
    {
        validate_state_worker_name(worker)?;
        let cached = {
            let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
            read_observation_optional(self.inner.observations.as_raw_fd(), worker)?
                .map(|observation| CachedAdmissionObservation::new(observation, now_millis))
                .transpose()?
        };
        if cached
            .as_ref()
            .is_some_and(|cached| cached.age_millis() <= OBSERVATION_TTL_MILLIS)
        {
            return Ok(cached.expect("fresh observation is present"));
        }

        let marker = {
            let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
            open_or_create_refresh_marker(
                self.inner.observations.as_raw_fd(),
                worker,
                &self.inner.sync_counts,
            )?
        };
        let refresh_lock = match RefreshLock::try_acquire(marker)? {
            RefreshAcquire::Acquired(lock) => lock,
            RefreshAcquire::Busy if cached.is_some() => {
                return Ok(cached.expect("busy refresh has a stale observation"));
            }
            RefreshAcquire::Busy => {
                let marker = {
                    let _lock =
                        StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
                    open_refresh_marker(self.inner.observations.as_raw_fd(), worker)?
                };
                RefreshLock::acquire(marker)?
            }
        };

        let current = {
            let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
            read_observation_optional(self.inner.observations.as_raw_fd(), worker)?
                .map(|observation| CachedAdmissionObservation::new(observation, now_millis))
                .transpose()?
        };
        if current
            .as_ref()
            .is_some_and(|cached| cached.age_millis() <= OBSERVATION_TTL_MILLIS)
        {
            drop(refresh_lock);
            return Ok(current.expect("fresh observation is present"));
        }

        let observation = refresh()?;
        observation.validate()?;
        if observation.worker_name() != worker {
            return Err(queue_error(
                "OBSERVATION_WORKER_MISMATCH",
                "refreshed observation belongs to another worker",
            ));
        }
        let cached = CachedAdmissionObservation::new(observation.clone(), now_millis)?;
        let bytes = canonical_json_bytes(&observation, "admission observation")?;
        {
            let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
            self.reach_concurrency_point(
                ClientStateConcurrencyPoint::ObservationRefreshPublication,
            );
            publish_observation(self.inner.observations.as_raw_fd(), worker, &bytes, self)?;
        }
        drop(refresh_lock);
        Ok(cached)
    }

    fn update_queue<R, F>(&self, update: F) -> Result<R, WorkerError>
    where
        F: FnOnce(&mut QueueSnapshot) -> Result<(R, bool), WorkerError>,
    {
        let _lock = QueueLock::acquire(
            self.inner.root.as_raw_fd(),
            self.inner.queue.as_raw_fd(),
            &self.inner.sync_counts,
        )?;
        let (mut snapshot, identity) = read_queue_snapshot(self.inner.queue.as_raw_fd())?;
        require_queue_client(&snapshot, self.inner.client_id)?;
        let (result, changed) = update(&mut snapshot)?;
        snapshot.validate()?;
        require_queue_client(&snapshot, self.inner.client_id)?;
        if changed {
            self.reach_concurrency_point(ClientStateConcurrencyPoint::QueuePublication);
            publish_queue_snapshot(self, &snapshot, identity)?;
        }
        Ok(result)
    }

    fn reach_concurrency_point(&self, point: ClientStateConcurrencyPoint) {
        if let Some(hook) = &self.inner.concurrency_hook {
            hook.reach(point);
        }
    }

    fn owner_is_live_or_ambiguous(
        &self,
        row_owner: &ProcessIdentity,
        caller: ProcessIdentity,
    ) -> bool {
        *row_owner == caller
            || matches!(
                self.inner.owner_inspector.observe(*row_owner),
                ProcessObservation::Matching { .. } | ProcessObservation::Ambiguous
            )
    }

    fn run_has_capacity(
        &self,
        snapshot: &QueueSnapshot,
        candidate: &QueueEntry,
    ) -> Result<bool, WorkerError> {
        let Some(run) = candidate.run() else {
            return Ok(true);
        };
        let mut active = HashSet::new();
        for sibling in snapshot.entries().iter().filter(|entry| {
            entry
                .run()
                .is_some_and(|other| other.run_id() == run.run_id())
        }) {
            if matches!(sibling.state(), QueueState::Dispatching { .. }) {
                active.insert(sibling.job_id());
            }
            let name = job_file_name(sibling.job_id())?;
            if let Some(record) = read_job_optional(self.inner.jobs.as_raw_fd(), &name)? {
                self.require_local_client(&record)?;
                if !local_record_matches_queue_entry(sibling, &record) {
                    return Err(queue_error(
                        "QUEUE_JOB_RECORD_MISMATCH",
                        "local run job metadata does not match its queue row",
                    ));
                }
                if record.last_status().is_some_and(|status| {
                    matches!(status.state(), JobState::Accepted | JobState::Running)
                }) {
                    active.insert(sibling.job_id());
                }
            }
        }
        Ok(active.len() < run.max_parallel() as usize)
    }

    fn queue_blocking_reason(
        &self,
        snapshot: &QueueSnapshot,
        entry: &QueueEntry,
        observations: &[CandidateObservation],
    ) -> Result<Option<QueueBlockingReason>, WorkerError> {
        if !matches!(entry.state(), QueueState::Waiting { .. }) {
            return Ok(None);
        }
        match SchedulerPolicy::select(
            observations,
            entry.requirements(),
            entry.preference(),
            &AffinityHints::none(),
        ) {
            Selection::Selected(_) if !self.run_has_capacity(snapshot, entry)? => {
                Ok(Some(QueueBlockingReason::RunCap))
            }
            Selection::Selected(_) => Ok(None),
            Selection::NoEligible { rejections } => {
                if let WorkerPreference::Pinned { worker } = entry.preference()
                    && rejections.iter().any(|rejection| {
                        matches!(rejection, CandidateRejection::Busy { name } if name == worker)
                    })
                {
                    return Ok(Some(QueueBlockingReason::PinnedWorkerBusy {
                        worker: worker.clone(),
                    }));
                }
                let mut missing = rejections
                    .iter()
                    .filter_map(|rejection| match rejection {
                        CandidateRejection::MissingCapabilities { missing, .. } => {
                            Some(missing.iter().cloned())
                        }
                        _ => None,
                    })
                    .flatten()
                    .collect::<Vec<_>>();
                missing.sort();
                missing.dedup();
                if !missing.is_empty() {
                    return Ok(Some(QueueBlockingReason::CapabilityMissing { missing }));
                }
                Ok(Some(QueueBlockingReason::NoEligibleWorker))
            }
        }
    }

    /// Removes a local job record that never obtained remote status or
    /// uncertainty. Lease-busy and other pre-acceptance retries must not leave
    /// a reservation-bound identity behind when the authoritative host refused
    /// admission without mutation.
    pub fn remove_unpublished_job(
        &self,
        job_id: JobId,
    ) -> Result<Option<LocalJobRecord>, WorkerError> {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let name = job_file_name(job_id)?;
        let Some(record) = read_job_optional(self.inner.jobs.as_raw_fd(), &name)? else {
            return Ok(None);
        };
        if record.meta().job_id() != job_id {
            return Err(invalid_state("job filename and record identity differ"));
        }
        self.require_local_client(&record)?;
        if record.last_status().is_some() || record.remote_uncertainty() != &RemoteUncertainty::None
        {
            return Err(WorkerError::Protocol(
                "JOB_RECORD_NOT_UNPUBLISHED: local job record already has remote evidence".into(),
            ));
        }
        unlink_at(self.inner.jobs.as_raw_fd(), &name, 0).map_err(WorkerError::Io)?;
        sync_counted(
            self.inner.jobs.as_raw_fd(),
            &self.inner.sync_counts,
            SyncKind::Jobs,
        )?;
        Ok(Some(record))
    }

    pub fn create_job(&self, record: LocalJobRecord) -> Result<(), WorkerError> {
        record.validate()?;
        self.require_local_client(&record)?;
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let name = job_file_name(record.meta().job_id())?;

        if let Some(existing) = read_job_optional(self.inner.jobs.as_raw_fd(), &name)? {
            self.require_local_client(&existing)?;
            sync_counted(
                self.inner.jobs.as_raw_fd(),
                &self.inner.sync_counts,
                SyncKind::Jobs,
            )?;
            return require_same_immutable(&existing, &record);
        }

        let bytes = canonical_record_bytes(&record)?;
        let operation = OperationFile::stage(
            self.inner.operations.as_raw_fd(),
            &bytes,
            Arc::clone(&self.inner.sync_counts),
        )?;
        if self.take_fault(ClientStateWritePoint::BeforePublish) {
            return Err(injected_failure(ClientStateWritePoint::BeforePublish));
        }

        match operation.publish_no_replace(
            self.inner.jobs.as_raw_fd(),
            &name,
            &self.inner.write_fault,
        ) {
            Ok(()) => {}
            Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::EEXIST) => {
                operation.cleanup(&self.inner.write_fault, &[], self.take_cleanup_pause())?;
                let existing = read_job(self.inner.jobs.as_raw_fd(), &name)?;
                self.require_local_client(&existing)?;
                sync_counted(
                    self.inner.jobs.as_raw_fd(),
                    &self.inner.sync_counts,
                    SyncKind::Jobs,
                )?;
                return require_same_immutable(&existing, &record);
            }
            Err(error) => return Err(error),
        }

        if self.take_fault(ClientStateWritePoint::AfterPublish) {
            return Err(injected_failure(ClientStateWritePoint::AfterPublish));
        }
        sync_counted(
            self.inner.jobs.as_raw_fd(),
            &self.inner.sync_counts,
            SyncKind::Jobs,
        )?;
        operation.cleanup(&self.inner.write_fault, &[], self.take_cleanup_pause())?;
        Ok(())
    }

    pub fn load_job(&self, job_id: JobId) -> Result<LocalJobRecord, WorkerError> {
        let name = job_file_name(job_id)?;
        let record = read_job(self.inner.jobs.as_raw_fd(), &name)?;
        if record.meta().job_id() != job_id {
            return Err(invalid_state("job filename and record identity differ"));
        }
        self.require_local_client(&record)?;
        Ok(record)
    }

    pub fn update_job(&self, replacement: LocalJobRecord) -> Result<(), WorkerError> {
        replacement.validate()?;
        self.require_local_client(&replacement)?;
        let job_id = replacement.meta().job_id();
        self.update_locked(job_id, move |existing| {
            require_same_immutable(existing, &replacement)?;
            if existing.remote_uncertainty() != replacement.remote_uncertainty() {
                return Err(WorkerError::Protocol(
                    "stale local job update cannot replace remote uncertainty; use the lock-internal uncertainty updater"
                        .into(),
                ));
            }
            require_forward_observation(existing.last_status(), replacement.last_status())?;
            Ok(Some(replacement))
        })
        .map(|_| ())
    }

    pub fn update_observation(
        &self,
        job_id: JobId,
        status: JobStatus,
    ) -> Result<LocalJobRecord, WorkerError> {
        status.validate()?;
        self.update_locked(job_id, move |existing| {
            require_forward_observation(existing.last_status(), Some(&status))?;
            LocalJobRecord::new(
                existing.meta().clone(),
                existing.lease_token(),
                Some(status),
                existing.remote_uncertainty().clone(),
            )
            .map(Some)
        })
    }

    pub(crate) fn reconcile_authoritative_status(
        &self,
        expected: &LocalJobRecord,
        status: JobStatus,
    ) -> Result<ConditionalStatusUpdate, WorkerError> {
        expected.validate()?;
        self.require_local_client(expected)?;
        status.validate()?;
        let job_id = expected.meta().job_id();
        let mut conflicted = false;
        let current = self.update_locked(job_id, |existing| {
            require_same_immutable(existing, expected)?;
            let selected = match authoritative_observation_relation(existing, &status) {
                ObservationRelation::CurrentAtLeastRemote => existing
                    .last_status()
                    .expect("a current-at-least-remote relation requires an observation")
                    .clone(),
                ObservationRelation::RemoteAdvances => status.clone(),
                ObservationRelation::Conflict => {
                    conflicted = true;
                    return Ok(None);
                }
            };
            LocalJobRecord::new(
                existing.meta().clone(),
                existing.lease_token(),
                Some(selected),
                RemoteUncertainty::None,
            )
            .map(Some)
        })?;
        Ok(if conflicted {
            ConditionalStatusUpdate::Conflict(current)
        } else {
            ConditionalStatusUpdate::Applied(current)
        })
    }

    pub fn set_remote_uncertainty(
        &self,
        job_id: JobId,
        uncertainty: RemoteUncertainty,
    ) -> Result<LocalJobRecord, WorkerError> {
        uncertainty.validate()?;
        self.update_locked(job_id, move |existing| {
            LocalJobRecord::new(
                existing.meta().clone(),
                existing.lease_token(),
                existing.last_status().cloned(),
                uncertainty,
            )
            .map(Some)
        })
    }

    pub(crate) fn set_remote_uncertainty_if_same_immutable(
        &self,
        expected: &LocalJobRecord,
        uncertainty: RemoteUncertainty,
    ) -> Result<LocalJobRecord, WorkerError> {
        expected.validate()?;
        self.require_local_client(expected)?;
        uncertainty.validate()?;
        let job_id = expected.meta().job_id();
        self.update_locked(job_id, move |existing| {
            require_same_immutable(existing, expected)?;
            LocalJobRecord::new(
                existing.meta().clone(),
                existing.lease_token(),
                existing.last_status().cloned(),
                uncertainty,
            )
            .map(Some)
        })
    }

    pub fn list_jobs(&self) -> Result<Vec<LocalJobRecord>, WorkerError> {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let mut entries = directory_entries(self.inner.jobs.as_raw_fd())?;
        entries.sort();
        entries
            .into_iter()
            .map(|entry| {
                let name = std::str::from_utf8(entry.to_bytes())
                    .map_err(|_| invalid_state("job registry contains a non-UTF-8 entry"))?;
                let id_text = name
                    .strip_suffix(".json")
                    .ok_or_else(|| invalid_state("job registry contains an unexpected entry"))?;
                let job_id = JobId::from_str(id_text)
                    .map_err(|_| invalid_state("job registry contains an invalid job filename"))?;
                let record = read_job(self.inner.jobs.as_raw_fd(), &entry)?;
                if record.meta().job_id() != job_id {
                    return Err(invalid_state("job filename and record identity differ"));
                }
                self.require_local_client(&record)?;
                Ok(record)
            })
            .collect()
    }

    #[doc(hidden)]
    pub fn inject_write_failure_once(&self, point: ClientStateWritePoint) {
        self.inner.write_fault.store(point as u8, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn pause_cleanup_after_move_once(&self) -> ClientStateCleanupPause {
        let state = Arc::new(CleanupPauseState {
            state: Mutex::new(CleanupPauseFlags::default()),
            changed: Condvar::new(),
        });
        *self
            .inner
            .cleanup_pause
            .lock()
            .expect("cleanup pause mutex poisoned") = Some(Arc::clone(&state));
        ClientStateCleanupPause { inner: state }
    }

    #[doc(hidden)]
    pub fn observe_next_lock_contention(&self) -> ClientStateLockContentionProbe {
        let state = Arc::new(LockContentionState {
            root: FileIdentity::from_stat(
                stat_fd(self.inner.root.as_raw_fd()).expect("anchored state root must be open"),
            ),
            confirmed: Mutex::new(false),
            changed: Condvar::new(),
        });
        LOCK_CONTENTION_PROBES
            .lock()
            .expect("lock-contention probes mutex poisoned")
            .push(Arc::downgrade(&state));
        ClientStateLockContentionProbe { inner: state }
    }

    #[doc(hidden)]
    pub fn durability_sync_counts(&self) -> ClientStateSyncCounts {
        ClientStateSyncCounts {
            parent_directories: self
                .inner
                .sync_counts
                .parent_directories
                .load(Ordering::SeqCst),
            root: self.inner.sync_counts.root.load(Ordering::SeqCst),
            jobs: self.inner.sync_counts.jobs.load(Ordering::SeqCst),
            concurrent_loser_parents: self
                .inner
                .sync_counts
                .concurrent_loser_parents
                .load(Ordering::SeqCst),
        }
    }

    fn require_local_client(&self, record: &LocalJobRecord) -> Result<(), WorkerError> {
        if record.meta().client_id() != self.inner.client_id {
            return Err(WorkerError::Protocol(
                "CLIENT_ID_MISMATCH: local job belongs to another client identity".into(),
            ));
        }
        Ok(())
    }

    fn take_fault(&self, point: ClientStateWritePoint) -> bool {
        self.inner
            .write_fault
            .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn take_cleanup_pause(&self) -> Option<Arc<CleanupPauseState>> {
        self.inner
            .cleanup_pause
            .lock()
            .expect("cleanup pause mutex poisoned")
            .take()
    }

    fn update_locked<F>(&self, job_id: JobId, update: F) -> Result<LocalJobRecord, WorkerError>
    where
        F: FnOnce(&LocalJobRecord) -> Result<Option<LocalJobRecord>, WorkerError>,
    {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let name = job_file_name(job_id)?;
        let (existing, identity) = read_job_with_identity(self.inner.jobs.as_raw_fd(), &name)?;
        if existing.meta().job_id() != job_id {
            return Err(invalid_state("job filename and record identity differ"));
        }
        self.require_local_client(&existing)?;
        let replacement = match update(&existing)? {
            Some(replacement) => replacement,
            None => return Ok(existing),
        };
        replacement.validate()?;
        self.require_local_client(&replacement)?;
        require_same_immutable(&existing, &replacement)?;

        let bytes = canonical_record_bytes(&replacement)?;
        let operation = OperationFile::stage(
            self.inner.operations.as_raw_fd(),
            &bytes,
            Arc::clone(&self.inner.sync_counts),
        )?;
        if self.take_fault(ClientStateWritePoint::BeforePublish) {
            return Err(injected_failure(ClientStateWritePoint::BeforePublish));
        }
        operation.replace_if_identity(
            self.inner.jobs.as_raw_fd(),
            &name,
            identity,
            &self.inner.write_fault,
        )?;
        if self.take_fault(ClientStateWritePoint::AfterPublish) {
            return Err(injected_failure(ClientStateWritePoint::AfterPublish));
        }
        sync_counted(
            self.inner.jobs.as_raw_fd(),
            &self.inner.sync_counts,
            SyncKind::Jobs,
        )?;
        operation.cleanup(
            &self.inner.write_fault,
            &[identity],
            self.take_cleanup_pause(),
        )?;
        Ok(replacement)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProjectAffinityRecord {
    project_id: String,
    worker: String,
    observed_at_millis: u64,
}

impl ProjectAffinityRecord {
    fn validate(&self) -> Result<(), WorkerError> {
        validate_affinity_key(&self.project_id, "project ID")?;
        validate_state_worker_name(&self.worker)?;
        if self.observed_at_millis == 0 {
            return Err(invalid_state("project affinity timestamp is invalid"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct WorktreeAffinityRecord {
    project_id: String,
    worktree_id: String,
    worker: String,
    observed_at_millis: u64,
}

impl WorktreeAffinityRecord {
    fn validate(&self) -> Result<(), WorkerError> {
        validate_affinity_key(&self.project_id, "project ID")?;
        validate_affinity_key(&self.worktree_id, "worktree ID")?;
        validate_state_worker_name(&self.worker)?;
        if self.observed_at_millis == 0 {
            return Err(invalid_state("worktree affinity timestamp is invalid"));
        }
        Ok(())
    }
}

struct QueueLock {
    _state: StateLock,
    marker: OwnedFd,
}

impl QueueLock {
    fn acquire(root: RawFd, queue: RawFd, sync_counts: &SyncCounters) -> Result<Self, WorkerError> {
        let state = StateLock::acquire(root, sync_counts)?;
        let marker = open_regular_at(queue, QUEUE_LOCK_NAME)?;
        require_owned_regular(marker.as_raw_fd(), 0)?;
        cvt(unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX) })?;
        Ok(Self {
            _state: state,
            marker,
        })
    }
}

impl Drop for QueueLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.marker.as_raw_fd(), libc::LOCK_UN) };
    }
}

struct RefreshLock {
    marker: OwnedFd,
}

enum RefreshAcquire {
    Acquired(RefreshLock),
    Busy,
}

impl RefreshLock {
    fn try_acquire(marker: OwnedFd) -> Result<RefreshAcquire, WorkerError> {
        match cvt(unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }) {
            Ok(()) => Ok(RefreshAcquire::Acquired(Self { marker })),
            Err(error) if lock_would_block(&error) => Ok(RefreshAcquire::Busy),
            Err(error) => Err(WorkerError::Io(error)),
        }
    }

    fn acquire(marker: OwnedFd) -> Result<Self, WorkerError> {
        cvt(unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX) })?;
        Ok(Self { marker })
    }
}

impl Drop for RefreshLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.marker.as_raw_fd(), libc::LOCK_UN) };
    }
}

fn open_or_create_queue_lock(queue: RawFd) -> Result<(), WorkerError> {
    match open_regular_at(queue, QUEUE_LOCK_NAME) {
        Ok(descriptor) => {
            require_owned_regular(descriptor.as_raw_fd(), 0)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let descriptor = create_lock_at(queue, QUEUE_LOCK_NAME)?;
            require_owned_regular(descriptor.as_raw_fd(), 0)?;
            sync_directory(queue)
        }
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn load_or_create_queue_snapshot(
    queue: RawFd,
    operations: RawFd,
    fault: &AtomicU8,
    sync_counts: &Arc<SyncCounters>,
) -> Result<QueueSnapshot, WorkerError> {
    let snapshot = match read_regular_optional(queue, QUEUE_STATE_NAME)? {
        Some((bytes, _)) => parse_queue_snapshot(&bytes)?,
        None => {
            let snapshot = QueueSnapshot::empty();
            let bytes = canonical_queue_bytes(&snapshot)?;
            let operation = OperationFile::stage(operations, &bytes, Arc::clone(sync_counts))?;
            operation.publish_no_replace(queue, QUEUE_STATE_NAME, fault)?;
            sync_directory(queue)?;
            operation.cleanup(fault, &[], None)?;
            snapshot
        }
    };
    validate_queue_entries(queue)?;
    Ok(snapshot)
}

fn validate_queue_entries(queue: RawFd) -> Result<(), WorkerError> {
    let mut state_seen = false;
    let mut lock_seen = false;
    for entry in directory_entries(queue)? {
        match entry.to_bytes() {
            b"state.json" if !state_seen => {
                read_queue_snapshot(queue)?;
                state_seen = true;
            }
            b"lock" if !lock_seen => {
                let lock = open_regular_at(queue, &entry)?;
                require_owned_regular(lock.as_raw_fd(), 0)?;
                lock_seen = true;
            }
            _ => return Err(invalid_state("queue contains an unexpected entry")),
        }
    }
    if !state_seen || !lock_seen {
        return Err(invalid_state("queue is incomplete"));
    }
    Ok(())
}

fn read_queue_snapshot(queue: RawFd) -> Result<(QueueSnapshot, FileIdentity), WorkerError> {
    let (bytes, identity) = read_regular(queue, QUEUE_STATE_NAME)?;
    Ok((parse_queue_snapshot(&bytes)?, identity))
}

fn parse_queue_snapshot(bytes: &[u8]) -> Result<QueueSnapshot, WorkerError> {
    parse_canonical_json(bytes, "queue snapshot")
}

fn canonical_queue_bytes(snapshot: &QueueSnapshot) -> Result<Vec<u8>, WorkerError> {
    canonical_json_bytes(snapshot, "queue snapshot")
}

fn require_queue_client(snapshot: &QueueSnapshot, client_id: ClientId) -> Result<(), WorkerError> {
    if snapshot
        .entries()
        .iter()
        .any(|entry| entry.client_id() != client_id)
    {
        return Err(queue_error(
            "QUEUE_CLIENT_MISMATCH",
            "queue contains a row for another client identity",
        ));
    }
    Ok(())
}

fn publish_queue_snapshot(
    store: &ClientStateStore,
    snapshot: &QueueSnapshot,
    expected: FileIdentity,
) -> Result<(), WorkerError> {
    let bytes = canonical_queue_bytes(snapshot)?;
    let operation = OperationFile::stage(
        store.inner.operations.as_raw_fd(),
        &bytes,
        Arc::clone(&store.inner.sync_counts),
    )?;
    if store.take_fault(ClientStateWritePoint::BeforePublish) {
        return Err(injected_failure(ClientStateWritePoint::BeforePublish));
    }
    operation.replace_if_identity(
        store.inner.queue.as_raw_fd(),
        QUEUE_STATE_NAME,
        expected,
        &store.inner.write_fault,
    )?;
    if store.take_fault(ClientStateWritePoint::AfterPublish) {
        return Err(injected_failure(ClientStateWritePoint::AfterPublish));
    }
    sync_directory(store.inner.queue.as_raw_fd())?;
    operation.cleanup(
        &store.inner.write_fault,
        &[expected],
        store.take_cleanup_pause(),
    )
}

fn find_queue_entry_mut(
    snapshot: &mut QueueSnapshot,
    job_id: JobId,
) -> Result<&mut QueueEntry, WorkerError> {
    snapshot
        .entries
        .iter_mut()
        .find(|entry| entry.job_id() == job_id)
        .ok_or_else(|| queue_error("QUEUE_NOT_FOUND", "queue row was not found"))
}

fn canonical_json_bytes<T: serde::Serialize>(
    record: &T,
    _label: &'static str,
) -> Result<Vec<u8>, WorkerError> {
    let mut bytes = serde_json::to_vec(record)
        .map_err(|_| invalid_state("state record cannot be serialized"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_canonical_json<T>(bytes: &[u8], _label: &'static str) -> Result<T, WorkerError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    if bytes.len() < 2 || bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(invalid_state("state record is not canonical JSON"));
    }
    let record = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|_| invalid_state("state record is corrupt"))?;
    if canonical_json_bytes(&record, "state record")? != bytes {
        return Err(invalid_state("state record is not canonical JSON"));
    }
    Ok(record)
}

fn project_affinity_name(project_id: &str) -> Result<CString, WorkerError> {
    CString::new(format!("{project_id}.json"))
        .map_err(|_| invalid_state("project affinity filename is invalid"))
}

fn worktree_affinity_name(project_id: &str, worktree_id: &str) -> Result<CString, WorkerError> {
    CString::new(format!("{project_id}-{worktree_id}.json"))
        .map_err(|_| invalid_state("worktree affinity filename is invalid"))
}

fn read_project_affinity_optional(
    directory: RawFd,
    name: &CStr,
) -> Result<Option<ProjectAffinityRecord>, WorkerError> {
    read_regular_optional(directory, name)?
        .map(|(bytes, _)| {
            let record: ProjectAffinityRecord = parse_canonical_json(&bytes, "project affinity")?;
            record.validate()?;
            Ok(record)
        })
        .transpose()
}

fn read_worktree_affinity_optional(
    directory: RawFd,
    name: &CStr,
) -> Result<Option<WorktreeAffinityRecord>, WorkerError> {
    read_regular_optional(directory, name)?
        .map(|(bytes, _)| {
            let record: WorktreeAffinityRecord = parse_canonical_json(&bytes, "worktree affinity")?;
            record.validate()?;
            Ok(record)
        })
        .transpose()
}

fn publish_affinity_record(
    directory: RawFd,
    name: &CStr,
    bytes: &[u8],
    store: &ClientStateStore,
) -> Result<(), WorkerError> {
    publish_optional_record(directory, name, bytes, store)
}

fn publish_optional_record(
    directory: RawFd,
    name: &CStr,
    bytes: &[u8],
    store: &ClientStateStore,
) -> Result<(), WorkerError> {
    let existing = read_regular_optional(directory, name)?.map(|(_, identity)| identity);
    let operation = OperationFile::stage(
        store.inner.operations.as_raw_fd(),
        bytes,
        Arc::clone(&store.inner.sync_counts),
    )?;
    if store.take_fault(ClientStateWritePoint::BeforePublish) {
        return Err(injected_failure(ClientStateWritePoint::BeforePublish));
    }
    match existing {
        Some(identity) => {
            operation.replace_if_identity(directory, name, identity, &store.inner.write_fault)?
        }
        None => operation.publish_no_replace(directory, name, &store.inner.write_fault)?,
    }
    if store.take_fault(ClientStateWritePoint::AfterPublish) {
        return Err(injected_failure(ClientStateWritePoint::AfterPublish));
    }
    sync_directory(directory)?;
    operation.cleanup(
        &store.inner.write_fault,
        &existing.into_iter().collect::<Vec<_>>(),
        store.take_cleanup_pause(),
    )
}

fn validate_affinity_entries(
    affinity: RawFd,
    projects: RawFd,
    worktrees: RawFd,
) -> Result<(), WorkerError> {
    let mut children = directory_entries(affinity)?;
    children.sort();
    if children
        .iter()
        .map(|name| name.to_bytes())
        .collect::<Vec<_>>()
        != [b"projects".as_slice(), b"worktrees".as_slice()]
    {
        return Err(invalid_state(
            "affinity directory contains an unexpected entry",
        ));
    }
    for entry in directory_entries(projects)? {
        let text = std::str::from_utf8(entry.to_bytes())
            .map_err(|_| invalid_state("project affinity filename is not UTF-8"))?;
        let project_id = text
            .strip_suffix(".json")
            .ok_or_else(|| invalid_state("project affinity filename is invalid"))?;
        validate_affinity_key(project_id, "project ID")?;
        let record = read_project_affinity_optional(projects, &entry)?
            .ok_or_else(|| invalid_state("project affinity record disappeared"))?;
        if record.project_id != project_id {
            return Err(invalid_state("project affinity filename and record differ"));
        }
    }
    for entry in directory_entries(worktrees)? {
        let text = std::str::from_utf8(entry.to_bytes())
            .map_err(|_| invalid_state("worktree affinity filename is not UTF-8"))?;
        let stem = text
            .strip_suffix(".json")
            .ok_or_else(|| invalid_state("worktree affinity filename is invalid"))?;
        let (project_id, worktree_id) = stem
            .split_once('-')
            .ok_or_else(|| invalid_state("worktree affinity filename is invalid"))?;
        validate_affinity_key(project_id, "project ID")?;
        validate_affinity_key(worktree_id, "worktree ID")?;
        let record = read_worktree_affinity_optional(worktrees, &entry)?
            .ok_or_else(|| invalid_state("worktree affinity record disappeared"))?;
        if record.project_id != project_id || record.worktree_id != worktree_id {
            return Err(invalid_state(
                "worktree affinity filename and record differ",
            ));
        }
    }
    Ok(())
}

fn observation_record_name(worker: &str) -> Result<CString, WorkerError> {
    CString::new(format!("{worker}.json"))
        .map_err(|_| invalid_state("observation filename is invalid"))
}

fn observation_marker_name(worker: &str) -> Result<CString, WorkerError> {
    CString::new(format!("{worker}.refresh"))
        .map_err(|_| invalid_state("observation marker filename is invalid"))
}

fn read_observation_optional(
    directory: RawFd,
    worker: &str,
) -> Result<Option<AdmissionObservation>, WorkerError> {
    let name = observation_record_name(worker)?;
    read_regular_optional(directory, &name)?
        .map(|(bytes, _)| {
            let observation: AdmissionObservation =
                parse_canonical_json(&bytes, "admission observation")?;
            if observation.worker_name() != worker {
                return Err(invalid_state(
                    "observation filename and worker identity differ",
                ));
            }
            Ok(observation)
        })
        .transpose()
}

fn publish_observation(
    directory: RawFd,
    worker: &str,
    bytes: &[u8],
    store: &ClientStateStore,
) -> Result<(), WorkerError> {
    read_observation_optional(directory, worker)?;
    publish_optional_record(directory, &observation_record_name(worker)?, bytes, store)
}

fn open_or_create_refresh_marker(
    directory: RawFd,
    worker: &str,
    sync_counts: &SyncCounters,
) -> Result<OwnedFd, WorkerError> {
    let name = observation_marker_name(worker)?;
    match open_regular_at(directory, &name) {
        Ok(marker) => {
            require_owned_regular(marker.as_raw_fd(), 0)?;
            Ok(marker)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match create_lock_at(directory, &name) {
                Ok(marker) => {
                    require_owned_regular(marker.as_raw_fd(), 0)?;
                    sync_counted(directory, sync_counts, SyncKind::Root)?;
                    Ok(marker)
                }
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                    open_refresh_marker(directory, worker)
                }
                Err(error) => Err(WorkerError::Io(error)),
            }
        }
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn open_refresh_marker(directory: RawFd, worker: &str) -> Result<OwnedFd, WorkerError> {
    let marker = open_regular_at(directory, &observation_marker_name(worker)?)?;
    require_owned_regular(marker.as_raw_fd(), 0)?;
    Ok(marker)
}

fn validate_observation_entries(directory: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(directory)? {
        let text = std::str::from_utf8(entry.to_bytes())
            .map_err(|_| invalid_state("observation filename is not UTF-8"))?;
        if let Some(worker) = text.strip_suffix(".json") {
            validate_state_worker_name(worker)?;
            read_observation_optional(directory, worker)?
                .ok_or_else(|| invalid_state("observation record disappeared"))?;
        } else if let Some(worker) = text.strip_suffix(".refresh") {
            validate_state_worker_name(worker)?;
            let marker = open_regular_at(directory, &entry)?;
            require_owned_regular(marker.as_raw_fd(), 0)?;
        } else {
            return Err(invalid_state(
                "observations directory contains an unexpected entry",
            ));
        }
    }
    Ok(())
}

fn validate_affinity_key(value: &str, _label: &'static str) -> Result<(), WorkerError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(queue_error(
            "AFFINITY_INVALID",
            "affinity identity is not canonical",
        ));
    }
    Ok(())
}

fn validate_state_worker_name(value: &str) -> Result<(), WorkerError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
    {
        return Err(queue_error(
            "QUEUE_WORKER_INVALID",
            "queue worker name is invalid",
        ));
    }
    Ok(())
}

fn queue_error(code: &'static str, message: &'static str) -> WorkerError {
    WorkerError::Queue {
        code,
        message: message.into(),
    }
}

fn terminal_unproven() -> WorkerError {
    queue_error(
        "QUEUE_TERMINAL_UNPROVEN",
        "queue row has no durable terminal or abandonment proof",
    )
}

fn abandonment_conflict() -> WorkerError {
    queue_error(
        "QUEUE_ABANDONMENT_CONFLICT",
        "queue abandonment proof does not match the rooted request and dispatch reservation",
    )
}

fn terminal_record_matches(entry: &QueueEntry, record: &LocalJobRecord) -> bool {
    matches!(entry.state(), QueueState::Dispatching { .. })
        && local_record_matches_queue_entry(entry, record)
        && record
            .last_status()
            .is_some_and(|status| status.state().is_terminal())
        && matches!(record.remote_uncertainty(), RemoteUncertainty::None)
}

fn local_record_matches_queue_entry(entry: &QueueEntry, record: &LocalJobRecord) -> bool {
    let meta = record.meta();
    let worker_matches = match entry.state() {
        QueueState::Dispatching {
            selected_worker, ..
        } => meta.worker_name() == selected_worker,
        QueueState::Waiting { .. } => match entry.preference() {
            WorkerPreference::Automatic => true,
            WorkerPreference::Pinned { worker } => meta.worker_name() == worker,
        },
    };
    meta.job_id() == entry.job_id()
        && meta.client_id() == entry.client_id()
        && worker_matches
        && meta.project_id() == entry.project_id()
        && meta.worktree_id() == entry.worktree_id()
        && meta.command_summary() == entry.command_summary()
}

fn require_same_immutable(
    existing: &LocalJobRecord,
    candidate: &LocalJobRecord,
) -> Result<(), WorkerError> {
    if existing.meta() == candidate.meta() && existing.lease_token() == candidate.lease_token() {
        Ok(())
    } else {
        Err(WorkerError::Protocol(
            "JOB_ID_CONFLICT: job ID is already bound to different immutable metadata".into(),
        ))
    }
}

fn require_forward_observation(
    existing: Option<&JobStatus>,
    replacement: Option<&JobStatus>,
) -> Result<(), WorkerError> {
    match (existing, replacement) {
        (None, _) => Ok(()),
        (Some(_), None) => Err(WorkerError::Protocol(
            "local job observation cannot be removed".into(),
        )),
        (Some(previous), Some(next)) if previous == next => Ok(()),
        (Some(previous), Some(next)) => {
            if next.updated_at_millis() < previous.updated_at_millis() {
                return Err(WorkerError::Protocol(
                    "local job observation timestamp moved backwards".into(),
                ));
            }
            if previous.state() == next.state() {
                return previous.transition(next.clone());
            }
            if previous.state().is_terminal() {
                return Err(WorkerError::Protocol(
                    "terminal local job observation cannot be rewritten".into(),
                ));
            }
            if !observation_can_advance(previous.state(), next.state()) {
                return Err(WorkerError::Protocol(
                    "local job observation moved backwards".into(),
                ));
            }
            require_sticky_observed_identity(
                previous.supervisor_identity(),
                next.supervisor_identity(),
                "supervisor",
            )?;
            require_sticky_observed_identity(
                previous.child_identity(),
                next.child_identity(),
                "child",
            )
        }
    }
}

pub(crate) fn authoritative_observation_relation(
    current: &LocalJobRecord,
    remote: &JobStatus,
) -> ObservationRelation {
    let Some(current) = current.last_status() else {
        return ObservationRelation::RemoteAdvances;
    };
    if current == remote || status_is_valid_forward(remote, current) {
        ObservationRelation::CurrentAtLeastRemote
    } else if status_is_valid_forward(current, remote) {
        ObservationRelation::RemoteAdvances
    } else {
        ObservationRelation::Conflict
    }
}

fn status_is_valid_forward(previous: &JobStatus, next: &JobStatus) -> bool {
    require_forward_observation(Some(previous), Some(next)).is_ok()
        || accepted_enrichment_is_transitively_forward(previous, next)
}

fn accepted_enrichment_is_transitively_forward(previous: &JobStatus, observed: &JobStatus) -> bool {
    let Some(supervisor) = observed.supervisor_identity() else {
        return false;
    };
    if observed.child_identity().is_none() {
        return false;
    }
    let Ok(intermediate) = previous.with_supervisor(supervisor, observed.updated_at_millis())
    else {
        return false;
    };
    intermediate.transition(observed.clone()).is_ok()
}

fn require_sticky_observed_identity<T: Copy + PartialEq>(
    previous: Option<T>,
    next: Option<T>,
    label: &str,
) -> Result<(), WorkerError> {
    if previous.is_some_and(|identity| next != Some(identity)) {
        Err(WorkerError::Protocol(format!(
            "local {label} process identity is not sticky"
        )))
    } else {
        Ok(())
    }
}

fn observation_can_advance(previous: JobState, next: JobState) -> bool {
    match previous {
        JobState::Uploading => next != JobState::Uploading,
        JobState::Verified => !matches!(next, JobState::Uploading | JobState::Verified),
        JobState::Accepted => !matches!(
            next,
            JobState::Uploading | JobState::Verified | JobState::Accepted
        ),
        JobState::Running => next.is_terminal(),
        JobState::Succeeded
        | JobState::Failed
        | JobState::Cancelled
        | JobState::TimedOut
        | JobState::Lost => false,
    }
}

fn load_or_create_client_id(
    root: RawFd,
    operations: RawFd,
    fault: &AtomicU8,
    sync_counts: &Arc<SyncCounters>,
) -> Result<ClientId, WorkerError> {
    match read_regular_optional(root, CLIENT_ID_NAME)? {
        Some((bytes, _)) => {
            let client_id = parse_client_id(&bytes)?;
            sync_counted(root, sync_counts, SyncKind::Root)?;
            Ok(client_id)
        }
        None => {
            let candidate = ClientId::generate();
            let bytes = format!("{candidate}\n").into_bytes();
            let operation = OperationFile::stage(operations, &bytes, Arc::clone(sync_counts))?;
            match operation.publish_no_replace(root, CLIENT_ID_NAME, fault) {
                Ok(()) => {
                    sync_counted(root, sync_counts, SyncKind::Root)?;
                    operation.cleanup(fault, &[], None)?;
                    Ok(candidate)
                }
                Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::EEXIST) => {
                    operation.cleanup(fault, &[], None)?;
                    let (winner, _) = read_regular(root, CLIENT_ID_NAME)?;
                    let winner = parse_client_id(&winner)?;
                    sync_counted(root, sync_counts, SyncKind::Root)?;
                    Ok(winner)
                }
                Err(error) => Err(error),
            }
        }
    }
}

fn parse_client_id(bytes: &[u8]) -> Result<ClientId, WorkerError> {
    if bytes.len() != 33 || bytes.last() != Some(&b'\n') {
        return Err(invalid_state("client identity is not canonical"));
    }
    let text = std::str::from_utf8(&bytes[..32])
        .map_err(|_| invalid_state("client identity is not UTF-8"))?;
    ClientId::from_str(text).map_err(|_| invalid_state("client identity is invalid"))
}

fn canonical_record_bytes(record: &LocalJobRecord) -> Result<Vec<u8>, WorkerError> {
    let mut bytes = serde_json::to_vec(record)
        .map_err(|_| invalid_state("local job record cannot be serialized"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_record(bytes: &[u8]) -> Result<LocalJobRecord, WorkerError> {
    if bytes.len() < 2 || bytes.last() != Some(&b'\n') || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Err(invalid_state("local job record is not canonical JSON"));
    }
    let record: LocalJobRecord = serde_json::from_slice(&bytes[..bytes.len() - 1])
        .map_err(|_| invalid_state("local job record is corrupt"))?;
    if canonical_record_bytes(&record)? != bytes {
        return Err(invalid_state("local job record is not canonical JSON"));
    }
    Ok(record)
}

fn read_job(directory: RawFd, name: &CStr) -> Result<LocalJobRecord, WorkerError> {
    read_job_with_identity(directory, name).map(|(record, _)| record)
}

fn read_job_optional(directory: RawFd, name: &CStr) -> Result<Option<LocalJobRecord>, WorkerError> {
    match read_regular_optional(directory, name)? {
        Some((bytes, _)) => parse_record(&bytes).map(Some),
        None => Ok(None),
    }
}

fn read_job_with_identity(
    directory: RawFd,
    name: &CStr,
) -> Result<(LocalJobRecord, FileIdentity), WorkerError> {
    let (bytes, identity) = read_regular(directory, name)?;
    Ok((parse_record(&bytes)?, identity))
}

fn job_file_name(job_id: JobId) -> Result<CString, WorkerError> {
    CString::new(format!("{job_id}.json"))
        .map_err(|_| invalid_state("job ID produced an invalid filename"))
}

struct OperationFile {
    operations: RawFd,
    name: CString,
    directory: OwnedFd,
    directory_identity: FileIdentity,
    payload: OwnedFd,
    payload_identity: FileIdentity,
    expected_bytes: Vec<u8>,
    sync_counts: Arc<SyncCounters>,
}

impl OperationFile {
    fn stage(
        operations: RawFd,
        bytes: &[u8],
        sync_counts: Arc<SyncCounters>,
    ) -> Result<Self, WorkerError> {
        let name = CString::new(Uuid::new_v4().simple().to_string())
            .map_err(|_| invalid_state("operation identity is invalid"))?;
        mkdir_at(operations, &name, 0o700)?;
        sync_counted(operations, &sync_counts, SyncKind::Operations)?;
        let directory = match open_directory_at(operations, &name) {
            Ok(directory) => directory,
            Err(error) => {
                let _ = unlink_at(operations, &name, libc::AT_REMOVEDIR);
                return Err(WorkerError::Io(error));
            }
        };
        require_owned_directory(directory.as_raw_fd())?;
        let directory_identity = FileIdentity::from_stat(stat_fd(directory.as_raw_fd())?);
        let descriptor = create_regular_at(directory.as_raw_fd(), PAYLOAD_NAME)?;
        let mut file = File::from(descriptor);
        file.write_all(bytes)?;
        file.sync_all()?;
        let payload = OwnedFd::from(file);
        let payload_identity = FileIdentity::from_stat(require_owned_regular(
            payload.as_raw_fd(),
            MAX_STATE_FILE_BYTES,
        )?);
        sync_counted(directory.as_raw_fd(), &sync_counts, SyncKind::Operations)?;
        Ok(Self {
            operations,
            name,
            directory,
            directory_identity,
            payload,
            payload_identity,
            expected_bytes: bytes.to_vec(),
            sync_counts,
        })
    }

    fn publish_no_replace(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        fault: &AtomicU8,
    ) -> Result<(), WorkerError> {
        let retained_identity = FileIdentity::from_stat(stat_fd(self.payload.as_raw_fd())?);
        if retained_identity != self.payload_identity {
            return Err(invalid_state("retained staged payload identity changed"));
        }
        if take_fault(
            fault,
            ClientStateWritePoint::SwapOperationPayloadBeforePublish,
        ) {
            self.inject_payload_swap()?;
        }
        link_no_replace(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            destination_parent,
            destination,
        )?;
        let published = open_regular_at(destination_parent, destination)?;
        let (published_bytes, published_identity) = read_open_regular(published)?;
        if published_identity != self.payload_identity
            || published_bytes != self.expected_bytes
            || rollback_fault_is_armed(fault)
        {
            if published_identity == self.payload_identity {
                remove_entry_if_identity(
                    destination_parent,
                    destination,
                    self.payload_identity,
                    fault,
                    &self.sync_counts,
                    self.operations,
                )?;
            }
            return Err(invalid_state(
                "staged payload identity or bytes changed before publication",
            ));
        }
        Ok(())
    }

    fn replace_if_identity(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        expected: FileIdentity,
        fault: &AtomicU8,
    ) -> Result<(), WorkerError> {
        if let Some(point) = take_live_job_swap_fault(fault) {
            inject_live_job_swap(destination_parent, destination, point)?;
        }
        exchange_entries(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            destination_parent,
            destination,
        )?;

        let displaced_stat = stat_at(self.directory.as_raw_fd(), PAYLOAD_NAME).ok();
        if displaced_stat.is_some_and(|stat| {
            self.exchange_state_is_valid(destination_parent, destination, expected, stat)
        }) {
            return Ok(());
        }

        self.rollback_exchange(
            destination_parent,
            destination,
            displaced_stat.map(PathIdentity::from_stat),
        )?;
        Err(invalid_state(
            "job state changed during conditional atomic replacement",
        ))
    }

    fn exchange_state_is_valid(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        expected: FileIdentity,
        displaced_stat: libc::stat,
    ) -> bool {
        if !is_owned_regular_stat(&displaced_stat, MAX_STATE_FILE_BYTES)
            || FileIdentity::from_stat(displaced_stat) != expected
        {
            return false;
        }
        let displaced = match open_regular_at(self.directory.as_raw_fd(), PAYLOAD_NAME) {
            Ok(displaced) => displaced,
            Err(_) => return false,
        };
        let displaced_open =
            match require_owned_regular(displaced.as_raw_fd(), MAX_STATE_FILE_BYTES) {
                Ok(stat) => stat,
                Err(_) => return false,
            };
        if FileIdentity::from_stat(displaced_open) != expected {
            return false;
        }
        let published = match open_regular_at(destination_parent, destination) {
            Ok(published) => published,
            Err(_) => return false,
        };
        match read_open_regular(published) {
            Ok((bytes, identity)) => {
                identity == self.payload_identity && bytes == self.expected_bytes
            }
            Err(_) => false,
        }
    }

    fn rollback_exchange(
        &self,
        destination_parent: RawFd,
        destination: &CStr,
        displaced: Option<PathIdentity>,
    ) -> Result<(), WorkerError> {
        exchange_entries(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            destination_parent,
            destination,
        )
        .map_err(|_| invalid_state("conditional replacement rollback failed"))?;
        sync_counted(destination_parent, &self.sync_counts, SyncKind::Jobs)
            .map_err(|_| invalid_state("conditional replacement rollback fsync failed"))?;
        sync_counted(
            self.directory.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )
        .map_err(|_| invalid_state("conditional replacement rollback fsync failed"))?;
        let restored = stat_at(destination_parent, destination)
            .map(PathIdentity::from_stat)
            .map_err(|_| invalid_state("conditional replacement rollback verification failed"))?;
        if displaced != Some(restored) {
            return Err(invalid_state(
                "job replacement rollback did not restore the displaced entry",
            ));
        }
        Ok(())
    }

    fn cleanup(
        self,
        fault: &AtomicU8,
        extra_owned: &[FileIdentity],
        pause: Option<Arc<CleanupPauseState>>,
    ) -> Result<(), WorkerError> {
        if take_fault(
            fault,
            ClientStateWritePoint::SwapOperationDirectoryBeforeCleanup,
        ) {
            self.inject_directory_swap()?;
        }

        let (namespace_name, namespace) = create_private_directory(
            self.operations,
            "cleanup",
            &self.sync_counts,
            SyncKind::Operations,
        )?;
        if take_fault(
            fault,
            ClientStateWritePoint::CrashCleanupAfterRetirementCreated,
        ) {
            return Err(injected_failure(
                ClientStateWritePoint::CrashCleanupAfterRetirementCreated,
            ));
        }
        let acquired_name = c"operation";
        rename_no_replace(
            self.operations,
            &self.name,
            namespace.as_raw_fd(),
            acquired_name,
        )?;
        sync_counted(
            namespace.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )?;
        sync_counted(self.operations, &self.sync_counts, SyncKind::Operations)?;
        if take_fault(
            fault,
            ClientStateWritePoint::CrashCleanupAfterOperationMoved,
        ) {
            return Err(injected_failure(
                ClientStateWritePoint::CrashCleanupAfterOperationMoved,
            ));
        }
        if let Some(pause) = pause {
            pause_cleanup(pause);
        }
        let acquired = open_directory_at(namespace.as_raw_fd(), acquired_name)?;
        let acquired_identity = FileIdentity::from_stat(stat_fd(acquired.as_raw_fd())?);
        if acquired_identity != self.directory_identity {
            restore_quarantine(
                namespace.as_raw_fd(),
                acquired_name,
                self.operations,
                &self.name,
            )?;
            remove_empty_directory_if_identity(self.operations, &namespace_name, &namespace)?;
            return Err(invalid_state(
                "operation directory changed before identity-bound cleanup",
            ));
        }

        if take_fault(
            fault,
            ClientStateWritePoint::SwapOperationDirectoryAfterValidationBeforeRemoval,
        ) {
            inject_directory_swap_at(
                namespace.as_raw_fd(),
                acquired_name,
                c"post-validation-directory-sentinel",
            )?;
        }

        let retired_name = c"retired";
        mkdir_at(namespace.as_raw_fd(), retired_name, 0o700)?;
        sync_counted(
            namespace.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )?;
        let retired = open_directory_at(namespace.as_raw_fd(), retired_name)?;
        require_owned_directory(retired.as_raw_fd())?;
        if take_fault(
            fault,
            ClientStateWritePoint::CrashCleanupBeforeNestedCleanup,
        ) {
            return Err(injected_failure(
                ClientStateWritePoint::CrashCleanupBeforeNestedCleanup,
            ));
        }
        self.cleanup_owned_payload_links(
            acquired.as_raw_fd(),
            retired.as_raw_fd(),
            extra_owned,
            fault,
        )?;
        if !directory_entries(acquired.as_raw_fd())?.is_empty() {
            return Err(invalid_state(
                "operation directory contains substituted entries",
            ));
        }
        rename_no_replace(
            namespace.as_raw_fd(),
            acquired_name,
            retired.as_raw_fd(),
            acquired_name,
        )?;
        let final_object = open_directory_at(retired.as_raw_fd(), acquired_name)?;
        if FileIdentity::from_stat(stat_fd(final_object.as_raw_fd())?) != self.directory_identity {
            restore_quarantine(
                retired.as_raw_fd(),
                acquired_name,
                namespace.as_raw_fd(),
                acquired_name,
            )?;
            return Err(invalid_state(
                "operation directory changed before private retirement",
            ));
        }
        unlink_at(retired.as_raw_fd(), acquired_name, libc::AT_REMOVEDIR)?;
        sync_counted(retired.as_raw_fd(), &self.sync_counts, SyncKind::Operations)?;
        remove_empty_directory_if_identity(namespace.as_raw_fd(), retired_name, &retired)?;
        remove_empty_directory_if_identity(self.operations, &namespace_name, &namespace)?;
        sync_counted(self.operations, &self.sync_counts, SyncKind::Operations)?;
        Ok(())
    }

    fn cleanup_owned_payload_links(
        &self,
        directory: RawFd,
        retirement: RawFd,
        extra_owned: &[FileIdentity],
        fault: &AtomicU8,
    ) -> Result<(), WorkerError> {
        let mut removed = false;
        for entry in directory_entries(directory)? {
            let descriptor = match open_regular_at(directory, &entry) {
                Ok(descriptor) => descriptor,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(WorkerError::Io(error)),
            };
            let identity = FileIdentity::from_stat(require_owned_regular(
                descriptor.as_raw_fd(),
                MAX_STATE_FILE_BYTES,
            )?);
            if identity == self.payload_identity || extra_owned.contains(&identity) {
                if take_fault(
                    fault,
                    ClientStateWritePoint::SwapOperationChildAfterValidationBeforeRemoval,
                ) {
                    let preserved = random_component();
                    rename_no_replace(directory, &entry, directory, &preserved)?;
                    write_new_file(directory, &entry, b"post-validation-child-substitution\n")?;
                    sync_directory(directory)?;
                }
                let retired_name = entry.clone();
                rename_no_replace(directory, &entry, retirement, &retired_name)?;
                let retired_entry = open_regular_at(retirement, &retired_name)?;
                let retired_identity = FileIdentity::from_stat(require_owned_regular(
                    retired_entry.as_raw_fd(),
                    MAX_STATE_FILE_BYTES,
                )?);
                if retired_identity != identity {
                    restore_quarantine(retirement, &retired_name, directory, &entry)?;
                    return Err(invalid_state(
                        "operation child changed before private retirement",
                    ));
                }
                unlink_at(retirement, &retired_name, 0)?;
                removed = true;
            }
        }
        if removed {
            sync_counted(directory, &self.sync_counts, SyncKind::Operations)?;
        }
        Ok(())
    }

    fn inject_payload_swap(&self) -> Result<(), WorkerError> {
        let original_name = c"payload-original";
        rename_no_replace(
            self.directory.as_raw_fd(),
            PAYLOAD_NAME,
            self.directory.as_raw_fd(),
            original_name,
        )?;
        let replacement = create_regular_at(self.directory.as_raw_fd(), PAYLOAD_NAME)?;
        let mut replacement = File::from(replacement);
        replacement.write_all(b"11111111111111111111111111111111\n")?;
        replacement.sync_all()?;
        sync_counted(
            self.directory.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )
    }

    fn inject_directory_swap(&self) -> Result<(), WorkerError> {
        let original_name = random_component();
        rename_no_replace(self.operations, &self.name, self.operations, &original_name)?;
        mkdir_at(self.operations, &self.name, 0o700)?;
        let replacement = open_directory_at(self.operations, &self.name)?;
        let sentinel = create_regular_at(replacement.as_raw_fd(), c"substitution-sentinel")?;
        File::from(sentinel).sync_all()?;
        sync_counted(
            replacement.as_raw_fd(),
            &self.sync_counts,
            SyncKind::Operations,
        )?;
        sync_counted(self.operations, &self.sync_counts, SyncKind::Operations)
    }
}

struct StateLock {
    authoritative: OwnedFd,
    marker: OwnedFd,
}

impl StateLock {
    fn acquire(root: RawFd, sync_counts: &SyncCounters) -> Result<Self, WorkerError> {
        Self::acquire_inner(root, sync_counts, None)
    }

    fn acquire_with_creation_race(
        root: RawFd,
        sync_counts: &SyncCounters,
        creation_race: &AtomicU8,
    ) -> Result<Self, WorkerError> {
        Self::acquire_inner(root, sync_counts, Some(creation_race))
    }

    fn acquire_inner(
        root: RawFd,
        sync_counts: &SyncCounters,
        creation_race: Option<&AtomicU8>,
    ) -> Result<Self, WorkerError> {
        let authoritative = open_directory_at(root, c".")?;
        let anchored_identity = FileIdentity::from_stat(stat_fd(root)?);
        let lock_identity = FileIdentity::from_stat(stat_fd(authoritative.as_raw_fd())?);
        if anchored_identity != lock_identity {
            return Err(invalid_state(
                "authoritative local state lock identity changed",
            ));
        }
        require_owned_directory(authoritative.as_raw_fd())?;
        match cvt(unsafe { libc::flock(authoritative.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) })
        {
            Ok(()) => {}
            Err(error) if lock_would_block(&error) => {
                notify_lock_contention(root)?;
                cvt(unsafe { libc::flock(authoritative.as_raw_fd(), libc::LOCK_EX) })?;
            }
            Err(error) => return Err(WorkerError::Io(error)),
        }

        let (marker, outcome) = open_lock_file_with_creation(root, creation_race)?;
        if outcome != CreationOutcome::Existing {
            sync_counted(root, sync_counts, SyncKind::Root)?;
        }
        if outcome == CreationOutcome::ConcurrentExisting {
            sync_counts
                .concurrent_loser_parents
                .fetch_add(1, Ordering::SeqCst);
        }
        cvt(unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX) })?;
        Ok(Self {
            authoritative,
            marker,
        })
    }
}

fn lock_would_block(error: &io::Error) -> bool {
    let code = error.raw_os_error();
    code == Some(libc::EWOULDBLOCK) || code == Some(libc::EAGAIN)
}

fn notify_lock_contention(root: RawFd) -> Result<(), WorkerError> {
    let root = FileIdentity::from_stat(stat_fd(root)?);
    let mut probes = LOCK_CONTENTION_PROBES
        .lock()
        .expect("lock-contention probes mutex poisoned");
    probes.retain(|probe| {
        let Some(probe) = probe.upgrade() else {
            return false;
        };
        if probe.root == root {
            *probe
                .confirmed
                .lock()
                .expect("lock-contention probe mutex poisoned") = true;
            probe.changed.notify_all();
        }
        true
    });
    Ok(())
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.marker.as_raw_fd(), libc::LOCK_UN) };
        let _ = unsafe { libc::flock(self.authoritative.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct PathIdentity {
    file: FileIdentity,
    kind: libc::mode_t,
    permissions: libc::mode_t,
    owner: libc::uid_t,
}

impl PathIdentity {
    fn from_stat(stat: libc::stat) -> Self {
        Self {
            file: FileIdentity::from_stat(stat),
            kind: file_type(stat.st_mode),
            permissions: stat.st_mode & 0o777,
            owner: stat.st_uid,
        }
    }
}

impl FileIdentity {
    fn from_stat(stat: libc::stat) -> Self {
        Self {
            device: stat.st_dev,
            inode: stat.st_ino,
        }
    }
}

#[derive(Clone, Copy)]
enum SyncKind {
    ParentDirectory,
    Root,
    Jobs,
    Operations,
}

fn open_or_create_root(
    path: &Path,
    sync_counts: &Arc<SyncCounters>,
    creation_race: &AtomicU8,
) -> Result<OwnedFd, WorkerError> {
    if path.as_os_str().is_empty() {
        return Err(invalid_state("state root path is empty"));
    }
    let mut current = if path.is_absolute() {
        open_directory_path(Path::new("/"))?
    } else {
        open_directory_path(Path::new("."))?
    };
    let mut saw_normal = false;
    for component in path.components() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => name,
            Component::ParentDir | Component::Prefix(_) => {
                return Err(invalid_state("state root path is not normalized"));
            }
        };
        saw_normal = true;
        let name = cstring(name)?;
        let next = match open_directory_at(current.as_raw_fd(), &name) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if take_creation_race(creation_race, ClientStateCreationRacePoint::RootComponent) {
                    mkdir_at(current.as_raw_fd(), &name, 0o700)?;
                }
                let mut created = false;
                let mut concurrent_existing = false;
                match mkdir_at(current.as_raw_fd(), &name, 0o700) {
                    Ok(()) => created = true,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                        concurrent_existing = true;
                    }
                    Err(error) => return Err(WorkerError::Io(error)),
                }
                let directory = open_directory_at(current.as_raw_fd(), &name)?;
                if created || concurrent_existing {
                    sync_counted(current.as_raw_fd(), sync_counts, SyncKind::ParentDirectory)?;
                }
                if concurrent_existing {
                    sync_counts
                        .concurrent_loser_parents
                        .fetch_add(1, Ordering::SeqCst);
                }
                directory
            }
            Err(error) => return Err(WorkerError::Io(error)),
        };
        current = next;
    }
    if !saw_normal {
        return Err(invalid_state("state root must not be a filesystem root"));
    }
    Ok(current)
}

fn open_or_create_owned_directory(
    parent: RawFd,
    name: &CStr,
    sync_counts: &Arc<SyncCounters>,
    sync_kind: SyncKind,
    creation_race: &AtomicU8,
) -> Result<OwnedFd, WorkerError> {
    let directory = match open_directory_at(parent, name) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if take_creation_race(creation_race, ClientStateCreationRacePoint::OwnedDirectory) {
                mkdir_at(parent, name, 0o700)?;
            }
            let mut created = false;
            let mut concurrent_existing = false;
            match mkdir_at(parent, name, 0o700) {
                Ok(()) => created = true,
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                    concurrent_existing = true;
                }
                Err(error) => return Err(WorkerError::Io(error)),
            }
            let directory = open_directory_at(parent, name)?;
            if created || concurrent_existing {
                sync_counted(parent, sync_counts, sync_kind)?;
            }
            if concurrent_existing {
                sync_counts
                    .concurrent_loser_parents
                    .fetch_add(1, Ordering::SeqCst);
            }
            directory
        }
        Err(error) => return Err(WorkerError::Io(error)),
    };
    require_owned_directory(directory.as_raw_fd())?;
    Ok(directory)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CreationOutcome {
    Existing,
    Created,
    ConcurrentExisting,
}

fn open_lock_file_with_creation(
    root: RawFd,
    creation_race: Option<&AtomicU8>,
) -> Result<(OwnedFd, CreationOutcome), WorkerError> {
    let open_existing = || {
        cvt_fd(unsafe {
            libc::openat(
                root,
                LOCK_NAME.as_ptr(),
                libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            )
        })
    };
    let (descriptor, outcome) = match open_existing() {
        Ok(descriptor) => (descriptor, CreationOutcome::Existing),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if creation_race.is_some_and(|race| {
                take_creation_race(race, ClientStateCreationRacePoint::LockFile)
            }) {
                drop(create_lock_file(root)?);
            }
            match cvt_fd(unsafe {
                libc::openat(
                    root,
                    LOCK_NAME.as_ptr(),
                    libc::O_RDWR
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW
                        | libc::O_NONBLOCK,
                    0o600,
                )
            }) {
                Ok(descriptor) => (descriptor, CreationOutcome::Created),
                Err(error) if error.raw_os_error() == Some(libc::EEXIST) => {
                    (open_existing()?, CreationOutcome::ConcurrentExisting)
                }
                Err(error) => return Err(WorkerError::Io(error)),
            }
        }
        Err(error) => return Err(WorkerError::Io(error)),
    };
    require_owned_regular(descriptor.as_raw_fd(), 0)?;
    Ok((descriptor, outcome))
}

fn create_lock_file(root: RawFd) -> io::Result<OwnedFd> {
    cvt_fd(unsafe {
        libc::openat(
            root,
            LOCK_NAME.as_ptr(),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0o600,
        )
    })
}

fn validate_root_entries(root: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(root)? {
        match entry.to_bytes() {
            b"client-id" | b"jobs" | b".mac-worker-state" | b"jobs.lock" | b"queue"
            | b"affinity" | b"observations" => {}
            bytes if std::str::from_utf8(bytes).is_err() => {
                return Err(invalid_state("state root contains a non-UTF-8 entry"));
            }
            _ => return Err(invalid_state("state root contains an unexpected entry")),
        }
    }
    Ok(())
}

fn validate_operation_entries(operations: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(operations)? {
        let text = std::str::from_utf8(entry.to_bytes())
            .map_err(|_| invalid_state("operation state contains a non-UTF-8 entry"))?;
        let directory = open_owned_directory_entry(operations, &entry)?;
        if is_lower_hex_id(text) {
            validate_active_operation(directory.as_raw_fd())?;
        } else if has_operation_namespace_id(text, "cleanup-") {
            validate_cleanup_namespace(directory.as_raw_fd())?;
        } else if has_operation_namespace_id(text, "rollback-") {
            validate_rollback_namespace(directory.as_raw_fd())?;
        } else {
            return Err(invalid_state(
                "operation state contains an unexpected entry",
            ));
        }
    }
    Ok(())
}

fn is_lower_hex_id(text: &str) -> bool {
    text.len() == 32
        && text
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn has_operation_namespace_id(text: &str, prefix: &str) -> bool {
    text.strip_prefix(prefix).is_some_and(is_lower_hex_id)
}

fn open_owned_directory_entry(parent: RawFd, name: &CStr) -> Result<OwnedFd, WorkerError> {
    let before = stat_at(parent, name)?;
    let directory = open_directory_at(parent, name)?;
    let opened = stat_fd(directory.as_raw_fd())?;
    if file_type(before.st_mode) != libc::S_IFDIR
        || FileIdentity::from_stat(before) != FileIdentity::from_stat(opened)
    {
        return Err(invalid_state("operation directory identity changed"));
    }
    require_owned_directory(directory.as_raw_fd())?;
    Ok(directory)
}

fn validate_active_operation(directory: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(directory)? {
        if entry.as_c_str() != PAYLOAD_NAME {
            return Err(invalid_state(
                "active operation contains an unexpected entry",
            ));
        }
        validate_operation_regular(directory, &entry)?;
    }
    Ok(())
}

fn validate_cleanup_namespace(directory: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(directory)? {
        match entry.to_bytes() {
            b"operation" => {
                let operation = open_owned_directory_entry(directory, &entry)?;
                validate_active_operation(operation.as_raw_fd())?;
            }
            b"retired" => {
                let retired = open_owned_directory_entry(directory, &entry)?;
                validate_cleanup_retired(retired.as_raw_fd())?;
            }
            _ => {
                return Err(invalid_state(
                    "cleanup namespace contains an unexpected entry",
                ));
            }
        }
    }
    Ok(())
}

fn validate_cleanup_retired(directory: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(directory)? {
        match entry.to_bytes() {
            b"payload" => validate_operation_regular(directory, &entry)?,
            b"operation" => {
                let operation = open_owned_directory_entry(directory, &entry)?;
                if !directory_entries(operation.as_raw_fd())?.is_empty() {
                    return Err(invalid_state("retired operation directory is not empty"));
                }
            }
            _ => {
                return Err(invalid_state(
                    "cleanup retirement contains an unexpected entry",
                ));
            }
        }
    }
    Ok(())
}

fn validate_rollback_namespace(directory: RawFd) -> Result<(), WorkerError> {
    for entry in directory_entries(directory)? {
        match entry.to_bytes() {
            b"published" => validate_operation_regular(directory, &entry)?,
            b"retired" => {
                let retired = open_owned_directory_entry(directory, &entry)?;
                for child in directory_entries(retired.as_raw_fd())? {
                    if child.to_bytes() != b"published" {
                        return Err(invalid_state(
                            "rollback retirement contains an unexpected entry",
                        ));
                    }
                    validate_operation_regular(retired.as_raw_fd(), &child)?;
                }
            }
            _ => {
                return Err(invalid_state(
                    "rollback namespace contains an unexpected entry",
                ));
            }
        }
    }
    Ok(())
}

fn validate_operation_regular(parent: RawFd, name: &CStr) -> Result<(), WorkerError> {
    let descriptor = open_regular_at(parent, name)?;
    require_owned_regular(descriptor.as_raw_fd(), MAX_STATE_FILE_BYTES)?;
    Ok(())
}

fn require_owned_directory(descriptor: RawFd) -> Result<(), WorkerError> {
    let stat = stat_fd(descriptor)?;
    if file_type(stat.st_mode) != libc::S_IFDIR
        || stat.st_uid != effective_user_id()
        || stat.st_mode & 0o777 != 0o700
    {
        return Err(invalid_state("state directory is not owner-only"));
    }
    Ok(())
}

fn require_same_device(root: RawFd, children: &[RawFd]) -> Result<(), WorkerError> {
    let device = stat_fd(root)?.st_dev;
    for child in children {
        if stat_fd(*child)?.st_dev != device {
            return Err(invalid_state(
                "local state directories must share one filesystem",
            ));
        }
    }
    Ok(())
}

fn require_owned_regular(descriptor: RawFd, max_size: usize) -> Result<libc::stat, WorkerError> {
    let stat = stat_fd(descriptor)?;
    if !is_owned_regular_stat(&stat, max_size) {
        return Err(invalid_state(
            "state file is not an owner-only regular file",
        ));
    }
    Ok(stat)
}

fn is_owned_regular_stat(stat: &libc::stat, max_size: usize) -> bool {
    file_type(stat.st_mode) == libc::S_IFREG
        && stat.st_uid == effective_user_id()
        && stat.st_mode & 0o777 == 0o600
        && stat.st_size >= 0
        && stat.st_size as usize <= max_size
}

fn read_regular_optional(
    directory: RawFd,
    name: &CStr,
) -> Result<Option<(Vec<u8>, FileIdentity)>, WorkerError> {
    match open_regular_at(directory, name) {
        Ok(descriptor) => read_open_regular(descriptor).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn read_regular(directory: RawFd, name: &CStr) -> Result<(Vec<u8>, FileIdentity), WorkerError> {
    let descriptor = open_regular_at(directory, name)?;
    read_open_regular(descriptor)
}

fn read_open_regular(descriptor: OwnedFd) -> Result<(Vec<u8>, FileIdentity), WorkerError> {
    let stat = require_owned_regular(descriptor.as_raw_fd(), MAX_STATE_FILE_BYTES)?;
    let identity = FileIdentity {
        device: stat.st_dev,
        inode: stat.st_ino,
    };
    let expected = stat.st_size as usize;
    let mut file = File::from(descriptor);
    let mut bytes = Vec::with_capacity(expected);
    (&mut file)
        .take((MAX_STATE_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() != expected {
        return Err(invalid_state("state file changed while being read"));
    }
    let after = stat_fd(file.as_raw_fd())?;
    if after.st_dev != identity.device
        || after.st_ino != identity.inode
        || after.st_size != stat.st_size
    {
        return Err(invalid_state("state file changed while being read"));
    }
    Ok((bytes, identity))
}

fn directory_entries(directory: RawFd) -> Result<Vec<CString>, WorkerError> {
    let independent = open_directory_at(directory, c".")?;
    let independent = independent.into_raw_fd();
    let stream = unsafe { libc::fdopendir(independent) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        let _ = unsafe { libc::close(independent) };
        return Err(WorkerError::Io(error));
    }
    let mut entries = Vec::new();
    loop {
        clear_errno();
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let errno = current_errno();
            let close_result = unsafe { libc::closedir(stream) };
            if errno != 0 {
                return Err(WorkerError::Io(io::Error::from_raw_os_error(errno)));
            }
            cvt(close_result)?;
            return Ok(entries);
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            entries.push(name.to_owned());
        }
    }
}

fn open_directory_path(path: &Path) -> Result<OwnedFd, WorkerError> {
    let path = cstring(path.as_os_str())?;
    cvt_fd(unsafe { libc::open(path.as_ptr(), DIRECTORY_FLAGS) }).map_err(WorkerError::Io)
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe { libc::openat(parent, name.as_ptr(), DIRECTORY_FLAGS) })
}

fn open_regular_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe { libc::openat(parent, name.as_ptr(), READ_FILE_FLAGS) })
}

fn create_regular_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0o600,
        )
    })
}

fn create_lock_at(parent: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    cvt_fd(unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
            0o600,
        )
    })
}

fn mkdir_at(parent: RawFd, name: &CStr, mode: libc::mode_t) -> io::Result<()> {
    cvt(unsafe { libc::mkdirat(parent, name.as_ptr(), mode) })
}

fn unlink_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    cvt(unsafe { libc::unlinkat(parent, name.as_ptr(), flags) })
}

fn link_no_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> io::Result<()> {
    cvt(unsafe {
        libc::linkat(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            0,
        )
    })
}

#[cfg(target_vendor = "apple")]
fn rename_no_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> io::Result<()> {
    cvt(unsafe {
        libc::renameatx_np(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            libc::RENAME_EXCL,
        )
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_no_replace(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> io::Result<()> {
    cvt(unsafe {
        libc::renameat2(
            source_parent,
            source.as_ptr(),
            destination_parent,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    })
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn rename_no_replace(
    _source_parent: RawFd,
    _source: &CStr,
    _destination_parent: RawFd,
    _destination: &CStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace rename is unavailable",
    ))
}

#[cfg(target_vendor = "apple")]
fn exchange_entries(
    left_parent: RawFd,
    left: &CStr,
    right_parent: RawFd,
    right: &CStr,
) -> Result<(), WorkerError> {
    cvt(unsafe {
        libc::renameatx_np(
            left_parent,
            left.as_ptr(),
            right_parent,
            right.as_ptr(),
            libc::RENAME_SWAP,
        )
    })
    .map_err(WorkerError::Io)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn exchange_entries(
    left_parent: RawFd,
    left: &CStr,
    right_parent: RawFd,
    right: &CStr,
) -> Result<(), WorkerError> {
    cvt(unsafe {
        libc::renameat2(
            left_parent,
            left.as_ptr(),
            right_parent,
            right.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    })
    .map_err(WorkerError::Io)
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn exchange_entries(
    _left_parent: RawFd,
    _left: &CStr,
    _right_parent: RawFd,
    _right: &CStr,
) -> Result<(), WorkerError> {
    Err(WorkerError::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic entry exchange is unavailable",
    )))
}

fn remove_entry_if_identity(
    parent: RawFd,
    name: &CStr,
    expected: FileIdentity,
    fault: &AtomicU8,
    sync_counts: &Arc<SyncCounters>,
    operations: RawFd,
) -> Result<(), WorkerError> {
    if stat_fd(parent)?.st_dev != stat_fd(operations)?.st_dev {
        return Err(invalid_state(
            "rollback destination and operation state are on different filesystems",
        ));
    }
    let (namespace_name, namespace) =
        create_private_directory(operations, "rollback", sync_counts, SyncKind::Operations)?;
    let acquired_name = c"published";
    rename_no_replace(parent, name, namespace.as_raw_fd(), acquired_name)?;
    sync_directory(parent)?;
    sync_counted(namespace.as_raw_fd(), sync_counts, SyncKind::Operations)?;
    if take_fault(fault, ClientStateWritePoint::CrashRollbackAfterEntryMoved) {
        return Err(injected_failure(
            ClientStateWritePoint::CrashRollbackAfterEntryMoved,
        ));
    }
    let acquired = open_regular_at(namespace.as_raw_fd(), acquired_name)?;
    let identity = FileIdentity::from_stat(require_owned_regular(
        acquired.as_raw_fd(),
        MAX_STATE_FILE_BYTES,
    )?);
    if identity != expected {
        restore_quarantine(namespace.as_raw_fd(), acquired_name, parent, name)?;
        sync_directory(parent)?;
        remove_empty_directory_if_identity(operations, &namespace_name, &namespace)?;
        return Err(invalid_state(
            "published entry changed before identity-bound rollback",
        ));
    }

    if take_fault(
        fault,
        ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval,
    ) {
        let preserved = random_component();
        rename_no_replace(
            namespace.as_raw_fd(),
            acquired_name,
            namespace.as_raw_fd(),
            &preserved,
        )?;
        write_new_file(
            namespace.as_raw_fd(),
            acquired_name,
            b"post-validation-published-substitution\n",
        )?;
        sync_directory(namespace.as_raw_fd())?;
    }

    let retired_name = c"retired";
    mkdir_at(namespace.as_raw_fd(), retired_name, 0o700)?;
    sync_counted(namespace.as_raw_fd(), sync_counts, SyncKind::Operations)?;
    let retired = open_directory_at(namespace.as_raw_fd(), retired_name)?;
    require_owned_directory(retired.as_raw_fd())?;
    if take_fault(
        fault,
        ClientStateWritePoint::CrashRollbackBeforeNestedCleanup,
    ) {
        return Err(injected_failure(
            ClientStateWritePoint::CrashRollbackBeforeNestedCleanup,
        ));
    }
    rename_no_replace(
        namespace.as_raw_fd(),
        acquired_name,
        retired.as_raw_fd(),
        acquired_name,
    )?;
    let final_entry = open_regular_at(retired.as_raw_fd(), acquired_name)?;
    let final_identity = FileIdentity::from_stat(require_owned_regular(
        final_entry.as_raw_fd(),
        MAX_STATE_FILE_BYTES,
    )?);
    if final_identity != expected {
        restore_quarantine(
            retired.as_raw_fd(),
            acquired_name,
            namespace.as_raw_fd(),
            acquired_name,
        )?;
        return Err(invalid_state(
            "published entry changed before private retirement",
        ));
    }
    unlink_at(retired.as_raw_fd(), acquired_name, 0)?;
    sync_directory(retired.as_raw_fd())?;
    remove_empty_directory_if_identity(namespace.as_raw_fd(), retired_name, &retired)?;
    remove_empty_directory_if_identity(operations, &namespace_name, &namespace)?;
    sync_counted(operations, sync_counts, SyncKind::Operations)?;
    sync_directory(parent)
}

fn create_private_directory(
    parent: RawFd,
    kind_name: &str,
    sync_counts: &Arc<SyncCounters>,
    kind: SyncKind,
) -> Result<(CString, OwnedFd), WorkerError> {
    for _ in 0..16 {
        let name = CString::new(format!("{kind_name}-{}", Uuid::new_v4().simple()))
            .expect("operation namespace name contains no NUL");
        match mkdir_at(parent, &name, 0o700) {
            Ok(()) => {
                sync_counted(parent, sync_counts, kind)?;
                let directory = open_directory_at(parent, &name)?;
                require_owned_directory(directory.as_raw_fd())?;
                return Ok((name, directory));
            }
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
            Err(error) => return Err(WorkerError::Io(error)),
        }
    }
    Err(WorkerError::Io(io::Error::from_raw_os_error(libc::EEXIST)))
}

fn remove_empty_directory_if_identity(
    parent: RawFd,
    name: &CStr,
    directory: &OwnedFd,
) -> Result<(), WorkerError> {
    if !directory_entries(directory.as_raw_fd())?.is_empty() {
        return Err(invalid_state("private retirement directory is not empty"));
    }
    let opened = FileIdentity::from_stat(stat_fd(directory.as_raw_fd())?);
    let current = FileIdentity::from_stat(stat_at(parent, name)?);
    if opened != current {
        return Err(invalid_state(
            "private retirement directory identity changed",
        ));
    }
    unlink_at(parent, name, libc::AT_REMOVEDIR)?;
    Ok(())
}

fn inject_directory_swap_at(
    parent: RawFd,
    name: &CStr,
    sentinel_name: &CStr,
) -> Result<(), WorkerError> {
    let preserved = random_component();
    rename_no_replace(parent, name, parent, &preserved)?;
    mkdir_at(parent, name, 0o700)?;
    let replacement = open_directory_at(parent, name)?;
    write_new_file(replacement.as_raw_fd(), sentinel_name, b"sentinel\n")?;
    sync_directory(replacement.as_raw_fd())?;
    sync_directory(parent)
}

fn restore_quarantine(
    source_parent: RawFd,
    source: &CStr,
    destination_parent: RawFd,
    destination: &CStr,
) -> Result<(), WorkerError> {
    rename_no_replace(source_parent, source, destination_parent, destination)
        .map_err(WorkerError::Io)
}

fn take_live_job_swap_fault(fault: &AtomicU8) -> Option<ClientStateWritePoint> {
    let point = match fault.load(Ordering::SeqCst) {
        value if value == ClientStateWritePoint::SwapLiveJobBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobBeforeReplace
        }
        value if value == ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace
        }
        value if value == ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace
        }
        value if value == ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace as u8 => {
            ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace
        }
        value
            if value == ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace as u8 =>
        {
            ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace
        }
        _ => return None,
    };
    take_fault(fault, point).then_some(point)
}

fn inject_live_job_swap(
    parent: RawFd,
    name: &CStr,
    point: ClientStateWritePoint,
) -> Result<(), WorkerError> {
    let original = random_component();
    rename_no_replace(parent, name, parent, &original)?;
    match point {
        ClientStateWritePoint::SwapLiveJobBeforeReplace => {
            write_new_file(parent, name, b"injected-live-replacement\n")?;
        }
        ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace => {
            cvt(unsafe { libc::symlinkat(c"swap-target".as_ptr(), parent, name.as_ptr()) })?;
        }
        ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace => {
            mkdir_at(parent, name, 0o700)?;
            let directory = open_directory_at(parent, name)?;
            write_new_file(directory.as_raw_fd(), c"sentinel", b"sentinel\n")?;
            sync_directory(directory.as_raw_fd())?;
        }
        ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace => {
            cvt(unsafe { libc::mkfifoat(parent, name.as_ptr(), 0o600) })?;
        }
        ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace => {
            let replacement = create_regular_at(parent, name)?;
            cvt(unsafe { libc::fchmod(replacement.as_raw_fd(), 0o644) })?;
            let mut replacement = File::from(replacement);
            replacement.write_all(b"permissive-live-substitution\n")?;
            replacement.sync_all()?;
        }
        _ => return Err(invalid_state("invalid live job swap injection")),
    }
    sync_directory(parent)
}

fn write_new_file(parent: RawFd, name: &CStr, bytes: &[u8]) -> Result<(), WorkerError> {
    let descriptor = create_regular_at(parent, name)?;
    let mut file = File::from(descriptor);
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn random_component() -> CString {
    CString::new(Uuid::new_v4().simple().to_string()).expect("simple UUID contains no NUL")
}

fn stat_fd(descriptor: RawFd) -> Result<libc::stat, WorkerError> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    cvt(unsafe { libc::fstat(descriptor, stat.as_mut_ptr()) }).map_err(WorkerError::Io)?;
    Ok(unsafe { stat.assume_init() })
}

fn stat_at(parent: RawFd, name: &CStr) -> io::Result<libc::stat> {
    let mut stat = MaybeUninit::<libc::stat>::zeroed();
    cvt(unsafe {
        libc::fstatat(
            parent,
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })?;
    Ok(unsafe { stat.assume_init() })
}

fn sync_directory(descriptor: RawFd) -> Result<(), WorkerError> {
    cvt(unsafe { libc::fsync(descriptor) }).map_err(WorkerError::Io)
}

fn sync_counted(
    descriptor: RawFd,
    counts: &SyncCounters,
    kind: SyncKind,
) -> Result<(), WorkerError> {
    sync_directory(descriptor)?;
    let counter = match kind {
        SyncKind::ParentDirectory => &counts.parent_directories,
        SyncKind::Root => &counts.root,
        SyncKind::Jobs => &counts.jobs,
        SyncKind::Operations => &counts.operations,
    };
    counter.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn take_fault(fault: &AtomicU8, point: ClientStateWritePoint) -> bool {
    fault
        .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

fn rollback_fault_is_armed(fault: &AtomicU8) -> bool {
    matches!(
        fault.load(Ordering::SeqCst),
        value if value
            == ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval as u8
            || value == ClientStateWritePoint::CrashRollbackAfterEntryMoved as u8
            || value == ClientStateWritePoint::CrashRollbackBeforeNestedCleanup as u8
    )
}

fn take_creation_race(fault: &AtomicU8, point: ClientStateCreationRacePoint) -> bool {
    fault
        .compare_exchange(point as u8, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

fn file_type(mode: libc::mode_t) -> libc::mode_t {
    mode & libc::S_IFMT
}

fn effective_user_id() -> libc::uid_t {
    unsafe { libc::geteuid() }
}

fn cstring(value: &OsStr) -> Result<CString, WorkerError> {
    CString::new(value.as_bytes()).map_err(|_| invalid_state("state path contains NUL"))
}

fn cvt(result: libc::c_int) -> io::Result<()> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn cvt_fd(result: libc::c_int) -> io::Result<OwnedFd> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(result) })
    }
}

fn invalid_state(message: &'static str) -> WorkerError {
    WorkerError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn injected_failure(point: ClientStateWritePoint) -> WorkerError {
    let label = match point {
        ClientStateWritePoint::BeforePublish => "before publication",
        ClientStateWritePoint::AfterPublish => "after publication",
        ClientStateWritePoint::SwapOperationPayloadBeforePublish => {
            "after staged payload substitution"
        }
        ClientStateWritePoint::SwapOperationDirectoryBeforeCleanup => {
            "after operation directory substitution"
        }
        ClientStateWritePoint::SwapLiveJobBeforeReplace => "after live job substitution",
        ClientStateWritePoint::SwapOperationDirectoryAfterValidationBeforeRemoval => {
            "after validated operation directory substitution"
        }
        ClientStateWritePoint::SwapOperationChildAfterValidationBeforeRemoval => {
            "after validated operation child substitution"
        }
        ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval => {
            "after validated published rollback substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace => {
            "after symlink live job substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace => {
            "after directory live job substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace => {
            "after fifo live job substitution"
        }
        ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace => {
            "after permissive live job substitution"
        }
        ClientStateWritePoint::CrashCleanupAfterRetirementCreated => {
            "after cleanup retirement creation"
        }
        ClientStateWritePoint::CrashCleanupAfterOperationMoved => "after operation retirement move",
        ClientStateWritePoint::CrashCleanupBeforeNestedCleanup => "before nested operation cleanup",
        ClientStateWritePoint::CrashRollbackAfterEntryMoved => "after published rollback move",
        ClientStateWritePoint::CrashRollbackBeforeNestedCleanup => {
            "before nested published rollback cleanup"
        }
    };
    WorkerError::Io(io::Error::other(format!(
        "injected local state failure {label}"
    )))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn clear_errno() {
    unsafe { *libc::__errno_location() = 0 };
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

#[cfg(target_vendor = "apple")]
fn clear_errno() {
    unsafe { *libc::__error() = 0 };
}

#[cfg(target_vendor = "apple")]
fn current_errno() -> libc::c_int {
    unsafe { *libc::__error() }
}
