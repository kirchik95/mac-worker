//! Independent, bounded task-only reads of existing authoritative state.

use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    inputs::RelativePath,
    job::{ClientId, QueueEntryKind, QueueSnapshot, QueueState},
    paths::PathLayout,
    rooted_fs::RootedDir,
    task::{LocalTaskRecord, TaskId, TaskOutcome, TaskState},
};

pub use crate::controller::events::{
    EventRuntime, MAX_DISPATCH_ASSOCIATIONS, MAX_OPAQUE_CURSOR_BYTES as MAX_CONTINUATION_BYTES,
    MAX_STALE_RETRIES as SAME_BINDING_ESTALE_RETRIES,
    MAX_STATE_RECORD_BYTES as MAX_TASK_RECORD_BYTES, REPAIR_MAX_DIRECTORY_ENTRIES,
    REPAIR_MAX_INPUT_BYTES, REPAIR_MAX_LIMIT as REPAIR_MAX_ROWS, REPAIR_WORK_BUDGET,
};
use crate::controller::events::{
    OpaqueCursor, TaskAddressQuery, TaskFacts, TaskFactsBatch, TaskFactsWire,
    TaskProjectionProvider, TaskProjectionReader, TaskRepairPage, TaskRepairQuery,
};

/// Validate the names-only registry before reading any task or queue record.
pub fn sorted_task_ids(names: Vec<Vec<u8>>) -> Result<Vec<TaskId>, WorkerError> {
    if names.len() > REPAIR_MAX_DIRECTORY_ENTRIES {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE: repair unavailable, registry too large"
                .into(),
        ));
    }
    let mut ids = Vec::with_capacity(names.len());
    for name in names {
        if name == b".mac-worker-rooted-fs" || crate::rooted_fs::is_private_replacement_name(&name)
        {
            continue;
        }
        let text = std::str::from_utf8(&name).map_err(|_| invalid_state())?;
        let id_text = text.strip_suffix(".json").ok_or_else(invalid_state)?;
        ids.push(id_text.parse::<TaskId>().map_err(|_| invalid_state())?);
    }
    ids.sort_by_cached_key(TaskId::to_string);
    Ok(ids)
}

fn invalid_state() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_EVENTS_UNAVAILABLE: invalid task state".into())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TaskReadStats {
    pub names_calls: usize,
    pub directory_entries: usize,
    pub name_bytes: usize,
    pub record_reads: usize,
    pub input_bytes: usize,
    pub queue_reads: usize,
    pub association_checks: usize,
    pub names_elapsed: Duration,
    pub work_elapsed: Duration,
}

pub struct TaskReadResult<T> {
    pub value: T,
    pub stats: TaskReadStats,
}

pub struct TaskEventReadStore {
    root: RootedDir,
    tasks: RootedDir,
    queue: RootedDir,
    turns: RootedDir,
    client_id: ClientId,
    runtime: Arc<dyn EventRuntime>,
}

impl TaskProjectionReader for TaskEventReadStore {
    fn addressed(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError> {
        query.validate()?;
        Ok(self
            .addressed_measured(
                &query.task_ids,
                query.include_titles,
                query.proof_after.as_ref().map(OpaqueCursor::as_str),
                deadline,
            )?
            .value)
    }

    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError> {
        let query = query.normalized();
        let mut page = self
            .repair_read(
                query.after.as_ref().map(OpaqueCursor::as_str),
                query.limit,
                deadline,
            )?
            .value;
        page.baseline_after = query.baseline_after;
        Ok(page)
    }
}

pub struct ExistingTaskProjectionProvider {
    paths: PathLayout,
    runtime: Arc<dyn EventRuntime>,
}

impl ExistingTaskProjectionProvider {
    pub fn new(paths: PathLayout, runtime: Arc<dyn EventRuntime>) -> Self {
        Self { paths, runtime }
    }
}

impl TaskProjectionProvider for ExistingTaskProjectionProvider {
    fn open_existing(
        &self,
        deadline: Duration,
    ) -> Result<Arc<dyn TaskProjectionReader>, WorkerError> {
        if self.runtime.cancelled() {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_CANCELLED: cancelled".into(),
            ));
        }
        if self.runtime.now() >= deadline {
            return Err(invalid_state());
        }
        let store = TaskEventReadStore::open_existing(&self.paths, Arc::clone(&self.runtime))?;
        store.check_deadline(deadline)?;
        Ok(Arc::new(store))
    }
}

impl TaskEventReadStore {
    pub fn open_existing(
        paths: &PathLayout,
        runtime: Arc<dyn EventRuntime>,
    ) -> Result<Self, WorkerError> {
        let mut root = RootedDir::open_anchored_absolute(&paths.state)?;
        let metadata = root.root_metadata()?;
        require_private_directory(&metadata)?;
        root.bind_host_device(metadata.st_dev as u64)?;
        let tasks = open_directory(&root, "tasks")?;
        let queue = open_directory(&root, "queue")?;
        let turns = open_directory(&root, "turns")?;
        let client_id = read_client_id(&root)?;
        Ok(Self {
            root,
            tasks,
            queue,
            turns,
            client_id,
            runtime,
        })
    }

    pub fn addressed_measured(
        &self,
        ids: &[TaskId],
        include_titles: bool,
        proof_after: Option<&str>,
        deadline: Duration,
    ) -> Result<TaskReadResult<TaskFactsBatch>, WorkerError> {
        if ids.is_empty()
            || ids.len() > 16
            || ids
                .iter()
                .enumerate()
                .any(|(index, id)| ids[..index].contains(id))
        {
            return Err(WorkerError::Protocol("CONTROLLER_EVENTS_INVALID".into()));
        }
        let token = proof_after.map(decode_token::<ProofToken>).transpose()?;
        let _fence = self.acquire(deadline)?;
        let started = Instant::now();
        let binding = ProofBinding {
            state: self.repair_binding()?,
            queue: directory_identity(&self.queue)?,
            turns: directory_identity(&self.turns)?,
        };
        if token.as_ref().is_some_and(|token| {
            token.version != 1 || token.kind != "proof" || token.binding != binding
        }) {
            return Err(invalid_cursor());
        }
        let mut stats = TaskReadStats::default();
        let (queue, queue_digest) = self.read_queue(deadline, &mut stats)?;
        let mut result = TaskFactsBatch {
            rows: Vec::new(),
            missing: Vec::new(),
            proof_after: None,
            baseline_after: None,
        };
        let mut snapshots = Vec::new();
        for &id in ids {
            self.check_deadline(deadline)?;
            let Some(record) = self.read_task(id, deadline, &mut stats)? else {
                result.missing.push(id);
                continue;
            };
            let facts = record_facts(&record, None, include_titles)?;
            let known_busy = facts.busy == Some(true);
            let namespace = if known_busy {
                None
            } else {
                self.turn_namespace(id, deadline)?
            };
            let generation = namespace.as_ref().map(directory_generation).transpose()?;
            snapshots.push(TaskSnapshot {
                facts,
                known_busy,
                namespace,
                generation,
            });
        }
        let proofs = snapshots
            .iter()
            .map(|snapshot| {
                (
                    &snapshot.facts.task_id,
                    &snapshot.facts.fact_digest,
                    &snapshot.generation,
                )
            })
            .collect::<Vec<_>>();
        let fingerprint = digest(
            &serde_json::to_vec(&(&binding, &queue_digest, ids, proofs))
                .map_err(|_| invalid_state())?,
        );
        let mut progress = match token {
            Some(token) if token.fingerprint == fingerprint => token,
            _ => ProofToken {
                version: 1,
                kind: "proof".into(),
                binding,
                fingerprint,
                next_task: 0,
                next_queue: 0,
                matched: 0,
            },
        };
        let mask = ((1_u32 << snapshots.len()) - 1) as u16;
        if progress.next_task > snapshots.len()
            || progress.next_queue > queue.entries().len()
            || (progress.next_task == snapshots.len() && progress.next_queue != 0)
            || progress.matched & !mask != 0
        {
            return Err(invalid_cursor());
        }
        for (index, snapshot) in snapshots.iter_mut().enumerate() {
            let dispatching = if snapshot.known_busy {
                if index == progress.next_task {
                    progress.next_task += 1;
                    progress.next_queue = 0;
                }
                None
            } else if index < progress.next_task {
                Some(progress.matched & (1 << index) != 0)
            } else if index == progress.next_task {
                let (dispatching, next) = self.scan_associations(
                    snapshot.namespace.as_ref(),
                    &queue,
                    progress.next_queue,
                    deadline,
                    &mut stats,
                )?;
                match dispatching {
                    Some(found) => {
                        if found {
                            progress.matched |= 1 << index;
                        }
                        progress.next_task += 1;
                        progress.next_queue = 0;
                    }
                    None => progress.next_queue = next,
                }
                dispatching
            } else if snapshot.namespace.is_none() {
                Some(false)
            } else {
                None
            };
            snapshot.facts.queue_dispatching = dispatching;
            if !snapshot.known_busy {
                snapshot.facts.busy = dispatching;
                snapshot.facts.quiescent = dispatching.map(|busy| {
                    !busy
                        && matches!(
                            snapshot.facts.state.as_str(),
                            "open" | "closed" | "abandoned" | "lost"
                        )
                });
            }
            self.check_deadline(deadline)?;
            if !snapshot.known_busy {
                let current = self.turn_namespace(snapshot.facts.task_id, deadline)?;
                if current.as_ref().map(directory_generation).transpose()? != snapshot.generation {
                    return Err(invalid_state());
                }
            }
            if serde_json::to_vec(&snapshot.facts)
                .map_err(|_| invalid_state())?
                .len()
                > 2048
            {
                return Err(invalid_state());
            }
            result.rows.push(snapshot.facts.clone());
        }
        if progress.next_task < snapshots.len() {
            result.proof_after = Some(OpaqueCursor::parse(encode_token(&progress)?)?);
        }
        self.verify_bindings()?;
        self.check_deadline(deadline)?;
        stats.work_elapsed = started.elapsed();
        Ok(TaskReadResult {
            value: result,
            stats,
        })
    }

    pub fn repair_read(
        &self,
        after: Option<&str>,
        limit: usize,
        deadline: Duration,
    ) -> Result<TaskReadResult<TaskRepairPage>, WorkerError> {
        let (value, stats) = self.repair_measured(after, limit, deadline);
        Ok(TaskReadResult {
            value: value?,
            stats,
        })
    }

    /// Measurements are returned even on admission failure, before record/fact work.
    pub fn repair_measured(
        &self,
        after: Option<&str>,
        limit: usize,
        deadline: Duration,
    ) -> (Result<TaskRepairPage, WorkerError>, TaskReadStats) {
        let mut stats = TaskReadStats::default();
        let value = self.repair_inner(after, limit.clamp(1, REPAIR_MAX_ROWS), deadline, &mut stats);
        (value, stats)
    }

    fn retry_same_binding<T>(
        &self,
        deadline: Duration,
        mut operation: impl FnMut() -> Result<T, WorkerError>,
    ) -> Result<T, WorkerError> {
        for attempt in 0..=SAME_BINDING_ESTALE_RETRIES {
            self.check_deadline(deadline)?;
            match operation() {
                Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::ESTALE) => {
                    self.check_deadline(deadline)?;
                    self.verify_bindings()?;
                    if attempt == SAME_BINDING_ESTALE_RETRIES {
                        return Err(invalid_state());
                    }
                }
                result => return result,
            }
        }
        unreachable!("the last ESTALE attempt returns unavailable")
    }

    fn repair_inner(
        &self,
        after: Option<&str>,
        limit: usize,
        deadline: Duration,
        stats: &mut TaskReadStats,
    ) -> Result<TaskRepairPage, WorkerError> {
        let token = after.map(decode_token::<RepairToken>).transpose()?;
        let _fence = self.acquire(deadline)?;
        let binding = self.repair_binding()?;
        if token.as_ref().is_some_and(|token| {
            token.version != 1 || token.kind != "repair" || token.binding != binding
        }) {
            return Err(invalid_cursor());
        }
        self.check_deadline(deadline)?;
        let names_started = Instant::now();
        stats.names_calls += 1;
        let names = self.retry_same_binding(deadline, || Ok(self.tasks.list_names()?))?;
        stats.directory_entries = names.len();
        stats.name_bytes = names.iter().map(Vec::len).sum();
        self.check_deadline(deadline)?;
        let ids = sorted_task_ids(names);
        stats.names_elapsed = names_started.elapsed();
        let ids = ids?;
        self.check_deadline(deadline)?;
        let after_text = token.map(|token| token.after_task_id.to_string());
        let start = after_text.as_ref().map_or(0, |after| {
            ids.partition_point(|id| id.to_string() <= *after)
        });
        if start == ids.len() {
            return Ok(TaskRepairPage {
                rows: Vec::new(),
                next: None,
                complete: true,
                restart: false,
                baseline_after: None,
            });
        }
        let work_started = Instant::now();
        let result = self.repair_rows(&ids[start..], binding, limit, deadline, stats);
        stats.work_elapsed = work_started.elapsed();
        result
    }

    fn repair_rows(
        &self,
        ids: &[TaskId],
        binding: RepairBinding,
        limit: usize,
        deadline: Duration,
        stats: &mut TaskReadStats,
    ) -> Result<TaskRepairPage, WorkerError> {
        let work_started = self.runtime.now();
        let (queue, _) = self.read_queue(deadline, stats)?;
        let mut rows = Vec::new();
        let mut processed = 0;
        for &id in ids {
            if processed >= limit
                || (processed > 0
                    && (self.runtime.now().saturating_sub(work_started) >= REPAIR_WORK_BUDGET
                        || stats.input_bytes > REPAIR_MAX_INPUT_BYTES - MAX_TASK_RECORD_BYTES))
            {
                break;
            }
            self.check_deadline(deadline)?;
            if let Some(record) = self.read_task(id, deadline, stats)? {
                let dispatching =
                    if crate::client_state::task_operator_busy_reason(&record, None).is_some() {
                        None
                    } else {
                        self.dispatching(id, &queue, deadline, stats)?
                    };
                rows.push(record_facts(&record, dispatching, false)?);
            }
            processed += 1;
        }
        self.verify_bindings()?;
        self.check_deadline(deadline)?;
        let complete = processed == ids.len();
        let next = if complete {
            None
        } else {
            Some(OpaqueCursor::parse(encode_token(&RepairToken {
                version: 1,
                kind: "repair".into(),
                binding,
                after_task_id: ids[processed - 1],
            })?)?)
        };
        Ok(TaskRepairPage {
            rows,
            next,
            complete,
            restart: false,
            baseline_after: None,
        })
    }

    fn repair_binding(&self) -> Result<RepairBinding, WorkerError> {
        Ok(RepairBinding {
            client_id: self.client_id,
            root: directory_identity(&self.root)?,
            tasks: directory_identity(&self.tasks)?,
        })
    }

    fn check_deadline(&self, deadline: Duration) -> Result<(), WorkerError> {
        if self.runtime.cancelled() {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_CANCELLED: cancelled".into(),
            ));
        }
        if self.runtime.now() >= deadline {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_UNAVAILABLE: deadline expired".into(),
            ));
        }
        Ok(())
    }

    fn verify_bindings(&self) -> Result<(), WorkerError> {
        for directory in [&self.root, &self.tasks, &self.queue, &self.turns] {
            directory.verify_bound()?;
            require_private_directory(&directory.root_metadata()?)?;
        }
        if read_client_id(&self.root)? != self.client_id {
            return Err(invalid_state());
        }
        Ok(())
    }

    fn acquire(&self, deadline: Duration) -> Result<ReadFence, WorkerError> {
        self.check_deadline(deadline)?;
        self.verify_bindings()?;
        let directory = self.root.reopen()?;
        loop {
            self.check_deadline(deadline)?;
            let result =
                unsafe { libc::flock(directory.raw_directory_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result == 0 {
                let fence = ReadFence { directory };
                self.verify_bindings()?;
                self.check_deadline(deadline)?;
                return Ok(fence);
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::EWOULDBLOCK) | Some(libc::EINTR)
            ) {
                return Err(error.into());
            }
            self.runtime
                .sleep(Duration::from_millis(10).min(deadline.saturating_sub(self.runtime.now())));
        }
    }

    fn read_task(
        &self,
        id: TaskId,
        deadline: Duration,
        stats: &mut TaskReadStats,
    ) -> Result<Option<LocalTaskRecord>, WorkerError> {
        stats.record_reads += 1;
        let bytes = match self.retry_same_binding(deadline, || {
            Ok(self
                .tasks
                .read_private_regular(&format!("{id}.json"), MAX_TASK_RECORD_BYTES as u64)?)
        }) {
            Ok(bytes) => bytes,
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        stats.input_bytes += bytes.len();
        let record: LocalTaskRecord =
            serde_json::from_slice(&bytes).map_err(|_| invalid_state())?;
        if record.meta().task_id() != id
            || record.canonical_bytes().map_err(|_| invalid_state())? != bytes
        {
            return Err(invalid_state());
        }
        Ok(Some(record))
    }

    fn read_queue(
        &self,
        deadline: Duration,
        stats: &mut TaskReadStats,
    ) -> Result<(QueueSnapshot, String), WorkerError> {
        stats.queue_reads += 1;
        let bytes = self.retry_same_binding(deadline, || {
            Ok(self
                .queue
                .read_private_regular("state.json", MAX_TASK_RECORD_BYTES as u64)?)
        })?;
        let snapshot: QueueSnapshot =
            serde_json::from_slice(&bytes).map_err(|_| invalid_state())?;
        let mut canonical = serde_json::to_vec(&snapshot).map_err(|_| invalid_state())?;
        canonical.push(b'\n');
        if canonical != bytes
            || snapshot
                .entries()
                .iter()
                .any(|entry| entry.client_id() != self.client_id)
        {
            return Err(invalid_state());
        }
        Ok((snapshot, digest(&bytes)))
    }

    fn dispatching(
        &self,
        task_id: TaskId,
        queue: &QueueSnapshot,
        deadline: Duration,
        stats: &mut TaskReadStats,
    ) -> Result<Option<bool>, WorkerError> {
        let namespace = self.turn_namespace(task_id, deadline)?;
        Ok(self
            .scan_associations(namespace.as_ref(), queue, 0, deadline, stats)?
            .0)
    }

    fn turn_namespace(
        &self,
        task_id: TaskId,
        deadline: Duration,
    ) -> Result<Option<RootedDir>, WorkerError> {
        Ok(
            match self.retry_same_binding(deadline, || {
                open_directory(&self.turns, &task_id.to_string())
            }) {
                Ok(directory) => Some(directory),
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            },
        )
    }

    fn scan_associations(
        &self,
        namespace: Option<&RootedDir>,
        queue: &QueueSnapshot,
        after: usize,
        deadline: Duration,
        stats: &mut TaskReadStats,
    ) -> Result<(Option<bool>, usize), WorkerError> {
        let Some(task_turns) = namespace else {
            return Ok((Some(false), 0));
        };
        for (index, entry) in queue.entries().iter().enumerate().skip(after) {
            if entry.kind() != QueueEntryKind::TaskTurn
                || !matches!(entry.state(), QueueState::Dispatching { .. })
            {
                continue;
            }
            if stats.association_checks == MAX_DISPATCH_ASSOCIATIONS {
                return Ok((None, index));
            }
            self.check_deadline(deadline)?;
            let found = self.retry_same_binding(deadline, || {
                if stats.association_checks == MAX_DISPATCH_ASSOCIATIONS {
                    return Ok(None);
                }
                stats.association_checks += 1;
                match open_directory(task_turns, &entry.job_id().to_string()) {
                    Ok(_) => Ok(Some(true)),
                    Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                        Ok(Some(false))
                    }
                    Err(error) => Err(error),
                }
            })?;
            match found {
                Some(true) => return Ok((Some(true), 0)),
                None => return Ok((None, index)),
                Some(false) => {}
            }
        }
        task_turns.verify_bound()?;
        Ok((Some(false), 0))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairBinding {
    client_id: ClientId,
    root: DirectoryIdentity,
    tasks: DirectoryIdentity,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RepairToken {
    version: u8,
    kind: String,
    binding: RepairBinding,
    after_task_id: TaskId,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ProofBinding {
    state: RepairBinding,
    queue: DirectoryIdentity,
    turns: DirectoryIdentity,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProofToken {
    version: u8,
    kind: String,
    binding: ProofBinding,
    fingerprint: String,
    next_task: usize,
    next_queue: usize,
    matched: u16,
}

struct TaskSnapshot {
    facts: TaskFacts,
    known_busy: bool,
    namespace: Option<RootedDir>,
    generation: Option<DirectoryGeneration>,
}

#[derive(Serialize, PartialEq, Eq)]
struct DirectoryGeneration {
    identity: DirectoryIdentity,
    modified: (libc::time_t, libc::c_long),
    changed: (libc::time_t, libc::c_long),
    links: libc::nlink_t,
    bytes: libc::off_t,
}

fn directory_generation(directory: &RootedDir) -> Result<DirectoryGeneration, WorkerError> {
    let metadata = directory.root_metadata()?;
    Ok(DirectoryGeneration {
        identity: DirectoryIdentity {
            device: metadata.st_dev as u64,
            inode: metadata.st_ino,
        },
        modified: (metadata.st_mtime, metadata.st_mtime_nsec),
        changed: (metadata.st_ctime, metadata.st_ctime_nsec),
        links: metadata.st_nlink,
        bytes: metadata.st_size,
    })
}

fn directory_identity(directory: &RootedDir) -> Result<DirectoryIdentity, WorkerError> {
    let metadata = directory.root_metadata()?;
    Ok(DirectoryIdentity {
        device: metadata.st_dev as u64,
        inode: metadata.st_ino,
    })
}

fn encode_token(value: &impl Serialize) -> Result<String, WorkerError> {
    let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).map_err(|_| invalid_cursor())?);
    if encoded.len() > MAX_CONTINUATION_BYTES {
        return Err(invalid_cursor());
    }
    Ok(encoded)
}

fn decode_token<T: DeserializeOwned>(encoded: &str) -> Result<T, WorkerError> {
    if encoded.is_empty() || encoded.len() > MAX_CONTINUATION_BYTES {
        return Err(invalid_cursor());
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| invalid_cursor())?;
    if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(invalid_cursor());
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid_cursor())
}

fn invalid_cursor() -> WorkerError {
    WorkerError::Protocol("CONTROLLER_EVENTS_INVALID_CURSOR".into())
}

pub fn record_facts(
    record: &LocalTaskRecord,
    dispatching: Option<bool>,
    include_titles: bool,
) -> Result<TaskFacts, WorkerError> {
    let busy = if crate::client_state::task_operator_busy_reason(record, None).is_some() {
        Some(true)
    } else {
        dispatching
    };
    let quiescent = busy.map(|busy| {
        !busy
            && matches!(
                record.status().state(),
                TaskState::Open | TaskState::Closed | TaskState::Abandoned | TaskState::Lost
            )
    });
    let turn = record.status().turns().last();
    let outcome = turn.and_then(|turn| turn.outcome());
    let code = match outcome {
        Some(TaskOutcome::Failed { reason }) => Some(
            crate::controller::events::SafeCode::from_public_code(reason)
                .as_str()
                .into(),
        ),
        _ => record.abandon_code().map(|code| {
            crate::controller::events::SafeCode::from_public_code(code)
                .as_str()
                .into()
        }),
    };
    let state = match record.status().state() {
        TaskState::Queued => "queued",
        TaskState::Active => "active",
        TaskState::Open => "open",
        TaskState::Closed => "closed",
        TaskState::Abandoned => "abandoned",
        TaskState::Lost => "lost",
    };
    let facts = TaskFacts::try_new(TaskFactsWire {
        task_id: record.meta().task_id(),
        run_id: record.meta().run_id(),
        state: state.into(),
        latest_turn_id: turn.map(|turn| turn.turn_id()),
        outcome: outcome.map(|outcome| outcome.kind().into()),
        code,
        runner_present: record.runner().is_some(),
        close_intent: record.close_intent().is_some(),
        auto_continue_intent: record.auto_continue_intent().is_some(),
        queue_dispatching: dispatching,
        result_imported: record.fetched_head().is_some(),
        busy,
        quiescent,
        fact_digest: digest(&record.canonical_bytes().map_err(|_| invalid_state())?),
        title: include_titles.then(|| {
            crate::redaction::RedactionBoundary::from_env().title(record.meta().title().as_str())
        }),
    })?;
    if serde_json::to_vec(&facts)
        .map_err(|_| invalid_state())?
        .len()
        > 2048
    {
        return Err(invalid_state());
    }
    Ok(facts)
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_client_id(root: &RootedDir) -> Result<ClientId, WorkerError> {
    let bytes = root.read_private_regular("client-id", 33)?;
    if bytes.len() != 33 || bytes[32] != b'\n' {
        return Err(invalid_state());
    }
    std::str::from_utf8(&bytes[..32])
        .map_err(|_| invalid_state())?
        .parse()
        .map_err(|_| invalid_state())
}

fn open_directory(parent: &RootedDir, name: &str) -> Result<RootedDir, WorkerError> {
    let path = RelativePath::parse(name.as_bytes()).map_err(|_| invalid_state())?;
    let child = parent.open_child_directory(&path, false)?;
    require_private_directory(&child.root_metadata()?)?;
    Ok(child)
}

fn require_private_directory(metadata: &libc::stat) -> Result<(), WorkerError> {
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o777 != 0o700 {
        return Err(invalid_state());
    }
    Ok(())
}

struct ReadFence {
    directory: RootedDir,
}

impl Drop for ReadFence {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.directory.raw_directory_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
    };

    use crate::{
        agent::{AgentKind, PermissionPolicy},
        client_state::ClientStateStore,
        job::ProcessIdentity,
        task::{
            ClosePolicy, GitIdentity, PublishMode, QuestionsPolicy, RunnerIdentity,
            TaskCloseIntent, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource,
            TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
        },
    };

    use super::*;
    use crate::controller::events::SafeOutcome;

    #[derive(Default)]
    struct ManualRuntime {
        millis: AtomicU64,
        step: AtomicU64,
        cancelled: AtomicBool,
    }

    impl EventRuntime for ManualRuntime {
        fn now(&self) -> Duration {
            Duration::from_millis(
                self.millis
                    .fetch_add(self.step.load(Ordering::SeqCst), Ordering::SeqCst),
            )
        }

        fn sleep(&self, duration: Duration) {
            self.millis
                .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        }

        fn cancelled(&self) -> bool {
            self.cancelled.load(Ordering::SeqCst)
        }
    }

    fn fixture() -> (tempfile::TempDir, PathLayout, Arc<ManualRuntime>) {
        let root = tempfile::tempdir().unwrap();
        let physical = root.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: physical.join("config.toml"),
            state: physical.join("state"),
            cache: physical.join("cache"),
            data: physical.join("data"),
        };
        ClientStateStore::open(&paths.state).unwrap();
        (root, paths, Arc::new(ManualRuntime::default()))
    }

    fn record(number: u128, state: TaskState, outcome: TaskOutcome) -> LocalTaskRecord {
        let meta = TaskMeta::new(TaskMetaInput {
            task_id: id(number),
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: false,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: "a".repeat(40).parse().unwrap(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: Some("fixture title".into()),
            prompt: "private fixture prompt".into(),
            created_at_millis: 1,
        })
        .unwrap();
        let status = TaskStatus::new(
            state,
            Some(outcome.clone()),
            Some("mini-1".into()),
            false,
            Some(meta.base_oid().clone()),
            Some("private fixture summary".into()),
            vec![],
            vec![],
            None,
            vec![TurnSummary::new(
                1,
                TurnId::new(uuid::Uuid::from_u128(number + 1_000_000)),
                Some(TurnTerminal::Succeeded),
                Some(outcome),
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
        .unwrap();
        LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            None,
            "c".repeat(64),
            None,
            true,
            None,
        )
        .unwrap()
    }

    fn write_record(paths: &PathLayout, record: &LocalTaskRecord) {
        let file = paths
            .state
            .join("tasks")
            .join(format!("{}.json", record.meta().task_id()));
        fs::write(&file, record.canonical_bytes().unwrap()).unwrap();
        fs::set_permissions(file, fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn id(number: u128) -> TaskId {
        TaskId::new(uuid::Uuid::from_u128(number))
    }

    fn name(number: u128) -> Vec<u8> {
        format!("{}.json", id(number)).into_bytes()
    }

    #[test]
    fn repair_names_are_sorted_keys_and_residue_is_excluded() {
        let names = vec![
            name(3),
            b".mac-worker-rooted-fs".to_vec(),
            name(1),
            b"replace-00000000-0000-0000-0000-000000000001".to_vec(),
            name(2),
        ];
        assert_eq!(sorted_task_ids(names).unwrap(), vec![id(1), id(2), id(3)]);
    }

    #[test]
    fn registry_over_cap_is_rejected_before_name_validation() {
        let error = sorted_task_ids(vec![b"invalid".to_vec(); 100_001]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE")
        );
    }

    #[test]
    fn residue_counts_toward_cap_but_at_cap_is_admitted() {
        let names = vec![b".mac-worker-rooted-fs".to_vec(); 100_000];
        assert!(sorted_task_ids(names.clone()).unwrap().is_empty());
        let mut over = names;
        over.push(name(1));
        assert!(sorted_task_ids(over).is_err());
    }

    #[test]
    fn unsafe_registry_names_are_rejected() {
        for invalid in [
            b"not-a-task.json".as_slice(),
            b"00000000-0000-0000-0000-000000000001.json",
            b"0000000000000000000000000000000A.json",
            b"00000000000000000000000000000001",
            b"replace-invalid",
            b"\xff.json",
        ] {
            assert!(
                sorted_task_ids(vec![invalid.to_vec()]).is_err(),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn addressed_hidden_auto_continue_runner_none_is_busy() {
        let task = record(1, TaskState::Open, TaskOutcome::NeedsInput)
            .with_questions_policy(QuestionsPolicy::Decide);
        let intent = crate::prepared_followup::PreparedFollowup::automatic(&task)
            .unwrap()
            .unwrap();
        let task = task.with_auto_continue_intent(Some(intent)).unwrap();
        let facts = record_facts(&task, Some(false), false).unwrap();
        assert!(!facts.runner_present);
        assert!(facts.auto_continue_intent);
        assert_eq!(facts.busy, Some(true));
        assert_eq!(facts.quiescent, Some(false));
    }

    #[test]
    fn close_intent_and_incomplete_dispatch_proof_prevent_quiescence() {
        let task = record(1, TaskState::Open, TaskOutcome::Done);
        let pending = task
            .with_close_intent(TaskCloseIntent::from_record(&task, false).unwrap())
            .unwrap();
        assert_eq!(
            record_facts(&pending, Some(false), false).unwrap().busy,
            Some(true)
        );
        assert_eq!(record_facts(&task, None, false).unwrap().quiescent, None);
        assert_eq!(
            record_facts(&task, Some(true), false).unwrap().quiescent,
            Some(false)
        );
    }

    #[test]
    fn task_facts_exclude_prose_and_titles_unless_requested() {
        let task = record(
            1,
            TaskState::Open,
            TaskOutcome::Failed {
                reason: "private failure prose".into(),
            },
        );
        let facts = record_facts(&task, Some(false), false).unwrap();
        assert_eq!(facts.outcome, Some(SafeOutcome::Failed));
        assert_eq!(
            facts.code.as_ref().map(|code| code.as_str()),
            Some("TURN_FAILED")
        );
        assert_eq!(facts.title, None);
        let encoded = serde_json::to_string(&facts).unwrap();
        assert!(!encoded.contains("private"));
        assert!(!encoded.contains("fixture title"));
        assert!(encoded.len() <= 2048);
        assert_eq!(
            record_facts(&task, Some(false), true)
                .unwrap()
                .title
                .as_deref(),
            Some("fixture title")
        );
    }

    fn private_directory(path: &std::path::Path) {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn dispatching_queue(paths: &PathLayout, count: usize) -> Vec<TurnId> {
        use crate::{
            job::{CommandSummary, QueueEntry, QueueId},
            scheduler::WorkerPreference,
        };

        let client_id: ClientId = fs::read_to_string(paths.state.join("client-id"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let owner = ProcessIdentity::new(2_000_000_001, 1).unwrap();
        let mut entries = Vec::new();
        let mut jobs = Vec::new();
        for index in 0..count {
            let job_id = TurnId::new(uuid::Uuid::from_u128(100_000 + index as u128));
            let mut entry = QueueEntry::new(
                job_id,
                client_id,
                "a".repeat(64),
                "b".repeat(64),
                CommandSummary::argv(1).unwrap(),
                vec![],
                WorkerPreference::Automatic,
                QueueEntryKind::TaskTurn,
                None,
                owner,
                1,
            )
            .unwrap();
            entry.assign_queue_id(QueueId::new(index as u64 + 1).unwrap());
            entry.dispatch(owner, "mini-1".into(), 2).unwrap();
            entries.push(entry);
            jobs.push(job_id);
        }
        let snapshot = QueueSnapshot {
            next_id: QueueId::new(count as u64 + 1).unwrap(),
            entries,
        };
        let mut bytes = serde_json::to_vec(&snapshot).unwrap();
        bytes.push(b'\n');
        fs::write(paths.state.join("queue/state.json"), bytes).unwrap();
        jobs
    }

    #[test]
    fn dispatch_proof_resumes_without_repeating_completed_associations() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::Done));
        private_directory(&paths.state.join("turns").join(id(1).to_string()));
        dispatching_queue(&paths, 70);
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let first = reader
            .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
            .unwrap();
        assert_eq!(first.stats.association_checks, 32);
        assert_eq!(first.value.rows[0].quiescent, None);
        let cursor = first.value.proof_after.unwrap();
        assert!(cursor.as_str().len() <= 2048);
        let second = reader
            .addressed_measured(
                &[id(1)],
                false,
                Some(cursor.as_str()),
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(second.stats.association_checks, 32);
        assert_eq!(second.value.rows[0].busy, None);
        let final_page = reader
            .addressed_measured(
                &[id(1)],
                false,
                second.value.proof_after.as_ref().map(OpaqueCursor::as_str),
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(final_page.stats.association_checks, 6);
        assert_eq!(final_page.value.rows[0].quiescent, Some(true));
        assert!(final_page.value.proof_after.is_none());
    }

    #[test]
    fn namespace_insertion_behind_proof_cursor_restarts_dispatch_checks() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::Done));
        let turns = paths.state.join("turns").join(id(1).to_string());
        private_directory(&turns);
        let jobs = dispatching_queue(&paths, 70);
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let first = reader
            .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
            .unwrap()
            .value;
        assert!(first.proof_after.is_some());
        private_directory(&turns.join(jobs[0].to_string()));
        // Prompt contents are never inspected to establish this association.
        std::os::unix::fs::symlink("missing", turns.join(jobs[0].to_string()).join("prompt"))
            .unwrap();
        let updated = reader
            .addressed_measured(
                &[id(1)],
                false,
                first.proof_after.as_ref().map(OpaqueCursor::as_str),
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(updated.stats.association_checks, 1);
        assert_eq!(updated.value.rows[0].queue_dispatching, Some(true));
        assert_eq!(updated.value.rows[0].busy, Some(true));
        assert_eq!(updated.value.rows[0].quiescent, Some(false));
    }

    #[test]
    fn task_and_queue_digest_changes_restart_dispatch_proof() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::Done));
        private_directory(&paths.state.join("turns").join(id(1).to_string()));
        dispatching_queue(&paths, 70);
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let first = reader
            .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
            .unwrap()
            .value;
        let second = reader
            .addressed_measured(
                &[id(1)],
                false,
                first.proof_after.as_ref().map(OpaqueCursor::as_str),
                Duration::from_secs(30),
            )
            .unwrap()
            .value;
        assert!(second.proof_after.is_some());
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::NeedsInput));
        let changed_task = reader
            .addressed_measured(
                &[id(1)],
                false,
                second.proof_after.as_ref().map(OpaqueCursor::as_str),
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(changed_task.stats.association_checks, 32);
        assert_eq!(changed_task.value.rows[0].quiescent, None);
        assert_eq!(
            changed_task.value.rows[0].outcome,
            Some(SafeOutcome::NeedsInput)
        );
        dispatching_queue(&paths, 35);
        let changed_queue = reader
            .addressed_measured(
                &[id(1)],
                false,
                changed_task
                    .value
                    .proof_after
                    .as_ref()
                    .map(OpaqueCursor::as_str),
                Duration::from_secs(30),
            )
            .unwrap();
        assert_eq!(changed_queue.stats.association_checks, 32);
        assert_eq!(changed_queue.value.rows[0].quiescent, None);
    }

    #[test]
    fn sixteen_task_proof_group_is_bounded_and_eventually_confirmed() {
        let (_root, paths, runtime) = fixture();
        let ids = (1..=16).map(id).collect::<Vec<_>>();
        for number in 1..=16 {
            write_record(&paths, &record(number, TaskState::Open, TaskOutcome::Done));
            private_directory(&paths.state.join("turns").join(id(number).to_string()));
        }
        dispatching_queue(&paths, 33);
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let mut cursor = None;
        let mut associations = 0;
        let mut calls = 0;
        loop {
            let result = reader
                .addressed_measured(
                    &ids,
                    false,
                    cursor.as_ref().map(OpaqueCursor::as_str),
                    Duration::from_secs(30),
                )
                .unwrap();
            calls += 1;
            assert_eq!(result.value.rows.len(), 16);
            assert_eq!(result.stats.queue_reads, 1);
            assert!(result.stats.association_checks <= 32);
            associations += result.stats.association_checks;
            cursor = result.value.proof_after;
            if cursor.is_none() {
                assert!(
                    result
                        .value
                        .rows
                        .iter()
                        .all(|row| row.quiescent == Some(true))
                );
                break;
            }
            assert!(cursor.as_ref().unwrap().as_str().len() <= 2048);
            assert!(calls <= 17);
        }
        assert_eq!(associations, 528);
        assert_eq!(calls, 17);
    }

    #[test]
    fn repair_commits_unknown_dispatch_row_without_restarting_its_key() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::Done));
        private_directory(&paths.state.join("turns").join(id(1).to_string()));
        dispatching_queue(&paths, 70);
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let page = reader
            .repair_read(None, 64, Duration::from_secs(30))
            .unwrap();
        assert_eq!(page.stats.association_checks, 32);
        assert_eq!(page.value.rows[0].quiescent, None);
        assert!(page.value.complete);
        assert!(page.value.next.is_none());
    }

    #[test]
    fn estale_retries_three_times_within_the_same_binding() {
        let (_root, paths, runtime) = fixture();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let mut calls = 0;
        let bytes = reader
            .retry_same_binding(Duration::from_secs(30), || {
                calls += 1;
                if calls <= 3 {
                    Err(io::Error::from_raw_os_error(libc::ESTALE).into())
                } else {
                    Ok(reader.root.read_private_regular("client-id", 33)?)
                }
            })
            .unwrap();
        assert_eq!(calls, 4);
        assert_eq!(bytes, fs::read(paths.state.join("client-id")).unwrap());
    }

    #[test]
    fn persistent_estale_is_bounded_and_non_estale_errors_are_not_retried() {
        let (_root, paths, runtime) = fixture();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let mut calls = 0;
        let error = reader
            .retry_same_binding::<()>(Duration::from_secs(30), || {
                calls += 1;
                Err(io::Error::from_raw_os_error(libc::ESTALE).into())
            })
            .unwrap_err();
        assert_eq!(calls, 4);
        assert!(error.to_string().contains("CONTROLLER_EVENTS_UNAVAILABLE"));
        calls = 0;
        assert!(
            reader
                .retry_same_binding::<()>(Duration::from_secs(30), || {
                    calls += 1;
                    Err(io::Error::from_raw_os_error(libc::EACCES).into())
                })
                .is_err()
        );
        assert_eq!(calls, 1);
    }

    #[test]
    fn estale_retry_never_adopts_a_replacement_root() {
        let (_root, paths, runtime) = fixture();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let mut calls = 0;
        assert!(
            reader
                .retry_same_binding::<()>(Duration::from_secs(30), || {
                    calls += 1;
                    if calls == 1 {
                        fs::rename(&paths.state, paths.state.with_file_name("retired-state"))
                            .unwrap();
                        private_directory(&paths.state);
                        Err(io::Error::from_raw_os_error(libc::ESTALE).into())
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        assert_eq!(calls, 1);
    }

    #[test]
    fn known_busy_row_does_not_open_its_turn_namespace() {
        let (_root, paths, runtime) = fixture();
        let busy = record(1, TaskState::Open, TaskOutcome::Done)
            .with_runner(Some(RunnerIdentity::new(
                ProcessIdentity::new(2_000_000_001, 1).unwrap(),
            )))
            .unwrap();
        write_record(&paths, &busy);
        std::os::unix::fs::symlink("missing", paths.state.join("turns").join(id(1).to_string()))
            .unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let result = reader
            .addressed_measured(&[id(1)], false, None, Duration::from_secs(30))
            .unwrap();
        assert_eq!(result.stats.association_checks, 0);
        assert_eq!(result.value.rows[0].busy, Some(true));
        assert_eq!(result.value.rows[0].quiescent, Some(false));
        assert!(result.value.proof_after.is_none());
    }

    #[test]
    fn completed_dispatch_match_survives_group_proof_continuation() {
        let (_root, paths, runtime) = fixture();
        for number in 1..=2 {
            write_record(&paths, &record(number, TaskState::Open, TaskOutcome::Done));
            private_directory(&paths.state.join("turns").join(id(number).to_string()));
        }
        let jobs = dispatching_queue(&paths, 33);
        private_directory(
            &paths
                .state
                .join("turns")
                .join(id(1).to_string())
                .join(jobs[0].to_string()),
        );
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        let first = reader
            .addressed_measured(&[id(1), id(2)], false, None, Duration::from_secs(30))
            .unwrap()
            .value;
        assert_eq!(first.rows[0].busy, Some(true));
        assert_eq!(first.rows[1].quiescent, None);
        let second = reader
            .addressed_measured(
                &[id(1), id(2)],
                false,
                first.proof_after.as_ref().map(OpaqueCursor::as_str),
                Duration::from_secs(30),
            )
            .unwrap()
            .value;
        assert_eq!(second.rows[0].queue_dispatching, Some(true));
        assert_eq!(second.rows[0].busy, Some(true));
        assert_eq!(second.rows[1].quiescent, Some(true));
        assert!(second.proof_after.is_none());
    }
}
