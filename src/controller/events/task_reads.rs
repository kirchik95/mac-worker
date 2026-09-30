//! Independent task-only event reads; Phase A module placement is temporary.

use std::{io, sync::Arc, time::Duration};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    inputs::RelativePath,
    job::{ClientId, QueueEntryKind, QueueSnapshot, QueueState},
    paths::PathLayout,
    rooted_fs::RootedDir,
    task::{LocalTaskRecord, RunId, TaskId, TaskOutcome, TaskState, TurnId},
};

pub const REPAIR_MAX_DIRECTORY_ENTRIES: usize = 100_000;
pub const MAX_TASK_RECORD_BYTES: usize = 1024 * 1024;
pub const MAX_DISPATCH_ASSOCIATIONS: usize = 32;

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
    ids.sort_unstable_by_key(TaskId::to_string);
    Ok(ids)
}

fn invalid_state() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_EVENTS_UNAVAILABLE: invalid task state".into())
}

/// Phase A timing seam; replaced by the identical T1 EventRuntime trait in Phase B.
pub trait TaskReadRuntime: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
    fn cancelled(&self) -> bool;
}

/// Internal, title-free by default. T1 wire conversion belongs to the RPC adapter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TaskRecordFacts {
    pub task_id: TaskId,
    pub run_id: Option<RunId>,
    pub state: String,
    pub latest_turn_id: Option<TurnId>,
    pub outcome: Option<String>,
    pub code: Option<String>,
    pub runner_present: bool,
    pub close_intent: bool,
    pub auto_continue_intent: bool,
    pub queue_dispatching: Option<bool>,
    pub result_imported: bool,
    pub busy: Option<bool>,
    pub quiescent: Option<bool>,
    pub fact_digest: String,
    pub title: Option<String>,
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

#[derive(Debug, Serialize)]
pub struct AddressedFacts {
    pub rows: Vec<TaskRecordFacts>,
    pub missing: Vec<TaskId>,
    pub proof_after: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RepairFacts {
    pub rows: Vec<TaskRecordFacts>,
    pub next: Option<String>,
    pub complete: bool,
}

pub struct TaskEventReadStore {
    root: RootedDir,
    tasks: RootedDir,
    queue: RootedDir,
    turns: RootedDir,
    client_id: ClientId,
    runtime: Arc<dyn TaskReadRuntime>,
}

impl TaskEventReadStore {
    pub fn open_existing(
        paths: &PathLayout,
        runtime: Arc<dyn TaskReadRuntime>,
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

    pub fn addressed(
        &self,
        ids: &[TaskId],
        include_titles: bool,
        proof_after: Option<&str>,
        deadline: Duration,
    ) -> Result<TaskReadResult<AddressedFacts>, WorkerError> {
        if ids.is_empty()
            || ids.len() > 16
            || ids
                .iter()
                .enumerate()
                .any(|(index, id)| ids[..index].contains(id))
            || proof_after.is_some()
        {
            return Err(WorkerError::Protocol("CONTROLLER_EVENTS_INVALID".into()));
        }
        let _fence = self.acquire(deadline)?;
        let started = self.runtime.now();
        let mut stats = TaskReadStats::default();
        let (queue, _) = self.read_queue(&mut stats)?;
        let mut result = AddressedFacts {
            rows: Vec::new(),
            missing: Vec::new(),
            proof_after: None,
        };
        for &id in ids {
            self.check_deadline(deadline)?;
            let Some(record) = self.read_task(id, &mut stats)? else {
                result.missing.push(id);
                continue;
            };
            let dispatching =
                if crate::client_state::task_operator_busy_reason(&record, None).is_some() {
                    None
                } else {
                    self.dispatching(id, &queue, &mut stats)?
                };
            result
                .rows
                .push(record_facts(&record, dispatching, include_titles)?);
        }
        self.verify_bindings()?;
        self.check_deadline(deadline)?;
        stats.work_elapsed = self.runtime.now().saturating_sub(started);
        Ok(TaskReadResult {
            value: result,
            stats,
        })
    }

    pub fn repair(
        &self,
        _after: Option<&str>,
        _limit: usize,
        _deadline: Duration,
    ) -> Result<TaskReadResult<RepairFacts>, WorkerError> {
        Err(invalid_state())
    }

    fn check_deadline(&self, deadline: Duration) -> Result<(), WorkerError> {
        if self.runtime.cancelled() {
            return Err(crate::error::ProcessError::Cancelled.into());
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
        stats: &mut TaskReadStats,
    ) -> Result<Option<LocalTaskRecord>, WorkerError> {
        stats.record_reads += 1;
        let bytes = match self
            .tasks
            .read_private_regular(&format!("{id}.json"), MAX_TASK_RECORD_BYTES as u64)
        {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
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
        stats: &mut TaskReadStats,
    ) -> Result<(QueueSnapshot, String), WorkerError> {
        stats.queue_reads += 1;
        let bytes = self
            .queue
            .read_private_regular("state.json", MAX_TASK_RECORD_BYTES as u64)?;
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
        stats: &mut TaskReadStats,
    ) -> Result<Option<bool>, WorkerError> {
        let task_turns = match open_directory(&self.turns, &task_id.to_string()) {
            Ok(directory) => Some(directory),
            Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let Some(task_turns) = task_turns else {
            return Ok(Some(false));
        };
        for entry in queue.entries().iter().filter(|entry| {
            entry.kind() == QueueEntryKind::TaskTurn
                && matches!(entry.state(), QueueState::Dispatching { .. })
        }) {
            if stats.association_checks == MAX_DISPATCH_ASSOCIATIONS {
                return Ok(None);
            }
            stats.association_checks += 1;
            match open_directory(&task_turns, &entry.job_id().to_string()) {
                Ok(_) => return Ok(Some(true)),
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        task_turns.verify_bound()?;
        Ok(Some(false))
    }
}

pub fn record_facts(
    record: &LocalTaskRecord,
    dispatching: Option<bool>,
    include_titles: bool,
) -> Result<TaskRecordFacts, WorkerError> {
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
        Some(TaskOutcome::Failed { reason }) => Some(safe_code(reason)),
        _ => record.abandon_code().map(safe_code),
    };
    let state = match record.status().state() {
        TaskState::Queued => "queued",
        TaskState::Active => "active",
        TaskState::Open => "open",
        TaskState::Closed => "closed",
        TaskState::Abandoned => "abandoned",
        TaskState::Lost => "lost",
    };
    let facts = TaskRecordFacts {
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
    };
    if serde_json::to_vec(&facts)
        .map_err(|_| invalid_state())?
        .len()
        > 2048
    {
        return Err(invalid_state());
    }
    Ok(facts)
}

fn safe_code(code: &str) -> String {
    match code {
        "TURN_FAILED"
        | "PUBLISH_FAILED"
        | "RESULT_FETCH_FAILED"
        | "RESULT_UNPARSEABLE"
        | "LOG_DRAIN_UNAVAILABLE"
        | "TASK_ABANDONED"
        | "TURN_LOST"
        | "CANCELLED"
        | "TIMED_OUT" => code.into(),
        _ => "TURN_FAILED".into(),
    }
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
            TaskState, TaskStatus, TurnSummary, TurnTerminal,
        },
    };

    use super::*;

    #[derive(Default)]
    struct ManualRuntime {
        millis: AtomicU64,
        step: AtomicU64,
        cancelled: AtomicBool,
    }

    impl TaskReadRuntime for ManualRuntime {
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
    fn minimal_existing_open_never_bootstraps_and_avoids_other_domains() {
        let (_root, paths, runtime) = fixture();
        let task = record(1, TaskState::Open, TaskOutcome::Done);
        write_record(&paths, &task);
        fs::write(
            paths.state.join("runs").join("invalid.json"),
            b"broken large run",
        )
        .unwrap();
        fs::remove_dir_all(paths.state.join("active-tasks")).unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime.clone()).unwrap();
        let result = reader
            .addressed(&[id(1), id(2)], false, None, Duration::from_secs(30))
            .unwrap();
        assert_eq!(result.value.rows.len(), 1);
        assert_eq!(result.value.missing, vec![id(2)]);
        assert_eq!(result.stats.queue_reads, 1);
        assert_eq!(result.stats.names_calls, 0);
        assert!(!paths.state.join("active-tasks").exists());
        let mut absent = paths;
        absent.state = absent.state.with_file_name("absent");
        assert!(TaskEventReadStore::open_existing(&absent, runtime).is_err());
        assert!(!absent.state.exists());
    }

    #[test]
    fn event_directory_or_lock_damage_still_returns_state() {
        let (_root, paths, runtime) = fixture();
        write_record(&paths, &record(1, TaskState::Open, TaskOutcome::NeedsInput));
        let events = paths.controller_state_root().join("events");
        fs::create_dir_all(&events).unwrap();
        std::os::unix::fs::symlink("missing", events.join("journal.lock")).unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        assert_eq!(
            reader
                .addressed(&[id(1)], false, None, Duration::from_secs(30))
                .unwrap()
                .value
                .rows[0]
                .quiescent,
            Some(true)
        );
        fs::remove_dir_all(&events).unwrap();
        fs::write(events, b"not a directory").unwrap();
        assert_eq!(
            reader
                .addressed(&[id(1)], false, None, Duration::from_secs(30))
                .unwrap()
                .value
                .rows
                .len(),
            1
        );
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
    fn recorded_dead_runner_is_still_busy_without_liveness_inspection() {
        let task = record(1, TaskState::Open, TaskOutcome::Done)
            .with_runner(Some(RunnerIdentity::new(
                ProcessIdentity::new(2_000_000_001, 1).unwrap(),
            )))
            .unwrap();
        let facts = record_facts(&task, Some(false), false).unwrap();
        assert!(facts.runner_present);
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
        assert_eq!(facts.outcome.as_deref(), Some("failed"));
        assert_eq!(facts.code.as_deref(), Some("TURN_FAILED"));
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

    #[test]
    fn unsafe_root_and_changed_task_binding_fail_closed() {
        let (_root, paths, runtime) = fixture();
        fs::set_permissions(&paths.state, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(TaskEventReadStore::open_existing(&paths, runtime.clone()).is_err());
        fs::set_permissions(&paths.state, fs::Permissions::from_mode(0o700)).unwrap();
        let reader = TaskEventReadStore::open_existing(&paths, runtime).unwrap();
        fs::rename(paths.state.join("tasks"), paths.state.join("old-tasks")).unwrap();
        fs::create_dir(paths.state.join("tasks")).unwrap();
        fs::set_permissions(paths.state.join("tasks"), fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            reader
                .addressed(&[id(1)], false, None, Duration::from_secs(30))
                .is_err()
        );
    }
}
