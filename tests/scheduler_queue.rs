use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    os::unix::{
        fs::{FileTypeExt, PermissionsExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::{
        ClientStateStore, ClientStateWritePoint, ReservedSlotTakeover, RunnerSlotDecision,
    },
    config::{Config, WorkerEntry},
    error::{ProcessError, WorkerError},
    job::{
        AdmissionObservation, CommandSpec, CommandSummary, JobId, JobMeta, JobState, JobStatus,
        LeaseToken, LocalJobRecord, PreacceptanceDisposition, ProcessIdentity, QueueCancel,
        QueueEntry, QueueEntryKind, QueueId, QueueRunReference, QueueSnapshot, QueueState,
        RemoteUncertainty, RequestFingerprintMaterial, ResolveOrAbandonRequest,
        ResolveOrAbandonResponse, RunId,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    scheduler::{CandidateSlot, QueueBlockingReason, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
    },
    transfer::{PreacceptanceAbandonmentReceipt, RemoteJobClient, ResolutionRuntime},
};
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_PROJECT_ID: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const OTHER_WORKTREE_ID: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const COMMAND_SECRET: &str = "PLANTED_EXACT_COMMAND_SECRET";
const PATH_SECRET: &str = "/Users/alice/PLANTED_QUEUE_PATH";
const ENV_SECRET: &str = "PLANTED_QUEUE_ENV_LIKE_VALUE";
const TOKEN_SECRET: &str = "dddddddddddddddddddddddddddddddd";

#[derive(Clone, Copy)]
struct FixedOwnerInspector {
    observation: ProcessObservation,
}

impl ProcessInspector for FixedOwnerInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 1_000 + 1)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        match self.observation {
            ProcessObservation::Matching { .. } => ProcessObservation::Matching {
                process_group: expected.pid(),
            },
            observation => observation,
        }
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

struct QueueFixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    store: ClientStateStore,
}

fn open_queue() -> QueueFixture {
    open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Matching { process_group: 1 },
    })
}

#[derive(Clone)]
struct LiveSetInspector {
    live: std::collections::BTreeSet<(u32, u64)>,
}

impl ProcessInspector for LiveSetInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        if self
            .live
            .contains(&(expected.pid(), expected.start_time_micros()))
        {
            ProcessObservation::Matching {
                process_group: expected.pid(),
            }
        } else {
            ProcessObservation::Absent
        }
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

fn live_set(identities: &[ProcessIdentity]) -> LiveSetInspector {
    LiveSetInspector {
        live: identities
            .iter()
            .map(|identity| (identity.pid(), identity.start_time_micros()))
            .collect(),
    }
}

fn plant_task_turn(
    store: &ClientStateStore,
    number: u128,
    enqueue_owner: ProcessIdentity,
    runner: Option<ProcessIdentity>,
) -> (TaskId, TurnId) {
    let task_id = TaskId::new(Uuid::from_u128(number));
    let turn_id = TurnId::new(Uuid::from_u128(number + 1_000));
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        title: None,
        prompt: "private prompt".into(),
        created_at_millis: number as u64,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Active,
        None,
        None,
        false,
        Some(meta.base_oid().clone()),
        None,
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1, turn_id, None, None, None, false, None, None,
        )],
        number as u64,
    )
    .unwrap();
    store
        .create_task(
            LocalTaskRecord::new(
                meta,
                status,
                None,
                runner.map(RunnerIdentity::new),
                None,
                "c".repeat(64),
                None,
                true,
                None,
            )
            .unwrap(),
        )
        .unwrap();
    store
        .enqueue(queued_with(
            store,
            &turn_id.to_string(),
            number as u64,
            enqueue_owner,
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::TaskTurn,
            None,
        ))
        .unwrap();
    (task_id, turn_id)
}

fn open_queue_with_owner_inspector<I>(inspector: I) -> QueueFixture
where
    I: ProcessInspector + 'static,
{
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open_with_owner_inspector(&root, inspector).unwrap();
    QueueFixture {
        _directory: directory,
        root,
        store,
    }
}

fn owner(pid: u32) -> ProcessIdentity {
    ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7).unwrap()
}

fn queued(
    store: &ClientStateStore,
    job_id: &str,
    enqueued_at_millis: u64,
    owner: ProcessIdentity,
) -> QueueEntry {
    queued_with(
        store,
        job_id,
        enqueued_at_millis,
        owner,
        WorkerPreference::Automatic,
        Vec::new(),
        QueueEntryKind::Batch,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn queued_with(
    store: &ClientStateStore,
    job_id: &str,
    enqueued_at_millis: u64,
    owner: ProcessIdentity,
    preference: WorkerPreference,
    requirements: Vec<String>,
    kind: QueueEntryKind,
    run: Option<QueueRunReference>,
) -> QueueEntry {
    QueueEntry::new(
        job_id.parse().unwrap(),
        store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSummary::argv(2).unwrap(),
        requirements,
        preference,
        kind,
        run,
        owner,
        enqueued_at_millis,
    )
    .unwrap()
}

fn run_reference(id: &str, max_parallel: u32) -> QueueRunReference {
    QueueRunReference::new(RunId::new(id.into()).unwrap(), max_parallel).unwrap()
}

fn cache_observation(store: &ClientStateStore, worker: &str, capabilities: &[&str], now: u64) {
    let observation = AdmissionObservation::new(
        worker.into(),
        true,
        CandidateSlot::Idle,
        capabilities.iter().map(|value| (*value).into()).collect(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        now,
    )
    .unwrap();
    store
        .admission_observation(worker, now, || Ok(observation))
        .unwrap();
}

fn cache_unavailable_observation(store: &ClientStateStore, worker: &str, now: u64) {
    let observation = AdmissionObservation::new(
        worker.into(),
        false,
        CandidateSlot::Busy,
        Vec::new(),
        None,
        0,
        now,
    )
    .unwrap();
    store
        .admission_observation(worker, now, || Ok(observation))
        .unwrap();
}

fn cache_busy_observation(store: &ClientStateStore, worker: &str, now: u64) {
    let observation = AdmissionObservation::new(
        worker.into(),
        true,
        CandidateSlot::Busy,
        Vec::new(),
        Some(8 * 1024 * 1024 * 1024),
        64 * 1024 * 1024 * 1024,
        now,
    )
    .unwrap();
    store
        .admission_observation(worker, now, || Ok(observation))
        .unwrap();
}

fn mode(path: impl AsRef<Path>) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

fn local_record(
    store: &ClientStateStore,
    job_id: JobId,
    worker: &str,
    status: Option<JobStatus>,
) -> LocalJobRecord {
    local_record_with_uncertainty(store, job_id, worker, status, RemoteUncertainty::None)
}

fn local_record_with_uncertainty(
    store: &ClientStateStore,
    job_id: JobId,
    worker: &str,
    status: Option<JobStatus>,
    uncertainty: RemoteUncertainty,
) -> LocalJobRecord {
    local_record_with_binding(
        store,
        job_id,
        worker,
        PROJECT_ID,
        WORKTREE_ID,
        CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
        status,
        uncertainty,
    )
}

#[allow(clippy::too_many_arguments)]
fn local_record_with_binding(
    store: &ClientStateStore,
    job_id: JobId,
    worker: &str,
    project_id: &str,
    worktree_id: &str,
    command: CommandSpec,
    status: Option<JobStatus>,
    uncertainty: RemoteUncertainty,
) -> LocalJobRecord {
    let material = RequestFingerprintMaterial::new(
        job_id,
        store.client_id(),
        LeaseToken::new(TOKEN_SECRET.parse().unwrap()),
        1_000,
        worker.into(),
        project_id.into(),
        worktree_id.into(),
        DIGEST.into(),
        "packages/app".into(),
        30_000,
        "heavy".into(),
        command,
    )
    .unwrap();
    LocalJobRecord::new(
        JobMeta::new(&material, material.fingerprint()).unwrap(),
        material.lease_token(),
        status,
        uncertainty,
    )
    .unwrap()
}

fn install_local_record_as(fixture: &QueueFixture, job_id: JobId, record: &LocalJobRecord) {
    let mut bytes = serde_json::to_vec(record).unwrap();
    bytes.push(b'\n');
    let path = fixture.root.join("jobs").join(format!("{job_id}.json"));
    fs::write(&path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}

fn install_abandonment_request(
    fixture: &QueueFixture,
    job_id: JobId,
    worker: &str,
) -> ResolveOrAbandonRequest {
    let record = local_record(&fixture.store, job_id, worker, None);
    fixture.store.create_job(record.clone()).unwrap();
    ResolveOrAbandonRequest::from_local_record(&record).unwrap()
}

struct AbandonedResolutionRunner {
    results: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
}

impl ProcessRunner for AbandonedResolutionRunner {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected remote resolution call")
    }
}

#[derive(Default)]
struct ImmediateResolutionRuntime {
    now: Mutex<Duration>,
}

impl ResolutionRuntime for ImmediateResolutionRuntime {
    fn monotonic_now(&self) -> Duration {
        *self.now.lock().unwrap()
    }

    fn sleep(&self, _duration: Duration) {
        *self.now.lock().unwrap() = Duration::from_secs(30);
    }
}

fn queue_worker(name: &str) -> WorkerEntry {
    WorkerEntry {
        name: name.into(),
        ssh: "unused-test-host".into(),
        slots: 1,
        capabilities: Vec::new(),
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    }
}

fn remote_abandonment_receipt(
    request: &ResolveOrAbandonRequest,
) -> PreacceptanceAbandonmentReceipt {
    let mut stdout = serde_json::to_vec(&ResolveOrAbandonResponse::abandoned()).unwrap();
    stdout.push(b'\n');
    let runner = AbandonedResolutionRunner {
        results: Mutex::new(
            vec![
                Err(ProcessError::DeadlineExceeded {
                    deadline: Duration::from_secs(30),
                }
                .into()),
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout,
                    stderr: Vec::new(),
                }),
            ]
            .into(),
        ),
    };
    let runtime = ImmediateResolutionRuntime::default();
    let resolution = RemoteJobClient::new_with_runtime(&runner, &runtime)
        .resolve_preacceptance_with_receipt(&queue_worker(request.worker_name()), request)
        .unwrap();
    assert!(matches!(
        resolution.disposition(),
        PreacceptanceDisposition::Abandoned
    ));
    resolution
        .into_abandonment_receipt()
        .expect("an actual Abandoned response must carry authority")
}

fn dispatching_batch(
    store: &ClientStateStore,
    job_id: &str,
    dispatcher: ProcessIdentity,
    enqueued_at_millis: u64,
) -> QueueEntry {
    let entry = store
        .enqueue(queued(store, job_id, enqueued_at_millis, dispatcher))
        .unwrap();
    let claimed = store
        .claim_next(dispatcher, &["mini-1".into()], enqueued_at_millis + 1)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.entry().job_id(), entry.job_id());
    entry
}

fn assert_terminal_unproven(error: WorkerError) {
    assert!(matches!(
        error,
        WorkerError::Queue {
            code: "QUEUE_TERMINAL_UNPROVEN",
            ..
        }
    ));
}

#[test]
fn queue_rows_expose_advisory_blocking_reasons_from_cached_observations_only() {
    // Break caught: a dashboard-facing read refreshes/probes or mutates queue
    // state, or reinterprets the scheduler's policy independently.
    let fixture = open_queue();
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
        workers: vec![
            queue_worker("mini-a"),
            queue_worker("mini-b"),
            queue_worker("mini-c"),
        ],
    };
    cache_observation(&fixture.store, "mini-a", &[], 10);
    cache_busy_observation(&fixture.store, "mini-b", 10);
    cache_unavailable_observation(&fixture.store, "mini-c", 10);

    let reservation = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f0",
            10,
            owner(240),
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::Batch,
            Some(run_reference("run-cap", 1)),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(owner(240), &["mini-a".into()], 11)
        .unwrap()
        .unwrap();
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f1",
            12,
            owner(241),
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::Batch,
            Some(run_reference("run-cap", 1)),
        ))
        .unwrap();
    let pinned_busy = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f2",
            13,
            owner(242),
            WorkerPreference::Pinned {
                worker: "mini-b".into(),
            },
            Vec::new(),
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    let capability_missing = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f3",
            14,
            owner(243),
            WorkerPreference::Automatic,
            vec!["gpu".into()],
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    let no_eligible_worker = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f4",
            15,
            owner(244),
            WorkerPreference::Pinned {
                worker: "mini-c".into(),
            },
            Vec::new(),
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();

    let before_read = fixture.store.queue_snapshot().unwrap();
    let rows = fixture
        .store
        .queue_rows_with_blocking_reasons(&config)
        .unwrap();
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before_read);
    let reason_for = |job_id| {
        rows.iter()
            .find(|row| row.entry().job_id() == job_id)
            .unwrap()
            .blocking_reason()
    };

    assert_eq!(reason_for(reservation.job_id()), None);
    assert_eq!(
        reason_for("000000000000000000000000000000f1".parse().unwrap()),
        Some(&QueueBlockingReason::RunCap)
    );
    assert_eq!(
        reason_for(pinned_busy.job_id()),
        Some(&QueueBlockingReason::PinnedWorkerBusy {
            worker: "mini-b".into()
        })
    );
    assert_eq!(
        reason_for(capability_missing.job_id()),
        Some(&QueueBlockingReason::CapabilityMissing {
            missing: vec!["gpu".into()]
        })
    );
    assert_eq!(
        reason_for(no_eligible_worker.job_id()),
        Some(&QueueBlockingReason::NoEligibleWorker)
    );
}

#[test]
fn queue_blocking_reason_without_cached_observations_is_no_eligible_worker() {
    // Break caught: an empty cache vacuously claims every requested capability
    // is absent instead of honestly reporting missing eligibility information.
    let fixture = open_queue();
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
        workers: vec![queue_worker("mini-a")],
    };
    let entry = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f5",
            16,
            owner(245),
            WorkerPreference::Automatic,
            vec!["gpu".into()],
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();

    let rows = fixture
        .store
        .queue_rows_with_blocking_reasons(&config)
        .unwrap();
    let row = rows
        .iter()
        .find(|row| row.entry().job_id() == entry.job_id())
        .unwrap();
    assert_eq!(
        row.blocking_reason(),
        Some(&QueueBlockingReason::NoEligibleWorker)
    );
}

#[test]
fn queue_blocking_reason_uses_pinned_policy_rejections_for_capabilities() {
    // Break caught: an unpinned worker advertising the capability masks the
    // pinned worker's capability incompatibility in the dashboard view.
    let fixture = open_queue();
    let config = Config {
        version: 1,
        notifications: mac_worker::config::NotificationsConfig::default(),
        controller: Default::default(),
        workers: vec![queue_worker("mini-a"), queue_worker("mini-b")],
    };
    cache_observation(&fixture.store, "mini-a", &[], 17);
    cache_observation(&fixture.store, "mini-b", &["gpu"], 17);
    let entry = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000f6",
            17,
            owner(246),
            WorkerPreference::Pinned {
                worker: "mini-a".into(),
            },
            vec!["gpu".into()],
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();

    let rows = fixture
        .store
        .queue_rows_with_blocking_reasons(&config)
        .unwrap();
    let row = rows
        .iter()
        .find(|row| row.entry().job_id() == entry.job_id())
        .unwrap();
    assert_eq!(
        row.blocking_reason(),
        Some(&QueueBlockingReason::CapabilityMissing {
            missing: vec!["gpu".into()],
        })
    );
}

#[test]
fn queue_records_are_canonical_owner_only_and_contain_summaries_not_secrets() {
    // Break caught: queue persistence retains exact inputs or publishes
    // permissive/non-canonical state that another invocation can misread.
    let fixture = open_queue();
    let entry = QueueEntry::new(
        "00000000000000000000000000000001".parse().unwrap(),
        fixture.store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSpec::argv(vec![
            "tool".into(),
            COMMAND_SECRET.into(),
            PATH_SECRET.into(),
            ENV_SECRET.into(),
        ])
        .unwrap()
        .summary()
        .unwrap(),
        Vec::new(),
        WorkerPreference::Automatic,
        QueueEntryKind::Batch,
        None,
        owner(10),
        10,
    )
    .unwrap();

    let persisted = fixture.store.enqueue(entry).unwrap();
    fixture
        .store
        .record_affinity(PROJECT_ID, WORKTREE_ID, "mini-a", 11)
        .unwrap();
    let bytes = fs::read(fixture.root.join("queue/state.json")).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    let affinity = [
        fs::read_to_string(
            fixture
                .root
                .join("affinity/projects")
                .join(format!("{PROJECT_ID}.json")),
        )
        .unwrap(),
        fs::read_to_string(
            fixture
                .root
                .join("affinity/worktrees")
                .join(format!("{PROJECT_ID}-{WORKTREE_ID}.json")),
        )
        .unwrap(),
    ];

    assert_eq!(persisted.queue_id().value(), 1);
    assert_eq!(
        fixture.store.queue_snapshot().unwrap().entries(),
        &[persisted]
    );
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert!(text.contains(r#""kind":"batch""#));
    assert!(text.contains(r#""state":"waiting""#));
    assert!(text.contains(r#""command_summary":{"mode":"argv","arg_count":4}"#));
    assert!(text.contains(r#""preacceptance_abandonment_proof":null"#));
    for secret in [
        COMMAND_SECRET,
        PATH_SECRET,
        ENV_SECRET,
        DIGEST,
        TOKEN_SECRET,
        "prompt text",
        "session_id",
    ] {
        assert!(!text.contains(secret), "queue leaked {secret}");
        assert!(
            affinity.iter().all(|record| !record.contains(secret)),
            "affinity leaked {secret}"
        );
    }
    assert_eq!(mode(fixture.root.join("queue")), 0o700);
    assert_eq!(mode(fixture.root.join("queue/state.json")), 0o600);
    assert_eq!(mode(fixture.root.join("queue/lock")), 0o600);
    let mut queue_entries = fs::read_dir(fixture.root.join("queue"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    queue_entries.sort();
    assert_eq!(queue_entries, ["lock", "state.json"]);
}

#[test]
fn schema_rejects_invalid_ids_caps_capabilities_modes_and_unknown_fields() {
    // Break caught: permissive constructors or decoding admit values that
    // cannot be used as canonical path/capacity decisions.
    assert!(RunId::new("UPPER".into()).is_err());
    assert!(RunId::new("../escape".into()).is_err());
    assert!(QueueId::new(0).is_err());
    assert!(QueueRunReference::new(RunId::new("run-1".into()).unwrap(), 0).is_err());

    let fixture = open_queue();
    let pending = queued(
        &fixture.store,
        "00000000000000000000000000000001",
        10,
        owner(10),
    );
    assert!(
        serde_json::to_vec(&pending).is_err(),
        "an unassigned queue ID must never serialize as a persistent record"
    );
    assert!(
        serde_json::to_vec(&QueueState::Dispatching {
            dispatch_owner: owner(10),
            selected_worker: "../mini-1".into(),
            claimed_at_millis: 0,
        })
        .is_err()
    );
    assert!(
        serde_json::from_slice::<QueueState>(
            br#"{"state":"dispatching","dispatch_owner":{"pid":10,"start_time_micros":100007},"selected_worker":"../mini-1","claimed_at_millis":0}"#,
        )
        .is_err()
    );
    assert!(
        QueueEntry::new(
            "00000000000000000000000000000002".parse().unwrap(),
            fixture.store.client_id(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            CommandSummary::shell(),
            vec!["Docker".into()],
            WorkerPreference::Automatic,
            QueueEntryKind::Batch,
            None,
            owner(11),
            11,
        )
        .is_err()
    );

    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000003",
            12,
            owner(12),
        ))
        .unwrap();
    let path = fixture.root.join("queue/state.json");
    let bytes = fs::read(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["unknown"] = serde_json::json!(true);
    let mut tampered = serde_json::to_vec(&value).unwrap();
    tampered.push(b'\n');
    fs::write(&path, tampered).unwrap();
    assert!(fixture.store.queue_snapshot().is_err());

    for (needle, replacement) in [
        (r#""kind":"batch""#, r#""kind":"interactive""#),
        (
            r#""preference":{"mode":"automatic"}"#,
            r#""preference":{"mode":"random"}"#,
        ),
    ] {
        let fixture = open_queue();
        fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000004",
                13,
                owner(13),
            ))
            .unwrap();
        let path = fixture.root.join("queue/state.json");
        let original = String::from_utf8(fs::read(&path).unwrap()).unwrap();
        assert!(original.contains(needle));
        fs::write(path, original.replace(needle, replacement)).unwrap();
        assert!(fixture.store.queue_snapshot().is_err());
    }
}

#[test]
fn malformed_noncanonical_duplicate_job_and_sequence_state_fails_closed() {
    // Break caught: validation trusts syntax but not canonical ordering and
    // uniqueness, allowing ambiguous FIFO identities to reach claim logic.
    for mutation in [
        "malformed",
        "leading-space",
        "duplicate-job",
        "duplicate-sequence",
    ] {
        let fixture = open_queue();
        fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000004",
                20,
                owner(20),
            ))
            .unwrap();
        fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000005",
                21,
                owner(21),
            ))
            .unwrap();
        let path = fixture.root.join("queue/state.json");
        let original = fs::read(&path).unwrap();
        let replacement = match mutation {
            "malformed" => b"{\"next_id\":".to_vec(),
            "leading-space" => {
                let mut bytes = vec![b' '];
                bytes.extend(original);
                bytes
            }
            "duplicate-job" => String::from_utf8(original)
                .unwrap()
                .replace(
                    "00000000000000000000000000000005",
                    "00000000000000000000000000000004",
                )
                .into_bytes(),
            "duplicate-sequence" => String::from_utf8(original)
                .unwrap()
                .replacen("\"queue_id\":2", "\"queue_id\":1", 1)
                .into_bytes(),
            _ => unreachable!(),
        };
        fs::write(path, replacement).unwrap();
        assert!(
            fixture.store.queue_snapshot().is_err(),
            "mutation {mutation}"
        );
    }
}

#[test]
fn duplicate_enqueue_and_backwards_time_do_not_mutate_fifo_state() {
    // Break caught: enqueue aliases an existing job, or inverted caller clocks
    // reorder FIFO / fail closed instead of coalescing under QueueLock.
    let fixture = open_queue();
    let first = queued(
        &fixture.store,
        "00000000000000000000000000000006",
        30,
        owner(30),
    );
    let first = fixture.store.enqueue(first.clone()).unwrap();
    assert_eq!(
        fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000006",
                30,
                owner(30),
            ))
            .unwrap_err()
            .public_code(),
        "QUEUE_JOB_CONFLICT"
    );
    let second = fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000007",
            29,
            owner(31),
        ))
        .unwrap();
    assert_eq!(first.enqueued_at_millis(), 30);
    assert_eq!(second.enqueued_at_millis(), 30);
    assert!(second.queue_id().value() > first.queue_id().value());
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 2);
}

#[test]
fn simultaneous_enqueues_allocate_unique_ids_without_lost_rows() {
    // Break caught: read-unlocked/write-later enqueue loses a peer or allocates
    // the same queue sequence twice.
    let fixture = open_queue();
    let store = Arc::new(fixture.store.clone());
    let barrier = Arc::new(Barrier::new(32));
    let handles = (1_u128..=32)
        .map(|number| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let id = format!("{number:032x}");
                let entry = queued(&store, &id, 100, owner(number as u32 + 100));
                barrier.wait();
                store.enqueue(entry)
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap().unwrap();
    }
    let snapshot = store.queue_snapshot().unwrap();
    assert_eq!(snapshot.entries().len(), 32);
    assert_eq!(snapshot.next_id().value(), 33);
    assert_eq!(
        snapshot
            .entries()
            .iter()
            .map(|entry| entry.queue_id().value())
            .collect::<Vec<_>>(),
        (1..=32).collect::<Vec<_>>()
    );
}

#[test]
fn replaced_queue_lock_cannot_split_the_authoritative_lock_domain() {
    // Break caught: queue/lock replacement creates two mutexes and concurrent
    // enqueues overwrite one another.
    let fixture = open_queue();
    fs::rename(
        fixture.root.join("queue/lock"),
        fixture.root.parent().unwrap().join("displaced-queue-lock"),
    )
    .unwrap();
    fs::write(fixture.root.join("queue/lock"), b"").unwrap();
    fs::set_permissions(
        fixture.root.join("queue/lock"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let reopened = ClientStateStore::open(&fixture.root).unwrap();
    let first = fixture.store.clone();
    let second = reopened.clone();
    let barrier = Arc::new(Barrier::new(2));
    let left = {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            first.enqueue(queued(
                &first,
                "00000000000000000000000000000040",
                200,
                owner(200),
            ))
        })
    };
    let right = {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            second.enqueue(queued(
                &second,
                "00000000000000000000000000000041",
                200,
                owner(201),
            ))
        })
    };
    left.join().unwrap().unwrap();
    right.join().unwrap().unwrap();
    assert_eq!(reopened.queue_snapshot().unwrap().entries().len(), 2);
}

#[test]
fn queue_symlink_fifo_and_device_backed_replacements_fail_without_blocking() {
    // Break caught: path-based reads follow a planted final entry or block on a
    // FIFO instead of rejecting non-regular state.
    for kind in ["symlink", "fifo", "device"] {
        let fixture = open_queue();
        let state = fixture.root.join("queue/state.json");
        fs::rename(&state, fixture.root.join(format!("saved-{kind}"))).unwrap();
        match kind {
            "symlink" => symlink(fixture.root.join("saved-symlink"), &state).unwrap(),
            "fifo" => {
                let path = std::ffi::CString::new(state.as_os_str().as_encoded_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
                assert!(fs::symlink_metadata(&state).unwrap().file_type().is_fifo());
            }
            "device" => symlink("/dev/null", &state).unwrap(),
            _ => unreachable!(),
        }
        let (sent, received) = mpsc::channel();
        let store = fixture.store.clone();
        thread::spawn(move || sent.send(store.queue_snapshot().is_err()).unwrap());
        assert!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            "kind {kind}"
        );
    }
}

#[test]
fn device_namespace_substituted_for_queue_is_rejected_on_reopen() {
    // Break caught: component open follows a replacement queue link outside the
    // local state root.
    let fixture = open_queue();
    fs::rename(fixture.root.join("queue"), fixture.root.join("saved-queue")).unwrap();
    symlink("/dev", fixture.root.join("queue")).unwrap();
    assert!(ClientStateStore::open(&fixture.root).is_err());
}

#[test]
fn interrupted_publication_retains_exactly_old_or_fully_published_state() {
    // Break caught: queue replacement writes in place and a crash exposes a
    // truncated or half-updated claim.
    for (point, dispatching) in [
        (ClientStateWritePoint::BeforePublish, false),
        (ClientStateWritePoint::AfterPublish, true),
    ] {
        let fixture = open_queue();
        let dispatcher = owner(300);
        fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000050",
                300,
                dispatcher,
            ))
            .unwrap();
        fixture.store.inject_write_failure_once(point);
        assert!(
            fixture
                .store
                .claim_next(dispatcher, &["mini-1".into()], 301)
                .is_err()
        );
        let reopened = ClientStateStore::open(&fixture.root).unwrap();
        let snapshot = reopened.queue_snapshot().unwrap();
        assert_eq!(
            matches!(
                snapshot.entries()[0].state(),
                QueueState::Dispatching { .. }
            ),
            dispatching
        );
    }
}

#[test]
fn crash_after_retirement_rename_keeps_published_queue_reopenable() {
    // Break caught: cleanup retirement is treated as transaction commit and
    // reopening loses queue state already durably published.
    let fixture = open_queue();
    let dispatcher = owner(310);
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000051",
            310,
            dispatcher,
        ))
        .unwrap();
    fixture
        .store
        .inject_write_failure_once(ClientStateWritePoint::CrashCleanupAfterOperationMoved);
    assert!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-1".into()], 311)
            .is_err()
    );

    let reopened = ClientStateStore::open(&fixture.root).unwrap();
    assert!(matches!(
        reopened.queue_snapshot().unwrap().entries()[0].state(),
        QueueState::Dispatching { selected_worker, .. } if selected_worker == "mini-1"
    ));
}

#[test]
fn claim_marks_oldest_eligible_waiting_entry_dispatching_without_removing_it() {
    // Break caught: claim pops before acceptance or bypasses the owner's older
    // row for the same worker.
    let fixture = open_queue();
    let dispatcher = owner(400);
    let first = queued(
        &fixture.store,
        "00000000000000000000000000000060",
        400,
        dispatcher,
    );
    let second = queued(
        &fixture.store,
        "00000000000000000000000000000061",
        401,
        dispatcher,
    );
    fixture.store.enqueue(first.clone()).unwrap();
    fixture.store.enqueue(second).unwrap();
    let claim = fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 403)
        .unwrap()
        .unwrap();

    assert_eq!(claim.entry().job_id(), first.job_id());
    assert!(matches!(
        claim.entry().state(),
        QueueState::Dispatching { selected_worker, .. } if selected_worker == "mini-1"
    ));
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 2);
}

#[test]
fn claims_are_owner_scoped_and_an_older_live_owner_wins_for_same_worker() {
    // Break caught: a dispatcher steals another process's row or bypasses an
    // older live-owned row eligible for the same idle worker.
    let fixture = open_queue();
    let older_owner = owner(410);
    let younger_owner = owner(411);
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000062",
            410,
            older_owner,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000063",
            411,
            younger_owner,
        ))
        .unwrap();

    assert!(
        fixture
            .store
            .claim_next(younger_owner, &["mini-1".into()], 412)
            .unwrap()
            .is_none()
    );
    let older = fixture
        .store
        .claim_next(older_owner, &["mini-1".into()], 413)
        .unwrap()
        .unwrap();
    assert_eq!(
        older.entry().job_id().to_string(),
        "00000000000000000000000000000062"
    );
    assert!(
        fixture
            .store
            .claim_next(owner(999), &["mini-2".into()], 414)
            .unwrap()
            .is_none()
    );
}

#[test]
fn busy_pinned_head_does_not_block_younger_row_for_another_worker() {
    // Break caught: queue-global head-of-line blocking prevents mini-2 work just
    // because the oldest row is pinned to busy mini-1.
    let fixture = open_queue();
    let pinned_owner = owner(420);
    let younger_owner = owner(421);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000064",
            420,
            pinned_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000065",
            421,
            younger_owner,
        ))
        .unwrap();
    let claim = fixture
        .store
        .claim_next(younger_owner, &["mini-2".into()], 422)
        .unwrap()
        .unwrap();
    assert_eq!(
        claim.entry().job_id().to_string(),
        "00000000000000000000000000000065"
    );
}

#[test]
fn capability_ineligible_head_blocks_no_unrelated_worker() {
    // Break caught: FIFO compares age without checking the older row's actual
    // capability eligibility for that worker.
    let fixture = open_queue();
    cache_observation(&fixture.store, "mini-2", &["rust"], 430);
    let blocked_owner = owner(430);
    let younger_owner = owner(431);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000066",
            430,
            blocked_owner,
            WorkerPreference::Automatic,
            vec!["docker".into()],
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000067",
            431,
            younger_owner,
        ))
        .unwrap();
    let claim = fixture
        .store
        .claim_next(younger_owner, &["mini-2".into()], 432)
        .unwrap()
        .unwrap();
    assert_eq!(
        claim.entry().job_id().to_string(),
        "00000000000000000000000000000067"
    );
}

#[test]
fn two_slot_ceiling_admits_two_dispatching_rows_on_one_worker() {
    let fixture = open_queue();
    let first = owner(900);
    let second = owner(901);
    let third = owner(902);
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000090",
            900,
            first,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000091",
            901,
            second,
        ))
        .unwrap();
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000092",
            902,
            third,
        ))
        .unwrap();
    let mut ceilings = BTreeMap::new();
    ceilings.insert("mini-1".into(), 2);
    assert!(
        fixture
            .store
            .claim_next_with_slot_ceilings(first, &["mini-1".into()], 910, &ceilings)
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .store
            .claim_next_with_slot_ceilings(second, &["mini-1".into()], 911, &ceilings)
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .store
            .claim_next_with_slot_ceilings(third, &["mini-1".into()], 912, &ceilings)
            .unwrap()
            .is_none()
    );
    let dispatching = fixture
        .store
        .queue_snapshot()
        .unwrap()
        .entries()
        .iter()
        .filter(|entry| {
            matches!(
                entry.state(),
                QueueState::Dispatching {
                    selected_worker, ..
                } if selected_worker == "mini-1"
            )
        })
        .count();
    assert_eq!(dispatching, 2);
}

#[test]
fn three_rows_dispatch_only_to_three_distinct_workers() {
    // Break caught: dispatch state reserves no worker slot and persists two
    // in-flight rows for one single-slot worker.
    let fixture = open_queue();
    for (index, worker) in ["mini-1", "mini-2", "mini-3"].into_iter().enumerate() {
        let dispatcher = owner(440 + index as u32);
        let id = format!("{:032x}", 0x70 + index);
        fixture
            .store
            .enqueue(queued(&fixture.store, &id, 440 + index as u64, dispatcher))
            .unwrap();
        assert!(
            fixture
                .store
                .claim_next(dispatcher, &[worker.into()], 450 + index as u64)
                .unwrap()
                .is_some()
        );
    }
    let fourth_owner = owner(449);
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000079",
            449,
            fourth_owner,
        ))
        .unwrap();
    assert!(
        fixture
            .store
            .claim_next(fourth_owner, &["mini-1".into()], 460)
            .unwrap()
            .is_none()
    );
    let workers = fixture
        .store
        .queue_snapshot()
        .unwrap()
        .entries()
        .iter()
        .filter_map(|entry| match entry.state() {
            QueueState::Dispatching {
                selected_worker, ..
            } => Some(selected_worker.clone()),
            QueueState::Waiting { .. } => None,
            QueueState::Parked => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(workers, ["mini-1", "mini-2", "mini-3"]);
}

#[test]
fn waiting_task_turn_owner_can_be_adopted() {
    // Break caught: Phase 5 cannot hand a waiting turn to its replacement
    // runner without changing FIFO identity.
    let fixture = open_queue();
    let initial = owner(470);
    let adopted = owner(471);
    let task = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000080",
            470,
            initial,
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run_reference("run-80", 2)),
        ))
        .unwrap();
    let adopted_entry = fixture.store.adopt_row(task.job_id(), adopted).unwrap();
    assert_eq!(*adopted_entry.owner(), adopted);
    assert_eq!(*adopted_entry.enqueue_owner(), initial);
    assert!(
        fixture
            .store
            .claim_next(initial, &["mini-1".into()], 471)
            .unwrap()
            .is_none()
    );
    fixture
        .store
        .claim_next(adopted, &["mini-1".into()], 472)
        .unwrap()
        .unwrap();
}

#[test]
fn dispatching_task_turn_adoption_preserves_reservation_and_cancellation() {
    // Break caught: a replacement runner cannot take ownership of an in-flight
    // task turn, or adoption loses the exact worker/claim/cancel state.
    let fixture = open_queue();
    let initial = owner(472);
    let replacement = owner(473);
    let task = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000081",
            473,
            initial,
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run_reference("run-81", 2)),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(initial, &["mini-1".into()], 474)
        .unwrap()
        .unwrap();
    fixture
        .store
        .request_queue_cancel(task.job_id(), 475)
        .unwrap()
        .unwrap();

    let adopted = fixture.store.adopt_row(task.job_id(), replacement).unwrap();

    assert_eq!(*adopted.enqueue_owner(), initial);
    assert_eq!(adopted.cancel_requested_at_millis(), Some(475));
    assert!(matches!(
        adopted.state(),
        QueueState::Dispatching {
            dispatch_owner,
            selected_worker,
            claimed_at_millis,
        } if *dispatch_owner == replacement
            && selected_worker == "mini-1"
            && *claimed_at_millis == 474
    ));
    assert!(
        fixture
            .store
            .remove_after_terminal(task.job_id(), initial)
            .is_err()
    );
}

#[test]
fn batch_ownership_is_immutable_while_waiting_or_dispatching() {
    // Break caught: the task-turn adoption rule is accidentally generalized to
    // batch rows in either persistent state.
    let waiting_fixture = open_queue();
    let initial = owner(474);
    let replacement = owner(475);
    let waiting = waiting_fixture
        .store
        .enqueue(queued(
            &waiting_fixture.store,
            "00000000000000000000000000000082",
            476,
            initial,
        ))
        .unwrap();
    assert!(
        waiting_fixture
            .store
            .adopt_row(waiting.job_id(), replacement)
            .is_err()
    );
    assert_eq!(
        *waiting_fixture.store.queue_snapshot().unwrap().entries()[0].owner(),
        initial
    );

    let dispatching_fixture = open_queue();
    let dispatching = dispatching_fixture
        .store
        .enqueue(queued(
            &dispatching_fixture.store,
            "00000000000000000000000000000083",
            477,
            initial,
        ))
        .unwrap();
    dispatching_fixture
        .store
        .claim_next(initial, &["mini-1".into()], 478)
        .unwrap()
        .unwrap();
    assert!(
        dispatching_fixture
            .store
            .adopt_row(dispatching.job_id(), replacement)
            .is_err()
    );
    assert!(matches!(
        dispatching_fixture.store.queue_snapshot().unwrap().entries()[0].state(),
        QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == initial
    ));
}

#[test]
fn preacceptance_abandonment_requires_matching_request_row_owner_and_state() {
    // Break caught: an unrelated caller, unclaimed row, or request for another
    // durable job can manufacture an abandonment proof.
    let fixture = open_queue();
    let dispatcher = owner(476);
    let row = fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "000000000000000000000000000000b1",
            479,
            dispatcher,
        ))
        .unwrap();
    let request = install_abandonment_request(&fixture, row.job_id(), "mini-1");
    let receipt = remote_abandonment_receipt(&request);
    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt, dispatcher)
            .is_err()
    );
    fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 480)
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt, owner(477))
            .is_err()
    );
    let missing_request = install_abandonment_request(
        &fixture,
        "000000000000000000000000000000b2".parse().unwrap(),
        "mini-1",
    );
    let missing_receipt = remote_abandonment_receipt(&missing_request);
    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&missing_receipt, dispatcher)
            .is_err()
    );
    let error = fixture
        .store
        .remove_after_terminal(row.job_id(), dispatcher)
        .unwrap_err();
    assert_terminal_unproven(error);
    assert!(
        fixture.store.queue_snapshot().unwrap().entries()[0]
            .preacceptance_abandonment_proof()
            .is_none()
    );
}

#[test]
fn remote_abandonment_receipt_for_a_cannot_authorize_valid_row_b() {
    // Break caught: a caller observes an actual Abandoned outcome for A, then
    // supplies B's freely chosen request to manufacture authority for B.
    let fixture = open_queue();
    let dispatcher = owner(475);
    let row_b = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000af",
        dispatcher,
        475,
    );
    install_abandonment_request(&fixture, row_b.job_id(), "mini-1");

    let record_a = local_record(
        &fixture.store,
        "000000000000000000000000000000ae".parse().unwrap(),
        "mini-1",
        None,
    );
    let request_a = ResolveOrAbandonRequest::from_local_record(&record_a).unwrap();
    let receipt_a = remote_abandonment_receipt(&request_a);

    let before = fixture.store.queue_snapshot().unwrap();
    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt_a, dispatcher)
            .is_err()
    );
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
    assert!(
        fixture.store.queue_snapshot().unwrap().entries()[0]
            .preacceptance_abandonment_proof()
            .is_none()
    );

    let debug = format!("{receipt_a:?}");
    for secret in [COMMAND_SECRET, PATH_SECRET, DIGEST, TOKEN_SECRET] {
        assert!(!debug.contains(secret), "receipt debug leaked {secret}");
    }
}

#[test]
fn abandonment_receipt_rejects_every_positive_status_and_remote_uncertainty() {
    // Break caught: an already accepted/running/terminal or remotely uncertain
    // local record can be mislabeled pre-acceptance-abandoned and retired.
    let cases = vec![
        (
            "uploading",
            Some(
                JobStatus::new(
                    JobState::Uploading,
                    1_100,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ),
            RemoteUncertainty::None,
        ),
        (
            "verified",
            Some(
                JobStatus::new(
                    JobState::Verified,
                    1_101,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ),
            RemoteUncertainty::None,
        ),
        (
            "accepted",
            Some(JobStatus::accepted(1_102).unwrap()),
            RemoteUncertainty::None,
        ),
        (
            "running",
            Some(JobStatus::running(1_103, 11, 12, 13, 14).unwrap()),
            RemoteUncertainty::None,
        ),
        (
            "terminal",
            Some(JobStatus::succeeded(1_104, 1, 1).unwrap()),
            RemoteUncertainty::None,
        ),
        (
            "unknown remote",
            None,
            RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
        ),
        (
            "cleanup pending",
            None,
            RemoteUncertainty::cleanup_pending("CLEANUP_INCOMPLETE").unwrap(),
        ),
    ];

    for (index, (label, status, uncertainty)) in cases.into_iter().enumerate() {
        let fixture = open_queue();
        let dispatcher = owner(700 + index as u32);
        let job_id: JobId = format!("000000000000000000000000000001{index:02x}")
            .parse()
            .unwrap();
        dispatching_batch(
            &fixture.store,
            &job_id.to_string(),
            dispatcher,
            700 + index as u64 * 2,
        );
        let base = local_record(&fixture.store, job_id, "mini-1", None);
        let request = ResolveOrAbandonRequest::from_local_record(&base).unwrap();
        let receipt = remote_abandonment_receipt(&request);
        fixture
            .store
            .create_job(local_record_with_uncertainty(
                &fixture.store,
                job_id,
                "mini-1",
                status,
                uncertainty,
            ))
            .unwrap();
        let before = fixture.store.queue_snapshot().unwrap();

        assert!(
            fixture
                .store
                .record_preacceptance_abandoned(&receipt, dispatcher)
                .is_err(),
            "accepted invalid abandonment authority for {label}"
        );
        assert_eq!(fixture.store.queue_snapshot().unwrap(), before, "{label}");
    }
}

#[test]
fn proven_preacceptance_abandonment_is_exactly_idempotent_and_can_be_retired() {
    // Break caught: retries can replace a proof with a different request
    // fingerprint or cannot durably retire a verified abandonment.
    let fixture = open_queue();
    let dispatcher = owner(478);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000b3",
        dispatcher,
        481,
    );
    let request = install_abandonment_request(&fixture, row.job_id(), "mini-1");
    let receipt = remote_abandonment_receipt(&request);

    let marked = fixture
        .store
        .record_preacceptance_abandoned(&receipt, dispatcher)
        .unwrap();
    let proof = marked.preacceptance_abandonment_proof().unwrap();
    assert_eq!(proof.job_id(), row.job_id());
    assert_eq!(proof.recorded_by(), &dispatcher);
    assert_eq!(proof.request_fingerprint(), request.request_fingerprint());
    assert_eq!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt, dispatcher)
            .unwrap(),
        marked
    );

    let changed_record = local_record_with_binding(
        &fixture.store,
        row.job_id(),
        "mini-1",
        PROJECT_ID,
        WORKTREE_ID,
        CommandSpec::argv(vec!["tool".into(), "DIFFERENT_COMMAND_SECRET".into()]).unwrap(),
        None,
        RemoteUncertainty::None,
    );
    install_local_record_as(&fixture, row.job_id(), &changed_record);
    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt, dispatcher)
            .is_err()
    );
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries(), &[marked]);

    assert_eq!(
        fixture
            .store
            .remove_after_terminal(row.job_id(), dispatcher)
            .unwrap()
            .job_id(),
        row.job_id()
    );
    assert!(fixture.store.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn one_dispatcher_cannot_apply_one_resolution_to_its_other_live_row() {
    // Break caught: a dispatcher with two reservations can apply A's remote
    // abandonment result to B merely because both rows have the same owner.
    let fixture = open_queue();
    let dispatcher = owner(483);
    let run_a = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000c1",
            481,
            dispatcher,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    let run_b = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000c2",
            482,
            dispatcher,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            Vec::new(),
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 483)
        .unwrap()
        .unwrap();
    fixture
        .store
        .claim_next(dispatcher, &["mini-2".into()], 484)
        .unwrap()
        .unwrap();
    let request_a = install_abandonment_request(&fixture, run_a.job_id(), "mini-1");
    install_abandonment_request(&fixture, run_b.job_id(), "mini-2");
    let receipt_a = remote_abandonment_receipt(&request_a);

    fixture
        .store
        .record_preacceptance_abandoned(&receipt_a, dispatcher)
        .unwrap();

    let snapshot = fixture.store.queue_snapshot().unwrap();
    assert!(
        snapshot
            .entries()
            .iter()
            .find(|entry| entry.job_id() == run_a.job_id())
            .unwrap()
            .preacceptance_abandonment_proof()
            .is_some()
    );
    assert!(
        snapshot
            .entries()
            .iter()
            .find(|entry| entry.job_id() == run_b.job_id())
            .unwrap()
            .preacceptance_abandonment_proof()
            .is_none()
    );
    let mut copied: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.root.join("queue/state.json")).unwrap()).unwrap();
    let proof_a = copied["entries"][0]["preacceptance_abandonment_proof"].clone();
    copied["entries"][1]["preacceptance_abandonment_proof"] = proof_a;
    assert!(serde_json::from_value::<QueueSnapshot>(copied).is_err());
    assert_terminal_unproven(
        fixture
            .store
            .remove_after_terminal(run_b.job_id(), dispatcher)
            .unwrap_err(),
    );
    assert!(
        fixture
            .store
            .queue_snapshot()
            .unwrap()
            .entries()
            .iter()
            .any(|entry| entry.job_id() == run_b.job_id())
    );
}

#[test]
fn abandonment_rejects_mismatched_local_request_fingerprint_or_queue_binding() {
    // Break caught: a request that does not equal the rooted local record, or a
    // locally consistent request for metadata unlike the queue row, is trusted.
    let missing_fixture = open_queue();
    let dispatcher = owner(484);
    let row = dispatching_batch(
        &missing_fixture.store,
        "000000000000000000000000000000c0",
        dispatcher,
        483,
    );
    let unpersisted_record = local_record(&missing_fixture.store, row.job_id(), "mini-1", None);
    let unpersisted_request =
        ResolveOrAbandonRequest::from_local_record(&unpersisted_record).unwrap();
    let unpersisted_receipt = remote_abandonment_receipt(&unpersisted_request);
    let before = missing_fixture.store.queue_snapshot().unwrap();
    assert!(
        missing_fixture
            .store
            .record_preacceptance_abandoned(&unpersisted_receipt, dispatcher)
            .is_err()
    );
    assert_eq!(missing_fixture.store.queue_snapshot().unwrap(), before);

    let fingerprint_fixture = open_queue();
    let row = dispatching_batch(
        &fingerprint_fixture.store,
        "000000000000000000000000000000c3",
        dispatcher,
        485,
    );
    let record = local_record(&fingerprint_fixture.store, row.job_id(), "mini-1", None);
    fingerprint_fixture.store.create_job(record).unwrap();
    let mismatched_record = local_record_with_binding(
        &fingerprint_fixture.store,
        row.job_id(),
        "mini-1",
        PROJECT_ID,
        WORKTREE_ID,
        CommandSpec::argv(vec!["tool".into(), "OTHER_SAME_SUMMARY_SECRET".into()]).unwrap(),
        None,
        RemoteUncertainty::None,
    );
    let mismatched_request =
        ResolveOrAbandonRequest::from_local_record(&mismatched_record).unwrap();
    let mismatched_receipt = remote_abandonment_receipt(&mismatched_request);
    let before = fingerprint_fixture.store.queue_snapshot().unwrap();
    assert!(
        fingerprint_fixture
            .store
            .record_preacceptance_abandoned(&mismatched_receipt, dispatcher)
            .is_err()
    );
    assert_eq!(fingerprint_fixture.store.queue_snapshot().unwrap(), before);

    let binding_fixture = open_queue();
    let row = dispatching_batch(
        &binding_fixture.store,
        "000000000000000000000000000000c4",
        dispatcher,
        487,
    );
    let mismatched_record = local_record_with_binding(
        &binding_fixture.store,
        row.job_id(),
        "mini-1",
        OTHER_PROJECT_ID,
        WORKTREE_ID,
        CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
        None,
        RemoteUncertainty::None,
    );
    binding_fixture
        .store
        .create_job(mismatched_record.clone())
        .unwrap();
    let mismatched_request =
        ResolveOrAbandonRequest::from_local_record(&mismatched_record).unwrap();
    let mismatched_receipt = remote_abandonment_receipt(&mismatched_request);
    let before = binding_fixture.store.queue_snapshot().unwrap();
    assert!(
        binding_fixture
            .store
            .record_preacceptance_abandoned(&mismatched_receipt, dispatcher)
            .is_err()
    );
    assert_eq!(binding_fixture.store.queue_snapshot().unwrap(), before);
}

#[test]
fn abandonment_receipt_requires_the_exact_durable_resolution_request() {
    // Break caught: checking only the public request fingerprint/row summary
    // misses canonical local corruption in a secret request field.
    let fixture = open_queue();
    let dispatcher = owner(489);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000c5",
        dispatcher,
        489,
    );
    let record = local_record(&fixture.store, row.job_id(), "mini-1", None);
    fixture.store.create_job(record.clone()).unwrap();
    let request = ResolveOrAbandonRequest::from_local_record(&record).unwrap();
    let receipt = remote_abandonment_receipt(&request);

    let changed_token = LocalJobRecord::new(
        record.meta().clone(),
        LeaseToken::new("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".parse().unwrap()),
        None,
        RemoteUncertainty::None,
    )
    .unwrap();
    install_local_record_as(&fixture, row.job_id(), &changed_token);
    let before = fixture.store.queue_snapshot().unwrap();

    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt, dispatcher)
            .is_err()
    );
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
}

#[test]
fn preacceptance_abandonment_proof_is_row_bound_canonical_strict_and_private() {
    // Break caught: the queue-local proof accepts ambiguous extension fields,
    // fails to bind a row dimension, leaks request secrets, or exists on Waiting.
    let fixture = open_queue();
    let dispatcher = owner(479);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000b4",
        dispatcher,
        489,
    );
    let request = install_abandonment_request(&fixture, row.job_id(), "mini-1");
    let receipt = remote_abandonment_receipt(&request);
    fixture
        .store
        .record_preacceptance_abandoned(&receipt, dispatcher)
        .unwrap();
    let path = fixture.root.join("queue/state.json");
    let text = String::from_utf8(fs::read(&path).unwrap()).unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    let proof = &value["entries"][0]["preacceptance_abandonment_proof"];
    let fields = proof.as_object().unwrap();
    assert_eq!(fields.len(), 10);
    assert_eq!(proof["queue_id"], serde_json::json!(1));
    assert_eq!(proof["job_id"], serde_json::json!(row.job_id()));
    assert_eq!(
        proof["client_id"],
        serde_json::json!(fixture.store.client_id())
    );
    assert_eq!(proof["selected_worker"], serde_json::json!("mini-1"));
    assert_eq!(proof["project_id"], serde_json::json!(PROJECT_ID));
    assert_eq!(proof["worktree_id"], serde_json::json!(WORKTREE_ID));
    assert_eq!(
        proof["command_summary"],
        serde_json::json!({"mode": "argv", "arg_count": 2})
    );
    assert_eq!(
        proof["request_fingerprint"],
        serde_json::json!(request.request_fingerprint())
    );
    assert_eq!(proof["claimed_at_millis"], serde_json::json!(490));
    assert_eq!(proof["recorded_by"], serde_json::json!(dispatcher));
    for forbidden in [
        "lease_token",
        "manifest_digest",
        "relative_working_dir",
        "resource_class",
        "command",
    ] {
        assert!(
            !fields.contains_key(forbidden),
            "proof retained {forbidden}"
        );
    }
    for secret in [COMMAND_SECRET, PATH_SECRET, DIGEST, TOKEN_SECRET] {
        assert!(!text.contains(secret), "abandonment proof leaked {secret}");
    }

    let mut unknown = value.clone();
    unknown["entries"][0]["preacceptance_abandonment_proof"]["unknown"] = serde_json::json!(true);
    assert!(serde_json::from_value::<QueueSnapshot>(unknown).is_err());

    for (field, replacement) in [
        ("queue_id", serde_json::json!(99)),
        (
            "job_id",
            serde_json::json!("000000000000000000000000000000ff"),
        ),
        (
            "client_id",
            serde_json::json!("000000000000000000000000000000fe"),
        ),
        ("selected_worker", serde_json::json!("mini-2")),
        ("project_id", serde_json::json!(OTHER_PROJECT_ID)),
        ("worktree_id", serde_json::json!(OTHER_WORKTREE_ID)),
        ("command_summary", serde_json::json!({"mode": "shell"})),
        ("claimed_at_millis", serde_json::json!(491)),
        ("recorded_by", serde_json::json!(owner(999))),
    ] {
        let mut mismatched = value.clone();
        mismatched["entries"][0]["preacceptance_abandonment_proof"][field] = replacement;
        assert!(
            serde_json::from_value::<QueueSnapshot>(mismatched).is_err(),
            "accepted mismatched proof field {field}"
        );
    }

    let mut waiting = value;
    waiting["entries"][0]["state"] = serde_json::json!({
        "state": "waiting",
        "owner": dispatcher,
    });
    assert!(serde_json::from_value::<QueueSnapshot>(waiting).is_err());
}

#[test]
fn dead_batch_recovery_retires_persisted_abandonment_instead_of_reclaiming_it() {
    // Break caught: a crash after proof publication lets recovery revert the
    // abandoned batch to Waiting and execute it again.
    let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Absent,
    });
    let dispatcher = owner(480);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000b6",
        dispatcher,
        486,
    );
    let request = install_abandonment_request(&fixture, row.job_id(), "mini-1");
    let receipt = remote_abandonment_receipt(&request);
    fixture
        .store
        .inject_write_failure_once(ClientStateWritePoint::AfterPublish);
    assert!(
        fixture
            .store
            .record_preacceptance_abandoned(&receipt, dispatcher)
            .is_err()
    );

    let reopened = ClientStateStore::open_with_owner_inspector(
        &fixture.root,
        FixedOwnerInspector {
            observation: ProcessObservation::Absent,
        },
    )
    .unwrap();
    let persisted = reopened.queue_snapshot().unwrap();
    assert_eq!(
        persisted.entries()[0]
            .preacceptance_abandonment_proof()
            .unwrap()
            .recorded_by(),
        &dispatcher
    );
    assert_eq!(
        reopened.recover_dead_dispatches().unwrap(),
        vec![row.job_id()]
    );
    assert!(reopened.queue_snapshot().unwrap().entries().is_empty());
    assert!(
        reopened
            .claim_next(dispatcher, &["mini-1".into()], 488)
            .unwrap()
            .is_none()
    );
}

#[test]
fn abandoned_task_turn_proof_survives_dispatch_adoption_until_retirement() {
    // Break caught: task-turn reownership erases abandonment proof or reverts
    // the row into runnable work after the original runner dies.
    let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Absent,
    });
    let initial = owner(481);
    let replacement = owner(482);
    let task = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "000000000000000000000000000000b7",
            489,
            initial,
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run_reference("run-b7", 2)),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(initial, &["mini-1".into()], 490)
        .unwrap()
        .unwrap();
    fixture
        .store
        .request_queue_cancel(task.job_id(), 491)
        .unwrap()
        .unwrap();
    let request = install_abandonment_request(&fixture, task.job_id(), "mini-1");
    let receipt = remote_abandonment_receipt(&request);
    fixture
        .store
        .record_preacceptance_abandoned(&receipt, initial)
        .unwrap();

    assert!(fixture.store.recover_dead_dispatches().unwrap().is_empty());
    let adopted = fixture.store.adopt_row(task.job_id(), replacement).unwrap();
    assert_eq!(
        adopted
            .preacceptance_abandonment_proof()
            .unwrap()
            .recorded_by(),
        &initial
    );
    assert_eq!(adopted.cancel_requested_at_millis(), Some(491));
    assert!(matches!(
        adopted.state(),
        QueueState::Dispatching {
            dispatch_owner,
            selected_worker,
            claimed_at_millis,
        } if *dispatch_owner == replacement
            && selected_worker == "mini-1"
            && *claimed_at_millis == 490
    ));
    assert!(
        fixture
            .store
            .revert_dispatch(task.job_id(), replacement)
            .is_err()
    );
    fixture
        .store
        .remove_after_terminal(task.job_id(), replacement)
        .unwrap();
    assert!(fixture.store.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn dead_batch_dispatch_is_first_persisted_waiting_and_pid_reuse_is_dead() {
    // Break caught: a dead or PID-reused dispatcher deletes its durable row,
    // losing the original identity needed for remote resolution.
    for observation in [ProcessObservation::Absent, ProcessObservation::Reused] {
        let fixture = open_queue_with_owner_inspector(FixedOwnerInspector { observation });
        let dispatcher = owner(480);
        let row = fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000082",
                480,
                dispatcher,
            ))
            .unwrap();
        fixture
            .store
            .claim_next(dispatcher, &["mini-1".into()], 481)
            .unwrap()
            .unwrap();

        assert_eq!(
            fixture.store.recover_dead_dispatches().unwrap(),
            vec![row.job_id()]
        );
        let snapshot = fixture.store.queue_snapshot().unwrap();
        assert_eq!(snapshot.entries().len(), 1);
        assert!(
            matches!(snapshot.entries()[0].state(), QueueState::Waiting { owner } if *owner == dispatcher)
        );
    }
}

#[test]
fn dead_batch_recovery_reverts_statusless_publication_and_frees_the_reservation() {
    // Break caught: a crash after local-record publication leaves the row
    // Dispatching, so the selected worker stays reserved forever.
    let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Absent,
    });
    let dispatcher = owner(609);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000bf",
        dispatcher,
        1_000,
    );
    fixture
        .store
        .create_job(local_record(&fixture.store, row.job_id(), "mini-1", None))
        .unwrap();

    assert_eq!(
        fixture.store.recover_dead_dispatches().unwrap(),
        vec![row.job_id()]
    );
    let snapshot = fixture.store.queue_snapshot().unwrap();
    assert_eq!(snapshot.entries().len(), 1);
    assert!(matches!(
        snapshot.entries()[0].state(),
        QueueState::Waiting { owner } if *owner == dispatcher
    ));
    let claimed = fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 1_002)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.entry().job_id(), row.job_id());
    assert!(matches!(
        claimed.entry().state(),
        QueueState::Dispatching {
            selected_worker,
            ..
        } if selected_worker == "mini-1"
    ));
}

#[test]
fn dead_batch_recovery_preserves_bound_remote_evidence_and_uncertainty() {
    // Break caught: generic dead-owner recovery demotes an accepted or
    // ambiguous remote job to Waiting, allowing its dispatch reservation to be
    // reused before reconciliation proves a terminal outcome.
    let cases = vec![
        (
            "000000000000000000000000000000c0",
            Some(JobStatus::accepted(1_010).unwrap()),
            RemoteUncertainty::None,
        ),
        (
            "000000000000000000000000000000c1",
            Some(JobStatus::running(1_011, 21, 22, 23, 24).unwrap()),
            RemoteUncertainty::None,
        ),
        (
            "000000000000000000000000000000c2",
            None,
            RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
        ),
        (
            "000000000000000000000000000000c3",
            None,
            RemoteUncertainty::cleanup_pending("CLEANUP_INCOMPLETE").unwrap(),
        ),
    ];

    for (index, (job_id, status, uncertainty)) in cases.into_iter().enumerate() {
        let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
            observation: ProcessObservation::Absent,
        });
        let dispatcher = owner(610 + index as u32);
        let row = dispatching_batch(&fixture.store, job_id, dispatcher, 1_000 + index as u64);
        fixture
            .store
            .create_job(local_record_with_uncertainty(
                &fixture.store,
                row.job_id(),
                "mini-1",
                status,
                uncertainty,
            ))
            .unwrap();
        let before = fixture.store.queue_snapshot().unwrap();

        assert!(fixture.store.recover_dead_dispatches().unwrap().is_empty());
        assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
    }
}

#[test]
fn dead_batch_recovery_retires_a_bound_terminal_record() {
    // Break caught: making recovery conservative also strands a row whose
    // exact bound local record already proves safe terminal completion.
    let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Absent,
    });
    let dispatcher = owner(620);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000c4",
        dispatcher,
        1_020,
    );
    fixture
        .store
        .create_job(local_record(
            &fixture.store,
            row.job_id(),
            "mini-1",
            Some(JobStatus::succeeded(1_021, 10, 20).unwrap()),
        ))
        .unwrap();

    assert_eq!(
        fixture.store.recover_dead_dispatches().unwrap(),
        vec![row.job_id()]
    );
    assert!(fixture.store.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn dead_batch_recovery_fails_closed_on_a_mismatched_local_record() {
    // Break caught: recovery trusts a job filename without binding its durable
    // identity to the queue row, and silently reopens or retires corrupt work.
    let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Absent,
    });
    let dispatcher = owner(621);
    let row = dispatching_batch(
        &fixture.store,
        "000000000000000000000000000000c5",
        dispatcher,
        1_030,
    );
    fixture
        .store
        .create_job(local_record_with_binding(
            &fixture.store,
            row.job_id(),
            "mini-1",
            OTHER_PROJECT_ID,
            WORKTREE_ID,
            CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
            Some(JobStatus::accepted(1_031).unwrap()),
            RemoteUncertainty::None,
        ))
        .unwrap();
    let before = fixture.store.queue_snapshot().unwrap();

    let error = fixture.store.recover_dead_dispatches().unwrap_err();
    assert!(matches!(
        error,
        WorkerError::Queue {
            code: "QUEUE_JOB_RECORD_MISMATCH",
            ..
        }
    ));
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
}

#[test]
fn live_or_ambiguous_owner_is_never_recovered_as_abandoned() {
    // Break caught: uncertainty is interpreted as death and mutates a row still
    // owned by a possibly live dispatcher.
    for observation in [
        ProcessObservation::Matching { process_group: 1 },
        ProcessObservation::Ambiguous,
    ] {
        let fixture = open_queue_with_owner_inspector(FixedOwnerInspector { observation });
        let dispatcher = owner(490);
        fixture
            .store
            .enqueue(queued(
                &fixture.store,
                "00000000000000000000000000000083",
                490,
                dispatcher,
            ))
            .unwrap();
        fixture
            .store
            .claim_next(dispatcher, &["mini-1".into()], 491)
            .unwrap()
            .unwrap();
        let before = fixture.store.queue_snapshot().unwrap();
        assert!(fixture.store.recover_dead_dispatches().unwrap().is_empty());
        assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
    }
}

#[test]
fn dead_owner_task_turn_survives_recovery_unchanged_for_reownership() {
    // Break caught: the batch reaper consumes a dead task-turn row that Phase 5
    // must adopt using its durable task record.
    let fixture = open_queue_with_owner_inspector(FixedOwnerInspector {
        observation: ProcessObservation::Absent,
    });
    let dispatcher = owner(500);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000084",
            500,
            dispatcher,
            WorkerPreference::Automatic,
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run_reference("run-84", 1)),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 501)
        .unwrap()
        .unwrap();
    let before = fixture.store.queue_snapshot().unwrap();
    assert!(fixture.store.recover_dead_dispatches().unwrap().is_empty());
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
}

#[test]
fn lease_busy_and_failed_preacceptance_revert_exact_dispatch_to_waiting() {
    // Break caught: a failed attempt re-enqueues a row, loses FIFO identity, or
    // lets a non-owner revert another dispatcher.
    let fixture = open_queue();
    let dispatcher = owner(510);
    let row = fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000085",
            510,
            dispatcher,
        ))
        .unwrap();
    fixture
        .store
        .claim_next(dispatcher, &["mini-1".into()], 511)
        .unwrap()
        .unwrap();
    assert!(
        fixture
            .store
            .revert_dispatch(row.job_id(), owner(999))
            .is_err()
    );
    let reverted = fixture
        .store
        .revert_dispatch(row.job_id(), dispatcher)
        .unwrap();
    assert_eq!(reverted.queue_id(), row.queue_id());
    assert!(matches!(reverted.state(), QueueState::Waiting { owner } if *owner == dispatcher));
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);

    fixture
        .store
        .claim_next(dispatcher, &["mini-2".into()], 512)
        .unwrap()
        .unwrap();
    let again = fixture
        .store
        .revert_dispatch(row.job_id(), dispatcher)
        .unwrap();
    assert_eq!(again.queue_id(), row.queue_id());
}

#[test]
fn cancel_racing_claim_is_removed_waiting_or_durably_requested_dispatch() {
    // Break caught: cancel/claim interleaving loses the flag or returns a
    // dispatch target after deleting its row.
    for iteration in 0_u128..32 {
        let fixture = open_queue();
        let store = Arc::new(fixture.store.clone());
        let dispatcher = owner(520 + iteration as u32);
        let job_id: JobId = format!("{:032x}", 0x100 + iteration).parse().unwrap();
        store
            .enqueue(queued(&store, &job_id.to_string(), 520, dispatcher))
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let claim = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store.claim_next(dispatcher, &["mini-1".into()], 521)
            })
        };
        let cancel = {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store.request_queue_cancel(job_id, 522)
            })
        };
        let claim = claim.join().unwrap().unwrap();
        let cancel = cancel.join().unwrap().unwrap().unwrap();
        let snapshot = store.queue_snapshot().unwrap();
        match cancel {
            QueueCancel::RemovedWaiting { job_id: removed } => {
                assert_eq!(removed, job_id);
                assert!(claim.is_none());
                assert!(snapshot.entries().is_empty());
            }
            QueueCancel::RequestedDispatch {
                job_id: requested,
                dispatch_owner,
            } => {
                assert_eq!(requested, job_id);
                assert_eq!(dispatch_owner, dispatcher);
                assert!(claim.is_some());
                assert_eq!(snapshot.entries().len(), 1);
                assert!(snapshot.entries()[0].is_cancel_requested());
            }
        }
    }
}

#[test]
fn waiting_remove_and_dispatch_removal_are_state_and_owner_scoped() {
    // Break caught: waiting cleanup deletes an in-flight row or a stale process
    // removes a claim owned by another dispatcher.
    let fixture = open_queue();
    let dispatcher = owner(560);
    let waiting = fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000086",
            560,
            dispatcher,
        ))
        .unwrap();
    assert_eq!(
        fixture.store.remove_queued(waiting.job_id()).unwrap(),
        Some(waiting)
    );

    let dispatching = dispatching_batch(
        &fixture.store,
        "00000000000000000000000000000087",
        dispatcher,
        561,
    );
    assert!(fixture.store.remove_queued(dispatching.job_id()).is_err());
    assert!(
        fixture
            .store
            .remove_after_terminal(dispatching.job_id(), owner(999))
            .is_err()
    );
    let snapshot = fixture.store.queue_snapshot().unwrap();
    assert_eq!(snapshot.entries().len(), 1);
    assert_eq!(snapshot.entries()[0].job_id(), dispatching.job_id());
    assert!(matches!(
        snapshot.entries()[0].state(),
        QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == dispatcher
    ));
}

#[test]
fn terminal_remove_rejects_a_missing_local_job_record() {
    // Break caught: dispatcher ownership alone is mistaken for durable terminal
    // or abandonment proof.
    let fixture = open_queue();
    let dispatcher = owner(562);
    let dispatching = dispatching_batch(
        &fixture.store,
        "00000000000000000000000000000096",
        dispatcher,
        562,
    );

    let error = fixture
        .store
        .remove_after_terminal(dispatching.job_id(), dispatcher)
        .unwrap_err();

    assert_terminal_unproven(error);
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);
}

#[test]
fn terminal_remove_rejects_a_statusless_local_job_record() {
    // Break caught: merely publishing the pre-acceptance local record is
    // mistaken for durable terminal proof.
    let fixture = open_queue();
    let dispatcher = owner(563);
    let dispatching = dispatching_batch(
        &fixture.store,
        "00000000000000000000000000000097",
        dispatcher,
        563,
    );
    fixture
        .store
        .create_job(local_record(
            &fixture.store,
            dispatching.job_id(),
            "mini-1",
            None,
        ))
        .unwrap();

    let error = fixture
        .store
        .remove_after_terminal(dispatching.job_id(), dispatcher)
        .unwrap_err();

    assert_terminal_unproven(error);
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);
}

#[test]
fn terminal_remove_rejects_a_nonterminal_local_job_record() {
    // Break caught: an accepted but still nonterminal job releases its queue
    // reservation early.
    let fixture = open_queue();
    let dispatcher = owner(564);
    let dispatching = dispatching_batch(
        &fixture.store,
        "00000000000000000000000000000098",
        dispatcher,
        564,
    );
    fixture
        .store
        .create_job(local_record(
            &fixture.store,
            dispatching.job_id(),
            "mini-1",
            Some(JobStatus::accepted(566).unwrap()),
        ))
        .unwrap();

    let error = fixture
        .store
        .remove_after_terminal(dispatching.job_id(), dispatcher)
        .unwrap_err();

    assert_terminal_unproven(error);
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);
}

#[test]
fn terminal_remove_rejects_remote_uncertainty_even_with_a_terminal_status() {
    // Break caught: a terminal observation with unresolved remote ownership or
    // cleanup is treated as safe durable completion.
    for (job_id, dispatcher, uncertainty) in [
        (
            "00000000000000000000000000000099",
            owner(565),
            RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
        ),
        (
            "0000000000000000000000000000009a",
            owner(566),
            RemoteUncertainty::cleanup_pending("CLEANUP_INCOMPLETE").unwrap(),
        ),
    ] {
        let fixture = open_queue();
        let dispatching = dispatching_batch(&fixture.store, job_id, dispatcher, 567);
        fixture
            .store
            .create_job(local_record_with_uncertainty(
                &fixture.store,
                dispatching.job_id(),
                "mini-1",
                Some(JobStatus::succeeded(569, 10, 20).unwrap()),
                uncertainty,
            ))
            .unwrap();

        let error = fixture
            .store
            .remove_after_terminal(dispatching.job_id(), dispatcher)
            .unwrap_err();

        assert_terminal_unproven(error);
        assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);
    }
}

#[test]
fn terminal_remove_rejects_every_same_client_record_binding_mismatch() {
    // Break caught: terminal status under the queue filename is treated as
    // authority even when its embedded immutable identity names another job,
    // worker, project, worktree, or command summary.
    #[derive(Clone, Copy)]
    enum Mismatch {
        JobId,
        Worker,
        Project,
        Worktree,
        Command,
    }

    for (target_id, mismatch) in [
        ("0000000000000000000000000000009d", Mismatch::JobId),
        ("0000000000000000000000000000009e", Mismatch::Worker),
        ("0000000000000000000000000000009f", Mismatch::Project),
        ("000000000000000000000000000000a0", Mismatch::Worktree),
        ("000000000000000000000000000000a1", Mismatch::Command),
    ] {
        let fixture = open_queue();
        let dispatcher = owner(600);
        let dispatching = dispatching_batch(&fixture.store, target_id, dispatcher, 600);
        let embedded_job_id = match mismatch {
            Mismatch::JobId => "000000000000000000000000000000b0".parse().unwrap(),
            _ => dispatching.job_id(),
        };
        let worker = match mismatch {
            Mismatch::Worker => "mini-2",
            _ => "mini-1",
        };
        let project_id = match mismatch {
            Mismatch::Project => OTHER_PROJECT_ID,
            _ => PROJECT_ID,
        };
        let worktree_id = match mismatch {
            Mismatch::Worktree => OTHER_WORKTREE_ID,
            _ => WORKTREE_ID,
        };
        let command = match mismatch {
            Mismatch::Command => CommandSpec::argv(vec!["tool".into()]).unwrap(),
            _ => CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
        };
        let record = local_record_with_binding(
            &fixture.store,
            embedded_job_id,
            worker,
            project_id,
            worktree_id,
            command,
            Some(JobStatus::succeeded(602, 10, 20).unwrap()),
            RemoteUncertainty::None,
        );
        install_local_record_as(&fixture, dispatching.job_id(), &record);

        let error = fixture
            .store
            .remove_after_terminal(dispatching.job_id(), dispatcher)
            .unwrap_err();

        assert_terminal_unproven(error);
        let snapshot = fixture.store.queue_snapshot().unwrap();
        assert_eq!(snapshot.entries().len(), 1);
        assert_eq!(snapshot.entries()[0].job_id(), dispatching.job_id());
    }
}

#[test]
fn terminal_remove_rejects_a_terminal_record_from_another_client() {
    // Break caught: a copied terminal record from another client is accepted as
    // authority to retire this client's queue reservation.
    let fixture = open_queue();
    let foreign = open_queue();
    let dispatcher = owner(568);
    let dispatching = dispatching_batch(
        &fixture.store,
        "0000000000000000000000000000009c",
        dispatcher,
        573,
    );
    let foreign_record = local_record(
        &foreign.store,
        dispatching.job_id(),
        "mini-1",
        Some(JobStatus::succeeded(575, 10, 20).unwrap()),
    );
    foreign.store.create_job(foreign_record).unwrap();
    let filename = format!("{}.json", dispatching.job_id());
    fs::copy(
        foreign.root.join("jobs").join(&filename),
        fixture.root.join("jobs").join(&filename),
    )
    .unwrap();
    fs::set_permissions(
        fixture.root.join("jobs").join(&filename),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();

    assert!(
        fixture
            .store
            .remove_after_terminal(dispatching.job_id(), dispatcher)
            .is_err()
    );
    assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);
}

#[test]
fn terminal_remove_accepts_a_durably_terminal_same_client_record() {
    // Break caught: terminal cleanup cannot retire a proven completed local
    // reservation after tightening the lifecycle guard.
    let fixture = open_queue();
    let dispatcher = owner(567);
    let dispatching = dispatching_batch(
        &fixture.store,
        "0000000000000000000000000000009b",
        dispatcher,
        570,
    );
    fixture
        .store
        .create_job(local_record(
            &fixture.store,
            dispatching.job_id(),
            "mini-1",
            Some(JobStatus::succeeded(572, 10, 20).unwrap()),
        ))
        .unwrap();

    assert_eq!(
        fixture
            .store
            .remove_after_terminal(dispatching.job_id(), dispatcher)
            .unwrap()
            .job_id(),
        dispatching.job_id()
    );
    assert!(fixture.store.queue_snapshot().unwrap().entries().is_empty());
}

#[test]
fn two_dispatchers_racing_last_local_run_slot_admit_at_most_cap() {
    // Break caught: run-cap accounting occurs outside the queue mutation lock
    // and two sibling claims both consume the final slot.
    let fixture = open_queue();
    let store = Arc::new(fixture.store.clone());
    let run = run_reference("run-cap-race", 1);
    let left_owner = owner(570);
    let right_owner = owner(571);
    store
        .enqueue(queued_with(
            &store,
            "00000000000000000000000000000088",
            570,
            left_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run.clone()),
        ))
        .unwrap();
    store
        .enqueue(queued_with(
            &store,
            "00000000000000000000000000000089",
            571,
            right_owner,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run),
        ))
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let left = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            store.claim_next(left_owner, &["mini-1".into()], 572)
        })
    };
    let right = {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            store.claim_next(right_owner, &["mini-2".into()], 572)
        })
    };
    let admitted = [
        left.join().unwrap().unwrap(),
        right.join().unwrap().unwrap(),
    ]
    .into_iter()
    .filter(Option::is_some)
    .count();
    assert_eq!(admitted, 1);
}

#[test]
fn run_capped_head_does_not_block_a_younger_row_eligible_for_another_worker() {
    // Break caught: an older row that cannot consume its run's final slot is
    // treated as FIFO-eligible and blocks younger work on another worker.
    let fixture = open_queue();
    let run = run_reference("head-cap-does-not-block", 1);
    let active_owner = owner(572);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008c",
            572,
            active_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run.clone()),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(active_owner, &["mini-1".into()], 573)
        .unwrap()
        .expect("the first row consumes the run slot");

    let capped_head_owner = owner(573);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008d",
            574,
            capped_head_owner,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run),
        ))
        .unwrap();
    let younger_owner = owner(574);
    let younger = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008e",
            575,
            younger_owner,
            WorkerPreference::Pinned {
                worker: "mini-3".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            None,
        ))
        .unwrap();

    let claim = fixture
        .store
        .claim_next(younger_owner, &["mini-3".into()], 576)
        .unwrap()
        .expect("the capped head must not block another worker");
    assert_eq!(claim.entry().job_id(), younger.job_id());
    assert!(matches!(
        fixture.store.queue_snapshot().unwrap().entries()[1].state(),
        QueueState::Waiting { owner } if *owner == capped_head_owner
    ));
}

#[test]
fn accepted_nonterminal_local_sibling_also_consumes_run_cap() {
    // Break caught: recovery reverts a dispatch and forgets its already-accepted
    // local job still occupies the run slot.
    let fixture = open_queue();
    let run = run_reference("run-local-active", 1);
    let active_owner = owner(580);
    let active = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008a",
            580,
            active_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run.clone()),
        ))
        .unwrap();
    fixture
        .store
        .claim_next(active_owner, &["mini-1".into()], 581)
        .unwrap()
        .unwrap();
    fixture
        .store
        .create_job(local_record(
            &fixture.store,
            active.job_id(),
            "mini-1",
            Some(JobStatus::accepted(582).unwrap()),
        ))
        .unwrap();
    fixture
        .store
        .revert_dispatch(active.job_id(), active_owner)
        .unwrap();

    let sibling_owner = owner(581);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008b",
            583,
            sibling_owner,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run),
        ))
        .unwrap();
    assert!(
        fixture
            .store
            .claim_next(sibling_owner, &["mini-2".into()], 584)
            .unwrap()
            .is_none()
    );
}

#[test]
fn run_cap_counts_only_durably_accepted_nonterminal_local_siblings() {
    // Break caught: pre-acceptance Uploading/Verified state consumes the run
    // cap even though no durable acceptance exists yet.
    let cases = vec![
        (
            "uploading",
            JobStatus::new(
                JobState::Uploading,
                600,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap(),
            false,
        ),
        (
            "verified",
            JobStatus::new(
                JobState::Verified,
                601,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap(),
            false,
        ),
        ("accepted", JobStatus::accepted(602).unwrap(), true),
        (
            "running",
            JobStatus::running(603, 11, 12, 13, 14).unwrap(),
            true,
        ),
    ];

    for (index, (label, status, blocks)) in cases.into_iter().enumerate() {
        let fixture = open_queue();
        let run = run_reference(&format!("run-stage-{index}"), 1);
        let first_owner = owner(590 + index as u32 * 2);
        let first_id = format!("000000000000000000000000000002{index:02x}");
        let first = fixture
            .store
            .enqueue(queued_with(
                &fixture.store,
                &first_id,
                610 + index as u64 * 2,
                first_owner,
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                Vec::new(),
                QueueEntryKind::TaskTurn,
                Some(run.clone()),
            ))
            .unwrap();
        fixture
            .store
            .create_job(local_record(
                &fixture.store,
                first.job_id(),
                "mini-1",
                Some(status),
            ))
            .unwrap();

        let sibling_owner = owner(591 + index as u32 * 2);
        let sibling_id = format!("000000000000000000000000000003{index:02x}");
        fixture
            .store
            .enqueue(queued_with(
                &fixture.store,
                &sibling_id,
                611 + index as u64 * 2,
                sibling_owner,
                WorkerPreference::Pinned {
                    worker: "mini-2".into(),
                },
                Vec::new(),
                QueueEntryKind::TaskTurn,
                Some(run),
            ))
            .unwrap();

        let claim = fixture
            .store
            .claim_next(sibling_owner, &["mini-2".into()], 620 + index as u64)
            .unwrap();
        assert_eq!(claim.is_none(), blocks, "unexpected capacity for {label}");
    }
}

#[test]
fn run_cap_rejects_same_job_and_client_with_mismatched_queue_metadata() {
    // Break caught: a copied same-client record with the right filename but
    // different immutable queue metadata is counted instead of failing closed.
    let fixture = open_queue();
    let run = run_reference("run-binding-mismatch", 1);
    let first_owner = owner(598);
    let first = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000400",
            630,
            first_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run.clone()),
        ))
        .unwrap();
    fixture
        .store
        .create_job(local_record_with_binding(
            &fixture.store,
            first.job_id(),
            "mini-1",
            OTHER_PROJECT_ID,
            WORKTREE_ID,
            CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
            Some(JobStatus::accepted(631).unwrap()),
            RemoteUncertainty::None,
        ))
        .unwrap();

    let sibling_owner = owner(599);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000401",
            632,
            sibling_owner,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run),
        ))
        .unwrap();
    let before = fixture.store.queue_snapshot().unwrap();

    assert!(
        fixture
            .store
            .claim_next(sibling_owner, &["mini-2".into()], 633)
            .is_err()
    );
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
}

#[test]
fn wrong_name_local_run_record_fails_closed_without_claiming() {
    // Break caught: a copied/corrupt job file whose embedded ID differs from
    // its queue-row filename is ignored, undercounting the durable run cap.
    let fixture = open_queue();
    let run = run_reference("run-wrong-name", 1);
    let first_owner = owner(582);
    let first = fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008c",
            585,
            first_owner,
            WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run.clone()),
        ))
        .unwrap();
    let copied = local_record(
        &fixture.store,
        "000000000000000000000000000000ee".parse().unwrap(),
        "mini-1",
        Some(JobStatus::succeeded(586, 1, 1).unwrap()),
    );
    install_local_record_as(&fixture, first.job_id(), &copied);

    let second_owner = owner(583);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "0000000000000000000000000000008d",
            587,
            second_owner,
            WorkerPreference::Pinned {
                worker: "mini-2".into(),
            },
            Vec::new(),
            QueueEntryKind::TaskTurn,
            Some(run),
        ))
        .unwrap();
    let before = fixture.store.queue_snapshot().unwrap();

    assert!(
        fixture
            .store
            .claim_next(second_owner, &["mini-2".into()], 588)
            .is_err()
    );
    assert_eq!(fixture.store.queue_snapshot().unwrap(), before);
}

#[test]
fn one_run_id_cannot_publish_conflicting_parallel_caps_in_either_order() {
    // Break caught: capacity depends on whichever same-run row is selected as
    // the candidate because contradictory caps coexist in one snapshot.
    for (first_cap, second_cap) in [(1, 2), (2, 1)] {
        let fixture = open_queue();
        fixture
            .store
            .enqueue(queued_with(
                &fixture.store,
                "0000000000000000000000000000008e",
                589,
                owner(584),
                WorkerPreference::Automatic,
                Vec::new(),
                QueueEntryKind::TaskTurn,
                Some(run_reference("run-conflicting-cap", first_cap)),
            ))
            .unwrap();
        let before = fs::read(fixture.root.join("queue/state.json")).unwrap();

        assert!(
            fixture
                .store
                .enqueue(queued_with(
                    &fixture.store,
                    "0000000000000000000000000000008f",
                    590,
                    owner(585),
                    WorkerPreference::Automatic,
                    Vec::new(),
                    QueueEntryKind::TaskTurn,
                    Some(run_reference("run-conflicting-cap", second_cap)),
                ))
                .is_err()
        );
        assert_eq!(
            fs::read(fixture.root.join("queue/state.json")).unwrap(),
            before
        );
        assert_eq!(fixture.store.queue_snapshot().unwrap().entries().len(), 1);
    }
}

#[test]
fn persisted_snapshot_with_conflicting_run_caps_is_rejected() {
    // Break caught: strict load validation accepts contradictory same-run caps
    // that bypassed the enqueue API or arrived through corruption.
    let fixture = open_queue();
    for (job_id, at, owner_pid) in [
        ("00000000000000000000000000000090", 591, 586),
        ("00000000000000000000000000000091", 592, 587),
    ] {
        fixture
            .store
            .enqueue(queued_with(
                &fixture.store,
                job_id,
                at,
                owner(owner_pid),
                WorkerPreference::Automatic,
                Vec::new(),
                QueueEntryKind::TaskTurn,
                Some(run_reference("run-persisted-conflict", 1)),
            ))
            .unwrap();
    }
    let path = fixture.root.join("queue/state.json");
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    value["entries"][1]["run"]["max_parallel"] = serde_json::json!(2);
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    fs::write(path, bytes).unwrap();

    assert!(fixture.store.queue_snapshot().is_err());
}

#[test]
fn affinity_records_keep_worktree_and_project_fallbacks_separate_and_strict() {
    // Break caught: a combined blob lets worktree updates clobber project
    // fallback or malformed records silently become no affinity.
    let fixture = open_queue();
    fixture
        .store
        .record_affinity(PROJECT_ID, WORKTREE_ID, "mini-2", 600)
        .unwrap();
    let hints = fixture
        .store
        .affinity_hints(PROJECT_ID, WORKTREE_ID)
        .unwrap();
    assert_eq!(hints.worktree_worker.as_deref(), Some("mini-2"));
    assert_eq!(hints.project_worker.as_deref(), Some("mini-2"));
    assert_eq!(
        mode(
            fixture
                .root
                .join("affinity/projects")
                .join(format!("{PROJECT_ID}.json"))
        ),
        0o600
    );
    assert_eq!(
        mode(
            fixture
                .root
                .join("affinity/worktrees")
                .join(format!("{PROJECT_ID}-{WORKTREE_ID}.json"))
        ),
        0o600
    );

    fs::write(
        fixture
            .root
            .join("affinity/projects")
            .join(format!("{PROJECT_ID}.json")),
        b"{\"project_id\":\"bad\"}\n",
    )
    .unwrap();
    assert!(
        fixture
            .store
            .affinity_hints(PROJECT_ID, WORKTREE_ID)
            .is_err()
    );
}

#[test]
fn observation_refresh_loser_records_the_winner_timestamp() {
    // Break caught: a dispatcher that loses the single-flight refresh records
    // the time it started waiting, so the next TTL check treats a fresh
    // observation as stale.
    let fixture = open_queue();
    let store = Arc::new(fixture.store.clone());
    let refreshes = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let leader = {
        let store = Arc::clone(&store);
        let refreshes = Arc::clone(&refreshes);
        thread::spawn(move || {
            store.admission_observation("mini-1", 1_000, || {
                refreshes.fetch_add(1, Ordering::SeqCst);
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                AdmissionObservation::new(
                    "mini-1".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["rust".into()],
                    Some(16),
                    32,
                    2_500,
                )
            })
        })
    };
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let loser_refreshes = Arc::clone(&refreshes);
    let loser_store = Arc::clone(&store);
    let loser = thread::spawn(move || {
        loser_store.admission_observation("mini-1", 1_000, || {
            loser_refreshes.fetch_add(100, Ordering::SeqCst);
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec![],
                None,
                0,
                1_000,
            )
        })
    });
    thread::sleep(Duration::from_millis(50));
    release_tx.send(()).unwrap();
    let winner = leader.join().unwrap().unwrap();
    let loser = loser.join().unwrap().unwrap();
    assert_eq!(winner.observation().observed_at_millis(), 2_500);
    assert_eq!(winner.age_millis(), 0);
    assert_eq!(loser.observation().observed_at_millis(), 2_500);
    assert_eq!(loser.age_millis(), 0);
    assert_eq!(loser.observation().capabilities(), &["rust"]);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);
}

#[test]
fn stale_observation_refresh_is_single_flight_and_loser_keeps_age() {
    // Break caught: every dispatcher probes the same stale worker, or a loser
    // blocks instead of returning cached facts with their age.
    let fixture = open_queue();
    cache_observation(&fixture.store, "mini-1", &["rust"], 1_000);
    let store = Arc::new(fixture.store.clone());
    let refreshes = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let leader = {
        let store = Arc::clone(&store);
        let refreshes = Arc::clone(&refreshes);
        thread::spawn(move || {
            store.admission_observation("mini-1", 3_001, || {
                refreshes.fetch_add(1, Ordering::SeqCst);
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                AdmissionObservation::new(
                    "mini-1".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["rust".into(), "docker".into()],
                    Some(16),
                    32,
                    3_001,
                )
            })
        })
    };
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let loser_refreshes = Arc::clone(&refreshes);
    let stale = store
        .admission_observation("mini-1", 3_001, || {
            loser_refreshes.fetch_add(100, Ordering::SeqCst);
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec![],
                None,
                0,
                3_001,
            )
        })
        .unwrap();
    assert_eq!(stale.age_millis(), 2_001);
    assert_eq!(stale.observation().capabilities(), &["rust"]);
    assert_eq!(refreshes.load(Ordering::SeqCst), 1);

    release_tx.send(()).unwrap();
    let fresh = leader.join().unwrap().unwrap();
    assert_eq!(fresh.age_millis(), 0);
    assert_eq!(fresh.observation().capabilities(), &["rust", "docker"]);
    assert_eq!(mode(fixture.root.join("observations/mini-1.json")), 0o600);
    assert_eq!(
        mode(fixture.root.join("observations/mini-1.refresh")),
        0o600
    );
}

#[test]
fn exact_two_second_observation_is_fresh_and_unknown_fields_fail_closed() {
    // Break caught: TTL boundary refreshes early or decoding drops fields a
    // newer writer added instead of failing closed.
    let fixture = open_queue();
    cache_observation(&fixture.store, "mini-3", &["rust"], 10_000);
    let calls = AtomicUsize::new(0);
    let cached = fixture
        .store
        .admission_observation("mini-3", 12_000, || {
            calls.fetch_add(1, Ordering::SeqCst);
            unreachable!("exact TTL remains fresh")
        })
        .unwrap();
    assert_eq!(cached.age_millis(), 2_000);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let path = fixture.root.join("observations/mini-3.json");
    let bytes = fs::read(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["unknown"] = serde_json::json!("field");
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    fs::write(path, bytes).unwrap();
    assert!(
        fixture
            .store
            .admission_observation("mini-3", 12_001, || unreachable!())
            .is_err()
    );
}

#[test]
fn admission_observation_loads_records_that_predate_interactive_agents() {
    let json = r#"{"worker_name":"mini-1","ready":true,"slot":"idle","capabilities":["rust"],"available_memory_bytes":1,"free_disk_bytes":2,"observed_at_millis":10}"#;
    let observation: AdmissionObservation = serde_json::from_str(json).unwrap();
    assert_eq!(observation.interactive_agents(), None);
    assert_eq!(observation.worker_name(), "mini-1");

    let with_count = AdmissionObservation::new(
        "mini-1".into(),
        true,
        CandidateSlot::Idle,
        vec!["rust".into()],
        Some(1),
        2,
        10,
    )
    .unwrap()
    .with_interactive_agents(Some(4));
    let bytes = serde_json::to_vec(&with_count).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(text.contains(r#""interactive_agents":4"#), "{text}");
    let back: AdmissionObservation = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(back.interactive_agents(), Some(4));
}

#[test]
fn observation_cache_rejects_symlink_and_fifo_replacement() {
    // Break caught: cache reads follow external files or hang on attacker FIFOs.
    for kind in ["symlink", "fifo"] {
        let fixture = open_queue();
        cache_observation(&fixture.store, "mini-1", &["rust"], 20_000);
        let path = fixture.root.join("observations/mini-1.json");
        fs::rename(
            &path,
            fixture.root.join(format!("saved-observation-{kind}")),
        )
        .unwrap();
        if kind == "symlink" {
            symlink(fixture.root.join("saved-observation-symlink"), &path).unwrap();
        } else {
            let path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        }
        let (tx, rx) = mpsc::channel();
        let store = fixture.store.clone();
        thread::spawn(move || {
            tx.send(
                store
                    .admission_observation("mini-1", 20_001, || unreachable!())
                    .is_err(),
            )
            .unwrap()
        });
        assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap());
    }
}

#[test]
fn independent_worker_capability_maps_do_not_cross_admit() {
    // Break caught: claim uses a global capability set and admits a row on a
    // worker that did not report the required capability.
    let fixture = open_queue();
    cache_observation(&fixture.store, "mini-1", &["docker"], 970);
    cache_observation(&fixture.store, "mini-2", &["rust"], 970);
    let dispatcher = owner(970);
    fixture
        .store
        .enqueue(queued_with(
            &fixture.store,
            "00000000000000000000000000000097",
            970,
            dispatcher,
            WorkerPreference::Automatic,
            vec!["docker".into()],
            QueueEntryKind::Batch,
            None,
        ))
        .unwrap();
    assert!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-2".into()], 971)
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .store
            .claim_next(dispatcher, &["mini-1".into()], 972)
            .unwrap()
            .is_some()
    );
}

#[test]
fn run_reference_round_trips_as_opaque_canonical_metadata() {
    // Break caught: run metadata expands with task/session data or accepts a
    // nonpositive cap.
    let reference = run_reference("018f0f4a6b5c7d8e9f00112233445566", 3);
    let bytes = serde_json::to_vec(&reference).unwrap();
    assert_eq!(
        bytes,
        br#"{"run_id":"018f0f4a6b5c7d8e9f00112233445566","max_parallel":3}"#
    );
    assert_eq!(
        serde_json::from_slice::<QueueRunReference>(&bytes).unwrap(),
        reference
    );
    assert!(
        serde_json::from_slice::<QueueRunReference>(
            br#"{"run_id":"run","max_parallel":1,"task_id":"secret"}"#
        )
        .is_err()
    );
}

#[test]
fn cancelled_waiting_row_does_not_block_fifo_after_dispatch_reversion() {
    // Break caught: a cancel flag retained through a race still blocks a younger
    // row after exact dispatch reversion.
    let fixture = open_queue();
    let older_owner = owner(940);
    let younger_owner = owner(941);
    let older = fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000094",
            940,
            older_owner,
        ))
        .unwrap();
    fixture
        .store
        .claim_next(older_owner, &["mini-1".into()], 941)
        .unwrap()
        .unwrap();
    fixture
        .store
        .request_queue_cancel(older.job_id(), 942)
        .unwrap();
    fixture
        .store
        .revert_dispatch(older.job_id(), older_owner)
        .unwrap();
    fixture
        .store
        .enqueue(queued(
            &fixture.store,
            "00000000000000000000000000000095",
            943,
            younger_owner,
        ))
        .unwrap();
    let claim = fixture
        .store
        .claim_next(younger_owner, &["mini-1".into()], 944)
        .unwrap()
        .unwrap();
    assert_eq!(
        claim.entry().job_id().to_string(),
        "00000000000000000000000000000095"
    );
}

#[test]
fn same_job_after_adoption_does_not_spawn_again() {
    // Break caught: clearing slot_reservation after complete let cap>=2
    // acquire another spawn for the same live turn.
    let fixture = open_queue();
    let parent = owner(700);
    let child = owner(701);
    let (task_id, turn_id) = plant_task_turn(&fixture.store, 700, parent, None);
    let RunnerSlotDecision::Acquired { token } = fixture
        .store
        .reserve_runner_slot(turn_id, parent, 2, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    fixture
        .store
        .complete_runner_spawn(task_id, turn_id, token, child)
        .unwrap();
    assert!(
        fixture
            .store
            .queue_entry(turn_id)
            .unwrap()
            .unwrap()
            .slot_reservation()
            .is_none()
    );
    assert!(matches!(
        fixture
            .store
            .reserve_runner_slot(turn_id, parent, 2, false)
            .unwrap(),
        RunnerSlotDecision::Pending { .. }
    ));
}

#[test]
fn recovered_dispatch_owner_without_token_or_runner_may_acquire() {
    // Break caught: reconcile adopt_row onto the current process made
    // acting_queue_holder Some, so reserve returned Pending/Saturated even
    // though that PID is the controller, not a live runner or handoff token.
    let fixture = open_queue();
    let controller = owner(730);
    let (_task_id, turn_id) = plant_task_turn(&fixture.store, 730, controller, None);
    fixture
        .store
        .claim_next(controller, &["mini-1".into()], 731)
        .unwrap()
        .unwrap();
    fixture.store.adopt_row(turn_id, controller).unwrap();
    assert!(
        fixture
            .store
            .queue_entry(turn_id)
            .unwrap()
            .unwrap()
            .slot_reservation()
            .is_none()
    );
    assert!(matches!(
        fixture
            .store
            .reserve_runner_slot(turn_id, controller, 1, false)
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
}

#[test]
fn exclude_reserver_still_counts_that_pid_reservations_against_cap() {
    // Break caught: exclude_reserver dropped every reservation sharing the
    // finishing PID, so a parked handoff over-admitted against cap=1.
    let fixture = open_queue();
    let finishing = owner(710);
    let (_task_a, turn_a) = plant_task_turn(&fixture.store, 710, finishing, Some(finishing));
    let (_task_b, turn_b) = plant_task_turn(&fixture.store, 711, finishing, None);
    let (_task_c, turn_c) = plant_task_turn(&fixture.store, 712, finishing, None);
    assert!(matches!(
        fixture
            .store
            .reserve_runner_slot(turn_b, finishing, 2, false)
            .unwrap(),
        RunnerSlotDecision::Acquired { .. }
    ));
    let _ = turn_a;
    assert!(
        matches!(
            fixture
                .store
                .reserve_runner_slot(turn_c, finishing, 1, true)
                .unwrap(),
            RunnerSlotDecision::Saturated
        ),
        "outstanding reservation must occupy the only slot after excluding the finishing runner"
    );
}

#[test]
fn queue_publish_before_runner_record_does_not_free_the_slot() {
    // Break caught: complete published a cleared reservation, then crashed
    // before task.runner, so occupancy under-counted the live child.
    let fixture = open_queue();
    let parent = owner(720);
    let child = owner(721);
    let (task_id, turn_id) = plant_task_turn(&fixture.store, 720, parent, None);
    let extra = plant_task_turn(&fixture.store, 721, parent, None).1;
    let RunnerSlotDecision::Acquired { token } = fixture
        .store
        .reserve_runner_slot(turn_id, parent, 2, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    fixture
        .store
        .inject_write_failure_once(ClientStateWritePoint::AfterPublish);
    assert!(
        fixture
            .store
            .complete_runner_spawn(task_id, turn_id, token, child)
            .is_err()
    );
    assert!(
        fixture
            .store
            .queue_entry(turn_id)
            .unwrap()
            .unwrap()
            .slot_reservation()
            .is_some(),
        "reservation must remain until runner persistence succeeds"
    );
    assert!(
        fixture.store.live_runner_slot_count().unwrap() >= 1,
        "handoff crash must not drop occupancy"
    );
    assert!(matches!(
        fixture
            .store
            .reserve_runner_slot(extra, parent, 1, false)
            .unwrap(),
        RunnerSlotDecision::Saturated
    ));
}

#[test]
fn stale_complete_after_steal_does_not_clear_the_new_child() {
    // Break caught: complete error cleanup recorded runner=None and erased
    // the child that stole the slot.
    let parent = owner(730);
    let child = owner(731);
    let fixture = open_queue_with_owner_inspector(live_set(&[parent, child]));
    let (task_id, turn_id) = plant_task_turn(&fixture.store, 730, parent, None);
    let RunnerSlotDecision::Acquired { token: old } = fixture
        .store
        .reserve_runner_slot(turn_id, parent, 1, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    let stolen =
        ClientStateStore::open_with_owner_inspector(&fixture.root, live_set(&[child])).unwrap();
    stolen.note_confirmed_runner_absence(parent);
    let RunnerSlotDecision::Acquired { token: new } = stolen
        .reserve_runner_slot(turn_id, child, 1, false)
        .unwrap()
    else {
        panic!("dead parent must steal with a new token");
    };
    assert_ne!(old, new);
    stolen
        .complete_runner_spawn(task_id, turn_id, new, child)
        .unwrap();
    assert_eq!(
        stolen
            .complete_runner_spawn(task_id, turn_id, old, parent)
            .unwrap_err()
            .public_code(),
        "QUEUE_SLOT_TOKEN_MISMATCH"
    );
    assert_eq!(
        stolen
            .load_task(task_id)
            .unwrap()
            .runner()
            .unwrap()
            .process_identity(),
        child
    );
}

#[test]
fn token_current_child_takes_over_dead_parent_without_a_second_spawn() {
    // Break caught: the original child waited for adoption until timeout
    // while reconcile stole and spawned a duplicate.
    let parent = owner(740);
    let child = owner(741);
    let fixture = open_queue_with_owner_inspector(live_set(&[parent, child]));
    let (_task_id, turn_id) = plant_task_turn(&fixture.store, 740, parent, None);
    let RunnerSlotDecision::Acquired { token } = fixture
        .store
        .reserve_runner_slot(turn_id, parent, 1, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    fixture
        .store
        .bind_runner_slot_child(turn_id, token, child)
        .unwrap();
    fixture
        .store
        .release_runner_slot(turn_id, token, parent)
        .unwrap();
    assert!(
        fixture
            .store
            .queue_entry(turn_id)
            .unwrap()
            .unwrap()
            .slot_reservation()
            .is_some(),
        "bound child reservation must survive parent release"
    );
    let orphaned =
        ClientStateStore::open_with_owner_inspector(&fixture.root, live_set(&[child])).unwrap();
    orphaned.note_confirmed_runner_absence(parent);
    assert_eq!(
        orphaned
            .take_over_reserved_slot(turn_id, token, child)
            .unwrap(),
        ReservedSlotTakeover::Ready
    );
    assert_eq!(
        orphaned
            .queue_entry(turn_id)
            .unwrap()
            .unwrap()
            .owner_opt()
            .copied(),
        Some(child)
    );
    assert!(matches!(
        orphaned
            .reserve_runner_slot(turn_id, child, 1, false)
            .unwrap(),
        RunnerSlotDecision::Pending { .. }
    ));
}

#[test]
fn released_then_reused_job_rejects_the_old_token() {
    let fixture = open_queue();
    let reserver = owner(750);
    let child = owner(751);
    let (task_id, turn_id) = plant_task_turn(&fixture.store, 750, reserver, None);
    let other = plant_task_turn(&fixture.store, 751, reserver, None).1;
    let RunnerSlotDecision::Acquired { token: old } = fixture
        .store
        .reserve_runner_slot(turn_id, reserver, 2, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    fixture
        .store
        .release_runner_slot(turn_id, old, reserver)
        .unwrap();
    let RunnerSlotDecision::Acquired { token: other_token } = fixture
        .store
        .reserve_runner_slot(other, reserver, 1, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    fixture
        .store
        .release_runner_slot(other, other_token, reserver)
        .unwrap();
    fixture.store.remove_queued(other).unwrap();
    let RunnerSlotDecision::Acquired { token: new } = fixture
        .store
        .reserve_runner_slot(turn_id, reserver, 1, false)
        .unwrap()
    else {
        panic!("expected acquire");
    };
    assert_ne!(old, new);
    assert_eq!(
        fixture
            .store
            .complete_runner_spawn(task_id, turn_id, old, child)
            .unwrap_err()
            .public_code(),
        "QUEUE_SLOT_TOKEN_MISMATCH"
    );
}

#[test]
fn old_queue_snapshot_without_slot_reservation_deserializes() {
    let fixture = open_queue();
    let owner = owner(760);
    plant_task_turn(&fixture.store, 760, owner, None);
    let bytes = fs::read(fixture.root.join("queue/state.json")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value["entries"][0].get("slot_reservation").is_none());
    let snapshot: QueueSnapshot = serde_json::from_value(value).unwrap();
    assert!(snapshot.entries()[0].slot_reservation().is_none());
}
