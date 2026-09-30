use std::{fs, os::unix::fs::MetadataExt, path::Path};

use uuid::Uuid;

use super::*;
use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    scheduler::CandidateSlot,
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnSummary,
    },
};

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn sample_record() -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(Uuid::from_u128(1)),
        run_id: None,
        project_id: PROJECT_ID.to_owned(),
        worktree_id: WORKTREE_ID.to_owned(),
        agent: AgentKind::Codex,
        model: Some("gpt-5".into()),
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: BASE_OID.parse().expect("fixture base oid"),
        limits: TaskLimits::new(TurnLimits::new(30 * 60 * 1000, None, None).unwrap(), 10)
            .expect("fixture limits"),
        close_policy: ClosePolicy::Done,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "Fix the flaky login spec\n\nDetails…".into(),
        created_at_millis: 1_700_000_000_000,
    })
    .expect("fixture meta");
    let turn_id: TurnId = "018f0f4a6b5c7d8e9f00112233445566"
        .parse()
        .expect("fixture turn id");
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        None,
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1, turn_id, None, None, None, false, None, None,
        )],
        1_700_000_000_000,
    )
    .expect("fixture status");
    LocalTaskRecord::new(
        meta,
        status,
        None,
        Some(RunnerIdentity::new(
            ProcessIdentity::new(crate::fixture_pid::fixture_pid(42), 1_700_000_000_001)
                .expect("fixture process"),
        )),
        None,
        REPO_ID.to_owned(),
        Some("mini-1".into()),
        true,
        None,
    )
    .expect("fixture record")
}

fn open_store() -> (tempfile::TempDir, ClientStateStore, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state_path).unwrap();
    (dir, store, state_path)
}

fn task_record_path(state: &Path, record: &LocalTaskRecord) -> std::path::PathBuf {
    state
        .join("tasks")
        .join(format!("{}.json", record.meta().task_id()))
}

fn regular_file_identity(path: &Path) -> (u64, u64, Vec<u8>) {
    let meta = fs::metadata(path).unwrap();
    (meta.dev(), meta.ino(), fs::read(path).unwrap())
}

#[test]
fn matching_cas_of_an_identical_record_keeps_the_underlying_file_identity() {
    let (_dir, store, state_path) = open_store();
    let record = sample_record();
    store.create_task(record.clone()).unwrap();
    let path = task_record_path(&state_path, &record);
    let before = regular_file_identity(&path);

    assert!(
        store
            .update_task_if_current(&record, record.clone())
            .unwrap()
    );
    assert_eq!(regular_file_identity(&path), before);
    assert_eq!(store.load_task(record.meta().task_id()).unwrap(), record);
}

#[test]
fn stale_cas_fails_even_when_the_replacement_equals_the_current_record() {
    let (_dir, store, state_path) = open_store();
    let original = sample_record();
    store.create_task(original.clone()).unwrap();
    let current = original.clone().with_runner(None).unwrap();
    store.update_task(current.clone()).unwrap();
    let path = task_record_path(&state_path, &original);
    let before = regular_file_identity(&path);

    assert!(
        !store
            .update_task_if_current(&original, current.clone())
            .unwrap()
    );
    assert_eq!(regular_file_identity(&path), before);
    assert_eq!(store.load_task(original.meta().task_id()).unwrap(), current);
}

#[test]
fn matching_cas_still_persists_a_changed_runner() {
    let (_dir, store, state_path) = open_store();
    let original = sample_record();
    store.create_task(original.clone()).unwrap();
    let path = task_record_path(&state_path, &original);
    let created = regular_file_identity(&path);
    let replacement = original.clone().with_runner(None).unwrap();

    assert!(
        store
            .update_task_if_current(&original, replacement.clone())
            .unwrap()
    );
    let after = regular_file_identity(&path);
    assert_ne!(after.1, created.1);
    assert_eq!(
        store.load_task(original.meta().task_id()).unwrap(),
        replacement
    );
}

struct MutationGate {
    entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl ClientStateConcurrencyHook for MutationGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::BeforeTaskMutation
            && let Some(entered) = self.entered.lock().unwrap().take()
        {
            entered.send(()).unwrap();
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(20))
                .unwrap();
        }
    }
}

#[test]
fn runner_mutation_preserves_concurrent_fetched_head_and_deliveries() {
    let (_dir, store, state_path) = open_store();
    let original = sample_record();
    let task_id = original.meta().task_id();
    let turn_id = original.status().turns().last().unwrap().turn_id();
    store.create_task(original.clone()).unwrap();
    let head: BaseOid = "cccccccccccccccccccccccccccccccccccccccc".parse().unwrap();
    let delivery = crate::task::OriginDelivery::new(
        turn_id,
        crate::task::DeliveryState::Delivered,
        head.clone(),
        "https://example.test/repo.git".into(),
        "refs/heads/result".into(),
        1,
        0,
        None,
        None,
        10,
        20,
    )
    .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let writer = ClientStateStore::open_with_concurrency_hook(
        &state_path,
        Arc::new(MutationGate {
            entered: Mutex::new(Some(entered_tx)),
            resume: Mutex::new(resume_rx),
        }),
    )
    .unwrap();

    std::thread::scope(|scope| {
        let mutation = scope.spawn(|| writer.record_runner(task_id, None));
        entered_rx.recv_timeout(Duration::from_secs(20)).unwrap();
        assert!(
            store
                .update_fetched_head_for_current_turn(&original, head.clone())
                .unwrap()
        );
        let current = store.load_task(task_id).unwrap();
        assert!(
            store
                .update_task_if_current(
                    &current,
                    current.with_delivery(Some(delivery.clone())).unwrap()
                )
                .unwrap()
        );
        resume_tx.send(()).unwrap();
        mutation.join().unwrap().unwrap();
    });
    let current = store.load_task(task_id).unwrap();
    assert!(current.runner().is_none());
    assert_eq!(current.fetched_head(), Some(&head));
    assert_eq!(current.deliveries(), &[delivery]);
}

#[test]
fn runner_mutation_rejects_a_concurrently_replaced_turn() {
    let (_dir, store, state_path) = open_store();
    let original = sample_record();
    let task_id = original.meta().task_id();
    store.create_task(original.clone()).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let writer = ClientStateStore::open_with_concurrency_hook(
        &state_path,
        Arc::new(MutationGate {
            entered: Mutex::new(Some(entered_tx)),
            resume: Mutex::new(resume_rx),
        }),
    )
    .unwrap();
    let new_turn = TurnId::generate();
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![TurnSummary::new(
            1, new_turn, None, None, None, false, None, None,
        )],
        1_700_000_000_001,
    )
    .unwrap();
    let next = original.with_status(status).unwrap();
    let result = std::thread::scope(|scope| {
        let mutation = scope.spawn(|| writer.record_runner(task_id, None));
        entered_rx.recv_timeout(Duration::from_secs(20)).unwrap();
        assert!(
            store
                .update_task_if_current(&original, next.clone())
                .unwrap()
        );
        resume_tx.send(()).unwrap();
        mutation.join().unwrap()
    });
    assert_eq!(result.unwrap_err().public_code(), "TASK_STALE");
    assert_eq!(store.load_task(task_id).unwrap(), next);
}

struct ScriptedInspector {
    sequence: std::sync::Mutex<std::collections::VecDeque<ProcessObservation>>,
    exhausted: ProcessObservation,
}

impl ScriptedInspector {
    fn new(
        sequence: impl IntoIterator<Item = ProcessObservation>,
        exhausted: ProcessObservation,
    ) -> Self {
        Self {
            sequence: std::sync::Mutex::new(sequence.into_iter().collect()),
            exhausted,
        }
    }
}

impl ProcessInspector for ScriptedInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, _expected: ProcessIdentity) -> ProcessObservation {
        self.sequence
            .lock()
            .expect("scripted inspector mutex poisoned")
            .pop_front()
            .unwrap_or(self.exhausted)
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Ambiguous
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::Ambiguous
    }
}

fn identity() -> ProcessIdentity {
    ProcessIdentity::new(crate::fixture_pid::fixture_pid(42), 1_700_000_000_001)
        .expect("fixture process")
}

fn open_with_script(
    sequence: impl IntoIterator<Item = ProcessObservation>,
    exhausted: ProcessObservation,
) -> (tempfile::TempDir, ClientStateStore, ProcessIdentity) {
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open_with_owner_inspector(
        &state_path,
        ScriptedInspector::new(sequence, exhausted),
    )
    .unwrap();
    (dir, store, identity())
}

#[test]
fn runner_identity_verdicts_follow_the_absence_confirmation_window() {
    use RunnerLivenessVerdict::{Exited, Live, Unverifiable};
    enum Step {
        Expect(RunnerLivenessVerdict),
        Advance,
    }
    use Step::{Advance, Expect};
    let cases: Vec<(&str, Vec<ProcessObservation>, ProcessObservation, Vec<Step>)> = vec![
        (
            "a matching pid is live",
            vec![],
            ProcessObservation::Matching { process_group: 42 },
            vec![Expect(Live)],
        ),
        (
            "a reused pid has exited immediately",
            vec![],
            ProcessObservation::Reused,
            vec![Expect(Exited)],
        ),
        (
            "a single absence stays unverifiable",
            vec![],
            ProcessObservation::Absent,
            vec![Expect(Unverifiable), Expect(Unverifiable)],
        ),
        (
            "absence is confirmed after the window",
            vec![],
            ProcessObservation::Absent,
            vec![Expect(Unverifiable), Advance, Expect(Exited)],
        ),
        (
            "a matching pid clears the absence",
            vec![
                ProcessObservation::Absent,
                ProcessObservation::Matching { process_group: 42 },
                ProcessObservation::Absent,
            ],
            ProcessObservation::Absent,
            vec![
                Expect(Unverifiable),
                Advance,
                Expect(Live),
                Expect(Unverifiable),
            ],
        ),
        (
            "ambiguous stays unverifiable past the window",
            vec![],
            ProcessObservation::Ambiguous,
            vec![Expect(Unverifiable), Advance, Expect(Unverifiable)],
        ),
    ];
    for (label, script, exhausted, steps) in cases {
        let (_dir, store, identity) = open_with_script(script, exhausted);
        for step in steps {
            match step {
                Expect(verdict) => {
                    assert_eq!(store.runner_identity_verdict(identity), verdict, "{label}")
                }
                Advance => store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION),
            }
        }
    }
}

#[test]
fn admission_observation_refreshes_after_local_release_invalidation() {
    let (_dir, store, _) = open_store();
    store
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Busy,
                vec!["darwin-arm64".into()],
                Some(10),
                20,
                1_000,
            )
            .unwrap(),
        )
        .unwrap();
    let mut refreshed = false;
    store
        .admission_observation("mini-1", 1_000, || {
            refreshed = true;
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec!["darwin-arm64".into()],
                Some(10),
                20,
                1_000,
            )
        })
        .unwrap();
    assert!(
        !refreshed,
        "fresh saturated occupancy must stay on the advisory fast path until a local release"
    );

    store.invalidate_admission_observation("mini-1").unwrap();
    let cached = store
        .admission_observation("mini-1", 1_000, || {
            refreshed = true;
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec!["darwin-arm64".into()],
                Some(10),
                20,
                1_000,
            )
        })
        .unwrap();
    assert!(
        refreshed,
        "local release must drop the saturated observation so the next dispatch probes"
    );
    assert_eq!(cached.observation().slot(), CandidateSlot::Idle);
}

#[test]
fn admission_observation_reuses_fresh_idle_occupancy() {
    let (_dir, store, _) = open_store();
    store
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec!["darwin-arm64".into()],
                Some(10),
                20,
                1_000,
            )
            .unwrap(),
        )
        .unwrap();
    let mut refreshed = false;
    let cached = store
        .admission_observation("mini-1", 1_000, || {
            refreshed = true;
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Busy,
                vec!["darwin-arm64".into()],
                Some(10),
                20,
                1_000,
            )
        })
        .unwrap();
    assert!(
        !refreshed,
        "fresh Idle occupancy must stay on the advisory fast path"
    );
    assert_eq!(cached.observation().slot(), CandidateSlot::Idle);
}
#[test]
fn wait_deadline_bounds_admission_refresh_lock() {
    use std::time::{Duration, Instant};
    let root = tempfile::tempdir().unwrap();
    let store =
        super::ClientStateStore::open(&root.path().canonicalize().unwrap().join("state")).unwrap();
    let held = store
        .acquire_observation_refresh("mini-1", Instant::now() + Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let waiting =
        store.with_wait_deadline(super::WaitDeadline::new(Some(Duration::from_millis(150))));
    std::thread::scope(|scope| {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = scope.spawn(move || {
            let started = Instant::now();
            let result = waiting
                .acquire_observation_refresh("mini-1", Instant::now() + Duration::from_secs(5));
            done_tx.send(started.elapsed()).unwrap();
            result.map(|guard| guard.is_some())
        });
        let elapsed = done_rx.recv_timeout(Duration::from_secs(1));
        drop(held);
        let result = waiter.join().unwrap();
        assert!(
            elapsed.is_ok(),
            "admission refresh lock outlived the caller's wait deadline"
        );
        assert!(elapsed.unwrap() < Duration::from_secs(1));
        assert_eq!(result.unwrap_err().public_code(), "WAIT_TIMEOUT");
    });
    assert!(
        store
            .acquire_observation_refresh("mini-1", Instant::now() + Duration::from_secs(1))
            .unwrap()
            .is_some()
    );
}
