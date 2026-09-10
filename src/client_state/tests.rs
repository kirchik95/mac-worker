use std::{fs, os::unix::fs::MetadataExt, path::Path};

use uuid::Uuid;

use super::*;
use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
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
            ProcessIdentity::new(42, 1_700_000_000_001).expect("fixture process"),
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
    ProcessIdentity::new(42, 1_700_000_000_001).expect("fixture process")
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
fn runner_identity_verdict_treats_matching_as_live() {
    let (_dir, store, identity) =
        open_with_script([], ProcessObservation::Matching { process_group: 42 });
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Live
    );
}

#[test]
fn runner_identity_verdict_treats_reused_as_exited_immediately() {
    let (_dir, store, identity) = open_with_script([], ProcessObservation::Reused);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Exited
    );
}

#[test]
fn runner_identity_verdict_keeps_a_single_absent_unverifiable() {
    let (_dir, store, identity) = open_with_script([], ProcessObservation::Absent);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
}

#[test]
fn runner_identity_verdict_confirms_absent_after_the_window() {
    let (_dir, store, identity) = open_with_script([], ProcessObservation::Absent);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Exited
    );
}

#[test]
fn runner_identity_verdict_clears_absence_when_the_pid_matches_again() {
    let (_dir, store, identity) = open_with_script(
        [
            ProcessObservation::Absent,
            ProcessObservation::Matching { process_group: 42 },
            ProcessObservation::Absent,
        ],
        ProcessObservation::Absent,
    );
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Live
    );
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
}

#[test]
fn runner_identity_verdict_treats_ambiguous_as_unverifiable() {
    let (_dir, store, identity) = open_with_script([], ProcessObservation::Ambiguous);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
    store.advance_liveness_clock(RUNNER_ABSENCE_CONFIRMATION);
    assert_eq!(
        store.runner_identity_verdict(identity),
        RunnerLivenessVerdict::Unverifiable
    );
}
