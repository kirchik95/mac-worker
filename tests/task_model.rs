#[allow(dead_code)]
mod support;

use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use mac_worker::{
    agent::{AdapterError, AgentKind, AgentOutcome, PermissionPolicy, Question, TurnLimits},
    client_state::{
        ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore,
        ClientStateWritePoint,
    },
    error::{ExitKind, WorkerError},
    job::{JobId, ProcessIdentity},
    task::{
        BaseOid, BranchName, ClosePolicy, GitIdentity, LocalTaskRecord, MAX_FOLLOWUPS,
        MAX_PROMPT_BYTES, PublishMode, PushTarget, RunId, RunProgress, RunRecord, RunnerIdentity,
        RunnerState, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource,
        TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
};
use proptest::prelude::*;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const LEGACY_FETCH_ONLY_LOCAL_RECORD: &str = r#"{"meta":{"task_id":"00000000000000000000000000000001","run_id":"00000000000000000000000000000002","project_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","worktree_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","agent":"codex","model":"gpt-5","policy":"workspace","source":{"kind":"local","wip":false},"publish":["fetch"],"publish_branch":null,"base_oid":"0123456789abcdef0123456789abcdef01234567","limits":{"turn":{"timeout_millis":1800000,"max_turns":null,"max_budget_usd_cents":null},"max_followups":10},"close_policy":"done","env_profile":null,"git_identity":{"name":"Ada Lovelace","email":"ada@example.test"},"title":"Fix the flaky login spec","created_at_millis":1700000000000},"status":{"state":"queued","last_outcome":null,"worker":null,"session_present":false,"head_oid":null,"summary":null,"questions":[],"files_changed":[],"diff_stat":null,"turns":[{"turn_number":1,"turn_id":"018f0f4a6b5c7d8e9f00112233445566","terminal":null,"outcome":null,"agent_committed":null,"log_truncated":false,"started_at_millis":null,"ended_at_millis":null}],"updated_at_millis":1700000000000},"status_observed_at_millis":null,"runner":{"pid":42,"start_time_micros":1700000000001},"fetched_head":null,"repo_id":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","pinned_worker":"mini-1","wait_for_capacity":true,"abandon_code":null}"#;
const LEGACY_RUN_RECORD_WITH_RESERVATION: &str = r#"{"run_id":"00000000000000000000000000000002","name":"batch-1","task_ids":["00000000000000000000000000000001"],"max_parallel":2,"created_at_millis":99,"reserved_publish_branches":["release-candidate"]}"#;

fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn run_id() -> RunId {
    RunId::new(Uuid::from_u128(2))
}

fn turn_id() -> TurnId {
    "018f0f4a6b5c7d8e9f00112233445566"
        .parse::<JobId>()
        .expect("fixture turn id")
}

fn git_identity() -> GitIdentity {
    GitIdentity::new("Ada Lovelace", "ada@example.test").expect("fixture identity")
}

fn turn_limits() -> TurnLimits {
    TurnLimits::new(30 * 60 * 1000, None, None).expect("fixture turn limits")
}

fn task_limits() -> TaskLimits {
    TaskLimits::new(turn_limits(), 10).expect("fixture task limits")
}

fn fields_with_prompt(prompt: String) -> TaskMetaInput {
    TaskMetaInput {
        task_id: task_id(),
        run_id: Some(run_id()),
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
        limits: task_limits(),
        close_policy: ClosePolicy::Done,
        env_profile: None,
        git_identity: git_identity(),
        title: None,
        prompt,
        created_at_millis: 1_700_000_000_000,
    }
}

fn sample_meta() -> TaskMeta {
    TaskMeta::new(fields_with_prompt(
        "Fix the flaky login spec\n\nDetails…".into(),
    ))
    .expect("fixture meta")
}

fn sample_status() -> TaskStatus {
    TaskStatus::new(
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
            1,
            turn_id(),
            None,
            None,
            None,
            false,
            None,
            None,
        )],
        1_700_000_000_000,
    )
    .expect("fixture status")
}

fn sample_record() -> LocalTaskRecord {
    LocalTaskRecord::new(
        sample_meta(),
        sample_status(),
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

struct PreExchangeReplacementGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    used: AtomicBool,
}

impl ClientStateConcurrencyHook for PreExchangeReplacementGate {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::TaskReplacementPreExchange
            && !self.used.swap(true, Ordering::SeqCst)
        {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
    }
}

#[test]
fn base_oid_and_branch_names_are_validated() {
    assert!(
        "0123456789abcdef0123456789abcdef01234567"
            .parse::<BaseOid>()
            .is_ok()
    );
    assert!(
        "0123456789ABCDEF0123456789abcdef01234567"
            .parse::<BaseOid>()
            .is_err()
    );
    for invalid in [
        "",
        "-x",
        "a..b",
        "a/",
        "/a",
        "a.lock",
        "a//b",
        "a b",
        "a\u{7}b",
        "refs/heads/x",
    ] {
        assert!(invalid.parse::<BranchName>().is_err(), "{invalid:?}");
    }
    assert_eq!(
        BranchName::for_task(task_id()).as_str(),
        "task/00000000000000000000000000000001"
    );
}

#[test]
fn task_state_allows_only_documented_transitions() {
    use TaskState::*;
    assert!(Queued.can_transition_to(Active) && Queued.can_transition_to(Abandoned));
    assert!(
        Active.can_transition_to(Open)
            && Active.can_transition_to(Closed)
            && Active.can_transition_to(Lost)
    );
    assert!(
        Open.can_transition_to(Active)
            && Open.can_transition_to(Closed)
            && Open.can_transition_to(Abandoned)
    );
    assert!(
        !Closed.can_transition_to(Open)
            && !Queued.can_transition_to(Open)
            && !Lost.can_transition_to(Open)
    );
}

#[test]
fn lost_turn_leaves_task_open_with_lost_outcome() {
    assert_eq!(
        TaskOutcome::from_turn(TurnTerminal::Lost, None),
        TaskOutcome::Lost
    );
    assert_eq!(
        TaskOutcome::from_turn(TurnTerminal::TimedOut, None),
        TaskOutcome::TimedOut
    );
    assert_eq!(
        TaskOutcome::from_turn(TurnTerminal::Succeeded, Some(AgentOutcome::NeedsInput)),
        TaskOutcome::NeedsInput
    );
    assert_eq!(
        TaskOutcome::from_turn(
            TurnTerminal::Failed,
            Some(AgentOutcome::Failed { exit_code: 3 })
        ),
        TaskOutcome::Failed {
            reason: "agent exited 3".into()
        }
    );
}

#[test]
fn task_meta_bounds_prompt_and_summary_hides_it() {
    assert_eq!(
        TaskMeta::new(fields_with_prompt("x".repeat(MAX_PROMPT_BYTES + 1)))
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
    let meta = TaskMeta::new(fields_with_prompt(
        "Fix the flaky login spec\n\nDetails…".into(),
    ))
    .unwrap();
    let json = serde_json::to_value(meta.summary()).unwrap();
    assert_eq!(json["title"], "Fix the flaky login spec");
    assert!(json.get("prompt").is_none() && json.get("session_ref").is_none());
}

#[test]
fn task_meta_round_trips_the_recorded_reasoning_effort() {
    let mut fields = fields_with_prompt("Fix the flaky login spec".into());
    fields.model = Some("gpt-5.6-luna".into());
    fields.effort = Some("max".into());
    let meta = TaskMeta::new(fields).unwrap();
    assert_eq!(meta.effort(), Some("max"));

    let json = serde_json::to_value(&meta).unwrap();
    assert_eq!(json["effort"], "max");
    let restored: TaskMeta = serde_json::from_value(json).unwrap();
    assert_eq!(restored.effort(), Some("max"));

    // A task without an effort writes no key at all, so records created before
    // the field existed keep their exact bytes and still load.
    let without = TaskMeta::new(fields_with_prompt("Fix the flaky login spec".into())).unwrap();
    let json = serde_json::to_value(&without).unwrap();
    assert!(json.get("effort").is_none());
    let restored: TaskMeta = serde_json::from_value(json).unwrap();
    assert_eq!(restored.effort(), None);

    let mut invalid = fields_with_prompt("Fix the flaky login spec".into());
    invalid.effort = Some("max\" -c sandbox_mode=\"danger-full-access".into());
    assert_eq!(
        TaskMeta::new(invalid).unwrap_err().public_code(),
        "TASK_CONFIG_INVALID"
    );
}

#[test]
fn questions_keep_their_options_and_read_back_plain_strings() {
    let status = TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::NeedsInput),
        Some("mini-1".into()),
        true,
        None,
        Some("pick a base".into()),
        vec![
            Question::new("Which base?", vec!["main".into(), "release".into()]),
            Question::open("Anything else?"),
        ],
        Vec::new(),
        None,
        Vec::new(),
        1_700,
    )
    .unwrap();

    let json = serde_json::to_value(&status).unwrap();
    assert_eq!(json["questions"][0]["text"], "Which base?");
    assert_eq!(json["questions"][0]["options"][1], "release");
    // An open question stays a bare string, so records written before options
    // existed round-trip byte for byte.
    assert_eq!(json["questions"][1], "Anything else?");

    let restored: TaskStatus = serde_json::from_value(json).unwrap();
    assert_eq!(restored.questions(), status.questions());

    let legacy: TaskStatus = serde_json::from_value(serde_json::json!({
        "state": "open",
        "last_outcome": {"kind": "needs_input"},
        "worker": "mini-1",
        "session_present": true,
        "head_oid": null,
        "summary": "pick a base",
        "questions": ["Which base?"],
        "files_changed": [],
        "diff_stat": null,
        "turns": [],
        "updated_at_millis": 1_700,
    }))
    .unwrap();
    assert_eq!(legacy.questions(), &[Question::open("Which base?")]);
}

#[test]
fn outcome_kind_names_match_the_serialized_tag() {
    for outcome in [
        TaskOutcome::Done,
        TaskOutcome::NeedsInput,
        TaskOutcome::Blocked,
        TaskOutcome::Unknown,
        TaskOutcome::failed("agent exited 2"),
        TaskOutcome::Cancelled,
        TaskOutcome::TimedOut,
        TaskOutcome::Lost,
    ] {
        let json = serde_json::to_value(&outcome).unwrap();
        assert_eq!(json["kind"], outcome.kind());
        assert!(TaskOutcome::KINDS.contains(&outcome.kind()));
    }
}

#[test]
fn unknown_and_duplicate_json_fields_are_rejected() {
    let record = sample_record();
    let bytes = record.canonical_bytes().unwrap();
    let mut unknown = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
    unknown["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<LocalTaskRecord>(unknown).is_err());

    let text = String::from_utf8(bytes).unwrap();
    let duplicate = text.replacen('{', r#"{"wait_for_capacity":true,"#, 1);
    assert!(serde_json::from_str::<LocalTaskRecord>(&duplicate).is_err());
}

#[test]
fn nested_duplicate_json_fields_are_rejected() {
    let record = String::from_utf8(sample_record().canonical_bytes().unwrap()).unwrap();
    let duplicate = record.replacen(
        r#""status":{"state":"queued","#,
        r#""status":{"state":"queued","state":"queued","#,
        1,
    );
    assert_ne!(
        duplicate, record,
        "fixture must contain a task status object"
    );

    assert!(serde_json::from_str::<LocalTaskRecord>(&duplicate).is_err());
}

#[test]
fn task_limits_bound_followups_and_default_to_ten() {
    assert_eq!(TaskLimits::default().max_followups, 10);
    assert_eq!(TaskLimits::default().turn.timeout_millis, 45 * 60 * 1000);
    assert!(TaskLimits::new(turn_limits(), 0).is_ok());
    assert!(TaskLimits::new(turn_limits(), MAX_FOLLOWUPS).is_ok());
    assert_eq!(
        TaskLimits::new(turn_limits(), MAX_FOLLOWUPS + 1)
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
}

#[test]
fn git_identity_bounds_name_and_email() {
    assert!(GitIdentity::new("Ada", "ada@example.test").is_ok());
    assert_eq!(
        GitIdentity::new("x".repeat(257), "ada@example.test")
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
    assert_eq!(
        GitIdentity::new("Ada", "x".repeat(257))
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
    for (name, email) in [
        ("Ada\nLovelace", "ada@example.test"),
        ("Ada", "ada\u{7}@example.test"),
        ("Ada <hidden>", "ada@example.test"),
        ("Ada", "ada@example.test>"),
    ] {
        assert_eq!(
            GitIdentity::new(name, email).unwrap_err().public_code(),
            "TASK_CONFIG_INVALID",
            "{name:?} {email:?}"
        );
    }
}

#[test]
fn run_progress_counts_by_state() {
    let progress = RunProgress::from_states([
        TaskState::Queued,
        TaskState::Active,
        TaskState::Open,
        TaskState::Closed,
        TaskState::Abandoned,
        TaskState::Lost,
        TaskState::Closed,
    ]);
    assert_eq!(progress.total, 7);
    assert_eq!(progress.queued, 1);
    assert_eq!(progress.active, 1);
    assert_eq!(progress.open, 1);
    assert_eq!(progress.closed, 2);
    assert_eq!(progress.failed_like, 2);
}

#[test]
fn task_summary_contains_no_prompt_session_or_path_fields() {
    let json = serde_json::to_value(sample_record().summary()).unwrap();
    for field in [
        "prompt",
        "session_ref",
        "repo_id",
        "alternates_target",
        "path",
        "files_changed",
    ] {
        assert!(
            json.get(field).is_none(),
            "{field} must stay off the summary"
        );
    }
    assert_eq!(json["title"], "Fix the flaky login spec");
    assert_eq!(json["agent"], "codex");
}

#[test]
fn local_task_record_never_persists_the_transfer_alternates_path() {
    let bytes = sample_record().canonical_bytes().unwrap();
    let record = String::from_utf8(bytes).unwrap();

    assert!(!record.contains("alternates_target"));
    assert!(!record.contains("/tmp/objects"));
}

#[test]
fn legacy_fetch_only_local_record_remains_canonical_when_reopened_through_client_state() {
    let parsed: LocalTaskRecord = serde_json::from_str(LEGACY_FETCH_ONLY_LOCAL_RECORD).unwrap();
    assert_eq!(
        parsed.canonical_bytes().unwrap(),
        LEGACY_FETCH_ONLY_LOCAL_RECORD.as_bytes()
    );

    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    state.create_task(parsed.clone()).unwrap();
    let record_path = paths
        .state
        .join("tasks")
        .join(format!("{}.json", parsed.meta().task_id()));
    std::fs::write(&record_path, LEGACY_FETCH_ONLY_LOCAL_RECORD).unwrap();
    drop(state);

    let reopened = ClientStateStore::open(&paths.state).unwrap();
    assert_eq!(reopened.load_task(parsed.meta().task_id()).unwrap(), parsed);
    assert_eq!(reopened.list_tasks().unwrap(), vec![parsed]);
    assert_eq!(
        std::fs::read(record_path).unwrap(),
        LEGACY_FETCH_ONLY_LOCAL_RECORD.as_bytes()
    );
}

#[test]
fn local_task_record_never_persists_prompt_content() {
    let secret = "token=planted-secret at /Users/alice/.ssh/id_ed25519";
    let prompt = format!("Investigate the deployment failure\n\n{secret}");
    let record = LocalTaskRecord::new(
        TaskMeta::new(fields_with_prompt(prompt)).expect("fixture meta"),
        sample_status(),
        None,
        None,
        None,
        REPO_ID.to_owned(),
        None,
        true,
        None,
    )
    .expect("fixture record");

    let canonical = String::from_utf8(record.canonical_bytes().unwrap()).unwrap();

    assert!(!canonical.contains("\"prompt\""));
    assert!(!canonical.contains(secret));
    assert!(!canonical.contains("/Users/alice/.ssh/id_ed25519"));
}

#[test]
fn titles_use_the_first_non_empty_prompt_line() {
    let meta = TaskMeta::new(fields_with_prompt(
        "\n\nFix the tab\tlogin spec\nDetails".into(),
    ))
    .unwrap();
    assert_eq!(meta.title().as_str(), "Fix the tab\\tlogin spec");

    let long = format!("{}\ntrailer", "safe title word ".repeat(20));
    let meta = TaskMeta::new(fields_with_prompt(long)).unwrap();
    assert_eq!(meta.title().as_str().len(), 120);
    assert!(meta.title().as_str().starts_with("safe title word"));
}

#[test]
fn explicit_title_wins_and_derived_titles_are_redacted() {
    let mut fields = fields_with_prompt("sk-abcdefghijklmnopqrstuvwxyz012345\nDetails".into());
    fields.title = Some("Ship the billing client".into());
    let meta = TaskMeta::new(fields).unwrap();
    assert_eq!(meta.title().as_str(), "Ship the billing client");

    let home = std::env::var("HOME").unwrap_or_else(|_| "/Users/alice".into());
    let meta = TaskMeta::new(fields_with_prompt(format!(
        "read {home}/.ssh/id_ed25519 and Bearer leaked-title-token\nbody"
    )))
    .unwrap();
    assert!(!meta.title().as_str().contains(&home));
    assert!(!meta.title().as_str().contains("leaked-title-token"));
    assert!(meta.title().as_str().contains("[path]") || meta.title().as_str().contains("[token]"));
}

#[test]
fn runner_identity_wraps_process_identity() {
    let process = ProcessIdentity::new(9, 11).unwrap();
    let runner = RunnerIdentity::new(process);
    assert_eq!(runner.process_identity(), process);
    let json = serde_json::to_value(&runner).unwrap();
    assert_eq!(json["pid"], 9);
    assert_eq!(json["start_time_micros"], 11);
}

#[test]
fn origin_source_and_push_publication_are_valid_task_scopes() {
    let origin: TaskSource = serde_json::from_value(serde_json::json!({
        "kind": "origin",
        "url": "https://example.test/repo.git"
    }))
    .unwrap();
    assert!(matches!(origin, TaskSource::Origin { .. }));
    let push: PublishMode = serde_json::from_value(serde_json::json!("push")).unwrap();
    assert_eq!(push, PublishMode::Push);

    let mut origin_fields = fields_with_prompt("Ship it".into());
    origin_fields.source = origin;
    let origin_meta = TaskMeta::new(origin_fields).unwrap();
    assert_eq!(
        origin_meta.source(),
        &TaskSource::Origin {
            url: "https://example.test/repo.git".into(),
        }
    );

    let mut push_fields = fields_with_prompt("Ship it".into());
    push_fields.publish = vec![PublishMode::Fetch, push];
    push_fields.source = TaskSource::Local {
        wip: false,
        push_target: Some(PushTarget::new("https://example.test/repo.git".into()).unwrap()),
    };
    let push_meta = TaskMeta::new(push_fields).unwrap();
    assert_eq!(
        push_meta.publish(),
        &[PublishMode::Fetch, PublishMode::Push]
    );

    let mut wip_push_fields = fields_with_prompt("Ship it".into());
    wip_push_fields.source = TaskSource::Local {
        wip: true,
        push_target: Some(PushTarget::new("https://example.test/repo.git".into()).unwrap()),
    };
    wip_push_fields.publish = vec![PublishMode::Fetch, PublishMode::Push];
    let error = TaskMeta::new(wip_push_fields).unwrap_err();
    assert_eq!(error.public_code(), "PUBLISH_REQUIRES_COMMITTED_BASE");

    let mut branch_fields = fields_with_prompt("Ship it".into());
    branch_fields.publish_branch = Some("feature/ship-it".parse().unwrap());
    let error = TaskMeta::new(branch_fields).unwrap_err();
    assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
    assert!(error.to_string().contains("publish push"), "{error}");
}

#[test]
fn task_records_require_the_always_on_fetch_publish_mode() {
    let mut fields = fields_with_prompt("Keep the result importable".into());
    fields.publish.clear();

    let error = TaskMeta::new(fields).unwrap_err();
    assert_eq!(error.public_code(), "TASK_CONFIG_INVALID");
    assert!(error.to_string().contains("fetch"), "{error}");
}

#[test]
fn task_enumeration_waits_for_a_pre_exchange_replacement_writer() {
    // Break caught: list_tasks recovered a staged replace-<uuid> entry while
    // the writer still owned it, so the writer later failed its exact exchange.
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let (writer_entered_tx, writer_entered_rx) = mpsc::channel();
    let (writer_release_tx, writer_release_rx) = mpsc::channel();
    let state = Arc::new(
        ClientStateStore::open_with_concurrency_hook(
            &paths.state,
            Arc::new(PreExchangeReplacementGate {
                entered: writer_entered_tx,
                release: Mutex::new(writer_release_rx),
                used: AtomicBool::new(false),
            }),
        )
        .unwrap(),
    );
    let original = sample_record();
    let replacement = original.clone().with_runner(None).unwrap();
    let expected = replacement.clone();
    state.create_task(original).unwrap();

    thread::scope(|scope| {
        let writer_state = Arc::clone(&state);
        let writer = scope.spawn(move || writer_state.update_task(replacement.clone()));
        writer_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let reader_state = Arc::clone(&state);
        let contention = state.observe_next_lock_contention();
        let (listed_tx, listed_rx) = mpsc::channel();
        scope.spawn(move || {
            listed_tx.send(reader_state.list_tasks()).unwrap();
        });

        let reader_contended = contention.confirmed_within(Duration::from_secs(2));
        writer_release_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        assert!(
            reader_contended,
            "enumeration must wait for the pre-exchange writer"
        );
        assert_eq!(
            listed_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap(),
            vec![expected]
        );
    });
}

#[test]
fn task_enumeration_does_not_recover_replacement_residue() {
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let original = sample_record();
    let replacement = original.clone().with_runner(None).unwrap();
    state.create_task(original).unwrap();

    state.inject_task_replacement_after_exchange_failure_once();
    assert!(state.update_task(replacement.clone()).is_err());

    let residue_before = fs::read_dir(paths.state.join("tasks"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("replace-"))
        .count();
    assert_eq!(residue_before, 1);

    assert_eq!(state.list_tasks().unwrap(), vec![replacement.clone()]);

    let residue_after = fs::read_dir(paths.state.join("tasks"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("replace-"))
        .count();
    assert_eq!(residue_after, residue_before);

    state
        .update_task(
            replacement
                .with_status_observed_at(Some(1_700_000_000_001))
                .unwrap(),
        )
        .unwrap();
    assert!(
        fs::read_dir(paths.state.join("tasks"))
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().starts_with("replace-"))
    );
}

#[test]
fn reopen_recovers_run_reservation_replacement_residue_after_exchange_fsync_failure() {
    // Break caught: releasing a task-owned publish reservation can exchange
    // the updated run before its directory fsync fails. Reopening must remove
    // the displaced replace-<uuid> record and retain the released run.
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let run_id = run_id();
    let task_id = task_id();
    let branch: BranchName = "release-candidate".parse().unwrap();
    state
        .create_run(RunRecord::new(run_id, None, vec![task_id], 1, 99).unwrap())
        .unwrap();
    state
        .reserve_run_publish_branch_for_task(run_id, task_id, branch.clone())
        .unwrap();

    state.inject_write_failure_once(
        ClientStateWritePoint::AfterRunReplacementExchangeBeforeFirstDirectorySync,
    );
    let error = state
        .release_run_publish_branch_for_task(run_id, task_id, &branch)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("after run replacement exchange before first directory sync")
    );

    let reopened = ClientStateStore::open(&paths.state).unwrap();
    reopened
        .release_run_publish_branch_for_task(run_id, task_id, &branch)
        .unwrap();
    let runs = reopened.list_runs().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id(), run_id);
    assert!(runs[0].publish_branches().is_empty());
    assert!(
        std::fs::read_dir(paths.state.join("runs"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("replace-"))
    );
}

#[test]
fn run_enumeration_does_not_recover_replacement_residue() {
    let state_root = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(state_root.path().canonicalize().unwrap());
    let state = ClientStateStore::open(&paths.state).unwrap();
    let run_id = run_id();
    let task_id = task_id();
    let branch: BranchName = "release-candidate".parse().unwrap();
    state
        .create_run(RunRecord::new(run_id, None, vec![task_id], 1, 99).unwrap())
        .unwrap();
    state
        .reserve_run_publish_branch_for_task(run_id, task_id, branch.clone())
        .unwrap();

    state.inject_write_failure_once(
        ClientStateWritePoint::AfterRunReplacementExchangeBeforeFirstDirectorySync,
    );
    assert!(
        state
            .release_run_publish_branch_for_task(run_id, task_id, &branch)
            .is_err()
    );

    let residue_before = fs::read_dir(paths.state.join("runs"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("replace-"))
        .count();
    assert_eq!(residue_before, 1);

    let listed = state.list_runs().unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].publish_branches().is_empty());

    let residue_after = fs::read_dir(paths.state.join("runs"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("replace-"))
        .count();
    assert_eq!(residue_after, residue_before);
}

#[test]
fn new_worker_error_variants_map_to_documented_exit_kinds() {
    let cases = [
        (
            WorkerError::Git {
                code: "BASE_PUSH_FAILED",
                message: "push failed".into(),
            },
            ExitKind::Unavailable,
            69,
        ),
        (
            WorkerError::Git {
                code: "RESULT_FETCH_FAILED",
                message: "fetch failed".into(),
            },
            ExitKind::Unavailable,
            69,
        ),
        (
            WorkerError::Git {
                code: "WORKTREE_CREATE_FAILED",
                message: "create failed".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Git {
                code: "WORKTREE_INCONSISTENT",
                message: "inconsistent".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Git {
                code: "BASE_UNAVAILABLE",
                message: "missing".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Git {
                code: "PUBLISH_FAILED",
                message: "publish failed".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Agent {
                code: "AGENT_NOT_INSTALLED",
                message: "missing binary".into(),
            },
            ExitKind::Capacity,
            75,
        ),
        (
            WorkerError::Agent {
                code: "AGENT_NOT_AUTHENTICATED",
                message: "no login".into(),
            },
            ExitKind::Capacity,
            75,
        ),
        (
            WorkerError::Agent {
                code: "AGENT_UNSUPPORTED",
                message: "later agent".into(),
            },
            ExitKind::Usage,
            64,
        ),
        (
            WorkerError::Agent {
                code: "RESULT_UNPARSEABLE",
                message: "bad result".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Agent {
                code: "SESSION_UNBOUND",
                message: "no session".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Agent {
                code: "ENV_PROFILE_PERMISSIONS",
                message: "mode 0644".into(),
            },
            ExitKind::Infrastructure,
            70,
        ),
        (
            WorkerError::Task {
                code: "TASK_BUSY",
                message: "active".into(),
            },
            ExitKind::Usage,
            64,
        ),
        (
            WorkerError::Task {
                code: "FOLLOWUP_LIMIT",
                message: "too many".into(),
            },
            ExitKind::Usage,
            64,
        ),
        (
            WorkerError::Task {
                code: "TASK_CLOSED",
                message: "closed".into(),
            },
            ExitKind::Usage,
            64,
        ),
        (
            WorkerError::Task {
                code: "TASK_NOT_FOUND",
                message: "missing".into(),
            },
            ExitKind::Usage,
            64,
        ),
        (
            WorkerError::Task {
                code: "TASK_CONFIG_INVALID",
                message: "bad config".into(),
            },
            ExitKind::Usage,
            64,
        ),
        (
            WorkerError::Task {
                code: "RUNNER_HANDOFF_FAILED",
                message: "handoff".into(),
            },
            ExitKind::Io,
            74,
        ),
    ];

    for (error, kind, code) in cases {
        assert_eq!(error.exit_kind(), kind, "{error}");
        assert_eq!(error.exit_code(), code, "{error}");
        assert!(error.public_message().len() <= 4096);
        assert!(!error.public_message().contains("push failed"));
        assert!(!error.public_message().contains("missing binary"));
    }

    let limit = WorkerError::Agent {
        code: "AGENT_LIMIT_REACHED",
        message: "budget".into(),
    };
    assert_eq!(limit.exit_code(), 1);
    assert_eq!(limit.public_code(), "AGENT_LIMIT_REACHED");

    let exited = WorkerError::CommandExit { code: 3 };
    assert_eq!(exited.exit_code(), 3);
    assert_eq!(exited.public_code(), "COMMAND_EXIT");

    let outdated = WorkerError::Unavailable("HOST_LAYOUT_OUTDATED: migrate".into());
    assert_eq!(outdated.public_code(), "HOST_LAYOUT_OUTDATED");
    assert_eq!(outdated.exit_kind(), ExitKind::Infrastructure);
    assert_eq!(outdated.exit_code(), 70);

    let mapped = WorkerError::from(AdapterError::new("cursor is later"));
    assert_eq!(mapped.public_code(), "AGENT_UNSUPPORTED");
    assert_eq!(mapped.exit_kind(), ExitKind::Usage);
}

#[test]
fn run_record_round_trips_and_rejects_unknown_fields() {
    let record = RunRecord::new(run_id(), Some("batch-1".into()), vec![task_id()], 2, 99).unwrap();
    let bytes = record.canonical_bytes().unwrap();
    let parsed: RunRecord = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed.canonical_bytes().unwrap(), bytes);

    let mut unknown = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
    unknown["path"] = serde_json::json!("/secret");
    assert!(serde_json::from_value::<RunRecord>(unknown).is_err());
}

#[test]
fn run_record_reserves_each_publish_branch_only_once() {
    let record = RunRecord::new(run_id(), Some("batch-1".into()), vec![task_id()], 2, 99).unwrap();
    let branch: BranchName = "release-candidate".parse().unwrap();
    let reserved = record.reserve_publish_branch(branch.clone()).unwrap();
    assert_eq!(reserved.publish_branches(), std::slice::from_ref(&branch));
    let duplicate = reserved.reserve_publish_branch(branch).unwrap_err();
    assert_eq!(duplicate.public_code(), "TASK_CONFIG_INVALID");
}

#[test]
fn legacy_run_publish_reservation_remains_canonical_without_an_owner() {
    let parsed: RunRecord = serde_json::from_str(LEGACY_RUN_RECORD_WITH_RESERVATION).unwrap();
    assert_eq!(
        parsed.canonical_bytes().unwrap(),
        LEGACY_RUN_RECORD_WITH_RESERVATION.as_bytes()
    );
}

#[test]
fn runner_state_and_close_policy_are_documented_values() {
    assert_eq!(
        serde_json::to_value(RunnerState::Live).unwrap(),
        serde_json::json!("live")
    );
    assert_eq!(
        serde_json::to_value(ClosePolicy::Never).unwrap(),
        serde_json::json!("never")
    );
}

fn hex_id(value: u128) -> String {
    format!("{value:032x}")
}

fn arbitrary_prompt() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just('a'),
            Just('Z'),
            Just(' '),
            Just('\n'),
            Just('é'),
            Just('1')
        ],
        1..=240,
    )
    .prop_map(|characters| characters.into_iter().collect())
}

fn arbitrary_local_task_record() -> impl Strategy<Value = LocalTaskRecord> {
    (
        (0u128..16),
        prop::option::of(16u128..32),
        arbitrary_prompt(),
        prop::bool::ANY,
        prop::option::of("[A-Za-z0-9._@-]{1,16}".prop_map(|name| name)),
        prop::bool::ANY,
        0u64..10_000,
        prop::option::of(1u32..1000),
    )
        .prop_map(
            |(
                task,
                run,
                prompt,
                wip,
                pinned_worker,
                wait_for_capacity,
                created_at_millis,
                runner_pid,
            )| {
                let mut input = fields_with_prompt(prompt);
                input.task_id = TaskId::new(Uuid::from_u128(task + 1));
                input.run_id = run.map(|value| RunId::new(Uuid::from_u128(value)));
                input.source = TaskSource::Local {
                    wip,
                    push_target: None,
                };
                input.created_at_millis = created_at_millis;
                let meta = TaskMeta::new(input).expect("generated meta");
                let runner = runner_pid.map(|pid| {
                    RunnerIdentity::new(
                        ProcessIdentity::new(pid, created_at_millis.max(1))
                            .expect("generated process"),
                    )
                });
                LocalTaskRecord::new(
                    meta,
                    sample_status(),
                    None,
                    runner,
                    None,
                    REPO_ID.to_owned(),
                    pinned_worker,
                    wait_for_capacity,
                    None,
                )
                .expect("generated record")
            },
        )
}

proptest! {
    #[test]
    fn records_round_trip_canonically(record in arbitrary_local_task_record()) {
        let bytes = record.canonical_bytes().unwrap();
        let parsed: LocalTaskRecord = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(parsed.canonical_bytes().unwrap(), bytes);
    }
}

#[test]
fn hex_id_helper_matches_task_id_display() {
    assert_eq!(task_id().to_string(), hex_id(1));
}

#[test]
fn turn_summary_carries_an_optional_herdr_report() {
    use mac_worker::task::{HerdrTurnReport, HerdrTurnState};

    let plain = TurnSummary::new(1, turn_id(), None, None, None, false, None, None);
    let plain_json = serde_json::to_string(&plain).unwrap();
    assert!(!plain_json.contains("herdr"), "{plain_json}");

    let attached = plain.clone().with_herdr(Some(HerdrTurnReport {
        state: HerdrTurnState::Attached,
        pane_id: Some("w3:p1".into()),
    }));
    let json = serde_json::to_string(&attached).unwrap();
    assert!(
        json.contains(r#""herdr":{"state":"attached","pane_id":"w3:p1"}"#),
        "{json}"
    );
    let back: TurnSummary = serde_json::from_str(&json).unwrap();
    assert_eq!(back, attached);

    let old: TurnSummary = serde_json::from_str(&plain_json).unwrap();
    assert_eq!(old.herdr(), None);
    assert_eq!(old, plain);

    let unavailable: TurnSummary = serde_json::from_str(&json.replace(
        r#"{"state":"attached","pane_id":"w3:p1"}"#,
        r#"{"state":"unavailable"}"#,
    ))
    .unwrap();
    assert_eq!(
        unavailable.herdr().map(|report| report.state),
        Some(HerdrTurnState::Unavailable)
    );
    assert!(
        serde_json::from_str::<TurnSummary>(&json.replace(r#""pane_id""#, r#""pane""#)).is_err(),
        "the herdr report rejects unknown fields"
    );
}
