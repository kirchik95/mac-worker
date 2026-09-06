use std::collections::HashMap;

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    job::ProcessIdentity,
    protocol::PROTOCOL_VERSION,
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId, RunProgress, RunRecord,
        RunnerIdentity, RunnerState, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome,
        TaskSource, TaskState, TaskStatus, TurnId, TurnSummary, TurnTerminal,
    },
    task_view::{
        TaskDetailProjection, TaskFreshness, TaskListJson, TaskListProjection, project_task_detail,
        project_task_list, project_task_list_with_blocking_codes,
    },
};
use serde::Serialize;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const REPO_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const BASE_OID: &str = "0123456789abcdef0123456789abcdef01234567";
const TOKEN: &str = "deadbeefdeadbeefdeadbeefdeadbeef";

#[derive(Serialize)]
struct SnapshotProjection {
    #[serde(flatten)]
    projection: TaskListProjection,
}

fn task_id(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn run_id() -> RunId {
    RunId::new(Uuid::from_u128(10))
}

fn turn_id(value: &str) -> TurnId {
    value.parse().expect("valid turn ID fixture")
}

fn home_path() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/Users/alice".into())
}

fn task_limits() -> TaskLimits {
    TaskLimits::new(
        TurnLimits::new(30 * 60 * 1000, None, None).expect("valid turn limits"),
        10,
    )
    .expect("valid task limits")
}

fn task_meta(task: TaskId, title: &str, created_at_millis: u64) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: task,
        run_id: Some(run_id()),
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid: BASE_OID.parse().expect("valid base OID"),
        limits: task_limits(),
        close_policy: ClosePolicy::Done,
        env_profile: Some("production-secret-profile".into()),
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test")
            .expect("valid git identity"),
        title: Some(title.into()),
        prompt: format!(
            "prompt text must stay private: {TOKEN} {} session_ref=private",
            home_path()
        ),
        created_at_millis,
    })
    .expect("valid task metadata")
}

fn active_record() -> LocalTaskRecord {
    let task = task_id(1);
    let home = home_path();
    let turns = vec![
        TurnSummary::new(
            1,
            turn_id("018f0f4a6b5c7d8e9f00112233445566"),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            Some(1_700_000_000_001),
            Some(1_700_000_000_100),
        ),
        TurnSummary::new(
            2,
            turn_id("118f0f4a6b5c7d8e9f00112233445566"),
            None,
            None,
            None,
            false,
            Some(1_700_000_000_101),
            None,
        ),
    ];
    let status = TaskStatus::new(
        TaskState::Active,
        None,
        Some("mini-1".into()),
        true,
        Some(BASE_OID.parse().expect("valid head OID")),
        Some(format!(
            "<script>alert(1)</script> summary at {home}/private with {TOKEN}\nnext"
        )),
        vec![
            format!("{home}/repo/src/lib.rs").into(),
            "src/main.rs".into(),
            "~/private/token.txt".into(),
        ],
        vec![format!("{home}/repo/src/lib.rs"), "src/main.rs".into()],
        Some(format!("changed {home}/repo and Bearer {TOKEN}")),
        turns,
        1_700_000_000_200,
    )
    .expect("valid active status");
    LocalTaskRecord::new(
        task_meta(task, "Repair login", 1_700_000_000_000),
        status,
        None,
        Some(RunnerIdentity::new(
            ProcessIdentity::new(42, 1_700_000_000_001).expect("valid process identity"),
        )),
        None,
        REPO_ID.into(),
        Some("mini-1".into()),
        true,
        None,
    )
    .expect("valid active record")
}

fn terminal_record() -> LocalTaskRecord {
    let task = task_id(2);
    let status = TaskStatus::new(
        TaskState::Closed,
        Some(TaskOutcome::Failed {
            reason: format!("failed at {} with {TOKEN}", home_path()),
        }),
        Some("mini-1".into()),
        false,
        Some(BASE_OID.parse().expect("valid head OID")),
        Some("terminal summary".into()),
        vec!["needs input".into()],
        vec!["src/closed.rs".into()],
        Some("1 file changed".into()),
        vec![TurnSummary::new(
            1,
            turn_id("218f0f4a6b5c7d8e9f00112233445566"),
            Some(TurnTerminal::Failed),
            Some(TaskOutcome::Failed {
                reason: format!("failed at {} with {TOKEN}", home_path()),
            }),
            Some(false),
            true,
            Some(1_700_000_000_301),
            Some(1_700_000_000_400),
        )],
        1_700_000_000_300,
    )
    .expect("valid terminal status");
    LocalTaskRecord::new(
        task_meta(task, "Terminal task", 1_700_000_000_010),
        status,
        None,
        None,
        None,
        REPO_ID.into(),
        Some("mini-1".into()),
        true,
        None,
    )
    .expect("valid terminal record")
}

fn fixture_records() -> Vec<LocalTaskRecord> {
    let mut active = active_record();
    let active_meta = task_meta(
        task_id(1),
        &format!("Repair <b>login</b> at {}/repo", home_path()),
        1_700_000_000_000,
    );
    active = LocalTaskRecord::new(
        active_meta,
        active.status().clone(),
        active.status_observed_at_millis(),
        active.runner().cloned(),
        active.fetched_head().cloned(),
        active.repo_id().into(),
        active.pinned_worker().map(str::to_owned),
        active.wait_for_capacity(),
        active.abandon_code().map(str::to_owned),
    )
    .expect("active hostile metadata record");
    vec![terminal_record(), active]
}

fn fixture_runs() -> Vec<RunRecord> {
    vec![
        RunRecord::new(
            run_id(),
            Some(format!("sprint at {}/private", home_path())),
            vec![task_id(1), task_id(2)],
            1,
            1_700_000_000_000,
        )
        .expect("valid run record"),
    ]
}

fn fixture_projection() -> TaskListProjection {
    let runner_states = HashMap::from([(task_id(1), Some(RunnerState::Dead)), (task_id(2), None)]);
    let freshness = HashMap::from([
        (task_id(1), TaskFreshness::Current),
        (task_id(2), TaskFreshness::Stale),
    ]);
    project_task_list(
        &fixture_records(),
        &fixture_runs(),
        &runner_states,
        &freshness,
    )
    .expect("valid task projection")
}

fn fixture_detail() -> TaskDetailProjection {
    let record = active_record();
    project_task_detail(
        &record,
        record.status(),
        Some(RunnerState::Dead),
        TaskFreshness::Stale,
    )
    .expect("valid task detail projection")
}

fn queued_record() -> LocalTaskRecord {
    let record = terminal_record();
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        Some(record.meta().base_oid().clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        1_700_000_000_500,
    )
    .expect("valid queued status");
    LocalTaskRecord::new(
        record.meta().clone(),
        status,
        None,
        None,
        None,
        record.repo_id().to_owned(),
        None,
        true,
        None,
    )
    .expect("valid queued record")
}

#[test]
fn cli_and_snapshot_projection_fields_are_identical() {
    let projection = fixture_projection();
    let cli =
        serde_json::to_value(TaskListJson::new(PROTOCOL_VERSION, projection.clone())).unwrap();
    let snapshot = serde_json::to_value(SnapshotProjection { projection }).unwrap();

    assert_eq!(cli["tasks"], snapshot["tasks"]);
    assert_eq!(cli["runs"], snapshot["runs"]);
    assert_eq!(cli["progress"], snapshot["progress"]);
    assert_eq!(cli["protocol_version"], PROTOCOL_VERSION);
}

#[test]
fn rows_keep_run_position_and_safe_task_fields() {
    let projection = fixture_projection();

    assert_eq!(projection.tasks[0].run_position, Some(1));
    assert_eq!(projection.tasks[1].run_position, Some(2));
    assert_eq!(
        projection.tasks[0].branch.as_str(),
        format!("task/{}", projection.tasks[0].task_id)
    );
    assert_eq!(projection.tasks[0].runner, Some(RunnerState::Dead));

    let value = serde_json::to_value(projection).unwrap();
    assert_absent_keys_and_values(&value, &["prompt", "env", "session_ref", "ssh", "path"]);
}

#[test]
fn detail_timeline_is_derived_from_turn_records_without_raw_events() {
    let detail = fixture_detail();

    assert_eq!(detail.timeline.len(), detail.turns.len());
    assert_eq!(detail.timeline[0].turn_id, detail.turns[0].turn_id);
    assert!(detail.summary.as_deref().is_some());
    assert!(detail.questions.len() <= 16);
    assert!(
        detail
            .files_changed
            .iter()
            .all(|path| !path.starts_with('/'))
    );

    let value = serde_json::to_value(detail).unwrap();
    assert_absent_keys_and_values(&value, &["prompt", "env", "session_ref", "ssh"]);
    assert!(
        value.get("events").is_none(),
        "raw events must not be projected"
    );
}

#[test]
fn progress_counts_every_documented_state_for_the_list_and_run() {
    let projection = fixture_projection();

    assert_eq!(
        projection.progress,
        RunProgress {
            total: 2,
            queued: 0,
            active: 1,
            open: 0,
            closed: 1,
            failed_like: 0,
        }
    );
    assert_eq!(projection.runs.len(), 1);
    assert_eq!(projection.runs[0].progress, projection.progress);
}

#[test]
fn detail_redacts_failure_reasons_controls_paths_and_tokens() {
    let detail = fixture_detail();
    let value = serde_json::to_value(detail).unwrap();
    let encoded = serde_json::to_string(&value).unwrap();

    assert!(!encoded.contains(&home_path()));
    assert!(!encoded.contains(TOKEN));
    assert!(!encoded.contains("production-secret-profile"));
    assert!(!encoded.contains("prompt text must stay private"));
    assert!(!encoded.contains("session_ref=private"));
    assert!(encoded.contains("[path]"));
    assert!(!encoded.contains("~/private"));
    assert!(encoded.chars().all(|character| !character.is_control()));
}

#[test]
fn queued_task_projection_includes_blocking_code_in_cli_and_snapshot() {
    let task = queued_record();
    let task_id = task.meta().task_id();
    let blocking_codes = HashMap::from([(task_id, "CAPABILITY_MISSING".to_owned())]);
    let projection = project_task_list_with_blocking_codes(
        &[task],
        &[],
        &HashMap::new(),
        &HashMap::new(),
        &blocking_codes,
    )
    .expect("valid queued task projection");

    assert_eq!(
        projection.tasks[0].blocking_code.as_deref(),
        Some("CAPABILITY_MISSING")
    );
    let cli = serde_json::to_value(TaskListJson::new(PROTOCOL_VERSION, projection.clone()))
        .expect("serialize CLI projection");
    let snapshot = serde_json::to_value(SnapshotProjection { projection })
        .expect("serialize dashboard projection");
    assert_eq!(cli["tasks"], snapshot["tasks"]);
    assert_eq!(cli["tasks"][0]["blocking_code"], "CAPABILITY_MISSING");
}

fn assert_absent_keys_and_values(value: &serde_json::Value, forbidden: &[&str]) {
    let encoded = serde_json::to_string(value).unwrap();
    for key in forbidden {
        assert!(
            !encoded.contains(&format!("\"{key}\":")),
            "forbidden key {key}"
        );
    }
}
