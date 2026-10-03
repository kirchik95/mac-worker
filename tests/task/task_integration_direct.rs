use mac_worker::test_support::{
    agents::agent::{Question, ReportedCheck},
    integration::*,
    task::{model::*, prepared_followup::PreparedFollowup},
};
use serde_json::{Value, json};

// Strict wire decoders copied from T1 base ce7f62f (protocol 7).
// Opaque private title/session fields use Value; key and enum strictness stays frozen.
mod baseline {
    #![allow(dead_code)]
    use super::*;
    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum AgentKindWire {
        Codex,
        Claude,
        Cursor,
        Opencode,
    }
    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PermissionPolicyWire {
        Workspace,
        Unattended,
    }
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct TaskMeta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_import: Option<Value>,
        task_id: TaskId,
        run_id: Option<RunId>,
        project_id: String,
        worktree_id: String,
        agent: AgentKindWire,
        model: Option<String>,
        #[serde(default)]
        effort: Option<String>,
        policy: PermissionPolicyWire,
        #[serde(default)]
        effective_policy: Option<PermissionPolicyWire>,
        source: TaskSource,
        publish: Vec<PublishMode>,
        publish_branch: Option<BranchName>,
        base_oid: BaseOid,
        limits: TaskLimits,
        close_policy: ClosePolicy,
        env_profile: Option<String>,
        git_identity: GitIdentity,
        title: String,
        created_at_millis: u64,
    }
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct TaskStatus {
        state: TaskState,
        last_outcome: Option<TaskOutcome>,
        worker: Option<String>,
        session_present: bool,
        head_oid: Option<BaseOid>,
        summary: Option<String>,
        questions: Vec<Question>,
        files_changed: Vec<String>,
        diff_stat: Option<String>,
        turns: Vec<TurnSummary>,
        updated_at_millis: u64,
        #[serde(default)]
        reported_checks: Vec<ReportedCheck>,
    }
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct PreparedFollowup {
        #[serde(default, skip_serializing_if = "is_false")]
        auto_continue: bool,
        expected: LocalTaskRecord,
        turn_id: TurnId,
        turn_number: u32,
        created_at_millis: u64,
        message: String,
        composed_prompt: String,
        base_oid: BaseOid,
        agent: String,
        model: Option<String>,
        worker: String,
        max_followups: u32,
    }
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct FrozenSubmitBody {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub session_import: Option<Value>,
        /// Explicit override only: older controllers reject unknown fields.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub questions: Option<QuestionsPolicy>,
        pub task_id: TaskId,
        pub turn_id: TurnId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub run_id: Option<RunId>,
        pub created_at_millis: u64,
        pub prompt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub title: Option<String>,
        pub agent: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub effort: Option<String>,
        pub source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub origin_url: Option<String>,
        pub publish: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub publish_branch: Option<String>,
        pub close_on: ClosePolicy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub env_profile: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub worker: Option<String>,
        pub wip: bool,
        pub project_id: String,
        pub worktree_id: String,
        pub base_oid: BaseOid,
        pub timeout_millis: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub max_turns: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub max_budget_usd_cents: Option<u64>,
        pub max_followups: u32,
        pub permissions: String,
        pub requires: Vec<String>,
        pub include_untracked: Vec<String>,
        pub include_empty_dirs: Vec<String>,
        pub allow_sensitive: Vec<String>,
        pub cli_includes: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub branch: Option<String>,
        /// CLI `--no-wait` inverts this. Omitted old bodies default true, preserving
        /// capacity behavior.
        #[serde(default = "default_true")]
        pub wait_for_capacity: bool,
    }
    #[derive(Debug, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct DagFrozenSpec {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub questions: Option<QuestionsPolicy>,
        pub prompt: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub title: Option<String>,
        pub agent: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub effort: Option<String>,
        pub source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub origin_url: Option<String>,
        pub publish: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub publish_branch: Option<String>,
        pub close_on: ClosePolicy,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub env_profile: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub worker: Option<String>,
        pub wip: bool,
        pub project_path: String,
        pub project_id: String,
        pub worktree_id: String,
        pub timeout_millis: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub max_turns: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub max_budget_usd_cents: Option<u64>,
        pub max_followups: u32,
        /// Resolved `PermissionPolicy` (`workspace` or `unattended`).
        pub permissions: String,
        pub requires: Vec<String>,
        pub include_untracked: Vec<String>,
        pub include_empty_dirs: Vec<String>,
        pub allow_sensitive: Vec<String>,
        pub cli_includes: Vec<String>,
        /// Branch frozen at initial batch submit for first-turn prompt composition.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub branch: Option<String>,
    }
    fn default_true() -> bool {
        true
    }
    fn is_false(value: &bool) -> bool {
        !value
    }
}

#[test]
fn disabled_ordinary_status_meta_and_followup_keep_baseline_strict_keys_and_bytes() {
    let ordinary = sample_ordinary(fixture_task(), fixture_source());
    let prepared = sample_prepared_turn(
        &sample_record(fixture_task(), fixture_source(), "main"),
        IntegrationTurnPurpose::Resolve,
        1,
        1,
    );
    for (kind, wire) in [
        ("meta", serde_json::to_value(ordinary.meta()).unwrap()),
        ("status", serde_json::to_value(ordinary.status()).unwrap()),
        (
            "followup",
            serde_json::to_value(&prepared.followup).unwrap(),
        ),
    ] {
        let accept = |wire: Value| match kind {
            "meta" => serde_json::from_value::<baseline::TaskMeta>(wire).is_ok(),
            "status" => serde_json::from_value::<baseline::TaskStatus>(wire).is_ok(),
            _ => serde_json::from_value::<baseline::PreparedFollowup>(wire).is_ok(),
        };
        assert!(accept(wire.clone()), "{kind}");
        for key in ["integration", "workflow_state", "integrate", "verify_merge"] {
            let mut changed = wire.clone();
            changed[key] = json!(null);
            assert!(!accept(changed), "{kind}:{key}");
        }
    }
    let bytes = serde_json::to_vec(ordinary.meta()).unwrap();
    let decoded: TaskMeta = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let bytes = serde_json::to_vec(ordinary.status()).unwrap();
    let decoded: TaskStatus = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
    let bytes = serde_json::to_vec(&prepared.followup).unwrap();
    let decoded: PreparedFollowup = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
}

fn frozen() -> Value {
    json!({
        "task_id":fixture_task(),"turn_id":fixture_source(),"created_at_millis":1000,
        "prompt":"work","agent":"codex","source":"local","origin_url":"https://example.test/repo.git",
        "publish":["fetch"],"close_on":"never","wip":false,
        "project_id":"a".repeat(64),"worktree_id":"b".repeat(64),"base_oid":"b".repeat(40),
        "timeout_millis":2700000,"max_followups":10,"permissions":"workspace","requires":[],
        "include_untracked":[],"include_empty_dirs":[],"allow_sensitive":[],"cli_includes":[],
        "wait_for_capacity":true
    })
}

#[test]
fn disabled_submit_and_dag_remain_accepted_by_copied_baseline_decoders() {
    let submit: FrozenSubmitBody = serde_json::from_value(frozen()).unwrap();
    let bytes = serde_json::to_vec(&submit).unwrap();
    serde_json::from_slice::<baseline::FrozenSubmitBody>(&bytes).unwrap();
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<FrozenSubmitBody>(&bytes).unwrap()).unwrap(),
        bytes
    );
    let mut dag = frozen();
    for key in [
        "task_id",
        "turn_id",
        "created_at_millis",
        "base_oid",
        "wait_for_capacity",
    ] {
        dag.as_object_mut().unwrap().remove(key);
    }
    dag["project_path"] = json!("/fixture/project");
    let dag: DagFrozenSpec = serde_json::from_value(dag).unwrap();
    let bytes = serde_json::to_vec(&dag).unwrap();
    serde_json::from_slice::<baseline::DagFrozenSpec>(&bytes).unwrap();
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<DagFrozenSpec>(&bytes).unwrap()).unwrap(),
        bytes
    );
    let mut wrapped = serde_json::to_value(&submit).unwrap();
    wrapped["integration"] = serde_json::to_value(sample_policy("main")).unwrap();
    assert!(serde_json::from_value::<baseline::FrozenSubmitBody>(wrapped).is_err());
}

fn integration_fixture(capable: bool) -> super::session_import_e2e::Fixture {
    let f = super::session_import_e2e::Fixture::new();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    f.project
        .git(&["clone", "--bare", ".", origin.to_str().unwrap()]);
    f.project.git(&[
        "remote",
        "add",
        "origin",
        &format!("file://{}", origin.display()),
    ]);
    let mut config = std::fs::read_to_string(&f.config).unwrap();
    config.push_str("capabilities = ['origin:file']\n");
    std::fs::write(&f.config, config).unwrap();
    if capable {
        let ssh = std::fs::read_to_string(&f.ssh).unwrap().replace(
            "probe.update(memory_pressure",
            "probe['features'].append('task.integration')\n    probe.update(memory_pressure",
        );
        std::fs::write(&f.ssh, ssh).unwrap();
    }
    f
}

#[test]
fn direct_import_is_complete_and_host_is_armed_before_the_first_launch() {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let before = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap();
    let check = "find \"$HOME/.local/share/mac-worker/host/tasks\" -name policy.json > \"$HOME/armed-policies\"\n[ -s \"$HOME/armed-policies\" ] || exit 94\n";
    std::fs::write(
        &agent,
        script.replace(
            "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
            &format!("{check}printf '%s\\n' \"$@\" > \"$HOME/argv\""),
        ),
    )
    .unwrap();
    let output = f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "continue safely",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]);
    assert!(
        output.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .rfind(|v| v.get("session_import").is_some())
        .unwrap();
    let task: TaskId = report["task_id"].as_str().unwrap().parse().unwrap();
    let owner_policy = f
        .laptop
        .join(".local/state/mac-worker/integrations/tasks")
        .join(task.to_string())
        .join("policy.json");
    let policy: FrozenIntegrationPolicy =
        serde_json::from_slice(&std::fs::read(owner_policy).unwrap()).unwrap();
    assert_eq!(policy.requested_close, ClosePolicy::Never);
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    let prepare = journal.find("host task-prepare").unwrap();
    let arm = journal.find("host task-integration\n").unwrap();
    let launch = journal.find("host task-turn").unwrap();
    assert!(prepare < arm && arm < launch, "{journal}");
    let store = mac_worker::test_support::host::store::HostStore::open(&f.host_root()).unwrap();
    let task_dir = store.task_dir(&policy.project_id, task).unwrap();
    let receipt: Value =
        serde_json::from_slice(&std::fs::read(task_dir.join("session-import.json")).unwrap())
            .unwrap();
    assert_eq!(receipt["stage"], "complete");
    assert_eq!(
        std::fs::read(source).unwrap(),
        before,
        "integration recaptured the source transcript"
    );
}

#[test]
fn direct_missing_helper_feature_refuses_before_pins_and_admission() {
    let f = integration_fixture(false);
    let output = f.worker(&[
        "--json",
        "task",
        "submit",
        "--prompt",
        "work",
        "--integrate",
        "main",
        "--worker",
        "fixture",
        "--no-wait",
    ]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("INTEGRATION_UNAVAILABLE"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let tasks = mac_worker::test_support::client_state::ClientStateStore::open(
        &f.laptop.join(".local/state/mac-worker"),
    )
    .unwrap();
    assert!(tasks.list_tasks().unwrap().is_empty());
    assert!(!f.laptop.join(".cache/mac-worker/transfer").exists());
    assert!(!f.host.join("argv").exists());
}
