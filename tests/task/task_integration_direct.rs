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

fn owner_paths(
    f: &super::session_import_e2e::Fixture,
) -> mac_worker::test_support::core::paths::PathLayout {
    mac_worker::test_support::core::paths::PathLayout {
        config: f.config.clone(),
        state: f.laptop.join(".local/state/mac-worker"),
        cache: f.laptop.join(".cache/mac-worker"),
        data: f.laptop.join(".local/share/mac-worker"),
    }
}

fn submitted_task(output: &std::process::Output) -> TaskId {
    assert!(
        output.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .rfind(|v| v.get("session_import").is_some())
        .unwrap()["task_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn terminal_child_imports_and_acknowledges_the_merge_before_done_close() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        host::{process::SystemProcessRunner, store::HostStore},
        session::SessionAgent,
        task::store::TaskStore,
    };
    let f = integration_fixture(true);
    f.capture_fixture(SessionAgent::Codex);
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
        "printf 'ordinary result\\n' > T6-result\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
    );
    std::fs::write(agent, script).unwrap();
    let output = f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce work",
        "--integrate",
        "main",
        "--close-on",
        "done",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]);
    let task = submitted_task(&output);
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        wait.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&wait.stdout),
        String::from_utf8_lossy(&wait.stderr)
    );
    let paths = owner_paths(&f);
    let owner = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let record = owner
        .load(task)
        .unwrap()
        .expect("terminal runner must stage an integration cycle");
    assert_eq!(record.snapshot.state, IntegrationStatus::Integrated);
    let receipt = record.receipt.as_ref().unwrap();
    assert!(receipt.imported);
    let merge = receipt
        .merge_oid
        .as_ref()
        .expect("ordinary output needs one merge");
    assert_ne!(merge, &receipt.source_head);
    let local = ClientStateStore::open(&paths.state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(local.status().state(), TaskState::Closed);
    assert_eq!(local.status().head_oid(), Some(merge));
    assert_eq!(local.fetched_head(), Some(merge));
    let host = HostStore::open(&f.host_root()).unwrap();
    let retained = HostIntegrationStore::new(&host)
        .load(&record.policy.project_id, task)
        .unwrap()
        .unwrap();
    assert!(
        retained.receipt.unwrap().imported,
        "host must receive the import acknowledgement before close"
    );
    assert_eq!(
        TaskStore::new(&host, &SystemProcessRunner)
            .load_status(&record.policy.project_id, task)
            .unwrap()
            .state(),
        TaskState::Closed
    );
    assert_eq!(
        f.project
            .git(&["show", &format!("{merge}:T6-result")])
            .stdout,
        b"ordinary result\n"
    );
    let driver: Value = serde_json::from_slice(
        &std::fs::read(
            paths
                .state
                .join(format!("integrations/tasks/{task}/driver.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_ne!(
        driver["actor"]["pid"].as_u64(),
        Some(std::process::id() as u64)
    );
}

fn parked_source_fixture() -> (super::session_import_e2e::Fixture, TaskId) {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    f.capture_fixture(SessionAgent::Codex);
    f.install_agent(SessionAgent::Codex);
    let quote = |s: &str| format!("'{}'", s.replace('\'', "'\"'\"'"));
    let drain = format!(
        "printf 'ordinary result\\n' > T6-result\n/usr/bin/env HOME={} {} --config {} controller drain >/dev/null || exit 94\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
        quote(f.laptop.to_str().unwrap()),
        quote(env!("CARGO_BIN_EXE_worker")),
        quote(f.config.to_str().unwrap())
    );
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent)
        .unwrap()
        .replace("printf '%s\\n' \"$@\" > \"$HOME/argv\"", &drain);
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce work",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    assert_eq!(
        owner.load(task).unwrap().unwrap().snapshot.state,
        IntegrationStatus::Parked
    );
    (f, task)
}

#[test]
fn native_recovery_reclaims_a_confirmed_dead_phase_actor_before_reexecuting_the_driver() {
    use mac_worker::test_support::{
        client_state::RunnerLivenessVerdict, host::supervisor::SystemProcessInspector,
    };
    use std::{
        io::Write,
        os::unix::fs::OpenOptionsExt,
        process::{Command, Stdio},
    };
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let mut child = Command::new("/bin/cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let actor = SystemProcessInspector.identity_for_pid(child.id()).unwrap();
    let runtime = std::sync::Arc::new(ManualIntegrationRuntime::default());
    runtime.set_actor_verdict(actor, RunnerLivenessVerdict::Live);
    let state = RootedIntegrationState::open(&paths, runtime).unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let expected = record.snapshot.revision;
    record.snapshot.state = IntegrationStatus::Fetching;
    record.snapshot.resume_state = None;
    record.snapshot.pause_reason = None;
    record.pause = None;
    record.actor = Some(actor);
    record.snapshot.revision = expected.next().unwrap();
    state.replace(task, expected, &record).unwrap();
    state
        .reserve(
            &record.target_key,
            record.snapshot.integration_id,
            record.snapshot.epoch,
            actor,
        )
        .unwrap()
        .unwrap();
    let binding = json!({"task": task, "intent": record.snapshot.integration_id, "epoch": record.snapshot.epoch, "actor": actor});
    let path = paths
        .state
        .join(format!("integrations/tasks/{task}/driver.json"));
    let mut binding_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    binding_file
        .write_all(&serde_json::to_vec(&binding).unwrap())
        .unwrap();
    binding_file.sync_all().unwrap();
    let undrain = f.worker(&["--json", "controller", "drain", "--off"]);
    assert!(
        undrain.status.success(),
        "{}",
        String::from_utf8_lossy(&undrain.stdout)
    );
    let reconcile = f.worker(&["--json", "task", "reconcile"]);
    assert!(reconcile.status.success());
    assert_eq!(
        state.load(task).unwrap().unwrap().actor,
        Some(actor),
        "live phase was reclaimed"
    );
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
    let settled = wait_integrated(&f, task);
    assert!(settled.actor.is_none());
    assert!(settled.receipt.unwrap().imported);
}

#[test]
fn native_close_revokes_a_parked_cycle_without_starting_a_git_phase() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let (f, task) = parked_source_fixture();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    let before = f
        .project
        .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
        .stdout;
    let close = f.worker(&["--json", "task", "close", &task.to_string()]);
    assert!(
        close.status.success(),
        "{}",
        String::from_utf8_lossy(&close.stdout)
    );
    let state = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let stopped = state.load(task).unwrap().unwrap();
    assert_eq!(stopped.snapshot.state, IntegrationStatus::Revoked);
    assert!(stopped.tombstone.unwrap().acknowledged);
    assert_eq!(stopped.snapshot.attempts, 0);
    assert_eq!(
        ClientStateStore::open(&owner_paths(&f).state)
            .unwrap()
            .load_task(task)
            .unwrap()
            .status()
            .state(),
        TaskState::Closed
    );
    assert_eq!(
        f.project
            .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
            .stdout,
        before
    );
}

#[test]
fn native_explicit_redrive_advances_one_blocked_epoch_and_recovers_via_reexec() {
    use mac_worker::test_support::{
        client_state::{ClientStateStore, dag::ParentGate},
        core::config::Config,
        host::process::SystemProcessRunner,
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let expected = record.snapshot.revision;
    record.snapshot.revision = expected.next().unwrap();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.resume_state = None;
    record.snapshot.pause_reason = None;
    record.pause = None;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
    record.snapshot.retry_exhausted = true;
    state.replace(task, expected, &record).unwrap();
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let config = Config::load(&paths.config).unwrap();
    let client = TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &tasks,
        &InlineRunnerExecutor,
    );
    assert_eq!(
        client
            .integration_parent_gate(&tasks.load_task(task).unwrap())
            .unwrap(),
        ParentGate::Waiting
    );
    let undrain = f.worker(&["--json", "controller", "drain", "--off"]);
    assert!(undrain.status.success());
    let redrive = f.worker(&["--json", "task", "integrate", &task.to_string()]);
    assert!(
        redrive.status.success(),
        "{}",
        String::from_utf8_lossy(&redrive.stdout)
    );
    let pending: Value = serde_json::from_slice(&redrive.stdout).unwrap();
    assert_eq!(pending["integration"]["epoch"], 1);
    let done = wait_integrated(&f, task);
    assert_eq!(done.snapshot.integration_id, record.snapshot.integration_id);
    assert_eq!(done.snapshot.epoch, 1);
    assert!(done.receipt.unwrap().imported);
    assert_eq!(
        client
            .integration_parent_gate(&tasks.load_task(task).unwrap())
            .unwrap(),
        ParentGate::Ready
    );
}

#[test]
fn a_brief_helper_rollback_parks_and_restores_the_same_open_source_cycle() {
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let before = state.load(task).unwrap().unwrap();
    let policy = state.load_policy(task).unwrap().unwrap();
    let ssh = std::fs::read_to_string(&f.ssh).unwrap();
    assert!(ssh.contains("probe['features'].append('task.integration')"));
    std::fs::write(
        &f.ssh,
        ssh.replace(
            "probe['features'].append('task.integration')",
            "pass # previous helper fixture",
        ),
    )
    .unwrap();
    let journal_before = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let waited = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "15s",
    ]);
    assert!(String::from_utf8_lossy(&waited.stdout).contains("WAIT_TIMEOUT"));
    let parked = state.load(task).unwrap().unwrap();
    assert_eq!(parked.snapshot.state, IntegrationStatus::Parked);
    assert_eq!(
        parked.snapshot.pause_reason,
        Some(IntegrationPauseReason::HelperUnavailable)
    );
    assert_eq!(
        parked.snapshot.integration_id,
        before.snapshot.integration_id
    );
    assert_eq!(parked.snapshot.epoch, before.snapshot.epoch);
    assert_eq!(parked.snapshot.source_head, before.snapshot.source_head);
    assert_eq!(parked.cycle_base, before.cycle_base);
    assert_eq!(parked.followups_spent, before.followups_spent);
    assert_eq!(parked.snapshot.attempts, before.snapshot.attempts);
    assert_eq!(state.load_policy(task).unwrap().unwrap(), policy);
    let during = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(
        during.matches("host task-prepare\n").count(),
        journal_before.matches("host task-prepare\n").count()
    );
    assert_eq!(
        during.matches("host task-integration\n").count(),
        journal_before.matches("host task-integration\n").count()
    );
    std::fs::write(&f.ssh, ssh).unwrap();
    let restored = wait_integrated(&f, task);
    assert_eq!(
        restored.snapshot.integration_id,
        before.snapshot.integration_id
    );
    assert_eq!(restored.snapshot.epoch, before.snapshot.epoch);
    assert_eq!(restored.followups_spent, before.followups_spent);
    assert!(restored.receipt.unwrap().imported);
    assert_eq!(
        std::fs::read_to_string(f.host.join("ssh-journal"))
            .unwrap()
            .matches("host task-prepare\n")
            .count(),
        1
    );
}

#[test]
fn native_wait_returns_the_blocked_code_after_successful_source_import() {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    f.capture_fixture(SessionAgent::Codex);
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(r#"\"files_changed\":[]"#, r#"\"files_changed\":[],\"checks\":[{\"name\":\"fixture-check\",\"command\":\"fixture-check\",\"status\":\"fail\",\"detail\":\"fixture failed\"}]"#);
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "work with failing checks",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--wait",
    ]));
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert_eq!(
        wait.status.code(),
        Some(i32::from(
            IntegrationCode::IntegrationChecksFailed.error().exit_code()
        )),
        "{}",
        String::from_utf8_lossy(&wait.stdout)
    );
    let state = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    assert_eq!(
        state.load(task).unwrap().unwrap().snapshot.blocked_code,
        Some(IntegrationCode::IntegrationChecksFailed)
    );
}

#[test]
fn native_dag_uses_the_imported_parent_merge_for_its_configured_child() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        core::config::Config,
        host::process::SystemProcessRunner,
        task::{client::TaskClient, turn_runner::InlineRunnerExecutor},
    };
    let f = integration_fixture(true);
    let agent = f.host.join("bin/codex");
    std::fs::write(&agent, r#"#!/bin/sh
case "$1" in --version) printf '0.160.0\n'; exit 0;; auth|login) printf '{"loggedIn":true}\n'; exit 0;; esac
printf '%s\n' "$@" >> "$HOME/dag-agent-argv"
printf 'fixture task result\n' >> dag-result
printf '%s\n' '{"type":"thread.started","thread_id":"00000000-0000-0000-0000-000000000031"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"dag work done\",\"questions\":[],\"files_changed\":[],\"checks\":[]}"}}'
"#).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        f.project.root().join(".worker.toml"),
        "[task]\nintegrate = 'main'\n",
    )
    .unwrap();
    let batch = f.project.root().join("tasks.toml");
    std::fs::write(&batch, "close_on = 'never'\n[[tasks]]\nid = 'parent'\nprompt = 'parent work'\n[[tasks]]\nid = 'child'\nprompt = 'child work'\ndepends_on = ['parent']\nbase = 'from:parent'\n").unwrap();
    mac_worker::test_support::controller::drain::set_drained(
        &owner_paths(&f).controller_state_root(),
        true,
    )
    .unwrap();
    let output = f.worker(&["--json", "task", "batch", batch.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    let run: RunId = report["run_id"].as_str().unwrap().parse().unwrap();
    let paths = owner_paths(&f);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let dag = tasks.load_run_dag(run).unwrap().unwrap();
    let parent = dag.nodes["parent"].task_id;
    let child = dag.nodes["child"].task_id;
    let config = Config::load(&paths.config).unwrap();
    // Advancing is done by real finalizers/wait/driver. This read proves the
    // unimported source cannot release its configured dependency.
    let client = TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &tasks,
        &InlineRunnerExecutor,
    );
    assert!(tasks.load_task_optional(child).unwrap().is_none());
    assert_ne!(
        client
            .integration_parent_gate(&tasks.load_task(parent).unwrap())
            .unwrap(),
        mac_worker::test_support::client_state::dag::ParentGate::Ready
    );
    assert!(f.worker(&["controller", "drain", "--off"]).status.success());
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--run",
        &run.to_string(),
        "--timeout",
        "90s",
    ]);
    assert!(
        wait.status.success(),
        "{}",
        String::from_utf8_lossy(&wait.stdout)
    );
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let parent_record = state.load(parent).unwrap().unwrap();
    let receipt = parent_record.receipt.unwrap();
    assert!(receipt.imported);
    assert_eq!(
        tasks.load_task(child).unwrap().meta().base_oid(),
        receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head)
    );
    assert_eq!(
        state.load(child).unwrap().unwrap().snapshot.state,
        IntegrationStatus::Integrated
    );
    assert_eq!(
        tasks.load_task(parent).unwrap().status().state(),
        TaskState::Open
    );
}

#[test]
fn native_mismatched_from_parent_policy_refuses_before_capture_or_admission() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let f = integration_fixture(true);
    let batch = f.project.root().join("tasks.toml");
    std::fs::write(&batch, "[[tasks]]\nid = 'parent'\nprompt = 'parent work'\nintegrate = 'main'\n[[tasks]]\nid = 'child'\nprompt = 'child work'\nintegrate = 'different'\ndepends_on = ['parent']\nbase = 'from:parent'\n").unwrap();
    let output = f.worker(&["--json", "task", "batch", batch.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("TASK_CONFIG_INVALID"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let paths = owner_paths(&f);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    assert!(tasks.list_tasks().unwrap().is_empty());
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    assert!(!paths.cache.join("transfer").exists());
    assert!(!f.host.join("dag-agent-argv").exists());
}

fn refuse_integration_transport(f: &super::session_import_e2e::Fixture) -> String {
    let original = std::fs::read_to_string(&f.ssh).unwrap();
    std::fs::write(&f.ssh, original.replace("os.execv('/bin/sh', ['/bin/sh', '-c', command])", "if command.endswith(' host task-integration'):\n    sys.stdin.buffer.read()\n    sys.exit(255)\nos.execv('/bin/sh', ['/bin/sh', '-c', command])")).unwrap();
    original
}

#[test]
fn native_offline_cancel_close_and_discard_remain_unconfirmed_until_revoke_proof() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let original_ssh = refuse_integration_transport(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let initial = state.load(task).unwrap().unwrap();
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    for args in [
        vec!["--json", "task", "cancel"],
        vec!["--json", "task", "close"],
        vec!["--json", "task", "close", "--discard"],
    ] {
        let id = task.to_string();
        let mut arguments = args;
        arguments.push(&id);
        let refused = f.worker(&arguments);
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stdout).contains("INTEGRATION_STOP_UNCONFIRMED"),
            "{}",
            String::from_utf8_lossy(&refused.stdout)
        );
        let pending = state.load(task).unwrap().unwrap();
        assert_eq!(
            pending.snapshot.integration_id,
            initial.snapshot.integration_id
        );
        assert_eq!(pending.snapshot.epoch, initial.snapshot.epoch);
        assert!(!pending.tombstone.unwrap().acknowledged);
        assert_eq!(
            tasks.load_task(task).unwrap().status().state(),
            TaskState::Open
        );
    }
    std::fs::write(&f.ssh, original_ssh).unwrap();
    assert!(
        f.worker(&["--json", "task", "cancel", &task.to_string()])
            .status
            .success()
    );
    assert!(
        state
            .load(task)
            .unwrap()
            .unwrap()
            .tombstone
            .unwrap()
            .acknowledged
    );
    assert!(
        f.worker(&["--json", "task", "close", "--discard", &task.to_string()])
            .status
            .success()
    );
    assert_eq!(
        tasks.load_task(task).unwrap().status().state(),
        TaskState::Abandoned
    );
    assert_eq!(state.load(task).unwrap().unwrap().snapshot.attempts, 0);
}

#[test]
fn native_interrupted_say_waits_for_revoke_then_queues_one_ordinary_turn() {
    use mac_worker::test_support::client_state::ClientStateStore;
    let (f, task) = parked_source_fixture();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let mut record = state.load(task).unwrap().unwrap();
    let expected = record.snapshot.revision;
    record.snapshot.revision = expected.next().unwrap();
    record.snapshot.state = IntegrationStatus::Blocked;
    record.snapshot.blocked_code = Some(IntegrationCode::IntegrationWorkerOffline);
    record.snapshot.resume_state = None;
    record.snapshot.pause_reason = None;
    record.pause = None;
    state.replace(task, expected, &record).unwrap();
    let original_ssh = refuse_integration_transport(&f);
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let say = || {
        f.worker(&[
            "--json",
            "task",
            "say",
            &task.to_string(),
            "--message",
            "ordinary replacement",
        ])
    };
    let refused = say();
    assert!(
        String::from_utf8_lossy(&refused.stdout).contains("INTEGRATION_STOP_UNCONFIRMED"),
        "{}",
        String::from_utf8_lossy(&refused.stdout)
    );
    assert_eq!(tasks.load_task(task).unwrap().status().turns().len(), 1);
    assert!(tasks.queue_snapshot().unwrap().entries().is_empty());
    std::fs::write(&f.ssh, original_ssh).unwrap();
    let queued = say();
    assert!(
        queued.status.success(),
        "{}",
        String::from_utf8_lossy(&queued.stdout)
    );
    assert_eq!(tasks.load_task(task).unwrap().status().turns().len(), 2);
    assert_eq!(tasks.queue_snapshot().unwrap().entries().len(), 1);
    assert!(
        state
            .load(task)
            .unwrap()
            .unwrap()
            .tombstone
            .unwrap()
            .acknowledged
    );
    assert_eq!(state.load(task).unwrap().unwrap().snapshot.attempts, 0);
}

struct ReleaseFixtureFifo(std::path::PathBuf);
impl Drop for ReleaseFixtureFifo {
    fn drop(&mut self) {
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.0)
        {
            let _ = file.write_all(b"continue\n");
        }
    }
}
fn fixture_fifo(path: &std::path::Path) {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
}

#[test]
fn native_cancel_with_a_lost_push_reply_imports_the_committed_success_before_close() {
    use mac_worker::test_support::{client_state::ClientStateStore, session::SessionAgent};
    use std::io::BufRead;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original_source = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
        "printf 'lost reply source\\n' > T6-result\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
    );
    std::fs::write(agent, script).unwrap();
    let ready = f.laptop.parent().unwrap().join("push-ready");
    let release = f.laptop.parent().unwrap().join("push-release");
    fixture_fifo(&ready);
    fixture_fifo(&release);
    let release_on_drop = ReleaseFixtureFifo(release.clone());
    let interception = format!(
        r#"if command.endswith(' host task-integration'):
    data = sys.stdin.buffer.read()
    action = json.loads(data)['action']
    result = subprocess.run(['/bin/sh','-c',command], input=data, capture_output=True)
    if action.get('step') == 'push' and result.returncode == 0:
        with open({ready:?}, 'w') as f: f.write('published\n')
        with open({release:?}, 'r') as f: f.readline()
        sys.exit(255)
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    sys.exit(result.returncode)
os.execv('/bin/sh', ['/bin/sh', '-c', command])"#,
        ready = ready.to_str().unwrap(),
        release = release.to_str().unwrap()
    );
    let ssh = std::fs::read_to_string(&f.ssh).unwrap().replace(
        "os.execv('/bin/sh', ['/bin/sh', '-c', command])",
        &interception,
    );
    std::fs::write(&f.ssh, ssh).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "work",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--wait",
    ]));
    let mut signal = String::new();
    std::io::BufReader::new(std::fs::File::open(ready).unwrap())
        .read_line(&mut signal)
        .unwrap();
    assert_eq!(signal, "published\n");
    let cancel = f.worker(&["--json", "task", "cancel", &task.to_string()]);
    assert!(
        String::from_utf8_lossy(&cancel.stdout).contains("INTEGRATION_ALREADY_COMMITTED"),
        "{}",
        String::from_utf8_lossy(&cancel.stdout)
    );
    drop(release_on_drop);
    let committed = wait_integrated(&f, task);
    let receipt = committed.receipt.unwrap();
    assert!(receipt.imported);
    assert_eq!(receipt.disposition, IntegrationDisposition::Merged);
    let tasks = ClientStateStore::open(&owner_paths(&f).state).unwrap();
    assert_eq!(
        tasks.load_task(task).unwrap().status().last_outcome(),
        Some(&TaskOutcome::Done)
    );
    assert!(
        f.worker(&["--json", "task", "close", &task.to_string()])
            .status
            .success()
    );
    assert_eq!(
        tasks.load_task(task).unwrap().status().state(),
        TaskState::Closed
    );
    let origin = f.laptop.parent().unwrap().join("origin.git");
    assert_eq!(
        String::from_utf8(
            f.project
                .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
                .stdout
        )
        .unwrap()
        .trim(),
        receipt.merge_oid.unwrap().as_str()
    );
    assert_eq!(std::fs::read(source).unwrap(), original_source);
}

fn wait_integrated(f: &super::session_import_e2e::Fixture, task: TaskId) -> IntegrationRecord {
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        wait.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&wait.stdout),
        String::from_utf8_lossy(&wait.stderr)
    );
    let owner = RootedIntegrationState::open(
        &owner_paths(f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let record = owner.load(task).unwrap().unwrap();
    assert_eq!(
        record.snapshot.state,
        IntegrationStatus::Integrated,
        "{:?}",
        record.snapshot
    );
    record
}

#[test]
fn never_receipt_is_idempotent_and_the_next_say_uses_its_accepted_head() {
    use mac_worker::test_support::{client_state::ClientStateStore, session::SessionAgent};
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "printf '%s\\n' \"$@\" > \"$HOME/argv\"",
        "count=0; [ ! -f \"$HOME/turn-count\" ] || count=$(cat \"$HOME/turn-count\")\ncount=$((count + 1)); printf '%s' \"$count\" > \"$HOME/turn-count\"\nprintf 'ordinary %s\\n' \"$count\" > T6-result\nprintf '%s\\n' \"$@\" > \"$HOME/argv\"",
    );
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce work",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    let first = wait_integrated(&f, task);
    let accepted = first.receipt.as_ref().unwrap().merge_oid.as_ref().unwrap();
    let reconcile = f.worker(&["--json", "task", "reconcile"]);
    assert!(
        reconcile.status.success(),
        "{}",
        String::from_utf8_lossy(&reconcile.stdout)
    );
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    assert_eq!(
        owner.load(task).unwrap().unwrap().snapshot.integration_id,
        first.snapshot.integration_id
    );
    let say = f.worker(&[
        "--json",
        "task",
        "say",
        &task.to_string(),
        "--message",
        "produce later work",
        "--wait",
    ]);
    assert!(
        say.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&say.stdout),
        String::from_utf8_lossy(&say.stderr)
    );
    let next = wait_integrated(&f, task);
    assert_ne!(next.snapshot.integration_id, first.snapshot.integration_id);
    assert_eq!(&next.cycle_base, accepted);
    assert_eq!(next.archived_receipts, vec![first.receipt.unwrap()]);
    let local = ClientStateStore::open(&owner_paths(&f).state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(local.status().state(), TaskState::Open);
    assert_eq!(local.status().turns().len(), 2);
    assert_eq!(std::fs::read(source).unwrap(), original);
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(journal.matches("host task-prepare\n").count(), 1);
}

#[test]
fn conflict_auxiliary_resumes_the_imported_session_without_ordinary_publication() {
    use mac_worker::test_support::{client_state::ClientStateStore, session::SessionAgent};
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let target = f.laptop.parent().unwrap().join("outside");
    let origin = f.laptop.parent().unwrap().join("origin.git");
    f.project
        .git(&["clone", origin.to_str().unwrap(), target.to_str().unwrap()]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.email",
        "fixture@example.test",
    ]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.name",
        "Fixture",
    ]);
    let agent = f.host.join("bin/codex");
    let work = format!(
        r#"count=0; [ ! -f "$HOME/turn-count" ] || count=$(cat "$HOME/turn-count")
count=$((count + 1)); printf '%s' "$count" > "$HOME/turn-count"
if [ "$count" = 1 ]; then
  printf 'ordinary\n' > README
  (cd '{}' && printf 'outside\n' > README && /usr/bin/git add README && /usr/bin/git commit -m outside && /usr/bin/git push origin main) >/dev/null 2>&1 || exit 95
else
  /usr/bin/git rev-parse MERGE_HEAD > "$HOME/aux-target" || exit 96
  /usr/bin/git rev-parse HEAD > "$HOME/aux-head"
  printf 'resolved\n' > README
fi
printf '%s\n' "$@" > "$HOME/argv""#,
        target.display()
    );
    let script = std::fs::read_to_string(&agent).unwrap().replace("printf '%s\\n' \"$@\" > \"$HOME/argv\"", &work)
        .replace("done < \"$HOME/placed-files\"\n", "done < \"$HOME/placed-files\"\nwhile IFS= read -r file; do printf '%s\\n' '{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"native append\"}}' >> \"$file\"; done < \"$HOME/placed-files\"\n");
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce conflict",
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    let record = wait_integrated(&f, task);
    assert_eq!(record.snapshot.resolve_turns, 1);
    assert_eq!(
        record.snapshot.verification,
        IntegrationVerification::ResolveAgentReport
    );
    let auxiliary = &record.auxiliaries[0];
    assert!(auxiliary.accepted && auxiliary.completed);
    assert!(auxiliary.queue_position.is_some());
    let local = ClientStateStore::open(&owner_paths(&f).state)
        .unwrap()
        .load_task(task)
        .unwrap();
    assert_eq!(local.status().turns().len(), 2);
    assert_eq!(local.status().turns()[1].turn_id(), auxiliary.turn_id);
    let merge = record.receipt.unwrap().merge_oid.unwrap();
    assert_eq!(
        f.project.git(&["show", &format!("{merge}:README")]).stdout,
        b"resolved\n"
    );
    assert_eq!(std::fs::read(source).unwrap(), original);
    let placed = std::fs::read_to_string(f.host.join("placed-files")).unwrap();
    assert_eq!(
        std::fs::read_to_string(placed.lines().next().unwrap())
            .unwrap()
            .matches("native append")
            .count(),
        2
    );
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    assert_eq!(journal.matches("host task-prepare\n").count(), 1);
    assert_eq!(journal.matches("host task-turn\n").count(), 1);
    assert_eq!(journal.matches("host task-integration-turn\n").count(), 1);
}

struct ReleaseAuxiliary(std::fs::File);
impl Drop for ReleaseAuxiliary {
    fn drop(&mut self) {
        use std::io::Write;
        let _ = self.0.write_all(b"resume\n");
    }
}
struct RunningAuxiliaryFixture {
    f: super::session_import_e2e::Fixture,
    task: TaskId,
    ready: std::fs::File,
    _release: ReleaseAuxiliary,
    source: std::path::PathBuf,
    original: Vec<u8>,
}
fn running_auxiliary_fixture(timeout: &str) -> RunningAuxiliaryFixture {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let original = std::fs::read(&source).unwrap();
    f.install_agent(SessionAgent::Codex);
    let target = f.laptop.parent().unwrap().join("outside");
    let origin = f.laptop.parent().unwrap().join("origin.git");
    f.project
        .git(&["clone", origin.to_str().unwrap(), target.to_str().unwrap()]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.email",
        "fixture@example.test",
    ]);
    f.project.git(&[
        "-C",
        target.to_str().unwrap(),
        "config",
        "user.name",
        "Fixture",
    ]);
    let fifo = |name: &str| {
        use std::os::unix::ffi::OsStrExt;
        let path = f.host.join(name);
        let path_c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path_c.as_ptr(), 0o600) }, 0);
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    };
    let ready = fifo("aux-ready");
    let release = ReleaseAuxiliary(fifo("aux-release"));
    let agent = f.host.join("bin/codex");
    let work = format!(
        r#"count=0; [ ! -f "$HOME/turn-count" ] || count=$(cat "$HOME/turn-count")
count=$((count + 1)); printf '%s' "$count" > "$HOME/turn-count"
if [ "$count" = 1 ]; then
  printf 'ordinary\n' > README
  (cd '{}' && printf 'outside\n' > README && /usr/bin/git add README && /usr/bin/git commit -m outside && /usr/bin/git push origin main) >/dev/null 2>&1 || exit 95
else
  /usr/bin/git rev-parse MERGE_HEAD > "$HOME/aux-target" || exit 96
  /usr/bin/git rev-parse HEAD > "$HOME/aux-head"
  printf 'a' > "$HOME/aux-ready"
  IFS= read -r release < "$HOME/aux-release"
  printf 'resolved\n' > README
fi
printf '%s\n' "$@" > "$HOME/argv""#,
        target.display()
    );
    let script = std::fs::read_to_string(&agent).unwrap().replace("printf '%s\\n' \"$@\" > \"$HOME/argv\"", &work)
        .replace("done < \"$HOME/placed-files\"\n", "done < \"$HOME/placed-files\"\nwhile IFS= read -r file; do printf '%s\\n' '{\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"native append\"}}' >> \"$file\"; done < \"$HOME/placed-files\"\n");
    std::fs::write(agent, script).unwrap();
    let task = submitted_task(&f.worker(&[
        "--json",
        "task",
        "submit",
        "--from-session",
        "codex",
        "--prompt",
        "produce conflict",
        "--timeout",
        timeout,
        "--integrate",
        "main",
        "--close-on",
        "never",
        "--worker",
        "fixture",
        "--no-wait",
        "--wait",
    ]));
    RunningAuxiliaryFixture {
        f,
        task,
        ready,
        _release: release,
        source,
        original,
    }
}

#[test]
fn cancelling_a_live_auxiliary_retains_stop_until_the_host_and_runner_retire() {
    use mac_worker::test_support::client_state::ClientStateStore;
    use std::io::Read;
    let RunningAuxiliaryFixture {
        f,
        task,
        mut ready,
        _release,
        source,
        original,
    } = running_auxiliary_fixture("45m");
    ready.read_exact(&mut [0]).unwrap();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    let origin_before = f
        .project
        .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
        .stdout;
    let cancel = f.worker(&["--json", "task", "cancel", &task.to_string()]);
    assert!(
        !cancel.status.success(),
        "a live auxiliary was acknowledged as cancelled"
    );
    assert!(String::from_utf8_lossy(&cancel.stdout).contains("INTEGRATION_STOP_UNCONFIRMED"));
    let client = ClientStateStore::open(&owner_paths(&f).state).unwrap();
    let owner = RootedIntegrationState::open(
        &owner_paths(&f),
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let pending = owner.load(task).unwrap().unwrap();
    assert!(
        pending
            .tombstone
            .as_ref()
            .is_some_and(|stop| !stop.acknowledged)
    );
    if let Some(entry) = client.queue_entry_for_task_turn(task).unwrap() {
        assert!(
            entry.is_cancel_requested(),
            "stop did not reach the live auxiliary queue"
        );
    }
    let wait = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        !String::from_utf8_lossy(&wait.stdout).contains("WAIT_TIMEOUT"),
        "{}",
        String::from_utf8_lossy(&wait.stdout)
    );
    let settled = owner.load(task).unwrap().unwrap();
    assert_eq!(settled.snapshot.state, IntegrationStatus::Revoked);
    assert!(settled.tombstone.unwrap().acknowledged);
    assert!(client.queue_entry_for_task_turn(task).unwrap().is_none());
    assert!(client.load_task(task).unwrap().runner().is_none());
    assert_eq!(
        f.project
            .git(&["--git-dir", origin.to_str().unwrap(), "rev-parse", "main"])
            .stdout,
        origin_before
    );
    assert_eq!(std::fs::read(source).unwrap(), original);
    let close = f.worker(&["--json", "task", "close", &task.to_string(), "--discard"]);
    assert!(
        close.status.success(),
        "{}",
        String::from_utf8_lossy(&close.stdout)
    );
}

#[test]
fn a_running_auxiliary_times_out_while_the_native_owner_gate_is_drained() {
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        task::model::{TaskOutcome, TaskState},
    };
    use std::io::Read;
    let RunningAuxiliaryFixture {
        f,
        task,
        mut ready,
        _release,
        source,
        original,
    } = running_auxiliary_fixture("30s");
    ready.read_exact(&mut [0]).unwrap();
    let paths = owner_paths(&f);
    let state = RootedIntegrationState::open(
        &paths,
        std::sync::Arc::new(ManualIntegrationRuntime::default()),
    )
    .unwrap();
    let running = state.load(task).unwrap().unwrap();
    let turn = running.auxiliaries[0].turn_id;
    let prepared = state.load_prepared(task, turn).unwrap().unwrap();
    assert_eq!(prepared.approved_turn_limits.timeout_millis, 30_000);
    assert!(f.worker(&["controller", "drain"]).status.success());
    // The real supervisor's lease clock keeps running. Waiting observes its
    // terminal result while the owner remains barred from another Git phase.
    let waited = f.worker(&[
        "--json",
        "task",
        "wait",
        "--task-id",
        &task.to_string(),
        "--timeout",
        "60s",
    ]);
    assert!(
        String::from_utf8_lossy(&waited.stdout).contains("WAIT_TIMEOUT"),
        "the parked integration must still keep wait pending: {}",
        String::from_utf8_lossy(&waited.stdout)
    );
    let client = ClientStateStore::open(&paths.state).unwrap();
    let ordinary = client.load_task(task).unwrap();
    assert_eq!(ordinary.status().state(), TaskState::Open);
    assert_eq!(ordinary.status().turns().last().unwrap().turn_id(), turn);
    assert_eq!(
        ordinary.status().last_outcome(),
        Some(&TaskOutcome::TimedOut)
    );
    assert!(ordinary.runner().is_none());
    assert!(client.queue_entry_for_task_turn(task).unwrap().is_none());
    let parked = state.load(task).unwrap().unwrap();
    assert_eq!(parked.snapshot.state, IntegrationStatus::Parked);
    assert_eq!(
        parked.snapshot.pause_reason,
        Some(IntegrationPauseReason::ControllerDrained)
    );
    assert!(parked.auxiliaries[0].accepted && parked.auxiliaries[0].completed);
    assert_eq!(parked.followups_spent, 1);
    assert!(parked.receipt.is_none());
    assert_eq!(state.load_prepared(task, turn).unwrap().unwrap(), prepared);
    assert_eq!(std::fs::read(source).unwrap(), original);
}

#[test]
fn direct_import_is_complete_and_host_is_armed_before_the_first_launch() {
    use mac_worker::test_support::session::SessionAgent;
    let f = integration_fixture(true);
    let source = f.capture_fixture(SessionAgent::Codex);
    let before = std::fs::read(&source).unwrap();
    let origin = f.laptop.parent().unwrap().join("origin.git");
    let origin_refs = f
        .project
        .git(&[
            "--git-dir",
            origin.to_str().unwrap(),
            "for-each-ref",
            "--format=%(refname) %(objectname)",
        ])
        .stdout;
    f.install_agent(SessionAgent::Codex);
    let agent = f.host.join("bin/codex");
    let script = std::fs::read_to_string(&agent).unwrap().replace(
        "done < \"$HOME/placed-files\"\n",
        r#"done < "$HOME/placed-files"
while IFS= read -r file; do
  printf '%s\n' '{"type":"event_msg","payload":{"type":"agent_message","message":"T6 native tail"}}' >> "$file"
done < "$HOME/placed-files"
"#,
    );
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
        "done",
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
    assert_eq!(policy.requested_close, ClosePolicy::Done);
    let journal = std::fs::read_to_string(f.host.join("ssh-journal")).unwrap();
    let prepare = journal.find("host task-prepare").unwrap();
    let arm = journal.find("host task-integration\n").unwrap();
    let launch = journal.find("host task-turn").unwrap();
    assert!(prepare < arm && arm < launch, "{journal}");
    let store = mac_worker::test_support::host::store::HostStore::open(&f.host_root()).unwrap();
    let meta = mac_worker::test_support::task::store::TaskStore::new(
        &store,
        &mac_worker::test_support::host::process::SystemProcessRunner,
    )
    .load_meta(&policy.project_id, task)
    .unwrap();
    assert_eq!(meta.close_policy(), ClosePolicy::Never);
    let task_dir = store.task_dir(&policy.project_id, task).unwrap();
    let receipt: Value =
        serde_json::from_slice(&std::fs::read(task_dir.join("session-import.json")).unwrap())
            .unwrap();
    assert_eq!(receipt["stage"], "complete");
    let placed = std::fs::read_to_string(f.host.join("placed-files")).unwrap();
    let native = std::fs::read_to_string(placed.lines().next().unwrap()).unwrap();
    assert!(native.contains("T6 native tail"));
    assert_eq!(
        f.project
            .git(&[
                "--git-dir",
                origin.to_str().unwrap(),
                "for-each-ref",
                "--format=%(refname) %(objectname)"
            ])
            .stdout,
        origin_refs
    );
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

#[test]
fn direct_integrating_batch_publishes_policies_and_requirements_before_drained_launch() {
    use mac_worker::test_support::{
        client_state::ClientStateStore, controller::drain::set_drained, core::paths::PathLayout,
    };
    let f = integration_fixture(true);
    std::fs::write(
        f.project.root().join(".worker.toml"),
        "[task]\nintegrate = 'main'\n",
    )
    .unwrap();
    let batch = f.project.root().join("tasks.toml");
    std::fs::write(&batch, "[[tasks]]\nid = 'enabled'\nprompt = 'work'\n[[tasks]]\nid = 'ordinary'\nprompt = 'work'\nintegrate = false\n").unwrap();
    let paths = PathLayout {
        config: f.config.clone(),
        state: f.laptop.join(".local/state/mac-worker"),
        cache: f.laptop.join(".cache/mac-worker"),
        data: f.laptop.join(".local/share/mac-worker"),
    };
    set_drained(&paths.controller_state_root(), true).unwrap();
    let output = f.worker(&["--json", "task", "batch", batch.to_str().unwrap()]);
    assert!(
        output.status.success(),
        "out={} err={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let tasks = ClientStateStore::open(&paths.state).unwrap();
    let rows = tasks.list_tasks().unwrap();
    assert_eq!(rows.len(), 2);
    let mut configured = 0;
    for row in &rows {
        let policy = paths.state.join(format!(
            "integrations/tasks/{}/policy.json",
            row.meta().task_id()
        ));
        if policy.exists() {
            let policy: FrozenIntegrationPolicy =
                serde_json::from_slice(&std::fs::read(policy).unwrap()).unwrap();
            assert_eq!(policy.requested_close, ClosePolicy::Done);
            assert_eq!(row.meta().close_policy(), ClosePolicy::Never);
            let entry = tasks
                .queue_entry_for_task_turn(row.meta().task_id())
                .unwrap()
                .unwrap();
            assert!(
                entry
                    .requirements()
                    .contains(&"feature:task.integration".to_owned())
            );
            assert!(entry.requirements().contains(&"origin:file".to_owned()));
            configured += 1;
        } else {
            assert_eq!(row.meta().close_policy(), ClosePolicy::Done);
        }
    }
    assert_eq!(configured, 1);
    assert!(!f.host.join("argv").exists());
}
