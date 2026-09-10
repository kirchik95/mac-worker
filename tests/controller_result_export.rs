//! Controller result-export regressions: typed durable resolution (ordinary vs
//! batch) and quiescence-gated result prepare.
//!
//! All tests drive the real `host controller-rpc` stdio boundary with real
//! Git objects. Durable rows are persisted through the real `ControllerStore`
//! machinery; binds use the real registry API. No worker binary is spawned.

mod support;

use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::Cursor,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};

use mac_worker::{
    RuntimeContext,
    agent::{AgentKind, PermissionPolicy},
    cli::{Cli, Command as WorkerCommand, HostCommand},
    client_state::ClientStateStore,
    controller::{
        ControllerCommandHandler, ControllerFault, ControllerStore,
        OperationMeta, canonical_request_sha256,
        protocol::{ControllerRequest, decode_frame, encode_frame},
        registry::ProjectRegistry,
    },
    dag::{DagBase, DagFrozenSpec, DagNode, DagNodeState},
    controller::batch::{BatchKind, FrozenBatchBody},
    error::WorkerError,
    git_transport::GitTransport,
    job::RequestFingerprint,
    paths::PathLayout,
    prepared_submit::FrozenSubmitBody,
    process::SystemProcessRunner,
    protocol::PROTOCOL_VERSION,
    run_with_stdio_in_context,
    task::{
        BaseOid, ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunId, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId,
        TurnSummary, TurnTerminal,
    },
    transfer_repo::TransferRepo,
};
use serde_json::{Value, json};
use support::GitRepo;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FAKE_SSH_DEST: &str = "fakecontroller";
const RUNNER: SystemProcessRunner = SystemProcessRunner;

fn task_n(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn turn_n(n: u128) -> TurnId {
    TurnId::new(Uuid::from_u128(n))
}

fn request_hex(n: u128) -> String {
    format!("{:x}", Uuid::from_u128(n).simple())
}

struct Isolated {
    _temp: tempfile::TempDir,
    _home: PathBuf,
    _environment: BTreeMap<OsString, OsString>,
    runtime: RuntimeContext,
    fake_ssh: PathBuf,
    paths: PathLayout,
}

impl Isolated {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join("home");
        let state_home = root.join("state");
        let cache_home = root.join("cache");
        let config_home = root.join("config");
        let data_home = root.join("data");
        for dir in [&home, &state_home, &cache_home, &config_home, &data_home] {
            support::create_directory(dir);
        }
        std::fs::create_dir_all(home.join(".local/bin")).unwrap();
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_worker"), home.join(".local/bin/worker"))
            .unwrap();
        let fake_ssh = root.join("fake-ssh");
        std::fs::write(
            &fake_ssh,
            format!(
                "#!/bin/sh\n# fake SSH hop: destination is not a live network host.\nexport HOME={home:?}\nexport XDG_CACHE_HOME={cache_home:?}\nexport XDG_STATE_HOME={state_home:?}\nexport XDG_CONFIG_HOME={config_home:?}\nexport XDG_DATA_HOME={data_home:?}\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    -o) shift 2 ;;\n    --) shift; break ;;\n    -*) shift ;;\n    *) break ;;\n  esac\ndone\n[ \"$#\" -gt 0 ] && shift\nif [ \"$#\" -eq 1 ]; then exec /bin/sh -c \"$1\"; fi\nexec \"$@\"\n",
                home = home,
                cache_home = cache_home,
                state_home = state_home,
                config_home = config_home,
                data_home = data_home,
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&fake_ssh).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&fake_ssh, permissions).unwrap();
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.as_os_str().to_os_string()),
            (OsString::from("XDG_STATE_HOME"), state_home.into_os_string()),
            (OsString::from("XDG_CACHE_HOME"), cache_home.into_os_string()),
            (OsString::from("XDG_CONFIG_HOME"), config_home.into_os_string()),
            (OsString::from("XDG_DATA_HOME"), data_home.into_os_string()),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        let runtime =
            RuntimeContext::isolated(environment.clone(), home.clone(), root.clone());
        Self {
            _temp: temp,
            _home: home,
            _environment: environment,
            runtime,
            fake_ssh,
            paths,
        }
    }
}

/// Durable persistence through the real store without executing the command
/// (b92 does not route `task.batch`; ordinary submit execution is covered by
/// `controller_streamed_submit`). The persisted row — command, body, digest —
/// is exactly what real execution would publish first.
struct PassthroughDurable;

impl ControllerCommandHandler for PassthroughDurable {
    fn prepare(&self, _request: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
        Ok(OperationMeta {
            task_id: None,
            turn_id: None,
            created_at_millis: 1_700_000_000_000,
            prepared: Value::Null,
        })
    }

    fn execute(
        &self,
        _record: &mac_worker::controller::DurableRequest,
    ) -> Result<Value, WorkerError> {
        Ok(Value::Null)
    }
}

fn persist_durable(
    isolated: &Isolated,
    request_id: &str,
    command: &str,
    body: Value,
) -> (String, String) {
    let payload = serde_json::to_vec(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": command,
        "body": body,
    }))
    .unwrap();
    let request =
        mac_worker::controller::parse_request(&payload).expect("valid controller request");
    let fingerprint = canonical_request_sha256(PROTOCOL_VERSION, command, &body).unwrap();
    let store = ControllerStore::open(&isolated.paths.controller_state_root()).unwrap();
    store
        .handle_with(&request, &PassthroughDurable, ControllerFault::None)
        .expect("durable persist");
    (request_id.to_owned(), fingerprint)
}

fn bind_task(
    isolated: &Isolated,
    task_id: TaskId,
    request_id: &str,
    fingerprint: &str,
) {
    ProjectRegistry::open(&isolated.paths.controller_state_root())
        .unwrap()
        .bind_task_request(task_id, request_id, fingerprint)
        .unwrap();
}

fn submit_body(task: u128, turn: u128, oid: &BaseOid) -> FrozenSubmitBody {
    FrozenSubmitBody {
        task_id: task_n(task),
        turn_id: turn_n(turn),
        run_id: None,
        created_at_millis: 1_700_000_000_000,
        prompt: "export proof".into(),
        title: None,
        agent: "codex".into(),
        model: None,
        effort: None,
        source: "local".into(),
        origin_url: None,
        publish: vec!["fetch".into()],
        publish_branch: None,
        close_on: ClosePolicy::Never,
        env_profile: None,
        worker: None,
        wip: true,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        base_oid: oid.clone(),
        timeout_millis: 45 * 60 * 1000,
        max_turns: None,
        max_budget_usd_cents: None,
        max_followups: 10,
        permissions: "workspace".into(),
        requires: Vec::new(),
        include_untracked: Vec::new(),
        include_empty_dirs: Vec::new(),
        allow_sensitive: Vec::new(),
        cli_includes: Vec::new(),
        branch: None,
        wait_for_capacity: true,
    }
}

fn dag_spec() -> DagFrozenSpec {
    DagFrozenSpec {
        prompt: "batch node".into(),
        title: None,
        agent: "codex".into(),
        model: None,
        effort: None,
        source: "local".into(),
        origin_url: None,
        publish: vec!["fetch".into()],
        publish_branch: None,
        close_on: ClosePolicy::Never,
        env_profile: None,
        worker: None,
        wip: false,
        project_path: "/tmp/batch".into(),
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        timeout_millis: 45 * 60 * 1000,
        max_turns: None,
        max_budget_usd_cents: None,
        max_followups: 10,
        permissions: "workspace".into(),
        requires: Vec::new(),
        include_untracked: Vec::new(),
        include_empty_dirs: Vec::new(),
        allow_sensitive: Vec::new(),
        cli_includes: Vec::new(),
        branch: None,
    }
}

fn dag_node(batch_id: &str, task: u128, turn: u128, base: DagBase) -> DagNode {
    DagNode {
        batch_id: batch_id.into(),
        task_id: task_n(task),
        turn_id: turn_n(turn),
        depends_on: Vec::new(),
        base,
        frozen: dag_spec(),
        state: DagNodeState::Waiting,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: None,
        claimed_at_millis: None,
    }
}

fn batch_body(run: u128, parent: u128, child: u128, parent_oid: &BaseOid) -> FrozenBatchBody {
    let mut nodes = BTreeMap::new();
    let run_id = RunId::new(Uuid::from_u128(run));
    nodes.insert(
        "parent".into(),
        dag_node(
            "parent",
            parent,
            parent + 1000,
            DagBase::Frozen {
                oid: parent_oid.clone(),
                pin_ref: format!("refs/mac-worker/dag/{run_id}/parent"),
                wip: false,
            },
        ),
    );
    let mut child_node = dag_node(
        "child",
        child,
        child + 1000,
        DagBase::From {
            parent: "parent".into(),
        },
    );
    child_node.depends_on = vec!["parent".into()];
    nodes.insert("child".into(), child_node);
    FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: None,
        name: None,
        created_at_millis: 1_700_000_000_000,
        nodes,
        sources: Vec::new(),
    }
}

fn task_meta(task: u128, oid: &BaseOid) -> TaskMeta {
    task_meta_in(task, oid, PROJECT_ID, WORKTREE_ID)
}

fn task_meta_in(task: u128, oid: &BaseOid, project_id: &str, worktree_id: &str) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: task_n(task),
        run_id: None,
        project_id: project_id.into(),
        worktree_id: worktree_id.into(),
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
        base_oid: oid.clone(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: None,
        prompt: "export proof".into(),
        created_at_millis: 1_700_000_000_000,
    })
    .unwrap()
}

fn done_turn(number: u32, turn: u128, _oid: &BaseOid) -> TurnSummary {
    TurnSummary::new(
        number,
        turn_n(turn),
        Some(TurnTerminal::Succeeded),
        Some(TaskOutcome::Done),
        Some(true),
        false,
        Some(1),
        Some(2),
    )
}

fn pending_turn(number: u32, turn: u128) -> TurnSummary {
    TurnSummary::new(number, turn_n(turn), None, None, None, false, Some(3), None)
}

struct PlantRecord {
    task: u128,
    state: TaskState,
    last_outcome: Option<TaskOutcome>,
    head: Option<BaseOid>,
    fetched: Option<BaseOid>,
    turns: Vec<TurnSummary>,
}

fn plant_record(
    isolated: &Isolated,
    oid: &BaseOid,
    plant: PlantRecord,
) -> LocalTaskRecord {
    plant_record_in(isolated, oid, plant, PROJECT_ID, WORKTREE_ID)
}

fn plant_record_in(
    isolated: &Isolated,
    oid: &BaseOid,
    plant: PlantRecord,
    project_id: &str,
    worktree_id: &str,
) -> LocalTaskRecord {
    let status = TaskStatus::new(
        plant.state,
        plant.last_outcome,
        Some("mini-1".into()),
        false,
        plant.head,
        Some("seeded".into()),
        Vec::new(),
        Vec::new(),
        None,
        plant.turns,
        2,
    )
    .unwrap();
    let record = LocalTaskRecord::new(
        task_meta_in(plant.task, oid, project_id, worktree_id),
        status,
        None,
        None,
        plant.fetched,
        "c".repeat(64),
        None,
        true,
        None,
    )
    .unwrap();
    ClientStateStore::open(&isolated.paths.state)
        .unwrap()
        .create_task(record.clone())
        .unwrap();
    record
}

fn seed_result(isolated: &Isolated, label: &[u8]) -> BaseOid {
    let scratch = GitRepo::init();
    scratch.write("result.txt", label);
    scratch.commit_all("result");
    let stdout = scratch.git(&["rev-parse", "HEAD"]).stdout;
    let oid: BaseOid = String::from_utf8(stdout).unwrap().trim().parse().unwrap();
    let cache = TransferRepo::open_or_create_controller_cache(
        &isolated.paths.cache,
        PROJECT_ID,
        WORKTREE_ID,
    )
    .unwrap();
    let spec = format!("{oid}:refs/mac-worker/scratch/{oid}");
    let output = std::process::Command::new("/usr/bin/git")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(["-C"])
        .arg(cache.path())
        .args(["fetch", "--quiet", "--no-write-fetch-head"])
        .arg(scratch.root().join(".git"))
        .arg(&spec)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "seed fetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    oid
}

fn host_controller_rpc_cli() -> Cli {
    Cli {
        config: None,
        json: false,
        command: WorkerCommand::Host {
            command: HostCommand::ControllerRpc,
        },
    }
}

fn prepare_frame(task_id: TaskId, request_id: &str) -> Vec<u8> {
    encode_frame(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "command": "controller.transfer.result.prepare",
            "body": { "task_id": task_id.to_string() },
        }))
        .unwrap(),
    )
    .unwrap()
}

fn run_prepare(
    isolated: &Isolated,
    task_id: TaskId,
    request_id: &str,
) -> (u8, Vec<u8>, Vec<u8>) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let frame = prepare_frame(task_id, request_id);
    let exit = run_with_stdio_in_context(
        host_controller_rpc_cli(),
        &RUNNER,
        &isolated.runtime,
        &mut Cursor::new(frame),
        &mut stdout,
        &mut stderr,
    );
    (exit, stdout, stderr)
}

fn decode_ok(stdout: &[u8]) -> Value {
    serde_json::from_slice(decode_frame(stdout).expect("framed reply")).expect("reply JSON")
}

fn decode_error_code(stdout: &[u8]) -> String {
    let value: Value =
        serde_json::from_slice(decode_frame(stdout).expect("framed reply")).expect("reply JSON");
    value
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("<missing>")
        .to_owned()
}

fn base_oid() -> BaseOid {
    "dddddddddddddddddddddddddddddddddddddddd".parse().unwrap()
}

#[test]
fn ordinary_terminal_imported_prepares_exact_receipt() {
    let isolated = Isolated::new();
    let base = base_oid();
    let result = seed_result(&isolated, b"ordinary\n");
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x11),
        "task.submit",
        serde_json::to_value(submit_body(0xaaaa, 0xbbbb, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0xaaaa), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0xaaaa,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(result.clone()),
            fetched: Some(result.clone()),
            turns: vec![done_turn(1, 0xbbbb, &base)],
        },
    );
    let (exit, stdout, stderr) = run_prepare(&isolated, task_n(0xaaaa), &request_hex(0x99));
    assert_eq!(
        exit,
        0,
        "terminal imported ordinary task must prepare; stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let reply = decode_ok(&stdout);
    let prepared = reply.get("result").expect("result payload");
    assert_eq!(
        prepared.get("task_id").and_then(Value::as_str),
        Some(task_n(0xaaaa).to_string()).as_deref()
    );
    assert_eq!(
        prepared.get("turn_id").and_then(Value::as_str),
        Some(turn_n(0xbbbb).to_string()).as_deref()
    );
    assert_eq!(
        prepared.get("imported_oid").and_then(Value::as_str),
        Some(result.as_str())
    );
    assert!(!prepared
        .get("token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .is_empty());
}

#[test]
fn batch_node_terminal_imported_prepares_receipt() {
    let isolated = Isolated::new();
    let base = base_oid();
    let result = seed_result(&isolated, b"batch root\n");
    let body = batch_body(0xbeef, 0x1001, 0x1002, &base);
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x21),
        "task.batch",
        serde_json::to_value(&body).unwrap(),
    );
    bind_task(&isolated, task_n(0x1001), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x1001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(result.clone()),
            fetched: Some(result.clone()),
            turns: vec![done_turn(1, 0x1001 + 1000, &base)],
        },
    );
    let (exit, stdout, stderr) = run_prepare(&isolated, task_n(0x1001), &request_hex(0x99));
    assert_eq!(
        exit,
        0,
        "terminal imported batch node must prepare; stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let reply = decode_ok(&stdout);
    let prepared = reply.get("result").expect("result payload");
    assert_eq!(
        prepared.get("project_id").and_then(Value::as_str),
        Some(PROJECT_ID)
    );
    assert_eq!(
        prepared.get("worktree_id").and_then(Value::as_str),
        Some(WORKTREE_ID)
    );
    assert_eq!(
        prepared.get("imported_oid").and_then(Value::as_str),
        Some(result.as_str())
    );
}

#[test]
fn batch_from_parent_child_resolves() {
    let isolated = Isolated::new();
    let base = base_oid();
    let parent_result = seed_result(&isolated, b"parent\n");
    let child_result = seed_result(&isolated, b"child\n");
    let body = batch_body(0xbee0, 0x2001, 0x2002, &base);
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x22),
        "task.batch",
        serde_json::to_value(&body).unwrap(),
    );
    bind_task(&isolated, task_n(0x2002), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &parent_result,
        PlantRecord {
            task: 0x2002,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(child_result.clone()),
            fetched: Some(child_result.clone()),
            turns: vec![done_turn(1, 0x2002 + 1000, &parent_result)],
        },
    );
    let (exit, stdout, stderr) = run_prepare(&isolated, task_n(0x2002), &request_hex(0x99));
    assert_eq!(
        exit,
        0,
        "terminal imported from-parent child must prepare; stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let reply = decode_ok(&stdout);
    assert_eq!(
        reply
            .get("result")
            .and_then(|result| result.get("imported_oid"))
            .and_then(Value::as_str),
        Some(child_result.as_str())
    );
    assert_ne!(child_result, parent_result);
}

#[test]
fn batch_unbound_task_fails_closed() {
    let isolated = Isolated::new();
    let base = base_oid();
    let body = batch_body(0xbee1, 0x3001, 0x3002, &base);
    persist_durable(
        &isolated,
        &request_hex(0x23),
        "task.batch",
        serde_json::to_value(&body).unwrap(),
    );
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x3001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(base.clone()),
            fetched: Some(base.clone()),
            turns: vec![done_turn(1, 0x3001 + 1000, &base)],
        },
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0x3001), &request_hex(0x99));
    assert_ne!(exit, 0, "unbound batch task must not prepare");
    assert_eq!(decode_error_code(&stdout), "CONTROLLER_TRANSPORT");
}

#[test]
fn batch_missing_node_fails_closed() {
    let isolated = Isolated::new();
    let base = base_oid();
    let body = batch_body(0xbee2, 0x4001, 0x4002, &base);
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x24),
        "task.batch",
        serde_json::to_value(&body).unwrap(),
    );
    bind_task(&isolated, task_n(0x9999), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x9999,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(base.clone()),
            fetched: Some(base.clone()),
            turns: vec![done_turn(1, 0x7777, &base)],
        },
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0x9999), &request_hex(0x99));
    assert_ne!(exit, 0, "bound task absent from the batch must not prepare");
    assert_eq!(decode_error_code(&stdout), "CONTROLLER_TRANSPORT");
}

#[test]
fn active_first_turn_without_import_has_no_receipt() {
    let isolated = Isolated::new();
    let base = base_oid();
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x31),
        "task.submit",
        serde_json::to_value(submit_body(0x5001, 0x5002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0x5001), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x5001,
            state: TaskState::Active,
            last_outcome: None,
            head: Some(base.clone()),
            fetched: None,
            turns: vec![pending_turn(1, 0x5002)],
        },
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0x5001), &request_hex(0x99));
    assert_ne!(exit, 0, "active first turn must not mint a result receipt");
    assert_eq!(decode_error_code(&stdout), "RESULT_FETCH_FAILED");
}

#[test]
fn active_followup_with_stale_fetch_has_no_receipt() {
    let isolated = Isolated::new();
    let base = base_oid();
    let stale = seed_result(&isolated, b"stale turn one\n");
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x32),
        "task.submit",
        serde_json::to_value(submit_body(0x6001, 0x6002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0x6001), &request_id, &fingerprint);
    let mut turns = vec![done_turn(1, 0x6002, &base)];
    turns.push(pending_turn(2, 0x6003));
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x6001,
            state: TaskState::Active,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(stale.clone()),
            fetched: Some(stale.clone()),
            turns,
        },
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0x6001), &request_hex(0x99));
    assert_ne!(
        exit, 0,
        "active followup must not pair stale fetched head with the pending turn"
    );
    assert_eq!(decode_error_code(&stdout), "RESULT_FETCH_FAILED");
}

#[test]
fn completed_followup_imported_prepares_and_fetches_normally() {
    let isolated = Isolated::new();
    let base = base_oid();
    let first = seed_result(&isolated, b"followup one\n");
    let second = seed_result(&isolated, b"followup two\n");
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x33),
        "task.submit",
        serde_json::to_value(submit_body(0x7001, 0x7002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0x7001), &request_id, &fingerprint);
    let turns = vec![done_turn(1, 0x7002, &first), done_turn(2, 0x7003, &second)];
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x7001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(second.clone()),
            fetched: Some(second.clone()),
            turns,
        },
    );
    let (exit, stdout, stderr) = run_prepare(&isolated, task_n(0x7001), &request_hex(0x99));
    assert_eq!(
        exit,
        0,
        "completed imported followup must prepare; stderr={}",
        String::from_utf8_lossy(&stderr)
    );
    let reply = decode_ok(&stdout);
    let prepared = reply.get("result").expect("result payload");
    assert_eq!(
        prepared.get("turn_id").and_then(Value::as_str),
        Some(turn_n(0x7003).to_string()).as_deref()
    );
    assert_eq!(
        prepared.get("imported_oid").and_then(Value::as_str),
        Some(second.as_str())
    );
    let token = prepared
        .get("token")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    assert!(!token.is_empty());
    let laptop = GitRepo::init();
    let laptop_transfer =
        TransferRepo::open_or_create(&isolated.paths.cache, &laptop.root().join(".git")).unwrap();
    GitTransport::new(&RUNNER)
        .fetch_controller_result(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            &token,
            &request_hex(0x33),
            &RequestFingerprint::new(fingerprint.clone()).unwrap(),
            PROJECT_ID,
            task_n(0x7001),
            turn_n(0x7003),
            &second,
            laptop_transfer.path(),
        )
        .expect("real result fetch");
    let imported = mac_worker::controller::import_controller_result(
        &laptop_transfer,
        &RUNNER,
        &laptop.root().join(".git"),
        &request_hex(0x33),
        &mac_worker::controller::VerifiedResultMeta {
            task_id: task_n(0x7001),
            turn_id: turn_n(0x7003),
            imported_oid: second.clone(),
            worker: "mini-1".into(),
        },
    )
    .expect("real result import");
    assert_eq!(imported.head(), &second);
}

#[test]
fn abandoned_terminal_imported_is_rejected() {
    let isolated = Isolated::new();
    let base = base_oid();
    let result = seed_result(&isolated, b"abandoned\n");
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x41),
        "task.submit",
        serde_json::to_value(submit_body(0x8001, 0x8002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0x8001), &request_id, &fingerprint);
    let status = TaskStatus::new(
        TaskState::Abandoned,
        Some(TaskOutcome::Done),
        Some("mini-1".into()),
        false,
        Some(result.clone()),
        Some("seeded".into()),
        Vec::new(),
        Vec::new(),
        None,
        vec![done_turn(1, 0x8002, &base)],
        2,
    )
    .unwrap();
    let record = LocalTaskRecord::new(
        task_meta(0x8001, &base),
        status,
        None,
        None,
        Some(result.clone()),
        "c".repeat(64),
        None,
        true,
        Some("discarded".into()),
    )
    .unwrap();
    ClientStateStore::open(&isolated.paths.state)
        .unwrap()
        .create_task(record)
        .unwrap();
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0x8001), &request_hex(0x99));
    assert_ne!(exit, 0, "discarded task must not prepare a receipt");
    assert_eq!(decode_error_code(&stdout), "TASK_CLOSED");
}

#[test]
fn retry_same_completed_triple_reuses_receipt() {
    let isolated = Isolated::new();
    let base = base_oid();
    let result = seed_result(&isolated, b"retry\n");
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x51),
        "task.submit",
        serde_json::to_value(submit_body(0x9001, 0x9002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0x9001), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0x9001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(result.clone()),
            fetched: Some(result.clone()),
            turns: vec![done_turn(1, 0x9002, &base)],
        },
    );
    let (first_exit, first_out, _) = run_prepare(&isolated, task_n(0x9001), &request_hex(0x99));
    assert_eq!(first_exit, 0);
    let (second_exit, second_out, _) =
        run_prepare(&isolated, task_n(0x9001), &request_hex(0x98));
    assert_eq!(second_exit, 0, "exact retry must reuse the receipt");
    let first_token = decode_ok(&first_out)
        .get("result")
        .and_then(|result| result.get("token"))
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    let second_token = decode_ok(&second_out)
        .get("result")
        .and_then(|result| result.get("token"))
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    assert_eq!(first_token, second_token);
}

#[test]
fn mismatched_bound_task_fails_closed() {
    let isolated = Isolated::new();
    let base = base_oid();
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x61),
        "task.submit",
        serde_json::to_value(submit_body(0xa001, 0xa002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0xb001), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0xb001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(base.clone()),
            fetched: Some(base.clone()),
            turns: vec![done_turn(1, 0xb002, &base)],
        },
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0xb001), &request_hex(0x99));
    assert_ne!(exit, 0, "task bound to another frozen envelope must not prepare");
    assert_eq!(decode_error_code(&stdout), "CONTROLLER_REQUEST_CONFLICT");
}

#[test]
fn unsupported_command_fails_closed() {
    let isolated = Isolated::new();
    let base = base_oid();
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x71),
        "checkpoint.submit",
        json!({ "note": "not a task envelope" }),
    );
    bind_task(&isolated, task_n(0xc001), &request_id, &fingerprint);
    plant_record(
        &isolated,
        &base,
        PlantRecord {
            task: 0xc001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(base.clone()),
            fetched: Some(base.clone()),
            turns: vec![done_turn(1, 0xc002, &base)],
        },
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0xc001), &request_hex(0x99));
    assert_ne!(exit, 0, "non-task durable must not prepare");
    assert_eq!(decode_error_code(&stdout), "CONTROLLER_TRANSPORT");
}

#[test]
fn mismatched_logical_project_has_no_receipt() {
    let isolated = Isolated::new();
    let base = base_oid();
    let result = seed_result(&isolated, b"wrong project\n");
    let (request_id, fingerprint) = persist_durable(
        &isolated,
        &request_hex(0x81),
        "task.submit",
        serde_json::to_value(submit_body(0xd001, 0xd002, &base)).unwrap(),
    );
    bind_task(&isolated, task_n(0xd001), &request_id, &fingerprint);
    // Same task id, terminal turn, matching imported head — but the local
    // record lives under a different logical project/worktree than the
    // frozen original it is bound to.
    plant_record_in(
        &isolated,
        &base,
        PlantRecord {
            task: 0xd001,
            state: TaskState::Open,
            last_outcome: Some(TaskOutcome::Done),
            head: Some(result.clone()),
            fetched: Some(result.clone()),
            turns: vec![done_turn(1, 0xd002, &base)],
        },
        &"d".repeat(64),
        &"e".repeat(64),
    );
    let (exit, stdout, _) = run_prepare(&isolated, task_n(0xd001), &request_hex(0x99));
    assert_ne!(
        exit, 0,
        "record under a different logical project/worktree must not prepare"
    );
    assert_eq!(decode_error_code(&stdout), "CONTROLLER_REQUEST_CONFLICT");
}
