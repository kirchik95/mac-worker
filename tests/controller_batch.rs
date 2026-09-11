#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    client_state::{ClientStateStore, ClientStateWritePoint},
    config::Config,
    controller::{
        BatchExecuteContext, BatchKind, ControllerCheckoutMap, ControllerTransfer, FrozenBatchBody,
        FrozenBatchSource, canonical_request_sha256, controller_transfer_git_path,
        execute_task_batch, parse_request, prepare_task_batch,
    },
    dag::{DagBase, DagFrozenSpec, DagNode, DagNodeState, dag_pin_ref},
    error::WorkerError,
    job::{AdmissionObservation, ProcessIdentity, RequestFingerprint},
    process::SystemProcessRunner,
    project_state::ProjectState,
    protocol::PROTOCOL_VERSION,
    scheduler::CandidateSlot,
    task::{BaseOid, ClosePolicy, RunId, TaskId, TurnId},
    task_client::TaskClient,
    turn_runner::InlineRunnerExecutor,
};
use support::GitRepo;
use uuid::Uuid;

const RUNNER: SystemProcessRunner = SystemProcessRunner;
const LAPTOP_PATH: &str = "/tmp/mac-worker-batch-laptop-provenance";

struct Harness {
    _root: tempfile::TempDir,
    _repos: Vec<GitRepo>,
    paths: mac_worker::paths::PathLayout,
    store: ClientStateStore,
    transfer: ControllerTransfer,
    config: Config,
    executor: InlineRunnerExecutor,
    checkouts: ControllerCheckoutMap,
}

impl Harness {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
        fs::create_dir_all(paths.controller_state_root()).unwrap();
        fs::create_dir_all(&paths.cache).unwrap();
        chmod_owner_only(&paths.controller_state_root());
        chmod_owner_only(&paths.cache);
        let store = ClientStateStore::open(&paths.state).unwrap();
        plant_bound_mini1_ready(&store);
        let transfer = ControllerTransfer::open(&paths.controller_state_root()).unwrap();
        Self {
            _root: root,
            _repos: Vec::new(),
            paths,
            store,
            transfer,
            config: controller_config(2),
            executor: InlineRunnerExecutor,
            checkouts: ControllerCheckoutMap::new(),
        }
    }

    fn with_fault(point: ClientStateWritePoint) -> Self {
        let root = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
        fs::create_dir_all(paths.controller_state_root()).unwrap();
        fs::create_dir_all(&paths.cache).unwrap();
        chmod_owner_only(&paths.controller_state_root());
        chmod_owner_only(&paths.cache);
        let store = ClientStateStore::open_with_write_fault(&paths.state, point).unwrap();
        plant_bound_mini1_ready(&store);
        let transfer = ControllerTransfer::open(&paths.controller_state_root()).unwrap();
        Self {
            _root: root,
            _repos: Vec::new(),
            paths,
            store,
            transfer,
            config: controller_config(2),
            executor: InlineRunnerExecutor,
            checkouts: ControllerCheckoutMap::new(),
        }
    }

    fn client(&self) -> TaskClient<'_> {
        TaskClient::new(
            &RUNNER,
            &self.config,
            &self.paths,
            &self.store,
            &self.executor,
        )
    }

    fn ctx<'a>(&'a self, client: &'a TaskClient<'a>) -> BatchExecuteContext<'a> {
        BatchExecuteContext {
            client,
            store: &self.store,
            paths: &self.paths,
            runner: &RUNNER,
            checkouts: &self.checkouts,
        }
    }

    fn add_repo(&mut self) -> (String, String, PathBuf, BaseOid) {
        let repo = GitRepo::init();
        repo.write("src/lib.rs", b"fn main() {}\n");
        repo.commit_all("base");
        let project = ProjectState::load(&RUNNER, repo.root(), &[]).unwrap();
        let oid = head_oid(repo.root());
        let project_id = project.context.project_id.clone();
        let worktree_id = project.context.worktree_id.clone();
        self.checkouts.insert(
            project_id.clone(),
            worktree_id.clone(),
            project.context.root.clone(),
        );
        let root = repo.root().to_path_buf();
        self._repos.push(repo);
        (project_id, worktree_id, root, oid)
    }

    fn extra_commit(&self, name: &str, contents: &[u8], message: &str) -> BaseOid {
        let repo = self._repos.last().expect("repo");
        repo.write(name, contents);
        repo.commit_all(message);
        head_oid(repo.root())
    }
}

fn controller_config(slots: u8) -> Config {
    Config::parse(&format!(
        "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = {slots}\ncapabilities = [\"darwin-arm64\"]\n"
    ))
    .unwrap()
}

fn empty_laptop_config() -> Config {
    Config::parse("version = 1\nworkers = []\n").unwrap()
}

fn plant_bound_mini1_ready(state: &ClientStateStore) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    state
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec!["darwin-arm64".into(), "agent:codex".into()],
                Some(8 * 1024 * 1024 * 1024),
                64 * 1024 * 1024 * 1024,
                now,
            )
            .unwrap()
            .with_local_binding(
                "mac1".into(),
                "~/.local/bin/worker".into(),
                vec!["darwin-arm64".into()],
                1,
                Some(0),
                now,
            ),
        )
        .unwrap();
}

fn head_oid(root: &Path) -> BaseOid {
    let stdout = Command::new("/usr/bin/git")
        .args(["-C"])
        .arg(root)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap()
        .stdout;
    String::from_utf8(stdout).unwrap().trim().parse().unwrap()
}

fn fetch_oid_into(cache: &Path, source: &Path, oid: &BaseOid) {
    let spec = format!("{oid}:refs/mac-worker/scratch/{oid}");
    let output = Command::new("/usr/bin/git")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(["-C"])
        .arg(cache)
        .args(["fetch", "--quiet", "--no-write-fetch-head"])
        .arg(source)
        .arg(&spec)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "seed fetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn receive_source(
    harness: &Harness,
    nested_id: &str,
    fingerprint: &RequestFingerprint,
    project_id: &str,
    worktree_id: &str,
    oid: &BaseOid,
    source_root: &Path,
) {
    let identity = harness
        .transfer
        .prepare_source_receive(
            &harness.paths.cache,
            &RUNNER,
            nested_id,
            fingerprint,
            project_id,
            worktree_id,
            oid,
        )
        .unwrap();
    let cache =
        controller_transfer_git_path(&harness.paths.cache, project_id, worktree_id).unwrap();
    fetch_oid_into(&cache, source_root, oid);
    chmod_owner_only_tree(&cache);
    harness
        .transfer
        .finish_source_receive(&harness.paths.cache, &RUNNER, &identity)
        .unwrap();
}

fn chmod_owner_only(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).unwrap();
}

fn chmod_owner_only_tree(path: &Path) {
    chmod_owner_only(path);
    if !path.is_dir() {
        return;
    }
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let child = entry.path();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            chmod_owner_only_tree(&child);
        } else {
            let mut permissions = fs::metadata(&child).unwrap().permissions();
            permissions.set_mode(0o600);
            fs::set_permissions(&child, permissions).unwrap();
        }
    }
}

fn simple_id() -> String {
    format!("{:x}", Uuid::new_v4().simple())
}

fn frozen_node(
    _run_id: RunId,
    batch_id: &str,
    project_id: &str,
    worktree_id: &str,
    _oid: BaseOid,
    depends_on: Vec<String>,
    base: DagBase,
) -> DagNode {
    DagNode {
        batch_id: batch_id.to_owned(),
        task_id: TaskId::generate(),
        turn_id: TurnId::generate(),
        depends_on,
        base,
        frozen: DagFrozenSpec {
            prompt: format!("do {batch_id}"),
            title: None,
            agent: "codex".into(),
            model: None,
            effort: None,
            source: "local".into(),
            origin_url: None,
            publish: vec!["fetch".into()],
            publish_branch: None,
            close_on: ClosePolicy::Done,
            env_profile: None,
            worker: None,
            wip: false,
            project_path: LAPTOP_PATH.into(),
            project_id: project_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            timeout_millis: 45 * 60 * 1000,
            max_turns: None,
            max_budget_usd_cents: None,
            max_followups: 10,
            permissions: "workspace".into(),
            requires: vec!["agent:codex".into()],
            include_untracked: Vec::new(),
            include_empty_dirs: Vec::new(),
            allow_sensitive: Vec::new(),
            cli_includes: Vec::new(),
            branch: Some("main".into()),
        },
        state: DagNodeState::Waiting,
        bound_oid: None,
        bound_turn_id: None,
        pin_ref: None,
        blocked_by: None,
        claimed_by: None,
        claimed_at_millis: None,
    }
}

fn frozen_base(run_id: RunId, batch_id: &str, oid: BaseOid) -> DagBase {
    DagBase::Frozen {
        oid,
        pin_ref: dag_pin_ref(run_id, batch_id),
        wip: false,
    }
}

fn request_from_body(body: &FrozenBatchBody) -> mac_worker::controller::ControllerRequest {
    let value = serde_json::to_value(body).unwrap();
    let payload = serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": simple_id(),
        "command": "task.batch",
        "body": value,
    });
    parse_request(serde_json::to_vec(&payload).unwrap().as_slice()).unwrap()
}

fn fingerprint_for(body: &FrozenBatchBody) -> RequestFingerprint {
    let value = serde_json::to_value(body).unwrap();
    RequestFingerprint::new(
        canonical_request_sha256(PROTOCOL_VERSION, "task.batch", &value).unwrap(),
    )
    .unwrap()
}

fn prepare_ok(
    harness: &Harness,
    body: &FrozenBatchBody,
) -> mac_worker::controller::PreparedTaskBatch {
    let request = request_from_body(body);
    prepare_task_batch(
        &request,
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &harness.config,
    )
    .unwrap()
}

fn code(error: &WorkerError) -> String {
    error.public_code()
}

fn assert_no_run(store: &ClientStateStore, run_id: RunId) {
    assert!(store.load_run_dag(run_id).unwrap().is_none());
    match store.load_run(run_id) {
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        other => panic!("run should be missing, got {other:?}"),
    }
}

#[test]
fn prepared_roundtrip_preserves_resolved_max_parallel_and_source_digest() {
    let mut harness = Harness::new();
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    let run_id = RunId::generate();
    let node = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let nested = simple_id();
    let body = FrozenBatchBody {
        kind: BatchKind::Independent,
        run_id,
        max_parallel: None,
        name: Some("sprint".into()),
        created_at_millis: 9,
        nodes: BTreeMap::from([("root".into(), node)]),
        sources: vec![FrozenBatchSource {
            request_id: nested,
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid,
        }],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &body.sources[0].expected_oid,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    assert_eq!(prepared.command(), "task.batch");
    assert!(prepared.task_id().is_none());
    assert!(prepared.turn_id().is_none());
    assert_eq!(prepared.max_parallel(), 2);
    assert_eq!(prepared.requested_max_parallel(), None);
    assert_eq!(prepared.sources()[0].fingerprint(), fp.as_str());
    let encoded = serde_json::to_value(&prepared).unwrap();
    let decoded: mac_worker::controller::PreparedTaskBatch =
        serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, prepared);
    assert_eq!(empty_laptop_config().configured_runner_slots(), 0);
}

#[test]
fn omitted_max_parallel_does_not_use_empty_laptop_pool() {
    let mut harness = Harness::new();
    harness.config = controller_config(3);
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    let run_id = RunId::generate();
    let node = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let body = FrozenBatchBody {
        kind: BatchKind::Independent,
        run_id,
        max_parallel: None,
        name: None,
        created_at_millis: 1,
        nodes: BTreeMap::from([("root".into(), node)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid,
        }],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &body.sources[0].expected_oid,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    assert_eq!(prepared.max_parallel(), 3);
    let err = prepare_task_batch(
        &request_from_body(&body),
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &empty_laptop_config(),
    )
    .unwrap_err();
    assert_eq!(code(&err), "TASK_CONFIG_INVALID");
}

#[test]
fn malformed_graph_and_source_errors_do_not_write_run_or_queue() {
    let mut harness = Harness::new();
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    let run_id = RunId::generate();
    let mut extra = serde_json::to_value(FrozenBatchBody {
        kind: BatchKind::Independent,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 1,
        nodes: BTreeMap::from([(
            "root".into(),
            frozen_node(
                run_id,
                "root",
                &project_id,
                &worktree_id,
                oid.clone(),
                Vec::new(),
                frozen_base(run_id, "root", oid.clone()),
            ),
        )]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid.clone(),
        }],
    })
    .unwrap();
    extra["unexpected"] = serde_json::json!(true);
    let payload = serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": simple_id(),
        "command": "task.batch",
        "body": extra,
    });
    let request = parse_request(serde_json::to_vec(&payload).unwrap().as_slice()).unwrap();
    let err = prepare_task_batch(
        &request,
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &harness.config,
    )
    .unwrap_err();
    assert_eq!(code(&err), "INVALID_REQUEST");
    assert_no_run(&harness.store, run_id);

    let a = frozen_node(
        run_id,
        "a",
        &project_id,
        &worktree_id,
        oid.clone(),
        vec!["b".into()],
        frozen_base(run_id, "a", oid.clone()),
    );
    let b = frozen_node(
        run_id,
        "b",
        &project_id,
        &worktree_id,
        oid.clone(),
        vec!["a".into()],
        frozen_base(run_id, "b", oid.clone()),
    );
    let cyclic = FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 1,
        nodes: BTreeMap::from([("a".into(), a), ("b".into(), b)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid.clone(),
        }],
    };
    let err = prepare_task_batch(
        &request_from_body(&cyclic),
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &harness.config,
    )
    .unwrap_err();
    assert_eq!(code(&err), "TASK_CONFIG_INVALID");
    assert_no_run(&harness.store, run_id);

    let child = frozen_node(
        run_id,
        "child",
        &project_id,
        &worktree_id,
        oid.clone(),
        vec!["missing".into()],
        DagBase::From {
            parent: "missing".into(),
        },
    );
    let missing_parent = FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 1,
        nodes: BTreeMap::from([("child".into(), child)]),
        sources: Vec::new(),
    };
    let err = prepare_task_batch(
        &request_from_body(&missing_parent),
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &harness.config,
    )
    .unwrap_err();
    assert_eq!(code(&err), "TASK_CONFIG_INVALID");
    assert_no_run(&harness.store, run_id);

    let node = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let unfinished = FrozenBatchBody {
        kind: BatchKind::Independent,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 1,
        nodes: BTreeMap::from([("root".into(), node)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid.clone(),
        }],
    };
    harness
        .transfer
        .prepare_source_receive(
            &harness.paths.cache,
            &RUNNER,
            &unfinished.sources[0].request_id,
            &fingerprint_for(&unfinished),
            &project_id,
            &worktree_id,
            &oid,
        )
        .unwrap();
    let err = prepare_task_batch(
        &request_from_body(&unfinished),
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &harness.config,
    )
    .unwrap_err();
    assert_ne!(code(&err), "OK");
    assert_no_run(&harness.store, run_id);
    let _ = root;
}

#[test]
fn source_oid_mismatch_and_from_node_without_source_row() {
    let mut harness = Harness::new();
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    let other = harness.extra_commit("other.txt", b"other\n", "other");
    let run_id = RunId::generate();
    let root_node = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let child = frozen_node(
        run_id,
        "child",
        &project_id,
        &worktree_id,
        oid.clone(),
        vec!["root".into()],
        DagBase::From {
            parent: "root".into(),
        },
    );
    let body = FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 4,
        nodes: BTreeMap::from([("root".into(), root_node), ("child".into(), child)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid.clone(),
        }],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &oid,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    assert_eq!(prepared.sources().len(), 1);

    let mut mismatched = body.clone();
    mismatched.nodes.get_mut("root").unwrap().base = frozen_base(run_id, "root", other.clone());
    mismatched.sources[0].expected_oid = other;
    let err = prepare_task_batch(
        &request_from_body(&mismatched),
        &harness.transfer,
        &harness.paths.cache,
        &RUNNER,
        &harness.config,
    )
    .unwrap_err();
    assert_eq!(code(&err), "CONTROLLER_REQUEST_CONFLICT");
}

#[test]
fn multi_root_receipts_and_independent_does_not_write_dag() {
    let mut harness = Harness::new();
    let (project_id, worktree_id, root, oid_a) = harness.add_repo();
    let oid_b = harness.extra_commit("b.txt", b"b\n", "second");
    let run_id = RunId::generate();
    let a = frozen_node(
        run_id,
        "a",
        &project_id,
        &worktree_id,
        oid_a.clone(),
        Vec::new(),
        frozen_base(run_id, "a", oid_a.clone()),
    );
    let b = frozen_node(
        run_id,
        "b",
        &project_id,
        &worktree_id,
        oid_b.clone(),
        Vec::new(),
        frozen_base(run_id, "b", oid_b.clone()),
    );
    let body = FrozenBatchBody {
        kind: BatchKind::Independent,
        run_id,
        max_parallel: Some(2),
        name: None,
        created_at_millis: 11,
        nodes: BTreeMap::from([("a".into(), a), ("b".into(), b)]),
        sources: vec![
            FrozenBatchSource {
                request_id: simple_id(),
                project_id: project_id.clone(),
                worktree_id: worktree_id.clone(),
                expected_oid: oid_a.clone(),
            },
            FrozenBatchSource {
                request_id: simple_id(),
                project_id: project_id.clone(),
                worktree_id: worktree_id.clone(),
                expected_oid: oid_b.clone(),
            },
        ],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &oid_a,
        &root,
    );
    receive_source(
        &harness,
        &body.sources[1].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &oid_b,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    assert_eq!(prepared.sources().len(), 2);
    assert_eq!(
        prepared.sources()[0].cache_id(),
        prepared.sources()[1].cache_id()
    );
    let client = harness.client();
    let report = execute_task_batch(&harness.ctx(&client), &prepared).unwrap();
    assert_eq!(report.run_id(), run_id);
    assert_eq!(report.task_ids().len(), 2);
    assert!(harness.store.load_run_dag(run_id).unwrap().is_none());
    let replay = execute_task_batch(&harness.ctx(&client), &prepared).unwrap();
    assert_eq!(replay.run_id(), run_id);
    assert_eq!(replay.task_ids(), report.task_ids());
}

#[test]
fn mapping_miss_fails_before_run_effects() {
    let mut harness = Harness::new();
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    harness.checkouts = ControllerCheckoutMap::new();
    let run_id = RunId::generate();
    let node = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let body = FrozenBatchBody {
        kind: BatchKind::Independent,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 1,
        nodes: BTreeMap::from([("root".into(), node)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid,
        }],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &body.sources[0].expected_oid,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    let client = harness.client();
    let err = execute_task_batch(&harness.ctx(&client), &prepared).unwrap_err();
    assert_eq!(code(&err), "TASK_CONFIG_INVALID");
    assert_no_run(&harness.store, run_id);
}

#[test]
fn dag_replay_after_claim_submit_and_bind_preserves_progress() {
    let mut harness = Harness::new();
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    let run_id = RunId::generate();
    let parent = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let child = frozen_node(
        run_id,
        "child",
        &project_id,
        &worktree_id,
        oid.clone(),
        vec!["root".into()],
        DagBase::From {
            parent: "root".into(),
        },
    );
    let body = FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 8,
        nodes: BTreeMap::from([("root".into(), parent), ("child".into(), child)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid.clone(),
        }],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &oid,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    let client = harness.client();
    let first = execute_task_batch(&harness.ctx(&client), &prepared).unwrap();
    let dag = harness.store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(
        dag.nodes.get("root").unwrap().state,
        DagNodeState::Submitted
    );
    assert!(!first.task_ids().is_empty());

    let caller = ProcessIdentity::new(std::process::id(), 1).unwrap();
    let _ = harness
        .store
        .claim_next_eligible_dag_node(run_id, caller, 100)
        .unwrap();
    let child_task = dag.nodes.get("child").unwrap().task_id;
    let bind_oid = oid.clone();
    harness
        .store
        .publish_dag_binding(
            run_id,
            "child",
            bind_oid.clone(),
            dag.nodes.get("root").unwrap().turn_id,
            dag_pin_ref(run_id, "child"),
        )
        .unwrap();
    let progressed = harness.store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(
        progressed.nodes.get("child").unwrap().bound_oid.as_ref(),
        Some(&bind_oid)
    );
    let _ = child_task;

    let replay = execute_task_batch(&harness.ctx(&client), &prepared).unwrap();
    assert_eq!(replay.run_id(), run_id);
    let after = harness.store.load_run_dag(run_id).unwrap().unwrap();
    assert_eq!(
        after.nodes.get("root").unwrap().state,
        DagNodeState::Submitted
    );
    assert_eq!(
        after.nodes.get("child").unwrap().bound_oid.as_ref(),
        Some(&bind_oid)
    );
    assert_eq!(
        after.nodes.get("root").unwrap().frozen.project_path,
        harness
            .checkouts
            .get(&project_id, &worktree_id)
            .unwrap()
            .to_string_lossy()
    );
    assert!(
        !Path::new(LAPTOP_PATH).exists()
            || after.nodes.get("root").unwrap().frozen.project_path != LAPTOP_PATH
    );
}

#[test]
fn dag_before_run_heal_and_different_graph_conflicts() {
    let mut harness = Harness::with_fault(ClientStateWritePoint::AfterDagPublishBeforeRun);
    let (project_id, worktree_id, root, oid) = harness.add_repo();
    let run_id = RunId::generate();
    let node = frozen_node(
        run_id,
        "root",
        &project_id,
        &worktree_id,
        oid.clone(),
        Vec::new(),
        frozen_base(run_id, "root", oid.clone()),
    );
    let mut child = frozen_node(
        run_id,
        "child",
        &project_id,
        &worktree_id,
        oid.clone(),
        vec!["root".into()],
        DagBase::From {
            parent: "root".into(),
        },
    );
    child.depends_on = vec!["root".into()];
    let body = FrozenBatchBody {
        kind: BatchKind::Dag,
        run_id,
        max_parallel: Some(1),
        name: None,
        created_at_millis: 5,
        nodes: BTreeMap::from([("root".into(), node), ("child".into(), child)]),
        sources: vec![FrozenBatchSource {
            request_id: simple_id(),
            project_id: project_id.clone(),
            worktree_id: worktree_id.clone(),
            expected_oid: oid.clone(),
        }],
    };
    let fp = fingerprint_for(&body);
    receive_source(
        &harness,
        &body.sources[0].request_id,
        &fp,
        &project_id,
        &worktree_id,
        &oid,
        &root,
    );
    let prepared = prepare_ok(&harness, &body);
    let client = harness.client();
    let err = execute_task_batch(&harness.ctx(&client), &prepared).unwrap_err();
    assert_ne!(code(&err), "OK");
    assert!(harness.store.load_run_dag(run_id).unwrap().is_some());
    match harness.store.load_run(run_id) {
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => panic!("run should be missing after DAG-before-run fault"),
        Err(error) => panic!("unexpected load_run: {error}"),
    }
    let healed = execute_task_batch(&harness.ctx(&client), &prepared).unwrap();
    assert_eq!(healed.run_id(), run_id);
    assert!(harness.store.load_run(run_id).is_ok());

    let mut conflict_body = body.clone();
    conflict_body.nodes.get_mut("root").unwrap().frozen.prompt = "other prompt".into();
    conflict_body.sources[0].request_id = simple_id();
    receive_source(
        &harness,
        &conflict_body.sources[0].request_id,
        &fingerprint_for(&conflict_body),
        &project_id,
        &worktree_id,
        &oid,
        &root,
    );
    let conflicting = prepare_ok(&harness, &conflict_body);
    let err = execute_task_batch(&harness.ctx(&client), &conflicting).unwrap_err();
    assert_eq!(code(&err), "RUN_ID_CONFLICT");
}
