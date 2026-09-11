//! Streamed source handshake bound to real TaskClient submit, plus result
//! prepare/fetch/import. Not the ENV process harness.

#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
};

use mac_worker::{
    client_state::ClientStateStore,
    config::Config,
    controller::{
        ActiveResumeConfig, ControllerFault, ControllerStore, ControllerTransfer, OwnedCheckoutMap,
        TaskSubmitHandler, VerifiedResultMeta, canonical_request_sha256,
        controller_transfer_git_path, decode_frame, encode_frame, import_controller_result,
        load_operation_envelope, parse_request, registry::ProjectRegistry,
    },
    git_transport::GitTransport,
    host_store::HostStore,
    job::{
        ClientId, CommandSpec, JobId, LeaseAcquireRequest, LeaseToken, RequestFingerprint,
        RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService},
    paths::PathLayout,
    prepared_submit::FrozenSubmitBody,
    process::SystemProcessRunner,
    protocol::{MemoryPressure, PROTOCOL_VERSION},
    task::{BaseOid, ClosePolicy, TaskId, TurnId},
    transfer_repo::TransferRepo,
};
use support::GitRepo;
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FAKE_SSH_DEST: &str = "fakecontroller";
const RUNNER: SystemProcessRunner = SystemProcessRunner;

struct Isolated {
    _temp: TempDir,
    home: PathBuf,
    environment: BTreeMap<OsString, OsString>,
    fake_ssh: PathBuf,
    paths: PathLayout,
}

impl Isolated {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join("home");
        let xdg_cache = root.join("xdg-cache");
        let xdg_state = root.join("xdg-state");
        let xdg_config = root.join("xdg-config");
        let xdg_data = root.join("xdg-data");
        let runtime = root.join("xdg-runtime");
        for dir in [
            &home,
            &xdg_cache,
            &xdg_state,
            &xdg_config,
            &xdg_data,
            &runtime,
        ] {
            fs::create_dir_all(dir).unwrap();
        }
        fs::create_dir_all(home.join(".local/bin")).unwrap();
        symlink(env!("CARGO_BIN_EXE_worker"), home.join(".local/bin/worker")).unwrap();
        let fake_ssh = temp.path().join("fake-ssh");
        fs::write(
            &fake_ssh,
            format!(
                "#!/bin/sh\n# fake SSH hop: destination is not a live network host.\nexport HOME={home:?}\nexport XDG_CACHE_HOME={xdg_cache:?}\nexport XDG_STATE_HOME={xdg_state:?}\nexport XDG_CONFIG_HOME={xdg_config:?}\nexport XDG_DATA_HOME={xdg_data:?}\nexport XDG_RUNTIME_DIR={runtime:?}\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    -o) shift 2 ;;\n    --) shift; break ;;\n    -*) shift ;;\n    *) break ;;\n  esac\ndone\n[ \"$#\" -gt 0 ] && shift\nif [ \"$#\" -eq 1 ]; then exec /bin/sh -c \"$1\"; fi\nexec \"$@\"\n"
            ),
        )
        .unwrap();
        let mut permissions = fs::metadata(&fake_ssh).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&fake_ssh, permissions).unwrap();
        let mut env = BTreeMap::new();
        env.insert(OsString::from("HOME"), home.as_os_str().to_os_string());
        env.insert(OsString::from("XDG_CACHE_HOME"), xdg_cache.into());
        env.insert(OsString::from("XDG_STATE_HOME"), xdg_state.into());
        env.insert(OsString::from("XDG_CONFIG_HOME"), xdg_config.into());
        env.insert(OsString::from("XDG_DATA_HOME"), xdg_data.into());
        let paths = PathLayout::discover(None, &env, &home).unwrap();
        Self {
            _temp: temp,
            home,
            environment: env,
            fake_ssh,
            paths,
        }
    }
}

fn oid_of(repo: &GitRepo) -> BaseOid {
    let stdout = repo.git(&["rev-parse", "HEAD"]).stdout;
    String::from_utf8(stdout).unwrap().trim().parse().unwrap()
}

fn frozen_body(oid: &BaseOid) -> FrozenSubmitBody {
    FrozenSubmitBody {
        task_id: TaskId::new(Uuid::from_u128(0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5566)),
        turn_id: TurnId::new(Uuid::from_u128(0x018f_0f4a_6b5c_7d8e_9f00_1122_3344_5577)),
        run_id: None,
        created_at_millis: 1_700_000_000_000,
        prompt: "stream this snapshot".into(),
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

fn submit_request(
    request_id: &str,
    body: &FrozenSubmitBody,
) -> mac_worker::controller::ControllerRequest {
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": "task.submit",
        "body": body,
    }))
    .unwrap();
    parse_request(&payload).unwrap()
}

fn fetch_oid_into(cache: &Path, source: &Path, oid: &BaseOid) {
    let spec = format!("{oid}:refs/mac-worker/scratch/{oid}");
    let output = std::process::Command::new("/usr/bin/git")
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

#[test]
fn streamed_source_receipt_binds_real_taskclient_submit_and_result_fetch() {
    let isolated = Isolated::new();
    let repo = GitRepo::init();
    repo.write("README", b"streamed-source\n");
    repo.commit_all("init");
    let oid = oid_of(&repo);
    let request_id = format!("{:x}", Uuid::from_u128(0x11).simple());
    let body = frozen_body(&oid);
    let value = serde_json::to_value(&body).unwrap();
    assert!(value.get("bundle_base64").is_none());
    assert!(value.get("token").is_none());
    let request = submit_request(&request_id, &body);
    let fingerprint = RequestFingerprint::new(request.payload_sha256().to_owned()).unwrap();
    assert_eq!(
        fingerprint.as_str(),
        canonical_request_sha256(PROTOCOL_VERSION, "task.submit", &value).unwrap()
    );

    let identity = {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .prepare_source_receive(
                &isolated.paths.cache,
                &RUNNER,
                &request_id,
                &fingerprint,
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap()
    };
    assert_ne!(identity.token(), fingerprint.as_str());
    GitTransport::new(&RUNNER)
        .push_controller_source(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            identity.token(),
            identity.request_id(),
            identity.fingerprint(),
            identity.project_id(),
            identity.worktree_id(),
            identity.expected_oid(),
            repo.root(),
        )
        .unwrap();
    let receipt = {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .finish_source_receive(&isolated.paths.cache, &RUNNER, &identity)
            .unwrap()
    };
    assert_eq!(receipt.oid(), &oid);

    let config = Config::parse("version = 1\n").unwrap();
    let client_state = ClientStateStore::open(&isolated.paths.state).unwrap();
    let handler = TaskSubmitHandler::new(&RUNNER, &config, &isolated.paths, &client_state);
    let store = ControllerStore::open(&isolated.paths.controller_state_root()).unwrap();
    let ack = store
        .handle_with(&request, &handler, ControllerFault::None)
        .unwrap();
    let expected_task = body.task_id.to_string();
    let expected_turn = body.turn_id.to_string();
    assert_eq!(ack.task_id(), Some(expected_task.as_str()));
    assert_eq!(ack.turn_id(), Some(expected_turn.as_str()));
    assert_eq!(ack.payload_sha256(), fingerprint.as_str());

    let record = client_state.load_task(body.task_id).unwrap();
    assert_eq!(record.meta().project_id(), PROJECT_ID);
    assert_eq!(record.meta().worktree_id(), WORKTREE_ID);
    assert_eq!(record.meta().base_oid(), &oid);
    let cache = TransferRepo::open_or_create_controller_cache(
        &isolated.paths.cache,
        PROJECT_ID,
        WORKTREE_ID,
    )
    .unwrap();
    let expected =
        controller_transfer_git_path(&isolated.paths.cache, PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(cache.path(), expected.as_path());
    assert!(
        cache
            .path()
            .to_str()
            .unwrap()
            .contains("/controller-transfer/")
    );
    let checkout = ProjectRegistry::open(&isolated.paths.controller_state_root())
        .unwrap()
        .resolve(PROJECT_ID, WORKTREE_ID)
        .unwrap();
    assert!(checkout.is_dir());
    let bind = ProjectRegistry::open(&isolated.paths.controller_state_root())
        .unwrap()
        .lookup_task_request(body.task_id)
        .unwrap()
        .expect("task bind");
    assert_eq!(bind.request_id, request_id);
    assert_eq!(bind.fingerprint, fingerprint.as_str());

    let results = GitRepo::init();
    results.write("result.txt", b"controller-result\n");
    results.commit_all("result");
    let result_oid = oid_of(&results);
    fetch_oid_into(cache.path(), &results.root().join(".git"), &result_oid);
    let meta = VerifiedResultMeta {
        task_id: body.task_id,
        turn_id: body.turn_id,
        imported_oid: result_oid.clone(),
        worker: "mini-1".into(),
    };
    let result_identity = {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .prepare_result_upload(
                &isolated.paths.cache,
                &RUNNER,
                &request_id,
                &fingerprint,
                PROJECT_ID,
                WORKTREE_ID,
                &meta,
            )
            .unwrap()
    };
    let laptop = GitRepo::init();
    let laptop_transfer =
        TransferRepo::open_or_create(&isolated.paths.cache, &laptop.root().join(".git")).unwrap();
    GitTransport::new(&RUNNER)
        .fetch_controller_result(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            result_identity.token(),
            result_identity.request_id(),
            result_identity.fingerprint(),
            result_identity.project_id(),
            result_identity.task_id(),
            result_identity.turn_id(),
            result_identity.imported_oid(),
            laptop_transfer.path(),
        )
        .unwrap();
    let imported = import_controller_result(
        &laptop_transfer,
        &RUNNER,
        &laptop.root().join(".git"),
        &request_id,
        &meta,
    )
    .unwrap();
    assert_eq!(imported.head(), &result_oid);
}

#[test]
fn submit_without_source_prepare_has_no_task_row() {
    let isolated = Isolated::new();
    let oid = "0123456789abcdef0123456789abcdef01234567"
        .parse::<BaseOid>()
        .unwrap();
    let body = frozen_body(&oid);
    let request_id = format!("{:x}", Uuid::from_u128(0x22).simple());
    let request = submit_request(&request_id, &body);
    let config = Config::parse("version = 1\n").unwrap();
    let client_state = ClientStateStore::open(&isolated.paths.state).unwrap();
    let handler = TaskSubmitHandler::new(&RUNNER, &config, &isolated.paths, &client_state);
    let store = ControllerStore::open(&isolated.paths.controller_state_root()).unwrap();
    let error = store
        .handle_with(&request, &handler, ControllerFault::None)
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("token") || format!("{error:?}").contains("BASE_UNAVAILABLE"),
        "{error:?}"
    );
    assert!(
        client_state
            .load_task_optional(body.task_id)
            .unwrap()
            .is_none()
    );
}

fn write_enabled_config(paths: &PathLayout) {
    fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
    fs::write(
        &paths.config,
        r#"
version = 1
[controller]
enabled = true
ssh = "user@always-on-host"
"#,
    )
    .unwrap();
}

fn run_cli_submit(isolated: &Isolated, project: &Path, prompt: &str) -> (i32, String, String) {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_worker"));
    for (key, value) in &isolated.environment {
        command.env(key, value);
    }
    command
        .env("MAC_WORKER_TEST_SSH", &isolated.fake_ssh)
        .env("HOME", &isolated.home)
        .args(["--json", "task", "submit", "--prompt", prompt, "--project"])
        .arg(project);
    let output = command.output().unwrap();
    (
        output.status.code().unwrap_or(1),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn cli_freeze_keeps_project_task_limits_after_settings_change() {
    const PROJECT_TIMEOUT_MILLIS: u64 = 90 * 60 * 1000;
    const PROJECT_MAX_FOLLOWUPS: u32 = 3;
    const DEFAULT_TIMEOUT_MILLIS: u64 = 45 * 60 * 1000;
    const DEFAULT_MAX_FOLLOWUPS: u32 = 10;

    let isolated = Isolated::new();
    write_enabled_config(&isolated.paths);
    let repo = GitRepo::init();
    repo.write("README", b"limits-project\n");
    repo.write(
        ".worker.toml",
        b"[task]\ntimeout = \"90m\"\nmax_followups = 3\n",
    );
    repo.commit_all("init with project task limits");

    let (exit, stdout, stderr) =
        run_cli_submit(&isolated, repo.root(), "freeze project task limits");
    assert_eq!(exit, 0, "stdout={stdout} stderr={stderr}");
    let ack: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    let task_id: TaskId = ack["task_id"].as_str().unwrap().parse().unwrap();
    assert_ne!(
        PROJECT_TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS,
        "fixture must differ from global timeout default"
    );
    assert_ne!(
        PROJECT_MAX_FOLLOWUPS, DEFAULT_MAX_FOLLOWUPS,
        "fixture must differ from global follow-up default"
    );

    let client_state = ClientStateStore::open(&isolated.paths.state).unwrap();
    let record = client_state.load_task(task_id).unwrap();
    assert_eq!(
        record.meta().limits().turn.timeout_millis,
        PROJECT_TIMEOUT_MILLIS
    );
    assert_eq!(record.meta().limits().max_followups, PROJECT_MAX_FOLLOWUPS);

    let bind = ProjectRegistry::open(&isolated.paths.controller_state_root())
        .unwrap()
        .lookup_task_request(task_id)
        .unwrap()
        .expect("task bind");
    let envelope =
        load_operation_envelope(&isolated.paths.controller_cache_root(), &bind.request_id)
            .unwrap()
            .expect("laptop freeze envelope");
    assert_eq!(
        envelope.body()["timeout_millis"].as_u64(),
        Some(PROJECT_TIMEOUT_MILLIS)
    );
    assert_eq!(
        envelope.body()["max_followups"].as_u64(),
        Some(u64::from(PROJECT_MAX_FOLLOWUPS))
    );

    repo.write(
        ".worker.toml",
        b"[task]\ntimeout = \"15m\"\nmax_followups = 1\n",
    );
    let reread = ClientStateStore::open(&isolated.paths.state)
        .unwrap()
        .load_task(task_id)
        .unwrap();
    assert_eq!(
        reread.meta().limits().turn.timeout_millis,
        PROJECT_TIMEOUT_MILLIS
    );
    assert_eq!(reread.meta().limits().max_followups, PROJECT_MAX_FOLLOWUPS);
    let envelope =
        load_operation_envelope(&isolated.paths.controller_cache_root(), &bind.request_id)
            .unwrap()
            .expect("laptop freeze envelope after settings change");
    assert_eq!(
        envelope.body()["timeout_millis"].as_u64(),
        Some(PROJECT_TIMEOUT_MILLIS)
    );
    assert_eq!(
        envelope.body()["max_followups"].as_u64(),
        Some(u64::from(PROJECT_MAX_FOLLOWUPS))
    );
}

#[test]
fn owned_checkout_map_fails_closed_when_unregistered() {
    let isolated = Isolated::new();
    let registry = ProjectRegistry::open(&isolated.paths.controller_state_root()).unwrap();
    let error =
        OwnedCheckoutMap::from_identities(&registry, [(PROJECT_ID, WORKTREE_ID)]).unwrap_err();
    assert!(
        format!("{error:?}").contains("TASK_CONFIG_INVALID"),
        "{error:?}"
    );
}

/// C4 at the real submit seam: a genuine `--no-wait` admission rejection from
/// the real `TaskClient` (no eligible worker for the frozen request) must
/// terminalise the durable row, reach the caller with the capacity category,
/// create no task, and stay terminal across a controller restart and an
/// exact-envelope replay.
///
/// Complementary to the counting-executor kernel tests: this one produces the
/// error from production code rather than injecting it.
#[test]
fn real_no_wait_admission_rejection_terminalises_the_durable_row() {
    let isolated = Isolated::new();
    let repo = GitRepo::init();
    repo.write("README", b"no-wait-rejection\n");
    repo.commit_all("init");
    let oid = oid_of(&repo);
    let request_id = format!("{:x}", Uuid::from_u128(0x5a).simple());
    let mut body = frozen_body(&oid);
    // The only difference from the accepted submit path: the caller asked not
    // to be queued.
    body.wait_for_capacity = false;
    let request = submit_request(&request_id, &body);
    let fingerprint = RequestFingerprint::new(request.payload_sha256().to_owned()).unwrap();

    let identity = {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .prepare_source_receive(
                &isolated.paths.cache,
                &RUNNER,
                &request_id,
                &fingerprint,
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap()
    };
    GitTransport::new(&RUNNER)
        .push_controller_source(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            identity.token(),
            identity.request_id(),
            identity.fingerprint(),
            identity.project_id(),
            identity.worktree_id(),
            identity.expected_oid(),
            repo.root(),
        )
        .unwrap();
    {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .finish_source_receive(&isolated.paths.cache, &RUNNER, &identity)
            .unwrap();
    }

    // No configured worker: admission has nothing eligible, so a no-wait
    // request is rejected by `capacity_busy()` in the real TaskClient.
    let config = Config::parse("version = 1\n").unwrap();
    let client_state = ClientStateStore::open(&isolated.paths.state).unwrap();
    let handler = TaskSubmitHandler::new(&RUNNER, &config, &isolated.paths, &client_state);
    let store = ControllerStore::open(&isolated.paths.controller_state_root()).unwrap();
    let error = store
        .handle_with(&request, &handler, ControllerFault::None)
        .expect_err("a no-wait request with no eligible worker must be rejected");
    assert_eq!(error.public_code(), "CAPACITY_BUSY");
    assert_eq!(
        error.public_message(),
        "no eligible worker currently has an available heavy slot"
    );
    assert_eq!(
        error.exit_code(),
        75,
        "capacity category must reach the caller"
    );

    // The rejection is durable and terminal, and no task exists.
    let controller_state = isolated.paths.controller_state_root();
    let row: serde_json::Value = serde_json::from_slice(
        &fs::read(controller_state.join(format!("req-{request_id}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(row["phase"], serde_json::json!("acked"));
    assert_eq!(
        row["result"]["controller_rejection"]["code"],
        serde_json::json!("CAPACITY_BUSY")
    );
    let active = controller_state.join("active");
    let pending: Vec<_> = fs::read_dir(&active)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| !name.starts_with('.'))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        pending.is_empty(),
        "pending receipt must be retired: {pending:?}"
    );
    assert!(
        client_state.load_task(body.task_id).is_err(),
        "a rejected submit must not create a task row"
    );

    // Controller restart plus an exact-envelope replay: same rejection, still
    // no task, nothing re-executed.
    drop(store);
    let store = ControllerStore::open(&isolated.paths.controller_state_root()).unwrap();
    store
        .resume_active_bounded(&handler, &ActiveResumeConfig::default())
        .unwrap();
    let replay = store
        .handle_with(&request, &handler, ControllerFault::None)
        .expect_err("replay must return the saved rejection");
    assert_eq!(replay.public_code(), "CAPACITY_BUSY");
    assert_eq!(replay.exit_code(), 75);
    assert!(
        client_state.load_task(body.task_id).is_err(),
        "replay must not create a task row"
    );
}

// ---------------------------------------------------------------------------
// Occupied-slot regression at the real transport/process seam.
//
// The empty-inventory test above proves the handler/journal seam but does NOT
// prove a full-then-freed slot: with no configured worker there is no
// occupancy to free. This test configures a real worker, occupies its only
// slot with a real lease in the isolated host store, proves through the actual
// `worker host probe` that the slot is full, drives the submit through a real
// `worker host controller-rpc` child, then frees a slot and proves the
// rejected request is never executed by a restart or an exact-envelope replay.
// ---------------------------------------------------------------------------

const OCCUPIED_WORKER: &str = "mini-1";

fn healthy_admission_facts() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

/// Writes a controller-host config: one real worker reachable through the
/// loopback fake SSH, with a client ceiling of two slots.
fn write_occupied_worker_config(paths: &PathLayout) {
    fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
    fs::write(
        &paths.config,
        format!(
            "version = 1\n\n[[workers]]\nname = \"{OCCUPIED_WORKER}\"\nssh = \"{FAKE_SSH_DEST}\"\nslots = 2\n"
        ),
    )
    .unwrap();
}

/// Takes a real lease on the isolated host so the worker genuinely has no free
/// slot while `slot_count` is one.
fn occupy_single_slot(paths: &PathLayout) {
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let material = RequestFingerprintMaterial::new(
        JobId::new(Uuid::from_u128(0x9a_0001)),
        ClientId::new(Uuid::from_u128(0x9a_0002)),
        LeaseToken::new(Uuid::from_u128(0x9a_0003)),
        now,
        OCCUPIED_WORKER.into(),
        "a".repeat(64),
        "b".repeat(64),
        "c".repeat(64),
        String::new(),
        60_000,
        "heavy".into(),
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    )
    .unwrap();
    let store = HostStore::open(&paths.host_state_root()).unwrap();
    let service = LeaseService::new(&store);
    service.set_slot_count(1).unwrap();
    service
        .acquire(
            &LeaseAcquireRequest::new(material),
            &healthy_admission_facts(),
            now,
        )
        .unwrap();
    let occupancy = service.occupancy().unwrap();
    assert_eq!(occupancy.configured_slots, 1);
    assert_eq!(occupancy.busy_slots, 1, "the only slot must be occupied");
}

/// Raises the durable host slot count so a free slot appears next to the lease
/// that is still held. The probe below proves the transition really happened.
fn free_one_slot(paths: &PathLayout) {
    let store = HostStore::open(&paths.host_state_root()).unwrap();
    let service = LeaseService::new(&store);
    service.set_slot_count(2).unwrap();
    let occupancy = service.occupancy().unwrap();
    assert_eq!(occupancy.configured_slots, 2);
    assert_eq!(occupancy.busy_slots, 1, "one slot stays held, one is free");
}

fn worker_child(isolated: &Isolated, args: &[&str]) -> std::process::Output {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_worker"));
    for (key, value) in &isolated.environment {
        command.env(key, value);
    }
    command
        .env("MAC_WORKER_TEST_SSH", &isolated.fake_ssh)
        .env("HOME", &isolated.home)
        .args(args);
    command.output().unwrap()
}

/// Runs the real `worker host controller-rpc` child over a framed request.
fn controller_rpc_child(isolated: &Isolated, frame: &[u8]) -> std::process::Output {
    use std::io::Write as _;

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_worker"));
    for (key, value) in &isolated.environment {
        child.env(key, value);
    }
    let mut spawned = child
        .env("MAC_WORKER_TEST_SSH", &isolated.fake_ssh)
        .env("HOME", &isolated.home)
        .args(["host", "controller-rpc"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    spawned.stdin.as_mut().unwrap().write_all(frame).unwrap();
    spawned.wait_with_output().unwrap()
}

/// Probe the configured worker through the real `workers` command and return
/// `(configured_slots, busy_slots, slot_state)`.
fn probe_slots(isolated: &Isolated) -> (u64, u64, String) {
    let output = worker_child(isolated, &["--json", "workers"]);
    assert!(
        output.status.success(),
        "workers probe failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let worker = value["workers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == OCCUPIED_WORKER)
        .expect("configured worker must appear in the inventory")
        .clone();
    assert_eq!(
        worker["status"], "ready",
        "the probe itself must succeed, otherwise the premise is 'unreachable', not 'full': {worker}"
    );
    let probe = &worker["probe"];
    (
        probe["configured_slots"].as_u64().unwrap(),
        probe["busy_slots"].as_u64().unwrap(),
        probe["slot_state"].as_str().unwrap().to_owned(),
    )
}

#[test]
fn occupied_slot_no_wait_rejection_survives_freed_capacity_and_replay() {
    let isolated = Isolated::new();
    write_occupied_worker_config(&isolated.paths);
    occupy_single_slot(&isolated.paths);

    // Premise, proven rather than assumed: the real probe reports a reachable
    // worker whose only slot is taken.
    let (configured, busy, state) = probe_slots(&isolated);
    assert_eq!(
        (configured, busy),
        (1, 1),
        "slot must be full before the submit (state={state})"
    );

    let repo = GitRepo::init();
    repo.write("README", b"occupied-slot\n");
    repo.commit_all("init");
    let oid = oid_of(&repo);
    let request_id = format!("{:x}", Uuid::from_u128(0x5b).simple());
    let mut body = frozen_body(&oid);
    body.wip = false;
    body.worker = Some(OCCUPIED_WORKER.into());
    body.wait_for_capacity = false;
    let request = submit_request(&request_id, &body);
    let fingerprint = RequestFingerprint::new(request.payload_sha256().to_owned()).unwrap();

    let identity = {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .prepare_source_receive(
                &isolated.paths.cache,
                &RUNNER,
                &request_id,
                &fingerprint,
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap()
    };
    GitTransport::new(&RUNNER)
        .push_controller_source(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            identity.token(),
            identity.request_id(),
            identity.fingerprint(),
            identity.project_id(),
            identity.worktree_id(),
            identity.expected_oid(),
            repo.root(),
        )
        .unwrap();
    {
        let transfer = ControllerTransfer::open(&isolated.paths.controller_state_root()).unwrap();
        transfer
            .finish_source_receive(&isolated.paths.cache, &RUNNER, &identity)
            .unwrap();
    }

    let frame = encode_frame(
        &serde_json::to_vec(&serde_json::json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "command": "task.submit",
            "body": body,
        }))
        .unwrap(),
    )
    .unwrap();

    // Real RPC child against a full slot: a no-wait pin must be rejected.
    let rejected = controller_rpc_child(&isolated, &frame);
    assert!(
        !rejected.status.success(),
        "a rejected submit must fail the child: {}",
        String::from_utf8_lossy(&rejected.stdout)
    );
    let payload = decode_frame(&rejected.stdout).unwrap();
    let error: serde_json::Value = serde_json::from_slice(payload).unwrap();
    assert_eq!(
        error["error"]["code"], "CAPACITY_BUSY",
        "unexpected rejection: {error}"
    );
    assert_eq!(
        rejected.status.code(),
        Some(75),
        "the controller child must exit with the capacity category"
    );

    let controller_state = isolated.paths.controller_state_root();
    let row: serde_json::Value = serde_json::from_slice(
        &fs::read(controller_state.join(format!("req-{request_id}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(row["phase"], serde_json::json!("acked"));
    assert_eq!(
        row["result"]["controller_rejection"]["code"],
        serde_json::json!("CAPACITY_BUSY")
    );
    let pending: Vec<_> = fs::read_dir(controller_state.join("active"))
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| !name.starts_with('.'))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        pending.is_empty(),
        "the pending receipt must be retired: {pending:?}"
    );
    let client_state = ClientStateStore::open(&isolated.paths.state).unwrap();
    assert!(client_state.load_task(body.task_id).is_err());

    // Capacity frees while the original lease is still held.
    free_one_slot(&isolated.paths);
    let (configured, busy, state) = probe_slots(&isolated);
    assert_eq!(
        (configured, busy),
        (2, 1),
        "a slot must now be free (state={state})"
    );

    // A fresh controller process replaying the exact envelope must return the
    // same rejection and must not execute the request now that a slot exists.
    let replayed = controller_rpc_child(&isolated, &frame);
    assert!(!replayed.status.success());
    let payload = decode_frame(&replayed.stdout).unwrap();
    let error: serde_json::Value = serde_json::from_slice(payload).unwrap();
    assert_eq!(error["error"]["code"], "CAPACITY_BUSY");
    assert_eq!(replayed.status.code(), Some(75));
    assert!(
        client_state.load_task(body.task_id).is_err(),
        "freed capacity must not resurrect a rejected request"
    );

    // A leader tick over the pending index must also find nothing to do.
    let config = Config::load(&isolated.paths.config).unwrap();
    let handler = TaskSubmitHandler::new(&RUNNER, &config, &isolated.paths, &client_state);
    let store = ControllerStore::open(&isolated.paths.controller_state_root()).unwrap();
    let tick = store
        .resume_active_bounded(&handler, &ActiveResumeConfig::default())
        .unwrap();
    assert!(tick.completed.is_empty());
    assert!(tick.failed.is_empty());
    assert!(client_state.load_task(body.task_id).is_err());
}
