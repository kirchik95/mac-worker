#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    convert::Infallible,
    ffi::OsString,
    fs,
    io::Write,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Mutex,
    thread,
};

use clap::Parser;
use mac_worker::{
    cli::{Cli, Command as WorkerCommand, HostCommand},
    controller::{
        CONTROLLER_TRANSFER_CACHE_DOMAIN, ControllerResultIdentity, ControllerTransfer,
        VerifiedResultMeta, controller_transfer_cache_id, controller_transfer_git_path,
        frozen_result_ref, import_controller_result, result_digest, source_digest,
    },
    error::WorkerError,
    git_transport::{GitServerExecutor, GitTransport, PushReceipt},
    job::RequestFingerprint,
    paths::PathLayout,
    process::SystemProcessRunner,
    protocol::PROTOCOL_VERSION,
    task::{BaseOid, TaskId, TurnId},
    transfer::HostOperation,
    transfer_repo::{TransferRepo, repo_id_for},
};
use support::GitRepo;
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const OTHER_WORKTREE: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const FAKE_SSH_DEST: &str = "fakecontroller";
const LARGE_BYTES: usize = 1_048_576 + 8192;
const MIN_PACK_BYTES: usize = 1_048_576 + 1;
const RUNNER: SystemProcessRunner = SystemProcessRunner;

struct Isolated {
    _temp: TempDir,
    fake_ssh: PathBuf,
    paths: PathLayout,
}

impl Isolated {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let xdg_cache = temp.path().join("xdg-cache");
        let xdg_state = temp.path().join("xdg-state");
        let xdg_config = temp.path().join("xdg-config");
        let xdg_data = temp.path().join("xdg-data");
        let runtime = temp.path().join("xdg-runtime");
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
                "#!/bin/sh\n# fake SSH hop: destination is not a live network host.\nexport HOME={home:?}\nexport XDG_CACHE_HOME={xdg_cache:?}\nexport XDG_STATE_HOME={xdg_state:?}\nexport XDG_CONFIG_HOME={xdg_config:?}\nexport XDG_DATA_HOME={xdg_data:?}\nexport XDG_RUNTIME_DIR={runtime:?}\nshift\nif [ \"$#\" -eq 1 ]; then exec /bin/sh -c \"$1\"; fi\nexec \"$@\"\n"
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
            fake_ssh,
            paths,
        }
    }

    fn transfer(&self) -> ControllerTransfer {
        ControllerTransfer::open(&self.paths.controller_state_root()).unwrap()
    }

    fn transport(&self) -> GitTransport<'static> {
        GitTransport::new(&RUNNER)
    }

    fn cache(&self) -> &Path {
        &self.paths.cache
    }
}

struct HaltExecutor {
    calls: Mutex<usize>,
}

impl HaltExecutor {
    fn new() -> Self {
        Self {
            calls: Mutex::new(0),
        }
    }
}

impl GitServerExecutor for HaltExecutor {
    fn exec(
        &self,
        _program: &str,
        _mirror: &mac_worker::rooted_fs::RootedDir,
        _environment: &[(OsString, OsString)],
    ) -> Result<Infallible, WorkerError> {
        *self.calls.lock().unwrap() += 1;
        Err(WorkerError::Protocol(
            "TEST_EXECUTOR_INVOKED: sentinel".into(),
        ))
    }
}

fn fingerprint() -> RequestFingerprint {
    RequestFingerprint::new("ab".repeat(32)).unwrap()
}

fn other_fingerprint() -> RequestFingerprint {
    RequestFingerprint::new("cd".repeat(32)).unwrap()
}

fn request_id(n: u128) -> String {
    format!("{:x}", Uuid::from_u128(n).simple())
}

fn oid_of(repo: &GitRepo) -> BaseOid {
    let stdout = repo.git(&["rev-parse", "HEAD"]).stdout;
    String::from_utf8(stdout).unwrap().trim().parse().unwrap()
}

fn poorly_compressible(len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    let mut state = seed | 1;
    for byte in &mut bytes {
        state = state.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
        *byte = (state >> 24) as u8;
    }
    bytes
}

fn pack_size(repo: &Path, oid: &BaseOid) -> usize {
    let mut command = Command::new("/usr/bin/git");
    command
        .current_dir(repo)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .args(["pack-objects", "--stdout", "--revs"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn pack-objects");
    {
        let mut stdin = child.stdin.take().expect("pack-objects stdin");
        writeln!(stdin, "{oid}").unwrap();
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "pack-objects failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout.len()
}

fn assert_packed_over_limit(repo: &Path, oid: &BaseOid, label: &str) {
    let size = pack_size(repo, oid);
    assert!(
        size >= MIN_PACK_BYTES,
        "{label} packed representation is {size} bytes, need >1 MiB"
    );
}

fn commit_large(repo: &GitRepo, name: &str, seed: u64) -> BaseOid {
    repo.write(name, &poorly_compressible(LARGE_BYTES, seed));
    repo.commit_all(name);
    let oid = oid_of(repo);
    assert_packed_over_limit(repo.root(), &oid, name);
    oid
}

fn git_ref_cas_busy(error: &WorkerError) -> bool {
    let WorkerError::Git { code, message } = error else {
        return false;
    };
    if *code != "BASE_PUSH_FAILED" {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    lower.contains("cannot lock ref")
        || (lower.contains("unable to create") && lower.contains(".lock"))
        || lower.contains("failed to update ref")
        || lower.contains("failed to lock")
        || (lower.contains("remote rejected") && lower.contains("lock"))
}

fn same_token_source_needs_idempotent_replay(
    first: &Result<PushReceipt, WorkerError>,
    second: &Result<PushReceipt, WorkerError>,
) -> bool {
    match (first, second) {
        (Ok(_), Ok(_)) => false,
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => {
            assert!(
                git_ref_cas_busy(error),
                "same-token first attempt failed without Git ref-CAS lock evidence: {error:?}"
            );
            true
        }
        (Err(first_error), Err(second_error)) => {
            panic!("both first same-token source pushes failed: {first_error:?}; {second_error:?}")
        }
    }
}

fn git_at(dir: &Path, args: &[&str]) -> std::process::Output {
    let mut command = Command::new("/usr/bin/git");
    command
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .current_dir(dir)
        .args(args);
    command.output().unwrap()
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

fn client_state_untouched(isolated: &Isolated) {
    assert!(
        !isolated.paths.state.exists(),
        "controller transfer must not create laptop ClientStateStore"
    );
}

fn shared_hook_absent(cache: &Path) {
    assert!(!cache.join("hooks/pre-receive").exists());
}

fn invocation_hook_present(cache: &Path, token: &str) -> bool {
    let scratch = cache.join("scratch");
    let prefix = format!("hooks-{token}-");
    scratch
        .read_dir()
        .map(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                let name = entry.file_name();
                name.to_string_lossy().starts_with(&prefix)
                    && entry.path().join("pre-receive").is_file()
            })
        })
        .unwrap_or(false)
}

fn source_record_path(isolated: &Isolated, request: &str) -> PathBuf {
    isolated
        .paths
        .controller_state_root()
        .join(format!("src-{request}.json"))
}

fn source_lookup_path(isolated: &Isolated, token: &str) -> PathBuf {
    isolated
        .paths
        .controller_state_root()
        .join(format!("tok-{token}.json"))
}

fn result_lookup_path(isolated: &Isolated, token: &str) -> PathBuf {
    isolated
        .paths
        .controller_state_root()
        .join(format!("rtk-{token}.json"))
}

fn result_record_path(isolated: &Isolated, request: &str, turn: TurnId) -> PathBuf {
    isolated
        .paths
        .controller_state_root()
        .join(format!("res-{request}-{turn}.json"))
}

fn poison_remote_git_env(isolated: &Isolated, peer: &GitRepo, ambient_config: &Path) {
    let git_dir = peer.root().join(".git");
    let original = fs::read_to_string(&isolated.fake_ssh).unwrap();
    let poison = format!(
        "export GIT_DIR={git_dir:?}\nexport GIT_WORK_TREE={work:?}\nexport GIT_COMMON_DIR={git_dir:?}\nexport GIT_OBJECT_DIRECTORY={objects:?}\nexport GIT_INDEX_FILE={index:?}\nexport GIT_CONFIG={config:?}\nexport GIT_CONFIG_GLOBAL={ambient:?}\nexport GIT_CONFIG_COUNT=1\nexport GIT_CONFIG_KEY_0=user.name\nexport GIT_CONFIG_VALUE_0=ambient-leak\nexport GIT_CONFIG_PARAMETERS='user.email=ambient@example.com'\n",
        git_dir = git_dir,
        work = peer.root(),
        objects = git_dir.join("objects"),
        index = git_dir.join("index"),
        config = git_dir.join("config"),
        ambient = ambient_config,
    );
    let (shebang, rest) = original.split_once('\n').unwrap();
    fs::write(&isolated.fake_ssh, format!("{shebang}\n{poison}{rest}")).unwrap();
}

fn advertised_controller_refs(isolated: &Isolated, identity: &ControllerResultIdentity) -> String {
    let upload = format!(
        "~/.local/bin/worker host controller-upload-pack {} {} {} {} {} {}",
        identity.token(),
        identity.request_id(),
        identity.fingerprint(),
        identity.task_id(),
        identity.turn_id(),
        identity.imported_oid()
    );
    let output = Command::new("/usr/bin/git")
        .env("GIT_SSH_COMMAND", &isolated.fake_ssh)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .args([
            "ls-remote",
            &format!("--upload-pack={upload}"),
            &format!("{FAKE_SSH_DEST}:{PROJECT_ID}"),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "ls-remote failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn flow_resolver_hook_is_domain_separated_and_not_the_laptop_transfer_repo() {
    let isolated = Isolated::new();
    let cache_id = controller_transfer_cache_id(PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(cache_id.len(), 64);
    assert_ne!(
        cache_id,
        controller_transfer_cache_id(PROJECT_ID, OTHER_WORKTREE).unwrap()
    );
    assert_eq!(
        CONTROLLER_TRANSFER_CACHE_DOMAIN,
        b"mac-worker/controller-transfer-cache\0"
    );

    let created =
        TransferRepo::open_or_create_controller_cache(isolated.cache(), PROJECT_ID, WORKTREE_ID)
            .unwrap();
    let expected = controller_transfer_git_path(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(created.path(), expected);
    assert_eq!(created.repo_id(), cache_id);
    assert!(
        created
            .path()
            .to_string_lossy()
            .contains("/controller-transfer/")
    );
    assert!(!created.path().to_string_lossy().contains("/transfer/"));

    let helper =
        TransferRepo::open_controller_cache(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    assert_eq!(helper.path(), created.path());

    let user = GitRepo::init();
    let laptop = TransferRepo::open_or_create(isolated.cache(), &user.root().join(".git")).unwrap();
    assert_ne!(laptop.path(), created.path());
    assert_eq!(
        laptop.repo_id(),
        repo_id_for(&user.root().join(".git")).unwrap()
    );
    client_state_untouched(&isolated);
}

#[test]
fn nested_digests_ignore_token_and_client_payload_sha256() {
    let oid: BaseOid = "0123456789abcdef0123456789abcdef01234567".parse().unwrap();
    let first = source_digest(PROJECT_ID, WORKTREE_ID, &oid).unwrap();
    let second = source_digest(PROJECT_ID, WORKTREE_ID, &oid).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.len(), 64);
    let other_oid: BaseOid = "89abcdef0123456789abcdef0123456789abcdef".parse().unwrap();
    assert_ne!(
        first,
        source_digest(PROJECT_ID, WORKTREE_ID, &other_oid).unwrap()
    );
    let task = TaskId::new(Uuid::from_u128(7));
    let turn = TurnId::new(Uuid::from_u128(8));
    let result = result_digest(task, turn, &oid).unwrap();
    assert_ne!(first, result);
    assert_eq!(
        result,
        mac_worker::controller::protocol::canonical_request_sha256(
            PROTOCOL_VERSION,
            "controller.transfer.result",
            &serde_json::json!({
                "imported_oid": oid.as_str(),
                "task_id": task.to_string(),
                "turn_id": turn.to_string(),
            })
        )
        .unwrap()
    );
}

#[test]
fn hidden_commands_parse_and_stay_off_host_help() {
    let receive = Cli::try_parse_from([
        "worker",
        "host",
        "controller-receive-pack",
        &request_id(1),
        &request_id(2),
        &"ab".repeat(32),
        PROJECT_ID,
        WORKTREE_ID,
        "0123456789abcdef0123456789abcdef01234567",
        PROJECT_ID,
    ])
    .unwrap();
    assert!(matches!(
        receive.command,
        WorkerCommand::Host {
            command: HostCommand::ControllerReceivePack { .. }
        }
    ));
    let upload = Cli::try_parse_from([
        "worker",
        "host",
        "controller-upload-pack",
        &request_id(1),
        &request_id(2),
        &"ab".repeat(32),
        &request_id(3),
        &request_id(4),
        "0123456789abcdef0123456789abcdef01234567",
        PROJECT_ID,
    ])
    .unwrap();
    assert!(matches!(
        upload.command,
        WorkerCommand::Host {
            command: HostCommand::ControllerUploadPack { .. }
        }
    ));
    assert_eq!(
        HostOperation::ControllerReceivePack.command(),
        "~/.local/bin/worker host controller-receive-pack"
    );
    assert_eq!(
        HostOperation::ControllerUploadPack.command(),
        "~/.local/bin/worker host controller-upload-pack"
    );
}

#[test]
fn source_push_over_fake_ssh_pins_owned_graph_and_retries_the_same_oid() {
    let isolated = Isolated::new();
    let repo = GitRepo::init();
    let oid = commit_large(&repo, "blob.bin", 0x11);
    let request = request_id(0x11);
    let identity = {
        let transfer = isolated.transfer();
        transfer
            .prepare_source_receive(
                isolated.cache(),
                &SystemProcessRunner,
                &request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap()
    };
    let cache_path =
        controller_transfer_git_path(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    assert!(cache_path.join("HEAD").exists());
    shared_hook_absent(&cache_path);

    let lookup = source_lookup_path(&isolated, identity.token());
    assert!(lookup.exists());
    fs::remove_file(&lookup).unwrap();
    let recovered = {
        let transfer = isolated.transfer();
        transfer
            .prepare_source_receive(
                isolated.cache(),
                &SystemProcessRunner,
                &request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap()
    };
    assert_eq!(recovered.token(), identity.token());
    assert_eq!(recovered.request_id(), identity.request_id());
    assert!(lookup.exists());
    assert_eq!(
        fs::read(&lookup).unwrap(),
        fs::read(source_record_path(&isolated, &request)).unwrap()
    );

    let (first_push, second) = thread::scope(|scope| {
        let isolated = &isolated;
        let identity = &identity;
        let repo = &repo;
        let first = scope.spawn(|| {
            isolated.transport().push_controller_source(
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
        });
        let second = scope.spawn(|| {
            isolated.transport().push_controller_source(
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
        });
        (first.join().unwrap(), second.join().unwrap())
    });
    if same_token_source_needs_idempotent_replay(&first_push, &second) {
        isolated
            .transport()
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
            .expect("idempotent same-OID replay after Git ref-CAS busy");
    }
    assert_packed_over_limit(&cache_path, &oid, "controller source after receive-pack");

    let receipt = {
        let transfer = isolated.transfer();
        transfer
            .finish_source_receive(isolated.cache(), &SystemProcessRunner, &identity)
            .unwrap()
    };
    assert_eq!(receipt.oid(), &oid);
    assert_eq!(
        receipt.request_ref(),
        TransferRepo::frozen_request_ref(&request).unwrap()
    );

    let retry = {
        let transfer = isolated.transfer();
        transfer
            .prepare_source_receive(
                isolated.cache(),
                &SystemProcessRunner,
                &request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap()
    };
    assert_eq!(retry.token(), identity.token());
    isolated
        .transport()
        .push_controller_source(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            retry.token(),
            retry.request_id(),
            retry.fingerprint(),
            retry.project_id(),
            retry.worktree_id(),
            retry.expected_oid(),
            repo.root(),
        )
        .unwrap();
    {
        let transfer = isolated.transfer();
        let again = transfer
            .finish_source_receive(isolated.cache(), &SystemProcessRunner, &retry)
            .unwrap();
        assert_eq!(again.oid(), &oid);
    }

    let mut stale: serde_json::Value = serde_json::from_slice(&fs::read(&lookup).unwrap()).unwrap();
    stale["receipt_oid"] = serde_json::Value::Null;
    fs::write(&lookup, serde_json::to_vec(&stale).unwrap()).unwrap();
    let after_receipt_gap = {
        let transfer = isolated.transfer();
        transfer
            .finish_source_receive(isolated.cache(), &SystemProcessRunner, &identity)
            .unwrap()
    };
    assert_eq!(after_receipt_gap.oid(), &oid);
    assert_eq!(after_receipt_gap.token(), identity.token());
    let repaired_lookup: serde_json::Value =
        serde_json::from_slice(&fs::read(&lookup).unwrap()).unwrap();
    assert_eq!(
        repaired_lookup["receipt_oid"],
        serde_json::json!(oid.as_str())
    );
    assert_eq!(
        fs::read(&lookup).unwrap(),
        fs::read(source_record_path(&isolated, &request)).unwrap()
    );

    fs::remove_dir_all(repo.root().join(".git/objects")).unwrap();
    let cat = git_at(&cache_path, &["cat-file", "-t", oid.as_str()]);
    assert!(cat.status.success());
    assert_eq!(String::from_utf8_lossy(&cat.stdout).trim(), "commit");
    let rev = git_at(&cache_path, &["rev-list", "--objects", oid.as_str()]);
    assert!(rev.status.success());
    assert!(String::from_utf8_lossy(&rev.stdout).contains(oid.as_str()));
    shared_hook_absent(&cache_path);
    assert!(invocation_hook_present(&cache_path, identity.token()));
    client_state_untouched(&isolated);
}

#[test]
fn fingerprint_and_oid_conflicts_and_corrupt_tokens_never_yield_a_receipt() {
    let isolated = Isolated::new();
    let repo = GitRepo::init();
    let oid = commit_large(&repo, "blob.bin", 0x22);
    repo.write("other.bin", b"different-oid\n");
    repo.commit_all("other");
    let other = oid_of(&repo);
    let request = request_id(0x22);
    let transfer = isolated.transfer();
    let identity = transfer
        .prepare_source_receive(
            isolated.cache(),
            &SystemProcessRunner,
            &request,
            &fingerprint(),
            PROJECT_ID,
            WORKTREE_ID,
            &oid,
        )
        .unwrap();

    let conflict = transfer
        .prepare_source_receive(
            isolated.cache(),
            &SystemProcessRunner,
            &request,
            &other_fingerprint(),
            PROJECT_ID,
            WORKTREE_ID,
            &oid,
        )
        .unwrap_err();
    assert_eq!(conflict.public_code(), "CONTROLLER_REQUEST_CONFLICT");

    let oid_conflict = transfer
        .prepare_source_receive(
            isolated.cache(),
            &SystemProcessRunner,
            &request,
            &fingerprint(),
            PROJECT_ID,
            WORKTREE_ID,
            &other,
        )
        .unwrap_err();
    assert_eq!(oid_conflict.public_code(), "CONTROLLER_REQUEST_CONFLICT");

    let executor = HaltExecutor::new();
    let missing = transfer
        .receive_pack(
            isolated.cache(),
            &request_id(0x99),
            &request_id(0x99),
            fingerprint().as_str(),
            PROJECT_ID,
            WORKTREE_ID,
            oid.as_str(),
            Some(PROJECT_ID),
            &executor,
        )
        .unwrap_err();
    assert_eq!(missing.public_code(), "INVALID_COMPONENT");
    assert_eq!(*executor.calls.lock().unwrap(), 0);

    let wrong_token = transfer
        .receive_pack(
            isolated.cache(),
            &request_id(0x99),
            &request,
            fingerprint().as_str(),
            PROJECT_ID,
            WORKTREE_ID,
            oid.as_str(),
            Some(PROJECT_ID),
            &executor,
        )
        .unwrap_err();
    assert_eq!(wrong_token.public_code(), "TRANSFER_IDENTITY_MISMATCH");
    assert_eq!(*executor.calls.lock().unwrap(), 0);

    let wrong_path = transfer
        .receive_pack(
            isolated.cache(),
            identity.token(),
            identity.request_id(),
            identity.fingerprint().as_str(),
            PROJECT_ID,
            WORKTREE_ID,
            oid.as_str(),
            Some(OTHER_WORKTREE),
            &executor,
        )
        .unwrap_err();
    assert_eq!(wrong_path.public_code(), "INVALID_COMPONENT");
    assert_eq!(*executor.calls.lock().unwrap(), 0);

    let token_path = source_lookup_path(&isolated, identity.token());
    let original = fs::read(&token_path).unwrap();
    fs::write(&token_path, &original[..original.len() / 2]).unwrap();
    let transfer = isolated.transfer();
    let repaired = transfer
        .receive_pack(
            isolated.cache(),
            identity.token(),
            identity.request_id(),
            identity.fingerprint().as_str(),
            PROJECT_ID,
            WORKTREE_ID,
            oid.as_str(),
            Some(PROJECT_ID),
            &executor,
        )
        .unwrap_err();
    assert_eq!(repaired.public_code(), "TEST_EXECUTOR_INVOKED");
    assert_eq!(*executor.calls.lock().unwrap(), 1);
    assert_eq!(fs::read(&token_path).unwrap(), original);

    let request_path = source_record_path(&isolated, &request);
    let request_bytes = fs::read(&request_path).unwrap();
    fs::write(&request_path, &request_bytes[..request_bytes.len() / 2]).unwrap();
    let truncated = transfer
        .receive_pack(
            isolated.cache(),
            identity.token(),
            identity.request_id(),
            identity.fingerprint().as_str(),
            PROJECT_ID,
            WORKTREE_ID,
            oid.as_str(),
            Some(PROJECT_ID),
            &executor,
        )
        .unwrap_err();
    assert_eq!(truncated.public_code(), "CONTROLLER_TRANSPORT");
    assert_eq!(*executor.calls.lock().unwrap(), 1);
    fs::write(&request_path, request_bytes).unwrap();

    let cache_path =
        controller_transfer_git_path(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    fs::remove_dir_all(&cache_path).unwrap();
    let missing_cache = transfer
        .receive_pack(
            isolated.cache(),
            identity.token(),
            identity.request_id(),
            identity.fingerprint().as_str(),
            PROJECT_ID,
            WORKTREE_ID,
            oid.as_str(),
            Some(PROJECT_ID),
            &executor,
        )
        .unwrap_err();
    assert_eq!(missing_cache.public_code(), "BASE_UNAVAILABLE");
    assert_eq!(*executor.calls.lock().unwrap(), 1);
    assert!(!cache_path.join("HEAD").exists());
    client_state_untouched(&isolated);
}

fn result_upload_pack_err(
    isolated: &Isolated,
    identity: &ControllerResultIdentity,
    executor: &HaltExecutor,
) -> WorkerError {
    isolated
        .transfer()
        .upload_pack(
            isolated.cache(),
            identity.token(),
            identity.request_id(),
            identity.fingerprint().as_str(),
            &identity.task_id().to_string(),
            &identity.turn_id().to_string(),
            identity.imported_oid().as_str(),
            Some(PROJECT_ID),
            executor,
        )
        .unwrap_err()
}

#[test]
fn altered_result_digest_never_reaches_upload_pack() {
    let isolated = Isolated::new();
    let results = GitRepo::init();
    results.write("seed.txt", b"controller-result\n");
    results.commit_all("seed");
    let oid = oid_of(&results);
    TransferRepo::open_or_create_controller_cache(isolated.cache(), PROJECT_ID, WORKTREE_ID)
        .unwrap();
    let cache_path =
        controller_transfer_git_path(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    fetch_oid_into(&cache_path, &results.root().join(".git"), &oid);

    let request = request_id(0x77);
    let task = TaskId::new(Uuid::from_u128(71));
    let turn = TurnId::new(Uuid::from_u128(72));
    let meta = VerifiedResultMeta {
        task_id: task,
        turn_id: turn,
        imported_oid: oid.clone(),
        worker: "mini-1".into(),
    };
    let identity = {
        let transfer = isolated.transfer();
        transfer
            .prepare_result_upload(
                isolated.cache(),
                &SystemProcessRunner,
                &request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &meta,
            )
            .unwrap()
    };
    let path = result_record_path(&isolated, &request, turn);
    let original = fs::read(&path).unwrap();
    let mut tampered: serde_json::Value = serde_json::from_slice(&original).unwrap();
    assert_eq!(
        tampered["result_digest"],
        serde_json::json!(result_digest(task, turn, &oid).unwrap())
    );
    tampered["result_digest"] = serde_json::json!("ab".repeat(32));
    fs::write(&path, serde_json::to_vec(&tampered).unwrap()).unwrap();

    let executor = HaltExecutor::new();
    let digest_err = result_upload_pack_err(&isolated, &identity, &executor);
    assert_eq!(digest_err.public_code(), "TRANSFER_IDENTITY_MISMATCH");
    assert_eq!(*executor.calls.lock().unwrap(), 0);

    let mut wrong_kind: serde_json::Value = serde_json::from_slice(&original).unwrap();
    wrong_kind["kind"] = serde_json::json!("source_receive");
    fs::write(&path, serde_json::to_vec(&wrong_kind).unwrap()).unwrap();
    let kind_err = result_upload_pack_err(&isolated, &identity, &executor);
    assert_eq!(kind_err.public_code(), "TRANSFER_IDENTITY_MISMATCH");
    assert_eq!(*executor.calls.lock().unwrap(), 0);

    let mut swapped: serde_json::Value = serde_json::from_slice(&original).unwrap();
    swapped["request_id"] = serde_json::json!(request_id(0x88));
    fs::write(&path, serde_json::to_vec(&swapped).unwrap()).unwrap();
    let name_err = result_upload_pack_err(&isolated, &identity, &executor);
    assert_eq!(name_err.public_code(), "TRANSFER_IDENTITY_MISMATCH");
    assert_eq!(*executor.calls.lock().unwrap(), 0);
    client_state_untouched(&isolated);
}

#[test]
fn upload_pack_ignores_ambient_git_env_and_does_not_touch_a_peer_repo() {
    let isolated = Isolated::new();
    let results = GitRepo::init();
    results.write("seed.txt", b"controller-result\n");
    results.commit_all("seed");
    let oid = oid_of(&results);
    TransferRepo::open_or_create_controller_cache(isolated.cache(), PROJECT_ID, WORKTREE_ID)
        .unwrap();
    let cache_path =
        controller_transfer_git_path(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    fetch_oid_into(&cache_path, &results.root().join(".git"), &oid);

    let request = request_id(0x78);
    let task = TaskId::new(Uuid::from_u128(81));
    let turn = TurnId::new(Uuid::from_u128(82));
    let meta = VerifiedResultMeta {
        task_id: task,
        turn_id: turn,
        imported_oid: oid.clone(),
        worker: "mini-1".into(),
    };
    let identity = {
        let transfer = isolated.transfer();
        transfer
            .prepare_result_upload(
                isolated.cache(),
                &SystemProcessRunner,
                &request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &meta,
            )
            .unwrap()
    };

    let peer = GitRepo::init();
    peer.write("secret.txt", b"peer-secret\n");
    peer.commit_all("peer");
    assert!(
        peer.git(&["update-ref", "refs/heads/peer-secret", "HEAD"])
            .status
            .success()
    );
    assert!(
        peer.git(&["config", "user.name", "peer-leak"])
            .status
            .success()
    );
    let refs_before = peer.git(&["show-ref"]).stdout;
    let objects_before = peer.git(&["rev-list", "--objects", "--all"]).stdout;
    let config_before = fs::read(peer.root().join(".git/config")).unwrap();
    let ambient = isolated.cache().join("ambient.gitconfig");
    fs::write(&ambient, "[user]\n\tname = ambient-leak\n").unwrap();
    poison_remote_git_env(&isolated, &peer, &ambient);

    let advertised = advertised_controller_refs(&isolated, &identity);
    let result_ref = frozen_result_ref(&request, turn).unwrap();
    assert!(
        advertised.contains(&result_ref),
        "missing allowed result ref in {advertised:?}"
    );
    assert!(
        !advertised.contains("peer-secret"),
        "peer ref leaked through upload-pack: {advertised:?}"
    );

    let laptop_cache = isolated.paths.cache.join("laptop-user");
    fs::create_dir_all(&laptop_cache).unwrap();
    let user = GitRepo::init();
    let laptop = TransferRepo::open_or_create(&laptop_cache, &user.root().join(".git")).unwrap();
    isolated
        .transport()
        .fetch_controller_result(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            identity.token(),
            identity.request_id(),
            identity.fingerprint(),
            identity.project_id(),
            identity.task_id(),
            identity.turn_id(),
            identity.imported_oid(),
            laptop.path(),
        )
        .unwrap();
    assert_eq!(
        git_at(laptop.path(), &["rev-parse", result_ref.as_str()]).stdout,
        format!("{oid}\n").into_bytes()
    );

    assert_eq!(peer.git(&["show-ref"]).stdout, refs_before);
    assert_eq!(
        peer.git(&["rev-list", "--objects", "--all"]).stdout,
        objects_before
    );
    assert_eq!(
        fs::read(peer.root().join(".git/config")).unwrap(),
        config_before
    );
    client_state_untouched(&isolated);
}

#[test]
fn concurrent_identities_use_isolated_hooks_and_result_next_turn_keeps_the_earlier_oid() {
    let isolated = Isolated::new();
    let first_repo = GitRepo::init();
    let second_repo = GitRepo::init();
    let first_oid = commit_large(&first_repo, "one.bin", 0x44);
    let second_oid = commit_large(&second_repo, "two.bin", 0x55);
    let first_request = request_id(0x44);
    let second_request = request_id(0x55);
    let transfer = isolated.transfer();
    let first = transfer
        .prepare_source_receive(
            isolated.cache(),
            &SystemProcessRunner,
            &first_request,
            &fingerprint(),
            PROJECT_ID,
            WORKTREE_ID,
            &first_oid,
        )
        .unwrap();
    let second = transfer
        .prepare_source_receive(
            isolated.cache(),
            &SystemProcessRunner,
            &second_request,
            &other_fingerprint(),
            PROJECT_ID,
            OTHER_WORKTREE,
            &second_oid,
        )
        .unwrap();

    thread::scope(|scope| {
        let isolated = &isolated;
        scope.spawn(|| {
            isolated
                .transport()
                .push_controller_source(
                    isolated.fake_ssh.to_str().unwrap(),
                    FAKE_SSH_DEST,
                    "~/.local/bin/worker",
                    first.token(),
                    first.request_id(),
                    first.fingerprint(),
                    first.project_id(),
                    first.worktree_id(),
                    first.expected_oid(),
                    first_repo.root(),
                )
                .unwrap();
        });
        scope.spawn(|| {
            isolated
                .transport()
                .push_controller_source(
                    isolated.fake_ssh.to_str().unwrap(),
                    FAKE_SSH_DEST,
                    "~/.local/bin/worker",
                    second.token(),
                    second.request_id(),
                    second.fingerprint(),
                    second.project_id(),
                    second.worktree_id(),
                    second.expected_oid(),
                    second_repo.root(),
                )
                .unwrap();
        });
    });

    transfer
        .finish_source_receive(isolated.cache(), &SystemProcessRunner, &first)
        .unwrap();
    transfer
        .finish_source_receive(isolated.cache(), &SystemProcessRunner, &second)
        .unwrap();

    let first_cache =
        controller_transfer_git_path(isolated.cache(), PROJECT_ID, WORKTREE_ID).unwrap();
    let second_cache =
        controller_transfer_git_path(isolated.cache(), PROJECT_ID, OTHER_WORKTREE).unwrap();
    assert_ne!(first_cache, second_cache);
    assert!(invocation_hook_present(&first_cache, first.token()));
    assert!(invocation_hook_present(&second_cache, second.token()));
    shared_hook_absent(&first_cache);
    shared_hook_absent(&second_cache);

    assert_packed_over_limit(&first_cache, &first_oid, "distinct-token source");
    assert_packed_over_limit(&second_cache, &second_oid, "distinct-token source");

    let results = GitRepo::init();
    let turn1 = commit_large(&results, "turn1.bin", 0x61);
    let turn2 = commit_large(&results, "turn2.bin", 0x62);
    fetch_oid_into(&first_cache, &results.root().join(".git"), &turn1);
    fetch_oid_into(&first_cache, &results.root().join(".git"), &turn2);

    let task = TaskId::new(Uuid::from_u128(21));
    let turn_one = TurnId::new(Uuid::from_u128(31));
    let turn_two = TurnId::new(Uuid::from_u128(32));
    let meta1 = VerifiedResultMeta {
        task_id: task,
        turn_id: turn_one,
        imported_oid: turn1.clone(),
        worker: "mini-1".into(),
    };
    let meta2 = VerifiedResultMeta {
        task_id: task,
        turn_id: turn_two,
        imported_oid: turn2.clone(),
        worker: "mini-1".into(),
    };
    let result1 = {
        let transfer = isolated.transfer();
        transfer
            .prepare_result_upload(
                isolated.cache(),
                &SystemProcessRunner,
                &first_request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &meta1,
            )
            .unwrap()
    };
    let result_lookup = result_lookup_path(&isolated, result1.token());
    assert!(result_lookup.exists());
    fs::remove_file(&result_lookup).unwrap();
    let recovered_result = {
        let transfer = isolated.transfer();
        transfer
            .prepare_result_upload(
                isolated.cache(),
                &SystemProcessRunner,
                &first_request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &meta1,
            )
            .unwrap()
    };
    assert_eq!(recovered_result.token(), result1.token());
    assert!(result_lookup.exists());
    assert_eq!(
        fs::read(&result_lookup).unwrap(),
        fs::read(
            isolated
                .paths
                .controller_state_root()
                .join(format!("res-{first_request}-{turn_one}.json"))
        )
        .unwrap()
    );
    let result2 = {
        let transfer = isolated.transfer();
        transfer
            .prepare_result_upload(
                isolated.cache(),
                &SystemProcessRunner,
                &first_request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &meta2,
            )
            .unwrap()
    };
    assert_ne!(result1.token(), result2.token());
    assert_eq!(
        frozen_result_ref(&first_request, turn_one).unwrap(),
        TransferRepo::frozen_controller_result_ref(&first_request, &turn_one.to_string()).unwrap()
    );

    let user = GitRepo::init();
    let laptop_cache = isolated.paths.cache.join("laptop-user");
    fs::create_dir_all(&laptop_cache).unwrap();
    let laptop = TransferRepo::open_or_create(&laptop_cache, &user.root().join(".git")).unwrap();
    let peer_user = GitRepo::init();
    let peer_cache = isolated.paths.cache.join("laptop-peer");
    fs::create_dir_all(&peer_cache).unwrap();
    let peer = TransferRepo::open_or_create(&peer_cache, &peer_user.root().join(".git")).unwrap();
    thread::scope(|scope| {
        let isolated = &isolated;
        let result1 = &result1;
        let dests = [laptop.path().to_path_buf(), peer.path().to_path_buf()];
        for dest in dests {
            scope.spawn(move || {
                isolated
                    .transport()
                    .fetch_controller_result(
                        isolated.fake_ssh.to_str().unwrap(),
                        FAKE_SSH_DEST,
                        "~/.local/bin/worker",
                        result1.token(),
                        result1.request_id(),
                        result1.fingerprint(),
                        result1.project_id(),
                        result1.task_id(),
                        result1.turn_id(),
                        result1.imported_oid(),
                        &dest,
                    )
                    .unwrap();
            });
        }
    });
    assert_packed_over_limit(laptop.path(), &turn1, "fetched controller result");
    assert_packed_over_limit(peer.path(), &turn1, "peer fetched controller result");
    {
        let transfer = isolated.transfer();
        transfer
            .prepare_result_upload(
                isolated.cache(),
                &SystemProcessRunner,
                &first_request,
                &fingerprint(),
                PROJECT_ID,
                WORKTREE_ID,
                &meta2,
            )
            .unwrap();
    }
    let fetched = isolated
        .transport()
        .fetch_controller_result(
            isolated.fake_ssh.to_str().unwrap(),
            FAKE_SSH_DEST,
            "~/.local/bin/worker",
            result1.token(),
            result1.request_id(),
            result1.fingerprint(),
            result1.project_id(),
            result1.task_id(),
            result1.turn_id(),
            result1.imported_oid(),
            laptop.path(),
        )
        .unwrap();
    assert_eq!(fetched.head().as_str(), turn1.as_str());
    assert_packed_over_limit(
        laptop.path(),
        &turn1,
        "next-turn still fetches earlier result",
    );
    let imported = import_controller_result(
        &laptop,
        &SystemProcessRunner,
        &user.root().join(".git"),
        &first_request,
        &meta1,
    )
    .unwrap();
    assert_eq!(imported.head().as_str(), turn1.as_str());
    client_state_untouched(&isolated);
}
