//! Synthetic packages over the real controller receive-pack/RPC boundary.
//! The raw two-ref Git push keeps this track independent of W6's push builder.

use crate::support::GitRepo;
use mac_worker::test_support::{
    client_state::ClientStateStore,
    controller::{
        ControllerFault, ControllerStore, TaskSubmitHandler, canonical_request_sha256,
        decode_frame, encode_json_frame, parse_request, registry::ProjectRegistry, source_digest,
    },
    core::{config::Config, paths::PathLayout, protocol::PROTOCOL_VERSION},
    host::{job::RequestFingerprint, process::SystemProcessRunner},
    session::{
        CODEX_ROLLOUT_FILE, PACKAGE_SESSION_DIR, PackageFile, PackageSource,
        REQUEST_SESSION_REF_PREFIX, SESSION_REF_PREFIX, SESSION_TOKEN, SessionAgent,
        SessionImportMeta, SessionPackage, WORKSPACE_TOKEN, codex_fixture,
    },
    task::{model::BaseOid, prepared_submit::FrozenSubmitBody},
    transfer::repo::TransferRepo,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::Write,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};
use tempfile::TempDir;

const PROJECT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const REQUEST: &str = "00000000000000000000000000000011";
const RUNNER: SystemProcessRunner = SystemProcessRunner;

struct Fixture {
    _temp: TempDir,
    environment: BTreeMap<OsString, OsString>,
    paths: PathLayout,
    ssh: PathBuf,
    repo: GitRepo,
    base: BaseOid,
    package: BaseOid,
    body: FrozenSubmitBody,
    request: mac_worker::test_support::controller::ControllerRequest,
}

impl Fixture {
    fn new(session: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let home = root.join("home");
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.clone().into_os_string()),
            (
                OsString::from("XDG_STATE_HOME"),
                root.join("state").into_os_string(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                root.join("cache").into_os_string(),
            ),
            (
                OsString::from("XDG_CONFIG_HOME"),
                root.join("config").into_os_string(),
            ),
            (
                OsString::from("XDG_DATA_HOME"),
                root.join("data").into_os_string(),
            ),
            (
                OsString::from("XDG_RUNTIME_DIR"),
                root.join("runtime").into_os_string(),
            ),
        ]);
        for path in environment.values() {
            fs::create_dir_all(path).unwrap();
        }
        fs::create_dir_all(home.join(".local/bin")).unwrap();
        symlink(env!("CARGO_BIN_EXE_worker"), home.join(".local/bin/worker")).unwrap();
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        let ssh = root.join("fake-ssh");
        let mut script = String::from("#!/bin/sh\n");
        for (key, value) in &environment {
            script.push_str(&format!("export {}={:?}\n", key.to_str().unwrap(), value));
        }
        script
            .push_str("shift\nif [ \"$#\" -eq 1 ]; then exec /bin/sh -c \"$1\"; fi\nexec \"$@\"\n");
        fs::write(&ssh, script).unwrap();
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700)).unwrap();
        let repo = GitRepo::init();
        repo.write("README", b"synthetic source\n");
        repo.commit_all("base");
        let base = head(&repo);
        // An orphan canonical package commit, independent of the source tree.
        repo.git(&["checkout", "--orphan", "session-package"]);
        repo.git(&["rm", "-rf", "."]);
        let session_package = SessionPackage::build(
            PackageSource {
                agent: SessionAgent::Codex,
                source_session_id: "018f0f4a-6b5c-7d8e-9f00-112233445599".into(),
                source_agent_version: "0.160.0".into(),
                source_cwd_relative: ".".into(),
                scrubbed: 0,
            },
            vec![PackageFile {
                path: CODEX_ROLLOUT_FILE.into(),
                bytes: codex_fixture(SESSION_TOKEN, WORKSPACE_TOKEN, "0.160.0", 1),
            }],
        )
        .unwrap();
        repo.write("manifest.json", &session_package.manifest_json());
        for file in session_package.files() {
            repo.write(&format!("{PACKAGE_SESSION_DIR}/{}", file.path), &file.bytes);
        }
        repo.commit_all("package");
        let package = head(&repo);
        let mut body: FrozenSubmitBody = serde_json::from_value(json!({
            "task_id": "018f0f4a6b5c7d8e9f00112233445566",
            "turn_id": "018f0f4a6b5c7d8e9f00112233445577",
            "run_id": null, "created_at_millis": 1700000000000u64,
            "prompt": "continue synthetic conversation", "title": null,
            "agent": "codex", "model": null, "effort": null,
            "source": "local", "origin_url": null, "publish": ["fetch"],
            "publish_branch": null, "close_on": "never", "env_profile": null,
            "worker": null, "wip": true, "project_id": PROJECT, "worktree_id": WORKTREE,
            "base_oid": base.as_str(), "timeout_millis": 2700000,
            "max_turns": null, "max_budget_usd_cents": null, "max_followups": 10,
            "permissions": "workspace", "requires": [], "include_untracked": [],
            "include_empty_dirs": [], "allow_sensitive": [], "cli_includes": [],
            "branch": null, "wait_for_capacity": true
        }))
        .unwrap();
        if session {
            body.session_import = Some(
                SessionImportMeta::new(SessionAgent::Codex, package.as_str(), "0.160.0").unwrap(),
            );
        }
        let request = submit_request(&body);
        Self {
            _temp: temp,
            environment,
            paths,
            ssh,
            repo,
            base,
            package,
            body,
            request,
        }
    }

    fn rpc_output(&self, command: &str, body: Value) -> Output {
        let frame = encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "000000000000000000000000000011bb",
            "command": command, "body": body
        }))
        .unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_worker"))
            .envs(&self.environment)
            .args(["host", "controller-rpc"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&frame).unwrap();
        child.wait_with_output().unwrap()
    }

    fn rpc(&self, command: &str, body: Value) -> Value {
        let output = self.rpc_output(command, body);
        assert!(
            output.status.success(),
            "RPC failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(decode_frame(&output.stdout).unwrap()).unwrap()
    }

    fn source_body(&self, session: Option<&str>) -> Value {
        let mut body = json!({
            "request_id": REQUEST, "fingerprint": self.request.payload_sha256(),
            "project_id": PROJECT, "worktree_id": WORKTREE, "expected_oid": self.base.as_str()
        });
        if let Some(oid) = session {
            body["session_oid"] = json!(oid);
        }
        body
    }

    fn prepare(&self, session: Option<&str>) -> Value {
        self.rpc(
            "controller.transfer.source.prepare",
            self.source_body(session),
        )["result"]
            .clone()
    }

    fn finish_output(&self, identity: &Value, session: Option<&str>) -> Output {
        let mut body = self.source_body(session);
        body["token"] = identity["token"].clone();
        self.rpc_output("controller.transfer.source.finish", body)
    }

    fn finish(&self, identity: &Value, session: Option<&str>) -> Value {
        let output = self.finish_output(identity, session);
        assert!(
            output.status.success(),
            "finish failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(decode_frame(&output.stdout).unwrap()).unwrap()["result"]
            .clone()
    }

    fn push(&self, identity: &Value, refspecs: &[String]) -> Output {
        git(self.repo.root(), &[
            "push".into(), "--atomic".into(), "--porcelain".into(), "--no-verify".into(),
            format!("--receive-pack=~/.local/bin/worker host controller-receive-pack {} {REQUEST} {} {PROJECT} {WORKTREE} {}", identity["token"].as_str().unwrap(), self.request.payload_sha256(), identity["expected_oid"].as_str().unwrap()),
            format!("fakecontroller:{PROJECT}"),
        ].into_iter().chain(refspecs.iter().cloned()).collect::<Vec<_>>(), Some(&self.ssh))
    }

    fn specs(&self, session: Option<&BaseOid>) -> Vec<String> {
        let mut specs = vec![format!("{}:refs/mac-worker/requests/{REQUEST}", self.base)];
        if let Some(oid) = session {
            specs.push(format!("{oid}:{REQUEST_SESSION_REF_PREFIX}{REQUEST}"));
        }
        specs
    }

    fn cache(&self) -> TransferRepo {
        TransferRepo::open_or_create_controller_cache(&self.paths.cache, PROJECT, WORKTREE).unwrap()
    }

    fn fetch_scratch(&self, oid: &BaseOid) {
        let output = git(
            self.cache().path(),
            &[
                "fetch".into(),
                "--no-write-fetch-head".into(),
                self.repo.root().to_str().unwrap().into(),
                format!("{oid}:refs/scratch/{oid}"),
            ],
            None,
        );
        assert!(
            output.status.success(),
            "scratch fetch failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn submit(&self) -> Result<(), mac_worker::test_support::core::error::WorkerError> {
        let config = Config::parse("version = 1\n").unwrap();
        let state = ClientStateStore::open(&self.paths.state).unwrap();
        let handler = TaskSubmitHandler::new(&RUNNER, &config, &self.paths, &state);
        ControllerStore::open(&self.paths.controller_state_root())
            .unwrap()
            .handle_with(&self.request, &handler, ControllerFault::None)
            .map(|_| ())
    }
}

fn submit_request(
    body: &FrozenSubmitBody,
) -> mac_worker::test_support::controller::ControllerRequest {
    parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION, "request_id": REQUEST,
            "command": "task.submit", "body": body
        }))
        .unwrap(),
    )
    .unwrap()
}

fn head(repo: &GitRepo) -> BaseOid {
    String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn git(path: &Path, args: &[String], ssh: Option<&Path>) -> Output {
    let mut command = Command::new("/usr/bin/git");
    command
        .current_dir(path)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE");
    if let Some(ssh) = ssh {
        command.env("GIT_SSH_COMMAND", ssh);
    }
    command.output().unwrap()
}

fn ref_oid(path: &Path, reference: &str) -> Option<String> {
    let output = git(
        path,
        &["rev-parse".into(), "--verify".into(), reference.into()],
        None,
    );
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).unwrap().trim().to_owned())
}

fn assert_failed(output: Output) {
    assert!(
        !output.status.success(),
        "unexpected success: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn review_r5_controller_pre_record_failure_retires_task_pin() {
    let mut f = Fixture::new(true);
    // A laptop may freeze a pinned name that no longer exists on the controller.
    f.body.worker = Some("removed-worker".into());
    f.request = submit_request(&f.body);
    let identity = f.prepare(Some(f.package.as_str()));
    assert!(
        f.push(&identity, &f.specs(Some(&f.package)))
            .status
            .success()
    );
    f.finish(&identity, Some(f.package.as_str()));
    assert!(f.submit().is_err());
    let state = ClientStateStore::open(&f.paths.state).unwrap();
    assert!(state.load_task_optional(f.body.task_id).unwrap().is_none());
    assert_eq!(
        ref_oid(
            f.cache().path(),
            &format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}")
        ),
        Some(f.package.to_string()),
        "frozen request remains replayable"
    );
    assert!(
        ref_oid(
            f.cache().path(),
            &format!("{SESSION_REF_PREFIX}{}", f.body.task_id)
        )
        .is_none(),
        "unpublished task pin leaked after submit rejection"
    );
}

#[test]
fn pre_record_rejection_preserves_a_preexisting_task_session_pin() {
    for same_package in [false, true] {
        let mut f = Fixture::new(true);
        f.body.worker = Some("removed-worker".into());
        f.request = submit_request(&f.body);
        let identity = f.prepare(Some(f.package.as_str()));
        assert!(
            f.push(&identity, &f.specs(Some(&f.package)))
                .status
                .success()
        );
        f.finish(&identity, Some(f.package.as_str()));
        let reference = format!("{SESSION_REF_PREFIX}{}", f.body.task_id);
        let oid = if same_package { &f.package } else { &f.base };
        assert!(
            git(
                f.cache().path(),
                &["update-ref".into(), reference.clone(), oid.to_string()],
                None
            )
            .status
            .success()
        );
        assert!(f.submit().is_err());
        assert_eq!(ref_oid(f.cache().path(), &reference), Some(oid.to_string()));
    }
}

#[test]
fn package_stream_verifies_both_refs_and_repins_for_real_submit() {
    let f = Fixture::new(true);
    let identity = f.prepare(Some(f.package.as_str()));
    assert_eq!(identity["session_oid"], f.package.as_str());
    let pushed = f.push(&identity, &f.specs(Some(&f.package)));
    assert!(
        pushed.status.success(),
        "{}",
        String::from_utf8_lossy(&pushed.stderr)
    );
    let receipt = f.finish(&identity, Some(f.package.as_str()));
    assert_eq!(receipt["oid"], f.base.as_str());
    assert_eq!(receipt["session_oid"], f.package.as_str());
    assert_eq!(
        ref_oid(
            f.cache().path(),
            &format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}")
        ),
        Some(f.package.to_string())
    );
    f.submit().unwrap();
    assert_eq!(
        ref_oid(
            f.cache().path(),
            &format!("{SESSION_REF_PREFIX}{}", f.body.task_id)
        ),
        Some(f.package.to_string())
    );
    let state = ClientStateStore::open(&f.paths.state).unwrap();
    assert_eq!(
        state
            .load_task(f.body.task_id)
            .unwrap()
            .meta()
            .session_import(),
        f.body.session_import.as_ref()
    );
    let checkout = ProjectRegistry::open(&f.paths.controller_state_root())
        .unwrap()
        .resolve(PROJECT, WORKTREE)
        .unwrap();
    assert_eq!(
        ref_oid(
            &checkout.join(".git"),
            &format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}")
        ),
        Some(f.package.to_string())
    );
    f.submit().unwrap();
}

#[test]
fn cached_objects_are_verified_and_pinned_without_preexisting_request_refs() {
    for session in [false, true] {
        let f = Fixture::new(session);
        let session_oid = session.then(|| f.package.as_str());
        let identity = f.prepare(session_oid);
        f.fetch_scratch(&f.base);
        if session {
            f.fetch_scratch(&f.package);
        }
        let request_ref = format!("refs/mac-worker/requests/{REQUEST}");
        let session_ref = format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}");
        assert!(ref_oid(f.cache().path(), &request_ref).is_none());
        assert!(ref_oid(f.cache().path(), &session_ref).is_none());

        let receipt = f.finish(&identity, session_oid);
        assert_eq!(receipt["oid"], f.base.as_str());
        assert_eq!(
            ref_oid(f.cache().path(), &request_ref),
            Some(f.base.to_string())
        );
        assert_eq!(
            ref_oid(f.cache().path(), &session_ref),
            session.then(|| f.package.to_string())
        );
        assert_eq!(f.finish(&identity, session_oid), receipt);
        f.submit().unwrap();
    }
}

#[test]
fn completed_receipt_replay_revalidates_the_owned_session_graph() {
    let f = Fixture::new(true);
    let identity = f.prepare(Some(f.package.as_str()));
    f.fetch_scratch(&f.base);
    f.fetch_scratch(&f.package);
    // Pre-seed exact refs to reach the completed-receipt path even before
    // the object-only receive regression is fixed.
    for (reference, oid) in [
        (format!("refs/mac-worker/requests/{REQUEST}"), &f.base),
        (format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}"), &f.package),
    ] {
        assert!(
            git(
                f.cache().path(),
                &["update-ref".into(), reference, oid.to_string()],
                None
            )
            .status
            .success()
        );
    }
    f.finish(&identity, Some(f.package.as_str()));
    let blob = git(
        f.cache().path(),
        &[
            "rev-parse".into(),
            format!("{}:{PACKAGE_SESSION_DIR}/{CODEX_ROLLOUT_FILE}", f.package),
        ],
        None,
    );
    assert!(blob.status.success());
    let blob = String::from_utf8(blob.stdout).unwrap();
    let blob = blob.trim();
    fs::remove_file(
        f.cache()
            .path()
            .join("objects")
            .join(&blob[..2])
            .join(&blob[2..]),
    )
    .unwrap();
    assert_eq!(
        ref_oid(
            f.cache().path(),
            &format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}")
        ),
        Some(f.package.to_string())
    );
    assert_failed(f.finish_output(&identity, Some(f.package.as_str())));
    assert!(f.submit().is_err());
    assert!(!f.paths.controller_project_root().exists());
}

#[test]
fn missing_session_object_cannot_finish_or_bind() {
    let f = Fixture::new(true);
    let identity = f.prepare(Some(f.package.as_str()));
    assert!(f.push(&identity, &f.specs(None)).status.success());
    assert_failed(f.finish_output(&identity, Some(f.package.as_str())));
    assert!(f.submit().is_err());
    assert!(
        ref_oid(
            f.cache().path(),
            &format!("{SESSION_REF_PREFIX}{}", f.body.task_id)
        )
        .is_none()
    );
    assert!(!f.paths.controller_project_root().exists());
}

#[test]
fn unfinished_stream_cannot_be_completed_by_envelope_only_submit() {
    let f = Fixture::new(true);
    let identity = f.prepare(Some(f.package.as_str()));
    assert!(
        f.push(&identity, &f.specs(Some(&f.package)))
            .status
            .success()
    );
    assert!(f.submit().is_err());
    assert!(!f.paths.controller_project_root().exists());
    f.finish(&identity, Some(f.package.as_str()));
    f.submit().unwrap();
}

#[test]
fn wrong_session_oid_and_unexpected_refs_are_atomically_rejected() {
    for extra in [false, true] {
        let f = Fixture::new(true);
        let identity = f.prepare(Some(f.package.as_str()));
        let mut specs = f.specs(Some(if extra { &f.package } else { &f.base }));
        if extra {
            specs.push(format!("{}:refs/heads/unexpected", f.base));
        }
        assert_failed(f.push(&identity, &specs));
        assert!(
            ref_oid(
                f.cache().path(),
                &format!("refs/mac-worker/requests/{REQUEST}")
            )
            .is_none()
        );
        assert_failed(f.finish_output(&identity, Some(f.package.as_str())));
    }
}

#[test]
fn zero_oid_binding_does_not_authorize_ref_deletion() {
    let f = Fixture::new(false);
    let zero = "0".repeat(40);
    let mut body = f.source_body(None);
    body["expected_oid"] = json!(zero);
    let identity = f.rpc("controller.transfer.source.prepare", body)["result"].clone();
    let cache = f.cache();
    let seeded = git(
        cache.path(),
        &[
            "fetch".into(),
            "--no-write-fetch-head".into(),
            f.repo.root().to_str().unwrap().into(),
            format!("{}:refs/mac-worker/requests/{REQUEST}", f.base),
        ],
        None,
    );
    assert!(seeded.status.success());
    assert_failed(f.push(&identity, &[format!(":refs/mac-worker/requests/{REQUEST}")]));
    assert_eq!(
        ref_oid(cache.path(), &format!("refs/mac-worker/requests/{REQUEST}")),
        Some(f.base.to_string())
    );
}

#[test]
fn corrupted_completed_refs_and_conflicting_task_pin_refuse_before_checkout() {
    for reference in [
        format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}"),
        format!("refs/mac-worker/requests/{REQUEST}"),
        format!("{SESSION_REF_PREFIX}018f0f4a6b5c7d8e9f00112233445566"),
    ] {
        let f = Fixture::new(true);
        let identity = f.prepare(Some(f.package.as_str()));
        assert!(
            f.push(&identity, &f.specs(Some(&f.package)))
                .status
                .success()
        );
        f.finish(&identity, Some(f.package.as_str()));
        let wrong = if reference.starts_with("refs/mac-worker/requests/") {
            &f.package
        } else {
            &f.base
        };
        assert!(
            git(
                f.cache().path(),
                &["update-ref".into(), reference, wrong.to_string()],
                None
            )
            .status
            .success()
        );
        assert!(f.submit().is_err());
        assert!(!f.paths.controller_project_root().exists());
        assert!(
            ClientStateStore::open(&f.paths.state)
                .unwrap()
                .list_tasks()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn no_session_hook_rejects_a_session_ref() {
    let f = Fixture::new(false);
    let identity = f.prepare(None);
    assert_failed(f.push(&identity, &f.specs(Some(&f.package))));
}

#[test]
fn replay_compares_session_identity_and_repairs_missing_receipt_refs() {
    let f = Fixture::new(true);
    let identity = f.prepare(Some(f.package.as_str()));
    assert_eq!(f.prepare(Some(f.package.as_str())), identity);
    assert_failed(f.rpc_output(
        "controller.transfer.source.prepare",
        f.source_body(Some(f.base.as_str())),
    ));
    assert_failed(f.rpc_output("controller.transfer.source.prepare", f.source_body(None)));
    assert!(
        f.push(&identity, &f.specs(Some(&f.package)))
            .status
            .success()
    );
    let receipt = f.finish(&identity, Some(f.package.as_str()));
    assert_eq!(f.finish(&identity, Some(f.package.as_str())), receipt);
    assert_failed(f.finish_output(&identity, Some(f.base.as_str())));
    assert_failed(f.finish_output(&identity, None));
    for (reference, oid) in [
        (format!("refs/mac-worker/requests/{REQUEST}"), &f.base),
        (format!("{REQUEST_SESSION_REF_PREFIX}{REQUEST}"), &f.package),
    ] {
        let deleted = git(
            f.cache().path(),
            &["update-ref".into(), "-d".into(), reference.clone()],
            None,
        );
        assert!(deleted.status.success());
        assert_eq!(f.finish(&identity, Some(f.package.as_str())), receipt);
        assert_eq!(ref_oid(f.cache().path(), &reference), Some(oid.to_string()));
    }
    f.submit().unwrap();
}

#[test]
fn no_session_digests_and_reply_bytes_remain_unchanged() {
    let f = Fixture::new(false);
    let identity = f.prepare(None);
    assert!(identity.get("session_oid").is_none());
    let expected = canonical_request_sha256(
        PROTOCOL_VERSION,
        "controller.transfer.source",
        &json!({
            "expected_oid": f.base.as_str(), "project_id": PROJECT, "worktree_id": WORKTREE
        }),
    )
    .unwrap();
    assert_eq!(source_digest(PROJECT, WORKTREE, &f.base).unwrap(), expected);
    let record: Value = serde_json::from_slice(
        &fs::read(
            f.paths
                .controller_state_root()
                .join(format!("src-{REQUEST}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(record["source_digest"], expected);
    assert!(record.get("session_oid").is_none());
    assert!(f.push(&identity, &f.specs(None)).status.success());
    let receipt = f.finish(&identity, None);
    assert_eq!(
        serde_json::to_vec(&receipt).unwrap(),
        serde_json::to_vec(&json!({
            "token": identity["token"], "request_id": REQUEST, "oid": f.base.as_str(),
            "request_ref": format!("refs/mac-worker/requests/{REQUEST}")
        }))
        .unwrap()
    );
    f.submit().unwrap();
}

#[test]
fn malformed_session_oids_are_rejected_before_source_record_creation() {
    for oid in ["", "abc", &"A".repeat(40), &"g".repeat(64), &"a".repeat(41)] {
        let f = Fixture::new(true);
        assert_failed(f.rpc_output(
            "controller.transfer.source.prepare",
            f.source_body(Some(oid)),
        ));
        assert!(
            !f.paths
                .controller_state_root()
                .join(format!("src-{REQUEST}.json"))
                .exists()
        );
    }
}

#[test]
fn session_digest_covers_the_oid_and_sha256_oids_are_accepted() {
    let f = Fixture::new(true);
    let oid = "a".repeat(64);
    let identity = f.prepare(Some(&oid));
    assert_eq!(identity["session_oid"], oid);
    let record: Value = serde_json::from_slice(
        &fs::read(
            f.paths
                .controller_state_root()
                .join(format!("src-{REQUEST}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    let expected = canonical_request_sha256(PROTOCOL_VERSION, "controller.transfer.source", &json!({
        "expected_oid": f.base.as_str(), "project_id": PROJECT, "worktree_id": WORKTREE, "session_oid": oid
    })).unwrap();
    assert_eq!(record["source_digest"], expected);
    assert_ne!(expected, source_digest(PROJECT, WORKTREE, &f.base).unwrap());
    // Keep the outer submit fingerprint a separate binding.
    assert_ne!(
        expected,
        RequestFingerprint::new(f.request.payload_sha256().to_owned())
            .unwrap()
            .as_str()
    );
}
