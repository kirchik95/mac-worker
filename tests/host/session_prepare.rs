use crate::support::GitRepo;
use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    core::{error::WorkerError, protocol::MemoryPressure},
    host::{
        job::{
            ClientId, CommandSpec, ExecutionScope, JobId, LeaseAcquireRequest,
            LeaseAcquireResponse, LeaseToken, RequestFingerprintMaterial,
        },
        lease::{AdmissionFacts, LeaseService},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        store::{HostGc, HostStore, TASK_RETENTION_MILLIS},
    },
    session::{
        MAX_PACKAGE_BYTES, MAX_PACKAGE_FILES, PackageFile, PackageSource, PlaceContext,
        PlacedSession, SESSION_REF_PREFIX, SessionAgent, SessionImportMeta, SessionPackage,
        SessionPlace, imported_session_id, materialize,
    },
    task::{
        model::{
            BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskSource, TaskState, TaskStatus,
        },
        store::{SessionBinding, TaskCloseRequest, TaskPrepareRequest, TaskStore},
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    cell::Cell,
    ffi::OsStr,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const OLD: u64 = 1_700_000_000_000;
fn task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}
fn job_id() -> JobId {
    JobId::new(Uuid::from_u128(2))
}

fn git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .current_dir(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn private_write(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
struct Fixture {
    _temp: TempDir,
    store: HostStore,
    home: PathBuf,
    base: BaseOid,
    oid: String,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        fs::create_dir(&home).unwrap();
        // nextest isolates environment-mutating fixtures in their own process.
        unsafe {
            std::env::set_var("HOME", &home);
        }
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let mirror = store.mirror(PROJECT).unwrap();
        let source = GitRepo::init();
        source.write("base.txt", b"base\n");
        source.commit_all("base");
        let base = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
        git(
            mirror.path(),
            &[
                "fetch",
                source.root().to_str().unwrap(),
                "HEAD:refs/mac-worker/bases/00000000000000000000000000000001",
            ],
        );
        let package = SessionPackage::build(
            PackageSource {
                agent: SessionAgent::Codex,
                source_session_id: "11111111-1111-4111-8111-111111111111".into(),
                source_agent_version: "0.160.0".into(),
                source_cwd_relative: String::new(),
                scrubbed: 0,
            },
            vec![PackageFile {
                path: "rollout.jsonl".into(),
                bytes: b"{\"cwd\":\"@@MW_WORKSPACE@@\",\"id\":\"@@MW_SESSION@@\"}\n".to_vec(),
            }],
        )
        .unwrap();
        let repo = GitRepo::init();
        repo.write("manifest.json", &package.manifest_json());
        repo.write("session/rollout.jsonl", &package.files()[0].bytes);
        repo.commit_all("package");
        let oid = git(repo.root(), &["rev-parse", "HEAD"]);
        git(
            mirror.path(),
            &[
                "fetch",
                repo.root().to_str().unwrap(),
                &format!("HEAD:{SESSION_REF_PREFIX}{}", task_id()),
            ],
        );
        Self {
            _temp: temp,
            store,
            home,
            base,
            oid,
        }
    }
    fn meta(&self, import: bool, profile: Option<&str>) -> TaskMeta {
        TaskMeta::new(TaskMetaInput {
            session_import: import.then(|| {
                SessionImportMeta::new(SessionAgent::Codex, &self.oid, "0.160.0").unwrap()
            }),
            task_id: task_id(),
            run_id: None,
            project_id: PROJECT.into(),
            worktree_id: WORKTREE.into(),
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
            base_oid: self.base.clone(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: profile.map(str::to_owned),
            git_identity: GitIdentity::new("Test", "test@example.test").unwrap(),
            title: None,
            prompt: "continue fixture".into(),
            created_at_millis: OLD,
        })
        .unwrap()
    }
    fn lease(&self) {
        let request = LeaseAcquireRequest::new(
            RequestFingerprintMaterial::new(
                job_id(),
                ClientId::new(Uuid::from_u128(3)),
                LeaseToken::new(Uuid::from_u128(4)),
                100,
                "mini-1".into(),
                PROJECT.into(),
                WORKTREE.into(),
                "c".repeat(64),
                String::new(),
                30_000,
                "heavy".into(),
                CommandSpec::shell("true".into()).unwrap(),
            )
            .unwrap(),
        )
        .with_execution_scope(ExecutionScope::task(task_id()));
        assert!(matches!(
            LeaseService::new(&self.store)
                .acquire(
                    &request,
                    &AdmissionFacts {
                        free_disk_bytes: 100 << 30,
                        total_disk_bytes: 200 << 30,
                        memory_pressure: MemoryPressure::Normal,
                        swap_used_bytes: Some(0),
                    },
                    100
                )
                .unwrap(),
            LeaseAcquireResponse::Acquired { .. }
        ));
    }
    fn prepare(&self, meta: TaskMeta) -> Result<(), WorkerError> {
        let admission = self.store.admission_lock(job_id()).unwrap();
        let guard = self
            .store
            .transfer_lock_after(&admission, job_id())
            .unwrap();
        TaskStore::new(&self.store, &SystemProcessRunner)
            .prepare(&TaskPrepareRequest::new(meta, job_id(), "mini-1"), &guard)
            .map(|_| ())
    }
    fn prepare_fake(
        &self,
        meta: TaskMeta,
        place: &dyn SessionPlace,
        runner: &dyn ProcessRunner,
    ) -> Result<(), WorkerError> {
        let admission = self.store.admission_lock(job_id()).unwrap();
        let guard = self
            .store
            .transfer_lock_after(&admission, job_id())
            .unwrap();
        TaskStore::new(&self.store, runner)
            .prepare_with_session_place(
                &TaskPrepareRequest::new(meta, job_id(), "mini-1"),
                &guard,
                place,
            )
            .map(|_| ())
    }
    fn profile(&self, text: &str) {
        let dir = self.home.join(".config/mac-worker/env");
        fs::create_dir_all(&dir).unwrap();
        private_write(&dir.join("test.env"), text.as_bytes());
    }
    fn receipt(&self) -> Value {
        serde_json::from_slice(&fs::read(self.task().join("session-import.json")).unwrap()).unwrap()
    }
    fn planned(&self, placed_at_millis: u64, root: &Path) {
        let receipt = Receipt {
            schema: 1,
            stage: "planned",
            package_oid: self.oid.clone(),
            session_id: imported_session_id(&task_id()),
            agent: SessionAgent::Codex,
            placed_at_millis,
            store_root: root.canonicalize().unwrap().to_str().unwrap().into(),
            primary_relative: String::new(),
            files: vec![],
        };
        private_write(
            &self.task().join("session-import.json"),
            &serde_json::to_vec(&receipt).unwrap(),
        );
    }
    fn mirror(&self) -> PathBuf {
        self.store.mirror(PROJECT).unwrap().path().to_path_buf()
    }
    fn reference(&self) -> String {
        format!("{SESSION_REF_PREFIX}{}", task_id())
    }
    fn task(&self) -> PathBuf {
        self.store.task_dir(PROJECT, task_id()).unwrap()
    }
    fn ref_exists(&self, name: &str) -> bool {
        Command::new("/usr/bin/git")
            .current_dir(self.mirror())
            .args(["show-ref", "--verify", "--quiet", name])
            .status()
            .unwrap()
            .success()
    }
    fn record(&self, state: TaskState) {
        let dir = self.task();
        fs::create_dir_all(&dir).unwrap();
        fs::set_permissions(dir.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let meta = self.meta(false, None);
        private_write(&dir.join("meta.json"), &serde_json::to_vec(&meta).unwrap());
        let status = TaskStatus::new(
            state,
            None,
            None,
            false,
            Some(self.base.clone()),
            None,
            vec![],
            vec![],
            None,
            vec![],
            OLD,
        )
        .unwrap();
        private_write(
            &dir.join("status.json"),
            &serde_json::to_vec(&status).unwrap(),
        );
    }
}
#[derive(Serialize)]
struct Receipt<'a> {
    schema: u32,
    stage: &'a str,
    package_oid: String,
    session_id: String,
    agent: SessionAgent,
    placed_at_millis: u64,
    store_root: String,
    primary_relative: String,
    files: Vec<String>,
}
#[derive(Default)]
struct FakePlace {
    calls: Cell<usize>,
    stop: u8,
}
impl SessionPlace for FakePlace {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Codex
    }
    fn place(
        &self,
        package: &SessionPackage,
        cx: &PlaceContext<'_>,
    ) -> Result<PlacedSession, WorkerError> {
        self.calls.set(self.calls.get() + 1);
        if self.stop == 1 {
            return Err(WorkerError::task(
                "SESSION_PLACEMENT_FAILED",
                "fixture before write",
            ));
        }
        let relative = format!("sessions/{}-{}.jsonl", cx.placed_at_millis, cx.session_id);
        let bytes = materialize(
            &package.files()[0].bytes,
            cx.workspace.to_str().unwrap(),
            cx.session_id,
        )?;
        cx.store.write_file(&relative, &bytes)?;
        if self.stop == 2 {
            return Err(WorkerError::task(
                "SESSION_PLACEMENT_FAILED",
                "fixture after write",
            ));
        }
        Ok(PlacedSession {
            primary_relative: relative.clone(),
            files: vec![relative],
        })
    }
}
struct GitFault {
    tree: Option<Vec<u8>>,
    delete: bool,
    blob_reads: AtomicUsize,
}
impl ProcessRunner for GitFault {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let has = |arg: &str| request.args.iter().any(|value| value == OsStr::new(arg));
        if self.delete && has("update-ref") && has("-d") {
            return Err(WorkerError::task(
                "SESSION_PLACEMENT_FAILED",
                "fixture before delete",
            ));
        }
        if let Some(tree) = &self.tree
            && has("ls-tree")
        {
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: tree.clone(),
                stderr: vec![],
            });
        }
        if has("cat-file") && has("blob") {
            self.blob_reads.fetch_add(1, Ordering::Relaxed);
        }
        SystemProcessRunner.run(request)
    }
}
fn placement_failed(result: Result<(), WorkerError>) {
    assert!(
        matches!(
            result,
            Err(WorkerError::Task {
                code: "SESSION_PLACEMENT_FAILED",
                ..
            })
        ),
        "{result:?}"
    );
}
#[test]
fn happy_path_places_binds_completes_and_deletes() {
    let f = Fixture::new();
    f.lease();
    let place = FakePlace::default();
    f.prepare_fake(f.meta(true, None), &place, &SystemProcessRunner)
        .unwrap();
    let receipt = f.receipt();
    assert_eq!(receipt["schema"], 1);
    assert_eq!(receipt["stage"], "complete");
    assert_eq!(receipt["package_oid"], f.oid);
    assert_eq!(receipt["session_id"], imported_session_id(&task_id()));
    let binding = TaskStore::new(&f.store, &SystemProcessRunner)
        .session(PROJECT, task_id())
        .unwrap()
        .unwrap();
    assert_eq!(binding.agent(), AgentKind::Codex);
    assert_eq!(binding.session_ref(), imported_session_id(&task_id()));
    assert!(!f.ref_exists(&f.reference()));
    let path = f
        .home
        .join(".codex")
        .join(receipt["primary_relative"].as_str().unwrap());
    let content: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(content["id"], binding.session_ref());
    assert_eq!(
        content["cwd"],
        f.task()
            .join("workspace")
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
    );
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(f.home.join(".codex"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(f.task().join("session-import.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
#[test]
fn retry_before_or_after_file_write_reuses_frozen_time() {
    for stop in [1, 2] {
        let f = Fixture::new();
        f.lease();
        placement_failed(f.prepare_fake(
            f.meta(true, None),
            &FakePlace {
                stop,
                ..Default::default()
            },
            &SystemProcessRunner,
        ));
        let planned = f.receipt();
        assert_eq!(planned["stage"], "planned");
        let place = FakePlace::default();
        f.prepare_fake(f.meta(true, None), &place, &SystemProcessRunner)
            .unwrap();
        assert_eq!(f.receipt()["placed_at_millis"], planned["placed_at_millis"]);
        assert_eq!(
            fs::read_dir(f.home.join(".codex/sessions"))
                .unwrap()
                .count(),
            1
        );
        assert_eq!(place.calls.get(), 1);
    }
}
#[test]
fn retry_after_workspace_status_receipt_and_binding() {
    let f = Fixture::new();
    f.lease();
    placement_failed(f.prepare_fake(
        f.meta(true, None),
        &FakePlace {
            stop: 1,
            ..Default::default()
        },
        &SystemProcessRunner,
    ));
    assert!(f.task().join("workspace").exists());
    assert!(f.task().join("status.json").exists());
    // Crash after workspace creation, before status publication/planning.
    // Both records are reconstructed without touching the prepared workspace.
    fs::remove_file(f.task().join("status.json")).unwrap();
    fs::remove_file(f.task().join("session-import.json")).unwrap();
    placement_failed(f.prepare_fake(
        f.meta(true, None),
        &FakePlace {
            stop: 1,
            ..Default::default()
        },
        &SystemProcessRunner,
    ));
    // Crash after planned receipt, with a frozen timestamp on a past date.
    f.planned(OLD, &f.home.join(".codex"));
    placement_failed(f.prepare_fake(
        f.meta(true, None),
        &FakePlace {
            stop: 2,
            ..Default::default()
        },
        &SystemProcessRunner,
    ));
    // Crash after binding but before recording completion.
    private_write(
        &f.task().join("session.json"),
        &serde_json::to_vec(
            &SessionBinding::new(AgentKind::Codex, imported_session_id(&task_id()), OLD).unwrap(),
        )
        .unwrap(),
    );
    f.prepare_fake(
        f.meta(true, None),
        &FakePlace::default(),
        &SystemProcessRunner,
    )
    .unwrap();
    assert_eq!(f.receipt()["placed_at_millis"], OLD);
    assert!(
        f.store
            .task_status(PROJECT, task_id())
            .unwrap()
            .session_present()
    );
    assert!(
        f.home
            .join(".codex/sessions")
            .join(format!("{OLD}-{}.jsonl", imported_session_id(&task_id())))
            .exists()
    );
}
#[test]
fn complete_retry_after_delete_failure_never_rewrites_appended_files() {
    let f = Fixture::new();
    f.lease();
    let place = FakePlace::default();
    let runner = GitFault {
        tree: None,
        delete: true,
        blob_reads: AtomicUsize::new(0),
    };
    placement_failed(f.prepare_fake(f.meta(true, None), &place, &runner));
    let receipt = f.receipt();
    assert_eq!(receipt["stage"], "complete");
    assert!(f.ref_exists(&f.reference()));
    let path = f
        .home
        .join(".codex")
        .join(receipt["primary_relative"].as_str().unwrap());
    let mut appended = fs::read(&path).unwrap();
    appended.extend_from_slice(b"{\"agent_appended\":true}\n");
    fs::write(&path, &appended).unwrap();
    // Also repair the crash boundary after session.json was written but the
    // status flag was not, or after its binding record was lost.
    fs::remove_file(f.task().join("session.json")).unwrap();
    let never = FakePlace {
        stop: 1,
        ..Default::default()
    };
    f.prepare_fake(f.meta(true, None), &never, &SystemProcessRunner)
        .unwrap();
    f.prepare_fake(f.meta(true, None), &never, &SystemProcessRunner)
        .unwrap();
    assert_eq!(never.calls.get(), 0);
    assert_eq!(fs::read(&path).unwrap(), appended);
    assert!(!f.ref_exists(&f.reference()));
}
#[test]
fn complete_retry_does_not_open_missing_native_root_or_profile() {
    let f = Fixture::new();
    f.lease();
    let root = f.home.join("profile-codex");
    f.profile(&format!("CODEX_HOME={}\n", root.display()));
    f.prepare_fake(
        f.meta(true, Some("test")),
        &FakePlace::default(),
        &SystemProcessRunner,
    )
    .unwrap();
    fs::remove_dir_all(root).unwrap();
    fs::remove_file(f.home.join(".config/mac-worker/env/test.env")).unwrap();
    f.prepare(f.meta(true, Some("test"))).unwrap();
}
#[test]
fn profile_override_is_used_and_invalid_relative_override_is_refused() {
    let f = Fixture::new();
    f.lease();
    f.profile("CODEX_HOME=relative/store\n");
    placement_failed(f.prepare_fake(
        f.meta(true, Some("test")),
        &FakePlace::default(),
        &SystemProcessRunner,
    ));
    assert!(!f.home.join(".codex").exists());
    let root = f.home.join("profile-codex");
    f.profile(&format!("CODEX_HOME={}\n", root.display()));
    f.prepare_fake(
        f.meta(true, Some("test")),
        &FakePlace::default(),
        &SystemProcessRunner,
    )
    .unwrap();
    assert_eq!(
        f.receipt()["store_root"],
        root.canonicalize().unwrap().to_str().unwrap()
    );
    assert!(
        root.join(f.receipt()["primary_relative"].as_str().unwrap())
            .exists()
    );
    assert!(!f.home.join(".codex").exists());
}
#[test]
fn planned_retry_refuses_retargeted_store_root_symlink() {
    let f = Fixture::new();
    f.lease();
    let first = f.home.join("first");
    let second = f.home.join("second");
    let link = f.home.join("link");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    std::os::unix::fs::symlink(&first, &link).unwrap();
    f.profile(&format!("CODEX_HOME={}\n", link.display()));
    placement_failed(f.prepare_fake(
        f.meta(true, Some("test")),
        &FakePlace {
            stop: 1,
            ..Default::default()
        },
        &SystemProcessRunner,
    ));
    fs::remove_file(&link).unwrap();
    std::os::unix::fs::symlink(&second, &link).unwrap();
    let place = FakePlace::default();
    placement_failed(f.prepare_fake(f.meta(true, Some("test")), &place, &SystemProcessRunner));
    assert_eq!(place.calls.get(), 0);
    assert!(!second.join("sessions").exists());
}
#[test]
fn planned_retry_refuses_changed_store_root() {
    let f = Fixture::new();
    f.lease();
    let first = f.home.join("first");
    f.profile(&format!("CODEX_HOME={}\n", first.display()));
    placement_failed(f.prepare_fake(
        f.meta(true, Some("test")),
        &FakePlace {
            stop: 1,
            ..Default::default()
        },
        &SystemProcessRunner,
    ));
    let second = f.home.join("second");
    f.profile(&format!("CODEX_HOME={}\n", second.display()));
    let place = FakePlace::default();
    placement_failed(f.prepare_fake(f.meta(true, Some("test")), &place, &SystemProcessRunner));
    assert_eq!(place.calls.get(), 0);
    assert!(!second.exists());
}
#[test]
fn malformed_receipts_and_wrong_binding_are_refused_without_native_writes() {
    let f = Fixture::new();
    f.lease();
    placement_failed(f.prepare_fake(
        f.meta(true, None),
        &FakePlace {
            stop: 1,
            ..Default::default()
        },
        &SystemProcessRunner,
    ));
    let valid = fs::read(f.task().join("session-import.json")).unwrap();
    let valid_text = std::str::from_utf8(&valid).unwrap();
    let original: Value = serde_json::from_slice(&valid).unwrap();
    for (key, value) in [
        ("schema", json!(2)),
        ("stage", json!("other")),
        ("placed_at_millis", json!(0)),
        ("extra", json!(true)),
        ("store_root", json!("relative")),
        ("session_id", json!("wrong")),
        ("files", json!(["../escape"])),
    ] {
        // Preserve field order so these prove validating reads rather than
        // merely triggering the task store's canonical-byte comparison.
        let altered = if key == "extra" {
            format!("{},\"extra\":true}}", valid_text.strip_suffix('}').unwrap())
        } else {
            valid_text.replacen(
                &format!("\"{key}\":{}", original[key]),
                &format!("\"{key}\":{value}"),
                1,
            )
        };
        assert_ne!(altered.as_bytes(), valid);
        private_write(&f.task().join("session-import.json"), altered.as_bytes());
        let place = FakePlace::default();
        placement_failed(f.prepare_fake(f.meta(true, None), &place, &SystemProcessRunner));
        assert_eq!(place.calls.get(), 0);
    }
    private_write(&f.task().join("session-import.json"), &valid);
    f.prepare_fake(
        f.meta(true, None),
        &FakePlace::default(),
        &SystemProcessRunner,
    )
    .unwrap();
    private_write(
        &f.task().join("session.json"),
        &serde_json::to_vec(
            &SessionBinding::new(
                AgentKind::Codex,
                "22222222-2222-4222-8222-222222222222",
                OLD,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    placement_failed(f.prepare(f.meta(true, None)));
}
#[test]
fn manifest_hash_mismatch_is_refused_before_placement() {
    let mut f = Fixture::new();
    f.lease();
    let mut manifest: Value = serde_json::from_str(&git(
        &f.mirror(),
        &["show", &format!("{}:manifest.json", f.oid)],
    ))
    .unwrap();
    manifest["files"][0]["sha256"] = json!("0".repeat(64));
    let repo = GitRepo::init();
    repo.write("manifest.json", &serde_json::to_vec(&manifest).unwrap());
    repo.write("session/rollout.jsonl", b"{}\n");
    repo.commit_all("invalid package");
    f.oid = git(repo.root(), &["rev-parse", "HEAD"]);
    git(
        &f.mirror(),
        &[
            "fetch",
            repo.root().to_str().unwrap(),
            &format!("+HEAD:{}", f.reference()),
        ],
    );
    let place = FakePlace::default();
    placement_failed(f.prepare_fake(f.meta(true, None), &place, &SystemProcessRunner));
    assert_eq!(place.calls.get(), 0);
    assert_eq!(f.receipt()["stage"], "planned");
    assert!(f.ref_exists(&f.reference()));
}
#[test]
fn tree_caps_and_non_package_entries_are_checked_before_blob_reads() {
    let f = Fixture::new();
    f.lease();
    let header = |size: u64, path: &str| format!("100644 blob {} {size}\t{path}\0", f.oid);
    let mut too_many = header(0, "manifest.json");
    for i in 0..=MAX_PACKAGE_FILES {
        too_many.push_str(&header(0, &format!("session/{i}")));
    }
    for tree in [
        header(MAX_PACKAGE_BYTES + 1, "manifest.json"),
        too_many,
        header(1, "outside"),
        format!("120000 blob {} 1\tsession/link\0", f.oid),
        header(1, "session/../escape"),
    ] {
        let runner = GitFault {
            tree: Some(tree.into_bytes()),
            delete: false,
            blob_reads: AtomicUsize::new(0),
        };
        let place = FakePlace::default();
        placement_failed(f.prepare_fake(f.meta(true, None), &place, &runner));
        assert_eq!(runner.blob_reads.load(Ordering::Relaxed), 0);
        assert_eq!(place.calls.get(), 0);
    }
}
#[test]
fn gc_newly_pushed_old_package_is_still_young() {
    let f = Fixture::new();
    let tree = git(&f.mirror(), &["rev-parse", &format!("{}^{{tree}}", f.oid)]);
    let output = Command::new("/usr/bin/git")
        .current_dir(f.mirror())
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .args([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=test@example.test",
            "commit-tree",
            &tree,
            "-m",
            "old package",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let ancient = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    git(&f.mirror(), &["update-ref", &f.reference(), &ancient]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
    // Packing does not prove a fresh ref is old either.
    git(&f.mirror(), &["pack-refs", "--all", "--prune"]);
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
}
#[test]
fn gc_absent_record_with_live_execution_lease_is_protected() {
    let f = Fixture::new();
    f.lease();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(now + TASK_RETENTION_MILLIS * 2)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
}
#[test]
fn discard_deletes_with_profile_environment_and_keeps_cross_reference_protection() {
    struct DeleteRunner {
        deletes: AtomicUsize,
        root: PathBuf,
    }
    impl ProcessRunner for DeleteRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == OsStr::new("/bin/zsh") {
                assert!(
                    request
                        .environment
                        .iter()
                        .any(|(key, value)| key == "CODEX_HOME" && value == self.root.as_os_str())
                );
                self.deletes.fetch_add(1, Ordering::Relaxed);
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: vec![],
                    stderr: vec![],
                });
            }
            SystemProcessRunner.run(request)
        }
    }
    let f = Fixture::new();
    f.record(TaskState::Open);
    let root = f.home.join("custom");
    f.profile(&format!("CODEX_HOME={}\n", root.display()));
    private_write(
        &f.task().join("meta.json"),
        &serde_json::to_vec(&f.meta(false, Some("test"))).unwrap(),
    );
    let binding =
        SessionBinding::new(AgentKind::Codex, imported_session_id(&task_id()), OLD).unwrap();
    TaskStore::new(&f.store, &SystemProcessRunner)
        .bind_session(PROJECT, task_id(), binding.clone())
        .unwrap();
    let other = f
        .task()
        .parent()
        .unwrap()
        .join(TaskId::new(Uuid::from_u128(10)).to_string());
    fs::create_dir(&other).unwrap();
    fs::set_permissions(&other, fs::Permissions::from_mode(0o700)).unwrap();
    private_write(
        &other.join("session.json"),
        &serde_json::to_vec(&binding).unwrap(),
    );
    let runner = DeleteRunner {
        deletes: AtomicUsize::new(0),
        root,
    };
    TaskStore::new(&f.store, &runner)
        .close(&TaskCloseRequest::new(PROJECT, task_id(), true))
        .unwrap();
    assert_eq!(runner.deletes.load(Ordering::Relaxed), 0);
    fs::remove_dir_all(&other).unwrap();
    let closed = TaskStore::new(&f.store, &runner)
        .close(&TaskCloseRequest::new(PROJECT, task_id(), true))
        .unwrap();
    assert!(closed.warnings().is_empty());
    assert_eq!(runner.deletes.load(Ordering::Relaxed), 1);
}
#[test]
fn missing_ref_fails_with_planned_receipt() {
    let f = Fixture::new();
    f.lease();
    git(&f.mirror(), &["update-ref", "-d", &f.reference()]);
    placement_failed(f.prepare(f.meta(true, None)));
    let receipt: Value =
        serde_json::from_slice(&fs::read(f.task().join("session-import.json")).unwrap()).unwrap();
    assert_eq!(receipt["stage"], "planned");
}
#[test]
fn oid_mismatch_is_refused() {
    let f = Fixture::new();
    f.lease();
    git(
        &f.mirror(),
        &["update-ref", &f.reference(), f.base.as_str()],
    );
    placement_failed(f.prepare(f.meta(true, None)));
    assert!(f.ref_exists(&f.reference()));
}
#[test]
fn no_import_preserves_records_and_never_creates_native_store() {
    let f = Fixture::new();
    f.lease();
    f.prepare(f.meta(false, None)).unwrap();
    let meta = fs::read(f.task().join("meta.json")).unwrap();
    let status = fs::read(f.task().join("status.json")).unwrap();
    f.prepare(f.meta(false, None)).unwrap();
    assert_eq!(fs::read(f.task().join("meta.json")).unwrap(), meta);
    assert_eq!(fs::read(f.task().join("status.json")).unwrap(), status);
    assert!(!f.task().join("session-import.json").exists());
    assert!(!f.task().join("session.json").exists());
    assert!(!f.home.join(".codex").exists());
    assert!(f.ref_exists(&f.reference()));
}
#[test]
fn gc_terminal_task_with_live_lease_keeps_session_ref() {
    let f = Fixture::new();
    f.lease();
    f.record(TaskState::Closed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(now + TASK_RETENTION_MILLIS * 2)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
}
#[test]
fn gc_terminal_session_ref_is_removed_without_branch_retention() {
    let f = Fixture::new();
    f.record(TaskState::Closed);
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(OLD + 1000)
        .unwrap();
    assert!(!f.ref_exists(&f.reference()));
}
#[test]
fn gc_absent_old_is_removed_but_absent_young_and_malformed_are_protected() {
    let f = Fixture::new();
    git(
        &f.mirror(),
        &["update-ref", "refs/mac-worker/sessions/not-a-task", &f.oid],
    );
    let young = HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(OLD)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
    assert!(
        young
            .warnings()
            .iter()
            .any(|warning| warning.contains("session ref"))
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(now + TASK_RETENTION_MILLIS + 1000)
        .unwrap();
    assert!(!f.ref_exists(&f.reference()));
    assert!(f.ref_exists("refs/mac-worker/sessions/not-a-task"));
}
#[test]
fn gc_live_and_uncertain_records_are_protected() {
    let f = Fixture::new();
    f.record(TaskState::Open);
    HostGc::new(&f.store, &SystemProcessRunner)
        .preview_at(OLD + TASK_RETENTION_MILLIS * 10)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
    private_write(
        &f.task().join("status.json"),
        &serde_json::to_vec(&json!({"broken": true})).unwrap(),
    );
    HostGc::new(&f.store, &SystemProcessRunner)
        .apply_at(OLD + TASK_RETENTION_MILLIS * 10)
        .unwrap();
    assert!(f.ref_exists(&f.reference()));
}
