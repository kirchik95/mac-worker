#[allow(dead_code)]
mod support;

use std::{
    fs,
    io::Write,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, PromptDelivery, TurnLaunch, TurnLimits},
    error::WorkerError,
    git_transport::{GitTransport, OBJECT_STORE_SYNC_RECEIPT},
    host_store::{HostGc, HostStore, HostStoreWritePoint},
    job::{
        ClientId, CommandSpec, JobId, JobState, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseRecord, LeaseToken, RequestFingerprintMaterial, SubmitRequest,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService, SlotState},
    outbox::{
        DELIVERY_UNREADABLE, DeliveryCommit, OUTBOX_BUSY, OUTBOX_WORKER_REQUIRED, OriginOutbox,
        OutboxActivation, OutboxLauncher, due_index_reads_for, task_directory_scans_for,
    },
    process::SystemProcessRunner,
    protocol::MemoryPressure,
    remote_snapshot::RemoteSnapshotService,
    supervisor::{Supervisor, SystemProcessInspector},
    task::{
        BaseOid, BranchName, ClosePolicy, DeliveryState, GitIdentity, PublishMode, PushTarget,
        TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome, TaskSource, TaskState,
        TurnTerminal,
    },
    task_store::{TaskCloseRequest, TaskPrepareRequest, TaskStore},
    turn::{TaskTurnRequest, TurnMaterial},
};
use sha2::{Digest, Sha256};
use support::GitRepo;
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_PROJECT_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SUPERVISOR_JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const SUPERVISOR_CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const SUPERVISOR_LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";

struct InactiveLauncher;

impl OutboxLauncher for InactiveLauncher {
    fn ensure_watch(&self, host_root: &Path) -> Result<OutboxActivation, WorkerError> {
        assert!(host_root.is_absolute());
        Ok(OutboxActivation::Inactive)
    }
}

struct FalseActiveLauncher;

impl OutboxLauncher for FalseActiveLauncher {
    fn ensure_watch(&self, host_root: &Path) -> Result<OutboxActivation, WorkerError> {
        assert!(host_root.is_absolute());
        Ok(OutboxActivation::Active)
    }
}

struct InlineSupervisorLauncher {
    store: HostStore,
}

impl SupervisorLauncher for InlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: JobId,
        guard: mac_worker::host_store::SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        let helper = PathBuf::from(env!("CARGO_BIN_EXE_worker"));
        Supervisor::new(&self.store, &inspector)
            .with_prepare_turn_helper(helper)
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

struct WatchChild {
    child: Option<Child>,
    log: PathBuf,
}

impl WatchChild {
    fn spawn(host_root: &Path, home: &Path, data: &Path, tmp: &Path, log: PathBuf) -> Self {
        assert!(host_root.is_absolute());
        fs::create_dir_all(home).unwrap();
        fs::create_dir_all(data).unwrap();
        fs::create_dir_all(tmp).unwrap();
        if let Some(parent) = log.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let stderr = fs::File::create(&log).unwrap();
        let stdout = fs::File::create(log.with_extension("stdout")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
        command
            .env_clear()
            .env("HOME", home)
            .env("XDG_DATA_HOME", data)
            .env("TMPDIR", tmp)
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "host",
                "outbox",
                "--watch",
                "--host-root",
                host_root.to_str().expect("utf-8 host root"),
            ])
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        // SAFETY: setpgid is async-signal-safe and runs in the child between fork and exec.
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn().expect("spawn worker host outbox --watch");
        Self {
            child: Some(child),
            log,
        }
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for WatchChild {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let pid = child.id() as i32;
        if pid > 0 {
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = child.wait();
    }
}

struct DetachedWatch {
    host_root: PathBuf,
}

impl DetachedWatch {
    fn spawn_via_wake(
        host_root: &Path,
        home: &Path,
        data: &Path,
        tmp: &Path,
        log: PathBuf,
    ) -> Self {
        assert!(host_root.is_absolute());
        fs::create_dir_all(home).unwrap();
        fs::create_dir_all(data).unwrap();
        fs::create_dir_all(tmp).unwrap();
        if let Some(parent) = log.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let stderr = fs::File::create(&log).unwrap();
        let stdout = fs::File::create(log.with_extension("stdout")).unwrap();
        let status = Command::new(env!("CARGO_BIN_EXE_worker"))
            .env_clear()
            .env("HOME", home)
            .env("XDG_DATA_HOME", data)
            .env("TMPDIR", tmp)
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "host",
                "outbox",
                "--wake",
                "--host-root",
                host_root.to_str().expect("utf-8 host root"),
            ])
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .status()
            .expect("worker host outbox --wake");
        assert!(
            status.success(),
            "production --wake parent failed: {}",
            fs::read_to_string(&log).unwrap_or_default()
        );
        Self {
            host_root: host_root.to_path_buf(),
        }
    }
}

impl Drop for DetachedWatch {
    fn drop(&mut self) {
        if let Some(pid) = outbox_worker_pid(&self.host_root) {
            let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
            let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
            let mut status = 0;
            let _ = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        }
    }
}

struct ModeRestore {
    path: PathBuf,
    mode: u32,
}

impl Drop for ModeRestore {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(self.mode));
    }
}

fn task_id(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn turn_id(value: u128) -> JobId {
    JobId::new(Uuid::from_u128(value))
}

fn git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .current_dir(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .expect("git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn ref_exists(path: &Path, reference: &str) -> bool {
    Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .args(["show-ref", "--verify", "--quiet", reference])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .status()
        .expect("show-ref")
        .success()
}

fn set_ref(path: &Path, reference: &str, oid: &str) {
    let output = Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .args(["update-ref", reference, oid])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("update-ref");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn origin_bare() -> (TempDir, PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("origin.git");
    fs::create_dir_all(&path).unwrap();
    assert!(
        Command::new("/usr/bin/git")
            .args(["init", "--bare"])
            .arg(&path)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .status()
            .unwrap()
            .success()
    );
    let hook = path.join("hooks/update");
    fs::write(
        &hook,
        "#!/bin/sh\nif [ -f reject ]; then echo denied >&2; exit 1; fi\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o700)).unwrap();
    (directory, path)
}

fn admissions() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 200 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn lease_request(job: JobId) -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job,
            ClientId::new(Uuid::from_u128(20)),
            LeaseToken::new(Uuid::from_u128(30 + job.as_uuid().as_u128())),
            1,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            "c".repeat(64),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap(),
    )
}

fn acquire_lease(store: &HostStore, job: JobId) -> LeaseAcquireRequest {
    let request = lease_request(job);
    LeaseService::new(store)
        .acquire(&request, &admissions(), 1)
        .unwrap();
    request
}

fn release_lease(store: &HostStore, request: &LeaseAcquireRequest) {
    store.record_abandoned(request, 2).unwrap();
    let lease = LeaseService::new(store).load().unwrap().unwrap();
    let receipt = store.cleanup_job_owned(&lease).unwrap();
    LeaseService::new(store)
        .release_after_cleanup(&lease, &receipt)
        .unwrap();
}

fn fixture() -> (
    TempDir,
    HostStore,
    GitRepo,
    TempDir,
    PathBuf,
    String,
    BaseOid,
) {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"one\n");
    source.commit_all("one");
    let oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    let (origin_dir, origin) = origin_bare();
    let origin_url = file_url(&origin);
    (temp, store, source, origin_dir, origin, origin_url, oid)
}

fn valid_manifest_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"","#,
            r#""entries":[{{"path":"payload.txt","kind":"file","mode":420,"size":7,"#,
            r#""sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","#,
            r#""symlink_target":null}}],"tracked_deletions":[]}}"#
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
    )
    .into_bytes()
}

fn prepared_supervisor_job(
    store: &HostStore,
    command: CommandSpec,
) -> (mac_worker::job::LeaseRecord, SubmitRequest) {
    let manifest = valid_manifest_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let request = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            SUPERVISOR_JOB_ID.parse().unwrap(),
            SUPERVISOR_CLIENT_ID.parse().unwrap(),
            SUPERVISOR_LEASE_TOKEN.parse().unwrap(),
            3,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest.clone(),
            String::new(),
            30_000,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    let lease = match LeaseService::new(store)
        .acquire(&request, &admissions(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(incoming.join("tree")).unwrap();
    for private in [
        incoming.parent().unwrap().parent().unwrap(),
        incoming.parent().unwrap(),
        incoming.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(incoming.join("manifest.json"), &manifest).unwrap();
    fs::write(incoming.join("tree/payload.txt"), b"payload").unwrap();
    fs::set_permissions(
        incoming.join("manifest.json"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/payload.txt"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(incoming.join("tree"), fs::Permissions::from_mode(0o555)).unwrap();
    RemoteSnapshotService::new(store)
        .verify_and_promote_at(&lease, &digest, 2)
        .unwrap();
    (lease, SubmitRequest::new(request.material().clone()))
}

fn task_meta(task: TaskId, base_oid: BaseOid) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: task,
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid,
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "outbox fixture".into(),
        created_at_millis: 1,
    })
    .unwrap()
}

fn prepare_task(
    store: &HostStore,
    task: TaskId,
    job: JobId,
    base_oid: &BaseOid,
) -> LeaseAcquireRequest {
    let request = acquire_lease(store, job);
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(
            &TaskPrepareRequest::new(task_meta(task, base_oid.clone()), job, "mini-1"),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);
    TaskStore::new(store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task)
        .unwrap();
    request
}

fn finish_done(store: &HostStore, task: TaskId, turn: JobId, oid: &BaseOid) {
    TaskStore::new(store, &SystemProcessRunner)
        .finish_turn(
            PROJECT_ID,
            task,
            turn,
            TurnTerminal::Succeeded,
            TaskOutcome::Done,
            true,
            false,
            Some(oid.clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            Vec::new(),
            false,
        )
        .unwrap();
}

fn commit_for(
    store: &HostStore,
    project: &str,
    task: TaskId,
    origin: &str,
    oid: &BaseOid,
    turn: JobId,
    now: u64,
) -> mac_worker::task::OriginDelivery {
    let branch: BranchName = "release-candidate".parse().unwrap();
    OriginOutbox::new(store, &SystemProcessRunner)
        .commit_intent(DeliveryCommit {
            project_id: project,
            task_id: task,
            turn_id: turn,
            oid,
            origin,
            branch: &branch,
            now_millis: now,
        })
        .unwrap()
}

fn commit(
    store: &HostStore,
    origin: &str,
    oid: &BaseOid,
    turn: JobId,
    now: u64,
) -> mac_worker::task::OriginDelivery {
    commit_for(store, PROJECT_ID, task_id(1), origin, oid, turn, now)
}

fn wall_clock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock precedes the Unix epoch")
        .as_millis()
        .try_into()
        .expect("system clock is outside the supported range")
}

fn done_agent_script() -> &'static str {
    r#"printf changed > agent.txt; printf '%s\n' '{"type":"thread.started","thread_id":"session-outbox"}' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"finished\",\"questions\":[],\"files_changed\":[]}"}}'"#
}

fn prepared_origin_push_turn(
    store: &HostStore,
    origin_url: &str,
    base_oid: &BaseOid,
) -> TaskTurnRequest {
    let prompt = "outbox fixture";
    let turn_limits = TurnLimits::new(30_000, None, None).unwrap();
    let turn = TurnMaterial::from_prompt(
        task_id(1),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        turn_limits.clone(),
        base_oid.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap();
    let launch = TurnLaunch::new(
        "/bin/sh",
        vec!["-c".into(), done_agent_script().into()],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let job = turn_id(2);
    let seed_material = RequestFingerprintMaterial::new(
        job,
        ClientId::new(Uuid::from_u128(20)),
        LeaseToken::new(Uuid::from_u128(30)),
        100,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed_material, seed_material.fingerprint(), 100, 30_100).unwrap();
    let projected = turn.v1_material(&seed_lease, &launch).unwrap();
    LeaseService::new(store)
        .acquire(
            &LeaseAcquireRequest::new(projected.clone()),
            &admissions(),
            wall_clock_millis(),
        )
        .unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: task_id(1),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: Some(PushTarget::new(origin_url.to_owned()).unwrap()),
        },
        publish: vec![PublishMode::Fetch, PublishMode::Push],
        publish_branch: Some("release-candidate".parse().unwrap()),
        base_oid: base_oid.clone(),
        limits: TaskLimits::new(turn_limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: prompt.into(),
        created_at_millis: 100,
    })
    .unwrap();
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(&TaskPrepareRequest::new(meta, job, "mini-1"), &transfer)
        .unwrap();
    drop(transfer);
    drop(admission);
    TaskTurnRequest::new_with_origin(
        SubmitRequest::new(projected),
        turn,
        prompt,
        Some(origin_url.to_owned()),
    )
    .unwrap()
}

fn outbox(store: &HostStore) -> OriginOutbox<'_> {
    OriginOutbox::new(store, &SystemProcessRunner)
}

fn target_ledgers(store: &HostStore) -> Vec<serde_json::Value> {
    let locks = store.root().join("locks");
    let mut ledgers = Vec::new();
    for entry in fs::read_dir(locks).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        if name.starts_with("outbox-target-") && name.ends_with(".json") {
            ledgers.push(serde_json::from_slice(&fs::read(path).unwrap()).unwrap());
        }
    }
    ledgers
}

fn delivery_path(host: &Path, project: &str, task: TaskId, turn: JobId) -> PathBuf {
    host.join("tasks")
        .join(project)
        .join(task.to_string())
        .join("delivery")
        .join(format!("{turn}.json"))
}

fn plant_task_delivery_dir(store: &HostStore, task: TaskId) -> PathBuf {
    let task_path = store.task_dir(PROJECT_ID, task).unwrap();
    fs::create_dir_all(&task_path).unwrap();
    if let Some(project) = task_path.parent() {
        fs::set_permissions(project, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::set_permissions(&task_path, fs::Permissions::from_mode(0o700)).unwrap();
    let delivery = task_path.join("delivery");
    fs::create_dir_all(&delivery).unwrap();
    fs::set_permissions(&delivery, fs::Permissions::from_mode(0o700)).unwrap();
    delivery
}

fn delivery_json_state(path: &Path) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    value.get("state")?.as_str().map(str::to_owned)
}

fn wait_until(seconds: u64, mut probe: impl FnMut() -> bool, on_timeout: impl FnOnce()) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if probe() {
            return;
        }
        if Instant::now() >= deadline {
            on_timeout();
            panic!("timed out waiting {seconds}s");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn xmlish(path: &Path) -> String {
    path.to_string_lossy()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn push_base(source: &GitRepo, store: &HostStore, project: &str, task: TaskId, spec: &str) {
    let mirror = store.mirror(project).unwrap();
    let output = source.git(&[
        "push",
        mirror.path().to_str().unwrap(),
        &format!("{spec}:refs/mac-worker/bases/{task}"),
    ]);
    assert!(
        output.status.success(),
        "base push failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn outbox_worker_pid(host_root: &Path) -> Option<i32> {
    let bytes = fs::read(host_root.join("locks/outbox-worker.json")).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("identity")?
        .get("pid")?
        .as_u64()
        .and_then(|pid| i32::try_from(pid).ok())
}

fn delivery_pack_stats(mirror: &Path) -> (usize, u64) {
    let pack_dir = mirror.join("objects/pack");
    let mut files = 0;
    let mut bytes = 0;
    let Ok(entries) = fs::read_dir(&pack_dir) else {
        return (0, 0);
    };
    for entry in entries {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy();
        if name.starts_with("mw-delivery-") && name.ends_with(".pack") {
            files += 1;
            bytes += fs::metadata(&path).unwrap().len();
        }
    }
    (files, bytes)
}

fn history_walk_count(runner: &support::recording_runner::RecordingRunner) -> usize {
    runner
        .requests()
        .into_iter()
        .filter(|request| {
            request.args.iter().any(|argument| {
                matches!(
                    argument.to_str(),
                    Some("rev-list" | "pack-objects" | "for-each-ref")
                )
            })
        })
        .count()
}

fn pack_store_names(mirror: &Path) -> Vec<String> {
    let pack_dir = mirror.join("objects/pack");
    let Ok(entries) = fs::read_dir(&pack_dir) else {
        return Vec::new();
    };
    let mut names = Vec::new();
    for entry in entries {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if name.ends_with(".pack") || name.ends_with(".idx") {
            names.push(name);
        }
    }
    names.sort();
    names
}

fn object_store_receipt(mirror: &Path) -> Option<serde_json::Value> {
    let bytes = fs::read(mirror.join(OBJECT_STORE_SYNC_RECEIPT)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn loose_object_exists(mirror: &Path, oid: &str) -> bool {
    oid.len() == 40
        && mirror
            .join("objects")
            .join(&oid[..2])
            .join(&oid[2..])
            .is_file()
}

fn assert_object_store_receipt(mirror: &Path) {
    assert_eq!(
        git(mirror, &["config", "--get", "core.fsync"]),
        "objects,derived-metadata,reference"
    );
    assert_eq!(
        git(mirror, &["config", "--get", "core.fsyncMethod"]),
        "fsync"
    );
    let receipt = object_store_receipt(mirror).expect("object-store receipt");
    assert_eq!(
        receipt.get("fsync").and_then(|value| value.as_str()),
        Some("objects,derived-metadata,reference")
    );
    assert_eq!(
        receipt.get("fsync_method").and_then(|value| value.as_str()),
        Some("fsync")
    );
    let packs = receipt
        .get("packs")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert_eq!(packs, pack_store_names(mirror));
    assert!(
        packs.iter().any(|name| name.ends_with(".pack")),
        "receipt must record at least one existing pack"
    );
}

fn unpack_non_delivery_packs(mirror: &Path) {
    let pack_dir = mirror.join("objects/pack");
    let Ok(entries) = fs::read_dir(&pack_dir) else {
        return;
    };
    let packs: Vec<PathBuf> = entries
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.ends_with(".pack") && !name.starts_with("mw-delivery-")
        })
        .collect();
    for pack in packs {
        let bytes = fs::read(&pack).unwrap();
        let mut child = Command::new("/usr/bin/git")
            .args(["--git-dir"])
            .arg(mirror)
            .args(["unpack-objects", "-q"])
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("unpack-objects");
        child
            .stdin
            .take()
            .expect("unpack stdin")
            .write_all(&bytes)
            .unwrap();
        assert!(child.wait().unwrap().success(), "unpack-objects failed");
        let _ = fs::remove_file(&pack);
        let _ = fs::remove_file(pack.with_extension("idx"));
    }
}

fn repack_existing_objects(mirror: &Path) {
    let output = Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(mirror)
        .args(["repack", "-Adq"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("repack");
    assert!(
        output.status.success(),
        "repack failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn spawn_and_plist_pass_the_exact_host_root() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let args = OriginOutbox::watch_args(store.root()).unwrap();
    let args = args
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(args[0], "host");
    assert_eq!(args[1], "outbox");
    assert_eq!(args[2], "--watch");
    assert_eq!(args[3], "--host-root");
    assert_eq!(args[4], store.root().to_string_lossy());
    let plist = OriginOutbox::launchd_plist(Path::new("/usr/bin/worker"), store.root()).unwrap();
    assert!(plist.contains("<key>KeepAlive</key>"));
    assert!(plist.contains("<true/>"));
    assert!(plist.contains("--host-root"));
    assert!(plist.contains(&xmlish(store.root())));
    assert!(!plist.contains("WorkingDirectory"));
}

#[test]
fn delayed_origin_leaves_the_heavy_slot_idle() {
    let (_temp, store, _source, origin_dir, origin, origin_url, oid) = fixture();
    fs::write(origin.join("reject"), b"1").unwrap();
    let request = prepared_origin_push_turn(&store, &origin_url, &oid);
    assert_eq!(
        LeaseService::new(&store).occupancy().unwrap().slot_state,
        SlotState::Busy
    );
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = JobService::new(&store, &launcher)
        .submit_turn(request)
        .unwrap();
    assert_eq!(response.submit().status().state(), JobState::Succeeded);
    assert_eq!(response.task().last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(response.task().state(), TaskState::Open);
    let result_oid = response.task().head_oid().unwrap().clone();
    assert_ne!(result_oid.as_str(), oid.as_str());
    let turn = response.task().turns().last().unwrap().turn_id();
    let pin = OriginOutbox::delivery_refs(task_id(1), turn);
    assert!(ref_exists(
        store.mirror_if_present(PROJECT_ID).unwrap().unwrap().path(),
        &pin
    ));
    let delivery = outbox(&store).dto(PROJECT_ID, task_id(1)).unwrap().unwrap();
    assert_eq!(delivery.state(), DeliveryState::Pending);
    assert_eq!(delivery.oid(), &result_oid);
    assert_eq!(delivery.turn_id(), turn);
    assert_eq!(
        LeaseService::new(&store).occupancy().unwrap().slot_state,
        SlotState::Idle
    );
    acquire_lease(&store, turn_id(99));
    assert_eq!(
        LeaseService::new(&store).occupancy().unwrap().slot_state,
        SlotState::Busy
    );
    let _ = origin_dir;
    fs::remove_file(origin.join("reject")).unwrap();
    let delivered = outbox(&store).pump_due(1).unwrap();
    assert!(
        delivered
            .iter()
            .any(|item| item.turn_id() == turn && item.state() == DeliveryState::Delivered)
    );
}

#[test]
fn cleanup_after_durable_intent_releases_the_heavy_slot() {
    let (_temp, store, _source, _origin_dir, origin, origin_url, oid) = fixture();
    fs::write(origin.join("reject"), b"1").unwrap();
    let (_lease, request) = prepared_supervisor_job(
        &store,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    );
    assert_eq!(
        LeaseService::new(&store).occupancy().unwrap().slot_state,
        SlotState::Busy
    );
    let delivery = commit(&store, &origin_url, &oid, turn_id(2), 1);
    assert_eq!(delivery.state(), DeliveryState::Pending);
    let pin = OriginOutbox::delivery_refs(task_id(1), turn_id(2));
    assert!(ref_exists(
        store.mirror_if_present(PROJECT_ID).unwrap().unwrap().path(),
        &pin
    ));
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(
        LeaseService::new(&store).occupancy().unwrap().slot_state,
        SlotState::Idle
    );
}

#[test]
fn failing_origin_retries_without_flipping_agent_outcome() {
    let (_temp, store, _source, _origin_dir, origin, origin_url, oid) = fixture();
    fs::write(origin.join("reject"), b"1").unwrap();
    let job = turn_id(2);
    let request = prepare_task(&store, task_id(1), job, &oid);
    commit(&store, &origin_url, &oid, job, 1);
    finish_done(&store, task_id(1), job, &oid);
    let results = outbox(&store).pump_due(1).unwrap();
    assert_eq!(results[0].state(), DeliveryState::Retrying);
    assert_eq!(results[0].last_error(), Some("PUBLISH_FAILED"));
    let status = TaskStore::new(&store, &SystemProcessRunner)
        .load_status(PROJECT_ID, task_id(1))
        .unwrap();
    assert_eq!(status.last_outcome(), Some(&TaskOutcome::Done));
    assert_eq!(status.state(), TaskState::Open);
    release_lease(&store, &request);
}

#[test]
fn crash_after_intent_re_pins_before_push() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"one\n");
    source.commit_all("one");
    let oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    drop(store);
    let store =
        HostStore::open_with_write_fault(&host, HostStoreWritePoint::AfterOutboxIntent).unwrap();
    let (_origin_dir, origin_path) = origin_bare();
    let origin_url = file_url(&origin_path);
    let branch: BranchName = "release-candidate".parse().unwrap();
    let error = OriginOutbox::new(&store, &SystemProcessRunner)
        .commit_intent(DeliveryCommit {
            project_id: PROJECT_ID,
            task_id: task_id(1),
            turn_id: turn_id(2),
            oid: &oid,
            origin: &origin_url,
            branch: &branch,
            now_millis: 1,
        })
        .unwrap_err();
    assert_eq!(error.public_code(), "IO");
    let pin = OriginOutbox::delivery_refs(task_id(1), turn_id(2));
    assert!(!ref_exists(
        store.mirror_if_present(PROJECT_ID).unwrap().unwrap().path(),
        &pin
    ));
    drop(store);
    let store = HostStore::open(&host).unwrap();
    let delivered = outbox(&store).pump_due(1).unwrap();
    assert_eq!(delivered[0].state(), DeliveryState::Delivered);
    assert!(ref_exists(
        store.mirror_if_present(PROJECT_ID).unwrap().unwrap().path(),
        &pin
    ));
}

#[test]
fn pin_only_crash_fails_closed() {
    let (_temp, store, _source, _origin_dir, _origin, _origin_url, oid) = fixture();
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    GitTransport::new(&SystemProcessRunner)
        .pin_delivery_ref(&store, &mirror, task_id(1), turn_id(2), &oid)
        .unwrap();
    assert!(outbox(&store).retains(PROJECT_ID, task_id(1), 1).unwrap());
    assert!(
        outbox(&store)
            .dto(PROJECT_ID, task_id(1))
            .unwrap()
            .is_none()
    );
    assert!(outbox(&store).pump_due(1).unwrap().is_empty());
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
}

#[test]
fn two_turns_on_the_same_task_keep_independent_pins() {
    let (_temp, store, source, _origin_dir, origin, origin_url, first) = fixture();
    source.write("base.txt", b"two\n");
    source.commit_all("two");
    let second: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), second.as_str());
    let first_delivery = commit(&store, &origin_url, &first, turn_id(2), 1);
    fs::write(origin.join("reject"), b"1").unwrap();
    let second_delivery = commit(&store, &origin_url, &second, turn_id(3), 2);
    assert_eq!(first_delivery.state(), DeliveryState::Pending);
    assert_eq!(second_delivery.state(), DeliveryState::Pending);
    assert_ne!(first_delivery.oid(), second_delivery.oid());
    let deliveries = outbox(&store).deliveries(PROJECT_ID, task_id(1)).unwrap();
    assert_eq!(deliveries.len(), 2);
}

#[test]
fn mutating_the_task_branch_does_not_change_the_pinned_oid() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, first) = fixture();
    commit(&store, &origin_url, &first, turn_id(2), 1);
    source.write("base.txt", b"mutated\n");
    source.commit_all("mutated");
    let mutated: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), mutated.as_str());
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    set_ref(
        mirror.path(),
        &format!("refs/heads/task/{}", task_id(1)),
        mutated.as_str(),
    );
    let delivered = outbox(&store).pump_due(1).unwrap();
    assert_eq!(delivered[0].oid(), &first);
    assert_eq!(delivered[0].state(), DeliveryState::Delivered);
}

#[test]
fn older_retry_does_not_rewind_a_newer_target_ledger() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, first) = fixture();
    source.write("base.txt", b"two\n");
    source.commit_all("two");
    let second: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), second.as_str());
    commit(&store, &origin_url, &first, turn_id(2), 1);
    commit(&store, &origin_url, &second, turn_id(3), 2);
    let delivered = outbox(&store).pump_due(3).unwrap();
    assert!(
        delivered
            .iter()
            .any(|item| item.turn_id() == turn_id(3) && item.state() == DeliveryState::Delivered)
    );
    let ledgers = target_ledgers(&store);
    assert_eq!(ledgers.len(), 1);
    assert_eq!(ledgers[0]["last_oid"], serde_json::json!(second.as_str()));
    let replay = outbox(&store).deliveries(PROJECT_ID, task_id(1)).unwrap();
    let older = replay
        .iter()
        .find(|item| item.turn_id() == turn_id(2))
        .unwrap();
    assert!(older.state() == DeliveryState::Delivered || older.state() == DeliveryState::Retrying);
    if older.superseded_by().is_some() {
        assert_eq!(older.superseded_by().unwrap().as_str(), second.as_str());
    }
    let ledgers = target_ledgers(&store);
    assert_eq!(ledgers[0]["last_oid"], serde_json::json!(second.as_str()));
}

#[test]
fn generic_non_fast_forward_stays_retrying() {
    let (_temp, store, source, _origin_dir, origin, origin_url, first) = fixture();
    let unrelated = GitRepo::init();
    unrelated.write("other.txt", b"other\n");
    unrelated.commit_all("other");
    assert!(
        unrelated
            .git(&[
                "push",
                origin.to_str().unwrap(),
                "HEAD:refs/heads/release-candidate",
            ])
            .status
            .success()
    );
    let _ = source;
    commit(&store, &origin_url, &first, turn_id(2), 1);
    let results = outbox(&store).pump_due(1).unwrap();
    assert_eq!(results[0].state(), DeliveryState::Retrying);
    assert!(results[0].superseded_by().is_none());
}

#[test]
fn close_and_gc_retain_pending_pins() {
    let (_temp, store, _source, _origin_dir, origin, origin_url, oid) = fixture();
    fs::write(origin.join("reject"), b"1").unwrap();
    let job = turn_id(2);
    let request = prepare_task(&store, task_id(1), job, &oid);
    commit(&store, &origin_url, &oid, job, 1);
    finish_done(&store, task_id(1), job, &oid);
    outbox(&store).pump_due(1).unwrap();
    release_lease(&store, &request);
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(1), true))
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_BUSY");
    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(u64::MAX / 2)
        .unwrap();
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), job)
    ));
}

#[test]
fn gc_and_discard_retain_an_orphaned_delivery_pin() {
    let (_temp, store, _source, _origin_dir, _origin, _origin_url, oid) = fixture();
    let job = turn_id(2);
    let request = prepare_task(&store, task_id(1), job, &oid);
    finish_done(&store, task_id(1), job, &oid);
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    GitTransport::new(&SystemProcessRunner)
        .pin_delivery_ref(&store, &mirror, task_id(1), job, &oid)
        .unwrap();
    release_lease(&store, &request);
    let error = TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(1), true))
        .unwrap_err();
    assert_eq!(error.public_code(), "TASK_BUSY");
    TaskStore::new(&store, &SystemProcessRunner)
        .close(&TaskCloseRequest::new(PROJECT_ID, task_id(1), false))
        .unwrap();
    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(u64::MAX)
        .unwrap();
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), job)
    ));
}

#[test]
fn in_process_watch_retries_until_origin_recovers() {
    let (_temp, store, _source, _origin_dir, origin, origin_url, oid) = fixture();
    fs::write(origin.join("reject"), b"1").unwrap();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    let stop = Arc::new(AtomicBool::new(false));
    let now = Arc::new(AtomicU64::new(1));
    let cloned = store.clone();
    let stop_thread = stop.clone();
    let now_thread = now.clone();
    let join = thread::spawn(move || {
        OriginOutbox::new(&cloned, &SystemProcessRunner)
            .run_watch_with(&stop_thread, || now_thread.load(Ordering::SeqCst), 5)
            .unwrap();
    });
    wait_until(
        8,
        || {
            outbox(&store)
                .dto(PROJECT_ID, task_id(1))
                .ok()
                .flatten()
                .is_some_and(|delivery| delivery.state() == DeliveryState::Retrying)
        },
        || panic!("in-process watcher did not retry a rejected origin"),
    );
    stop.store(true, Ordering::SeqCst);
    join.join().unwrap();
    fs::remove_file(origin.join("reject")).unwrap();
    now.store(1_000_000, Ordering::SeqCst);
    let stop = Arc::new(AtomicBool::new(false));
    let cloned = store.clone();
    let stop_thread = stop.clone();
    let now_thread = now.clone();
    let join = thread::spawn(move || {
        OriginOutbox::new(&cloned, &SystemProcessRunner)
            .run_watch_with(&stop_thread, || now_thread.load(Ordering::SeqCst), 5)
            .unwrap();
    });
    wait_until(
        8,
        || {
            outbox(&store)
                .dto(PROJECT_ID, task_id(1))
                .ok()
                .flatten()
                .is_some_and(|delivery| delivery.state() == DeliveryState::Delivered)
        },
        || panic!("in-process watcher did not deliver after origin recovery"),
    );
    stop.store(true, Ordering::SeqCst);
    join.join().unwrap();
}

#[test]
fn worker_process_watch_retries_after_parent_returns_and_restart_recovers() {
    let (temp, store, _source, origin_dir, origin, origin_url, oid) = fixture();
    let host_root = store.root().to_path_buf();
    fs::write(origin.join("reject"), b"1").unwrap();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    drop(store);
    let _ = origin_dir;
    let home = temp.path().join("home");
    let data = temp.path().join("xdg");
    let tmp = temp.path().join("tmp");
    let delivery = delivery_path(&host_root, PROJECT_ID, task_id(1), turn_id(2));
    let first_log = temp.path().join("watch-1.err");
    let first = WatchChild::spawn(&host_root, &home, &data, &tmp, first_log);
    wait_until(
        5,
        || delivery_json_state(&delivery).as_deref() == Some("retrying"),
        || {
            let log = first.log_text();
            panic!("first watch did not retry: {log}");
        },
    );
    drop(first);
    fs::remove_file(origin.join("reject")).unwrap();
    let second_log = temp.path().join("watch-2.err");
    let second = WatchChild::spawn(&host_root, &home, &data, &tmp, second_log);
    wait_until(
        12,
        || delivery_json_state(&delivery).as_deref() == Some("delivered"),
        || {
            let log = second.log_text();
            panic!("restarted watch did not deliver: {log}");
        },
    );
    drop(second);
    let store = HostStore::open(&host_root).unwrap();
    let recovered = outbox(&store).dto(PROJECT_ID, task_id(1)).unwrap().unwrap();
    assert_eq!(recovered.state(), DeliveryState::Delivered);
}

#[test]
fn idle_watcher_does_not_repack_finalized_history() {
    let (_temp, store, _source, _origin_dir, _origin, origin_url, oid) = fixture();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    outbox(&store).pump_due(1).unwrap();
    let scans_before = task_directory_scans_for(store.root());
    let due_before = due_index_reads_for(store.root());
    let runner = support::recording_runner::RecordingRunner::passthrough();
    let stop = AtomicBool::new(false);
    let ticks = AtomicU64::new(0);
    thread::scope(|scope| {
        scope.spawn(|| {
            OriginOutbox::new(&store, &runner)
                .run_watch_with(
                    &stop,
                    || {
                        let tick = ticks.fetch_add(1, Ordering::SeqCst);
                        if tick > 3 {
                            stop.store(true, Ordering::SeqCst);
                        }
                        10
                    },
                    5,
                )
                .unwrap();
        });
    });
    let git_ops = runner
        .requests()
        .into_iter()
        .filter(|request| {
            request.args.iter().any(|argument| {
                matches!(
                    argument.to_str(),
                    Some("pack-objects" | "push" | "update-ref" | "cat-file")
                )
            })
        })
        .count();
    assert_eq!(git_ops, 0);
    assert_eq!(task_directory_scans_for(store.root()), scans_before + 1);
    assert!(due_index_reads_for(store.root()) > due_before);
}

#[test]
fn pin_failure_increments_attempt_and_applies_backoff() {
    let (_temp, store, _source, _origin_dir, _origin, origin_url, oid) = fixture();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    let runner = support::recording_runner::RecordingRunner::returning_results(vec![Err(
        WorkerError::task("PUBLISH_FAILED", "pin failed"),
    )]);
    let results = OriginOutbox::new(&store, &runner).pump_due(10).unwrap();
    assert_eq!(results[0].state(), DeliveryState::Retrying);
    assert!(results[0].attempt() >= 2);
    assert!(
        results[0].next_attempt_at_millis() >= 10 + 1_000,
        "backoff must not collapse to a 1ms hot loop"
    );
}

#[test]
fn malformed_intent_does_not_block_an_unrelated_delivery() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, oid) = fixture();
    push_base(&source, &store, PROJECT_ID, task_id(9), oid.as_str());
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    GitTransport::new(&SystemProcessRunner)
        .pin_delivery_ref(&store, &mirror, task_id(9), turn_id(8), &oid)
        .unwrap();
    let bad = plant_task_delivery_dir(&store, task_id(9));
    let garbage = bad.join(format!("{}.json", turn_id(8)));
    fs::write(&garbage, b"{not json").unwrap();
    fs::set_permissions(&garbage, fs::Permissions::from_mode(0o600)).unwrap();
    commit_for(
        &store,
        PROJECT_ID,
        task_id(2),
        &origin_url,
        &oid,
        turn_id(3),
        1,
    );
    let results = outbox(&store).pump_due(1).unwrap();
    assert!(
        results
            .iter()
            .any(|item| item.turn_id() == turn_id(3) && item.state() == DeliveryState::Delivered)
    );
    assert!(outbox(&store).retains(PROJECT_ID, task_id(9), 1).unwrap());
    assert!(
        outbox(&store)
            .discard_blocked(PROJECT_ID, task_id(9))
            .unwrap()
    );
    let visible = outbox(&store).deliveries(PROJECT_ID, task_id(9)).unwrap();
    assert!(
        visible
            .iter()
            .any(|item| item.last_error() == Some(DELIVERY_UNREADABLE) && item.oid() == &oid)
    );
}

#[test]
fn unavailable_mirror_does_not_block_an_unrelated_delivery() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, oid) = fixture();
    push_base(&source, &store, OTHER_PROJECT_ID, task_id(1), oid.as_str());
    commit_for(
        &store,
        OTHER_PROJECT_ID,
        task_id(1),
        &origin_url,
        &oid,
        turn_id(2),
        1,
    );
    commit(&store, &origin_url, &oid, turn_id(3), 2);
    let other_mirror = store.mirror(OTHER_PROJECT_ID).unwrap();
    fs::remove_dir_all(other_mirror.path()).unwrap();
    let results = outbox(&store).pump_due(3).unwrap();
    let other = results
        .iter()
        .find(|item| item.turn_id() == turn_id(2))
        .cloned();
    let local = results
        .iter()
        .find(|item| item.turn_id() == turn_id(3))
        .cloned();
    if let Some(other) = other {
        assert_ne!(other.state(), DeliveryState::Delivered);
    }
    assert_eq!(local.unwrap().state(), DeliveryState::Delivered);
}

#[test]
fn extra_watchers_exit_instead_of_blocking() {
    let (_temp, store, _source, _origin_dir, _origin, origin_url, oid) = fixture();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    let stop = Arc::new(AtomicBool::new(false));
    let cloned = store.clone();
    let stop_thread = stop.clone();
    thread::scope(|scope| {
        scope.spawn(|| {
            OriginOutbox::new(&cloned, &SystemProcessRunner)
                .run_watch_with(&stop_thread, || 1, 50)
                .unwrap();
        });
        wait_until(2, || outbox(&store).live_watch().unwrap(), || {});
        let started = Instant::now();
        OriginOutbox::new(&store, &SystemProcessRunner)
            .run_watch_with(&AtomicBool::new(false), || 1, 50)
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "extra watcher blocked on singleton election"
        );
        stop.store(true, Ordering::SeqCst);
    });
}

#[test]
fn unconfigured_launcher_surfaces_worker_required() {
    let (_temp, store, _source, _origin_dir, _origin, origin_url, oid) = fixture();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    let activation = outbox(&store).wake(&InactiveLauncher).unwrap();
    assert_eq!(activation, OutboxActivation::Inactive);
    let delivery = outbox(&store).dto(PROJECT_ID, task_id(1)).unwrap().unwrap();
    assert_eq!(delivery.last_error(), Some(OUTBOX_WORKER_REQUIRED));
    assert_eq!(delivery.state(), DeliveryState::Pending);
}

#[test]
fn production_wake_survives_parent_exit() {
    let (temp, store, _source, origin_dir, origin, origin_url, oid) = fixture();
    let host_root = store.root().to_path_buf();
    fs::write(origin.join("reject"), b"1").unwrap();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    drop(store);
    let _ = origin_dir;
    let home = temp.path().join("home");
    let data = temp.path().join("xdg");
    let tmp = temp.path().join("tmp");
    let delivery = delivery_path(&host_root, PROJECT_ID, task_id(1), turn_id(2));
    let log = temp.path().join("wake.err");
    let watch = DetachedWatch::spawn_via_wake(&host_root, &home, &data, &tmp, log);
    wait_until(
        5,
        || outbox_worker_pid(&host_root).is_some(),
        || panic!("detached watcher did not publish worker identity"),
    );
    wait_until(
        8,
        || delivery_json_state(&delivery).as_deref() == Some("retrying"),
        || panic!("detached watcher did not retry after parent exit"),
    );
    fs::remove_file(origin.join("reject")).unwrap();
    wait_until(
        15,
        || delivery_json_state(&delivery).as_deref() == Some("delivered"),
        || panic!("detached watcher did not deliver after origin recovery"),
    );
    drop(watch);
}

#[test]
fn same_oid_retry_does_not_repack_history() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, _first) = fixture();
    for round in 0..12 {
        source.write("base.txt", format!("history-{round}\n").as_bytes());
        source.commit_all(&format!("history-{round}"));
    }
    let history: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), history.as_str());
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    unpack_non_delivery_packs(mirror.path());
    assert!(loose_object_exists(mirror.path(), history.as_str()));
    assert!(object_store_receipt(mirror.path()).is_none());
    commit(&store, &origin_url, &history, turn_id(2), 1);
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
    assert!(loose_object_exists(mirror.path(), history.as_str()));
    let receipt = object_store_receipt(mirror.path()).expect("complete baseline receipt");
    assert_eq!(
        receipt.get("fsync").and_then(|value| value.as_str()),
        Some("objects,derived-metadata,reference")
    );
    assert_eq!(
        receipt.get("fsync_method").and_then(|value| value.as_str()),
        Some("fsync")
    );
    assert!(
        receipt.get("anchors").is_none(),
        "baseline receipt must not accumulate per-result anchors"
    );
    let packs = receipt
        .get("packs")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert_eq!(packs, pack_store_names(mirror.path()));
    let receipt_after_first = fs::read(mirror.path().join(OBJECT_STORE_SYNC_RECEIPT)).unwrap();

    let retry_runner = support::recording_runner::RecordingRunner::passthrough();
    GitTransport::new(&retry_runner)
        .pin_delivery_ref(&store, &mirror, task_id(1), turn_id(2), &history)
        .unwrap();
    assert_eq!(
        history_walk_count(&retry_runner),
        0,
        "same-OID retry must not re-enumerate or repack reachable history"
    );
    assert_eq!(
        fs::read(mirror.path().join(OBJECT_STORE_SYNC_RECEIPT)).unwrap(),
        receipt_after_first,
        "same-OID retry must reuse the established baseline receipt"
    );
}

#[test]
fn recover_continues_past_an_unreadable_delivery_directory() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, oid) = fixture();
    push_base(&source, &store, PROJECT_ID, task_id(9), oid.as_str());
    let bad = plant_task_delivery_dir(&store, task_id(9));
    fs::write(bad.join(format!("{}.json", turn_id(8))), b"{not json").unwrap();
    fs::set_permissions(&bad, fs::Permissions::from_mode(0o000)).unwrap();
    let _restore = ModeRestore {
        path: bad,
        mode: 0o700,
    };
    commit_for(
        &store,
        PROJECT_ID,
        task_id(2),
        &origin_url,
        &oid,
        turn_id(3),
        1,
    );
    let due_dir = store.root().join("locks/outbox-due");
    if due_dir.is_dir() {
        for entry in fs::read_dir(&due_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "json") {
                fs::remove_file(path).unwrap();
            }
        }
    }
    let stop = AtomicBool::new(false);
    let ticks = AtomicU64::new(0);
    thread::scope(|scope| {
        scope.spawn(|| {
            OriginOutbox::new(&store, &SystemProcessRunner)
                .run_watch_with(
                    &stop,
                    || {
                        let tick = ticks.fetch_add(1, Ordering::SeqCst);
                        if tick > 2 {
                            stop.store(true, Ordering::SeqCst);
                        }
                        1
                    },
                    5,
                )
                .unwrap();
        });
    });
    let delivered = outbox(&store)
        .deliveries(PROJECT_ID, task_id(2))
        .unwrap()
        .into_iter()
        .any(|item| item.turn_id() == turn_id(3) && item.state() == DeliveryState::Delivered);
    assert!(delivered, "unreadable sibling must not abort due recovery");
}

#[test]
fn wake_fails_closed_when_the_watcher_does_not_acknowledge() {
    let (_temp, store, _source, _origin_dir, _origin, origin_url, oid) = fixture();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    let error = outbox(&store).wake(&FalseActiveLauncher).unwrap_err();
    assert_eq!(error.public_code(), OUTBOX_WORKER_REQUIRED);
    assert!(!outbox(&store).live_watch().unwrap());
    let delivery = outbox(&store).dto(PROJECT_ID, task_id(1)).unwrap().unwrap();
    assert_eq!(delivery.last_error(), Some(OUTBOX_WORKER_REQUIRED));
}

#[test]
fn once_returns_busy_while_a_watcher_holds_the_pump() {
    let (_temp, store, _source, _origin_dir, _origin, origin_url, oid) = fixture();
    commit(&store, &origin_url, &oid, turn_id(2), 1);
    let stop = Arc::new(AtomicBool::new(false));
    let cloned = store.clone();
    let stop_thread = stop.clone();
    thread::scope(|scope| {
        scope.spawn(|| {
            OriginOutbox::new(&cloned, &SystemProcessRunner)
                .run_watch_with(&stop_thread, || 1, 50)
                .unwrap();
        });
        wait_until(2, || outbox(&store).live_watch().unwrap(), || {});
        let started = Instant::now();
        let error = outbox(&store).run_once(1).unwrap_err();
        assert_eq!(error.public_code(), OUTBOX_BUSY);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(outbox(&store).live_watch().unwrap());
        stop.store(true, Ordering::SeqCst);
    });
}

#[test]
fn already_packed_result_is_durable_without_repacking() {
    let (_temp, store, source, _origin_dir, _origin, origin_url, first) = fixture();
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    repack_existing_objects(mirror.path());
    assert!(
        pack_store_names(mirror.path())
            .iter()
            .any(|name| name.ends_with(".pack")),
        "fixture must leave the result packed before the first pin"
    );
    assert!(object_store_receipt(mirror.path()).is_none());
    let (files_before, bytes_before) = delivery_pack_stats(mirror.path());
    commit(&store, &origin_url, &first, turn_id(2), 1);
    let (files_after_first, bytes_after_first) = delivery_pack_stats(mirror.path());
    assert_eq!(files_after_first, files_before);
    assert_eq!(bytes_after_first, bytes_before);
    assert_object_store_receipt(mirror.path());
    let receipt_after_first = fs::read(mirror.path().join(OBJECT_STORE_SYNC_RECEIPT)).unwrap();
    commit(&store, &origin_url, &first, turn_id(2), 2);
    let (files_after_retry, bytes_after_retry) = delivery_pack_stats(mirror.path());
    assert_eq!(files_after_retry, files_after_first);
    assert_eq!(bytes_after_retry, bytes_after_first);
    assert_eq!(
        fs::read(mirror.path().join(OBJECT_STORE_SYNC_RECEIPT)).unwrap(),
        receipt_after_first,
        "same-OID retry must reuse the established baseline receipt"
    );
    source.write("base.txt", b"packed-second\n");
    source.commit_all("packed-second");
    let second: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), second.as_str());
    repack_existing_objects(mirror.path());
    commit(&store, &origin_url, &second, turn_id(3), 3);
    let (files_after_second, bytes_after_second) = delivery_pack_stats(mirror.path());
    assert_eq!(files_after_second, files_after_first);
    assert_eq!(bytes_after_second, bytes_after_first);
    assert_object_store_receipt(mirror.path());
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), turn_id(3))
    ));
}

#[test]
fn second_distinct_packed_result_skips_history_walk() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    for index in 0..40 {
        source.write("base.txt", format!("{index}\n").as_bytes());
        source.commit_all(&format!("commit-{index}"));
    }
    let first: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    repack_existing_objects(mirror.path());
    GitTransport::new(&SystemProcessRunner)
        .pin_delivery_ref(&store, &mirror, task_id(1), turn_id(2), &first)
        .unwrap();
    assert_object_store_receipt(mirror.path());
    let receipt = object_store_receipt(mirror.path()).expect("object-store receipt");
    assert!(
        receipt.get("anchors").is_none(),
        "baseline receipt must not accumulate per-result anchors"
    );
    source.write("base.txt", b"second-result\n");
    source.commit_all("second");
    let second: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), second.as_str());
    repack_existing_objects(mirror.path());
    let runner = support::recording_runner::RecordingRunner::passthrough();
    GitTransport::new(&runner)
        .pin_delivery_ref(&store, &mirror, task_id(1), turn_id(3), &second)
        .unwrap();
    assert_eq!(
        history_walk_count(&runner),
        0,
        "second distinct pin must not re-enumerate or repack reachable history"
    );
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), turn_id(3))
    ));
}

#[test]
fn all_loose_ancestor_is_synced_before_baseline_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"ancestor\n");
    source.commit_all("ancestor");
    let ancestor: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(2), "HEAD");
    source.write("base.txt", b"result\n");
    source.commit_all("result");
    let result: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    let mirror = store
        .mirror_if_present(PROJECT_ID)
        .unwrap()
        .unwrap()
        .path()
        .to_path_buf();
    unpack_non_delivery_packs(&mirror);
    assert!(
        loose_object_exists(&mirror, ancestor.as_str()),
        "ancestor must remain a separately referenced loose object"
    );
    assert!(loose_object_exists(&mirror, result.as_str()));
    let (_origin_dir, origin_path) = origin_bare();
    let origin_url = file_url(&origin_path);
    drop(store);
    assert!(object_store_receipt(&mirror).is_none());
    let store = HostStore::open_with_write_fault(
        &host,
        HostStoreWritePoint::AfterOutboxObjectBaselinePacks,
    )
    .unwrap();
    let branch: BranchName = "release-candidate".parse().unwrap();
    let error = OriginOutbox::new(&store, &SystemProcessRunner)
        .commit_intent(DeliveryCommit {
            project_id: PROJECT_ID,
            task_id: task_id(1),
            turn_id: turn_id(2),
            oid: &result,
            origin: &origin_url,
            branch: &branch,
            now_millis: 1,
        })
        .unwrap_err();
    assert_eq!(error.public_code(), "REF_UPDATE_FAILED");
    assert!(object_store_receipt(&mirror).is_none());
    assert!(!ref_exists(
        &mirror,
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
    assert!(loose_object_exists(&mirror, ancestor.as_str()));
    drop(store);
    let store = HostStore::open(&host).unwrap();
    commit(&store, &origin_url, &result, turn_id(2), 2);
    assert!(ref_exists(
        &mirror,
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
    let receipt = object_store_receipt(&mirror).expect("loose baseline receipt");
    assert_eq!(
        receipt.get("fsync").and_then(|value| value.as_str()),
        Some("objects,derived-metadata,reference")
    );
    assert!(
        receipt.get("anchors").is_none(),
        "loose baseline must not record graph anchors"
    );
    assert!(loose_object_exists(&mirror, ancestor.as_str()));
}

#[test]
fn second_pin_survives_gc_of_an_unrelated_completed_branch() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"first\n");
    source.commit_all("first");
    let first: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    GitTransport::new(&SystemProcessRunner)
        .pin_delivery_ref(&store, &mirror, task_id(1), turn_id(2), &first)
        .unwrap();
    let unrelated = GitRepo::init();
    unrelated.write("other.txt", b"unrelated\n");
    unrelated.commit_all("unrelated");
    let other: BaseOid = git(unrelated.root(), &["rev-parse", "HEAD"])
        .parse()
        .unwrap();
    push_base(&unrelated, &store, PROJECT_ID, task_id(9), "HEAD");
    GitTransport::new(&SystemProcessRunner)
        .pin_delivery_ref(&store, &mirror, task_id(9), turn_id(8), &other)
        .unwrap();
    let other_ref = format!("refs/mac-worker/bases/{}", task_id(9));
    git(mirror.path(), &["update-ref", "-d", &other_ref]);
    git(
        mirror.path(),
        &[
            "update-ref",
            "-d",
            &OriginOutbox::delivery_refs(task_id(9), turn_id(8)),
        ],
    );
    let gc = Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(mirror.path())
        .args(["gc", "--prune=now", "--quiet"])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("gc");
    assert!(
        gc.status.success(),
        "gc failed: {}",
        String::from_utf8_lossy(&gc.stderr)
    );
    source.write("base.txt", b"second\n");
    source.commit_all("second");
    let second: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), second.as_str());
    let runner = support::recording_runner::RecordingRunner::passthrough();
    GitTransport::new(&runner)
        .pin_delivery_ref(&store, &mirror, task_id(1), turn_id(3), &second)
        .unwrap();
    assert!(
        runner.requests().iter().all(|request| {
            request
                .args
                .iter()
                .all(|argument| argument.to_str() != Some("rev-list"))
        }),
        "post-GC pin must not rev-list a pruned unrelated tip"
    );
    assert!(ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), turn_id(3))
    ));
}

#[test]
fn packed_baseline_fails_closed_before_receipt_or_pin() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"one\n");
    source.commit_all("one");
    let oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    let mirror = store
        .mirror_if_present(PROJECT_ID)
        .unwrap()
        .unwrap()
        .path()
        .to_path_buf();
    repack_existing_objects(&mirror);
    let (_origin_dir, origin_path) = origin_bare();
    let origin_url = file_url(&origin_path);
    drop(store);
    assert!(
        pack_store_names(&mirror)
            .iter()
            .any(|name| name.ends_with(".pack")),
        "pre-policy packed objects must exist before the baseline"
    );
    assert!(object_store_receipt(&mirror).is_none());
    let store = HostStore::open_with_write_fault(
        &host,
        HostStoreWritePoint::AfterOutboxObjectBaselinePacks,
    )
    .unwrap();
    let branch: BranchName = "release-candidate".parse().unwrap();
    let error = OriginOutbox::new(&store, &SystemProcessRunner)
        .commit_intent(DeliveryCommit {
            project_id: PROJECT_ID,
            task_id: task_id(1),
            turn_id: turn_id(2),
            oid: &oid,
            origin: &origin_url,
            branch: &branch,
            now_millis: 1,
        })
        .unwrap_err();
    assert_eq!(error.public_code(), "REF_UPDATE_FAILED");
    assert!(!ref_exists(
        &mirror,
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
    assert!(object_store_receipt(&mirror).is_none());
}

#[test]
fn packed_baseline_receipt_is_retained_when_pin_publication_fails() {
    let temp = tempfile::tempdir().unwrap();
    let host = temp.path().join("host");
    let store = HostStore::open(&host).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"one\n");
    source.commit_all("one");
    let oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    push_base(&source, &store, PROJECT_ID, task_id(1), "HEAD");
    let mirror_for_pack = store
        .mirror_if_present(PROJECT_ID)
        .unwrap()
        .unwrap()
        .path()
        .to_path_buf();
    repack_existing_objects(&mirror_for_pack);
    let (_origin_dir, origin_path) = origin_bare();
    let origin_url = file_url(&origin_path);
    drop(store);
    let store =
        HostStore::open_with_write_fault(&host, HostStoreWritePoint::AfterOutboxObjectBaseline)
            .unwrap();
    let branch: BranchName = "release-candidate".parse().unwrap();
    let error = OriginOutbox::new(&store, &SystemProcessRunner)
        .commit_intent(DeliveryCommit {
            project_id: PROJECT_ID,
            task_id: task_id(1),
            turn_id: turn_id(2),
            oid: &oid,
            origin: &origin_url,
            branch: &branch,
            now_millis: 1,
        })
        .unwrap_err();
    assert_eq!(error.public_code(), "REF_UPDATE_FAILED");
    let mirror = store.mirror_if_present(PROJECT_ID).unwrap().unwrap();
    assert!(!ref_exists(
        mirror.path(),
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
    assert_object_store_receipt(mirror.path());
    let mirror_path = mirror.path().to_path_buf();
    let receipt = fs::read(mirror_path.join(OBJECT_STORE_SYNC_RECEIPT)).unwrap();
    drop(mirror);
    drop(store);
    let store = HostStore::open_with_write_fault(
        &host,
        HostStoreWritePoint::AfterOutboxObjectBaselinePacks,
    )
    .unwrap();
    commit(&store, &origin_url, &oid, turn_id(2), 2);
    assert!(ref_exists(
        &mirror_path,
        &OriginOutbox::delivery_refs(task_id(1), turn_id(2))
    ));
    assert_eq!(
        fs::read(mirror_path.join(OBJECT_STORE_SYNC_RECEIPT)).unwrap(),
        receipt,
        "recovery pin must not re-baseline already receipted packs"
    );
    assert_eq!(delivery_pack_stats(&mirror_path), (0, 0));
}
