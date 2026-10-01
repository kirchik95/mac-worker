use crate::fixture_pid;

use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read as _, Write as _},
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    agent::{AgentKind, PermissionPolicy, PromptDelivery, TurnLaunch, TurnLimits},
    error::WorkerError,
    host_store::{HostStore, HostStoreWritePoint, SupervisorGuard},
    job::{
        CommandSpec, ExecutionScope, JobState, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LogChunkRequest, LogChunkResponse, LogStream,
        ProcessIdentity, RequestFingerprintMaterial, StatusRequest, StatusResponse, SubmitRequest,
        SubmitResponse,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    process::SystemProcessRunner,
    protocol::{MemoryPressure, PROTOCOL_VERSION},
    supervisor::{ProcessInspector, Supervisor, SupervisorFaultPoint, SystemProcessInspector},
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskSource,
    },
    task_store::{SessionBinding, TaskPrepareRequest, TaskStore},
    turn::{TaskTurnRequest, TaskTurnResponse, TurnMaterial},
};
use sha2::{Digest, Sha256};

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const JOB_ID_B: &str = "028f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN_B: &str = "212f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn material(command: CommandSpec) -> RequestFingerprintMaterial {
    RequestFingerprintMaterial::new(
        JOB_ID.parse().unwrap(),
        CLIENT_ID.parse().unwrap(),
        LEASE_TOKEN.parse().unwrap(),
        3,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        MANIFEST_DIGEST.into(),
        "packages/app".into(),
        30_000,
        "heavy".into(),
        command,
    )
    .unwrap()
}

fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn valid_manifest_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"","#,
            r#""entries":[{{"path":"payload.txt","kind":"file","mode":420,"size":7,"#,
            r#""sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","#,
            r#""symlink_target":null}}],"tracked_deletions":[]}}"#,
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
    )
    .into_bytes()
}

fn wall_clock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock precedes the Unix epoch")
        .as_millis()
        .try_into()
        .expect("system clock is outside the supported range")
}

pub(super) const HOST_SAFETY_TURN_PROMPT: &str = "host safety turn prompt";

// Reuse the task_turn fixture's Git base, scoped lease and TaskStore preparation.
pub(super) fn task_turn_request_on(
    store: &HostStore,
    command: CommandSpec,
) -> (TaskTurnRequest, TaskMeta) {
    task_turn_request_with_identity(
        store,
        command,
        JOB_ID,
        LEASE_TOKEN,
        10,
        30_000,
        TaskId::new(uuid::Uuid::from_u128(17)),
    )
}

fn task_turn_request_with_identity(
    store: &HostStore,
    command: CommandSpec,
    job_id: &str,
    lease_token: &str,
    created_at: u64,
    timeout: u64,
    task_id: TaskId,
) -> (TaskTurnRequest, TaskMeta) {
    let source = crate::support::GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = String::from_utf8(source.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    assert!(
        source
            .git(&[
                "push",
                mirror.path().to_str().unwrap(),
                &format!("HEAD:refs/mac-worker/bases/{task_id}"),
            ])
            .status
            .success()
    );
    let prompt = HOST_SAFETY_TURN_PROMPT;
    let limits = TurnLimits::new(timeout, None, None).unwrap();
    let turn = TurnMaterial::from_prompt(
        task_id,
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        limits.clone(),
        base_oid.clone(),
        prompt,
        None,
        uuid::Uuid::from_u128(18),
        false,
    )
    .unwrap();
    let launch = match command {
        CommandSpec::Argv { argv } => TurnLaunch::new(
            &argv[0],
            argv[1..].to_vec(),
            PromptDelivery::Stdin,
            Vec::new(),
            false,
        ),
        CommandSpec::Shell { shell } => TurnLaunch::new(
            "/bin/sh",
            vec!["-c".into(), shell],
            PromptDelivery::Stdin,
            Vec::new(),
            false,
        ),
    };
    let seed = RequestFingerprintMaterial::new(
        job_id.parse().unwrap(),
        CLIENT_ID.parse().unwrap(),
        lease_token.parse().unwrap(),
        created_at,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        turn.digest(),
        String::new(),
        timeout,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap();
    let seed_lease =
        LeaseRecord::new(&seed, seed.fingerprint(), created_at, created_at + timeout).unwrap();
    let material = turn.v1_material(&seed_lease, &launch).unwrap();
    let request = TaskTurnRequest::new(
        SubmitRequest::new(material).with_execution_scope(ExecutionScope::task(task_id)),
        turn,
        prompt,
    );
    request.validate().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id,
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
        limits: TaskLimits::new(limits, 3).unwrap(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Host Test", "host@example.test").unwrap(),
        title: None,
        prompt: prompt.into(),
        created_at_millis: created_at,
    })
    .unwrap();
    (request, meta)
}

pub(super) fn acquire_task_turn(store: &HostStore, request: &TaskTurnRequest) -> LeaseRecord {
    match LeaseService::new(store)
        .acquire(
            &LeaseAcquireRequest::new(request.submit().material().clone())
                .with_execution_scope(ExecutionScope::task(request.turn().task_id())),
            &healthy(),
            wall_clock_millis(),
        )
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => {
            panic!("fresh task turn was already accepted")
        }
    }
}

pub(super) fn prepare_task_turn(store: &HostStore, request: &TaskTurnRequest, meta: TaskMeta) {
    let job = request.submit().material().job_id();
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(&TaskPrepareRequest::new(meta, job, "mini-1"), &transfer)
        .unwrap();
    bind_task_probe_session(store, request).unwrap();
}

pub(super) fn bind_task_probe_session(
    store: &HostStore,
    request: &TaskTurnRequest,
) -> Result<(), WorkerError> {
    // As in task_turn::existing_session_and_parsed_last_md_still_record_raw_stdout_auth_failure,
    // prebind a fixture session so the original probe's exact stdout stays unchanged.
    TaskStore::new(store, &SystemProcessRunner).bind_session(
        request.submit().material().project_id(),
        request.turn().task_id(),
        SessionBinding::new(
            request.turn().agent(),
            "host-safety-session",
            wall_clock_millis(),
        )?,
    )
}

fn prepared_task_host_with_command(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, TaskTurnRequest) {
    let store = HostStore::open(root).unwrap();
    let (request, meta) = task_turn_request_on(&store, command);
    let lease = acquire_task_turn(&store, &request);
    prepare_task_turn(&store, &request, meta);
    (store, lease, request)
}

// Preserve the original assertions' shared SubmitResponse view while calling the real turn producer.
struct TurnJobService<'a>(JobService<'a>);
impl<'a> TurnJobService<'a> {
    fn new(store: &'a HostStore, launcher: &'a dyn SupervisorLauncher) -> Self {
        Self(JobService::new(store, launcher))
    }
    fn submit_at(
        &self,
        request: TaskTurnRequest,
        _logical_now: u64,
    ) -> Result<SubmitResponse, WorkerError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let response = self.0.submit_turn(request.clone())?;
            if response.submit().status().supervisor_identity().is_some()
                || Instant::now() >= deadline
            {
                return Ok(response.submit().clone());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl<'a> std::ops::Deref for TurnJobService<'a> {
    type Target = JobService<'a>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
trait TurnRequestFields {
    fn material(&self) -> &RequestFingerprintMaterial;
}
impl TurnRequestFields for TaskTurnRequest {
    fn material(&self) -> &RequestFingerprintMaterial {
        self.submit().material()
    }
}
// The agent fixture writes its structured result separately, preserving exact raw stdout/stderr probes.
pub(super) fn task_probe_command(
    store: &HostStore,
    job: mac_worker::job::JobId,
    command: CommandSpec,
) -> CommandSpec {
    let last = store
        .job(PROJECT_ID, WORKTREE_ID, job)
        .unwrap()
        .join("last.md");
    let mut argv = vec!["/bin/sh".into(), "-c".into(),
        r#"umask 077; printf '%s' '$RESULT' > "$1"; shift; exec "$@""#.replace("$RESULT", r#"{"status":"done","summary":"probe complete","questions":[],"files_changed":[],"checks":[]}"#),
        "task-safety-probe".into(), last.to_string_lossy().into_owned()];
    match command {
        CommandSpec::Argv { argv: command } => argv.extend(command),
        CommandSpec::Shell { shell } => argv.extend(["/bin/sh".into(), "-c".into(), shell]),
    }
    CommandSpec::argv(argv).unwrap()
}

fn prepared_turn(root: &Path) -> (HostStore, LeaseRecord, TaskTurnRequest) {
    prepared_turn_with_command(
        root,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    )
}
fn prepared_turn_with_command(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, TaskTurnRequest) {
    prepared_turn_with_command_and_timeout(root, command, 30_000)
}
fn prepared_turn_with_command_and_timeout(
    root: &Path,
    command: CommandSpec,
    timeout: u64,
) -> (HostStore, LeaseRecord, TaskTurnRequest) {
    let store = HostStore::open(root).unwrap();
    let (lease, request) =
        prepared_turn_on_with_timeout(&store, JOB_ID, LEASE_TOKEN, 3, command, timeout);
    (store, lease, request)
}
fn prepared_turn_on(
    store: &HostStore,
    job_id: &str,
    lease_token: &str,
    created_at: u64,
    command: CommandSpec,
) -> (LeaseRecord, TaskTurnRequest) {
    prepared_turn_on_with_timeout(store, job_id, lease_token, created_at, command, 30_000)
}
fn prepared_turn_on_with_timeout(
    store: &HostStore,
    job_id: &str,
    lease_token: &str,
    created_at: u64,
    command: CommandSpec,
    timeout: u64,
) -> (LeaseRecord, TaskTurnRequest) {
    let task = TaskId::new(uuid::Uuid::parse_str(job_id).unwrap());
    let command = task_probe_command(store, job_id.parse().unwrap(), command);
    let (request, meta) = task_turn_request_with_identity(
        store,
        command,
        job_id,
        lease_token,
        created_at,
        timeout,
        task,
    );
    let lease = acquire_task_turn(store, &request);
    prepare_task_turn(store, &request, meta);
    (lease, request)
}
fn publish_unlaunched_turn(root: &Path, request: TaskTurnRequest) {
    let job_id = request.material().job_id();
    let faulted =
        HostStore::open_with_write_fault(root, HostStoreWritePoint::AfterJobIndexParentSync)
            .unwrap();
    assert!(
        JobService::new(&faulted, &RejectLauncher)
            .submit_turn(request)
            .is_err()
    );
    assert!(faulted.job_index(job_id).unwrap().is_file());
}

fn archived_host(root: &Path) -> (HostStore, LeaseRecord, SubmitRequest) {
    archived_host_with_command(
        root,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    )
}
fn archived_host_with_command(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, SubmitRequest) {
    let store = HostStore::open(root).unwrap();
    let manifest = valid_manifest_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let acquire = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            3,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest,
            String::new(),
            30_000,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    let now = wall_clock_millis();
    let lease = super::legacy_archive::lease(&store, &acquire, now);
    super::legacy_archive::snapshot(&store, &lease, &manifest, now);
    let request = SubmitRequest::new(acquire.material().clone());
    super::legacy_archive::accepted(&store, &request, 3, true);
    (store, lease, request)
}

#[test]
fn indexed_payload_without_turn_is_refused_without_execution() {
    assert_turnless_prelaunch_refusal(true, false);
}

#[test]
fn unindexed_payload_without_turn_is_refused_without_execution() {
    assert_turnless_prelaunch_refusal(false, false);
}

#[test]
fn resolving_payload_without_turn_is_refused_without_execution() {
    for indexed in [true, false] {
        assert_turnless_prelaunch_refusal(indexed, true);
    }
}

fn assert_turnless_prelaunch_refusal(indexed: bool, resolve: bool) {
    for task_scope in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("turnless-archive");
        let marker = temp.path().join("must-not-execute");
        let command = CommandSpec::argv(vec![
            "/usr/bin/touch".into(),
            marker.to_string_lossy().into_owned(),
        ])
        .unwrap();
        let (store, lease, request) = archived_host_with_command(&root, command);
        if task_scope {
            let scope = ExecutionScope::task(TaskId::new(uuid::Uuid::from_u128(17)));
            let path = root
                .join("leases/slots")
                .join(
                    LeaseService::new(&store)
                        .occupied_slot_for_job(lease.job_id())
                        .unwrap()
                        .unwrap()
                        .slot_id
                        .to_string(),
                )
                .join("scope.json");
            fs::write(&path, serde_json::to_vec(&scope).unwrap()).unwrap();
            File::open(&path).unwrap().sync_all().unwrap();
        }
        if !indexed {
            let path = store.job_index(lease.job_id()).unwrap();
            fs::remove_file(&path).unwrap();
            File::open(path.parent().unwrap())
                .unwrap()
                .sync_all()
                .unwrap();
        }
        let launcher = CountingInlineSupervisorLauncher {
            store: store.clone(),
            launches: Arc::new(AtomicUsize::new(0)),
        };
        let service = JobService::new(&store, &launcher);
        let error = if resolve {
            service
                .resolve_or_abandon(
                    mac_worker::job::ResolveOrAbandonRequest::from_submit_request(&request)
                        .unwrap(),
                )
                .unwrap_err()
        } else {
            service.status(lease.job_id()).unwrap_err()
        };
        assert_protocol_code(error, "EXECUTION_SCOPE_CONFLICT");
        let response = service.status(lease.job_id()).unwrap();
        assert_eq!(response.status().state(), JobState::Lost);
        assert_eq!(
            response.status().error_code(),
            Some("EXECUTION_SCOPE_CONFLICT")
        );
        assert!(response.status().supervisor_identity().is_some());
        assert_eq!(response.status().child_identity(), None);
        assert_eq!(launcher.launches.load(Ordering::SeqCst), 1);
        assert!(!marker.exists(), "turnless payload executed a user command");
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        assert!(!job.join("execution.json").exists());
        assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"");
        assert_eq!(fs::read(job.join("stderr.log")).unwrap(), b"");
        assert_eq!(LeaseService::new(&store).load().unwrap(), None);
        assert!(store.job_index(lease.job_id()).unwrap().is_file());
        let before = durable_tree(&root);
        assert_eq!(service.status(lease.job_id()).unwrap(), response);
        assert_eq!(durable_tree(&root), before);
        assert_eq!(launcher.launches.load(Ordering::SeqCst), 1);
        assert!(!marker.exists());
    }
}

fn seed_archived_succeeded_status(store: &HostStore, lease: &LeaseRecord) {
    let path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap()
        .join("status.json");
    let status: JobStatus = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let status = status
        .with_supervisor(
            ProcessIdentity::new(fixture_pid::fixture_pid(42_901), 42_901).unwrap(),
            11,
        )
        .unwrap()
        .with_child(
            ProcessIdentity::new(fixture_pid::fixture_pid(42_902), 42_902).unwrap(),
            12,
        )
        .unwrap()
        .into_running(13)
        .unwrap()
        .into_succeeded(14, 0, 0)
        .unwrap();
    fs::write(&path, serde_json::to_vec(&status).unwrap()).unwrap();
    File::open(&path).unwrap().sync_all().unwrap();
}

fn compile_exit_parking_dylib(directory: &Path) -> PathBuf {
    let source = directory.join("exit-parking.c");
    let library = directory.join("libexit-parking.dylib");
    fs::write(
        &source,
        concat!(
            "#include <fcntl.h>\n",
            "#include <stdlib.h>\n",
            "#include <unistd.h>\n",
            "__attribute__((destructor))\n",
            "static void park_accepted_client_at_exit(void) {\n",
            "  const char *path = getenv(\"MAC_WORKER_TEST_CLIENT_EXIT_FIFO\");\n",
            "  if (path == NULL) return;\n",
            "  int fd = open(path, O_RDONLY);\n",
            "  if (fd >= 0) close(fd);\n",
            "}\n",
        ),
    )
    .unwrap();
    let output = Command::new("/usr/bin/cc")
        .args(["-dynamiclib", "-o"])
        .arg(&library)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "failed to compile accepted-client exit parking library: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    library
}

fn create_fifo(path: &Path) {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
}

struct DirectChildCleanup {
    child: Option<Child>,
}

impl DirectChildCleanup {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().unwrap()
    }

    fn kill_and_reap(&mut self) -> ExitStatus {
        let child = self.child.as_mut().unwrap();
        let pid = child.id() as libc::pid_t;
        assert_eq!(
            unsafe { libc::kill(pid, libc::SIGKILL) },
            0,
            "exact-PID kill of the disconnecting client must succeed"
        );
        let status = child.wait().unwrap();
        self.child = None;
        status
    }
}

impl Drop for DirectChildCleanup {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct DetachedJobCleanup {
    job: PathBuf,
    armed: bool,
}

impl DetachedJobCleanup {
    fn new(job: PathBuf) -> Self {
        Self { job, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DetachedJobCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(bytes) = fs::read(self.job.join("status.json")) else {
            return;
        };
        let Ok(status) = serde_json::from_slice::<JobStatus>(&bytes) else {
            return;
        };
        let inspector = SystemProcessInspector;
        for identity in [status.child_identity(), status.supervisor_identity()]
            .into_iter()
            .flatten()
        {
            if matches!(
                inspector.observe(identity),
                mac_worker::supervisor::ProcessObservation::Matching { .. }
            ) {
                let _ = unsafe { libc::kill(identity.pid() as libc::pid_t, libc::SIGKILL) };
            }
        }
    }
}

fn try_host_control<Request, Response>(
    home: &Path,
    data: &Path,
    operation: &str,
    request: &Request,
) -> Result<Response, String>
where
    Request: serde::Serialize,
    Response: serde::de::DeserializeOwned,
{
    let mut child = Command::new(env!("CARGO_BIN_EXE_worker"))
        .env_clear()
        .env("HOME", home)
        .env("XDG_DATA_HOME", data)
        .args(["host", operation])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    child
        .stdin
        .take()
        .ok_or_else(|| "host stdin was not piped".to_owned())?
        .write_all(&serde_json::to_vec(request).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    let output = child
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

struct RejectLauncher;

struct InlineSupervisorLauncher {
    store: HostStore,
}

const TEST_TERM_GRACE: Duration = Duration::from_millis(250);

struct TimedSupervisorLauncher {
    store: HostStore,
    term_grace: Option<Duration>,
}

impl SupervisorLauncher for TimedSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        let mut supervisor = Supervisor::new(&self.store, &inspector)
            .with_prepare_turn_helper(PathBuf::from(env!("CARGO_BIN_EXE_worker")));
        if let Some(grace) = self.term_grace {
            supervisor = supervisor.with_term_grace(grace);
        }
        supervisor.run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

struct FaultingInlineSupervisorLauncher {
    store: HostStore,
    point: SupervisorFaultPoint,
}

struct TamperingInlineSupervisorLauncher {
    store: HostStore,
    job_path: PathBuf,
}

impl SupervisorLauncher for RejectLauncher {
    fn launch(
        &self,
        _job_id: mac_worker::job::JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        panic!("peer fixture publication must not launch a supervisor")
    }
}

impl SupervisorLauncher for InlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector)
            .with_prepare_turn_helper(PathBuf::from(env!("CARGO_BIN_EXE_worker")))
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

impl SupervisorLauncher for FaultingInlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new_with_fault(&self.store, &inspector, self.point)
            .with_prepare_turn_helper(PathBuf::from(env!("CARGO_BIN_EXE_worker")))
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

impl SupervisorLauncher for TamperingInlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let payload_path = self.job_path.join("execution.json");
        let mut bytes = fs::read(&payload_path)?;
        let original = b"/bin/true";
        let replacement = b"/bin/echo";
        let offset = bytes
            .windows(original.len())
            .position(|window| window == original)
            .ok_or_else(|| {
                mac_worker::error::WorkerError::Protocol(
                    "test execution payload did not contain its command".into(),
                )
            })?;
        bytes[offset..offset + original.len()].copy_from_slice(replacement);
        let temporary = self.job_path.join(".test-tampered-execution");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &payload_path)?;
        File::open(&self.job_path)?.sync_all()?;

        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector)
            .with_prepare_turn_helper(PathBuf::from(env!("CARGO_BIN_EXE_worker")))
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

struct RecordingLauncher {
    launches: Arc<AtomicUsize>,
    job_path: PathBuf,
    identity: ProcessIdentity,
}

struct FailingLauncher {
    launches: Arc<AtomicUsize>,
}

struct CapturedPreidentityLauncher {
    held: Arc<Mutex<Option<SupervisorGuard>>>,
    entered: mpsc::Sender<()>,
}

struct CountingInlineSupervisorLauncher {
    store: HostStore,
    launches: Arc<AtomicUsize>,
}

struct MarkingInlineSupervisorLauncher {
    store: HostStore,
    launches: Arc<AtomicUsize>,
    marker: PathBuf,
}

impl SupervisorLauncher for FailingLauncher {
    fn launch(
        &self,
        _job_id: mac_worker::job::JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        Err(mac_worker::error::WorkerError::Protocol(
            "injected launcher failure".into(),
        ))
    }
}

impl SupervisorLauncher for CapturedPreidentityLauncher {
    fn launch(
        &self,
        _job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        *self.held.lock().unwrap() = Some(guard);
        self.entered.send(()).unwrap();
        Ok(LaunchCandidate::new(
            ProcessIdentity::new(fixture_pid::fixture_pid(99_001), 9_900_001).unwrap(),
        ))
    }
}

impl SupervisorLauncher for CountingInlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector)
            .with_prepare_turn_helper(PathBuf::from(env!("CARGO_BIN_EXE_worker")))
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

impl SupervisorLauncher for MarkingInlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        fs::write(&self.marker, b"launched")?;
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector)
            .with_prepare_turn_helper(PathBuf::from(env!("CARGO_BIN_EXE_worker")))
            .run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

impl SupervisorLauncher for RecordingLauncher {
    fn launch(
        &self,
        _job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        let bytes = fs::read(self.job_path.join("status.json"))?;
        let status: JobStatus = serde_json::from_slice(&bytes)
            .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))?;
        let status = status.with_supervisor(self.identity, 4)?;
        let replacement = self.job_path.join(".test-launcher-status");
        let mut replacement_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&replacement)?;
        replacement_file.write_all(&serde_json::to_vec(&status).unwrap())?;
        replacement_file.sync_all()?;
        fs::rename(&replacement, self.job_path.join("status.json"))?;
        File::open(&self.job_path)?.sync_all()?;
        drop(guard);
        Ok(LaunchCandidate::new(self.identity))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DurableTreeEntry {
    Directory { mode: u32 },
    File { mode: u32, bytes: Vec<u8> },
    Symlink { target: PathBuf },
}

fn durable_tree(root: &Path) -> BTreeMap<PathBuf, DurableTreeEntry> {
    fn walk(root: &Path, directory: &Path, entries: &mut BTreeMap<PathBuf, DurableTreeEntry>) {
        let mut children = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for path in children {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            let metadata = fs::symlink_metadata(&path).unwrap();
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                entries.insert(
                    relative,
                    DurableTreeEntry::Symlink {
                        target: fs::read_link(&path).unwrap(),
                    },
                );
            } else if file_type.is_dir() {
                entries.insert(
                    relative,
                    DurableTreeEntry::Directory {
                        mode: metadata.permissions().mode() & 0o7777,
                    },
                );
                walk(root, &path, entries);
            } else if file_type.is_file() {
                entries.insert(
                    relative,
                    DurableTreeEntry::File {
                        mode: metadata.permissions().mode() & 0o7777,
                        bytes: fs::read(&path).unwrap(),
                    },
                );
            } else {
                panic!("unexpected durable entry type at {}", path.display());
            }
        }
    }

    let mut entries = BTreeMap::new();
    walk(root, root, &mut entries);
    entries
}

fn assert_protocol_code(error: WorkerError, expected: &str) {
    match error {
        WorkerError::Protocol(message) => {
            let actual = message
                .split_once(':')
                .map_or(message.as_str(), |(code, _)| code);
            assert_eq!(actual, expected, "unexpected protocol error: {message}");
        }
        other => panic!("expected protocol error {expected}, got {other}"),
    }
}

#[test]
fn status_enrichment_and_terminal_outcomes_preserve_sticky_process_identities() {
    let supervisor = ProcessIdentity::new(fixture_pid::fixture_pid(101), 1_000_001).unwrap();
    let child = ProcessIdentity::new(fixture_pid::fixture_pid(202), 2_000_002).unwrap();

    let accepted = JobStatus::accepted(10).unwrap();
    let supervised = accepted.with_supervisor(supervisor, 11).unwrap();
    let ready = supervised.with_child(child, 12).unwrap();
    let running = ready.into_running(13).unwrap();
    let succeeded = running.clone().into_succeeded(14, 7, 9).unwrap();
    let signalled = running.into_failed_signal(15, 15, 11, 13).unwrap();

    assert_eq!(succeeded.state(), JobState::Succeeded);
    assert_eq!(succeeded.supervisor_identity(), Some(supervisor));
    assert_eq!(succeeded.child_identity(), Some(child));
    assert_eq!(succeeded.exit_code(), Some(0));
    assert_eq!(succeeded.terminating_signal(), None);
    assert_eq!(signalled.supervisor_identity(), Some(supervisor));
    assert_eq!(signalled.child_identity(), Some(child));
    assert_eq!(signalled.exit_code(), None);
    assert_eq!(signalled.terminating_signal(), Some(15));

    assert!(supervised.with_supervisor(supervisor, 12).is_err());
    assert!(ready.with_child(child, 13).is_err());
}

#[test]
fn status_rejects_inconsistent_signal_identity_and_cleanup_shapes() {
    let supervisor = ProcessIdentity::new(fixture_pid::fixture_pid(101), 1_000_001).unwrap();
    let child = ProcessIdentity::new(fixture_pid::fixture_pid(202), 2_000_002).unwrap();
    let running = JobStatus::accepted(10)
        .unwrap()
        .with_supervisor(supervisor, 11)
        .unwrap()
        .with_child(child, 12)
        .unwrap()
        .into_running(13)
        .unwrap();
    let failed = running.into_failed_exit(14, 7, 3, 4).unwrap();
    let cleanup_failed = failed
        .clone()
        .with_cleanup_error("CLEANUP_IO".into(), 15)
        .unwrap();

    assert_eq!(cleanup_failed.state(), JobState::Failed);
    assert_eq!(cleanup_failed.exit_code(), Some(7));
    assert_eq!(cleanup_failed.cleanup_error_code(), Some("CLEANUP_IO"));
    assert!(
        cleanup_failed
            .with_cleanup_error("SECOND".into(), 16)
            .is_err()
    );

    let value = serde_json::to_value(&failed).unwrap();
    let mut both_outcomes = value.clone();
    both_outcomes["terminating_signal"] = serde_json::json!(15);
    assert!(serde_json::from_value::<JobStatus>(both_outcomes).is_err());

    let mut child_without_supervisor = value;
    child_without_supervisor["supervisor_pid"] = serde_json::Value::Null;
    child_without_supervisor["supervisor_start_identity"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<JobStatus>(child_without_supervisor).is_err());

    let mut partial_nonterminal_lengths =
        serde_json::to_value(JobStatus::accepted(10).unwrap()).unwrap();
    partial_nonterminal_lengths["final_stdout_bytes"] = serde_json::json!(1);
    assert!(serde_json::from_value::<JobStatus>(partial_nonterminal_lengths).is_err());

    assert!(
        JobStatus::accepted(10)
            .unwrap()
            .transition(JobStatus::running(11, 101, 1_000_001, 202, 2_000_002).unwrap())
            .is_err(),
        "running must not bypass the two durable Accepted identity enrichments"
    );
}

#[test]
fn status_wire_is_canonical_strict_and_rejects_duplicate_fields() {
    let status = JobStatus::accepted(10).unwrap();
    assert_eq!(
        serde_json::to_string(&status).unwrap(),
        r#"{"state":"accepted","updated_at_millis":10,"supervisor_pid":null,"supervisor_start_identity":null,"child_pid":null,"child_start_identity":null,"exit_code":null,"terminating_signal":null,"final_stdout_bytes":null,"final_stderr_bytes":null,"error_code":null,"cleanup_error_code":null}"#
    );
    assert_eq!(
        serde_json::from_str::<JobStatus>(&serde_json::to_string(&status).unwrap()).unwrap(),
        status
    );
    assert!(
        serde_json::from_str::<JobStatus>(
            r#"{"state":"accepted","state":"accepted","updated_at_millis":10,"supervisor_pid":null,"supervisor_start_identity":null,"child_pid":null,"child_start_identity":null,"exit_code":null,"terminating_signal":null,"final_stdout_bytes":null,"final_stderr_bytes":null,"error_code":null,"cleanup_error_code":null}"#,
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<JobStatus>(
            r#"{"state":"accepted","updated_at_millis":10,"supervisor_pid":null,"supervisor_start_identity":null,"child_pid":null,"child_start_identity":null,"exit_code":null,"terminating_signal":null,"final_stdout_bytes":null,"final_stderr_bytes":null,"error_code":null,"cleanup_error_code":null,"extra":true}"#,
        )
        .is_err()
    );
}

#[test]
fn every_command_bearing_debug_is_content_free() {
    let marker = "payload-should-never-appear";
    let command = CommandSpec::shell(marker.into()).unwrap();
    let material = material(command.clone());
    let request = SubmitRequest::new(material.clone());

    for rendered in [
        format!("{command:?}"),
        format!("{material:?}"),
        format!("{request:?}"),
    ] {
        assert!(!rendered.contains(marker), "{rendered}");
        assert!(!rendered.contains(LEASE_TOKEN), "{rendered}");
    }
    assert_eq!(PROTOCOL_VERSION, 7);
}

#[test]
fn supervisor_recomputes_the_fingerprint_from_exact_command_and_cwd_before_fork() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn_with_command(
        &temp.path().join("tampered-command"),
        CommandSpec::argv(vec!["/bin/true".into()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = TamperingInlineSupervisorLauncher {
        store: store.clone(),
        job_path: job.clone(),
    };

    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 3)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Lost);
    assert!(status.supervisor_identity().is_some());
    assert!(status.child_identity().is_none());
    assert_eq!(status.error_code(), Some("EXECUTION_PAYLOAD_INVALID"));
    assert!(!job.join("execution.json").exists());
    assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn submit_publishes_complete_job_before_index_and_is_idempotent_after_identity() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("host"));
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let identity = ProcessIdentity::new(fixture_pid::fixture_pid(41_001), 4_100_001).unwrap();
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity,
    };
    let service = TurnJobService::new(&store, &launcher);

    let first = service.submit_at(request.clone(), 3).unwrap();
    assert_eq!(first.status().supervisor_identity(), Some(identity));
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    let names = fs::read_dir(&job_path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        names,
        [
            "execution.json",
            "meta.json",
            "status.json",
            "stderr.log",
            "supervisor.log",
            "stdout.log",
            "tmp",
            "prompt.md",
            "result.schema.json",
            "tail.log",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    );
    assert!(store.job_index(lease.job_id()).unwrap().is_file());
    for log in ["stdout.log", "stderr.log"] {
        let metadata = fs::metadata(job_path.join(log)).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(metadata.len(), 0);
    }

    let second = service.submit_at(request, 5).unwrap();
    assert_eq!(second.status().supervisor_identity(), Some(identity));
    assert_eq!(launches.load(Ordering::SeqCst), 1);
}

#[test]
fn retired_child_terminal_retries_remain_existing_without_relaunching() {
    // Break caught: applying the prelaunch retry rejection to every terminal
    // would reject legitimate completed commands after lease retirement.
    for (label, program, expected_state, expected_exit) in [
        ("succeeded", "/usr/bin/true", JobState::Succeeded, 0),
        ("failed", "/usr/bin/false", JobState::Failed, 1),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(label);
        let retry_marker = temp.path().join(format!("{label}-retry-launch-marker"));
        let (store, lease, request) =
            prepared_turn_with_command(&root, CommandSpec::argv(vec![program.into()]).unwrap());
        let first_launcher = InlineSupervisorLauncher {
            store: store.clone(),
        };

        let first = TurnJobService::new(&store, &first_launcher)
            .submit_at(request.clone(), 20)
            .unwrap();

        assert_eq!(first.status().state(), expected_state, "{label}");
        assert_eq!(first.status().exit_code(), Some(expected_exit), "{label}");
        assert!(first.status().supervisor_identity().is_some(), "{label}");
        assert!(first.status().child_identity().is_some(), "{label}");
        assert_eq!(LeaseService::new(&store).load().unwrap(), None, "{label}");
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        assert!(!job.join("execution.json").exists(), "{label}");
        let durable_before = durable_tree(&root);
        let retry_launches = Arc::new(AtomicUsize::new(0));
        let retry_launcher = MarkingInlineSupervisorLauncher {
            store: store.clone(),
            launches: Arc::clone(&retry_launches),
            marker: retry_marker.clone(),
        };

        let retry = TurnJobService::new(&store, &retry_launcher)
            .submit_at(request, 21)
            .unwrap();

        assert!(matches!(retry, SubmitResponse::Existing { .. }), "{label}");
        assert_eq!(retry.status(), first.status(), "{label}");
        assert_eq!(retry_launches.load(Ordering::SeqCst), 0, "{label}");
        assert!(!retry_marker.exists(), "{label}");
        assert_eq!(LeaseService::new(&store).load().unwrap(), None, "{label}");
        assert!(!job.join("execution.json").exists(), "{label}");
        assert_eq!(durable_tree(&root), durable_before, "{label}");
    }
}

#[test]
fn submit_conflicts_on_changed_request_and_honours_an_abandonment_tombstone() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("conflict-host"));
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: ProcessIdentity::new(fixture_pid::fixture_pid(41_002), 4_100_002).unwrap(),
    };
    let changed_submit = SubmitRequest::new(
        RequestFingerprintMaterial::new(
            request.material().job_id(),
            request.material().client_id(),
            request.material().lease_token(),
            request.material().created_at_millis(),
            request.material().worker_name().into(),
            request.material().project_id().into(),
            request.material().worktree_id().into(),
            request.material().manifest_digest().into(),
            request.material().relative_working_dir().into(),
            request.material().timeout_millis(),
            request.material().resource_class().into(),
            CommandSpec::argv(vec!["/usr/bin/false".into()]).unwrap(),
        )
        .unwrap(),
    );
    let changed = TaskTurnRequest::new(
        changed_submit.with_execution_scope(request.submit().execution_scope().clone()),
        request.turn().clone(),
        HOST_SAFETY_TURN_PROMPT,
    );
    let error = TurnJobService::new(&store, &launcher)
        .submit_at(changed, 3)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(launches.load(Ordering::SeqCst), 0);

    let abandoned_root = temp.path().join("abandoned-host");
    let (abandoned_store, abandoned_lease, abandoned_request) = prepared_turn(&abandoned_root);
    abandoned_store
        .record_abandoned(
            &LeaseAcquireRequest::new(abandoned_request.material().clone())
                .with_execution_scope(abandoned_request.submit().execution_scope().clone()),
            3,
        )
        .unwrap();
    let abandoned_launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: abandoned_store
            .job(
                abandoned_lease.project_id(),
                abandoned_lease.worktree_id(),
                abandoned_lease.job_id(),
            )
            .unwrap(),
        identity: ProcessIdentity::new(fixture_pid::fixture_pid(41_003), 4_100_003).unwrap(),
    };
    let error = TurnJobService::new(&abandoned_store, &abandoned_launcher)
        .submit_at(abandoned_request, 4)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ABANDONED"));
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[test]
fn submit_treats_a_mismatched_same_job_abandonment_as_a_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("host"));
    store
        .record_abandoned(
            &LeaseAcquireRequest::new(request.material().clone())
                .with_execution_scope(request.submit().execution_scope().clone()),
            3,
        )
        .unwrap();
    let disposition_path = store.job_index(request.material().job_id()).unwrap();
    let disposition = fs::read_to_string(&disposition_path).unwrap();
    fs::write(
        disposition_path,
        disposition.replacen(
            &request.material().client_id().to_string(),
            "ffffffffffffffffffffffffffffffff",
            1,
        ),
    )
    .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: ProcessIdentity::new(fixture_pid::fixture_pid(41_004), 4_100_004).unwrap(),
    };

    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 4)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[test]
fn recovering_one_faulted_slot_leaves_the_other_lease_workspace_and_token() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("slot-recovery-isolation");
    let marker = temp.path().join("executions");
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf x >> \"$1\"".into(),
        "slot-recovery".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let store = HostStore::open(&root).unwrap();
    LeaseService::new(&store).set_slot_count(2).unwrap();
    let (lease_a, request_a) = prepared_turn_on(&store, JOB_ID, LEASE_TOKEN, 3, command);
    let (lease_b, request_b) = prepared_turn_on(
        &store,
        JOB_ID_B,
        LEASE_TOKEN_B,
        4,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    );
    drop(store);
    publish_unlaunched_turn(&root, request_b.clone());
    let store = HostStore::open(&root).unwrap();
    let workspace_b = store
        .task_workspace(lease_b.project_id(), request_b.turn().task_id())
        .unwrap();
    assert!(workspace_b.is_dir());
    drop(store);

    let now = wall_clock_millis();
    for lease in [&lease_a, &lease_b] {
        assert!(
            lease.expires_at_millis() > now,
            "slot recovery must start with a live execution lease: {lease:?}; now={now}"
        );
    }
    let faulted = HostStore::open(&root).unwrap();
    let launcher = FaultingInlineSupervisorLauncher {
        store: faulted.clone(),
        point: SupervisorFaultPoint::AfterTerminalStatus,
    };
    assert!(
        TurnJobService::new(&faulted, &launcher)
            .submit_at(request_a.clone(), 10)
            .is_err()
    );
    let live_b = LeaseService::new(&faulted)
        .load_for_job(lease_b.job_id())
        .unwrap()
        .expect("peer slot lease must remain during faulted recovery");
    assert_eq!(live_b, lease_b);
    assert_eq!(live_b.lease_token(), lease_b.lease_token());
    assert!(workspace_b.is_dir());
    drop(launcher);
    drop(faulted);

    let recovered = HostStore::open(&root).unwrap();
    let retry = InlineSupervisorLauncher {
        store: recovered.clone(),
    };
    let response = TurnJobService::new(&recovered, &retry)
        .submit_at(request_a, 11)
        .unwrap_or_else(|error| panic!("faulted slot was not recoverable: {error}"));
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    // Submit of an already-supervised terminal job is Existing only. Cleanup
    // and exact lease release are the host status / reconcile path.
    let cleaned = TurnJobService::new(&recovered, &RejectLauncher)
        .status(lease_a.job_id())
        .unwrap_or_else(|error| panic!("terminal slot status cleanup failed: {error}"));
    assert_eq!(cleaned.status().state(), JobState::Succeeded);
    assert_eq!(fs::read(&marker).unwrap(), b"x");
    let live_b = LeaseService::new(&recovered)
        .load_for_job(lease_b.job_id())
        .unwrap()
        .expect("peer slot lease must remain after recovery");
    assert_eq!(live_b, lease_b);
    assert_eq!(live_b.lease_token(), lease_b.lease_token());
    assert!(workspace_b.is_dir());
    assert_eq!(
        LeaseService::new(&recovered)
            .load_for_job(lease_a.job_id())
            .unwrap(),
        None
    );
}

#[test]
fn conflicting_index_staging_is_preserved_and_never_adopted() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("index-staging-conflict");
    let marker = temp.path().join("must-not-run");
    let command = CommandSpec::argv(vec![
        "/usr/bin/touch".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_turn_with_command(&root, command);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobIndexFileSync)
            .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: faulted.clone(),
    };
    assert!(
        TurnJobService::new(&faulted, &launcher)
            .submit_at(request.clone(), 10)
            .is_err()
    );
    drop(launcher);
    drop(faulted);

    let staging = root
        .join("job-index")
        .join(format!(".accept-{}.json", lease.job_id()));
    let bytes = fs::read(&staging).unwrap();
    let bytes = String::from_utf8(bytes)
        .unwrap()
        .replace(
            "\"supervisor_pid\":null,\"supervisor_start_identity\":null",
            "\"supervisor_pid\":9001,\"supervisor_start_identity\":9002",
        )
        .into_bytes();
    fs::write(&staging, bytes).unwrap();

    let reopened = HostStore::open(&root).unwrap();
    let retry = InlineSupervisorLauncher {
        store: reopened.clone(),
    };
    let error = TurnJobService::new(&reopened, &retry)
        .submit_at(request, 11)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert!(staging.is_file());
    assert!(!reopened.job_index(lease.job_id()).unwrap().exists());
    assert!(!marker.exists());
    assert_eq!(
        LeaseService::new(&reopened).load().unwrap(),
        Some(lease.clone())
    );
}

#[test]
fn accepted_index_cannot_claim_a_lifecycle_identity_and_trigger_launch() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let marker = temp.path().join("must-not-run");
    let command = CommandSpec::argv(vec![
        "/usr/bin/touch".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_turn_with_command(&root, command);
    let failing = FailingLauncher {
        launches: Arc::new(AtomicUsize::new(0)),
    };
    assert!(
        TurnJobService::new(&store, &failing)
            .submit_at(request.clone(), 3)
            .is_err()
    );

    let index = store.job_index(lease.job_id()).unwrap();
    let bytes = String::from_utf8(fs::read(&index).unwrap())
        .unwrap()
        .replace(
            "\"supervisor_pid\":null,\"supervisor_start_identity\":null",
            "\"supervisor_pid\":9001,\"supervisor_start_identity\":9002",
        );
    fs::write(&index, bytes).unwrap();

    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 4)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert!(!marker.exists());
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
}

#[test]
fn unsafe_preexisting_final_evidence_is_preserved_and_never_indexed_or_launched() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("host"));
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    fs::create_dir_all(&job_path).unwrap();
    for private in [
        job_path.parent().unwrap().parent().unwrap(),
        job_path.parent().unwrap(),
        job_path.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(job_path.join("foreign-evidence"), b"retain-me").unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity: ProcessIdentity::new(fixture_pid::fixture_pid(41_005), 4_100_005).unwrap(),
    };

    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 3)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(
        fs::read(job_path.join("foreign-evidence")).unwrap(),
        b"retain-me"
    );
    assert!(!store.job_index(lease.job_id()).unwrap().exists());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[test]
fn one_hundred_concurrent_identical_submits_publish_and_launch_exactly_once() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("host"));
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity: ProcessIdentity::new(fixture_pid::fixture_pid(41_006), 4_100_006).unwrap(),
    };
    let service = TurnJobService::new(&store, &launcher);
    let barrier = Arc::new(Barrier::new(100));

    std::thread::scope(|scope| {
        let handles = (0..100)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let request = request.clone();
                let service = &service;
                scope.spawn(move || {
                    barrier.wait();
                    service.submit_at(request, 3).unwrap()
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            let response = handle.join().unwrap();
            assert_eq!(
                response.status().supervisor_identity(),
                Some(ProcessIdentity::new(fixture_pid::fixture_pid(41_006), 4_100_006).unwrap())
            );
        }
    });

    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(job_path.is_dir());
    assert!(store.job_index(lease.job_id()).unwrap().is_file());
}

#[test]
fn dead_preidentity_owner_is_reelected_once_after_the_bounded_wait() {
    let temp = tempfile::tempdir().unwrap();
    let (store, _lease, request) = prepared_turn(&temp.path().join("dead-owner"));
    let held = Arc::new(Mutex::new(None));
    let (entered, observed) = mpsc::channel();
    let first_store = store.clone();
    let first_request = request.clone();
    let first_held = Arc::clone(&held);
    let first = std::thread::spawn(move || {
        let launcher = CapturedPreidentityLauncher {
            held: first_held,
            entered,
        };
        TurnJobService::new(&first_store, &launcher).submit_at(first_request, 10)
    });
    observed
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();

    let launches = Arc::new(AtomicUsize::new(0));
    let retry_store = store.clone();
    let retry_request = request.clone();
    let retry_launches = Arc::clone(&launches);
    let retry = std::thread::spawn(move || {
        let launcher = CountingInlineSupervisorLauncher {
            store: retry_store.clone(),
            launches: retry_launches,
        };
        TurnJobService::new(&retry_store, &launcher).submit_at(retry_request, 11)
    });
    std::thread::sleep(Duration::from_millis(250));
    drop(held.lock().unwrap().take());

    let response = retry.join().unwrap().unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert!(response.status().supervisor_identity().is_some());
    assert!(response.status().child_identity().is_some());
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert!(first.join().unwrap().is_err());
}

#[test]
fn still_busy_preidentity_owner_remains_ambiguous_without_a_second_launch() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("busy-owner"));
    let held = Arc::new(Mutex::new(None));
    let (entered, observed) = mpsc::channel();
    let first_store = store.clone();
    let first_request = request.clone();
    let first_held = Arc::clone(&held);
    let first = std::thread::spawn(move || {
        let launcher = CapturedPreidentityLauncher {
            held: first_held,
            entered,
        };
        TurnJobService::new(&first_store, &launcher).submit_at(first_request, 10)
    });
    observed
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .unwrap();

    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = FailingLauncher {
        launches: Arc::clone(&launches),
    };
    let response = TurnJobService::new(&store, &launcher)
        .submit_at(request, 11)
        .unwrap();

    assert_eq!(response.status().state(), JobState::Accepted);
    assert!(response.status().supervisor_identity().is_none());
    assert!(response.status().child_identity().is_none());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
    drop(held.lock().unwrap().take());
    assert!(first.join().unwrap().is_err());
}

#[test]
fn real_execution_records_exit_signal_timeout_and_binary_split_logs() {
    let temp = tempfile::tempdir().unwrap();
    let cases = [
        (
            "exit-seven",
            CommandSpec::argv(vec!["/bin/sh".into(), "-c".into(), "exit 7".into()]).unwrap(),
            30_000,
            JobState::Failed,
            Some(7),
            None,
        ),
        (
            "signal-term",
            CommandSpec::argv(vec!["/bin/sh".into(), "-c".into(), "kill -TERM $$".into()]).unwrap(),
            30_000,
            JobState::Failed,
            None,
            Some(15),
        ),
        (
            "timeout",
            CommandSpec::argv(vec!["/bin/sleep".into(), "60".into()]).unwrap(),
            25,
            JobState::TimedOut,
            None,
            None,
        ),
    ];

    for (name, command, timeout, state, exit, signal) in cases {
        let (store, lease, request) =
            prepared_turn_with_command_and_timeout(&temp.path().join(name), command, timeout);
        let launcher = TimedSupervisorLauncher {
            store: store.clone(),
            term_grace: Some(TEST_TERM_GRACE),
        };
        let response = TurnJobService::new(&store, &launcher)
            .submit_at(request, 10)
            .unwrap();
        assert_eq!(response.status().state(), state, "{name}");
        assert_eq!(response.status().exit_code(), exit, "{name}");
        assert_eq!(response.status().terminating_signal(), signal, "{name}");
        assert!(
            LeaseService::new(&store).load().unwrap().is_none(),
            "{name}"
        );
        let job = store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap();
        assert!(!job.join("execution.json").exists(), "{name}");
    }

    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf 'out\\000bytes'; printf 'err\\000bytes' >&2".into(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_turn_with_command(&temp.path().join("binary-logs"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = TurnJobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"out\0bytes");
    assert_eq!(fs::read(job.join("stderr.log")).unwrap(), b"err\0bytes");
    assert_eq!(response.status().final_stdout_bytes(), Some(9));
    assert_eq!(response.status().final_stderr_bytes(), Some(9));
}

#[test]
fn terminal_status_counts_stderr_written_after_the_leader_exits() {
    // The leader writes stdout, then exits while a setsid grandchild still
    // holds stderr and writes after a short delay. Accounting that stats the
    // log as soon as waitpid returns records zero; joining the copy until EOF
    // (with a bound for a writer that never closes) keeps the late bytes.
    let temp = tempfile::tempdir().unwrap();
    let stdout_length = 65_536 + 17;
    let stderr_length = 65_536 + 9;
    let command = CommandSpec::argv(vec![
        "/usr/bin/python3".into(),
        "-c".into(),
        format!(
            "import os, time\nos.write(1, b'\\0' * {stdout_length})\nchild = os.fork()\nif child == 0:\n    os.setsid()\n    time.sleep(0.2)\n    os.write(2, b'\\0' * {stderr_length})\n    os._exit(0)\nos._exit(0)\n"
        ),
    ])
    .unwrap();
    let (store, _lease, request) =
        prepared_turn_with_command(&temp.path().join("late-stderr"), command);
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = TurnJobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(
        response.status().final_stdout_bytes(),
        Some(stdout_length as u64)
    );
    assert_eq!(
        response.status().final_stderr_bytes(),
        Some(stderr_length as u64)
    );
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn terminal_status_includes_a_one_byte_stdout_write_before_publication() {
    // Same publication order as terminal_accepted_submit_retry_after_lease_retirement:
    // the child's one-byte write must be in the log before terminal status.
    let temp = tempfile::tempdir().unwrap();
    let command = CommandSpec::argv(vec!["/usr/bin/printf".into(), "x".into()]).unwrap();
    let (store, lease, request) =
        prepared_turn_with_command(&temp.path().join("one-byte-stdout"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = TurnJobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(response.status().final_stdout_bytes(), Some(1));
    assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"x");
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn timeout_keeps_a_waitable_leader_anchor_then_kills_and_proves_the_group_absent() {
    assert_timeout_group_cleanup(Some(TEST_TERM_GRACE));
}

// Nightly: CARGO_BUILD_JOBS=4 cargo nextest run --locked --test host --run-ignored only -E 'test(=supervisor::production_term_grace_stress)'
#[test]
#[ignore = "real production TERM grace; nightly stress check"]
fn production_term_grace_stress() {
    assert_timeout_group_cleanup(None);
}

fn assert_timeout_group_cleanup(term_grace: Option<Duration>) {
    let grace = term_grace.unwrap_or(mac_worker::supervisor::SUPERVISOR_TERM_GRACE);
    let temp = tempfile::tempdir().unwrap();
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "trap 'exit 0' TERM; /bin/sh -c 'trap \"\" TERM; while :; do sleep 1; done' & echo $!; wait"
            .into(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_turn_with_command_and_timeout(&temp.path().join("surviving-group"), command, 100);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = TimedSupervisorLauncher {
        store: store.clone(),
        term_grace,
    };

    let started = Instant::now();
    let result = TurnJobService::new(&store, &launcher).submit_at(request, 10);
    let elapsed = started.elapsed();
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    let process_group = status.child_identity().unwrap().pid() as i32;
    let group_alive = unsafe { libc::kill(-process_group, 0) } == 0;
    if result.is_err() && group_alive {
        unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }

    let response = result.unwrap_or_else(|error| {
        panic!(
            "{error}; leader={:?}; group={:?}",
            SystemProcessInspector.observe(status.child_identity().unwrap()),
            SystemProcessInspector.observe_group(status.child_identity().unwrap().pid())
        )
    });
    assert_eq!(response.status().state(), JobState::TimedOut);
    assert_eq!(status.state(), JobState::TimedOut);
    assert!(!group_alive, "timed-out process group remains observable");
    assert!(
        elapsed >= grace,
        "configured TERM grace was skipped: {elapsed:?}"
    );
    assert_eq!(
        SystemProcessInspector.observe_group(process_group as u32),
        mac_worker::supervisor::ProcessGroupObservation::Absent,
        "targeted timeout cleanup must prove the group absent"
    );
    assert!(LeaseService::new(&store).load().unwrap().is_none());
}

#[test]
fn successful_leader_cannot_leave_a_background_process_group_after_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    // Install ignored signals before forking so cleanup cannot beat the
    // background shell to its traps. The child keeps them across exec.
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "trap '' HUP TERM; /bin/sh -c 'exec /bin/sleep 120' & echo $!; exit 0".into(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_turn_with_command_and_timeout(
        &temp.path().join("background-group"),
        command,
        30_000,
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = TimedSupervisorLauncher {
        store: store.clone(),
        term_grace: Some(TEST_TERM_GRACE),
    };

    let started = Instant::now();
    let result = TurnJobService::new(&store, &launcher).submit_at(request, 10);
    let elapsed = started.elapsed();
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    let process_group = status.child_identity().unwrap().pid() as i32;
    let group_alive = unsafe { libc::kill(-process_group, 0) } == 0;
    if group_alive {
        unsafe { libc::kill(-process_group, libc::SIGKILL) };
    }

    let response = result.unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert!(
        !group_alive,
        "successful command left its process group alive"
    );
    assert!(
        elapsed >= TEST_TERM_GRACE,
        "background group skipped its TERM grace: {elapsed:?}"
    );
    assert_eq!(
        SystemProcessInspector.observe_group(process_group as u32),
        mac_worker::supervisor::ProcessGroupObservation::Absent,
        "background group cleanup must prove the group absent"
    );
    assert!(LeaseService::new(&store).load().unwrap().is_none());
}

#[test]
fn mutable_cleanup_failure_preserves_command_outcome_and_retains_lease() {
    let temp = tempfile::tempdir().unwrap();
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "rmdir \"$HOME\" && : > \"$HOME\"".into(),
    ])
    .unwrap();
    let (store, lease, _request) =
        archived_host_with_command(&temp.path().join("cleanup-failure"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    fs::remove_dir(job.join("home")).unwrap();
    fs::write(job.join("home"), b"").unwrap();
    fs::set_permissions(job.join("home"), fs::Permissions::from_mode(0o600)).unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    seed_archived_succeeded_status(&store, &lease);
    let error = JobService::new(&store, &launcher)
        .reconcile_job(lease.job_id())
        .unwrap_err();

    assert!(!error.to_string().contains(job.to_string_lossy().as_ref()));
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Succeeded);
    assert_eq!(status.exit_code(), Some(0));
    assert_eq!(status.cleanup_error_code(), Some("MUTABLE_CLEANUP_FAILED"));
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(job.join("home").is_file());
    assert!(job.join("workspace").is_dir());
}

#[test]
fn unexpected_incoming_evidence_is_preserved_and_blocks_cleanup_and_release() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, _request) = archived_host(&temp.path().join("incoming-evidence"));
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let incoming_job = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let foreign = incoming_job.join("foreign-evidence");
    fs::create_dir(&foreign).unwrap();
    fs::set_permissions(&foreign, fs::Permissions::from_mode(0o700)).unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    seed_archived_succeeded_status(&store, &lease);
    let error = JobService::new(&store, &launcher)
        .reconcile_job(lease.job_id())
        .unwrap_err();

    assert!(
        !error
            .to_string()
            .contains(foreign.to_string_lossy().as_ref())
    );
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Succeeded);
    assert_eq!(status.exit_code(), Some(0));
    assert_eq!(status.cleanup_error_code(), Some("MUTABLE_CLEANUP_FAILED"));
    assert!(foreign.is_dir());
    assert!(job.join("workspace").is_dir());
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
}

#[test]
fn lease_release_failure_preserves_terminal_outcome_and_live_lease_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let host_root = temp.path().join("release-failure");
    let lease_file = host_root.join("leases/slots/0/lease.json");
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf x > \"$1\"".into(),
        "lease-corruptor".into(),
        lease_file.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_turn_with_command(&host_root, command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(
        !error
            .to_string()
            .contains(lease_file.to_string_lossy().as_ref())
    );
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Succeeded);
    assert_eq!(status.exit_code(), Some(0));
    assert_eq!(status.cleanup_error_code(), Some("LEASE_RELEASE_FAILED"));
    assert_eq!(fs::read(&lease_file).unwrap(), b"x");
    assert!(host_root.join("leases/slots/0").is_dir());
    for removed in ["workspace", "home", "tmp"] {
        assert!(!job.join(removed).exists());
    }
}

#[test]
fn transient_command_payload_is_placed_owner_only_before_child_launch() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("must-not-run");
    let command = CommandSpec::argv(vec![
        "/usr/bin/touch".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_turn_with_command(&temp.path().join("payload-permission"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = FailingLauncher {
        launches: Arc::clone(&launches),
    };

    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(
        error.to_string().contains("injected launcher failure"),
        "{error}"
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    let payload_path = job.join("execution.json");
    let metadata = fs::metadata(&payload_path).unwrap();
    assert_eq!(
        metadata.permissions().mode() & 0o777,
        0o600,
        "the transient execution payload must be owner-only while it exists"
    );
    assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
    let payload_bytes = fs::read(&payload_path).unwrap();
    let marker_bytes = marker.to_string_lossy().into_owned().into_bytes();
    assert!(
        payload_bytes
            .windows(marker_bytes.len())
            .any(|window| window == marker_bytes.as_slice()),
        "the transient payload must hold the exact owner-scoped command it will launch"
    );
    assert!(
        !marker.exists(),
        "gated user command executed before launch"
    );

    // A fresh authoritative status request must recover the identityless
    // Accepted job, launch it once, and erase the exact payload afterward.
    let recovery = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let recovered = TurnJobService::new(&store, &recovery)
        .status(lease.job_id())
        .unwrap();
    assert_eq!(recovered.status().state(), JobState::Succeeded);
    assert!(marker.is_file(), "recovered command did not execute");
    assert!(
        !payload_path.exists(),
        "recovery left the exact execution payload durable"
    );
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn supervisor_retains_a_sanitized_error_before_terminal_status() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn_with_command(
        &temp.path().join("supervisor-error-log"),
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = FaultingInlineSupervisorLauncher {
        store: store.clone(),
        point: SupervisorFaultPoint::AfterRunningStatus,
    };

    let error = TurnJobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(
        error.to_string().contains("injected supervisor fault"),
        "{error}"
    );
    let supervisor_log = String::from_utf8(fs::read(job.join("supervisor.log")).unwrap()).unwrap();
    assert!(
        supervisor_log.starts_with("error_code=IO message="),
        "{supervisor_log}"
    );
    assert!(
        supervisor_log.contains("injected supervisor fault"),
        "the message survives beside the code: {supervisor_log}"
    );
    assert_eq!(supervisor_log.lines().count(), 1, "{supervisor_log}");
    assert!(supervisor_log.len() <= 1024);
    assert_eq!(
        serde_json::from_slice::<JobStatus>(&fs::read(job.join("status.json")).unwrap())
            .unwrap()
            .state(),
        JobState::Running
    );
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
    assert!(
        !fs::read(job.join("supervisor.log"))
            .unwrap()
            .windows(temp.path().to_string_lossy().len())
            .any(|window| window == temp.path().to_string_lossy().as_bytes())
    );
}

#[test]
// Supersedes v1 test: hidden_submit_detaches_the_same_worker_and_inherited_lock_runs_supervisor.
fn task_turn_detaches_the_same_worker_and_inherited_lock_runs_supervisor() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let data = temp.path().join("data");
    fs::create_dir_all(&home).unwrap();
    let host_root = data.join("mac-worker/host");
    let (store, lease, request) = prepared_task_host_with_command(
        &host_root,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    drop(store);

    let mut child = Command::new(env!("CARGO_BIN_EXE_worker"))
        .env_clear()
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &data)
        .args(["host", "task-turn"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&request).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let response: TaskTurnResponse = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response.submit().status().supervisor_identity().is_some());

    let deadline = Instant::now() + Duration::from_secs(5);
    let terminal = loop {
        let status: JobStatus =
            serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
        if status.state().is_terminal() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "detached supervisor did not finish"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(terminal.state(), JobState::Succeeded);
    while job.join("execution.json").exists() {
        assert!(
            Instant::now() < deadline,
            "detached turn publication did not finish"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!job.join("execution.json").exists());
    loop {
        if LeaseService::new(&HostStore::open(&host_root).unwrap())
            .load()
            .unwrap()
            .is_none()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "detached cleanup did not release lease"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let rejected = Command::new(env!("CARGO_BIN_EXE_worker"))
        .env_clear()
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &data)
        .args(["host", "supervise", &lease.job_id().to_string()])
        .output()
        .unwrap();
    assert!(!rejected.status.success());
}

#[test]
// Supersedes v1 test: accepted_client_disconnect_after_the_launch_handshake_preserves_supervision_and_status_reconnect.
fn task_turn_client_disconnect_after_the_launch_handshake_preserves_supervision_and_status_reconnect()
 {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let data = temp.path().join("data");
    fs::create_dir_all(&home).unwrap();
    let ready_fifo = temp.path().join("command-ready.fifo");
    let release_fifo = temp.path().join("command-release.fifo");
    let client_exit_fifo = temp.path().join("client-exit.fifo");
    create_fifo(&ready_fifo);
    create_fifo(&release_fifo);
    create_fifo(&client_exit_fifo);
    let exit_parking_dylib = compile_exit_parking_dylib(temp.path());
    let (ready_tx, ready_rx) = mpsc::channel();
    let ready_reader = ready_fifo.clone();
    let ready_thread = std::thread::spawn(move || {
        let result = (|| -> std::io::Result<u8> {
            let mut ready = [0_u8; 1];
            OpenOptions::new()
                .read(true)
                .open(ready_reader)?
                .read_exact(&mut ready)?;
            Ok(ready[0])
        })();
        let _ = ready_tx.send(result);
    });

    let host_root = data.join("mac-worker/host");
    let (store, lease, request) = prepared_task_host_with_command(
        &host_root,
        CommandSpec::argv(vec![
            "/bin/sh".into(),
            "-c".into(),
            concat!(
                "printf R > \"$1\"; ",
                "IFS= read -r release < \"$2\"; ",
                "[ \"$release\" = X ] || exit 9; ",
                "printf reconnected"
            )
            .into(),
            "disconnect-probe".into(),
            ready_fifo.to_string_lossy().into_owned(),
            release_fifo.to_string_lossy().into_owned(),
        ])
        .unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let mut detached_cleanup = DetachedJobCleanup::new(job.clone());
    drop(store);

    let mut submit_client = DirectChildCleanup::new(
        Command::new(env!("CARGO_BIN_EXE_worker"))
            .env_clear()
            .env("HOME", &home)
            .env("XDG_DATA_HOME", &data)
            .env("DYLD_INSERT_LIBRARIES", &exit_parking_dylib)
            .env("MAC_WORKER_TEST_CLIENT_EXIT_FIFO", &client_exit_fifo)
            .args(["host", "task-turn"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    submit_client
        .child_mut()
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&request).unwrap())
        .unwrap();
    let (ack_tx, ack_rx) = mpsc::channel();
    let submit_stdout = submit_client.child_mut().stdout.take().unwrap();
    let ack_thread = std::thread::spawn(move || {
        let result = (|| -> std::io::Result<String> {
            let mut ack_line = String::new();
            BufReader::new(submit_stdout).read_line(&mut ack_line)?;
            Ok(ack_line)
        })();
        let _ = ack_tx.send(result);
    });
    let ack_line = ack_rx
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .expect("accepted submit client did not emit its response within the deadline")
        .unwrap();
    ack_thread.join().unwrap();
    let accepted_turn: TaskTurnResponse = serde_json::from_str(ack_line.trim_end()).unwrap();
    let accepted = accepted_turn.submit();
    assert!(accepted.status().supervisor_identity().is_some());

    assert_eq!(
        ready_rx
            .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
            .unwrap()
            .unwrap(),
        b'R',
        "the command must be FIFO-parked before the client is disconnected"
    );
    ready_thread.join().unwrap();

    // After the accepted response, prove the submitting process is still live
    // and SIGKILL that exact PID. The detached supervisor was already handed
    // off before the response was written, so its lifetime must not depend on
    // this process surviving.
    assert!(
        submit_client.child_mut().try_wait().unwrap().is_none(),
        "accepted submit client must still be live immediately before SIGKILL"
    );
    let submit_status = submit_client.kill_and_reap();
    assert_eq!(
        submit_status.signal(),
        Some(libc::SIGKILL),
        "accepted submit client must be reaped with SIGKILL status"
    );

    let reconnect_deadline = Instant::now() + Duration::from_secs(5);
    let running: StatusResponse = loop {
        match try_host_control::<StatusRequest, StatusResponse>(
            &home,
            &data,
            "status",
            &StatusRequest::new(lease.job_id()),
        ) {
            Ok(response) if response.status().state() == JobState::Running => break response,
            Ok(response) if response.status().state().is_terminal() => {
                panic!(
                    "FIFO-parked command became terminal before release: {:?}",
                    response.status().state()
                )
            }
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < reconnect_deadline,
                    "status reconnect never crossed the detached-supervisor handoff"
                );
                std::thread::yield_now();
            }
        }
    };
    assert_eq!(
        running.status().supervisor_identity(),
        accepted.status().supervisor_identity()
    );
    assert_eq!(running.status().state(), JobState::Running);

    OpenOptions::new()
        .write(true)
        .open(&release_fifo)
        .unwrap()
        .write_all(b"X\n")
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last_status_error = String::from("none observed");
    let terminal = loop {
        let response: StatusResponse = match try_host_control(
            &home,
            &data,
            "status",
            &StatusRequest::new(lease.job_id()),
        ) {
            Ok(response) => response,
            Err(error) => {
                last_status_error = error;
                assert!(
                    Instant::now() < deadline,
                    "reconnected status polling never recovered a valid response; last control error: {last_status_error}"
                );
                std::thread::yield_now();
                continue;
            }
        };
        assert_eq!(
            response.status().supervisor_identity(),
            accepted.status().supervisor_identity()
        );
        if response.status().state().is_terminal() {
            break response;
        }
        assert!(
            Instant::now() < deadline,
            "reconnected status polling never observed a terminal outcome; last control error: {last_status_error}"
        );
        std::thread::yield_now();
    };

    assert_eq!(terminal.status().state(), JobState::Succeeded);
    assert_eq!(terminal.status().exit_code(), Some(0));
    assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"reconnected");
    let log_deadline = Instant::now() + Duration::from_secs(5);
    let reconnected_log: LogChunkResponse = loop {
        match try_host_control(
            &home,
            &data,
            "log-chunk",
            &LogChunkRequest::new(lease.job_id(), LogStream::Stdout, 0, 1024),
        ) {
            Ok(response) => break response,
            Err(error) => {
                assert!(
                    Instant::now() < log_deadline,
                    "same-ID log reconnect never recovered a valid response; last control error: {error}"
                );
                std::thread::yield_now();
            }
        }
    };
    assert_eq!(reconnected_log.chunk().stream(), LogStream::Stdout);
    assert_eq!(reconnected_log.chunk().offset(), 0);
    assert_eq!(
        reconnected_log.chunk().decoded_bytes().unwrap(),
        b"reconnected"
    );
    while job.join("execution.json").exists() {
        assert!(
            Instant::now() < deadline,
            "disconnected turn publication did not finish"
        );
        std::thread::yield_now();
    }
    assert!(!job.join("execution.json").exists());

    let release_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if LeaseService::new(&HostStore::open(&host_root).unwrap())
            .load()
            .unwrap()
            .is_none()
        {
            break;
        }
        assert!(
            Instant::now() < release_deadline,
            "detached cleanup did not release the lease after the client disconnect"
        );
        std::thread::yield_now();
    }
    detached_cleanup.disarm();
}

#[test]
fn a_diagnostic_left_by_a_failed_earlier_attempt_does_not_block_the_retry() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_turn(&temp.path().join("host"));
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let failing = FailingLauncher {
        launches: Arc::clone(&launches),
    };
    let first_error = TurnJobService::new(&store, &failing)
        .submit_at(request.clone(), 3)
        .unwrap_err();
    assert!(
        first_error
            .to_string()
            .contains("injected launcher failure"),
        "{first_error}"
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);

    // What a supervisor that failed before launching leaves behind: the
    // submit created the empty log, the supervisor appended its reason.
    assert_eq!(fs::metadata(job.join("supervisor.log")).unwrap().len(), 0);
    let mut diagnostic = fs::OpenOptions::new()
        .append(true)
        .open(job.join("supervisor.log"))
        .unwrap();
    diagnostic
        .write_all(b"error_code=PROTOCOL message=simulated first attempt\n")
        .unwrap();
    drop(diagnostic);

    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = TurnJobService::new(&store, &launcher)
        .submit_at(request, 4)
        .unwrap_or_else(|error| panic!("the retry must launch: {error}"));
    assert!(response.status().state().is_terminal());
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Succeeded);
    let supervisor_log = String::from_utf8(fs::read(job.join("supervisor.log")).unwrap()).unwrap();
    assert_eq!(
        supervisor_log, "error_code=PROTOCOL message=simulated first attempt\n",
        "the retry appends nothing on success"
    );
}
