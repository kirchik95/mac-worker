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
    time::{Duration, Instant},
};

use mac_worker::{
    error::WorkerError,
    host_store::{HostStore, HostStoreWritePoint, SupervisorGuard},
    job::{
        CommandSpec, JobState, JobStatus, LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord,
        LogChunkRequest, LogChunkResponse, LogStream, ProcessIdentity, RequestFingerprintMaterial,
        StatusRequest, StatusResponse, SubmitRequest, SubmitResponse,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    protocol::{MemoryPressure, PROTOCOL_VERSION},
    remote_snapshot::RemoteSnapshotService,
    supervisor::{ProcessInspector, Supervisor, SupervisorFaultPoint, SystemProcessInspector},
};
use sha2::{Digest, Sha256};

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
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

fn prepared_host(root: &Path) -> (HostStore, LeaseRecord, SubmitRequest) {
    prepared_host_with_command(
        root,
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    )
}

fn prepared_host_with_command(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, SubmitRequest) {
    prepared_host_with_command_and_timeout(root, command, 30_000)
}

fn prepared_host_with_command_and_timeout(
    root: &Path,
    command: CommandSpec,
    timeout_millis: u64,
) -> (HostStore, LeaseRecord, SubmitRequest) {
    let store = HostStore::open(root).unwrap();
    let manifest = valid_manifest_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let request = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            3,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest.clone(),
            String::new(),
            timeout_millis,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    let lease = match LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
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
    fs::write(incoming.join("manifest.json"), manifest).unwrap();
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
    RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 2)
        .unwrap();
    (store, lease, SubmitRequest::new(request.material().clone()))
}

fn prepared_host_with_nested_cwd(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, SubmitRequest) {
    let store = HostStore::open(root).unwrap();
    let script = b"#!/bin/sh\nprintf '%s' \"$1\"\n";
    let script_digest = format!("{:x}", Sha256::digest(script));
    let manifest = format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"nested","#,
            r#""entries":[{{"path":"nested","kind":"directory","mode":493,"size":0,"#,
            r#""sha256":"4b66bf93b3932a9539880b2421e35019af9daf84363a0246280ffcac173b2678","#,
            r#""symlink_target":null}},{{"path":"nested/payload.txt","kind":"file","mode":420,"size":7,"#,
            r#""sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","#,
            r#""symlink_target":null}},{{"path":"nested/run-argv","kind":"file","mode":493,"size":{SCRIPT_SIZE},"#,
            r#""sha256":"{SCRIPT_DIGEST}","symlink_target":null}}],"tracked_deletions":[]}}"#,
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
        SCRIPT_SIZE = script.len(),
        SCRIPT_DIGEST = script_digest,
    )
    .into_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let request = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            3,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest.clone(),
            "nested".into(),
            30_000,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    let lease = match LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(incoming.join("tree/nested")).unwrap();
    for private in [
        incoming.parent().unwrap().parent().unwrap(),
        incoming.parent().unwrap(),
        incoming.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(incoming.join("manifest.json"), manifest).unwrap();
    fs::write(incoming.join("tree/nested/payload.txt"), b"payload").unwrap();
    fs::write(incoming.join("tree/nested/run-argv"), script).unwrap();
    fs::set_permissions(
        incoming.join("manifest.json"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/nested/payload.txt"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/nested/run-argv"),
        fs::Permissions::from_mode(0o555),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/nested"),
        fs::Permissions::from_mode(0o555),
    )
    .unwrap();
    fs::set_permissions(incoming.join("tree"), fs::Permissions::from_mode(0o555)).unwrap();
    RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 2)
        .unwrap();
    (store, lease, SubmitRequest::new(request.material().clone()))
}

fn prepared_host_with_empty_nested_cwd(
    root: &Path,
    command: CommandSpec,
) -> (HostStore, LeaseRecord, SubmitRequest) {
    let store = HostStore::open(root).unwrap();
    let manifest = format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"nested","#,
            r#""entries":[{{"path":"nested","kind":"directory","mode":493,"size":0,"#,
            r#""sha256":"4b66bf93b3932a9539880b2421e35019af9daf84363a0246280ffcac173b2678","#,
            r#""symlink_target":null}}],"tracked_deletions":[]}}"#,
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
    )
    .into_bytes();
    let digest = format!("{:x}", Sha256::digest(&manifest));
    let request = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            3,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest.clone(),
            "nested".into(),
            30_000,
            "heavy".into(),
            command,
        )
        .unwrap(),
    );
    let lease = match LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(incoming.join("tree/nested")).unwrap();
    for private in [
        incoming.parent().unwrap().parent().unwrap(),
        incoming.parent().unwrap(),
        incoming.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(incoming.join("manifest.json"), manifest).unwrap();
    fs::set_permissions(
        incoming.join("manifest.json"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/nested"),
        fs::Permissions::from_mode(0o555),
    )
    .unwrap();
    fs::set_permissions(incoming.join("tree"), fs::Permissions::from_mode(0o555)).unwrap();
    RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 2)
        .unwrap();
    (store, lease, SubmitRequest::new(request.material().clone()))
}

fn compile_native_argv_reflector(directory: &Path) -> PathBuf {
    let source = directory.join("argv-reflector.c");
    let executable = directory.join("native argv[0];$()`'\" literal");
    fs::write(
        &source,
        concat!(
            "#include <stdio.h>\n",
            "#include <string.h>\n",
            "int main(int argc, char **argv) {\n",
            "  for (int i = 0; i < argc; ++i) {\n",
            "    size_t length = strlen(argv[i]);\n",
            "    if (fwrite(argv[i], 1, length, stdout) != length || fputc(0, stdout) == EOF) return 2;\n",
            "  }\n",
            "  return fflush(stdout) == 0 ? 0 : 3;\n",
            "}\n",
        ),
    )
    .unwrap();
    let output = Command::new("/usr/bin/cc")
        .args(["-o"])
        .arg(&executable)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "failed to compile native argv reflector: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    executable
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

struct InlineSupervisorLauncher {
    store: HostStore,
}

struct FaultingInlineSupervisorLauncher {
    store: HostStore,
    point: SupervisorFaultPoint,
}

struct TamperingInlineSupervisorLauncher {
    store: HostStore,
    job_path: PathBuf,
}

impl SupervisorLauncher for InlineSupervisorLauncher {
    fn launch(
        &self,
        job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let inspector = SystemProcessInspector;
        let identity = inspector.identity_for_pid(std::process::id())?;
        Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?;
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
        Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

struct RecordingLauncher {
    launches: Arc<AtomicUsize>,
    job_path: PathBuf,
    identity: ProcessIdentity,
}

struct PrelaunchTerminalLauncher {
    job_path: PathBuf,
    supervisor: ProcessIdentity,
    child: ProcessIdentity,
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
            ProcessIdentity::new(99_001, 9_900_001).unwrap(),
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
        Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?;
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
        Supervisor::new(&self.store, &inspector).run_with_guard(job_id, guard)?;
        Ok(LaunchCandidate::new(identity))
    }
}

impl SupervisorLauncher for PrelaunchTerminalLauncher {
    fn launch(
        &self,
        _job_id: mac_worker::job::JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, mac_worker::error::WorkerError> {
        let status: JobStatus =
            serde_json::from_slice(&fs::read(self.job_path.join("status.json"))?)
                .map_err(|error| mac_worker::error::WorkerError::Protocol(error.to_string()))?;
        let terminal = status
            .with_supervisor(self.supervisor, 4)?
            .with_child(self.child, 5)?
            .into_infrastructure_terminal(JobState::Lost, 6, 0, 0, "EXEC_FAILED".into())?;
        let replacement = self.job_path.join(".test-prelaunch-status");
        let mut replacement_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&replacement)?;
        replacement_file.write_all(&serde_json::to_vec(&terminal).unwrap())?;
        replacement_file.sync_all()?;
        fs::rename(&replacement, self.job_path.join("status.json"))?;
        File::open(&self.job_path)?.sync_all()?;
        drop(guard);
        Ok(LaunchCandidate::new(self.supervisor))
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
    let supervisor = ProcessIdentity::new(101, 1_000_001).unwrap();
    let child = ProcessIdentity::new(202, 2_000_002).unwrap();

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
    let supervisor = ProcessIdentity::new(101, 1_000_001).unwrap();
    let child = ProcessIdentity::new(202, 2_000_002).unwrap();
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
    assert_eq!(PROTOCOL_VERSION, 4);
}

#[test]
fn supervisor_recomputes_the_fingerprint_from_exact_command_and_cwd_before_fork() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host_with_command(
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

    let error = JobService::new(&store, &launcher)
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
    let (store, lease, request) = prepared_host(&temp.path().join("host"));
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let identity = ProcessIdentity::new(41_001, 4_100_001).unwrap();
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity,
    };
    let service = JobService::new(&store, &launcher);

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
            "home",
            "meta.json",
            "status.json",
            "stderr.log",
            "supervisor.log",
            "stdout.log",
            "tmp",
            "workspace",
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
fn fast_prelaunch_terminal_with_child_identity_is_not_acknowledged_as_accepted() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host(&temp.path().join("host"));
    let supervisor = ProcessIdentity::new(41_101, 4_110_001).unwrap();
    let launcher = PrelaunchTerminalLauncher {
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        supervisor,
        child: ProcessIdentity::new(41_102, 4_110_002).unwrap(),
    };

    let service = JobService::new(&store, &launcher);
    let error = service.submit_at(request.clone(), 3).unwrap_err();

    assert!(
        error.to_string().contains("SUPERVISOR_PRELAUNCH_FAILED"),
        "{error}"
    );
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));

    let retry_error = service.submit_at(request, 4).unwrap_err();
    assert!(
        retry_error
            .to_string()
            .contains("SUPERVISOR_PRELAUNCH_FAILED"),
        "{retry_error}"
    );
}

#[test]
fn retired_prelaunch_terminal_retry_repeats_the_typed_failure_without_relaunching() {
    // Break caught: the Accepted-without-a-live-lease retry path turns a real
    // prelaunch terminal into Existing even though no child ever launched.
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("retired-prelaunch-terminal");
    let retry_marker = temp.path().join("retry-launch-marker");
    let (store, lease, request) = prepared_host_with_command(
        &root,
        CommandSpec::argv(vec!["/definitely/not/a/program".into()]).unwrap(),
    );
    let first_launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let first_error = JobService::new(&store, &first_launcher)
        .submit_at(request.clone(), 10)
        .unwrap_err();

    assert!(
        first_error.to_string().contains("EXECUTABLE_NOT_FOUND"),
        "{first_error}"
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let terminal: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(terminal.state(), JobState::Lost);
    assert_eq!(terminal.error_code(), Some("EXECUTABLE_NOT_FOUND"));
    assert!(terminal.supervisor_identity().is_some());
    assert_eq!(terminal.child_identity(), None);
    assert!(!job.join("execution.json").exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    let durable_before = durable_tree(&root);
    let retry_launches = Arc::new(AtomicUsize::new(0));
    let retry_launcher = MarkingInlineSupervisorLauncher {
        store: store.clone(),
        launches: Arc::clone(&retry_launches),
        marker: retry_marker.clone(),
    };

    let retry_error = JobService::new(&store, &retry_launcher)
        .submit_at(request, 11)
        .unwrap_err();

    assert_protocol_code(retry_error, "SUPERVISOR_PRELAUNCH_FAILED");
    assert_eq!(retry_launches.load(Ordering::SeqCst), 0);
    assert!(!retry_marker.exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert!(!job.join("execution.json").exists());
    let terminal_after: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(terminal_after, terminal);
    assert_eq!(durable_tree(&root), durable_before);
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
            prepared_host_with_command(&root, CommandSpec::argv(vec![program.into()]).unwrap());
        let first_launcher = InlineSupervisorLauncher {
            store: store.clone(),
        };

        let first = JobService::new(&store, &first_launcher)
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

        let retry = JobService::new(&store, &retry_launcher)
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
    let (store, lease, request) = prepared_host(&temp.path().join("conflict-host"));
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
            .unwrap(),
        identity: ProcessIdentity::new(41_002, 4_100_002).unwrap(),
    };
    let changed = SubmitRequest::new(
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
    let error = JobService::new(&store, &launcher)
        .submit_at(changed, 3)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(launches.load(Ordering::SeqCst), 0);

    let abandoned_root = temp.path().join("abandoned-host");
    let (abandoned_store, abandoned_lease, abandoned_request) = prepared_host(&abandoned_root);
    abandoned_store
        .record_abandoned(
            &LeaseAcquireRequest::new(abandoned_request.material().clone()),
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
        identity: ProcessIdentity::new(41_003, 4_100_003).unwrap(),
    };
    let error = JobService::new(&abandoned_store, &abandoned_launcher)
        .submit_at(abandoned_request, 4)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ABANDONED"));
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[test]
fn submit_treats_a_mismatched_same_job_abandonment_as_a_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host(&temp.path().join("host"));
    store
        .record_abandoned(&LeaseAcquireRequest::new(request.material().clone()), 3)
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
        identity: ProcessIdentity::new(41_004, 4_100_004).unwrap(),
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 4)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}

#[test]
fn submit_repairs_a_complete_final_job_after_crash_before_index() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, request) = prepared_host(&root);
    drop(store);
    let launches = Arc::new(AtomicUsize::new(0));
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobPublish).unwrap();
    let job_path = faulted
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity: ProcessIdentity::new(41_004, 4_100_004).unwrap(),
    };
    let error = JobService::new(&faulted, &launcher)
        .submit_at(request.clone(), 3)
        .unwrap_err();
    assert!(error.to_string().contains("injected"));
    assert!(job_path.is_dir());
    assert!(!faulted.job_index(lease.job_id()).unwrap().exists());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path,
        identity: ProcessIdentity::new(41_004, 4_100_004).unwrap(),
    };
    let repaired = JobService::new(&reopened, &launcher)
        .submit_at(request, 4)
        .unwrap();
    assert_eq!(
        repaired.status().supervisor_identity(),
        Some(ProcessIdentity::new(41_004, 4_100_004).unwrap())
    );
    assert!(reopened.job_index(lease.job_id()).unwrap().is_file());
    assert_eq!(launches.load(Ordering::SeqCst), 1);
}

#[test]
fn fresh_reopen_repairs_every_durable_job_and_index_publication_boundary_once() {
    let boundaries = [
        ("home-sync", HostStoreWritePoint::AfterJobHomeSync),
        ("tmp-sync", HostStoreWritePoint::AfterJobTmpSync),
        ("stdout-write", HostStoreWritePoint::AfterJobStdoutWrite),
        (
            "stdout-file-sync",
            HostStoreWritePoint::AfterJobStdoutFileSync,
        ),
        ("stderr-write", HostStoreWritePoint::AfterJobStderrWrite),
        (
            "stderr-file-sync",
            HostStoreWritePoint::AfterJobStderrFileSync,
        ),
        ("meta-write", HostStoreWritePoint::AfterJobMetaWrite),
        ("meta-file-sync", HostStoreWritePoint::AfterJobMetaFileSync),
        ("status-write", HostStoreWritePoint::AfterJobStatusWrite),
        (
            "status-file-sync",
            HostStoreWritePoint::AfterJobStatusFileSync,
        ),
        (
            "execution-write",
            HostStoreWritePoint::AfterJobExecutionWrite,
        ),
        (
            "execution-file-sync",
            HostStoreWritePoint::AfterJobExecutionFileSync,
        ),
        (
            "staging-directory-sync",
            HostStoreWritePoint::AfterJobStagingDirectorySync,
        ),
        ("job-rename", HostStoreWritePoint::AfterJobRename),
        ("job-parent-sync", HostStoreWritePoint::AfterJobPublish),
        (
            "index-file-sync",
            HostStoreWritePoint::AfterJobIndexFileSync,
        ),
        ("index-rename", HostStoreWritePoint::AfterJobIndexRename),
        (
            "index-parent-sync",
            HostStoreWritePoint::AfterJobIndexParentSync,
        ),
    ];

    for (label, point) in boundaries {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(label);
        let marker = temp.path().join("executions");
        let command = CommandSpec::argv(vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf x >> \"$1\"".into(),
            "publication-crash".into(),
            marker.to_string_lossy().into_owned(),
        ])
        .unwrap();
        let (store, lease, request) = prepared_host_with_command(&root, command);
        drop(store);

        let faulted = HostStore::open_with_write_fault(&root, point).unwrap();
        let first_launcher = InlineSupervisorLauncher {
            store: faulted.clone(),
        };
        assert!(
            JobService::new(&faulted, &first_launcher)
                .submit_at(request.clone(), 10)
                .is_err(),
            "{label} did not stop at its injected boundary"
        );
        assert!(!marker.exists(), "{label} executed before recovery");
        drop(first_launcher);
        drop(faulted);

        let reopened = HostStore::open(&root).unwrap();
        let retry_launcher = InlineSupervisorLauncher {
            store: reopened.clone(),
        };
        let response = JobService::new(&reopened, &retry_launcher)
            .submit_at(request, 11)
            .unwrap_or_else(|error| panic!("{label} was not repairable: {error}"));
        assert_eq!(response.status().state(), JobState::Succeeded, "{label}");
        assert_eq!(fs::read(&marker).unwrap(), b"x", "{label}");
        assert!(
            reopened.job_index(lease.job_id()).unwrap().is_file(),
            "{label} did not leave one indexed job"
        );
        assert_eq!(
            LeaseService::new(&reopened).load().unwrap(),
            None,
            "{label}"
        );
    }
}

#[test]
fn rename_crash_recovery_durably_syncs_job_and_index_parents_before_launch() {
    let cases = [
        (
            "job-rename-repair",
            HostStoreWritePoint::AfterJobRename,
            HostStoreWritePoint::AfterJobPublish,
        ),
        (
            "index-rename-repair",
            HostStoreWritePoint::AfterJobIndexRename,
            HostStoreWritePoint::AfterJobIndexParentSync,
        ),
    ];

    for (label, initial_fault, repair_fault) in cases {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(label);
        let marker = temp.path().join(format!("{label}-executions"));
        let command = CommandSpec::argv(vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf x >> \"$1\"".into(),
            "publication-repair".into(),
            marker.to_string_lossy().into_owned(),
        ])
        .unwrap();
        let (store, _lease, request) = prepared_host_with_command(&root, command);
        drop(store);

        let faulted = HostStore::open_with_write_fault(&root, initial_fault).unwrap();
        let launcher = InlineSupervisorLauncher {
            store: faulted.clone(),
        };
        assert!(
            JobService::new(&faulted, &launcher)
                .submit_at(request.clone(), 10)
                .is_err(),
            "{label} did not stop after rename"
        );
        assert!(!marker.exists(), "{label} executed at the rename boundary");
        drop(launcher);
        drop(faulted);

        let repair = HostStore::open_with_write_fault(&root, repair_fault).unwrap();
        let launcher = InlineSupervisorLauncher {
            store: repair.clone(),
        };
        assert!(
            JobService::new(&repair, &launcher)
                .submit_at(request.clone(), 11)
                .is_err(),
            "{label} skipped the durable parent-sync repair boundary"
        );
        assert!(
            !marker.exists(),
            "{label} launched before repair was durable"
        );
        drop(launcher);
        drop(repair);

        let reopened = HostStore::open(&root).unwrap();
        let launcher = InlineSupervisorLauncher {
            store: reopened.clone(),
        };
        let response = JobService::new(&reopened, &launcher)
            .submit_at(request, 12)
            .unwrap_or_else(|error| panic!("{label} was not repairable: {error}"));
        assert_eq!(response.status().state(), JobState::Succeeded, "{label}");
        assert_eq!(fs::read(&marker).unwrap(), b"x", "{label}");
    }
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
    let (store, lease, request) = prepared_host_with_command(&root, command);
    drop(store);
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobIndexFileSync)
            .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: faulted.clone(),
    };
    assert!(
        JobService::new(&faulted, &launcher)
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
    let error = JobService::new(&reopened, &retry)
        .submit_at(request, 11)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert!(staging.is_file());
    assert!(!reopened.job_index(lease.job_id()).unwrap().exists());
    assert!(!marker.exists());
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), Some(lease));
}

#[test]
fn fresh_reopen_supervision_crash_matrix_never_executes_twice() {
    #[derive(Clone, Copy)]
    enum Boundary {
        Spawn,
        Supervisor(SupervisorFaultPoint),
        Store(HostStoreWritePoint),
    }

    let boundaries = [
        ("supervisor-spawn", Boundary::Spawn, true),
        (
            "supervisor-identity",
            Boundary::Supervisor(SupervisorFaultPoint::AfterSupervisorIdentity),
            false,
        ),
        (
            "payload-read",
            Boundary::Supervisor(SupervisorFaultPoint::AfterPayloadRead),
            false,
        ),
        (
            "child-ready",
            Boundary::Supervisor(SupervisorFaultPoint::AfterChildReady),
            false,
        ),
        (
            "child-identity",
            Boundary::Supervisor(SupervisorFaultPoint::AfterChildIdentity),
            false,
        ),
        (
            "payload-erase",
            Boundary::Supervisor(SupervisorFaultPoint::AfterPayloadErase),
            false,
        ),
        (
            "go",
            Boundary::Supervisor(SupervisorFaultPoint::AfterGo),
            true,
        ),
        (
            "exec-ack",
            Boundary::Supervisor(SupervisorFaultPoint::AfterExecAck),
            true,
        ),
        (
            "running-status",
            Boundary::Supervisor(SupervisorFaultPoint::BeforeRunningStatus),
            true,
        ),
        (
            "running-durable",
            Boundary::Supervisor(SupervisorFaultPoint::AfterRunningStatus),
            true,
        ),
        (
            "log-sync",
            Boundary::Supervisor(SupervisorFaultPoint::AfterLogSync),
            true,
        ),
        (
            "terminal-status",
            Boundary::Supervisor(SupervisorFaultPoint::AfterTerminalStatus),
            true,
        ),
        (
            "cleanup-proof",
            Boundary::Store(HostStoreWritePoint::AfterJobCleanupProof),
            true,
        ),
        (
            "lease-retirement",
            Boundary::Store(HostStoreWritePoint::AfterJobLeaseRetirement),
            true,
        ),
    ];

    for (label, boundary, expects_execution) in boundaries {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(label);
        let marker = temp.path().join("executions");
        let command = CommandSpec::argv(vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf x >> \"$1\"".into(),
            "supervisor-crash".into(),
            marker.to_string_lossy().into_owned(),
        ])
        .unwrap();
        let (prepared, lease, request) = prepared_host_with_command(&root, command);
        drop(prepared);
        let store = match boundary {
            Boundary::Store(point) => HostStore::open_with_write_fault(&root, point).unwrap(),
            Boundary::Spawn | Boundary::Supervisor(_) => HostStore::open(&root).unwrap(),
        };
        let first_result = match boundary {
            Boundary::Spawn => {
                let launcher = FailingLauncher {
                    launches: Arc::new(AtomicUsize::new(0)),
                };
                JobService::new(&store, &launcher).submit_at(request.clone(), 10)
            }
            Boundary::Supervisor(point) => {
                let launcher = FaultingInlineSupervisorLauncher {
                    store: store.clone(),
                    point,
                };
                JobService::new(&store, &launcher).submit_at(request.clone(), 10)
            }
            Boundary::Store(_) => {
                let launcher = InlineSupervisorLauncher {
                    store: store.clone(),
                };
                JobService::new(&store, &launcher).submit_at(request.clone(), 10)
            }
        };
        if label != "lease-retirement" {
            assert!(first_result.is_err(), "{label} unexpectedly acknowledged");
        }
        drop(store);

        let reopened = HostStore::open(&root).unwrap();
        let retry = InlineSupervisorLauncher {
            store: reopened.clone(),
        };
        let _ = JobService::new(&reopened, &retry).submit_at(request, 11);
        let bytes = fs::read(&marker).unwrap_or_default();
        assert_eq!(
            bytes,
            if expects_execution {
                b"x".as_slice()
            } else {
                b"".as_slice()
            },
            "{label} execution cardinality changed after fresh reopen"
        );
        assert!(
            reopened.job_index(lease.job_id()).unwrap().is_file(),
            "{label} lost the accepted index"
        );
    }
}

#[test]
fn unindexed_final_with_corrupt_workspace_is_preserved_and_never_repaired_or_launched() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("host");
    let (store, lease, request) = prepared_host(&root);
    drop(store);
    let launches = Arc::new(AtomicUsize::new(0));
    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterJobPublish).unwrap();
    let job = faulted
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let first_launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job.clone(),
        identity: ProcessIdentity::new(41_104, 4_110_004).unwrap(),
    };
    assert!(
        JobService::new(&faulted, &first_launcher)
            .submit_at(request.clone(), 3)
            .is_err()
    );
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    fs::write(job.join("workspace/tree/payload.txt"), b"corrupt").unwrap();
    drop(faulted);

    let reopened = HostStore::open(&root).unwrap();
    let retry_launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job.clone(),
        identity: ProcessIdentity::new(41_105, 4_110_005).unwrap(),
    };
    let error = JobService::new(&reopened, &retry_launcher)
        .submit_at(request, 4)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert!(!reopened.job_index(lease.job_id()).unwrap().exists());
    assert_eq!(
        fs::read(job.join("workspace/tree/payload.txt")).unwrap(),
        b"corrupt"
    );
    assert_eq!(LeaseService::new(&reopened).load().unwrap(), Some(lease));
}

#[test]
fn indexed_prelaunch_retry_rejects_incomplete_final_without_a_second_launch() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host(&temp.path().join("host"));
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = FailingLauncher {
        launches: Arc::clone(&launches),
    };
    let service = JobService::new(&store, &launcher);
    let first_error = service.submit_at(request.clone(), 3).unwrap_err();
    assert!(
        first_error
            .to_string()
            .contains("injected launcher failure")
    );
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    fs::remove_file(job.join("execution.json")).unwrap();

    let retry_error = service.submit_at(request, 4).unwrap_err();

    assert!(retry_error.to_string().contains("JOB_ID_CONFLICT"));
    assert_eq!(launches.load(Ordering::SeqCst), 1);
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
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
    let (store, lease, request) = prepared_host_with_command(&root, command);
    let failing = FailingLauncher {
        launches: Arc::new(AtomicUsize::new(0)),
    };
    assert!(
        JobService::new(&store, &failing)
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
    let error = JobService::new(&store, &launcher)
        .submit_at(request, 4)
        .unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert!(!marker.exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn unsafe_preexisting_final_evidence_is_preserved_and_never_indexed_or_launched() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host(&temp.path().join("host"));
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
        identity: ProcessIdentity::new(41_005, 4_100_005).unwrap(),
    };

    let error = JobService::new(&store, &launcher)
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
    let (store, lease, request) = prepared_host(&temp.path().join("host"));
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = RecordingLauncher {
        launches: Arc::clone(&launches),
        job_path: job_path.clone(),
        identity: ProcessIdentity::new(41_006, 4_100_006).unwrap(),
    };
    let service = JobService::new(&store, &launcher);
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
                Some(ProcessIdentity::new(41_006, 4_100_006).unwrap())
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
    let (store, _lease, request) = prepared_host(&temp.path().join("dead-owner"));
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
        JobService::new(&first_store, &launcher).submit_at(first_request, 10)
    });
    observed.recv_timeout(Duration::from_secs(2)).unwrap();

    let launches = Arc::new(AtomicUsize::new(0));
    let retry_store = store.clone();
    let retry_request = request.clone();
    let retry_launches = Arc::clone(&launches);
    let retry = std::thread::spawn(move || {
        let launcher = CountingInlineSupervisorLauncher {
            store: retry_store.clone(),
            launches: retry_launches,
        };
        JobService::new(&retry_store, &launcher).submit_at(retry_request, 11)
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
    let (store, lease, request) = prepared_host(&temp.path().join("busy-owner"));
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
        JobService::new(&first_store, &launcher).submit_at(first_request, 10)
    });
    observed.recv_timeout(Duration::from_secs(2)).unwrap();

    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = FailingLauncher {
        launches: Arc::clone(&launches),
    };
    let response = JobService::new(&store, &launcher)
        .submit_at(request, 11)
        .unwrap();

    assert_eq!(response.status().state(), JobState::Accepted);
    assert!(response.status().supervisor_identity().is_none());
    assert!(response.status().child_identity().is_none());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    drop(held.lock().unwrap().take());
    assert!(first.join().unwrap().is_err());
}

#[test]
fn real_gated_argv_execution_is_literal_terminal_and_releases_only_its_lease() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("must-not-exist");
    let literal = format!(
        "spaces 'quotes' \"double\" $(touch {}) `touch {}` ; semicolon ünicode\nnewline",
        marker.display(),
        marker.display()
    );
    let command =
        CommandSpec::argv(vec!["/usr/bin/printf".into(), "%s".into(), literal.clone()]).unwrap();
    let (store, lease, request) = prepared_host_with_command(&temp.path().join("host"), command);
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();

    assert_eq!(response.status().state(), JobState::Succeeded);
    assert!(response.status().supervisor_identity().is_some());
    assert!(response.status().child_identity().is_some());
    assert_eq!(response.status().exit_code(), Some(0));
    assert_eq!(
        fs::read(job_path.join("stdout.log")).unwrap(),
        literal.as_bytes()
    );
    assert_eq!(fs::read(job_path.join("stderr.log")).unwrap(), b"");
    assert!(!marker.exists(), "argv bytes were interpreted by a shell");
    assert!(!job_path.join("execution.json").exists());
    for removed in ["workspace", "home", "tmp"] {
        assert!(!job_path.join(removed).exists());
    }
    assert!(
        !store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap()
            .parent()
            .unwrap()
            .exists(),
        "job-owned incoming namespace was not cleaned"
    );
    assert!(LeaseService::new(&store).load().unwrap().is_none());
}

#[test]
fn argv_at_the_two_hundred_fifty_six_boundary_executes_every_unique_metacharacter_value_literally()
{
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("must-not-exist");
    // A native reflector observes the execve argv vector directly. In
    // particular, its first NUL-delimited value is the delivered argv[0], not a
    // shebang interpreter's reconstructed script path.
    let reflector = compile_native_argv_reflector(temp.path());
    let metacharacters = [
        "$(", ")", "`", ";", "&", "|", "<", ">", "*", "?", "~", "!", "#", "'", "\"", "\\",
    ];
    let literal_args: Vec<String> = (0..255)
        .map(|index| {
            format!(
                "arg-{index:03}-{}-$(touch {})-`touch {}`-ünicode-\n-\t-{index}",
                metacharacters[index % metacharacters.len()],
                marker.display(),
                marker.display(),
            )
        })
        .collect();
    assert_eq!(
        literal_args
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        255,
        "all 255 literal values must be unique"
    );
    let mut argv = vec![reflector.to_string_lossy().into_owned()];
    argv.extend(literal_args.iter().cloned());
    assert_eq!(
        argv.len(),
        256,
        "must exercise the exact MAX_ARG_COUNT boundary"
    );
    assert_eq!(
        argv.iter().collect::<std::collections::BTreeSet<_>>().len(),
        256,
        "all 256 literal argv values must be unique"
    );
    let command = CommandSpec::argv(argv.clone()).unwrap();
    let (store, lease, request) = prepared_host_with_command(&temp.path().join("host"), command);
    let job_path = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();

    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(response.status().exit_code(), Some(0));
    let stdout = fs::read(job_path.join("stdout.log")).unwrap();
    let mut received: Vec<&[u8]> = stdout.split(|byte| *byte == 0).collect();
    assert_eq!(received.pop(), Some(&[][..]));
    assert_eq!(received.len(), 256);
    assert_eq!(
        received[0],
        argv[0].as_bytes(),
        "native helper did not observe the literal delivered argv[0]"
    );
    for (actual, expected) in received.iter().zip(argv.iter()) {
        assert_eq!(*actual, expected.as_bytes());
    }
    assert_eq!(fs::read(job_path.join("stderr.log")).unwrap(), b"");
    assert!(
        !marker.exists(),
        "metacharacters inside argv values must never be interpreted by a shell"
    );
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
            prepared_host_with_command_and_timeout(&temp.path().join(name), command, timeout);
        let launcher = InlineSupervisorLauncher {
            store: store.clone(),
        };
        let response = JobService::new(&store, &launcher)
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
        prepared_host_with_command(&temp.path().join("binary-logs"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(fs::read(job.join("stdout.log")).unwrap(), b"out\0bytes");
    assert_eq!(fs::read(job.join("stderr.log")).unwrap(), b"err\0bytes");
    assert_eq!(response.status().final_stdout_bytes(), Some(9));
    assert_eq!(response.status().final_stderr_bytes(), Some(9));
}

#[test]
fn timeout_keeps_a_waitable_leader_anchor_then_kills_and_proves_the_group_absent() {
    let temp = tempfile::tempdir().unwrap();
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "trap 'exit 0' TERM; /bin/sh -c 'trap \"\" TERM; while :; do sleep 1; done' & echo $!; wait"
            .into(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_host_with_command_and_timeout(&temp.path().join("surviving-group"), command, 100);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let started = Instant::now();
    let result = JobService::new(&store, &launcher).submit_at(request, 10);
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
        elapsed >= Duration::from_secs(10),
        "ten-second TERM grace was skipped: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "targeted timeout cleanup exceeded its bounded proof window: {elapsed:?}"
    );
    assert!(LeaseService::new(&store).load().unwrap().is_none());
}

#[test]
fn successful_leader_cannot_leave_a_background_process_group_after_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "/bin/sh -c 'trap \"\" HUP TERM; while :; do sleep 1; done' & echo $!; exit 0".into(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_host_with_command_and_timeout(
        &temp.path().join("background-group"),
        command,
        30_000,
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let started = Instant::now();
    let result = JobService::new(&store, &launcher).submit_at(request, 10);
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
        elapsed >= Duration::from_secs(10),
        "background group skipped its TERM grace: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "background group cleanup exceeded its bounded proof window: {elapsed:?}"
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
    let (store, lease, request) =
        prepared_host_with_command(&temp.path().join("cleanup-failure"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
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
    let (store, lease, request) = prepared_host(&temp.path().join("incoming-evidence"));
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

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
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
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
}

#[test]
fn lease_release_failure_preserves_terminal_outcome_and_live_lease_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let host_root = temp.path().join("release-failure");
    let lease_file = host_root.join("leases/heavy/lease.json");
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf x > \"$1\"".into(),
        "lease-corruptor".into(),
        lease_file.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_host_with_command(&host_root, command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let error = JobService::new(&store, &launcher)
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
    assert!(host_root.join("leases/heavy").is_dir());
    for removed in ["workspace", "home", "tmp"] {
        assert!(!job.join(removed).exists());
    }
}

#[test]
fn terminal_status_write_conflict_retains_lease_and_mutable_job() {
    let temp = tempfile::tempdir().unwrap();
    let host_root = temp.path().join("status-failure");
    let status_file = host_root
        .join("jobs")
        .join(PROJECT_ID)
        .join(WORKTREE_ID)
        .join(JOB_ID)
        .join("status.json");
    let marker = temp.path().join("command-ran");
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "sleep 0.05; : > \"$2\"; printf x > \"$1\"".into(),
        "status-conflict".into(),
        status_file.to_string_lossy().into_owned(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_host_with_command(&host_root, command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(
        !error
            .to_string()
            .contains(status_file.to_string_lossy().as_ref())
    );
    assert!(marker.is_file());
    assert_eq!(fs::read(&status_file).unwrap(), b"x");
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(job.join("workspace").is_dir());
    assert!(!job.join("execution.json").exists());
}

#[test]
fn payload_erase_failure_closes_the_gate_before_any_user_byte_and_retains_lease() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("must-not-run");
    let command = CommandSpec::argv(vec![
        "/usr/bin/touch".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_host_with_command(&temp.path().join("payload-erase-failure"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = FaultingInlineSupervisorLauncher {
        store: store.clone(),
        point: SupervisorFaultPoint::BeforePayloadErase,
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(error.to_string().contains("injected"), "{error}");
    assert!(
        !marker.exists(),
        "gated user command executed before erasure"
    );
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Lost);
    assert_eq!(status.error_code(), Some("PAYLOAD_ERASURE_FAILED"));
    assert!(status.child_identity().is_some());
    assert!(job.join("execution.json").is_file());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(job.join("workspace").is_dir());
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
        prepared_host_with_command(&temp.path().join("payload-permission"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launches = Arc::new(AtomicUsize::new(0));
    let launcher = FailingLauncher {
        launches: Arc::clone(&launches),
    };

    let error = JobService::new(&store, &launcher)
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
    let recovered = JobService::new(&store, &recovery)
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
fn pre_go_child_status_failure_erases_payload_but_abort_ambiguity_retains_lease() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("must-not-run");
    let command = CommandSpec::argv(vec![
        "/usr/bin/touch".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_host_with_command(&temp.path().join("child-status-abort-ambiguity"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = FaultingInlineSupervisorLauncher {
        store: store.clone(),
        point: SupervisorFaultPoint::BeforeChildStatusAbortProof,
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(error.to_string().contains("abort proof"), "{error}");
    assert!(!marker.exists(), "pre-GO command unexpectedly executed");
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Lost);
    assert_eq!(status.error_code(), Some("CHILD_STATUS_WRITE_FAILED"));
    assert!(status.child_identity().is_none());
    assert!(!job.join("execution.json").exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(job.join("workspace").is_dir());
}

#[test]
fn running_status_failure_reaps_the_post_go_child_and_retains_lease() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("ran-exactly-once");
    let command = CommandSpec::argv(vec![
        "/usr/bin/touch".into(),
        marker.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) =
        prepared_host_with_command(&temp.path().join("running-status-failure"), command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = FaultingInlineSupervisorLauncher {
        store: store.clone(),
        point: SupervisorFaultPoint::BeforeRunningStatus,
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(error.to_string().contains("injected"), "{error}");
    assert!(marker.is_file(), "post-GO user command did not run");
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Lost);
    assert_eq!(status.error_code(), Some("RUNNING_STATUS_WRITE_FAILED"));
    let child = status.child_identity().unwrap();
    let mut wait_status = 0;
    assert_eq!(
        unsafe { libc::waitpid(child.pid() as i32, &raw mut wait_status, libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
    assert!(!job.join("execution.json").exists());
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(job.join("workspace").is_dir());
}

#[test]
fn supervisor_retains_a_sanitized_error_before_terminal_status() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host_with_command(
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

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(
        error.to_string().contains("injected supervisor fault"),
        "{error}"
    );
    assert_eq!(
        fs::read(job.join("supervisor.log")).unwrap(),
        b"error_code=IO\n"
    );
    assert!(fs::read(job.join("supervisor.log")).unwrap().len() <= 1024);
    assert_eq!(
        serde_json::from_slice::<JobStatus>(&fs::read(job.join("status.json")).unwrap())
            .unwrap()
            .state(),
        JobState::Running
    );
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(
        !fs::read(job.join("supervisor.log"))
            .unwrap()
            .windows(temp.path().to_string_lossy().len())
            .any(|window| window == temp.path().to_string_lossy().as_bytes())
    );
}

#[test]
fn log_path_replacement_after_go_retains_running_status_and_lease() {
    let temp = tempfile::tempdir().unwrap();
    let host_root = temp.path().join("log-failure");
    let stdout_path = host_root
        .join("jobs")
        .join(PROJECT_ID)
        .join(WORKTREE_ID)
        .join(JOB_ID)
        .join("stdout.log");
    let command = CommandSpec::argv(vec![
        "/bin/sh".into(),
        "-c".into(),
        "sleep 0.05; rm -f \"$1\"; : > \"$1\"".into(),
        "log-replacement".into(),
        stdout_path.to_string_lossy().into_owned(),
    ])
    .unwrap();
    let (store, lease, request) = prepared_host_with_command(&host_root, command);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let error = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap_err();

    assert!(
        !error
            .to_string()
            .contains(stdout_path.to_string_lossy().as_ref())
    );
    let status: JobStatus =
        serde_json::from_slice(&fs::read(job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Running);
    assert_eq!(LeaseService::new(&store).load().unwrap(), Some(lease));
    assert!(job.join("workspace").is_dir());
    assert!(!job.join("execution.json").exists());
}

#[test]
fn child_receives_only_the_exact_controlled_environment_and_workspace_cwd() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host_with_command(
        &temp.path().join("env-host"),
        CommandSpec::argv(vec!["/usr/bin/env".into()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    let actual = String::from_utf8(fs::read(job.join("stdout.log")).unwrap())
        .unwrap()
        .lines()
        .map(String::from)
        .collect::<std::collections::BTreeSet<_>>();
    let expected = [
        "LC_ALL=C".to_owned(),
        "LANG=C".to_owned(),
        "PATH=/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin".to_owned(),
        format!("HOME={}", job.join("home").display()),
        format!("TMPDIR={}", job.join("tmp").display()),
        format!("MAC_WORKER_JOB_ID={}", lease.job_id()),
        format!("MAC_WORKER_CLIENT_ID={}", lease.client_id()),
        format!("MAC_WORKER_PROJECT_ID={}", lease.project_id()),
        format!("MAC_WORKER_WORKTREE_ID={}", lease.worktree_id()),
    ]
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected);

    let (cwd_store, cwd_lease, cwd_request) = prepared_host_with_command(
        &temp.path().join("cwd-host"),
        CommandSpec::argv(vec!["/bin/pwd".into()]).unwrap(),
    );
    let cwd_job = cwd_store
        .job(
            cwd_lease.project_id(),
            cwd_lease.worktree_id(),
            cwd_lease.job_id(),
        )
        .unwrap();
    let cwd_launcher = InlineSupervisorLauncher {
        store: cwd_store.clone(),
    };
    JobService::new(&cwd_store, &cwd_launcher)
        .submit_at(cwd_request, 10)
        .unwrap();
    let physical_job = if let Ok(suffix) = cwd_job.strip_prefix("/var") {
        Path::new("/private/var").join(suffix)
    } else {
        cwd_job.clone()
    };
    assert_eq!(
        fs::read(cwd_job.join("stdout.log")).unwrap(),
        format!("{}\n", physical_job.join("workspace/tree").display()).as_bytes()
    );
}

#[test]
fn nested_manifest_cwd_and_bare_executable_use_descriptor_root_and_fixed_path() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host_with_nested_cwd(
        &temp.path().join("nested-cwd"),
        CommandSpec::argv(vec!["pwd".into()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();

    let physical_job = if let Ok(suffix) = job.strip_prefix("/var") {
        Path::new("/private/var").join(suffix)
    } else {
        job.clone()
    };
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(
        fs::read(job.join("stdout.log")).unwrap(),
        format!("{}\n", physical_job.join("workspace/tree/nested").display()).as_bytes()
    );
}

#[test]
fn empty_nested_relative_working_directory_is_materialized_and_used_as_cwd() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host_with_empty_nested_cwd(
        &temp.path().join("empty-nested-cwd"),
        CommandSpec::argv(vec!["pwd".into()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();

    let physical_job = if let Ok(suffix) = job.strip_prefix("/var") {
        Path::new("/private/var").join(suffix)
    } else {
        job.clone()
    };
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(response.status().exit_code(), Some(0));
    assert_eq!(
        fs::read(job.join("stdout.log")).unwrap(),
        format!("{}\n", physical_job.join("workspace/tree/nested").display()).as_bytes()
    );
    for removed in ["workspace", "home", "tmp"] {
        assert!(!job.join(removed).exists());
    }
}

#[test]
fn relative_slash_argv_executes_exact_script_from_descriptor_bound_cwd() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("must-not-exist");
    let literal = format!("; touch {}", marker.display());
    let (store, lease, request) = prepared_host_with_nested_cwd(
        &temp.path().join("relative-script"),
        CommandSpec::argv(vec!["./run-argv".into(), literal.clone()]).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };

    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();

    assert_eq!(response.status().state(), JobState::Succeeded);
    assert_eq!(
        fs::read(job.join("stdout.log")).unwrap(),
        literal.as_bytes()
    );
    assert_eq!(fs::read(job.join("stderr.log")).unwrap(), b"");
    assert!(!marker.exists(), "relative argv was interpreted by a shell");
}

#[test]
fn shell_mode_uses_zsh_and_prelaunch_resolution_failure_is_terminal_and_erased() {
    let temp = tempfile::tempdir().unwrap();
    let (store, lease, request) = prepared_host_with_command(
        &temp.path().join("shell-host"),
        CommandSpec::shell("printf '%s' $ZSH_VERSION".into()).unwrap(),
    );
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    let launcher = InlineSupervisorLauncher {
        store: store.clone(),
    };
    let response = JobService::new(&store, &launcher)
        .submit_at(request, 10)
        .unwrap();
    assert_eq!(response.status().state(), JobState::Succeeded);
    assert!(!fs::read(job.join("stdout.log")).unwrap().is_empty());

    let (failed_store, failed_lease, failed_request) = prepared_host_with_command(
        &temp.path().join("missing-host"),
        CommandSpec::argv(vec!["/definitely/not/a/program".into()]).unwrap(),
    );
    let failed_job = failed_store
        .job(
            failed_lease.project_id(),
            failed_lease.worktree_id(),
            failed_lease.job_id(),
        )
        .unwrap();
    let failed_launcher = InlineSupervisorLauncher {
        store: failed_store.clone(),
    };
    let error = JobService::new(&failed_store, &failed_launcher)
        .submit_at(failed_request, 10)
        .unwrap_err();
    assert!(error.to_string().contains("EXECUTABLE_NOT_FOUND"));
    let status: JobStatus =
        serde_json::from_slice(&fs::read(failed_job.join("status.json")).unwrap()).unwrap();
    assert_eq!(status.state(), JobState::Lost);
    assert_eq!(status.error_code(), Some("EXECUTABLE_NOT_FOUND"));
    assert!(!failed_job.join("execution.json").exists());
    assert!(LeaseService::new(&failed_store).load().unwrap().is_none());
}

#[test]
fn hidden_submit_detaches_the_same_worker_and_inherited_lock_runs_supervisor() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let data = temp.path().join("data");
    fs::create_dir_all(&home).unwrap();
    let host_root = data.join("mac-worker/host");
    let (store, lease, request) = prepared_host(&host_root);
    let job = store
        .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        .unwrap();
    drop(store);

    let mut child = Command::new(env!("CARGO_BIN_EXE_worker"))
        .env_clear()
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &data)
        .args(["host", "submit"])
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
    let response: mac_worker::job::SubmitResponse = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response.status().supervisor_identity().is_some());

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
fn accepted_client_disconnect_after_the_launch_handshake_preserves_supervision_and_status_reconnect()
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
    let (store, lease, request) = prepared_host_with_command(
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
            .args(["host", "submit"])
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
        .recv_timeout(Duration::from_secs(5))
        .expect("accepted submit client did not emit its response within the deadline")
        .unwrap();
    ack_thread.join().unwrap();
    let accepted: SubmitResponse = serde_json::from_str(ack_line.trim_end()).unwrap();
    assert!(accepted.status().supervisor_identity().is_some());

    assert_eq!(
        ready_rx
            .recv_timeout(Duration::from_secs(5))
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
