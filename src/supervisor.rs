use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStrExt, fs::PermissionsExt},
    },
    path::{Path, PathBuf},
    ptr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    error::WorkerError,
    host_store::{HostStore, JobDisposition, SupervisorGuard},
    inputs::RelativePath,
    job::{CommandSpec, JobId, JobMeta, JobState, JobStatus, LeaseRecord, ProcessIdentity},
    job_service::{ExecutionPayload, LaunchCandidate, SupervisorLauncher},
    lease::LeaseService,
    rooted_fs::RootedDir,
};

const CONTROLLED_PATH: &str = "/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin";
const CONTROLLED_PATHS: &[&str] = &[
    "/usr/local/bin",
    "/opt/homebrew/bin",
    "/usr/bin",
    "/bin",
    "/usr/sbin",
    "/sbin",
];
const MAX_HOST_JSON_BYTES: u64 = 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const CHILD_TRANSITION_RETRY: Duration = Duration::from_millis(250);
const SUPERVISOR_IDENTITY_RETRY: Duration = Duration::from_millis(250);
const TERM_GRACE: Duration = Duration::from_secs(10);
pub(crate) const SUPERVISOR_LOCK_FD: RawFd = 3;

pub(crate) fn validate_detached_supervisor_context() -> Result<(), WorkerError> {
    let pid = unsafe { libc::getpid() };
    if pid <= 0 || unsafe { libc::getsid(0) } != pid || unsafe { libc::getpgrp() } != pid {
        return Err(protocol_code(
            "SUPERVISOR_SESSION_INVALID",
            "supervisor is not the leader of its detached session and process group",
        ));
    }
    let stdin = descriptor_stat(libc::STDIN_FILENO)?;
    let stdout = descriptor_stat(libc::STDOUT_FILENO)?;
    let stderr = descriptor_stat(libc::STDERR_FILENO)?;
    let dev_null = open_dev_null()?;
    let expected_null = descriptor_stat(dev_null.as_raw_fd())?;
    drop(dev_null);
    validate_null_stdio([stdin, stdout, stderr], expected_null)?;
    let lock_flags = unsafe { libc::fcntl(SUPERVISOR_LOCK_FD, libc::F_GETFD) };
    if lock_flags < 0 || lock_flags & libc::FD_CLOEXEC != 0 {
        return Err(protocol_code(
            "SUPERVISOR_FD_INVALID",
            "fixed inherited supervisor lock descriptor is absent or cloexec",
        ));
    }
    let ceiling = descriptor_ceiling()?;
    for descriptor in (SUPERVISOR_LOCK_FD + 1)..ceiling {
        if unsafe { libc::fcntl(descriptor, libc::F_GETFD) } >= 0 {
            return Err(protocol_code(
                "SUPERVISOR_FD_INVALID",
                "supervisor inherited an unrelated descriptor",
            ));
        }
        if io::Error::last_os_error().raw_os_error() != Some(libc::EBADF) {
            return Err(protocol_code(
                "SUPERVISOR_FD_AMBIGUOUS",
                "supervisor descriptor inventory could not be inspected exactly",
            ));
        }
    }
    Ok(())
}

fn validate_null_stdio(actual: [libc::stat; 3], expected: libc::stat) -> Result<(), WorkerError> {
    let character_device = libc::S_IFCHR as libc::mode_t;
    let matches = actual.into_iter().all(|descriptor| {
        descriptor.st_mode & libc::S_IFMT == character_device
            && descriptor.st_dev == expected.st_dev
            && descriptor.st_ino == expected.st_ino
            && descriptor.st_rdev == expected.st_rdev
    });
    if expected.st_mode & libc::S_IFMT != character_device || !matches {
        return Err(protocol_code(
            "SUPERVISOR_STDIO_INVALID",
            "supervisor standard descriptors are not the null device",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessObservation {
    Matching { process_group: u32 },
    Absent,
    Reused,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessGroupObservation {
    Present,
    Absent,
    Ambiguous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessGroupMembership {
    LeaderOnly,
    OtherMembers,
    Ambiguous,
}

pub trait ProcessInspector: Send + Sync {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError>;
    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation;
    fn observe_group(&self, process_group: u32) -> ProcessGroupObservation;
    fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership;
}

#[doc(hidden)]
pub trait ReconciliationRuntime: ProcessInspector + Send + Sync {
    fn signal_process_group(&self, process_group: u32, signal: i32) -> Result<(), WorkerError>;
    fn monotonic_now(&self) -> Duration;
    fn sleep(&self, duration: Duration);
}

pub(crate) struct SystemReconciliationRuntime {
    started: Instant,
    inspector: SystemProcessInspector,
}

impl SystemReconciliationRuntime {
    pub(crate) fn new() -> Self {
        Self {
            started: Instant::now(),
            inspector: SystemProcessInspector,
        }
    }
}

impl ProcessInspector for SystemReconciliationRuntime {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        self.inspector.identity_for_pid(pid)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        self.inspector.observe(expected)
    }

    fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
        self.inspector.observe_group(process_group)
    }

    fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
        self.inspector.observe_group_members(leader)
    }
}

impl ReconciliationRuntime for SystemReconciliationRuntime {
    fn signal_process_group(&self, process_group: u32, signal: i32) -> Result<(), WorkerError> {
        if !matches!(signal, libc::SIGTERM | libc::SIGKILL) {
            return Err(protocol_code(
                "RECONCILIATION_SIGNAL_INVALID",
                "reconciliation signal is not permitted",
            ));
        }
        let process_group = i32::try_from(process_group).map_err(|_| {
            protocol_code(
                "RECONCILIATION_IDENTITY_INVALID",
                "process-group identity is outside the supported range",
            )
        })?;
        if process_group <= 0 {
            return Err(protocol_code(
                "RECONCILIATION_IDENTITY_INVALID",
                "process-group identity must be positive",
            ));
        }
        if unsafe { libc::kill(-process_group, signal) } == 0 {
            Ok(())
        } else {
            Err(WorkerError::Io(io::Error::last_os_error()))
        }
    }

    fn monotonic_now(&self) -> Duration {
        self.started.elapsed()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

pub(crate) fn reconcile_orphan_processes(
    runtime: &dyn ReconciliationRuntime,
    supervisor: ProcessIdentity,
    child: Option<ProcessIdentity>,
) -> Result<(), WorkerError> {
    if runtime.observe(supervisor) != ProcessObservation::Absent {
        return Err(reconciliation_ambiguous(
            "recorded supervisor identity is live, reused, or ambiguous",
        ));
    }
    let Some(child) = child else {
        return Ok(());
    };

    match runtime.observe(child) {
        ProcessObservation::Matching { process_group } if process_group == child.pid() => {}
        ProcessObservation::Absent => {
            return require_absent_group(runtime, child.pid());
        }
        ProcessObservation::Matching { .. }
        | ProcessObservation::Reused
        | ProcessObservation::Ambiguous => {
            return Err(reconciliation_ambiguous(
                "recorded child identity is reused, has the wrong process group, or is ambiguous",
            ));
        }
    }

    runtime.signal_process_group(child.pid(), libc::SIGTERM)?;
    wait_full_reconciliation_grace(runtime)?;
    match runtime.observe(child) {
        ProcessObservation::Absent => require_absent_group(runtime, child.pid()),
        ProcessObservation::Matching { process_group } if process_group == child.pid() => {
            runtime.signal_process_group(child.pid(), libc::SIGKILL)?;
            prove_killed_group_absent(runtime, child)
        }
        ProcessObservation::Matching { .. }
        | ProcessObservation::Reused
        | ProcessObservation::Ambiguous => Err(reconciliation_ambiguous(
            "child identity changed or became ambiguous during the TERM grace period",
        )),
    }
}

fn wait_full_reconciliation_grace(runtime: &dyn ReconciliationRuntime) -> Result<(), WorkerError> {
    let deadline = runtime
        .monotonic_now()
        .checked_add(TERM_GRACE)
        .ok_or_else(|| reconciliation_ambiguous("TERM grace deadline overflowed"))?;
    loop {
        let now = runtime.monotonic_now();
        if now >= deadline {
            return Ok(());
        }
        runtime.sleep(deadline - now);
    }
}

fn require_absent_group(
    runtime: &dyn ReconciliationRuntime,
    process_group: u32,
) -> Result<(), WorkerError> {
    if runtime.observe_group(process_group) == ProcessGroupObservation::Absent {
        Ok(())
    } else {
        Err(reconciliation_ambiguous(
            "child leader is absent but its exact process group is not proven absent",
        ))
    }
}

fn prove_killed_group_absent(
    runtime: &dyn ReconciliationRuntime,
    child: ProcessIdentity,
) -> Result<(), WorkerError> {
    let deadline = runtime
        .monotonic_now()
        .checked_add(TERM_GRACE)
        .ok_or_else(|| reconciliation_ambiguous("KILL proof deadline overflowed"))?;
    loop {
        match (runtime.observe(child), runtime.observe_group(child.pid())) {
            (ProcessObservation::Absent, ProcessGroupObservation::Absent) => return Ok(()),
            (ProcessObservation::Matching { process_group }, ProcessGroupObservation::Present)
                if process_group == child.pid() && runtime.monotonic_now() < deadline =>
            {
                let remaining = deadline - runtime.monotonic_now();
                runtime.sleep(POLL_INTERVAL.min(remaining));
            }
            _ => {
                return Err(reconciliation_ambiguous(
                    "targeted child leader and process group were not both proven absent",
                ));
            }
        }
    }
}

fn reconciliation_ambiguous(message: &str) -> WorkerError {
    protocol_code("RECONCILIATION_AMBIGUOUS", message)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemProcessInspector;

impl SystemProcessInspector {
    pub fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        <Self as ProcessInspector>::identity_for_pid(self, pid)
    }
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Clone, Copy)]
struct ProcBsdInfo {
    flags: u32,
    status: u32,
    xstatus: u32,
    pid: u32,
    ppid: u32,
    uid: u32,
    gid: u32,
    ruid: u32,
    rgid: u32,
    svuid: u32,
    svgid: u32,
    reserved: u32,
    command: [c_char; 16],
    name: [c_char; 32],
    nfiles: u32,
    process_group: u32,
    job_control_count: u32,
    controlling_device: u32,
    foreground_process_group: u32,
    nice: i32,
    start_seconds: u64,
    start_microseconds: u64,
}

#[cfg(target_os = "macos")]
#[link(name = "proc")]
unsafe extern "C" {
    fn proc_pidinfo(
        pid: c_int,
        flavor: c_int,
        arg: u64,
        buffer: *mut c_void,
        buffer_size: c_int,
    ) -> c_int;
    fn proc_listpgrppids(process_group: c_int, buffer: *mut c_void, buffer_size: c_int) -> c_int;
}

#[cfg(target_os = "macos")]
fn inspect_process(pid: u32) -> Result<Option<(ProcessIdentity, u32)>, ()> {
    const PROC_PIDTBSDINFO: c_int = 3;
    let mut info = std::mem::MaybeUninit::<ProcBsdInfo>::zeroed();
    let size = size_of::<ProcBsdInfo>();
    let result = unsafe {
        proc_pidinfo(
            pid.try_into().map_err(|_| ())?,
            PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size.try_into().map_err(|_| ())?,
        )
    };
    if result == 0 {
        return match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => Ok(None),
            _ => Err(()),
        };
    }
    if result as usize != size {
        return Err(());
    }
    let info = unsafe { info.assume_init() };
    if info.pid != pid || info.process_group == 0 || info.start_microseconds >= 1_000_000 {
        return Err(());
    }
    let start = info
        .start_seconds
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.start_microseconds))
        .ok_or(())?;
    let identity = ProcessIdentity::new(pid, start).map_err(|_| ())?;
    Ok(Some((identity, info.process_group)))
}

#[cfg(target_os = "macos")]
fn inspect_group_members(leader: u32) -> ProcessGroupMembership {
    let Ok(process_group) = c_int::try_from(leader) else {
        return ProcessGroupMembership::Ambiguous;
    };
    let mut pids = [0 as c_int; 16];
    let buffer_size = size_of_val(&pids);
    let returned = unsafe {
        proc_listpgrppids(
            process_group,
            pids.as_mut_ptr().cast(),
            buffer_size as c_int,
        )
    };
    if returned < 0 || returned as usize > pids.len() {
        return ProcessGroupMembership::Ambiguous;
    }
    for pid in pids.into_iter().take(returned as usize) {
        if pid > 0 && pid as u32 != leader {
            return ProcessGroupMembership::OtherMembers;
        }
    }
    ProcessGroupMembership::LeaderOnly
}

#[cfg(not(target_os = "macos"))]
fn inspect_process(_pid: u32) -> Result<Option<(ProcessIdentity, u32)>, ()> {
    Err(())
}

#[cfg(not(target_os = "macos"))]
fn inspect_group_members(_leader: u32) -> ProcessGroupMembership {
    ProcessGroupMembership::Ambiguous
}

impl ProcessInspector for SystemProcessInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        match inspect_process(pid) {
            Ok(Some((identity, _))) => Ok(identity),
            Ok(None) => Err(protocol_code(
                "PROCESS_ABSENT",
                "process identity was absent",
            )),
            Err(()) => Err(protocol_code(
                "PROCESS_AMBIGUOUS",
                "process identity could not be inspected exactly",
            )),
        }
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        match inspect_process(expected.pid()) {
            Ok(Some((actual, process_group))) if actual == expected => {
                ProcessObservation::Matching { process_group }
            }
            Ok(Some(_)) => ProcessObservation::Reused,
            Ok(None) => ProcessObservation::Absent,
            Err(()) => ProcessObservation::Ambiguous,
        }
    }

    fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
        let Ok(process_group) = i32::try_from(process_group) else {
            return ProcessGroupObservation::Ambiguous;
        };
        if unsafe { libc::kill(-process_group, 0) } == 0 {
            return ProcessGroupObservation::Present;
        }
        match io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => ProcessGroupObservation::Absent,
            _ => ProcessGroupObservation::Ambiguous,
        }
    }

    fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
        inspect_group_members(leader)
    }
}

pub struct SystemSupervisorLauncher {
    executable: CString,
    arguments: Vec<CString>,
    environment: Vec<CString>,
    inspector: SystemProcessInspector,
}

impl SystemSupervisorLauncher {
    pub fn new() -> Result<Self, WorkerError> {
        let executable_path = std::env::current_exe()?;
        let executable = CString::new(executable_path.as_os_str().as_bytes()).map_err(|_| {
            protocol_code(
                "SUPERVISOR_EXECUTABLE_INVALID",
                "installed worker executable path contains NUL",
            )
        })?;
        let arguments = vec![
            CString::new("worker").expect("static worker argv is valid"),
            CString::new("host").expect("static worker argv is valid"),
            CString::new("supervise").expect("static worker argv is valid"),
        ];
        let mut environment = vec![
            CString::new(format!("PATH={CONTROLLED_PATH}"))
                .expect("static controlled PATH is valid"),
        ];
        for key in [
            "HOME",
            "XDG_CONFIG_HOME",
            "XDG_STATE_HOME",
            "XDG_CACHE_HOME",
            "XDG_DATA_HOME",
        ] {
            if let Some(value) = std::env::var_os(key) {
                let mut entry = key.as_bytes().to_vec();
                entry.push(b'=');
                entry.extend_from_slice(value.as_bytes());
                environment.push(CString::new(entry).map_err(|_| {
                    protocol_code(
                        "SUPERVISOR_ENVIRONMENT_INVALID",
                        "supervisor control environment contains NUL",
                    )
                })?);
            }
        }
        Ok(Self {
            executable,
            arguments,
            environment,
            inspector: SystemProcessInspector,
        })
    }
}

impl SupervisorLauncher for SystemSupervisorLauncher {
    fn launch(
        &self,
        job_id: JobId,
        guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        guard.validate()?;
        if guard.job_id() != job_id {
            return Err(protocol_code(
                "SUPERVISOR_LOCK_MISMATCH",
                "supervisor lock belongs to another job",
            ));
        }
        let lock = guard.raw_lock_fd()?;
        let job = CString::new(job_id.to_string()).expect("job ID contains no NUL");
        let mut argument_pointers = self
            .arguments
            .iter()
            .map(|argument| argument.as_ptr())
            .collect::<Vec<_>>();
        argument_pointers.push(job.as_ptr());
        argument_pointers.push(ptr::null());
        let mut environment_pointers = self
            .environment
            .iter()
            .map(|entry| entry.as_ptr())
            .collect::<Vec<_>>();
        environment_pointers.push(ptr::null());
        let exec_result = Pipe::cloexec()?;
        let dev_null = open_dev_null()?;
        let descriptor_ceiling = descriptor_ceiling()?;
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(WorkerError::Io(io::Error::last_os_error()));
        }
        if pid == 0 {
            unsafe {
                detached_supervisor_child(
                    &self.executable,
                    &argument_pointers,
                    &environment_pointers,
                    lock,
                    dev_null.as_raw_fd(),
                    exec_result.write.as_raw_fd(),
                    descriptor_ceiling,
                )
            }
        }
        drop(exec_result.write);
        drop(dev_null);
        let result = File::from(exec_result.read);
        let mut bytes = Vec::new();
        result.take(5).read_to_end(&mut bytes)?;
        if !bytes.is_empty() {
            let _ = wait_blocking(pid);
            return Err(protocol_code(
                "SUPERVISOR_EXEC_FAILED",
                "installed worker supervisor could not be started",
            ));
        }
        let identity =
            identify_launched_supervisor(pid, &self.inspector, SUPERVISOR_IDENTITY_RETRY)?;
        drop(guard);
        Ok(LaunchCandidate::new(identity))
    }
}

fn identify_launched_supervisor(
    pid: libc::pid_t,
    inspector: &dyn ProcessInspector,
    retry_for: Duration,
) -> Result<ProcessIdentity, WorkerError> {
    let pid_u32: u32 = pid
        .try_into()
        .map_err(|_| protocol_code("SUPERVISOR_IDENTITY_INVALID", "supervisor PID is invalid"))?;
    let deadline = Instant::now()
        .checked_add(retry_for)
        .ok_or_else(|| protocol_code("SUPERVISOR_HANDSHAKE", "identity deadline overflow"))?;
    loop {
        if let Ok(identity) = inspector.identity_for_pid(pid_u32) {
            return Ok(identity);
        }
        loop {
            let mut status = 0;
            let result = unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) };
            if result == pid {
                return Err(protocol_code(
                    "SUPERVISOR_EXITED",
                    "detached supervisor exited before its identity was observed",
                ));
            }
            if result == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.raw_os_error() == Some(libc::ECHILD) {
                return Err(protocol_code(
                    "SUPERVISOR_ACCEPTANCE_AMBIGUOUS",
                    "detached supervisor ownership became ambiguous before handshake",
                ));
            }
            return Err(WorkerError::Io(error));
        }
        if Instant::now() >= deadline {
            return Err(protocol_code(
                "SUPERVISOR_ACCEPTANCE_AMBIGUOUS",
                "detached supervisor identity could not be inspected within the bounded retry",
            ));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

unsafe fn detached_supervisor_child(
    executable: &CStr,
    arguments: &[*const c_char],
    environment: &[*const c_char],
    lock: RawFd,
    dev_null: RawFd,
    exec_result: RawFd,
    descriptor_ceiling: RawFd,
) -> ! {
    if unsafe { libc::setsid() } < 0
        || unsafe { libc::dup2(dev_null, libc::STDIN_FILENO) } < 0
        || unsafe { libc::dup2(dev_null, libc::STDOUT_FILENO) } < 0
        || unsafe { libc::dup2(dev_null, libc::STDERR_FILENO) } < 0
        || unsafe { libc::dup2(lock, SUPERVISOR_LOCK_FD) } < 0
    {
        unsafe { child_fail(exec_result) };
    }
    let lock_flags = unsafe { libc::fcntl(SUPERVISOR_LOCK_FD, libc::F_GETFD) };
    if lock_flags < 0
        || unsafe {
            libc::fcntl(
                SUPERVISOR_LOCK_FD,
                libc::F_SETFD,
                lock_flags & !libc::FD_CLOEXEC,
            )
        } < 0
    {
        unsafe { child_fail(exec_result) };
    }
    let mut descriptor = 3;
    while descriptor < descriptor_ceiling {
        if descriptor != SUPERVISOR_LOCK_FD && descriptor != exec_result {
            unsafe { libc::close(descriptor) };
        }
        descriptor += 1;
    }
    unsafe {
        libc::execve(
            executable.as_ptr(),
            arguments.as_ptr(),
            environment.as_ptr(),
        )
    };
    unsafe { child_fail(exec_result) }
}

pub struct Supervisor<'a> {
    store: &'a HostStore,
    inspector: &'a dyn ProcessInspector,
    fault: Option<SupervisorFaultPoint>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorFaultPoint {
    AfterSupervisorIdentity,
    AfterPayloadRead,
    AfterChildReady,
    BeforePayloadErase,
    AfterChildIdentity,
    AfterPayloadErase,
    AfterGo,
    AfterExecAck,
    BeforeChildStatusAbortProof,
    BeforeRunningStatus,
    AfterRunningStatus,
    AfterLogSync,
    AfterTerminalStatus,
}

impl<'a> Supervisor<'a> {
    pub fn new(store: &'a HostStore, inspector: &'a dyn ProcessInspector) -> Self {
        Self {
            store,
            inspector,
            fault: None,
        }
    }

    #[doc(hidden)]
    pub fn new_with_fault(
        store: &'a HostStore,
        inspector: &'a dyn ProcessInspector,
        fault: SupervisorFaultPoint,
    ) -> Self {
        Self {
            store,
            inspector,
            fault: Some(fault),
        }
    }

    pub fn run_with_guard(
        &self,
        job_id: JobId,
        mut guard: SupervisorGuard,
    ) -> Result<(), WorkerError> {
        guard.validate()?;
        if guard.job_id() != job_id {
            return Err(protocol_code(
                "SUPERVISOR_LOCK_MISMATCH",
                "supervisor lock belongs to another job",
            ));
        }
        let lease = LeaseService::new(self.store)
            .load()?
            .ok_or_else(|| protocol_code("LEASE_MISSING", "matching live lease is absent"))?;
        if lease.job_id() != job_id {
            return Err(protocol_code(
                "LEASE_IDENTITY_MISMATCH",
                "live lease belongs to another job",
            ));
        }
        require_accepted_disposition(self.store.disposition(job_id)?, &lease)?;
        let job = self.store.open_directory(
            &format!(
                "jobs/{}/{}/{}",
                lease.project_id(),
                lease.worktree_id(),
                lease.job_id()
            ),
            false,
        )?;
        let meta: JobMeta = read_canonical_json(&job, "meta.json")?;
        require_meta_matches_lease(&meta, &lease)?;
        let (mut status_bytes, mut status): (Vec<u8>, JobStatus) =
            read_canonical_json_with_bytes(&job, "status.json")?;
        if status.state() != JobState::Accepted || status.supervisor_identity().is_some() {
            return Err(protocol_code(
                "SUPERVISOR_ALREADY_RECORDED",
                "durable supervisor identity already exists",
            ));
        }

        let supervisor_identity = self.inspector.identity_for_pid(std::process::id())?;
        let supervised = status.with_supervisor(supervisor_identity, now_millis()?)?;
        replace_status(
            self.store,
            &lease,
            &mut guard,
            &job,
            &status_bytes,
            &status,
            &supervised,
        )?;
        status = supervised;
        status_bytes = canonical_json(&status)?;
        if self.fault == Some(SupervisorFaultPoint::AfterSupervisorIdentity) {
            return Err(injected_supervisor_fault("supervisor identity"));
        }

        // A successful descriptor-bound read establishes ownership of this
        // transient payload. Every later zero-execution failure path must
        // erase it (and fsync the parent through RootedDir) before returning.
        // If the read itself cannot prove the pathname binding, fail closed
        // without mutating whatever now occupies the canonical name.
        let payload_bytes = job.read_private_regular("execution.json", MAX_HOST_JSON_BYTES)?;
        let payload: ExecutionPayload = match parse_canonical_json(&payload_bytes) {
            Ok(payload) => payload,
            Err(error) => {
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    "EXECUTION_PAYLOAD_INVALID",
                    error,
                );
            }
        };
        if let Err(error) = payload.validate_for_durable_job(&lease, &meta) {
            return self.finish_owned_prelaunch_failure(
                &lease,
                &job,
                &status_bytes,
                &status,
                guard,
                "EXECUTION_PAYLOAD_INVALID",
                error,
            );
        }
        if self.fault == Some(SupervisorFaultPoint::AfterPayloadRead) {
            return self.finish_owned_prelaunch_failure(
                &lease,
                &job,
                &status_bytes,
                &status,
                guard,
                "CRASH_AFTER_PAYLOAD_READ",
                injected_supervisor_fault("payload read"),
            );
        }
        let workspace = match relative("workspace/tree").and_then(|path| {
            job.open_child_directory(&path, false)
                .map_err(WorkerError::Io)
        }) {
            Ok(workspace) => workspace,
            Err(error) => {
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    "COMMAND_PREPARATION_FAILED",
                    error,
                );
            }
        };
        let cwd = if meta.relative_working_dir().is_empty() {
            workspace
        } else {
            match relative(meta.relative_working_dir()).and_then(|path| {
                workspace
                    .open_child_directory(&path, false)
                    .map_err(WorkerError::Io)
            }) {
                Ok(cwd) => cwd,
                Err(error) => {
                    return self.finish_owned_prelaunch_failure(
                        &lease,
                        &job,
                        &status_bytes,
                        &status,
                        guard,
                        "COMMAND_PREPARATION_FAILED",
                        error,
                    );
                }
            }
        };
        let stdout = match job.open_private_append("stdout.log") {
            Ok(stdout) => stdout,
            Err(error) => {
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    "LOG_BINDING_INVALID",
                    WorkerError::Io(error),
                );
            }
        };
        let stderr = match job.open_private_append("stderr.log") {
            Ok(stderr) => stderr,
            Err(error) => {
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    "LOG_BINDING_INVALID",
                    WorkerError::Io(error),
                );
            }
        };
        let job_path = match self
            .store
            .job(lease.project_id(), lease.worktree_id(), lease.job_id())
        {
            Ok(job_path) => job_path,
            Err(error) => {
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    "COMMAND_PREPARATION_FAILED",
                    error,
                );
            }
        };
        let home_path = job_path.join("home");
        let tmp_path = job_path.join("tmp");
        let command = match PreparedCommand::new(payload.command(), &lease, &home_path, &tmp_path) {
            Ok(command) => command,
            Err(error) => {
                let error_code = prelaunch_error_code(&error);
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    error_code,
                    error,
                );
            }
        };

        let mut child = match GatedChild::spawn(
            &command,
            cwd.raw_directory_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            self.inspector,
        ) {
            Ok(child) => child,
            Err(error) => {
                let error_code = prelaunch_error_code(&error);
                return self.finish_owned_prelaunch_failure(
                    &lease,
                    &job,
                    &status_bytes,
                    &status,
                    guard,
                    error_code,
                    error,
                );
            }
        };
        if self.fault == Some(SupervisorFaultPoint::AfterChildReady) {
            let abort_result = child.abort_and_reap();
            let terminal = match erase_payload_and_record_prelaunch(
                self.store,
                &lease,
                &mut guard,
                &job,
                &status_bytes,
                &status,
                "CRASH_AFTER_CHILD_READY",
            ) {
                Ok(terminal) => terminal,
                Err(error) => {
                    drop(guard);
                    return Err(error);
                }
            };
            drop(guard);
            abort_result?;
            self.cleanup_and_release(&lease, &job, terminal)?;
            return Err(injected_supervisor_fault("child READY"));
        }
        let child_identity = child.identity();
        let child_status = if self.fault == Some(SupervisorFaultPoint::BeforeChildStatusAbortProof)
        {
            Err(WorkerError::Io(io::Error::other(
                "injected child status write failure",
            )))
        } else {
            now_millis()
                .and_then(|updated_at| status.with_child(child_identity, updated_at))
                .and_then(|child_status| {
                    replace_status(
                        self.store,
                        &lease,
                        &mut guard,
                        &job,
                        &status_bytes,
                        &status,
                        &child_status,
                    )?;
                    Ok(child_status)
                })
        };
        let child_status = match child_status {
            Ok(child_status) => child_status,
            Err(error) => {
                let abort_result = child.abort_and_reap().and_then(|()| {
                    if self.fault == Some(SupervisorFaultPoint::BeforeChildStatusAbortProof) {
                        Err(protocol_code(
                            "CHILD_ABORT_AMBIGUOUS",
                            "injected child abort proof ambiguity",
                        ))
                    } else {
                        Ok(())
                    }
                });
                let terminal = match erase_payload_and_record_prelaunch(
                    self.store,
                    &lease,
                    &mut guard,
                    &job,
                    &status_bytes,
                    &status,
                    "CHILD_STATUS_WRITE_FAILED",
                ) {
                    Ok(terminal) => terminal,
                    Err(erase_error) => {
                        drop(guard);
                        return Err(erase_error);
                    }
                };
                drop(guard);
                abort_result?;
                self.cleanup_and_release(&lease, &job, terminal)?;
                return Err(error);
            }
        };
        status = child_status;
        status_bytes = canonical_json(&status)?;
        if self.fault == Some(SupervisorFaultPoint::AfterChildIdentity) {
            let abort_result = child.abort_and_reap();
            let terminal = match erase_payload_and_record_prelaunch(
                self.store,
                &lease,
                &mut guard,
                &job,
                &status_bytes,
                &status,
                "CHILD_PRE_GO_FAILED",
            ) {
                Ok(terminal) => terminal,
                Err(error) => {
                    drop(guard);
                    return Err(error);
                }
            };
            drop(guard);
            abort_result?;
            self.cleanup_and_release(&lease, &job, terminal)?;
            return Err(injected_supervisor_fault("child identity"));
        }

        let payload_removal = if self.fault == Some(SupervisorFaultPoint::BeforePayloadErase) {
            Err(io::Error::other("injected supervisor fault"))
        } else {
            job.remove_owned_regular("execution.json")
        };
        if let Err(error) = payload_removal {
            child.abort();
            let _ = record_prelaunch_failure(
                self.store,
                &lease,
                &mut guard,
                &job,
                &status_bytes,
                &status,
                &stdout,
                &stderr,
                "PAYLOAD_ERASURE_FAILED",
            );
            drop(guard);
            return Err(WorkerError::Io(error));
        }
        if self.fault == Some(SupervisorFaultPoint::AfterPayloadErase) {
            let abort_result = child.abort_and_reap();
            drop(guard);
            abort_result?;
            return Err(injected_supervisor_fault("payload erasure"));
        }

        match child.allow_exec(self.fault) {
            Ok(()) => {}
            Err(ExecStartError::ProvenPrelaunch { code, error }) => {
                child.abort();
                let terminal = prelaunch_terminal(
                    self.store,
                    &lease,
                    &mut guard,
                    &job,
                    &status_bytes,
                    &status,
                    &stdout,
                    &stderr,
                    code,
                )?;
                drop(guard);
                self.cleanup_and_release(&lease, &job, terminal)?;
                return Err(error);
            }
            Err(ExecStartError::Ambiguous(error)) => {
                finish_ambiguous_child(
                    self.store,
                    &lease,
                    &mut guard,
                    &job,
                    &status_bytes,
                    &status,
                    &stdout,
                    &stderr,
                    child_identity,
                    lease.timeout_millis(),
                    self.inspector,
                    "EXEC_ACK_AMBIGUOUS",
                )?;
                drop(guard);
                return Err(error);
            }
        }

        let running_result = if self.fault == Some(SupervisorFaultPoint::BeforeRunningStatus) {
            Err(WorkerError::Io(io::Error::other(
                "injected supervisor fault",
            )))
        } else {
            now_millis()
                .and_then(|updated_at| status.into_running(updated_at))
                .and_then(|running| {
                    replace_status(
                        self.store,
                        &lease,
                        &mut guard,
                        &job,
                        &status_bytes,
                        &status,
                        &running,
                    )?;
                    Ok(running)
                })
        };
        let running = match running_result {
            Ok(running) => running,
            Err(error) => {
                finish_ambiguous_child(
                    self.store,
                    &lease,
                    &mut guard,
                    &job,
                    &status_bytes,
                    &status,
                    &stdout,
                    &stderr,
                    child_identity,
                    lease.timeout_millis(),
                    self.inspector,
                    "RUNNING_STATUS_WRITE_FAILED",
                )?;
                drop(guard);
                return Err(error);
            }
        };
        status = running;
        status_bytes = canonical_json(&status)?;

        if self.fault == Some(SupervisorFaultPoint::AfterRunningStatus) {
            let _ = wait_for_child(child_identity, lease.timeout_millis(), self.inspector)?;
            return Err(injected_supervisor_fault("running status"));
        }

        let outcome = wait_for_child(child_identity, lease.timeout_millis(), self.inspector)?;
        stdout.sync_all()?;
        stderr.sync_all()?;
        if self.fault == Some(SupervisorFaultPoint::AfterLogSync) {
            return Err(injected_supervisor_fault("log sync"));
        }
        let stdout_length = job.validate_private_append_binding("stdout.log", &stdout)?;
        let stderr_length = job.validate_private_append_binding("stderr.log", &stderr)?;
        let terminal = match outcome {
            ChildOutcome::Exited(0) => {
                status.into_succeeded(now_millis()?, stdout_length, stderr_length)?
            }
            ChildOutcome::Exited(code) => {
                status.into_failed_exit(now_millis()?, code, stdout_length, stderr_length)?
            }
            ChildOutcome::Signalled(signal) => {
                status.into_failed_signal(now_millis()?, signal, stdout_length, stderr_length)?
            }
            ChildOutcome::TimedOut => status.into_infrastructure_terminal(
                JobState::TimedOut,
                now_millis()?,
                stdout_length,
                stderr_length,
                "COMMAND_TIMEOUT".into(),
            )?,
        };
        replace_status(
            self.store,
            &lease,
            &mut guard,
            &job,
            &status_bytes,
            &status,
            &terminal,
        )?;
        if self.fault == Some(SupervisorFaultPoint::AfterTerminalStatus) {
            return Err(injected_supervisor_fault("terminal status"));
        }
        drop(guard);
        self.cleanup_and_release(&lease, &job, terminal)
    }

    fn cleanup_and_release(
        &self,
        lease: &LeaseRecord,
        job: &RootedDir,
        terminal: JobStatus,
    ) -> Result<(), WorkerError> {
        match self.store.cleanup_job_owned(lease) {
            Ok(receipt) => {
                if let Err(error) =
                    LeaseService::new(self.store).release_after_cleanup(lease, &receipt)
                {
                    enrich_cleanup_error(
                        self.store,
                        lease,
                        job,
                        &terminal,
                        "LEASE_RELEASE_FAILED",
                    )?;
                    return Err(error);
                }
                Ok(())
            }
            Err(error) => {
                enrich_cleanup_error(self.store, lease, job, &terminal, "MUTABLE_CLEANUP_FAILED")?;
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_owned_prelaunch_failure(
        &self,
        lease: &LeaseRecord,
        job: &RootedDir,
        expected_status_bytes: &[u8],
        status: &JobStatus,
        mut guard: SupervisorGuard,
        code: &str,
        original: WorkerError,
    ) -> Result<(), WorkerError> {
        let terminal = match erase_payload_and_record_prelaunch(
            self.store,
            lease,
            &mut guard,
            job,
            expected_status_bytes,
            status,
            code,
        ) {
            Ok(terminal) => terminal,
            Err(error) => {
                drop(guard);
                return Err(error);
            }
        };
        drop(guard);
        self.cleanup_and_release(lease, job, terminal)?;
        Err(original)
    }
}

struct PreparedCommand {
    program: CString,
    _arguments: Vec<CString>,
    argument_pointers: Vec<*const c_char>,
    _environment: Vec<CString>,
    environment_pointers: Vec<*const c_char>,
}

impl PreparedCommand {
    fn new(
        command: &CommandSpec,
        lease: &LeaseRecord,
        home: &Path,
        tmp: &Path,
    ) -> Result<Self, WorkerError> {
        command.validate()?;
        let (program, arguments) = match command {
            CommandSpec::Argv { argv } => {
                let executable = resolve_executable(&argv[0])?;
                (executable, argv.clone())
            }
            CommandSpec::Shell { shell } => (
                PathBuf::from("/bin/zsh"),
                vec!["/bin/zsh".into(), "-lc".into(), shell.clone()],
            ),
        };
        let program = CString::new(program.as_os_str().as_bytes())
            .map_err(|_| protocol_code("INVALID_EXECUTABLE", "executable path contains NUL"))?;
        let arguments = arguments
            .into_iter()
            .map(|argument| {
                CString::new(argument)
                    .map_err(|_| protocol_code("INVALID_COMMAND", "command argument contains NUL"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut environment = [
            "LC_ALL=C".to_owned(),
            "LANG=C".to_owned(),
            format!("PATH={CONTROLLED_PATH}"),
        ]
        .into_iter()
        .map(controlled_environment_entry)
        .collect::<Result<Vec<_>, _>>()?;
        environment.push(controlled_path_environment_entry(b"HOME=", home)?);
        environment.push(controlled_path_environment_entry(b"TMPDIR=", tmp)?);
        environment.extend(
            [
                format!("MAC_WORKER_JOB_ID={}", lease.job_id()),
                format!("MAC_WORKER_CLIENT_ID={}", lease.client_id()),
                format!("MAC_WORKER_PROJECT_ID={}", lease.project_id()),
                format!("MAC_WORKER_WORKTREE_ID={}", lease.worktree_id()),
            ]
            .into_iter()
            .map(controlled_environment_entry)
            .collect::<Result<Vec<_>, _>>()?,
        );
        let mut argument_pointers = arguments
            .iter()
            .map(|argument| argument.as_ptr())
            .collect::<Vec<_>>();
        argument_pointers.push(ptr::null());
        let mut environment_pointers = environment
            .iter()
            .map(|entry| entry.as_ptr())
            .collect::<Vec<_>>();
        environment_pointers.push(ptr::null());
        Ok(Self {
            program,
            _arguments: arguments,
            argument_pointers,
            _environment: environment,
            environment_pointers,
        })
    }
}

fn controlled_environment_entry(entry: String) -> Result<CString, WorkerError> {
    CString::new(entry)
        .map_err(|_| protocol_code("INVALID_ENVIRONMENT", "controlled environment is invalid"))
}

fn controlled_path_environment_entry(key: &[u8], path: &Path) -> Result<CString, WorkerError> {
    let path = path.as_os_str().as_bytes();
    let mut entry = Vec::with_capacity(key.len() + path.len());
    entry.extend_from_slice(key);
    entry.extend_from_slice(path);
    CString::new(entry)
        .map_err(|_| protocol_code("INVALID_ENVIRONMENT", "controlled environment is invalid"))
}

struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
}

impl Pipe {
    fn cloexec() -> io::Result<Self> {
        let mut descriptors = [0; 2];
        if unsafe { libc::pipe(descriptors.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let read = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
        let write = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
        set_cloexec(read.as_raw_fd())?;
        set_cloexec(write.as_raw_fd())?;
        Ok(Self { read, write })
    }
}

struct GatedChild {
    pid: libc::pid_t,
    identity: ProcessIdentity,
    go: Option<OwnedFd>,
    exec_result: Option<OwnedFd>,
}

#[derive(Debug)]
enum ExecStartError {
    ProvenPrelaunch {
        code: &'static str,
        error: WorkerError,
    },
    Ambiguous(WorkerError),
}

impl std::fmt::Display for ExecStartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProvenPrelaunch { error, .. } | Self::Ambiguous(error) => error.fmt(formatter),
        }
    }
}

impl GatedChild {
    fn spawn(
        command: &PreparedCommand,
        cwd: RawFd,
        stdout: RawFd,
        stderr: RawFd,
        inspector: &dyn ProcessInspector,
    ) -> Result<Self, WorkerError> {
        let ready = Pipe::cloexec()?;
        let go = Pipe::cloexec()?;
        let exec_result = Pipe::cloexec()?;
        let dev_null = open_dev_null()?;
        let descriptor_ceiling = descriptor_ceiling()?;
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(WorkerError::Io(io::Error::last_os_error()));
        }
        if pid == 0 {
            unsafe {
                gated_child_main(
                    command,
                    cwd,
                    stdout,
                    stderr,
                    dev_null.as_raw_fd(),
                    ready.write.as_raw_fd(),
                    go.read.as_raw_fd(),
                    exec_result.write.as_raw_fd(),
                    descriptor_ceiling,
                )
            }
        }

        drop(ready.write);
        drop(go.read);
        drop(exec_result.write);
        drop(dev_null);
        let mut go_write = Some(go.write);
        let mut exec_result_read = Some(exec_result.read);
        let mut ready_file = File::from(ready.read);
        let mut byte = [0_u8; 1];
        let ready_count = match ready_file.read(&mut byte) {
            Ok(count) => count,
            Err(error) => {
                abort_pre_go(pid, &mut go_write, &mut exec_result_read);
                return Err(WorkerError::Io(error));
            }
        };
        if ready_count != 1 || byte[0] != 1 {
            abort_pre_go(pid, &mut go_write, &mut exec_result_read);
            return Err(protocol_code(
                "CHILD_READY_FAILED",
                "gated child did not reach its ready boundary",
            ));
        }
        let pid_u32: u32 = match pid.try_into() {
            Ok(pid) => pid,
            Err(_) => {
                abort_pre_go(pid, &mut go_write, &mut exec_result_read);
                return Err(protocol_code(
                    "CHILD_IDENTITY_INVALID",
                    "child PID is invalid",
                ));
            }
        };
        let identity = match inspector.identity_for_pid(pid_u32) {
            Ok(identity) => identity,
            Err(error) => {
                abort_pre_go(pid, &mut go_write, &mut exec_result_read);
                return Err(error);
            }
        };
        match inspector.observe(identity) {
            ProcessObservation::Matching { process_group } if process_group == pid_u32 => {}
            _ => {
                abort_pre_go(pid, &mut go_write, &mut exec_result_read);
                return Err(protocol_code(
                    "CHILD_IDENTITY_AMBIGUOUS",
                    "gated child process-group identity was not exact",
                ));
            }
        }
        Ok(Self {
            pid,
            identity,
            go: go_write,
            exec_result: exec_result_read,
        })
    }

    fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    fn allow_exec(&mut self, fault: Option<SupervisorFaultPoint>) -> Result<(), ExecStartError> {
        if self.go.is_none() {
            return Err(ExecStartError::ProvenPrelaunch {
                code: "CHILD_GATE_INVALID",
                error: protocol_code(
                    "CHILD_GATE_INVALID",
                    "child execution gate was already consumed",
                ),
            });
        }
        if self.exec_result.is_none() {
            return Err(ExecStartError::ProvenPrelaunch {
                code: "CHILD_EXEC_INVALID",
                error: protocol_code(
                    "CHILD_EXEC_INVALID",
                    "child exec result capability is absent",
                ),
            });
        }
        let go = self.go.take().expect("validated child gate is present");
        let result = self
            .exec_result
            .take()
            .expect("validated exec result capability is present");
        let mut go = File::from(go);
        if let Err(error) = go.write_all(&[1]) {
            drop(go);
            drop(result);
            return match wait_blocking(self.pid) {
                Ok(_) => Err(ExecStartError::ProvenPrelaunch {
                    code: "CHILD_GO_FAILED",
                    error: WorkerError::Io(error),
                }),
                Err(wait_error) => Err(ExecStartError::Ambiguous(wait_error)),
            };
        }
        drop(go);
        if fault == Some(SupervisorFaultPoint::AfterGo) {
            drop(result);
            return Err(ExecStartError::Ambiguous(injected_supervisor_fault(
                "child GO",
            )));
        }
        let result = File::from(result);
        let mut bytes = Vec::new();
        result
            .take(5)
            .read_to_end(&mut bytes)
            .map_err(|error| ExecStartError::Ambiguous(WorkerError::Io(error)))?;
        if bytes.is_empty() {
            if fault == Some(SupervisorFaultPoint::AfterExecAck) {
                return Err(ExecStartError::Ambiguous(injected_supervisor_fault(
                    "exec acknowledgement",
                )));
            }
            return Ok(());
        }
        match wait_blocking(self.pid) {
            Ok(_) => Err(ExecStartError::ProvenPrelaunch {
                code: "EXEC_FAILED",
                error: protocol_code("CHILD_EXEC_FAILED", "child executable could not be started"),
            }),
            Err(error) => Err(ExecStartError::Ambiguous(error)),
        }
    }

    fn abort(&mut self) {
        let _ = self.abort_and_reap();
    }

    fn abort_and_reap(&mut self) -> Result<(), WorkerError> {
        if self.go.is_none() {
            return Ok(());
        }
        drop(self.go.take());
        drop(self.exec_result.take());
        wait_blocking(self.pid).map(|_| ())
    }
}

impl Drop for GatedChild {
    fn drop(&mut self) {
        self.abort();
    }
}

fn abort_pre_go(pid: libc::pid_t, go: &mut Option<OwnedFd>, exec_result: &mut Option<OwnedFd>) {
    drop(go.take());
    drop(exec_result.take());
    let _ = wait_blocking(pid);
}

#[allow(clippy::too_many_arguments)]
unsafe fn gated_child_main(
    command: &PreparedCommand,
    cwd: RawFd,
    stdout: RawFd,
    stderr: RawFd,
    dev_null: RawFd,
    ready: RawFd,
    go: RawFd,
    exec_result: RawFd,
    descriptor_ceiling: RawFd,
) -> ! {
    if unsafe { libc::setpgid(0, 0) } != 0
        || unsafe { libc::fchdir(cwd) } != 0
        || unsafe { libc::dup2(dev_null, libc::STDIN_FILENO) } < 0
        || unsafe { libc::dup2(stdout, libc::STDOUT_FILENO) } < 0
        || unsafe { libc::dup2(stderr, libc::STDERR_FILENO) } < 0
    {
        unsafe { child_fail(exec_result) };
    }
    let mut descriptor = 3;
    while descriptor < descriptor_ceiling {
        if descriptor != ready && descriptor != go && descriptor != exec_result {
            unsafe { libc::close(descriptor) };
        }
        descriptor += 1;
    }
    if unsafe { libc::write(ready, [1_u8].as_ptr().cast(), 1) } != 1 {
        unsafe { libc::_exit(70) };
    }
    unsafe { libc::close(ready) };
    let mut byte = 0_u8;
    if unsafe { libc::read(go, (&raw mut byte).cast(), 1) } != 1 || byte != 1 {
        unsafe { libc::_exit(70) };
    }
    unsafe { libc::close(go) };
    unsafe {
        libc::execve(
            command.program.as_ptr(),
            command.argument_pointers.as_ptr(),
            command.environment_pointers.as_ptr(),
        )
    };
    unsafe { child_fail(exec_result) }
}

unsafe fn child_fail(exec_result: RawFd) -> ! {
    #[cfg(target_os = "macos")]
    let error = unsafe { *libc::__error() };
    #[cfg(not(target_os = "macos"))]
    let error = unsafe { *libc::__errno_location() };
    let bytes = error.to_ne_bytes();
    unsafe { libc::write(exec_result, bytes.as_ptr().cast(), bytes.len()) };
    unsafe { libc::_exit(70) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildOutcome {
    Exited(u8),
    Signalled(u32),
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedChildState {
    MatchingGroup,
    Waitable,
    Transitioning,
}

fn wait_for_child(
    identity: ProcessIdentity,
    timeout_millis: u64,
    inspector: &dyn ProcessInspector,
) -> Result<ChildOutcome, WorkerError> {
    let pid: libc::pid_t = identity
        .pid()
        .try_into()
        .map_err(|_| protocol_code("CHILD_IDENTITY_INVALID", "child PID is invalid"))?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(timeout_millis))
        .ok_or_else(|| protocol_code("TIMEOUT_INVALID", "child deadline overflow"))?;
    loop {
        if child_is_waitable(pid)? {
            return complete_waitable_child(identity, inspector);
        }
        if Instant::now() >= deadline {
            return terminate_exact_group(identity, inspector);
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn complete_waitable_child(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<ChildOutcome, WorkerError> {
    let pid: libc::pid_t = identity
        .pid()
        .try_into()
        .map_err(|_| protocol_code("CHILD_IDENTITY_INVALID", "child PID is invalid"))?;
    if !child_is_waitable(pid)? {
        return Err(protocol_code(
            "CHILD_WAIT_AMBIGUOUS",
            "child lost its waitable identity anchor",
        ));
    }
    match inspector.observe_group_members(identity.pid()) {
        ProcessGroupMembership::LeaderOnly => {}
        ProcessGroupMembership::OtherMembers => {
            if unsafe { libc::kill(-pid, libc::SIGTERM) } != 0 {
                let error = io::Error::last_os_error();
                if !failed_group_signal_has_only_waitable_leader(identity, inspector)? {
                    return Err(WorkerError::Io(error));
                }
            } else {
                let deadline = Instant::now() + TERM_GRACE;
                loop {
                    if !child_is_waitable(pid)? {
                        return Err(protocol_code(
                            "CHILD_WAIT_AMBIGUOUS",
                            "child lost its waitable identity anchor during group cleanup",
                        ));
                    }
                    match inspector.observe_group_members(identity.pid()) {
                        ProcessGroupMembership::LeaderOnly => break,
                        ProcessGroupMembership::OtherMembers if Instant::now() < deadline => {
                            std::thread::sleep(POLL_INTERVAL);
                        }
                        ProcessGroupMembership::OtherMembers => {
                            if unsafe { libc::kill(-pid, libc::SIGKILL) } != 0 {
                                let error = io::Error::last_os_error();
                                if !failed_group_signal_has_only_waitable_leader(
                                    identity, inspector,
                                )? {
                                    return Err(WorkerError::Io(error));
                                }
                            }
                            break;
                        }
                        ProcessGroupMembership::Ambiguous => {
                            return Err(protocol_code(
                                "CHILD_TERMINATION_AMBIGUOUS",
                                "completed child process-group membership could not be inspected",
                            ));
                        }
                    }
                }
            }
        }
        ProcessGroupMembership::Ambiguous => {
            return Err(protocol_code(
                "CHILD_TERMINATION_AMBIGUOUS",
                "completed child process-group membership could not be inspected",
            ));
        }
    }
    let outcome = wait_blocking(pid)?;
    wait_for_terminated_group_absence(identity, inspector)?;
    Ok(outcome)
}

fn terminate_exact_group(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<ChildOutcome, WorkerError> {
    let pid = identity.pid();
    match wait_for_owned_child_state(identity, inspector, CHILD_TRANSITION_RETRY)? {
        OwnedChildState::Waitable => return complete_waitable_child(identity, inspector),
        OwnedChildState::MatchingGroup => {}
        OwnedChildState::Transitioning => unreachable!("bounded ownership wait returns a state"),
    }
    if unsafe { libc::kill(-(pid as i32), libc::SIGTERM) } != 0 {
        let error = WorkerError::Io(io::Error::last_os_error());
        return reconcile_failed_group_signal(identity, inspector, error);
    }
    let deadline = Instant::now() + TERM_GRACE;
    let final_state = loop {
        // An exact waitid(P_PID, ..., WNOWAIT) event retains this child as a
        // waitable zombie. Its PID cannot be reused before we reap it, so the
        // already-validated PID/start/PGID remains the authority for a final
        // group signal even though macOS proc_pidinfo no longer reports the
        // zombie as a live matching process.
        let state = observe_owned_child_state(identity, inspector)?;
        if Instant::now() >= deadline {
            break match state {
                OwnedChildState::Transitioning => {
                    wait_for_owned_child_state(identity, inspector, CHILD_TRANSITION_RETRY)?
                }
                observed => observed,
            };
        }
        std::thread::sleep(POLL_INTERVAL);
    };
    let should_kill = match final_state {
        OwnedChildState::Waitable => match inspector.observe_group_members(pid) {
            ProcessGroupMembership::LeaderOnly => false,
            ProcessGroupMembership::OtherMembers => true,
            ProcessGroupMembership::Ambiguous => {
                return Err(protocol_code(
                    "CHILD_TERMINATION_AMBIGUOUS",
                    "exact process-group membership could not be inspected",
                ));
            }
        },
        OwnedChildState::MatchingGroup => true,
        OwnedChildState::Transitioning => unreachable!("bounded ownership wait returns a state"),
    };
    if should_kill && unsafe { libc::kill(-(pid as i32), libc::SIGKILL) } != 0 {
        let error = io::Error::last_os_error();
        if !failed_group_signal_has_only_waitable_leader(identity, inspector)? {
            return Err(WorkerError::Io(error));
        }
    }
    wait_blocking(pid as libc::pid_t)?;
    wait_for_terminated_group_absence(identity, inspector)?;
    Ok(ChildOutcome::TimedOut)
}

fn reconcile_failed_group_signal(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
    original: WorkerError,
) -> Result<ChildOutcome, WorkerError> {
    match wait_after_failed_group_signal(identity, inspector)? {
        OwnedChildState::Waitable => complete_waitable_child(identity, inspector),
        OwnedChildState::MatchingGroup => Err(original),
        OwnedChildState::Transitioning => unreachable!("bounded ownership wait returns a state"),
    }
}

fn failed_group_signal_has_only_waitable_leader(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<bool, WorkerError> {
    match wait_after_failed_group_signal(identity, inspector)? {
        OwnedChildState::Waitable => match inspector.observe_group_members(identity.pid()) {
            ProcessGroupMembership::LeaderOnly => Ok(true),
            ProcessGroupMembership::OtherMembers => Ok(false),
            ProcessGroupMembership::Ambiguous => Err(protocol_code(
                "CHILD_TERMINATION_AMBIGUOUS",
                "failed group signal could not be reconciled to the waitable leader anchor",
            )),
        },
        OwnedChildState::MatchingGroup => Ok(false),
        OwnedChildState::Transitioning => unreachable!("bounded ownership wait returns a state"),
    }
}

fn wait_after_failed_group_signal(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<OwnedChildState, WorkerError> {
    let deadline = Instant::now()
        .checked_add(CHILD_TRANSITION_RETRY)
        .ok_or_else(|| protocol_code("CHILD_IDENTITY_AMBIGUOUS", "identity deadline overflow"))?;
    loop {
        let state = observe_owned_child_state(identity, inspector)?;
        if state == OwnedChildState::Waitable {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return match state {
                OwnedChildState::MatchingGroup => Ok(state),
                OwnedChildState::Transitioning => Err(protocol_code(
                    "CHILD_IDENTITY_AMBIGUOUS",
                    "failed group signal left neither an exact live child group nor its waitable anchor",
                )),
                OwnedChildState::Waitable => unreachable!("waitable state returns immediately"),
            };
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn observe_owned_child_state(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<OwnedChildState, WorkerError> {
    if child_is_waitable(identity.pid() as libc::pid_t)? {
        return Ok(OwnedChildState::Waitable);
    }
    Ok(match inspector.observe(identity) {
        ProcessObservation::Matching { process_group } if process_group == identity.pid() => {
            OwnedChildState::MatchingGroup
        }
        _ => OwnedChildState::Transitioning,
    })
}

fn wait_for_owned_child_state(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
    retry_for: Duration,
) -> Result<OwnedChildState, WorkerError> {
    let deadline = Instant::now()
        .checked_add(retry_for)
        .ok_or_else(|| protocol_code("CHILD_IDENTITY_AMBIGUOUS", "identity deadline overflow"))?;
    loop {
        let state = observe_owned_child_state(identity, inspector)?;
        if state != OwnedChildState::Transitioning {
            return Ok(state);
        }
        if Instant::now() >= deadline {
            return Err(protocol_code(
                "CHILD_IDENTITY_AMBIGUOUS",
                "neither an exact live child group nor its waitable anchor became observable",
            ));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_terminated_group_absence(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<(), WorkerError> {
    let deadline = Instant::now() + TERM_GRACE;
    loop {
        let leader = inspector.observe(identity);
        let group = inspector.observe_group(identity.pid());
        match (leader, group) {
            (
                ProcessObservation::Absent | ProcessObservation::Reused,
                ProcessGroupObservation::Absent,
            ) => return Ok(()),
            _ if Instant::now() < deadline => {
                std::thread::sleep(POLL_INTERVAL);
            }
            _ => {
                return Err(protocol_code(
                    "CHILD_TERMINATION_AMBIGUOUS",
                    "targeted child leader and process group were not both proven absent",
                ));
            }
        }
    }
}

fn child_is_waitable(pid: libc::pid_t) -> Result<bool, WorkerError> {
    loop {
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            let info = unsafe { info.assume_init() };
            let observed = unsafe { info.si_pid() };
            if observed == 0 {
                return Ok(false);
            }
            if observed == pid {
                return Ok(true);
            }
            return Err(protocol_code(
                "CHILD_WAIT_AMBIGUOUS",
                "waitid reported another child identity",
            ));
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(WorkerError::Io(error));
    }
}

fn wait_blocking(pid: libc::pid_t) -> Result<ChildOutcome, WorkerError> {
    let mut status = 0;
    loop {
        let result = unsafe { libc::waitpid(pid, &raw mut status, 0) };
        if result == pid {
            return decode_wait_status(status);
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(WorkerError::Io(error));
    }
}

fn decode_wait_status(status: c_int) -> Result<ChildOutcome, WorkerError> {
    if libc::WIFEXITED(status) {
        return Ok(ChildOutcome::Exited(libc::WEXITSTATUS(status) as u8));
    }
    if libc::WIFSIGNALED(status) {
        return Ok(ChildOutcome::Signalled(libc::WTERMSIG(status) as u32));
    }
    Err(protocol_code(
        "CHILD_WAIT_AMBIGUOUS",
        "child wait status was not terminal",
    ))
}

#[allow(clippy::too_many_arguments)]
fn prelaunch_terminal(
    store: &HostStore,
    lease: &LeaseRecord,
    status_guard: &mut SupervisorGuard,
    job: &RootedDir,
    expected_bytes: &[u8],
    status: &JobStatus,
    stdout: &File,
    stderr: &File,
    code: &str,
) -> Result<JobStatus, WorkerError> {
    stdout.sync_all()?;
    stderr.sync_all()?;
    let stdout_length = job.validate_private_append_binding("stdout.log", stdout)?;
    let stderr_length = job.validate_private_append_binding("stderr.log", stderr)?;
    let terminal = status.into_infrastructure_terminal(
        JobState::Lost,
        now_millis()?,
        stdout_length,
        stderr_length,
        code.into(),
    )?;
    replace_status(
        store,
        lease,
        status_guard,
        job,
        expected_bytes,
        status,
        &terminal,
    )?;
    Ok(terminal)
}

fn erase_payload_and_record_prelaunch(
    store: &HostStore,
    lease: &LeaseRecord,
    status_guard: &mut SupervisorGuard,
    job: &RootedDir,
    expected_bytes: &[u8],
    status: &JobStatus,
    code: &str,
) -> Result<JobStatus, WorkerError> {
    if let Err(removal_error) = job.remove_owned_regular("execution.json") {
        if let (Ok(stdout), Ok(stderr)) = (
            job.open_private_append("stdout.log"),
            job.open_private_append("stderr.log"),
        ) {
            let _ = record_prelaunch_failure(
                store,
                lease,
                status_guard,
                job,
                expected_bytes,
                status,
                &stdout,
                &stderr,
                "PAYLOAD_ERASURE_FAILED",
            );
        }
        return Err(WorkerError::Io(removal_error));
    }
    let stdout = job.open_private_append("stdout.log")?;
    let stderr = job.open_private_append("stderr.log")?;
    prelaunch_terminal(
        store,
        lease,
        status_guard,
        job,
        expected_bytes,
        status,
        &stdout,
        &stderr,
        code,
    )
}

#[allow(clippy::too_many_arguments)]
fn finish_ambiguous_child(
    store: &HostStore,
    lease: &LeaseRecord,
    status_guard: &mut SupervisorGuard,
    job: &RootedDir,
    expected_bytes: &[u8],
    status: &JobStatus,
    stdout: &File,
    stderr: &File,
    child_identity: ProcessIdentity,
    timeout_millis: u64,
    inspector: &dyn ProcessInspector,
    code: &str,
) -> Result<JobStatus, WorkerError> {
    let _ = wait_for_child(child_identity, timeout_millis, inspector)?;
    stdout.sync_all()?;
    stderr.sync_all()?;
    let stdout_length = job.validate_private_append_binding("stdout.log", stdout)?;
    let stderr_length = job.validate_private_append_binding("stderr.log", stderr)?;
    let terminal = status.into_infrastructure_terminal(
        JobState::Lost,
        now_millis()?,
        stdout_length,
        stderr_length,
        code.into(),
    )?;
    replace_status(
        store,
        lease,
        status_guard,
        job,
        expected_bytes,
        status,
        &terminal,
    )?;
    Ok(terminal)
}

#[allow(clippy::too_many_arguments)]
fn record_prelaunch_failure(
    store: &HostStore,
    lease: &LeaseRecord,
    status_guard: &mut SupervisorGuard,
    job: &RootedDir,
    expected_bytes: &[u8],
    status: &JobStatus,
    stdout: &File,
    stderr: &File,
    code: &str,
) -> Result<(), WorkerError> {
    prelaunch_terminal(
        store,
        lease,
        status_guard,
        job,
        expected_bytes,
        status,
        stdout,
        stderr,
        code,
    )
    .map(|_| ())
}

fn enrich_cleanup_error(
    store: &HostStore,
    lease: &LeaseRecord,
    job: &RootedDir,
    terminal: &JobStatus,
    code: &str,
) -> Result<(), WorkerError> {
    let admission = store.admission_lock(lease.job_id())?;
    let mut status_guard = store
        .supervisor_lock_after(&admission, lease.job_id(), true)?
        .ok_or_else(|| {
            protocol_code(
                "STATUS_CAPABILITY_BUSY",
                "status capability remained busy during terminal enrichment",
            )
        })?;
    drop(admission);
    let (bytes, current): (Vec<u8>, JobStatus) =
        read_canonical_json_with_bytes(job, "status.json")?;
    if current != *terminal {
        return Err(protocol_code(
            "STATUS_CHANGED",
            "terminal status changed before cleanup enrichment",
        ));
    }
    let enriched = current.with_cleanup_error(code.into(), now_millis()?)?;
    replace_status(
        store,
        lease,
        &mut status_guard,
        job,
        &bytes,
        &current,
        &enriched,
    )
}

#[allow(clippy::too_many_arguments)]
fn replace_status(
    store: &HostStore,
    lease: &LeaseRecord,
    status_guard: &mut SupervisorGuard,
    job: &RootedDir,
    expected_bytes: &[u8],
    expected: &JobStatus,
    replacement: &JobStatus,
) -> Result<(), WorkerError> {
    store.replace_job_status_after(
        status_guard,
        lease,
        job,
        expected_bytes,
        expected,
        replacement,
    )
}

fn require_accepted_disposition(
    disposition: Option<JobDisposition>,
    lease: &LeaseRecord,
) -> Result<(), WorkerError> {
    match disposition {
        Some(JobDisposition::Accepted {
            job_id,
            client_id,
            project_id,
            worktree_id,
            request_fingerprint,
            ..
        }) if job_id == lease.job_id()
            && client_id == lease.client_id()
            && project_id == lease.project_id()
            && worktree_id == lease.worktree_id()
            && request_fingerprint == *lease.request_fingerprint() =>
        {
            Ok(())
        }
        _ => Err(protocol_code(
            "JOB_ID_CONFLICT",
            "accepted disposition does not match the live lease",
        )),
    }
}

fn require_meta_matches_lease(meta: &JobMeta, lease: &LeaseRecord) -> Result<(), WorkerError> {
    meta.validate()?;
    lease.validate()?;
    if meta.job_id() != lease.job_id()
        || meta.client_id() != lease.client_id()
        || meta.request_fingerprint() != lease.request_fingerprint()
        || meta.worker_name() != lease.worker_name()
        || meta.project_id() != lease.project_id()
        || meta.worktree_id() != lease.worktree_id()
        || meta.manifest_digest() != lease.manifest_digest()
        || meta.timeout_millis() != lease.timeout_millis()
        || meta.resource_class() != lease.resource_class()
        || meta.command_summary() != lease.command_summary()
    {
        return Err(protocol_code(
            "JOB_ID_CONFLICT",
            "canonical job metadata does not match the live lease",
        ));
    }
    Ok(())
}

fn read_canonical_json<T>(directory: &RootedDir, name: &str) -> Result<T, WorkerError>
where
    T: DeserializeOwned + Serialize,
{
    read_canonical_json_with_bytes(directory, name).map(|(_, value)| value)
}

fn read_canonical_json_with_bytes<T>(
    directory: &RootedDir,
    name: &str,
) -> Result<(Vec<u8>, T), WorkerError>
where
    T: DeserializeOwned + Serialize,
{
    let bytes = directory.read_private_regular(name, MAX_HOST_JSON_BYTES)?;
    let value = parse_canonical_json(&bytes)?;
    Ok((bytes, value))
}

fn parse_canonical_json<T>(bytes: &[u8]) -> Result<T, WorkerError>
where
    T: DeserializeOwned + Serialize,
{
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|_| WorkerError::Protocol("canonical job JSON is invalid".into()))?;
    deserializer
        .end()
        .map_err(|_| WorkerError::Protocol("canonical job JSON has trailing data".into()))?;
    if canonical_json(&value)? != bytes {
        return Err(WorkerError::Protocol(
            "canonical job JSON has a non-canonical encoding".into(),
        ));
    }
    Ok(value)
}

fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, WorkerError> {
    serde_json::to_vec(value)
        .map_err(|_| WorkerError::Protocol("host JSON serialization failed".into()))
}

fn resolve_executable(program: &str) -> Result<PathBuf, WorkerError> {
    let candidate = Path::new(program);
    if candidate.is_absolute() {
        return require_executable(candidate);
    }
    if program.contains('/') {
        return Ok(candidate.to_owned());
    }
    CONTROLLED_PATHS
        .iter()
        .map(|directory| Path::new(directory).join(program))
        .find_map(|path| require_executable(&path).ok())
        .ok_or_else(|| protocol_code("EXECUTABLE_NOT_FOUND", "executable was not found"))
}

fn require_executable(path: &Path) -> Result<PathBuf, WorkerError> {
    let metadata = std::fs::metadata(path)
        .map_err(|_| protocol_code("EXECUTABLE_NOT_FOUND", "executable was not found"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(protocol_code(
            "EXECUTABLE_NOT_FOUND",
            "executable is not a regular executable file",
        ));
    }
    Ok(path.to_owned())
}

fn open_dev_null() -> io::Result<OwnedFd> {
    let descriptor = unsafe {
        libc::open(
            c"/dev/null".as_ptr(),
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

fn set_cloexec(descriptor: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn descriptor_ceiling() -> Result<RawFd, WorkerError> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::zeroed();
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
        return Err(WorkerError::Io(io::Error::last_os_error()));
    }
    let limit = unsafe { limit.assume_init() };
    limit
        .rlim_cur
        .try_into()
        .map_err(|_| protocol_code("FD_LIMIT_INVALID", "descriptor limit is unsupported"))
}

fn descriptor_stat(descriptor: RawFd) -> Result<libc::stat, WorkerError> {
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::zeroed();
    if unsafe { libc::fstat(descriptor, metadata.as_mut_ptr()) } != 0 {
        return Err(WorkerError::Io(io::Error::last_os_error()));
    }
    Ok(unsafe { metadata.assume_init() })
}

fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes())
        .map_err(|_| WorkerError::Protocol("job relative directory is invalid".into()))
}

fn now_millis() -> Result<u64, WorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::Protocol("system clock precedes the Unix epoch".into()))?
        .as_millis()
        .try_into()
        .map_err(|_| WorkerError::Protocol("system clock is outside the supported range".into()))
}

fn protocol_code(code: &'static str, message: &str) -> WorkerError {
    WorkerError::Protocol(format!("{code}: {message}"))
}

fn injected_supervisor_fault(boundary: &str) -> WorkerError {
    WorkerError::Io(io::Error::other(format!(
        "injected supervisor fault after {boundary}"
    )))
}

fn prelaunch_error_code(error: &WorkerError) -> &'static str {
    match error {
        WorkerError::Protocol(message) if message.starts_with("EXECUTABLE_NOT_FOUND:") => {
            "EXECUTABLE_NOT_FOUND"
        }
        WorkerError::Protocol(message) if message.starts_with("INVALID_EXECUTABLE:") => {
            "INVALID_EXECUTABLE"
        }
        WorkerError::Protocol(message) if message.starts_with("INVALID_COMMAND:") => {
            "INVALID_COMMAND"
        }
        WorkerError::Protocol(message) if message.starts_with("CHILD_READY_FAILED:") => {
            "CHILD_READY_FAILED"
        }
        WorkerError::Protocol(message) if message.starts_with("CHILD_IDENTITY_INVALID:") => {
            "CHILD_IDENTITY_INVALID"
        }
        WorkerError::Protocol(message) if message.starts_with("CHILD_IDENTITY_AMBIGUOUS:") => {
            "CHILD_IDENTITY_AMBIGUOUS"
        }
        _ => "COMMAND_PREPARATION_FAILED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, OpenOptions},
        os::unix::{ffi::OsStringExt, process::CommandExt},
        process::Command,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
            mpsc,
        },
    };

    use crate::job::RequestFingerprintMaterial;

    struct RejectingInspector {
        observed_pid: AtomicU32,
    }

    impl RejectingInspector {
        fn new() -> Self {
            Self {
                observed_pid: AtomicU32::new(0),
            }
        }
    }

    impl ProcessInspector for RejectingInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            self.observed_pid.store(pid, Ordering::SeqCst);
            Err(protocol_code(
                "PROCESS_AMBIGUOUS",
                "injected identity rejection",
            ))
        }

        fn observe(&self, _expected: ProcessIdentity) -> ProcessObservation {
            ProcessObservation::Ambiguous
        }

        fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
            ProcessGroupObservation::Ambiguous
        }

        fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
            ProcessGroupMembership::Ambiguous
        }
    }

    struct TransientLauncherInspector {
        ambiguous: AtomicBool,
    }

    impl ProcessInspector for TransientLauncherInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            if self.ambiguous.load(Ordering::SeqCst) {
                return Err(protocol_code(
                    "PROCESS_AMBIGUOUS",
                    "injected transient launcher inspection failure",
                ));
            }
            SystemProcessInspector.identity_for_pid(pid)
        }

        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            SystemProcessInspector.observe(expected)
        }

        fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
            SystemProcessInspector.observe_group(process_group)
        }

        fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
            SystemProcessInspector.observe_group_members(leader)
        }
    }

    struct ExitBeforeTermInspector {
        release: Mutex<Option<OwnedFd>>,
        triggered: AtomicBool,
    }

    impl ProcessInspector for ExitBeforeTermInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            SystemProcessInspector.identity_for_pid(pid)
        }

        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            if !self.triggered.swap(true, Ordering::SeqCst) {
                let release = self.release.lock().unwrap().take().unwrap();
                let byte = 1_u8;
                assert_eq!(
                    unsafe {
                        libc::write(
                            release.as_raw_fd(),
                            (&raw const byte).cast(),
                            std::mem::size_of_val(&byte),
                        )
                    },
                    1
                );
                drop(release);
                let deadline = Instant::now() + Duration::from_secs(1);
                while !child_is_waitable(expected.pid() as libc::pid_t).unwrap() {
                    assert!(
                        Instant::now() < deadline,
                        "test child did not become waitable"
                    );
                    std::thread::yield_now();
                }
            }
            SystemProcessInspector.observe(expected)
        }

        fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
            SystemProcessInspector.observe_group(process_group)
        }

        fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
            SystemProcessInspector.observe_group_members(leader)
        }
    }

    struct VanishingBeforeKillInspector {
        descendant: libc::pid_t,
        triggered: AtomicBool,
    }

    struct PostTermTransitionInspector {
        observations: AtomicUsize,
        release: mpsc::Sender<()>,
    }

    impl ProcessInspector for VanishingBeforeKillInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            SystemProcessInspector.identity_for_pid(pid)
        }

        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            SystemProcessInspector.observe(expected)
        }

        fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
            SystemProcessInspector.observe_group(process_group)
        }

        fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
            let observed = SystemProcessInspector.observe_group_members(leader);
            if !self.triggered.swap(true, Ordering::SeqCst) {
                assert_eq!(observed, ProcessGroupMembership::OtherMembers);
                assert_eq!(unsafe { libc::kill(self.descendant, libc::SIGKILL) }, 0);
                let deadline = Instant::now() + Duration::from_secs(2);
                while SystemProcessInspector.observe_group_members(leader)
                    != ProcessGroupMembership::LeaderOnly
                {
                    assert!(
                        Instant::now() < deadline,
                        "descendant did not leave the process group"
                    );
                    std::thread::yield_now();
                }
                return ProcessGroupMembership::OtherMembers;
            }
            observed
        }
    }

    impl ProcessInspector for PostTermTransitionInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            SystemProcessInspector.identity_for_pid(pid)
        }

        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            if self.observations.fetch_add(1, Ordering::SeqCst) == 1 {
                self.release.send(()).unwrap();
                return ProcessObservation::Absent;
            }
            SystemProcessInspector.observe(expected)
        }

        fn observe_group(&self, process_group: u32) -> ProcessGroupObservation {
            SystemProcessInspector.observe_group(process_group)
        }

        fn observe_group_members(&self, leader: u32) -> ProcessGroupMembership {
            SystemProcessInspector.observe_group_members(leader)
        }
    }

    struct RawProcessGroupGuard {
        leader: libc::pid_t,
        identity: Option<ProcessIdentity>,
        armed: bool,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RawChildAnchor {
        Live,
        Waitable,
        Reaped,
    }

    impl RawProcessGroupGuard {
        fn new(leader: libc::pid_t) -> Self {
            Self {
                leader,
                identity: None,
                armed: true,
            }
        }

        fn leader(&self) -> libc::pid_t {
            self.leader
        }

        fn bind_identity(&mut self, identity: ProcessIdentity) {
            assert_eq!(identity.pid(), self.leader as u32);
            assert!(self.identity.replace(identity).is_none());
        }

        fn finish(&mut self) -> Result<(), WorkerError> {
            self.cleanup_owned()
        }

        fn cleanup_owned(&mut self) -> Result<(), WorkerError> {
            if !self.armed {
                return Ok(());
            }
            let anchor = raw_child_anchor(self.leader)?;
            if anchor == RawChildAnchor::Reaped {
                self.armed = false;
                return match SystemProcessInspector.observe_group(self.leader as u32) {
                    ProcessGroupObservation::Absent => Ok(()),
                    ProcessGroupObservation::Present | ProcessGroupObservation::Ambiguous => {
                        Err(protocol_code(
                            "RAW_GROUP_CHILD_ANCHOR_LOST",
                            "raw process-group cleanup lost its direct-child anchor",
                        ))
                    }
                };
            }
            let Some(identity) = self.identity else {
                return self.cleanup_unbound_child(anchor);
            };
            if anchor == RawChildAnchor::Live
                && !matches!(
                    SystemProcessInspector.observe(identity),
                    ProcessObservation::Matching { process_group }
                        if process_group == identity.pid()
                )
            {
                self.cleanup_unbound_child(anchor)?;
                return Err(protocol_code(
                    "RAW_GROUP_IDENTITY_LOST",
                    "raw process-group cleanup lost its bound live identity",
                ));
            }

            match SystemProcessInspector.observe_group_members(identity.pid()) {
                ProcessGroupMembership::LeaderOnly if anchor == RawChildAnchor::Live => {
                    if unsafe { libc::kill(self.leader, libc::SIGKILL) } != 0 {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::ESRCH) {
                            return Err(WorkerError::Io(error));
                        }
                    }
                }
                ProcessGroupMembership::LeaderOnly => {}
                ProcessGroupMembership::OtherMembers | ProcessGroupMembership::Ambiguous => {
                    if unsafe { libc::kill(-self.leader, libc::SIGKILL) } != 0 {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::ESRCH) {
                            return Err(WorkerError::Io(error));
                        }
                    }
                }
            }

            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match raw_child_anchor(self.leader)? {
                    RawChildAnchor::Waitable => break,
                    RawChildAnchor::Live if Instant::now() < deadline => {
                        std::thread::yield_now();
                    }
                    RawChildAnchor::Live => {
                        return Err(protocol_code(
                            "RAW_GROUP_CLEANUP_TIMEOUT",
                            "raw group leader did not become waitable after exact cleanup",
                        ));
                    }
                    RawChildAnchor::Reaped => {
                        self.armed = false;
                        return Err(protocol_code(
                            "RAW_GROUP_CHILD_ANCHOR_LOST",
                            "raw group leader was reaped outside exact cleanup",
                        ));
                    }
                }
            }
            loop {
                match SystemProcessInspector.observe_group_members(identity.pid()) {
                    ProcessGroupMembership::LeaderOnly => break,
                    ProcessGroupMembership::OtherMembers | ProcessGroupMembership::Ambiguous
                        if Instant::now() < deadline =>
                    {
                        std::thread::yield_now();
                    }
                    ProcessGroupMembership::OtherMembers | ProcessGroupMembership::Ambiguous => {
                        return Err(protocol_code(
                            "RAW_GROUP_CLEANUP_TIMEOUT",
                            "raw group descendants did not terminate while the leader was anchored",
                        ));
                    }
                }
            }
            wait_blocking(self.leader)?;
            self.armed = false;
            wait_for_terminated_group_absence(identity, &SystemProcessInspector)
        }

        fn cleanup_unbound_child(&mut self, anchor: RawChildAnchor) -> Result<(), WorkerError> {
            // A successful WNOWAIT probe with no event still proves this PID is
            // our live direct child. It authorizes the positive-PID signal,
            // but never a process-group signal without the bound start/PGID.
            if anchor == RawChildAnchor::Live
                && unsafe { libc::kill(self.leader, libc::SIGKILL) } != 0
            {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(WorkerError::Io(error));
                }
            }
            if anchor == RawChildAnchor::Live {
                let deadline = Instant::now() + Duration::from_secs(2);
                while raw_child_anchor(self.leader)? == RawChildAnchor::Live {
                    if Instant::now() >= deadline {
                        return Err(protocol_code(
                            "RAW_CHILD_CLEANUP_TIMEOUT",
                            "unbound raw child did not become waitable after exact PID cleanup",
                        ));
                    }
                    std::thread::yield_now();
                }
            }
            if raw_child_anchor(self.leader)? == RawChildAnchor::Waitable {
                wait_blocking(self.leader)?;
            }
            self.armed = false;
            match SystemProcessInspector.observe_group(self.leader as u32) {
                ProcessGroupObservation::Absent => Ok(()),
                ProcessGroupObservation::Present | ProcessGroupObservation::Ambiguous => {
                    Err(protocol_code(
                        "RAW_GROUP_IDENTITY_MISSING",
                        "raw child was reaped without authority to signal its process group",
                    ))
                }
            }
        }
    }

    fn raw_child_anchor(pid: libc::pid_t) -> Result<RawChildAnchor, WorkerError> {
        match child_is_waitable(pid) {
            Ok(true) => Ok(RawChildAnchor::Waitable),
            Ok(false) => Ok(RawChildAnchor::Live),
            Err(WorkerError::Io(error)) if error.raw_os_error() == Some(libc::ECHILD) => {
                Ok(RawChildAnchor::Reaped)
            }
            Err(error) => Err(error),
        }
    }

    impl Drop for RawProcessGroupGuard {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            assert!(
                std::thread::panicking(),
                "armed raw process-group guard requires explicit cleanup"
            );
            if self.cleanup_owned().is_err() && self.armed {
                std::process::abort();
            }
        }
    }

    struct RawChildGuard {
        pid: libc::pid_t,
        armed: bool,
    }

    impl RawChildGuard {
        fn new(pid: libc::pid_t) -> Self {
            Self { pid, armed: true }
        }

        fn disarm(&mut self) {
            self.armed = false;
        }
    }

    impl Drop for RawChildGuard {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            let mut status = 0;
            let observed = unsafe { libc::waitpid(self.pid, &raw mut status, libc::WNOHANG) };
            if observed == 0 {
                unsafe { libc::kill(self.pid, libc::SIGKILL) };
                loop {
                    let result = unsafe { libc::waitpid(self.pid, &raw mut status, 0) };
                    if result == self.pid {
                        break;
                    }
                    if result < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted
                    {
                        continue;
                    }
                    break;
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    struct RawDescendantGate {
        ready: Pipe,
        go: Pipe,
    }

    #[cfg(target_os = "macos")]
    impl RawDescendantGate {
        const READY: u8 = 0x52;
        const GO: u8 = 0x47;

        fn new() -> io::Result<Self> {
            Ok(Self {
                ready: Pipe::cloexec()?,
                go: Pipe::cloexec()?,
            })
        }

        /// Called only in the raw-fork child. Every operation is
        /// async-signal-safe; returning false requires the caller to `_exit`
        /// without creating descendants.
        unsafe fn child_ready_then_wait_for_go(&self) -> bool {
            unsafe {
                libc::close(self.ready.read.as_raw_fd());
                libc::close(self.go.write.as_raw_fd());
                if !raw_write_byte(self.ready.write.as_raw_fd(), Self::READY) {
                    return false;
                }
                libc::close(self.ready.write.as_raw_fd());
                let mut byte = 0_u8;
                let read = loop {
                    let read = libc::read(self.go.read.as_raw_fd(), (&raw mut byte).cast(), 1);
                    if read >= 0 || *libc::__error() != libc::EINTR {
                        break read;
                    }
                };
                libc::close(self.go.read.as_raw_fd());
                read == 1 && byte == Self::GO
            }
        }

        fn parent_bind_and_go<F>(
            self,
            cleanup: &mut RawProcessGroupGuard,
            bind: F,
        ) -> Result<(), WorkerError>
        where
            F: FnOnce(libc::pid_t) -> Result<ProcessIdentity, WorkerError>,
        {
            let Self { ready, go } = self;
            let Pipe {
                read: ready_read,
                write: ready_write,
            } = ready;
            let Pipe {
                read: go_read,
                write: go_write,
            } = go;
            drop(ready_write);
            drop(go_read);

            let operation = (|| {
                let ready = read_parent_gate_byte(ready_read.as_raw_fd())?;
                if ready != Self::READY {
                    return Err(protocol_code(
                        "RAW_GATE_READY_INVALID",
                        "raw group leader did not publish the READY byte",
                    ));
                }
                let identity = bind(cleanup.leader())?;
                cleanup.bind_identity(identity);
                write_parent_gate_byte(go_write.as_raw_fd(), Self::GO)
            })();

            // Closing GO before cleanup makes every ordinary pre-GO failure a
            // zero-descendant path. During unwinding these owned descriptors
            // are likewise dropped before the armed guard in the caller.
            drop(go_write);
            drop(ready_read);
            if let Err(error) = operation {
                if let Err(cleanup_error) = cleanup.finish() {
                    return Err(protocol_code(
                        "RAW_GATE_CLEANUP_FAILED",
                        &format!("{error}; cleanup: {cleanup_error}"),
                    ));
                }
                return Err(error);
            }
            Ok(())
        }
    }

    #[cfg(target_os = "macos")]
    unsafe fn raw_write_byte(fd: RawFd, byte: u8) -> bool {
        unsafe {
            loop {
                let written = libc::write(fd, (&raw const byte).cast(), 1);
                if written == 1 {
                    return true;
                }
                if written >= 0 || *libc::__error() != libc::EINTR {
                    return false;
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn read_parent_gate_byte(fd: RawFd) -> Result<u8, WorkerError> {
        let mut byte = 0_u8;
        loop {
            let read = unsafe { libc::read(fd, (&raw mut byte).cast(), 1) };
            if read == 1 {
                return Ok(byte);
            }
            if read == 0 {
                return Err(protocol_code(
                    "RAW_GATE_READY_CLOSED",
                    "raw group leader closed READY before the handshake",
                ));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(WorkerError::Io(error));
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn write_parent_gate_byte(fd: RawFd, byte: u8) -> Result<(), WorkerError> {
        loop {
            let written = unsafe { libc::write(fd, (&raw const byte).cast(), 1) };
            if written == 1 {
                return Ok(());
            }
            if written == 0 {
                return Err(protocol_code(
                    "RAW_GATE_GO_CLOSED",
                    "raw group leader closed GO before release",
                ));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(WorkerError::Io(error));
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn wait_for_group_identity(pid: libc::pid_t) -> ProcessIdentity {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if let Ok(identity) = SystemProcessInspector.identity_for_pid(pid as u32)
                && matches!(
                    SystemProcessInspector.observe(identity),
                    ProcessObservation::Matching { process_group } if process_group == pid as u32
                )
            {
                return identity;
            }
            assert!(
                Instant::now() < deadline,
                "group leader identity was not ready"
            );
            std::thread::yield_now();
        }
    }

    #[cfg(target_os = "macos")]
    fn spawn_blocked_group_leader(exit_code: u8) -> (RawProcessGroupGuard, OwnedFd) {
        let mut descriptors = [-1; 2];
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed: {}", io::Error::last_os_error());
        if pid == 0 {
            unsafe {
                libc::close(descriptors[1]);
                if libc::setpgid(0, 0) != 0 {
                    libc::_exit(70);
                }
                let mut byte = 0_u8;
                libc::read(descriptors[0], (&raw mut byte).cast(), 1);
                libc::_exit(exit_code as i32);
            }
        }
        let mut cleanup = RawProcessGroupGuard::new(pid);
        cleanup.bind_identity(wait_for_group_identity(pid));
        assert_eq!(unsafe { libc::close(descriptors[0]) }, 0);
        (cleanup, unsafe { OwnedFd::from_raw_fd(descriptors[1]) })
    }

    #[cfg(target_os = "macos")]
    fn spawn_group_with_ignoring_descendant() -> (RawProcessGroupGuard, libc::pid_t) {
        let mut descriptors = [-1; 2];
        assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
        let gate = RawDescendantGate::new().unwrap();
        let leader = unsafe { libc::fork() };
        assert!(leader >= 0, "fork failed: {}", io::Error::last_os_error());
        if leader == 0 {
            unsafe {
                libc::close(descriptors[0]);
                if libc::setpgid(0, 0) != 0 {
                    libc::_exit(70);
                }
                if !gate.child_ready_then_wait_for_go() {
                    libc::_exit(70);
                }
                let descendant = libc::fork();
                if descendant < 0 {
                    libc::_exit(70);
                }
                if descendant == 0 {
                    libc::signal(libc::SIGHUP, libc::SIG_IGN);
                    libc::signal(libc::SIGTERM, libc::SIG_IGN);
                    let own_pid = libc::getpid();
                    libc::write(
                        descriptors[1],
                        (&raw const own_pid).cast(),
                        std::mem::size_of::<libc::pid_t>(),
                    );
                    loop {
                        libc::pause();
                    }
                }
                loop {
                    libc::pause();
                }
            }
        }
        let mut cleanup = RawProcessGroupGuard::new(leader);
        gate.parent_bind_and_go(&mut cleanup, |pid| Ok(wait_for_group_identity(pid)))
            .unwrap();
        assert_eq!(unsafe { libc::close(descriptors[1]) }, 0);
        let mut descendant = 0;
        let read = unsafe {
            libc::read(
                descriptors[0],
                (&raw mut descendant).cast(),
                std::mem::size_of::<libc::pid_t>(),
            )
        };
        assert_eq!(read as usize, std::mem::size_of::<libc::pid_t>());
        assert_eq!(unsafe { libc::close(descriptors[0]) }, 0);
        (cleanup, descendant)
    }

    #[cfg(target_os = "macos")]
    fn spawn_group_with_releasable_descendant() -> (RawProcessGroupGuard, libc::pid_t, OwnedFd) {
        let mut report = [-1; 2];
        let mut release = [-1; 2];
        assert_eq!(unsafe { libc::pipe(report.as_mut_ptr()) }, 0);
        assert_eq!(unsafe { libc::pipe(release.as_mut_ptr()) }, 0);
        let gate = RawDescendantGate::new().unwrap();
        let leader = unsafe { libc::fork() };
        assert!(leader >= 0, "fork failed: {}", io::Error::last_os_error());
        if leader == 0 {
            unsafe {
                libc::close(report[0]);
                libc::close(release[1]);
                if libc::setpgid(0, 0) != 0 {
                    libc::_exit(70);
                }
                if !gate.child_ready_then_wait_for_go() {
                    libc::_exit(70);
                }
                let descendant = libc::fork();
                if descendant < 0 {
                    libc::_exit(70);
                }
                if descendant == 0 {
                    let own_pid = libc::getpid();
                    libc::write(
                        report[1],
                        (&raw const own_pid).cast(),
                        std::mem::size_of::<libc::pid_t>(),
                    );
                    libc::close(report[1]);
                    let mut byte = 0_u8;
                    libc::read(release[0], (&raw mut byte).cast(), 1);
                    libc::_exit(0);
                }
                libc::close(report[1]);
                libc::close(release[0]);
                loop {
                    libc::pause();
                }
            }
        }
        let mut cleanup = RawProcessGroupGuard::new(leader);
        gate.parent_bind_and_go(&mut cleanup, |pid| Ok(wait_for_group_identity(pid)))
            .unwrap();
        assert_eq!(unsafe { libc::close(report[1]) }, 0);
        assert_eq!(unsafe { libc::close(release[0]) }, 0);
        let mut descendant = 0;
        let read = unsafe {
            libc::read(
                report[0],
                (&raw mut descendant).cast(),
                std::mem::size_of::<libc::pid_t>(),
            )
        };
        assert_eq!(read as usize, std::mem::size_of::<libc::pid_t>());
        assert_eq!(unsafe { libc::close(report[0]) }, 0);
        (cleanup, descendant, unsafe {
            OwnedFd::from_raw_fd(release[1])
        })
    }

    #[cfg(target_os = "macos")]
    fn spawn_term_ignoring_group_leader(exit_code: u8) -> (RawProcessGroupGuard, OwnedFd) {
        let mut release = [-1; 2];
        let mut ready = [-1; 2];
        assert_eq!(unsafe { libc::pipe(release.as_mut_ptr()) }, 0);
        assert_eq!(unsafe { libc::pipe(ready.as_mut_ptr()) }, 0);
        let leader = unsafe { libc::fork() };
        assert!(leader >= 0, "fork failed: {}", io::Error::last_os_error());
        if leader == 0 {
            unsafe {
                libc::close(release[1]);
                libc::close(ready[0]);
                if libc::setpgid(0, 0) != 0 {
                    libc::_exit(70);
                }
                libc::signal(libc::SIGTERM, libc::SIG_IGN);
                let ready_byte = 1_u8;
                libc::write(
                    ready[1],
                    (&raw const ready_byte).cast(),
                    std::mem::size_of_val(&ready_byte),
                );
                let mut release_byte = 0_u8;
                libc::read(
                    release[0],
                    (&raw mut release_byte).cast(),
                    std::mem::size_of_val(&release_byte),
                );
                libc::_exit(exit_code as i32);
            }
        }
        let mut cleanup = RawProcessGroupGuard::new(leader);
        cleanup.bind_identity(wait_for_group_identity(leader));
        assert_eq!(unsafe { libc::close(release[0]) }, 0);
        assert_eq!(unsafe { libc::close(ready[1]) }, 0);
        let mut ready_byte = 0_u8;
        assert_eq!(
            unsafe {
                libc::read(
                    ready[0],
                    (&raw mut ready_byte).cast(),
                    std::mem::size_of_val(&ready_byte),
                )
            },
            1
        );
        assert_eq!(ready_byte, 1);
        assert_eq!(unsafe { libc::close(ready[0]) }, 0);
        (cleanup, unsafe { OwnedFd::from_raw_fd(release[1]) })
    }

    #[cfg(target_os = "macos")]
    fn fence_test_subprocess_descriptors(command: &mut Command) -> Result<(), WorkerError> {
        let descriptor_ceiling = descriptor_ceiling()?;
        unsafe {
            command.pre_exec(move || {
                let mut descriptor = 3;
                while descriptor < descriptor_ceiling {
                    let flags = libc::fcntl(descriptor, libc::F_GETFD);
                    if flags < 0 {
                        let error = io::Error::last_os_error();
                        if error.raw_os_error() != Some(libc::EBADF) {
                            return Err(error);
                        }
                    } else if flags & libc::FD_CLOEXEC == 0
                        && libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) < 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    descriptor += 1;
                }
                Ok(())
            });
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn run_isolated_raw_fork_scenario<F>(test_name: &'static str, scenario: F)
    where
        F: FnOnce(),
    {
        const SCENARIO_ENV: &str = "MAC_WORKER_TEST_RAW_FORK_SCENARIO";
        const SENTINEL_FD_ENV: &str = "MAC_WORKER_TEST_RAW_FORK_SENTINEL_FD";
        const SENTINEL_DEVICE_ENV: &str = "MAC_WORKER_TEST_RAW_FORK_SENTINEL_DEVICE";
        const SENTINEL_INODE_ENV: &str = "MAC_WORKER_TEST_RAW_FORK_SENTINEL_INODE";
        const STDOUT_MARKER: &str = "raw-fork-isolation-stdout-ready";
        const STDERR_MARKER: &str = "raw-fork-isolation-stderr-ready";

        if let Some(actual) = std::env::var_os(SCENARIO_ENV) {
            assert_eq!(actual, test_name);
            let sentinel_fd: RawFd = std::env::var(SENTINEL_FD_ENV).unwrap().parse().unwrap();
            let expected_device: u64 = std::env::var(SENTINEL_DEVICE_ENV).unwrap().parse().unwrap();
            let expected_inode: u64 = std::env::var(SENTINEL_INODE_ENV).unwrap().parse().unwrap();
            let mut actual = std::mem::MaybeUninit::<libc::stat>::zeroed();
            if unsafe { libc::fstat(sentinel_fd, actual.as_mut_ptr()) } == 0 {
                let actual = unsafe { actual.assume_init() };
                assert_ne!(
                    (actual.st_dev as u64, actual.st_ino),
                    (expected_device, expected_inode),
                    "raw-fork subprocess inherited the parent test descriptor"
                );
            } else {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::EBADF),
                    "raw-fork sentinel inspection was ambiguous"
                );
            }
            println!("{STDOUT_MARKER}");
            eprintln!("{STDERR_MARKER}");
            scenario();
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let sentinel = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(temp.path().join("parent-test-sentinel"))
            .unwrap();
        let sentinel_flags = unsafe { libc::fcntl(sentinel.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(sentinel_flags, -1);
        assert_eq!(
            unsafe {
                libc::fcntl(
                    sentinel.as_raw_fd(),
                    libc::F_SETFD,
                    sentinel_flags & !libc::FD_CLOEXEC,
                )
            },
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(sentinel.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0,
            "sentinel must model a foreign inheritable test descriptor"
        );
        let sentinel_stat = descriptor_stat(sentinel.as_raw_fd()).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .env(SCENARIO_ENV, test_name)
            .env(SENTINEL_FD_ENV, sentinel.as_raw_fd().to_string())
            .env(SENTINEL_DEVICE_ENV, sentinel_stat.st_dev.to_string())
            .env(SENTINEL_INODE_ENV, sentinel_stat.st_ino.to_string())
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"]);
        fence_test_subprocess_descriptors(&mut command).unwrap();
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(STDOUT_MARKER),
            "isolated child stdout capture was fenced"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(STDERR_MARKER),
            "isolated child stderr capture was fenced"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_fork_scenarios_do_not_inherit_parent_test_descriptors() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_fork_scenarios_do_not_inherit_parent_test_descriptors",
            || {},
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_fork_descriptor_fence_preserves_spawn_exec_errors() {
        let mut command = Command::new("/definitely/missing/mac-worker-test-subprocess");
        fence_test_subprocess_descriptors(&mut command).unwrap();

        let error = command
            .output()
            .expect_err("missing executable must remain an observable spawn error");

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[cfg(target_os = "macos")]
    #[derive(Clone, Copy)]
    enum InjectedBindMode {
        Error,
        Panic,
    }

    #[cfg(target_os = "macos")]
    struct BindGateEvidence {
        outcome: String,
        marker_read: isize,
        wait_result: libc::pid_t,
        wait_errno: Option<i32>,
        group: ProcessGroupObservation,
        elapsed: Duration,
    }

    #[cfg(target_os = "macos")]
    fn exercise_descendant_gate_bind_failure(mode: InjectedBindMode) -> BindGateEvidence {
        let marker = Pipe::cloexec().unwrap();
        let gate = RawDescendantGate::new().unwrap();
        let started = Instant::now();
        let leader = unsafe { libc::fork() };
        assert!(leader >= 0, "fork failed: {}", io::Error::last_os_error());
        if leader == 0 {
            unsafe {
                libc::close(marker.read.as_raw_fd());
                if libc::setpgid(0, 0) != 0 || !gate.child_ready_then_wait_for_go() {
                    libc::_exit(0);
                }
                let descendant = libc::fork();
                if descendant < 0 {
                    libc::_exit(70);
                }
                if descendant == 0 {
                    let created = 1_u8;
                    libc::write(
                        marker.write.as_raw_fd(),
                        (&raw const created).cast(),
                        std::mem::size_of_val(&created),
                    );
                    libc::_exit(0);
                }
                libc::close(marker.write.as_raw_fd());
                loop {
                    libc::pause();
                }
            }
        }

        // Ownership is armed before any parent-side handshake or identity step.
        let cleanup = RawProcessGroupGuard::new(leader);
        drop(marker.write);
        let outcome = match mode {
            InjectedBindMode::Panic => {
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let mut cleanup = cleanup;
                    gate.parent_bind_and_go(
                        &mut cleanup,
                        |_| -> Result<ProcessIdentity, WorkerError> {
                            panic!("injected group-leader bind panic")
                        },
                    )
                }));
                assert!(panic.is_err(), "injected bind panic was not propagated");
                "panic".to_owned()
            }
            InjectedBindMode::Error => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let mut cleanup = cleanup;
                    gate.parent_bind_and_go(&mut cleanup, |_| {
                        Err(protocol_code(
                            "INJECTED_BIND_FAILURE",
                            "group-leader identity bind failed",
                        ))
                    })
                }));
                let error = result
                    .expect("ordinary bind failure unexpectedly panicked")
                    .expect_err("injected bind failure unexpectedly sent GO");
                error.to_string()
            }
        };

        let mut marker_byte = 0_u8;
        let marker_read = unsafe {
            libc::read(
                marker.read.as_raw_fd(),
                (&raw mut marker_byte).cast(),
                std::mem::size_of_val(&marker_byte),
            )
        };
        let mut status = 0;
        let wait_result = unsafe { libc::waitpid(leader, &raw mut status, libc::WNOHANG) };
        let wait_errno = (wait_result == -1).then(|| {
            io::Error::last_os_error()
                .raw_os_error()
                .expect("waitpid failure omitted errno")
        });
        let group = SystemProcessInspector.observe_group(leader as u32);
        BindGateEvidence {
            outcome,
            marker_read,
            wait_result,
            wait_errno,
            group,
            elapsed: started.elapsed(),
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_descendant_gate_bind_failure_forks_nothing_and_reaps_leader() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_descendant_gate_bind_failure_forks_nothing_and_reaps_leader",
            || {
                let evidence = exercise_descendant_gate_bind_failure(InjectedBindMode::Error);

                assert!(
                    evidence.outcome.contains("INJECTED_BIND_FAILURE"),
                    "{}",
                    evidence.outcome
                );
                assert_eq!(
                    evidence.marker_read, 0,
                    "a descendant crossed the closed GO gate"
                );
                assert_eq!(
                    evidence.wait_result, -1,
                    "failed-bind leader was not reaped"
                );
                assert_eq!(evidence.wait_errno, Some(libc::ECHILD));
                assert_eq!(evidence.group, ProcessGroupObservation::Absent);
                assert!(
                    evidence.elapsed < Duration::from_secs(1),
                    "failed bind cleanup was unbounded: {:?}",
                    evidence.elapsed
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_descendant_gate_bind_panic_forks_nothing_and_reaps_leader() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_descendant_gate_bind_panic_forks_nothing_and_reaps_leader",
            || {
                let evidence = exercise_descendant_gate_bind_failure(InjectedBindMode::Panic);

                assert_eq!(evidence.outcome, "panic");
                assert_eq!(
                    evidence.marker_read, 0,
                    "a descendant crossed the closed GO gate"
                );
                assert_eq!(
                    evidence.wait_result, -1,
                    "panicking-bind leader was not reaped"
                );
                assert_eq!(evidence.wait_errno, Some(libc::ECHILD));
                assert_eq!(evidence.group, ProcessGroupObservation::Absent);
                assert!(
                    evidence.elapsed < Duration::from_secs(1),
                    "panicking bind cleanup was unbounded: {:?}",
                    evidence.elapsed
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_process_group_guard_reaps_during_a_panicking_scenario() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_process_group_guard_reaps_during_a_panicking_scenario",
            || {
                let (cleanup, _descendant) = spawn_group_with_ignoring_descendant();
                let leader = cleanup.leader();
                let panic = std::panic::catch_unwind(move || {
                    let _cleanup = cleanup;
                    panic!("injected raw-fork scenario panic");
                });
                assert!(panic.is_err());
                let deadline = Instant::now() + Duration::from_secs(1);
                loop {
                    let mut status = 0;
                    let result = unsafe { libc::waitpid(leader, &raw mut status, libc::WNOHANG) };
                    if result == -1
                        && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD)
                    {
                        break;
                    }
                    assert_ne!(
                        result, leader,
                        "panic cleanup did not reap the group leader"
                    );
                    assert_eq!(result, 0, "unexpected panic-cleanup wait result");
                    assert!(
                        Instant::now() < deadline,
                        "panic cleanup did not terminate the group leader"
                    );
                    std::thread::yield_now();
                }
                assert_eq!(
                    SystemProcessInspector.observe_group(leader as u32),
                    ProcessGroupObservation::Absent
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_process_group_finish_reaps_a_live_anchor_after_descendant_cleanup() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_process_group_finish_reaps_a_live_anchor_after_descendant_cleanup",
            || {
                let (mut cleanup, _descendant) = spawn_group_with_ignoring_descendant();
                let leader = cleanup.leader();

                cleanup.finish().unwrap();

                let mut status = 0;
                assert_eq!(
                    unsafe { libc::waitpid(leader, &raw mut status, libc::WNOHANG) },
                    -1,
                    "explicit cleanup did not reap its direct child"
                );
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::ECHILD)
                );
                assert_eq!(
                    SystemProcessInspector.observe_group(leader as u32),
                    ProcessGroupObservation::Absent
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_process_group_guard_never_signals_after_its_child_anchor_was_reaped() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_process_group_guard_never_signals_after_its_child_anchor_was_reaped",
            || {
                let (cleanup, descendant, release) = spawn_group_with_releasable_descendant();
                let leader = cleanup.leader();
                let descendant_identity = SystemProcessInspector
                    .identity_for_pid(descendant as u32)
                    .unwrap();
                assert_eq!(unsafe { libc::kill(leader, libc::SIGKILL) }, 0);
                assert_eq!(
                    wait_blocking(leader).unwrap(),
                    ChildOutcome::Signalled(libc::SIGKILL as u32)
                );
                assert_eq!(
                    SystemProcessInspector.observe_group(leader as u32),
                    ProcessGroupObservation::Present,
                    "descendant did not retain the original process group"
                );

                let drop_result = std::panic::catch_unwind(move || drop(cleanup));
                let descendant_after_drop = SystemProcessInspector.observe(descendant_identity);
                drop(release);
                let deadline = Instant::now() + Duration::from_secs(2);
                while SystemProcessInspector.observe(descendant_identity)
                    != ProcessObservation::Absent
                {
                    assert!(
                        Instant::now() < deadline,
                        "releasable descendant did not exit"
                    );
                    std::thread::yield_now();
                }

                assert!(
                    matches!(
                        descendant_after_drop,
                        ProcessObservation::Matching { process_group }
                            if process_group == leader as u32
                    ),
                    "guard signalled a numeric process group after losing its child anchor"
                );
                assert!(
                    drop_result.is_err(),
                    "ordinary armed-guard disposal hid the lost ownership proof"
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_process_group_cleanup_reports_a_reaped_anchor_without_signalling() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_process_group_cleanup_reports_a_reaped_anchor_without_signalling",
            || {
                let (mut cleanup, descendant, release) = spawn_group_with_releasable_descendant();
                let leader = cleanup.leader();
                let descendant_identity = SystemProcessInspector
                    .identity_for_pid(descendant as u32)
                    .unwrap();
                assert_eq!(unsafe { libc::kill(leader, libc::SIGKILL) }, 0);
                assert_eq!(
                    wait_blocking(leader).unwrap(),
                    ChildOutcome::Signalled(libc::SIGKILL as u32)
                );

                let error = cleanup
                    .finish()
                    .expect_err("a reaped anchor with a live group must fail closed");
                let descendant_after_cleanup = SystemProcessInspector.observe(descendant_identity);
                drop(release);
                let deadline = Instant::now() + Duration::from_secs(2);
                while SystemProcessInspector.observe(descendant_identity)
                    != ProcessObservation::Absent
                {
                    assert!(
                        Instant::now() < deadline,
                        "releasable descendant did not exit"
                    );
                    std::thread::yield_now();
                }

                assert!(
                    error.to_string().contains("RAW_GROUP_CHILD_ANCHOR_LOST"),
                    "{error}"
                );
                assert!(
                    matches!(
                        descendant_after_cleanup,
                        ProcessObservation::Matching { process_group }
                            if process_group == leader as u32
                    ),
                    "explicit cleanup signalled after losing the direct-child anchor"
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn raw_process_group_panicking_guard_does_not_signal_after_its_child_anchor_was_reaped() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::raw_process_group_panicking_guard_does_not_signal_after_its_child_anchor_was_reaped",
            || {
                let (cleanup, descendant, release) = spawn_group_with_releasable_descendant();
                let leader = cleanup.leader();
                let descendant_identity = SystemProcessInspector
                    .identity_for_pid(descendant as u32)
                    .unwrap();
                assert_eq!(unsafe { libc::kill(leader, libc::SIGKILL) }, 0);
                assert_eq!(
                    wait_blocking(leader).unwrap(),
                    ChildOutcome::Signalled(libc::SIGKILL as u32)
                );

                let panic = std::panic::catch_unwind(move || {
                    let _cleanup = cleanup;
                    panic!("injected panic after direct-child reap");
                });
                let descendant_after_panic = SystemProcessInspector.observe(descendant_identity);
                drop(release);
                let deadline = Instant::now() + Duration::from_secs(2);
                while SystemProcessInspector.observe(descendant_identity)
                    != ProcessObservation::Absent
                {
                    assert!(
                        Instant::now() < deadline,
                        "releasable descendant did not exit"
                    );
                    std::thread::yield_now();
                }

                assert!(panic.is_err());
                assert!(
                    matches!(
                        descendant_after_panic,
                        ProcessObservation::Matching { process_group }
                            if process_group == leader as u32
                    ),
                    "panic cleanup signalled a numeric group without a child anchor"
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn timeout_rechecks_waitable_anchor_when_leader_exits_before_term_identity_check() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::timeout_rechecks_waitable_anchor_when_leader_exits_before_term_identity_check",
            || {
                let (mut cleanup, release) = spawn_blocked_group_leader(7);
                let pid = cleanup.leader();
                let identity = wait_for_group_identity(pid);
                let inspector = ExitBeforeTermInspector {
                    release: Mutex::new(Some(release)),
                    triggered: AtomicBool::new(false),
                };

                let result = wait_for_child(identity, 0, &inspector);
                cleanup.finish().unwrap();

                assert_eq!(result.unwrap(), ChildOutcome::Exited(7));
                assert_eq!(
                    SystemProcessInspector.observe_group(identity.pid()),
                    ProcessGroupObservation::Absent
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn timeout_rechecks_waitable_anchor_when_descendants_vanish_before_kill() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::timeout_rechecks_waitable_anchor_when_descendants_vanish_before_kill",
            || {
                let (mut cleanup, descendant) = spawn_group_with_ignoring_descendant();
                let leader = cleanup.leader();
                let identity = wait_for_group_identity(leader);
                assert_eq!(
                    SystemProcessInspector.observe_group_members(identity.pid()),
                    ProcessGroupMembership::OtherMembers,
                    "descendant was not ready in the leader's process group"
                );
                let inspector = VanishingBeforeKillInspector {
                    descendant,
                    triggered: AtomicBool::new(false),
                };
                let started = Instant::now();

                let result = wait_for_child(identity, 0, &inspector);
                cleanup.finish().unwrap();

                assert_eq!(result.unwrap(), ChildOutcome::TimedOut);
                assert!(
                    started.elapsed() >= TERM_GRACE,
                    "full TERM grace was skipped"
                );
                assert_eq!(
                    SystemProcessInspector.observe_group(identity.pid()),
                    ProcessGroupObservation::Absent
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn timeout_owns_the_real_exit_to_waitable_transition_after_successful_term() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::timeout_owns_the_real_exit_to_waitable_transition_after_successful_term",
            || {
                let (mut cleanup, release) = spawn_term_ignoring_group_leader(9);
                let leader = cleanup.leader();
                let identity = wait_for_group_identity(leader);
                let (trigger, triggered) = mpsc::channel();
                let releaser = std::thread::spawn(move || {
                    triggered.recv_timeout(Duration::from_secs(2)).unwrap();
                    std::thread::sleep(Duration::from_millis(50));
                    let byte = 1_u8;
                    assert_eq!(
                        unsafe {
                            libc::write(
                                release.as_raw_fd(),
                                (&raw const byte).cast(),
                                std::mem::size_of_val(&byte),
                            )
                        },
                        1
                    );
                });
                let inspector = PostTermTransitionInspector {
                    observations: AtomicUsize::new(0),
                    release: trigger,
                };
                let started = Instant::now();

                let result = wait_for_child(identity, 0, &inspector);
                releaser.join().unwrap();
                cleanup.finish().unwrap();

                assert_eq!(result.unwrap(), ChildOutcome::TimedOut);
                assert!(
                    started.elapsed() >= TERM_GRACE,
                    "successful TERM did not retain ownership through the full grace"
                );
                assert_eq!(
                    SystemProcessInspector.observe_group(identity.pid()),
                    ProcessGroupObservation::Absent
                );
                let mut status = 0;
                assert_eq!(
                    unsafe { libc::waitpid(leader, &raw mut status, libc::WNOHANG) },
                    -1,
                    "timeout leader was not reaped"
                );
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::ECHILD)
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn failed_group_signal_waits_for_the_owned_child_to_become_waitable() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::failed_group_signal_waits_for_the_owned_child_to_become_waitable",
            || {
                let (mut cleanup, release) = spawn_term_ignoring_group_leader(9);
                let leader = cleanup.leader();
                let identity = wait_for_group_identity(leader);
                let releaser = std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(50));
                    let byte = 1_u8;
                    assert_eq!(
                        unsafe {
                            libc::write(
                                release.as_raw_fd(),
                                (&raw const byte).cast(),
                                std::mem::size_of_val(&byte),
                            )
                        },
                        1
                    );
                });
                let started = Instant::now();

                let result = reconcile_failed_group_signal(
                    identity,
                    &SystemProcessInspector,
                    WorkerError::Io(io::Error::from_raw_os_error(libc::ESRCH)),
                );
                releaser.join().unwrap();
                cleanup.finish().unwrap();

                assert_eq!(result.unwrap(), ChildOutcome::Exited(9));
                assert!(
                    started.elapsed() >= Duration::from_millis(50),
                    "failed-signal reconciliation used only an adjacent poll"
                );
                assert_eq!(
                    SystemProcessInspector.observe_group(identity.pid()),
                    ProcessGroupObservation::Absent
                );
            },
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn transient_launcher_inspection_never_waits_for_a_live_detached_candidate() {
        run_isolated_raw_fork_scenario(
            "supervisor::tests::transient_launcher_inspection_never_waits_for_a_live_detached_candidate",
            || {
                let inspector = TransientLauncherInspector {
                    ambiguous: AtomicBool::new(true),
                };
                let pid = unsafe { libc::fork() };
                assert!(pid >= 0, "fork failed: {}", io::Error::last_os_error());
                if pid == 0 {
                    loop {
                        unsafe { libc::pause() };
                    }
                }
                let mut cleanup = RawChildGuard::new(pid);
                let started = Instant::now();

                let error =
                    identify_launched_supervisor(pid, &inspector, Duration::from_millis(50))
                        .expect_err("transient identity inspection remains ambiguous");

                assert!(
                    started.elapsed() < Duration::from_secs(1),
                    "launcher blocked on the live detached candidate"
                );
                assert!(
                    error
                        .to_string()
                        .contains("SUPERVISOR_ACCEPTANCE_AMBIGUOUS"),
                    "{error}"
                );
                let mut wait_status = 0;
                assert_eq!(
                    unsafe { libc::waitpid(pid, &raw mut wait_status, libc::WNOHANG) },
                    0,
                    "ambiguous candidate was reaped or exited"
                );
                inspector.ambiguous.store(false, Ordering::SeqCst);
                let later = inspector.identity_for_pid(pid as u32).unwrap();
                assert_eq!(later.pid(), pid as u32, "candidate is later discoverable");

                assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
                assert_eq!(unsafe { libc::waitpid(pid, &raw mut wait_status, 0) }, pid);
                cleanup.disarm();
            },
        );
    }

    #[test]
    fn prepared_command_preserves_raw_home_and_tmpdir_bytes_under_non_utf8_host_root() {
        let command = CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap();
        let material = RequestFingerprintMaterial::new(
            "018f0f4a6b5c7d8e9f00112233445566".parse().unwrap(),
            "102f0f4a6b5c7d8e9f00112233445566".parse().unwrap(),
            "202f0f4a6b5c7d8e9f00112233445566".parse().unwrap(),
            "mini-1".into(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into(),
            String::new(),
            30_000,
            "heavy".into(),
            command.clone(),
        )
        .unwrap();
        let lease = LeaseRecord::new(&material, material.fingerprint(), 1, 2).unwrap();
        let host_root = PathBuf::from(std::ffi::OsString::from_vec(
            b"/tmp/mac-worker-\x80-host".to_vec(),
        ));
        let home = host_root.join("jobs/job/home");
        let tmp = host_root.join("jobs/job/tmp");

        let prepared = PreparedCommand::new(&command, &lease, &home, &tmp).unwrap();
        let environment = prepared
            ._environment
            .iter()
            .map(|entry| entry.as_bytes())
            .collect::<Vec<_>>();
        let expected_home = [b"HOME=".as_slice(), home.as_os_str().as_bytes()].concat();
        let expected_tmpdir = [b"TMPDIR=".as_slice(), tmp.as_os_str().as_bytes()].concat();

        assert!(environment.contains(&expected_home.as_slice()));
        assert!(environment.contains(&expected_tmpdir.as_slice()));
    }

    fn prepared_touch(marker: &Path) -> PreparedCommand {
        let program = CString::new("/usr/bin/touch").unwrap();
        let arguments = vec![
            CString::new("/usr/bin/touch").unwrap(),
            CString::new(marker.as_os_str().as_bytes()).unwrap(),
        ];
        let environment = vec![CString::new(format!("PATH={CONTROLLED_PATH}")).unwrap()];
        let mut argument_pointers = arguments
            .iter()
            .map(|argument| argument.as_ptr())
            .collect::<Vec<_>>();
        argument_pointers.push(ptr::null());
        let mut environment_pointers = environment
            .iter()
            .map(|entry| entry.as_ptr())
            .collect::<Vec<_>>();
        environment_pointers.push(ptr::null());
        PreparedCommand {
            program,
            _arguments: arguments,
            argument_pointers,
            _environment: environment,
            environment_pointers,
        }
    }

    fn child_descriptors(root: &Path) -> (File, File, File) {
        let cwd = File::open(root).unwrap();
        let stdout = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("stdout"))
            .unwrap();
        let stderr = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("stderr"))
            .unwrap();
        (cwd, stdout, stderr)
    }

    fn assert_already_reaped(pid: libc::pid_t) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let mut status = 0;
            let result = unsafe { libc::waitpid(pid, &raw mut status, libc::WNOHANG) };
            if result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
                return;
            }
            if result == pid {
                panic!("gated child exited but was not reaped by its owner");
            }
            assert_eq!(result, 0, "unexpected waitpid result");
            assert!(Instant::now() < deadline, "gated child did not exit");
            std::thread::yield_now();
        }
    }

    #[test]
    fn dropping_a_pre_go_child_closes_the_gate_reaps_and_executes_zero_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let command = prepared_touch(&marker);
        let (cwd, stdout, stderr) = child_descriptors(temp.path());
        let child = GatedChild::spawn(
            &command,
            cwd.as_raw_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            &SystemProcessInspector,
        )
        .unwrap();
        let pid = child.pid;

        drop(child);

        assert_already_reaped(pid);
        assert!(!marker.exists());
    }

    #[test]
    fn identity_rejection_after_ready_closes_the_gate_and_reaps() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let command = prepared_touch(&marker);
        let (cwd, stdout, stderr) = child_descriptors(temp.path());
        let inspector = RejectingInspector::new();

        let error = GatedChild::spawn(
            &command,
            cwd.as_raw_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            &inspector,
        )
        .err()
        .expect("injected inspector rejects the child identity");

        assert!(error.to_string().contains("PROCESS_AMBIGUOUS"));
        let pid = inspector.observed_pid.load(Ordering::SeqCst) as libc::pid_t;
        assert_ne!(pid, 0);
        assert_already_reaped(pid);
        assert!(!marker.exists());
    }

    #[test]
    fn missing_exec_ack_capability_is_rejected_before_go_and_executes_zero_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let command = prepared_touch(&marker);
        let (cwd, stdout, stderr) = child_descriptors(temp.path());
        let mut child = GatedChild::spawn(
            &command,
            cwd.as_raw_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            &SystemProcessInspector,
        )
        .unwrap();
        let pid = child.pid;
        drop(child.exec_result.take());

        let error = child.allow_exec(None).unwrap_err();
        drop(child);

        assert!(error.to_string().contains("CHILD_EXEC_INVALID"), "{error}");
        assert_already_reaped(pid);
        assert!(!marker.exists());
    }

    #[test]
    fn exec_ack_read_failure_after_go_is_classified_ambiguous_and_child_is_owned() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("did-execute");
        let command = prepared_touch(&marker);
        let (cwd, stdout, stderr) = child_descriptors(temp.path());
        let mut child = GatedChild::spawn(
            &command,
            cwd.as_raw_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            &SystemProcessInspector,
        )
        .unwrap();
        let original_ack = child
            .exec_result
            .replace(OwnedFd::from(File::open(temp.path()).unwrap()));
        drop(original_ack);

        let error = child.allow_exec(None).unwrap_err();

        assert!(matches!(error, ExecStartError::Ambiguous(_)));
        assert_eq!(
            wait_for_child(child.identity(), 1_000, &SystemProcessInspector).unwrap(),
            ChildOutcome::Exited(0)
        );
        assert!(marker.exists());
    }

    #[test]
    fn parent_death_helper_process() {
        let Some(marker) = std::env::var_os("MAC_WORKER_TEST_PARENT_DEATH_MARKER") else {
            return;
        };
        let identity_path = PathBuf::from(
            std::env::var_os("MAC_WORKER_TEST_PARENT_DEATH_IDENTITY")
                .expect("parent-death helper identity path is set"),
        );
        let root = identity_path
            .parent()
            .expect("parent-death helper has a parent directory");
        let command = prepared_touch(Path::new(&marker));
        let (cwd, stdout, stderr) = child_descriptors(root);
        let child = GatedChild::spawn(
            &command,
            cwd.as_raw_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            &SystemProcessInspector,
        )
        .unwrap();
        fs::write(
            identity_path,
            serde_json::to_vec(&child.identity()).unwrap(),
        )
        .unwrap();
        std::process::exit(0);
    }

    #[test]
    fn real_parent_process_exit_closes_go_before_exec() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let identity_path = temp.path().join("child-identity.json");
        let output = Command::new(std::env::current_exe().unwrap())
            .env("MAC_WORKER_TEST_PARENT_DEATH_MARKER", &marker)
            .env("MAC_WORKER_TEST_PARENT_DEATH_IDENTITY", &identity_path)
            .args([
                "--exact",
                "supervisor::tests::parent_death_helper_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let identity: ProcessIdentity =
            serde_json::from_slice(&fs::read(identity_path).unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match SystemProcessInspector.observe(identity) {
                ProcessObservation::Absent | ProcessObservation::Reused => break,
                _ if Instant::now() < deadline => std::thread::sleep(POLL_INTERVAL),
                observation => panic!("orphaned gated child remained {observation:?}"),
            }
        }
        assert!(!marker.exists());
    }

    #[test]
    fn gated_exec_closes_a_planted_unrelated_descriptor() {
        const PLANTED_FD: RawFd = 200;
        const HELPER_ROOT: &str = "MAC_WORKER_TEST_FD_INVENTORY_ROOT";
        let Some(root) = std::env::var_os(HELPER_ROOT) else {
            let temp = tempfile::tempdir().unwrap();
            let output = Command::new(std::env::current_exe().unwrap())
                .env(HELPER_ROOT, temp.path())
                .args([
                    "--exact",
                    "supervisor::tests::gated_exec_closes_a_planted_unrelated_descriptor",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        };
        let root = PathBuf::from(root);
        let planted_path = root.join("planted-fd-output");
        let planted = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&planted_path)
            .unwrap();
        let duplicated = unsafe { libc::dup2(planted.as_raw_fd(), PLANTED_FD) };
        assert_eq!(duplicated, PLANTED_FD);
        let planted_descriptor = unsafe { OwnedFd::from_raw_fd(PLANTED_FD) };

        let program = CString::new("/bin/sh").unwrap();
        let arguments = vec![
            CString::new("/bin/sh").unwrap(),
            CString::new("-c").unwrap(),
            CString::new(format!("printf leaked >&{PLANTED_FD}")).unwrap(),
        ];
        let environment = vec![CString::new(format!("PATH={CONTROLLED_PATH}")).unwrap()];
        let mut argument_pointers = arguments
            .iter()
            .map(|argument| argument.as_ptr())
            .collect::<Vec<_>>();
        argument_pointers.push(ptr::null());
        let mut environment_pointers = environment
            .iter()
            .map(|entry| entry.as_ptr())
            .collect::<Vec<_>>();
        environment_pointers.push(ptr::null());
        let command = PreparedCommand {
            program,
            _arguments: arguments,
            argument_pointers,
            _environment: environment,
            environment_pointers,
        };
        let (cwd, stdout, stderr) = child_descriptors(&root);
        let mut child = GatedChild::spawn(
            &command,
            cwd.as_raw_fd(),
            stdout.as_raw_fd(),
            stderr.as_raw_fd(),
            &SystemProcessInspector,
        )
        .unwrap();

        child.allow_exec(None).unwrap();
        let outcome = wait_for_child(child.identity(), 1_000, &SystemProcessInspector).unwrap();
        drop(planted_descriptor);
        planted.sync_all().unwrap();

        assert!(matches!(outcome, ChildOutcome::Exited(code) if code != 0));
        assert_eq!(fs::metadata(planted_path).unwrap().len(), 0);
    }

    #[test]
    fn detached_stdio_contract_rejects_another_character_device() {
        let dev_null = File::open("/dev/null").unwrap();
        let dev_zero = File::open("/dev/zero").unwrap();
        let null_stat = descriptor_stat(dev_null.as_raw_fd()).unwrap();
        let zero_stat = descriptor_stat(dev_zero.as_raw_fd()).unwrap();

        validate_null_stdio([null_stat, null_stat, null_stat], null_stat).unwrap();
        assert!(validate_null_stdio([zero_stat, zero_stat, zero_stat], null_stat).is_err());
    }
}
