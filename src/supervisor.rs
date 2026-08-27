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
        let pid_u32: u32 = pid.try_into().map_err(|_| {
            protocol_code("SUPERVISOR_IDENTITY_INVALID", "supervisor PID is invalid")
        })?;
        let identity = match self.inspector.identity_for_pid(pid_u32) {
            Ok(identity) => identity,
            Err(error) => {
                let _ = wait_blocking(pid);
                return Err(error);
            }
        };
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
        drop(guard);
        Ok(LaunchCandidate::new(identity))
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

    pub fn run_with_guard(&self, job_id: JobId, guard: SupervisorGuard) -> Result<(), WorkerError> {
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
        replace_status(&job, &status_bytes, &status, &supervised)?;
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
                    replace_status(&job, &status_bytes, &status, &child_status)?;
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
                let terminal =
                    prelaunch_terminal(&job, &status_bytes, &status, &stdout, &stderr, code)?;
                drop(guard);
                self.cleanup_and_release(&lease, &job, terminal)?;
                return Err(error);
            }
            Err(ExecStartError::Ambiguous(error)) => {
                finish_ambiguous_child(
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
                    replace_status(&job, &status_bytes, &status, &running)?;
                    Ok(running)
                })
        };
        let running = match running_result {
            Ok(running) => running,
            Err(error) => {
                finish_ambiguous_child(
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
        replace_status(&job, &status_bytes, &status, &terminal)?;
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
                    enrich_cleanup_error(job, &terminal, "LEASE_RELEASE_FAILED")?;
                    return Err(error);
                }
                Ok(())
            }
            Err(error) => {
                enrich_cleanup_error(job, &terminal, "MUTABLE_CLEANUP_FAILED")?;
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
        guard: SupervisorGuard,
        code: &str,
        original: WorkerError,
    ) -> Result<(), WorkerError> {
        let terminal =
            match erase_payload_and_record_prelaunch(job, expected_status_bytes, status, code) {
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
        let environment = [
            "LC_ALL=C".to_owned(),
            "LANG=C".to_owned(),
            format!("PATH={CONTROLLED_PATH}"),
            format!("HOME={}", home.display()),
            format!("TMPDIR={}", tmp.display()),
            format!("MAC_WORKER_JOB_ID={}", lease.job_id()),
            format!("MAC_WORKER_CLIENT_ID={}", lease.client_id()),
            format!("MAC_WORKER_PROJECT_ID={}", lease.project_id()),
            format!("MAC_WORKER_WORKTREE_ID={}", lease.worktree_id()),
        ]
        .into_iter()
        .map(|entry| {
            CString::new(entry).map_err(|_| {
                protocol_code("INVALID_ENVIRONMENT", "controlled environment is invalid")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
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
            terminate_exact_group(identity, inspector)?;
            return Ok(ChildOutcome::TimedOut);
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
                if error.raw_os_error() != Some(libc::ESRCH)
                    || inspector.observe_group_members(identity.pid())
                        != ProcessGroupMembership::LeaderOnly
                {
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
                                if error.raw_os_error() != Some(libc::ESRCH)
                                    || inspector.observe_group_members(identity.pid())
                                        != ProcessGroupMembership::LeaderOnly
                                {
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
) -> Result<(), WorkerError> {
    let pid = identity.pid();
    require_matching_group(identity, inspector)?;
    if unsafe { libc::kill(-(pid as i32), libc::SIGTERM) } != 0 {
        return Err(WorkerError::Io(io::Error::last_os_error()));
    }
    let deadline = Instant::now() + TERM_GRACE;
    loop {
        // An exact waitid(P_PID, ..., WNOWAIT) event retains this child as a
        // waitable zombie. Its PID cannot be reused before we reap it, so the
        // already-validated PID/start/PGID remains the authority for a final
        // group signal even though macOS proc_pidinfo no longer reports the
        // zombie as a live matching process.
        if !child_is_waitable(pid as libc::pid_t)? {
            require_matching_group(identity, inspector)?;
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    let waitable = child_is_waitable(pid as libc::pid_t)?;
    if !waitable {
        require_matching_group(identity, inspector)?;
    }
    let should_kill = if waitable {
        match inspector.observe_group_members(pid) {
            ProcessGroupMembership::LeaderOnly => false,
            ProcessGroupMembership::OtherMembers => true,
            ProcessGroupMembership::Ambiguous => {
                let _ = wait_blocking(pid as libc::pid_t);
                return Err(protocol_code(
                    "CHILD_TERMINATION_AMBIGUOUS",
                    "exact process-group membership could not be inspected",
                ));
            }
        }
    } else {
        true
    };
    if should_kill && unsafe { libc::kill(-(pid as i32), libc::SIGKILL) } != 0 {
        return Err(WorkerError::Io(io::Error::last_os_error()));
    }
    wait_blocking(pid as libc::pid_t)?;
    wait_for_terminated_group_absence(identity, inspector)
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

fn require_matching_group(
    identity: ProcessIdentity,
    inspector: &dyn ProcessInspector,
) -> Result<(), WorkerError> {
    match inspector.observe(identity) {
        ProcessObservation::Matching { process_group } if process_group == identity.pid() => Ok(()),
        _ => Err(protocol_code(
            "CHILD_IDENTITY_AMBIGUOUS",
            "targeted child process group no longer matches",
        )),
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

fn prelaunch_terminal(
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
    replace_status(job, expected_bytes, status, &terminal)?;
    Ok(terminal)
}

fn erase_payload_and_record_prelaunch(
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
    prelaunch_terminal(job, expected_bytes, status, &stdout, &stderr, code)
}

#[allow(clippy::too_many_arguments)]
fn finish_ambiguous_child(
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
    replace_status(job, expected_bytes, status, &terminal)?;
    Ok(terminal)
}

fn record_prelaunch_failure(
    job: &RootedDir,
    expected_bytes: &[u8],
    status: &JobStatus,
    stdout: &File,
    stderr: &File,
    code: &str,
) -> Result<(), WorkerError> {
    prelaunch_terminal(job, expected_bytes, status, stdout, stderr, code).map(|_| ())
}

fn enrich_cleanup_error(
    job: &RootedDir,
    terminal: &JobStatus,
    code: &str,
) -> Result<(), WorkerError> {
    let (bytes, current): (Vec<u8>, JobStatus) =
        read_canonical_json_with_bytes(job, "status.json")?;
    if current != *terminal {
        return Err(protocol_code(
            "STATUS_CHANGED",
            "terminal status changed before cleanup enrichment",
        ));
    }
    let enriched = current.with_cleanup_error(code.into(), now_millis()?)?;
    replace_status(job, &bytes, &current, &enriched)
}

fn replace_status(
    job: &RootedDir,
    expected_bytes: &[u8],
    expected: &JobStatus,
    replacement: &JobStatus,
) -> Result<(), WorkerError> {
    expected.transition(replacement.clone())?;
    let bytes = canonical_json(replacement)?;
    job.replace_private_regular_exact("status.json", expected_bytes, &bytes)?;
    Ok(())
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
        return Err(protocol_code(
            "EXECUTABLE_NOT_FOUND",
            "relative executable paths containing separators are unsupported",
        ));
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
        process::Command,
        sync::atomic::{AtomicU32, Ordering},
    };

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
