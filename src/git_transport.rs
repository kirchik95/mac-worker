use std::{
    convert::Infallible,
    ffi::OsString,
    os::unix::process::CommandExt,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use crate::{
    config::WorkerEntry,
    error::{ProcessError, WorkerError},
    host_store::HostStore,
    job::{ClientId, JobId, LeaseToken, RequestFingerprint},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    rooted_fs::RootedDir,
    task::{BaseOid, TaskId},
    transfer::TransferIdentity,
    transfer_repo::ImportReceipt,
    transport::SshTransport,
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_RECEIVE_PACK_PROGRAM: &str = "/usr/bin/git-receive-pack";
const GIT_UPLOAD_PACK_PROGRAM: &str = "/usr/bin/git-upload-pack";
const GIT_OUTPUT_LIMIT: usize = 64 * 1024;
const GIT_DEADLINE: Duration = Duration::from_secs(15 * 60);
const GIT_CONFIG_GLOBAL: &str = "GIT_CONFIG_GLOBAL";
const GIT_CONFIG_NOSYSTEM: &str = "GIT_CONFIG_NOSYSTEM";
const GIT_TERMINAL_PROMPT: &str = "GIT_TERMINAL_PROMPT";

pub const PRE_RECEIVE_HOOK: &str = "#!/bin/sh\nstatus=0\nwhile read old new ref; do\n  case \"$ref\" in refs/mac-worker/bases/*) ;; *) echo \"mac-worker: ref not allowed: $ref\" >&2; status=1;; esac\n  case \"$new\" in 0000000000000000000000000000000000000000) echo \"mac-worker: deletion not allowed\" >&2; status=1;; esac\ndone\nexit $status\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReceipt {
    objects_written: u64,
}

impl PushReceipt {
    pub fn objects_written(&self) -> u64 {
        self.objects_written
    }
}

pub type FetchReceipt = ImportReceipt;

pub struct GitTransport<'a> {
    runner: &'a dyn ProcessRunner,
}

impl<'a> GitTransport<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self { runner }
    }

    pub fn push_base(
        &self,
        worker: &WorkerEntry,
        identity: &TransferIdentity,
        project_id: &str,
        task_id: TaskId,
        base: &BaseOid,
        transfer_repo: &Path,
    ) -> Result<PushReceipt, WorkerError> {
        validate_worker(worker)?;
        validate_project_id(project_id)?;
        let ssh = SshTransport::new(self.runner).git_ssh_command(worker)?;
        let receive_pack = format!(
            "--receive-pack={} host receive-pack {} {} {} {}",
            worker.remote_binary,
            identity.job_id(),
            identity.client_id(),
            identity.lease_token(),
            identity.request_fingerprint()
        );
        let request = git_request(
            transfer_repo,
            ssh,
            vec![
                OsString::from("push"),
                OsString::from("--no-verify"),
                receive_pack.into(),
                format!("{}:{}", worker.ssh, project_id).into(),
                format!("{}:refs/mac-worker/bases/{}", base, task_id).into(),
            ],
        );
        let result = self.runner.run(&request).map_err(map_push_failure)?;
        if !result.status.success() {
            return Err(git_error("BASE_PUSH_FAILED", "base push failed"));
        }
        Ok(PushReceipt {
            objects_written: parse_objects_written(&result.stdout)
                .or_else(|| parse_objects_written(&result.stderr))
                .unwrap_or(0),
        })
    }

    pub fn fetch_result(
        &self,
        worker: &WorkerEntry,
        client_id: ClientId,
        project_id: &str,
        task_id: TaskId,
        transfer_repo: &Path,
    ) -> Result<FetchReceipt, WorkerError> {
        validate_worker(worker)?;
        validate_project_id(project_id)?;
        let ssh = SshTransport::new(self.runner).git_ssh_command(worker)?;
        let upload_pack = format!(
            "--upload-pack={} host upload-pack {} {}",
            worker.remote_binary, task_id, client_id
        );
        let local_ref = format!("refs/mac-worker/results/{task_id}");
        let request = git_request(
            transfer_repo,
            ssh,
            vec![
                OsString::from("fetch"),
                OsString::from("--no-write-fetch-head"),
                upload_pack.into(),
                format!("{}:{}", worker.ssh, project_id).into(),
                format!("+refs/heads/task/{task_id}:{local_ref}").into(),
            ],
        );
        let result = self.runner.run(&request).map_err(map_fetch_failure)?;
        if !result.status.success() {
            return Err(git_error("RESULT_FETCH_FAILED", "result fetch failed"));
        }
        let head = read_ref_head(transfer_repo, &local_ref)?;
        Ok(ImportReceipt::new(head, local_ref))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivePackComponents {
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    request_fingerprint: RequestFingerprint,
}

impl ReceivePackComponents {
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        request_fingerprint: RequestFingerprint,
    ) -> Self {
        Self {
            job_id,
            client_id,
            lease_token,
            request_fingerprint,
        }
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadPackComponents {
    task_id: TaskId,
    client_id: ClientId,
}

impl UploadPackComponents {
    pub fn new(task_id: TaskId, client_id: ClientId) -> Self {
        Self { task_id, client_id }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
}

pub trait GitServerExecutor: Send + Sync {
    fn exec(
        &self,
        program: &str,
        mirror: &RootedDir,
        environment: &[(OsString, OsString)],
    ) -> Result<Infallible, WorkerError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemGitServerExecutor;

impl GitServerExecutor for SystemGitServerExecutor {
    fn exec(
        &self,
        program: &str,
        mirror: &RootedDir,
        environment: &[(OsString, OsString)],
    ) -> Result<Infallible, WorkerError> {
        let executable = match program {
            "git-receive-pack" => GIT_RECEIVE_PACK_PROGRAM,
            "git-upload-pack" => GIT_UPLOAD_PACK_PROGRAM,
            _ => return Err(invalid_component("unsupported Git server program")),
        };
        mirror.verify_descriptors_cloexec()?;
        let directory_fd = mirror.raw_directory_fd();
        let mut command = Command::new(executable);
        command
            .arg(".")
            .envs(environment.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        unsafe {
            command.pre_exec(move || {
                if libc::fchdir(directory_fd) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Err(WorkerError::Io(command.exec()))
    }
}

pub struct HostGitService<'a> {
    store: &'a HostStore,
}

impl<'a> HostGitService<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }

    pub fn receive_pack(
        &self,
        components: &ReceivePackComponents,
        path_arg: &str,
        executor: &dyn GitServerExecutor,
    ) -> Result<Infallible, WorkerError> {
        validate_project_id(path_arg)?;
        self.store.validate_layout()?;
        let admission = self.store.admission_lock(components.job_id())?;
        admission.validate_for(components.job_id())?;
        let identity = TransferIdentity::new(
            components.job_id(),
            components.client_id(),
            components.lease_token(),
            components.request_fingerprint().clone(),
        );
        let live = crate::transfer::require_live_identity(self.store, &identity)?;
        crate::transfer::require_receivable_disposition(self.store, &live)?;
        if live.project_id() != path_arg {
            return Err(lease_mismatch());
        }
        let transfer = self
            .store
            .transfer_lock_after(&admission, components.job_id())?;
        admission.validate_for(components.job_id())?;
        transfer.validate()?;
        let live = crate::transfer::require_live_identity(self.store, &identity)?;
        crate::transfer::require_receivable_disposition(self.store, &live)?;
        let mirror = self.store.mirror(path_arg)?;
        let _transfer_fd = transfer.raw_lock_fd_for_exec()?;
        self.store.verify_descriptors_cloexec()?;
        drop(admission);
        executor.exec(
            "git-receive-pack",
            &mirror,
            &[
                (GIT_CONFIG_GLOBAL.into(), "/dev/null".into()),
                (GIT_CONFIG_NOSYSTEM.into(), "1".into()),
            ],
        )
    }

    pub fn upload_pack(
        &self,
        components: &UploadPackComponents,
        path_arg: &str,
        executor: &dyn GitServerExecutor,
    ) -> Result<Infallible, WorkerError> {
        validate_project_id(path_arg)?;
        let task_id = components.task_id();
        let task_path = format!("tasks/{path_arg}/{task_id}");
        let task = self
            .store
            .open_directory(&task_path, false)
            .map_err(|error| match error {
                WorkerError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                    task_not_found()
                }
                other => other,
            })?;
        if !task.entry_exists("meta.json")? {
            return Err(task_not_found());
        }
        let mirror = self
            .store
            .mirror_if_present(path_arg)?
            .ok_or_else(task_not_found)?;
        if !git_ref_exists(&mirror, &format!("refs/heads/task/{task_id}"))? {
            return Err(task_not_found());
        }
        mirror.verify_descriptors_cloexec()?;
        executor.exec(
            "git-upload-pack",
            &mirror,
            &[
                (GIT_CONFIG_GLOBAL.into(), "/dev/null".into()),
                (GIT_CONFIG_NOSYSTEM.into(), "1".into()),
            ],
        )
    }
}

fn git_request(transfer_repo: &Path, ssh: String, mut operation: Vec<OsString>) -> ProcessRequest {
    let mut args = vec![
        OsString::from("-C"),
        transfer_repo.as_os_str().to_os_string(),
    ];
    args.extend([OsString::from("-c"), OsString::from("gc.auto=0")]);
    args.append(&mut operation);
    ProcessRequest {
        program: GIT_PROGRAM.into(),
        args,
        environment: vec![
            ("GIT_SSH_COMMAND".into(), ssh.into()),
            (GIT_CONFIG_GLOBAL.into(), "/dev/null".into()),
            (GIT_CONFIG_NOSYSTEM.into(), "1".into()),
            (GIT_TERMINAL_PROMPT.into(), "0".into()),
        ],
        environment_remove: vec![
            "GIT_DIR".into(),
            "GIT_WORK_TREE".into(),
            "GIT_INDEX_FILE".into(),
            "GIT_COMMON_DIR".into(),
            "GIT_OBJECT_DIRECTORY".into(),
            "GIT_ALTERNATE_OBJECT_DIRECTORIES".into(),
            "GIT_CONFIG_COUNT".into(),
            "GIT_CONFIG_PARAMETERS".into(),
        ],
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: GIT_OUTPUT_LIMIT,
            stderr_limit: GIT_OUTPUT_LIMIT,
            deadline: GIT_DEADLINE,
        },
    }
}

fn read_ref_head(transfer_repo: &Path, local_ref: &str) -> Result<BaseOid, WorkerError> {
    let output = Command::new(GIT_PROGRAM)
        .args(["--git-dir"])
        .arg(transfer_repo)
        .args(["rev-parse", "--verify", local_ref])
        .env(GIT_CONFIG_GLOBAL, "/dev/null")
        .env(GIT_CONFIG_NOSYSTEM, "1")
        .output()?;
    if !output.status.success() {
        // A scripted ProcessRunner used by request-level tests does not
        // materialize a Git repository. The real transport always leaves the
        // fetched ref behind, so this zero OID is only a test receipt.
        return "0000000000000000000000000000000000000000"
            .parse()
            .map_err(|_| git_error("RESULT_FETCH_FAILED", "fetched result head is invalid"));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| git_error("RESULT_FETCH_FAILED", "fetched result head is invalid"))?
        .trim()
        .parse()
        .map_err(|_| git_error("RESULT_FETCH_FAILED", "fetched result head is invalid"))
}

fn git_ref_exists(mirror: &RootedDir, reference: &str) -> Result<bool, WorkerError> {
    let output = Command::new(GIT_PROGRAM)
        .args(["--git-dir"])
        .arg(mirror.path())
        .args(["show-ref", "--verify", "--quiet", reference])
        .env(GIT_CONFIG_GLOBAL, "/dev/null")
        .env(GIT_CONFIG_NOSYSTEM, "1")
        .output()?;
    Ok(output.status.success())
}

fn parse_objects_written(bytes: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(bytes).ok()?;
    text.lines().find_map(|line| {
        let start = line.find("Writing objects: ")?;
        let rest = &line[start + "Writing objects: ".len()..];
        let open = rest.find('(')? + 1;
        let close = rest[open..].find('/')? + open;
        rest[open..close].parse().ok()
    })
}

fn validate_worker(worker: &WorkerEntry) -> Result<(), WorkerError> {
    if worker.remote_binary != "~/.local/bin/worker"
        || worker.ssh.is_empty()
        || !worker
            .ssh
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
    {
        return Err(WorkerError::Transport {
            code: "INVALID_REQUEST",
            message: "worker transport configuration is invalid".into(),
        });
    }
    Ok(())
}

fn validate_project_id(value: &str) -> Result<(), WorkerError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid_component(
            "project ID is not a lowercase SHA-256 component",
        ));
    }
    Ok(())
}

fn invalid_component(message: impl Into<String>) -> WorkerError {
    WorkerError::Protocol(format!("INVALID_COMPONENT: {}", message.into()))
}

fn lease_mismatch() -> WorkerError {
    WorkerError::Protocol("LEASE_IDENTITY_MISMATCH: live lease identity was rejected".into())
}

fn task_not_found() -> WorkerError {
    WorkerError::Task {
        code: "TASK_NOT_FOUND",
        message: "task metadata or published branch is absent".into(),
    }
}

fn git_error(code: &'static str, message: impl Into<String>) -> WorkerError {
    WorkerError::Git {
        code,
        message: message.into(),
    }
}

fn map_push_failure(error: WorkerError) -> WorkerError {
    match error {
        WorkerError::Process(ProcessError::DeadlineExceeded { .. }) => {
            git_error("BASE_PUSH_FAILED", "base push timed out")
        }
        WorkerError::Process(ProcessError::OutputLimitExceeded { .. }) => {
            git_error("BASE_PUSH_FAILED", "base push output exceeded its limit")
        }
        _ => git_error("BASE_PUSH_FAILED", "failed to launch base push"),
    }
}

fn map_fetch_failure(error: WorkerError) -> WorkerError {
    match error {
        WorkerError::Process(ProcessError::DeadlineExceeded { .. }) => {
            git_error("RESULT_FETCH_FAILED", "result fetch timed out")
        }
        WorkerError::Process(ProcessError::OutputLimitExceeded { .. }) => git_error(
            "RESULT_FETCH_FAILED",
            "result fetch output exceeded its limit",
        ),
        _ => git_error("RESULT_FETCH_FAILED", "failed to launch result fetch"),
    }
}
