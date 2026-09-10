use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    fs::{File, OpenOptions},
    io::{self, Read},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    account_launch::account_login_shell_request,
    error::{ProcessError, WorkerError},
    process::{ProcessPolicy, ProcessRunner},
    project_config::SetupSettings,
    redaction::RedactionBoundary,
    rooted_fs::RootedDir,
};

pub const SETUP_RECEIPT_VERSION: u32 = 1;
pub const SETUP_LOG_LIMIT: usize = 64 * 1024;
pub const SETUP_RESULT_FILE: &str = "setup-result.json";
pub const SETUP_STAGE_EXIT_CODE: u8 = 78;
const SETUP_CACHE_DIR: &str = "project-setup";
const RECEIPT_MAX_BYTES: u64 = 16 * 1024;
const SETUP_RESULT_MAX_BYTES: u64 = 4 * 1024;
const WORKSPACE_FILE_MAX_BYTES: u64 = 8 * 1024 * 1024;
const LOCK_POLL: Duration = Duration::from_millis(50);
const DIAGNOSTIC_TAIL_BYTES: usize = 200;

pub struct SetupRequest<'a> {
    pub account_home: &'a Path,
    pub project_id: &'a str,
    pub task_id: &'a str,
    pub env_profile_name: Option<&'a str>,
    pub workspace: &'a Path,
    pub cache_root: &'a Path,
    pub requires: &'a [String],
    pub recipe: &'a SetupSettings,
    pub profile_entries: &'a [(OsString, OsString)],
    pub now_millis: u64,
    pub deadline: Instant,
    pub should_cancel: &'a dyn Fn() -> bool,
}

impl fmt::Debug for SetupRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let profile_entry_names = self
            .profile_entries
            .iter()
            .map(|(name, _)| name.to_string_lossy())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("SetupRequest")
            .field("account_home", &self.account_home)
            .field("project_id", &self.project_id)
            .field("task_id", &self.task_id)
            .field("env_profile_name", &self.env_profile_name)
            .field("workspace", &self.workspace)
            .field("cache_root", &self.cache_root)
            .field("requires", &self.requires)
            .field("recipe", &self.recipe)
            .field("profile_entry_count", &self.profile_entries.len())
            .field("profile_entry_names", &profile_entry_names)
            .field("now_millis", &self.now_millis)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupAction {
    Reused { identity: String },
    Ran { identity: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupStageResult {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupReceipt {
    pub version: u32,
    pub identity: String,
    pub task_id: String,
    pub completed_at_millis: u64,
}

#[derive(Debug, Serialize)]
struct SetupIdentityMaterial<'a> {
    account: &'a str,
    project_id: &'a str,
    env_profile: &'a str,
    requires: &'a [String],
    timeout_millis: u64,
    commands: &'a [String],
    check: Option<&'a str>,
    lockfiles: &'a BTreeMap<String, String>,
    inputs: &'a BTreeMap<String, String>,
}

pub fn setup_cache_dir(cache_root: &Path) -> PathBuf {
    cache_root.join(SETUP_CACHE_DIR)
}

pub fn compute_setup_identity(
    account_home: &Path,
    project_id: &str,
    env_profile_name: Option<&str>,
    requires: &[String],
    recipe: &SetupSettings,
    workspace: &Path,
) -> Result<String, WorkerError> {
    let lockfiles = file_digests(workspace, &recipe.lockfiles)?;
    let inputs = file_digests(workspace, &recipe.inputs)?;
    let account = sha256_hex(account_home.as_os_str().as_encoded_bytes());
    let timeout_millis = duration_millis(recipe.timeout)?;
    let material = SetupIdentityMaterial {
        account: &account,
        project_id,
        env_profile: env_profile_name.unwrap_or(""),
        requires,
        timeout_millis,
        commands: &recipe.commands,
        check: recipe.check.as_deref(),
        lockfiles: &lockfiles,
        inputs: &inputs,
    };
    let encoded = serde_json::to_vec(&material).map_err(|error| {
        setup_error(
            "SETUP_FAILED",
            format!("setup identity could not be encoded: {error}"),
        )
    })?;
    Ok(sha256_hex(&encoded))
}

pub fn persist_setup_stage_result(turn_dir: &Path, error: &WorkerError) -> Result<(), WorkerError> {
    let directory = RootedDir::open(turn_dir).map_err(|error| {
        setup_error(
            "SETUP_FAILED",
            format!("setup result directory is unavailable: {error}"),
        )
    })?;
    write_setup_stage_result(&directory, error)
}

pub fn write_setup_stage_result(
    turn_dir: &RootedDir,
    error: &WorkerError,
) -> Result<(), WorkerError> {
    let result = SetupStageResult {
        code: error.public_code(),
        message: setup_stage_message(error),
    };
    let bytes = serde_json::to_vec(&result).map_err(|error| {
        setup_error(
            "SETUP_FAILED",
            format!("setup result could not be encoded: {error}"),
        )
    })?;
    if turn_dir
        .entry_exists(SETUP_RESULT_FILE)
        .map_err(cache_unavailable)?
    {
        return Ok(());
    }
    match turn_dir.write_private_atomic_no_replace(SETUP_RESULT_FILE, &bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(cache_unavailable(error)),
    }
}

pub fn load_setup_stage_result(job: &RootedDir) -> Result<Option<SetupStageResult>, WorkerError> {
    match job.read_private_regular(SETUP_RESULT_FILE, SETUP_RESULT_MAX_BYTES) {
        Ok(bytes) => {
            let result = serde_json::from_slice(&bytes)
                .map_err(|_| setup_error("SETUP_FAILED", "setup result is not valid JSON"))?;
            Ok(Some(result))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => Ok(None),
        Err(error) => Err(cache_unavailable(error)),
    }
}

fn setup_stage_message(error: &WorkerError) -> String {
    match error {
        WorkerError::Task { message, .. } => message.as_ref().to_owned(),
        _ => error.public_code(),
    }
}

pub fn prepare_project_setup(
    runner: &dyn ProcessRunner,
    request: SetupRequest<'_>,
) -> Result<SetupAction, WorkerError> {
    validate_task_id(request.task_id)?;
    if (request.should_cancel)() {
        return Err(setup_error(
            "SETUP_CANCELLED",
            "project setup was cancelled",
        ));
    }
    remaining_deadline(request.deadline)?;
    let identity = compute_setup_identity(
        request.account_home,
        request.project_id,
        request.env_profile_name,
        request.requires,
        request.recipe,
        request.workspace,
    )?;
    let cache = open_setup_cache(request.cache_root)?;
    let _lock = exclusive_lock(
        &cache,
        request.task_id,
        request.deadline,
        request.should_cancel,
    )?;
    if (request.should_cancel)() {
        return Err(setup_error(
            "SETUP_CANCELLED",
            "project setup was cancelled",
        ));
    }
    remaining_deadline(request.deadline)?;

    let receipt_matches = task_receipt_matches(&cache, request.task_id, &identity)?;
    if let Some(check) = request.recipe.check.as_deref() {
        match run_setup_step(
            runner,
            &request,
            SetupStep::Check {
                command: check,
                after_recipe: false,
            },
        ) {
            Ok(()) if receipt_matches => {
                return Ok(SetupAction::Reused { identity });
            }
            Ok(()) => {}
            Err(error) if error.public_code() == "SETUP_TIMEOUT" => return Err(error),
            Err(error) if error.public_code() == "SETUP_CANCELLED" => return Err(error),
            Err(_) => {}
        }
    }

    let total = request.recipe.commands.len();
    for (index, command) in request.recipe.commands.iter().enumerate() {
        if (request.should_cancel)() {
            return Err(setup_error(
                "SETUP_CANCELLED",
                "project setup was cancelled",
            ));
        }
        remaining_deadline(request.deadline)?;
        run_setup_step(
            runner,
            &request,
            SetupStep::Command {
                command,
                index: index + 1,
                total,
            },
        )?;
    }
    if let Some(check) = request.recipe.check.as_deref() {
        run_setup_step(
            runner,
            &request,
            SetupStep::Check {
                command: check,
                after_recipe: true,
            },
        )?;
    }
    if (request.should_cancel)() {
        return Err(setup_error(
            "SETUP_CANCELLED",
            "project setup was cancelled",
        ));
    }
    remaining_deadline(request.deadline)?;
    publish_receipt(
        &cache,
        request.task_id,
        &SetupReceipt {
            version: SETUP_RECEIPT_VERSION,
            identity: identity.clone(),
            task_id: request.task_id.to_owned(),
            completed_at_millis: request.now_millis,
        },
    )?;
    Ok(SetupAction::Ran { identity })
}

#[derive(Clone, Copy)]
enum SetupStep<'a> {
    Command {
        command: &'a str,
        index: usize,
        total: usize,
    },
    Check {
        command: &'a str,
        after_recipe: bool,
    },
}

impl<'a> SetupStep<'a> {
    fn command(self) -> &'a str {
        match self {
            Self::Command { command, .. } | Self::Check { command, .. } => command,
        }
    }

    fn failed_message(self, detail: &str) -> String {
        let body = if detail.is_empty() {
            String::new()
        } else {
            format!(": {detail}")
        };
        match self {
            Self::Command { index, total, .. } => {
                format!("setup command {index}/{total} failed{body}")
            }
            Self::Check {
                after_recipe: true, ..
            } => format!("project setup did not satisfy the current-workspace check{body}"),
            Self::Check {
                after_recipe: false,
                ..
            } => format!("setup check failed{body}"),
        }
    }
}

fn task_receipt_matches(
    cache: &RootedDir,
    task_id: &str,
    identity: &str,
) -> Result<bool, WorkerError> {
    let name = receipt_name(task_id);
    match cache.read_private_regular(&name, RECEIPT_MAX_BYTES) {
        Ok(bytes) => {
            let Ok(receipt) = serde_json::from_slice::<SetupReceipt>(&bytes) else {
                return Ok(false);
            };
            Ok(receipt.version == SETUP_RECEIPT_VERSION
                && receipt.task_id == task_id
                && receipt.identity == identity)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::InvalidData => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => Ok(false),
        Err(error) => Err(cache_unavailable(error)),
    }
}

fn run_setup_step(
    runner: &dyn ProcessRunner,
    request: &SetupRequest<'_>,
    step: SetupStep<'_>,
) -> Result<(), WorkerError> {
    if (request.should_cancel)() {
        return Err(setup_error(
            "SETUP_CANCELLED",
            "project setup was cancelled",
        ));
    }
    let remaining = remaining_deadline(request.deadline)?;
    let shell = workspace_shell_command(request.workspace, step.command())?;
    let process = account_login_shell_request(
        request.account_home,
        request.profile_entries,
        &shell,
        ProcessPolicy {
            stdout_limit: SETUP_LOG_LIMIT,
            stderr_limit: SETUP_LOG_LIMIT,
            deadline: remaining,
        },
    );
    match runner.run_interruptible(&process, request.should_cancel) {
        Ok(result) if result.status.success() => Ok(()),
        Ok(result) => Err(setup_error(
            "SETUP_FAILED",
            step.failed_message(&sanitized_stderr(request, &result.stderr)),
        )),
        Err(WorkerError::Process(ProcessError::DeadlineExceeded { .. })) => Err(setup_error(
            "SETUP_TIMEOUT",
            "project setup exceeded its deadline",
        )),
        Err(WorkerError::Process(ProcessError::Cancelled)) => Err(setup_error(
            "SETUP_CANCELLED",
            "project setup was cancelled",
        )),
        Err(_) => Err(setup_error("SETUP_FAILED", step.failed_message(""))),
    }
}

fn sanitized_stderr(request: &SetupRequest<'_>, stderr: &[u8]) -> String {
    let lossy = String::from_utf8_lossy(stderr);
    if lossy.trim().is_empty() {
        return String::new();
    }
    let secrets = request
        .profile_entries
        .iter()
        .filter_map(|(_, value)| value.to_str());
    let redacted = RedactionBoundary::new(request.account_home)
        .with_secrets(secrets)
        .text(&lossy, usize::MAX);
    byte_tail(&redacted, DIAGNOSTIC_TAIL_BYTES)
}

fn byte_tail(input: &str, max_bytes: usize) -> String {
    let trimmed = input.trim();
    if trimmed.len() <= max_bytes {
        return trimmed.to_owned();
    }
    let mut start = trimmed.len() - max_bytes;
    while start < trimmed.len() && !trimmed.is_char_boundary(start) {
        start += 1;
    }
    trimmed[start..].to_owned()
}

fn workspace_shell_command(workspace: &Path, command: &str) -> Result<String, WorkerError> {
    let path = workspace.to_str().ok_or_else(|| {
        setup_error(
            "SETUP_FAILED",
            "task workspace path is not valid UTF-8 for setup",
        )
    })?;
    Ok(format!("cd -- {} && {}", zsh_single_quote(path), command))
}

fn zsh_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn file_digests(
    workspace: &Path,
    files: &[String],
) -> Result<BTreeMap<String, String>, WorkerError> {
    let mut digests = BTreeMap::new();
    for relative in files {
        let path = workspace.join(relative);
        let bytes = read_bounded_nofollow(&path, WORKSPACE_FILE_MAX_BYTES)?;
        digests.insert(relative.clone(), sha256_hex(&bytes));
    }
    Ok(digests)
}

fn read_bounded_nofollow(path: &Path, maximum: u64) -> Result<Vec<u8>, WorkerError> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| {
            setup_error(
                "SETUP_FAILED",
                "setup lockfile or input is missing or not a regular file",
            )
        })?;
    let metadata = file.metadata().map_err(WorkerError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(setup_error(
            "SETUP_FAILED",
            "setup lockfile or input is missing or not a regular file",
        ));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(WorkerError::Io)?;
    if bytes.len() as u64 > maximum {
        return Err(setup_error(
            "SETUP_FAILED",
            "setup receipt or input is not a bounded regular file",
        ));
    }
    Ok(bytes)
}

fn publish_receipt(
    cache: &RootedDir,
    task_id: &str,
    receipt: &SetupReceipt,
) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(receipt).map_err(|error| {
        setup_error(
            "SETUP_FAILED",
            format!("setup receipt could not be encoded: {error}"),
        )
    })?;
    let name = receipt_name(task_id);
    if cache.entry_exists(&name).map_err(cache_unavailable)? {
        let previous = cache
            .read_private_regular(&name, RECEIPT_MAX_BYTES)
            .map_err(cache_unavailable)?;
        cache
            .replace_private_regular_exact(&name, &previous, &bytes)
            .map_err(cache_unavailable)
    } else {
        match cache.write_private_atomic_no_replace(&name, &bytes) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let previous = cache
                    .read_private_regular(&name, RECEIPT_MAX_BYTES)
                    .map_err(cache_unavailable)?;
                cache
                    .replace_private_regular_exact(&name, &previous, &bytes)
                    .map_err(cache_unavailable)
            }
            Err(error) => Err(cache_unavailable(error)),
        }
    }
}

fn open_setup_cache(cache_root: &Path) -> Result<RootedDir, WorkerError> {
    let parent = open_or_create_rooted(cache_root)?;
    match parent.create_new_child_directory(SETUP_CACHE_DIR) {
        Ok(child) => Ok(child),
        Err(error)
            if error.kind() == io::ErrorKind::AlreadyExists
                || error.raw_os_error() == Some(libc::EEXIST) =>
        {
            RootedDir::open(&setup_cache_dir(cache_root)).map_err(cache_unavailable)
        }
        Err(error) => Err(cache_unavailable(error)),
    }
}

fn open_or_create_rooted(path: &Path) -> Result<RootedDir, WorkerError> {
    match RootedDir::open(path) {
        Ok(directory) => Ok(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => match RootedDir::create(path) {
            Ok(directory) => Ok(directory),
            Err(error)
                if error.kind() == io::ErrorKind::AlreadyExists
                    || error.raw_os_error() == Some(libc::EEXIST) =>
            {
                RootedDir::open(path).map_err(cache_unavailable)
            }
            Err(error) => Err(cache_unavailable(error)),
        },
        Err(error) => Err(cache_unavailable(error)),
    }
}

fn exclusive_lock(
    cache: &RootedDir,
    task_id: &str,
    deadline: Instant,
    should_cancel: &dyn Fn() -> bool,
) -> Result<File, WorkerError> {
    let name = format!("task-{task_id}.lock");
    let file = cache.open_private_lock(&name).map_err(cache_unavailable)?;
    loop {
        if should_cancel() {
            return Err(setup_error(
                "SETUP_CANCELLED",
                "project setup was cancelled",
            ));
        }
        let remaining = remaining_deadline(deadline)?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            return Ok(file);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EAGAIN) {
            return Err(cache_unavailable(error));
        }
        thread::sleep(LOCK_POLL.min(remaining));
    }
}

fn remaining_deadline(deadline: Instant) -> Result<Duration, WorkerError> {
    match deadline.checked_duration_since(Instant::now()) {
        Some(remaining) if !remaining.is_zero() => Ok(remaining),
        _ => Err(setup_error(
            "SETUP_TIMEOUT",
            "project setup exceeded its deadline",
        )),
    }
}

fn receipt_name(task_id: &str) -> String {
    format!("task-{task_id}.json")
}

fn duration_millis(timeout: Duration) -> Result<u64, WorkerError> {
    timeout
        .as_millis()
        .try_into()
        .map_err(|_| setup_error("SETUP_FAILED", "setup timeout is too large to record"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn validate_task_id(task_id: &str) -> Result<(), WorkerError> {
    if task_id.len() != 32
        || !task_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(setup_error("SETUP_FAILED", "setup task id is invalid"));
    }
    Ok(())
}

fn cache_unavailable(_error: io::Error) -> WorkerError {
    setup_error("SETUP_FAILED", "setup cache is unavailable")
}

fn setup_error(
    code: &'static str,
    message: impl Into<std::borrow::Cow<'static, str>>,
) -> WorkerError {
    WorkerError::task(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::{fs::PermissionsExt, process::ExitStatusExt},
        process::ExitStatus,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    use crate::process::{ProcessRequest, ProcessResult, SystemProcessRunner};

    struct ScriptedRunner {
        results: Mutex<Vec<Result<ProcessResult, WorkerError>>>,
        requests: Mutex<Vec<ProcessRequest>>,
    }

    impl ScriptedRunner {
        fn succeeding() -> Self {
            Self {
                results: Mutex::new(Vec::new()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn failing() -> Self {
            Self::failing_with(b"failed")
        }

        fn failing_with(stderr: &[u8]) -> Self {
            Self {
                results: Mutex::new(vec![Ok(ProcessResult {
                    status: ExitStatus::from_raw(1 << 8),
                    stdout: Vec::new(),
                    stderr: stderr.to_vec(),
                })]),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn timed_out() -> Self {
            Self {
                results: Mutex::new(vec![Err(WorkerError::Process(
                    ProcessError::DeadlineExceeded {
                        deadline: Duration::from_secs(1),
                    },
                ))]),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn take_requests(&self) -> Vec<ProcessRequest> {
            self.requests.lock().expect("runner mutex").clone()
        }
    }

    impl ProcessRunner for ScriptedRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.requests
                .lock()
                .expect("runner mutex")
                .push(request.clone());
            let mut results = self.results.lock().expect("runner mutex");
            if results.is_empty() {
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            results.remove(0)
        }
    }

    fn recipe(
        commands: &[&str],
        check: Option<&str>,
        lockfiles: &[&str],
        inputs: &[&str],
    ) -> SetupSettings {
        SetupSettings {
            timeout: Duration::from_secs(30),
            commands: commands.iter().map(|value| (*value).to_owned()).collect(),
            check: check.map(str::to_owned),
            lockfiles: lockfiles.iter().map(|value| (*value).to_owned()).collect(),
            inputs: inputs.iter().map(|value| (*value).to_owned()).collect(),
        }
    }

    fn write_workspace(root: &Path, lock_contents: &[u8], script: Option<&str>) {
        fs::create_dir_all(root).unwrap();
        fs::write(root.join("Cargo.lock"), lock_contents).unwrap();
        if let Some(script) = script {
            fs::write(root.join("setup.sh"), script.as_bytes()).unwrap();
        }
    }

    fn task(n: u8) -> String {
        format!("{n:032x}")
    }

    const EMPTY_REQUIRES: &[String] = &[];

    fn request<'a>(
        account: &'a Path,
        project_id: &'a str,
        task_id: &'a str,
        workspace: &'a Path,
        cache: &'a Path,
        recipe: &'a SetupSettings,
        profile: &'a [(OsString, OsString)],
        cancel: &'a dyn Fn() -> bool,
    ) -> SetupRequest<'a> {
        request_until(
            account,
            project_id,
            task_id,
            workspace,
            cache,
            recipe,
            profile,
            Instant::now() + recipe.timeout,
            cancel,
        )
    }

    fn request_until<'a>(
        account: &'a Path,
        project_id: &'a str,
        task_id: &'a str,
        workspace: &'a Path,
        cache: &'a Path,
        recipe: &'a SetupSettings,
        profile: &'a [(OsString, OsString)],
        deadline: Instant,
        cancel: &'a dyn Fn() -> bool,
    ) -> SetupRequest<'a> {
        SetupRequest {
            account_home: account,
            project_id,
            task_id,
            env_profile_name: Some("agents"),
            workspace,
            cache_root: cache,
            requires: EMPTY_REQUIRES,
            recipe,
            profile_entries: profile,
            now_millis: 1,
            deadline,
            should_cancel: cancel,
        }
    }

    fn pid_alive(pid: libc::pid_t) -> bool {
        unsafe { libc::kill(pid, 0) == 0 }
    }

    fn wait_for_file(path: &Path, timeout: Duration) -> Vec<u8> {
        let started = Instant::now();
        loop {
            if let Ok(bytes) = fs::read(path)
                && !bytes.is_empty()
            {
                return bytes;
            }
            if started.elapsed() >= timeout {
                panic!("timed out waiting for {}", path.display());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn two_workspaces_with_identical_lockfiles_both_materialize_and_repair() {
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        let workspace_a = tempfile::tempdir().unwrap();
        let workspace_b = tempfile::tempdir().unwrap();
        write_workspace(workspace_a.path(), b"lock-v1", None);
        write_workspace(workspace_b.path(), b"lock-v1", None);
        let recipe = recipe(
            &["PATH=/bin:/usr/bin mkdir -p deps && printf ready > deps/ready"],
            Some("PATH=/bin:/usr/bin test -f deps/ready"),
            &["Cargo.lock"],
            &[],
        );
        let cancel = || false;
        let project_id = "ab".repeat(32);
        let task_a = task(1);
        let task_b = task(2);
        let runner = SystemProcessRunner;
        prepare_project_setup(
            &runner,
            request(
                account.path(),
                &project_id,
                &task_a,
                workspace_a.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap();
        prepare_project_setup(
            &runner,
            request(
                account.path(),
                &project_id,
                &task_b,
                workspace_b.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(workspace_a.path().join("deps/ready")).unwrap(),
            "ready"
        );
        assert_eq!(
            fs::read_to_string(workspace_b.path().join("deps/ready")).unwrap(),
            "ready"
        );

        fs::remove_file(workspace_a.path().join("deps/ready")).unwrap();
        prepare_project_setup(
            &runner,
            request(
                account.path(),
                &project_id,
                &task_a,
                workspace_a.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(workspace_a.path().join("deps/ready")).unwrap(),
            "ready"
        );
    }

    #[test]
    fn changed_input_script_does_not_skip_stable_command_string() {
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", Some("printf one > deps/ready"));
        fs::create_dir_all(workspace.path().join("deps")).unwrap();
        let recipe = recipe(
            &["./setup.sh"],
            Some("PATH=/bin:/usr/bin test -f deps/ready"),
            &["Cargo.lock"],
            &["setup.sh"],
        );
        let cancel = || false;
        let project_id = "cd".repeat(32);
        let task_id = task(3);
        let runner = SystemProcessRunner;
        fs::write(
            workspace.path().join("setup.sh"),
            b"#!/bin/sh\nPATH=/bin:/usr/bin\nmkdir -p deps\nprintf one > deps/ready\n",
        )
        .unwrap();
        fs::set_permissions(
            workspace.path().join("setup.sh"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        prepare_project_setup(
            &runner,
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(workspace.path().join("deps/ready")).unwrap(),
            "one"
        );
        fs::write(
            workspace.path().join("setup.sh"),
            b"#!/bin/sh\nPATH=/bin:/usr/bin\nmkdir -p deps\nprintf two > deps/ready\n",
        )
        .unwrap();
        fs::set_permissions(
            workspace.path().join("setup.sh"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        prepare_project_setup(
            &runner,
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(workspace.path().join("deps/ready")).unwrap(),
            "two"
        );
    }

    #[test]
    fn failed_and_cancelled_prep_do_not_publish_receipts() {
        let workspace = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", None);
        let recipe = recipe(&["cargo fetch --locked"], None, &["Cargo.lock"], &[]);
        let cancel = || false;
        let project_id = "ee".repeat(32);
        let task_id = task(4);
        let failed = prepare_project_setup(
            &ScriptedRunner::failing(),
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap_err();
        assert_eq!(failed.public_code(), "SETUP_FAILED");
        assert!(
            failed.to_string().contains("command 1/1"),
            "{}",
            failed.to_string()
        );

        let cancel = || true;
        let cancelled = prepare_project_setup(
            &ScriptedRunner::succeeding(),
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap_err();
        assert_eq!(cancelled.public_code(), "SETUP_CANCELLED");

        let cancel = || false;
        let timed_out = prepare_project_setup(
            &ScriptedRunner::timed_out(),
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                &cancel,
            ),
        )
        .unwrap_err();
        assert_eq!(timed_out.public_code(), "SETUP_TIMEOUT");
        let receipts: Vec<_> = fs::read_dir(setup_cache_dir(cache.path()))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().ends_with(".json"))
            .collect();
        assert!(receipts.is_empty(), "failed prep published {receipts:?}");
    }

    #[test]
    fn setup_uses_isolated_account_environment_and_omits_secrets_from_receipts() {
        let workspace = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", None);
        fs::write(workspace.path().join("ready"), b"poison").unwrap();
        let recipe = recipe(&["true"], None, &["Cargo.lock"], &[]);
        let cancel = || false;
        let project_id = "ff".repeat(32);
        let task_id = task(5);
        let profile = vec![(OsString::from("TOKEN"), OsString::from("profile-secret"))];
        let runner = ScriptedRunner::succeeding();
        prepare_project_setup(
            &runner,
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &profile,
                &cancel,
            ),
        )
        .unwrap();
        let launched = runner.take_requests().pop().expect("setup command");
        assert!(launched.isolate_parent_environment);
        assert!(
            launched
                .environment
                .iter()
                .any(|(name, value)| name == "HOME" && value == account.path().as_os_str())
        );
        let receipt_bytes =
            fs::read(setup_cache_dir(cache.path()).join(format!("task-{task_id}.json"))).unwrap();
        let receipt_text = String::from_utf8(receipt_bytes).unwrap();
        assert!(!receipt_text.contains("profile-secret"));
        assert!(!receipt_text.contains("TOKEN"));
        let receipt: SetupReceipt = serde_json::from_str(&receipt_text).unwrap();
        assert_eq!(receipt.task_id, task_id);

        let debug = format!(
            "{:?}",
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &profile,
                &cancel,
            )
        );
        assert!(!debug.contains("profile-secret"));
        assert!(debug.contains("TOKEN"));
        assert!(debug.contains("profile_entry_count"));
    }

    #[test]
    fn failed_setup_redacts_profile_secrets_from_diagnostics() {
        let workspace = tempfile::tempdir().unwrap();
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", None);
        let recipe = recipe(&["false"], None, &["Cargo.lock"], &[]);
        let cancel = || false;
        let project_id = "aa".repeat(32);
        let task_id = task(6);
        let profile = vec![(OsString::from("TOKEN"), OsString::from("profile-secret"))];
        let error = prepare_project_setup(
            &ScriptedRunner::failing_with(b"boom profile-secret leaked"),
            request(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &profile,
                &cancel,
            ),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "SETUP_FAILED");
        assert!(error.to_string().contains("command 1/1"));
        assert!(
            !error.to_string().contains("profile-secret"),
            "{}",
            error.to_string()
        );
    }

    #[test]
    fn setup_stderr_redacts_the_full_buffer_before_the_byte_tail() {
        let account = tempfile::tempdir().unwrap();
        let secret = "profile-secret-token";
        let mut stderr = vec![b'a'; 190];
        stderr.extend_from_slice(secret.as_bytes());
        stderr.extend(std::iter::repeat_n(b'b', 50));
        let recipe = recipe(&["true"], None, &[], &[]);
        let profile = [(OsString::from("TOKEN"), OsString::from(secret))];
        let cancel = || false;
        let request = SetupRequest {
            account_home: account.path(),
            project_id: "aa",
            task_id: "01",
            env_profile_name: None,
            workspace: account.path(),
            cache_root: account.path(),
            requires: &[],
            recipe: &recipe,
            profile_entries: &profile,
            now_millis: 1,
            deadline: Instant::now() + Duration::from_secs(1),
            should_cancel: &cancel,
        };
        let diagnostic = sanitized_stderr(&request, &stderr);
        assert!(
            !diagnostic.contains(secret),
            "secret crossed the tail boundary: {diagnostic}"
        );
        assert!(diagnostic.contains("[token]"), "{diagnostic}");
        assert!(diagnostic.as_bytes().len() <= DIAGNOSTIC_TAIL_BYTES);
    }

    #[test]
    fn setup_stage_result_round_trips_without_command_text() {
        let turn = tempfile::tempdir().unwrap();
        fs::set_permissions(turn.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let directory = RootedDir::open(turn.path()).unwrap();
        let error = setup_error("SETUP_FAILED", "setup command 1/1 failed");
        write_setup_stage_result(&directory, &error).unwrap();
        let loaded = load_setup_stage_result(&directory).unwrap().unwrap();
        assert_eq!(loaded.code, "SETUP_FAILED");
        assert_eq!(loaded.message, "setup command 1/1 failed");
        assert!(!loaded.message.contains("npm"));
    }

    #[test]
    fn bounded_workspace_input_rejects_overflow_without_unbounded_read() {
        let workspace = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join("big.lock"), vec![b'x'; 32]).unwrap();
        let error = read_bounded_nofollow(&workspace.path().join("big.lock"), 8).unwrap_err();
        assert_eq!(error.public_code(), "SETUP_FAILED");
        assert!(error.public_message().contains("bounded"));
    }

    #[test]
    fn cancel_after_child_starts_reaps_descendants_without_a_receipt() {
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", None);
        let mut recipe = recipe(
            &["printf $$ > leader.pid; /bin/sleep 30 & printf $! > child.pid; wait"],
            None,
            &["Cargo.lock"],
            &[],
        );
        recipe.timeout = Duration::from_secs(10);
        let project_id = "11".repeat(32);
        let task_id = task(7);
        let cancel = AtomicBool::new(false);
        let started = Instant::now();
        thread::scope(|scope| {
            let result = scope.spawn(|| {
                prepare_project_setup(
                    &SystemProcessRunner,
                    request(
                        account.path(),
                        &project_id,
                        &task_id,
                        workspace.path(),
                        cache.path(),
                        &recipe,
                        &[],
                        &|| cancel.load(Ordering::SeqCst),
                    ),
                )
            });
            let leader_bytes =
                wait_for_file(&workspace.path().join("leader.pid"), Duration::from_secs(2));
            let child_bytes =
                wait_for_file(&workspace.path().join("child.pid"), Duration::from_secs(2));
            let leader: libc::pid_t = String::from_utf8(leader_bytes)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            let child: libc::pid_t = String::from_utf8(child_bytes)
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            assert!(
                pid_alive(leader),
                "setup child must be running before cancel"
            );
            cancel.store(true, Ordering::SeqCst);
            let error = result.join().expect("setup thread").unwrap_err();
            assert_eq!(error.public_code(), "SETUP_CANCELLED");
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline && (pid_alive(leader) || pid_alive(child)) {
                thread::sleep(Duration::from_millis(20));
            }
            assert!(!pid_alive(leader), "setup leader was not reaped");
            assert!(!pid_alive(child), "setup descendant was not reaped");
        });
        assert!(started.elapsed() < Duration::from_secs(3));
        let receipts: Vec<_> = fs::read_dir(setup_cache_dir(cache.path()))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().ends_with(".json"))
            .collect();
        assert!(receipts.is_empty(), "cancelled prep published {receipts:?}");
    }

    #[test]
    fn multiple_commands_share_one_absolute_deadline() {
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", None);
        let mut recipe = recipe(
            &["/bin/sleep 0.3", "/bin/sleep 0.3"],
            None,
            &["Cargo.lock"],
            &[],
        );
        recipe.timeout = Duration::from_millis(400);
        let cancel = || false;
        let project_id = "22".repeat(32);
        let task_id = task(8);
        let started = Instant::now();
        let error = prepare_project_setup(
            &SystemProcessRunner,
            request_until(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                started + recipe.timeout,
                &cancel,
            ),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "SETUP_TIMEOUT");
        assert!(started.elapsed() < Duration::from_secs(2));
        let receipts: Vec<_> = fs::read_dir(setup_cache_dir(cache.path()))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| name.to_string_lossy().ends_with(".json"))
            .collect();
        assert!(receipts.is_empty(), "timed out prep published {receipts:?}");
    }

    #[test]
    fn occupied_setup_lock_times_out_instead_of_blocking() {
        let cache = tempfile::tempdir().unwrap();
        let account = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        write_workspace(workspace.path(), b"lock", None);
        let mut recipe = recipe(&["/bin/true"], None, &["Cargo.lock"], &[]);
        recipe.timeout = Duration::from_millis(300);
        let project_id = "33".repeat(32);
        let task_id = task(9);
        let cache_dir = open_setup_cache(cache.path()).unwrap();
        let held = exclusive_lock(
            &cache_dir,
            &task_id,
            Instant::now() + Duration::from_secs(5),
            &|| false,
        )
        .unwrap();
        let cancel = || false;
        let started = Instant::now();
        let error = prepare_project_setup(
            &SystemProcessRunner,
            request_until(
                account.path(),
                &project_id,
                &task_id,
                workspace.path(),
                cache.path(),
                &recipe,
                &[],
                started + recipe.timeout,
                &cancel,
            ),
        )
        .unwrap_err();
        drop(held);
        assert_eq!(error.public_code(), "SETUP_TIMEOUT");
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
