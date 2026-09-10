use std::{
    convert::Infallible,
    ffi::OsString,
    fs::{self, File},
    os::unix::process::CommandExt,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    config::WorkerEntry,
    error::{ProcessError, WorkerError},
    host_store::{HostStore, HostStoreWritePoint},
    job::{ClientId, JobId, LeaseToken, RequestFingerprint},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
    rooted_fs::RootedDir,
    task::{BaseOid, BranchName, TaskId},
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
pub(crate) const GIT_FSYNC_COMPONENTS: &str = "objects,derived-metadata,reference";
pub(crate) const GIT_FSYNC_METHOD: &str = "fsync";
pub const OBJECT_STORE_SYNC_RECEIPT: &str = "mw-object-sync.json";
const OBJECT_STORE_RECEIPT_LIMIT: u64 = 1024 * 1024;
const ORIGIN_GIT_SSH_COMMAND: &str = "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes";
const ORIGIN_PREFLIGHT_OUTPUT_LIMIT: usize = 8 * 1024 * 1024;
const ORIGIN_PREFLIGHT_DEADLINE: Duration = Duration::from_secs(30);

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
            Some(ssh),
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

    /// Checks the advertised refs of a normalized origin for one exact
    /// object ID.  The remote output is treated as untrusted transport data:
    /// only the equality result is returned to the task layer.
    pub fn preflight_origin(&self, origin: &str, base: &BaseOid) -> Result<(), WorkerError> {
        let normalized = crate::project::normalize_origin(origin)
            .map_err(|_| git_error("BASE_NOT_ON_ORIGIN", "origin URL is invalid"))?;
        let result = self
            .runner
            .run(&origin_request(normalized))
            .map_err(|_| git_error("BASE_NOT_ON_ORIGIN", "origin did not advertise the base"))?;
        if !result.status.success() {
            return Err(git_error(
                "BASE_NOT_ON_ORIGIN",
                "origin did not advertise the base",
            ));
        }
        let advertised = result.stdout.split(|byte| *byte == b'\n').any(|line| {
            std::str::from_utf8(line)
                .ok()
                .and_then(|line| line.split_ascii_whitespace().next())
                == Some(base.as_str())
        });
        if advertised {
            Ok(())
        } else {
            Err(git_error(
                "BASE_NOT_ON_ORIGIN",
                "origin did not advertise the base",
            ))
        }
    }

    /// Fetches one exact origin object into a worker-owned bare mirror.  The
    /// caller supplies the mirror selected by the rooted host store; this
    /// operation never accepts a remote repository path or refspec.
    pub fn fetch_origin(
        &self,
        origin: &str,
        base: &BaseOid,
        mirror: &RootedDir,
    ) -> Result<(), WorkerError> {
        let normalized = crate::project::normalize_origin(origin)
            .map_err(|_| git_error("BASE_UNAVAILABLE", "origin URL is invalid"))?;
        let result = self
            .runner
            .run(&git_request(
                mirror.path(),
                Some(ORIGIN_GIT_SSH_COMMAND.to_owned()),
                vec![
                    OsString::from("fetch"),
                    OsString::from("--no-write-fetch-head"),
                    normalized.into(),
                    base.to_string().into(),
                ],
            ))
            .map_err(|_| git_error("BASE_UNAVAILABLE", "origin base fetch failed"))?;
        if result.status.success() {
            Ok(())
        } else {
            Err(git_error("BASE_UNAVAILABLE", "origin base fetch failed"))
        }
    }

    /// Publishes one exact object ID to a normalized origin branch.  The
    /// caller supplies the pinned OID; this never resolves a mutable task
    /// branch at retry time and never passes `--force`.
    pub fn push_origin(
        &self,
        origin: &str,
        oid: &BaseOid,
        branch: &BranchName,
        mirror: &RootedDir,
    ) -> Result<(), WorkerError> {
        let normalized = delivery_origin(origin)?;
        let destination = format!("{oid}:refs/heads/{branch}");
        let result = self
            .runner
            .run(&git_request(
                mirror.path(),
                Some(ORIGIN_GIT_SSH_COMMAND.to_owned()),
                vec![
                    OsString::from("push"),
                    OsString::from("--no-verify"),
                    normalized.into(),
                    destination.into(),
                ],
            ))
            .map_err(|_| git_error("PUBLISH_FAILED", "origin publication failed"))?;
        if result.status.success() {
            Ok(())
        } else {
            Err(git_error("PUBLISH_FAILED", "origin publication failed"))
        }
    }

    /// Reads the advertised OID of one origin branch.  Missing refs are
    /// `None`; transport failures stay `PUBLISH_FAILED` without remote text.
    pub fn advertise_origin_ref(
        &self,
        origin: &str,
        branch: &BranchName,
    ) -> Result<Option<BaseOid>, WorkerError> {
        let normalized = delivery_origin(origin)?;
        let wanted = format!("refs/heads/{branch}");
        let result = self
            .runner
            .run(&origin_ref_request(normalized, &wanted))
            .map_err(|_| git_error("PUBLISH_FAILED", "origin advertisement failed"))?;
        if !result.status.success() {
            return Err(git_error("PUBLISH_FAILED", "origin advertisement failed"));
        }
        let advertised = result.stdout.split(|byte| *byte == b'\n').find_map(|line| {
            let line = std::str::from_utf8(line).ok()?;
            let (oid, reference) = line.split_once('\t')?;
            (reference == wanted).then_some(oid.trim().parse().ok())?
        });
        Ok(advertised)
    }

    pub fn is_ancestor(
        &self,
        mirror: &RootedDir,
        ancestor: &BaseOid,
        descendant: &BaseOid,
    ) -> Result<bool, WorkerError> {
        let result = self
            .runner
            .run(&git_request(
                mirror.path(),
                None,
                vec![
                    OsString::from("merge-base"),
                    OsString::from("--is-ancestor"),
                    ancestor.to_string().into(),
                    descendant.to_string().into(),
                ],
            ))
            .map_err(|_| git_error("PUBLISH_FAILED", "ancestry check failed"))?;
        match result.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(git_error("PUBLISH_FAILED", "ancestry check failed")),
        }
    }

    /// True when the delivery pin already names this OID. Does not pack or fsync.
    pub fn delivery_pin_matches(
        &self,
        mirror: &RootedDir,
        task_id: TaskId,
        turn_id: crate::task::TurnId,
        oid: &BaseOid,
    ) -> Result<bool, WorkerError> {
        let reference = delivery_pin_ref(task_id, turn_id);
        let current = self
            .runner
            .run(&git_request(
                mirror.path(),
                None,
                vec![
                    OsString::from("rev-parse"),
                    OsString::from("--verify"),
                    OsString::from("--quiet"),
                    reference.into(),
                ],
            ))
            .map_err(|_| git_error("REF_UPDATE_FAILED", "delivery pin could not be read"))?;
        if !current.status.success() {
            return Ok(false);
        }
        Ok(String::from_utf8_lossy(&current.stdout).trim() == oid.as_str())
    }

    pub fn list_delivery_pins(
        &self,
        mirror: &RootedDir,
        task_id: TaskId,
    ) -> Result<Vec<(crate::task::TurnId, BaseOid)>, WorkerError> {
        let prefix = format!("refs/mac-worker/delivery/{task_id}/");
        let listed = self
            .runner
            .run(&git_request(
                mirror.path(),
                None,
                vec![
                    OsString::from("for-each-ref"),
                    OsString::from("--format=%(objectname)%09%(refname)"),
                    prefix.clone().into(),
                ],
            ))
            .map_err(|_| git_error("REF_UPDATE_FAILED", "delivery pins could not be listed"))?;
        if !listed.status.success() {
            return Err(git_error(
                "REF_UPDATE_FAILED",
                "delivery pins could not be listed",
            ));
        }
        let mut pins = Vec::new();
        for line in String::from_utf8_lossy(&listed.stdout).lines() {
            let Some((oid, reference)) = line.split_once('\t') else {
                continue;
            };
            let Some(turn) = reference.strip_prefix(&prefix) else {
                continue;
            };
            let Ok(turn_id) = turn.parse() else {
                continue;
            };
            let Ok(oid) = oid.parse() else {
                continue;
            };
            pins.push((turn_id, oid));
        }
        Ok(pins)
    }

    /// Pins `refs/mac-worker/delivery/<task>/<turn>` to an existing commit.
    /// Create-or-same-OID only. A one-time pack-content baseline/receipt makes
    /// pre-policy packs durable; later pins only sync new writes. Still-loose
    /// objects are packed and fsynced; already packed history is reused.
    /// Same-OID retry does not enumerate or rebuild history. Then `update-ref`
    /// and fsync of the ref. Syncing only `objects/<tip>` is not the guarantee.
    pub fn pin_delivery_ref(
        &self,
        store: &HostStore,
        mirror: &RootedDir,
        task_id: TaskId,
        turn_id: crate::task::TurnId,
        oid: &BaseOid,
    ) -> Result<(), WorkerError> {
        let reference = delivery_pin_ref(task_id, turn_id);
        let kind = self
            .runner
            .run(&git_request(
                mirror.path(),
                None,
                vec![
                    OsString::from("cat-file"),
                    OsString::from("-t"),
                    oid.to_string().into(),
                ],
            ))
            .map_err(|_| git_error("PUBLISH_FAILED", "delivery object is missing"))?;
        if !kind.status.success() || String::from_utf8_lossy(&kind.stdout).trim() != "commit" {
            return Err(git_error(
                "PUBLISH_FAILED",
                "delivery object is not a commit",
            ));
        }
        let current = self
            .runner
            .run(&git_request(
                mirror.path(),
                None,
                vec![
                    OsString::from("rev-parse"),
                    OsString::from("--verify"),
                    OsString::from("--quiet"),
                    reference.clone().into(),
                ],
            ))
            .map_err(|_| git_error("REF_UPDATE_FAILED", "delivery pin could not be read"))?;
        if current.status.success() {
            let existing = String::from_utf8_lossy(&current.stdout).trim().to_owned();
            if existing != oid.as_str() {
                return Err(git_error(
                    "DELIVERY_REF_CONFLICT",
                    "delivery pin already points at a different object",
                ));
            }
            ensure_object_store_durable(store, mirror)?;
            return fsync_delivery_ref(mirror, task_id, turn_id);
        }
        ensure_object_store_durable(store, mirror)?;
        let zero = "0000000000000000000000000000000000000000";
        let updated = self
            .runner
            .run(&git_request(
                mirror.path(),
                None,
                vec![
                    OsString::from("update-ref"),
                    reference.into(),
                    oid.to_string().into(),
                    OsString::from(zero),
                ],
            ))
            .map_err(|_| git_error("REF_UPDATE_FAILED", "delivery pin could not be written"))?;
        if !updated.status.success() {
            return Err(git_error(
                "REF_UPDATE_FAILED",
                "delivery pin could not be written",
            ));
        }
        fsync_delivery_ref(mirror, task_id, turn_id)
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
            Some(ssh),
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
        let head = read_ref_head(self.runner, transfer_repo, &local_ref)?;
        Ok(ImportReceipt::new(head, local_ref))
    }
}

pub fn delivery_pin_ref(task_id: TaskId, turn_id: crate::task::TurnId) -> String {
    format!("refs/mac-worker/delivery/{task_id}/{turn_id}")
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

fn git_request(
    transfer_repo: &Path,
    ssh: Option<String>,
    mut operation: Vec<OsString>,
) -> ProcessRequest {
    let mut args = vec![
        OsString::from("-C"),
        transfer_repo.as_os_str().to_os_string(),
    ];
    args.extend([
        OsString::from("-c"),
        OsString::from("gc.auto=0"),
        OsString::from("-c"),
        OsString::from(format!("core.fsync={GIT_FSYNC_COMPONENTS}")),
        OsString::from("-c"),
        OsString::from(format!("core.fsyncMethod={GIT_FSYNC_METHOD}")),
    ]);
    args.append(&mut operation);
    let mut environment = vec![
        (GIT_CONFIG_GLOBAL.into(), "/dev/null".into()),
        (GIT_CONFIG_NOSYSTEM.into(), "1".into()),
        (GIT_TERMINAL_PROMPT.into(), "0".into()),
    ];
    if let Some(ssh) = ssh {
        environment.insert(0, ("GIT_SSH_COMMAND".into(), ssh.into()));
    }
    ProcessRequest {
        program: GIT_PROGRAM.into(),
        args,
        environment,
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
        isolate_parent_environment: false,
    }
}

fn origin_request(origin: String) -> ProcessRequest {
    ProcessRequest {
        program: GIT_PROGRAM.into(),
        args: vec![OsString::from("ls-remote"), origin.into()],
        environment: vec![
            ("GIT_SSH_COMMAND".into(), ORIGIN_GIT_SSH_COMMAND.into()),
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
            "GIT_CEILING_DIRECTORIES".into(),
            "GIT_CONFIG_COUNT".into(),
            "GIT_CONFIG_PARAMETERS".into(),
        ],
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: ORIGIN_PREFLIGHT_OUTPUT_LIMIT,
            stderr_limit: GIT_OUTPUT_LIMIT,
            deadline: ORIGIN_PREFLIGHT_DEADLINE,
        },
        isolate_parent_environment: false,
    }
}

fn origin_ref_request(origin: String, reference: &str) -> ProcessRequest {
    let mut request = origin_request(origin);
    request.args.push(OsString::from(reference));
    request
}

fn delivery_origin(origin: &str) -> Result<String, WorkerError> {
    if let Ok(url) = url::Url::parse(origin)
        && url.scheme() == "file"
    {
        if url.query().is_some() || url.fragment().is_some() {
            return Err(git_error("PUBLISH_FAILED", "origin URL is invalid"));
        }
        let path = url
            .to_file_path()
            .map_err(|_| git_error("PUBLISH_FAILED", "origin URL is invalid"))?;
        if !path.is_absolute() {
            return Err(git_error("PUBLISH_FAILED", "origin URL is invalid"));
        }
        return Ok(url.to_string());
    }
    crate::project::normalize_origin(origin)
        .map_err(|_| git_error("PUBLISH_FAILED", "origin URL is invalid"))
}

fn ensure_object_store_durable(store: &HostStore, mirror: &RootedDir) -> Result<(), WorkerError> {
    let current = pack_store_files(mirror)?;
    let loaded = load_object_store_receipt(mirror)?;
    let (known, previous_bytes, valid) = match &loaded {
        Some((receipt, bytes))
            if receipt.fsync == GIT_FSYNC_COMPONENTS
                && receipt.fsync_method == GIT_FSYNC_METHOD =>
        {
            (receipt.packs.clone(), Some(bytes.clone()), true)
        }
        Some((_, bytes)) => (Vec::new(), Some(bytes.clone()), false),
        None => (Vec::new(), None, false),
    };
    for name in &current {
        if valid && known.contains(name) {
            continue;
        }
        fsync_required(mirror.path().join("objects/pack").join(name))?;
    }
    if !valid {
        fsync_loose_object_store(mirror)?;
    }
    if !valid && store.consume_fault(HostStoreWritePoint::AfterOutboxObjectBaselinePacks) {
        return Err(git_error(
            "REF_UPDATE_FAILED",
            "injected object-store pack sync failure",
        ));
    }
    fsync_object_store(mirror)?;
    if valid && current == known {
        return Ok(());
    }
    let receipt = ObjectStoreReceipt {
        fsync: GIT_FSYNC_COMPONENTS.to_owned(),
        fsync_method: GIT_FSYNC_METHOD.to_owned(),
        packs: current,
    };
    publish_object_store_receipt(mirror, previous_bytes.as_deref(), &receipt)?;
    if previous_bytes.is_none()
        && store.consume_fault(HostStoreWritePoint::AfterOutboxObjectBaseline)
    {
        return Err(git_error(
            "REF_UPDATE_FAILED",
            "injected object-store baseline receipt failure",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ObjectStoreReceipt {
    fsync: String,
    fsync_method: String,
    packs: Vec<String>,
}

fn pack_store_files(mirror: &RootedDir) -> Result<Vec<String>, WorkerError> {
    let pack = mirror.path().join("objects/pack");
    if !pack.is_dir() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    let entries = fs::read_dir(&pack).map_err(|error| {
        git_error(
            "REF_UPDATE_FAILED",
            format!("object pack directory could not be listed: {error}"),
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            git_error(
                "REF_UPDATE_FAILED",
                format!("object pack directory could not be listed: {error}"),
            )
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.ends_with(".pack") && !name.ends_with(".idx") {
            continue;
        }
        let metadata = entry.metadata().map_err(|error| {
            git_error(
                "REF_UPDATE_FAILED",
                format!("object pack entry could not be read: {error}"),
            )
        })?;
        if !metadata.is_file() {
            continue;
        }
        names.push(name.to_owned());
    }
    names.sort();
    Ok(names)
}

fn load_object_store_receipt(
    mirror: &RootedDir,
) -> Result<Option<(ObjectStoreReceipt, Vec<u8>)>, WorkerError> {
    if !mirror
        .entry_exists(OBJECT_STORE_SYNC_RECEIPT)
        .map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?
    {
        return Ok(None);
    }
    let bytes = mirror
        .read_private_regular(OBJECT_STORE_SYNC_RECEIPT, OBJECT_STORE_RECEIPT_LIMIT)
        .map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?;
    match serde_json::from_slice::<ObjectStoreReceipt>(&bytes) {
        Ok(receipt) => Ok(Some((receipt, bytes))),
        Err(_) => Ok(Some((
            ObjectStoreReceipt {
                fsync: String::new(),
                fsync_method: String::new(),
                packs: Vec::new(),
            },
            bytes,
        ))),
    }
}

fn publish_object_store_receipt(
    mirror: &RootedDir,
    previous: Option<&[u8]>,
    receipt: &ObjectStoreReceipt,
) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(receipt).map_err(|error| {
        git_error(
            "REF_UPDATE_FAILED",
            format!("object-store receipt could not be encoded: {error}"),
        )
    })?;
    if bytes.len() as u64 > OBJECT_STORE_RECEIPT_LIMIT {
        return Err(git_error(
            "REF_UPDATE_FAILED",
            "object-store receipt exceeds its size limit",
        ));
    }
    let result = if let Some(previous) = previous {
        mirror.rewrite_private_regular_exact(OBJECT_STORE_SYNC_RECEIPT, previous, &bytes)
    } else {
        mirror.write_private_atomic_no_replace(OBJECT_STORE_SYNC_RECEIPT, &bytes)
    };
    result.map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))
}

fn fsync_loose_object_store(mirror: &RootedDir) -> Result<(), WorkerError> {
    let objects = mirror.path().join("objects");
    if !objects.is_dir() {
        return Ok(());
    }
    let entries = fs::read_dir(&objects).map_err(|error| {
        git_error(
            "REF_UPDATE_FAILED",
            format!("object store could not be listed: {error}"),
        )
    })?;
    let mut fanouts = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            git_error(
                "REF_UPDATE_FAILED",
                format!("object store could not be listed: {error}"),
            )
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name == "pack" || name == "info" {
            continue;
        }
        if name.len() != 2 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            continue;
        }
        let fanout = objects.join(name);
        if !fanout.is_dir() {
            continue;
        }
        let objects_in_fanout = fs::read_dir(&fanout).map_err(|error| {
            git_error(
                "REF_UPDATE_FAILED",
                format!("loose object directory could not be listed: {error}"),
            )
        })?;
        for object in objects_in_fanout {
            let object = object.map_err(|error| {
                git_error(
                    "REF_UPDATE_FAILED",
                    format!("loose object directory could not be listed: {error}"),
                )
            })?;
            let path = object.path();
            let metadata = object.metadata().map_err(|error| {
                git_error(
                    "REF_UPDATE_FAILED",
                    format!("loose object could not be read: {error}"),
                )
            })?;
            if metadata.is_file() {
                fsync_required(&path)?;
            }
        }
        fanouts.push(fanout);
    }
    for fanout in fanouts {
        fsync_required(fanout)?;
    }
    Ok(())
}

fn fsync_object_store(mirror: &RootedDir) -> Result<(), WorkerError> {
    let objects = mirror.path().join("objects");
    let pack = objects.join("pack");
    if pack.is_dir() {
        fsync_required(&pack)?;
    }
    if objects.is_dir() {
        fsync_required(&objects)?;
    }
    Ok(())
}

fn fsync_delivery_ref(
    mirror: &RootedDir,
    task_id: TaskId,
    turn_id: crate::task::TurnId,
) -> Result<(), WorkerError> {
    let mut refs = mirror.path().join("refs/mac-worker/delivery");
    fsync_required(&refs)?;
    refs.push(task_id.to_string());
    fsync_required(&refs)?;
    refs.push(turn_id.to_string());
    fsync_required(&refs)
}

fn fsync_required(path: impl AsRef<Path>) -> Result<(), WorkerError> {
    let path = path.as_ref();
    let file =
        File::open(path).map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?;
    file.sync_all()
        .map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(|error| git_error("REF_UPDATE_FAILED", error.to_string()))?;
    }
    Ok(())
}

fn read_ref_head(
    runner: &dyn ProcessRunner,
    transfer_repo: &Path,
    local_ref: &str,
) -> Result<BaseOid, WorkerError> {
    let output = runner
        .run(&git_request(
            transfer_repo,
            None,
            vec![
                OsString::from("rev-parse"),
                OsString::from("--verify"),
                local_ref.into(),
            ],
        ))
        .map_err(map_fetch_failure)?;
    if !output.status.success() {
        return Err(git_error(
            "RESULT_FETCH_FAILED",
            "fetched result ref is missing or invalid",
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| git_error("RESULT_FETCH_FAILED", "fetched result head is invalid"))?
        .trim()
        .parse()
        .map_err(|_| git_error("RESULT_FETCH_FAILED", "fetched result head is invalid"))
}

fn git_ref_exists(mirror: &RootedDir, reference: &str) -> Result<bool, WorkerError> {
    mirror.verify_descriptors_cloexec()?;
    let directory_fd = mirror.raw_directory_fd();
    let mut command = Command::new(GIT_PROGRAM);
    command
        .args([
            "--git-dir",
            ".",
            "show-ref",
            "--verify",
            "--quiet",
            reference,
        ])
        .env(GIT_CONFIG_GLOBAL, "/dev/null")
        .env(GIT_CONFIG_NOSYSTEM, "1");
    unsafe {
        command.pre_exec(move || {
            if libc::fchdir(directory_fd) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let output = command.output()?;
    mirror.verify_bound()?;
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
    WorkerError::task(
        "TASK_NOT_FOUND",
        "task metadata or published branch is absent",
    )
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
