//! T5 facade. Captured master, config-free control and positive cleanup evidence.
use super::contracts::{
    CleanupContext, ConfiguredRoute, ForwardControl, ForwardDisposition, ForwardLease,
    ForwardOpenFailure, ForwardPaths, MasterPlan,
};

use super::contracts::*;
use crate::{
    config::{SshConfig, valid_ssh_destination},
    error::{ProcessError, WorkerError},
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    rooted_fs::RootedDir,
    transport,
};
use std::{
    ffi::CString,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub struct MasterForwardControl {
    paths: PathLayout,
    files: Arc<dyn ForwardPaths>,
    ssh: SshConfig,
}
impl MasterForwardControl {
    pub fn new(paths: PathLayout, files: Arc<dyn ForwardPaths>, ssh: SshConfig) -> Self {
        Self { paths, files, ssh }
    }
}
impl ForwardControl for MasterForwardControl {
    fn resolve(
        &self,
        raw: &dyn ProcessRunner,
        route: &ConfiguredRoute,
        ctx: &ClientContext<'_>,
    ) -> Result<MasterPlan, ChannelFailure> {
        check_client(ctx)?;
        if !self.ssh.multiplex {
            return Err(unavailable(ChannelReason::Unsupported));
        }
        if route.ssh_config_file != self.ssh.config_file
            || !valid_ssh_destination(&route.ssh)
            || route.remote_binary != "~/.local/bin/worker"
        {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        let directory = transport::channel_control_directory(&self.ssh)
            .ok_or_else(|| unavailable(ChannelReason::UnsafePath))?;
        // No adoption, chmod or namespace scan. Existing unsafe directories are
        // preserved; creation uses the existing descriptor-rooted helpers.
        let parent = RootedDir::open_or_create_anchored_absolute(&directory)
            .map_err(|_| unavailable(ChannelReason::UnsafePath))?;
        let binding = directory_binding(&parent)?;
        let deadline = ctx
            .deadline
            .min(ctx.runtime.now().saturating_add(SETUP_GUARD));
        let request = transport::channel_resolution_request(
            &self.ssh,
            &route.ssh,
            &directory,
            policy(deadline.saturating_sub(ctx.runtime.now())),
        )
        .map_err(process_failure)?;
        let result = run_control(raw, request, ctx, deadline)?;
        let endpoint = resolved_control_path(&result.stdout)?;
        if endpoint.parent() != Some(directory.as_path()) {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        if directory_binding(&parent)? != binding {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        match socket_binding(&parent, &endpoint) {
            Ok(_) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                // OpenSSH creates <endpoint>.<16 random hex> before publishing
                // a cold master. Keep the existing namespace unchanged.
                if endpoint
                    .as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .saturating_add(17)
                    >= SOCKET_PATH_BYTES
                {
                    return Err(unavailable(ChannelReason::UnsafePath));
                }
            }
            Err(_) => return Err(unavailable(ChannelReason::UnsafePath)),
        }
        let bootstrap_request = transport::channel_bootstrap_request(
            &self.ssh,
            &route.ssh,
            &endpoint,
            policy(
                ctx.deadline
                    .saturating_sub(ctx.runtime.now())
                    .min(SETUP_GUARD),
            ),
        )
        .map_err(process_failure)?;
        check_client(ctx)?;
        Ok(MasterPlan {
            control_path: endpoint,
            parent: binding,
            bootstrap_request,
        })
    }
    fn open(
        &self,
        raw: &dyn ProcessRunner,
        master: &MasterPlan,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn ForwardLease>, ForwardOpenFailure> {
        let clean_failure = |failure| ForwardOpenFailure {
            failure,
            disposition: ForwardDisposition::Cleaned,
        };
        check_client(ctx).map_err(clean_failure)?;
        if !self.ssh.multiplex {
            return Err(clean_failure(unavailable(ChannelReason::Unsupported)));
        }
        if !safe_socket_path(&identity.service.socket_path)
            || master.control_path.parent()
                != transport::channel_control_directory(&self.ssh).as_deref()
        {
            return Err(clean_failure(unavailable(ChannelReason::UnsafePath)));
        }
        let endpoint = MasterEndpoint::capture(master).map_err(clean_failure)?;
        let deadline = ctx
            .deadline
            .min(ctx.runtime.now().saturating_add(SETUP_GUARD));
        let check = transport::channel_control_request(
            master,
            "check",
            None,
            policy(deadline.saturating_sub(ctx.runtime.now())),
        )
        .map_err(|error| clean_failure(process_failure(error)))?;
        run_control(raw, check, ctx, deadline).map_err(clean_failure)?;
        endpoint.verify().map_err(clean_failure)?;
        check_client(ctx).map_err(clean_failure)?;
        if ctx.runtime.now() >= deadline {
            return Err(clean_failure(unavailable(ChannelReason::Timeout)));
        }
        let path = self.files.allocate(&self.paths).map_err(clean_failure)?;
        let retained = ForwardOpenFailure::retained;
        validate_allocation(&path).map_err(retained)?;
        check_client(ctx).map_err(retained)?;
        if ctx.runtime.now() >= deadline {
            return Err(retained(unavailable(ChannelReason::Timeout)));
        }
        let pair = format!(
            "{}:{}",
            path.socket_path
                .to_str()
                .ok_or_else(|| retained(unavailable(ChannelReason::UnsafePath)))?,
            identity
                .service
                .socket_path
                .to_str()
                .ok_or_else(|| retained(unavailable(ChannelReason::UnsafePath)))?
        );
        let cancel =
            transport::channel_control_request(master, "cancel", Some(&pair), policy(SETUP_GUARD))
                .map_err(|error| retained(process_failure(error)))?;
        let open = transport::channel_control_request(
            master,
            "forward",
            Some(&pair),
            policy(deadline.saturating_sub(ctx.runtime.now())),
        )
        .map_err(|error| retained(process_failure(error)))?;
        if let Err(failure) = run_control(raw, open, ctx, deadline) {
            // Reaping local ssh, refusal, and even best-effort cancel success
            // cannot prove the shared master's open has settled.
            let cleanup = cleanup_context();
            cancel_owned(raw, &endpoint, &cancel, &cleanup);
            return Err(retained(failure));
        }
        // Only a successful terminal open establishes the producer precondition
        // for refusal cleanup. Validation failure still needs positive proof.
        let socket = self.files.validate_socket(&path);
        let validation = socket
            .and_then(|binding| validate_socket_identity(&binding))
            .and_then(|()| endpoint.verify())
            .and_then(|()| check_client(ctx));
        if let Err(failure) = validation {
            let cleanup = cleanup_context();
            cancel_owned(raw, &endpoint, &cancel, &cleanup);
            let disposition = self.files.cleanup_if_refused(&path, socket.ok(), &cleanup);
            return Err(ForwardOpenFailure {
                failure,
                disposition,
            });
        }
        Ok(Box::new(MasterForwardLease {
            files: self.files.clone(),
            path,
            socket: socket.map_err(retained)?,
            endpoint,
            cancel,
            disposition: None,
        }))
    }
}

struct MasterEndpoint {
    parent: RootedDir,
    parent_binding: EntryIdentity,
    path: PathBuf,
    socket: EntryIdentity,
}
impl MasterEndpoint {
    fn capture(master: &MasterPlan) -> Result<Self, ChannelFailure> {
        if !safe_socket_path(&master.control_path) {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        let parent = RootedDir::open_anchored_absolute(
            master
                .control_path
                .parent()
                .ok_or_else(|| unavailable(ChannelReason::UnsafePath))?,
        )
        .map_err(|_| unavailable(ChannelReason::UnsafePath))?;
        let parent_binding = directory_binding(&parent)?;
        if parent_binding != master.parent {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        let socket = socket_binding(&parent, &master.control_path)
            .map_err(|_| unavailable(ChannelReason::ForwardLost))?;
        Ok(Self {
            parent,
            parent_binding,
            path: master.control_path.clone(),
            socket,
        })
    }
    fn verify(&self) -> Result<(), ChannelFailure> {
        if directory_binding(&self.parent)? != self.parent_binding
            || socket_binding(&self.parent, &self.path)
                .map_err(|_| unavailable(ChannelReason::ForwardLost))?
                != self.socket
        {
            return Err(unavailable(ChannelReason::ForwardLost));
        }
        Ok(())
    }
}

struct MasterForwardLease {
    files: Arc<dyn ForwardPaths>,
    path: ForwardPath,
    socket: EntryIdentity,
    endpoint: MasterEndpoint,
    cancel: ProcessRequest,
    disposition: Option<ForwardDisposition>,
}
impl ForwardLease for MasterForwardLease {
    fn local_socket(&self) -> &Path {
        &self.path.socket_path
    }
    fn verify(&self) -> Result<(), ChannelFailure> {
        if self.disposition.is_some() {
            return Err(unavailable(ChannelReason::ForwardLost));
        }
        self.endpoint.verify()?;
        let socket = self.files.validate_socket(&self.path)?;
        validate_socket_identity(&socket)?;
        if socket != self.socket {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        Ok(())
    }
    fn cancel(&mut self, raw: &dyn ProcessRunner, ctx: &CleanupContext) -> ForwardDisposition {
        if let Some(disposition) = self.disposition {
            return disposition;
        }
        // The scoped client closes its stream first. No destructor guesses at
        // cleanup or alters the shared SSH master.
        let disposition = if cleanup_remaining(ctx).is_zero()
            || self.files.validate_socket(&self.path).ok().as_ref() != Some(&self.socket)
        {
            ForwardDisposition::Retained
        } else {
            cancel_owned(raw, &self.endpoint, &self.cancel, ctx);
            if cleanup_remaining(ctx).is_zero() {
                ForwardDisposition::Retained
            } else {
                self.files
                    .cleanup_if_refused(&self.path, Some(self.socket), ctx)
            }
        };
        self.disposition = Some(disposition);
        disposition
    }
}

fn validate_allocation(path: &ForwardPath) -> Result<(), ChannelFailure> {
    if !safe_socket_path(&path.socket_path)
        || path.socket_path.parent() != Some(path.directory.as_path())
    {
        return Err(unavailable(ChannelReason::UnsafePath));
    }
    let directory = RootedDir::open_anchored_absolute(&path.directory)
        .map_err(|_| unavailable(ChannelReason::UnsafePath))?;
    if directory_binding(&directory)? != path.directory_identity {
        return Err(unavailable(ChannelReason::UnsafePath));
    }
    Ok(())
}
fn validate_socket_identity(socket: &EntryIdentity) -> Result<(), ChannelFailure> {
    if socket.kind != libc::S_IFSOCK as u32
        || socket.mode != 0o600
        || socket.owner != unsafe { libc::geteuid() }
    {
        return Err(unavailable(ChannelReason::UnsafePath));
    }
    Ok(())
}
fn run_control(
    raw: &dyn ProcessRunner,
    mut request: ProcessRequest,
    ctx: &ClientContext<'_>,
    deadline: Duration,
) -> Result<ProcessResult, ChannelFailure> {
    check_client(ctx)?;
    if ctx.runtime.now() >= deadline {
        return Err(unavailable(ChannelReason::Timeout));
    }
    request.policy.deadline = deadline.saturating_sub(ctx.runtime.now()).min(SETUP_GUARD);
    let result = raw.run_interruptible(&request, &|| {
        check_client(ctx).is_err() || ctx.runtime.now() >= deadline
    });
    check_client(ctx)?;
    if ctx.runtime.now() >= deadline {
        return Err(unavailable(ChannelReason::Timeout));
    }
    let result = result.map_err(process_failure)?;
    if !result.status.success() {
        return Err(unavailable(ChannelReason::ForwardLost));
    }
    Ok(result)
}

struct CleanupClock(Instant);
impl ChannelRuntime for CleanupClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
    fn cancelled(&self) -> bool {
        false
    }
}
fn cleanup_context() -> CleanupContext {
    CleanupContext::new(Arc::new(CleanupClock(Instant::now())))
}
fn cleanup_remaining(ctx: &CleanupContext) -> Duration {
    ctx.remaining().min(SETUP_GUARD)
}
fn cancel_owned(
    raw: &dyn ProcessRunner,
    endpoint: &MasterEndpoint,
    captured: &ProcessRequest,
    ctx: &CleanupContext,
) {
    if cleanup_remaining(ctx).is_zero() || endpoint.verify().is_err() {
        return;
    }
    let deadline = ctx
        .deadline
        .min(ctx.runtime.now().saturating_add(SETUP_GUARD));
    let mut request = captured.clone();
    request.policy.deadline = deadline.saturating_sub(ctx.runtime.now());
    // Cleanup ignores foreground cancellation. Its result is never deletion
    // evidence, and its clock budget cannot admit application setup/read work.
    let _ = raw.run_interruptible(&request, &|| ctx.runtime.now() >= deadline);
}

fn unavailable(reason: ChannelReason) -> ChannelFailure {
    ChannelFailure::Unavailable(reason)
}

fn check_client(ctx: &ClientContext<'_>) -> Result<(), ChannelFailure> {
    ctx.check()
}

fn policy(remaining: Duration) -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 64 * 1024,
        stderr_limit: 256 * 1024,
        deadline: remaining.min(SETUP_GUARD),
    }
}

fn process_failure(error: WorkerError) -> ChannelFailure {
    unavailable(match error {
        WorkerError::Process(ProcessError::Cancelled) => ChannelReason::Cancelled,
        WorkerError::Process(ProcessError::DeadlineExceeded { .. }) => ChannelReason::Timeout,
        _ => ChannelReason::ForwardLost,
    })
}

fn safe_socket_path(path: &Path) -> bool {
    path.to_str().is_some_and(|text| {
        path.is_absolute()
            && !text.is_empty()
            && text.len() < SOCKET_PATH_BYTES
            && !text
                .bytes()
                .any(|byte| byte.is_ascii_control() || matches!(byte, b':' | b'%' | b'$'))
            && path
                .components()
                .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
    })
}

fn resolved_control_path(output: &[u8]) -> Result<PathBuf, ChannelFailure> {
    if output.len() > 64 * 1024 {
        return Err(unavailable(ChannelReason::UnsafePath));
    }
    let output = std::str::from_utf8(output).map_err(|_| unavailable(ChannelReason::UnsafePath))?;
    let mut paths = output
        .lines()
        .filter_map(|line| line.strip_prefix("controlpath "));
    let endpoint = PathBuf::from(
        paths
            .next()
            .ok_or_else(|| unavailable(ChannelReason::UnsafePath))?,
    );
    if paths.next().is_some() || !safe_socket_path(&endpoint) {
        return Err(unavailable(ChannelReason::UnsafePath));
    }
    Ok(endpoint)
}

fn directory_binding(directory: &RootedDir) -> Result<EntryIdentity, ChannelFailure> {
    let entry = directory
        .identity()
        .map_err(|_| unavailable(ChannelReason::UnsafePath))?;
    if entry.kind != libc::S_IFDIR as u32
        || entry.mode != 0o700
        || entry.owner != unsafe { libc::geteuid() }
    {
        return Err(unavailable(ChannelReason::UnsafePath));
    }
    Ok(EntryIdentity {
        device: entry.device,
        inode: entry.inode,
        owner: entry.owner,
        kind: entry.kind,
        mode: entry.mode,
    })
}

fn socket_binding(parent: &RootedDir, path: &Path) -> std::io::Result<EntryIdentity> {
    parent.identity()?;
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let name = CString::new(name.as_encoded_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe {
        libc::fstatat(
            parent.raw_directory_fd(),
            name.as_ptr(),
            &mut metadata,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    let entry = EntryIdentity {
        device: metadata.st_dev as u64,
        inode: metadata.st_ino,
        owner: metadata.st_uid,
        kind: (metadata.st_mode & libc::S_IFMT) as u32,
        mode: (metadata.st_mode & 0o7777) as u32,
    };
    if entry.kind != libc::S_IFSOCK as u32
        || entry.mode != 0o600
        || entry.owner != unsafe { libc::geteuid() }
    {
        return Err(std::io::Error::from_raw_os_error(libc::EACCES));
    }
    parent.identity()?;
    Ok(entry)
}
