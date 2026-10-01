//! T5 facade. Captured master, config-free control and positive cleanup evidence.
pub use super::contracts::{
    CleanupContext, ConfiguredRoute, ForwardControl, ForwardDisposition, ForwardLease,
    ForwardOpenFailure, ForwardPaths, MasterPlan,
};

use super::contracts::*;
use crate::{
    config::{SshConfig, valid_ssh_destination},
    error::{ProcessError, WorkerError},
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRunner},
    rooted_fs::RootedDir,
    transport,
};
use std::{
    ffi::CString,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
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
        let result = raw
            .run_interruptible(&request, &|| {
                check_client(ctx).is_err() || ctx.runtime.now() >= deadline
            })
            .map_err(process_failure)?;
        check_client(ctx)?;
        if ctx.runtime.now() >= deadline {
            return Err(unavailable(ChannelReason::Timeout));
        }
        if !result.status.success() {
            return Err(unavailable(ChannelReason::ForwardLost));
        }
        let endpoint = resolved_control_path(&result.stdout)?;
        if endpoint.parent() != Some(directory.as_path()) {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        if directory_binding(&parent)? != binding {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        match socket_binding(&parent, &endpoint) {
            Ok(_) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {}
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
        _: &dyn ProcessRunner,
        _: &MasterPlan,
        _: &SocketIdentity,
        _: &ClientContext<'_>,
    ) -> Result<Box<dyn ForwardLease>, ForwardOpenFailure> {
        let _ = (&self.paths, &self.files);
        Err(ForwardOpenFailure {
            failure: ChannelFailure::Unavailable(ChannelReason::Unsupported),
            disposition: ForwardDisposition::Cleaned,
        })
    }
}

fn unavailable(reason: ChannelReason) -> ChannelFailure {
    ChannelFailure::Unavailable(reason)
}

fn check_client(ctx: &ClientContext<'_>) -> Result<(), ChannelFailure> {
    let stopped = (ctx.should_stop)();
    if stopped || ctx.runtime.cancelled() {
        return Err(unavailable(ChannelReason::Cancelled));
    }
    if ctx.runtime.now() >= ctx.deadline {
        return Err(unavailable(ChannelReason::Timeout));
    }
    Ok(())
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
            && text.len() < 104
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
