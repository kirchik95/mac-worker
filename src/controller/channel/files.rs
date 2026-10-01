//! Blocking private channel files; call only on bounded native control jobs.
//! Generation link withdrawal requires stopped admission and proven exit of all
//! generation socket RPC children. Unknown proof retains it. Detached task
//! groups have no link dependency and must never be awaited or cancelled for it.
use super::contracts::{
    ChannelFailure, ChannelReason, CleanupContext, ForwardDisposition, IDENTITY_BYTES,
    PinnedExecutable, RunningImage, ServiceIdentity, UuidString,
};
pub use super::contracts::{
    EntryIdentity, ForwardPath, ForwardPaths, ServiceRecord, SocketBinding,
};
use crate::{
    controller::{ControllerLeader, health_read::observe_leader},
    error::WorkerError,
    inputs::RelativePath,
    job::ProcessIdentity,
    paths::PathLayout,
    rooted_fs::{PrivateEntryIdentity, RootedDir},
    supervisor::ProcessObservation,
};
use std::{io, os::unix::net::UnixListener, path::Path};
mod core;

pub(crate) fn public_entry(value: PrivateEntryIdentity) -> EntryIdentity {
    EntryIdentity {
        device: value.device,
        inode: value.inode,
        owner: value.owner,
        kind: value.kind,
        mode: value.mode,
    }
}
pub(crate) fn private_entry(value: EntryIdentity) -> PrivateEntryIdentity {
    PrivateEntryIdentity {
        device: value.device,
        inode: value.inode,
        owner: value.owner,
        kind: value.kind,
        mode: value.mode,
    }
}
fn allocation(path: &ForwardPath) -> core::Allocation {
    core::Allocation {
        directory: path.directory.clone(),
        identity: private_entry(path.directory_identity),
        socket: path.socket_path.clone(),
    }
}
fn unsafe_path(_: io::Error) -> ChannelFailure {
    ChannelFailure::Unavailable(ChannelReason::UnsafePath)
}
fn invalid() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_CHANNEL: invalid private service evidence".into())
}

#[derive(Default)]
pub struct PrivateChannelFiles;
impl PrivateChannelFiles {
    pub fn new() -> Self {
        Self
    }
}
impl ForwardPaths for PrivateChannelFiles {
    fn allocate(&self, paths: &PathLayout) -> Result<ForwardPath, ChannelFailure> {
        let path = core::allocate(paths).map_err(unsafe_path)?;
        Ok(ForwardPath {
            directory: path.directory,
            directory_identity: public_entry(path.identity),
            socket_path: path.socket,
        })
    }
    fn validate_socket(&self, path: &ForwardPath) -> Result<EntryIdentity, ChannelFailure> {
        core::validate_socket(&allocation(path))
            .map(public_entry)
            .map_err(unsafe_path)
    }
    /// The producer must be settled and unable to create/listen again. This
    /// primitive cannot establish settlement of an unacknowledged forward open.
    fn cleanup_if_refused(
        &self,
        path: &ForwardPath,
        socket: Option<EntryIdentity>,
        ctx: &CleanupContext,
    ) -> ForwardDisposition {
        if core::cleanup_settled_refused(&allocation(path), socket.map(private_entry), &|| {
            ctx.check().is_ok()
        }) {
            ForwardDisposition::Cleaned
        } else {
            ForwardDisposition::Retained
        }
    }
}

pub struct LeaderSocketLease {
    files: core::GenerationFiles,
    leader: ProcessIdentity,
    controller_root: std::path::PathBuf,
}

/// Holds the leader lock. A prior generation has no RPC-exit proof at startup;
/// its exactly recorded image is preserved while a safely stale socket recovers.
pub fn bind_leader(
    paths: &PathLayout,
    leader: &ControllerLeader,
    image: &RunningImage,
    generation: &UuidString,
) -> Result<LeaderSocketLease, WorkerError> {
    if !matches!(
        observe_leader(&paths.controller_state_root(), leader.identity())?,
        ProcessObservation::Matching { .. }
    ) {
        return Err(invalid());
    }
    let parent = RootedDir::open_anchored_absolute(&paths.controller_state_root())?;
    parent.channel_private_root()?;
    let root =
        parent.open_child_directory(&RelativePath::parse(b"rpc").map_err(|_| invalid())?, true)?;
    root.channel_private_root()?;
    let files = if root.entry_exists("service.json")? {
        let (record, bytes, binding) = read_record(&root)?;
        let observation = observe_leader(&paths.controller_state_root(), record.service.leader)?;
        let stale = stale_evidence(&root, &record, bytes, binding)?;
        let prior = core::prepare_stale(
            &root,
            &stale,
            matches!(
                observation,
                ProcessObservation::Absent | ProcessObservation::Reused
            ),
            false,
        )?;
        core::bind_generation_replacing(
            root,
            &image.path,
            image.device,
            image.inode,
            generation.as_str(),
            prior,
        )?
    } else {
        core::bind_generation(
            root,
            &image.path,
            image.device,
            image.inode,
            generation.as_str(),
        )?
    };
    Ok(LeaderSocketLease {
        files,
        leader: leader.identity(),
        controller_root: parent.path().to_owned(),
    })
}

pub(crate) fn read_record(
    root: &RootedDir,
) -> Result<(ServiceRecord, Vec<u8>, PrivateEntryIdentity), WorkerError> {
    root.channel_private_root()?;
    let binding = root.private_entry_identity("service.json")?;
    let bytes = root.read_private_regular("service.json", IDENTITY_BYTES as u64)?;
    if root.private_entry_identity("service.json")? != binding {
        return Err(invalid());
    }
    let record: ServiceRecord =
        serde_json::from_value(super::identity::core::decode_unique(&bytes)?)
            .map_err(|_| invalid())?;
    record.validate().map_err(|_| invalid())?;
    validate_record_paths(root, &record)?;
    Ok((record, bytes, binding))
}
fn validate_record_paths(root: &RootedDir, record: &ServiceRecord) -> Result<(), WorkerError> {
    let socket = record.binding.socket;
    let image = record.executable.binding;
    let owner = unsafe { libc::geteuid() };
    if socket.owner != owner
        || socket.kind != libc::S_IFSOCK as u32
        || socket.mode != 0o600
        || socket.inode == 0
        || socket.device == 0
        || socket.device != record.binding.parent.device
        || image.owner != owner
        || image.kind != libc::S_IFREG as u32
        || image.mode & !0o7777 != 0
        || image.mode & 0o7022 != 0
        || image.mode & 0o100 == 0
        || image.inode == 0
        || image.device == 0
        || image.device != record.binding.parent.device
    {
        return Err(invalid());
    }
    let name = format!(
        "e{}",
        record.service.service_generation.as_str().replace('-', "")
    );
    if record.service.socket_path != root.path().join("s")
        || record.executable.path != root.path().join(name)
        || public_entry(root.identity()?) != record.binding.parent
    {
        return Err(invalid());
    }
    Ok(())
}
pub(crate) fn validate_live_bindings(
    root: &RootedDir,
    record: &ServiceRecord,
) -> Result<(), WorkerError> {
    validate_record_paths(root, record)?;
    if public_entry(root.channel_socket_entry("s")?) != record.binding.socket
        || public_entry(
            root.channel_executable_entry(
                record
                    .executable
                    .path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(invalid)?,
            )?,
        ) != record.executable.binding
    {
        return Err(invalid());
    }
    Ok(())
}
fn stale_evidence(
    root: &RootedDir,
    record: &ServiceRecord,
    bytes: Vec<u8>,
    binding: PrivateEntryIdentity,
) -> Result<core::StaleEvidence, WorkerError> {
    validate_record_paths(root, record)?;
    Ok(core::StaleEvidence {
        record: bytes,
        record_binding: binding,
        parent: private_entry(record.binding.parent),
        socket: private_entry(record.binding.socket),
        executable: record.executable.path.clone(),
        executable_binding: private_entry(record.executable.binding),
    })
}

/// Optional prior-generation cleanup on native control work. `true` is proof
/// that admission stopped and every prior socket RPC exited, never PID liveness
/// or detached-group completion. The safely stale record stays for replacement.
pub fn cleanup_prior_generation(paths: &PathLayout, rpc_exits_proven: bool) -> ForwardDisposition {
    let clean = || -> Result<(), WorkerError> {
        if !rpc_exits_proven {
            return Err(invalid());
        }
        let root = RootedDir::open_anchored_absolute(&paths.controller_state_root().join("rpc"))?;
        let (record, bytes, binding) = read_record(&root)?;
        let observation = observe_leader(&paths.controller_state_root(), record.service.leader)?;
        let stale = stale_evidence(&root, &record, bytes, binding)?;
        core::prepare_stale(
            &root,
            &stale,
            matches!(
                observation,
                ProcessObservation::Absent | ProcessObservation::Reused
            ),
            true,
        )?;
        Ok(())
    };
    if clean().is_ok() {
        ForwardDisposition::Cleaned
    } else {
        ForwardDisposition::Retained
    }
}
impl LeaderSocketLease {
    pub fn take_listener(&mut self) -> Option<UnixListener> {
        self.files.take_listener()
    }
    pub fn binding(&self) -> SocketBinding {
        SocketBinding {
            parent: public_entry(self.files.parent_binding),
            socket: public_entry(self.files.socket_binding),
        }
    }
    pub fn executable(&self) -> PinnedExecutable {
        PinnedExecutable {
            path: self.files.executable.clone(),
            binding: public_entry(self.files.executable_binding),
        }
    }
    pub fn detached_runner_executable(&self) -> &Path {
        &self.files.installed
    }
    /// Call only after the runtime has observed its listener-ready barrier.
    pub fn publish(&mut self, service: &ServiceIdentity) -> Result<(), WorkerError> {
        service.validate().map_err(|_| invalid())?;
        if service.leader != self.leader
            || !matches!(
                observe_leader(&self.controller_root, self.leader)?,
                ProcessObservation::Matching { .. }
            )
        {
            return Err(invalid());
        }
        let record = ServiceRecord {
            schema_version: 1,
            service: service.clone(),
            binding: self.binding(),
            executable: self.executable(),
        };
        record.validate().map_err(|_| invalid())?;
        self.files
            .publish(&serde_json::to_value(service).map_err(|_| invalid())?)?;
        Ok(())
    }
    /// Call after stopped admission/listener closure. `true` requires proven
    /// exit of every generation RPC child. Unknown proof preserves all evidence.
    pub fn withdraw(&mut self, rpc_exits_proven: bool) -> ForwardDisposition {
        if self.files.withdraw(rpc_exits_proven) {
            ForwardDisposition::Cleaned
        } else {
            ForwardDisposition::Retained
        }
    }
}
