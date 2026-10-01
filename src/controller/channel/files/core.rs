//! Blocking private-channel allocation and evidence-based lifecycle primitives.
use crate::{
    inputs::RelativePath,
    paths::PathLayout,
    rooted_fs::{PrivateEntryIdentity, RootedDir},
};
use std::{
    io,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::net::UnixListener,
    },
    path::{Path, PathBuf},
};

#[derive(Clone, Debug)]
pub(crate) struct Allocation {
    pub(crate) directory: PathBuf,
    pub(crate) identity: PrivateEntryIdentity,
    pub(crate) socket: PathBuf,
}

fn invalid() -> io::Error {
    io::Error::from_raw_os_error(libc::EINVAL)
}

pub(crate) fn validate_socket_path(path: &Path) -> io::Result<()> {
    let text = path.to_str().ok_or_else(invalid)?;
    if !path.is_absolute()
        || text.is_empty()
        || text.len() >= 104
        || text
            .chars()
            .any(|c| c.is_control() || matches!(c, ':' | '%' | '$'))
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(invalid());
    }
    Ok(())
}

fn private_root(root: &RootedDir) -> io::Result<()> {
    let metadata = root.root_metadata()?;
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o7777 != 0o700 {
        return Err(io::Error::from_raw_os_error(libc::EACCES));
    }
    Ok(())
}

pub(crate) fn allocate(paths: &PathLayout) -> io::Result<Allocation> {
    let name = format!("c{}", &uuid::Uuid::new_v4().simple().to_string()[..16]);
    let candidate = paths
        .controller_cache_root()
        .join("channel")
        .join(&name)
        .join("s");
    validate_socket_path(&candidate)?;
    let cache = RootedDir::open_or_create_anchored_absolute(&paths.controller_cache_root())?;
    private_root(&cache)?;
    let channel = cache.open_child_directory(
        &RelativePath::parse(b"channel").map_err(|_| invalid())?,
        true,
    )?;
    private_root(&channel)?;
    validate_socket_path(&channel.path().join(&name).join("s"))?;
    let directory = channel.create_new_child_directory(&name)?;
    Ok(Allocation {
        directory: directory.path().to_owned(),
        identity: directory.identity()?,
        socket: directory.path().join("s"),
    })
}

fn open_allocation(path: &Allocation) -> io::Result<RootedDir> {
    validate_socket_path(&path.socket)?;
    if path.socket != path.directory.join("s") {
        return Err(invalid());
    }
    let root = RootedDir::open_anchored_absolute(&path.directory)?;
    private_root(&root)?;
    if root.identity()? != path.identity {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    Ok(root)
}

pub(crate) fn validate_socket(path: &Allocation) -> io::Result<PrivateEntryIdentity> {
    open_allocation(path)?.channel_socket_entry("s")
}

/// A single nonblocking local connect. Only ECONNREFUSED is positive evidence;
/// success, pending, missing, timeout and every other error are inconclusive.
pub(crate) fn probe_refused(path: &Path) -> io::Result<bool> {
    validate_socket_path(path)?;
    let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    let socket = unsafe { OwnedFd::from_raw_fd(raw) };
    if unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0
        || unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let bytes = path.to_str().ok_or_else(invalid)?.as_bytes();
    for (target, source) in address.sun_path.iter_mut().zip(bytes.iter()) {
        *target = *source as libc::c_char;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = length as u8;
    }
    let result = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            (&address as *const libc::sockaddr_un).cast(),
            length as libc::socklen_t,
        )
    };
    Ok(result < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::ECONNREFUSED))
}
/// Precondition: the producer has settled and can no longer bind/listen.
pub(crate) fn cleanup_settled_refused(
    path: &Allocation,
    expected: Option<PrivateEntryIdentity>,
    allowed: &dyn Fn() -> bool,
) -> bool {
    cleanup_with_hook(path, expected, allowed, || {})
}
fn cleanup_with_hook(
    path: &Allocation,
    expected: Option<PrivateEntryIdentity>,
    allowed: &dyn Fn() -> bool,
    after_probe: impl FnOnce(),
) -> bool {
    let clean = || -> io::Result<()> {
        if !allowed() {
            return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
        }
        let expected = expected.ok_or_else(invalid)?;
        let root = open_allocation(path)?;
        if root.channel_socket_entry("s")? != expected || root.list_names()? != [b"s".to_vec()] {
            return Err(invalid());
        }
        if !probe_refused(&path.socket)? {
            return Err(invalid());
        }
        after_probe();
        if !allowed() || open_allocation(path)?.channel_socket_entry("s")? != expected {
            return Err(invalid());
        }
        root.channel_unlink_exact("s", expected)?;
        root.channel_remove_empty()
    };
    clean().is_ok()
}

pub(crate) struct GenerationFiles {
    pub(crate) executable: PathBuf,
    pub(crate) installed: PathBuf,
    pub(crate) executable_binding: PrivateEntryIdentity,
    pub(crate) socket_binding: PrivateEntryIdentity,
    pub(crate) parent_binding: PrivateEntryIdentity,
    root: RootedDir,
    listener: Option<UnixListener>,
    generation: String,
    published: Option<(Vec<u8>, PrivateEntryIdentity)>,
    prior_record: Option<PriorRecord>,
}

pub(crate) struct PriorRecord {
    bytes: Vec<u8>,
    binding: PrivateEntryIdentity,
}

pub(crate) struct StaleEvidence {
    pub(crate) record: Vec<u8>,
    pub(crate) record_binding: PrivateEntryIdentity,
    pub(crate) parent: PrivateEntryIdentity,
    pub(crate) socket: PrivateEntryIdentity,
    pub(crate) executable: PathBuf,
    pub(crate) executable_binding: PrivateEntryIdentity,
}

/// Requires an already schema/identity-validated prior service record. The
/// caller observes its leader as absent/reused, never merely an absent PID.
pub(crate) fn prepare_stale(
    root: &RootedDir,
    evidence: &StaleEvidence,
    prior_leader_dead: bool,
    rpc_exits_proven: bool,
) -> io::Result<PriorRecord> {
    private_root(root)?;
    let verify_record = || -> io::Result<()> {
        if !prior_leader_dead
            || root.identity()? != evidence.parent
            || evidence.record.len() > 8192
            || root.private_entry_identity("service.json")? != evidence.record_binding
            || root.read_private_regular("service.json", 8192)? != evidence.record
            || root.private_entry_identity("service.json")? != evidence.record_binding
        {
            return Err(invalid());
        }
        Ok(())
    };
    verify_record()?;
    let mut decoder = serde_json::Deserializer::from_slice(&evidence.record);
    let record = crate::task::deserialize_unique_json(&mut decoder).map_err(|_| invalid())?;
    decoder.end().map_err(|_| invalid())?;
    let generation = record["service"]["service_generation"]
        .as_str()
        .ok_or_else(invalid)?;
    let uuid = uuid::Uuid::parse_str(generation).map_err(|_| invalid())?;
    let name = format!("e{}", uuid.simple());
    if uuid.is_nil()
        || uuid.get_version_num() != 4
        || uuid.get_variant() != uuid::Variant::RFC4122
        || uuid.hyphenated().to_string() != generation
        || record.as_object().is_none_or(|o| o.len() != 4)
        || record["schema_version"] != 1
        || record["binding"]
            != serde_json::json!({"parent":binding_json(evidence.parent),"socket":binding_json(evidence.socket)})
        || record["executable"]
            != serde_json::json!({"path":evidence.executable,"binding":binding_json(evidence.executable_binding)})
        || record["service"]["socket_path"].as_str() != root.path().join("s").to_str()
        || evidence.executable != root.path().join(&name)
    {
        return Err(invalid());
    }
    let image_present = root.entry_exists(&name)?;
    if image_present && root.channel_executable_entry(&name)? != evidence.executable_binding {
        return Err(invalid());
    }
    if root.entry_exists("s")? {
        if root.channel_socket_entry("s")? != evidence.socket
            || !probe_refused(&root.path().join("s"))?
        {
            return Err(invalid());
        }
        verify_record()?;
        root.channel_unlink_exact("s", evidence.socket)?;
    }
    verify_record()?;
    if rpc_exits_proven && image_present {
        root.channel_unlink_exact(&name, evidence.executable_binding)?;
    }
    verify_record()?;
    Ok(PriorRecord {
        bytes: evidence.record.clone(),
        binding: evidence.record_binding,
    })
}

pub(crate) fn bind_generation_replacing(
    root: RootedDir,
    installed: &Path,
    device: u64,
    inode: u64,
    generation: &str,
    prior: PriorRecord,
) -> io::Result<GenerationFiles> {
    bind_generation_inner(root, installed, device, inode, generation, Some(prior))
}

pub(crate) fn bind_generation(
    root: RootedDir,
    installed: &Path,
    device: u64,
    inode: u64,
    generation: &str,
) -> io::Result<GenerationFiles> {
    bind_generation_inner(root, installed, device, inode, generation, None)
}

fn bind_generation_inner(
    root: RootedDir,
    installed: &Path,
    device: u64,
    inode: u64,
    generation: &str,
    prior: Option<PriorRecord>,
) -> io::Result<GenerationFiles> {
    private_root(&root)?;
    validate_socket_path(&root.path().join("s"))?;
    let uuid = uuid::Uuid::parse_str(generation).map_err(|_| invalid())?;
    if uuid.is_nil()
        || uuid.get_version_num() != 4
        || uuid.get_variant() != uuid::Variant::RFC4122
        || uuid.hyphenated().to_string() != generation
        || !installed.is_absolute()
        || std::fs::canonicalize(installed)? != installed
        || root.entry_exists("s")?
    {
        return Err(invalid());
    }
    if let Some(record) = &prior {
        if root.private_entry_identity("service.json")? != record.binding
            || root.read_private_regular("service.json", 8192)? != record.bytes
            || root.private_entry_identity("service.json")? != record.binding
        {
            return Err(invalid());
        }
    } else if !root.list_names()?.is_empty() {
        // Without a valid prior record, even a bare generation link is a
        // creation gap. Preserve the private directory's residue and decline.
        return Err(invalid());
    }
    let name = format!("e{}", uuid.simple());
    let executable_binding = root.channel_link_executable(installed, device, inode, &name)?;
    let (listener, socket_binding) = root.channel_bind_socket("s")?;
    let parent_binding = root.identity()?;
    Ok(GenerationFiles {
        executable: root.path().join(name),
        installed: installed.to_owned(),
        executable_binding,
        socket_binding,
        parent_binding,
        root,
        listener: Some(listener),
        generation: generation.into(),
        published: None,
        prior_record: prior,
    })
}

fn binding_json(binding: PrivateEntryIdentity) -> serde_json::Value {
    serde_json::json!({"device":binding.device,"inode":binding.inode,"owner":binding.owner,"kind":binding.kind,"mode":binding.mode})
}

impl GenerationFiles {
    pub(crate) fn take_listener(&mut self) -> Option<UnixListener> {
        self.listener.take()
    }
    /// The caller has already observed the runtime's listener-ready barrier.
    pub(crate) fn publish(&mut self, service: &serde_json::Value) -> io::Result<()> {
        if service
            .get("socket_path")
            .and_then(serde_json::Value::as_str)
            != self.root.path().join("s").to_str()
            || service
                .get("service_generation")
                .and_then(serde_json::Value::as_str)
                != Some(self.generation.as_str())
            || self.root.identity()? != self.parent_binding
            || self.root.channel_socket_entry("s")? != self.socket_binding
            || self.root.channel_executable_entry(
                self.executable
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(invalid)?,
            )? != self.executable_binding
        {
            return Err(invalid());
        }
        let record = serde_json::json!({"schema_version":1,"service":service,"binding":{"parent":binding_json(self.parent_binding),"socket":binding_json(self.socket_binding)},"executable":{"path":self.executable,"binding":binding_json(self.executable_binding)}});
        let bytes = serde_json::to_vec(&record).map_err(|_| invalid())?;
        if bytes.len() > 8192 {
            return Err(invalid());
        }
        let binding = if let Some(prior) = &self.prior_record {
            self.root.channel_replace_private_exact(
                "service.json",
                prior.binding,
                &prior.bytes,
                &bytes,
            )?;
            self.root.private_entry_identity("service.json")?
        } else {
            self.root
                .write_private_atomic_no_replace_with_identity("service.json", |_| {
                    Ok(bytes.clone())
                })?
        };
        if self.root.private_entry_identity("service.json")? != binding
            || self.root.read_private_regular("service.json", 8192)? != bytes
        {
            return Err(invalid());
        }
        self.published = Some((bytes, binding));
        self.prior_record = None;
        Ok(())
    }
    /// Admission stopped; proof covers generation RPCs only, never detached tasks.
    pub(crate) fn withdraw(&mut self, rpc_exits_proven: bool) -> bool {
        drop(self.listener.take());
        if !rpc_exits_proven {
            return false;
        }
        let clean = || -> io::Result<()> {
            let verify_record = || -> io::Result<()> {
                if let Some((bytes, binding)) = &self.published
                    && (self.root.private_entry_identity("service.json")? != *binding
                        || self.root.read_private_regular("service.json", 8192)? != *bytes
                        || self.root.private_entry_identity("service.json")? != *binding)
                {
                    return Err(invalid());
                }
                Ok(())
            };
            verify_record()?;
            let executable_name = self
                .executable
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(invalid)?;
            if self.root.identity()? != self.parent_binding
                || self.root.channel_socket_entry("s")? != self.socket_binding
                || self.root.channel_executable_entry(executable_name)? != self.executable_binding
                || !probe_refused(&self.root.path().join("s"))?
            {
                return Err(invalid());
            }
            verify_record()?;
            if self.root.channel_executable_entry(executable_name)? != self.executable_binding {
                return Err(invalid());
            }
            self.root.channel_unlink_exact("s", self.socket_binding)?;
            verify_record()?;
            self.root
                .channel_unlink_exact(executable_name, self.executable_binding)?;
            if let Some((_, binding)) = &self.published {
                verify_record()?;
                self.root.channel_unlink_exact("service.json", *binding)?;
            }
            Ok(())
        };
        clean().is_ok()
    }
}
