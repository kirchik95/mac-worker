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
}

pub(crate) fn bind_generation(
    root: RootedDir,
    installed: &Path,
    device: u64,
    inode: u64,
    generation: &str,
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
        || root.entry_exists("service.json")?
        || root.entry_exists("s")?
    {
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
        let binding = self
            .root
            .write_private_atomic_no_replace_with_identity("service.json", |_| Ok(bytes.clone()))?;
        if self.root.private_entry_identity("service.json")? != binding
            || self.root.read_private_regular("service.json", 8192)? != bytes
        {
            return Err(invalid());
        }
        self.published = Some((bytes, binding));
        Ok(())
    }
    /// Admission stopped; proof covers generation RPCs only, never detached tasks.
    pub(crate) fn withdraw(&mut self, rpc_exits_proven: bool) -> bool {
        drop(self.listener.take());
        if !rpc_exits_proven {
            return false;
        }
        let clean = || -> io::Result<()> {
            if let Some((bytes, binding)) = &self.published {
                if self.root.private_entry_identity("service.json")? != *binding
                    || self.root.read_private_regular("service.json", 8192)? != *bytes
                {
                    return Err(invalid());
                }
            }
            if self.root.identity()? != self.parent_binding
                || self.root.channel_socket_entry("s")? != self.socket_binding
                || !probe_refused(&self.root.path().join("s"))?
            {
                return Err(invalid());
            }
            self.root.channel_unlink_exact("s", self.socket_binding)?;
            self.root.channel_unlink_exact(
                self.executable
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(invalid)?,
                self.executable_binding,
            )?;
            if let Some((_, binding)) = &self.published {
                self.root.channel_unlink_exact("service.json", *binding)?;
            }
            Ok(())
        };
        clean().is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };
    fn fixture() -> (tempfile::TempDir, PathLayout) {
        let temp = tempfile::tempdir_in("/private/tmp").unwrap();
        let p = temp.path();
        let paths = PathLayout {
            config: p.join("config"),
            state: p.join("state"),
            cache: p.join("cache"),
            data: p.join("data"),
        };
        (temp, paths)
    }
    fn bind(path: &Allocation) -> (UnixListener, PrivateEntryIdentity) {
        RootedDir::open_anchored_absolute(&path.directory)
            .unwrap()
            .channel_bind_socket("s")
            .unwrap()
    }

    #[test]
    fn allocations_are_fresh_private_and_clean_only_after_positive_refusal() {
        let (_temp, paths) = fixture();
        let a = allocate(&paths).unwrap();
        let b = allocate(&paths).unwrap();
        assert_ne!(a.directory, b.directory);
        assert!(a.socket.as_os_str().len() < 104);
        assert_eq!(
            fs::metadata(&a.directory).unwrap().permissions().mode() & 0o7777,
            0o700
        );
        let (listener, socket) = bind(&a);
        assert_eq!(validate_socket(&a).unwrap(), socket);
        assert!(!cleanup_settled_refused(&a, Some(socket), &|| true));
        assert_eq!(fs::symlink_metadata(&a.socket).unwrap().ino(), socket.inode);
        drop(listener);
        assert!(cleanup_settled_refused(&a, Some(socket), &|| true));
        assert!(!a.directory.exists());
        assert!(b.directory.exists());
    }

    #[test]
    fn missing_socket_or_evidence_and_expired_cleanup_retain_residue() {
        let (_temp, paths) = fixture();
        let a = allocate(&paths).unwrap();
        assert!(!cleanup_settled_refused(&a, None, &|| true));
        assert!(a.directory.exists());
        let (listener, socket) = bind(&a);
        drop(listener);
        assert!(!cleanup_settled_refused(&a, None, &|| true));
        assert!(!cleanup_settled_refused(&a, Some(socket), &|| false));
        let mut wrong = socket;
        wrong.inode += 1;
        assert!(!cleanup_settled_refused(&a, Some(wrong), &|| true));
        assert_eq!(validate_socket(&a).unwrap(), socket);
    }

    #[test]
    fn swapped_parent_or_socket_after_refusal_is_preserved() {
        let (_temp, paths) = fixture();
        let a = allocate(&paths).unwrap();
        let (listener, socket) = bind(&a);
        drop(listener);
        assert!(!cleanup_with_hook(&a, Some(socket), &|| true, || {
            fs::rename(&a.socket, a.directory.join("old")).unwrap();
            fs::write(&a.socket, b"replacement").unwrap();
        }));
        assert_eq!(fs::read(&a.socket).unwrap(), b"replacement");
        let b = allocate(&paths).unwrap();
        let (listener, socket) = bind(&b);
        drop(listener);
        let old = b.directory.with_extension("old");
        assert!(!cleanup_with_hook(&b, Some(socket), &|| true, || {
            fs::rename(&b.directory, &old).unwrap();
            fs::create_dir(&b.directory).unwrap();
            fs::write(&b.socket, b"new parent").unwrap();
        }));
        assert_eq!(fs::read(&b.socket).unwrap(), b"new parent");
        assert!(old.join("s").exists());
    }

    #[test]
    fn unknown_files_and_extra_entries_are_never_deleted() {
        let (_temp, paths) = fixture();
        let a = allocate(&paths).unwrap();
        fs::write(&a.socket, b"keep").unwrap();
        assert!(validate_socket(&a).is_err());
        assert!(!cleanup_settled_refused(&a, None, &|| true));
        assert_eq!(fs::read(&a.socket).unwrap(), b"keep");
        let b = allocate(&paths).unwrap();
        let (listener, socket) = bind(&b);
        drop(listener);
        fs::write(b.directory.join("extra"), b"keep").unwrap();
        assert!(!cleanup_settled_refused(&b, Some(socket), &|| true));
        assert!(b.socket.exists());
    }

    #[test]
    fn unsafe_or_long_roots_decline_before_forward_allocation() {
        let (_temp, mut paths) = fixture();
        paths.cache = paths.cache.join("x".repeat(104));
        assert!(allocate(&paths).is_err());
        assert!(!paths.controller_cache_root().exists());
        let (_temp, paths) = fixture();
        fs::create_dir_all(paths.controller_cache_root()).unwrap();
        fs::set_permissions(
            paths.controller_cache_root(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert!(allocate(&paths).is_err());
        assert!(!paths.controller_cache_root().join("channel").exists());
    }

    fn generation() -> (tempfile::TempDir, GenerationFiles) {
        let temp = tempfile::tempdir_in("/private/tmp").unwrap();
        let path = temp.path().join("worker");
        fs::write(&path, b"#!/bin/sh\nprintf old").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let root = RootedDir::create(&temp.path().join("rpc")).unwrap();
        let lease = bind_generation(
            root,
            &path,
            metadata.dev(),
            metadata.ino(),
            "11111111-1111-4111-8111-111111111111",
        )
        .unwrap();
        (temp, lease)
    }
    fn service(lease: &GenerationFiles) -> serde_json::Value {
        serde_json::json!({"socket_path":lease.root.path().join("s"),"service_generation":"11111111-1111-4111-8111-111111111111"})
    }

    #[test]
    fn image_withdrawal_requires_rpc_exit_proof_and_never_detached_proof() {
        let (_temp, mut lease) = generation();
        assert!(!lease.root.path().join("service.json").exists());
        lease.publish(&service(&lease)).unwrap();
        assert!(
            lease
                .executable
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("11111111111141118111111111111111")
        );
        assert!(!lease.withdraw(false));
        assert!(lease.executable.exists());
        assert!(lease.root.path().join("service.json").exists());
        assert!(lease.withdraw(true));
        assert!(!lease.executable.exists());
        assert!(lease.installed.exists());
        assert!(!lease.root.path().join("service.json").exists());
    }

    #[test]
    fn published_record_captures_exact_bindings_and_unknown_replacements_survive() {
        let (_temp, mut lease) = generation();
        lease.publish(&service(&lease)).unwrap();
        let file = lease.root.path().join("service.json");
        let bytes = fs::read(&file).unwrap();
        assert!(bytes.len() <= 8192);
        let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record["schema_version"], 1);
        assert_eq!(
            record["binding"]["socket"]["inode"],
            lease.socket_binding.inode
        );
        assert_eq!(
            record["executable"]["binding"]["inode"],
            lease.executable_binding.inode
        );
        assert_eq!(
            fs::metadata(&file).unwrap().permissions().mode() & 0o7777,
            0o600
        );
        fs::rename(&file, file.with_extension("old")).unwrap();
        fs::write(&file, b"replacement").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(!lease.withdraw(true));
        assert!(lease.executable.exists());
        assert_eq!(fs::read(&file).unwrap(), b"replacement");
    }

    #[test]
    fn installed_replacement_and_rollback_leave_generation_rpc_image_fixed() {
        let (temp, mut lease) = generation();
        let old = temp.path().join("old");
        fs::rename(&lease.installed, &old).unwrap();
        fs::write(&lease.installed, b"#!/bin/sh\nprintf new").unwrap();
        fs::set_permissions(&lease.installed, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            std::process::Command::new(&lease.executable)
                .output()
                .unwrap()
                .stdout,
            b"old"
        );
        assert_eq!(
            std::process::Command::new(&lease.installed)
                .output()
                .unwrap()
                .stdout,
            b"new"
        );
        fs::rename(&old, &lease.installed).unwrap();
        assert_eq!(
            std::process::Command::new(&lease.executable)
                .output()
                .unwrap()
                .stdout,
            b"old"
        );
        assert!(lease.withdraw(true));
    }
}
