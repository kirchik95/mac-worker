//! Native-only generation history. Within a boot, exit proof is independent of
//! age; neither a dead leader nor elapsed grace proves unknown cleanup complete.
//! An earlier boot has no surviving RPC processes or executable-validation scans.
use super::{core, private_entry, public_entry};
use crate::controller::channel::contracts::{
    EntryIdentity, PinnedExecutable, ServiceRecord, UuidString,
};
use crate::{job::ProcessIdentity, rooted_fs::RootedDir};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io, time::Duration};

const FILE: &str = "links.json";
const BYTES: u64 = 64 * 1024;
const MAX_GENERATIONS: usize = 64;
pub const LINK_GRACE: Duration = Duration::from_secs(10 * 60);

fn invalid() -> io::Error {
    io::Error::from_raw_os_error(libc::EINVAL)
}

/// Inject this clock stamp on native file operations. A boot-wide monotonic
/// reading survives leader restarts; a changed boot identity ends prior scans.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionTime {
    epoch: String,
    now: Duration,
}
impl RetentionTime {
    pub fn new(epoch: String, now: Duration) -> Self {
        Self { epoch, now }
    }
    pub fn system() -> io::Result<Self> {
        #[cfg(target_os = "macos")]
        let epoch = {
            let mut boot: libc::timeval = unsafe { std::mem::zeroed() };
            let mut size = std::mem::size_of_val(&boot);
            if unsafe {
                libc::sysctlbyname(
                    c"kern.boottime".as_ptr(),
                    (&mut boot as *mut libc::timeval).cast(),
                    &mut size,
                    std::ptr::null_mut(),
                    0,
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
            if size != std::mem::size_of_val(&boot)
                || boot.tv_sec <= 0
                || !(0..1_000_000).contains(&boot.tv_usec)
            {
                return Err(invalid());
            }
            format!("macos:{}:{}", boot.tv_sec, boot.tv_usec)
        };
        #[cfg(target_os = "linux")]
        let epoch = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .to_owned();
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "retention clock unavailable",
        ));
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let mut stamp: libc::timespec = unsafe { std::mem::zeroed() };
            if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut stamp) } != 0 {
                return Err(io::Error::last_os_error());
            }
            if stamp.tv_sec < 0 || !(0..1_000_000_000).contains(&stamp.tv_nsec) {
                return Err(invalid());
            }
            let time = Self::new(
                epoch,
                Duration::new(stamp.tv_sec as u64, stamp.tv_nsec as u32),
            );
            time.validate()?;
            Ok(time)
        }
    }
    fn validate(&self) -> io::Result<()> {
        if self.epoch.is_empty()
            || self.epoch.len() > 128
            || self.epoch.chars().any(char::is_control)
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn expired(&self, last: &Self) -> bool {
        self.epoch != last.epoch
            || self
                .now
                .checked_sub(last.now)
                .is_some_and(|age| age > LINK_GRACE)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Generation {
    generation: UuidString,
    executable: PinnedExecutable,
    socket: EntryIdentity,
    leader: ProcessIdentity,
    last_use: RetentionTime,
    rpc_exits_proven: bool,
}
impl Generation {
    fn name(&self) -> String {
        format!("e{}", self.generation.as_str().replace('-', ""))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct History {
    schema_version: u32,
    parent: EntryIdentity,
    generations: Vec<Generation>,
}

pub(super) struct State {
    history: History,
    snapshot: Option<(Vec<u8>, crate::rooted_fs::PrivateEntryIdentity)>,
}
impl State {
    pub(super) fn load(root: &RootedDir) -> io::Result<Self> {
        root.channel_private_root()?;
        let state = if root.entry_exists(FILE)? {
            let binding = root.private_entry_identity(FILE)?;
            let bytes = root.read_private_regular(FILE, BYTES)?;
            if root.private_entry_identity(FILE)? != binding {
                return Err(invalid());
            }
            let value =
                super::super::identity::core::decode_unique(&bytes).map_err(|_| invalid())?;
            Self {
                history: serde_json::from_value(value).map_err(|_| invalid())?,
                snapshot: Some((bytes, binding)),
            }
        } else {
            Self {
                history: History {
                    schema_version: 1,
                    parent: public_entry(root.identity()?),
                    generations: vec![],
                },
                snapshot: None,
            }
        };
        state.validate(root)?;
        Ok(state)
    }
    fn validate(&self, root: &RootedDir) -> io::Result<()> {
        if self.history.schema_version != 1
            || self.history.parent != public_entry(root.identity()?)
            || self.history.generations.len() > MAX_GENERATIONS
        {
            return Err(invalid());
        }
        let mut names = BTreeSet::new();
        let owner = unsafe { libc::geteuid() };
        for entry in &self.history.generations {
            entry.last_use.validate()?;
            let image = entry.executable.binding;
            let socket = entry.socket;
            if !names.insert(entry.name())
                || entry.executable.path != root.path().join(entry.name())
                || image.owner != owner
                || image.kind != libc::S_IFREG as u32
                || image.device != self.history.parent.device
                || image.inode == 0
                || image.mode & !0o7777 != 0
                || image.mode & 0o7022 != 0
                || image.mode & 0o100 == 0
                || socket.owner != owner
                || socket.kind != libc::S_IFSOCK as u32
                || socket.mode != 0o600
                || socket.device != self.history.parent.device
                || socket.inode == 0
            {
                return Err(invalid());
            }
        }
        self.verify(root)
    }
    fn verify(&self, root: &RootedDir) -> io::Result<()> {
        root.channel_private_root()?;
        if public_entry(root.identity()?) != self.history.parent {
            return Err(invalid());
        }
        if let Some((bytes, binding)) = &self.snapshot {
            if root.private_entry_identity(FILE)? != *binding
                || root.read_private_regular(FILE, BYTES)? != *bytes
                || root.private_entry_identity(FILE)? != *binding
            {
                return Err(invalid());
            }
        } else if root.entry_exists(FILE)? {
            return Err(invalid());
        }
        Ok(())
    }
    fn save(&mut self, root: &RootedDir) -> io::Result<()> {
        self.verify(root)?;
        let bytes = serde_json::to_vec(&self.history).map_err(|_| invalid())?;
        if bytes.len() as u64 > BYTES {
            return Err(invalid());
        }
        let binding = if let Some((old, binding)) = &self.snapshot {
            root.channel_replace_private_exact(FILE, *binding, old, &bytes)?;
            root.private_entry_identity(FILE)?
        } else {
            root.write_private_atomic_no_replace_with_identity(FILE, |_| Ok(bytes.clone()))?
        };
        self.snapshot = Some((bytes, binding));
        self.verify(root)
    }
    pub(super) fn import_record(
        &mut self,
        root: &RootedDir,
        record: &ServiceRecord,
        now: &RetentionTime,
    ) -> io::Result<()> {
        if self.history.generations.is_empty() && self.snapshot.is_none() {
            self.history.generations.push(Generation {
                generation: record.service.service_generation.clone(),
                executable: record.executable.clone(),
                socket: record.binding.socket,
                leader: record.service.leader,
                last_use: now.clone(),
                rpc_exits_proven: false,
            });
            self.validate(root)?;
        }
        Ok(())
    }
    pub(super) fn known_names(&self, root: &RootedDir) -> io::Result<Vec<Vec<u8>>> {
        self.verify(root)?;
        let mut known: Vec<_> = self
            .history
            .generations
            .iter()
            .map(|entry| entry.name().into_bytes())
            .collect();
        known.extend([FILE.as_bytes().to_vec(), b"service.json".to_vec()]);
        // Exact atomic replacement uses rooted_fs's private cleanup namespace.
        // It is retained internal state, not an unrecorded generation image.
        let namespace = ".mac-worker-rooted-fs";
        if root.entry_exists(namespace)? {
            root.open_child_directory(
                &crate::inputs::RelativePath::parse(namespace.as_bytes()).map_err(|_| invalid())?,
                false,
            )?
            .channel_private_root()?;
            known.push(namespace.as_bytes().to_vec());
        }
        if root
            .list_names()?
            .iter()
            .any(|name| name != b"s" && !known.contains(name))
        {
            return Err(invalid());
        }
        Ok(known)
    }
    pub(super) fn recover_unpublished_socket(
        &self,
        root: &RootedDir,
        controller_root: &std::path::Path,
    ) -> io::Result<()> {
        if !root.entry_exists("s")? {
            return Ok(());
        }
        let entry = self.history.generations.last().ok_or_else(invalid)?;
        let dead = matches!(
            crate::controller::health_read::observe_leader(controller_root, entry.leader)
                .map_err(io::Error::other)?,
            crate::supervisor::ProcessObservation::Absent
                | crate::supervisor::ProcessObservation::Reused
        );
        if !dead
            || root.channel_socket_entry("s")? != private_entry(entry.socket)
            || !core::probe_refused(&root.path().join("s"))?
        {
            return Err(invalid());
        }
        self.verify(root)?;
        root.channel_unlink_exact("s", private_entry(entry.socket))
    }
    pub(super) fn reserve(&self) -> io::Result<()> {
        if self.history.generations.len() >= MAX_GENERATIONS {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    pub(super) fn register(
        &mut self,
        root: &RootedDir,
        generation: &UuidString,
        files: &core::GenerationFiles,
        leader: ProcessIdentity,
        now: &RetentionTime,
    ) -> io::Result<()> {
        now.validate()?;
        self.reserve()?;
        self.history.generations.push(Generation {
            generation: generation.clone(),
            executable: PinnedExecutable {
                path: files.executable.clone(),
                binding: public_entry(files.executable_binding),
            },
            socket: public_entry(files.socket_binding),
            leader,
            last_use: now.clone(),
            rpc_exits_proven: false,
        });
        self.validate(root)?;
        self.save(root)
    }
    pub(super) fn closed(
        &mut self,
        root: &RootedDir,
        executable: &PinnedExecutable,
        now: &RetentionTime,
    ) -> io::Result<()> {
        now.validate()?;
        let entry = self
            .history
            .generations
            .iter_mut()
            .find(|entry| entry.executable == *executable)
            .ok_or_else(invalid)?;
        // Clock rollback extends retention instead of shortening last use.
        if entry.last_use.epoch != now.epoch || entry.last_use.now <= now.now {
            entry.last_use = now.clone();
        }
        entry.rpc_exits_proven = true;
        self.save(root)
    }
    /// The last two entries are current/previous even after their services stop.
    /// Pending grace or unknown same-boot proof is never discarded for a count.
    pub(super) fn sweep(&mut self, root: &RootedDir, now: &RetentionTime) -> io::Result<()> {
        now.validate()?;
        let candidates: Vec<_> = self
            .history
            .generations
            .iter()
            .take(self.history.generations.len().saturating_sub(2))
            .filter(|entry| {
                // Both stamps are validated. A reboot makes prior RPC exits
                // moot; elapsed grace alone still requires recorded exit proof.
                (entry.rpc_exits_proven || now.epoch != entry.last_use.epoch)
                    && now.expired(&entry.last_use)
            })
            .cloned()
            .collect();
        let mut removed = BTreeSet::new();
        for entry in candidates {
            self.verify(root)?;
            if !root.entry_exists(&entry.name())? {
                removed.insert(entry.name());
            } else if root.channel_executable_entry(&entry.name()).ok()
                == Some(private_entry(entry.executable.binding))
            {
                self.verify(root)?;
                if root
                    .channel_unlink_exact(&entry.name(), private_entry(entry.executable.binding))
                    .is_ok()
                {
                    removed.insert(entry.name());
                }
            }
        }
        if !removed.is_empty() {
            self.history
                .generations
                .retain(|entry| !removed.contains(&entry.name()));
            self.save(root)?;
        }
        Ok(())
    }
}
