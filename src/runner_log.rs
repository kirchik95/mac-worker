//! Durable, private per-turn append journal. Readers never repair state.
use crate::{
    error::WorkerError,
    inputs::RelativePath,
    job::{LogChunk, LogStream},
    rooted_fs::RootedDir,
    task::{TaskId, TaskOutcome, TurnId},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, os::fd::AsRawFd, path::Path};
const LIMIT: usize = 128 * 1024;
const CHUNK: usize = 64 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Completion {
    pub outcome: TaskOutcome,
    pub drained: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Commit {
    offsets: [u64; 2],
    len: u64,
    accepted: bool,
    completion: Option<Completion>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    payload: String,
    next: Commit,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u32,
    task_id: TaskId,
    turn_id: TurnId,
    committed: Commit,
    pending: Option<Pending>,
}
pub(crate) struct RunnerLog {
    dir: RootedDir,
    name: String,
    sidecar: String,
    file: File,
    journal: Journal,
    encoded: Vec<u8>,
    poisoned: bool,
    /// Dropped after `file` so the exclusion covers the locked FD's lifetime.
    #[cfg(test)]
    _fork_exclusion: crate::test_sync::HeldFlock,
}
fn invalid() -> WorkerError {
    WorkerError::task(
        "LOG_CHECKPOINT_INVALID",
        "runner log checkpoint is inconsistent",
    )
}

fn is_undrainable_outcome(outcome: &TaskOutcome) -> bool {
    matches!(outcome, TaskOutcome::Failed { reason } if reason == "LOG_DRAIN_UNAVAILABLE")
}

impl Completion {
    /// Accepted journals may finish as not-drained only for this explicit
    /// degraded outcome. Completion here is not proof that local Failed
    /// status, abandon_code, or result/base publication finished.
    pub(crate) fn is_undrainable(&self) -> bool {
        !self.drained && is_undrainable_outcome(&self.outcome)
    }
}

/// `drained=true` still requires acceptance. `drained=false` is the empty
/// pre-acceptance finish, or the explicit undrainable degraded state: the
/// host accepted the turn but remaining stdout/stderr cannot be proven.
fn completion_is_valid(accepted: bool, offsets: [u64; 2], completion: &Completion) -> bool {
    if completion.drained {
        accepted
    } else if is_undrainable_outcome(&completion.outcome) {
        true
    } else {
        !accepted && offsets == [0, 0]
    }
}
fn turn_terminal_bytes(
    task: TaskId,
    turn: TurnId,
    outcome: &TaskOutcome,
) -> Result<Vec<u8>, WorkerError> {
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "type": "turn_terminal",
        "protocol_version": crate::protocol::PROTOCOL_VERSION,
        "task_id": task,
        "turn_id": turn,
        "outcome": outcome,
    }))
    .map_err(|_| invalid())?;
    bytes.push(b'\n');
    Ok(bytes)
}
fn directory(root: &Path, task: TaskId, create: bool) -> Result<RootedDir, WorkerError> {
    let mut root = RootedDir::open(root)?;
    let device = root.root_metadata()?.st_dev as u64;
    root.bind_host_device(device)?;
    let runners = root.open_child_directory(
        &RelativePath::parse(b"runners").map_err(|_| invalid())?,
        create,
    )?;
    Ok(runners.open_child_directory(
        &RelativePath::parse(task.to_string().as_bytes()).map_err(|_| invalid())?,
        create,
    )?)
}
impl RunnerLog {
    /// A busy journal belongs to an active writer/finalizer. Never wait while
    /// holding another state lock; reconciliation can retry on its next pass.
    pub(crate) fn try_open(
        root: &Path,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<Self>, WorkerError> {
        match Self::open(root, task, turn) {
            Ok(log) => Ok(Some(log)),
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Opens an existing journal without creating one. Missing or busy
    /// journals are `None` so DAG bind can retry without fabricating a log.
    pub(crate) fn try_open_existing(
        root: &Path,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<Self>, WorkerError> {
        let dir = match directory(root, task, false) {
            Ok(dir) => dir,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let name = format!("{turn}.log");
        if !dir.entry_exists(&name)? {
            return Ok(None);
        }
        Self::try_open(root, task, turn)
    }

    /// The journal flock is the finalization fence. Validate the current exact
    /// turn only after acquiring it, and retain this writer through retirement.
    pub(crate) fn current_entry(
        &self,
        store: &crate::client_state::ClientStateStore,
    ) -> Result<Option<crate::job::QueueEntry>, WorkerError> {
        Ok(store
            .queue_entry_for_task_turn(self.journal.task_id)?
            .filter(|entry| entry.job_id() == self.journal.turn_id))
    }

    pub(crate) fn require_owner(
        &self,
        store: &crate::client_state::ClientStateStore,
        owner: crate::job::ProcessIdentity,
    ) -> Result<crate::job::QueueEntry, WorkerError> {
        self.current_entry(store)?
            .filter(|entry| entry.owner_opt() == Some(&owner))
            .ok_or_else(|| {
                WorkerError::task(
                    "TASK_BUSY",
                    "task turn ownership changed before finalization",
                )
            })
    }

    pub(crate) fn open(root: &Path, task_id: TaskId, turn_id: TurnId) -> Result<Self, WorkerError> {
        #[cfg(test)]
        let fork_exclusion = crate::test_sync::HeldFlock::acquire();
        let dir = directory(root, task_id, true)?;
        let name = format!("{turn_id}.log");
        let sidecar = format!("{turn_id}.checkpoint.json");
        if !dir.entry_exists(&name)? {
            if dir.entry_exists(&sidecar)? {
                return Err(invalid());
            }
            dir.write_private_atomic_no_replace(&name, &[])?;
        }
        let file = dir.open_private_append(&name)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(WorkerError::Io(std::io::Error::last_os_error()));
        }
        let len = dir.validate_private_append_binding(&name, &file)?;
        if !dir.entry_exists(&sidecar)? {
            if len != 0 {
                return Err(WorkerError::task(
                    "LOG_CHECKPOINT_MISSING",
                    "nonempty legacy log has no native offsets",
                ));
            }
            let initial = Journal {
                version: 1,
                task_id,
                turn_id,
                committed: Commit::default(),
                pending: None,
            };
            dir.write_private_atomic_no_replace(
                &sidecar,
                &serde_json::to_vec(&initial).map_err(|_| invalid())?,
            )?;
        }
        let encoded = dir.read_private_regular(&sidecar, LIMIT as u64)?;
        let journal = decode(&encoded, task_id, turn_id)?;
        let mut writer = Self {
            dir,
            name,
            sidecar,
            file,
            journal,
            encoded,
            poisoned: false,
            #[cfg(test)]
            _fork_exclusion: fork_exclusion,
        };
        writer.recover()?;
        Ok(writer)
    }
    pub(crate) fn offsets(&self) -> [u64; 2] {
        self.journal.committed.offsets
    }
    pub(crate) fn completion(&self) -> Option<&Completion> {
        self.journal.committed.completion.as_ref()
    }
    pub(crate) fn is_accepted(&self) -> bool {
        self.journal.committed.accepted
    }
    pub(crate) fn len(&self) -> u64 {
        self.journal.committed.len
    }
    fn publish(&mut self, journal: Journal) -> Result<(), WorkerError> {
        let bytes = serde_json::to_vec(&journal).map_err(|_| invalid())?;
        if bytes.len() > LIMIT {
            return Err(invalid());
        }
        self.poisoned = true;
        self.dir
            .replace_private_regular_exact(&self.sidecar, &self.encoded, &bytes)?;
        self.encoded = bytes;
        self.journal = journal;
        self.poisoned = false;
        Ok(())
    }
    fn recover(&mut self) -> Result<(), WorkerError> {
        let n = self
            .dir
            .validate_private_append_binding(&self.name, &self.file)?;
        let l = self.journal.committed.len;
        let Some(pending) = self.journal.pending.clone() else {
            return if n == l { Ok(()) } else { Err(invalid()) };
        };
        let bytes = STANDARD.decode(&pending.payload).map_err(|_| invalid())?;
        let end = l.checked_add(bytes.len() as u64).ok_or_else(invalid)?;
        if n < l || n > end {
            return Err(invalid());
        }
        let written = usize::try_from(n - l).map_err(|_| invalid())?;
        if self
            .dir
            .read_private_regular_chunk(&self.name, l, written)?
            != bytes[..written]
        {
            return Err(invalid());
        }
        self.poisoned = true;
        self.file.write_all(&bytes[written..])?;
        self.file.sync_all()?;
        if self
            .dir
            .validate_private_append_binding(&self.name, &self.file)?
            != end
        {
            return Err(invalid());
        }
        let mut next = self.journal.clone();
        next.committed = pending.next;
        next.pending = None;
        self.publish(next)
    }
    fn append(&mut self, bytes: &[u8], mut next: Commit) -> Result<(), WorkerError> {
        if self.poisoned
            || self.journal.pending.is_some()
            || self.completion().is_some()
            || bytes.len() > CHUNK
        {
            return Err(invalid());
        }
        if self
            .dir
            .validate_private_append_binding(&self.name, &self.file)?
            != self.len()
        {
            return Err(invalid());
        }
        next.len = self
            .len()
            .checked_add(bytes.len() as u64)
            .ok_or_else(invalid)?;
        let mut intent = self.journal.clone();
        intent.pending = Some(Pending {
            payload: STANDARD.encode(bytes),
            next,
        });
        self.publish(intent)?;
        self.recover()
    }
    pub(crate) fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), WorkerError> {
        for chunk in bytes.chunks(CHUNK) {
            self.append(chunk, self.journal.committed.clone())?;
        }
        Ok(())
    }
    pub(crate) fn append_chunk(&mut self, chunk: &LogChunk) -> Result<(), WorkerError> {
        let bytes = chunk.decoded_bytes()?;
        let i = if chunk.stream() == LogStream::Stdout {
            0
        } else {
            1
        };
        if chunk.offset() != self.offsets()[i] {
            return Err(invalid());
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let mut next = self.journal.committed.clone();
        next.offsets[i] = chunk.next_offset();
        self.append(&bytes, next)
    }
    pub(crate) fn accepted(&mut self, bytes: &[u8]) -> Result<bool, WorkerError> {
        if self.journal.committed.accepted {
            return Ok(false);
        }
        let mut next = self.journal.committed.clone();
        next.accepted = true;
        self.append(bytes, next)?;
        Ok(true)
    }
    pub(crate) fn finish(
        &mut self,
        completion: Completion,
        bytes: &[u8],
    ) -> Result<bool, WorkerError> {
        if let Some(current) = self.completion() {
            return if *current == completion {
                Ok(false)
            } else {
                Err(invalid())
            };
        }
        if !completion_is_valid(self.journal.committed.accepted, self.offsets(), &completion) {
            return Err(invalid());
        }
        let mut next = self.journal.committed.clone();
        next.completion = Some(completion);
        self.append(bytes, next)?;
        Ok(true)
    }
}

impl RunnerLog {
    pub(crate) fn finish_local(
        &mut self,
        task: TaskId,
        turn: TurnId,
        outcome: TaskOutcome,
    ) -> Result<(), WorkerError> {
        let bytes = turn_terminal_bytes(task, turn, &outcome)?;
        self.finish(
            Completion {
                outcome,
                drained: false,
            },
            &bytes,
        )?;
        Ok(())
    }
}
fn decode(bytes: &[u8], task_id: TaskId, turn_id: TurnId) -> Result<Journal, WorkerError> {
    if bytes.len() > LIMIT {
        return Err(invalid());
    }
    let j: Journal = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if j.version != 1 || j.task_id != task_id || j.turn_id != turn_id {
        return Err(invalid());
    }
    if j.committed.offsets[0]
        .checked_add(j.committed.offsets[1])
        .is_none_or(|n| n > j.committed.len)
    {
        return Err(invalid());
    }
    if j.committed
        .completion
        .as_ref()
        .is_some_and(|c| !completion_is_valid(j.committed.accepted, j.committed.offsets, c))
    {
        return Err(invalid());
    }
    if let Some(p) = &j.pending {
        let b = STANDARD.decode(&p.payload).map_err(|_| invalid())?;
        if b.len() > CHUNK
            || j.committed.completion.is_some()
            || j.committed.len.checked_add(b.len() as u64) != Some(p.next.len)
            || (j.committed.accepted && !p.next.accepted)
        {
            return Err(invalid());
        }
        let mut delta = 0u64;
        for i in 0..2 {
            delta = delta
                .checked_add(
                    p.next.offsets[i]
                        .checked_sub(j.committed.offsets[i])
                        .ok_or_else(invalid)?,
                )
                .ok_or_else(invalid)?;
        }
        if delta != 0
            && (delta != b.len() as u64
                || (p.next.offsets[0] != j.committed.offsets[0]
                    && p.next.offsets[1] != j.committed.offsets[1])
                || p.next.accepted != j.committed.accepted
                || p.next.completion != j.committed.completion)
        {
            return Err(invalid());
        }
        if p.next
            .completion
            .as_ref()
            .is_some_and(|c| !completion_is_valid(p.next.accepted, p.next.offsets, c))
        {
            return Err(invalid());
        }
    }
    Ok(j)
}
pub(crate) struct Snapshot {
    pub len: u64,
    pub completion: Option<Completion>,
}
pub(crate) fn snapshot(
    root: &Path,
    task: TaskId,
    turn: TurnId,
) -> Result<Option<Snapshot>, WorkerError> {
    let result = (|| {
        let dir = directory(root, task, false)?;
        let bytes = dir.read_private_regular(&format!("{turn}.checkpoint.json"), LIMIT as u64)?;
        let j = decode(&bytes, task, turn)?;
        Ok(Snapshot {
            len: j.committed.len,
            completion: j.committed.completion,
        })
    })();
    match result {
        Ok(s) => Ok(Some(s)),
        Err(WorkerError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}
/// Read exactly the selected snapshot's committed prefix using bounded rooted reads.
/// Later writer transactions may append, but cannot extend this snapshot.
pub(crate) fn read_committed(
    root: &Path,
    task: TaskId,
    turn: TurnId,
    snapshot: &Snapshot,
) -> Result<Vec<u8>, WorkerError> {
    let dir = directory(root, task, false)?;
    let name = format!("{turn}.log");
    let mut bytes = Vec::new();
    let mut offset = 0u64;
    // Even an empty committed prefix must validate the file binding and permissions.
    if snapshot.len == 0 {
        dir.read_private_regular_chunk(&name, 0, 0)?;
    }
    while offset < snapshot.len {
        let limit =
            usize::try_from((snapshot.len - offset).min(CHUNK as u64)).map_err(|_| invalid())?;
        let chunk = dir.read_private_regular_chunk(&name, offset, limit)?;
        if chunk.len() != limit {
            return Err(invalid());
        }
        offset = offset.checked_add(chunk.len() as u64).ok_or_else(invalid)?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (tempfile::TempDir, TaskId, TurnId) {
        let d = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            d.path(),
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
        )
        .unwrap();
        (d, TaskId::generate(), TurnId::generate())
    }
    #[test]
    fn pending_crash_windows_recover_without_replay() {
        for written in [0, 2, 5] {
            let (d, t, u) = setup();
            let mut w = RunnerLog::open(d.path(), t, u).unwrap();
            w.append_bytes(b"before").unwrap();
            let mut intent = w.journal.clone();
            let mut next = intent.committed.clone();
            next.len += 5;
            next.offsets[0] = 5;
            intent.pending = Some(Pending {
                payload: STANDARD.encode(b"hello"),
                next,
            });
            w.publish(intent).unwrap();
            w.file.write_all(&b"hello"[..written]).unwrap();
            w.file.sync_all().unwrap();
            assert_eq!(snapshot(d.path(), t, u).unwrap().unwrap().len, 6);
            drop(w);
            let w = RunnerLog::open(d.path(), t, u).unwrap();
            assert_eq!(w.offsets(), [5, 0]);
            assert_eq!(
                w.dir.read_private_regular(&w.name, 100).unwrap(),
                b"beforehello"
            );
        }
    }
    #[test]
    fn rejects_legacy_and_concurrent_writers() {
        let (d, t, u) = setup();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        assert!(RunnerLog::open(d.path(), t, u).is_err());
        w.append_bytes(b"old").unwrap();
        let sidecar = w.sidecar.clone();
        drop(w);
        directory(d.path(), t, false)
            .unwrap()
            .remove_owned_regular(&sidecar)
            .unwrap();
        assert_eq!(
            RunnerLog::open(d.path(), t, u).err().unwrap().public_code(),
            "LOG_CHECKPOINT_MISSING"
        );
    }
    #[test]
    fn completion_and_accepted_are_atomic_and_idempotent() {
        let (d, t, u) = setup();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        assert!(w.accepted(b"accepted\n").unwrap());
        assert!(!w.accepted(b"accepted\n").unwrap());
        let c = Completion {
            outcome: TaskOutcome::Done,
            drained: true,
        };
        assert!(w.finish(c.clone(), b"done\n").unwrap());
        assert!(!w.finish(c.clone(), b"done\n").unwrap());
        assert_eq!(w.len(), 14);
        assert_eq!(
            snapshot(d.path(), t, u).unwrap().unwrap().completion,
            Some(c)
        );
        assert!(w.append_bytes(b"late").is_err());
    }

    #[test]
    fn accepted_undrainable_completion_is_valid_without_claiming_drained() {
        let (d, t, u) = setup();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        assert!(w.accepted(b"accepted\n").unwrap());
        let c = Completion {
            outcome: TaskOutcome::failed("LOG_DRAIN_UNAVAILABLE"),
            drained: false,
        };
        assert!(w.finish(c.clone(), b"undrainable\n").unwrap());
        assert_eq!(
            snapshot(d.path(), t, u).unwrap().unwrap().completion,
            Some(c)
        );
        assert!(
            w.finish(
                Completion {
                    outcome: TaskOutcome::Done,
                    drained: false,
                },
                b"nope\n"
            )
            .is_err()
        );
    }

    #[test]
    fn accepted_success_cannot_complete_as_not_drained() {
        let (d, t, u) = setup();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        assert!(w.accepted(b"accepted\n").unwrap());
        assert!(
            w.finish(
                Completion {
                    outcome: TaskOutcome::Done,
                    drained: false,
                },
                b"nope\n"
            )
            .is_err()
        );
    }
}
#[cfg(test)]
mod validation_tests {
    use super::*;
    #[test]
    fn pending_cannot_advance_two_streams_with_one_payload() {
        let task = TaskId::generate();
        let turn = TurnId::generate();
        let j = Journal {
            version: 1,
            task_id: task,
            turn_id: turn,
            committed: Commit::default(),
            pending: Some(Pending {
                payload: STANDARD.encode(b"abcd"),
                next: Commit {
                    offsets: [2, 2],
                    len: 4,
                    ..Commit::default()
                },
            }),
        };
        assert!(decode(&serde_json::to_vec(&j).unwrap(), task, turn).is_err());
    }
    #[test]
    fn pending_drained_completion_requires_an_accepted_turn() {
        let task = TaskId::generate();
        let turn = TurnId::generate();
        let journal = Journal {
            version: 1,
            task_id: task,
            turn_id: turn,
            committed: Commit::default(),
            pending: Some(Pending {
                payload: STANDARD.encode(b"terminal"),
                next: Commit {
                    len: 8,
                    completion: Some(Completion {
                        outcome: TaskOutcome::Done,
                        drained: true,
                    }),
                    ..Commit::default()
                },
            }),
        };
        assert!(decode(&serde_json::to_vec(&journal).unwrap(), task, turn).is_err());
    }

    #[test]
    fn malformed_pending_suffix_is_preserved() {
        let d = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            d.path(),
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o700),
        )
        .unwrap();
        let t = TaskId::generate();
        let u = TurnId::generate();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        let mut intent = w.journal.clone();
        intent.pending = Some(Pending {
            payload: STANDARD.encode(b"expected"),
            next: Commit {
                len: 8,
                ..Commit::default()
            },
        });
        w.publish(intent).unwrap();
        w.file.write_all(b"wrong").unwrap();
        w.file.sync_all().unwrap();
        drop(w);
        assert!(RunnerLog::open(d.path(), t, u).is_err());
        assert_eq!(
            std::fs::read(
                d.path()
                    .join("runners")
                    .join(t.to_string())
                    .join(format!("{u}.log"))
            )
            .unwrap(),
            b"wrong"
        );
    }
}

#[cfg(test)]
mod crash_tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    fn setup() -> (tempfile::TempDir, TaskId, TurnId) {
        let d = tempfile::tempdir().unwrap();
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        (d, TaskId::generate(), TurnId::generate())
    }
    #[test]
    fn failed_checkpoint_cas_exposes_only_old_commit_until_reopen() {
        let (d, t, u) = setup();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        w.append_bytes(b"prefix").unwrap();
        let mut intent = w.journal.clone();
        let mut next = intent.committed.clone();
        next.len += 4;
        intent.pending = Some(Pending {
            payload: STANDARD.encode(b"tail"),
            next,
        });
        w.publish(intent).unwrap();
        w.file.write_all(b"tail").unwrap();
        w.file.sync_all().unwrap();
        // A competing equivalent sidecar publication makes the retained expected bytes stale.
        let mut replacement = w.encoded.clone();
        replacement.push(b' ');
        w.dir
            .replace_private_regular_exact(&w.sidecar, &w.encoded, &replacement)
            .unwrap();
        assert!(w.recover().is_err());
        assert!(w.append_bytes(b"late").is_err());
        assert_eq!(snapshot(d.path(), t, u).unwrap().unwrap().len, 6);
        drop(w);
        let w = RunnerLog::open(d.path(), t, u).unwrap();
        assert_eq!(w.len(), 10);
        assert_eq!(
            w.dir.read_private_regular(&w.name, 100).unwrap(),
            b"prefixtail"
        );
    }
    #[test]
    fn unsafe_or_corrupt_journal_never_changes_visible_log() {
        for kind in [
            "version",
            "identity",
            "unknown",
            "symlink",
            "permissions",
            "oversized",
        ] {
            let (d, t, u) = setup();
            let mut w = RunnerLog::open(d.path(), t, u).unwrap();
            w.append_bytes(b"keep").unwrap();
            let name = w.sidecar.clone();
            let dir = d.path().join("runners").join(t.to_string());
            drop(w);
            let path = dir.join(name);
            let original = std::fs::read(&path).unwrap();
            let mut j: serde_json::Value = serde_json::from_slice(&original).unwrap();
            match kind {
                "version" => {
                    j["version"] = 2.into();
                    std::fs::write(&path, serde_json::to_vec(&j).unwrap()).unwrap();
                }
                "identity" => {
                    j["task_id"] = TaskId::generate().to_string().into();
                    std::fs::write(&path, serde_json::to_vec(&j).unwrap()).unwrap();
                }
                "unknown" => {
                    j["extra"] = true.into();
                    std::fs::write(&path, serde_json::to_vec(&j).unwrap()).unwrap();
                }
                "symlink" => {
                    let target = dir.join("target");
                    std::fs::rename(&path, &target).unwrap();
                    symlink(target, &path).unwrap();
                }
                "permissions" => {
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap()
                }
                "oversized" => std::fs::write(&path, vec![b' '; LIMIT + 1]).unwrap(),
                _ => unreachable!(),
            }
            assert!(RunnerLog::open(d.path(), t, u).is_err(), "{kind}");
            assert!(snapshot(d.path(), t, u).is_err(), "{kind}");
            assert_eq!(
                std::fs::read(dir.join(format!("{u}.log"))).unwrap(),
                b"keep"
            );
        }
    }
    #[test]
    fn substituted_log_binding_is_rejected_before_append() {
        let (d, t, u) = setup();
        let mut w = RunnerLog::open(d.path(), t, u).unwrap();
        w.append_bytes(b"old").unwrap();
        let dir = d.path().join("runners").join(t.to_string());
        let path = dir.join(&w.name);
        std::fs::rename(&path, dir.join("retained")).unwrap();
        std::fs::write(&path, b"new").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(w.append_bytes(b"unsafe").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"new");
        assert_eq!(std::fs::read(dir.join("retained")).unwrap(), b"old");
    }
}
