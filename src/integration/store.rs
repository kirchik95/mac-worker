//! Rooted owner sidecars. Fences cover only local durable publication.
use super::contracts::*;
use crate::{
    client_state::RunnerLivenessVerdict,
    error::WorkerError,
    inputs::RelativePath,
    job::ProcessIdentity,
    paths::PathLayout,
    rooted_fs::RootedDir,
    task::{TaskId, TurnId},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs::File, io, os::fd::AsRawFd, sync::Arc};

pub struct RootedIntegrationState {
    root: RootedDir,
    runtime: Arc<dyn IntegrationRuntime>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DueEntry {
    task: TaskId,
    revision: IntegrationRevision,
    due: u64,
    ready: u64,
    position: u64,
}
fn invalid() -> WorkerError {
    IntegrationCode::IntegrationStateInvalid.error()
}
fn child(root: &RootedDir, name: &str, create: bool) -> Result<RootedDir, WorkerError> {
    let path = RelativePath::parse(name.as_bytes()).map_err(|_| invalid())?;
    root.open_child_directory(&path, create)
        .map_err(WorkerError::Io)
}
fn read(root: &RootedDir, name: &str, max: usize) -> Result<Option<Vec<u8>>, WorkerError> {
    for attempt in 0..3 {
        match root.read_private_regular(name, max as u64) {
            Ok(bytes) => return Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e)
                if attempt < 2 && matches!(e.raw_os_error(), Some(libc::ESTALE | libc::EINTR)) => {}
            Err(e) => return Err(WorkerError::Io(e)),
        }
    }
    Err(invalid())
}
fn write(root: &RootedDir, name: &str, bytes: &[u8]) -> Result<(), WorkerError> {
    match read(root, name, MAX_PREPARED_TURN_BYTES)? {
        Some(old) if old == bytes => Ok(()),
        Some(old) => root
            .replace_private_regular_exact(name, &old, bytes)
            .map_err(WorkerError::Io),
        None => root
            .write_private_atomic_no_replace(name, bytes)
            .map_err(WorkerError::Io),
    }
}
fn exact(root: &RootedDir, name: &str, bytes: &[u8]) -> Result<(), WorkerError> {
    match read(root, name, MAX_PREPARED_TURN_BYTES)? {
        Some(old) if old == bytes => Ok(()),
        Some(_) => Err(invalid()),
        None => root
            .write_private_atomic_no_replace(name, bytes)
            .map_err(WorkerError::Io),
    }
}
fn prepared_name(turn: TurnId) -> String {
    format!("{turn}.json")
}
fn due_entry(record: &IntegrationRecord) -> Option<DueEntry> {
    if matches!(
        record.snapshot.state,
        IntegrationStatus::Armed
            | IntegrationStatus::Integrated
            | IntegrationStatus::Blocked
            | IntegrationStatus::Revoked
    ) && record.tombstone.as_ref().is_none_or(|t| t.acknowledged)
    {
        return None;
    }
    Some(DueEntry {
        task: record.task_id,
        revision: record.snapshot.revision,
        due: record
            .snapshot
            .retry_at_millis
            .unwrap_or(record.ready_at_millis)
            .max(record.ready_at_millis),
        ready: record.ready_at_millis,
        position: record.run_position,
    })
}
fn due_name(e: &DueEntry) -> String {
    format!(
        "{:020}-{:020}-{:020}-{}-{:020}.json",
        e.due, e.ready, e.position, e.task, e.revision.0
    )
}
fn reference(p: &PreparedIntegrationTurn, record: &IntegrationRecord) -> Result<(), WorkerError> {
    if let Some(intent) = record
        .auxiliaries
        .iter()
        .find(|i| i.turn_id == p.followup.turn_id())
    {
        p.validate_for(record)?;
        if intent.prepared_binding != p.binding()?
            || intent.created_at_millis != p.followup.created_at_millis()
        {
            return Err(invalid());
        }
    }
    Ok(())
}
impl RootedIntegrationState {
    pub fn open(
        paths: &PathLayout,
        runtime: Arc<dyn IntegrationRuntime>,
    ) -> Result<Self, WorkerError> {
        let state =
            RootedDir::open_or_create_anchored_absolute(&paths.state).map_err(WorkerError::Io)?;
        let root = child(&state, "integrations", true)?;
        let store = Self { root, runtime };
        {
            let _lock = store.lock()?;
            for name in ["tasks", "due", "reservations"] {
                child(&store.root, name, true)?;
            }
        }
        Ok(store)
    }
    fn lock(&self) -> Result<File, WorkerError> {
        for attempt in 0..3 {
            let file = self
                .root
                .open_private_lock("store.lock")
                .map_err(WorkerError::Io)?;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
                let e = io::Error::last_os_error();
                if attempt < 2 && e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(WorkerError::Io(e));
            }
            let identity = self
                .root
                .private_entry_identity("store.lock")
                .map_err(WorkerError::Io)?;
            match self
                .root
                .validate_private_regular_binding("store.lock", &file, identity)
            {
                Ok(()) => return Ok(file),
                Err(e) if attempt < 2 && e.raw_os_error() == Some(libc::ESTALE) => {}
                Err(e) => return Err(WorkerError::Io(e)),
            }
        }
        Err(invalid())
    }
    fn task(&self, task: TaskId, create: bool) -> Result<Option<RootedDir>, WorkerError> {
        match child(&self.root, &format!("tasks/{task}"), create) {
            Ok(dir) => Ok(Some(dir)),
            Err(WorkerError::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn record(&self, task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError> {
        let Some(dir) = self.task(task, false)? else {
            return Ok(None);
        };
        let record = read(&dir, "record.json", MAX_PRIVATE_RECORD_BYTES)?
            .map(|b| decode_record(&b))
            .transpose()?;
        if record.as_ref().is_some_and(|r| r.task_id != task) {
            return Err(invalid());
        }
        Ok(record)
    }
    fn preparation(
        &self,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<PreparedIntegrationTurn>, WorkerError> {
        let Some(dir) = self.task(task, false)? else {
            return Ok(None);
        };
        let dir = match child(&dir, "prepared", false) {
            Ok(dir) => dir,
            Err(WorkerError::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let p = read(&dir, &prepared_name(turn), MAX_PREPARED_TURN_BYTES)?
            .map(|b| decode_prepared_turn(&b))
            .transpose()?;
        if p.as_ref()
            .is_some_and(|p| p.followup.task_id() != task || p.followup.turn_id() != turn)
        {
            return Err(invalid());
        }
        Ok(p)
    }
    /// Read-only companion access; never creates state for disabled tasks.
    pub(crate) fn read_task(
        paths: &PathLayout,
        task: TaskId,
    ) -> Result<(Option<FrozenIntegrationPolicy>, Option<IntegrationRecord>), WorkerError> {
        Self::read_task_at(&paths.state, task)
    }
    pub(crate) fn read_task_at(
        state: &std::path::Path,
        task: TaskId,
    ) -> Result<(Option<FrozenIntegrationPolicy>, Option<IntegrationRecord>), WorkerError> {
        let root = match RootedDir::open_anchored_absolute(&state.join("integrations")) {
            Ok(root) => root,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((None, None)),
            Err(e) => return Err(WorkerError::Io(e)),
        };
        let dir = match child(&root, &format!("tasks/{task}"), false) {
            Ok(dir) => dir,
            Err(WorkerError::Io(e)) if e.kind() == io::ErrorKind::NotFound => {
                return Ok((None, None));
            }
            Err(e) => return Err(e),
        };
        let policy = read(&dir, "policy.json", MAX_PRIVATE_RECORD_BYTES)?
            .map(|b| decode_bounded(&b, MAX_PRIVATE_RECORD_BYTES))
            .transpose()?;
        let record = read(&dir, "record.json", MAX_PRIVATE_RECORD_BYTES)?
            .map(|b| decode_record(&b))
            .transpose()?;
        if record
            .as_ref()
            .is_some_and(|r| r.task_id != task || policy.as_ref() != Some(&r.policy))
        {
            return Err(invalid());
        }
        Ok((policy, record))
    }
    /// Durable purpose read before ordinary follow-up/terminal effects. No creation or flock.
    pub(crate) fn read_auxiliary(
        paths: &PathLayout,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<PreparedIntegrationTurn>, WorkerError> {
        let (_, record) = Self::read_task(paths, task)?;
        let referenced = record
            .as_ref()
            .is_some_and(|r| r.auxiliaries.iter().any(|a| a.turn_id == turn));
        let root = match RootedDir::open_anchored_absolute(&paths.state.join("integrations")) {
            Ok(root) => root,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(WorkerError::Io(e)),
        };
        let dir = match child(&root, &format!("tasks/{task}/prepared"), false) {
            Ok(dir) => dir,
            Err(WorkerError::Io(e)) if e.kind() == io::ErrorKind::NotFound && !referenced => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let Some(bytes) = read(&dir, &prepared_name(turn), MAX_PREPARED_TURN_BYTES)? else {
            return if referenced { Err(invalid()) } else { Ok(None) };
        };
        let prepared = decode_prepared_turn(&bytes)?;
        if prepared.followup.task_id() != task || prepared.followup.turn_id() != turn {
            return Err(invalid());
        }
        if let Some(record) = record {
            reference(&prepared, &record)?;
        }
        Ok(Some(prepared))
    }
}
impl IntegrationState for RootedIntegrationState {
    fn load(&self, task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError> {
        let _lock = self.lock()?;
        self.record(task)
    }
    fn load_policy(&self, task: TaskId) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
        let _lock = self.lock()?;
        let Some(dir) = self.task(task, false)? else {
            return Ok(None);
        };
        read(&dir, "policy.json", MAX_PRIVATE_RECORD_BYTES)?
            .map(|b| decode_bounded(&b, MAX_PRIVATE_RECORD_BYTES))
            .transpose()
    }
    fn publish_policy(
        &self,
        task: TaskId,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<(), WorkerError> {
        let bytes = encode_bounded(policy, MAX_PRIVATE_RECORD_BYTES)?;
        let lock = self.lock()?;
        exact(
            &self.task(task, true)?.ok_or_else(invalid)?,
            "policy.json",
            &bytes,
        )?;
        drop(lock);
        self.runtime.reach(IntegrationHook::AfterPolicy);
        Ok(())
    }
    fn publish_prepared(
        &self,
        task: TaskId,
        p: &PreparedIntegrationTurn,
    ) -> Result<(), WorkerError> {
        let bytes = encode_prepared_turn(p)?;
        if p.followup.task_id() != task {
            return Err(invalid());
        }
        let _lock = self.lock()?;
        if let Some(record) = self.record(task)? {
            p.validate_for(&record)?;
            reference(p, &record)?;
        }
        let dir = self.task(task, true)?.ok_or_else(invalid)?;
        exact(
            &child(&dir, "prepared", true)?,
            &prepared_name(p.followup.turn_id()),
            &bytes,
        )
    }
    fn load_prepared(
        &self,
        task: TaskId,
        turn: TurnId,
    ) -> Result<Option<PreparedIntegrationTurn>, WorkerError> {
        let _lock = self.lock()?;
        let p = self.preparation(task, turn)?;
        if let (Some(p), Some(record)) = (&p, self.record(task)?) {
            reference(p, &record)?;
        }
        Ok(p)
    }
    fn replace(
        &self,
        task: TaskId,
        expected: IntegrationRevision,
        next: &IntegrationRecord,
    ) -> Result<bool, WorkerError> {
        let bytes = encode_record(next)?;
        if next.task_id != task || next.snapshot.revision != expected.next()? {
            return Err(invalid());
        }
        let _lock = self.lock()?;
        let old = self.record(task)?;
        if old
            .as_ref()
            .map_or(IntegrationRevision(0), |r| r.snapshot.revision)
            != expected
        {
            return Ok(false);
        }
        let dir = self.task(task, true)?.ok_or_else(invalid)?;
        let policy: FrozenIntegrationPolicy = read(&dir, "policy.json", MAX_PRIVATE_RECORD_BYTES)?
            .ok_or_else(invalid)
            .and_then(|b| decode_bounded(&b, MAX_PRIVATE_RECORD_BYTES))?;
        if policy != next.policy {
            return Err(invalid());
        }
        for intent in &next.auxiliaries {
            if let Some(p) = self.preparation(task, intent.turn_id)? {
                reference(&p, next)?;
            }
        }
        let index = child(&self.root, "due", false)?;
        // Index publication precedes the record, so a crash cannot hide enabled work.
        if let Some(e) = due_entry(next) {
            exact(
                &index,
                &due_name(&e),
                &serde_json::to_vec(&e).map_err(|_| invalid())?,
            )?;
        }
        write(&dir, "record.json", &bytes)?;
        if let Some(e) = old.as_ref().and_then(due_entry) {
            index
                .remove_owned_regular(&due_name(&e))
                .map_err(WorkerError::Io)?;
        }
        Ok(true)
    }
    fn reserve(
        &self,
        key: &TargetKey,
        id: IntegrationId,
        epoch: u32,
        actor: ProcessIdentity,
    ) -> Result<Option<TargetReservation>, WorkerError> {
        let reservation = TargetReservation {
            key: key.clone(),
            integration_id: id,
            epoch,
            actor,
        };
        reservation.validate()?;
        let name = format!("{:x}.json", Sha256::digest(key.canonical_bytes()?));
        let _lock = self.lock()?;
        let dir = child(&self.root, "reservations", false)?;
        let mut count = 0;
        for raw in dir.list_names().map_err(WorkerError::Io)? {
            if raw == b".mac-worker-rooted-fs"
                || crate::rooted_fs::is_private_replacement_name(&raw)
            {
                continue;
            }
            let entry = std::str::from_utf8(&raw).map_err(|_| invalid())?;
            let old: TargetReservation = decode_bounded(
                &read(&dir, entry, MAX_PRIVATE_RECORD_BYTES)?.ok_or_else(invalid)?,
                MAX_PRIVATE_RECORD_BYTES,
            )?;
            if entry != format!("{:x}.json", Sha256::digest(old.key.canonical_bytes()?)) {
                return Err(invalid());
            }
            if old == reservation {
                return Ok(Some(old));
            }
            if self.runtime.actor_verdict(old.actor) == RunnerLivenessVerdict::Exited {
                dir.remove_owned_regular(entry).map_err(WorkerError::Io)?;
            } else {
                count += 1;
                if entry == name {
                    return Ok(None);
                }
            }
        }
        if count >= MAX_GIT_DRIVERS {
            return Ok(None);
        }
        exact(
            &dir,
            &name,
            &encode_bounded(&reservation, MAX_PRIVATE_RECORD_BYTES)?,
        )?;
        Ok(Some(reservation))
    }
    fn release(&self, reservation: &TargetReservation) -> Result<(), WorkerError> {
        reservation.validate()?;
        let _lock = self.lock()?;
        let dir = child(&self.root, "reservations", false)?;
        let name = format!(
            "{:x}.json",
            Sha256::digest(reservation.key.canonical_bytes()?)
        );
        if let Some(bytes) = read(&dir, &name, MAX_PRIVATE_RECORD_BYTES)? {
            if decode_bounded::<TargetReservation>(&bytes, MAX_PRIVATE_RECORD_BYTES)?
                != *reservation
            {
                return Err(invalid());
            }
            dir.remove_owned_regular(&name).map_err(WorkerError::Io)?;
        }
        Ok(())
    }
    fn due(&self, now: u64, limit: usize) -> Result<Vec<TaskId>, WorkerError> {
        let _lock = self.lock()?;
        let dir = child(&self.root, "due", false)?;
        let mut names = dir.list_names().map_err(WorkerError::Io)?;
        names.sort();
        let mut rows = Vec::new();
        for raw in names {
            if rows.len() >= limit.min(32) {
                break;
            }
            if raw == b".mac-worker-rooted-fs"
                || crate::rooted_fs::is_private_replacement_name(&raw)
            {
                continue;
            }
            let name = std::str::from_utf8(&raw).map_err(|_| invalid())?;
            let e: DueEntry = serde_json::from_slice(&read(&dir, name, 2048)?.ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            if due_name(&e) != name {
                return Err(invalid());
            }
            if e.due > now {
                break;
            }
            if let Some(record) = self.record(e.task)?
                && record.snapshot.revision == e.revision
                && due_entry(&record).is_some()
            {
                rows.push((e.ready, e.position, e.task));
            } else {
                dir.remove_owned_regular(name).map_err(WorkerError::Io)?;
            }
        }
        rows.sort_by_key(|r| (r.0, r.1, r.2.to_string()));
        Ok(rows.into_iter().map(|r| r.2).collect())
    }
}
