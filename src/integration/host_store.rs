//! Rooted, bounded host execution evidence, separate from ordinary task DTOs.
use super::contracts::*;
use crate::{
    error::WorkerError, host_store::HostStore, inputs::RelativePath, rooted_fs::RootedDir,
    task::TaskId,
};
use std::{fs::File, io, os::fd::AsRawFd};

pub struct HostIntegrationStore<'a> {
    store: &'a HostStore,
}

/// A stop can precede the first host phase, or the next ordinary cycle's first
/// phase. Retain its authenticated identity without inventing an owner record.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostRevokeEvidence {
    pub request: HostIntegrationRequest,
    pub head: Option<crate::task::BaseOid>,
    pub turn: Option<crate::task::TurnId>,
    pub acknowledged: bool,
}
impl<'a> HostIntegrationStore<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }
    fn directory(
        &self,
        project: &str,
        task: TaskId,
        create: bool,
    ) -> Result<RootedDir, WorkerError> {
        Ok(self
            .store
            .open_task_directory(project, task, false)?
            .open_child_directory(&relative("integration")?, create)?)
    }
    pub fn load(
        &self,
        project_id: &str,
        task: TaskId,
    ) -> Result<Option<IntegrationRecord>, WorkerError> {
        let Some(bytes) = self.read(project_id, task, "record.json", MAX_PRIVATE_RECORD_BYTES)?
        else {
            return Ok(None);
        };
        let record: IntegrationRecord = decode_bounded(&bytes, MAX_PRIVATE_RECORD_BYTES)?;
        if record.task_id != task || record.policy.project_id != project_id {
            return Err(invalid());
        }
        Ok(Some(record))
    }
    pub(crate) fn save(&self, record: &IntegrationRecord) -> Result<(), WorkerError> {
        let bytes = encode_bounded(record, MAX_PRIVATE_RECORD_BYTES)?;
        self.write(
            &record.policy.project_id,
            record.task_id,
            "record.json",
            &bytes,
        )
    }
    pub(crate) fn revoke_evidence(
        &self,
        project: &str,
        task: TaskId,
    ) -> Result<Option<HostRevokeEvidence>, WorkerError> {
        let Some(bytes) = self.read(project, task, "revoke.json", MAX_PRIVATE_RECORD_BYTES)? else {
            return Ok(None);
        };
        let proof: HostRevokeEvidence = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        proof.request.validate()?;
        if proof.request.task_id != task
            || !matches!(proof.request.action, HostIntegrationAction::Revoke { .. })
            || serde_json::to_vec(&proof).map_err(|_| invalid())? != bytes
        {
            return Err(invalid());
        }
        Ok(Some(proof))
    }
    /// Ordinary status is the durable source succession fence. Preparations
    /// survive epoch changes, so auxiliary completions cannot retire a source.
    pub(crate) fn latest_ordinary_source(
        &self,
        project: &str,
        task: TaskId,
        status: &crate::task::TaskStatus,
    ) -> Result<Option<crate::task::TurnId>, WorkerError> {
        for turn in status.turns().iter().rev() {
            if self.prepared(project, task, turn.turn_id())?.is_none() {
                return Ok(Some(turn.turn_id()));
            }
        }
        Ok(None)
    }
    pub(crate) fn save_revoke_evidence(
        &self,
        project: &str,
        proof: &HostRevokeEvidence,
    ) -> Result<(), WorkerError> {
        proof.request.validate()?;
        let bytes = serde_json::to_vec(proof).map_err(|_| invalid())?;
        if bytes.len() > MAX_PRIVATE_RECORD_BYTES {
            return Err(invalid());
        }
        self.write(project, proof.request.task_id, "revoke.json", &bytes)
    }
    pub(crate) fn revoke_proved_at(
        &self,
        project: &str,
        task: TaskId,
        status: &crate::task::TaskStatus,
    ) -> Result<bool, WorkerError> {
        let Some(proof) = self.revoke_evidence(project, task)? else {
            return Ok(false);
        };
        if !proof.acknowledged
            || proof.head.as_ref() != status.head_oid()
            || proof.turn != status.turns().last().map(|turn| turn.turn_id())
        {
            return Ok(false);
        }
        Ok(self.load(project, task)?.is_none_or(|record| {
            record.tombstone.as_ref().is_some_and(|t| t.acknowledged)
                && record.push_intent.as_ref().is_none_or(|p| !p.uncertain)
                || record.receipt.as_ref().is_some_and(|r| r.imported)
        }))
    }
    pub(crate) fn policy(
        &self,
        project: &str,
        task: TaskId,
    ) -> Result<Option<FrozenIntegrationPolicy>, WorkerError> {
        self.read(project, task, "policy.json", MAX_PRIVATE_RECORD_BYTES)?
            .map(|bytes| decode_bounded(&bytes, MAX_PRIVATE_RECORD_BYTES))
            .transpose()
    }
    pub(crate) fn arm(
        &self,
        project: &str,
        task: TaskId,
        policy: &FrozenIntegrationPolicy,
    ) -> Result<(), WorkerError> {
        if let Some(old) = self.policy(project, task)? {
            if old != *policy {
                return Err(invalid());
            }
            return Ok(());
        }
        self.write(
            project,
            task,
            "policy.json",
            &encode_bounded(policy, MAX_PRIVATE_RECORD_BYTES)?,
        )
    }
    pub(crate) fn find_project(&self, task: TaskId) -> Result<String, WorkerError> {
        let tasks = self.store.open_directory("tasks", false)?;
        let mut found = None;
        for raw in tasks.list_names()? {
            let Ok(project) = std::str::from_utf8(&raw) else {
                continue;
            };
            if project.len() != 64
                || !project
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                continue;
            }
            let dir = tasks.open_child_directory(&relative(project)?, false)?;
            if dir.entry_exists(&task.to_string())? && found.replace(project.to_owned()).is_some() {
                return Err(invalid());
            }
        }
        found.ok_or_else(invalid)
    }
    pub(crate) fn retains(&self, project: &str, task: TaskId) -> Result<bool, WorkerError> {
        if let Some(record) = self.load(project, task)? {
            return Ok(!((record.snapshot.state == IntegrationStatus::Revoked
                && record.tombstone.as_ref().is_some_and(|t| t.acknowledged)
                && record.push_intent.as_ref().is_none_or(|p| !p.uncertain))
                || (record.snapshot.state == IntegrationStatus::Integrated
                    && record.receipt.as_ref().is_some_and(|r| r.imported))));
        }
        Ok(self.policy(project, task)?.is_some()
            && !self.store.task_status(project, task)?.state().is_terminal())
    }
    pub(crate) fn prepared(
        &self,
        project: &str,
        task: TaskId,
        turn: crate::task::TurnId,
    ) -> Result<Option<PreparedIntegrationTurn>, WorkerError> {
        self.read(
            project,
            task,
            &format!("turn-{turn}.json"),
            MAX_PREPARED_TURN_BYTES,
        )?
        .map(|bytes| decode_prepared_turn(&bytes))
        .transpose()
    }
    pub(crate) fn persist_prepared(
        &self,
        project: &str,
        prepared: &PreparedIntegrationTurn,
    ) -> Result<(), WorkerError> {
        let task = prepared.followup.task_id();
        let record = self.load(project, task)?.ok_or_else(invalid)?;
        prepared.validate_for(&record)?;
        prepared.followup.validate_self_consistency()?;
        if record.tombstone.is_some() {
            return Err(invalid());
        }
        if let Some(existing) = self.prepared(project, task, prepared.followup.turn_id())? {
            if existing != *prepared {
                return Err(invalid());
            }
            return Ok(());
        }
        self.write(
            project,
            task,
            &format!("turn-{}.json", prepared.followup.turn_id()),
            &encode_prepared_turn(prepared)?,
        )
    }
    pub(crate) fn lock(&self, project: &str, task: TaskId) -> Result<File, WorkerError> {
        let file = self
            .directory(project, task, true)?
            .open_private_lock("fence.lock")?;
        // Nonblocking: an unacknowledged revoke remains retryable while a bounded push owns the fence.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(IntegrationCode::IntegrationStopUnconfirmed.error());
        }
        Ok(file)
    }
    pub(crate) fn read(
        &self,
        project: &str,
        task: TaskId,
        name: &str,
        max: usize,
    ) -> Result<Option<Vec<u8>>, WorkerError> {
        for attempt in 0..3 {
            let result = self
                .directory(project, task, false)
                .and_then(|dir| Ok(dir.read_private_regular(name, max as u64)?));
            match result {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(WorkerError::Io(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(WorkerError::Io(e))
                    if e.raw_os_error() == Some(libc::ESTALE) && attempt < 2 =>
                {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(invalid())
    }
    pub(crate) fn write(
        &self,
        project: &str,
        task: TaskId,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), WorkerError> {
        for attempt in 0..3 {
            let dir = self.directory(project, task, true)?;
            let result = match dir.read_private_regular(name, MAX_PREPARED_TURN_BYTES as u64) {
                Ok(old) if old == bytes => return Ok(()),
                Ok(old) => dir.rewrite_private_regular_exact(name, &old, bytes),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    dir.write_private_atomic_no_replace(name, bytes)
                }
                Err(e) => Err(e),
            };
            match result {
                Ok(()) => {
                    dir.sync_root()?;
                    return Ok(());
                }
                Err(e)
                    if (e.raw_os_error() == Some(libc::ESTALE)
                        || e.kind() == io::ErrorKind::AlreadyExists)
                        && attempt < 2 =>
                {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(invalid())
    }
}
pub(crate) fn relative(path: &str) -> Result<RelativePath, WorkerError> {
    RelativePath::parse(path.as_bytes()).map_err(|_| invalid())
}
pub(crate) fn invalid() -> WorkerError {
    IntegrationCode::IntegrationStateInvalid.error()
}
