//! Derived active-task-ID index for bounded leader ticks.
//!
//! Receipts live in `active-tasks/` and are not a second task/queue/runner
//! store. FLOW/integrator consume `select_active_task_ids` plus
//! `refresh_active_task_index` and own the TaskClient reconcile engine. This
//! module never calls `list_tasks`, public `queue_snapshot`, or
//! `reconcile_runners` on the idle path.

use std::{collections::HashSet, fs::File, io, os::fd::AsRawFd};

use serde::{Deserialize, Serialize};

use super::*;
use crate::{
    error::WorkerError,
    job::{JobId, QueueEntry, QueueEntryKind, QueueSnapshot},
    task::{LocalTaskRecord, TaskId, TaskState, TurnSummary},
};

const BOOTSTRAP_FILE: &str = "bootstrap-v1.json";
const CURSOR_FILE: &str = "cursor.json";
const INDEX_VERSION: u32 = 1;

/// Pure record predicate. Queue leftover is a separate locked proof: a
/// terminal row with no runner/intent still keeps its receipt while any
/// TaskTurn `job_id` matches this record's turn history or current intent.
pub fn task_record_needs_active_index(record: &LocalTaskRecord) -> bool {
    match record.status().state() {
        TaskState::Queued | TaskState::Active | TaskState::Open => true,
        TaskState::Closed | TaskState::Abandoned | TaskState::Lost => {
            record.submission_intent_turn_id().is_some()
                || record.submission_rollback_turn_id().is_some()
                || record.runner().is_some()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveTaskConfig {
    pub max_tasks_per_tick: usize,
}

impl Default for ActiveTaskConfig {
    fn default() -> Self {
        Self {
            max_tasks_per_tick: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTaskSelection {
    pub selected: Vec<TaskId>,
    pub busy_skipped: Vec<TaskId>,
    pub orphan_ids: Vec<TaskId>,
    pub failed: Vec<(String, String)>,
    pub truncated: bool,
    pub cursor_stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTaskBootstrapReport {
    pub already_bootstrapped: bool,
    pub rebuilt: Vec<TaskId>,
    pub corrupt: Vec<String>,
}

/// FLOW calls this after the selected-ID reconcile engine. One StateLock,
/// one existing queue snapshot for the page, at most `max_tasks_per_tick`
/// unique IDs. Does not walk historical `tasks/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTaskRefreshReport {
    pub retired: Vec<TaskId>,
    pub retained: Vec<TaskId>,
    pub failed: Vec<(String, String)>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveTaskReceipt {
    version: u32,
    task_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveTaskCursor {
    version: u32,
    last_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveTaskBootstrapReceipt {
    version: u32,
    created_at_millis: u64,
    rebuilt: Vec<String>,
    corrupt: Vec<String>,
}

impl ClientStateStore {
    pub fn bootstrap_active_task_index(&self) -> Result<ActiveTaskBootstrapReport, WorkerError> {
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let index = self.active_tasks_dir()?;
        if index
            .entry_exists(BOOTSTRAP_FILE)
            .map_err(WorkerError::Io)?
        {
            return read_bootstrap_receipt(&index);
        }
        let tasks = self.tasks_dir()?;
        let names = tasks.list_names().map_err(WorkerError::Io)?;
        self.recover_task_replacement_residue(&tasks, &names)?;
        let queued_turns = self.load_queue_task_turn_ids_locked()?;
        let mut rebuilt = Vec::new();
        let mut corrupt = Vec::new();
        for name in tasks.list_names().map_err(WorkerError::Io)? {
            if name.as_slice() == ROOTED_FS_NAMESPACE || is_private_replacement_name(&name) {
                continue;
            }
            let text = match std::str::from_utf8(&name) {
                Ok(text) => text,
                Err(_) => {
                    corrupt.push("non-UTF-8 task entry".to_owned());
                    continue;
                }
            };
            let Some(id_text) = text.strip_suffix(".json") else {
                corrupt.push(format!("{text}: unexpected task registry entry"));
                continue;
            };
            let task_id = match id_text.parse::<TaskId>() {
                Ok(task_id) => task_id,
                Err(_) => {
                    corrupt.push(format!("{text}: invalid task filename"));
                    continue;
                }
            };
            match read_task_from_dir(&tasks, text, task_id) {
                Ok(record) => {
                    if !should_keep_active_index(&record, &queued_turns) {
                        continue;
                    }
                    match self.ensure_active_task_index_locked(task_id) {
                        Ok(()) => rebuilt.push(task_id),
                        Err(WorkerError::Io(error))
                            if error.kind() == io::ErrorKind::InvalidData =>
                        {
                            corrupt.push(format!("{text}: {error}"));
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(error) => corrupt.push(format!("{text}: {error}")),
            }
        }
        rebuilt.sort_by_key(ToString::to_string);
        corrupt.sort();
        let receipt = ActiveTaskBootstrapReceipt {
            version: INDEX_VERSION,
            created_at_millis: advisory_now_millis(),
            rebuilt: rebuilt.iter().map(TaskId::to_string).collect(),
            corrupt: corrupt.clone(),
        };
        let bytes = encode_index_json(&receipt)?;
        match index.write_private_atomic_no_replace(BOOTSTRAP_FILE, &bytes) {
            Ok(()) => Ok(ActiveTaskBootstrapReport {
                already_bootstrapped: false,
                rebuilt,
                corrupt,
            }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                read_bootstrap_receipt(&index)
            }
            Err(error) => Err(WorkerError::Io(error)),
        }
    }

    pub fn select_active_task_ids(
        &self,
        config: &ActiveTaskConfig,
    ) -> Result<ActiveTaskSelection, WorkerError> {
        let bound = config.max_tasks_per_tick.max(1);
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let index = self.active_tasks_dir()?;
        let sorted = list_active_index_names(&index)?;
        let last = read_cursor(&index)?;
        let rotated = rotate_from_cursor(last.as_deref(), &sorted);
        let truncated = linear_remaining_after_cursor(last.as_deref(), &sorted) > bound;
        let window: Vec<&str> = rotated.into_iter().take(bound).collect();
        let mut report = ActiveTaskSelection {
            selected: Vec::new(),
            busy_skipped: Vec::new(),
            orphan_ids: Vec::new(),
            failed: Vec::new(),
            truncated,
            cursor_stale: false,
        };
        let mut last_attempted: Option<&str> = None;
        let tasks = self.tasks_dir()?;
        for name in window {
            last_attempted = Some(name);
            let task_id = match active_task_id_from_name(name) {
                Ok(task_id) => task_id,
                Err(error) => {
                    report.failed.push((name.to_owned(), error));
                    continue;
                }
            };
            let _receipt_lock = match try_lock_receipt(&index, name) {
                Ok(Some(file)) => file,
                Ok(None) => {
                    report.busy_skipped.push(task_id);
                    continue;
                }
                Err(error) => {
                    report.failed.push((task_id.to_string(), error.to_string()));
                    continue;
                }
            };
            if let Err(error) = read_receipt(&index, name, task_id) {
                report.failed.push((task_id.to_string(), error.to_string()));
                continue;
            }
            let task_name = match task_file_name(task_id) {
                Ok(name) => name,
                Err(error) => {
                    report.failed.push((task_id.to_string(), error.to_string()));
                    continue;
                }
            };
            match read_task_from_dir(&tasks, &task_name, task_id) {
                Ok(_record) => {
                    report.selected.push(task_id);
                }
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    report.orphan_ids.push(task_id);
                }
                Err(error) => report.failed.push((task_id.to_string(), error.to_string())),
            }
        }
        if let Some(last) = last_attempted {
            match persist_cursor(&index, last) {
                Ok(stale) => report.cursor_stale = !stale,
                Err(_) => report.cursor_stale = true,
            }
        }
        Ok(report)
    }

    /// Bounded post-reconcile refresh. FLOW passes the IDs it just selected.
    /// Reloads those records and one queue snapshot; retires only proven
    /// terminal + queue-free rows. A corrupt queue read does not delete.
    pub fn refresh_active_task_index(
        &self,
        task_ids: &[TaskId],
        config: &ActiveTaskConfig,
    ) -> Result<ActiveTaskRefreshReport, WorkerError> {
        let bound = config.max_tasks_per_tick.max(1);
        let _lock = StateLock::acquire(self.inner.root.as_raw_fd(), &self.inner.sync_counts)?;
        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for task_id in task_ids {
            if seen.insert(*task_id) {
                unique.push(*task_id);
            }
        }
        let truncated = unique.len() > bound;
        let page: Vec<TaskId> = unique.into_iter().take(bound).collect();
        let queued_turns = self.load_queue_task_turn_ids_locked()?;
        let tasks = self.tasks_dir()?;
        let mut report = ActiveTaskRefreshReport {
            retired: Vec::new(),
            retained: Vec::new(),
            failed: Vec::new(),
            truncated,
        };
        for task_id in page {
            let name = match task_file_name(task_id) {
                Ok(name) => name,
                Err(error) => {
                    report.failed.push((task_id.to_string(), error.to_string()));
                    continue;
                }
            };
            match read_task_from_dir(&tasks, &name, task_id) {
                Ok(record) => {
                    if should_keep_active_index(&record, &queued_turns) {
                        if let Err(error) = self.ensure_active_task_index_locked(task_id) {
                            report.failed.push((task_id.to_string(), error.to_string()));
                            continue;
                        }
                        report.retained.push(task_id);
                    } else if let Err(error) = self.retire_active_task_index_locked(task_id) {
                        report.failed.push((task_id.to_string(), error.to_string()));
                    } else {
                        report.retired.push(task_id);
                    }
                }
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    report.retained.push(task_id);
                }
                Err(error) => report.failed.push((task_id.to_string(), error.to_string())),
            }
        }
        Ok(report)
    }

    /// Occupancy input for runner-slot accounting. Caller already holds
    /// StateLock via QueueLock. After a valid bootstrap marker, loads every
    /// indexed current task record (not a reconcile page of 32). Missing
    /// task files are orphans and skipped. Any other read failure fails closed
    /// so a corrupt row cannot undercount holders. Absent marker keeps the
    /// historical `list_tasks_locked` fallback.
    pub(crate) fn occupancy_task_records_locked(
        &self,
    ) -> Result<Vec<LocalTaskRecord>, WorkerError> {
        let index = self.active_tasks_dir()?;
        if !index
            .entry_exists(BOOTSTRAP_FILE)
            .map_err(WorkerError::Io)?
        {
            return self.list_tasks_locked();
        }
        read_bootstrap_receipt(&index)?;
        let tasks = self.tasks_dir()?;
        let mut records = Vec::new();
        for name in list_active_index_names(&index)? {
            let task_id = active_task_id_from_name(&name).map_err(|_| {
                invalid_state("active-task occupancy index contains an invalid receipt name")
            })?;
            let task_name = task_file_name(task_id)?;
            match read_task_from_dir(&tasks, &task_name, task_id) {
                Ok(record) => records.push(record),
                Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(records)
    }

    pub(crate) fn should_keep_active_index_locked(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<bool, WorkerError> {
        if task_record_needs_active_index(record) {
            return Ok(true);
        }
        match self.load_queue_task_turn_ids_locked() {
            Ok(queued) => Ok(record_has_relevant_queue_turn(record, &queued)),
            Err(_) => Ok(true),
        }
    }

    fn load_queue_task_turn_ids_locked(&self) -> Result<HashSet<JobId>, WorkerError> {
        let (snapshot, _) = read_queue_snapshot(self.inner.queue.as_raw_fd())?;
        require_queue_client(&snapshot, self.inner.client_id)?;
        Ok(task_turn_ids_from_snapshot(&snapshot))
    }

    pub(crate) fn sync_active_task_index_locked(
        &self,
        record: &LocalTaskRecord,
    ) -> Result<(), WorkerError> {
        let task_id = record.meta().task_id();
        if self.should_keep_active_index_locked(record)? {
            self.ensure_active_task_index_locked(task_id)
        } else {
            self.retire_active_task_index_locked(task_id)
        }
    }

    pub(crate) fn ensure_active_task_index_locked(
        &self,
        task_id: TaskId,
    ) -> Result<(), WorkerError> {
        let index = self.active_tasks_dir()?;
        let name = active_receipt_name(task_id)?;
        let receipt = ActiveTaskReceipt {
            version: INDEX_VERSION,
            task_id: task_id.to_string(),
        };
        let bytes = encode_index_json(&receipt)?;
        match index.write_private_atomic_no_replace(&name, &bytes) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                read_receipt(&index, &name, task_id).map(|_| ())
            }
            Err(error) => Err(WorkerError::Io(error)),
        }
    }

    pub(crate) fn retire_active_task_index_locked(
        &self,
        task_id: TaskId,
    ) -> Result<(), WorkerError> {
        let index = self.active_tasks_dir()?;
        let name = active_receipt_name(task_id)?;
        match index.remove_owned_regular(&name) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WorkerError::Io(error)),
        }
    }
}

fn encode_index_json<T: Serialize>(value: &T) -> Result<Vec<u8>, WorkerError> {
    serde_json::to_vec(value)
        .map_err(|_| invalid_state("active-task index record cannot be serialized"))
}

fn active_receipt_name(task_id: TaskId) -> Result<String, WorkerError> {
    let name = format!("{task_id}.json");
    if name.as_bytes().contains(&0) {
        return Err(invalid_state(
            "task ID produced an invalid active-task filename",
        ));
    }
    Ok(name)
}

fn active_task_id_from_name(name: &str) -> Result<TaskId, String> {
    let id_text = name
        .strip_suffix(".json")
        .ok_or_else(|| "active-task receipt name is invalid".to_owned())?;
    id_text
        .parse::<TaskId>()
        .map_err(|_| "active-task receipt name is not a task ID".to_owned())
}

fn is_index_metadata_name(name: &str) -> bool {
    name == BOOTSTRAP_FILE || name == CURSOR_FILE
}

fn list_active_index_names(index: &RootedDir) -> Result<Vec<String>, WorkerError> {
    let mut names = Vec::new();
    for raw in index.list_names().map_err(WorkerError::Io)? {
        if raw.as_slice() == ROOTED_FS_NAMESPACE || is_private_replacement_name(&raw) {
            continue;
        }
        let Ok(name) = String::from_utf8(raw) else {
            names.push("<non-utf8>".to_owned());
            continue;
        };
        if is_index_metadata_name(&name) {
            continue;
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

fn read_receipt(
    index: &RootedDir,
    name: &str,
    task_id: TaskId,
) -> Result<ActiveTaskReceipt, WorkerError> {
    let bytes = index
        .read_private_regular(name, MAX_STATE_FILE_BYTES as u64)
        .map_err(WorkerError::Io)?;
    let receipt: ActiveTaskReceipt = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_state("active-task receipt is corrupt"))?;
    if receipt.version != INDEX_VERSION {
        return Err(invalid_state("active-task receipt version is unsupported"));
    }
    if receipt.task_id != task_id.to_string() {
        return Err(invalid_state(
            "active-task receipt identity does not match its filename",
        ));
    }
    Ok(receipt)
}

fn read_bootstrap_receipt(index: &RootedDir) -> Result<ActiveTaskBootstrapReport, WorkerError> {
    let bytes = index
        .read_private_regular(BOOTSTRAP_FILE, MAX_STATE_FILE_BYTES as u64)
        .map_err(WorkerError::Io)?;
    let receipt: ActiveTaskBootstrapReceipt = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_state("active-task bootstrap receipt is corrupt"))?;
    if receipt.version != INDEX_VERSION {
        return Err(invalid_state(
            "active-task bootstrap receipt version is unsupported",
        ));
    }
    let mut rebuilt = Vec::new();
    for id in &receipt.rebuilt {
        rebuilt.push(
            id.parse::<TaskId>()
                .map_err(|_| invalid_state("active-task bootstrap rebuilt id is invalid"))?,
        );
    }
    Ok(ActiveTaskBootstrapReport {
        already_bootstrapped: true,
        rebuilt,
        corrupt: receipt.corrupt,
    })
}

fn rotate_from_cursor<'a>(last: Option<&str>, sorted: &'a [String]) -> Vec<&'a str> {
    let Some(last) = last else {
        return sorted.iter().map(String::as_str).collect();
    };
    match sorted.iter().position(|name| name.as_str() > last) {
        Some(pos) => sorted[pos..]
            .iter()
            .chain(sorted[..pos].iter())
            .map(String::as_str)
            .collect(),
        None => sorted.iter().map(String::as_str).collect(),
    }
}

fn linear_remaining_after_cursor(last: Option<&str>, sorted: &[String]) -> usize {
    match last {
        None => sorted.len(),
        Some(last) => {
            let tail = sorted.iter().filter(|name| name.as_str() > last).count();
            if tail == 0 { sorted.len() } else { tail }
        }
    }
}

fn read_cursor(index: &RootedDir) -> Result<Option<String>, WorkerError> {
    if !index.entry_exists(CURSOR_FILE).map_err(WorkerError::Io)? {
        return Ok(None);
    }
    let bytes = index
        .read_private_regular(CURSOR_FILE, MAX_STATE_FILE_BYTES as u64)
        .map_err(WorkerError::Io)?;
    let cursor: ActiveTaskCursor = match serde_json::from_slice(&bytes) {
        Ok(cursor) => cursor,
        Err(_) => return Ok(None),
    };
    if cursor.version != INDEX_VERSION {
        return Ok(None);
    }
    Ok(Some(cursor.last_key))
}

fn persist_cursor(index: &RootedDir, last_key: &str) -> Result<bool, WorkerError> {
    let next = ActiveTaskCursor {
        version: INDEX_VERSION,
        last_key: last_key.to_owned(),
    };
    let bytes = encode_index_json(&next)?;
    if !index.entry_exists(CURSOR_FILE).map_err(WorkerError::Io)? {
        match index.write_private_atomic_no_replace(CURSOR_FILE, &bytes) {
            Ok(()) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(WorkerError::Io(error)),
        }
    }
    for _ in 0..2 {
        let expected = index
            .read_private_regular(CURSOR_FILE, MAX_STATE_FILE_BYTES as u64)
            .map_err(WorkerError::Io)?;
        match index.replace_private_regular_exact(CURSOR_FILE, &expected, &bytes) {
            Ok(()) => return Ok(true),
            Err(error) if error.raw_os_error() == Some(libc::ESTALE) => continue,
            Err(error) => return Err(WorkerError::Io(error)),
        }
    }
    Ok(false)
}

fn try_lock_receipt(index: &RootedDir, name: &str) -> Result<Option<File>, WorkerError> {
    let file = index
        .open_existing_private_lock(name)
        .map_err(WorkerError::Io)?;
    match cvt(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) }) {
        Ok(()) => Ok(Some(file)),
        Err(error) if lock_would_block(&error) => Ok(None),
        Err(error) => Err(WorkerError::Io(error)),
    }
}

fn task_turn_ids_from_snapshot(snapshot: &QueueSnapshot) -> HashSet<JobId> {
    snapshot
        .entries()
        .iter()
        .filter(|entry| entry.kind() == QueueEntryKind::TaskTurn)
        .map(QueueEntry::job_id)
        .collect()
}

fn record_referenced_turn_ids(record: &LocalTaskRecord) -> Vec<JobId> {
    let mut ids: Vec<JobId> = record
        .status()
        .turns()
        .iter()
        .map(TurnSummary::turn_id)
        .collect();
    if let Some(turn_id) = record.submission_intent_turn_id() {
        ids.push(turn_id);
    }
    if let Some(turn_id) = record.submission_rollback_turn_id() {
        ids.push(turn_id);
    }
    ids
}

fn record_has_relevant_queue_turn(record: &LocalTaskRecord, queued: &HashSet<JobId>) -> bool {
    record_referenced_turn_ids(record)
        .into_iter()
        .any(|turn_id| queued.contains(&turn_id))
}

fn should_keep_active_index(record: &LocalTaskRecord, queued: &HashSet<JobId>) -> bool {
    task_record_needs_active_index(record) || record_has_relevant_queue_turn(record, queued)
}
