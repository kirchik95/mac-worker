//! Notification policy and the private per-target cache.
//!
//! Decisions are an evicting ring of title-free fingerprints. The attention
//! overflow fingerprint is stored separately so a cold start cannot re-alert
//! an unchanged set after those decisions fall out of the ring. The cache is
//! saved before any channel runs.

use std::{collections::HashSet, fs::File, io, os::fd::AsRawFd, path::PathBuf, sync::Arc};

use sha2::{Digest, Sha256};

use crate::{
    config::ControllerConfig,
    controller::events::contracts::{
        AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventRuntime,
        MAX_DISPLAY_TITLE_BYTES, MAX_NOTIFY_STATE_BYTES, NOTICE_CHANNEL_BUDGET,
        NOTIFY_COALESCE_AFTER, NOTIFY_COALESCE_COUNT, NOTIFY_DECISION_CAPACITY,
        NOTIFY_PENDING_CAPACITY, Notice, NoticeChannel, NoticeSound, NotifyOptions, NotifyPlan,
        NotifyState, PendingCandidate, Reconciliation, RepairProgress, SCHEMA_VERSION, SafeCode,
        SafeOutcome, TaskFacts,
    },
    error::WorkerError,
    paths::PathLayout,
    redaction::RedactionBoundary,
    rooted_fs::RootedDir,
    task::TaskId,
};

const COALESCE_GAP_MILLIS: u64 = NOTIFY_COALESCE_AFTER.as_millis() as u64;
const LOCK_FILE: &str = "notify.lock";
const STATE_FILE: &str = "notify.json";
const MAX_NOTIFY_BYTES: u64 = MAX_NOTIFY_STATE_BYTES as u64;

struct Decision {
    fingerprint: String,
    task_id: TaskId,
    title: Option<String>,
    label: &'static str,
    sound: NoticeSound,
    attention: bool,
    done: bool,
}

pub fn plan_notifications(
    saved: &NotifyState,
    result: &Reconciliation,
    options: &NotifyOptions,
    now_millis: u64,
) -> NotifyPlan {
    let mut next = saved.clone();
    next.schema_version = SCHEMA_VERSION;
    if result.consumed_after.is_some() {
        next.consumed_after = result.consumed_after;
    }

    let fresh = collect_fresh(result, saved);
    if result.baseline == BaselineKind::Cold {
        for facts in &result.confirmed {
            if let Some(decision) = decision_of(facts) {
                remember(&mut next.decisions, &decision.fingerprint);
            }
        }
    }
    for decision in &fresh {
        remember(&mut next.decisions, &decision.fingerprint);
    }

    let repair_complete = repair_complete(result);
    let attention_count = attention_count(result);
    let next_fingerprint = if repair_complete {
        Some(resolved_attention_fingerprint(result))
    } else {
        saved.attention_overflow.clone()
    };
    let fingerprint_changed = next_fingerprint != saved.attention_overflow;
    if repair_complete {
        next.attention_overflow = next_fingerprint.clone();
        next.last_complete_repair_millis = Some(now_millis);
    }

    let mut eligible = HashSet::new();
    for facts in &result.confirmed {
        if decision_of(facts).is_some() {
            eligible.insert(facts.task_id);
        }
    }
    for decision in &fresh {
        eligible.insert(decision.task_id);
    }
    let (pending, pending_overflow) = next_pending(saved, result, &eligible);
    next.pending = pending;
    next.repair_needed = !repair_complete || pending_overflow;

    let mut notices = Vec::new();
    if !options.quiet {
        let epoch_changed = epoch_changed(saved, result);
        let disconnected = saved
            .last_complete_repair_millis
            .is_some_and(|then| now_millis.saturating_sub(then) > COALESCE_GAP_MILLIS);
        let batch = epoch_changed || disconnected || fresh.len() > NOTIFY_COALESCE_COUNT;
        let attention_new = fingerprint_changed && attention_count > 0;
        let summarize_attention = attention_new
            && (result.baseline == BaselineKind::Cold || attention_count > NOTIFY_PENDING_CAPACITY);
        let boundary = RedactionBoundary::from_env();
        if batch && (!fresh.is_empty() || attention_new) {
            notices.push(summary_notice(
                &fresh,
                next_fingerprint.as_deref(),
                if fresh.is_empty() {
                    attention_count
                } else {
                    fresh.len()
                },
                attention_new,
            ));
        } else {
            if summarize_attention {
                notices.push(summary_notice(
                    &[],
                    next_fingerprint.as_deref(),
                    attention_count,
                    true,
                ));
            }
            for decision in &fresh {
                if summarize_attention && decision.attention {
                    continue;
                }
                notices.push(individual_notice(decision, options, &boundary));
            }
        }
    }

    NotifyPlan { next, notices }
}

fn collect_fresh(result: &Reconciliation, saved: &NotifyState) -> Vec<Decision> {
    let cold = result.baseline == BaselineKind::Cold;
    let mut fresh = Vec::new();
    for change in &result.changes {
        let Some(decision) = fresh_decision(change, saved, cold) else {
            continue;
        };
        if fresh
            .iter()
            .any(|existing: &Decision| existing.fingerprint == decision.fingerprint)
        {
            continue;
        }
        fresh.push(decision);
    }
    fresh
}

fn fresh_decision(change: &DerivedTaskChange, saved: &NotifyState, cold: bool) -> Option<Decision> {
    if cold
        && !matches!(
            change.cause,
            ChangeCause::ReplayTerminal { .. } | ChangeCause::ReplayAbandoned
        )
    {
        return None;
    }
    if !hint_applies(change) {
        return None;
    }
    let decision = decision_of(change.current.as_ref()?)?;
    if saved
        .decisions
        .iter()
        .any(|existing| existing == &decision.fingerprint)
    {
        return None;
    }
    Some(decision)
}

fn hint_applies(change: &DerivedTaskChange) -> bool {
    let Some(current) = change.current.as_ref() else {
        return false;
    };
    if current.task_id != change.task_id {
        return false;
    }
    match change.cause {
        ChangeCause::ReplayTerminal { turn_id, outcome } => {
            current.latest_turn_id == Some(turn_id) && current.outcome == Some(outcome)
        }
        ChangeCause::ReplayAbandoned => {
            current.state == "abandoned"
                && current.latest_turn_id.is_none()
                && current.outcome.is_none()
        }
        ChangeCause::RepairDifference => true,
    }
}

fn decision_of(facts: &TaskFacts) -> Option<Decision> {
    let signature = facts.eligibility_signature();
    if signature.quiescent != Some(true) {
        return None;
    }
    if signature.abandoned_without_turn {
        let code = signature
            .code
            .as_ref()
            .map(SafeCode::as_str)
            .unwrap_or("TURN_FAILED");
        return Some(Decision {
            fingerprint: sha256_hex(&format!("abandon:{}:{code}", facts.task_id)),
            task_id: facts.task_id,
            title: facts.title.clone(),
            label: "Abandoned",
            sound: NoticeSound::None,
            attention: false,
            done: false,
        });
    }
    let outcome = signature.outcome?;
    let turn_id = signature.latest_turn_id?;
    Some(Decision {
        fingerprint: sha256_hex(&format!(
            "turn:{}:{turn_id}:{}",
            facts.task_id,
            outcome_token(outcome)
        )),
        task_id: facts.task_id,
        title: facts.title.clone(),
        label: outcome_label(outcome),
        sound: sound_for_outcome(outcome),
        attention: signature.current_attention,
        done: outcome == SafeOutcome::Done,
    })
}

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn outcome_token(outcome: SafeOutcome) -> &'static str {
    match outcome {
        SafeOutcome::Done => "done",
        SafeOutcome::NeedsInput => "needs_input",
        SafeOutcome::Blocked => "blocked",
        SafeOutcome::Unknown => "unknown",
        SafeOutcome::Failed => "failed",
        SafeOutcome::Cancelled => "cancelled",
        SafeOutcome::TimedOut => "timed_out",
        SafeOutcome::Lost => "lost",
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn sound_for_outcome(outcome: SafeOutcome) -> NoticeSound {
    match outcome {
        SafeOutcome::Done => NoticeSound::Done,
        SafeOutcome::NeedsInput | SafeOutcome::Blocked => NoticeSound::Request,
        SafeOutcome::Unknown
        | SafeOutcome::Failed
        | SafeOutcome::Cancelled
        | SafeOutcome::TimedOut
        | SafeOutcome::Lost => NoticeSound::None,
    }
}

fn outcome_label(outcome: SafeOutcome) -> &'static str {
    match outcome {
        SafeOutcome::Done => "Done",
        SafeOutcome::NeedsInput => "Needs input",
        SafeOutcome::Blocked => "Blocked",
        SafeOutcome::Unknown => "Unknown",
        SafeOutcome::Failed => "Failed",
        SafeOutcome::Cancelled => "Cancelled",
        SafeOutcome::TimedOut => "Timed out",
        SafeOutcome::Lost => "Lost",
    }
}

fn remember(decisions: &mut Vec<String>, fingerprint: &str) {
    if decisions.iter().any(|existing| existing == fingerprint) {
        return;
    }
    decisions.push(fingerprint.to_owned());
    if decisions.len() > NOTIFY_DECISION_CAPACITY {
        let overflow = decisions.len() - NOTIFY_DECISION_CAPACITY;
        decisions.drain(0..overflow);
    }
}

fn repair_complete(result: &Reconciliation) -> bool {
    result.repair == RepairProgress::Complete && !result.repair_needed
}

fn attention_count(result: &Reconciliation) -> usize {
    result
        .attention
        .as_ref()
        .map(|summary| summary.count)
        .unwrap_or_else(|| {
            result
                .confirmed
                .iter()
                .filter(|facts| decision_of(facts).is_some_and(|decision| decision.attention))
                .count()
        })
}

fn resolved_attention_fingerprint(result: &Reconciliation) -> String {
    if let Some(AttentionSummary { fingerprint, .. }) = &result.attention
        && is_digest(fingerprint)
    {
        return fingerprint.clone();
    }
    attention_fingerprint(&result.confirmed)
}

fn attention_fingerprint(facts: &[TaskFacts]) -> String {
    let mut lines = Vec::new();
    for facts in facts {
        let Some(decision) = decision_of(facts) else {
            continue;
        };
        if !decision.attention {
            continue;
        }
        let turn = facts
            .latest_turn_id
            .map(|turn| turn.to_string())
            .unwrap_or_else(|| "-".to_owned());
        let label = facts.outcome.map(outcome_token).unwrap_or("abandoned");
        lines.push(format!("{}|{turn}|{label}", facts.task_id));
    }
    lines.sort();
    lines.dedup();
    let mut payload = format!("count={}\n", lines.len());
    for line in &lines {
        payload.push_str(line);
        payload.push('\n');
    }
    format!("{:x}", Sha256::digest(payload.as_bytes()))
}

fn epoch_changed(saved: &NotifyState, result: &Reconciliation) -> bool {
    match (&saved.consumed_after, &result.consumed_after) {
        (Some(saved_cursor), Some(next_cursor)) => {
            saved_cursor.journal_id != next_cursor.journal_id
        }
        _ => false,
    }
}

fn push_pending(
    pending: &mut Vec<PendingCandidate>,
    seen: &mut HashSet<TaskId>,
    eligible: &HashSet<TaskId>,
    candidate: PendingCandidate,
) {
    if eligible.contains(&candidate.task_id) || !seen.insert(candidate.task_id) {
        return;
    }
    pending.push(candidate);
}

fn next_pending(
    saved: &NotifyState,
    result: &Reconciliation,
    eligible: &HashSet<TaskId>,
) -> (Vec<PendingCandidate>, bool) {
    let mut pending = Vec::new();
    let mut seen = HashSet::new();
    if !repair_complete(result) {
        for candidate in &saved.pending {
            push_pending(&mut pending, &mut seen, eligible, candidate.clone());
        }
    }
    for task_id in &result.pending_ids {
        push_pending(
            &mut pending,
            &mut seen,
            eligible,
            PendingCandidate {
                task_id: *task_id,
                turn_id: None,
            },
        );
    }
    for facts in &result.confirmed {
        if decision_of(facts).is_none() {
            push_pending(
                &mut pending,
                &mut seen,
                eligible,
                PendingCandidate {
                    task_id: facts.task_id,
                    turn_id: facts.latest_turn_id,
                },
            );
        }
    }
    pending.sort_by_key(|candidate| candidate.task_id.to_string());
    let overflow = pending.len() > NOTIFY_PENDING_CAPACITY;
    pending.truncate(NOTIFY_PENDING_CAPACITY);
    (pending, overflow)
}

fn individual_notice(
    decision: &Decision,
    options: &NotifyOptions,
    boundary: &RedactionBoundary,
) -> Notice {
    Notice {
        fingerprint: decision.fingerprint.clone(),
        title: decision.label.to_owned(),
        body: notice_body(
            decision.task_id,
            decision.title.as_deref(),
            options,
            boundary,
        ),
        sound: decision.sound,
    }
}

fn notice_body(
    task_id: TaskId,
    title: Option<&str>,
    options: &NotifyOptions,
    boundary: &RedactionBoundary,
) -> String {
    let id = task_id.to_string();
    if options.no_titles {
        return id;
    }
    let Some(title) = title.map(str::trim).filter(|title| !title.is_empty()) else {
        return id;
    };
    let safe = boundary.text(title, MAX_DISPLAY_TITLE_BYTES);
    if safe.is_empty() {
        id
    } else {
        format!("{id} {safe}")
    }
}

fn summary_notice(
    fresh: &[Decision],
    attention_fingerprint: Option<&str>,
    count: usize,
    attention_new: bool,
) -> Notice {
    let sound = if attention_new || fresh.iter().any(|decision| decision.attention) {
        NoticeSound::Request
    } else if !fresh.is_empty() && fresh.iter().all(|decision| decision.done) {
        NoticeSound::Done
    } else {
        NoticeSound::None
    };
    let title = match sound {
        NoticeSound::Request => "Tasks need attention",
        NoticeSound::Done => "Tasks finished",
        NoticeSound::None => "Tasks updated",
    };
    let fingerprint = if fresh.is_empty() {
        attention_fingerprint
            .filter(|value| is_digest(value))
            .unwrap_or("0000000000000000000000000000000000000000000000000000000000000000")
            .to_owned()
    } else {
        batch_fingerprint(fresh)
    };
    let body = if count == 1 {
        "1 task".to_owned()
    } else {
        format!("{count} tasks")
    };
    Notice {
        fingerprint,
        title: title.to_owned(),
        body,
        sound,
    }
}

fn batch_fingerprint(fresh: &[Decision]) -> String {
    let mut lines: Vec<&str> = fresh
        .iter()
        .map(|decision| decision.fingerprint.as_str())
        .collect();
    lines.sort_unstable();
    let mut payload = format!("batch={}\n", lines.len());
    for line in lines {
        payload.push_str(line);
        payload.push('\n');
    }
    format!("{:x}", Sha256::digest(payload.as_bytes()))
}

pub struct NotifyCache {
    root: RootedDir,
    _lock: File,
}

impl std::fmt::Debug for NotifyCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NotifyCache")
    }
}

impl NotifyCache {
    pub fn open(paths: &PathLayout, controller: &ControllerConfig) -> Result<Self, WorkerError> {
        let directory = cache_directory(paths, controller);
        let root = RootedDir::open_or_create_anchored_absolute(&directory)?;
        require_private_dir(root.path())?;
        let lock = root.open_private_lock(LOCK_FILE).map_err(cache_io)?;
        try_lock_exclusive(&lock)?;
        Ok(Self { root, _lock: lock })
    }

    pub fn load(&self) -> Result<NotifyState, WorkerError> {
        if !self.root.entry_exists(STATE_FILE).map_err(cache_io)? {
            return Ok(NotifyState::empty());
        }
        let bytes = self
            .root
            .read_private_regular(STATE_FILE, MAX_NOTIFY_BYTES)
            .map_err(cache_io)?;
        decode_state(&bytes)
    }

    pub fn save(&self, state: &NotifyState) -> Result<(), WorkerError> {
        validate_state(state)?;
        let bytes = serde_json::to_vec(state).map_err(|_| corrupt_cache())?;
        if bytes.len() as u64 > MAX_NOTIFY_BYTES {
            return Err(corrupt_cache());
        }
        if self.root.entry_exists(STATE_FILE).map_err(cache_io)? {
            let previous = self
                .root
                .read_private_regular(STATE_FILE, MAX_NOTIFY_BYTES)
                .map_err(cache_io)?;
            self.root
                .replace_private_regular_exact(STATE_FILE, &previous, &bytes)
                .map_err(cache_io)?;
        } else {
            self.root
                .write_private_atomic_no_replace(STATE_FILE, &bytes)
                .map_err(cache_io)?;
        }
        Ok(())
    }

    pub fn rebaseline(&self) -> Result<NotifyState, WorkerError> {
        let state = NotifyState::empty();
        self.save(&state)?;
        Ok(state)
    }
}

pub fn commit_then_deliver(
    cache: &NotifyCache,
    plan: NotifyPlan,
    channels: &[Arc<dyn NoticeChannel>],
    runtime: &dyn EventRuntime,
) -> Result<Vec<bool>, WorkerError> {
    cache.save(&plan.next)?;
    if runtime.cancelled() || plan.notices.is_empty() || channels.is_empty() {
        return Ok(Vec::new());
    }
    let mut results = Vec::new();
    for notice in &plan.notices {
        if runtime.cancelled() {
            break;
        }
        for channel in channels {
            match channel.deliver(notice, NOTICE_CHANNEL_BUDGET) {
                Ok(()) => results.push(true),
                Err(_) => results.push(false),
            }
        }
    }
    Ok(results)
}

pub(crate) fn target_digest(controller: &ControllerConfig) -> String {
    let mut hasher = Sha256::new();
    hasher.update(controller.ssh.as_bytes());
    hasher.update([0xff]);
    hasher.update(controller.remote_binary.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(crate) fn cache_directory(paths: &PathLayout, controller: &ControllerConfig) -> PathBuf {
    paths
        .controller_cache_root()
        .join("events")
        .join(target_digest(controller))
}

fn require_private_dir(path: &std::path::Path) -> Result<(), WorkerError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = std::fs::symlink_metadata(path)?;
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.file_type().is_symlink()
        || !metadata.file_type().is_dir()
        || mode & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(unsafe_cache());
    }
    Ok(())
}

fn try_lock_exclusive(file: &File) -> Result<(), WorkerError> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::EWOULDBLOCK) || error.raw_os_error() == Some(libc::EAGAIN)
    {
        return Err(WorkerError::Protocol(
            "CONTROLLER_EVENTS_NOTIFY_LOCK_HELD: another notifier owns this controller cache"
                .into(),
        ));
    }
    Err(WorkerError::Io(error))
}

fn decode_state(bytes: &[u8]) -> Result<NotifyState, WorkerError> {
    let state: NotifyState = serde_json::from_slice(bytes).map_err(|_| corrupt_cache())?;
    validate_state(&state)?;
    Ok(state)
}

fn validate_state(state: &NotifyState) -> Result<(), WorkerError> {
    state.validate().map_err(|_| corrupt_cache())
}

fn cache_io(error: io::Error) -> WorkerError {
    match error.kind() {
        io::ErrorKind::PermissionDenied => unsafe_cache(),
        io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => corrupt_cache(),
        _ => WorkerError::Io(error),
    }
}

fn unsafe_cache() -> WorkerError {
    WorkerError::Protocol(
        "CONTROLLER_EVENTS_NOTIFY_CACHE_UNSAFE: notify cache is not an owner-only directory".into(),
    )
}

fn corrupt_cache() -> WorkerError {
    WorkerError::Protocol(
        "CONTROLLER_EVENTS_NOTIFY_CACHE_CORRUPT: notify cache was discarded and must rebaseline"
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use uuid::Uuid;

    use super::{cache_directory, commit_then_deliver, plan_notifications};
    use crate::{
        config::ControllerConfig,
        controller::events::contracts::{
            AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventCursor,
            EventRuntime, NOTIFY_DECISION_CAPACITY, Notice, NoticeChannel, NoticeSound,
            NotifyOptions, NotifyState, Reconciliation, RepairProgress, SafeCode, SafeOutcome, Seq,
            TaskFacts,
        },
        error::WorkerError,
        paths::PathLayout,
        task::{TaskId, TurnId},
    };

    struct ManualRuntime {
        cancelled: bool,
    }

    impl EventRuntime for ManualRuntime {
        fn now(&self) -> Duration {
            Duration::ZERO
        }
        fn sleep(&self, _: Duration) {}
        fn cancelled(&self) -> bool {
            self.cancelled
        }
    }

    struct RecordingChannel {
        notices: Mutex<Vec<Notice>>,
        fail: bool,
    }

    impl NoticeChannel for RecordingChannel {
        fn deliver(&self, notice: &Notice, _: Duration) -> Result<(), WorkerError> {
            if self.fail {
                return Err(WorkerError::Unavailable(
                    "CONTROLLER_EVENTS_NOTIFY_CHANNEL: failed".into(),
                ));
            }
            self.notices.lock().expect("notices").push(notice.clone());
            Ok(())
        }
    }

    struct Harness {
        root: tempfile::TempDir,
        paths: PathLayout,
        controller: ControllerConfig,
        cache: Option<super::NotifyCache>,
        saved: NotifyState,
        journal_id: Uuid,
        delivered: Vec<Notice>,
        delivered_mark: usize,
        evicted_task: TaskId,
        evicted_turn: TurnId,
        overflow: Option<String>,
        now: u64,
        interrupt: bool,
    }

    impl Harness {
        fn new() -> Self {
            let root = tempfile::tempdir().expect("tempdir");
            let paths = layout(root.path());
            let controller = controller("notifier@cache-host");
            let cache = super::NotifyCache::open(&paths, &controller).expect("cache");
            let journal_id = Uuid::from_u128(7);
            let mut saved = NotifyState::empty();
            saved.consumed_after = Some(EventCursor {
                journal_id,
                seq: Seq::new(1),
            });
            cache.save(&saved).expect("seed");
            Self {
                root,
                paths,
                controller,
                cache: Some(cache),
                saved,
                journal_id,
                delivered: Vec::new(),
                delivered_mark: 0,
                evicted_task: TaskId::new(Uuid::from_u128(1)),
                evicted_turn: TurnId::new(Uuid::from_u128(100_001)),
                overflow: None,
                now: 10_000,
                interrupt: false,
            }
        }

        fn consume(&mut self, result: Reconciliation, options: NotifyOptions) {
            let plan = plan_notifications(&self.saved, &result, &options, self.now);
            self.now += 1_000;
            if self.interrupt {
                let cache = self.cache.as_ref().expect("cache");
                cache.save(&plan.next).expect("save");
                self.saved = cache.load().expect("load");
                self.interrupt = false;
                return;
            }
            let recorded = Arc::new(RecordingChannel {
                notices: Mutex::new(Vec::new()),
                fail: false,
            });
            let channel: Arc<dyn NoticeChannel> = recorded.clone();
            let delivered = commit_then_deliver(
                self.cache.as_ref().expect("cache"),
                plan,
                std::slice::from_ref(&channel),
                &ManualRuntime { cancelled: false },
            )
            .expect("deliver");
            assert!(delivered.iter().all(|ok| *ok));
            self.delivered
                .extend(recorded.notices.lock().expect("notices").clone());
            self.saved = self.cache.as_ref().expect("cache").load().expect("reload");
        }

        fn consume_more_than_4096_decisions(&mut self) {
            let mut changes = Vec::with_capacity(4_097);
            let mut confirmed = Vec::with_capacity(4_097);
            for index in 1..=4_097_u128 {
                let task_id = TaskId::new(Uuid::from_u128(index));
                let turn_id = TurnId::new(Uuid::from_u128(100_000 + index));
                if index == 1 {
                    self.evicted_task = task_id;
                    self.evicted_turn = turn_id;
                }
                let facts = done_facts(task_id, turn_id);
                changes.push(change(facts.clone()));
                confirmed.push(facts);
            }
            self.consume(
                warm_complete(self.journal_id, 2, changes, confirmed),
                NotifyOptions::default(),
            );
            assert_eq!(self.saved.decisions.len(), NOTIFY_DECISION_CAPACITY);
            let evicted = decision_text(&done_facts(self.evicted_task, self.evicted_turn));
            assert!(
                !self
                    .saved
                    .decisions
                    .iter()
                    .any(|decision| decision == &evicted)
            );
        }

        fn consume_attention_overflow_quiet(&mut self) {
            let fingerprint = format!("{:064x}", 257_u128);
            self.overflow = Some(fingerprint.clone());
            let mut result = cold_complete(self.journal_id, 3, Vec::new());
            result.attention = Some(AttentionSummary {
                count: 257,
                fingerprint: fingerprint.clone(),
            });
            self.consume(
                result,
                NotifyOptions {
                    quiet: true,
                    ..NotifyOptions::default()
                },
            );
            assert_eq!(
                self.saved.attention_overflow.as_deref(),
                Some(fingerprint.as_str())
            );
        }

        fn restart_with_valid_cursor(&mut self) {
            assert!(self.saved.consumed_after.is_some());
            self.cache = None;
            self.cache =
                Some(super::NotifyCache::open(&self.paths, &self.controller).expect("reopen"));
            self.saved = self.cache.as_ref().expect("cache").load().expect("reload");
            self.delivered_mark = self.delivered.len();
        }

        fn consume_cold_history_with_lost_hint(&mut self) {
            let facts = done_facts(self.evicted_task, self.evicted_turn);
            let mut result = cold_complete(self.journal_id, 4, vec![facts]);
            result.attention =
                self.saved
                    .attention_overflow
                    .clone()
                    .map(|fingerprint| AttentionSummary {
                        count: 257,
                        fingerprint,
                    });
            result.consumed_after = self.saved.consumed_after;
            self.consume(result, NotifyOptions::default());
        }

        fn change_epoch(&mut self) {
            self.journal_id = Uuid::from_u128(8);
            if let Some(cursor) = self.saved.consumed_after.as_mut() {
                cursor.journal_id = self.journal_id;
                cursor.seq = Seq::ZERO;
            }
            let decisions = self.saved.decisions.clone();
            let overflow = self.saved.attention_overflow.clone();
            let cache = self.cache.as_ref().expect("cache");
            cache.save(&self.saved).expect("epoch save");
            self.saved = cache.load().expect("epoch load");
            assert_eq!(self.saved.decisions, decisions);
            assert_eq!(self.saved.attention_overflow, overflow);
        }

        fn consume_unchanged_attention_overflow(&mut self) {
            let fingerprint = self.overflow.clone().expect("overflow");
            let mut result = cold_complete(self.journal_id, 1, Vec::new());
            result.attention = Some(AttentionSummary {
                count: 257,
                fingerprint,
            });
            self.consume(result, NotifyOptions::default());
        }

        fn overflow_fingerprint(&self) -> Option<String> {
            self.saved.attention_overflow.clone()
        }

        fn delivered_since_restart(&self) -> usize {
            self.delivered.len().saturating_sub(self.delivered_mark)
        }
    }

    fn layout(root: &std::path::Path) -> PathLayout {
        PathLayout {
            config: root.join("config.toml"),
            state: root.join("state"),
            cache: root.join("cache"),
            data: root.join("data"),
        }
    }

    fn controller(ssh: &str) -> ControllerConfig {
        ControllerConfig {
            enabled: true,
            ssh: ssh.to_owned(),
            remote_binary: "~/.local/bin/worker".into(),
        }
    }

    fn done_facts(task_id: TaskId, turn_id: TurnId) -> TaskFacts {
        proved(task_id, Some(turn_id), "closed", Some(SafeOutcome::Done))
    }

    fn proved(
        task_id: TaskId,
        turn_id: Option<TurnId>,
        state: &str,
        outcome: Option<SafeOutcome>,
    ) -> TaskFacts {
        TaskFacts {
            task_id,
            run_id: None,
            state: state.to_owned(),
            latest_turn_id: turn_id,
            outcome,
            code: None,
            runner_present: false,
            close_intent: false,
            auto_continue_intent: false,
            queue_dispatching: Some(false),
            result_imported: true,
            busy: Some(false),
            quiescent: Some(true),
            fact_digest: "a".repeat(64),
            title: None,
        }
    }

    fn change(facts: TaskFacts) -> DerivedTaskChange {
        DerivedTaskChange {
            task_id: facts.task_id,
            previous: None,
            current: Some(facts),
            cause: ChangeCause::RepairDifference,
        }
    }

    fn warm_complete(
        journal_id: Uuid,
        seq: u64,
        changes: Vec<DerivedTaskChange>,
        confirmed: Vec<TaskFacts>,
    ) -> Reconciliation {
        Reconciliation {
            consumed_after: Some(EventCursor {
                journal_id,
                seq: Seq::new(seq),
            }),
            baseline: BaselineKind::Warm,
            changes,
            confirmed,
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        }
    }

    fn cold_complete(journal_id: Uuid, seq: u64, confirmed: Vec<TaskFacts>) -> Reconciliation {
        Reconciliation {
            consumed_after: Some(EventCursor {
                journal_id,
                seq: Seq::new(seq),
            }),
            baseline: BaselineKind::Cold,
            changes: Vec::new(),
            confirmed,
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        }
    }

    fn decision_text(facts: &TaskFacts) -> String {
        super::sha256_hex(&format!(
            "turn:{}:{}:{}",
            facts.task_id,
            facts.latest_turn_id.expect("turn"),
            super::outcome_token(facts.outcome.expect("outcome"))
        ))
    }

    fn plan_warm(facts: TaskFacts) -> crate::controller::events::contracts::NotifyPlan {
        let saved = NotifyState::empty();
        plan_notifications(
            &saved,
            &warm_complete(
                Uuid::from_u128(9),
                1,
                vec![change(facts.clone())],
                vec![facts],
            ),
            &NotifyOptions::default(),
            5_000,
        )
    }

    #[test]
    fn evicted_history_and_unchanged_overflow_do_not_replay() {
        let mut harness = Harness::new();
        harness.consume_more_than_4096_decisions();
        harness.consume_attention_overflow_quiet();
        let fingerprint = harness.overflow_fingerprint();
        harness.restart_with_valid_cursor();
        harness.consume_cold_history_with_lost_hint();
        harness.change_epoch();
        harness.consume_unchanged_attention_overflow();
        assert_eq!(harness.overflow_fingerprint(), fingerprint);
        assert_eq!(harness.delivered_since_restart(), 0);
        let _ = harness.root;
    }

    #[test]
    fn replay_hint_after_saved_cursor_notifies_fresh_eligibility() {
        let mut harness = Harness::new();
        let historical = done_facts(
            TaskId::new(Uuid::from_u128(20)),
            TurnId::new(Uuid::from_u128(21)),
        );
        harness.consume(
            cold_complete(harness.journal_id, 2, vec![historical]),
            NotifyOptions::default(),
        );
        assert!(harness.delivered.is_empty());
        harness.restart_with_valid_cursor();
        let task_id = TaskId::new(Uuid::from_u128(30));
        let turn_id = TurnId::new(Uuid::from_u128(31));
        let facts = done_facts(task_id, turn_id);
        let mut result = cold_complete(harness.journal_id, 3, vec![facts.clone()]);
        result.changes.push(DerivedTaskChange {
            task_id,
            previous: None,
            current: Some(facts),
            cause: ChangeCause::ReplayTerminal {
                turn_id,
                outcome: SafeOutcome::Done,
            },
        });
        harness.consume(result, NotifyOptions::default());
        assert_eq!(harness.delivered_since_restart(), 1);
        assert_eq!(
            harness.delivered.last().expect("notice").sound,
            NoticeSound::Done
        );
    }

    #[test]
    fn eight_outcomes_notify_after_confirmed_quiescence() {
        let cases = [
            (SafeOutcome::Done, NoticeSound::Done, "Done", "closed"),
            (
                SafeOutcome::NeedsInput,
                NoticeSound::Request,
                "Needs input",
                "open",
            ),
            (
                SafeOutcome::Blocked,
                NoticeSound::Request,
                "Blocked",
                "open",
            ),
            (SafeOutcome::Unknown, NoticeSound::None, "Unknown", "closed"),
            (SafeOutcome::Failed, NoticeSound::None, "Failed", "closed"),
            (
                SafeOutcome::Cancelled,
                NoticeSound::None,
                "Cancelled",
                "closed",
            ),
            (
                SafeOutcome::TimedOut,
                NoticeSound::None,
                "Timed out",
                "closed",
            ),
            (SafeOutcome::Lost, NoticeSound::None, "Lost", "lost"),
        ];
        for (index, (outcome, sound, label, state)) in cases.into_iter().enumerate() {
            let facts = proved(
                TaskId::new(Uuid::from_u128(200 + index as u128)),
                Some(TurnId::new(Uuid::from_u128(300 + index as u128))),
                state,
                Some(outcome),
            );
            let plan = plan_warm(facts);
            assert_eq!(plan.notices.len(), 1, "{label}");
            assert_eq!(plan.notices[0].sound, sound, "{label}");
            assert_eq!(plan.notices[0].title, label);
        }
    }

    #[test]
    fn open_needs_input_is_attention_and_closed_history_is_not() {
        let open = proved(
            TaskId::new(Uuid::from_u128(40)),
            Some(TurnId::new(Uuid::from_u128(41))),
            "open",
            Some(SafeOutcome::NeedsInput),
        );
        let closed = proved(
            TaskId::new(Uuid::from_u128(42)),
            Some(TurnId::new(Uuid::from_u128(43))),
            "closed",
            Some(SafeOutcome::NeedsInput),
        );
        let open_plan = plan_warm(open.clone());
        let closed_plan = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(9), 1, vec![closed.clone()]),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(open_plan.notices.len(), 1);
        assert!(closed_plan.notices.is_empty());
        assert_ne!(
            open_plan.next.attention_overflow,
            closed_plan.next.attention_overflow
        );
        assert_eq!(closed_plan.next.decisions, vec![decision_text(&closed)]);
        let _ = (open, closed);
    }

    #[test]
    fn abandonment_without_terminal_uses_stable_code_and_no_sound() {
        let mut facts = proved(TaskId::new(Uuid::from_u128(50)), None, "abandoned", None);
        facts.code = Some(SafeCode::from_public_code("LEFT"));
        let plan = plan_warm(facts.clone());
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].sound, NoticeSound::None);
        assert_eq!(plan.notices[0].title, "Abandoned");
        assert_eq!(
            plan.notices[0].fingerprint,
            super::sha256_hex(&format!("abandon:{}:TURN_FAILED", facts.task_id))
        );
        let stored = serde_json::to_string(&plan.next).expect("json");
        assert!(!stored.contains("LEFT"));
    }

    #[test]
    fn busy_unknown_continuation_close_runner_and_dispatch_suppress() {
        let mutators: &[fn(&mut TaskFacts)] = &[
            |facts| facts.busy = Some(true),
            |facts| facts.busy = None,
            |facts| facts.quiescent = None,
            |facts| facts.quiescent = Some(false),
            |facts| facts.auto_continue_intent = true,
            |facts| facts.close_intent = true,
            |facts| facts.runner_present = true,
            |facts| facts.queue_dispatching = Some(true),
            |facts| facts.queue_dispatching = None,
            |facts| facts.state = "active".into(),
        ];
        for (index, mutate) in mutators.iter().enumerate() {
            let mut facts = done_facts(
                TaskId::new(Uuid::from_u128(400 + index as u128)),
                TurnId::new(Uuid::from_u128(500 + index as u128)),
            );
            mutate(&mut facts);
            let task_id = facts.task_id;
            let plan = plan_warm(facts);
            assert!(plan.notices.is_empty(), "case {index}");
            assert!(
                plan.next
                    .pending
                    .iter()
                    .any(|candidate| candidate.task_id == task_id),
                "case {index}"
            );
        }
    }

    #[test]
    fn superseded_hint_does_not_notify_and_latest_turn_can_replace() {
        let task_id = TaskId::new(Uuid::from_u128(60));
        let old_turn = TurnId::new(Uuid::from_u128(61));
        let new_turn = TurnId::new(Uuid::from_u128(62));
        let current = done_facts(task_id, new_turn);
        let mut result = warm_complete(Uuid::from_u128(9), 1, Vec::new(), vec![current.clone()]);
        result.changes.push(DerivedTaskChange {
            task_id,
            previous: None,
            current: Some(current.clone()),
            cause: ChangeCause::ReplayTerminal {
                turn_id: old_turn,
                outcome: SafeOutcome::Done,
            },
        });
        let suppressed = plan_notifications(
            &NotifyState::empty(),
            &result,
            &NotifyOptions::default(),
            5_000,
        );
        assert!(suppressed.notices.is_empty());
        assert!(suppressed.next.decisions.is_empty());

        let first = plan_warm(done_facts(task_id, old_turn));
        assert_eq!(first.notices.len(), 1);
        let replacement = plan_notifications(
            &first.next,
            &warm_complete(
                Uuid::from_u128(9),
                2,
                vec![change(current.clone())],
                vec![current.clone()],
            ),
            &NotifyOptions::default(),
            6_000,
        );
        assert_eq!(replacement.notices.len(), 1);
        assert_eq!(replacement.notices[0].fingerprint, decision_text(&current));
        assert_eq!(replacement.next.decisions.len(), 2);
    }

    #[test]
    fn warm_busy_to_quiescent_notifies_and_unchanged_eviction_stays_silent() {
        let task_id = TaskId::new(Uuid::from_u128(70));
        let turn_id = TurnId::new(Uuid::from_u128(71));
        let mut busy = done_facts(task_id, turn_id);
        busy.busy = Some(true);
        busy.quiescent = Some(false);
        let ready = done_facts(task_id, turn_id);
        let result = Reconciliation {
            consumed_after: Some(EventCursor {
                journal_id: Uuid::from_u128(9),
                seq: Seq::new(1),
            }),
            baseline: BaselineKind::Warm,
            changes: vec![DerivedTaskChange {
                task_id,
                previous: Some(busy),
                current: Some(ready.clone()),
                cause: ChangeCause::RepairDifference,
            }],
            confirmed: vec![ready],
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        };
        let plan = plan_notifications(
            &NotifyState::empty(),
            &result,
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(plan.notices.len(), 1);

        let mut harness = Harness::new();
        harness.consume_more_than_4096_decisions();
        let before = harness.delivered.len();
        let evicted = done_facts(harness.evicted_task, harness.evicted_turn);
        let fingerprint = decision_text(&evicted);
        harness.consume(
            warm_complete(harness.journal_id, 9, Vec::new(), vec![evicted]),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered.len(), before);
        assert!(
            !harness
                .saved
                .decisions
                .iter()
                .any(|decision| decision == &fingerprint)
        );
    }

    #[test]
    fn present_empty_warm_projection_is_not_a_cold_baseline() {
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(80)),
            TurnId::new(Uuid::from_u128(81)),
        );
        let warm = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, Vec::new(), vec![facts.clone()]),
            &NotifyOptions::default(),
            5_000,
        );
        let cold = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(9), 1, vec![facts]),
            &NotifyOptions::default(),
            5_000,
        );
        assert!(warm.notices.is_empty());
        assert!(warm.next.decisions.is_empty());
        assert!(cold.notices.is_empty());
        assert_eq!(cold.next.decisions.len(), 1);
    }

    #[test]
    fn incomplete_repair_keeps_the_previous_summary() {
        let saved = NotifyState {
            attention_overflow: Some("keep-me".into()),
            ..NotifyState::empty()
        };
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(90)),
            TurnId::new(Uuid::from_u128(91)),
        );
        let mut result = warm_complete(
            Uuid::from_u128(9),
            4,
            vec![change(facts.clone())],
            vec![facts],
        );
        result.repair = RepairProgress::InProgress;
        result.repair_needed = true;
        result.attention = Some(AttentionSummary {
            count: 3,
            fingerprint: "do-not-claim".into(),
        });
        let plan = plan_notifications(&saved, &result, &NotifyOptions::default(), 5_000);
        assert_eq!(plan.next.attention_overflow.as_deref(), Some("keep-me"));
        assert!(plan.next.repair_needed);
        assert_eq!(plan.notices.len(), 1);
    }

    #[test]
    fn coalesce_over_five_after_disconnect_and_on_epoch_repair() {
        let mut confirmed = Vec::new();
        let mut changes = Vec::new();
        for index in 0..6 {
            let facts = done_facts(
                TaskId::new(Uuid::from_u128(600 + index)),
                TurnId::new(Uuid::from_u128(700 + index)),
            );
            changes.push(change(facts.clone()));
            confirmed.push(facts);
        }
        let batch = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, changes, confirmed),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(batch.notices.len(), 1);
        assert_eq!(batch.notices[0].sound, NoticeSound::Done);
        assert_eq!(batch.notices[0].title, "Tasks finished");
        assert_eq!(batch.notices[0].body, "6 tasks");
        assert_eq!(batch.next.decisions.len(), 6);

        let later = done_facts(
            TaskId::new(Uuid::from_u128(610)),
            TurnId::new(Uuid::from_u128(710)),
        );
        let saved = NotifyState {
            last_complete_repair_millis: Some(0),
            consumed_after: Some(EventCursor {
                journal_id: Uuid::from_u128(9),
                seq: Seq::new(1),
            }),
            ..NotifyState::empty()
        };
        let disconnected = plan_notifications(
            &saved,
            &warm_complete(
                Uuid::from_u128(9),
                2,
                vec![change(later.clone())],
                vec![later],
            ),
            &NotifyOptions::default(),
            60_001,
        );
        assert_eq!(disconnected.notices.len(), 1);
        assert_eq!(disconnected.notices[0].title, "Tasks finished");

        let epoch_facts = done_facts(
            TaskId::new(Uuid::from_u128(611)),
            TurnId::new(Uuid::from_u128(711)),
        );
        let epoch = plan_notifications(
            &saved,
            &warm_complete(
                Uuid::from_u128(99),
                1,
                vec![change(epoch_facts.clone())],
                vec![epoch_facts],
            ),
            &NotifyOptions::default(),
            1_000,
        );
        assert_eq!(epoch.notices.len(), 1);
        assert_eq!(epoch.notices[0].title, "Tasks finished");
        assert_ne!(
            epoch.next.consumed_after.unwrap().journal_id,
            Uuid::from_u128(9)
        );
    }

    #[test]
    fn mixed_and_attention_summaries_use_request_otherwise_none() {
        let mut changes = Vec::new();
        let mut confirmed = Vec::new();
        for index in 0..5 {
            let facts = proved(
                TaskId::new(Uuid::from_u128(800 + index)),
                Some(TurnId::new(Uuid::from_u128(900 + index))),
                "closed",
                Some(SafeOutcome::Failed),
            );
            changes.push(change(facts.clone()));
            confirmed.push(facts);
        }
        let attention = proved(
            TaskId::new(Uuid::from_u128(806)),
            Some(TurnId::new(Uuid::from_u128(906))),
            "open",
            Some(SafeOutcome::Blocked),
        );
        changes.push(change(attention.clone()));
        confirmed.push(attention);
        let plan = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, changes, confirmed),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].sound, NoticeSound::Request);
        assert_eq!(plan.notices[0].title, "Tasks need attention");

        let mut failed_changes = Vec::new();
        let mut failed = Vec::new();
        for index in 0..6 {
            let facts = proved(
                TaskId::new(Uuid::from_u128(820 + index)),
                Some(TurnId::new(Uuid::from_u128(920 + index))),
                "closed",
                Some(SafeOutcome::Failed),
            );
            failed_changes.push(change(facts.clone()));
            failed.push(facts);
        }
        let none = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, failed_changes, failed),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(none.notices[0].sound, NoticeSound::None);
        assert_eq!(none.notices[0].title, "Tasks updated");
    }

    #[test]
    fn quiet_saves_decisions_without_notices_or_a_backlog() {
        let mut harness = Harness::new();
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(80)),
            TurnId::new(Uuid::from_u128(81)),
        );
        harness.consume(
            warm_complete(
                harness.journal_id,
                2,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            NotifyOptions {
                quiet: true,
                ..NotifyOptions::default()
            },
        );
        assert!(harness.delivered.is_empty());
        assert_eq!(harness.saved.decisions.len(), 1);
        harness.restart_with_valid_cursor();
        harness.consume(
            warm_complete(
                harness.journal_id,
                3,
                vec![change(facts.clone())],
                vec![facts],
            ),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered_since_restart(), 0);
    }

    #[test]
    fn titles_default_on_and_no_titles_and_redaction_stay_out_of_the_cache() {
        let mut facts = done_facts(
            TaskId::new(Uuid::from_u128(100)),
            TurnId::new(Uuid::from_u128(101)),
        );
        facts.title = Some("Ship the lock".into());
        let shown = plan_warm(facts.clone());
        assert!(shown.notices[0].body.contains("Ship the lock"));
        assert!(shown.notices[0].body.contains(&facts.task_id.to_string()));
        let hidden = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(
                Uuid::from_u128(9),
                1,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            &NotifyOptions {
                no_titles: true,
                ..NotifyOptions::default()
            },
            5_000,
        );
        assert_eq!(hidden.notices[0].body, facts.task_id.to_string());
        assert!(!hidden.notices[0].title.contains("Ship the lock"));
        let stored = serde_json::to_string(&hidden.next).expect("json");
        assert!(!stored.contains("Ship the lock"));

        let mut secret = facts.clone();
        secret.task_id = TaskId::new(Uuid::from_u128(102));
        secret.title = Some("see /Users/hidden/notes sk-supersecretvalue".into());
        let redacted = plan_warm(secret.clone());
        let body = &redacted.notices[0].body;
        assert!(!body.contains("/Users/hidden"));
        assert!(!body.contains("sk-supersecretvalue"));
        assert!(body.contains("[path]"));
        assert!(body.contains("[token]"));
        let cache_json = serde_json::to_string(&redacted.next).expect("json");
        assert!(!cache_json.contains("sk-supersecretvalue"));
        assert!(!cache_json.contains("/Users/hidden"));

        let mut controlled = secret.clone();
        controlled.task_id = TaskId::new(Uuid::from_u128(103));
        controlled.title = Some("see /Users/hidden/notes\nsk-supersecretvalue\u{0001}".into());
        let blocked = plan_warm(controlled);
        assert!(blocked.notices.is_empty());
        let blocked_json = serde_json::to_string(&blocked.next).expect("json");
        assert!(!blocked_json.contains("sk-supersecretvalue"));
        assert!(!blocked_json.contains('\u{0001}'));

        let mut wide = secret.clone();
        wide.task_id = TaskId::new(Uuid::from_u128(104));
        wide.title = Some("b".repeat(512));
        let capped = plan_warm(wide);
        assert!(!capped.notices[0].body.contains(&"b".repeat(513)));
        assert!(capped.notices[0].body.len() <= 32 + 1 + 512);
        let mut oversized = secret;
        oversized.task_id = TaskId::new(Uuid::from_u128(105));
        oversized.title = Some("c".repeat(513));
        assert!(plan_warm(oversized).notices.is_empty());
    }

    #[test]
    fn attention_fingerprint_ignores_order_epoch_and_persists_below_the_cap() {
        let first = proved(
            TaskId::new(Uuid::from_u128(1)),
            Some(TurnId::new(Uuid::from_u128(2))),
            "open",
            Some(SafeOutcome::NeedsInput),
        );
        let second = proved(
            TaskId::new(Uuid::from_u128(3)),
            Some(TurnId::new(Uuid::from_u128(4))),
            "open",
            Some(SafeOutcome::Blocked),
        );
        let forward = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(11), 1, vec![first.clone(), second.clone()]),
            &NotifyOptions::default(),
            1,
        );
        let backward = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(99), 8, vec![second, first]),
            &NotifyOptions::default(),
            99_000,
        );
        assert_eq!(
            forward.next.attention_overflow,
            backward.next.attention_overflow
        );
        let fingerprint = forward
            .next
            .attention_overflow
            .clone()
            .expect("fingerprint below the cap");
        let journal = Uuid::from_u128(11).to_string();
        assert!(!fingerprint.contains(&journal));
        assert!(
            forward
                .next
                .decisions
                .iter()
                .all(|decision| !decision.contains(&journal))
        );
        assert!(forward.notices.iter().all(|notice| notice.title != "Done"));
    }

    #[test]
    fn duplicate_notifier_lock_exits_without_the_ssh_target() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("user@secret-host");
        let first = super::NotifyCache::open(&paths, &controller).expect("first");
        let error = super::NotifyCache::open(&paths, &controller).expect_err("second");
        let text = error.to_string();
        assert!(text.contains("CONTROLLER_EVENTS_NOTIFY_LOCK_HELD"));
        assert!(!text.contains("secret-host"));
        drop(first);
        super::NotifyCache::open(&paths, &controller).expect("after release");
    }

    #[test]
    fn symlink_and_unsafe_cache_are_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("user@secret-host");
        let events = paths.controller_cache_root().join("events");
        fs::create_dir_all(&events).expect("events");
        let real = events.join("real");
        fs::create_dir(&real).expect("real");
        std::os::unix::fs::symlink("real", events.join(super::target_digest(&controller)))
            .expect("symlink");
        let error = super::NotifyCache::open(&paths, &controller).expect_err("symlink");
        assert!(!error.to_string().contains("secret-host"));

        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let directory = cache_directory(&paths, &controller);
        fs::create_dir_all(&directory).expect("dir");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).expect("mode");
        let error = super::NotifyCache::open(&paths, &controller).expect_err("mode");
        assert!(
            error
                .to_string()
                .contains("CONTROLLER_EVENTS_NOTIFY_CACHE_UNSAFE")
        );
        assert!(!error.to_string().contains("secret-host"));
    }

    #[test]
    fn corrupt_cache_rebaseline_and_schema_bounds() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("notifier@cache-host");
        let cache = super::NotifyCache::open(&paths, &controller).expect("open");
        let file = cache_directory(&paths, &controller).join("notify.json");
        fs::write(&file, b"not-json").expect("write");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("mode");
        let error = cache.load().expect_err("corrupt");
        assert!(
            error
                .to_string()
                .contains("CONTROLLER_EVENTS_NOTIFY_CACHE_CORRUPT")
        );
        let fresh = cache.rebaseline().expect("rebaseline");
        assert!(fresh.decisions.is_empty());
        assert!(cache.load().expect("empty").decisions.is_empty());

        let mut decisions = Vec::new();
        for index in 0..4_097 {
            decisions.push(format!("{index:064x}"));
        }
        let oversized = serde_json::json!({
            "schema_version": 1,
            "consumed_after": null,
            "last_complete_repair_millis": null,
            "decisions": decisions,
            "pending": [],
            "attention_overflow": null,
            "repair_needed": false
        });
        fs::write(&file, oversized.to_string()).expect("overwrite");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("mode");
        assert!(
            cache
                .load()
                .expect_err("bounds")
                .to_string()
                .contains("CACHE_CORRUPT")
        );
    }

    #[test]
    fn save_precedes_channel_and_channel_failure_does_not_replay() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("notifier@cache-host");
        let cache = super::NotifyCache::open(&paths, &controller).expect("open");
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(120)),
            TurnId::new(Uuid::from_u128(121)),
        );
        let fingerprint = decision_text(&facts);
        let plan = plan_warm(facts.clone());
        let directory = cache_directory(&paths, &controller);
        let saw = Arc::new(Mutex::new(false));
        struct Observing {
            path: std::path::PathBuf,
            fingerprint: String,
            saw: Arc<Mutex<bool>>,
        }
        impl NoticeChannel for Observing {
            fn deliver(&self, notice: &Notice, _: Duration) -> Result<(), WorkerError> {
                let text = fs::read_to_string(self.path.join("notify.json")).expect("json");
                assert!(text.contains(&self.fingerprint));
                assert!(text.contains(&notice.fingerprint));
                *self.saw.lock().expect("saw") = true;
                Ok(())
            }
        }
        let channel = Arc::new(Observing {
            path: directory.clone(),
            fingerprint: fingerprint.clone(),
            saw: Arc::clone(&saw),
        });
        let channel: Arc<dyn NoticeChannel> = channel;
        commit_then_deliver(
            &cache,
            plan,
            std::slice::from_ref(&channel),
            &ManualRuntime { cancelled: false },
        )
        .expect("deliver");
        assert!(*saw.lock().expect("saw"));
        let mode = fs::metadata(directory.join("notify.json"))
            .expect("meta")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            fs::metadata(&directory).expect("dir").permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(directory.join("notify.lock"))
                .expect("lock")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let _ = facts;
    }

    #[test]
    fn channel_failure_and_crash_after_save_do_not_replay() {
        let mut harness = Harness::new();
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(130)),
            TurnId::new(Uuid::from_u128(131)),
        );
        let plan = plan_notifications(
            &harness.saved,
            &warm_complete(
                harness.journal_id,
                2,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            &NotifyOptions::default(),
            harness.now,
        );
        let failing = Arc::new(RecordingChannel {
            notices: Mutex::new(Vec::new()),
            fail: true,
        });
        let channel: Arc<dyn NoticeChannel> = failing.clone();
        let results = commit_then_deliver(
            harness.cache.as_ref().expect("cache"),
            plan,
            std::slice::from_ref(&channel),
            &ManualRuntime { cancelled: false },
        )
        .expect("committed");
        assert_eq!(results, vec![false]);
        harness.saved = harness.cache.as_ref().expect("cache").load().expect("load");
        harness.now += 1_000;
        let calls = Arc::new(Mutex::new(0_u32));
        struct Counting(Arc<Mutex<u32>>);
        impl NoticeChannel for Counting {
            fn deliver(&self, _: &Notice, _: Duration) -> Result<(), WorkerError> {
                *self.0.lock().expect("calls") += 1;
                Ok(())
            }
        }
        let counting = Arc::new(Counting(Arc::clone(&calls)));
        let channel: Arc<dyn NoticeChannel> = counting;
        let replay = plan_notifications(
            &harness.saved,
            &warm_complete(
                harness.journal_id,
                3,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            &NotifyOptions::default(),
            harness.now,
        );
        assert!(replay.notices.is_empty());
        commit_then_deliver(
            harness.cache.as_ref().expect("cache"),
            replay,
            std::slice::from_ref(&channel),
            &ManualRuntime { cancelled: false },
        )
        .expect("second");
        assert_eq!(*calls.lock().expect("calls"), 0);

        let other = done_facts(
            TaskId::new(Uuid::from_u128(132)),
            TurnId::new(Uuid::from_u128(133)),
        );
        harness.interrupt = true;
        harness.consume(
            warm_complete(
                harness.journal_id,
                4,
                vec![change(other.clone())],
                vec![other.clone()],
            ),
            NotifyOptions::default(),
        );
        assert!(harness.delivered.is_empty());
        harness.restart_with_valid_cursor();
        harness.consume(
            warm_complete(
                harness.journal_id,
                5,
                vec![change(other.clone())],
                vec![other],
            ),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered_since_restart(), 0);
    }

    #[test]
    fn second_cold_start_with_empty_saved_registry_baselines_without_banners() {
        let mut harness = Harness::new();
        assert!(harness.saved.decisions.is_empty());
        assert!(harness.saved.consumed_after.is_some());
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(140)),
            TurnId::new(Uuid::from_u128(141)),
        );
        harness.consume(
            cold_complete(harness.journal_id, 2, vec![facts.clone()]),
            NotifyOptions::default(),
        );
        assert!(harness.delivered.is_empty());
        assert_eq!(harness.saved.decisions.len(), 1);
        harness.restart_with_valid_cursor();
        harness.consume(
            cold_complete(harness.journal_id, 3, vec![facts]),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered_since_restart(), 0);
    }

    #[test]
    fn target_hash_omits_the_ssh_destination() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let left = controller("user@secret-host");
        let right = controller("user@other-host");
        let left_dir = cache_directory(&paths, &left);
        let right_dir = cache_directory(&paths, &right);
        assert_ne!(left_dir, right_dir);
        assert_eq!(super::target_digest(&left).len(), 64);
        assert!(!left_dir.display().to_string().contains("secret-host"));
    }
}
