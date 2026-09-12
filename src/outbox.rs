use std::{
    collections::HashMap,
    ffi::OsString,
    fs::File,
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    git_transport::{GitTransport, delivery_pin_ref},
    host_store::{HostStore, HostStoreWritePoint},
    job::ProcessIdentity,
    process::ProcessRunner,
    rooted_fs::RootedDir,
    supervisor::{ProcessInspector, ProcessObservation, SystemProcessInspector},
    task::{BaseOid, BranchName, DeliveryState, OriginDelivery, TaskId, TurnId},
};

const MAX_RECORD_BYTES: u64 = 1024 * 1024;
const MAX_ATTEMPTS: u32 = 12;
const MAX_BACKOFF_MILLIS: u64 = 300_000;
pub const OUTBOX_WORKER_REQUIRED: &str = "OUTBOX_WORKER_REQUIRED";
pub const OUTBOX_BUSY: &str = "OUTBOX_BUSY";
pub const DELIVERY_UNREADABLE: &str = "DELIVERY_UNREADABLE";
const DELIVERY_DIR: &str = "delivery";
const DEFAULT_WATCH_IDLE_MILLIS: u64 = 1_000;
const DUE_DIRECTORY: &str = "locks/outbox-due";
const WATCH_HANDSHAKE_ATTEMPTS: u32 = 100;
const WATCH_HANDSHAKE_MILLIS: u64 = 10;

static ROOT_COUNTERS: Mutex<Option<HashMap<PathBuf, (u64, u64)>>> = Mutex::new(None);

fn bump_task_directory_scans(root: &Path) {
    bump_root_counter(root, true);
}

fn bump_due_index_reads(root: &Path) {
    bump_root_counter(root, false);
}

fn bump_root_counter(root: &Path, task_scans: bool) {
    let Ok(mut guard) = ROOT_COUNTERS.lock() else {
        return;
    };
    let map = guard.get_or_insert_with(HashMap::new);
    let entry = map.entry(root.to_path_buf()).or_insert((0, 0));
    if task_scans {
        entry.0 = entry.0.saturating_add(1);
    } else {
        entry.1 = entry.1.saturating_add(1);
    }
}

pub fn task_directory_scans_for(root: &Path) -> u64 {
    ROOT_COUNTERS
        .lock()
        .ok()
        .and_then(|guard| {
            guard
                .as_ref()
                .and_then(|map| map.get(root).map(|entry| entry.0))
        })
        .unwrap_or(0)
}

pub fn due_index_reads_for(root: &Path) -> u64 {
    ROOT_COUNTERS
        .lock()
        .ok()
        .and_then(|guard| {
            guard
                .as_ref()
                .and_then(|map| map.get(root).map(|entry| entry.1))
        })
        .unwrap_or(0)
}

pub struct OriginOutbox<'a> {
    store: &'a HostStore,
    runner: &'a dyn ProcessRunner,
}

#[derive(Debug, Clone)]
pub struct DeliveryCommit<'a> {
    pub project_id: &'a str,
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub oid: &'a BaseOid,
    pub origin: &'a str,
    pub branch: &'a BranchName,
    pub now_millis: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxActivation {
    Active,
    Inactive,
}

pub trait OutboxLauncher: Send + Sync {
    fn ensure_watch(&self, host_root: &Path) -> Result<OutboxActivation, WorkerError>;
    fn report_unconfigured(&self) -> bool {
        true
    }
}

pub struct SystemOutboxLauncher;

impl OutboxLauncher for SystemOutboxLauncher {
    fn ensure_watch(&self, host_root: &Path) -> Result<OutboxActivation, WorkerError> {
        let executable = std::env::current_exe().map_err(WorkerError::Io)?;
        if !is_worker_executable(&executable) {
            return Ok(OutboxActivation::Inactive);
        }
        spawn_watch(&executable, host_root)?;
        Ok(OutboxActivation::Active)
    }

    fn report_unconfigured(&self) -> bool {
        std::env::current_exe()
            .ok()
            .is_some_and(|path| is_worker_executable(&path))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryIntent {
    project_id: String,
    task_id: TaskId,
    turn_id: TurnId,
    origin: String,
    branch: String,
    oid: BaseOid,
    state: DeliveryState,
    attempt: u32,
    next_attempt_at_millis: u64,
    last_error: Option<String>,
    superseded_by: Option<BaseOid>,
    created_at_millis: u64,
    updated_at_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetLedger {
    origin: String,
    branch: String,
    generation: u64,
    last_oid: Option<BaseOid>,
    last_task_id: Option<TaskId>,
    last_turn_id: Option<TurnId>,
    updated_at_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxWorkerRecord {
    identity: ProcessIdentity,
    watch: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxEnabled {
    watch: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DueEntry {
    project_id: String,
    task_id: TaskId,
    turn_id: TurnId,
    next_attempt_at_millis: u64,
}

struct IntentGuard {
    file: File,
}

impl Drop for IntentGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

impl<'a> OriginOutbox<'a> {
    pub fn new(store: &'a HostStore, runner: &'a dyn ProcessRunner) -> Self {
        Self { store, runner }
    }

    pub fn host_root(&self) -> &Path {
        self.store.root()
    }

    pub fn commit_intent(&self, commit: DeliveryCommit<'_>) -> Result<OriginDelivery, WorkerError> {
        let origin = delivery_origin(commit.origin)?;
        let _guard = self.intent_lock(commit.task_id, commit.turn_id)?;
        let mut intent = DeliveryIntent {
            project_id: commit.project_id.to_owned(),
            task_id: commit.task_id,
            turn_id: commit.turn_id,
            origin,
            branch: commit.branch.as_str().to_owned(),
            oid: commit.oid.clone(),
            state: DeliveryState::Pending,
            attempt: 1,
            next_attempt_at_millis: 0,
            last_error: None,
            superseded_by: None,
            created_at_millis: commit.now_millis,
            updated_at_millis: commit.now_millis,
        };
        if let Some(existing) =
            self.read_intent(commit.project_id, commit.task_id, commit.turn_id)?
        {
            if existing.oid != intent.oid
                || existing.origin != intent.origin
                || existing.branch != intent.branch
            {
                return Err(WorkerError::task(
                    "DELIVERY_REF_CONFLICT",
                    "delivery intent already exists with a different identity",
                ));
            }
            intent = existing;
        }
        // Identity-hit visibility is not a durability receipt: the create
        // write can expose the intent name and then fail delivery/task
        // directory sync. Reestablish file + parent barriers before pin/due.
        self.write_intent(&intent, true)?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterOutboxIntent)
        {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected outbox intent failure",
            )));
        }
        let mirror = self
            .store
            .mirror_if_present(commit.project_id)?
            .ok_or_else(|| WorkerError::task("PUBLISH_FAILED", "project mirror is absent"))?;
        GitTransport::new(self.runner).pin_delivery_ref(
            self.store,
            &mirror,
            commit.task_id,
            commit.turn_id,
            &intent.oid,
        )?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterOutboxPin)
        {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected outbox pin failure",
            )));
        }
        self.sync_due(&intent)?;
        intent.into_dto()
    }

    pub(crate) fn has_intent(
        &self,
        project_id: &str,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<bool, WorkerError> {
        Ok(self.read_intent(project_id, task_id, turn_id)?.is_some())
    }

    pub fn dto(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Option<OriginDelivery>, WorkerError> {
        Ok(self.deliveries(project_id, task_id)?.into_iter().next())
    }

    pub fn deliveries(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Vec<OriginDelivery>, WorkerError> {
        let mut items = Vec::new();
        for intent in self
            .load_intent_files(project_id, task_id)?
            .into_iter()
            .flatten()
        {
            items.push(intent.into_dto()?);
        }
        items.extend(self.unreadable_projections(project_id, task_id)?);
        items.sort_by_key(|item| std::cmp::Reverse(item.created_at_millis()));
        Ok(items)
    }

    pub fn retains(
        &self,
        project_id: &str,
        task_id: TaskId,
        now_millis: u64,
    ) -> Result<bool, WorkerError> {
        let files = match self.load_intent_files(project_id, task_id) {
            Ok(files) => files,
            Err(_) => return Ok(true),
        };
        if files.iter().any(|intent| intent.is_none()) {
            return Ok(true);
        }
        for intent in files.into_iter().flatten() {
            if intent.state.retains_objects() {
                return Ok(true);
            }
            if intent.state == DeliveryState::Failed
                && now_millis.saturating_sub(intent.updated_at_millis)
                    < crate::host_store::BRANCH_RETENTION_MILLIS
            {
                return Ok(true);
            }
        }
        self.has_orphaned_pin(project_id, task_id)
    }

    pub fn discard_blocked(&self, project_id: &str, task_id: TaskId) -> Result<bool, WorkerError> {
        let files = match self.load_intent_files(project_id, task_id) {
            Ok(files) => files,
            Err(_) => return Ok(true),
        };
        if files.iter().any(|intent| intent.is_none()) {
            return Ok(true);
        }
        if files
            .into_iter()
            .flatten()
            .any(|intent| intent.state.retains_objects())
        {
            return Ok(true);
        }
        self.has_orphaned_pin(project_id, task_id)
    }

    pub fn pump_due(&self, now_millis: u64) -> Result<Vec<OriginDelivery>, WorkerError> {
        let Some(_pump) = self.try_pump_lock()? else {
            return Err(WorkerError::task(
                OUTBOX_BUSY,
                "outbox pump is already active",
            ));
        };
        self.pump_due_locked(now_millis)
    }

    pub fn wake(&self, launcher: &dyn OutboxLauncher) -> Result<OutboxActivation, WorkerError> {
        if self.live_watch()? {
            return Ok(OutboxActivation::Active);
        }
        let _launch = self.launch_lock()?;
        if self.live_watch()? {
            return Ok(OutboxActivation::Active);
        }
        match launcher.ensure_watch(self.store.root()) {
            Ok(OutboxActivation::Active) => {
                for _ in 0..WATCH_HANDSHAKE_ATTEMPTS {
                    if self.live_watch()? {
                        return Ok(OutboxActivation::Active);
                    }
                    std::thread::sleep(Duration::from_millis(WATCH_HANDSHAKE_MILLIS));
                }
                let _ = self.mark_worker_required(now_millis()?);
                Err(WorkerError::task(
                    OUTBOX_WORKER_REQUIRED,
                    "outbox watcher did not acknowledge launch",
                ))
            }
            Ok(OutboxActivation::Inactive) => {
                if launcher.report_unconfigured() {
                    let _ = self.mark_worker_required(now_millis()?);
                }
                Ok(OutboxActivation::Inactive)
            }
            Err(error) => {
                let _ = self.mark_worker_required(now_millis()?);
                Err(error)
            }
        }
    }

    pub fn enable_watch(&self) -> Result<(), WorkerError> {
        let locks = self.store.open_directory("locks", false)?;
        write_json_once(
            &locks,
            "outbox-enabled.json",
            &OutboxEnabled { watch: true },
        )
    }

    pub fn is_enabled(&self) -> Result<bool, WorkerError> {
        let locks = self.store.open_directory("locks", false)?;
        if !locks.entry_exists("outbox-enabled.json")? {
            return Ok(false);
        }
        let enabled: OutboxEnabled = read_json(&locks, "outbox-enabled.json")?;
        Ok(enabled.watch)
    }

    pub fn watch_args(host_root: &Path) -> Result<Vec<OsString>, WorkerError> {
        let root = validate_host_root(host_root)?;
        Ok(vec![
            OsString::from("host"),
            OsString::from("outbox"),
            OsString::from("--watch"),
            OsString::from("--host-root"),
            root.as_os_str().to_os_string(),
        ])
    }

    pub fn launchd_plist(executable: &Path, host_root: &Path) -> Result<String, WorkerError> {
        let root = validate_host_root(host_root)?;
        let mut arguments = vec![xml_escape(&executable.to_string_lossy())];
        for argument in Self::watch_args(root)? {
            arguments.push(xml_escape(&argument.to_string_lossy()));
        }
        let argument_xml = arguments
            .into_iter()
            .map(|argument| format!("    <string>{argument}</string>"))
            .collect::<Vec<_>>()
            .join("\n");
        Ok(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.mac-worker.outbox</string>
  <key>KeepAlive</key>
  <true/>
  <key>ProgramArguments</key>
  <array>
{argument_xml}
  </array>
</dict>
</plist>
"#
        ))
    }

    pub fn write_launchd_plist(
        directory: &Path,
        executable: &Path,
        host_root: &Path,
    ) -> Result<PathBuf, WorkerError> {
        std::fs::create_dir_all(directory).map_err(WorkerError::Io)?;
        let path = directory.join("com.mac-worker.outbox.plist");
        std::fs::write(&path, Self::launchd_plist(executable, host_root)?)
            .map_err(WorkerError::Io)?;
        Ok(path)
    }

    pub fn run_watch(&self, stop: &AtomicBool, now: impl Fn() -> u64) -> Result<(), WorkerError> {
        self.run_watch_with(stop, now, DEFAULT_WATCH_IDLE_MILLIS)
    }

    pub fn run_watch_with(
        &self,
        stop: &AtomicBool,
        now: impl Fn() -> u64,
        idle_millis: u64,
    ) -> Result<(), WorkerError> {
        let Some(_pump) = self.try_pump_lock()? else {
            return Ok(());
        };
        self.publish_worker(true)?;
        self.recover_due_index()?;
        while !stop.load(Ordering::SeqCst) {
            let now_millis = now();
            let _ = self.pump_due_locked(now_millis);
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let sleep_for = self
                .sleep_millis(now_millis, idle_millis.max(1))
                .unwrap_or(idle_millis.max(1));
            std::thread::sleep(Duration::from_millis(sleep_for));
        }
        Ok(())
    }

    pub fn run_once(&self, now_millis: u64) -> Result<Vec<OriginDelivery>, WorkerError> {
        self.pump_due(now_millis)
    }

    pub fn live_watch(&self) -> Result<bool, WorkerError> {
        let locks = self.store.open_directory("locks", false)?;
        if !locks.entry_exists("outbox-worker.json")? {
            return Ok(false);
        }
        let record: OutboxWorkerRecord = read_json(&locks, "outbox-worker.json")?;
        if !record.watch {
            return Ok(false);
        }
        Ok(matches!(
            SystemProcessInspector.observe(record.identity),
            ProcessObservation::Matching { .. }
        ))
    }

    fn pump_due_locked(&self, now_millis: u64) -> Result<Vec<OriginDelivery>, WorkerError> {
        let mut due = self.list_due(now_millis)?;
        if due.is_empty() {
            return Ok(Vec::new());
        }
        due.sort_by_key(|intent| {
            (
                intent.origin.clone(),
                intent.branch.clone(),
                intent.created_at_millis,
                intent.turn_id.to_string(),
            )
        });
        let mut results = Vec::new();
        for intent in due {
            match self.deliver_one(intent.clone(), now_millis) {
                Ok(dto) => results.push(dto),
                Err(error) => {
                    let _ = self.mark_operation_failure(&intent, now_millis, &error);
                    if let Ok(Some(current)) =
                        self.read_intent(&intent.project_id, intent.task_id, intent.turn_id)
                        && let Ok(dto) = current.into_dto()
                    {
                        results.push(dto);
                    }
                }
            }
        }
        Ok(results)
    }

    fn deliver_one(
        &self,
        intent: DeliveryIntent,
        now_millis: u64,
    ) -> Result<OriginDelivery, WorkerError> {
        self.ensure_pin(&intent)?;
        self.pump_one(intent, now_millis)
    }

    fn pump_one(
        &self,
        snapshot: DeliveryIntent,
        now_millis: u64,
    ) -> Result<OriginDelivery, WorkerError> {
        let snapshot = {
            let _guard = self.intent_lock(snapshot.task_id, snapshot.turn_id)?;
            let current = self
                .read_intent(&snapshot.project_id, snapshot.task_id, snapshot.turn_id)?
                .ok_or_else(|| {
                    WorkerError::task("PUBLISH_FAILED", "delivery intent was removed during push")
                })?;
            if current.oid != snapshot.oid
                || current.created_at_millis != snapshot.created_at_millis
                || current.turn_id != snapshot.turn_id
            {
                return Err(WorkerError::task(
                    "DELIVERY_REF_CONFLICT",
                    "delivery intent changed during push",
                ));
            }
            if !current.state.retains_objects() || current.next_attempt_at_millis > now_millis {
                return current.into_dto();
            }
            current
        };
        let ledger = {
            let _target = self.target_lock(&snapshot.origin, &snapshot.branch)?;
            self.read_ledger(&snapshot.origin, &snapshot.branch)?
        };
        let classified = self.classify_before_push(&snapshot, ledger.as_ref())?;
        let mut next = match classified {
            Some(done) => done,
            None => match self.push_snapshot(&snapshot) {
                Ok(()) => {
                    snapshot
                        .clone()
                        .into_state(DeliveryState::Delivered, now_millis, None, None)?
                }
                Err(error) => {
                    self.classify_after_failure(&snapshot, ledger.as_ref(), now_millis, &error)?
                }
            },
        };
        let _guard = self.intent_lock(snapshot.task_id, snapshot.turn_id)?;
        let current = self
            .read_intent(&snapshot.project_id, snapshot.task_id, snapshot.turn_id)?
            .ok_or_else(|| {
                WorkerError::task("PUBLISH_FAILED", "delivery intent was removed during push")
            })?;
        if current.oid != snapshot.oid
            || current.created_at_millis != snapshot.created_at_millis
            || current.turn_id != snapshot.turn_id
        {
            return Err(WorkerError::task(
                "DELIVERY_REF_CONFLICT",
                "delivery intent changed during push",
            ));
        }
        if !current.state.retains_objects() {
            return current.into_dto();
        }
        next.attempt = current.attempt;
        if next.state == DeliveryState::Retrying || next.state == DeliveryState::Failed {
            next.attempt = current.attempt.saturating_add(1);
            if next.attempt > MAX_ATTEMPTS {
                next.state = DeliveryState::Failed;
                if next.last_error.is_none() {
                    next.last_error = Some("PUBLISH_FAILED".into());
                }
            }
            next.next_attempt_at_millis = now_millis.saturating_add(backoff_millis(next.attempt));
        }
        next.updated_at_millis = now_millis;
        self.write_intent(&next, false)?;
        if next.state == DeliveryState::Delivered && next.superseded_by.is_none() {
            let _target = self.target_lock(&next.origin, &next.branch)?;
            let generation = ledger.as_ref().map(|item| item.generation).unwrap_or(0) + 1;
            self.write_ledger(&TargetLedger {
                origin: next.origin.clone(),
                branch: next.branch.clone(),
                generation,
                last_oid: Some(next.oid.clone()),
                last_task_id: Some(next.task_id),
                last_turn_id: Some(next.turn_id),
                updated_at_millis: now_millis,
            })?;
        }
        next.into_dto()
    }

    fn classify_before_push(
        &self,
        intent: &DeliveryIntent,
        ledger: Option<&TargetLedger>,
    ) -> Result<Option<DeliveryIntent>, WorkerError> {
        let Some(ledger) = ledger else {
            return Ok(None);
        };
        let Some(last) = ledger.last_oid.as_ref() else {
            return Ok(None);
        };
        if last == &intent.oid {
            return Ok(Some(intent.clone().into_state(
                DeliveryState::Delivered,
                intent.updated_at_millis,
                None,
                None,
            )?));
        }
        let Some(mirror) = self.store.mirror_if_present(&intent.project_id)? else {
            return Ok(None);
        };
        let git = GitTransport::new(self.runner);
        match git.is_ancestor(&mirror, &intent.oid, last) {
            Ok(true) => {
                return Ok(Some(intent.clone().into_state(
                    DeliveryState::Delivered,
                    intent.updated_at_millis,
                    None,
                    Some(last.clone()),
                )?));
            }
            Ok(false) => {}
            Err(_) => return Ok(None),
        }
        match git.is_ancestor(&mirror, last, &intent.oid) {
            Ok(true) => Ok(None),
            Ok(false) => Ok(Some(intent.clone().into_state(
                DeliveryState::Retrying,
                intent.updated_at_millis,
                Some("PUBLISH_FAILED".into()),
                None,
            )?)),
            Err(_) => Ok(None),
        }
    }

    fn classify_after_failure(
        &self,
        intent: &DeliveryIntent,
        ledger: Option<&TargetLedger>,
        now_millis: u64,
        error: &WorkerError,
    ) -> Result<DeliveryIntent, WorkerError> {
        let branch: BranchName = intent
            .branch
            .parse()
            .map_err(|_| WorkerError::task("PUBLISH_FAILED", "delivery target is invalid"))?;
        let remote = GitTransport::new(self.runner).advertise_origin_ref(&intent.origin, &branch);
        match remote {
            Ok(Some(oid)) if oid_eq(&oid, &intent.oid) => {
                intent
                    .clone()
                    .into_state(DeliveryState::Delivered, now_millis, None, None)
            }
            Ok(Some(oid)) => {
                if ledger
                    .and_then(|item| item.last_oid.as_ref())
                    .is_some_and(|last| last == &oid)
                    && let Some(mirror) = self.store.mirror_if_present(&intent.project_id)?
                    && GitTransport::new(self.runner)
                        .is_ancestor(&mirror, &intent.oid, &oid)
                        .unwrap_or(false)
                {
                    return intent.clone().into_state(
                        DeliveryState::Delivered,
                        now_millis,
                        None,
                        Some(oid),
                    );
                }
                intent.clone().into_state(
                    DeliveryState::Retrying,
                    now_millis,
                    Some(delivery_retry_code(error)),
                    None,
                )
            }
            _ => intent.clone().into_state(
                DeliveryState::Retrying,
                now_millis,
                Some(delivery_retry_code(error)),
                None,
            ),
        }
    }

    fn push_snapshot(&self, intent: &DeliveryIntent) -> Result<(), WorkerError> {
        let branch: BranchName = intent
            .branch
            .parse()
            .map_err(|_| WorkerError::task("PUBLISH_FAILED", "delivery target is invalid"))?;
        let mirror = self
            .store
            .mirror_if_present(&intent.project_id)?
            .ok_or_else(|| WorkerError::task("PUBLISH_FAILED", "project mirror is absent"))?;
        GitTransport::new(self.runner).push_origin(&intent.origin, &intent.oid, &branch, &mirror)
    }

    fn ensure_pin(&self, intent: &DeliveryIntent) -> Result<(), WorkerError> {
        let _guard = self.intent_lock(intent.task_id, intent.turn_id)?;
        let Some(current) = self.read_intent(&intent.project_id, intent.task_id, intent.turn_id)?
        else {
            return Err(WorkerError::task(
                "PUBLISH_FAILED",
                "delivery intent was removed before pin",
            ));
        };
        if !current.state.retains_objects() {
            return Ok(());
        }
        let mirror = self
            .store
            .mirror_if_present(&current.project_id)?
            .ok_or_else(|| WorkerError::task("PUBLISH_FAILED", "project mirror is absent"))?;
        GitTransport::new(self.runner).pin_delivery_ref(
            self.store,
            &mirror,
            current.task_id,
            current.turn_id,
            &current.oid,
        )
    }

    fn mark_operation_failure(
        &self,
        snapshot: &DeliveryIntent,
        now_millis: u64,
        error: &WorkerError,
    ) -> Result<(), WorkerError> {
        let _guard = self.intent_lock(snapshot.task_id, snapshot.turn_id)?;
        let Some(mut current) =
            self.read_intent(&snapshot.project_id, snapshot.task_id, snapshot.turn_id)?
        else {
            return Ok(());
        };
        if !current.state.retains_objects() {
            return Ok(());
        }
        current.state = DeliveryState::Retrying;
        current.attempt = current.attempt.saturating_add(1);
        current.last_error = Some(delivery_retry_code(error));
        if current.attempt > MAX_ATTEMPTS {
            current.state = DeliveryState::Failed;
        }
        current.next_attempt_at_millis = now_millis.saturating_add(backoff_millis(current.attempt));
        current.updated_at_millis = now_millis;
        self.write_intent(&current, false)
    }

    fn has_orphaned_pin(&self, project_id: &str, task_id: TaskId) -> Result<bool, WorkerError> {
        let Some(mirror) = self.store.mirror_if_present(project_id)? else {
            return Ok(false);
        };
        let pins = GitTransport::new(self.runner).list_delivery_pins(&mirror, task_id)?;
        for (turn_id, _) in pins {
            if self.read_intent(project_id, task_id, turn_id)?.is_none() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn list_due(&self, now_millis: u64) -> Result<Vec<DeliveryIntent>, WorkerError> {
        bump_due_index_reads(self.store.root());
        let mut due = Vec::new();
        for entry in self.load_due_entries()? {
            if entry.next_attempt_at_millis > now_millis {
                continue;
            }
            match self.read_intent(&entry.project_id, entry.task_id, entry.turn_id) {
                Ok(Some(intent)) if intent_due(&intent, now_millis) => due.push(intent),
                Ok(Some(intent)) if !intent.state.retains_objects() => {
                    let _ = self.clear_due(intent.task_id, intent.turn_id);
                }
                Ok(_) | Err(_) => {}
            }
        }
        Ok(due)
    }

    fn sleep_millis(&self, now_millis: u64, idle_millis: u64) -> Result<u64, WorkerError> {
        bump_due_index_reads(self.store.root());
        let mut next = None;
        for entry in self.load_due_entries()? {
            next = Some(next.map_or(entry.next_attempt_at_millis, |current: u64| {
                current.min(entry.next_attempt_at_millis)
            }));
        }
        Ok(match next {
            Some(due) if due <= now_millis => idle_millis.max(1),
            Some(due) => (due - now_millis).min(idle_millis).max(1),
            None => idle_millis,
        })
    }

    fn due_namespace(&self, create: bool) -> Result<Option<RootedDir>, WorkerError> {
        match self.store.open_directory(DUE_DIRECTORY, create) {
            Ok(directory) => Ok(Some(directory)),
            Err(WorkerError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound && !create =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn load_due_entries(&self) -> Result<Vec<DueEntry>, WorkerError> {
        let Some(due) = self.due_namespace(false)? else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for name in utf8_names(&due)? {
            if !name.ends_with(".json") {
                continue;
            }
            match read_json::<DueEntry>(&due, &name) {
                Ok(entry) => entries.push(entry),
                Err(_) => continue,
            }
        }
        Ok(entries)
    }

    fn recover_due_index(&self) -> Result<(), WorkerError> {
        bump_task_directory_scans(self.store.root());
        let tasks = match self.store.open_directory("tasks", false) {
            Ok(tasks) => tasks,
            Err(_) => return Ok(()),
        };
        let projects = match utf8_names(&tasks) {
            Ok(projects) => projects,
            Err(_) => return Ok(()),
        };
        for project in projects {
            let project_dir = match tasks.open_child_directory(&relative(&project)?, false) {
                Ok(dir) => dir,
                Err(_) => continue,
            };
            let task_names = match utf8_names(&project_dir) {
                Ok(names) => names,
                Err(_) => continue,
            };
            for task_name in task_names {
                let Ok(task_id) = task_name.parse::<TaskId>() else {
                    continue;
                };
                let files = match self.load_intent_files(&project, task_id) {
                    Ok(files) => files,
                    Err(_) => continue,
                };
                for intent in files.into_iter().flatten() {
                    let _ = self.sync_due(&intent);
                }
            }
        }
        Ok(())
    }

    fn load_intents(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Vec<DeliveryIntent>, WorkerError> {
        Ok(self
            .load_intent_files(project_id, task_id)?
            .into_iter()
            .flatten()
            .collect())
    }

    fn unreadable_projections(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Vec<OriginDelivery>, WorkerError> {
        let task = match self.store.open_task_directory(project_id, task_id, false) {
            Ok(task) => task,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        if !task.entry_exists(DELIVERY_DIR)? {
            return Ok(Vec::new());
        }
        let delivery = match task.open_child_directory(&relative(DELIVERY_DIR)?, false) {
            Ok(delivery) => delivery,
            Err(_) => return Ok(Vec::new()),
        };
        let mut items = Vec::new();
        for name in utf8_names(&delivery)? {
            if !name.ends_with(".json") {
                continue;
            }
            if read_json::<DeliveryIntent>(&delivery, &name).is_ok() {
                continue;
            }
            if let Some(item) = self.projection_for_unreadable(project_id, task_id, &name) {
                items.push(item);
            }
        }
        Ok(items)
    }

    fn projection_for_unreadable(
        &self,
        project_id: &str,
        task_id: TaskId,
        name: &str,
    ) -> Option<OriginDelivery> {
        let turn_id = name.strip_suffix(".json")?.parse().ok()?;
        let oid = self.pin_oid(project_id, task_id, turn_id)?;
        OriginDelivery::new(
            turn_id,
            DeliveryState::Retrying,
            oid,
            "unreadable".into(),
            "refs/heads/unreadable".into(),
            1,
            0,
            Some(DELIVERY_UNREADABLE.into()),
            None,
            0,
            0,
        )
        .ok()
    }

    fn pin_oid(&self, project_id: &str, task_id: TaskId, turn_id: TurnId) -> Option<BaseOid> {
        let mirror = self.store.mirror_if_present(project_id).ok().flatten()?;
        GitTransport::new(self.runner)
            .list_delivery_pins(&mirror, task_id)
            .ok()?
            .into_iter()
            .find_map(|(pinned, oid)| (pinned == turn_id).then_some(oid))
    }

    fn load_intent_files(
        &self,
        project_id: &str,
        task_id: TaskId,
    ) -> Result<Vec<Option<DeliveryIntent>>, WorkerError> {
        let task = match self.store.open_task_directory(project_id, task_id, false) {
            Ok(task) => task,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        if !task.entry_exists(DELIVERY_DIR)? {
            return Ok(Vec::new());
        }
        let delivery = task.open_child_directory(&relative(DELIVERY_DIR)?, false)?;
        let mut intents = Vec::new();
        for name in utf8_names(&delivery)? {
            if !name.ends_with(".json") {
                continue;
            }
            match read_json(&delivery, &name) {
                Ok(intent) => intents.push(Some(intent)),
                Err(_) => intents.push(None),
            }
        }
        Ok(intents)
    }

    fn read_intent(
        &self,
        project_id: &str,
        task_id: TaskId,
        turn_id: TurnId,
    ) -> Result<Option<DeliveryIntent>, WorkerError> {
        let task = match self.store.open_task_directory(project_id, task_id, false) {
            Ok(task) => task,
            Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !task.entry_exists(DELIVERY_DIR)? {
            return Ok(None);
        }
        let delivery = task.open_child_directory(&relative(DELIVERY_DIR)?, false)?;
        let name = format!("{turn_id}.json");
        if !delivery.entry_exists(&name)? {
            return Ok(None);
        }
        Ok(Some(read_json(&delivery, &name)?))
    }

    fn write_intent(&self, intent: &DeliveryIntent, create: bool) -> Result<(), WorkerError> {
        let task = self
            .store
            .open_task_directory(&intent.project_id, intent.task_id, true)?;
        let delivery = task.open_child_directory(&relative(DELIVERY_DIR)?, true)?;
        let name = format!("{}.json", intent.turn_id);
        if create && !delivery.entry_exists(&name)? {
            write_json_once(&delivery, &name, intent)?;
        } else if delivery.entry_exists(&name)? {
            let current: DeliveryIntent = read_json(&delivery, &name)?;
            replace_json(&delivery, &name, &current, intent)?;
        } else {
            write_json_once(&delivery, &name, intent)?;
        }
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterOutboxIntentPublish)
        {
            return Err(WorkerError::Io(std::io::Error::other(
                "injected outbox intent publication failure",
            )));
        }
        delivery.sync_root().map_err(WorkerError::Io)?;
        task.sync_root().map_err(WorkerError::Io)?;
        self.sync_due(intent)
    }

    fn sync_due(&self, intent: &DeliveryIntent) -> Result<(), WorkerError> {
        if intent.state.retains_objects() {
            self.write_due(intent)
        } else {
            self.clear_due(intent.task_id, intent.turn_id)
        }
    }

    fn write_due(&self, intent: &DeliveryIntent) -> Result<(), WorkerError> {
        let due = self
            .due_namespace(true)?
            .ok_or_else(|| WorkerError::task("PUBLISH_FAILED", "outbox due registry is absent"))?;
        let name = due_name(intent.task_id, intent.turn_id);
        let entry = DueEntry {
            project_id: intent.project_id.clone(),
            task_id: intent.task_id,
            turn_id: intent.turn_id,
            next_attempt_at_millis: intent.next_attempt_at_millis,
        };
        if due.entry_exists(&name)? {
            match read_json::<DueEntry>(&due, &name) {
                Ok(current) => replace_json(&due, &name, &current, &entry)?,
                Err(_) => {
                    let _ = due.remove_owned_regular(&name);
                    write_json_once(&due, &name, &entry)?;
                }
            }
        } else {
            write_json_once(&due, &name, &entry)?;
        }
        due.sync_root().map_err(WorkerError::Io)
    }

    fn clear_due(&self, task_id: TaskId, turn_id: TurnId) -> Result<(), WorkerError> {
        let Some(due) = self.due_namespace(false)? else {
            return Ok(());
        };
        let name = due_name(task_id, turn_id);
        if due.entry_exists(&name)? {
            due.remove_owned_regular(&name).map_err(WorkerError::Io)?;
            due.sync_root().map_err(WorkerError::Io)?;
        }
        Ok(())
    }

    fn intent_lock(&self, task_id: TaskId, turn_id: TurnId) -> Result<IntentGuard, WorkerError> {
        flock_named(self.store, &format!("outbox-{task_id}-{turn_id}.lock"))
    }

    fn target_lock(&self, origin: &str, branch: &str) -> Result<IntentGuard, WorkerError> {
        flock_named(
            self.store,
            &format!("outbox-target-{}.lock", target_key(origin, branch)),
        )
    }

    fn try_pump_lock(&self) -> Result<Option<IntentGuard>, WorkerError> {
        flock_named_nb(self.store, "outbox.lock")
    }

    fn launch_lock(&self) -> Result<IntentGuard, WorkerError> {
        flock_named(self.store, "outbox-launch.lock")
    }

    fn read_ledger(&self, origin: &str, branch: &str) -> Result<Option<TargetLedger>, WorkerError> {
        let locks = self.store.open_directory("locks", false)?;
        let name = format!("outbox-target-{}.json", target_key(origin, branch));
        if !locks.entry_exists(&name)? {
            return Ok(None);
        }
        Ok(Some(read_json(&locks, &name)?))
    }

    fn write_ledger(&self, ledger: &TargetLedger) -> Result<(), WorkerError> {
        let locks = self.store.open_directory("locks", false)?;
        let name = format!(
            "outbox-target-{}.json",
            target_key(&ledger.origin, &ledger.branch)
        );
        if locks.entry_exists(&name)? {
            let current: TargetLedger = read_json(&locks, &name)?;
            replace_json(&locks, &name, &current, ledger)
        } else {
            write_json_once(&locks, &name, ledger)
        }
    }

    fn publish_worker(&self, watch: bool) -> Result<(), WorkerError> {
        let locks = self.store.open_directory("locks", false)?;
        let identity = current_identity()?;
        let next = OutboxWorkerRecord { identity, watch };
        if locks.entry_exists("outbox-worker.json")? {
            let current: OutboxWorkerRecord = read_json(&locks, "outbox-worker.json")?;
            replace_json(&locks, "outbox-worker.json", &current, &next)
        } else {
            write_json_once(&locks, "outbox-worker.json", &next)
        }
    }

    fn mark_worker_required(&self, now_millis: u64) -> Result<(), WorkerError> {
        let tasks = self.store.open_directory("tasks", false)?;
        for project in utf8_names(&tasks)? {
            let project_dir = match tasks.open_child_directory(&relative(&project)?, false) {
                Ok(dir) => dir,
                Err(_) => continue,
            };
            for task_name in utf8_names(&project_dir)? {
                let Ok(task_id) = task_name.parse::<TaskId>() else {
                    continue;
                };
                for mut intent in self.load_intents(&project, task_id)? {
                    if !intent.state.retains_objects() {
                        continue;
                    }
                    let _guard = self.intent_lock(task_id, intent.turn_id)?;
                    if let Some(current) = self.read_intent(&project, task_id, intent.turn_id)? {
                        intent = current;
                    }
                    if intent.last_error.as_deref() == Some(OUTBOX_WORKER_REQUIRED) {
                        continue;
                    }
                    intent.last_error = Some(OUTBOX_WORKER_REQUIRED.into());
                    intent.updated_at_millis = now_millis;
                    self.write_intent(&intent, false)?;
                }
            }
        }
        Ok(())
    }
}

impl DeliveryIntent {
    fn into_dto(self) -> Result<OriginDelivery, WorkerError> {
        OriginDelivery::new(
            self.turn_id,
            self.state,
            self.oid,
            self.origin,
            format!("refs/heads/{}", self.branch),
            self.attempt,
            self.next_attempt_at_millis,
            self.last_error,
            self.superseded_by,
            self.created_at_millis,
            self.updated_at_millis,
        )
    }

    fn into_state(
        mut self,
        state: DeliveryState,
        now_millis: u64,
        last_error: Option<String>,
        superseded_by: Option<BaseOid>,
    ) -> Result<Self, WorkerError> {
        self.state = state;
        self.updated_at_millis = now_millis;
        self.last_error = last_error;
        self.superseded_by = superseded_by;
        Ok(self)
    }
}

impl OriginOutbox<'_> {
    pub fn delivery_refs(task_id: TaskId, turn_id: TurnId) -> String {
        delivery_pin_ref(task_id, turn_id)
    }
}

fn spawn_watch(executable: &Path, host_root: &Path) -> Result<(), WorkerError> {
    let mut command = std::process::Command::new(executable);
    command.args(OriginOutbox::watch_args(host_root)?);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: setsid is async-signal-safe and runs between fork and exec so the
    // watcher survives submitting-parent session teardown.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    command
        .spawn()
        .map_err(|error| WorkerError::task(OUTBOX_WORKER_REQUIRED, error.to_string()))?;
    Ok(())
}

fn is_worker_executable(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "worker")
}

fn validate_host_root(path: &Path) -> Result<&Path, WorkerError> {
    if !path.is_absolute() {
        return Err(WorkerError::Protocol(
            "outbox host root must be an absolute path".into(),
        ));
    }
    Ok(path)
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn flock_named(store: &HostStore, name: &str) -> Result<IntentGuard, WorkerError> {
    let locks = store.open_directory("locks", false)?;
    let file = locks.open_private_lock(name).map_err(WorkerError::Io)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result != 0 {
        return Err(WorkerError::Io(std::io::Error::last_os_error()));
    }
    Ok(IntentGuard { file })
}

fn flock_named_nb(store: &HostStore, name: &str) -> Result<Option<IntentGuard>, WorkerError> {
    let locks = store.open_directory("locks", false)?;
    let file = locks.open_private_lock(name).map_err(WorkerError::Io)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::EAGAIN) => Ok(None),
            _ => Err(WorkerError::Io(error)),
        };
    }
    Ok(Some(IntentGuard { file }))
}

fn due_name(task_id: TaskId, turn_id: TurnId) -> String {
    format!("{task_id}-{turn_id}.json")
}

fn intent_due(intent: &DeliveryIntent, now_millis: u64) -> bool {
    intent.state.retains_objects() && intent.next_attempt_at_millis <= now_millis
}

fn delivery_origin(origin: &str) -> Result<String, WorkerError> {
    if let Ok(url) = url::Url::parse(origin)
        && url.scheme() == "file"
    {
        return Ok(url.to_string());
    }
    crate::project::normalize_origin(origin)
        .map_err(|_| WorkerError::task("PUBLISH_FAILED", "origin URL is invalid"))
}

fn delivery_retry_code(error: &WorkerError) -> String {
    let code = error.public_code();
    if code == crate::git_transport::ORIGIN_AUTH_FAILED {
        code
    } else {
        "PUBLISH_FAILED".into()
    }
}

fn target_key(origin: &str, branch: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(origin.as_bytes());
    hasher.update([0]);
    hasher.update(branch.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn backoff_millis(attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(16);
    (1000_u64.saturating_mul(1_u64.checked_shl(shift).unwrap_or(u64::MAX))).min(MAX_BACKOFF_MILLIS)
}

fn now_millis() -> Result<u64, WorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            WorkerError::task("TASK_CLOCK_INVALID", "system clock precedes the Unix epoch")
        })?
        .as_millis()
        .try_into()
        .map_err(|_| WorkerError::task("TASK_CLOCK_INVALID", "system clock is outside the range"))
}

fn current_identity() -> Result<ProcessIdentity, WorkerError> {
    SystemProcessInspector.identity_for_pid(std::process::id())
}

fn oid_eq(left: &BaseOid, right: &BaseOid) -> bool {
    left.as_str() == right.as_str()
}

fn utf8_names(directory: &RootedDir) -> Result<Vec<String>, WorkerError> {
    let mut names = Vec::new();
    for raw in directory.list_names().map_err(WorkerError::Io)? {
        if raw == b".mac-worker-rooted-fs" || raw.first() == Some(&b'.') {
            continue;
        }
        if let Ok(name) = String::from_utf8(raw) {
            names.push(name);
        }
    }
    Ok(names)
}

fn relative(path: &str) -> Result<crate::inputs::RelativePath, WorkerError> {
    crate::inputs::RelativePath::parse(path.as_bytes())
        .map_err(|error| WorkerError::Protocol(error.to_string()))
}

fn read_json<T: DeserializeOwned + Serialize>(
    directory: &RootedDir,
    name: &str,
) -> Result<T, WorkerError> {
    let bytes = directory
        .read_private_regular(name, MAX_RECORD_BYTES)
        .map_err(WorkerError::Io)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
    let value = T::deserialize(&mut deserializer)
        .map_err(|error| WorkerError::Protocol(format!("invalid outbox JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| WorkerError::Protocol(format!("trailing outbox JSON: {error}")))?;
    let canonical = serde_json::to_vec(&value).map_err(|error| {
        WorkerError::Protocol(format!("failed to canonicalize outbox JSON: {error}"))
    })?;
    if canonical != bytes {
        return Err(WorkerError::Protocol("outbox JSON is not canonical".into()));
    }
    Ok(value)
}

fn write_json_once<T: Serialize>(
    directory: &RootedDir,
    name: &str,
    value: &T,
) -> Result<(), WorkerError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize outbox JSON: {error}"))
    })?;
    directory
        .write_private_atomic_no_replace(name, &bytes)
        .map_err(WorkerError::Io)
}

fn replace_json<T: Serialize>(
    directory: &RootedDir,
    name: &str,
    current: &T,
    next: &T,
) -> Result<(), WorkerError> {
    let old = serde_json::to_vec(current).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize outbox JSON: {error}"))
    })?;
    let new = serde_json::to_vec(next).map_err(|error| {
        WorkerError::Protocol(format!("failed to serialize outbox JSON: {error}"))
    })?;
    directory
        .replace_private_regular_exact(name, &old, &new)
        .map_err(WorkerError::Io)?;
    directory.sync_root().map_err(WorkerError::Io)
}
