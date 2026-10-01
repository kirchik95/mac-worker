//! Private, bounded controller journal and a try-only process-local publisher.
pub use super::contracts::{
    EventBatch, EventCursor, EventReadResult, EventRuntime, EventSink, JournalProvider,
    JournalReader, JournalWindow, JournalWriter, PublishAttempt, ReadBatch, ReadQuery,
    SnapshotRequired,
};

mod fs;
mod publisher;

use super::contracts::{
    CONTROLLER_EVENTS_CANCELLED, JOURNAL_CHECK_INTERVAL, MAX_BATCH_BYTES, MAX_EVENT_BYTES,
    RPC_BUDGET, SCHEMA_VERSION, Seq, unavailable,
};
use crate::{
    controller::ControllerLeader,
    error::WorkerError,
    paths::PathLayout,
    rooted_fs::{PrivateRolePoint, RootedDir},
};
use std::{io, sync::Arc, time::Duration};

pub struct JournalOptions {
    pub runtime: Arc<dyn EventRuntime>,
}

/// Fixed roles admitted under the journal EX lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalRole {
    Initialization,
    Manifest,
    Pending,
    Segment,
    Retirement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalRoleBoundary {
    /// Empty stage and directory are durable; creation evidence is not written yet.
    StageCreated,
    CreationRecorded,
    PartialStage,
    StageSynced,
    Published,
    DirectorySynced,
    /// Emitted only when an exchange displaces an existing target.
    CleanupDecisionDurable,
    DisplacedRemoved,
    FinalSynced,
}

/// Deterministic gates for the journal fault matrix; callbacks run on journal I/O only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalFaultPoint {
    Role(JournalRole, JournalRoleBoundary),
    PendingDurable,
    PartialAppend,
    SegmentSynced,
    ManifestCommitted,
    PendingRemoved,
    Retired,
    ReadAttempt,
    RecoveryAttempt,
    AppendPrepared,
    SegmentRead { first_seq: u64 },
}

#[doc(hidden)]
pub trait JournalFaultHook: Send + Sync {
    fn at(&self, point: JournalFaultPoint) -> io::Result<()>;
}

impl<F: Fn(JournalFaultPoint) -> io::Result<()> + Send + Sync> JournalFaultHook for F {
    fn at(&self, point: JournalFaultPoint) -> io::Result<()> {
        self(point)
    }
}

struct Hooks(Option<Arc<dyn JournalFaultHook>>);
impl Hooks {
    fn at(&self, point: JournalFaultPoint) -> io::Result<()> {
        self.0.as_ref().map_or(Ok(()), |hook| hook.at(point))
    }
}

impl fs::FaultHooks for Hooks {
    fn at(&self, point: fs::FaultPoint) -> io::Result<()> {
        use JournalFaultPoint as J;
        use fs::{FaultPoint as F, RoleKind as R};
        let point = match point {
            F::Role(role, boundary) => J::Role(
                match role {
                    R::Initialization => JournalRole::Initialization,
                    R::Manifest => JournalRole::Manifest,
                    R::Pending => JournalRole::Pending,
                    R::Segment => JournalRole::Segment,
                    R::Retirement => JournalRole::Retirement,
                },
                match boundary {
                    PrivateRolePoint::StageCreated => JournalRoleBoundary::StageCreated,
                    PrivateRolePoint::CreationRecorded => JournalRoleBoundary::CreationRecorded,
                    PrivateRolePoint::PartialStage => JournalRoleBoundary::PartialStage,
                    PrivateRolePoint::StageSynced => JournalRoleBoundary::StageSynced,
                    PrivateRolePoint::Published => JournalRoleBoundary::Published,
                    PrivateRolePoint::DirectorySynced => JournalRoleBoundary::DirectorySynced,
                    PrivateRolePoint::CleanupDecisionDurable => {
                        JournalRoleBoundary::CleanupDecisionDurable
                    }
                    PrivateRolePoint::DisplacedRemoved => JournalRoleBoundary::DisplacedRemoved,
                    PrivateRolePoint::FinalSynced => JournalRoleBoundary::FinalSynced,
                },
            ),
            F::PendingDurable => J::PendingDurable,
            F::PartialAppend => J::PartialAppend,
            F::SegmentSynced => J::SegmentSynced,
            F::ManifestCommitted => J::ManifestCommitted,
            F::PendingRemoved => J::PendingRemoved,
            F::Retired => J::Retired,
            F::SegmentRead(first_seq) => J::SegmentRead { first_seq },
        };
        self.at(point)
    }
}

struct RuntimeClock(Arc<dyn EventRuntime>);
impl EventRuntime for RuntimeClock {
    fn now(&self) -> Duration {
        self.0.now()
    }
    fn sleep(&self, duration: Duration) {
        self.0.sleep(duration);
    }
    fn cancelled(&self) -> bool {
        self.0.cancelled()
    }
}

/// A retained directory/lock/epoch binding. Replacements are never adopted on retry.
pub struct ControllerJournal {
    root: RootedDir,
    binding: fs::Binding,
    lock: fs::Binding,
    journal_id: uuid::Uuid,
    clock: RuntimeClock,
    hooks: Hooks,
}

fn io_unavailable(error: io::Error) -> WorkerError {
    // Never expose filesystem paths or arbitrary syscall text to event consumers.
    if let Some(stage) = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<fs::UnprovedStage>())
    {
        unavailable(&stage.to_string())
    } else if error.raw_os_error() == Some(libc::ESTALE) {
        unavailable("transient journal binding race; retryable")
    } else {
        unavailable("journal binding, contents or admission unavailable")
    }
}

fn check(runtime: &dyn EventRuntime, deadline: Duration) -> Result<(), WorkerError> {
    if runtime.cancelled() {
        return Err(WorkerError::Unavailable(format!(
            "{CONTROLLER_EVENTS_CANCELLED}: cancelled"
        )));
    }
    if runtime.now() >= deadline {
        return Err(unavailable("journal deadline exhausted"));
    }
    Ok(())
}

fn runtime_io(clock: &RuntimeClock, error: io::Error) -> WorkerError {
    if clock.0.cancelled() {
        WorkerError::Unavailable(format!("{CONTROLLER_EVENTS_CANCELLED}: cancelled"))
    } else {
        io_unavailable(error)
    }
}

fn journal_root(paths: &PathLayout, create: bool) -> io::Result<Option<RootedDir>> {
    let mut controller = match RootedDir::open_anchored_absolute(&paths.controller_state_root()) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !create => return Ok(None),
        Err(error) => return Err(error),
    };
    let device = controller.identity()?.device;
    controller.bind_host_device(device)?;
    match controller.open_private_direct_child_on_device("events", device) {
        Ok(root) => Ok(Some(root)),
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            Ok(Some(controller.create_new_child_directory("events")?))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

impl ControllerJournal {
    /// Epoch already established by initialization/open. Optional transport
    /// hints must not query journal health or acquire a journal fence.
    pub fn initialized_journal_id(&self) -> uuid::Uuid {
        self.journal_id
    }

    pub fn initialize_for_leader(
        paths: &PathLayout,
        leader: &ControllerLeader,
        options: JournalOptions,
    ) -> Result<Arc<Self>, WorkerError> {
        Self::initialize(paths, leader, options, Hooks(None))
    }

    #[doc(hidden)]
    pub fn initialize_for_leader_with_hook(
        paths: &PathLayout,
        leader: &ControllerLeader,
        options: JournalOptions,
        hook: Arc<dyn JournalFaultHook>,
    ) -> Result<Arc<Self>, WorkerError> {
        Self::initialize(paths, leader, options, Hooks(Some(hook)))
    }

    fn initialize(
        paths: &PathLayout,
        leader: &ControllerLeader,
        options: JournalOptions,
        hooks: Hooks,
    ) -> Result<Arc<Self>, WorkerError> {
        // Each flock has its own admission cap; successful filesystem work can
        // consume the longer cooperative operation budget between acquisitions.
        let deadline = options.runtime.now().saturating_add(RPC_BUDGET);
        check(options.runtime.as_ref(), deadline)?;
        if leader.identity().pid() != std::process::id() {
            return Err(unavailable(
                "initialization requires the local controller leader",
            ));
        }
        let root = journal_root(paths, true)
            .map_err(io_unavailable)?
            .ok_or_else(|| unavailable("controller leader root missing"))?;
        let binding = root.identity().map_err(io_unavailable)?.into();
        let clock = RuntimeClock(options.runtime);
        fs::retry_same_binding(&root, binding, deadline, &clock, || {
            fs::initialize_storage(&root, deadline, &clock, &hooks)
        })
        .map_err(|error| runtime_io(&clock, error))?;
        Self::attach(root, clock, hooks, deadline)
    }

    pub fn open_existing(
        paths: &PathLayout,
        options: JournalOptions,
    ) -> Result<Option<Arc<Self>>, WorkerError> {
        let deadline = options.runtime.now().saturating_add(RPC_BUDGET);
        Self::open_until(paths, options, deadline, Hooks(None))
    }

    #[doc(hidden)]
    pub fn open_existing_with_hook(
        paths: &PathLayout,
        options: JournalOptions,
        hook: Arc<dyn JournalFaultHook>,
    ) -> Result<Option<Arc<Self>>, WorkerError> {
        let deadline = options.runtime.now().saturating_add(RPC_BUDGET);
        Self::open_until(paths, options, deadline, Hooks(Some(hook)))
    }

    fn open_until(
        paths: &PathLayout,
        options: JournalOptions,
        deadline: Duration,
        hooks: Hooks,
    ) -> Result<Option<Arc<Self>>, WorkerError> {
        check(options.runtime.as_ref(), deadline)?;
        let Some(root) = journal_root(paths, false).map_err(io_unavailable)? else {
            return Ok(None);
        };
        Ok(Some(Self::attach(
            root,
            RuntimeClock(options.runtime),
            hooks,
            deadline,
        )?))
    }

    fn attach(
        root: RootedDir,
        clock: RuntimeClock,
        hooks: Hooks,
        deadline: Duration,
    ) -> Result<Arc<Self>, WorkerError> {
        let binding = root.identity().map_err(io_unavailable)?.into();
        let lock = root
            .private_entry_identity("journal.lock")
            .map_err(io_unavailable)?
            .into();
        let initialization = {
            let _shared = fs::acquire_lock(&root, lock, true, deadline, &clock)
                .map_err(|error| runtime_io(&clock, error))?;
            fs::load_initialization(&root).map_err(io_unavailable)?
        };
        let journal_id = uuid::Uuid::parse_str(&initialization.journal_id)
            .map_err(|_| unavailable("invalid journal epoch"))?;
        let journal = Arc::new(Self {
            root,
            binding,
            lock,
            journal_id,
            clock,
            hooks,
        });
        // Healthy attachment checks retained bindings and sizes. Ambiguous
        // state still takes full EX recovery; read_after validates served data.
        journal.with_manifest(deadline, |_| Ok(()))?;
        Ok(journal)
    }

    fn validate_epoch(&self) -> io::Result<()> {
        let initialization = fs::load_initialization(&self.root)?;
        if initialization.journal_id != self.journal_id.to_string()
            || initialization.lock != self.lock
        {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        Ok(())
    }

    fn with_manifest<T>(
        &self,
        deadline: Duration,
        read: impl FnMut(&fs::Manifest) -> io::Result<T>,
    ) -> Result<T, WorkerError> {
        check(self.clock.0.as_ref(), deadline)?;
        self.with_manifest_io(deadline, read)
            .map_err(|error| runtime_io(&self.clock, error))
    }

    fn with_manifest_io<T>(
        &self,
        deadline: Duration,
        mut read: impl FnMut(&fs::Manifest) -> io::Result<T>,
    ) -> io::Result<T> {
        fs::retry_same_binding(&self.root, self.binding, deadline, &self.clock, || {
            let shared = fs::acquire_lock(&self.root, self.lock, true, deadline, &self.clock)?;
            self.validate_epoch()?;
            self.hooks.at(JournalFaultPoint::ReadAttempt)?;
            if let Some(manifest) = fs::clean_manifest(&self.root, &self.hooks)? {
                return read(&manifest);
            }
            drop(shared);
            self.hooks.at(JournalFaultPoint::RecoveryAttempt)?;
            let _exclusive = fs::acquire_lock(&self.root, self.lock, false, deadline, &self.clock)?;
            self.validate_epoch()?;
            let manifest = fs::recover_append(&self.root, &self.hooks)?;
            read(&manifest)
        })
    }

    fn manifest_window(&self, manifest: &fs::Manifest) -> JournalWindow {
        JournalWindow {
            journal_id: self.journal_id,
            oldest_seq: Seq::new(manifest.oldest),
            head_seq: Seq::new(manifest.head),
        }
    }

    pub fn append(
        &self,
        batch: EventBatch,
        deadline: Duration,
    ) -> Result<EventCursor, WorkerError> {
        check(self.clock.0.as_ref(), deadline)?;
        // This exact plan survives ESTALE retries, including after manifest publication.
        let mut prepared = None;
        let manifest =
            fs::retry_same_binding(&self.root, self.binding, deadline, &self.clock, || {
                let _exclusive =
                    fs::acquire_lock(&self.root, self.lock, false, deadline, &self.clock)?;
                self.validate_epoch()?;
                if let Some(pending) = &prepared {
                    return fs::publish_append(&self.root, pending, &self.hooks);
                }
                let manifest = fs::recover_append(&self.root, &self.hooks)?;
                if batch.is_empty() {
                    return Ok(manifest);
                }
                let mut bytes = Vec::with_capacity(batch.len() * MAX_EVENT_BYTES);
                let millis = crate::controller::leader::now_millis()
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                for (index, event) in batch.events().iter().enumerate() {
                    let seq = manifest
                        .head
                        .checked_add(index as u64 + 1)
                        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
                    let event = event
                        .to_wire(self.journal_id, Seq::new(seq), millis)
                        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                    serde_json::to_writer(&mut bytes, &event).map_err(io::Error::other)?;
                    bytes.push(b'\n');
                }
                if bytes.len() > MAX_BATCH_BYTES {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                let segment = if fs::rotation_required(&manifest, bytes.len())? {
                    Some(fs::create_empty_segment(
                        &self.root,
                        &manifest.journal_id,
                        manifest
                            .head
                            .checked_add(1)
                            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?,
                        &self.hooks,
                    )?)
                } else {
                    None
                };
                prepared = Some(fs::prepare_append(&manifest, &bytes, segment)?);
                self.hooks.at(JournalFaultPoint::AppendPrepared)?;
                fs::publish_append(
                    &self.root,
                    prepared.as_ref().expect("prepared append"),
                    &self.hooks,
                )
            })
            .map_err(|error| runtime_io(&self.clock, error))?;
        Ok(self.manifest_window(&manifest).cursor())
    }
}

impl JournalReader for ControllerJournal {
    fn window(&self, deadline: Duration) -> Result<JournalWindow, WorkerError> {
        self.with_manifest(deadline, |manifest| Ok(self.manifest_window(manifest)))
    }

    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        let query = query.normalized();
        let wait_until = deadline.min(
            self.clock
                .0
                .now()
                .saturating_add(Duration::from_millis(query.wait_ms)),
        );
        loop {
            check(self.clock.0.as_ref(), deadline)?;
            let result = self.with_manifest_io(deadline, |manifest| {
                let window = self.manifest_window(manifest);
                let epoch = query
                    .after
                    .as_ref()
                    .map(|cursor| cursor.journal_id.to_string());
                let after = query
                    .after
                    .as_ref()
                    .map(|cursor| (epoch.as_deref().expect("cursor epoch"), cursor.seq.as_u64()));
                if let Some(reason) = fs::cursor_reason(manifest, after) {
                    return Ok(EventReadResult::SnapshotRequired(SnapshotRequired {
                        reason: reason.into(),
                        window,
                    }));
                }
                let cursor = query.after.as_ref().expect("validated cursor");
                let events = fs::read_after(
                    &self.root,
                    manifest,
                    cursor.seq.as_u64(),
                    query.limit,
                    &self.hooks,
                )?;
                let next_after = events.last().map_or_else(
                    || *cursor,
                    |event| EventCursor {
                        journal_id: self.journal_id,
                        seq: event.seq,
                    },
                );
                let result = EventReadResult::Batch(ReadBatch {
                    schema_version: SCHEMA_VERSION,
                    journal_id: self.journal_id,
                    oldest_seq: window.oldest_seq,
                    head_seq: window.head_seq,
                    has_more: next_after.seq < window.head_seq,
                    next_after,
                    events,
                });
                result
                    .validate()
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                Ok(result)
            });
            match result {
                Ok(result) => {
                    if !matches!(&result, EventReadResult::Batch(batch) if batch.events.is_empty())
                        || self.clock.0.now() >= wait_until
                    {
                        return Ok(result);
                    }
                }
                Err(error)
                    if error.kind() == io::ErrorKind::TimedOut
                        && !self.clock.0.cancelled()
                        && self.clock.0.now() < wait_until => {}
                Err(error) => return Err(runtime_io(&self.clock, error)),
            }
            // All SH/EX guards have dropped before the injected wait.
            self.clock.0.sleep(
                wait_until
                    .saturating_sub(self.clock.0.now())
                    .min(JOURNAL_CHECK_INTERVAL),
            );
        }
    }
}

impl JournalWriter for ControllerJournal {
    fn append(&self, batch: EventBatch, deadline: Duration) -> Result<EventCursor, WorkerError> {
        self.append(batch, deadline)
    }
}

pub struct ExistingJournalProvider {
    paths: PathLayout,
    runtime: Arc<dyn EventRuntime>,
}
impl ExistingJournalProvider {
    pub fn new(paths: PathLayout, runtime: Arc<dyn EventRuntime>) -> Self {
        Self { paths, runtime }
    }
}
impl JournalProvider for ExistingJournalProvider {
    fn open_existing(
        &self,
        deadline: Duration,
    ) -> Result<Option<Arc<dyn JournalReader>>, WorkerError> {
        Ok(ControllerJournal::open_until(
            &self.paths,
            JournalOptions {
                runtime: self.runtime.clone(),
            },
            deadline,
            Hooks(None),
        )?
        .map(|journal| journal as Arc<dyn JournalReader>))
    }
}

pub struct BoundedPublisher;
struct Sink(Arc<publisher::Queue<EventBatch>>);
impl EventSink for Sink {
    fn try_publish(&self, batch: EventBatch) -> PublishAttempt {
        match self.0.try_enqueue(batch) {
            publisher::EnqueueResult::Queued => PublishAttempt::Queued,
            _ => PublishAttempt::Dropped,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PublisherDiagnostic {
    pub code: &'static str,
    pub count: u64,
}

pub struct PublisherHandle {
    worker: publisher::WorkerHandle,
    queue: Arc<publisher::Queue<EventBatch>>,
    clock: RuntimeClock,
}
impl BoundedPublisher {
    pub fn start(
        journal: Arc<dyn JournalWriter>,
        runtime: Arc<dyn EventRuntime>,
    ) -> (Arc<dyn EventSink>, PublisherHandle) {
        let clock = runtime.clone();
        let (queue, worker) = publisher::start(
            move |batch| {
                journal
                    .append(batch, clock.now().saturating_add(RPC_BUDGET))
                    .map(|_| ())
                    .map_err(|_| ())
            },
            |batch: &EventBatch| batch.len() * MAX_EVENT_BYTES,
        );
        (
            Arc::new(Sink(queue.clone())),
            PublisherHandle {
                worker,
                queue,
                clock: RuntimeClock(runtime),
            },
        )
    }
}
impl PublisherHandle {
    pub fn finish_with_grace(&self, grace: Duration) {
        self.worker.finish_with_grace(grace, &self.clock);
    }
    pub fn stop_without_join(&self) {
        self.worker.stop_without_join();
    }
    /// Snapshot atomic counters outside producer fences; enqueue performs no logging.
    pub fn diagnostics(&self) -> Vec<PublisherDiagnostic> {
        self.queue
            .diagnostics()
            .into_iter()
            .map(|item| PublisherDiagnostic {
                code: item.code,
                count: item.count,
            })
            .collect()
    }
}
