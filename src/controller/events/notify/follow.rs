//! Foreground notification consumption with durable candidate admission.

use std::{
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use super::{NotifyCache, commit_then_deliver, plan_notifications};
use crate::{
    controller::events::{
        CONTROLLER_EVENTS_UNAVAILABLE, CONTROLLER_EVENTS_UNSUPPORTED, EventReadResult,
        EventReconciler, EventRuntime, EventSource, EventSupport, JOURNAL_CHECK_INTERVAL,
        NOTIFY_PENDING_CAPACITY, Notice, NoticeChannel, NotifyOptions, NotifyState,
        PendingCandidate, READ_DEFAULT_LIMIT, READ_FOLLOW_WAIT_MS, RECONNECT_BACKOFF,
        REPAIR_INTERVAL, RPC_BUDGET, ReadQuery, ReconcileInput, RepairProgress, TaskAddressQuery,
        TaskFactsBatch, TaskRepairPage, TaskRepairQuery,
    },
    error::WorkerError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyExit {
    Complete,
    Incomplete,
    Cancelled,
}

pub struct NotifyLoop<'a> {
    pub source: &'a dyn EventSource,
    pub reconciler: &'a mut dyn EventReconciler,
    pub cache: &'a NotifyCache,
    pub channels: &'a [Arc<dyn NoticeChannel>],
    pub options: &'a NotifyOptions,
    pub runtime: Arc<dyn EventRuntime>,
    pub stop_at: Option<Duration>,
}

impl NotifyLoop<'_> {
    #[cfg(any(test, feature = "test-support"))]
    pub fn run(self, diagnostics: &mut dyn Write) -> Result<NotifyExit, WorkerError> {
        if self.runtime.cancelled() {
            return Ok(NotifyExit::Cancelled);
        }
        let (saved, suppress) = load_baseline(self.cache, diagnostics)?;
        self.run_with_state(diagnostics, saved, suppress)
    }

    fn run_with_state(
        self,
        diagnostics: &mut dyn Write,
        saved: NotifyState,
        mut suppress: bool,
    ) -> Result<NotifyExit, WorkerError> {
        let stop_at = if self.options.follow {
            self.stop_at
        } else {
            Some(
                self.stop_at
                    .unwrap_or(Duration::MAX)
                    .min(self.runtime.now().saturating_add(RPC_BUDGET)),
            )
        };
        let staged = StagingSource {
            inner: self.source,
            cache: self.cache,
            state: Mutex::new(saved),
            journal_available: Mutex::new(None),
            unsupported: AtomicBool::new(false),
            fault: Mutex::new(None),
        };
        let channels: Vec<Arc<dyn NoticeChannel>> = self
            .channels
            .iter()
            .map(|channel| {
                Arc::new(CancelChannel {
                    inner: channel.clone(),
                    runtime: self.runtime.clone(),
                    stop_at,
                }) as Arc<dyn NoticeChannel>
            })
            .collect();
        let mut discovered = false;
        let mut cold = true;
        let mut repair_active = true;
        let mut next_repair = self.runtime.now().saturating_add(REPAIR_INTERVAL);
        let mut retry = 0usize;
        let mut state_only_reported = false;
        loop {
            if stopped(self.runtime.as_ref(), stop_at) {
                return finish_stopped(self.options, cold, self.runtime.cancelled(), diagnostics);
            }
            let deadline = operation_deadline(self.runtime.as_ref(), stop_at);
            if !discovered {
                match self.source.discover(deadline) {
                    Ok(EventSupport::Supported) => {
                        discovered = true;
                        retry = 0;
                        staged.unsupported.store(false, Ordering::Release);
                    }
                    Ok(EventSupport::Unsupported) => {
                        writeln!(
                            diagnostics,
                            "eligibility unknown: controller events unsupported"
                        )?;
                        if !self.options.follow {
                            return Ok(NotifyExit::Incomplete);
                        }
                        pause(self.runtime.as_ref(), Duration::from_secs(2), stop_at);
                        continue;
                    }
                    Err(_) => {
                        if stopped(self.runtime.as_ref(), stop_at) {
                            continue;
                        }
                        writeln!(
                            diagnostics,
                            "eligibility unknown: controller discovery unavailable; baseline incomplete"
                        )?;
                        if !self.options.follow {
                            return Ok(NotifyExit::Incomplete);
                        }
                        pause(
                            self.runtime.as_ref(),
                            RECONNECT_BACKOFF[retry.min(3)],
                            stop_at,
                        );
                        retry = retry.saturating_add(1);
                        continue;
                    }
                }
            }
            let mut repair_due = cold || self.runtime.now() >= next_repair;
            if self.runtime.now() >= next_repair {
                next_repair = self.runtime.now().saturating_add(REPAIR_INTERVAL);
            }
            let saved = staged.saved();
            let mut read = None;
            if !cold && !repair_active && !repair_due {
                if let Some(after) = saved.consumed_after {
                    let poll_deadline = deadline.min(next_repair);
                    let wait_ms = if saved.pending.is_empty() {
                        millis(
                            poll_deadline
                                .saturating_sub(self.runtime.now())
                                .saturating_sub(JOURNAL_CHECK_INTERVAL),
                        )
                        .min(READ_FOLLOW_WAIT_MS)
                    } else {
                        0
                    };
                    match staged.read(
                        ReadQuery {
                            after: Some(after),
                            limit: READ_DEFAULT_LIMIT,
                            wait_ms,
                        },
                        poll_deadline,
                    ) {
                        Ok(result) => read = Some(result),
                        Err(error) => {
                            staged.check_fault()?;
                            if stopped(self.runtime.as_ref(), stop_at) {
                                continue;
                            }
                            if error_has_code(&error, CONTROLLER_EVENTS_UNSUPPORTED) {
                                staged.unsupported.store(true, Ordering::Release);
                            } else {
                                repair_due = true;
                            }
                        }
                    }
                } else if saved.pending.is_empty() {
                    pause(
                        self.runtime.as_ref(),
                        next_repair.saturating_sub(self.runtime.now()),
                        stop_at,
                    );
                    continue;
                }
            }
            if staged.unsupported.load(Ordering::Acquire) {
                writeln!(
                    diagnostics,
                    "eligibility unknown: controller events unsupported"
                )?;
                if !self.options.follow {
                    return Ok(NotifyExit::Incomplete);
                }
                discovered = false;
                cold = true;
                repair_active = true;
                pause(self.runtime.as_ref(), Duration::from_secs(2), stop_at);
                continue;
            }
            if stopped(self.runtime.as_ref(), stop_at) {
                continue;
            }
            let had_events =
                matches!(&read, Some(EventReadResult::Batch(batch)) if !batch.events.is_empty());
            let input = ReconcileInput {
                read,
                repair_due,
                include_titles: !self.options.no_titles
                    && (cold || had_events || (!cold && !repair_active && !repair_due)),
            };
            let result = self.reconciler.reconcile(&staged, input, deadline);
            staged.check_fault()?;
            if staged.unsupported.load(Ordering::Acquire)
                || result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error_has_code(error, CONTROLLER_EVENTS_UNSUPPORTED))
            {
                writeln!(
                    diagnostics,
                    "eligibility unknown: controller events unsupported"
                )?;
                if !self.options.follow {
                    return Ok(NotifyExit::Incomplete);
                }
                discovered = false;
                cold = true;
                repair_active = true;
                pause(self.runtime.as_ref(), Duration::from_secs(2), stop_at);
                continue;
            }
            let mut result = match result {
                Ok(result) => result,
                Err(_) => {
                    if stopped(self.runtime.as_ref(), stop_at) {
                        continue;
                    }
                    writeln!(
                        diagnostics,
                        "notification baseline incomplete: confirmation or repair unavailable"
                    )?;
                    if !self.options.follow {
                        return Ok(NotifyExit::Incomplete);
                    }
                    pause(
                        self.runtime.as_ref(),
                        RECONNECT_BACKOFF[retry.min(3)].min(
                            next_repair
                                .saturating_sub(self.runtime.now())
                                .max(JOURNAL_CHECK_INTERVAL),
                        ),
                        stop_at,
                    );
                    retry = retry.saturating_add(1);
                    repair_active = true;
                    continue;
                }
            };
            result.validate()?;
            if !confirm_integrations(self.source, &mut result, deadline) {
                writeln!(
                    diagnostics,
                    "integration confirmation unavailable or changed; notification deferred"
                )?;
            }
            retry = 0;
            if *staged.journal_available.lock().unwrap() == Some(false) {
                result.consumed_after = None;
                if !state_only_reported {
                    writeln!(
                        diagnostics,
                        "event journal unavailable: state-only confirmation"
                    )?;
                    state_only_reported = true;
                }
            } else {
                state_only_reported = false;
            }
            let baseline_complete =
                result.repair == RepairProgress::Complete && !result.repair_needed;
            // TaskReconciler installs its projection before all addressed
            // confirmations finish. Keep initial history/attention cold for
            // policy across those pages, while preserving genuine warm
            // differences reported during the sweep.
            if cold
                && !result
                    .changes
                    .iter()
                    .any(|change| change.cause == super::super::ChangeCause::RepairDifference)
            {
                result.baseline = super::super::BaselineKind::Cold;
            }
            let mut options = self.options.clone();
            options.quiet |= suppress;
            let mut plan = plan_notifications(
                &staged.saved(),
                &result,
                &options,
                millis(self.runtime.now()),
            );
            // T7 keeps a non-null saved cursor when a result has none. State-only
            // confirmation must explicitly clear that transport position.
            plan.next.consumed_after = result.consumed_after;
            let next = plan.next.clone();
            let delivered =
                commit_then_deliver(self.cache, plan, &channels, self.runtime.as_ref())?;
            staged.replace(next);
            if delivered.contains(&false) && !stopped(self.runtime.as_ref(), stop_at) {
                writeln!(
                    diagnostics,
                    "notification channel failed; saved decision will not be retried"
                )?;
            }
            if baseline_complete {
                cold = false;
                suppress = false;
                if !self.options.follow {
                    return Ok(NotifyExit::Complete);
                }
            }
            repair_active = matches!(
                result.repair,
                RepairProgress::InProgress | RepairProgress::Restarted
            ) || result.repair_needed;
            if repair_active || (!cold && !baseline_complete && !had_events) {
                pause(self.runtime.as_ref(), JOURNAL_CHECK_INTERVAL, stop_at);
            }
        }
    }
}

fn confirm_integrations(
    source: &dyn EventSource,
    result: &mut crate::controller::events::Reconciliation,
    deadline: Duration,
) -> bool {
    use crate::integration::contracts::{IntegrationStatus, MAX_READ_TASKS, ValidateIntegration};
    let ids: std::collections::BTreeSet<_> = result
        .confirmed
        .iter()
        .chain(
            result
                .changes
                .iter()
                .filter_map(|change| change.current.as_ref()),
        )
        .filter(|facts| {
            facts.integration.as_ref().is_some_and(|annotation| {
                matches!(
                    annotation.state,
                    IntegrationStatus::Integrated | IntegrationStatus::Blocked
                )
            })
        })
        .map(|facts| facts.task_id)
        .collect();
    if ids.is_empty() {
        return true;
    }
    let mut snapshots = std::collections::BTreeMap::new();
    for chunk in ids.into_iter().collect::<Vec<_>>().chunks(MAX_READ_TASKS) {
        if let Ok(read) = source.integrations(chunk, deadline)
            && read.validate().is_ok()
            && read.integrations.keys().copied().collect::<Vec<_>>() == chunk
        {
            snapshots.extend(read.integrations);
        }
    }
    let mut complete = true;
    for facts in result.confirmed.iter_mut().chain(
        result
            .changes
            .iter_mut()
            .filter_map(|change| change.current.as_mut()),
    ) {
        if facts.integration.is_none() {
            continue;
        }
        facts.integration_confirmation = None;
        if !facts.integration.as_ref().is_some_and(|annotation| {
            matches!(
                annotation.state,
                IntegrationStatus::Integrated | IntegrationStatus::Blocked
            )
        }) {
            continue;
        }
        let confirmed = snapshots
            .get(&facts.task_id)
            .and_then(Option::as_ref)
            .is_some_and(|snapshot| facts.confirm_integration(snapshot));
        if !confirmed {
            complete = false;
            if result.pending_ids.len() < NOTIFY_PENDING_CAPACITY
                && !result.pending_ids.contains(&facts.task_id)
            {
                result.pending_ids.push(facts.task_id);
            }
        }
    }
    if !complete {
        result.repair_needed = true;
        result.attention = None;
    }
    complete
}

fn load_baseline(
    cache: &NotifyCache,
    diagnostics: &mut dyn Write,
) -> Result<(NotifyState, bool), WorkerError> {
    match cache.load() {
        Ok(state) => Ok((state, false)),
        Err(error) if error_has_code(&error, "CONTROLLER_EVENTS_NOTIFY_CACHE_CORRUPT") => {
            writeln!(
                diagnostics,
                "notification cache corrupt: rebuilding baseline with display suppressed"
            )?;
            Ok((cache.rebaseline()?, true))
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn error_has_code(error: &WorkerError, code: &str) -> bool {
    matches!(error, WorkerError::Unavailable(message) | WorkerError::Protocol(message) if message.starts_with(code))
}
pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
pub(crate) fn stopped(runtime: &dyn EventRuntime, stop_at: Option<Duration>) -> bool {
    runtime.cancelled() || stop_at.is_some_and(|deadline| runtime.now() >= deadline)
}
pub(crate) fn operation_deadline(
    runtime: &dyn EventRuntime,
    stop_at: Option<Duration>,
) -> Duration {
    runtime
        .now()
        .saturating_add(RPC_BUDGET)
        .min(stop_at.unwrap_or(Duration::MAX))
}
pub(crate) fn pause(runtime: &dyn EventRuntime, duration: Duration, stop_at: Option<Duration>) {
    let until = runtime
        .now()
        .saturating_add(duration)
        .min(stop_at.unwrap_or(Duration::MAX));
    while !stopped(runtime, stop_at) && runtime.now() < until {
        runtime.sleep(
            until
                .saturating_sub(runtime.now())
                .min(JOURNAL_CHECK_INTERVAL),
        );
    }
}
fn finish_stopped(
    options: &NotifyOptions,
    cold: bool,
    cancelled: bool,
    diagnostics: &mut dyn Write,
) -> Result<NotifyExit, WorkerError> {
    if !options.follow && cold && !cancelled {
        writeln!(
            diagnostics,
            "notification baseline incomplete: deadline exhausted"
        )?;
        Ok(NotifyExit::Incomplete)
    } else {
        Ok(NotifyExit::Cancelled)
    }
}

struct StagingSource<'a> {
    inner: &'a dyn EventSource,
    cache: &'a NotifyCache,
    state: Mutex<NotifyState>,
    journal_available: Mutex<Option<bool>>,
    unsupported: AtomicBool,
    fault: Mutex<Option<WorkerError>>,
}
impl StagingSource<'_> {
    fn saved(&self) -> NotifyState {
        self.state.lock().unwrap().clone()
    }
    fn replace(&self, state: NotifyState) {
        *self.state.lock().unwrap() = state;
    }
    fn check_fault(&self) -> Result<(), WorkerError> {
        self.fault.lock().unwrap().take().map_or(Ok(()), Err)
    }
    fn support<T>(&self, result: Result<T, WorkerError>) -> Result<T, WorkerError> {
        if result
            .as_ref()
            .err()
            .is_some_and(|error| error_has_code(error, CONTROLLER_EVENTS_UNSUPPORTED))
        {
            self.unsupported.store(true, Ordering::Release);
        }
        result
    }
}
impl EventSource for StagingSource<'_> {
    fn integrations(
        &self,
        task_ids: &[crate::task::TaskId],
        deadline: Duration,
    ) -> Result<crate::integration::contracts::IntegrationReadResult, WorkerError> {
        self.inner.integrations(task_ids, deadline)
    }
    fn discover(&self, deadline: Duration) -> Result<EventSupport, WorkerError> {
        self.inner.discover(deadline)
    }
    fn read(&self, query: ReadQuery, deadline: Duration) -> Result<EventReadResult, WorkerError> {
        let result = self.support(self.inner.read(query, deadline));
        if let Err(error) = &result {
            if error_has_code(error, CONTROLLER_EVENTS_UNAVAILABLE) {
                *self.journal_available.lock().unwrap() = Some(false);
            }
            return result;
        }
        let read = result?;
        read.validate()?;
        *self.journal_available.lock().unwrap() = Some(true);
        if let EventReadResult::Batch(batch) = &read {
            let mut state = self.state.lock().unwrap();
            for event in &batch.events {
                if let Some(task_id) = event.affected_task() {
                    if !state
                        .pending
                        .iter()
                        .any(|candidate| candidate.task_id == task_id)
                    {
                        if state.pending.len() < NOTIFY_PENDING_CAPACITY {
                            state.pending.push(PendingCandidate {
                                task_id,
                                turn_id: None,
                            });
                        } else {
                            state.repair_needed = true;
                        }
                    }
                } else {
                    state.repair_needed = true;
                }
            }
            // Includes replay reads made inside TaskReconciler, not just the
            // outer feed. The cursor remains unchanged until confirmation.
            if !batch.events.is_empty()
                && let Err(error) = self.cache.save(&state)
            {
                *self.fault.lock().unwrap() = Some(error);
                return Err(WorkerError::Unavailable(
                    "CONTROLLER_EVENTS_UNAVAILABLE: candidate persistence failed".into(),
                ));
            }
        }
        Ok(read)
    }
    fn tasks(
        &self,
        query: TaskAddressQuery,
        deadline: Duration,
    ) -> Result<TaskFactsBatch, WorkerError> {
        self.support(self.inner.tasks(query, deadline))
    }
    fn repair(
        &self,
        query: TaskRepairQuery,
        deadline: Duration,
    ) -> Result<TaskRepairPage, WorkerError> {
        self.support(self.inner.repair(query, deadline))
    }
}

struct CancelChannel {
    inner: Arc<dyn NoticeChannel>,
    runtime: Arc<dyn EventRuntime>,
    stop_at: Option<Duration>,
}
impl NoticeChannel for CancelChannel {
    fn deliver(&self, notice: &Notice, budget: Duration) -> Result<(), WorkerError> {
        if stopped(self.runtime.as_ref(), self.stop_at) {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_CANCELLED: cancelled".into(),
            ));
        }
        self.inner.deliver(
            notice,
            budget.min(
                self.stop_at
                    .unwrap_or(Duration::MAX)
                    .saturating_sub(self.runtime.now()),
            ),
        )
    }
}

pub(crate) fn run_command(
    cli: &crate::cli::Cli,
    context: &crate::RuntimeContext,
    _stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| {
        let (paths, config) = super::super::foreground::configuration(cli, context)?;
        let crate::cli::Command::Notify {
            follow,
            quiet,
            channel,
            no_titles,
        } = &cli.command
        else {
            unreachable!("notify dispatch");
        };
        let options = NotifyOptions {
            follow: *follow,
            quiet: *quiet,
            no_titles: *no_titles,
            channel: match channel.as_str() {
                "macos" => super::super::NotifyChannel::Macos,
                "herdr" => super::super::NotifyChannel::Herdr,
                "both" => super::super::NotifyChannel::Both,
                _ => super::super::NotifyChannel::Auto,
            },
        };
        let cache = NotifyCache::open(&paths, &config.controller)?;
        let (saved, suppress) = load_baseline(&cache, stderr)?;
        let runtime = super::super::foreground::ForegroundRuntime::install()?;
        let runner: Arc<dyn crate::process::ProcessRunner> =
            Arc::new(crate::process::SystemProcessRunner);
        let source = super::super::foreground::event_client(
            runner.clone(),
            crate::controller::channel::ReadLoopScope::Notify,
            &paths,
            &config,
            runtime.clone(),
            context,
        );
        let mut reconciler = super::super::client::TaskReconciler::new(
            super::super::PreviousProjection::Absent,
            saved.consumed_after,
            saved
                .pending
                .iter()
                .map(|candidate| candidate.task_id)
                .collect(),
            runtime.clone(),
        );
        let socket = super::laptop_notification_socket(context.home(), |key| {
            context
                .environment()
                .get(std::ffi::OsStr::new(key))
                .cloned()
        });
        let reachable = !options.quiet && super::herdr_socket_reachable(&socket);
        if let (_, Some(diagnostic)) =
            super::select_channels(&options, &config.notifications, reachable)
        {
            writeln!(stderr, "{diagnostic}")?;
        }
        let channels =
            super::channels_for(&options, &config.notifications, socket, reachable, runner);
        NotifyLoop {
            source: &source,
            reconciler: &mut reconciler,
            cache: &cache,
            channels: &channels,
            options: &options,
            runtime,
            stop_at: None,
        }
        .run_with_state(stderr, saved, suppress)
    })();
    match result {
        Ok(NotifyExit::Incomplete) => crate::error::ExitKind::Unavailable as u8,
        Ok(NotifyExit::Complete | NotifyExit::Cancelled) => 0,
        Err(error) => super::super::foreground::report(error, stderr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::ControllerConfig,
        controller::events::{
            AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventCursor,
            EventReadResult, EventSupport, ReadBatch, ReadQuery, ReconcileInput, Reconciliation,
            RepairProgress, SCHEMA_VERSION, SafeOutcome, Seq, TaskAddressQuery, TaskFacts,
            WireEvent,
            testing::{
                FakeEventReconciler, ManualEventRuntime, RecordingNoticeChannel,
                ScriptedEventSource,
            },
        },
        paths::PathLayout,
        task::{TaskId, TurnId},
    };
    use std::sync::Mutex;

    fn cursor(seq: u64) -> EventCursor {
        EventCursor {
            journal_id: uuid::Uuid::from_u128(1),
            seq: Seq::new(seq),
        }
    }
    fn facts(outcome: SafeOutcome) -> TaskFacts {
        TaskFacts::test_terminal(
            TaskId::new(uuid::Uuid::from_u128(2)),
            TurnId::new(uuid::Uuid::from_u128(3)),
            outcome,
            true,
        )
    }
    fn complete() -> Reconciliation {
        Reconciliation::test_cold(Some(cursor(5)), Vec::new())
    }
    fn progress() -> Reconciliation {
        Reconciliation {
            repair: RepairProgress::InProgress,
            repair_needed: true,
            consumed_after: None,
            ..complete()
        }
    }
    fn fresh() -> Reconciliation {
        let current = facts(SafeOutcome::Done);
        Reconciliation {
            consumed_after: Some(cursor(6)),
            baseline: BaselineKind::Warm,
            changes: vec![DerivedTaskChange {
                task_id: current.task_id,
                previous: None,
                current: Some(current.clone()),
                cause: ChangeCause::ReplayTerminal {
                    turn_id: current.latest_turn_id.unwrap(),
                    outcome: SafeOutcome::Done,
                },
            }],
            confirmed: vec![current],
            pending_ids: Vec::new(),
            repair: RepairProgress::NotStarted,
            attention: None,
            repair_needed: false,
        }
    }
    fn batch(after: u64, event: bool) -> EventReadResult {
        let seq = after + u64::from(event);
        EventReadResult::Batch(ReadBatch {
            schema_version: SCHEMA_VERSION,
            journal_id: cursor(0).journal_id,
            oldest_seq: Seq::new(1),
            head_seq: Seq::new(seq),
            next_after: cursor(seq),
            events: if event {
                vec![WireEvent {
                    schema_version: 1,
                    journal_id: cursor(0).journal_id,
                    seq: Seq::new(seq),
                    time_millis: 1,
                    kind: "turn.finished".into(),
                    data: serde_json::json!({"task_id":facts(SafeOutcome::Done).task_id,"run_id":null,"turn_id":facts(SafeOutcome::Done).latest_turn_id,"outcome":"done","code":null}),
                }]
            } else {
                Vec::new()
            },
            has_more: false,
        })
    }

    struct Fixture {
        _root: tempfile::TempDir,
        cache: NotifyCache,
        source: ScriptedEventSource,
        reconciler: FakeEventReconciler,
        runtime: Arc<ManualEventRuntime>,
        channel: Arc<RecordingNoticeChannel>,
    }
    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().canonicalize().unwrap();
            let paths = PathLayout {
                config: path.join("config"),
                state: path.join("state"),
                cache: path.join("cache"),
                data: path.join("data"),
            };
            let cache = NotifyCache::open(
                &paths,
                &ControllerConfig {
                    enabled: true,
                    ssh: "fixture-only".into(),
                    remote_binary: "worker".into(),
                },
            )
            .unwrap();
            Self {
                _root: root,
                cache,
                source: ScriptedEventSource::new(),
                reconciler: FakeEventReconciler::new(),
                runtime: Arc::new(ManualEventRuntime::new()),
                channel: Arc::new(RecordingNoticeChannel::new()),
            }
        }
        fn supported(&self) {
            self.source
                .queue_discovery(Ok(EventSupport::Supported))
                .unwrap();
        }
        fn run(
            &mut self,
            options: NotifyOptions,
            stop_at: Option<Duration>,
        ) -> (NotifyExit, String) {
            let channels: Vec<Arc<dyn NoticeChannel>> = vec![self.channel.clone()];
            let mut diagnostics = Vec::new();
            let exit = NotifyLoop {
                source: &self.source,
                reconciler: &mut self.reconciler,
                cache: &self.cache,
                channels: &channels,
                options: &options,
                runtime: self.runtime.clone(),
                stop_at,
            }
            .run(&mut diagnostics)
            .unwrap();
            (exit, String::from_utf8(diagnostics).unwrap())
        }
    }

    #[test]
    fn integration_lost_hints_confirm_through_companion_and_restart_without_replay() {
        use crate::integration::{contracts::*, testing::*};
        for state in [IntegrationStatus::Integrated, IntegrationStatus::Blocked] {
            let mut h = Fixture::new();
            let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
            snapshot.state = state;
            snapshot.epoch = 1;
            let outcome = if state == IntegrationStatus::Integrated {
                snapshot.disposition = Some(IntegrationDisposition::Merged);
                snapshot.merge_oid = Some("e".repeat(40).parse().unwrap());
                SafeOutcome::Done
            } else {
                snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
                SafeOutcome::Blocked
            };
            let mut current = facts(outcome);
            current.integration = Some(snapshot.annotation().unwrap());
            current.code = snapshot
                .blocked_code
                .map(|code| crate::controller::events::SafeCode::from_public_code(code.as_str()));
            let mut result = fresh();
            result.changes[0].current = Some(current.clone());
            result.changes[0].cause = ChangeCause::RepairDifference;
            result.confirmed = vec![current.clone()];
            result.repair = RepairProgress::Complete;
            h.supported();
            h.source
                .queue_integrations(Ok(IntegrationReadResult {
                    schema_version: 1,
                    integrations: [(current.task_id, Some(snapshot.clone()))].into(),
                }))
                .unwrap();
            h.reconciler.queue(Ok(result)).unwrap();
            assert_eq!(
                h.run(NotifyOptions::default(), None).0,
                NotifyExit::Complete
            );
            assert_eq!(h.channel.records().len(), 1, "{state:?}");
            assert!(
                h.source.requests().is_empty(),
                "lost hint is repaired from facts, with no journal read"
            );
            h.supported();
            h.source
                .queue_integrations(Ok(IntegrationReadResult {
                    schema_version: 1,
                    integrations: [(current.task_id, Some(snapshot))].into(),
                }))
                .unwrap();
            h.reconciler
                .queue(Ok(Reconciliation::test_cold(
                    Some(cursor(6)),
                    vec![current],
                )))
                .unwrap();
            assert_eq!(
                h.run(NotifyOptions::default(), None).0,
                NotifyExit::Complete
            );
            assert_eq!(h.channel.records().len(), 1);
        }
    }

    #[test]
    fn integration_changed_companion_defers_notice_and_keeps_durable_candidate() {
        use crate::integration::{contracts::*, testing::*};
        let mut h = Fixture::new();
        let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
        snapshot.state = IntegrationStatus::Blocked;
        snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
        let mut current = facts(SafeOutcome::Blocked);
        current.integration = Some(snapshot.annotation().unwrap());
        current.code = Some(crate::controller::events::SafeCode::from_public_code(
            "INTEGRATION_CHECKS_FAILED",
        ));
        let mut result = fresh();
        result.changes[0].current = Some(current.clone());
        result.changes[0].cause = ChangeCause::RepairDifference;
        result.confirmed = vec![current.clone()];
        result.repair = RepairProgress::Complete;
        snapshot.revision = snapshot.revision.next().unwrap();
        h.supported();
        h.source
            .queue_integrations(Ok(IntegrationReadResult {
                schema_version: 1,
                integrations: [(current.task_id, Some(snapshot))].into(),
            }))
            .unwrap();
        h.reconciler.queue(Ok(result)).unwrap();
        let (exit, diagnostics) = h.run(NotifyOptions::default(), Some(Duration::from_secs(1)));
        assert_eq!(exit, NotifyExit::Incomplete);
        assert!(diagnostics.contains("confirmation unavailable or changed"));
        assert!(h.channel.records().is_empty());
        assert!(
            h.cache
                .load()
                .unwrap()
                .pending
                .iter()
                .any(|candidate| candidate.task_id == current.task_id)
        );
        assert!(h.cache.load().unwrap().decisions.is_empty());
    }

    #[test]
    fn saved_cursor_still_starts_a_cold_baseline_without_historical_banners() {
        let mut h = Fixture::new();
        h.supported();
        let mut saved = crate::controller::events::NotifyState::empty();
        saved.consumed_after = Some(cursor(4));
        h.cache.save(&saved).unwrap();
        h.reconciler
            .queue(Ok(Reconciliation::test_cold(
                Some(cursor(5)),
                vec![facts(SafeOutcome::Done)],
            )))
            .unwrap();
        assert_eq!(
            h.run(NotifyOptions::default(), None).0,
            NotifyExit::Complete
        );
        assert!(h.reconciler.inputs()[0].0.repair_due);
        assert!(h.reconciler.inputs()[0].0.read.is_none());
        assert!(h.channel.records().is_empty());
        let saved = h.cache.load().unwrap();
        assert_eq!(saved.consumed_after, Some(cursor(5)));
        assert_eq!(saved.decisions.len(), 1);
    }

    #[test]
    fn one_shot_finishes_every_baseline_page_before_exiting() {
        let mut h = Fixture::new();
        h.supported();
        for result in [progress(), progress(), complete()] {
            h.reconciler.queue(Ok(result)).unwrap();
        }
        assert_eq!(
            h.run(NotifyOptions::default(), None).0,
            NotifyExit::Complete
        );
        assert_eq!(h.reconciler.inputs().len(), 3);
        assert!(h.source.requests().is_empty());
        assert!(
            h.reconciler.inputs()[1..]
                .iter()
                .all(|(input, _)| input.include_titles)
        );
    }

    #[test]
    fn one_shot_failure_reports_incomplete_without_raw_transport_text() {
        let mut h = Fixture::new();
        h.supported();
        h.reconciler
            .queue(Err(WorkerError::Unavailable(
                "private /home/token-secret".into(),
            )))
            .unwrap();
        let (exit, diagnostics) = h.run(NotifyOptions::default(), None);
        assert_eq!(exit, NotifyExit::Incomplete);
        assert!(diagnostics.contains("incomplete"));
        assert!(!diagnostics.contains("token-secret"));
        assert!(h.channel.records().is_empty());
    }

    #[test]
    fn deadline_ends_an_unfinished_one_shot_with_an_explicit_diagnostic() {
        let mut h = Fixture::new();
        h.supported();
        for _ in 0..20 {
            h.reconciler.queue(Ok(progress())).unwrap();
        }
        let (exit, diagnostics) = h.run(NotifyOptions::default(), Some(Duration::from_secs(1)));
        assert_eq!(exit, NotifyExit::Incomplete);
        assert!(diagnostics.contains("incomplete"));
        assert!(h.cache.load().unwrap().repair_needed);
    }

    #[test]
    fn unsupported_uses_reduced_discovery_polling_without_reads_or_banners() {
        let mut h = Fixture::new();
        for _ in 0..10 {
            h.source
                .queue_discovery(Ok(EventSupport::Unsupported))
                .unwrap();
        }
        let (exit, diagnostics) = h.run(
            NotifyOptions {
                follow: true,
                ..NotifyOptions::default()
            },
            Some(Duration::from_secs(20)),
        );
        assert_eq!(exit, NotifyExit::Cancelled);
        assert!(diagnostics.contains("eligibility unknown"));
        assert_eq!(h.source.discovery_deadlines().len(), 10);
        assert!(h.source.requests().is_empty());
        assert!(h.reconciler.inputs().is_empty());
        assert!(h.channel.records().is_empty());
    }

    #[test]
    fn rollback_after_discovery_stops_confirmation_and_delivery() {
        let mut h = Fixture::new();
        h.supported();
        h.reconciler.queue(Ok(complete())).unwrap();
        h.reconciler.queue(Ok(fresh())).unwrap();
        h.source
            .queue_read(Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_UNSUPPORTED: legacy selector rejection".into(),
            )))
            .unwrap();
        let (_, diagnostics) = h.run(
            NotifyOptions {
                follow: true,
                ..NotifyOptions::default()
            },
            Some(Duration::from_secs(1)),
        );
        assert!(diagnostics.contains("eligibility unknown"));
        assert_eq!(h.reconciler.inputs().len(), 1);
        assert!(h.channel.records().is_empty());
    }

    #[test]
    fn quiet_and_no_titles_consume_state_without_fetching_titles_or_channels() {
        let mut h = Fixture::new();
        h.supported();
        h.reconciler.queue(Ok(complete())).unwrap();
        h.reconciler.queue(Ok(fresh())).unwrap();
        h.source.queue_read(Ok(batch(5, true))).unwrap();
        let clock = h.runtime.clone();
        h.runtime.on_sleep(move |_| clock.cancel());
        h.run(
            NotifyOptions {
                follow: true,
                quiet: true,
                no_titles: true,
                ..NotifyOptions::default()
            },
            None,
        );
        assert!(
            h.reconciler
                .inputs()
                .iter()
                .all(|(input, _)| !input.include_titles)
        );
        assert!(h.channel.records().is_empty());
        assert_eq!(h.cache.load().unwrap().decisions.len(), 1);
        assert_eq!(h.cache.load().unwrap().consumed_after, Some(cursor(6)));
    }

    struct Advancing<'a> {
        inner: &'a mut FakeEventReconciler,
        clock: Arc<ManualEventRuntime>,
    }
    impl EventReconciler for Advancing<'_> {
        fn reconcile(
            &mut self,
            source: &dyn EventSource,
            input: ReconcileInput,
            deadline: Duration,
        ) -> Result<Reconciliation, WorkerError> {
            let result = self.inner.reconcile(source, input, deadline);
            self.clock.advance(Duration::from_secs(5));
            result
        }
    }

    #[test]
    fn busy_feed_cannot_postpone_repair_and_polls_stop_at_the_next_deadline() {
        let mut h = Fixture::new();
        h.supported();
        h.reconciler.queue(Ok(complete())).unwrap();
        for _ in 0..5 {
            let mut result = complete();
            result.baseline = BaselineKind::Warm;
            result.repair = RepairProgress::NotStarted;
            h.reconciler.queue(Ok(result)).unwrap();
            h.source.queue_read(Ok(batch(5, true))).unwrap();
        }
        let channels: Vec<Arc<dyn NoticeChannel>> = vec![h.channel.clone()];
        let options = NotifyOptions {
            follow: true,
            ..NotifyOptions::default()
        };
        let mut advancing = Advancing {
            inner: &mut h.reconciler,
            clock: h.runtime.clone(),
        };
        NotifyLoop {
            source: &h.source,
            reconciler: &mut advancing,
            cache: &h.cache,
            channels: &channels,
            options: &options,
            runtime: h.runtime.clone(),
            stop_at: Some(Duration::from_secs(26)),
        }
        .run(&mut Vec::new())
        .unwrap();
        assert!(
            h.reconciler.inputs()[1..]
                .iter()
                .any(|(input, _)| input.repair_due)
        );
        assert!(
            h.reconciler.inputs()[1..]
                .iter()
                .filter(|(input, _)| input.repair_due)
                .all(|(input, _)| !input.include_titles)
        );
        for request in h.source.requests() {
            if let crate::controller::events::EventSelector::Read(query) = request.selector {
                assert!(request.deadline <= Duration::from_secs(26));
                assert!(query.wait_ms <= 10_000);
            }
        }
    }

    struct AdmissionCheck<'a> {
        inner: &'a mut FakeEventReconciler,
        cache: &'a NotifyCache,
        checked: bool,
    }
    impl EventReconciler for AdmissionCheck<'_> {
        fn reconcile(
            &mut self,
            source: &dyn EventSource,
            input: ReconcileInput,
            deadline: Duration,
        ) -> Result<Reconciliation, WorkerError> {
            if input.read.is_some() {
                let state = self.cache.load().unwrap();
                assert_eq!(state.consumed_after, Some(cursor(5)));
                assert_eq!(state.pending[0].task_id, facts(SafeOutcome::Done).task_id);
                self.checked = true;
            }
            self.inner.reconcile(source, input, deadline)
        }
    }
    struct SaveCheck {
        cache_file: std::path::PathBuf,
        records: RecordingNoticeChannel,
        clock: Arc<ManualEventRuntime>,
    }
    impl NoticeChannel for SaveCheck {
        fn deliver(
            &self,
            notice: &crate::controller::events::Notice,
            deadline: Duration,
        ) -> Result<(), WorkerError> {
            let state: crate::controller::events::NotifyState =
                serde_json::from_slice(&std::fs::read(&self.cache_file).unwrap()).unwrap();
            assert_eq!(state.consumed_after, Some(cursor(6)));
            assert_eq!(state.decisions.len(), 1);
            self.records.deliver(notice, deadline)?;
            self.clock.cancel();
            Ok(())
        }
    }
    fn cache_file(root: &std::path::Path) -> std::path::PathBuf {
        let events = root.join("cache/controller/events");
        std::fs::read_dir(events)
            .unwrap()
            .map(Result::unwrap)
            .find(|entry| {
                entry.file_type().unwrap().is_dir()
                    && entry.file_name().to_string_lossy().len() == 64
            })
            .unwrap()
            .path()
            .join("notify.json")
    }

    #[test]
    fn candidates_are_saved_before_reconciliation_and_decisions_before_channels() {
        let mut h = Fixture::new();
        h.supported();
        h.reconciler.queue(Ok(complete())).unwrap();
        h.reconciler.queue(Ok(fresh())).unwrap();
        h.source.queue_read(Ok(batch(5, true))).unwrap();
        let channel = Arc::new(SaveCheck {
            cache_file: cache_file(h._root.path()),
            records: RecordingNoticeChannel::new(),
            clock: h.runtime.clone(),
        });
        let channels: Vec<Arc<dyn NoticeChannel>> = vec![channel.clone()];
        let mut checked = AdmissionCheck {
            inner: &mut h.reconciler,
            cache: &h.cache,
            checked: false,
        };
        NotifyLoop {
            source: &h.source,
            reconciler: &mut checked,
            cache: &h.cache,
            channels: &channels,
            options: &NotifyOptions {
                follow: true,
                ..NotifyOptions::default()
            },
            runtime: h.runtime.clone(),
            stop_at: None,
        }
        .run(&mut Vec::new())
        .unwrap();
        assert!(checked.checked);
        assert_eq!(channel.records.records().len(), 1);
    }

    struct ReadDuringReconcile<'a> {
        inner: &'a mut FakeEventReconciler,
    }
    impl EventReconciler for ReadDuringReconcile<'_> {
        fn reconcile(
            &mut self,
            source: &dyn EventSource,
            input: ReconcileInput,
            deadline: Duration,
        ) -> Result<Reconciliation, WorkerError> {
            let _ = source.read(ReadQuery::default(), deadline);
            self.inner.reconcile(source, input, deadline)
        }
    }
    #[test]
    fn unavailable_journal_keeps_supported_state_only_confirmation_and_null_cursor() {
        let mut h = Fixture::new();
        h.supported();
        let mut saved = crate::controller::events::NotifyState::empty();
        saved.consumed_after = Some(cursor(4));
        h.cache.save(&saved).unwrap();
        h.source
            .queue_read(Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_UNAVAILABLE: no journal".into(),
            )))
            .unwrap();
        let attention = facts(SafeOutcome::NeedsInput);
        let mut result = Reconciliation::test_cold(Some(cursor(4)), vec![attention]);
        result.attention = Some(AttentionSummary {
            count: 1,
            fingerprint: "a".repeat(64),
        });
        h.reconciler.queue(Ok(result)).unwrap();
        let mut probing = ReadDuringReconcile {
            inner: &mut h.reconciler,
        };
        let channels: Vec<Arc<dyn NoticeChannel>> = vec![h.channel.clone()];
        let mut diagnostics = Vec::new();
        assert_eq!(
            NotifyLoop {
                source: &h.source,
                reconciler: &mut probing,
                cache: &h.cache,
                channels: &channels,
                options: &NotifyOptions::default(),
                runtime: h.runtime.clone(),
                stop_at: None
            }
            .run(&mut diagnostics)
            .unwrap(),
            NotifyExit::Complete
        );
        assert_eq!(h.cache.load().unwrap().consumed_after, None);
        assert_eq!(h.channel.records().len(), 1);
        assert!(
            !String::from_utf8(diagnostics)
                .unwrap()
                .contains("eligibility unknown")
        );
    }

    struct CancelChannel {
        clock: Arc<ManualEventRuntime>,
        records: Mutex<usize>,
    }
    impl NoticeChannel for CancelChannel {
        fn deliver(
            &self,
            _: &crate::controller::events::Notice,
            _: Duration,
        ) -> Result<(), WorkerError> {
            *self.records.lock().unwrap() += 1;
            self.clock.cancel();
            Ok(())
        }
    }
    #[test]
    fn cancellation_between_both_channels_prevents_the_second_attempt() {
        let mut h = Fixture::new();
        h.supported();
        let mut result =
            Reconciliation::test_cold(Some(cursor(5)), vec![facts(SafeOutcome::NeedsInput)]);
        result.attention = Some(AttentionSummary {
            count: 1,
            fingerprint: "a".repeat(64),
        });
        h.reconciler.queue(Ok(result)).unwrap();
        let cancel = Arc::new(CancelChannel {
            clock: h.runtime.clone(),
            records: Mutex::new(0),
        });
        let channels: Vec<Arc<dyn NoticeChannel>> = vec![cancel.clone(), h.channel.clone()];
        NotifyLoop {
            source: &h.source,
            reconciler: &mut h.reconciler,
            cache: &h.cache,
            channels: &channels,
            options: &NotifyOptions::default(),
            runtime: h.runtime.clone(),
            stop_at: None,
        }
        .run(&mut Vec::new())
        .unwrap();
        assert_eq!(*cancel.records.lock().unwrap(), 1);
        assert!(h.channel.records().is_empty());
        assert_eq!(h.cache.load().unwrap().decisions.len(), 1);
    }

    #[test]
    fn cancellation_before_start_admits_no_discovery_read_or_channel() {
        let mut h = Fixture::new();
        h.runtime.cancel();
        assert_eq!(
            h.run(NotifyOptions::default(), None).0,
            NotifyExit::Cancelled
        );
        assert!(h.source.discovery_deadlines().is_empty());
        assert!(h.reconciler.inputs().is_empty());
        assert!(h.channel.records().is_empty());
    }

    struct MemorySource {
        journal: crate::controller::events::testing::MemoryJournal,
        tasks: crate::controller::events::testing::MemoryTaskReader,
    }
    impl EventSource for MemorySource {
        fn discover(&self, _: Duration) -> Result<EventSupport, WorkerError> {
            Ok(EventSupport::Supported)
        }
        fn read(
            &self,
            query: ReadQuery,
            deadline: Duration,
        ) -> Result<EventReadResult, WorkerError> {
            crate::controller::events::JournalReader::read(&self.journal, query, deadline)
        }
        fn tasks(
            &self,
            query: TaskAddressQuery,
            deadline: Duration,
        ) -> Result<TaskFactsBatch, WorkerError> {
            crate::controller::events::TaskProjectionReader::addressed(&self.tasks, query, deadline)
        }
        fn repair(
            &self,
            query: TaskRepairQuery,
            deadline: Duration,
        ) -> Result<TaskRepairPage, WorkerError> {
            crate::controller::events::TaskProjectionReader::repair(&self.tasks, query, deadline)
        }
    }
    fn memory_source(h: &Fixture, row: TaskFacts, hint: bool) -> MemorySource {
        use crate::controller::events::{EventBatch, JournalWriter, NewEvent, TurnHint};
        let source = MemorySource {
            journal: crate::controller::events::testing::MemoryJournal::with_runtime(
                h.runtime.clone(),
            ),
            tasks: crate::controller::events::testing::MemoryTaskReader::with_runtime(
                h.runtime.clone(),
            ),
        };
        if hint {
            source
                .journal
                .append(
                    EventBatch::try_new(vec![NewEvent::TurnFinished(TurnHint {
                        task_id: row.task_id,
                        run_id: None,
                        turn_id: row.latest_turn_id.unwrap(),
                        outcome: row.outcome.unwrap(),
                        code: None,
                    })])
                    .unwrap(),
                    Duration::from_secs(30),
                )
                .unwrap();
        }
        source.tasks.insert(row).unwrap();
        source
    }
    fn real_reconciler_loop(
        h: &Fixture,
        source: &MemorySource,
        previous: crate::controller::events::PreviousProjection,
        saved_cursor: Option<EventCursor>,
        options: NotifyOptions,
    ) -> NotifyExit {
        let mut reconciler = crate::controller::events::client::TaskReconciler::new(
            previous,
            saved_cursor,
            Vec::new(),
            h.runtime.clone(),
        );
        let channels: Vec<Arc<dyn NoticeChannel>> = vec![h.channel.clone()];
        NotifyLoop {
            source,
            reconciler: &mut reconciler,
            cache: &h.cache,
            channels: &channels,
            options: &options,
            runtime: h.runtime.clone(),
            stop_at: options.follow.then_some(Duration::from_secs(1)),
        }
        .run(&mut Vec::new())
        .unwrap()
    }

    #[test]
    fn real_t4_unchanged_warm_repair_and_duplicate_hint_stay_silent_after_ring_eviction() {
        use crate::controller::events::PreviousProjection;
        for hint in [false, true] {
            let h = Fixture::new();
            let current = facts(SafeOutcome::Done);
            let source = memory_source(&h, current.clone(), hint);
            let mut saved = NotifyState::empty();
            saved.consumed_after = Some(cursor(0));
            saved.decisions = (0..4096).map(|n| format!("{n:064x}")).collect();
            h.cache.save(&saved).unwrap();
            assert_eq!(
                real_reconciler_loop(
                    &h,
                    &source,
                    PreviousProjection::Present([(current.task_id, current)].into()),
                    Some(cursor(0)),
                    NotifyOptions::default()
                ),
                NotifyExit::Complete
            );
            assert!(h.channel.records().is_empty());
        }
    }

    #[test]
    fn real_t4_retained_post_cursor_hint_notifies_but_lost_cold_history_does_not() {
        use crate::controller::events::PreviousProjection;
        for (saved_seq, wanted) in [(0, 1), (1, 0)] {
            let h = Fixture::new();
            let mut current = facts(SafeOutcome::Done);
            current.title = Some("Fixture task".into());
            let source = memory_source(&h, current, true);
            let mut saved = NotifyState::empty();
            saved.consumed_after = Some(cursor(saved_seq));
            h.cache.save(&saved).unwrap();
            assert_eq!(
                real_reconciler_loop(
                    &h,
                    &source,
                    PreviousProjection::Absent,
                    Some(cursor(saved_seq)),
                    NotifyOptions::default()
                ),
                NotifyExit::Complete
            );
            assert_eq!(h.channel.records().len(), wanted);
            if wanted == 1 {
                assert!(
                    source
                        .tasks
                        .addressed_requests()
                        .iter()
                        .any(|query| query.include_titles)
                );
            }
        }
    }

    #[test]
    fn real_t4_busy_to_quiescent_repair_notifies_the_same_turn_once() {
        use crate::controller::events::PreviousProjection;
        let h = Fixture::new();
        let current = facts(SafeOutcome::Done);
        let mut previous = current.clone();
        previous.runner_present = true;
        previous.busy = Some(true);
        previous.quiescent = Some(false);
        let source = memory_source(&h, current, false);
        assert_eq!(
            real_reconciler_loop(
                &h,
                &source,
                PreviousProjection::Present([(previous.task_id, previous)].into()),
                Some(cursor(0)),
                NotifyOptions {
                    follow: true,
                    ..NotifyOptions::default()
                }
            ),
            NotifyExit::Cancelled
        );
        assert_eq!(h.channel.records().len(), 1);
        assert_eq!(h.cache.load().unwrap().decisions.len(), 1);
    }

    #[test]
    fn corrupt_cache_rebuilds_a_real_cold_baseline_with_display_suppressed() {
        use crate::controller::events::PreviousProjection;
        for oversized in [false, true] {
            let h = Fixture::new();
            h.cache.save(&NotifyState::empty()).unwrap();
            if oversized {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(cache_file(h._root.path()))
                    .unwrap()
                    .set_len(crate::controller::events::MAX_NOTIFY_STATE_BYTES as u64 + 1)
                    .unwrap();
            } else {
                std::fs::write(cache_file(h._root.path()), b"broken json").unwrap();
            }
            let source = memory_source(&h, facts(SafeOutcome::NeedsInput), false);
            assert_eq!(
                real_reconciler_loop(
                    &h,
                    &source,
                    PreviousProjection::Absent,
                    None,
                    NotifyOptions::default()
                ),
                NotifyExit::Complete
            );
            assert!(h.channel.records().is_empty());
            let saved = h.cache.load().unwrap();
            assert_eq!(saved.decisions.len(), 1);
            assert!(saved.attention_overflow.is_some());
        }
    }

    #[test]
    fn real_t4_title_opt_out_and_periodic_baseline_pages_never_request_titles() {
        use crate::controller::events::PreviousProjection;
        let h = Fixture::new();
        let mut current = facts(SafeOutcome::NeedsInput);
        current.title = Some("Fixture task".into());
        let source = memory_source(&h, current, false);
        assert_eq!(
            real_reconciler_loop(
                &h,
                &source,
                PreviousProjection::Absent,
                None,
                NotifyOptions {
                    no_titles: true,
                    quiet: true,
                    ..NotifyOptions::default()
                }
            ),
            NotifyExit::Complete
        );
        assert!(!source.tasks.addressed_requests().is_empty());
        assert!(
            source
                .tasks
                .addressed_requests()
                .iter()
                .all(|query| !query.include_titles)
        );
        assert!(
            source
                .tasks
                .repair_requests()
                .iter()
                .all(|query| query.limit <= 64)
        );
        let bytes = std::fs::read(cache_file(h._root.path())).unwrap();
        assert!(!String::from_utf8(bytes).unwrap().contains("Fixture task"));
    }

    struct CancelAfterReconcile<'a> {
        inner: &'a mut FakeEventReconciler,
        clock: Arc<ManualEventRuntime>,
    }
    impl EventReconciler for CancelAfterReconcile<'_> {
        fn reconcile(
            &mut self,
            source: &dyn EventSource,
            input: ReconcileInput,
            deadline: Duration,
        ) -> Result<Reconciliation, WorkerError> {
            let result = self.inner.reconcile(source, input, deadline);
            self.clock.cancel();
            result
        }
    }
    #[test]
    fn discovery_retries_with_bounded_backoff_before_admitting_the_baseline() {
        let mut h = Fixture::new();
        for _ in 0..4 {
            h.source
                .queue_discovery(Err(WorkerError::Unavailable("private target".into())))
                .unwrap();
        }
        h.supported();
        h.reconciler.queue(Ok(complete())).unwrap();
        let mut reconciler = CancelAfterReconcile {
            inner: &mut h.reconciler,
            clock: h.runtime.clone(),
        };
        let mut diagnostics = Vec::new();
        NotifyLoop {
            source: &h.source,
            reconciler: &mut reconciler,
            cache: &h.cache,
            channels: &[],
            options: &NotifyOptions {
                follow: true,
                ..NotifyOptions::default()
            },
            runtime: h.runtime.clone(),
            stop_at: None,
        }
        .run(&mut diagnostics)
        .unwrap();
        assert_eq!(
            h.source.discovery_deadlines(),
            vec![
                Duration::from_secs(30),
                Duration::from_secs(31),
                Duration::from_secs(33),
                Duration::from_secs(37),
                Duration::from_secs(42)
            ]
        );
        assert_eq!(h.reconciler.inputs().len(), 1);
        assert!(
            !String::from_utf8(diagnostics)
                .unwrap()
                .contains("private target")
        );
    }

    #[test]
    fn failed_explicit_channel_reports_a_safe_diagnostic_after_persistence() {
        let mut h = Fixture::new();
        h.supported();
        h.channel.set_error(Some("private channel socket".into()));
        let mut result =
            Reconciliation::test_cold(Some(cursor(5)), vec![facts(SafeOutcome::NeedsInput)]);
        result.attention = Some(AttentionSummary {
            count: 1,
            fingerprint: "a".repeat(64),
        });
        h.reconciler.queue(Ok(result)).unwrap();
        let (exit, diagnostics) = h.run(NotifyOptions::default(), None);
        assert_eq!(exit, NotifyExit::Complete);
        assert!(diagnostics.contains("notification channel failed"));
        assert!(!diagnostics.contains("private channel socket"));
        assert_eq!(h.cache.load().unwrap().decisions.len(), 1);
    }

    #[test]
    fn real_t4_initial_attention_stays_cold_through_addressed_confirmation() {
        let h = Fixture::new();
        let source = memory_source(&h, facts(SafeOutcome::NeedsInput), false);
        assert_eq!(
            real_reconciler_loop(
                &h,
                &source,
                crate::controller::events::PreviousProjection::Absent,
                None,
                NotifyOptions::default()
            ),
            NotifyExit::Complete
        );
        let notices = h.channel.records();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].0.title, "Tasks need attention");
        assert_eq!(notices[0].0.body, "1 task");
        assert_eq!(h.cache.load().unwrap().decisions.len(), 1);
    }

    #[test]
    fn same_epoch_repair_coalesces_fresh_confirmed_decisions_without_jumping_to_head() {
        let mut h = Fixture::new();
        h.supported();
        h.reconciler.queue(Ok(complete())).unwrap();
        let mut result = fresh();
        result.consumed_after = Some(cursor(5));
        result.repair = RepairProgress::Restarted;
        result.repair_needed = true;
        result.changes[0].cause = ChangeCause::RepairDifference;
        h.reconciler.queue(Ok(result)).unwrap();
        h.source
            .queue_read(Ok(EventReadResult::SnapshotRequired(
                crate::controller::events::SnapshotRequired {
                    reason: "cursor_expired".into(),
                    window: crate::controller::events::JournalWindow {
                        journal_id: cursor(0).journal_id,
                        oldest_seq: Seq::new(10),
                        head_seq: Seq::new(20),
                    },
                },
            )))
            .unwrap();
        let cancel = h.runtime.clone();
        h.runtime.on_sleep(move |_| cancel.cancel());
        h.run(
            NotifyOptions {
                follow: true,
                ..NotifyOptions::default()
            },
            None,
        );
        let notices = h.channel.records();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].0.title, "Tasks finished");
        assert_eq!(notices[0].0.body, "1 task");
        assert_eq!(h.cache.load().unwrap().consumed_after, Some(cursor(5)));
    }
}
