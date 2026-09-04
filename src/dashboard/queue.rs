use std::sync::Arc;

use crate::{
    client_state::ClientStateStore,
    config::Config,
    dashboard::{
        model::{
            DashboardCommandMode, DashboardCommandSummary, DashboardError, DashboardJob,
            DashboardQueueEntry,
        },
        service::DashboardQueueReader,
    },
    error::WorkerError,
    job::{CommandSummary, QueueState},
    scheduler::QueueBlockingReason,
};

/// A queue row already projected by the phase-4 scheduler boundary.
///
/// The producer owns queue-state interpretation, including FIFO eligibility and
/// the blocking reason. Keeping that work on the producer side prevents the
/// dashboard from making a second scheduling decision from incomplete data.
#[derive(Debug, Clone, PartialEq)]
pub struct PhaseFourQueueEntry {
    pub position: u32,
    pub job: DashboardJob,
    pub requirements: Vec<String>,
    pub blocking_code: String,
}

pub trait PhaseFourQueueReader: Send + Sync + 'static {
    fn ordered_pending(&self) -> Result<Vec<PhaseFourQueueEntry>, DashboardError>;
}

/// Read-only dashboard projection over the Phase-4 client-state queue.
///
/// `ClientStateStore::queue_rows_with_blocking_reasons` reads the durable FIFO
/// rows and last cached admission observations without running a refresh or an
/// admission path. This reader only converts that advisory view to dashboard
/// DTOs.
pub(crate) struct ClientStateDashboardQueueReader {
    config: Arc<Config>,
    state: Arc<ClientStateStore>,
}

impl ClientStateDashboardQueueReader {
    pub(crate) fn new(config: Arc<Config>, state: Arc<ClientStateStore>) -> Self {
        Self { config, state }
    }
}

impl DashboardQueueReader for ClientStateDashboardQueueReader {
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        self.state
            .queue_rows_with_blocking_reasons(&self.config)
            .map_err(map_local_queue_error)?
            .into_iter()
            .filter(|row| matches!(row.entry().state(), QueueState::Waiting { .. }))
            .enumerate()
            .map(|(index, row)| {
                let position = u32::try_from(index + 1).map_err(|_| {
                    DashboardError::new(
                        "QUEUE_POSITION_INVALID",
                        "local dashboard queue position exceeds the supported range",
                    )
                })?;
                let entry = row.entry();
                Ok(DashboardQueueEntry {
                    position,
                    job_id: entry.job_id(),
                    project_id: entry.project_id().to_owned(),
                    worktree_id: entry.worktree_id().to_owned(),
                    project_label: None,
                    command_summary: project_command_summary(entry.command_summary()),
                    created_at_millis: entry.enqueued_at_millis(),
                    requirements: entry.requirements().to_vec(),
                    blocking_code: dashboard_blocking_code(row.blocking_reason()).into(),
                })
            })
            .collect()
    }
}

/// Projects phase-4 queue rows into the stable dashboard queue DTO.
///
/// This adapter is deliberately read-only: it does not inspect jobs or worker
/// probes, reorder rows, choose a worker, or alter queue state.
pub struct SchedulerQueueAdapter<R> {
    reader: R,
}

impl<R: PhaseFourQueueReader + Send + Sync + 'static> SchedulerQueueAdapter<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    pub fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        self.reader
            .ordered_pending()?
            .into_iter()
            .map(project_queue_entry)
            .collect()
    }

    pub fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        self.queue_entries()
    }
}

impl<R: PhaseFourQueueReader + Send + Sync + 'static> DashboardQueueReader
    for SchedulerQueueAdapter<R>
{
    fn ordered_pending(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        self.queue_entries()
    }
}

fn project_queue_entry(entry: PhaseFourQueueEntry) -> Result<DashboardQueueEntry, DashboardError> {
    let job = entry.job;
    Ok(DashboardQueueEntry {
        position: entry.position,
        job_id: job.job_id,
        project_id: job.project_id,
        worktree_id: job.worktree_id,
        project_label: job.project_label,
        command_summary: job.command_summary,
        created_at_millis: job.created_at_millis,
        requirements: entry.requirements,
        blocking_code: entry.blocking_code,
    })
}

fn project_command_summary(summary: &CommandSummary) -> DashboardCommandSummary {
    match summary.arg_count() {
        Some(arg_count) => DashboardCommandSummary {
            mode: DashboardCommandMode::Argv,
            arg_count: Some(u16::try_from(arg_count).unwrap_or(u16::MAX)),
        },
        None => DashboardCommandSummary {
            mode: DashboardCommandMode::Shell,
            arg_count: None,
        },
    }
}

fn dashboard_blocking_code(reason: Option<&QueueBlockingReason>) -> &'static str {
    match reason {
        Some(QueueBlockingReason::PinnedWorkerBusy { .. }) => "PINNED_WORKER_BUSY",
        Some(QueueBlockingReason::CapabilityMissing { .. }) => "CAPABILITY_MISSING",
        Some(QueueBlockingReason::RunCap) => "RUN_MAX_PARALLEL",
        Some(QueueBlockingReason::NoEligibleWorker) => "NO_COMPATIBLE_IDLE_WORKER",
        None => "WAITING_FOR_DISPATCH",
    }
}

fn map_local_queue_error(error: WorkerError) -> DashboardError {
    DashboardError::new(error.public_code(), "local dashboard queue is unavailable")
}
