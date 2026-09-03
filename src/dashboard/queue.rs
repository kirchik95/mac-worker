use crate::dashboard::{
    model::{DashboardError, DashboardJob, DashboardQueueEntry},
    service::DashboardQueueReader,
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
