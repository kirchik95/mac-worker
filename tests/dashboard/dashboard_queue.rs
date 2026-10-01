use mac_worker::{
    dashboard::{
        model::{DashboardCommandMode, DashboardCommandSummary, DashboardQueueEntryKind},
        queue::{PhaseFourQueueEntry, PhaseFourQueueReader, SchedulerQueueAdapter},
    },
    job::JobId,
};

const SSH_SECRET: &str = "operator@mini-1.internal";
const PATH_SECRET: &str = "/Users/alice/private-worktree";
const COMMAND_SECRET: &str = "secret-command-value";

#[test]
fn queue_adapter_preserves_scheduler_fifo_order_and_reasons() {
    let adapter = SchedulerQueueAdapter::new(FakeQueue {
        rows: vec![
            phase_four_entry(1, 1, "PINNED_WORKER_BUSY"),
            phase_four_entry(2, 2, "CAPABILITY_MISSING"),
            phase_four_entry(3, 3, "RUN_MAX_PARALLEL"),
        ],
    });

    let queue = adapter.queue_entries().unwrap();

    assert_eq!(
        queue.iter().map(|entry| entry.position).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        queue.iter().map(|entry| entry.job_id).collect::<Vec<_>>(),
        vec![job_id(1), job_id(2), job_id(3)]
    );
    assert_eq!(queue[0].blocking_code, "PINNED_WORKER_BUSY");
    assert_eq!(queue[1].blocking_code, "CAPABILITY_MISSING");
    assert_eq!(queue[2].blocking_code, "RUN_MAX_PARALLEL");
}

#[test]
fn queue_adapter_projects_only_the_safe_queue_fields() {
    let queue = SchedulerQueueAdapter::new(FakeQueue {
        rows: vec![phase_four_entry(1, 7, "NO_COMPATIBLE_IDLE_WORKER")],
    })
    .queue_entries()
    .unwrap();
    let wire = serde_json::to_string(&queue).unwrap();

    assert_eq!(queue[0].job_id, job_id(7));
    assert!(!wire.contains(SSH_SECRET));
    assert!(!wire.contains(PATH_SECRET));
    assert!(!wire.contains(COMMAND_SECRET));
    assert!(!wire.contains("worker_name"));
    assert!(!wire.contains("manifest_digest"));
    assert!(!wire.contains("remote_uncertainty"));
}

struct FakeQueue {
    rows: Vec<PhaseFourQueueEntry>,
}

impl PhaseFourQueueReader for FakeQueue {
    fn ordered_pending(
        &self,
    ) -> Result<Vec<PhaseFourQueueEntry>, mac_worker::dashboard::model::DashboardError> {
        Ok(self.rows.clone())
    }
}

fn phase_four_entry(position: u32, id: u128, blocking_code: &str) -> PhaseFourQueueEntry {
    PhaseFourQueueEntry {
        position,
        job_id: job_id(id),
        project_id: format!("project-{id}"),
        worktree_id: format!("worktree-{id}"),
        project_label: Some(format!("Project {id}")),
        command_summary: DashboardCommandSummary {
            mode: DashboardCommandMode::Argv,
            arg_count: Some(2),
        },
        created_at_millis: 1_000 + id as u64,
        entry_kind: DashboardQueueEntryKind::Batch,
        task_id: None,
        turn_id: None,
        run_id: None,
        run_max_parallel: None,
        pinned_worker: None,
        requirements: vec!["swift".into()],
        blocking_code: blocking_code.into(),
    }
}

fn job_id(value: u128) -> JobId {
    JobId::new(uuid::Uuid::from_u128(value))
}
