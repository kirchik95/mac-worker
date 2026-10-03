//! Explicit integration-test access for host contracts.

pub mod binary_identity {
    pub use crate::binary_identity::{current_binary_sha256, sha256_hex};
}
pub mod gc {
    pub use crate::gc::HostGc;

    /// Baseline ce7f62f selection (seven days from host status, idle Open,
    /// no live lease/job/outbox), followed by the unchanged non-discard close.
    /// Integration sidecars deliberately have no part in this legacy fixture.
    pub fn apply_baseline_retention_close(
        store: &crate::host_store::HostStore,
        runner: &dyn crate::process::ProcessRunner,
        project: &str,
        task: crate::task::TaskId,
        now: u64,
    ) -> Result<bool, crate::error::WorkerError> {
        use crate::{job::JobStatus, task::TaskState};
        let _installation = store.capacity_lock()?;
        let status = store.task_status(project, task)?;
        if status.state() != TaskState::Open
            || now.saturating_sub(status.updated_at_millis()) < crate::gc::TASK_RETENTION_MILLIS
            || crate::lease::LeaseService::new(store).task_scope_is_live(project, task)?
        {
            return Ok(false);
        }
        let meta = crate::task_store::TaskStore::new(store, runner).load_meta(project, task)?;
        for turn in status.turns() {
            if turn.terminal().is_none() {
                return Ok(false);
            }
            let job = match store.open_directory(
                &format!("jobs/{project}/{}/{}", meta.worktree_id(), turn.turn_id()),
                false,
            ) {
                Ok(job) => job,
                Err(crate::error::WorkerError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            let bytes = job
                .read_private_regular("status.json", 256 * 1024)
                .map_err(crate::error::WorkerError::Io)?;
            let job: JobStatus = serde_json::from_slice(&bytes).map_err(|_| {
                crate::error::WorkerError::Protocol(
                    "TASK_STATE_INVALID: legacy job status invalid".into(),
                )
            })?;
            if !job.state().is_terminal() {
                return Ok(false);
            }
        }
        if crate::outbox::OriginOutbox::new(store, runner).retains(project, task, now)? {
            return Ok(false);
        }
        Ok(crate::task_store::TaskStore::new(store, runner)
            .close_for_retention(project, task, now)?
            .is_some())
    }
}
pub mod job {
    pub use crate::job::{
        AdmissionObservation, CancelRequest, CancelResponse, ClientId, CommandSpec, CommandSummary,
        ExecutionScope, HostControlError, JobId, JobMeta, JobState, JobStatus, JsonEvent,
        LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord, LeaseToken, LocalJobRecord,
        LogChunk, LogChunkRequest, LogChunkResponse, LogCursor, LogStream, MAX_LOG_CHUNK_BYTES,
        PreacceptanceDisposition, ProcessIdentity, QueueCancel, QueueClaim, QueueEntry,
        QueueEntryKind, QueueId, QueueRunReference, QueueSnapshot, QueueState,
        REPLACEMENT_FAILURE_PARK_AFTER, RUNNER_REPEATED_FAILURE, RUNNER_UNVERIFIABLE,
        RemoteUncertainty, ReplacementFailureBudget, RequestFingerprint,
        RequestFingerprintMaterial, ResolveOrAbandonOutcome, ResolveOrAbandonRequest,
        ResolveOrAbandonResponse, RunId, StatusLogsRequest, StatusLogsResponse, StatusRequest,
        StatusResponse, SubmitRequest, SubmitResponse, TerminalLogDrain,
    };
}
pub mod job_service {
    pub use crate::job_service::{JobService, LaunchCandidate, SupervisorLauncher};
}
pub mod lease {
    pub use crate::lease::{AdmissionFacts, LeaseService, LeaseSummary, MAX_HOST_SLOTS, SlotState};
}
pub mod legacy_snapshot_receipt {
    pub use crate::legacy_snapshot_receipt::VerifiedReceipt;
}
pub mod process {
    pub use crate::process::{
        CleanupState, ProcessCompletion, ProcessPolicy, ProcessRequest, ProcessResult,
        ProcessRunner, SystemProcessRunner, TrackedProcessRunner,
    };
}
pub mod rooted_fs {
    pub use crate::rooted_fs::{EntryKind, RootedDir};
}
pub mod store {
    pub use crate::gc::{
        BRANCH_RETENTION_MILLIS, HostGc, JOB_RETENTION_MILLIS, TASK_RETENTION_MILLIS,
    };
    pub use crate::host_store::{
        AdmissionGuard, CleanupReceipt, HOST_LAYOUT_VERSION, HostStore, HostStoreWritePoint,
        JobDisposition, PREVIOUS_HOST_LAYOUT_VERSION, SupervisorGuard, TransferGuard,
    };
}
pub mod supervisor {
    pub use crate::supervisor::{
        LaunchPlan, ProcessGroupMembership, ProcessGroupObservation, ProcessInspector,
        ProcessObservation, ReconciliationRuntime, SUPERVISOR_TERM_GRACE, StdinSource, StdoutSink,
        Supervisor, SupervisorFaultPoint, SystemProcessInspector, SystemSupervisorLauncher,
    };
}
