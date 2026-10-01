//! Explicit integration-test access for host contracts.

pub mod binary_identity {
    pub use crate::binary_identity::{current_binary_sha256, sha256_hex};
}
pub mod gc {
    pub use crate::gc::HostGc;
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
