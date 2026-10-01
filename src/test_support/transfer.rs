//! Explicit integration-test access for transfer contracts.

pub use crate::transfer::{
    AbandonTransferResult, HostOperation, HostTransferService, OptionalHostResponse,
    PreacceptanceAbandonmentReceipt, PreacceptanceResolution, RemoteJobClient, ResolutionRuntime,
    SshJsonTransport, TransferIdentity, controller_host_request,
};
pub mod git {
    pub use crate::git_transport::{
        FetchReceipt, GitServerExecutor, GitTransport, HostGitService, OBJECT_STORE_SYNC_RECEIPT,
        ORIGIN_AUTH_FAILED, PushReceipt, ReceivePackComponents, UploadPackComponents,
    };
}
pub mod outbox {
    pub use crate::outbox::{
        DELIVERY_ALREADY_DELIVERED, DELIVERY_NOT_FOUND, DELIVERY_UNREADABLE, DeliveryCommit,
        OUTBOX_BUSY, OUTBOX_WORKER_REQUIRED, OriginOutbox, OutboxActivation, OutboxLauncher,
        OutboxRetryResponse, due_index_reads_for, task_directory_scans_for,
    };
}
pub mod repo {
    pub use crate::transfer_repo::{
        BaseCommit, BaseKind, CapturedTree, DirtyReport, ImportReceipt, RepositoryFingerprint,
        ResultImport, TransferGc, TransferRepo, repo_id_for,
    };
}
pub mod snapshot {
    pub use crate::snapshot::{Snapshot, SnapshotBuilder, SnapshotHook, SnapshotSummary};
}
pub mod transport {
    pub use crate::transport::{ProbeClock, SshTransport, WorkersService};
}
