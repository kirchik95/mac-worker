//! Explicit integration-test access for core contracts.

pub mod build_id {
    pub use crate::build_id::BUILD_ID;
}
pub mod config {
    pub use crate::config::{
        Config, ControllerConfig, NotificationsConfig, SshConfig, WorkerEntry,
    };
}
pub mod error {
    pub use crate::error::{ExitKind, ProcessError, ProcessStream, WorkerError, hint_for};
}
pub mod failure_receipt {
    pub use crate::failure_receipt::{
        FailureReceipt, RESIDUAL_CLEANUP_TREE, RESIDUAL_LEASE, RESIDUAL_WORKSPACE, STAGE_CLEANUP,
        STAGE_DRAIN, STAGE_FOLLOW, STAGE_PUBLISH,
    };
}
pub mod inputs {
    pub use crate::inputs::{
        InputOrigin, InputSelection, InputSelector, RelativePath, SelectedInput, SelectedInputKind,
        SelectionFailure,
    };
}
pub mod manifest {
    pub use crate::manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest};
}
pub mod output {
    pub use crate::output::CommandOutput;
}
pub mod paths {
    pub use crate::paths::PathLayout;
}
pub mod protocol {
    pub use crate::protocol::{
        ControllerConfigureRequest, CpuCounters, DoctorIssue, DoctorProject, DoctorReport,
        HERDR_FACTS_STALE_MESSAGE, HERDR_UNAVAILABLE_MESSAGE, HealthStatus, IssueSeverity,
        MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION, SetupFailureKind,
        SetupHostResult, SetupReport, SetupWarning, SetupWarningCode, WorkerHealth, WorkersReport,
    };
}
pub mod redaction {
    pub use crate::redaction::{
        MAX_CHANGED_FILE_BYTES, MAX_CHANGED_FILE_COUNT, MAX_FAILURE_REASON_BYTES,
        MAX_QUESTION_BYTES, MAX_QUESTION_COUNT, MAX_QUESTION_OPTION_BYTES,
        MAX_QUESTION_OPTION_COUNT, MAX_SUMMARY_BYTES, MAX_TITLE_BYTES, RedactionBoundary,
    };
}
pub mod requirements {
    pub use crate::requirements::RequirementDetector;
}
