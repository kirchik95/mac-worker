//! Explicit integration-test access for dashboard contracts.

pub use crate::dashboard::run_controller_dashboard_tunnel_with_readiness_timeout;
pub mod cache {
    pub use crate::dashboard::cache::{
        CpuBusyPercent, CpuCounters, IDLE_PROBE_INTERVAL_MILLIS, MAX_SAMPLES_PER_WORKER,
        OBSERVATION_TTL_MILLIS, Observation, ObservationCache,
    };
}
pub mod command {
    pub use crate::dashboard::command::{
        BrowserOpener, DashboardCommandRequest, DashboardLauncher, run_dashboard,
    };
}
pub mod events {
    pub use crate::dashboard::events::LocalViewerEventSource;
}
pub mod model {
    pub use crate::dashboard::model::{
        AgentFactsFreshness, ApiError, CollectionSummary, DASHBOARD_API_VERSION,
        DashboardCommandMode, DashboardCommandSummary, DashboardError, DashboardLaptop,
        DashboardLogChunk, DashboardMemoryPressure, DashboardQueueEntry, DashboardQueueEntryKind,
        DashboardSlotState, DashboardSnapshot, DashboardWorker, Freshness, SlotSummary,
        SystemSummary, WorkerHealth,
    };
}
pub mod queue {
    pub use crate::dashboard::queue::{
        PhaseFourQueueEntry, PhaseFourQueueReader, SchedulerQueueAdapter,
    };
}
pub mod service {
    pub use crate::dashboard::service::{
        Clock, DashboardDataSource, DashboardDeadlines, DashboardQueueReader, DashboardService,
        DashboardSnapshotRequest, DashboardTaskCollection, EmptyDashboardQueueReader,
        GLOBAL_COLLECTION_DEADLINE, MAX_COLLECTION_ERRORS, MonotonicClock, SNAPSHOT_PENDING,
        SystemClock, SystemMonotonicClock, WORKER_COLLECTION_DEADLINE, WorkerObservationResult,
    };
}
pub mod settings {
    pub use crate::dashboard::settings::{DashboardSettingsSource, SystemDashboardSettingsSource};
}
pub mod source {
    pub use crate::dashboard::source::{
        DashboardRemoteReader, DashboardWorkerReader, MacWorkerDashboardSource, cache_counters,
        project_worker,
    };
}
pub mod task {
    pub use crate::dashboard::task::{
        DashboardTaskMutationSource, DashboardTaskSource, MAX_TASK_LOG_LIMIT,
        MacWorkerTaskMutationSource, MacWorkerTaskSource, TaskMutationRequest,
    };
}
pub mod web {
    pub use crate::dashboard::web::{DashboardHttpServer, DashboardHttpState};
}
