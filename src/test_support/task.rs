//! Explicit integration-test access for task contracts.

pub mod client {
    pub use crate::task_client::{
        BatchFile, ReconcileReport, RunReport, TaskClient, TaskListFilter, TaskSubmitRequest,
        WaitReport, WaitSelector, preview_batch_plan,
    };
}
pub mod follow_turn {
    pub use crate::follow_turn::{follow_turn, follow_turn_with_poll_interval};
}
pub mod model {
    pub use crate::task::{
        BaseOid, BranchName, ClosePolicy, DeliveryState, GitIdentity, HerdrTurnReport,
        HerdrTurnState, LocalTaskRecord, MAX_FOLLOWUPS, MAX_PROMPT_BYTES, OriginDelivery,
        PublishMode, PushTarget, QuestionsPolicy, RunId, RunProgress, RunRecord, RunnerIdentity,
        RunnerState, TaskCloseIntent, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskOutcome,
        TaskSource, TaskState, TaskStatus, TaskSummary, TurnId, TurnSummary, TurnTerminal,
    };
}
pub mod prepare_turn {
    pub use crate::prepare_turn::ARG;
}
pub mod prepared_followup {
    pub use crate::prepared_followup::PreparedFollowup;
}
pub mod prepared_submit {
    pub use crate::prepared_submit::FrozenSubmitBody;
}
pub mod project {
    pub use crate::project::{ProjectContext, ProjectInspector};
}
pub mod project_config {
    pub use crate::project_config::{
        ArtifactSettings, ProjectSettings, ResourceClass, SnapshotSettings, TaskSettings,
    };
}
pub mod project_readiness {
    pub use crate::project_readiness::{FrozenSetup, SetupStageResult};
}
pub mod project_state {
    pub use crate::project_state::{
        ProjectPreparationError, ProjectPreparationRequest, ProjectState,
    };
}
pub mod store {
    pub use crate::task_store::{
        MAX_DIFF_BYTES, SessionBinding, TaskCancelRequest, TaskCancelResponse, TaskCloseRequest,
        TaskCloseResponse, TaskDiffRequest, TaskDiffResponse, TaskPrebindRequest,
        TaskPrepareRequest, TaskPrepareResponse, TaskSessionRequest, TaskSessionResponse,
        TaskStatusRequest, TaskStatusResponse, TaskStore,
    };
}
pub mod turn {
    pub use crate::turn::{
        EnvProfile, PreparedTask, TaskTurnRequest, TaskTurnResponse, TurnMaterial, TurnPublisher,
        TurnReceipt, TurnResult, TurnSection, prebind_session,
    };
}
pub mod turn_log {
    pub use crate::turn_log::render_agent_log;
}
pub mod turn_runner {
    pub use crate::turn_runner::{
        DetachedRunnerExecutor, InlineRunnerExecutor, RunnerExecutor, RunnerStart,
        TurnOutcomeReport, TurnRunner, start_runner_with_reservation,
    };
}
pub mod view {
    pub use crate::task_view::{
        ReviewState, TaskDetailProjection, TaskFreshness, TaskListJson, TaskListProjection,
        TaskListRow, TaskRunProjection, TaskTimelineEvent, TaskTurnProjection, filter_task_list,
        project_task_detail, project_task_list, project_task_list_with_blocking_codes,
        remote_status_refresh_allowed, review_state,
    };
}
