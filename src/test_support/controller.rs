//! Explicit integration-test access for controller contracts.

pub use super::{channel, events};

pub use crate::controller::{
    ActiveResumeConfig, BatchExecuteContext, BatchKind, CONTROLLER_TRANSFER_CACHE_DOMAIN,
    ControllerCheckoutMap, ControllerCommandHandler, ControllerFault, ControllerLeader,
    ControllerReadIdentity, ControllerReadReply, ControllerRequest, ControllerResultIdentity,
    ControllerStore, ControllerTaskLogsResult, ControllerTaskStatusResult, ControllerTransfer,
    ControllerWaitSelector, DurableRequest, FakeControllerExecutor, FrozenBatchBody,
    FrozenBatchSource, LaptopFrozenBatch, MAX_FRAME_BYTES, OperationEnvelope, OperationMeta,
    OperationOutcome, OwnedCheckoutMap, PreparedTaskBatch, PreparedTaskMutation, ProjectRegistry,
    RequestPhase, TaskSubmitHandler, VerifiedResultMeta, canonical_request_sha256,
    controller_dashboard_ssh_request, controller_rpc_ssh_request, controller_transfer_cache_id,
    controller_transfer_git_path, decode_frame, decode_request, default_prepare_operation,
    encode_frame, encode_json_frame, execute_task_batch, execute_task_mutation,
    freeze_laptop_batch, frozen_result_ref, import_controller_result, is_read_command,
    load_operation_envelope, parse_request, persist_operation_envelope, prepare_task_batch,
    prepare_task_mutation, result_digest, serve_rpc, serve_rpc_with_runtime, source_digest,
    tick_controller_leader, wait_via_controller,
};
pub mod batch {
    pub use crate::controller::batch::{BatchKind, FrozenBatchBody};
}
pub mod control {
    pub use crate::controller::control::drain_via_controller;
}
pub mod drain {
    pub use crate::controller::drain::{is_drained, set_drained, set_drained_with_event_sink};
}
pub mod health {
    pub use crate::controller::health::{
        ControllerHealth, ControllerTickReport, HealthLogger, HealthStore,
    };
}
pub mod health_read {
    pub use crate::controller::health_read::{
        ControllerHealthStatus, HealthReason, HealthState, assess_health, fetch_controller_health,
        is_health_read, serve_health_read,
    };
}
pub mod init {
    pub use crate::controller::init::{
        ConfiguredHost, InitReport, InitRequest, disable, initialize, initialize_with_wait,
    };
}
pub mod protocol {
    pub use crate::controller::protocol::{
        ControllerRequest, MAX_FRAME_BYTES, MAX_STORED_REQUEST_BYTES, canonical_request_sha256,
        decode_frame, decode_request, encode_frame, encode_json_frame, parse_request, read_frame,
    };
}
pub mod provision {
    pub use crate::controller::provision::{
        PlannedWorker, ResolvedSsh, authorize_controller_key, ensure_controller_key,
        plan_inventory, trusted_host_keys, worker_ssh_alias, write_controller_config,
        write_ssh_settings,
    };
}
pub mod read {
    pub use crate::controller::read::{
        ControllerReadReply, ControllerTaskLogsResult, logs_reply_with_runtime, serve_read_command,
    };
}
pub mod registry {
    pub use crate::controller::registry::ProjectRegistry;
}
pub mod runtime {
    pub use crate::controller::runtime::{
        ControllerProcessRunner, LeaderChannel, LeaderChannelConfig, LeaderChannelDeps,
        LeaderChannelShutdown, SystemChannelRuntime, run_tick_loop, run_tick_loop_with_shutdown,
    };
}
pub mod service {
    pub use crate::controller::service::{
        ServiceAction, ServicePaths, ServiceStatus, launchdaemon_commands, manage,
        restart_and_verify, truncate_log,
    };
}
