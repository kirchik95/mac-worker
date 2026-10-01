//! Explicit integration-test access for client state contracts.

pub use crate::client_state::{
    ActiveTaskConfig, ActiveTaskSelection, ClientStateConcurrencyHook, ClientStateConcurrencyPoint,
    ClientStateCreationRacePoint, ClientStateStore, ClientStateTimings, ClientStateWritePoint,
    RUNNER_ABSENCE_CONFIRMATION, RUNNER_UNVERIFIABLE_AFTER, ReservedSlotTakeover,
    RunnerSlotDecision, task_record_needs_active_index,
};
pub mod dag {
    pub use crate::dag::{
        DAG_PARENT_FAILED, DagBase, DagFrozenSpec, DagNode, DagNodeProjection, DagNodeState,
        DagRecord, ParentGate, dag_pin_ref, parent_gate,
    };
}
pub mod events {
    pub use crate::client_state::events::{DeferredHints, dropped_hint_count};
}
pub mod scheduler {
    pub use crate::scheduler::{
        AffinityHints, CandidateObservation, CandidateObservationError, CandidateRejection,
        CandidateSlot, QueueBlockingReason, RankedCandidate, SchedulerPolicy, Selection,
        WorkerPreference,
    };
}
pub mod scheduler_adapter {
    pub use crate::scheduler_adapter::SchedulerProbeAdapter;
}
