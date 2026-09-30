//! Laptop event/reconciliation facade. Transport and reconciliation are T4.
pub use super::contracts::{
    AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventReconciler, EventSource,
    EventSupport, PreviousProjection, ReconcileInput, Reconciliation, RepairProgress,
    TaskEligibilitySignature,
};
