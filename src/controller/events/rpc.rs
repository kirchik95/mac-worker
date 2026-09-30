//! Safe selector facade. Serving RPC and state-only readers are implemented by T4.
pub use super::contracts::{
    EventReadResult, EventSelector, JournalProvider, OpaqueCursor, ReadQuery, TaskAddressQuery,
    TaskFacts, TaskFactsBatch, TaskFactsWire, TaskProjectionProvider, TaskProjectionReader,
    TaskRepairPage, TaskRepairQuery, ensure_frame_bound,
};
