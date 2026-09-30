//! Journal component facade. Production journal/publisher implementations are T2.
pub use super::contracts::{
    EventBatch, EventCursor, EventReadResult, EventRuntime, EventSink, JournalProvider,
    JournalReader, JournalWindow, JournalWriter, PublishAttempt, ReadBatch, ReadQuery,
    SnapshotRequired,
};
