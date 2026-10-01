//! Explicit integration-test access for events contracts.

pub use crate::controller::events::{
    ADDRESSED_MAX_TASKS, AcceptedHint, AttentionSummary, BaselineKind, CONTROLLER_EVENTS_CANCELLED,
    CONTROLLER_EVENTS_INVALID, CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE,
    CONTROLLER_EVENTS_UNAVAILABLE, CONTROLLER_EVENTS_UNSUPPORTED, ChangeCause, DerivedTaskChange,
    EventBatch, EventCursor, EventReadResult, EventReconciler, EventRuntime, EventSelector,
    EventSink, EventSource, EventSupport, JOURNAL_ADMISSION_BUDGET, JOURNAL_CHECK_INTERVAL,
    JournalProvider, JournalReader, JournalWindow, JournalWriter, LocalProjectionRefresh,
    MAX_BATCH_BYTES, MAX_BATCH_EVENTS, MAX_DISPATCH_ASSOCIATIONS, MAX_DISPLAY_TITLE_BYTES,
    MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_JOURNAL_BYTES, MAX_JOURNAL_FILES, MAX_METADATA_BYTES,
    MAX_NOTIFY_STATE_BYTES, MAX_OPAQUE_CURSOR_BYTES, MAX_PUBLISHER_BYTES, MAX_RECONCILIATION_ROWS,
    MAX_RECOVERY_EVIDENCE_BYTES, MAX_RECOVERY_EVIDENCE_FILES, MAX_RETAINED_BYTES,
    MAX_RETAINED_SEGMENTS, MAX_SEGMENT_BYTES, MAX_STALE_RETRIES, MAX_STATE_RECORD_BYTES,
    MAX_TASK_FACT_BYTES, NOTICE_CHANNEL_BUDGET, NOTIFY_COALESCE_AFTER, NOTIFY_COALESCE_COUNT,
    NOTIFY_DECISION_CAPACITY, NOTIFY_PENDING_CAPACITY, NewEvent, Notice, NoticeChannel,
    NoticeSound, NotifyChannel, NotifyOptions, NotifyPlan, NotifyState, OpaqueCursor,
    PUBLISHER_CAPACITY, PUBLISHER_EXIT_GRACE, PendingCandidate, PreviousProjection, PublishAttempt,
    QueueHint, READ_DEFAULT_LIMIT, READ_FOLLOW_WAIT_MS, READ_MAX_LIMIT, READ_MAX_WAIT_MS,
    RECONNECT_BACKOFF, REFRESH_DEBOUNCE, REPAIR_DEFAULT_LIMIT, REPAIR_INTERVAL,
    REPAIR_MAX_DIRECTORY_ENTRIES, REPAIR_MAX_INPUT_BYTES, REPAIR_MAX_LIMIT, REPAIR_WORK_BUDGET,
    RPC_BUDGET, ReadBatch, ReadQuery, ReconcileInput, Reconciliation, RepairProgress,
    SCHEMA_VERSION, SSE_CAPACITY, SSE_HEARTBEAT_INTERVAL, SSE_MAX_STREAMS, SafeCode, SafeOutcome,
    Seq, SnapshotRequired, TUNNEL_TIMEOUT, TaskAddressQuery, TaskEligibilitySignature, TaskFacts,
    TaskFactsBatch, TaskFactsWire, TaskHint, TaskProjectionProvider, TaskProjectionReader,
    TaskRepairPage, TaskRepairQuery, TurnHint, ViewerEventSource, ViewerMessage, WireEvent,
    WorkerName, ensure_frame_bound,
};
pub mod client {
    pub use crate::controller::events::client::{ControllerEventClient, TaskReconciler};
}
pub mod contracts {
    pub use crate::controller::events::contracts::{
        AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventCursor,
        EventReconciler, EventSupport, MAX_EVENT_BYTES, MAX_NOTIFY_STATE_BYTES,
        MAX_RECONCILIATION_ROWS, NOTIFY_DECISION_CAPACITY, Notice, NoticeChannel, NoticeSound,
        NotifyChannel, NotifyOptions, NotifyState, ReconcileInput, Reconciliation, RepairProgress,
        SafeCode, SafeOutcome, Seq, TaskFacts,
    };
}
pub mod journal {
    pub use crate::controller::events::journal::{
        BoundedPublisher, ControllerJournal, ExistingJournalProvider, JournalFaultHook,
        JournalFaultPoint, JournalOptions, JournalRole, JournalRoleBoundary, PublisherHandle,
    };
}
pub mod notify {
    pub use crate::controller::events::notify::{
        ChannelOptions, HerdrChannel, MacosChannel, NotifyCache, NotifyOptions, NotifyPlan,
        NotifyState, OSASCRIPT_HANDLER, SelectedChannel, UnconfirmedTask, channels_for,
        commit_then_deliver, eligibility_unknown_diagnostic, herdr_socket_reachable, herdr_sound,
        laptop_notification_socket, notices_for_support, plan_notifications, select_channels,
    };
    pub mod follow {
        pub use crate::controller::events::notify::follow::NotifyLoop;
    }
}
pub mod rpc {
    pub use crate::controller::events::rpc::{
        ExistingTaskProjectionProvider, TaskEventReadStore, is_event_selector, serve_selector_with,
        task_reads,
    };
}
pub mod testing {
    pub use crate::controller::events::testing::{
        FakeEventReconciler, FakeJournalProvider, FakeLocalProjectionRefresh,
        FakeTaskProjectionProvider, ManualEventRuntime, MemoryJournal, MemoryTaskReader,
        MemoryViewerEventSource, RecordingNoticeChannel, RecordingSink, ScriptedEventSource,
    };
}
