use mac_worker::controller::events::{JournalWindow, Seq, SnapshotRequired, ViewerMessage};

#[test]
fn sse_controls_never_advance_the_journal_cursor() {
    let window = JournalWindow {
        journal_id: uuid::Uuid::from_u128(1),
        oldest_seq: Seq::new(1),
        head_seq: Seq::new(42),
    };
    for (message, name) in [
        (ViewerMessage::Ready(window.clone()), "ready"),
        (
            ViewerMessage::SnapshotRequired(SnapshotRequired {
                reason: "bootstrap".into(),
                window,
            }),
            "snapshot_required",
        ),
        (
            ViewerMessage::SnapshotReady { revision: 42 },
            "snapshot.ready",
        ),
        (ViewerMessage::Heartbeat, "heartbeat"),
        (
            ViewerMessage::Unavailable {
                code: "CONTROLLER_EVENTS_UNAVAILABLE".into(),
            },
            "snapshot_required",
        ),
    ] {
        assert_eq!(message.event_name(), name);
        assert!(message.cursor().is_none());
        let value = serde_json::to_value(message).unwrap();
        assert_eq!(value["event"], name);
        assert!(value.get("id").is_none());
    }
}
