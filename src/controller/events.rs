//! Shared contracts for optional, lossy controller lifecycle hints.
//!
//! Saved task state remains authoritative. This module advertises no feature
//! and opens no state or journal. Component facades are filled by later tracks.

pub mod contracts;
pub use contracts::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{TaskId, TaskOutcome, TurnId};
    use std::collections::BTreeMap;

    fn task_id() -> TaskId {
        TaskId::new(uuid::Uuid::from_u128(2))
    }
    fn turn_id() -> TurnId {
        TurnId::new(uuid::Uuid::from_u128(3))
    }
    fn window() -> JournalWindow {
        JournalWindow {
            journal_id: uuid::Uuid::from_u128(1),
            oldest_seq: Seq::new(1),
            head_seq: Seq::new(42),
        }
    }
    fn facts_wire() -> TaskFactsWire {
        TaskFactsWire {
            task_id: task_id(),
            run_id: None,
            state: "open".into(),
            latest_turn_id: Some(turn_id()),
            outcome: Some("done".into()),
            code: None,
            runner_present: false,
            close_intent: false,
            auto_continue_intent: false,
            queue_dispatching: Some(false),
            result_imported: true,
            busy: Some(false),
            quiescent: Some(true),
            fact_digest: "a".repeat(64),
            title: None,
        }
    }

    #[test]
    fn cursor_and_dispatch_are_safe_contracts() {
        let seq = "9007199254740993".parse::<Seq>().unwrap();
        assert_eq!(serde_json::to_string(&seq).unwrap(), "\"9007199254740993\"");
        assert!("01".parse::<Seq>().is_err());
        let body = EventSelector::Read(ReadQuery {
            after: None,
            limit: 128,
            wait_ms: 15_000,
        })
        .request_body()
        .unwrap();
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert_eq!(body["controller_events"]["op"], "read");
    }

    #[test]
    fn selector_is_one_safe_task_list_key() {
        let body = EventSelector::Read(ReadQuery {
            after: None,
            limit: 128,
            wait_ms: 15_000,
        })
        .request_body()
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({"controller_events": {
                "op": "read", "after": null, "limit": 128, "wait_ms": 15_000
            }})
        );
    }

    #[test]
    fn all_outcomes_are_safe_title_free_events() {
        let cases = [
            (TaskOutcome::Done, "done"),
            (TaskOutcome::NeedsInput, "needs_input"),
            (TaskOutcome::Blocked, "blocked"),
            (TaskOutcome::Unknown, "unknown"),
            (TaskOutcome::failed("secret /private/path"), "failed"),
            (TaskOutcome::Cancelled, "cancelled"),
            (TaskOutcome::TimedOut, "timed_out"),
            (TaskOutcome::Lost, "lost"),
        ];
        for (outcome, want) in cases {
            let event = NewEvent::TurnFinished(TurnHint {
                task_id: task_id(),
                turn_id: turn_id(),
                run_id: None,
                outcome: SafeOutcome::from(&outcome),
                code: Some(SafeCode::from_public_code("secret /private/path")),
            })
            .to_wire(window().journal_id, Seq::new(1), 42)
            .unwrap();
            assert_eq!(event.kind, "turn.finished");
            assert_eq!(event.data["outcome"], want);
            assert_eq!(event.data["code"], "TURN_FAILED");
            assert_eq!(event.affected_task(), Some(task_id()));
            let encoded = serde_json::to_string(&event).unwrap();
            for forbidden in ["secret", "/private", "title", "reason", "prompt"] {
                assert!(!encoded.contains(forbidden));
            }
        }
    }

    #[test]
    fn stable_codes_preserve_catalog_only() {
        assert_eq!(
            SafeCode::from_public_code("PUBLISH_FAILED").as_str(),
            "PUBLISH_FAILED"
        );
        for text in [
            "PUBLISH_FAILED: secret",
            "TOKEN_ABC123",
            "/tmp/secret",
            "agent exited 1",
        ] {
            assert_eq!(SafeCode::from_public_code(text).as_str(), "TURN_FAILED");
        }
        for text in ["", "mini/1", "a\nb", &"a".repeat(129)] {
            assert!(WorkerName::parse(text).is_err());
        }
        assert_eq!(
            WorkerName::parse("mini-1@controller").unwrap().as_str(),
            "mini-1@controller"
        );
    }

    #[test]
    fn unsafe_state_and_queue_prose_never_form_a_batch() {
        assert!(
            EventBatch::try_new(vec![NewEvent::TaskChanged(TaskHint {
                task_id: task_id(),
                run_id: None,
                turn_id: None,
                state: "secret /tmp/a".into(),
                code: None
            })])
            .is_err()
        );
        assert!(
            EventBatch::try_new(vec![NewEvent::QueueChanged(QueueHint {
                turn_id: None,
                state: Some("secret".into()),
                kind: None,
                code: None
            })])
            .is_err()
        );
        assert!(
            EventBatch::try_new(vec![NewEvent::QueueChanged(QueueHint {
                turn_id: None,
                state: None,
                kind: Some("private label".into()),
                code: None
            })])
            .is_err()
        );
        let event = NewEvent::ControllerDrainChanged { drained: true };
        assert_eq!(
            EventBatch::try_new(vec![event.clone(); 32]).unwrap().len(),
            32
        );
        assert!(EventBatch::try_new(vec![event; 33]).is_err());
    }

    #[test]
    fn unknown_kind_invalidates_globally_and_newline_counts() {
        let mut event = WireEvent {
            schema_version: 1,
            journal_id: window().journal_id,
            seq: Seq::new(1),
            time_millis: 0,
            kind: "future.changed".into(),
            data: serde_json::json!({"task_id": task_id()}),
        };
        assert!(event.validate().is_ok());
        assert_eq!(event.affected_task(), None);
        let bytes = serde_json::to_vec(&event).unwrap().len();
        assert_eq!(event.encoded_len().unwrap(), bytes + 1);
        event.data = serde_json::json!({"x": ""});
        let base = serde_json::to_vec(&event).unwrap().len();
        event.data = serde_json::json!({"x": "a".repeat(1024 - base - 1)});
        assert!(event.validate().is_ok());
        event.data = serde_json::json!({"x": "a".repeat(1024 - base)});
        assert!(event.validate().is_err());
    }

    #[test]
    fn frame_accounting_includes_the_full_envelope() {
        let data = serde_json::json!({"result": ""});
        let base = serde_json::to_vec(&data).unwrap().len();
        assert!(
            ensure_frame_bound(
                &serde_json::json!({"result": "a".repeat(MAX_FRAME_BYTES - base - 1)})
            )
            .is_ok()
        );
        assert!(
            ensure_frame_bound(&serde_json::json!({"result": "a".repeat(MAX_FRAME_BYTES - base)}))
                .is_err()
        );
    }

    #[test]
    fn selector_key_exclusivity_and_limits() {
        for body in [
            serde_json::json!({"controller_events": {"op":"read"}, "state":"open"}),
            serde_json::json!({"controller_events": {"op":"read", "wait_ms":-1}}),
            serde_json::json!({"controller_events": {"op":"read", "path":"/tmp/a"}}),
            serde_json::json!({"controller_events": {"op":"tasks", "task_ids":[task_id(),task_id()]}}),
            serde_json::json!({"controller_events": {"op":"tasks", "task_ids":[]}}),
        ] {
            assert!(EventSelector::from_request_body(&body).is_err(), "{body}");
        }
        let read = EventSelector::from_request_body(
            &serde_json::json!({"controller_events":{"op":"read"}}),
        )
        .unwrap();
        assert_eq!(read, EventSelector::Read(ReadQuery::default()));
        assert_eq!(
            ReadQuery {
                after: None,
                limit: usize::MAX,
                wait_ms: u64::MAX
            }
            .normalized()
            .limit,
            256
        );
        assert_eq!(
            ReadQuery {
                after: None,
                limit: 0,
                wait_ms: u64::MAX
            }
            .normalized()
            .wait_ms,
            20_000
        );
        assert_eq!(
            TaskRepairQuery {
                after: None,
                limit: 0,
                baseline_after: None
            }
            .normalized()
            .limit,
            1
        );
        assert!(OpaqueCursor::parse("a".repeat(2049)).is_err());
        assert!(OpaqueCursor::parse("../private/path").is_err());
        assert!(serde_json::from_str::<Seq>("9007199254740993").is_err());
        assert!(Seq::new(u64::MAX).checked_increment().is_none());
    }

    #[test]
    fn unknown_proof_never_confirms_quiescence() {
        let good = TaskFacts::try_new(facts_wire()).unwrap();
        assert_eq!(good.eligibility_signature().quiescent, Some(true));
        for flag in 0..6 {
            let mut wire = facts_wire();
            match flag {
                0 => wire.runner_present = true,
                1 => wire.close_intent = true,
                2 => wire.auto_continue_intent = true,
                3 => wire.queue_dispatching = None,
                4 => wire.busy = Some(true),
                _ => wire.state = "future".into(),
            }
            let facts = TaskFacts::try_new(wire).unwrap();
            assert_ne!(facts.eligibility_signature().quiescent, Some(true));
        }
        let mut wire = facts_wire();
        wire.outcome = Some("future_outcome".into());
        assert_eq!(TaskFacts::try_new(wire).unwrap().outcome, None);
        let mut wire = facts_wire();
        wire.fact_digest.clear();
        assert!(TaskFacts::try_new(wire).is_err());
    }

    #[test]
    fn eligibility_signature_ignores_done_close_and_title_but_tracks_busy() {
        let open = TaskFacts::try_new(facts_wire()).unwrap();
        let mut wire = facts_wire();
        wire.state = "closed".into();
        wire.title = Some("display only".into());
        assert_eq!(
            open.eligibility_signature(),
            TaskFacts::try_new(wire).unwrap().eligibility_signature()
        );
        let mut wire = facts_wire();
        wire.runner_present = true;
        assert_ne!(
            open.eligibility_signature(),
            TaskFacts::try_new(wire).unwrap().eligibility_signature()
        );
        let mut wire = facts_wire();
        wire.outcome = Some("needs_input".into());
        assert!(
            TaskFacts::try_new(wire.clone())
                .unwrap()
                .eligibility_signature()
                .current_attention
        );
        wire.state = "closed".into();
        assert!(
            !TaskFacts::try_new(wire)
                .unwrap()
                .eligibility_signature()
                .current_attention
        );
    }

    #[test]
    fn no_baseline_is_not_an_empty_registry() {
        assert_ne!(
            PreviousProjection::Absent,
            PreviousProjection::Present(BTreeMap::new())
        );
        assert_eq!(ViewerMessage::Heartbeat.event_name(), "heartbeat");
        assert!(ViewerMessage::Heartbeat.cursor().is_none());
    }
}
