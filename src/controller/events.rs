//! Shared contracts for optional, lossy controller lifecycle hints.
//!
//! Saved task state remains authoritative. This module advertises no feature
//! and opens no state or journal. Component facades are filled by later tracks.

pub(crate) mod client;
pub(crate) mod contracts;
pub(crate) mod foreground;
pub(crate) mod journal;
pub(crate) mod notify;
pub(crate) mod rpc;
pub(crate) mod tail;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testing;
pub(crate) use contracts::*;

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
            integration: None,
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

    #[test]
    fn tolerant_replies_validate_identifiers_windows_and_committed_cursors() {
        let event = NewEvent::TurnFinished(TurnHint {
            task_id: task_id(),
            turn_id: turn_id(),
            run_id: None,
            outcome: SafeOutcome::Done,
            code: None,
        })
        .to_wire(window().journal_id, Seq::new(42), 0)
        .unwrap();
        let mut encoded = serde_json::to_value(&event).unwrap();
        encoded["future_field"] = serde_json::json!(true);
        assert_eq!(
            serde_json::from_value::<WireEvent>(encoded.clone()).unwrap(),
            event
        );
        encoded["seq"] = serde_json::json!("0");
        assert!(serde_json::from_value::<WireEvent>(encoded).is_err());
        assert!(serde_json::from_value::<JournalWindow>(serde_json::json!({"journal_id":window().journal_id.to_string(),"oldest_seq":"43","head_seq":"41"})).is_err());
        let mut batch = serde_json::json!({"type":"batch","schema_version":1,"journal_id":window().journal_id.to_string(),"oldest_seq":"1","head_seq":"42","next_after":{"journal_id":window().journal_id.to_string(),"seq":"42"},"events":[event],"has_more":false,"future_field":true});
        assert!(serde_json::from_value::<EventReadResult>(batch.clone()).is_ok());
        batch["next_after"]["seq"] = serde_json::json!("41");
        assert!(serde_json::from_value::<EventReadResult>(batch).is_err());
    }

    #[test]
    fn task_and_cache_wire_bounds_are_checked_on_decode() {
        let facts = TaskFacts::try_new(facts_wire()).unwrap();
        let batch = serde_json::json!({"rows":[facts.clone(),facts],"missing":[],"proof_after":null,"baseline_after":null});
        assert!(serde_json::from_value::<TaskFactsBatch>(batch).is_err());
        let mut wire = facts_wire();
        wire.code = Some("x".repeat(2049));
        assert!(TaskFacts::try_new(wire).is_err());
        let mut cache = serde_json::to_value(NotifyState::empty()).unwrap();
        cache["decisions"] = serde_json::json!(vec!["a".repeat(64); 4097]);
        assert!(serde_json::from_value::<NotifyState>(cache).is_err());
    }

    #[test]
    fn json_fixtures_match_rust_and_controls_have_no_cursor() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../ui/src/lib/controllerEvents.fixtures.json"
        ))
        .unwrap();
        let win: JournalWindow =
            serde_json::from_value(fixtures["bootstrap"]["data"]["window"].clone()).unwrap();
        let event: WireEvent =
            serde_json::from_value(fixtures["event_above_2pow53"]["data"].clone()).unwrap();
        assert_eq!(event.seq.as_u64(), 9_007_199_254_740_993);
        for (key, message) in [
            (
                "bootstrap",
                ViewerMessage::SnapshotRequired(SnapshotRequired {
                    reason: "bootstrap".into(),
                    window: win,
                }),
            ),
            ("event_above_2pow53", ViewerMessage::ControllerEvent(event)),
            (
                "snapshot.ready",
                ViewerMessage::SnapshotReady { revision: 42 },
            ),
            ("heartbeat", ViewerMessage::Heartbeat),
        ] {
            assert_eq!(serde_json::to_value(&message).unwrap(), fixtures[key]);
            assert_eq!(message.cursor().is_some(), key == "event_above_2pow53");
        }
    }

    #[test]
    fn review_direct_addressed_dtos_reject_invalid_task_sets() {
        for ids in [
            Vec::new(),
            vec![task_id(), task_id()],
            (1..=17)
                .map(|id| TaskId::new(uuid::Uuid::from_u128(id)))
                .collect(),
        ] {
            let query =
                serde_json::json!({"task_ids":ids,"include_titles":false,"proof_after":null});
            assert!(serde_json::from_value::<TaskAddressQuery>(query.clone()).is_err());
            let mut selector = query;
            selector["op"] = serde_json::json!("tasks");
            assert!(serde_json::from_value::<EventSelector>(selector).is_err());
        }
    }

    #[test]
    fn review_request_cursor_keys_are_strict_but_reply_cursors_tolerant() {
        let cursor = serde_json::json!({"journal_id":window().journal_id.to_string(),"seq":"42","future":true});
        assert!(serde_json::from_value::<EventCursor>(cursor.clone()).is_ok());
        assert!(
            serde_json::from_value::<ReadQuery>(serde_json::json!({"after":cursor.clone()}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<TaskRepairQuery>(
                serde_json::json!({"baseline_after":cursor.clone()})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<EventSelector>(
                serde_json::json!({"op":"read","after":cursor})
            )
            .is_err()
        );
    }

    #[test]
    fn review_direct_hints_reject_owned_prose_fields() {
        assert!(serde_json::from_value::<TaskHint>(serde_json::json!({"task_id":task_id(),"run_id":null,"turn_id":null,"state":"secret /tmp/a","code":null})).is_err());
        assert!(serde_json::from_value::<QueueHint>(serde_json::json!({"turn_id":null,"state":null,"kind":"private queue label","code":null})).is_err());
    }
}
