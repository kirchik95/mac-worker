use mac_worker::test_support::{
    events::{
        BaselineKind, ChangeCause, DerivedTaskChange, Reconciliation, SafeOutcome, TaskFacts,
    },
    task::model::{TaskId, TurnId},
};
#[test]
fn cold_repair_cannot_make_historical_notification_changes() {
    let id = TaskId::new(uuid::Uuid::from_u128(1));
    let turn = TurnId::new(uuid::Uuid::from_u128(2));
    let facts = TaskFacts::test_terminal(id, turn, SafeOutcome::Done, true);
    let mut cold = Reconciliation::test_cold(None, vec![facts.clone()]);
    cold.validate().unwrap();
    assert_eq!(cold.baseline, BaselineKind::Cold);
    assert!(cold.changes.is_empty());
    cold.changes.push(DerivedTaskChange {
        task_id: id,
        previous: None,
        current: Some(facts),
        cause: ChangeCause::RepairDifference,
    });
    assert!(cold.validate().is_err());
}

mod policy {
    use sha2::{Digest, Sha256};
    fn sha256_hex(value: &str) -> String {
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    fn outcome_token(outcome: SafeOutcome) -> &'static str {
        match outcome {
            SafeOutcome::Done => "done",
            SafeOutcome::NeedsInput => "needs_input",
            SafeOutcome::Blocked => "blocked",
            SafeOutcome::Unknown => "unknown",
            SafeOutcome::Failed => "failed",
            SafeOutcome::Cancelled => "cancelled",
            SafeOutcome::TimedOut => "timed_out",
            SafeOutcome::Lost => "lost",
        }
    }

    fn target_digest(
        controller: &mac_worker::test_support::core::config::ControllerConfig,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(controller.ssh.as_bytes());
        hasher.update([0xff]);
        hasher.update(controller.remote_binary.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn cache_directory(
        paths: &mac_worker::test_support::core::paths::PathLayout,
        controller: &mac_worker::test_support::core::config::ControllerConfig,
    ) -> std::path::PathBuf {
        paths
            .controller_cache_root()
            .join("events")
            .join(target_digest(controller))
    }

    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use uuid::Uuid;

    use mac_worker::test_support::events::notify::{
        NotifyCache, NotifyPlan, commit_then_deliver, plan_notifications,
    };
    use mac_worker::test_support::events::testing::{
        FakeEventReconciler, ManualEventRuntime, RecordingNoticeChannel, ScriptedEventSource,
    };
    use mac_worker::test_support::{
        core::{config::ControllerConfig, error::WorkerError, paths::PathLayout},
        events::contracts::{
            AttentionSummary, BaselineKind, ChangeCause, DerivedTaskChange, EventCursor,
            EventReconciler, MAX_NOTIFY_STATE_BYTES, MAX_RECONCILIATION_ROWS,
            NOTIFY_DECISION_CAPACITY, Notice, NoticeChannel, NoticeSound, NotifyOptions,
            NotifyState, ReconcileInput, Reconciliation, RepairProgress, SafeCode, SafeOutcome,
            Seq, TaskFacts,
        },
        task::model::{TaskId, TurnId},
    };

    struct NotifierHarness {
        root: tempfile::TempDir,
        paths: PathLayout,
        controller: ControllerConfig,
        cache: Option<NotifyCache>,
        saved: NotifyState,
        journal_id: Uuid,
        delivered: Vec<Notice>,
        delivered_mark: usize,
        evicted_task: TaskId,
        evicted_turn: TurnId,
        overflow: Option<String>,
        now: u64,
        interrupt: bool,
    }

    impl NotifierHarness {
        fn new() -> Self {
            let root = tempfile::tempdir().expect("tempdir");
            let paths = layout(root.path());
            let controller = controller("notifier@cache-host");
            let cache = NotifyCache::open(&paths, &controller).expect("cache");
            let journal_id = Uuid::from_u128(7);
            let mut saved = NotifyState::empty();
            saved.consumed_after = Some(EventCursor {
                journal_id,
                seq: Seq::new(1),
            });
            cache.save(&saved).expect("seed");
            Self {
                root,
                paths,
                controller,
                cache: Some(cache),
                saved,
                journal_id,
                delivered: Vec::new(),
                delivered_mark: 0,
                evicted_task: TaskId::new(Uuid::from_u128(1)),
                evicted_turn: TurnId::new(Uuid::from_u128(100_001)),
                overflow: None,
                now: 10_000,
                interrupt: false,
            }
        }

        fn consume(&mut self, result: Reconciliation, options: NotifyOptions) {
            let plan = plan_notifications(&self.saved, &result, &options, self.now);
            self.now += 1_000;
            if self.interrupt {
                let cache = self.cache.as_ref().expect("cache");
                cache.save(&plan.next).expect("save");
                self.saved = cache.load().expect("load");
                self.interrupt = false;
                return;
            }
            let recorded = Arc::new(RecordingNoticeChannel::new());
            let channel: Arc<dyn NoticeChannel> = recorded.clone();
            let delivered = commit_then_deliver(
                self.cache.as_ref().expect("cache"),
                plan,
                std::slice::from_ref(&channel),
                &ManualEventRuntime::new(),
            )
            .expect("deliver");
            assert!(delivered.iter().all(|ok| *ok));
            self.delivered
                .extend(recorded.records().into_iter().map(|(notice, _)| notice));
            self.saved = self.cache.as_ref().expect("cache").load().expect("reload");
        }

        fn consume_more_than_4096_decisions(&mut self) {
            let mut facts = Vec::with_capacity(4_097);
            for index in 1..=4_097_u128 {
                let task_id = TaskId::new(Uuid::from_u128(index));
                let turn_id = TurnId::new(Uuid::from_u128(100_000 + index));
                if index == 1 {
                    self.evicted_task = task_id;
                    self.evicted_turn = turn_id;
                }
                facts.push(done_facts(task_id, turn_id));
            }
            for (offset, chunk) in facts.chunks(MAX_RECONCILIATION_ROWS).enumerate() {
                let result = warm_complete(
                    self.journal_id,
                    2 + offset as u64,
                    chunk.iter().cloned().map(change).collect(),
                    chunk.to_vec(),
                );
                result.validate().expect("decision chunk");
                self.consume(result, NotifyOptions::default());
            }
            assert_eq!(self.decisions().len(), NOTIFY_DECISION_CAPACITY);
            let evicted = decision_text(&done_facts(self.evicted_task, self.evicted_turn));
            assert!(
                !self
                    .saved
                    .decisions
                    .iter()
                    .any(|decision| decision == &evicted)
            );
        }

        fn consume_attention_overflow_quiet(&mut self) {
            let mut facts = Vec::with_capacity(257);
            for index in 0..257_u128 {
                let outcome = if index % 2 == 0 {
                    SafeOutcome::NeedsInput
                } else {
                    SafeOutcome::Blocked
                };
                facts.push(proved(
                    TaskId::new(Uuid::from_u128(50_000 + index)),
                    Some(TurnId::new(Uuid::from_u128(80_000 + index))),
                    "open",
                    Some(outcome),
                ));
            }
            let fingerprint = attention_set_digest(&facts);
            assert_ne!(fingerprint, format!("{:064x}", 257_u128));
            self.overflow = Some(fingerprint.clone());
            let quiet = NotifyOptions {
                quiet: true,
                ..NotifyOptions::default()
            };
            let chunks: Vec<_> = facts.chunks(MAX_RECONCILIATION_ROWS).collect();
            let last = chunks.len() - 1;
            for (index, chunk) in chunks.into_iter().enumerate() {
                let mut result = warm_complete(
                    self.journal_id,
                    100 + index as u64,
                    chunk.iter().cloned().map(change).collect(),
                    chunk.to_vec(),
                );
                if index == last {
                    result.repair = RepairProgress::Complete;
                    result.repair_needed = false;
                    result.attention = Some(AttentionSummary {
                        count: facts.len(),
                        fingerprint: fingerprint.clone(),
                    });
                } else {
                    result.repair = RepairProgress::InProgress;
                    result.repair_needed = true;
                    result.attention = None;
                }
                result.validate().expect("attention chunk");
                self.consume(result, quiet.clone());
                if index != last {
                    assert_ne!(
                        self.saved.attention_overflow.as_deref(),
                        Some(fingerprint.as_str())
                    );
                }
            }
            assert_eq!(
                self.saved.attention_overflow.as_deref(),
                Some(fingerprint.as_str())
            );
        }

        fn restart_with_valid_cursor(&mut self) {
            assert!(self.saved.consumed_after.is_some());
            self.cache = None;
            self.cache = Some(NotifyCache::open(&self.paths, &self.controller).expect("reopen"));
            self.saved = self.cache.as_ref().expect("cache").load().expect("reload");
            self.delivered_mark = self.delivered.len();
        }

        fn consume_cold_history_with_lost_hint(&mut self) {
            let facts = done_facts(self.evicted_task, self.evicted_turn);
            let mut result = cold_complete(self.journal_id, 4, vec![facts]);
            result.attention =
                self.saved
                    .attention_overflow
                    .clone()
                    .map(|fingerprint| AttentionSummary {
                        count: 257,
                        fingerprint,
                    });
            result.consumed_after = self.saved.consumed_after;
            result.validate().expect("cold history");
            self.consume(result, NotifyOptions::default());
        }

        fn change_epoch(&mut self) {
            self.journal_id = Uuid::from_u128(8);
            if let Some(cursor) = self.saved.consumed_after.as_mut() {
                cursor.journal_id = self.journal_id;
                cursor.seq = Seq::ZERO;
            }
            let decisions = self.saved.decisions.clone();
            let overflow = self.saved.attention_overflow.clone();
            let cache = self.cache.as_ref().expect("cache");
            cache.save(&self.saved).expect("epoch save");
            self.saved = cache.load().expect("epoch load");
            assert_eq!(self.saved.decisions, decisions);
            assert_eq!(self.saved.attention_overflow, overflow);
        }

        fn consume_unchanged_attention_overflow(&mut self) {
            let fingerprint = self.overflow.clone().expect("overflow");
            let mut result = cold_complete(self.journal_id, 1, Vec::new());
            result.attention = Some(AttentionSummary {
                count: 257,
                fingerprint,
            });
            result.validate().expect("unchanged overflow");
            self.consume(result, NotifyOptions::default());
        }

        fn decisions(&self) -> &[String] {
            &self.saved.decisions
        }

        fn delivered(&self) -> &[Notice] {
            &self.delivered
        }

        fn interrupt_after_save(&mut self) {
            self.interrupt = true;
        }

        fn overflow_fingerprint(&self) -> Option<String> {
            self.saved.attention_overflow.clone()
        }

        fn delivered_since_restart(&self) -> usize {
            self.delivered.len().saturating_sub(self.delivered_mark)
        }
    }

    fn layout(root: &std::path::Path) -> PathLayout {
        PathLayout {
            config: root.join("config.toml"),
            state: root.join("state"),
            cache: root.join("cache"),
            data: root.join("data"),
        }
    }

    fn controller(ssh: &str) -> ControllerConfig {
        ControllerConfig {
            enabled: true,
            ssh: ssh.to_owned(),
            remote_binary: "~/.local/bin/worker".into(),
        }
    }

    fn done_facts(task_id: TaskId, turn_id: TurnId) -> TaskFacts {
        proved(task_id, Some(turn_id), "closed", Some(SafeOutcome::Done))
    }

    fn proved(
        task_id: TaskId,
        turn_id: Option<TurnId>,
        state: &str,
        outcome: Option<SafeOutcome>,
    ) -> TaskFacts {
        TaskFacts {
            integration: None,
            task_id,
            run_id: None,
            state: state.to_owned(),
            latest_turn_id: turn_id,
            outcome,
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

    fn change(facts: TaskFacts) -> DerivedTaskChange {
        DerivedTaskChange {
            task_id: facts.task_id,
            previous: None,
            current: Some(facts),
            cause: ChangeCause::RepairDifference,
        }
    }

    fn warm_complete(
        journal_id: Uuid,
        seq: u64,
        changes: Vec<DerivedTaskChange>,
        confirmed: Vec<TaskFacts>,
    ) -> Reconciliation {
        Reconciliation {
            consumed_after: Some(EventCursor {
                journal_id,
                seq: Seq::new(seq),
            }),
            baseline: BaselineKind::Warm,
            changes,
            confirmed,
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        }
    }

    fn cold_complete(journal_id: Uuid, seq: u64, confirmed: Vec<TaskFacts>) -> Reconciliation {
        Reconciliation {
            consumed_after: Some(EventCursor {
                journal_id,
                seq: Seq::new(seq),
            }),
            baseline: BaselineKind::Cold,
            changes: Vec::new(),
            confirmed,
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        }
    }

    fn attention_set_digest(facts: &[TaskFacts]) -> String {
        let mut lines = Vec::new();
        for facts in facts {
            let Some(outcome) = facts.outcome else {
                continue;
            };
            if facts.state != "open" || facts.latest_turn_id.is_none() {
                continue;
            }
            if !matches!(outcome, SafeOutcome::NeedsInput | SafeOutcome::Blocked) {
                continue;
            }
            let turn = facts
                .latest_turn_id
                .map(|turn| turn.to_string())
                .unwrap_or_else(|| "-".to_owned());
            lines.push(format!(
                "{}|{turn}|{}",
                facts.task_id,
                outcome_token(outcome)
            ));
        }
        lines.sort();
        lines.dedup();
        let mut payload = format!("count={}\n", lines.len());
        for line in &lines {
            payload.push_str(line);
            payload.push('\n');
        }
        sha256_hex(&payload)
    }

    fn decision_text(facts: &TaskFacts) -> String {
        sha256_hex(&format!(
            "turn:{}:{}:{}",
            facts.task_id,
            facts.latest_turn_id.expect("turn"),
            outcome_token(facts.outcome.expect("outcome"))
        ))
    }

    fn plan_warm(facts: TaskFacts) -> NotifyPlan {
        let saved = NotifyState::empty();
        plan_notifications(
            &saved,
            &warm_complete(
                Uuid::from_u128(9),
                1,
                vec![change(facts.clone())],
                vec![facts],
            ),
            &NotifyOptions::default(),
            5_000,
        )
    }

    #[test]
    fn evicted_history_and_unchanged_overflow_do_not_replay() {
        let mut harness = NotifierHarness::new();
        harness.consume_more_than_4096_decisions();
        harness.consume_attention_overflow_quiet();
        let fingerprint = harness.overflow_fingerprint();
        harness.restart_with_valid_cursor();
        harness.consume_cold_history_with_lost_hint();
        harness.change_epoch();
        harness.consume_unchanged_attention_overflow();
        assert_eq!(harness.overflow_fingerprint(), fingerprint);
        assert_eq!(harness.delivered_since_restart(), 0);
        let _ = harness.root;
    }

    #[test]
    fn replay_hint_after_saved_cursor_notifies_fresh_eligibility() {
        let mut harness = NotifierHarness::new();
        let historical = done_facts(
            TaskId::new(Uuid::from_u128(20)),
            TurnId::new(Uuid::from_u128(21)),
        );
        harness.consume(
            cold_complete(harness.journal_id, 2, vec![historical]),
            NotifyOptions::default(),
        );
        assert!(harness.delivered().is_empty());
        harness.restart_with_valid_cursor();
        let task_id = TaskId::new(Uuid::from_u128(30));
        let turn_id = TurnId::new(Uuid::from_u128(31));
        let facts = done_facts(task_id, turn_id);
        let mut result = cold_complete(harness.journal_id, 3, vec![facts.clone()]);
        result.changes.push(DerivedTaskChange {
            task_id,
            previous: None,
            current: Some(facts),
            cause: ChangeCause::ReplayTerminal {
                turn_id,
                outcome: SafeOutcome::Done,
            },
        });
        harness.consume(result, NotifyOptions::default());
        assert_eq!(harness.delivered_since_restart(), 1);
        assert_eq!(
            harness.delivered().last().expect("notice").sound,
            NoticeSound::Done
        );
    }

    #[test]
    fn eight_outcomes_notify_after_confirmed_quiescence() {
        let cases = [
            (SafeOutcome::Done, NoticeSound::Done, "Done", "closed"),
            (
                SafeOutcome::NeedsInput,
                NoticeSound::Request,
                "Needs input",
                "open",
            ),
            (
                SafeOutcome::Blocked,
                NoticeSound::Request,
                "Blocked",
                "open",
            ),
            (SafeOutcome::Unknown, NoticeSound::None, "Unknown", "closed"),
            (SafeOutcome::Failed, NoticeSound::None, "Failed", "closed"),
            (
                SafeOutcome::Cancelled,
                NoticeSound::None,
                "Cancelled",
                "closed",
            ),
            (
                SafeOutcome::TimedOut,
                NoticeSound::None,
                "Timed out",
                "closed",
            ),
            (SafeOutcome::Lost, NoticeSound::None, "Lost", "lost"),
        ];
        for (index, (outcome, sound, label, state)) in cases.into_iter().enumerate() {
            let facts = proved(
                TaskId::new(Uuid::from_u128(200 + index as u128)),
                Some(TurnId::new(Uuid::from_u128(300 + index as u128))),
                state,
                Some(outcome),
            );
            let plan = plan_warm(facts);
            assert_eq!(plan.notices.len(), 1, "{label}");
            assert_eq!(plan.notices[0].sound, sound, "{label}");
            assert_eq!(plan.notices[0].title, label);
        }
    }

    #[test]
    fn open_needs_input_is_attention_and_closed_history_is_not() {
        let open = proved(
            TaskId::new(Uuid::from_u128(40)),
            Some(TurnId::new(Uuid::from_u128(41))),
            "open",
            Some(SafeOutcome::NeedsInput),
        );
        let closed = proved(
            TaskId::new(Uuid::from_u128(42)),
            Some(TurnId::new(Uuid::from_u128(43))),
            "closed",
            Some(SafeOutcome::NeedsInput),
        );
        let open_plan = plan_warm(open.clone());
        let closed_plan = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(9), 1, vec![closed.clone()]),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(open_plan.notices.len(), 1);
        assert!(closed_plan.notices.is_empty());
        assert_ne!(
            open_plan.next.attention_overflow,
            closed_plan.next.attention_overflow
        );
        assert_eq!(closed_plan.next.decisions, vec![decision_text(&closed)]);
        let _ = (open, closed);
    }

    #[test]
    fn abandonment_without_terminal_uses_stable_code_and_no_sound() {
        let mut facts = proved(TaskId::new(Uuid::from_u128(50)), None, "abandoned", None);
        facts.code = Some(SafeCode::from_public_code("LEFT"));
        let plan = plan_warm(facts.clone());
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].sound, NoticeSound::None);
        assert_eq!(plan.notices[0].title, "Abandoned");
        assert_eq!(
            plan.notices[0].fingerprint,
            sha256_hex(&format!("abandon:{}:TURN_FAILED", facts.task_id))
        );
        let stored = serde_json::to_string(&plan.next).expect("json");
        assert!(!stored.contains("LEFT"));
    }

    #[test]
    fn busy_unknown_continuation_close_runner_and_dispatch_suppress() {
        let mutators: &[fn(&mut TaskFacts)] = &[
            |facts| facts.busy = Some(true),
            |facts| facts.busy = None,
            |facts| facts.quiescent = None,
            |facts| facts.quiescent = Some(false),
            |facts| facts.auto_continue_intent = true,
            |facts| facts.close_intent = true,
            |facts| facts.runner_present = true,
            |facts| facts.queue_dispatching = Some(true),
            |facts| facts.queue_dispatching = None,
            |facts| facts.state = "active".into(),
        ];
        for (index, mutate) in mutators.iter().enumerate() {
            let mut facts = done_facts(
                TaskId::new(Uuid::from_u128(400 + index as u128)),
                TurnId::new(Uuid::from_u128(500 + index as u128)),
            );
            mutate(&mut facts);
            let task_id = facts.task_id;
            let plan = plan_warm(facts);
            assert!(plan.notices.is_empty(), "case {index}");
            assert!(
                plan.next
                    .pending
                    .iter()
                    .any(|candidate| candidate.task_id == task_id),
                "case {index}"
            );
        }
    }

    #[test]
    fn superseded_hint_does_not_notify_and_latest_turn_can_replace() {
        let task_id = TaskId::new(Uuid::from_u128(60));
        let old_turn = TurnId::new(Uuid::from_u128(61));
        let new_turn = TurnId::new(Uuid::from_u128(62));
        let current = done_facts(task_id, new_turn);
        let mut result = warm_complete(Uuid::from_u128(9), 1, Vec::new(), vec![current.clone()]);
        result.changes.push(DerivedTaskChange {
            task_id,
            previous: None,
            current: Some(current.clone()),
            cause: ChangeCause::ReplayTerminal {
                turn_id: old_turn,
                outcome: SafeOutcome::Done,
            },
        });
        let suppressed = plan_notifications(
            &NotifyState::empty(),
            &result,
            &NotifyOptions::default(),
            5_000,
        );
        assert!(suppressed.notices.is_empty());
        assert!(suppressed.next.decisions.is_empty());

        let first = plan_warm(done_facts(task_id, old_turn));
        assert_eq!(first.notices.len(), 1);
        let replacement = plan_notifications(
            &first.next,
            &warm_complete(
                Uuid::from_u128(9),
                2,
                vec![change(current.clone())],
                vec![current.clone()],
            ),
            &NotifyOptions::default(),
            6_000,
        );
        assert_eq!(replacement.notices.len(), 1);
        assert_eq!(replacement.notices[0].fingerprint, decision_text(&current));
        assert_eq!(replacement.next.decisions.len(), 2);
    }

    #[test]
    fn warm_busy_to_quiescent_notifies_and_unchanged_eviction_stays_silent() {
        let task_id = TaskId::new(Uuid::from_u128(70));
        let turn_id = TurnId::new(Uuid::from_u128(71));
        let mut busy = done_facts(task_id, turn_id);
        busy.busy = Some(true);
        busy.quiescent = Some(false);
        let ready = done_facts(task_id, turn_id);
        let result = Reconciliation {
            consumed_after: Some(EventCursor {
                journal_id: Uuid::from_u128(9),
                seq: Seq::new(1),
            }),
            baseline: BaselineKind::Warm,
            changes: vec![DerivedTaskChange {
                task_id,
                previous: Some(busy),
                current: Some(ready.clone()),
                cause: ChangeCause::RepairDifference,
            }],
            confirmed: vec![ready],
            pending_ids: Vec::new(),
            repair: RepairProgress::Complete,
            attention: None,
            repair_needed: false,
        };
        let plan = plan_notifications(
            &NotifyState::empty(),
            &result,
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(plan.notices.len(), 1);

        let mut harness = NotifierHarness::new();
        harness.consume_more_than_4096_decisions();
        let before = harness.delivered().len();
        let evicted = done_facts(harness.evicted_task, harness.evicted_turn);
        let fingerprint = decision_text(&evicted);
        harness.consume(
            warm_complete(harness.journal_id, 9, Vec::new(), vec![evicted]),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered().len(), before);
        assert!(
            !harness
                .saved
                .decisions
                .iter()
                .any(|decision| decision == &fingerprint)
        );
    }

    #[test]
    fn present_empty_warm_projection_is_not_a_cold_baseline() {
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(80)),
            TurnId::new(Uuid::from_u128(81)),
        );
        let warm = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, Vec::new(), vec![facts.clone()]),
            &NotifyOptions::default(),
            5_000,
        );
        let cold = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(9), 1, vec![facts]),
            &NotifyOptions::default(),
            5_000,
        );
        assert!(warm.notices.is_empty());
        assert!(warm.next.decisions.is_empty());
        assert!(cold.notices.is_empty());
        assert_eq!(cold.next.decisions.len(), 1);
    }

    #[test]
    fn incomplete_repair_keeps_the_previous_summary() {
        let saved = NotifyState {
            attention_overflow: Some("keep-me".into()),
            ..NotifyState::empty()
        };
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(90)),
            TurnId::new(Uuid::from_u128(91)),
        );
        let mut result = warm_complete(
            Uuid::from_u128(9),
            4,
            vec![change(facts.clone())],
            vec![facts],
        );
        result.repair = RepairProgress::InProgress;
        result.repair_needed = true;
        result.attention = Some(AttentionSummary {
            count: 3,
            fingerprint: "do-not-claim".into(),
        });
        let plan = plan_notifications(&saved, &result, &NotifyOptions::default(), 5_000);
        assert_eq!(plan.next.attention_overflow.as_deref(), Some("keep-me"));
        assert!(plan.next.repair_needed);
        assert_eq!(plan.notices.len(), 1);
    }

    #[test]
    fn coalesce_over_five_after_disconnect_and_on_epoch_repair() {
        let mut confirmed = Vec::new();
        let mut changes = Vec::new();
        for index in 0..6 {
            let facts = done_facts(
                TaskId::new(Uuid::from_u128(600 + index)),
                TurnId::new(Uuid::from_u128(700 + index)),
            );
            changes.push(change(facts.clone()));
            confirmed.push(facts);
        }
        let batch = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, changes, confirmed),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(batch.notices.len(), 1);
        assert_eq!(batch.notices[0].sound, NoticeSound::Done);
        assert_eq!(batch.notices[0].title, "Tasks finished");
        assert_eq!(batch.notices[0].body, "6 tasks");
        assert_eq!(batch.next.decisions.len(), 6);

        let later = done_facts(
            TaskId::new(Uuid::from_u128(610)),
            TurnId::new(Uuid::from_u128(710)),
        );
        let saved = NotifyState {
            last_complete_repair_millis: Some(0),
            consumed_after: Some(EventCursor {
                journal_id: Uuid::from_u128(9),
                seq: Seq::new(1),
            }),
            ..NotifyState::empty()
        };
        let disconnected = plan_notifications(
            &saved,
            &warm_complete(
                Uuid::from_u128(9),
                2,
                vec![change(later.clone())],
                vec![later],
            ),
            &NotifyOptions::default(),
            60_001,
        );
        assert_eq!(disconnected.notices.len(), 1);
        assert_eq!(disconnected.notices[0].title, "Tasks finished");

        let epoch_facts = done_facts(
            TaskId::new(Uuid::from_u128(611)),
            TurnId::new(Uuid::from_u128(711)),
        );
        let epoch = plan_notifications(
            &saved,
            &warm_complete(
                Uuid::from_u128(99),
                1,
                vec![change(epoch_facts.clone())],
                vec![epoch_facts],
            ),
            &NotifyOptions::default(),
            1_000,
        );
        assert_eq!(epoch.notices.len(), 1);
        assert_eq!(epoch.notices[0].title, "Tasks finished");
        assert_ne!(
            epoch.next.consumed_after.unwrap().journal_id,
            Uuid::from_u128(9)
        );
    }

    #[test]
    fn same_epoch_restart_coalesces_two_to_five_decisions() {
        let journal = Uuid::from_u128(9);
        let saved = NotifyState {
            last_complete_repair_millis: Some(1_000),
            consumed_after: Some(EventCursor {
                journal_id: journal,
                seq: Seq::new(4),
            }),
            attention_overflow: Some("a".repeat(64)),
            ..NotifyState::empty()
        };
        let mut changes = Vec::new();
        let mut confirmed = Vec::new();
        for index in 0..3_u128 {
            let facts = done_facts(
                TaskId::new(Uuid::from_u128(800 + index)),
                TurnId::new(Uuid::from_u128(900 + index)),
            );
            changes.push(change(facts.clone()));
            confirmed.push(facts);
        }
        let mut result = warm_complete(journal, 5, changes, confirmed);
        result.repair = RepairProgress::Restarted;
        result.repair_needed = true;
        result.attention = Some(AttentionSummary {
            count: 9,
            fingerprint: "b".repeat(64),
        });
        let plan = plan_notifications(&saved, &result, &NotifyOptions::default(), 2_000);
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].title, "Tasks finished");
        assert_eq!(plan.notices[0].body, "3 tasks");
        assert_eq!(plan.notices[0].sound, NoticeSound::Done);
        assert_eq!(plan.next.decisions.len(), 3);
        assert_eq!(
            plan.next.attention_overflow.as_deref(),
            Some("a".repeat(64).as_str())
        );
        assert!(plan.next.repair_needed);
    }

    #[test]
    fn mixed_and_attention_summaries_use_request_otherwise_none() {
        let mut changes = Vec::new();
        let mut confirmed = Vec::new();
        for index in 0..5 {
            let facts = proved(
                TaskId::new(Uuid::from_u128(800 + index)),
                Some(TurnId::new(Uuid::from_u128(900 + index))),
                "closed",
                Some(SafeOutcome::Failed),
            );
            changes.push(change(facts.clone()));
            confirmed.push(facts);
        }
        let attention = proved(
            TaskId::new(Uuid::from_u128(806)),
            Some(TurnId::new(Uuid::from_u128(906))),
            "open",
            Some(SafeOutcome::Blocked),
        );
        changes.push(change(attention.clone()));
        confirmed.push(attention);
        let plan = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, changes, confirmed),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(plan.notices.len(), 1);
        assert_eq!(plan.notices[0].sound, NoticeSound::Request);
        assert_eq!(plan.notices[0].title, "Tasks need attention");

        let mut failed_changes = Vec::new();
        let mut failed = Vec::new();
        for index in 0..6 {
            let facts = proved(
                TaskId::new(Uuid::from_u128(820 + index)),
                Some(TurnId::new(Uuid::from_u128(920 + index))),
                "closed",
                Some(SafeOutcome::Failed),
            );
            failed_changes.push(change(facts.clone()));
            failed.push(facts);
        }
        let none = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(Uuid::from_u128(9), 1, failed_changes, failed),
            &NotifyOptions::default(),
            5_000,
        );
        assert_eq!(none.notices[0].sound, NoticeSound::None);
        assert_eq!(none.notices[0].title, "Tasks updated");
    }

    #[test]
    fn quiet_saves_decisions_without_notices_or_a_backlog() {
        let mut harness = NotifierHarness::new();
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(80)),
            TurnId::new(Uuid::from_u128(81)),
        );
        harness.consume(
            warm_complete(
                harness.journal_id,
                2,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            NotifyOptions {
                quiet: true,
                ..NotifyOptions::default()
            },
        );
        assert!(harness.delivered().is_empty());
        assert_eq!(harness.saved.decisions.len(), 1);
        harness.restart_with_valid_cursor();
        harness.consume(
            warm_complete(
                harness.journal_id,
                3,
                vec![change(facts.clone())],
                vec![facts],
            ),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered_since_restart(), 0);
    }

    #[test]
    fn titles_default_on_and_no_titles_and_redaction_stay_out_of_the_cache() {
        let mut facts = done_facts(
            TaskId::new(Uuid::from_u128(100)),
            TurnId::new(Uuid::from_u128(101)),
        );
        facts.title = Some("Ship the lock".into());
        let shown = plan_warm(facts.clone());
        assert!(shown.notices[0].body.contains("Ship the lock"));
        assert!(shown.notices[0].body.contains(&facts.task_id.to_string()));
        let hidden = plan_notifications(
            &NotifyState::empty(),
            &warm_complete(
                Uuid::from_u128(9),
                1,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            &NotifyOptions {
                no_titles: true,
                ..NotifyOptions::default()
            },
            5_000,
        );
        assert_eq!(hidden.notices[0].body, facts.task_id.to_string());
        assert!(!hidden.notices[0].title.contains("Ship the lock"));
        let stored = serde_json::to_string(&hidden.next).expect("json");
        assert!(!stored.contains("Ship the lock"));

        let mut secret = facts.clone();
        secret.task_id = TaskId::new(Uuid::from_u128(102));
        secret.title = Some("see /Users/hidden/notes sk-supersecretvalue".into());
        let redacted = plan_warm(secret.clone());
        let body = &redacted.notices[0].body;
        assert!(!body.contains("/Users/hidden"));
        assert!(!body.contains("sk-supersecretvalue"));
        assert!(body.contains("[path]"));
        assert!(body.contains("[token]"));
        let cache_json = serde_json::to_string(&redacted.next).expect("json");
        assert!(!cache_json.contains("sk-supersecretvalue"));
        assert!(!cache_json.contains("/Users/hidden"));

        let mut controlled = secret.clone();
        controlled.task_id = TaskId::new(Uuid::from_u128(103));
        controlled.title = Some("see /Users/hidden/notes\nsk-supersecretvalue\u{0001}".into());
        let blocked = plan_warm(controlled);
        assert!(blocked.notices.is_empty());
        let blocked_json = serde_json::to_string(&blocked.next).expect("json");
        assert!(!blocked_json.contains("sk-supersecretvalue"));
        assert!(!blocked_json.contains('\u{0001}'));

        let mut wide = secret.clone();
        wide.task_id = TaskId::new(Uuid::from_u128(104));
        wide.title = Some("b".repeat(512));
        let capped = plan_warm(wide);
        assert!(!capped.notices[0].body.contains(&"b".repeat(513)));
        assert!(capped.notices[0].body.len() <= 32 + 1 + 512);
        let mut oversized = secret;
        oversized.task_id = TaskId::new(Uuid::from_u128(105));
        oversized.title = Some("c".repeat(513));
        assert!(plan_warm(oversized).notices.is_empty());
    }

    #[test]
    fn attention_fingerprint_ignores_order_epoch_and_persists_below_the_cap() {
        let first = proved(
            TaskId::new(Uuid::from_u128(1)),
            Some(TurnId::new(Uuid::from_u128(2))),
            "open",
            Some(SafeOutcome::NeedsInput),
        );
        let second = proved(
            TaskId::new(Uuid::from_u128(3)),
            Some(TurnId::new(Uuid::from_u128(4))),
            "open",
            Some(SafeOutcome::Blocked),
        );
        let forward = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(11), 1, vec![first.clone(), second.clone()]),
            &NotifyOptions::default(),
            1,
        );
        let backward = plan_notifications(
            &NotifyState::empty(),
            &cold_complete(Uuid::from_u128(99), 8, vec![second, first]),
            &NotifyOptions::default(),
            99_000,
        );
        assert_eq!(
            forward.next.attention_overflow,
            backward.next.attention_overflow
        );
        let fingerprint = forward
            .next
            .attention_overflow
            .clone()
            .expect("fingerprint below the cap");
        let journal = Uuid::from_u128(11).to_string();
        assert!(!fingerprint.contains(&journal));
        assert!(
            forward
                .next
                .decisions
                .iter()
                .all(|decision| !decision.contains(&journal))
        );
        assert!(forward.notices.iter().all(|notice| notice.title != "Done"));
    }

    #[test]
    fn duplicate_notifier_lock_exits_without_the_ssh_target() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("user@secret-host");
        let first = NotifyCache::open(&paths, &controller).expect("first");
        let error = NotifyCache::open(&paths, &controller).expect_err("second");
        let text = error.to_string();
        assert!(text.contains("CONTROLLER_EVENTS_NOTIFY_LOCK_HELD"));
        assert!(!text.contains("secret-host"));
        drop(first);
        NotifyCache::open(&paths, &controller).expect("after release");
    }

    #[test]
    fn symlink_and_unsafe_cache_are_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("user@secret-host");
        let events = paths.controller_cache_root().join("events");
        fs::create_dir_all(&events).expect("events");
        let real = events.join("real");
        fs::create_dir(&real).expect("real");
        std::os::unix::fs::symlink("real", events.join(target_digest(&controller)))
            .expect("symlink");
        let error = NotifyCache::open(&paths, &controller).expect_err("symlink");
        assert!(!error.to_string().contains("secret-host"));

        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let directory = cache_directory(&paths, &controller);
        fs::create_dir_all(&directory).expect("dir");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).expect("mode");
        let error = NotifyCache::open(&paths, &controller).expect_err("mode");
        assert!(
            error
                .to_string()
                .contains("CONTROLLER_EVENTS_NOTIFY_CACHE_UNSAFE")
        );
        assert!(!error.to_string().contains("secret-host"));
    }

    #[test]
    fn corrupt_cache_rebaseline_and_schema_bounds() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("notifier@cache-host");
        let cache = NotifyCache::open(&paths, &controller).expect("open");
        let file = cache_directory(&paths, &controller).join("notify.json");
        fs::write(&file, b"not-json").expect("write");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("mode");
        let error = cache.load().expect_err("corrupt");
        assert!(
            error
                .to_string()
                .contains("CONTROLLER_EVENTS_NOTIFY_CACHE_CORRUPT")
        );
        let fresh = cache.rebaseline().expect("rebaseline");
        assert!(fresh.decisions.is_empty());
        assert!(cache.load().expect("empty").decisions.is_empty());

        let mut decisions = Vec::new();
        for index in 0..4_097 {
            decisions.push(format!("{index:064x}"));
        }
        let oversized = serde_json::json!({
            "schema_version": 1,
            "consumed_after": null,
            "last_complete_repair_millis": null,
            "decisions": decisions,
            "pending": [],
            "attention_overflow": null,
            "repair_needed": false
        });
        fs::write(&file, oversized.to_string()).expect("overwrite");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("mode");
        assert!(
            cache
                .load()
                .expect_err("bounds")
                .to_string()
                .contains("CACHE_CORRUPT")
        );
    }

    #[test]
    fn oversized_cache_rebaselines_and_the_next_load_is_valid() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("notifier@cache-host");
        let cache = NotifyCache::open(&paths, &controller).expect("open");
        let file = cache_directory(&paths, &controller).join("notify.json");
        fs::write(&file, vec![0_u8; MAX_NOTIFY_STATE_BYTES + 1]).expect("write");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("mode");
        assert!(
            cache
                .load()
                .expect_err("oversized")
                .to_string()
                .contains("CONTROLLER_EVENTS_NOTIFY_CACHE_CORRUPT")
        );
        let fresh = cache.rebaseline().expect("rebaseline");
        assert!(fresh.decisions.is_empty());
        assert!(cache.load().expect("valid").decisions.is_empty());
    }

    #[test]
    fn save_precedes_channel_and_channel_failure_does_not_replay() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("notifier@cache-host");
        let cache = NotifyCache::open(&paths, &controller).expect("open");
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(120)),
            TurnId::new(Uuid::from_u128(121)),
        );
        let fingerprint = decision_text(&facts);
        let plan = plan_warm(facts.clone());
        let directory = cache_directory(&paths, &controller);
        let saw = Arc::new(Mutex::new(false));
        struct Observing {
            path: std::path::PathBuf,
            fingerprint: String,
            saw: Arc<Mutex<bool>>,
        }
        impl NoticeChannel for Observing {
            fn deliver(&self, notice: &Notice, _: Duration) -> Result<(), WorkerError> {
                let text = fs::read_to_string(self.path.join("notify.json")).expect("json");
                assert!(text.contains(&self.fingerprint));
                assert!(text.contains(&notice.fingerprint));
                *self.saw.lock().expect("saw") = true;
                Ok(())
            }
        }
        let channel = Arc::new(Observing {
            path: directory.clone(),
            fingerprint: fingerprint.clone(),
            saw: Arc::clone(&saw),
        });
        let channel: Arc<dyn NoticeChannel> = channel;
        commit_then_deliver(
            &cache,
            plan,
            std::slice::from_ref(&channel),
            &ManualEventRuntime::new(),
        )
        .expect("deliver");
        assert!(*saw.lock().expect("saw"));
        let mode = fs::metadata(directory.join("notify.json"))
            .expect("meta")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert_eq!(
            fs::metadata(&directory).expect("dir").permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(directory.join("notify.lock"))
                .expect("lock")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let _ = facts;
    }

    #[test]
    fn channel_failure_and_crash_after_save_do_not_replay() {
        let mut harness = NotifierHarness::new();
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(130)),
            TurnId::new(Uuid::from_u128(131)),
        );
        let plan = plan_notifications(
            &harness.saved,
            &warm_complete(
                harness.journal_id,
                2,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            &NotifyOptions::default(),
            harness.now,
        );
        let failing = Arc::new(RecordingNoticeChannel::new());
        failing.set_error(Some("channel failed".into()));
        let channel: Arc<dyn NoticeChannel> = failing.clone();
        let results = commit_then_deliver(
            harness.cache.as_ref().expect("cache"),
            plan,
            std::slice::from_ref(&channel),
            &ManualEventRuntime::new(),
        )
        .expect("committed");
        assert_eq!(results, vec![false]);
        harness.saved = harness.cache.as_ref().expect("cache").load().expect("load");
        harness.now += 1_000;
        let calls = Arc::new(Mutex::new(0_u32));
        struct Counting(Arc<Mutex<u32>>);
        impl NoticeChannel for Counting {
            fn deliver(&self, _: &Notice, _: Duration) -> Result<(), WorkerError> {
                *self.0.lock().expect("calls") += 1;
                Ok(())
            }
        }
        let counting = Arc::new(Counting(Arc::clone(&calls)));
        let channel: Arc<dyn NoticeChannel> = counting;
        let replay = plan_notifications(
            &harness.saved,
            &warm_complete(
                harness.journal_id,
                3,
                vec![change(facts.clone())],
                vec![facts.clone()],
            ),
            &NotifyOptions::default(),
            harness.now,
        );
        assert!(replay.notices.is_empty());
        commit_then_deliver(
            harness.cache.as_ref().expect("cache"),
            replay,
            std::slice::from_ref(&channel),
            &ManualEventRuntime::new(),
        )
        .expect("second");
        assert_eq!(*calls.lock().expect("calls"), 0);

        let other = done_facts(
            TaskId::new(Uuid::from_u128(132)),
            TurnId::new(Uuid::from_u128(133)),
        );
        harness.interrupt_after_save();
        harness.consume(
            warm_complete(
                harness.journal_id,
                4,
                vec![change(other.clone())],
                vec![other.clone()],
            ),
            NotifyOptions::default(),
        );
        assert!(harness.delivered().is_empty());
        harness.restart_with_valid_cursor();
        harness.consume(
            warm_complete(
                harness.journal_id,
                5,
                vec![change(other.clone())],
                vec![other],
            ),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered_since_restart(), 0);
    }

    #[test]
    fn second_cold_start_with_empty_saved_registry_baselines_without_banners() {
        let mut harness = NotifierHarness::new();
        assert!(harness.saved.decisions.is_empty());
        assert!(harness.saved.consumed_after.is_some());
        let facts = done_facts(
            TaskId::new(Uuid::from_u128(140)),
            TurnId::new(Uuid::from_u128(141)),
        );
        harness.consume(
            cold_complete(harness.journal_id, 2, vec![facts.clone()]),
            NotifyOptions::default(),
        );
        assert!(harness.delivered().is_empty());
        assert_eq!(harness.saved.decisions.len(), 1);
        harness.restart_with_valid_cursor();
        harness.consume(
            cold_complete(harness.journal_id, 3, vec![facts]),
            NotifyOptions::default(),
        );
        assert_eq!(harness.delivered_since_restart(), 0);
    }

    #[test]
    fn target_hash_omits_the_ssh_destination() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let left = controller("user@secret-host");
        let right = controller("user@other-host");
        let left_dir = cache_directory(&paths, &left);
        let right_dir = cache_directory(&paths, &right);
        assert_ne!(left_dir, right_dir);
        assert_eq!(target_digest(&left).len(), 64);
        assert!(!left_dir.display().to_string().contains("secret-host"));
    }

    #[test]
    fn scripted_reconciler_cold_baseline_records_without_a_banner() {
        let facts = TaskFacts::test_terminal(
            TaskId::new(Uuid::from_u128(9)),
            TurnId::new(Uuid::from_u128(10)),
            SafeOutcome::Done,
            true,
        );
        let cold = Reconciliation::test_cold(None, vec![facts]);
        let source = ScriptedEventSource::new();
        let mut reconciler = FakeEventReconciler::new();
        reconciler.queue(Ok(cold)).expect("queue");
        let result = reconciler
            .reconcile(
                &source,
                ReconcileInput {
                    read: None,
                    repair_due: true,
                    include_titles: true,
                },
                Duration::from_secs(1),
            )
            .expect("reconcile");
        let plan = plan_notifications(&NotifyState::empty(), &result, &NotifyOptions::default(), 1);
        assert!(plan.notices.is_empty());
        assert_eq!(plan.next.decisions.len(), 1);
        assert_eq!(reconciler.inputs().len(), 1);
    }

    #[test]
    fn cancelled_runtime_saves_then_skips_the_channel() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = layout(root.path());
        let controller = controller("notifier@cache-host");
        let cache = NotifyCache::open(&paths, &controller).expect("cache");
        let facts = TaskFacts::test_terminal(
            TaskId::new(Uuid::from_u128(11)),
            TurnId::new(Uuid::from_u128(12)),
            SafeOutcome::Done,
            true,
        );
        let plan = plan_warm(facts);
        assert_eq!(plan.notices.len(), 1);
        let runtime = ManualEventRuntime::new();
        runtime.cancel();
        let recorded = Arc::new(RecordingNoticeChannel::new());
        let channel: Arc<dyn NoticeChannel> = recorded.clone();
        let results = commit_then_deliver(&cache, plan, std::slice::from_ref(&channel), &runtime)
            .expect("save");
        assert!(results.is_empty());
        assert!(recorded.records().is_empty());
        assert_eq!(cache.load().expect("load").decisions.len(), 1);
    }
}

mod channels {
    use std::{
        fs,
        io::{BufRead, BufReader, Write},
        os::unix::{fs::FileTypeExt, net::UnixListener, process::ExitStatusExt},
        process::ExitStatus,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use serde_json::Value;

    use mac_worker::test_support::events::notify::{
        ChannelOptions, HerdrChannel, MacosChannel, OSASCRIPT_HANDLER, SelectedChannel,
        UnconfirmedTask, channels_for, eligibility_unknown_diagnostic, herdr_socket_reachable,
        herdr_sound, laptop_notification_socket, notices_for_support, select_channels,
    };
    use mac_worker::test_support::{
        agents::herdr::HerdrSocket,
        core::{config::NotificationsConfig, error::WorkerError},
        events::contracts::{
            EventSupport, Notice, NoticeChannel, NoticeSound, NotifyChannel, NotifyOptions,
            SafeOutcome,
        },
        host::process::{ProcessRequest, ProcessResult, ProcessRunner},
        task::model::TaskId,
    };
    use uuid::Uuid;

    struct RecordingRunner {
        requests: Mutex<Vec<ProcessRequest>>,
    }

    impl ProcessRunner for RecordingRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    fn notice(sound: NoticeSound) -> Notice {
        Notice {
            fingerprint: "turn:abc".into(),
            title: "Done".into(),
            body: "body".into(),
            sound,
        }
    }

    #[test]
    fn macos_uses_a_fixed_argv_handler_and_a_two_second_budget() {
        let runner = Arc::new(RecordingRunner {
            requests: Mutex::new(Vec::new()),
        });
        let channel = MacosChannel::new(
            runner.clone(),
            ChannelOptions {
                deadline: Duration::from_secs(2),
            },
        );
        let title = "$(rm -rf /) \" ; do shell script \"echo pwned";
        let body = "item 1 of argv\nsecond";
        channel
            .deliver(
                &Notice {
                    fingerprint: "fp".into(),
                    title: title.into(),
                    body: body.into(),
                    sound: NoticeSound::Done,
                },
                Duration::from_secs(2),
            )
            .expect("deliver");
        let requests = runner.requests.lock().expect("requests");
        let request = &requests[0];
        assert_eq!(request.program, "/usr/bin/osascript");
        assert_eq!(request.policy.deadline, Duration::from_secs(2));
        assert_eq!(request.args[0], "-e");
        assert_eq!(request.args[1], OSASCRIPT_HANDLER);
        assert!(OSASCRIPT_HANDLER.contains("on run argv"));
        assert!(
            OSASCRIPT_HANDLER
                .contains("display notification (item 2 of argv) with title (item 1 of argv)")
        );
        assert!(!request.args[1].to_string_lossy().contains("rm -rf"));
        assert_eq!(request.args[2], "--");
        assert_eq!(
            request.args[3],
            "$(rm -rf /) \" ; do shell script \"echo pwned"
        );
        assert_eq!(request.args[4], "item 1 of argv\nsecond");
        assert!(request.args.iter().all(|arg| arg != "sh" && arg != "-c"));
        assert!(request.stdin.is_none());
    }

    #[test]
    fn herdr_maps_sounds_and_does_not_use_the_controller_socket() {
        let root = tempfile::tempdir().expect("tempdir");
        let socket_path = root.path().join("laptop.sock");
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let listener = UnixListener::bind(&socket_path).expect("bind");
        let recorded_thread = Arc::clone(&recorded);
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut line = String::new();
            reader.read_line(&mut line).expect("read");
            let value: Value = serde_json::from_str(line.trim()).expect("json");
            recorded_thread.lock().expect("record").push(value.clone());
            let reply = serde_json::json!({"id": value["id"], "result": {}});
            let mut stream = stream;
            writeln!(stream, "{reply}").expect("reply");
        });
        let socket = HerdrSocket::at(&socket_path);
        let channel = HerdrChannel::new(socket, ChannelOptions::default());
        channel
            .deliver(&notice(NoticeSound::Request), Duration::from_secs(2))
            .expect("show");
        let requests = recorded.lock().expect("requests");
        assert_eq!(requests[0]["method"], "notification.show");
        assert_eq!(requests[0]["params"]["sound"], "request");
        assert_eq!(requests[0]["params"]["title"], "Done");
        assert_eq!(requests[0]["params"]["body"], "body");

        let laptop = tempfile::tempdir().expect("laptop");
        let controller = tempfile::tempdir().expect("controller");
        let env_socket = laptop.path().join("env.sock");
        let discovered = laptop_notification_socket(controller.path(), |key| {
            (key == "HERDR_SOCKET_PATH").then(|| std::ffi::OsString::from(&env_socket))
        });
        assert_eq!(discovered.path(), env_socket.as_path());
        assert_ne!(
            discovered.path(),
            HerdrSocket::default_for_home(controller.path()).path()
        );
        assert!(!herdr_socket_reachable(&HerdrSocket::default_for_home(
            controller.path()
        )));
    }

    #[test]
    fn auto_selects_macos_when_a_bound_herdr_socket_is_closed_without_unlinking() {
        let root = tempfile::tempdir().expect("tempdir");
        let path = root.path().join("laptop.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let socket = HerdrSocket::at(&path);
        assert!(herdr_socket_reachable(&socket));
        drop(listener);
        assert!(
            fs::symlink_metadata(&path)
                .expect("socket name")
                .file_type()
                .is_socket()
        );
        assert!(!herdr_socket_reachable(&socket));
        let selected = select_channels(
            &NotifyOptions::default(),
            &NotificationsConfig { herdr: true },
            herdr_socket_reachable(&socket),
        );
        assert_eq!(selected.0, vec![SelectedChannel::Macos]);
        assert!(selected.1.is_none());
    }

    #[test]
    fn auto_both_quiet_and_explicit_herdr_failure() {
        let enabled = NotificationsConfig { herdr: true };
        let disabled = NotificationsConfig { herdr: false };
        let auto = NotifyOptions::default();
        assert_eq!(
            select_channels(&auto, &enabled, true).0,
            vec![SelectedChannel::Herdr]
        );
        assert_eq!(
            select_channels(&auto, &enabled, false).0,
            vec![SelectedChannel::Macos]
        );
        assert_eq!(
            select_channels(&auto, &disabled, true).0,
            vec![SelectedChannel::Macos]
        );
        let both = NotifyOptions {
            channel: NotifyChannel::Both,
            ..NotifyOptions::default()
        };
        assert_eq!(
            select_channels(&both, &enabled, true).0,
            vec![SelectedChannel::Herdr, SelectedChannel::Macos]
        );
        let quiet = NotifyOptions {
            quiet: true,
            channel: NotifyChannel::Both,
            ..NotifyOptions::default()
        };
        assert!(select_channels(&quiet, &enabled, true).0.is_empty());
        let explicit = NotifyOptions {
            channel: NotifyChannel::Herdr,
            ..NotifyOptions::default()
        };
        let (channels, diagnostic) = select_channels(&explicit, &enabled, false);
        assert_eq!(channels, vec![SelectedChannel::Herdr]);
        assert_eq!(
            diagnostic,
            Some("herdr notification channel is unavailable")
        );

        let missing = tempfile::tempdir().expect("missing");
        let path = missing.path().join("controller-secret-socket");
        let error = HerdrChannel::new(HerdrSocket::at(&path), ChannelOptions::default())
            .deliver(&notice(NoticeSound::None), Duration::from_secs(2))
            .expect_err("absent");
        let text = error.to_string();
        assert!(text.contains("herdr absent"));
        assert!(!text.contains("controller-secret-socket"));
        assert!(!fs::metadata(&path).is_ok());
    }

    #[test]
    fn both_attempts_each_channel_once_and_quiet_calls_neither() {
        let runner = Arc::new(RecordingRunner {
            requests: Mutex::new(Vec::new()),
        });
        let root = tempfile::tempdir().expect("tempdir");
        let socket_path = root.path().join("both.sock");
        let hits = Arc::new(Mutex::new(0_u32));
        let listener = UnixListener::bind(&socket_path).expect("bind");
        let hits_thread = Arc::clone(&hits);
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut line = String::new();
            reader.read_line(&mut line).expect("read");
            let value: Value = serde_json::from_str(line.trim()).expect("json");
            *hits_thread.lock().expect("hits") += 1;
            assert_eq!(value["params"]["sound"], "done");
            let reply = serde_json::json!({"id": value["id"], "result": {}});
            let mut stream = stream;
            writeln!(stream, "{reply}").expect("reply");
        });
        let options = NotifyOptions {
            channel: NotifyChannel::Both,
            ..NotifyOptions::default()
        };
        let channels = channels_for(
            &options,
            &NotificationsConfig { herdr: true },
            HerdrSocket::at(&socket_path),
            true,
            runner.clone(),
        );
        assert_eq!(channels.len(), 2);
        let notice = Notice {
            fingerprint: "fp".into(),
            title: "Done".into(),
            body: "1 task".into(),
            sound: NoticeSound::Done,
        };
        for channel in &channels {
            channel
                .deliver(&notice, Duration::from_secs(2))
                .expect("attempt");
        }
        assert_eq!(runner.requests.lock().expect("argv").len(), 1);
        assert_eq!(*hits.lock().expect("hits"), 1);

        let quiet = channels_for(
            &NotifyOptions {
                quiet: true,
                channel: NotifyChannel::Both,
                ..NotifyOptions::default()
            },
            &NotificationsConfig { herdr: true },
            HerdrSocket::at(&socket_path),
            true,
            runner.clone(),
        );
        assert!(quiet.is_empty());
    }

    #[test]
    fn unsupported_controller_says_eligibility_unknown_and_raises_no_banners() {
        let task_id = TaskId::new(Uuid::from_u128(77));
        let text = eligibility_unknown_diagnostic(&[UnconfirmedTask {
            task_id,
            outcome: Some(SafeOutcome::NeedsInput),
        }]);
        assert!(text.contains("eligibility unknown"));
        assert!(text.contains(&task_id.to_string()));
        assert!(text.contains("needs_input"));
        assert!(!text.contains("task.wait.poll"));
        assert!(notices_for_support(EventSupport::Unsupported).is_empty());
        assert!(notices_for_support(EventSupport::Supported).is_empty());
    }

    #[test]
    fn herdr_sound_map_covers_done_request_and_none() {
        assert_eq!(herdr_sound(NoticeSound::Done).as_str(), "done");
        assert_eq!(herdr_sound(NoticeSound::Request).as_str(), "request");
        assert_eq!(herdr_sound(NoticeSound::None).as_str(), "none");
    }
}
