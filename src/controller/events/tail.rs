//! Safe debugging of future committed events; no task projection or local state.

use super::notify::follow::{millis, operation_deadline, pause, stopped};
use super::{
    CONTROLLER_EVENTS_UNSUPPORTED, EventReadResult, EventRuntime, EventSource, EventSupport,
    JOURNAL_CHECK_INTERVAL, JournalWindow, READ_FOLLOW_WAIT_MS, ReadQuery, ViewerMessage,
};
use crate::error::WorkerError;
use std::{io::Write, sync::Arc, time::Duration};

pub struct TailLoop<'a> {
    pub source: &'a dyn EventSource,
    pub runtime: Arc<dyn EventRuntime>,
    pub json: bool,
    pub stop_at: Option<Duration>,
}

impl TailLoop<'_> {
    pub fn run(self, output: &mut dyn Write) -> Result<(), WorkerError> {
        if stopped(self.runtime.as_ref(), self.stop_at) {
            return Ok(());
        }
        let discovery = self
            .source
            .discover(operation_deadline(self.runtime.as_ref(), self.stop_at));
        if stopped(self.runtime.as_ref(), self.stop_at) {
            return Ok(());
        }
        if discovery? == EventSupport::Unsupported {
            return Err(WorkerError::Unavailable(format!(
                "{CONTROLLER_EVENTS_UNSUPPORTED}: controller has no event feed"
            )));
        }
        let mut after = None;
        loop {
            if stopped(self.runtime.as_ref(), self.stop_at) {
                return Ok(());
            }
            let deadline = operation_deadline(self.runtime.as_ref(), self.stop_at);
            let query = ReadQuery {
                after,
                wait_ms: if after.is_some() {
                    millis(
                        deadline
                            .saturating_sub(self.runtime.now())
                            .saturating_sub(JOURNAL_CHECK_INTERVAL),
                    )
                    .min(READ_FOLLOW_WAIT_MS)
                } else {
                    0
                },
                ..ReadQuery::default()
            };
            let result = self.source.read(query.clone(), deadline);
            if stopped(self.runtime.as_ref(), self.stop_at) {
                return Ok(());
            }
            let result = result?;
            result.validate()?;
            match result {
                EventReadResult::SnapshotRequired(control) => {
                    if after.is_some() {
                        write_control(
                            output,
                            self.json,
                            ViewerMessage::SnapshotRequired(control.clone()),
                        )?;
                    }
                    after = Some(control.window.cursor());
                    write_control(output, self.json, ViewerMessage::Ready(control.window))?;
                }
                EventReadResult::Batch(batch) => {
                    let cursor = after.ok_or_else(|| WorkerError::Protocol("CONTROLLER_EVENTS_INVALID: initial tail read did not return a journal window".into()))?;
                    if cursor.journal_id != batch.journal_id
                        || batch
                            .events
                            .first()
                            .is_some_and(|event| cursor.seq.checked_increment() != Some(event.seq))
                        || (batch.events.is_empty() && batch.next_after != cursor)
                        || batch.events.len() > query.limit
                    {
                        return Err(WorkerError::Protocol(
                            "CONTROLLER_EVENTS_INVALID: tail batch does not follow its cursor"
                                .into(),
                        ));
                    }
                    for event in &batch.events {
                        if stopped(self.runtime.as_ref(), self.stop_at) {
                            return Ok(());
                        }
                        let safe = event.debug_value()?;
                        if self.json {
                            write_json(output, &safe)?;
                        } else {
                            write!(output, "{}:{} {}", event.journal_id, event.seq, event.kind)?;
                            if let Some(data) = safe.get("data") {
                                write!(output, " {data}")?;
                            }
                            writeln!(output)?;
                            output.flush()?;
                        }
                    }
                    after = Some(batch.next_after);
                    if batch.events.is_empty() {
                        pause(self.runtime.as_ref(), JOURNAL_CHECK_INTERVAL, self.stop_at);
                    }
                }
            }
        }
    }
}

fn write_json(output: &mut dyn Write, value: &serde_json::Value) -> Result<(), WorkerError> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| WorkerError::Io(std::io::Error::other(error)))?;
    output.write_all(&bytes)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
fn write_control(
    output: &mut dyn Write,
    json: bool,
    message: ViewerMessage,
) -> Result<(), WorkerError> {
    if json {
        return write_json(
            output,
            &serde_json::to_value(message)
                .map_err(|error| WorkerError::Io(std::io::Error::other(error)))?,
        );
    }
    match message {
        ViewerMessage::Ready(JournalWindow {
            journal_id,
            oldest_seq,
            head_seq,
        }) => writeln!(
            output,
            "ready journal_id={journal_id} oldest_seq={oldest_seq} head_seq={head_seq}"
        )?,
        ViewerMessage::SnapshotRequired(control) => {
            writeln!(output, "snapshot_required reason={}", control.reason)?
        }
        _ => unreachable!("tail only emits journal controls"),
    }
    output.flush()?;
    Ok(())
}

pub(crate) fn run_command(
    cli: &crate::cli::Cli,
    context: &crate::RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let result = (|| {
        let (paths, config) = super::foreground::configuration(cli, context)?;
        let runtime = super::foreground::ForegroundRuntime::install()?;
        let source = super::foreground::event_client(
            Arc::new(crate::process::SystemProcessRunner),
            crate::controller::channel::ReadLoopScope::EventsFollow,
            &paths,
            &config,
            runtime.clone(),
            context,
        );
        TailLoop {
            source: &source,
            runtime,
            json: cli.json,
            stop_at: None,
        }
        .run(stdout)
    })();
    match result {
        Ok(()) => 0,
        Err(error) => super::foreground::report(error, stderr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::events::{
        EventCursor, EventReadResult, EventSelector, EventSupport, JournalWindow, ReadBatch,
        SCHEMA_VERSION, Seq, SnapshotRequired, WireEvent,
        testing::{ManualEventRuntime, ScriptedEventSource},
    };

    fn window(head: u64) -> JournalWindow {
        JournalWindow {
            journal_id: uuid::Uuid::from_u128(1),
            oldest_seq: Seq::new(1),
            head_seq: Seq::new(head),
        }
    }
    fn control(reason: &str, head: u64) -> EventReadResult {
        EventReadResult::SnapshotRequired(SnapshotRequired {
            reason: reason.into(),
            window: window(head),
        })
    }
    fn event(seq: u64, kind: &str, data: serde_json::Value) -> WireEvent {
        WireEvent {
            schema_version: 1,
            journal_id: window(0).journal_id,
            seq: Seq::new(seq),
            time_millis: 42,
            kind: kind.into(),
            data,
        }
    }
    fn batch(head: u64, next: u64, events: Vec<WireEvent>) -> EventReadResult {
        EventReadResult::Batch(ReadBatch {
            schema_version: SCHEMA_VERSION,
            journal_id: window(0).journal_id,
            oldest_seq: Seq::new(1),
            head_seq: Seq::new(head),
            next_after: EventCursor {
                journal_id: window(0).journal_id,
                seq: Seq::new(next),
            },
            events,
            has_more: next < head,
        })
    }
    struct Output {
        bytes: Vec<u8>,
        clock: Arc<ManualEventRuntime>,
        cancel_after: usize,
    }
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.clock.advance(Duration::from_secs(1));
            if self.bytes.iter().filter(|byte| **byte == b'\n').count() >= self.cancel_after {
                self.clock.cancel();
            }
            Ok(())
        }
    }
    fn output(clock: Arc<ManualEventRuntime>, cancel_after: usize) -> Output {
        Output {
            bytes: Vec::new(),
            clock,
            cancel_after,
        }
    }

    #[test]
    fn captures_current_head_and_follows_only_delivered_future_records() {
        let source = ScriptedEventSource::new();
        source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
        source.queue_read(Ok(control("bootstrap", 5))).unwrap();
        for seq in [6, 7] {
            source
                .queue_read(Ok(batch(
                    7,
                    seq,
                    vec![event(
                        seq,
                        "worker.changed",
                        serde_json::json!({"worker":"mini_1","ready":true,"observed_at_millis":42,"code":null,"private":"secret /home/private"}),
                    )],
                )))
                .unwrap();
        }
        let clock = Arc::new(ManualEventRuntime::new());
        let mut out = output(clock.clone(), 3);
        TailLoop {
            source: &source,
            runtime: clock,
            json: true,
            stop_at: None,
        }
        .run(&mut out)
        .unwrap();
        let text = String::from_utf8(out.bytes).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines[0]["event"], "ready");
        assert_eq!(lines[0]["data"]["head_seq"], "5");
        assert_eq!(lines[1]["seq"], "6");
        assert_eq!(lines[2]["seq"], "7");
        assert!(!text.contains("private"));
        let requests = source.requests();
        let after: Vec<_> = requests
            .iter()
            .map(|request| match &request.selector {
                EventSelector::Read(query) => query.after.map(|cursor| cursor.seq.as_u64()),
                _ => panic!("tail performed a state read"),
            })
            .collect();
        assert_eq!(after, vec![None, Some(5), Some(6)]);
    }

    #[test]
    fn unknown_kinds_and_versions_emit_metadata_without_opaque_data() {
        for json in [false, true] {
            let source = ScriptedEventSource::new();
            source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
            source.queue_read(Ok(control("bootstrap", 0))).unwrap();
            let unknown = event(
                1,
                "future.kind",
                serde_json::json!({"task_id":"private-task", "prompt":"token secret /home/path"}),
            );
            let mut future_version = event(
                2,
                "task.changed",
                serde_json::json!({"anything":"private-prose"}),
            );
            future_version.schema_version = 2;
            source
                .queue_read(Ok(batch(2, 2, vec![unknown, future_version])))
                .unwrap();
            let clock = Arc::new(ManualEventRuntime::new());
            let mut out = output(clock.clone(), 3);
            TailLoop {
                source: &source,
                runtime: clock,
                json,
                stop_at: None,
            }
            .run(&mut out)
            .unwrap();
            let text = String::from_utf8(out.bytes).unwrap();
            assert!(text.contains("future.kind"));
            assert!(!text.contains("private"));
            assert!(!text.contains("token"));
            assert!(!text.contains("prompt"));
            if json {
                for line in text.lines().skip(1) {
                    let value: serde_json::Value = serde_json::from_str(line).unwrap();
                    assert!(value.get("data").is_none());
                }
            }
        }
    }

    #[test]
    fn reset_and_expiry_print_controls_and_rebaseline_at_the_new_head() {
        for reason in ["journal_changed", "cursor_expired", "cursor_ahead"] {
            let source = ScriptedEventSource::new();
            source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
            source.queue_read(Ok(control("bootstrap", 5))).unwrap();
            source.queue_read(Ok(control(reason, 20))).unwrap();
            source
                .queue_read(Ok(batch(
                    21,
                    21,
                    vec![event(21, "future.kind", serde_json::json!({}))],
                )))
                .unwrap();
            let clock = Arc::new(ManualEventRuntime::new());
            let mut out = output(clock.clone(), 4);
            TailLoop {
                source: &source,
                runtime: clock,
                json: true,
                stop_at: None,
            }
            .run(&mut out)
            .unwrap();
            let lines: Vec<serde_json::Value> = String::from_utf8(out.bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(lines[1]["event"], "snapshot_required");
            assert_eq!(lines[1]["data"]["reason"], reason);
            assert_eq!(lines[2]["event"], "ready");
            assert_eq!(lines[2]["data"]["head_seq"], "20");
            assert!(
                matches!(&source.requests()[2].selector, EventSelector::Read(query) if query.after == Some(window(20).cursor()))
            );
        }
    }

    #[test]
    fn unsupported_discovery_or_rollback_exits_without_emulated_state_reads() {
        for rollback in [false, true] {
            let source = ScriptedEventSource::new();
            source
                .queue_discovery(Ok(if rollback {
                    EventSupport::Supported
                } else {
                    EventSupport::Unsupported
                }))
                .unwrap();
            if rollback {
                source
                    .queue_read(Err(WorkerError::Unavailable(
                        "CONTROLLER_EVENTS_UNSUPPORTED: legacy selector rejection".into(),
                    )))
                    .unwrap();
            }
            let error = TailLoop {
                source: &source,
                runtime: Arc::new(ManualEventRuntime::new()),
                json: true,
                stop_at: None,
            }
            .run(&mut Vec::new())
            .unwrap_err();
            assert!(error.to_string().contains("CONTROLLER_EVENTS_UNSUPPORTED"));
            assert!(
                source
                    .requests()
                    .iter()
                    .all(|request| matches!(request.selector, EventSelector::Read(_)))
            );
        }
    }

    #[test]
    fn cancellation_prevents_admission_and_each_poll_is_deadline_bounded() {
        let source = ScriptedEventSource::new();
        let clock = Arc::new(ManualEventRuntime::new());
        clock.cancel();
        let mut out = Vec::new();
        TailLoop {
            source: &source,
            runtime: clock,
            json: true,
            stop_at: None,
        }
        .run(&mut out)
        .unwrap();
        assert!(out.is_empty());
        assert!(source.discovery_deadlines().is_empty());
        source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
        source.queue_read(Ok(control("bootstrap", 5))).unwrap();
        source.queue_read(Ok(batch(5, 5, Vec::new()))).unwrap();
        let clock = Arc::new(ManualEventRuntime::new());
        let cancel = clock.clone();
        clock.on_sleep(move |_| cancel.cancel());
        let mut out = output(clock.clone(), 100);
        TailLoop {
            source: &source,
            runtime: clock,
            json: false,
            stop_at: Some(Duration::from_secs(3)),
        }
        .run(&mut out)
        .unwrap();
        let requests = source.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            matches!(&requests[1].selector, EventSelector::Read(query) if query.wait_ms > 0 && query.wait_ms < 2_000)
        );
        assert_eq!(requests[1].deadline, Duration::from_secs(3));
    }

    #[test]
    fn malformed_batch_cannot_print_private_data_or_advance_the_cursor() {
        let source = ScriptedEventSource::new();
        source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
        source.queue_read(Ok(control("bootstrap", 5))).unwrap();
        source
            .queue_read(Ok(batch(
                6,
                6,
                vec![event(
                    6,
                    "turn.finished",
                    serde_json::json!({"prompt":"private-secret"}),
                )],
            )))
            .unwrap();
        let clock = Arc::new(ManualEventRuntime::new());
        let mut out = output(clock.clone(), 100);
        assert!(
            TailLoop {
                source: &source,
                runtime: clock,
                json: true,
                stop_at: None
            }
            .run(&mut out)
            .is_err()
        );
        let text = String::from_utf8(out.bytes).unwrap();
        assert_eq!(text.lines().count(), 1);
        assert!(!text.contains("private-secret"));
    }

    #[test]
    fn initial_batch_is_rejected_instead_of_inventing_a_snapshot() {
        let source = ScriptedEventSource::new();
        source.queue_discovery(Ok(EventSupport::Supported)).unwrap();
        source.queue_read(Ok(batch(5, 5, Vec::new()))).unwrap();
        let mut out = Vec::new();
        assert!(
            TailLoop {
                source: &source,
                runtime: Arc::new(ManualEventRuntime::new()),
                json: true,
                stop_at: None
            }
            .run(&mut out)
            .is_err()
        );
        assert!(out.is_empty());
    }
}
