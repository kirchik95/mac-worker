//! Local journal replay, bounded live fan-out and viewer controls.
use crate::{
    controller::events::{
        CONTROLLER_EVENTS_UNAVAILABLE as UNAVAILABLE, EventCursor, EventReadResult, EventRuntime,
        JOURNAL_CHECK_INTERVAL as CHECK, JournalReader, JournalWindow, LocalProjectionRefresh,
        ReadQuery, SSE_CAPACITY as CAPACITY, SSE_HEARTBEAT_INTERVAL as HEARTBEAT,
        SSE_MAX_STREAMS as MAX_STREAMS, Seq, SnapshotRequired, ViewerEventSource, ViewerMessage,
    },
    error::WorkerError,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::{Semaphore, broadcast, mpsc, watch};

pub struct LocalViewerEventSource {
    reader: Arc<dyn JournalReader>,
    runtime: Arc<dyn EventRuntime>,
    live: broadcast::Sender<ViewerMessage>,
    stopped: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    streams: Arc<Semaphore>,
}

impl LocalViewerEventSource {
    pub fn new(
        reader: Arc<dyn JournalReader>,
        runtime: Arc<dyn EventRuntime>,
        refresh: Arc<dyn LocalProjectionRefresh>,
    ) -> Arc<Self> {
        let (live, _) = broadcast::channel(CAPACITY);
        let (shutdown, _) = watch::channel(false);
        let stopped = Arc::new(AtomicBool::new(false));
        // The tail baseline exists before any subscriber can capture its H.
        // Starting the tail at a later head could lose post-H events.
        let after = reader
            .window(deadline(runtime.as_ref()))
            .ok()
            .map(|window| EventCursor {
                journal_id: window.journal_id,
                seq: window.head_seq,
            });
        let source = Arc::new(Self {
            reader: Arc::clone(&reader),
            runtime: Arc::clone(&runtime),
            live: live.clone(),
            stopped: Arc::clone(&stopped),
            shutdown: shutdown.clone(),
            streams: Arc::new(Semaphore::new(MAX_STREAMS)),
        });
        let publications = refresh.subscribe_publications();
        let spawn = thread::Builder::new()
            .name("dashboard-event-tailer".into())
            .spawn(move || {
                let mut tail = Tailer {
                    reader,
                    runtime,
                    refresh,
                    live,
                    after,
                    publications,
                    next_heartbeat: Duration::ZERO,
                };
                tail.next_heartbeat = tail.runtime.now().saturating_add(HEARTBEAT);
                while !stopped.load(Ordering::SeqCst) && !tail.runtime.cancelled() {
                    tail.runtime.sleep(CHECK);
                    if stopped.load(Ordering::SeqCst) || tail.runtime.cancelled() {
                        break;
                    }
                    tail.check();
                }
                shutdown.send_replace(true);
            });
        if spawn.is_err() {
            source.stop();
        }
        source
    }
}

impl ViewerEventSource for LocalViewerEventSource {
    fn subscribe(
        &self,
        after: Option<EventCursor>,
    ) -> Result<mpsc::Receiver<ViewerMessage>, WorkerError> {
        if self.stopped.load(Ordering::SeqCst)
            || *self.shutdown.borrow()
            || self.runtime.cancelled()
        {
            return Err(unavailable());
        }
        let permit = Arc::clone(&self.streams).try_acquire_owned().map_err(|_| {
            WorkerError::Unavailable(
                "CONTROLLER_EVENTS_STREAM_LIMIT: viewer stream limit reached".into(),
            )
        })?;
        let handle = tokio::runtime::Handle::try_current().map_err(|_| unavailable())?;
        // The live receiver is registered BEFORE the replay window is read.
        let live = self.live.subscribe();
        let (sender, receiver) = mpsc::channel(CAPACITY);
        let reader = Arc::clone(&self.reader);
        let runtime = Arc::clone(&self.runtime);
        let mut shutdown = self.shutdown.subscribe();
        handle.spawn(async move {
            let _permit = permit;
            let disconnect = sender.clone();
            tokio::select! {
                biased;
                _ = cancelled(&mut shutdown) => {},
                _ = disconnect.closed() => {},
                _ = replay_and_follow(reader, runtime, after, live, sender) => {},
            }
        });
        Ok(receiver)
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.shutdown.send_replace(true);
    }
}

impl Drop for LocalViewerEventSource {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn cancelled(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

fn unavailable() -> WorkerError {
    WorkerError::Unavailable(UNAVAILABLE.into())
}
fn deadline(runtime: &dyn EventRuntime) -> Duration {
    runtime.now().saturating_add(CHECK)
}

struct Tailer {
    reader: Arc<dyn JournalReader>,
    runtime: Arc<dyn EventRuntime>,
    refresh: Arc<dyn LocalProjectionRefresh>,
    live: broadcast::Sender<ViewerMessage>,
    after: Option<EventCursor>,
    publications: broadcast::Receiver<u64>,
    next_heartbeat: Duration,
}
impl Tailer {
    fn check(&mut self) {
        if let Some(after) = self.after {
            match self.reader.read(
                ReadQuery {
                    after: Some(after),
                    limit: CAPACITY,
                    wait_ms: 0,
                },
                deadline(self.runtime.as_ref()),
            ) {
                Ok(EventReadResult::Batch(batch)) => {
                    if !batch.events.is_empty() {
                        self.refresh.request_refresh();
                    }
                    for event in batch.events {
                        let _ = self.live.send(ViewerMessage::ControllerEvent(event));
                    }
                    self.after = Some(batch.next_after);
                }
                Ok(EventReadResult::SnapshotRequired(repair)) => {
                    self.after = Some(EventCursor {
                        journal_id: repair.window.journal_id,
                        seq: repair.window.head_seq,
                    });
                    let _ = self
                        .live
                        .send(ViewerMessage::SnapshotRequired(repair.clone()));
                    let _ = self.live.send(ViewerMessage::Ready(repair.window));
                    self.refresh.request_refresh();
                }
                Err(_) => {
                    let _ = self.live.send(ViewerMessage::Unavailable {
                        code: UNAVAILABLE.into(),
                    });
                }
            }
        } else {
            match self.reader.window(deadline(self.runtime.as_ref())) {
                Ok(window) => {
                    self.after = Some(EventCursor {
                        journal_id: window.journal_id,
                        seq: window.head_seq,
                    });
                    // The first successful tail window may be newer than a
                    // subscriber's captured H. Explicitly repair that gap.
                    let _ = self
                        .live
                        .send(ViewerMessage::SnapshotRequired(SnapshotRequired {
                            reason: "bootstrap".into(),
                            window: window.clone(),
                        }));
                    let _ = self.live.send(ViewerMessage::Ready(window));
                    self.refresh.request_refresh();
                }
                Err(_) => {
                    let _ = self.live.send(ViewerMessage::Unavailable {
                        code: UNAVAILABLE.into(),
                    });
                }
            }
        }
        loop {
            match self.publications.try_recv() {
                Ok(revision) => {
                    let _ = self.live.send(ViewerMessage::SnapshotReady { revision });
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
        let now = self.runtime.now();
        if now >= self.next_heartbeat {
            let _ = self.live.send(ViewerMessage::Heartbeat);
            self.next_heartbeat = now.saturating_add(HEARTBEAT);
        }
    }
}

async fn replay_and_follow(
    reader: Arc<dyn JournalReader>,
    runtime: Arc<dyn EventRuntime>,
    requested: Option<EventCursor>,
    mut live: broadcast::Receiver<ViewerMessage>,
    sender: mpsc::Sender<ViewerMessage>,
) {
    let window_reader = Arc::clone(&reader);
    let read_deadline = deadline(runtime.as_ref());
    let Ok(Ok(mut window)) =
        tokio::task::spawn_blocking(move || window_reader.window(read_deadline)).await
    else {
        let _ = sender.try_send(ViewerMessage::Unavailable {
            code: UNAVAILABLE.into(),
        });
        return;
    };
    let reason = repair_reason(requested.as_ref(), &window);
    let mut cursor = if let Some(reason) = reason {
        if !send(
            &sender,
            ViewerMessage::SnapshotRequired(SnapshotRequired {
                reason: reason.into(),
                window: window.clone(),
            }),
            &window,
        ) {
            return;
        }
        EventCursor {
            journal_id: window.journal_id,
            seq: window.head_seq,
        }
    } else {
        requested.expect("valid requested cursor")
    };
    if !send(&sender, ViewerMessage::Ready(window.clone()), &window) {
        return;
    }
    let head = window.head_seq;
    while cursor.seq < head {
        let query = ReadQuery {
            after: Some(cursor),
            limit: CAPACITY,
            wait_ms: 0,
        };
        let replay_reader = Arc::clone(&reader);
        let read_deadline = deadline(runtime.as_ref());
        let Ok(Ok(result)) =
            tokio::task::spawn_blocking(move || replay_reader.read(query, read_deadline)).await
        else {
            let _ = sender.try_send(ViewerMessage::Unavailable {
                code: UNAVAILABLE.into(),
            });
            return;
        };
        match result {
            EventReadResult::SnapshotRequired(repair) => {
                if !send(
                    &sender,
                    ViewerMessage::SnapshotRequired(repair.clone()),
                    &repair.window,
                ) {
                    return;
                }
                let _ = send(&sender, ViewerMessage::Ready(repair.window), &window);
                return;
            }
            EventReadResult::Batch(batch) => {
                let before = cursor.seq;
                for event in batch.events {
                    if event.journal_id != cursor.journal_id {
                        let _ = sender.try_send(ViewerMessage::Unavailable {
                            code: UNAVAILABLE.into(),
                        });
                        return;
                    }
                    if event.seq <= cursor.seq || event.seq > head {
                        continue;
                    }
                    cursor.seq = event.seq;
                    if !send(&sender, ViewerMessage::ControllerEvent(event), &window) {
                        return;
                    }
                }
                if cursor.seq == before {
                    let _ = sender.try_send(ViewerMessage::Unavailable {
                        code: UNAVAILABLE.into(),
                    });
                    return;
                }
            }
        }
    }
    loop {
        let message = match live.recv().await {
            Ok(message) => message,
            Err(broadcast::error::RecvError::Lagged(_)) => {
                repair_slow(&sender, &window);
                return;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        match &message {
            ViewerMessage::ControllerEvent(event) => {
                if event.journal_id != cursor.journal_id {
                    repair_slow(&sender, &window);
                    return;
                }
                if event.seq <= cursor.seq {
                    continue;
                }
                cursor.seq = event.seq;
                window.head_seq = event.seq;
            }
            ViewerMessage::SnapshotRequired(repair) => {
                cursor = EventCursor {
                    journal_id: repair.window.journal_id,
                    seq: repair.window.head_seq,
                };
                window = repair.window.clone();
            }
            ViewerMessage::Ready(current) => {
                if current.journal_id != cursor.journal_id {
                    cursor = EventCursor {
                        journal_id: current.journal_id,
                        seq: current.head_seq,
                    };
                }
                window = current.clone();
            }
            _ => {}
        }
        let close = matches!(message, ViewerMessage::Unavailable { .. });
        if !send(&sender, message, &window) || close {
            return;
        }
    }
}

fn repair_reason(cursor: Option<&EventCursor>, window: &JournalWindow) -> Option<&'static str> {
    let Some(cursor) = cursor else {
        return Some("bootstrap");
    };
    if cursor.journal_id != window.journal_id {
        Some("journal_changed")
    } else if cursor.seq.as_u64() < window.oldest_seq.as_u64().saturating_sub(1) {
        Some("cursor_expired")
    } else if cursor.seq > window.head_seq {
        Some("cursor_ahead")
    } else {
        None
    }
}

fn repair_slow(sender: &mpsc::Sender<ViewerMessage>, window: &JournalWindow) {
    let _ = sender.try_send(ViewerMessage::SnapshotRequired(SnapshotRequired {
        reason: "lagged".into(),
        window: window.clone(),
    }));
}
fn send(
    sender: &mpsc::Sender<ViewerMessage>,
    message: ViewerMessage,
    window: &JournalWindow,
) -> bool {
    // Reserve the final queue slot for an explicit repair before disconnecting.
    if sender.capacity() <= 1 {
        repair_slow(sender, window);
        return false;
    }
    sender.try_send(message).is_ok()
}

pub(crate) fn resolve_cursor(
    query: Option<&str>,
    last_event_id: Option<&str>,
) -> Result<Option<EventCursor>, WorkerError> {
    let mut after = None;
    if let Some(query) = query {
        if query.len() > 256 {
            return Err(invalid_cursor());
        }
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if key != "after" || after.is_some() {
                return Err(invalid_cursor());
            }
            after = Some(parse_cursor(&value)?);
        }
    }
    let last = last_event_id.map(parse_cursor).transpose()?;
    if after
        .as_ref()
        .zip(last.as_ref())
        .is_some_and(|(a, b)| a != b)
    {
        return Err(invalid_cursor());
    }
    Ok(after.or(last))
}
fn parse_cursor(value: &str) -> Result<EventCursor, WorkerError> {
    if value.len() > 57 {
        return Err(invalid_cursor());
    }
    let (journal, seq) = value.split_once(':').ok_or_else(invalid_cursor)?;
    let journal_id = uuid::Uuid::parse_str(journal).map_err(|_| invalid_cursor())?;
    if journal_id.is_nil()
        || journal_id.to_string() != journal
        || seq.is_empty()
        || (seq.len() > 1 && seq.starts_with('0'))
        || !seq.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid_cursor());
    }
    let seq = seq.parse::<Seq>().map_err(|_| invalid_cursor())?;
    Ok(EventCursor { journal_id, seq })
}
fn invalid_cursor() -> WorkerError {
    WorkerError::Config("INVALID_EVENT_CURSOR: invalid or conflicting event cursor".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_sources_validate_canonical_sequences_and_conflicts() {
        let id = "00000000-0000-0000-0000-000000000001";
        let cursor = resolve_cursor(Some(&format!("after={id}%3A9007199254740993")), None)
            .unwrap()
            .unwrap();
        assert_eq!(cursor.seq.as_u64(), 9_007_199_254_740_993);
        for raw in [
            format!("{id}:01"),
            format!("{id}:+1"),
            format!("{id}:18446744073709551616"),
            "bad:1".into(),
        ] {
            assert!(resolve_cursor(None, Some(&raw)).is_err(), "{raw}");
        }
        assert!(resolve_cursor(Some(&format!("after={id}:1")), Some(&format!("{id}:2"))).is_err());
        assert!(resolve_cursor(Some(&format!("after={id}:1&after={id}:1")), None).is_err());
        assert_eq!(
            resolve_cursor(Some(&format!("after={id}:1")), Some(&format!("{id}:1")))
                .unwrap()
                .unwrap()
                .seq
                .as_u64(),
            1
        );
    }
}
