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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch};

/// Local replay work has admission separate from live response slots. Started
/// replay reads retain their permits until they return, even after disconnect.
/// Reads run on detached OS threads, so stopping a viewer or its Tokio runtime
/// never joins a stalled journal call. Unstarted reads observe cancellation.
pub struct LocalViewerEventSource {
    reader: Arc<dyn JournalReader>,
    runtime: Arc<dyn EventRuntime>,
    live: broadcast::Sender<ViewerMessage>,
    stopped: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    streams: Arc<Semaphore>,
    journal_work: Arc<Semaphore>,
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
            journal_work: Arc::new(Semaphore::new(MAX_STREAMS)),
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
        let work = match Arc::clone(&self.journal_work).try_acquire_owned() {
            Ok(permit) => ReplayWork {
                reader: Arc::clone(&self.reader),
                runtime: Arc::clone(&self.runtime),
                stopped: Arc::clone(&self.stopped),
                _permit: permit,
            },
            Err(_) => {
                let (sender, receiver) = mpsc::channel(1);
                if !self.stopped.load(Ordering::SeqCst) {
                    let _ = sender.try_send(ViewerMessage::Unavailable {
                        code: UNAVAILABLE.into(),
                    });
                }
                return Ok(receiver);
            }
        };
        let permit = Arc::clone(&self.streams).try_acquire_owned().map_err(|_| {
            WorkerError::Unavailable(
                "CONTROLLER_EVENTS_STREAM_LIMIT: viewer stream limit reached".into(),
            )
        })?;
        let handle = tokio::runtime::Handle::try_current().map_err(|_| unavailable())?;
        // The live receiver is registered BEFORE the replay window is read.
        let live = self.live.subscribe();
        let (sender, receiver) = mpsc::channel(CAPACITY);
        let mut shutdown = self.shutdown.subscribe();
        handle.spawn(async move {
            let _permit = permit;
            let disconnect = sender.clone();
            tokio::select! {
                biased;
                _ = cancelled(&mut shutdown) => {},
                _ = disconnect.closed() => {},
                _ = replay_and_follow(work, after, live, sender) => {},
            }
        });
        Ok(receiver)
    }
    fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.journal_work.close();
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

// One permit travels with the whole replay, including into each queued or
// running closure. Dropping an async receiver cannot release an active read's
// admission. There is no executor queue: at most eight detached jobs can exist.
struct ReplayWork {
    reader: Arc<dyn JournalReader>,
    runtime: Arc<dyn EventRuntime>,
    stopped: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
}
impl ReplayWork {
    async fn run<T: Send + 'static>(
        self,
        read: impl FnOnce(&dyn JournalReader, Duration) -> Result<T, WorkerError> + Send + 'static,
    ) -> Result<(T, Self), WorkerError> {
        let read_deadline = deadline(self.runtime.as_ref());
        let (reply, result) = oneshot::channel();
        thread::Builder::new()
            .name("dashboard-event-replay".into())
            .spawn(move || {
                if self.stopped.load(Ordering::SeqCst)
                    || self.runtime.cancelled()
                    || self.stopped.load(Ordering::SeqCst)
                    || reply.is_closed()
                {
                    return;
                }
                let value = read(self.reader.as_ref(), read_deadline);
                let _ = reply.send((value, self));
            })
            .map_err(|_| unavailable())?;
        let (value, work) = result.await.map_err(|_| unavailable())?;
        Ok((value?, work))
    }
}

async fn replay_and_follow(
    work: ReplayWork,
    requested: Option<EventCursor>,
    mut live: broadcast::Receiver<ViewerMessage>,
    sender: mpsc::Sender<ViewerMessage>,
) {
    let Ok((mut window, mut work)) = work.run(|reader, deadline| reader.window(deadline)).await
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
        let Ok((result, continued)) = work
            .run(move |reader, deadline| reader.read(query, deadline))
            .await
        else {
            let _ = sender.try_send(ViewerMessage::Unavailable {
                code: UNAVAILABLE.into(),
            });
            return;
        };
        work = continued;
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
    // Live delivery holds a response slot, but no journal work admission.
    drop(work);
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
    use crate::controller::events::testing::{ManualEventRuntime, MemoryJournal};
    use std::sync::{Mutex, atomic::AtomicUsize, mpsc as std_mpsc};

    struct BeforeReadRuntime {
        clock: ManualEventRuntime,
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<Option<std_mpsc::Receiver<()>>>,
    }
    impl EventRuntime for BeforeReadRuntime {
        fn now(&self) -> Duration {
            self.clock.now()
        }
        fn sleep(&self, duration: Duration) {
            self.clock.sleep(duration);
        }
        fn cancelled(&self) -> bool {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                let _ = entered.send(());
                let _ = self.release.lock().unwrap().take().unwrap().recv();
            }
            self.clock.cancelled()
        }
    }
    struct CountingReader {
        journal: MemoryJournal,
        calls: AtomicUsize,
    }
    impl JournalReader for CountingReader {
        fn window(&self, deadline: Duration) -> Result<JournalWindow, WorkerError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.journal.window(deadline)
        }
        fn read(
            &self,
            query: ReadQuery,
            deadline: Duration,
        ) -> Result<EventReadResult, WorkerError> {
            self.journal.read(query, deadline)
        }
    }
    #[tokio::test]
    async fn cancelled_unstarted_journal_work_never_calls_the_reader() {
        for stop in [true, false] {
            let admission = Arc::new(Semaphore::new(1));
            let stopped = Arc::new(AtomicBool::new(false));
            let reader = Arc::new(CountingReader {
                journal: MemoryJournal::new(),
                calls: AtomicUsize::new(0),
            });
            let (entered, entering) = oneshot::channel();
            let (release, waiting) = std_mpsc::channel();
            let work = ReplayWork {
                reader: reader.clone(),
                runtime: Arc::new(BeforeReadRuntime {
                    clock: ManualEventRuntime::new(),
                    entered: Mutex::new(Some(entered)),
                    release: Mutex::new(Some(waiting)),
                }),
                stopped: stopped.clone(),
                _permit: admission.clone().try_acquire_owned().unwrap(),
            };
            let task = tokio::spawn(work.run(|reader, deadline| reader.window(deadline)));
            entering.await.unwrap();
            let task = if stop {
                stopped.store(true, Ordering::SeqCst);
                Some(task)
            } else {
                task.abort();
                let error = match task.await {
                    Err(error) => error,
                    Ok(_) => panic!("cancelled replay task completed"),
                };
                assert!(error.is_cancelled());
                None
            };
            assert_eq!(admission.available_permits(), 0);
            release.send(()).unwrap();
            if let Some(task) = task {
                drop(task.await.unwrap());
            }
            let _returned = admission.acquire_owned().await.unwrap();
            assert_eq!(
                reader.calls.load(Ordering::SeqCst),
                0,
                "cancelled work entered the journal (stop={stop})"
            );
        }
    }

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
