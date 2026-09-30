use mac_worker::controller::events::{
    EventBatch, EventReadResult, JournalReader, JournalWriter, NewEvent, ReadQuery, Seq,
    testing::MemoryJournal,
};
use std::time::Duration;

use mac_worker::{
    controller::{ControllerLeader, events::{JournalProvider, WorkerName,
        journal::{ControllerJournal, ExistingJournalProvider, JournalOptions},
        testing::ManualEventRuntime}},
    paths::PathLayout,
};
use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

struct JournalHarness {
    _temporary: tempfile::TempDir,
    paths: PathLayout,
    _leader: ControllerLeader,
    runtime: Arc<ManualEventRuntime>,
    journal: Arc<ControllerJournal>,
}

impl JournalHarness {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let paths = PathLayout {
            config: temporary.path().join("config"), state: temporary.path().join("state/mac-worker"),
            cache: temporary.path().join("cache"), data: temporary.path().join("data"),
        };
        let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
        let runtime = Arc::new(ManualEventRuntime::new());
        let journal = ControllerJournal::initialize_for_leader(&paths, &leader,
            JournalOptions { runtime: runtime.clone() }).unwrap();
        Self { _temporary: temporary, paths, _leader: leader, runtime, journal }
    }

    fn root(&self) -> std::path::PathBuf { self.paths.controller_state_root().join("events") }

    fn append_one(&self) -> Result<mac_worker::controller::events::EventCursor, mac_worker::error::WorkerError> {
        self.journal.append(EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }]).unwrap(), Duration::from_secs(60))
    }

    fn head(&self) -> mac_worker::controller::events::JournalWindow {
        self.journal.window(Duration::from_secs(60)).unwrap()
    }

    fn reopen(&self) -> Result<Arc<ControllerJournal>, mac_worker::error::WorkerError> {
        ControllerJournal::open_existing(&self.paths, JournalOptions { runtime: self.runtime.clone() })
            .map(|journal| journal.expect("initialized journal exists"))
    }

    fn seal_active_for_test(&self) {
        let event = NewEvent::WorkerChanged { worker: WorkerName::parse("w".repeat(128)).unwrap(),
            ready: Some(true), observed_at_millis: 0, code: None };
        loop {
            self.journal.append(EventBatch::try_new(vec![event.clone(); 32]).unwrap(), Duration::from_secs(60)).unwrap();
            let manifest: serde_json::Value = serde_json::from_slice(&fs::read(self.root().join("manifest.json")).unwrap()).unwrap();
            if manifest["segments"].as_array().unwrap().len() > 1 { break; }
        }
    }

    fn replace_sealed_owned_copy(&self) {
        let target = self.root().join("segment-1.jsonl");
        let replacement = self.root().join("replacement");
        fs::write(&replacement, fs::read(&target).unwrap()).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(replacement, target).unwrap();
    }
}

#[test]
fn sealed_binding_is_pinned_across_reopen() {
    let h = JournalHarness::new();
    h.append_one().unwrap();
    h.seal_active_for_test();
    let reopened = h.reopen().unwrap();
    h.replace_sealed_owned_copy();
    assert_eq!(h.reopen().err().unwrap().public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
    assert_eq!(reopened.window(Duration::from_secs(60)).err().unwrap().public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
}

#[test]
fn sealed_replacement_before_first_reader_open_fails_closed() {
    let h = JournalHarness::new();
    h.seal_active_for_test();
    h.replace_sealed_owned_copy();
    assert_eq!(h.reopen().err().unwrap().public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
}

#[test]
fn existing_open_never_initializes_or_creates_a_lock() {
    let temporary = tempfile::tempdir().unwrap();
    let paths = PathLayout { config: temporary.path().join("config"), state: temporary.path().join("state/mac-worker"),
        cache: temporary.path().join("cache"), data: temporary.path().join("data") };
    let runtime = Arc::new(ManualEventRuntime::new());
    assert!(ControllerJournal::open_existing(&paths, JournalOptions { runtime: runtime.clone() }).unwrap().is_none());
    let provider = ExistingJournalProvider::new(paths.clone(), runtime);
    assert!(provider.open_existing(Duration::from_secs(1)).unwrap().is_none());
    assert!(!paths.controller_state_root().exists());
}

#[test]
fn real_committed_cursor_matches_memory_contract_and_wakes_outside_lock() {
    let h = JournalHarness::new();
    let before = h.head().cursor();
    let writer = h.journal.clone();
    h.runtime.on_sleep(move |_| { writer.append(EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: false }; 2]).unwrap(), Duration::from_secs(60)).unwrap(); });
    let EventReadResult::Batch(batch) = h.journal.read(ReadQuery { after: Some(before), limit: 1, wait_ms: 1000 }, Duration::from_secs(60)).unwrap() else { panic!("expected batch"); };
    assert_eq!(batch.next_after.seq, Seq::new(1));
    assert_eq!(batch.head_seq, Seq::new(2));
    assert!(batch.has_more);
    batch.validate().unwrap();
    assert_eq!(h.runtime.sleeps(), vec![Duration::from_millis(200)]);
}

#[test]
fn committed_batch_exposes_last_delivered_cursor() {
    let journal = MemoryJournal::new();
    let deadline = Duration::from_secs(60);
    let before = journal.window(deadline).unwrap().cursor();
    let head = journal
        .append(
            EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }; 2])
                .unwrap(),
            deadline,
        )
        .unwrap();
    let EventReadResult::Batch(batch) = journal
        .read(
            ReadQuery {
                after: Some(before),
                limit: 1,
                wait_ms: 0,
            },
            deadline,
        )
        .unwrap()
    else {
        panic!("expected committed batch")
    };
    assert_eq!(head.seq, Seq::new(2));
    assert_eq!(batch.next_after.seq, Seq::new(1));
    assert!(batch.has_more);
    batch.validate().unwrap();
}
