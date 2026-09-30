use mac_worker::controller::events::{
    EventBatch, EventReadResult, JournalReader, JournalWriter, NewEvent, ReadQuery, Seq,
    testing::MemoryJournal,
};
use std::time::Duration;

use mac_worker::{
    controller::{
        ControllerLeader,
        events::{
            JournalProvider, WorkerName,
            journal::{
                BoundedPublisher, ControllerJournal, ExistingJournalProvider, JournalFaultHook,
                JournalFaultPoint, JournalOptions, JournalRole, JournalRoleBoundary,
            },
            testing::ManualEventRuntime,
        },
    },
    paths::PathLayout,
};
use std::{
    fs, io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const DEADLINE: Duration = Duration::from_secs(60);
const BOUNDARIES: [JournalRoleBoundary; 7] = [
    JournalRoleBoundary::CreationRecorded,
    JournalRoleBoundary::PartialStage,
    JournalRoleBoundary::StageSynced,
    JournalRoleBoundary::Published,
    JournalRoleBoundary::DirectorySynced,
    JournalRoleBoundary::DisplacedRemoved,
    JournalRoleBoundary::FinalSynced,
];

fn private_paths(root: &std::path::Path) -> PathLayout {
    let root = root.canonicalize().unwrap();
    PathLayout {
        config: root.join("config"),
        state: root.join("state/mac-worker"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

fn store_single_event(h: &JournalHarness, event: &mac_worker::controller::events::WireEvent) {
    let path = h.root().join("segment-1.jsonl");
    let mut bytes = serde_json::to_vec(event).unwrap();
    bytes.push(b'\n');
    fs::write(&path, &bytes).unwrap();
    let metadata = fs::metadata(path).unwrap();
    fs::write(h.root().join("manifest.json"), fixture_manifest_bytes(serde_json::json!({
        "schema_version":1,"journal_id":event.journal_id.to_string(),"head":1,"oldest":1,
        "segments":[{"name":"segment-1.jsonl","first":1,"last":1,"committed_len":bytes.len(),"sealed":false,
            "binding":{"device":metadata.dev(),"inode":metadata.ino(),"kind":libc::S_IFREG,"owner":metadata.uid(),"mode":0o600}}]
    }))).unwrap();
}

fn fixture_manifest_bytes(value: serde_json::Value) -> Vec<u8> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Binding {
        device: u64,
        inode: u64,
        kind: u32,
        owner: u32,
        mode: u32,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Segment {
        name: String,
        first: u64,
        last: Option<u64>,
        committed_len: usize,
        binding: Binding,
        sealed: bool,
    }
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Manifest {
        schema_version: u32,
        journal_id: String,
        head: u64,
        oldest: u64,
        segments: Vec<Segment>,
    }
    serde_json::to_vec(&serde_json::from_value::<Manifest>(value).unwrap()).unwrap()
}

#[derive(Default)]
struct FaultControl {
    next: Mutex<Option<(JournalFaultPoint, i32, usize)>>,
    hits: AtomicUsize,
}
impl JournalFaultHook for FaultControl {
    fn at(&self, point: JournalFaultPoint) -> io::Result<()> {
        let mut next = self.next.lock().unwrap();
        if let Some((expected, errno, remaining)) = next.as_mut()
            && *expected == point
        {
            let error = io::Error::from_raw_os_error(*errno);
            self.hits.fetch_add(1, Ordering::SeqCst);
            *remaining -= 1;
            if *remaining == 0 {
                *next = None;
            }
            return Err(error);
        }
        Ok(())
    }
}

struct JournalHarness {
    _temporary: tempfile::TempDir,
    paths: PathLayout,
    _leader: ControllerLeader,
    runtime: Arc<ManualEventRuntime>,
    journal: Arc<ControllerJournal>,
    faults: Arc<FaultControl>,
}

impl JournalHarness {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let paths = private_paths(temporary.path());
        let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
        let runtime = Arc::new(ManualEventRuntime::new());
        let faults = Arc::new(FaultControl::default());
        let journal = ControllerJournal::initialize_for_leader_with_hook(
            &paths,
            &leader,
            JournalOptions {
                runtime: runtime.clone(),
            },
            faults.clone(),
        )
        .unwrap();
        Self {
            _temporary: temporary,
            paths,
            _leader: leader,
            runtime,
            journal,
            faults,
        }
    }

    fn root(&self) -> std::path::PathBuf {
        self.paths.controller_state_root().join("events")
    }

    fn append_one(
        &self,
    ) -> Result<mac_worker::controller::events::EventCursor, mac_worker::error::WorkerError> {
        self.journal.append(
            EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }]).unwrap(),
            Duration::from_secs(60),
        )
    }

    fn head(&self) -> mac_worker::controller::events::JournalWindow {
        self.journal.window(Duration::from_secs(60)).unwrap()
    }

    fn reopen(&self) -> Result<Arc<ControllerJournal>, mac_worker::error::WorkerError> {
        ControllerJournal::open_existing(
            &self.paths,
            JournalOptions {
                runtime: self.runtime.clone(),
            },
        )
        .map(|journal| journal.expect("initialized journal exists"))
    }

    fn seal_active_for_test(&self) {
        let event = NewEvent::WorkerChanged {
            worker: WorkerName::parse("w".repeat(128)).unwrap(),
            ready: Some(true),
            observed_at_millis: 0,
            code: None,
        };
        loop {
            self.journal
                .append(
                    EventBatch::try_new(vec![event.clone(); 32]).unwrap(),
                    Duration::from_secs(60),
                )
                .unwrap();
            let manifest: serde_json::Value =
                serde_json::from_slice(&fs::read(self.root().join("manifest.json")).unwrap())
                    .unwrap();
            if manifest["segments"].as_array().unwrap().len() > 1 {
                break;
            }
        }
    }

    fn replace_sealed_owned_copy(&self) {
        let target = self.root().join("segment-1.jsonl");
        let replacement = self.root().join("replacement");
        fs::write(&replacement, fs::read(&target).unwrap()).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(replacement, target).unwrap();
    }

    fn inject(&self, point: JournalFaultPoint) {
        self.inject_errno(point, libc::EIO, 1);
    }

    fn inject_errno(&self, point: JournalFaultPoint, errno: i32, count: usize) {
        self.faults.hits.store(0, Ordering::SeqCst);
        *self.faults.next.lock().unwrap() = Some((point, errno, count));
    }

    fn recover(&self) -> Arc<ControllerJournal> {
        self.reopen().unwrap()
    }

    fn residue_bytes(&self) -> usize {
        self.usage().0
    }
    fn regular_file_count(&self) -> usize {
        self.usage().1
    }

    fn usage(&self) -> (usize, usize, usize, usize) {
        fn visit(path: &std::path::Path, depth: usize) -> (usize, usize, usize, usize) {
            let mut usage = (0, 0, 0, 0);
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let metadata = fs::symlink_metadata(entry.path()).unwrap();
                let next = if metadata.is_dir() {
                    visit(&entry.path(), depth + 1)
                } else {
                    let evidence =
                        depth > 0 || entry.file_name().to_str().unwrap().ends_with(".role");
                    (
                        metadata.len() as usize,
                        1,
                        if evidence { metadata.len() as usize } else { 0 },
                        usize::from(evidence),
                    )
                };
                usage.0 += next.0;
                usage.1 += next.1;
                usage.2 += next.2;
                usage.3 += next.3;
            }
            usage
        }
        visit(&self.root(), 0)
    }

    fn assert_budget(&self) {
        use mac_worker::controller::events::*;
        let (_, _, evidence_bytes, evidence_files) = self.usage();
        assert!(self.residue_bytes() <= MAX_JOURNAL_BYTES);
        assert!(self.regular_file_count() <= MAX_JOURNAL_FILES);
        assert!(evidence_bytes <= MAX_RECOVERY_EVIDENCE_BYTES);
        assert!(evidence_files <= MAX_RECOVERY_EVIDENCE_FILES);
    }

    /// Build a valid pinned boundary fixture without hundreds of unrelated fsyncs.
    /// Rotation, pending, retirement and reads still use the real public journal.
    fn fill_segments(&self, count: u64) {
        let epoch = self.head().journal_id;
        let mut segments = Vec::new();
        for index in 0..count {
            let first = index * 256 + 1;
            let name = format!("segment-{first}.jsonl");
            let mut bytes = Vec::new();
            for seq in first..first + 256 {
                let mut event = mac_worker::controller::events::WireEvent {
                    schema_version: 1,
                    journal_id: epoch,
                    seq: Seq::new(seq),
                    time_millis: 0,
                    kind: "future.changed".into(),
                    data: serde_json::json!({"padding":""}),
                };
                let base = event.encoded_len().unwrap();
                event.data["padding"] = serde_json::json!("a".repeat(1024 - base));
                event.validate().unwrap();
                serde_json::to_writer(&mut bytes, &event).unwrap();
                bytes.push(b'\n');
            }
            assert_eq!(bytes.len(), 256 * 1024);
            let path = self.root().join(&name);
            fs::write(&path, bytes).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let metadata = fs::metadata(&path).unwrap();
            segments.push(
                serde_json::json!({"name":name,"first":first,"last":first+255,
                "committed_len":256*1024,"sealed":index+1<count,
                "binding":{"device":metadata.dev(),"inode":metadata.ino(),"kind":libc::S_IFREG,
                    "owner":metadata.uid(),"mode":metadata.mode() & 0o7777}}),
            );
        }
        fs::write(self.root().join("manifest.json"), fixture_manifest_bytes(serde_json::json!({
            "schema_version":1,"journal_id":epoch.to_string(),"head":count*256,"oldest":1,"segments":segments
        }))).unwrap();
        assert_eq!(
            self.reopen().unwrap().window(DEADLINE).unwrap().head_seq,
            Seq::new(count * 256)
        );
    }

    fn read_from(&self, seq: u64, limit: usize) -> mac_worker::controller::events::ReadBatch {
        let EventReadResult::Batch(batch) = self
            .recover()
            .read(
                ReadQuery {
                    after: Some(mac_worker::controller::events::EventCursor {
                        journal_id: self.head().journal_id,
                        seq: Seq::new(seq),
                    }),
                    limit,
                    wait_ms: 0,
                },
                DEADLINE,
            )
            .unwrap()
        else {
            panic!("expected committed records");
        };
        batch.validate().unwrap();
        batch
    }
}

#[test]
fn sealed_binding_is_pinned_across_reopen() {
    let h = JournalHarness::new();
    h.append_one().unwrap();
    h.seal_active_for_test();
    let reopened = h.reopen().unwrap();
    h.replace_sealed_owned_copy();
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_eq!(
        reopened
            .window(Duration::from_secs(60))
            .err()
            .unwrap()
            .public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
}

#[test]
fn sealed_replacement_before_first_reader_open_fails_closed() {
    let h = JournalHarness::new();
    h.seal_active_for_test();
    h.replace_sealed_owned_copy();
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
}

#[test]
fn existing_open_never_initializes_or_creates_a_lock() {
    let temporary = tempfile::tempdir().unwrap();
    let paths = PathLayout {
        config: temporary.path().join("config"),
        state: temporary.path().join("state/mac-worker"),
        cache: temporary.path().join("cache"),
        data: temporary.path().join("data"),
    };
    let runtime = Arc::new(ManualEventRuntime::new());
    assert!(
        ControllerJournal::open_existing(
            &paths,
            JournalOptions {
                runtime: runtime.clone()
            }
        )
        .unwrap()
        .is_none()
    );
    let provider = ExistingJournalProvider::new(paths.clone(), runtime);
    assert!(
        provider
            .open_existing(Duration::from_secs(1))
            .unwrap()
            .is_none()
    );
    assert!(!paths.controller_state_root().exists());
}

#[test]
fn real_committed_cursor_matches_memory_contract_and_wakes_outside_lock() {
    let h = JournalHarness::new();
    let before = h.head().cursor();
    let writer = h.journal.clone();
    h.runtime.on_sleep(move |_| {
        writer
            .append(
                EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: false }; 2])
                    .unwrap(),
                Duration::from_secs(60),
            )
            .unwrap();
    });
    let EventReadResult::Batch(batch) = h
        .journal
        .read(
            ReadQuery {
                after: Some(before),
                limit: 1,
                wait_ms: 1000,
            },
            Duration::from_secs(60),
        )
        .unwrap()
    else {
        panic!("expected batch");
    };
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

#[test]
fn public_initialization_role_fault_matrix_preserves_epoch_and_budgets() {
    for role in [
        JournalRole::Initialization,
        JournalRole::Segment,
        JournalRole::Manifest,
    ] {
        for boundary in BOUNDARIES {
            let temporary = tempfile::tempdir().unwrap();
            let paths = private_paths(temporary.path());
            let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
            let runtime = Arc::new(ManualEventRuntime::new());
            let faults = Arc::new(FaultControl::default());
            *faults.next.lock().unwrap() =
                Some((JournalFaultPoint::Role(role, boundary), libc::EIO, 1));
            assert!(
                ControllerJournal::initialize_for_leader_with_hook(
                    &paths,
                    &leader,
                    JournalOptions {
                        runtime: runtime.clone()
                    },
                    faults.clone()
                )
                .is_err(),
                "{role:?} {boundary:?}"
            );
            assert_eq!(faults.hits.load(Ordering::SeqCst), 1);
            let root = paths.controller_state_root().join("events");
            let identity = if root.join("initialization.json").exists() {
                "initialization.json"
            } else {
                "initialization.role"
            };
            let before: serde_json::Value =
                serde_json::from_slice(&fs::read(root.join(identity)).unwrap()).unwrap();
            let journal = ControllerJournal::initialize_for_leader(
                &paths,
                &leader,
                JournalOptions { runtime },
            )
            .unwrap();
            let window = journal.window(DEADLINE).unwrap();
            assert_eq!(window.head_seq, Seq::ZERO);
            assert_eq!(window.journal_id.to_string(), before["journal_id"]);
            assert_eq!(
                journal.append(drain_batch(1), DEADLINE).unwrap().seq,
                Seq::new(1)
            );
            let h = JournalHarness {
                _temporary: temporary,
                paths,
                _leader: leader,
                runtime: Arc::new(ManualEventRuntime::new()),
                journal,
                faults,
            };
            h.assert_budget();
        }
    }
}

fn drain_batch(count: usize) -> EventBatch {
    EventBatch::try_new(vec![
        NewEvent::ControllerDrainChanged { drained: true };
        count
    ])
    .unwrap()
}

fn run_role_matrix() {
    for role in [
        JournalRole::Pending,
        JournalRole::Manifest,
        JournalRole::Segment,
        JournalRole::Retirement,
    ] {
        for boundary in BOUNDARIES {
            let h = JournalHarness::new();
            let baseline = match role {
                JournalRole::Segment => {
                    h.fill_segments(1);
                    256
                }
                JournalRole::Retirement => {
                    h.fill_segments(64);
                    16384
                }
                _ => {
                    h.append_one().unwrap();
                    1
                }
            };
            h.inject(JournalFaultPoint::Role(role, boundary));
            assert!(h.append_one().is_err(), "{role:?} {boundary:?}");
            assert_eq!(
                h.faults.hits.load(Ordering::SeqCst),
                1,
                "fault must execute"
            );
            h.assert_budget();
            let recovered = h.recover();
            let head = recovered.window(DEADLINE).unwrap().head_seq.as_u64();
            assert!(head == baseline || head == baseline + 1);
            let next = recovered.append(drain_batch(1), DEADLINE).unwrap();
            assert_eq!(next.seq.as_u64(), head + 1);
            let oldest = recovered.window(DEADLINE).unwrap().oldest_seq.as_u64();
            let batch = h.read_from(head.max(oldest - 1), 256);
            assert_eq!(batch.events.len(), 1);
            assert_eq!(batch.events[0].seq, next.seq);
            assert!(h.read_from(next.seq.as_u64(), 256).events.is_empty());
            h.assert_budget();
            let names: Vec<_> = fs::read_dir(h.root())
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert!(
                !names
                    .iter()
                    .any(|name| name.to_str().unwrap().ends_with(".stage")
                        || name.to_str().unwrap().ends_with(".role"))
            );
        }
    }
}

#[test]
fn public_append_rotation_retirement_role_fault_matrix() {
    run_role_matrix();
}

#[test]
#[ignore = "200 deterministic role-matrix iterations; explicit stress gate only"]
fn public_journal_fault_matrix_stress() {
    for _ in 0..200 {
        run_role_matrix();
    }
}

#[test]
fn public_append_fault_boundaries_and_repeated_crashes_never_duplicate_sequences() {
    for point in [
        JournalFaultPoint::PendingDurable,
        JournalFaultPoint::PartialAppend,
        JournalFaultPoint::SegmentSynced,
        JournalFaultPoint::ManifestCommitted,
        JournalFaultPoint::PendingRemoved,
    ] {
        let h = JournalHarness::new();
        for _ in 0..5 {
            let before = h.head().head_seq;
            h.inject(point);
            assert!(h.journal.append(drain_batch(2), DEADLINE).is_err());
            h.assert_budget();
            assert_eq!(
                h.recover().window(DEADLINE).unwrap().head_seq.as_u64(),
                before.as_u64() + 2
            );
        }
        let batch = h.read_from(0, 256);
        assert_eq!(batch.events.len(), 10);
        assert_eq!(
            batch
                .events
                .iter()
                .map(|event| event.seq.as_u64())
                .collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        assert!(
            h.read_from(batch.next_after.seq.as_u64(), 256)
                .events
                .is_empty()
        );
    }
}

#[test]
fn public_rotation_retention_expiry_oldest_minus_one_ahead_and_reset() {
    let h = JournalHarness::new();
    h.fill_segments(64);
    let epoch = h.head().journal_id;
    assert_eq!(h.append_one().unwrap().seq, Seq::new(16385));
    assert_eq!(h.head().oldest_seq, Seq::new(257));
    assert!(!h.root().join("segment-1.jsonl").exists());
    assert_eq!(h.read_from(256, 1).events[0].seq, Seq::new(257));
    for (journal_id, seq, reason) in [
        (epoch, 255, "cursor_expired"),
        (epoch, 16386, "cursor_ahead"),
        (uuid::Uuid::new_v4(), 0, "journal_changed"),
    ] {
        let EventReadResult::SnapshotRequired(control) = h
            .journal
            .read(
                ReadQuery {
                    after: Some(mac_worker::controller::events::EventCursor {
                        journal_id,
                        seq: Seq::new(seq),
                    }),
                    limit: 1,
                    wait_ms: 0,
                },
                DEADLINE,
            )
            .unwrap()
        else {
            panic!("expected repair control");
        };
        assert_eq!(control.reason, reason);
        control.window.validate().unwrap();
    }
    assert!(matches!(
        h.journal.read(ReadQuery::default(), DEADLINE).unwrap(),
        EventReadResult::SnapshotRequired(_)
    ));
    h.assert_budget();
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(h.root().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["segments"].as_array().unwrap().len(), 64);
}

#[test]
fn unsafe_journal_symlink_hardlink_modes_lock_and_epoch_fail_without_reset() {
    for corruption in [
        "symlink",
        "hardlink",
        "file_mode",
        "root_mode",
        "lock_copy",
        "lock_content",
        "epoch",
        "tail",
        "unknown_stage",
    ] {
        let h = JournalHarness::new();
        h.append_one().unwrap();
        let manifest = fs::read(h.root().join("manifest.json")).unwrap();
        let segment = h.root().join("segment-1.jsonl");
        match corruption {
            "symlink" => {
                let saved = h._temporary.path().join("saved");
                fs::rename(&segment, &saved).unwrap();
                std::os::unix::fs::symlink(saved, &segment).unwrap();
            }
            "hardlink" => {
                fs::hard_link(&segment, h._temporary.path().join("linked")).unwrap();
            }
            "file_mode" => {
                fs::set_permissions(&segment, fs::Permissions::from_mode(0o640)).unwrap()
            }
            "root_mode" => {
                fs::set_permissions(h.root(), fs::Permissions::from_mode(0o750)).unwrap()
            }
            "lock_copy" => {
                let copy = h.root().join("copy");
                fs::write(&copy, b"").unwrap();
                fs::set_permissions(&copy, fs::Permissions::from_mode(0o600)).unwrap();
                fs::rename(copy, h.root().join("journal.lock")).unwrap();
            }
            "lock_content" => fs::write(h.root().join("journal.lock"), b"x").unwrap(),
            "epoch" => {
                let mut value: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
                value["journal_id"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
                fs::write(
                    h.root().join("manifest.json"),
                    serde_json::to_vec(&value).unwrap(),
                )
                .unwrap();
            }
            "tail" => {
                use std::io::Write;
                fs::OpenOptions::new()
                    .append(true)
                    .open(&segment)
                    .unwrap()
                    .write_all(b"unproved tail")
                    .unwrap();
            }
            "unknown_stage" => {
                fs::write(h.root().join("pending.stage"), b"unproved").unwrap();
                fs::set_permissions(
                    h.root().join("pending.stage"),
                    fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = fs::read(h.root().join("manifest.json")).unwrap();
        assert_eq!(
            h.reopen().err().unwrap().public_code(),
            "CONTROLLER_EVENTS_UNAVAILABLE",
            "{corruption}"
        );
        assert_eq!(
            h.append_one().err().unwrap().public_code(),
            "CONTROLLER_EVENTS_UNAVAILABLE",
            "{corruption}"
        );
        assert_eq!(fs::read(h.root().join("manifest.json")).unwrap(), before);
    }
}

#[test]
fn unknown_rooted_cleanup_evidence_is_preserved_and_unavailable() {
    let h = JournalHarness::new();
    let foreign = h.root().join(".mac-worker-rooted-fs/foreign-evidence");
    fs::write(&foreign, b"unknown").unwrap();
    fs::set_permissions(&foreign, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_eq!(fs::read(&foreign).unwrap(), b"unknown");
}

#[test]
fn public_estale_retries_once_prepared_range_and_exhausts_after_three_retries() {
    let h = JournalHarness::new();
    h.inject_errno(JournalFaultPoint::ReadAttempt, libc::ESTALE, 2);
    assert_eq!(h.head().head_seq, Seq::ZERO);
    assert_eq!(h.faults.hits.load(Ordering::SeqCst), 2);
    h.inject_errno(JournalFaultPoint::AppendPrepared, libc::ESTALE, 1);
    assert_eq!(
        h.journal.append(drain_batch(2), DEADLINE).unwrap().seq,
        Seq::new(2)
    );
    assert_eq!(h.read_from(0, 256).events.len(), 2);
    h.inject_errno(JournalFaultPoint::ReadAttempt, libc::ESTALE, 4);
    let error = h.journal.window(DEADLINE).unwrap_err();
    assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
    assert!(error.to_string().contains("retryable"));
    assert_eq!(h.faults.hits.load(Ordering::SeqCst), 4);
}

#[test]
fn public_empty_timeout_cancellation_and_provider_keep_original_deadline() {
    use mac_worker::controller::events::{EventCursor, EventRuntime};
    let h = JournalHarness::new();
    let cursor = h.head().cursor();
    let EventReadResult::Batch(batch) = h
        .journal
        .read(
            ReadQuery {
                after: Some(cursor),
                limit: 0,
                wait_ms: 500,
            },
            DEADLINE,
        )
        .unwrap()
    else {
        panic!("expected empty timeout");
    };
    assert_eq!(batch.next_after, cursor);
    assert!(batch.events.is_empty());
    assert_eq!(h.runtime.now(), Duration::from_millis(500));
    assert_eq!(
        h.runtime.sleeps(),
        vec![
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_millis(100)
        ]
    );
    let runtime = h.runtime.clone();
    h.runtime.on_sleep(move |_| runtime.cancel());
    assert_eq!(
        h.journal
            .read(
                ReadQuery {
                    after: Some(EventCursor {
                        journal_id: h.head().journal_id,
                        seq: Seq::ZERO
                    }),
                    limit: 1,
                    wait_ms: 500
                },
                DEADLINE
            )
            .unwrap_err()
            .public_code(),
        "CONTROLLER_EVENTS_CANCELLED"
    );
    let provider =
        ExistingJournalProvider::new(h.paths.clone(), Arc::new(ManualEventRuntime::new()));
    assert_eq!(
        provider
            .open_existing(Duration::ZERO)
            .err()
            .unwrap()
            .public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
}

#[test]
fn public_batch_bounds_and_sequence_exhaustion_do_not_mutate_head() {
    let h = JournalHarness::new();
    assert!(
        EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }; 33]).is_err()
    );
    assert_eq!(
        h.journal.append(drain_batch(32), DEADLINE).unwrap().seq,
        Seq::new(32)
    );
    assert_eq!(
        h.journal.append(drain_batch(0), DEADLINE).unwrap().seq,
        Seq::new(32)
    );
    let manifest = fs::read(h.root().join("manifest.json")).unwrap();
    assert!(h.journal.append(drain_batch(1), Duration::ZERO).is_err());
    assert_eq!(fs::read(h.root().join("manifest.json")).unwrap(), manifest);
    let epoch = h.head().journal_id;
    let event = NewEvent::ControllerDrainChanged { drained: true }
        .to_wire(epoch, Seq::new(u64::MAX), 0)
        .unwrap();
    let mut bytes = serde_json::to_vec(&event).unwrap();
    bytes.push(b'\n');
    let old_segment = h.root().join("segment-1.jsonl");
    fs::remove_file(old_segment).unwrap();
    let name = format!("segment-{}.jsonl", u64::MAX);
    let path = h.root().join(&name);
    fs::write(&path, &bytes).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let metadata = fs::metadata(path).unwrap();
    let mut manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    manifest["head"] = serde_json::json!(u64::MAX);
    manifest["oldest"] = serde_json::json!(u64::MAX);
    manifest["segments"] = serde_json::json!([{"name":name,"first":u64::MAX,"last":u64::MAX,"committed_len":bytes.len(),"sealed":false,
        "binding":{"device":metadata.dev(),"inode":metadata.ino(),"kind":libc::S_IFREG,"owner":metadata.uid(),"mode":0o600}}]);
    fs::write(
        h.root().join("manifest.json"),
        fixture_manifest_bytes(manifest),
    )
    .unwrap();
    assert_eq!(
        h.reopen().unwrap().window(DEADLINE).unwrap().head_seq,
        Seq::new(u64::MAX)
    );
    assert_eq!(
        h.append_one().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_eq!(h.head().head_seq, Seq::new(u64::MAX));
}

#[test]
fn blocked_journal_fsync_gate_leaves_publisher_state_and_drain_independent() {
    use mac_worker::controller::events::{EventSink, PublishAttempt, testing::RecordingSink};
    use std::sync::mpsc;
    let h = JournalHarness::new();
    let (reached, wait) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let gate = Mutex::new(gate);
    let once = std::sync::atomic::AtomicBool::new(true);
    let hook = Arc::new(move |point| {
        if point == JournalFaultPoint::SegmentSynced && once.swap(false, Ordering::SeqCst) {
            reached.send(()).unwrap();
            gate.lock().unwrap().recv().unwrap();
        }
        Ok(())
    });
    let journal = ControllerJournal::open_existing_with_hook(
        &h.paths,
        JournalOptions {
            runtime: h.runtime.clone(),
        },
        hook,
    )
    .unwrap()
    .unwrap();
    struct CompletionWriter {
        journal: Arc<ControllerJournal>,
        complete: mpsc::Sender<()>,
    }
    impl JournalReader for CompletionWriter {
        fn window(
            &self,
            deadline: Duration,
        ) -> Result<mac_worker::controller::events::JournalWindow, mac_worker::error::WorkerError>
        {
            self.journal.window(deadline)
        }
        fn read(
            &self,
            query: ReadQuery,
            deadline: Duration,
        ) -> Result<EventReadResult, mac_worker::error::WorkerError> {
            self.journal.read(query, deadline)
        }
    }
    impl JournalWriter for CompletionWriter {
        fn append(
            &self,
            batch: EventBatch,
            deadline: Duration,
        ) -> Result<mac_worker::controller::events::EventCursor, mac_worker::error::WorkerError>
        {
            let result = self.journal.append(batch, deadline);
            self.complete.send(()).unwrap();
            result
        }
    }
    let (complete, finished) = mpsc::channel();
    let (sink, handle) = BoundedPublisher::start(
        Arc::new(CompletionWriter { journal, complete }),
        h.runtime.clone(),
    );
    assert_eq!(sink.try_publish(drain_batch(1)), PublishAttempt::Queued);
    wait.recv().unwrap();
    for _ in 0..128 {
        assert_eq!(sink.try_publish(drain_batch(32)), PublishAttempt::Queued);
    }
    assert_eq!(sink.try_publish(drain_batch(1)), PublishAttempt::Dropped);
    let state = mac_worker::client_state::ClientStateStore::open(&h.paths.state).unwrap();
    assert!(state.list_tasks().unwrap().is_empty());
    mac_worker::controller::drain::set_drained(&h.paths.controller_state_root(), true).unwrap();
    assert!(mac_worker::controller::drain::is_drained(&h.paths.controller_state_root()).unwrap());
    let recording = RecordingSink::new();
    assert_eq!(
        recording.try_publish(drain_batch(1)),
        PublishAttempt::Queued
    );
    handle.finish_with_grace(Duration::from_secs(10));
    assert_eq!(sink.try_publish(drain_batch(1)), PublishAttempt::Dropped);
    assert!(handle.diagnostics().iter().any(|diagnostic| diagnostic.code
        == "CONTROLLER_EVENTS_DROPPED_FULL"
        && diagnostic.count == 1));
    release.send(()).unwrap();
    // Completion is observed only after append has released EX; no clock polling.
    finished.recv().unwrap();
    assert_eq!(h.head().head_seq, Seq::new(1));
}

#[test]
fn committed_envelopes_use_frozen_validation_and_allow_unknown_versions() {
    let h = JournalHarness::new();
    let epoch = h.head().journal_id;
    let mut event = NewEvent::ControllerDrainChanged { drained: true }
        .to_wire(epoch, Seq::new(1), 0)
        .unwrap();
    event.data = serde_json::json!({"drained":"invalid"});
    store_single_event(&h, &event);
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    event.schema_version = 2;
    store_single_event(&h, &event);
    assert_eq!(
        h.reopen().unwrap().window(DEADLINE).unwrap().head_seq,
        Seq::new(1)
    );
    assert_eq!(h.read_from(0, 1).events[0].affected_task(), None);
}

#[test]
fn committed_event_limit_counts_newline_and_rejects_one_extra_byte() {
    let h = JournalHarness::new();
    let mut event = mac_worker::controller::events::WireEvent {
        schema_version: 1,
        journal_id: h.head().journal_id,
        seq: Seq::new(1),
        time_millis: 0,
        kind: "future.changed".into(),
        data: serde_json::json!({"padding":""}),
    };
    let base = event.encoded_len().unwrap();
    event.data["padding"] = serde_json::json!("a".repeat(1024 - base));
    store_single_event(&h, &event);
    assert_eq!(h.read_from(0, 1).events[0].encoded_len().unwrap(), 1024);
    event.data["padding"] = serde_json::json!("a".repeat(1025 - base));
    store_single_event(&h, &event);
    let bytes = fs::read(h.root().join("segment-1.jsonl")).unwrap();
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_eq!(fs::read(h.root().join("segment-1.jsonl")).unwrap(), bytes);
}

#[test]
fn original_provider_deadline_and_retained_root_are_not_reset_or_adopted() {
    use mac_worker::controller::events::EventRuntime;
    use std::os::fd::AsRawFd;
    let h = JournalHarness::new();
    let lock = fs::File::open(h.root().join("journal.lock")).unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let provider = ExistingJournalProvider::new(h.paths.clone(), h.runtime.clone());
    assert_eq!(
        provider
            .open_existing(Duration::from_millis(3))
            .err()
            .unwrap()
            .public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_eq!(h.runtime.now(), Duration::from_millis(3));
    assert_eq!(h.runtime.sleeps().len(), 3);
    drop(lock);
    let foreign = JournalHarness::new();
    let foreign_epoch = foreign.head().journal_id;
    fs::rename(h.root(), h._temporary.path().join("saved-root")).unwrap();
    fs::rename(foreign.root(), h.root()).unwrap();
    assert_eq!(
        h.journal.window(DEADLINE).unwrap_err().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_ne!(foreign_epoch, h.journal_id_from_saved());
}

impl JournalHarness {
    fn journal_id_from_saved(&self) -> uuid::Uuid {
        let initialization: serde_json::Value = serde_json::from_slice(
            &fs::read(
                self._temporary
                    .path()
                    .join("saved-root/initialization.json"),
            )
            .unwrap(),
        )
        .unwrap();
        initialization["journal_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }
}

#[test]
fn retired_segment_removal_fault_is_recovered_without_reappend() {
    let h = JournalHarness::new();
    h.fill_segments(64);
    h.inject(JournalFaultPoint::Retired);
    assert!(h.append_one().is_err());
    assert_eq!(h.faults.hits.load(Ordering::SeqCst), 1);
    h.assert_budget();
    assert_eq!(
        h.recover().window(DEADLINE).unwrap().head_seq,
        Seq::new(16385)
    );
    assert_eq!(h.append_one().unwrap().seq, Seq::new(16386));
    assert_eq!(h.read_from(16384, 256).events.len(), 2);
    h.assert_budget();
}

#[test]
fn concurrent_processes_append_and_read_strictly_contiguous_commits() {
    use std::{
        io::{BufRead, Write},
        process::{Command, Stdio},
    };
    let h = JournalHarness::new();
    let mut children = Vec::new();
    for _ in 0..2 {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "controller_event_journal::journal_process_child",
                "--nocapture",
            ])
            .env("EV_T2_JOURNAL_PROCESS_ROOT", h._temporary.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                output.read_line(&mut line).unwrap() > 0,
                "child exited before ready"
            );
            if line.trim() == "JOURNAL_CHILD_READY" {
                break;
            }
        }
        children.push((child, output));
    }
    for (child, _) in &mut children {
        child.stdin.as_mut().unwrap().write_all(b"x").unwrap();
    }
    for (child, _) in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let batch = h.read_from(0, 256);
    assert_eq!(
        batch
            .events
            .iter()
            .map(|event| event.seq.as_u64())
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert_eq!(batch.head_seq, Seq::new(8));
}

#[test]
fn journal_process_child() {
    use std::io::{Read, Write};
    let Some(root) = std::env::var_os("EV_T2_JOURNAL_PROCESS_ROOT") else {
        return;
    };
    let paths = private_paths(std::path::Path::new(&root));
    let runtime = Arc::new(ManualEventRuntime::new());
    runtime.on_sleep(|_| std::thread::yield_now());
    let journal = ControllerJournal::open_existing(&paths, JournalOptions { runtime })
        .unwrap()
        .unwrap();
    let epoch = journal
        .window(Duration::from_secs(1_000_000))
        .unwrap()
        .journal_id;
    println!("JOURNAL_CHILD_READY");
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_exact(&mut [0]).unwrap();
    let deadline = Duration::from_secs(1_000_000);
    for _ in 0..4 {
        loop {
            match journal.append(drain_batch(1), deadline) {
                Ok(_) => break,
                Err(error) => {
                    assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
                    std::thread::yield_now();
                }
            }
        }
        loop {
            match journal.read(
                ReadQuery {
                    after: Some(mac_worker::controller::events::EventCursor {
                        journal_id: epoch,
                        seq: Seq::ZERO,
                    }),
                    limit: 256,
                    wait_ms: 0,
                },
                deadline,
            ) {
                Ok(result) => {
                    result.validate().unwrap();
                    break;
                }
                Err(error) => {
                    assert_eq!(error.public_code(), "CONTROLLER_EVENTS_UNAVAILABLE");
                    std::thread::yield_now();
                }
            }
        }
    }
}

#[test]
fn initialization_rejects_unknown_cleanup_before_allocating_lock_or_epoch() {
    let temporary = tempfile::tempdir().unwrap();
    let paths = private_paths(temporary.path());
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let root = paths.controller_state_root().join("events");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let namespace = root.join(".mac-worker-rooted-fs");
    fs::create_dir(&namespace).unwrap();
    fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
    let unknown = namespace.join("foreign-evidence");
    fs::write(&unknown, b"unknown").unwrap();
    fs::set_permissions(&unknown, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        ControllerJournal::initialize_for_leader(
            &paths,
            &leader,
            JournalOptions {
                runtime: Arc::new(ManualEventRuntime::new())
            }
        )
        .err()
        .unwrap()
        .public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert!(!root.join("journal.lock").exists());
    assert!(!root.join("initialization.json").exists());
    assert_eq!(fs::read(unknown).unwrap(), b"unknown");
}

#[test]
fn cancellation_during_lock_admission_is_reported_as_cancelled() {
    use std::os::fd::AsRawFd;
    let h = JournalHarness::new();
    let lock = fs::File::open(h.root().join("journal.lock")).unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let runtime = h.runtime.clone();
    h.runtime.on_sleep(move |_| runtime.cancel());
    assert_eq!(
        h.journal.window(DEADLINE).unwrap_err().public_code(),
        "CONTROLLER_EVENTS_CANCELLED"
    );
    assert_eq!(h.runtime.sleeps().len(), 1);
}

#[test]
fn publisher_panic_is_contained_and_only_drops_optional_hints() {
    use mac_worker::controller::events::{EventCursor, JournalWindow, PublishAttempt};
    struct PanickingWriter(MemoryJournal);
    impl JournalReader for PanickingWriter {
        fn window(
            &self,
            deadline: Duration,
        ) -> Result<JournalWindow, mac_worker::error::WorkerError> {
            self.0.window(deadline)
        }
        fn read(
            &self,
            query: ReadQuery,
            deadline: Duration,
        ) -> Result<EventReadResult, mac_worker::error::WorkerError> {
            self.0.read(query, deadline)
        }
    }
    impl JournalWriter for PanickingWriter {
        fn append(
            &self,
            _: EventBatch,
            _: Duration,
        ) -> Result<EventCursor, mac_worker::error::WorkerError> {
            panic!("injected optional journal panic");
        }
    }
    let (sink, handle) = BoundedPublisher::start(
        Arc::new(PanickingWriter(MemoryJournal::new())),
        Arc::new(ManualEventRuntime::new()),
    );
    assert_eq!(sink.try_publish(drain_batch(1)), PublishAttempt::Queued);
    while !handle
        .diagnostics()
        .iter()
        .any(|item| item.code == "CONTROLLER_EVENTS_PUBLISHER_PANIC" && item.count == 1)
    {
        std::thread::yield_now();
    }
    assert_eq!(sink.try_publish(drain_batch(1)), PublishAttempt::Dropped);
    handle.stop_without_join();
}

#[test]
fn manifest_exchange_cleanup_intent_resumes_with_pinned_old_generation() {
    let h = JournalHarness::new();
    h.append_one().unwrap();
    h.inject(JournalFaultPoint::Role(
        JournalRole::Manifest,
        JournalRoleBoundary::CleanupDecisionDurable,
    ));
    assert!(h.append_one().is_err());
    assert_eq!(h.faults.hits.load(Ordering::SeqCst), 1);
    let namespace = h.root().join(".mac-worker-rooted-fs");
    assert!(fs::read_dir(&namespace).unwrap().count() > 0);
    h.assert_budget();
    assert_eq!(h.recover().window(DEADLINE).unwrap().head_seq, Seq::new(2));
    assert_eq!(h.append_one().unwrap().seq, Seq::new(3));
    assert_eq!(
        h.read_from(0, 256)
            .events
            .iter()
            .map(|event| event.seq.as_u64())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(fs::read_dir(namespace).unwrap().count(), 0);
}

#[test]
fn unbound_segment_prevents_pending_recovery_from_growing_or_committing() {
    let h = JournalHarness::new();
    h.append_one().unwrap();
    h.inject(JournalFaultPoint::PendingDurable);
    assert!(h.append_one().is_err());
    let unknown = h.root().join("segment-777.jsonl");
    fs::write(&unknown, b"unbound").unwrap();
    fs::set_permissions(&unknown, fs::Permissions::from_mode(0o600)).unwrap();
    let manifest = fs::read(h.root().join("manifest.json")).unwrap();
    let segment = fs::read(h.root().join("segment-1.jsonl")).unwrap();
    let pending = fs::read(h.root().join("pending.json")).unwrap();
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
    assert_eq!(fs::read(h.root().join("manifest.json")).unwrap(), manifest);
    assert_eq!(fs::read(h.root().join("segment-1.jsonl")).unwrap(), segment);
    assert_eq!(fs::read(h.root().join("pending.json")).unwrap(), pending);
    assert_eq!(fs::read(unknown).unwrap(), b"unbound");
}

#[test]
fn empty_manifest_cannot_advertise_an_impossible_window() {
    let h = JournalHarness::new();
    let epoch = h.head().journal_id;
    let path = h.root().join("segment-2.jsonl");
    fs::rename(h.root().join("segment-1.jsonl"), &path).unwrap();
    let metadata = fs::metadata(path).unwrap();
    fs::write(h.root().join("manifest.json"), fixture_manifest_bytes(serde_json::json!({
        "schema_version":1,"journal_id":epoch.to_string(),"head":0,"oldest":2,
        "segments":[{"name":"segment-2.jsonl","first":2,"last":null,"committed_len":0,"sealed":false,
            "binding":{"device":metadata.dev(),"inode":metadata.ino(),"kind":libc::S_IFREG,"owner":metadata.uid(),"mode":0o600}}]
    }))).unwrap();
    assert_eq!(
        h.reopen().err().unwrap().public_code(),
        "CONTROLLER_EVENTS_UNAVAILABLE"
    );
}
