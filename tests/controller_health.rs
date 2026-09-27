use mac_worker::{
    controller::{
        ControllerCommandHandler, ControllerFault, ControllerStore, DurableRequest,
        FakeControllerExecutor, parse_request, tick_controller_leader,
    },
    error::WorkerError,
    protocol::PROTOCOL_VERSION,
};
use serde_json::{Value, json};

const BAD: &str = "018f0f4a6b5c7d8e9f00112233445560";
const GOOD: &str = "018f0f4a6b5c7d8e9f00112233445561";

struct FailingRequest;

impl ControllerCommandHandler for FailingRequest {
    fn execute(&self, record: &DurableRequest) -> Result<Value, WorkerError> {
        if record.request_id() == BAD {
            Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: /private/secret token=private-value\nunsafe".into(),
            ))
        } else {
            Ok(json!({"progress": true}))
        }
    }
}

fn publish(store: &ControllerStore, id: &str) {
    let request = parse_request(
        &serde_json::to_vec(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": id,
            "command": "checkpoint.submit",
            "body": {},
        }))
        .unwrap(),
    )
    .unwrap();
    store
        .handle_with(
            &request,
            &FakeControllerExecutor,
            ControllerFault::StopAfterPublish,
        )
        .unwrap();
}

#[test]
fn leader_tick_exposes_per_request_failure_and_neighbor_progress() {
    // Removing report propagation must lose the failure and completed neighbor.
    let temp = tempfile::tempdir().unwrap();
    let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
    publish(&store, BAD);
    publish(&store, GOOD);
    let report = tick_controller_leader(&store, &FailingRequest);
    let bootstrap = report.bootstrap.unwrap();
    assert_eq!(bootstrap.rebuilt.len(), 2);
    let resumed = report.resume.unwrap();
    assert_eq!(resumed.failed.len(), 1);
    assert_eq!(resumed.failed[0].0, BAD);
    assert!(resumed.failed[0].1.starts_with("CONTROLLER_TRANSPORT"));
    assert_eq!(resumed.completed[0].request_id(), GOOD);
}

#[test]
fn repeated_failures_are_counted_without_hiding_neighbor_progress() {
    use mac_worker::{
        controller::health::{ControllerHealth, ControllerTickReport},
        job::ProcessIdentity,
        task_client::ReconcileReport,
    };
    let temp = tempfile::tempdir().unwrap();
    let store = ControllerStore::open(&temp.path().join("controller")).unwrap();
    publish(&store, BAD);
    publish(&store, GOOD);
    let mut health = ControllerHealth::new(ProcessIdentity::new(42, 1).unwrap(), 100);
    for end in [200, 300] {
        let report = ControllerTickReport::collect(
            &store,
            &FailingRequest,
            || Ok(ReconcileReport::default()),
            || Ok(end),
        );
        health.begin_tick(end - 10);
        health.finish_tick(end, 10, &report);
    }
    assert_eq!(health.failures["CONTROLLER_TRANSPORT"].count, 2);
    assert_eq!(
        health.failures["CONTROLLER_TRANSPORT"].last_seen_millis,
        300
    );
    assert_eq!(health.last_progress_millis, Some(200));
    assert_eq!(health.last_success_millis, None);
    assert_eq!(health.active_count, 1);
    let json = serde_json::to_string(&health).unwrap();
    assert!(!json.contains("private-value"));
    assert!(!json.contains("/private/"));
    assert!(!json.contains(BAD));
}

#[test]
fn health_persistence_is_atomic_private_and_bounded() {
    use mac_worker::{
        controller::health::{ControllerHealth, HealthStore},
        job::ProcessIdentity,
    };
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("controller");
    let store = HealthStore::open(&path).unwrap();
    let mut health = ControllerHealth::new(ProcessIdentity::new(42, 1).unwrap(), 100);
    store.write(&health).unwrap();
    let held = std::fs::File::open(path.join("health.json")).unwrap();
    for i in 0..1_000 {
        health.record_failure(&format!("FAIL_{i}"), 200);
    }
    health.record_failure("/private/secret\nTOKEN=x", 200);
    store.write(&health).unwrap();
    // Atomic rename leaves an already-open reader with the complete old record.
    let old: Value = serde_json::from_reader(&held).unwrap();
    assert_eq!(old["failures"], json!({}));
    let metadata = std::fs::metadata(path.join("health.json")).unwrap();
    assert_ne!(held.metadata().unwrap().ino(), metadata.ino());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert!(metadata.len() < 16_384);
    let current = store.read().unwrap().unwrap();
    assert!(current.failures.len() <= 32);
    assert_eq!(
        current.failures.values().map(|f| f.count).sum::<u64>(),
        1_001
    );
    assert!(!serde_json::to_string(&current).unwrap().contains("TOKEN"));
}

#[test]
fn pending_age_counts_only_active_receipts_and_marks_unknown_ages() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("controller");
    let store = ControllerStore::open(&path).unwrap();
    publish(&store, BAD);
    publish(&store, GOOD);
    let receipt = path.join("active").join(format!("{BAD}.json"));
    let mut value: Value = serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    value["created_at_millis"] = json!(100);
    std::fs::write(receipt, serde_json::to_vec(&value).unwrap()).unwrap();
    let pending = store.pending_health(600).unwrap();
    assert_eq!(pending.active_count, 2);
    assert_eq!(pending.oldest_pending_age_millis, Some(500));
    assert!(!pending.age_incomplete);
    std::fs::write(path.join("active").join(format!("{GOOD}.json")), b"broken").unwrap();
    let pending = store.pending_health(600).unwrap();
    assert_eq!(pending.active_count, 2);
    assert!(pending.age_incomplete);
}

#[test]
fn failure_logging_is_rate_limited_but_counts_every_tick() {
    use mac_worker::{
        controller::health::{ControllerHealth, HealthLogger},
        job::ProcessIdentity,
    };
    let mut health = ControllerHealth::new(ProcessIdentity::new(42, 1).unwrap(), 100);
    let mut logger = HealthLogger::default();
    health.record_failure("CONTROLLER_TRANSPORT", 200);
    let first = logger.failure_line(&health, 200).unwrap();
    assert!(first.contains("CONTROLLER_TRANSPORT=1"));
    for now in 201..300 {
        health.record_failure("CONTROLLER_TRANSPORT", now);
        assert!(logger.failure_line(&health, now).is_none());
    }
    let next = logger.failure_line(&health, 30_200).unwrap();
    assert!(next.contains("CONTROLLER_TRANSPORT=100"));
}
