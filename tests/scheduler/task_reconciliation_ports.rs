use crate::{
    support::HANDSHAKE_TIMEOUT,
    task_diagnostics_ports::{PublicRuntime, runtime, seed_task},
    task_ports_fixture::{canonical, config},
};
use mac_worker::test_support::{
    client_state::{ClientStateConcurrencyHook, ClientStateConcurrencyPoint, ClientStateStore},
    core::{config::Config, error::WorkerError},
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    task::{
        client::{TaskClient, TaskListFilter},
        model::{
            LocalTaskRecord, TaskId, TaskOutcome, TaskState, TaskStatus, TurnSummary, TurnTerminal,
        },
        store::{TaskStatusRequest, TaskStatusResponse},
        turn_runner::InlineRunnerExecutor,
        view::TaskFreshness,
    },
    transfer::HostOperation,
};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

#[derive(Clone, Copy)]
enum Failure {
    Unreachable,
    Timeout,
}
struct StatusReader {
    replies: BTreeMap<TaskId, TaskStatus>,
    unavailable: Option<String>,
    failure: Failure,
    requests: Mutex<Vec<ProcessRequest>>,
    gate: Option<(mpsc::Sender<()>, Mutex<mpsc::Receiver<()>>)>,
    gated: AtomicUsize,
}
impl ProcessRunner for StatusReader {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
        assert_eq!(
            request.args.last().unwrap(),
            HostOperation::TaskStatus.command(),
            "query attempted a mutating host operation"
        );
        assert_eq!(request.policy.deadline, Duration::from_secs(30));
        let query: TaskStatusRequest =
            serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
        assert_eq!(query.project_id(), "a".repeat(64));
        let reply = &self.replies[&query.task_id()];
        if reply.worker() == self.unavailable.as_deref() {
            return Err(match self.failure {
                Failure::Unreachable => WorkerError::Transport {
                    code: "SSH_UNAVAILABLE",
                    message: "fixture host unreachable".into(),
                },
                Failure::Timeout => {
                    mac_worker::test_support::core::error::ProcessError::DeadlineExceeded {
                        deadline: request.policy.deadline,
                    }
                    .into()
                }
            });
        }
        if let Some((entered, release)) = &self.gate
            && self.gated.fetch_add(1, Ordering::SeqCst) == 0
        {
            entered.send(()).unwrap();
            release
                .lock()
                .unwrap()
                .recv_timeout(HANDSHAKE_TIMEOUT)
                .unwrap();
        }
        canonical(&TaskStatusResponse::new(reply.clone()))
    }
}
#[derive(Default)]
struct Exchanges(AtomicUsize);
impl ClientStateConcurrencyHook for Exchanges {
    fn reach(&self, point: ClientStateConcurrencyPoint) {
        if point == ClientStateConcurrencyPoint::TaskReplacementPreExchange {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}
fn open_status(record: &LocalTaskRecord, at: u64) -> TaskStatus {
    TaskStatus::new(
        TaskState::Open,
        Some(TaskOutcome::Done),
        record.status().worker().map(str::to_owned),
        true,
        Some(record.meta().base_oid().clone()),
        Some("complete".into()),
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1,
            record.status().turns()[0].turn_id(),
            Some(TurnTerminal::Succeeded),
            Some(TaskOutcome::Done),
            Some(true),
            false,
            Some(100),
            Some(at),
        )],
        at,
    )
    .unwrap()
}
struct Fleet {
    _temp: tempfile::TempDir,
    runtime: PublicRuntime,
    store: ClientStateStore,
    config: Config,
    reader: StatusReader,
    records: Vec<LocalTaskRecord>,
}
fn fleet(unavailable: Option<&str>, failure: Failure) -> Fleet {
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state).unwrap();
    let mut records = Vec::new();
    let mut replies = BTreeMap::new();
    let mut config = config(1);
    config.workers.clear();
    for (index, worker) in ["mini-a", "mini-b", "mini-c"].into_iter().enumerate() {
        let mut entry = crate::task_ports_fixture::config(1).workers.remove(0);
        entry.name = worker.into();
        entry.ssh = worker.into();
        config.workers.push(entry);
        let active = seed_task(&store, index as u128 + 1, worker);
        let record = active.with_status(open_status(&active, 101)).unwrap();
        assert!(
            store
                .update_task_if_current(&active, record.clone())
                .unwrap()
        );
        replies.insert(record.meta().task_id(), open_status(&record, 2_000));
        records.push(record);
    }
    Fleet {
        _temp: temp,
        runtime,
        store,
        config,
        records,
        reader: StatusReader {
            replies,
            unavailable: unavailable.map(str::to_owned),
            failure,
            requests: Mutex::new(Vec::new()),
            gate: None,
            gated: AtomicUsize::new(0),
        },
    }
}
fn task_bytes(fleet: &Fleet) -> Vec<Vec<u8>> {
    fleet
        .records
        .iter()
        .map(|record| {
            fs::read(
                fleet
                    .runtime
                    .paths
                    .state
                    .join("tasks")
                    .join(format!("{}.json", record.meta().task_id())),
            )
            .unwrap()
        })
        .collect()
}
fn assert_no_capture_or_job_mutation(fleet: &Fleet) {
    assert!(!fleet.runtime.paths.cache.join("snapshots").exists());
    assert!(fleet.store.list_jobs().unwrap().is_empty());
    assert!(fleet.store.queue_snapshot().unwrap().entries().is_empty());
}
#[test]
fn task_queries_keep_unreachable_and_timed_out_hosts_stale_without_mutation_or_capture() {
    // Supersedes shared query/uncertainty safety in fleet_reconciliation::{failed_probe_is_not_evidence_that_a_retained_job_is_lost,reconcile_timeout_retains_remote_uncertainty_and_affinity}.
    for failure in [Failure::Unreachable, Failure::Timeout] {
        let fleet = fleet(Some("mini-a"), failure);
        let before = task_bytes(&fleet);
        fleet
            .store
            .record_affinity(&"a".repeat(64), &"b".repeat(64), "mini-a", 1_001)
            .unwrap();
        let client = TaskClient::new(
            &fleet.reader,
            &fleet.config,
            &fleet.runtime.paths,
            &fleet.store,
            &InlineRunnerExecutor,
        );
        let report = client.list(TaskListFilter::default()).unwrap();
        assert_eq!(report.tasks().len(), 3);
        for record in &fleet.records {
            let stale = record.status().worker() == Some("mini-a");
            let row = report
                .tasks()
                .iter()
                .find(|row| row.task_id == record.meta().task_id())
                .unwrap();
            assert_eq!(
                row.freshness,
                if stale {
                    TaskFreshness::Stale
                } else {
                    TaskFreshness::Current
                }
            );
            assert_eq!(row.state, TaskState::Open);
            let status = client.status(record.meta().task_id()).unwrap();
            assert_eq!(status.freshness(), row.freshness);
            assert_eq!(status.status().state(), TaskState::Open);
            assert_eq!(
                status.status().updated_at_millis(),
                if stale { 101 } else { 2_000 }
            );
        }
        assert_eq!(
            task_bytes(&fleet),
            before,
            "queries changed durable task observations"
        );
        let affinity = fleet
            .store
            .affinity_hints(&"a".repeat(64), &"b".repeat(64))
            .unwrap();
        assert_eq!(affinity.worktree_worker.as_deref(), Some("mini-a"));
        assert_eq!(affinity.project_worker.as_deref(), Some("mini-a"));
        assert_eq!(fleet.reader.requests.lock().unwrap().len(), 6);
        assert_no_capture_or_job_mutation(&fleet);
    }
}
#[test]
fn task_reconciliation_handoff_matrix_publishes_each_authoritative_observation_once() {
    // Supersedes fleet_reconciliation::{reconciliation_response_handoff_is_observable_without_changing_authority,reconciliation_response_handoff_matrix_preserves_one_authoritative_update_per_case}.
    for case in 0..5 {
        let fleet = fleet(Some("mini-a"), Failure::Unreachable);
        let counter = Arc::new(Exchanges::default());
        let store = ClientStateStore::open_with_concurrency_hook(
            &fleet.runtime.paths.state,
            counter.clone(),
        )
        .unwrap();
        let client = TaskClient::new(
            &fleet.reader,
            &fleet.config,
            &fleet.runtime.paths,
            &store,
            &InlineRunnerExecutor,
        );
        client.reconcile_runners().unwrap();
        assert_eq!(counter.0.load(Ordering::SeqCst), 2, "case={case}");
        assert_eq!(store.list_tasks().unwrap().len(), 3);
        for record in &fleet.records {
            let saved = store.load_task(record.meta().task_id()).unwrap();
            assert_eq!(saved.meta(), record.meta());
            assert_eq!(
                saved.status().updated_at_millis(),
                if record.status().worker() == Some("mini-a") {
                    101
                } else {
                    2_000
                }
            );
        }
        let once = task_bytes(&fleet);
        client.reconcile_runners().unwrap();
        assert_eq!(task_bytes(&fleet), once);
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            2,
            "case={case} duplicated durable observation"
        );
        assert_no_capture_or_job_mutation(&fleet);
    }
}
#[test]
fn task_reconciliation_reopen_handoff_matrix_retains_ambiguity_and_atomic_observation() {
    // Supersedes fleet_reconciliation::reboot_reconcile_handoff_matrix_retains_ambiguity_and_applies_only_authoritative_status.
    let mut schedules = [0; 4];
    for (schedule, unavailable) in [Some("mini-a"), Some("mini-b"), Some("mini-c"), None]
        .into_iter()
        .enumerate()
    {
        schedules[schedule] += 1;
        let mut fleet = fleet(unavailable, Failure::Unreachable);
        let (entered, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        fleet.reader.gate = Some((entered, Mutex::new(release_rx)));
        let counter = Arc::new(Exchanges::default());
        let store = ClientStateStore::open_with_concurrency_hook(
            &fleet.runtime.paths.state,
            counter.clone(),
        )
        .unwrap();
        let fleet = Arc::new(fleet);
        let reconciling = Arc::clone(&fleet);
        let task = fleet.records[0].meta().task_id();
        let run = thread::spawn(move || {
            TaskClient::new(
                &reconciling.reader,
                &reconciling.config,
                &reconciling.runtime.paths,
                &store,
                &InlineRunnerExecutor,
            )
            .reconcile_runners()
        });
        entered_rx.recv_timeout(HANDSHAKE_TIMEOUT).unwrap();
        let (before, after) = match schedule {
            0 => {
                let reopened = ClientStateStore::open(&fleet.runtime.paths.state).unwrap();
                let before = reopened.load_task(task).unwrap();
                let after = reopened.load_task(task).unwrap();
                release.send(()).unwrap();
                run.join().unwrap().unwrap();
                (before, after)
            }
            1 => {
                let reopened = ClientStateStore::open(&fleet.runtime.paths.state).unwrap();
                let before = reopened.load_task(task).unwrap();
                release.send(()).unwrap();
                run.join().unwrap().unwrap();
                (before, reopened.load_task(task).unwrap())
            }
            2 => {
                release.send(()).unwrap();
                run.join().unwrap().unwrap();
                let reopened = ClientStateStore::open(&fleet.runtime.paths.state).unwrap();
                let before = reopened.load_task(task).unwrap();
                (before.clone(), reopened.load_task(task).unwrap())
            }
            3 => {
                let reopened = ClientStateStore::open(&fleet.runtime.paths.state).unwrap();
                let before = reopened.load_task(task).unwrap();
                release.send(()).unwrap();
                let after = reopened.load_task(task).unwrap();
                run.join().unwrap().unwrap();
                (before, after)
            }
            _ => unreachable!(),
        };
        let final_record = fleet.store.load_task(task).unwrap();
        if schedule == 2 {
            assert_eq!(before, final_record);
        } else {
            assert_eq!(before, fleet.records[0]);
        }
        if schedule == 0 {
            assert_eq!(after, fleet.records[0]);
        } else if schedule == 3 {
            assert!(after == before || after == final_record);
        } else {
            assert_eq!(after, final_record);
        }
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            3 - usize::from(unavailable.is_some())
        );
        assert_eq!(fleet.store.list_tasks().unwrap().len(), 3);
        for original in &fleet.records {
            let current = fleet.store.load_task(original.meta().task_id()).unwrap();
            assert_eq!(current.meta(), original.meta());
            if original.status().worker() == unavailable {
                assert_eq!(current, *original);
            } else {
                assert_eq!(current.status().updated_at_millis(), 2_000);
                assert_eq!(current.status().state(), TaskState::Open);
            }
        }
        assert_no_capture_or_job_mutation(&fleet);
    }
    assert_eq!(schedules, [1, 1, 1, 1]);
}
