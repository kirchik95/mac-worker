use mac_worker::test_support::integration::*;
use mac_worker::test_support::task::model::{TaskState, TaskStatus};

fn facts() -> IntegrationTaskFacts {
    IntegrationTaskFacts {
        ordinary: sample_ordinary(fixture_task(), fixture_source()),
        cycle_base: fixture_head(),
        result_imported: true,
        session_import_complete: true,
        continuation_pending: false,
        runner_present: false,
        stop_requested: false,
        close_pending: false,
        submission_pending: false,
        auxiliary_purpose: None,
    }
}

#[test]
fn disabled_projection_preserves_review_and_omits_extension_fields() {
    let view = project_integration(None, &facts()).unwrap();
    let json = serde_json::to_value(view).unwrap();
    assert_eq!(json["review_state"], "ready_for_review");
    assert_eq!(json["attention"], true);
    assert!(json.get("integration").is_none());
    assert!(json.get("workflow_state").is_none());
}

#[test]
fn automatic_phases_and_parked_receipts_never_request_review() {
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    for state in [
        IntegrationStatus::Pending,
        IntegrationStatus::Fetching,
        IntegrationStatus::Resolving,
        IntegrationStatus::Verifying,
        IntegrationStatus::CommitReady,
        IntegrationStatus::Pushing,
        IntegrationStatus::Published,
        IntegrationStatus::RetryWait,
        IntegrationStatus::Parked,
    ] {
        snapshot.state = state;
        snapshot.resume_state = matches!(
            state,
            IntegrationStatus::RetryWait | IntegrationStatus::Parked
        )
        .then_some(IntegrationStatus::Published);
        snapshot.pause_reason = (state == IntegrationStatus::Parked)
            .then_some(IntegrationPauseReason::ControllerDrained);
        let view = project_integration(Some(&snapshot), &facts()).unwrap();
        assert_eq!(
            view.workflow_state,
            Some(WorkflowState::Integrating),
            "{state:?}"
        );
        assert!(!view.attention, "{state:?}");
        assert_eq!(
            serde_json::to_value(view.review_state).unwrap(),
            "not_reviewable"
        );
    }
    let mut running = facts();
    running.runner_present = true;
    running.auxiliary_purpose = Some(IntegrationTurnPurpose::Resolve);
    snapshot.state = IntegrationStatus::Resolving;
    snapshot.resume_state = None;
    snapshot.pause_reason = None;
    assert_eq!(
        project_integration(Some(&snapshot), &running)
            .unwrap()
            .workflow_state,
        Some(WorkflowState::Running)
    );
}

#[test]
fn integrated_open_never_is_done_and_blocked_retains_source_done() {
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Integrated;
    snapshot.merge_oid = Some("e".repeat(40).parse().unwrap());
    snapshot.disposition = Some(IntegrationDisposition::Merged);
    let facts = facts();
    let success = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(success.workflow_state, Some(WorkflowState::Done));
    assert!(!success.attention);
    snapshot.state = IntegrationStatus::Blocked;
    snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
    let blocked = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(blocked.workflow_state, Some(WorkflowState::NeedsYou));
    assert!(blocked.attention);
    assert_eq!(
        serde_json::to_value(blocked.review_state).unwrap(),
        "ready_for_follow_up"
    );
    assert_eq!(
        facts.ordinary.status().last_outcome().unwrap().kind(),
        "done"
    );
}

#[test]
fn armed_admission_dependency_wait_and_terminal_cancellation_keep_ordinary_state() {
    let mut facts = facts();
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Armed;
    let queued = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![],
        1000,
    )
    .unwrap();
    facts.ordinary = facts.ordinary.with_status(queued).unwrap();
    assert_eq!(
        project_integration(Some(&snapshot), &facts)
            .unwrap()
            .workflow_state,
        Some(WorkflowState::Queued)
    );
    snapshot.state = IntegrationStatus::Revoked;
    let closed = TaskStatus::new(
        TaskState::Closed,
        Some(mac_worker::test_support::task::model::TaskOutcome::Cancelled),
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![],
        1001,
    )
    .unwrap();
    facts.ordinary = facts.ordinary.with_status(closed).unwrap();
    let view = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(view.workflow_state, Some(WorkflowState::Done));
    assert!(!view.attention);
}

#[test]
fn compact_confirmation_requires_the_complete_same_revision_identity() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let annotation = record.snapshot.annotation().unwrap();
    assert!(annotation.confirms(&record.snapshot));
    let mut newer = record.snapshot.clone();
    newer.revision = newer.revision.next().unwrap();
    assert!(!annotation.confirms(&newer));
    let mut other_epoch = record.snapshot.clone();
    other_epoch.epoch += 1;
    assert!(!annotation.confirms(&other_epoch));
    let mut other_target = sample_record(fixture_task(), fixture_source(), "release").snapshot;
    other_target.revision = annotation.revision;
    assert!(!annotation.confirms(&other_target));
}

#[test]
fn terminal_dependency_failure_needs_you_while_dependency_wait_stays_queued() {
    let mut facts = facts();
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Armed;
    let status = TaskStatus::new(
        TaskState::Abandoned,
        None,
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![],
        1002,
    )
    .unwrap();
    facts.ordinary = facts
        .ordinary
        .with_status(status)
        .unwrap()
        .with_abandon_code(Some("INTEGRATION_DEPENDENCY_NOT_INTEGRATED".into()))
        .unwrap();
    let view = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(view.workflow_state, Some(WorkflowState::NeedsYou));
    assert!(view.attention);
}

#[test]
fn armed_lost_human_outcome_needs_you_instead_of_terminal_done() {
    let mut facts = facts();
    let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
    snapshot.state = IntegrationStatus::Armed;
    let status = TaskStatus::new(
        TaskState::Lost,
        Some(mac_worker::test_support::task::model::TaskOutcome::Lost),
        None,
        false,
        None,
        None,
        vec![],
        vec![],
        None,
        vec![],
        1002,
    )
    .unwrap();
    facts.ordinary = facts.ordinary.with_status(status).unwrap();
    let view = project_integration(Some(&snapshot), &facts).unwrap();
    assert_eq!(view.workflow_state, Some(WorkflowState::NeedsYou));
    assert!(view.attention);
    assert_eq!(
        serde_json::to_value(view.review_state).unwrap(),
        "ready_for_follow_up"
    );
}

mod route {
    use super::*;
    use mac_worker::test_support::{
        dashboard::{
            model::{ApiError, DashboardError, DashboardLogChunk, DashboardQueueEntry},
            service::{
                Clock, DashboardDataSource, DashboardService, MonotonicClock,
                WorkerObservationResult,
            },
            task::{
                DashboardTaskMutationSource, DashboardTaskSource, TaskIntegrationRequest,
                TaskMutationRequest,
            },
            web::{DashboardHttpServer, DashboardHttpState},
        },
        host::job::LogStream,
        task::{
            model::{TaskId, TurnId},
            view::{TaskDetailProjection, TaskFreshness, project_task_detail},
        },
    };
    use std::{
        io::{Read, Write},
        net::TcpStream,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    struct Source {
        calls: AtomicUsize,
    }
    impl DashboardDataSource for Source {
        fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
            Ok(vec![])
        }
        fn collect_workers(&self, _: Duration) -> Vec<WorkerObservationResult> {
            vec![]
        }
        fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
            Ok(vec![])
        }
    }
    impl DashboardTaskSource for Source {
        fn task_detail(&self, _: TaskId) -> Result<TaskDetailProjection, ApiError> {
            Ok(detail())
        }
        fn read_task_log(
            &self,
            _: TaskId,
            _: TurnId,
            _: LogStream,
            _: u64,
            _: u32,
        ) -> Result<DashboardLogChunk, ApiError> {
            panic!("no host operation")
        }
    }
    impl DashboardTaskMutationSource for Source {
        fn reply(
            &self,
            _: TaskId,
            _: &TaskMutationRequest,
        ) -> Result<TaskDetailProjection, ApiError> {
            panic!("no ordinary mutation")
        }
        fn accept(
            &self,
            _: TaskId,
            _: &TaskMutationRequest,
        ) -> Result<TaskDetailProjection, ApiError> {
            panic!("no ordinary mutation")
        }
        fn integrate(
            &self,
            id: TaskId,
            request: &TaskIntegrationRequest,
        ) -> Result<TaskDetailProjection, ApiError> {
            assert_eq!(id, fixture_task());
            assert_eq!(request.integration.task_id, id);
            assert_eq!(
                request.expected_integration_id,
                sample_record(id, fixture_source(), "main")
                    .snapshot
                    .integration_id
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            match request.integration.expected.0 {
                1 => Ok(detail()),
                2 => Err(ApiError::new(
                    "TASK_REVISION_CONFLICT",
                    "integration changed",
                )),
                _ => Err(ApiError::new(
                    "INTEGRATION_UNAVAILABLE",
                    "compatible owner unavailable",
                )),
            }
        }
    }
    struct FixedClock;
    impl Clock for FixedClock {
        fn now_millis(&self) -> u64 {
            1000
        }
    }
    impl MonotonicClock for FixedClock {
        fn now_millis(&self) -> u64 {
            0
        }
    }
    fn detail() -> TaskDetailProjection {
        let facts = facts();
        project_task_detail(
            &facts.ordinary,
            facts.ordinary.status(),
            None,
            TaskFreshness::Current,
        )
        .unwrap()
    }
    fn post(host: &str, body: &serde_json::Value, protected: bool) -> (u16, serde_json::Value) {
        let bytes = serde_json::to_vec(body).unwrap();
        let mut stream = TcpStream::connect(host).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        write!(stream, "POST /api/v1/tasks/{}/integrate HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n", fixture_task(), bytes.len()).unwrap();
        if protected {
            write!(stream, "Origin: http://{host}\r\nx-mac-worker-task: 1\r\n").unwrap();
        }
        stream.write_all(b"\r\n").unwrap();
        stream.write_all(&bytes).unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        let (headers, body) = reply.split_once("\r\n\r\n").unwrap();
        (
            headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
            serde_json::from_str(body).unwrap(),
        )
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn redrive_route_uses_revision_fenced_adapter_and_preserves_unavailable_errors() {
        let source = Arc::new(Source {
            calls: AtomicUsize::new(0),
        });
        let service = Arc::new(DashboardService::new(
            Source {
                calls: AtomicUsize::new(0),
            },
            FixedClock,
            FixedClock,
        ));
        let server = DashboardHttpServer::bind(
            None,
            Arc::new(DashboardHttpState {
                service,
                task_source: source.clone(),
                settings_source: None,
                mutation_source: Some(source.clone()),
            }),
        )
        .await
        .unwrap();
        let host = server.local_url().trim_start_matches("http://").to_owned();
        let mut body = serde_json::json!({"expected": {"expected_task_id":fixture_task(), "expected_turn_id":fixture_source(),
            "expected_turn_count":1,"expected_head_oid":fixture_head(), "expected_updated_at_millis":1001,"expected_state":"open"},
            "expected_integration_id":sample_record(fixture_task(), fixture_source(), "main").snapshot.integration_id,
            "integration":{"task_id":fixture_task(),"expected":1,"request_id":"f".repeat(32)}});
        assert_eq!(post(&host, &body, false).0, 400);
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
        assert_eq!(post(&host, &body, true).0, 200);
        body["integration"]["expected"] = serde_json::json!(2);
        assert_eq!(post(&host, &body, true).0, 409);
        body["integration"]["expected"] = serde_json::json!(3);
        let unavailable = post(&host, &body, true);
        assert_eq!(unavailable.0, 503);
        assert_eq!(unavailable.1["error"]["code"], "INTEGRATION_UNAVAILABLE");
        assert_eq!(source.calls.load(Ordering::SeqCst), 3);
        server.shutdown().await.unwrap();
    }
}

mod owner_adapter {
    use super::*;
    use mac_worker::test_support::{
        client_state::ClientStateStore,
        core::{config::Config, error::WorkerError, paths::PathLayout},
        dashboard::{
            model::ApiError,
            source::DashboardRemoteReader,
            task::{
                DashboardIntegrationSource, DashboardTaskMutationSource, DashboardTaskSource,
                MacWorkerTaskMutationSource, MacWorkerTaskSource, TaskIntegrationRequest,
                TaskMutationRequest,
            },
        },
        host::job::{LogChunk, LogStream},
        task::{
            model::{TaskId, TurnId},
            store::{TaskStatusRequest, TaskStatusResponse},
        },
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct NoRemote;
    struct NoProcesses;
    impl mac_worker::test_support::host::process::ProcessRunner for NoProcesses {
        fn run(
            &self,
            _: &mac_worker::test_support::host::process::ProcessRequest,
        ) -> Result<mac_worker::test_support::host::process::ProcessResult, WorkerError> {
            Err(WorkerError::Protocol(
                "no process may execute in the fixture".into(),
            ))
        }
    }
    impl DashboardRemoteReader for NoRemote {
        fn task_status(
            &self,
            _: &mac_worker::test_support::core::config::WorkerEntry,
            _: &TaskStatusRequest,
        ) -> Result<TaskStatusResponse, WorkerError> {
            Err(WorkerError::Unavailable("fixture offline".into()))
        }
        fn log_chunk(
            &self,
            _: &mac_worker::test_support::core::config::WorkerEntry,
            _: TurnId,
            _: LogStream,
            _: u64,
            _: u32,
        ) -> Result<LogChunk, WorkerError> {
            panic!("no host operation")
        }
    }
    struct Adapter {
        snapshot: Mutex<IntegrationSnapshot>,
        calls: AtomicUsize,
    }
    impl DashboardIntegrationSource for Adapter {
        fn snapshot(&self, _: TaskId) -> Result<Option<IntegrationSnapshot>, WorkerError> {
            Ok(Some(self.snapshot.lock().unwrap().clone()))
        }
        fn redrive(
            &self,
            request: &IntegrationRedriveRequest,
        ) -> Result<IntegrationSnapshot, WorkerError> {
            let mut snapshot = self.snapshot.lock().unwrap();
            assert_eq!(request.expected, snapshot.revision);
            self.calls.fetch_add(1, Ordering::SeqCst);
            snapshot.epoch += 1;
            snapshot.revision = snapshot.revision.next().unwrap();
            snapshot.state = IntegrationStatus::Pending;
            snapshot.blocked_code = None;
            Ok(snapshot.clone())
        }
    }
    #[test]
    fn dashboard_reads_and_mutations_use_durable_companion_and_reject_changed_identity() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let paths = PathLayout {
            config: base.join("config"),
            state: base.join("state"),
            cache: base.join("cache"),
            data: base.join("data"),
        };
        let state = Arc::new(ClientStateStore::open(&paths.state).unwrap());
        let config = Arc::new(Config::parse("version = 1\n[[workers]]\nname = \"fixture-worker\"\nssh = \"fixture\"\nslots = 1\n").unwrap());
        let ordinary = sample_ordinary(fixture_task(), fixture_source());
        state.create_task(ordinary.clone()).unwrap();
        let mut snapshot = sample_record(fixture_task(), fixture_source(), "main").snapshot;
        snapshot.state = IntegrationStatus::Blocked;
        snapshot.blocked_code = Some(IntegrationCode::IntegrationChecksFailed);
        let adapter = Arc::new(Adapter {
            snapshot: Mutex::new(snapshot.clone()),
            calls: AtomicUsize::new(0),
        });
        let reader = MacWorkerTaskSource::new(config.clone(), state.clone(), Arc::new(NoRemote))
            .with_integrations(adapter.clone());
        let detail = reader.task_detail(fixture_task()).unwrap();
        assert_eq!(detail.workflow_state, Some(WorkflowState::NeedsYou));
        assert_eq!(detail.integration, detail.task.integration);
        let mutations = MacWorkerTaskMutationSource::new(config, state, paths)
            .with_integrations(adapter.clone())
            .with_process_runner(Arc::new(NoProcesses));
        let mut request = TaskIntegrationRequest {
            expected: TaskMutationRequest {
                expected_integration_id: None,
                expected_integration_revision: None,
                message: None,
                expected_task_id: fixture_task(),
                expected_turn_id: Some(fixture_source()),
                expected_turn_count: 1,
                expected_head_oid: Some(fixture_head()),
                expected_updated_at_millis: 1001,
                expected_state: TaskState::Open,
            },
            expected_integration_id: snapshot.integration_id,
            integration: IntegrationRedriveRequest {
                task_id: fixture_task(),
                expected: snapshot.revision,
                request_id: "f".repeat(32),
            },
        };
        assert_eq!(
            mutations
                .accept(fixture_task(), &request.expected)
                .unwrap_err()
                .code,
            "TASK_REVISION_CONFLICT"
        );
        let mut close = request.expected.clone();
        close.expected_integration_id = Some(snapshot.integration_id);
        close.expected_integration_revision = Some(snapshot.revision);
        assert_eq!(
            mutations.accept(fixture_task(), &close).unwrap_err().code,
            "INTEGRATION_UNAVAILABLE"
        );
        request.expected_integration_id =
            sample_record(fixture_task(), fixture_source(), "release")
                .snapshot
                .integration_id;
        let error: ApiError = mutations.integrate(fixture_task(), &request).unwrap_err();
        assert_eq!(error.code, "TASK_REVISION_CONFLICT");
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
        request.expected_integration_id = snapshot.integration_id;
        let pending = mutations.integrate(fixture_task(), &request).unwrap();
        assert_eq!(pending.workflow_state, Some(WorkflowState::Integrating));
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            mutations
                .integrate(fixture_task(), &request)
                .unwrap_err()
                .code,
            "TASK_REVISION_CONFLICT"
        );
        let unavailable = MacWorkerTaskMutationSource::new(
            reader.config.clone(),
            reader.local_tasks.clone(),
            PathLayout {
                config: base.join("config"),
                state: base.join("state"),
                cache: base.join("cache"),
                data: base.join("data"),
            },
        );
        assert_eq!(
            unavailable
                .integrate(fixture_task(), &request)
                .unwrap_err()
                .code,
            "INTEGRATION_UNAVAILABLE"
        );
    }
}
