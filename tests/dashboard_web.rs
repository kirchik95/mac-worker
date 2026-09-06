use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::TcpStream,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use mac_worker::{
    dashboard::{
        cache::Observation,
        model::{
            ApiError, DASHBOARD_API_VERSION, DashboardCommandMode, DashboardCommandSummary,
            DashboardError, DashboardJob, DashboardJobState, DashboardLogChunk,
            DashboardMemoryPressure, DashboardQueueEntry, DashboardSlotState, DashboardWorker,
            Freshness, SlotSummary, SystemSummary, WorkerHealth,
        },
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        task::DashboardTaskSource,
        web::{DashboardHttpServer, DashboardHttpState, DashboardLogSource},
    },
    job::{JobId, LogStream},
    task::{BaseOid, BranchName, RunId, RunnerState, TaskId, TaskState, TurnId, TurnTerminal},
    task_view::{
        TaskDetailProjection, TaskFreshness, TaskListRow, TaskTimelineEvent, TaskTurnProjection,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_router_serves_embedded_assets_snapshot_and_security_headers() {
    let (server, _logs) = started_server().await;
    let host = listener_host(&server);

    let shell = request(&host, "/", &host);
    assert_eq!(shell.status, 200);
    assert!(content_type(&shell).starts_with("text/html"));
    assert_security(&shell);
    let shell_body = body_text(&shell);
    assert!(shell_body.contains("/assets/dashboard.css"));
    assert!(shell_body.contains("/assets/dashboard.mjs"));
    assert!(!shell_body.contains("http://"));
    assert!(!shell_body.contains("https://"));

    let asset = request(&host, "/assets/dashboard.mjs", &host);
    assert_eq!(asset.status, 200);
    assert!(content_type(&asset).starts_with("application/javascript"));
    assert_security(&asset);
    assert!(!body_text(&asset).contains("https://"));

    for name in [
        "plex-sans-regular.ttf",
        "plex-sans-medium.ttf",
        "plex-mono-regular.ttf",
    ] {
        let font = request(&host, &format!("/assets/{name}"), &host);
        assert_eq!(font.status, 200);
        assert_eq!(content_type(&font), "font/ttf");
        assert!(font.body.len() > 10_000);
        assert_security(&font);
    }
    assert_eq!(request(&host, "/assets/private.ttf", &host).status, 404);

    let snapshot = request(&host, "/api/v1/snapshot", &host);
    assert_eq!(snapshot.status, 200);
    assert_eq!(snapshot.header("cache-control"), Some("no-store"));
    assert_security(&snapshot);
    let snapshot_json: serde_json::Value = serde_json::from_slice(&snapshot.body).unwrap();
    assert_eq!(snapshot_json["api_version"], DASHBOARD_API_VERSION);
    assert_eq!(snapshot_json["queue"], serde_json::json!([]));

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_rejects_invalid_log_inputs_without_calling_the_log_source() {
    let (server, logs) = started_server().await;
    let host = listener_host(&server);
    let id = job_id(1);

    for path in [
        "/api/v1/jobs/not-an-id/logs?stream=stdout&offset=0&limit=1",
        &format!("/api/v1/jobs/{id}/logs?stream=merged&offset=0&limit=1"),
        &format!("/api/v1/jobs/{id}/logs?stream=stdout&offset=0&limit=0"),
        &format!("/api/v1/jobs/{id}/logs?stream=stdout&offset=0&limit=65537"),
    ] {
        let response = request(&host, path, &host);
        assert_eq!(response.status, 400, "{path}");
        assert_security(&response);
        assert!(error_code(&response).starts_with("INVALID_"));
    }
    assert_eq!(logs.log_calls(), 0);
    assert_eq!(logs.detail_calls(), 0);

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_projects_typed_detail_and_log_routes_and_preserves_not_found() {
    let (server, logs) = started_server().await;
    let host = listener_host(&server);
    let id = job_id(1);

    let detail = request(&host, &format!("/api/v1/jobs/{id}"), &host);
    assert_eq!(detail.status, 200);
    assert_security(&detail);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&detail.body).unwrap()["job_id"],
        id.to_string()
    );
    assert_eq!(logs.detail_calls(), 1);

    let log = request(
        &host,
        &format!("/api/v1/jobs/{id}/logs?stream=stderr&offset=7&limit=12"),
        &host,
    );
    assert_eq!(log.status, 200);
    assert_security(&log);
    let log_json: serde_json::Value = serde_json::from_slice(&log.body).unwrap();
    assert_eq!(log_json["stream"], "stderr");
    assert_eq!(log_json["offset"], 7);
    assert_eq!(log_json["next_offset"], 12);
    assert_eq!(logs.log_calls(), 1);

    let missing = request(&host, &format!("/api/v1/jobs/{}", job_id(2)), &host);
    assert_eq!(missing.status, 404);
    assert_security(&missing);
    assert_eq!(error_code(&missing), "JOB_NOT_FOUND");

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_detail_route_returns_safe_detail_and_preserves_legacy_routes() {
    let task_source = Arc::new(FixtureTaskSource::with_detail(fixture_detail()));
    let (server, _logs) = started_server_with_task_source(Arc::clone(&task_source)).await;
    let host = listener_host(&server);
    let task_id = fixture_task_id();

    let response = request(&host, &format!("/api/v1/tasks/{task_id}"), &host);
    assert_eq!(response.status, 200);
    assert_security(&response);
    assert_eq!(response.header("cache-control"), Some("no-store"));
    let json: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(json["task"]["task_id"], task_id.to_string());
    assert!(json["task"]["prompt"].is_null());
    assert!(
        json["fetch_command"]
            .as_str()
            .unwrap()
            .starts_with("worker task fetch ")
    );

    let legacy = request(&host, &format!("/api/v1/jobs/{}", job_id(1)), &host);
    assert_eq!(legacy.status, 200);
    assert_security(&legacy);
    assert_eq!(task_source.detail_calls(), 1);

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_log_route_validates_turn_membership_and_byte_ranges() {
    let task_source = Arc::new(FixtureTaskSource::with_log_chunk());
    let (server, _logs) = started_server_with_task_source(Arc::clone(&task_source)).await;
    let host = listener_host(&server);
    let task_id = fixture_task_id();
    let turn_id = fixture_turn_id();

    let good = request(
        &host,
        &format!("/api/v1/tasks/{task_id}/turns/{turn_id}/logs?stream=stdout&offset=3&limit=4"),
        &host,
    );
    assert_eq!(good.status, 200);
    assert_security(&good);
    let json: serde_json::Value = serde_json::from_slice(&good.body).unwrap();
    assert_eq!(json["offset"], 3);
    assert_eq!(json["next_offset"], 7);
    assert_eq!(
        task_source.log_requests(),
        vec![(task_id, turn_id, LogStream::Stdout, 3, 4)]
    );

    let invalid_id = request(&host, "/api/v1/tasks/not-an-id", &host);
    assert_eq!(invalid_id.status, 400);
    assert_eq!(error_code(&invalid_id), "INVALID_TASK_ID");

    let missing_turn = request(
        &host,
        &format!(
            "/api/v1/tasks/{task_id}/turns/018f0f4a6b5c7d8e9f00112233445568/logs?stream=stdout&offset=0&limit=1"
        ),
        &host,
    );
    assert_eq!(missing_turn.status, 404);
    assert_eq!(error_code(&missing_turn), "TURN_NOT_FOUND");

    let invalid_range = request(
        &host,
        &format!("/api/v1/tasks/{task_id}/turns/{turn_id}/logs?stream=stdout&offset=0&limit=0"),
        &host,
    );
    assert_eq!(invalid_range.status, 400);
    assert_eq!(error_code(&invalid_range), "INVALID_LOG_RANGE");

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_routes_keep_loopback_security_and_issue_no_mutations() {
    let task_source = Arc::new(FixtureTaskSource::with_detail(fixture_detail()));
    let (server, _logs) = started_server_with_task_source(Arc::clone(&task_source)).await;
    let host = listener_host(&server);

    let response = request(
        &host,
        &format!("/api/v1/tasks/{}", fixture_task_id()),
        "evil.example",
    );
    assert_eq!(response.status, 400);
    assert_eq!(error_code(&response), "INVALID_HOST");
    assert_security(&response);
    assert_eq!(response.header("cache-control"), Some("no-store"));
    assert_eq!(task_source.detail_calls(), 0);
    assert_eq!(task_source.log_calls(), 0);
    assert_eq!(task_source.mutation_calls(), 0);

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_rejects_a_host_that_does_not_match_the_actual_loopback_listener() {
    let (server, _logs) = started_server().await;
    let host = listener_host(&server);

    let response = request(&host, "/api/v1/snapshot", "localhost:9999");
    assert_eq!(response.status, 400);
    assert_security(&response);
    assert_eq!(error_code(&response), "INVALID_HOST");

    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_fixture_preserves_mixed_freshness_fifo_terminal_cursors_and_read_only_lifecycle() {
    let mutations = MutationRecorder::default();
    let terminal = terminal_job(job_id(3));
    let source = FixtureSource::new(terminal.clone(), mutations);
    let logs = Arc::new(FixtureLogs::new(terminal.clone()));
    let state = Arc::new(DashboardHttpState {
        service: Arc::new(DashboardService::new(
            source.clone(),
            FixedClock,
            FixedMonotonic,
        )),
        log_source: logs,
        task_source: Arc::new(FixtureTaskSource::default()),
        settings_source: None,
    });
    let server = DashboardHttpServer::bind(None, state).await.unwrap();
    let host = listener_host(&server);

    let warm = request(&host, "/api/v1/snapshot", &host);
    assert_eq!(warm.status, 200);

    let snapshot = request(&host, "/api/v1/snapshot", &host);
    assert_eq!(snapshot.status, 200);
    let snapshot_json: serde_json::Value = serde_json::from_slice(&snapshot.body).unwrap();
    assert_eq!(snapshot_json["workers"].as_array().unwrap().len(), 3);
    assert_eq!(snapshot_json["workers"][0]["freshness"], "current");
    assert_eq!(snapshot_json["workers"][1]["freshness"], "current");
    assert_eq!(snapshot_json["workers"][2]["freshness"], "stale");
    assert_eq!(snapshot_json["workers"][2]["observed_at_millis"], 1_000);
    assert_eq!(snapshot_json["queue"][0]["position"], 1);
    assert_eq!(
        snapshot_json["queue"][0]["blocking_code"],
        "NO_COMPATIBLE_IDLE_WORKER"
    );

    let recent = &snapshot_json["recent_jobs"][0];
    assert_eq!(recent["project_label"], serde_json::Value::Null);
    assert_eq!(recent["final_stdout_bytes"], 5);
    assert_eq!(recent["final_stderr_bytes"], 3);

    let detail = request(&host, &format!("/api/v1/jobs/{}", terminal.job_id), &host);
    assert_eq!(detail.status, 200);
    let detail_json: serde_json::Value = serde_json::from_slice(&detail.body).unwrap();
    assert_eq!(detail_json["project_label"], serde_json::Value::Null);

    let stdout_first = log_request(&host, terminal.job_id, "stdout", 0);
    let stdout_second = log_request(&host, terminal.job_id, "stdout", 3);
    let stderr_first = log_request(&host, terminal.job_id, "stderr", 0);
    let stderr_second = log_request(&host, terminal.job_id, "stderr", 2);
    assert_log_chunk(&stdout_first, 0, 3, "YWJj");
    assert_log_chunk(&stdout_second, 3, 5, "ZGU=");
    assert_log_chunk(&stderr_first, 0, 2, "eHk=");
    assert_log_chunk(&stderr_second, 2, 3, "eg==");

    let mut disconnected_client = TcpStream::connect(&host).unwrap();
    disconnected_client
        .write_all(b"GET /api/v1/snapshot HTTP/1.1\r\nHost: ")
        .unwrap();
    drop(disconnected_client);

    server.shutdown().await.unwrap();
    source.assert_no_mutations();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_http_clients_share_one_in_flight_snapshot_refresh() {
    let gate = Arc::new(CollectionGate::default());
    let source = CoalescingSource::new(Arc::clone(&gate));
    let monotonic = CountingMonotonic::default();
    let state = Arc::new(DashboardHttpState {
        service: Arc::new(DashboardService::new(
            source.clone(),
            FixedClock,
            monotonic.clone(),
        )),
        log_source: Arc::new(RecordingLogs::new(job(job_id(1)))),
        task_source: Arc::new(FixtureTaskSource::default()),
        settings_source: None,
    });
    let server = DashboardHttpServer::bind(None, state).await.unwrap();
    let host = listener_host(&server);

    let first_host = host.clone();
    let first = thread::spawn(move || request(&first_host, "/api/v1/snapshot", &first_host));
    gate.wait_until_entered();

    let second_host = host.clone();
    let second = thread::spawn(move || request(&second_host, "/api/v1/snapshot", &second_host));
    monotonic.wait_for_calls(3);
    gate.release();

    let first = first.join().unwrap();
    let second = second.join().unwrap();
    assert_eq!(first.status, 200);
    assert_eq!(second.status, 200);
    let first_json: serde_json::Value = serde_json::from_slice(&first.body).unwrap();
    let second_json: serde_json::Value = serde_json::from_slice(&second.body).unwrap();
    assert_eq!(first_json["revision"], 1);
    assert_eq!(second_json["revision"], 1);
    assert_eq!(source.collect_calls(), 1);

    server.shutdown().await.unwrap();
}

async fn started_server() -> (DashboardHttpServer, Arc<RecordingLogs>) {
    started_server_with_task_source(Arc::new(FixtureTaskSource::default())).await
}

async fn started_server_with_task_source(
    task_source: Arc<FixtureTaskSource>,
) -> (DashboardHttpServer, Arc<RecordingLogs>) {
    let source = FakeSource;
    let service = Arc::new(DashboardService::new(source, FixedClock, FixedMonotonic));
    let logs = Arc::new(RecordingLogs::new(job(job_id(1))));
    let state = Arc::new(DashboardHttpState {
        service,
        log_source: logs.clone(),
        task_source,
        settings_source: None,
    });
    let server = DashboardHttpServer::bind(None, state).await.unwrap();
    (server, logs)
}

fn listener_host(server: &DashboardHttpServer) -> String {
    server
        .local_url()
        .strip_prefix("http://")
        .unwrap()
        .to_owned()
}

fn request(address: &str, path: &str, host: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    HttpResponse::parse(raw)
}

fn log_request(address: &str, job_id: JobId, stream: &str, offset: u64) -> serde_json::Value {
    let response = request(
        address,
        &format!("/api/v1/jobs/{job_id}/logs?stream={stream}&offset={offset}&limit=65536"),
        address,
    );
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

fn assert_log_chunk(chunk: &serde_json::Value, offset: u64, next_offset: u64, data: &str) {
    assert_eq!(chunk["offset"], offset);
    assert_eq!(chunk["next_offset"], next_offset);
    assert_eq!(chunk["data"], data);
}

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn parse(raw: Vec<u8>) -> Self {
        let split = raw
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap_or_else(|| {
                panic!(
                    "HTTP response lacked a header terminator: {:?}",
                    String::from_utf8_lossy(&raw)
                )
            });
        let (head, body) = raw.split_at(split + 4);
        let head = std::str::from_utf8(head).unwrap();
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
            .collect();
        Self {
            status,
            headers,
            body: body.to_vec(),
        }
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
    }
}

fn content_type(response: &HttpResponse) -> &str {
    response.header("content-type").unwrap()
}

fn body_text(response: &HttpResponse) -> &str {
    std::str::from_utf8(&response.body).unwrap()
}

fn error_code(response: &HttpResponse) -> String {
    serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["error"]["code"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_security(response: &HttpResponse) {
    assert!(
        response
            .header("content-security-policy")
            .is_some_and(|value| value.contains("default-src 'self'"))
    );
    assert_eq!(response.header("x-content-type-options"), Some("nosniff"));
    assert_eq!(response.header("referrer-policy"), Some("no-referrer"));
    assert!(response.header("access-control-allow-origin").is_none());
}

struct FakeSource;

impl DashboardDataSource for FakeSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(vec!["mini-1".into()])
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        vec![WorkerObservationResult::Current(Observation {
            worker: DashboardWorker {
                name: "mini-1".into(),
                health: WorkerHealth::Ready,
                freshness: Freshness::Current,
                observed_at_millis: Some(1_000),
                hostname: Some("mini-1.local".into()),
                agent_facts: None,
                slot: SlotSummary {
                    state: DashboardSlotState::Idle,
                    capacity: 1,
                    active_job_id: None,
                },
                capabilities: vec!["swift".into()],
                missing_capabilities: Vec::new(),
                system: SystemSummary {
                    free_disk_bytes: Some(10),
                    total_disk_bytes: Some(100),
                    memory_pressure: Some(DashboardMemoryPressure::Normal),
                    swap_used_bytes: Some(0),
                    cpu_busy_percent: None,
                },
                error: None,
                active_task: None,
            },
            observed_at_millis: 1_000,
            cpu_counters: None,
        })]
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }

    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>> {
        Vec::new()
    }

    fn queue_entries(
        &self,
    ) -> Result<Vec<mac_worker::dashboard::model::DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1_000
    }
}

struct FixedMonotonic;

impl MonotonicClock for FixedMonotonic {
    fn now_millis(&self) -> u64 {
        0
    }
}

#[derive(Clone)]
struct FixtureSource {
    observations: Arc<Mutex<VecDeque<Vec<WorkerObservationResult>>>>,
    terminal: DashboardJob,
    mutations: MutationRecorder,
}

impl FixtureSource {
    fn new(terminal: DashboardJob, mutations: MutationRecorder) -> Self {
        Self {
            observations: Arc::new(Mutex::new(VecDeque::from([
                vec![
                    WorkerObservationResult::Current(worker_observation("mini-1", 1_000)),
                    WorkerObservationResult::Current(worker_observation("mini-2", 1_000)),
                    WorkerObservationResult::Current(worker_observation("mini-3", 1_000)),
                ],
                vec![
                    WorkerObservationResult::Current(worker_observation("mini-1", 1_001)),
                    WorkerObservationResult::Current(worker_observation("mini-2", 1_001)),
                    WorkerObservationResult::Failed {
                        worker_name: "mini-3".into(),
                        error: DashboardError::new(
                            "WORKER_UNAVAILABLE",
                            "worker observation is unavailable",
                        ),
                    },
                ],
            ]))),
            terminal,
            mutations,
        }
    }

    fn assert_no_mutations(&self) {
        self.mutations.assert_no_calls();
    }
}

impl DashboardDataSource for FixtureSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(vec!["mini-1".into(), "mini-2".into(), "mini-3".into()])
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        self.observations
            .lock()
            .unwrap()
            .pop_front()
            .expect("fixture contains an observation row for each snapshot")
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(vec![self.terminal.clone()])
    }

    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>> {
        Vec::new()
    }

    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        Ok(vec![DashboardQueueEntry {
            position: 1,
            job_id: job_id(4),
            entry_kind: mac_worker::dashboard::model::DashboardQueueEntryKind::Batch,
            task_id: None,
            turn_id: None,
            run_id: None,
            run_max_parallel: None,
            pinned_worker: None,
            project_id: "queued-project".into(),
            worktree_id: "queued-worktree".into(),
            project_label: None,
            command_summary: DashboardCommandSummary {
                mode: DashboardCommandMode::Argv,
                arg_count: Some(2),
            },
            created_at_millis: 1_001,
            requirements: vec!["darwin-arm64".into()],
            blocking_code: "NO_COMPATIBLE_IDLE_WORKER".into(),
        }])
    }
}

#[derive(Clone)]
struct MutationRecorder(Arc<MutationState>);

#[derive(Default)]
struct MutationState {
    submit_calls: AtomicUsize,
    cancel_calls: AtomicUsize,
    retry_calls: AtomicUsize,
    delete_calls: AtomicUsize,
    lease_calls: AtomicUsize,
}

impl Default for MutationRecorder {
    fn default() -> Self {
        Self(Arc::new(MutationState::default()))
    }
}

impl MutationRecorder {
    fn assert_no_calls(&self) {
        assert_eq!(
            [
                self.0.submit_calls.load(Ordering::SeqCst),
                self.0.cancel_calls.load(Ordering::SeqCst),
                self.0.retry_calls.load(Ordering::SeqCst),
                self.0.delete_calls.load(Ordering::SeqCst),
                self.0.lease_calls.load(Ordering::SeqCst),
            ],
            [0; 5],
            "dashboard requests, disconnects, and shutdown must not invoke fake submit, cancel, retry, delete, or lease operations"
        );
    }
}

struct FixtureLogs {
    terminal: DashboardJob,
}

impl FixtureLogs {
    fn new(terminal: DashboardJob) -> Self {
        Self { terminal }
    }
}

impl DashboardLogSource for FixtureLogs {
    fn job_detail(&self, job_id: JobId) -> Result<DashboardJob, ApiError> {
        (job_id == self.terminal.job_id)
            .then(|| self.terminal.clone())
            .ok_or_else(|| ApiError::new("JOB_NOT_FOUND", "job is not retained"))
    }

    fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        if job_id != self.terminal.job_id {
            return Err(ApiError::new("JOB_NOT_FOUND", "job is not retained"));
        }
        let bytes = match (stream, offset) {
            (LogStream::Stdout, 0) => b"abc".to_vec(),
            (LogStream::Stdout, 3) => b"de".to_vec(),
            (LogStream::Stderr, 0) => b"xy".to_vec(),
            (LogStream::Stderr, 2) => b"z".to_vec(),
            _ => Vec::new(),
        };
        DashboardLogChunk::from_bytes(stream, offset, bytes)
            .map_err(|_| ApiError::new("LOG_UNAVAILABLE", "log is unavailable"))
    }
}

#[derive(Clone)]
struct CoalescingSource {
    gate: Arc<CollectionGate>,
    collect_calls: Arc<AtomicUsize>,
}

impl CoalescingSource {
    fn new(gate: Arc<CollectionGate>) -> Self {
        Self {
            gate,
            collect_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn collect_calls(&self) -> usize {
        self.collect_calls.load(Ordering::SeqCst)
    }
}

impl DashboardDataSource for CoalescingSource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(vec!["mini-1".into()])
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        self.collect_calls.fetch_add(1, Ordering::SeqCst);
        self.gate.enter_and_wait();
        vec![WorkerObservationResult::Current(worker_observation(
            "mini-1", 1_000,
        ))]
    }

    fn local_jobs(&self) -> Result<Vec<DashboardJob>, DashboardError> {
        Ok(Vec::new())
    }

    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<DashboardJob, DashboardError>> {
        Vec::new()
    }

    fn queue_entries(&self) -> Result<Vec<DashboardQueueEntry>, DashboardError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct CollectionGate {
    state: Mutex<CollectionGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct CollectionGateState {
    entered: bool,
    released: bool,
}

impl CollectionGate {
    fn enter_and_wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

#[derive(Clone, Default)]
struct CountingMonotonic(Arc<CountingMonotonicState>);

#[derive(Default)]
struct CountingMonotonicState {
    calls: Mutex<usize>,
    changed: Condvar,
}

impl CountingMonotonic {
    fn wait_for_calls(&self, minimum: usize) {
        let mut calls = self.0.calls.lock().unwrap();
        while *calls < minimum {
            calls = self.0.changed.wait(calls).unwrap();
        }
    }
}

impl MonotonicClock for CountingMonotonic {
    fn now_millis(&self) -> u64 {
        let mut calls = self.0.calls.lock().unwrap();
        *calls += 1;
        self.0.changed.notify_all();
        0
    }
}

fn worker_observation(name: &str, observed_at_millis: u64) -> Observation {
    Observation {
        worker: DashboardWorker {
            name: name.into(),
            health: WorkerHealth::Ready,
            freshness: Freshness::Current,
            observed_at_millis: Some(observed_at_millis),
            hostname: Some(format!("{name}.local")),
            agent_facts: None,
            slot: SlotSummary {
                state: DashboardSlotState::Idle,
                capacity: 1,
                active_job_id: None,
            },
            capabilities: vec!["darwin-arm64".into()],
            missing_capabilities: Vec::new(),
            system: SystemSummary {
                free_disk_bytes: Some(10),
                total_disk_bytes: Some(100),
                memory_pressure: Some(DashboardMemoryPressure::Normal),
                swap_used_bytes: Some(0),
                cpu_busy_percent: None,
            },
            error: None,
            active_task: None,
        },
        observed_at_millis,
        cpu_counters: None,
    }
}

struct RecordingLogs {
    job: DashboardJob,
    detail_calls: AtomicUsize,
    log_calls: AtomicUsize,
    seen_log_requests: Mutex<Vec<(JobId, LogStream, u64, u32)>>,
}

impl RecordingLogs {
    fn new(job: DashboardJob) -> Self {
        Self {
            job,
            detail_calls: AtomicUsize::new(0),
            log_calls: AtomicUsize::new(0),
            seen_log_requests: Mutex::new(Vec::new()),
        }
    }

    fn detail_calls(&self) -> usize {
        self.detail_calls.load(Ordering::SeqCst)
    }

    fn log_calls(&self) -> usize {
        self.log_calls.load(Ordering::SeqCst)
    }
}

impl DashboardLogSource for RecordingLogs {
    fn job_detail(&self, job_id: JobId) -> Result<DashboardJob, ApiError> {
        self.detail_calls.fetch_add(1, Ordering::SeqCst);
        (job_id == self.job.job_id)
            .then(|| self.job.clone())
            .ok_or_else(|| ApiError::new("JOB_NOT_FOUND", "job is not retained"))
    }

    fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        self.log_calls.fetch_add(1, Ordering::SeqCst);
        self.seen_log_requests
            .lock()
            .unwrap()
            .push((job_id, stream, offset, limit));
        if job_id != self.job.job_id {
            return Err(ApiError::new("JOB_NOT_FOUND", "job is not retained"));
        }
        DashboardLogChunk::from_bytes(stream, offset, b"hello".to_vec())
            .map_err(|_| ApiError::new("LOG_UNAVAILABLE", "log is unavailable"))
    }
}

type TaskLogRequest = (TaskId, TurnId, LogStream, u64, u32);

#[derive(Default)]
struct FixtureTaskSource {
    detail: Option<Result<TaskDetailProjection, ApiError>>,
    log_chunk: Option<Result<DashboardLogChunk, ApiError>>,
    detail_calls: AtomicUsize,
    log_calls: AtomicUsize,
    seen_log_requests: Mutex<Vec<TaskLogRequest>>,
    mutation_calls: AtomicUsize,
}

impl FixtureTaskSource {
    fn with_detail(detail: TaskDetailProjection) -> Self {
        Self {
            detail: Some(Ok(detail)),
            ..Self::default()
        }
    }

    fn with_log_chunk() -> Self {
        let chunk = DashboardLogChunk::from_bytes(LogStream::Stdout, 3, b"data".to_vec())
            .map_err(|_| ApiError::new("LOG_UNAVAILABLE", "log is unavailable"));
        Self {
            detail: Some(Ok(fixture_detail())),
            log_chunk: Some(chunk),
            ..Self::default()
        }
    }

    fn detail_calls(&self) -> usize {
        self.detail_calls.load(Ordering::SeqCst)
    }

    fn log_calls(&self) -> usize {
        self.log_calls.load(Ordering::SeqCst)
    }

    fn log_requests(&self) -> Vec<TaskLogRequest> {
        self.seen_log_requests.lock().unwrap().clone()
    }

    fn mutation_calls(&self) -> usize {
        self.mutation_calls.load(Ordering::SeqCst)
    }
}

impl DashboardTaskSource for FixtureTaskSource {
    fn task_detail(&self, task_id: TaskId) -> Result<TaskDetailProjection, ApiError> {
        self.detail_calls.fetch_add(1, Ordering::SeqCst);
        match self.detail.clone() {
            Some(Ok(detail)) if detail.task.task_id == task_id => Ok(detail),
            Some(Ok(_)) | None => Err(ApiError::new(
                "TASK_NOT_FOUND",
                "task is not present in the fixture",
            )),
            Some(Err(error)) => Err(error),
        }
    }

    fn read_task_log(
        &self,
        task_id: TaskId,
        turn_id: TurnId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        self.log_calls.fetch_add(1, Ordering::SeqCst);
        self.seen_log_requests
            .lock()
            .unwrap()
            .push((task_id, turn_id, stream, offset, limit));
        if task_id != fixture_task_id() {
            return Err(ApiError::new(
                "TASK_NOT_FOUND",
                "task is not present in the fixture",
            ));
        }
        if turn_id != fixture_turn_id() {
            return Err(ApiError::new(
                "TURN_NOT_FOUND",
                "turn is not present in the requested task",
            ));
        }
        self.log_chunk.clone().unwrap_or_else(|| {
            Err(ApiError::new(
                "TASK_SOURCE_FAILED",
                "task log source is unavailable",
            ))
        })
    }
}

fn fixture_task_id() -> TaskId {
    "018f0f4a6b5c7d8e9f00112233445566"
        .parse()
        .expect("valid task ID fixture")
}

fn fixture_turn_id() -> TurnId {
    "018f0f4a6b5c7d8e9f00112233445567"
        .parse()
        .expect("valid turn ID fixture")
}

fn fixture_detail() -> TaskDetailProjection {
    let task_id = fixture_task_id();
    let turn_id = fixture_turn_id();
    let run_id: RunId = "118f0f4a6b5c7d8e9f00112233445566"
        .parse()
        .expect("valid run ID fixture");
    let turn = TaskTurnProjection {
        turn_number: 1,
        turn_id,
        terminal: Some(TurnTerminal::Succeeded),
        outcome: None,
        agent_committed: Some(true),
        log_truncated: false,
        started_at_millis: Some(1_000),
        ended_at_millis: Some(2_000),
    };
    TaskDetailProjection {
        task: TaskListRow {
            task_id,
            run_id: Some(run_id),
            run_position: Some(1),
            title: "Repair login".into(),
            agent: "codex".into(),
            model: None,
            effort: None,
            permissions: Some("workspace".into()),
            env_profile: None,
            state: TaskState::Active,
            blocking_code: None,
            last_outcome: None,
            worker: Some("mini-1".into()),
            branch: BranchName::for_task(task_id),
            turn_count: 1,
            runner: Some(RunnerState::Live),
            freshness: TaskFreshness::Current,
            created_at_millis: 900,
            updated_at_millis: 1_500,
            active_turn_id: Some(turn_id),
        },
        project_id: "a".repeat(64),
        worktree_id: "b".repeat(64),
        base_oid: Some(
            "0123456789abcdef0123456789abcdef01234567"
                .parse::<BaseOid>()
                .expect("valid base OID fixture"),
        ),
        head_oid: None,
        session_present: true,
        summary: Some("safe summary".into()),
        questions: vec!["safe question".into()],
        files_changed: vec!["src/login.rs".into()],
        diff_stat: Some("1 file changed".into()),
        fetch_command: format!("worker task fetch {task_id}"),
        turns: vec![turn.clone()],
        timeline: vec![TaskTimelineEvent {
            turn_number: turn.turn_number,
            turn_id: turn.turn_id,
            outcome: turn.outcome.clone(),
            started_at_millis: turn.started_at_millis,
            ended_at_millis: turn.ended_at_millis,
            terminal: turn.terminal,
        }],
    }
}

fn job(job_id: JobId) -> DashboardJob {
    DashboardJob {
        job_id,
        worker_name: "mini-1".into(),
        project_id: "project-1".into(),
        worktree_id: "worktree-1".into(),
        project_label: None,
        manifest_digest: "a".repeat(64),
        command_summary: DashboardCommandSummary {
            mode: DashboardCommandMode::Argv,
            arg_count: Some(2),
        },
        resource_class: "default".into(),
        created_at_millis: 1,
        updated_at_millis: 2,
        state: DashboardJobState::Running,
        exit_code: None,
        terminating_signal: None,
        final_stdout_bytes: None,
        final_stderr_bytes: None,
        artifact_status: None,
        remote_uncertainty: None,
    }
}

fn terminal_job(job_id: JobId) -> DashboardJob {
    let mut job = job(job_id);
    job.project_label = None;
    job.state = DashboardJobState::Succeeded;
    job.exit_code = Some(0);
    job.final_stdout_bytes = Some(5);
    job.final_stderr_bytes = Some(3);
    job
}

fn job_id(value: u128) -> JobId {
    format!("{value:032x}").parse().unwrap()
}
