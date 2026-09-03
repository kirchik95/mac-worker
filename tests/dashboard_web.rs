use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use mac_worker::{
    dashboard::{
        cache::Observation,
        model::{
            ApiError, DASHBOARD_API_VERSION, DashboardCommandMode, DashboardCommandSummary,
            DashboardError, DashboardJob, DashboardJobState, DashboardLogChunk,
            DashboardMemoryPressure, DashboardSlotState, DashboardWorker, Freshness, SlotSummary,
            SystemSummary, WorkerHealth,
        },
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        web::{DashboardHttpServer, DashboardHttpState, DashboardLogSource},
    },
    job::{JobId, LogStream},
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
async fn router_rejects_a_host_that_does_not_match_the_actual_loopback_listener() {
    let (server, _logs) = started_server().await;
    let host = listener_host(&server);

    let response = request(&host, "/api/v1/snapshot", "localhost:9999");
    assert_eq!(response.status, 400);
    assert_security(&response);
    assert_eq!(error_code(&response), "INVALID_HOST");

    server.shutdown().await.unwrap();
}

async fn started_server() -> (DashboardHttpServer, Arc<RecordingLogs>) {
    let source = FakeSource;
    let service = Arc::new(DashboardService::new(source, FixedClock, FixedMonotonic));
    let logs = Arc::new(RecordingLogs::new(job(job_id(1))));
    let state = Arc::new(DashboardHttpState {
        service,
        log_source: logs.clone(),
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

fn job_id(value: u128) -> JobId {
    format!("{value:032x}").parse().unwrap()
}
