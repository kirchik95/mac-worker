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
    agent_settings::{AgentDefaultSettings, AgentSettingsList, AgentSettingsSaveRequest},
    dashboard::{
        model::{ApiError, DashboardJob, DashboardLogChunk, DashboardQueueEntry},
        service::{
            Clock, DashboardDataSource, DashboardService, DashboardTaskCollection, MonotonicClock,
            WorkerObservationResult,
        },
        settings::DashboardSettingsSource,
        task::DashboardTaskSource,
        web::{DashboardHttpServer, DashboardHttpState, DashboardLogSource},
    },
    job::{JobId, LogStream},
    task::{TaskId, TurnId},
    task_view::TaskListProjection,
};

#[derive(Clone)]
struct SettingsFixture {
    saves: Arc<AtomicUsize>,
    result: Arc<Mutex<Result<AgentDefaultSettings, ApiError>>>,
}

impl SettingsFixture {
    fn new() -> Self {
        Self {
            saves: Arc::new(AtomicUsize::new(0)),
            result: Arc::new(Mutex::new(Ok(entry()))),
        }
    }
}

impl DashboardSettingsSource for SettingsFixture {
    fn worker_exists(&self, worker_name: &str) -> bool {
        worker_name == "mini-1"
    }

    fn read(&self, worker_name: &str) -> Result<AgentSettingsList, ApiError> {
        if worker_name != "mini-1" {
            return Err(ApiError::new(
                "WORKER_NOT_FOUND",
                "configured worker was not found",
            ));
        }
        Ok(AgentSettingsList {
            agents: vec![entry()],
        })
    }

    fn save(
        &self,
        worker_name: &str,
        _request: &AgentSettingsSaveRequest,
    ) -> Result<AgentDefaultSettings, ApiError> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        if worker_name != "mini-1" {
            return Err(ApiError::new(
                "WORKER_NOT_FOUND",
                "configured worker was not found",
            ));
        }
        self.result.lock().unwrap().clone()
    }
}

fn entry() -> AgentDefaultSettings {
    AgentDefaultSettings {
        agent: "codex".into(),
        model: Some("gpt-test".into()),
        effort: Some("high".into()),
        effort_options: vec!["low".into(), "high".into()],
        source: "fixture".into(),
        revision: Some("a".repeat(64)),
        writable: true,
        message: None,
    }
}

#[derive(Clone, Copy)]
struct FixedClock;
impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1_000
    }
}
impl MonotonicClock for FixedClock {
    fn now_millis(&self) -> u64 {
        0
    }
}

struct EmptyDashboard;
impl DashboardDataSource for EmptyDashboard {
    fn configured_workers(
        &self,
    ) -> Result<Vec<String>, mac_worker::dashboard::model::DashboardError> {
        Ok(Vec::new())
    }
    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        Vec::new()
    }
    fn local_jobs(
        &self,
    ) -> Result<Vec<DashboardJob>, mac_worker::dashboard::model::DashboardError> {
        Ok(Vec::new())
    }
    fn authoritative_active_jobs(
        &self,
        _deadline: Duration,
    ) -> Vec<Result<DashboardJob, mac_worker::dashboard::model::DashboardError>> {
        Vec::new()
    }
    fn queue_entries(
        &self,
    ) -> Result<Vec<DashboardQueueEntry>, mac_worker::dashboard::model::DashboardError> {
        Ok(Vec::new())
    }
    fn task_projection(
        &self,
        _deadline: Duration,
    ) -> Result<DashboardTaskCollection, mac_worker::dashboard::model::DashboardError> {
        Ok(DashboardTaskCollection {
            projection: TaskListProjection::empty(),
            errors: Vec::new(),
        })
    }
}

#[derive(Default)]
struct EmptyLogs;
impl DashboardLogSource for EmptyLogs {
    fn job_detail(&self, _job_id: JobId) -> Result<DashboardJob, ApiError> {
        Err(ApiError::new("JOB_NOT_FOUND", "job is not available"))
    }
    fn read_log(
        &self,
        _job_id: JobId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new("JOB_NOT_FOUND", "job is not available"))
    }
}

#[derive(Default)]
struct EmptyTasks;
impl DashboardTaskSource for EmptyTasks {
    fn task_detail(
        &self,
        _task_id: TaskId,
    ) -> Result<mac_worker::task_view::TaskDetailProjection, ApiError> {
        Err(ApiError::new("TASK_NOT_FOUND", "task is not available"))
    }
    fn read_task_log(
        &self,
        _task_id: TaskId,
        _turn_id: TurnId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new("TASK_NOT_FOUND", "task is not available"))
    }
}

async fn server(settings: Arc<SettingsFixture>) -> (DashboardHttpServer, Arc<SettingsFixture>) {
    let state = Arc::new(DashboardHttpState {
        service: Arc::new(DashboardService::new(
            EmptyDashboard,
            FixedClock,
            FixedClock,
        )),
        log_source: Arc::new(EmptyLogs),
        task_source: Arc::new(EmptyTasks),
        settings_source: Some(settings.clone()),
    });
    (
        DashboardHttpServer::bind(None, state).await.unwrap(),
        settings,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_routes_read_and_protect_save() {
    let (server, settings) = server(Arc::new(SettingsFixture::new())).await;
    let address = server
        .local_url()
        .strip_prefix("http://")
        .unwrap()
        .to_owned();

    let read = request(
        &address,
        "GET",
        "/api/v1/workers/mini-1/agent-settings",
        &[],
        b"",
    );
    assert_eq!(read.status, 200);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&read.body).unwrap()["agents"][0]["agent"],
        "codex"
    );

    let body = serde_json::to_vec(&AgentSettingsSaveRequest {
        agent: "codex".into(),
        model: Some("gpt-next".into()),
        effort: Some("high".into()),
        revision: "a".repeat(64),
    })
    .unwrap();
    let rejected = request(
        &address,
        "POST",
        "/api/v1/workers/mini-1/agent-settings",
        &[("Content-Type", "application/json")],
        &body,
    );
    assert_eq!(rejected.status, 400);
    assert_eq!(settings.saves.load(Ordering::SeqCst), 0);

    let origin = format!("http://{address}");
    let headers = [
        ("Content-Type", "application/json"),
        ("Origin", origin.as_str()),
        ("X-Mac-Worker-Settings", "1"),
    ];
    let accepted = request(
        &address,
        "POST",
        "/api/v1/workers/mini-1/agent-settings",
        &headers,
        &body,
    );
    assert_eq!(accepted.status, 200);
    assert_eq!(settings.saves.load(Ordering::SeqCst), 1);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_reject_unknown_worker_before_save() {
    let (server, settings) = server(Arc::new(SettingsFixture::new())).await;
    let address = server
        .local_url()
        .strip_prefix("http://")
        .unwrap()
        .to_owned();
    let body = serde_json::to_vec(&AgentSettingsSaveRequest {
        agent: "codex".into(),
        model: Some("gpt-next".into()),
        effort: Some("high".into()),
        revision: "a".repeat(64),
    })
    .unwrap();
    let origin = format!("http://{address}");
    let headers = [
        ("Content-Type", "application/json"),
        ("Origin", origin.as_str()),
        ("X-Mac-Worker-Settings", "1"),
    ];
    let response = request(
        &address,
        "POST",
        "/api/v1/workers/unknown/agent-settings",
        &headers,
        &body,
    );
    assert_eq!(response.status, 404);
    assert_eq!(settings.saves.load(Ordering::SeqCst), 0);
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_rejects_invalid_save_variants_before_source_and_maps_conflict() {
    let (server, settings) = server(Arc::new(SettingsFixture::new())).await;
    let address = server
        .local_url()
        .strip_prefix("http://")
        .unwrap()
        .to_owned();
    let path = "/api/v1/workers/mini-1/agent-settings";
    let body = serde_json::to_vec(&AgentSettingsSaveRequest {
        agent: "codex".into(),
        model: Some("gpt-next".into()),
        effort: Some("high".into()),
        revision: "a".repeat(64),
    })
    .unwrap();
    let origin = format!("http://{address}");

    let wrong_origin = [
        ("Content-Type", "application/json"),
        ("Origin", "http://evil.invalid"),
        ("X-Mac-Worker-Settings", "1"),
    ];
    assert_eq!(
        request(&address, "POST", path, &wrong_origin, &body).status,
        400
    );

    let missing_custom_header = [
        ("Content-Type", "application/json"),
        ("Origin", origin.as_str()),
    ];
    assert_eq!(
        request(&address, "POST", path, &missing_custom_header, &body).status,
        400
    );

    let wrong_content_type = [
        ("Content-Type", "text/plain"),
        ("Origin", origin.as_str()),
        ("X-Mac-Worker-Settings", "1"),
    ];
    assert_eq!(
        request(&address, "POST", path, &wrong_content_type, &body).status,
        400
    );

    let valid_headers = [
        ("Content-Type", "application/json"),
        ("Origin", origin.as_str()),
        ("X-Mac-Worker-Settings", "1"),
    ];
    let unknown_agent = serde_json::to_vec(&serde_json::json!({
        "agent": "shell",
        "model": "gpt-next",
        "effort": null,
        "revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }))
    .unwrap();
    assert_eq!(
        request(&address, "POST", path, &valid_headers, &unknown_agent).status,
        400
    );
    let extra_field = serde_json::to_vec(&serde_json::json!({
        "agent": "codex",
        "model": "gpt-next",
        "effort": "high",
        "revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "extra": true
    }))
    .unwrap();
    assert_eq!(
        request(&address, "POST", path, &valid_headers, &extra_field).status,
        400
    );
    let missing_model = serde_json::to_vec(&serde_json::json!({
        "agent": "codex",
        "effort": "high",
        "revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }))
    .unwrap();
    assert_eq!(
        request(&address, "POST", path, &valid_headers, &missing_model).status,
        400
    );
    let missing_effort = serde_json::to_vec(&serde_json::json!({
        "agent": "codex",
        "model": "gpt-next",
        "revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }))
    .unwrap();
    assert_eq!(
        request(&address, "POST", path, &valid_headers, &missing_effort).status,
        400
    );
    assert_eq!(
        request(&address, "POST", path, &valid_headers, &vec![b'x'; 8_193],).status,
        400
    );
    assert_eq!(settings.saves.load(Ordering::SeqCst), 0);

    *settings.result.lock().unwrap() = Err(ApiError::new("SETTINGS_CONFLICT", "refresh required"));
    let conflict = request(&address, "POST", path, &valid_headers, &body);
    assert_eq!(conflict.status, 409);
    assert_eq!(settings.saves.load(Ordering::SeqCst), 1);
    server.shutdown().await.unwrap();
}

struct Response {
    status: u16,
    body: Vec<u8>,
}

fn request(
    address: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Response {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request =
        format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut wire = request.into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).unwrap();
    let mut raw = Vec::new();
    let split = loop {
        let mut chunk = [0u8; 4096];
        let count = stream.read(&mut chunk).unwrap();
        if count == 0 {
            break raw
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .unwrap();
        }
        raw.extend_from_slice(&chunk[..count]);
        let Some(split) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&raw[..split]);
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if raw.len() >= split + 4 + length {
            break split;
        }
    };
    let head = String::from_utf8_lossy(&raw[..split]);
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    Response {
        status,
        body: raw[split + 4..].to_vec(),
    }
}
