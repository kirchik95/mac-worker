use std::{
    future::Future,
    io::{Read, Write},
    net::TcpStream,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::Parser;
use mac_worker::{
    cli::{Cli, Command as WorkerCommand},
    dashboard::{
        command::{BrowserOpener, DashboardCommandRequest, DashboardLauncher, run_dashboard},
        model::{ApiError, DashboardError, DashboardJob, DashboardLogChunk, DashboardQueueEntry},
        service::{
            Clock, DashboardDataSource, DashboardService, MonotonicClock, WorkerObservationResult,
        },
        task::DashboardTaskSource,
        web::{DashboardHttpServer, DashboardHttpState, DashboardLogSource},
    },
    error::WorkerError,
    job::{JobId, LogStream},
    task::{TaskId, TurnId},
    task_view::TaskDetailProjection,
};
use tokio::sync::oneshot;

#[test]
fn dashboard_parses_only_the_documented_public_forms() {
    let cases = [
        (vec!["worker", "dashboard"], None, false),
        (
            vec!["worker", "dashboard", "--port", "9173"],
            Some(9173),
            false,
        ),
        (vec!["worker", "dashboard", "--no-open"], None, true),
        (
            vec!["worker", "dashboard", "--port", "9173", "--no-open"],
            Some(9173),
            true,
        ),
    ];

    for (arguments, expected_port, expected_no_open) in cases {
        let cli = Cli::try_parse_from(arguments).expect("dashboard form must parse");
        let WorkerCommand::Dashboard { port, no_open } = cli.command else {
            panic!("dashboard arguments must select the dashboard command");
        };
        assert_eq!(port, expected_port);
        assert_eq!(no_open, expected_no_open);
    }

    for arguments in [
        vec!["worker", "dashboard", "--port", "0"],
        vec!["worker", "dashboard", "--port", "65536"],
        vec!["worker", "dashboard", "--port", "9173", "--port", "9174"],
        vec!["worker", "dashboard", "unexpected"],
    ] {
        assert!(Cli::try_parse_from(arguments).is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_open_prints_loopback_url_without_invoking_browser() {
    let launcher = RecordingLauncher::new(None);
    let opener = RecordingOpener::succeeds();
    let mut output = Vec::new();
    let mut warnings = Vec::new();

    let result = run_dashboard(
        DashboardCommandRequest {
            port: None,
            no_open: true,
        },
        &launcher,
        &opener,
        Box::pin(async {}),
        &mut output,
        &mut warnings,
    )
    .await
    .unwrap();

    assert!(result.url.starts_with("http://127.0.0.1:"));
    assert_eq!(
        String::from_utf8(output).unwrap(),
        format!("{}\n", result.url)
    );
    assert!(warnings.is_empty());
    assert_eq!(
        launcher.requests(),
        vec![DashboardCommandRequest::new(None, true)]
    );
    assert!(opener.urls().is_empty());
    launcher.assert_no_mutations();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_command_opens_the_exact_loopback_url_once() {
    let launcher = RecordingLauncher::new(None);
    let opener = RecordingOpener::succeeds();
    let mut output = Vec::new();
    let mut warnings = Vec::new();

    let result = run_dashboard(
        DashboardCommandRequest::new(Some(0), false),
        &launcher,
        &opener,
        Box::pin(async {}),
        &mut output,
        &mut warnings,
    )
    .await
    .unwrap();

    assert_eq!(opener.urls(), vec![result.url.clone()]);
    assert!(warnings.is_empty());
    launcher.assert_no_mutations();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dashboard_lifecycle_stays_read_only_after_a_browser_client_disconnects() {
    let (started_sender, started_receiver) = oneshot::channel();
    let launcher = RecordingLauncher::new(Some(started_sender));
    let opener = RecordingOpener::fails();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let mut output = Vec::new();
    let mut warnings = Vec::new();
    let (result, url) = {
        let dashboard = run_dashboard(
            DashboardCommandRequest::new(None, false),
            &launcher,
            &opener,
            Box::pin(async move {
                let _ = shutdown_receiver.await;
            }),
            &mut output,
            &mut warnings,
        );
        tokio::pin!(dashboard);

        let url = tokio::select! {
            url = started_receiver => url.unwrap(),
            result = &mut dashboard => panic!("dashboard stopped before the server became usable: {result:?}"),
        };
        assert_eq!(request_status(&url, "/"), 200);
        let host = url.strip_prefix("http://").unwrap();
        let mut disconnected_client = TcpStream::connect(host).unwrap();
        disconnected_client
            .write_all(b"GET /api/v1/snapshot HTTP/1.1\r\nHost: ")
            .unwrap();
        drop(disconnected_client);
        shutdown_sender.send(()).unwrap();
        (dashboard.as_mut().await.unwrap(), url)
    };

    let warning = String::from_utf8(warnings).unwrap();
    assert_eq!(opener.urls(), vec![url.clone()]);
    assert_eq!(result.url, url);
    assert!(warning.contains("DASHBOARD_BROWSER_OPEN_FAILED"));
    assert!(!warning.contains("private browser failure"));
    assert_eq!(
        String::from_utf8(output).unwrap(),
        format!("{}\n", result.url)
    );
    launcher.assert_no_mutations();
}

struct RecordingLauncher {
    requests: Mutex<Vec<DashboardCommandRequest>>,
    source: ReadOnlySource,
    logs: Arc<ReadOnlyLogs>,
    task_source: Arc<ReadOnlyTaskSource>,
    started: Mutex<Option<oneshot::Sender<String>>>,
}

impl RecordingLauncher {
    fn new(started: Option<oneshot::Sender<String>>) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            source: ReadOnlySource::default(),
            logs: Arc::new(ReadOnlyLogs::default()),
            task_source: Arc::new(ReadOnlyTaskSource::default()),
            started: Mutex::new(started),
        }
    }

    fn requests(&self) -> Vec<DashboardCommandRequest> {
        self.requests.lock().unwrap().clone()
    }

    fn assert_no_mutations(&self) {
        assert_eq!(
            self.source.mutation_count() + self.logs.mutation_count(),
            0,
            "dashboard launch, browser requests, disconnects, and shutdown must not invoke fake submit, cancel, retry, delete, or lease operations"
        );
        assert_eq!(
            self.task_source.mutation_count(),
            0,
            "dashboard task routes must remain read-only"
        );
    }
}

impl DashboardLauncher for RecordingLauncher {
    fn launch<'a>(
        &'a self,
        request: DashboardCommandRequest,
    ) -> Pin<Box<dyn Future<Output = Result<DashboardHttpServer, WorkerError>> + Send + 'a>> {
        self.requests.lock().unwrap().push(request);
        let source = self.source.clone();
        let logs = Arc::clone(&self.logs);
        let task_source = Arc::clone(&self.task_source);
        let started = self.started.lock().unwrap().take();
        Box::pin(async move {
            let state = Arc::new(DashboardHttpState {
                service: Arc::new(DashboardService::new(source, FixedClock, FixedClock)),
                log_source: logs,
                task_source,
                settings_source: None,
            });
            let server = DashboardHttpServer::bind(Some(0), state)
                .await
                .map_err(api_error)?;
            if let Some(sender) = started {
                let _ = sender.send(server.local_url());
            }
            Ok(server)
        })
    }
}

#[derive(Clone, Default)]
struct ReadOnlySource {
    mutation_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl ReadOnlySource {
    fn mutation_count(&self) -> usize {
        self.mutation_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl DashboardDataSource for ReadOnlySource {
    fn configured_workers(&self) -> Result<Vec<String>, DashboardError> {
        Ok(Vec::new())
    }

    fn collect_workers(&self, _deadline: Duration) -> Vec<WorkerObservationResult> {
        Vec::new()
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
struct ReadOnlyLogs {
    mutation_count: std::sync::atomic::AtomicUsize,
}

impl ReadOnlyLogs {
    fn mutation_count(&self) -> usize {
        self.mutation_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl DashboardLogSource for ReadOnlyLogs {
    fn job_detail(&self, _job_id: JobId) -> Result<DashboardJob, ApiError> {
        Err(ApiError::new(
            "JOB_NOT_FOUND",
            "job is not present in fake state",
        ))
    }

    fn read_log(
        &self,
        _job_id: JobId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new(
            "JOB_NOT_FOUND",
            "job is not present in fake state",
        ))
    }
}

#[derive(Default)]
struct ReadOnlyTaskSource {
    mutation_count: std::sync::atomic::AtomicUsize,
}

impl ReadOnlyTaskSource {
    fn mutation_count(&self) -> usize {
        self.mutation_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl DashboardTaskSource for ReadOnlyTaskSource {
    fn task_detail(&self, _task_id: TaskId) -> Result<TaskDetailProjection, ApiError> {
        Err(ApiError::new(
            "TASK_NOT_FOUND",
            "task is not present in fake state",
        ))
    }

    fn read_task_log(
        &self,
        _task_id: TaskId,
        _turn_id: TurnId,
        _stream: LogStream,
        _offset: u64,
        _limit: u32,
    ) -> Result<DashboardLogChunk, ApiError> {
        Err(ApiError::new(
            "TURN_NOT_FOUND",
            "turn is not present in fake state",
        ))
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

struct RecordingOpener {
    urls: Mutex<Vec<String>>,
    failure: bool,
}

impl RecordingOpener {
    fn succeeds() -> Self {
        Self {
            urls: Mutex::new(Vec::new()),
            failure: false,
        }
    }

    fn fails() -> Self {
        Self {
            urls: Mutex::new(Vec::new()),
            failure: true,
        }
    }

    fn urls(&self) -> Vec<String> {
        self.urls.lock().unwrap().clone()
    }
}

impl BrowserOpener for RecordingOpener {
    fn open(&self, url: &str) -> Result<(), WorkerError> {
        self.urls.lock().unwrap().push(url.to_owned());
        if self.failure {
            Err(WorkerError::Unavailable(
                "DASHBOARD_BROWSER_OPEN_FAILED: private browser failure".into(),
            ))
        } else {
            Ok(())
        }
    }
}

fn request_status(url: &str, path: &str) -> u16 {
    let host = url.strip_prefix("http://").unwrap();
    let mut stream = TcpStream::connect(host).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response.split_whitespace().nth(1).unwrap().parse().unwrap()
}

fn api_error(error: ApiError) -> WorkerError {
    WorkerError::Protocol(format!("{}: {}", error.code, error.message))
}
