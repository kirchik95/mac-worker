use std::{future::Future, io::Write, pin::Pin, process::Command as ProcessCommand, sync::Arc};

use crate::{
    client_state::ClientStateStore,
    config::Config,
    dashboard::{
        service::{DashboardService, SystemClock, SystemMonotonicClock},
        source::{
            DashboardRemoteReader, DashboardWorkerReader, MacWorkerDashboardSource,
            MacWorkerLogSource, SystemDashboardRemoteReader, SystemDashboardWorkerReader,
        },
        task::{DashboardTaskSource, MacWorkerTaskSource},
        web::{DashboardHttpServer, DashboardHttpState},
    },
    error::WorkerError,
    process::{ProcessRunner, SystemProcessRunner},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DashboardCommandRequest {
    pub port: Option<u16>,
    pub no_open: bool,
}

impl DashboardCommandRequest {
    pub fn new(port: Option<u16>, no_open: bool) -> Self {
        Self { port, no_open }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardRunResult {
    pub url: String,
}

pub trait DashboardLauncher: Send + Sync {
    fn launch<'a>(
        &'a self,
        request: DashboardCommandRequest,
    ) -> Pin<Box<dyn Future<Output = Result<DashboardHttpServer, WorkerError>> + Send + 'a>>;
}

pub trait BrowserOpener: Send + Sync {
    fn open(&self, url: &str) -> Result<(), WorkerError>;
}

pub struct SystemDashboardLauncher {
    pub state: Arc<DashboardHttpState<MacWorkerDashboardSource, SystemClock, SystemMonotonicClock>>,
}

impl SystemDashboardLauncher {
    pub fn from_system(config: Arc<Config>, local_jobs: Arc<ClientStateStore>) -> Self {
        let runner: Arc<dyn ProcessRunner> = Arc::new(SystemProcessRunner);
        let workers: Arc<dyn DashboardWorkerReader> =
            Arc::new(SystemDashboardWorkerReader::new(Arc::clone(&runner)));
        let remote: Arc<dyn DashboardRemoteReader> =
            Arc::new(SystemDashboardRemoteReader::new(runner));
        let source = MacWorkerDashboardSource::new(
            Arc::clone(&config),
            workers,
            Arc::clone(&local_jobs),
            Arc::clone(&remote),
        );
        let task_source: Arc<dyn DashboardTaskSource> = Arc::new(MacWorkerTaskSource::new(
            Arc::clone(&config),
            Arc::clone(&local_jobs),
            Arc::clone(&remote),
        ));
        let log_source = Arc::new(MacWorkerLogSource::new(config, local_jobs, remote));
        Self {
            state: Arc::new(DashboardHttpState {
                service: Arc::new(DashboardService::new(
                    source,
                    SystemClock,
                    SystemMonotonicClock::new(),
                )),
                log_source,
                task_source,
            }),
        }
    }
}

impl DashboardLauncher for SystemDashboardLauncher {
    fn launch<'a>(
        &'a self,
        request: DashboardCommandRequest,
    ) -> Pin<Box<dyn Future<Output = Result<DashboardHttpServer, WorkerError>> + Send + 'a>> {
        Box::pin(async move {
            DashboardHttpServer::bind(request.port, Arc::clone(&self.state))
                .await
                .map_err(api_error)
        })
    }
}

#[derive(Default)]
pub struct SystemBrowserOpener;

impl BrowserOpener for SystemBrowserOpener {
    fn open(&self, url: &str) -> Result<(), WorkerError> {
        validate_dashboard_url(url)?;
        let status = ProcessCommand::new("/usr/bin/open")
            .arg(url)
            .status()
            .map_err(|_| browser_open_error())?;
        if status.success() {
            Ok(())
        } else {
            Err(browser_open_error())
        }
    }
}

pub async fn run_dashboard(
    request: DashboardCommandRequest,
    launcher: &dyn DashboardLauncher,
    opener: &dyn BrowserOpener,
    shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send>>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<DashboardRunResult, WorkerError> {
    let server = launcher.launch(request).await?;
    let url = server.local_url();
    if let Err(error) = write_url(stdout, &url) {
        let _ = server.shutdown().await;
        return Err(error);
    }
    if !request.no_open
        && opener.open(&url).is_err()
        && let Err(error) = write_browser_warning(stderr)
    {
        let _ = server.shutdown().await;
        return Err(error);
    }
    shutdown_signal.await;
    server.shutdown().await.map_err(api_error)?;
    Ok(DashboardRunResult { url })
}

fn write_url(stdout: &mut dyn Write, url: &str) -> Result<(), WorkerError> {
    writeln!(stdout, "{url}")?;
    stdout.flush()?;
    Ok(())
}

fn write_browser_warning(stderr: &mut dyn Write) -> Result<(), WorkerError> {
    writeln!(
        stderr,
        "DASHBOARD_BROWSER_OPEN_FAILED: dashboard browser could not be opened"
    )?;
    stderr.flush()?;
    Ok(())
}

fn validate_dashboard_url(url: &str) -> Result<(), WorkerError> {
    let parsed = url::Url::parse(url).map_err(|_| invalid_dashboard_url())?;
    if parsed.scheme() == "http"
        && parsed.host_str() == Some("127.0.0.1")
        && parsed.port().is_some_and(|port| port != 0)
        && parsed.username().is_empty()
        && parsed.password().is_none()
        && parsed.path() == "/"
        && parsed.query().is_none()
        && parsed.fragment().is_none()
    {
        Ok(())
    } else {
        Err(invalid_dashboard_url())
    }
}

fn invalid_dashboard_url() -> WorkerError {
    WorkerError::Protocol("DASHBOARD_INVALID_URL: dashboard URL must be loopback only".into())
}

fn browser_open_error() -> WorkerError {
    WorkerError::Unavailable(
        "DASHBOARD_BROWSER_OPEN_FAILED: dashboard browser could not be opened".into(),
    )
}

fn api_error(error: crate::dashboard::model::ApiError) -> WorkerError {
    WorkerError::Protocol(format!("{}: {}", error.code, error.message))
}
