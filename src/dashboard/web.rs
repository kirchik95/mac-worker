use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, RawQuery, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tokio_stream::{
    Stream,
    wrappers::{ReceiverStream, WatchStream},
};

use crate::{
    agent_settings::{
        AgentSettingsError, AgentSettingsGetRequest, AgentSettingsSaveRequest,
        validate_get_request, validate_save_request,
    },
    controller::events::{ViewerEventSource, ViewerMessage},
    dashboard::{
        model::{ApiError, DashboardError, DashboardJob, DashboardLogChunk},
        service::{
            Clock, CollectorHandle, DashboardDataSource, DashboardService, LocalRefreshHandle,
            MonotonicClock,
        },
        settings::DashboardSettingsSource,
        task::{DashboardTaskMutationSource, DashboardTaskSource, TaskMutationRequest},
    },
    error::WorkerError,
    job::{JobId, LogStream},
    task::{TaskId, TurnId},
};

const MAX_LOG_LIMIT: u32 = 65_536;
const MAX_SETTINGS_REQUEST_BYTES: usize = 8_192;
const MAX_TASK_MUTATION_REQUEST_BYTES: usize = 8_192;
// `style-src` admits inline styles because the component library the dashboard
// UI is built from sets them on elements it positions. Scripts stay first-party
// only, and the page still renders every remote string as text.
const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; base-uri 'none'; connect-src 'self'; form-action 'none'; frame-ancestors 'none'; object-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'";

// The UI is built from `ui/` with `npm run build`, which writes this tree.
const INDEX_HTML: &str = include_str!("static/app/index.html");
const DASHBOARD_CSS: &str = include_str!("static/app/assets/index.css");
const DASHBOARD_JS: &str = include_str!("static/app/assets/index.js");
const FAVICON_SVG: &str = include_str!("static/app/favicon.svg");

pub trait DashboardLogSource: Send + Sync + 'static {
    fn job_detail(&self, job_id: JobId) -> Result<DashboardJob, ApiError>;
    fn read_log(
        &self,
        job_id: JobId,
        stream: LogStream,
        offset: u64,
        limit: u32,
    ) -> Result<DashboardLogChunk, ApiError>;
}

pub struct DashboardHttpState<S, C, M> {
    pub service: Arc<DashboardService<S, C, M>>,
    pub log_source: Arc<dyn DashboardLogSource>,
    pub task_source: Arc<dyn DashboardTaskSource>,
    pub settings_source: Option<Arc<dyn DashboardSettingsSource>>,
    pub mutation_source: Option<Arc<dyn DashboardTaskMutationSource>>,
}

pub struct DashboardHttpServer {
    local_addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<(), ApiError>>,
    collector: CollectorHandle,
    events: EventStopGuard,
    local_refresh: Option<LocalRefreshHandle>,
}

struct EventStopGuard(Option<Arc<dyn ViewerEventSource>>);
impl EventStopGuard {
    fn stop(&self) {
        if let Some(source) = &self.0 {
            source.stop();
        }
    }
}
impl Drop for EventStopGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

impl DashboardHttpServer {
    pub async fn bind_with_events<S, C, M>(
        port: Option<u16>,
        state: Arc<DashboardHttpState<S, C, M>>,
        events: Arc<dyn ViewerEventSource>,
    ) -> Result<Self, ApiError>
    where
        S: DashboardDataSource,
        C: Clock,
        M: MonotonicClock,
    {
        Self::bind_inner(port, state, Some(events)).await
    }

    pub async fn bind<S, C, M>(
        port: Option<u16>,
        state: Arc<DashboardHttpState<S, C, M>>,
    ) -> Result<Self, ApiError>
    where
        S: DashboardDataSource,
        C: Clock,
        M: MonotonicClock,
    {
        Self::bind_inner(port, state, None).await
    }

    async fn bind_inner<S, C, M>(
        port: Option<u16>,
        state: Arc<DashboardHttpState<S, C, M>>,
        events: Option<Arc<dyn ViewerEventSource>>,
    ) -> Result<Self, ApiError>
    where
        S: DashboardDataSource,
        C: Clock,
        M: MonotonicClock,
    {
        let listener =
            TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port.unwrap_or(0))))
                .await
                .map_err(|_| {
                    ApiError::new(
                        "DASHBOARD_BIND_FAILED",
                        "dashboard loopback port is unavailable",
                    )
                })?;
        let local_addr = listener.local_addr().map_err(|_| {
            ApiError::new(
                "DASHBOARD_BIND_FAILED",
                "dashboard loopback address is unavailable",
            )
        })?;
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let local_refresh = if events.is_some() {
            Some(
                state
                    .service
                    .start_local_projection_refresh()
                    .map_err(|error| ApiError::new(error.code, error.message))?,
            )
        } else {
            None
        };
        let collector = state
            .service
            .start_background_collection()
            .map_err(|error| ApiError::new(error.code, error.message))?;
        let app_state = AppState {
            dashboard: state,
            expected_host: local_addr.to_string(),
            events: events.clone(),
            shutdown: shutdown_receiver.clone(),
        };
        let task = tokio::spawn(async move {
            axum::serve(listener, router(app_state))
                .with_graceful_shutdown(wait_for_shutdown(shutdown_receiver))
                .await
                .map_err(|_| {
                    ApiError::new(
                        "DASHBOARD_SERVER_FAILED",
                        "dashboard server stopped unexpectedly",
                    )
                })
        });
        Ok(Self {
            local_addr,
            shutdown,
            task,
            collector,
            events: EventStopGuard(events),
            local_refresh,
        })
    }

    pub fn local_url(&self) -> String {
        format!("http://{}", self.local_addr)
    }

    pub async fn shutdown(self) -> Result<(), ApiError> {
        self.events.stop();
        if let Some(handle) = &self.local_refresh {
            handle.stop();
        }
        self.collector.stop();
        let _ = self.shutdown.send(true);
        self.task.await.map_err(|_| {
            ApiError::new(
                "DASHBOARD_SERVER_FAILED",
                "dashboard server task stopped unexpectedly",
            )
        })?
    }
}

struct AppState<S, C, M> {
    dashboard: Arc<DashboardHttpState<S, C, M>>,
    expected_host: String,
    events: Option<Arc<dyn ViewerEventSource>>,
    shutdown: watch::Receiver<bool>,
}

impl<S, C, M> Clone for AppState<S, C, M> {
    fn clone(&self) -> Self {
        Self {
            dashboard: Arc::clone(&self.dashboard),
            expected_host: self.expected_host.clone(),
            events: self.events.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

fn router<S, C, M>(state: AppState<S, C, M>) -> Router
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    Router::new()
        .route("/", get(index))
        .route("/assets/index.css", get(stylesheet))
        .route("/assets/index.js", get(script))
        .route("/favicon.svg", get(favicon))
        .route("/assets/{font}", get(font))
        .route("/api/v1/snapshot", get(snapshot::<S, C, M>))
        .route("/api/v1/events", get(events::<S, C, M>))
        .route(
            "/api/v1/tasks/{task_id}/turns/{turn_id}/logs",
            get(task_log_chunk::<S, C, M>),
        )
        .route("/api/v1/tasks/{task_id}", get(task_detail::<S, C, M>))
        .route("/api/v1/tasks/{task_id}/reply", post(task_reply::<S, C, M>))
        .route(
            "/api/v1/tasks/{task_id}/accept",
            post(task_accept::<S, C, M>),
        )
        .route("/api/v1/jobs/{job_id}", get(job_detail::<S, C, M>))
        .route("/api/v1/jobs/{job_id}/logs", get(log_chunk::<S, C, M>))
        .route(
            "/api/v1/workers/{worker_name}/agent-settings",
            get(agent_settings_get::<S, C, M>).post(agent_settings_post::<S, C, M>),
        )
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            validate_host::<S, C, M>,
        ))
        .layer(middleware::map_response(security_headers))
        .with_state(state)
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<bool>) {
    if *receiver.borrow() {
        return;
    }
    while receiver.changed().await.is_ok() {
        if *receiver.borrow() {
            return;
        }
    }
}

async fn validate_host<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    request: Request,
    next: Next,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let host_matches = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|host| host == state.expected_host);
    if !host_matches {
        return api_error(
            StatusCode::BAD_REQUEST,
            ApiError::new(
                "INVALID_HOST",
                "request Host must match the loopback listener",
            ),
        );
    }
    next.run(request).await
}

async fn security_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn events<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let Some(source) = state.events else {
        return not_found().await;
    };
    let expected_origin = format!("http://{}", state.expected_host);
    let origins = headers.get_all(header::ORIGIN).iter().collect::<Vec<_>>();
    if origins.len() > 1
        || origins
            .first()
            .is_some_and(|value| value.to_str().ok() != Some(expected_origin.as_str()))
        || headers.get_all("sec-fetch-site").iter().any(|value| {
            value
                .to_str()
                .map_or(true, |value| value.eq_ignore_ascii_case("cross-site"))
        })
    {
        return api_error(
            StatusCode::FORBIDDEN,
            ApiError::new("INVALID_ORIGIN", "event stream requires the viewer origin"),
        );
    }
    let last_ids = headers.get_all("last-event-id").iter().collect::<Vec<_>>();
    if last_ids.len() > 1
        || last_ids
            .first()
            .is_some_and(|value| value.to_str().is_err())
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            ApiError::new(
                "INVALID_EVENT_CURSOR",
                "event cursor is invalid or conflicting",
            ),
        );
    }
    let last_id = last_ids.first().and_then(|value| value.to_str().ok());
    let after = match crate::dashboard::events::resolve_cursor(query.as_deref(), last_id) {
        Ok(after) => after,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                ApiError::new(
                    "INVALID_EVENT_CURSOR",
                    "event cursor is invalid or conflicting",
                ),
            );
        }
    };
    let receiver = match source.subscribe(after) {
        Ok(receiver) => receiver,
        Err(_) => {
            return api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                ApiError::new(
                    "CONTROLLER_EVENTS_UNAVAILABLE",
                    "viewer event stream is unavailable",
                ),
            );
        }
    };
    let stream = ResponseEvents {
        messages: ReceiverStream::new(receiver),
        shutdown: WatchStream::new(state.shutdown),
    };
    let mut response = Sse::new(stream).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// Poll cancellation first, including when an unread subscriber queue is full.
/// Closing just its sender would otherwise drain that queue during shutdown.
struct ResponseEvents {
    messages: ReceiverStream<ViewerMessage>,
    shutdown: WatchStream<bool>,
}
impl Stream for ResponseEvents {
    type Item = Result<Event, WorkerError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.shutdown).poll_next(cx) {
                Poll::Ready(Some(true) | None) => return Poll::Ready(None),
                Poll::Ready(Some(false)) => continue,
                Poll::Pending => break,
            }
        }
        Pin::new(&mut self.messages)
            .poll_next(cx)
            .map(|message| message.map(sse_message))
    }
}

fn sse_message(message: ViewerMessage) -> Result<Event, WorkerError> {
    let name = message.event_name();
    let cursor = message.cursor();
    let data = message.data_value()?;
    let mut event = Event::default().event(name).data(data.to_string());
    if let Some(cursor) = cursor {
        event = event.id(format!("{}:{}", cursor.journal_id, cursor.seq));
    }
    if name == "heartbeat" {
        event = event.comment("keepalive");
    }
    Ok(event)
}

async fn index() -> Response {
    embedded_asset("text/html; charset=utf-8", INDEX_HTML)
}

async fn stylesheet() -> Response {
    embedded_asset("text/css; charset=utf-8", DASHBOARD_CSS)
}

async fn script() -> Response {
    embedded_asset("application/javascript; charset=utf-8", DASHBOARD_JS)
}

async fn favicon() -> Response {
    embedded_asset("image/svg+xml", FAVICON_SVG)
}

async fn font(Path(name): Path<String>) -> Response {
    let bytes: &'static [u8] = match name.as_str() {
        "inter-variable.ttf" => include_bytes!("static/app/assets/inter-variable.ttf"),
        "plex-mono-regular.ttf" => include_bytes!("static/app/assets/plex-mono-regular.ttf"),
        _ => return not_found().await,
    };
    ([(header::CONTENT_TYPE, "font/ttf")], bytes).into_response()
}

async fn snapshot<S, C, M>(State(state): State<AppState<S, C, M>>) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let service = Arc::clone(&state.dashboard.service);
    match tokio::task::spawn_blocking(move || service.read_snapshot()).await {
        Ok(Ok(snapshot)) => api_json(StatusCode::OK, snapshot),
        Ok(Err(error)) => api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            api_error_from_dashboard(error),
        ),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                "DASHBOARD_SNAPSHOT_FAILED",
                "dashboard snapshot collection failed",
            ),
        ),
    }
}

async fn agent_settings_get<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(worker_name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let request = match parse_settings_query(raw_query.as_deref()) {
        Ok(request) => request,
        Err(error) => return settings_request_error(error),
    };
    let Some(source) = state.dashboard.settings_source.clone() else {
        return settings_error(ApiError::new(
            "SETTINGS_UNAVAILABLE",
            "native settings are unavailable on this worker",
        ));
    };
    if !source.worker_exists(&worker_name) {
        return api_error(
            StatusCode::NOT_FOUND,
            ApiError::new("WORKER_NOT_FOUND", "configured worker was not found"),
        );
    }
    match tokio::task::spawn_blocking(move || source.read(&worker_name, &request)).await {
        Ok(Ok(settings)) => api_json(StatusCode::OK, settings),
        Ok(Err(error)) => settings_error(error),
        Err(_) => settings_error(ApiError::new(
            "SETTINGS_UNAVAILABLE",
            "native settings are unavailable on this worker",
        )),
    }
}

fn parse_settings_query(
    raw_query: Option<&str>,
) -> Result<AgentSettingsGetRequest, AgentSettingsError> {
    let query = raw_query.unwrap_or_default();
    if query.len() > MAX_SETTINGS_REQUEST_BYTES {
        return Err(AgentSettingsError::InvalidRequest(
            "settings query is invalid",
        ));
    }
    let mut request = AgentSettingsGetRequest::default();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key != "env_profile" || request.env_profile.is_some() {
            return Err(AgentSettingsError::InvalidRequest(
                "settings query is invalid",
            ));
        }
        request.env_profile = Some(value.into_owned());
    }
    validate_get_request(&request)?;
    Ok(request)
}

async fn agent_settings_post<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(worker_name): Path<String>,
    request: Request,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let Some(source) = state.dashboard.settings_source.clone() else {
        return settings_error(ApiError::new(
            "SETTINGS_UNAVAILABLE",
            "native settings are unavailable on this worker",
        ));
    };
    if !source.worker_exists(&worker_name) {
        return api_error(
            StatusCode::NOT_FOUND,
            ApiError::new("WORKER_NOT_FOUND", "configured worker was not found"),
        );
    }
    if !request_has_settings_headers(&request, &state.expected_host) {
        return api_error(
            StatusCode::BAD_REQUEST,
            ApiError::new(
                "SETTINGS_REQUEST_INVALID",
                "settings save requires same-origin JSON and the settings header",
            ),
        );
    }
    let body = match to_bytes(request.into_body(), MAX_SETTINGS_REQUEST_BYTES + 1).await {
        Ok(body) if body.len() <= MAX_SETTINGS_REQUEST_BYTES => body,
        _ => {
            return api_error(
                StatusCode::BAD_REQUEST,
                ApiError::new(
                    "SETTINGS_REQUEST_INVALID",
                    "settings request exceeds 8192 bytes",
                ),
            );
        }
    };
    let save: AgentSettingsSaveRequest = match serde_json::from_slice(&body) {
        Ok(save) => save,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                ApiError::new("SETTINGS_REQUEST_INVALID", "settings request was invalid"),
            );
        }
    };
    if let Err(error) = validate_save_request(&save) {
        return settings_request_error(error);
    }
    match tokio::task::spawn_blocking(move || source.save(&worker_name, &save)).await {
        Ok(Ok(settings)) => api_json(StatusCode::OK, settings),
        Ok(Err(error)) => settings_error(error),
        Err(_) => settings_error(ApiError::new(
            "SETTINGS_UNAVAILABLE",
            "native settings are unavailable on this worker",
        )),
    }
}

async fn task_reply<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(raw_task_id): Path<String>,
    request: Request,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    task_mutation(state, raw_task_id, request, MutationKind::Reply).await
}

async fn task_accept<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(raw_task_id): Path<String>,
    request: Request,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    task_mutation(state, raw_task_id, request, MutationKind::Accept).await
}

enum MutationKind {
    Reply,
    Accept,
}

async fn task_mutation<S, C, M>(
    state: AppState<S, C, M>,
    raw_task_id: String,
    request: Request,
    kind: MutationKind,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let Some(source) = state.dashboard.mutation_source.clone() else {
        return api_error(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiError::new(
                "TASK_MUTATION_UNAVAILABLE",
                "task mutations are unavailable on this dashboard",
            ),
        );
    };
    let task_id = match parse_task_id(&raw_task_id) {
        Ok(task_id) => task_id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    if !request_has_task_headers(&request, &state.expected_host) {
        return api_error(
            StatusCode::BAD_REQUEST,
            ApiError::new(
                "TASK_REQUEST_INVALID",
                "task action requires same-origin JSON and the task header",
            ),
        );
    }
    let body = match to_bytes(request.into_body(), MAX_TASK_MUTATION_REQUEST_BYTES + 1).await {
        Ok(body) if body.len() <= MAX_TASK_MUTATION_REQUEST_BYTES => body,
        _ => {
            return api_error(
                StatusCode::BAD_REQUEST,
                ApiError::new("TASK_REQUEST_INVALID", "task request exceeds 8192 bytes"),
            );
        }
    };
    let mutation: TaskMutationRequest = match serde_json::from_slice(&body) {
        Ok(mutation) => mutation,
        Err(_) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                ApiError::new("TASK_REQUEST_INVALID", "task request was invalid"),
            );
        }
    };
    match tokio::task::spawn_blocking(move || match kind {
        MutationKind::Reply => source.reply(task_id, &mutation),
        MutationKind::Accept => source.accept(task_id, &mutation),
    })
    .await
    {
        Ok(Ok(detail)) => api_json(StatusCode::OK, detail),
        Ok(Err(error)) => task_mutation_error(error),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                "DASHBOARD_TASK_HANDLER_FAILED",
                "dashboard task mutation failed",
            ),
        ),
    }
}

fn request_has_task_headers(request: &Request, expected_host: &str) -> bool {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    let custom_header = request
        .headers()
        .get("x-mac-worker-task")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == "1");
    let expected_origin = format!("http://{expected_host}");
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected_origin);
    content_type && custom_header && origin
}

fn request_has_settings_headers(request: &Request, expected_host: &str) -> bool {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    let custom_header = request
        .headers()
        .get("x-mac-worker-settings")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == "1");
    let expected_origin = format!("http://{expected_host}");
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected_origin);
    content_type && custom_header && origin
}

async fn job_detail<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(raw_job_id): Path<String>,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let job_id = match parse_job_id(&raw_job_id) {
        Ok(job_id) => job_id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let log_source = Arc::clone(&state.dashboard.log_source);
    match tokio::task::spawn_blocking(move || log_source.job_detail(job_id)).await {
        Ok(Ok(job)) => api_json(StatusCode::OK, job),
        Ok(Err(error)) => source_error(error),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new("DASHBOARD_SOURCE_FAILED", "dashboard job lookup failed"),
        ),
    }
}

async fn log_chunk<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(raw_job_id): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let job_id = match parse_job_id(&raw_job_id) {
        Ok(job_id) => job_id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let query = match parse_log_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let log_source = Arc::clone(&state.dashboard.log_source);
    match tokio::task::spawn_blocking(move || {
        log_source.read_log(job_id, query.stream, query.offset, query.limit)
    })
    .await
    {
        Ok(Ok(chunk)) => api_json(StatusCode::OK, chunk),
        Ok(Err(error)) => source_error(error),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new("DASHBOARD_SOURCE_FAILED", "dashboard log lookup failed"),
        ),
    }
}

async fn task_detail<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path(raw_task_id): Path<String>,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let task_id = match parse_task_id(&raw_task_id) {
        Ok(task_id) => task_id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let task_source = Arc::clone(&state.dashboard.task_source);
    match tokio::task::spawn_blocking(move || task_source.task_detail(task_id)).await {
        Ok(Ok(detail)) => api_json(StatusCode::OK, detail),
        Ok(Err(error)) => task_source_error(error),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                "DASHBOARD_TASK_HANDLER_FAILED",
                "dashboard task lookup failed",
            ),
        ),
    }
}

async fn task_log_chunk<S, C, M>(
    State(state): State<AppState<S, C, M>>,
    Path((raw_task_id, raw_turn_id)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
) -> Response
where
    S: DashboardDataSource,
    C: Clock,
    M: MonotonicClock,
{
    let task_id = match parse_task_id(&raw_task_id) {
        Ok(task_id) => task_id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let turn_id = match parse_turn_id(&raw_turn_id) {
        Ok(turn_id) => turn_id,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let query = match parse_log_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(error) => return api_error(StatusCode::BAD_REQUEST, error),
    };
    let task_source = Arc::clone(&state.dashboard.task_source);
    match tokio::task::spawn_blocking(move || {
        task_source.read_task_log(task_id, turn_id, query.stream, query.offset, query.limit)
    })
    .await
    {
        Ok(Ok(chunk)) => api_json(StatusCode::OK, chunk),
        Ok(Err(error)) => task_source_error(error),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::new(
                "DASHBOARD_TASK_HANDLER_FAILED",
                "dashboard task log lookup failed",
            ),
        ),
    }
}

async fn not_found() -> Response {
    api_error(
        StatusCode::NOT_FOUND,
        ApiError::new("NOT_FOUND", "dashboard route was not found"),
    )
}

fn embedded_asset(content_type: &'static str, body: &'static str) -> Response {
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn api_json<T>(status: StatusCode, value: T) -> Response
where
    T: serde::Serialize,
{
    let mut response = (status, Json(value)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn api_error(status: StatusCode, error: ApiError) -> Response {
    api_json(status, error)
}

fn source_error(error: ApiError) -> Response {
    let status = if error.code == "JOB_NOT_FOUND" {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_GATEWAY
    };
    api_error(status, error)
}

fn settings_error(error: ApiError) -> Response {
    let status = match error.code.as_str() {
        "SETTINGS_CONFLICT" => StatusCode::CONFLICT,
        "SETTINGS_INVALID" | "SETTINGS_REQUEST_INVALID" => StatusCode::BAD_REQUEST,
        "WORKER_NOT_FOUND" => StatusCode::NOT_FOUND,
        "SETTINGS_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    };
    api_error(status, error)
}

fn settings_request_error(error: AgentSettingsError) -> Response {
    api_error(
        StatusCode::BAD_REQUEST,
        ApiError::new(error.code(), error.safe_message()),
    )
}

fn task_source_error(error: ApiError) -> Response {
    let (status, message) = match error.code.as_str() {
        "TASK_NOT_FOUND" => (
            StatusCode::NOT_FOUND,
            "task is not available in the dashboard source",
        ),
        "TURN_NOT_FOUND" => (
            StatusCode::NOT_FOUND,
            "turn is not available in the requested task",
        ),
        _ => (
            StatusCode::BAD_GATEWAY,
            "dashboard task source is unavailable",
        ),
    };
    api_error(status, ApiError::new(error.code, message))
}

fn task_mutation_error(error: ApiError) -> Response {
    let status = match error.code.as_str() {
        "TASK_REVISION_CONFLICT" | "TASK_BUSY" | "TASK_CLOSED" | "FOLLOWUP_LIMIT" => {
            StatusCode::CONFLICT
        }
        "TASK_NOT_FOUND" => StatusCode::NOT_FOUND,
        "TASK_REQUEST_INVALID" | "TASK_CONFIG_INVALID" => StatusCode::BAD_REQUEST,
        "TASK_MUTATION_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_GATEWAY,
    };
    api_error(status, error)
}

fn api_error_from_dashboard(error: DashboardError) -> ApiError {
    ApiError::new(error.code, error.message)
}

fn parse_job_id(raw_job_id: &str) -> Result<JobId, ApiError> {
    raw_job_id
        .parse()
        .map_err(|_| ApiError::new("INVALID_JOB_ID", "job ID must be a canonical identifier"))
}

fn parse_task_id(raw_task_id: &str) -> Result<TaskId, ApiError> {
    raw_task_id
        .parse()
        .map_err(|_| ApiError::new("INVALID_TASK_ID", "task ID must be a canonical identifier"))
}

fn parse_turn_id(raw_turn_id: &str) -> Result<TurnId, ApiError> {
    raw_turn_id
        .parse()
        .map_err(|_| ApiError::new("INVALID_TURN_ID", "turn ID must be a canonical identifier"))
}

struct LogQuery {
    stream: LogStream,
    offset: u64,
    limit: u32,
}

fn parse_log_query(raw_query: Option<&str>) -> Result<LogQuery, ApiError> {
    let mut values = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        if !matches!(key.as_ref(), "stream" | "offset" | "limit")
            || values
                .insert(key.into_owned(), value.into_owned())
                .is_some()
        {
            return Err(ApiError::new(
                "INVALID_LOG_QUERY",
                "log query must contain one stream, offset, and limit",
            ));
        }
    }

    let stream = match values.remove("stream").as_deref() {
        Some("stdout") => LogStream::Stdout,
        Some("stderr") => LogStream::Stderr,
        _ => {
            return Err(ApiError::new(
                "INVALID_LOG_STREAM",
                "log stream must be stdout or stderr",
            ));
        }
    };
    let offset = values
        .remove("offset")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| ApiError::new("INVALID_LOG_RANGE", "log offset must be an unsigned byte"))?;
    let limit: u32 = values
        .remove("limit")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            ApiError::new(
                "INVALID_LOG_RANGE",
                "log limit must be between 1 and 65536 bytes",
            )
        })?;
    if !values.is_empty() || !(1..=MAX_LOG_LIMIT).contains(&limit) {
        return Err(ApiError::new(
            "INVALID_LOG_RANGE",
            "log limit must be between 1 and 65536 bytes",
        ));
    }
    Ok(LogQuery {
        stream,
        offset,
        limit,
    })
}

#[cfg(test)]
mod event_tests {
    use super::*;
    use crate::controller::events::Seq;
    use tokio::sync::mpsc;
    #[tokio::test]
    async fn encoded_controls_have_no_ids_and_heartbeat_has_a_keepalive_comment() {
        use crate::controller::events::WireEvent;
        let (sender, receiver) = mpsc::channel(8);
        sender
            .try_send(ViewerMessage::ControllerEvent(WireEvent {
                schema_version: 1,
                journal_id: uuid::Uuid::from_u128(1),
                seq: Seq::new(9_007_199_254_740_993),
                time_millis: 0,
                kind: "queue.changed".into(),
                data: serde_json::json!({}),
            }))
            .unwrap();
        sender
            .try_send(ViewerMessage::SnapshotReady { revision: 5 })
            .unwrap();
        sender.try_send(ViewerMessage::Heartbeat).unwrap();
        sender
            .try_send(ViewerMessage::Unavailable {
                code: "private path must not escape".into(),
            })
            .unwrap();
        drop(sender);
        let (_shutdown, receiver_shutdown) = watch::channel(false);
        let response = Sse::new(ResponseEvents {
            messages: ReceiverStream::new(receiver),
            shutdown: WatchStream::new(receiver_shutdown),
        })
        .into_response();
        let bytes = to_bytes(response.into_body(), 4_096).await.unwrap();
        let encoded = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(encoded.contains("id: 00000000-0000-0000-0000-000000000001:9007199254740993"));
        assert!(encoded.contains("\"seq\":\"9007199254740993\""));
        assert!(encoded.contains("event: snapshot.ready"));
        assert!(encoded.contains("event: heartbeat"));
        assert!(encoded.contains(": keepalive"));
        assert!(encoded.contains("event: snapshot_required"));
        assert!(encoded.contains("\"window\":null"));
        assert!(!encoded.contains("private path"));
        assert_eq!(encoded.matches("id:").count(), 1);
    }

    #[tokio::test]
    async fn shutdown_discards_an_unread_sse_queue_before_draining() {
        let (sender, receiver) = mpsc::channel(8);
        sender.try_send(ViewerMessage::Heartbeat).unwrap();
        sender
            .try_send(ViewerMessage::SnapshotReady { revision: 7 })
            .unwrap();
        drop(sender);
        let (shutdown, receiver_shutdown) = watch::channel(false);
        shutdown.send(true).unwrap();
        let response = Sse::new(ResponseEvents {
            messages: ReceiverStream::new(receiver),
            shutdown: WatchStream::new(receiver_shutdown),
        })
        .into_response();
        assert!(
            to_bytes(response.into_body(), 4_096)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
