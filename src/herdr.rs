//! Client for the herdr socket API.
//!
//! Herdr, the terminal workspace manager the operator runs on the MacBook
//! and on every worker, exposes a Unix socket that speaks one JSON object
//! per line: `{"id", "method", "params"}` in, `{"id", "result"}` or
//! `{"id", "error": {"code", "message"}}` out.  Herdr's own agent
//! integrations use that surface to report agent state.  mac-worker uses
//! it for the same purpose, to show pool turns in the operator's sidebar,
//! and to notify the operator when a turn ends.
//!
//! Every call opens one connection, sends one request, reads one answer,
//! and closes.  A Unix socket connect either completes or fails at once,
//! so the connect deadline bounds the request write and the response
//! deadline bounds the answer.  Failures are classified so a caller can
//! stay silent about an absent or unresponsive herdr without ever waiting
//! on it: nothing here retries, and nothing here blocks past its deadline.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fmt,
    io::{self, BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Value, json};

/// Socket of herdr's default session, relative to an account home.
pub const DEFAULT_SOCKET_RELATIVE: &str = ".config/herdr/herdr.sock";
/// Set by herdr for processes it runs: the socket of the session that
/// started them, which is the one the operator is looking at.
pub const SOCKET_ENV_NAME: &str = "HERDR_SOCKET_PATH";
/// The `source` every report from mac-worker carries; herdr keys report
/// authority on it.
pub const SOURCE: &str = "mac-worker";
/// Bound on connecting and writing one request.
pub const CONNECT_DEADLINE: Duration = Duration::from_millis(500);
/// Bound on waiting for one answer.
pub const RESPONSE_DEADLINE: Duration = Duration::from_secs(3);

/// Why a call did not produce a result.
#[derive(Debug)]
pub enum HerdrError {
    /// No socket at the path: herdr is not running for this account.
    Absent,
    /// The socket exists but nothing accepts on it.
    Refused(io::Error),
    /// Herdr did not answer within the response deadline.
    Timeout,
    /// Reading or writing the connection failed for another reason.
    Io(io::Error),
    /// The answer was not a herdr response to this request.
    Protocol(String),
    /// Herdr answered with an error body.
    Server { code: String, message: String },
}

impl HerdrError {
    /// A short, path-free label suitable for a diagnostic line.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Refused(_) => "refused",
            Self::Timeout => "timeout",
            Self::Io(_) => "io",
            Self::Protocol(_) => "protocol",
            Self::Server { .. } => "server",
        }
    }
}

impl fmt::Display for HerdrError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => write!(formatter, "herdr socket is absent"),
            Self::Refused(error) => {
                write!(formatter, "herdr socket refused the connection: {error}")
            }
            Self::Timeout => write!(formatter, "herdr did not answer within the deadline"),
            Self::Io(error) => write!(formatter, "herdr connection failed: {error}"),
            Self::Protocol(message) => write!(formatter, "herdr protocol error: {message}"),
            Self::Server { code, message } => write!(formatter, "herdr error {code}: {message}"),
        }
    }
}

impl std::error::Error for HerdrError {}

/// Where a herdr session listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdrSocket {
    path: PathBuf,
}

impl HerdrSocket {
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The default session of the account whose home this is.
    pub fn default_for_home(home: &Path) -> Self {
        Self::at(home.join(DEFAULT_SOCKET_RELATIVE))
    }

    /// The session herdr named in the environment when it started this
    /// process, else the account's default session.
    pub fn from_env_or_home<F>(lookup: F, home: &Path) -> Self
    where
        F: Fn(&str) -> Option<OsString>,
    {
        match lookup(SOCKET_ENV_NAME) {
            Some(value) if !value.is_empty() => Self::at(PathBuf::from(value)),
            _ => Self::default_for_home(home),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Lifecycle states herdr accepts from an external source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    Idle,
    Working,
    Blocked,
    Unknown,
}

impl AgentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Working => "working",
            Self::Blocked => "blocked",
            Self::Unknown => "unknown",
        }
    }
}

/// Sounds herdr can play with a notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationSound {
    None,
    Done,
    Request,
}

impl NotificationSound {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Done => "done",
            Self::Request => "request",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    pub workspace_id: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tab {
    pub tab_id: String,
    pub label: Option<String>,
    pub workspace_id: Option<String>,
}

/// What `workspace.create` and `tab.create` hand back: the new container
/// and the pane herdr opened inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Created {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundProcess {
    pub pid: u32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessInfo {
    pub shell_pid: Option<u32>,
    pub foreground: Vec<ForegroundProcess>,
}

impl ProcessInfo {
    /// The pane's shell is at its prompt with nothing running in front of it.
    pub fn is_idle_shell(&self) -> bool {
        match (&self.foreground[..], self.shell_pid) {
            ([only], Some(shell)) => only.pid == shell,
            _ => false,
        }
    }
}

/// Sidebar metadata for a pane; every field is optional and only set
/// fields are sent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PaneMetadata {
    pub agent: Option<String>,
    pub title: Option<String>,
    pub display_agent: Option<String>,
    pub state_labels: BTreeMap<String, String>,
    pub tokens: BTreeMap<String, String>,
    pub ttl_ms: Option<u64>,
}

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_request_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default();
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("mac-worker:{millis}:{counter}")
}

/// A monotonic-enough sequence number for reports, in the scheme herdr's
/// own hooks use.
pub fn report_seq() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

/// One herdr session, addressed for one-shot calls.
#[derive(Debug, Clone)]
pub struct HerdrClient {
    socket: HerdrSocket,
    connect_deadline: Duration,
    response_deadline: Duration,
}

impl HerdrClient {
    pub fn new(socket: HerdrSocket) -> Self {
        Self::with_deadlines(socket, CONNECT_DEADLINE, RESPONSE_DEADLINE)
    }

    pub fn with_deadlines(
        socket: HerdrSocket,
        connect_deadline: Duration,
        response_deadline: Duration,
    ) -> Self {
        Self {
            socket,
            connect_deadline,
            response_deadline,
        }
    }

    pub fn socket(&self) -> &HerdrSocket {
        &self.socket
    }

    /// Send one request and return its `result`.
    pub fn request(&self, method: &str, params: Value) -> Result<Value, HerdrError> {
        let id = next_request_id();
        let mut stream = match UnixStream::connect(self.socket.path()) {
            Ok(stream) => stream,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(HerdrError::Absent);
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                return Err(HerdrError::Refused(error));
            }
            Err(error) => return Err(HerdrError::Io(error)),
        };
        stream
            .set_write_timeout(Some(self.connect_deadline))
            .map_err(HerdrError::Io)?;
        stream
            .set_read_timeout(Some(self.response_deadline))
            .map_err(HerdrError::Io)?;
        let request = json!({ "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&request)
            .map_err(|error| HerdrError::Protocol(error.to_string()))?;
        line.push('\n');
        stream.write_all(line.as_bytes()).map_err(classify_io)?;
        stream.flush().map_err(classify_io)?;

        let mut reader = BufReader::new(stream);
        let mut answer = String::new();
        let read = reader.read_line(&mut answer).map_err(classify_io)?;
        if read == 0 {
            return Err(HerdrError::Protocol(
                "herdr closed the connection without answering".into(),
            ));
        }
        let value: Value = serde_json::from_str(answer.trim_end())
            .map_err(|error| HerdrError::Protocol(format!("herdr answer is not JSON: {error}")))?;
        if value.get("id").and_then(Value::as_str) != Some(id.as_str()) {
            return Err(HerdrError::Protocol(
                "herdr answered a different request".into(),
            ));
        }
        if let Some(error) = value.get("error") {
            return Err(HerdrError::Server {
                code: string_at(error, "code").unwrap_or_else(|| "unknown".to_owned()),
                message: string_at(error, "message").unwrap_or_default(),
            });
        }
        value.get("result").cloned().ok_or_else(|| {
            HerdrError::Protocol("herdr answer carries neither result nor error".into())
        })
    }

    pub fn ping(&self) -> Result<(), HerdrError> {
        self.request("ping", json!({})).map(|_| ())
    }

    pub fn workspace_list(&self) -> Result<Vec<Workspace>, HerdrError> {
        let result = self.request("workspace.list", json!({}))?;
        let workspaces = array_at(&result, "workspaces")?;
        workspaces
            .iter()
            .map(|workspace| {
                Ok(Workspace {
                    workspace_id: required_string(workspace, "workspace_id")?,
                    label: string_at(workspace, "label"),
                })
            })
            .collect()
    }

    /// Create a workspace without moving the operator's focus.  Herdr picks
    /// the working directory when none is given, so no path has to cross
    /// the socket.
    pub fn workspace_create(&self, label: &str, cwd: Option<&Path>) -> Result<Created, HerdrError> {
        let mut params = Map::new();
        params.insert("label".into(), json!(label));
        params.insert("focus".into(), json!(false));
        if let Some(cwd) = cwd {
            params.insert("cwd".into(), json!(cwd));
        }
        let result = self.request("workspace.create", Value::Object(params))?;
        Ok(Created {
            workspace_id: required_string(object_at(&result, "workspace")?, "workspace_id")?,
            tab_id: required_string(object_at(&result, "tab")?, "tab_id")?,
            pane_id: required_string(object_at(&result, "root_pane")?, "pane_id")?,
        })
    }

    pub fn tab_list(&self, workspace_id: &str) -> Result<Vec<Tab>, HerdrError> {
        let result = self.request("tab.list", json!({ "workspace_id": workspace_id }))?;
        array_at(&result, "tabs")?
            .iter()
            .map(|tab| {
                Ok(Tab {
                    tab_id: required_string(tab, "tab_id")?,
                    label: string_at(tab, "label"),
                    workspace_id: string_at(tab, "workspace_id"),
                })
            })
            .collect()
    }

    /// Create a tab in a workspace without moving the operator's focus.
    pub fn tab_create(
        &self,
        workspace_id: &str,
        label: &str,
        cwd: Option<&Path>,
    ) -> Result<Created, HerdrError> {
        let mut params = Map::new();
        params.insert("workspace_id".into(), json!(workspace_id));
        params.insert("label".into(), json!(label));
        params.insert("focus".into(), json!(false));
        if let Some(cwd) = cwd {
            params.insert("cwd".into(), json!(cwd));
        }
        let result = self.request("tab.create", Value::Object(params))?;
        let tab = object_at(&result, "tab")?;
        Ok(Created {
            workspace_id: string_at(tab, "workspace_id").unwrap_or_else(|| workspace_id.to_owned()),
            tab_id: required_string(tab, "tab_id")?,
            pane_id: required_string(object_at(&result, "root_pane")?, "pane_id")?,
        })
    }

    pub fn tab_close(&self, tab_id: &str) -> Result<(), HerdrError> {
        self.request("tab.close", json!({ "tab_id": tab_id }))
            .map(|_| ())
    }

    /// Close a whole workspace with every tab in it.
    pub fn workspace_close(&self, workspace_id: &str) -> Result<(), HerdrError> {
        self.request("workspace.close", json!({ "workspace_id": workspace_id }))
            .map(|_| ())
    }

    pub fn pane_process_info(&self, pane_id: &str) -> Result<ProcessInfo, HerdrError> {
        let result = self.request("pane.process_info", json!({ "pane_id": pane_id }))?;
        let info = object_at(&result, "process_info")?;
        let foreground = info
            .get("foreground_processes")
            .and_then(Value::as_array)
            .map(|processes| {
                processes
                    .iter()
                    .filter_map(|process| {
                        Some(ForegroundProcess {
                            pid: u32::try_from(process.get("pid")?.as_u64()?).ok()?,
                            name: string_at(process, "name").unwrap_or_default(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(ProcessInfo {
            shell_pid: info
                .get("shell_pid")
                .and_then(Value::as_u64)
                .and_then(|pid| u32::try_from(pid).ok()),
            foreground,
        })
    }

    /// Type text into a pane, followed by the given logical keys.
    pub fn pane_send_input(
        &self,
        pane_id: &str,
        text: &str,
        keys: &[&str],
    ) -> Result<(), HerdrError> {
        self.request(
            "pane.send_input",
            json!({ "pane_id": pane_id, "text": text, "keys": keys }),
        )
        .map(|_| ())
    }

    pub fn pane_report_agent(
        &self,
        pane_id: &str,
        agent: &str,
        state: AgentState,
        message: Option<&str>,
    ) -> Result<(), HerdrError> {
        let mut params = Map::new();
        params.insert("pane_id".into(), json!(pane_id));
        params.insert("source".into(), json!(SOURCE));
        params.insert("agent".into(), json!(agent));
        params.insert("state".into(), json!(state.as_str()));
        params.insert("seq".into(), json!(report_seq()));
        if let Some(message) = message {
            params.insert("message".into(), json!(message));
        }
        self.request("pane.report_agent", Value::Object(params))
            .map(|_| ())
    }

    pub fn pane_report_metadata(
        &self,
        pane_id: &str,
        metadata: &PaneMetadata,
    ) -> Result<(), HerdrError> {
        let mut params = Map::new();
        params.insert("pane_id".into(), json!(pane_id));
        params.insert("source".into(), json!(SOURCE));
        params.insert("seq".into(), json!(report_seq()));
        if let Some(agent) = &metadata.agent {
            params.insert("agent".into(), json!(agent));
        }
        if let Some(title) = &metadata.title {
            params.insert("title".into(), json!(title));
        }
        if let Some(display_agent) = &metadata.display_agent {
            params.insert("display_agent".into(), json!(display_agent));
        }
        if !metadata.state_labels.is_empty() {
            params.insert("state_labels".into(), json!(metadata.state_labels));
        }
        if !metadata.tokens.is_empty() {
            params.insert("tokens".into(), json!(metadata.tokens));
        }
        if let Some(ttl_ms) = metadata.ttl_ms {
            params.insert("ttl_ms".into(), json!(ttl_ms));
        }
        self.request("pane.report_metadata", Value::Object(params))
            .map(|_| ())
    }

    pub fn pane_release_agent(&self, pane_id: &str, agent: &str) -> Result<(), HerdrError> {
        self.request(
            "pane.release_agent",
            json!({ "pane_id": pane_id, "source": SOURCE, "agent": agent, "seq": report_seq() }),
        )
        .map(|_| ())
    }

    pub fn notification_show(
        &self,
        title: &str,
        body: Option<&str>,
        sound: NotificationSound,
    ) -> Result<(), HerdrError> {
        let mut params = Map::new();
        params.insert("title".into(), json!(title));
        params.insert("sound".into(), json!(sound.as_str()));
        if let Some(body) = body {
            params.insert("body".into(), json!(body));
        }
        self.request("notification.show", Value::Object(params))
            .map(|_| ())
    }
}

fn classify_io(error: io::Error) -> HerdrError {
    match error.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => HerdrError::Timeout,
        _ => HerdrError::Io(error),
    }
}

fn string_at(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn required_string(value: &Value, key: &str) -> Result<String, HerdrError> {
    string_at(value, key).ok_or_else(|| HerdrError::Protocol(format!("herdr answer lacks `{key}`")))
}

fn object_at<'a>(value: &'a Value, key: &str) -> Result<&'a Value, HerdrError> {
    match value.get(key) {
        Some(object) if object.is_object() => Ok(object),
        _ => Err(HerdrError::Protocol(format!("herdr answer lacks `{key}`"))),
    }
}

fn array_at<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>, HerdrError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| HerdrError::Protocol(format!("herdr answer lacks `{key}`")))
}
