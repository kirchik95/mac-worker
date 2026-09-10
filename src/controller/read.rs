//! Controller read RPC: Status, List, bounded Logs chunks, Diff, Result.
//!
//! These commands do not publish durable request rows and do not mint task or
//! turn UUIDs. The payload is the existing TaskClient projection.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::AgentKind,
    controller::protocol::{ControllerRequest, encode_json_frame},
    error::WorkerError,
    job::MAX_LOG_CHUNK_BYTES,
    protocol::PROTOCOL_VERSION,
    task::{RunId, RunnerState, TaskId, TaskState, TaskStatus, TurnId},
    task_client::{TaskClient, TaskListFilter, TaskLogChunkReport, TaskReport, TaskResultReport},
    task_view::TaskListProjection,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerReadReply<T> {
    protocol_version: u32,
    command: String,
    request_id: String,
    payload_sha256: String,
    result: T,
}

impl<T> ControllerReadReply<T> {
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }

    pub fn result(&self) -> &T {
        &self.result
    }

    pub fn into_result(self) -> T {
        self.result
    }

    pub(crate) fn from_request(request: &ControllerRequest, result: T) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            command: request.command().to_owned(),
            request_id: request.request_id().to_owned(),
            payload_sha256: request.payload_sha256().to_owned(),
            result,
        }
    }

    pub fn verify_envelope(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION
            || self.command != request.command()
            || self.request_id != request.request_id()
            || self.payload_sha256 != request.payload_sha256()
        {
            return Err(invalid_controller_reply());
        }
        Ok(())
    }
}

pub trait ControllerReadIdentity {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError>;
}

pub(crate) fn invalid_controller_reply() -> WorkerError {
    WorkerError::Unavailable(
        "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
    )
}

pub fn is_read_command(command: &str) -> bool {
    matches!(
        command,
        "task.status" | "task.list" | "task.logs" | "task.diff" | "task.result"
    )
}

pub fn serve_read_command(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<Vec<u8>, WorkerError> {
    match request.command() {
        "task.status" => Ok(encode_json_frame(&status_reply(request, client)?)?),
        "task.list" => Ok(encode_json_frame(&list_reply(request, client)?)?),
        "task.logs" => Ok(encode_json_frame(&logs_reply(request, client)?)?),
        "task.diff" => Ok(encode_json_frame(&diff_reply(request, client)?)?),
        "task.result" => Ok(encode_json_frame(&result_reply(request, client)?)?),
        other => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: unsupported controller command {other}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerTaskStatusResult {
    task_id: TaskId,
    run_id: Option<RunId>,
    status: TaskStatus,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    events: Vec<Value>,
    runner: Option<RunnerState>,
    exit_code: Option<u8>,
}

impl ControllerTaskStatusResult {
    pub fn from_report(report: &TaskReport) -> Self {
        Self {
            task_id: report.task_id(),
            run_id: report.run_id(),
            status: report.status().clone(),
            warnings: report.warnings().to_vec(),
            events: report.events().to_vec(),
            runner: report.runner(),
            exit_code: report.exit_code(),
        }
    }

    pub fn into_report(self) -> TaskReport {
        TaskReport::from_controller(
            self.task_id,
            self.run_id,
            self.status,
            self.warnings,
            self.events,
            self.runner,
            self.exit_code,
        )
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

impl ControllerReadIdentity for ControllerTaskStatusResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        require_matching_task_id(request, self.task_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerTaskLogsResult {
    task_id: TaskId,
    turn_id: TurnId,
    turn_number: u32,
    agent: String,
    offset: u64,
    next_offset: u64,
    exhausted: bool,
    complete: bool,
    raw: bool,
    bytes_base64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure: Option<String>,
}

impl ControllerTaskLogsResult {
    pub fn from_chunk(chunk: &TaskLogChunkReport, raw: bool) -> Self {
        Self {
            task_id: chunk.task_id(),
            turn_id: chunk.turn_id(),
            turn_number: chunk.turn_number(),
            agent: agent_name(chunk.agent()).to_owned(),
            offset: chunk.offset(),
            next_offset: chunk.next_offset(),
            exhausted: chunk.exhausted(),
            complete: chunk.complete(),
            raw,
            bytes_base64: base64::engine::general_purpose::STANDARD.encode(chunk.bytes()),
            failure: chunk.failure().map(str::to_owned),
        }
    }

    pub fn decode_bytes(&self) -> Result<Vec<u8>, WorkerError> {
        base64::engine::general_purpose::STANDARD
            .decode(self.bytes_base64.as_bytes())
            .map_err(|_| {
                WorkerError::Protocol(
                    "CONTROLLER_TRANSPORT: task.logs bytes were not valid base64".into(),
                )
            })
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn raw(&self) -> bool {
        self.raw
    }

    pub fn turn_id(&self) -> TurnId {
        self.turn_id
    }

    pub fn turn_number(&self) -> u32 {
        self.turn_number
    }

    pub fn agent(&self) -> Result<AgentKind, WorkerError> {
        parse_agent(&self.agent).ok_or_else(invalid_controller_reply)
    }

    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
}

impl ControllerReadIdentity for ControllerTaskLogsResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        require_matching_task_id(request, self.task_id)?;
        let requested_offset = optional_u64(request.body(), "offset", "task.logs")?.unwrap_or(0);
        let requested_raw = optional_bool(request.body(), "raw", "task.logs")?.unwrap_or(false);
        let requested_limit = optional_u64(request.body(), "limit", "task.logs")?
            .map(|value| usize::try_from(value).unwrap_or(MAX_LOG_CHUNK_BYTES))
            .unwrap_or(MAX_LOG_CHUNK_BYTES)
            .min(MAX_LOG_CHUNK_BYTES)
            .max(1);
        if self.offset != requested_offset
            || self.raw != requested_raw
            || self.next_offset < self.offset
            || (self.next_offset == self.offset && !self.exhausted)
        {
            return Err(invalid_controller_reply());
        }
        if let Some(turn_id) = optional_turn_id(request.body(), "task.logs")?
            && self.turn_id != turn_id
        {
            return Err(invalid_controller_reply());
        }
        if let Some(turn) = optional_u32(request.body(), "turn", "task.logs")?
            && self.turn_number != turn
        {
            return Err(invalid_controller_reply());
        }
        self.agent()?;
        let advance = self.next_offset - self.offset;
        if advance > requested_limit as u64 {
            return Err(invalid_controller_reply());
        }
        let bytes = self.decode_bytes()?;
        if u64::try_from(bytes.len()).ok() != Some(advance) {
            return Err(invalid_controller_reply());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerTaskDiffResult {
    task_id: TaskId,
    stat: bool,
    text: String,
}

impl ControllerTaskDiffResult {
    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn stat(&self) -> bool {
        self.stat
    }
}

impl ControllerReadIdentity for ControllerTaskDiffResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        require_matching_task_id(request, self.task_id)?;
        let requested_stat = match request.body().get("stat") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => return Err(invalid_controller_reply()),
        };
        if self.stat != requested_stat {
            return Err(invalid_controller_reply());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerTaskResult {
    task_id: TaskId,
    status: TaskStatus,
    branch: String,
    fetch: String,
}

impl ControllerTaskResult {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn into_report(self) -> TaskResultReport {
        TaskResultReport::from_controller(self.task_id, self.status, self.branch, self.fetch)
    }
}

impl ControllerReadIdentity for ControllerTaskResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        require_matching_task_id(request, self.task_id)
    }
}

impl ControllerReadIdentity for TaskListProjection {
    fn verify_payload(&self, _request: &ControllerRequest) -> Result<(), WorkerError> {
        Ok(())
    }
}

fn status_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<ControllerTaskStatusResult>, WorkerError> {
    let task_id = required_task_id(request, "task.status")?;
    reject_unknown_keys(request.body(), &["task_id"], "task.status")?;
    let report = map_missing_task(client.status(task_id))?;
    Ok(reply(
        request,
        ControllerTaskStatusResult::from_report(&report),
    ))
}

fn list_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<TaskListProjection>, WorkerError> {
    reject_unknown_keys(
        request.body(),
        &["run", "state", "outcome", "full"],
        "task.list",
    )?;
    let run_id = match optional_string(request.body(), "run")? {
        Some(value) => Some(client.resolve_run(&value)?),
        None => None,
    };
    let state = match optional_string(request.body(), "state")? {
        Some(value) => Some(parse_task_state(&value)?),
        None => None,
    };
    let outcome = optional_string(request.body(), "outcome")?
        .map(parse_task_outcome_kind)
        .transpose()?
        .map(str::to_owned);
    let full = match request.body().get("full") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: task.list full must be a boolean".into(),
            ));
        }
    };
    let report = client.list(TaskListFilter {
        run_id,
        state,
        outcome,
        full,
    })?;
    Ok(reply(request, report.projection().clone()))
}

fn logs_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<ControllerTaskLogsResult>, WorkerError> {
    reject_unknown_keys(
        request.body(),
        &[
            "task_id", "turn", "turn_id", "offset", "limit", "raw", "follow",
        ],
        "task.logs",
    )?;
    let task_id = required_task_id(request, "task.logs")?;
    let turn = optional_u32(request.body(), "turn", "task.logs")?;
    let pinned_turn_id = optional_turn_id(request.body(), "task.logs")?;
    let offset = optional_u64(request.body(), "offset", "task.logs")?.unwrap_or(0);
    let limit = optional_u64(request.body(), "limit", "task.logs")?
        .map(|value| usize::try_from(value).unwrap_or(MAX_LOG_CHUNK_BYTES))
        .unwrap_or(MAX_LOG_CHUNK_BYTES)
        .min(MAX_LOG_CHUNK_BYTES);
    let raw = optional_bool(request.body(), "raw", "task.logs")?.unwrap_or(false);
    let follow = optional_bool(request.body(), "follow", "task.logs")?.unwrap_or(false);
    let chunk = client.log_chunk(task_id, turn, pinned_turn_id, offset, limit, raw, follow)?;
    Ok(reply(
        request,
        ControllerTaskLogsResult::from_chunk(&chunk, raw),
    ))
}

fn diff_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<ControllerTaskDiffResult>, WorkerError> {
    reject_unknown_keys(request.body(), &["task_id", "stat"], "task.diff")?;
    let task_id = required_task_id(request, "task.diff")?;
    let stat = match request.body().get("stat") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: task.diff stat must be a boolean".into(),
            ));
        }
    };
    let text = map_missing_task(client.diff_text(task_id, stat))?;
    Ok(reply(
        request,
        ControllerTaskDiffResult {
            task_id,
            stat,
            text,
        },
    ))
}

fn result_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<ControllerTaskResult>, WorkerError> {
    let task_id = required_task_id(request, "task.result")?;
    reject_unknown_keys(request.body(), &["task_id"], "task.result")?;
    let report = map_missing_task(client.result(task_id))?;
    Ok(reply(
        request,
        ControllerTaskResult {
            task_id: report.task_id(),
            status: report.status().clone(),
            branch: report.branch().to_owned(),
            fetch: report.fetch_instruction().to_owned(),
        },
    ))
}

fn reply<T>(request: &ControllerRequest, result: T) -> ControllerReadReply<T> {
    ControllerReadReply::from_request(request, result)
}

fn require_matching_task_id(
    request: &ControllerRequest,
    task_id: TaskId,
) -> Result<(), WorkerError> {
    let Some(value) = request.body().get("task_id").and_then(Value::as_str) else {
        return Err(invalid_controller_reply());
    };
    let parsed: TaskId = value.parse().map_err(|_| invalid_controller_reply())?;
    if parsed != task_id {
        return Err(invalid_controller_reply());
    }
    Ok(())
}

fn required_task_id(request: &ControllerRequest, command: &str) -> Result<TaskId, WorkerError> {
    let Some(value) = request.body().get("task_id").and_then(Value::as_str) else {
        return Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: {command} requires task_id"
        )));
    };
    value.parse().map_err(|_| {
        WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: {command} task_id is invalid"
        ))
    })
}

fn optional_turn_id(body: &Value, command: &str) -> Result<Option<TurnId>, WorkerError> {
    match optional_string(body, "turn_id")? {
        None => Ok(None),
        Some(value) => value.parse().map(Some).map_err(|_| {
            WorkerError::Protocol(format!(
                "CONTROLLER_TRANSPORT: {command} turn_id is invalid"
            ))
        }),
    }
}

fn agent_name(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

fn parse_agent(value: &str) -> Option<AgentKind> {
    match value {
        "codex" => Some(AgentKind::Codex),
        "claude" => Some(AgentKind::Claude),
        "cursor" => Some(AgentKind::Cursor),
        "opencode" => Some(AgentKind::Opencode),
        _ => None,
    }
}

fn optional_string(body: &Value, key: &str) -> Result<Option<String>, WorkerError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: {key} must be a string"
        ))),
    }
}

fn optional_bool(body: &Value, key: &str, command: &str) -> Result<Option<bool>, WorkerError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: {command} {key} must be a boolean"
        ))),
    }
}

fn optional_u32(body: &Value, key: &str, command: &str) -> Result<Option<u32>, WorkerError> {
    match optional_u64(body, key, command)? {
        None => Ok(None),
        Some(value) => u32::try_from(value).map(Some).map_err(|_| {
            WorkerError::Protocol(format!(
                "CONTROLLER_TRANSPORT: {command} {key} must fit in 32 bits"
            ))
        }),
    }
}

fn optional_u64(body: &Value, key: &str, command: &str) -> Result<Option<u64>, WorkerError> {
    match body.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .ok_or_else(|| {
                WorkerError::Protocol(format!(
                    "CONTROLLER_TRANSPORT: {command} {key} must be an unsigned integer"
                ))
            })
            .map(Some),
        Some(_) => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: {command} {key} must be an unsigned integer"
        ))),
    }
}

fn reject_unknown_keys(body: &Value, allowed: &[&str], command: &str) -> Result<(), WorkerError> {
    let Some(object) = body.as_object() else {
        return Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: {command} body must be a JSON object"
        )));
    };
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(WorkerError::Protocol(format!(
            "INVALID_REQUEST: {command} body contained unexpected key {key}"
        )));
    }
    Ok(())
}

fn parse_task_state(value: &str) -> Result<TaskState, WorkerError> {
    match value {
        "queued" => Ok(TaskState::Queued),
        "active" => Ok(TaskState::Active),
        "open" => Ok(TaskState::Open),
        "closed" => Ok(TaskState::Closed),
        "abandoned" => Ok(TaskState::Abandoned),
        "lost" => Ok(TaskState::Lost),
        _ => Err(WorkerError::task(
            "TASK_CONFIG_INVALID",
            "unknown task state",
        )),
    }
}

fn parse_task_outcome_kind(value: String) -> Result<&'static str, WorkerError> {
    let normalized = value.replace('-', "_");
    crate::task::TaskOutcome::KINDS
        .into_iter()
        .find(|kind| *kind == normalized)
        .ok_or(WorkerError::task(
            "TASK_CONFIG_INVALID",
            "unknown task outcome",
        ))
}

fn map_missing_task<T>(result: Result<T, WorkerError>) -> Result<T, WorkerError> {
    match result {
        Err(WorkerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Err(
            WorkerError::task("TASK_NOT_FOUND", "task is not present in controller state"),
        ),
        other => other,
    }
}
