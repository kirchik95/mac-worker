//! Server-side task.say / task.cancel / task.close preparation and execution.
//!
//! Preparation reads the controller [`ClientStateStore`] and freezes a typed
//! [`PreparedTaskMutation`]. Execution consumes only that saved value and an
//! already-configured [`TaskClient`](crate::task_client::TaskClient). FLOW
//! persists the preparation (STORE rev2) and maps [`TaskReport`] into ACK.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    client_state::ClientStateStore,
    controller::ControllerRequest,
    error::WorkerError,
    prepared_followup::PreparedFollowup,
    task::{LocalTaskRecord, TaskId, TurnId, TurnSummary},
    task_client::{TaskClient, TaskReport, task_error},
};

pub const COMMAND_SAY: &str = "task.say";
pub const COMMAND_CANCEL: &str = "task.cancel";
pub const COMMAND_CLOSE: &str = "task.close";

/// Frozen mutation identity. Persist this before any task/queue/runner write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", deny_unknown_fields)]
pub enum PreparedTaskMutation {
    #[serde(rename = "task.say")]
    Say { prepared: PreparedFollowup },
    #[serde(rename = "task.cancel")]
    Cancel {
        expected: LocalTaskRecord,
        created_at_millis: u64,
    },
    #[serde(rename = "task.close")]
    Close {
        expected: LocalTaskRecord,
        discard: bool,
        created_at_millis: u64,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SayBody {
    task_id: TaskId,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelBody {
    task_id: TaskId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CloseBody {
    task_id: TaskId,
    #[serde(default)]
    discard: bool,
}

impl PreparedTaskMutation {
    pub fn command(&self) -> &'static str {
        match self {
            Self::Say { .. } => COMMAND_SAY,
            Self::Cancel { .. } => COMMAND_CANCEL,
            Self::Close { .. } => COMMAND_CLOSE,
        }
    }

    pub fn task_id(&self) -> TaskId {
        match self {
            Self::Say { prepared } => prepared.task_id(),
            Self::Cancel { expected, .. } | Self::Close { expected, .. } => {
                expected.meta().task_id()
            }
        }
    }

    /// New say turn, or the fenced last turn. `None` only when cancel/close
    /// freeze a snapshot that has no turns.
    pub fn turn_id(&self) -> Option<TurnId> {
        match self {
            Self::Say { prepared } => Some(prepared.turn_id()),
            Self::Cancel { expected, .. } | Self::Close { expected, .. } => {
                expected.status().turns().last().map(TurnSummary::turn_id)
            }
        }
    }

    pub fn created_at_millis(&self) -> u64 {
        match self {
            Self::Say { prepared } => prepared.created_at_millis(),
            Self::Cancel {
                created_at_millis, ..
            }
            | Self::Close {
                created_at_millis, ..
            } => *created_at_millis,
        }
    }

    pub fn discard(&self) -> Option<bool> {
        match self {
            Self::Close { discard, .. } => Some(*discard),
            Self::Say { .. } | Self::Cancel { .. } => None,
        }
    }
}

/// Typed body parse only. Validates the mutation body and returns the
/// targeted task id without loading or freezing expected state.
pub fn mutation_task_id(request: &ControllerRequest) -> Result<TaskId, WorkerError> {
    match request.command() {
        COMMAND_SAY => Ok(parse_body::<SayBody>(request.body(), COMMAND_SAY)?.task_id),
        COMMAND_CANCEL => Ok(parse_body::<CancelBody>(request.body(), COMMAND_CANCEL)?.task_id),
        COMMAND_CLOSE => Ok(parse_body::<CloseBody>(request.body(), COMMAND_CLOSE)?.task_id),
        _ => Err(invalid_request("unsupported controller command")),
    }
}

/// Load the server task row and freeze a typed preparation.
///
/// Rejects unsupported commands and malformed bodies before touching the
/// store. The load itself is a read; this function does not write task,
/// queue, or runner state. `now_millis` is the trusted server timestamp.
pub fn prepare_task_mutation(
    request: &ControllerRequest,
    store: &ClientStateStore,
    now_millis: u64,
) -> Result<PreparedTaskMutation, WorkerError> {
    match request.command() {
        COMMAND_SAY => prepare_say(request.body(), store, now_millis),
        COMMAND_CANCEL => prepare_cancel(request.body(), store, now_millis),
        COMMAND_CLOSE => prepare_close(request.body(), store, now_millis),
        _ => Err(invalid_request("unsupported controller command")),
    }
}

/// Invoke the existing fenced TaskClient methods from a saved preparation.
///
/// `task.say` is always detached (`attached = false`) and must not write RPC
/// stdout. Does not reload expected state, model, limits, or time.
pub fn execute_task_mutation(
    client: &TaskClient<'_>,
    prepared: &PreparedTaskMutation,
) -> Result<TaskReport, WorkerError> {
    match prepared {
        PreparedTaskMutation::Say { prepared } => {
            client.say_prepared(prepared, false, &mut std::io::sink(), &mut std::io::sink())
        }
        PreparedTaskMutation::Cancel { expected, .. } => client.cancel_from_expected(expected),
        PreparedTaskMutation::Close {
            expected, discard, ..
        } => client.close_from_expected(expected, *discard),
    }
}

fn prepare_say(
    body: &Value,
    store: &ClientStateStore,
    now_millis: u64,
) -> Result<PreparedTaskMutation, WorkerError> {
    let body: SayBody = parse_body(body, COMMAND_SAY)?;
    let expected = load_server_task(store, body.task_id)?;
    let prepared =
        PreparedFollowup::prepare(&expected, body.message, TurnId::generate(), now_millis)?;
    Ok(PreparedTaskMutation::Say { prepared })
}

fn prepare_cancel(
    body: &Value,
    store: &ClientStateStore,
    now_millis: u64,
) -> Result<PreparedTaskMutation, WorkerError> {
    let body: CancelBody = parse_body(body, COMMAND_CANCEL)?;
    let expected = load_server_task(store, body.task_id)?;
    Ok(PreparedTaskMutation::Cancel {
        expected,
        created_at_millis: now_millis,
    })
}

fn prepare_close(
    body: &Value,
    store: &ClientStateStore,
    now_millis: u64,
) -> Result<PreparedTaskMutation, WorkerError> {
    let body: CloseBody = parse_body(body, COMMAND_CLOSE)?;
    let expected = load_server_task(store, body.task_id)?;
    Ok(PreparedTaskMutation::Close {
        expected,
        discard: body.discard,
        created_at_millis: now_millis,
    })
}

fn load_server_task(
    store: &ClientStateStore,
    task_id: TaskId,
) -> Result<LocalTaskRecord, WorkerError> {
    store
        .load_task_optional(task_id)?
        .ok_or_else(|| task_error("TASK_NOT_FOUND", "task is not present in controller state"))
}

fn parse_body<T: DeserializeOwned>(body: &Value, command: &str) -> Result<T, WorkerError> {
    serde_json::from_value(body.clone())
        .map_err(|_| invalid_request(&format!("{command} body is invalid")))
}

fn invalid_request(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("INVALID_REQUEST: {message}"))
}
