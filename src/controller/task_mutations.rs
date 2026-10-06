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
    integration::{contracts::*, store::RootedIntegrationState},
    paths::PathLayout,
    prepared_followup::PreparedFollowup,
    task::{LocalTaskRecord, TaskId, TaskOutcome, TaskState, TurnId, TurnSummary},
    task_client::{TaskClient, TaskReport, task_error},
};

const COMMAND_SAY: &str = "task.say";
const COMMAND_CANCEL: &str = "task.cancel";
const COMMAND_CLOSE: &str = "task.close";

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

/// Private saved operation envelope. Disabled operations retain their exact encoding.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntegrationMutationBinding {
    integration_id: IntegrationId,
    epoch: u32,
    mutation: PreparedTaskMutation,
}

fn latest_ordinary_turn(
    paths: &PathLayout,
    record: &LocalTaskRecord,
) -> Result<Option<TurnId>, WorkerError> {
    for turn in record.status().turns().iter().rev() {
        if RootedIntegrationState::read_auxiliary(paths, record.meta().task_id(), turn.turn_id())?
            .is_none()
        {
            return Ok(Some(turn.turn_id()));
        }
    }
    Ok(None)
}

/// Unbound cancel/close has no cycle binding, so decode never refreshed it.
/// Importing this freeze's own Active turn as terminal Done, with a policy
/// already published, is the stop's allowed progress. A new turn, a different
/// meta, or a later epoch stays stale and conflicts at execution.
fn refresh_unbound_done_import(
    paths: &PathLayout,
    store: &ClientStateStore,
    mutation: &mut PreparedTaskMutation,
) -> Result<(), WorkerError> {
    let task = match &*mutation {
        PreparedTaskMutation::Cancel { expected, .. }
        | PreparedTaskMutation::Close { expected, .. } => expected.meta().task_id(),
        PreparedTaskMutation::Say { .. } => return Ok(()),
    };
    let current = match store.load_task(task) {
        Ok(current) => current,
        Err(_) => return Ok(()),
    };
    let refresh = match &*mutation {
        PreparedTaskMutation::Cancel { expected, .. }
        | PreparedTaskMutation::Close { expected, .. } => {
            let same_identity = current.meta() == expected.meta()
                && current
                    .status()
                    .turns()
                    .iter()
                    .map(TurnSummary::turn_id)
                    .eq(expected.status().turns().iter().map(TurnSummary::turn_id));
            let imported_done = expected.status().state() == TaskState::Active
                && expected
                    .status()
                    .turns()
                    .last()
                    .is_some_and(|turn| turn.terminal().is_none())
                && current.status().state() == TaskState::Open
                && current.status().turns().last().is_some_and(|turn| {
                    turn.terminal().is_some() && turn.outcome() == Some(&TaskOutcome::Done)
                });
            if !same_identity || !imported_done {
                false
            } else {
                let (policy, record) = RootedIntegrationState::read_task(paths, task)?;
                if policy.is_none() {
                    false
                } else if let Some(record) = record.as_ref() {
                    record.snapshot.epoch == 0
                        && latest_ordinary_turn(paths, &current)?
                            == Some(record.snapshot.source_turn_id)
                } else {
                    true
                }
            }
        }
        PreparedTaskMutation::Say { .. } => false,
    };
    if refresh {
        match mutation {
            PreparedTaskMutation::Cancel { expected, .. }
            | PreparedTaskMutation::Close { expected, .. } => *expected = current,
            PreparedTaskMutation::Say { .. } => {}
        }
    }
    Ok(())
}

fn expected_record(mutation: &PreparedTaskMutation) -> &LocalTaskRecord {
    match mutation {
        PreparedTaskMutation::Say { prepared } => prepared.expected(),
        PreparedTaskMutation::Cancel { expected, .. }
        | PreparedTaskMutation::Close { expected, .. } => expected,
    }
}

pub(super) fn encode_prepared_mutation(
    paths: &PathLayout,
    mutation: &PreparedTaskMutation,
) -> Result<Value, WorkerError> {
    let task = mutation.task_id();
    let (_, integration) = RootedIntegrationState::read_task(paths, task)?;
    if let Some(record) = integration {
        let mut latest = None;
        for turn in expected_record(mutation).status().turns().iter().rev() {
            if RootedIntegrationState::read_auxiliary(paths, task, turn.turn_id())?.is_none() {
                latest = Some(turn.turn_id());
                break;
            }
        }
        if latest == Some(record.snapshot.source_turn_id) {
            return serde_json::to_value(IntegrationMutationBinding {
                integration_id: record.snapshot.integration_id,
                epoch: record.snapshot.epoch,
                mutation: mutation.clone(),
            })
            .map_err(|_| IntegrationCode::IntegrationStateInvalid.error());
        }
    }
    serde_json::to_value(mutation).map_err(|_| invalid_request("prepared mutation is invalid"))
}

pub(super) fn decode_prepared_mutation(
    paths: &PathLayout,
    store: &ClientStateStore,
    value: &Value,
) -> Result<PreparedTaskMutation, WorkerError> {
    if value.get("integration_id").is_none() {
        let mut mutation = serde_json::from_value(value.clone())
            .map_err(|_| invalid_request("prepared mutation is invalid"))?;
        refresh_unbound_done_import(paths, store, &mut mutation)?;
        return Ok(mutation);
    }
    let mut bound: IntegrationMutationBinding = serde_json::from_value(value.clone())
        .map_err(|_| IntegrationCode::IntegrationStateInvalid.error())?;
    let task = bound.mutation.task_id();
    let current = store.load_task(task)?;
    // Say replay already bound to its own new turn cannot stop a newer cycle.
    if let PreparedTaskMutation::Say { prepared } = &bound.mutation
        && current.status().turns().last().map(TurnSummary::turn_id) == Some(prepared.turn_id())
    {
        return Ok(bound.mutation);
    }
    let (_, record) = RootedIntegrationState::read_task(paths, task)?;
    let record = record.ok_or_else(integration_unavailable)?;
    if record.snapshot.integration_id != bound.integration_id
        || record.snapshot.epoch != bound.epoch
    {
        return Err(task_error(
            "TASK_REVISION_CONFLICT",
            "integration cycle changed before mutation",
        ));
    }
    let expected = expected_record(&bound.mutation);
    if current == *expected {
        return Ok(bound.mutation);
    }
    let same_work = current.meta() == expected.meta()
        && current
            .status()
            .turns()
            .iter()
            .map(TurnSummary::turn_id)
            .eq(expected.status().turns().iter().map(TurnSummary::turn_id));
    let stop_progress =
        record.tombstone.is_some() && current.status().head_oid() == expected.status().head_oid();
    let imported_progress = record.receipt.as_ref().is_some_and(|receipt| {
        let accepted = receipt.merge_oid.as_ref().unwrap_or(&receipt.target_head);
        receipt.imported
            && current.status().head_oid() == Some(accepted)
            && current.fetched_head() == Some(accepted)
    });
    if !same_work || !(stop_progress || imported_progress) {
        return Err(task_error(
            "TASK_REVISION_CONFLICT",
            "task changed before mutation",
        ));
    }
    // Retirement and import are this operation's allowed progress, with the
    // cycle and entire turn history still fenced. Say retains its frozen input.
    match &mut bound.mutation {
        PreparedTaskMutation::Cancel { expected, .. }
        | PreparedTaskMutation::Close { expected, .. } => *expected = current,
        PreparedTaskMutation::Say { .. } => {}
    }
    Ok(bound.mutation)
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
    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn discard(&self) -> Option<bool> {
        match self {
            Self::Close { discard, .. } => Some(*discard),
            Self::Say { .. } | Self::Cancel { .. } => None,
        }
    }
}

/// Typed body parse only. Validates the mutation body and returns the
/// targeted task id without loading or freezing expected state.
pub(super) fn mutation_task_id(request: &ControllerRequest) -> Result<TaskId, WorkerError> {
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
