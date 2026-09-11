//! Short wait/reconcile controller RPC.
//!
//! Local `TaskClient::wait` loops reconcile+sleep on one process. A blocking
//! wait on one SSH hop would exceed the 30s controller RPC deadline, and a
//! status-only poll would return before quiescence (`TASK_BUSY` on close).
//! Laptop loops `task.wait.poll`; each frame runs selected-ID reconcile
//! (no 00ce budget reset) then a DAG-aware quiescence snapshot. Explicit
//! `task.reconcile` is `operator_reconcile`. These commands are outside
//! STORE and `is_read_command`.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    config::ControllerConfig,
    controller::{
        execute::send_controller_read,
        protocol::{ControllerRequest, encode_json_frame},
        read::{
            ControllerReadIdentity, ControllerReadReply, invalid_controller_reply,
            map_missing_task, optional_string, reject_unknown_keys,
        },
    },
    error::WorkerError,
    process::ProcessRunner,
    protocol::PROTOCOL_VERSION,
    task::TaskId,
    task_client::{
        ReconcileReport, TaskClient, WAIT_MAX_POLL, WAIT_POLL, WaitReport, WaitSelector,
        WaitSnapshot,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerWaitSelector {
    Task(TaskId),
    Run(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerWaitPollResult {
    task_ids: Vec<TaskId>,
    quiescent: bool,
    exit_code: u8,
}

impl ControllerWaitPollResult {
    fn from_snapshot(snapshot: WaitSnapshot) -> Self {
        Self {
            task_ids: snapshot.task_ids().to_vec(),
            quiescent: snapshot.quiescent(),
            exit_code: snapshot.exit_code(),
        }
    }

    pub fn task_ids(&self) -> &[TaskId] {
        &self.task_ids
    }

    pub fn quiescent(&self) -> bool {
        self.quiescent
    }

    pub fn exit_code(&self) -> u8 {
        self.exit_code
    }
}

impl ControllerReadIdentity for ControllerWaitPollResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        if let Some(value) = request.body().get("task_id").and_then(Value::as_str) {
            let parsed: TaskId = value.parse().map_err(|_| invalid_controller_reply())?;
            if self.task_ids.as_slice() != [parsed] {
                return Err(invalid_controller_reply());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerReconcileResult {
    replaced_runners: usize,
    started_runners: usize,
    repaired_rows: usize,
    #[serde(default)]
    unverifiable_rows: usize,
}

impl ControllerReconcileResult {
    pub fn replaced_runners(&self) -> usize {
        self.replaced_runners
    }

    pub fn started_runners(&self) -> usize {
        self.started_runners
    }

    pub fn repaired_rows(&self) -> usize {
        self.repaired_rows
    }

    pub fn unverifiable_rows(&self) -> usize {
        self.unverifiable_rows
    }
}

impl ControllerReadIdentity for ControllerReconcileResult {
    fn verify_payload(&self, _request: &ControllerRequest) -> Result<(), WorkerError> {
        Ok(())
    }
}

pub(crate) fn is_lifecycle_command(command: &str) -> bool {
    matches!(command, "task.wait.poll" | "task.reconcile")
}

pub(crate) fn serve_lifecycle_command(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<Vec<u8>, WorkerError> {
    match request.command() {
        "task.wait.poll" => Ok(encode_json_frame(&wait_poll_reply(request, client)?)?),
        "task.reconcile" => Ok(encode_json_frame(&reconcile_reply(request, client)?)?),
        other => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: unsupported controller command {other}"
        ))),
    }
}

pub fn wait_via_controller(
    runner: &dyn ProcessRunner,
    controller: &ControllerConfig,
    selector: ControllerWaitSelector,
    timeout: Option<Duration>,
) -> Result<WaitReport, WorkerError> {
    let started = SystemTime::now();
    loop {
        let snapshot = poll_wait(runner, controller, &selector)?;
        if snapshot.quiescent() {
            return Ok(WaitReport::new(
                snapshot.task_ids().to_vec(),
                snapshot.exit_code(),
            ));
        }
        if timeout.is_some_and(|limit| started.elapsed().is_ok_and(|elapsed| elapsed >= limit)) {
            return Err(WorkerError::task(
                "WAIT_TIMEOUT",
                "task wait timed out without cancelling the task",
            ));
        }
        std::thread::sleep(WAIT_POLL.min(WAIT_MAX_POLL));
    }
}

pub fn reconcile_via_controller(
    runner: &dyn ProcessRunner,
    controller: &ControllerConfig,
) -> Result<ReconcileReport, WorkerError> {
    let request = lifecycle_request("task.reconcile", serde_json::json!({}))?;
    let reply = send_controller_read::<ControllerReconcileResult>(runner, controller, &request)?;
    let result = reply.into_result();
    Ok(ReconcileReport::from_counts(
        result.replaced_runners(),
        result.started_runners(),
        result.repaired_rows(),
        result.unverifiable_rows(),
    ))
}

fn wait_poll_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<ControllerWaitPollResult>, WorkerError> {
    reject_unknown_keys(request.body(), &["task_id", "run"], "task.wait.poll")?;
    let selector = match (
        optional_string(request.body(), "task_id")?,
        optional_string(request.body(), "run")?,
    ) {
        (Some(task_id), None) => {
            let task_id: TaskId = task_id.parse().map_err(|_| {
                WorkerError::Protocol(
                    "CONTROLLER_TRANSPORT: task.wait.poll task_id is invalid".into(),
                )
            })?;
            WaitSelector::Task(task_id)
        }
        (None, Some(run)) => WaitSelector::Run(client.resolve_run(&run)?),
        _ => {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "wait requires exactly one of --task-id or --run",
            ));
        }
    };
    let snapshot = map_missing_task(client.wait_poll(selector))?;
    Ok(ControllerReadReply::from_request(
        request,
        ControllerWaitPollResult::from_snapshot(snapshot),
    ))
}

fn reconcile_reply(
    request: &ControllerRequest,
    client: &TaskClient<'_>,
) -> Result<ControllerReadReply<ControllerReconcileResult>, WorkerError> {
    reject_unknown_keys(request.body(), &[], "task.reconcile")?;
    let report = client.operator_reconcile()?;
    Ok(ControllerReadReply::from_request(
        request,
        ControllerReconcileResult {
            replaced_runners: report.replaced_runners(),
            started_runners: report.started_runners(),
            repaired_rows: report.repaired_rows(),
            unverifiable_rows: report.unverifiable_rows(),
        },
    ))
}

fn poll_wait(
    runner: &dyn ProcessRunner,
    controller: &ControllerConfig,
    selector: &ControllerWaitSelector,
) -> Result<ControllerWaitPollResult, WorkerError> {
    let body = match selector {
        ControllerWaitSelector::Task(task_id) => {
            serde_json::json!({ "task_id": task_id.to_string() })
        }
        ControllerWaitSelector::Run(run) => serde_json::json!({ "run": run }),
    };
    let request = lifecycle_request("task.wait.poll", body)?;
    let reply = send_controller_read::<ControllerWaitPollResult>(runner, controller, &request)?;
    Ok(reply.into_result())
}

fn lifecycle_request(command: &str, body: Value) -> Result<ControllerRequest, WorkerError> {
    let request_id = format!("{:x}", uuid::Uuid::new_v4().simple());
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": command,
        "body": body,
    }))
    .map_err(|_| {
        WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: controller lifecycle request could not be encoded".into(),
        )
    })?;
    crate::controller::protocol::parse_request(&payload)
}
