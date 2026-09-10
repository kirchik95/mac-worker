//! Control-plane RPC for streamed Git handshake.
//!
//! Pack bytes never enter these frames. Token is minted here and is not an
//! input to the frozen `task.submit` digest.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    client_state::ClientStateStore,
    controller::{
        protocol::{ControllerRequest, encode_json_frame},
        read::{ControllerReadIdentity, ControllerReadReply, invalid_controller_reply},
        registry::ProjectRegistry,
        store::ControllerStore,
        transfer::{ControllerReceiveIdentity, ControllerTransfer, VerifiedResultMeta},
    },
    error::WorkerError,
    job::RequestFingerprint,
    paths::PathLayout,
    prepared_submit::FrozenSubmitBody,
    process::ProcessRunner,
    task::{BaseOid, TaskId, TurnId},
};

pub fn is_transfer_command(command: &str) -> bool {
    matches!(
        command,
        "controller.transfer.source.prepare"
            | "controller.transfer.source.finish"
            | "controller.transfer.result.prepare"
    )
}

pub fn serve_transfer_command(
    request: &ControllerRequest,
    paths: &PathLayout,
    runner: &dyn ProcessRunner,
) -> Result<Vec<u8>, WorkerError> {
    match request.command() {
        "controller.transfer.source.prepare" => Ok(encode_json_frame(&prepare_source_reply(
            request, paths, runner,
        )?)?),
        "controller.transfer.source.finish" => Ok(encode_json_frame(&finish_source_reply(
            request, paths, runner,
        )?)?),
        "controller.transfer.result.prepare" => Ok(encode_json_frame(&prepare_result_reply(
            request, paths, runner,
        )?)?),
        other => Err(WorkerError::Protocol(format!(
            "CONTROLLER_TRANSPORT: unsupported controller command {other}"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerSourcePrepareResult {
    token: String,
    request_id: String,
    fingerprint: String,
    project_id: String,
    worktree_id: String,
    expected_oid: String,
}

impl ControllerSourcePrepareResult {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub fn expected_oid(&self) -> &str {
        &self.expected_oid
    }
}

impl ControllerReadIdentity for ControllerSourcePrepareResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        require_string(request, "request_id", &self.request_id)?;
        require_string(request, "fingerprint", &self.fingerprint)?;
        require_string(request, "project_id", &self.project_id)?;
        require_string(request, "worktree_id", &self.worktree_id)?;
        require_string(request, "expected_oid", &self.expected_oid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerSourceFinishResult {
    token: String,
    request_id: String,
    oid: String,
    request_ref: String,
}

impl ControllerSourceFinishResult {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn oid(&self) -> &str {
        &self.oid
    }
    pub fn request_ref(&self) -> &str {
        &self.request_ref
    }
}

impl ControllerReadIdentity for ControllerSourceFinishResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        require_string(request, "token", &self.token)?;
        require_string(request, "request_id", &self.request_id)?;
        require_string(request, "expected_oid", &self.oid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerResultPrepareResult {
    token: String,
    request_id: String,
    fingerprint: String,
    project_id: String,
    worktree_id: String,
    task_id: TaskId,
    turn_id: TurnId,
    imported_oid: String,
    worker: String,
}

impl ControllerResultPrepareResult {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
    pub fn turn_id(&self) -> TurnId {
        self.turn_id
    }
    pub fn imported_oid(&self) -> &str {
        &self.imported_oid
    }
    pub fn worker(&self) -> &str {
        &self.worker
    }
}

impl ControllerReadIdentity for ControllerResultPrepareResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        let Some(value) = request.body().get("task_id").and_then(Value::as_str) else {
            return Err(invalid_controller_reply());
        };
        let parsed: TaskId = value.parse().map_err(|_| invalid_controller_reply())?;
        if parsed != self.task_id {
            return Err(invalid_controller_reply());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePrepareBody {
    request_id: String,
    fingerprint: String,
    project_id: String,
    worktree_id: String,
    expected_oid: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceFinishBody {
    token: String,
    request_id: String,
    fingerprint: String,
    project_id: String,
    worktree_id: String,
    expected_oid: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultPrepareBody {
    task_id: TaskId,
}

fn prepare_source_reply(
    request: &ControllerRequest,
    paths: &PathLayout,
    runner: &dyn ProcessRunner,
) -> Result<ControllerReadReply<ControllerSourcePrepareResult>, WorkerError> {
    let body: SourcePrepareBody = serde_json::from_value(request.body().clone()).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: source prepare body is invalid".into())
    })?;
    let fingerprint = RequestFingerprint::new(body.fingerprint.clone()).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: source prepare fingerprint is invalid".into())
    })?;
    let oid: BaseOid = body.expected_oid.parse().map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: source prepare OID is invalid".into())
    })?;
    let transfer = ControllerTransfer::open(&paths.controller_state_root())?;
    let identity = transfer.prepare_source_receive(
        &paths.cache,
        runner,
        &body.request_id,
        &fingerprint,
        &body.project_id,
        &body.worktree_id,
        &oid,
    )?;
    Ok(ControllerReadReply::from_request(
        request,
        ControllerSourcePrepareResult {
            token: identity.token().to_owned(),
            request_id: identity.request_id().to_owned(),
            fingerprint: identity.fingerprint().as_str().to_owned(),
            project_id: identity.project_id().to_owned(),
            worktree_id: identity.worktree_id().to_owned(),
            expected_oid: identity.expected_oid().as_str().to_owned(),
        },
    ))
}

fn finish_source_reply(
    request: &ControllerRequest,
    paths: &PathLayout,
    runner: &dyn ProcessRunner,
) -> Result<ControllerReadReply<ControllerSourceFinishResult>, WorkerError> {
    let body: SourceFinishBody = serde_json::from_value(request.body().clone()).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: source finish body is invalid".into())
    })?;
    let fingerprint = RequestFingerprint::new(body.fingerprint.clone()).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: source finish fingerprint is invalid".into())
    })?;
    let oid: BaseOid = body.expected_oid.parse().map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: source finish OID is invalid".into())
    })?;
    let identity = ControllerReceiveIdentity::from_parts(
        body.token,
        body.request_id,
        fingerprint,
        body.project_id,
        body.worktree_id,
        oid,
    );
    let transfer = ControllerTransfer::open(&paths.controller_state_root())?;
    let receipt = transfer.finish_source_receive(&paths.cache, runner, &identity)?;
    Ok(ControllerReadReply::from_request(
        request,
        ControllerSourceFinishResult {
            token: receipt.token().to_owned(),
            request_id: receipt.request_id().to_owned(),
            oid: receipt.oid().as_str().to_owned(),
            request_ref: receipt.request_ref().to_owned(),
        },
    ))
}

fn prepare_result_reply(
    request: &ControllerRequest,
    paths: &PathLayout,
    runner: &dyn ProcessRunner,
) -> Result<ControllerReadReply<ControllerResultPrepareResult>, WorkerError> {
    let body: ResultPrepareBody = serde_json::from_value(request.body().clone()).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: result prepare body is invalid".into())
    })?;
    let registry = ProjectRegistry::open(&paths.controller_state_root())?;
    let bind = registry.lookup_task_request(body.task_id)?.ok_or_else(|| {
        WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: no streamed request is bound to this task".into(),
        )
    })?;
    let store = ControllerStore::open(&paths.controller_state_root())?;
    let durable = store.load(&bind.request_id)?.ok_or_else(|| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: bound controller request is missing".into())
    })?;
    if durable.payload_sha256() != bind.fingerprint {
        return Err(WorkerError::Protocol(
            "CONTROLLER_REQUEST_CONFLICT: bound fingerprint does not match the durable request"
                .into(),
        ));
    }
    let frozen: FrozenSubmitBody =
        serde_json::from_value(durable.body().clone()).map_err(|_| {
            WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: bound request is not a frozen submit".into(),
            )
        })?;
    if frozen.task_id != body.task_id {
        return Err(WorkerError::Protocol(
            "CONTROLLER_REQUEST_CONFLICT: bound task does not match the frozen envelope".into(),
        ));
    }
    let client_state = ClientStateStore::open(&paths.state)?;
    let record = client_state.load_task(body.task_id)?;
    let imported = record
        .fetched_head()
        .or_else(|| record.status().head_oid())
        .cloned()
        .ok_or_else(|| WorkerError::Git {
            code: "RESULT_FETCH_FAILED",
            message: "controller result is not imported yet".into(),
        })?;
    let worker = record
        .status()
        .worker()
        .or_else(|| record.pinned_worker())
        .ok_or_else(|| WorkerError::Git {
            code: "RESULT_FETCH_FAILED",
            message: "controller result has no worker identity".into(),
        })?
        .to_owned();
    let turn_id = record
        .status()
        .turns()
        .last()
        .map(|turn| turn.turn_id())
        .unwrap_or(frozen.turn_id);
    let fingerprint = RequestFingerprint::new(bind.fingerprint.clone()).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: bound fingerprint is invalid".into())
    })?;
    let meta = VerifiedResultMeta {
        task_id: body.task_id,
        turn_id,
        imported_oid: imported.clone(),
        worker: worker.clone(),
    };
    let transfer = ControllerTransfer::open(&paths.controller_state_root())?;
    let identity = transfer.prepare_result_upload(
        &paths.cache,
        runner,
        &bind.request_id,
        &fingerprint,
        &frozen.project_id,
        &frozen.worktree_id,
        &meta,
    )?;
    Ok(ControllerReadReply::from_request(
        request,
        ControllerResultPrepareResult {
            token: identity.token().to_owned(),
            request_id: identity.request_id().to_owned(),
            fingerprint: identity.fingerprint().as_str().to_owned(),
            project_id: identity.project_id().to_owned(),
            worktree_id: identity.worktree_id().to_owned(),
            task_id: identity.task_id(),
            turn_id: identity.turn_id(),
            imported_oid: identity.imported_oid().as_str().to_owned(),
            worker,
        },
    ))
}

fn require_string(
    request: &ControllerRequest,
    key: &str,
    expected: &str,
) -> Result<(), WorkerError> {
    let Some(value) = request.body().get(key).and_then(Value::as_str) else {
        return Err(invalid_controller_reply());
    };
    if value != expected {
        return Err(invalid_controller_reply());
    }
    Ok(())
}
