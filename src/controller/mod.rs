//! Remote persistent controller.
//!
//! Checkpoint 1: framed RPC codec, optional `[controller]` config, dedicated
//! leader lock, durable request envelope, and hidden `host controller-rpc`.
//! Full TaskClient/transfer/routing follows. Fake executor only.
//!
//! Stdio RPC is one-shot: the SSH client writes one frame, closes stdin, then
//! reads the response. Request identity is protocol version + command +
//! canonical body, never a client-supplied digest.
//!
//! Dashboard transport (documented, not implemented here): managed SSH
//! local-forward to the controller loopback HTTP service. Not RPC DTOs.

pub mod envelope;
pub mod leader;
pub mod protocol;
pub mod store;

use std::time::Duration;

pub use envelope::{OperationEnvelope, load_operation_envelope, persist_operation_envelope};
pub use leader::ControllerLeader;
pub use protocol::{
    ControllerRequest, MAX_FRAME_BYTES, MAX_STORED_REQUEST_BYTES, canonical_request_sha256,
    decode_frame, decode_request, encode_frame, encode_json_frame, parse_request, read_frame,
    write_frame,
};
pub use store::{
    ControllerAck, ControllerFault, ControllerStore, DurableRequest, FakeControllerExecutor,
    RequestPhase, serve_rpc,
};

use crate::{
    config::{ControllerConfig, WorkerEntry, valid_ssh_destination},
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest},
    transfer::HostOperation,
    transport::ssh_request,
};

const CONTROLLER_RPC_POLICY: ProcessPolicy = ProcessPolicy {
    stdout_limit: MAX_FRAME_BYTES + 4,
    stderr_limit: 256 * 1024,
    deadline: Duration::from_secs(30),
};

/// Construct the SSH request for `~/.local/bin/worker host controller-rpc`.
/// No remote path interpolation.
pub fn controller_rpc_ssh_request(
    controller: &ControllerConfig,
) -> Result<ProcessRequest, WorkerError> {
    if !controller.enabled {
        return Err(WorkerError::Protocol(
            "CONTROLLER_UNAVAILABLE: controller is not enabled".into(),
        ));
    }
    if !valid_ssh_destination(&controller.ssh) || controller.remote_binary != "~/.local/bin/worker"
    {
        return Err(WorkerError::Protocol(
            "CONTROLLER_UNAVAILABLE: controller transport configuration is invalid".into(),
        ));
    }
    let worker = WorkerEntry {
        name: "controller".into(),
        ssh: controller.ssh.clone(),
        slots: 1,
        capabilities: Vec::new(),
        remote_binary: controller.remote_binary.clone(),
        herdr: false,
    };
    Ok(ssh_request(
        &worker,
        HostOperation::ControllerRpc.command().into(),
        CONTROLLER_RPC_POLICY,
    ))
}
