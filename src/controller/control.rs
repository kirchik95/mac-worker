//! Direct controller admission control. Flag writes are idempotent setters,
//! not durable request replays: a later `--off` must remain effective.

use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    config::ControllerConfig, error::WorkerError, process::ProcessRunner,
    protocol::PROTOCOL_VERSION,
};

use super::{
    ControllerReadIdentity, ControllerReadReply, ControllerRequest, drain, encode_json_frame,
    parse_request, read::invalid_controller_reply, send_controller_read,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DrainBody {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_bool"
    )]
    drained: Option<bool>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DrainResult {
    drained: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IntegrationPauseResult {
    integration_pause: Option<crate::integration::contracts::IntegrationPauseEvidence>,
}
impl ControllerReadIdentity for IntegrationPauseResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        if !is_pause_read(request)
            || self.integration_pause.is_some_and(|pause| {
                !matches!(
                    pause.reason,
                    crate::integration::contracts::IntegrationPauseReason::ControllerDrained
                        | crate::integration::contracts::IntegrationPauseReason::ControllerDisabled
                )
            })
        {
            return Err(invalid_controller_reply());
        }
        Ok(())
    }
}

fn is_pause_read(request: &ControllerRequest) -> bool {
    request.command() == "controller.drain"
        && request.body() == &serde_json::json!({"integration_pause": true})
}

pub(crate) fn integration_pause_via_controller(
    runner: &dyn ProcessRunner,
    controller: &ControllerConfig,
) -> Result<Option<crate::integration::contracts::IntegrationPauseEvidence>, WorkerError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION, "request_id": uuid::Uuid::new_v4().simple().to_string(),
        "command": "controller.drain", "body": {"integration_pause": true},
    })).map_err(|_| invalid_request())?;
    let request = parse_request(&payload)?;
    Ok(
        send_controller_read::<IntegrationPauseResult>(runner, controller, &request)?
            .into_result()
            .integration_pause,
    )
}

impl ControllerReadIdentity for DrainResult {
    fn verify_payload(&self, request: &ControllerRequest) -> Result<(), WorkerError> {
        let body = parse_body(request).map_err(|_| invalid_controller_reply())?;
        if body
            .drained
            .is_some_and(|expected| expected != self.drained)
        {
            return Err(invalid_controller_reply());
        }
        Ok(())
    }
}

/// None observes the persisted flag; Some atomically sets it. The existing
/// read envelope binds both modes to the exact command, ID, and body digest.
pub fn drain_via_controller(
    runner: &dyn ProcessRunner,
    controller: &ControllerConfig,
    drained: Option<bool>,
) -> Result<bool, WorkerError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": uuid::Uuid::new_v4().simple().to_string(),
        "command": "controller.drain",
        "body": DrainBody { drained },
    }))
    .map_err(|_| invalid_request())?;
    let request = parse_request(&payload)?;
    Ok(
        send_controller_read::<DrainResult>(runner, controller, &request)?
            .into_result()
            .drained,
    )
}

pub(crate) fn serve_drain(
    request: &ControllerRequest,
    state_root: &Path,
    sink: Option<std::sync::Arc<dyn super::events::EventSink>>,
) -> Result<Vec<u8>, WorkerError> {
    if is_pause_read(request) {
        return encode_json_frame(&ControllerReadReply::from_request(
            request,
            IntegrationPauseResult {
                integration_pause: drain::integration_pause(state_root)?,
            },
        ));
    }
    let body = parse_body(request)?;
    let drained = match body.drained {
        Some(drained) => {
            drain::set_drained_with_event_sink(state_root, drained, sink)?;
            // Acknowledge this write's committed value. Another operator may
            // change the flag immediately after our exclusive lock releases.
            drained
        }
        None => drain::is_drained(state_root)?,
    };
    encode_json_frame(&ControllerReadReply::from_request(
        request,
        DrainResult { drained },
    ))
}

fn parse_body(request: &ControllerRequest) -> Result<DrainBody, WorkerError> {
    if request.command() != "controller.drain" {
        return Err(invalid_request());
    }
    serde_json::from_value(request.body().clone()).map_err(|_| invalid_request())
}

fn present_bool<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<bool>, D::Error> {
    // Omission selects a read; an explicitly present null is malformed.
    bool::deserialize(deserializer).map(Some)
}

fn invalid_request() -> WorkerError {
    WorkerError::Protocol("INVALID_REQUEST: invalid controller drain request".into())
}
