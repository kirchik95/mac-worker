//! Existing-only identity reads and an interruptible authenticated stdio exchange.
use crate::{
    controller::{ControllerReadReply, ControllerRequest, decode_frame, encode_json_frame},
    error::{ProcessError, WorkerError},
    job::ClientId,
    paths::PathLayout,
    process::{ProcessRequest, ProcessRunner},
    rooted_fs::RootedDir,
};
use serde_json::Value;
use std::{io, time::Duration};

fn invalid_selector() -> WorkerError {
    WorkerError::Protocol("CONTROLLER_TRANSPORT: invalid socket identity selector".into())
}

pub(crate) fn selector_route(request: &ControllerRequest) -> Result<Option<String>, WorkerError> {
    if request.command() != "task.list" || request.body().get("controller_socket").is_none() {
        return Ok(None);
    }
    if request
        .body()
        .as_object()
        .is_none_or(|body| body.len() != 1)
    {
        return Err(invalid_selector());
    }
    let selector = request.body()["controller_socket"]
        .as_object()
        .ok_or_else(invalid_selector)?;
    let route = selector
        .get("route_sha256")
        .and_then(Value::as_str)
        .ok_or_else(invalid_selector)?;
    if selector.len() != 2
        || selector.get("op").and_then(Value::as_str) != Some("identity")
        || route.len() != 64
        || !route
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid_selector());
    }
    Ok(Some(route.to_owned()))
}

fn existing_private_root(path: &std::path::Path) -> io::Result<Option<RootedDir>> {
    let root = match RootedDir::open_anchored_absolute(path) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let metadata = root.root_metadata()?;
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o7777 != 0o700 {
        return Err(io::Error::from_raw_os_error(libc::EACCES));
    }
    Ok(Some(root))
}

pub(crate) fn existing_client_id(paths: &PathLayout) -> io::Result<Option<ClientId>> {
    let Some(root) = existing_private_root(&paths.state)? else {
        return Ok(None);
    };
    let bytes = match root.read_private_regular("client-id", 33) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if bytes.len() != 33 || bytes[32] != b'\n' {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    let id = std::str::from_utf8(&bytes[..32])
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?
        .parse()
        .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    Ok(Some(id))
}

pub(crate) fn decode_unique(bytes: &[u8]) -> Result<Value, WorkerError> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value =
        crate::task::deserialize_unique_json(&mut decoder).map_err(|_| invalid_selector())?;
    decoder.end().map_err(|_| invalid_selector())?;
    Ok(value)
}

pub(crate) fn raw_exchange(
    raw: &dyn ProcessRunner,
    transport: &ProcessRequest,
    query: &ControllerRequest,
    remaining: Duration,
    should_stop: &dyn Fn() -> bool,
) -> Result<Value, WorkerError> {
    if should_stop() {
        return Err(ProcessError::Cancelled.into());
    }
    if remaining.is_zero() {
        return Err(ProcessError::DeadlineExceeded {
            deadline: remaining,
        }
        .into());
    }
    if selector_route(query)?.is_none() {
        return Err(invalid_selector());
    }
    let mut request = transport.clone();
    request.policy.deadline = request
        .policy
        .deadline
        .min(remaining)
        .min(Duration::from_secs(5));
    request.policy.stdout_limit = request.policy.stdout_limit.min(8196);
    request.stdin = Some(encode_json_frame(
        &serde_json::json!({"protocol_version":query.protocol_version(),"request_id":query.request_id(),"command":query.command(),"payload_sha256":query.payload_sha256(),"body":query.body()}),
    )?);
    let result = raw.run_interruptible(&request, should_stop)?;
    if should_stop() {
        return Err(ProcessError::Cancelled.into());
    }
    if !result.status.success() || result.stdout.len() > 8196 {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: socket identity is unavailable".into(),
        ));
    }
    let payload = decode_frame(&result.stdout)?;
    let reply: ControllerReadReply<Value> =
        serde_json::from_value(decode_unique(payload)?).map_err(|_| invalid_selector())?;
    reply.verify_envelope(query)?;
    Ok(reply.into_result())
}

