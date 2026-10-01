//! Authenticated existing-only identity. Blocking native work; no store/journal initialization.
use super::contracts::{
    ChannelCodec, ChannelFailure, ChannelReason, ClientContext, ConfiguredRoute, MasterPlan,
    RouteDigest,
};
use super::contracts::{
    ControllerAccount, IdentitySource, ServiceIdentity, SocketIdentity, SocketIdentityResult,
};
use super::files::{read_record, validate_live_bindings};
use crate::{
    config::ControllerConfig,
    controller::{
        ControllerReadReply, ControllerRequest, encode_json_frame, health_read::observe_leader,
        init::host_identity, parse_request,
    },
    error::{ProcessError, WorkerError},
    paths::PathLayout,
    process::{ProcessRequest, ProcessRunner},
    rooted_fs::RootedDir,
    supervisor::ProcessObservation,
};
use std::{io, path::Path};
pub(crate) mod core;
mod proof;

#[derive(Default)]
pub struct StdioIdentitySource;
impl StdioIdentitySource {
    pub fn new() -> Self {
        Self
    }
}
impl IdentitySource for StdioIdentitySource {
    fn read(
        &self,
        raw: &dyn ProcessRunner,
        route: &ConfiguredRoute,
        master: Option<&MasterPlan>,
        ctx: &ClientContext<'_>,
    ) -> Result<SocketIdentity, ChannelFailure> {
        ctx.check()?;
        let digest = route.digest()?;
        let query=parse_request(&serde_json::to_vec(&serde_json::json!({"protocol_version":crate::protocol::PROTOCOL_VERSION,"request_id":crate::job::ClientId::generate().to_string(),"command":"task.list","body":{"controller_socket":{"op":"identity","route_sha256":digest}}})).map_err(|_|ChannelFailure::UnverifiedReply)?).map_err(|_|ChannelFailure::UnverifiedReply)?;
        let transport = match master {
            Some(master) => master.bootstrap_request.clone(),
            None => raw_request(route)
                .map_err(|_| ChannelFailure::Unavailable(ChannelReason::UnsafePath))?,
        };
        let stop = || ctx.check().is_err();
        let result = core::raw_exchange(raw, &transport, &query, ctx.remaining(), &stop);
        // Preserve live cancellation/deadline reasons even if the runner returned
        // an ordinary transport error while the borrowed predicate changed.
        ctx.check()?;
        let value = result.map_err(raw_failure)?;
        match serde_json::from_value::<SocketIdentityResult>(value)
            .map_err(|_| ChannelFailure::UnverifiedReply)?
        {
            SocketIdentityResult::Available(identity) => {
                identity.validate()?;
                if identity.route_sha256 != digest {
                    return Err(ChannelFailure::UnverifiedReply);
                }
                Ok(identity)
            }
            SocketIdentityResult::Unavailable(reason) => Err(ChannelFailure::Unavailable(reason)),
        }
    }
}
fn raw_request(route: &ConfiguredRoute) -> Result<ProcessRequest, WorkerError> {
    let mut request = crate::controller::controller_rpc_ssh_request(&ControllerConfig {
        enabled: true,
        ssh: route.ssh.clone(),
        remote_binary: route.remote_binary.clone(),
    })?;
    // The configured route is the authority, including its optional -F pathname.
    // Retain ordinary RPC options/trust and do not replace it with mux controls.
    if request.args.first().is_some_and(|arg| arg == "-F") {
        request.args.drain(..2);
    }
    if let Some(path) = &route.ssh_config_file {
        request
            .args
            .splice(..0, ["-F".into(), path.as_os_str().to_owned()]);
    }
    Ok(request)
}
fn raw_failure(error: WorkerError) -> ChannelFailure {
    match error {
        WorkerError::Process(ProcessError::Cancelled) => {
            ChannelFailure::Unavailable(ChannelReason::Cancelled)
        }
        WorkerError::Process(ProcessError::DeadlineExceeded { .. }) => {
            ChannelFailure::Unavailable(ChannelReason::Timeout)
        }
        WorkerError::Unavailable(_) | WorkerError::Io(_) => {
            ChannelFailure::Unavailable(ChannelReason::ServiceUnavailable)
        }
        _ => ChannelFailure::UnverifiedReply,
    }
}
fn invalid() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_UNAVAILABLE: invalid socket identity evidence".into())
}
fn check(ctx: &ClientContext<'_>) -> Result<(), WorkerError> {
    ctx.check().map_err(|failure| match failure {
        ChannelFailure::Unavailable(ChannelReason::Cancelled) => ProcessError::Cancelled.into(),
        ChannelFailure::Unavailable(ChannelReason::Timeout) => ProcessError::DeadlineExceeded {
            deadline: ctx.remaining(),
        }
        .into(),
        _ => invalid(),
    })
}

/// Recognizes the namespace even when its grammar is invalid, so malformed or
/// mixed identity selectors are rejected before task-list/store dispatch.
pub fn is_socket_selector(request: &ControllerRequest) -> bool {
    request.command() == "task.list" && request.body().get("controller_socket").is_some()
}

pub fn read_live_service(
    paths: &PathLayout,
    home: &Path,
    codec: &dyn ChannelCodec,
    ctx: &ClientContext<'_>,
) -> Result<Option<ServiceIdentity>, WorkerError> {
    check(ctx)?;
    let Some(client_id) = core::existing_client_id(paths)? else {
        return Ok(None);
    };
    let parent = match RootedDir::open_anchored_absolute(&paths.controller_state_root()) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    parent.channel_private_root()?;
    let root = match RootedDir::open_anchored_absolute(&parent.path().join("rpc")) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let (record, bytes, binding) = match read_record(&root) {
        Ok(record) => record,
        Err(WorkerError::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let host = host_identity(home)?;
    let account = ControllerAccount {
        uid: host.uid,
        username: host.username,
        home: host.home,
    };
    account.validate().map_err(|_| invalid())?;
    if record.service.controller_client_id != client_id
        || record.service.account != account
        || !matches!(
            observe_leader(parent.path(), record.service.leader)?,
            ProcessObservation::Matching { .. }
        )
    {
        return Ok(None);
    }
    validate_live_bindings(&root, &record)?;
    check(ctx)?;
    // Local hello proves this record's generation is actually being served.
    // It never adopts any service identity learned from the socket.
    let expected = SocketIdentity {
        route_sha256: RouteDigest::parse(&"0".repeat(64)).map_err(|_| invalid())?,
        service: record.service.clone(),
    };
    if proof::hello(&record.service.socket_path, &expected, codec, ctx).is_err() {
        check(ctx)?;
        return Ok(None);
    }
    check(ctx)?;
    parent.verify_bound()?;
    validate_live_bindings(&root, &record)?;
    if root.private_entry_identity("service.json")? != binding
        || root.read_private_regular("service.json", super::contracts::IDENTITY_BYTES as u64)?
            != bytes
        || root.private_entry_identity("service.json")? != binding
        || core::existing_client_id(paths)? != Some(client_id)
        || !matches!(
            observe_leader(parent.path(), record.service.leader)?,
            ProcessObservation::Matching { .. }
        )
    {
        return Ok(None);
    }
    Ok(Some(record.service))
}

pub fn serve_identity_selector(
    request: &ControllerRequest,
    paths: &PathLayout,
    home: &Path,
    codec: &dyn ChannelCodec,
    ctx: &ClientContext<'_>,
) -> Result<Vec<u8>, WorkerError> {
    let route = core::selector_route(request)?.ok_or_else(invalid)?;
    check(ctx)?;
    let result = match read_live_service(paths, home, codec, ctx) {
        Ok(Some(service)) => SocketIdentityResult::Available(SocketIdentity {
            route_sha256: RouteDigest::parse(&route).map_err(|_| invalid())?,
            service,
        }),
        _ => {
            check(ctx)?;
            SocketIdentityResult::Unavailable(ChannelReason::ServiceUnavailable)
        }
    };
    check(ctx)?;
    encode_json_frame(&ControllerReadReply::from_request(request, result))
}
