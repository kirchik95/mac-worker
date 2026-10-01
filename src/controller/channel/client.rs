//! Foreground-owned policy for the frozen controller read-loop scopes.
use std::{ffi::OsStr, sync::Mutex};

pub use super::contracts::{
    CHANNEL_VERSION, ChannelFailure, ChannelReason, CleanupContext, ClientContext, ClientDeps,
    ConfiguredRoute, ForwardDisposition, ForwardLease, ReadLoopScope, SETUP_GUARD, SocketSession,
    eligible_read,
};
use crate::{
    controller::{ControllerRequest, decode_request},
    error::{ProcessError, WorkerError},
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
};

struct Session {
    socket: Box<dyn SocketSession>,
    forward: Box<dyn ForwardLease>,
}
struct State {
    session: Option<Session>,
    disposition: ForwardDisposition,
    retired: bool,
    last_read_id: Option<String>,
}
/// Construct only inside one of the explicitly scoped foreground read loops.
pub struct ChannelProcessRunner<R: ProcessRunner> {
    raw: R,
    scope: ReadLoopScope,
    route: ConfiguredRoute,
    paths: PathLayout,
    deps: ClientDeps,
    state: Mutex<State>,
}
impl<R: ProcessRunner> ChannelProcessRunner<R> {
    pub fn new(
        raw: R,
        scope: ReadLoopScope,
        route: ConfiguredRoute,
        paths: PathLayout,
        deps: ClientDeps,
    ) -> Self {
        Self {
            raw,
            scope,
            route,
            paths,
            deps,
            state: Mutex::new(State {
                session: None,
                disposition: ForwardDisposition::Cleaned,
                retired: false,
                last_read_id: None,
            }),
        }
    }

    /// Close the application stream before cancelling its single owned forward.
    pub fn close(&self) -> ForwardDisposition {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.close_session(&mut state);
        state.disposition
    }

    fn close_session(&self, state: &mut State) {
        if let Some(mut session) = state.session.take() {
            session.socket.close();
            state.disposition = self.cancel_forward(&mut *session.forward);
        }
    }

    fn cancel_forward(&self, forward: &mut dyn ForwardLease) -> ForwardDisposition {
        forward.cancel(
            &self.raw,
            &CleanupContext {
                runtime: self.deps.runtime.clone(),
                deadline: self.deps.runtime.now().saturating_add(SETUP_GUARD),
            },
        )
    }

    fn setup(&self, state: &mut State, ctx: &ClientContext<'_>) -> Result<(), ChannelFailure> {
        ctx.check()?;
        let master = self.deps.forwards.resolve(&self.raw, &self.route, ctx)?;
        ctx.check()?;
        let identity = self
            .deps
            .identity
            .read(&self.raw, &self.route, Some(&master), ctx)?;
        ctx.check()?;
        if identity.route_sha256 != self.route.digest()? {
            return Err(ChannelFailure::Unavailable(ChannelReason::PinMismatch));
        }
        if identity.service.protocol_version != PROTOCOL_VERSION
            || identity.service.channel_version != CHANNEL_VERSION
            || !identity
                .service
                .features
                .iter()
                .any(|feature| feature == "controller.socket")
        {
            return Err(ChannelFailure::Unavailable(ChannelReason::Unsupported));
        }
        self.deps.pins.verify_or_create(&self.paths, &identity)?;
        ctx.check()?;
        let mut forward = self
            .deps
            .forwards
            .open(&self.raw, &master, &identity, ctx)
            .map_err(|failure| {
                state.disposition = failure.disposition;
                failure.failure
            })?;
        if let Err(failure) = ctx.check() {
            state.disposition = self.cancel_forward(&mut *forward);
            return Err(failure);
        }
        let socket = self
            .deps
            .connector
            .connect(forward.local_socket(), &identity, ctx)
            .map_err(|failure| {
                state.disposition = self.cancel_forward(&mut *forward);
                failure
            })?;
        state.session = Some(Session { socket, forward });
        ctx.check()
    }

    fn eligible(&self, request: &ProcessRequest) -> Option<ControllerRequest> {
        if !matches_route(request, &self.route) {
            return None;
        }
        let frame = request.stdin.as_deref()?;
        let parsed = decode_request(frame).ok()?;
        eligible_read(self.scope, &parsed).then_some(parsed)
    }

    fn run_scoped(
        &self,
        request: &ProcessRequest,
        parsed: &ControllerRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        let ctx = ClientContext {
            runtime: &*self.deps.runtime,
            deadline: self
                .deps
                .runtime
                .now()
                .saturating_add(request.policy.deadline),
            should_stop,
        };
        ctx.check().map_err(unavailable)?;
        let Ok(mut state) = self.state.try_lock() else {
            return self.raw_read(request, &ctx);
        };
        // Existing retries of this same sequential read never re-enter the
        // channel, even if reconnect eligibility advances between attempts.
        if state.retired || state.last_read_id.as_deref() == Some(parsed.request_id()) {
            drop(state);
            return self.raw_read(request, &ctx);
        }
        state.last_read_id = Some(parsed.request_id().to_owned());
        if state.session.is_none() {
            if ctx.remaining() <= SETUP_GUARD {
                drop(state);
                return self.raw_read(request, &ctx);
            }
            let setup_ctx = ClientContext {
                runtime: ctx.runtime,
                should_stop: ctx.should_stop,
                deadline: ctx
                    .deadline
                    .min(ctx.runtime.now().saturating_add(SETUP_GUARD)),
            };
            if let Err(failure) = self.setup(&mut state, &setup_ctx) {
                self.close_session(&mut state);
                drop(state);
                if matches!(
                    failure,
                    ChannelFailure::Unavailable(ChannelReason::Cancelled)
                ) {
                    return Err(unavailable(failure));
                }
                return self.raw_read(request, &ctx);
            }
        }
        let session = state
            .session
            .as_mut()
            .expect("successful setup supplies a session");
        if let Err(_failure) = session.forward.verify() {
            self.close_session(&mut state);
            drop(state);
            return self.raw_read(request, &ctx);
        }
        ctx.check().map_err(unavailable)?;
        let result = session.socket.exchange(
            request.stdin.as_deref().expect("eligible frame"),
            parsed,
            &ctx,
        );
        match result {
            Ok(result) => Ok(result),
            Err(failure) => {
                self.close_session(&mut state);
                if matches!(failure, ChannelFailure::UnverifiedReply) {
                    state.retired = true;
                    return Err(unavailable(failure));
                }
                drop(state);
                if matches!(
                    failure,
                    ChannelFailure::Unavailable(ChannelReason::Cancelled)
                ) {
                    return Err(unavailable(failure));
                }
                self.raw_read(request, &ctx)
            }
        }
    }

    fn raw_read(
        &self,
        request: &ProcessRequest,
        ctx: &ClientContext<'_>,
    ) -> Result<ProcessResult, WorkerError> {
        ctx.check().map_err(unavailable)?;
        let mut remaining = request.clone();
        remaining.policy.deadline = ctx.remaining();
        self.raw
            .run_interruptible(&remaining, &|| ctx.check().is_err())
    }
}
impl<R: ProcessRunner> ProcessRunner for ChannelProcessRunner<R> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        match self.eligible(request) {
            Some(parsed) => self.run_scoped(request, &parsed, &|| false),
            None => self.raw.run(request),
        }
    }
    fn run_in_new_session(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.raw.run_in_new_session(request)
    }
    fn run_interruptible(
        &self,
        request: &ProcessRequest,
        should_stop: &dyn Fn() -> bool,
    ) -> Result<ProcessResult, WorkerError> {
        match self.eligible(request) {
            Some(parsed) => self.run_scoped(request, &parsed, should_stop),
            None => self.raw.run_interruptible(request, should_stop),
        }
    }
}
impl<R: ProcessRunner> Drop for ChannelProcessRunner<R> {
    fn drop(&mut self) {
        self.close();
    }
}

fn unavailable(failure: ChannelFailure) -> WorkerError {
    match failure {
        ChannelFailure::Unavailable(ChannelReason::Cancelled) => ProcessError::Cancelled.into(),
        ChannelFailure::Unavailable(ChannelReason::Timeout) => ProcessError::DeadlineExceeded {
            deadline: SETUP_GUARD,
        }
        .into(),
        _ => WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller read channel unavailable".into(),
        ),
    }
}

// Recognize only the worker's structured SSH invocation. In particular, no
// alternate executable, config, destination, forwarding or remote shell text.
fn matches_route(request: &ProcessRequest, route: &ConfiguredRoute) -> bool {
    if crate::transport::ssh_program().ok().as_ref() != Some(&request.program)
        || !request.environment.is_empty()
        || !request.environment_remove.is_empty()
        || request.isolate_parent_environment
        || route.remote_binary != "~/.local/bin/worker"
    {
        return false;
    }
    let Some(split) = request.args.len().checked_sub(3) else {
        return false;
    };
    if request.args[split] != "--"
        || request.args[split + 1] != OsStr::new(&route.ssh)
        || request.args[split + 2] != "~/.local/bin/worker host controller-rpc"
    {
        return false;
    }
    let mut options = &request.args[..split];
    if let Some(config) = &route.ssh_config_file {
        if options.len() < 2 || options[0] != "-F" || options[1] != config.as_os_str() {
            return false;
        }
        options = &options[2..];
    }
    let required = [
        "BatchMode=yes",
        "ConnectTimeout=5",
        "ForwardAgent=no",
        "ClearAllForwardings=yes",
    ];
    if options.len() < 8 || !options.len().is_multiple_of(2) {
        return false;
    }
    for (index, pair) in options.chunks_exact(2).enumerate() {
        if pair[0] != "-o" {
            return false;
        }
        let Some(value) = pair[1].to_str() else {
            return false;
        };
        if index < required.len() {
            if value != required[index] {
                return false;
            }
        } else if !matches!(
            value,
            "ControlMaster=auto"
                | "ControlPersist=60"
                | "ServerAliveInterval=10"
                | "ServerAliveCountMax=3"
                | "StreamLocalBindMask=0177"
                | "StreamLocalBindUnlink=no"
        ) && !value
            .strip_prefix("ControlPath=")
            .is_some_and(|path| !path.is_empty())
        {
            return false;
        }
    }
    true
}
#[cfg(test)]
mod tests;
