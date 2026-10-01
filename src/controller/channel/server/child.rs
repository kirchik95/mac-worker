use std::{
    ffi::OsString,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    controller::{
        MAX_FRAME_BYTES,
        channel::contracts::{
            ChannelExecutor, ChildRpcSpec, DETACHED_RUNNER_EXECUTABLE_ENV, REQUEST_GUARD,
            ServerContext,
        },
        decode_request,
    },
    error::{ProcessError, WorkerError},
    process::{
        CleanupState, ProcessCompletion, ProcessPolicy, ProcessRequest, TrackedProcessRunner,
    },
};

pub struct ChildRpcExecutor {
    runner: Arc<dyn TrackedProcessRunner>,
    launch: Option<ProcessRequest>,
}

impl ChildRpcExecutor {
    pub fn new(runner: Arc<dyn TrackedProcessRunner>, spec: ChildRpcSpec) -> Self {
        Self {
            runner,
            launch: launch_request(spec),
        }
    }
}

impl ChannelExecutor for ChildRpcExecutor {
    fn run(&self, frame: &[u8], ctx: &ServerContext) -> ProcessCompletion {
        let no_child = |error| ProcessCompletion {
            outcome: Err(error),
            cleanup: CleanupState::Completed,
        };
        let Some(mut request) = self.launch.clone() else {
            return no_child(WorkerError::Protocol(
                "CONTROLLER_CHANNEL: invalid captured child launch".into(),
            ));
        };
        if !decode_request(frame).is_ok_and(|request| {
            crate::controller::channel::contracts::server_eligible_read(&request)
        }) {
            return no_child(WorkerError::Protocol(
                "CONTROLLER_CHANNEL: invalid read request".into(),
            ));
        }
        let should_stop = || {
            ctx.cancelled.load(Ordering::Acquire)
                || ctx.runtime.cancelled()
                || ctx.runtime.now() >= ctx.deadline
        };
        if should_stop() {
            return no_child(ProcessError::Cancelled.into());
        }
        request.stdin = Some(frame.to_vec());
        request.policy.deadline = ctx
            .deadline
            .saturating_sub(ctx.runtime.now())
            .min(REQUEST_GUARD);
        self.runner
            .run_interruptible_with_cleanup(&request, &should_stop)
    }
}

fn safe_absolute(path: &Path) -> bool {
    path.is_absolute()
        && path
            .to_str()
            .is_some_and(|text| text.len() <= 4096 && !text.chars().any(char::is_control))
}

fn launch_request(spec: ChildRpcSpec) -> Option<ProcessRequest> {
    if !safe_absolute(&spec.executable.path)
        || !safe_absolute(&spec.detached_runner_executable)
        || !safe_absolute(&spec.config)
        || spec.executable.path == spec.detached_runner_executable
        || spec.executable.binding.kind != libc::S_IFREG as u32
        || spec.executable.binding.owner != unsafe { libc::geteuid() }
        || spec.executable.binding.device == 0
        || spec.executable.binding.inode == 0
        || spec.executable.binding.mode & 0o111 == 0
    {
        return None;
    }
    let mut environment = std::collections::BTreeMap::new();
    for (key, value) in spec.environment {
        if key == DETACHED_RUNNER_EXECUTABLE_ENV {
            continue;
        }
        let name = key.to_str()?;
        if !matches!(
            name,
            "HOME"
                | "XDG_CONFIG_HOME"
                | "XDG_STATE_HOME"
                | "XDG_DATA_HOME"
                | "XDG_CACHE_HOME"
                | "PATH"
        ) || value
            .to_str()
            .is_none_or(|text| text.len() > 8192 || text.chars().any(char::is_control))
            || (name != "PATH" && !safe_absolute(Path::new(&value)))
            || environment.insert(key, value).is_some()
        {
            return None;
        }
    }
    for required in [
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_STATE_HOME",
        "XDG_DATA_HOME",
        "XDG_CACHE_HOME",
    ] {
        if !environment.contains_key(&OsString::from(required)) {
            return None;
        }
    }
    environment.insert(
        DETACHED_RUNNER_EXECUTABLE_ENV.into(),
        spec.detached_runner_executable.into_os_string(),
    );
    Some(ProcessRequest {
        program: spec.executable.path.into_os_string(),
        args: vec![
            "--config".into(),
            spec.config.into_os_string(),
            "host".into(),
            "controller-rpc".into(),
        ],
        environment: environment.into_iter().collect(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: MAX_FRAME_BYTES + 4,
            stderr_limit: 256 * 1024,
            deadline: REQUEST_GUARD,
        },
        isolate_parent_environment: true,
    })
}
