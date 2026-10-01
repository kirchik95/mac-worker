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
        if !decode_request(frame).is_ok_and(|request| super::eligible_request(&request)) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::channel::contracts::{
            ChannelRuntime, DETACHED_RUNNER_EXECUTABLE_ENV, EntryIdentity, PinnedExecutable,
        },
        controller::{MAX_FRAME_BYTES, encode_json_frame},
        error::ProcessError,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
    };
    use std::{
        ffi::OsString,
        os::unix::process::ExitStatusExt,
        process::ExitStatus,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    struct Clock;
    impl ChannelRuntime for Clock {
        fn now(&self) -> Duration {
            Duration::from_secs(7)
        }
        fn cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Default)]
    struct Runner {
        calls: Mutex<Vec<ProcessRequest>>,
        stop_during_entry: Option<Arc<AtomicBool>>,
    }
    impl ProcessRunner for Runner {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("child must use the tracked interruptible seam")
        }
    }
    impl TrackedProcessRunner for Runner {
        fn run_interruptible_with_cleanup(
            &self,
            request: &ProcessRequest,
            stop: &dyn Fn() -> bool,
        ) -> ProcessCompletion {
            self.calls.lock().unwrap().push(request.clone());
            assert!(!stop());
            let outcome = if let Some(flag) = &self.stop_during_entry {
                flag.store(true, Ordering::Release);
                assert!(
                    stop(),
                    "owned cancellation predicate stays live after entry"
                );
                Err(ProcessError::Cancelled.into())
            } else {
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(75 << 8),
                    stdout: vec![1, 2],
                    stderr: vec![3],
                })
            };
            ProcessCompletion {
                outcome,
                cleanup: CleanupState::Completed,
            }
        }
    }

    fn spec() -> ChildRpcSpec {
        ChildRpcSpec {
            executable: PinnedExecutable {
                path: "/private/controller/rpc/generation-worker".into(),
                binding: EntryIdentity {
                    device: 1,
                    inode: 9,
                    owner: unsafe { libc::geteuid() },
                    kind: libc::S_IFREG as u32,
                    mode: 0o755,
                },
            },
            detached_runner_executable: "/installed/bin/worker".into(),
            config: "/captured/config.toml".into(),
            environment: [
                ("HOME", "/captured/home"),
                ("XDG_CONFIG_HOME", "/captured/config"),
                ("XDG_STATE_HOME", "/captured/state"),
                ("XDG_DATA_HOME", "/captured/data"),
                ("XDG_CACHE_HOME", "/captured/cache"),
                ("PATH", "/usr/bin:/bin"),
                (DETACHED_RUNNER_EXECUTABLE_ENV, "/inherited/wrong-worker"),
            ]
            .map(|(key, value)| (key.into(), value.into()))
            .to_vec(),
        }
    }

    fn context(flag: Arc<AtomicBool>) -> ServerContext {
        ServerContext {
            runtime: Arc::new(Clock),
            deadline: Duration::from_secs(24),
            cancelled: flag,
        }
    }
    fn frame(body: serde_json::Value) -> Vec<u8> {
        encode_json_frame(&serde_json::json!({ "protocol_version": 7, "request_id": "0123456789abcdef0123456789abcdef", "command": "task.wait.poll", "body": body })).unwrap()
    }

    #[test]
    fn child_uses_fixed_rpc_argv_captured_roots_and_pinned_image_with_installed_runner_input() {
        let runner = Arc::new(Runner::default());
        let executor = ChildRpcExecutor::new(runner.clone(), spec());
        let input = frame(serde_json::json!({"run": "fixture"}));
        let completion = executor.run(&input, &context(Arc::new(AtomicBool::new(false))));
        let output = completion.outcome.unwrap();
        assert_eq!(output.status.code(), Some(75));
        assert_eq!(output.stderr, [3]);
        assert_eq!(completion.cleanup, CleanupState::Completed);
        let calls = runner.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let request = &calls[0];
        assert_eq!(
            request.program,
            OsString::from("/private/controller/rpc/generation-worker")
        );
        assert_eq!(
            request.args,
            [
                "--config",
                "/captured/config.toml",
                "host",
                "controller-rpc"
            ]
            .map(OsString::from)
        );
        assert_eq!(request.stdin.as_deref(), Some(input.as_slice()));
        assert_eq!(request.policy.stdout_limit, MAX_FRAME_BYTES + 4);
        assert_eq!(request.policy.stderr_limit, 256 * 1024);
        assert_eq!(request.policy.deadline, Duration::from_secs(17));
        assert!(request.isolate_parent_environment);
        assert!(request.environment_remove.is_empty());
        let env: std::collections::BTreeMap<_, _> = request.environment.iter().cloned().collect();
        assert_eq!(env[&OsString::from("HOME")], "/captured/home");
        assert_eq!(env[&OsString::from("XDG_STATE_HOME")], "/captured/state");
        assert_eq!(
            env[&OsString::from(DETACHED_RUNNER_EXECUTABLE_ENV)],
            "/installed/bin/worker"
        );
        assert_eq!(
            request
                .environment
                .iter()
                .filter(|(key, _)| key == DETACHED_RUNNER_EXECUTABLE_ENV)
                .count(),
            1
        );
    }

    #[test]
    fn invalid_launch_environment_or_request_never_reaches_the_runner() {
        for invalid in 0..4 {
            let runner = Arc::new(Runner::default());
            let mut launch = spec();
            let mut input = frame(serde_json::json!({"run": "fixture"}));
            match invalid {
                0 => launch
                    .environment
                    .push(("DYLD_INSERT_LIBRARIES".into(), "/untrusted.dylib".into())),
                1 => launch.config = "relative-config".into(),
                2 => launch.detached_runner_executable = launch.executable.path.clone(),
                _ => {
                    input = frame(serde_json::json!({"run": "fixture", "executable": "/untrusted"}))
                }
            }
            let outcome = ChildRpcExecutor::new(runner.clone(), launch)
                .run(&input, &context(Arc::new(AtomicBool::new(false))));
            assert!(outcome.outcome.is_err());
            assert_eq!(outcome.cleanup, CleanupState::Completed);
            assert!(runner.calls.lock().unwrap().is_empty());
        }
        // Valid input proves this is an admission test, not a blanket rejection.
        let runner = Arc::new(Runner::default());
        assert!(
            ChildRpcExecutor::new(runner.clone(), spec())
                .run(
                    &frame(serde_json::json!({"run": "fixture"})),
                    &context(Arc::new(AtomicBool::new(false)))
                )
                .outcome
                .is_ok()
        );
    }

    #[test]
    fn cancellation_is_live_inside_the_tracked_runner() {
        let flag = Arc::new(AtomicBool::new(false));
        let runner = Arc::new(Runner {
            stop_during_entry: Some(flag.clone()),
            ..Runner::default()
        });
        let result = ChildRpcExecutor::new(runner.clone(), spec()).run(
            &frame(serde_json::json!({"run": "fixture"})),
            &context(flag),
        );
        assert!(matches!(
            result.outcome,
            Err(WorkerError::Process(ProcessError::Cancelled))
        ));
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
        assert_eq!(result.cleanup, CleanupState::Completed);
    }
}
