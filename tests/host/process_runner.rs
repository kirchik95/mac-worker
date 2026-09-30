use mac_worker::{
    agent::prebind_login_request,
    agent_facts::AgentAuth,
    error::{ProcessError, ProcessStream, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner, SystemProcessRunner},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::support;

use support::agent_launch_fixture::{
    DiagnosticProcessRunner, FixtureLayout, PARENT_ONLY, assert_subprocess_success,
    classify_cursor_process_result, fixture_home_from_env, fixture_only_path, skip_unless_subtest,
};

fn policy(stdout_limit: usize, stderr_limit: usize, deadline: Duration) -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit,
        stderr_limit,
        deadline,
    }
}

#[test]
fn stdout_overflow_terminates_the_child_with_a_typed_error() {
    // This catches returning an oversized stdout buffer after the child has
    // already consumed unbounded memory.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "while :; do printf 0123456789; done".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(32, 32, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };
    let started = Instant::now();

    let error = SystemProcessRunner.run(&request).unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Process(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stdout,
            limit: 32
        })
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn stderr_overflow_terminates_the_child_with_a_typed_error() {
    // This catches bounding stdout while still allowing diagnostics to grow
    // without limit.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "while :; do printf 0123456789 >&2; done".into(),
        ],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(32, 32, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let error = SystemProcessRunner.run(&request).unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Process(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stderr,
            limit: 32
        })
    ));
}

#[test]
fn overflow_drains_the_pipe_without_waiting_out_the_deadline() {
    // A finite writer larger than the pipe buffer blocks until the capture
    // side drains. stderr is given a high cap so diagnostic lines cannot
    // steal the overflow error.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "dd if=/dev/zero bs=65536 count=32 2>/dev/null".into(),
        ],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(32, 1024 * 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };
    let started = Instant::now();

    let error = SystemProcessRunner.run(&request).unwrap_err();

    assert!(
        matches!(
            error,
            WorkerError::Process(ProcessError::OutputLimitExceeded {
                stream: ProcessStream::Stdout,
                limit: 32
            })
        ),
        "{error:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn run_interruptible_cancels_an_in_flight_child() {
    let cancel = Arc::new(AtomicBool::new(false));
    let request = ProcessRequest {
        program: "/bin/sleep".into(),
        args: vec!["5".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(32, 32, Duration::from_secs(5)),
        isolate_parent_environment: false,
    };
    let started = Instant::now();
    let flag = Arc::clone(&cancel);
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        flag.store(true, Ordering::SeqCst);
    });

    let error = SystemProcessRunner
        .run_interruptible(&request, &|| cancel.load(Ordering::SeqCst))
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Process(ProcessError::Cancelled)
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn deadline_terminates_and_reaps_a_non_exiting_child() {
    // This catches a probe child that can keep the CLI blocked forever.
    let request = ProcessRequest {
        program: "/bin/sleep".into(),
        args: vec!["5".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(32, 32, Duration::from_millis(100)),
        isolate_parent_environment: false,
    };
    let started = Instant::now();

    let error = SystemProcessRunner.run(&request).unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Process(ProcessError::DeadlineExceeded { .. })
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn deadline_terminates_descendants_that_hold_inherited_pipes_open() {
    // This catches regressing teardown to kill only the direct child: its
    // backgrounded descendant retains stdout and stderr, so the capture
    // threads cannot reach EOF until the descendant's five-second sleep ends.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "sleep 5 & exit 0".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(32, 32, Duration::from_millis(100)),
        isolate_parent_environment: false,
    };
    let started = Instant::now();

    let error = SystemProcessRunner.run(&request).unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Process(ProcessError::DeadlineExceeded { .. })
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn normal_process_preserves_literal_argv() {
    // This catches reintroducing a shell parsing step while adding the policy
    // enforcement machinery.
    let literal = "$(printf injected); $HOME *";
    let request = ProcessRequest {
        program: "/usr/bin/printf".into(),
        args: vec!["%s".into(), literal.into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, literal.as_bytes());
    assert!(result.stderr.is_empty());
}

#[test]
fn requested_environment_reaches_the_child_as_literal_os_strings() {
    // This catches dropping the controlled host PATH override or introducing
    // shell parsing while passing environment overrides to host commands.
    let literal = "$(printf injected); $HOME *";
    let request = ProcessRequest {
        program: "/usr/bin/printenv".into(),
        args: vec!["WORKER_LITERAL".into()],
        environment: vec![("WORKER_LITERAL".into(), literal.into())],
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, format!("{literal}\n").as_bytes());
    assert!(result.stderr.is_empty());
}

#[test]
fn requested_environment_removals_do_not_clear_unrelated_xdg_variables() {
    // This catches applying a full environment clear to remove a Git override,
    // which would silently discard the caller's XDG configuration semantics.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "test -z \"${GIT_DIR+x}\" && printf %s \"$XDG_CONFIG_HOME\"".into(),
        ],
        environment: vec![
            ("GIT_DIR".into(), "/tmp/redirected-git-dir".into()),
            ("XDG_CONFIG_HOME".into(), "/tmp/preserved-xdg".into()),
        ],
        environment_remove: vec!["GIT_DIR".into()],
        stdin: None,
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, b"/tmp/preserved-xdg");
}

#[test]
fn normal_process_receives_the_complete_stdin_payload() {
    // This catches the concurrent capture implementation dropping stdin or
    // closing it before the requested bytes have been written.
    let request = ProcessRequest {
        program: "/bin/cat".into(),
        args: Vec::new(),
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: Some(b"raw stdin bytes\n".to_vec()),
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, b"raw stdin bytes\n");
    assert!(result.stderr.is_empty());
}

#[test]
fn new_session_process_runs_without_a_tty() {
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "test ! -t 0".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: None,
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run_in_new_session(&request).unwrap();

    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn nonzero_exit_remains_authoritative_after_stdin_broken_pipe() {
    // Regression: a fast SSH rejection closed stdin while the parent was
    // uploading, and the writer's BrokenPipe hid the remote exit and stderr.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "exec 0<&-; printf '%s\n' rejected >&2; exit 23".into(),
        ],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: Some(vec![b'x'; 16 * 1024 * 1024]),
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert_eq!(result.status.code(), Some(23));
    assert!(result.stdout.is_empty());
    assert_eq!(result.stderr, b"rejected\n");
}

#[test]
fn isolate_parent_environment_rejects_parent_only_credentials_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(PARENT_ONLY);
    assert_subprocess_success(
        &crate::support::libtest_name(
            module_path!(),
            "isolate_parent_environment_rejects_parent_only_credentials",
        ),
        &[
            ("FIXTURE_HOME", fixture.home.to_str().unwrap()),
            ("CURSOR_API_KEY", PARENT_ONLY),
            ("PATH", support::agent_launch_fixture::empty_base_path()),
        ],
        true,
    );
}

#[test]
#[ignore = "subprocess body: run by its *_wrapper test with the fixture environment"]
fn isolate_parent_environment_rejects_parent_only_credentials() {
    if skip_unless_subtest() {
        return;
    }
    let home = fixture_home_from_env();
    let request =
        prebind_login_request(&["cursor-agent".into(), "status".into()], &home, &[]).unwrap();
    assert!(request.isolate_parent_environment);
    let result = DiagnosticProcessRunner.run(&request).unwrap();
    assert_eq!(
        classify_cursor_process_result(&result),
        AgentAuth::Unauthenticated
    );
}

#[test]
fn successful_exit_does_not_hide_stdin_broken_pipe() {
    // Regression guard: deferring a stdin error until the child exits must
    // not turn an incomplete upload into a successful request.
    let request = ProcessRequest {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "exec 0<&-; exit 0".into()],
        environment: Vec::new(),
        environment_remove: Vec::new(),
        stdin: Some(vec![b'x'; 16 * 1024 * 1024]),
        policy: policy(1024, 1024, Duration::from_secs(2)),
        isolate_parent_environment: false,
    };

    let error = SystemProcessRunner.run(&request).unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Io(ref error) if error.kind() == std::io::ErrorKind::BrokenPipe
    ));
}

#[cfg(target_os = "macos")]
mod termination {
    use super::*;
    use mac_worker::{
        job::ProcessIdentity,
        supervisor::{ProcessInspector, ProcessObservation, SystemProcessInspector},
    };
    use std::{
        fs,
        io::{self, Read, Write},
        os::unix::{
            net::{UnixListener, UnixStream},
            process::CommandExt,
        },
        path::Path,
        process::{Command, Stdio},
    };

    const FIXTURE_TEST: &str = "pipe_holder_fixture";

    struct FixtureProcess(ProcessIdentity);

    impl Drop for FixtureProcess {
        fn drop(&mut self) {
            if self.0.pid() > 1
                && matches!(
                    SystemProcessInspector.observe(self.0),
                    ProcessObservation::Matching { .. }
                )
            {
                unsafe { libc::kill(self.0.pid() as libc::pid_t, libc::SIGKILL) };
            }
        }
    }

    struct Fixture(tempfile::TempDir);

    impl Fixture {
        fn new() -> Self {
            Self(tempfile::tempdir().unwrap())
        }

        fn request(&self, mode: &str) -> ProcessRequest {
            ProcessRequest {
                program: std::env::current_exe().unwrap().into(),
                args: [
                    "--ignored",
                    "--exact",
                    &crate::support::libtest_name(module_path!(), FIXTURE_TEST),
                    "--nocapture",
                ]
                .into_iter()
                .map(Into::into)
                .collect(),
                environment: vec![
                    ("RUNNER_FIXTURE_MODE".into(), mode.into()),
                    ("RUNNER_FIXTURE_DIR".into(), self.0.path().into()),
                ],
                environment_remove: Vec::new(),
                stdin: None,
                policy: policy(4096, 4096, Duration::from_secs(60)),
                isolate_parent_environment: false,
            }
        }

        fn identity(&self, name: &str) -> ProcessIdentity {
            serde_json::from_slice(&fs::read(self.0.path().join(name)).unwrap()).unwrap()
        }

        fn assert_original_group_gone(&self) {
            let pgid = self.identity("leader.pid").pid() as libc::pid_t;
            assert!(pgid > 1);
            assert_eq!(unsafe { libc::kill(-pgid, 0) }, -1);
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // These records contain the real PID and start time. Never signal
            // a reused PID, including when an assertion unwinds after 60 s.
            for name in ["escaped.pid", "background.pid", "leader.pid"] {
                let Ok(bytes) = fs::read(self.0.path().join(name)) else {
                    continue;
                };
                let Ok(identity) = serde_json::from_slice::<ProcessIdentity>(&bytes) else {
                    continue;
                };
                drop(FixtureProcess(identity));
            }
        }
    }

    fn record_identity(directory: &Path, name: &str, pid: u32) -> ProcessIdentity {
        let identity = SystemProcessInspector.identity_for_pid(pid).unwrap();
        let pending = directory.join(format!("{name}.pending"));
        fs::write(&pending, serde_json::to_vec(&identity).unwrap()).unwrap();
        fs::rename(pending, directory.join(name)).unwrap();
        identity
    }

    fn pause_after_escape(directory: &Path, identity: ProcessIdentity) {
        let path = directory.join("escape.sock");
        if path.exists() {
            let mut gate = UnixStream::connect(path).unwrap();
            serde_json::to_writer(&mut gate, &identity).unwrap();
            gate.shutdown(std::net::Shutdown::Write).unwrap();
            // The parent cancels the runner while this boundary is held.
            gate.read_exact(&mut [0]).unwrap();
        }
    }

    fn accept_fixture_gate(listener: &UnixListener) -> UnixStream {
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match listener.accept() {
                // BSD sockets inherit O_NONBLOCK from the listener; the read
                // timeout below only applies to a blocking stream.
                Ok((gate, _)) => {
                    gate.set_nonblocking(false).unwrap();
                    return gate;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "fixture never connected");
                    thread::yield_now();
                }
                Err(error) => panic!("fixture gate: {error}"),
            }
        }
    }

    #[test]
    #[ignore = "subprocess body: invoked only by the runner termination tests"]
    // Leaders intentionally exec/exit before descendants; the parent test
    // owns their cleanup by PID and start time, including on assertion failure.
    #[allow(clippy::zombie_processes)]
    fn pipe_holder_fixture() {
        let Ok(mode) = std::env::var("RUNNER_FIXTURE_MODE") else {
            return;
        };
        let directory = std::env::var_os("RUNNER_FIXTURE_DIR").unwrap();
        let directory = Path::new(&directory);
        if mode == "escaped-child" {
            // Publish while still in the owned group: the leader can be
            // killed at any point, but every escaped survivor is recorded.
            let identity = record_identity(directory, "escaped.pid", std::process::id());
            assert_ne!(unsafe { libc::setsid() }, -1);
            pause_after_escape(directory, identity);
            fs::write(directory.join("escaped.ready"), []).unwrap();
            panic!(
                "exec escaped sleep fixture: {}",
                Command::new("/bin/sleep").arg("60").exec()
            );
        }
        if mode == "background-child" {
            record_identity(directory, "background.pid", std::process::id());
            let mut gate = UnixStream::connect(directory.join("gate.sock")).unwrap();
            let mut bytes = Vec::new();
            gate.read_to_end(&mut bytes).unwrap();
            io::stdout().write_all(&bytes).unwrap();
            io::stderr().write_all(b"background stderr").unwrap();
            std::process::exit(0);
        }
        record_identity(directory, "leader.pid", std::process::id());
        if mode == "background" {
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    &crate::support::libtest_name(module_path!(), FIXTURE_TEST),
                    "--nocapture",
                ])
                .env("RUNNER_FIXTURE_MODE", "background-child")
                .spawn()
                .unwrap();
            // The descendant deliberately outlives a successful leader. Its
            // socket gate, not a sleep, controls when stdout reaches EOF.
            std::process::exit(0);
        }
        if mode == "fork-race" {
            // Intentional forks: this is the race the runner must converge.
            // Each child is short-lived even if a broken runner misses it.
            panic!(
                "exec fork fixture: {}",
                Command::new("/bin/sh")
                    .args(["-c", "while :; do /bin/sleep 1 & printf '%0128d' 0; done",])
                    .exec()
            );
        }

        Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                &crate::support::libtest_name(module_path!(), FIXTURE_TEST),
                "--nocapture",
            ])
            .env("RUNNER_FIXTURE_MODE", "escaped-child")
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !directory.join("escaped.ready").exists() {
            assert!(Instant::now() < deadline, "descendant did not escape");
            thread::yield_now();
        }
        if mode == "overflow" {
            io::stdout().write_all(&[b'x'; 8192]).unwrap();
            io::stdout().flush().unwrap();
        }
        // No final fork can race termination of the leader.
        panic!(
            "exec sleep fixture: {}",
            Command::new("/bin/sleep").arg("60").exec()
        );
    }

    fn escaped_descendant_returns_original_error(new_session: bool, overflow: bool) {
        let fixture = Fixture::new();
        let mut request = fixture.request(if overflow { "overflow" } else { "deadline" });
        if !overflow {
            request.policy.deadline = Duration::from_secs(1);
        }
        let started = Instant::now();
        let error = if new_session {
            SystemProcessRunner.run_in_new_session(&request)
        } else {
            SystemProcessRunner.run(&request)
        }
        .unwrap_err();
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "runner waited {:?} for a 60 s escaped pipe holder",
            started.elapsed()
        );
        if overflow {
            assert!(matches!(
                error,
                WorkerError::Process(ProcessError::OutputLimitExceeded {
                    stream: ProcessStream::Stdout,
                    limit: 4096
                })
            ));
        } else {
            assert!(matches!(
                error,
                WorkerError::Process(ProcessError::DeadlineExceeded { .. })
            ));
        }
        fixture.assert_original_group_gone();
        let escaped = fixture.identity("escaped.pid");
        assert!(matches!(
            SystemProcessInspector.observe(escaped),
            ProcessObservation::Matching { process_group } if process_group == escaped.pid()
        ));
        // Fixture::drop kills this exact escaped PID after the assertions.
    }

    #[test]
    fn deadline_abandons_an_escaped_descendants_pipes() {
        escaped_descendant_returns_original_error(false, false);
    }

    #[test]
    fn overflow_abandons_an_escaped_descendants_pipes() {
        escaped_descendant_returns_original_error(false, true);
    }

    #[test]
    fn new_session_deadline_abandons_an_escaped_descendants_pipes() {
        escaped_descendant_returns_original_error(true, false);
    }

    #[test]
    fn new_session_overflow_abandons_an_escaped_descendants_pipes() {
        escaped_descendant_returns_original_error(true, true);
    }

    #[test]
    fn cancellation_abandons_escaped_pipes_and_a_blocked_stdin_writer() {
        let fixture = Fixture::new();
        let mut request = fixture.request("cancel");
        request.stdin = Some(vec![b'x'; 16 * 1024 * 1024]);
        let started = Instant::now();
        let error = SystemProcessRunner
            .run_interruptible(&request, &|| {
                fixture.0.path().join("escaped.ready").exists()
            })
            .unwrap_err();
        assert!(matches!(
            error,
            WorkerError::Process(ProcessError::Cancelled)
        ));
        assert!(started.elapsed() < Duration::from_secs(20));
        fixture.assert_original_group_gone();
        assert!(matches!(
            SystemProcessInspector.observe(fixture.identity("escaped.pid")),
            ProcessObservation::Matching { .. }
        ));
    }

    #[test]
    fn escaped_fixture_records_cleanup_identity_before_its_leader_can_be_killed() {
        let fixture = Fixture::new();
        let listener = UnixListener::bind(fixture.0.path().join("escape.sock")).unwrap();
        let request = fixture.request("cancel");
        let cancel = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&cancel);
        let (sender, receiver) = std::sync::mpsc::channel();
        let runner = thread::spawn(move || {
            let result =
                SystemProcessRunner.run_interruptible(&request, &|| flag.load(Ordering::SeqCst));
            let _ = sender.send(result);
        });
        let mut gate = accept_fixture_gate(&listener);
        gate.set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut bytes = Vec::new();
        gate.read_to_end(&mut bytes).unwrap();
        let escaped = FixtureProcess(serde_json::from_slice(&bytes).unwrap());
        cancel.store(true, Ordering::SeqCst);
        let error = receiver
            .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
            .unwrap()
            .unwrap_err();
        runner.join().unwrap();
        assert!(matches!(
            error,
            WorkerError::Process(ProcessError::Cancelled)
        ));
        fixture.assert_original_group_gone();
        assert_eq!(fixture.identity("escaped.pid"), escaped.0);
    }

    #[test]
    fn overflow_converges_while_the_leader_forks() {
        let fixture = Fixture::new();
        let request = fixture.request("fork-race");
        let error = SystemProcessRunner.run(&request).unwrap_err();
        assert!(matches!(
            error,
            WorkerError::Process(ProcessError::OutputLimitExceeded {
                stream: ProcessStream::Stdout,
                limit: 4096
            })
        ));
        fixture.assert_original_group_gone();
    }

    #[test]
    fn successful_leader_waits_for_background_output_without_killing_the_group() {
        let fixture = Fixture::new();
        let listener = UnixListener::bind(fixture.0.path().join("gate.sock")).unwrap();
        let request = fixture.request("background");
        let (sender, receiver) = std::sync::mpsc::channel();
        let runner = thread::spawn(move || {
            let _ = sender.send(SystemProcessRunner.run(&request));
        });
        let mut gate = accept_fixture_gate(&listener);
        let deadline = Instant::now() + Duration::from_secs(20);
        let leader = fixture.identity("leader.pid");
        while !matches!(
            SystemProcessInspector.observe(leader),
            ProcessObservation::Absent | ProcessObservation::Reused
        ) {
            assert!(Instant::now() < deadline, "fixture leader did not exit");
            thread::yield_now();
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        gate.write_all(b"background stdout").unwrap();
        drop(gate);
        let result = receiver
            .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
            .unwrap()
            .unwrap();
        runner.join().unwrap();
        assert!(result.status.success());
        assert!(result.stdout.ends_with(b"background stdout"));
        assert_eq!(result.stderr, b"background stderr");
    }
}
