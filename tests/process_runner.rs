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

#[allow(dead_code)]
mod support;

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
        "isolate_parent_environment_rejects_parent_only_credentials",
        &[
            ("FIXTURE_HOME", fixture.home.to_str().unwrap()),
            ("CURSOR_API_KEY", PARENT_ONLY),
            ("PATH", support::agent_launch_fixture::empty_base_path()),
        ],
        true,
    );
}

#[test]
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
