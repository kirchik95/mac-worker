use std::time::{Duration, Instant};

use mac_worker::{
    error::{ProcessError, ProcessStream, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner, SystemProcessRunner},
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
        stdin: None,
        policy: policy(32, 32, Duration::from_secs(2)),
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
        stdin: None,
        policy: policy(32, 32, Duration::from_secs(2)),
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
fn deadline_terminates_and_reaps_a_non_exiting_child() {
    // This catches a probe child that can keep the CLI blocked forever.
    let request = ProcessRequest {
        program: "/bin/sleep".into(),
        args: vec!["5".into()],
        stdin: None,
        policy: policy(32, 32, Duration::from_millis(100)),
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
        stdin: None,
        policy: policy(32, 32, Duration::from_millis(100)),
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
        stdin: None,
        policy: policy(1024, 1024, Duration::from_secs(2)),
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, literal.as_bytes());
    assert!(result.stderr.is_empty());
}

#[test]
fn normal_process_receives_the_complete_stdin_payload() {
    // This catches the concurrent capture implementation dropping stdin or
    // closing it before the requested bytes have been written.
    let request = ProcessRequest {
        program: "/bin/cat".into(),
        args: Vec::new(),
        stdin: Some(b"raw stdin bytes\n".to_vec()),
        policy: policy(1024, 1024, Duration::from_secs(2)),
    };

    let result = SystemProcessRunner.run(&request).unwrap();

    assert!(result.status.success());
    assert_eq!(result.stdout, b"raw stdin bytes\n");
    assert!(result.stderr.is_empty());
}
