#[allow(dead_code)]
mod support;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ffi::{OsStr, OsString},
    fs,
    io::{self, Write},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use assert_cmd::Command;
use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::{Cli, Command as WorkerCommand},
    client_state::{ClientStateStore, ClientStateWritePoint},
    config::{Config, WorkerEntry},
    error::WorkerError,
    job::{
        AdmissionObservation, CancelRequest, CancelResponse, ClientId, CommandSpec,
        FleetReconcileJobResult, FleetReconcileRequest, FleetReconcileResponse, HostControlError,
        JobId, JobMeta, JobState, JobStatus, JsonEvent, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseRecord, LeaseToken, LocalJobRecord, LogChunk, LogChunkRequest, LogChunkResponse,
        LogStream, ProcessIdentity, QueueEntry, QueueEntryKind, QueueState, RemoteUncertainty,
        RequestFingerprintMaterial, ResolveOrAbandonRequest, ResolveOrAbandonResponse,
        StatusRequest, StatusResponse, SubmitRequest, SubmitResponse,
    },
    lease::{LeaseSummary, SlotState},
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project_state::ProjectState,
    protocol::{MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    remote_snapshot::{SnapshotVerifyRequest, VerifiedSnapshotResponse},
    run::{
        CancelObserver, CancelReport, CancelService, CancelStage, FollowRuntime, JobFollower,
        LogsService, RunCompletion, RunObserver, RunReport, RunRequest, RunService, RunStage,
        STATUS_LIST_LIMIT, STATUS_REFRESH_DEADLINE, STATUS_REFRESH_LIMIT, SchedulerRuntime,
        StatusReport, StatusRow, StatusService, SystemFollowRuntime,
    },
    run_with_io_in_context,
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
    },
    transfer::{HostOperation, RemoteJobClient, ResolutionRuntime},
};
use predicates::prelude::*;
use serde_json::Value;
use support::GitRepo;

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn assert_usage(arguments: &[&str]) {
    let mut command = Command::cargo_bin("worker").unwrap();
    command.args(arguments);
    command
        .assert()
        .code(64)
        .stdout(predicates::str::is_empty())
        .stderr(predicates::str::is_empty().not());
}

fn local_record(relative_working_dir: &str) -> LocalJobRecord {
    let material = RequestFingerprintMaterial::new(
        JOB_ID.parse().unwrap(),
        CLIENT_ID.parse().unwrap(),
        LEASE_TOKEN.parse().unwrap(),
        100,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        MANIFEST_DIGEST.into(),
        relative_working_dir.into(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("printf TASK9_COMMAND_SECRET".into()).unwrap(),
    )
    .unwrap();
    let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
    LocalJobRecord::new(
        meta,
        LEASE_TOKEN.parse().unwrap(),
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::None,
    )
    .unwrap()
}

type BeforeResultHook = dyn Fn(&ProcessRequest) + Send + Sync;

struct RecordingRunner {
    results: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
    requests: Mutex<Vec<ProcessRequest>>,
    before_result: Option<Box<BeforeResultHook>>,
}

impl RecordingRunner {
    fn returning(results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            results: Mutex::new(results.into()),
            requests: Mutex::new(Vec::new()),
            before_result: None,
        }
    }

    fn with_hook(
        results: Vec<Result<ProcessResult, WorkerError>>,
        hook: impl Fn(&ProcessRequest) + Send + Sync + 'static,
    ) -> Self {
        Self {
            results: Mutex::new(results.into()),
            requests: Mutex::new(Vec::new()),
            before_result: Some(Box::new(hook)),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        if let Some(hook) = &self.before_result {
            hook(request);
        }
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("a scripted SSH result must exist")
    }
}

struct ImmediateResolution {
    calls: AtomicUsize,
}

impl ImmediateResolution {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
        }
    }
}

impl ResolutionRuntime for ImmediateResolution {
    fn monotonic_now(&self) -> Duration {
        if self.calls.fetch_add(1, Ordering::SeqCst) % 3 < 2 {
            Duration::ZERO
        } else {
            Duration::from_secs(30)
        }
    }

    fn sleep(&self, _duration: Duration) {
        panic!("status lookup must not sleep")
    }
}

struct TestSchedulerRuntime {
    now_millis: AtomicU64,
    owner: ProcessIdentity,
    sleeps: Mutex<Vec<Duration>>,
}

impl TestSchedulerRuntime {
    fn new(now_millis: u64, owner_seed: u32) -> Self {
        Self {
            now_millis: AtomicU64::new(now_millis),
            owner: ProcessIdentity::new(owner_seed, u64::from(owner_seed) * 10_000 + 7).unwrap(),
            sleeps: Mutex::new(Vec::new()),
        }
    }

    fn sleeps(&self) -> Vec<Duration> {
        self.sleeps.lock().unwrap().clone()
    }
}

impl SchedulerRuntime for TestSchedulerRuntime {
    fn now_millis(&self) -> Result<u64, WorkerError> {
        Ok(self.now_millis.load(Ordering::SeqCst))
    }

    fn process_identity(&self) -> Result<ProcessIdentity, WorkerError> {
        Ok(self.owner)
    }

    fn sleep(&self, duration: Duration) {
        self.sleeps.lock().unwrap().push(duration);
        let millis = u64::try_from(duration.as_millis()).unwrap();
        self.now_millis.fetch_add(millis, Ordering::SeqCst);
    }
}

struct FailingSchedulerRuntime {
    calls: AtomicUsize,
    fail_on_call: usize,
    owner: ProcessIdentity,
}

impl FailingSchedulerRuntime {
    fn failing_on(fail_on_call: usize, owner_seed: u32) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fail_on_call,
            owner: ProcessIdentity::new(owner_seed, u64::from(owner_seed) * 10_000 + 7).unwrap(),
        }
    }
}

impl SchedulerRuntime for FailingSchedulerRuntime {
    fn now_millis(&self) -> Result<u64, WorkerError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.fail_on_call {
            Err(WorkerError::Io(io::Error::other(
                "planted scheduler clock failure",
            )))
        } else {
            Ok(70_000 + call as u64)
        }
    }

    fn process_identity(&self) -> Result<ProcessIdentity, WorkerError> {
        Ok(self.owner)
    }

    fn sleep(&self, _duration: Duration) {
        panic!("the failing no-wait scheduler must not sleep")
    }
}

#[derive(Clone, Copy)]
struct LiveOwnerInspector;

impl ProcessInspector for LiveOwnerInspector {
    fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
        ProcessIdentity::new(pid, u64::from(pid) * 10_000 + 7)
    }

    fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
        ProcessObservation::Matching {
            process_group: expected.pid(),
        }
    }

    fn observe_group(&self, _process_group: u32) -> ProcessGroupObservation {
        ProcessGroupObservation::Present
    }

    fn observe_group_members(&self, _leader: u32) -> ProcessGroupMembership {
        ProcessGroupMembership::LeaderOnly
    }
}

fn config() -> Config {
    Config {
        version: 1,
        workers: vec![WorkerEntry {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            slots: 1,
            capabilities: Vec::new(),
            remote_binary: "~/.local/bin/worker".into(),
        }],
    }
}

fn status_result(response: &impl serde::Serialize) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(response).unwrap();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })
}

fn transport_failure() -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: ExitStatus::from_raw(255 << 8),
        stdout: Vec::new(),
        stderr: Vec::new(),
    })
}

fn launch_reply_loss() -> Result<ProcessResult, WorkerError> {
    // The transport maps this post-dispatch loss to SSH_LAUNCH_FAILED. It
    // remains ambiguous because the host may already have accepted and acted
    // on the complete cancel request before its reply is lost locally.
    Err(WorkerError::Io(io::Error::other(
        "injected post-dispatch reply loss",
    )))
}

fn authoritative_protocol_failure(code: &str, message: &str) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(&HostControlError::new(code, message).unwrap()).unwrap();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(23 << 8),
        stdout,
        stderr: Vec::new(),
    })
}

fn oversized_authoritative_protocol_failure(
    code: &str,
    secret: &str,
) -> Result<ProcessResult, WorkerError> {
    // Bypasses HostControlError::new's own 4096-byte bound so the wire
    // envelope itself carries a message far past MAX_CONTROL_MESSAGE_BYTES.
    // Built as a raw canonical literal (rather than via serde_json::Value,
    // whose default map reorders keys) so it round-trips exactly like the
    // real type's derived Serialize output.
    let oversized_message = format!("{}{secret}", "X".repeat(200_000));
    let mut stdout = format!(
        r#"{{"protocol_version":{PROTOCOL_VERSION},"error":{{"code":"{code}","message":"{oversized_message}"}}}}"#
    )
    .into_bytes();
    stdout.push(b'\n');
    Ok(ProcessResult {
        status: ExitStatus::from_raw(23 << 8),
        stdout,
        stderr: Vec::new(),
    })
}

fn test_record(
    store: &ClientStateStore,
    seed: u128,
    created_at_millis: u64,
    status: Option<JobStatus>,
    uncertainty: RemoteUncertainty,
) -> LocalJobRecord {
    record_with_meta_parts(
        JobId::new(uuid::Uuid::from_u128(seed)),
        store.client_id(),
        "mini-1",
        &"a".repeat(64),
        &"b".repeat(64),
        &"c".repeat(64),
        "packages/app",
        30_000,
        "heavy",
        CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        created_at_millis,
        None,
        status,
        uncertainty,
    )
}

#[allow(clippy::too_many_arguments)]
fn record_with_meta_parts(
    job_id: JobId,
    client_id: ClientId,
    worker: &str,
    project_id: &str,
    worktree_id: &str,
    manifest_digest: &str,
    relative_working_dir: &str,
    timeout_millis: u64,
    resource_class: &str,
    command: CommandSpec,
    created_at_millis: u64,
    lease_token: Option<LeaseToken>,
    status: Option<JobStatus>,
    uncertainty: RemoteUncertainty,
) -> LocalJobRecord {
    let lease_token = lease_token.unwrap_or_else(|| {
        LeaseToken::new(uuid::Uuid::from_u128(job_id.as_uuid().as_u128() + 10_000))
    });
    let material = RequestFingerprintMaterial::new(
        job_id,
        client_id,
        lease_token,
        created_at_millis,
        worker.into(),
        project_id.into(),
        worktree_id.into(),
        manifest_digest.into(),
        relative_working_dir.into(),
        timeout_millis,
        resource_class.into(),
        command,
    )
    .unwrap();
    let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
    LocalJobRecord::new(meta, material.lease_token(), status, uncertainty).unwrap()
}

fn service<'a>(
    config: &'a Config,
    store: &'a ClientStateStore,
    remote: &'a RemoteJobClient<'a>,
) -> StatusService<'a> {
    StatusService {
        config,
        client_state: store,
        remote,
    }
}

fn state_store(temp: &tempfile::TempDir) -> ClientStateStore {
    ClientStateStore::open(&temp.path().canonicalize().unwrap().join("state")).unwrap()
}

fn job_file(temp: &tempfile::TempDir, job_id: JobId) -> PathBuf {
    temp.path()
        .canonicalize()
        .unwrap()
        .join("state/jobs")
        .join(format!("{job_id}.json"))
}

fn canonical_job_bytes(record: &LocalJobRecord) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(record).unwrap();
    bytes.push(b'\n');
    bytes
}

fn assert_status_call(request: &ProcessRequest, deadline: Duration, job_id: JobId) {
    assert_control_call(request, HostOperation::Status, deadline);
    let status: StatusRequest = serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
    assert_eq!(status.job_id(), job_id);
}

fn assert_resolution_status_call(request: &ProcessRequest, job_id: JobId) {
    assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
    assert_eq!(
        request.args.last().unwrap(),
        HostOperation::Status.command()
    );
    assert_eq!(request.args[9], OsStr::new("mac1"));
    assert!(
        request.policy.deadline > Duration::ZERO
            && request.policy.deadline <= Duration::from_secs(30),
        "resolution status request must retain a finite control deadline"
    );
    let status: StatusRequest = serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
    assert_eq!(status.job_id(), job_id);
}

fn assert_control_call(request: &ProcessRequest, operation: HostOperation, deadline: Duration) {
    assert_eq!(request.program, OsStr::new("/usr/bin/ssh"));
    assert_eq!(request.args.last().unwrap(), operation.command());
    assert_eq!(request.args[9], OsStr::new("mac1"));
    assert_eq!(request.policy.deadline, deadline);
}

#[test]
fn run_parses_literal_argv_and_explicit_shell_forms() {
    // Catches parsing that consumes command flags, loses include order, or
    // interprets the literal argv rather than retaining it as UTF-8 strings.
    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--",
        "npm",
        "test",
        "--",
        "--literal",
    ])
    .unwrap();
    let WorkerCommand::Run {
        worker,
        no_wait,
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker.as_deref(), Some("mini-1"));
    assert!(!no_wait);
    assert_eq!(project, None);
    assert!(includes.is_empty());
    assert_eq!(timeout, None);
    assert_eq!(shell, None);
    assert_eq!(argv, ["npm", "test", "--", "--literal"]);

    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--project",
        "/repo",
        "--include",
        "fixtures/generated/**",
        "--timeout",
        "45m",
        "--",
        "npm",
        "test",
    ])
    .unwrap();
    let WorkerCommand::Run {
        worker,
        no_wait,
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker.as_deref(), Some("mini-1"));
    assert!(!no_wait);
    assert_eq!(project, Some(PathBuf::from("/repo")));
    assert_eq!(includes, ["fixtures/generated/**"]);
    assert_eq!(timeout, Some(Duration::from_secs(45 * 60)));
    assert_eq!(shell, None);
    assert_eq!(argv, ["npm", "test"]);

    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--shell",
        "npm run build && npm test",
    ])
    .unwrap();
    let WorkerCommand::Run {
        worker,
        no_wait,
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker.as_deref(), Some("mini-1"));
    assert!(!no_wait);
    assert_eq!(project, None);
    assert!(includes.is_empty());
    assert_eq!(timeout, None);
    assert_eq!(shell.as_deref(), Some("npm run build && npm test"));
    assert!(argv.is_empty());

    let request = RunRequest {
        preference: WorkerPreference::Pinned {
            worker: worker.unwrap(),
        },
        wait_for_capacity: !no_wait,
        project: PathBuf::from("/path/to/worktree"),
        cli_includes: includes,
        timeout,
        command: CommandSpec::shell(shell.unwrap()).unwrap(),
    };
    assert_eq!(
        request.preference,
        WorkerPreference::Pinned {
            worker: "mini-1".into()
        }
    );
    assert!(request.wait_for_capacity);

    let cli = Cli::try_parse_from([
        "worker",
        "run",
        "--worker",
        "mini-1",
        "--include",
        "fixtures/generated/**",
        "--include",
        "tmp/contract.json",
        "--",
        "true",
    ])
    .unwrap();
    let WorkerCommand::Run { includes, .. } = cli.command else {
        panic!("run form must select run command");
    };
    assert_eq!(includes, ["fixtures/generated/**", "tmp/contract.json"]);
}

#[test]
fn run_rejects_empty_worker_or_command_and_mutually_exclusive_modes() {
    // Catches accepting an empty explicit pin or ambiguous command mode.
    for arguments in [
        vec!["run", "--worker", "", "--", "true"],
        vec!["run"],
        vec!["run", "--worker", "mini-1"],
        vec!["run", "--shell", ""],
        vec!["run", "--shell", "true", "--", "echo"],
    ] {
        assert_usage(&arguments);
    }
}

#[test]
fn run_parses_automatic_pinned_wait_and_no_wait_forms() {
    // Break caught: automatic scheduling still requires --worker, --no-wait is
    // ignored, or an explicit pin is collapsed into the automatic form.
    let cases = [
        (vec!["worker", "run", "--", "npm", "test"], None, false),
        (
            vec!["worker", "run", "--worker", "mini-2", "--", "npm", "test"],
            Some("mini-2"),
            false,
        ),
        (
            vec!["worker", "run", "--no-wait", "--", "npm", "test"],
            None,
            true,
        ),
        (
            vec![
                "worker",
                "run",
                "--worker",
                "mini-2",
                "--no-wait",
                "--",
                "npm",
                "test",
            ],
            Some("mini-2"),
            true,
        ),
    ];

    for (arguments, expected_worker, expected_no_wait) in cases {
        let cli = Cli::try_parse_from(arguments).expect("scheduler run form must parse");
        let WorkerCommand::Run {
            worker,
            no_wait,
            argv,
            ..
        } = cli.command
        else {
            panic!("scheduler form must select run command");
        };
        assert_eq!(worker.as_deref(), expected_worker);
        assert_eq!(no_wait, expected_no_wait);
        assert_eq!(argv, ["npm", "test"]);
    }
}

#[test]
fn run_rejects_invalid_timeout_and_future_phase_flags() {
    // Catches timeout boundaries or future-phase options becoming accepted.
    for arguments in [
        vec![
            "run",
            "--worker",
            "mini-1",
            "--timeout",
            "invalid",
            "--",
            "true",
        ],
        vec!["run", "--worker", "mini-1", "--timeout", "0s", "--", "true"],
        vec![
            "run",
            "--worker",
            "mini-1",
            "--timeout",
            "24h1s",
            "--",
            "true",
        ],
        vec![
            "run",
            "--worker",
            "mini-1",
            "--artifact",
            "out",
            "--",
            "true",
        ],
        vec![
            "run",
            "--worker",
            "mini-1",
            "--env",
            "TOKEN=value",
            "--",
            "true",
        ],
        vec![
            "run", "--worker", "mini-1", "--cache", "target", "--", "true",
        ],
    ] {
        assert_usage(&arguments);
    }
}

#[test]
fn status_is_a_list_without_id_and_exact_lookup_with_id() {
    // Catches making status require an ID or accepting a non-canonical ID.
    let cli = Cli::try_parse_from(["worker", "status"]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Status { job_id: None }
    ));

    let cli = Cli::try_parse_from(["worker", "status", JOB_ID]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Status { job_id: Some(id) } if id.to_string() == JOB_ID
    ));

    assert_usage(&["status", "not-a-job-id"]);
    assert_usage(&["status", JOB_ID, JOB_ID]);
}

#[test]
fn logs_requires_exact_id_and_accepts_short_follow_flag() {
    // Catches optional or non-canonical log identity and loss of `-f`.
    let cli = Cli::try_parse_from(["worker", "logs", JOB_ID]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Logs {
            follow: false,
            job_id
        } if job_id.to_string() == JOB_ID
    ));

    let cli = Cli::try_parse_from(["worker", "logs", "-f", JOB_ID]).unwrap();
    assert!(matches!(
        cli.command,
        WorkerCommand::Logs {
            follow: true,
            job_id
        } if job_id.to_string() == JOB_ID
    ));

    assert_usage(&["logs"]);
    assert_usage(&["logs", "not-a-job-id"]);
    assert_usage(&["logs", JOB_ID, JOB_ID]);
}

#[test]
fn status_row_serialization_omits_private_identity_and_payload() {
    // Catches exposing any record field beyond the explicitly sanitized row.
    let command_secret = "printf TASK9_COMMAND_SECRET";
    let material = RequestFingerprintMaterial::new(
        JOB_ID.parse().unwrap(),
        CLIENT_ID.parse().unwrap(),
        LEASE_TOKEN.parse().unwrap(),
        100,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        MANIFEST_DIGEST.into(),
        "packages/app".into(),
        30_000,
        "heavy".into(),
        CommandSpec::shell(command_secret.into()).unwrap(),
    )
    .unwrap();
    let meta = JobMeta::new(&material, material.fingerprint()).unwrap();
    let status = JobStatus::accepted(101).unwrap();
    let record = LocalJobRecord::new(
        meta,
        LEASE_TOKEN.parse().unwrap(),
        Some(status.clone()),
        RemoteUncertainty::unknown_remote("STATUS_UNCERTAIN").unwrap(),
    )
    .unwrap();

    let row = StatusRow::try_from_record(&record).unwrap();
    let value = serde_json::to_value(&row).unwrap();
    let keys = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        BTreeSet::from([
            "command_summary",
            "created_at_millis",
            "job_id",
            "manifest_digest",
            "project_id",
            "relative_working_dir",
            "remote_uncertainty",
            "status",
            "worker",
            "worktree_id",
        ])
    );
    let encoded = serde_json::to_string(&row).unwrap();
    for secret in [CLIENT_ID, LEASE_TOKEN, command_secret] {
        assert!(!encoded.contains(secret));
    }
    assert_eq!(value["job_id"], JOB_ID);
    assert_eq!(value["worker"], "mini-1");
    assert_eq!(value["command_summary"]["mode"], "shell");

    let run_report = RunReport::new(JOB_ID.parse().unwrap(), "mini-1".into(), status).unwrap();
    assert_eq!(run_report.protocol_version, PROTOCOL_VERSION);
    let completion = RunCompletion {
        report: run_report,
        exit_code: 0,
    };
    assert_eq!(completion.exit_code, 0);

    let report = StatusReport::new(vec![row], 3);
    assert_eq!(report.protocol_version, PROTOCOL_VERSION);
    assert_eq!(report.omitted, 3);
    let report_value: Value = serde_json::to_value(report).unwrap();
    assert_eq!(report_value["protocol_version"], PROTOCOL_VERSION);
}

#[test]
fn status_row_rejects_unsafe_relative_working_directories_without_echoing_them() {
    // Catches a valid persistent record exposing a full or traversal path at
    // the narrower public-report boundary.
    let unsafe_paths = [
        "/Users/alice/secret-project",
        "../../secret",
        "packages/./secret",
        "packages//secret",
        "packages\\secret",
        ".git/config",
    ];
    let mut leaked = Vec::new();

    for planted_path in unsafe_paths {
        match StatusRow::try_from_record(&local_record(planted_path)) {
            Err(WorkerError::Protocol(message)) => {
                assert!(message.contains("INVALID_LOCAL_RECORD"));
                assert!(!message.contains(planted_path));
            }
            Err(error) => panic!("unsafe local path returned the wrong error: {error}"),
            Ok(row) => leaked.push(row.relative_working_dir),
        }
    }

    assert!(
        leaked.is_empty(),
        "unsafe paths reached the public status row: {leaked:?}"
    );
}

#[test]
fn status_row_allows_an_empty_root_relative_working_directory() {
    // Catches treating the canonical project root as an unsafe empty path.
    let row = StatusRow::try_from_record(&local_record("")).unwrap();
    assert_eq!(row.relative_working_dir, "");
}

#[test]
fn status_without_id_lists_100_newest_and_reports_exact_omission() {
    // Catches an unbounded list, wrong newest-first ordering, unstable ties,
    // or an approximate omitted count.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    for seed in 1..=101 {
        let created = match seed {
            1 | 2 => 999,
            101 => 1_000,
            _ => seed as u64,
        };
        store
            .create_job(test_record(
                &store,
                seed,
                created,
                Some(JobStatus::succeeded(created, 0, 0).unwrap()),
                RemoteUncertainty::None,
            ))
            .unwrap();
    }
    let runner = RecordingRunner::returning(Vec::new());
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let report = service(&config, &store, &remote).inspect(None).unwrap();

    assert_eq!(report.jobs.len(), 100);
    assert_eq!(report.omitted, 1);
    assert_eq!(
        report.jobs[0].job_id,
        JobId::new(uuid::Uuid::from_u128(101))
    );
    assert_eq!(report.jobs[1].job_id, JobId::new(uuid::Uuid::from_u128(1)));
    assert_eq!(report.jobs[2].job_id, JobId::new(uuid::Uuid::from_u128(2)));
    assert_eq!(report.jobs.last().unwrap().created_at_millis, 4);
    assert!(runner.requests().is_empty());
    assert_eq!(STATUS_LIST_LIMIT, 100);
}

#[test]
fn status_list_never_implicitly_selects_latest_job() {
    // Catches no-ID mode returning only the newest record as if it were an
    // exact-ID lookup.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    for (seed, created) in [(1, 100), (2, 200)] {
        store
            .create_job(test_record(
                &store,
                seed,
                created,
                Some(JobStatus::succeeded(created, 0, 0).unwrap()),
                RemoteUncertainty::None,
            ))
            .unwrap();
    }
    let runner = RecordingRunner::returning(Vec::new());
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let report = service(&config, &store, &remote).inspect(None).unwrap();

    assert_eq!(report.jobs.len(), 2);
    assert_eq!(report.jobs[0].job_id, JobId::new(uuid::Uuid::from_u128(2)));
    assert_eq!(report.jobs[1].job_id, JobId::new(uuid::Uuid::from_u128(1)));
    assert_eq!(report.omitted, 0);
}

#[test]
fn status_list_refreshes_at_most_16_active_or_unknown_rows_once_at_five_seconds() {
    // Catches refreshing beyond the cap, using the 30-second exact deadline,
    // retrying, or selecting rows outside newest-first list order.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let mut newest = Vec::new();
    let mut results = Vec::new();
    for seed in 1..=18 {
        let created = 1_000 + seed as u64;
        let uncertainty = if seed % 2 == 0 {
            RemoteUncertainty::unknown_remote("STATUS_UNCERTAIN").unwrap()
        } else {
            RemoteUncertainty::None
        };
        let record = test_record(&store, seed, created, None, uncertainty);
        store.create_job(record.clone()).unwrap();
        newest.push(record);
    }
    newest.sort_by(|left, right| {
        right
            .meta()
            .created_at_millis()
            .cmp(&left.meta().created_at_millis())
    });
    for (index, record) in newest.iter().take(16).enumerate() {
        results.push(if index == 0 {
            transport_failure()
        } else {
            status_result(
                &StatusResponse::new(
                    record.meta().clone(),
                    JobStatus::accepted(record.meta().created_at_millis()).unwrap(),
                )
                .unwrap(),
            )
        });
    }
    let runner = RecordingRunner::returning(results);
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let report = service(&config, &store, &remote).inspect(None).unwrap();

    let requests = runner.requests();
    assert_eq!(requests.len(), 16);
    for (request, record) in requests.iter().zip(&newest) {
        assert_status_call(request, Duration::from_secs(5), record.meta().job_id());
    }
    assert!(report.jobs[0].status.is_none());
    assert!(
        matches!(report.jobs[0].remote_uncertainty, RemoteUncertainty::UnknownRemote { ref code } if code == "STATUS_UNCERTAIN")
    );
    assert!(report.jobs[1..16].iter().all(|row| row.status.is_some()));
    assert!(report.jobs[16..].iter().all(|row| row.status.is_none()));
    assert_eq!(STATUS_REFRESH_LIMIT, 16);
    assert_eq!(STATUS_REFRESH_DEADLINE, Duration::from_secs(5));
}

#[test]
fn status_exact_id_never_falls_back_to_another_record() {
    // Catches load failure falling back to newest local history.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    store
        .create_job(test_record(
            &store,
            1,
            100,
            Some(JobStatus::succeeded(100, 0, 0).unwrap()),
            RemoteUncertainty::None,
        ))
        .unwrap();
    let runner = RecordingRunner::returning(Vec::new());
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let error = service(&config, &store, &remote)
        .inspect(Some(JobId::new(uuid::Uuid::from_u128(999))))
        .unwrap_err();

    assert!(matches!(error, WorkerError::Config(message) if message.starts_with("JOB_NOT_FOUND:")));
    assert!(runner.requests().is_empty());
}

#[test]
fn status_rejects_same_id_response_with_any_immutable_meta_difference() {
    // Catches partial metadata checks. Each persisted immutable JobMeta field
    // is independently mismatched, including creation time and fingerprint.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let local = test_record(
        &store,
        1,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(local.clone()).unwrap();
    let id = local.meta().job_id();
    let different_client = ClientId::new(uuid::Uuid::from_u128(777));
    let different_lease = LeaseToken::new(uuid::Uuid::from_u128(777));
    let cases = vec![
        (
            "job_id",
            record_with_meta_parts(
                JobId::new(uuid::Uuid::from_u128(2)),
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "client_id",
            record_with_meta_parts(
                id,
                different_client,
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "worker_name",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-2",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "project_id",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"d".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "worktree_id",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"d".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "manifest_digest",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"d".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "request_fingerprint",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                Some(different_lease),
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "command_summary",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::shell("cargo test".into()).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "relative_working_dir",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/other",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "timeout_millis",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                45_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "resource_class",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "light",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                Some(JobStatus::accepted(101).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
        (
            "created_at_millis",
            record_with_meta_parts(
                id,
                store.client_id(),
                "mini-1",
                &"a".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                102,
                None,
                Some(JobStatus::accepted(102).unwrap()),
                RemoteUncertainty::None,
            ),
        ),
    ];
    let config = config();
    for (field, remote_record) in cases {
        let response = StatusResponse::new(
            remote_record.meta().clone(),
            remote_record.last_status().unwrap().clone(),
        )
        .unwrap();
        let runner = RecordingRunner::returning(vec![status_result(&response)]);
        let remote = RemoteJobClient::new(&runner);
        let error = service(&config, &store, &remote)
            .inspect(Some(id))
            .unwrap_err();
        assert!(
            matches!(
                error,
                WorkerError::Transport {
                    code: "INVALID_RESPONSE",
                    ..
                }
            ),
            "field {field}: {error}"
        );
        let requests = runner.requests();
        assert_eq!(requests.len(), 1, "field {field}");
        assert_status_call(&requests[0], Duration::from_secs(30), id);
        assert_eq!(store.load_job(id).unwrap(), local, "field {field}");
    }

    let valid =
        StatusResponse::new(local.meta().clone(), JobStatus::accepted(101).unwrap()).unwrap();
    let mut wrong_protocol = serde_json::to_value(valid).unwrap();
    wrong_protocol["meta"]["protocol_version"] = serde_json::json!(999);
    let mut stdout = serde_json::to_vec(&wrong_protocol).unwrap();
    stdout.push(b'\n');
    let runner = RecordingRunner::returning(vec![Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout,
        stderr: Vec::new(),
    })]);
    let remote = RemoteJobClient::new(&runner);
    let error = service(&config, &store, &remote)
        .inspect(Some(id))
        .unwrap_err();
    assert!(matches!(
        error,
        WorkerError::Transport {
            code: "INVALID_RESPONSE",
            ..
        }
    ));
    assert_eq!(runner.requests().len(), 1);
    assert_eq!(store.load_job(id).unwrap(), local);
}

#[test]
fn status_exact_unknown_and_cleanup_pending_retry_resolution_without_conflation() {
    // Catches uncertainty markers taking the normal status path, being merged,
    // or being cleared without accepted authority.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let to_unknown = test_record(
        &store,
        1,
        100,
        None,
        RemoteUncertainty::cleanup_pending("OLD_CLEANUP").unwrap(),
    );
    let to_cleanup = test_record(
        &store,
        2,
        200,
        None,
        RemoteUncertainty::unknown_remote("LOST_REPLY").unwrap(),
    );
    let to_accepted = test_record(
        &store,
        3,
        300,
        None,
        RemoteUncertainty::unknown_remote("LOST_ACCEPT").unwrap(),
    );
    store.create_job(to_unknown.clone()).unwrap();
    store.create_job(to_cleanup.clone()).unwrap();
    store.create_job(to_accepted.clone()).unwrap();
    let cleanup_response =
        ResolveOrAbandonResponse::cleanup_pending("LEASE_RELEASE_FAILED").unwrap();
    let accepted_status = StatusResponse::new(
        to_accepted.meta().clone(),
        JobStatus::accepted(301).unwrap(),
    )
    .unwrap();
    let accepted_response = ResolveOrAbandonResponse::accepted(accepted_status.clone()).unwrap();
    let runner = RecordingRunner::returning(vec![
        transport_failure(),
        transport_failure(),
        transport_failure(),
        status_result(&cleanup_response),
        transport_failure(),
        status_result(&accepted_response),
    ]);
    let runtime = ImmediateResolution::new();
    let remote = RemoteJobClient::new_with_runtime(&runner, &runtime);
    let config = config();

    let unknown_report = service(&config, &store, &remote)
        .inspect(Some(to_unknown.meta().job_id()))
        .unwrap();
    let cleanup_report = service(&config, &store, &remote)
        .inspect(Some(to_cleanup.meta().job_id()))
        .unwrap();
    let accepted_report = service(&config, &store, &remote)
        .inspect(Some(to_accepted.meta().job_id()))
        .unwrap();

    assert!(
        matches!(unknown_report.jobs[0].remote_uncertainty, RemoteUncertainty::UnknownRemote { ref code } if code == "UNKNOWN_REMOTE")
    );
    assert!(
        matches!(cleanup_report.jobs[0].remote_uncertainty, RemoteUncertainty::CleanupPending { ref code } if code == "LEASE_RELEASE_FAILED")
    );
    assert_eq!(
        accepted_report.jobs[0].status.as_ref(),
        Some(accepted_status.status())
    );
    assert_eq!(
        accepted_report.jobs[0].remote_uncertainty,
        RemoteUncertainty::None
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 6);
    let (pairs, remainder) = requests.as_chunks::<2>();
    assert!(remainder.is_empty());
    for (pair, record) in pairs.iter().zip([&to_unknown, &to_cleanup, &to_accepted]) {
        assert_status_call(&pair[0], Duration::from_secs(30), record.meta().job_id());
        assert_control_call(
            &pair[1],
            HostOperation::ResolveOrAbandon,
            Duration::from_secs(30),
        );
    }
    let unknown_request: ResolveOrAbandonRequest =
        serde_json::from_slice(requests[1].stdin.as_deref().unwrap()).unwrap();
    let cleanup_request: ResolveOrAbandonRequest =
        serde_json::from_slice(requests[3].stdin.as_deref().unwrap()).unwrap();
    let accepted_request: ResolveOrAbandonRequest =
        serde_json::from_slice(requests[5].stdin.as_deref().unwrap()).unwrap();
    assert_eq!(unknown_request.job_id(), to_unknown.meta().job_id());
    assert_eq!(cleanup_request.job_id(), to_cleanup.meta().job_id());
    assert_eq!(accepted_request.job_id(), to_accepted.meta().job_id());
}

#[test]
fn status_never_persists_or_fabricates_abandoned_job_state() {
    // Catches converting transient Abandoned authority into a durable status
    // or erasing the pre-existing uncertainty marker.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        1,
        100,
        None,
        RemoteUncertainty::unknown_remote("LOST_REPLY").unwrap(),
    );
    store.create_job(record.clone()).unwrap();
    let runner = RecordingRunner::returning(vec![
        transport_failure(),
        status_result(&ResolveOrAbandonResponse::abandoned()),
    ]);
    let runtime = ImmediateResolution::new();
    let remote = RemoteJobClient::new_with_runtime(&runner, &runtime);
    let config = config();

    let error = service(&config, &store, &remote)
        .inspect(Some(record.meta().job_id()))
        .unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(message) if message.starts_with("JOB_ABANDONED:"))
    );
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
    let requests = runner.requests();
    assert_eq!(requests.len(), 2);
    assert_status_call(
        &requests[0],
        Duration::from_secs(30),
        record.meta().job_id(),
    );
    assert_control_call(
        &requests[1],
        HostOperation::ResolveOrAbandon,
        Duration::from_secs(30),
    );
    let resolve: ResolveOrAbandonRequest =
        serde_json::from_slice(requests[1].stdin.as_deref().unwrap()).unwrap();
    assert_eq!(resolve.job_id(), record.meta().job_id());
}

#[test]
fn status_terminal_rows_are_not_refreshed_in_list_mode() {
    // Catches needless remote effects for already-terminal rows.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    for seed in 1..=20 {
        store
            .create_job(test_record(
                &store,
                seed,
                100 + seed as u64,
                Some(JobStatus::succeeded(100 + seed as u64, 0, 0).unwrap()),
                RemoteUncertainty::None,
            ))
            .unwrap();
    }
    let runner = RecordingRunner::returning(Vec::new());
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let report = service(&config, &store, &remote).inspect(None).unwrap();

    assert_eq!(report.jobs.len(), 20);
    assert!(
        report
            .jobs
            .iter()
            .all(|row| row.status.as_ref().unwrap().state().is_terminal())
    );
    assert!(runner.requests().is_empty());
}

#[test]
fn status_refresh_preserves_concurrent_forward_local_observation() {
    // Catches writing a stale remote response over a local observation that
    // advanced while SSH status was in flight.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        1,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::unknown_remote("LOST_REPLY").unwrap(),
    );
    store.create_job(record.clone()).unwrap();
    let remote_status = JobStatus::running(200, 11, 12, 13, 14).unwrap();
    let response = StatusResponse::new(record.meta().clone(), remote_status.clone()).unwrap();
    let concurrent = remote_status.into_succeeded(300, 7, 9).unwrap();
    let hook_store = store.clone();
    let hook_status = concurrent.clone();
    let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
        hook_store
            .update_observation(record.meta().job_id(), hook_status.clone())
            .unwrap();
    });
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let report = service(&config, &store, &remote).inspect(None).unwrap();

    assert_eq!(report.jobs[0].status.as_ref(), Some(&concurrent));
    let persisted = store.load_job(report.jobs[0].job_id).unwrap();
    assert_eq!(persisted.last_status(), Some(&concurrent));
    assert_eq!(persisted.remote_uncertainty(), &RemoteUncertainty::None);
    let requests = runner.requests();
    assert_eq!(requests.len(), 1);
    assert_status_call(&requests[0], Duration::from_secs(5), report.jobs[0].job_id);

    let terminal_temp = tempfile::tempdir().unwrap();
    let terminal_store = state_store(&terminal_temp);
    let terminal_record = test_record(
        &terminal_store,
        2,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::unknown_remote("TERMINAL_RACE").unwrap(),
    );
    let terminal_id = terminal_record.meta().job_id();
    terminal_store.create_job(terminal_record.clone()).unwrap();
    let response = StatusResponse::new(
        terminal_record.meta().clone(),
        JobStatus::succeeded(200, 1, 2).unwrap(),
    )
    .unwrap();
    let concurrent_failed = JobStatus::failed(300, 7, 3, 4).unwrap();
    let hook_store = terminal_store.clone();
    let hook_status = concurrent_failed.clone();
    let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
        hook_store
            .update_observation(terminal_id, hook_status.clone())
            .unwrap();
    });
    let remote = RemoteJobClient::new(&runner);
    let report = service(&config, &terminal_store, &remote)
        .inspect(None)
        .unwrap();
    assert_eq!(report.jobs[0].status.as_ref(), Some(&concurrent_failed));
    assert!(
        matches!(report.jobs[0].remote_uncertainty, RemoteUncertainty::UnknownRemote { ref code } if code == "TERMINAL_RACE")
    );

    let identity_temp = tempfile::tempdir().unwrap();
    let identity_store = state_store(&identity_temp);
    let identity_record = test_record(
        &identity_store,
        3,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::unknown_remote("IDENTITY_RACE").unwrap(),
    );
    let identity_id = identity_record.meta().job_id();
    identity_store.create_job(identity_record.clone()).unwrap();
    let response = StatusResponse::new(
        identity_record.meta().clone(),
        JobStatus::running(200, 11, 12, 13, 14).unwrap(),
    )
    .unwrap();
    let concurrent_running = JobStatus::running(300, 21, 22, 23, 24).unwrap();
    let hook_store = identity_store.clone();
    let hook_status = concurrent_running.clone();
    let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
        hook_store
            .update_observation(identity_id, hook_status.clone())
            .unwrap();
    });
    let remote = RemoteJobClient::new(&runner);
    let report = service(&config, &identity_store, &remote)
        .inspect(None)
        .unwrap();
    assert_eq!(report.jobs[0].status.as_ref(), Some(&concurrent_running));
    assert!(
        matches!(report.jobs[0].remote_uncertainty, RemoteUncertainty::UnknownRemote { ref code } if code == "IDENTITY_RACE")
    );
}

#[test]
fn status_refresh_binds_remote_authority_to_full_expected_local_immutable_identity() {
    // Catches a same-ID record replacement between response validation and
    // the two local mutations, including the lease identity absent from meta.
    for replacement_kind in ["meta", "lease"] {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let original = test_record(
            &store,
            40,
            100,
            Some(JobStatus::accepted(101).unwrap()),
            RemoteUncertainty::None,
        );
        store.create_job(original.clone()).unwrap();
        let replacement = if replacement_kind == "meta" {
            record_with_meta_parts(
                original.meta().job_id(),
                store.client_id(),
                "mini-1",
                &"d".repeat(64),
                &"b".repeat(64),
                &"c".repeat(64),
                "packages/app",
                30_000,
                "heavy",
                CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
                100,
                None,
                None,
                RemoteUncertainty::unknown_remote("REPLACED_META").unwrap(),
            )
        } else {
            LocalJobRecord::new(
                original.meta().clone(),
                LeaseToken::new(uuid::Uuid::from_u128(999_999)),
                None,
                RemoteUncertainty::unknown_remote("REPLACED_LEASE").unwrap(),
            )
            .unwrap()
        };
        let response = StatusResponse::new(
            original.meta().clone(),
            JobStatus::running(200, 11, 12, 13, 14).unwrap(),
        )
        .unwrap();
        let path = job_file(&temp, original.meta().job_id());
        let replacement_bytes = canonical_job_bytes(&replacement);
        let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
            fs::write(&path, &replacement_bytes).unwrap();
        });
        let remote = RemoteJobClient::new(&runner);
        let config = config();

        let error = service(&config, &store, &remote)
            .inspect(Some(original.meta().job_id()))
            .unwrap_err();

        assert!(
            matches!(error, WorkerError::Protocol(ref message) if message.contains("JOB_ID_CONFLICT")),
            "{replacement_kind}: {error}"
        );
        assert_eq!(
            store.load_job(original.meta().job_id()).unwrap(),
            replacement,
            "{replacement_kind}"
        );
    }
}

#[test]
fn status_list_propagates_in_flight_local_registry_corruption_and_replacement() {
    // Catches list mode treating local corruption, disappearance, or a new
    // immutable binding as a tolerable remote refresh failure.
    for mutation in ["truncate", "remove", "replace"] {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let original = test_record(&store, 50, 100, None, RemoteUncertainty::None);
        store.create_job(original.clone()).unwrap();
        let path = job_file(&temp, original.meta().job_id());
        let replacement = record_with_meta_parts(
            original.meta().job_id(),
            store.client_id(),
            "mini-1",
            &"e".repeat(64),
            &"b".repeat(64),
            &"c".repeat(64),
            "packages/app",
            30_000,
            "heavy",
            CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
            100,
            None,
            None,
            RemoteUncertainty::None,
        );
        let replacement_bytes = canonical_job_bytes(&replacement);
        let runner =
            RecordingRunner::with_hook(vec![transport_failure()], move |_| match mutation {
                "truncate" => fs::write(&path, b"{").unwrap(),
                "remove" => fs::remove_file(&path).unwrap(),
                "replace" => fs::write(&path, &replacement_bytes).unwrap(),
                _ => unreachable!(),
            });
        let remote = RemoteJobClient::new(&runner);
        let config = config();

        let error = service(&config, &store, &remote).inspect(None).unwrap_err();

        match mutation {
            "truncate" => assert!(
                matches!(error, WorkerError::Io(ref io) if io.kind() == std::io::ErrorKind::InvalidData)
            ),
            "remove" => assert!(
                matches!(error, WorkerError::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound)
            ),
            "replace" => assert!(
                matches!(error, WorkerError::Protocol(ref message) if message.contains("JOB_ID_CONFLICT"))
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn status_list_propagates_local_conditional_update_io_failure() {
    // Catches treating a local conditional-write failure as a tolerable
    // remote response failure or as a concurrent observation conflict.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(&store, 55, 100, None, RemoteUncertainty::None);
    store.create_job(record.clone()).unwrap();
    let response =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(101).unwrap()).unwrap();
    let hook_store = store.clone();
    let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
        hook_store.inject_write_failure_once(ClientStateWritePoint::BeforePublish);
    });
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let error = service(&config, &store, &remote).inspect(None).unwrap_err();

    assert!(matches!(error, WorkerError::Io(ref io) if io.kind() == std::io::ErrorKind::Other));
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
}

#[test]
fn status_refresh_preserves_each_valid_accepted_identity_enrichment_step() {
    // Catches requiring one direct transition when the local observation may
    // have advanced through both valid Accepted identity enrichments in flight.
    for observed_step in ["supervisor", "transitive_child", "direct_child"] {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let bare = JobStatus::accepted(101).unwrap();
        let supervisor = bare
            .with_supervisor(ProcessIdentity::new(11, 12).unwrap(), 150)
            .unwrap();
        let child = supervisor
            .with_child(ProcessIdentity::new(13, 14).unwrap(), 200)
            .unwrap();
        let initial = if observed_step == "direct_child" {
            supervisor.clone()
        } else {
            bare.clone()
        };
        let record = test_record(
            &store,
            60,
            100,
            Some(initial.clone()),
            RemoteUncertainty::unknown_remote("ENRICHMENT_RACE").unwrap(),
        );
        store.create_job(record.clone()).unwrap();
        let response = StatusResponse::new(record.meta().clone(), initial).unwrap();
        let expected = if observed_step == "supervisor" {
            supervisor.clone()
        } else {
            child.clone()
        };
        let hook_store = store.clone();
        let hook_supervisor = supervisor.clone();
        let hook_child = child.clone();
        let job_id = record.meta().job_id();
        let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
            if observed_step == "direct_child" {
                hook_store
                    .update_observation(job_id, hook_child.clone())
                    .unwrap();
            } else {
                hook_store
                    .update_observation(job_id, hook_supervisor.clone())
                    .unwrap();
                if observed_step == "transitive_child" {
                    hook_store
                        .update_observation(job_id, hook_child.clone())
                        .unwrap();
                }
            }
        });
        let remote = RemoteJobClient::new(&runner);
        let config = config();

        let report = service(&config, &store, &remote).inspect(None).unwrap();

        assert_eq!(
            report.jobs[0].status.as_ref(),
            Some(&expected),
            "{observed_step}"
        );
        assert_eq!(report.jobs[0].remote_uncertainty, RemoteUncertainty::None);
    }
}

#[test]
fn status_exact_surfaces_same_immutable_concurrent_observation_conflict() {
    // Catches exact mode silently returning a divergent terminal or identity
    // observation as a successful status lookup.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        65,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(record.clone()).unwrap();
    let response = StatusResponse::new(
        record.meta().clone(),
        JobStatus::succeeded(200, 1, 2).unwrap(),
    )
    .unwrap();
    let concurrent = JobStatus::failed(300, 7, 3, 4).unwrap();
    let hook_store = store.clone();
    let hook_status = concurrent.clone();
    let job_id = record.meta().job_id();
    let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
        hook_store
            .update_observation(job_id, hook_status.clone())
            .unwrap();
    });
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let error = service(&config, &store, &remote)
        .inspect(Some(job_id))
        .unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message.starts_with("LOCAL_STATUS_CONFLICT:"))
    );
    assert_eq!(
        store.load_job(job_id).unwrap().last_status(),
        Some(&concurrent)
    );
}

#[test]
fn status_list_missing_workers_do_not_consume_the_remote_refresh_budget() {
    // Catches counting eligible but uncallable rows against the 16 actual
    // bounded status calls available to later configured rows.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    for index in 0..17 {
        let record = record_with_meta_parts(
            JobId::new(uuid::Uuid::from_u128(1_000 + index)),
            store.client_id(),
            "missing-mini",
            &"a".repeat(64),
            &"b".repeat(64),
            &"c".repeat(64),
            "packages/app",
            30_000,
            "heavy",
            CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
            2_000 + index as u64,
            None,
            None,
            RemoteUncertainty::None,
        );
        store.create_job(record).unwrap();
    }
    let mut configured = Vec::new();
    for index in 0..16 {
        let record = test_record(
            &store,
            2_000 + index,
            1_000 + index as u64,
            None,
            RemoteUncertainty::None,
        );
        store.create_job(record.clone()).unwrap();
        configured.push(record);
    }
    configured.sort_by_key(|record| std::cmp::Reverse(record.meta().created_at_millis()));
    let results = configured
        .iter()
        .map(|record| {
            status_result(
                &StatusResponse::new(
                    record.meta().clone(),
                    JobStatus::accepted(record.meta().created_at_millis()).unwrap(),
                )
                .unwrap(),
            )
        })
        .collect();
    let runner = RecordingRunner::returning(results);
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let report = service(&config, &store, &remote).inspect(None).unwrap();

    let requests = runner.requests();
    assert_eq!(requests.len(), 16);
    for (request, record) in requests.iter().zip(&configured) {
        assert_status_call(request, Duration::from_secs(5), record.meta().job_id());
    }
    assert!(report.jobs[..17].iter().all(|row| row.status.is_none()));
    assert!(report.jobs[17..].iter().all(|row| row.status.is_some()));
}

#[test]
fn status_list_surfaces_each_authoritative_host_protocol_error_after_local_revalidation() {
    // Break caught: list mode classifies a canonical nonzero host error as
    // tolerable transport ambiguity and silently returns the persisted row.
    for (index, (code, message)) in [
        ("JOB_NOT_FOUND", "the exact remote job does not exist"),
        ("JOB_ID_CONFLICT", "the remote job identity conflicts"),
        (
            "REMOTE_PROTOCOL_FAILURE",
            "the host rejected the status request",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let record = test_record(
            &store,
            3_000 + index as u128,
            100,
            Some(JobStatus::accepted(101).unwrap()),
            RemoteUncertainty::None,
        );
        store.create_job(record.clone()).unwrap();
        let runner =
            RecordingRunner::returning(vec![authoritative_protocol_failure(code, message)]);
        let remote = RemoteJobClient::new(&runner);
        let config = config();

        let error = service(&config, &store, &remote).inspect(None).unwrap_err();

        assert!(
            matches!(error, WorkerError::Protocol(ref actual) if actual == &format!("{code}: {message}")),
            "{code}: {error}"
        );
        assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
        let requests = runner.requests();
        assert_eq!(requests.len(), 1, "{code}");
        assert_status_call(&requests[0], Duration::from_secs(5), record.meta().job_id());
    }

    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let original = test_record(
        &store,
        3_099,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(original.clone()).unwrap();
    let replacement = record_with_meta_parts(
        original.meta().job_id(),
        store.client_id(),
        "mini-1",
        &"d".repeat(64),
        &"b".repeat(64),
        &"c".repeat(64),
        "packages/app",
        30_000,
        "heavy",
        CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        100,
        None,
        original.last_status().cloned(),
        RemoteUncertainty::None,
    );
    let replacement_bytes = canonical_job_bytes(&replacement);
    let path = job_file(&temp, original.meta().job_id());
    let runner = RecordingRunner::with_hook(
        vec![authoritative_protocol_failure(
            "REMOTE_PROTOCOL_FAILURE",
            "the host rejected the status request",
        )],
        move |_| fs::write(&path, &replacement_bytes).unwrap(),
    );
    let remote = RemoteJobClient::new(&runner);
    let config = config();

    let error = service(&config, &store, &remote).inspect(None).unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message.starts_with("JOB_ID_CONFLICT:")),
        "{error}"
    );
    assert_eq!(
        store.load_job(original.meta().job_id()).unwrap(),
        replacement
    );
}

#[test]
fn status_exact_never_clears_or_accepts_a_divergent_record_in_the_old_two_write_window() {
    // Break caught: observation publication succeeds, a divergent same-binding
    // record lands during its cleanup pause, then the separate uncertainty
    // write clears that replacement and exact mode reports success.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let original = test_record(
        &store,
        3_100,
        100,
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::unknown_remote("ORIGINAL_AMBIGUITY").unwrap(),
    );
    store.create_job(original.clone()).unwrap();
    let remote_status = JobStatus::running(200, 11, 12, 13, 14).unwrap();
    let response = StatusResponse::new(original.meta().clone(), remote_status).unwrap();
    let concurrent = LocalJobRecord::new(
        original.meta().clone(),
        original.lease_token(),
        Some(JobStatus::failed(300, 7, 3, 4).unwrap()),
        RemoteUncertainty::cleanup_pending("CONCURRENT_CLEANUP").unwrap(),
    )
    .unwrap();
    let concurrent_bytes = canonical_job_bytes(&concurrent);
    let path = job_file(&temp, original.meta().job_id());
    let pause = store.pause_cleanup_after_move_once();
    let status_store = store.clone();
    let job_id = original.meta().job_id();
    let status_thread = thread::spawn(move || {
        let runner = RecordingRunner::returning(vec![status_result(&response)]);
        let retry = ImmediateResolution::new();
        let remote = RemoteJobClient::new_with_runtime(&runner, &retry);
        let config = config();
        let result = service(&config, &status_store, &remote).inspect(Some(job_id));
        (result, runner.requests())
    });
    pause.wait_until_paused();
    fs::write(path, concurrent_bytes).unwrap();
    pause.resume();

    let (result, requests) = status_thread.join().unwrap();
    let error = result.unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message.starts_with("LOCAL_STATUS_CONFLICT:")),
        "{error}"
    );
    assert_eq!(store.load_job(job_id).unwrap(), concurrent);
    assert_eq!(requests.len(), 1);
    assert_status_call(&requests[0], Duration::from_secs(30), job_id);
}

#[test]
fn status_transitive_accepted_enrichment_rejects_divergent_non_identity_fields() {
    // Break caught: the transitive Accepted shortcut checks only identities
    // and time, so two individually valid statuses with different error codes
    // are incorrectly treated as a valid two-hop enrichment.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let remote_bare = JobStatus::new(
        JobState::Accepted,
        101,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some("REMOTE_ACCEPTED_DETAIL".into()),
        None,
    )
    .unwrap();
    let local_enriched = JobStatus::new(
        JobState::Accepted,
        200,
        Some(11),
        Some(12),
        Some(13),
        Some(14),
        None,
        None,
        None,
        None,
        Some("DIVERGENT_LOCAL_DETAIL".into()),
        None,
    )
    .unwrap();
    let record = test_record(
        &store,
        3_200,
        100,
        Some(local_enriched),
        RemoteUncertainty::unknown_remote("IDENTITY_AMBIGUITY").unwrap(),
    );
    store.create_job(record.clone()).unwrap();
    let response = StatusResponse::new(record.meta().clone(), remote_bare).unwrap();
    let runner = RecordingRunner::returning(vec![status_result(&response)]);
    let retry = ImmediateResolution::new();
    let remote = RemoteJobClient::new_with_runtime(&runner, &retry);
    let config = config();

    let error = service(&config, &store, &remote)
        .inspect(Some(record.meta().job_id()))
        .unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message.starts_with("LOCAL_STATUS_CONFLICT:")),
        "{error}"
    );
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
    let requests = runner.requests();
    assert_eq!(requests.len(), 1);
    assert_status_call(
        &requests[0],
        Duration::from_secs(30),
        record.meta().job_id(),
    );
}

#[test]
fn status_exact_reconciles_a_current_at_least_remote_update_queued_before_the_cas() {
    // Break caught: an ordinary compatible observation update wins after the
    // pre-CAS reload, but the stale full-record CAS reports a conflict instead
    // of atomically preserving that observation and clearing uncertainty.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let bare = JobStatus::accepted(101).unwrap();
    let concurrent = bare
        .with_supervisor(ProcessIdentity::new(11, 12).unwrap(), 150)
        .unwrap();
    let target = test_record(
        &store,
        4_000,
        100,
        Some(bare.clone()),
        RemoteUncertainty::unknown_remote("PRE_CAS_AMBIGUITY").unwrap(),
    );
    store.create_job(target.clone()).unwrap();
    let blocker_status = JobStatus::succeeded(101, 0, 0).unwrap();
    let blocker = test_record(
        &store,
        4_001,
        50,
        Some(blocker_status.clone()),
        RemoteUncertainty::None,
    );
    store.create_job(blocker.clone()).unwrap();

    let blocker_pause = store.pause_cleanup_after_move_once();
    let blocker_store = store.clone();
    let blocker_id = blocker.meta().job_id();
    let blocker_update = blocker_status
        .with_cleanup_error("BLOCKER_CLEANUP".into(), 150)
        .unwrap();
    let blocker_thread =
        thread::spawn(move || blocker_store.update_observation(blocker_id, blocker_update));
    blocker_pause.wait_until_paused();

    let concurrent_pause = store.pause_cleanup_after_move_once();
    let update_contention = store.observe_next_lock_contention();
    let update_store = store.clone();
    let target_id = target.meta().job_id();
    let update_status = concurrent.clone();
    let update_thread =
        thread::spawn(move || update_store.update_observation(target_id, update_status));
    update_contention.wait_until_confirmed();

    let status_contention = store.observe_next_lock_contention();
    let status_store = store.clone();
    let response = StatusResponse::new(target.meta().clone(), bare).unwrap();
    let status_thread = thread::spawn(move || {
        let runner = RecordingRunner::returning(vec![status_result(&response)]);
        let retry = ImmediateResolution::new();
        let remote = RemoteJobClient::new_with_runtime(&runner, &retry);
        let config = config();
        let result = service(&config, &status_store, &remote).inspect(Some(target_id));
        (result, runner.requests())
    });
    status_contention.wait_until_confirmed();

    blocker_pause.resume();
    concurrent_pause.wait_until_paused();
    let queued = store.load_job(target_id).unwrap();
    assert_eq!(queued.last_status(), Some(&concurrent));
    assert!(matches!(
        queued.remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { code } if code == "PRE_CAS_AMBIGUITY"
    ));
    concurrent_pause.resume();
    blocker_thread.join().unwrap().unwrap();
    update_thread.join().unwrap().unwrap();

    let (report, requests) = status_thread.join().unwrap();
    let report = report.unwrap();

    assert_eq!(report.jobs[0].status.as_ref(), Some(&concurrent));
    assert_eq!(report.jobs[0].remote_uncertainty, RemoteUncertainty::None);
    let persisted = store.load_job(target_id).unwrap();
    assert_eq!(persisted.last_status(), Some(&concurrent));
    assert_eq!(persisted.remote_uncertainty(), &RemoteUncertainty::None);
    assert_eq!(requests.len(), 1);
    assert_status_call(&requests[0], Duration::from_secs(30), target_id);
}

#[test]
fn status_list_advances_a_compatible_intermediate_update_queued_before_the_cas() {
    // Break caught: a compatible intermediate observation wins after the
    // pre-CAS reload, but list mode returns it with stale uncertainty instead
    // of atomically advancing to the authoritative remote observation.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let bare = JobStatus::accepted(101).unwrap();
    let intermediate = bare
        .with_supervisor(ProcessIdentity::new(31, 32).unwrap(), 150)
        .unwrap();
    let remote_status = intermediate
        .with_child(ProcessIdentity::new(33, 34).unwrap(), 200)
        .unwrap();
    let target = test_record(
        &store,
        4_100,
        100,
        Some(bare),
        RemoteUncertainty::unknown_remote("PRE_CAS_AMBIGUITY").unwrap(),
    );
    store.create_job(target.clone()).unwrap();
    let blocker_status = JobStatus::succeeded(101, 0, 0).unwrap();
    let blocker = test_record(
        &store,
        4_101,
        50,
        Some(blocker_status.clone()),
        RemoteUncertainty::None,
    );
    store.create_job(blocker.clone()).unwrap();

    let target_id = target.meta().job_id();
    let response = StatusResponse::new(target.meta().clone(), remote_status.clone()).unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let status_store = store.clone();
    let status_thread = thread::spawn(move || {
        let release_receiver = Mutex::new(release_receiver);
        let runner = RecordingRunner::with_hook(vec![status_result(&response)], move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.lock().unwrap().recv().unwrap();
        });
        let remote = RemoteJobClient::new(&runner);
        let config = config();
        let result = service(&config, &status_store, &remote).inspect(None);
        (result, runner.requests())
    });
    entered_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("list status call did not reach the remote runner");

    let blocker_pause = store.pause_cleanup_after_move_once();
    let blocker_store = store.clone();
    let blocker_id = blocker.meta().job_id();
    let blocker_update = blocker_status
        .with_cleanup_error("BLOCKER_CLEANUP".into(), 150)
        .unwrap();
    let blocker_thread =
        thread::spawn(move || blocker_store.update_observation(blocker_id, blocker_update));
    blocker_pause.wait_until_paused();

    let concurrent_pause = store.pause_cleanup_after_move_once();
    let update_contention = store.observe_next_lock_contention();
    let update_store = store.clone();
    let update_status = intermediate.clone();
    let update_thread =
        thread::spawn(move || update_store.update_observation(target_id, update_status));
    update_contention.wait_until_confirmed();

    let status_contention = store.observe_next_lock_contention();
    release_sender.send(()).unwrap();
    status_contention.wait_until_confirmed();

    blocker_pause.resume();
    concurrent_pause.wait_until_paused();
    let queued = store.load_job(target_id).unwrap();
    assert_eq!(queued.last_status(), Some(&intermediate));
    assert!(matches!(
        queued.remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { code } if code == "PRE_CAS_AMBIGUITY"
    ));
    concurrent_pause.resume();
    blocker_thread.join().unwrap().unwrap();
    update_thread.join().unwrap().unwrap();

    let (report, requests) = status_thread.join().unwrap();
    let report = report.unwrap();

    let row = report
        .jobs
        .iter()
        .find(|row| row.job_id == target_id)
        .unwrap();
    assert_eq!(row.status.as_ref(), Some(&remote_status));
    assert_eq!(row.remote_uncertainty, RemoteUncertainty::None);
    let persisted = store.load_job(target_id).unwrap();
    assert_eq!(persisted.last_status(), Some(&remote_status));
    assert_eq!(persisted.remote_uncertainty(), &RemoteUncertainty::None);
    assert_eq!(requests.len(), 1);
    assert_status_call(&requests[0], Duration::from_secs(5), target_id);
}

const STOCK_RSYNC_STATS: &[u8] = b"Number of files: 2\nNumber of files transferred: 1\nTotal file size: 8 B\nTotal transferred file size: 8 B\nUnmatched data: 8 B\nMatched data: 0 B\nFile list size: 64 B\nTotal sent: 128 B\nTotal received: 32 B\n\nsent 128 bytes  received 32 bytes  1000 bytes/sec\ntotal size is 8  speedup is 0.05\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunScriptStep {
    ProbeReady,
    ProbeBusy,
    ProbeMiniOneReadyElseBusy,
    ReconcileOrBusy,
    ProbeUnavailable,
    Acquire,
    AcquireMismatch(LeaseMismatch),
    AcquireExisting,
    Upload,
    Verify,
    VerifyMismatch(VerificationMismatch),
    SubmitAccepted,
    SubmitAcceptedMismatch,
    SubmitExisting,
    CancelPrelaunch,
    StatusStoredRecord,
    CancelStoredRecord,
    TransportFailure(HostOperation),
    AuthoritativeFailure(HostOperation, &'static str),
    UploadFailure,
    StatusAccepted,
    StatusTerminal {
        exit_code: u8,
        stdout_bytes: u64,
        stderr_bytes: u64,
    },
    StatusMismatch,
    StatusTransportFailure,
    LogChunkBytes {
        stream: LogStream,
        offset: u64,
        bytes: &'static [u8],
    },
    ResolveAccepted,
    ResolveAbandoned,
    ResolveCleanupPending,
    ResolveTransportFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseMismatch {
    JobId,
    ClientId,
    LeaseToken,
    RequestFingerprint,
    WorkerName,
    ProjectId,
    WorktreeId,
    ManifestDigest,
    TimeoutMillis,
    ResourceClass,
    CommandSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VerificationMismatch {
    JobId,
    ClientId,
    ProjectId,
    WorktreeId,
    ManifestDigest,
}

#[derive(Default)]
struct RunScriptState {
    steps: VecDeque<RunScriptStep>,
    requests: Vec<ProcessRequest>,
    events: Vec<String>,
    material: Option<RequestFingerprintMaterial>,
    fail_index_query: bool,
}

struct RunScriptRunner {
    state: Mutex<RunScriptState>,
    state_root: PathBuf,
}

impl RunScriptRunner {
    fn new(state_root: PathBuf, steps: impl IntoIterator<Item = RunScriptStep>) -> Self {
        Self {
            state: Mutex::new(RunScriptState {
                steps: steps.into_iter().collect(),
                ..RunScriptState::default()
            }),
            state_root,
        }
    }

    fn with_index_query_failure(
        state_root: PathBuf,
        steps: impl IntoIterator<Item = RunScriptStep>,
    ) -> Self {
        Self {
            state: Mutex::new(RunScriptState {
                steps: steps.into_iter().collect(),
                fail_index_query: true,
                ..RunScriptState::default()
            }),
            state_root,
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.state.lock().unwrap().requests.clone()
    }

    fn events(&self) -> Vec<String> {
        self.state.lock().unwrap().events.clone()
    }

    fn remaining_steps(&self) -> Vec<RunScriptStep> {
        self.state.lock().unwrap().steps.iter().copied().collect()
    }

    fn assert_consumed(&self) {
        assert!(
            self.state.lock().unwrap().steps.is_empty(),
            "unused scripted run steps remain"
        );
    }

    fn material(&self) -> RequestFingerprintMaterial {
        self.state
            .lock()
            .unwrap()
            .material
            .clone()
            .expect("acquire material was recorded")
    }
}

fn successful_process(bytes: Vec<u8>) -> Result<ProcessResult, WorkerError> {
    Ok(ProcessResult {
        status: ExitStatus::from_raw(0),
        stdout: bytes,
        stderr: Vec::new(),
    })
}

fn canonical_process(value: &impl serde::Serialize) -> Result<ProcessResult, WorkerError> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    successful_process(bytes)
}

fn ready_probe() -> ProbeResponse {
    ProbeResponse {
        protocol_version: PROTOCOL_VERSION,
        supervision_version: SUPERVISION_VERSION,
        hostname: "mini-1.local".into(),
        arch: "arm64".into(),
        os_version: "26.2".into(),
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
        available_memory_bytes: Some(12 * 1024 * 1024 * 1024),
        cpu_counters: Some(mac_worker::protocol::CpuCounters {
            user_ticks: 10,
            system_ticks: 20,
            idle_ticks: 30,
            nice_ticks: 40,
        }),
        slot_state: mac_worker::lease::SlotState::Idle,
        active_lease: None,
        capabilities: vec!["declared-capability".into(), "project-capability".into()],
    }
}

fn material_status(material: &RequestFingerprintMaterial, status: JobStatus) -> StatusResponse {
    StatusResponse::new(
        JobMeta::new(material, material.fingerprint()).unwrap(),
        status,
    )
    .unwrap()
}

fn mismatched_lease(material: &RequestFingerprintMaterial, mismatch: LeaseMismatch) -> LeaseRecord {
    let lease = LeaseRecord::new(
        material,
        material.fingerprint(),
        material.created_at_millis(),
        material.created_at_millis() + material.timeout_millis(),
    )
    .unwrap();
    let mut value = serde_json::to_value(lease).unwrap();
    match mismatch {
        LeaseMismatch::JobId => value["job_id"] = Value::String(JOB_ID.into()),
        LeaseMismatch::ClientId => value["client_id"] = Value::String(CLIENT_ID.into()),
        LeaseMismatch::LeaseToken => value["lease_token"] = Value::String(LEASE_TOKEN.into()),
        LeaseMismatch::RequestFingerprint => {
            value["request_fingerprint"] = Value::String("d".repeat(64));
        }
        LeaseMismatch::WorkerName => value["worker_name"] = Value::String("other-worker".into()),
        LeaseMismatch::ProjectId => value["project_id"] = Value::String("d".repeat(64)),
        LeaseMismatch::WorktreeId => value["worktree_id"] = Value::String("d".repeat(64)),
        LeaseMismatch::ManifestDigest => {
            value["manifest_digest"] = Value::String("d".repeat(64));
        }
        LeaseMismatch::TimeoutMillis => {
            value["timeout_millis"] = Value::from(material.timeout_millis() + 1);
        }
        LeaseMismatch::ResourceClass => {
            value["resource_class"] = Value::String("other-class".into());
        }
        LeaseMismatch::CommandSummary => {
            value["command_summary"] = serde_json::json!({"mode": "shell"});
        }
    }
    serde_json::from_value(value).unwrap()
}

fn mismatched_verification(
    material: &RequestFingerprintMaterial,
    mismatch: VerificationMismatch,
) -> VerifiedSnapshotResponse {
    let job_id = if mismatch == VerificationMismatch::JobId {
        JOB_ID.parse().unwrap()
    } else {
        material.job_id()
    };
    let client_id = if mismatch == VerificationMismatch::ClientId {
        CLIENT_ID.parse().unwrap()
    } else {
        material.client_id()
    };
    let project_id = if mismatch == VerificationMismatch::ProjectId {
        "d".repeat(64)
    } else {
        material.project_id().into()
    };
    let worktree_id = if mismatch == VerificationMismatch::WorktreeId {
        "d".repeat(64)
    } else {
        material.worktree_id().into()
    };
    let manifest_digest = if mismatch == VerificationMismatch::ManifestDigest {
        "d".repeat(64)
    } else {
        material.manifest_digest().into()
    };
    VerifiedSnapshotResponse::new(
        job_id,
        client_id,
        project_id,
        worktree_id,
        manifest_digest,
        material.created_at_millis() + 1,
        false,
    )
    .unwrap()
}

fn mismatched_status(material: &RequestFingerprintMaterial, status: JobStatus) -> StatusResponse {
    let response = material_status(material, status);
    let mut value = serde_json::to_value(response).unwrap();
    value["meta"]["client_id"] = Value::String(CLIENT_ID.into());
    serde_json::from_value(value).unwrap()
}

fn assert_resolution_matches(
    request: &ResolveOrAbandonRequest,
    material: &RequestFingerprintMaterial,
) {
    assert_eq!(request.job_id(), material.job_id());
    assert_eq!(request.client_id(), material.client_id());
    assert_eq!(request.lease_token(), material.lease_token());
    assert_eq!(request.created_at_millis(), material.created_at_millis());
    assert_eq!(request.request_fingerprint(), &material.fingerprint());
    assert_eq!(request.worker_name(), material.worker_name());
    assert_eq!(request.project_id(), material.project_id());
    assert_eq!(request.worktree_id(), material.worktree_id());
    assert_eq!(request.manifest_digest(), material.manifest_digest());
    assert_eq!(
        request.relative_working_dir(),
        material.relative_working_dir()
    );
    assert_eq!(request.timeout_millis(), material.timeout_millis());
    assert_eq!(request.resource_class(), material.resource_class());
}

impl ProcessRunner for RunScriptRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.program == OsStr::new("/usr/bin/git") {
            let mut state = self.state.lock().unwrap();
            state.events.push("git".into());
            if state.fail_index_query
                && request.args.iter().any(|argument| argument == "ls-files")
                && request.args.iter().any(|argument| argument == "--stage")
            {
                state.fail_index_query = false;
                return Err(WorkerError::Io(io::Error::other(
                    "planted Git input query failure",
                )));
            }
            drop(state);
            return SystemProcessRunner.run(request);
        }

        let mut state = self.state.lock().unwrap();
        state.requests.push(request.clone());
        let step = state
            .steps
            .pop_front()
            .expect("an unexpected post-failure stage was reached");
        state.events.push(format!("{step:?}"));

        match step {
            RunScriptStep::ProbeReady => {
                assert_eq!(
                    request.args.last().unwrap(),
                    "~/.local/bin/worker host probe"
                );
                canonical_process(&ready_probe())
            }
            RunScriptStep::ProbeBusy => {
                assert_eq!(
                    request.args.last().unwrap(),
                    "~/.local/bin/worker host probe"
                );
                let mut probe = ready_probe();
                probe.slot_state = SlotState::Busy;
                probe.active_lease = Some(LeaseSummary {
                    job_id: JOB_ID.parse().unwrap(),
                    project_id: "a".repeat(64),
                    worktree_id: "b".repeat(64),
                    created_at_millis: 10,
                });
                canonical_process(&probe)
            }
            RunScriptStep::ProbeMiniOneReadyElseBusy => {
                assert_eq!(
                    request.args.last().unwrap(),
                    "~/.local/bin/worker host probe"
                );
                let mut probe = ready_probe();
                if request.args.iter().any(|argument| argument == "mac2") {
                    probe.slot_state = SlotState::Busy;
                    probe.active_lease = Some(LeaseSummary {
                        job_id: JOB_ID.parse().unwrap(),
                        project_id: "a".repeat(64),
                        worktree_id: "b".repeat(64),
                        created_at_millis: 10,
                    });
                }
                canonical_process(&probe)
            }
            RunScriptStep::ReconcileOrBusy => {
                if request.args.last().unwrap() == HostOperation::Reconcile.command() {
                    let reconcile: FleetReconcileRequest =
                        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                    let store = ClientStateStore::open(&self.state_root).unwrap();
                    let results = reconcile
                        .known_job_ids()
                        .iter()
                        .map(|job_id| FleetReconcileJobResult::Status {
                            status: Box::new(
                                StatusResponse::new(
                                    store.load_job(*job_id).unwrap().meta().clone(),
                                    JobStatus::accepted(1_001).unwrap(),
                                )
                                .unwrap(),
                            ),
                        })
                        .collect();
                    state
                        .steps
                        .push_front(RunScriptStep::ProbeMiniOneReadyElseBusy);
                    canonical_process(&FleetReconcileResponse::new(results).unwrap())
                } else {
                    assert_eq!(
                        request.args.last().unwrap(),
                        "~/.local/bin/worker host probe"
                    );
                    let mut probe = ready_probe();
                    if request.args.iter().any(|argument| argument == "mac1") {
                        state.steps.push_front(RunScriptStep::ReconcileOrBusy);
                    } else {
                        probe.slot_state = SlotState::Busy;
                        probe.active_lease = Some(LeaseSummary {
                            job_id: JOB_ID.parse().unwrap(),
                            project_id: "a".repeat(64),
                            worktree_id: "b".repeat(64),
                            created_at_millis: 10,
                        });
                    }
                    canonical_process(&probe)
                }
            }
            RunScriptStep::ProbeUnavailable => {
                assert_eq!(
                    request.args.last().unwrap(),
                    "~/.local/bin/worker host probe"
                );
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(255 << 8),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
            RunScriptStep::Acquire
            | RunScriptStep::AcquireMismatch(_)
            | RunScriptStep::AcquireExisting => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::LeaseAcquire.command()
                );
                let acquire: LeaseAcquireRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = acquire.material().clone();
                assert!(
                    self.state_root
                        .join("jobs")
                        .join(format!("{}.json", material.job_id()))
                        .is_file(),
                    "the durable local identity must precede lease acquisition"
                );
                let persisted = ClientStateStore::open(&self.state_root)
                    .unwrap()
                    .load_job(material.job_id())
                    .unwrap();
                assert_eq!(persisted.meta().job_id(), material.job_id());
                assert_eq!(persisted.lease_token(), material.lease_token());
                assert_eq!(
                    persisted.meta().created_at_millis(),
                    material.created_at_millis()
                );
                let queue = ClientStateStore::open(&self.state_root)
                    .unwrap()
                    .queue_snapshot()
                    .unwrap();
                let queued = queue
                    .entries()
                    .iter()
                    .find(|entry| entry.job_id() == material.job_id())
                    .expect("the original queue row must remain reserved during lease acquisition");
                assert!(matches!(
                    queued.state(),
                    QueueState::Dispatching { selected_worker, .. }
                        if selected_worker == material.worker_name()
                ));
                state.material = Some(material.clone());
                if step == RunScriptStep::AcquireExisting {
                    canonical_process(&LeaseAcquireResponse::ExistingAccepted {
                        status: JobStatus::accepted(material.created_at_millis()).unwrap(),
                    })
                } else if let RunScriptStep::AcquireMismatch(mismatch) = step {
                    canonical_process(&LeaseAcquireResponse::Acquired {
                        lease: mismatched_lease(&material, mismatch),
                    })
                } else {
                    let lease = LeaseRecord::new(
                        &material,
                        material.fingerprint(),
                        material.created_at_millis(),
                        material.created_at_millis() + material.timeout_millis(),
                    )
                    .unwrap();
                    canonical_process(&LeaseAcquireResponse::Acquired { lease })
                }
            }
            RunScriptStep::Upload | RunScriptStep::UploadFailure => {
                assert_eq!(request.program, OsStr::new("/usr/bin/rsync"));
                let material = state.material.as_ref().unwrap();
                let args = request
                    .args
                    .iter()
                    .map(|value| value.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(" ");
                for identity in [
                    material.job_id().to_string(),
                    material.client_id().to_string(),
                    material.lease_token().to_string(),
                    material.fingerprint().to_string(),
                ] {
                    assert!(args.contains(&identity));
                }
                if step == RunScriptStep::UploadFailure {
                    Ok(ProcessResult {
                        status: ExitStatus::from_raw(23 << 8),
                        stdout: Vec::new(),
                        stderr: b"content-free rsync failure".to_vec(),
                    })
                } else {
                    successful_process(STOCK_RSYNC_STATS.to_vec())
                }
            }
            RunScriptStep::Verify | RunScriptStep::VerifyMismatch(_) => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::SnapshotVerify.command()
                );
                let verify: SnapshotVerifyRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = state.material.as_ref().unwrap();
                assert_eq!(verify.job_id(), material.job_id());
                assert_eq!(verify.client_id(), material.client_id());
                assert_eq!(verify.lease_token(), material.lease_token());
                assert_eq!(verify.request_fingerprint(), &material.fingerprint());
                assert_eq!(verify.project_id(), material.project_id());
                assert_eq!(verify.worktree_id(), material.worktree_id());
                assert_eq!(verify.manifest_digest(), material.manifest_digest());
                if let RunScriptStep::VerifyMismatch(mismatch) = step {
                    canonical_process(&mismatched_verification(material, mismatch))
                } else {
                    canonical_process(
                        &VerifiedSnapshotResponse::new(
                            verify.job_id(),
                            verify.client_id(),
                            verify.project_id().into(),
                            verify.worktree_id().into(),
                            verify.manifest_digest().into(),
                            material.created_at_millis() + 1,
                            false,
                        )
                        .unwrap(),
                    )
                }
            }
            RunScriptStep::SubmitAccepted
            | RunScriptStep::SubmitAcceptedMismatch
            | RunScriptStep::SubmitExisting => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::Submit.command()
                );
                let submit: SubmitRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = state.material.as_ref().unwrap();
                assert_eq!(submit.material(), material);
                assert_eq!(submit.request_fingerprint(), &material.fingerprint());
                if step == RunScriptStep::SubmitExisting {
                    canonical_process(&SubmitResponse::Existing {
                        status: JobStatus::accepted(material.created_at_millis()).unwrap(),
                    })
                } else {
                    let meta = if step == RunScriptStep::SubmitAcceptedMismatch {
                        mismatched_status(
                            material,
                            JobStatus::accepted(material.created_at_millis()).unwrap(),
                        )
                        .meta()
                        .clone()
                    } else {
                        JobMeta::new(material, material.fingerprint()).unwrap()
                    };
                    canonical_process(&SubmitResponse::Accepted {
                        meta: Box::new(meta),
                        status: JobStatus::accepted(material.created_at_millis()).unwrap(),
                    })
                }
            }
            RunScriptStep::CancelPrelaunch => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::Cancel.command()
                );
                let cancel: CancelRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = state.material.as_ref().unwrap();
                assert_eq!(cancel.job_id(), material.job_id());
                assert_eq!(cancel.client_id(), material.client_id());
                assert_eq!(cancel.lease_token(), material.lease_token());
                assert_eq!(cancel.request_fingerprint(), &material.fingerprint());
                let cancelled = JobStatus::accepted(material.created_at_millis())
                    .unwrap()
                    .into_prelaunch_cancelled(material.created_at_millis() + 1, 0, 0)
                    .unwrap();
                canonical_process(
                    &CancelResponse::new(material_status(material, cancelled)).unwrap(),
                )
            }
            RunScriptStep::StatusStoredRecord => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::Status.command()
                );
                let status: StatusRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let record = ClientStateStore::open(&self.state_root)
                    .unwrap()
                    .load_job(status.job_id())
                    .unwrap();
                let current = record
                    .last_status()
                    .cloned()
                    .unwrap_or(JobStatus::accepted(record.meta().created_at_millis()).unwrap());
                canonical_process(&StatusResponse::new(record.meta().clone(), current).unwrap())
            }
            RunScriptStep::CancelStoredRecord => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::Cancel.command()
                );
                let cancel: CancelRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let record = ClientStateStore::open(&self.state_root)
                    .unwrap()
                    .load_job(cancel.job_id())
                    .unwrap();
                assert_eq!(cancel.client_id(), record.meta().client_id());
                assert_eq!(
                    cancel.request_fingerprint(),
                    record.meta().request_fingerprint()
                );
                let cancelled = JobStatus::accepted(record.meta().created_at_millis())
                    .unwrap()
                    .into_prelaunch_cancelled(record.meta().created_at_millis() + 1, 0, 0)
                    .unwrap();
                canonical_process(
                    &CancelResponse::new(
                        StatusResponse::new(record.meta().clone(), cancelled).unwrap(),
                    )
                    .unwrap(),
                )
            }
            RunScriptStep::TransportFailure(operation) => {
                assert_eq!(request.args.last().unwrap(), operation.command());
                if operation == HostOperation::LeaseAcquire {
                    let acquire: LeaseAcquireRequest =
                        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                    state.material = Some(acquire.material().clone());
                } else if operation == HostOperation::SnapshotVerify {
                    let verify: SnapshotVerifyRequest =
                        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                    assert_eq!(verify.job_id(), state.material.as_ref().unwrap().job_id());
                } else if operation == HostOperation::Submit {
                    let submit: SubmitRequest =
                        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                    assert_eq!(submit.material(), state.material.as_ref().unwrap());
                }
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(255 << 8),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
            RunScriptStep::AuthoritativeFailure(operation, code) => {
                assert_eq!(request.args.last().unwrap(), operation.command());
                if operation == HostOperation::LeaseAcquire {
                    let acquire: LeaseAcquireRequest =
                        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                    state.material = Some(acquire.material().clone());
                } else if operation == HostOperation::Submit {
                    let submit: SubmitRequest =
                        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                    assert_eq!(submit.material(), state.material.as_ref().unwrap());
                }
                let mut stdout = serde_json::to_vec(
                    &HostControlError::new(code, "authoritative admission failure").unwrap(),
                )
                .unwrap();
                stdout.push(b'\n');
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(23 << 8),
                    stdout,
                    stderr: Vec::new(),
                })
            }
            RunScriptStep::StatusAccepted
            | RunScriptStep::StatusTerminal { .. }
            | RunScriptStep::StatusMismatch
            | RunScriptStep::StatusTransportFailure
            | RunScriptStep::ResolveAccepted => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::Status.command()
                );
                let status: StatusRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = state.material.as_ref().unwrap();
                assert_eq!(status.job_id(), material.job_id());
                if step == RunScriptStep::StatusTransportFailure {
                    Ok(ProcessResult {
                        status: ExitStatus::from_raw(255 << 8),
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    })
                } else if step == RunScriptStep::StatusMismatch {
                    canonical_process(&mismatched_status(
                        material,
                        JobStatus::accepted(material.created_at_millis()).unwrap(),
                    ))
                } else if let RunScriptStep::StatusTerminal {
                    exit_code,
                    stdout_bytes,
                    stderr_bytes,
                } = step
                {
                    let updated = material.created_at_millis() + 2;
                    let status = if exit_code == 0 {
                        JobStatus::succeeded(updated, stdout_bytes, stderr_bytes).unwrap()
                    } else {
                        JobStatus::failed(updated, exit_code, stdout_bytes, stderr_bytes).unwrap()
                    };
                    canonical_process(&material_status(material, status))
                } else {
                    canonical_process(&material_status(
                        material,
                        JobStatus::accepted(material.created_at_millis()).unwrap(),
                    ))
                }
            }
            RunScriptStep::LogChunkBytes {
                stream,
                offset,
                bytes,
            } => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::LogChunk.command()
                );
                let chunk: LogChunkRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = state.material.as_ref().unwrap();
                assert_eq!(chunk.job_id(), material.job_id());
                assert_eq!(chunk.stream(), stream);
                assert_eq!(chunk.offset(), offset);
                log_chunk_result(stream, offset, bytes)
            }
            RunScriptStep::ResolveAbandoned
            | RunScriptStep::ResolveCleanupPending
            | RunScriptStep::ResolveTransportFailure => {
                assert_eq!(
                    request.args.last().unwrap(),
                    HostOperation::ResolveOrAbandon.command()
                );
                let resolution: ResolveOrAbandonRequest =
                    serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
                let material = state.material.as_ref().unwrap();
                assert_resolution_matches(&resolution, material);
                match step {
                    RunScriptStep::ResolveAbandoned => {
                        canonical_process(&ResolveOrAbandonResponse::abandoned())
                    }
                    RunScriptStep::ResolveCleanupPending => canonical_process(
                        &ResolveOrAbandonResponse::cleanup_pending("CLEANUP_STILL_PENDING")
                            .unwrap(),
                    ),
                    RunScriptStep::ResolveTransportFailure => Ok(ProcessResult {
                        status: ExitStatus::from_raw(255 << 8),
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    }),
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[derive(Default)]
struct OutputState {
    bytes: Vec<u8>,
    flushes: usize,
}

struct RecordingWriter {
    state: Arc<Mutex<OutputState>>,
    fail_write: bool,
    fail_flush: bool,
}

impl RecordingWriter {
    fn new(state: Arc<Mutex<OutputState>>) -> Self {
        Self {
            state,
            fail_write: false,
            fail_flush: false,
        }
    }

    fn failing(state: Arc<Mutex<OutputState>>) -> Self {
        Self {
            state,
            fail_write: true,
            fail_flush: false,
        }
    }

    fn failing_flush(state: Arc<Mutex<OutputState>>) -> Self {
        Self {
            state,
            fail_write: false,
            fail_flush: true,
        }
    }
}

impl Write for RecordingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_write {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "planted writer failure",
            ));
        }
        self.state.lock().unwrap().bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "planted flush failure",
            ));
        }
        self.state.lock().unwrap().flushes += 1;
        Ok(())
    }
}

struct RecordingFollower {
    calls: AtomicUsize,
    output: Option<Arc<Mutex<OutputState>>>,
    outcome: FollowerOutcome,
    cleanup_error: bool,
    sabotage_cleanup_under: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowerOutcome {
    Exit(u8),
    Cancelled,
    Signal(u32),
    Infrastructure,
    Nonterminal,
    MismatchedMeta,
    Error(&'static str),
}

impl RecordingFollower {
    fn succeeding(exit_code: u8) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            output: None,
            outcome: FollowerOutcome::Exit(exit_code),
            cleanup_error: false,
            sabotage_cleanup_under: None,
        }
    }

    fn requiring_flushed(output: Arc<Mutex<OutputState>>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            output: Some(output),
            outcome: FollowerOutcome::Exit(0),
            cleanup_error: false,
            sabotage_cleanup_under: None,
        }
    }

    fn failing() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            output: None,
            outcome: FollowerOutcome::Error("planted follower failure"),
            cleanup_error: false,
            sabotage_cleanup_under: None,
        }
    }

    fn terminal(outcome: FollowerOutcome, cleanup_error: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            output: None,
            outcome,
            cleanup_error,
            sabotage_cleanup_under: None,
        }
    }

    fn failing_with_cleanup_sabotage(cache: PathBuf) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            output: None,
            outcome: FollowerOutcome::Error("primary follower failure"),
            cleanup_error: false,
            sabotage_cleanup_under: Some(cache),
        }
    }
}

fn find_snapshot_publication(directory: &Path) -> Option<PathBuf> {
    for entry in fs::read_dir(directory).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if path.join("manifest.json").is_file() {
                return Some(path);
            }
            if let Some(found) = find_snapshot_publication(&path) {
                return Some(found);
            }
        }
    }
    None
}

impl JobFollower for RecordingFollower {
    fn follow(
        &self,
        record: &LocalJobRecord,
        _json: bool,
        _stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(output) = &self.output {
            let output = output.lock().unwrap();
            assert!(output.flushes > 0, "accepted output was not flushed");
            assert!(!output.bytes.is_empty(), "accepted output was not written");
        }
        if let Some(cache) = &self.sabotage_cleanup_under {
            let publication = find_snapshot_publication(&cache.join("snapshots/ready"))
                .expect("prepared snapshot publication must exist while following");
            let moved = publication.with_extension("moved-by-test");
            fs::rename(&publication, &moved).unwrap();
            std::os::unix::fs::symlink(&moved, &publication).unwrap();
        }
        let updated = record.meta().created_at_millis() + 2;
        let status = match self.outcome {
            FollowerOutcome::Exit(0) => JobStatus::succeeded(updated, 0, 0).unwrap(),
            FollowerOutcome::Exit(code) => JobStatus::failed(updated, code, 0, 0).unwrap(),
            FollowerOutcome::Cancelled => JobStatus::accepted(record.meta().created_at_millis())
                .unwrap()
                .into_prelaunch_cancelled(record.meta().created_at_millis() + 1, 0, 0)
                .unwrap(),
            FollowerOutcome::Signal(signal) => JobStatus::new(
                JobState::Failed,
                updated,
                None,
                None,
                None,
                None,
                None,
                Some(signal),
                Some(0),
                Some(0),
                None,
                None,
            )
            .unwrap(),
            FollowerOutcome::Infrastructure => JobStatus::new(
                JobState::TimedOut,
                updated,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(0),
                Some(0),
                Some("REMOTE_TIMEOUT".into()),
                None,
            )
            .unwrap(),
            FollowerOutcome::Nonterminal | FollowerOutcome::MismatchedMeta => {
                JobStatus::accepted(updated).unwrap()
            }
            FollowerOutcome::Error(message) => {
                return Err(WorkerError::Transport {
                    code: "FOLLOW_FAILED",
                    message: message.into(),
                });
            }
        };
        let status = if self.cleanup_error {
            status
                .with_cleanup_error("REMOTE_CLEANUP_FAILED".into(), updated + 1)
                .unwrap()
        } else {
            status
        };
        let meta = if self.outcome == FollowerOutcome::MismatchedMeta {
            let mut meta = serde_json::to_value(record.meta()).unwrap();
            meta["worker_name"] = Value::String("different-worker".into());
            serde_json::from_value(meta).unwrap()
        } else {
            record.meta().clone()
        };
        StatusResponse::new(meta, status)
    }
}

struct ConcurrentTerminalFollower {
    store: ClientStateStore,
}

struct FinalPersistenceFailureFollower {
    store: ClientStateStore,
}

impl JobFollower for FinalPersistenceFailureFollower {
    fn follow(
        &self,
        record: &LocalJobRecord,
        _json: bool,
        _stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError> {
        self.store
            .inject_write_failure_once(ClientStateWritePoint::BeforePublish);
        StatusResponse::new(
            record.meta().clone(),
            JobStatus::succeeded(record.meta().created_at_millis() + 2, 0, 0).unwrap(),
        )
    }
}

struct RecordingRunObserver {
    stages: Mutex<Vec<RunStage>>,
    fail_at: Option<RunStage>,
}

struct CancelAtClaimObserver {
    store: ClientStateStore,
    requests: AtomicUsize,
}

struct CancelAtSubmissionObserver {
    store: ClientStateStore,
    requests: AtomicUsize,
}

struct CancelAtAcceptedOutputObserver<'a> {
    store: ClientStateStore,
    runner: &'a RunScriptRunner,
    config: &'a Config,
    requests: AtomicUsize,
}

#[derive(Default)]
struct CountingCancelObserver(AtomicUsize);

struct BlockingDispatchObserver {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

struct BlockingCancelObserver {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    calls: AtomicUsize,
}

impl CancelObserver for CountingCancelObserver {
    fn observe(&self, stage: CancelStage) -> Result<(), WorkerError> {
        assert_eq!(stage, CancelStage::RemoteCancelHandoff);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl CancelObserver for BlockingCancelObserver {
    fn observe(&self, stage: CancelStage) -> Result<(), WorkerError> {
        assert_eq!(stage, CancelStage::RemoteCancelHandoff);
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send(()).map_err(|_| {
            WorkerError::Protocol("cancel handoff gate receiver disappeared".into())
        })?;
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| WorkerError::Protocol("cancel handoff gate timed out".into()))?;
        Ok(())
    }
}

impl RunObserver for BlockingDispatchObserver {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage == RunStage::DispatchToLeaseHandoff {
            self.entered.send(()).map_err(|_| {
                WorkerError::Protocol("dispatch handoff gate receiver disappeared".into())
            })?;
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .map_err(|_| WorkerError::Protocol("dispatch handoff gate timed out".into()))?;
        }
        Ok(())
    }
}

impl RecordingRunObserver {
    fn recording() -> Self {
        Self {
            stages: Mutex::new(Vec::new()),
            fail_at: None,
        }
    }

    fn failing_at(stage: RunStage) -> Self {
        Self {
            stages: Mutex::new(Vec::new()),
            fail_at: Some(stage),
        }
    }

    fn stages(&self) -> Vec<RunStage> {
        self.stages.lock().unwrap().clone()
    }

    fn cleanup_count(&self) -> usize {
        self.stages()
            .into_iter()
            .filter(|stage| *stage == RunStage::SnapshotCleanup)
            .count()
    }
}

struct PanicAfterLocalRecordPublication;

impl RunObserver for PanicAfterLocalRecordPublication {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage == RunStage::LocalRecordPublication {
            panic!("injected crash after local-record publication");
        }
        Ok(())
    }
}

impl RunObserver for RecordingRunObserver {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        self.stages.lock().unwrap().push(stage);
        if self.fail_at == Some(stage) {
            Err(WorkerError::Protocol(format!(
                "INJECTED_RUN_STAGE_FAILURE: {stage:?}"
            )))
        } else {
            Ok(())
        }
    }
}

impl RunObserver for CancelAtClaimObserver {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage != RunStage::QueueClaim {
            return Ok(());
        }
        let job_id = self
            .store
            .queue_snapshot()?
            .entries()
            .first()
            .ok_or_else(|| WorkerError::Queue {
                code: "QUEUE_NOT_FOUND",
                message: "claim observer found no durable queue row".into(),
            })?
            .job_id();
        self.store.request_queue_cancel(job_id, 90_201)?;
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl RunObserver for CancelAtSubmissionObserver {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage != RunStage::JobSubmission {
            return Ok(());
        }
        let entry = self
            .store
            .queue_snapshot()?
            .entries()
            .first()
            .cloned()
            .ok_or_else(|| WorkerError::Queue {
                code: "QUEUE_NOT_FOUND",
                message: "submission observer found no durable queue row".into(),
            })?;
        self.store
            .request_queue_cancel(entry.job_id(), entry.enqueued_at_millis())?;
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl RunObserver for CancelAtAcceptedOutputObserver<'_> {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage != RunStage::AcceptedOutputFlush {
            return Ok(());
        }
        let job_id = self
            .store
            .queue_snapshot()?
            .entries()
            .first()
            .ok_or_else(|| WorkerError::Queue {
                code: "QUEUE_NOT_FOUND",
                message: "accepted-output observer found no durable queue row".into(),
            })?
            .job_id();
        let report = CancelService::new(self.runner, self.config, &self.store).cancel(job_id)?;
        if !matches!(report, CancelReport::RemoteCancelled { .. }) {
            return Err(WorkerError::Protocol(
                "CANCEL_RESPONSE_INVALID: accepted-output cancellation was not remote".into(),
            ));
        }
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CorruptAffinityAfterEnqueue {
    path: PathBuf,
}

impl RunObserver for CorruptAffinityAfterEnqueue {
    fn observe(&self, stage: RunStage) -> Result<(), WorkerError> {
        if stage == RunStage::QueueEnqueue {
            fs::write(&self.path, b"{")?;
        }
        Ok(())
    }
}

impl JobFollower for ConcurrentTerminalFollower {
    fn follow(
        &self,
        record: &LocalJobRecord,
        _json: bool,
        _stdout: &mut dyn Write,
        _stderr: &mut dyn Write,
    ) -> Result<StatusResponse, WorkerError> {
        let updated = record.meta().created_at_millis() + 2;
        let follower_status = JobStatus::failed(updated, 7, 0, 0).unwrap();
        let concurrent = follower_status
            .clone()
            .with_cleanup_error("CONCURRENT_CLEANUP".into(), updated + 1)
            .unwrap();
        self.store
            .update_observation(record.meta().job_id(), concurrent)
            .unwrap();
        StatusResponse::new(record.meta().clone(), follower_status)
    }
}

fn run_config() -> Config {
    Config {
        version: 1,
        workers: vec![
            WorkerEntry {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                slots: 1,
                capabilities: vec!["declared-capability".into()],
                remote_binary: "~/.local/bin/worker".into(),
            },
            WorkerEntry {
                name: "poison-worker".into(),
                ssh: "poison-host".into(),
                slots: 1,
                capabilities: Vec::new(),
                remote_binary: "~/.local/bin/worker".into(),
            },
        ],
    }
}

fn run_paths(temp: &tempfile::TempDir) -> PathLayout {
    let root = temp.path().canonicalize().unwrap();
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

fn run_repo(settings: &[u8]) -> GitRepo {
    let repo = GitRepo::init();
    repo.write(".worker.toml", settings);
    repo.write("tracked.txt", b"tracked\n");
    repo.commit_all("run orchestration fixture");
    repo
}

fn orchestration_request(repo: &GitRepo) -> RunRequest {
    RunRequest {
        preference: WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        wait_for_capacity: false,
        project: repo.root().to_path_buf(),
        cli_includes: Vec::new(),
        timeout: None,
        command: CommandSpec::argv(vec![
            "printf".into(),
            "literal $HOME".into(),
            "--literal".into(),
        ])
        .unwrap(),
    }
}

fn assert_no_run_capture(cache: &Path) {
    for directory in [
        cache.join("snapshots/staging"),
        cache.join("snapshots/ready"),
    ] {
        if !directory.exists() {
            continue;
        }
        for entry in fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            assert_eq!(entry.file_name(), ".mac-worker-rooted-fs");
            assert!(fs::read_dir(entry.path()).unwrap().next().is_none());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_with_script(
    repo: &GitRepo,
    temp: &tempfile::TempDir,
    steps: impl IntoIterator<Item = RunScriptStep>,
    follower: &RecordingFollower,
    runtime: Option<&dyn ResolutionRuntime>,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> (
    Result<RunCompletion, WorkerError>,
    RunScriptRunner,
    ClientStateStore,
) {
    let paths = run_paths(temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), steps);
    let config = run_config();
    let service = if let Some(runtime) = runtime {
        RunService::with_follower_and_resolution_runtime(
            &runner, &config, &paths, &store, follower, runtime,
        )
    } else {
        RunService::with_follower(&runner, &config, &paths, &store, follower)
    };
    let result = service.submit_and_follow(orchestration_request(repo), json, stdout, stderr);
    (result, runner, store)
}

#[allow(clippy::too_many_arguments)]
fn run_scheduled_with_script(
    temp: &tempfile::TempDir,
    config: &Config,
    request: RunRequest,
    steps: impl IntoIterator<Item = RunScriptStep>,
    follower: &dyn JobFollower,
    resolution_runtime: Option<&dyn ResolutionRuntime>,
    scheduler_runtime: &dyn SchedulerRuntime,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> (
    Result<RunCompletion, WorkerError>,
    RunScriptRunner,
    ClientStateStore,
) {
    let paths = run_paths(temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), steps);
    let service = if let Some(runtime) = resolution_runtime {
        RunService::with_follower_and_resolution_runtime(
            &runner, config, &paths, &store, follower, runtime,
        )
    } else {
        RunService::with_follower(&runner, config, &paths, &store, follower)
    }
    .with_scheduler_runtime(scheduler_runtime);
    let result = service.submit_and_follow(request, false, stdout, stderr);
    (result, runner, store)
}

#[allow(clippy::too_many_arguments)]
fn run_with_script_and_observer(
    repo: &GitRepo,
    temp: &tempfile::TempDir,
    steps: impl IntoIterator<Item = RunScriptStep>,
    follower: &dyn JobFollower,
    runtime: Option<&dyn ResolutionRuntime>,
    observer: &dyn RunObserver,
    json: bool,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> (
    Result<RunCompletion, WorkerError>,
    RunScriptRunner,
    ClientStateStore,
) {
    let paths = run_paths(temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), steps);
    let config = run_config();
    let service = if let Some(runtime) = runtime {
        RunService::with_follower_and_resolution_runtime(
            &runner, &config, &paths, &store, follower, runtime,
        )
    } else {
        RunService::with_follower(&runner, &config, &paths, &store, follower)
    }
    .with_observer(observer);
    let result = service.submit_and_follow(orchestration_request(repo), json, stdout, stderr);
    (result, runner, store)
}

fn all_run_stages() -> [RunStage; 17] {
    [
        RunStage::InitialProjectInspection,
        RunStage::AdmissionObservation,
        RunStage::QueueEnqueue,
        RunStage::DeadDispatchRecovery,
        RunStage::QueueClaim,
        RunStage::StableProjectReload,
        RunStage::SnapshotSelectionAndCapture,
        RunStage::LocalRecordPublication,
        RunStage::DispatchToLeaseHandoff,
        RunStage::LeaseAcquire,
        RunStage::SnapshotUpload,
        RunStage::SnapshotVerification,
        RunStage::JobSubmission,
        RunStage::AcceptedOutputFlush,
        RunStage::JobFollower,
        RunStage::QueueTerminalRemoval,
        RunStage::SnapshotCleanup,
    ]
}

#[test]
fn no_wait_busy_pin_rejects_before_enqueue_snapshot_or_reroute() {
    // Break caught: --no-wait queues/captures despite an ineligible pin, or a
    // busy pin is silently rerouted to another configured SSH destination.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(10_000, 901);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut request = orchestration_request(&repo);
    request.wait_for_capacity = false;

    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &run_config(),
        request,
        [RunScriptStep::ProbeBusy],
        &follower,
        None,
        &scheduler,
        &mut stdout,
        &mut stderr,
    );

    assert!(matches!(
        result.unwrap_err(),
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(!run_paths(&temp).cache.join("snapshots").exists());
    assert!(scheduler.sleeps().is_empty());
    assert_eq!(runner.requests().len(), 1);
    assert_eq!(runner.requests()[0].args[9], OsStr::new("mac1"));
    assert!(
        runner
            .requests()
            .iter()
            .all(|request| !request.args.iter().any(|arg| arg == "poison-host"))
    );
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    runner.assert_consumed();
}

#[test]
fn no_wait_busy_pin_never_reconciles_a_recoverable_existing_job() {
    // Break caught: --no-wait performs mutating fleet repair before returning
    // CAPACITY_BUSY for a different pinned worker.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    store
        .create_job(test_record(
            &store,
            909,
            1_000,
            None,
            RemoteUncertainty::None,
        ))
        .unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), [RunScriptStep::ReconcileOrBusy]);
    let config = Config {
        version: 1,
        workers: vec![
            run_config().workers[0].clone(),
            WorkerEntry {
                name: "mini-2".into(),
                ssh: "mac2".into(),
                slots: 1,
                capabilities: Vec::new(),
                remote_binary: "~/.local/bin/worker".into(),
            },
        ],
    };
    let scheduler = TestSchedulerRuntime::new(10_000, 902);
    let follower = RecordingFollower::succeeding(0);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler);
    let mut request = orchestration_request(&repo);
    request.preference = WorkerPreference::Pinned {
        worker: "mini-2".into(),
    };

    let error = service
        .submit_and_follow(request, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    assert!(runner.requests().iter().all(|request| {
        request
            .args
            .last()
            .is_none_or(|argument| argument != OsStr::new(HostOperation::Reconcile.command()))
    }));
    runner.assert_consumed();
}

#[test]
fn cancel_removes_waiting_entry_without_ssh_or_snapshot() {
    // Break caught: public cancellation reaches a worker or creates a snapshot
    // after the queue lock proves this row is still waiting.
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let job_id = JobId::new(uuid::Uuid::from_u128(90_001));
    store
        .enqueue(
            QueueEntry::new(
                job_id,
                store.client_id(),
                "a".repeat(64),
                "b".repeat(64),
                CommandSpec::argv(vec!["queued".into()])
                    .unwrap()
                    .summary()
                    .unwrap(),
                Vec::new(),
                WorkerPreference::Automatic,
                QueueEntryKind::Batch,
                None,
                ProcessIdentity::new(90_001, 900_010_007).unwrap(),
                1,
            )
            .unwrap(),
        )
        .unwrap();
    let runner = RecordingRunner::returning(Vec::new());
    let runtime = TestSchedulerRuntime::new(2_000, 900);
    let cancel_config = config();
    let service =
        CancelService::new(&runner, &cancel_config, &store).with_scheduler_runtime(&runtime);

    assert_eq!(
        service.cancel(job_id).unwrap(),
        CancelReport::QueuedCancelled { job_id }
    );
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(runner.requests().is_empty());
    assert!(!paths.cache.join("snapshots").exists());
}

#[test]
fn cancel_resolves_the_original_record_then_sends_one_exact_typed_remote_request() {
    // Break caught: cancellation fabricates a new identity, targets a worker
    // chosen by fresh scheduling, or puts a version envelope on the fixed
    // cancel request before reaching the recorded host.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        90_101,
        100,
        Some(JobStatus::accepted(100).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(record.clone()).unwrap();
    let cancelled = JobStatus::accepted(100)
        .unwrap()
        .into_prelaunch_cancelled(101, 0, 0)
        .unwrap();
    let accepted =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(100).unwrap()).unwrap();
    let response =
        CancelResponse::new(StatusResponse::new(record.meta().clone(), cancelled.clone()).unwrap())
            .unwrap();
    let runner =
        RecordingRunner::returning(vec![status_result(&accepted), status_result(&response)]);
    let config = config();
    let observer = CountingCancelObserver::default();
    let report = CancelService::new(&runner, &config, &store)
        .with_cancel_observer(&observer)
        .cancel(record.meta().job_id())
        .unwrap();

    assert_eq!(
        report,
        CancelReport::RemoteCancelled {
            response: response.clone()
        }
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 2);
    assert_resolution_status_call(&requests[0], record.meta().job_id());
    assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
    let request: CancelRequest =
        serde_json::from_slice(requests[1].stdin.as_deref().unwrap()).unwrap();
    assert_eq!(request.job_id(), record.meta().job_id());
    assert_eq!(request.client_id(), record.meta().client_id());
    assert_eq!(request.lease_token(), record.lease_token());
    assert_eq!(
        request.request_fingerprint(),
        record.meta().request_fingerprint()
    );
    let wire: Value = serde_json::from_slice(requests[1].stdin.as_deref().unwrap()).unwrap();
    assert_eq!(wire.as_object().unwrap().len(), 4);
    assert!(wire.get("protocol_version").is_none());
    assert_eq!(observer.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .load_job(record.meta().job_id())
            .unwrap()
            .last_status(),
        Some(&cancelled)
    );
}

#[test]
fn remote_cancel_handoff_matrix_uses_one_typed_request_per_id_across_100_cases() {
    // Break caught: a cancel handoff can duplicate the remote mutation or
    // switch to a different identity while interleavings vary by job ID.
    for case in 0..100_u128 {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let created_at = 1_000 + case as u64;
        let record = test_record(
            &store,
            91_000 + case,
            created_at,
            Some(JobStatus::accepted(created_at).unwrap()),
            RemoteUncertainty::None,
        );
        store.create_job(record.clone()).unwrap();
        let cancelled = JobStatus::accepted(created_at)
            .unwrap()
            .into_prelaunch_cancelled(created_at + 1, 0, 0)
            .unwrap();
        let accepted = StatusResponse::new(
            record.meta().clone(),
            JobStatus::accepted(created_at).unwrap(),
        )
        .unwrap();
        let response =
            CancelResponse::new(StatusResponse::new(record.meta().clone(), cancelled).unwrap())
                .unwrap();
        let runner =
            RecordingRunner::returning(vec![status_result(&accepted), status_result(&response)]);
        let observer = CountingCancelObserver::default();

        let report = CancelService::new(&runner, &config(), &store)
            .with_cancel_observer(&observer)
            .cancel(record.meta().job_id())
            .unwrap();
        assert!(
            matches!(report, CancelReport::RemoteCancelled { .. }),
            "case {case}"
        );
        assert_eq!(observer.0.load(Ordering::SeqCst), 1, "case {case}");
        let requests = runner.requests();
        assert_eq!(requests.len(), 2, "case {case}");
        assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
    }
}

#[test]
fn remote_cancel_handoff_gate_matrix_preserves_exact_identity_across_100_cases() {
    // Break caught: a reader or retry racing the remote-cancel handoff can
    // cause duplicate cancellation, mutate a different job, or lose the
    // authoritative terminal response.
    for case in 0..100_u128 {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(state_store(&temp));
        let created_at = 2_000 + case as u64;
        let record = test_record(
            &store,
            92_000 + case,
            created_at,
            Some(JobStatus::accepted(created_at).unwrap()),
            RemoteUncertainty::None,
        );
        store.create_job(record.clone()).unwrap();
        let cancelled = JobStatus::accepted(created_at)
            .unwrap()
            .into_prelaunch_cancelled(created_at + 1, 0, 0)
            .unwrap();
        let accepted = StatusResponse::new(
            record.meta().clone(),
            JobStatus::accepted(created_at).unwrap(),
        )
        .unwrap();
        let response = CancelResponse::new(
            StatusResponse::new(record.meta().clone(), cancelled.clone()).unwrap(),
        )
        .unwrap();
        let runner = Arc::new(RecordingRunner::returning(vec![
            status_result(&accepted),
            status_result(&response),
        ]));
        let config = Arc::new(config());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let observer = Arc::new(BlockingCancelObserver {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
        });
        let cancel_store = Arc::clone(&store);
        let cancel_runner = Arc::clone(&runner);
        let cancel_config = Arc::clone(&config);
        let cancel_observer = Arc::clone(&observer);
        let job_id = record.meta().job_id();
        let canceller = thread::spawn(move || {
            CancelService::new(&*cancel_runner, &cancel_config, &cancel_store)
                .with_cancel_observer(&*cancel_observer)
                .cancel(job_id)
        });
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("case {case} did not reach remote-cancel handoff"));

        let (reader_ready_tx, reader_ready_rx) = mpsc::channel();
        let reader_store = Arc::clone(&store);
        let reader = thread::spawn(move || {
            let snapshot = reader_store.load_job(job_id).unwrap();
            reader_ready_tx.send(()).unwrap();
            snapshot
        });
        if case % 2 == 0 {
            reader_ready_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| panic!("case {case} reader did not overlap handoff"));
            release_tx.send(()).unwrap();
        } else {
            release_tx.send(()).unwrap();
            reader_ready_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| panic!("case {case} reader did not observe durable state"));
        }

        let report = canceller.join().unwrap().unwrap();
        let observed = reader.join().unwrap();
        assert!(
            matches!(report, CancelReport::RemoteCancelled { .. }),
            "case {case}"
        );
        assert_eq!(observer.calls.load(Ordering::SeqCst), 1, "case {case}");
        assert_eq!(
            observed.meta(),
            record.meta(),
            "case {case} reader identity"
        );
        assert!(matches!(
            observed.last_status().map(JobStatus::state),
            Some(JobState::Accepted | JobState::Cancelled)
        ));

        let requests = runner.requests();
        assert_eq!(requests.len(), 2, "case {case}");
        assert_resolution_status_call(&requests[0], job_id);
        assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
        let cancel_request: CancelRequest =
            serde_json::from_slice(requests[1].stdin.as_deref().unwrap()).unwrap();
        assert_eq!(cancel_request.job_id(), job_id, "case {case}");
        assert_eq!(
            cancel_request.client_id(),
            record.meta().client_id(),
            "case {case}"
        );
        assert_eq!(
            cancel_request.lease_token(),
            record.lease_token(),
            "case {case}"
        );
        assert_eq!(
            cancel_request.request_fingerprint(),
            record.meta().request_fingerprint(),
            "case {case}"
        );
        assert_eq!(
            store
                .load_job(job_id)
                .unwrap()
                .last_status()
                .map(JobStatus::state),
            Some(JobState::Cancelled),
            "case {case} authoritative cancellation"
        );
    }
}

#[test]
fn cancel_transport_loss_is_resolved_only_by_original_id_status() {
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        90_102,
        100,
        Some(JobStatus::accepted(100).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(record.clone()).unwrap();
    let accepted =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(100).unwrap()).unwrap();
    let cancelled = JobStatus::accepted(100)
        .unwrap()
        .into_prelaunch_cancelled(101, 0, 0)
        .unwrap();
    let terminal = StatusResponse::new(record.meta().clone(), cancelled.clone()).unwrap();
    let runner = RecordingRunner::returning(vec![
        status_result(&accepted),
        transport_failure(),
        status_result(&terminal),
    ]);
    let config = config();

    let report = CancelService::new(&runner, &config, &store)
        .cancel(record.meta().job_id())
        .unwrap();

    assert!(matches!(report, CancelReport::RemoteCancelled { .. }));
    let requests = runner.requests();
    assert_eq!(requests.len(), 3);
    assert_resolution_status_call(&requests[0], record.meta().job_id());
    assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
    assert_status_call(
        &requests[2],
        Duration::from_secs(30),
        record.meta().job_id(),
    );
    assert_eq!(
        store
            .load_job(record.meta().job_id())
            .unwrap()
            .last_status(),
        Some(&cancelled)
    );
}

#[test]
fn cancel_launch_reply_loss_is_resolved_only_by_original_id_status() {
    // Break caught: SSH_LAUNCH_FAILED is also used for a response loss after
    // an already-dispatched request, so treating it as a preflight failure
    // loses the original-ID-only reconciliation path.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        901_026,
        100,
        Some(JobStatus::accepted(100).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(record.clone()).unwrap();
    let accepted =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(100).unwrap()).unwrap();
    let cancelled = JobStatus::accepted(100)
        .unwrap()
        .into_prelaunch_cancelled(101, 0, 0)
        .unwrap();
    let terminal = StatusResponse::new(record.meta().clone(), cancelled.clone()).unwrap();
    let runner = RecordingRunner::returning(vec![
        status_result(&accepted),
        launch_reply_loss(),
        status_result(&terminal),
    ]);
    let config = config();

    let report = CancelService::new(&runner, &config, &store)
        .cancel(record.meta().job_id())
        .unwrap();

    assert!(matches!(report, CancelReport::RemoteCancelled { .. }));
    let requests = runner.requests();
    assert_eq!(requests.len(), 3);
    assert_resolution_status_call(&requests[0], record.meta().job_id());
    assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
    assert_status_call(
        &requests[2],
        Duration::from_secs(30),
        record.meta().job_id(),
    );
    assert_eq!(
        store
            .load_job(record.meta().job_id())
            .unwrap()
            .last_status(),
        Some(&cancelled)
    );
}

#[test]
fn cancel_preserves_a_typed_host_failure_without_status_fallback() {
    // Break caught: a nonzero canonical host response is incorrectly treated
    // as a lost reply, allowing a later terminal status to mask the host's
    // authoritative cancellation failure.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        901_021,
        100,
        Some(JobStatus::accepted(100).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(record.clone()).unwrap();
    let accepted =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(100).unwrap()).unwrap();
    let cancelled = JobStatus::accepted(100)
        .unwrap()
        .into_prelaunch_cancelled(101, 0, 0)
        .unwrap();
    let terminal = StatusResponse::new(record.meta().clone(), cancelled).unwrap();
    let runner = RecordingRunner::returning(vec![
        status_result(&accepted),
        authoritative_protocol_failure("JOB_ID_CONFLICT", "exact cancellation conflict"),
        status_result(&terminal),
    ]);
    let config = config();

    let error = CancelService::new(&runner, &config, &store)
        .cancel(record.meta().job_id())
        .unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message == "JOB_ID_CONFLICT: exact cancellation conflict"),
        "{error}"
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 2);
    assert_resolution_status_call(&requests[0], record.meta().job_id());
    assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
    assert_eq!(
        store
            .load_job(record.meta().job_id())
            .unwrap()
            .last_status(),
        record.last_status()
    );
}

#[test]
fn cancel_persists_resolved_acceptance_before_an_ambiguous_cancel_result() {
    // Break caught: an accepted resolution exists only in memory while the
    // cancel reply is lost, allowing a later Abandoned result to erase the
    // only durable evidence that remote acceptance occurred.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(&store, 901_022, 100, None, RemoteUncertainty::None);
    store.create_job(record.clone()).unwrap();
    let accepted_status = JobStatus::accepted(101).unwrap();
    let accepted = StatusResponse::new(record.meta().clone(), accepted_status.clone()).unwrap();
    let runner = RecordingRunner::returning(vec![
        status_result(&accepted),
        transport_failure(),
        status_result(&accepted),
    ]);
    let config = config();

    let error = CancelService::new(&runner, &config, &store)
        .cancel(record.meta().job_id())
        .unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message.starts_with("CANCEL_TRANSPORT_AMBIGUOUS:")),
        "{error}"
    );
    let persisted = store.load_job(record.meta().job_id()).unwrap();
    assert_eq!(persisted.last_status(), Some(&accepted_status));
    assert_eq!(
        persisted.remote_uncertainty().code(),
        Some("CANCEL_TRANSPORT_AMBIGUOUS")
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 3);
    assert_resolution_status_call(&requests[0], record.meta().job_id());
    assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
    assert_status_call(
        &requests[2],
        Duration::from_secs(30),
        record.meta().job_id(),
    );
}

#[test]
fn cancel_rejects_abandoned_resolution_against_durable_acceptance_evidence() {
    // Break caught: a local accepted/running/terminal observation is silently
    // downgraded to a queued cancellation when remote resolution says
    // Abandoned, instead of retaining a recovery-required contradiction.
    let cases = [
        (901_023, JobStatus::accepted(100).unwrap()),
        (901_024, JobStatus::running(101, 11, 12, 13, 14).unwrap()),
        (901_025, JobStatus::succeeded(102, 0, 0).unwrap()),
    ];

    for (id, status) in cases {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let record = test_record(
            &store,
            id,
            100,
            Some(status.clone()),
            RemoteUncertainty::None,
        );
        store.create_job(record.clone()).unwrap();
        let runner = RecordingRunner::returning(vec![
            authoritative_protocol_failure("JOB_ABANDONED", "exact remote abandonment"),
            status_result(&ResolveOrAbandonResponse::abandoned()),
        ]);
        let config = config();

        let error = CancelService::new(&runner, &config, &store)
            .cancel(record.meta().job_id())
            .unwrap_err();

        assert!(
            matches!(error, WorkerError::Protocol(ref message) if message.starts_with("ACCEPTANCE_EVIDENCE_CONFLICT:")),
            "{error}"
        );
        let persisted = store.load_job(record.meta().job_id()).unwrap();
        assert_eq!(persisted.last_status(), Some(&status));
        assert_eq!(
            persisted.remote_uncertainty().code(),
            Some("ACCEPTANCE_EVIDENCE_CONFLICT")
        );
        let requests = runner.requests();
        assert_eq!(requests.len(), 2);
        assert_resolution_status_call(&requests[0], record.meta().job_id());
        assert_control_call(
            &requests[1],
            HostOperation::ResolveOrAbandon,
            Duration::from_secs(30),
        );
    }
}

#[test]
fn cancel_unknown_remote_record_is_recovery_required_and_never_guessed_cancelled() {
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        90_103,
        100,
        None,
        RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
    );
    store.create_job(record.clone()).unwrap();
    let runner = RecordingRunner::returning(Vec::new());
    let config = config();

    let error = CancelService::new(&runner, &config, &store)
        .cancel(record.meta().job_id())
        .unwrap_err();

    assert!(error.to_string().contains("UNKNOWN_REMOTE"), "{error}");
    assert!(runner.requests().is_empty());
    assert_eq!(
        store
            .load_job(record.meta().job_id())
            .unwrap()
            .remote_uncertainty(),
        record.remote_uncertainty()
    );
}

#[test]
fn claim_race_cancellation_retires_only_the_exact_prelocal_dispatch_without_remote_work() {
    // Break caught: cancel deletes a claimed row, the owner misses the durable
    // request and crosses a remote boundary, or a queue report says queued
    // after an accepted job could exist.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), [RunScriptStep::ProbeReady]);
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(90_200, 90_201);
    let observer = CancelAtClaimObserver {
        store: store.clone(),
        requests: AtomicUsize::new(0),
    };
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_observer(&observer)
        .with_scheduler_runtime(&scheduler);

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Queue {
            code: "CANCELLED",
            ..
        }
    ));
    assert_eq!(observer.requests.load(Ordering::SeqCst), 1);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert_eq!(
        runner.requests().len(),
        1,
        "only pre-claim admission probe is allowed"
    );
    assert_eq!(
        runner.requests()[0].args.last().unwrap(),
        "~/.local/bin/worker host probe"
    );
    assert_no_run_capture(&paths.cache);
    runner.assert_consumed();
}

#[test]
fn cancel_launch_handoff_matrix_has_one_terminal_outcome_and_no_lease_launch() {
    // Break caught: a cancel that completes during the dispatch-to-lease
    // handoff is ignored, causing an extra lease/launch or a second terminal
    // outcome for the exact same job ID.
    let repo = run_repo(b"version = 1\n");
    let mut schedule_counts = [0usize; 4];
    for case in 0..100 {
        let schedule = case % 4;
        schedule_counts[schedule] += 1;
        let run_request = orchestration_request(&repo);
        let temp = tempfile::tempdir().unwrap();
        let paths = run_paths(&temp);
        let store = Arc::new(ClientStateStore::open(&paths.state).unwrap());
        let runner = Arc::new(RunScriptRunner::new(
            paths.state.clone(),
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::StatusStoredRecord,
                RunScriptStep::CancelStoredRecord,
            ],
        ));
        let config = Arc::new(run_config());
        let follower = Arc::new(RecordingFollower::succeeding(0));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let dispatch_observer = Arc::new(BlockingDispatchObserver {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let run_store = Arc::clone(&store);
        let run_runner = Arc::clone(&runner);
        let run_config = Arc::clone(&config);
        let run_follower = Arc::clone(&follower);
        let run_observer = Arc::clone(&dispatch_observer);
        let run_paths = paths.clone();
        let run_thread = thread::spawn(move || {
            let service = RunService::with_follower(
                &*run_runner,
                &run_config,
                &run_paths,
                &run_store,
                &*run_follower,
            )
            .with_observer(&*run_observer);
            service.submit_and_follow(run_request, false, &mut Vec::new(), &mut Vec::new())
        });
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("case {case} did not reach dispatch handoff"));
        let job_id = store
            .queue_snapshot()
            .unwrap()
            .entries()
            .first()
            .expect("dispatch handoff must retain its queue row")
            .job_id();
        let cancel_runner = Arc::clone(&runner);
        let cancel_store = Arc::clone(&store);
        let cancel_config = Arc::clone(&config);
        let cancel_observer = Arc::new(CountingCancelObserver::default());
        let cancel_observer_ref = Arc::clone(&cancel_observer);
        let (start_cancel, wait_for_cancel_start) = mpsc::channel();
        let canceller = thread::spawn(move || {
            wait_for_cancel_start.recv().unwrap();
            CancelService::new(&*cancel_runner, &cancel_config, &cancel_store)
                .with_cancel_observer(&*cancel_observer_ref)
                .cancel(job_id)
        });

        let spawn_reader = || {
            let (ready_tx, ready_rx) = mpsc::channel();
            let reader_store = Arc::clone(&store);
            let reader = thread::spawn(move || {
                let snapshot = reader_store.load_job(job_id).unwrap();
                ready_tx.send(()).unwrap();
                snapshot
            });
            (reader, ready_rx)
        };
        let mut reader = None;
        match schedule {
            0 => {
                start_cancel.send(()).unwrap();
            }
            1 => {
                let (handle, ready) = spawn_reader();
                ready
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| panic!("case {case} reader did not start"));
                reader = Some(handle);
                start_cancel.send(()).unwrap();
            }
            2 => {
                let enqueued_at = store
                    .queue_snapshot()
                    .unwrap()
                    .entries()
                    .first()
                    .unwrap()
                    .enqueued_at_millis();
                let pre_cancel = store.request_queue_cancel(job_id, enqueued_at).unwrap();
                assert!(matches!(
                    pre_cancel,
                    Some(mac_worker::job::QueueCancel::RequestedDispatch { job_id: id, .. })
                        if id == job_id
                ));
                start_cancel.send(()).unwrap();
            }
            3 => {
                start_cancel.send(()).unwrap();
                let (handle, ready) = spawn_reader();
                ready
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap_or_else(|_| panic!("case {case} reader did not start"));
                reader = Some(handle);
            }
            _ => unreachable!(),
        }
        let cancel_report = canceller.join().unwrap().unwrap();
        assert!(
            matches!(cancel_report, CancelReport::RemoteCancelled { .. }),
            "case {case}: {cancel_report:?}"
        );
        if let Some(reader) = reader {
            let observed = reader.join().unwrap();
            assert_eq!(observed.meta(), store.load_job(job_id).unwrap().meta());
            assert!(matches!(
                observed.last_status().map(JobStatus::state),
                None | Some(JobState::Accepted | JobState::Cancelled)
            ));
        }
        release_tx.send(()).unwrap();

        let run_result = run_thread.join().unwrap();
        assert!(
            matches!(
                run_result,
                Err(WorkerError::Queue {
                    code: "CANCELLED",
                    ..
                })
            ),
            "case {case}: {run_result:?}"
        );
        assert_eq!(cancel_observer.0.load(Ordering::SeqCst), 1, "case {case}");
        assert_eq!(follower.calls.load(Ordering::SeqCst), 0, "case {case}");
        let requests = runner.requests();
        assert!(
            requests.iter().all(|request| request.args.last()
                != Some(&HostOperation::LeaseAcquire.command().into())),
            "case {case} crossed lease acquire after cancellation"
        );
        assert!(
            requests
                .iter()
                .filter(
                    |request| request.args.last() == Some(&HostOperation::Cancel.command().into())
                )
                .count()
                == 1,
            "case {case} did not issue exactly one remote cancel"
        );
        assert!(
            store.queue_snapshot().unwrap().entries().is_empty(),
            "case {case}"
        );
        assert_eq!(
            store
                .load_job(job_id)
                .unwrap()
                .last_status()
                .map(JobStatus::state),
            Some(JobState::Cancelled),
            "case {case}"
        );
    }
    assert_eq!(schedule_counts, [25, 25, 25, 25]);
}

#[test]
fn dispatch_owner_cancels_an_acceptance_that_races_submit_completion() {
    // Break caught: a cancellation recorded while submit returns is skipped,
    // allowing the dispatcher to publish/follow an accepted job instead of
    // resolving the original ID and issuing its exact typed cancellation.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
            RunScriptStep::StatusAccepted,
            RunScriptStep::CancelPrelaunch,
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let observer = CancelAtSubmissionObserver {
        store: store.clone(),
        requests: AtomicUsize::new(0),
    };
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_observer(&observer);

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Queue {
            code: "CANCELLED",
            ..
        }
    ));
    assert_eq!(observer.requests.load(Ordering::SeqCst), 1);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let record = store
        .list_jobs()
        .unwrap()
        .into_iter()
        .next()
        .expect("local record remains as authoritative cancellation evidence");
    assert_eq!(
        record.last_status().map(JobStatus::state),
        Some(JobState::Cancelled)
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 7);
    assert_resolution_status_call(&requests[5], record.meta().job_id());
    assert_control_call(&requests[6], HostOperation::Cancel, Duration::from_secs(30));
    runner.assert_consumed();
}

#[test]
fn dispatch_owner_preserves_terminal_outcome_that_wins_a_submit_cancel_race() {
    // Break caught: after a cancellation request, original-ID resolution can
    // prove the exact job already failed, but the dispatcher overwrites that
    // authoritative terminal outcome with a generic cancellation error.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
            RunScriptStep::StatusTerminal {
                exit_code: 7,
                stdout_bytes: 0,
                stderr_bytes: 0,
            },
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let observer = CancelAtSubmissionObserver {
        store: store.clone(),
        requests: AtomicUsize::new(0),
    };
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_observer(&observer);

    let completion = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();

    assert_eq!(completion.exit_code, 7);
    assert_eq!(completion.report.status.state(), JobState::Failed);
    assert_eq!(observer.requests.load(Ordering::SeqCst), 1);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let record = store.list_jobs().unwrap().pop().unwrap();
    assert_eq!(
        record.last_status().map(JobStatus::state),
        Some(JobState::Failed)
    );
    runner.assert_consumed();
}

#[test]
fn dispatch_cancel_preserves_a_typed_host_failure_without_status_fallback() {
    // Break caught: the dispatch-owner cancellation path turns an
    // authoritative host failure into a later terminal status instead of
    // retaining the exact cancellation error and queue recovery state.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
            RunScriptStep::StatusAccepted,
            RunScriptStep::AuthoritativeFailure(HostOperation::Cancel, "CLEANUP_PROOF_MISSING"),
            RunScriptStep::StatusTerminal {
                exit_code: 0,
                stdout_bytes: 0,
                stderr_bytes: 0,
            },
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let observer = CancelAtSubmissionObserver {
        store: store.clone(),
        requests: AtomicUsize::new(0),
    };
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_observer(&observer);

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(
        matches!(error, WorkerError::Protocol(ref message) if message == "CLEANUP_PROOF_MISSING: authoritative admission failure"),
        "{error}"
    );
    assert_eq!(observer.requests.load(Ordering::SeqCst), 1);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    let record = store.list_jobs().unwrap().pop().unwrap();
    assert_eq!(
        record.last_status().map(JobStatus::state),
        Some(JobState::Accepted)
    );
    assert_eq!(store.queue_snapshot().unwrap().entries().len(), 1);
    assert_eq!(runner.requests().len(), 7);
}

#[test]
fn dispatch_does_not_follow_after_public_cancel_retires_a_terminal_row() {
    // Break caught: a public canceller can finish the exact terminal update
    // after the dispatcher's final pre-acceptance check, while the dispatcher
    // still starts a follower and then mistakes its already-retired row for a
    // queue failure.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
            RunScriptStep::StatusAccepted,
            RunScriptStep::CancelPrelaunch,
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::terminal(FollowerOutcome::Cancelled, false);
    let observer = CancelAtAcceptedOutputObserver {
        store: store.clone(),
        runner: &runner,
        config: &config,
        requests: AtomicUsize::new(0),
    };
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_observer(&observer);

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(
        matches!(
            error,
            WorkerError::Queue {
                code: "CANCELLED",
                ..
            }
        ),
        "{error}"
    );
    assert_eq!(observer.requests.load(Ordering::SeqCst), 1);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let record = store.list_jobs().unwrap().pop().unwrap();
    assert_eq!(
        record.last_status().map(JobStatus::state),
        Some(JobState::Cancelled)
    );
    runner.assert_consumed();
}

#[test]
fn dispatch_cancel_with_a_durable_record_reports_remote_never_queued() {
    // Break caught: a caller observes a dispatch row, then reports a queued
    // cancellation even though the owner has already durably published the
    // exact identity that may have reached remote acceptance.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = test_record(
        &store,
        90_104,
        100,
        Some(JobStatus::accepted(100).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(record.clone()).unwrap();
    let owner = ProcessIdentity::new(90_204, 902_040_007).unwrap();
    store
        .enqueue(
            QueueEntry::new(
                record.meta().job_id(),
                store.client_id(),
                record.meta().project_id().into(),
                record.meta().worktree_id().into(),
                record.meta().command_summary().clone(),
                Vec::new(),
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::Batch,
                None,
                owner,
                record.meta().created_at_millis(),
            )
            .unwrap(),
        )
        .unwrap();
    store
        .claim_next(
            owner,
            &["mini-1".into()],
            record.meta().created_at_millis() + 1,
        )
        .unwrap()
        .expect("exact owner must claim its row");
    let accepted =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(100).unwrap()).unwrap();
    let cancelled = JobStatus::accepted(100)
        .unwrap()
        .into_prelaunch_cancelled(101, 0, 0)
        .unwrap();
    let response =
        CancelResponse::new(StatusResponse::new(record.meta().clone(), cancelled).unwrap())
            .unwrap();
    let runner =
        RecordingRunner::returning(vec![status_result(&accepted), status_result(&response)]);
    let runtime = TestSchedulerRuntime::new(90_205, 90_205);
    let config = config();

    let report = CancelService::new(&runner, &config, &store)
        .with_scheduler_runtime(&runtime)
        .cancel(record.meta().job_id())
        .unwrap();

    assert_eq!(report, CancelReport::RemoteCancelled { response });
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let requests = runner.requests();
    assert_eq!(requests.len(), 2);
    assert_resolution_status_call(&requests[0], record.meta().job_id());
    assert_control_call(&requests[1], HostOperation::Cancel, Duration::from_secs(30));
}

#[test]
fn dispatch_cancel_without_a_record_waits_boundedly_and_never_reports_queued() {
    // Break caught: the public caller retires a claimed row itself or claims
    // queued cancellation while the dispatch owner could still publish the
    // original identity and resolve remote acceptance.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let job_id = JobId::new(uuid::Uuid::from_u128(90_105));
    let owner = ProcessIdentity::new(90_205, 902_050_007).unwrap();
    store
        .enqueue(
            QueueEntry::new(
                job_id,
                store.client_id(),
                "a".repeat(64),
                "b".repeat(64),
                CommandSpec::argv(vec!["queued".into()])
                    .unwrap()
                    .summary()
                    .unwrap(),
                Vec::new(),
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::Batch,
                None,
                owner,
                1,
            )
            .unwrap(),
        )
        .unwrap();
    store
        .claim_next(owner, &["mini-1".into()], 2)
        .unwrap()
        .expect("exact owner must claim its row");
    let runner = RecordingRunner::returning(Vec::new());
    let scheduler = TestSchedulerRuntime::new(90_206, 90_206);
    let config = config();

    let error = CancelService::new(&runner, &config, &store)
        .with_scheduler_runtime(&scheduler)
        .cancel(job_id)
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Queue {
            code: "CANCEL_PENDING",
            ..
        }
    ));
    assert!(runner.requests().is_empty());
    assert_eq!(scheduler.sleeps(), vec![Duration::from_secs(1)]);
    let queue = store.queue_snapshot().unwrap();
    assert_eq!(queue.entries().len(), 1);
    assert!(
        matches!(queue.entries()[0].state(), QueueState::Dispatching { dispatch_owner, .. } if *dispatch_owner == owner)
    );
    assert!(queue.entries()[0].is_cancel_requested());
}

#[test]
fn invalid_pin_rejects_before_observation_or_durable_mutation() {
    // Break caught: an unknown explicit pin reaches SSH, queue publication, or
    // snapshot capture before configuration validation rejects it.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(15_000, 911);
    let mut request = orchestration_request(&repo);
    request.preference = WorkerPreference::Pinned {
        worker: "missing-worker".into(),
    };

    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &run_config(),
        request,
        [],
        &follower,
        None,
        &scheduler,
        &mut Vec::new(),
        &mut Vec::new(),
    );

    assert!(matches!(
        result.unwrap_err(),
        WorkerError::Config(message) if message.contains("WORKER_NOT_FOUND")
    ));
    assert!(runner.requests().is_empty());
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(!run_paths(&temp).cache.join("snapshots").exists());
    assert!(scheduler.sleeps().is_empty());
    runner.assert_consumed();
}

#[test]
fn no_wait_claim_loss_removes_only_the_new_waiting_row() {
    // Break caught: --no-wait snapshots after losing the atomic FIFO claim,
    // deletes the older row, or leaves its own unserviceable row behind.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store =
        ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwnerInspector).unwrap();
    let initial = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let older_job = JobId::new(uuid::Uuid::from_u128(8_002));
    store
        .enqueue(
            QueueEntry::new(
                older_job,
                store.client_id(),
                initial.context.project_id.clone(),
                initial.context.worktree_id.clone(),
                CommandSpec::argv(vec!["older".into()])
                    .unwrap()
                    .summary()
                    .unwrap(),
                initial.requirements.clone(),
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::Batch,
                None,
                ProcessIdentity::new(802, 8_020_007).unwrap(),
                59_000,
            )
            .unwrap(),
        )
        .unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), [RunScriptStep::ProbeReady]);
    let config = config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(60_000, 912);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler);
    let request = orchestration_request(&repo);

    let error = service
        .submit_and_follow(request, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    let queue = store.queue_snapshot().unwrap();
    assert_eq!(queue.entries().len(), 1);
    assert_eq!(queue.entries()[0].job_id(), older_job);
    assert!(matches!(
        queue.entries()[0].state(),
        QueueState::Waiting { .. }
    ));
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(!paths.cache.join("snapshots").exists());
    assert!(scheduler.sleeps().is_empty());
    runner.assert_consumed();
}

#[test]
fn automatic_run_selects_the_eligible_worker_while_a_pin_never_reroutes() {
    // Break caught: automatic mode keeps the Phase 3 explicit worker route, or
    // pinned policy is discarded after observing the pinned worker busy.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(20_000, 902);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut request = orchestration_request(&repo);
    request.preference = WorkerPreference::Automatic;

    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &run_config(),
        request,
        [
            RunScriptStep::ProbeBusy,
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        &scheduler,
        &mut stdout,
        &mut stderr,
    );

    let completion = result.unwrap();
    let material = runner.material();
    assert_eq!(completion.report.worker, "poison-worker");
    assert_eq!(material.worker_name(), "poison-worker");
    let requests = runner.requests();
    assert_eq!(requests[0].args[9], OsStr::new("mac1"));
    assert_eq!(requests[1].args[9], OsStr::new("poison-host"));
    assert!(requests[2..].iter().all(|request| {
        request.program == OsStr::new("/usr/bin/rsync")
            || request
                .args
                .get(9)
                .is_some_and(|argument| argument == "poison-host")
    }));
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let affinity = store
        .affinity_hints(material.project_id(), material.worktree_id())
        .unwrap();
    assert_eq!(affinity.worktree_worker.as_deref(), Some("poison-worker"));
    assert_eq!(affinity.project_worker.as_deref(), Some("poison-worker"));
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    runner.assert_consumed();
}

#[test]
fn automatic_fifo_admission_skips_an_unavailable_worker_and_claims_the_next_compatible_row() {
    // Break caught: one failed fresh worker observation prevents a later
    // compatible worker from claiming and admitting the FIFO row.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(20_100, 903);
    let mut request = orchestration_request(&repo);
    request.preference = WorkerPreference::Automatic;

    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &run_config(),
        request,
        [
            RunScriptStep::ProbeUnavailable,
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        &scheduler,
        &mut Vec::new(),
        &mut Vec::new(),
    );

    let completion = result.unwrap();
    assert_eq!(completion.report.worker, "poison-worker");
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert_eq!(
        runner
            .requests()
            .iter()
            .filter(|request| request.args.last()
                == Some(&OsString::from(HostOperation::LeaseAcquire.command())))
            .count(),
        1
    );
    runner.assert_consumed();
}

#[test]
fn cached_ready_observation_is_consumed_without_claiming_fresh_affinity() {
    // Break caught: a cache hit is mistaken for a remote refresh merely
    // because its age is zero, causing unobserved affinity to be published.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    store
        .admission_observation("mini-1", 25_000, || {
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Idle,
                vec!["declared-capability".into(), "project-capability".into()],
                Some(12 * 1024 * 1024 * 1024),
                100 * 1024 * 1024 * 1024,
                25_000,
            )
        })
        .unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(25_000, 913);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler);

    let completion = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();

    assert_eq!(completion.report.worker, "mini-1");
    let material = runner.material();
    let affinity = store
        .affinity_hints(material.project_id(), material.worktree_id())
        .unwrap();
    assert_eq!(affinity.worktree_worker, None);
    assert_eq!(affinity.project_worker, None);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    runner.assert_consumed();
}

#[test]
fn shared_admission_cache_remains_project_neutral_across_requirement_sets() {
    // Break caught: a project-specific missing requirement is cached as worker
    // unavailability and incorrectly rejects a later compatible project.
    let incompatible = run_repo(b"version = 1\nrequires = [\"missing-capability\"]\n");
    let compatible = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(26_000, 914);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler);

    let incompatible_error = service
        .submit_and_follow(
            orchestration_request(&incompatible),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(matches!(
        incompatible_error,
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));

    let completion = service
        .submit_and_follow(
            orchestration_request(&compatible),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();

    assert_eq!(completion.report.worker, "mini-1");
    assert_eq!(
        runner
            .requests()
            .iter()
            .filter(|request| request
                .args
                .last()
                .is_some_and(|argument| argument == "~/.local/bin/worker host probe"))
            .count(),
        1
    );
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    runner.assert_consumed();
}

#[test]
fn post_enqueue_clock_failure_removes_the_new_waiting_row() {
    // Break caught: failure obtaining the claim timestamp escapes after enqueue
    // and strands this invocation's unclaimed durable row.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(paths.state.clone(), [RunScriptStep::ProbeReady]);
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = FailingSchedulerRuntime::failing_on(3, 915);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler);

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(matches!(error, WorkerError::Io(_)));
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(!paths.cache.join("snapshots").exists());
    runner.assert_consumed();
}

#[test]
fn post_enqueue_affinity_failure_removes_the_new_waiting_row() {
    // Break caught: corrupt affinity state is discovered after enqueue and the
    // scheduler returns without cleaning its still-unclaimed row.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let initial = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let observer = CorruptAffinityAfterEnqueue {
        path: paths
            .state
            .join("affinity/projects")
            .join(format!("{}.json", initial.context.project_id)),
    };
    let runner = RunScriptRunner::new(paths.state.clone(), [RunScriptStep::ProbeReady]);
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(27_000, 916);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler)
        .with_observer(&observer);

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();

    assert!(matches!(error, WorkerError::Io(_)));
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert!(!paths.cache.join("snapshots").exists());
    runner.assert_consumed();
}

#[test]
fn older_live_pin_wins_its_worker_without_blocking_another_worker() {
    // Break caught: public orchestration bypasses owner-scoped per-worker FIFO,
    // or a pinned head blocks younger automatic work on a distinct idle host.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store =
        ClientStateStore::open_with_owner_inspector(&paths.state, LiveOwnerInspector).unwrap();
    let initial = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let older_job = JobId::new(uuid::Uuid::from_u128(8_001));
    store
        .enqueue(
            QueueEntry::new(
                older_job,
                store.client_id(),
                initial.context.project_id.clone(),
                initial.context.worktree_id.clone(),
                CommandSpec::argv(vec!["older".into()])
                    .unwrap()
                    .summary()
                    .unwrap(),
                initial.requirements.clone(),
                WorkerPreference::Pinned {
                    worker: "mini-1".into(),
                },
                QueueEntryKind::Batch,
                None,
                ProcessIdentity::new(801, 8_010_007).unwrap(),
                49_000,
            )
            .unwrap(),
        )
        .unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(50_000, 905);
    let mut request = orchestration_request(&repo);
    request.preference = WorkerPreference::Automatic;
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler);

    let completion = service
        .submit_and_follow(request, false, &mut Vec::new(), &mut Vec::new())
        .unwrap();

    assert_eq!(completion.report.worker, "poison-worker");
    let queue = store.queue_snapshot().unwrap();
    assert_eq!(queue.entries().len(), 1);
    assert_eq!(queue.entries()[0].job_id(), older_job);
    assert!(matches!(
        queue.entries()[0].state(),
        QueueState::Waiting { .. }
    ));
    runner.assert_consumed();
}

#[test]
fn waiting_dispatch_polls_with_only_bounded_one_second_sleeps() {
    // Break caught: waiting mode snapshots while busy, busy-spins, sleeps for
    // an unbounded interval, or fails to refresh the two-second cache.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(30_000, 903);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let mut request = orchestration_request(&repo);
    request.wait_for_capacity = true;
    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &config(),
        request,
        [
            RunScriptStep::ProbeBusy,
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        &scheduler,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(result.unwrap().exit_code, 0);
    assert_eq!(scheduler.sleeps(), vec![Duration::from_secs(1); 3]);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    runner.assert_consumed();
}

#[test]
fn waiting_lease_capacity_busy_resumes_fifo_and_executes_once() {
    // Break caught: an authoritative CAPACITY_BUSY lease returns immediately
    // in waiting mode instead of reverting and resuming FIFO polling.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(40_000, 904);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut request = orchestration_request(&repo);
    request.wait_for_capacity = true;

    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &config(),
        request,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::AuthoritativeFailure(HostOperation::LeaseAcquire, "CAPACITY_BUSY"),
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        &scheduler,
        &mut stdout,
        &mut stderr,
    );

    let completion = result.unwrap();
    assert_eq!(completion.exit_code, 0);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    assert_eq!(scheduler.sleeps(), vec![Duration::from_secs(1)]);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let lease_attempts = runner
        .events()
        .into_iter()
        .filter(|event| event.contains("LeaseAcquire") || event == "Acquire")
        .count();
    assert_eq!(lease_attempts, 2);
    assert_eq!(
        runner
            .events()
            .into_iter()
            .filter(|event| event == "SubmitAccepted")
            .count(),
        1
    );
    runner.assert_consumed();
}

#[test]
fn no_wait_lease_capacity_busy_exits_clean_without_local_residue() {
    // Break caught: --no-wait lease CAPACITY_BUSY leaves a waiting queue row,
    // unpublished local job, or dispatch reservation behind.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(40_000, 904);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let (result, runner, store) = run_scheduled_with_script(
        &temp,
        &config(),
        orchestration_request(&repo),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::AuthoritativeFailure(HostOperation::LeaseAcquire, "CAPACITY_BUSY"),
        ],
        &follower,
        None,
        &scheduler,
        &mut stdout,
        &mut stderr,
    );

    assert!(matches!(
        result.unwrap_err(),
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert_eq!(runner.requests().len(), 2);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    assert!(scheduler.sleeps().is_empty());
    runner.assert_consumed();
    assert_no_run_capture(&run_paths(&temp).cache);
}

#[test]
fn crash_after_local_record_publication_is_recovered_on_the_next_pass() {
    // Break caught: dying after create_job and before dispatch handoff strands
    // the Dispatching reservation so a later scheduler pass cannot proceed.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let config = config();
    let crashed_runner = RunScriptRunner::new(paths.state.clone(), [RunScriptStep::ProbeReady]);
    let crashed_follower = RecordingFollower::succeeding(0);
    let crashed_scheduler = TestSchedulerRuntime::new(50_000, 906);
    let crash = PanicAfterLocalRecordPublication;
    let crashed =
        RunService::with_follower(&crashed_runner, &config, &paths, &store, &crashed_follower)
            .with_scheduler_runtime(&crashed_scheduler)
            .with_observer(&crash);

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crashed.submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
    }));
    assert!(
        panicked.is_err(),
        "local-record publication crash point was not injected"
    );
    let stranded = store.queue_snapshot().unwrap();
    assert_eq!(stranded.entries().len(), 1);
    assert!(matches!(
        stranded.entries()[0].state(),
        QueueState::Dispatching { .. }
    ));
    let unpublished = store.list_jobs().unwrap();
    assert_eq!(unpublished.len(), 1);
    assert!(unpublished[0].last_status().is_none());
    assert_eq!(
        unpublished[0].remote_uncertainty(),
        &RemoteUncertainty::None
    );

    let follower = RecordingFollower::succeeding(0);
    let scheduler = TestSchedulerRuntime::new(60_000, 907);
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let completion = RunService::with_follower(&runner, &config, &paths, &store, &follower)
        .with_scheduler_runtime(&scheduler)
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap();

    assert_eq!(completion.exit_code, 0);
    assert_eq!(completion.report.worker, "mini-1");
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    let leftover = store.queue_snapshot().unwrap();
    assert_eq!(leftover.entries().len(), 1);
    assert!(matches!(
        leftover.entries()[0].state(),
        QueueState::Waiting { .. }
    ));
    runner.assert_consumed();
}

#[test]
fn run_performs_the_exact_scheduler_stage_order_against_one_worker() {
    // Break caught: any remote effect precedes durable identity, any stage is
    // reordered/duplicated, or terminal follower authority is not persisted.
    let repo = run_repo(b"version = 1\ntimeout = \"2m\"\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(7);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );

    let completion = result.unwrap();
    assert_eq!(completion.exit_code, 7);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.list_jobs().unwrap().len(), 1);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    let persisted = store.load_job(completion.report.job_id).unwrap();
    assert_eq!(persisted.last_status().unwrap().state(), JobState::Failed);
    assert_eq!(persisted.last_status().unwrap().exit_code(), Some(7));
    assert_eq!(persisted.remote_uncertainty(), &RemoteUncertainty::None);
    let remote_events = runner
        .events()
        .into_iter()
        .filter(|event| event != "git")
        .collect::<Vec<_>>();
    assert_eq!(
        remote_events,
        [
            "ProbeReady",
            "Acquire",
            "Upload",
            "Verify",
            "SubmitAccepted",
        ]
    );
    assert!(String::from_utf8(stdout).unwrap().starts_with("job "));
    assert!(stderr.is_empty());
    runner.assert_consumed();
    assert_no_run_capture(&run_paths(&temp).cache);
}

#[test]
fn run_observer_records_the_exact_scheduler_effect_boundaries() {
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let observer = RecordingRunObserver::recording();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let (result, runner, _) = run_with_script_and_observer(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        &observer,
        false,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(result.unwrap().exit_code, 0);
    assert_eq!(observer.stages(), all_run_stages());
    assert_eq!(observer.cleanup_count(), 1);
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    runner.assert_consumed();
    assert_no_run_capture(&run_paths(&temp).cache);
}

#[test]
fn default_run_observer_is_behaviorally_noop() {
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );

    let completion = result.unwrap();
    assert_eq!(completion.exit_code, 0);
    assert!(
        store
            .load_job(completion.report.job_id)
            .unwrap()
            .last_status()
            .unwrap()
            .state()
            .is_terminal()
    );
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    runner.assert_consumed();
    assert_no_run_capture(&run_paths(&temp).cache);
}

#[test]
fn poisoned_run_boundary_stops_later_effects_and_cleans_each_owned_snapshot_once() {
    let stages = all_run_stages();
    let success_steps = [
        RunScriptStep::ProbeReady,
        RunScriptStep::Acquire,
        RunScriptStep::Upload,
        RunScriptStep::Verify,
        RunScriptStep::SubmitAccepted,
    ];

    for (index, failed_stage) in stages.iter().copied().enumerate() {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let observer = RecordingRunObserver::failing_at(failed_stage);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let (result, runner, store) = run_with_script_and_observer(
            &repo,
            &temp,
            success_steps,
            &follower,
            None,
            &observer,
            false,
            &mut stdout,
            &mut stderr,
        );

        let error = result.unwrap_err();
        assert!(
            matches!(&error, WorkerError::Protocol(message) if message.contains("INJECTED_RUN_STAGE_FAILURE")),
            "unexpected error for {failed_stage:?}: {error:?}"
        );
        let mut expected_trace = stages[..=index].to_vec();
        if matches!(
            failed_stage,
            RunStage::SnapshotSelectionAndCapture
                | RunStage::LocalRecordPublication
                | RunStage::DispatchToLeaseHandoff
                | RunStage::LeaseAcquire
                | RunStage::SnapshotUpload
                | RunStage::SnapshotVerification
                | RunStage::JobSubmission
                | RunStage::AcceptedOutputFlush
                | RunStage::JobFollower
                | RunStage::QueueTerminalRemoval
        ) {
            expected_trace.push(RunStage::SnapshotCleanup);
        }
        assert_eq!(
            observer.stages(),
            expected_trace,
            "failed at {failed_stage:?}"
        );

        let expected_remaining = match failed_stage {
            RunStage::InitialProjectInspection => 5,
            RunStage::AdmissionObservation
            | RunStage::QueueEnqueue
            | RunStage::DeadDispatchRecovery
            | RunStage::QueueClaim
            | RunStage::StableProjectReload
            | RunStage::SnapshotSelectionAndCapture
            | RunStage::LocalRecordPublication
            | RunStage::DispatchToLeaseHandoff => 4,
            RunStage::LeaseAcquire => 3,
            RunStage::SnapshotUpload => 2,
            RunStage::SnapshotVerification => 1,
            RunStage::JobSubmission
            | RunStage::AcceptedOutputFlush
            | RunStage::JobFollower
            | RunStage::QueueTerminalRemoval
            | RunStage::SnapshotCleanup => 0,
        };
        assert_eq!(
            runner.remaining_steps().len(),
            expected_remaining,
            "failed at {failed_stage:?}"
        );
        assert_eq!(
            follower.calls.load(Ordering::SeqCst),
            usize::from(index >= 14),
            "failed at {failed_stage:?}"
        );
        assert_eq!(
            store.list_jobs().unwrap().len(),
            usize::from(index >= 7),
            "failed at {failed_stage:?}"
        );
        assert_eq!(
            observer.cleanup_count(),
            usize::from(index >= 6),
            "failed at {failed_stage:?}"
        );
        assert_no_run_capture(&run_paths(&temp).cache);
    }
}

#[test]
fn actual_snapshot_cleanup_runs_once_on_representative_prepared_paths() {
    let success_steps = [
        RunScriptStep::ProbeReady,
        RunScriptStep::Acquire,
        RunScriptStep::Upload,
        RunScriptStep::Verify,
        RunScriptStep::SubmitAccepted,
    ];

    // Success.
    {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let observer = RecordingRunObserver::recording();
        let (result, runner, _) = run_with_script_and_observer(
            &repo,
            &temp,
            success_steps,
            &follower,
            None,
            &observer,
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        );
        assert!(result.is_ok());
        assert_eq!(observer.cleanup_count(), 1);
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    // Remote preacceptance failure resolved as abandoned.
    {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let observer = RecordingRunObserver::recording();
        let (result, runner, _) = run_with_script_and_observer(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::TransportFailure(HostOperation::LeaseAcquire),
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveAbandoned,
            ],
            &follower,
            Some(&runtime),
            &observer,
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        );
        assert!(result.is_err());
        assert_eq!(observer.cleanup_count(), 1);
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    // Typed recovery persists cleanup-pending uncertainty.
    {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let observer = RecordingRunObserver::recording();
        let (result, runner, store) = run_with_script_and_observer(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::TransportFailure(HostOperation::Submit),
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveCleanupPending,
            ],
            &follower,
            Some(&runtime),
            &observer,
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        );
        assert!(result.is_err());
        assert!(matches!(
            store.list_jobs().unwrap()[0].remote_uncertainty(),
            RemoteUncertainty::CleanupPending { .. }
        ));
        assert_eq!(observer.cleanup_count(), 1);
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    // Structurally valid but mismatched remote identity.
    {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let observer = RecordingRunObserver::recording();
        let (result, runner, _) = run_with_script_and_observer(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::AcquireMismatch(LeaseMismatch::ProjectId),
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveAbandoned,
            ],
            &follower,
            Some(&runtime),
            &observer,
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        );
        assert!(result.is_err());
        assert_eq!(observer.cleanup_count(), 1);
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    // Accepted output failure.
    {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let observer = RecordingRunObserver::recording();
        let output = Arc::new(Mutex::new(OutputState::default()));
        let mut stdout = RecordingWriter::failing(output);
        let (result, runner, _) = run_with_script_and_observer(
            &repo,
            &temp,
            success_steps,
            &follower,
            None,
            &observer,
            false,
            &mut stdout,
            &mut Vec::new(),
        );
        assert!(matches!(result.unwrap_err(), WorkerError::Io(_)));
        assert_eq!(observer.cleanup_count(), 1);
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    // Final terminal persistence failure.
    {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let paths = run_paths(&temp);
        let store = ClientStateStore::open(&paths.state).unwrap();
        let runner = RunScriptRunner::new(paths.state.clone(), success_steps);
        let config = run_config();
        let follower = FinalPersistenceFailureFollower {
            store: store.clone(),
        };
        let observer = RecordingRunObserver::recording();
        let service = RunService::with_follower(&runner, &config, &paths, &store, &follower)
            .with_observer(&observer);

        let result = service.submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        );

        assert!(matches!(result.unwrap_err(), WorkerError::Io(_)));
        assert_eq!(observer.cleanup_count(), 1);
        runner.assert_consumed();
        assert_no_run_capture(&paths.cache);
    }
}

#[test]
fn real_cleanup_failure_precedes_cleanup_observer_failure_without_double_cleanup() {
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let follower = RecordingFollower::failing_with_cleanup_sabotage(paths.cache.clone());
    let observer = RecordingRunObserver::failing_at(RunStage::SnapshotCleanup);
    let (result, runner, _) = run_with_script_and_observer(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        &observer,
        false,
        &mut Vec::new(),
        &mut Vec::new(),
    );

    assert!(matches!(result.unwrap_err(), WorkerError::Io(_)));
    assert_eq!(observer.cleanup_count(), 1);
    assert_eq!(
        observer
            .stages()
            .iter()
            .filter(|stage| **stage == RunStage::SnapshotCleanup)
            .count(),
        1
    );
    runner.assert_consumed();
}

#[test]
fn artifact_configuration_stops_before_every_remote_or_capture_effect() {
    // Break caught: unsupported artifact policy reaches probe, capture, state
    // creation, or any remote mutation before being rejected.
    let repo =
        run_repo(b"version = 1\n[artifacts]\ninclude = [\"target/**\"]\nmax_total_bytes = 10\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );

    assert!(matches!(
        result.unwrap_err(),
        WorkerError::Project {
            code: "ARTIFACTS_UNSUPPORTED",
            ..
        }
    ));
    assert!(runner.requests().is_empty());
    assert!(store.list_jobs().unwrap().is_empty());
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    assert!(!run_paths(&temp).cache.join("snapshots").exists());
    runner.assert_consumed();
}

#[test]
fn each_preflight_or_stage_failure_poisons_all_later_stages() {
    // Break caught: a failed stage falls through to a later remote operation or
    // follower. An empty script after the intended recovery is a poison guard.
    let cases = [
        vec![RunScriptStep::ProbeUnavailable],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::TransportFailure(HostOperation::LeaseAcquire),
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAbandoned,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::UploadFailure,
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAbandoned,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::TransportFailure(HostOperation::SnapshotVerify),
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAbandoned,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::TransportFailure(HostOperation::Submit),
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAbandoned,
        ],
    ];

    for steps in cases {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, _) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert!(result.is_err());
        assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }
}

#[test]
fn run_reuses_one_identity_through_acquire_upload_verify_submit_and_resolution() {
    // Break caught: recovery regenerates an ID/token/time/fingerprint or a
    // transfer/control request is built from different immutable material.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let runtime = ImmediateResolution::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::TransportFailure(HostOperation::Submit),
            RunScriptStep::ResolveAccepted,
        ],
        &follower,
        Some(&runtime),
        false,
        &mut stdout,
        &mut stderr,
    );

    let completion = result.unwrap();
    let material = runner.material();
    assert_eq!(completion.report.job_id, material.job_id());
    let records = store.list_jobs().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].meta().job_id(), material.job_id());
    assert_eq!(records[0].lease_token(), material.lease_token());
    assert_eq!(
        records[0].meta().created_at_millis(),
        material.created_at_millis()
    );
    assert_eq!(
        records[0].meta().request_fingerprint(),
        &material.fingerprint()
    );
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    runner.assert_consumed();
}

#[test]
fn existing_accepted_skips_upload_verify_and_submit() {
    // Break caught: idempotent acquire acceptance retransfers or resubmits
    // instead of obtaining exact authoritative status and following the job.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, _) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::AcquireExisting,
            RunScriptStep::StatusAccepted,
        ],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );

    assert!(result.is_ok());
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    assert!(runner.requests().iter().all(|request| {
        request.program != OsStr::new("/usr/bin/rsync")
            && request.args.last().is_none_or(|argument| {
                argument != HostOperation::SnapshotVerify.command()
                    && argument != HostOperation::Submit.command()
            })
    }));
    runner.assert_consumed();
}

#[test]
fn accepted_resolution_continues_the_same_job() {
    // Break caught: a lost acquire reply that resolves Accepted returns an
    // error, starts transfer, or allocates a replacement identity.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let runtime = ImmediateResolution::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::TransportFailure(HostOperation::LeaseAcquire),
            RunScriptStep::ResolveAccepted,
        ],
        &follower,
        Some(&runtime),
        false,
        &mut stdout,
        &mut stderr,
    );

    let completion = result.unwrap();
    assert_eq!(completion.report.job_id, runner.material().job_id());
    assert_eq!(store.list_jobs().unwrap().len(), 1);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
    runner.assert_consumed();
}

#[test]
fn abandoned_preserves_original_typed_failure_without_persisting_abandoned() {
    // Break caught: authoritative abandonment becomes a fabricated local state
    // or masks the original typed admission error.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let runtime = ImmediateResolution::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::AuthoritativeFailure(HostOperation::Submit, "CAPACITY_BUSY"),
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAbandoned,
        ],
        &follower,
        Some(&runtime),
        false,
        &mut stdout,
        &mut stderr,
    );

    assert!(matches!(
        result.unwrap_err(),
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    let record = store.list_jobs().unwrap().pop().unwrap();
    assert!(record.last_status().is_none());
    assert_eq!(record.remote_uncertainty(), &RemoteUncertainty::None);
    assert!(store.queue_snapshot().unwrap().entries().is_empty());
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    runner.assert_consumed();
}

#[test]
fn unknown_remote_and_cleanup_pending_persist_distinct_markers() {
    // Break caught: typed recovery outcomes are conflated, discarded, or
    // represented as an invented durable Abandoned status.
    for (tail, cleanup) in [
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveTransportFailure,
            ],
            false,
        ),
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveCleanupPending,
            ],
            true,
        ),
    ] {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut steps = vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::TransportFailure(HostOperation::Submit),
        ];
        steps.extend(tail);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        let error = result.unwrap_err();
        assert_eq!(error.exit_code(), 70);
        let record = store.list_jobs().unwrap().pop().unwrap();
        if cleanup {
            assert!(matches!(
                record.remote_uncertainty(),
                RemoteUncertainty::CleanupPending { code } if code == "CLEANUP_STILL_PENDING"
            ));
        } else {
            assert!(matches!(
                record.remote_uncertainty(),
                RemoteUncertainty::UnknownRemote { code } if code == "UNKNOWN_REMOTE"
            ));
        }
        let diagnostic = error.to_string();
        assert!(diagnostic.contains(&record.meta().job_id().to_string()));
        assert!(diagnostic.contains("worker status"));
        assert!(record.last_status().is_none());
        let queue = store.queue_snapshot().unwrap();
        assert_eq!(queue.entries().len(), 1);
        assert_eq!(queue.entries()[0].job_id(), record.meta().job_id());
        assert!(matches!(
            queue.entries()[0].state(),
            QueueState::Dispatching { .. }
        ));
        runner.assert_consumed();
    }
}

#[test]
fn snapshot_cleanup_runs_once_on_every_success_and_failure_path() {
    // Break caught: prepared snapshot ownership leaks on remote, writer, or
    // follower failure, or cleanup is attempted twice.
    for (steps, follower, writer_fails) in [
        (
            vec![
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::SubmitAccepted,
            ],
            RecordingFollower::succeeding(0),
            false,
        ),
        (
            vec![
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::SubmitAccepted,
            ],
            RecordingFollower::failing(),
            false,
        ),
        (
            vec![
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::SubmitAccepted,
            ],
            RecordingFollower::succeeding(0),
            true,
        ),
    ] {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let output = Arc::new(Mutex::new(OutputState::default()));
        let mut stdout = if writer_fails {
            RecordingWriter::failing(Arc::clone(&output))
        } else {
            RecordingWriter::new(Arc::clone(&output))
        };
        let mut stderr = Vec::new();
        let (result, runner, _) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            None,
            false,
            &mut stdout,
            &mut stderr,
        );
        if !matches!(follower.outcome, FollowerOutcome::Error(_)) && !writer_fails {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let cache = run_paths(&temp).cache;
    let follower = RecordingFollower::failing_with_cleanup_sabotage(cache);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, _) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );
    assert!(
        matches!(result.unwrap_err(), WorkerError::Io(_)),
        "cleanup I/O failure must take precedence over the follower failure"
    );
    runner.assert_consumed();
}

#[test]
fn accepted_human_and_json_records_are_flushed_before_following() {
    // Break caught: accepted output is buffered until after follower output, is
    // sent to stderr in JSON mode, or uses a noncanonical event shape.
    for json in [false, true] {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let output = Arc::new(Mutex::new(OutputState::default()));
        let follower = RecordingFollower::requiring_flushed(Arc::clone(&output));
        let mut stdout = RecordingWriter::new(Arc::clone(&output));
        let mut stderr = Vec::new();
        let (result, runner, _) = run_with_script(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::SubmitAccepted,
            ],
            &follower,
            None,
            json,
            &mut stdout,
            &mut stderr,
        );
        assert!(result.is_ok());
        let output = output.lock().unwrap();
        assert_eq!(output.flushes, 1);
        assert!(stderr.is_empty());
        if json {
            assert_eq!(
                output.bytes.iter().filter(|byte| **byte == b'\n').count(),
                1
            );
            assert!(matches!(
                serde_json::from_slice::<JsonEvent>(&output.bytes).unwrap(),
                JsonEvent::Accepted { .. }
            ));
        } else {
            let text = std::str::from_utf8(&output.bytes).unwrap();
            assert!(text.starts_with("job "));
            assert!(text.ends_with(" accepted on mini-1\n"));
        }
        runner.assert_consumed();
    }
}

#[test]
fn run_uses_cli_timeout_over_project_timeout_and_preserves_literal_argv() {
    // Break caught: project timeout overrides CLI, argv is shell-expanded, or
    // command material changes between local persistence and submission.
    let repo = run_repo(b"version = 1\ntimeout = \"99s\"\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower);
    let command = CommandSpec::argv(vec![
        "printf".into(),
        "literal $HOME".into(),
        "semi;colon".into(),
    ])
    .unwrap();
    let result = service.submit_and_follow(
        RunRequest {
            timeout: Some(Duration::from_secs(45)),
            command: command.clone(),
            ..orchestration_request(&repo)
        },
        false,
        &mut Vec::new(),
        &mut Vec::new(),
    );

    assert!(result.is_ok());
    let material = runner.material();
    assert_eq!(material.timeout_millis(), 45_000);
    assert_eq!(material.command(), &command);
    runner.assert_consumed();
}

#[test]
fn run_probes_only_the_explicit_inventory_worker() {
    // Break caught: run fans out across inventory, accepts the SSH destination
    // as a worker name, or loses project requirements during policy ranking.
    let repo = run_repo(b"version = 1\nrequires = [\"project-capability\"]\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, _) = run_with_script(
        &repo,
        &temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );

    assert!(result.is_ok());
    let requests = runner.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request
                .args
                .last()
                .is_some_and(|arg| arg == "~/.local/bin/worker host probe"))
            .count(),
        1
    );
    assert!(requests.iter().all(|request| {
        if request.program == OsStr::new("/usr/bin/ssh") {
            request.args[9] == OsStr::new("mac1")
        } else {
            !request
                .args
                .iter()
                .any(|argument| argument == "poison-host")
        }
    }));
    runner.assert_consumed();
}

#[test]
fn run_completion_preserves_exact_command_outcomes_before_cleanup_failures() {
    // Break caught: cleanup enrichment masks an established exact command
    // result, or infrastructure/signal outcomes are normalized as command exits.
    let cases = [
        (FollowerOutcome::Exit(7), true, Ok(7)),
        (FollowerOutcome::Exit(0), true, Ok(0)),
        (FollowerOutcome::Exit(64), false, Ok(64)),
        (FollowerOutcome::Signal(15), false, Ok(143)),
        (FollowerOutcome::Infrastructure, true, Err(70)),
    ];

    for (outcome, cleanup_error, expected) in cases {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::terminal(outcome, cleanup_error);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::SubmitAccepted,
            ],
            &follower,
            None,
            false,
            &mut stdout,
            &mut stderr,
        );
        match expected {
            Ok(code) => assert_eq!(result.unwrap().exit_code, code, "{outcome:?}"),
            Err(code) => assert_eq!(result.unwrap_err().exit_code(), code, "{outcome:?}"),
        }
        let persisted = store.list_jobs().unwrap().pop().unwrap();
        assert!(persisted.last_status().unwrap().state().is_terminal());
        assert_eq!(
            persisted
                .last_status()
                .unwrap()
                .cleanup_error_code()
                .is_some(),
            cleanup_error
        );
        runner.assert_consumed();
    }
}

#[test]
fn run_completion_classifies_inconsistent_terminal_outcomes_as_infrastructure() {
    // Break caught: post-acceptance outcome-shape failures are mistaken for a
    // remote transport failure, or a real follower transport failure is hidden.
    let cases = [
        (FollowerOutcome::Nonterminal, 70),
        (FollowerOutcome::MismatchedMeta, 70),
        (FollowerOutcome::Signal(128), 70),
        (FollowerOutcome::Error("remote follow failed"), 69),
    ];

    for (outcome, expected_code) in cases {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::terminal(outcome, false);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::Verify,
                RunScriptStep::SubmitAccepted,
            ],
            &follower,
            None,
            false,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(
            result.unwrap_err().exit_code(),
            expected_code,
            "{outcome:?}"
        );
        if matches!(outcome, FollowerOutcome::Error(_)) {
            let persisted = store.list_jobs().unwrap().pop().unwrap();
            assert_eq!(persisted.last_status().unwrap().state(), JobState::Accepted);
            assert_eq!(persisted.remote_uncertainty(), &RemoteUncertainty::None);
        }
        runner.assert_consumed();
    }
}

#[test]
fn mismatched_acquire_and_verify_identity_responses_always_resolve_before_returning() {
    // Break caught: a structurally valid but wrong acquire/verify response
    // returns immediately even though the remote side may already have mutated.
    let lease_mismatches = [
        LeaseMismatch::JobId,
        LeaseMismatch::ClientId,
        LeaseMismatch::LeaseToken,
        LeaseMismatch::RequestFingerprint,
        LeaseMismatch::WorkerName,
        LeaseMismatch::ProjectId,
        LeaseMismatch::WorktreeId,
        LeaseMismatch::ManifestDigest,
        LeaseMismatch::TimeoutMillis,
        LeaseMismatch::ResourceClass,
        LeaseMismatch::CommandSummary,
    ];
    for mismatch in lease_mismatches {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::AcquireMismatch(mismatch),
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveAbandoned,
            ],
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert!(
            matches!(
                result.unwrap_err(),
                WorkerError::Transport {
                    code: "INVALID_RESPONSE",
                    ..
                }
            ),
            "{mismatch:?}"
        );
        let record = store.list_jobs().unwrap().pop().unwrap();
        assert!(record.last_status().is_none(), "{mismatch:?}");
        assert_eq!(record.remote_uncertainty(), &RemoteUncertainty::None);
        assert!(
            store.queue_snapshot().unwrap().entries().is_empty(),
            "{mismatch:?}"
        );
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }

    let verification_mismatches = [
        VerificationMismatch::JobId,
        VerificationMismatch::ClientId,
        VerificationMismatch::ProjectId,
        VerificationMismatch::WorktreeId,
        VerificationMismatch::ManifestDigest,
    ];
    for mismatch in verification_mismatches {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            [
                RunScriptStep::ProbeReady,
                RunScriptStep::Acquire,
                RunScriptStep::Upload,
                RunScriptStep::VerifyMismatch(mismatch),
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveAbandoned,
            ],
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert!(
            matches!(
                result.unwrap_err(),
                WorkerError::Transport {
                    code: "INVALID_RESPONSE",
                    ..
                }
            ),
            "{mismatch:?}"
        );
        assert!(
            store.queue_snapshot().unwrap().entries().is_empty(),
            "{mismatch:?}"
        );
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }
}

#[test]
fn mismatched_mutation_responses_preserve_typed_resolution_outcomes() {
    // Break caught: mismatch recovery cannot continue an accepted job or
    // durably distinguish unknown remote state from cleanup pending.
    for steps in [
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::AcquireMismatch(LeaseMismatch::ProjectId),
            RunScriptStep::ResolveAccepted,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::VerifyMismatch(VerificationMismatch::ManifestDigest),
            RunScriptStep::ResolveAccepted,
        ],
    ] {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(result.unwrap().exit_code, 0);
        assert!(
            store.list_jobs().unwrap()[0]
                .last_status()
                .unwrap()
                .state()
                .is_terminal()
        );
        assert!(store.queue_snapshot().unwrap().entries().is_empty());
        runner.assert_consumed();
    }

    for (tail, cleanup_pending) in [
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveTransportFailure,
            ],
            false,
        ),
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveCleanupPending,
            ],
            true,
        ),
    ] {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut steps = vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::AcquireMismatch(LeaseMismatch::LeaseToken),
        ];
        steps.extend(tail);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(result.unwrap_err().exit_code(), 70);
        let record = store.list_jobs().unwrap().pop().unwrap();
        assert_eq!(
            matches!(
                record.remote_uncertainty(),
                RemoteUncertainty::CleanupPending { .. }
            ),
            cleanup_pending
        );
        assert_eq!(
            matches!(
                record.remote_uncertainty(),
                RemoteUncertainty::UnknownRemote { .. }
            ),
            !cleanup_pending
        );
        let queue = store.queue_snapshot().unwrap();
        assert_eq!(queue.entries().len(), 1);
        assert_eq!(queue.entries()[0].job_id(), record.meta().job_id());
        assert!(matches!(
            queue.entries()[0].state(),
            QueueState::Dispatching { .. }
        ));
        runner.assert_consumed();
        assert_no_run_capture(&run_paths(&temp).cache);
    }
}

#[test]
fn every_acceptance_evidence_failure_resolves_before_following_or_returning() {
    // Break caught: accepted/existing evidence can return transport failure
    // without persisting an exact accepted status or typed uncertainty.
    let cases = [
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::AcquireExisting,
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAccepted,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::AcquireExisting,
            RunScriptStep::StatusMismatch,
            RunScriptStep::ResolveAccepted,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAcceptedMismatch,
            RunScriptStep::ResolveAccepted,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitExisting,
            RunScriptStep::StatusTransportFailure,
            RunScriptStep::ResolveAccepted,
        ],
        vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitExisting,
            RunScriptStep::StatusMismatch,
            RunScriptStep::ResolveAccepted,
        ],
    ];
    for steps in cases {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(result.unwrap().exit_code, 0);
        assert_eq!(follower.calls.load(Ordering::SeqCst), 1);
        let record = store.list_jobs().unwrap().pop().unwrap();
        assert!(record.last_status().unwrap().state().is_terminal());
        assert_eq!(record.remote_uncertainty(), &RemoteUncertainty::None);
        assert!(store.queue_snapshot().unwrap().entries().is_empty());
        runner.assert_consumed();
    }
}

#[test]
fn contradictory_or_uncertain_acceptance_evidence_is_durable_and_infrastructure() {
    // Break caught: contradictory acceptance evidence becomes unavailable/69
    // or exits without a durable uncertainty marker.
    for (tail, expected_code) in [
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveTransportFailure,
            ],
            "UNKNOWN_REMOTE",
        ),
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveCleanupPending,
            ],
            "CLEANUP_STILL_PENDING",
        ),
        (
            vec![
                RunScriptStep::StatusTransportFailure,
                RunScriptStep::ResolveAbandoned,
            ],
            "ACCEPTANCE_EVIDENCE_CONFLICT",
        ),
        (
            vec![RunScriptStep::AuthoritativeFailure(
                HostOperation::Status,
                "JOB_ID_CONFLICT",
            )],
            "ACCEPTANCE_EVIDENCE_CONFLICT",
        ),
    ] {
        let repo = run_repo(b"version = 1\n");
        let temp = tempfile::tempdir().unwrap();
        let follower = RecordingFollower::succeeding(0);
        let runtime = ImmediateResolution::new();
        let mut steps = vec![
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAcceptedMismatch,
        ];
        steps.extend(tail);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let (result, runner, store) = run_with_script(
            &repo,
            &temp,
            steps,
            &follower,
            Some(&runtime),
            false,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(result.unwrap_err().exit_code(), 70);
        let record = store.list_jobs().unwrap().pop().unwrap();
        assert_eq!(record.remote_uncertainty().code(), Some(expected_code));
        assert!(record.last_status().is_none());
        let queue = store.queue_snapshot().unwrap();
        assert_eq!(queue.entries().len(), 1);
        assert_eq!(queue.entries()[0].job_id(), record.meta().job_id());
        assert!(matches!(
            queue.entries()[0].state(),
            QueueState::Dispatching { .. }
        ));
        assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
        runner.assert_consumed();
    }
}

#[test]
fn completion_uses_the_concurrently_enriched_persisted_terminal_status() {
    // Break caught: terminal reconciliation preserves a compatible concurrent
    // enrichment, but RunCompletion is built from the stale follower status.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner = RunScriptRunner::new(
        paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let config = run_config();
    let follower = ConcurrentTerminalFollower {
        store: store.clone(),
    };
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let completion = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();

    assert_eq!(completion.exit_code, 7);
    assert_eq!(
        completion.report.status.cleanup_error_code(),
        Some("CONCURRENT_CLEANUP")
    );
    let persisted = store.load_job(completion.report.job_id).unwrap();
    assert_eq!(completion.report.status, *persisted.last_status().unwrap());
    runner.assert_consumed();
}

#[test]
fn user_correctable_input_selection_failures_are_project_usage_errors() {
    // Break caught: an actionable input-policy blocker is classified as an
    // infrastructure snapshot failure instead of Project/usage 64.
    let repo = run_repo(b"version = 1\n");
    repo.write("untracked.txt", b"declare me explicitly\n");
    let temp = tempfile::tempdir().unwrap();
    let follower = RecordingFollower::succeeding(0);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let (result, runner, store) = run_with_script(
        &repo,
        &temp,
        [RunScriptStep::ProbeReady],
        &follower,
        None,
        false,
        &mut stdout,
        &mut stderr,
    );

    assert!(matches!(
        result.unwrap_err(),
        WorkerError::Project {
            code: "UNTRACKED_INPUT",
            ..
        }
    ));
    assert!(store.list_jobs().unwrap().is_empty());
    let queue = store.queue_snapshot().unwrap();
    assert_eq!(queue.entries().len(), 1);
    assert!(matches!(
        queue.entries()[0].state(),
        QueueState::Waiting { .. }
    ));
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    runner.assert_consumed();
}

#[test]
fn operational_git_selection_failures_remain_snapshot_infrastructure_errors() {
    // Break caught: the Project/64 classification for actionable selection
    // policy blockers accidentally masks a genuine Git/process failure.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let paths = run_paths(&temp);
    let store = ClientStateStore::open(&paths.state).unwrap();
    let runner =
        RunScriptRunner::with_index_query_failure(paths.state.clone(), [RunScriptStep::ProbeReady]);
    let config = run_config();
    let follower = RecordingFollower::succeeding(0);
    let service = RunService::with_follower(&runner, &config, &paths, &store, &follower);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let error = service
        .submit_and_follow(
            orchestration_request(&repo),
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();

    assert!(matches!(
        error,
        WorkerError::Snapshot {
            code: "GIT_INPUT_SELECTION_FAILED",
            ..
        }
    ));
    assert_eq!(error.exit_code(), 70);
    assert!(store.list_jobs().unwrap().is_empty());
    let queue = store.queue_snapshot().unwrap();
    assert_eq!(queue.entries().len(), 1);
    assert!(matches!(
        queue.entries()[0].state(),
        QueueState::Waiting { .. }
    ));
    assert_eq!(follower.calls.load(Ordering::SeqCst), 0);
    runner.assert_consumed();
}

fn run_worktree_git(directory: &Path, home: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new("/usr/bin/git")
        .current_dir(directory)
        .env("HOME", home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_CEILING_DIRECTORIES")
        .env_remove("GIT_DISCOVERY_ACROSS_FILESYSTEM")
        .env_remove("GIT_CONFIG_COUNT")
        .env_remove("GIT_CONFIG_PARAMETERS")
        .args(args)
        .output()
        .expect("run isolated git fixture command in the linked worktree")
}

#[test]
fn linked_worktrees_submit_distinct_immutable_snapshots_under_the_same_project_id() {
    // Catches collapsing two linked worktrees of one project onto a shared
    // snapshot identity, or letting one worktree's captured content leak
    // into the other worktree's manifest or job.
    let repo = run_repo(b"version = 1\n");
    let home = repo.root().join("home");
    let linked_root = tempfile::tempdir().unwrap();
    let linked_path = linked_root.path().join("linked-worktree");
    assert!(
        repo.git(&[
            "worktree",
            "add",
            "--quiet",
            linked_path.to_str().unwrap(),
            "-b",
            "linked-branch",
        ])
        .status
        .success()
    );
    fs::write(
        linked_path.join("linked-only.txt"),
        b"linked worktree contents\n",
    )
    .unwrap();
    assert!(
        run_worktree_git(&linked_path, &home, &["add", "--all"])
            .status
            .success()
    );
    assert!(
        run_worktree_git(
            &linked_path,
            &home,
            &["commit", "-m", "linked worktree fixture"],
        )
        .status
        .success()
    );

    let primary_temp = tempfile::tempdir().unwrap();
    let linked_temp = tempfile::tempdir().unwrap();
    let primary_follower = RecordingFollower::succeeding(0);
    let linked_follower = RecordingFollower::succeeding(0);
    let mut primary_stdout = Vec::new();
    let mut primary_stderr = Vec::new();
    let mut linked_stdout = Vec::new();
    let mut linked_stderr = Vec::new();

    let (primary_result, primary_runner, primary_store) = run_with_script(
        &repo,
        &primary_temp,
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
        &primary_follower,
        None,
        false,
        &mut primary_stdout,
        &mut primary_stderr,
    );

    let linked_paths = run_paths(&linked_temp);
    let linked_store = ClientStateStore::open(&linked_paths.state).unwrap();
    let linked_config = run_config();
    let linked_runner = RunScriptRunner::new(
        linked_paths.state.clone(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
        ],
    );
    let linked_service = RunService::with_follower(
        &linked_runner,
        &linked_config,
        &linked_paths,
        &linked_store,
        &linked_follower,
    );
    let linked_result = linked_service.submit_and_follow(
        RunRequest {
            preference: WorkerPreference::Pinned {
                worker: "mini-1".into(),
            },
            wait_for_capacity: true,
            project: linked_path.clone(),
            cli_includes: Vec::new(),
            timeout: None,
            command: CommandSpec::argv(vec!["printf".into(), "linked".into()]).unwrap(),
        },
        false,
        &mut linked_stdout,
        &mut linked_stderr,
    );

    let primary_completion = primary_result.unwrap();
    let linked_completion = linked_result.unwrap();
    assert_eq!(primary_completion.exit_code, 0);
    assert_eq!(linked_completion.exit_code, 0);
    assert_eq!(primary_follower.calls.load(Ordering::SeqCst), 1);
    assert_eq!(linked_follower.calls.load(Ordering::SeqCst), 1);

    let primary_material = primary_runner.material();
    let linked_material = linked_runner.material();
    assert_eq!(primary_material.project_id(), linked_material.project_id());
    assert_ne!(
        primary_material.worktree_id(),
        linked_material.worktree_id()
    );
    assert_ne!(
        primary_material.manifest_digest(),
        linked_material.manifest_digest()
    );
    assert_ne!(primary_material.job_id(), linked_material.job_id());

    assert_eq!(primary_store.list_jobs().unwrap().len(), 1);
    assert_eq!(linked_store.list_jobs().unwrap().len(), 1);
    primary_runner.assert_consumed();
    linked_runner.assert_consumed();
    assert_no_run_capture(&run_paths(&primary_temp).cache);
    assert_no_run_capture(&run_paths(&linked_temp).cache);
}

const LOG_CHUNK_LIMIT: u32 = 65_536;
const PLANTED_STDOUT_TEXT: &[u8] = b"PLANTED_STDOUT_SECRET";
const PLANTED_STDERR_TEXT: &[u8] = b"PLANTED_STDERR_SECRET";

struct PanicFollowRuntime;

impl FollowRuntime for PanicFollowRuntime {
    fn sleep(&self, _duration: Duration) {
        panic!("non-follow logs must not sleep");
    }
}

struct RecordingFollowRuntime {
    sleeps: Mutex<Vec<Duration>>,
}

impl RecordingFollowRuntime {
    fn new() -> Self {
        Self {
            sleeps: Mutex::new(Vec::new()),
        }
    }

    fn sleeps(&self) -> Vec<Duration> {
        self.sleeps.lock().unwrap().clone()
    }
}

impl FollowRuntime for RecordingFollowRuntime {
    fn sleep(&self, duration: Duration) {
        self.sleeps.lock().unwrap().push(duration);
    }
}

fn logs_service<'a>(
    config: &'a Config,
    store: &'a ClientStateStore,
    remote: &'a RemoteJobClient<'a>,
    runtime: &'a dyn FollowRuntime,
) -> LogsService<'a> {
    LogsService {
        config,
        client_state: store,
        remote,
        runtime,
    }
}

fn persist_log_job(store: &ClientStateStore, status: Option<JobStatus>) -> LocalJobRecord {
    let record = test_record(store, 1, 100, status, RemoteUncertainty::None);
    store.create_job(record.clone()).unwrap();
    record
}

fn log_status_response(record: &LocalJobRecord, status: JobStatus) -> StatusResponse {
    StatusResponse::new(record.meta().clone(), status).unwrap()
}

fn log_chunk_result(
    stream: LogStream,
    offset: u64,
    bytes: &[u8],
) -> Result<ProcessResult, WorkerError> {
    status_result(
        &LogChunkResponse::new(LogChunk::new(stream, offset, bytes.to_vec()).unwrap()).unwrap(),
    )
}

fn running_status(updated_at_millis: u64) -> JobStatus {
    JobStatus::running(updated_at_millis, 42, 1_000, 43, 1_001).unwrap()
}

fn signal_status(updated_at_millis: u64, signal: u32, stdout: u64, stderr: u64) -> JobStatus {
    JobStatus::new(
        JobState::Failed,
        updated_at_millis,
        None,
        None,
        None,
        None,
        None,
        Some(signal),
        Some(stdout),
        Some(stderr),
        None,
        None,
    )
    .unwrap()
}

fn assert_status_query(request: &ProcessRequest, job_id: JobId) {
    assert_status_call(request, Duration::from_secs(30), job_id);
}

fn is_status_operation(request: &ProcessRequest) -> bool {
    request.args.last().and_then(|argument| argument.to_str())
        == Some(HostOperation::Status.command())
}

fn assert_log_chunk_call(request: &ProcessRequest, job_id: JobId, stream: LogStream, offset: u64) {
    assert_control_call(request, HostOperation::LogChunk, Duration::from_secs(30));
    let parsed: LogChunkRequest =
        serde_json::from_slice(request.stdin.as_deref().unwrap()).unwrap();
    assert_eq!(parsed.job_id(), job_id);
    assert_eq!(parsed.stream(), stream);
    assert_eq!(parsed.offset(), offset);
    assert_eq!(parsed.limit(), LOG_CHUNK_LIMIT);
}

fn assert_no_mutating_control(requests: &[ProcessRequest]) {
    for request in requests {
        let command = request.args.last().and_then(|argument| argument.to_str());
        assert_ne!(command, Some(HostOperation::LeaseAcquire.command()));
        assert_ne!(command, Some(HostOperation::SnapshotVerify.command()));
        assert_ne!(command, Some(HostOperation::Submit.command()));
        assert_ne!(command, Some(HostOperation::ResolveOrAbandon.command()));
        assert!(command.is_none_or(|value| !value.contains("cancel") && !value.contains("signal")));
    }
}

fn parse_json_events(bytes: &[u8]) -> Vec<JsonEvent> {
    let text = std::str::from_utf8(bytes).unwrap();
    text.split_inclusive('\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            assert!(line.ends_with('\n'), "JSON log events must be NDJSON");
            serde_json::from_str::<JsonEvent>(line.trim_end()).unwrap()
        })
        .collect()
}

#[test]
fn human_logs_preserve_raw_stdout_and_stderr_bytes() {
    // Catches UTF-8 conversion, prefixes, extra newlines, or mixing the two
    // streams when the remote payload contains arbitrary non-text bytes.
    let raw_stdout = [0xff, 0xfe, 0x00, b'\n', 0x80, b'A'];
    let raw_stderr = [0xc0, 0x00, b'\r', 0x81];
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let runner = RecordingRunner::returning(vec![
        status_result(&log_status_response(
            &record,
            JobStatus::accepted(101).unwrap(),
        )),
        log_chunk_result(LogStream::Stdout, 0, &raw_stdout),
        log_chunk_result(LogStream::Stderr, 0, &raw_stderr),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = PanicFollowRuntime;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();

    assert_eq!(stdout, raw_stdout);
    assert_eq!(stderr, raw_stderr);
    assert_eq!(response.status().state(), JobState::Accepted);
    assert_no_mutating_control(&runner.requests());
}

#[test]
fn json_logs_emit_only_versioned_base64_ndjson_on_stdout() {
    // Catches emitting application bytes, leaking planted plaintext before
    // base64 decode, writing diagnostics to stderr, or pretty-printing events.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let terminal = JobStatus::succeeded(
        110,
        PLANTED_STDOUT_TEXT.len() as u64,
        PLANTED_STDERR_TEXT.len() as u64,
    )
    .unwrap();
    let expected = log_status_response(&record, terminal);
    let runner = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, PLANTED_STDOUT_TEXT),
        log_chunk_result(LogStream::Stderr, 0, PLANTED_STDERR_TEXT),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = PanicFollowRuntime;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            true,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();

    assert!(stderr.is_empty());
    let raw = std::str::from_utf8(&stdout).unwrap();
    assert!(!raw.contains("PLANTED_STDOUT_SECRET"));
    assert!(!raw.contains("PLANTED_STDERR_SECRET"));
    let events = parse_json_events(&stdout);
    assert_eq!(events.len(), 3);
    match &events[0] {
        JsonEvent::Log {
            protocol_version,
            chunk,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(chunk.stream(), LogStream::Stdout);
            assert_eq!(chunk.offset(), 0);
            assert_eq!(chunk.next_offset(), PLANTED_STDOUT_TEXT.len() as u64);
            assert_eq!(chunk.decoded_bytes().unwrap(), PLANTED_STDOUT_TEXT);
        }
        other => panic!("first JSON event must be a log chunk, got {other:?}"),
    }
    match &events[1] {
        JsonEvent::Log {
            protocol_version,
            chunk,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(chunk.stream(), LogStream::Stderr);
            assert_eq!(chunk.decoded_bytes().unwrap(), PLANTED_STDERR_TEXT);
        }
        other => panic!("second JSON event must be a log chunk, got {other:?}"),
    }
    match &events[2] {
        JsonEvent::Status {
            protocol_version,
            response: status,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(status.as_ref(), &expected);
        }
        other => panic!("final JSON event must be the original status, got {other:?}"),
    }
    assert_eq!(response, expected);
    assert!(stderr.is_empty());
}

#[test]
fn non_follow_logs_make_one_status_and_one_query_per_stream_without_sleeping() {
    // Catches extra polls, implicit follow, sleeping, or coupling the two
    // stream offsets on a one-shot snapshot.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let expected = log_status_response(&record, JobStatus::accepted(101).unwrap());
    let runner = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b""),
        log_chunk_result(LogStream::Stderr, 0, b""),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = PanicFollowRuntime;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();

    assert_eq!(response, expected);
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());
    let requests = runner.requests();
    assert_eq!(requests.len(), 3);
    assert_status_query(&requests[0], record.meta().job_id());
    assert_log_chunk_call(&requests[1], record.meta().job_id(), LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], record.meta().job_id(), LogStream::Stderr, 0);
    assert_no_mutating_control(&requests);
    assert_eq!(
        store
            .load_job(record.meta().job_id())
            .unwrap()
            .last_status()
            .unwrap()
            .updated_at_millis(),
        101
    );

    let zero_temp = tempfile::tempdir().unwrap();
    let zero_store = state_store(&zero_temp);
    let zero_record = persist_log_job(&zero_store, Some(JobStatus::accepted(101).unwrap()));
    let zero_terminal = log_status_response(&zero_record, JobStatus::succeeded(140, 0, 0).unwrap());
    let zero_runner = RecordingRunner::returning(vec![
        status_result(&zero_terminal),
        log_chunk_result(LogStream::Stdout, 0, b""),
        log_chunk_result(LogStream::Stderr, 0, b""),
    ]);
    let zero_remote = RemoteJobClient::new(&zero_runner);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let response = logs_service(&config, &zero_store, &zero_remote, &runtime)
        .stream(
            zero_record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
    assert_eq!(response, zero_terminal);
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());
    let requests = zero_runner.requests();
    assert_eq!(requests.len(), 3);
    assert_status_query(&requests[0], zero_record.meta().job_id());
    assert_log_chunk_call(
        &requests[1],
        zero_record.meta().job_id(),
        LogStream::Stdout,
        0,
    );
    assert_log_chunk_call(
        &requests[2],
        zero_record.meta().job_id(),
        LogStream::Stderr,
        0,
    );
    assert_no_mutating_control(&requests);
}

#[test]
fn follow_uses_independent_offsets_and_one_second_empty_polling() {
    // Catches shared offsets, sleeping after asymmetric progress, or skipping
    // status-first polling while either stream is still live.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let live = log_status_response(&record, JobStatus::accepted(101).unwrap());
    let terminal = log_status_response(&record, JobStatus::succeeded(120, 3, 2).unwrap());
    let runner = RecordingRunner::returning(vec![
        status_result(&live),
        log_chunk_result(LogStream::Stdout, 0, b""),
        log_chunk_result(LogStream::Stderr, 0, b""),
        status_result(&live),
        log_chunk_result(LogStream::Stdout, 0, b"abc"),
        log_chunk_result(LogStream::Stderr, 0, b""),
        status_result(&live),
        log_chunk_result(LogStream::Stdout, 3, b""),
        log_chunk_result(LogStream::Stderr, 0, b"de"),
        status_result(&terminal),
        log_chunk_result(LogStream::Stdout, 3, b""),
        log_chunk_result(LogStream::Stderr, 2, b""),
        status_result(&terminal),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = JobFollower::follow(
        &logs_service(&config, &store, &remote, &runtime),
        &record,
        false,
        &mut stdout,
        &mut stderr,
    )
    .unwrap();

    assert_eq!(stdout, b"abc");
    assert_eq!(stderr, b"de");
    assert_eq!(response, terminal);
    assert_eq!(runtime.sleeps(), [Duration::from_secs(1)]);
    let requests = runner.requests();
    assert_eq!(requests.len(), 13);
    assert_status_query(&requests[0], record.meta().job_id());
    assert_log_chunk_call(&requests[1], record.meta().job_id(), LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], record.meta().job_id(), LogStream::Stderr, 0);
    assert_status_query(&requests[3], record.meta().job_id());
    assert_log_chunk_call(&requests[4], record.meta().job_id(), LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[5], record.meta().job_id(), LogStream::Stderr, 0);
    assert_status_query(&requests[6], record.meta().job_id());
    assert_log_chunk_call(&requests[7], record.meta().job_id(), LogStream::Stdout, 3);
    assert_log_chunk_call(&requests[8], record.meta().job_id(), LogStream::Stderr, 0);
    assert_status_query(&requests[9], record.meta().job_id());
    assert_log_chunk_call(&requests[10], record.meta().job_id(), LogStream::Stdout, 3);
    assert_log_chunk_call(&requests[11], record.meta().job_id(), LogStream::Stderr, 2);
    assert_status_query(&requests[12], record.meta().job_id());
    assert_no_mutating_control(&requests);
}

#[test]
fn terminal_follow_drains_to_exact_lengths_confirms_both_eofs_and_revalidates_status() {
    // Catches stopping at the recorded length without the extra empty EOF
    // probes, querying a drained stream again, or reconstructing status.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let terminal = log_status_response(&record, JobStatus::succeeded(140, 5, 3).unwrap());
    let runner = RecordingRunner::returning(vec![
        status_result(&terminal),
        log_chunk_result(LogStream::Stdout, 0, b"hello"),
        log_chunk_result(LogStream::Stderr, 0, b"err"),
        log_chunk_result(LogStream::Stdout, 5, b""),
        log_chunk_result(LogStream::Stderr, 3, b""),
        status_result(&terminal),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = logs_service(&config, &store, &remote, &runtime)
        .stream(record.meta().job_id(), true, true, &mut stdout, &mut stderr)
        .unwrap();

    assert!(runtime.sleeps().is_empty());
    assert_eq!(response, terminal);
    let events = parse_json_events(&stdout);
    assert_eq!(events.len(), 5);
    assert!(matches!(
        &events[0],
        JsonEvent::Log { chunk, .. } if chunk.stream() == LogStream::Stdout && chunk.next_offset() == 5
    ));
    assert!(matches!(
        &events[1],
        JsonEvent::Log { chunk, .. } if chunk.stream() == LogStream::Stderr && chunk.next_offset() == 3
    ));
    assert!(matches!(
        &events[2],
        JsonEvent::Log { chunk, .. } if chunk.stream() == LogStream::Stdout && chunk.offset() == 5 && chunk.next_offset() == 5
    ));
    assert!(matches!(
        &events[3],
        JsonEvent::Log { chunk, .. } if chunk.stream() == LogStream::Stderr && chunk.offset() == 3 && chunk.next_offset() == 3
    ));
    match &events[4] {
        JsonEvent::Status { response, .. } => assert_eq!(response.as_ref(), &terminal),
        other => panic!("final event must be the original terminal status, got {other:?}"),
    }
    assert!(stderr.is_empty());
    let requests = runner.requests();
    assert_eq!(requests.len(), 6);
    assert_status_query(&requests[0], record.meta().job_id());
    assert_log_chunk_call(&requests[1], record.meta().job_id(), LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], record.meta().job_id(), LogStream::Stderr, 0);
    assert_log_chunk_call(&requests[3], record.meta().job_id(), LogStream::Stdout, 5);
    assert_log_chunk_call(&requests[4], record.meta().job_id(), LogStream::Stderr, 3);
    assert_status_query(&requests[5], record.meta().job_id());
    assert_no_mutating_control(&requests);
}

#[test]
fn terminal_status_meta_or_lengths_cannot_change_during_drain() {
    // Catches accepting a mutated terminal status after EOF, or leaking
    // crossing bytes when a chunk exceeds the bound authoritative length.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let terminal = log_status_response(&record, JobStatus::succeeded(150, 3, 0).unwrap());
    let crossing = RecordingRunner::returning(vec![
        status_result(&terminal),
        log_chunk_result(LogStream::Stdout, 0, b"abcd"),
    ]);
    let remote = RemoteJobClient::new(&crossing);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            true,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 70);
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());
    assert_no_mutating_control(&crossing.requests());

    let mutated = record_with_meta_parts(
        record.meta().job_id(),
        store.client_id(),
        "mini-1",
        &"a".repeat(64),
        &"b".repeat(64),
        &"d".repeat(64),
        "packages/app",
        30_000,
        "heavy",
        CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        100,
        Some(record.lease_token()),
        Some(JobStatus::succeeded(150, 3, 0).unwrap()),
        RemoteUncertainty::None,
    );
    assert_ne!(mutated.meta(), record.meta());
    let changed = StatusResponse::new(
        mutated.meta().clone(),
        JobStatus::succeeded(150, 3, 0).unwrap(),
    )
    .unwrap();
    let revalidate = RecordingRunner::returning(vec![
        status_result(&terminal),
        log_chunk_result(LogStream::Stdout, 0, b"abc"),
        log_chunk_result(LogStream::Stderr, 0, b""),
        log_chunk_result(LogStream::Stdout, 3, b""),
        status_result(&changed),
    ]);
    let remote = RemoteJobClient::new(&revalidate);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            true,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 70);
    assert_eq!(stdout, b"abc");
    assert!(stderr.is_empty());
    let requests = revalidate.requests();
    assert_eq!(requests.len(), 5);
    assert_log_chunk_call(&requests[1], record.meta().job_id(), LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], record.meta().job_id(), LogStream::Stderr, 0);
    assert_log_chunk_call(&requests[3], record.meta().job_id(), LogStream::Stdout, 3);
    assert_status_query(&requests[4], record.meta().job_id());
    assert_no_mutating_control(&requests);
}

#[test]
fn terminal_binary_non_utf8_multi_chunk_logs_drain_to_exact_lengths_in_human_mode() {
    // Catches losing bytes across a multi-chunk boundary, corrupting a
    // non-UTF-8 payload, or stopping before the recorded terminal length.
    let stdout_chunk_a: &[u8] = &[0xc3, 0x28, 0x00, 0xfe, b'A'];
    let stdout_chunk_b: &[u8] = &[0x80, 0x81, b'B', 0xed, 0xa0, 0x80];
    let stderr_chunk_a: &[u8] = &[0xff, 0x00, b'C'];
    let stderr_chunk_b: &[u8] = &[0x80, b'D', 0x00, 0xc0, 0xaf];
    let stdout_total = (stdout_chunk_a.len() + stdout_chunk_b.len()) as u64;
    let stderr_total = (stderr_chunk_a.len() + stderr_chunk_b.len()) as u64;

    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let terminal = log_status_response(
        &record,
        JobStatus::succeeded(140, stdout_total, stderr_total).unwrap(),
    );
    let runner = RecordingRunner::returning(vec![
        status_result(&terminal),
        log_chunk_result(LogStream::Stdout, 0, stdout_chunk_a),
        log_chunk_result(LogStream::Stderr, 0, stderr_chunk_a),
        log_chunk_result(
            LogStream::Stdout,
            stdout_chunk_a.len() as u64,
            stdout_chunk_b,
        ),
        log_chunk_result(
            LogStream::Stderr,
            stderr_chunk_a.len() as u64,
            stderr_chunk_b,
        ),
        log_chunk_result(LogStream::Stdout, stdout_total, b""),
        log_chunk_result(LogStream::Stderr, stderr_total, b""),
        status_result(&terminal),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            true,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();

    assert!(runtime.sleeps().is_empty());
    assert_eq!(response, terminal);
    let mut expected_stdout = stdout_chunk_a.to_vec();
    expected_stdout.extend_from_slice(stdout_chunk_b);
    let mut expected_stderr = stderr_chunk_a.to_vec();
    expected_stderr.extend_from_slice(stderr_chunk_b);
    assert_eq!(stdout, expected_stdout);
    assert_eq!(stderr, expected_stderr);
    assert!(std::str::from_utf8(&stdout).is_err());
    assert!(std::str::from_utf8(&stderr).is_err());
    let requests = runner.requests();
    assert_eq!(requests.len(), 8);
    assert_status_query(&requests[0], record.meta().job_id());
    assert_log_chunk_call(&requests[1], record.meta().job_id(), LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], record.meta().job_id(), LogStream::Stderr, 0);
    assert_log_chunk_call(
        &requests[3],
        record.meta().job_id(),
        LogStream::Stdout,
        stdout_chunk_a.len() as u64,
    );
    assert_log_chunk_call(
        &requests[4],
        record.meta().job_id(),
        LogStream::Stderr,
        stderr_chunk_a.len() as u64,
    );
    assert_log_chunk_call(
        &requests[5],
        record.meta().job_id(),
        LogStream::Stdout,
        stdout_total,
    );
    assert_log_chunk_call(
        &requests[6],
        record.meta().job_id(),
        LogStream::Stderr,
        stderr_total,
    );
    assert_status_query(&requests[7], record.meta().job_id());
    assert_no_mutating_control(&requests);
}

#[test]
fn terminal_binary_non_utf8_multi_chunk_logs_emit_ndjson_base64_events_in_json_mode() {
    // Catches losing bytes across a multi-chunk boundary, corrupting the
    // base64 payload, or emitting anything besides versioned NDJSON stdout.
    let stdout_chunk_a: &[u8] = &[0xff, 0x00, 0x80, b'Q'];
    let stdout_chunk_b: &[u8] = &[0xed, 0xa0, 0x80, 0x00, b'R', 0xfe];
    let stderr_chunk_a: &[u8] = &[0xc0, 0xaf, b'S'];
    let stderr_chunk_b: &[u8] = &[0x00, 0x81, b'T', 0xff];
    let stdout_total = (stdout_chunk_a.len() + stdout_chunk_b.len()) as u64;
    let stderr_total = (stderr_chunk_a.len() + stderr_chunk_b.len()) as u64;

    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let terminal = log_status_response(
        &record,
        JobStatus::succeeded(150, stdout_total, stderr_total).unwrap(),
    );
    let runner = RecordingRunner::returning(vec![
        status_result(&terminal),
        log_chunk_result(LogStream::Stdout, 0, stdout_chunk_a),
        log_chunk_result(LogStream::Stderr, 0, stderr_chunk_a),
        log_chunk_result(
            LogStream::Stdout,
            stdout_chunk_a.len() as u64,
            stdout_chunk_b,
        ),
        log_chunk_result(
            LogStream::Stderr,
            stderr_chunk_a.len() as u64,
            stderr_chunk_b,
        ),
        log_chunk_result(LogStream::Stdout, stdout_total, b""),
        log_chunk_result(LogStream::Stderr, stderr_total, b""),
        status_result(&terminal),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let response = logs_service(&config, &store, &remote, &runtime)
        .stream(record.meta().job_id(), true, true, &mut stdout, &mut stderr)
        .unwrap();

    assert!(runtime.sleeps().is_empty());
    assert!(stderr.is_empty());
    assert_eq!(response, terminal);
    let raw = std::str::from_utf8(&stdout).expect("NDJSON stdout must remain valid UTF-8 text");
    assert!(!raw.contains('\u{fffd}'));
    let events = parse_json_events(&stdout);
    assert_eq!(events.len(), 7);

    let mut decoded_stdout = Vec::new();
    let mut decoded_stderr = Vec::new();
    for event in &events[..6] {
        match event {
            JsonEvent::Log {
                protocol_version,
                chunk,
            } => {
                assert_eq!(*protocol_version, PROTOCOL_VERSION);
                match chunk.stream() {
                    LogStream::Stdout => {
                        decoded_stdout.extend_from_slice(&chunk.decoded_bytes().unwrap());
                    }
                    LogStream::Stderr => {
                        decoded_stderr.extend_from_slice(&chunk.decoded_bytes().unwrap());
                    }
                }
            }
            other => panic!("expected a log chunk event, got {other:?}"),
        }
    }
    let mut expected_stdout = stdout_chunk_a.to_vec();
    expected_stdout.extend_from_slice(stdout_chunk_b);
    let mut expected_stderr = stderr_chunk_a.to_vec();
    expected_stderr.extend_from_slice(stderr_chunk_b);
    assert_eq!(decoded_stdout, expected_stdout);
    assert_eq!(decoded_stderr, expected_stderr);
    match &events[6] {
        JsonEvent::Status {
            protocol_version,
            response: status,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(status.as_ref(), &terminal);
        }
        other => panic!("final event must be the terminal status, got {other:?}"),
    }
    let requests = runner.requests();
    assert_eq!(requests.len(), 8);
    assert_no_mutating_control(&requests);
}

#[test]
fn initial_compatible_concurrent_forward_binds_and_emits_winner() {
    // Catches binding/emitting the in-flight remote snapshot when lock-internal
    // reconciliation already selected a compatible concurrent-forward winner.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let remote_live = log_status_response(&record, running_status(110));
    let winner_status = running_status(110).into_succeeded(120, 3, 0).unwrap();
    let hook_store = store.clone();
    let hook_status = winner_status.clone();
    let job_id = record.meta().job_id();
    let crossing = RecordingRunner::with_hook(
        vec![
            status_result(&remote_live),
            log_chunk_result(LogStream::Stdout, 0, b"abcd"),
        ],
        move |request| {
            if !is_status_operation(request) {
                return;
            }
            hook_store
                .update_observation(job_id, hook_status.clone())
                .unwrap();
        },
    );
    let remote = RemoteJobClient::new(&crossing);
    let config = config();
    let runtime = PanicFollowRuntime;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(job_id, false, false, &mut stdout, &mut stderr)
        .unwrap_err();
    assert_eq!(error.exit_code(), 70);
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());
    assert_eq!(
        store.load_job(job_id).unwrap().last_status(),
        Some(&winner_status)
    );
    assert_eq!(crossing.requests().len(), 2);
    assert_log_chunk_call(&crossing.requests()[1], job_id, LogStream::Stdout, 0);
    assert_no_mutating_control(&crossing.requests());

    let emit_temp = tempfile::tempdir().unwrap();
    let emit_store = state_store(&emit_temp);
    let emit_record = persist_log_job(&emit_store, Some(JobStatus::accepted(101).unwrap()));
    let emit_live = log_status_response(&emit_record, running_status(110));
    let emit_winner_status = running_status(110).into_succeeded(120, 3, 0).unwrap();
    let emit_winner = log_status_response(&emit_record, emit_winner_status.clone());
    let hook_store = emit_store.clone();
    let hook_status = emit_winner_status.clone();
    let emit_id = emit_record.meta().job_id();
    let emit = RecordingRunner::with_hook(
        vec![
            status_result(&emit_live),
            log_chunk_result(LogStream::Stdout, 0, b"abc"),
            log_chunk_result(LogStream::Stderr, 0, b""),
        ],
        move |request| {
            if !is_status_operation(request) {
                return;
            }
            hook_store
                .update_observation(emit_id, hook_status.clone())
                .unwrap();
        },
    );
    let remote = RemoteJobClient::new(&emit);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let response = logs_service(&config, &emit_store, &remote, &runtime)
        .stream(emit_id, false, true, &mut stdout, &mut stderr)
        .unwrap();
    assert_eq!(response, emit_winner);
    assert_eq!(
        emit_store.load_job(emit_id).unwrap().last_status(),
        Some(&emit_winner_status)
    );
    assert!(stderr.is_empty());
    let events = parse_json_events(&stdout);
    assert_eq!(events.len(), 3);
    match &events[2] {
        JsonEvent::Status {
            response: status, ..
        } => assert_eq!(status.as_ref(), &emit_winner),
        other => panic!("final JSON event must be the concurrent-forward winner, got {other:?}"),
    }
    let requests = emit.requests();
    assert_eq!(requests.len(), 3);
    assert_log_chunk_call(&requests[1], emit_id, LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], emit_id, LogStream::Stderr, 0);
    assert_no_mutating_control(&requests);
}

#[test]
fn final_compatible_concurrent_forward_revalidates_actual_winner() {
    // Catches revalidating/emitting the stale final remote snapshot when
    // reconciliation selected a compatible cleanup-enriched winner.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let terminal_status = JobStatus::succeeded(150, 3, 0).unwrap();
    let terminal = log_status_response(&record, terminal_status.clone());
    let enriched_status = terminal_status
        .with_cleanup_error("LATE_CLEANUP_ERROR".into(), 151)
        .unwrap();
    let job_id = record.meta().job_id();
    let hook_store = store.clone();
    let hook_status = enriched_status.clone();
    let status_calls = AtomicUsize::new(0);
    let runner = RecordingRunner::with_hook(
        vec![
            status_result(&terminal),
            log_chunk_result(LogStream::Stdout, 0, b"abc"),
            log_chunk_result(LogStream::Stderr, 0, b""),
            log_chunk_result(LogStream::Stdout, 3, b""),
            status_result(&terminal),
        ],
        move |request| {
            if !is_status_operation(request) {
                return;
            }
            if status_calls.fetch_add(1, Ordering::SeqCst) == 1 {
                hook_store
                    .update_observation(job_id, hook_status.clone())
                    .unwrap();
            }
        },
    );
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(job_id, true, false, &mut stdout, &mut stderr)
        .unwrap_err();
    assert_eq!(error.exit_code(), 70);
    assert_eq!(stdout, b"abc");
    assert!(stderr.is_empty());
    assert_eq!(
        store.load_job(job_id).unwrap().last_status(),
        Some(&enriched_status)
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 5);
    assert_status_query(&requests[0], job_id);
    assert_log_chunk_call(&requests[1], job_id, LogStream::Stdout, 0);
    assert_log_chunk_call(&requests[2], job_id, LogStream::Stderr, 0);
    assert_log_chunk_call(&requests[3], job_id, LogStream::Stdout, 3);
    assert_status_query(&requests[4], job_id);
    assert_no_mutating_control(&requests);
}

#[test]
fn broken_stdout_or_stderr_is_io_74_and_never_cancels() {
    // Catches treating a local writer failure as a remote mutation, or
    // committing a cursor after a failed flush.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let expected = log_status_response(&record, JobStatus::accepted(101).unwrap());
    let before = store.load_job(record.meta().job_id()).unwrap();

    let write_fail = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b"hello"),
    ]);
    let remote = RemoteJobClient::new(&write_fail);
    let config = config();
    let runtime = PanicFollowRuntime;
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut stdout = RecordingWriter::failing(Arc::clone(&output));
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 74);
    assert!(output.lock().unwrap().bytes.is_empty());
    assert!(stderr.is_empty());
    assert_eq!(write_fail.requests().len(), 2);
    assert_no_mutating_control(&write_fail.requests());
    let after_write_fail = store.load_job(record.meta().job_id()).unwrap();
    assert_eq!(after_write_fail.meta(), before.meta());
    assert_eq!(after_write_fail.lease_token(), before.lease_token());
    assert_eq!(
        after_write_fail.remote_uncertainty(),
        before.remote_uncertainty()
    );

    let flush_fail = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b"hello"),
    ]);
    let remote = RemoteJobClient::new(&flush_fail);
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut stdout = RecordingWriter::failing_flush(Arc::clone(&output));
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 74);
    assert!(matches!(
        error,
        WorkerError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe
    ));
    assert_eq!(output.lock().unwrap().bytes, b"hello");
    assert_eq!(flush_fail.requests().len(), 2);
    assert_log_chunk_call(
        &flush_fail.requests()[1],
        record.meta().job_id(),
        LogStream::Stdout,
        0,
    );
    assert_no_mutating_control(&flush_fail.requests());
    let after_flush_fail = store.load_job(record.meta().job_id()).unwrap();
    assert_eq!(after_flush_fail.meta(), before.meta());
    assert_eq!(after_flush_fail.lease_token(), before.lease_token());
    assert_eq!(
        after_flush_fail.remote_uncertainty(),
        before.remote_uncertainty()
    );

    let retry = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b"hello"),
        log_chunk_result(LogStream::Stderr, 0, b""),
    ]);
    let remote = RemoteJobClient::new(&retry);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
    assert_eq!(stdout, b"hello");
    assert!(stderr.is_empty());
    assert_log_chunk_call(
        &retry.requests()[1],
        record.meta().job_id(),
        LogStream::Stdout,
        0,
    );
    assert_log_chunk_call(
        &retry.requests()[2],
        record.meta().job_id(),
        LogStream::Stderr,
        0,
    );

    let eof_temp = tempfile::tempdir().unwrap();
    let eof_store = state_store(&eof_temp);
    let eof_record = persist_log_job(&eof_store, Some(JobStatus::accepted(101).unwrap()));
    let eof_flush = RecordingRunner::returning(vec![
        status_result(&log_status_response(
            &eof_record,
            JobStatus::succeeded(180, 0, 0).unwrap(),
        )),
        log_chunk_result(LogStream::Stdout, 0, b""),
    ]);
    let remote = RemoteJobClient::new(&eof_flush);
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut stdout = RecordingWriter::failing_flush(Arc::clone(&output));
    let mut stderr = Vec::new();
    let error = logs_service(&config, &eof_store, &remote, &runtime)
        .stream(
            eof_record.meta().job_id(),
            true,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 74);
    assert!(matches!(
        error,
        WorkerError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe
    ));
    assert!(output.lock().unwrap().bytes.is_empty());
    assert_eq!(eof_flush.requests().len(), 2);
    assert_log_chunk_call(
        &eof_flush.requests()[1],
        eof_record.meta().job_id(),
        LogStream::Stdout,
        0,
    );
    assert_no_mutating_control(&eof_flush.requests());

    let json_write_fail = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b"hello"),
    ]);
    let remote = RemoteJobClient::new(&json_write_fail);
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut stdout = RecordingWriter::failing(Arc::clone(&output));
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            true,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 74);
    assert!(matches!(
        error,
        WorkerError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe
    ));
    assert!(output.lock().unwrap().bytes.is_empty());
    assert!(stderr.is_empty());
    assert_eq!(json_write_fail.requests().len(), 2);
    assert_no_mutating_control(&json_write_fail.requests());

    let json_flush_fail = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b"hello"),
    ]);
    let remote = RemoteJobClient::new(&json_flush_fail);
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut stdout = RecordingWriter::failing_flush(Arc::clone(&output));
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            true,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 74);
    assert!(matches!(
        error,
        WorkerError::Io(ref io) if io.kind() == io::ErrorKind::BrokenPipe
    ));
    let json_bytes = output.lock().unwrap().bytes.clone();
    assert!(json_bytes.ends_with(b"\n"));
    assert!(matches!(
        serde_json::from_slice::<JsonEvent>(json_bytes.strip_suffix(b"\n").unwrap()).unwrap(),
        JsonEvent::Log { .. }
    ));
    assert!(stderr.is_empty());
    assert_eq!(json_flush_fail.requests().len(), 2);

    let stderr_fail = RecordingRunner::returning(vec![
        status_result(&expected),
        log_chunk_result(LogStream::Stdout, 0, b"ok"),
        log_chunk_result(LogStream::Stderr, 0, b"nope"),
    ]);
    let remote = RemoteJobClient::new(&stderr_fail);
    let mut stdout = Vec::new();
    let output = Arc::new(Mutex::new(OutputState::default()));
    let mut stderr = RecordingWriter::failing(Arc::clone(&output));
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            false,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();
    assert_eq!(error.exit_code(), 74);
    assert_eq!(stdout, b"ok");
    assert!(output.lock().unwrap().bytes.is_empty());
    assert_eq!(stderr_fail.requests().len(), 3);
    assert_no_mutating_control(&stderr_fail.requests());
}

#[test]
fn terminal_exit_codes_0_7_64_and_signal_143_are_exact() {
    // Catches remapping established command bytes through ExitKind or losing
    // an exact code when cleanup enrichment is present.
    let cases = [
        (JobStatus::succeeded(160, 0, 0).unwrap(), Ok(0)),
        (
            JobStatus::failed(161, 7, 0, 0)
                .unwrap()
                .with_cleanup_error("REMOTE_CLEANUP_FAILED".into(), 162)
                .unwrap(),
            Ok(7),
        ),
        (JobStatus::failed(163, 64, 0, 0).unwrap(), Ok(64)),
        (JobStatus::failed(164, 255, 0, 0).unwrap(), Ok(255)),
        (signal_status(165, 15, 0, 0), Ok(143)),
        (
            JobStatus::succeeded(166, 0, 0)
                .unwrap()
                .with_cleanup_error("REMOTE_CLEANUP_FAILED".into(), 167)
                .unwrap(),
            Ok(0),
        ),
        (signal_status(168, 128, 0, 0), Err(70)),
    ];

    for (status, _expected) in cases {
        let temp = tempfile::tempdir().unwrap();
        let store = state_store(&temp);
        let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
        let response = log_status_response(&record, status.clone());
        let runner = RecordingRunner::returning(vec![
            status_result(&response),
            log_chunk_result(LogStream::Stdout, 0, b""),
            log_chunk_result(LogStream::Stderr, 0, b""),
        ]);
        let remote = RemoteJobClient::new(&runner);
        let config = config();
        let runtime = PanicFollowRuntime;
        let streamed = logs_service(&config, &store, &remote, &runtime)
            .stream(
                record.meta().job_id(),
                false,
                false,
                &mut Vec::new(),
                &mut Vec::new(),
            )
            .unwrap();
        assert_eq!(streamed.status(), &status);
        assert_no_mutating_control(&runner.requests());
    }
}

#[test]
fn disconnect_and_default_sigint_require_no_cancel_or_signal_dependency() {
    // Catches inventing cancellation, installing a signal hook, or treating a
    // transport disconnect as a remote state change. Executable SIGINT belongs
    // to Slice 9.5; this slice proves the service boundary only.
    let _system_runtime = SystemFollowRuntime;
    let source = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/run.rs"));
    assert!(!source.contains("SIGINT"));
    assert!(!source.contains("sigaction"));
    assert!(!source.contains("signal_hook"));
    assert!(!source.contains("ctrlc"));
    let cargo = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
    assert!(!cargo.contains("signal-hook"));
    assert!(!cargo.contains("ctrlc"));

    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let before = store.load_job(record.meta().job_id()).unwrap();
    let runner = RecordingRunner::returning(vec![
        status_result(&log_status_response(&record, running_status(111))),
        log_chunk_result(LogStream::Stdout, 0, b"partial"),
        transport_failure(),
    ]);
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = RecordingFollowRuntime::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(
            record.meta().job_id(),
            true,
            false,
            &mut stdout,
            &mut stderr,
        )
        .unwrap_err();

    assert_eq!(error.exit_code(), 69);
    assert_eq!(stdout, b"partial");
    assert!(stderr.is_empty());
    assert!(runtime.sleeps().is_empty());
    assert_no_mutating_control(&runner.requests());
    let persisted = store.load_job(record.meta().job_id()).unwrap();
    assert_eq!(persisted.meta(), before.meta());
    assert_eq!(persisted.lease_token(), before.lease_token());
    assert_eq!(persisted.remote_uncertainty(), before.remote_uncertainty());
    assert_ne!(persisted.last_status(), before.last_status());
}

#[test]
fn logs_requires_the_exact_local_job_and_never_falls_back() {
    // Catches implicit newest-job selection, following a different local
    // identity, or probing a worker that is not the recorded one.
    let temp = tempfile::tempdir().unwrap();
    let store = state_store(&temp);
    let newest = test_record(
        &store,
        2,
        200,
        Some(JobStatus::succeeded(200, 0, 0).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(newest.clone()).unwrap();
    let runner = RecordingRunner::returning(Vec::new());
    let remote = RemoteJobClient::new(&runner);
    let config = config();
    let runtime = PanicFollowRuntime;
    let missing = JobId::new(uuid::Uuid::from_u128(99));
    let error = logs_service(&config, &store, &remote, &runtime)
        .stream(missing, false, false, &mut Vec::new(), &mut Vec::new())
        .unwrap_err();
    assert!(matches!(error, WorkerError::Config(message) if message.contains("JOB_NOT_FOUND")));
    assert!(runner.requests().is_empty());

    let target = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let empty_config = Config {
        version: 1,
        workers: Vec::new(),
    };
    let error = logs_service(&empty_config, &store, &remote, &runtime)
        .stream(
            target.meta().job_id(),
            false,
            false,
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(matches!(error, WorkerError::Config(message) if message.contains("WORKER_NOT_FOUND")));
    assert!(runner.requests().is_empty());

    let mismatched = LocalJobRecord::new(
        target.meta().clone(),
        LeaseToken::new(uuid::Uuid::from_u128(99_999)),
        target.last_status().cloned(),
        RemoteUncertainty::None,
    )
    .unwrap();
    let error = JobFollower::follow(
        &logs_service(&config, &store, &remote, &runtime),
        &mismatched,
        false,
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(matches!(error, WorkerError::Protocol(message) if message.contains("JOB_ID_CONFLICT")));
    assert!(runner.requests().is_empty());
}

const PUBLIC_INVENTORY: &str =
    "version = 1\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n";
const PUBLIC_STDOUT_BYTES: &[u8] = b"run-stdout";
const PUBLIC_STDERR_BYTES: &[u8] = b"run-stderr";
const HIDDEN_HOST_FRAGMENTS: &[&str] = &[
    "lease-acquire",
    "log-chunk",
    "resolve-or-abandon",
    "rsync-receive",
    "snapshot-verify",
    "host supervise",
    "host submit",
];

struct PublicRuntime {
    config: PathBuf,
    state: PathBuf,
    context: RuntimeContext,
}

struct FailOnJsonErrorEvent {
    bytes: Vec<u8>,
}

impl Write for FailOnJsonErrorEvent {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes
            .windows(b"\"event\":\"error\"".len())
            .any(|window| window == b"\"event\":\"error\"")
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "planted error-event write failure",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn public_runtime(root: &Path, current_dir: &Path) -> PublicRuntime {
    let root = root.canonicalize().unwrap();
    let current_dir = current_dir.canonicalize().unwrap();
    let home = root.join("runtime-home");
    let config_home = root.join("xdg-config");
    let state_home = root.join("xdg-state");
    let cache_home = root.join("xdg-cache");
    let data_home = root.join("xdg-data");
    fs::create_dir_all(&home).unwrap();
    let config = root.join("config.toml");
    fs::write(&config, PUBLIC_INVENTORY).unwrap();
    let environment = BTreeMap::from([
        (
            OsString::from("XDG_CONFIG_HOME"),
            config_home.into_os_string(),
        ),
        (
            OsString::from("XDG_STATE_HOME"),
            state_home.into_os_string(),
        ),
        (
            OsString::from("XDG_CACHE_HOME"),
            cache_home.into_os_string(),
        ),
        (OsString::from("XDG_DATA_HOME"), data_home.into_os_string()),
    ]);
    PublicRuntime {
        config,
        state: root.join("xdg-state/mac-worker"),
        context: RuntimeContext::isolated(environment, home, current_dir.to_path_buf()),
    }
}

fn dispatch(
    cli: Cli,
    runner: &dyn ProcessRunner,
    runtime: &RuntimeContext,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    run_with_io_in_context(cli, runner, runtime, stdout, stderr)
}

fn public_cli(config: PathBuf, json: bool, command: WorkerCommand) -> Cli {
    Cli {
        config: Some(config),
        json,
        command,
    }
}

fn persist_public_job(state: &Path, status: Option<JobStatus>) -> LocalJobRecord {
    let store = ClientStateStore::open(state).unwrap();
    persist_log_job(&store, status)
}

fn expected_status_line(row: &StatusRow) -> String {
    let state = row.status.as_ref().map_or_else(
        || "unknown".to_owned(),
        |status| {
            match status.state() {
                JobState::Uploading => "uploading",
                JobState::Verified => "verified",
                JobState::Accepted => "accepted",
                JobState::Running => "running",
                JobState::Succeeded => "succeeded",
                JobState::Failed => "failed",
                JobState::Cancelled => "cancelled",
                JobState::TimedOut => "timed_out",
                JobState::Lost => "lost",
            }
            .to_owned()
        },
    );
    let uncertainty = match &row.remote_uncertainty {
        RemoteUncertainty::None => "none".to_owned(),
        RemoteUncertainty::UnknownRemote { code } => format!("unknown_remote {code}"),
        RemoteUncertainty::CleanupPending { code } => format!("cleanup_pending {code}"),
    };
    let command = match row.command_summary.arg_count() {
        Some(count) => format!("argv {count}"),
        None => "shell".into(),
    };
    format!(
        "{} {} {state} {uncertainty} {} {} {command}",
        row.job_id, row.worker, row.created_at_millis, row.manifest_digest
    )
}

fn expected_human_status(report: &StatusReport) -> String {
    let mut lines = report
        .jobs
        .iter()
        .map(expected_status_line)
        .collect::<Vec<_>>();
    if report.omitted > 0 {
        lines.push(format!("{} older jobs omitted", report.omitted));
    }
    lines.join("\n")
}

fn assert_no_hidden_host_leak(stdout: &[u8], stderr: &[u8]) {
    let combined = [stdout, stderr].concat();
    let text = String::from_utf8_lossy(&combined);
    for fragment in HIDDEN_HOST_FRAGMENTS {
        assert!(
            !text.contains(fragment),
            "public output leaked hidden host command {fragment}"
        );
    }
}

fn run_terminal_steps(exit_code: u8) -> Vec<RunScriptStep> {
    run_follow_steps(exit_code, b"", b"")
}

fn run_follow_steps(
    exit_code: u8,
    stdout: &'static [u8],
    stderr: &'static [u8],
) -> Vec<RunScriptStep> {
    let stdout_len = stdout.len() as u64;
    let stderr_len = stderr.len() as u64;
    let mut steps = vec![
        RunScriptStep::ProbeReady,
        RunScriptStep::Acquire,
        RunScriptStep::Upload,
        RunScriptStep::Verify,
        RunScriptStep::SubmitAccepted,
        RunScriptStep::StatusTerminal {
            exit_code,
            stdout_bytes: stdout_len,
            stderr_bytes: stderr_len,
        },
    ];
    if !stdout.is_empty() {
        steps.push(RunScriptStep::LogChunkBytes {
            stream: LogStream::Stdout,
            offset: 0,
            bytes: stdout,
        });
    }
    if !stderr.is_empty() {
        steps.push(RunScriptStep::LogChunkBytes {
            stream: LogStream::Stderr,
            offset: 0,
            bytes: stderr,
        });
    }
    steps.push(RunScriptStep::LogChunkBytes {
        stream: LogStream::Stdout,
        offset: stdout_len,
        bytes: b"",
    });
    steps.push(RunScriptStep::LogChunkBytes {
        stream: LogStream::Stderr,
        offset: stderr_len,
        bytes: b"",
    });
    steps.push(RunScriptStep::StatusTerminal {
        exit_code,
        stdout_bytes: stdout_len,
        stderr_bytes: stderr_len,
    });
    steps
}

fn terminal_log_script(
    record: &LocalJobRecord,
    status: JobStatus,
) -> Vec<Result<ProcessResult, WorkerError>> {
    vec![
        status_result(&log_status_response(record, status)),
        log_chunk_result(LogStream::Stdout, 0, b""),
        log_chunk_result(LogStream::Stderr, 0, b""),
    ]
}

#[test]
fn dispatcher_routes_run_and_logs_directly_to_live_writers() {
    // Break caught: public run/logs buffer into CommandOutput/String or skip
    // the live writers, so accepted/log bytes appear only as one document.
    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), repo.root());
    let store = ClientStateStore::open(&runtime.state).unwrap();
    let runner = RunScriptRunner::new(
        runtime.state.canonicalize().unwrap(),
        run_follow_steps(0, PUBLIC_STDOUT_BYTES, PUBLIC_STDERR_BYTES),
    );
    let stdout_state = Arc::new(Mutex::new(OutputState::default()));
    let stderr_state = Arc::new(Mutex::new(OutputState::default()));
    let mut stdout = RecordingWriter::new(stdout_state.clone());
    let mut stderr = RecordingWriter::new(stderr_state.clone());

    let exit = dispatch(
        public_cli(
            runtime.config.clone(),
            false,
            WorkerCommand::Run {
                worker: Some("mini-1".into()),
                no_wait: false,
                project: None,
                includes: Vec::new(),
                timeout: None,
                shell: None,
                argv: vec!["npm".into(), "test".into(), "--".into(), "--literal".into()],
            },
        ),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    let run_stdout = stdout_state.lock().unwrap().bytes.clone();
    let run_stderr = stderr_state.lock().unwrap().bytes.clone();
    let run_flushes = stdout_state.lock().unwrap().flushes;
    assert_eq!(exit, 0);
    let run_text = String::from_utf8(run_stdout.clone()).unwrap();
    let accepted_end = run_text
        .find('\n')
        .map(|index| index + 1)
        .expect("accepted line must be flushed as its own write");
    let accepted = &run_text[..accepted_end];
    assert!(accepted.starts_with("job "));
    assert!(accepted.ends_with(" accepted on mini-1\n"));
    assert!(!accepted.starts_with('{'));
    assert!(!run_text.contains("\"kind\""));
    assert_eq!(&run_stdout[accepted_end..], PUBLIC_STDOUT_BYTES);
    assert_eq!(run_stderr, PUBLIC_STDERR_BYTES);
    assert!(run_flushes >= 1, "accepted output must be flushed live");
    assert_no_hidden_host_leak(&run_stdout, &run_stderr);
    runner.assert_consumed();
    let job_id = accepted
        .trim()
        .strip_prefix("job ")
        .and_then(|line| line.strip_suffix(" accepted on mini-1"))
        .unwrap()
        .parse::<JobId>()
        .unwrap();
    assert_eq!(store.list_jobs().unwrap().len(), 1);

    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let raw_stdout = [0xff, 0xfe, 0x00, b'\n', 0x80];
    let raw_stderr = [0xc0, 0x00, 0x81];
    let logs_runner = RecordingRunner::returning(vec![
        status_result(&log_status_response(
            &record,
            JobStatus::accepted(101).unwrap(),
        )),
        log_chunk_result(LogStream::Stdout, 0, &raw_stdout),
        log_chunk_result(LogStream::Stderr, 0, &raw_stderr),
    ]);
    let mut logs_stdout = Vec::new();
    let mut logs_stderr = Vec::new();

    let logs_exit = dispatch(
        public_cli(
            runtime.config,
            false,
            WorkerCommand::Logs {
                follow: false,
                job_id: record.meta().job_id(),
            },
        ),
        &logs_runner,
        &runtime.context,
        &mut logs_stdout,
        &mut logs_stderr,
    );

    assert_eq!(logs_exit, 0);
    assert_eq!(logs_stdout, raw_stdout);
    assert_eq!(logs_stderr, raw_stderr);
    assert!(!logs_stdout.starts_with(b"{"));
    assert_no_hidden_host_leak(&logs_stdout, &logs_stderr);
    let _ = job_id;
}

#[test]
fn dispatcher_keeps_status_as_one_document_and_other_commands_unchanged() {
    // Break caught: status becomes NDJSON, or setup/doctor/workers/probe leave
    // the CommandOutput document path.
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let store = ClientStateStore::open(&runtime.state).unwrap();
    let record = persist_log_job(&store, Some(JobStatus::accepted(101).unwrap()));
    let expected = StatusRow::try_from_record(&record).unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let status_exit = dispatch(
        public_cli(
            runtime.config.clone(),
            true,
            WorkerCommand::Status {
                job_id: Some(record.meta().job_id()),
            },
        ),
        &RecordingRunner::returning(vec![status_result(&log_status_response(
            &record,
            JobStatus::accepted(101).unwrap(),
        ))]),
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(status_exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let status: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(status["kind"], "status");
    assert_eq!(status["protocol_version"], PROTOCOL_VERSION);
    assert_eq!(status["omitted"], 0);
    assert_eq!(status["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(status["jobs"][0]["job_id"], expected.job_id.to_string());
    assert!(status.get("event").is_none());
    assert!(!stdout.windows(2).any(|pair| pair == b"}\n{"));

    stdout.clear();
    let workers_exit = dispatch(
        public_cli(runtime.config.clone(), true, WorkerCommand::Workers),
        &RecordingRunner::returning(vec![canonical_process(&ready_probe())]),
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(workers_exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let workers: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(workers["kind"], "workers");
    assert_eq!(workers["protocol_version"], PROTOCOL_VERSION);

    stdout.clear();
    let probe_exit = dispatch(
        public_cli(
            runtime.config,
            true,
            WorkerCommand::Host {
                command: mac_worker::cli::HostCommand::Probe,
            },
        ),
        &RecordingRunner::returning(Vec::new()),
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(probe_exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let probe: Value = serde_json::from_slice(&stdout).unwrap();
    assert!(probe.get("kind").is_none());
    assert_eq!(probe["protocol_version"], PROTOCOL_VERSION);
}

#[test]
fn json_stream_errors_use_stdout_only_and_writer_failure_is_74() {
    // Break caught: JSON follow errors go to stderr, emit a second diagnostic,
    // or treat a failed error-event write as the original service exit.
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let record = persist_public_job(&runtime.state, Some(JobStatus::accepted(101).unwrap()));
    let terminal = JobStatus::succeeded(
        110,
        PUBLIC_STDOUT_BYTES.len() as u64,
        PUBLIC_STDERR_BYTES.len() as u64,
    )
    .unwrap();
    let runner = RecordingRunner::returning(vec![
        status_result(&log_status_response(&record, terminal.clone())),
        log_chunk_result(LogStream::Stdout, 0, PUBLIC_STDOUT_BYTES),
        transport_failure(),
    ]);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = dispatch(
        public_cli(
            runtime.config.clone(),
            true,
            WorkerCommand::Logs {
                follow: false,
                job_id: record.meta().job_id(),
            },
        ),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 69);
    assert!(stderr.is_empty());
    let events = parse_json_events(&stdout);
    assert_eq!(events.len(), 2);
    match &events[0] {
        JsonEvent::Log {
            protocol_version,
            chunk,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(chunk.stream(), LogStream::Stdout);
            assert_eq!(chunk.decoded_bytes().unwrap(), PUBLIC_STDOUT_BYTES);
        }
        other => panic!("first JSON stream event must be the log chunk, got {other:?}"),
    }
    match &events[1] {
        JsonEvent::Error {
            protocol_version,
            code,
            message,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(code, "SSH_UNAVAILABLE");
            assert_eq!(message, "transport error");
        }
        other => panic!("JSON stream error must be one error event, got {other:?}"),
    }
    assert!(!String::from_utf8_lossy(&stdout).contains("mac1"));

    let fail_runner = RecordingRunner::returning(vec![
        status_result(&log_status_response(&record, terminal)),
        log_chunk_result(LogStream::Stdout, 0, PUBLIC_STDOUT_BYTES),
        transport_failure(),
    ]);
    let mut failing = FailOnJsonErrorEvent { bytes: Vec::new() };
    let mut fail_stderr = Vec::new();
    let fail_exit = dispatch(
        public_cli(
            runtime.config,
            true,
            WorkerCommand::Logs {
                follow: false,
                job_id: record.meta().job_id(),
            },
        ),
        &fail_runner,
        &runtime.context,
        &mut failing,
        &mut fail_stderr,
    );
    assert_eq!(fail_exit, 74);
    assert!(fail_stderr.is_empty());
    let surviving = parse_json_events(&failing.bytes);
    assert_eq!(surviving.len(), 1);
    assert!(matches!(surviving[0], JsonEvent::Log { .. }));
    assert!(
        !failing
            .bytes
            .windows(b"\"event\":\"error\"".len())
            .any(|window| window == b"\"event\":\"error\"")
    );
}

#[test]
fn human_status_and_json_status_share_the_same_sanitized_report() {
    // Break caught: human status invents fields, omits the shared DTO, or
    // renders planted command/path/token/SSH values.
    let command_secret = "printf TASK9_COMMAND_SECRET PLANTED_ENV_LIKE_VALUE";
    let safe_relative_path = "packages/app";
    let planted_env = "PLANTED_ENV_LIKE_VALUE";
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let store = ClientStateStore::open(&runtime.state).unwrap();
    let secret_record = record_with_meta_parts(
        JOB_ID.parse().unwrap(),
        store.client_id(),
        "mini-1",
        PROJECT_ID,
        WORKTREE_ID,
        MANIFEST_DIGEST,
        safe_relative_path,
        30_000,
        "heavy",
        CommandSpec::shell(command_secret.into()).unwrap(),
        1_000,
        Some(LEASE_TOKEN.parse().unwrap()),
        Some(JobStatus::succeeded(1_001, 0, 0).unwrap()),
        RemoteUncertainty::None,
    );
    store.create_job(secret_record.clone()).unwrap();
    for seed in 1..=100 {
        store
            .create_job(test_record(
                &store,
                seed,
                seed as u64,
                Some(JobStatus::succeeded(seed as u64, 0, 0).unwrap()),
                RemoteUncertainty::None,
            ))
            .unwrap();
    }
    let report = StatusService {
        config: &config(),
        client_state: &store,
        remote: &RemoteJobClient::new(&RecordingRunner::returning(Vec::new())),
    }
    .inspect(None)
    .unwrap();
    assert_eq!(report.jobs.len(), 100);
    assert_eq!(report.omitted, 1);
    let expected_human = expected_human_status(&report);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let human_exit = dispatch(
        public_cli(
            runtime.config.clone(),
            false,
            WorkerCommand::Status { job_id: None },
        ),
        &RecordingRunner::returning(Vec::new()),
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(human_exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(
        String::from_utf8(stdout.clone()).unwrap(),
        format!("{expected_human}\n")
    );
    let human = String::from_utf8(stdout.clone()).unwrap();
    assert!(human.contains("1 older jobs omitted"));
    for secret in [command_secret, planted_env, CLIENT_ID, LEASE_TOKEN, "mac1"] {
        assert!(!human.contains(secret), "human status leaked {secret}");
    }

    stdout.clear();
    let json_exit = dispatch(
        public_cli(runtime.config, true, WorkerCommand::Status { job_id: None }),
        &RecordingRunner::returning(Vec::new()),
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(json_exit, 0);
    assert!(stderr.is_empty());
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let value: Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["kind"], "status");
    assert_eq!(value["omitted"], report.omitted);
    assert_eq!(value["jobs"].as_array().unwrap().len(), report.jobs.len());
    assert_eq!(
        value["jobs"][0]["job_id"],
        report.jobs[0].job_id.to_string()
    );
    assert_eq!(value["jobs"][0]["worker"], report.jobs[0].worker);
    assert_eq!(
        value["jobs"][0]["created_at_millis"],
        report.jobs[0].created_at_millis
    );
    assert_eq!(
        value["jobs"][0]["manifest_digest"],
        report.jobs[0].manifest_digest
    );
    assert_eq!(value["jobs"][0]["relative_working_dir"], safe_relative_path);
    let encoded = String::from_utf8(stdout).unwrap();
    for secret in [command_secret, planted_env, CLIENT_ID, LEASE_TOKEN, "mac1"] {
        assert!(!encoded.contains(secret), "JSON status leaked {secret}");
    }
}

#[test]
fn reserved_preexecution_exits_are_64_69_70_74_75() {
    // Break caught: pre-execution project/transport/protocol/I/O/capacity
    // failures are remapped or emit a started-command exit.
    let repo = run_repo(b"version = 1\n[artifacts]\ninclude = [\"target/**\"]\n");
    let temp = tempfile::tempdir().unwrap();
    let artifacts_runtime = public_runtime(temp.path(), repo.root());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let artifacts_exit = dispatch(
        public_cli(
            artifacts_runtime.config,
            false,
            WorkerCommand::Run {
                worker: Some("mini-1".into()),
                no_wait: false,
                project: None,
                includes: Vec::new(),
                timeout: None,
                shell: None,
                argv: vec!["true".into()],
            },
        ),
        &RunScriptRunner::new(
            artifacts_runtime
                .state
                .canonicalize()
                .unwrap_or_else(|_| artifacts_runtime.state.clone()),
            [],
        ),
        &artifacts_runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(artifacts_exit, 64);
    assert!(stdout.is_empty());
    assert!(!stderr.is_empty());

    let missing = tempfile::tempdir().unwrap();
    let missing_runtime = public_runtime(missing.path(), missing.path());
    stdout.clear();
    stderr.clear();
    let missing_exit = dispatch(
        public_cli(
            missing_runtime.config,
            false,
            WorkerCommand::Logs {
                follow: false,
                job_id: JOB_ID.parse().unwrap(),
            },
        ),
        &RecordingRunner::returning(Vec::new()),
        &missing_runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(missing_exit, 64);
    assert!(stdout.is_empty());

    let transport = tempfile::tempdir().unwrap();
    let transport_runtime = public_runtime(transport.path(), transport.path());
    let record = persist_public_job(
        &transport_runtime.state,
        Some(JobStatus::accepted(101).unwrap()),
    );
    stdout.clear();
    stderr.clear();
    let transport_exit = dispatch(
        public_cli(
            transport_runtime.config,
            false,
            WorkerCommand::Logs {
                follow: false,
                job_id: record.meta().job_id(),
            },
        ),
        &RecordingRunner::returning(vec![transport_failure()]),
        &transport_runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(transport_exit, 69);
    assert!(stdout.is_empty());

    let protocol = tempfile::tempdir().unwrap();
    let protocol_runtime = public_runtime(protocol.path(), protocol.path());
    let protocol_record = persist_public_job(
        &protocol_runtime.state,
        Some(JobStatus::accepted(101).unwrap()),
    );
    stdout.clear();
    stderr.clear();
    let protocol_exit = dispatch(
        public_cli(
            protocol_runtime.config,
            false,
            WorkerCommand::Logs {
                follow: false,
                job_id: protocol_record.meta().job_id(),
            },
        ),
        &RecordingRunner::returning(vec![status_result(&mismatched_status_for(
            &protocol_record,
        ))]),
        &protocol_runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(protocol_exit, 70);
    assert!(stdout.is_empty());

    let io_temp = tempfile::tempdir().unwrap();
    let io_runtime = public_runtime(io_temp.path(), io_temp.path());
    persist_public_job(
        &io_runtime.state,
        Some(JobStatus::succeeded(110, 0, 0).unwrap()),
    );
    let fail_state = Arc::new(Mutex::new(OutputState::default()));
    let mut failing = RecordingWriter::failing(fail_state);
    stderr.clear();
    let io_exit = dispatch(
        public_cli(
            io_runtime.config,
            true,
            WorkerCommand::Status { job_id: None },
        ),
        &RecordingRunner::returning(Vec::new()),
        &io_runtime.context,
        &mut failing,
        &mut stderr,
    );
    assert_eq!(io_exit, 74);

    let busy = tempfile::tempdir().unwrap();
    let busy_repo = run_repo(b"version = 1\n");
    let busy_runtime = public_runtime(busy.path(), busy_repo.root());
    stdout.clear();
    stderr.clear();
    let busy_exit = dispatch(
        public_cli(
            busy_runtime.config,
            false,
            WorkerCommand::Run {
                worker: Some("mini-1".into()),
                no_wait: true,
                project: None,
                includes: Vec::new(),
                timeout: None,
                shell: None,
                argv: vec!["true".into()],
            },
        ),
        &RunScriptRunner::new(
            busy_runtime
                .state
                .canonicalize()
                .unwrap_or(busy_runtime.state.clone()),
            [RunScriptStep::ProbeBusy],
        ),
        &busy_runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(busy_exit, 75);
    assert!(stdout.is_empty());
}

fn mismatched_status_for(record: &LocalJobRecord) -> StatusResponse {
    let response = log_status_response(record, JobStatus::accepted(101).unwrap());
    let mut value = serde_json::to_value(response).unwrap();
    value["meta"]["client_id"] = Value::String(CLIENT_ID.into());
    serde_json::from_value(value).unwrap()
}

#[test]
fn started_command_exits_0_7_64_143_are_preserved() {
    // Break caught: representable command exits are remapped through reserved
    // usage/capacity/infrastructure codes after the remote command started.
    let cases: [(u8, JobStatus); 4] = [
        (0, JobStatus::succeeded(110, 0, 0).unwrap()),
        (7, JobStatus::failed(110, 7, 0, 0).unwrap()),
        (64, JobStatus::failed(110, 64, 0, 0).unwrap()),
        (143, signal_status(110, 15, 0, 0)),
    ];
    for (expected, status) in cases {
        let temp = tempfile::tempdir().unwrap();
        let runtime = public_runtime(temp.path(), temp.path());
        let record = persist_public_job(&runtime.state, Some(JobStatus::accepted(101).unwrap()));
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = dispatch(
            public_cli(
                runtime.config,
                false,
                WorkerCommand::Logs {
                    follow: false,
                    job_id: record.meta().job_id(),
                },
            ),
            &RecordingRunner::returning(terminal_log_script(&record, status)),
            &runtime.context,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(exit, expected, "logs must preserve started exit {expected}");
        assert!(stderr.is_empty());
    }

    let repo = run_repo(b"version = 1\n");
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), repo.root());
    let _ = ClientStateStore::open(&runtime.state).unwrap();
    let runner = RunScriptRunner::new(
        runtime.state.canonicalize().unwrap(),
        [
            RunScriptStep::ProbeReady,
            RunScriptStep::Acquire,
            RunScriptStep::Upload,
            RunScriptStep::Verify,
            RunScriptStep::SubmitAccepted,
            RunScriptStep::StatusTerminal {
                exit_code: 64,
                stdout_bytes: 0,
                stderr_bytes: 0,
            },
            RunScriptStep::LogChunkBytes {
                stream: LogStream::Stdout,
                offset: 0,
                bytes: b"",
            },
            RunScriptStep::LogChunkBytes {
                stream: LogStream::Stderr,
                offset: 0,
                bytes: b"",
            },
            RunScriptStep::StatusTerminal {
                exit_code: 64,
                stdout_bytes: 0,
                stderr_bytes: 0,
            },
        ],
    );
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let run_exit = dispatch(
        public_cli(
            runtime.config,
            false,
            WorkerCommand::Run {
                worker: Some("mini-1".into()),
                no_wait: false,
                project: None,
                includes: Vec::new(),
                timeout: None,
                shell: None,
                argv: vec!["true".into()],
            },
        ),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(run_exit, 64);
    assert!(
        String::from_utf8(stdout)
            .unwrap()
            .contains(" accepted on mini-1\n")
    );
    assert!(stderr.is_empty());
    runner.assert_consumed();
}

#[test]
fn full_public_forms_execute_without_exposing_hidden_host_commands() {
    // Break caught: one of the seven public forms is rejected, or public
    // execution prints a hidden host verb.
    let repo = run_repo(b"version = 1\n");
    let project = repo.root().display().to_string();
    let parsed = [
        vec![
            "worker",
            "run",
            "--worker",
            "mini-1",
            "--",
            "npm",
            "test",
            "--",
            "--literal",
        ],
        vec![
            "worker",
            "run",
            "--worker",
            "mini-1",
            "--project",
            "/repo",
            "--include",
            "fixtures/generated/**",
            "--timeout",
            "45m",
            "--",
            "npm",
            "test",
        ],
        vec![
            "worker",
            "run",
            "--worker",
            "mini-1",
            "--shell",
            "npm run build && npm test",
        ],
        vec!["worker", "status"],
        vec!["worker", "status", JOB_ID],
        vec!["worker", "logs", JOB_ID],
        vec!["worker", "logs", "-f", JOB_ID],
    ];
    for arguments in parsed {
        let cli = Cli::try_parse_from(&arguments).expect("public form must parse");
        match cli.command {
            WorkerCommand::Run { .. }
            | WorkerCommand::Status { .. }
            | WorkerCommand::Logs { .. } => {}
            WorkerCommand::Host { .. } => panic!("public form selected a hidden host command"),
            _ => {}
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), repo.root());
    let store = ClientStateStore::open(&runtime.state).unwrap();
    let record = persist_log_job(&store, Some(JobStatus::succeeded(110, 0, 0).unwrap()));
    let job_id = record.meta().job_id().to_string();
    let forms = [
        Cli::try_parse_from([
            "worker",
            "run",
            "--worker",
            "mini-1",
            "--",
            "npm",
            "test",
            "--",
            "--literal",
        ])
        .unwrap(),
        Cli::try_parse_from([
            "worker",
            "run",
            "--worker",
            "mini-1",
            "--project",
            project.as_str(),
            "--include",
            "tracked.txt",
            "--timeout",
            "45m",
            "--",
            "npm",
            "test",
        ])
        .unwrap(),
        Cli::try_parse_from([
            "worker",
            "run",
            "--worker",
            "mini-1",
            "--shell",
            "npm run build && npm test",
        ])
        .unwrap(),
    ];
    for (index, mut cli) in forms.into_iter().enumerate() {
        let form_root = temp.path().join(format!("run-form-{index}"));
        fs::create_dir(&form_root).unwrap();
        let form_runtime = public_runtime(&form_root, repo.root());
        let form_store = ClientStateStore::open(&form_runtime.state).unwrap();
        let runner = RunScriptRunner::new(
            form_runtime.state.canonicalize().unwrap(),
            run_terminal_steps(0),
        );
        cli.config = Some(form_runtime.config.clone());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = dispatch(
            cli,
            &runner,
            &form_runtime.context,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(exit, 0);
        assert_no_hidden_host_leak(&stdout, &stderr);
        assert_eq!(form_store.list_jobs().unwrap().len(), 1);
        runner.assert_consumed();
    }

    let status_runner = RecordingRunner::returning(Vec::new());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut status_cli = Cli::try_parse_from(["worker", "status"]).unwrap();
    status_cli.config = Some(runtime.config.clone());
    assert_eq!(
        dispatch(
            status_cli,
            &status_runner,
            &runtime.context,
            &mut stdout,
            &mut stderr
        ),
        0
    );
    assert_no_hidden_host_leak(&stdout, &stderr);

    stdout.clear();
    let mut exact_cli = Cli::try_parse_from(["worker", "status", job_id.as_str()]).unwrap();
    exact_cli.config = Some(runtime.config.clone());
    assert_eq!(
        dispatch(
            exact_cli,
            &RecordingRunner::returning(vec![status_result(&log_status_response(
                &record,
                JobStatus::succeeded(110, 0, 0).unwrap(),
            ))]),
            &runtime.context,
            &mut stdout,
            &mut stderr
        ),
        0
    );
    assert_no_hidden_host_leak(&stdout, &stderr);

    stdout.clear();
    let mut logs_cli = Cli::try_parse_from(["worker", "logs", job_id.as_str()]).unwrap();
    logs_cli.config = Some(runtime.config.clone());
    assert_eq!(
        dispatch(
            logs_cli,
            &RecordingRunner::returning(terminal_log_script(
                &record,
                JobStatus::succeeded(110, 0, 0).unwrap(),
            )),
            &runtime.context,
            &mut stdout,
            &mut stderr
        ),
        0
    );
    assert_no_hidden_host_leak(&stdout, &stderr);

    stdout.clear();
    let mut follow_cli = Cli::try_parse_from(["worker", "logs", "-f", job_id.as_str()]).unwrap();
    follow_cli.config = Some(runtime.config);
    assert_eq!(
        dispatch(
            follow_cli,
            &RecordingRunner::returning(vec![
                status_result(&log_status_response(
                    &record,
                    JobStatus::succeeded(110, 0, 0).unwrap(),
                )),
                log_chunk_result(LogStream::Stdout, 0, b""),
                log_chunk_result(LogStream::Stderr, 0, b""),
                status_result(&log_status_response(
                    &record,
                    JobStatus::succeeded(110, 0, 0).unwrap(),
                )),
            ]),
            &runtime.context,
            &mut stdout,
            &mut stderr
        ),
        0
    );
    assert_no_hidden_host_leak(&stdout, &stderr);
}

enum ScriptedIo {
    Write(io::Result<usize>),
    Flush(io::Result<()>),
}

struct ScriptedWriter {
    script: VecDeque<ScriptedIo>,
    writes: usize,
    flushes: usize,
    bytes: Vec<u8>,
}

impl ScriptedWriter {
    fn scripted(script: Vec<ScriptedIo>) -> Self {
        Self {
            script: script.into(),
            writes: 0,
            flushes: 0,
            bytes: Vec::new(),
        }
    }

    fn contains_json_error_event(&self) -> bool {
        self.bytes
            .windows(b"\"event\":\"error\"".len())
            .any(|window| window == b"\"event\":\"error\"")
    }
}

impl Write for ScriptedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        match self.script.pop_front() {
            Some(ScriptedIo::Write(Ok(n))) => {
                let n = n.min(buf.len());
                self.bytes.extend_from_slice(&buf[..n]);
                Ok(n)
            }
            Some(ScriptedIo::Write(Err(error))) => Err(error),
            Some(ScriptedIo::Flush(_)) => {
                panic!("scripted writer expected a write, but the next scripted step was flush")
            }
            None => {
                self.bytes.extend_from_slice(buf);
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        match self.script.pop_front() {
            Some(ScriptedIo::Flush(result)) => result,
            Some(ScriptedIo::Write(_)) => {
                panic!("scripted writer expected a flush, but the next scripted step was write")
            }
            None => Ok(()),
        }
    }
}

fn planted_live_stdout_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "planted live stdout failure")
}

fn dispatch_json_logs_to(
    stdout: &mut dyn Write,
) -> (u8, Vec<u8>, tempfile::TempDir, PublicRuntime) {
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let record = persist_public_job(&runtime.state, Some(JobStatus::accepted(101).unwrap()));
    let mut stderr = Vec::new();
    let exit = dispatch(
        public_cli(
            runtime.config.clone(),
            true,
            WorkerCommand::Logs {
                follow: false,
                job_id: record.meta().job_id(),
            },
        ),
        &RecordingRunner::returning(terminal_log_script(
            &record,
            JobStatus::succeeded(110, 0, 0).unwrap(),
        )),
        &runtime.context,
        stdout,
        &mut stderr,
    );
    (exit, stderr, temp, runtime)
}

fn assert_no_second_json_error(stdout: &ScriptedWriter, stderr: &[u8]) {
    assert!(
        stderr.is_empty(),
        "stderr must stay empty: {}",
        String::from_utf8_lossy(stderr)
    );
    assert!(
        !stdout.contains_json_error_event(),
        "failed live stdout must not emit a JSON error event: {}",
        String::from_utf8_lossy(&stdout.bytes)
    );
}

#[test]
fn public_diag_json_live_stdout_fail_once_recovering_is_74() {
    // Break caught: a failed accepted/log write is retried as a JSON error
    // event on the same recovering stdout.
    let mut stdout =
        ScriptedWriter::scripted(vec![ScriptedIo::Write(Err(planted_live_stdout_error()))]);
    let (exit, stderr, _temp, _runtime) = dispatch_json_logs_to(&mut stdout);
    assert_eq!(exit, 74);
    assert_eq!(stdout.writes, 1);
    assert_eq!(stdout.flushes, 0);
    assert_no_second_json_error(&stdout, &stderr);
}

#[test]
fn public_diag_json_live_stdout_partial_then_error_is_74() {
    // Break caught: write_all partial success plus a later error is treated
    // as a recoverable service failure and retried as an error event.
    let mut stdout = ScriptedWriter::scripted(vec![
        ScriptedIo::Write(Ok(8)),
        ScriptedIo::Write(Err(planted_live_stdout_error())),
    ]);
    let (exit, stderr, _temp, _runtime) = dispatch_json_logs_to(&mut stdout);
    assert_eq!(exit, 74);
    assert_eq!(stdout.writes, 2);
    assert_eq!(stdout.flushes, 0);
    assert_no_second_json_error(&stdout, &stderr);
}

#[test]
fn public_diag_json_live_stdout_zero_write_is_74() {
    // Break caught: write_all Ok(0) on a nonempty buffer is not treated as a
    // live-writer failure, so the dispatcher retries an error event.
    let mut stdout = ScriptedWriter::scripted(vec![ScriptedIo::Write(Ok(0))]);
    let (exit, stderr, _temp, _runtime) = dispatch_json_logs_to(&mut stdout);
    assert_eq!(exit, 74);
    assert_eq!(stdout.writes, 1);
    assert_eq!(stdout.flushes, 0);
    assert_no_second_json_error(&stdout, &stderr);
}

#[test]
fn public_diag_json_live_stdout_flush_failure_is_74() {
    // Break caught: a failed live flush is followed by a second write of a
    // JSON error event once later writes/flushes recover.
    let mut stdout = ScriptedWriter::scripted(vec![
        ScriptedIo::Write(Ok(usize::MAX)),
        ScriptedIo::Write(Ok(usize::MAX)),
        ScriptedIo::Flush(Err(planted_live_stdout_error())),
    ]);
    let (exit, stderr, _temp, _runtime) = dispatch_json_logs_to(&mut stdout);
    assert_eq!(exit, 74);
    assert_eq!(stdout.writes, 2);
    assert_eq!(stdout.flushes, 1);
    assert_no_second_json_error(&stdout, &stderr);
}

#[test]
fn public_diag_json_error_keeps_exit_and_rejects_oversized_internal_payload() {
    // Break caught: Protocol/Capacity detail is copied into JsonEvent::Error
    // and can leak paths/argv/secrets or exceed the 4096-byte bound.
    let planted_path = "/Users/alice/PLANTED_PUBLIC_PATH";
    let planted_secret = "PLANTED_PUBLIC_SECRET";
    let planted_argv = "printf TASK9_COMMAND_SECRET";
    let planted_host = "planted-local-mac.example";
    let planted = format!(
        "{} {planted_path} {planted_secret} {planted_argv} {planted_host} {CLIENT_ID} {LEASE_TOKEN}",
        "X".repeat(3900)
    );
    assert!(planted.len() <= 4096);
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let record = persist_public_job(&runtime.state, Some(JobStatus::accepted(101).unwrap()));
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = dispatch(
        public_cli(
            runtime.config,
            true,
            WorkerCommand::Logs {
                follow: false,
                job_id: record.meta().job_id(),
            },
        ),
        &RecordingRunner::returning(vec![authoritative_protocol_failure(
            "INVALID_REQUEST",
            &planted,
        )]),
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 70);
    assert!(stderr.is_empty());
    let events = parse_json_events(&stdout);
    assert_eq!(events.len(), 1);
    match &events[0] {
        JsonEvent::Error {
            protocol_version,
            code,
            message,
        } => {
            assert_eq!(*protocol_version, PROTOCOL_VERSION);
            assert_eq!(code, "INVALID_REQUEST");
            assert_eq!(message, "protocol error");
        }
        other => panic!("expected one JSON error event, got {other:?}"),
    }
    let text = String::from_utf8_lossy(&stdout);
    for planted in [
        planted_path,
        planted_secret,
        planted_argv,
        planted_host,
        CLIENT_ID,
        LEASE_TOKEN,
        &"X".repeat(32),
    ] {
        assert!(!text.contains(planted), "JSON error leaked {planted}");
    }
}

#[test]
fn public_diag_missing_config_is_sanitized_for_human_and_json() {
    // Break caught: public run/logs/status/cancel render WorkerError::Display, so a
    // missing --config prints its absolute path and planted secret.
    let planted_secret = "PLANTED_PUBLIC_SECRET";
    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let missing = runtime
        .config
        .parent()
        .unwrap()
        .join(planted_secret)
        .join("missing-config.toml");
    assert!(missing.is_absolute());
    let planted_path = missing.to_string_lossy().into_owned();
    let job_id = JOB_ID.parse().unwrap();
    let command_kinds = ["run", "logs", "status", "cancel"];

    for kind in command_kinds {
        let is_status = matches!(kind, "status" | "cancel");
        for json in [false, true] {
            let command = match kind {
                "run" => WorkerCommand::Run {
                    worker: Some("mini-1".into()),
                    no_wait: false,
                    project: None,
                    includes: Vec::new(),
                    timeout: None,
                    shell: None,
                    argv: vec!["true".into()],
                },
                "logs" => WorkerCommand::Logs {
                    follow: false,
                    job_id,
                },
                "status" => WorkerCommand::Status {
                    job_id: Some(job_id),
                },
                _ => WorkerCommand::Cancel { job_id },
            };
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let exit = dispatch(
                public_cli(missing.clone(), json, command),
                &RecordingRunner::returning(Vec::new()),
                &runtime.context,
                &mut stdout,
                &mut stderr,
            );
            assert_eq!(exit, 64, "json={json} command={kind}");
            let combined = [stdout.as_slice(), stderr.as_slice()].concat();
            let text = String::from_utf8_lossy(&combined);
            assert!(
                !text.contains(&planted_path),
                "json={json} leaked path {planted_path}: {text}"
            );
            assert!(
                !text.contains(planted_secret),
                "json={json} leaked secret: {text}"
            );
            if json && !is_status {
                assert!(stderr.is_empty(), "JSON stream stderr: {text}");
                let events = parse_json_events(&stdout);
                assert_eq!(events.len(), 1, "{text}");
                match &events[0] {
                    JsonEvent::Error {
                        protocol_version,
                        code,
                        message,
                    } => {
                        assert_eq!(*protocol_version, PROTOCOL_VERSION);
                        assert_eq!(code, "CONFIG");
                        assert_eq!(message, "configuration error");
                    }
                    other => panic!("JSON run/logs must emit JsonEvent::Error, got {other:?}"),
                }
            } else {
                assert!(stdout.is_empty(), "human/status stdout: {text}");
                assert_eq!(
                    String::from_utf8_lossy(&stderr),
                    "CONFIG: configuration error\n",
                    "json={json} command={kind}"
                );
            }
        }
    }
}

#[test]
fn public_diag_oversized_authoritative_host_envelope_stays_sanitized_and_preserves_persisted_state()
{
    // Break caught: an over-limit host-authoritative error is echoed,
    // treated as an authoritative Protocol/Capacity classification instead
    // of a generic transport failure, skips human-mode sanitization, or a
    // read-only status/logs failure mutates the persisted job record.
    //
    // HostControlErrorDetail::serialize re-validates its 4096-byte bound, so
    // an oversized envelope can never round-trip through
    // decode_host_control_error and always degrades to the same generic
    // HOST_REQUEST_FAILED transport classification (exit 69), regardless of
    // the code/message the host actually planted on the wire.
    let planted_code = "OVERSIZED_HOST_ENVELOPE";
    let secret = "PLANTED_OVERSIZED_SECRET";
    let filler_sample = "X".repeat(64);
    let expected_code = "HOST_REQUEST_FAILED";
    let expected_message = "transport error";

    for (kind, json) in [
        ("status", false),
        ("status", true),
        ("logs", false),
        ("logs", true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let runtime = public_runtime(temp.path(), temp.path());
        let record = persist_public_job(&runtime.state, Some(JobStatus::accepted(101).unwrap()));
        let job_id = record.meta().job_id();
        let job_path = runtime.state.join("jobs").join(format!("{job_id}.json"));
        let before = fs::read(&job_path).unwrap();

        let command = match kind {
            "status" => WorkerCommand::Status {
                job_id: Some(job_id),
            },
            _ => WorkerCommand::Logs {
                follow: false,
                job_id,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = dispatch(
            public_cli(runtime.config.clone(), json, command),
            &RecordingRunner::returning(vec![oversized_authoritative_protocol_failure(
                planted_code,
                secret,
            )]),
            &runtime.context,
            &mut stdout,
            &mut stderr,
        );

        assert_eq!(exit, 69, "kind={kind} json={json}");
        let combined = [stdout.as_slice(), stderr.as_slice()].concat();
        let text = String::from_utf8_lossy(&combined);
        assert!(
            !text.contains(secret),
            "kind={kind} json={json} leaked the planted secret: {text}"
        );
        assert!(
            !text.contains(&filler_sample),
            "kind={kind} json={json} leaked the oversized filler: {text}"
        );
        assert!(
            !text.contains(planted_code),
            "kind={kind} json={json} leaked the planted host code: {text}"
        );
        assert!(
            text.len() < 1_000,
            "kind={kind} json={json} diagnostic was not bounded: {} bytes",
            text.len()
        );

        if kind == "logs" && json {
            assert!(stderr.is_empty(), "kind={kind} json={json}: {text}");
            let events = parse_json_events(&stdout);
            assert_eq!(events.len(), 1, "kind={kind} json={json}: {text}");
            match &events[0] {
                JsonEvent::Error {
                    protocol_version,
                    code: actual_code,
                    message,
                } => {
                    assert_eq!(*protocol_version, PROTOCOL_VERSION);
                    assert_eq!(actual_code, expected_code);
                    assert_eq!(message, expected_message);
                }
                other => {
                    panic!("kind={kind} json={json}: expected a JSON error event, got {other:?}")
                }
            }
        } else {
            assert!(stdout.is_empty(), "kind={kind} json={json} stdout: {text}");
            assert_eq!(
                String::from_utf8_lossy(&stderr),
                format!("{expected_code}: {expected_message}\n"),
                "kind={kind} json={json}"
            );
        }

        let after = fs::read(&job_path).unwrap();
        assert_eq!(
            before, after,
            "kind={kind} json={json}: persisted job record changed on a read-only failure"
        );
    }
}

#[test]
fn public_diag_non_utf8_config_path_stays_sanitized_and_the_persisted_job_record_is_unchanged() {
    // Break caught: a non-UTF-8 local config path panics, or its raw bytes
    // (lossily rendered) leak into a public diagnostic instead of the
    // generic sanitized message.
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let temp = tempfile::tempdir().unwrap();
    let runtime = public_runtime(temp.path(), temp.path());
    let record = persist_public_job(&runtime.state, Some(JobStatus::accepted(101).unwrap()));
    let job_id = record.meta().job_id();
    let job_path = runtime.state.join("jobs").join(format!("{job_id}.json"));
    let before = fs::read(&job_path).unwrap();

    let non_utf8_segment = OsString::from_vec(b"non-utf8-\xffconfig-dir".to_vec());
    let non_utf8_config = runtime
        .config
        .parent()
        .unwrap()
        .join(PathBuf::from(non_utf8_segment))
        .join("missing.toml");
    assert!(std::str::from_utf8(non_utf8_config.as_os_str().as_bytes()).is_err());

    for json in [false, true] {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = dispatch(
            public_cli(
                non_utf8_config.clone(),
                json,
                WorkerCommand::Status {
                    job_id: Some(job_id),
                },
            ),
            &RecordingRunner::returning(Vec::new()),
            &runtime.context,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(exit, 64, "json={json}");
        assert!(stdout.is_empty(), "json={json}");
        assert_eq!(
            String::from_utf8(stderr.clone()).expect("diagnostic must stay valid UTF-8"),
            "CONFIG: configuration error\n",
            "json={json}"
        );
    }

    let after = fs::read(&job_path).unwrap();
    assert_eq!(
        before, after,
        "persisted job record changed after a non-UTF-8 config path failure"
    );
}
