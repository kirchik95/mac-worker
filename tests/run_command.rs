use std::{
    collections::{BTreeSet, VecDeque},
    ffi::OsStr,
    fs,
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::ExitStatus,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use assert_cmd::Command;
use clap::Parser;
use mac_worker::{
    cli::{Cli, Command as WorkerCommand},
    client_state::{ClientStateStore, ClientStateWritePoint},
    config::{Config, WorkerEntry},
    error::WorkerError,
    job::{
        ClientId, CommandSpec, HostControlError, JobId, JobMeta, JobState, JobStatus, LeaseToken,
        LocalJobRecord, ProcessIdentity, RemoteUncertainty, RequestFingerprintMaterial,
        ResolveOrAbandonRequest, ResolveOrAbandonResponse, StatusRequest, StatusResponse,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    run::{
        RunCompletion, RunReport, RunRequest, STATUS_LIST_LIMIT, STATUS_REFRESH_DEADLINE,
        STATUS_REFRESH_LIMIT, StatusReport, StatusRow, StatusService,
    },
    transfer::{HostOperation, RemoteJobClient, ResolutionRuntime},
};
use predicates::prelude::*;
use serde_json::Value;

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
    let meta = JobMeta::new(&material, material.fingerprint(), 100).unwrap();
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

fn authoritative_protocol_failure(code: &str, message: &str) -> Result<ProcessResult, WorkerError> {
    let mut stdout = serde_json::to_vec(&HostControlError::new(code, message).unwrap()).unwrap();
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
    let meta = JobMeta::new(&material, material.fingerprint(), created_at_millis).unwrap();
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
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker, "mini-1");
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
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker, "mini-1");
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
        project,
        includes,
        timeout,
        shell,
        argv,
    } = cli.command
    else {
        panic!("run form must select run command");
    };
    assert_eq!(worker, "mini-1");
    assert_eq!(project, None);
    assert!(includes.is_empty());
    assert_eq!(timeout, None);
    assert_eq!(shell.as_deref(), Some("npm run build && npm test"));
    assert!(argv.is_empty());

    let request = RunRequest {
        worker,
        project: PathBuf::from("/path/to/worktree"),
        cli_includes: includes,
        timeout,
        command: CommandSpec::shell(shell.unwrap()).unwrap(),
    };
    assert_eq!(request.worker, "mini-1");

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
fn run_rejects_missing_worker_empty_command_and_mutually_exclusive_modes() {
    // Catches accidental implicit worker selection or ambiguous command mode.
    for arguments in [
        vec!["run", "--", "true"],
        vec!["run", "--worker", "", "--", "true"],
        vec!["run", "--worker", "mini-1"],
        vec!["run", "--worker", "mini-1", "--shell", ""],
        vec!["run", "--worker", "mini-1", "--shell", "true", "--", "echo"],
    ] {
        assert_usage(&arguments);
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
    let meta = JobMeta::new(&material, material.fingerprint(), 100).unwrap();
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
