#[allow(dead_code)]
mod support;

use std::{
    collections::{BTreeMap, VecDeque},
    ffi::{OsStr, OsString},
    fs,
    io::Cursor,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::MetadataExt,
        process::ExitStatusExt,
    },
    path::Path,
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::WorkerEntry,
    error::{ProcessError, ProcessStream, WorkerError},
    host_store::{HostStore, HostStoreWritePoint, SupervisorGuard},
    inputs::RelativePath,
    job::{
        ClientId, CommandSpec, JobId, JobStatus, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseRecord, LeaseToken, RequestFingerprint, RequestFingerprintMaterial,
        ResolveOrAbandonOutcome, ResolveOrAbandonRequest, SubmitRequest,
    },
    job_service::{JobService, LaunchCandidate, SupervisorLauncher},
    lease::{AdmissionFacts, LeaseService},
    paths::PathLayout,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project::{ProjectContext, ProjectInspector},
    project_config::SnapshotSettings,
    protocol::MemoryPressure,
    run_with_rsync_executor_in_context,
    snapshot::{Snapshot, SnapshotBuilder},
    transfer::{
        AbandonTransferResult, HostOperation, HostTransferService, RsyncServerExecutor,
        RsyncServerInvocation, RsyncTransport, SshJsonTransport, TransferFailureDisposition,
        TransferIdentity, TransferReceipt, transfer_failure_disposition,
    },
};
use sha2::Digest;
use support::GitRepo;

static HOST_FD_STRESS_LOCK: Mutex<()> = Mutex::new(());

struct NeverLaunchResolution;

impl SupervisorLauncher for NeverLaunchResolution {
    fn launch(
        &self,
        _job_id: JobId,
        _guard: SupervisorGuard,
    ) -> Result<LaunchCandidate, WorkerError> {
        panic!("invalid accepted authority must never launch")
    }
}

#[derive(serde::Serialize)]
struct LiteralRequest<'a> {
    alpha: u8,
    beta: &'a str,
}

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LiteralResponse {
    accepted: bool,
}

struct RecordingRunner {
    results: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
    requests: Mutex<Vec<ProcessRequest>>,
}

impl RecordingRunner {
    fn returning(results: Vec<Result<ProcessResult, WorkerError>>) -> Self {
        Self {
            results: Mutex::new(results.into()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<ProcessRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("a scripted result must exist")
    }
}

fn worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
    }
}

fn policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(15),
    }
}

fn status(code: i32) -> ExitStatus {
    ExitStatus::from_raw(code << 8)
}

fn signalled(signal: i32) -> ExitStatus {
    ExitStatus::from_raw(signal)
}

fn result(status: ExitStatus, stdout: &[u8], stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status,
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

fn transfer_identity() -> TransferIdentity {
    TransferIdentity::new(
        JobId::new(uuid::Uuid::from_u128(1)),
        ClientId::new(uuid::Uuid::from_u128(2)),
        LeaseToken::new(uuid::Uuid::from_u128(3)),
        RequestFingerprint::new("d".repeat(64)).unwrap(),
    )
}

fn acquire_request(seed: u128) -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(seed)),
            ClientId::new(uuid::Uuid::from_u128(seed + 1)),
            LeaseToken::new(uuid::Uuid::from_u128(seed + 2)),
            "mini-1".into(),
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
        )
        .unwrap(),
    )
}

fn replace_disposition_value(store: &HostStore, job: JobId, old: &str, new: &str) {
    let path = store.job_index(job).unwrap();
    let bytes = fs::read_to_string(&path).unwrap();
    assert!(bytes.contains(old));
    fs::write(path, bytes.replacen(old, new, 1)).unwrap();
}

fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn leased_store(root: &Path, seed: u128) -> (HostStore, LeaseAcquireRequest, TransferIdentity) {
    let store = HostStore::open(root).unwrap();
    let request = acquire_request(seed);
    LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap();
    let identity = TransferIdentity::from_acquire_request(&request).unwrap();
    (store, request, identity)
}

fn stock_server_args() -> Vec<OsString> {
    [
        "--server",
        "--delete-before",
        "-l",
        "-p",
        "-D",
        "-r",
        "-t",
        "--dirs",
        ".",
        "incoming",
    ]
    .into_iter()
    .map(OsString::from)
    .collect()
}

fn write_self_consistent_internal_transfer_identity(
    transfer_dir: &Path,
    external_identity_path: &Path,
) {
    let identity_path = transfer_dir.join("identity.json");
    let mut bytes = fs::read_to_string(external_identity_path).unwrap();
    fs::write(&identity_path, &bytes).unwrap();
    let identity: serde_json::Value = serde_json::from_str(&bytes).unwrap();
    for (entry, metadata) in [
        ("directory", fs::metadata(transfer_dir).unwrap()),
        (
            "lock",
            fs::metadata(transfer_dir.join("transfer.lock")).unwrap(),
        ),
        ("identity_file", fs::metadata(&identity_path).unwrap()),
    ] {
        let old_inode = identity[entry]["inode"].as_u64().unwrap();
        bytes = bytes.replacen(
            &format!("\"inode\":{old_inode}"),
            &format!("\"inode\":{}", metadata.ino()),
            1,
        );
    }
    bytes = bytes.replacen(
        identity["identity_file"]["path"].as_str().unwrap(),
        &format!(
            "{}/transfer/identity.json",
            identity["job_id"].as_str().unwrap()
        ),
        1,
    );
    fs::write(identity_path, bytes).unwrap();
}

fn snapshot(cache: &Path) -> (GitRepo, Snapshot) {
    let repo = GitRepo::init();
    repo.write("ordinary.txt", b"ordinary\n");
    repo.write("child with spaces.txt", b"spaces\n");
    repo.write("child\nwith-newline.txt", b"newline\n");
    repo.write("unicode-β.txt", b"unicode\n");
    repo.commit_all("snapshot transfer fixture");
    let context: ProjectContext = ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .unwrap();
    let settings = SnapshotSettings {
        include_untracked: Vec::new(),
        include_empty_dirs: Vec::new(),
        allow_sensitive: Vec::new(),
    };
    let selection = mac_worker::inputs::InputSelector::new(&SystemProcessRunner)
        .select(&context, &settings)
        .unwrap();
    let snapshot = SnapshotBuilder::new(&SystemProcessRunner, cache)
        .capture(&context, &settings, selection)
        .unwrap();
    (repo, snapshot)
}

const STOCK_STATS: &[u8] = b"Number of files: 5\nNumber of files transferred: 4\nTotal file size: 41 B\nTotal transferred file size: 41 B\nUnmatched data: 41 B\nMatched data: 0 B\nFile list size: 172 B\nTotal sent: 501 B\nTotal received: 94 B\n\nsent 501 bytes  received 94 bytes  540909 bytes/sec\ntotal size is 41  speedup is 0.07\n";

#[test]
fn ssh_json_request_uses_exact_fixed_argv_and_compact_stdin() {
    // Break caught: a caller-controlled path/shell fragment or pretty JSON is
    // allowed into the SSH boundary.
    let runner =
        RecordingRunner::returning(vec![Ok(result(status(0), br#"{"accepted":true}"#, b""))]);
    let transport = SshJsonTransport::new(&runner);

    let response: LiteralResponse = transport
        .request(
            &worker(),
            HostOperation::LeaseAcquire,
            &LiteralRequest {
                alpha: 7,
                beta: "fixed",
            },
            policy(),
        )
        .unwrap();

    assert_eq!(response, LiteralResponse { accepted: true });
    assert_eq!(
        runner.requests(),
        vec![ProcessRequest {
            program: OsString::from("/usr/bin/ssh"),
            args: [
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "ForwardAgent=no",
                "-o",
                "ClearAllForwardings=yes",
                "--",
                "mac1",
                "~/.local/bin/worker host lease-acquire",
            ]
            .into_iter()
            .map(OsString::from)
            .collect(),
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: Some(br#"{"alpha":7,"beta":"fixed"}"#.to_vec()),
            policy: policy(),
        }]
    );
}

#[test]
fn ssh_json_request_classifies_nonzero_and_255_without_raw_stderr() {
    // Break caught: raw remote stderr (which may contain secrets) reaches a
    // typed/public diagnostic.
    for (exit, expected_code) in [(23, "HOST_REQUEST_FAILED"), (255, "SSH_UNAVAILABLE")] {
        let planted = "PLANTED-REMOTE-SECRET";
        let runner =
            RecordingRunner::returning(vec![Ok(result(status(exit), b"", planted.as_bytes()))]);
        let error = SshJsonTransport::new(&runner)
            .request::<_, LiteralResponse>(
                &worker(),
                HostOperation::LeaseAcquire,
                &LiteralRequest {
                    alpha: 7,
                    beta: "fixed",
                },
                policy(),
            )
            .unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains(expected_code), "{rendered}");
        assert!(!rendered.contains(planted), "{rendered}");
    }
}

#[test]
fn ssh_json_request_classifies_signal_without_raw_stderr() {
    let planted = "PLANTED-SIGNAL-SECRET";
    let runner = RecordingRunner::returning(vec![Ok(result(
        signalled(libc::SIGKILL),
        b"",
        planted.as_bytes(),
    ))]);

    let error = SshJsonTransport::new(&runner)
        .request::<_, LiteralResponse>(
            &worker(),
            HostOperation::LeaseAcquire,
            &LiteralRequest {
                alpha: 7,
                beta: "fixed",
            },
            policy(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("HOST_REQUEST_FAILED"));
    assert!(!error.to_string().contains(planted));
}

#[test]
fn ssh_json_request_classifies_launch_deadline_and_output_limits() {
    // Break caught: process-boundary details or ambiguous raw output are
    // surfaced instead of stable content-free transport codes.
    let cases = [
        (
            WorkerError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "PLANTED-LAUNCH-SECRET",
            )),
            "SSH_LAUNCH_FAILED",
        ),
        (
            ProcessError::DeadlineExceeded {
                deadline: policy().deadline,
            }
            .into(),
            "SSH_TIMEOUT",
        ),
        (
            ProcessError::OutputLimitExceeded {
                stream: ProcessStream::Stdout,
                limit: policy().stdout_limit,
            }
            .into(),
            "SSH_RESPONSE_TOO_LARGE",
        ),
        (
            ProcessError::OutputLimitExceeded {
                stream: ProcessStream::Stderr,
                limit: policy().stderr_limit,
            }
            .into(),
            "SSH_DIAGNOSTIC_TOO_LARGE",
        ),
    ];

    for (failure, expected_code) in cases {
        let runner = RecordingRunner::returning(vec![Err(failure)]);
        let error = SshJsonTransport::new(&runner)
            .request::<_, LiteralResponse>(
                &worker(),
                HostOperation::LeaseAcquire,
                &LiteralRequest {
                    alpha: 7,
                    beta: "fixed",
                },
                policy(),
            )
            .unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains(expected_code), "{rendered}");
        assert!(!rendered.contains("PLANTED"), "{rendered}");
    }
}

#[test]
fn ssh_json_request_rejects_manual_overflow_utf8_json_and_trailing_data() {
    // Break caught: a runner double that does not enforce policy can smuggle
    // oversized or ambiguous response bytes through the transport decoder.
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (
            vec![b' '; policy().stdout_limit + 1],
            "SSH_RESPONSE_TOO_LARGE",
        ),
        (vec![0xff], "INVALID_RESPONSE"),
        (b"not-json".to_vec(), "INVALID_RESPONSE"),
        (
            br#"{"accepted":true}{"accepted":false}"#.to_vec(),
            "INVALID_RESPONSE",
        ),
    ];

    for (stdout, expected_code) in cases {
        let runner = RecordingRunner::returning(vec![Ok(result(status(0), &stdout, b""))]);
        let error = SshJsonTransport::new(&runner)
            .request::<_, LiteralResponse>(
                &worker(),
                HostOperation::LeaseAcquire,
                &LiteralRequest {
                    alpha: 7,
                    beta: "fixed",
                },
                policy(),
            )
            .unwrap_err();
        assert!(error.to_string().contains(expected_code), "{error}");
    }
}

#[test]
fn ssh_json_request_runs_semantic_dto_validation_during_decode() {
    // Break caught: serde field shape is accepted while the typed job-state
    // invariants are invalid (running without process identities).
    let invalid = br#"{"outcome":"existing_accepted","status":{"state":"running","updated_at_millis":1,"supervisor_pid":null,"supervisor_start_identity":null,"child_pid":null,"child_start_identity":null,"exit_code":null,"final_stdout_bytes":null,"final_stderr_bytes":null,"error_code":null}}"#;
    let runner = RecordingRunner::returning(vec![Ok(result(status(0), invalid, b""))]);

    let error = SshJsonTransport::new(&runner)
        .request::<_, LeaseAcquireResponse>(
            &worker(),
            HostOperation::LeaseAcquire,
            &LiteralRequest {
                alpha: 7,
                beta: "fixed",
            },
            policy(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("INVALID_RESPONSE"));
}

#[test]
fn rsync_upload_uses_exact_stock_argv_and_only_the_publication_root() {
    // Break caught: a live worktree/child path, host-returned sink, shell
    // fragment, or unsupported rsync option enters the upload argv.
    let cache_parent = tempfile::tempdir().unwrap();
    let cache = cache_parent.path().join("cache root with spaces");
    let (_repo, snapshot) = snapshot(&cache);
    let runner = RecordingRunner::returning(vec![Ok(result(status(0), STOCK_STATS, b""))]);

    let receipt = RsyncTransport::new(&runner)
        .upload(&worker(), &snapshot, &transfer_identity())
        .unwrap();

    assert_eq!(
        receipt,
        TransferReceipt {
            files_transferred: 4,
            file_bytes_transferred: 41,
            wire_bytes_sent: 501,
            wire_bytes_received: 94,
        }
    );
    let mut source = snapshot.publication_root().as_os_str().as_bytes().to_vec();
    source.push(b'/');
    let expected = ProcessRequest {
        program: OsString::from("/usr/bin/rsync"),
        args: vec![
            "--archive".into(),
            "--delete".into(),
            "--no-owner".into(),
            "--no-group".into(),
            "--stats".into(),
            "-e".into(),
            "/usr/bin/ssh -o BatchMode=yes -o ConnectTimeout=5 -o ForwardAgent=no -o ClearAllForwardings=yes --".into(),
            "--rsync-path=~/.local/bin/worker host rsync-receive 00000000000000000000000000000001 00000000000000000000000000000002 00000000000000000000000000000003 dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into(),
            OsString::from_vec(source),
            "mac1:incoming".into(),
        ],
        environment: vec![("LC_ALL".into(), "C".into()), ("LANG".into(), "C".into())],
        environment_remove: vec!["LANGUAGE".into()],
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: 256 * 1024,
            stderr_limit: 256 * 1024,
            deadline: Duration::from_secs(15 * 60),
        },
    };
    assert_eq!(runner.requests(), vec![expected]);

    let args = &runner.requests()[0].args;
    for forbidden in [
        OsStr::new("child with spaces.txt"),
        OsStr::new("child\nwith-newline.txt"),
        OsStr::new("unicode-β.txt"),
        OsStr::new("--protect-args"),
    ] {
        assert!(!args.iter().any(|arg| arg == forbidden));
    }
}

#[test]
fn transfer_identity_debug_and_failures_never_expose_the_lease_token() {
    // Break caught: the rsync-path secret is copied into Debug or a public
    // process diagnostic.
    let token = "00000000000000000000000000000003";
    assert!(!format!("{:?}", transfer_identity()).contains(token));

    let cache = tempfile::tempdir().unwrap();
    let (_repo, snapshot) = snapshot(cache.path());
    let runner = RecordingRunner::returning(vec![Err(WorkerError::Io(std::io::Error::other(
        format!("launch failed near {token}"),
    )))]);
    let error = RsyncTransport::new(&runner)
        .upload(&worker(), &snapshot, &transfer_identity())
        .unwrap_err();

    assert!(!error.to_string().contains(token));
    assert!(
        !format!("{:?}", runner.requests()[0]).contains(token),
        "ProcessRequest Debug must redact its argument payload"
    );
    assert_eq!(
        transfer_failure_disposition(&error),
        TransferFailureDisposition::ResolveOrAbandon
    );
}

#[test]
fn rsync_runtime_failures_are_ambiguous_and_content_free() {
    // Break caught: a launched rsync failure is falsely classified as proving
    // the receiver never started, or captured output reaches diagnostics.
    let cache = tempfile::tempdir().unwrap();
    let (_repo, snapshot) = snapshot(cache.path());
    let cases = vec![
        Ok(result(status(23), b"PLANTED-OUT", b"PLANTED-ERR")),
        Ok(result(
            signalled(libc::SIGKILL),
            b"PLANTED-OUT",
            b"PLANTED-ERR",
        )),
        Err(ProcessError::DeadlineExceeded {
            deadline: Duration::from_secs(15 * 60),
        }
        .into()),
        Err(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stdout,
            limit: 256 * 1024,
        }
        .into()),
        Err(ProcessError::OutputLimitExceeded {
            stream: ProcessStream::Stderr,
            limit: 256 * 1024,
        }
        .into()),
    ];

    for scripted in cases {
        let runner = RecordingRunner::returning(vec![scripted]);
        let error = RsyncTransport::new(&runner)
            .upload(&worker(), &snapshot, &transfer_identity())
            .unwrap_err();
        assert_eq!(
            transfer_failure_disposition(&error),
            TransferFailureDisposition::ResolveOrAbandon,
            "{error}"
        );
        let rendered = error.to_string();
        assert!(!rendered.contains("PLANTED"), "{rendered}");
        assert!(!rendered.contains("00000000000000000000000000000003"));
    }
}

#[test]
fn invalid_local_transport_configuration_proves_no_receiver_started() {
    let cache = tempfile::tempdir().unwrap();
    let (_repo, snapshot) = snapshot(cache.path());
    let runner = RecordingRunner::returning(Vec::new());
    let mut invalid = worker();
    invalid.ssh = "-oProxyCommand=PLANTED".into();

    let error = RsyncTransport::new(&runner)
        .upload(&invalid, &snapshot, &transfer_identity())
        .unwrap_err();

    assert_eq!(
        transfer_failure_disposition(&error),
        TransferFailureDisposition::DefinitelyNotStarted
    );
    assert!(runner.requests().is_empty());
    assert!(!error.to_string().contains("PLANTED"));
}

#[test]
fn rsync_stats_parser_rejects_duplicates_malformed_overflow_and_trailing_text() {
    // Break caught: attacker-controlled stdout is partially parsed while
    // duplicate/trailing/malformed counters are silently ignored.
    let cache = tempfile::tempdir().unwrap();
    let (_repo, snapshot) = snapshot(cache.path());
    let invalid = [
        STOCK_STATS.replace_bytes(
            b"Number of files transferred: 4",
            b"Number of files transferred: 4\nNumber of files transferred: 4",
        ),
        STOCK_STATS.replace_bytes(
            b"Total transferred file size: 41 B",
            b"Total transferred file size: nope B",
        ),
        STOCK_STATS.replace_bytes(
            b"Number of files transferred: 4",
            b"Number of files transferred: 18446744073709551616",
        ),
        [STOCK_STATS, b"PLANTED trailing text\n"].concat(),
    ];

    for stdout in invalid {
        let runner = RecordingRunner::returning(vec![Ok(result(status(0), &stdout, b""))]);
        let error = RsyncTransport::new(&runner)
            .upload(&worker(), &snapshot, &transfer_identity())
            .unwrap_err();
        assert!(error.to_string().contains("INVALID_RSYNC_STATS"), "{error}");
        assert!(!error.to_string().contains("PLANTED"));
        assert_eq!(
            transfer_failure_disposition(&error),
            TransferFailureDisposition::ResolveOrAbandon
        );
    }
}

trait ReplaceBytes {
    fn replace_bytes(&self, from: &[u8], to: &[u8]) -> Vec<u8>;
}

impl ReplaceBytes for [u8] {
    fn replace_bytes(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        let offset = self
            .windows(from.len())
            .position(|window| window == from)
            .expect("fixture needle");
        [&self[..offset], to, &self[offset + from.len()..]].concat()
    }
}

#[test]
fn stock_macos_rsync_supports_the_upload_flags_and_local_fixture() {
    // This characterizes the platform dependency instead of mocking it. A
    // platform without the fixed binary is an explicit deterministic skip.
    if !Path::new("/usr/bin/rsync").is_file() {
        eprintln!("skipping: /usr/bin/rsync is absent");
        return;
    }
    let help = Command::new("/usr/bin/rsync")
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    // Apple's openrsync accepts the no-owner/no-group long negations but does
    // not list them in its terse help; the real transfer below characterizes
    // those two flags directly.
    for option in ["--delete", "--stats", "--rsync-path"] {
        assert!(help.contains(option), "stock help omitted {option}");
    }

    let fixture = tempfile::tempdir().unwrap();
    let source = fixture.path().join("source with spaces");
    let destination = fixture.path().join("destination with spaces");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&destination).unwrap();
    fs::write(source.join("unicode-β\nfile"), b"payload").unwrap();
    let mut source_arg = source.as_os_str().as_bytes().to_vec();
    source_arg.push(b'/');
    let output = Command::new("/usr/bin/rsync")
        .args([
            OsStr::new("--archive"),
            OsStr::new("--delete"),
            OsStr::new("--no-owner"),
            OsStr::new("--no-group"),
            OsStr::new("--stats"),
        ])
        .arg(OsString::from_vec(source_arg))
        .arg(&destination)
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env_remove("LANGUAGE")
        .output()
        .unwrap();
    assert!(output.status.success(), "stock rsync failed");
    assert_eq!(
        fs::read(destination.join("unicode-β\nfile")).unwrap(),
        b"payload"
    );
    assert!(output.stdout.starts_with(b"Number of files:"));
}

#[test]
fn stock_macos_client_generates_the_single_allowed_server_shape() {
    if !Path::new("/usr/bin/rsync").is_file() {
        eprintln!("skipping: /usr/bin/rsync is absent");
        return;
    }
    let fixture = tempfile::tempdir().unwrap();
    let source = fixture.path().join("source");
    let capture = fixture.path().join("remote-argv");
    let remote_shell = fixture.path().join("capture-rsh");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("payload"), b"payload").unwrap();
    fs::write(
        &remote_shell,
        b"#!/bin/sh\n: > \"$MAC_WORKER_CAPTURE\"\nfor arg do\n  printf '%s\\n' \"$arg\" >> \"$MAC_WORKER_CAPTURE\"\ndone\nexit 23\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    let mut mode = fs::metadata(&remote_shell).unwrap().permissions();
    mode.set_mode(0o700);
    fs::set_permissions(&remote_shell, mode).unwrap();
    let mut source_arg = source.as_os_str().as_bytes().to_vec();
    source_arg.push(b'/');
    let status = Command::new("/usr/bin/rsync")
        .args([
            OsStr::new("--archive"),
            OsStr::new("--delete"),
            OsStr::new("--no-owner"),
            OsStr::new("--no-group"),
            OsStr::new("--stats"),
            OsStr::new("-e"),
        ])
        .arg(&remote_shell)
        .arg("--rsync-path=~/.local/bin/worker host rsync-receive 00000000000000000000000000000001 00000000000000000000000000000002 00000000000000000000000000000003 dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
        .arg(OsString::from_vec(source_arg))
        .arg("mac1:incoming")
        .env("MAC_WORKER_CAPTURE", &capture)
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env_remove("LANGUAGE")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "capture shell intentionally rejects protocol"
    );

    let actual = fs::read_to_string(&capture).unwrap();
    let actual = actual.lines().collect::<Vec<_>>();
    let expected = [
        "mac1",
        "~/.local/bin/worker",
        "host",
        "rsync-receive",
        "00000000000000000000000000000001",
        "00000000000000000000000000000002",
        "00000000000000000000000000000003",
        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        "--server",
        "--delete-before",
        "-l",
        "-p",
        "-D",
        "-r",
        "-t",
        "--dirs",
        ".",
        "incoming",
    ];
    assert_eq!(
        actual.len(),
        expected.len(),
        "unexpected server argv length"
    );
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert!(actual == &expected, "server argv mismatch at index {index}");
    }
}

struct GateExecutor {
    entered: mpsc::Sender<mpsc::Sender<()>>,
    calls: AtomicUsize,
}

impl RsyncServerExecutor for GateExecutor {
    fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
        assert_eq!(
            invocation.server_args(),
            [
                OsStr::new("--server"),
                OsStr::new("--delete-before"),
                OsStr::new("-l"),
                OsStr::new("-p"),
                OsStr::new("-D"),
                OsStr::new("-r"),
                OsStr::new("-t"),
                OsStr::new("--dirs"),
                OsStr::new("."),
                OsStr::new("."),
            ]
        );
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        invocation.destination().create_empty_directory(
            &RelativePath::parse(format!("payload-{call}").as_bytes()).unwrap(),
        )?;
        let (release, wait) = mpsc::channel();
        self.entered.send(release).unwrap();
        wait.recv_timeout(Duration::from_secs(5))
            .map_err(|_| WorkerError::Protocol("test executor release timed out".into()))?;
        Ok(())
    }
}

#[derive(Default)]
struct CountingExecutor(AtomicUsize);

impl RsyncServerExecutor for CountingExecutor {
    fn execute(&self, invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(invocation.server_args().last(), Some(&OsString::from(".")));
        Ok(())
    }
}

#[test]
fn receiver_rewrites_only_the_literal_sink_and_rejects_every_other_server_shape() {
    // Break caught: arbitrary rsync options or a caller path reach exec.
    let fixture = tempfile::tempdir().unwrap();
    let (store, _request, identity) = leased_store(&fixture.path().join("host"), 10);
    let executor = CountingExecutor::default();
    HostTransferService::new(&store)
        .receive(&identity, &stock_server_args(), &executor)
        .unwrap();
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);

    let invalid = [
        vec![
            "--server",
            "--delete-before",
            "-l",
            "-p",
            "-D",
            "-r",
            "-t",
            "--dirs",
            ".",
            "/tmp/attacker",
        ],
        vec![
            "--server",
            "--delete-before",
            "-l",
            "-p",
            "-D",
            "-r",
            "-t",
            "--dirs",
            ".",
            "incoming",
            "extra",
        ],
        vec![
            "--server",
            "--delete-before",
            "-p",
            "-l",
            "-D",
            "-r",
            "-t",
            "--dirs",
            ".",
            "incoming",
        ],
        vec![
            "--server",
            "--delete-before",
            "-l",
            "-p",
            "-D",
            "-r",
            "-t",
            "--dirs",
            "--",
            "incoming",
        ],
    ];
    for args in invalid {
        let args = args.into_iter().map(OsString::from).collect::<Vec<_>>();
        let error = HostTransferService::new(&store)
            .receive(&identity, &args, &executor)
            .unwrap_err();
        assert!(error.to_string().contains("INVALID_RSYNC_SERVER_ARGS"));
    }
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
}

#[test]
fn exact_receiver_identity_must_match_the_live_lease_before_sink_creation() {
    let fixture = tempfile::tempdir().unwrap();
    let (store, request, identity) = leased_store(&fixture.path().join("host"), 20);
    let wrong = TransferIdentity::new(
        identity.job_id(),
        identity.client_id(),
        LeaseToken::new(uuid::Uuid::from_u128(999)),
        identity.request_fingerprint().clone(),
    );
    let executor = CountingExecutor::default();

    let error = HostTransferService::new(&store)
        .receive(&wrong, &stock_server_args(), &executor)
        .unwrap_err();

    assert!(error.to_string().contains("LEASE_IDENTITY_MISMATCH"));
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert!(
        !store
            .incoming_job(
                request.material().job_id(),
                request.material().lease_token()
            )
            .unwrap()
            .exists()
    );
}

#[test]
fn receiver_first_blocks_abandon_then_tombstone_fences_every_delayed_receiver() {
    // Break caught: abandon can win while a receiver still writes, or a
    // delayed receiver recreates incoming after Abandoned is returned.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let (store, request, identity) = leased_store(&root, 30);
    let (entered_tx, entered_rx) = mpsc::channel();
    let executor = Arc::new(GateExecutor {
        entered: entered_tx,
        calls: AtomicUsize::new(0),
    });

    let receiver_store = store.clone();
    let receiver_identity = identity.clone();
    let receiver_executor = Arc::clone(&executor);
    let receiver = std::thread::spawn(move || {
        HostTransferService::new(&receiver_store).receive(
            &receiver_identity,
            &stock_server_args(),
            receiver_executor.as_ref(),
        )
    });
    let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (resolved_tx, resolved_rx) = mpsc::channel();
    let (classified_tx, classified_rx) = mpsc::channel();
    let resolver_store = HostStore::open(&root).unwrap();
    let resolver_request = ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(
        request.material().clone(),
    ))
    .unwrap();
    let resolver = std::thread::spawn(move || {
        let service = JobService::new_with_resolution_before_transfer(
            &resolver_store,
            &NeverLaunchResolution,
            Arc::new(move || classified_tx.send(()).unwrap()),
        );
        resolved_tx
            .send(service.resolve_or_abandon(resolver_request))
            .unwrap();
    });
    classified_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("resolver did not reach the held transfer lock");
    assert!(matches!(
        resolved_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    receiver.join().unwrap().unwrap();
    let resolved = resolved_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert!(matches!(
        resolved.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
    resolver.join().unwrap();

    let sink = store
        .incoming_job(
            request.material().job_id(),
            request.material().lease_token(),
        )
        .unwrap();
    assert!(!sink.exists());
    let delayed = CountingExecutor::default();
    let error = HostTransferService::new(&store)
        .receive(&identity, &stock_server_args(), &delayed)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ABANDONED"));
    assert_eq!(delayed.0.load(Ordering::SeqCst), 0);
    assert!(!sink.exists());
}

#[test]
fn post_transfer_reread_rejects_live_authority_changed_while_receiver_owned_transfer() {
    // Break caught: resolution trusts its pre-transfer lease observation and
    // tombstones/cleans after the receiver releases even though live authority
    // changed while it was blocked on the shared transfer lock.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let (store, request, identity) = leased_store(&root, 35);
    let (receiver_entered_tx, receiver_entered_rx) = mpsc::channel();
    let executor = Arc::new(GateExecutor {
        entered: receiver_entered_tx,
        calls: AtomicUsize::new(0),
    });
    let receiver_store = store.clone();
    let receiver_identity = identity.clone();
    let receiver_executor = Arc::clone(&executor);
    let receiver = std::thread::spawn(move || {
        HostTransferService::new(&receiver_store).receive(
            &receiver_identity,
            &stock_server_args(),
            receiver_executor.as_ref(),
        )
    });
    let release_receiver = receiver_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();

    let submit = SubmitRequest::new(request.material().clone());
    let resolve = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let (classified_tx, classified_rx) = mpsc::channel();
    let resolver_store = HostStore::open(&root).unwrap();
    let (resolved_tx, resolved_rx) = mpsc::channel();
    let resolver = std::thread::spawn(move || {
        let service = JobService::new_with_resolution_before_transfer(
            &resolver_store,
            &NeverLaunchResolution,
            Arc::new(move || classified_tx.send(()).unwrap()),
        );
        resolved_tx
            .send(service.resolve_or_abandon(resolve))
            .unwrap();
    });
    classified_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("resolver did not complete initial authority classification");
    assert!(matches!(
        resolved_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    let changed_material = RequestFingerprintMaterial::new(
        request.material().job_id(),
        ClientId::new(uuid::Uuid::from_u128(35_901)),
        LeaseToken::new(uuid::Uuid::from_u128(35_902)),
        request.material().worker_name().into(),
        request.material().project_id().into(),
        request.material().worktree_id().into(),
        request.material().manifest_digest().into(),
        request.material().relative_working_dir().into(),
        request.material().timeout_millis(),
        request.material().resource_class().into(),
        request.material().command().clone(),
    )
    .unwrap();
    let changed =
        LeaseRecord::new(&changed_material, changed_material.fingerprint(), 2, 60_002).unwrap();
    let changed_bytes = serde_json::to_vec(&changed).unwrap();
    let live_path = root.join("leases/heavy/lease.json");
    fs::write(&live_path, &changed_bytes).unwrap();
    release_receiver.send(()).unwrap();
    receiver.join().unwrap().unwrap();

    let error = resolved_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap_err();
    resolver.join().unwrap();
    assert!(error.to_string().contains("JOB_ID_CONFLICT"), "{error}");
    assert!(
        !store
            .job_index(request.material().job_id())
            .unwrap()
            .exists()
    );
    assert_eq!(fs::read(live_path).unwrap(), changed_bytes);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn tombstone_first_refuses_before_opening_or_recreating_the_sink() {
    let fixture = tempfile::tempdir().unwrap();
    let (store, request, identity) = leased_store(&fixture.path().join("host"), 40);
    assert_eq!(
        HostTransferService::new(&store)
            .abandon(&request, 2)
            .unwrap(),
        AbandonTransferResult::Abandoned
    );
    let sink = store
        .incoming_job(
            request.material().job_id(),
            request.material().lease_token(),
        )
        .unwrap();
    assert!(!sink.exists());

    let executor = CountingExecutor::default();
    let error = HostTransferService::new(&store)
        .receive(&identity, &stock_server_args(), &executor)
        .unwrap_err();
    assert!(error.to_string().contains("JOB_ABANDONED"));
    assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    assert!(!sink.exists());
}

#[test]
fn index_only_accepted_fences_receiver_but_both_resolution_entries_fail_closed() {
    // Break caught: the legacy transfer entry returns JOB_ACCEPTED from the
    // index alone while the canonical resolver rejects the missing final job.
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let (store, request, identity) = leased_store(&fixture.path().join("host"), 41);
    store
        .record_accepted(&request, &JobStatus::accepted(2).unwrap(), 2)
        .unwrap();
    let receiver_error = HostTransferService::new(&store)
        .receive(
            &identity,
            &stock_server_args(),
            &CountingExecutor::default(),
        )
        .unwrap_err();
    let submit = SubmitRequest::new(request.material().clone());
    let direct_error = JobService::new(&store, &NeverLaunchResolution)
        .resolve_or_abandon(ResolveOrAbandonRequest::from_submit_request(&submit).unwrap())
        .unwrap_err();
    let abandon_error = HostTransferService::new(&store)
        .abandon(&request, 3)
        .unwrap_err();
    assert!(receiver_error.to_string().contains("JOB_ACCEPTED"));
    assert!(direct_error.to_string().contains("JOB_ID_CONFLICT"));
    assert_eq!(abandon_error.to_string(), direct_error.to_string());
}

#[test]
fn accepted_disposition_immutable_mismatches_are_job_id_conflicts() {
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    // Break caught: an unrelated accepted record is misclassified as an
    // idempotent acceptance fence instead of a global job-ID conflict.
    for (field, replacement) in [
        (
            "client",
            ClientId::new(uuid::Uuid::from_u128(999)).to_string(),
        ),
        ("fingerprint", "d".repeat(64)),
        ("project", "e".repeat(64)),
        ("worktree", "f".repeat(64)),
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let (store, request, identity) = leased_store(&fixture.path().join("host"), 42);
        store
            .record_accepted(&request, &JobStatus::accepted(2).unwrap(), 2)
            .unwrap();
        let old = match field {
            "client" => request.material().client_id().to_string(),
            "fingerprint" => request.request_fingerprint().to_string(),
            "project" => request.material().project_id().into(),
            "worktree" => request.material().worktree_id().into(),
            _ => unreachable!(),
        };
        replace_disposition_value(&store, identity.job_id(), &old, &replacement);

        let receiver_error = HostTransferService::new(&store)
            .receive(
                &identity,
                &stock_server_args(),
                &CountingExecutor::default(),
            )
            .unwrap_err();
        let abandon_error = HostTransferService::new(&store)
            .abandon(&request, 3)
            .unwrap_err();
        assert!(
            receiver_error.to_string().contains("JOB_ID_CONFLICT"),
            "{field}: {receiver_error}"
        );
        assert!(
            abandon_error.to_string().contains("JOB_ID_CONFLICT"),
            "{field}: {abandon_error}"
        );
    }
}

#[test]
fn abandoned_disposition_requires_the_full_exact_request_identity() {
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    for field in ["client", "fingerprint", "project", "worktree", "token_hash"] {
        let fixture = tempfile::tempdir().unwrap();
        let (store, request, identity) = leased_store(&fixture.path().join("host"), 43);
        store.record_abandoned(&request, 2).unwrap();
        let token_hash = format!(
            "{:x}",
            sha2::Sha256::digest(request.material().lease_token().to_string().as_bytes())
        );
        let (old, replacement) = match field {
            "client" => (
                request.material().client_id().to_string(),
                ClientId::new(uuid::Uuid::from_u128(999)).to_string(),
            ),
            "fingerprint" => (request.request_fingerprint().to_string(), "d".repeat(64)),
            "project" => (request.material().project_id().into(), "e".repeat(64)),
            "worktree" => (request.material().worktree_id().into(), "f".repeat(64)),
            "token_hash" => (token_hash, "0".repeat(64)),
            _ => unreachable!(),
        };
        replace_disposition_value(&store, identity.job_id(), &old, &replacement);
        let receiver_error = HostTransferService::new(&store)
            .receive(
                &identity,
                &stock_server_args(),
                &CountingExecutor::default(),
            )
            .unwrap_err();
        let abandon_error = HostTransferService::new(&store)
            .abandon(&request, 3)
            .unwrap_err();
        for error in [receiver_error, abandon_error] {
            let rendered = error.to_string();
            assert!(rendered.contains("JOB_ID_CONFLICT"), "{field}: {rendered}");
            assert!(!rendered.contains(&request.material().lease_token().to_string()));
        }
    }
}

#[test]
fn independent_and_cloned_stores_share_one_64_way_transfer_lock_domain() {
    // Break caught: flock self-coalescing or replacement creates concurrent
    // receiver domains. Each entrant stays blocked until explicitly released.
    const CONTENDERS: usize = 64;
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let (store, _request, identity) = leased_store(&root, 50);
    let (entered_tx, entered_rx) = mpsc::channel();
    let executor = Arc::new(GateExecutor {
        entered: entered_tx,
        calls: AtomicUsize::new(0),
    });
    let barrier = Arc::new(Barrier::new(CONTENDERS + 1));
    let mut threads = Vec::new();
    for index in 0..CONTENDERS {
        let contender_store = if index % 2 == 0 {
            store.clone()
        } else {
            HostStore::open(&root).unwrap()
        };
        let contender_identity = identity.clone();
        let contender_executor = Arc::clone(&executor);
        let contender_barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            contender_barrier.wait();
            HostTransferService::new(&contender_store).receive(
                &contender_identity,
                &stock_server_args(),
                contender_executor.as_ref(),
            )
        }));
    }
    barrier.wait();

    for _ in 0..CONTENDERS {
        let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(entered_rx.recv_timeout(Duration::from_millis(25)).is_err());
        release.send(()).unwrap();
    }
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    assert_eq!(executor.calls.load(Ordering::SeqCst), CONTENDERS);
}

#[test]
fn replacing_an_initialized_transfer_lock_cannot_create_a_second_domain() {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let (store, _request, identity) = leased_store(&root, 60);
    let (entered_tx, entered_rx) = mpsc::channel();
    let executor = Arc::new(GateExecutor {
        entered: entered_tx,
        calls: AtomicUsize::new(0),
    });
    let active_store = store.clone();
    let active_identity = identity.clone();
    let active_executor = Arc::clone(&executor);
    let active = std::thread::spawn(move || {
        HostTransferService::new(&active_store).receive(
            &active_identity,
            &stock_server_args(),
            active_executor.as_ref(),
        )
    });
    let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let transfer_dir = root
        .join("locks/jobs")
        .join(identity.job_id().to_string())
        .join("transfer");
    fs::rename(
        transfer_dir.join("transfer.lock"),
        transfer_dir.join("detached.lock"),
    )
    .unwrap();
    fs::write(transfer_dir.join("transfer.lock"), b"").unwrap();
    let mut permissions = fs::metadata(transfer_dir.join("transfer.lock"))
        .unwrap()
        .permissions();
    use std::os::unix::fs::PermissionsExt as _;
    permissions.set_mode(0o600);
    fs::set_permissions(transfer_dir.join("transfer.lock"), permissions).unwrap();

    let second = CountingExecutor::default();
    let error = HostTransferService::new(&HostStore::open(&root).unwrap())
        .receive(&identity, &stock_server_args(), &second)
        .unwrap_err();
    assert_eq!(second.0.load(Ordering::SeqCst), 0);
    assert!(
        error.to_string().contains("transfer lock identity"),
        "{error}"
    );
    release.send(()).unwrap();
    active.join().unwrap().unwrap();
}

#[test]
fn self_consistent_internal_lock_and_identity_replacement_fails_closed() {
    // Break caught: replacing both files consistently lets a fresh store lock
    // a new inode while the active receiver still owns the original flock.
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let (store, _request, identity) = leased_store(&root, 61);
    let (entered_tx, entered_rx) = mpsc::channel();
    let executor = Arc::new(GateExecutor {
        entered: entered_tx,
        calls: AtomicUsize::new(0),
    });
    let active_store = store.clone();
    let active_identity = identity.clone();
    let active_executor = Arc::clone(&executor);
    let active = std::thread::spawn(move || {
        HostTransferService::new(&active_store).receive(
            &active_identity,
            &stock_server_args(),
            active_executor.as_ref(),
        )
    });
    let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let transfer_dir = root
        .join("locks/jobs")
        .join(identity.job_id().to_string())
        .join("transfer");
    fs::rename(
        transfer_dir.join("transfer.lock"),
        transfer_dir.join("detached.lock"),
    )
    .unwrap();
    fs::write(transfer_dir.join("transfer.lock"), b"").unwrap();
    let mut permissions = fs::metadata(transfer_dir.join("transfer.lock"))
        .unwrap()
        .permissions();
    use std::os::unix::fs::PermissionsExt as _;
    permissions.set_mode(0o600);
    fs::set_permissions(transfer_dir.join("transfer.lock"), permissions).unwrap();
    write_self_consistent_internal_transfer_identity(
        &transfer_dir,
        &root
            .join("locks/jobs")
            .join(format!("{}.transfer-lock.json", identity.job_id())),
    );

    let second = CountingExecutor::default();
    let result = HostTransferService::new(&HostStore::open(&root).unwrap()).receive(
        &identity,
        &stock_server_args(),
        &second,
    );
    release.send(()).unwrap();
    active.join().unwrap().unwrap();

    let error = result.expect_err("coordinated internal replacement must fail closed");
    assert_eq!(second.0.load(Ordering::SeqCst), 0);
    assert!(
        error.to_string().contains("transfer lock identity"),
        "{error}"
    );
}

#[test]
fn self_consistent_whole_transfer_directory_replacement_fences_resolver() {
    // Break caught: a resolver locks a replacement directory and publishes an
    // abandonment while the active receiver still owns the detached lock.
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let (store, request, identity) = leased_store(&root, 62);
    let (entered_tx, entered_rx) = mpsc::channel();
    let executor = Arc::new(GateExecutor {
        entered: entered_tx,
        calls: AtomicUsize::new(0),
    });
    let active_store = store.clone();
    let active_identity = identity.clone();
    let active_executor = Arc::clone(&executor);
    let active = std::thread::spawn(move || {
        HostTransferService::new(&active_store).receive(
            &active_identity,
            &stock_server_args(),
            active_executor.as_ref(),
        )
    });
    let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let job_locks = root.join("locks/jobs").join(identity.job_id().to_string());
    let transfer_dir = job_locks.join("transfer");
    let detached = job_locks.join("detached-transfer");
    fs::rename(&transfer_dir, &detached).unwrap();
    fs::create_dir(&transfer_dir).unwrap();
    let mut directory_permissions = fs::metadata(&transfer_dir).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt as _;
    directory_permissions.set_mode(0o700);
    fs::set_permissions(&transfer_dir, directory_permissions).unwrap();
    let lock_name = "transfer.lock";
    fs::copy(detached.join(lock_name), transfer_dir.join(lock_name)).unwrap();
    let mut permissions = fs::metadata(transfer_dir.join(lock_name))
        .unwrap()
        .permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(transfer_dir.join(lock_name), permissions).unwrap();
    write_self_consistent_internal_transfer_identity(
        &transfer_dir,
        &root
            .join("locks/jobs")
            .join(format!("{}.transfer-lock.json", identity.job_id())),
    );

    let result = HostTransferService::new(&HostStore::open(&root).unwrap()).abandon(&request, 2);
    release.send(()).unwrap();
    active.join().unwrap().unwrap();

    let error = result.expect_err("whole transfer replacement must fence resolution");
    assert!(
        error.to_string().contains("transfer lock identity"),
        "{error}"
    );
    assert!(!store.job_index(identity.job_id()).unwrap().exists());
}

#[test]
fn missing_replaced_and_unsafe_external_transfer_identities_fail_closed() {
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    for attack in ["missing", "replaced", "symlink", "permissive"] {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("host");
        let (store, request, identity) = leased_store(&root, 63);
        HostTransferService::new(&store)
            .receive(
                &identity,
                &stock_server_args(),
                &CountingExecutor::default(),
            )
            .unwrap();
        let external = root
            .join("locks/jobs")
            .join(format!("{}.transfer-lock.json", identity.job_id()));
        match attack {
            "missing" => fs::remove_file(&external).unwrap(),
            "replaced" => {
                let bytes = fs::read(&external).unwrap();
                fs::rename(&external, external.with_extension("detached")).unwrap();
                fs::write(&external, bytes).unwrap();
                let mut permissions = fs::metadata(&external).unwrap().permissions();
                use std::os::unix::fs::PermissionsExt as _;
                permissions.set_mode(0o600);
                fs::set_permissions(&external, permissions).unwrap();
            }
            "symlink" => {
                let detached = external.with_extension("detached");
                fs::rename(&external, &detached).unwrap();
                std::os::unix::fs::symlink(&detached, &external).unwrap();
            }
            "permissive" => {
                let mut permissions = fs::metadata(&external).unwrap().permissions();
                use std::os::unix::fs::PermissionsExt as _;
                permissions.set_mode(0o644);
                fs::set_permissions(&external, permissions).unwrap();
            }
            _ => unreachable!(),
        }

        let second = CountingExecutor::default();
        let error = HostTransferService::new(&HostStore::open(&root).unwrap())
            .receive(&identity, &stock_server_args(), &second)
            .unwrap_err();
        assert_eq!(second.0.load(Ordering::SeqCst), 0, "{attack}");
        assert!(
            !error
                .to_string()
                .contains(&request.material().lease_token().to_string()),
            "{attack}: {error}"
        );
        assert!(!store.job_index(identity.job_id()).unwrap().exists());
    }
}

#[test]
fn hidden_binary_receiver_ignores_json_and_keeps_stdout_and_errors_protocol_clean() {
    // Break caught: the binary rsync endpoint loads inventory, writes JSON to
    // stdout, or reflects its secret/path arguments on a pre-exec failure.
    let fixture = tempfile::tempdir().unwrap();
    let environment = BTreeMap::from([(
        OsString::from("XDG_DATA_HOME"),
        fixture.path().join("data").into_os_string(),
    )]);
    let home = fixture.path().join("home");
    let runtime = RuntimeContext::isolated(
        environment.clone(),
        home.clone(),
        fixture.path().to_path_buf(),
    );
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    let (store, request, _identity) = leased_store(&paths.host_state_root(), 70);
    drop(store);
    let material = request.material();
    let fixed = [
        material.job_id().to_string(),
        material.client_id().to_string(),
        material.lease_token().to_string(),
        request.request_fingerprint().to_string(),
    ];

    let mut valid = vec![
        "worker".to_owned(),
        "--json".into(),
        "--config".into(),
        "/definitely/missing/inventory.toml".into(),
        "host".into(),
        "rsync-receive".into(),
    ];
    valid.extend(fixed.iter().cloned());
    valid.extend(
        stock_server_args()
            .into_iter()
            .map(|arg| arg.into_string().unwrap()),
    );
    let cli = Cli::try_parse_from(valid).unwrap();
    let executor = CountingExecutor::default();
    let mut stdin = Cursor::new(b"\0rsync-binary-protocol\xff".to_vec());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_rsync_executor_in_context(
        cli,
        &NoProcess,
        &executor,
        &runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 0);
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);

    let planted = "/tmp/PLANTED-REMOTE-SINK";
    let mut invalid = vec![
        "worker".to_owned(),
        "--json".into(),
        "--config".into(),
        "/definitely/missing/inventory.toml".into(),
        "host".into(),
        "rsync-receive".into(),
    ];
    invalid.extend(fixed.iter().cloned());
    let mut invalid_server = stock_server_args();
    *invalid_server.last_mut().unwrap() = planted.into();
    invalid.extend(
        invalid_server
            .into_iter()
            .map(|arg| arg.into_string().unwrap()),
    );
    let cli = Cli::try_parse_from(invalid).unwrap();
    let mut stdin = Cursor::new(Vec::new());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_rsync_executor_in_context(
        cli,
        &NoProcess,
        &executor,
        &runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );
    assert_ne!(exit, 0);
    assert!(stdout.is_empty());
    assert_eq!(stderr, b"HOST_RSYNC_INVALID_ARGS\n");
    let diagnostic = String::from_utf8(stderr).unwrap();
    assert!(!diagnostic.contains(planted));
    assert!(!diagnostic.contains(&fixed[2]));
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
}

#[test]
fn cleanup_failure_keeps_the_tombstone_and_unsafe_evidence_for_an_exact_retry() {
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let outside = fixture.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("sentinel"), b"keep").unwrap();
    let (store, request, identity) = leased_store(&root, 80);
    let sink = store
        .incoming_job(
            request.material().job_id(),
            request.material().lease_token(),
        )
        .unwrap();
    fs::create_dir_all(sink.parent().unwrap()).unwrap();
    let mut parent_permissions = fs::metadata(sink.parent().unwrap()).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt as _;
    parent_permissions.set_mode(0o700);
    fs::set_permissions(sink.parent().unwrap(), parent_permissions).unwrap();
    std::os::unix::fs::symlink(&outside, &sink).unwrap();

    let error = HostTransferService::new(&store)
        .abandon(&request, 2)
        .unwrap_err();
    assert!(
        !error
            .to_string()
            .contains(&request.material().lease_token().to_string())
    );
    assert!(sink.symlink_metadata().is_ok());
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
    let disposition = fs::read_to_string(store.job_index(identity.job_id()).unwrap()).unwrap();
    assert!(disposition.contains("\"disposition\":\"abandoned\""));
    assert!(!disposition.contains(&request.material().lease_token().to_string()));

    fs::remove_file(&sink).unwrap();
    fs::create_dir(&sink).unwrap();
    let mut sink_permissions = fs::metadata(&sink).unwrap().permissions();
    sink_permissions.set_mode(0o700);
    fs::set_permissions(&sink, sink_permissions).unwrap();
    fs::write(sink.join("partial"), b"evidence").unwrap();
    let mut evidence_permissions = fs::metadata(sink.join("partial")).unwrap().permissions();
    evidence_permissions.set_mode(0o600);
    fs::set_permissions(sink.join("partial"), evidence_permissions).unwrap();

    assert_eq!(
        HostTransferService::new(&store)
            .abandon(&request, 3)
            .unwrap(),
        AbandonTransferResult::Abandoned
    );
    assert!(!sink.exists());
    assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
    let delayed = CountingExecutor::default();
    assert!(
        HostTransferService::new(&store)
            .receive(&identity, &stock_server_args(), &delayed)
            .is_err()
    );
    assert_eq!(delayed.0.load(Ordering::SeqCst), 0);
}

#[test]
fn every_transfer_lock_initialization_crash_is_recoverable_or_fully_published() {
    // Break caught: a crash leaves a split/adopted lock identity or permanently
    // wedges the exact leased job.
    for point in [
        HostStoreWritePoint::AfterTransferDirectoryCreate,
        HostStoreWritePoint::AfterTransferLockSync,
        HostStoreWritePoint::AfterTransferIdentityPublish,
        HostStoreWritePoint::AfterTransferPublish,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().join("host");
        let store = HostStore::open_with_write_fault(&root, point).unwrap();
        let request = acquire_request(100 + point as u128);
        LeaseService::new(&store)
            .acquire(&request, &healthy(), 1)
            .unwrap();
        let identity = TransferIdentity::from_acquire_request(&request).unwrap();
        let first = CountingExecutor::default();
        assert!(
            HostTransferService::new(&store)
                .receive(&identity, &stock_server_args(), &first)
                .is_err(),
            "fault {point:?} must interrupt first initialization"
        );
        assert_eq!(first.0.load(Ordering::SeqCst), 0);
        drop(store);

        let reopened = HostStore::open(&root).unwrap();
        let retry = CountingExecutor::default();
        HostTransferService::new(&reopened)
            .receive(&identity, &stock_server_args(), &retry)
            .unwrap();
        assert_eq!(retry.0.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn tampered_external_marker_never_publishes_its_staged_transfer_directory() {
    let _serial = HOST_FD_STRESS_LOCK.lock().unwrap();
    // Break caught: recovery mutates the canonical namespace before comparing
    // every externally anchored identity field against the staged directory.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().join("host");
    let store =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterTransferIdentityPublish)
            .unwrap();
    let request = acquire_request(119);
    LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap();
    let identity = TransferIdentity::from_acquire_request(&request).unwrap();
    assert!(
        HostTransferService::new(&store)
            .receive(
                &identity,
                &stock_server_args(),
                &CountingExecutor::default(),
            )
            .is_err()
    );
    drop(store);

    let marker = root
        .join("locks/jobs")
        .join(format!("{}.transfer-lock.json", identity.job_id()));
    let bytes = fs::read_to_string(&marker).unwrap();
    fs::write(
        &marker,
        bytes.replacen(
            &format!("{}/transfer/transfer.lock", identity.job_id()),
            &format!("{}/transfer/planted.lock", identity.job_id()),
            1,
        ),
    )
    .unwrap();

    let error = HostTransferService::new(&HostStore::open(&root).unwrap())
        .receive(
            &identity,
            &stock_server_args(),
            &CountingExecutor::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("transfer lock identity"));
    assert!(
        !root
            .join("locks/jobs")
            .join(identity.job_id().to_string())
            .join("transfer")
            .exists()
    );
}

struct NoProcess;

impl ProcessRunner for NoProcess {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("hidden rsync receiver must not invoke the control ProcessRunner")
    }
}
