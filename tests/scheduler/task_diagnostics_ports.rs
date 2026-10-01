use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    process::ExitStatus,
};

use crate::{support::recording_runner::RecordingRunner, task_ports_fixture::canonical};
use mac_worker::{
    RuntimeContext,
    agent::{AgentKind, PermissionPolicy},
    cli::{Cli, Command, TaskCommand},
    client_state::ClientStateStore,
    job::{
        CommandSpec, JobId, JobMeta, JobStatus, LeaseToken, LocalJobRecord, RemoteUncertainty,
        RequestFingerprintMaterial,
    },
    paths::PathLayout,
    process::ProcessResult,
    protocol::PROTOCOL_VERSION,
    run_with_io_in_context,
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnSummary,
    },
};

pub(super) struct PublicRuntime {
    pub paths: PathLayout,
    pub context: RuntimeContext,
}
pub(super) fn runtime(root: &Path) -> PublicRuntime {
    let root = root.canonicalize().unwrap();
    let home = root.join("home");
    fs::create_dir(&home).unwrap();
    let environment = BTreeMap::from([
        (
            OsString::from("XDG_STATE_HOME"),
            root.join("state").into_os_string(),
        ),
        (
            OsString::from("XDG_CACHE_HOME"),
            root.join("cache").into_os_string(),
        ),
        (
            OsString::from("XDG_DATA_HOME"),
            root.join("data").into_os_string(),
        ),
    ]);
    let paths = PathLayout::discover(Some(root.join("config.toml")), &environment, &home).unwrap();
    fs::write(
        &paths.config,
        "version = 1\n[[workers]]\nname = 'mini-a'\nssh = 'mini-a'\nslots = 1\n",
    )
    .unwrap();
    PublicRuntime {
        paths,
        context: RuntimeContext::isolated(environment, home, root),
    }
}
pub(super) fn seed_task(store: &ClientStateStore, seed: u128, worker: &str) -> LocalTaskRecord {
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(uuid::Uuid::from_u128(seed)),
        run_id: None,
        project_id: "a".repeat(64),
        worktree_id: "b".repeat(64),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid: "a".repeat(40).parse().unwrap(),
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: None,
        prompt: "private fixture prompt".into(),
        created_at_millis: 100,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Active,
        None,
        Some(worker.into()),
        true,
        Some(meta.base_oid().clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        vec![TurnSummary::new(
            1,
            JobId::new(uuid::Uuid::from_u128(seed + 100)),
            None,
            None,
            None,
            false,
            Some(100),
            None,
        )],
        101,
    )
    .unwrap();
    let record = LocalTaskRecord::new(
        meta,
        status,
        None,
        None,
        None,
        "c".repeat(64),
        None,
        false,
        None,
    )
    .unwrap();
    store.create_task(record.clone()).unwrap();
    record
}
fn seed_legacy_job(store: &ClientStateStore) -> JobId {
    // Canonical retained read-only job state, not a batch execution fixture.
    let job = JobId::new(uuid::Uuid::from_u128(900));
    let material = RequestFingerprintMaterial::new(
        job,
        store.client_id(),
        LeaseToken::generate(),
        100,
        "mini-a".into(),
        "a".repeat(64),
        "b".repeat(64),
        "c".repeat(64),
        "".into(),
        30_000,
        "heavy".into(),
        CommandSpec::argv(vec!["true".into()]).unwrap(),
    )
    .unwrap();
    let record = LocalJobRecord::new(
        JobMeta::new(&material, material.fingerprint()).unwrap(),
        material.lease_token(),
        Some(JobStatus::accepted(101).unwrap()),
        RemoteUncertainty::None,
    )
    .unwrap();
    store.create_job(record).unwrap();
    job
}
fn cli(config: PathBuf, json: bool, command: TaskCommand) -> Cli {
    Cli {
        config: Some(config),
        json,
        command: Command::Task { command },
    }
}
fn events(bytes: &[u8]) -> Vec<serde_json::Value> {
    bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}
fn assert_error(bytes: &[u8], code: &str, message: &str) {
    let events = events(bytes);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event"], "error");
    assert_eq!(events[0]["protocol_version"], PROTOCOL_VERSION);
    assert_eq!(events[0]["code"], code);
    assert_eq!(events[0]["message"], message);
}
fn bytes_for(paths: &PathLayout, task: TaskId, job: JobId) -> Vec<Vec<u8>> {
    [
        paths.state.join("tasks").join(format!("{task}.json")),
        paths.state.join("jobs").join(format!("{job}.json")),
    ]
    .iter()
    .map(|path| fs::read(path).unwrap())
    .collect()
}
fn failing_envelope(code: &str, message: &str, oversized: bool) -> ProcessResult {
    let mut result = if oversized {
        ProcessResult { status: ExitStatus::from_raw(0), stdout: format!(r#"{{"protocol_version":{PROTOCOL_VERSION},"error":{{"code":"{code}","message":"{message}"}}}}"#).into_bytes(), stderr: Vec::new() }
    } else {
        canonical(&mac_worker::job::HostControlError::new(code, message).unwrap()).unwrap()
    };
    result.status = ExitStatus::from_raw(23 << 8);
    result
}

#[test]
fn task_diag_json_error_keeps_exit_and_rejects_oversized_internal_payload() {
    // Supersedes run_command::public_diag_json_error_keeps_exit_and_rejects_oversized_internal_payload.
    let private = [
        "/Users/alice/PLANTED_PUBLIC_PATH",
        "PLANTED_PUBLIC_SECRET",
        "printf TASK9_COMMAND_SECRET",
        "planted-local-mac.example",
        "018f0f4a-6b5c-7d8e-9f00-112233445566",
        "018f0f4a-6b5c-7d8e-9f00-112233445577",
    ];
    let detail = format!("{} {}", "X".repeat(3900), private.join(" "));
    assert!(detail.len() <= 4096);
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state).unwrap();
    let task = seed_task(&store, 1, "mini-a");
    let runner = RecordingRunner::returning(failing_envelope("INVALID_REQUEST", &detail, false));
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_io_in_context(
        cli(
            runtime.paths.config,
            true,
            TaskCommand::Diff {
                task_id: task.meta().task_id(),
                stat: false,
            },
        ),
        &runner,
        &runtime.context,
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(exit, 70);
    assert!(stderr.is_empty());
    assert_error(&stdout, "INVALID_REQUEST", "protocol error");
    let text = String::from_utf8_lossy(&stdout);
    for planted in private
        .into_iter()
        .chain(std::iter::once("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"))
    {
        assert!(!text.contains(planted), "leaked {planted}");
    }
    assert_eq!(
        runner.single_request().args.last().unwrap(),
        "~/.local/bin/worker host task-diff"
    );
}

#[test]
fn task_diag_missing_config_is_sanitized_for_human_and_json() {
    // Supersedes run_command::public_diag_missing_config_is_sanitized_for_human_and_json.
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let missing = temp.path().join("PLANTED_PUBLIC_SECRET/missing.toml");
    assert!(missing.is_absolute());
    let task_id = TaskId::new(uuid::Uuid::from_u128(1));
    for kind in ["list", "logs", "status", "cancel"] {
        for json in [false, true] {
            let command = match kind {
                "list" => TaskCommand::List {
                    run: None,
                    state: None,
                    outcome: None,
                    full: false,
                },
                "logs" => TaskCommand::Logs {
                    task_id,
                    turn: None,
                    follow: false,
                    raw: false,
                },
                "status" => TaskCommand::Status {
                    task_id,
                    full: false,
                },
                _ => TaskCommand::Cancel { task_id },
            };
            let runner = RecordingRunner::default();
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let exit = run_with_io_in_context(
                cli(missing.clone(), json, command),
                &runner,
                &runtime.context,
                &mut stdout,
                &mut stderr,
            );
            assert_eq!(exit, 64, "{kind} json={json}");
            assert!(runner.requests().is_empty());
            let combined = [stdout.as_slice(), stderr.as_slice()].concat();
            let text = String::from_utf8_lossy(&combined);
            assert!(!text.contains(&*missing.to_string_lossy()));
            assert!(!text.contains("PLANTED_PUBLIC_SECRET"));
            if json {
                assert!(stderr.is_empty());
                assert_error(&stdout, "CONFIG_MISSING", "configuration error");
            } else {
                assert!(stdout.is_empty());
                assert_eq!(
                    String::from_utf8(stderr).unwrap(),
                    format!(
                        "CONFIG_MISSING: configuration error\n{}\n",
                        mac_worker::error::hint_for("CONFIG_MISSING").unwrap()
                    )
                );
            }
        }
    }
}

#[test]
fn task_diag_oversized_host_envelope_is_sanitized_and_preserves_persisted_state() {
    // Supersedes run_command::public_diag_oversized_authoritative_host_envelope_stays_sanitized_and_preserves_persisted_state.
    for stat in [false, true] {
        for json in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let runtime = runtime(temp.path());
            let store = ClientStateStore::open(&runtime.paths.state).unwrap();
            let task = seed_task(&store, 1, "mini-a");
            let job = seed_legacy_job(&store);
            let before = bytes_for(&runtime.paths, task.meta().task_id(), job);
            let runner = RecordingRunner::returning(failing_envelope(
                "OVERSIZED_HOST_ENVELOPE",
                &format!("{}PLANTED_OVERSIZED_SECRET", "X".repeat(200_000)),
                true,
            ));
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let exit = run_with_io_in_context(
                cli(
                    runtime.paths.config.clone(),
                    json,
                    TaskCommand::Diff {
                        task_id: task.meta().task_id(),
                        stat,
                    },
                ),
                &runner,
                &runtime.context,
                &mut stdout,
                &mut stderr,
            );
            assert_eq!(exit, 69, "stat={stat} json={json}");
            let combined = [stdout.as_slice(), stderr.as_slice()].concat();
            let text = String::from_utf8_lossy(&combined);
            for planted in [
                "OVERSIZED_HOST_ENVELOPE",
                "PLANTED_OVERSIZED_SECRET",
                &"X".repeat(64),
            ] {
                assert!(!text.contains(planted));
            }
            assert!(text.len() < 1000);
            if json {
                assert!(stderr.is_empty());
                assert_error(&stdout, "HOST_REQUEST_FAILED", "transport error");
            } else {
                assert!(stdout.is_empty());
                assert_eq!(
                    String::from_utf8(stderr).unwrap(),
                    "HOST_REQUEST_FAILED: transport error\n"
                );
            }
            assert_eq!(
                bytes_for(&runtime.paths, task.meta().task_id(), job),
                before
            );
            assert_eq!(
                runner.single_request().args.last().unwrap(),
                "~/.local/bin/worker host task-diff"
            );
        }
    }
}

#[test]
fn task_diag_non_utf8_config_path_is_sanitized_and_preserves_persisted_state() {
    // Supersedes run_command::public_diag_non_utf8_config_path_stays_sanitized_and_the_persisted_job_record_is_unchanged.
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state).unwrap();
    let task = seed_task(&store, 1, "mini-a");
    let job = seed_legacy_job(&store);
    let before = bytes_for(&runtime.paths, task.meta().task_id(), job);
    let missing = temp
        .path()
        .join(OsString::from_vec(b"non-utf8-\xffconfig-dir".to_vec()))
        .join("missing.toml");
    assert!(std::str::from_utf8(missing.as_os_str().as_bytes()).is_err());
    for json in [false, true] {
        let runner = RecordingRunner::default();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_io_in_context(
            cli(
                missing.clone(),
                json,
                TaskCommand::Status {
                    task_id: task.meta().task_id(),
                    full: false,
                },
            ),
            &runner,
            &runtime.context,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(exit, 64);
        assert!(runner.requests().is_empty());
        if json {
            assert!(stderr.is_empty());
            assert_error(&stdout, "CONFIG_MISSING", "configuration error");
        } else {
            assert!(stdout.is_empty());
            assert_eq!(
                String::from_utf8(stderr).unwrap(),
                format!(
                    "CONFIG_MISSING: configuration error\n{}\n",
                    mac_worker::error::hint_for("CONFIG_MISSING").unwrap()
                )
            );
        }
    }
    assert_eq!(
        bytes_for(&runtime.paths, task.meta().task_id(), job),
        before
    );
}

#[test]
fn task_json_error_uses_stdout_only_and_error_writer_failure_is_74() {
    // Supersedes the dispatcher assertions in run_command::json_stream_errors_use_stdout_only_and_writer_failure_is_74; controller_json_stream_errors_keep_stdout_only_and_writer_failure_is_74 proves the prefix case.
    struct FailWrite {
        writes: usize,
        flushes: usize,
    }
    impl std::io::Write for FailWrite {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "planted error writer failure",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let runtime = runtime(temp.path());
    let store = ClientStateStore::open(&runtime.paths.state).unwrap();
    let task = seed_task(&store, 1, "mini-a");
    let job = seed_legacy_job(&store);
    let before = bytes_for(&runtime.paths, task.meta().task_id(), job);
    let failing_runner = || {
        RecordingRunner::returning(ProcessResult {
            status: ExitStatus::from_raw(255 << 8),
            stdout: Vec::new(),
            stderr: b"planted private mac1".to_vec(),
        })
    };
    let command = || {
        cli(
            runtime.paths.config.clone(),
            true,
            TaskCommand::Diff {
                task_id: task.meta().task_id(),
                stat: false,
            },
        )
    };
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    assert_eq!(
        run_with_io_in_context(
            command(),
            &failing_runner(),
            &runtime.context,
            &mut stdout,
            &mut stderr
        ),
        69
    );
    assert!(stderr.is_empty());
    assert_error(&stdout, "SSH_UNAVAILABLE", "transport error");
    assert!(!String::from_utf8_lossy(&stdout).contains("mac1"));
    let mut stdout = FailWrite {
        writes: 0,
        flushes: 0,
    };
    let mut stderr = Vec::new();
    assert_eq!(
        run_with_io_in_context(
            command(),
            &failing_runner(),
            &runtime.context,
            &mut stdout,
            &mut stderr
        ),
        74
    );
    assert!(stderr.is_empty());
    assert_eq!(stdout.writes, 1);
    assert_eq!(stdout.flushes, 0);
    assert_eq!(
        bytes_for(&runtime.paths, task.meta().task_id(), job),
        before
    );
}
