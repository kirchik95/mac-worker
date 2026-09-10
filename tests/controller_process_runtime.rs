//! Process-level controller acceptance harness.
//!
//! Plumbing tests (`harness_*`) run on accepted de610. Runtime tests
//! (`runtime_*`) assert FLOW-owned enabled routing and stay honest red until
//! that checkpoint; they are not `#[ignore]`.

#[path = "support/controller_process.rs"]
mod controller_process;
mod support;

use std::env;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use clap::Parser;
use controller_process::{FAKE_CONTROLLER_DEST, ProcessFixture, REMOTE_BINARY, TEST_SSH_ENV};
use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    cli::{Cli, Command as WorkerCommand, ControllerCommand, HostCommand, TaskCommand},
    config::Config,
    controller::{OperationEnvelope, decode_frame, encode_json_frame, load_operation_envelope},
    job::{
        ClientId, CommandSpec, ExecutionScope, JobId, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseToken, RequestFingerprintMaterial, StatusLogsRequest, StatusLogsResponse,
        SubmitResponse,
    },
    protocol::{PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION},
    remote_snapshot::{SnapshotVerifyRequest, VerifiedSnapshotResponse},
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta, TaskMetaInput,
    },
    task_store::{
        TaskDiffRequest, TaskDiffResponse, TaskPrepareRequest, TaskPrepareResponse,
        TaskSessionRequest, TaskSessionResponse,
    },
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TaskTurnResponse, TurnMaterial},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use uuid::Uuid;

const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const TOKEN: &str = "018f0f4a6b5c7d8e9f00112233445566";
const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PROJECT_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const WORKTREE_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const OID: &str = "dddddddddddddddddddddddddddddddddddddddd";
const TURN_ID: &str = "118f0f4a6b5c7d8e9f00112233445566";

#[test]
fn harness_separate_homes_do_not_mutate_global_env() {
    let process_home = env::var_os("HOME");
    let process_cwd = env::current_dir().unwrap();
    let fixture = ProcessFixture::new();
    assert_ne!(fixture.laptop_home, fixture.controller_home);
    assert_ne!(fixture.laptop_state(), fixture.controller_state());
    assert!(!fixture.laptop_state().exists());
    assert!(!fixture.controller_state().exists());
    assert_eq!(env::var_os("HOME"), process_home);
    assert_eq!(env::current_dir().unwrap(), process_cwd);
    assert!(env::var_os(TEST_SSH_ENV).is_none());
}

#[test]
fn harness_fake_ssh_is_labeled() {
    let fixture = ProcessFixture::new();
    assert!(fixture.fake_ssh_is_labeled());
    assert!(fixture.fake_ssh.is_absolute());
    let displayed = fixture.fake_ssh.to_string_lossy();
    assert!(
        displayed.contains(' ') && displayed.contains('\''),
        "FLOW quotes MAC_WORKER_TEST_SSH for Git/rsync; path must be quote-sensitive: {displayed}"
    );
    let script = std::fs::read_to_string(&fixture.fake_ssh).unwrap();
    assert!(script.contains("destination is not a live network host"));
    assert!(script.contains("controller_process_fake_exec.py"));
    assert!(script.contains("host upload-pack"));
    assert!(script.contains("git-upload-pack"));
    assert!(script.contains(FAKE_CONTROLLER_DEST));
    assert!(fixture.fake_exec_py.is_file());
    assert!(
        fixture.exec_git.join("HEAD").exists(),
        "fakeexec git must be a real bare repo, not a pre-minted independent worktree"
    );
}

#[test]
fn harness_child_env_uses_test_ssh_not_public_setting() {
    let fixture = ProcessFixture::new();
    let mut command = Command::new("/usr/bin/env");
    fixture.apply_laptop_env(&mut command);
    let output = command.output().expect("env");
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    let expected = format!("{TEST_SSH_ENV}={}", fixture.fake_ssh.to_string_lossy());
    assert!(
        text.lines().any(|line| line == expected),
        "child env must set absolute {TEST_SSH_ENV}; got:\n{text}"
    );
    assert!(
        !text.lines().any(|line| line.starts_with("MAC_WORKER_SSH=")),
        "withdrawn public MAC_WORKER_SSH must not be set on the child"
    );
    assert!(env::var_os(TEST_SSH_ENV).is_none());
}

#[test]
fn harness_public_cli_grammar() {
    let submit = Cli::try_parse_from([
        "worker",
        "task",
        "submit",
        "--prompt",
        "freeze local wip",
        "--wip",
        "--no-wait",
    ])
    .unwrap();
    assert!(matches!(
        submit.command,
        WorkerCommand::Task {
            command: TaskCommand::Submit {
                wip: true,
                no_wait: true,
                ..
            }
        }
    ));

    for (args, describe) in [
        (vec!["worker", "task", "status", TASK_ID], "task status"),
        (vec!["worker", "task", "logs", TASK_ID], "task logs"),
        (vec!["worker", "task", "result", TASK_ID], "task result"),
        (vec!["worker", "task", "fetch", TASK_ID], "task fetch"),
    ] {
        Cli::try_parse_from(args).unwrap_or_else(|error| panic!("{describe}: {error}"));
    }

    let run = Cli::try_parse_from(["worker", "controller", "run"]).unwrap();
    assert!(matches!(
        run.command,
        WorkerCommand::Controller {
            command: ControllerCommand::Run
        }
    ));

    let rpc = Cli::try_parse_from(["worker", "host", "controller-rpc"]).unwrap();
    assert!(matches!(
        rpc.command,
        WorkerCommand::Host {
            command: HostCommand::ControllerRpc
        }
    ));

    let receive = Cli::try_parse_from([
        "worker",
        "host",
        "controller-receive-pack",
        TOKEN,
        TOKEN,
        FINGERPRINT,
        PROJECT_ID,
        WORKTREE_ID,
        OID,
    ])
    .unwrap();
    assert!(matches!(
        receive.command,
        WorkerCommand::Host {
            command: HostCommand::ControllerReceivePack { .. }
        }
    ));

    let upload = Cli::try_parse_from([
        "worker",
        "host",
        "controller-upload-pack",
        TOKEN,
        TOKEN,
        FINGERPRINT,
        TASK_ID,
        TURN_ID,
        OID,
    ])
    .unwrap();
    assert!(matches!(
        upload.command,
        WorkerCommand::Host {
            command: HostCommand::ControllerUploadPack { .. }
        }
    ));
}

#[test]
fn harness_enabled_controller_only_config_parses() {
    let fixture = ProcessFixture::new();
    let laptop =
        std::fs::read_to_string(fixture.laptop_xdg_config.join("mac-worker/config.toml")).unwrap();
    let config = Config::parse(&laptop).unwrap();
    config.validate().unwrap();
    assert!(config.controller.enabled);
    assert_eq!(config.controller.ssh, FAKE_CONTROLLER_DEST);
    assert_eq!(config.controller.remote_binary, REMOTE_BINARY);
    assert!(config.workers.is_empty());

    let controller =
        std::fs::read_to_string(fixture.controller_xdg_config.join("mac-worker/config.toml"))
            .unwrap();
    let config = Config::parse(&controller).unwrap();
    config.validate().unwrap();
    assert_eq!(config.workers.len(), 1);
    assert_eq!(config.workers[0].ssh, controller_process::FAKE_EXEC_DEST);
}

#[test]
fn harness_controller_run_prints_leader_and_reaps() {
    let fixture = ProcessFixture::new();
    let mut leader = fixture.spawn_controller_run();
    let pid = leader.id();
    let line = fixture.wait_until_leader_ready(&mut leader);
    assert!(line.contains("controller leader acquired"));
    assert!(process_alive(pid));
    leader.terminate_and_reap();
    assert!(!process_alive(pid));
}

fn process_alive(pid: u32) -> bool {
    let status = unsafe { libc::kill(pid as i32, 0) };
    status == 0
}

fn decode_protocol_json<T: DeserializeOwned>(stdout: &[u8], stderr: &[u8]) -> T {
    let text = String::from_utf8_lossy(stdout);
    let line = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    serde_json::from_str(line).unwrap_or_else(|error| {
        panic!(
            "expected protocol JSON ({error}); stdout={text} stderr={}",
            String::from_utf8_lossy(stderr)
        )
    })
}

fn plumbing_task_id() -> TaskId {
    TaskId::new(Uuid::from_u128(1))
}

fn plumbing_job_id() -> JobId {
    JobId::new(Uuid::from_u128(10))
}

fn plumbing_client_id() -> ClientId {
    ClientId::new(Uuid::from_u128(20))
}

fn plumbing_lease_token() -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(30))
}

fn plumbing_task_meta(base_oid: BaseOid) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id: plumbing_task_id(),
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        agent: AgentKind::Codex,
        model: None,
        effort: None,
        policy: PermissionPolicy::Workspace,
        source: mac_worker::task::TaskSource::Local {
            wip: false,
            push_target: None,
        },
        publish: vec![PublishMode::Fetch],
        publish_branch: None,
        base_oid,
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "make the requested change".into(),
        created_at_millis: 100,
    })
    .unwrap()
}

fn plumbing_material(manifest_digest: String) -> RequestFingerprintMaterial {
    plumbing_material_for(
        plumbing_job_id(),
        plumbing_lease_token(),
        100,
        manifest_digest,
    )
}

fn plumbing_material_for(
    job_id: JobId,
    lease_token: LeaseToken,
    created_at_millis: u64,
    manifest_digest: String,
) -> RequestFingerprintMaterial {
    RequestFingerprintMaterial::new(
        job_id,
        plumbing_client_id(),
        lease_token,
        created_at_millis,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        manifest_digest,
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap()
}

fn seed_controller_transfer(fixture: &ProcessFixture, source: &support::GitRepo) -> String {
    let stdout = source.git(&["rev-parse", "HEAD"]).stdout;
    let base_oid = String::from_utf8(stdout).unwrap().trim().to_owned();
    assert_eq!(base_oid.len(), 40);
    let cache_root = fixture.controller_xdg_cache.join("mac-worker");
    let dest =
        TransferRepo::controller_transfer_git_path(&cache_root, PROJECT_ID, WORKTREE_ID).unwrap();
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    let clone = source.git(&["clone", "--bare", ".", dest.to_str().unwrap()]);
    assert!(
        clone.status.success(),
        "bare clone into controller-transfer failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    base_oid
}

fn fakeexec_ok<T: DeserializeOwned>(fixture: &ProcessFixture, opcode: &str, stdin: &[u8]) -> T {
    let (status, stdout, stderr) = fixture.run_fakeexec(opcode, stdin);
    assert!(
        status.success(),
        "fakeexec {opcode} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    decode_protocol_json(&stdout, &stderr)
}

/// Protocol-valid fakeexec on de610: JSON opcodes + descendant Git objects.
/// This is not FLOW runtime acceptance (enabled routing still missing).
#[test]
fn harness_fake_exec_protocol_and_descendant_git() {
    let fixture = ProcessFixture::new();
    let source = support_git_repo();
    let base_oid = seed_controller_transfer(&fixture, &source);
    let base: BaseOid = base_oid.parse().unwrap();

    let probe: ProbeResponse = fakeexec_ok(&fixture, "host probe", b"");
    assert_eq!(probe.protocol_version, PROTOCOL_VERSION);
    assert_eq!(probe.supervision_version, SUPERVISION_VERSION);
    assert!(probe.has_free_execution_slot());
    assert!(probe.agent_facts.is_some());

    let prompt = "one protocol-valid fakeexec turn";
    let turn = TurnMaterial::from_prompt(
        plumbing_task_id(),
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        TurnLimits::new(30_000, None, None).unwrap(),
        base.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        false,
    )
    .unwrap();
    let material = plumbing_material(turn.digest());
    let lease_req = LeaseAcquireRequest::new(material.clone())
        .with_execution_scope(ExecutionScope::task(plumbing_task_id()));
    let lease: LeaseAcquireResponse = fakeexec_ok(
        &fixture,
        "host lease-acquire",
        &serde_json::to_vec(&lease_req).unwrap(),
    );
    match lease {
        LeaseAcquireResponse::Acquired { lease } => {
            assert_eq!(lease.job_id(), plumbing_job_id());
        }
        other => panic!("expected acquired lease, got {other:?}"),
    }

    let snapshot = SnapshotVerifyRequest::new(
        plumbing_job_id(),
        plumbing_client_id(),
        plumbing_lease_token(),
        material.fingerprint(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        material.manifest_digest().to_owned(),
    )
    .unwrap();
    let verified: VerifiedSnapshotResponse = fakeexec_ok(
        &fixture,
        "host snapshot-verify",
        &serde_json::to_vec(&snapshot).unwrap(),
    );
    assert_eq!(verified.job_id(), plumbing_job_id());
    assert!(!verified.cache_reused());

    let prepare_req = TaskPrepareRequest::new(
        plumbing_task_meta(base.clone()),
        plumbing_job_id(),
        "mini-1",
    );
    let prepare: TaskPrepareResponse = fakeexec_ok(
        &fixture,
        "host task-prepare",
        &serde_json::to_vec(&prepare_req).unwrap(),
    );
    assert_eq!(prepare.head().as_str(), base_oid);
    assert!(!prepare.reused());

    let session: TaskSessionResponse = fakeexec_ok(
        &fixture,
        "host task-session",
        &serde_json::to_vec(&TaskSessionRequest::new(PROJECT_ID, plumbing_task_id())).unwrap(),
    );
    assert_eq!(session.binding().agent(), AgentKind::Codex);
    assert!(!session.binding().session_ref().is_empty());

    let submit = mac_worker::job::SubmitRequest::new(material.clone())
        .with_execution_scope(ExecutionScope::task(plumbing_task_id()));
    let turn_req = TaskTurnRequest::new(submit, turn, prompt);
    let turn_resp: TaskTurnResponse = fakeexec_ok(
        &fixture,
        "host task-turn",
        &serde_json::to_vec(&turn_req).unwrap(),
    );
    match turn_resp.submit() {
        SubmitResponse::Accepted { meta, status } => {
            assert_eq!(meta.job_id(), plumbing_job_id());
            assert_eq!(status.state(), mac_worker::job::JobState::Accepted);
        }
        other => panic!("expected accepted submit, got {other:?}"),
    }
    let result_oid = turn_resp
        .task()
        .head_oid()
        .expect("task-turn TaskStatus must carry a result head")
        .as_str()
        .to_owned();
    assert_ne!(
        result_oid, base_oid,
        "result must not be the frozen base; fakeexec must commit a descendant"
    );
    assert_eq!(turn_resp.task().turns().len(), 1);

    let logs: StatusLogsResponse = fakeexec_ok(
        &fixture,
        "host status-logs",
        &serde_json::to_vec(&StatusLogsRequest::new(plumbing_job_id(), 0, 32, 0, 32)).unwrap(),
    );
    assert_eq!(logs.status().meta().job_id(), plumbing_job_id());
    assert_eq!(
        logs.status().status().state(),
        mac_worker::job::JobState::Succeeded
    );

    fixture.git_fetch_result(
        source.root(),
        &plumbing_task_id().to_string(),
        &plumbing_client_id().to_string(),
        PROJECT_ID,
    );
    assert!(
        git_object_exists(&source, &result_oid),
        "fetched result commit must exist for actual import/fetch"
    );
    assert_eq!(git_object_type(&source, &result_oid), "commit");
    assert!(
        git_is_ancestor(&source, &base_oid, &result_oid),
        "result {result_oid} must be a descendant of transferred base {base_oid}"
    );
    assert_eq!(
        fixture.journal_opcode_count("host task-turn"),
        1,
        "journal={}",
        fixture.exec_journal()
    );
}

struct FakeexecTurnInput<'a> {
    job_id: JobId,
    lease_token: LeaseToken,
    created_at_millis: u64,
    turn_number: u32,
    base: &'a BaseOid,
    prompt: &'a str,
    resume: bool,
}

fn fakeexec_turn(
    fixture: &ProcessFixture,
    input: FakeexecTurnInput<'_>,
) -> (TaskTurnRequest, TaskTurnResponse) {
    let FakeexecTurnInput {
        job_id,
        lease_token,
        created_at_millis,
        turn_number,
        base,
        prompt,
        resume,
    } = input;
    let turn = TurnMaterial::from_prompt(
        plumbing_task_id(),
        turn_number,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        TurnLimits::new(30_000, None, None).unwrap(),
        base.clone(),
        prompt,
        None,
        Uuid::from_u128(2),
        resume,
    )
    .unwrap();
    let material = plumbing_material_for(job_id, lease_token, created_at_millis, turn.digest());
    let lease_req = LeaseAcquireRequest::new(material.clone())
        .with_execution_scope(ExecutionScope::task(plumbing_task_id()));
    let lease: LeaseAcquireResponse = fakeexec_ok(
        fixture,
        "host lease-acquire",
        &serde_json::to_vec(&lease_req).unwrap(),
    );
    match lease {
        LeaseAcquireResponse::Acquired { lease } => {
            assert_eq!(lease.job_id(), job_id);
        }
        other => panic!("expected acquired lease, got {other:?}"),
    }
    let prepare: TaskPrepareResponse = fakeexec_ok(
        fixture,
        "host task-prepare",
        &serde_json::to_vec(&TaskPrepareRequest::new(
            plumbing_task_meta(base.clone()),
            job_id,
            "mini-1",
        ))
        .unwrap(),
    );
    assert!(!prepare.reused());
    let submit = mac_worker::job::SubmitRequest::new(material)
        .with_execution_scope(ExecutionScope::task(plumbing_task_id()));
    let turn_req = TaskTurnRequest::new(submit, turn, prompt);
    let turn_resp: TaskTurnResponse = fakeexec_ok(
        fixture,
        "host task-turn",
        &serde_json::to_vec(&turn_req).unwrap(),
    );
    (turn_req, turn_resp)
}

fn result_head(turn: &TaskTurnResponse) -> String {
    turn.task()
        .head_oid()
        .expect("task-turn TaskStatus must carry a result head")
        .as_str()
        .to_owned()
}

fn fakeexec_git(fixture: &ProcessFixture, args: &[&str]) -> std::process::Output {
    Command::new("/usr/bin/git")
        .arg(format!("--git-dir={}", fixture.exec_git.display()))
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("fakeexec git")
}

fn fakeexec_result_commit_count(fixture: &ProcessFixture) -> usize {
    let output = fakeexec_git(fixture, &["log", "--all", "--pretty=%s"]);
    assert!(
        output.status.success(),
        "git log failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter(|line| *line == "fake worker result")
        .count()
}

fn fakeexec_commit_parent(fixture: &ProcessFixture, oid: &str) -> String {
    let spec = format!("{oid}^");
    let output = fakeexec_git(fixture, &["rev-parse", &spec]);
    assert!(
        output.status.success(),
        "rev-parse {oid}^ failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn journal_turn_job_ids(fixture: &ProcessFixture) -> Vec<String> {
    fixture
        .journal_task_turns()
        .into_iter()
        .filter_map(|row| {
            row.get("job_id")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .collect()
}

/// First turn, second turn on head1, exact first-turn retry: two real
/// descendant commits, two unique job identities, retry does not move HEAD.
/// `task-diff` is real git diff from the original task base.
#[test]
fn harness_fake_exec_followup_commits_and_task_diff() {
    let fixture = ProcessFixture::new();
    let source = support_git_repo();
    let frozen = seed_controller_transfer(&fixture, &source);
    let frozen_oid: BaseOid = frozen.parse().unwrap();

    let job1 = plumbing_job_id();
    let (turn1_req, turn1) = fakeexec_turn(
        &fixture,
        FakeexecTurnInput {
            job_id: job1,
            lease_token: plumbing_lease_token(),
            created_at_millis: 100,
            turn_number: 1,
            base: &frozen_oid,
            prompt: "first protocol-valid fakeexec turn",
            resume: false,
        },
    );
    let head1 = result_head(&turn1);
    assert_ne!(head1, frozen);
    assert_eq!(fakeexec_commit_parent(&fixture, &head1), frozen);
    assert_eq!(turn1.task().turns().len(), 1);

    let job2 = JobId::new(Uuid::from_u128(11));
    let head1_oid: BaseOid = head1.parse().unwrap();
    let (_turn2_req, turn2) = fakeexec_turn(
        &fixture,
        FakeexecTurnInput {
            job_id: job2,
            lease_token: LeaseToken::new(Uuid::from_u128(31)),
            created_at_millis: 200,
            turn_number: 2,
            base: &head1_oid,
            prompt: "second protocol-valid fakeexec turn",
            resume: true,
        },
    );
    let head2 = result_head(&turn2);
    assert_ne!(head2, head1);
    assert_ne!(head2, frozen);
    assert_eq!(fakeexec_commit_parent(&fixture, &head2), head1);
    assert_eq!(turn2.task().turns().len(), 2);
    assert_eq!(
        turn2.task().turns()[0].turn_id().to_string(),
        job1.to_string()
    );
    assert_eq!(
        turn2.task().turns()[1].turn_id().to_string(),
        job2.to_string()
    );
    assert_eq!(turn2.task().head_oid().unwrap().as_str(), head2.as_str());
    assert_eq!(fakeexec_result_commit_count(&fixture), 2);

    let retry: TaskTurnResponse = fakeexec_ok(
        &fixture,
        "host task-turn",
        &serde_json::to_vec(&turn1_req).unwrap(),
    );
    assert_eq!(
        result_head(&retry),
        head2,
        "retry must not regress current head"
    );
    assert_eq!(fakeexec_result_commit_count(&fixture), 2);
    let task_ref = fakeexec_git(
        &fixture,
        &[
            "rev-parse",
            &format!("refs/heads/task/{}", plumbing_task_id()),
        ],
    );
    assert!(task_ref.status.success());
    assert_eq!(String::from_utf8(task_ref.stdout).unwrap().trim(), head2);

    let job_ids = journal_turn_job_ids(&fixture);
    assert_eq!(job_ids.len(), 3, "journal={}", fixture.exec_journal());
    let unique: std::collections::BTreeSet<_> = job_ids.iter().cloned().collect();
    assert_eq!(unique.len(), 2, "journal={}", fixture.exec_journal());
    assert_eq!(job_ids[0], job1.to_string());
    assert_eq!(job_ids[1], job2.to_string());
    assert_eq!(job_ids[2], job1.to_string());
    let turns = fixture.journal_task_turns();
    assert_eq!(turns[0]["base_oid"], frozen);
    assert_eq!(turns[0]["task_id"], plumbing_task_id().to_string());
    assert_eq!(turns[0]["turn_number"], 1);
    assert_eq!(turns[1]["base_oid"], head1);
    assert_eq!(turns[1]["task_id"], plumbing_task_id().to_string());
    assert_eq!(turns[1]["turn_number"], 2);
    assert_eq!(turns[2]["base_oid"], frozen);
    assert_eq!(turns[2]["job_id"], job1.to_string());

    let patch: TaskDiffResponse = fakeexec_ok(
        &fixture,
        "host task-diff",
        &serde_json::to_vec(&TaskDiffRequest::new(PROJECT_ID, plumbing_task_id(), false)).unwrap(),
    );
    assert_eq!(patch.protocol_version(), PROTOCOL_VERSION);
    assert!(!patch.truncated());
    assert!(
        patch.text().contains("RESULT.txt"),
        "patch={}",
        patch.text()
    );
    assert!(
        patch.text().contains("fake-worker-result"),
        "patch must be real git diff, not identity text: {}",
        patch.text()
    );
    assert!(
        !patch.text().contains(&plumbing_task_id().to_string()),
        "diff text must not be canned identity"
    );

    let stat: TaskDiffResponse = fakeexec_ok(
        &fixture,
        "host task-diff",
        &serde_json::to_vec(&TaskDiffRequest::new(PROJECT_ID, plumbing_task_id(), true)).unwrap(),
    );
    assert!(!stat.truncated());
    assert!(stat.text().contains("RESULT.txt"), "stat={}", stat.text());
    assert!(
        !stat.text().contains(&head1) && !stat.text().contains(&head2),
        "stat must not embed result OIDs: {}",
        stat.text()
    );
}

fn laptop_tasks_exist(root: &Path) -> bool {
    root.join("tasks").exists()
}

fn parse_json_value(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).unwrap_or_else(|_| {
        let text = String::from_utf8_lossy(stdout);
        panic!("expected JSON stdout, got: {text}");
    })
}

fn json_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn status_object(report: &Value) -> &Value {
    report.get("status").unwrap_or(report)
}

fn turn_completed_with_oid(
    report: &Value,
    expected_oid: &str,
    expected_turn: Option<&str>,
) -> bool {
    terminal_head_oid(report, expected_turn) == Some(expected_oid)
}

fn terminal_head_oid<'a>(report: &'a Value, expected_turn: Option<&str>) -> Option<&'a str> {
    let status = status_object(report);
    let state = json_string(status, "state").unwrap_or("");
    if !matches!(state, "open" | "closed") {
        return None;
    }
    let outcome = status
        .get("last_outcome")
        .and_then(|value| json_string(value, "kind"))
        .unwrap_or("");
    if outcome != "done" {
        return None;
    }
    let turns = status.get("turns").and_then(Value::as_array)?;
    if turns.len() != 1 {
        return None;
    }
    let turn = &turns[0];
    if json_string(turn, "terminal") != Some("succeeded") {
        return None;
    }
    if turn
        .get("outcome")
        .and_then(|value| json_string(value, "kind"))
        != Some("done")
    {
        return None;
    }
    if let Some(expected_turn) = expected_turn {
        if json_string(turn, "turn_id") != Some(expected_turn) {
            return None;
        }
    }
    json_string(status, "head_oid")
}

fn envelope_from_laptop(fixture: &ProcessFixture) -> OperationEnvelope {
    let paths = fixture.envelope_paths();
    assert_eq!(
        paths.len(),
        1,
        "enabled submit must persist one laptop OperationEnvelope; found {paths:?}"
    );
    let name = paths[0].file_name().unwrap().to_string_lossy().to_string();
    let request_id = name
        .strip_prefix("op-")
        .and_then(|name| name.strip_suffix(".json"))
        .unwrap_or_else(|| panic!("unexpected envelope name {name}"));
    load_operation_envelope(&fixture.laptop_controller_cache(), request_id)
        .expect("load envelope")
        .unwrap_or_else(|| panic!("missing envelope {request_id}"))
}

fn rpc_frame(envelope: &OperationEnvelope, body: Value) -> Vec<u8> {
    encode_json_frame(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": envelope.request_id(),
        "command": envelope.command(),
        "body": body,
    }))
    .expect("encode rpc frame")
}

fn decode_rpc_json(stdout: &[u8], stderr: &[u8]) -> Value {
    let payload = decode_frame(stdout).unwrap_or_else(|error| {
        panic!(
            "rpc stdout was not a controller frame ({error}); stdout={} stderr={}",
            String::from_utf8_lossy(stdout),
            String::from_utf8_lossy(stderr)
        )
    });
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        panic!(
            "rpc payload was not JSON; stdout={} stderr={}",
            String::from_utf8_lossy(stdout),
            String::from_utf8_lossy(stderr)
        )
    })
}

fn frozen_oid(body: &Value) -> Option<&str> {
    json_string(body, "base_oid")
        .or_else(|| json_string(body, "oid"))
        .or_else(|| json_string(body, "head_oid"))
}

fn git_object_exists(repo: &support::GitRepo, oid: &str) -> bool {
    repo.git(&["cat-file", "-t", oid]).status.success()
}

fn git_object_type(repo: &support::GitRepo, oid: &str) -> String {
    String::from_utf8_lossy(&repo.git(&["cat-file", "-t", oid]).stdout)
        .trim()
        .to_owned()
}

fn git_is_ancestor(repo: &support::GitRepo, ancestor: &str, oid: &str) -> bool {
    repo.git(&["merge-base", "--is-ancestor", ancestor, oid])
        .status
        .success()
}

fn wait_for_terminal_turn(
    fixture: &ProcessFixture,
    repo: &support::GitRepo,
    task_id: &str,
    expected_turn: Option<&str>,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, stdout, stderr) =
            fixture.run_laptop(&["--json", "task", "status", task_id], Some(repo.root()));
        let last_status = String::from_utf8_lossy(&stdout);
        let last_stderr = String::from_utf8_lossy(&stderr);
        assert!(
            status.success(),
            "reconnect status must not open a laptop store; stdout={last_status} stderr={last_stderr}"
        );
        let report = parse_json_value(&stdout);
        if terminal_head_oid(&report, expected_turn).is_some() {
            return report;
        }
        if Instant::now() > deadline {
            panic!(
                "blocked FLOW seam: controller/runner did not reach a terminal succeeded turn; status={last_status} stderr={last_stderr} journal={}",
                fixture.exec_journal()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Enabled submit must fail closed when the fakecontroller SSH hop is
/// refused. Omitting `controller run` is not a transport outage: one-shot
/// RPC still execs the real worker.
#[test]
fn runtime_outage_does_not_create_laptop_state() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo();
    let (status, stdout, stderr) = fixture.run_laptop_with_test_ssh(
        &fixture.controller_outage_ssh(),
        &[
            "task",
            "submit",
            "--prompt",
            "outage must not create laptop tasks",
            "--wip",
            "--no-wait",
        ],
        Some(repo.root()),
    );
    let stdout = String::from_utf8_lossy(&stdout);
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(
        !status.success(),
        "enabled outage must fail; stdout={stdout} stderr={stderr}"
    );
    assert!(
        stderr.contains("CONTROLLER_UNAVAILABLE") || stdout.contains("CONTROLLER_UNAVAILABLE"),
        "blocked FLOW seam: enabled task CLI must return CONTROLLER_UNAVAILABLE when the controller is down; stdout={stdout} stderr={stderr}"
    );
    assert!(
        !fixture.laptop_state().exists() && !laptop_tasks_exist(&fixture.laptop_state()),
        "blocked FLOW seam: enabled outage must not create laptop ClientStateStore at {:?}",
        fixture.laptop_state()
    );
}

/// Short-lived laptop submit → controller execution → reconnect status/fetch.
#[test]
fn runtime_short_lived_submit_imports_exact_result() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo();
    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let (status, stdout, stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "one local wip through the controller",
            "--wip",
            "--no-wait",
        ],
        Some(repo.root()),
    );
    let stdout_text = String::from_utf8_lossy(&stdout);
    let stderr_text = String::from_utf8_lossy(&stderr);
    assert!(
        status.success(),
        "blocked FLOW seam: enabled task submit must ACK through host controller-rpc; status={status} stdout={stdout_text} stderr={stderr_text}"
    );
    assert!(
        !fixture.laptop_state().exists(),
        "blocked FLOW seam: laptop must not open ClientStateStore; path={:?}",
        fixture.laptop_state()
    );
    let submit = parse_json_value(&stdout);
    let task_id = json_string(&submit, "task_id")
        .unwrap_or_else(|| panic!("submit ACK JSON must include task_id; stdout={stdout_text}"))
        .to_owned();
    let expected_turn = json_string(&submit, "turn_id").map(str::to_owned);
    let envelope = envelope_from_laptop(&fixture);
    let frozen = frozen_oid(envelope.body())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            panic!(
                "enabled submit envelope must carry the frozen base OID; body={:?}",
                envelope.body()
            )
        });

    let completed = wait_for_terminal_turn(&fixture, &repo, &task_id, expected_turn.as_deref());
    let status = status_object(&completed);
    let result_oid = json_string(status, "head_oid")
        .expect("terminal status must include head_oid")
        .to_owned();
    assert!(
        turn_completed_with_oid(&completed, &result_oid, expected_turn.as_deref()),
        "terminal status must match the imported result; status={completed}"
    );
    assert_ne!(
        result_oid, frozen,
        "result head must be a fakeexec descendant of the transferred frozen base, not the base itself"
    );
    let turns = status.get("turns").and_then(Value::as_array).unwrap();
    assert_eq!(turns.len(), 1, "one execution; status={completed}");

    let (wait_status, wait_stdout, wait_stderr) =
        fixture.wait_for_task_quiescence(Some(repo.root()), &task_id);
    let wait_text = String::from_utf8_lossy(&wait_stdout);
    let wait_err = String::from_utf8_lossy(&wait_stderr);
    assert!(
        wait_status.success(),
        "blocked FLOW seam: public task wait must confirm quiescence before import proof; stdout={wait_text} stderr={wait_err}"
    );

    let imported = fixture.controller_imported_oids();
    assert_eq!(
        imported,
        vec![result_oid.clone()],
        "controller imported head must be fetched_head plus controller Git, not a source OID or export token"
    );
    assert!(
        !git_object_exists(&repo, &result_oid),
        "laptop Git must not hold the result commit before explicit fetch"
    );

    let (fetch_status, fetch_stdout, fetch_stderr) =
        fixture.run_laptop(&["--json", "task", "fetch", &task_id], Some(repo.root()));
    let fetch_text = String::from_utf8_lossy(&fetch_stdout);
    let fetch_err = String::from_utf8_lossy(&fetch_stderr);
    assert!(
        fetch_status.success(),
        "explicit laptop fetch must obtain the imported OID; stdout={fetch_text} stderr={fetch_err}"
    );
    let fetch = parse_json_value(&fetch_stdout);
    assert_eq!(
        json_string(&fetch, "head_oid"),
        Some(result_oid.as_str()),
        "fetched OID must equal fake-worker result {result_oid}; stdout={fetch_text}"
    );
    assert!(
        git_object_exists(&repo, &result_oid),
        "exact result commit must exist in the laptop Git object store"
    );
    assert_eq!(git_object_type(&repo, &result_oid), "commit");
    assert!(
        git_is_ancestor(&repo, &frozen, &result_oid),
        "imported result {result_oid} must be a descendant of frozen base {frozen}"
    );
    assert_eq!(
        fixture.journal_opcode_count("host task-turn"),
        1,
        "one execution; journal={}",
        fixture.exec_journal()
    );
    assert!(
        !fixture.laptop_state().exists(),
        "reconnect/fetch must not create laptop ClientStateStore"
    );
    drop(leader);
}

/// Original envelope replay after laptop HEAD change and controller restart.
#[test]
fn runtime_envelope_replay_after_controller_restart() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo();
    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);
    let (first_status, first_stdout, first_stderr) = fixture.run_laptop(
        &[
            "--json",
            "task",
            "submit",
            "--prompt",
            "replay original envelope",
            "--wip",
            "--no-wait",
        ],
        Some(repo.root()),
    );
    let first_stdout_text = String::from_utf8_lossy(&first_stdout);
    let first_stderr_text = String::from_utf8_lossy(&first_stderr);
    assert!(
        first_status.success(),
        "blocked FLOW seam: first enabled submit must ACK; stdout={first_stdout_text} stderr={first_stderr_text}"
    );
    let first = parse_json_value(&first_stdout);
    let first_task = json_string(&first, "task_id")
        .unwrap_or_else(|| {
            panic!("first ACK JSON must include task_id; stdout={first_stdout_text}")
        })
        .to_owned();
    let first_turn = json_string(&first, "turn_id").map(str::to_owned);
    let envelope = envelope_from_laptop(&fixture);
    let original_body = envelope.body().clone();
    let original_oid = frozen_oid(&original_body).map(str::to_owned);
    let original_request = envelope.request_id().to_owned();
    let original_command = envelope.command().to_owned();
    assert_ne!(
        original_command, "checkpoint.submit",
        "initial CLI must persist a real task submit envelope, not CP1 checkpoint.submit"
    );
    wait_for_terminal_turn(&fixture, &repo, &first_task, first_turn.as_deref());

    repo.write("changed.txt", b"laptop HEAD moved\n");
    repo.commit_all("laptop moved HEAD");
    std::fs::write(repo.root().join(".worker.toml"), "model = \"changed\"\n").ok();

    leader.terminate_and_reap();
    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let frame = rpc_frame(&envelope, original_body.clone());
    let (rpc_status, rpc_stdout, rpc_stderr) = fixture.run_controller_rpc(&frame);
    let rpc_err = String::from_utf8_lossy(&rpc_stderr);
    assert!(
        rpc_status.success(),
        "blocked FLOW seam: original envelope must replay over child controller-rpc; stdout={} stderr={rpc_err}",
        String::from_utf8_lossy(&rpc_stdout)
    );
    let ack = decode_rpc_json(&rpc_stdout, &rpc_stderr);
    assert_eq!(
        json_string(&ack, "request_id"),
        Some(original_request.as_str())
    );
    assert_eq!(json_string(&ack, "task_id"), Some(first_task.as_str()));
    if let Some(turn) = first_turn.as_deref() {
        assert_eq!(json_string(&ack, "turn_id"), Some(turn));
    }
    let reloaded = load_operation_envelope(&fixture.laptop_controller_cache(), &original_request)
        .unwrap()
        .expect("original envelope remains");
    assert_eq!(reloaded.body(), &original_body);
    assert_eq!(frozen_oid(reloaded.body()).map(str::to_owned), original_oid);

    let mut conflict_body = original_body.clone();
    conflict_body
        .as_object_mut()
        .expect("submit body is an object")
        .insert("conflict_marker".into(), json!(true));
    let conflict_frame = rpc_frame(&envelope, conflict_body);
    let (conflict_status, conflict_stdout, conflict_stderr) =
        fixture.run_controller_rpc(&conflict_frame);
    let conflict_out = String::from_utf8_lossy(&conflict_stdout);
    let conflict_err = String::from_utf8_lossy(&conflict_stderr);
    assert!(
        !conflict_status.success()
            || conflict_out.contains("CONTROLLER_REQUEST_CONFLICT")
            || conflict_err.contains("CONTROLLER_REQUEST_CONFLICT"),
        "same request_id with a different body must be CONTROLLER_REQUEST_CONFLICT; stdout={conflict_out} stderr={conflict_err}"
    );
    assert!(
        conflict_out.contains("CONTROLLER_REQUEST_CONFLICT")
            || conflict_err.contains("CONTROLLER_REQUEST_CONFLICT"),
        "conflict public code missing; stdout={conflict_out} stderr={conflict_err}"
    );
    wait_for_terminal_turn(&fixture, &repo, &first_task, first_turn.as_deref());
    assert_eq!(
        fixture.journal_opcode_count("host task-turn"),
        1,
        "replay must not start a second execution; journal={}",
        fixture.exec_journal()
    );
    drop(leader);
}

fn support_git_repo() -> support::GitRepo {
    let repo = support::GitRepo::init();
    repo.write("src.txt", b"fixture source\n");
    repo.commit_all("fixture");
    repo
}
