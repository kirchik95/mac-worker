//! Process-level `task.batch` fixtures on the 102a protocol-valid harness.
//!
//! Plumbing (`harness_*`) compiles and runs against 3b847. Runtime tests
//! (`runtime_*`) assert FLOW-owned enabled `task.batch` routing and stay
//! present without `#[ignore]`. Public batch grammar is one checkout plus
//! per-task `base` refs/OIDs; there is no TOML `project`. Do not treat a
//! helper green as FLOW runtime acceptance. No production edits. No seeded
//! completion.

#[path = "support/controller_process.rs"]
mod controller_process;
mod support;

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Parser;
use controller_process::{ProcessFixture, TEST_SSH_ENV};
use mac_worker::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    cli::{Cli, Command as WorkerCommand, TaskCommand},
    config::Config,
    controller::{
        canonical_request_sha256, decode_frame, encode_json_frame, load_operation_envelope,
        BatchKind, FrozenBatchBody, OperationEnvelope,
    },
    dag::DagBase,
    job::{
        ClientId, CommandSpec, ExecutionScope, JobId, LeaseAcquireRequest, LeaseAcquireResponse,
        LeaseToken, RequestFingerprintMaterial, StatusLogsRequest, StatusLogsResponse,
        SubmitResponse,
    },
    process::SystemProcessRunner,
    project_state::ProjectState,
    protocol::PROTOCOL_VERSION,
    remote_snapshot::{SnapshotVerifyRequest, VerifiedSnapshotResponse},
    task::{
        BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta, TaskMetaInput,
    },
    task_client::BatchFile,
    task_store::{
        TaskPrepareRequest, TaskPrepareResponse, TaskSessionRequest, TaskSessionResponse,
    },
    transfer_repo::TransferRepo,
    turn::{TaskTurnRequest, TaskTurnResponse, TurnMaterial},
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use uuid::Uuid;

const RUNNER: SystemProcessRunner = SystemProcessRunner;

#[test]
fn harness_batch_cli_grammar() {
    let batch = Cli::try_parse_from(["worker", "task", "batch", "tasks.toml"]).unwrap();
    assert!(matches!(
        batch.command,
        WorkerCommand::Task {
            command: TaskCommand::Batch {
                max_parallel: None,
                wait: false,
                preview: false,
                ..
            }
        }
    ));

    let omitted = Cli::try_parse_from(["worker", "--json", "task", "batch", "tasks.toml"]).unwrap();
    assert!(matches!(
        omitted.command,
        WorkerCommand::Task {
            command: TaskCommand::Batch {
                max_parallel: None,
                ..
            }
        }
    ));

    Cli::try_parse_from(["worker", "task", "close", TASK_ID])
        .unwrap_or_else(|error| panic!("task close: {error}"));
    Cli::try_parse_from(["worker", "task", "fetch", TASK_ID])
        .unwrap_or_else(|error| panic!("task fetch: {error}"));
}

#[test]
fn harness_laptop_workers_empty_controller_slots_resolve_omitted_parallel() {
    let fixture = ProcessFixture::new();
    fixture.set_controller_slots(2);
    let laptop =
        std::fs::read_to_string(fixture.laptop_xdg_config.join("mac-worker/config.toml")).unwrap();
    let laptop_config = Config::parse(&laptop).unwrap();
    laptop_config.validate().unwrap();
    assert!(laptop_config.workers.is_empty());
    assert!(laptop_config.controller.enabled);

    let controller =
        std::fs::read_to_string(fixture.controller_xdg_config.join("mac-worker/config.toml"))
            .unwrap();
    let controller_config = Config::parse(&controller).unwrap();
    controller_config.validate().unwrap();
    assert_eq!(controller_config.configured_runner_slots(), 2);
    assert_ne!(
        laptop_config.configured_runner_slots(),
        controller_config.configured_runner_slots()
    );
    assert!(std::env::var_os(TEST_SSH_ENV).is_none());
}

#[test]
fn harness_batch_file_parses_two_committed_bases_and_rejects_project() {
    let checkout = two_committed_bases();
    assert_ne!(checkout.alpha_oid, checkout.beta_oid);

    let path = checkout.repo.root().join("two-root.toml");
    write_independent_two_root_batch(&path);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(!text.contains("max_parallel"));
    assert!(!text.contains("project"));
    let parsed: BatchFile = toml::from_str(&text).expect("public two-root batch must parse");
    assert_eq!(parsed.tasks.len(), 2);
    assert_eq!(parsed.tasks[0].id.as_deref(), Some("alpha"));
    assert_eq!(parsed.tasks[0].base.as_deref(), Some("alpha-base"));
    assert_eq!(parsed.tasks[1].id.as_deref(), Some("beta"));
    assert_eq!(parsed.tasks[1].base.as_deref(), Some("beta-base"));
    assert!(parsed.tasks.iter().all(|task| task.wip.is_none()));

    let dag_path = checkout.repo.root().join("from-parent.toml");
    write_from_parent_batch(&dag_path);
    let dag: BatchFile =
        toml::from_str(&std::fs::read_to_string(&dag_path).unwrap()).expect("from:parent batch");
    assert_eq!(dag.tasks[0].id.as_deref(), Some("parent"));
    assert_eq!(dag.tasks[0].close_on.as_deref(), Some("never"));
    assert_eq!(dag.tasks[1].base.as_deref(), Some("from:parent"));
    assert!(std::fs::read_to_string(&dag_path)
        .unwrap()
        .lines()
        .all(|line| !line.contains("project")));

    let unknown = r#"
version = 1
[[tasks]]
id = "alpha"
prompt = "unknown project key must not parse"
project = "/tmp/not-public-grammar"
"#;
    let error = toml::from_str::<BatchFile>(unknown)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unknown field") && error.contains("project"),
        "BatchTask deny_unknown_fields must reject project=; got {error}"
    );
}

/// Two frozen bases from one checkout mint their own descendants.
/// Plumbing only: does not submit through enabled CLI or seed runtime tasks.
#[test]
fn harness_fakeexec_two_bases_use_own_captured_descendants() {
    let fixture = ProcessFixture::new();
    let checkout = two_committed_bases();
    seed_controller_transfer(
        &fixture,
        &checkout.repo,
        &checkout.project_id,
        &checkout.worktree_id,
    );

    let alpha_task = task_id_n(11);
    let beta_task = task_id_n(12);
    let alpha_result = fakeexec_one_turn(
        &fixture,
        alpha_task,
        &checkout.project_id,
        &checkout.worktree_id,
        &checkout.alpha_oid,
        110,
        "alpha turn",
    );
    let beta_result = fakeexec_one_turn(
        &fixture,
        beta_task,
        &checkout.project_id,
        &checkout.worktree_id,
        &checkout.beta_oid,
        120,
        "beta turn",
    );
    assert_ne!(alpha_result, checkout.alpha_oid);
    assert_ne!(beta_result, checkout.beta_oid);
    assert_ne!(alpha_result, beta_result);

    fixture.git_fetch_result(
        checkout.repo.root(),
        &alpha_task.to_string(),
        &client_id_n(110).to_string(),
        &checkout.project_id,
    );
    fixture.git_fetch_result(
        checkout.repo.root(),
        &beta_task.to_string(),
        &client_id_n(120).to_string(),
        &checkout.project_id,
    );
    assert!(git_is_ancestor(
        &checkout.repo,
        &checkout.alpha_oid,
        &alpha_result
    ));
    assert!(git_is_ancestor(
        &checkout.repo,
        &checkout.beta_oid,
        &beta_result
    ));
    assert!(
        !git_is_ancestor(&checkout.repo, &checkout.beta_oid, &alpha_result),
        "alpha result must not be a descendant of beta's captured base"
    );

    let turns = fixture.journal_task_turns();
    assert_eq!(turns.len(), 2, "journal={}", fixture.exec_journal());
    let bases: HashSet<&str> = turns
        .iter()
        .filter_map(|row| row.get("base_oid").and_then(Value::as_str))
        .collect();
    assert_eq!(
        bases,
        HashSet::from([checkout.alpha_oid.as_str(), checkout.beta_oid.as_str()])
    );
}

/// Parent result becomes the child's frozen base; child result descends from it.
/// Not FLOW parent-acceptance: that path is `runtime_dag_from_parent_*`.
#[test]
fn harness_fakeexec_child_descends_from_parent_result_oid() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo("src.txt", b"parent source\n");
    let (project_id, worktree_id, parent_base) = inspect_repo(&repo);
    seed_controller_transfer(&fixture, &repo, &project_id, &worktree_id);

    let parent_task = task_id_n(21);
    let child_task = task_id_n(22);
    let parent_result = fakeexec_one_turn(
        &fixture,
        parent_task,
        &project_id,
        &worktree_id,
        &parent_base,
        210,
        "parent turn",
    );
    assert_eq!(
        fixture.journal_task_turns().len(),
        1,
        "child must not execute before the parent result exists; journal={}",
        fixture.exec_journal()
    );
    let child_result = fakeexec_one_turn(
        &fixture,
        child_task,
        &project_id,
        &worktree_id,
        &parent_result,
        220,
        "child turn from parent result",
    );
    fixture.git_fetch_result(
        repo.root(),
        &parent_task.to_string(),
        &client_id_n(210).to_string(),
        &project_id,
    );
    fixture.git_fetch_result(
        repo.root(),
        &child_task.to_string(),
        &client_id_n(220).to_string(),
        &project_id,
    );
    assert!(git_is_ancestor(&repo, &parent_base, &parent_result));
    assert!(git_is_ancestor(&repo, &parent_result, &child_result));
    assert_ne!(child_result, parent_result);

    let turns = fixture.journal_task_turns();
    assert_eq!(turns.len(), 2, "journal={}", fixture.exec_journal());
    let child_row = turns
        .iter()
        .find(|row| row.get("task_id").and_then(Value::as_str) == Some(&child_task.to_string()))
        .expect("child task-turn journal row");
    assert_eq!(
        child_row.get("base_oid").and_then(Value::as_str),
        Some(parent_result.as_str()),
        "child execution base must be the accepted parent result OID"
    );
}

/// One `task.batch` RPC, two independent roots with distinct committed bases in
/// one current checkout. Omitted `--max-parallel` resolved on the controller.
#[test]
fn runtime_independent_two_root_batch_uses_own_bases() {
    let fixture = ProcessFixture::new();
    fixture.set_controller_slots(2);
    let checkout = two_committed_bases();
    let batch = fixture.laptop_home.join("two-root.toml");
    write_independent_two_root_batch(&batch);

    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let (status, stdout, stderr) = fixture.run_laptop(
        &["--json", "task", "batch", batch.to_str().unwrap()],
        Some(checkout.repo.root()),
    );
    let stdout_text = String::from_utf8_lossy(&stdout);
    let stderr_text = String::from_utf8_lossy(&stderr);
    assert!(
        status.success(),
        "blocked FLOW seam: enabled task batch must ACK one task.batch RPC; stdout={stdout_text} stderr={stderr_text}"
    );
    assert!(
        !fixture.laptop_state().exists(),
        "blocked FLOW seam: laptop must not open ClientStateStore; path={:?}",
        fixture.laptop_state()
    );
    let submit = parse_json_value(&stdout);
    let run_id = json_string(&submit, "run_id")
        .unwrap_or_else(|| panic!("batch ACK JSON must include run_id; stdout={stdout_text}"))
        .to_owned();
    let task_ids = json_strings(&submit, "task_ids");
    assert_eq!(
        task_ids.len(),
        2,
        "one RunId with two stable TaskIds; stdout={stdout_text}"
    );
    assert_ne!(task_ids[0], task_ids[1]);

    let envelope = envelope_from_laptop(&fixture);
    assert_eq!(
        envelope.command(),
        "task.batch",
        "no per-node RPC substitute; command={}",
        envelope.command()
    );
    assert_eq!(
        canonical_request_sha256(PROTOCOL_VERSION, envelope.command(), envelope.body()).unwrap(),
        envelope.payload_sha256(),
        "sources bind with the OUTER batch digest"
    );
    let body: FrozenBatchBody =
        serde_json::from_value(envelope.body().clone()).unwrap_or_else(|error| {
            panic!(
                "blocked FLOW seam: envelope body must be FrozenBatchBody ({error}); body={:?}",
                envelope.body()
            )
        });
    assert!(
        envelope.body().get("max_parallel").is_none(),
        "omitted CLI --max-parallel must omit the wire key; body={:?}",
        envelope.body()
    );
    assert!(
        body.max_parallel.is_none(),
        "omitted CLI --max-parallel must stay None on FrozenBatchBody"
    );
    assert_eq!(body.kind, BatchKind::Independent);
    assert_eq!(body.run_id.to_string(), run_id);
    assert_eq!(body.sources.len(), 2, "two nested source receipts");
    let nested: HashSet<&str> = body
        .sources
        .iter()
        .map(|source| source.request_id.as_str())
        .collect();
    assert_eq!(nested.len(), 2);
    assert!(!nested.contains(envelope.request_id()));
    for source in &body.sources {
        assert_eq!(source.request_id.len(), 32);
        assert!(source.request_id.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_eq!(source.project_id, checkout.project_id);
        assert_eq!(source.worktree_id, checkout.worktree_id);
    }
    let source_oids: HashSet<&str> = body
        .sources
        .iter()
        .map(|source| source.expected_oid.as_str())
        .collect();
    assert_eq!(
        source_oids,
        HashSet::from([checkout.alpha_oid.as_str(), checkout.beta_oid.as_str()]),
        "two nested receipts must keep distinct expected_oid; do not collapse to one OID"
    );

    let alpha_task = task_id_for_node(&body, "alpha");
    let beta_task = task_id_for_node(&body, "beta");
    let alpha_turn = turn_id_for_task(&body, &alpha_task);
    let beta_turn = turn_id_for_task(&body, &beta_task);
    let alpha_base = frozen_oid_for_task(&body, &alpha_task);
    let beta_base = frozen_oid_for_task(&body, &beta_task);
    assert_eq!(alpha_base, checkout.alpha_oid);
    assert_eq!(beta_base, checkout.beta_oid);
    assert_eq!(
        sorted_strings(&task_ids),
        sorted_strings(&[alpha_task.clone(), beta_task.clone()])
    );

    let alpha_done =
        wait_for_terminal_turn(&fixture, &checkout.repo, &alpha_task, Some(&alpha_turn));
    let beta_done = wait_for_terminal_turn(&fixture, &checkout.repo, &beta_task, Some(&beta_turn));
    let alpha_result = terminal_head_oid(&alpha_done, Some(&alpha_turn))
        .expect("alpha terminal head")
        .to_owned();
    let beta_result = terminal_head_oid(&beta_done, Some(&beta_turn))
        .expect("beta terminal head")
        .to_owned();
    assert_ne!(alpha_result, alpha_base);
    assert_ne!(beta_result, beta_base);

    for task_id in [&alpha_task, &beta_task] {
        let (wait_status, wait_stdout, wait_stderr) =
            fixture.wait_for_task_quiescence(Some(checkout.repo.root()), task_id);
        assert!(
            wait_status.success(),
            "public task wait must confirm quiescence before import/fetch; stdout={} stderr={}",
            String::from_utf8_lossy(&wait_stdout),
            String::from_utf8_lossy(&wait_stderr)
        );
    }

    let (alpha_fetch_status, alpha_fetch_out, alpha_fetch_err) = fixture.run_laptop(
        &["--json", "task", "fetch", &alpha_task],
        Some(checkout.repo.root()),
    );
    assert!(
        alpha_fetch_status.success(),
        "alpha fetch must bind this checkout/task; stdout={} stderr={}",
        String::from_utf8_lossy(&alpha_fetch_out),
        String::from_utf8_lossy(&alpha_fetch_err)
    );
    let (beta_fetch_status, beta_fetch_out, beta_fetch_err) = fixture.run_laptop(
        &["--json", "task", "fetch", &beta_task],
        Some(checkout.repo.root()),
    );
    assert!(
        beta_fetch_status.success(),
        "beta fetch must bind this checkout/task; stdout={} stderr={}",
        String::from_utf8_lossy(&beta_fetch_out),
        String::from_utf8_lossy(&beta_fetch_err)
    );
    assert_eq!(
        json_string(&parse_json_value(&alpha_fetch_out), "head_oid"),
        Some(alpha_result.as_str())
    );
    assert_eq!(
        json_string(&parse_json_value(&beta_fetch_out), "head_oid"),
        Some(beta_result.as_str())
    );
    assert!(git_object_exists(&checkout.repo, &alpha_result));
    assert!(git_is_ancestor(&checkout.repo, &alpha_base, &alpha_result));
    assert!(git_object_exists(&checkout.repo, &beta_result));
    assert!(git_is_ancestor(&checkout.repo, &beta_base, &beta_result));

    let imported = fixture.controller_imported_oids();
    assert_eq!(
        sorted_strings(&imported),
        sorted_strings(&[alpha_result.clone(), beta_result.clone()])
    );

    let turns = fixture.journal_task_turns();
    assert_eq!(
        turns.len(),
        2,
        "one execution per root; journal={}",
        fixture.exec_journal()
    );
    let executed_bases: HashSet<&str> = turns
        .iter()
        .filter_map(|row| row.get("base_oid").and_then(Value::as_str))
        .collect();
    assert_eq!(
        executed_bases,
        HashSet::from([alpha_base.as_str(), beta_base.as_str()]),
        "each actual execution must use its own captured base"
    );
    drop(leader);
}

/// DAG root Done + `close_on=never` holds the child until actual `task close`.
#[test]
fn runtime_dag_from_parent_requires_actual_close() {
    let fixture = ProcessFixture::new();
    let repo = support_git_repo("src.txt", b"parent source\n");
    let batch = fixture.laptop_home.join("from-parent.toml");
    write_from_parent_batch(&batch);

    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let (status, stdout, stderr) = fixture.run_laptop(
        &["--json", "task", "batch", batch.to_str().unwrap()],
        Some(repo.root()),
    );
    let stdout_text = String::from_utf8_lossy(&stdout);
    let stderr_text = String::from_utf8_lossy(&stderr);
    assert!(
        status.success(),
        "blocked FLOW seam: enabled DAG batch must ACK; stdout={stdout_text} stderr={stderr_text}"
    );
    let submit = parse_json_value(&stdout);
    let envelope = envelope_from_laptop(&fixture);
    assert_eq!(envelope.command(), "task.batch");
    let body: FrozenBatchBody = serde_json::from_value(envelope.body().clone()).unwrap();
    assert_eq!(body.kind, BatchKind::Dag);
    assert_eq!(
        body.sources.len(),
        1,
        "from: nodes are not source rows; sources={:?}",
        body.sources
    );
    let parent_node = body.nodes.get("parent").expect("parent node");
    let child_node = body.nodes.get("child").expect("child node");
    assert!(matches!(child_node.base, DagBase::From { .. }));
    assert!(parent_node.depends_on.is_empty());
    assert_eq!(child_node.depends_on, vec!["parent".to_string()]);
    let parent_task = parent_node.task_id.to_string();
    let child_task = child_node.task_id.to_string();
    let parent_turn = parent_node.turn_id.to_string();
    let child_turn = child_node.turn_id.to_string();
    let expected_run = body.run_id.to_string();
    assert_eq!(json_string(&submit, "run_id"), Some(expected_run.as_str()));

    let parent_done = wait_for_terminal_turn(&fixture, &repo, &parent_task, Some(&parent_turn));
    let parent_result = terminal_head_oid(&parent_done, Some(&parent_turn))
        .expect("parent Done head")
        .to_owned();
    let parent_status = status_object(&parent_done);
    assert_eq!(json_string(parent_status, "state"), Some("open"));
    assert_eq!(
        parent_status
            .get("last_outcome")
            .and_then(|value| json_string(value, "kind")),
        Some("done")
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        assert_eq!(
            fixture.journal_task_turns().len(),
            1,
            "child must not execute while close_on=never parent is Open+Done; journal={}",
            fixture.exec_journal()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let (child_status_rc, child_stdout, _) = fixture.run_laptop(
        &["--json", "task", "status", &child_task],
        Some(repo.root()),
    );
    if child_status_rc.success() {
        let child_report = parse_json_value(&child_stdout);
        assert!(
            terminal_head_oid(&child_report, Some(&child_turn)).is_none(),
            "child must not be terminal before parent close; status={child_report}"
        );
    }

    let (close_status, close_stdout, close_stderr) = fixture.run_laptop(
        &["--json", "task", "close", &parent_task],
        Some(repo.root()),
    );
    assert!(
        close_status.success(),
        "blocked FLOW seam: parent accept is actual enabled task close; stdout={} stderr={}",
        String::from_utf8_lossy(&close_stdout),
        String::from_utf8_lossy(&close_stderr)
    );

    let child_done = wait_for_terminal_turn(&fixture, &repo, &child_task, Some(&child_turn));
    let child_result = terminal_head_oid(&child_done, Some(&child_turn))
        .expect("child terminal head")
        .to_owned();
    assert_ne!(child_result, parent_result);

    let child_row = fixture
        .journal_task_turns()
        .into_iter()
        .find(|row| row.get("task_id").and_then(Value::as_str) == Some(child_task.as_str()))
        .expect("child task-turn after parent close");
    assert_eq!(
        child_row.get("base_oid").and_then(Value::as_str),
        Some(parent_result.as_str()),
        "child base from:parent must be the exact accepted result OID"
    );

    let (fetch_parent, parent_out, parent_err) = fixture.run_laptop(
        &["--json", "task", "fetch", &parent_task],
        Some(repo.root()),
    );
    let (fetch_child, child_out, child_err) =
        fixture.run_laptop(&["--json", "task", "fetch", &child_task], Some(repo.root()));
    assert!(
        fetch_parent.success(),
        "parent fetch; stdout={} stderr={}",
        String::from_utf8_lossy(&parent_out),
        String::from_utf8_lossy(&parent_err)
    );
    assert!(
        fetch_child.success(),
        "child fetch must bind the same repo/task; stdout={} stderr={}",
        String::from_utf8_lossy(&child_out),
        String::from_utf8_lossy(&child_err)
    );
    assert_eq!(
        json_string(&parse_json_value(&parent_out), "head_oid"),
        Some(parent_result.as_str())
    );
    assert_eq!(
        json_string(&parse_json_value(&child_out), "head_oid"),
        Some(child_result.as_str())
    );
    assert!(git_object_exists(&repo, &parent_result));
    assert!(git_object_exists(&repo, &child_result));
    assert!(git_is_ancestor(&repo, &parent_result, &child_result));
    drop(leader);
}

/// Save the original `task.batch` envelope, restart the controller, change laptop
/// HEAD/settings, replay the exact envelope. Not a second `task batch`.
#[test]
fn runtime_batch_envelope_replay_after_controller_restart() {
    let fixture = ProcessFixture::new();
    fixture.set_controller_slots(2);
    let checkout = two_committed_bases();
    let batch = fixture.laptop_home.join("two-root.toml");
    write_independent_two_root_batch(&batch);

    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);
    let (first_status, first_stdout, first_stderr) = fixture.run_laptop(
        &["--json", "task", "batch", batch.to_str().unwrap()],
        Some(checkout.repo.root()),
    );
    let first_stdout_text = String::from_utf8_lossy(&first_stdout);
    let first_stderr_text = String::from_utf8_lossy(&first_stderr);
    assert!(
        first_status.success(),
        "blocked FLOW seam: first enabled batch must ACK; stdout={first_stdout_text} stderr={first_stderr_text}"
    );
    let first = parse_json_value(&first_stdout);
    let first_run = json_string(&first, "run_id")
        .expect("first ACK run_id")
        .to_owned();
    let first_tasks = json_strings(&first, "task_ids");
    let envelope = envelope_from_laptop(&fixture);
    let original_body = envelope.body().clone();
    let original_request = envelope.request_id().to_owned();
    assert_eq!(envelope.command(), "task.batch");
    let body: FrozenBatchBody = serde_json::from_value(original_body.clone()).unwrap();
    let turns_by_task: BTreeMap<String, String> = body
        .nodes
        .values()
        .map(|node| (node.task_id.to_string(), node.turn_id.to_string()))
        .collect();
    let frozen_oids: BTreeMap<String, String> = body
        .nodes
        .values()
        .map(|node| {
            let oid = match &node.base {
                DagBase::Frozen { oid, .. } => oid.as_str().to_owned(),
                other => panic!("replay fixture uses Frozen roots, got {other:?}"),
            };
            (node.task_id.to_string(), oid)
        })
        .collect();
    assert_eq!(
        frozen_oids.values().cloned().collect::<HashSet<_>>(),
        HashSet::from([checkout.alpha_oid.clone(), checkout.beta_oid.clone()])
    );
    for (task_id, turn_id) in &turns_by_task {
        wait_for_terminal_turn(&fixture, &checkout.repo, task_id, Some(turn_id));
    }
    let executed_before = fixture.journal_task_turns().len();
    assert_eq!(executed_before, 2);

    checkout.repo.write("changed.txt", b"laptop HEAD moved\n");
    checkout.repo.commit_all("laptop moved HEAD");
    std::fs::write(
        checkout.repo.root().join(".worker.toml"),
        "model = \"changed\"\n",
    )
    .ok();

    leader.terminate_and_reap();
    let mut leader = fixture.spawn_controller_run();
    fixture.wait_until_leader_ready(&mut leader);

    let frame = rpc_frame(&envelope, original_body.clone());
    let (rpc_status, rpc_stdout, rpc_stderr) = fixture.run_controller_rpc(&frame);
    assert!(
        rpc_status.success(),
        "blocked FLOW seam: original batch envelope must replay over child controller-rpc; stdout={} stderr={}",
        String::from_utf8_lossy(&rpc_stdout),
        String::from_utf8_lossy(&rpc_stderr)
    );
    let ack = decode_rpc_json(&rpc_stdout, &rpc_stderr);
    assert_eq!(
        json_string(&ack, "request_id"),
        Some(original_request.as_str())
    );
    assert_eq!(json_string(&ack, "run_id"), Some(first_run.as_str()));
    assert_eq!(json_strings(&ack, "task_ids"), first_tasks);
    let reloaded = load_operation_envelope(&fixture.laptop_controller_cache(), &original_request)
        .unwrap()
        .expect("original envelope remains");
    assert_eq!(reloaded.body(), &original_body);
    assert_eq!(reloaded.command(), "task.batch");
    let replayed: FrozenBatchBody = serde_json::from_value(reloaded.body().clone()).unwrap();
    assert_eq!(replayed.run_id.to_string(), first_run);
    for (task_id, oid) in &frozen_oids {
        assert_eq!(frozen_oid_for_task(&replayed, task_id), *oid);
        assert_eq!(turn_id_for_task(&replayed, task_id), turns_by_task[task_id]);
    }
    assert!(reloaded.body().get("max_parallel").is_none());

    let mut conflict_body = original_body.clone();
    conflict_body
        .as_object_mut()
        .expect("batch body is an object")
        .insert("conflict_marker".into(), json!(true));
    let (conflict_status, conflict_stdout, conflict_stderr) =
        fixture.run_controller_rpc(&rpc_frame(&envelope, conflict_body));
    let conflict_out = String::from_utf8_lossy(&conflict_stdout);
    let conflict_err = String::from_utf8_lossy(&conflict_stderr);
    assert!(
        !conflict_status.success()
            || conflict_out.contains("CONTROLLER_REQUEST_CONFLICT")
            || conflict_err.contains("CONTROLLER_REQUEST_CONFLICT"),
        "same request_id with a different body must be CONTROLLER_REQUEST_CONFLICT; stdout={conflict_out} stderr={conflict_err}"
    );
    assert_eq!(
        fixture.journal_task_turns().len(),
        executed_before,
        "replay must not start extra actual execution; journal={}",
        fixture.exec_journal()
    );
    drop(leader);
}

const TASK_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";

/// Public grammar: two independent roots in one checkout via `base` refs.
/// `project` is not a BatchTask field (`deny_unknown_fields`).
fn write_independent_two_root_batch(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(
        path,
        "version = 1\n\n[[tasks]]\nid = \"alpha\"\nprompt = \"independent root alpha\"\nbase = \"alpha-base\"\n\n[[tasks]]\nid = \"beta\"\nprompt = \"independent root beta\"\nbase = \"beta-base\"\n",
    )
    .unwrap();
}

fn write_from_parent_batch(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(
        path,
        "version = 1\n\n[[tasks]]\nid = \"parent\"\nprompt = \"parent root stays open after Done\"\nclose_on = \"never\"\n\n[[tasks]]\nid = \"child\"\ndepends_on = [\"parent\"]\nbase = \"from:parent\"\nprompt = \"child from accepted parent\"\n",
    )
    .unwrap();
}

struct TwoRootCheckout {
    repo: support::GitRepo,
    alpha_oid: String,
    beta_oid: String,
    project_id: String,
    worktree_id: String,
}

fn two_committed_bases() -> TwoRootCheckout {
    let repo = support::GitRepo::init();
    repo.write("alpha-src.txt", b"alpha source\n");
    repo.commit_all("alpha base");
    let alpha_oid = head_oid(&repo);
    assert!(
        repo.git(&["tag", "alpha-base"]).status.success(),
        "tag alpha-base"
    );
    repo.write("beta-src.txt", b"beta source\n");
    repo.commit_all("beta base");
    let beta_oid = head_oid(&repo);
    assert!(
        repo.git(&["tag", "beta-base"]).status.success(),
        "tag beta-base"
    );
    let (project_id, worktree_id, _) = inspect_repo(&repo);
    TwoRootCheckout {
        repo,
        alpha_oid,
        beta_oid,
        project_id,
        worktree_id,
    }
}

fn support_git_repo(file: &str, bytes: &[u8]) -> support::GitRepo {
    let repo = support::GitRepo::init();
    repo.write(file, bytes);
    repo.commit_all("fixture");
    repo
}

fn inspect_repo(repo: &support::GitRepo) -> (String, String, String) {
    let project = ProjectState::load(&RUNNER, repo.root(), &[]).unwrap();
    (
        project.context.project_id,
        project.context.worktree_id,
        head_oid(repo),
    )
}

fn head_oid(repo: &support::GitRepo) -> String {
    String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned()
}

fn seed_controller_transfer(
    fixture: &ProcessFixture,
    source: &support::GitRepo,
    project_id: &str,
    worktree_id: &str,
) -> PathBuf {
    let cache_root = fixture.controller_xdg_cache.join("mac-worker");
    let dest =
        TransferRepo::controller_transfer_git_path(&cache_root, project_id, worktree_id).unwrap();
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    let clone = source.git(["clone", "--bare", ".", dest.to_str().unwrap()].as_slice());
    assert!(
        clone.status.success(),
        "bare clone into controller-transfer failed: {}",
        String::from_utf8_lossy(&clone.stderr)
    );
    dest
}

fn task_id_n(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn job_id_n(n: u128) -> JobId {
    JobId::new(Uuid::from_u128(n))
}

fn client_id_n(n: u128) -> ClientId {
    ClientId::new(Uuid::from_u128(n))
}

fn lease_token_n(n: u128) -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(n))
}

fn plumbing_task_meta(
    task_id: TaskId,
    project_id: &str,
    worktree_id: &str,
    base_oid: BaseOid,
) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: project_id.into(),
        worktree_id: worktree_id.into(),
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

fn plumbing_material(
    job_id: JobId,
    client_id: ClientId,
    token: LeaseToken,
    project_id: &str,
    worktree_id: &str,
    manifest_digest: String,
) -> RequestFingerprintMaterial {
    RequestFingerprintMaterial::new(
        job_id,
        client_id,
        token,
        100,
        "mini-1".into(),
        project_id.into(),
        worktree_id.into(),
        manifest_digest,
        String::new(),
        30_000,
        "heavy".into(),
        CommandSpec::shell("true".into()).unwrap(),
    )
    .unwrap()
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

fn fakeexec_one_turn(
    fixture: &ProcessFixture,
    task_id: TaskId,
    project_id: &str,
    worktree_id: &str,
    base_oid: &str,
    n: u128,
    prompt: &str,
) -> String {
    let base: BaseOid = base_oid.parse().unwrap();
    let job_id = job_id_n(n);
    let client_id = client_id_n(n);
    let token = lease_token_n(n);
    let turn = TurnMaterial::from_prompt(
        task_id,
        1,
        AgentKind::Codex,
        None,
        None,
        PermissionPolicy::Workspace,
        TurnLimits::new(30_000, None, None).unwrap(),
        base.clone(),
        prompt,
        None,
        Uuid::from_u128(n + 1),
        false,
    )
    .unwrap();
    let material = plumbing_material(
        job_id,
        client_id,
        token.clone(),
        project_id,
        worktree_id,
        turn.digest(),
    );
    let lease_req = LeaseAcquireRequest::new(material.clone())
        .with_execution_scope(ExecutionScope::task(task_id));
    let lease: LeaseAcquireResponse = fakeexec_ok(
        fixture,
        "host lease-acquire",
        &serde_json::to_vec(&lease_req).unwrap(),
    );
    match lease {
        LeaseAcquireResponse::Acquired { .. } => {}
        other => panic!("expected acquired lease, got {other:?}"),
    }
    let snapshot = SnapshotVerifyRequest::new(
        job_id,
        client_id,
        token,
        material.fingerprint(),
        project_id.into(),
        worktree_id.into(),
        material.manifest_digest().to_owned(),
    )
    .unwrap();
    let _: VerifiedSnapshotResponse = fakeexec_ok(
        fixture,
        "host snapshot-verify",
        &serde_json::to_vec(&snapshot).unwrap(),
    );
    let prepare_req = TaskPrepareRequest::new(
        plumbing_task_meta(task_id, project_id, worktree_id, base.clone()),
        job_id,
        "mini-1",
    );
    let prepare: TaskPrepareResponse = fakeexec_ok(
        fixture,
        "host task-prepare",
        &serde_json::to_vec(&prepare_req).unwrap(),
    );
    assert_eq!(prepare.head().as_str(), base_oid);
    let _: TaskSessionResponse = fakeexec_ok(
        fixture,
        "host task-session",
        &serde_json::to_vec(&TaskSessionRequest::new(project_id, task_id)).unwrap(),
    );
    let submit = mac_worker::job::SubmitRequest::new(material)
        .with_execution_scope(ExecutionScope::task(task_id));
    let turn_req = TaskTurnRequest::new(submit, turn, prompt);
    let turn_resp: TaskTurnResponse = fakeexec_ok(
        fixture,
        "host task-turn",
        &serde_json::to_vec(&turn_req).unwrap(),
    );
    match turn_resp.submit() {
        SubmitResponse::Accepted { .. } => {}
        other => panic!("expected accepted submit, got {other:?}"),
    }
    let _: StatusLogsResponse = fakeexec_ok(
        fixture,
        "host status-logs",
        &serde_json::to_vec(&StatusLogsRequest::new(job_id, 0, 32, 0, 32)).unwrap(),
    );
    turn_resp
        .task()
        .head_oid()
        .expect("task-turn TaskStatus must carry a result head")
        .as_str()
        .to_owned()
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

fn parse_json_value(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).unwrap_or_else(|_| {
        let text = String::from_utf8_lossy(stdout);
        panic!("expected JSON stdout, got: {text}");
    })
}

fn json_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn json_strings(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("JSON {key} must be an array; got {value}"))
        .iter()
        .map(|item| {
            item.as_str()
                .unwrap_or_else(|| panic!("{key} items must be strings; got {item}"))
                .to_owned()
        })
        .collect()
}

fn sorted_strings(values: &[String]) -> Vec<String> {
    let mut values = values.to_vec();
    values.sort();
    values
}

fn status_object(report: &Value) -> &Value {
    report.get("status").unwrap_or(report)
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
        "enabled batch must persist one laptop OperationEnvelope; found {paths:?}"
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
            String::from_utf8_lossy(&stderr)
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

fn git_object_exists(repo: &support::GitRepo, oid: &str) -> bool {
    repo.git(&["cat-file", "-t", oid]).status.success()
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
    let deadline = Instant::now() + Duration::from_secs(45);
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

fn task_id_for_node(body: &FrozenBatchBody, batch_id: &str) -> String {
    body.nodes
        .get(batch_id)
        .unwrap_or_else(|| panic!("no frozen node {batch_id}"))
        .task_id
        .to_string()
}

fn turn_id_for_task(body: &FrozenBatchBody, task_id: &str) -> String {
    body.nodes
        .values()
        .find(|node| node.task_id.to_string() == task_id)
        .expect("node for task")
        .turn_id
        .to_string()
}

fn frozen_oid_for_task(body: &FrozenBatchBody, task_id: &str) -> String {
    match &body
        .nodes
        .values()
        .find(|node| node.task_id.to_string() == task_id)
        .expect("node for task")
        .base
    {
        DagBase::Frozen { oid, .. } => oid.as_str().to_owned(),
        other => panic!("expected Frozen base for {task_id}, got {other:?}"),
    }
}
