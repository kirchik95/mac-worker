use crate::support;
use mac_worker::test_support::cli::from_parts;

use std::{
    collections::BTreeMap,
    fs,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::Path,
    process::{Command, ExitStatus},
    sync::{Mutex, mpsc},
    thread,
    time::Duration,
};

use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    cli::Command as WorkerCommand,
    core::{
        error::{ProcessError, WorkerError},
        protocol::{MemoryPressure, PROTOCOL_VERSION},
    },
    host::{
        job::{
            ClientId, CommandSpec, ExecutionScope, JobId, JobMeta, JobStatus, LeaseAcquireRequest,
            LeaseToken, RequestFingerprintMaterial,
        },
        lease::{AdmissionFacts, LeaseService},
        process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
        store::{HostGc, HostStore},
    },
    runtime::{RuntimeContext, run_with_io_in_context},
    task::{
        model::{
            BaseOid, ClosePolicy, GitIdentity, PublishMode, TaskId, TaskLimits, TaskMeta,
            TaskMetaInput, TaskOutcome, TaskSource, TaskState, TurnSummary, TurnTerminal,
        },
        store::{SessionBinding, TaskStore},
    },
    transfer::repo::{TransferGc, TransferRepo},
};
use support::{GitRepo, recording_runner::RecordingRunner};
use tempfile::TempDir;
use uuid::Uuid;

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_PROJECT_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const LEGACY_PROJECT_ID: &str = "ac2486b82bd3f012ce72f1ba60cad94aeef2f8dfe10329ce5451794ec87b3bd1";
const LEGACY_WORKTREE_ID: &str = "e59fa72efe82e49c977312206e4bfa7bbe82b3b5afc238fa2694800244e27532";
const LEGACY_JOB_ID: &str = "14aff6a4642846809f24a0bf9e13b441";
const LEGACY_JOB_ID_MINI2: &str = "4ce2435b1eb34c18b24dbb42fa7e3a31";

fn task_id(value: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(value))
}

fn job_id(value: u128) -> JobId {
    JobId::new(Uuid::from_u128(value))
}

fn client_id() -> ClientId {
    ClientId::new(Uuid::from_u128(20))
}

fn lease_token(job: JobId) -> LeaseToken {
    LeaseToken::new(Uuid::from_u128(job.as_uuid().as_u128().wrapping_add(30)))
}

fn lease_request(job: JobId) -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            job,
            client_id(),
            lease_token(job),
            1,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            "c".repeat(64),
            String::new(),
            30_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap(),
    )
    .with_execution_scope(ExecutionScope::task(TaskId::new(job.as_uuid())))
}

fn git(path: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .current_dir(path)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args)
        .output()
        .expect("run git fixture command");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

fn ref_exists(path: &Path, reference: &str) -> bool {
    Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .args(["show-ref", "--verify", "--quiet", reference])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .status()
        .expect("check git ref")
        .success()
}

fn set_ref(path: &Path, reference: &str, oid: &str) {
    let output = Command::new("/usr/bin/git")
        .args(["--git-dir"])
        .arg(path)
        .args(["update-ref", reference, oid])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .expect("set git ref");
    assert!(
        output.status.success(),
        "set ref failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn task_meta(task_id: TaskId, base_oid: BaseOid, created_at_millis: u64) -> TaskMeta {
    TaskMeta::new(TaskMetaInput {
        task_id,
        run_id: None,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
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
        base_oid,
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Ada Lovelace", "ada@example.test").unwrap(),
        title: None,
        prompt: "gc fixture".into(),
        created_at_millis,
    })
    .unwrap()
}

fn acquire_lease(store: &HostStore, job: JobId) {
    let request = lease_request(job);
    LeaseService::new(store)
        .acquire(
            &request,
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            1,
        )
        .unwrap();
}

fn release_lease(store: &HostStore, job: JobId) {
    let request = lease_request(job);
    store.record_abandoned(&request, 2).unwrap();
    let lease = LeaseService::new(store).load().unwrap().unwrap();
    let receipt = store.cleanup_job_owned(&lease).unwrap();
    LeaseService::new(store)
        .release_after_cleanup(&lease, &receipt)
        .unwrap();
}

fn write_job(store: &HostStore, job: JobId, status: &JobStatus) {
    let job_dir = store.job(PROJECT_ID, WORKTREE_ID, job).unwrap();
    fs::create_dir_all(&job_dir).unwrap();
    fs::set_permissions(
        job_dir.parent().unwrap().parent().unwrap(),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    fs::set_permissions(job_dir.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&job_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let request = lease_request(job);
    let meta = JobMeta::new(request.material(), request.request_fingerprint().clone()).unwrap();
    fs::write(
        job_dir.join("meta.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    fs::set_permissions(job_dir.join("meta.json"), fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(
        job_dir.join("status.json"),
        serde_json::to_vec(status).unwrap(),
    )
    .unwrap();
    fs::set_permissions(
        job_dir.join("status.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    ensure_job_siblings(store, job);
}

fn ensure_job_siblings(store: &HostStore, job: JobId) {
    drop(store.admission_lock(job).unwrap());
    let index = store.job_index(job).unwrap();
    if !index.exists() {
        fs::write(&index, b"{}").unwrap();
        fs::set_permissions(index, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn write_legacy_fixture_job(store: &HostStore) -> (String, JobId) {
    write_legacy_fixture_job_from(
        store,
        LEGACY_JOB_ID,
        include_bytes!("../fixtures/gc/legacy-job-meta-v3.json"),
        include_bytes!("../fixtures/gc/legacy-job-status.json"),
    )
}

fn write_legacy_fixture_job_from(
    store: &HostStore,
    job_name: &str,
    meta: &[u8],
    status: &[u8],
) -> (String, JobId) {
    let job: JobId = job_name.parse().unwrap();
    let job_dir = store
        .job(LEGACY_PROJECT_ID, LEGACY_WORKTREE_ID, job)
        .unwrap();
    fs::create_dir_all(&job_dir).unwrap();
    fs::set_permissions(
        job_dir.parent().unwrap().parent().unwrap(),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    fs::set_permissions(job_dir.parent().unwrap(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&job_dir, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(job_dir.join("meta.json"), meta).unwrap();
    fs::set_permissions(job_dir.join("meta.json"), fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(job_dir.join("status.json"), status).unwrap();
    fs::set_permissions(
        job_dir.join("status.json"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    ensure_job_siblings(store, job);
    let identifier = format!("{LEGACY_PROJECT_ID}/{LEGACY_WORKTREE_ID}/{job}");
    (identifier, job)
}

fn fixture() -> (TempDir, HostStore, GitRepo, BaseOid) {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let base_oid: BaseOid = git(source.root(), &["rev-parse", "HEAD"]).parse().unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let output = source.git(&[
        "push",
        mirror.path().to_str().unwrap(),
        &format!("HEAD:refs/mac-worker/bases/{}", task_id(1)),
    ]);
    assert!(output.status.success());
    (temp, store, source, base_oid)
}

fn prepare_task(store: &HostStore, task: TaskId, job: JobId, base_oid: &BaseOid) {
    let request = lease_request(job).with_execution_scope(ExecutionScope::task(task));
    LeaseService::new(store)
        .acquire(
            &request,
            &AdmissionFacts {
                free_disk_bytes: 100 * 1024 * 1024 * 1024,
                total_disk_bytes: 200 * 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(0),
            },
            1,
        )
        .unwrap();
    let admission = store.admission_lock(job).unwrap();
    let transfer = store.transfer_lock_after(&admission, job).unwrap();
    TaskStore::new(store, &SystemProcessRunner)
        .prepare(
            &mac_worker::test_support::task::store::TaskPrepareRequest::new(
                task_meta(task, base_oid.clone(), 1),
                job,
                "mini-1",
            ),
            &transfer,
        )
        .unwrap();
    drop(transfer);
    drop(admission);
    TaskStore::new(store, &SystemProcessRunner)
        .publish_branch_into_mirror(PROJECT_ID, task)
        .unwrap();
}

fn rewrite_status(store: &HostStore, task: TaskId, state: TaskState, updated_at_millis: u64) {
    let status = store.task_status(PROJECT_ID, task).unwrap();
    let turns = if state.is_terminal() || state == TaskState::Open {
        status
            .turns()
            .iter()
            .map(|turn| {
                if turn.terminal().is_some() {
                    turn.clone()
                } else {
                    TurnSummary::new(
                        turn.turn_number(),
                        turn.turn_id(),
                        Some(TurnTerminal::Lost),
                        Some(TaskOutcome::Lost),
                        Some(false),
                        turn.log_truncated(),
                        turn.started_at_millis(),
                        Some(updated_at_millis),
                    )
                }
            })
            .collect()
    } else {
        status.turns().to_vec()
    };
    let replacement = mac_worker::test_support::task::model::TaskStatus::new(
        state,
        status.last_outcome().cloned(),
        status.worker().map(str::to_owned),
        status.session_present(),
        status.head_oid().cloned(),
        status.summary().map(str::to_owned),
        status.questions().to_vec(),
        status.files_changed().to_vec(),
        status.diff_stat().map(str::to_owned),
        turns,
        updated_at_millis,
    )
    .unwrap();
    fs::write(
        store
            .task_dir(PROJECT_ID, task)
            .unwrap()
            .join("status.json"),
        serde_json::to_vec(&replacement).unwrap(),
    )
    .unwrap();
}

#[test]
fn gc_closes_idle_open_task_but_preserves_result_branch_and_metadata() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Open, 1);

    let now = 1 + mac_worker::test_support::host::store::TASK_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    let candidate = preview
        .candidates()
        .iter()
        .find(|candidate| candidate.kind() == "task" && candidate.reason() == "open task retention")
        .expect("idle open task candidate");
    assert_eq!(candidate.identifier(), format!("{PROJECT_ID}/{task}"));
    assert!(candidate.size_bytes() > 0);

    let applied = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(
        applied
            .applied()
            .iter()
            .any(|item| item.identifier() == candidate.identifier())
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task).unwrap().state(),
        TaskState::Closed
    );
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task)
            .unwrap()
            .is_none()
    );
    let mirror = store.mirror(PROJECT_ID).unwrap();
    assert!(ref_exists(
        mirror.path(),
        &format!("refs/heads/task/{task}")
    ));
}

#[test]
fn gc_does_not_close_an_open_task_while_its_task_scope_lease_is_live() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    rewrite_status(&store, task, TaskState::Open, 1);
    let now = 1 + mac_worker::test_support::host::store::TASK_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(
        preview
            .candidates()
            .iter()
            .all(|candidate| candidate.kind() != "task"
                || candidate.reason() != "open task retention")
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task).unwrap().state(),
        TaskState::Open
    );
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task)
            .unwrap()
            .is_some()
    );
}

#[test]
fn gc_does_not_close_two_open_tasks_while_both_slot_leases_are_live() {
    let (_temp, store, source, base_oid) = fixture();
    LeaseService::new(&store).set_slot_count(2).unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let pushed = source.git(&[
        "push",
        mirror.path().to_str().unwrap(),
        &format!("HEAD:refs/mac-worker/bases/{}", task_id(2)),
    ]);
    assert!(pushed.status.success());
    prepare_task(&store, task_id(1), job_id(1), &base_oid);
    prepare_task(&store, task_id(2), job_id(2), &base_oid);
    rewrite_status(&store, task_id(1), TaskState::Open, 1);
    rewrite_status(&store, task_id(2), TaskState::Open, 1);
    assert_eq!(LeaseService::new(&store).occupied_slots().unwrap().len(), 2);
    let now = 1 + mac_worker::test_support::host::store::TASK_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().all(|candidate| {
        candidate.kind() != "task" || candidate.reason() != "open task retention"
    }));
}

#[test]
fn gc_keeps_all_slot_records_when_a_slot_lease_is_unreadable() {
    let (temp, store, source, base_oid) = fixture();
    LeaseService::new(&store).set_slot_count(2).unwrap();
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let pushed = source.git(&[
        "push",
        mirror.path().to_str().unwrap(),
        &format!("HEAD:refs/mac-worker/bases/{}", task_id(2)),
    ]);
    assert!(pushed.status.success());
    prepare_task(&store, task_id(1), job_id(1), &base_oid);
    prepare_task(&store, task_id(2), job_id(2), &base_oid);
    rewrite_status(&store, task_id(1), TaskState::Open, 1);
    rewrite_status(&store, task_id(2), TaskState::Open, 1);
    fs::write(
        temp.path().join("host/leases/slots/0/lease.json"),
        b"not-a-canonical-lease",
    )
    .unwrap();
    let now = 1 + mac_worker::test_support::host::store::TASK_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(
        preview
            .warnings()
            .iter()
            .any(|warning| warning.contains("lease record unreadable")),
        "{preview:?}"
    );
    assert!(preview.candidates().iter().all(|candidate| {
        candidate.kind() != "task" || candidate.reason() != "open task retention"
    }));
    assert_eq!(
        store.task_status(PROJECT_ID, task_id(1)).unwrap().state(),
        TaskState::Open
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task_id(2)).unwrap().state(),
        TaskState::Open
    );
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id(1))
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .task_workspace_if_present(PROJECT_ID, task_id(2))
            .unwrap()
            .is_some()
    );
}

#[test]
fn gc_prunes_expired_task_branch_without_removing_foreign_mirror_refs() {
    let (_temp, store, source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror = store.mirror(PROJECT_ID).unwrap();
    let keep = Command::new("/usr/bin/git")
        .args(["--git-dir", mirror.path().to_str().unwrap(), "update-ref"])
        .args([
            "refs/heads/keep",
            &git(source.root(), &["rev-parse", "HEAD"]),
        ])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        keep.status.success(),
        "keep ref push failed: {}",
        String::from_utf8_lossy(&keep.stderr)
    );

    let now = 1 + mac_worker::test_support::host::store::BRANCH_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "branch" && candidate.reason() == "branch retention"
    }));
    let applied = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!applied.applied().is_empty());
    assert!(!ref_exists(
        mirror.path(),
        &format!("refs/heads/task/{task}")
    ));
    assert!(!ref_exists(
        mirror.path(),
        &format!("refs/mac-worker/bases/{task}")
    ));
    assert!(ref_exists(mirror.path(), "refs/heads/keep"));
}

#[test]
fn gc_runs_one_maintenance_pass_without_immediate_prune() {
    let (_temp, store, source, base_oid) = fixture();
    let task = task_id(1);
    let second = task_id(2);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror_path = store.mirror(PROJECT_ID).unwrap().path().to_path_buf();
    let oid = git(source.root(), &["rev-parse", "HEAD"]);
    set_ref(&mirror_path, &format!("refs/heads/task/{second}"), &oid);
    set_ref(
        &mirror_path,
        &format!("refs/mac-worker/bases/{second}"),
        &oid,
    );

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(1 + mac_worker::test_support::host::store::JOB_RETENTION_MILLIS)
        .unwrap();
    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    let branch_candidates = preview
        .candidates()
        .iter()
        .filter(|candidate| candidate.kind() == "branch")
        .count();
    assert!(
        branch_candidates >= 2,
        "expected several branch candidates in one mirror, got {branch_candidates}: {preview:?}"
    );

    let runner = RecordingRunner::passthrough();
    HostGc::new(&store, &runner).apply_at(now).unwrap();

    let maintenance = git_gc_commands(&runner);
    assert_eq!(
        maintenance.len(),
        1,
        "one mirror pass must run one maintenance gc, saw {maintenance:?}"
    );
    assert!(
        maintenance[0]
            .iter()
            .all(|argument| argument != "--prune=now" && !argument.contains("now")),
        "maintenance gc must not prune immediately: {:?}",
        maintenance[0]
    );
    assert!(
        maintenance[0]
            .iter()
            .any(|argument| argument == "--prune=2.weeks.ago"),
        "maintenance gc must keep a two-week grace: {:?}",
        maintenance[0]
    );
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/heads/task/{task}")
    ));
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/mac-worker/bases/{task}")
    ));
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/heads/task/{second}")
    ));
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/mac-worker/bases/{second}")
    ));
}

fn git_gc_commands(runner: &RecordingRunner) -> Vec<Vec<String>> {
    runner
        .requests()
        .into_iter()
        .filter_map(|request| {
            let arguments = request
                .args
                .iter()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            arguments
                .iter()
                .any(|argument| argument == "gc")
                .then_some(arguments)
        })
        .collect()
}

#[test]
fn gc_does_not_prune_a_terminal_task_with_a_live_turn_lease() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    rewrite_status(&store, task, TaskState::Closed, 1);
    assert_eq!(
        LeaseService::new(&store).load().unwrap().unwrap().job_id(),
        job
    );
    assert_eq!(
        store.task_status(PROJECT_ID, task).unwrap().turns()[0].turn_id(),
        job
    );

    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();

    assert!(!preview.candidates().iter().any(|candidate| {
        candidate.kind() == "branch" && candidate.identifier() == format!("{PROJECT_ID}/{task}")
    }));
}

#[test]
fn gc_does_not_remove_a_terminal_job_while_its_lease_is_live() {
    let (_temp, store, _source, _base_oid) = fixture();
    let job = job_id(1);
    acquire_lease(&store, job);
    write_job(&store, job, &JobStatus::succeeded(1, 0, 0).unwrap());

    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();

    assert!(!preview.candidates().iter().any(|candidate| {
        candidate.kind() == "job"
            && candidate.identifier() == format!("{PROJECT_ID}/{WORKTREE_ID}/{job}")
    }));
}

#[test]
fn gc_does_not_prune_a_task_with_a_nonterminal_turn_job() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Closed, 1);
    write_job(&store, job, &JobStatus::running(1, 100, 1, 200, 1).unwrap());

    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();

    assert!(!preview.candidates().iter().any(|candidate| {
        candidate.kind() == "branch" && candidate.identifier() == format!("{PROJECT_ID}/{task}")
    }));
}

#[test]
fn gc_expires_task_metadata_before_a_newer_result_branch() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror_path = store.mirror(PROJECT_ID).unwrap().path().to_path_buf();

    let now = 1 + mac_worker::test_support::host::store::JOB_RETENTION_MILLIS;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "task" && candidate.reason() == "task metadata retention"
    }));
    assert!(
        !preview
            .candidates()
            .iter()
            .any(|candidate| candidate.kind() == "branch")
    );

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!store.task_dir(PROJECT_ID, task).unwrap().exists());
    assert!(ref_exists(&mirror_path, &format!("refs/heads/task/{task}")));
    assert!(ref_exists(
        &mirror_path,
        &format!("refs/mac-worker/bases/{task}")
    ));
}

#[test]
fn gc_prunes_orphaned_task_refs_after_task_metadata_retention() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let mirror_path = store.mirror(PROJECT_ID).unwrap().path().to_path_buf();

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(1 + mac_worker::test_support::host::store::JOB_RETENTION_MILLIS)
        .unwrap();
    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "branch" && candidate.identifier() == format!("{PROJECT_ID}/{task}")
    }));
    assert!(
        !preview
            .candidates()
            .iter()
            .any(|candidate| candidate.kind() == "mirror" && candidate.identifier() == PROJECT_ID)
    );

    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/heads/task/{task}")
    ));
    assert!(!ref_exists(
        &mirror_path,
        &format!("refs/mac-worker/bases/{task}")
    ));
    let mirror_preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(
        mirror_preview.candidates().iter().any(|candidate| {
            candidate.kind() == "mirror" && candidate.identifier() == PROJECT_ID
        })
    );
}

#[test]
fn gc_does_not_touch_active_tasks_or_their_base_refs() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    rewrite_status(&store, task, TaskState::Active, 1);

    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(
        !preview
            .candidates()
            .iter()
            .any(|candidate| { candidate.identifier().ends_with(&format!("/{task}")) })
    );
    assert!(ref_exists(
        store.mirror(PROJECT_ID).unwrap().path(),
        &format!("refs/mac-worker/bases/{task}")
    ));
}

#[test]
fn gc_fails_closed_on_malformed_task_metadata() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    fs::write(
        store.task_dir(PROJECT_ID, task).unwrap().join("meta.json"),
        br#"{"#,
    )
    .unwrap();

    let report = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    assert!(
        report
            .warnings()
            .iter()
            .any(|warning| { warning.contains("unreadable task record") })
    );
    assert!(store.task_dir(PROJECT_ID, task).unwrap().exists());
}

#[test]
fn gc_preview_reports_legacy_job_metadata_without_aborting_other_records() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let (legacy_identifier, legacy_job) = write_legacy_fixture_job(&store);
    let (legacy_mini2_identifier, legacy_mini2_job) = write_legacy_fixture_job_from(
        &store,
        LEGACY_JOB_ID_MINI2,
        include_bytes!("../fixtures/gc/legacy-job-meta-v3-mini2.json"),
        include_bytes!("../fixtures/gc/legacy-job-status-v3-mini2.json"),
    );
    let valid_job = job_id(99);
    write_job(&store, valid_job, &JobStatus::succeeded(1, 0, 0).unwrap());

    let report = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    assert!(report.candidates().iter().any(|candidate| {
        candidate.kind() == "job"
            && candidate.identifier() == legacy_identifier
            && candidate.reason() == "legacy protocol"
    }));
    assert!(report.candidates().iter().any(|candidate| {
        candidate.kind() == "job"
            && candidate.identifier() == legacy_mini2_identifier
            && candidate.reason() == "legacy protocol"
    }));
    assert!(report.candidates().iter().any(|candidate| {
        candidate.kind() == "job"
            && candidate.identifier() == format!("{PROJECT_ID}/{WORKTREE_ID}/{valid_job}")
            && candidate.reason() == "job retention"
    }));

    let applied = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(u64::MAX / 2)
        .unwrap();
    assert!(
        !applied
            .applied()
            .iter()
            .any(|candidate| { candidate.identifier() == legacy_identifier })
    );
    assert!(
        store
            .job(LEGACY_PROJECT_ID, LEGACY_WORKTREE_ID, legacy_job)
            .unwrap()
            .exists()
    );
    assert!(
        store
            .job(LEGACY_PROJECT_ID, LEGACY_WORKTREE_ID, legacy_mini2_job)
            .unwrap()
            .exists()
    );
}

#[test]
fn gc_preview_reports_a_legacy_task_record_without_aborting() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(101);
    prepare_task(&store, task, job_id(101), &base_oid);
    release_lease(&store, job_id(101));
    let status_path = store
        .task_dir(PROJECT_ID, task)
        .unwrap()
        .join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_slice(&fs::read(&status_path).unwrap()).unwrap();
    status
        .as_object_mut()
        .unwrap()
        .insert("protocol_version".into(), serde_json::json!(3));
    fs::write(status_path, serde_json::to_vec(&status).unwrap()).unwrap();

    let report = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    assert!(report.candidates().iter().any(|candidate| {
        candidate.kind() == "task"
            && candidate.identifier() == format!("{PROJECT_ID}/{task}")
            && candidate.reason() == "legacy protocol"
    }));
}

#[test]
fn gc_preview_warns_and_keeps_a_job_with_missing_index_and_lock_records() {
    let temp = tempfile::tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let job = job_id(100);
    write_job(&store, job, &JobStatus::succeeded(1, 0, 0).unwrap());

    let index = store.job_index(job).unwrap();
    fs::remove_file(index).unwrap();
    let index_path = store.job_index(job).unwrap();
    let root = index_path.parent().unwrap().parent().unwrap();
    fs::remove_file(root.join("locks/jobs").join(format!("{job}.lock.json"))).unwrap();
    fs::remove_file(
        root.join("locks/jobs")
            .join(job.to_string())
            .join("admission.lock"),
    )
    .unwrap();

    let report = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    let identifier = format!("{PROJECT_ID}/{WORKTREE_ID}/{job}");
    assert!(
        !report
            .candidates()
            .iter()
            .any(|candidate| { candidate.identifier() == identifier })
    );
    assert!(
        report
            .warnings()
            .iter()
            .any(|warning| { warning.contains("inconsistent job record") })
    );
    assert!(store.job(PROJECT_ID, WORKTREE_ID, job).unwrap().exists());
}

#[test]
fn gc_is_idempotent_and_marks_empty_mirrors_only_after_all_refs_are_gone() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    prepare_task(&store, task, job_id(1), &base_oid);
    rewrite_status(&store, task, TaskState::Closed, 1);
    let empty_mirror = store.mirror(OTHER_PROJECT_ID).unwrap();
    let empty_path = empty_mirror.path().to_path_buf();

    let now = u64::MAX / 2;
    let preview = HostGc::new(&store, &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "mirror" && candidate.identifier() == OTHER_PROJECT_ID
    }));
    HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!empty_path.exists());

    let second = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(
        !second
            .candidates()
            .iter()
            .any(|candidate| { candidate.identifier().ends_with(&format!("/{task}")) })
    );
}

#[test]
fn gc_never_requests_native_agent_session_deletion() {
    let (_temp, store, _source, base_oid) = fixture();
    let task = task_id(1);
    let job = job_id(1);
    prepare_task(&store, task, job, &base_oid);
    release_lease(&store, job);
    TaskStore::new(&store, &SystemProcessRunner)
        .bind_session(
            PROJECT_ID,
            task,
            SessionBinding::new(AgentKind::Codex, "session-1", 1).unwrap(),
        )
        .unwrap();
    rewrite_status(&store, task, TaskState::Open, 1);

    let report = HostGc::new(&store, &SystemProcessRunner)
        .apply_at(1 + mac_worker::test_support::host::store::TASK_RETENTION_MILLIS)
        .unwrap();
    assert!(
        report
            .applied()
            .iter()
            .any(|candidate| candidate.kind() == "task")
    );
    assert!(
        store
            .task_dir(PROJECT_ID, task)
            .unwrap()
            .join("session.json")
            .exists()
    );
}

#[test]
fn transfer_gc_previews_and_removes_only_an_unreferenced_empty_transfer_repo() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let transfer =
        TransferRepo::open_or_create(&temp.path().join("cache"), &source.root().join(".git"))
            .unwrap();
    let repo_path = transfer.path().to_path_buf();
    let repo_id = transfer.repo_id().to_owned();
    drop(transfer);
    let malformed_path = temp.path().join("cache/transfer/not-a-transfer-repo");
    fs::create_dir_all(&malformed_path).unwrap();
    fs::set_permissions(&malformed_path, fs::Permissions::from_mode(0o700)).unwrap();
    let now = u64::MAX / 2;
    let preview = TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .preview_at(now)
        .unwrap();
    assert!(preview.candidates().iter().any(|candidate| {
        candidate.kind() == "transfer_repo" && candidate.identifier() == repo_id
    }));
    assert!(
        preview
            .warnings()
            .iter()
            .any(|warning| warning.contains("inconsistent transfer repository record"))
    );
    TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .apply_at(now)
        .unwrap();
    assert!(!repo_path.exists());
    assert!(malformed_path.exists());
}

#[test]
fn transfer_gc_skips_a_live_transfer_repository_handle_with_a_warning() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let cache = temp.path().join("cache");
    let transfer = TransferRepo::open_or_create(&cache, &source.root().join(".git")).unwrap();
    let held_repo_id = transfer.repo_id().to_owned();
    let held_path = transfer.path().to_path_buf();
    let other = GitRepo::init();
    other.write("other.txt", b"other\n");
    other.commit_all("other");
    let free = TransferRepo::open_or_create(&cache, &other.root().join(".git")).unwrap();
    let free_repo_id = free.repo_id().to_owned();
    let free_path = free.path().to_path_buf();
    drop(free);

    let (sender, receiver) = mpsc::channel();
    let gc_cache = cache.clone();
    let handle = thread::spawn(move || {
        let runner = SystemProcessRunner;
        let preview = TransferGc::new(&gc_cache, &runner).preview_at(u64::MAX / 2);
        let applied = TransferGc::new(&gc_cache, &runner).apply_at(u64::MAX / 2);
        sender.send((preview, applied)).unwrap();
    });
    let (preview, applied) = receiver
        .recv_timeout(crate::support::HANDSHAKE_TIMEOUT)
        .expect("transfer GC must not wait for a live repository handle");
    handle.join().unwrap();
    let preview = preview.unwrap();
    let applied = applied.unwrap();
    for report in [&preview, &applied] {
        assert!(
            report
                .candidates()
                .iter()
                .all(|candidate| candidate.identifier() != held_repo_id)
        );
        assert!(
            report
                .candidates()
                .iter()
                .any(|candidate| candidate.identifier() == free_repo_id)
        );
        assert!(
            report
                .warnings()
                .iter()
                .any(|warning| warning.contains("in use")),
            "{:?}",
            report.warnings()
        );
    }
    assert!(held_path.exists());
    assert!(!free_path.exists());

    drop(transfer);
    let report = TransferGc::new(&cache, &SystemProcessRunner)
        .apply_at(u64::MAX / 2)
        .unwrap();
    assert!(
        report
            .applied()
            .iter()
            .any(|candidate| candidate.identifier() == held_repo_id)
    );
    assert!(report.warnings().is_empty(), "{:?}", report.warnings());
    assert!(!held_path.exists());
}

#[test]
fn transfer_gc_ignores_the_init_lock_file() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let cache = temp.path().join("cache");
    let transfer = TransferRepo::open_or_create(&cache, &source.root().join(".git")).unwrap();
    let repo_id = transfer.repo_id().to_owned();
    drop(transfer);
    assert!(
        cache
            .join("transfer")
            .join(format!("{repo_id}.init.lock"))
            .exists()
    );
    let report = TransferGc::new(&cache, &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    assert!(report.warnings().is_empty(), "{:?}", report.warnings());
    assert_eq!(report.candidates().len(), 1);
}

#[test]
fn a_child_process_does_not_inherit_the_transfer_repository_lock() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let cache = temp.path().join("cache");
    let transfer = TransferRepo::open_or_create(&cache, &source.root().join(".git")).unwrap();
    let mut child = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("echo ready; exec /bin/sleep 30")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = String::new();
    let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
    std::io::BufRead::read_line(&mut stdout, &mut ready).unwrap();
    assert_eq!(ready.trim(), "ready", "the child has exec'ed");
    drop(transfer);
    let report = TransferGc::new(&cache, &SystemProcessRunner).preview_at(u64::MAX / 2);
    child.kill().unwrap();
    child.wait().unwrap();
    let report = report.unwrap();
    assert!(report.warnings().is_empty(), "{:?}", report.warnings());
    assert_eq!(report.candidates().len(), 1);
}

#[test]
fn transfer_gc_keeps_a_repo_with_a_live_base_ref() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let oid = git(source.root(), &["rev-parse", "HEAD"]);
    let transfer =
        TransferRepo::open_or_create(&temp.path().join("cache"), &source.root().join(".git"))
            .unwrap();
    let repo_path = transfer.path().to_path_buf();
    let task = task_id(1);
    set_ref(&repo_path, &format!("refs/mac-worker/bases/{task}"), &oid);
    drop(transfer);

    let report = TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .apply_at(u64::MAX / 2)
        .unwrap();
    assert!(report.candidates().is_empty());
    assert!(repo_path.exists());
}

#[test]
fn transfer_gc_can_collect_stale_result_refs_without_collecting_base_refs() {
    let temp = tempfile::tempdir().unwrap();
    let source = GitRepo::init();
    source.write("base.txt", b"base\n");
    source.commit_all("base");
    let oid = git(source.root(), &["rev-parse", "HEAD"]);
    let transfer =
        TransferRepo::open_or_create(&temp.path().join("cache"), &source.root().join(".git"))
            .unwrap();
    let repo_path = transfer.path().to_path_buf();
    let repo_id = transfer.repo_id().to_owned();
    let task = task_id(1);
    set_ref(&repo_path, &format!("refs/mac-worker/results/{task}"), &oid);
    drop(transfer);

    let report = TransferGc::new(&temp.path().join("cache"), &SystemProcessRunner)
        .preview_at(u64::MAX / 2)
        .unwrap();
    assert!(report.candidates().iter().any(|candidate| {
        candidate.kind() == "transfer_repo" && candidate.identifier() == repo_id
    }));
    assert!(repo_path.exists());
}

#[derive(Clone, Copy)]
enum SecondHostFault {
    Unavailable,
    Timeout,
}

struct FleetGcRunner {
    requests: Mutex<Vec<ProcessRequest>>,
    second_host: SecondHostFault,
}

impl ProcessRunner for FleetGcRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let destination = ssh_destination(request);
        assert!(
            request
                .args
                .iter()
                .any(|argument| argument.to_string_lossy().contains("host gc")),
            "fleet gc must call host gc, saw {:?}",
            request.args
        );
        if destination == "mac2" {
            return match self.second_host {
                SecondHostFault::Unavailable => Ok(ProcessResult {
                    status: ExitStatus::from_raw(255 << 8),
                    stdout: Vec::new(),
                    stderr: b"connection refused".to_vec(),
                }),
                SecondHostFault::Timeout => {
                    Err(WorkerError::Process(ProcessError::DeadlineExceeded {
                        deadline: Duration::from_secs(30),
                    }))
                }
            };
        }
        let applied = match destination.as_str() {
            "mac1" => "applied-on-mini-1",
            "mac3" => "applied-on-mini-3",
            other => panic!("unexpected gc destination {other}"),
        };
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: host_gc_stdout(applied),
            stderr: Vec::new(),
        })
    }
}

fn ssh_destination(request: &ProcessRequest) -> String {
    let separator = request
        .args
        .iter()
        .position(|argument| argument == "--")
        .expect("ssh options are separated from the destination");
    request.args[separator + 1].to_string_lossy().into_owned()
}

fn host_gc_stdout(identifier: &str) -> Vec<u8> {
    format!(
        r#"{{"protocol_version":{PROTOCOL_VERSION},"apply":true,"candidates":[{{"kind":"branch","identifier":"{identifier}","size_bytes":0,"reason":"branch retention"}}],"applied":[{{"kind":"branch","identifier":"{identifier}","size_bytes":0,"reason":"branch retention"}}],"warnings":[]}}"#
    )
    .into_bytes()
}

fn fleet_gc(second_host: SecondHostFault, json: bool) -> (u8, String, String, Vec<ProcessRequest>) {
    let runner = FleetGcRunner {
        requests: Mutex::new(Vec::new()),
        second_host,
    };
    let temporary = tempfile::tempdir().unwrap();
    // Client state opens each path component with O_NOFOLLOW. The fixture uses
    // the canonical directory so that walk stays on real directories.
    let root = temporary.path().canonicalize().unwrap();
    let home = root.join("home");
    fs::create_dir_all(&home).unwrap();
    let config_path = root.join("config.toml");
    fs::write(
        &config_path,
        "\
version = 1
[[workers]]
name = \"mini-1\"
ssh = \"mac1\"
slots = 1
[[workers]]
name = \"mini-2\"
ssh = \"mac2\"
slots = 1
[[workers]]
name = \"mini-3\"
ssh = \"mac3\"
slots = 1
",
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(BTreeMap::new(), home, root);
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_io_in_context(
        from_parts(Some(config_path), json, WorkerCommand::Gc { apply: true }),
        &runner,
        &runtime,
        &mut stdout,
        &mut stderr,
    );
    let requests = runner.requests.lock().expect("requests").clone();
    (
        exit,
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
        requests,
    )
}

fn assert_gc_calls_stay_bounded(requests: &[ProcessRequest]) {
    let destinations = requests.iter().map(ssh_destination).collect::<Vec<_>>();
    assert_eq!(destinations, ["mac1", "mac2", "mac3"]);
    assert!(
        requests
            .iter()
            .all(|request| request.policy.deadline == Duration::from_secs(30)),
        "host gc must keep the 30s control deadline: {requests:?}"
    );
}

#[test]
fn fleet_gc_reports_the_second_host_outcome_without_losing_the_others() {
    // An unavailable second host is an error row and a timed-out one an
    // unknown row; either way the first and third hosts keep their apply
    // results and the gc calls stay bounded.
    for (fault, status, code) in [
        (SecondHostFault::Unavailable, "error", "SSH_UNAVAILABLE"),
        (SecondHostFault::Timeout, "unknown", "SSH_TIMEOUT"),
    ] {
        let (exit, stdout, stderr, requests) = fleet_gc(fault, false);
        assert_ne!(exit, 0, "stderr={stderr}\nstdout={stdout}");
        assert!(
            stdout.contains("worker mini-1") && stdout.contains("applied-on-mini-1"),
            "first host apply result missing from stdout={stdout:?} stderr={stderr:?} exit={exit}"
        );
        assert!(
            stdout.contains("worker mini-2") && stdout.contains(status) && stdout.contains(code),
            "second host {status} missing from {stdout}"
        );
        assert!(
            stdout.contains("worker mini-3") && stdout.contains("applied-on-mini-3"),
            "third host missing from {stdout}"
        );
        assert_gc_calls_stay_bounded(&requests);

        let (exit, stdout, stderr, requests) = fleet_gc(fault, true);
        assert_ne!(exit, 0, "stderr={stderr}\nstdout={stdout}");
        let report: serde_json::Value = serde_json::from_str(stdout.trim()).expect(&stdout);
        assert_eq!(report["apply"], true);
        assert_eq!(report["workers"][0]["status"], "success");
        assert_eq!(
            report["workers"][0]["report"]["applied"][0]["identifier"],
            "applied-on-mini-1"
        );
        assert_eq!(report["workers"][1]["worker"], "mini-2");
        assert_eq!(report["workers"][1]["status"], status);
        assert_eq!(report["workers"][1]["error_code"], code);
        assert!(report["workers"][1].get("report").is_none(), "{report}");
        assert_eq!(report["workers"][2]["status"], "success");
        assert_eq!(
            report["workers"][2]["report"]["applied"][0]["identifier"],
            "applied-on-mini-3"
        );
        assert!(report.get("transfer").is_some(), "{report}");
        assert_gc_calls_stay_bounded(&requests);
    }
}
