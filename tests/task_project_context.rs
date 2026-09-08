#[allow(dead_code)]
mod support;

use std::{
    ffi::OsString,
    fs,
    os::unix::{ffi::OsStringExt, fs::PermissionsExt},
    path::{Path, PathBuf},
};

#[cfg(target_os = "macos")]
use base64::{Engine as _, engine::general_purpose::STANDARD};
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStrExt;

use mac_worker::{
    agent::{AgentKind, PermissionPolicy},
    client_state::{ClientStateStore, ClientStateWritePoint},
    config::Config,
    error::WorkerError,
    job::{AdmissionObservation, CommandSummary, ProcessIdentity, QueueEntry, QueueEntryKind},
    process::SystemProcessRunner,
    project_state::ProjectState,
    scheduler::{CandidateSlot, WorkerPreference},
    supervisor::{
        ProcessGroupMembership, ProcessGroupObservation, ProcessInspector, ProcessObservation,
        SystemProcessInspector,
    },
    task::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, RunnerIdentity, TaskId, TaskLimits,
        TaskMeta, TaskMetaInput, TaskSource, TaskState, TaskStatus, TurnId,
    },
    task_client::{TaskClient, TaskSubmitRequest},
    transfer_repo::repo_id_for,
    turn_runner::InlineRunnerExecutor,
};
use uuid::Uuid;

fn task_record(number: u128, project: char, worktree: char, repo: char) -> LocalTaskRecord {
    task_record_with_ids(
        number,
        project.to_string().repeat(64),
        worktree.to_string().repeat(64),
        repo.to_string().repeat(64),
    )
}

fn task_record_with_ids(
    number: u128,
    project_id: String,
    worktree_id: String,
    repo_id: String,
) -> LocalTaskRecord {
    let base = "a".repeat(40).parse().unwrap();
    let meta = TaskMeta::new(TaskMetaInput {
        task_id: TaskId::new(Uuid::from_u128(number)),
        run_id: None,
        project_id,
        worktree_id,
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
        base_oid: base,
        limits: TaskLimits::default(),
        close_policy: ClosePolicy::Never,
        env_profile: None,
        git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
        title: None,
        prompt: "fixture prompt".into(),
        created_at_millis: 1,
    })
    .unwrap();
    let status = TaskStatus::new(
        TaskState::Queued,
        None,
        None,
        false,
        Some(meta.base_oid().clone()),
        None,
        Vec::new(),
        Vec::new(),
        None,
        Vec::new(),
        1,
    )
    .unwrap();
    LocalTaskRecord::new(meta, status, None, None, None, repo_id, None, true, None).unwrap()
}

fn context_file(root: &Path, record: &LocalTaskRecord) -> PathBuf {
    root.join("turns")
        .join(record.meta().task_id().to_string())
        .join("project.json")
}

#[test]
fn private_project_context_roundtrips_without_changing_public_records() {
    // Break caught: task execution loses its project after the submitting cwd
    // changes, or solving that leaks the local path through task/queue JSON.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().canonicalize().unwrap().join("state");
    let project = fixture.path().join("private-project");
    fs::create_dir(&project).unwrap();
    let canonical_project = project.canonicalize().unwrap();
    let record = task_record(1, 'a', 'b', 'c');
    let store = ClientStateStore::open(&root).unwrap();
    store.create_task(record.clone()).unwrap();
    let task_file = root
        .join("tasks")
        .join(format!("{}.json", record.meta().task_id()));
    let public_task_before = fs::read(&task_file).unwrap();
    let queue_before = fs::read(root.join("queue/state.json")).unwrap();
    assert_eq!(store.task_project_path(&record).unwrap(), None);

    store.write_task_project_path(&record, &project).unwrap();
    store
        .write_task_project_path(&record, &canonical_project)
        .unwrap();

    let reopened = ClientStateStore::open(&root).unwrap();
    assert_eq!(
        reopened.task_project_path(&record).unwrap(),
        Some(canonical_project)
    );
    assert_eq!(fs::read(&task_file).unwrap(), public_task_before);
    assert_eq!(
        fs::read(root.join("queue/state.json")).unwrap(),
        queue_before
    );
    assert_eq!(
        fs::metadata(context_file(&root, &record))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(context_file(&root, &record).parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );

    let turn_id = TurnId::new(Uuid::from_u128(10));
    reopened
        .write_turn_prompt(record.meta().task_id(), turn_id, "private prompt")
        .unwrap();
    reopened
        .remove_turn_prompt(record.meta().task_id(), turn_id)
        .unwrap();
    assert!(reopened.task_project_path(&record).unwrap().is_some());
    reopened
        .remove_task_submission_turns(record.meta().task_id())
        .unwrap();
    assert_eq!(reopened.task_project_path(&record).unwrap(), None);
}

#[test]
fn private_project_context_is_immutable_and_bound_to_all_task_identifiers() {
    // Break caught: an immutable context is rebound to another checkout or
    // copied across task/project/worktree/repository identities.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().canonicalize().unwrap().join("state");
    let first = fixture.path().join("first");
    let second = fixture.path().join("second");
    fs::create_dir(&first).unwrap();
    fs::create_dir(&second).unwrap();
    let record = task_record(2, 'a', 'b', 'c');
    let store = ClientStateStore::open(&root).unwrap();
    store.write_task_project_path(&record, &first).unwrap();
    let original = fs::read(context_file(&root, &record)).unwrap();
    assert!(store.write_task_project_path(&record, &second).is_err());
    assert_eq!(fs::read(context_file(&root, &record)).unwrap(), original);

    for wrong in [
        task_record(2, 'd', 'b', 'c'),
        task_record(2, 'a', 'd', 'c'),
        task_record(2, 'a', 'b', 'd'),
    ] {
        assert_eq!(
            store.task_project_path(&wrong).unwrap_err().public_code(),
            "TASK_PROJECT_CONTEXT_MISMATCH"
        );
        assert!(store.write_task_project_path(&wrong, &first).is_err());
    }
    let other = task_record(3, 'a', 'b', 'c');
    store.write_task_project_path(&other, &first).unwrap();
    fs::write(context_file(&root, &other), &original).unwrap();
    assert_eq!(
        store.task_project_path(&other).unwrap_err().public_code(),
        "TASK_PROJECT_CONTEXT_MISMATCH"
    );
}

#[test]
fn private_project_context_preserves_non_utf8_unix_paths() {
    // Break caught: lossy UTF-8 conversion changes the checkout selected by a
    // detached runner.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().canonicalize().unwrap().join("state");
    let project = fixture
        .path()
        .canonicalize()
        .unwrap()
        .join(OsString::from_vec(b"project-\xff".to_vec()));
    let record = task_record(4, 'a', 'b', 'c');
    let store = ClientStateStore::open(&root).unwrap();
    #[cfg(not(target_os = "macos"))]
    {
        fs::create_dir(&project).unwrap();
        store.write_task_project_path(&record, &project).unwrap();
    }
    #[cfg(target_os = "macos")]
    {
        // APFS refuses invalid UTF-8 names. Decode a private Unix context
        // fixture to exercise byte preservation without requiring that the
        // represented checkout is available on this filesystem.
        let existing = fixture.path().canonicalize().unwrap();
        store.write_task_project_path(&record, &existing).unwrap();
        let file = context_file(&root, &record);
        let bytes = fs::read_to_string(&file).unwrap();
        let before = format!(
            "\"path_base64\":\"{}\"",
            STANDARD.encode(existing.as_os_str().as_bytes())
        );
        let after = format!(
            "\"path_base64\":\"{}\"",
            STANDARD.encode(project.as_os_str().as_bytes())
        );
        let changed = bytes.replace(&before, &after);
        assert_ne!(bytes, changed);
        fs::write(file, changed).unwrap();
    }
    assert_eq!(store.task_project_path(&record).unwrap(), Some(project));
}

#[test]
fn private_project_context_rejects_unrooted_or_nonprivate_storage() {
    // Break caught: context lookup follows an attacker-controlled symlink or
    // reads a file exposed to other local users.
    for attack in [
        "file_symlink",
        "directory_symlink",
        "permissions",
        "oversized",
        "noncanonical",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let root = fixture.path().canonicalize().unwrap().join("state");
        let project = fixture.path().join("project");
        fs::create_dir(&project).unwrap();
        let record = task_record(5, 'a', 'b', 'c');
        let store = ClientStateStore::open(&root).unwrap();
        store.write_task_project_path(&record, &project).unwrap();
        let file = context_file(&root, &record);
        match attack {
            "file_symlink" => {
                let outside = fixture.path().join("outside.json");
                fs::rename(&file, &outside).unwrap();
                std::os::unix::fs::symlink(&outside, &file).unwrap();
            }
            "directory_symlink" => {
                let task_dir = file.parent().unwrap();
                let outside = fixture.path().join("outside");
                fs::rename(task_dir, &outside).unwrap();
                std::os::unix::fs::symlink(&outside, task_dir).unwrap();
            }
            "permissions" => fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap(),
            "oversized" => fs::write(&file, vec![b' '; 1024 * 1024 + 1]).unwrap(),
            "noncanonical" => {
                let mut bytes = fs::read(&file).unwrap();
                bytes.push(b'\n');
                fs::write(&file, bytes).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            store.task_project_path(&record).is_err(),
            "accepted {attack}"
        );
        assert!(
            store.write_task_project_path(&record, &project).is_err(),
            "overwrote {attack}"
        );
    }
}

#[test]
fn private_project_context_rejects_relative_and_nondirectory_paths() {
    // Break caught: an execution context depends on the later caller's cwd
    // or stores a regular file as the execution directory.
    let fixture = tempfile::tempdir().unwrap();
    let root = fixture.path().canonicalize().unwrap().join("state");
    let record = task_record(6, 'a', 'b', 'c');
    let store = ClientStateStore::open(&root).unwrap();
    assert!(
        store
            .write_task_project_path(&record, Path::new("."))
            .is_err()
    );
    let file = fixture.path().join("ordinary-file");
    fs::write(&file, b"content").unwrap();
    assert!(store.write_task_project_path(&record, &file).is_err());
    assert_eq!(store.task_project_path(&record).unwrap(), None);
}

#[test]
fn reconciliation_counts_process_identities_instead_of_task_runner_records() {
    // Break caught: one process recorded on both sides of a handoff consumes
    // two slots, or PID reuse is collapsed despite a different start time.
    struct LiveInspector;
    impl ProcessInspector for LiveInspector {
        fn identity_for_pid(&self, pid: u32) -> Result<ProcessIdentity, WorkerError> {
            SystemProcessInspector.identity_for_pid(pid)
        }

        fn observe(&self, expected: ProcessIdentity) -> ProcessObservation {
            ProcessObservation::Matching {
                process_group: expected.pid(),
            }
        }

        fn observe_group(&self, _: u32) -> ProcessGroupObservation {
            ProcessGroupObservation::Ambiguous
        }

        fn observe_group_members(&self, _: u32) -> ProcessGroupMembership {
            ProcessGroupMembership::Ambiguous
        }
    }
    for same_start_time in [true, false] {
        let fixture = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(fixture.path().canonicalize().unwrap());
        let store =
            ClientStateStore::open_with_owner_inspector(&paths.state, LiveInspector).unwrap();
        let owner = SystemProcessInspector
            .identity_for_pid(std::process::id())
            .unwrap();
        for number in 1..=3 {
            let record = task_record(number, 'a', 'b', 'c');
            let identity = if number == 2 && !same_start_time {
                ProcessIdentity::new(owner.pid(), owner.start_time_micros() + 1).unwrap()
            } else {
                owner
            };
            let record = record
                .with_runner((number < 3).then_some(RunnerIdentity::new(identity)))
                .unwrap();
            let turn_id = TurnId::new(Uuid::from_u128(number + 10));
            store.create_task(record.clone()).unwrap();
            store
                .write_turn_prompt(record.meta().task_id(), turn_id, "fixture prompt")
                .unwrap();
            store
                .enqueue(
                    QueueEntry::new(
                        turn_id,
                        store.client_id(),
                        record.meta().project_id().into(),
                        record.meta().worktree_id().into(),
                        CommandSummary::argv(1).unwrap(),
                        Vec::new(),
                        WorkerPreference::Automatic,
                        QueueEntryKind::TaskTurn,
                        None,
                        owner,
                        number as u64,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let config = Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n\n[[workers]]\nname = \"mini-2\"\nssh = \"mac2\"\nslots = 1\n").unwrap();
        let report = TaskClient::new(
            &SystemProcessRunner,
            &config,
            &paths,
            &store,
            &InlineRunnerExecutor,
        )
        .reconcile_runners()
        .unwrap();
        assert_eq!(report.started_runners(), usize::from(same_start_time));
        assert_eq!(
            store
                .load_task(TaskId::new(Uuid::from_u128(3)))
                .unwrap()
                .runner()
                .is_some(),
            same_start_time
        );
    }
}

#[test]
fn reconciliation_reconstructs_a_turn_using_its_saved_project() {
    // Break caught: recovery runs in another checkout and derives the queue
    // requirements from that caller's cwd instead of the task's checkout.
    let repo = support::GitRepo::init();
    repo.write(
        ".worker.toml",
        b"version = 1\nrequires = [\"saved-project\"]\n",
    );
    let project = ProjectState::load(&SystemProcessRunner, repo.root(), &[]).unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let paths = support::task_harness::paths(fixture.path().canonicalize().unwrap());
    let store = ClientStateStore::open(&paths.state).unwrap();
    let record = task_record_with_ids(
        20,
        project.context.project_id.clone(),
        project.context.worktree_id.clone(),
        repo_id_for(&project.context.common_dir).unwrap(),
    )
    .with_runner(Some(RunnerIdentity::new(
        SystemProcessInspector
            .identity_for_pid(std::process::id())
            .unwrap(),
    )))
    .unwrap();
    store.create_task(record.clone()).unwrap();
    store.write_task_project_path(&record, repo.root()).unwrap();
    store
        .write_turn_prompt(
            record.meta().task_id(),
            TurnId::new(Uuid::from_u128(21)),
            "fixture prompt",
        )
        .unwrap();
    let config =
        Config::parse("version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n")
            .unwrap();
    TaskClient::new(
        &SystemProcessRunner,
        &config,
        &paths,
        &store,
        &InlineRunnerExecutor,
    )
    .reconcile_runners()
    .unwrap();
    let entry = store
        .queue_entry_for_task_turn(record.meta().task_id())
        .unwrap()
        .unwrap();
    assert!(entry.requirements().contains(&"saved-project".to_owned()));
    assert_eq!(entry.project_id(), project.context.project_id);
    assert_eq!(entry.worktree_id(), project.context.worktree_id);
}

#[test]
fn submit_persists_private_context_before_handoff_and_retires_it_on_rollback() {
    // Break caught: only manually-created contexts work, or a failure after
    // context publication leaves the task's private project path behind.
    for fail_before_prompt in [false, true] {
        let repo = support::GitRepo::init();
        repo.write("base.txt", b"base\n");
        repo.commit_all("base");
        let fixture = tempfile::tempdir().unwrap();
        let paths = support::task_harness::paths(fixture.path().canonicalize().unwrap());
        let store = ClientStateStore::open(&paths.state).unwrap();
        let config = Config::parse(
            "version = 1\n\n[[workers]]\nname = \"mini-1\"\nssh = \"mac1\"\nslots = 1\n",
        )
        .unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        store
            .admission_observation("mini-1", now, || {
                AdmissionObservation::new(
                    "mini-1".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["agent:codex".into()],
                    Some(8 * 1024 * 1024 * 1024),
                    64 * 1024 * 1024 * 1024,
                    now,
                )
            })
            .unwrap();
        if fail_before_prompt {
            store.inject_write_failure_once(ClientStateWritePoint::BeforeTurnPromptWrite);
        }
        let result = TaskClient::new(
            &SystemProcessRunner,
            &config,
            &paths,
            &store,
            &InlineRunnerExecutor,
        )
        .submit(
            TaskSubmitRequest {
                agent: AgentKind::Codex,
                model: None,
                effort: None,
                prompt: "fixture task".into(),
                project: repo.root().to_path_buf(),
                base: "main".into(),
                wip: false,
                source: Some("local".into()),
                publish: Some(vec!["fetch".into()]),
                publish_branch: None,
                cli_includes: Vec::new(),
                limits: TaskLimits::default(),
                close_policy: ClosePolicy::Never,
                env_profile: None,
                preference: WorkerPreference::Automatic,
                wait_for_capacity: true,
                attached: false,
                run_id: None,
            },
            &mut Vec::new(),
            &mut Vec::new(),
        );
        if fail_before_prompt {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("before task turn prompt write")
            );
            assert!(store.list_tasks().unwrap().is_empty());
            assert!(store.queue_snapshot().unwrap().entries().is_empty());
            assert!(
                fs::read_dir(paths.state.join("turns"))
                    .unwrap()
                    .all(|entry| {
                        entry
                            .unwrap()
                            .file_name()
                            .to_str()
                            .and_then(|name| name.parse::<TaskId>().ok())
                            .is_none()
                    })
            );
        } else {
            let report = result.unwrap();
            let record = store.load_task(report.task_id()).unwrap();
            assert_eq!(
                store.task_project_path(&record).unwrap(),
                Some(repo.root().canonicalize().unwrap())
            );
            assert!(record.runner().is_some());
            assert!(
                !String::from_utf8(record.canonical_bytes().unwrap())
                    .unwrap()
                    .contains("project.json")
            );
        }
    }
}
