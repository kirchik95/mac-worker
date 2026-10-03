use mac_worker::test_support::integration::*;
use mac_worker::test_support::{
    core::error::{ProcessError, WorkerError},
    host::process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct NativeRecordingRunner {
    calls: Mutex<Vec<ProcessRequest>>,
}

#[test]
fn host_deadline_interrupts_push_and_retains_its_uncertain_intent() {
    struct Expire<'a>(&'a ManualIntegrationRuntime);
    impl ProcessRunner for Expire<'_> {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            SystemProcessRunner.run(request)
        }
        fn run_interruptible(
            &self,
            request: &ProcessRequest,
            stop: &dyn Fn() -> bool,
        ) -> Result<ProcessResult, WorkerError> {
            if is_push(request) {
                self.0.advance(HOST_DEADLINE);
            }
            SystemProcessRunner.run_interruptible(request, stop)
        }
    }
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    f.prepare();
    assert!(
        f.execute_with(IntegrationStep::Push, &Expire(&f.runtime))
            .is_err()
    );
    assert_eq!(f.origin_tip(), target);
    let record = HostIntegrationStore::new(&f.store)
        .load(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .unwrap();
    assert!(record.push_intent.unwrap().uncertain);
    f.push();
}

#[test]
fn workspace_merge_crash_replays_before_any_auxiliary_admission() {
    struct CrashAfterMerge;
    impl ProcessRunner for CrashAfterMerge {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let result = SystemProcessRunner.run(request)?;
            if request.args.iter().any(|arg| arg == "--no-commit") {
                panic!("crash after native merge");
            }
            Ok(result)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || f.execute_with(IntegrationStep::Prepare, &CrashAfterMerge)
        ))
        .is_err()
    );
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::NeedTurn {
            purpose: IntegrationTurnPurpose::Resolve,
            ..
        }
    ));
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert_eq!(f.git(&["rev-parse", "MERGE_HEAD"]), target.as_str());
}

#[test]
fn resolution_tree_is_frozen_before_commit_and_cannot_rebind_after_crash() {
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    f.commit_task();
    f.advance_target_with("payload.txt", b"theirs\n");
    f.execute(IntegrationStep::Prepare).unwrap();
    f.write("payload.txt", b"resolved\n");
    f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
    f.runtime.crash_at(IntegrationHook::AfterCommitBeforePin);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || f.execute(IntegrationStep::AcceptTurn)
        ))
        .is_err()
    );
    let tree = f.git(&["write-tree"]);
    let record = HostIntegrationStore::new(&f.store)
        .load(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .unwrap();
    assert_eq!(
        record
            .candidates
            .last()
            .unwrap()
            .tree_oid
            .as_ref()
            .map(|tree| tree.as_str()),
        Some(tree.as_str())
    );
    f.runtime.restart();
    f.write("payload.txt", b"different resolution\n");
    assert!(f.execute(IntegrationStep::AcceptTurn).is_err());
}

#[test]
fn clean_candidate_crashes_keep_one_manifest_and_one_merge_oid() {
    for hook in [
        IntegrationHook::AfterIntent,
        IntegrationHook::AfterFetchBeforePin,
        IntegrationHook::AfterTargetPin,
        IntegrationHook::AfterWorkspaceManifest,
        IntegrationHook::AfterCommitBeforePin,
        IntegrationHook::AfterMergePin,
    ] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        f.runtime.crash_at(hook);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || f.execute(IntegrationStep::Prepare)
            ))
            .is_err()
        );
        f.runtime.restart();
        let first = f.prepare();
        let second = f.prepare();
        assert_eq!(first, second);
        assert_eq!(f.record.candidates.len(), 1);
        f.push();
        assert_eq!(f.origin_tip(), first);
    }
}

#[test]
fn remote_host_uses_bounded_typed_transport_and_rejects_rebound_replies() {
    use mac_worker::test_support::{core::config::WorkerEntry, transfer::RemoteJobClient};
    use std::os::unix::process::ExitStatusExt;
    struct Reply {
        bytes: Vec<u8>,
        calls: Mutex<Vec<ProcessRequest>>,
    }
    impl ProcessRunner for Reply {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(ProcessResult {
                status: std::process::ExitStatus::from_raw(0),
                stdout: self.bytes.clone(),
                stderr: vec![],
            })
        }
    }
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let request = step_request(IntegrationStep::Build, record.clone());
    let response = HostIntegrationResponse::CandidateReady {
        identity: IntegrationResponseIdentity::for_request(&request),
        candidate: Box::new(sample_candidate(&record)),
    };
    let worker = WorkerEntry {
        name: "fixture-worker".into(),
        ssh: "fixture-ssh".into(),
        slots: 1,
        capabilities: vec![],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    };
    let runner = Reply {
        bytes: encode_host_response(&response).unwrap(),
        calls: Mutex::new(vec![]),
    };
    let client = RemoteJobClient::new(&runner);
    assert_eq!(
        RemoteIntegrationHost::new(&client, &worker)
            .execute(&request)
            .unwrap(),
        response
    );
    let calls = runner.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].policy.deadline, HOST_DEADLINE);
    assert_eq!(calls[0].policy.stdout_limit, MAX_INTEGRATION_RPC_BYTES);
    assert_eq!(
        calls[0].stdin.as_deref(),
        Some(encode_host_request(&request).unwrap().as_slice())
    );
    assert!(
        calls[0]
            .args
            .iter()
            .any(|arg| arg.to_string_lossy().contains("host task-integration"))
    );
    drop(calls);
    let mut identity = IntegrationResponseIdentity::for_request(&request);
    identity.revision = identity.revision.next().unwrap();
    let rebound = HostIntegrationResponse::Blocked {
        identity,
        code: IntegrationCode::IntegrationNetwork,
        retry_exhausted: false,
    };
    let runner = Reply {
        bytes: encode_host_response(&rebound).unwrap(),
        calls: Mutex::new(vec![]),
    };
    let client = RemoteJobClient::new(&runner);
    assert_eq!(
        RemoteIntegrationHost::new(&client, &worker)
            .execute(&request)
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationStateInvalid.as_str()
    );
}
impl ProcessRunner for NativeRecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.calls.lock().unwrap().push(request.clone());
        SystemProcessRunner.run(request)
    }
}
fn is_push(request: &ProcessRequest) -> bool {
    request.args.iter().any(|arg| arg == "push")
}

#[test]
fn every_failed_push_reobserves_before_auth_network_missing_or_unknown_classification() {
    use std::os::unix::process::ExitStatusExt;
    struct Failure {
        kind: &'static str,
        pushed: AtomicUsize,
        calls: Mutex<Vec<ProcessRequest>>,
    }
    impl ProcessRunner for Failure {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.calls.lock().unwrap().push(request.clone());
            if is_push(request) {
                self.pushed.fetch_add(1, Ordering::SeqCst);
                return Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(256),
                    stdout: vec![],
                    stderr: if self.kind == "auth" {
                        b"fatal: Authentication failed for fixture".to_vec()
                    } else {
                        b"network unavailable".to_vec()
                    },
                });
            }
            if self.pushed.load(Ordering::SeqCst) > 0
                && request.args.iter().any(|arg| arg == "ls-remote")
            {
                if self.kind == "unknown" {
                    return Err(ProcessError::Cancelled.into());
                }
                if self.kind == "missing" {
                    return Ok(ProcessResult {
                        status: std::process::ExitStatus::from_raw(0),
                        stdout: vec![],
                        stderr: vec![],
                    });
                }
            }
            SystemProcessRunner.run(request)
        }
    }
    for (kind, code) in [
        ("auth", IntegrationCode::IntegrationAuthFailed),
        ("network", IntegrationCode::IntegrationNetwork),
        ("missing", IntegrationCode::IntegrationTargetMissing),
        ("unknown", IntegrationCode::IntegrationNetwork),
    ] {
        let mut f = GitIntegrationFixture::new();
        let target = f.commit_base();
        f.commit_task();
        f.prepare();
        let runner = Failure {
            kind,
            pushed: AtomicUsize::new(0),
            calls: Mutex::new(vec![]),
        };
        assert_eq!(
            f.execute_with(IntegrationStep::Push, &runner)
                .unwrap_err()
                .public_code(),
            code.as_str()
        );
        assert_eq!(f.origin_tip(), target);
        let calls = runner.calls.lock().unwrap();
        let push = calls.iter().position(is_push).unwrap();
        let observations: Vec<_> = calls
            .iter()
            .enumerate()
            .filter(|(_, request)| request.args.iter().any(|arg| arg == "ls-remote"))
            .collect();
        assert_eq!(observations.len(), 2);
        assert!(observations[0].0 < push && observations[1].0 > push);
        assert_eq!(observations[0].1.environment, observations[1].1.environment);
        assert_eq!(observations[0].1.args, observations[1].1.args);
        assert_eq!(runner.pushed.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn failed_push_with_source_already_reachable_settles_without_pushing_a_second_merge() {
    use std::os::unix::process::ExitStatusExt;
    struct PublishSource<'a>(&'a GitIntegrationFixture, AtomicUsize);
    impl ProcessRunner for PublishSource<'_> {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if is_push(request) {
                self.1.fetch_add(1, Ordering::SeqCst);
                self.0
                    .git(&["push", &self.0.record.policy.origin, "HEAD:refs/heads/main"]);
                return Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(256),
                    stdout: b"!\tfixture\t[remote rejected]\n".to_vec(),
                    stderr: vec![],
                });
            }
            SystemProcessRunner.run(request)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    let source = f.commit_task();
    f.prepare();
    let runner = PublishSource(&f, AtomicUsize::new(0));
    assert!(
        matches!(f.execute_with(IntegrationStep::Push, &runner).unwrap(),
        HostIntegrationResponse::Integrated { receipt, .. }
            if receipt.disposition == IntegrationDisposition::AlreadyIntegrated && receipt.merge_oid.is_none())
    );
    assert_eq!(f.origin_tip(), source);
    assert_eq!(runner.1.load(Ordering::SeqCst), 1);
}

#[test]
fn missing_target_and_base_not_on_target_fail_before_a_candidate_or_push() {
    for missing in [true, false] {
        let mut f = GitIntegrationFixture::new();
        let base = f.commit_base();
        let head = f.commit_task();
        let code = if missing {
            f.record.policy.target = validate_integration_target("absent").unwrap();
            f.record.target_key = f.record.policy.target_key().unwrap();
            f.record.snapshot.integration_id = IntegrationId::derive(
                f.record.task_id,
                f.record.snapshot.source_turn_id,
                &head,
                &f.record.target_key,
            )
            .unwrap();
            IntegrationCode::IntegrationTargetMissing
        } else {
            f.record.cycle_base = head;
            IntegrationCode::IntegrationBaseNotOnTarget
        };
        assert_eq!(
            f.execute(IntegrationStep::Prepare)
                .unwrap_err()
                .public_code(),
            code.as_str()
        );
        assert_eq!(f.origin_tip(), base);
        assert!(
            HostIntegrationStore::new(&f.store)
                .load(&f.record.policy.project_id, f.record.task_id)
                .unwrap()
                .unwrap()
                .candidates
                .is_empty()
        );
    }
}

#[test]
fn unsafe_private_records_fail_structurally_before_any_git_command() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for kind in ["mode", "hardlink", "symlink"] {
        let mut f = GitIntegrationFixture::new();
        f.commit_base();
        f.commit_task();
        f.prepare();
        let record = f
            .store
            .task_dir(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .join("integration/record.json");
        match kind {
            "mode" => {
                std::fs::set_permissions(&record, std::fs::Permissions::from_mode(0o644)).unwrap()
            }
            "hardlink" => std::fs::hard_link(&record, record.with_extension("linked")).unwrap(),
            _ => {
                let saved = record.with_extension("saved");
                std::fs::rename(&record, &saved).unwrap();
                symlink(saved, &record).unwrap();
            }
        }
        let runner = NativeRecordingRunner::default();
        let error = f.execute_with(IntegrationStep::Push, &runner).unwrap_err();
        assert!(
            matches!(error, WorkerError::Io(ref error) if error.kind() == std::io::ErrorKind::PermissionDenied),
            "{error:?}"
        );
        assert!(runner.calls.lock().unwrap().is_empty());
    }
}

#[test]
fn bounded_push_fence_refuses_a_concurrent_stop_without_acknowledging_it() {
    struct StopDuringPush<'a> {
        fixture: &'a GitIntegrationFixture,
        observed: Mutex<Option<String>>,
    }
    impl ProcessRunner for StopDuringPush<'_> {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if is_push(request) {
                let f = self.fixture;
                let revoke = HostIntegrationRequest {
                    protocol_version: 7,
                    task_id: f.record.task_id,
                    integration_id: Some(f.record.snapshot.integration_id),
                    epoch: f.record.snapshot.epoch,
                    revision: f.record.snapshot.revision,
                    action: HostIntegrationAction::Revoke {
                        tombstone: IntegrationTombstone {
                            epoch: f.record.snapshot.epoch,
                            revision: f.record.snapshot.revision,
                            requested_at_millis: 1001,
                            acknowledged: false,
                        },
                    },
                };
                let error = HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime)
                    .execute(&revoke)
                    .unwrap_err();
                *self.observed.lock().unwrap() = Some(error.public_code());
            }
            SystemProcessRunner.run(request)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    let runner = StopDuringPush {
        fixture: &f,
        observed: Mutex::new(None),
    };
    f.execute_with(IntegrationStep::Push, &runner).unwrap();
    assert_eq!(
        runner.observed.lock().unwrap().as_deref(),
        Some(IntegrationCode::IntegrationStopUnconfirmed.as_str())
    );
    assert_eq!(f.origin_tip(), merge);
    assert!(
        HostIntegrationStore::new(&f.store)
            .load(&f.record.policy.project_id, f.record.task_id)
            .unwrap()
            .unwrap()
            .tombstone
            .is_none()
    );
}

#[test]
fn candidate_attempts_are_bounded_and_a_conflicting_pin_is_never_replaced() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.prepare();
    for number in 2..=3 {
        f.advance_target_with(&format!("target-{number}.txt"), b"advance\n");
        assert!(
            matches!(f.execute(IntegrationStep::Prepare).unwrap(), HostIntegrationResponse::CandidateReady { candidate, .. } if candidate.id.attempt == number)
        );
    }
    let target = f.advance_target_with("target-4.txt", b"advance\n");
    assert_eq!(
        f.execute(IntegrationStep::Prepare)
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationTargetMovedExhausted.as_str()
    );
    assert_eq!(f.origin_tip(), target);

    let mut f = GitIntegrationFixture::new();
    let base = f.commit_base();
    let source = f.commit_task();
    f.runtime.crash_at(IntegrationHook::AfterMergePin);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || f.execute(IntegrationStep::Prepare)
        ))
        .is_err()
    );
    f.runtime.restart();
    let mirror = f.store.mirror(&f.record.policy.project_id).unwrap();
    let pin = format!(
        "refs/mac-worker/integration/{}/{}/1/merge",
        f.record.snapshot.integration_id, f.record.snapshot.epoch
    );
    f.git(&[
        "--git-dir",
        mirror.path().to_str().unwrap(),
        "update-ref",
        &pin,
        source.as_str(),
    ]);
    assert!(f.execute(IntegrationStep::Prepare).is_err());
    assert_eq!(
        f.git(&[
            "--git-dir",
            mirror.path().to_str().unwrap(),
            "rev-parse",
            &pin
        ]),
        source.as_str()
    );
    assert_eq!(f.origin_tip(), base);
}

#[test]
fn info_attributes_must_be_absent_or_empty_in_both_native_repositories() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let mirror = f
        .store
        .mirror(&f.record.policy.project_id)
        .unwrap()
        .path()
        .to_path_buf();
    let info = f.workspace().join(".git/info/attributes");
    let bare_info = mirror.join("info/attributes");
    std::fs::write(&info, b"").unwrap();
    std::fs::write(&bare_info, b"").unwrap();
    f.prepare();
    for path in [info, bare_info] {
        std::fs::write(&path, b"* merge=union\n").unwrap();
        assert_eq!(
            f.execute(IntegrationStep::Prepare)
                .unwrap_err()
                .public_code(),
            IntegrationCode::IntegrationStateInvalid.as_str()
        );
        std::fs::write(path, b"").unwrap();
    }
}

#[test]
fn configured_union_driver_keeps_builtin_union_semantics_without_project_execution() {
    let mut f = GitIntegrationFixture::new();
    f.write(".gitattributes", b"payload.txt merge=union\n");
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    f.commit_task();
    f.advance_target_with("payload.txt", b"theirs\n");
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    let mirror = f.store.mirror(&f.record.policy.project_id).unwrap();
    f.git(&["config", "merge.union.driver", "exit 77"]);
    f.git(&[
        "--git-dir",
        mirror.path().to_str().unwrap(),
        "config",
        "merge.union.driver",
        "exit 77",
    ]);
    let response = f.execute(IntegrationStep::Prepare).unwrap();
    let HostIntegrationResponse::NeedTurn {
        candidate,
        purpose: IntegrationTurnPurpose::Verify,
        ..
    } = response
    else {
        panic!("union became a conflict")
    };
    assert_eq!(
        f.git(&["write-tree"]),
        candidate.tree_oid.as_ref().unwrap().as_str()
    );
    assert_eq!(
        std::fs::read(f.workspace().join("payload.txt")).unwrap(),
        b"ours\ntheirs\n"
    );
}

// The test receiver forwards the native advertisement before advancing origin, then
// lets native receive-pack perform its old-OID transaction. No production argv changes.
struct NativeReceiver {
    shim: std::path::PathBuf,
    replies: Mutex<Vec<ProcessResult>>,
}
impl NativeReceiver {
    fn new(f: &GitIntegrationFixture, move_oid: &str) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let shim = f.workspace().parent().unwrap().join("receive-barrier.py");
        let config = serde_json::json!({"workspace":f.workspace(), "origin":f.record.policy.origin, "move_oid":move_oid});
        let script = format!("#!/usr/bin/env python3\nCONFIG = {config}\n")
            + r#"
import os, subprocess, sys, threading
env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_CONFIG_')}
env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null')
receiver = subprocess.Popen(['/usr/bin/git', 'receive-pack', sys.argv[1]],
    stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
while True:
    header = receiver.stdout.read(4)
    if len(header) != 4: raise RuntimeError('missing native advertisement')
    sys.stdout.buffer.write(header)
    size = int(header, 16)
    if size == 0: break
    sys.stdout.buffer.write(receiver.stdout.read(size - 4))
sys.stdout.buffer.flush()
if CONFIG['move_oid']:
    subprocess.run(['/usr/bin/git', '-c', 'core.hooksPath=/dev/null', '-C', CONFIG['workspace'],
        'push', CONFIG['origin'], CONFIG['move_oid'] + ':refs/heads/main'],
        env=env, stdout=sys.stderr, stderr=sys.stderr, check=True)
def send_commands():
    try:
        while True:
            data = sys.stdin.buffer.read1(65536)
            if not data: break
            receiver.stdin.write(data)
            receiver.stdin.flush()
        receiver.stdin.close()
    except BrokenPipeError: pass
threading.Thread(target=send_commands, daemon=True).start()
while True:
    data = receiver.stdout.read1(65536)
    if not data: break
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
sys.exit(receiver.wait())
"#;
        std::fs::write(&shim, script).unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            shim,
            replies: Mutex::new(vec![]),
        }
    }
}
impl ProcessRunner for NativeReceiver {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let mut native = request.clone();
        if let Some(at) = native.args.iter().position(|arg| arg == "push") {
            native.args.insert(
                at + 1,
                format!("--receive-pack={}", self.shim.display()).into(),
            );
        }
        let result = SystemProcessRunner.run(&native)?;
        if is_push(request) {
            self.replies.lock().unwrap().push(ProcessResult {
                status: result.status,
                stdout: result.stdout.clone(),
                stderr: result.stderr.clone(),
            });
        }
        Ok(result)
    }
}

#[test]
fn server_cas_after_advertisement_is_movement_and_rebuilds() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.write("intermediate.txt", b"intermediate\n");
    let ancestor = f.commit("intermediate");
    let head = f.commit_task();
    f.prepare();
    let runner = NativeReceiver::new(&f, ancestor.as_str());
    assert!(
        matches!(f.execute_with(IntegrationStep::Push, &runner).unwrap(), HostIntegrationResponse::TargetMoved { observed_target, .. } if observed_target == ancestor)
    );
    let replies = runner.replies.lock().unwrap();
    assert_eq!(replies.len(), 1);
    assert!(String::from_utf8_lossy(&replies[0].stdout).contains("[remote rejected]"));
    assert!(!replies[0].status.success());
    drop(replies);
    assert_eq!(f.origin_tip(), ancestor);
    let rebuilt = f.prepare();
    assert_eq!(f.parents(&rebuilt), vec![ancestor, head]);
    f.push();
    assert_eq!(f.origin_tip(), rebuilt);
}

#[test]
fn unchanged_target_native_policy_rejection_is_not_movement() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    f.prepare();
    f.install_policy_rejection();
    let runner = NativeReceiver::new(&f, "");
    let error = f.execute_with(IntegrationStep::Push, &runner).unwrap_err();
    assert_eq!(
        error.public_code(),
        IntegrationCode::IntegrationPolicyRejected.as_str()
    );
    assert_eq!(f.origin_tip(), target);
    assert_eq!(runner.replies.lock().unwrap().len(), 1);
}

#[test]
fn host_merge_stage_abort_and_reset_never_execute_planted_project_commands() {
    use std::os::unix::fs::PermissionsExt;
    for revoke in [false, true] {
        let mut f = GitIntegrationFixture::new();
        f.write(
            ".gitattributes",
            b"payload.txt filter=evil merge=evil diff=evil\n",
        );
        f.write("payload.txt", b"base\n");
        f.write("check.sh", b"exit 77\n");
        f.write("setup.sh", b"exit 77\n");
        f.commit_base();
        f.write("payload.txt", b"ours\n");
        f.commit_task();
        f.advance_target_with("payload.txt", b"theirs\n");
        let sentinel = f.workspace().parent().unwrap().join("project-ran");
        let script = f.workspace().parent().unwrap().join("project-command");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf ran > '{}'\nexit 77\n",
                sentinel.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mirror = f
            .store
            .mirror(&f.record.policy.project_id)
            .unwrap()
            .path()
            .to_path_buf();
        for repo in [f.workspace(), mirror.as_path()] {
            for key in [
                "core.fsmonitor",
                "filter.evil.clean",
                "filter.evil.smudge",
                "filter.evil.process",
                "merge.evil.driver",
                "diff.evil.command",
                "diff.evil.textconv",
                "gpg.program",
            ] {
                let output = std::process::Command::new("/usr/bin/git")
                    .args([
                        "-C",
                        repo.to_str().unwrap(),
                        "config",
                        key,
                        script.to_str().unwrap(),
                    ])
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            for (key, value) in [("filter.evil.required", "true"), ("commit.gpgSign", "true")] {
                assert!(
                    std::process::Command::new("/usr/bin/git")
                        .args(["-C", repo.to_str().unwrap(), "config", key, value])
                        .status()
                        .unwrap()
                        .success()
                );
            }
            let hooks = if repo == f.workspace() {
                repo.join(".git/hooks")
            } else {
                repo.join("hooks")
            };
            for hook in [
                "pre-commit",
                "commit-msg",
                "post-commit",
                "pre-push",
                "pre-merge-commit",
                "post-merge",
                "post-checkout",
                "post-rewrite",
                "reference-transaction",
            ] {
                std::fs::copy(&script, hooks.join(hook)).unwrap();
            }
        }
        assert!(matches!(
            f.execute(IntegrationStep::Prepare).unwrap(),
            HostIntegrationResponse::NeedTurn {
                purpose: IntegrationTurnPurpose::Resolve,
                ..
            }
        ));
        if revoke {
            let request = HostIntegrationRequest {
                protocol_version: 7,
                task_id: f.record.task_id,
                integration_id: Some(f.record.snapshot.integration_id),
                epoch: f.record.snapshot.epoch,
                revision: f.record.snapshot.revision,
                action: HostIntegrationAction::Revoke {
                    tombstone: IntegrationTombstone {
                        epoch: f.record.snapshot.epoch,
                        revision: f.record.snapshot.revision,
                        requested_at_millis: 1005,
                        acknowledged: false,
                    },
                },
            };
            HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime)
                .execute(&request)
                .unwrap();
        } else {
            f.write("payload.txt", b"resolved\n");
            f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
            f.execute(IntegrationStep::AcceptTurn).unwrap();
            f.push();
            f.execute(IntegrationStep::Repair).unwrap();
        }
        assert!(
            !sentinel.exists(),
            "project command executed (revoke={revoke})"
        );
    }
}

#[test]
fn sender_uses_exact_single_ref_argv_and_hardens_every_git_command() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    let runner = NativeRecordingRunner::default();
    assert!(matches!(
        f.execute_with(IntegrationStep::Push, &runner).unwrap(),
        HostIntegrationResponse::Integrated { .. }
    ));
    let calls = runner.calls.lock().unwrap();
    let pushes: Vec<_> = calls.iter().filter(|request| is_push(request)).collect();
    assert_eq!(pushes.len(), 1);
    let args: Vec<_> = pushes[0].args.iter().map(|s| s.to_str().unwrap()).collect();
    let at = args.iter().position(|arg| *arg == "push").unwrap();
    assert_eq!(
        &args[at..],
        &[
            "push",
            "--porcelain",
            "--no-verify",
            &format!("--force-with-lease=refs/heads/main:{target}"),
            &f.record.policy.origin,
            &format!("{merge}:refs/heads/main")
        ]
    );
    for call in calls.iter() {
        for arg in [
            "core.hooksPath=/dev/null",
            "core.fsmonitor=false",
            "commit.gpgSign=false",
            "submodule.recurse=false",
            "core.attributesFile=/dev/null",
            "push.followTags=false",
            "push.recurseSubmodules=no",
        ] {
            assert!(call.args.iter().any(|value| value == arg), "missing {arg}");
        }
        assert!(
            call.environment
                .iter()
                .any(|(key, value)| key == "GIT_ATTR_NOSYSTEM" && value == "1")
        );
        assert_eq!(call.policy.deadline, GIT_DEADLINE);
    }
}

#[test]
fn lost_native_push_reply_observes_success_before_any_repeat() {
    struct LostReply(AtomicUsize);
    impl ProcessRunner for LostReply {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let result = SystemProcessRunner.run(request)?;
            if is_push(request) {
                self.0.fetch_add(1, Ordering::SeqCst);
                return Err(ProcessError::DeadlineExceeded {
                    deadline: GIT_DEADLINE,
                }
                .into());
            }
            Ok(result)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    let merge = f.prepare();
    let runner = LostReply(AtomicUsize::new(0));
    for _ in 0..2 {
        assert!(matches!(
            f.execute_with(IntegrationStep::Push, &runner).unwrap(),
            HostIntegrationResponse::Integrated { .. }
        ));
    }
    assert_eq!(runner.0.load(Ordering::SeqCst), 1);
    assert_eq!(f.origin_tip(), merge);
}

#[test]
fn movement_to_an_ancestor_of_h_rejects_the_old_lease_and_rebuilds() {
    struct MoveBeforePush<'a> {
        f: &'a GitIntegrationFixture,
        target: String,
        moved: AtomicUsize,
    }
    impl ProcessRunner for MoveBeforePush<'_> {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if is_push(request) && self.moved.fetch_add(1, Ordering::SeqCst) == 0 {
                self.f.git(&[
                    "push",
                    &self.f.record.policy.origin,
                    &format!("{}:refs/heads/main", self.target),
                ]);
            }
            SystemProcessRunner.run(request)
        }
    }
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.write("intermediate.txt", b"intermediate\n");
    let ancestor = f.commit("intermediate");
    let head = f.commit_task();
    f.prepare();
    let runner = MoveBeforePush {
        f: &f,
        target: ancestor.to_string(),
        moved: AtomicUsize::new(0),
    };
    assert!(
        matches!(f.execute_with(IntegrationStep::Push, &runner).unwrap(), HostIntegrationResponse::TargetMoved { observed_target, .. } if observed_target == ancestor)
    );
    assert_eq!(f.origin_tip(), ancestor);
    drop(runner);
    let rebuilt = f.prepare();
    assert_eq!(f.parents(&rebuilt), vec![ancestor, head]);
    f.push();
    assert_eq!(f.origin_tip(), rebuilt);
}

#[test]
fn fast_forwardable_task_still_gets_one_merge_with_exact_lease() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    let task = f.commit_task();
    let merge = f.prepare();
    f.push();
    assert_eq!(f.parents(&merge), vec![target, task]);
    assert_eq!(f.origin_tip(), merge);
}

#[test]
fn built_in_minus_merge_conflicts_in_bare_and_workspace() {
    let mut f = GitIntegrationFixture::new();
    f.write(".gitattributes", b"payload.txt -merge\n");
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    let response = f.execute(IntegrationStep::Prepare).unwrap();
    let HostIntegrationResponse::NeedTurn {
        candidate, purpose, ..
    } = response
    else {
        panic!("expected resolver, got {response:?}")
    };
    assert_eq!(purpose, IntegrationTurnPurpose::Resolve);
    assert_eq!(candidate.conflict_paths, vec!["payload.txt"]);
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert_eq!(f.git(&["rev-parse", "MERGE_HEAD"]), target.as_str());
    assert!(!f.git(&["ls-files", "--unmerged"]).is_empty());
}

#[test]
fn native_workspace_preserves_untracked_symlinks_and_nul_delimited_conflict_names() {
    use std::os::unix::fs::symlink;
    let mut f = GitIntegrationFixture::new();
    let name = "odd 'name ; [x].txt";
    f.write(name, b"base\n");
    f.commit_base();
    f.write(name, b"ours\n");
    let head = f.commit_task();
    symlink("base.txt", f.workspace().join("keep-link")).unwrap();
    f.advance_target_with(name, b"theirs\n");
    let response = f.execute(IntegrationStep::Prepare).unwrap();
    assert!(
        matches!(response,HostIntegrationResponse::NeedTurn { candidate,.. } if candidate.conflict_paths == vec![name])
    );
    f.write(name, b"resolved\n");
    f.write("new/during.txt", b"auxiliary\n");
    let revoke = HostIntegrationRequest {
        protocol_version: 7,
        task_id: f.record.task_id,
        integration_id: Some(f.record.snapshot.integration_id),
        epoch: f.record.snapshot.epoch,
        revision: f.record.snapshot.revision,
        action: HostIntegrationAction::Revoke {
            tombstone: IntegrationTombstone {
                epoch: f.record.snapshot.epoch,
                revision: f.record.snapshot.revision,
                requested_at_millis: 1005,
                acknowledged: false,
            },
        },
    };
    HostIntegrationService::new(&f.store, &SystemProcessRunner, &f.runtime)
        .execute(&revoke)
        .unwrap();
    assert_eq!(
        std::fs::read_link(f.workspace().join("keep-link")).unwrap(),
        std::path::Path::new("base.txt")
    );
    assert!(!f.workspace().join("new/during.txt").exists());
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert_eq!(std::fs::read(f.workspace().join(name)).unwrap(), b"ours\n");
}

#[test]
fn native_resolution_stages_modes_links_binary_renames_and_deletions() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    f.write("binary.bin", b"\0base\n");
    f.write("run.sh", b"exit 0\n");
    f.write("old.txt", b"rename\n");
    f.write("delete.txt", b"delete\n");
    symlink("base.txt", f.workspace().join("link")).unwrap();
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    f.execute(IntegrationStep::Prepare).unwrap();
    f.write("payload.txt", b"resolved\n");
    f.write("binary.bin", b"\0resolved\n");
    std::fs::set_permissions(
        f.workspace().join("run.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::remove_file(f.workspace().join("link")).unwrap();
    symlink("payload.txt", f.workspace().join("link")).unwrap();
    symlink("binary.bin", f.workspace().join("extra-link")).unwrap();
    std::fs::rename(
        f.workspace().join("old.txt"),
        f.workspace().join("renamed.txt"),
    )
    .unwrap();
    std::fs::remove_file(f.workspace().join("delete.txt")).unwrap();
    f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
    let HostIntegrationResponse::CandidateReady { candidate, .. } =
        f.execute(IntegrationStep::AcceptTurn).unwrap()
    else {
        panic!("candidate missing")
    };
    let merge = candidate.merge_oid.unwrap();
    assert_eq!(f.parents(&merge), vec![target, head]);
    let tree = f.git(&["ls-tree", merge.as_str()]);
    assert!(tree.contains("100755 blob") && tree.contains("120000 blob"));
    assert!(
        tree.contains("renamed.txt") && !tree.contains("old.txt") && !tree.contains("delete.txt")
    );
    assert_eq!(f.git(&["show", &format!("{merge}:link")]), "payload.txt");
    assert_eq!(
        f.git(&["show", &format!("{merge}:binary.bin")]),
        "\0resolved"
    );
}

#[test]
fn large_binary_resolution_does_not_exceed_the_git_marker_output_cap() {
    let mut f = GitIntegrationFixture::new();
    let bytes = |value| {
        let mut bytes = vec![value; 100_000];
        bytes[0] = 0;
        bytes
    };
    f.write("payload.bin", &bytes(1));
    f.commit_base();
    f.write("payload.bin", &bytes(2));
    f.commit_task();
    f.advance_target_with("payload.bin", &bytes(3));
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::NeedTurn {
            purpose: IntegrationTurnPurpose::Resolve,
            ..
        }
    ));
    f.write("payload.bin", &bytes(4));
    f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
    assert!(matches!(
        f.execute(IntegrationStep::AcceptTurn).unwrap(),
        HostIntegrationResponse::CandidateReady { .. }
    ));
}

#[test]
fn symlink_resolution_targets_are_not_conflict_marker_text() {
    use std::os::unix::fs::symlink;
    let mut f = GitIntegrationFixture::new();
    symlink("base-target", f.workspace().join("link")).unwrap();
    f.commit_base();
    std::fs::remove_file(f.workspace().join("link")).unwrap();
    symlink("ours-target", f.workspace().join("link")).unwrap();
    f.commit_task();
    f.advance_target();
    let peer = f.origin().parent().unwrap().join("link-writer");
    f.git(&[
        "-C",
        f.origin().parent().unwrap().to_str().unwrap(),
        "clone",
        "-b",
        "main",
        f.origin().to_str().unwrap(),
        peer.to_str().unwrap(),
    ]);
    std::fs::remove_file(peer.join("link")).unwrap();
    symlink("theirs-target", peer.join("link")).unwrap();
    f.git(&["-C", peer.to_str().unwrap(), "add", "-A"]);
    f.git(&["-C", peer.to_str().unwrap(), "commit", "-m", "move link"]);
    f.git(&[
        "-C",
        peer.to_str().unwrap(),
        "push",
        "origin",
        "HEAD:refs/heads/main",
    ]);
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::NeedTurn {
            purpose: IntegrationTurnPurpose::Resolve,
            ..
        }
    ));
    std::fs::remove_file(f.workspace().join("link")).unwrap();
    symlink("<<<<<<<literal-target", f.workspace().join("link")).unwrap();
    f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
    assert!(matches!(
        f.execute(IntegrationStep::AcceptTurn).unwrap(),
        HostIntegrationResponse::CandidateReady { .. }
    ));
}

#[test]
fn multiple_merge_bases_use_native_recursive_merging() {
    let mut f = GitIntegrationFixture::new();
    let base = f.commit_base();
    f.write("left.txt", b"left\n");
    let left = f.commit("left");
    f.git(&["checkout", "--detach", base.as_str()]);
    f.write("right.txt", b"right\n");
    let right = f.commit("right");
    f.git(&["merge", "--no-ff", left.as_str(), "-m", "right merge"]);
    let target = f.git(&["rev-parse", "HEAD"]);
    f.git(&["checkout", "--detach", left.as_str()]);
    f.git(&["merge", "--no-ff", right.as_str(), "-m", "left merge"]);
    let branch = format!("task/{}", f.record.task_id);
    f.git(&["branch", "-f", &branch, "HEAD"]);
    f.git(&["checkout", &branch]);
    let head = f.commit_task();
    f.git(&[
        "push",
        &f.record.policy.origin,
        &format!("{target}:refs/heads/main"),
    ]);
    let bases = f.git(&["merge-base", "--all", head.as_str(), &target]);
    assert_eq!(bases.lines().count(), 2);
    let merge = f.prepare();
    assert_eq!(f.parents(&merge), vec![target.parse().unwrap(), head]);
    f.push();
    f.execute(IntegrationStep::Repair).unwrap();
    assert_eq!(f.git(&["show", &format!("{merge}:left.txt")]), "left");
    assert_eq!(f.git(&["show", &format!("{merge}:right.txt")]), "right");
}

#[test]
fn native_control_names_fail_closed_at_the_frozen_contract_bound() {
    let mut f = GitIntegrationFixture::new();
    let name = "odd\nname.txt";
    f.write(name, b"base\n");
    f.commit_base();
    f.write(name, b"ours\n");
    f.commit_task();
    f.advance_target_with(name, b"theirs\n");
    assert_eq!(
        f.execute(IntegrationStep::Prepare)
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationConflictListTooLarge.as_str()
    );
    assert_eq!(std::fs::read(f.workspace().join(name)).unwrap(), b"ours\n");
}

#[test]
fn markers_are_rejected_only_in_conflicted_or_newly_changed_text() {
    for introduced in [false, true] {
        let mut f = GitIntegrationFixture::new();
        f.write("payload.txt", b"base\n");
        f.write(
            "unchanged.md",
            b"<<<<<<< literal documentation\n=======\n>>>>>>> literal\n",
        );
        f.commit_base();
        f.write("payload.txt", b"ours\n");
        f.commit_task();
        f.advance_target_with("payload.txt", b"theirs\n");
        f.execute(IntegrationStep::Prepare).unwrap();
        f.write("payload.txt", b"resolved\n");
        if introduced {
            f.write("new.md", b"<<<<<<< unresolved\n");
            f.git(&["config", "diff.outputIndicatorNew", "."]);
            f.git(&["config", "color.ui", "always"]);
        }
        f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
        let result = f.execute(IntegrationStep::AcceptTurn);
        if introduced {
            assert_eq!(
                result.unwrap_err().public_code(),
                IntegrationCode::IntegrationResolutionIncomplete.as_str()
            );
        } else {
            assert!(matches!(
                result.unwrap(),
                HostIntegrationResponse::CandidateReady { .. }
            ));
        }
    }
}

#[test]
fn union_uses_h_as_ours_in_mirror_and_pinned_verify_workspace() {
    let mut f = GitIntegrationFixture::new();
    f.write(".gitattributes", b"payload.txt merge=union\n");
    f.write("payload.txt", b"base\n");
    f.commit_base();
    f.write("payload.txt", b"ours\n");
    f.commit_task();
    f.advance_target_with("payload.txt", b"theirs\n");
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    let response = f.execute(IntegrationStep::Prepare).unwrap();
    let HostIntegrationResponse::NeedTurn {
        candidate, purpose, ..
    } = response
    else {
        panic!("expected verifier, got {response:?}")
    };
    assert_eq!(purpose, IntegrationTurnPurpose::Verify);
    assert_eq!(f.git(&["write-tree"]), candidate.tree_oid.unwrap().as_str());
    assert_eq!(
        std::fs::read(f.workspace().join("payload.txt")).unwrap(),
        b"ours\ntheirs\n"
    );
}

#[test]
fn resolved_candidate_stages_native_edits_without_an_ordinary_commit() {
    let mut f = GitIntegrationFixture::new();
    f.write("payload.txt", b"base\n");
    let base = f.commit_base();
    f.write("payload.txt", b"ours\n");
    let head = f.commit_task();
    let target = f.advance_target_with("payload.txt", b"theirs\n");
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::NeedTurn {
            purpose: IntegrationTurnPurpose::Resolve,
            ..
        }
    ));
    f.write("payload.txt", b"resolved\n");
    f.write("extra.txt", b"additional repair\n");
    f.complete_auxiliary(IntegrationTurnPurpose::Resolve);
    let response = f.execute(IntegrationStep::AcceptTurn).unwrap();
    assert!(!matches!(response, HostIntegrationResponse::Blocked { .. }));
    let record = HostIntegrationStore::new(&f.store)
        .load(&f.record.policy.project_id, f.record.task_id)
        .unwrap()
        .unwrap();
    let candidate = record.candidates.last().unwrap();
    assert_eq!(
        f.parents(candidate.merge_oid.as_ref().unwrap()),
        vec![target, head.clone()]
    );
    assert_eq!(f.git(&["rev-parse", "HEAD"]), head.as_str());
    assert_ne!(
        candidate.tree_oid.as_ref().unwrap().as_str(),
        f.git(&["rev-parse", &format!("{base}^{{tree}}")]).as_str()
    );
}

#[test]
fn verifier_index_tampering_is_a_tree_mismatch() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    assert!(matches!(
        f.execute(IntegrationStep::Prepare).unwrap(),
        HostIntegrationResponse::NeedTurn {
            purpose: IntegrationTurnPurpose::Verify,
            ..
        }
    ));
    f.write("target.txt", b"tampered\n");
    f.git(&["add", "target.txt"]);
    f.complete_auxiliary(IntegrationTurnPurpose::Verify);
    let error = f.execute(IntegrationStep::AcceptTurn).unwrap_err();
    assert_eq!(error.public_code(), "INTEGRATION_VERIFY_TREE_MISMATCH");
}

#[test]
fn verifier_worktree_edits_have_a_distinct_failure_code() {
    let mut f = GitIntegrationFixture::new();
    f.commit_base();
    f.commit_task();
    f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    f.execute(IntegrationStep::Prepare).unwrap();
    f.write("target.txt", b"edited\n");
    f.complete_auxiliary(IntegrationTurnPurpose::Verify);
    let error = f.execute(IntegrationStep::AcceptTurn).unwrap_err();
    assert_eq!(error.public_code(), "INTEGRATION_VERIFY_CHANGED_TREE");
}

#[test]
fn verifier_rejects_raw_edits_even_when_git_normalizes_the_same_tree() {
    let mut f = GitIntegrationFixture::new();
    f.write(".gitattributes", b"*.txt text\n");
    f.commit_base();
    f.commit_task();
    f.advance_target();
    f.record.policy.verify = VerifyPolicy::MovedTarget;
    f.execute(IntegrationStep::Prepare).unwrap();
    f.write("base.txt", b"base\r\n");
    assert!(
        f.git(&["diff", "--no-ext-diff", "--no-textconv"])
            .is_empty()
    );
    f.complete_auxiliary(IntegrationTurnPurpose::Verify);
    assert_eq!(
        f.execute(IntegrationStep::AcceptTurn)
            .unwrap_err()
            .public_code(),
        IntegrationCode::IntegrationVerifyChangedTree.as_str()
    );
}

fn step_request(step: IntegrationStep, record: IntegrationRecord) -> HostIntegrationRequest {
    HostIntegrationRequest {
        protocol_version: 7,
        task_id: record.task_id,
        integration_id: Some(record.snapshot.integration_id),
        epoch: record.snapshot.epoch,
        revision: record.snapshot.revision,
        action: HostIntegrationAction::Step {
            step,
            record: Box::new(record),
        },
    }
}

#[test]
fn candidate_ready_requires_a_tree_and_a_merge_pin_from_build_onward() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    for step in [
        IntegrationStep::Fetch,
        IntegrationStep::Prepare,
        IntegrationStep::AcceptTurn,
        IntegrationStep::Build,
        IntegrationStep::Push,
        IntegrationStep::Repair,
    ] {
        let request = step_request(step, record.clone());
        let mut candidate = sample_candidate(&record);
        candidate.merge_oid = None;
        let ready = HostIntegrationResponse::CandidateReady {
            identity: IntegrationResponseIdentity::for_request(&request),
            candidate: Box::new(candidate.clone()),
        };
        assert_eq!(
            ready.validate_for(&request).is_ok(),
            matches!(
                step,
                IntegrationStep::Fetch | IntegrationStep::Prepare | IntegrationStep::AcceptTurn
            )
        );
        candidate.tree_oid = None;
        let missing_tree = HostIntegrationResponse::CandidateReady {
            identity: IntegrationResponseIdentity::for_request(&request),
            candidate: Box::new(candidate),
        };
        assert!(encode_host_response(&missing_tree).is_err());
    }
}

#[test]
fn candidate_ready_completes_absent_pins_once_and_keeps_all_frozen_inputs() {
    let mut record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    let mut partial = candidate.clone();
    partial.tree_oid = None;
    partial.merge_oid = None;
    record.candidates.push(partial);
    let request = step_request(IntegrationStep::Build, record.clone());
    let ready = HostIntegrationResponse::CandidateReady {
        identity: IntegrationResponseIdentity::for_request(&request),
        candidate: Box::new(candidate.clone()),
    };
    ready.validate_for(&request).unwrap();
    for field in [
        "tree_oid",
        "merge_oid",
        "target_head",
        "message",
        "identity",
        "timestamp_millis",
        "conflict_paths",
        "clean_h",
    ] {
        let mut frozen = record.clone();
        frozen.candidates[0] = candidate.clone();
        let mut wire = serde_json::to_value(&candidate).unwrap();
        wire[field] = match field {
            "target_head" => {
                wire["theirs"] = serde_json::json!("f".repeat(40));
                serde_json::json!("f".repeat(40))
            }
            "tree_oid" | "merge_oid" => serde_json::json!("f".repeat(40)),
            "message" => serde_json::json!("Different commit message"),
            "identity" => serde_json::json!({"name":"Other", "email":"other@example.test"}),
            "timestamp_millis" => serde_json::json!(1005),
            "conflict_paths" => serde_json::json!(["different.txt"]),
            "clean_h" => {
                let mut value = wire[field].clone();
                value["untracked_files"] = serde_json::json!(["different.txt"]);
                value
            }
            _ => unreachable!(),
        };
        let changed: IntegrationCandidate = serde_json::from_value(wire).unwrap();
        let response = HostIntegrationResponse::CandidateReady {
            identity: IntegrationResponseIdentity::for_request(&request),
            candidate: Box::new(changed),
        };
        assert!(
            response
                .validate_for(&step_request(IntegrationStep::Build, frozen))
                .is_err(),
            "{field}"
        );
        if !matches!(field, "tree_oid" | "merge_oid") {
            assert!(response.validate_for(&request).is_err(), "partial {field}");
        }
    }
    record.candidates[0].tree_oid = candidate.tree_oid.clone();
    ready
        .validate_for(&step_request(IntegrationStep::Build, record.clone()))
        .unwrap();
    record.candidates[0] = candidate;
    ready
        .validate_for(&step_request(IntegrationStep::Build, record))
        .unwrap();
}

#[test]
fn candidate_freezes_h_attributes_and_h_t_merge_orientation() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let candidate = sample_candidate(&record);
    candidate.validate().unwrap();
    assert_eq!(candidate.attribute_source, candidate.source_head);
    assert_eq!(candidate.ours, candidate.source_head);
    assert_eq!(candidate.theirs, candidate.target_head);
    for field in ["attribute_source", "ours", "theirs"] {
        let mut wire = serde_json::to_value(&candidate).unwrap();
        wire[field] = serde_json::json!("f".repeat(40));
        assert!(
            serde_json::from_value::<IntegrationCandidate>(wire).is_err(),
            "{field}"
        );
    }
}

#[test]
fn conflicts_and_prompts_are_bounded_without_dropping_paths() {
    let path = "a".repeat(MAX_CONFLICT_PATH_BYTES);
    assert!(validate_conflict_paths(std::slice::from_ref(&path)).is_ok());
    assert!(validate_conflict_paths(&[format!("{path}a")]).is_err());
    assert!(validate_conflict_paths(&vec!["a".into(); MAX_CONFLICT_PATHS + 1]).is_err());
    assert!(validate_conflict_paths(&vec![path; MAX_CONFLICT_PATHS]).is_err());
    for path in ["../escape", "/absolute", "a\0b", "a/../../b"] {
        assert!(validate_conflict_paths(&[path.into()]).is_err(), "{path}");
    }
    assert!(validate_prompt(&"p".repeat(MAX_PROMPT_BYTES)).is_ok());
    assert!(validate_prompt(&"p".repeat(MAX_PROMPT_BYTES + 1)).is_err());
    assert_eq!(MAX_MESSAGE_TITLE_BYTES, 120);
    assert_eq!(MAX_MESSAGE_SUMMARY_BYTES, 1024);
    assert_eq!(MAX_COMMIT_MESSAGE_BYTES, 2048);
    assert_eq!(GIT_OUTPUT_BYTES, 65536);
    assert_eq!(GIT_DEADLINE.as_secs(), 60);
    assert_eq!(HOST_DEADLINE.as_secs(), 90);
    assert_eq!(HELPER_LOOKUP_DEADLINE.as_secs(), 5);
}

#[test]
fn whole_conflict_list_counts_array_brackets_at_the_exact_prompt_bound() {
    let mut at = vec!["a".repeat(1024); 15];
    at.push("b".repeat(975));
    assert_eq!(serde_json::to_vec(&at).unwrap().len(), MAX_PROMPT_BYTES);
    validate_conflict_paths(&at).unwrap();
    at.last_mut().unwrap().push('b');
    assert_eq!(serde_json::to_vec(&at).unwrap().len(), MAX_PROMPT_BYTES + 1);
    assert!(validate_conflict_paths(&at).is_err());
}
