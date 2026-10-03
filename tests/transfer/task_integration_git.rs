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
