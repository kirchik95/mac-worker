use mac_worker::{
    error::{ExitKind, WorkerError},
    job::{
        CommandSpec, CommandSummary, JobId, JobMeta, JobState, JobStatus, JsonEvent,
        LeaseAcquireRequest, LeaseAcquireResponse, LeaseRecord, LocalJobRecord, LogChunk,
        LogStream, ProcessIdentity, RequestFingerprint, RequestFingerprintMaterial, StatusResponse,
        SubmitRequest, SubmitResponse,
    },
    protocol::PROTOCOL_VERSION,
};

const JOB_ID: &str = "018f0f4a6b5c7d8e9f00112233445566";
const CLIENT_ID: &str = "102f0f4a6b5c7d8e9f00112233445566";
const LEASE_TOKEN: &str = "202f0f4a6b5c7d8e9f00112233445566";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn material(command: CommandSpec) -> RequestFingerprintMaterial {
    RequestFingerprintMaterial::new(
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
        command,
    )
    .unwrap()
}

fn meta_json() -> serde_json::Value {
    serde_json::json!({
        "protocol_version": PROTOCOL_VERSION,
        "job_id": JOB_ID,
        "client_id": CLIENT_ID,
        "worker_name": "mini-1",
        "project_id": PROJECT_ID,
        "worktree_id": WORKTREE_ID,
        "manifest_digest": MANIFEST_DIGEST,
        "request_fingerprint": material(CommandSpec::shell("true".into()).unwrap()).fingerprint(),
        "command_summary": { "mode": "shell" },
        "relative_working_dir": "packages/app",
        "timeout_millis": 30_000,
        "resource_class": "heavy",
        "created_at_millis": 100,
    })
}

fn lease_json() -> serde_json::Value {
    serde_json::json!({
        "job_id": JOB_ID,
        "client_id": CLIENT_ID,
        "lease_token": LEASE_TOKEN,
        "request_fingerprint": material(CommandSpec::shell("true".into()).unwrap()).fingerprint(),
        "worker_name": "mini-1",
        "project_id": PROJECT_ID,
        "worktree_id": WORKTREE_ID,
        "manifest_digest": MANIFEST_DIGEST,
        "timeout_millis": 30_000,
        "resource_class": "heavy",
        "command_summary": { "mode": "shell" },
        "created_at_millis": 100,
        "expires_at_millis": 30_100,
    })
}

#[test]
fn job_ids_are_canonical_lowercase_uuid_components() {
    let id: JobId = JOB_ID.parse().unwrap();
    assert_eq!(id.to_string(), JOB_ID);
    for invalid in [
        "",
        "../job",
        "018F0F4A6B5C7D8E9F00112233445566",
        "018f0f4a-6b5c-7d8e-9f00-112233445566",
    ] {
        assert!(invalid.parse::<JobId>().is_err(), "{invalid:?}");
    }
}

#[test]
fn request_fingerprints_are_canonical_lowercase_sha256_components() {
    let digest = "0c582832035740179241a519e91943fea5e8e58341646026f82897e3a0946a78";
    assert_eq!(
        digest.parse::<RequestFingerprint>().unwrap().to_string(),
        digest
    );
    assert!(
        "0C582832035740179241A519E91943FEA5E8E58341646026F82897E3A0946A78"
            .parse::<RequestFingerprint>()
            .is_err()
    );
}

#[test]
fn command_specs_are_bounded_and_mutually_exclusive() {
    assert!(CommandSpec::argv(vec!["npm".into(), "test".into()]).is_ok());
    assert!(CommandSpec::argv(Vec::new()).is_err());
    assert!(CommandSpec::shell("npm test".into()).is_ok());
    assert!(CommandSpec::shell(String::new()).is_err());
}

#[test]
fn command_spec_preserves_non_nul_utf8_and_repeated_arguments() {
    let command = CommandSpec::argv(vec!["npm\ttest".into(), "npm\ttest".into()]).unwrap();
    let json = serde_json::to_string(&command).unwrap();
    assert_eq!(json, r#"{"mode":"argv","argv":["npm\ttest","npm\ttest"]}"#);
    assert_eq!(serde_json::from_str::<CommandSpec>(&json).unwrap(), command);

    let shell = CommandSpec::shell("echo first\necho second".into()).unwrap();
    assert_eq!(
        serde_json::from_str::<CommandSpec>(&serde_json::to_string(&shell).unwrap()).unwrap(),
        shell
    );
}

#[test]
fn command_spec_preserves_empty_and_repeated_arguments() {
    let command = CommandSpec::argv(vec!["".into(), "repeat".into(), "repeat".into()]).unwrap();
    assert_eq!(
        serde_json::to_string(&command).unwrap(),
        r#"{"mode":"argv","argv":["","repeat","repeat"]}"#
    );
    assert_eq!(
        serde_json::from_str::<CommandSpec>(r#"{"mode":"argv","argv":["","repeat","repeat"]}"#)
            .unwrap(),
        command
    );
}

#[test]
fn serialization_rejects_directly_constructed_invalid_public_protocol_variants() {
    assert!(serde_json::to_string(&CommandSpec::Argv { argv: Vec::new() }).is_err());
    assert!(
        serde_json::to_string(&CommandSpec::Shell {
            shell: String::new(),
        })
        .is_err()
    );
    assert!(
        serde_json::to_string(&JsonEvent::Error {
            protocol_version: 1,
            code: "CAPACITY_BUSY".into(),
            message: "worker is busy".into(),
        })
        .is_err()
    );
    assert!(
        serde_json::to_string(&JsonEvent::Error {
            protocol_version: PROTOCOL_VERSION,
            code: "CAPACITY_BUSY\0".into(),
            message: "worker is busy".into(),
        })
        .is_err()
    );
}

#[test]
fn public_protocol_variants_keep_valid_round_trip_behavior() {
    let command = CommandSpec::Argv {
        argv: vec!["".into(), "repeat".into(), "repeat".into()],
    };
    let command_json = serde_json::to_string(&command).unwrap();
    assert_eq!(
        serde_json::from_str::<CommandSpec>(&command_json).unwrap(),
        command
    );

    let event = JsonEvent::Error {
        protocol_version: PROTOCOL_VERSION,
        code: "CAPACITY_BUSY".into(),
        message: "worker is busy".into(),
    };
    let event_json = serde_json::to_string(&event).unwrap();
    assert_eq!(
        serde_json::from_str::<JsonEvent>(&event_json).unwrap(),
        event
    );
}

#[test]
fn command_summary_returns_a_typed_error_for_directly_invalid_commands() {
    let invalid = CommandSpec::Argv { argv: Vec::new() };
    assert!(invalid.summary().is_err());

    let valid = CommandSpec::Argv {
        argv: vec!["tool".into()],
    };
    assert_eq!(
        serde_json::to_string(&valid.summary().unwrap()).unwrap(),
        r#"{"mode":"argv","arg_count":1}"#
    );
}

#[test]
fn serde_rejects_semantically_invalid_persistent_and_wire_dtos() {
    assert!(serde_json::from_str::<CommandSummary>(r#"{"mode":"argv","arg_count":0}"#).is_err());

    let mut protocol_one_meta = meta_json();
    protocol_one_meta["protocol_version"] = serde_json::json!(1);
    assert!(serde_json::from_value::<JobMeta>(protocol_one_meta.clone()).is_err());
    assert!(
        serde_json::from_value::<LocalJobRecord>(serde_json::json!({
            "meta": protocol_one_meta,
            "lease_token": LEASE_TOKEN,
            "last_status": null,
            "cleanup_pending": false,
        }))
        .is_err()
    );
    assert!(serde_json::from_value::<StatusResponse>(serde_json::json!({
        "meta": protocol_one_meta,
        "status": { "state": "accepted", "updated_at_millis": 100, "supervisor_pid": null, "supervisor_start_identity": null, "child_pid": null, "child_start_identity": null, "exit_code": null, "final_stdout_bytes": null, "final_stderr_bytes": null, "error_code": null },
    })).is_err());

    let mut expired_lease = lease_json();
    expired_lease["expires_at_millis"] = serde_json::json!(99);
    assert!(serde_json::from_value::<LeaseRecord>(expired_lease.clone()).is_err());
    assert!(
        serde_json::from_value::<LeaseAcquireResponse>(serde_json::json!({
            "outcome": "acquired",
            "lease": expired_lease,
        }))
        .is_err()
    );

    let material = material(CommandSpec::shell("true".into()).unwrap());
    let mut request = serde_json::to_value(LeaseAcquireRequest::new(material.clone())).unwrap();
    request["request_fingerprint"] =
        serde_json::json!("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
    assert!(serde_json::from_value::<LeaseAcquireRequest>(request.clone()).is_err());
    assert!(serde_json::from_value::<SubmitRequest>(request).is_err());

    let mut bad_submit_meta = meta_json();
    bad_submit_meta["protocol_version"] = serde_json::json!(1);
    assert!(serde_json::from_value::<SubmitResponse>(serde_json::json!({
        "outcome": "accepted",
        "meta": bad_submit_meta,
        "status": { "state": "accepted", "updated_at_millis": 100, "supervisor_pid": null, "supervisor_start_identity": null, "child_pid": null, "child_start_identity": null, "exit_code": null, "final_stdout_bytes": null, "final_stderr_bytes": null, "error_code": null },
    })).is_err());
}

#[test]
fn command_spec_rejects_invalid_json_and_bounds() {
    assert!(
        serde_json::from_str::<CommandSpec>(r#"{"mode":"argv","argv":["ok\u0000no"]}"#).is_err()
    );
    assert!(
        serde_json::from_str::<CommandSpec>(r#"{"mode":"shell","shell":"x","extra":true}"#)
            .is_err()
    );
    assert!(
        serde_json::from_str::<CommandSpec>(r#"{"mode":"shell","shell":"x","shell":"y"}"#).is_err()
    );
    assert!(CommandSpec::argv(vec!["x".repeat(16 * 1024 + 1)]).is_err());
    assert!(CommandSpec::argv(vec!["x".to_owned(); 257]).is_err());
}

#[test]
fn lifecycle_allows_only_documented_forward_transitions() {
    assert!(JobState::Accepted.can_transition_to(JobState::Running));
    assert!(JobState::Running.can_transition_to(JobState::Succeeded));
    assert!(!JobState::Succeeded.can_transition_to(JobState::Running));
    assert!(!JobState::Verified.can_transition_to(JobState::Succeeded));
}

#[test]
fn fingerprint_uses_the_canonical_material_in_fixed_field_order() {
    let material = material(CommandSpec::argv(vec!["npm".into(), "test".into()]).unwrap());
    assert_eq!(
        serde_json::to_string(&material).unwrap(),
        format!(
            r#"{{"protocol_version":2,"job_id":"{JOB_ID}","client_id":"{CLIENT_ID}","lease_token":"{LEASE_TOKEN}","worker_name":"mini-1","project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","manifest_digest":"{MANIFEST_DIGEST}","relative_working_dir":"packages/app","timeout_millis":30000,"resource_class":"heavy","command":{{"mode":"argv","argv":["npm","test"]}}}}"#
        )
    );
    assert_eq!(
        material.fingerprint().to_string(),
        "0c582832035740179241a519e91943fea5e8e58341646026f82897e3a0946a78"
    );
}

#[test]
fn material_rejects_noncanonical_identifiers_and_duplicate_json_fields() {
    assert!(
        RequestFingerprintMaterial::new(
            JOB_ID.parse().unwrap(),
            CLIENT_ID.parse().unwrap(),
            LEASE_TOKEN.parse().unwrap(),
            "mini-1".into(),
            "A".repeat(64),
            WORKTREE_ID.into(),
            MANIFEST_DIGEST.into(),
            "".into(),
            1,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .is_err()
    );
    assert!(serde_json::from_str::<RequestFingerprintMaterial>(&format!(
        r#"{{"protocol_version":2,"protocol_version":2,"job_id":"{JOB_ID}","client_id":"{CLIENT_ID}","lease_token":"{LEASE_TOKEN}","worker_name":"mini-1","project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","manifest_digest":"{MANIFEST_DIGEST}","relative_working_dir":"","timeout_millis":1,"resource_class":"heavy","command":{{"mode":"shell","shell":"true"}}}}"#
    )).is_err());
}

#[test]
fn status_requires_process_identities_for_running_terminal_lengths_and_monotonic_time() {
    let accepted = JobStatus::accepted(100).unwrap();
    let supervised = accepted
        .with_supervisor(ProcessIdentity::new(10, 11).unwrap(), 100)
        .unwrap();
    let ready = supervised
        .with_child(ProcessIdentity::new(20, 21).unwrap(), 101)
        .unwrap();
    let running = ready.into_running(101).unwrap();
    assert!(
        accepted
            .transition(JobStatus::running(101, 10, 11, 20, 21).unwrap())
            .is_err()
    );
    assert!(JobStatus::running(101, 10, 0, 20, 21).is_err());
    assert!(
        running
            .transition(running.clone().into_succeeded(102, 9, 10).unwrap())
            .is_ok()
    );
    assert!(running.clone().into_succeeded(99, 9, 10).is_err());
    assert!(
        JobStatus::succeeded(102, 9, 10)
            .unwrap()
            .transition(JobStatus::failed(103, 1, 9, 10).unwrap())
            .is_err()
    );
}

#[test]
fn log_chunks_are_bounded_base64_records_with_exact_offsets() {
    let chunk = LogChunk::new(LogStream::Stdout, 4, b"\x00\xffA".to_vec()).unwrap();
    assert_eq!(
        serde_json::to_string(&chunk).unwrap(),
        r#"{"stream":"stdout","offset":4,"next_offset":7,"data":"AP9B"}"#
    );
    assert_eq!(chunk.decoded_bytes().unwrap(), b"\x00\xffA");
    assert!(LogChunk::new(LogStream::Stderr, 0, vec![0; 64 * 1024 + 1]).is_err());
    assert!(
        serde_json::from_str::<LogChunk>(
            r#"{"stream":"stdout","offset":1,"next_offset":3,"data":"AA=="}"#
        )
        .is_err()
    );
}

#[test]
fn streaming_events_are_versioned_strict_ndjson_records() {
    let event = JsonEvent::Error {
        protocol_version: PROTOCOL_VERSION,
        code: "CAPACITY_BUSY".into(),
        message: "worker is busy".into(),
    };
    assert_eq!(
        serde_json::to_string(&event).unwrap(),
        r#"{"event":"error","protocol_version":2,"code":"CAPACITY_BUSY","message":"worker is busy"}"#
    );
    assert!(serde_json::from_str::<JsonEvent>(
        r#"{"event":"error","protocol_version":1,"code":"CAPACITY_BUSY","message":"worker is busy"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<JsonEvent>(
        r#"{"event":"error","protocol_version":2,"code":"A","code":"B","message":"worker is busy"}"#
    )
    .is_err());
}

#[test]
fn protocol_and_exit_kinds_keep_their_wire_contracts() {
    assert_eq!(PROTOCOL_VERSION, 2);
    assert_eq!(
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            message: "busy".into()
        }
        .exit_kind(),
        ExitKind::Capacity
    );
    assert_eq!(
        WorkerError::Transport {
            code: "SSH_UNAVAILABLE",
            message: "offline".into()
        }
        .exit_kind(),
        ExitKind::Unavailable
    );
    assert_eq!(WorkerError::CommandExit { code: 7 }.exit_code(), 7);
}
