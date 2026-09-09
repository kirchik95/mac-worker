use mac_worker::{
    dashboard::model::{
        ApiError, ArtifactStatus, CollectionSummary, DASHBOARD_API_VERSION, DashboardCommandMode,
        DashboardCommandSummary, DashboardError, DashboardJob, DashboardJobState,
        DashboardLogChunk, DashboardMemoryPressure, DashboardQueueEntry, DashboardQueueEntryKind,
        DashboardSlotState, DashboardSnapshot, DashboardWorker, Freshness, SlotSummary,
        SystemSummary, WorkerHealth, project_label_or_fallback,
    },
    job::{JobId, LogStream},
    task_view::TaskListProjection,
};

const JOB_ID: &str = "0123456789abcdef0123456789abcdef";

#[test]
fn snapshot_v1_keeps_empty_queue_and_never_serializes_private_job_fields() {
    let snapshot = fixture_snapshot();
    let value = serde_json::to_value(snapshot).unwrap();

    assert_eq!(value["api_version"], 1);
    assert_eq!(value["queue"], serde_json::json!([]));
    assert_eq!(value["recent_jobs"][0]["job_id"], serde_json::json!(JOB_ID));
    assert!(value["workers"][0]["system"]["cpu_busy_percent"].is_null());
    assert!(value["recent_jobs"][0]["artifact_status"].is_null());
    assert_absent_object_keys(
        &value,
        &[
            "ssh",
            "lease_token",
            "command",
            "argv",
            "shell",
            "path",
            "environment",
        ],
    );
}

#[test]
fn snapshot_v1_serializes_the_complete_projection_contract() {
    let snapshot = complete_fixture_snapshot();
    let value = serde_json::to_value(snapshot).unwrap();

    assert_eq!(
        value,
        serde_json::json!({
            "api_version": 1,
            "revision": 42,
            "generated_at_millis": 1_725_000_000_100_u64,
            "collection": {
                "freshness": "stale",
                "errors": [{
                    "code": "WORKER_UNAVAILABLE",
                    "message": "worker observation timed out",
                }],
            },
            "project_defaults": null,
            "workers": [
                {
                    "name": "mini-unobserved",
                    "health": "unavailable",
                    "freshness": "offline",
                    "observed_at_millis": null,
                    "hostname": null,
                    "agent_facts": null,
                    "herdr": null,
                    "slot": {
                        "state": "idle",
                        "capacity": 1,
                        "active_job_id": null,
                    },
                    "capabilities": [],
                    "missing_capabilities": ["swift"],
                    "system": {
                        "free_disk_bytes": null,
                        "total_disk_bytes": null,
                        "memory_pressure": null,
                        "swap_used_bytes": null,
                        "cpu_busy_percent": null,
                    },
                    "error": null,
                    "active_task": null,
                },
                {
                    "name": "mini-observed",
                    "health": "ready",
                    "freshness": "current",
                    "observed_at_millis": 1_725_000_000_099_u64,
                    "hostname": "mini-observed.local",
                    "agent_facts": null,
                    "herdr": null,
                    "slot": {
                        "state": "busy",
                        "capacity": 1,
                        "active_job_id": "fedcba9876543210fedcba9876543210",
                    },
                    "capabilities": ["swift", "xcode"],
                    "missing_capabilities": [],
                    "system": {
                        "free_disk_bytes": 300,
                        "total_disk_bytes": 500,
                        "memory_pressure": "warn",
                        "swap_used_bytes": 12,
                        "cpu_busy_percent": 37.5,
                    },
                    "error": {
                        "code": "PROBE_PARTIAL",
                        "message": "swap measurement unavailable",
                    },
                    "active_task": null,
                },
            ],
            "tasks": [],
            "runs": [],
            "progress": {
                "total": 0,
                "queued": 0,
                "active": 0,
                "open": 0,
                "closed": 0,
                "failed_like": 0,
            },
            "queue": [{
                "position": 1,
                "job_id": "11111111111111111111111111111111",
                "entry_kind": "batch",
                "task_id": null,
                "turn_id": null,
                "run_id": null,
                "run_max_parallel": null,
                "pinned_worker": null,
                "project_id": "queue-project-id",
                "worktree_id": "queue-worktree-id",
                "project_label": "Queued Project",
                "command_summary": {"mode": "shell", "arg_count": null},
                "created_at_millis": 1_725_000_000_010_u64,
                "requirements": ["swift", "xcode"],
                "blocking_code": "NO_COMPATIBLE_IDLE_WORKER",
            }],
            "active_jobs": [{
                "job_id": "fedcba9876543210fedcba9876543210",
                "worker_name": "mini-observed",
                "project_id": "active-project-id",
                "worktree_id": "active-worktree-id",
                "project_label": "Active Project",
                "manifest_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "command_summary": {"mode": "argv", "arg_count": 3},
                "resource_class": "heavy",
                "created_at_millis": 1_725_000_000_020_u64,
                "updated_at_millis": 1_725_000_000_030_u64,
                "state": "running",
                "exit_code": null,
                "terminating_signal": null,
                "final_stdout_bytes": null,
                "final_stderr_bytes": null,
                "artifact_status": "pending",
                "remote_uncertainty": "STATUS_QUERY_TIMEOUT",
            }],
            "recent_jobs": [{
                "job_id": "22222222222222222222222222222222",
                "worker_name": "mini-observed",
                "project_id": "recent-project-id",
                "worktree_id": "recent-worktree-id",
                "project_label": null,
                "manifest_digest": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "command_summary": {"mode": "shell", "arg_count": null},
                "resource_class": "heavy",
                "created_at_millis": 1_725_000_000_040_u64,
                "updated_at_millis": 1_725_000_000_050_u64,
                "state": "lost",
                "exit_code": null,
                "terminating_signal": null,
                "final_stdout_bytes": null,
                "final_stderr_bytes": null,
                "artifact_status": null,
                "remote_uncertainty": null,
            }],
        })
    );
    assert_absent_object_keys(
        &value,
        &[
            "ssh",
            "lease_token",
            "command",
            "argv",
            "shell",
            "path",
            "environment",
        ],
    );
}

#[test]
fn snapshot_rejects_non_v1_api_versions_at_the_wire_boundary() {
    for invalid_version in [0, 2] {
        let mut snapshot = fixture_snapshot();
        snapshot.api_version = invalid_version;

        let error = serde_json::to_value(snapshot).unwrap_err();
        assert_eq!(
            error.to_string(),
            "dashboard snapshot API version must be 1",
            "version {invalid_version} must not be silently rewritten"
        );
    }
}

#[test]
fn every_public_dashboard_enum_uses_its_required_snake_case_spelling() {
    assert_enum_spelling(Freshness::Current, "current");
    assert_enum_spelling(Freshness::Stale, "stale");
    assert_enum_spelling(Freshness::Offline, "offline");
    assert_enum_spelling(WorkerHealth::Ready, "ready");
    assert_enum_spelling(WorkerHealth::Unavailable, "unavailable");
    assert_enum_spelling(DashboardJobState::Uploading, "uploading");
    assert_enum_spelling(DashboardJobState::Verified, "verified");
    assert_enum_spelling(DashboardJobState::Accepted, "accepted");
    assert_enum_spelling(DashboardJobState::Running, "running");
    assert_enum_spelling(DashboardJobState::Succeeded, "succeeded");
    assert_enum_spelling(DashboardJobState::Failed, "failed");
    assert_enum_spelling(DashboardJobState::Cancelled, "cancelled");
    assert_enum_spelling(DashboardJobState::TimedOut, "timed_out");
    assert_enum_spelling(DashboardJobState::Lost, "lost");
    assert_enum_spelling(DashboardSlotState::Idle, "idle");
    assert_enum_spelling(DashboardSlotState::Busy, "busy");
    assert_enum_spelling(DashboardMemoryPressure::Normal, "normal");
    assert_enum_spelling(DashboardMemoryPressure::Warn, "warn");
    assert_enum_spelling(DashboardMemoryPressure::Critical, "critical");
    assert_enum_spelling(DashboardMemoryPressure::Unknown, "unknown");
    assert_enum_spelling(DashboardCommandMode::Argv, "argv");
    assert_enum_spelling(DashboardCommandMode::Shell, "shell");
    assert_enum_spelling(ArtifactStatus::Pending, "pending");
    assert_enum_spelling(ArtifactStatus::Available, "available");
    assert_enum_spelling(ArtifactStatus::Failed, "failed");
}

#[test]
fn project_label_uses_short_identifiers_without_paths_when_missing() {
    let job = fixture_job(None);

    assert_eq!(
        project_label_or_fallback(&job),
        "project-0123456789ab/worktree-fedcba987654"
    );
}

#[test]
fn supplied_project_label_is_bounded_and_escapes_control_characters() {
    let job = fixture_job(Some(format!("visible\n{}", "x".repeat(100))));

    assert_eq!(
        project_label_or_fallback(&job),
        "visible\\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx…"
    );
    assert_eq!(
        serde_json::to_value(job).unwrap()["project_label"],
        "visible\\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx…"
    );
}

#[test]
fn log_chunk_serializes_base64_and_byte_exact_offsets() {
    let chunk = DashboardLogChunk::from_bytes(LogStream::Stderr, 41, b"\0\xffA".to_vec()).unwrap();

    assert_eq!(
        serde_json::to_string(&chunk).unwrap(),
        r#"{"stream":"stderr","offset":41,"next_offset":44,"data":"AP9B"}"#
    );
}

#[test]
fn api_error_uses_the_global_error_envelope() {
    let error = ApiError::new("DASHBOARD_UNAVAILABLE", "worker could not be observed");

    assert_eq!(
        serde_json::to_string(&error).unwrap(),
        r#"{"error":{"code":"DASHBOARD_UNAVAILABLE","message":"worker could not be observed"}}"#
    );
}

#[test]
fn public_dashboard_error_codes_are_stable_and_bounded() {
    for invalid in [
        "",
        "lower_case",
        "DASHBOARD ERROR",
        "A\nB",
        &"A".repeat(129),
    ] {
        let dashboard = DashboardError::new(invalid, "safe message");
        let api = ApiError::new(invalid, "safe message");

        assert_eq!(dashboard.code, "DASHBOARD_ERROR", "{invalid:?}");
        assert_eq!(
            serde_json::to_value(api).unwrap()["error"]["code"],
            "DASHBOARD_ERROR",
            "{invalid:?}"
        );
    }
}

#[test]
fn system_summary_serializes_none_and_finite_cpu_percentages_without_changing_shape() {
    for (cpu_busy_percent, expected) in [
        (None, serde_json::Value::Null),
        (Some(0.0), serde_json::json!(0.0)),
        (Some(37.5), serde_json::json!(37.5)),
        (Some(100.0), serde_json::json!(100.0)),
    ] {
        let value = serde_json::to_value(SystemSummary {
            free_disk_bytes: Some(100),
            total_disk_bytes: Some(200),
            memory_pressure: Some(DashboardMemoryPressure::Normal),
            swap_used_bytes: Some(0),
            cpu_busy_percent,
        })
        .unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "free_disk_bytes": 100,
                "total_disk_bytes": 200,
                "memory_pressure": "normal",
                "swap_used_bytes": 0,
                "cpu_busy_percent": expected,
            })
        );
    }
}

#[test]
fn system_summary_rejects_every_invalid_cpu_percentage_at_serialization() {
    for invalid in [f64::NEG_INFINITY, -0.1, 100.1, f64::INFINITY, f64::NAN] {
        let error = serde_json::to_value(SystemSummary {
            free_disk_bytes: None,
            total_disk_bytes: None,
            memory_pressure: None,
            swap_used_bytes: None,
            cpu_busy_percent: Some(invalid),
        })
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "dashboard CPU busy percent must be finite and between 0 and 100"
        );
    }
}

fn fixture_snapshot() -> DashboardSnapshot {
    DashboardSnapshot {
        api_version: DASHBOARD_API_VERSION,
        revision: 17,
        generated_at_millis: 1_725_000_000_000,
        collection: CollectionSummary {
            freshness: Freshness::Current,
            errors: Vec::new(),
        },
        project_defaults: None,
        task_view: TaskListProjection::empty(),
        workers: vec![DashboardWorker {
            name: "mini-a".into(),
            health: WorkerHealth::Ready,
            freshness: Freshness::Current,
            observed_at_millis: Some(1_725_000_000_000),
            hostname: Some("mini-a.local".into()),
            agent_facts: None,
            herdr: None,
            slot: SlotSummary {
                state: DashboardSlotState::Idle,
                capacity: 1,
                active_job_id: None,
            },
            capabilities: vec!["swift".into()],
            missing_capabilities: Vec::new(),
            system: SystemSummary {
                free_disk_bytes: Some(100),
                total_disk_bytes: Some(200),
                memory_pressure: Some(DashboardMemoryPressure::Normal),
                swap_used_bytes: Some(0),
                cpu_busy_percent: None,
            },
            error: None,
            active_task: None,
        }],
        queue: Vec::new(),
        active_jobs: Vec::new(),
        recent_jobs: vec![fixture_job(None)],
    }
}

fn complete_fixture_snapshot() -> DashboardSnapshot {
    DashboardSnapshot {
        api_version: DASHBOARD_API_VERSION,
        revision: 42,
        generated_at_millis: 1_725_000_000_100,
        collection: CollectionSummary {
            freshness: Freshness::Stale,
            errors: vec![DashboardError::new(
                "WORKER_UNAVAILABLE",
                "worker observation timed out",
            )],
        },
        project_defaults: None,
        task_view: TaskListProjection::empty(),
        workers: vec![
            DashboardWorker {
                name: "mini-unobserved".into(),
                health: WorkerHealth::Unavailable,
                freshness: Freshness::Offline,
                observed_at_millis: None,
                hostname: None,
                agent_facts: None,
                herdr: None,
                slot: SlotSummary {
                    state: DashboardSlotState::Idle,
                    capacity: 1,
                    active_job_id: None,
                },
                capabilities: Vec::new(),
                missing_capabilities: vec!["swift".into()],
                system: SystemSummary {
                    free_disk_bytes: None,
                    total_disk_bytes: None,
                    memory_pressure: None,
                    swap_used_bytes: None,
                    cpu_busy_percent: None,
                },
                error: None,
                active_task: None,
            },
            DashboardWorker {
                name: "mini-observed".into(),
                health: WorkerHealth::Ready,
                freshness: Freshness::Current,
                observed_at_millis: Some(1_725_000_000_099),
                hostname: Some("mini-observed.local".into()),
                agent_facts: None,
                herdr: None,
                slot: SlotSummary {
                    state: DashboardSlotState::Busy,
                    capacity: 1,
                    active_job_id: Some("fedcba9876543210fedcba9876543210".parse().unwrap()),
                },
                capabilities: vec!["swift".into(), "xcode".into()],
                missing_capabilities: Vec::new(),
                system: SystemSummary {
                    free_disk_bytes: Some(300),
                    total_disk_bytes: Some(500),
                    memory_pressure: Some(DashboardMemoryPressure::Warn),
                    swap_used_bytes: Some(12),
                    cpu_busy_percent: Some(37.5),
                },
                error: Some(DashboardError::new(
                    "PROBE_PARTIAL",
                    "swap measurement unavailable",
                )),
                active_task: None,
            },
        ],
        queue: vec![DashboardQueueEntry {
            position: 1,
            job_id: "11111111111111111111111111111111".parse().unwrap(),
            entry_kind: DashboardQueueEntryKind::Batch,
            task_id: None,
            turn_id: None,
            run_id: None,
            run_max_parallel: None,
            pinned_worker: None,
            project_id: "queue-project-id".into(),
            worktree_id: "queue-worktree-id".into(),
            project_label: Some("Queued Project".into()),
            command_summary: DashboardCommandSummary {
                mode: DashboardCommandMode::Shell,
                arg_count: None,
            },
            created_at_millis: 1_725_000_000_010,
            requirements: vec!["swift".into(), "xcode".into()],
            blocking_code: "NO_COMPATIBLE_IDLE_WORKER".into(),
        }],
        active_jobs: vec![DashboardJob {
            job_id: "fedcba9876543210fedcba9876543210".parse().unwrap(),
            worker_name: "mini-observed".into(),
            project_id: "active-project-id".into(),
            worktree_id: "active-worktree-id".into(),
            project_label: Some("Active Project".into()),
            manifest_digest: "b".repeat(64),
            command_summary: DashboardCommandSummary {
                mode: DashboardCommandMode::Argv,
                arg_count: Some(3),
            },
            resource_class: "heavy".into(),
            created_at_millis: 1_725_000_000_020,
            updated_at_millis: 1_725_000_000_030,
            state: DashboardJobState::Running,
            exit_code: None,
            terminating_signal: None,
            final_stdout_bytes: None,
            final_stderr_bytes: None,
            artifact_status: Some(ArtifactStatus::Pending),
            remote_uncertainty: Some("STATUS_QUERY_TIMEOUT".into()),
        }],
        recent_jobs: vec![DashboardJob {
            job_id: "22222222222222222222222222222222".parse().unwrap(),
            worker_name: "mini-observed".into(),
            project_id: "recent-project-id".into(),
            worktree_id: "recent-worktree-id".into(),
            project_label: None,
            manifest_digest: "c".repeat(64),
            command_summary: DashboardCommandSummary {
                mode: DashboardCommandMode::Shell,
                arg_count: None,
            },
            resource_class: "heavy".into(),
            created_at_millis: 1_725_000_000_040,
            updated_at_millis: 1_725_000_000_050,
            state: DashboardJobState::Lost,
            exit_code: None,
            terminating_signal: None,
            final_stdout_bytes: None,
            final_stderr_bytes: None,
            artifact_status: None,
            remote_uncertainty: None,
        }],
    }
}

fn fixture_job(project_label: Option<String>) -> DashboardJob {
    DashboardJob {
        job_id: JOB_ID.parse::<JobId>().unwrap(),
        worker_name: "mini-a".into(),
        project_id: "0123456789abcdef".into(),
        worktree_id: "fedcba9876543210".into(),
        project_label,
        manifest_digest: "a".repeat(64),
        command_summary: DashboardCommandSummary {
            mode: DashboardCommandMode::Argv,
            arg_count: Some(3),
        },
        resource_class: "heavy".into(),
        created_at_millis: 1_725_000_000_000,
        updated_at_millis: 1_725_000_000_001,
        state: DashboardJobState::Succeeded,
        exit_code: Some(0),
        terminating_signal: None,
        final_stdout_bytes: Some(3),
        final_stderr_bytes: Some(0),
        artifact_status: None,
        remote_uncertainty: None,
    }
}

fn assert_absent_object_keys(value: &serde_json::Value, forbidden: &[&str]) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                assert!(
                    !forbidden.contains(&key.as_str()),
                    "leaked object key {key}"
                );
                assert_absent_object_keys(child, forbidden);
            }
        }
        serde_json::Value::Array(values) => values
            .iter()
            .for_each(|child| assert_absent_object_keys(child, forbidden)),
        _ => {}
    }
}

fn assert_enum_spelling(value: impl serde::Serialize, expected: &str) {
    assert_eq!(
        serde_json::to_value(value).unwrap(),
        serde_json::json!(expected)
    );
}
