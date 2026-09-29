#[path = "support/questions_v7_dtos.rs"]
mod v7;

use mac_worker::{prepared_submit::FrozenSubmitBody, task::QuestionsPolicy};
use serde_json::json;

fn legacy_submit() -> serde_json::Value {
    json!({
        "task_id":"00000000000000000000000000000001", "turn_id":"00000000000000000000000000000002",
        "created_at_millis":1, "prompt":"work", "agent":"codex", "source":"local", "publish":["fetch"],
        "close_on":"done", "wip":false, "project_id":"a".repeat(64), "worktree_id":"b".repeat(64),
        "base_oid":"c".repeat(40), "timeout_millis":1000, "max_followups":10,
        "permissions":"workspace", "requires":[], "include_untracked":[], "include_empty_dirs":[],
        "allow_sensitive":[], "cli_includes":[]
    })
}

#[test]
fn questions_submit_works_with_f59e56a_dtos_in_both_directions_by_default() {
    let old: v7::FrozenSubmitBody = serde_json::from_value(legacy_submit()).unwrap();
    let new: FrozenSubmitBody = serde_json::from_value(serde_json::to_value(old).unwrap()).unwrap();
    assert_eq!(new.questions, None);
    assert_eq!(new.prepared().unwrap().questions, None);
    let wire = serde_json::to_value(new).unwrap();
    assert!(wire.get("questions").is_none());
    let _: v7::FrozenSubmitBody = serde_json::from_value(wire).unwrap();
}

#[test]
fn questions_explicit_submit_override_is_opt_in_for_strict_old_controllers() {
    let mut body: FrozenSubmitBody = serde_json::from_value(legacy_submit()).unwrap();
    for policy in [QuestionsPolicy::Ask, QuestionsPolicy::Decide] {
        body.questions = Some(policy);
        assert_eq!(body.prepared().unwrap().questions, Some(policy));
        assert!(
            serde_json::from_value::<v7::FrozenSubmitBody>(serde_json::to_value(&body).unwrap())
                .is_err()
        );
    }
}

#[test]
fn questions_dag_wire_works_with_f59e56a_in_both_directions_by_default() {
    let mut wire = legacy_submit();
    let object = wire.as_object_mut().unwrap();
    for key in ["task_id", "turn_id", "created_at_millis", "base_oid"] {
        object.remove(key);
    }
    object.insert("project_path".into(), json!("/fixture"));
    let old: v7::DagFrozenSpec = serde_json::from_value(wire).unwrap();
    let new: mac_worker::dag::DagFrozenSpec =
        serde_json::from_value(serde_json::to_value(old).unwrap()).unwrap();
    assert_eq!(new.questions, None);
    let _: v7::DagFrozenSpec = serde_json::from_value(serde_json::to_value(new).unwrap()).unwrap();
}

#[test]
fn questions_status_roundtrips_through_strict_f59e56a_dto() {
    use mac_worker::controller::ControllerTaskStatusResult;
    let wire = json!({
        "task_id":"00000000000000000000000000000001", "run_id":null,
        "status": {"state":"queued", "last_outcome":null, "worker":null,
            "session_present":false, "head_oid":null, "summary":null, "questions":[],
            "files_changed":[], "diff_stat":null, "turns":[], "updated_at_millis":1},
        "warnings":[], "events":[], "runner":null, "exit_code":null
    });
    let old: v7::ControllerTaskStatusResult = serde_json::from_value(wire.clone()).unwrap();
    let parsed: ControllerTaskStatusResult =
        serde_json::from_value(serde_json::to_value(old).unwrap()).unwrap();
    assert_eq!(
        parsed.into_report().questions_policy(),
        QuestionsPolicy::Ask
    );
    let mut wire = wire;
    wire["events"] = json!([{"type":"questions_policy", "policy":"decide"}]);
    let report = serde_json::from_value::<ControllerTaskStatusResult>(wire)
        .unwrap()
        .into_report();
    let new = serde_json::to_value(ControllerTaskStatusResult::from_report(&report)).unwrap();
    let old: v7::ControllerTaskStatusResult = serde_json::from_value(new).unwrap();
    let report =
        serde_json::from_value::<ControllerTaskStatusResult>(serde_json::to_value(old).unwrap())
            .unwrap()
            .into_report();
    assert_eq!(report.questions_policy(), QuestionsPolicy::Decide);
}
