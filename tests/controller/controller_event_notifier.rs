use mac_worker::{
    controller::events::{
        BaselineKind, ChangeCause, DerivedTaskChange, Reconciliation, SafeOutcome, TaskFacts,
    },
    task::{TaskId, TurnId},
};

#[test]
fn cold_repair_cannot_make_historical_notification_changes() {
    let id = TaskId::new(uuid::Uuid::from_u128(1));
    let turn = TurnId::new(uuid::Uuid::from_u128(2));
    let facts = TaskFacts::test_terminal(id, turn, SafeOutcome::Done, true);
    let mut cold = Reconciliation::test_cold(None, vec![facts.clone()]);
    cold.validate().unwrap();
    assert_eq!(cold.baseline, BaselineKind::Cold);
    assert!(cold.changes.is_empty());
    cold.changes.push(DerivedTaskChange {
        task_id: id,
        previous: None,
        current: Some(facts),
        cause: ChangeCause::RepairDifference,
    });
    assert!(cold.validate().is_err());
}
