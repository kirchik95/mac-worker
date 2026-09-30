use mac_worker::{
    controller::events::{EventSelector, ReadQuery, TaskAddressQuery, TaskRepairQuery},
    task::TaskId,
};

#[test]
fn all_reads_use_only_the_safe_task_list_selector() {
    let id = TaskId::new(uuid::Uuid::from_u128(1));
    for (selector, op) in [
        (EventSelector::Read(ReadQuery::default()), "read"),
        (
            EventSelector::Tasks(TaskAddressQuery::try_new(vec![id], false, None).unwrap()),
            "tasks",
        ),
        (EventSelector::Repair(TaskRepairQuery::default()), "repair"),
    ] {
        let body = selector.request_body().unwrap();
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert_eq!(body["controller_events"]["op"], op);
        assert_eq!(EventSelector::from_request_body(&body).unwrap(), selector);
    }
}
