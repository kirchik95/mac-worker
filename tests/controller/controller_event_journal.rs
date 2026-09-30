use mac_worker::controller::events::{
    EventBatch, EventReadResult, JournalReader, JournalWriter, NewEvent, ReadQuery, Seq,
    testing::MemoryJournal,
};
use std::time::Duration;

#[test]
fn committed_batch_exposes_last_delivered_cursor() {
    let journal = MemoryJournal::new();
    let deadline = Duration::from_secs(60);
    let before = journal.window(deadline).unwrap().cursor();
    let head = journal
        .append(
            EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }; 2])
                .unwrap(),
            deadline,
        )
        .unwrap();
    let EventReadResult::Batch(batch) = journal
        .read(
            ReadQuery {
                after: Some(before),
                limit: 1,
                wait_ms: 0,
            },
            deadline,
        )
        .unwrap()
    else {
        panic!("expected committed batch")
    };
    assert_eq!(head.seq, Seq::new(2));
    assert_eq!(batch.next_after.seq, Seq::new(1));
    assert!(batch.has_more);
    batch.validate().unwrap();
}
