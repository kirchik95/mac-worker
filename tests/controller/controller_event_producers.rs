use mac_worker::controller::events::{
    EventBatch, EventSink, NewEvent, PublishAttempt, testing::RecordingSink,
};

#[test]
fn producer_sink_drops_a_whole_batch() {
    let sink = RecordingSink::new();
    sink.set_drop_mode(true);
    let batch =
        EventBatch::try_new(vec![NewEvent::ControllerDrainChanged { drained: true }; 2]).unwrap();
    assert_eq!(sink.try_publish(batch.clone()), PublishAttempt::Dropped);
    assert!(sink.batches().is_empty());
    sink.set_drop_mode(false);
    assert_eq!(sink.try_publish(batch), PublishAttempt::Queued);
    assert_eq!(sink.batches()[0].len(), 2);
}
