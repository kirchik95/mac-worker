use mac_worker::test_support::integration::*;

#[test]
fn compact_confirmation_requires_the_complete_same_revision_identity() {
    let record = sample_record(fixture_task(), fixture_source(), "main");
    let annotation = record.snapshot.annotation().unwrap();
    assert!(annotation.confirms(&record.snapshot));
    let mut newer = record.snapshot.clone();
    newer.revision = newer.revision.next().unwrap();
    assert!(!annotation.confirms(&newer));
    let mut other_epoch = record.snapshot.clone();
    other_epoch.epoch += 1;
    assert!(!annotation.confirms(&other_epoch));
    let mut other_target = sample_record(fixture_task(), fixture_source(), "release").snapshot;
    other_target.revision = annotation.revision;
    assert!(!annotation.confirms(&other_target));
}
