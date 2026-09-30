use mac_worker::controller::events::{JournalProvider, testing::FakeJournalProvider};
use std::time::Duration;

#[test]
fn absent_host_journal_is_optional_and_never_initialized() {
    let provider = FakeJournalProvider::absent();
    assert!(
        provider
            .open_existing(Duration::from_secs(60))
            .unwrap()
            .is_none()
    );
    assert_eq!(provider.open_count(), 1);
}
