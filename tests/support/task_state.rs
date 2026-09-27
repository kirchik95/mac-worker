/// Explicit full-record replacement for test setup. Production callers must
/// mutate a fresh record or supply their own expected snapshot.
pub trait TaskStateFixture {
    fn replace_task_fixture(
        &self,
        replacement: mac_worker::task::LocalTaskRecord,
    ) -> Result<(), mac_worker::error::WorkerError>;
}

impl TaskStateFixture for mac_worker::client_state::ClientStateStore {
    fn replace_task_fixture(
        &self,
        replacement: mac_worker::task::LocalTaskRecord,
    ) -> Result<(), mac_worker::error::WorkerError> {
        let current = self.load_task(replacement.meta().task_id())?;
        if self.update_task_if_current(&current, replacement)? {
            Ok(())
        } else {
            Err(mac_worker::error::WorkerError::task(
                "TASK_STALE",
                "task fixture changed concurrently",
            ))
        }
    }
}
