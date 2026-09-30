//! Test coordination watchdogs, independent of product timing budgets.
#![allow(dead_code)]

pub const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Move the last release sender into a scope callback. Unwinding disconnects
/// every pending/future receive before the scope joins its threads, including
/// hooks reached more than once. Successful paths keep their explicit sends.
pub struct ScopedSender<T>(pub std::sync::mpsc::Sender<T>);

impl<T> std::ops::Deref for ScopedSender<T> {
    type Target = std::sync::mpsc::Sender<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Declare inside a thread scope, before a fallible handshake: scope joins run
/// after these locals drop, so a panic must release every parked test hook.
pub fn on_drop<F: FnOnce()>(action: F) -> impl Drop {
    struct Guard<F: FnOnce()>(Option<F>);

    impl<F: FnOnce()> Drop for Guard<F> {
        fn drop(&mut self) {
            if let Some(action) = self.0.take() {
                action();
            }
        }
    }

    Guard(Some(action))
}
