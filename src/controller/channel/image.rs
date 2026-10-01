//! Independently verified loaded image and installed runner path. Blocking native work.
pub use super::contracts::{PinnedExecutable, RunningImage, RunningImageSource};
use super::contracts::{ChannelFailure, ChannelReason};
mod core;

#[derive(Default)]
pub struct SystemRunningImageSource;
impl SystemRunningImageSource {
    pub fn new() -> Self { Self }
}
impl RunningImageSource for SystemRunningImageSource {
    fn capture(&self) -> Result<RunningImage, ChannelFailure> {
        core::capture_current().map_err(|_| ChannelFailure::Unavailable(ChannelReason::Unsupported))
    }
}
