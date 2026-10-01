//! Stable private pin independent of the existing notify cache key. Blocking native work.
pub use super::contracts::{Pin, PinStore, RouteDigest};
use super::contracts::{ChannelFailure, ChannelReason, SocketIdentity};
use crate::{job::ClientId, paths::PathLayout};
mod core;

#[derive(Default)]
pub struct PrivatePinStore;
impl PrivatePinStore { pub fn new() -> Self { Self } }
impl PinStore for PrivatePinStore {
    fn verify_or_create(&self, paths: &PathLayout, identity: &SocketIdentity) -> Result<(),ChannelFailure> {
        identity.validate()?;
        core::verify_or_create(paths,&Pin::from_identity(identity)).map_err(pin_failure)
    }
    /// The caller must supply a fresh authenticated raw-stdio identity.
    fn repin(&self, paths: &PathLayout, identity: &SocketIdentity, expected: ClientId) -> Result<(),ChannelFailure> {
        identity.validate()?;
        core::repin(paths,&Pin::from_identity(identity),expected).map_err(pin_failure)
    }
}
fn pin_failure(error: std::io::Error) -> ChannelFailure {
    ChannelFailure::Unavailable(if error.raw_os_error()==Some(libc::ESTALE) {ChannelReason::PinMismatch} else {ChannelReason::UnsafePath})
}
