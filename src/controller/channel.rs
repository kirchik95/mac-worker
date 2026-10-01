//! Frozen interfaces for the optional, foreground read-loop channel.
//!
//! No production channel is started, selected or advertised by this module.
//! Each implementation facade is owned by its later track; shared interfaces
//! and test doubles stay here so those tracks need no sibling implementations.

pub(crate) mod client;
pub(crate) mod codec;
pub(crate) mod contracts;
pub(crate) mod files;
pub(crate) mod forward;
pub(crate) mod identity;
pub(crate) mod image;
pub(crate) mod pin;
pub(crate) mod server;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testing;

pub(crate) use contracts::*;
