//! Frozen interfaces for the optional, foreground read-loop channel.
//!
//! No production channel is started, selected or advertised by this module.
//! Each implementation facade is owned by its later track; shared interfaces
//! and test doubles stay here so those tracks need no sibling implementations.

pub mod client;
pub mod codec;
pub mod contracts;
pub mod files;
pub mod forward;
pub mod identity;
pub mod image;
pub mod pin;
pub mod server;
#[doc(hidden)]
pub mod testing;

pub use contracts::*;
