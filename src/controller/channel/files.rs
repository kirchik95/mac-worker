//! T4 facade. Filesystem implementations run only on bounded native jobs.
//! Generation link withdrawal requires proven socket RPC child exit after
//! admission stops. Unknown proof retains the exact-bound link; detached task
//! groups have no link dependency and must never be awaited or cancelled for it.
pub use super::contracts::{
    EntryIdentity, ForwardPath, ForwardPaths, ServiceRecord, SocketBinding,
};
