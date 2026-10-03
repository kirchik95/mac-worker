//! Frozen, opt-in integration contracts. Entry-point wiring belongs to T6.
// Contract consumers land in later tracks; nothing here is advertised yet.
#![allow(dead_code)]

pub(crate) mod config;
pub(crate) mod contracts;
pub(crate) mod coordinator;
pub(crate) mod git;
pub(crate) mod host;
pub(crate) mod host_store;
pub(crate) mod remote;
pub(crate) mod runner;
pub(crate) mod store;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testing;
pub(crate) mod view;
