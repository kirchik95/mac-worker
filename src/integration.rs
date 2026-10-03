//! Frozen, opt-in integration contracts. Entry-point wiring belongs to T6.
// Contract consumers land in later tracks; nothing here is advertised yet.
#![allow(dead_code)]

pub(crate) mod contracts;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod testing;
