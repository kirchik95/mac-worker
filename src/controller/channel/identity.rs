//! T4 facade. Authenticated raw bootstrap; optional journal hint is not authority.
pub use super::contracts::{
    ControllerAccount, IdentitySource, ServiceIdentity, SocketIdentity, SocketIdentityResult,
    verify_expected_service,
};
