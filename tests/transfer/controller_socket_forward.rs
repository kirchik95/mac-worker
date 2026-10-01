//! T1 dependency gate, not concrete OpenSSH/mux coverage (owned by T5).
use mac_worker::controller::channel::{
    ChannelFailure, ChannelReason,
    forward::{ForwardDisposition, ForwardOpenFailure},
};

#[test]
fn gate_unacknowledged_forward_failure_retains_allocation() {
    let failure =
        ForwardOpenFailure::retained(ChannelFailure::Unavailable(ChannelReason::Cancelled));
    assert_eq!(failure.disposition, ForwardDisposition::Retained);
}
