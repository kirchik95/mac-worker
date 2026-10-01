//! Keep these v1 assertions until the fleet DTO and host operation are removed.

use mac_worker::{
    job::{FleetReconcileRequest, JobId},
    transfer::HostOperation,
};
use proptest::prelude::*;

fn job_id(value: u128) -> JobId {
    format!("{value:032x}").parse().unwrap()
}

#[test]
fn fleet_request_is_bounded_and_rejects_duplicate_canonical_job_ids() {
    assert!(FleetReconcileRequest::new(vec![job_id(1), job_id(1)]).is_err());
    assert!(FleetReconcileRequest::new((1..=101).map(job_id).collect()).is_err());
    assert_eq!(
        HostOperation::Reconcile.command(),
        "~/.local/bin/worker host reconcile"
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn fleet_requests_accept_every_varying_unique_input_within_the_bound(
        values in prop::collection::btree_set(any::<u128>(), 0..=100)
    ) {
        let ids = values.into_iter().map(job_id).collect::<Vec<_>>();
        prop_assert!(FleetReconcileRequest::new(ids).is_ok());
    }
}
