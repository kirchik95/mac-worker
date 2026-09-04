use mac_worker::scheduler::{
    AffinityHints, CandidateObservation, CandidateObservationError, CandidateRejection,
    CandidateSlot, SchedulerPolicy, Selection, WorkerPreference,
};
use proptest::prelude::*;

fn observation(
    name: &str,
    ready: bool,
    slot: CandidateSlot,
    capabilities: &[&str],
    memory: Option<u64>,
    disk: u64,
) -> CandidateObservation {
    CandidateObservation::new(
        name.into(),
        ready,
        slot,
        capabilities
            .iter()
            .map(|capability| (*capability).into())
            .collect(),
        memory,
        disk,
    )
    .unwrap()
}

fn names(ranked: &[mac_worker::scheduler::RankedCandidate]) -> Vec<&str> {
    ranked
        .iter()
        .map(|candidate| candidate.worker_name())
        .collect()
}

fn observations() -> Vec<CandidateObservation> {
    vec![
        observation(
            "mini-1",
            true,
            CandidateSlot::Idle,
            &["node"],
            Some(32),
            500,
        ),
        observation(
            "mini-2",
            true,
            CandidateSlot::Idle,
            &["node"],
            Some(16),
            250,
        ),
        observation(
            "mini-3",
            true,
            CandidateSlot::Idle,
            &["node"],
            Some(64),
            1_000,
        ),
    ]
}

fn mixed_observations() -> Vec<CandidateObservation> {
    vec![
        observation("busy", true, CandidateSlot::Busy, &["ruby"], Some(8), 100),
        observation(
            "lacks-ruby",
            true,
            CandidateSlot::Idle,
            &["node"],
            Some(8),
            100,
        ),
        observation(
            "offline",
            false,
            CandidateSlot::Idle,
            &["ruby"],
            Some(8),
            100,
        ),
        observation("twin", true, CandidateSlot::Idle, &["ruby"], Some(8), 100),
        observation("twin", true, CandidateSlot::Idle, &["ruby"], Some(8), 100),
    ]
}

#[test]
fn ranks_worktree_affinity_before_more_memory_and_disk() {
    let ranked = SchedulerPolicy::rank(
        &observations(),
        &["node".into()],
        &AffinityHints {
            worktree_worker: Some("mini-2".into()),
            project_worker: Some("mini-1".into()),
        },
    );

    assert_eq!(names(&ranked), vec!["mini-2", "mini-1", "mini-3"]);
}

#[test]
fn ranks_known_memory_before_missing_memory_then_disk_and_name() {
    let ranked = SchedulerPolicy::rank(
        &[
            observation("alpha", true, CandidateSlot::Idle, &[], None, 10_000),
            observation("bravo", true, CandidateSlot::Idle, &[], Some(8), 100),
            observation("charlie", true, CandidateSlot::Idle, &[], Some(16), 1),
            observation("delta", true, CandidateSlot::Idle, &[], Some(8), 500),
            observation("echo", true, CandidateSlot::Idle, &[], Some(8), 500),
        ],
        &[],
        &AffinityHints::none(),
    );

    assert_eq!(
        names(&ranked),
        vec!["charlie", "delta", "echo", "bravo", "alpha"]
    );
}

#[test]
fn rank_omits_unavailable_occupied_duplicate_and_incompatible_observations() {
    let ranked = SchedulerPolicy::rank(
        &mixed_observations(),
        &["ruby".into()],
        &AffinityHints::none(),
    );

    assert!(ranked.is_empty());
}

#[test]
fn select_reports_rejections_in_lexical_worker_order() {
    let selection = SchedulerPolicy::select(
        &mixed_observations(),
        &["ruby".into()],
        &WorkerPreference::Automatic,
        &AffinityHints::none(),
    );

    assert!(matches!(
        selection,
        Selection::NoEligible { rejections }
            if rejections == vec![
                CandidateRejection::Busy { name: "busy".into() },
                CandidateRejection::MissingCapabilities {
                    name: "lacks-ruby".into(),
                    missing: vec!["ruby".into()],
                },
                CandidateRejection::Unavailable { name: "offline".into() },
                CandidateRejection::DuplicateIdentity { name: "twin".into() },
                CandidateRejection::DuplicateIdentity { name: "twin".into() },
            ]
    ));
}

#[test]
fn pin_filters_candidates_before_selection_and_absent_pin_is_empty_no_eligible() {
    let candidates = observations();
    let selected = SchedulerPolicy::select(
        &candidates,
        &["node".into()],
        &WorkerPreference::Pinned {
            worker: "mini-1".into(),
        },
        &AffinityHints {
            worktree_worker: Some("mini-2".into()),
            project_worker: None,
        },
    );
    assert!(
        matches!(selected, Selection::Selected(candidate) if candidate.worker_name() == "mini-1")
    );

    let absent_pin = SchedulerPolicy::select(
        &candidates,
        &[],
        &WorkerPreference::Pinned {
            worker: "not-configured".into(),
        },
        &AffinityHints::none(),
    );
    assert!(matches!(absent_pin, Selection::NoEligible { rejections } if rejections.is_empty()));
}

#[test]
fn observation_constructor_rejects_invalid_names_and_duplicate_capabilities() {
    assert_eq!(
        CandidateObservation::new(" ".into(), true, CandidateSlot::Idle, vec![], None, 0),
        Err(CandidateObservationError::InvalidWorkerName)
    );
    assert_eq!(
        CandidateObservation::new(
            "mini-1".into(),
            true,
            CandidateSlot::Idle,
            vec!["node".into(), "node".into()],
            None,
            0,
        ),
        Err(CandidateObservationError::DuplicateCapability {
            capability: "node".into(),
        })
    );
}

fn arbitrary_health_sets() -> impl Strategy<Value = Vec<CandidateObservation>> {
    prop::collection::vec(
        (
            "worker-[a-z]{1,3}",
            any::<bool>(),
            any::<bool>(),
            prop::collection::btree_set("[a-z]{1,4}", 0..4),
            proptest::option::of(any::<u16>()),
            any::<u16>(),
        ),
        0..20,
    )
    .prop_map(|items| {
        items
            .into_iter()
            .map(|(name, ready, busy, capabilities, memory, disk)| {
                observation(
                    &name,
                    ready,
                    if busy {
                        CandidateSlot::Busy
                    } else {
                        CandidateSlot::Idle
                    },
                    &capabilities.iter().map(String::as_str).collect::<Vec<_>>(),
                    memory.map(u64::from),
                    u64::from(disk),
                )
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn ranking_is_deterministic_and_unique(input in arbitrary_health_sets()) {
        let a = SchedulerPolicy::rank(&input, &[], &AffinityHints::none());
        let b = SchedulerPolicy::rank(&input, &[], &AffinityHints::none());
        prop_assert_eq!(&a, &b);
        let names = names(&a);
        let unique = names.iter().collect::<std::collections::BTreeSet<_>>();
        prop_assert_eq!(names.len(), unique.len());
    }

    #[test]
    fn ranking_is_invariant_under_arbitrary_permutations(
        pair in arbitrary_health_sets().prop_flat_map(|input| {
            let original = input.clone();
            prop::collection::vec(any::<u64>(), input.len()).prop_map(move |keys| {
                let mut keyed = input.clone().into_iter().zip(keys).collect::<Vec<_>>();
                keyed.sort_by_key(|(_, key)| *key);
                (original.clone(), keyed.into_iter().map(|(value, _)| value).collect::<Vec<_>>())
            })
        })
    ) {
        let (input, permuted) = pair;
        let forward = SchedulerPolicy::rank(&input, &[], &AffinityHints::none());
        let reversed = SchedulerPolicy::rank(&permuted, &[], &AffinityHints::none());
        prop_assert_eq!(forward, reversed);
    }
}
