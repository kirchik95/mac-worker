//! Stage 5.2 RED tests for admission cache reuse rules (unchanged runtime).

#![allow(dead_code)]
mod support;

use std::{
    fs,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use mac_worker::{
    client_state::ClientStateStore, job::AdmissionObservation, scheduler::CandidateSlot,
};

fn temp_state() -> (tempfile::TempDir, ClientStateStore) {
    let fixture = tempfile::tempdir().unwrap();
    let state = fixture.path().canonicalize().unwrap().join("state");
    let store = ClientStateStore::open(&state).unwrap();
    (fixture, store)
}

fn ready_observation(at: u64) -> AdmissionObservation {
    AdmissionObservation::new(
        "mini-1".into(),
        true,
        CandidateSlot::Idle,
        vec!["darwin-arm64".into()],
        Some(10),
        20,
        at,
    )
    .unwrap()
}

fn negative_observation(at: u64) -> AdmissionObservation {
    AdmissionObservation::new(
        "mini-1".into(),
        false,
        CandidateSlot::Busy,
        Vec::new(),
        None,
        0,
        at,
    )
    .unwrap()
}

/// Advisory `admission_observation` still clamps a future timestamp to age 0.
/// Strict submit/runner reuse is covered by turn_runner coverage, not this helper.
#[test]
fn advisory_admission_observation_still_clamps_future_timestamps() {
    let (_dir, store) = temp_state();
    store
        .publish_admission_observation(ready_observation(50_000))
        .unwrap();
    let mut refreshed = false;
    store
        .admission_observation("mini-1", 40_000, || {
            refreshed = true;
            Ok(ready_observation(40_000))
        })
        .unwrap();
    assert!(
        !refreshed,
        "legacy advisory cache keeps clamping future timestamps; strict reuse lives on the submit/runner path"
    );
}

/// Break caught: refresh publication omits the newer-timestamp guard, so a
/// slow ready refresh restores admission after a newer negative observation.
#[test]
fn observation_refresh_does_not_overwrite_a_newer_negative() {
    let (dir, store) = temp_state();
    let store = Arc::new(store);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let refreshing = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.admission_observation("mini-1", 1_000, || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(ready_observation(1_000))
            })
        })
    };
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("refresh started");
    store
        .publish_admission_observation(negative_observation(2_000))
        .unwrap();
    release_tx.send(()).unwrap();
    refreshing.join().unwrap().unwrap();

    let path = dir
        .path()
        .canonicalize()
        .unwrap()
        .join("state/observations/mini-1.json");
    let observation: AdmissionObservation =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert!(
        !observation.ready(),
        "slow ready refresh restored a row older than the published negative"
    );
    assert_eq!(observation.observed_at_millis(), 2_000);
}

fn idle_observation(at: u64) -> AdmissionObservation {
    AdmissionObservation::new(
        "mini-1".into(),
        true,
        CandidateSlot::Idle,
        vec!["darwin-arm64".into(), "agent:codex".into()],
        Some(10),
        20,
        at,
    )
    .unwrap()
}

/// Break caught: equal timestamps overwrite, so a same-ms Idle/capability
/// replacement during an in-flight refresh is lost to the stale ready vector.
#[test]
fn observation_refresh_does_not_restore_old_projection_at_the_same_millis() {
    let (dir, store) = temp_state();
    let store = Arc::new(store);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let refreshing = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.admission_observation("mini-1", 1_000, || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(idle_observation(1_000))
            })
        })
    };
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("refresh started");
    store
        .publish_admission_observation(
            AdmissionObservation::new(
                "mini-1".into(),
                true,
                CandidateSlot::Busy,
                vec!["darwin-arm64".into()],
                Some(10),
                20,
                1_000,
            )
            .unwrap(),
        )
        .unwrap();
    release_tx.send(()).unwrap();
    refreshing.join().unwrap().unwrap();

    let path = dir
        .path()
        .canonicalize()
        .unwrap()
        .join("state/observations/mini-1.json");
    let observation: AdmissionObservation =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(observation.slot(), CandidateSlot::Busy);
    assert_eq!(observation.capabilities(), &["darwin-arm64".to_string()]);
}
