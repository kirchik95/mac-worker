use std::{
    ffi::OsString,
    fs,
    os::unix::{
        ffi::OsStringExt,
        fs::{FileTypeExt, PermissionsExt, symlink},
    },
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
};

use mac_worker::{
    client_state::{ClientStateCreationRacePoint, ClientStateStore, ClientStateWritePoint},
    job::{
        ClientId, CommandSpec, JobId, JobMeta, JobStatus, LeaseToken, LocalJobRecord,
        RequestFingerprintMaterial,
    },
};

const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const DIGEST_A: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const DIGEST_B: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const COMMAND_SECRET: &str = "secret-exact-command-value";

fn mode(path: impl AsRef<Path>) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

fn temp_root(directory: &tempfile::TempDir) -> PathBuf {
    directory.path().canonicalize().unwrap()
}

fn make_record(
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    digest: &str,
    last_status: Option<JobStatus>,
    cleanup_pending: bool,
) -> LocalJobRecord {
    let material = RequestFingerprintMaterial::new(
        job_id,
        client_id,
        lease_token,
        "mini-1".into(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        digest.into(),
        "packages/app".into(),
        30_000,
        "heavy".into(),
        CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
    )
    .unwrap();
    let fingerprint = material.fingerprint();
    let meta = JobMeta::new(&material, fingerprint, 1_000).unwrap();
    LocalJobRecord::new(meta, lease_token, last_status, cleanup_pending).unwrap()
}

fn fresh_record(store: &ClientStateStore) -> LocalJobRecord {
    make_record(
        JobId::generate(),
        store.client_id(),
        LeaseToken::generate(),
        DIGEST_A,
        None,
        false,
    )
}

#[test]
fn first_open_publishes_one_stable_owner_only_client_identity() {
    // Catches writing the final client-id in place: a concurrent/crashed first
    // open could otherwise expose an empty or partial identity.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state/mac-worker");
    let threads = 32;
    let barrier = Arc::new(Barrier::new(threads));
    let handles = (0..threads)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let state = state.clone();
            thread::spawn(move || {
                barrier.wait();
                ClientStateStore::open(&state).unwrap().client_id()
            })
        })
        .collect::<Vec<_>>();
    let ids = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(mode(&state), 0o700);
    assert_eq!(mode(state.join("jobs")), 0o700);
    assert_eq!(mode(state.join(".mac-worker-state")), 0o700);
    assert_eq!(mode(state.join("client-id")), 0o600);
    assert_eq!(
        fs::read(state.join("client-id")).unwrap(),
        format!("{}\n", ids[0]).as_bytes()
    );
    assert_eq!(ClientStateStore::open(&state).unwrap().client_id(), ids[0]);
}

#[test]
fn create_load_and_list_round_trip_canonical_sanitized_job_json() {
    // Catches persisting the exact command or emitting non-canonical/truncated
    // state that a later invocation cannot validate independently.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let later = fresh_record(&store);
    let earlier = make_record(
        "00000000000000000000000000000001".parse().unwrap(),
        store.client_id(),
        LeaseToken::generate(),
        DIGEST_A,
        None,
        false,
    );

    store.create_job(later.clone()).unwrap();
    store.create_job(earlier.clone()).unwrap();

    assert_eq!(store.load_job(later.meta().job_id()).unwrap(), later);
    assert_eq!(
        store.list_jobs().unwrap(),
        vec![earlier.clone(), later.clone()]
    );
    let persisted = fs::read(
        state
            .join("jobs")
            .join(format!("{}.json", later.meta().job_id())),
    )
    .unwrap();
    assert_eq!(persisted.last(), Some(&b'\n'));
    assert_eq!(persisted.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert!(!String::from_utf8_lossy(&persisted).contains(COMMAND_SECRET));
    assert!(
        String::from_utf8_lossy(&persisted)
            .contains(r#""command_summary":{"mode":"argv","arg_count":2}"#)
    );
    assert_eq!(
        mode(
            state
                .join("jobs")
                .join(format!("{}.json", later.meta().job_id()))
        ),
        0o600
    );
    assert!(
        fs::read_dir(state.join(".mac-worker-state"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn repeated_listing_and_listing_after_create_reopen_the_directory_stream() {
    // Catches iterating through a dup of the anchored jobs descriptor: dup
    // shares the directory offset, so later status calls would see no jobs.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let first = fresh_record(&store);
    store.create_job(first.clone()).unwrap();

    assert_eq!(store.list_jobs().unwrap(), vec![first.clone()]);
    assert_eq!(store.list_jobs().unwrap(), vec![first.clone()]);

    let second = fresh_record(&store);
    store.create_job(second.clone()).unwrap();
    let mut expected = vec![first, second];
    expected.sort_by_key(|record| record.meta().job_id().to_string());
    assert_eq!(store.list_jobs().unwrap(), expected);
}

#[test]
fn sixty_four_identical_creators_are_idempotent_and_never_clobber() {
    // Catches check-then-create races and treating a retry of the exact same
    // durable identity as a conflict.
    let fixture = tempfile::tempdir().unwrap();
    let store = Arc::new(ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap());
    let record = fresh_record(&store);
    let barrier = Arc::new(Barrier::new(64));
    let handles = (0..64)
        .map(|_| {
            let store = Arc::clone(&store);
            let record = record.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                store.create_job(record)
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 64);
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
}

#[test]
fn a_different_immutable_record_for_an_existing_job_id_is_a_conflict() {
    // Catches idempotency comparing only the job ID and silently accepting a
    // different digest/fingerprint under the existing identity.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let original = fresh_record(&store);
    let changed = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_B,
        None,
        false,
    );
    store.create_job(original.clone()).unwrap();

    let error = store.create_job(changed).unwrap_err();

    assert!(error.to_string().contains("JOB_ID_CONFLICT"));
    assert!(
        !error
            .to_string()
            .contains(&original.lease_token().to_string())
    );
    assert_eq!(store.load_job(original.meta().job_id()).unwrap(), original);
}

#[test]
fn updates_replace_only_mutable_observations_atomically() {
    // Catches updates rewriting immutable recovery credentials or accepting a
    // backwards observation timestamp.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let accepted = JobStatus::accepted(2_000).unwrap();
    let observed = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(accepted.clone()),
        true,
    );

    store.update_job(observed.clone()).unwrap();
    assert_eq!(store.load_job(original.meta().job_id()).unwrap(), observed);

    let backwards = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(JobStatus::accepted(1_999).unwrap()),
        false,
    );
    assert!(store.update_job(backwards).is_err());

    let terminal = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(JobStatus::succeeded(3_000, 10, 20).unwrap()),
        false,
    );
    store.update_job(terminal.clone()).unwrap();
    assert_eq!(store.load_job(original.meta().job_id()).unwrap(), terminal);

    let terminal_regression = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(JobStatus::running(4_000, 10, 100, 11, 101).unwrap()),
        false,
    );
    assert!(store.update_job(terminal_regression).is_err());

    let changed_digest = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_B,
        Some(accepted),
        false,
    );
    let error = store.update_job(changed_digest).unwrap_err();
    assert!(error.to_string().contains("JOB_ID_CONFLICT"));
}

#[test]
fn corrupt_truncated_and_noncanonical_job_json_fail_closed() {
    // Catches serde's permissive trailing-whitespace behavior masking a torn
    // or manually altered persistent record.
    for bytes in [
        b"{\"meta\":".as_slice(),
        b"{}\n".as_slice(),
        b" {}\n".as_slice(),
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("state");
        let store = ClientStateStore::open(&state).unwrap();
        let job_id = JobId::generate();
        fs::write(state.join("jobs").join(format!("{job_id}.json")), bytes).unwrap();
        fs::set_permissions(
            state.join("jobs").join(format!("{job_id}.json")),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();

        assert!(store.load_job(job_id).is_err());
        assert!(store.list_jobs().is_err());
    }
}

#[test]
fn symlinked_roots_components_and_final_files_are_rejected() {
    // Catches path-based traversal following a planted link outside the local
    // state boundary.
    let fixture = tempfile::tempdir().unwrap();
    let physical = temp_root(&fixture);
    let outside = physical.join("outside");
    fs::create_dir(&outside).unwrap();
    let linked_root = physical.join("linked-state");
    symlink(&outside, &linked_root).unwrap();
    assert!(ClientStateStore::open(&linked_root).is_err());

    let component_state = physical.join("component-state");
    fs::create_dir(&component_state).unwrap();
    fs::set_permissions(&component_state, fs::Permissions::from_mode(0o700)).unwrap();
    symlink(&outside, component_state.join("jobs")).unwrap();
    assert!(ClientStateStore::open(&component_state).is_err());

    let identity_state = physical.join("identity-state");
    fs::create_dir(&identity_state).unwrap();
    fs::set_permissions(&identity_state, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        outside.join("identity"),
        b"00000000000000000000000000000001\n",
    )
    .unwrap();
    symlink(outside.join("identity"), identity_state.join("client-id")).unwrap();
    assert!(ClientStateStore::open(&identity_state).is_err());

    let final_state = physical.join("final-state");
    let store = ClientStateStore::open(&final_state).unwrap();
    let job_id = JobId::generate();
    fs::write(outside.join("job"), b"{}\n").unwrap();
    symlink(
        outside.join("job"),
        final_state.join("jobs").join(format!("{job_id}.json")),
    )
    .unwrap();
    assert!(store.load_job(job_id).is_err());
}

#[test]
fn a_directory_substituted_for_a_job_file_is_rejected() {
    // Catches treating an attacker-controlled directory as an absent record
    // and publishing state beneath or over it.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    fs::create_dir(
        state
            .join("jobs")
            .join(format!("{}.json", record.meta().job_id())),
    )
    .unwrap();

    assert!(store.load_job(record.meta().job_id()).is_err());
    assert!(store.create_job(record).is_err());
}

#[test]
fn non_utf8_unexpected_job_entries_fail_listing_without_removal() {
    // Catches lossy filename conversion making an unaddressable file invisible
    // to status while mutation continues around it.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let bad_name = OsString::from_vec(b"bad-\xff.json".to_vec());
    let bad_path = state.join("jobs").join(&bad_name);
    if let Err(error) = fs::write(&bad_path, b"{}\n") {
        // APFS rejects non-UTF-8 directory entry creation at the kernel
        // boundary. On filesystems that can represent it, the assertion below
        // exercises the store's independent fail-closed check.
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        return;
    }

    assert!(store.list_jobs().is_err());
    assert!(bad_path.exists());
}

#[test]
fn injected_crash_boundaries_expose_absent_or_complete_json_never_partial() {
    // Catches publication before the staged bytes are complete/fsynced and
    // replacement paths that truncate the final record in place.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let before = fresh_record(&store);
    store.inject_write_failure_once(ClientStateWritePoint::BeforePublish);
    assert!(store.create_job(before.clone()).is_err());
    assert!(store.load_job(before.meta().job_id()).is_err());

    let after = fresh_record(&store);
    store.inject_write_failure_once(ClientStateWritePoint::AfterPublish);
    assert!(store.create_job(after.clone()).is_err());
    assert_eq!(
        ClientStateStore::open(&state)
            .unwrap()
            .load_job(after.meta().job_id())
            .unwrap(),
        after
    );
}

#[test]
fn injected_update_rename_boundaries_preserve_one_complete_observation() {
    // Catches truncating the live record before replacement and treating a
    // post-rename acknowledgement failure as proof that the old bytes remain.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let accepted = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(JobStatus::accepted(2_000).unwrap()),
        true,
    );

    store.inject_write_failure_once(ClientStateWritePoint::BeforePublish);
    assert!(store.update_job(accepted.clone()).is_err());
    assert_eq!(store.load_job(original.meta().job_id()).unwrap(), original);

    store.inject_write_failure_once(ClientStateWritePoint::AfterPublish);
    assert!(store.update_job(accepted.clone()).is_err());
    assert_eq!(
        ClientStateStore::open(&state)
            .unwrap()
            .load_job(accepted.meta().job_id())
            .unwrap(),
        accepted
    );
}

#[test]
fn swapped_staged_payload_is_never_published_as_client_or_job_state() {
    // Catches dropping the staged payload descriptor and later trusting only
    // the mutable `payload` pathname for publication.
    let fixture = tempfile::tempdir().unwrap();
    let physical = temp_root(&fixture);
    let identity_state = physical.join("identity-state");
    let error = ClientStateStore::open_with_write_fault(
        &identity_state,
        ClientStateWritePoint::SwapOperationPayloadBeforePublish,
    )
    .err()
    .expect("swapped client identity payload must fail closed");
    assert!(
        !error
            .to_string()
            .contains("11111111111111111111111111111111")
    );
    assert!(identity_state.join("client-id").exists());
    assert!(operation_tree_contains(
        &identity_state.join(".mac-worker-state"),
        b"11111111111111111111111111111111\n"
    ));

    let job_state = physical.join("job-state");
    let store = ClientStateStore::open(&job_state).unwrap();
    let record = fresh_record(&store);
    store.inject_write_failure_once(ClientStateWritePoint::SwapOperationPayloadBeforePublish);
    assert!(store.create_job(record.clone()).is_err());
    assert!(
        job_state
            .join("jobs")
            .join(format!("{}.json", record.meta().job_id()))
            .exists()
    );
    assert!(store.load_job(record.meta().job_id()).is_err());
    assert!(operation_tree_contains(
        &job_state.join(".mac-worker-state"),
        b"11111111111111111111111111111111\n"
    ));
}

#[test]
fn substituted_operation_directory_is_preserved_during_cleanup() {
    // Catches cleanup checking an operation directory and then unlinking a
    // same-name replacement planted after the check.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.inject_write_failure_once(ClientStateWritePoint::SwapOperationDirectoryBeforeCleanup);

    assert!(store.create_job(record.clone()).is_err());
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
    let operation_root = state.join(".mac-worker-state");
    let sentinel_count = fs::read_dir(operation_root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().join("substitution-sentinel").is_file())
        .count();
    assert_eq!(
        sentinel_count, 1,
        "replacement operation directory was deleted"
    );
}

#[test]
fn post_validation_operation_directory_substitution_is_never_removed() {
    // Catches validating an acquired operation directory and then rmdir'ing a
    // same-name replacement at the final pathname.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.inject_write_failure_once(
        ClientStateWritePoint::SwapOperationDirectoryAfterValidationBeforeRemoval,
    );

    assert!(store.create_job(record.clone()).is_err());
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
    assert!(tree_contains_named_entry(
        &state.join(".mac-worker-state"),
        "post-validation-directory-sentinel"
    ));
}

#[test]
fn post_validation_payload_substitution_is_never_unlinked() {
    // Catches opening/stat'ing an owned payload and then unlinking a replacement
    // through the same mutable child name.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.inject_write_failure_once(
        ClientStateWritePoint::SwapOperationChildAfterValidationBeforeRemoval,
    );

    assert!(store.create_job(record.clone()).is_err());
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
    assert!(operation_tree_contains(
        &state.join(".mac-worker-state"),
        b"post-validation-child-substitution\n"
    ));
}

#[test]
fn post_validation_published_rollback_substitution_is_preserved() {
    // Catches validating a quarantined bad publication and then unlinking a
    // replacement planted at the quarantine name.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.inject_write_failure_once(
        ClientStateWritePoint::SwapPublishedRollbackAfterValidationBeforeRemoval,
    );

    assert!(store.create_job(record.clone()).is_err());
    assert!(
        !state
            .join("jobs")
            .join(format!("{}.json", record.meta().job_id()))
            .exists()
    );
    assert!(operation_tree_contains(
        &state,
        b"post-validation-published-substitution\n"
    ));
}

#[test]
fn live_job_swap_between_validation_and_replace_is_rolled_back() {
    // Catches an inode check followed by unconditional renameat, which would
    // silently overwrite a same-name replacement in the TOCTOU window.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let replacement = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(JobStatus::accepted(2_000).unwrap()),
        false,
    );
    store.inject_write_failure_once(ClientStateWritePoint::SwapLiveJobBeforeReplace);

    assert!(store.update_job(replacement.clone()).is_err());
    let final_path = state
        .join("jobs")
        .join(format!("{}.json", original.meta().job_id()));
    assert_eq!(
        fs::read(final_path).unwrap(),
        b"injected-live-replacement\n"
    );
    let mut original_bytes = serde_json::to_vec(&original).unwrap();
    original_bytes.push(b'\n');
    assert!(fs::read_dir(state.join("jobs")).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        path.is_file() && fs::read(path).is_ok_and(|bytes| bytes == original_bytes)
    }));
}

#[test]
fn every_unreadable_exchange_displacement_is_restored_and_fsynced() {
    // Catches `?` exits after atomic exchange that leave staged JSON live when
    // the displaced entry cannot be opened as an owner-only regular file.
    for point in [
        ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace,
        ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace,
        ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace,
        ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("state");
        let store = ClientStateStore::open(&state).unwrap();
        let original = fresh_record(&store);
        store.create_job(original.clone()).unwrap();
        let replacement = make_record(
            original.meta().job_id(),
            store.client_id(),
            original.lease_token(),
            DIGEST_A,
            Some(JobStatus::accepted(2_000).unwrap()),
            false,
        );
        let jobs_syncs = store.durability_sync_counts().jobs;
        store.inject_write_failure_once(point);

        assert!(store.update_job(replacement).is_err(), "{point:?}");
        assert!(
            store.durability_sync_counts().jobs > jobs_syncs,
            "{point:?}"
        );
        let final_path = state
            .join("jobs")
            .join(format!("{}.json", original.meta().job_id()));
        let metadata = fs::symlink_metadata(&final_path).unwrap();
        match point {
            ClientStateWritePoint::SwapLiveJobWithSymlinkBeforeReplace => {
                assert!(metadata.file_type().is_symlink());
                assert_eq!(
                    fs::read_link(&final_path).unwrap(),
                    PathBuf::from("swap-target")
                );
            }
            ClientStateWritePoint::SwapLiveJobWithDirectoryBeforeReplace => {
                assert!(metadata.is_dir());
                assert!(final_path.join("sentinel").is_file());
            }
            ClientStateWritePoint::SwapLiveJobWithFifoBeforeReplace => {
                assert!(metadata.file_type().is_fifo());
            }
            ClientStateWritePoint::SwapLiveJobWithPermissiveFileBeforeReplace => {
                assert!(metadata.is_file());
                assert_eq!(mode(&final_path), 0o644);
                assert_eq!(
                    fs::read(final_path).unwrap(),
                    b"permissive-live-substitution\n"
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn successful_creation_recovery_and_idempotency_cross_durability_barriers() {
    // Catches returning success while newly created parent entries, an
    // observed client identity, or an idempotent job entry remain unsynced.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("nested/state");
    let store = ClientStateStore::open(&state).unwrap();
    let opened = store.durability_sync_counts();
    assert!(opened.parent_directories >= 2, "{opened:?}");
    assert!(opened.root >= 4, "{opened:?}");

    let record = fresh_record(&store);
    let before_create = store.durability_sync_counts().jobs;
    store.create_job(record.clone()).unwrap();
    let after_create = store.durability_sync_counts().jobs;
    assert!(after_create > before_create);
    store.create_job(record).unwrap();
    assert!(store.durability_sync_counts().jobs > after_create);

    let reopened = ClientStateStore::open(&state).unwrap();
    assert!(reopened.durability_sync_counts().root >= 1);
}

#[test]
fn concurrent_creation_losers_fsync_every_parent_they_observe() {
    // Catches ENOENT followed by EEXIST being treated like an already-existing
    // entry even though the winning creator may crash before its own fsync.
    for point in [
        ClientStateCreationRacePoint::RootComponent,
        ClientStateCreationRacePoint::OwnedDirectory,
        ClientStateCreationRacePoint::LockFile,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("nested/state");
        let store = ClientStateStore::open_with_creation_race(&state, point).unwrap();
        let counts = store.durability_sync_counts();
        assert!(
            counts.concurrent_loser_parents >= 1,
            "{point:?}: {counts:?}"
        );
        assert!(
            counts.parent_directories + counts.root >= 1,
            "{point:?}: {counts:?}"
        );
    }
}

#[test]
fn permissive_state_directories_and_files_are_rejected_not_repaired() {
    // Catches silently inheriting group/world-readable recovery credentials or
    // chmodding a pre-existing object that the store did not create.
    let fixture = tempfile::tempdir().unwrap();
    let physical = temp_root(&fixture);
    let permissive_root = physical.join("permissive-root");
    fs::create_dir(&permissive_root).unwrap();
    fs::set_permissions(&permissive_root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(ClientStateStore::open(&permissive_root).is_err());
    assert_eq!(mode(&permissive_root), 0o755);

    let state = physical.join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.create_job(record.clone()).unwrap();
    let job_path = state
        .join("jobs")
        .join(format!("{}.json", record.meta().job_id()));
    fs::set_permissions(&job_path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(store.load_job(record.meta().job_id()).is_err());
    assert_eq!(mode(&job_path), 0o644);

    fs::set_permissions(state.join("client-id"), fs::Permissions::from_mode(0o644)).unwrap();
    assert!(ClientStateStore::open(&state).is_err());
    assert_eq!(mode(state.join("client-id")), 0o644);
}

#[test]
fn copied_records_from_another_client_are_rejected_by_load_and_list() {
    // Catches treating a valid canonical record copied from a different local
    // client as this MacBook's recovery credential.
    let fixture = tempfile::tempdir().unwrap();
    let physical = temp_root(&fixture);
    let source_state = physical.join("source-state");
    let source = ClientStateStore::open(&source_state).unwrap();
    let record = fresh_record(&source);
    source.create_job(record.clone()).unwrap();

    let target_state = physical.join("target-state");
    let target = ClientStateStore::open(&target_state).unwrap();
    let filename = format!("{}.json", record.meta().job_id());
    fs::copy(
        source_state.join("jobs").join(&filename),
        target_state.join("jobs").join(&filename),
    )
    .unwrap();
    fs::set_permissions(
        target_state.join("jobs").join(&filename),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();

    assert!(target.load_job(record.meta().job_id()).is_err());
    assert!(target.list_jobs().is_err());
    let local_candidate = make_record(
        record.meta().job_id(),
        target.client_id(),
        record.lease_token(),
        DIGEST_A,
        None,
        false,
    );
    assert!(
        target
            .create_job(local_candidate.clone())
            .unwrap_err()
            .to_string()
            .contains("CLIENT_ID_MISMATCH")
    );
    assert!(
        target
            .update_job(local_candidate)
            .unwrap_err()
            .to_string()
            .contains("CLIENT_ID_MISMATCH")
    );
}

fn operation_tree_contains(root: &Path, expected: &[u8]) -> bool {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                pending.push(entry.path());
            } else if fs::read(entry.path()).is_ok_and(|bytes| bytes == expected) {
                return true;
            }
        }
    }
    false
}

fn tree_contains_named_entry(root: &Path, expected_name: &str) -> bool {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = fs::read_dir(directory) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            if entry.file_name() == expected_name {
                return true;
            }
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                pending.push(entry.path());
            }
        }
    }
    false
}
