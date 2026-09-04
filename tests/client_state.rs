use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    os::unix::{
        ffi::OsStringExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink},
        process::ExitStatusExt,
    },
    path::{Path, PathBuf},
    sync::{Arc, Barrier, Mutex, mpsc},
    thread,
    time::Duration,
};

use mac_worker::{
    client_state::{ClientStateCreationRacePoint, ClientStateStore, ClientStateWritePoint},
    config::WorkerEntry,
    error::{ProcessError, WorkerError},
    job::{
        AdmissionObservation, ClientId, CommandSpec, CommandSummary, JobId, JobMeta, JobState,
        JobStatus, LeaseToken, LocalJobRecord, PreacceptanceDisposition, ProcessIdentity,
        QueueEntry, QueueEntryKind, RemoteUncertainty, RequestFingerprintMaterial,
        ResolveOrAbandonRequest, ResolveOrAbandonResponse, StatusResponse, SubmitRequest,
    },
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    scheduler::{CandidateSlot, WorkerPreference},
    transfer::{RemoteJobClient, ResolutionRuntime},
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
        1_000,
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
    let meta = JobMeta::new(&material, fingerprint).unwrap();
    let uncertainty = if cleanup_pending {
        RemoteUncertainty::cleanup_pending("CLEANUP_INCOMPLETE").unwrap()
    } else {
        RemoteUncertainty::None
    };
    LocalJobRecord::new(meta, lease_token, last_status, uncertainty).unwrap()
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

fn queue_record(store: &ClientStateStore, job_id: &str, at: u64) -> QueueEntry {
    QueueEntry::new(
        job_id.parse().unwrap(),
        store.client_id(),
        PROJECT_ID.into(),
        WORKTREE_ID.into(),
        CommandSummary::shell(),
        Vec::new(),
        WorkerPreference::Automatic,
        QueueEntryKind::Batch,
        None,
        ProcessIdentity::new(90_000, 90_000_001).unwrap(),
        at,
    )
    .unwrap()
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
        false,
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
        // boundary, as EPERM on some macOS releases and EILSEQ on others. On
        // filesystems that can represent it, the assertion below exercises
        // the store's independent fail-closed check.
        let rejected_by_kernel = error.kind() == std::io::ErrorKind::PermissionDenied
            || error.raw_os_error() == Some(libc::EILSEQ);
        assert!(
            rejected_by_kernel,
            "unexpected error creating non-UTF-8 entry: {error:?}"
        );
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
        false,
    );

    store.inject_write_failure_once(ClientStateWritePoint::BeforePublish);
    assert!(store.update_job(accepted.clone()).is_err());
    assert_eq!(store.load_job(original.meta().job_id()).unwrap(), original);

    store.inject_write_failure_once(ClientStateWritePoint::AfterPublish);
    assert!(store.update_job(accepted.clone()).is_err());
    let reopened = ClientStateStore::open(&state).unwrap();
    assert_eq!(
        reopened.load_job(accepted.meta().job_id()).unwrap(),
        accepted
    );
    let before_retry = reopened.durability_sync_counts().jobs;
    reopened
        .update_observation(
            accepted.meta().job_id(),
            accepted.last_status().unwrap().clone(),
        )
        .unwrap();
    assert!(reopened.durability_sync_counts().jobs > before_retry);
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
fn recognized_cleanup_crash_residue_does_not_poison_reopen_or_job_listing() {
    for point in [
        ClientStateWritePoint::CrashCleanupAfterRetirementCreated,
        ClientStateWritePoint::CrashCleanupAfterOperationMoved,
        ClientStateWritePoint::CrashCleanupBeforeNestedCleanup,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("state");
        let store = ClientStateStore::open(&state).unwrap();
        let client_id = store.client_id();
        let baseline = make_record(
            "00000000000000000000000000000001".parse().unwrap(),
            client_id,
            LeaseToken::generate(),
            DIGEST_A,
            None,
            false,
        );
        store.create_job(baseline.clone()).unwrap();
        let record = fresh_record(&store);
        store.inject_write_failure_once(point);

        assert!(store.create_job(record.clone()).is_err(), "{point:?}");
        let reopened = ClientStateStore::open(&state).unwrap();
        assert_eq!(reopened.client_id(), client_id, "{point:?}");
        assert_eq!(
            reopened.list_jobs().unwrap(),
            vec![baseline, record.clone()],
            "{point:?}"
        );
        assert_eq!(reopened.load_job(record.meta().job_id()).unwrap(), record);
    }
}

#[test]
fn rollback_crash_residue_stays_out_of_root_and_jobs() {
    for point in [
        ClientStateWritePoint::CrashRollbackAfterEntryMoved,
        ClientStateWritePoint::CrashRollbackBeforeNestedCleanup,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("state");
        let store = ClientStateStore::open(&state).unwrap();
        let client_id = store.client_id();
        let baseline = fresh_record(&store);
        store.create_job(baseline.clone()).unwrap();
        let interrupted = fresh_record(&store);
        store.inject_write_failure_once(point);

        assert!(store.create_job(interrupted.clone()).is_err(), "{point:?}");
        assert!(
            !state
                .join("jobs")
                .join(format!("{}.json", interrupted.meta().job_id()))
                .exists()
        );
        let reopened = ClientStateStore::open(&state).unwrap();
        assert_eq!(reopened.client_id(), client_id);
        assert_eq!(reopened.list_jobs().unwrap(), vec![baseline.clone()]);
        assert_eq!(
            reopened.load_job(baseline.meta().job_id()).unwrap(),
            baseline
        );
    }
}

#[test]
fn client_identity_rollback_crash_residue_is_reopenable() {
    for point in [
        ClientStateWritePoint::CrashRollbackAfterEntryMoved,
        ClientStateWritePoint::CrashRollbackBeforeNestedCleanup,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("state");
        assert!(ClientStateStore::open_with_write_fault(&state, point).is_err());
        assert!(!state.join("client-id").exists(), "{point:?}");
        let reopened = ClientStateStore::open(&state).unwrap();
        assert!(reopened.list_jobs().unwrap().is_empty());
        assert_eq!(
            reopened.client_id(),
            ClientStateStore::open(&state).unwrap().client_id()
        );
    }
}

#[test]
fn operation_residue_schema_rejects_symlink_permissive_and_malformed_entries() {
    for kind in ["symlink", "permissive", "malformed", "non-utf8"] {
        let fixture = tempfile::tempdir().unwrap();
        let state = temp_root(&fixture).join("state");
        ClientStateStore::open(&state).unwrap();
        let operations = state.join(".mac-worker-state");
        let recognized = operations.join(format!("cleanup-{}", JobId::generate()));
        match kind {
            "symlink" => symlink("outside", &recognized).unwrap(),
            "permissive" => {
                fs::create_dir(&recognized).unwrap();
                fs::set_permissions(&recognized, fs::Permissions::from_mode(0o755)).unwrap();
            }
            "malformed" => {
                fs::create_dir(&recognized).unwrap();
                fs::set_permissions(&recognized, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(recognized.join("unexpected"), b"unexpected\n").unwrap();
                fs::set_permissions(
                    recognized.join("unexpected"),
                    fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            }
            "non-utf8" => {
                let invalid = operations.join(OsString::from_vec(vec![0xff, 0xfe]));
                if fs::create_dir(&invalid).is_err() {
                    continue;
                }
                fs::set_permissions(&invalid, fs::Permissions::from_mode(0o700)).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(ClientStateStore::open(&state).is_err(), "{kind}");
    }
}

#[test]
fn concurrent_open_waits_for_cooperating_cleanup_lock() {
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    let cleanup_pause = store.pause_cleanup_after_move_once();
    let writer_store = store.clone();
    let writer_record = record.clone();
    let (writer_sender, writer_receiver) = mpsc::channel();
    let writer = thread::spawn(move || {
        writer_sender
            .send(writer_store.create_job(writer_record))
            .unwrap();
    });
    cleanup_pause.wait_until_paused();

    let lock_probe = store.observe_next_lock_contention();
    let (result_sender, result_receiver) = mpsc::channel();
    let open_state = state.clone();
    let opener = thread::spawn(move || {
        result_sender
            .send(ClientStateStore::open(&open_state))
            .unwrap();
    });
    lock_probe.wait_until_confirmed();
    assert!(matches!(
        result_receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    cleanup_pause.resume();
    writer_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let reopened = result_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.join().unwrap();
    opener.join().unwrap();
    assert_eq!(reopened.list_jobs().unwrap(), vec![record]);
    assert!(
        fs::read_dir(state.join(".mac-worker-state"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn replacing_jobs_lock_does_not_split_the_authoritative_lock_domain() {
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    let cleanup_pause = store.pause_cleanup_after_move_once();
    let writer_store = store.clone();
    let writer_record = record.clone();
    let (writer_sender, writer_receiver) = mpsc::channel();
    let writer = thread::spawn(move || {
        writer_sender
            .send(writer_store.create_job(writer_record))
            .unwrap();
    });
    cleanup_pause.wait_until_paused();

    let lock_path = state.join("jobs.lock");
    let old_inode = fs::metadata(&lock_path).unwrap().ino();
    fs::rename(&lock_path, fixture.path().join("displaced-jobs-lock")).unwrap();
    fs::write(&lock_path, b"").unwrap();
    fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600)).unwrap();
    let new_inode = fs::metadata(&lock_path).unwrap().ino();
    assert_ne!(old_inode, new_inode);

    let contention = store.observe_next_lock_contention();
    let (open_sender, open_receiver) = mpsc::channel();
    let open_state = state.clone();
    let opener = thread::spawn(move || {
        open_sender
            .send(ClientStateStore::open(&open_state))
            .unwrap();
    });
    contention.wait_until_confirmed();
    assert!(matches!(
        open_receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    cleanup_pause.resume();
    writer_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let reopened = open_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    writer.join().unwrap();
    opener.join().unwrap();
    assert_eq!(reopened.list_jobs().unwrap(), vec![record]);
}

#[test]
fn cloned_stores_use_independent_lock_descriptions_and_serialize() {
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    let cleanup_pause = store.pause_cleanup_after_move_once();
    let writer_store = store.clone();
    let writer_record = record.clone();
    let (writer_sender, writer_receiver) = mpsc::channel();
    let writer = thread::spawn(move || {
        writer_sender
            .send(writer_store.create_job(writer_record))
            .unwrap();
    });
    cleanup_pause.wait_until_paused();

    let contention = store.observe_next_lock_contention();
    let reader_store = store.clone();
    let (reader_sender, reader_receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        reader_sender.send(reader_store.list_jobs()).unwrap();
    });
    contention.wait_until_confirmed();
    assert!(matches!(
        reader_receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    cleanup_pause.resume();
    writer_receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(
        reader_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        vec![record]
    );
    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn uncontended_lock_acquisition_does_not_report_contention() {
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let contention = store.observe_next_lock_contention();

    assert!(store.list_jobs().unwrap().is_empty());
    assert!(!contention.confirmed_within(Duration::from_millis(50)));
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

#[test]
fn remote_uncertainty_has_distinct_canonical_persistent_states() {
    // Catches the old boolean schema collapsing an unreachable host into a
    // host-confirmed abandonment whose cleanup still needs retrying.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.create_job(record.clone()).unwrap();

    let persisted = serde_json::to_value(store.load_job(record.meta().job_id()).unwrap()).unwrap();
    assert_eq!(
        persisted["remote_uncertainty"],
        serde_json::json!({"state":"none"})
    );
    assert!(persisted.get("cleanup_pending").is_none());

    let unknown = store
        .set_remote_uncertainty(
            record.meta().job_id(),
            RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
        )
        .unwrap();
    assert_eq!(
        unknown.remote_uncertainty(),
        &RemoteUncertainty::UnknownRemote {
            code: "UNKNOWN_REMOTE".into()
        }
    );
    let cleanup = store
        .set_remote_uncertainty(
            record.meta().job_id(),
            RemoteUncertainty::cleanup_pending("CLEANUP_INCOMPLETE").unwrap(),
        )
        .unwrap();
    assert_eq!(
        cleanup.remote_uncertainty(),
        &RemoteUncertainty::CleanupPending {
            code: "CLEANUP_INCOMPLETE".into()
        }
    );

    let bytes = fs::read(
        state
            .join("jobs")
            .join(format!("{}.json", record.meta().job_id())),
    )
    .unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains(
        r#""remote_uncertainty":{"state":"cleanup_pending","code":"CLEANUP_INCOMPLETE"}"#
    ));
}

#[test]
fn lock_internal_mutations_preserve_the_other_fresh_mutable_field() {
    // Catches read-unlocked/write-later helpers clobbering a newer uncertainty
    // marker or observation from a stale LocalJobRecord clone.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();

    store
        .set_remote_uncertainty(
            original.meta().job_id(),
            RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
        )
        .unwrap();
    let observed = store
        .update_observation(
            original.meta().job_id(),
            JobStatus::accepted(2_000).unwrap(),
        )
        .unwrap();
    assert_eq!(
        observed.remote_uncertainty(),
        &RemoteUncertainty::UnknownRemote {
            code: "UNKNOWN_REMOTE".into()
        }
    );

    let marked = store
        .set_remote_uncertainty(
            original.meta().job_id(),
            RemoteUncertainty::cleanup_pending("CLEANUP_INCOMPLETE").unwrap(),
        )
        .unwrap();
    assert_eq!(marked.last_status(), observed.last_status());

    let stale = make_record(
        original.meta().job_id(),
        store.client_id(),
        original.lease_token(),
        DIGEST_A,
        Some(JobStatus::succeeded(3_000, 1, 2).unwrap()),
        false,
    );
    assert!(store.update_job(stale).is_err());
    assert_eq!(store.load_job(original.meta().job_id()).unwrap(), marked);
}

#[test]
fn concurrent_lock_internal_mutations_preserve_both_results() {
    // Catches each updater cloning the same pre-lock record and last-writer
    // wins publication dropping the other updater's successful result.
    let fixture = tempfile::tempdir().unwrap();
    let store = Arc::new(ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap());
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let observation_store = Arc::clone(&store);
    let observation_barrier = Arc::clone(&barrier);
    let job_id = original.meta().job_id();
    let observation = thread::spawn(move || {
        observation_barrier.wait();
        observation_store.update_observation(job_id, JobStatus::accepted(2_000).unwrap())
    });
    let uncertainty_store = Arc::clone(&store);
    let uncertainty_barrier = Arc::clone(&barrier);
    let uncertainty = thread::spawn(move || {
        uncertainty_barrier.wait();
        uncertainty_store.set_remote_uncertainty(
            job_id,
            RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
        )
    });
    observation.join().unwrap().unwrap();
    uncertainty.join().unwrap().unwrap();

    let final_record = store.load_job(job_id).unwrap();
    assert_eq!(
        final_record.last_status(),
        Some(&JobStatus::accepted(2_000).unwrap())
    );
    assert_eq!(
        final_record.remote_uncertainty(),
        &RemoteUncertainty::UnknownRemote {
            code: "UNKNOWN_REMOTE".into()
        }
    );
}

#[test]
fn observations_use_status_transitions_for_same_state_enrichment() {
    // Catches same-state Accepted updates replacing identities arbitrarily or
    // terminal updates rewriting the command outcome under a newer timestamp.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let accepted = JobStatus::accepted(2_000).unwrap();
    store
        .update_observation(original.meta().job_id(), accepted.clone())
        .unwrap();
    let supervised = accepted
        .with_supervisor(ProcessIdentity::new(10, 100).unwrap(), 2_001)
        .unwrap();
    store
        .update_observation(original.meta().job_id(), supervised.clone())
        .unwrap();
    let child_bound = supervised
        .with_child(ProcessIdentity::new(11, 101).unwrap(), 2_002)
        .unwrap();
    store
        .update_observation(original.meta().job_id(), child_bound.clone())
        .unwrap();
    assert_eq!(
        store
            .update_observation(original.meta().job_id(), child_bound.clone())
            .unwrap()
            .last_status(),
        Some(&child_bound)
    );

    let replaced_identity = JobStatus::accepted(2_000)
        .unwrap()
        .with_supervisor(ProcessIdentity::new(20, 200).unwrap(), 2_003)
        .unwrap();
    assert!(
        store
            .update_observation(original.meta().job_id(), replaced_identity)
            .is_err()
    );
}

#[test]
fn skipped_observations_require_sticky_identities_and_terminal_outcomes() {
    // Catches polling skips dropping a previously learned process identity or
    // allowing a terminal command outcome to be changed on a later refresh.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let accepted = JobStatus::accepted(2_000)
        .unwrap()
        .with_supervisor(ProcessIdentity::new(10, 100).unwrap(), 2_001)
        .unwrap();
    store
        .update_observation(original.meta().job_id(), accepted.clone())
        .unwrap();

    let skipped_terminal = JobStatus::new(
        JobState::Succeeded,
        3_000,
        Some(10),
        Some(100),
        None,
        None,
        Some(0),
        None,
        Some(5),
        Some(6),
        None,
        None,
    )
    .unwrap();
    store
        .update_observation(original.meta().job_id(), skipped_terminal.clone())
        .unwrap();

    let dropped_identity = JobStatus::succeeded(3_001, 5, 6).unwrap();
    assert!(
        store
            .update_observation(original.meta().job_id(), dropped_identity)
            .is_err()
    );
    let rewritten_outcome = JobStatus::new(
        JobState::Failed,
        3_002,
        Some(10),
        Some(100),
        None,
        None,
        Some(7),
        None,
        Some(5),
        Some(6),
        None,
        None,
    )
    .unwrap();
    assert!(
        store
            .update_observation(original.meta().job_id(), rewritten_outcome)
            .is_err()
    );
    assert_eq!(
        store
            .load_job(original.meta().job_id())
            .unwrap()
            .last_status(),
        Some(&skipped_terminal)
    );
}

#[test]
fn terminal_observation_allows_only_cleanup_error_enrichment() {
    // Catches cleanup retry state being impossible to persist, or its update
    // accidentally changing a completed command's immutable result.
    let fixture = tempfile::tempdir().unwrap();
    let store = ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap();
    let original = fresh_record(&store);
    store.create_job(original.clone()).unwrap();
    let terminal = JobStatus::succeeded(3_000, 5, 6).unwrap();
    store
        .update_observation(original.meta().job_id(), terminal.clone())
        .unwrap();
    let enriched = terminal
        .with_cleanup_error("LEASE_RELEASE_FAILED".into(), 3_001)
        .unwrap();
    store
        .update_observation(original.meta().job_id(), enriched.clone())
        .unwrap();
    assert_eq!(
        store
            .load_job(original.meta().job_id())
            .unwrap()
            .last_status(),
        Some(&enriched)
    );

    let changed_lengths = JobStatus::new(
        JobState::Succeeded,
        3_002,
        None,
        None,
        None,
        None,
        Some(0),
        None,
        Some(50),
        Some(6),
        None,
        Some("LEASE_RELEASE_FAILED".into()),
    )
    .unwrap();
    assert!(
        store
            .update_observation(original.meta().job_id(), changed_lengths)
            .is_err()
    );
}

fn remote_worker() -> WorkerEntry {
    WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
    }
}

fn response_line(value: &impl serde::Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

struct BlockingStatusRunner {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    response: Vec<u8>,
}

impl ProcessRunner for BlockingStatusRunner {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("blocked runner was not released");
        Ok(ProcessResult {
            status: std::process::ExitStatus::from_raw(0),
            stdout: self.response.clone(),
            stderr: Vec::new(),
        })
    }
}

struct ScriptedClientRunner {
    results: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
}

impl ProcessRunner for ScriptedClientRunner {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected remote call")
    }
}

struct BlockingRetryRuntime {
    now: Mutex<Duration>,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl ResolutionRuntime for BlockingRetryRuntime {
    fn monotonic_now(&self) -> Duration {
        *self.now.lock().unwrap()
    }

    fn sleep(&self, duration: Duration) {
        assert_eq!(duration, Duration::from_secs(1));
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("blocked retry sleep was not released");
        *self.now.lock().unwrap() = Duration::from_secs(30);
    }
}

#[test]
fn remote_status_call_holds_no_local_client_state_lock() {
    // Break caught: the caller keeps jobs.lock around SSH, preventing a fresh
    // observation update while the process runner is blocked.
    let fixture = tempfile::tempdir().unwrap();
    let store = Arc::new(ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap());
    let record = fresh_record(&store);
    store.create_job(record.clone()).unwrap();
    let response =
        StatusResponse::new(record.meta().clone(), JobStatus::accepted(2_000).unwrap()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let runner = Arc::new(BlockingStatusRunner {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        response: response_line(&response),
    });
    let job_id = record.meta().job_id();
    let client_runner = Arc::clone(&runner);
    let remote = thread::spawn(move || {
        RemoteJobClient::new(client_runner.as_ref()).status(&remote_worker(), job_id)
    });
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("remote runner did not block");

    let contention = store.observe_next_lock_contention();
    let updater = Arc::clone(&store);
    let (updated_tx, updated_rx) = mpsc::channel();
    let update = thread::spawn(move || {
        updated_tx
            .send(updater.update_observation(job_id, JobStatus::accepted(2_000).unwrap()))
            .unwrap();
    });
    let updated = updated_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("local update blocked behind remote call")
        .unwrap();
    assert_eq!(updated.last_status(), Some(response.status()));
    assert!(!contention.confirmed_within(Duration::ZERO));
    release_tx.send(()).unwrap();
    remote.join().unwrap().unwrap();
    update.join().unwrap();
}

#[test]
fn remote_retry_sleep_holds_no_local_client_state_lock() {
    // Break caught: the 30-second resolver owns jobs.lock across retry sleep,
    // blocking an independent uncertainty update.
    let fixture = tempfile::tempdir().unwrap();
    let store = Arc::new(ClientStateStore::open(&temp_root(&fixture).join("state")).unwrap());
    let record = fresh_record(&store);
    store.create_job(record.clone()).unwrap();
    let material = RequestFingerprintMaterial::new(
        record.meta().job_id(),
        store.client_id(),
        record.lease_token(),
        record.meta().created_at_millis(),
        record.meta().worker_name().into(),
        record.meta().project_id().into(),
        record.meta().worktree_id().into(),
        record.meta().manifest_digest().into(),
        record.meta().relative_working_dir().into(),
        record.meta().timeout_millis(),
        record.meta().resource_class().into(),
        CommandSpec::argv(vec!["tool".into(), COMMAND_SECRET.into()]).unwrap(),
    )
    .unwrap();
    let submit = SubmitRequest::new(material);
    assert_eq!(
        submit.request_fingerprint(),
        record.meta().request_fingerprint()
    );
    let request = ResolveOrAbandonRequest::from_submit_request(&submit).unwrap();
    let runner = Arc::new(ScriptedClientRunner {
        results: Mutex::new(
            vec![
                Err(ProcessError::DeadlineExceeded {
                    deadline: Duration::from_secs(30),
                }
                .into()),
                Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(0),
                    stdout: response_line(&ResolveOrAbandonResponse::abandoned()),
                    stderr: Vec::new(),
                }),
            ]
            .into(),
        ),
    });
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let runtime = Arc::new(BlockingRetryRuntime {
        now: Mutex::new(Duration::ZERO),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let client_runner = Arc::clone(&runner);
    let client_runtime = Arc::clone(&runtime);
    let remote = thread::spawn(move || {
        RemoteJobClient::new_with_runtime(client_runner.as_ref(), client_runtime.as_ref())
            .resolve_preacceptance(&remote_worker(), &request)
    });
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("retry runtime did not block");

    let contention = store.observe_next_lock_contention();
    let updater = Arc::clone(&store);
    let (updated_tx, updated_rx) = mpsc::channel();
    let update = thread::spawn(move || {
        updated_tx
            .send(updater.set_remote_uncertainty(
                record.meta().job_id(),
                RemoteUncertainty::unknown_remote("UNKNOWN_REMOTE").unwrap(),
            ))
            .unwrap();
    });
    let updated = updated_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("local uncertainty update blocked behind retry sleep")
        .unwrap();
    assert!(matches!(
        updated.remote_uncertainty(),
        RemoteUncertainty::UnknownRemote { code } if code == "UNKNOWN_REMOTE"
    ));
    assert!(!contention.confirmed_within(Duration::ZERO));
    release_tx.send(()).unwrap();
    assert!(matches!(
        remote.join().unwrap().unwrap(),
        PreacceptanceDisposition::Abandoned
    ));
    update.join().unwrap();
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

#[test]
fn observation_refresh_holds_no_queue_or_root_state_lock() {
    // Break caught: a slow external refresh retains the queue/root lock and
    // blocks an unrelated local enqueue for the duration of SSH.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = Arc::new(ClientStateStore::open(&state).unwrap());
    let (refresh_entered_tx, refresh_entered_rx) = mpsc::channel();
    let (release_refresh_tx, release_refresh_rx) = mpsc::channel();
    let refreshing = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.admission_observation("mini-1", 50_000, || {
                refresh_entered_tx.send(()).unwrap();
                release_refresh_rx.recv().unwrap();
                AdmissionObservation::new(
                    "mini-1".into(),
                    true,
                    CandidateSlot::Idle,
                    vec!["rust".into()],
                    Some(10),
                    20,
                    50_000,
                )
            })
        })
    };
    refresh_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap();

    let contention = store.observe_next_lock_contention();
    let (enqueue_tx, enqueue_rx) = mpsc::channel();
    let enqueueing = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            enqueue_tx
                .send(store.enqueue(queue_record(
                    &store,
                    "00000000000000000000000000001000",
                    50_001,
                )))
                .unwrap();
        })
    };
    enqueue_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("queue mutation blocked behind observation refresh")
        .unwrap();
    assert!(!contention.confirmed_within(Duration::from_millis(100)));
    release_refresh_tx.send(()).unwrap();
    refreshing.join().unwrap().unwrap();
    enqueueing.join().unwrap();
}

#[test]
fn queue_update_crash_after_publication_is_a_complete_canonical_replacement() {
    // Break caught: the queue uses an in-place write instead of the established
    // staged/fsynced/renamed client-state publication protocol.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let dispatcher = ProcessIdentity::new(90_000, 90_000_001).unwrap();
    store
        .enqueue(queue_record(
            &store,
            "00000000000000000000000000001001",
            51_000,
        ))
        .unwrap();
    store.inject_write_failure_once(ClientStateWritePoint::AfterPublish);
    assert!(
        store
            .claim_next(dispatcher, &["mini-1".into()], 51_001)
            .is_err()
    );

    let reopened = ClientStateStore::open(&state).unwrap();
    let bytes = fs::read(state.join("queue/state.json")).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert!(matches!(
        reopened.queue_snapshot().unwrap().entries()[0].state(),
        mac_worker::job::QueueState::Dispatching { selected_worker, .. }
            if selected_worker == "mini-1"
    ));
}

#[test]
fn leftover_rooted_fs_namespaces_never_brick_the_state_root() {
    // Catches treating RootedDir's private `.mac-worker-rooted-fs` namespace,
    // which a process dying mid-write leaves behind, as an unexpected entry:
    // every later command then failed with a bare I/O error.
    let fixture = tempfile::tempdir().unwrap();
    let state = temp_root(&fixture).join("state");
    let store = ClientStateStore::open(&state).unwrap();
    let record = fresh_record(&store);
    store.create_job(record.clone()).unwrap();
    drop(store);

    for relative in [
        "",
        "jobs",
        "tasks",
        "runs",
        "runners",
        "turns",
        "queue",
        "observations",
        "affinity",
        "affinity/projects",
        "affinity/worktrees",
    ] {
        let namespace = state.join(relative).join(".mac-worker-rooted-fs");
        fs::create_dir(&namespace).unwrap();
        fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700)).unwrap();
    }

    let store = ClientStateStore::open(&state).unwrap();
    assert_eq!(
        store
            .list_jobs()
            .unwrap()
            .iter()
            .map(|job| job.meta().job_id())
            .collect::<Vec<_>>(),
        vec![record.meta().job_id()]
    );
    assert!(store.list_tasks().unwrap().is_empty());
    assert!(store.list_runs().unwrap().is_empty());
    assert_eq!(store.load_job(record.meta().job_id()).unwrap(), record);
}
