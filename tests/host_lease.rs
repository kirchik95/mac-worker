use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::Cursor,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::{OpenOptionsExt, PermissionsExt, symlink},
    },
    path::Path,
    sync::{Arc, Barrier},
    thread,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    error::WorkerError,
    host_store::{HostStore, HostStoreWritePoint},
    job::{
        ClientId, CommandSpec, HostControlError, JobId, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LeaseToken, RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService, SlotState},
    paths::PathLayout,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{MemoryPressure, PROTOCOL_VERSION},
    run_with_stdio_in_context,
};
use tempfile::tempdir;

const GIB: u64 = 1024 * 1024 * 1024;

struct NoProcess;

impl ProcessRunner for NoProcess {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("host lease input validation must not invoke a subprocess")
    }
}

fn request(seed: u128) -> LeaseAcquireRequest {
    let job = JobId::new(uuid::Uuid::from_u128(seed));
    let client = ClientId::new(uuid::Uuid::from_u128(seed + 10_000));
    let token = LeaseToken::new(uuid::Uuid::from_u128(seed + 20_000));
    let material = RequestFingerprintMaterial::new(
        job,
        client,
        token,
        50,
        "mini-1".into(),
        "a".repeat(64),
        "b".repeat(64),
        "c".repeat(64),
        String::new(),
        60_000,
        "heavy".into(),
        CommandSpec::argv(vec!["cargo".into(), "test".into()]).unwrap(),
    )
    .unwrap();
    LeaseAcquireRequest::new(material)
}

fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * GIB,
        total_disk_bytes: 250 * GIB,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

#[test]
fn host_layout_is_private_and_identifier_paths_are_canonical() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("data/mac-worker");
    fs::create_dir(temp.path().join("data")).unwrap();
    fs::write(temp.path().join("data/sentinel"), b"keep").unwrap();

    let store = HostStore::open(&root).unwrap();
    let req = request(1);
    let material = req.material();

    assert_eq!(
        store.job_index(material.job_id()).unwrap(),
        root.join(format!("job-index/{}.json", material.job_id()))
    );
    assert_eq!(
        store
            .incoming_job(material.job_id(), material.lease_token())
            .unwrap(),
        root.join(format!(
            "incoming/{}/{}",
            material.job_id(),
            material.lease_token()
        ))
    );
    assert_eq!(
        store.verified_receipt(material.job_id()).unwrap(),
        root.join(format!("verified/{}.json", material.job_id()))
    );
    assert_eq!(
        store
            .job(
                material.project_id(),
                material.worktree_id(),
                material.job_id(),
            )
            .unwrap(),
        root.join("jobs")
            .join(material.project_id())
            .join(material.worktree_id())
            .join(material.job_id().to_string())
    );
    assert_eq!(
        store
            .snapshot(
                material.project_id(),
                material.worktree_id(),
                material.manifest_digest(),
            )
            .unwrap(),
        root.join("snapshots")
            .join(material.project_id())
            .join(material.worktree_id())
            .join(material.manifest_digest())
    );
    assert!(
        store
            .snapshot(
                "../escape",
                material.worktree_id(),
                material.manifest_digest()
            )
            .is_err()
    );
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(root.join("job-index"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        fs::read(temp.path().join("data/sentinel")).unwrap(),
        b"keep"
    );

    let staged = store
        .begin_job(
            material.project_id(),
            material.worktree_id(),
            material.job_id(),
        )
        .unwrap();
    let debug = format!("{staged:?}");
    assert!(!debug.contains(root.to_string_lossy().as_ref()));
}

#[test]
fn symlinked_root_or_owned_component_is_rejected() {
    let temp = tempdir().unwrap();
    let outside = temp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let linked_root = temp.path().join("linked");
    symlink(&outside, &linked_root).unwrap();
    assert!(HostStore::open(&linked_root).is_err());

    let nested = outside.join("nested");
    fs::create_dir(&nested).unwrap();
    let linked_parent = temp.path().join("linked-parent");
    symlink(&outside, &linked_parent).unwrap();
    assert!(HostStore::open(&linked_parent.join("nested")).is_err());

    let root = temp.path().join("real");
    fs::create_dir(&root).unwrap();
    symlink(&outside, root.join("leases")).unwrap();
    assert!(HostStore::open(&root).is_err());
}

#[test]
fn replacing_a_retained_nested_namespace_never_redirects_host_mutation() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let outside = temp.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let store = HostStore::open(&root).unwrap();
    fs::rename(root.join("locks/jobs"), root.join("locks/jobs-original")).unwrap();
    symlink(&outside, root.join("locks/jobs")).unwrap();

    assert!(
        LeaseService::new(&store)
            .acquire(&request(1), &healthy(), 1)
            .is_err()
    );
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    assert_eq!(
        fs::read_dir(root.join("locks/jobs-original"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn safe_unrelated_entries_are_preserved_but_non_utf8_entries_fail_closed() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    HostStore::open(&root).unwrap();
    let unrelated = root.join("operator-note");
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&unrelated)
        .unwrap();
    HostStore::open(&root).unwrap();
    assert!(unrelated.exists());

    let invalid = root.join(OsString::from_vec(b"invalid-\xff".to_vec()));
    let created = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(invalid);
    if created.is_ok() {
        assert!(HostStore::open(&root).is_err());
    }
    assert!(unrelated.exists());
}

#[test]
fn permissive_preexisting_host_components_are_rejected_not_repaired() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(HostStore::open(&root).is_err());
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o755
    );

    let initialized_root = temp.path().join("initialized-host");
    let store = HostStore::open(&initialized_root).unwrap();
    fs::set_permissions(
        initialized_root.join("leases"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    assert!(
        LeaseService::new(&store)
            .acquire(&request(1), &healthy(), 1)
            .is_err()
    );
    assert_eq!(
        fs::metadata(initialized_root.join("leases"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
}

#[test]
fn sixty_four_distinct_contenders_produce_one_live_lease() {
    let temp = tempdir().unwrap();
    let store = Arc::new(HostStore::open(&temp.path().join("host")).unwrap());
    let barrier = Arc::new(Barrier::new(64));
    let handles = (1..=64)
        .map(|seed| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let request = request(seed);
                barrier.wait();
                let result = LeaseService::new(&store).acquire(&request, &healthy(), 1_000);
                (request.material().job_id(), result)
            })
        })
        .collect::<Vec<_>>();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let winners = outcomes
        .iter()
        .filter(|(_, result)| matches!(result, Ok(LeaseAcquireResponse::Acquired { .. })))
        .collect::<Vec<_>>();

    assert_eq!(winners.len(), 1);
    assert_eq!(
        LeaseService::new(&store).load().unwrap().unwrap().job_id(),
        winners[0].0
    );
    assert!(outcomes.iter().all(|(_, result)| match result {
        Ok(LeaseAcquireResponse::Acquired { .. }) => true,
        Err(WorkerError::Capacity { code, .. }) => code == &"CAPACITY_BUSY",
        _ => false,
    }));
}

#[test]
fn exact_retry_is_idempotent_but_changed_identity_is_busy() {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let service = LeaseService::new(&store);
    let first = request(1);

    let acquired = service.acquire(&first, &healthy(), 10).unwrap();
    let retried = service.acquire(&first, &healthy(), 20).unwrap();
    assert_eq!(acquired, retried);

    let changed = request(2);
    let changed_token = changed.material().lease_token().to_string();
    let error = service.acquire(&changed, &healthy(), 30).unwrap_err();
    assert!(matches!(
        error,
        WorkerError::Capacity {
            code: "CAPACITY_BUSY",
            ..
        }
    ));
    assert!(!error.to_string().contains(&changed_token));
}

#[test]
fn exact_retry_compares_live_fields_to_independently_recomputed_request() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let req = request(1);
    LeaseService::new(&store)
        .acquire(&req, &healthy(), 10)
        .unwrap();
    let path = root.join("leases/heavy/lease.json");
    let original = fs::read_to_string(&path).unwrap();
    let altered = original.replacen(
        &format!("\"project_id\":\"{}\"", "a".repeat(64)),
        &format!("\"project_id\":\"{}\"", "d".repeat(64)),
        1,
    );
    assert_ne!(altered, original);
    fs::write(path, altered).unwrap();

    assert!(
        LeaseService::new(&store)
            .acquire(&req, &healthy(), 20)
            .is_err()
    );
}

#[test]
fn admission_uses_disk_total_memory_pressure_and_swap_thresholds() {
    for (facts, code) in [
        (
            AdmissionFacts {
                free_disk_bytes: 49 * GIB,
                ..healthy()
            },
            "INSUFFICIENT_DISK",
        ),
        (
            AdmissionFacts {
                free_disk_bytes: 59 * GIB,
                total_disk_bytes: 300 * GIB,
                ..healthy()
            },
            "INSUFFICIENT_DISK",
        ),
        (
            AdmissionFacts {
                memory_pressure: MemoryPressure::Critical,
                ..healthy()
            },
            "MEMORY_PRESSURE",
        ),
        (
            AdmissionFacts {
                swap_used_bytes: Some(2 * GIB + 1),
                ..healthy()
            },
            "SWAP_LIMIT",
        ),
    ] {
        let temp = tempdir().unwrap();
        let store = HostStore::open(&temp.path().join("host")).unwrap();
        let error = LeaseService::new(&store)
            .acquire(&request(1), &facts, 1)
            .unwrap_err();
        assert!(matches!(error, WorkerError::Capacity { code: actual, .. } if actual == code));
        assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    }
}

#[test]
fn accepted_and_abandoned_dispositions_fence_job_ids_without_leases() {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let accepted = request(1);
    let status = JobStatus::accepted(100).unwrap();
    store.record_accepted(&accepted, &status, 100).unwrap();
    store.record_accepted(&accepted, &status, 101).unwrap();

    assert_eq!(
        LeaseService::new(&store)
            .acquire(&accepted, &healthy(), 200)
            .unwrap(),
        LeaseAcquireResponse::ExistingAccepted {
            status: status.clone()
        }
    );
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);

    let abandoned = request(2);
    store.record_abandoned(&abandoned, 300).unwrap();
    store.record_abandoned(&abandoned, 301).unwrap();
    let error = LeaseService::new(&store)
        .acquire(&abandoned, &healthy(), 400)
        .unwrap_err();
    assert!(matches!(error, WorkerError::Protocol(message) if message.contains("JOB_ABANDONED")));
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn mismatched_same_job_abandonment_is_a_conflict_during_lease_acquisition() {
    let temp = tempdir().unwrap();
    let store = HostStore::open(&temp.path().join("host")).unwrap();
    let abandoned = request(3);
    store.record_abandoned(&abandoned, 100).unwrap();
    let different_client = request(4).material().client_id();
    let material = abandoned.material();
    let mismatched = LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            material.job_id(),
            different_client,
            material.lease_token(),
            material.created_at_millis(),
            material.worker_name().into(),
            material.project_id().into(),
            material.worktree_id().into(),
            material.manifest_digest().into(),
            material.relative_working_dir().into(),
            material.timeout_millis(),
            material.resource_class().into(),
            material.command().clone(),
        )
        .unwrap(),
    );

    let error = LeaseService::new(&store)
        .acquire(&mismatched, &healthy(), 200)
        .unwrap_err();

    assert!(matches!(error, WorkerError::Protocol(message) if message.contains("JOB_ID_CONFLICT")));
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn accepted_disposition_embedded_job_id_must_match_canonical_filename() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let accepted = request(1);
    store
        .record_accepted(&accepted, &JobStatus::accepted(100).unwrap(), 100)
        .unwrap();
    let requested = accepted.material().job_id();
    let path = store.job_index(requested).unwrap();
    let bytes = fs::read_to_string(&path).unwrap();
    let mismatched = request(2).material().job_id();
    fs::write(
        &path,
        bytes.replacen(&requested.to_string(), &mismatched.to_string(), 1),
    )
    .unwrap();

    let error = LeaseService::new(&store)
        .acquire(&accepted, &healthy(), 200)
        .unwrap_err();

    assert!(matches!(error, WorkerError::Protocol(message) if message.contains("JOB_ID_CONFLICT")));
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn abandoned_disposition_embedded_job_id_must_match_canonical_filename() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let abandoned = request(1);
    store.record_abandoned(&abandoned, 100).unwrap();
    let requested = abandoned.material().job_id();
    let path = store.job_index(requested).unwrap();
    let bytes = fs::read_to_string(&path).unwrap();
    let mismatched = request(2).material().job_id();
    fs::write(
        &path,
        bytes.replacen(&requested.to_string(), &mismatched.to_string(), 1),
    )
    .unwrap();

    let error = LeaseService::new(&store)
        .acquire(&abandoned, &healthy(), 200)
        .unwrap_err();

    assert!(matches!(error, WorkerError::Protocol(message) if message.contains("JOB_ID_CONFLICT")));
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
}

#[test]
fn read_only_absent_probe_is_idle_and_does_not_create_root() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("absent");

    let occupancy = LeaseService::load_if_present(&root).unwrap();

    assert_eq!(occupancy.slot_state, SlotState::Idle);
    assert!(occupancy.active_lease.is_none());
    assert!(!root.exists());
}

#[test]
fn setup_container_is_isolated_from_production_probe_and_lease_acquire() {
    let temp = tempdir().unwrap();
    let environment = BTreeMap::from([(
        OsString::from("XDG_DATA_HOME"),
        temp.path().join("data").into_os_string(),
    )]);
    let home = temp.path().join("home");
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    fs::create_dir_all(paths.data.join("setup")).unwrap();
    fs::set_permissions(&paths.data, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(paths.data.join("setup"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(paths.data.join("setup/sentinel"), b"setup-owned").unwrap();
    let host_root = paths.host_state_root();
    let runtime = RuntimeContext::isolated(environment, home, temp.path().to_path_buf());
    let cli = Cli::try_parse_from(["worker", "host", "probe"]).unwrap();
    let mut stdin = Cursor::new(Vec::<u8>::new());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit = run_with_stdio_in_context(
        cli,
        &NoProcess,
        &runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(
        exit,
        0,
        "host probe failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert!(stderr.is_empty());
    let response: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(response["slot_state"], "idle");
    assert!(response["active_lease"].is_null());
    let entries = fs::read_dir(&paths.data)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries, [OsString::from("setup")]);
    assert!(!host_root.exists());

    let lease_request = request(901);
    let store = HostStore::open(&host_root).unwrap();
    let acquired = LeaseService::new(&store)
        .acquire(&lease_request, &healthy(), 1)
        .unwrap();
    drop(store);

    let cli = Cli::try_parse_from(["worker", "host", "lease-acquire"]).unwrap();
    let mut stdin = Cursor::new(serde_json::to_vec(&lease_request).unwrap());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &NoProcess,
        &runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(
        exit,
        0,
        "host lease-acquire failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert!(stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<LeaseAcquireResponse>(&stdout).unwrap(),
        acquired
    );
    assert_eq!(
        fs::read(paths.data.join("setup/sentinel")).unwrap(),
        b"setup-owned"
    );
    assert!(host_root.join("leases/heavy/lease.json").is_file());
    let entries = fs::read_dir(&paths.data)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 5);
    assert!(entries.contains(&OsString::from("setup")));
    assert!(entries.contains(&OsString::from("host")));
    assert!(entries.contains(&OsString::from(".mac-worker-rooted-fs")));
    let anchors = entries
        .iter()
        .filter_map(|entry| entry.to_str())
        .filter(|entry| entry.starts_with(".mac-worker-installation-"))
        .collect::<Vec<_>>();
    assert_eq!(anchors.len(), 2);
    assert!(anchors.iter().any(|entry| entry.ends_with(".lock")));
    assert!(anchors.iter().any(|entry| entry.ends_with(".json")));
}

#[test]
fn read_only_probe_treats_every_partial_installation_tuple_as_corruption() {
    let root_without_anchor = tempdir().unwrap();
    let root = root_without_anchor.path().join("host");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(LeaseService::load_if_present(&root).is_err());

    let lock_only = tempdir().unwrap();
    let root = lock_only.path().join("host");
    assert!(
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterInstallationLock)
            .is_err()
    );
    assert!(!root.exists());
    assert!(LeaseService::load_if_present(&root).is_err());
    assert!(!root.exists());

    let missing_root = tempdir().unwrap();
    let root = missing_root.path().join("host");
    let _store = HostStore::open(&root).unwrap();
    let detached = missing_root.path().join("detached-host");
    fs::rename(&root, &detached).unwrap();
    assert!(LeaseService::load_if_present(&root).is_err());
    assert!(!root.exists());
}

#[test]
fn every_lease_crash_boundary_leaves_absent_or_complete_live_state() {
    for point in [
        HostStoreWritePoint::AfterLeaseWrite,
        HostStoreWritePoint::AfterLeaseFileSync,
        HostStoreWritePoint::AfterLeaseDirectorySync,
        HostStoreWritePoint::AfterLeaseParentSync,
        HostStoreWritePoint::AfterLeasePublish,
        HostStoreWritePoint::AfterLeasePublishSync,
    ] {
        let temp = tempdir().unwrap();
        let root = temp.path().join("host");
        let store = HostStore::open_with_write_fault(&root, point).unwrap();
        assert!(
            LeaseService::new(&store)
                .acquire(&request(1), &healthy(), 1)
                .is_err()
        );
        drop(store);

        let recovered = HostStore::open(&root).unwrap();
        let live = LeaseService::new(&recovered).load().unwrap();
        if matches!(
            point,
            HostStoreWritePoint::AfterLeasePublish | HostStoreWritePoint::AfterLeasePublishSync
        ) {
            assert!(
                live.is_some(),
                "published state must be complete at {point:?}"
            );
            assert_eq!(
                fs::metadata(root.join("leases/heavy/lease.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        } else {
            assert_eq!(live, None, "staging residue must be idle at {point:?}");
        }
    }
}

#[test]
fn after_cleanup_intent_commit_acquire_leftover_retries() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let req = request(1);
    let leftover = root
        .join("leases")
        .join(format!(".acquire-{}", req.material().job_id()));
    fs::create_dir(&leftover).unwrap();
    fs::set_permissions(&leftover, fs::Permissions::from_mode(0o700)).unwrap();
    let leftover_leaf = leftover.join("leftover-leaf");
    fs::write(&leftover_leaf, b"acquire-leftover").unwrap();
    fs::set_permissions(&leftover_leaf, fs::Permissions::from_mode(0o600)).unwrap();
    fs::File::open(&leftover_leaf).unwrap().sync_all().unwrap();
    fs::File::open(&leftover).unwrap().sync_all().unwrap();
    fs::File::open(root.join("leases"))
        .unwrap()
        .sync_all()
        .unwrap();
    drop(store);

    let faulted =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    assert!(
        LeaseService::new(&faulted)
            .acquire(&req, &healthy(), 1)
            .is_err()
    );
    assert_eq!(LeaseService::new(&faulted).load().unwrap(), None);
    assert!(!leftover.exists());
    let namespace = root.join("leases/.mac-worker-rooted-fs");
    assert_canonical_tree_delete_journal(&namespace);
    drop(faulted);

    let recovered = HostStore::open(&root).unwrap();
    assert!(matches!(
        LeaseService::new(&recovered)
            .acquire(&req, &healthy(), 1)
            .unwrap(),
        LeaseAcquireResponse::Acquired { .. }
    ));
    assert!(!leftover.exists());
    if namespace.exists() {
        assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
    }
}

fn assert_canonical_tree_delete_journal(namespace: &Path) {
    assert!(
        namespace.is_dir(),
        "expected tree journal {}",
        namespace.display()
    );
    let mut intent = 0usize;
    let mut decision = 0usize;
    let mut operation = 0usize;
    let mut quarantine = 0usize;
    let mut decision_path = None;
    for entry in fs::read_dir(namespace).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let name = entry.file_name();
        let bytes = name.as_bytes();
        let file_type = entry.file_type().unwrap();
        if bytes.starts_with(b"cleanup-intent-v1-") {
            intent += 1;
            assert!(
                file_type.is_file(),
                "cleanup intent must be a regular file: {}",
                path.display()
            );
        } else if bytes.starts_with(b"cleanup-decision-v1-") {
            decision += 1;
            assert!(
                file_type.is_file(),
                "cleanup decision must be a regular file: {}",
                path.display()
            );
            decision_path = Some(path);
        } else if bytes.starts_with(b"cleanup-op-v1-") {
            operation += 1;
            assert!(
                file_type.is_dir(),
                "cleanup operation must be a directory: {}",
                path.display()
            );
        } else if bytes.starts_with(b"cleanup-tree-v1-") {
            quarantine += 1;
            assert!(
                file_type.is_dir(),
                "cleanup tree quarantine must be a directory: {}",
                path.display()
            );
        } else {
            panic!(
                "unexpected journal entry {} under {}",
                name.to_string_lossy(),
                namespace.display()
            );
        }
    }
    assert_eq!(
        (intent, decision, operation, quarantine),
        (1, 1, 1, 1),
        "unexpected tree journal under {}",
        namespace.display()
    );
    let decision_path = decision_path.expect("decision role path");
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&decision_path).unwrap())
        .expect("cleanup decision must be valid JSON");
    assert!(
        value.is_object(),
        "cleanup decision must be a JSON object: {value:?}"
    );
    assert_eq!(
        value.get("decision"),
        Some(&serde_json::json!("delete")),
        "cleanup decision must be Delete: {value}"
    );
}

fn abandoned_lease_with_receipt(
    store: &HostStore,
    seed: u128,
) -> (LeaseRecord, mac_worker::host_store::CleanupReceipt) {
    let req = request(seed);
    let lease = match LeaseService::new(store)
        .acquire(&req, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    store.record_abandoned(&req, 2).unwrap();
    let receipt = store.cleanup_job_owned(&lease).unwrap();
    (lease, receipt)
}

#[test]
fn after_cleanup_intent_commit_release_leftover_keeps_heavy() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let (lease, receipt) = abandoned_lease_with_receipt(&store, 2);
    let leftover = root
        .join("leases")
        .join(format!(".released-{}", lease.job_id()));
    fs::create_dir(&leftover).unwrap();
    fs::set_permissions(&leftover, fs::Permissions::from_mode(0o700)).unwrap();
    let leftover_leaf = leftover.join("released-leaf");
    fs::write(&leftover_leaf, b"released-leftover").unwrap();
    fs::set_permissions(&leftover_leaf, fs::Permissions::from_mode(0o600)).unwrap();
    fs::File::open(&leftover_leaf).unwrap().sync_all().unwrap();
    fs::File::open(&leftover).unwrap().sync_all().unwrap();
    fs::File::open(root.join("leases"))
        .unwrap()
        .sync_all()
        .unwrap();

    let error = LeaseService::new(&store)
        .release_after_cleanup(&lease, &receipt)
        .unwrap_err();
    assert!(matches!(error, WorkerError::Io(_)), "{error}");
    assert_eq!(
        LeaseService::new(&store).load().unwrap(),
        Some(lease.clone())
    );
    assert!(root.join("leases/heavy").is_dir());
    assert!(!leftover.exists());
    let namespace = root.join("leases/.mac-worker-rooted-fs");
    assert_canonical_tree_delete_journal(&namespace);
    drop(store);

    let recovered = HostStore::open(&root).unwrap();
    LeaseService::new(&recovered)
        .release_after_cleanup(&lease, &receipt)
        .unwrap();
    assert_eq!(LeaseService::new(&recovered).load().unwrap(), None);
    assert!(!leftover.exists());
    if namespace.exists() {
        assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
    }
}

#[test]
fn after_cleanup_intent_commit_does_not_break_post_publish_retirement() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store =
        HostStore::open_with_write_fault(&root, HostStoreWritePoint::AfterCleanupIntentCommit)
            .unwrap();
    let (lease, receipt) = abandoned_lease_with_receipt(&store, 3);
    LeaseService::new(&store)
        .release_after_cleanup(&lease, &receipt)
        .unwrap();
    assert_eq!(LeaseService::new(&store).load().unwrap(), None);
    assert!(
        !root
            .join("leases")
            .join(format!(".released-{}", lease.job_id()))
            .exists()
    );
    let namespace = root.join("leases/.mac-worker-rooted-fs");
    if namespace.exists() {
        assert_eq!(fs::read_dir(&namespace).unwrap().count(), 0);
    }
}

#[test]
fn corrupt_or_unsafe_live_lease_fails_closed_and_is_never_released() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    LeaseService::new(&store)
        .acquire(&request(1), &healthy(), 1)
        .unwrap();
    fs::write(root.join("leases/heavy/lease.json"), b"{}\n").unwrap();

    assert!(LeaseService::new(&store).load().is_err());
    assert!(root.join("leases/heavy").exists());
}

#[test]
fn noncanonical_live_lease_json_fails_closed() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    LeaseService::new(&store)
        .acquire(&request(1), &healthy(), 1)
        .unwrap();
    let path = root.join("leases/heavy/lease.json");
    let mut bytes = fs::read(&path).unwrap();
    bytes.push(b'\n');
    fs::write(&path, bytes).unwrap();

    assert!(LeaseService::new(&store).load().is_err());
    assert!(root.join("leases/heavy").exists());
}

#[test]
fn abandoned_disposition_hashes_the_token_and_public_occupancy_omits_it() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("host");
    let store = HostStore::open(&root).unwrap();
    let req = request(7);
    let raw_token = req.material().lease_token().to_string();
    store.record_abandoned(&req, 1).unwrap();
    let disposition =
        fs::read_to_string(store.job_index(req.material().job_id()).unwrap()).unwrap();
    assert!(!disposition.contains(&raw_token));
    assert!(disposition.contains("lease_token_sha256"));

    let other_root = temp.path().join("other-host");
    let other = HostStore::open(&other_root).unwrap();
    LeaseService::new(&other)
        .acquire(&req, &healthy(), 1)
        .unwrap();
    let private_lease = fs::read_to_string(other_root.join("leases/heavy/lease.json")).unwrap();
    assert!(!private_lease.contains("cargo"));
    assert!(!private_lease.contains("test"));
    let public = serde_json::to_string(&LeaseService::new(&other).occupancy().unwrap()).unwrap();
    assert!(!public.contains(&raw_token));
    assert!(!public.contains(&req.material().client_id().to_string()));
    assert!(!public.contains(other_root.to_string_lossy().as_ref()));
    assert!(!format!("{req:?}").contains(&raw_token));
    assert!(!format!("{req:?}").contains(&req.material().lease_token().as_uuid().to_string()));
}

#[test]
fn hidden_acquire_is_bounded_strict_json_with_one_compact_response() {
    let temp = tempdir().unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(
            OsString::from("XDG_DATA_HOME"),
            temp.path().join("data").into_os_string(),
        )]),
        temp.path().join("home"),
        temp.path().to_path_buf(),
    );

    let mut valid_with_trailing = serde_json::to_vec(&request(1)).unwrap();
    valid_with_trailing.extend_from_slice(b" trailing");
    for input in [
        b"{} trailing".to_vec(),
        valid_with_trailing,
        vec![b' '; 1024 * 1024 + 1],
    ] {
        let cli = Cli::try_parse_from(["worker", "host", "lease-acquire"]).unwrap();
        let mut stdin = Cursor::new(input);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_stdio_in_context(
            cli,
            &NoProcess,
            &runtime,
            &mut stdin,
            &mut stdout,
            &mut stderr,
        );

        assert_ne!(exit, 0);
        assert!(stderr.is_empty());
        assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
        let response: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
        assert!(response.get("error").is_some());
        assert!(!String::from_utf8_lossy(&stdout).contains("lease_token"));
    }
}

#[test]
fn hidden_lease_acquire_failures_are_versioned_and_capacity_typed() {
    // Break caught: lease-acquire returns its legacy unversioned error object,
    // so a remote client cannot authoritatively preserve admission typing.
    let temp = tempdir().unwrap();
    let environment = BTreeMap::from([(
        OsString::from("XDG_DATA_HOME"),
        temp.path().join("data").into_os_string(),
    )]);
    let home = temp.path().join("home");
    let paths = PathLayout::discover(None, &environment, &home).unwrap();
    let runtime =
        RuntimeContext::isolated(environment, home, temp.path().join("PLANTED-HOST-PATH"));
    let store = HostStore::open(&paths.host_state_root()).unwrap();
    LeaseService::new(&store)
        .acquire(&request(80_000), &healthy(), 1)
        .unwrap();
    let contender = request(80_001);
    let planted_token = contender.material().lease_token().to_string();

    let cli = Cli::try_parse_from(["worker", "host", "lease-acquire"]).unwrap();
    let mut stdin = Cursor::new(serde_json::to_vec(&contender).unwrap());
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli,
        &NoProcess,
        &runtime,
        &mut stdin,
        &mut stdout,
        &mut stderr,
    );

    assert_eq!(exit, 75);
    assert!(stderr.is_empty());
    assert_eq!(stdout.last(), Some(&b'\n'));
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let error: HostControlError = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(error.protocol_version(), PROTOCOL_VERSION);
    assert_eq!(error.error().code(), "CAPACITY_BUSY");
    assert_eq!(error.error().message(), "worker admission rejected");
    assert_eq!(
        stdout,
        br#"{"protocol_version":2,"error":{"code":"CAPACITY_BUSY","message":"worker admission rejected"}}
"#
    );
    let rendered = String::from_utf8(stdout).unwrap();
    assert!(!rendered.contains(&planted_token));
    assert!(!rendered.contains("cargo"));
    assert!(!rendered.contains("PLANTED-HOST-PATH"));
}

#[test]
fn raw_lease_status_and_release_are_not_cli_operations() {
    assert!(Cli::try_parse_from(["worker", "host", "lease-status"]).is_err());
    assert!(Cli::try_parse_from(["worker", "host", "lease-release"]).is_err());
}
