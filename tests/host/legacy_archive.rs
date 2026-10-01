//! Exact persisted v1 fixtures for the surviving read, recovery and cleanup APIs.
//! These records are seeded without acquiring a Job lease or invoking a batch producer.

use std::{fs, os::unix::fs::PermissionsExt};

use mac_worker::test_support::host::{
    job::{ExecutionScope, JobMeta, JobStatus, LeaseAcquireRequest, LeaseRecord, SubmitRequest},
    legacy_snapshot_receipt::VerifiedReceipt,
    store::{HostStore, JobDisposition},
};
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    io::Write as _,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
};

struct ArchiveDirectory(PathBuf);

impl ArchiveDirectory {
    fn write_new_private_file(&self, name: &str, bytes: &[u8]) -> std::io::Result<File> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.0.join(name))?;
        file.write_all(bytes)?;
        Ok(file)
    }

    fn sync_root(&self) -> std::io::Result<()> {
        File::open(&self.0)?.sync_all()
    }
}

fn archive_directory(store: &HostStore, relative: &str, create: bool) -> ArchiveDirectory {
    let path = store.root().join(relative);
    if create {
        fs::create_dir_all(&path).unwrap();
        for ancestor in path.ancestors().take_while(|path| *path != store.root()) {
            fs::set_permissions(ancestor, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    ArchiveDirectory(path)
}
use sha2::{Digest, Sha256};

fn private_json<T: Serialize>(store: &HostStore, directory: &str, name: &str, value: &T) {
    let root = archive_directory(store, directory, true);
    root.write_new_private_file(name, &serde_json::to_vec(value).unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
    root.sync_root().unwrap();
}

pub(super) fn lease(store: &HostStore, request: &LeaseAcquireRequest, now: u64) -> LeaseRecord {
    let lease = LeaseRecord::new(
        request.material(),
        request.request_fingerprint().clone(),
        now,
        now + request.material().timeout_millis(),
    )
    .unwrap();
    let slots = mac_worker::test_support::host::lease::LeaseService::new(store)
        .occupied_slots()
        .unwrap();
    let slot = (0..=u8::MAX)
        .find(|id| !slots.iter().any(|slot| slot.slot_id == *id))
        .unwrap();
    let directory = format!("leases/slots/{slot}");
    private_json(store, &directory, "lease.json", &lease);
    private_json(store, &directory, "scope.json", &ExecutionScope::Job);
    assert_eq!(
        mac_worker::test_support::host::lease::LeaseService::new(store)
            .load_for_job(lease.job_id())
            .unwrap(),
        Some(lease.clone())
    );
    lease
}

pub(super) fn snapshot(store: &HostStore, lease: &LeaseRecord, manifest: &[u8], now: u64) {
    let cache = store
        .snapshot(
            lease.project_id(),
            lease.worktree_id(),
            lease.manifest_digest(),
        )
        .unwrap();
    if !cache.exists() {
        archive_directory(
            store,
            &format!("snapshots/{}/{}", lease.project_id(), lease.worktree_id()),
            true,
        );
        fs::create_dir(&cache).unwrap();
        fs::create_dir(cache.join("tree")).unwrap();
        fs::write(cache.join("manifest.json"), manifest).unwrap();
        fs::write(cache.join("tree/payload.txt"), b"payload").unwrap();
        for (path, mode) in [
            (cache.join("manifest.json"), 0o400),
            (cache.join("tree/payload.txt"), 0o400),
            (cache.join("tree"), 0o500),
            (cache.clone(), 0o500),
        ] {
            fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
        }
    }
    let receipt: VerifiedReceipt = serde_json::from_value(serde_json::json!({
        "version": 1,
        "job_id": lease.job_id(),
        "client_id": lease.client_id(),
        "lease_token_sha256": format!("{:x}", Sha256::digest(lease.lease_token().to_string().as_bytes())),
        "request_fingerprint": lease.request_fingerprint(),
        "project_id": lease.project_id(),
        "worktree_id": lease.worktree_id(),
        "manifest_digest": lease.manifest_digest(),
        "cache_key": {
            "project_id": lease.project_id(),
            "worktree_id": lease.worktree_id(),
            "manifest_digest": lease.manifest_digest(),
        },
        "verified_at_millis": now,
    }))
    .unwrap();
    private_json(
        store,
        "verified",
        &format!("{}.json", lease.job_id()),
        &receipt,
    );
    // Cleanup tests retain the ordinary empty incoming job left by promotion.
    archive_directory(store, &format!("incoming/{}", lease.job_id()), true);
}

pub(super) fn accepted(store: &HostStore, request: &SubmitRequest, now: u64, indexed: bool) {
    #[derive(Serialize)]
    struct LegacyExecution<'a> {
        version: u32,
        job_id: mac_worker::test_support::host::job::JobId,
        client_id: mac_worker::test_support::host::job::ClientId,
        request_fingerprint: &'a mac_worker::test_support::host::job::RequestFingerprint,
        lease_token: mac_worker::test_support::host::job::LeaseToken,
        command: &'a mac_worker::test_support::host::job::CommandSpec,
        turn: Option<()>,
    }
    let material = request.material();
    let directory = format!(
        "jobs/{}/{}/{}",
        material.project_id(),
        material.worktree_id(),
        material.job_id()
    );
    let root = archive_directory(store, &directory, true);
    let status = JobStatus::accepted(material.created_at_millis()).unwrap();
    private_json(
        store,
        &directory,
        "meta.json",
        &JobMeta::new(material, request.request_fingerprint().clone()).unwrap(),
    );
    private_json(store, &directory, "status.json", &status);
    private_json(
        store,
        &directory,
        "execution.json",
        &LegacyExecution {
            version: 2,
            job_id: material.job_id(),
            client_id: material.client_id(),
            request_fingerprint: request.request_fingerprint(),
            lease_token: material.lease_token(),
            command: material.command(),
            turn: None,
        },
    );
    for name in ["stdout.log", "stderr.log"] {
        root.write_new_private_file(name, b"")
            .unwrap()
            .sync_all()
            .unwrap();
    }
    for name in ["home", "tmp", "workspace", "workspace/tree"] {
        archive_directory(store, &format!("{directory}/{name}"), true);
    }
    let cache = store
        .snapshot(
            material.project_id(),
            material.worktree_id(),
            material.manifest_digest(),
        )
        .unwrap();
    let workspace = archive_directory(store, &format!("{directory}/workspace"), false);
    workspace
        .write_new_private_file(
            "manifest.json",
            &fs::read(cache.join("manifest.json")).unwrap(),
        )
        .unwrap();
    let tree = archive_directory(store, &format!("{directory}/workspace/tree"), false);
    tree.write_new_private_file("payload.txt", b"payload")
        .unwrap();
    root.sync_root().unwrap();
    if indexed {
        private_json(
            store,
            "job-index",
            &format!("{}.json", material.job_id()),
            &JobDisposition::Accepted {
                job_id: material.job_id(),
                client_id: material.client_id(),
                project_id: material.project_id().into(),
                worktree_id: material.worktree_id().into(),
                request_fingerprint: request.request_fingerprint().clone(),
                status,
                recorded_at_millis: now,
            },
        );
        let bytes = fs::read(store.job_index(material.job_id()).unwrap()).unwrap();
        let disposition: JobDisposition = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(serde_json::to_vec(&disposition).unwrap(), bytes);
    }
}
