use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    io::{self, Cursor, Write},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::Path,
    process::ExitStatus,
    sync::{Arc, Barrier, Mutex, mpsc},
    thread,
    time::Duration,
};

use clap::Parser;
use mac_worker::{
    RuntimeContext,
    cli::Cli,
    config::WorkerEntry,
    error::WorkerError,
    host_store::{HostStore, HostStoreWritePoint},
    job::{
        ClientId, CommandSpec, HostControlError, JobId, JobStatus, LeaseAcquireRequest,
        LeaseAcquireResponse, LeaseRecord, LeaseToken, RequestFingerprintMaterial,
    },
    lease::{AdmissionFacts, LeaseService},
    manifest::{ManifestEntry, ManifestEntryKind, SnapshotManifest},
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::{MemoryPressure, PROTOCOL_VERSION},
    remote_snapshot::{SnapshotVerifyRequest, VerifiedReceipt, VerifiedSnapshotResponse},
    run_with_stdio_in_context,
    transfer::{
        HostOperation, HostTransferService, RsyncServerExecutor, RsyncServerInvocation,
        SshJsonTransport, TransferIdentity,
    },
};
use sha2::{Digest, Sha256};

const JOB_ID: &str = "00000000000000000000000000000001";
const CLIENT_ID: &str = "00000000000000000000000000000002";
const LEASE_TOKEN: &str = "00000000000000000000000000000003";
const FINGERPRINT: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MANIFEST_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const TOKEN_HASH: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

struct NoProcess;

struct BrokenWriter;

struct RecordingControlRunner {
    response: Vec<u8>,
    request: Mutex<Option<ProcessRequest>>,
}

impl RecordingControlRunner {
    fn returning(response: Vec<u8>) -> Self {
        Self {
            response,
            request: Mutex::new(None),
        }
    }

    fn request(&self) -> ProcessRequest {
        self.request
            .lock()
            .unwrap()
            .clone()
            .expect("the control request must be recorded")
    }
}

impl ProcessRunner for NoProcess {
    fn run(&self, _request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!("snapshot verification must not launch a process")
    }
}

impl ProcessRunner for RecordingControlRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        *self.request.lock().unwrap() = Some(request.clone());
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: self.response.clone(),
            stderr: Vec::new(),
        })
    }
}

impl Write for BrokenWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "writer closed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn valid_manifest_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"version":1,"project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""head":null,"branch":null,"dirty":false,"relative_working_dir":"","#,
            r#""entries":[{{"path":"payload.txt","kind":"file","mode":420,"size":7,"#,
            r#""sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","#,
            r#""symlink_target":null}}],"tracked_deletions":[]}}"#,
        ),
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
    )
    .into_bytes()
}

fn file_entry(path: &str, bytes: &[u8], executable: bool) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        kind: ManifestEntryKind::File,
        mode: if executable { 0o755 } else { 0o644 },
        size: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        symlink_target: None,
    }
}

fn directory_entry(path: &str) -> ManifestEntry {
    ManifestEntry {
        path: path.into(),
        kind: ManifestEntryKind::Directory,
        mode: 0o755,
        size: 0,
        sha256: format!("{:x}", Sha256::digest(b"directory\0")),
        symlink_target: None,
    }
}

fn symlink_entry(path: &str, target: &str) -> ManifestEntry {
    let mut hasher = Sha256::new();
    hasher.update(b"symlink\0");
    hasher.update(target.as_bytes());
    ManifestEntry {
        path: path.into(),
        kind: ManifestEntryKind::Symlink,
        mode: 0o777,
        size: target.len() as u64,
        sha256: format!("{:x}", hasher.finalize()),
        symlink_target: Some(target.into()),
    }
}

fn verify_request_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"protocol_version":{PROTOCOL_VERSION},"job_id":"{JOB_ID}","client_id":"{CLIENT_ID}","#,
            r#""lease_token":"{LEASE_TOKEN}","request_fingerprint":"{FINGERPRINT}","#,
            r#""project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""manifest_digest":"{MANIFEST_DIGEST}"}}"#,
        ),
        PROTOCOL_VERSION = PROTOCOL_VERSION,
        JOB_ID = JOB_ID,
        CLIENT_ID = CLIENT_ID,
        LEASE_TOKEN = LEASE_TOKEN,
        FINGERPRINT = FINGERPRINT,
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
        MANIFEST_DIGEST = MANIFEST_DIGEST,
    )
    .into_bytes()
}

fn receipt_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"version":1,"job_id":"{JOB_ID}","client_id":"{CLIENT_ID}","#,
            r#""lease_token_sha256":"{TOKEN_HASH}","request_fingerprint":"{FINGERPRINT}","#,
            r#""project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""manifest_digest":"{MANIFEST_DIGEST}","cache_key":{{"project_id":"{PROJECT_ID}","#,
            r#""worktree_id":"{WORKTREE_ID}","manifest_digest":"{MANIFEST_DIGEST}"}},"#,
            r#""verified_at_millis":42}}"#,
        ),
        JOB_ID = JOB_ID,
        CLIENT_ID = CLIENT_ID,
        TOKEN_HASH = TOKEN_HASH,
        FINGERPRINT = FINGERPRINT,
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
        MANIFEST_DIGEST = MANIFEST_DIGEST,
    )
    .into_bytes()
}

fn response_bytes() -> Vec<u8> {
    format!(
        concat!(
            r#"{{"protocol_version":{PROTOCOL_VERSION},"job_id":"{JOB_ID}","client_id":"{CLIENT_ID}","#,
            r#""project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""manifest_digest":"{MANIFEST_DIGEST}","verified_at_millis":42,"cache_reused":false}}"#,
        ),
        PROTOCOL_VERSION = PROTOCOL_VERSION,
        JOB_ID = JOB_ID,
        CLIENT_ID = CLIENT_ID,
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
        MANIFEST_DIGEST = MANIFEST_DIGEST,
    )
    .into_bytes()
}

fn insert_before_final_brace(bytes: &[u8], insertion: &[u8]) -> Vec<u8> {
    let mut changed = bytes[..bytes.len() - 1].to_vec();
    changed.extend_from_slice(insertion);
    changed.push(b'}');
    changed
}

#[test]
fn manifest_request_receipt_and_response_reject_unknown_duplicate_and_invalid_fields() {
    // Break caught: an ambiguous or semantically invalid remote record is
    // accepted merely because serde can populate its Rust fields.
    let manifest: SnapshotManifest = serde_json::from_slice(&valid_manifest_bytes()).unwrap();
    assert_eq!(manifest.project_id, PROJECT_ID);

    let unknown_manifest = insert_before_final_brace(&valid_manifest_bytes(), b",\"extra\":1");
    assert!(serde_json::from_slice::<SnapshotManifest>(&unknown_manifest).is_err());

    let request: SnapshotVerifyRequest = serde_json::from_slice(&verify_request_bytes()).unwrap();
    assert_eq!(
        serde_json::to_vec(&request).unwrap(),
        verify_request_bytes()
    );
    assert!(!format!("{request:?}").contains(LEASE_TOKEN));
    let duplicate_request = insert_before_final_brace(
        &verify_request_bytes(),
        format!(r#",\"job_id\":\"{JOB_ID}\""#).as_bytes(),
    );
    assert!(serde_json::from_slice::<SnapshotVerifyRequest>(&duplicate_request).is_err());
    let unknown_request = insert_before_final_brace(&verify_request_bytes(), b",\"path\":\"/tmp\"");
    assert!(serde_json::from_slice::<SnapshotVerifyRequest>(&unknown_request).is_err());
    let current_version = format!("\"protocol_version\":{PROTOCOL_VERSION}");
    let invalid_version =
        verify_request_bytes().replace_bytes(current_version.as_bytes(), b"\"protocol_version\":1");
    assert!(serde_json::from_slice::<SnapshotVerifyRequest>(&invalid_version).is_err());

    let receipt: VerifiedReceipt = serde_json::from_slice(&receipt_bytes()).unwrap();
    assert_eq!(serde_json::to_vec(&receipt).unwrap(), receipt_bytes());
    let mismatched_cache_key = receipt_bytes().replace_bytes(
        format!(r#""manifest_digest":"{MANIFEST_DIGEST}"}}"#).as_bytes(),
        format!(r#""manifest_digest":"{}"}}"#, "f".repeat(64)).as_bytes(),
    );
    assert!(serde_json::from_slice::<VerifiedReceipt>(&mismatched_cache_key).is_err());
    let unknown_receipt =
        insert_before_final_brace(&receipt_bytes(), b",\"lease_token\":\"secret\"");
    assert!(serde_json::from_slice::<VerifiedReceipt>(&unknown_receipt).is_err());

    let response: VerifiedSnapshotResponse = serde_json::from_slice(&response_bytes()).unwrap();
    assert_eq!(serde_json::to_vec(&response).unwrap(), response_bytes());
    let invalid_response = response_bytes().replace_bytes(
        format!(r#""manifest_digest":"{MANIFEST_DIGEST}""#).as_bytes(),
        br#""manifest_digest":"not-a-digest""#,
    );
    assert!(serde_json::from_slice::<VerifiedSnapshotResponse>(&invalid_response).is_err());
}

trait ReplaceBytes {
    fn replace_bytes(&self, from: &[u8], to: &[u8]) -> Vec<u8>;
}

impl ReplaceBytes for [u8] {
    fn replace_bytes(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
        let offset = self
            .windows(from.len())
            .position(|window| window == from)
            .expect("fixture needle");
        [&self[..offset], to, &self[offset + from.len()..]].concat()
    }
}

fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * 1024 * 1024 * 1024,
        total_disk_bytes: 250 * 1024 * 1024 * 1024,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

fn acquired_bundle(root: &Path) -> (HostStore, LeaseRecord, String) {
    acquired_bundle_bytes(root, valid_manifest_bytes())
}

fn request_for_digest(digest: &str) -> LeaseAcquireRequest {
    LeaseAcquireRequest::new(
        RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(1)),
            ClientId::new(uuid::Uuid::from_u128(2)),
            LeaseToken::new(uuid::Uuid::from_u128(3)),
            1,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            digest.into(),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap(),
    )
}

fn acquired_bundle_bytes(root: &Path, manifest_bytes: Vec<u8>) -> (HostStore, LeaseRecord, String) {
    let store = HostStore::open(root).unwrap();
    let digest = format!("{:x}", Sha256::digest(&manifest_bytes));
    let request = request_for_digest(&digest);
    let lease = match LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(incoming.join("tree")).unwrap();
    for private in [
        incoming.parent().unwrap().parent().unwrap(),
        incoming.parent().unwrap(),
        incoming.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(incoming.join("manifest.json"), manifest_bytes).unwrap();
    fs::write(incoming.join("tree/payload.txt"), b"payload").unwrap();
    fs::set_permissions(
        incoming.join("manifest.json"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/payload.txt"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(incoming.join("tree"), fs::Permissions::from_mode(0o555)).unwrap();
    (store, lease, digest)
}

fn acquired_complex_bundle(root: &Path) -> (HostStore, LeaseRecord, String) {
    use std::os::unix::fs::symlink;

    const EXECUTABLE: &[u8] = b"#!/bin/sh\n";
    const UNUSUAL: &[u8] = b"unicode-newline";
    let unusual_path = "nested/β\nname.txt";
    let manifest = SnapshotManifest {
        version: 1,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        head: None,
        branch: Some("main".into()),
        dirty: true,
        relative_working_dir: "empty".into(),
        entries: vec![
            directory_entry("bin"),
            file_entry("bin/tool", EXECUTABLE, true),
            directory_entry("empty"),
            file_entry(unusual_path, UNUSUAL, false),
            symlink_entry("tool-link", "bin/tool"),
        ],
        tracked_deletions: vec!["deleted.txt".into()],
    };
    let manifest_bytes = manifest.canonical_bytes().unwrap();
    let digest = format!("{:x}", Sha256::digest(&manifest_bytes));
    let store = HostStore::open(root).unwrap();
    let request = request_for_digest(&digest);
    let lease = match LeaseService::new(&store)
        .acquire(&request, &healthy(), 1)
        .unwrap()
    {
        LeaseAcquireResponse::Acquired { lease } => lease,
        LeaseAcquireResponse::ExistingAccepted { .. } => unreachable!(),
    };
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    fs::create_dir_all(incoming.join("tree/bin")).unwrap();
    fs::create_dir(incoming.join("tree/empty")).unwrap();
    fs::create_dir(incoming.join("tree/nested")).unwrap();
    fs::write(incoming.join("manifest.json"), manifest_bytes).unwrap();
    fs::write(incoming.join("tree/bin/tool"), EXECUTABLE).unwrap();
    fs::write(incoming.join("tree").join(unusual_path), UNUSUAL).unwrap();
    symlink("bin/tool", incoming.join("tree/tool-link")).unwrap();
    for private in [
        incoming.parent().unwrap().parent().unwrap(),
        incoming.parent().unwrap(),
        incoming.as_path(),
    ] {
        fs::set_permissions(private, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::set_permissions(
        incoming.join("manifest.json"),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree/bin/tool"),
        fs::Permissions::from_mode(0o555),
    )
    .unwrap();
    fs::set_permissions(
        incoming.join("tree").join(unusual_path),
        fs::Permissions::from_mode(0o444),
    )
    .unwrap();
    for directory in ["tree/bin", "tree/empty", "tree/nested", "tree"] {
        fs::set_permissions(incoming.join(directory), fs::Permissions::from_mode(0o555)).unwrap();
    }
    (store, lease, digest)
}

fn stock_server_args() -> Vec<OsString> {
    [
        "--server",
        "--delete-before",
        "-l",
        "-p",
        "-D",
        "-r",
        "-t",
        "--dirs",
        ".",
        "incoming",
    ]
    .into_iter()
    .map(OsString::from)
    .collect()
}

struct HoldReceiver {
    entered: mpsc::Sender<mpsc::Sender<()>>,
}

impl RsyncServerExecutor for HoldReceiver {
    fn execute(&self, _invocation: RsyncServerInvocation<'_>) -> Result<(), WorkerError> {
        let (release, wait) = mpsc::channel();
        self.entered.send(release).unwrap();
        wait.recv_timeout(Duration::from_secs(5))
            .map_err(|_| WorkerError::Protocol("test receiver release timed out".into()))
    }
}

fn mutated_lease(lease: &LeaseRecord, field: &str) -> LeaseRecord {
    let mut wire = serde_json::to_value(lease).unwrap();
    wire[field] = match field {
        "lease_token" => {
            serde_json::Value::String(LeaseToken::new(uuid::Uuid::from_u128(999)).to_string())
        }
        "request_fingerprint" => serde_json::Value::String("f".repeat(64)),
        "client_id" => {
            serde_json::Value::String(ClientId::new(uuid::Uuid::from_u128(999)).to_string())
        }
        "project_id" => serde_json::Value::String("e".repeat(64)),
        "worktree_id" => serde_json::Value::String("f".repeat(64)),
        "manifest_digest" => serde_json::Value::String("0".repeat(64)),
        _ => unreachable!(),
    };
    serde_json::from_value(wire).unwrap()
}

fn replace_read_only_file(path: &Path, bytes: &[u8]) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o444)).unwrap();
}

fn make_owned_tree_writable(path: &Path) {
    let metadata = fs::symlink_metadata(path).unwrap();
    if metadata.file_type().is_symlink() {
        return;
    }
    if metadata.is_dir() {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        for entry in fs::read_dir(path).unwrap() {
            make_owned_tree_writable(&entry.unwrap().path());
        }
    } else {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn assert_no_workspace_entry(path: &Path, row: usize) {
    let mode = fs::symlink_metadata(path).unwrap().permissions().mode();
    if mode & 0o500 != 0o500 {
        fs::set_permissions(path, fs::Permissions::from_mode(mode | 0o500)).unwrap();
    }
    for entry in fs::read_dir(path).unwrap_or_else(|error| {
        panic!(
            "row {row} could not inspect {} for workspaces: {error}",
            path.display()
        )
    }) {
        let entry = entry.unwrap();
        let entry_path = entry.path();
        assert_ne!(
            entry.file_name(),
            OsString::from("workspace"),
            "row {row} created a workspace at {}",
            entry_path.display()
        );
        if fs::symlink_metadata(&entry_path).unwrap().is_dir() {
            assert_no_workspace_entry(&entry_path, row);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn assert_matrix_row_rejected(
    store: &HostStore,
    lease: &LeaseRecord,
    digest: &str,
    host_root: &Path,
    sentinel: &Path,
    sentinel_bytes: &[u8],
    marker: &str,
    row: usize,
) {
    let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(store)
        .verify_and_promote_at(lease, digest, 42)
        .unwrap_err();
    let diagnostic = error.to_string();
    assert!(
        [
            "snapshot error [MANIFEST_MISMATCH]: remote snapshot does not match its canonical manifest",
            "snapshot error [UNSAFE_REMOTE_SNAPSHOT]: remote snapshot filesystem state is unsafe",
        ]
        .contains(&diagnostic.as_str()),
        "row {row}: {diagnostic}"
    );
    assert!(
        !store
            .snapshot(PROJECT_ID, WORKTREE_ID, digest)
            .unwrap()
            .exists(),
        "row {row} published a cache"
    );
    assert!(
        !store.verified_receipt(lease.job_id()).unwrap().exists(),
        "row {row} published a verified receipt"
    );
    assert!(
        !store.job_index(lease.job_id()).unwrap().exists(),
        "row {row} published an Accepted index"
    );
    assert!(
        !store
            .job(PROJECT_ID, WORKTREE_ID, lease.job_id())
            .unwrap()
            .exists(),
        "row {row} published a final job"
    );
    assert_no_workspace_entry(host_root, row);
    assert_eq!(
        fs::read(sentinel).unwrap(),
        sentinel_bytes,
        "row {row} changed the external sentinel"
    );
    for private in [
        marker,
        LEASE_TOKEN,
        host_root.to_string_lossy().as_ref(),
        sentinel.to_string_lossy().as_ref(),
    ] {
        assert!(
            !diagnostic.contains(private),
            "row {row} emitted content-bearing diagnostic material"
        );
    }
}

#[test]
fn semantic_manifest_and_live_tree_mutation_matrix_100_never_publishes() {
    use std::ffi::CString;
    use std::os::unix::fs::symlink;

    let mut executed_rows = 0;
    for row in 0..100 {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let marker = format!("PLANTED-REMOTE-SNAPSHOT-ROW-{row:03}");
        let sentinel = fixture.path().join(format!("outside-sentinel-{row:03}"));
        let sentinel_bytes = format!("outside-content-{marker}").into_bytes();
        fs::write(&sentinel, &sentinel_bytes).unwrap();

        let path = if row < 50 {
            "payload.txt".to_owned()
        } else {
            format!("payload-{row:03}-{marker}.txt")
        };
        let mut manifest = SnapshotManifest {
            version: 1,
            project_id: PROJECT_ID.into(),
            worktree_id: WORKTREE_ID.into(),
            head: None,
            branch: Some(marker.clone()),
            dirty: true,
            relative_working_dir: String::new(),
            entries: vec![file_entry(&path, b"payload", false)],
            tracked_deletions: Vec::new(),
        };

        if row < 50 {
            let slot = row % 5;
            match row / 5 {
                0 => manifest.version = [0, 2, 3, 42, u32::MAX][slot],
                1 => {
                    manifest.head = Some(match slot {
                        0 => String::new(),
                        1 => "0".repeat(39),
                        2 => "0".repeat(41),
                        3 => "A".repeat(40),
                        4 => "g".repeat(64),
                        _ => unreachable!(),
                    })
                }
                2 => {
                    manifest.branch = Some(match slot {
                        0 => String::new(),
                        1 => "\0".into(),
                        2 => format!("{marker}\0suffix"),
                        3 => format!("prefix\0{marker}"),
                        4 => marker.repeat(4_682),
                        _ => unreachable!(),
                    })
                }
                3 => {
                    manifest.entries[0].path = match slot {
                        0 => format!("/{marker}"),
                        1 => format!("../{marker}"),
                        2 => format!("nested/../../{marker}"),
                        3 => format!("nested/./{marker}"),
                        4 => format!("nested\\{marker}"),
                        _ => unreachable!(),
                    }
                }
                4 => manifest.entries[0].mode = [0, 0o600, 0o700, 0o744, 0o777][slot],
                5 => {
                    let wrong_bytes = format!("valid-lower-hex-digest-mismatch-{slot}-{marker}");
                    let wrong_digest = format!("{:x}", Sha256::digest(wrong_bytes));
                    assert_eq!(wrong_digest.len(), 64);
                    assert!(
                        wrong_digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    );
                    assert_ne!(wrong_digest, manifest.entries[0].sha256);
                    manifest.entries[0].sha256 = wrong_digest;
                }
                6 => match slot {
                    0 => manifest.entries[0].symlink_target = Some(String::new()),
                    1 => manifest.entries[0].symlink_target = Some(marker.clone()),
                    2 => {
                        manifest.entries[0].mode = 0o755;
                        manifest.entries[0].symlink_target = Some(format!("../{marker}"));
                    }
                    3 => {
                        manifest.entries[0].size = 0;
                        manifest.entries[0].symlink_target = Some(format!("{marker}\0target"));
                    }
                    4 => {
                        manifest.entries[0].sha256 =
                            format!("{:x}", Sha256::digest(marker.as_bytes()));
                        manifest.entries[0].symlink_target = Some(marker.repeat(2_341));
                    }
                    _ => unreachable!(),
                },
                7 => {
                    let duplicate = manifest.entries[0].clone();
                    manifest
                        .entries
                        .extend(std::iter::repeat_n(duplicate, slot + 1));
                }
                8 => {
                    manifest.entries[0].path = match slot {
                        0 => "payload.txt".into(),
                        1 => "nested/payload.txt".into(),
                        2 => "β/payload.txt".into(),
                        3 => "space name/payload.txt".into(),
                        4 => "line\nbreak/payload.txt".into(),
                        _ => unreachable!(),
                    };
                    manifest
                        .tracked_deletions
                        .push(manifest.entries[0].path.clone());
                }
                9 => {
                    manifest.relative_working_dir = match slot {
                        0 => format!("missing-{marker}"),
                        1 => "payload.txt".into(),
                        2 => format!("/{marker}"),
                        3 => format!("./{marker}"),
                        4 => format!(".git/{marker}"),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
            let bytes = serde_json::to_vec(&manifest).unwrap();
            let (store, lease, digest) = acquired_bundle_bytes(&host_root, bytes);
            assert_matrix_row_rejected(
                &store,
                &lease,
                &digest,
                &host_root,
                &sentinel,
                &sentinel_bytes,
                &marker,
                row,
            );
        } else {
            let live_class = (row - 50) / 5;
            let slot = (row - 50) % 5;
            if live_class == 9 && slot >= 2 {
                manifest.entries[0].mode = 0o755;
            }
            let bytes = manifest.canonical_bytes().unwrap();
            let (store, lease, digest) = acquired_bundle_bytes(&host_root, bytes);
            let incoming = store
                .incoming_job(lease.job_id(), lease.lease_token())
                .unwrap();
            let tree = incoming.join("tree");
            let target = tree.join(&path);
            fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
            fs::remove_file(tree.join("payload.txt")).unwrap();
            fs::write(&target, b"payload").unwrap();
            fs::set_permissions(&target, fs::Permissions::from_mode(0o444)).unwrap();
            fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();

            match live_class {
                0 => {
                    let changed = match slot {
                        0 => Vec::new(),
                        1 => b"payloae".to_vec(),
                        2 => b"payload-with-trailing-bytes".to_vec(),
                        3 => vec![0, 0xff, b'\n', b'\r', 0x80],
                        4 => marker.repeat(257).into_bytes(),
                        _ => unreachable!(),
                    };
                    replace_read_only_file(&target, &changed);
                }
                1 => {
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                    match slot {
                        0 => fs::remove_file(&target).unwrap(),
                        1 => fs::rename(&target, tree.join(format!("renamed-{marker}"))).unwrap(),
                        2 => {
                            fs::remove_file(&target).unwrap();
                            fs::create_dir(tree.join(format!("empty-{marker}"))).unwrap();
                        }
                        3 => {
                            fs::remove_file(&target).unwrap();
                            fs::write(tree.join(format!("replacement-{marker}")), b"replacement")
                                .unwrap();
                        }
                        4 => {
                            fs::remove_file(&target).unwrap();
                            symlink(&sentinel, tree.join(format!("replacement-{marker}"))).unwrap();
                        }
                        _ => unreachable!(),
                    }
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
                }
                2 => {
                    fs::set_permissions(
                        &target,
                        fs::Permissions::from_mode([0o200, 0o600, 0o640, 0o644, 0o700][slot]),
                    )
                    .unwrap();
                }
                3 => {
                    let extra = tree.join(format!("extra-{slot}-{marker}"));
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                    match slot {
                        0 => {
                            fs::write(&extra, marker.as_bytes()).unwrap();
                            fs::set_permissions(&extra, fs::Permissions::from_mode(0o444)).unwrap();
                        }
                        1 => {
                            fs::create_dir(&extra).unwrap();
                            fs::set_permissions(&extra, fs::Permissions::from_mode(0o555)).unwrap();
                        }
                        2 => symlink(&sentinel, &extra).unwrap(),
                        3 => {
                            let fifo = CString::new(extra.as_os_str().as_encoded_bytes()).unwrap();
                            assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o444) }, 0);
                        }
                        4 => {
                            fs::create_dir(&extra).unwrap();
                            fs::write(extra.join("nested"), marker.as_bytes()).unwrap();
                            fs::set_permissions(
                                extra.join("nested"),
                                fs::Permissions::from_mode(0o444),
                            )
                            .unwrap();
                            fs::set_permissions(&extra, fs::Permissions::from_mode(0o555)).unwrap();
                        }
                        _ => unreachable!(),
                    }
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
                }
                4 => {
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                    fs::remove_file(&target).unwrap();
                    match slot {
                        0 => {
                            fs::create_dir(&target).unwrap();
                            fs::set_permissions(&target, fs::Permissions::from_mode(0o555))
                                .unwrap();
                        }
                        1 => symlink(&sentinel, &target).unwrap(),
                        2 => symlink(fixture.path().join("missing"), &target).unwrap(),
                        3 => {
                            let fifo = CString::new(target.as_os_str().as_encoded_bytes()).unwrap();
                            assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o444) }, 0);
                        }
                        4 => symlink(".", &target).unwrap(),
                        _ => unreachable!(),
                    }
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
                }
                5 => {
                    for alias in 0..=slot {
                        fs::hard_link(
                            &target,
                            fixture.path().join(format!("alias-{slot}-{alias}")),
                        )
                        .unwrap();
                    }
                }
                6 => {
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                    fs::remove_file(&target).unwrap();
                    let link_target = match slot {
                        0 => sentinel.clone(),
                        1 => fixture.path().to_path_buf(),
                        2 => fixture.path().join(format!("missing-{marker}")),
                        3 => Path::new("../..").join(format!("outside-{marker}")),
                        4 => Path::new(&path).to_path_buf(),
                        _ => unreachable!(),
                    };
                    symlink(link_target, &target).unwrap();
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
                }
                7 => {
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                    fs::remove_file(&target).unwrap();
                    let fifo = CString::new(target.as_os_str().as_encoded_bytes()).unwrap();
                    assert_eq!(
                        unsafe {
                            libc::mkfifo(fifo.as_ptr(), [0o400, 0o440, 0o444, 0o500, 0o555][slot])
                        },
                        0
                    );
                    fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
                }
                8 => {
                    fs::set_permissions(
                        &tree,
                        fs::Permissions::from_mode([0o600, 0o700, 0o711, 0o755, 0o777][slot]),
                    )
                    .unwrap();
                }
                9 => {
                    fs::set_permissions(
                        &target,
                        fs::Permissions::from_mode([0o500, 0o555, 0o400, 0o444, 0o455][slot]),
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            assert_matrix_row_rejected(
                &store,
                &lease,
                &digest,
                &host_root,
                &sentinel,
                &sentinel_bytes,
                &marker,
                row,
            );
        }
        executed_rows += 1;
    }
    assert_eq!(executed_rows, 100);
}

#[test]
fn canonical_exact_bundle_is_verified_and_promoted_owner_only() {
    // Break caught: verification trusts manifest text or paths without
    // descriptor-bound tree verification and immutable cache publication.
    let fixture = tempfile::tempdir().unwrap();
    let (store, lease, digest) = acquired_bundle(&fixture.path().join("host"));

    let snapshot = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 42)
        .unwrap();

    assert_eq!(snapshot.project_id(), PROJECT_ID);
    assert_eq!(snapshot.worktree_id(), WORKTREE_ID);
    assert_eq!(snapshot.digest(), digest);
    assert_eq!(snapshot.manifest().entries.len(), 1);
    let cache = store.snapshot(PROJECT_ID, WORKTREE_ID, &digest).unwrap();
    assert_eq!(
        fs::metadata(&cache).unwrap().permissions().mode() & 0o777,
        0o500
    );
    assert_eq!(
        fs::metadata(cache.join("tree"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o500
    );
    for file in [cache.join("manifest.json"), cache.join("tree/payload.txt")] {
        assert_eq!(
            fs::metadata(file).unwrap().permissions().mode() & 0o777,
            0o400
        );
    }
    assert!(
        !store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap()
            .exists()
    );
    let receipt_bytes = fs::read(store.verified_receipt(lease.job_id()).unwrap()).unwrap();
    let receipt: VerifiedReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
    assert_eq!(receipt.verified_at_millis(), 42);
    assert!(
        !String::from_utf8(receipt_bytes)
            .unwrap()
            .contains(LEASE_TOKEN)
    );
}

#[test]
fn verified_snapshot_debug_omits_manifest_targets_tokens_and_host_paths() {
    use std::os::unix::fs::symlink;

    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let target = fixture.path().join("outside-secret");
    let target_text = target.to_str().unwrap();
    let manifest = SnapshotManifest {
        version: 1,
        project_id: PROJECT_ID.into(),
        worktree_id: WORKTREE_ID.into(),
        head: None,
        branch: None,
        dirty: false,
        relative_working_dir: String::new(),
        entries: vec![symlink_entry("link", target_text)],
        tracked_deletions: Vec::new(),
    };
    let (store, lease, digest) =
        acquired_bundle_bytes(&host_root, manifest.canonical_bytes().unwrap());
    let incoming = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap();
    let tree = incoming.join("tree");
    fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
    fs::remove_file(tree.join("payload.txt")).unwrap();
    symlink(target_text, tree.join("link")).unwrap();
    fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();

    let snapshot = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 42)
        .unwrap();
    let debug = format!("{snapshot:?}");

    assert!(!debug.contains(target_text));
    assert!(!debug.contains(LEASE_TOKEN));
    assert!(!debug.contains(host_root.to_string_lossy().as_ref()));
}

#[test]
fn fresh_helper_revalidates_cache_and_rejects_a_hard_linked_receipt() {
    // Break caught: fresh-process loading trusts receipt JSON while its
    // supposedly immutable inode has another mutable name.
    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_bundle(&host_root);
    mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 42)
        .unwrap();
    drop(store);

    let reopened = HostStore::open(&host_root).unwrap();
    mac_worker::remote_snapshot::RemoteSnapshotService::new(&reopened)
        .load_verified(&lease, lease.request_fingerprint())
        .unwrap();
    drop(reopened);

    let receipt = host_root
        .join("verified")
        .join(format!("{}.json", lease.job_id()));
    fs::hard_link(&receipt, host_root.join("verified/receipt-alias")).unwrap();
    let reopened = HostStore::open(&host_root).unwrap();
    let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&reopened)
        .load_verified(&lease, lease.request_fingerprint())
        .unwrap_err();
    let diagnostic = error.to_string();
    assert!(diagnostic.contains("UNSAFE_REMOTE_SNAPSHOT"));
    assert!(!diagnostic.contains(LEASE_TOKEN));
    assert!(!diagnostic.contains(host_root.to_string_lossy().as_ref()));
}

#[test]
fn fresh_helpers_fail_closed_for_mutated_or_incomplete_verified_state() {
    use std::os::unix::fs::symlink;

    for case in [
        "cache-writable",
        "cache-bytes",
        "cache-missing",
        "cache-symlink",
        "receipt-mismatch",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let outside = fixture.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("sentinel"), b"keep").unwrap();
        let (store, lease, digest) = acquired_bundle(&host_root);
        mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&lease, &digest, 42)
            .unwrap();
        let cache = store.snapshot(PROJECT_ID, WORKTREE_ID, &digest).unwrap();
        let receipt = store.verified_receipt(lease.job_id()).unwrap();
        drop(store);

        match case {
            "cache-writable" => fs::set_permissions(
                cache.join("tree/payload.txt"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap(),
            "cache-bytes" => {
                let payload = cache.join("tree/payload.txt");
                fs::set_permissions(&payload, fs::Permissions::from_mode(0o600)).unwrap();
                fs::write(&payload, b"changed").unwrap();
                fs::set_permissions(&payload, fs::Permissions::from_mode(0o400)).unwrap();
            }
            "cache-missing" => {
                make_owned_tree_writable(&cache);
                fs::remove_dir_all(&cache).unwrap();
            }
            "cache-symlink" => {
                make_owned_tree_writable(&cache);
                fs::remove_dir_all(&cache).unwrap();
                symlink(&outside, &cache).unwrap();
            }
            "receipt-mismatch" => {
                let bytes = fs::read(&receipt).unwrap();
                let fingerprint = lease.request_fingerprint().to_string();
                let replacement = if fingerprint == "f".repeat(64) {
                    "e".repeat(64)
                } else {
                    "f".repeat(64)
                };
                let bytes = bytes.replace_bytes(fingerprint.as_bytes(), replacement.as_bytes());
                fs::write(&receipt, bytes).unwrap();
            }
            _ => unreachable!(),
        }

        let reopened = HostStore::open(&host_root).unwrap();
        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&reopened)
            .load_verified(&lease, lease.request_fingerprint())
            .unwrap_err();
        let diagnostic = error.to_string();
        assert!(
            diagnostic.contains("UNSAFE_REMOTE_SNAPSHOT")
                || diagnostic.contains("MANIFEST_MISMATCH")
                || diagnostic.contains("LEASE_IDENTITY_MISMATCH"),
            "case {case}: {diagnostic}"
        );
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
        assert!(!diagnostic.contains(LEASE_TOKEN), "case {case}");
        assert!(
            !diagnostic.contains(host_root.to_string_lossy().as_ref()),
            "case {case}"
        );
    }
}

#[test]
fn fresh_helper_rechecks_live_identity_and_terminal_disposition() {
    for case in ["stale-token", "stale-fingerprint", "abandoned", "accepted"] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let (store, lease, digest) = acquired_bundle(&host_root);
        mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&lease, &digest, 42)
            .unwrap();
        let request = request_for_digest(&digest);
        match case {
            "abandoned" => store.record_abandoned(&request, 2).unwrap(),
            "accepted" => store
                .record_accepted(&request, &JobStatus::accepted(2).unwrap(), 2)
                .unwrap(),
            _ => {}
        }
        drop(store);

        let caller_lease = if case == "stale-token" {
            mutated_lease(&lease, "lease_token")
        } else {
            lease.clone()
        };
        let stale_fingerprint = mutated_lease(&lease, "request_fingerprint");
        let caller_fingerprint = if case == "stale-fingerprint" {
            stale_fingerprint.request_fingerprint()
        } else {
            caller_lease.request_fingerprint()
        };
        let reopened = HostStore::open(&host_root).unwrap();
        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&reopened)
            .load_verified(&caller_lease, caller_fingerprint)
            .unwrap_err();
        let expected = match case {
            "abandoned" => "JOB_ABANDONED",
            "accepted" => "JOB_ACCEPTED",
            _ => "LEASE_IDENTITY_MISMATCH",
        };
        assert!(error.to_string().contains(expected), "case {case}: {error}");
    }
}

#[test]
fn valid_cache_without_receipt_recovers_but_conflicting_cache_is_never_replaced() {
    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_bundle(&host_root);
    mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 42)
        .unwrap();
    fs::remove_file(store.verified_receipt(lease.job_id()).unwrap()).unwrap();
    drop(store);

    let reopened = HostStore::open(&host_root).unwrap();
    let recovered = mac_worker::remote_snapshot::RemoteSnapshotService::new(&reopened)
        .verify_and_promote_at(&lease, &digest, 99)
        .unwrap();
    assert!(recovered.cache_reused());
    assert_eq!(recovered.verified_at_millis(), 99);
    drop(reopened);

    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_bundle(&host_root);
    let cache = store.snapshot(PROJECT_ID, WORKTREE_ID, &digest).unwrap();
    fs::create_dir_all(cache.join("tree")).unwrap();
    for parent in [
        cache.parent().unwrap().parent().unwrap(),
        cache.parent().unwrap(),
    ] {
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(cache.join("manifest.json"), valid_manifest_bytes()).unwrap();
    fs::write(cache.join("tree/payload.txt"), b"planted").unwrap();
    fs::set_permissions(
        cache.join("manifest.json"),
        fs::Permissions::from_mode(0o400),
    )
    .unwrap();
    fs::set_permissions(
        cache.join("tree/payload.txt"),
        fs::Permissions::from_mode(0o400),
    )
    .unwrap();
    fs::set_permissions(cache.join("tree"), fs::Permissions::from_mode(0o500)).unwrap();
    fs::set_permissions(&cache, fs::Permissions::from_mode(0o500)).unwrap();

    let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
        .verify_and_promote_at(&lease, &digest, 42)
        .unwrap_err();
    assert!(
        error.to_string().contains("MANIFEST_MISMATCH")
            || error.to_string().contains("UNSAFE_REMOTE_SNAPSHOT")
    );
    assert_eq!(
        fs::read(cache.join("tree/payload.txt")).unwrap(),
        b"planted"
    );
    assert!(!store.verified_receipt(lease.job_id()).unwrap().exists());
    assert!(
        store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap()
            .exists()
    );
}

#[test]
fn unsafe_preexisting_cache_shapes_are_never_replaced() {
    use std::os::unix::fs::symlink;

    for case in [
        "symlink",
        "permissive-directory",
        "regular-file",
        "incomplete",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let outside = fixture.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("sentinel"), b"keep").unwrap();
        let (store, lease, digest) = acquired_bundle(&host_root);
        let cache = store.snapshot(PROJECT_ID, WORKTREE_ID, &digest).unwrap();
        fs::create_dir_all(cache.parent().unwrap()).unwrap();
        for directory in [
            cache.parent().unwrap().parent().unwrap(),
            cache.parent().unwrap(),
        ] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        match case {
            "symlink" => symlink(&outside, &cache).unwrap(),
            "permissive-directory" => {
                fs::create_dir(&cache).unwrap();
                fs::set_permissions(&cache, fs::Permissions::from_mode(0o755)).unwrap();
            }
            "regular-file" => {
                fs::write(&cache, b"planted").unwrap();
                fs::set_permissions(&cache, fs::Permissions::from_mode(0o600)).unwrap();
            }
            "incomplete" => {
                fs::create_dir(&cache).unwrap();
                fs::write(cache.join("manifest.json"), valid_manifest_bytes()).unwrap();
                fs::set_permissions(
                    cache.join("manifest.json"),
                    fs::Permissions::from_mode(0o400),
                )
                .unwrap();
                fs::set_permissions(&cache, fs::Permissions::from_mode(0o500)).unwrap();
            }
            _ => unreachable!(),
        }

        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&lease, &digest, 42)
            .unwrap_err();
        assert!(
            error.to_string().contains("UNSAFE_REMOTE_SNAPSHOT")
                || error.to_string().contains("MANIFEST_MISMATCH"),
            "case {case}: {error}"
        );
        assert_eq!(fs::read(outside.join("sentinel")).unwrap(), b"keep");
        assert!(cache.exists() || fs::symlink_metadata(&cache).is_ok());
        assert!(!store.verified_receipt(lease.job_id()).unwrap().exists());
        assert!(
            store
                .incoming_job(lease.job_id(), lease.lease_token())
                .unwrap()
                .exists()
        );
    }
}

#[test]
fn workspace_materialization_is_writable_isolated_and_unpublished() {
    // Break caught: a job workspace aliases the immutable cache or publishes
    // the final job before Task 7 has recorded acceptance.
    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_bundle(&host_root);
    let service = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store);
    let snapshot = service.verify_and_promote_at(&lease, &digest, 42).unwrap();
    let mut staged = store
        .begin_job(PROJECT_ID, WORKTREE_ID, lease.job_id())
        .unwrap();

    let _receipt = service
        .materialize_workspace(&snapshot, &mut staged)
        .unwrap();

    let stage = fs::read_dir(host_root.join("leases"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(&format!(".job-{}-", lease.job_id()))
        })
        .unwrap();
    let workspace_file = stage.join("workspace/tree/payload.txt");
    let cache_file = store
        .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
        .unwrap()
        .join("tree/payload.txt");
    let workspace_metadata = fs::metadata(&workspace_file).unwrap();
    let cache_metadata = fs::metadata(&cache_file).unwrap();
    assert_eq!(workspace_metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(workspace_metadata.nlink(), 1);
    assert_ne!(workspace_metadata.ino(), cache_metadata.ino());
    fs::write(&workspace_file, b"changed").unwrap();
    assert_eq!(fs::read(&cache_file).unwrap(), b"payload");
    assert!(
        !store
            .job(PROJECT_ID, WORKTREE_ID, lease.job_id())
            .unwrap()
            .exists()
    );
}

#[test]
fn complex_workspace_materializes_twice_with_exact_types_modes_and_isolation() {
    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_complex_bundle(&host_root);
    let service = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store);
    let snapshot = service.verify_and_promote_at(&lease, &digest, 42).unwrap();
    let stage_paths = || {
        fs::read_dir(host_root.join("leases"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(&format!(".job-{}-", lease.job_id()))
            })
            .collect::<std::collections::BTreeSet<_>>()
    };

    let before = stage_paths();
    let mut first_staged = store
        .begin_job(PROJECT_ID, WORKTREE_ID, lease.job_id())
        .unwrap();
    let first_receipt = service
        .materialize_workspace(&snapshot, &mut first_staged)
        .unwrap();
    let first_stage = stage_paths().difference(&before).next().unwrap().clone();
    drop(first_staged);

    let before_second = stage_paths();
    let mut second_staged = store
        .begin_job(PROJECT_ID, WORKTREE_ID, lease.job_id())
        .unwrap();
    let second_receipt = service
        .materialize_workspace(&snapshot, &mut second_staged)
        .unwrap();
    let second_stage = stage_paths()
        .difference(&before_second)
        .next()
        .unwrap()
        .clone();

    let first_tree = first_stage.join("workspace/tree");
    let second_tree = second_stage.join("workspace/tree");
    for tree in [&first_tree, &second_tree] {
        for directory in [
            tree.clone(),
            tree.join("bin"),
            tree.join("empty"),
            tree.join("nested"),
        ] {
            assert_eq!(
                fs::metadata(directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        assert_eq!(
            fs::metadata(tree.join("bin/tool"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(tree.join("nested/β\nname.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::read_link(tree.join("tool-link")).unwrap(),
            Path::new("bin/tool")
        );
    }

    let cache_tree = store
        .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
        .unwrap()
        .join("tree");
    let first_tool = first_tree.join("bin/tool");
    let second_tool = second_tree.join("bin/tool");
    let cache_tool = cache_tree.join("bin/tool");
    let first_metadata = fs::metadata(&first_tool).unwrap();
    let second_metadata = fs::metadata(&second_tool).unwrap();
    let cache_metadata = fs::metadata(&cache_tool).unwrap();
    assert_eq!(first_metadata.nlink(), 1);
    assert_eq!(second_metadata.nlink(), 1);
    assert_ne!(first_metadata.ino(), second_metadata.ino());
    assert_ne!(first_metadata.ino(), cache_metadata.ino());
    assert_ne!(second_metadata.ino(), cache_metadata.ino());
    fs::write(&first_tool, b"first-only").unwrap();
    assert_eq!(fs::read(&second_tool).unwrap(), b"#!/bin/sh\n");
    assert_eq!(fs::read(&cache_tool).unwrap(), b"#!/bin/sh\n");
    assert_eq!(cache_metadata.permissions().mode() & 0o777, 0o500);
    assert_eq!(
        fs::metadata(cache_tree.join("empty"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o500
    );
    assert!(
        !store
            .job(PROJECT_ID, WORKTREE_ID, lease.job_id())
            .unwrap()
            .exists()
    );
    drop(first_receipt);
    drop(second_receipt);
    drop(second_staged);
}

#[test]
fn snapshot_verify_transport_uses_the_fixed_command_and_strict_response_dto() {
    let request: SnapshotVerifyRequest = serde_json::from_slice(&verify_request_bytes()).unwrap();
    let worker = WorkerEntry {
        name: "mini-1".into(),
        ssh: "mac1".into(),
        slots: 1,
        capabilities: vec!["darwin-arm64".into()],
        remote_binary: "~/.local/bin/worker".into(),
        herdr: false,
    };
    let policy = ProcessPolicy {
        stdout_limit: 1024 * 1024,
        stderr_limit: 64 * 1024,
        deadline: Duration::from_secs(15),
    };
    let runner = RecordingControlRunner::returning(response_bytes());

    let response: VerifiedSnapshotResponse = SshJsonTransport::new(&runner)
        .request(&worker, HostOperation::SnapshotVerify, &request, policy)
        .unwrap();

    assert_eq!(response.job_id(), request.job_id());
    assert_eq!(
        runner.request(),
        ProcessRequest {
            program: OsString::from("/usr/bin/ssh"),
            args: [
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "ForwardAgent=no",
                "-o",
                "ClearAllForwardings=yes",
                "--",
                "mac1",
                "~/.local/bin/worker host snapshot-verify",
            ]
            .into_iter()
            .map(OsString::from)
            .collect(),
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: Some(verify_request_bytes()),
            policy,
            isolate_parent_environment: false,
        }
    );

    let current_version = format!("\"protocol_version\":{PROTOCOL_VERSION}");
    let invalid_response =
        response_bytes().replace_bytes(current_version.as_bytes(), b"\"protocol_version\":9");
    let invalid_runner = RecordingControlRunner::returning(invalid_response);
    let error = SshJsonTransport::new(&invalid_runner)
        .request::<_, VerifiedSnapshotResponse>(
            &worker,
            HostOperation::SnapshotVerify,
            &request,
            policy,
        )
        .unwrap_err();
    assert!(error.to_string().contains("INVALID_RESPONSE"));
}

#[test]
fn hidden_snapshot_verify_is_fixed_bounded_compact_and_inventory_independent() {
    // Break caught: verification accepts a caller path, loads client config,
    // or varies its wire format with global --json.
    let fixture = tempfile::tempdir().unwrap();
    let data_home = fixture.path().join("data");
    let host_root = data_home.join("mac-worker/host");
    let (_store, lease, digest) = acquired_bundle(&host_root);
    let request = SnapshotVerifyRequest::new(
        lease.job_id(),
        lease.client_id(),
        lease.lease_token(),
        lease.request_fingerprint().clone(),
        lease.project_id().into(),
        lease.worktree_id().into(),
        digest.clone(),
    )
    .unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(OsString::from("XDG_DATA_HOME"), data_home.into_os_string())]),
        fixture.path().join("home"),
        fixture.path().to_path_buf(),
    );
    assert_eq!(
        HostOperation::SnapshotVerify.command(),
        "~/.local/bin/worker host snapshot-verify"
    );
    let invalid_config = fixture.path().join("invalid-config.toml");
    fs::write(&invalid_config, b"this is not valid = [toml").unwrap();
    let invalid_config = invalid_config.to_str().unwrap();

    for json in [false, true] {
        let mut argv = vec!["worker", "--config", invalid_config];
        if json {
            argv.push("--json");
        }
        argv.extend(["host", "snapshot-verify"]);
        let cli = Cli::try_parse_from(argv).unwrap();
        let mut stdin = Cursor::new(serde_json::to_vec(&request).unwrap());
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
        assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stdout));
        assert!(stderr.is_empty());
        assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
        let response: VerifiedSnapshotResponse = serde_json::from_slice(&stdout).unwrap();
        assert_eq!(response.job_id(), lease.job_id());
        assert_eq!(response.manifest_digest(), digest);
        assert!(!String::from_utf8_lossy(&stdout).contains(LEASE_TOKEN));
    }
}

#[test]
fn hidden_snapshot_verify_failures_are_versioned() {
    // Break caught: snapshot-verify emits its legacy unversioned error shape
    // and reflects private request material instead of a canonical envelope.
    let fixture = tempfile::tempdir().unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(
            OsString::from("XDG_DATA_HOME"),
            fixture.path().join("data").into_os_string(),
        )]),
        fixture.path().join("home"),
        fixture.path().join("PLANTED-HOST-PATH"),
    );
    let input = br#"{"protocol_version":2,"lease_token":"PLANTED-LEASE-TOKEN","incoming_path":"/tmp/PLANTED-SNAPSHOT-PATH","command":"PLANTED-COMMAND"}"#;
    let cli = Cli::try_parse_from(["worker", "host", "snapshot-verify"]).unwrap();
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

    assert_eq!(exit, 70);
    assert!(stderr.is_empty());
    assert_eq!(stdout.last(), Some(&b'\n'));
    assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
    let error: HostControlError = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(error.protocol_version(), PROTOCOL_VERSION);
    assert_eq!(error.error().code(), "INVALID_REQUEST");
    assert_eq!(error.error().message(), "host request was invalid");
    assert_eq!(
        stdout,
        br#"{"protocol_version":6,"error":{"code":"INVALID_REQUEST","message":"host request was invalid"}}
"#
    );
    let rendered = String::from_utf8(stdout).unwrap();
    for planted in [
        "PLANTED-LEASE-TOKEN",
        "/tmp/PLANTED-SNAPSHOT-PATH",
        "PLANTED-COMMAND",
        "PLANTED-HOST-PATH",
    ] {
        assert!(!rendered.contains(planted));
    }
}

#[test]
fn hidden_snapshot_verify_rejects_invalid_bounded_input_and_handles_broken_stdout() {
    let fixture = tempfile::tempdir().unwrap();
    let data_home = fixture.path().join("data");
    let host_root = data_home.join("mac-worker/host");
    let (_store, lease, digest) = acquired_bundle(&host_root);
    let request = SnapshotVerifyRequest::new(
        lease.job_id(),
        lease.client_id(),
        lease.lease_token(),
        lease.request_fingerprint().clone(),
        lease.project_id().into(),
        lease.worktree_id().into(),
        digest,
    )
    .unwrap();
    let valid = serde_json::to_vec(&request).unwrap();
    let runtime = RuntimeContext::isolated(
        BTreeMap::from([(OsString::from("XDG_DATA_HOME"), data_home.into_os_string())]),
        fixture.path().join("home"),
        fixture.path().to_path_buf(),
    );
    let cli = || Cli::try_parse_from(["worker", "host", "snapshot-verify"]).unwrap();
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b" trailing");
    let unknown = insert_before_final_brace(&valid, b",\"incoming_path\":\"/tmp/planted\"");
    let duplicate = insert_before_final_brace(
        &valid,
        format!(r#",\"lease_token\":\"{LEASE_TOKEN}\""#).as_bytes(),
    );
    for input in [
        b"{}".to_vec(),
        trailing,
        unknown,
        duplicate,
        vec![b' '; 1024 * 1024 + 1],
    ] {
        let mut stdin = Cursor::new(input);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_stdio_in_context(
            cli(),
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
        assert!(response["error"]["code"].is_string());
        let rendered = String::from_utf8(stdout).unwrap();
        assert!(!rendered.contains(LEASE_TOKEN));
        assert!(!rendered.contains("/tmp/planted"));
        assert!(!rendered.contains(host_root.to_string_lossy().as_ref()));
    }

    let mut stdin = Cursor::new(valid);
    let mut stderr = Vec::new();
    let exit = run_with_stdio_in_context(
        cli(),
        &NoProcess,
        &runtime,
        &mut stdin,
        &mut BrokenWriter,
        &mut stderr,
    );
    assert_eq!(exit, 74);
    assert!(stderr.is_empty());
}

#[test]
fn malformed_or_unsafe_bundles_never_publish_or_escape() {
    use std::ffi::CString;
    use std::os::unix::fs::symlink;

    for case in [
        "manifest-trailing",
        "root-extra",
        "tree-extra",
        "missing",
        "changed-bytes",
        "writable-file",
        "permissive-directory",
        "wrong-type",
        "hard-link",
        "fifo",
        "symlinked-tree",
        "oversized-manifest",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let sentinel = fixture.path().join("outside-sentinel");
        fs::write(&sentinel, b"keep").unwrap();
        let (store, lease, digest) = acquired_bundle(&host_root);
        let incoming = store
            .incoming_job(lease.job_id(), lease.lease_token())
            .unwrap();
        let tree = incoming.join("tree");
        let payload = tree.join("payload.txt");
        match case {
            "manifest-trailing" => {
                let mut bytes = valid_manifest_bytes();
                bytes.push(b'\n');
                replace_read_only_file(&incoming.join("manifest.json"), &bytes);
            }
            "root-extra" => fs::write(incoming.join("extra"), b"extra").unwrap(),
            "tree-extra" => {
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                fs::write(tree.join("extra"), b"extra").unwrap();
                fs::set_permissions(tree.join("extra"), fs::Permissions::from_mode(0o444)).unwrap();
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
            }
            "missing" => {
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                fs::remove_file(&payload).unwrap();
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
            }
            "changed-bytes" => replace_read_only_file(&payload, b"changed"),
            "writable-file" => {
                fs::set_permissions(&payload, fs::Permissions::from_mode(0o644)).unwrap();
            }
            "permissive-directory" => {
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
            }
            "wrong-type" => {
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                fs::remove_file(&payload).unwrap();
                fs::create_dir(&payload).unwrap();
                fs::set_permissions(&payload, fs::Permissions::from_mode(0o555)).unwrap();
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
            }
            "hard-link" => fs::hard_link(&payload, fixture.path().join("payload-alias")).unwrap(),
            "fifo" => {
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o755)).unwrap();
                fs::remove_file(&payload).unwrap();
                let fifo = CString::new(payload.as_os_str().as_encoded_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o444) }, 0);
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o555)).unwrap();
            }
            "symlinked-tree" => {
                fs::set_permissions(&tree, fs::Permissions::from_mode(0o700)).unwrap();
                fs::remove_dir_all(&tree).unwrap();
                symlink(&sentinel, &tree).unwrap();
            }
            "oversized-manifest" => replace_read_only_file(
                &incoming.join("manifest.json"),
                &vec![b' '; 8 * 1024 * 1024 + 1],
            ),
            _ => unreachable!(),
        }

        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&lease, &digest, 42)
            .unwrap_err();
        let diagnostic = error.to_string();
        assert!(
            diagnostic.contains("MANIFEST_MISMATCH")
                || diagnostic.contains("UNSAFE_REMOTE_SNAPSHOT"),
            "case {case}: {diagnostic}"
        );
        assert_eq!(fs::read(&sentinel).unwrap(), b"keep", "case {case}");
        assert!(
            !store
                .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
                .unwrap()
                .exists(),
            "case {case}"
        );
        assert!(
            !store.verified_receipt(lease.job_id()).unwrap().exists(),
            "case {case}"
        );
        assert!(!diagnostic.contains(LEASE_TOKEN), "case {case}");
        assert!(
            !diagnostic.contains(host_root.to_string_lossy().as_ref()),
            "case {case}"
        );
    }
}

#[test]
fn manifest_semantics_are_strict_before_tree_admission() {
    for case in [
        "version",
        "head",
        "branch-nul",
        "unsafe-path",
        "duplicate-entry",
        "unsorted-entry",
        "file-ancestor",
        "file-mode",
        "directory-combination",
        "symlink-combination",
        "duplicate-deletion",
        "unsafe-deletion",
        "cwd-not-explicit-directory",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let mut manifest: SnapshotManifest =
            serde_json::from_slice(&valid_manifest_bytes()).unwrap();
        let payload = manifest.entries[0].clone();
        match case {
            "version" => manifest.version = 2,
            "head" => manifest.head = Some("A".repeat(40)),
            "branch-nul" => manifest.branch = Some("main\0planted".into()),
            "unsafe-path" => manifest.entries[0].path = "../escape".into(),
            "duplicate-entry" => manifest.entries.push(payload.clone()),
            "unsorted-entry" => {
                let mut z = payload.clone();
                z.path = "z".into();
                let mut a = payload.clone();
                a.path = "a".into();
                manifest.entries = vec![z, a];
            }
            "file-ancestor" => {
                let mut parent = payload.clone();
                parent.path = "parent".into();
                let mut child = payload.clone();
                child.path = "parent/child".into();
                manifest.entries = vec![parent, child];
            }
            "file-mode" => manifest.entries[0].mode = 0o600,
            "directory-combination" => {
                manifest.entries[0] = ManifestEntry {
                    path: "empty".into(),
                    kind: ManifestEntryKind::Directory,
                    mode: 0o755,
                    size: 1,
                    sha256: "0".repeat(64),
                    symlink_target: None,
                };
            }
            "symlink-combination" => {
                manifest.entries[0] = ManifestEntry {
                    path: "link".into(),
                    kind: ManifestEntryKind::Symlink,
                    mode: 0o777,
                    size: 4,
                    sha256: "0".repeat(64),
                    symlink_target: Some("target".into()),
                };
            }
            "duplicate-deletion" => manifest.tracked_deletions = vec!["gone".into(), "gone".into()],
            "unsafe-deletion" => manifest.tracked_deletions = vec!["../gone".into()],
            "cwd-not-explicit-directory" => manifest.relative_working_dir = "packages/app".into(),
            _ => unreachable!(),
        }
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let (store, lease, digest) = acquired_bundle_bytes(&host_root, bytes);
        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&lease, &digest, 42)
            .unwrap_err();
        assert!(
            error.to_string().contains("MANIFEST_MISMATCH"),
            "case {case}: {error}"
        );
        assert!(!store.verified_receipt(lease.job_id()).unwrap().exists());
        assert!(
            !store
                .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
                .unwrap()
                .exists()
        );
    }
}

#[test]
fn interrupted_snapshot_commits_retry_to_one_valid_cache_and_receipt() {
    // Break caught: a process crash leaves an ambiguous mixed-mode bundle,
    // published-but-unsynced cache, or receipt that a fresh helper cannot
    // safely and idempotently resolve.
    for point in [
        HostStoreWritePoint::AfterSnapshotValidation,
        HostStoreWritePoint::DuringSnapshotConversion,
        HostStoreWritePoint::AfterSnapshotConversion,
        HostStoreWritePoint::AfterSnapshotRename,
        HostStoreWritePoint::AfterSnapshotCacheSync,
        HostStoreWritePoint::AfterSnapshotRootSeal,
        HostStoreWritePoint::AfterSnapshotReceiptFileSync,
        HostStoreWritePoint::AfterSnapshotReceiptPublish,
        HostStoreWritePoint::AfterSnapshotReceiptParentSync,
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let (store, lease, digest) = acquired_bundle(&host_root);
        drop(store);

        let faulted = HostStore::open_with_write_fault(&host_root, point).unwrap();
        assert!(
            mac_worker::remote_snapshot::RemoteSnapshotService::new(&faulted)
                .verify_and_promote_at(&lease, &digest, 42)
                .is_err(),
            "fault {point:?} did not interrupt the commit"
        );
        drop(faulted);
        let pending_receipt = host_root
            .join("verified")
            .join(format!(".verify-{}.json.pending", lease.job_id()));
        if point == HostStoreWritePoint::AfterSnapshotReceiptFileSync {
            assert!(pending_receipt.exists());
        }

        let reopened = HostStore::open(&host_root).unwrap();
        let snapshot = mac_worker::remote_snapshot::RemoteSnapshotService::new(&reopened)
            .verify_and_promote_at(&lease, &digest, 99)
            .unwrap_or_else(|error| panic!("retry after {point:?}: {error}"));
        assert_eq!(snapshot.digest(), digest);
        assert_eq!(
            fs::metadata(reopened.snapshot(PROJECT_ID, WORKTREE_ID, &digest).unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o500,
            "fault {point:?}"
        );
        let receipt = fs::read(reopened.verified_receipt(lease.job_id()).unwrap()).unwrap();
        let decoded: VerifiedReceipt = serde_json::from_slice(&receipt).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), receipt);
        assert!(!pending_receipt.exists(), "fault {point:?}");
    }
}

#[test]
fn concurrent_same_job_retries_are_serialized_and_revalidate_the_cache() {
    // The one-heavy-lease service has only one live job. This separate proof
    // covers same-job idempotency; the lower cache primitive has a distinct-
    // incoming no-replace race test in the remote_snapshot unit module.
    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_bundle(&host_root);
    drop(store);
    let barrier = Arc::new(Barrier::new(2));

    let mut handles = Vec::new();
    for now in [42, 99] {
        let host_root = host_root.clone();
        let lease = lease.clone();
        let digest = digest.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let store = HostStore::open(&host_root).unwrap();
            barrier.wait();
            let snapshot = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
                .verify_and_promote_at(&lease, &digest, now)
                .unwrap();
            (snapshot.digest().to_owned(), snapshot.cache_reused())
        }));
    }

    let mut outcomes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    outcomes.sort();
    assert_eq!(outcomes.iter().filter(|(_, reused)| !reused).count(), 1);
    assert_eq!(outcomes.iter().filter(|(_, reused)| *reused).count(), 1);
    assert!(outcomes.iter().all(|(actual, _)| actual == &digest));

    let reopened = HostStore::open(&host_root).unwrap();
    let cache_parent = host_root
        .join("snapshots")
        .join(PROJECT_ID)
        .join(WORKTREE_ID);
    assert_eq!(fs::read_dir(cache_parent).unwrap().count(), 1);
    assert!(
        reopened
            .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
            .unwrap()
            .exists()
    );
    let receipt = fs::read(reopened.verified_receipt(lease.job_id()).unwrap()).unwrap();
    let decoded: VerifiedReceipt = serde_json::from_slice(&receipt).unwrap();
    assert_eq!(serde_json::to_vec(&decoded).unwrap(), receipt);
}

#[test]
fn receiver_first_blocks_verification_until_the_final_bundle_is_stable() {
    let fixture = tempfile::tempdir().unwrap();
    let host_root = fixture.path().join("host");
    let (store, lease, digest) = acquired_bundle(&host_root);
    let request = request_for_digest(&digest);
    let identity = TransferIdentity::from_acquire_request(&request).unwrap();
    let payload = store
        .incoming_job(lease.job_id(), lease.lease_token())
        .unwrap()
        .join("tree/payload.txt");
    replace_read_only_file(&payload, b"partial");

    let (entered_tx, entered_rx) = mpsc::channel();
    let receiver_store = HostStore::open(&host_root).unwrap();
    let receiver = thread::spawn(move || {
        HostTransferService::new(&receiver_store).receive(
            &identity,
            &stock_server_args(),
            &HoldReceiver {
                entered: entered_tx,
            },
        )
    });
    let release = entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (verified_tx, verified_rx) = mpsc::channel();
    let verifier_store = HostStore::open(&host_root).unwrap();
    let verifier_lease = lease.clone();
    let verifier_digest = digest.clone();
    let verifier = thread::spawn(move || {
        verified_tx
            .send(
                mac_worker::remote_snapshot::RemoteSnapshotService::new(&verifier_store)
                    .verify_and_promote_at(&verifier_lease, &verifier_digest, 42),
            )
            .unwrap();
    });
    assert!(
        verified_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );
    replace_read_only_file(&payload, b"payload");
    release.send(()).unwrap();
    receiver.join().unwrap().unwrap();
    verified_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    verifier.join().unwrap();
}

#[test]
fn lease_identity_and_disposition_fences_precede_snapshot_mutation() {
    for field in [
        "lease_token",
        "request_fingerprint",
        "client_id",
        "project_id",
        "worktree_id",
        "manifest_digest",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let (store, lease, digest) = acquired_bundle(&host_root);
        let wrong = mutated_lease(&lease, field);
        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&wrong, &digest, 42)
            .unwrap_err();
        assert!(
            error.to_string().contains("LEASE_IDENTITY_MISMATCH"),
            "field {field}: {error}"
        );
        assert!(!store.verified_receipt(lease.job_id()).unwrap().exists());
        assert!(
            !store
                .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
                .unwrap()
                .exists()
        );
    }

    for case in [
        "abandoned",
        "abandoned-conflict",
        "accepted",
        "accepted-conflict",
    ] {
        let fixture = tempfile::tempdir().unwrap();
        let host_root = fixture.path().join("host");
        let (store, lease, digest) = acquired_bundle(&host_root);
        let request = request_for_digest(&digest);
        if case.starts_with("abandoned") {
            store.record_abandoned(&request, 2).unwrap();
        } else {
            store
                .record_accepted(&request, &JobStatus::accepted(2).unwrap(), 2)
                .unwrap();
        }
        if case.ends_with("conflict") {
            let disposition = store.job_index(lease.job_id()).unwrap();
            let bytes = fs::read(&disposition).unwrap();
            let bytes = bytes.replace_bytes(
                lease.client_id().to_string().as_bytes(),
                ClientId::new(uuid::Uuid::from_u128(999))
                    .to_string()
                    .as_bytes(),
            );
            fs::write(disposition, bytes).unwrap();
        }
        let error = mac_worker::remote_snapshot::RemoteSnapshotService::new(&store)
            .verify_and_promote_at(&lease, &digest, 42)
            .unwrap_err();
        let expected = match case {
            "abandoned" => "JOB_ABANDONED",
            "accepted" => "JOB_ACCEPTED",
            _ => "JOB_ID_CONFLICT",
        };
        assert!(error.to_string().contains(expected), "case {case}: {error}");
        assert!(!store.verified_receipt(lease.job_id()).unwrap().exists());
        assert!(
            !store
                .snapshot(PROJECT_ID, WORKTREE_ID, &digest)
                .unwrap()
                .exists()
        );
    }
}
