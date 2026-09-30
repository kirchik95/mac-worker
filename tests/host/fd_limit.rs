use std::{
    fs,
    io::Write as _,
    os::unix::process::CommandExt as _,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use mac_worker::{
    host_store::HostStore,
    job::{
        ClientId, CommandSpec, JobId, LeaseAcquireRequest, LeaseToken, RequestFingerprintMaterial,
        ResolveOrAbandonOutcome, ResolveOrAbandonRequest, ResolveOrAbandonResponse, SubmitRequest,
    },
    lease::{AdmissionFacts, LeaseService},
    protocol::MemoryPressure,
};

const GIB: u64 = 1024 * 1024 * 1024;
const SSH_NOFILE_LIMIT: libc::rlim_t = 256;

fn healthy() -> AdmissionFacts {
    AdmissionFacts {
        free_disk_bytes: 100 * GIB,
        total_disk_bytes: 250 * GIB,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    }
}

#[test]
fn resolve_or_abandon_completes_under_the_default_macos_ssh_fd_limit() {
    // Break caught: cloning every rooted lineage anchor exhausts the default
    // 256-descriptor SSH soft limit before the transfer lock can be published.
    let fixture = tempfile::Builder::new()
        .prefix("mac-worker-fd-limit-")
        .tempdir_in("/tmp")
        .unwrap();
    let data_home = fixture.path().join("data");
    let home = fixture.path().join("home");
    fs::create_dir(&home).unwrap();
    let host_root = data_home.join("mac-worker/host");
    let now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let material = RequestFingerprintMaterial::new(
        JobId::new(uuid::Uuid::from_u128(910_001)),
        ClientId::new(uuid::Uuid::from_u128(910_002)),
        LeaseToken::new(uuid::Uuid::from_u128(910_003)),
        now,
        "mini-1".into(),
        "a".repeat(64),
        "b".repeat(64),
        "c".repeat(64),
        String::new(),
        60_000,
        "heavy".into(),
        CommandSpec::argv(vec!["/usr/bin/true".into()]).unwrap(),
    )
    .unwrap();
    let job_id = material.job_id();
    let lease = LeaseAcquireRequest::new(material.clone());
    let resolve =
        ResolveOrAbandonRequest::from_submit_request(&SubmitRequest::new(material)).unwrap();
    let store = HostStore::open(&host_root).unwrap();
    LeaseService::new(&store)
        .acquire(&lease, &healthy(), now)
        .unwrap();
    drop(store);

    let mut command = Command::new(env!("CARGO_BIN_EXE_worker"));
    command
        .args(["host", "resolve-or-abandon"])
        .env("HOME", &home)
        .env("XDG_DATA_HOME", &data_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: the closure calls only async-signal-safe getrlimit/setrlimit and
    // mutates resource limits in the child between fork and exec.
    unsafe {
        command.pre_exec(|| {
            let mut current = std::mem::MaybeUninit::<libc::rlimit>::zeroed();
            if libc::getrlimit(libc::RLIMIT_NOFILE, current.as_mut_ptr()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let current = current.assume_init();
            if current.rlim_max < SSH_NOFILE_LIMIT {
                return Err(std::io::Error::other(
                    "hard descriptor limit is below the supported macOS SSH limit",
                ));
            }
            let limit = libc::rlimit {
                rlim_cur: SSH_NOFILE_LIMIT,
                rlim_max: current.rlim_max,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&resolve).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(
        output.status.success(),
        "resolve-or-abandon failed under RLIMIT_NOFILE=256: status={:?}, stdout={}, stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let response: ResolveOrAbandonResponse = serde_json::from_slice(&output.stdout).unwrap();
    assert!(matches!(
        response.outcome(),
        ResolveOrAbandonOutcome::Abandoned
    ));
    let job_locks = host_root.join("locks/jobs").join(job_id.to_string());
    assert!(job_locks.join("transfer").is_dir());
    assert!(fs::read_dir(&job_locks).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".transfer-init-")
    }));
    assert!(
        LeaseService::load_if_present(&host_root)
            .unwrap()
            .active_lease
            .is_none()
    );
}
