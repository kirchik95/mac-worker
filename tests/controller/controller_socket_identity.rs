//! T4 rooted image, identity, pin and forward lifecycle acceptance.
use mac_worker::test_support::{
    channel::{
        contracts::*,
        files::{PrivateChannelFiles, bind_leader},
        identity::{is_socket_selector, serve_identity_selector},
        image::SystemRunningImageSource,
        pin::PrivatePinStore,
        testing::{
            ManualRuntime, ScriptedImageSource, StubCodec, identity_fixture, request_fixture,
        },
    },
    controller::{ControllerLeader, ControllerReadReply, decode_frame},
    core::paths::PathLayout,
};
use serde_json::json;
use std::{
    fs,
    os::unix::{
        fs::{MetadataExt, PermissionsExt, symlink},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    time::Duration,
};

fn fixture() -> (tempfile::TempDir, PathLayout) {
    let temp = tempfile::tempdir_in("/private/tmp").unwrap();
    let p = temp.path();
    let paths = PathLayout {
        config: p.join("config"),
        state: p.join("state"),
        cache: p.join("cache"),
        data: p.join("data"),
    };
    (temp, paths)
}
fn private_dir(path: &Path) {
    fs::create_dir_all(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
fn private_file(path: &Path, bytes: &[u8]) {
    private_dir(path.parent().unwrap());
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn pin_path(paths: &PathLayout, identity: &SocketIdentity) -> PathBuf {
    paths
        .controller_cache_root()
        .join("channel/pins")
        .join(format!("{}.json", identity.route_sha256))
}
fn image(path: &Path) -> RunningImage {
    fs::write(path, b"#!/bin/sh\nprintf old").unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    let meta = fs::metadata(path).unwrap();
    RunningImage {
        path: fs::canonicalize(path).unwrap(),
        device: meta.dev(),
        inode: meta.ino(),
    }
}
fn service(
    paths: &PathLayout,
    leader: &ControllerLeader,
    generation: &UuidString,
) -> ServiceIdentity {
    let mut value = identity_fixture().service;
    value.leader = leader.identity();
    value.service_generation = generation.clone();
    value.socket_path = paths.controller_state_root().join("rpc/s");
    value
}
fn bound_forward(paths: &PathLayout) -> (ForwardPath, UnixListener, EntryIdentity) {
    let files = PrivateChannelFiles::new();
    let allocation = files.allocate(paths).unwrap();
    let listener = UnixListener::bind(&allocation.socket_path).unwrap();
    fs::set_permissions(&allocation.socket_path, fs::Permissions::from_mode(0o600)).unwrap();
    let evidence = files.validate_socket(&allocation).unwrap();
    (allocation, listener, evidence)
}

#[test]
fn first_pin_has_only_stable_fields_and_restart_preserves_bytes_mode_inode() {
    let (_temp, paths) = fixture();
    let mut identity = identity_fixture();
    let store = PrivatePinStore::new();
    store.verify_or_create(&paths, &identity).unwrap();
    let path = pin_path(&paths, &identity);
    let bytes = fs::read(&path).unwrap();
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.mode() & 0o7777, 0o600);
    assert_eq!(metadata.nlink(), 1);
    assert_eq!(
        fs::metadata(path.parent().unwrap()).unwrap().mode() & 0o7777,
        0o700
    );
    assert_eq!(
        serde_json::from_slice::<Pin>(&bytes).unwrap(),
        Pin::from_identity(&identity)
    );
    assert!(
        !String::from_utf8(bytes.clone())
            .unwrap()
            .contains(identity.service.service_generation.as_str())
    );
    identity.service.service_generation = UuidString::new_v4();
    identity.service.journal_id = None;
    store.verify_or_create(&paths, &identity).unwrap();
    assert_eq!(fs::read(path.clone()).unwrap(), bytes);
    assert_eq!(fs::metadata(path).unwrap().ino(), metadata.ino());
}
#[test]
fn concurrent_pin_bootstrap_checks_the_winner() {
    let (_temp, paths) = fixture();
    let identity = identity_fixture();
    let gate = Arc::new(Barrier::new(3));
    let joins: Vec<_> = (0..2)
        .map(|_| {
            let (paths, identity, gate) = (paths.clone(), identity.clone(), gate.clone());
            std::thread::spawn(move || {
                gate.wait();
                PrivatePinStore::new().verify_or_create(&paths, &identity)
            })
        })
        .collect();
    gate.wait();
    for join in joins {
        join.join().unwrap().unwrap();
    }
    assert_eq!(
        serde_json::from_slice::<Pin>(&fs::read(pin_path(&paths, &identity)).unwrap()).unwrap(),
        Pin::from_identity(&identity)
    );
}
#[test]
fn client_account_and_stored_route_mismatch_never_rotate_a_pin() {
    let (_temp, paths) = fixture();
    let identity = identity_fixture();
    let store = PrivatePinStore::new();
    store.verify_or_create(&paths, &identity).unwrap();
    let path = pin_path(&paths, &identity);
    let (bytes, inode) = (fs::read(&path).unwrap(), fs::metadata(&path).unwrap().ino());
    for case in 0..4 {
        let mut peer = identity.clone();
        match case {
            0 => {
                peer.service.controller_client_id =
                    mac_worker::test_support::host::job::ClientId::generate()
            }
            1 => peer.service.account.uid += 1,
            2 => peer.service.account.home = "/Users/other".into(),
            _ => peer.service.account.username = "other".into(),
        }
        assert!(store.verify_or_create(&paths, &peer).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
    }
    let mut pin = Pin::from_identity(&identity);
    pin.route_sha256 = RouteDigest::parse(&"b".repeat(64)).unwrap();
    fs::write(&path, serde_json::to_vec(&pin).unwrap()).unwrap();
    assert!(store.verify_or_create(&paths, &identity).is_err());
}
#[test]
fn unsafe_corrupt_hardlink_symlink_and_oversize_pins_are_preserved() {
    let (_temp, paths) = fixture();
    let identity = identity_fixture();
    let store = PrivatePinStore::new();
    store.verify_or_create(&paths, &identity).unwrap();
    let path = pin_path(&paths, &identity);
    let original = fs::read(&path).unwrap();
    for bytes in [
        b"invalid json".to_vec(),
        vec![b' '; 4097],
        br#"{"schema_version":9}"#.to_vec(),
    ] {
        fs::write(&path, &bytes).unwrap();
        assert!(store.verify_or_create(&paths, &identity).is_err());
        assert!(
            store
                .repin(&paths, &identity, identity.service.controller_client_id)
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    fs::write(&path, &original).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(store.verify_or_create(&paths, &identity).is_err());
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o7777, 0o644);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = path.with_extension("alias");
    fs::hard_link(&path, &alias).unwrap();
    assert!(
        store
            .repin(&paths, &identity, identity.service.controller_client_id)
            .is_err()
    );
    fs::remove_file(&alias).unwrap();
    fs::rename(&path, &alias).unwrap();
    symlink(&alias, &path).unwrap();
    assert!(store.verify_or_create(&paths, &identity).is_err());
    assert_eq!(fs::read(alias).unwrap(), original);
}
#[test]
fn repin_requires_expected_fresh_client_and_preserves_notify_and_envelopes() {
    let (_temp, paths) = fixture();
    let old = identity_fixture();
    let store = PrivatePinStore::new();
    store.verify_or_create(&paths, &old).unwrap();
    let path = pin_path(&paths, &old);
    let (original, inode) = (fs::read(&path).unwrap(), fs::metadata(&path).unwrap().ino());
    let files = [
        paths
            .controller_cache_root()
            .join("events/legacy/notify.json"),
        paths
            .controller_cache_root()
            .join("events/legacy/notify.lock"),
        paths.controller_cache_root().join("envelope.json"),
    ];
    for file in &files {
        private_file(file, b"retain");
    }
    let evidence: Vec<_> = files
        .iter()
        .map(|file| fs::metadata(file).unwrap())
        .collect();
    let mut fresh = old.clone();
    fresh.service.controller_client_id = mac_worker::test_support::host::job::ClientId::generate();
    assert!(
        store
            .repin(&paths, &fresh, old.service.controller_client_id)
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), original);
    store
        .repin(&paths, &fresh, fresh.service.controller_client_id)
        .unwrap();
    assert_ne!(fs::metadata(&path).unwrap().ino(), inode);
    assert_eq!(
        serde_json::from_slice::<Pin>(&fs::read(&path).unwrap()).unwrap(),
        Pin::from_identity(&fresh)
    );
    for (file, meta) in files.iter().zip(evidence) {
        assert_eq!(fs::read(file).unwrap(), b"retain");
        assert_eq!(
            (
                fs::metadata(file).unwrap().ino(),
                fs::metadata(file).unwrap().mode()
            ),
            (meta.ino(), meta.mode())
        );
    }
}
#[test]
fn fresh_private_forward_cleans_only_settled_positive_refusal() {
    let (_temp, paths) = fixture();
    let files = PrivateChannelFiles::new();
    let (path, listener, socket) = bound_forward(&paths);
    let second = files.allocate(&paths).unwrap();
    assert_ne!(path.directory, second.directory);
    assert_eq!(
        fs::metadata(&path.directory).unwrap().mode() & 0o7777,
        0o700
    );
    let ctx = CleanupContext::new(Arc::new(ManualRuntime::default()));
    assert_eq!(
        files.cleanup_if_refused(&path, Some(socket), &ctx),
        ForwardDisposition::Retained
    );
    drop(listener); // producer settled; live listener no longer exists
    assert_eq!(
        files.cleanup_if_refused(&path, None, &ctx),
        ForwardDisposition::Retained
    );
    assert!(path.socket_path.exists());
    assert_eq!(
        files.cleanup_if_refused(&path, Some(socket), &ctx),
        ForwardDisposition::Cleaned
    );
    assert!(!path.directory.exists());
    assert!(second.directory.exists());
}
#[test]
fn expired_missing_and_swapped_forward_evidence_retains_residue() {
    let (_temp, paths) = fixture();
    let files = PrivateChannelFiles::new();
    let missing = files.allocate(&paths).unwrap();
    let runtime = Arc::new(ManualRuntime::default());
    let ctx = CleanupContext::new(runtime.clone());
    assert_eq!(
        files.cleanup_if_refused(&missing, None, &ctx),
        ForwardDisposition::Retained
    );
    let (path, listener, socket) = bound_forward(&paths);
    drop(listener);
    runtime.advance(Duration::from_secs(5));
    assert_eq!(
        files.cleanup_if_refused(&path, Some(socket), &ctx),
        ForwardDisposition::Retained
    );
    let ctx = CleanupContext::new(Arc::new(ManualRuntime::default()));
    fs::rename(&path.socket_path, path.directory.join("old")).unwrap();
    let replacement = UnixListener::bind(&path.socket_path).unwrap();
    fs::set_permissions(&path.socket_path, fs::Permissions::from_mode(0o600)).unwrap();
    drop(replacement);
    assert_eq!(
        files.cleanup_if_refused(&path, Some(socket), &ctx),
        ForwardDisposition::Retained
    );
    assert!(path.directory.join("old").exists());
    assert!(path.socket_path.exists());
}
#[test]
fn pinned_image_survives_replacement_rollback_and_requires_rpc_exit_proof() {
    let (temp, paths) = fixture();
    let installed = image(&temp.path().join("worker"));
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let generation = UuidString::new_v4();
    let mut lease = bind_leader(&paths, &leader, &installed, &generation).unwrap();
    let executable = lease.executable();
    assert!(
        executable
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains(&generation.as_str().replace('-', ""))
    );
    assert_eq!(lease.detached_runner_executable(), installed.path);
    assert_eq!(
        fs::metadata(&installed.path).unwrap().mode() & 0o7777,
        0o755
    );
    lease
        .publish(&service(&paths, &leader, &generation))
        .unwrap();
    let old = temp.path().join("old");
    fs::rename(&installed.path, &old).unwrap();
    fs::write(&installed.path, b"#!/bin/sh\nprintf new").unwrap();
    fs::set_permissions(&installed.path, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        std::process::Command::new(&executable.path)
            .output()
            .unwrap()
            .stdout,
        b"old"
    );
    assert_eq!(
        std::process::Command::new(lease.detached_runner_executable())
            .output()
            .unwrap()
            .stdout,
        b"new"
    );
    fs::rename(old, &installed.path).unwrap();
    assert_eq!(
        std::process::Command::new(&executable.path)
            .output()
            .unwrap()
            .stdout,
        b"old"
    );
    assert_eq!(lease.withdraw(false), ForwardDisposition::Retained);
    assert!(executable.path.exists());
    assert_eq!(lease.withdraw(true), ForwardDisposition::Cleaned);
    assert!(executable.path.exists());
    assert!(installed.path.exists());
}
#[test]
fn changed_or_unverifiable_image_never_publishes_channel_state() {
    let (temp, paths) = fixture();
    let installed = image(&temp.path().join("worker"));
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let replacement = temp.path().join("replacement");
    image(&replacement);
    fs::rename(replacement, &installed.path).unwrap();
    assert!(bind_leader(&paths, &leader, &installed, &UuidString::new_v4()).is_err());
    assert!(
        !paths
            .controller_state_root()
            .join("rpc/service.json")
            .exists()
    );
    let unsupported = ScriptedImageSource::new(vec![Err(ChannelFailure::Unavailable(
        ChannelReason::Unsupported,
    ))]);
    assert!(unsupported.capture().is_err());
}
#[cfg(target_os = "macos")]
#[test]
fn system_image_matches_the_loaded_main_image_and_canonical_installed_path() {
    let captured = SystemRunningImageSource::new().capture().unwrap();
    let path = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(
        captured,
        RunningImage {
            path,
            device: metadata.dev(),
            inode: metadata.ino()
        }
    );
}
#[test]
fn identity_selector_rejects_mixed_grammar_and_creates_no_missing_state() {
    let (_temp, paths) = fixture();
    let runtime = ManualRuntime::default();
    let ctx = ClientContext {
        runtime: &runtime,
        deadline: Duration::from_secs(30),
        should_stop: &|| false,
    };
    let body = json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64)}});
    let request = request_fixture("task.list", body.clone());
    assert!(is_socket_selector(&request));
    let frame = serve_identity_selector(
        &request,
        &paths,
        Path::new("/Users/controller"),
        &StubCodec,
        &ctx,
    )
    .unwrap();
    let reply: ControllerReadReply<SocketIdentityResult> =
        serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
    reply.verify_envelope(&request).unwrap();
    assert!(matches!(
        reply.result(),
        SocketIdentityResult::Unavailable(_)
    ));
    assert!(!paths.state.exists());
    assert!(!paths.controller_state_root().exists());
    for body in [
        json!({"controller_socket":null}),
        json!({"controller_socket":{"op":"identity","route_sha256":"A".repeat(64)}}),
        json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64)},"controller_health":true}),
        json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64),"extra":1}}),
    ] {
        let request = request_fixture("task.list", body);
        assert!(is_socket_selector(&request));
        assert!(
            serve_identity_selector(
                &request,
                &paths,
                Path::new("/Users/controller"),
                &StubCodec,
                &ctx
            )
            .is_err()
        );
    }
    assert!(!paths.state.exists());
    assert!(!paths.controller_state_root().exists());
}

fn local_account(home: &Path) -> ControllerAccount {
    let uid = unsafe { libc::geteuid() };
    let mut record = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 65536];
    assert_eq!(
        unsafe {
            libc::getpwuid_r(
                uid,
                record.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        },
        0
    );
    assert!(!result.is_null());
    let record = unsafe { record.assume_init() };
    let username = unsafe { std::ffi::CStr::from_ptr(record.pw_name) }
        .to_str()
        .unwrap()
        .to_owned();
    ControllerAccount {
        uid,
        username,
        home: home.into(),
    }
}
fn live_fixture() -> (
    tempfile::TempDir,
    PathLayout,
    ControllerLeader,
    mac_worker::test_support::channel::files::LeaderSocketLease,
    ServiceIdentity,
) {
    let (temp, paths) = fixture();
    let installed = image(&temp.path().join("worker"));
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let generation = UuidString::new_v4();
    let lease = bind_leader(&paths, &leader, &installed, &generation).unwrap();
    let mut service = service(&paths, &leader, &generation);
    service.account = local_account(temp.path());
    private_file(
        &paths.state.join("client-id"),
        format!("{}\n", service.controller_client_id).as_bytes(),
    );
    (temp, paths, leader, lease, service)
}
fn reply_hello(
    listener: UnixListener,
    edit: impl FnOnce(&mut SocketIdentity) + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        use std::os::fd::AsRawFd;
        let mut fd = libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert!(
            unsafe { libc::poll(&mut fd, 1, 30_000) } > 0,
            "hello accept hang guard"
        );
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let mut prefix = [0u8; 4];
        stream.read_exact(&mut prefix).unwrap();
        let length = u32::from_be_bytes(prefix) as usize;
        assert!(length <= IDENTITY_BYTES);
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).unwrap();
        let mut expected = StubCodec.decode_hello(&bytes).unwrap();
        edit(&mut expected);
        stream
            .write_all(&StubCodec.encode_ready(&expected).unwrap())
            .unwrap();
    })
}
fn context(runtime: &dyn ChannelRuntime) -> ClientContext<'_> {
    ClientContext {
        runtime,
        deadline: Duration::from_secs(30),
        should_stop: &|| false,
    }
}
#[test]
fn live_identity_uses_existing_state_id_and_no_journal_receipt_or_active_rows() {
    use mac_worker::test_support::channel::identity::read_live_service;
    let (temp, paths, _leader, mut lease, mut service) = live_fixture();
    service.journal_id = None;
    lease.publish(&service).unwrap();
    // The similarly named controller file is never a client-id authority.
    private_file(
        &paths.controller_state_root().join("client-id"),
        b"33333333333343338333333333333333\n",
    );
    let record_path = paths.controller_state_root().join("rpc/service.json");
    let before = (
        fs::read(&record_path).unwrap(),
        fs::metadata(&record_path).unwrap().ino(),
    );
    let state_names: Vec<_> = fs::read_dir(&paths.state)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    let hello = reply_hello(lease.take_listener().unwrap(), |identity| {
        identity.service.journal_id = Some(UuidString::new_v4())
    });
    let runtime = ManualRuntime::default();
    assert_eq!(
        read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime)).unwrap(),
        Some(service)
    );
    hello.join().unwrap();
    assert_eq!(
        (
            fs::read(&record_path).unwrap(),
            fs::metadata(&record_path).unwrap().ino()
        ),
        before
    );
    assert_eq!(
        fs::read_dir(&paths.state)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>(),
        state_names
    );
    for name in ["requests", "active", "journal", "events"] {
        assert!(!paths.controller_state_root().join(name).exists());
    }
}
#[test]
fn identity_reply_echoes_route_and_optional_journal_changes_do_not_authenticate() {
    let (temp, paths, _leader, mut lease, service) = live_fixture();
    lease.publish(&service).unwrap();
    let hello = reply_hello(lease.take_listener().unwrap(), |identity| {
        identity.service.journal_id = None
    });
    let request = request_fixture(
        "task.list",
        json!({"controller_socket":{"op":"identity","route_sha256":"b".repeat(64)}}),
    );
    let runtime = ManualRuntime::default();
    let frame = serve_identity_selector(
        &request,
        &paths,
        temp.path(),
        &StubCodec,
        &context(&runtime),
    )
    .unwrap();
    let reply: ControllerReadReply<SocketIdentityResult> =
        serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
    reply.verify_envelope(&request).unwrap();
    assert_eq!(
        reply.into_result(),
        SocketIdentityResult::Available(SocketIdentity {
            route_sha256: RouteDigest::parse(&"b".repeat(64)).unwrap(),
            service
        })
    );
    hello.join().unwrap();
}
#[test]
fn missing_or_mismatched_client_id_and_dead_leader_do_not_advertise() {
    use mac_worker::test_support::channel::identity::read_live_service;
    let (temp, paths, leader, mut lease, service) = live_fixture();
    lease.publish(&service).unwrap();
    let runtime = ManualRuntime::default();
    fs::remove_file(paths.state.join("client-id")).unwrap();
    assert!(
        read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime))
            .unwrap()
            .is_none()
    );
    assert!(!paths.state.join("client-id").exists());
    private_file(
        &paths.state.join("client-id"),
        b"33333333333343338333333333333333\n",
    );
    assert!(
        read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime))
            .unwrap()
            .is_none()
    );
    private_file(
        &paths.state.join("client-id"),
        format!("{}\n", service.controller_client_id).as_bytes(),
    );
    drop(leader); // process still alive, but its leader lock is no longer held
    assert!(
        read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime))
            .unwrap()
            .is_none()
    );
    assert!(
        paths
            .controller_state_root()
            .join("rpc/service.json")
            .exists()
    );
}
#[test]
fn listener_ready_must_match_every_required_service_field() {
    use mac_worker::test_support::channel::identity::read_live_service;
    for field in 0..5 {
        let (temp, paths, _leader, mut lease, service) = live_fixture();
        lease.publish(&service).unwrap();
        let hello = reply_hello(
            lease.take_listener().unwrap(),
            move |identity| match field {
                0 => identity.service.service_generation = UuidString::new_v4(),
                1 => {
                    identity.service.controller_client_id =
                        mac_worker::test_support::host::job::ClientId::generate()
                }
                2 => identity.service.account.username.push('x'),
                3 => identity.service.features = vec!["controller.socket".into()],
                _ => identity.route_sha256 = RouteDigest::parse(&"c".repeat(64)).unwrap(),
            },
        );
        let runtime = ManualRuntime::default();
        assert!(
            read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime))
                .unwrap()
                .is_none(),
            "field {field}"
        );
        hello.join().unwrap();
    }
}
#[test]
fn swapped_socket_or_record_after_local_hello_is_never_advertised() {
    use mac_worker::test_support::channel::identity::read_live_service;
    for swap in 0..2 {
        let (temp, paths, _leader, mut lease, service) = live_fixture();
        lease.publish(&service).unwrap();
        let socket = service.socket_path.clone();
        let record = paths.controller_state_root().join("rpc/service.json");
        let hello = reply_hello(lease.take_listener().unwrap(), move |_| {
            if swap == 0 {
                fs::rename(&socket, socket.with_file_name("original-s")).unwrap();
                let replacement = UnixListener::bind(&socket).unwrap();
                fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
                drop(replacement);
            } else {
                let bytes = fs::read(&record).unwrap();
                fs::rename(&record, record.with_extension("original")).unwrap();
                private_file(&record, &bytes);
            }
        });
        let runtime = ManualRuntime::default();
        assert!(!matches!(
            read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime)),
            Ok(Some(_))
        ));
        hello.join().unwrap();
        assert!(service.socket_path.exists());
    }
}
#[test]
fn invalid_socket_record_types_modes_and_link_bindings_preserve_residue() {
    use mac_worker::test_support::channel::identity::read_live_service;
    for case in 0..7 {
        let (temp, paths, _leader, mut lease, service) = live_fixture();
        lease.publish(&service).unwrap();
        let record_path = paths.controller_state_root().join("rpc/service.json");
        let bytes = fs::read(&record_path).unwrap();
        match case {
            0 => fs::set_permissions(&record_path, fs::Permissions::from_mode(0o644)).unwrap(),
            1 => fs::hard_link(&record_path, record_path.with_extension("alias")).unwrap(),
            2 => {
                fs::rename(&record_path, record_path.with_extension("old")).unwrap();
                symlink(record_path.with_extension("old"), &record_path).unwrap();
            }
            3 => fs::write(&record_path, vec![b' '; IDENTITY_BYTES + 1]).unwrap(),
            4 => {
                let mut record: ServiceRecord = serde_json::from_slice(&bytes).unwrap();
                record.binding.socket.owner = record.binding.socket.owner.wrapping_add(1);
                fs::write(&record_path, serde_json::to_vec(&record).unwrap()).unwrap();
            }
            5 => {
                fs::remove_file(&service.socket_path).unwrap();
                private_file(&service.socket_path, b"unknown");
            }
            _ => {
                fs::remove_file(&service.socket_path).unwrap();
                let name = std::ffi::CString::new(service.socket_path.to_str().unwrap()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            }
        }
        let runtime = ManualRuntime::default();
        assert!(!matches!(
            read_live_service(&paths, temp.path(), &StubCodec, &context(&runtime)),
            Ok(Some(_))
        ));
        assert_eq!(lease.withdraw(true), ForwardDisposition::Retained);
        assert!(lease.executable().path.exists());
        assert!(fs::symlink_metadata(&record_path).is_ok());
    }
}
fn simulate_prior_process_identity(paths: &PathLayout) {
    // Two real guards in this single test process have the same PID/start time.
    // Give the prior record a valid distinct start time to model process reuse.
    let path = paths.controller_state_root().join("rpc/service.json");
    let mut record: ServiceRecord = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    record.service.leader = mac_worker::test_support::host::job::ProcessIdentity::new(
        record.service.leader.pid(),
        record.service.leader.start_time_micros() + 1,
    )
    .unwrap();
    fs::write(path, serde_json::to_vec(&record).unwrap()).unwrap();
}
#[test]
fn stale_record_recovers_only_after_dead_leader_and_exact_refusal() {
    let (temp, paths, old_leader, mut old, service) = live_fixture();
    old.publish(&service).unwrap();
    let old_image = old.executable();
    simulate_prior_process_identity(&paths);
    let bytes = fs::read(paths.controller_state_root().join("rpc/service.json")).unwrap();
    let installed = RunningImage {
        path: old.detached_runner_executable().into(),
        device: old_image.binding.device,
        inode: old_image.binding.inode,
    };
    drop(old_leader);
    let new_leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let new_generation = UuidString::new_v4();
    // Still-serving old socket is preserved even though its leader lock moved.
    assert!(bind_leader(&paths, &new_leader, &installed, &new_generation).is_err());
    assert_eq!(
        fs::read(paths.controller_state_root().join("rpc/service.json")).unwrap(),
        bytes
    );
    drop(old.take_listener());
    let mut new = bind_leader(&paths, &new_leader, &installed, &new_generation).unwrap();
    let mut new_service = service.clone();
    new_service.leader = new_leader.identity();
    new_service.service_generation = new_generation;
    new.publish(&new_service).unwrap();
    assert!(old_image.path.exists()); // startup cannot prove old RPC exits
    assert_ne!(new.executable().path, old_image.path);
    assert_eq!(new.withdraw(true), ForwardDisposition::Cleaned);
    assert!(old_image.path.exists());
    assert!(temp.path().join("worker").exists());
}
#[test]
fn missing_stale_socket_recovers_and_prior_image_cleanup_needs_rpc_exit_proof() {
    use mac_worker::test_support::channel::files::cleanup_prior_generation;
    let (_temp, paths, leader, mut old, service) = live_fixture();
    old.publish(&service).unwrap();
    let executable = old.executable();
    simulate_prior_process_identity(&paths);
    drop(old.take_listener());
    fs::remove_file(&service.socket_path).unwrap();
    drop(leader);
    assert_eq!(
        cleanup_prior_generation(&paths, false),
        ForwardDisposition::Retained
    );
    assert!(executable.path.exists());
    assert_eq!(
        cleanup_prior_generation(&paths, true),
        ForwardDisposition::Cleaned
    );
    assert!(executable.path.exists());
    assert!(
        paths
            .controller_state_root()
            .join("rpc/service.json")
            .exists()
    );
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let metadata = fs::metadata(old.detached_runner_executable()).unwrap();
    let installed = RunningImage {
        path: old.detached_runner_executable().into(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    assert!(bind_leader(&paths, &leader, &installed, &UuidString::new_v4()).is_ok());
}
#[test]
fn missing_creation_record_preserves_unknown_bare_image_socket_and_other_entries() {
    for case in 0..4 {
        let (temp, paths) = fixture();
        let installed = image(&temp.path().join("worker"));
        let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
        let rpc = paths.controller_state_root().join("rpc");
        private_dir(&rpc);
        let residue = if case == 0 {
            rpc.join(format!(
                "e{}",
                UuidString::new_v4().as_str().replace('-', "")
            ))
        } else {
            rpc.join("s")
        };
        match case {
            0 => fs::hard_link(&installed.path, &residue).unwrap(),
            1 => {
                let listener = UnixListener::bind(&residue).unwrap();
                fs::set_permissions(&residue, fs::Permissions::from_mode(0o600)).unwrap();
                drop(listener);
            }
            2 => private_file(&residue, b"unknown"),
            _ => symlink(&installed.path, &residue).unwrap(),
        }
        let inode = fs::symlink_metadata(&residue).unwrap().ino();
        assert!(bind_leader(&paths, &leader, &installed, &UuidString::new_v4()).is_err());
        assert_eq!(fs::symlink_metadata(&residue).unwrap().ino(), inode);
        assert!(!rpc.join("service.json").exists());
    }
}

use mac_worker::test_support::{
    channel::{
        identity::{StdioIdentitySource, read_live_service},
        testing::{RecordingRunner, result_fixture},
    },
    controller::{encode_json_frame, parse_request},
    core::error::{ProcessError, WorkerError},
    host::process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
};
use std::{
    io::{Read, Write},
    sync::Mutex,
};
struct IdentityRunner {
    calls: Mutex<Vec<ProcessRequest>>,
    identity: SocketIdentityResult,
    edit: fn(&mut serde_json::Value),
}
impl IdentityRunner {
    fn new(identity: SocketIdentity) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            identity: SocketIdentityResult::Available(identity),
            edit: |_| {},
        }
    }
}
impl ProcessRunner for IdentityRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.calls.lock().unwrap().push(request.clone());
        let query = parse_request(decode_frame(request.stdin.as_ref().unwrap())?).unwrap();
        let mut result = result_fixture(&query, serde_json::to_value(&self.identity).unwrap(), 0);
        let mut envelope: serde_json::Value =
            serde_json::from_slice(decode_frame(&result.stdout)?).unwrap();
        (self.edit)(&mut envelope);
        result.stdout = encode_json_frame(&envelope)?;
        Ok(result)
    }
}
fn route() -> ConfiguredRoute {
    ConfiguredRoute {
        ssh: "controller-fixture".into(),
        remote_binary: "~/.local/bin/worker".into(),
        ssh_config_file: Some("/private/tmp/original config".into()),
    }
}
fn bootstrap() -> ProcessRequest {
    ProcessRequest {
        program: "/usr/bin/ssh".into(),
        args: [
            "-F",
            "/private/tmp/original config",
            "-o",
            "StrictHostKeyChecking=yes",
            "-S",
            "/private/tmp/literal master",
            "--",
            "controller-fixture",
            "~/.local/bin/worker host controller-rpc",
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        environment: vec![("FIXTURE".into(), "retained".into())],
        environment_remove: vec!["REMOVE_FIXTURE".into()],
        stdin: None,
        policy: ProcessPolicy {
            deadline: Duration::from_secs(30),
            stdout_limit: 1024 * 1024 + 4,
            stderr_limit: 256 * 1024,
        },
        isolate_parent_environment: true,
    }
}
#[test]
fn raw_bootstrap_retains_original_config_trust_literal_master_and_budget() {
    let mut identity = identity_fixture();
    let route = route();
    identity.route_sha256 = route.digest().unwrap();
    let runner = IdentityRunner::new(identity.clone());
    let runtime = ManualRuntime::default();
    let master = MasterPlan {
        control_path: "/private/tmp/literal master".into(),
        parent: EntryIdentity {
            device: 1,
            inode: 2,
            owner: unsafe { libc::geteuid() },
            kind: libc::S_IFDIR as u32,
            mode: 0o700,
        },
        bootstrap_request: bootstrap(),
    };
    assert_eq!(
        StdioIdentitySource::new()
            .read(&runner, &route, Some(&master), &context(&runtime))
            .unwrap(),
        identity
    );
    let calls = runner.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].args, master.bootstrap_request.args);
    assert_eq!(calls[0].program, master.bootstrap_request.program);
    assert_eq!(calls[0].environment, master.bootstrap_request.environment);
    assert_eq!(
        calls[0].environment_remove,
        master.bootstrap_request.environment_remove
    );
    assert!(calls[0].isolate_parent_environment);
    assert_eq!(calls[0].policy.deadline, Duration::from_secs(5));
    assert_eq!(calls[0].policy.stdout_limit, IDENTITY_BYTES + 4);
    let request = parse_request(decode_frame(calls[0].stdin.as_ref().unwrap()).unwrap()).unwrap();
    assert_eq!(
        request.body(),
        &json!({"controller_socket":{"op":"identity","route_sha256":route.digest().unwrap()}})
    );
}
#[test]
fn raw_identity_without_master_keeps_route_config_and_authenticates_echo() {
    let route = route();
    let mut identity = identity_fixture();
    identity.route_sha256 = route.digest().unwrap();
    let runner = IdentityRunner::new(identity.clone());
    let runtime = ManualRuntime::default();
    assert_eq!(
        StdioIdentitySource::new()
            .read(&runner, &route, None, &context(&runtime))
            .unwrap(),
        identity
    );
    let calls = runner.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        &calls[0].args[..2],
        &[
            std::ffi::OsString::from("-F"),
            route.ssh_config_file.unwrap().into_os_string()
        ]
    );
    assert!(
        calls[0]
            .args
            .iter()
            .any(|arg| arg == "ClearAllForwardings=yes")
    );
    assert!(calls[0].args.iter().any(|arg| arg == "controller-fixture"));
    assert!(!calls[0].args.iter().any(|arg| arg == "/dev/null"));
}
#[test]
fn raw_identity_rejects_wrong_route_envelope_strings_and_trailing_bytes() {
    let route = route();
    let mut identity = identity_fixture();
    identity.route_sha256 = route.digest().unwrap();
    let edits: [fn(&mut serde_json::Value); 6] = [
        |reply| reply["result"]["available"]["route_sha256"] = json!("b".repeat(64)),
        |reply| reply["payload_sha256"] = json!("wrong"),
        |reply| reply["request_id"] = json!("33333333333343338333333333333333"),
        |reply| {
            reply["result"]["available"]["service"]["service_generation"] =
                json!("00000000-0000-0000-0000-000000000000")
        },
        |reply| reply["result"]["available"]["service"]["account"]["username"] = json!("bad\nname"),
        |reply| {
            reply["result"]["available"]["service"]["journal_id"] =
                reply["result"]["available"]["service"]["service_generation"].clone()
        },
    ];
    let runtime = ManualRuntime::default();
    for edit in edits {
        let mut runner = IdentityRunner::new(identity.clone());
        runner.edit = edit;
        assert!(
            StdioIdentitySource::new()
                .read(&runner, &route, None, &context(&runtime))
                .is_err()
        );
        assert_eq!(runner.calls.lock().unwrap().len(), 1);
    }
    struct Trailing(IdentityRunner);
    impl ProcessRunner for Trailing {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let mut reply = self.0.run(request)?;
            reply.stdout.push(b'x');
            Ok(reply)
        }
    }
    assert!(
        StdioIdentitySource::new()
            .read(
                &Trailing(IdentityRunner::new(identity)),
                &route,
                None,
                &context(&runtime)
            )
            .is_err()
    );
}
#[test]
fn cancellation_or_expiry_before_bootstrap_sends_nothing() {
    let runner = RecordingRunner::new(vec![]);
    let runtime = ManualRuntime::default();
    let source = StdioIdentitySource::new();
    let ctx = ClientContext {
        runtime: &runtime,
        deadline: Duration::from_secs(30),
        should_stop: &|| true,
    };
    assert_eq!(
        source.read(&runner, &route(), None, &ctx),
        Err(ChannelFailure::Unavailable(ChannelReason::Cancelled))
    );
    runtime.advance(Duration::from_secs(30));
    assert_eq!(
        source.read(&runner, &route(), None, &context(&runtime)),
        Err(ChannelFailure::Unavailable(ChannelReason::Timeout))
    );
    assert!(runner.calls().is_empty());
}
#[test]
fn borrowed_non_send_predicate_and_deadline_interrupt_blocked_bootstrap_after_entry() {
    use std::{
        cell::Cell,
        rc::Rc,
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
    };
    struct Gated {
        entered: mpsc::SyncSender<()>,
        calls: Mutex<usize>,
    }
    impl ProcessRunner for Gated {
        fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            panic!("must use borrowed interruptible runner")
        }
        fn run_interruptible(
            &self,
            _: &ProcessRequest,
            stop: &dyn Fn() -> bool,
        ) -> Result<ProcessResult, WorkerError> {
            *self.calls.lock().unwrap() += 1;
            self.entered.send(()).unwrap();
            let hang_guard = std::time::Instant::now() + Duration::from_secs(30);
            while !stop() {
                assert!(
                    std::time::Instant::now() < hang_guard,
                    "bootstrap cancellation hang guard"
                );
                std::thread::yield_now();
            }
            Err(ProcessError::Cancelled.into())
        }
    }
    for expiry in [false, true] {
        let runtime = Arc::new(ManualRuntime::default());
        let cancelled = Arc::new(AtomicBool::new(false));
        let borrowed = Rc::new(Cell::new(false));
        let (tx, rx) = mpsc::sync_channel(1);
        let runner = Gated {
            entered: tx,
            calls: Mutex::new(0),
        };
        let trigger_runtime = runtime.clone();
        let trigger_cancelled = cancelled.clone();
        let trigger = std::thread::spawn(move || {
            rx.recv_timeout(Duration::from_secs(30)).unwrap();
            if expiry {
                trigger_runtime.advance(Duration::from_secs(30));
            } else {
                trigger_cancelled.store(true, Ordering::Release);
            }
        });
        let stop = || {
            if cancelled.load(Ordering::Acquire) {
                borrowed.set(true);
            }
            borrowed.get()
        };
        let ctx = ClientContext {
            runtime: runtime.as_ref(),
            deadline: Duration::from_secs(30),
            should_stop: &stop,
        };
        assert_eq!(
            StdioIdentitySource::new().read(&runner, &route(), None, &ctx),
            Err(ChannelFailure::Unavailable(if expiry {
                ChannelReason::Timeout
            } else {
                ChannelReason::Cancelled
            }))
        );
        assert_eq!(*runner.calls.lock().unwrap(), 1);
        trigger.join().unwrap();
    }
}

#[test]
fn identity_length_cap_is_checked_before_codec_allocates() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountingCodec(Arc<AtomicUsize>);
    struct CountingDecoder {
        inner: Box<dyn FrameDecoder>,
        feeds: Arc<AtomicUsize>,
    }
    impl FrameDecoder for CountingDecoder {
        fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure> {
            self.feeds.fetch_add(1, Ordering::Relaxed);
            self.inner.feed(input)
        }
        fn retained_bytes(&self) -> usize {
            self.inner.retained_bytes()
        }
    }
    impl ChannelCodec for CountingCodec {
        fn decoder(&self) -> Box<dyn FrameDecoder> {
            Box::new(CountingDecoder {
                inner: StubCodec.decoder(),
                feeds: self.0.clone(),
            })
        }
        fn encode_hello(&self, expected: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
            StubCodec.encode_hello(expected)
        }
        fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure> {
            StubCodec.decode_hello(payload)
        }
        fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure> {
            StubCodec.encode_ready(identity)
        }
        fn decode_ready(
            &self,
            payload: &[u8],
            expected: &SocketIdentity,
        ) -> Result<(), ChannelFailure> {
            StubCodec.decode_ready(payload, expected)
        }
        fn encode_reply(
            &self,
            request: &mac_worker::test_support::controller::ControllerRequest,
            result: &ProcessResult,
        ) -> Result<Vec<u8>, ChannelFailure> {
            StubCodec.encode_reply(request, result)
        }
        fn decode_reply(
            &self,
            payload: &[u8],
            request: &mac_worker::test_support::controller::ControllerRequest,
        ) -> Result<ProcessResult, ChannelFailure> {
            StubCodec.decode_reply(payload, request)
        }
    }
    let (temp, paths, _leader, mut lease, service) = live_fixture();
    lease.publish(&service).unwrap();
    let listener = lease.take_listener().unwrap();
    let peer = std::thread::spawn(move || {
        use std::os::fd::AsRawFd;
        let mut fd = libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert!(unsafe { libc::poll(&mut fd, 1, 30_000) } > 0);
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let mut hello = vec![0; u32::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut hello).unwrap();
        stream
            .write_all(&((IDENTITY_BYTES + 1) as u32).to_be_bytes())
            .unwrap();
    });
    let feeds = Arc::new(AtomicUsize::new(0));
    let codec = CountingCodec(feeds.clone());
    let runtime = ManualRuntime::default();
    assert!(
        read_live_service(&paths, temp.path(), &codec, &context(&runtime))
            .unwrap()
            .is_none()
    );
    peer.join().unwrap();
    assert_eq!(
        feeds.load(Ordering::Relaxed),
        0,
        "oversize header reached an allocating decoder"
    );
}

#[test]
fn record_publication_rejects_invalid_identity_and_current_image_substitution() {
    let (_temp, paths, _leader, mut lease, service) = live_fixture();
    for case in 0..4 {
        let mut invalid = service.clone();
        match case {
            0 => invalid.service_generation = UuidString::new_v4(),
            1 => invalid.socket_path = "/private/other/s".into(),
            2 => {
                invalid.leader =
                    mac_worker::test_support::host::job::ProcessIdentity::new(1, 1).unwrap()
            }
            _ => invalid.features = vec!["controller.events".into()],
        }
        assert!(lease.publish(&invalid).is_err());
        assert!(
            !paths
                .controller_state_root()
                .join("rpc/service.json")
                .exists()
        );
    }
    lease.publish(&service).unwrap();
    let executable = lease.executable().path;
    fs::rename(&executable, executable.with_extension("original")).unwrap();
    fs::write(&executable, b"replacement").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(lease.withdraw(true), ForwardDisposition::Retained);
    // Uncertain executable evidence must be checked before deleting its socket.
    assert!(service.socket_path.exists());
    assert_eq!(fs::read(executable).unwrap(), b"replacement");
}
#[test]
fn detached_task_lifetime_does_not_gate_generation_image_withdrawal() {
    use std::process::{Command, Stdio};
    let (temp, paths) = fixture();
    let installed_path = temp.path().join("worker");
    let mut installed = image(&installed_path);
    fs::write(
        &installed_path,
        b"#!/bin/sh\nif [ \"$1\" = detached ]; then printf ready; read line; else printf old; fi\n",
    )
    .unwrap();
    let metadata = fs::metadata(&installed_path).unwrap();
    installed.device = metadata.dev();
    installed.inode = metadata.ino();
    let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let generation = UuidString::new_v4();
    let mut lease = bind_leader(&paths, &leader, &installed, &generation).unwrap();
    lease
        .publish(&service(&paths, &leader, &generation))
        .unwrap();
    let mut detached = Command::new(lease.detached_runner_executable())
        .arg("detached")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut ready = [0; 5];
    detached
        .stdout
        .as_mut()
        .unwrap()
        .read_exact(&mut ready)
        .unwrap();
    assert_eq!(&ready, b"ready");
    assert_eq!(lease.withdraw(true), ForwardDisposition::Cleaned);
    assert!(lease.executable().path.exists());
    assert!(detached.try_wait().unwrap().is_none());
    assert!(installed.path.exists());
    detached.stdin.take().unwrap().write_all(b"exit\n").unwrap();
    assert!(detached.wait().unwrap().success());
}
#[test]
fn private_containers_and_long_forward_paths_decline_without_repair() {
    let (_temp, mut paths) = fixture();
    paths.cache = paths.cache.join("x".repeat(104));
    assert!(PrivateChannelFiles::new().allocate(&paths).is_err());
    assert!(!paths.controller_cache_root().exists());
    let (_temp, paths) = fixture();
    private_dir(&paths.controller_cache_root());
    let channel = paths.controller_cache_root().join("channel");
    private_dir(&channel);
    fs::set_permissions(&channel, fs::Permissions::from_mode(0o1700)).unwrap();
    assert!(
        PrivatePinStore::new()
            .verify_or_create(&paths, &identity_fixture())
            .is_err()
    );
    assert!(PrivateChannelFiles::new().allocate(&paths).is_err());
    assert!(!channel.join("pins").exists());
    assert_eq!(fs::metadata(channel).unwrap().mode() & 0o7777, 0o1700);
}
#[test]
fn swapped_forward_parent_and_extra_entries_are_preserved() {
    let (_temp, paths) = fixture();
    let files = PrivateChannelFiles::new();
    let (path, listener, socket) = bound_forward(&paths);
    drop(listener);
    let ctx = CleanupContext::new(Arc::new(ManualRuntime::default()));
    private_file(&path.directory.join("unknown"), b"retain");
    assert_eq!(
        files.cleanup_if_refused(&path, Some(socket), &ctx),
        ForwardDisposition::Retained
    );
    assert!(path.socket_path.exists());
    let moved = path.directory.with_extension("original");
    fs::rename(&path.directory, &moved).unwrap();
    private_dir(&path.directory);
    let replacement = UnixListener::bind(&path.socket_path).unwrap();
    fs::set_permissions(&path.socket_path, fs::Permissions::from_mode(0o600)).unwrap();
    drop(replacement);
    assert_eq!(
        files.cleanup_if_refused(&path, Some(socket), &ctx),
        ForwardDisposition::Retained
    );
    assert!(path.socket_path.exists());
    assert_eq!(fs::read(moved.join("unknown")).unwrap(), b"retain");
}
#[test]
fn socket_and_parent_swaps_at_refusal_recheck_preserve_replacements() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct SwapClock {
        checks: AtomicUsize,
        path: PathBuf,
        parent: bool,
    }
    impl ChannelRuntime for SwapClock {
        fn now(&self) -> Duration {
            if self.checks.fetch_add(1, Ordering::SeqCst) == 1 {
                if self.parent {
                    fs::rename(
                        self.path.parent().unwrap(),
                        self.path.parent().unwrap().with_extension("original"),
                    )
                    .unwrap();
                    private_dir(self.path.parent().unwrap());
                } else {
                    fs::rename(&self.path, self.path.with_file_name("original-s")).unwrap();
                }
                let replacement = UnixListener::bind(&self.path).unwrap();
                fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600)).unwrap();
                drop(replacement);
            }
            Duration::ZERO
        }
        fn cancelled(&self) -> bool {
            false
        }
    }
    for parent in [false, true] {
        let (_temp, paths) = fixture();
        let (path, listener, socket) = bound_forward(&paths);
        drop(listener);
        // CleanupContext::new would read the clock before the operation. Keep
        // the two checks as the primitive's before/after-refusal barrier.
        let runtime = Arc::new(SwapClock {
            checks: AtomicUsize::new(0),
            path: path.socket_path.clone(),
            parent,
        });
        let ctx = CleanupContext {
            runtime,
            deadline: Duration::from_secs(5),
        };
        assert_eq!(
            PrivateChannelFiles::new().cleanup_if_refused(&path, Some(socket), &ctx),
            ForwardDisposition::Retained
        );
        assert!(path.socket_path.exists());
        assert_ne!(
            PrivateChannelFiles::new().validate_socket(&path).ok(),
            Some(socket)
        );
    }
}

#[test]
fn missing_socket_requires_a_valid_private_prior_binding_before_recovery() {
    for field in 0..5 {
        let (_temp, paths, leader, mut lease, service) = live_fixture();
        lease.publish(&service).unwrap();
        simulate_prior_process_identity(&paths);
        drop(lease.take_listener());
        fs::remove_file(&service.socket_path).unwrap();
        drop(leader);
        let leader = ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
        let record_path = paths.controller_state_root().join("rpc/service.json");
        let mut record: ServiceRecord =
            serde_json::from_slice(&fs::read(&record_path).unwrap()).unwrap();
        match field {
            0 => record.binding.socket.owner = record.binding.socket.owner.wrapping_add(1),
            1 => record.binding.socket.kind = libc::S_IFREG as u32,
            2 => record.binding.socket.mode = 0o666,
            3 => record.binding.socket.inode = 0,
            _ => record.executable.binding.kind = libc::S_IFDIR as u32,
        }
        let bytes = serde_json::to_vec(&record).unwrap();
        fs::write(&record_path, &bytes).unwrap();
        let pinned = lease.executable();
        let installed = RunningImage {
            path: lease.detached_runner_executable().into(),
            device: pinned.binding.device,
            inode: pinned.binding.inode,
        };
        assert!(
            bind_leader(&paths, &leader, &installed, &UuidString::new_v4()).is_err(),
            "field {field}"
        );
        assert_eq!(fs::read(&record_path).unwrap(), bytes);
        assert!(pinned.path.exists());
        assert!(!service.socket_path.exists());
    }
}
#[test]
fn lost_leader_guard_cannot_publish_an_optional_service() {
    let (_temp, paths, leader, mut lease, service) = live_fixture();
    drop(leader);
    assert!(lease.publish(&service).is_err());
    assert!(
        !paths
            .controller_state_root()
            .join("rpc/service.json")
            .exists()
    );
}
