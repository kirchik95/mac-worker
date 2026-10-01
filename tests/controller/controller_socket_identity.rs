//! T4 rooted image, identity, pin and forward lifecycle acceptance.
use mac_worker::{
    controller::{ControllerLeader, ControllerReadReply, decode_frame},
    controller::channel::{
        contracts::*,
        files::{PrivateChannelFiles, bind_leader},
        identity::{serve_identity_selector, is_socket_selector},
        image::SystemRunningImageSource,
        pin::PrivatePinStore,
        testing::{identity_fixture, request_fixture, ManualRuntime, StubCodec, ScriptedImageSource},
    },
    paths::PathLayout,
};
use serde_json::json;
use std::{
    fs,
    os::unix::{fs::{MetadataExt, PermissionsExt, symlink}, net::UnixListener},
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    time::Duration,
};

fn fixture() -> (tempfile::TempDir, PathLayout) {
    let temp = tempfile::tempdir_in("/private/tmp").unwrap();
    let p = temp.path();
    let paths = PathLayout { config:p.join("config"), state:p.join("state"), cache:p.join("cache"), data:p.join("data") };
    (temp,paths)
}
fn private_dir(path: &Path) {
    fs::create_dir_all(path).unwrap();
    fs::set_permissions(path,fs::Permissions::from_mode(0o700)).unwrap();
}
fn private_file(path: &Path, bytes: &[u8]) {
    private_dir(path.parent().unwrap());
    fs::write(path,bytes).unwrap();
    fs::set_permissions(path,fs::Permissions::from_mode(0o600)).unwrap();
}
fn pin_path(paths:&PathLayout, identity:&SocketIdentity)->PathBuf {
    paths.controller_cache_root().join("channel/pins").join(format!("{}.json",identity.route_sha256))
}
fn image(path:&Path)->RunningImage {
    fs::write(path,b"#!/bin/sh\nprintf old").unwrap();
    fs::set_permissions(path,fs::Permissions::from_mode(0o755)).unwrap();
    let meta=fs::metadata(path).unwrap();
    RunningImage{path:fs::canonicalize(path).unwrap(),device:meta.dev(),inode:meta.ino()}
}
fn service(paths:&PathLayout, leader:&ControllerLeader, generation:&UuidString)->ServiceIdentity {
    let mut value=identity_fixture().service;
    value.leader=leader.identity();
    value.service_generation=generation.clone();
    value.socket_path=paths.controller_state_root().join("rpc/s");
    value
}
fn bound_forward(paths:&PathLayout)->(ForwardPath,UnixListener,EntryIdentity) {
    let files=PrivateChannelFiles::new();
    let allocation=files.allocate(paths).unwrap();
    let listener=UnixListener::bind(&allocation.socket_path).unwrap();
    fs::set_permissions(&allocation.socket_path,fs::Permissions::from_mode(0o600)).unwrap();
    let evidence=files.validate_socket(&allocation).unwrap();
    (allocation,listener,evidence)
}

#[test]
fn first_pin_has_only_stable_fields_and_restart_preserves_bytes_mode_inode() {
    let (_temp,paths)=fixture();
    let mut identity=identity_fixture();
    let store=PrivatePinStore::new();
    store.verify_or_create(&paths,&identity).unwrap();
    let path=pin_path(&paths,&identity);
    let bytes=fs::read(&path).unwrap();
    let metadata=fs::metadata(&path).unwrap();
    assert_eq!(metadata.mode()&0o7777,0o600);
    assert_eq!(metadata.nlink(),1);
    assert_eq!(fs::metadata(path.parent().unwrap()).unwrap().mode()&0o7777,0o700);
    assert_eq!(serde_json::from_slice::<Pin>(&bytes).unwrap(),Pin::from_identity(&identity));
    assert!(!String::from_utf8(bytes.clone()).unwrap().contains(identity.service.service_generation.as_str()));
    identity.service.service_generation=UuidString::new_v4();
    identity.service.journal_id=None;
    store.verify_or_create(&paths,&identity).unwrap();
    assert_eq!(fs::read(path.clone()).unwrap(),bytes);
    assert_eq!(fs::metadata(path).unwrap().ino(),metadata.ino());
}
#[test]
fn concurrent_pin_bootstrap_checks_the_winner() {
    let (_temp,paths)=fixture();
    let identity=identity_fixture();
    let gate=Arc::new(Barrier::new(3));
    let joins:Vec<_>=(0..2).map(|_| {
        let (paths,identity,gate)=(paths.clone(),identity.clone(),gate.clone());
        std::thread::spawn(move || {gate.wait();PrivatePinStore::new().verify_or_create(&paths,&identity)})
    }).collect();
    gate.wait();
    for join in joins {join.join().unwrap().unwrap();}
    assert_eq!(serde_json::from_slice::<Pin>(&fs::read(pin_path(&paths,&identity)).unwrap()).unwrap(),Pin::from_identity(&identity));
}
#[test]
fn client_account_and_stored_route_mismatch_never_rotate_a_pin() {
    let (_temp,paths)=fixture();
    let identity=identity_fixture();
    let store=PrivatePinStore::new();
    store.verify_or_create(&paths,&identity).unwrap();
    let path=pin_path(&paths,&identity);
    let (bytes,inode)=(fs::read(&path).unwrap(),fs::metadata(&path).unwrap().ino());
    for case in 0..4 {
        let mut peer=identity.clone();
        match case {0=>peer.service.controller_client_id=mac_worker::job::ClientId::generate(),1=>peer.service.account.uid+=1,2=>peer.service.account.home="/Users/other".into(),_=>peer.service.account.username="other".into()}
        assert!(store.verify_or_create(&paths,&peer).is_err());
        assert_eq!(fs::read(&path).unwrap(),bytes);
        assert_eq!(fs::metadata(&path).unwrap().ino(),inode);
    }
    let mut pin=Pin::from_identity(&identity);
    pin.route_sha256=RouteDigest::parse(&"b".repeat(64)).unwrap();
    fs::write(&path,serde_json::to_vec(&pin).unwrap()).unwrap();
    assert!(store.verify_or_create(&paths,&identity).is_err());
}
#[test]
fn unsafe_corrupt_hardlink_symlink_and_oversize_pins_are_preserved() {
    let (_temp,paths)=fixture();
    let identity=identity_fixture();
    let store=PrivatePinStore::new();
    store.verify_or_create(&paths,&identity).unwrap();
    let path=pin_path(&paths,&identity);
    let original=fs::read(&path).unwrap();
    for bytes in [b"invalid json".to_vec(),vec![b' ';4097],br#"{"schema_version":9}"#.to_vec()] {
        fs::write(&path,&bytes).unwrap();
        assert!(store.verify_or_create(&paths,&identity).is_err());
        assert!(store.repin(&paths,&identity,identity.service.controller_client_id).is_err());
        assert_eq!(fs::read(&path).unwrap(),bytes);
    }
    fs::write(&path,&original).unwrap();
    fs::set_permissions(&path,fs::Permissions::from_mode(0o644)).unwrap();
    assert!(store.verify_or_create(&paths,&identity).is_err());
    assert_eq!(fs::metadata(&path).unwrap().mode()&0o7777,0o644);
    fs::set_permissions(&path,fs::Permissions::from_mode(0o600)).unwrap();
    let alias=path.with_extension("alias");
    fs::hard_link(&path,&alias).unwrap();
    assert!(store.repin(&paths,&identity,identity.service.controller_client_id).is_err());
    fs::remove_file(&alias).unwrap();
    fs::rename(&path,&alias).unwrap();
    symlink(&alias,&path).unwrap();
    assert!(store.verify_or_create(&paths,&identity).is_err());
    assert_eq!(fs::read(alias).unwrap(),original);
}
#[test]
fn repin_requires_expected_fresh_client_and_preserves_notify_and_envelopes() {
    let (_temp,paths)=fixture();
    let old=identity_fixture();
    let store=PrivatePinStore::new();
    store.verify_or_create(&paths,&old).unwrap();
    let path=pin_path(&paths,&old);
    let (original,inode)=(fs::read(&path).unwrap(),fs::metadata(&path).unwrap().ino());
    let files=[paths.controller_cache_root().join("events/legacy/notify.json"),paths.controller_cache_root().join("events/legacy/notify.lock"),paths.controller_cache_root().join("envelope.json")];
    for file in &files {private_file(file,b"retain");}
    let evidence:Vec<_>=files.iter().map(|file|fs::metadata(file).unwrap()).collect();
    let mut fresh=old.clone();
    fresh.service.controller_client_id=mac_worker::job::ClientId::generate();
    assert!(store.repin(&paths,&fresh,old.service.controller_client_id).is_err());
    assert_eq!(fs::read(&path).unwrap(),original);
    store.repin(&paths,&fresh,fresh.service.controller_client_id).unwrap();
    assert_ne!(fs::metadata(&path).unwrap().ino(),inode);
    assert_eq!(serde_json::from_slice::<Pin>(&fs::read(&path).unwrap()).unwrap(),Pin::from_identity(&fresh));
    for (file,meta) in files.iter().zip(evidence) {
        assert_eq!(fs::read(file).unwrap(),b"retain");
        assert_eq!((fs::metadata(file).unwrap().ino(),fs::metadata(file).unwrap().mode()),(meta.ino(),meta.mode()));
    }
}
#[test]
fn fresh_private_forward_cleans_only_settled_positive_refusal() {
    let (_temp,paths)=fixture();
    let files=PrivateChannelFiles::new();
    let (path,listener,socket)=bound_forward(&paths);
    let second=files.allocate(&paths).unwrap();
    assert_ne!(path.directory,second.directory);
    assert_eq!(fs::metadata(&path.directory).unwrap().mode()&0o7777,0o700);
    let ctx=CleanupContext::new(Arc::new(ManualRuntime::default()));
    assert_eq!(files.cleanup_if_refused(&path,Some(socket),&ctx),ForwardDisposition::Retained);
    drop(listener); // producer settled; live listener no longer exists
    assert_eq!(files.cleanup_if_refused(&path,None,&ctx),ForwardDisposition::Retained);
    assert!(path.socket_path.exists());
    assert_eq!(files.cleanup_if_refused(&path,Some(socket),&ctx),ForwardDisposition::Cleaned);
    assert!(!path.directory.exists());
    assert!(second.directory.exists());
}
#[test]
fn expired_missing_and_swapped_forward_evidence_retains_residue() {
    let (_temp,paths)=fixture();
    let files=PrivateChannelFiles::new();
    let missing=files.allocate(&paths).unwrap();
    let runtime=Arc::new(ManualRuntime::default());
    let ctx=CleanupContext::new(runtime.clone());
    assert_eq!(files.cleanup_if_refused(&missing,None,&ctx),ForwardDisposition::Retained);
    let (path,listener,socket)=bound_forward(&paths);
    drop(listener);
    runtime.advance(Duration::from_secs(5));
    assert_eq!(files.cleanup_if_refused(&path,Some(socket),&ctx),ForwardDisposition::Retained);
    let ctx=CleanupContext::new(Arc::new(ManualRuntime::default()));
    fs::rename(&path.socket_path,path.directory.join("old")).unwrap();
    let replacement=UnixListener::bind(&path.socket_path).unwrap();
    fs::set_permissions(&path.socket_path,fs::Permissions::from_mode(0o600)).unwrap();
    drop(replacement);
    assert_eq!(files.cleanup_if_refused(&path,Some(socket),&ctx),ForwardDisposition::Retained);
    assert!(path.directory.join("old").exists());
    assert!(path.socket_path.exists());
}
#[test]
fn pinned_image_survives_replacement_rollback_and_requires_rpc_exit_proof() {
    let (temp,paths)=fixture();
    let installed=image(&temp.path().join("worker"));
    let leader=ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let generation=UuidString::new_v4();
    let mut lease=bind_leader(&paths,&leader,&installed,&generation).unwrap();
    let executable=lease.executable();
    assert!(executable.path.file_name().unwrap().to_str().unwrap().contains(&generation.as_str().replace('-',"")));
    assert_eq!(lease.detached_runner_executable(),installed.path);
    assert_eq!(fs::metadata(&installed.path).unwrap().mode()&0o7777,0o755);
    lease.publish(&service(&paths,&leader,&generation)).unwrap();
    let old=temp.path().join("old");
    fs::rename(&installed.path,&old).unwrap();
    fs::write(&installed.path,b"#!/bin/sh\nprintf new").unwrap();
    fs::set_permissions(&installed.path,fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(std::process::Command::new(&executable.path).output().unwrap().stdout,b"old");
    assert_eq!(std::process::Command::new(lease.detached_runner_executable()).output().unwrap().stdout,b"new");
    fs::rename(old,&installed.path).unwrap();
    assert_eq!(std::process::Command::new(&executable.path).output().unwrap().stdout,b"old");
    assert_eq!(lease.withdraw(false),ForwardDisposition::Retained);
    assert!(executable.path.exists());
    assert_eq!(lease.withdraw(true),ForwardDisposition::Cleaned);
    assert!(!executable.path.exists());
    assert!(installed.path.exists());
}
#[test]
fn changed_or_unverifiable_image_never_publishes_channel_state() {
    let (temp,paths)=fixture();
    let installed=image(&temp.path().join("worker"));
    let leader=ControllerLeader::acquire(&paths.controller_state_root()).unwrap();
    let replacement=temp.path().join("replacement");
    image(&replacement);
    fs::rename(replacement,&installed.path).unwrap();
    assert!(bind_leader(&paths,&leader,&installed,&UuidString::new_v4()).is_err());
    assert!(!paths.controller_state_root().join("rpc/service.json").exists());
    let unsupported=ScriptedImageSource::new(vec![Err(ChannelFailure::Unavailable(ChannelReason::Unsupported))]);
    assert!(unsupported.capture().is_err());
}
#[cfg(target_os="macos")]
#[test]
fn system_image_matches_the_loaded_main_image_and_canonical_installed_path() {
    let captured=SystemRunningImageSource::new().capture().unwrap();
    let path=fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
    let metadata=fs::metadata(&path).unwrap();
    assert_eq!(captured,RunningImage{path,device:metadata.dev(),inode:metadata.ino()});
}
#[test]
fn identity_selector_rejects_mixed_grammar_and_creates_no_missing_state() {
    let (_temp,paths)=fixture();
    let runtime=ManualRuntime::default();
    let ctx=ClientContext{runtime:&runtime,deadline:Duration::from_secs(30),should_stop:&||false};
    let body=json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64)}});
    let request=request_fixture("task.list",body.clone());
    assert!(is_socket_selector(&request));
    let frame=serve_identity_selector(&request,&paths,Path::new("/Users/controller"),&StubCodec,&ctx).unwrap();
    let reply:ControllerReadReply<SocketIdentityResult>=serde_json::from_slice(decode_frame(&frame).unwrap()).unwrap();
    reply.verify_envelope(&request).unwrap();
    assert!(matches!(reply.result(),SocketIdentityResult::Unavailable(_)));
    assert!(!paths.state.exists());
    assert!(!paths.controller_state_root().exists());
    for body in [json!({"controller_socket":null}),json!({"controller_socket":{"op":"identity","route_sha256":"A".repeat(64)}}),json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64)},"controller_health":true}),json!({"controller_socket":{"op":"identity","route_sha256":"a".repeat(64),"extra":1}})] {
        let request=request_fixture("task.list",body);
        assert!(is_socket_selector(&request));
        assert!(serve_identity_selector(&request,&paths,Path::new("/Users/controller"),&StubCodec,&ctx).is_err());
    }
    assert!(!paths.state.exists());
    assert!(!paths.controller_state_root().exists());
}
