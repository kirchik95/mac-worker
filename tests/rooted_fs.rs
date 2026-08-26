use std::{
    ffi::{CString, OsStr},
    fs,
    io::Read,
    os::unix::{
        ffi::OsStrExt,
        fs::{PermissionsExt, symlink},
    },
    path::{Path, PathBuf},
};

use mac_worker::{
    inputs::RelativePath,
    rooted_fs::{EntryKind, RootedDir},
};

struct RootFixture {
    directory: tempfile::TempDir,
    source: PathBuf,
    destination: PathBuf,
    outside: PathBuf,
}

impl RootFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let physical_directory = directory.path().canonicalize().unwrap();
        let source = physical_directory.join("source");
        let destination = physical_directory.join("destination");
        let outside = physical_directory.join("outside");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("sentinel.txt"), b"outside sentinel\n").unwrap();
        fs::set_permissions(
            outside.join("sentinel.txt"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        Self {
            directory,
            source,
            destination,
            outside,
        }
    }

    fn source(&self) -> &Path {
        &self.source
    }

    fn destination(&self) -> &Path {
        &self.destination
    }

    fn outside(&self) -> &Path {
        &self.outside
    }

    fn write_source(&self, path: &str, bytes: &[u8], mode: u32) {
        let path = self.source.join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    fn assert_outside_unchanged(&self) {
        assert_eq!(
            fs::read(self.outside.join("sentinel.txt")).unwrap(),
            b"outside sentinel\n"
        );
        assert_eq!(mode(self.outside.join("sentinel.txt")), 0o600);
        assert!(!self.outside.join("copied.txt").exists());
        assert!(!self.outside.join("created").exists());
    }
}

#[test]
fn physical_root_paths_reject_first_and_middle_ancestor_symlinks() {
    // Catches acquiring the root with one pathname-based open, which follows
    // symlinks in parent_path before descriptor-relative traversal begins.
    let fixture = tempfile::tempdir().unwrap();
    let physical = fixture.path().canonicalize().unwrap();
    let real = physical.join("real");
    let outside = physical.join("outside");
    fs::create_dir_all(real.join("middle/source")).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("sentinel.txt"), b"outside\n").unwrap();
    symlink(&real, physical.join("first-link")).unwrap();
    symlink(&outside, real.join("middle-link")).unwrap();

    for path in [
        physical.join("first-link/middle/source"),
        real.join("middle-link"),
    ] {
        let error = RootedDir::open(&path)
            .err()
            .expect("ancestor link rejected");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::ELOOP),
            "{}",
            path.display()
        );
    }

    for path in [
        physical.join("first-link/created"),
        real.join("middle-link/created"),
    ] {
        let error = RootedDir::create(&path)
            .err()
            .expect("ancestor link rejected");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::ELOOP),
            "{}",
            path.display()
        );
    }

    assert!(!real.join("created").exists());
    assert!(!outside.join("created").exists());
    assert_eq!(
        fs::read(outside.join("sentinel.txt")).unwrap(),
        b"outside\n"
    );
}

fn relative(path: &str) -> RelativePath {
    RelativePath::parse(path.as_bytes()).unwrap()
}

fn mode(path: impl AsRef<Path>) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

#[test]
fn inspection_reads_and_restats_the_opened_regular_file_after_a_path_swap() {
    // Catches fingerprinting reopening the selected pathname after inspection,
    // which could read a replacement symlink target outside the root.
    let fixture = RootFixture::new();
    fixture.write_source("selected.txt", b"selected bytes\n", 0o640);
    let source = RootedDir::open(fixture.source()).unwrap();
    let mut inspection = source.inspect(&relative("selected.txt")).unwrap();
    let before = inspection.metadata();
    fs::rename(
        fixture.source().join("selected.txt"),
        fixture.source().join("moved-selected.txt"),
    )
    .unwrap();
    symlink(
        fixture.outside().join("sentinel.txt"),
        fixture.source().join("selected.txt"),
    )
    .unwrap();

    let mut bytes = Vec::new();
    inspection.read_to_end(&mut bytes).unwrap();
    let after = inspection.restat().unwrap();

    assert_eq!(bytes, b"selected bytes\n");
    assert_eq!(before, after);
    assert_eq!(before.kind, EntryKind::RegularFile);
    assert_eq!(before.mode, 0o640);
    fixture.assert_outside_unchanged();
}

#[test]
fn regular_binary_bytes_and_modes_are_copied_without_ambient_mode_bits() {
    // Catches path-based copying, lossy byte handling, and preservation of
    // writable or special source mode bits in an immutable snapshot.
    let fixture = RootFixture::new();
    fixture.write_source("nested/data.bin", b"\0\xffbinary\n\0", 0o664);
    fixture.write_source("nested/tool", b"#!/bin/sh\nexit 0\n", 0o641);
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();

    source
        .copy_regular_to(&relative("nested/data.bin"), &destination)
        .unwrap();
    source
        .copy_regular_to(&relative("nested/tool"), &destination)
        .unwrap();

    assert_eq!(
        fs::read(fixture.destination().join("nested/data.bin")).unwrap(),
        b"\0\xffbinary\n\0"
    );
    assert_eq!(mode(fixture.destination().join("nested/data.bin")), 0o444);
    assert_eq!(
        fs::read(fixture.destination().join("nested/tool")).unwrap(),
        b"#!/bin/sh\nexit 0\n"
    );
    assert_eq!(mode(fixture.destination().join("nested/tool")), 0o555);
}

#[test]
fn empty_regular_files_are_created_without_fabricated_bytes() {
    // Catches copy paths that omit zero-length files or add placeholder data.
    let fixture = RootFixture::new();
    fixture.write_source("empty.txt", b"", 0o600);
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();

    source
        .copy_regular_to(&relative("empty.txt"), &destination)
        .unwrap();

    assert_eq!(
        fs::read(fixture.destination().join("empty.txt")).unwrap(),
        b""
    );
    assert_eq!(mode(fixture.destination().join("empty.txt")), 0o444);
}

#[test]
fn explicit_empty_directories_are_created_and_published_read_only() {
    // Catches silently dropping declared empty directories and making only
    // regular files, rather than every snapshot directory, read-only.
    let fixture = RootFixture::new();
    let destination = RootedDir::create(fixture.destination()).unwrap();

    destination
        .create_empty_directory(&relative("fixtures/deep/empty"))
        .unwrap();
    destination.make_read_only().unwrap();

    assert!(fixture.destination().join("fixtures/deep/empty").is_dir());
    assert_eq!(mode(fixture.destination()), 0o555);
    assert_eq!(mode(fixture.destination().join("fixtures")), 0o555);
    assert_eq!(mode(fixture.destination().join("fixtures/deep")), 0o555);
    assert_eq!(
        mode(fixture.destination().join("fixtures/deep/empty")),
        0o555
    );

    destination.remove_owned_tree().unwrap();
    assert!(!fixture.destination().exists());
}

#[test]
fn symlink_targets_are_read_and_recreated_byte_exact_without_dereferencing() {
    // Catches copying a link target's contents or normalizing its relative,
    // absolute-looking, whitespace, or newline-bearing target text.
    let fixture = RootFixture::new();
    fs::create_dir(fixture.source().join("links")).unwrap();
    let target = "../outside/sentinel.txt\n";
    symlink(target, fixture.source().join("links/value")).unwrap();
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();
    let path = relative("links/value");

    let inspection = source.inspect(&path).unwrap();
    assert_eq!(inspection.kind, EntryKind::Symlink);
    assert_eq!(source.read_symlink(&path).unwrap(), target);
    destination
        .create_symlink(&path, &source.read_symlink(&path).unwrap())
        .unwrap();

    assert_eq!(
        fs::read_link(fixture.destination().join("links/value")).unwrap(),
        PathBuf::from(target)
    );
    fixture.assert_outside_unchanged();
}

#[test]
fn non_utf8_symlink_targets_fail_as_unsupported_path_encoding() {
    // Catches replacement-character conversion of a byte-exact Unix link
    // target, which would silently change the snapshot.
    let fixture = RootFixture::new();
    symlink(
        OsStr::from_bytes(b"bad\xfftarget"),
        fixture.source().join("bad-link"),
    )
    .unwrap();
    let source = RootedDir::open(fixture.source()).unwrap();

    let error = source.read_symlink(&relative("bad-link")).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn a_final_symlink_is_inspected_but_never_opened_as_a_regular_file() {
    // Catches a final-component symlink being followed after no-follow parent
    // traversal succeeded.
    let fixture = RootFixture::new();
    symlink(
        fixture.outside().join("sentinel.txt"),
        fixture.source().join("selected"),
    )
    .unwrap();
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();
    let path = relative("selected");

    assert_eq!(source.inspect(&path).unwrap().kind, EntryKind::Symlink);
    let error = source.copy_regular_to(&path, &destination).unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
    assert!(!fixture.destination().join("selected").exists());
    fixture.assert_outside_unchanged();
}

#[test]
fn a_symlink_in_each_source_parent_position_is_rejected() {
    // Catches traversing only the first or final component with O_NOFOLLOW.
    for (link, selected) in [
        ("first", "first/secret.txt"),
        ("real/middle", "real/middle/secret.txt"),
    ] {
        let fixture = RootFixture::new();
        fs::create_dir_all(fixture.source().join("real")).unwrap();
        symlink(fixture.outside(), fixture.source().join(link)).unwrap();
        let source = RootedDir::open(fixture.source()).unwrap();
        let destination = RootedDir::create(fixture.destination()).unwrap();

        let error = source
            .copy_regular_to(&relative(selected), &destination)
            .unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::ELOOP), "{link}");
        fixture.assert_outside_unchanged();
    }
}

#[test]
fn destination_parent_symlinks_never_receive_files_directories_or_links() {
    // Catches destination helpers disagreeing about no-follow traversal and
    // writing any selected entry through a planted parent symlink.
    let fixture = RootFixture::new();
    fixture.write_source("redirect/copied.txt", b"inside\n", 0o644);
    fs::create_dir(fixture.destination()).unwrap();
    symlink(fixture.outside(), fixture.destination().join("redirect")).unwrap();
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::open(fixture.destination()).unwrap();

    let errors = [
        source
            .copy_regular_to(&relative("redirect/copied.txt"), &destination)
            .unwrap_err(),
        destination
            .create_empty_directory(&relative("redirect/created"))
            .unwrap_err(),
        destination
            .create_symlink(&relative("redirect/link"), "sentinel.txt")
            .unwrap_err(),
    ];

    assert!(
        errors
            .iter()
            .all(|error| error.raw_os_error() == Some(libc::ELOOP))
    );
    fixture.assert_outside_unchanged();
}

#[test]
fn a_parent_swapped_to_a_symlink_after_root_open_is_rejected() {
    // Catches resolving a selected path lexically from the root pathname on
    // each operation instead of traversing from the already-open descriptor.
    let fixture = RootFixture::new();
    fixture.write_source("selected/original.txt", b"original\n", 0o644);
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();
    fs::rename(
        fixture.source().join("selected"),
        fixture.source().join("moved-selected"),
    )
    .unwrap();
    symlink(fixture.outside(), fixture.source().join("selected")).unwrap();

    let error = source
        .copy_regular_to(&relative("selected/sentinel.txt"), &destination)
        .unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
    assert!(!fixture.destination().join("selected/sentinel.txt").exists());
    fixture.assert_outside_unchanged();
}

#[test]
fn fifo_socket_and_device_entries_are_rejected_with_einval() {
    // Catches accepting filesystem objects whose reads can block, produce
    // ambient device data, or escape ordinary-file snapshot semantics.
    let fixture = RootFixture::new();
    let fifo = fixture.source().join("fifo");
    let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: `fifo_name` is a live NUL-terminated pathname for this call.
    assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);
    let source = RootedDir::open(fixture.source()).unwrap();

    let error = source.inspect(&relative("fifo")).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));

    #[cfg(target_os = "macos")]
    {
        let sockets = RootedDir::open(Path::new("/private/var/run")).unwrap();
        let error = sockets.inspect(&relative("syslog")).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _socket =
            std::os::unix::net::UnixListener::bind(fixture.source().join("socket")).unwrap();
        let error = source.inspect(&relative("socket")).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
    }

    let devices = RootedDir::open(Path::new("/dev")).unwrap();
    let error = devices.inspect(&relative("null")).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
}

#[test]
fn existing_destination_entries_are_never_truncated_or_replaced() {
    // Catches using O_TRUNC, non-exclusive creation, or unlink-before-copy on
    // a path that was not created by the current operation.
    let fixture = RootFixture::new();
    fixture.write_source("same.txt", b"new bytes\n", 0o644);
    fs::create_dir(fixture.destination()).unwrap();
    fs::write(fixture.destination().join("same.txt"), b"keep me\n").unwrap();
    symlink(
        fixture.outside().join("sentinel.txt"),
        fixture.destination().join("existing-link"),
    )
    .unwrap();
    fixture.write_source("existing-link", b"replacement\n", 0o644);
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::open(fixture.destination()).unwrap();

    for path in [relative("same.txt"), relative("existing-link")] {
        let error = source.copy_regular_to(&path, &destination).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EEXIST));
    }

    assert_eq!(
        fs::read(fixture.destination().join("same.txt")).unwrap(),
        b"keep me\n"
    );
    assert_eq!(
        fs::read_link(fixture.destination().join("existing-link")).unwrap(),
        fixture.outside().join("sentinel.txt")
    );
    fixture.assert_outside_unchanged();
}

#[test]
fn read_only_cleanup_unlinks_a_planted_symlink_without_touching_its_target() {
    // Catches cleanup following a link, depending on writable published
    // modes, or deleting a sibling outside the exact owned root.
    let fixture = RootFixture::new();
    fixture.write_source("nested/file.txt", b"snapshot\n", 0o644);
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();
    source
        .copy_regular_to(&relative("nested/file.txt"), &destination)
        .unwrap();
    destination
        .create_symlink(
            &relative("nested/outside"),
            fixture.outside().to_str().unwrap(),
        )
        .unwrap();
    destination.make_read_only().unwrap();

    destination.remove_owned_tree().unwrap();

    assert!(!fixture.destination().exists());
    fixture.assert_outside_unchanged();
    assert!(fixture.directory.path().join("source").is_dir());
}

#[test]
fn cleanup_refuses_a_root_path_replaced_by_a_symlink() {
    // Catches cleanup deleting a replacement object after the exact opened
    // root was renamed away.
    let fixture = RootFixture::new();
    let destination = RootedDir::create(fixture.destination()).unwrap();
    destination
        .create_empty_directory(&relative("owned/empty"))
        .unwrap();
    let moved = fixture.directory.path().join("moved-owned-tree");
    fs::rename(fixture.destination(), &moved).unwrap();
    symlink(fixture.outside(), fixture.destination()).unwrap();

    let error = destination.remove_owned_tree().unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
    assert!(moved.join("owned/empty").is_dir());
    fixture.assert_outside_unchanged();
}
