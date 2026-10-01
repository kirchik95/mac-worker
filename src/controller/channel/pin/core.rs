//! Stable private pin storage; no volatile generation or journal fields.
use super::Pin as PinDocument;
use crate::{
    inputs::RelativePath,
    job::ClientId,
    paths::PathLayout,
    rooted_fs::{PrivateEntryIdentity, RootedDir},
};
use std::io;

fn invalid() -> io::Error {
    io::Error::from_raw_os_error(libc::EINVAL)
}

pub(crate) fn validate(pin: &PinDocument) -> io::Result<()> {
    pin.validate().map_err(|_| invalid())
}

pub(crate) fn open_pin_directory(paths: &PathLayout) -> io::Result<RootedDir> {
    let require_private = |directory: &RootedDir| -> io::Result<()> {
        let metadata = directory.root_metadata()?;
        if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o7777 != 0o700 {
            return Err(io::Error::from_raw_os_error(libc::EACCES));
        }
        Ok(())
    };
    let root = RootedDir::open_or_create_anchored_absolute(&paths.controller_cache_root())?;
    require_private(&root)?;
    let channel = root.open_child_directory(
        &RelativePath::parse(b"channel").map_err(|_| invalid())?,
        true,
    )?;
    require_private(&channel)?;
    let pins = channel
        .open_child_directory(&RelativePath::parse(b"pins").map_err(|_| invalid())?, true)?;
    require_private(&pins)?;
    Ok(pins)
}

fn read_pin(
    root: &RootedDir,
    name: &str,
) -> io::Result<(PinDocument, Vec<u8>, PrivateEntryIdentity)> {
    let binding = root.private_entry_identity(name)?;
    let bytes = root.read_private_regular(name, 4096)?;
    if root.private_entry_identity(name)? != binding {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    let pin: PinDocument = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    validate(&pin)?;
    Ok((pin, bytes, binding))
}

pub(crate) fn verify_or_create(paths: &PathLayout, pin: &PinDocument) -> io::Result<()> {
    validate(pin)?;
    let bytes = serde_json::to_vec(pin).map_err(|_| invalid())?;
    if bytes.len() > 4096 {
        return Err(invalid());
    }
    let root = open_pin_directory(paths)?;
    let name = format!("{}.json", pin.route_sha256);
    match read_pin(&root, &name) {
        Ok((existing, _, _)) if existing == *pin => return Ok(()),
        Ok(_) => return Err(io::Error::from_raw_os_error(libc::ESTALE)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match root.write_private_atomic_no_replace(&name, &bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    // A concurrent winner is trusted only after a private, bounded reread.
    if read_pin(&root, &name)?.0 != *pin {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    Ok(())
}

pub(crate) fn repin(paths: &PathLayout, pin: &PinDocument, expected: ClientId) -> io::Result<()> {
    repin_with_hook(paths, pin, expected, || {})
}

fn repin_with_hook(
    paths: &PathLayout,
    pin: &PinDocument,
    expected: ClientId,
    before_replace: impl FnOnce(),
) -> io::Result<()> {
    validate(pin)?;
    if expected != pin.controller_client_id {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    let root = open_pin_directory(paths)?;
    let name = format!("{}.json", pin.route_sha256);
    let (previous, bytes, binding) = match read_pin(&root, &name) {
        Ok(previous) => previous,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return verify_or_create(paths, pin);
        }
        Err(error) => return Err(error),
    };
    if previous.route_sha256 != pin.route_sha256 {
        return Err(invalid());
    }
    let replacement = serde_json::to_vec(pin).map_err(|_| invalid())?;
    if replacement.len() > 4096 {
        return Err(invalid());
    }
    before_replace();
    root.channel_replace_private_exact(&name, binding, &bytes, &replacement)?;
    if read_pin(&root, &name)?.0 != *pin {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    Ok(())
}
