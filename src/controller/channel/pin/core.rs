//! Stable private pin storage; no volatile generation or journal fields.
use crate::{
    inputs::RelativePath,
    job::ClientId,
    paths::PathLayout,
    rooted_fs::{PrivateEntryIdentity, RootedDir},
};
use serde::{Deserialize, Serialize};
use std::{io, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Account {
    pub(crate) uid: u32,
    pub(crate) username: String,
    pub(crate) home: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinDocument {
    pub(crate) schema_version: u32,
    pub(crate) route_sha256: String,
    pub(crate) controller_client_id: ClientId,
    pub(crate) account: Account,
}

fn invalid() -> io::Error {
    io::Error::from_raw_os_error(libc::EINVAL)
}

pub(crate) fn validate(pin: &PinDocument) -> io::Result<()> {
    let home = pin.account.home.to_str().ok_or_else(invalid)?;
    if pin.schema_version != 1
        || pin.route_sha256.len() != 64
        || !pin
            .route_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || pin.account.username.is_empty()
        || pin.account.username.len() > 256
        || pin.account.username.chars().any(char::is_control)
        || !pin.account.home.is_absolute()
        || home.len() > 1024
        || home.chars().any(char::is_control)
        || pin.account.home.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn open_pin_directory(paths: &PathLayout) -> io::Result<RootedDir> {
    let root = RootedDir::open_or_create_anchored_absolute(&paths.controller_cache_root())?;
    let metadata = root.root_metadata()?;
    if metadata.st_uid != unsafe { libc::geteuid() } || metadata.st_mode & 0o7777 != 0o700 {
        return Err(io::Error::from_raw_os_error(libc::EACCES));
    }
    let channel = root.open_child_directory(
        &RelativePath::parse(b"channel").map_err(|_| invalid())?,
        true,
    )?;
    channel.open_child_directory(&RelativePath::parse(b"pins").map_err(|_| invalid())?, true)
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
    root.replace_private_regular_bound(&name, binding, bytes.len() as u64, &replacement)?;
    if read_pin(&root, &name)?.0 != *pin {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt, symlink},
        sync::{Arc, Barrier},
    };

    fn fixture() -> (tempfile::TempDir, PathLayout, PinDocument) {
        let temp = tempfile::tempdir().unwrap();
        let paths = PathLayout {
            config: temp.path().join("config"),
            state: temp.path().join("state"),
            cache: temp.path().join("cache"),
            data: temp.path().join("data"),
        };
        let pin = PinDocument {
            schema_version: 1,
            route_sha256: "a".repeat(64),
            controller_client_id: "11111111111141118111111111111111".parse().unwrap(),
            account: Account {
                uid: 501,
                username: "controller".into(),
                home: "/Users/controller".into(),
            },
        };
        (temp, paths, pin)
    }

    fn path(paths: &PathLayout, pin: &PinDocument) -> PathBuf {
        paths
            .controller_cache_root()
            .join("channel/pins")
            .join(format!("{}.json", pin.route_sha256))
    }

    #[test]
    fn first_pin_is_durable_private_and_contains_only_stable_fields() {
        let (_temp, paths, pin) = fixture();
        verify_or_create(&paths, &pin).unwrap();
        let path = path(&paths, &pin);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o7777,
            0o600
        );
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o700
        );
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.len() <= 4096);
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"schema_version":1,"route_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","controller_client_id":"11111111111141118111111111111111","account":{"uid":501,"username":"controller","home":"/Users/controller"}})
        );
        let inode = fs::metadata(&path).unwrap().ino();
        verify_or_create(&paths, &pin).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn concurrent_bootstrap_rereads_the_winning_pin() {
        let (_temp, paths, pin) = fixture();
        let gate = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let paths = paths.clone();
            let pin = pin.clone();
            let gate = gate.clone();
            handles.push(std::thread::spawn(move || {
                gate.wait();
                verify_or_create(&paths, &pin)
            }));
        }
        gate.wait();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        let stored: PinDocument =
            serde_json::from_slice(&fs::read(path(&paths, &pin)).unwrap()).unwrap();
        assert_eq!(stored, pin);
    }

    #[test]
    fn client_account_and_stored_route_mismatches_never_rotate() {
        let (_temp, paths, pin) = fixture();
        verify_or_create(&paths, &pin).unwrap();
        let file = path(&paths, &pin);
        let bytes = fs::read(&file).unwrap();
        let inode = fs::metadata(&file).unwrap().ino();
        for field in 0..4 {
            let mut other = pin.clone();
            match field {
                0 => other.controller_client_id = ClientId::generate(),
                1 => other.account.uid += 1,
                2 => other.account.home = "/Users/other".into(),
                _ => other.account.username = "other".into(),
            }
            assert!(verify_or_create(&paths, &other).is_err());
        }
        assert_eq!(fs::read(&file).unwrap(), bytes);
        assert_eq!(fs::metadata(&file).unwrap().ino(), inode);
        let mut other = pin.clone();
        other.route_sha256 = "b".repeat(64);
        fs::write(&file, serde_json::to_vec(&other).unwrap()).unwrap();
        assert!(verify_or_create(&paths, &pin).is_err());
    }

    #[test]
    fn unsafe_corrupt_hardlinked_and_oversize_pins_are_preserved() {
        let (_temp, paths, pin) = fixture();
        verify_or_create(&paths, &pin).unwrap();
        let file = path(&paths, &pin);
        let original = fs::read(&file).unwrap();
        for bytes in [
            b"invalid json".to_vec(),
            vec![b' '; 4097],
            br#"{"schema_version":9}"#.to_vec(),
        ] {
            fs::write(&file, &bytes).unwrap();
            assert!(verify_or_create(&paths, &pin).is_err());
            assert!(repin(&paths, &pin, pin.controller_client_id).is_err());
            assert_eq!(fs::read(&file).unwrap(), bytes);
        }
        fs::write(&file, &original).unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(verify_or_create(&paths, &pin).is_err());
        assert_eq!(
            fs::metadata(&file).unwrap().permissions().mode() & 0o7777,
            0o644
        );
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
        let alias = file.with_extension("alias");
        fs::hard_link(&file, &alias).unwrap();
        assert!(verify_or_create(&paths, &pin).is_err());
        assert!(repin(&paths, &pin, pin.controller_client_id).is_err());
        fs::remove_file(&alias).unwrap();
        fs::rename(&file, &alias).unwrap();
        symlink(&alias, &file).unwrap();
        assert!(verify_or_create(&paths, &pin).is_err());
        assert_eq!(fs::read(&alias).unwrap(), original);
    }

    #[test]
    fn explicit_repin_requires_fresh_expected_client_and_exact_inode() {
        let (_temp, paths, old) = fixture();
        verify_or_create(&paths, &old).unwrap();
        let file = path(&paths, &old);
        let original = fs::read(&file).unwrap();
        let inode = fs::metadata(&file).unwrap().ino();
        let mut fresh = old.clone();
        fresh.controller_client_id = ClientId::generate();
        assert!(repin(&paths, &fresh, old.controller_client_id).is_err());
        assert_eq!(fs::read(&file).unwrap(), original);
        repin(&paths, &fresh, fresh.controller_client_id).unwrap();
        assert_ne!(fs::metadata(&file).unwrap().ino(), inode);
        assert_eq!(
            serde_json::from_slice::<PinDocument>(&fs::read(&file).unwrap()).unwrap(),
            fresh
        );
        assert!(
            repin_with_hook(&paths, &fresh, fresh.controller_client_id, || {
                fs::rename(&file, file.with_extension("old")).unwrap();
                fs::write(&file, &original).unwrap();
                fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
            })
            .is_err()
        );
        assert_eq!(fs::read(&file).unwrap(), original);
    }

    #[test]
    fn pin_storage_preserves_notify_cache_and_mutation_envelopes() {
        let (_temp, paths, pin) = fixture();
        fs::create_dir_all(paths.controller_cache_root().join("events/legacy-route")).unwrap();
        fs::set_permissions(
            paths.controller_cache_root(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let notice = paths
            .controller_cache_root()
            .join("events/legacy-route/notify.json");
        let lock = notice.with_file_name("notify.lock");
        let envelope = paths.controller_cache_root().join("envelope.json");
        for file in [&notice, &lock, &envelope] {
            fs::write(file, b"retain").unwrap();
        }
        verify_or_create(&paths, &pin).unwrap();
        repin(&paths, &pin, pin.controller_client_id).unwrap();
        for file in [&notice, &lock, &envelope] {
            assert_eq!(fs::read(file).unwrap(), b"retain");
        }
    }

    #[test]
    fn invalid_identity_and_unsafe_container_do_not_create_or_repair() {
        let (_temp, paths, mut pin) = fixture();
        pin.route_sha256 = "../escape".into();
        assert!(verify_or_create(&paths, &pin).is_err());
        assert!(!paths.controller_cache_root().exists());
        pin.route_sha256 = "a".repeat(64);
        fs::create_dir_all(paths.controller_cache_root()).unwrap();
        fs::set_permissions(
            paths.controller_cache_root(),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert!(verify_or_create(&paths, &pin).is_err());
        assert!(!paths.controller_cache_root().join("channel").exists());
        assert_eq!(
            fs::metadata(paths.controller_cache_root())
                .unwrap()
                .permissions()
                .mode()
                & 0o7777,
            0o755
        );
    }
}
