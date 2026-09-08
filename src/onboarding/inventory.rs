//! Preserve hand-written inventories while registering a worker atomically.
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
};

use super::{InitRequest, worker_name};
use crate::{
    config::{Config, WorkerEntry},
    error::WorkerError,
};

const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

fn read(path: &Path) -> Result<Option<String>, WorkerError> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(WorkerError::Config(format!(
                "cannot open inventory {}: {error}",
                path.display()
            )));
        }
    };
    if !file.metadata()?.is_file() {
        return Err(WorkerError::Config(
            "inventory must be a regular file".into(),
        ));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(WorkerError::Config("inventory exceeds 1 MiB".into()));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| WorkerError::Config("inventory must be UTF-8".into()))
}

fn select(text: Option<&str>, request: &InitRequest) -> Result<(WorkerEntry, bool), WorkerError> {
    let config = match text {
        Some(text) => {
            let config = Config::parse(text).map_err(|_| invalid_inventory())?;
            config.validate().map_err(|_| invalid_inventory())?;
            Some(config)
        }
        None => None,
    };
    if let Some(existing) = config
        .as_ref()
        .and_then(|c| c.workers.iter().find(|w| w.ssh == request.destination))
    {
        if request
            .name
            .as_ref()
            .is_some_and(|name| name != &existing.name)
        {
            return Err(WorkerError::Config(format!(
                "this SSH destination is already registered as {}; reuse that name",
                existing.name
            )));
        }
        return Ok((existing.clone(), false));
    }
    let hostname = request
        .destination
        .rsplit('@')
        .next()
        .unwrap_or(&request.destination);
    let name = request.name.clone().unwrap_or_else(|| {
        hostname
            .strip_suffix(".local")
            .unwrap_or(hostname)
            .to_owned()
    });
    worker_name(&name).map_err(WorkerError::Config)?;
    if config.as_ref().is_some_and(|c| c.worker(&name).is_some()) {
        return Err(WorkerError::Config(format!(
            "worker name {name} is already used; choose another with --name"
        )));
    }
    Ok((
        WorkerEntry {
            name,
            ssh: request.destination.clone(),
            slots: 1,
            capabilities: vec!["darwin-arm64".into()],
            remote_binary: "~/.local/bin/worker".into(),
            herdr: false,
        },
        true,
    ))
}

pub(super) fn preview(path: &Path, request: &InitRequest) -> Result<WorkerEntry, WorkerError> {
    select(read(path)?.as_deref(), request).map(|(worker, _)| worker)
}

pub(super) fn register(path: &Path, request: &InitRequest) -> Result<WorkerEntry, WorkerError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let filename = path
        .file_name()
        .ok_or_else(|| WorkerError::Config("inventory path needs a filename".into()))?;
    let mut lockname = filename.to_os_string();
    lockname.push(".lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(parent.join(lockname))?;
    if !lock.metadata()?.is_file() {
        return Err(WorkerError::Config(
            "inventory lock must be a regular file".into(),
        ));
    }
    // The persistent lock inode is shared by cooperating init processes. Closing releases it.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(WorkerError::Config(
            "another init is updating the inventory; retry shortly".into(),
        ));
    }
    let original = read(path)?;
    let (worker, added) = select(original.as_deref(), request)?;
    if !added {
        return Ok(worker);
    }
    let mut contents = original.clone().unwrap_or_else(|| "version = 1\n".into());
    contents.push_str(&format!("\n[[workers]]\nname = \"{}\"\nssh = \"{}\"\nslots = 1\ncapabilities = [\"darwin-arm64\"]\n", worker.name, worker.ssh));
    // Validate before replacing: unusual TOML forms must never corrupt an existing file.
    Config::parse(&contents)
        .map_err(|_| invalid_inventory())?
        .validate()
        .map_err(|_| invalid_inventory())?;
    let temporary = parent.join(format!(
        ".mac-worker-config-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    let result = (|| -> Result<(), WorkerError> {
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        if read(path)? != original {
            return Err(WorkerError::Config(
                "inventory changed during init; retry to preserve those edits".into(),
            ));
        }
        fs::rename(&temporary, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    Ok(worker)
}

fn invalid_inventory() -> WorkerError {
    WorkerError::Config("inventory is not valid mac-worker TOML; check version, worker names, SSH destinations and slots (see config.example.toml)".into())
}
