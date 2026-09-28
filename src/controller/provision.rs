//! Controller provisioning primitives. All SSH and key generation use injectable runners.
use crate::{config::Config, error::WorkerError, rooted_fs::RootedDir};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, io, os::fd::AsRawFd, path::Path};

const MAX_FILE: u64 = 1024 * 1024;
const KEY_COMMENT: &str = "mac-worker-controller";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSsh {
    pub user: String,
    pub hostname: String,
    pub port: u16,
    pub proxy_jump: Option<String>,
    pub known_hosts_files: Vec<String>,
    pub host_key_alias: Option<String>,
}
impl ResolvedSsh {
    pub fn parse(output: &str) -> Result<Self, WorkerError> {
        let values: BTreeMap<_, _> = output
            .lines()
            .filter_map(|line| line.split_once(' '))
            .collect();
        let get = |key| values.get(key).copied().unwrap_or("");
        let target = Self {
            user: get("user").into(),
            hostname: get("hostname").into(),
            port: get("port")
                .parse()
                .map_err(|_| invalid("invalid SSH port"))?,
            proxy_jump: match get("proxyjump") {
                "" | "none" => None,
                value => Some(value.into()),
            },
            known_hosts_files: get("userknownhostsfile")
                .split_whitespace()
                .chain(get("globalknownhostsfile").split_whitespace())
                .map(str::to_owned)
                .collect(),
            host_key_alias: match get("hostkeyalias") {
                "" | "none" => None,
                value => Some(value.into()),
            },
        };
        target.validate()?;
        if !matches!(get("proxycommand"), "" | "none") && target.proxy_jump.is_none() {
            return Err(invalid(
                "ProxyCommand needs an explicit --worker-ssh override",
            ));
        }
        Ok(target)
    }
    pub fn validate(&self) -> Result<(), WorkerError> {
        if !token(&self.user)
            || !host_token(&self.hostname)
            || self.port == 0
            || self.proxy_jump.as_ref().is_some_and(|v| {
                !v.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"._-@,:[]".contains(&c))
            })
            || self.host_key_alias.as_ref().is_some_and(|v| !host_token(v))
        {
            return Err(invalid("invalid resolved SSH destination"));
        }
        Ok(())
    }
    pub fn same_host(&self, other: &Self) -> bool {
        self.hostname.eq_ignore_ascii_case(&other.hostname) && self.port == other.port
    }
}
fn token(s: &str) -> bool {
    !s.is_empty()
        && s.as_bytes()[0].is_ascii_alphanumeric()
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
}
fn host_token(s: &str) -> bool {
    !s.is_empty()
        && s.as_bytes()[0] != b'-'
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-:[]".contains(&c))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedWorker {
    pub name: String,
    pub alias: String,
    pub target: ResolvedSsh,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_keys: Option<String>,
}

pub fn plan_inventory(
    config: &Config,
    controller: &ResolvedSsh,
    controller_alias: &str,
    resolved: &BTreeMap<String, ResolvedSsh>,
) -> Result<Vec<PlannedWorker>, WorkerError> {
    config.require_local_inventory()?;
    config
        .workers
        .iter()
        .map(|worker| {
            let mut target = resolved
                .get(&worker.name)
                .ok_or_else(|| invalid("missing resolved worker"))?
                .clone();
            target.validate()?;
            // Preserve the original verification/certificate principal for loopback.
            target.host_key_alias = Some(
                target
                    .host_key_alias
                    .clone()
                    .unwrap_or_else(|| target.hostname.clone()),
            );
            if target.same_host(controller) {
                target.hostname = "127.0.0.1".into();
                target.proxy_jump = None;
            } else if let Some(jumps) = target.proxy_jump.clone() {
                let (first, rest) = jumps.split_once(',').unwrap_or((&jumps, ""));
                // Only the exact controller alias was already resolved. A
                // literal hostname or another alias can select a different
                // port; init resolves those before deciding to remove a hop.
                if first == controller_alias {
                    target.proxy_jump = if rest.is_empty() {
                        None
                    } else {
                        Some(rest.into())
                    };
                }
            }
            Ok(PlannedWorker {
                name: worker.name.clone(),
                alias: format!("mac-worker-controller-{}", worker.name),
                target,
                trusted_keys: None,
            })
        })
        .collect()
}

pub(crate) fn directory(path: &Path) -> Result<RootedDir, WorkerError> {
    let dir = match RootedDir::open(path) {
        Ok(dir) => Ok::<RootedDir, WorkerError>(dir),
        Err(e) if e.kind() == io::ErrorKind::NotFound => match RootedDir::create(path) {
            Ok(dir) => Ok::<RootedDir, WorkerError>(dir),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(RootedDir::open(path)?),
            Err(e) => Err(e.into()),
        },
        Err(e) => Err(e.into()),
    }?;
    let metadata = dir.root_metadata()?;
    if metadata.st_uid != unsafe { libc::geteuid() } {
        return Err(invalid(
            "provisioning directory must belong to this account",
        ));
    }
    if metadata.st_mode & 0o777 != 0o700
        && unsafe { libc::fchmod(dir.raw_directory_fd(), 0o700) } != 0
    {
        return Err(io::Error::last_os_error().into());
    }
    dir.verify_bound()?;
    Ok(dir)
}
pub(crate) fn read_optional(dir: &RootedDir, name: &str) -> Result<Option<Vec<u8>>, WorkerError> {
    if !dir.entry_exists(name)? {
        return Ok(None);
    }
    // Existing user-managed SSH/config files can be 0644. Repair by retained fd,
    // rejecting symlinks/hardlinks/foreign owners before reading any bytes.
    dir.set_private_regular_mode(name, 0o600)?;
    Ok(Some(dir.read_private_regular(name, MAX_FILE)?))
}
pub(crate) fn replace(
    dir: &RootedDir,
    name: &str,
    old: Option<&[u8]>,
    new: &[u8],
) -> Result<bool, WorkerError> {
    if old == Some(new) {
        return Ok(false);
    }
    match old {
        Some(old) => dir.replace_private_regular_exact(name, old, new)?,
        None => dir.write_private_atomic_no_replace(name, new)?,
    };
    Ok(true)
}
pub(crate) fn exclusive_lock(dir: &RootedDir, name: &str) -> Result<fs::File, WorkerError> {
    let file = dir.open_private_lock(name)?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(file)
}
fn public_key(line: &str) -> Result<String, WorkerError> {
    if line.len() > 16384 || line.contains(['\r', '\n']) {
        return Err(invalid("invalid controller public key"));
    }
    let fields: Vec<_> = line.split_whitespace().collect();
    if fields.len() < 2 || fields.len() > 3 || fields[0] != "ssh-ed25519" {
        return Err(invalid(
            "controller key must be an Ed25519 public key without options",
        ));
    }
    validate_key_blob(fields[0], fields[1])?;
    Ok(format!("{} {}", fields[0], fields[1]))
}
fn validate_key_blob(kind: &str, encoded: &str) -> Result<(), WorkerError> {
    let blob = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| invalid("invalid SSH public key encoding"))?;
    if blob.len() < 4 {
        return Err(invalid("invalid SSH public key"));
    }
    let size = u32::from_be_bytes(blob[..4].try_into().unwrap()) as usize;
    if size > blob.len() - 4 || &blob[4..4 + size] != kind.as_bytes() {
        return Err(invalid("SSH key type mismatch"));
    }
    if kind == "ssh-ed25519" && (blob.len() != 51 || blob[15..19] != 32u32.to_be_bytes()) {
        return Err(invalid("invalid Ed25519 key length"));
    }
    Ok(())
}
pub fn authorize_controller_key(home: &Path, key: &str) -> Result<bool, WorkerError> {
    let key = public_key(key)?;
    let dir = directory(&home.join(".ssh"))?;
    let _lock = exclusive_lock(&dir, "mac-worker-controller-authorize.lock")?;
    let old = read_optional(&dir, "authorized_keys")?;
    let mut bytes = old.clone().unwrap_or_default();
    let line = format!("{key} {KEY_COMMENT}");
    if bytes
        .split(|b| *b == b'\n')
        .any(|existing| existing == line.as_bytes())
    {
        return Ok(false);
    }
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bytes.push(b'\n')
    }
    bytes.extend(line.as_bytes());
    bytes.push(b'\n');
    replace(&dir, "authorized_keys", old.as_deref(), &bytes)
}
pub fn trusted_host_keys(found: &str, host: &str, port: u16) -> Result<String, WorkerError> {
    if !host_token(host) || port == 0 {
        return Err(invalid("invalid known-host target"));
    }
    let target = if port == 22 {
        host.to_owned()
    } else {
        format!("[{host}]:{port}")
    };
    let mut result = String::new();
    for line in found
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
    {
        let parts: Vec<_> = line.split_whitespace().collect();
        let (marker, offset) = if parts.first() == Some(&"@cert-authority") {
            ("@cert-authority ", 1)
        } else {
            ("", 0)
        };
        if parts.len() < offset + 3 || parts[0] == "@revoked" {
            return Err(invalid("trusted host key is revoked or invalid"));
        }
        let kind = parts[offset + 1];
        let key = parts[offset + 2];
        if !(kind == "ssh-ed25519" || kind == "ssh-rsa" || kind.starts_with("ecdsa-sha2-")) {
            return Err(invalid("unsupported trusted host key"));
        }
        validate_key_blob(kind, key)?;
        let entry = format!("{marker}{target} {kind} {key}\n");
        if !result.lines().any(|line| line == entry.trim_end()) {
            result.push_str(&entry)
        }
    }
    if result.is_empty() {
        return Err(invalid(
            "no laptop-trusted host key; verify this worker from the laptop first",
        ));
    }
    Ok(result)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigWriteResult {
    pub changed: bool,
    pub conflict: bool,
    pub diff: Option<String>,
}
pub fn write_controller_config(
    path: &Path,
    contents: &str,
    force: bool,
) -> Result<ConfigWriteResult, WorkerError> {
    let config = Config::parse(contents)?;
    config.validate()?;
    if config.controller.enabled {
        return Err(invalid("controller host inventory must run locally"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("config parent missing"))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("invalid config path"))?;
    let dir = directory(parent)?;
    let _lock = exclusive_lock(&dir, "controller-config.lock")?;
    let old = read_optional(&dir, name)?;
    if old.as_deref().is_some_and(|old| old != contents.as_bytes()) && !force {
        let previous =
            String::from_utf8(old.unwrap()).map_err(|_| invalid("existing config is not UTF-8"))?;
        // Only valid platform config is printed, never arbitrary file contents.
        Config::parse(&previous)?.validate()?;
        let diff = format!(
            "--- existing controller config\n+++ proposed controller config\n{}{}",
            previous
                .lines()
                .map(|s| format!("-{s}\n"))
                .collect::<String>(),
            contents
                .lines()
                .map(|s| format!("+{s}\n"))
                .collect::<String>()
        );
        return Ok(ConfigWriteResult {
            changed: false,
            conflict: true,
            diff: Some(diff),
        });
    }
    Ok(ConfigWriteResult {
        changed: replace(&dir, name, old.as_deref(), contents.as_bytes())?,
        conflict: false,
        diff: None,
    })
}
pub(crate) fn invalid(message: &str) -> WorkerError {
    WorkerError::Config(format!("CONTROLLER_INIT: {message}"))
}

pub(crate) const PROCESS_POLICY: crate::process::ProcessPolicy = crate::process::ProcessPolicy {
    stdout_limit: 1024 * 1024,
    stderr_limit: 64 * 1024,
    deadline: std::time::Duration::from_secs(30),
};
pub(crate) fn process(
    program: &str,
    args: Vec<std::ffi::OsString>,
) -> crate::process::ProcessRequest {
    crate::process::ProcessRequest {
        program: program.into(),
        args,
        environment: vec![("LC_ALL".into(), "C".into())],
        environment_remove: vec![],
        stdin: Some(Vec::new()),
        policy: PROCESS_POLICY,
        isolate_parent_environment: false,
    }
}

pub fn ensure_controller_key(
    home: &Path,
    runner: &dyn crate::process::ProcessRunner,
) -> Result<String, WorkerError> {
    let dir = directory(&home.join(".ssh"))?;
    let _lock = exclusive_lock(&dir, "mac-worker-controller-key.lock")?;
    let private = "mac-worker-controller_ed25519";
    let public = "mac-worker-controller_ed25519.pub";
    if dir.entry_exists(private)? {
        dir.set_private_regular_mode(private, 0o600)?;
        dir.validate_private_entry(private)?;
        if let Some(bytes) = read_optional(&dir, public)? {
            return public_key(
                std::str::from_utf8(&bytes)
                    .map_err(|_| invalid("invalid controller public key"))?
                    .trim_end(),
            );
        }
        // Recover a crash between private/public publication without replacing the key.
        let output = runner.run(&process(
            "/usr/bin/ssh-keygen",
            vec![
                "-y".into(),
                "-f".into(),
                home.join(".ssh").join(private).into_os_string(),
            ],
        ))?;
        if !output.status.success() {
            return Err(invalid("cannot recover controller public key"));
        }
        let key = public_key(
            std::str::from_utf8(&output.stdout)
                .map_err(|_| invalid("invalid controller public key"))?
                .trim_end(),
        )?;
        dir.write_private_atomic_no_replace(public, format!("{key} {KEY_COMMENT}\n").as_bytes())?;
        return Ok(key);
    }
    if dir.entry_exists(public)? {
        return Err(invalid(
            "controller public key exists without private key; restore the matching private key",
        ));
    }
    let temporary = home.join(".ssh").join(format!(
        ".mac-worker-controller-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let staging = directory(&temporary)?;
    let output = runner.run(&process(
        "/usr/bin/ssh-keygen",
        vec![
            "-q".into(),
            "-t".into(),
            "ed25519".into(),
            "-N".into(),
            "".into(),
            "-C".into(),
            KEY_COMMENT.into(),
            "-f".into(),
            temporary.join("key").into_os_string(),
        ],
    ))?;
    if !output.status.success() {
        return Err(invalid("controller key generation failed"));
    }
    let public_bytes = read_optional(&staging, "key.pub")?
        .ok_or_else(|| invalid("key generation produced no public key"))?;
    let key = public_key(
        std::str::from_utf8(&public_bytes)
            .map_err(|_| invalid("invalid controller public key"))?
            .trim_end(),
    )?;
    let private_bytes = staging.read_private_regular("key", 64 * 1024)?;
    dir.write_private_atomic_no_replace(private, &private_bytes)?;
    dir.write_private_atomic_no_replace(public, format!("{key} {KEY_COMMENT}\n").as_bytes())?;
    staging.remove_owned_regular("key")?;
    staging.remove_owned_regular("key.pub")?;
    // Only this invocation's empty staging directory, never an existing key.
    staging.remove_owned_tree()?;
    Ok(key)
}

pub fn write_ssh_settings(
    home: &Path,
    workers: &[PlannedWorker],
    known_hosts: &str,
) -> Result<(), WorkerError> {
    if workers.is_empty() || workers.len() > 256 || known_hosts.len() > MAX_FILE as usize {
        return Err(invalid("invalid controller SSH inventory"));
    }
    let mut generated = String::from("# Managed by worker controller init\n");
    for worker in workers {
        worker.target.validate()?;
        if !crate::config::valid_identifier(&worker.name)
            || worker.alias != format!("mac-worker-controller-{}", worker.name)
        {
            return Err(invalid("invalid managed SSH alias"));
        }
        let t = &worker.target;
        let expected = t
            .host_key_alias
            .clone()
            .unwrap_or_else(|| t.hostname.clone());
        let matches = worker
            .trusted_keys
            .as_deref()
            .unwrap_or(known_hosts)
            .lines()
            .filter(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                fields.first().copied() == Some(expected.as_str())
                    || (fields.first() == Some(&"@cert-authority")
                        && fields.get(1).copied() == Some(expected.as_str()))
            })
            .collect::<Vec<_>>()
            .join("\n");
        trusted_host_keys(&matches, &expected, 22)?;
        generated.push_str(&format!("Host {}\n  HostName {}\n  User {}\n  Port {}\n  ProxyJump {}\n  IdentityFile ~/.ssh/mac-worker-controller_ed25519\n  IdentitiesOnly yes\n  IdentityAgent none\n  BatchMode yes\n  StrictHostKeyChecking yes\n  NoHostAuthenticationForLocalhost no\n  HostKeyAlias {}\n  UserKnownHostsFile ~/.ssh/{}.known_hosts\n  GlobalKnownHostsFile /dev/null\n\n",worker.alias,t.hostname,t.user,t.port,t.proxy_jump.as_deref().unwrap_or("none"),expected,worker.alias));
    }
    generated.push_str("Host *\n");
    let dir = directory(&home.join(".ssh"))?;
    let _lock = exclusive_lock(&dir, "mac-worker-controller-ssh.lock")?;
    for (name, content) in [
        ("mac-worker-controller_known_hosts", known_hosts),
        ("mac-worker-controller.conf", &generated),
    ] {
        let old = read_optional(&dir, name)?;
        replace(&dir, name, old.as_deref(), content.as_bytes())?;
    }
    for worker in workers {
        let name = format!("{}.known_hosts", worker.alias);
        let identity = worker
            .target
            .host_key_alias
            .as_deref()
            .unwrap_or(&worker.target.hostname);
        let matching = worker
            .trusted_keys
            .as_deref()
            .unwrap_or(known_hosts)
            .lines()
            .filter(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                fields.first().copied() == Some(identity)
                    || (fields.first() == Some(&"@cert-authority")
                        && fields.get(1).copied() == Some(identity))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let content = trusted_host_keys(&matching, identity, 22)?;
        let old = read_optional(&dir, &name)?;
        replace(&dir, &name, old.as_deref(), content.as_bytes())?;
    }
    let old = read_optional(&dir, "config")?;
    let include = "Include ~/.ssh/mac-worker-controller.conf\n";
    let bytes = old.as_deref().unwrap_or_default();
    if !bytes.starts_with(include.as_bytes()) {
        let mut new = include.as_bytes().to_vec();
        new.extend(bytes);
        replace(&dir, "config", old.as_deref(), &new)?;
    }
    Ok(())
}
