//! Durable map from frozen logical project/worktree IDs to a controller-owned
//! checkout. The mapping is stored beside controller requests, never inside
//! `ClientStateStore` and never as a caller-supplied physical path.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    controller::{
        leader::{open_controller_root, open_existing_controller_root, store_io},
        protocol::validate_request_id,
    },
    error::WorkerError,
    paths::PathLayout,
    process::ProcessRunner,
    project_state::ProjectState,
    rooted_fs::RootedDir,
    task::{TaskId, TaskMeta},
};

const REGISTRY_PREFIX: &str = "map-";
const TASK_BIND_PREFIX: &str = "task-req-";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryRecord {
    project_id: String,
    worktree_id: String,
    checkout: String,
}

pub struct ProjectRegistry {
    root: RootedDir,
}

/// Trusted `(project_id, worktree_id) → owned checkout` for ENV batch execute.
/// FLOW materializes and registers checkouts first. Wire `project_path` is
/// provenance only; a missing mapping fails closed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnedCheckoutMap {
    inner: BTreeMap<(String, String), PathBuf>,
}

impl OwnedCheckoutMap {
    pub fn from_identities(
        registry: &ProjectRegistry,
        identities: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
    ) -> Result<Self, WorkerError> {
        let mut map = Self::default();
        for (project_id, worktree_id) in identities {
            let project_id = project_id.as_ref();
            let worktree_id = worktree_id.as_ref();
            let Some(checkout) = registry.try_resolve(project_id, worktree_id)? else {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "no controller checkout is registered for the frozen project",
                ));
            };
            map.inner
                .insert((project_id.to_owned(), worktree_id.to_owned()), checkout);
        }
        Ok(map)
    }

    pub fn checkout(&self, project_id: &str, worktree_id: &str) -> Result<&Path, WorkerError> {
        self.inner
            .get(&(project_id.to_owned(), worktree_id.to_owned()))
            .map(PathBuf::as_path)
            .ok_or_else(|| {
                WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "no controller checkout is registered for the frozen project",
                )
            })
    }
}

impl ProjectRegistry {
    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        Ok(Self {
            root: open_controller_root(state_root)?,
        })
    }

    /// Read an existing registry. Does not create laptop controller directories.
    pub fn open_existing(state_root: &Path) -> Result<Option<Self>, WorkerError> {
        match open_existing_controller_root(state_root)? {
            Some(root) => Ok(Some(Self { root })),
            None => Ok(None),
        }
    }

    pub fn try_resolve(
        &self,
        project_id: &str,
        worktree_id: &str,
    ) -> Result<Option<PathBuf>, WorkerError> {
        let name = map_file_name(project_id, worktree_id)?;
        if !self.root.entry_exists(&name).map_err(store_io)? {
            return Ok(None);
        }
        Ok(Some(self.resolve(project_id, worktree_id)?))
    }

    pub fn resolve(&self, project_id: &str, worktree_id: &str) -> Result<PathBuf, WorkerError> {
        let name = map_file_name(project_id, worktree_id)?;
        if !self.root.entry_exists(&name).map_err(store_io)? {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "no controller checkout is registered for the frozen project",
            ));
        }
        let bytes = self
            .root
            .read_private_regular(&name, 16 * 1024)
            .map_err(store_io)?;
        let record: RegistryRecord = serde_json::from_slice(&bytes).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: project registry row is invalid".into())
        })?;
        if record.project_id != project_id || record.worktree_id != worktree_id {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: project registry row does not match its filename".into(),
            ));
        }
        let checkout = PathBuf::from(&record.checkout);
        if !checkout.is_absolute() || !checkout.is_dir() {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "registered controller checkout is missing",
            ));
        }
        Ok(checkout)
    }

    pub fn register(
        &self,
        project_id: &str,
        worktree_id: &str,
        checkout: &Path,
    ) -> Result<PathBuf, WorkerError> {
        if !checkout.is_absolute() {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "controller checkout must be an absolute path",
            ));
        }
        let name = map_file_name(project_id, worktree_id)?;
        let record = RegistryRecord {
            project_id: project_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            checkout: checkout.to_string_lossy().into_owned(),
        };
        let bytes = serde_json::to_vec(&record).map_err(|_| {
            WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: project registry row could not be encoded".into(),
            )
        })?;
        if self.root.entry_exists(&name).map_err(store_io)? {
            let existing = self.resolve(project_id, worktree_id)?;
            if existing == checkout {
                return Ok(existing);
            }
            return Err(WorkerError::task(
                "TASK_ID_CONFLICT",
                "frozen project is already registered at a different checkout",
            ));
        }
        self.root
            .write_private_atomic_no_replace(&name, &bytes)
            .map_err(store_io)?;
        Ok(checkout.to_path_buf())
    }

    /// FLOW-owned task_id → original request/fingerprint map for result
    /// prepare. This is not a second task store.
    pub fn bind_task_request(
        &self,
        task_id: TaskId,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<(), WorkerError> {
        validate_request_id(request_id)?;
        if fingerprint.len() != 64
            || !fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: task bind fingerprint is invalid".into(),
            ));
        }
        let name = task_bind_name(task_id)?;
        let record = TaskBindRecord {
            task_id: task_id.to_string(),
            request_id: request_id.to_owned(),
            fingerprint: fingerprint.to_owned(),
        };
        let bytes = serde_json::to_vec(&record).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: task bind row could not be encoded".into())
        })?;
        if self.root.entry_exists(&name).map_err(store_io)? {
            let existing = self.lookup_task_request(task_id)?.ok_or_else(|| {
                WorkerError::Protocol("CONTROLLER_TRANSPORT: task bind row is missing".into())
            })?;
            if existing.request_id == request_id && existing.fingerprint == fingerprint {
                return Ok(());
            }
            return Err(WorkerError::Protocol(
                "CONTROLLER_REQUEST_CONFLICT: task is already bound to a different request".into(),
            ));
        }
        self.root
            .write_private_atomic_no_replace(&name, &bytes)
            .map_err(store_io)?;
        Ok(())
    }

    pub fn lookup_task_request(
        &self,
        task_id: TaskId,
    ) -> Result<Option<TaskRequestBind>, WorkerError> {
        let name = task_bind_name(task_id)?;
        if !self.root.entry_exists(&name).map_err(store_io)? {
            return Ok(None);
        }
        let bytes = self
            .root
            .read_private_regular(&name, 16 * 1024)
            .map_err(store_io)?;
        let record: TaskBindRecord = serde_json::from_slice(&bytes).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: task bind row is invalid".into())
        })?;
        if record.task_id != task_id.to_string() {
            return Err(WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: task bind row does not match its filename".into(),
            ));
        }
        Ok(Some(TaskRequestBind {
            request_id: record.request_id,
            fingerprint: record.fingerprint,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskBindRecord {
    task_id: String,
    request_id: String,
    fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRequestBind {
    pub request_id: String,
    pub fingerprint: String,
}

/// Remap logical IDs only after a validated registry hit whose checkout
/// actually contains `project`. Ordinary callers keep `load_for_task`.
pub fn load_registered_or_local(
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    project: &Path,
    cli_includes: &[String],
    meta: &TaskMeta,
) -> Result<ProjectState, WorkerError> {
    if let Some(registry) = ProjectRegistry::open_existing(&paths.controller_state_root())?
        && let Some(checkout) = registry.try_resolve(meta.project_id(), meta.worktree_id())?
        && is_registered_checkout(project, &checkout)
    {
        return ProjectState::load_for_registered_checkout(runner, project, cli_includes, meta);
    }
    ProjectState::load_for_task(runner, project, cli_includes, meta)
}

/// Validated controller-DAG mapping: logical laptop worktree IDs to a
/// trusted registered checkout. Ordinary local DAG callers see `None`.
pub fn mapped_controller_checkout(
    paths: &PathLayout,
    project_id: &str,
    worktree_id: &str,
) -> Result<Option<PathBuf>, WorkerError> {
    let Some(registry) = ProjectRegistry::open_existing(&paths.controller_state_root())? else {
        return Ok(None);
    };
    registry.try_resolve(project_id, worktree_id)
}

/// Registered controller contexts open `controller-transfer/<logical>.git`.
/// Ordinary unregistered local still uses `transfer/<physicalhash>`.
pub fn open_transfer_repo(
    paths: &PathLayout,
    project: &ProjectState,
    meta: &TaskMeta,
) -> Result<crate::transfer_repo::TransferRepo, WorkerError> {
    if let Some(registry) = ProjectRegistry::open_existing(&paths.controller_state_root())?
        && registry
            .try_resolve(meta.project_id(), meta.worktree_id())?
            .is_some()
    {
        return crate::transfer_repo::TransferRepo::open_or_create_controller_cache(
            &paths.cache,
            meta.project_id(),
            meta.worktree_id(),
        );
    }
    crate::transfer_repo::TransferRepo::open_or_create(&paths.cache, &project.context.common_dir)
}

fn is_registered_checkout(project: &Path, checkout: &Path) -> bool {
    let Ok(project) = fs::canonicalize(project) else {
        return false;
    };
    let Ok(checkout) = fs::canonicalize(checkout) else {
        return false;
    };
    project == checkout || project.starts_with(&checkout)
}

pub fn checkout_path(
    project_root: &Path,
    project_id: &str,
    worktree_id: &str,
) -> Result<PathBuf, WorkerError> {
    validate_id(project_id, "project ID")?;
    validate_id(worktree_id, "worktree ID")?;
    Ok(project_root.join(project_id).join(worktree_id))
}

pub fn ensure_checkout_dir(path: &Path) -> Result<(), WorkerError> {
    fs::create_dir_all(path).map_err(WorkerError::Io)?;
    Ok(())
}

fn map_file_name(project_id: &str, worktree_id: &str) -> Result<String, WorkerError> {
    validate_id(project_id, "project ID")?;
    validate_id(worktree_id, "worktree ID")?;
    Ok(format!("{REGISTRY_PREFIX}{project_id}-{worktree_id}.json"))
}

fn task_bind_name(task_id: TaskId) -> Result<String, WorkerError> {
    Ok(format!("{TASK_BIND_PREFIX}{task_id}.json"))
}

fn validate_id(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(WorkerError::task(
            "TASK_CONFIG_INVALID",
            format!("frozen {label} is not a 64-character lowercase hex digest"),
        ));
    }
    Ok(())
}
