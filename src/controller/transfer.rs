//! Streamed controller Git data plane.
//!
//! Hidden helpers resolve the Git cache from `PathLayout.cache` plus
//! validated project/worktree IDs. They never take an in-memory
//! `TransferRepo` or a client path.

use std::{
    convert::Infallible,
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    controller::{
        leader::{lock_exclusive, open_controller_root, store_io},
        protocol::{canonical_request_sha256, validate_request_id},
    },
    error::WorkerError,
    git_transport::GitServerExecutor,
    job::RequestFingerprint,
    process::ProcessRunner,
    protocol::PROTOCOL_VERSION,
    rooted_fs::RootedDir,
    task::{BaseOid, TaskId, TurnId},
    transfer_repo::{ImportReceipt, TransferRepo, apply_isolated_git_environment},
};

const TRANSFER_LOCK: &str = "xfer.lock";
const RECORD_VERSION: u32 = 1;
const SOURCE_KIND: &str = "source_receive";
const RESULT_KIND: &str = "result_upload";
const SOURCE_COMMAND: &str = "controller.transfer.source";
const RESULT_COMMAND: &str = "controller.transfer.result";
const RECORD_LIMIT: u64 = 16 * 1024;

pub const CONTROLLER_TRANSFER_CACHE_DOMAIN: &[u8] = TransferRepo::CONTROLLER_TRANSFER_CACHE_DOMAIN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerReceiveIdentity {
    token: String,
    request_id: String,
    fingerprint: RequestFingerprint,
    project_id: String,
    worktree_id: String,
    expected_oid: BaseOid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSourceReceipt {
    request_id: String,
    oid: BaseOid,
    request_ref: String,
    token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedResultMeta {
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub imported_oid: BaseOid,
    pub worker: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerResultIdentity {
    token: String,
    request_id: String,
    fingerprint: RequestFingerprint,
    project_id: String,
    worktree_id: String,
    task_id: TaskId,
    turn_id: TurnId,
    imported_oid: BaseOid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRecord {
    version: u32,
    kind: String,
    token: String,
    request_id: String,
    fingerprint: String,
    source_digest: String,
    project_id: String,
    worktree_id: String,
    expected_oid: String,
    cache_id: String,
    receipt_oid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultRecord {
    version: u32,
    kind: String,
    token: String,
    request_id: String,
    fingerprint: String,
    result_digest: String,
    project_id: String,
    worktree_id: String,
    task_id: String,
    turn_id: String,
    imported_oid: String,
    cache_id: String,
}

pub struct ControllerTransfer {
    root: RootedDir,
}

impl ControllerReceiveIdentity {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn fingerprint(&self) -> &RequestFingerprint {
        &self.fingerprint
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub fn expected_oid(&self) -> &BaseOid {
        &self.expected_oid
    }

    pub fn from_parts(
        token: String,
        request_id: String,
        fingerprint: RequestFingerprint,
        project_id: String,
        worktree_id: String,
        expected_oid: BaseOid,
    ) -> Self {
        Self {
            token,
            request_id,
            fingerprint,
            project_id,
            worktree_id,
            expected_oid,
        }
    }
}

impl ControllerSourceReceipt {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn oid(&self) -> &BaseOid {
        &self.oid
    }
    pub fn request_ref(&self) -> &str {
        &self.request_ref
    }
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl ControllerResultIdentity {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn fingerprint(&self) -> &RequestFingerprint {
        &self.fingerprint
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
    pub fn turn_id(&self) -> TurnId {
        self.turn_id
    }
    pub fn imported_oid(&self) -> &BaseOid {
        &self.imported_oid
    }
}

pub fn controller_transfer_cache_id(
    project_id: &str,
    worktree_id: &str,
) -> Result<String, WorkerError> {
    TransferRepo::controller_transfer_cache_id(project_id, worktree_id)
}

pub fn controller_transfer_git_path(
    cache_root: &Path,
    project_id: &str,
    worktree_id: &str,
) -> Result<PathBuf, WorkerError> {
    TransferRepo::controller_transfer_git_path(cache_root, project_id, worktree_id)
}

pub fn frozen_result_ref(request_id: &str, turn_id: TurnId) -> Result<String, WorkerError> {
    TransferRepo::frozen_controller_result_ref(request_id, &turn_id.to_string())
}

pub fn source_digest(
    project_id: &str,
    worktree_id: &str,
    expected_oid: &BaseOid,
) -> Result<String, WorkerError> {
    let body = json!({
        "expected_oid": expected_oid.as_str(),
        "project_id": project_id,
        "worktree_id": worktree_id,
    });
    canonical_request_sha256(PROTOCOL_VERSION, SOURCE_COMMAND, &body)
}

pub fn result_digest(
    task_id: TaskId,
    turn_id: TurnId,
    imported_oid: &BaseOid,
) -> Result<String, WorkerError> {
    let body = json!({
        "imported_oid": imported_oid.as_str(),
        "task_id": task_id.to_string(),
        "turn_id": turn_id.to_string(),
    });
    canonical_request_sha256(PROTOCOL_VERSION, RESULT_COMMAND, &body)
}

impl ControllerTransfer {
    pub fn open(state_root: &Path) -> Result<Self, WorkerError> {
        Ok(Self {
            root: open_controller_root(state_root)?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_source_receive(
        &self,
        cache_root: &Path,
        _runner: &dyn ProcessRunner,
        request_id: &str,
        fingerprint: &RequestFingerprint,
        project_id: &str,
        worktree_id: &str,
        expected_oid: &BaseOid,
    ) -> Result<ControllerReceiveIdentity, WorkerError> {
        validate_request_id(request_id)?;
        validate_sha256_id(project_id, "project ID")?;
        validate_sha256_id(worktree_id, "worktree ID")?;
        let digest = source_digest(project_id, worktree_id, expected_oid)?;
        let cache_id = controller_transfer_cache_id(project_id, worktree_id)?;
        let _lock = self.lock_transfers()?;
        TransferRepo::open_or_create_controller_cache(cache_root, project_id, worktree_id)?;
        if let Some(existing) = self.load_source_by_request(request_id)? {
            reuse_source(
                &existing,
                fingerprint,
                project_id,
                worktree_id,
                expected_oid,
                &digest,
                &cache_id,
            )?;
            self.repair_source_lookup(&existing)?;
            return identity_from_source(&existing);
        }
        let token = mint_token();
        let record = SourceRecord {
            version: RECORD_VERSION,
            kind: SOURCE_KIND.into(),
            token: token.clone(),
            request_id: request_id.to_owned(),
            fingerprint: fingerprint.as_str().to_owned(),
            source_digest: digest,
            project_id: project_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            expected_oid: expected_oid.as_str().to_owned(),
            cache_id,
            receipt_oid: None,
        };
        self.write_source(&record)?;
        identity_from_source(&record)
    }

    pub fn finish_source_receive(
        &self,
        cache_root: &Path,
        runner: &dyn ProcessRunner,
        identity: &ControllerReceiveIdentity,
    ) -> Result<ControllerSourceReceipt, WorkerError> {
        let _lock = self.lock_transfers()?;
        let mut record = self
            .load_source_by_request(&identity.request_id)?
            .ok_or_else(missing_token)?;
        self.require_source_match(&record, identity)?;
        self.repair_source_lookup(&record)?;
        let transfer = TransferRepo::open_controller_cache(
            cache_root,
            &record.project_id,
            &record.worktree_id,
        )?;
        if transfer.repo_id() != record.cache_id {
            return Err(conflict(
                "controller cache identity does not match the token record",
            ));
        }
        let oid: BaseOid = record
            .expected_oid
            .parse()
            .map_err(|_| invalid_component("expected OID"))?;
        if let Some(existing) = &record.receipt_oid {
            if existing != oid.as_str() {
                return Err(conflict("source receipt already names a different object"));
            }
            self.repair_source_lookup(&record)?;
            let request_ref = TransferRepo::frozen_request_ref(&record.request_id)?;
            return Ok(ControllerSourceReceipt {
                request_id: record.request_id,
                oid,
                request_ref,
                token: record.token,
            });
        }
        transfer.pin_frozen_source(runner, &record.request_id, &oid)?;
        let previous = encode(&record)?;
        record.receipt_oid = Some(oid.as_str().to_owned());
        let next = encode(&record)?;
        self.root
            .replace_private_regular_exact(
                &source_request_name(&record.request_id)?,
                &previous,
                &next,
            )
            .map_err(store_io)?;
        self.repair_source_lookup(&record)?;
        Ok(ControllerSourceReceipt {
            request_id: record.request_id,
            request_ref: TransferRepo::frozen_request_ref(identity.request_id())?,
            oid,
            token: record.token,
        })
    }

    /// Final submit binding: same outer fingerprint and nested source fields,
    /// then finish (idempotent) so a crash between push and the finish RPC
    /// still pins before ACK. Token is loaded from the durable record, never
    /// from the frozen submit envelope.
    pub fn bind_source_for_submit(
        &self,
        cache_root: &Path,
        runner: &dyn ProcessRunner,
        request_id: &str,
        fingerprint: &RequestFingerprint,
        project_id: &str,
        worktree_id: &str,
        expected_oid: &BaseOid,
    ) -> Result<ControllerSourceReceipt, WorkerError> {
        let identity = {
            let _lock = self.lock_transfers()?;
            let record = self
                .load_source_by_request(request_id)?
                .ok_or_else(missing_token)?;
            let digest = source_digest(project_id, worktree_id, expected_oid)?;
            let cache_id = controller_transfer_cache_id(project_id, worktree_id)?;
            reuse_source(
                &record,
                fingerprint,
                project_id,
                worktree_id,
                expected_oid,
                &digest,
                &cache_id,
            )?;
            identity_from_source(&record)?
        };
        self.finish_source_receive(cache_root, runner, &identity)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_result_upload(
        &self,
        cache_root: &Path,
        runner: &dyn ProcessRunner,
        request_id: &str,
        fingerprint: &RequestFingerprint,
        project_id: &str,
        worktree_id: &str,
        meta: &VerifiedResultMeta,
    ) -> Result<ControllerResultIdentity, WorkerError> {
        validate_request_id(request_id)?;
        validate_sha256_id(project_id, "project ID")?;
        validate_sha256_id(worktree_id, "worktree ID")?;
        let digest = result_digest(meta.task_id, meta.turn_id, &meta.imported_oid)?;
        let cache_id = controller_transfer_cache_id(project_id, worktree_id)?;
        let _lock = self.lock_transfers()?;
        let transfer =
            TransferRepo::open_or_create_controller_cache(cache_root, project_id, worktree_id)?;
        if let Some(existing) = self.load_result_by_request_turn(request_id, meta.turn_id)? {
            reuse_result(
                &existing,
                fingerprint,
                project_id,
                worktree_id,
                meta,
                &digest,
                &cache_id,
            )?;
            self.repair_result_lookup(&existing)?;
            return identity_from_result(&existing);
        }
        transfer.pin_controller_result(
            runner,
            request_id,
            &meta.turn_id.to_string(),
            &meta.imported_oid,
        )?;
        let token = mint_token();
        let record = ResultRecord {
            version: RECORD_VERSION,
            kind: RESULT_KIND.into(),
            token: token.clone(),
            request_id: request_id.to_owned(),
            fingerprint: fingerprint.as_str().to_owned(),
            result_digest: digest,
            project_id: project_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            task_id: meta.task_id.to_string(),
            turn_id: meta.turn_id.to_string(),
            imported_oid: meta.imported_oid.as_str().to_owned(),
            cache_id,
        };
        self.write_result(&record)?;
        identity_from_result(&record)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn receive_pack(
        &self,
        cache_root: &Path,
        token: &str,
        request_id: &str,
        fingerprint: &str,
        project_id: &str,
        worktree_id: &str,
        oid: &str,
        path_arg: Option<&str>,
        executor: &dyn GitServerExecutor,
    ) -> Result<Infallible, WorkerError> {
        reject_path_arg(project_id, path_arg)?;
        let record = self
            .load_source_by_request(request_id)?
            .ok_or_else(missing_token)?;
        if record.version != RECORD_VERSION {
            return Err(invalid_component(
                "source transfer record version is unsupported",
            ));
        }
        if record.token != token
            || record.request_id != request_id
            || record.fingerprint != fingerprint
            || record.project_id != project_id
            || record.worktree_id != worktree_id
            || record.expected_oid != oid
        {
            return Err(mismatch());
        }
        self.repair_source_lookup(&record)?;
        let expected: BaseOid = oid.parse().map_err(|_| invalid_component("expected OID"))?;
        let digest = source_digest(project_id, worktree_id, &expected)?;
        if digest != record.source_digest {
            return Err(mismatch());
        }
        let transfer = TransferRepo::open_controller_cache(cache_root, project_id, worktree_id)?;
        if transfer.repo_id() != record.cache_id {
            return Err(mismatch());
        }
        let request_ref = TransferRepo::frozen_request_ref(request_id)?;
        let hook_dir = write_receive_hook(transfer.path(), token, &request_ref, oid)?;
        let mirror = RootedDir::open(transfer.path()).map_err(WorkerError::Io)?;
        mirror
            .verify_descriptors_cloexec()
            .map_err(WorkerError::Io)?;
        executor.exec(
            "git-receive-pack",
            &mirror,
            &[
                ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
                ("GIT_CONFIG_COUNT".into(), "1".into()),
                ("GIT_CONFIG_KEY_0".into(), "core.hooksPath".into()),
                ("GIT_CONFIG_VALUE_0".into(), hook_dir.into()),
            ],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn upload_pack(
        &self,
        cache_root: &Path,
        token: &str,
        request_id: &str,
        fingerprint: &str,
        task_id: &str,
        turn_id: &str,
        oid: &str,
        path_arg: Option<&str>,
        executor: &dyn GitServerExecutor,
    ) -> Result<Infallible, WorkerError> {
        let parsed_task: TaskId = task_id.parse().map_err(|_| invalid_component("task ID"))?;
        let parsed_turn: TurnId = turn_id.parse().map_err(|_| invalid_component("turn ID"))?;
        let imported: BaseOid = oid.parse().map_err(|_| invalid_component("imported OID"))?;
        let record = self
            .load_result_by_request_turn(request_id, parsed_turn)?
            .ok_or_else(missing_token)?;
        reject_path_arg(&record.project_id, path_arg)?;
        if record.token != token
            || record.request_id != request_id
            || record.fingerprint != fingerprint
            || record.task_id != task_id
            || record.turn_id != turn_id
            || record.imported_oid != oid
        {
            return Err(mismatch());
        }
        let digest = result_digest(parsed_task, parsed_turn, &imported)?;
        if digest != record.result_digest {
            return Err(mismatch());
        }
        self.repair_result_lookup(&record)?;
        let transfer = TransferRepo::open_controller_cache(
            cache_root,
            &record.project_id,
            &record.worktree_id,
        )?;
        if transfer.repo_id() != record.cache_id {
            return Err(mismatch());
        }
        let result_ref = frozen_result_ref(request_id, parsed_turn)?;
        let isolate = write_upload_isolate(transfer.path(), token, &result_ref, oid)?;
        let mirror = RootedDir::open(&isolate).map_err(WorkerError::Io)?;
        mirror
            .verify_descriptors_cloexec()
            .map_err(WorkerError::Io)?;
        executor.exec(
            "git-upload-pack",
            &mirror,
            &[
                ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
            ],
        )
    }

    /// Global `xfer.lock` for this controller transfer store. It is held across
    /// `finish_source_receive` owned-graph Git and `prepare_result_upload` pin
    /// Git, so a slow repository serializes unrelated transfers. Shared-cache
    /// Git CAS is a separate identity boundary; concurrent helper pushes after
    /// prepare do not prove independent transfer-record progress.
    fn lock_transfers(&self) -> Result<std::fs::File, WorkerError> {
        let file = self
            .root
            .open_private_lock(TRANSFER_LOCK)
            .map_err(store_io)?;
        lock_exclusive(&file)?;
        Ok(file)
    }

    fn load_source_by_request(
        &self,
        request_id: &str,
    ) -> Result<Option<SourceRecord>, WorkerError> {
        let Some(record) = self.read_json(&source_request_name(request_id)?)? else {
            return Ok(None);
        };
        validate_source_record(&record, request_id)?;
        Ok(Some(record))
    }

    fn load_result_by_request_turn(
        &self,
        request_id: &str,
        turn_id: TurnId,
    ) -> Result<Option<ResultRecord>, WorkerError> {
        let Some(record) = self.read_json(&result_request_name(request_id, turn_id)?)? else {
            return Ok(None);
        };
        validate_result_record(&record, request_id, turn_id)?;
        Ok(Some(record))
    }

    fn write_source(&self, record: &SourceRecord) -> Result<(), WorkerError> {
        let bytes = encode(record)?;
        match self
            .root
            .write_private_atomic_no_replace(&source_request_name(&record.request_id)?, &bytes)
        {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing = self
                    .load_source_by_request(&record.request_id)?
                    .ok_or_else(missing_token)?;
                if encode(&existing)? != bytes {
                    return Err(conflict(
                        "source transfer identity does not match the durable request",
                    ));
                }
            }
            Err(error) => return Err(store_io(error)),
        }
        self.repair_source_lookup(record)
    }

    fn write_result(&self, record: &ResultRecord) -> Result<(), WorkerError> {
        let turn_id: TurnId = record
            .turn_id
            .parse()
            .map_err(|_| invalid_component("turn ID"))?;
        let bytes = encode(record)?;
        match self.root.write_private_atomic_no_replace(
            &result_request_name(&record.request_id, turn_id)?,
            &bytes,
        ) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let existing = self
                    .load_result_by_request_turn(&record.request_id, turn_id)?
                    .ok_or_else(missing_token)?;
                if encode(&existing)? != bytes {
                    return Err(conflict(
                        "result transfer identity does not match the durable request",
                    ));
                }
            }
            Err(error) => return Err(store_io(error)),
        }
        self.repair_result_lookup(record)
    }

    fn repair_source_lookup(&self, record: &SourceRecord) -> Result<(), WorkerError> {
        self.repair_derived_lookup(
            &source_token_name(&record.token)?,
            &encode(record)?,
            &record.token,
            &record.request_id,
            None,
        )
    }

    fn repair_result_lookup(&self, record: &ResultRecord) -> Result<(), WorkerError> {
        self.repair_derived_lookup(
            &result_token_name(&record.token)?,
            &encode(record)?,
            &record.token,
            &record.request_id,
            Some(("turn_id", record.turn_id.as_str())),
        )
    }

    fn repair_derived_lookup(
        &self,
        name: &str,
        bytes: &[u8],
        token: &str,
        request_id: &str,
        extra: Option<(&str, &str)>,
    ) -> Result<(), WorkerError> {
        if !self.root.entry_exists(name).map_err(store_io)? {
            return match self.root.write_private_atomic_no_replace(name, bytes) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    self.repair_derived_lookup(name, bytes, token, request_id, extra)
                }
                Err(error) => Err(store_io(error)),
            };
        }
        let existing = self
            .root
            .read_private_regular(name, RECORD_LIMIT)
            .map_err(store_io)?;
        if existing == bytes {
            return Ok(());
        }
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&existing) {
            let same_token = value.get("token").and_then(|v| v.as_str()) == Some(token);
            let same_request = value.get("request_id").and_then(|v| v.as_str()) == Some(request_id);
            let same_extra = extra.is_none_or(|(key, expected)| {
                value.get(key).and_then(|v| v.as_str()) == Some(expected)
            });
            if !same_token || !same_request || !same_extra {
                return Err(conflict(
                    "derived transfer lookup does not match the durable request",
                ));
            }
        }
        self.root
            .replace_private_regular_exact(name, &existing, bytes)
            .map_err(store_io)
    }

    fn read_json<T: for<'de> Deserialize<'de>>(
        &self,
        name: &str,
    ) -> Result<Option<T>, WorkerError> {
        if !self.root.entry_exists(name).map_err(store_io)? {
            return Ok(None);
        }
        let bytes = self
            .root
            .read_private_regular(name, RECORD_LIMIT)
            .map_err(store_io)?;
        serde_json::from_slice(&bytes).map(Some).map_err(|_| {
            WorkerError::Protocol("CONTROLLER_TRANSPORT: transfer record is invalid".into())
        })
    }

    fn require_source_match(
        &self,
        record: &SourceRecord,
        identity: &ControllerReceiveIdentity,
    ) -> Result<(), WorkerError> {
        if record.version != RECORD_VERSION {
            return Err(invalid_component(
                "source transfer record version is unsupported",
            ));
        }
        if record.token != identity.token
            || record.request_id != identity.request_id
            || record.fingerprint != identity.fingerprint.as_str()
            || record.project_id != identity.project_id
            || record.worktree_id != identity.worktree_id
            || record.expected_oid != identity.expected_oid.as_str()
        {
            return Err(mismatch());
        }
        let digest = source_digest(
            &record.project_id,
            &record.worktree_id,
            &record
                .expected_oid
                .parse()
                .map_err(|_| invalid_component("expected OID"))?,
        )?;
        if digest != record.source_digest {
            return Err(mismatch());
        }
        Ok(())
    }
}

pub fn import_controller_result(
    laptop_transfer: &TransferRepo,
    runner: &dyn ProcessRunner,
    user_common_dir: &Path,
    request_id: &str,
    meta: &VerifiedResultMeta,
) -> Result<ImportReceipt, WorkerError> {
    let source_ref = frozen_result_ref(request_id, meta.turn_id)?;
    laptop_transfer.import_result_from_ref(
        runner,
        user_common_dir,
        &meta.worker,
        meta.task_id,
        &source_ref,
        &meta.imported_oid,
    )
}

fn reuse_source(
    existing: &SourceRecord,
    fingerprint: &RequestFingerprint,
    project_id: &str,
    worktree_id: &str,
    expected_oid: &BaseOid,
    digest: &str,
    cache_id: &str,
) -> Result<(), WorkerError> {
    if existing.fingerprint != fingerprint.as_str()
        || existing.project_id != project_id
        || existing.worktree_id != worktree_id
        || existing.expected_oid != expected_oid.as_str()
        || existing.source_digest != digest
        || existing.cache_id != cache_id
        || existing.version != RECORD_VERSION
    {
        return Err(conflict(
            "source transfer identity does not match the durable request",
        ));
    }
    Ok(())
}

fn reuse_result(
    existing: &ResultRecord,
    fingerprint: &RequestFingerprint,
    project_id: &str,
    worktree_id: &str,
    meta: &VerifiedResultMeta,
    digest: &str,
    cache_id: &str,
) -> Result<(), WorkerError> {
    if existing.fingerprint != fingerprint.as_str()
        || existing.project_id != project_id
        || existing.worktree_id != worktree_id
        || existing.task_id != meta.task_id.to_string()
        || existing.turn_id != meta.turn_id.to_string()
        || existing.imported_oid != meta.imported_oid.as_str()
        || existing.result_digest != digest
        || existing.cache_id != cache_id
        || existing.version != RECORD_VERSION
    {
        return Err(conflict(
            "result transfer identity does not match the durable request",
        ));
    }
    Ok(())
}

fn validate_source_record(record: &SourceRecord, request_id: &str) -> Result<(), WorkerError> {
    if record.version != RECORD_VERSION {
        return Err(invalid_component(
            "source transfer record version is unsupported",
        ));
    }
    if record.kind != SOURCE_KIND || record.request_id != request_id {
        return Err(mismatch());
    }
    let expected: BaseOid = record
        .expected_oid
        .parse()
        .map_err(|_| invalid_component("expected OID"))?;
    let digest = source_digest(&record.project_id, &record.worktree_id, &expected)?;
    if digest != record.source_digest {
        return Err(mismatch());
    }
    Ok(())
}

fn validate_result_record(
    record: &ResultRecord,
    request_id: &str,
    turn_id: TurnId,
) -> Result<(), WorkerError> {
    if record.version != RECORD_VERSION {
        return Err(invalid_component(
            "result transfer record version is unsupported",
        ));
    }
    if record.kind != RESULT_KIND
        || record.request_id != request_id
        || record.turn_id != turn_id.to_string()
    {
        return Err(mismatch());
    }
    let task_id: TaskId = record
        .task_id
        .parse()
        .map_err(|_| invalid_component("task ID"))?;
    let parsed_turn: TurnId = record
        .turn_id
        .parse()
        .map_err(|_| invalid_component("turn ID"))?;
    let imported: BaseOid = record
        .imported_oid
        .parse()
        .map_err(|_| invalid_component("imported OID"))?;
    let digest = result_digest(task_id, parsed_turn, &imported)?;
    if digest != record.result_digest {
        return Err(mismatch());
    }
    Ok(())
}

fn identity_from_source(record: &SourceRecord) -> Result<ControllerReceiveIdentity, WorkerError> {
    Ok(ControllerReceiveIdentity {
        token: record.token.clone(),
        request_id: record.request_id.clone(),
        fingerprint: RequestFingerprint::new(record.fingerprint.clone())?,
        project_id: record.project_id.clone(),
        worktree_id: record.worktree_id.clone(),
        expected_oid: record
            .expected_oid
            .parse()
            .map_err(|_| invalid_component("expected OID"))?,
    })
}

fn identity_from_result(record: &ResultRecord) -> Result<ControllerResultIdentity, WorkerError> {
    Ok(ControllerResultIdentity {
        token: record.token.clone(),
        request_id: record.request_id.clone(),
        fingerprint: RequestFingerprint::new(record.fingerprint.clone())?,
        project_id: record.project_id.clone(),
        worktree_id: record.worktree_id.clone(),
        task_id: record
            .task_id
            .parse()
            .map_err(|_| invalid_component("task ID"))?,
        turn_id: record
            .turn_id
            .parse()
            .map_err(|_| invalid_component("turn ID"))?,
        imported_oid: record
            .imported_oid
            .parse()
            .map_err(|_| invalid_component("imported OID"))?,
    })
}

fn write_receive_hook(
    transfer: &Path,
    token: &str,
    request_ref: &str,
    oid: &str,
) -> Result<PathBuf, WorkerError> {
    validate_token(token)?;
    if !request_ref
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.'))
        || !oid
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_component("receive hook identity"));
    }
    let invocation = mint_token();
    let dir = transfer
        .join("scratch")
        .join(format!("hooks-{token}-{invocation}"));
    fs::create_dir_all(&dir).map_err(WorkerError::Io)?;
    let staged = dir.join("pre-receive.tmp");
    let hook = dir.join("pre-receive");
    let script = format!(
        "#!/bin/sh\nstatus=0\nwhile read old new ref; do\n  if [ \"$ref\" != '{request_ref}' ]; then echo 'mac-worker: ref not allowed' >&2; status=1; fi\n  if [ \"$new\" = '0000000000000000000000000000000000000000' ]; then echo 'mac-worker: deletion not allowed' >&2; status=1; fi\n  if [ \"$new\" != '{oid}' ]; then echo 'mac-worker: oid mismatch' >&2; status=1; fi\ndone\nexit $status\n"
    );
    fs::write(&staged, script).map_err(WorkerError::Io)?;
    let mut permissions = fs::metadata(&staged)
        .map_err(WorkerError::Io)?
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&staged, permissions).map_err(WorkerError::Io)?;
    fs::rename(&staged, &hook).map_err(WorkerError::Io)?;
    Ok(dir)
}

fn write_upload_isolate(
    transfer: &Path,
    token: &str,
    result_ref: &str,
    oid: &str,
) -> Result<PathBuf, WorkerError> {
    validate_token(token)?;
    let invocation = mint_token();
    let isolate = transfer
        .join("scratch")
        .join(format!("upload-{token}-{invocation}"));
    fs::create_dir_all(&isolate).map_err(WorkerError::Io)?;
    let status = isolated_git_command()
        .args(["init", "-q", "--bare"])
        .arg(&isolate)
        .status()
        .map_err(WorkerError::Io)?;
    if !status.success() {
        return Err(git_error("could not isolate the controller result upload"));
    }
    let objects = isolate.join("objects/info");
    fs::create_dir_all(&objects).map_err(WorkerError::Io)?;
    fs::write(
        objects.join("alternates"),
        format!("{}\n", transfer.join("objects").display()),
    )
    .map_err(WorkerError::Io)?;
    let status = isolated_git_command()
        .args(["-C"])
        .arg(&isolate)
        .args(["update-ref", result_ref, oid])
        .status()
        .map_err(WorkerError::Io)?;
    if !status.success() {
        return Err(git_error(
            "could not pin the isolated controller result ref",
        ));
    }
    Ok(isolate)
}

fn isolated_git_command() -> Command {
    let mut command = Command::new("/usr/bin/git");
    apply_isolated_git_environment(&mut command);
    command
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0");
    command
}

fn source_request_name(request_id: &str) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("src-{request_id}.json"))
}

fn source_token_name(token: &str) -> Result<String, WorkerError> {
    validate_token(token)?;
    Ok(format!("tok-{token}.json"))
}

fn result_request_name(request_id: &str, turn_id: TurnId) -> Result<String, WorkerError> {
    validate_request_id(request_id)?;
    Ok(format!("res-{request_id}-{turn_id}.json"))
}

fn result_token_name(token: &str) -> Result<String, WorkerError> {
    validate_token(token)?;
    Ok(format!("rtk-{token}.json"))
}

fn mint_token() -> String {
    format!("{:x}", Uuid::new_v4().simple())
}

fn validate_token(token: &str) -> Result<(), WorkerError> {
    validate_request_id(token).map_err(|_| invalid_component("receive token"))
}

fn validate_sha256_id(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(invalid_component(label))
    }
}

fn reject_path_arg(project_id: &str, path_arg: Option<&str>) -> Result<(), WorkerError> {
    match path_arg {
        None => Ok(()),
        Some(path) if path == project_id => Ok(()),
        Some(_) => Err(invalid_component(
            "git destination is not the project identity",
        )),
    }
}

fn encode<T: Serialize>(record: &T) -> Result<Vec<u8>, WorkerError> {
    serde_json::to_vec(record).map_err(|_| {
        WorkerError::Protocol("CONTROLLER_TRANSPORT: transfer record could not be encoded".into())
    })
}

fn missing_token() -> WorkerError {
    WorkerError::Protocol("INVALID_COMPONENT: controller transfer token is unknown".into())
}

fn mismatch() -> WorkerError {
    WorkerError::Protocol(
        "TRANSFER_IDENTITY_MISMATCH: controller transfer identity was rejected".into(),
    )
}

fn conflict(message: &str) -> WorkerError {
    WorkerError::Protocol(format!("CONTROLLER_REQUEST_CONFLICT: {message}"))
}

fn invalid_component(label: &str) -> WorkerError {
    WorkerError::Protocol(format!("INVALID_COMPONENT: invalid {label}"))
}

fn git_error(message: &str) -> WorkerError {
    WorkerError::Git {
        code: "BASE_UNAVAILABLE",
        message: message.into(),
    }
}
