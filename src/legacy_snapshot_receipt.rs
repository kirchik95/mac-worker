use std::io;

use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    de::DeserializeOwned,
    ser::{self, SerializeStruct},
};

use crate::{
    error::WorkerError,
    host_store::{
        AdmissionGuard, HostStore, HostStoreWritePoint, ResolutionIdentity, TransferGuard,
    },
    job::{ClientId, JobId, RequestFingerprint},
};

const VERIFIED_RECEIPT_VERSION: u32 = 1;

pub struct LegacySnapshotReceiptService<'a> {
    pub(crate) store: &'a HostStore,
}

impl<'a> LegacySnapshotReceiptService<'a> {
    pub fn new(store: &'a HostStore) -> Self {
        Self { store }
    }

    pub(crate) fn validate_resolution_evidence_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let directory = self.store.open_directory("verified", false)?;
        for name in [
            format!("{}.json", identity.job_id()),
            format!(".verify-{}.json.pending", identity.job_id()),
        ] {
            if directory.entry_exists(&name)? {
                let bytes = directory
                    .read_private_regular(&name, 1024 * 1024)
                    .map_err(|_| unsafe_remote_snapshot())?;
                let receipt: VerifiedReceipt = decode_canonical_json(&bytes, "verified receipt")?;
                require_resolution_receipt(&receipt, identity)?;
            }
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn remove_resolution_evidence_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        self.validate_resolution_evidence_after(admission, transfer, identity)?;
        let directory = self.store.open_directory("verified", false)?;
        let receipt = format!("{}.json", identity.job_id());
        self.store
            .remove_owned_regular_committed(&directory, &receipt)?;
        directory.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterResolutionVerifiedReceiptRemoval)
        {
            return Err(WorkerError::Io(io::Error::other(
                "injected resolution verified-receipt cleanup interruption",
            )));
        }
        let staging = format!(".verify-{}.json.pending", identity.job_id());
        self.store
            .remove_owned_regular_committed(&directory, &staging)?;
        directory.sync_root()?;
        if self
            .store
            .consume_fault(HostStoreWritePoint::AfterResolutionVerificationStageRemoval)
        {
            return Err(WorkerError::Io(io::Error::other(
                "injected resolution verification-stage cleanup interruption",
            )));
        }
        admission.validate_for(identity.job_id())?;
        transfer.validate()
    }

    pub(crate) fn resolution_evidence_absent_after(
        &self,
        admission: &AdmissionGuard,
        transfer: &TransferGuard,
        identity: &ResolutionIdentity,
    ) -> Result<(), WorkerError> {
        admission.validate_for(identity.job_id())?;
        transfer.validate()?;
        let directory = self.store.open_directory("verified", false)?;
        for name in [
            format!("{}.json", identity.job_id()),
            format!(".verify-{}.json.pending", identity.job_id()),
        ] {
            if directory.entry_exists(&name)? {
                return Err(WorkerError::Protocol(
                    "exact verified state remains after resolution cleanup".into(),
                ));
            }
        }
        if directory.has_private_cleanup_residue()? {
            return Err(WorkerError::Protocol(
                "verified private cleanup residue remains".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCacheKey {
    pub(crate) project_id: String,
    pub(crate) worktree_id: String,
    pub(crate) manifest_digest: String,
}

impl SnapshotCacheKey {
    pub fn new(
        project_id: String,
        worktree_id: String,
        manifest_digest: String,
    ) -> Result<Self, WorkerError> {
        let key = Self {
            project_id,
            worktree_id,
            manifest_digest,
        };
        key.validate()?;
        Ok(key)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        validate_digest(&self.project_id, "project ID")?;
        validate_digest(&self.worktree_id, "worktree ID")?;
        validate_digest(&self.manifest_digest, "manifest digest")
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }
}

impl Serialize for SnapshotCacheKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("SnapshotCacheKey", 3)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for SnapshotCacheKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        SnapshotCacheKey::new(wire.project_id, wire.worktree_id, wire.manifest_digest)
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReceipt {
    pub(crate) version: u32,
    pub(crate) job_id: JobId,
    pub(crate) client_id: ClientId,
    pub(crate) lease_token_sha256: String,
    pub(crate) request_fingerprint: RequestFingerprint,
    pub(crate) project_id: String,
    pub(crate) worktree_id: String,
    pub(crate) manifest_digest: String,
    pub(crate) cache_key: SnapshotCacheKey,
    pub(crate) verified_at_millis: u64,
}

impl VerifiedReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token_sha256: String,
        request_fingerprint: RequestFingerprint,
        cache_key: SnapshotCacheKey,
        verified_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let receipt = Self {
            version: VERIFIED_RECEIPT_VERSION,
            job_id,
            client_id,
            lease_token_sha256,
            request_fingerprint,
            project_id: cache_key.project_id.clone(),
            worktree_id: cache_key.worktree_id.clone(),
            manifest_digest: cache_key.manifest_digest.clone(),
            cache_key,
            verified_at_millis,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.version != VERIFIED_RECEIPT_VERSION {
            return Err(protocol_error("verified receipt version mismatch"));
        }
        validate_digest(&self.lease_token_sha256, "lease token hash")?;
        validate_digest(&self.project_id, "project ID")?;
        validate_digest(&self.worktree_id, "worktree ID")?;
        validate_digest(&self.manifest_digest, "manifest digest")?;
        self.cache_key.validate()?;
        if self.project_id != self.cache_key.project_id
            || self.worktree_id != self.cache_key.worktree_id
            || self.manifest_digest != self.cache_key.manifest_digest
        {
            return Err(protocol_error("verified receipt cache key mismatch"));
        }
        Ok(())
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn lease_token_sha256(&self) -> &str {
        &self.lease_token_sha256
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }

    pub fn cache_key(&self) -> &SnapshotCacheKey {
        &self.cache_key
    }

    pub fn verified_at_millis(&self) -> u64 {
        self.verified_at_millis
    }
}

impl Serialize for VerifiedReceipt {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("VerifiedReceipt", 10)?;
        record.serialize_field("version", &self.version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token_sha256", &self.lease_token_sha256)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("cache_key", &self.cache_key)?;
        record.serialize_field("verified_at_millis", &self.verified_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for VerifiedReceipt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u32,
            job_id: JobId,
            client_id: ClientId,
            lease_token_sha256: String,
            request_fingerprint: RequestFingerprint,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            cache_key: SnapshotCacheKey,
            verified_at_millis: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        let receipt = Self {
            version: wire.version,
            job_id: wire.job_id,
            client_id: wire.client_id,
            lease_token_sha256: wire.lease_token_sha256,
            request_fingerprint: wire.request_fingerprint,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
            cache_key: wire.cache_key,
            verified_at_millis: wire.verified_at_millis,
        };
        receipt.validate().map_err(de::Error::custom)?;
        Ok(receipt)
    }
}

fn require_resolution_receipt(
    receipt: &VerifiedReceipt,
    identity: &ResolutionIdentity,
) -> Result<(), WorkerError> {
    receipt.validate()?;
    if receipt.job_id() == identity.job_id()
        && receipt.client_id() == identity.client_id()
        && receipt.lease_token_sha256() == identity.token_hash()
        && receipt.request_fingerprint() == identity.request_fingerprint()
        && receipt.cache_key().project_id() == identity.project_id()
        && receipt.cache_key().worktree_id() == identity.worktree_id()
        && receipt.cache_key().manifest_digest() == identity.manifest_digest()
    {
        Ok(())
    } else {
        Err(WorkerError::Protocol(
            "JOB_ID_CONFLICT: verified state belongs to another immutable request".into(),
        ))
    }
}

pub(crate) fn decode_canonical_json<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    _label: &str,
) -> Result<T, WorkerError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = T::deserialize(&mut deserializer).map_err(|_| unsafe_remote_snapshot())?;
    deserializer.end().map_err(|_| unsafe_remote_snapshot())?;
    if serde_json::to_vec(&value).map_err(|_| unsafe_remote_snapshot())? != bytes {
        return Err(unsafe_remote_snapshot());
    }
    Ok(value)
}

pub(crate) fn unsafe_remote_snapshot() -> WorkerError {
    WorkerError::Snapshot {
        code: "UNSAFE_REMOTE_SNAPSHOT",
        message: "remote snapshot filesystem state is unsafe".into(),
    }
}

pub(crate) fn validate_digest(value: &str, field: &str) -> Result<(), WorkerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(protocol_error(&format!(
            "{field} must be 64 lowercase hexadecimal bytes"
        )))
    }
}

pub(crate) fn protocol_error(message: &str) -> WorkerError {
    WorkerError::Protocol(message.into())
}
