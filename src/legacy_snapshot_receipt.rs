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

#[cfg(test)]
mod tests {
    use super::*;
    const JOB_ID: &str = "00000000000000000000000000000001";
    const CLIENT_ID: &str = "00000000000000000000000000000002";
    const FINGERPRINT: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const MANIFEST_DIGEST: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const TOKEN_HASH: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    fn receipt_bytes() -> Vec<u8> {
        format!(
        concat!(
            r#"{{"version":1,"job_id":"{JOB_ID}","client_id":"{CLIENT_ID}","#,
            r#""lease_token_sha256":"{TOKEN_HASH}","request_fingerprint":"{FINGERPRINT}","#,
            r#""project_id":"{PROJECT_ID}","worktree_id":"{WORKTREE_ID}","#,
            r#""manifest_digest":"{MANIFEST_DIGEST}","cache_key":{{"project_id":"{PROJECT_ID}","#,
            r#""worktree_id":"{WORKTREE_ID}","manifest_digest":"{MANIFEST_DIGEST}"}},"#,
            r#""verified_at_millis":42}}"#,
        ),
        JOB_ID = JOB_ID,
        CLIENT_ID = CLIENT_ID,
        TOKEN_HASH = TOKEN_HASH,
        FINGERPRINT = FINGERPRINT,
        PROJECT_ID = PROJECT_ID,
        WORKTREE_ID = WORKTREE_ID,
        MANIFEST_DIGEST = MANIFEST_DIGEST,
    )
    .into_bytes()
    }

    trait ReplaceBytes {
        fn replace_bytes(&self, from: &[u8], to: &[u8]) -> Vec<u8>;
    }
    impl ReplaceBytes for [u8] {
        fn replace_bytes(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
            let offset = self
                .windows(from.len())
                .position(|window| window == from)
                .expect("fixture needle");
            [&self[..offset], to, &self[offset + from.len()..]].concat()
        }
    }
    fn insert_before_final_brace(bytes: &[u8], insertion: &[u8]) -> Vec<u8> {
        let mut changed = bytes[..bytes.len() - 1].to_vec();
        changed.extend_from_slice(insertion);
        changed.push(b'}');
        changed
    }

    #[test]
    // Supersedes the receipt/cache-key assertions in remote_snapshot::manifest_request_receipt_and_response_reject_unknown_duplicate_and_invalid_fields.
    fn receipt_and_cache_key_reject_unknown_duplicate_and_invalid_fields() {
        let receipt: VerifiedReceipt = serde_json::from_slice(&receipt_bytes()).unwrap();
        assert_eq!(serde_json::to_vec(&receipt).unwrap(), receipt_bytes());
        let mismatched_cache_key = receipt_bytes().replace_bytes(
            format!(r#""manifest_digest":"{MANIFEST_DIGEST}"}}"#).as_bytes(),
            format!(r#""manifest_digest":"{}"}}"#, "f".repeat(64)).as_bytes(),
        );
        assert!(serde_json::from_slice::<VerifiedReceipt>(&mismatched_cache_key).is_err());
        let unknown_receipt =
            insert_before_final_brace(&receipt_bytes(), b",\"lease_token\":\"secret\"");
        assert!(serde_json::from_slice::<VerifiedReceipt>(&unknown_receipt).is_err());

        let duplicate = insert_before_final_brace(
            &receipt_bytes(),
            format!(",\"job_id\":\"{JOB_ID}\"").as_bytes(),
        );
        assert!(serde_json::from_slice::<VerifiedReceipt>(&duplicate).is_err());
        for (old, new) in [
            ("\"version\":1".to_owned(), "\"version\":2".to_owned()),
            (
                format!("\"lease_token_sha256\":\"{TOKEN_HASH}\""),
                "\"lease_token_sha256\":\"NOT-A-HASH\"".into(),
            ),
        ] {
            assert!(
                serde_json::from_slice::<VerifiedReceipt>(
                    &receipt_bytes().replace_bytes(old.as_bytes(), new.as_bytes())
                )
                .is_err()
            );
        }
        let bytes = serde_json::to_vec(receipt.cache_key()).unwrap();
        let key: SnapshotCacheKey = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(serde_json::to_vec(&key).unwrap(), bytes);
        assert!(
            serde_json::from_slice::<SnapshotCacheKey>(&insert_before_final_brace(
                &bytes,
                b",\"extra\":1"
            ))
            .is_err()
        );
        assert!(
            serde_json::from_slice::<SnapshotCacheKey>(&insert_before_final_brace(
                &bytes,
                format!(",\"project_id\":\"{PROJECT_ID}\"").as_bytes()
            ))
            .is_err()
        );
        for field in ["project_id", "worktree_id", "manifest_digest"] {
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            value[field] = serde_json::json!("NOT-A-DIGEST");
            assert!(serde_json::from_value::<SnapshotCacheKey>(value).is_err());
        }
    }

    fn task_fixture(
        root: &std::path::Path,
    ) -> (HostStore, crate::job::LeaseRecord, ResolutionIdentity) {
        use crate::{
            job::{
                CommandSpec, ExecutionScope, LeaseAcquireRequest, LeaseAcquireResponse, LeaseToken,
                RequestFingerprintMaterial, ResolveOrAbandonRequest, SubmitRequest,
            },
            lease::{AdmissionFacts, LeaseService},
            protocol::MemoryPressure,
            task::TaskId,
        };
        let store = HostStore::open(root).unwrap();
        let material = RequestFingerprintMaterial::new(
            JobId::new(uuid::Uuid::from_u128(501)),
            ClientId::new(uuid::Uuid::from_u128(502)),
            LeaseToken::new(uuid::Uuid::from_u128(503)),
            1,
            "mini-1".into(),
            PROJECT_ID.into(),
            WORKTREE_ID.into(),
            MANIFEST_DIGEST.into(),
            String::new(),
            60_000,
            "heavy".into(),
            CommandSpec::shell("true".into()).unwrap(),
        )
        .unwrap();
        let scope = ExecutionScope::task(TaskId::new(uuid::Uuid::from_u128(504)));
        let acquire =
            LeaseAcquireRequest::new(material.clone()).with_execution_scope(scope.clone());
        let lease = match LeaseService::new(&store)
            .acquire(
                &acquire,
                &AdmissionFacts {
                    free_disk_bytes: 100 * 1024 * 1024 * 1024,
                    total_disk_bytes: 250 * 1024 * 1024 * 1024,
                    memory_pressure: MemoryPressure::Normal,
                    swap_used_bytes: Some(0),
                },
                1,
            )
            .unwrap()
        {
            LeaseAcquireResponse::Acquired { lease } => lease,
            _ => unreachable!(),
        };
        let submit = SubmitRequest::new(material).with_execution_scope(scope);
        let identity = ResolutionIdentity::from_request(
            &ResolveOrAbandonRequest::from_submit_request(&submit).unwrap(),
        )
        .unwrap();
        (store, lease, identity)
    }

    fn write_receipt(
        path: &std::path::Path,
        lease: &crate::job::LeaseRecord,
        identity: &ResolutionIdentity,
    ) {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let receipt = VerifiedReceipt::new(
            lease.job_id(),
            lease.client_id(),
            identity.token_hash(),
            lease.request_fingerprint().clone(),
            SnapshotCacheKey::new(
                lease.project_id().into(),
                lease.worktree_id().into(),
                lease.manifest_digest().into(),
            )
            .unwrap(),
            42,
        )
        .unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(&serde_json::to_vec(&receipt).unwrap())
            .unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }

    #[test]
    // Supersedes the shared typed-Io/lease-retention proposition in snapshot_transfer::publish_receipt_matching_pending_after_delete_is_retryable_io, including both moved cleanup fault points.
    fn exact_receipt_cleanup_faults_remain_io_keep_lease_and_converge_after_reopen() {
        use crate::lease::LeaseService;
        for point in [
            HostStoreWritePoint::AfterCleanupIntentCommit,
            HostStoreWritePoint::AfterResolutionVerifiedReceiptRemoval,
            HostStoreWritePoint::AfterResolutionVerificationStageRemoval,
        ] {
            let fixture = tempfile::tempdir().unwrap();
            let root = fixture.path().join("host");
            let (store, lease, identity) = task_fixture(&root);
            let receipt = store.verified_receipt(lease.job_id()).unwrap();
            let pending = receipt
                .parent()
                .unwrap()
                .join(format!(".verify-{}.json.pending", lease.job_id()));
            write_receipt(&receipt, &lease, &identity);
            write_receipt(&pending, &lease, &identity);
            drop(store);
            let faulted = HostStore::open_with_write_fault(&root, point).unwrap();
            let admission = faulted.admission_lock(lease.job_id()).unwrap();
            let transfer = faulted
                .transfer_lock_after(&admission, lease.job_id())
                .unwrap();
            let error = LegacySnapshotReceiptService::new(&faulted)
                .remove_resolution_evidence_after(&admission, &transfer, &identity)
                .unwrap_err();
            assert!(matches!(error, WorkerError::Io(_)), "{point:?}: {error}");
            assert_eq!(
                LeaseService::new(&faulted).load().unwrap(),
                Some(lease.clone())
            );
            assert!(!receipt.exists());
            if point == HostStoreWritePoint::AfterResolutionVerificationStageRemoval {
                assert!(!pending.exists());
            }
            drop(transfer);
            drop(admission);
            drop(faulted);
            let reopened = HostStore::open(&root).unwrap();
            let admission = reopened.admission_lock(lease.job_id()).unwrap();
            let transfer = reopened
                .transfer_lock_after(&admission, lease.job_id())
                .unwrap();
            let service = LegacySnapshotReceiptService::new(&reopened);
            service
                .remove_resolution_evidence_after(&admission, &transfer, &identity)
                .unwrap();
            service
                .resolution_evidence_absent_after(&admission, &transfer, &identity)
                .unwrap();
            assert!(!receipt.exists());
            assert!(!pending.exists());
            assert_eq!(LeaseService::new(&reopened).load().unwrap(), Some(lease));
            let namespace = root.join("verified/.mac-worker-rooted-fs");
            assert!(!namespace.exists() || std::fs::read_dir(namespace).unwrap().next().is_none());
        }
    }
}
