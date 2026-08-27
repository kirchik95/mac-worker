use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeStruct};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{error::WorkerError, protocol::PROTOCOL_VERSION};

pub const MAX_ARG_COUNT: usize = 256;
pub const MAX_ARG_BYTES: usize = 16 * 1024;
pub const MAX_COMMAND_BYTES: usize = 128 * 1024;
pub const MAX_LOG_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_TIMEOUT_MILLIS: u64 = 24 * 60 * 60 * 1000;

macro_rules! canonical_uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new(value: Uuid) -> Self {
                Self(value)
            }

            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:x}", self.0.simple())
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                if !is_lower_hex(value, 32) {
                    return Err("identifier must be a lowercase simple UUID".into());
                }
                Uuid::parse_str(value)
                    .map(Self)
                    .map_err(|_| "identifier must be a lowercase simple UUID".into())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

canonical_uuid_id!(JobId);
canonical_uuid_id!(ClientId);
canonical_uuid_id!(LeaseToken);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFingerprint(String);

impl RequestFingerprint {
    pub fn new(value: String) -> Result<Self, WorkerError> {
        if is_lower_hex(&value, 64) {
            Ok(Self(value))
        } else {
            Err(protocol_error(
                "request fingerprint must be 64 lowercase hexadecimal bytes",
            ))
        }
    }
}

impl fmt::Display for RequestFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RequestFingerprint {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if is_lower_hex(value, 64) {
            Ok(Self(value.into()))
        } else {
            Err("request fingerprint must be 64 lowercase hexadecimal bytes".into())
        }
    }
}

impl Serialize for RequestFingerprint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RequestFingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandSpec {
    Argv { argv: Vec<String> },
    Shell { shell: String },
}

impl CommandSpec {
    pub fn argv(argv: Vec<String>) -> Result<Self, WorkerError> {
        let command = Self::Argv { argv };
        command.validate()?;
        Ok(command)
    }

    pub fn shell(shell: String) -> Result<Self, WorkerError> {
        let command = Self::Shell { shell };
        command.validate()?;
        Ok(command)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Argv { argv } => {
                if argv.is_empty() || argv.len() > MAX_ARG_COUNT {
                    return Err(protocol_error("argv command has an invalid argument count"));
                }
                let mut total = 0_usize;
                for argument in argv {
                    validate_non_nul(argument, MAX_ARG_BYTES, "argv argument")?;
                    total = total
                        .checked_add(argument.len())
                        .ok_or_else(|| protocol_error("argv command exceeds its byte limit"))?;
                }
                if total > MAX_COMMAND_BYTES {
                    return Err(protocol_error("argv command exceeds its byte limit"));
                }
            }
            Self::Shell { shell } => validate_non_nul(shell, MAX_COMMAND_BYTES, "shell command")?,
        }
        Ok(())
    }

    pub fn summary(&self) -> CommandSummary {
        match self {
            Self::Argv { argv } => CommandSummary::Argv {
                arg_count: argv.len(),
            },
            Self::Shell { .. } => CommandSummary::Shell,
        }
    }
}

impl Serialize for CommandSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Argv { argv } => {
                let mut record = serializer.serialize_struct("CommandSpec", 2)?;
                record.serialize_field("mode", "argv")?;
                record.serialize_field("argv", argv)?;
                record.end()
            }
            Self::Shell { shell } => {
                let mut record = serializer.serialize_struct("CommandSpec", 2)?;
                record.serialize_field("mode", "shell")?;
                record.serialize_field("shell", shell)?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for CommandSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawCommandSpec {
            mode: String,
            #[serde(default)]
            argv: Option<Vec<String>>,
            #[serde(default)]
            shell: Option<String>,
        }

        let raw = RawCommandSpec::deserialize(deserializer)?;
        let command = match (raw.mode.as_str(), raw.argv, raw.shell) {
            ("argv", Some(argv), None) => Self::Argv { argv },
            ("shell", None, Some(shell)) => Self::Shell { shell },
            _ => return Err(de::Error::custom("command mode and payload must agree")),
        };
        command.validate().map_err(de::Error::custom)?;
        Ok(command)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandSummary {
    Argv { arg_count: usize },
    Shell,
}

impl CommandSummary {
    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Argv { arg_count } if (1..=MAX_ARG_COUNT).contains(arg_count) => Ok(()),
            Self::Shell => Ok(()),
            Self::Argv { .. } => Err(protocol_error(
                "command summary has an invalid argument count",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Uploading,
    Verified,
    Accepted,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Lost,
}

impl JobState {
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Uploading, Self::Verified | Self::Lost)
                | (Self::Verified, Self::Accepted | Self::Lost)
                | (Self::Accepted, Self::Running | Self::Failed | Self::Lost)
                | (
                    Self::Running,
                    Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut | Self::Lost
                )
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut | Self::Lost
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFingerprintMaterial {
    pub protocol_version: u32,
    pub job_id: JobId,
    pub client_id: ClientId,
    pub lease_token: LeaseToken,
    pub worker_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub manifest_digest: String,
    pub relative_working_dir: String,
    pub timeout_millis: u64,
    pub resource_class: String,
    pub command: CommandSpec,
}

impl RequestFingerprintMaterial {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        worker_name: String,
        project_id: String,
        worktree_id: String,
        manifest_digest: String,
        relative_working_dir: String,
        timeout_millis: u64,
        resource_class: String,
        command: CommandSpec,
    ) -> Result<Self, WorkerError> {
        let material = Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            client_id,
            lease_token,
            worker_name,
            project_id,
            worktree_id,
            manifest_digest,
            relative_working_dir,
            timeout_millis,
            resource_class,
            command,
        };
        material.validate()?;
        Ok(material)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "request fingerprint has an incompatible protocol version",
            ));
        }
        validate_non_nul(&self.worker_name, 128, "worker name")?;
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        validate_hex_component(&self.manifest_digest, "manifest digest")?;
        if self.relative_working_dir.as_bytes().contains(&0)
            || self.relative_working_dir.len() > MAX_COMMAND_BYTES
        {
            return Err(protocol_error("relative working directory is invalid"));
        }
        if self.timeout_millis == 0 || self.timeout_millis > MAX_TIMEOUT_MILLIS {
            return Err(protocol_error("timeout is outside the supported range"));
        }
        validate_non_nul(&self.resource_class, 64, "resource class")?;
        self.command.validate()
    }

    pub fn fingerprint(&self) -> RequestFingerprint {
        let bytes = serde_json::to_vec(self).expect("request fingerprint material is serializable");
        RequestFingerprint(format!("{:x}", Sha256::digest(bytes)))
    }
}

impl Serialize for RequestFingerprintMaterial {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("RequestFingerprintMaterial", 12)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("worker_name", &self.worker_name)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("relative_working_dir", &self.relative_working_dir)?;
        record.serialize_field("timeout_millis", &self.timeout_millis)?;
        record.serialize_field("resource_class", &self.resource_class)?;
        record.serialize_field("command", &self.command)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for RequestFingerprintMaterial {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RawMaterial {
            protocol_version: u32,
            job_id: JobId,
            client_id: ClientId,
            lease_token: LeaseToken,
            worker_name: String,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            relative_working_dir: String,
            timeout_millis: u64,
            resource_class: String,
            command: CommandSpec,
        }

        let raw = RawMaterial::deserialize(deserializer)?;
        let material = Self {
            protocol_version: raw.protocol_version,
            job_id: raw.job_id,
            client_id: raw.client_id,
            lease_token: raw.lease_token,
            worker_name: raw.worker_name,
            project_id: raw.project_id,
            worktree_id: raw.worktree_id,
            manifest_digest: raw.manifest_digest,
            relative_working_dir: raw.relative_working_dir,
            timeout_millis: raw.timeout_millis,
            resource_class: raw.resource_class,
            command: raw.command,
        };
        material.validate().map_err(de::Error::custom)?;
        Ok(material)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct JobStatus {
    pub state: JobState,
    pub updated_at_millis: u64,
    pub supervisor_pid: Option<u32>,
    pub supervisor_start_identity: Option<u64>,
    pub child_pid: Option<u32>,
    pub child_start_identity: Option<u64>,
    pub exit_code: Option<u8>,
    pub final_stdout_bytes: Option<u64>,
    pub final_stderr_bytes: Option<u64>,
    pub error_code: Option<String>,
}

impl JobStatus {
    pub fn accepted(updated_at_millis: u64) -> Result<Self, WorkerError> {
        Self::new(
            JobState::Accepted,
            updated_at_millis,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub fn running(
        updated_at_millis: u64,
        supervisor_pid: u32,
        supervisor_start_identity: u64,
        child_pid: u32,
        child_start_identity: u64,
    ) -> Result<Self, WorkerError> {
        Self::new(
            JobState::Running,
            updated_at_millis,
            Some(supervisor_pid),
            Some(supervisor_start_identity),
            Some(child_pid),
            Some(child_start_identity),
            None,
            None,
            None,
            None,
        )
    }

    pub fn succeeded(
        updated_at_millis: u64,
        stdout: u64,
        stderr: u64,
    ) -> Result<Self, WorkerError> {
        Self::new(
            JobState::Succeeded,
            updated_at_millis,
            None,
            None,
            None,
            None,
            Some(0),
            Some(stdout),
            Some(stderr),
            None,
        )
    }

    pub fn failed(
        updated_at_millis: u64,
        code: u8,
        stdout: u64,
        stderr: u64,
    ) -> Result<Self, WorkerError> {
        Self::new(
            JobState::Failed,
            updated_at_millis,
            None,
            None,
            None,
            None,
            Some(code),
            Some(stdout),
            Some(stderr),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state: JobState,
        updated_at_millis: u64,
        supervisor_pid: Option<u32>,
        supervisor_start_identity: Option<u64>,
        child_pid: Option<u32>,
        child_start_identity: Option<u64>,
        exit_code: Option<u8>,
        final_stdout_bytes: Option<u64>,
        final_stderr_bytes: Option<u64>,
        error_code: Option<String>,
    ) -> Result<Self, WorkerError> {
        let status = Self {
            state,
            updated_at_millis,
            supervisor_pid,
            supervisor_start_identity,
            child_pid,
            child_start_identity,
            exit_code,
            final_stdout_bytes,
            final_stderr_bytes,
            error_code,
        };
        status.validate()?;
        Ok(status)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_identity_pair(
            self.supervisor_pid,
            self.supervisor_start_identity,
            "supervisor",
        )?;
        validate_identity_pair(self.child_pid, self.child_start_identity, "child")?;
        if self.state == JobState::Running
            && (self.supervisor_pid.is_none() || self.child_pid.is_none())
        {
            return Err(protocol_error(
                "running status requires supervisor and child identities",
            ));
        }
        let has_lengths = self.final_stdout_bytes.is_some() && self.final_stderr_bytes.is_some();
        if self.state.is_terminal() != has_lengths {
            return Err(protocol_error(
                "terminal status must bind both final log lengths",
            ));
        }
        match self.state {
            JobState::Succeeded if self.exit_code != Some(0) => {
                return Err(protocol_error("succeeded status requires exit code zero"));
            }
            JobState::Failed if !self.exit_code.is_some_and(|code| code != 0) => {
                return Err(protocol_error("failed status requires a nonzero exit code"));
            }
            JobState::Succeeded | JobState::Failed => {}
            _ if self.exit_code.is_some() => {
                return Err(protocol_error(
                    "only command outcomes may include an exit code",
                ));
            }
            _ => {}
        }
        if let Some(error_code) = &self.error_code {
            validate_non_nul(error_code, 128, "status error code")?;
        }
        Ok(())
    }

    pub fn transition(&self, next: Self) -> Result<(), WorkerError> {
        self.validate()?;
        next.validate()?;
        if next.updated_at_millis < self.updated_at_millis {
            return Err(protocol_error("job status timestamp moved backwards"));
        }
        if !self.state.can_transition_to(next.state) {
            return Err(protocol_error("job state transition is not allowed"));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for JobStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            state: JobState,
            updated_at_millis: u64,
            supervisor_pid: Option<u32>,
            supervisor_start_identity: Option<u64>,
            child_pid: Option<u32>,
            child_start_identity: Option<u64>,
            exit_code: Option<u8>,
            final_stdout_bytes: Option<u64>,
            final_stderr_bytes: Option<u64>,
            error_code: Option<String>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.state,
            wire.updated_at_millis,
            wire.supervisor_pid,
            wire.supervisor_start_identity,
            wire.child_pid,
            wire.child_start_identity,
            wire.exit_code,
            wire.final_stdout_bytes,
            wire.final_stderr_bytes,
            wire.error_code,
        )
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobMeta {
    pub protocol_version: u32,
    pub job_id: JobId,
    pub client_id: ClientId,
    pub worker_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub manifest_digest: String,
    pub request_fingerprint: RequestFingerprint,
    pub command_summary: CommandSummary,
    pub relative_working_dir: String,
    pub timeout_millis: u64,
    pub resource_class: String,
    pub created_at_millis: u64,
}

impl JobMeta {
    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "job metadata has an incompatible protocol version",
            ));
        }
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        validate_hex_component(&self.manifest_digest, "manifest digest")?;
        self.command_summary.validate()?;
        if self.timeout_millis == 0 || self.timeout_millis > MAX_TIMEOUT_MILLIS {
            return Err(protocol_error(
                "job metadata timeout is outside the supported range",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LocalJobRecord {
    pub meta: JobMeta,
    pub lease_token: LeaseToken,
    pub last_status: Option<JobStatus>,
    pub cleanup_pending: bool,
}

impl LocalJobRecord {
    pub fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        if let Some(status) = &self.last_status {
            status.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseRecord {
    pub job_id: JobId,
    pub client_id: ClientId,
    pub lease_token: LeaseToken,
    pub request_fingerprint: RequestFingerprint,
    pub worker_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub manifest_digest: String,
    pub timeout_millis: u64,
    pub resource_class: String,
    pub command_summary: CommandSummary,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
}

impl LeaseRecord {
    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        validate_hex_component(&self.manifest_digest, "manifest digest")?;
        self.command_summary.validate()?;
        if self.timeout_millis == 0
            || self.timeout_millis > MAX_TIMEOUT_MILLIS
            || self.expires_at_millis < self.created_at_millis
        {
            return Err(protocol_error(
                "lease record timestamps or timeout are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseAcquireRequest {
    pub material: RequestFingerprintMaterial,
    pub request_fingerprint: RequestFingerprint,
}

impl LeaseAcquireRequest {
    pub fn new(material: RequestFingerprintMaterial) -> Self {
        let request_fingerprint = material.fingerprint();
        Self {
            material,
            request_fingerprint,
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.material.validate()?;
        if self.material.fingerprint() != self.request_fingerprint {
            return Err(protocol_error(
                "lease request fingerprint does not match its material",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum LeaseAcquireResponse {
    Acquired { lease: LeaseRecord },
    ExistingAccepted { status: JobStatus },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SubmitRequest {
    pub material: RequestFingerprintMaterial,
    pub request_fingerprint: RequestFingerprint,
}

impl SubmitRequest {
    pub fn new(material: RequestFingerprintMaterial) -> Self {
        let request_fingerprint = material.fingerprint();
        Self {
            material,
            request_fingerprint,
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        LeaseAcquireRequest {
            material: self.material.clone(),
            request_fingerprint: self.request_fingerprint.clone(),
        }
        .validate()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum SubmitResponse {
    Accepted {
        meta: Box<JobMeta>,
        status: JobStatus,
    },
    Existing {
        status: JobStatus,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StatusResponse {
    pub meta: JobMeta,
    pub status: JobStatus,
}

impl StatusResponse {
    pub fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        self.status.validate()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LogChunk {
    pub stream: LogStream,
    pub offset: u64,
    pub next_offset: u64,
    pub data: String,
}

impl LogChunk {
    pub fn new(stream: LogStream, offset: u64, bytes: Vec<u8>) -> Result<Self, WorkerError> {
        if bytes.len() > MAX_LOG_CHUNK_BYTES {
            return Err(protocol_error("log chunk exceeds its byte limit"));
        }
        let decoded_len = u64::try_from(bytes.len()).expect("usize fits in u64");
        let next_offset = offset
            .checked_add(decoded_len)
            .ok_or_else(|| protocol_error("log offset overflow"))?;
        Ok(Self {
            stream,
            offset,
            next_offset,
            data: STANDARD.encode(bytes),
        })
    }

    pub fn decoded_bytes(&self) -> Result<Vec<u8>, WorkerError> {
        let bytes = STANDARD
            .decode(&self.data)
            .map_err(|_| protocol_error("log chunk data is not base64"))?;
        if bytes.len() > MAX_LOG_CHUNK_BYTES
            || self.next_offset
                != self
                    .offset
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| protocol_error("log offset overflow"))?
        {
            return Err(protocol_error(
                "log chunk offsets do not match decoded bytes",
            ));
        }
        Ok(bytes)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.decoded_bytes().map(|_| ())
    }
}

impl<'de> Deserialize<'de> for LogChunk {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            stream: LogStream,
            offset: u64,
            next_offset: u64,
            data: String,
        }
        let wire = Wire::deserialize(deserializer)?;
        let chunk = Self {
            stream: wire.stream,
            offset: wire.offset,
            next_offset: wire.next_offset,
            data: wire.data,
        };
        chunk.validate().map_err(de::Error::custom)?;
        Ok(chunk)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum JsonEvent {
    Accepted {
        protocol_version: u32,
        response: SubmitResponse,
    },
    Log {
        protocol_version: u32,
        chunk: LogChunk,
    },
    Status {
        protocol_version: u32,
        response: Box<StatusResponse>,
    },
    Error {
        protocol_version: u32,
        code: String,
        message: String,
    },
}

impl JsonEvent {
    pub fn validate(&self) -> Result<(), WorkerError> {
        let version = match self {
            Self::Accepted {
                protocol_version,
                response,
            } => {
                response_status_validate(response)?;
                *protocol_version
            }
            Self::Log {
                protocol_version,
                chunk,
            } => {
                chunk.validate()?;
                *protocol_version
            }
            Self::Status {
                protocol_version,
                response,
            } => {
                response.validate()?;
                *protocol_version
            }
            Self::Error {
                protocol_version,
                code,
                message,
            } => {
                validate_non_nul(code, 128, "error code")?;
                validate_non_nul(message, 4096, "error message")?;
                *protocol_version
            }
        };
        if version != PROTOCOL_VERSION {
            return Err(protocol_error("event has an incompatible protocol version"));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for JsonEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Accepted {
                protocol_version: u32,
                response: SubmitResponse,
            },
            Log {
                protocol_version: u32,
                chunk: LogChunk,
            },
            Status {
                protocol_version: u32,
                response: Box<StatusResponse>,
            },
            Error {
                protocol_version: u32,
                code: String,
                message: String,
            },
        }

        let event = match Wire::deserialize(deserializer)? {
            Wire::Accepted {
                protocol_version,
                response,
            } => Self::Accepted {
                protocol_version,
                response,
            },
            Wire::Log {
                protocol_version,
                chunk,
            } => Self::Log {
                protocol_version,
                chunk,
            },
            Wire::Status {
                protocol_version,
                response,
            } => Self::Status {
                protocol_version,
                response,
            },
            Wire::Error {
                protocol_version,
                code,
                message,
            } => Self::Error {
                protocol_version,
                code,
                message,
            },
        };
        event.validate().map_err(de::Error::custom)?;
        Ok(event)
    }
}

fn response_status_validate(response: &SubmitResponse) -> Result<(), WorkerError> {
    match response {
        SubmitResponse::Accepted { meta, status } => {
            meta.validate()?;
            status.validate()
        }
        SubmitResponse::Existing { status } => status.validate(),
    }
}

fn validate_identity_pair(
    pid: Option<u32>,
    identity: Option<u64>,
    label: &str,
) -> Result<(), WorkerError> {
    if pid.is_some() != identity.is_some()
        || pid.is_some_and(|value| value == 0)
        || identity.is_some_and(|value| value == 0)
    {
        return Err(protocol_error(&format!(
            "{label} process identity is incomplete"
        )));
    }
    Ok(())
}

fn validate_non_nul(value: &str, max_bytes: usize, label: &str) -> Result<(), WorkerError> {
    if value.is_empty() || value.len() > max_bytes || value.as_bytes().contains(&0) {
        return Err(protocol_error(&format!(
            "{label} is empty, too long, or contains NUL"
        )));
    }
    Ok(())
}

fn validate_hex_component(value: &str, label: &str) -> Result<(), WorkerError> {
    if is_lower_hex(value, 64) {
        Ok(())
    } else {
        Err(protocol_error(&format!(
            "{label} must be 64 lowercase hexadecimal bytes"
        )))
    }
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn protocol_error(message: &str) -> WorkerError {
    WorkerError::Protocol(message.into())
}
