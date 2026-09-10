use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    str::FromStr,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    ser::{self, SerializeStruct},
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    error::WorkerError,
    protocol::PROTOCOL_VERSION,
    scheduler::{CandidateSlot, WorkerPreference},
};

pub const MAX_ARG_COUNT: usize = 256;
pub const MAX_ARG_BYTES: usize = 16 * 1024;
pub const MAX_COMMAND_BYTES: usize = 128 * 1024;
pub const MAX_LOG_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_TIMEOUT_MILLIS: u64 = 24 * 60 * 60 * 1000;
pub const MAX_CONTROL_CODE_BYTES: usize = 128;
pub const MAX_CONTROL_MESSAGE_BYTES: usize = 4 * 1024;

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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
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

impl fmt::Debug for LeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LeaseToken([REDACTED])")
    }
}

impl fmt::Display for LeaseToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:x}", self.0.simple())
    }
}

impl FromStr for LeaseToken {
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

impl Serialize for LeaseToken {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for LeaseToken {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

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

#[derive(Clone, PartialEq, Eq)]
pub enum CommandSpec {
    Argv { argv: Vec<String> },
    Shell { shell: String },
}

impl fmt::Debug for CommandSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Argv { argv } => formatter
                .debug_struct("CommandSpec::Argv")
                .field("argument_count", &argv.len())
                .finish_non_exhaustive(),
            Self::Shell { .. } => formatter
                .debug_struct("CommandSpec::Shell")
                .finish_non_exhaustive(),
        }
    }
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
                    validate_argument(argument)?;
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

    pub fn summary(&self) -> Result<CommandSummary, WorkerError> {
        self.validate()?;
        match self {
            Self::Argv { argv } => CommandSummary::argv(argv.len()),
            Self::Shell { .. } => Ok(CommandSummary::shell()),
        }
    }
}

impl Serialize for CommandSpec {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSummary {
    mode: CommandSummaryMode,
    arg_count: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandSummaryMode {
    Argv,
    Shell,
}

impl CommandSummary {
    pub fn argv(arg_count: usize) -> Result<Self, WorkerError> {
        let summary = Self {
            mode: CommandSummaryMode::Argv,
            arg_count: Some(arg_count),
        };
        summary.validate()?;
        Ok(summary)
    }

    pub fn shell() -> Self {
        Self {
            mode: CommandSummaryMode::Shell,
            arg_count: None,
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        match (self.mode, self.arg_count) {
            (CommandSummaryMode::Argv, Some(arg_count))
                if (1..=MAX_ARG_COUNT).contains(&arg_count) =>
            {
                Ok(())
            }
            (CommandSummaryMode::Shell, None) => Ok(()),
            _ => Err(protocol_error(
                "command summary has an invalid argument count",
            )),
        }
    }

    pub fn arg_count(&self) -> Option<usize> {
        self.arg_count
    }
}

impl Serialize for CommandSummary {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        match self.mode {
            CommandSummaryMode::Argv => {
                let mut record = serializer.serialize_struct("CommandSummary", 2)?;
                record.serialize_field("mode", "argv")?;
                let arg_count = self.arg_count.ok_or_else(|| {
                    ser::Error::custom("validated argv summary has an argument count")
                })?;
                record.serialize_field("arg_count", &arg_count)?;
                record.end()
            }
            CommandSummaryMode::Shell => {
                let mut record = serializer.serialize_struct("CommandSummary", 1)?;
                record.serialize_field("mode", "shell")?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for CommandSummary {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Argv { arg_count: usize },
            Shell,
        }

        let summary = match Wire::deserialize(deserializer)? {
            Wire::Argv { arg_count } => Self {
                mode: CommandSummaryMode::Argv,
                arg_count: Some(arg_count),
            },
            Wire::Shell => Self::shell(),
        };
        summary.validate().map_err(de::Error::custom)?;
        Ok(summary)
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
                | (
                    Self::Accepted,
                    Self::Running | Self::Failed | Self::Cancelled | Self::Lost
                )
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

#[derive(Clone, PartialEq, Eq)]
pub struct RequestFingerprintMaterial {
    protocol_version: u32,
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    created_at_millis: u64,
    worker_name: String,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    relative_working_dir: String,
    timeout_millis: u64,
    resource_class: String,
    command: CommandSpec,
}

impl fmt::Debug for RequestFingerprintMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestFingerprintMaterial")
            .field("protocol_version", &self.protocol_version)
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("created_at_millis", &self.created_at_millis)
            .field("worker_name", &self.worker_name)
            .field("project_id", &self.project_id)
            .field("worktree_id", &self.worktree_id)
            .field("manifest_digest", &self.manifest_digest)
            .field("timeout_millis", &self.timeout_millis)
            .field("resource_class", &self.resource_class)
            .field("command", &self.command)
            .finish_non_exhaustive()
    }
}

impl RequestFingerprintMaterial {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        created_at_millis: u64,
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
            created_at_millis,
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

    pub fn job_id(&self) -> JobId {
        self.job_id
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }
    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
    pub fn worker_name(&self) -> &str {
        &self.worker_name
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
    pub fn relative_working_dir(&self) -> &str {
        &self.relative_working_dir
    }
    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }
    pub fn resource_class(&self) -> &str {
        &self.resource_class
    }
    pub fn command(&self) -> &CommandSpec {
        &self.command
    }
}

impl Serialize for RequestFingerprintMaterial {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("RequestFingerprintMaterial", 13)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
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
            created_at_millis: u64,
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
            created_at_millis: raw.created_at_millis,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pid: u32,
    start_time_micros: u64,
}

impl ProcessIdentity {
    pub fn new(pid: u32, start_time_micros: u64) -> Result<Self, WorkerError> {
        if pid == 0 || start_time_micros == 0 {
            return Err(protocol_error("process identity is invalid"));
        }
        Ok(Self {
            pid,
            start_time_micros,
        })
    }

    pub fn pid(self) -> u32 {
        self.pid
    }

    pub fn start_time_micros(self) -> u64 {
        self.start_time_micros
    }

    pub(crate) fn validate(self) -> Result<(), WorkerError> {
        Self::new(self.pid, self.start_time_micros).map(|_| ())
    }
}

/// One outstanding spawn permit. The token is unique per spawn attempt and is
/// never reused after release, steal, or completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunnerSlotReservation {
    reserver: ProcessIdentity,
    token: Uuid,
    child: Option<ProcessIdentity>,
}

impl RunnerSlotReservation {
    pub fn new(reserver: ProcessIdentity, token: Uuid) -> Result<Self, WorkerError> {
        reserver.validate()?;
        if token.is_nil() {
            return Err(protocol_error(
                "runner slot token must be unique and non-nil",
            ));
        }
        Ok(Self {
            reserver,
            token,
            child: None,
        })
    }

    pub fn with_child(self, child: ProcessIdentity) -> Result<Self, WorkerError> {
        child.validate()?;
        Ok(Self {
            child: Some(child),
            ..self
        })
    }

    pub fn reserver(self) -> ProcessIdentity {
        self.reserver
    }

    pub fn token(self) -> Uuid {
        self.token
    }

    pub fn child(self) -> Option<ProcessIdentity> {
        self.child
    }
}

impl Serialize for RunnerSlotReservation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = 2 + usize::from(self.child.is_some());
        let mut record = serializer.serialize_struct("RunnerSlotReservation", fields)?;
        record.serialize_field("reserver", &self.reserver)?;
        record.serialize_field("token", &self.token.to_string())?;
        if let Some(child) = &self.child {
            record.serialize_field("child", child)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for RunnerSlotReservation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            reserver: ProcessIdentity,
            token: String,
            #[serde(default)]
            child: Option<ProcessIdentity>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let token = Uuid::parse_str(&wire.token).map_err(de::Error::custom)?;
        let mut reservation = Self::new(wire.reserver, token).map_err(de::Error::custom)?;
        if let Some(child) = wire.child {
            reservation = reservation.with_child(child).map_err(de::Error::custom)?;
        }
        Ok(reservation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueId(u64);

impl QueueId {
    pub fn new(value: u64) -> Result<Self, WorkerError> {
        if value == 0 {
            return Err(protocol_error("queue ID must be positive"));
        }
        Ok(Self(value))
    }

    pub fn value(self) -> u64 {
        self.0
    }

    pub(crate) fn pending() -> Self {
        Self(0)
    }

    pub(crate) fn checked_next(self) -> Result<Self, WorkerError> {
        self.0
            .checked_add(1)
            .ok_or_else(|| protocol_error("queue sequence is exhausted"))
            .and_then(Self::new)
    }
}

impl Serialize for QueueId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::new(self.0).map_err(ser::Error::custom)?;
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for QueueId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(u64::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(String);

impl RunId {
    pub fn new(value: String) -> Result<Self, WorkerError> {
        validate_opaque_identifier(&value, "run ID")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RunId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_opaque_identifier(value, "run ID")
            .map_err(|_| "run ID is not canonical".to_owned())?;
        Ok(Self(value.to_owned()))
    }
}

impl Serialize for RunId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        validate_opaque_identifier(&self.0, "run ID").map_err(ser::Error::custom)?;
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueEntryKind {
    Batch,
    TaskTurn,
}

impl Serialize for QueueEntryKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(match self {
            Self::Batch => "batch",
            Self::TaskTurn => "task_turn",
        })
    }
}

impl<'de> Deserialize<'de> for QueueEntryKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match String::deserialize(deserializer)?.as_str() {
            "batch" => Ok(Self::Batch),
            "task_turn" => Ok(Self::TaskTurn),
            _ => Err(de::Error::custom("queue entry kind is invalid")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRunReference {
    run_id: RunId,
    max_parallel: u32,
}

impl QueueRunReference {
    pub fn new(run_id: RunId, max_parallel: u32) -> Result<Self, WorkerError> {
        let reference = Self {
            run_id,
            max_parallel,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_opaque_identifier(self.run_id.as_str(), "run ID")?;
        if self.max_parallel == 0 {
            return Err(protocol_error("run maximum parallelism must be positive"));
        }
        Ok(())
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn max_parallel(&self) -> u32 {
        self.max_parallel
    }
}

impl Serialize for QueueRunReference {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("QueueRunReference", 2)?;
        record.serialize_field("run_id", &self.run_id)?;
        record.serialize_field("max_parallel", &self.max_parallel)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for QueueRunReference {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            run_id: RunId,
            max_parallel: u32,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.run_id, wire.max_parallel).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueState {
    Waiting {
        owner: ProcessIdentity,
    },
    Dispatching {
        dispatch_owner: ProcessIdentity,
        selected_worker: String,
        claimed_at_millis: u64,
    },
    /// A task-turn row that is waiting for a runner slot. Parked rows are
    /// deliberately ownerless; the oldest parked row is adopted before it
    /// can enter the normal owner-scoped queue.
    Parked,
}

impl QueueState {
    pub fn owner(&self) -> &ProcessIdentity {
        match self {
            Self::Waiting { owner } => owner,
            Self::Dispatching { dispatch_owner, .. } => dispatch_owner,
            Self::Parked => panic!("parked queue rows do not have an owner"),
        }
    }

    pub fn owner_opt(&self) -> Option<&ProcessIdentity> {
        match self {
            Self::Waiting { owner } => Some(owner),
            Self::Dispatching { dispatch_owner, .. } => Some(dispatch_owner),
            Self::Parked => None,
        }
    }

    fn validate(&self, enqueued_at_millis: u64) -> Result<(), WorkerError> {
        if let Some(owner) = self.owner_opt() {
            owner.validate()?;
        }
        if let Self::Dispatching {
            selected_worker,
            claimed_at_millis,
            ..
        } = self
        {
            validate_worker_name(selected_worker)?;
            if *claimed_at_millis == 0 || *claimed_at_millis < enqueued_at_millis {
                return Err(protocol_error(
                    "queue claim timestamp predates enqueue timestamp",
                ));
            }
        }
        Ok(())
    }
}

impl Serialize for QueueState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate(0).map_err(ser::Error::custom)?;
        match self {
            Self::Waiting { owner } => {
                let mut record = serializer.serialize_struct("QueueState", 2)?;
                record.serialize_field("state", "waiting")?;
                record.serialize_field("owner", owner)?;
                record.end()
            }
            Self::Dispatching {
                dispatch_owner,
                selected_worker,
                claimed_at_millis,
            } => {
                let mut record = serializer.serialize_struct("QueueState", 4)?;
                record.serialize_field("state", "dispatching")?;
                record.serialize_field("dispatch_owner", dispatch_owner)?;
                record.serialize_field("selected_worker", selected_worker)?;
                record.serialize_field("claimed_at_millis", claimed_at_millis)?;
                record.end()
            }
            Self::Parked => {
                let mut record = serializer.serialize_struct("QueueState", 1)?;
                record.serialize_field("state", "parked")?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for QueueState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Waiting {
                owner: ProcessIdentity,
            },
            Dispatching {
                dispatch_owner: ProcessIdentity,
                selected_worker: String,
                claimed_at_millis: u64,
            },
            Parked {},
        }
        let state = match Wire::deserialize(deserializer)? {
            Wire::Waiting { owner } => Self::Waiting { owner },
            Wire::Dispatching {
                dispatch_owner,
                selected_worker,
                claimed_at_millis,
            } => Self::Dispatching {
                dispatch_owner,
                selected_worker,
                claimed_at_millis,
            },
            Wire::Parked {} => Self::Parked,
        };
        state.validate(0).map_err(de::Error::custom)?;
        Ok(state)
    }
}

impl Serialize for WorkerPreference {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Automatic => {
                let mut record = serializer.serialize_struct("WorkerPreference", 1)?;
                record.serialize_field("mode", "automatic")?;
                record.end()
            }
            Self::Pinned { worker } => {
                validate_worker_name(worker).map_err(ser::Error::custom)?;
                let mut record = serializer.serialize_struct("WorkerPreference", 2)?;
                record.serialize_field("mode", "pinned")?;
                record.serialize_field("worker", worker)?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for WorkerPreference {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Automatic {},
            Pinned { worker: String },
        }
        let preference = match Wire::deserialize(deserializer)? {
            Wire::Automatic {} => Self::Automatic,
            Wire::Pinned { worker } => Self::Pinned { worker },
        };
        validate_worker_preference(&preference).map_err(de::Error::custom)?;
        Ok(preference)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueAbandonmentProof {
    queue_id: QueueId,
    job_id: JobId,
    client_id: ClientId,
    selected_worker: String,
    project_id: String,
    worktree_id: String,
    command_summary: CommandSummary,
    request_fingerprint: RequestFingerprint,
    claimed_at_millis: u64,
    recorded_by: ProcessIdentity,
}

impl QueueAbandonmentProof {
    pub(crate) fn from_resolution(
        entry: &QueueEntry,
        request: &ResolveOrAbandonRequest,
        recorded_by: ProcessIdentity,
    ) -> Result<Self, WorkerError> {
        request.validate()?;
        recorded_by.validate()?;
        let QueueState::Dispatching {
            dispatch_owner,
            selected_worker,
            claimed_at_millis,
        } = entry.state()
        else {
            return Err(protocol_error(
                "pre-acceptance abandonment proof requires a dispatch reservation",
            ));
        };
        if *dispatch_owner != recorded_by {
            return Err(protocol_error("queue dispatch owner does not match"));
        }
        if request.job_id() != entry.job_id()
            || request.client_id() != entry.client_id()
            || request.worker_name() != selected_worker
            || request.project_id() != entry.project_id()
            || request.worktree_id() != entry.worktree_id()
            || request.command_summary() != entry.command_summary()
        {
            return Err(protocol_error(
                "abandonment resolution does not match the queue reservation",
            ));
        }
        let proof = Self {
            queue_id: entry.queue_id(),
            job_id: entry.job_id(),
            client_id: entry.client_id(),
            selected_worker: selected_worker.clone(),
            project_id: entry.project_id().into(),
            worktree_id: entry.worktree_id().into(),
            command_summary: entry.command_summary().clone(),
            request_fingerprint: request.request_fingerprint().clone(),
            claimed_at_millis: *claimed_at_millis,
            recorded_by,
        };
        proof.validate_against(entry)?;
        Ok(proof)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        QueueId::new(self.queue_id.value())?;
        validate_worker_name(&self.selected_worker)?;
        validate_hex_component(&self.project_id, "abandonment proof project ID")?;
        validate_hex_component(&self.worktree_id, "abandonment proof worktree ID")?;
        self.command_summary.validate()?;
        self.recorded_by.validate()?;
        if self.claimed_at_millis == 0 {
            return Err(protocol_error(
                "abandonment proof claim timestamp must be positive",
            ));
        }
        Ok(())
    }

    fn validate_against(&self, entry: &QueueEntry) -> Result<(), WorkerError> {
        self.validate()?;
        let QueueState::Dispatching {
            selected_worker,
            claimed_at_millis,
            ..
        } = entry.state()
        else {
            return Err(protocol_error(
                "pre-acceptance abandonment proof requires a dispatch reservation",
            ));
        };
        if self.queue_id != entry.queue_id()
            || self.job_id != entry.job_id()
            || self.client_id != entry.client_id()
            || self.selected_worker != *selected_worker
            || self.project_id != entry.project_id()
            || self.worktree_id != entry.worktree_id()
            || self.command_summary != *entry.command_summary()
            || self.claimed_at_millis != *claimed_at_millis
        {
            return Err(protocol_error(
                "abandonment proof does not match its queue reservation",
            ));
        }
        if entry.kind() == QueueEntryKind::Batch && self.recorded_by != *entry.owner() {
            return Err(protocol_error(
                "batch abandonment proof must match its immutable owner",
            ));
        }
        Ok(())
    }

    pub fn queue_id(&self) -> QueueId {
        self.queue_id
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn selected_worker(&self) -> &str {
        &self.selected_worker
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn command_summary(&self) -> &CommandSummary {
        &self.command_summary
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }

    pub fn claimed_at_millis(&self) -> u64 {
        self.claimed_at_millis
    }

    pub fn recorded_by(&self) -> &ProcessIdentity {
        &self.recorded_by
    }
}

impl Serialize for QueueAbandonmentProof {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("QueueAbandonmentProof", 10)?;
        record.serialize_field("queue_id", &self.queue_id)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("selected_worker", &self.selected_worker)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("claimed_at_millis", &self.claimed_at_millis)?;
        record.serialize_field("recorded_by", &self.recorded_by)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for QueueAbandonmentProof {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            queue_id: QueueId,
            job_id: JobId,
            client_id: ClientId,
            selected_worker: String,
            project_id: String,
            worktree_id: String,
            command_summary: CommandSummary,
            request_fingerprint: RequestFingerprint,
            claimed_at_millis: u64,
            recorded_by: ProcessIdentity,
        }
        let wire = Wire::deserialize(deserializer)?;
        let proof = Self {
            queue_id: wire.queue_id,
            job_id: wire.job_id,
            client_id: wire.client_id,
            selected_worker: wire.selected_worker,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            command_summary: wire.command_summary,
            request_fingerprint: wire.request_fingerprint,
            claimed_at_millis: wire.claimed_at_millis,
            recorded_by: wire.recorded_by,
        };
        proof.validate().map_err(de::Error::custom)?;
        Ok(proof)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEntry {
    pub(crate) queue_id: QueueId,
    pub(crate) job_id: JobId,
    pub(crate) client_id: ClientId,
    pub(crate) project_id: String,
    pub(crate) worktree_id: String,
    pub(crate) command_summary: CommandSummary,
    pub(crate) requirements: Vec<String>,
    pub(crate) preference: WorkerPreference,
    pub(crate) kind: QueueEntryKind,
    pub(crate) run: Option<QueueRunReference>,
    pub(crate) enqueue_owner: ProcessIdentity,
    pub(crate) state: QueueState,
    pub(crate) preacceptance_abandonment_proof: Option<QueueAbandonmentProof>,
    pub(crate) cancel_requested_at_millis: Option<u64>,
    pub(crate) enqueued_at_millis: u64,
    pub(crate) slot_reservation: Option<RunnerSlotReservation>,
}

impl QueueEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        project_id: String,
        worktree_id: String,
        command_summary: CommandSummary,
        requirements: Vec<String>,
        preference: WorkerPreference,
        kind: QueueEntryKind,
        run: Option<QueueRunReference>,
        owner: ProcessIdentity,
        enqueued_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let entry = Self {
            queue_id: QueueId::pending(),
            job_id,
            client_id,
            project_id,
            worktree_id,
            command_summary,
            requirements,
            preference,
            kind,
            run,
            enqueue_owner: owner,
            state: QueueState::Waiting { owner },
            preacceptance_abandonment_proof: None,
            cancel_requested_at_millis: None,
            enqueued_at_millis,
            slot_reservation: None,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_hex_component(&self.project_id, "queue project ID")?;
        validate_hex_component(&self.worktree_id, "queue worktree ID")?;
        self.command_summary.validate()?;
        validate_requirements(&self.requirements)?;
        validate_worker_preference(&self.preference)?;
        self.enqueue_owner.validate()?;
        self.state.validate(self.enqueued_at_millis)?;
        if self.enqueued_at_millis == 0 {
            return Err(protocol_error("queue enqueue timestamp must be positive"));
        }
        if let Some(run) = &self.run {
            run.validate()?;
        }
        if let Some(cancelled_at) = self.cancel_requested_at_millis
            && cancelled_at < self.enqueued_at_millis
        {
            return Err(protocol_error(
                "queue cancellation timestamp predates enqueue timestamp",
            ));
        }
        if let Some(reserved) = self.slot_reservation {
            RunnerSlotReservation::new(reserved.reserver, reserved.token)?;
        }
        if let Some(proof) = &self.preacceptance_abandonment_proof {
            proof.validate_against(self)?;
        }
        if self.kind == QueueEntryKind::Batch && self.owner_opt() != Some(&self.enqueue_owner) {
            return Err(protocol_error("batch queue ownership is not replaceable"));
        }
        Ok(())
    }

    pub fn queue_id(&self) -> QueueId {
        self.queue_id
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn worktree_id(&self) -> &str {
        &self.worktree_id
    }

    pub fn command_summary(&self) -> &CommandSummary {
        &self.command_summary
    }

    pub fn requirements(&self) -> &[String] {
        &self.requirements
    }

    pub fn preference(&self) -> &WorkerPreference {
        &self.preference
    }

    pub fn kind(&self) -> QueueEntryKind {
        self.kind
    }

    pub fn run(&self) -> Option<&QueueRunReference> {
        self.run.as_ref()
    }

    pub fn enqueue_owner(&self) -> &ProcessIdentity {
        &self.enqueue_owner
    }

    pub fn owner(&self) -> &ProcessIdentity {
        self.state.owner()
    }

    /// Returns the durable owner, if this row is not parked. Unlike
    /// [`Self::owner`], this is safe for callers inspecting all queue rows.
    pub fn owner_opt(&self) -> Option<&ProcessIdentity> {
        self.state.owner_opt()
    }

    pub fn state(&self) -> &QueueState {
        &self.state
    }

    pub fn preacceptance_abandonment_proof(&self) -> Option<&QueueAbandonmentProof> {
        self.preacceptance_abandonment_proof.as_ref()
    }

    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested_at_millis.is_some()
    }

    pub fn cancel_requested_at_millis(&self) -> Option<u64> {
        self.cancel_requested_at_millis
    }

    pub fn enqueued_at_millis(&self) -> u64 {
        self.enqueued_at_millis
    }

    pub fn slot_reservation(&self) -> Option<RunnerSlotReservation> {
        self.slot_reservation
    }

    pub(crate) fn coalesce_enqueued_at(&mut self, floor: u64) {
        if self.enqueued_at_millis < floor {
            self.enqueued_at_millis = floor;
        }
    }

    pub(crate) fn set_slot_reservation(
        &mut self,
        reservation: Option<RunnerSlotReservation>,
    ) -> Result<(), WorkerError> {
        if let Some(reservation) = reservation {
            RunnerSlotReservation::new(reservation.reserver, reservation.token)?;
        }
        self.slot_reservation = reservation;
        self.validate()
    }

    pub(crate) fn assign_queue_id(&mut self, queue_id: QueueId) {
        self.queue_id = queue_id;
    }

    pub(crate) fn adopt(&mut self, owner: ProcessIdentity) -> Result<(), WorkerError> {
        if self.kind != QueueEntryKind::TaskTurn {
            return Err(protocol_error("only a task turn can be adopted"));
        }
        match &mut self.state {
            QueueState::Waiting {
                owner: current_owner,
            } => *current_owner = owner,
            QueueState::Dispatching { dispatch_owner, .. } => *dispatch_owner = owner,
            QueueState::Parked => {
                return Err(protocol_error("a parked task turn must be unparked first"));
            }
        }
        self.validate()
    }

    pub(crate) fn park(&mut self) -> Result<(), WorkerError> {
        if self.kind != QueueEntryKind::TaskTurn {
            return Err(protocol_error("only a task turn can be parked"));
        }
        if self.preacceptance_abandonment_proof.is_some() {
            return Err(protocol_error("an abandoned queue row cannot be parked"));
        }
        if !matches!(self.state, QueueState::Waiting { .. }) {
            return Err(protocol_error("only a waiting task turn can be parked"));
        }
        self.state = QueueState::Parked;
        self.slot_reservation = None;
        self.validate()
    }

    pub(crate) fn unpark(&mut self, owner: ProcessIdentity) -> Result<(), WorkerError> {
        owner.validate()?;
        if self.kind != QueueEntryKind::TaskTurn || !matches!(self.state, QueueState::Parked) {
            return Err(protocol_error("only a parked task turn can be unparked"));
        }
        self.state = QueueState::Waiting { owner };
        self.validate()
    }

    pub(crate) fn record_preacceptance_abandoned(
        &mut self,
        proof: QueueAbandonmentProof,
    ) -> Result<bool, WorkerError> {
        proof.validate_against(self)?;
        if !matches!(
            self.state,
            QueueState::Dispatching {
                dispatch_owner: current,
                ..
            } if current == proof.recorded_by
        ) {
            return Err(protocol_error("queue dispatch owner does not match"));
        }
        if let Some(existing) = &self.preacceptance_abandonment_proof {
            return if existing == &proof {
                Ok(false)
            } else {
                Err(protocol_error(
                    "queue row already has a different abandonment proof",
                ))
            };
        }
        self.preacceptance_abandonment_proof = Some(proof);
        self.validate()?;
        Ok(true)
    }

    pub(crate) fn dispatch(
        &mut self,
        dispatch_owner: ProcessIdentity,
        selected_worker: String,
        claimed_at_millis: u64,
    ) -> Result<(), WorkerError> {
        self.state = QueueState::Dispatching {
            dispatch_owner,
            selected_worker,
            claimed_at_millis,
        };
        self.validate()
    }

    pub(crate) fn revert(&mut self, dispatch_owner: ProcessIdentity) -> Result<(), WorkerError> {
        if self.preacceptance_abandonment_proof.is_some() {
            return Err(protocol_error(
                "a proven abandoned queue row cannot be reverted",
            ));
        }
        match self.state {
            QueueState::Dispatching {
                dispatch_owner: current,
                ..
            } if current == dispatch_owner => {
                self.state = QueueState::Waiting {
                    owner: dispatch_owner,
                };
                self.validate()
            }
            QueueState::Parked => Err(protocol_error("a parked queue row is not dispatching")),
            _ => Err(protocol_error("queue dispatch owner does not match")),
        }
    }

    pub(crate) fn request_cancel(&mut self, requested_at_millis: u64) -> Result<(), WorkerError> {
        if requested_at_millis < self.enqueued_at_millis {
            return Err(protocol_error(
                "queue cancellation timestamp predates enqueue timestamp",
            ));
        }
        if let Some(existing) = self.cancel_requested_at_millis
            && requested_at_millis < existing
        {
            return Err(protocol_error(
                "queue cancellation timestamp moved backwards",
            ));
        }
        self.cancel_requested_at_millis = Some(requested_at_millis);
        self.validate()
    }

    pub(crate) fn eligible_for(&self, worker: &str, capabilities: Option<&[String]>) -> bool {
        let preference_matches = match &self.preference {
            WorkerPreference::Automatic => true,
            WorkerPreference::Pinned { worker: pinned } => pinned == worker,
        };
        preference_matches
            && (self.requirements.is_empty()
                || capabilities.is_some_and(|capabilities| {
                    self.requirements
                        .iter()
                        .all(|required| capabilities.contains(required))
                }))
    }
}

impl Serialize for QueueEntry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let fields = 15 + usize::from(self.slot_reservation.is_some());
        let mut record = serializer.serialize_struct("QueueEntry", fields)?;
        record.serialize_field("queue_id", &self.queue_id)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.serialize_field("requirements", &self.requirements)?;
        record.serialize_field("preference", &self.preference)?;
        record.serialize_field("kind", &self.kind)?;
        record.serialize_field("run", &self.run)?;
        record.serialize_field("enqueue_owner", &self.enqueue_owner)?;
        record.serialize_field("state", &self.state)?;
        record.serialize_field(
            "preacceptance_abandonment_proof",
            &self.preacceptance_abandonment_proof,
        )?;
        record.serialize_field(
            "cancel_requested_at_millis",
            &self.cancel_requested_at_millis,
        )?;
        record.serialize_field("enqueued_at_millis", &self.enqueued_at_millis)?;
        if let Some(reservation) = &self.slot_reservation {
            record.serialize_field("slot_reservation", reservation)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for QueueEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            queue_id: QueueId,
            job_id: JobId,
            client_id: ClientId,
            project_id: String,
            worktree_id: String,
            command_summary: CommandSummary,
            requirements: Vec<String>,
            preference: WorkerPreference,
            kind: QueueEntryKind,
            run: Option<QueueRunReference>,
            enqueue_owner: ProcessIdentity,
            state: QueueState,
            preacceptance_abandonment_proof: Option<QueueAbandonmentProof>,
            cancel_requested_at_millis: Option<u64>,
            enqueued_at_millis: u64,
            #[serde(default)]
            slot_reservation: Option<RunnerSlotReservation>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let entry = Self {
            queue_id: wire.queue_id,
            job_id: wire.job_id,
            client_id: wire.client_id,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            command_summary: wire.command_summary,
            requirements: wire.requirements,
            preference: wire.preference,
            kind: wire.kind,
            run: wire.run,
            enqueue_owner: wire.enqueue_owner,
            state: wire.state,
            preacceptance_abandonment_proof: wire.preacceptance_abandonment_proof,
            cancel_requested_at_millis: wire.cancel_requested_at_millis,
            enqueued_at_millis: wire.enqueued_at_millis,
            slot_reservation: wire.slot_reservation,
        };
        entry.validate().map_err(de::Error::custom)?;
        Ok(entry)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueSnapshot {
    pub(crate) next_id: QueueId,
    pub(crate) entries: Vec<QueueEntry>,
}

impl QueueSnapshot {
    pub fn validate(&self) -> Result<(), WorkerError> {
        let mut previous_id = 0;
        let mut previous_timestamp = 0;
        let mut jobs = HashSet::new();
        let mut dispatch_workers = BTreeSet::new();
        let mut run_caps = HashMap::new();
        for entry in &self.entries {
            entry.validate()?;
            let queue_id = entry.queue_id.value();
            if queue_id == 0 || queue_id <= previous_id || queue_id >= self.next_id.value() {
                return Err(protocol_error(
                    "queue IDs are not strictly increasing below next ID",
                ));
            }
            if entry.enqueued_at_millis < previous_timestamp {
                return Err(protocol_error("queue timestamps are not monotonic"));
            }
            if !jobs.insert(entry.job_id) {
                return Err(protocol_error("queue contains a duplicate job ID"));
            }
            if let Some(run) = entry.run()
                && let Some(existing) = run_caps.insert(run.run_id().clone(), run.max_parallel())
                && existing != run.max_parallel()
            {
                return Err(protocol_error(
                    "queue contains conflicting maximum parallelism for one run ID",
                ));
            }
            if let QueueState::Dispatching {
                selected_worker, ..
            } = &entry.state
                && !dispatch_workers.insert(selected_worker.as_str())
            {
                return Err(protocol_error(
                    "queue contains duplicate dispatch worker reservations",
                ));
            }
            previous_id = queue_id;
            previous_timestamp = entry.enqueued_at_millis;
        }
        Ok(())
    }

    pub fn next_id(&self) -> QueueId {
        self.next_id
    }

    pub fn entries(&self) -> &[QueueEntry] {
        &self.entries
    }

    pub(crate) fn empty() -> Self {
        Self {
            next_id: QueueId(1),
            entries: Vec::new(),
        }
    }
}

impl Serialize for QueueSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("QueueSnapshot", 2)?;
        record.serialize_field("next_id", &self.next_id)?;
        record.serialize_field("entries", &self.entries)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for QueueSnapshot {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            next_id: QueueId,
            entries: Vec<QueueEntry>,
        }
        let snapshot = {
            let wire = Wire::deserialize(deserializer)?;
            Self {
                next_id: wire.next_id,
                entries: wire.entries,
            }
        };
        snapshot.validate().map_err(de::Error::custom)?;
        Ok(snapshot)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueClaim {
    entry: QueueEntry,
}

impl QueueClaim {
    pub(crate) fn new(entry: QueueEntry) -> Self {
        Self { entry }
    }

    pub fn entry(&self) -> &QueueEntry {
        &self.entry
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueCancel {
    RemovedWaiting {
        job_id: JobId,
    },
    RequestedDispatch {
        job_id: JobId,
        dispatch_owner: ProcessIdentity,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionObservation {
    worker_name: String,
    ready: bool,
    slot: CandidateSlot,
    capabilities: Vec<String>,
    available_memory_bytes: Option<u64>,
    free_disk_bytes: u64,
    observed_at_millis: u64,
    /// Interactive herdr agents excluding mac-worker's reporter. Absent
    /// from cache files written before this field existed.
    interactive_agents: Option<u32>,
    ssh: String,
    remote_binary: String,
    inventory_capabilities: Option<Vec<String>>,
    slots: Option<u8>,
    facts_age_millis: Option<u64>,
    final_probe_started_at_millis: Option<u64>,
}

impl AdmissionObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_name: String,
        ready: bool,
        slot: CandidateSlot,
        capabilities: Vec<String>,
        available_memory_bytes: Option<u64>,
        free_disk_bytes: u64,
        observed_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        let observation = Self {
            worker_name,
            ready,
            slot,
            capabilities,
            available_memory_bytes,
            free_disk_bytes,
            observed_at_millis,
            interactive_agents: None,
            ssh: String::new(),
            remote_binary: String::new(),
            inventory_capabilities: None,
            slots: None,
            facts_age_millis: None,
            final_probe_started_at_millis: None,
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn with_interactive_agents(mut self, interactive_agents: Option<u32>) -> Self {
        self.interactive_agents = interactive_agents;
        self
    }

    /// Local cache binding for skip-SSH. Missing fields keep the record an
    /// unbound miss. Not part of the public task/wire schema.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn with_local_binding(
        mut self,
        ssh: String,
        remote_binary: String,
        inventory_capabilities: Vec<String>,
        slots: u8,
        facts_age_millis: Option<u64>,
        final_probe_started_at_millis: u64,
    ) -> Self {
        self.ssh = ssh;
        self.remote_binary = remote_binary;
        self.inventory_capabilities = Some(inventory_capabilities);
        self.slots = Some(slots);
        self.facts_age_millis = facts_age_millis;
        self.final_probe_started_at_millis = Some(final_probe_started_at_millis);
        self
    }

    pub(crate) fn binding_complete(&self) -> bool {
        !self.ssh.is_empty()
            && !self.remote_binary.is_empty()
            && self.inventory_capabilities.is_some()
            && self.slots.is_some()
            && self.final_probe_started_at_millis.is_some()
    }

    pub(crate) fn matches_worker(&self, worker: &crate::config::WorkerEntry) -> bool {
        self.ssh == worker.ssh
            && self.remote_binary == worker.remote_binary
            && self.inventory_capabilities.as_deref() == Some(worker.capabilities.as_slice())
            && self.slots == Some(worker.slots)
    }

    pub(crate) fn facts_age_millis(&self) -> Option<u64> {
        self.facts_age_millis
    }

    pub(crate) fn final_probe_started_at_millis(&self) -> Option<u64> {
        self.final_probe_started_at_millis
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_worker_name(&self.worker_name)?;
        validate_requirements(&self.capabilities)?;
        if self.observed_at_millis == 0 {
            return Err(protocol_error(
                "admission observation timestamp must be positive",
            ));
        }
        Ok(())
    }

    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn slot(&self) -> CandidateSlot {
        self.slot
    }

    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    pub fn available_memory_bytes(&self) -> Option<u64> {
        self.available_memory_bytes
    }

    pub fn free_disk_bytes(&self) -> u64 {
        self.free_disk_bytes
    }

    pub fn observed_at_millis(&self) -> u64 {
        self.observed_at_millis
    }

    pub fn interactive_agents(&self) -> Option<u32> {
        self.interactive_agents
    }
}

impl Serialize for AdmissionObservation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let bound = self.binding_complete();
        let fields = 7 + usize::from(self.interactive_agents.is_some()) + if bound { 6 } else { 0 };
        let mut record = serializer.serialize_struct("AdmissionObservation", fields)?;
        record.serialize_field("worker_name", &self.worker_name)?;
        record.serialize_field("ready", &self.ready)?;
        record.serialize_field(
            "slot",
            match self.slot {
                CandidateSlot::Idle => "idle",
                CandidateSlot::Busy => "busy",
            },
        )?;
        record.serialize_field("capabilities", &self.capabilities)?;
        record.serialize_field("available_memory_bytes", &self.available_memory_bytes)?;
        record.serialize_field("free_disk_bytes", &self.free_disk_bytes)?;
        record.serialize_field("observed_at_millis", &self.observed_at_millis)?;
        if let Some(interactive_agents) = self.interactive_agents {
            record.serialize_field("interactive_agents", &interactive_agents)?;
        }
        if bound {
            record.serialize_field("ssh", &self.ssh)?;
            record.serialize_field("remote_binary", &self.remote_binary)?;
            record.serialize_field("inventory_capabilities", &self.inventory_capabilities)?;
            record.serialize_field("slots", &self.slots)?;
            record.serialize_field("facts_age_millis", &self.facts_age_millis)?;
            record.serialize_field(
                "final_probe_started_at_millis",
                &self.final_probe_started_at_millis,
            )?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for AdmissionObservation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            worker_name: String,
            ready: bool,
            slot: String,
            capabilities: Vec<String>,
            available_memory_bytes: Option<u64>,
            free_disk_bytes: u64,
            observed_at_millis: u64,
            #[serde(default)]
            interactive_agents: Option<u32>,
            #[serde(default)]
            ssh: String,
            #[serde(default)]
            remote_binary: String,
            #[serde(default)]
            inventory_capabilities: Option<Vec<String>>,
            #[serde(default)]
            slots: Option<u8>,
            #[serde(default)]
            facts_age_millis: Option<u64>,
            #[serde(default)]
            final_probe_started_at_millis: Option<u64>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let slot = match wire.slot.as_str() {
            "idle" => CandidateSlot::Idle,
            "busy" => CandidateSlot::Busy,
            _ => return Err(de::Error::custom("admission observation slot is invalid")),
        };
        let mut observation = Self::new(
            wire.worker_name,
            wire.ready,
            slot,
            wire.capabilities,
            wire.available_memory_bytes,
            wire.free_disk_bytes,
            wire.observed_at_millis,
        )
        .map_err(de::Error::custom)?
        .with_interactive_agents(wire.interactive_agents);
        observation.ssh = wire.ssh;
        observation.remote_binary = wire.remote_binary;
        observation.inventory_capabilities = wire.inventory_capabilities;
        observation.slots = wire.slots;
        observation.facts_age_millis = wire.facts_age_millis;
        observation.final_probe_started_at_millis = wire.final_probe_started_at_millis;
        Ok(observation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedAdmissionObservation {
    observation: AdmissionObservation,
    age_millis: u64,
}

impl CachedAdmissionObservation {
    pub(crate) fn new(
        observation: AdmissionObservation,
        now_millis: u64,
    ) -> Result<Self, WorkerError> {
        let age_millis = now_millis
            .checked_sub(observation.observed_at_millis())
            .ok_or_else(|| protocol_error("admission observation timestamp is in the future"))?;
        Ok(Self {
            observation,
            age_millis,
        })
    }

    pub fn observation(&self) -> &AdmissionObservation {
        &self.observation
    }

    pub fn age_millis(&self) -> u64 {
        self.age_millis
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobStatus {
    state: JobState,
    updated_at_millis: u64,
    supervisor_pid: Option<u32>,
    supervisor_start_identity: Option<u64>,
    child_pid: Option<u32>,
    child_start_identity: Option<u64>,
    exit_code: Option<u8>,
    terminating_signal: Option<u32>,
    final_stdout_bytes: Option<u64>,
    final_stderr_bytes: Option<u64>,
    error_code: Option<String>,
    cleanup_error_code: Option<String>,
}

impl Serialize for JobStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("JobStatus", 12)?;
        record.serialize_field("state", &self.state)?;
        record.serialize_field("updated_at_millis", &self.updated_at_millis)?;
        record.serialize_field("supervisor_pid", &self.supervisor_pid)?;
        record.serialize_field("supervisor_start_identity", &self.supervisor_start_identity)?;
        record.serialize_field("child_pid", &self.child_pid)?;
        record.serialize_field("child_start_identity", &self.child_start_identity)?;
        record.serialize_field("exit_code", &self.exit_code)?;
        record.serialize_field("terminating_signal", &self.terminating_signal)?;
        record.serialize_field("final_stdout_bytes", &self.final_stdout_bytes)?;
        record.serialize_field("final_stderr_bytes", &self.final_stderr_bytes)?;
        record.serialize_field("error_code", &self.error_code)?;
        record.serialize_field("cleanup_error_code", &self.cleanup_error_code)?;
        record.end()
    }
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
            None,
            Some(stdout),
            Some(stderr),
            None,
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
            None,
            Some(stdout),
            Some(stderr),
            None,
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
        terminating_signal: Option<u32>,
        final_stdout_bytes: Option<u64>,
        final_stderr_bytes: Option<u64>,
        error_code: Option<String>,
        cleanup_error_code: Option<String>,
    ) -> Result<Self, WorkerError> {
        let status = Self {
            state,
            updated_at_millis,
            supervisor_pid,
            supervisor_start_identity,
            child_pid,
            child_start_identity,
            exit_code,
            terminating_signal,
            final_stdout_bytes,
            final_stderr_bytes,
            error_code,
            cleanup_error_code,
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
        if self.child_pid.is_some() && self.supervisor_pid.is_none() {
            return Err(protocol_error(
                "child identity requires a supervisor identity",
            ));
        }
        if self.state == JobState::Running
            && (self.supervisor_pid.is_none() || self.child_pid.is_none())
        {
            return Err(protocol_error(
                "running status requires supervisor and child identities",
            ));
        }
        if self.final_stdout_bytes.is_some() != self.final_stderr_bytes.is_some() {
            return Err(protocol_error("final log lengths must be present together"));
        }
        let has_lengths = self.final_stdout_bytes.is_some();
        if self.state.is_terminal() != has_lengths {
            return Err(protocol_error(
                "terminal status must bind both final log lengths",
            ));
        }
        match self.state {
            JobState::Succeeded
                if self.exit_code != Some(0) || self.terminating_signal.is_some() =>
            {
                return Err(protocol_error("succeeded status requires exit code zero"));
            }
            JobState::Failed
                if !matches!(
                    (self.exit_code, self.terminating_signal),
                    (Some(1..=u8::MAX), None) | (None, Some(1..=u32::MAX))
                ) =>
            {
                return Err(protocol_error(
                    "failed status requires exactly one nonzero command outcome",
                ));
            }
            JobState::Succeeded | JobState::Failed => {}
            _ if self.exit_code.is_some() || self.terminating_signal.is_some() => {
                return Err(protocol_error(
                    "only command outcomes may include an exit code or signal",
                ));
            }
            _ => {}
        }
        if let Some(error_code) = &self.error_code {
            validate_non_nul(error_code, 128, "status error code")?;
        }
        if let Some(cleanup_error_code) = &self.cleanup_error_code {
            if !self.state.is_terminal() {
                return Err(protocol_error("cleanup error requires a terminal status"));
            }
            validate_non_nul(cleanup_error_code, 128, "cleanup error code")?;
        }
        Ok(())
    }

    pub fn transition(&self, next: Self) -> Result<(), WorkerError> {
        self.validate()?;
        next.validate()?;
        if next.updated_at_millis < self.updated_at_millis {
            return Err(protocol_error("job status timestamp moved backwards"));
        }
        require_sticky_identity(
            self.supervisor_identity(),
            next.supervisor_identity(),
            "supervisor",
        )?;
        require_sticky_identity(self.child_identity(), next.child_identity(), "child")?;
        if self.state == JobState::Accepted && next.state == JobState::Accepted {
            let supervisor_added = self.supervisor_pid.is_none() && next.supervisor_pid.is_some();
            let child_added = self.child_pid.is_none() && next.child_pid.is_some();
            let exactly_one_added = supervisor_added ^ child_added;
            if !exactly_one_added
                || self.exit_code != next.exit_code
                || self.terminating_signal != next.terminating_signal
                || self.error_code != next.error_code
                || self.cleanup_error_code != next.cleanup_error_code
            {
                return Err(protocol_error(
                    "accepted identity enrichment must add exactly one process identity",
                ));
            }
            return Ok(());
        }
        if self.supervisor_identity() != next.supervisor_identity()
            || self.child_identity() != next.child_identity()
        {
            return Err(protocol_error(
                "process identities may change only during accepted enrichment",
            ));
        }
        if self.state.is_terminal() && self.state == next.state {
            if self.cleanup_error_code.is_none()
                && next.cleanup_error_code.is_some()
                && self.exit_code == next.exit_code
                && self.terminating_signal == next.terminating_signal
                && self.final_stdout_bytes == next.final_stdout_bytes
                && self.final_stderr_bytes == next.final_stderr_bytes
                && self.error_code == next.error_code
            {
                return Ok(());
            }
            return Err(protocol_error(
                "terminal status permits only one cleanup-error enrichment",
            ));
        }
        if !self.state.can_transition_to(next.state) {
            return Err(protocol_error("job state transition is not allowed"));
        }
        Ok(())
    }

    pub fn with_supervisor(
        &self,
        identity: ProcessIdentity,
        updated_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        identity.validate()?;
        if self.state != JobState::Accepted || self.supervisor_pid.is_some() {
            return Err(protocol_error(
                "supervisor identity may enrich accepted status once",
            ));
        }
        let next = Self::new(
            self.state,
            updated_at_millis,
            Some(identity.pid()),
            Some(identity.start_time_micros()),
            self.child_pid,
            self.child_start_identity,
            self.exit_code,
            self.terminating_signal,
            self.final_stdout_bytes,
            self.final_stderr_bytes,
            self.error_code.clone(),
            self.cleanup_error_code.clone(),
        )?;
        self.transition(next.clone())?;
        Ok(next)
    }

    pub fn with_child(
        &self,
        identity: ProcessIdentity,
        updated_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        identity.validate()?;
        if self.state != JobState::Accepted
            || self.supervisor_pid.is_none()
            || self.child_pid.is_some()
        {
            return Err(protocol_error(
                "child identity may enrich supervised accepted status once",
            ));
        }
        let next = Self::new(
            self.state,
            updated_at_millis,
            self.supervisor_pid,
            self.supervisor_start_identity,
            Some(identity.pid()),
            Some(identity.start_time_micros()),
            self.exit_code,
            self.terminating_signal,
            self.final_stdout_bytes,
            self.final_stderr_bytes,
            self.error_code.clone(),
            self.cleanup_error_code.clone(),
        )?;
        self.transition(next.clone())?;
        Ok(next)
    }

    pub fn into_running(&self, updated_at_millis: u64) -> Result<Self, WorkerError> {
        let next = Self::new(
            JobState::Running,
            updated_at_millis,
            self.supervisor_pid,
            self.supervisor_start_identity,
            self.child_pid,
            self.child_start_identity,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;
        self.transition(next.clone())?;
        Ok(next)
    }

    pub fn into_succeeded(
        &self,
        updated_at_millis: u64,
        stdout: u64,
        stderr: u64,
    ) -> Result<Self, WorkerError> {
        self.transition_to_terminal(
            JobState::Succeeded,
            updated_at_millis,
            Some(0),
            None,
            stdout,
            stderr,
            None,
        )
    }

    pub fn into_failed_exit(
        &self,
        updated_at_millis: u64,
        exit_code: u8,
        stdout: u64,
        stderr: u64,
    ) -> Result<Self, WorkerError> {
        self.transition_to_terminal(
            JobState::Failed,
            updated_at_millis,
            Some(exit_code),
            None,
            stdout,
            stderr,
            None,
        )
    }

    pub fn into_failed_signal(
        &self,
        updated_at_millis: u64,
        signal: u32,
        stdout: u64,
        stderr: u64,
    ) -> Result<Self, WorkerError> {
        self.transition_to_terminal(
            JobState::Failed,
            updated_at_millis,
            None,
            Some(signal),
            stdout,
            stderr,
            None,
        )
    }

    pub fn into_infrastructure_terminal(
        &self,
        state: JobState,
        updated_at_millis: u64,
        stdout: u64,
        stderr: u64,
        error_code: String,
    ) -> Result<Self, WorkerError> {
        if !matches!(
            state,
            JobState::Cancelled | JobState::TimedOut | JobState::Lost
        ) {
            return Err(protocol_error(
                "infrastructure terminal constructor requires a non-command terminal state",
            ));
        }
        self.transition_to_terminal(
            state,
            updated_at_millis,
            None,
            None,
            stdout,
            stderr,
            Some(error_code),
        )
    }

    /// Marks an accepted job whose child was never durably recorded as
    /// cancelled.  This is deliberately narrower than the generic
    /// infrastructure-terminal constructor: query validation recognizes this
    /// one no-child terminal shape as the launch-fence cancellation outcome.
    pub fn into_prelaunch_cancelled(
        &self,
        updated_at_millis: u64,
        stdout: u64,
        stderr: u64,
    ) -> Result<Self, WorkerError> {
        if self.state != JobState::Accepted || self.child_identity().is_some() {
            return Err(protocol_error(
                "prelaunch cancellation requires accepted status without a child identity",
            ));
        }
        self.transition_to_terminal(
            JobState::Cancelled,
            updated_at_millis,
            None,
            None,
            stdout,
            stderr,
            Some("CANCELLED_PRELAUNCH".into()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn transition_to_terminal(
        &self,
        state: JobState,
        updated_at_millis: u64,
        exit_code: Option<u8>,
        terminating_signal: Option<u32>,
        stdout: u64,
        stderr: u64,
        error_code: Option<String>,
    ) -> Result<Self, WorkerError> {
        let next = Self::new(
            state,
            updated_at_millis,
            self.supervisor_pid,
            self.supervisor_start_identity,
            self.child_pid,
            self.child_start_identity,
            exit_code,
            terminating_signal,
            Some(stdout),
            Some(stderr),
            error_code,
            None,
        )?;
        self.transition(next.clone())?;
        Ok(next)
    }

    pub fn with_cleanup_error(
        &self,
        cleanup_error_code: String,
        updated_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        if !self.state.is_terminal() || self.cleanup_error_code.is_some() {
            return Err(protocol_error(
                "cleanup error may enrich a terminal status once",
            ));
        }
        let next = Self::new(
            self.state,
            updated_at_millis,
            self.supervisor_pid,
            self.supervisor_start_identity,
            self.child_pid,
            self.child_start_identity,
            self.exit_code,
            self.terminating_signal,
            self.final_stdout_bytes,
            self.final_stderr_bytes,
            self.error_code.clone(),
            Some(cleanup_error_code),
        )?;
        self.transition(next.clone())?;
        Ok(next)
    }

    pub fn state(&self) -> JobState {
        self.state
    }
    pub fn updated_at_millis(&self) -> u64 {
        self.updated_at_millis
    }
    pub fn final_stdout_bytes(&self) -> Option<u64> {
        self.final_stdout_bytes
    }
    pub fn final_stderr_bytes(&self) -> Option<u64> {
        self.final_stderr_bytes
    }
    pub fn exit_code(&self) -> Option<u8> {
        self.exit_code
    }
    pub fn terminating_signal(&self) -> Option<u32> {
        self.terminating_signal
    }
    pub fn supervisor_identity(&self) -> Option<ProcessIdentity> {
        match (self.supervisor_pid, self.supervisor_start_identity) {
            (Some(pid), Some(start)) => ProcessIdentity::new(pid, start).ok(),
            _ => None,
        }
    }
    pub fn child_identity(&self) -> Option<ProcessIdentity> {
        match (self.child_pid, self.child_start_identity) {
            (Some(pid), Some(start)) => ProcessIdentity::new(pid, start).ok(),
            _ => None,
        }
    }
    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }
    pub fn cleanup_error_code(&self) -> Option<&str> {
        self.cleanup_error_code.as_deref()
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
            terminating_signal: Option<u32>,
            final_stdout_bytes: Option<u64>,
            final_stderr_bytes: Option<u64>,
            error_code: Option<String>,
            cleanup_error_code: Option<String>,
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
            wire.terminating_signal,
            wire.final_stdout_bytes,
            wire.final_stderr_bytes,
            wire.error_code,
            wire.cleanup_error_code,
        )
        .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobMeta {
    protocol_version: u32,
    job_id: JobId,
    client_id: ClientId,
    worker_name: String,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    request_fingerprint: RequestFingerprint,
    command_summary: CommandSummary,
    relative_working_dir: String,
    timeout_millis: u64,
    resource_class: String,
    created_at_millis: u64,
}

impl Serialize for JobMeta {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("JobMeta", 13)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("worker_name", &self.worker_name)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.serialize_field("relative_working_dir", &self.relative_working_dir)?;
        record.serialize_field("timeout_millis", &self.timeout_millis)?;
        record.serialize_field("resource_class", &self.resource_class)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.end()
    }
}

impl JobMeta {
    pub fn new(
        material: &RequestFingerprintMaterial,
        request_fingerprint: RequestFingerprint,
    ) -> Result<Self, WorkerError> {
        if material.fingerprint() != request_fingerprint {
            return Err(protocol_error(
                "job metadata fingerprint does not match its material",
            ));
        }
        let meta = Self {
            protocol_version: PROTOCOL_VERSION,
            job_id: material.job_id,
            client_id: material.client_id,
            worker_name: material.worker_name.clone(),
            project_id: material.project_id.clone(),
            worktree_id: material.worktree_id.clone(),
            manifest_digest: material.manifest_digest.clone(),
            request_fingerprint,
            command_summary: material.command.summary()?,
            relative_working_dir: material.relative_working_dir.clone(),
            timeout_millis: material.timeout_millis,
            resource_class: material.resource_class.clone(),
            created_at_millis: material.created_at_millis,
        };
        meta.validate()?;
        Ok(meta)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "job metadata has an incompatible protocol version",
            ));
        }
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        validate_hex_component(&self.manifest_digest, "manifest digest")?;
        validate_non_nul(&self.worker_name, 128, "worker name")?;
        if self.relative_working_dir.as_bytes().contains(&0)
            || self.relative_working_dir.len() > MAX_COMMAND_BYTES
        {
            return Err(protocol_error(
                "job metadata relative working directory is invalid",
            ));
        }
        validate_non_nul(&self.resource_class, 64, "resource class")?;
        self.command_summary.validate()?;
        if self.timeout_millis == 0 || self.timeout_millis > MAX_TIMEOUT_MILLIS {
            return Err(protocol_error(
                "job metadata timeout is outside the supported range",
            ));
        }
        Ok(())
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
    pub fn command_summary(&self) -> &CommandSummary {
        &self.command_summary
    }
    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
    pub fn worker_name(&self) -> &str {
        &self.worker_name
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
    pub fn relative_working_dir(&self) -> &str {
        &self.relative_working_dir
    }
    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }
    pub fn resource_class(&self) -> &str {
        &self.resource_class
    }
}

impl<'de> Deserialize<'de> for JobMeta {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
            client_id: ClientId,
            worker_name: String,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            request_fingerprint: RequestFingerprint,
            command_summary: CommandSummary,
            relative_working_dir: String,
            timeout_millis: u64,
            resource_class: String,
            created_at_millis: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        let meta = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
            client_id: wire.client_id,
            worker_name: wire.worker_name,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
            request_fingerprint: wire.request_fingerprint,
            command_summary: wire.command_summary,
            relative_working_dir: wire.relative_working_dir,
            timeout_millis: wire.timeout_millis,
            resource_class: wire.resource_class,
            created_at_millis: wire.created_at_millis,
        };
        meta.validate().map_err(de::Error::custom)?;
        Ok(meta)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteUncertainty {
    None,
    UnknownRemote { code: String },
    CleanupPending { code: String },
}

impl RemoteUncertainty {
    pub fn unknown_remote(code: impl Into<String>) -> Result<Self, WorkerError> {
        let uncertainty = Self::UnknownRemote { code: code.into() };
        uncertainty.validate()?;
        Ok(uncertainty)
    }

    pub fn cleanup_pending(code: impl Into<String>) -> Result<Self, WorkerError> {
        let uncertainty = Self::CleanupPending { code: code.into() };
        uncertainty.validate()?;
        Ok(uncertainty)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::None => Ok(()),
            Self::UnknownRemote { code } | Self::CleanupPending { code } => {
                validate_control_code(code, "remote uncertainty code")
            }
        }
    }

    pub fn code(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::UnknownRemote { code } | Self::CleanupPending { code } => Some(code),
        }
    }
}

impl Serialize for RemoteUncertainty {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        #[derive(Serialize)]
        #[serde(tag = "state", rename_all = "snake_case")]
        enum Wire<'a> {
            None,
            UnknownRemote { code: &'a str },
            CleanupPending { code: &'a str },
        }
        match self {
            Self::None => Wire::None,
            Self::UnknownRemote { code } => Wire::UnknownRemote { code },
            Self::CleanupPending { code } => Wire::CleanupPending { code },
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RemoteUncertainty {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            None {},
            UnknownRemote { code: String },
            CleanupPending { code: String },
        }
        let uncertainty = match Wire::deserialize(deserializer)? {
            Wire::None {} => Self::None,
            Wire::UnknownRemote { code } => Self::UnknownRemote { code },
            Wire::CleanupPending { code } => Self::CleanupPending { code },
        };
        uncertainty.validate().map_err(de::Error::custom)?;
        Ok(uncertainty)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalJobRecord {
    meta: JobMeta,
    lease_token: LeaseToken,
    last_status: Option<JobStatus>,
    remote_uncertainty: RemoteUncertainty,
}

impl Serialize for LocalJobRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LocalJobRecord", 4)?;
        record.serialize_field("meta", &self.meta)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("last_status", &self.last_status)?;
        record.serialize_field("remote_uncertainty", &self.remote_uncertainty)?;
        record.end()
    }
}

impl LocalJobRecord {
    pub fn new(
        meta: JobMeta,
        lease_token: LeaseToken,
        last_status: Option<JobStatus>,
        remote_uncertainty: RemoteUncertainty,
    ) -> Result<Self, WorkerError> {
        let record = Self {
            meta,
            lease_token,
            last_status,
            remote_uncertainty,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        if let Some(status) = &self.last_status {
            status.validate()?;
        }
        self.remote_uncertainty.validate()
    }

    pub fn meta(&self) -> &JobMeta {
        &self.meta
    }
    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }
    pub fn last_status(&self) -> Option<&JobStatus> {
        self.last_status.as_ref()
    }
    pub fn remote_uncertainty(&self) -> &RemoteUncertainty {
        &self.remote_uncertainty
    }
}

impl<'de> Deserialize<'de> for LocalJobRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            meta: JobMeta,
            lease_token: LeaseToken,
            last_status: Option<JobStatus>,
            remote_uncertainty: RemoteUncertainty,
        }
        let wire = Wire::deserialize(deserializer)?;
        let record = Self {
            meta: wire.meta,
            lease_token: wire.lease_token,
            last_status: wire.last_status,
            remote_uncertainty: wire.remote_uncertainty,
        };
        record.validate().map_err(de::Error::custom)?;
        Ok(record)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    request_fingerprint: RequestFingerprint,
    worker_name: String,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    timeout_millis: u64,
    resource_class: String,
    command_summary: CommandSummary,
    created_at_millis: u64,
    expires_at_millis: u64,
}

impl Serialize for LeaseRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LeaseRecord", 13)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("worker_name", &self.worker_name)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("timeout_millis", &self.timeout_millis)?;
        record.serialize_field("resource_class", &self.resource_class)?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.serialize_field("expires_at_millis", &self.expires_at_millis)?;
        record.end()
    }
}

impl LeaseRecord {
    pub fn new(
        material: &RequestFingerprintMaterial,
        request_fingerprint: RequestFingerprint,
        created_at_millis: u64,
        expires_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        if material.fingerprint() != request_fingerprint {
            return Err(protocol_error(
                "lease fingerprint does not match its material",
            ));
        }
        let lease = Self {
            job_id: material.job_id,
            client_id: material.client_id,
            lease_token: material.lease_token,
            request_fingerprint,
            worker_name: material.worker_name.clone(),
            project_id: material.project_id.clone(),
            worktree_id: material.worktree_id.clone(),
            manifest_digest: material.manifest_digest.clone(),
            timeout_millis: material.timeout_millis,
            resource_class: material.resource_class.clone(),
            command_summary: material.command.summary()?,
            created_at_millis,
            expires_at_millis,
        };
        lease.validate()?;
        Ok(lease)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        validate_hex_component(&self.manifest_digest, "manifest digest")?;
        validate_non_nul(&self.worker_name, 128, "worker name")?;
        validate_non_nul(&self.resource_class, 64, "resource class")?;
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

    pub fn job_id(&self) -> JobId {
        self.job_id
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }
    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
    pub fn worker_name(&self) -> &str {
        &self.worker_name
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
    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }
    pub fn resource_class(&self) -> &str {
        &self.resource_class
    }
    pub fn command_summary(&self) -> &CommandSummary {
        &self.command_summary
    }
    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
    pub fn expires_at_millis(&self) -> u64 {
        self.expires_at_millis
    }
}

impl<'de> Deserialize<'de> for LeaseRecord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            job_id: JobId,
            client_id: ClientId,
            lease_token: LeaseToken,
            request_fingerprint: RequestFingerprint,
            worker_name: String,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            timeout_millis: u64,
            resource_class: String,
            command_summary: CommandSummary,
            created_at_millis: u64,
            expires_at_millis: u64,
        }
        let wire = Wire::deserialize(deserializer)?;
        let lease = Self {
            job_id: wire.job_id,
            client_id: wire.client_id,
            lease_token: wire.lease_token,
            request_fingerprint: wire.request_fingerprint,
            worker_name: wire.worker_name,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
            timeout_millis: wire.timeout_millis,
            resource_class: wire.resource_class,
            command_summary: wire.command_summary,
            created_at_millis: wire.created_at_millis,
            expires_at_millis: wire.expires_at_millis,
        };
        lease.validate().map_err(de::Error::custom)?;
        Ok(lease)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LeaseAcquireRequest {
    material: RequestFingerprintMaterial,
    request_fingerprint: RequestFingerprint,
}

impl fmt::Debug for LeaseAcquireRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseAcquireRequest")
            .field("job_id", &self.material.job_id())
            .field("client_id", &self.material.client_id())
            .field("request_fingerprint", &self.request_fingerprint)
            .finish_non_exhaustive()
    }
}

impl Serialize for LeaseAcquireRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LeaseAcquireRequest", 2)?;
        record.serialize_field("material", &self.material)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.end()
    }
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

    pub fn material(&self) -> &RequestFingerprintMaterial {
        &self.material
    }
    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
}

impl<'de> Deserialize<'de> for LeaseAcquireRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            material: RequestFingerprintMaterial,
            request_fingerprint: RequestFingerprint,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            material: wire.material,
            request_fingerprint: wire.request_fingerprint,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseAcquireResponse {
    Acquired { lease: LeaseRecord },
    ExistingAccepted { status: JobStatus },
}

impl LeaseAcquireResponse {
    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Acquired { lease } => lease.validate(),
            Self::ExistingAccepted { status } => status.validate(),
        }
    }
}

impl Serialize for LeaseAcquireResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        #[derive(Serialize)]
        #[serde(tag = "outcome", rename_all = "snake_case")]
        enum Wire<'a> {
            Acquired { lease: &'a LeaseRecord },
            ExistingAccepted { status: &'a JobStatus },
        }
        match self {
            Self::Acquired { lease } => Wire::Acquired { lease }.serialize(serializer),
            Self::ExistingAccepted { status } => {
                Wire::ExistingAccepted { status }.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for LeaseAcquireResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Acquired { lease: LeaseRecord },
            ExistingAccepted { status: JobStatus },
        }
        let response = match Wire::deserialize(deserializer)? {
            Wire::Acquired { lease } => Self::Acquired { lease },
            Wire::ExistingAccepted { status } => Self::ExistingAccepted { status },
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SubmitRequest {
    material: RequestFingerprintMaterial,
    request_fingerprint: RequestFingerprint,
}

impl fmt::Debug for SubmitRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubmitRequest")
            .field("job_id", &self.material.job_id())
            .field("client_id", &self.material.client_id())
            .field("request_fingerprint", &self.request_fingerprint)
            .finish_non_exhaustive()
    }
}

impl Serialize for SubmitRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("SubmitRequest", 2)?;
        record.serialize_field("material", &self.material)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.end()
    }
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

    pub fn material(&self) -> &RequestFingerprintMaterial {
        &self.material
    }
    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
}

impl<'de> Deserialize<'de> for SubmitRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            material: RequestFingerprintMaterial,
            request_fingerprint: RequestFingerprint,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            material: wire.material,
            request_fingerprint: wire.request_fingerprint,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ResolveOrAbandonRequest {
    protocol_version: u32,
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    created_at_millis: u64,
    request_fingerprint: RequestFingerprint,
    worker_name: String,
    project_id: String,
    worktree_id: String,
    manifest_digest: String,
    relative_working_dir: String,
    timeout_millis: u64,
    resource_class: String,
    command_summary: CommandSummary,
}

impl fmt::Debug for ResolveOrAbandonRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolveOrAbandonRequest")
            .field("protocol_version", &self.protocol_version)
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("lease_token", &"[REDACTED]")
            .field("created_at_millis", &self.created_at_millis)
            .field("request_fingerprint", &self.request_fingerprint)
            .field("worker_name", &self.worker_name)
            .field("project_id", &self.project_id)
            .field("worktree_id", &self.worktree_id)
            .field("manifest_digest", &self.manifest_digest)
            .field("relative_working_dir", &self.relative_working_dir)
            .field("timeout_millis", &self.timeout_millis)
            .field("resource_class", &self.resource_class)
            .field("command_summary", &self.command_summary)
            .finish()
    }
}

impl ResolveOrAbandonRequest {
    pub fn from_submit_request(request: &SubmitRequest) -> Result<Self, WorkerError> {
        request.validate()?;
        let material = request.material();
        Self::from_parts(
            material.job_id(),
            material.client_id(),
            material.lease_token(),
            material.created_at_millis(),
            request.request_fingerprint().clone(),
            material.worker_name().into(),
            material.project_id().into(),
            material.worktree_id().into(),
            material.manifest_digest().into(),
            material.relative_working_dir().into(),
            material.timeout_millis(),
            material.resource_class().into(),
            material.command().summary()?,
        )
    }

    pub fn from_local_record(record: &LocalJobRecord) -> Result<Self, WorkerError> {
        record.validate()?;
        let meta = record.meta();
        Self::from_parts(
            meta.job_id(),
            meta.client_id(),
            record.lease_token(),
            meta.created_at_millis(),
            meta.request_fingerprint().clone(),
            meta.worker_name().into(),
            meta.project_id().into(),
            meta.worktree_id().into(),
            meta.manifest_digest().into(),
            meta.relative_working_dir().into(),
            meta.timeout_millis(),
            meta.resource_class().into(),
            meta.command_summary().clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        created_at_millis: u64,
        request_fingerprint: RequestFingerprint,
        worker_name: String,
        project_id: String,
        worktree_id: String,
        manifest_digest: String,
        relative_working_dir: String,
        timeout_millis: u64,
        resource_class: String,
        command_summary: CommandSummary,
    ) -> Result<Self, WorkerError> {
        let request = Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            client_id,
            lease_token,
            created_at_millis,
            request_fingerprint,
            worker_name,
            project_id,
            worktree_id,
            manifest_digest,
            relative_working_dir,
            timeout_millis,
            resource_class,
            command_summary,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "resolve-or-abandon request has an incompatible protocol version",
            ));
        }
        validate_non_nul(&self.worker_name, 128, "worker name")?;
        validate_hex_component(&self.project_id, "project ID")?;
        validate_hex_component(&self.worktree_id, "worktree ID")?;
        validate_hex_component(&self.manifest_digest, "manifest digest")?;
        if self.relative_working_dir.as_bytes().contains(&0)
            || self.relative_working_dir.len() > MAX_COMMAND_BYTES
        {
            return Err(protocol_error(
                "resolve-or-abandon relative working directory is invalid",
            ));
        }
        if self.timeout_millis == 0 || self.timeout_millis > MAX_TIMEOUT_MILLIS {
            return Err(protocol_error(
                "resolve-or-abandon timeout is outside the supported range",
            ));
        }
        validate_non_nul(&self.resource_class, 64, "resource class")?;
        self.command_summary.validate()
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }
    pub fn job_id(&self) -> JobId {
        self.job_id
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }
    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
    pub fn worker_name(&self) -> &str {
        &self.worker_name
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
    pub fn relative_working_dir(&self) -> &str {
        &self.relative_working_dir
    }
    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }
    pub fn resource_class(&self) -> &str {
        &self.resource_class
    }
    pub fn command_summary(&self) -> &CommandSummary {
        &self.command_summary
    }
}

impl TryFrom<&SubmitRequest> for ResolveOrAbandonRequest {
    type Error = WorkerError;

    fn try_from(request: &SubmitRequest) -> Result<Self, Self::Error> {
        Self::from_submit_request(request)
    }
}

impl TryFrom<&LocalJobRecord> for ResolveOrAbandonRequest {
    type Error = WorkerError;

    fn try_from(record: &LocalJobRecord) -> Result<Self, Self::Error> {
        Self::from_local_record(record)
    }
}

impl Serialize for ResolveOrAbandonRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("ResolveOrAbandonRequest", 14)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("created_at_millis", &self.created_at_millis)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.serialize_field("worker_name", &self.worker_name)?;
        record.serialize_field("project_id", &self.project_id)?;
        record.serialize_field("worktree_id", &self.worktree_id)?;
        record.serialize_field("manifest_digest", &self.manifest_digest)?;
        record.serialize_field("relative_working_dir", &self.relative_working_dir)?;
        record.serialize_field("timeout_millis", &self.timeout_millis)?;
        record.serialize_field("resource_class", &self.resource_class)?;
        record.serialize_field("command_summary", &self.command_summary)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for ResolveOrAbandonRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
            client_id: ClientId,
            lease_token: LeaseToken,
            created_at_millis: u64,
            request_fingerprint: RequestFingerprint,
            worker_name: String,
            project_id: String,
            worktree_id: String,
            manifest_digest: String,
            relative_working_dir: String,
            timeout_millis: u64,
            resource_class: String,
            command_summary: CommandSummary,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
            client_id: wire.client_id,
            lease_token: wire.lease_token,
            created_at_millis: wire.created_at_millis,
            request_fingerprint: wire.request_fingerprint,
            worker_name: wire.worker_name,
            project_id: wire.project_id,
            worktree_id: wire.worktree_id,
            manifest_digest: wire.manifest_digest,
            relative_working_dir: wire.relative_working_dir,
            timeout_millis: wire.timeout_millis,
            resource_class: wire.resource_class,
            command_summary: wire.command_summary,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitResponse {
    Accepted {
        meta: Box<JobMeta>,
        status: JobStatus,
    },
    Existing {
        status: JobStatus,
    },
}

impl SubmitResponse {
    pub fn validate(&self) -> Result<(), WorkerError> {
        response_status_validate(self)
    }

    pub fn status(&self) -> &JobStatus {
        match self {
            Self::Accepted { status, .. } | Self::Existing { status } => status,
        }
    }
}

impl Serialize for SubmitResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        #[derive(Serialize)]
        #[serde(tag = "outcome", rename_all = "snake_case")]
        enum Wire<'a> {
            Accepted {
                meta: &'a JobMeta,
                status: &'a JobStatus,
            },
            Existing {
                status: &'a JobStatus,
            },
        }
        match self {
            Self::Accepted { meta, status } => {
                Wire::Accepted { meta, status }.serialize(serializer)
            }
            Self::Existing { status } => Wire::Existing { status }.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for SubmitResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Accepted {
                meta: Box<JobMeta>,
                status: JobStatus,
            },
            Existing {
                status: JobStatus,
            },
        }
        let response = match Wire::deserialize(deserializer)? {
            Wire::Accepted { meta, status } => Self::Accepted { meta, status },
            Wire::Existing { status } => Self::Existing { status },
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusRequest {
    protocol_version: u32,
    job_id: JobId,
}

impl StatusRequest {
    pub fn new(job_id: JobId) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "status request has an incompatible protocol version",
            ));
        }
        Ok(())
    }

    pub fn protocol_version(self) -> u32 {
        self.protocol_version
    }

    pub fn job_id(self) -> JobId {
        self.job_id
    }
}

impl Serialize for StatusRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("StatusRequest", 2)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for StatusRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusResponse {
    protocol_version: u32,
    meta: JobMeta,
    status: JobStatus,
}

impl Serialize for StatusResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("StatusResponse", 3)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("meta", &self.meta)?;
        record.serialize_field("status", &self.status)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for StatusResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            meta: JobMeta,
            status: JobStatus,
        }
        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            protocol_version: wire.protocol_version,
            meta: wire.meta,
            status: wire.status,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

impl StatusResponse {
    pub fn new(meta: JobMeta, status: JobStatus) -> Result<Self, WorkerError> {
        let response = Self {
            protocol_version: PROTOCOL_VERSION,
            meta,
            status,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "status response has an incompatible protocol version",
            ));
        }
        self.meta.validate()?;
        self.status.validate()?;
        if self.status.updated_at_millis() < self.meta.created_at_millis() {
            return Err(protocol_error(
                "status response timestamp predates job acceptance",
            ));
        }
        Ok(())
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn meta(&self) -> &JobMeta {
        &self.meta
    }
    pub fn status(&self) -> &JobStatus {
        &self.status
    }
}

pub const MAX_FLEET_RECONCILE_IDS: usize = 100;

/// Fixed, bounded host request.  It is deliberately only a set of known job
/// IDs: no path, namespace, or worker-wide discovery input is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetReconcileRequest {
    pub known_job_ids: Vec<JobId>,
}

impl FleetReconcileRequest {
    pub fn new(known_job_ids: Vec<JobId>) -> Result<Self, WorkerError> {
        let request = Self { known_job_ids };
        request.validate()?;
        Ok(request)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.known_job_ids.len() > MAX_FLEET_RECONCILE_IDS {
            return Err(protocol_error(
                "fleet reconciliation request exceeds 100 job IDs",
            ));
        }
        for (index, job_id) in self.known_job_ids.iter().enumerate() {
            if self.known_job_ids[..index].contains(job_id) {
                return Err(protocol_error(
                    "fleet reconciliation request contains duplicate job IDs",
                ));
            }
        }
        Ok(())
    }

    pub fn known_job_ids(&self) -> &[JobId] {
        &self.known_job_ids
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum FleetReconcileJobResult {
    Status {
        status: Box<StatusResponse>,
    },
    Error {
        job_id: JobId,
        error: HostControlError,
    },
}

impl FleetReconcileJobResult {
    pub fn job_id(&self) -> JobId {
        match self {
            Self::Status { status } => status.meta().job_id(),
            Self::Error { job_id, .. } => *job_id,
        }
    }

    pub fn status(&self) -> Option<&StatusResponse> {
        match self {
            Self::Status { status } => Some(status),
            Self::Error { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&HostControlError> {
        match self {
            Self::Status { .. } => None,
            Self::Error { error, .. } => Some(error),
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Status { status } => status.validate(),
            Self::Error { error, .. } => error.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetReconcileResponse {
    pub results: Vec<FleetReconcileJobResult>,
}

impl FleetReconcileResponse {
    pub fn new(results: Vec<FleetReconcileJobResult>) -> Result<Self, WorkerError> {
        let response = Self { results };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.results.len() > MAX_FLEET_RECONCILE_IDS {
            return Err(protocol_error(
                "fleet reconciliation response exceeds 100 job IDs",
            ));
        }
        for (index, result) in self.results.iter().enumerate() {
            result.validate()?;
            if self.results[..index]
                .iter()
                .any(|prior| prior.job_id() == result.job_id())
            {
                return Err(protocol_error(
                    "fleet reconciliation response contains duplicate job IDs",
                ));
            }
        }
        Ok(())
    }

    pub fn results(&self) -> &[FleetReconcileJobResult] {
        &self.results
    }
}

/// Fixed-operation request for cancelling exactly one accepted job.  Unlike
/// the general request envelopes it intentionally has no protocol-version
/// field: the operation is bound by the immutable accepted identity, while
/// its response carries the wire version.
#[derive(Clone, PartialEq, Eq)]
pub struct CancelRequest {
    job_id: JobId,
    client_id: ClientId,
    lease_token: LeaseToken,
    request_fingerprint: RequestFingerprint,
}

impl fmt::Debug for CancelRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancelRequest")
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
            .field("lease_token", &"[REDACTED]")
            .field("request_fingerprint", &"[REDACTED]")
            .finish()
    }
}

impl CancelRequest {
    pub fn new(
        job_id: JobId,
        client_id: ClientId,
        lease_token: LeaseToken,
        request_fingerprint: RequestFingerprint,
    ) -> Result<Self, WorkerError> {
        let request = Self {
            job_id,
            client_id,
            lease_token,
            request_fingerprint,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn from_local_record(record: &LocalJobRecord) -> Result<Self, WorkerError> {
        record.validate()?;
        Self::new(
            record.meta().job_id(),
            record.meta().client_id(),
            record.lease_token(),
            record.meta().request_fingerprint().clone(),
        )
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        // The constituent identifier types validate during construction and
        // deserialization. Keep this method so every fixed-operation record
        // has the same explicit validation boundary.
        let _ = self;
        Ok(())
    }

    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
    }

    pub fn request_fingerprint(&self) -> &RequestFingerprint {
        &self.request_fingerprint
    }
}

impl Serialize for CancelRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("CancelRequest", 4)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("client_id", &self.client_id)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("request_fingerprint", &self.request_fingerprint)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for CancelRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            job_id: JobId,
            client_id: ClientId,
            lease_token: LeaseToken,
            request_fingerprint: RequestFingerprint,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            job_id: wire.job_id,
            client_id: wire.client_id,
            lease_token: wire.lease_token,
            request_fingerprint: wire.request_fingerprint,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelResponse {
    protocol_version: u32,
    status: StatusResponse,
}

impl CancelResponse {
    pub fn new(status: StatusResponse) -> Result<Self, WorkerError> {
        let response = Self {
            protocol_version: PROTOCOL_VERSION,
            status,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "cancel response has an incompatible protocol version",
            ));
        }
        self.status.validate()
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    pub fn status(&self) -> &StatusResponse {
        &self.status
    }
}

impl Serialize for CancelResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("CancelResponse", 2)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("status", &self.status)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for CancelResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            status: StatusResponse,
        }
        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            protocol_version: wire.protocol_version,
            status: wire.status,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogChunkRequest {
    protocol_version: u32,
    job_id: JobId,
    stream: LogStream,
    offset: u64,
    limit: u32,
}

impl LogChunkRequest {
    pub fn new(job_id: JobId, stream: LogStream, offset: u64, limit: u32) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            stream,
            offset,
            limit,
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "log chunk request has an incompatible protocol version",
            ));
        }
        Ok(())
    }

    pub fn protocol_version(self) -> u32 {
        self.protocol_version
    }
    pub fn job_id(self) -> JobId {
        self.job_id
    }
    pub fn stream(self) -> LogStream {
        self.stream
    }
    pub fn offset(self) -> u64 {
        self.offset
    }
    pub fn limit(self) -> u32 {
        self.limit
    }
}

impl Serialize for LogChunkRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LogChunkRequest", 5)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("stream", &self.stream)?;
        record.serialize_field("offset", &self.offset)?;
        record.serialize_field("limit", &self.limit)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for LogChunkRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
            stream: LogStream,
            offset: u64,
            limit: u32,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
            stream: wire.stream,
            offset: wire.offset,
            limit: wire.limit,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogChunk {
    stream: LogStream,
    offset: u64,
    next_offset: u64,
    data: String,
}

impl Serialize for LogChunk {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LogChunk", 4)?;
        record.serialize_field("stream", &self.stream)?;
        record.serialize_field("offset", &self.offset)?;
        record.serialize_field("next_offset", &self.next_offset)?;
        record.serialize_field("data", &self.data)?;
        record.end()
    }
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

    pub fn stream(&self) -> LogStream {
        self.stream
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn data(&self) -> &str {
        &self.data
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogCursor {
    stream: LogStream,
    next_offset: u64,
    limit: u32,
    terminal_target: Option<u64>,
    eof_confirmed: bool,
}

impl LogCursor {
    pub fn new(stream: LogStream, next_offset: u64, limit: u32) -> Result<Self, WorkerError> {
        let cursor = Self {
            stream,
            next_offset,
            limit,
            terminal_target: None,
            eof_confirmed: false,
        };
        cursor.validate()?;
        Ok(cursor)
    }

    fn validate(&self) -> Result<(), WorkerError> {
        if self
            .terminal_target
            .is_some_and(|target| target < self.next_offset)
            || (self.eof_confirmed && self.terminal_target != Some(self.next_offset))
        {
            return Err(protocol_error("log cursor state is inconsistent"));
        }
        Ok(())
    }

    pub fn request(&self, job_id: JobId) -> LogChunkRequest {
        LogChunkRequest::new(job_id, self.stream, self.next_offset, self.limit)
    }

    pub fn observe_chunk(&mut self, chunk: &LogChunk) -> Result<(), WorkerError> {
        chunk.validate()?;
        if chunk.stream() != self.stream {
            return Err(protocol_error("log chunk stream does not match cursor"));
        }
        if chunk.offset() != self.next_offset {
            return Err(protocol_error("log chunk offset does not match cursor"));
        }
        if self
            .terminal_target
            .is_some_and(|target| chunk.next_offset() > target)
        {
            return Err(protocol_error("log chunk crosses terminal byte length"));
        }
        let empty = chunk.next_offset() == chunk.offset();
        self.next_offset = chunk.next_offset();
        self.eof_confirmed = empty && self.terminal_target == Some(self.next_offset);
        self.validate()
    }

    pub fn set_terminal_target(&mut self, bytes: u64) -> Result<(), WorkerError> {
        if bytes < self.next_offset {
            return Err(protocol_error(
                "terminal log byte length precedes the cursor",
            ));
        }
        if self.terminal_target.is_some_and(|current| current != bytes) {
            self.eof_confirmed = false;
            return Err(protocol_error("terminal log byte length changed"));
        }
        self.terminal_target = Some(bytes);
        self.eof_confirmed = false;
        self.validate()
    }

    pub fn stream(&self) -> LogStream {
        self.stream
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    pub fn terminal_target(&self) -> Option<u64> {
        self.terminal_target
    }

    pub fn is_drained(&self) -> bool {
        self.eof_confirmed && self.terminal_target == Some(self.next_offset)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalLogDrain {
    stdout: LogCursor,
    stderr: LogCursor,
    terminal: Option<StatusResponse>,
    status_revalidated: bool,
}

impl TerminalLogDrain {
    pub fn new(stdout: LogCursor, stderr: LogCursor) -> Result<Self, WorkerError> {
        stdout.validate()?;
        stderr.validate()?;
        if stdout.stream() != LogStream::Stdout || stderr.stream() != LogStream::Stderr {
            return Err(protocol_error(
                "terminal log drain requires stdout and stderr cursors",
            ));
        }
        Ok(Self {
            stdout,
            stderr,
            terminal: None,
            status_revalidated: false,
        })
    }

    pub fn cursor(&self, stream: LogStream) -> &LogCursor {
        match stream {
            LogStream::Stdout => &self.stdout,
            LogStream::Stderr => &self.stderr,
        }
    }

    pub fn observe_chunk(&mut self, chunk: &LogChunk) -> Result<(), WorkerError> {
        match chunk.stream() {
            LogStream::Stdout => self.stdout.observe_chunk(chunk)?,
            LogStream::Stderr => self.stderr.observe_chunk(chunk)?,
        }
        self.status_revalidated = false;
        Ok(())
    }

    pub fn set_terminal_status(&mut self, response: &StatusResponse) -> Result<(), WorkerError> {
        response.validate()?;
        if !response.status().state().is_terminal() {
            return Err(protocol_error("log drain status is not terminal"));
        }
        if self
            .terminal
            .as_ref()
            .is_some_and(|current| current != response)
        {
            self.status_revalidated = false;
            return Err(protocol_error("terminal log status changed"));
        }
        let stdout_target = response
            .status()
            .final_stdout_bytes()
            .expect("validated terminal status has stdout bytes");
        let stderr_target = response
            .status()
            .final_stderr_bytes()
            .expect("validated terminal status has stderr bytes");
        let mut stdout = self.stdout.clone();
        let mut stderr = self.stderr.clone();
        stdout.set_terminal_target(stdout_target)?;
        stderr.set_terminal_target(stderr_target)?;
        self.stdout = stdout;
        self.stderr = stderr;
        self.terminal = Some(response.clone());
        self.status_revalidated = false;
        Ok(())
    }

    pub fn revalidate_terminal_status(
        &mut self,
        response: &StatusResponse,
    ) -> Result<(), WorkerError> {
        self.status_revalidated = false;
        if !self.stdout.is_drained() || !self.stderr.is_drained() {
            return Err(protocol_error(
                "terminal logs are not both confirmed at EOF",
            ));
        }
        response.validate()?;
        let expected = self
            .terminal
            .as_ref()
            .ok_or_else(|| protocol_error("terminal log status is absent"))?;
        if expected != response {
            return Err(protocol_error("terminal log status changed"));
        }
        self.status_revalidated = true;
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.stdout.is_drained() && self.stderr.is_drained() && self.status_revalidated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogChunkResponse {
    protocol_version: u32,
    chunk: LogChunk,
}

impl LogChunkResponse {
    pub fn new(chunk: LogChunk) -> Result<Self, WorkerError> {
        let response = Self {
            protocol_version: PROTOCOL_VERSION,
            chunk,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "log chunk response has an incompatible protocol version",
            ));
        }
        self.chunk.validate()
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }
    pub fn chunk(&self) -> &LogChunk {
        &self.chunk
    }
    pub fn into_chunk(self) -> LogChunk {
        self.chunk
    }
}

impl Serialize for LogChunkResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LogChunkResponse", 2)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("chunk", &self.chunk)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for LogChunkResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            chunk: LogChunk,
        }
        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            protocol_version: wire.protocol_version,
            chunk: wire.chunk,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusLogsRequest {
    protocol_version: u32,
    job_id: JobId,
    stdout_offset: u64,
    stdout_limit: u32,
    stderr_offset: u64,
    stderr_limit: u32,
}

impl StatusLogsRequest {
    pub fn new(
        job_id: JobId,
        stdout_offset: u64,
        stdout_limit: u32,
        stderr_offset: u64,
        stderr_limit: u32,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            job_id,
            stdout_offset,
            stdout_limit,
            stderr_offset,
            stderr_limit,
        }
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "status-logs request has an incompatible protocol version",
            ));
        }
        Ok(())
    }

    pub fn protocol_version(self) -> u32 {
        self.protocol_version
    }
    pub fn job_id(self) -> JobId {
        self.job_id
    }
    pub fn stdout_offset(self) -> u64 {
        self.stdout_offset
    }
    pub fn stdout_limit(self) -> u32 {
        self.stdout_limit
    }
    pub fn stderr_offset(self) -> u64 {
        self.stderr_offset
    }
    pub fn stderr_limit(self) -> u32 {
        self.stderr_limit
    }
}

impl Serialize for StatusLogsRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("StatusLogsRequest", 6)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("job_id", &self.job_id)?;
        record.serialize_field("stdout_offset", &self.stdout_offset)?;
        record.serialize_field("stdout_limit", &self.stdout_limit)?;
        record.serialize_field("stderr_offset", &self.stderr_offset)?;
        record.serialize_field("stderr_limit", &self.stderr_limit)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for StatusLogsRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            job_id: JobId,
            stdout_offset: u64,
            stdout_limit: u32,
            stderr_offset: u64,
            stderr_limit: u32,
        }
        let wire = Wire::deserialize(deserializer)?;
        let request = Self {
            protocol_version: wire.protocol_version,
            job_id: wire.job_id,
            stdout_offset: wire.stdout_offset,
            stdout_limit: wire.stdout_limit,
            stderr_offset: wire.stderr_offset,
            stderr_limit: wire.stderr_limit,
        };
        request.validate().map_err(de::Error::custom)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusLogsResponse {
    protocol_version: u32,
    status: StatusResponse,
    stdout: LogChunk,
    stderr: LogChunk,
}

impl StatusLogsResponse {
    pub fn new(
        status: StatusResponse,
        stdout: LogChunk,
        stderr: LogChunk,
    ) -> Result<Self, WorkerError> {
        let response = Self {
            protocol_version: PROTOCOL_VERSION,
            status,
            stdout,
            stderr,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "status-logs response has an incompatible protocol version",
            ));
        }
        self.status.validate()?;
        self.stdout.validate()?;
        self.stderr.validate()?;
        if self.stdout.stream() != LogStream::Stdout || self.stderr.stream() != LogStream::Stderr {
            return Err(protocol_error(
                "status-logs chunks do not match stdout and stderr",
            ));
        }
        Ok(())
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }
    pub fn status(&self) -> &StatusResponse {
        &self.status
    }
    pub fn stdout(&self) -> &LogChunk {
        &self.stdout
    }
    pub fn stderr(&self) -> &LogChunk {
        &self.stderr
    }
    pub fn into_parts(self) -> (StatusResponse, LogChunk, LogChunk) {
        (self.status, self.stdout, self.stderr)
    }
}

impl Serialize for StatusLogsResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("StatusLogsResponse", 4)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("status", &self.status)?;
        record.serialize_field("stdout", &self.stdout)?;
        record.serialize_field("stderr", &self.stderr)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for StatusLogsResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            status: StatusResponse,
            stdout: LogChunk,
            stderr: LogChunk,
        }
        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            protocol_version: wire.protocol_version,
            status: wire.status,
            stdout: wire.stdout,
            stderr: wire.stderr,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
// The control-plane contract owns the complete accepted response so callers
// can pattern-match it without a second allocation or lifetime wrapper.
#[allow(clippy::large_enum_variant)]
pub enum ResolveOrAbandonOutcome {
    Accepted { response: StatusResponse },
    Abandoned,
    CleanupPending { code: String },
}

impl ResolveOrAbandonOutcome {
    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Accepted { response } => response.validate(),
            Self::Abandoned => Ok(()),
            Self::CleanupPending { code } => validate_control_code(code, "cleanup-pending code"),
        }
    }
}

impl Serialize for ResolveOrAbandonOutcome {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        match self {
            Self::Accepted { response } => {
                let mut record = serializer.serialize_struct("ResolveOrAbandonOutcome", 2)?;
                record.serialize_field("outcome", "accepted")?;
                record.serialize_field("response", response)?;
                record.end()
            }
            Self::Abandoned => {
                let mut record = serializer.serialize_struct("ResolveOrAbandonOutcome", 1)?;
                record.serialize_field("outcome", "abandoned")?;
                record.end()
            }
            Self::CleanupPending { code } => {
                let mut record = serializer.serialize_struct("ResolveOrAbandonOutcome", 2)?;
                record.serialize_field("outcome", "cleanup_pending")?;
                record.serialize_field("code", code)?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ResolveOrAbandonOutcome {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Accepted { response: Box<StatusResponse> },
            Abandoned {},
            CleanupPending { code: String },
        }
        let outcome = match Wire::deserialize(deserializer)? {
            Wire::Accepted { response } => Self::Accepted {
                response: *response,
            },
            Wire::Abandoned {} => Self::Abandoned,
            Wire::CleanupPending { code } => Self::CleanupPending { code },
        };
        outcome.validate().map_err(de::Error::custom)?;
        Ok(outcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveOrAbandonResponse {
    protocol_version: u32,
    outcome: ResolveOrAbandonOutcome,
}

impl ResolveOrAbandonResponse {
    pub fn new(outcome: ResolveOrAbandonOutcome) -> Result<Self, WorkerError> {
        let response = Self {
            protocol_version: PROTOCOL_VERSION,
            outcome,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn accepted(response: StatusResponse) -> Result<Self, WorkerError> {
        Self::new(ResolveOrAbandonOutcome::Accepted { response })
    }

    pub fn abandoned() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            outcome: ResolveOrAbandonOutcome::Abandoned,
        }
    }

    pub fn cleanup_pending(code: impl Into<String>) -> Result<Self, WorkerError> {
        Self::new(ResolveOrAbandonOutcome::CleanupPending { code: code.into() })
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "resolve-or-abandon response has an incompatible protocol version",
            ));
        }
        self.outcome.validate()
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }
    pub fn outcome(&self) -> &ResolveOrAbandonOutcome {
        &self.outcome
    }
}

impl Serialize for ResolveOrAbandonResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        match &self.outcome {
            ResolveOrAbandonOutcome::Accepted { response } => {
                let mut record = serializer.serialize_struct("ResolveOrAbandonResponse", 3)?;
                record.serialize_field("protocol_version", &self.protocol_version)?;
                record.serialize_field("outcome", "accepted")?;
                record.serialize_field("response", response)?;
                record.end()
            }
            ResolveOrAbandonOutcome::Abandoned => {
                let mut record = serializer.serialize_struct("ResolveOrAbandonResponse", 2)?;
                record.serialize_field("protocol_version", &self.protocol_version)?;
                record.serialize_field("outcome", "abandoned")?;
                record.end()
            }
            ResolveOrAbandonOutcome::CleanupPending { code } => {
                let mut record = serializer.serialize_struct("ResolveOrAbandonResponse", 3)?;
                record.serialize_field("protocol_version", &self.protocol_version)?;
                record.serialize_field("outcome", "cleanup_pending")?;
                record.serialize_field("code", code)?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ResolveOrAbandonResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Accepted {
                protocol_version: u32,
                response: Box<StatusResponse>,
            },
            Abandoned {
                protocol_version: u32,
            },
            CleanupPending {
                protocol_version: u32,
                code: String,
            },
        }
        let (protocol_version, outcome) = match Wire::deserialize(deserializer)? {
            Wire::Accepted {
                protocol_version,
                response,
            } => (
                protocol_version,
                ResolveOrAbandonOutcome::Accepted {
                    response: *response,
                },
            ),
            Wire::Abandoned { protocol_version } => {
                (protocol_version, ResolveOrAbandonOutcome::Abandoned)
            }
            Wire::CleanupPending {
                protocol_version,
                code,
            } => (
                protocol_version,
                ResolveOrAbandonOutcome::CleanupPending { code },
            ),
        };
        let response = Self {
            protocol_version,
            outcome,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
// This low-frequency recovery result deliberately matches the public contract.
#[allow(clippy::large_enum_variant)]
pub enum PreacceptanceDisposition {
    Accepted(StatusResponse),
    Abandoned,
    CleanupPending { code: String },
    UnknownRemote { code: String },
}

impl PreacceptanceDisposition {
    pub fn cleanup_pending(code: impl Into<String>) -> Result<Self, WorkerError> {
        let disposition = Self::CleanupPending { code: code.into() };
        disposition.validate()?;
        Ok(disposition)
    }

    pub fn unknown_remote(code: impl Into<String>) -> Result<Self, WorkerError> {
        let disposition = Self::UnknownRemote { code: code.into() };
        disposition.validate()?;
        Ok(disposition)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        match self {
            Self::Accepted(response) => response.validate(),
            Self::Abandoned => Ok(()),
            Self::CleanupPending { code } => validate_control_code(code, "cleanup-pending code"),
            Self::UnknownRemote { code } => validate_control_code(code, "unknown-remote code"),
        }
    }
}

impl Serialize for PreacceptanceDisposition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        match self {
            Self::Accepted(response) => {
                let mut record = serializer.serialize_struct("PreacceptanceDisposition", 2)?;
                record.serialize_field("disposition", "accepted")?;
                record.serialize_field("response", response)?;
                record.end()
            }
            Self::Abandoned => {
                let mut record = serializer.serialize_struct("PreacceptanceDisposition", 1)?;
                record.serialize_field("disposition", "abandoned")?;
                record.end()
            }
            Self::CleanupPending { code } => {
                let mut record = serializer.serialize_struct("PreacceptanceDisposition", 2)?;
                record.serialize_field("disposition", "cleanup_pending")?;
                record.serialize_field("code", code)?;
                record.end()
            }
            Self::UnknownRemote { code } => {
                let mut record = serializer.serialize_struct("PreacceptanceDisposition", 2)?;
                record.serialize_field("disposition", "unknown_remote")?;
                record.serialize_field("code", code)?;
                record.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for PreacceptanceDisposition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "disposition", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Accepted { response: Box<StatusResponse> },
            Abandoned {},
            CleanupPending { code: String },
            UnknownRemote { code: String },
        }
        let disposition = match Wire::deserialize(deserializer)? {
            Wire::Accepted { response } => Self::Accepted(*response),
            Wire::Abandoned {} => Self::Abandoned,
            Wire::CleanupPending { code } => Self::CleanupPending { code },
            Wire::UnknownRemote { code } => Self::UnknownRemote { code },
        };
        disposition.validate().map_err(de::Error::custom)?;
        Ok(disposition)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostControlErrorDetail {
    code: String,
    message: String,
}

impl HostControlErrorDetail {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Result<Self, WorkerError> {
        let detail = Self {
            code: code.into(),
            message: message.into(),
        };
        detail.validate()?;
        Ok(detail)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        validate_control_code(&self.code, "host control error code")?;
        validate_control_message(&self.message, "host control error message")
    }

    pub fn code(&self) -> &str {
        &self.code
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Serialize for HostControlErrorDetail {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("HostControlErrorDetail", 2)?;
        record.serialize_field("code", &self.code)?;
        record.serialize_field("message", &self.message)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for HostControlErrorDetail {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            code: String,
            message: String,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.code, wire.message).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostControlError {
    protocol_version: u32,
    error: HostControlErrorDetail,
}

impl HostControlError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Result<Self, WorkerError> {
        let error = Self {
            protocol_version: PROTOCOL_VERSION,
            error: HostControlErrorDetail::new(code, message)?,
        };
        error.validate()?;
        Ok(error)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(protocol_error(
                "host control error has an incompatible protocol version",
            ));
        }
        self.error.validate()
    }

    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }
    pub fn error(&self) -> &HostControlErrorDetail {
        &self.error
    }
}

impl Serialize for HostControlError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("HostControlError", 2)?;
        record.serialize_field("protocol_version", &self.protocol_version)?;
        record.serialize_field("error", &self.error)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for HostControlError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol_version: u32,
            error: HostControlErrorDetail,
        }
        let wire = Wire::deserialize(deserializer)?;
        let error = Self {
            protocol_version: wire.protocol_version,
            error: wire.error,
        };
        error.validate().map_err(de::Error::custom)?;
        Ok(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    TaskCreated {
        protocol_version: u32,
        task_id: crate::task::TaskId,
        run_id: Option<crate::task::RunId>,
        title: String,
    },
    TaskState {
        protocol_version: u32,
        task_id: crate::task::TaskId,
        status: crate::task::TaskStatus,
    },
    TurnAccepted {
        protocol_version: u32,
        task_id: crate::task::TaskId,
        turn_id: crate::task::TurnId,
        worker: String,
    },
    TurnTerminal {
        protocol_version: u32,
        task_id: crate::task::TaskId,
        turn_id: crate::task::TurnId,
        outcome: crate::task::TaskOutcome,
    },
    ResultImported {
        protocol_version: u32,
        task_id: crate::task::TaskId,
        head_oid: crate::task::BaseOid,
        local_ref: String,
    },
    Error {
        protocol_version: u32,
        code: String,
        message: String,
    },
}

impl Serialize for JsonEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        #[derive(Serialize)]
        #[serde(tag = "event", rename_all = "snake_case")]
        enum Wire<'a> {
            Accepted {
                protocol_version: u32,
                response: &'a SubmitResponse,
            },
            Log {
                protocol_version: u32,
                chunk: &'a LogChunk,
            },
            Status {
                protocol_version: u32,
                response: &'a StatusResponse,
            },
            TaskCreated {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                run_id: &'a Option<crate::task::RunId>,
                title: &'a str,
            },
            TaskState {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                status: &'a crate::task::TaskStatus,
            },
            TurnAccepted {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                turn_id: crate::task::TurnId,
                worker: &'a str,
            },
            TurnTerminal {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                turn_id: crate::task::TurnId,
                outcome: &'a crate::task::TaskOutcome,
            },
            ResultImported {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                head_oid: &'a crate::task::BaseOid,
                local_ref: &'a str,
            },
            Error {
                protocol_version: u32,
                code: &'a str,
                message: &'a str,
            },
        }

        match self {
            Self::Accepted {
                protocol_version,
                response,
            } => Wire::Accepted {
                protocol_version: *protocol_version,
                response,
            }
            .serialize(serializer),
            Self::Log {
                protocol_version,
                chunk,
            } => Wire::Log {
                protocol_version: *protocol_version,
                chunk,
            }
            .serialize(serializer),
            Self::Status {
                protocol_version,
                response,
            } => Wire::Status {
                protocol_version: *protocol_version,
                response,
            }
            .serialize(serializer),
            Self::TaskCreated {
                protocol_version,
                task_id,
                run_id,
                title,
            } => Wire::TaskCreated {
                protocol_version: *protocol_version,
                task_id: *task_id,
                run_id,
                title,
            }
            .serialize(serializer),
            Self::TaskState {
                protocol_version,
                task_id,
                status,
            } => Wire::TaskState {
                protocol_version: *protocol_version,
                task_id: *task_id,
                status,
            }
            .serialize(serializer),
            Self::TurnAccepted {
                protocol_version,
                task_id,
                turn_id,
                worker,
            } => Wire::TurnAccepted {
                protocol_version: *protocol_version,
                task_id: *task_id,
                turn_id: *turn_id,
                worker,
            }
            .serialize(serializer),
            Self::TurnTerminal {
                protocol_version,
                task_id,
                turn_id,
                outcome,
            } => Wire::TurnTerminal {
                protocol_version: *protocol_version,
                task_id: *task_id,
                turn_id: *turn_id,
                outcome,
            }
            .serialize(serializer),
            Self::ResultImported {
                protocol_version,
                task_id,
                head_oid,
                local_ref,
            } => Wire::ResultImported {
                protocol_version: *protocol_version,
                task_id: *task_id,
                head_oid,
                local_ref,
            }
            .serialize(serializer),
            Self::Error {
                protocol_version,
                code,
                message,
            } => Wire::Error {
                protocol_version: *protocol_version,
                code,
                message,
            }
            .serialize(serializer),
        }
    }
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
            Self::TaskCreated {
                protocol_version,
                title,
                ..
            } => {
                validate_non_nul(title, 120, "task title")?;
                *protocol_version
            }
            Self::TaskState {
                protocol_version,
                status,
                ..
            } => {
                serde_json::to_vec(status)
                    .map_err(|_| protocol_error("task status event is invalid"))?;
                *protocol_version
            }
            Self::TurnAccepted {
                protocol_version,
                worker,
                ..
            } => {
                validate_non_nul(worker, 128, "worker name")?;
                *protocol_version
            }
            Self::TurnTerminal {
                protocol_version,
                outcome,
                ..
            } => {
                serde_json::to_vec(outcome)
                    .map_err(|_| protocol_error("turn terminal event is invalid"))?;
                *protocol_version
            }
            Self::ResultImported {
                protocol_version,
                local_ref,
                ..
            } => {
                validate_non_nul(local_ref, 512, "result ref")?;
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
            TaskCreated {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                run_id: Option<crate::task::RunId>,
                title: String,
            },
            TaskState {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                status: crate::task::TaskStatus,
            },
            TurnAccepted {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                turn_id: crate::task::TurnId,
                worker: String,
            },
            TurnTerminal {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                turn_id: crate::task::TurnId,
                outcome: crate::task::TaskOutcome,
            },
            ResultImported {
                protocol_version: u32,
                task_id: crate::task::TaskId,
                head_oid: crate::task::BaseOid,
                local_ref: String,
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
            Wire::TaskCreated {
                protocol_version,
                task_id,
                run_id,
                title,
            } => Self::TaskCreated {
                protocol_version,
                task_id,
                run_id,
                title,
            },
            Wire::TaskState {
                protocol_version,
                task_id,
                status,
            } => Self::TaskState {
                protocol_version,
                task_id,
                status,
            },
            Wire::TurnAccepted {
                protocol_version,
                task_id,
                turn_id,
                worker,
            } => Self::TurnAccepted {
                protocol_version,
                task_id,
                turn_id,
                worker,
            },
            Wire::TurnTerminal {
                protocol_version,
                task_id,
                turn_id,
                outcome,
            } => Self::TurnTerminal {
                protocol_version,
                task_id,
                turn_id,
                outcome,
            },
            Wire::ResultImported {
                protocol_version,
                task_id,
                head_oid,
                local_ref,
            } => Self::ResultImported {
                protocol_version,
                task_id,
                head_oid,
                local_ref,
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

fn require_sticky_identity(
    previous: Option<ProcessIdentity>,
    next: Option<ProcessIdentity>,
    label: &str,
) -> Result<(), WorkerError> {
    if previous.is_some_and(|previous| next != Some(previous)) {
        return Err(protocol_error(&format!(
            "{label} process identity is not sticky"
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

fn validate_opaque_identifier(value: &str, label: &str) -> Result<(), WorkerError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-' | b':' | b'@')
        })
        || !value
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(protocol_error(&format!("{label} is not canonical")));
    }
    Ok(())
}

fn validate_worker_name(value: &str) -> Result<(), WorkerError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
    {
        return Err(protocol_error("queue worker name is invalid"));
    }
    Ok(())
}

fn validate_worker_preference(preference: &WorkerPreference) -> Result<(), WorkerError> {
    match preference {
        WorkerPreference::Automatic => Ok(()),
        WorkerPreference::Pinned { worker } => validate_worker_name(worker),
    }
}

fn validate_requirements(requirements: &[String]) -> Result<(), WorkerError> {
    if requirements.len() > 256 {
        return Err(protocol_error("queue capability count exceeds its limit"));
    }
    let mut seen = BTreeSet::new();
    for capability in requirements {
        if capability.is_empty()
            || capability.len() > 128
            || !capability.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-' | b':' | b'@')
            })
        {
            return Err(protocol_error("queue capability is invalid"));
        }
        if !seen.insert(capability) {
            return Err(protocol_error("queue capabilities contain a duplicate"));
        }
    }
    Ok(())
}

fn validate_control_code(value: &str, label: &str) -> Result<(), WorkerError> {
    validate_non_nul(value, MAX_CONTROL_CODE_BYTES, label)?;
    let mut bytes = value.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_uppercase())
        || !bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(protocol_error(&format!(
            "{label} must be an uppercase ASCII identifier"
        )));
    }
    Ok(())
}

fn validate_control_message(value: &str, label: &str) -> Result<(), WorkerError> {
    validate_non_nul(value, MAX_CONTROL_MESSAGE_BYTES, label)?;
    if value.chars().any(char::is_control) {
        return Err(protocol_error(&format!(
            "{label} contains a control character"
        )));
    }
    Ok(())
}

fn validate_argument(value: &str) -> Result<(), WorkerError> {
    if value.len() > MAX_ARG_BYTES || value.as_bytes().contains(&0) {
        return Err(protocol_error("argv argument is too long or contains NUL"));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_control_error_serialization_revalidates_message_content() {
        for message in [
            "unsafe\nmessage",
            "unsafe\rmessage",
            "unsafe\tmessage",
            "unsafe\u{001b}message",
            "unsafe\u{007f}message",
            "unsafe\u{0085}message",
            "unsafe\u{009f}message",
        ] {
            let invalid_detail = HostControlErrorDetail {
                code: "HOST_REQUEST_FAILED".into(),
                message: message.into(),
            };
            assert!(
                serde_json::to_string(&invalid_detail).is_err(),
                "{message:?}"
            );
            let invalid = HostControlError {
                protocol_version: PROTOCOL_VERSION,
                error: invalid_detail,
            };
            assert!(serde_json::to_string(&invalid).is_err(), "{message:?}");
        }
    }
}
