use std::{fmt, str::FromStr};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de,
    ser::{self, SerializeStruct},
};
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

#[derive(Clone, PartialEq, Eq)]
pub struct RequestFingerprintMaterial {
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

impl fmt::Debug for RequestFingerprintMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestFingerprintMaterial")
            .field("protocol_version", &self.protocol_version)
            .field("job_id", &self.job_id)
            .field("client_id", &self.client_id)
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

    pub fn job_id(&self) -> JobId {
        self.job_id
    }
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
    pub fn lease_token(&self) -> LeaseToken {
        self.lease_token
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

    fn validate(self) -> Result<(), WorkerError> {
        Self::new(self.pid, self.start_time_micros).map(|_| ())
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
        created_at_millis: u64,
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
            created_at_millis,
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
pub struct LocalJobRecord {
    meta: JobMeta,
    lease_token: LeaseToken,
    last_status: Option<JobStatus>,
    cleanup_pending: bool,
}

impl Serialize for LocalJobRecord {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("LocalJobRecord", 4)?;
        record.serialize_field("meta", &self.meta)?;
        record.serialize_field("lease_token", &self.lease_token)?;
        record.serialize_field("last_status", &self.last_status)?;
        record.serialize_field("cleanup_pending", &self.cleanup_pending)?;
        record.end()
    }
}

impl LocalJobRecord {
    pub fn new(
        meta: JobMeta,
        lease_token: LeaseToken,
        last_status: Option<JobStatus>,
        cleanup_pending: bool,
    ) -> Result<Self, WorkerError> {
        let record = Self {
            meta,
            lease_token,
            last_status,
            cleanup_pending,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        if let Some(status) = &self.last_status {
            status.validate()?;
        }
        Ok(())
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
    pub fn cleanup_pending(&self) -> bool {
        self.cleanup_pending
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
            cleanup_pending: bool,
        }
        let wire = Wire::deserialize(deserializer)?;
        let record = Self {
            meta: wire.meta,
            lease_token: wire.lease_token,
            last_status: wire.last_status,
            cleanup_pending: wire.cleanup_pending,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusResponse {
    meta: JobMeta,
    status: JobStatus,
}

impl Serialize for StatusResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("StatusResponse", 2)?;
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
            meta: JobMeta,
            status: JobStatus,
        }
        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            meta: wire.meta,
            status: wire.status,
        };
        response.validate().map_err(de::Error::custom)?;
        Ok(response)
    }
}

impl StatusResponse {
    pub fn new(meta: JobMeta, status: JobStatus) -> Result<Self, WorkerError> {
        let response = Self { meta, status };
        response.validate()?;
        Ok(response)
    }

    pub fn validate(&self) -> Result<(), WorkerError> {
        self.meta.validate()?;
        self.status.validate()
    }

    pub fn meta(&self) -> &JobMeta {
        &self.meta
    }
    pub fn status(&self) -> &JobStatus {
        &self.status
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
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
