//! Shared wire, cancellation, ownership and dependency contracts.
//!
//! The channel carries the union of explicit read-loop grammars only. Existing
//! strict stdio DTOs, protocol version, mutation retry and EOF behavior remain
//! authoritative. Blocking filesystem/control work belongs on bounded native
//! jobs; these interfaces do not perform it.

use std::{
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant, Version};

use crate::{
    config::{ControllerConfig, SshConfig, valid_ssh_destination},
    controller::{events::contracts::EventSelector, protocol::ControllerRequest},
    job::{ClientId, ProcessIdentity},
    paths::PathLayout,
    process::{ProcessCompletion, ProcessRequest, ProcessResult, ProcessRunner},
    protocol::PROTOCOL_VERSION,
    task::{TaskId, TurnId},
};

pub use crate::controller::protocol::MAX_FRAME_BYTES;

pub const CHANNEL_VERSION: u32 = 1;
pub const MAX_SESSIONS: usize = 16;
pub const MAX_SUPERVISORS: usize = 8;
pub const IDENTITY_BYTES: usize = 8 * 1024;
pub const PIN_BYTES: usize = 4 * 1024;
pub const READ_SCRATCH_BYTES: usize = 8 * 1024;
pub const SETUP_GUARD: Duration = Duration::from_secs(5);
pub const IDLE_GUARD: Duration = Duration::from_secs(60);
pub const REQUEST_GUARD: Duration = Duration::from_secs(30);
/// Fixed leader launch input; T3 overrides inherited values on every RPC child.
pub const DETACHED_RUNNER_EXECUTABLE_ENV: &str = "MAC_WORKER_DETACHED_RUNNER_EXECUTABLE";
pub const PIN_SCHEMA_VERSION: u32 = 1;
pub const SERVICE_SCHEMA_VERSION: u32 = 1;
pub const MAX_FEATURES: usize = 64;
pub const MAX_FEATURE_BYTES: usize = 64;
pub const MAX_USERNAME_BYTES: usize = 256;
pub const MAX_HOME_BYTES: usize = 4096;
/// Exclusive upper bound for literal Unix socket paths on the supported Macs.
pub const SOCKET_PATH_BYTES: usize = 104;

/// Explicit call-site scope: identical one-shot read bytes must use raw stdio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadLoopScope {
    Wait,
    LogsFollow,
    EventsFollow,
    Notify,
}

/// Literal configured route, not resolved DNS, host keys or config contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredRoute {
    pub ssh: String,
    pub remote_binary: String,
    pub ssh_config_file: Option<PathBuf>,
}

impl ConfiguredRoute {
    pub fn new(controller: &ControllerConfig, ssh: &SshConfig) -> Result<Self, ChannelFailure> {
        let route = Self {
            ssh: controller.ssh.clone(),
            remote_binary: controller.remote_binary.clone(),
            ssh_config_file: ssh.config_file.clone(),
        };
        route.validate()?;
        Ok(route)
    }

    fn validate(&self) -> Result<(), ChannelFailure> {
        if !valid_ssh_destination(&self.ssh)
            || !bounded_text(&self.ssh, MAX_HOME_BYTES)
            || !bounded_text(&self.remote_binary, MAX_HOME_BYTES)
            || self
                .ssh_config_file
                .as_ref()
                .is_some_and(|path| !absolute_text_path(path, MAX_HOME_BYTES))
        {
            return Err(unavailable(ChannelReason::UnsafePath));
        }
        Ok(())
    }

    /// SHA-256 of sorted compact JSON {schema_version,ssh,remote_binary,ssh_config_file}.
    pub fn digest(&self) -> Result<RouteDigest, ChannelFailure> {
        self.validate()?;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "ssh": self.ssh, "remote_binary": self.remote_binary,
            "ssh_config_file": self.ssh_config_file,
        }))
        .map_err(|_| unavailable(ChannelReason::InvalidFrame))?;
        Ok(RouteDigest(format!("{:x}", Sha256::digest(bytes))))
    }
}

/// Canonical 64-byte lowercase hexadecimal route hash; serialized as text.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteDigest(String);

impl RouteDigest {
    pub fn parse(value: &str) -> Result<Self, ChannelFailure> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(unavailable(ChannelReason::InvalidFrame));
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A lowercase hyphenated, RFC 4122, non-nil v4 UUID; never bare UUID serde.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UuidString(String);

impl UuidString {
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4().hyphenated().to_string())
    }
    pub fn parse(value: &str) -> Result<Self, ChannelFailure> {
        let uuid = Uuid::parse_str(value).map_err(|_| unavailable(ChannelReason::InvalidFrame))?;
        if uuid.is_nil()
            || uuid.get_version() != Some(Version::Random)
            || uuid.get_variant() != Variant::RFC4122
            || uuid.hyphenated().to_string() != value
        {
            return Err(unavailable(ChannelReason::InvalidFrame));
        }
        Ok(Self(value.into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! string_serde {
    ($name:ident) => {
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::parse(&String::deserialize(d)?).map_err(de::Error::custom)
            }
        }
    };
}
string_serde!(RouteDigest);
string_serde!(UuidString);

// Raw structs retain serde's duplicate-known-key checks. Identity structs
// tolerate additive fields; persisted pin/record/account schemas are strict.
macro_rules! wire_contract {
    ($(#[$meta:meta])* $name:ident, $raw:ident, $(#[$raw_meta:meta])* {
        $($(#[$field_meta:meta])* $field:ident : $ty:ty),* $(,)?
    }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name { $(pub $field: $ty),* }
        #[derive(Serialize, Deserialize)]
        $(#[$raw_meta])*
        struct $raw { $($(#[$field_meta])* $field: $ty),* }
        impl $name {
            #[allow(clippy::clone_on_copy)] // The same macro also handles owned fields.
            fn wire(&self) -> $raw { $raw { $($field: self.$field.clone()),* } }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                self.validate().map_err(serde::ser::Error::custom)?;
                self.wire().serialize(s)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let raw = $raw::deserialize(d)?;
                let value = Self { $($field: raw.$field),* };
                value.validate().map_err(de::Error::custom)?;
                Ok(value)
            }
        }
    };
}

wire_contract!(
    /// Stable Unix account authority, independent of leader/journal generations.
    ControllerAccount, WireAccount, #[serde(deny_unknown_fields)] {
        uid: u32, username: String, home: PathBuf,
    }
);
impl ControllerAccount {
    pub fn validate(&self) -> Result<(), ChannelFailure> {
        if !bounded_text(&self.username, MAX_USERNAME_BYTES)
            || !absolute_text_path(&self.home, MAX_HOME_BYTES)
        {
            return Err(unavailable(ChannelReason::InvalidFrame));
        }
        Ok(())
    }
}

wire_contract!(
    /// Trusted generation snapshot. journal_id is an optional cursor hint only.
    ServiceIdentity, WireServiceIdentity, {
        protocol_version: u32, channel_version: u32,
        controller_client_id: ClientId, account: ControllerAccount,
        leader: ProcessIdentity, service_generation: UuidString,
        socket_path: PathBuf, features: Vec<String>,
        #[serde(default)] journal_id: Option<UuidString>,
    }
);
impl ServiceIdentity {
    pub fn validate(&self) -> Result<(), ChannelFailure> {
        self.account.validate()?;
        self.leader
            .validate()
            .map_err(|_| unavailable(ChannelReason::InvalidFrame))?;
        if self.protocol_version != PROTOCOL_VERSION
            || self.channel_version != CHANNEL_VERSION
            || !safe_socket_path(&self.socket_path)
            || self.features.len() > MAX_FEATURES
            || self
                .features
                .iter()
                .any(|feature| !bounded_text(feature, MAX_FEATURE_BYTES))
            || self.features.windows(2).any(|pair| pair[0] >= pair[1])
            || !self
                .features
                .iter()
                .any(|feature| feature == "controller.socket")
            || self.journal_id.as_ref() == Some(&self.service_generation)
        {
            return Err(unavailable(ChannelReason::InvalidFrame));
        }
        check_size(&self.wire(), IDENTITY_BYTES)
    }
}

wire_contract!(
    /// Route echo plus the trusted required generation/account identity.
    SocketIdentity, WireSocketIdentity, {
    route_sha256: RouteDigest, service: ServiceIdentity,
});
impl SocketIdentity {
    pub fn validate(&self) -> Result<(), ChannelFailure> {
        self.service.validate()?;
        check_size(&self.wire(), IDENTITY_BYTES)
    }
}

wire_contract!(
    /// Strict stable pin; never stores a process, socket, feature or journal.
    Pin, WirePin, #[serde(deny_unknown_fields)] {
        schema_version: u32, route_sha256: RouteDigest,
        controller_client_id: ClientId, account: ControllerAccount,
    }
);
impl Pin {
    pub fn from_identity(identity: &SocketIdentity) -> Self {
        Self {
            schema_version: PIN_SCHEMA_VERSION,
            route_sha256: identity.route_sha256.clone(),
            controller_client_id: identity.service.controller_client_id,
            account: identity.service.account.clone(),
        }
    }
    pub fn validate(&self) -> Result<(), ChannelFailure> {
        self.account.validate()?;
        if self.schema_version != PIN_SCHEMA_VERSION {
            return Err(unavailable(ChannelReason::InvalidFrame));
        }
        check_size(&self.wire(), PIN_BYTES)
    }
}

/// Exact retained entry evidence; kind is S_IFMT, mode is the permission bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryIdentity {
    pub device: u64,
    pub inode: u64,
    pub owner: u32,
    pub kind: u32,
    pub mode: u32,
}
/// Exact private parent and socket evidence; never pathname-only ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SocketBinding {
    pub parent: EntryIdentity,
    pub socket: EntryIdentity,
}

/// Independently verified loaded-image identity and canonical absolute installed
/// path captured at startup. That installed path is also the detached launch input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningImage {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
}
/// Private no-replace hard link to the generation image, with exact binding.
/// Preserve executable mode; chmod of this link would change the installed inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedExecutable {
    pub path: PathBuf,
    pub binding: EntryIdentity,
}

/// Leader-only launch inputs. executable is the private generation RPC link;
/// detached_runner_executable is the distinct verified canonical installed path.
/// T3 sets DETACHED_RUNNER_EXECUTABLE_ENV from that field, overriding inheritance.
/// T7a injects it into DetachedRunnerExecutor; no request can choose either path.
/// Withdraw the exact-bound link only after admission stops and every socket RPC
/// child of this generation is proven exited. Unknown exit evidence retains it.
/// Detached groups use the installed path and are never awaited/cancelled for
/// link withdrawal, including prior-generation cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRpcSpec {
    pub executable: PinnedExecutable,
    pub detached_runner_executable: PathBuf,
    pub config: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
}

wire_contract!(
    /// Strict schema-1 publication, bounded with its binding/link evidence.
    ServiceRecord, WireServiceRecord, #[serde(deny_unknown_fields)] {
    schema_version: u32, service: ServiceIdentity,
    binding: SocketBinding, executable: PinnedExecutable,
});
impl ServiceRecord {
    pub fn validate(&self) -> Result<(), ChannelFailure> {
        self.service.validate()?;
        if self.schema_version != SERVICE_SCHEMA_VERSION {
            return Err(unavailable(ChannelReason::InvalidFrame));
        }
        check_size(&self.wire(), IDENTITY_BYTES)
    }
}

/// One exclusive foreground allocation and its retained private directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPath {
    pub directory: PathBuf,
    pub directory_identity: EntryIdentity,
    pub socket_path: PathBuf,
}

/// Captured expanded literal master endpoint and its retained private parent.
/// bootstrap_request keeps the original worker -F/trust policy with literal -S;
/// -O operations use this same endpoint and config-free -F /dev/null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterPlan {
    pub control_path: PathBuf,
    pub parent: EntryIdentity,
    pub bootstrap_request: ProcessRequest,
}

/// Additive identity-selector result; externally tagged snake_case JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // The frozen contract owns Available(SocketIdentity) directly.
pub enum SocketIdentityResult {
    Available(SocketIdentity),
    Unavailable(ChannelReason),
}
/// Transport loss versus a complete wrong-identity reply, which must not replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelFailure {
    Unavailable(ChannelReason),
    UnverifiedReply,
}
/// Bounded diagnostic reasons; no child output, secret or remote path detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelReason {
    Unsupported,
    ServiceUnavailable,
    PinMismatch,
    UnsafePath,
    ForwardLost,
    Busy,
    InvalidFrame,
    Timeout,
    Cancelled,
}
impl fmt::Display for ChannelFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "channel failure: {self:?}")
    }
}
impl std::error::Error for ChannelFailure {}

/// Cleaned requires positive exact cleanup evidence. Retained permanently
/// retires further setup for that foreground command, irrespective of backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardDisposition {
    Cleaned,
    Retained,
}

/// Retained for every interrupted/unacknowledged open without a demonstrable
/// terminal result. Bind-before-listen may refuse while the master can still
/// create/listen: neither refusal nor best-effort cancel proves settlement.
/// Cleaned means no owned allocation remains: no producer/allocation, or settled
/// creation followed by positive exact cleanup. Exit zero alone supplies no proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardOpenFailure {
    pub failure: ChannelFailure,
    pub disposition: ForwardDisposition,
}
impl ForwardOpenFailure {
    pub fn retained(failure: ChannelFailure) -> Self {
        Self {
            failure,
            disposition: ForwardDisposition::Retained,
        }
    }
}

/// Monotonic command clock and command cancellation, not a per-call predicate.
pub trait ChannelRuntime: Send + Sync {
    fn now(&self) -> Duration;
    fn cancelled(&self) -> bool;
}

/// Borrowed per-call cancellation stays live at every synchronous stage; it
/// deliberately has no Send, Sync or 'static bound and is never stored by a job.
pub struct ClientContext<'a> {
    pub runtime: &'a dyn ChannelRuntime,
    pub deadline: Duration,
    pub should_stop: &'a dyn Fn() -> bool,
}
impl ClientContext<'_> {
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.runtime.now())
    }
    pub fn check(&self) -> Result<(), ChannelFailure> {
        let stopped = (self.should_stop)();
        if stopped || self.runtime.cancelled() {
            return Err(unavailable(ChannelReason::Cancelled));
        }
        check_remaining(self.remaining())
    }
}

/// Clock-only cleanup budget, created after stream closure. Foreground cancel
/// cannot skip ownership cleanup; this context must never admit application work.
pub struct CleanupContext {
    pub runtime: Arc<dyn ChannelRuntime>,
    pub deadline: Duration,
}
impl CleanupContext {
    pub fn new(runtime: Arc<dyn ChannelRuntime>) -> Self {
        let deadline = runtime.now().saturating_add(SETUP_GUARD);
        Self { runtime, deadline }
    }
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.runtime.now())
    }
    pub fn check(&self) -> Result<(), ChannelFailure> {
        check_remaining(self.remaining())
    }
}

/// Owned runtime/child cancellation for native supervisors; unrelated to a
/// detached task's task.cancel or rollback semantics.
pub struct ServerContext {
    pub runtime: Arc<dyn ChannelRuntime>,
    pub deadline: Duration,
    pub cancelled: Arc<AtomicBool>,
}
impl ServerContext {
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.runtime.now())
    }
    pub fn check(&self) -> Result<(), ChannelFailure> {
        if self.cancelled.load(Ordering::Acquire) || self.runtime.cancelled() {
            return Err(unavailable(ChannelReason::Cancelled));
        }
        check_remaining(self.remaining())
    }
}

/// One decoder feed result: only consumed bytes and one completed payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeProgress {
    pub consumed: usize,
    pub payload: Option<Vec<u8>>,
}
/// One retained frame/prefix, no tail queue. consumed lets the caller enforce
/// phase-specific excess-byte policy; length is checked before payload allocation.
pub trait FrameDecoder: Send {
    fn feed(&mut self, input: &[u8]) -> Result<DecodeProgress, ChannelFailure>;
    fn retained_bytes(&self) -> usize;
}
/// Encoders return length-prefixed frames; decoders receive payloads only.
pub trait ChannelCodec: Send + Sync {
    fn decoder(&self) -> Box<dyn FrameDecoder>;
    fn encode_hello(&self, expected: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure>;
    fn decode_hello(&self, payload: &[u8]) -> Result<SocketIdentity, ChannelFailure>;
    fn encode_ready(&self, identity: &SocketIdentity) -> Result<Vec<u8>, ChannelFailure>;
    fn decode_ready(&self, payload: &[u8], expected: &SocketIdentity)
    -> Result<(), ChannelFailure>;
    fn encode_reply(
        &self,
        request: &ControllerRequest,
        result: &ProcessResult,
    ) -> Result<Vec<u8>, ChannelFailure>;
    fn decode_reply(
        &self,
        payload: &[u8],
        request: &ControllerRequest,
    ) -> Result<ProcessResult, ChannelFailure>;
}
/// One transient RPC child; Completed evidence alone releases its supervisor slot.
pub trait ChannelExecutor: Send + Sync {
    fn run(&self, frame: &[u8], ctx: &ServerContext) -> ProcessCompletion;
}
/// Blocking generation-start evidence source; native control work only.
pub trait RunningImageSource: Send + Sync {
    fn capture(&self) -> Result<RunningImage, ChannelFailure>;
}
/// Authenticated raw-stdio identity bootstrap. It cannot create client-id/state.
pub trait IdentitySource: Send + Sync {
    fn read(
        &self,
        raw: &dyn ProcessRunner,
        route: &ConfiguredRoute,
        master: Option<&MasterPlan>,
        ctx: &ClientContext<'_>,
    ) -> Result<SocketIdentity, ChannelFailure>;
}
/// Stable private pin operations, separate from notify cache/envelopes. Repin
/// requires a fresh authenticated raw identity and the exact expected ClientId.
pub trait PinStore: Send + Sync {
    fn verify_or_create(
        &self,
        paths: &PathLayout,
        identity: &SocketIdentity,
    ) -> Result<(), ChannelFailure>;
    fn repin(
        &self,
        paths: &PathLayout,
        identity: &SocketIdentity,
        expected: ClientId,
    ) -> Result<(), ChannelFailure>;
}
/// Blocking private allocation/ownership/refusal operations; bounded native jobs.
pub trait ForwardPaths: Send + Sync {
    fn allocate(&self, paths: &PathLayout) -> Result<ForwardPath, ChannelFailure>;
    fn validate_socket(&self, path: &ForwardPath) -> Result<EntryIdentity, ChannelFailure>;
    /// Precondition: creation has settled and its producer cannot still create
    /// or listen. Never use this operation to prove an unacknowledged open has
    /// settled. Requires bounded ECONNREFUSED plus exact retained parent/socket
    /// bindings after stream closure/cancel; zero status alone is insufficient.
    fn cleanup_if_refused(
        &self,
        path: &ForwardPath,
        socket: Option<EntryIdentity>,
        ctx: &CleanupContext,
    ) -> ForwardDisposition;
}
/// One owned stream-local forward on the captured master; never exits the master.
pub trait ForwardLease: Send {
    fn local_socket(&self) -> &Path;
    fn verify(&self) -> Result<(), ChannelFailure>;
    /// Caller closes the session first; uncertainty retains ownership residue
    /// and permanently retires setup for that foreground command.
    fn cancel(&mut self, raw: &dyn ProcessRunner, ctx: &CleanupContext) -> ForwardDisposition;
}
/// Original-config bounded resolution/bootstrap, followed by config-free -O
/// operations on exactly the captured literal master and single owned -L pair.
pub trait ForwardControl: Send + Sync {
    fn resolve(
        &self,
        raw: &dyn ProcessRunner,
        route: &ConfiguredRoute,
        ctx: &ClientContext<'_>,
    ) -> Result<MasterPlan, ChannelFailure>;
    fn open(
        &self,
        raw: &dyn ProcessRunner,
        master: &MasterPlan,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn ForwardLease>, ForwardOpenFailure>;
}
/// Sequential synchronous exchanges; the live borrowed predicate is polled
/// throughout I/O. Closing cancels only the transient RPC group, never a task.
pub trait SocketSession: Send {
    fn exchange(
        &mut self,
        frame: &[u8],
        request: &ControllerRequest,
        ctx: &ClientContext<'_>,
    ) -> Result<ProcessResult, ChannelFailure>;
    fn close(&mut self);
}
/// Connect and authenticate hello/ready before any application bytes are sent.
pub trait SocketConnector: Send + Sync {
    fn connect(
        &self,
        local: &Path,
        identity: &SocketIdentity,
        ctx: &ClientContext<'_>,
    ) -> Result<Box<dyn SocketSession>, ChannelFailure>;
}
/// Independent injectable stages for a command-scoped read-loop adapter.
#[derive(Clone)]
pub struct ClientDeps {
    pub identity: Arc<dyn IdentitySource>,
    pub pins: Arc<dyn PinStore>,
    pub forwards: Arc<dyn ForwardControl>,
    pub connector: Arc<dyn SocketConnector>,
    pub runtime: Arc<dyn ChannelRuntime>,
}

/// Verifies required route/stable/generation fields; journal hints are excluded.
pub fn verify_expected_service(
    expected: &SocketIdentity,
    actual: &SocketIdentity,
) -> Result<(), ChannelFailure> {
    expected.validate()?;
    actual.validate()?;
    let mut required = actual.clone();
    required.service.journal_id = expected.service.journal_id.clone();
    if *expected != required {
        return Err(ChannelFailure::UnverifiedReply);
    }
    Ok(())
}

/// Client permission requires both the explicit loop scope and valid body grammar.
pub fn eligible_read(scope: ReadLoopScope, request: &ControllerRequest) -> bool {
    server_eligible_read(request)
        && match scope {
            ReadLoopScope::Wait => request.command() == "task.wait.poll",
            ReadLoopScope::LogsFollow => request.command() == "task.logs" || loop_health(request),
            ReadLoopScope::EventsFollow | ReadLoopScope::Notify => request.command() == "task.list",
        }
}

/// Server-side grammar union; all setters, mutations and other selectors refuse
/// before child admission. The call-site scope remains a separate client check.
pub fn server_eligible_read(request: &ControllerRequest) -> bool {
    match request.command() {
        "task.wait.poll" => valid_wait_body(request.body()),
        "task.logs" => valid_logs_body(request.body()),
        "task.list" => {
            loop_health(request) || EventSelector::from_request_body(request.body()).is_ok()
        }
        _ => false,
    }
}

fn valid_wait_body(body: &Value) -> bool {
    let Some(object) = body.as_object() else {
        return false;
    };
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "task_id" | "run"))
    {
        return false;
    }
    match (
        object.get("task_id").filter(|value| !value.is_null()),
        object.get("run").filter(|value| !value.is_null()),
    ) {
        (Some(Value::String(id)), None) => id.parse::<TaskId>().is_ok(),
        (None, Some(Value::String(run))) => bounded_text(run, 256),
        _ => false,
    }
}

fn loop_health(request: &ControllerRequest) -> bool {
    request.command() == "task.list"
        && request.body() == &serde_json::json!({"controller_health": true})
}

fn valid_logs_body(body: &Value) -> bool {
    let Some(object) = body.as_object() else {
        return false;
    };
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "task_id" | "turn" | "turn_id" | "offset" | "limit" | "raw" | "follow" | "wait_ms"
        )
    }) {
        return false;
    }
    if !object
        .get("task_id")
        .and_then(Value::as_str)
        .is_some_and(|id| id.parse::<TaskId>().is_ok())
    {
        return false;
    }
    for (key, value) in object {
        let valid = match (key.as_str(), value) {
            ("task_id", _) | (_, Value::Null) => true,
            ("turn", Value::Number(number)) => number
                .as_u64()
                .is_some_and(|value| u32::try_from(value).is_ok()),
            ("turn_id", Value::String(id)) => id.parse::<TurnId>().is_ok(),
            ("offset" | "limit" | "wait_ms", Value::Number(number)) => number.as_u64().is_some(),
            ("raw" | "follow", Value::Bool(_)) => true,
            _ => false,
        };
        if !valid {
            return false;
        }
    }
    true
}

fn unavailable(reason: ChannelReason) -> ChannelFailure {
    ChannelFailure::Unavailable(reason)
}
fn check_remaining(remaining: Duration) -> Result<(), ChannelFailure> {
    if remaining.is_zero() {
        Err(unavailable(ChannelReason::Timeout))
    } else {
        Ok(())
    }
}
fn bounded_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
fn absolute_text_path(path: &Path, max: usize) -> bool {
    path.is_absolute() && path.to_str().is_some_and(|text| bounded_text(text, max))
}
fn safe_socket_path(path: &Path) -> bool {
    absolute_text_path(path, SOCKET_PATH_BYTES - 1)
        && path
            .to_str()
            .is_some_and(|text| !text.contains([':', '%', '$']))
        && !path.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}
fn check_size(value: &impl Serialize, max: usize) -> Result<(), ChannelFailure> {
    match serde_json::to_vec(value) {
        Ok(bytes) if bytes.len() <= max => Ok(()),
        _ => Err(unavailable(ChannelReason::InvalidFrame)),
    }
}
