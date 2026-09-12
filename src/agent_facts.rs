use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fmt,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned},
    ser::{self, SerializeStruct},
};
use serde_json::{Map, Value};

use crate::{
    account_launch::{account_environment_scaffold, account_login_shell_request},
    agent::{AgentKind, AuthProbe, AuthProbeResult, adapter_for, render_prebind_shell},
    error::{ProcessError, WorkerError},
    herdr::{HerdrClient, HerdrError, HerdrSocket, ListedAgent, RESPONSE_DEADLINE},
    herdr_reporter::DISPLAY_AGENT,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
};

/// Agent facts remain usable for fifteen minutes before a runner must refresh them.
pub const FACTS_TTL: u64 = 15 * 60 * 1000;
pub const FACTS_TTL_MILLIS: u64 = FACTS_TTL;
/// Prefix of the interned reason written when a turn died of an authentication
/// failure. The rest is an ISO-8601 UTC minute so the string stays bounded.
pub const TURN_AUTH_FAILURE_REASON_PREFIX: &str = "auth failed in a turn at ";

const PROBE_OUTPUT_LIMIT: usize = 4 * 1024;
/// Bound on each locate, version, and auth probe during facts collection.
pub const PROBE_DEADLINE: Duration = Duration::from_secs(2);
const MAX_TEXT_BYTES: usize = 128;
const HERDR_BINARY: &str = "herdr";
/// The herdr version is bounded tighter than agent text (see [`HerdrFacts`]).
const HERDR_VERSION_MAX_BYTES: usize = 64;
/// herdr installs itself into `~/.local/bin`, which the account's login shell
/// does not always put on PATH; the lookup appends it so the operator's own
/// PATH still wins.
const HERDR_PATH_EXTENSION: &str = r#"export PATH="$PATH:$HOME/.local/bin"; "#;
/// Marker line between `command -v` stdout and `--version` stdout. A version
/// token must start with a digit, so this can never be parsed as a version.
const LOCATE_VERSION_SEPARATOR: &str = "MAC_WORKER_FACTS_VERSION";
/// Profile env names that can change which binary a login shell's
/// `command -v` finds. Other entries (tokens, keychain keys) cannot, so
/// locate+version from the default profile is reused. The names live on
/// [`EnvProfile::entries`].
const BINARY_RESOLUTION_ENV: &[&str] = &["PATH", "ZDOTDIR", "HOME", "SHELL"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentAuth {
    Authenticated,
    Unauthenticated,
    Unknown,
    UnknownWithReason(&'static str),
}

impl AgentAuth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Authenticated => "authenticated",
            Self::Unauthenticated => "unauthenticated",
            Self::Unknown | Self::UnknownWithReason(_) => "unknown",
        }
    }

    pub fn reason(self) -> Option<&'static str> {
        match self {
            Self::UnknownWithReason(reason) => Some(reason),
            Self::Authenticated | Self::Unauthenticated | Self::Unknown => None,
        }
    }
}

impl Serialize for AgentAuth {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::UnknownWithReason(reason) => {
                let mut record = serializer.serialize_struct("AgentAuth", 2)?;
                record.serialize_field("state", "unknown")?;
                record.serialize_field("reason", reason)?;
                record.end()
            }
            other => serializer.serialize_str(other.as_str()),
        }
    }
}

impl<'de> Deserialize<'de> for AgentAuth {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(value) => match value.as_str() {
                "authenticated" => Ok(Self::Authenticated),
                "unauthenticated" => Ok(Self::Unauthenticated),
                "unknown" => Ok(Self::Unknown),
                value => Err(de::Error::custom(format!(
                    "unknown agent authentication state `{value}`"
                ))),
            },
            Value::Object(mut object) => {
                let state = object
                    .remove("state")
                    .and_then(|value| value.as_str().map(str::to_owned));
                let reason = object
                    .remove("reason")
                    .and_then(|value| value.as_str().map(str::to_owned));
                if state.as_deref() != Some("unknown") || !object.is_empty() {
                    return Err(de::Error::custom("invalid agent authentication state"));
                }
                match reason.as_deref() {
                    Some(reason) => known_auth_reason(reason)
                        .map(Self::UnknownWithReason)
                        .ok_or_else(|| de::Error::custom("unknown agent authentication reason")),
                    None => Ok(Self::Unknown),
                }
            }
            _ => Err(de::Error::custom("agent authentication state is not valid")),
        }
    }
}

fn known_auth_reason(value: &str) -> Option<&'static str> {
    match value {
        crate::keychain::UNLOCK_FAILED_REASON => Some(crate::keychain::UNLOCK_FAILED_REASON),
        crate::keychain::KEYCHAIN_LOCKED_REASON => Some(crate::keychain::KEYCHAIN_LOCKED_REASON),
        crate::agent::LOGIN_UNVERIFIED_REASON => Some(crate::agent::LOGIN_UNVERIFIED_REASON),
        crate::auth_incidents::AUTH_INCIDENTS_UNREADABLE_REASON => {
            Some(crate::auth_incidents::AUTH_INCIDENTS_UNREADABLE_REASON)
        }
        crate::auth_incidents::AUTH_INCIDENT_REASON => {
            Some(crate::auth_incidents::AUTH_INCIDENT_REASON)
        }
        other => intern_turn_auth_failure_reason(other),
    }
}

/// Interned `unknown` reason for an authentication failure observed in a turn.
///
/// The timestamp is UTC at minute precision so the string is bounded and can
/// round-trip through facts.json. Callers must not put paths, prompts, or
/// log payloads into this reason.
pub fn turn_auth_failure_reason(at_millis: u64) -> Option<&'static str> {
    intern_turn_auth_failure_reason(&format_turn_auth_failure_reason(at_millis)?)
}

const MAX_INTERNED_TURN_REASONS: usize = 32;

fn intern_turn_auth_failure_reason(value: &str) -> Option<&'static str> {
    if !is_turn_auth_failure_reason(value) {
        return None;
    }
    Some(intern_or_fallback(
        value,
        MAX_INTERNED_TURN_REASONS,
        crate::auth_incidents::AUTH_INCIDENT_REASON,
    ))
}

fn intern_or_fallback(value: &str, cap: usize, fallback: &'static str) -> &'static str {
    static POOL: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    let mut pool = POOL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    intern_or_fallback_in(&mut pool, value, cap, fallback)
}

fn intern_or_fallback_in(
    pool: &mut Vec<&'static str>,
    value: &str,
    cap: usize,
    fallback: &'static str,
) -> &'static str {
    if let Some(existing) = pool.iter().copied().find(|stored| *stored == value) {
        return existing;
    }
    if pool.len() >= cap {
        return fallback;
    }
    let leaked: &'static str = Box::leak(value.to_owned().into_boxed_str());
    pool.push(leaked);
    leaked
}

fn format_turn_auth_failure_reason(at_millis: u64) -> Option<String> {
    let (year, month, day, hour, minute) = utc_minute_from_millis(at_millis)?;
    Some(format!(
        "{TURN_AUTH_FAILURE_REASON_PREFIX}{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}Z"
    ))
}

fn is_turn_auth_failure_reason(value: &str) -> bool {
    let Some(rest) = value.strip_prefix(TURN_AUTH_FAILURE_REASON_PREFIX) else {
        return false;
    };
    let bytes = rest.as_bytes();
    if bytes.len() != 17
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b'Z'
        || !bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 | 10 | 13 | 16 => true,
            _ => byte.is_ascii_digit(),
        })
    {
        return false;
    }
    let Ok(year) = rest[0..4].parse::<i32>() else {
        return false;
    };
    let Ok(month) = rest[5..7].parse::<u8>() else {
        return false;
    };
    let Ok(day) = rest[8..10].parse::<u8>() else {
        return false;
    };
    let Ok(hour) = rest[11..13].parse::<u8>() else {
        return false;
    };
    let Ok(minute) = rest[14..16].parse::<u8>() else {
        return false;
    };
    (1970..=9999).contains(&year)
        && (1..=12).contains(&month)
        && hour <= 23
        && minute <= 59
        && day >= 1
        && day <= days_in_month(year, month)
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn utc_minute_from_millis(at_millis: u64) -> Option<(i32, u8, u8, u8, u8)> {
    let seconds = at_millis / 1000;
    let minute_of_day = ((seconds / 60) % (24 * 60)) as u32;
    let hour = (minute_of_day / 60) as u8;
    let minute = (minute_of_day % 60) as u8;
    let days = i64::try_from(seconds / 86_400).ok()?;
    let (year, month, day) = civil_date_from_unix_days(days)?;
    Some((year, month, day, hour, minute))
}

/// Civil date from days since Unix epoch. Years outside 1970–9999 are
/// rejected so the interned reason stays a fixed width.
fn civil_date_from_unix_days(days: i64) -> Option<(i32, u8, u8)> {
    let z = days.checked_add(719_468)?;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = i32::try_from(era)
        .ok()?
        .checked_mul(400)?
        .checked_add(i32::try_from(yoe).ok()?)?;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u8;
    let year = if month <= 2 { y + 1 } else { y };
    if !(1970..=9999).contains(&year) {
        return None;
    }
    Some((year, month, day))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProbe {
    pub name: String,
    pub version: Option<String>,
    pub auth: AgentAuth,
    pub auth_by_profile: Vec<(String, AgentAuth)>,
}

impl AgentProbe {
    pub fn new(
        name: impl Into<String>,
        version: Option<String>,
        auth: AgentAuth,
        auth_by_profile: Vec<(String, AgentAuth)>,
    ) -> Result<Self, String> {
        let probe = Self {
            name: name.into(),
            version,
            auth,
            auth_by_profile,
        };
        probe.validate()?;
        Ok(probe)
    }

    fn validate(&self) -> Result<(), String> {
        validate_text(&self.name, "agent name")?;
        if let Some(version) = &self.version {
            validate_text(version, "agent version")?;
        }
        let mut profiles = BTreeSet::new();
        for (profile, _) in &self.auth_by_profile {
            validate_text(profile, "profile name")?;
            if !profiles.insert(profile) {
                return Err(format!("duplicate profile authentication `{profile}`"));
            }
        }
        Ok(())
    }
}

impl Serialize for AgentProbe {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut auth_by_profile = self.auth_by_profile.clone();
        auth_by_profile.sort_by(|left, right| left.0.cmp(&right.0));
        let mut record = serializer.serialize_struct("AgentProbe", 4)?;
        record.serialize_field("name", &self.name)?;
        record.serialize_field("version", &self.version)?;
        record.serialize_field("auth", &self.auth)?;
        record.serialize_field("auth_by_profile", &auth_by_profile)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for AgentProbe {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Laptop wire: ignore additive host fields. Known values still go
        // through [`Self::new`].
        #[derive(Deserialize)]
        struct Wire {
            name: String,
            version: Option<String>,
            auth: AgentAuth,
            auth_by_profile: Vec<(String, AgentAuth)>,
        }

        let wire: Wire = deserialize_unique_object(deserializer)?;
        Self::new(wire.name, wire.version, wire.auth, wire.auth_by_profile)
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileProbe {
    pub name: String,
    pub secure: bool,
}

impl ProfileProbe {
    pub fn new(name: impl Into<String>, secure: bool) -> Result<Self, String> {
        let profile = Self {
            name: name.into(),
            secure,
        };
        profile.validate()?;
        Ok(profile)
    }

    fn validate(&self) -> Result<(), String> {
        validate_text(&self.name, "profile name")
    }
}

impl Serialize for ProfileProbe {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut record = serializer.serialize_struct("ProfileProbe", 2)?;
        record.serialize_field("name", &self.name)?;
        record.serialize_field("secure", &self.secure)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for ProfileProbe {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Laptop wire: ignore additive host fields. Known values still go
        // through [`Self::new`].
        #[derive(Deserialize)]
        struct Wire {
            name: String,
            secure: bool,
        }

        let wire: Wire = deserialize_unique_object(deserializer)?;
        Self::new(wire.name, wire.secure).map_err(de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct EnvProfile {
    pub name: String,
    pub secure: bool,
    pub entries: Vec<(OsString, OsString)>,
}

impl EnvProfile {
    pub fn new(name: impl Into<String>, secure: bool, entries: Vec<(OsString, OsString)>) -> Self {
        Self {
            name: name.into(),
            secure,
            entries,
        }
    }

    pub fn probe(&self) -> ProfileProbe {
        ProfileProbe {
            name: self.name.clone(),
            secure: self.secure,
        }
    }
}

impl fmt::Debug for EnvProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entry_names = self
            .entries
            .iter()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("EnvProfile")
            .field("name", &self.name)
            .field("secure", &self.secure)
            .field("entry_names", &entry_names)
            .finish()
    }
}

/// Input accepted by [`collect_agent_facts`]. Values are copied only into the
/// child request and are never represented by the returned facts.
pub trait ProfileInput {
    fn profile_name(&self) -> &str;
    fn profile_is_secure(&self) -> bool;
    fn profile_entries(&self) -> Vec<(OsString, OsString)>;
    fn keychain_config(&self) -> Option<crate::keychain::KeychainUnlockConfig> {
        None
    }
}

impl ProfileInput for EnvProfile {
    fn profile_name(&self) -> &str {
        &self.name
    }

    fn profile_is_secure(&self) -> bool {
        self.secure
    }

    fn profile_entries(&self) -> Vec<(OsString, OsString)> {
        self.entries
            .iter()
            .filter(|(name, _)| !crate::keychain::is_reserved_env_name(&name.to_string_lossy()))
            .cloned()
            .collect()
    }

    fn keychain_config(&self) -> Option<crate::keychain::KeychainUnlockConfig> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default();
        crate::keychain::KeychainUnlockConfig::from_entries(&self.entries, &home)
    }
}

impl ProfileInput for ProfileProbe {
    fn profile_name(&self) -> &str {
        &self.name
    }

    fn profile_is_secure(&self) -> bool {
        self.secure
    }

    fn profile_entries(&self) -> Vec<(OsString, OsString)> {
        Vec::new()
    }
}

impl<T: ProfileInput + ?Sized> ProfileInput for &T {
    fn profile_name(&self) -> &str {
        (**self).profile_name()
    }

    fn profile_is_secure(&self) -> bool {
        (**self).profile_is_secure()
    }

    fn profile_entries(&self) -> Vec<(OsString, OsString)> {
        (**self).profile_entries()
    }

    fn keychain_config(&self) -> Option<crate::keychain::KeychainUnlockConfig> {
        (**self).keychain_config()
    }
}

/// Agent facts produced by a host helper.
///
/// A host may add fields. Laptop readers ([`Deserialize`]) ignore unknown
/// keys so a still-running older dashboard survives an additive helper.
/// The host's own `facts.json` still uses [`Self::from_host_store`], which
/// rejects unknown fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentFacts {
    pub agents: Vec<AgentProbe>,
    pub env_profiles: Vec<ProfileProbe>,
    pub git_identity: bool,
    pub collected_at_millis: u64,
    /// Whether the worker's herdr can show turns; absent from facts written
    /// before the herdr reporter existed.
    pub herdr: Option<HerdrFacts>,
    /// HTTPS credential-helper presence. Booleans only; helper command text
    /// is never stored. Old facts omit this object.
    pub origin_https_helpers: OriginHttpsHelpers,
}

/// Whether the worker account's global Git config has HTTPS credential
/// helpers. `generic` is true when `credential.helper` has any value,
/// including an empty-string reset. `hosts` names HTTPS hosts that have a
/// URL-specific helper. Old readers skip an omitted object.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OriginHttpsHelpers {
    #[serde(default, skip_serializing_if = "is_false")]
    pub generic: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hosts: BTreeMap<String, bool>,
}

impl OriginHttpsHelpers {
    pub fn is_empty(&self) -> bool {
        !self.generic && self.hosts.is_empty()
    }

    pub fn configured_for(&self, host: &str) -> bool {
        self.generic || self.hosts.get(host).copied().unwrap_or(false)
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// What `refresh-facts` learned about herdr on the worker.
///
/// Laptop readers ignore unknown keys. The host store decoder still
/// rejects them via [`AgentFacts::from_host_store`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HerdrFacts {
    pub state: HerdrFactState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Agents herdr's `agent.list` reported whose `display_agent` is not
    /// mac-worker's reporter. Present only when the state is `available` and
    /// the list call succeeded; absent from records that predate the count.
    /// The list itself is never stored: only this number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interactive_agents: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HerdrFactState {
    /// A herdr binary, a socket, and an answer to `ping`.
    Available,
    /// No herdr binary on the controlled host paths.
    NotInstalled,
    /// A binary but no socket for the default session.
    NoSocket,
    /// A socket that did not answer `ping` in time.
    NoResponse,
}

impl HerdrFactState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::NotInstalled => "not_installed",
            Self::NoSocket => "no_socket",
            Self::NoResponse => "no_response",
        }
    }
}

impl HerdrFacts {
    fn validate(&self) -> Result<(), String> {
        if let Some(version) = &self.version
            && (version.is_empty() || version.len() > 64 || version.chars().any(char::is_control))
        {
            return Err("herdr version is invalid".into());
        }
        Ok(())
    }
}

impl AgentFacts {
    /// Strict decode for the host's own `facts.json`.
    ///
    /// Unknown fields fail here so a foreign or future writer cannot
    /// silently change the cache the helper is responsible for.
    pub fn from_host_store(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        Self::deserialize_host(&mut deserializer)
    }

    fn deserialize_host<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct HostAgentProbe {
            name: String,
            version: Option<String>,
            auth: AgentAuth,
            auth_by_profile: Vec<(String, AgentAuth)>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct HostProfileProbe {
            name: String,
            secure: bool,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct HostHerdrFacts {
            state: HerdrFactState,
            #[serde(default)]
            version: Option<String>,
            #[serde(default)]
            interactive_agents: Option<u32>,
        }
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct HostOriginHttpsHelpers {
            #[serde(default)]
            generic: bool,
            #[serde(default)]
            hosts: BTreeMap<String, bool>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct HostWire {
            agents: Vec<HostAgentProbe>,
            env_profiles: Vec<HostProfileProbe>,
            git_identity: bool,
            collected_at_millis: u64,
            #[serde(default)]
            origin_https_helpers: HostOriginHttpsHelpers,
            #[serde(default)]
            herdr: Option<HostHerdrFacts>,
        }

        let wire: HostWire = deserialize_unique_object(deserializer)?;
        let mut agents = Vec::with_capacity(wire.agents.len());
        for agent in wire.agents {
            agents.push(
                AgentProbe::new(agent.name, agent.version, agent.auth, agent.auth_by_profile)
                    .map_err(de::Error::custom)?,
            );
        }
        let mut env_profiles = Vec::with_capacity(wire.env_profiles.len());
        for profile in wire.env_profiles {
            env_profiles
                .push(ProfileProbe::new(profile.name, profile.secure).map_err(de::Error::custom)?);
        }
        let facts = Self {
            agents,
            env_profiles,
            git_identity: wire.git_identity,
            collected_at_millis: wire.collected_at_millis,
            herdr: wire.herdr.map(|herdr| HerdrFacts {
                state: herdr.state,
                version: herdr.version,
                interactive_agents: herdr.interactive_agents,
            }),
            origin_https_helpers: OriginHttpsHelpers {
                generic: wire.origin_https_helpers.generic,
                hosts: wire.origin_https_helpers.hosts,
            },
        };
        facts.validate().map_err(de::Error::custom)?;
        Ok(facts)
    }

    pub fn collected_at_millis(&self) -> u64 {
        self.collected_at_millis
    }

    pub fn age_millis(&self, now_millis: u64) -> u64 {
        now_millis.saturating_sub(self.collected_at_millis)
    }

    pub fn is_stale(&self, now_millis: u64) -> bool {
        self.age_millis(now_millis) > FACTS_TTL
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    fn validate(&self) -> Result<(), String> {
        let mut agents = BTreeSet::new();
        for agent in &self.agents {
            agent.validate()?;
            if !agents.insert(&agent.name) {
                return Err(format!("duplicate agent `{}`", agent.name));
            }
        }

        let mut profiles = BTreeSet::new();
        for profile in &self.env_profiles {
            profile.validate()?;
            if !profiles.insert(&profile.name) {
                return Err(format!("duplicate profile `{}`", profile.name));
            }
        }
        if let Some(herdr) = &self.herdr {
            herdr.validate()?;
        }
        Ok(())
    }
}

impl Serialize for AgentFacts {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(ser::Error::custom)?;
        let mut agents = self.agents.clone();
        agents.sort_by(|left, right| left.name.cmp(&right.name));
        let mut profiles = self.env_profiles.clone();
        profiles.sort_by(|left, right| left.name.cmp(&right.name));
        let mut fields = 4;
        if !self.origin_https_helpers.is_empty() {
            fields += 1;
        }
        if self.herdr.is_some() {
            fields += 1;
        }
        let mut record = serializer.serialize_struct("AgentFacts", fields)?;
        record.serialize_field("agents", &agents)?;
        record.serialize_field("env_profiles", &profiles)?;
        record.serialize_field("git_identity", &self.git_identity)?;
        record.serialize_field("collected_at_millis", &self.collected_at_millis)?;
        if !self.origin_https_helpers.is_empty() {
            record.serialize_field("origin_https_helpers", &self.origin_https_helpers)?;
        }
        if let Some(herdr) = &self.herdr {
            record.serialize_field("herdr", herdr)?;
        }
        record.end()
    }
}

impl<'de> Deserialize<'de> for AgentFacts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Laptop wire: ignore additive host fields. Known values still go
        // through [`Self::validate`].
        #[derive(Deserialize)]
        struct Wire {
            agents: Vec<AgentProbe>,
            env_profiles: Vec<ProfileProbe>,
            git_identity: bool,
            collected_at_millis: u64,
            #[serde(default)]
            origin_https_helpers: OriginHttpsHelpers,
            #[serde(default)]
            herdr: Option<HerdrFacts>,
        }

        let wire: Wire = deserialize_unique_object(deserializer)?;
        let facts = Self {
            agents: wire.agents,
            env_profiles: wire.env_profiles,
            git_identity: wire.git_identity,
            collected_at_millis: wire.collected_at_millis,
            herdr: wire.herdr,
            origin_https_helpers: wire.origin_https_helpers,
        };
        facts.validate().map_err(de::Error::custom)?;
        Ok(facts)
    }
}

/// Durations recorded while collecting agent facts. Printed only when the
/// operator asks (`worker host refresh-facts --timing`); each line names
/// public fact fields and millisecond counts, never command lines, paths,
/// or profile values.
#[derive(Debug, Clone)]
pub struct FactsTiming {
    steps: Vec<TimingStep>,
    started: Instant,
}

#[derive(Debug, Clone)]
struct TimingStep {
    agent: Option<String>,
    profile: Option<String>,
    step: String,
    ms: u128,
    result: Option<String>,
    deadline_ms: Option<u128>,
    hit_deadline: bool,
}

impl FactsTiming {
    fn new() -> Self {
        Self {
            steps: Vec::new(),
            started: Instant::now(),
        }
    }

    /// One stderr line per recorded step, in collection order, with `total` last.
    pub fn lines(&self) -> Vec<String> {
        self.steps.iter().map(TimingStep::render).collect()
    }

    pub fn write_to(&self, writer: &mut dyn Write) {
        for line in self.lines() {
            let _ = writeln!(writer, "{line}");
        }
    }

    fn probe<T>(
        &mut self,
        agent: Option<&str>,
        profile: Option<&str>,
        step: &str,
        deadline: Duration,
        run: impl FnOnce() -> ProbeRun<T>,
    ) -> T {
        self.probe_with_result(agent, profile, step, deadline, |_| None, run)
    }

    fn probe_with_result<T>(
        &mut self,
        agent: Option<&str>,
        profile: Option<&str>,
        step: &str,
        deadline: Duration,
        result_of: impl FnOnce(&T) -> Option<&'static str>,
        run: impl FnOnce() -> ProbeRun<T>,
    ) -> T {
        let started = Instant::now();
        let ProbeRun {
            value,
            hit_deadline,
        } = run();
        let elapsed = started.elapsed();
        self.steps.push(TimingStep::new(
            agent,
            profile,
            step,
            elapsed,
            result_of(&value),
            Some(deadline),
            hit_deadline || elapsed >= deadline,
        ));
        value
    }

    fn finish_total(&mut self) {
        self.steps.push(TimingStep::new(
            None,
            None,
            "total",
            self.started.elapsed(),
            None,
            None,
            false,
        ));
    }
}

impl TimingStep {
    fn new(
        agent: Option<&str>,
        profile: Option<&str>,
        step: &str,
        elapsed: Duration,
        result: Option<&str>,
        deadline: Option<Duration>,
        hit_deadline: bool,
    ) -> Self {
        Self {
            agent: agent.map(str::to_owned),
            profile: profile.map(str::to_owned),
            step: step.to_owned(),
            ms: elapsed.as_millis(),
            result: result.map(str::to_owned),
            deadline_ms: deadline.map(|deadline| deadline.as_millis()),
            hit_deadline,
        }
    }

    fn render(&self) -> String {
        let mut line = String::from("timing");
        if let Some(agent) = &self.agent {
            line.push_str(" agent=");
            line.push_str(agent);
        }
        if let Some(profile) = &self.profile {
            line.push_str(" profile=");
            line.push_str(profile);
        }
        line.push_str(" step=");
        line.push_str(&self.step);
        line.push_str(" ms=");
        line.push_str(&self.ms.to_string());
        if let Some(result) = &self.result {
            line.push_str(" result=");
            line.push_str(result);
        }
        if let Some(deadline_ms) = self.deadline_ms {
            line.push_str(" deadline_ms=");
            line.push_str(&deadline_ms.to_string());
        }
        if self.hit_deadline {
            line.push_str(" hit_deadline=true");
        }
        line
    }
}

struct ProbeRun<T> {
    value: T,
    hit_deadline: bool,
}

impl<T> ProbeRun<T> {
    fn ok(value: T) -> Self {
        Self {
            value,
            hit_deadline: false,
        }
    }
}

fn is_deadline_error(error: &WorkerError) -> bool {
    matches!(
        error,
        WorkerError::Process(ProcessError::DeadlineExceeded { .. })
    )
}

/// Collect facts using the current wall-clock time in milliseconds.
pub fn collect_agent_facts<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
) -> AgentFacts
where
    P: ProfileInput,
{
    collect_agent_facts_with_timing(runner, account_home, profiles).0
}

/// Collect facts and the per-step durations that produced them.
pub fn collect_agent_facts_with_timing<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
) -> (AgentFacts, FactsTiming)
where
    P: ProfileInput,
{
    collect_agent_facts_at_with_timing(runner, account_home, profiles, current_time_millis())
}

/// Deterministic-time variant used by local tests and cache callers.
pub fn collect_agent_facts_at<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
    collected_at_millis: u64,
) -> AgentFacts
where
    P: ProfileInput,
{
    collect_agent_facts_at_with_timing(runner, account_home, profiles, collected_at_millis).0
}

/// Deterministic-time collection that also returns per-step durations.
pub fn collect_agent_facts_at_with_timing<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
    collected_at_millis: u64,
) -> (AgentFacts, FactsTiming)
where
    P: ProfileInput,
{
    let (facts, timing, _) = collect_agent_facts_with_optional_host(
        runner,
        account_home,
        profiles,
        collected_at_millis,
        None,
    );
    (facts, timing)
}

/// Like [`collect_agent_facts_at_with_timing`], then overlays current auth
/// incidents from the host state root.
///
/// A current incident for an agent (newer than the last successful
/// auth-carrying turn of the same agent and profile, younger than 24 h, and
/// not cleared by a Codex re-login) overlays that agent's facts as
/// [`AgentAuth::UnknownWithReason`] with [`turn_auth_failure_reason`].
/// Status probes cannot see a burned refresh token or a Cursor login that
/// still prints authenticated; the overlay is what stops the scheduler
/// advertising `agent:<kind>` until the operator re-logs in, a later turn
/// succeeds, or they pass `--clear-auth-incidents`.
///
/// If the incident store is corrupt or unreadable, advertised authentication
/// is replaced with [`crate::auth_incidents::AUTH_INCIDENTS_UNREADABLE_REASON`]
/// so the scheduler never treats the overlay miss as a successful login.
pub fn collect_agent_facts_at_host_with_timing<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
    collected_at_millis: u64,
    host_state_root: &Path,
) -> (AgentFacts, FactsTiming)
where
    P: ProfileInput,
{
    let (facts, timing, _) = collect_agent_facts_with_optional_host(
        runner,
        account_home,
        profiles,
        collected_at_millis,
        Some(host_state_root),
    );
    (facts, timing)
}

/// Collects facts and overlays incidents. The `Result` is `Err` when the
/// overlay could not be applied; facts in the tuple are still conservative.
pub(crate) fn collect_agent_facts_at_host_with_overlay<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
    collected_at_millis: u64,
    host_state_root: &Path,
) -> (AgentFacts, FactsTiming, Result<(), WorkerError>)
where
    P: ProfileInput,
{
    collect_agent_facts_with_optional_host(
        runner,
        account_home,
        profiles,
        collected_at_millis,
        Some(host_state_root),
    )
}

fn collect_agent_facts_with_optional_host<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
    collected_at_millis: u64,
    host_state_root: Option<&Path>,
) -> (AgentFacts, FactsTiming, Result<(), WorkerError>)
where
    P: ProfileInput,
{
    let mut timing = FactsTiming::new();
    let mut facts = collect_agent_facts_into(
        runner,
        account_home,
        profiles,
        collected_at_millis,
        &mut timing,
    );
    let overlay = if let Some(host_state_root) = host_state_root {
        match crate::auth_incidents::merge_into_facts(
            &mut facts,
            host_state_root,
            account_home,
            collected_at_millis,
        ) {
            Ok(()) => Ok(()),
            Err(error) => {
                crate::auth_incidents::apply_unreadable_overlay(&mut facts);
                Err(error)
            }
        }
    } else {
        Ok(())
    };
    timing.finish_total();
    (facts, timing, overlay)
}

fn collect_agent_facts_into<P>(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    profiles: &[P],
    collected_at_millis: u64,
    timing: &mut FactsTiming,
) -> AgentFacts
where
    P: ProfileInput,
{
    let env_profiles = profiles
        .iter()
        .map(ProfileInput::profile_name)
        .zip(profiles.iter().map(ProfileInput::profile_is_secure))
        .map(|(name, secure)| ProfileProbe {
            name: name.to_owned(),
            secure,
        })
        .collect::<Vec<_>>();

    let mut agents = Vec::new();
    for kind in [
        AgentKind::Codex,
        AgentKind::Claude,
        AgentKind::Cursor,
        AgentKind::Opencode,
    ] {
        let name = agent_name(kind);
        let adapter = adapter_for(kind);
        let binary = adapter.binary();
        let auth_probe = adapter.auth_probe();
        let LocateVersion {
            available: base_available,
            version,
        } = timing.probe(
            Some(name),
            Some("-"),
            "locate+version",
            PROBE_DEADLINE,
            || run_locate_and_version(runner, account_home, binary, &[]),
        );
        let auth = if base_available {
            timing.probe_with_result(
                Some(name),
                Some("-"),
                "auth",
                PROBE_DEADLINE,
                |auth: &AgentAuth| Some(auth.as_str()),
                || run_auth_in_shell(runner, account_home, binary, &auth_probe, &[]),
            )
        } else {
            AgentAuth::Unknown
        };
        let mut auth_by_profile = Vec::new();
        let mut any_profile_binary = false;
        for profile in profiles
            .iter()
            .filter(|profile| profile.profile_is_secure())
        {
            let profile_name = profile.profile_name();
            let entries = profile.profile_entries();
            let available = resolve_profile_binary(
                runner,
                account_home,
                binary,
                name,
                profile_name,
                &entries,
                base_available,
                timing,
            );
            any_profile_binary |= available;
            let auth = probe_profile_auth(
                runner,
                account_home,
                binary,
                &auth_probe,
                name,
                profile_name,
                &entries,
                profile.keychain_config(),
                available,
                timing,
            );
            auth_by_profile.push((profile_name.to_owned(), auth));
        }
        if !base_available && !any_profile_binary {
            continue;
        }
        agents.push(AgentProbe {
            name: name.to_owned(),
            version,
            auth,
            auth_by_profile,
        });
    }

    AgentFacts {
        agents,
        env_profiles,
        git_identity: collect_git_identity(runner, account_home),
        collected_at_millis,
        herdr: Some(collect_herdr_facts(runner, account_home, timing)),
        origin_https_helpers: collect_origin_https_helpers(runner, account_home),
    }
}

/// Whether the account's herdr can show turns: a `herdr` binary on the login
/// shell's PATH (plus `~/.local/bin`), its `--version`, the default session
/// socket, and an answer to `ping` under the client deadlines.  The version is
/// recorded whenever the binary answered; the ping carries no path.  When
/// the socket is `available`, one timed `agent.list` step counts interactive
/// agents best-effort; a failed list leaves the count absent and does not
/// change the state.
fn collect_herdr_facts(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    timing: &mut FactsTiming,
) -> HerdrFacts {
    let LocateVersion { available, version } = timing.probe(
        Some("herdr"),
        Some("-"),
        "locate+version",
        PROBE_DEADLINE,
        || herdr_locate_and_version(runner, account_home),
    );
    if !available {
        return HerdrFacts {
            state: HerdrFactState::NotInstalled,
            version: None,
            interactive_agents: None,
        };
    }
    let socket = HerdrSocket::default_for_home(account_home);
    if !socket.path().exists() {
        return HerdrFacts {
            state: HerdrFactState::NoSocket,
            version,
            interactive_agents: None,
        };
    }
    let client = HerdrClient::new(socket);
    let state = timing.probe(
        None,
        None,
        "herdr-ping",
        RESPONSE_DEADLINE,
        || match client.ping() {
            Ok(()) => ProbeRun::ok(HerdrFactState::Available),
            Err(HerdrError::Absent) => ProbeRun::ok(HerdrFactState::NoSocket),
            Err(HerdrError::Timeout) => ProbeRun {
                value: HerdrFactState::NoResponse,
                hit_deadline: true,
            },
            Err(_) => ProbeRun::ok(HerdrFactState::NoResponse),
        },
    );
    let interactive_agents = if state == HerdrFactState::Available {
        timing.probe(
            None,
            None,
            "herdr-agents",
            RESPONSE_DEADLINE,
            || match client.agent_list() {
                Ok(listed) => ProbeRun::ok(count_interactive_agents(&listed)),
                Err(HerdrError::Timeout) => ProbeRun {
                    value: None,
                    hit_deadline: true,
                },
                Err(_) => ProbeRun::ok(None),
            },
        )
    } else {
        None
    };
    HerdrFacts {
        state,
        version,
        interactive_agents,
    }
}

/// Agents herdr lists whose `display_agent` is not the mac-worker reporter.
/// Only the count is returned; titles, cwds, and pane ids from the list
/// never leave this function.
fn count_interactive_agents(listed: &[ListedAgent]) -> Option<u32> {
    let count = listed
        .iter()
        .filter(|agent| agent.display_agent.as_deref() != Some(DISPLAY_AGENT))
        .count();
    u32::try_from(count).ok()
}

#[allow(clippy::too_many_arguments)]
fn resolve_profile_binary(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    agent: &str,
    profile_name: &str,
    entries: &[(OsString, OsString)],
    base_available: bool,
    timing: &mut FactsTiming,
) -> bool {
    if !profile_can_change_binary_resolution(entries) {
        return base_available;
    }
    timing
        .probe(
            Some(agent),
            Some(profile_name),
            "locate+version",
            PROBE_DEADLINE,
            || run_locate_and_version(runner, account_home, binary, entries),
        )
        .available
}

#[allow(clippy::too_many_arguments)]
fn probe_profile_auth(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    auth_probe: &AuthProbe,
    agent: &str,
    profile_name: &str,
    entries: &[(OsString, OsString)],
    keychain: Option<crate::keychain::KeychainUnlockConfig>,
    available: bool,
    timing: &mut FactsTiming,
) -> AgentAuth {
    if !available {
        return AgentAuth::Unknown;
    }
    match keychain {
        Some(config) => {
            let unlocked = timing.probe(
                Some(agent),
                Some(profile_name),
                "keychain-unlock",
                crate::keychain::UNLOCK_TIMEOUT,
                || unlock_for_probe(runner, &config),
            );
            if unlocked {
                timing.probe_with_result(
                    Some(agent),
                    Some(profile_name),
                    "auth",
                    PROBE_DEADLINE,
                    |auth: &AgentAuth| Some(auth.as_str()),
                    || run_auth_in_shell(runner, account_home, binary, auth_probe, entries),
                )
            } else {
                AgentAuth::UnknownWithReason(crate::keychain::UNLOCK_FAILED_REASON)
            }
        }
        None => timing.probe_with_result(
            Some(agent),
            Some(profile_name),
            "auth",
            PROBE_DEADLINE,
            |auth: &AgentAuth| Some(auth.as_str()),
            || run_auth_in_shell(runner, account_home, binary, auth_probe, entries),
        ),
    }
}

/// Whether a profile's entries can change which binary `command -v` finds.
/// Only [`BINARY_RESOLUTION_ENV`] (`PATH`, `ZDOTDIR`, `HOME`, `SHELL`) do.
fn profile_can_change_binary_resolution(entries: &[(OsString, OsString)]) -> bool {
    entries
        .iter()
        .any(|(name, _)| BINARY_RESOLUTION_ENV.iter().any(|env| name == env))
}

fn herdr_locate_and_version(
    runner: &dyn ProcessRunner,
    account_home: &Path,
) -> ProbeRun<LocateVersion> {
    run_locate_and_version_script(
        runner,
        account_home,
        HERDR_BINARY,
        &[],
        HERDR_PATH_EXTENSION,
        HERDR_VERSION_MAX_BYTES,
    )
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn agent_name(kind: AgentKind) -> &'static str {
    match kind {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

fn run_locate_and_version(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    profile_entries: &[(OsString, OsString)],
) -> ProbeRun<LocateVersion> {
    run_locate_and_version_script(
        runner,
        account_home,
        binary,
        profile_entries,
        "",
        MAX_TEXT_BYTES,
    )
}

fn run_locate_and_version_script(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    profile_entries: &[(OsString, OsString)],
    path_extension: &str,
    version_max_bytes: usize,
) -> ProbeRun<LocateVersion> {
    let Ok(version_exec) = render_prebind_shell(&[binary.to_owned(), "--version".to_owned()])
    else {
        return ProbeRun::ok(LocateVersion {
            available: false,
            version: None,
        });
    };
    let shell = format!(
        "{path_extension}command -v {binary} && printf '%s\\n' '{LOCATE_VERSION_SEPARATOR}' && {version_exec}"
    );
    let request =
        account_login_shell_request(account_home, profile_entries, &shell, probe_policy());
    match runner.run(&request) {
        Ok(result) => ProbeRun::ok(parse_locate_and_version(
            &result.stdout,
            result.status.success(),
            version_max_bytes,
        )),
        Err(error) => ProbeRun {
            value: LocateVersion {
                available: false,
                version: None,
            },
            hit_deadline: is_deadline_error(&error),
        },
    }
}

struct LocateVersion {
    available: bool,
    version: Option<String>,
}

fn parse_locate_and_version(
    stdout: &[u8],
    version_succeeded: bool,
    version_max_bytes: usize,
) -> LocateVersion {
    let Some(version_out) = stdout_after_separator(stdout) else {
        return LocateVersion {
            available: false,
            version: None,
        };
    };
    let version = if version_succeeded {
        parse_version(version_out)
            .map(|version| truncate_text_to(&version, version_max_bytes))
            .filter(|version| !version.is_empty())
    } else {
        None
    };
    LocateVersion {
        available: true,
        version,
    }
}

fn stdout_after_separator(stdout: &[u8]) -> Option<&[u8]> {
    let needle = LOCATE_VERSION_SEPARATOR.as_bytes();
    let pos = stdout
        .windows(needle.len())
        .position(|window| window == needle)?;
    let after = &stdout[pos + needle.len()..];
    Some(after.strip_prefix(b"\n").unwrap_or(after))
}

fn run_auth_in_shell(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    probe: &AuthProbe,
    profile_entries: &[(OsString, OsString)],
) -> ProbeRun<AgentAuth> {
    let mut argv = vec![binary.to_owned()];
    argv.extend(probe.args().iter().map(|arg| (*arg).to_owned()));
    let Ok(shell) = render_prebind_shell(&argv) else {
        return ProbeRun::ok(AgentAuth::Unknown);
    };
    let request =
        account_login_shell_request(account_home, profile_entries, &shell, probe_policy());
    match runner.run(&request) {
        Ok(result) => ProbeRun::ok(classify_auth_result(probe, &result)),
        Err(error) => ProbeRun {
            value: AgentAuth::Unknown,
            hit_deadline: is_deadline_error(&error),
        },
    }
}

fn classify_auth_result(probe: &AuthProbe, result: &ProcessResult) -> AgentAuth {
    if !is_valid_utf8(&result.stdout) || !is_valid_utf8(&result.stderr) {
        return AgentAuth::Unknown;
    }

    let classification = probe.classify(result);
    if let AuthProbeResult::UnknownWithReason(reason) = classification {
        return AgentAuth::UnknownWithReason(reason);
    }
    if !result.status.success() || contains_auth_probe_error(result) {
        return AgentAuth::Unknown;
    }
    match classification {
        AuthProbeResult::Authenticated => AgentAuth::Authenticated,
        AuthProbeResult::Unauthenticated => AgentAuth::Unauthenticated,
        AuthProbeResult::Unknown | AuthProbeResult::UnknownWithReason(_) => AgentAuth::Unknown,
    }
}

fn probe_policy() -> ProcessPolicy {
    ProcessPolicy {
        stdout_limit: PROBE_OUTPUT_LIMIT,
        stderr_limit: PROBE_OUTPUT_LIMIT,
        deadline: PROBE_DEADLINE,
    }
}

fn unlock_for_probe(
    runner: &dyn ProcessRunner,
    config: &crate::keychain::KeychainUnlockConfig,
) -> ProbeRun<bool> {
    #[cfg(target_os = "macos")]
    {
        match crate::keychain::unlock_keychain(
            runner,
            config,
            &crate::redaction::RedactionBoundary::from_env(),
        ) {
            Ok(()) => ProbeRun::ok(true),
            Err(error) => ProbeRun {
                value: false,
                hit_deadline: is_deadline_error(&error),
            },
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (runner, config);
        ProbeRun::ok(false)
    }
}

fn is_valid_utf8(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
}

fn contains_auth_probe_error(result: &ProcessResult) -> bool {
    [&result.stdout, &result.stderr]
        .into_iter()
        .filter_map(|bytes| std::str::from_utf8(bytes).ok())
        .any(|text| {
            let text = text.to_ascii_lowercase();
            [
                "keychain",
                "network error",
                "network unavailable",
                "connection failed",
                "connection refused",
                "unable to connect",
                "timed out",
                "timeout",
            ]
            .iter()
            .any(|marker| text.contains(marker))
        })
}

fn parse_version(output: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(output).ok()?;
    text.split_whitespace().find_map(|token| {
        let token = token.trim_matches(|character: char| {
            !character.is_ascii_alphanumeric() && !matches!(character, '.' | '-' | '_')
        });
        let token = token.strip_prefix('v').unwrap_or(token);
        if token.is_empty()
            || !token.starts_with(|character: char| character.is_ascii_digit())
            || !token.chars().any(|character| character.is_ascii_digit())
            || !token.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
            })
        {
            return None;
        }
        Some(truncate_text(token))
    })
}

fn truncate_text(value: &str) -> String {
    truncate_text_to(value, MAX_TEXT_BYTES)
}

fn truncate_text_to(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn collect_git_identity(runner: &dyn ProcessRunner, account_home: &Path) -> bool {
    ["user.name", "user.email"].into_iter().all(|key| {
        let request = account_login_shell_request(
            account_home,
            &[],
            &format!("git config --global --get {key}"),
            probe_policy(),
        );
        let Ok(result) = runner.run(&request) else {
            return false;
        };
        result.status.success()
            && String::from_utf8(result.stdout).is_ok_and(|value| !value.trim().is_empty())
    })
}

fn collect_origin_https_helpers(
    runner: &dyn ProcessRunner,
    account_home: &Path,
) -> OriginHttpsHelpers {
    let mut helpers = OriginHttpsHelpers::default();
    if let Some(result) = run_account_git_config(
        runner,
        account_home,
        &["config", "--global", "--get-all", "credential.helper"],
    ) && result.status.success()
        && !result.stdout.is_empty()
    {
        helpers.generic = true;
    }
    let Some(result) = run_account_git_config(
        runner,
        account_home,
        &[
            "config",
            "--global",
            "--get-regexp",
            r"^credential\..*\.helper$",
        ],
    ) else {
        return helpers;
    };
    if !result.status.success() {
        return helpers;
    }
    for line in String::from_utf8_lossy(&result.stdout).lines() {
        let Some(key) = line.split_whitespace().next() else {
            continue;
        };
        if key == "credential.helper" {
            helpers.generic = true;
            continue;
        }
        if let Some(host) = https_credential_helper_host(key) {
            helpers.hosts.insert(host, true);
        }
    }
    helpers
}

fn run_account_git_config(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    args: &[&str],
) -> Option<ProcessResult> {
    let mut environment = account_environment_scaffold(account_home);
    environment.push((OsString::from("GIT_TERMINAL_PROMPT"), OsString::from("0")));
    let request = ProcessRequest {
        program: OsString::from("/usr/bin/git"),
        args: args.iter().map(OsString::from).collect(),
        environment,
        environment_remove: vec![
            OsString::from("GIT_CONFIG_GLOBAL"),
            OsString::from("GIT_CONFIG_NOSYSTEM"),
            OsString::from("GIT_CONFIG"),
            OsString::from("GIT_CONFIG_COUNT"),
            OsString::from("GIT_CONFIG_PARAMETERS"),
        ],
        stdin: None,
        policy: probe_policy(),
        isolate_parent_environment: true,
    };
    runner.run(&request).ok()
}

fn https_credential_helper_host(key: &str) -> Option<String> {
    let rest = key.strip_prefix("credential.")?.strip_suffix(".helper")?;
    let url = url::Url::parse(rest).ok()?;
    if url.scheme() != "https" {
        return None;
    }
    url.host_str()
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
}

fn validate_text(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > MAX_TEXT_BYTES || value.chars().any(char::is_control) {
        return Err(format!(
            "{field} is empty, too long, or contains a control character"
        ));
    }
    Ok(())
}

struct UniqueObject(Map<String, Value>);

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = UniqueValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON value with unique object keys")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Number(value.into())))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .map(UniqueValue)
                    .ok_or_else(|| E::custom("JSON number is not finite"))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value.to_owned())))
            }

            fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
                self.visit_str(value)
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(value)))
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(UniqueValue(Value::Array(values)))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut object = Map::new();
                while let Some((key, UniqueValue(value))) = map.next_entry()? {
                    if object.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate field `{key}`")));
                    }
                    object.insert(key, value);
                }
                Ok(UniqueValue(Value::Object(object)))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl<'de> Deserialize<'de> for UniqueObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = UniqueObject;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut object = Map::new();
                while let Some((key, UniqueValue(value))) =
                    map.next_entry::<String, UniqueValue>()?
                {
                    if object.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate field `{key}`")));
                    }
                    object.insert(key, value);
                }
                Ok(UniqueObject(object))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

fn deserialize_unique_object<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: DeserializeOwned,
    D: Deserializer<'de>,
{
    let UniqueObject(object) = UniqueObject::deserialize(deserializer)?;
    T::deserialize(Value::Object(object)).map_err(de::Error::custom)
}

#[cfg(test)]
mod intern_tests {
    use super::*;

    #[test]
    fn intern_pool_falls_back_when_full() {
        let mut pool = Vec::new();
        let first = intern_or_fallback_in(
            &mut pool,
            "auth failed in a turn at 2024-01-01T00:00Z",
            1,
            crate::auth_incidents::AUTH_INCIDENT_REASON,
        );
        assert_eq!(first, "auth failed in a turn at 2024-01-01T00:00Z");
        let second = intern_or_fallback_in(
            &mut pool,
            "auth failed in a turn at 2024-01-01T00:01Z",
            1,
            crate::auth_incidents::AUTH_INCIDENT_REASON,
        );
        assert_eq!(second, crate::auth_incidents::AUTH_INCIDENT_REASON);
        assert_eq!(
            intern_or_fallback_in(
                &mut pool,
                "auth failed in a turn at 2024-01-01T00:00Z",
                1,
                crate::auth_incidents::AUTH_INCIDENT_REASON,
            ),
            first
        );
    }

    #[test]
    fn invalid_calendar_dates_are_not_auth_reasons() {
        assert!(known_auth_reason("auth failed in a turn at 2024-13-01T00:00Z").is_none());
        assert!(known_auth_reason("auth failed in a turn at 2024-01-32T00:00Z").is_none());
        assert!(known_auth_reason("auth failed in a turn at 2024-01-01T24:00Z").is_none());
        assert!(known_auth_reason("auth failed in a turn at 2024-02-30T00:00Z").is_none());
        assert!(known_auth_reason(crate::auth_incidents::AUTH_INCIDENT_REASON).is_some());
        assert!(
            known_auth_reason(crate::auth_incidents::AUTH_INCIDENTS_UNREADABLE_REASON).is_some()
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt};

    use tempfile::tempdir;

    use super::{collect_git_identity, collect_origin_https_helpers};
    use crate::process::SystemProcessRunner;

    #[test]
    fn collect_git_identity_uses_account_home_not_ambient_home() {
        let temp = tempdir().unwrap();
        let empty_account = temp.path().join("empty-account");
        fs::create_dir_all(&empty_account).unwrap();
        fs::set_permissions(&empty_account, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            !collect_git_identity(&SystemProcessRunner, &empty_account),
            "empty account HOME must not inherit the developer's git identity"
        );

        let configured = temp.path().join("configured-account");
        fs::create_dir_all(&configured).unwrap();
        fs::set_permissions(&configured, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            configured.join(".gitconfig"),
            "[user]\n    name = Ada Lovelace\n    email = ada@example.test\n",
        )
        .unwrap();
        assert!(
            collect_git_identity(&SystemProcessRunner, &configured),
            "account .gitconfig name+email must count as a configured identity"
        );
    }

    #[test]
    fn collect_origin_https_helpers_reports_boolean_presence_without_helper_text() {
        let temp = tempdir().unwrap();
        let empty_account = temp.path().join("empty-account");
        fs::create_dir_all(&empty_account).unwrap();
        fs::set_permissions(&empty_account, fs::Permissions::from_mode(0o700)).unwrap();
        let empty = collect_origin_https_helpers(&SystemProcessRunner, &empty_account);
        assert!(
            empty.is_empty(),
            "empty account HOME must not inherit the developer's helpers"
        );

        let configured = temp.path().join("configured-account");
        fs::create_dir_all(&configured).unwrap();
        fs::set_permissions(&configured, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            configured.join(".gitconfig"),
            "[credential]\n    helper = osxkeychain\n[credential \"https://github.com\"]\n    helper = !/usr/bin/gh auth git-credential\n",
        )
        .unwrap();
        let helpers = collect_origin_https_helpers(&SystemProcessRunner, &configured);
        assert!(helpers.generic);
        assert_eq!(helpers.hosts.get("github.com").copied(), Some(true));
        let json = serde_json::to_string(&helpers).unwrap();
        assert!(!json.contains("osxkeychain") && !json.contains("gh auth"));
    }
}
