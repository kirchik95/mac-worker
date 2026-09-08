use std::{
    collections::BTreeSet,
    ffi::OsString,
    fmt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned},
    ser::{self, SerializeStruct},
};
use serde_json::{Map, Value};

use crate::{
    account_launch::account_login_shell_request,
    agent::{AgentKind, AuthProbe, AuthProbeResult, adapter_for, render_prebind_shell},
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
};

/// Agent facts remain usable for fifteen minutes before a runner must refresh them.
pub const FACTS_TTL: u64 = 15 * 60 * 1000;
pub const FACTS_TTL_MILLIS: u64 = FACTS_TTL;

const PROBE_OUTPUT_LIMIT: usize = 4 * 1024;
const PROBE_DEADLINE: Duration = Duration::from_secs(2);
const MAX_TEXT_BYTES: usize = 128;

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
        _ => None,
    }
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
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
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
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentFacts {
    pub agents: Vec<AgentProbe>,
    pub env_profiles: Vec<ProfileProbe>,
    pub git_identity: bool,
    pub collected_at_millis: u64,
}

impl AgentFacts {
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
        let mut record = serializer.serialize_struct("AgentFacts", 4)?;
        record.serialize_field("agents", &agents)?;
        record.serialize_field("env_profiles", &profiles)?;
        record.serialize_field("git_identity", &self.git_identity)?;
        record.serialize_field("collected_at_millis", &self.collected_at_millis)?;
        record.end()
    }
}

impl<'de> Deserialize<'de> for AgentFacts {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            agents: Vec<AgentProbe>,
            env_profiles: Vec<ProfileProbe>,
            git_identity: bool,
            collected_at_millis: u64,
        }

        let wire: Wire = deserialize_unique_object(deserializer)?;
        let facts = Self {
            agents: wire.agents,
            env_profiles: wire.env_profiles,
            git_identity: wire.git_identity,
            collected_at_millis: wire.collected_at_millis,
        };
        facts.validate().map_err(de::Error::custom)?;
        Ok(facts)
    }
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
    collect_agent_facts_at(runner, account_home, profiles, current_time_millis())
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
        let adapter = adapter_for(kind);
        let binary = adapter.binary();
        let auth_probe = adapter.auth_probe();
        let base_available = adapter_available(runner, account_home, binary, &[]);
        let version = if base_available {
            run_version_in_shell(runner, account_home, binary, &[])
        } else {
            None
        };
        let auth = if base_available {
            run_auth_in_shell(runner, account_home, binary, &auth_probe, &[])
        } else {
            AgentAuth::Unknown
        };
        let mut auth_by_profile = Vec::new();
        let mut any_profile_binary = false;
        for profile in profiles
            .iter()
            .filter(|profile| profile.profile_is_secure())
        {
            let entries = profile.profile_entries();
            let available = adapter_available(runner, account_home, binary, &entries);
            any_profile_binary |= available;
            let auth = if !available {
                AgentAuth::Unknown
            } else {
                match profile.keychain_config() {
                    Some(config) if unlock_for_probe(runner, &config) => {
                        let _ = run_version_in_shell(runner, account_home, binary, &entries);
                        run_auth_in_shell(runner, account_home, binary, &auth_probe, &entries)
                    }
                    Some(_) => AgentAuth::UnknownWithReason(crate::keychain::UNLOCK_FAILED_REASON),
                    None => run_auth_in_shell(runner, account_home, binary, &auth_probe, &entries),
                }
            };
            auth_by_profile.push((profile.profile_name().to_owned(), auth));
        }
        if !base_available && !any_profile_binary {
            continue;
        }
        agents.push(AgentProbe {
            name: agent_name(kind).to_owned(),
            version,
            auth,
            auth_by_profile,
        });
    }

    AgentFacts {
        agents,
        env_profiles,
        git_identity: collect_git_identity(runner),
        collected_at_millis,
    }
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

fn adapter_available(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    profile_entries: &[(OsString, OsString)],
) -> bool {
    let shell = format!("command -v {binary}");
    let request =
        account_login_shell_request(account_home, profile_entries, &shell, probe_policy());
    runner
        .run(&request)
        .ok()
        .is_some_and(|result| result.status.success())
}

fn run_version_in_shell(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    profile_entries: &[(OsString, OsString)],
) -> Option<String> {
    let shell = render_prebind_shell(&[binary.to_owned(), "--version".to_owned()]).ok()?;
    let request =
        account_login_shell_request(account_home, profile_entries, &shell, probe_policy());
    let result = runner.run(&request).ok()?;
    if !result.status.success() {
        return None;
    }
    parse_version(&result.stdout)
}

fn run_auth_in_shell(
    runner: &dyn ProcessRunner,
    account_home: &Path,
    binary: &str,
    probe: &AuthProbe,
    profile_entries: &[(OsString, OsString)],
) -> AgentAuth {
    let mut argv = vec![binary.to_owned()];
    argv.extend(probe.args().iter().map(|arg| (*arg).to_owned()));
    let Ok(shell) = render_prebind_shell(&argv) else {
        return AgentAuth::Unknown;
    };
    let request =
        account_login_shell_request(account_home, profile_entries, &shell, probe_policy());
    let Some(result) = runner.run(&request).ok() else {
        return AgentAuth::Unknown;
    };
    if !is_valid_utf8(&result.stdout) || !is_valid_utf8(&result.stderr) {
        return AgentAuth::Unknown;
    }

    let classification = probe.classify(&result);
    if let AuthProbeResult::UnknownWithReason(reason) = classification {
        return AgentAuth::UnknownWithReason(reason);
    }
    if !result.status.success() || contains_auth_probe_error(&result) {
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

fn run_process(
    runner: &dyn ProcessRunner,
    program: OsString,
    args: Vec<OsString>,
    environment: Vec<(OsString, OsString)>,
) -> Option<ProcessResult> {
    let request = ProcessRequest {
        program,
        args,
        environment,
        environment_remove: Vec::new(),
        stdin: None,
        policy: probe_policy(),
        isolate_parent_environment: false,
    };
    runner.run(&request).ok()
}

fn unlock_for_probe(
    runner: &dyn ProcessRunner,
    config: &crate::keychain::KeychainUnlockConfig,
) -> bool {
    #[cfg(target_os = "macos")]
    {
        crate::keychain::unlock_keychain(
            runner,
            config,
            &crate::redaction::RedactionBoundary::from_env(),
        )
        .is_ok()
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (runner, config);
        false
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
    if value.len() <= MAX_TEXT_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_TEXT_BYTES;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn collect_git_identity(runner: &dyn ProcessRunner) -> bool {
    ["user.name", "user.email"].into_iter().all(|key| {
        let Some(result) = run_process(
            runner,
            OsString::from("zsh"),
            vec![
                OsString::from("-lc"),
                OsString::from(format!("git config --get {key}")),
            ],
            Vec::new(),
        ) else {
            return false;
        };
        result.status.success()
            && String::from_utf8(result.stdout).is_ok_and(|value| !value.trim().is_empty())
    })
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
