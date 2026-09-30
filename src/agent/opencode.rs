use std::{ffi::OsString, path::Path};

use serde_json::Value;

use crate::{error::WorkerError, process::ProcessResult};

use super::{
    AdapterError, AgentAdapter, AgentEvent, AgentKind, AuthProbe, AuthProbeResult,
    StructuredResult, TurnLaunch, TurnParams, argv_pointer_launch, bound_summary, combined_output,
    json_i32, parse_json_line, require_permission, require_session_ref,
    resolve_last_structured_result, strip_ansi, validate_params,
};

const BINARY: &str = "opencode";
/// "Run with a private server instead of the background service" (v2 only).
const STANDALONE: &str = "--standalone";

/// Launch refused: the worker's OpenCode is not the generation the argv was
/// built for.
pub const OPENCODE_DIALECT_MISMATCH: &str = "OPENCODE_DIALECT_MISMATCH";
/// Launch refused: the argv has no `--standalone` and the worker could not
/// tell which generation it is about to start.
pub const OPENCODE_VERSION_UNVERIFIED: &str = "OPENCODE_VERSION_UNVERIFIED";

/// The OpenCode CLI generation a command is written for.
///
/// OpenCode 2 sends `run` and most other commands to a shared background
/// service that outlives the turn's process group, so the supervisor's group
/// cleanup cannot reach it. `--standalone` gives the command a private server
/// instead, and every v2 command mac-worker runs carries it, apart from
/// `--version`, which needs no server. OpenCode 1 has no such service and
/// rejects the flag, so the two forms are not interchangeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpencodeDialect {
    V1,
    V2,
}

impl OpencodeDialect {
    /// The generation a version names: major 2 and later is v2, anything
    /// below is v1. `None` when the version is absent or has no numeric
    /// major, so callers choose what an unknown generation means for them.
    pub fn observed(version: Option<&str>) -> Option<Self> {
        let version = version?;
        let major = version
            .strip_prefix('v')
            .unwrap_or(version)
            .split('.')
            .next()?;
        if major.is_empty() || !major.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let major: u64 = major.parse().ok()?;
        Some(if major >= 2 { Self::V2 } else { Self::V1 })
    }

    /// The form a launch is built in for a worker whose facts record
    /// `version`. An unknown version keeps the v1 form mac-worker has always
    /// sent; [`verify_launch`] on the worker refuses it when the installed
    /// OpenCode is not v1.
    pub fn for_launch(recorded_version: Option<&str>) -> Self {
        Self::observed(recorded_version).unwrap_or(Self::V1)
    }

    /// The form of a command a host runs on its own account right after it
    /// observed `version`. Only a positively identified v1 gets the v1 form:
    /// v1 rejects `--standalone` and exits, while v2 without the flag would
    /// start the background service.
    pub fn for_host_command(observed_version: Option<&str>) -> Self {
        match Self::observed(observed_version) {
            Some(Self::V1) => Self::V1,
            Some(Self::V2) | None => Self::V2,
        }
    }
}

pub(super) struct OpencodeAdapter {
    dialect: OpencodeDialect,
}

static V1: OpencodeAdapter = OpencodeAdapter {
    dialect: OpencodeDialect::V1,
};
static V2: OpencodeAdapter = OpencodeAdapter {
    dialect: OpencodeDialect::V2,
};

/// Parsing and result extraction are the same in both dialects; they differ
/// in the argv of a turn, of a session delete and of the auth probe.
pub(super) fn adapter(dialect: OpencodeDialect) -> &'static dyn AgentAdapter {
    match dialect {
        OpencodeDialect::V1 => &V1,
        OpencodeDialect::V2 => &V2,
    }
}

impl AgentAdapter for OpencodeAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Opencode
    }

    fn binary(&self) -> &'static str {
        BINARY
    }

    fn auth_probe(&self) -> AuthProbe {
        match self.dialect {
            OpencodeDialect::V1 => AuthProbe::new(&["auth", "list"], classify_opencode_auth),
            OpencodeDialect::V2 => {
                AuthProbe::new(&["auth", "list", STANDALONE], classify_standalone_auth)
            }
        }
    }

    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        let permission = require_permission(params)?;
        Ok(argv_pointer_launch(
            self.binary(),
            opencode_args(self.dialect, params.model.as_deref(), None),
            Vec::new(),
            permission.fallback,
        ))
    }

    fn resume_turn(
        &self,
        params: &TurnParams,
        session_ref: &str,
    ) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        let permission = require_permission(params)?;
        let session_ref = require_session_ref(session_ref)?;
        Ok(argv_pointer_launch(
            self.binary(),
            opencode_args(self.dialect, params.model.as_deref(), Some(session_ref)),
            Vec::new(),
            permission.fallback,
        ))
    }

    fn delete_session(&self, session_ref: &str) -> Option<Vec<String>> {
        let session_ref = require_session_ref(session_ref).ok()?;
        let mut argv = vec![
            self.binary().into(),
            "session".into(),
            "delete".into(),
            session_ref.into(),
        ];
        if self.dialect == OpencodeDialect::V2 {
            argv.push(STANDALONE.into());
        }
        Some(argv)
    }

    fn parse_event(&self, line: &str) -> Option<AgentEvent> {
        let value = parse_json_line(line)?;
        match value.get("type").and_then(Value::as_str)? {
            "step_start" => Some(AgentEvent::SessionStarted {
                session_ref: session_id(&value)?.to_string(),
            }),
            "text" => Some(AgentEvent::AssistantMessage {
                text: value
                    .pointer("/part/text")
                    .and_then(Value::as_str)?
                    .to_string(),
            }),
            "tool_use" => parse_tool_use(value.get("part")?),
            "step_finish"
                if value.pointer("/part/reason").and_then(Value::as_str) != Some("tool-calls") =>
            {
                Some(AgentEvent::TurnEnd {
                    reason: value
                        .pointer("/part/reason")
                        .and_then(Value::as_str)
                        .unwrap_or("completed")
                        .to_string(),
                })
            }
            "error" => Some(AgentEvent::TurnEnd {
                reason: value
                    .pointer("/error/data/message")
                    .or_else(|| value.pointer("/error/name"))
                    .and_then(Value::as_str)
                    .unwrap_or("error")
                    .to_string(),
            }),
            _ => None,
        }
    }

    fn extract_result(
        &self,
        stream: &str,
        last_message_file: Option<&str>,
    ) -> Result<StructuredResult, AdapterError> {
        Ok(
            resolve_last_structured_result(last_message_file, &result_candidates(stream))
                .with_output_presence(stream),
        )
    }
}

/// The dialect an OpenCode turn launch was built for, read from the argv the
/// worker is about to exec. `None` for any other command: only `opencode run`
/// is guarded here.
pub fn launch_dialect(argv: &[OsString]) -> Option<OpencodeDialect> {
    let program = Path::new(argv.first()?).file_name()?;
    if program != BINARY || argv.get(1)? != "run" {
        return None;
    }
    // Both dialects put the flag, when present, right after the subcommand,
    // so a model or prompt that happens to read `--standalone` is not it.
    Some(if argv.get(2).is_some_and(|flag| flag == STANDALONE) {
        OpencodeDialect::V2
    } else {
        OpencodeDialect::V1
    })
}

/// Worker-side launch guard. Call it with the version the worker observed on
/// the executable it is about to exec.
///
/// Facts that reach the runner can be stale, so the argv is checked against
/// the installed generation before exec:
/// - v2 without `--standalone` would start the background service;
/// - v1 with it prints usage and exits, which hides the cause;
/// - without `--standalone` and with no observed version the generation is
///   unknown, and the launch is refused because it may be v2.
///
/// A launch that carries `--standalone` runs when the version is unknown: v2
/// uses a private server and v1 exits on the flag, so neither starts the
/// service.
pub fn verify_launch(
    built_for: OpencodeDialect,
    observed_version: Option<&str>,
) -> Result<(), WorkerError> {
    match (built_for, OpencodeDialect::observed(observed_version)) {
        (OpencodeDialect::V1, Some(OpencodeDialect::V1))
        | (OpencodeDialect::V2, Some(OpencodeDialect::V2) | None) => Ok(()),
        (OpencodeDialect::V1, Some(OpencodeDialect::V2)) => Err(WorkerError::task(
            OPENCODE_DIALECT_MISMATCH,
            "OpenCode v2 is installed but the launch has no --standalone",
        )),
        (OpencodeDialect::V2, Some(OpencodeDialect::V1)) => Err(WorkerError::task(
            OPENCODE_DIALECT_MISMATCH,
            "OpenCode v1 is installed but the launch carries --standalone",
        )),
        (OpencodeDialect::V1, None) => Err(WorkerError::task(
            OPENCODE_VERSION_UNVERIFIED,
            "the OpenCode version could not be observed for a launch without --standalone",
        )),
    }
}

fn classify_opencode_auth(result: &ProcessResult) -> AuthProbeResult {
    let text = combined_output(result);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return AuthProbeResult::Unknown;
    }
    if matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "no credentials" | "no credentials found" | "not authenticated"
    ) {
        return AuthProbeResult::Unauthenticated;
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return classify_credentials_listing(trimmed);
    };
    match value {
        Value::Array(values) if values.is_empty() => AuthProbeResult::Unauthenticated,
        Value::Array(values) if values.iter().any(is_configured_provider) => {
            AuthProbeResult::Authenticated
        }
        Value::Array(_) => AuthProbeResult::Unknown,
        Value::Object(values) => classify_provider_object(&values),
        _ => AuthProbeResult::Unknown,
    }
}

/// `opencode auth list --standalone` (2.0) prints one table row per stored
/// credential: `<provider>  <API key|OAuth>  stored`. A row ending in
/// `stored` is an authenticated store and no rows at all is an
/// unauthenticated one. Anything else stays unknown, such as the usage text
/// v1 prints for the unknown flag, or a sentence in place of an empty table.
fn classify_standalone_auth(result: &ProcessResult) -> AuthProbeResult {
    let plain = strip_ansi(&combined_output(result));
    let mut rows = plain
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .peekable();
    if rows.peek().is_none() {
        return AuthProbeResult::Unauthenticated;
    }
    if rows.any(is_stored_row) {
        AuthProbeResult::Authenticated
    } else {
        AuthProbeResult::Unknown
    }
}

fn is_stored_row(row: &str) -> bool {
    let mut columns = row.split_whitespace().rev();
    columns.next() == Some("stored") && columns.next().is_some()
}

/// `opencode auth list` (1.18) prints a decorated human listing rather than
/// JSON: a `Credentials <store>` header followed by one `<provider> <api|oauth>`
/// line per stored credential, with ANSI colour codes and box-drawing glyphs.
/// Any provider line with a credential type is an authenticated store; a
/// header without provider lines is an unauthenticated one.
fn classify_credentials_listing(text: &str) -> AuthProbeResult {
    let plain = strip_ansi(text);
    let mut saw_header = false;
    let mut providers = 0usize;
    for raw in plain.lines() {
        let line = raw
            .trim_matches(|character: char| {
                character.is_whitespace() || !(character.is_ascii_graphic() || character == ' ')
            })
            .trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("Credentials") {
            saw_header = true;
            continue;
        }
        let lower = line.to_ascii_lowercase();
        if lower == "no credentials" || lower == "no credentials found" {
            return AuthProbeResult::Unauthenticated;
        }
        let credential_type = line.rsplit(' ').next().unwrap_or("").to_ascii_lowercase();
        if matches!(credential_type.as_str(), "api" | "oauth" | "wellknown")
            && line.len() > credential_type.len() + 1
        {
            providers += 1;
        }
    }
    match (saw_header, providers) {
        (_, count) if count > 0 => AuthProbeResult::Authenticated,
        (true, 0) => AuthProbeResult::Unauthenticated,
        _ => AuthProbeResult::Unknown,
    }
}

fn classify_provider_object(values: &serde_json::Map<String, Value>) -> AuthProbeResult {
    let Some(providers) = values.get("providers") else {
        return AuthProbeResult::Unknown;
    };
    match providers {
        Value::Array(values) if values.is_empty() => AuthProbeResult::Unauthenticated,
        Value::Array(values) if values.iter().any(is_configured_provider) => {
            AuthProbeResult::Authenticated
        }
        Value::Array(_) => AuthProbeResult::Unknown,
        _ => AuthProbeResult::Unknown,
    }
}

fn is_configured_provider(value: &Value) -> bool {
    match value {
        Value::String(name) => !name.trim().is_empty(),
        Value::Object(values) => values
            .get("provider")
            .or_else(|| values.get("name"))
            .and_then(Value::as_str)
            .is_some_and(|name| !name.trim().is_empty()),
        _ => false,
    }
}

fn opencode_args(
    dialect: OpencodeDialect,
    model: Option<&str>,
    session_ref: Option<&str>,
) -> Vec<String> {
    let mut args = vec!["run".to_string()];
    if dialect == OpencodeDialect::V2 {
        // Keep the flag right after the subcommand: `launch_dialect` reads it
        // there.
        args.push(STANDALONE.into());
    }
    args.extend(["--format".into(), "json".into(), "--auto".into()]);
    if let Some(model) = model {
        args.push("--model".into());
        args.push(model.to_string());
    }
    if let Some(session_ref) = session_ref {
        args.push("--session".into());
        args.push(session_ref.to_string());
    }
    args
}

fn session_id(value: &Value) -> Option<&str> {
    value
        .get("sessionID")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/part/sessionID").and_then(Value::as_str))
}

fn parse_tool_use(part: &Value) -> Option<AgentEvent> {
    let name = part.get("tool").and_then(Value::as_str)?;
    let input = part.pointer("/state/input").cloned().unwrap_or(Value::Null);
    match name {
        "write" | "edit" => {
            let path = input.get("path").and_then(Value::as_str)?;
            Some(AgentEvent::FileChange {
                paths: vec![path.to_string()],
            })
        }
        "bash" => Some(AgentEvent::Command {
            summary: bound_summary(input.get("command").and_then(Value::as_str).unwrap_or("")),
            exit_code: part
                .pointer("/state/metadata")
                .and_then(|metadata| json_i32(metadata, "exit")),
        }),
        other => Some(AgentEvent::ToolCall {
            name: other.to_string(),
            summary: bound_summary(&input.to_string()),
        }),
    }
}

fn result_candidates(stream: &str) -> Vec<String> {
    let mut texts = Vec::new();
    for line in stream.lines() {
        let Some(value) = parse_json_line(line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        if let Some(text) = value.pointer("/part/text").and_then(Value::as_str) {
            texts.push(text.to_string());
        }
    }
    texts.reverse();
    texts
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(parts: &[&str]) -> Vec<OsString> {
        parts.iter().map(OsString::from).collect()
    }

    #[test]
    fn launch_dialect_reads_the_flag_only_right_after_run() {
        assert_eq!(
            launch_dialect(&argv(&["opencode", "run", "--format", "json"])),
            Some(OpencodeDialect::V1)
        );
        assert_eq!(
            launch_dialect(&argv(&["opencode", "run", "--standalone", "--format"])),
            Some(OpencodeDialect::V2)
        );
        // The helper execs the resolved path; only the file name identifies
        // the agent.
        assert_eq!(
            launch_dialect(&argv(&[
                "/opt/homebrew/bin/opencode",
                "run",
                "--standalone"
            ])),
            Some(OpencodeDialect::V2)
        );
        // A later `--standalone` is a model or prompt value, not the flag.
        assert_eq!(
            launch_dialect(&argv(&[
                "opencode",
                "run",
                "--format",
                "json",
                "--auto",
                "--model",
                "--standalone",
            ])),
            Some(OpencodeDialect::V1)
        );
        for other in [
            &["codex", "exec", "--standalone"][..],
            &["opencode", "session", "delete", "ses_1", "--standalone"][..],
            &["opencode-v2", "run", "--standalone"][..],
            &["opencode"][..],
            &[][..],
        ] {
            assert_eq!(launch_dialect(&argv(other)), None, "{other:?}");
        }
    }

    #[test]
    fn launch_dialect_matches_what_each_adapter_builds() {
        for dialect in [OpencodeDialect::V1, OpencodeDialect::V2] {
            for (model, session) in [
                (None, None),
                (Some("opencode/big-pickle"), None),
                (Some("--standalone"), Some("ses_1")),
            ] {
                let mut launch = vec![OsString::from(BINARY)];
                launch.extend(
                    opencode_args(dialect, model, session)
                        .into_iter()
                        .map(OsString::from),
                );
                launch.push(OsString::from(super::super::PROMPT_POINTER));
                assert_eq!(launch_dialect(&launch), Some(dialect), "{launch:?}");
            }
        }
    }
}
