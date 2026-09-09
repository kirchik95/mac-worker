use serde_json::Value;

use crate::keychain::KEYCHAIN_LOCKED_REASON;
use crate::process::ProcessResult;

use super::{
    AdapterError, AgentAdapter, AgentEvent, AgentKind, AuthProbe, AuthProbeResult,
    StructuredResult, TurnLaunch, TurnParams, argv_pointer_launch, bound_summary, combined_output,
    parse_json_line, require_session_ref, resolve_last_structured_result, strip_ansi,
    validate_params,
};

const ENV_NAMES: [&str; 1] = ["CURSOR_API_KEY"];
/// `cursor-agent status` can print `Logged in (...)` when a credential exists
/// but cannot be used. The reason is interned so facts.json round-trips it.
pub(crate) const LOGIN_UNVERIFIED_REASON: &str = "login unverified: user details unavailable";

pub(super) struct CursorAdapter;

impl AgentAdapter for CursorAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Cursor
    }

    fn binary(&self) -> &'static str {
        "cursor-agent"
    }

    fn auth_probe(&self) -> AuthProbe {
        AuthProbe::new(&["status"], classify_cursor_auth)
    }

    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        Ok(argv_pointer_launch(
            self.binary(),
            cursor_args(params.model.as_deref(), &params.session_seed.to_string()),
            ENV_NAMES.to_vec(),
            params.policy,
        ))
    }

    fn resume_turn(
        &self,
        params: &TurnParams,
        session_ref: &str,
    ) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        let session_ref = require_session_ref(session_ref)?;
        Ok(argv_pointer_launch(
            self.binary(),
            cursor_args(params.model.as_deref(), session_ref),
            ENV_NAMES.to_vec(),
            params.policy,
        ))
    }

    fn prebind_session(&self) -> Option<Vec<String>> {
        Some(vec![self.binary().to_string(), "create-chat".into()])
    }

    fn delete_session(&self, _session_ref: &str) -> Option<Vec<String>> {
        None
    }

    fn parse_event(&self, line: &str) -> Option<AgentEvent> {
        let value = parse_json_line(line)?;
        match value.get("type").and_then(Value::as_str)? {
            "system" if value.get("subtype").and_then(Value::as_str) == Some("init") => {
                Some(AgentEvent::SessionStarted {
                    session_ref: value.get("session_id").and_then(Value::as_str)?.to_string(),
                })
            }
            "assistant" => parse_assistant(&value),
            "tool_call" if value.get("subtype").and_then(Value::as_str) == Some("started") => {
                parse_tool_call(value.get("tool_call")?)
            }
            "result" => Some(AgentEvent::TurnEnd {
                reason: value
                    .get("subtype")
                    .and_then(Value::as_str)
                    .unwrap_or("completed")
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
        Ok(resolve_last_structured_result(
            last_message_file,
            &result_candidates(stream),
        ))
    }
}

fn classify_cursor_auth(result: &ProcessResult) -> AuthProbeResult {
    let text = strip_ansi(&combined_output(result));
    if text.lines().any(|line| {
        line.to_ascii_lowercase()
            .contains("macos login keychain is locked")
    }) {
        return AuthProbeResult::UnknownWithReason(KEYCHAIN_LOCKED_REASON);
    }
    // Every non-empty line must be a recognised status line. Authenticated
    // and unverified may share a status dump (`Login successful!` plus
    // `Logged in (unable to fetch user details)`); unverified wins so a
    // degraded credential is never advertised as ready. Any other mix is
    // ambiguous and stays unknown.
    let mut verdict = None;
    for line in text.lines().map(normalize_status_line) {
        if line.is_empty() {
            continue;
        }
        let Some(classified) = classify_status_line(&line) else {
            return AuthProbeResult::Unknown;
        };
        match merge_status_line(verdict, classified) {
            Some(merged) => verdict = Some(merged),
            None => return AuthProbeResult::Unknown,
        }
    }
    match verdict {
        Some(StatusLine::Authenticated) => AuthProbeResult::Authenticated,
        Some(StatusLine::Unauthenticated) => AuthProbeResult::Unauthenticated,
        Some(StatusLine::Unverified) => AuthProbeResult::UnknownWithReason(LOGIN_UNVERIFIED_REASON),
        None => AuthProbeResult::Unknown,
    }
}

fn normalize_status_line(line: &str) -> String {
    line.trim()
        .trim_start_matches(|character: char| !character.is_alphanumeric())
        .trim_end_matches(['!', '.'])
        .trim()
        .to_ascii_lowercase()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StatusLine {
    Authenticated,
    Unauthenticated,
    Unverified,
}

fn merge_status_line(previous: Option<StatusLine>, next: StatusLine) -> Option<StatusLine> {
    match previous {
        None => Some(next),
        Some(same) if same == next => Some(same),
        Some(StatusLine::Authenticated) if next == StatusLine::Unverified => {
            Some(StatusLine::Unverified)
        }
        Some(StatusLine::Unverified) if next == StatusLine::Authenticated => {
            Some(StatusLine::Unverified)
        }
        Some(_) => None,
    }
}

fn classify_status_line(line: &str) -> Option<StatusLine> {
    if matches!(line, "unauthenticated" | "authenticated: false")
        || line.starts_with("not logged in")
        || line.starts_with("not authenticated")
    {
        return Some(StatusLine::Unauthenticated);
    }
    if let Some(detail) = line.strip_prefix("logged in (") {
        return Some(if parenthetical_names_a_failure(detail) {
            StatusLine::Unverified
        } else {
            StatusLine::Authenticated
        });
    }
    if matches!(
        line,
        "authenticated" | "logged in" | "authenticated: true" | "login successful"
    ) || line
        .strip_prefix("authenticated as ")
        .is_some_and(|user| !user.trim().is_empty())
        || line
            .strip_prefix("logged in as ")
            .is_some_and(|user| !user.trim().is_empty())
    {
        return Some(StatusLine::Authenticated);
    }
    None
}

fn parenthetical_names_a_failure(detail: &str) -> bool {
    let detail = detail.strip_suffix(')').unwrap_or(detail);
    ["unable", "failed", "error", "expired"]
        .iter()
        .any(|marker| detail.contains(marker))
}

fn cursor_args(model: Option<&str>, session_ref: &str) -> Vec<String> {
    let mut args = vec![
        "-p".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--trust".into(),
        "--force".into(),
    ];
    if let Some(model) = model {
        args.push("--model".into());
        args.push(model.to_string());
    }
    args.push("--resume".into());
    args.push(session_ref.to_string());
    args
}

fn parse_assistant(value: &Value) -> Option<AgentEvent> {
    let text = message_text(value)?;
    Some(AgentEvent::AssistantMessage { text })
}

fn message_text(value: &Value) -> Option<String> {
    let content = value
        .pointer("/message/content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let text = content
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("");
    if text.is_empty() { None } else { Some(text) }
}

fn parse_tool_call(tool_call: &Value) -> Option<AgentEvent> {
    let object = tool_call.as_object()?;
    let (name, body) = object.iter().next()?;
    match name.as_str() {
        "writeToolCall" | "editToolCall" => {
            let path = body
                .pointer("/args/path")
                .and_then(Value::as_str)
                .or_else(|| body.pointer("/result/success/path").and_then(Value::as_str))?;
            Some(AgentEvent::FileChange {
                paths: vec![path.to_string()],
            })
        }
        "shellToolCall" | "bashToolCall" => Some(AgentEvent::Command {
            summary: bound_summary(
                body.pointer("/args/command")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
            ),
            exit_code: None,
        }),
        "function" => Some(AgentEvent::ToolCall {
            name: body.get("name").and_then(Value::as_str)?.to_string(),
            summary: bound_summary(body.get("arguments").and_then(Value::as_str).unwrap_or("")),
        }),
        other => Some(AgentEvent::ToolCall {
            name: other.strip_suffix("ToolCall").unwrap_or(other).to_string(),
            summary: bound_summary(&body.get("args").cloned().unwrap_or(Value::Null).to_string()),
        }),
    }
}

fn result_candidates(stream: &str) -> Vec<String> {
    let mut result_texts = Vec::new();
    let mut assistant_texts = Vec::new();
    for line in stream.lines() {
        let Some(value) = parse_json_line(line) else {
            continue;
        };
        match value.get("type").and_then(Value::as_str) {
            Some("result") => {
                if let Some(text) = value.get("result").and_then(Value::as_str) {
                    result_texts.push(text.to_string());
                }
            }
            Some("assistant") => {
                if let Some(text) = message_text(&value) {
                    assistant_texts.push(text);
                }
            }
            _ => {}
        }
    }
    result_texts.reverse();
    assistant_texts.reverse();
    result_texts.extend(assistant_texts);
    result_texts
}
