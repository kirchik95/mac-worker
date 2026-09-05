use serde_json::Value;

use crate::process::ProcessResult;

use super::{
    AdapterError, AgentAdapter, AgentEvent, AgentKind, AuthProbe, AuthProbeResult,
    StructuredResult, TurnLaunch, TurnParams, argv_pointer_launch, bound_summary, combined_output,
    json_i32, parse_json_line, require_session_ref, resolve_trailer_result, validate_params,
};

pub(super) struct OpencodeAdapter;

impl AgentAdapter for OpencodeAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Opencode
    }

    fn binary(&self) -> &'static str {
        "opencode"
    }

    fn auth_probe(&self) -> AuthProbe {
        AuthProbe::new(&["auth", "list"], classify_opencode_auth)
    }

    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        Ok(argv_pointer_launch(
            self.binary(),
            opencode_args(params.model.as_deref(), None),
            Vec::new(),
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
            opencode_args(params.model.as_deref(), Some(session_ref)),
            Vec::new(),
            params.policy,
        ))
    }

    fn delete_session(&self, session_ref: &str) -> Option<Vec<String>> {
        let session_ref = require_session_ref(session_ref).ok()?;
        Some(vec![
            self.binary().into(),
            "session".into(),
            "delete".into(),
            session_ref.into(),
        ])
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
        Ok(resolve_trailer_result(
            last_message_file,
            &result_candidates(stream),
        ))
    }
}

fn classify_opencode_auth(result: &ProcessResult) -> AuthProbeResult {
    let text = combined_output(result);
    let trimmed = text.trim();
    if trimmed.is_empty()
        || trimmed.to_ascii_lowercase().contains("no credentials")
        || trimmed.to_ascii_lowercase().contains("not authenticated")
    {
        return AuthProbeResult::Unauthenticated;
    }

    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return AuthProbeResult::Unknown;
    };
    match value {
        Value::Array(values) if values.is_empty() => AuthProbeResult::Unauthenticated,
        Value::Array(_) => AuthProbeResult::Authenticated,
        Value::Object(values) if values.is_empty() => AuthProbeResult::Unauthenticated,
        Value::Object(_) => AuthProbeResult::Authenticated,
        _ => AuthProbeResult::Unknown,
    }
}

fn opencode_args(model: Option<&str>, session_ref: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "run".into(),
        "--format".into(),
        "json".into(),
        "--auto".into(),
    ];
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
