use serde_json::Value;

use super::{
    AdapterError, AgentAdapter, AgentEvent, AgentKind, PermissionPolicy, PromptDelivery,
    RESULT_SCHEMA_JSON, StructuredResult, TurnLaunch, TurnParams, bound_summary, format_usd_cents,
    parse_json_line, require_session_ref, resolve_structured_result, validate_params,
};

const ENV_NAMES: [&str; 2] = ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"];

pub(super) struct ClaudeAdapter;

impl AgentAdapter for ClaudeAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Claude
    }

    fn binary(&self) -> &'static str {
        "claude"
    }

    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        let mut args = vec![
            "-p".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--session-id".into(),
            params.session_seed.to_string(),
            "--json-schema".into(),
            RESULT_SCHEMA_JSON.into(),
        ];
        push_optional_limits(&mut args, params);
        args.extend(["--permission-mode".into(), "bypassPermissions".into()]);
        Ok(TurnLaunch::new(
            self.binary(),
            args,
            PromptDelivery::Stdin,
            ENV_NAMES.to_vec(),
            params.policy == PermissionPolicy::Workspace,
        ))
    }

    fn resume_turn(
        &self,
        params: &TurnParams,
        session_ref: &str,
    ) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        let session_ref = require_session_ref(session_ref)?;
        let mut args = vec![
            "-p".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--resume".into(),
            session_ref.to_string(),
            "--json-schema".into(),
            RESULT_SCHEMA_JSON.into(),
        ];
        push_optional_limits(&mut args, params);
        args.extend(["--permission-mode".into(), "bypassPermissions".into()]);
        Ok(TurnLaunch::new(
            self.binary(),
            args,
            PromptDelivery::Stdin,
            ENV_NAMES.to_vec(),
            params.policy == PermissionPolicy::Workspace,
        ))
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
        Ok(resolve_structured_result(
            last_message_file,
            &result_candidates(stream),
        ))
    }
}

fn push_optional_limits(args: &mut Vec<String>, params: &TurnParams) {
    if let Some(model) = &params.model {
        args.push("--model".into());
        args.push(model.clone());
    }
    if let Some(max_turns) = params.limits.max_turns {
        args.push("--max-turns".into());
        args.push(max_turns.to_string());
    }
    if let Some(cents) = params.limits.max_budget_usd_cents {
        args.push("--max-budget-usd".into());
        args.push(format_usd_cents(cents));
    }
}

fn parse_assistant(value: &Value) -> Option<AgentEvent> {
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
    if !text.is_empty() {
        return Some(AgentEvent::AssistantMessage { text });
    }
    content.into_iter().find_map(|part| {
        if part.get("type").and_then(Value::as_str) != Some("tool_use") {
            return None;
        }
        Some(AgentEvent::ToolCall {
            name: part.get("name").and_then(Value::as_str)?.to_string(),
            summary: bound_summary(
                &part
                    .get("input")
                    .cloned()
                    .unwrap_or(Value::Null)
                    .to_string(),
            ),
        })
    })
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
                if let Some(AgentEvent::AssistantMessage { text }) = parse_assistant(&value) {
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
