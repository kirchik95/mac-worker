use serde_json::Value;

use super::{
    AdapterError, AgentAdapter, AgentEvent, AgentKind, PermissionPolicy, PromptDelivery,
    StructuredResult, TurnLaunch, TurnParams, bound_summary, json_i32, parse_json_line,
    require_session_ref, resolve_structured_result, validate_params,
};

const SCHEMA_PLACEHOLDER: &str = "{schema}";
const LAST_MESSAGE_PLACEHOLDER: &str = "{last_message}";

pub(super) struct CodexAdapter;

impl AgentAdapter for CodexAdapter {
    fn kind(&self) -> AgentKind {
        AgentKind::Codex
    }

    fn binary(&self) -> &'static str {
        "codex"
    }

    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError> {
        validate_params(params)?;
        let mut args = vec![
            "exec".into(),
            "--json".into(),
            "-o".into(),
            LAST_MESSAGE_PLACEHOLDER.into(),
            "--output-schema".into(),
            SCHEMA_PLACEHOLDER.into(),
        ];
        push_model(&mut args, params);
        push_effort(&mut args, params);
        push_first_turn_policy(&mut args, params.policy);
        args.push("-".into());
        Ok(TurnLaunch::new(
            self.binary(),
            args,
            PromptDelivery::Stdin,
            Vec::new(),
            false,
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
            "exec".into(),
            "resume".into(),
            session_ref.to_string(),
            "--json".into(),
            "-o".into(),
            LAST_MESSAGE_PLACEHOLDER.into(),
            "--output-schema".into(),
            SCHEMA_PLACEHOLDER.into(),
        ];
        push_model(&mut args, params);
        push_effort(&mut args, params);
        push_resume_policy(&mut args, params.policy);
        args.push("-".into());
        Ok(TurnLaunch::new(
            self.binary(),
            args,
            PromptDelivery::Stdin,
            Vec::new(),
            false,
        ))
    }

    fn delete_session(&self, session_ref: &str) -> Option<Vec<String>> {
        let session_ref = require_session_ref(session_ref).ok()?;
        Some(vec![
            self.binary().into(),
            "delete".into(),
            "--force".into(),
            session_ref.into(),
        ])
    }

    fn parse_event(&self, line: &str) -> Option<AgentEvent> {
        let value = parse_json_line(line)?;
        match value.get("type").and_then(Value::as_str)? {
            "thread.started" => Some(AgentEvent::SessionStarted {
                session_ref: value.get("thread_id").and_then(Value::as_str)?.to_string(),
            }),
            "item.completed" => parse_completed_item(value.get("item")?),
            "turn.completed" => Some(AgentEvent::TurnEnd {
                reason: "completed".into(),
            }),
            "error" => Some(AgentEvent::TurnEnd {
                reason: value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("error")
                    .to_string(),
            }),
            _ => None,
        }
    }

    fn quiet_event_keys(&self) -> &'static [&'static str] {
        // `item.updated` is the streaming patch Codex emits while an item
        // is still growing (token-by-token assistant text). The completed
        // item is already rendered; the patches would only repeat it.
        &["item.updated"]
    }

    fn extract_result(
        &self,
        stream: &str,
        last_message_file: Option<&str>,
    ) -> Result<StructuredResult, AdapterError> {
        Ok(resolve_structured_result(
            last_message_file,
            &assistant_candidates(stream),
        ))
    }
}

fn push_model(args: &mut Vec<String>, params: &TurnParams) {
    if let Some(model) = &params.model {
        args.push("-m".into());
        args.push(model.clone());
    }
}

/// Codex takes reasoning effort as a config override rather than a flag. The
/// value is validated by `validate_params` before it reaches this point.
fn push_effort(args: &mut Vec<String>, params: &TurnParams) {
    if let Some(effort) = &params.effort {
        args.push("-c".into());
        args.push(format!("model_reasoning_effort=\"{effort}\""));
    }
}

fn push_first_turn_policy(args: &mut Vec<String>, policy: PermissionPolicy) {
    match policy {
        PermissionPolicy::Workspace => {
            args.extend([
                "-s".into(),
                "workspace-write".into(),
                "-c".into(),
                "approval_policy=\"never\"".into(),
                "-c".into(),
                "sandbox_workspace_write.network_access=true".into(),
            ]);
        }
        PermissionPolicy::Unattended => {
            args.push("--dangerously-bypass-approvals-and-sandbox".into());
        }
    }
}

fn push_resume_policy(args: &mut Vec<String>, policy: PermissionPolicy) {
    match policy {
        PermissionPolicy::Workspace => {
            args.extend([
                "-c".into(),
                "sandbox_mode=\"workspace-write\"".into(),
                "-c".into(),
                "sandbox_workspace_write.network_access=true".into(),
                "-c".into(),
                "approval_policy=\"never\"".into(),
            ]);
        }
        PermissionPolicy::Unattended => {
            args.extend([
                "-c".into(),
                "sandbox_mode=\"danger-full-access\"".into(),
                "-c".into(),
                "approval_policy=\"never\"".into(),
            ]);
        }
    }
}

fn parse_completed_item(item: &Value) -> Option<AgentEvent> {
    match item.get("type").and_then(Value::as_str)? {
        "agent_message" => Some(AgentEvent::AssistantMessage {
            text: item.get("text").and_then(Value::as_str)?.to_string(),
        }),
        "command_execution" => Some(AgentEvent::Command {
            summary: bound_summary(item.get("command").and_then(Value::as_str).unwrap_or("")),
            exit_code: json_i32(item, "exit_code"),
        }),
        "file_change" => {
            let paths = item
                .get("changes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|change| {
                    change
                        .get("path")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect();
            Some(AgentEvent::FileChange { paths })
        }
        "mcp_tool_call" => Some(AgentEvent::ToolCall {
            name: item.get("tool").and_then(Value::as_str)?.to_string(),
            summary: bound_summary(
                &item
                    .get("arguments")
                    .cloned()
                    .unwrap_or(Value::Null)
                    .to_string(),
            ),
        }),
        other => Some(AgentEvent::ToolCall {
            name: other.to_string(),
            summary: bound_summary(&item.to_string()),
        }),
    }
}

fn assistant_candidates(stream: &str) -> Vec<String> {
    let mut texts = Vec::new();
    for line in stream.lines() {
        let Some(value) = parse_json_line(line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some("item.completed") {
            continue;
        }
        let Some(item) = value.get("item") else {
            continue;
        };
        if item.get("type").and_then(Value::as_str) != Some("agent_message") {
            continue;
        }
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            texts.push(text.to_string());
        }
    }
    texts.reverse();
    texts
}
