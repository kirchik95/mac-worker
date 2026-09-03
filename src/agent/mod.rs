mod claude;
mod codex;
mod cursor;
mod opencode;

use claude::ClaudeAdapter;
use codex::CodexAdapter;
use cursor::CursorAdapter;
use opencode::OpencodeAdapter;
use serde_json::Value;

pub const MAX_SUMMARY_BYTES: usize = 512;
pub const RESULT_SCHEMA_JSON: &str = r#"{"type":"object","properties":{"status":{"enum":["done","needs_input","blocked"]},"summary":{"type":"string"},"questions":{"type":"array","items":{"type":"string"}},"files_changed":{"type":"array","items":{"type":"string"}}},"required":["status","summary"],"additionalProperties":false}"#;
pub const TURN_DIR_ENV: &str = "MAC_WORKER_TURN_DIR";
pub const SCHEMA_FILE_NAME: &str = "result.schema.json";
pub const LAST_MESSAGE_FILE_NAME: &str = "last.md";
pub const PROMPT_FILE_NAME: &str = "prompt.md";
pub const PROMPT_POINTER: &str = "Read the task from $MAC_WORKER_TURN_DIR/prompt.md and follow it.";

const SCHEMA_PLACEHOLDER: &str = "{schema}";
const LAST_MESSAGE_PLACEHOLDER: &str = "{last_message}";
const RESULT_TRAILER_TAG: &str = "mac-worker-result";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentKind {
    Codex,
    Claude,
    Cursor,
    Opencode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionPolicy {
    Workspace,
    Unattended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptDelivery {
    Stdin,
    ArgvPointer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLimits {
    pub timeout_millis: u64,
    pub max_turns: Option<u32>,
    pub max_budget_usd_cents: Option<u64>,
}

impl TurnLimits {
    pub const MAX_TIMEOUT_MILLIS: u64 = 24 * 60 * 60 * 1000;
    pub const MAX_BUDGET_USD_CENTS: u64 = 100_000;

    pub fn new(
        timeout_millis: u64,
        max_turns: Option<u32>,
        max_budget_usd_cents: Option<u64>,
    ) -> Result<Self, AdapterError> {
        let limits = Self {
            timeout_millis,
            max_turns,
            max_budget_usd_cents,
        };
        limits.validate()?;
        Ok(limits)
    }

    pub fn validate(&self) -> Result<(), AdapterError> {
        if self.timeout_millis == 0 {
            return Err(AdapterError::new("timeout must be greater than zero"));
        }
        if self.timeout_millis > Self::MAX_TIMEOUT_MILLIS {
            return Err(AdapterError::new("timeout must not exceed 24 hours"));
        }
        if self
            .max_budget_usd_cents
            .is_some_and(|budget| budget > Self::MAX_BUDGET_USD_CENTS)
        {
            return Err(AdapterError::new("budget must not exceed 100000 cents"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnParams {
    pub kind: AgentKind,
    pub model: Option<String>,
    pub policy: PermissionPolicy,
    pub limits: TurnLimits,
    pub session_seed: uuid::Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLaunch {
    program: String,
    args: Vec<String>,
    prompt_delivery: PromptDelivery,
    env_names: Vec<&'static str>,
    permission_fallback: bool,
}

impl TurnLaunch {
    pub fn new(
        program: impl Into<String>,
        args: Vec<String>,
        prompt_delivery: PromptDelivery,
        env_names: Vec<&'static str>,
        permission_fallback: bool,
    ) -> Self {
        Self {
            program: program.into(),
            args,
            prompt_delivery,
            env_names,
            permission_fallback,
        }
    }

    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }

    pub fn prompt_delivery(&self) -> PromptDelivery {
        self.prompt_delivery
    }

    pub fn env_names(&self) -> &[&'static str] {
        &self.env_names
    }

    pub fn permission_fallback(&self) -> bool {
        self.permission_fallback
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentEvent {
    AssistantMessage {
        text: String,
    },
    ToolCall {
        name: String,
        summary: String,
    },
    FileChange {
        paths: Vec<String>,
    },
    Command {
        summary: String,
        exit_code: Option<i32>,
    },
    Usage {
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cost_usd_cents: Option<u64>,
    },
    SessionStarted {
        session_ref: String,
    },
    TurnEnd {
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultStatus {
    Done,
    NeedsInput,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredResult {
    status: ResultStatus,
    summary: String,
    questions: Vec<String>,
    files_changed: Vec<String>,
}

impl StructuredResult {
    pub fn status(&self) -> ResultStatus {
        self.status
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn questions(&self) -> &[String] {
        &self.questions
    }

    pub fn files_changed(&self) -> &[String] {
        &self.files_changed
    }

    fn unknown() -> Self {
        Self {
            status: ResultStatus::Unknown,
            summary: String::new(),
            questions: Vec::new(),
            files_changed: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentOutcome {
    Done,
    NeedsInput,
    Blocked,
    Unknown,
    Failed { exit_code: u8 },
    Signalled,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct AdapterError(String);

impl AdapterError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

pub trait AgentAdapter: Send + Sync {
    fn kind(&self) -> AgentKind;
    fn binary(&self) -> &'static str;
    fn first_turn(&self, params: &TurnParams) -> Result<TurnLaunch, AdapterError>;
    fn resume_turn(
        &self,
        params: &TurnParams,
        session_ref: &str,
    ) -> Result<TurnLaunch, AdapterError>;
    fn prebind_session(&self) -> Option<Vec<String>> {
        None
    }
    fn parse_event(&self, line: &str) -> Option<AgentEvent>;
    fn session_ref(&self, events: &[AgentEvent]) -> Option<String> {
        events.iter().find_map(|event| match event {
            AgentEvent::SessionStarted { session_ref } => Some(session_ref.clone()),
            _ => None,
        })
    }
    fn extract_result(
        &self,
        stream: &str,
        last_message_file: Option<&str>,
    ) -> Result<StructuredResult, AdapterError>;
    fn classify(&self, exit_code: Option<i32>, status: ResultStatus) -> AgentOutcome {
        match exit_code {
            None => AgentOutcome::Signalled,
            Some(0) => match status {
                ResultStatus::Done => AgentOutcome::Done,
                ResultStatus::NeedsInput => AgentOutcome::NeedsInput,
                ResultStatus::Blocked => AgentOutcome::Blocked,
                ResultStatus::Unknown => AgentOutcome::Unknown,
            },
            Some(code) => AgentOutcome::Failed {
                exit_code: u8::try_from(code).unwrap_or(1),
            },
        }
    }
}

pub fn adapter_for(kind: AgentKind) -> &'static dyn AgentAdapter {
    match kind {
        AgentKind::Codex => &CodexAdapter,
        AgentKind::Claude => &ClaudeAdapter,
        AgentKind::Cursor => &CursorAdapter,
        AgentKind::Opencode => &OpencodeAdapter,
    }
}

pub fn render_shell(launch: &TurnLaunch) -> Result<String, AdapterError> {
    let mut parts = Vec::with_capacity(1 + launch.args.len());
    parts.push(single_quote(launch.program())?);
    let pointer_index = match launch.prompt_delivery() {
        PromptDelivery::ArgvPointer => launch.args().len().checked_sub(1),
        PromptDelivery::Stdin => None,
    };
    for (index, argument) in launch.args().iter().enumerate() {
        if pointer_index == Some(index) {
            parts.push(double_quote(argument)?);
        } else {
            parts.push(render_argument(argument)?);
        }
    }
    Ok(format!("exec {}", parts.join(" ")))
}

fn render_argument(argument: &str) -> Result<String, AdapterError> {
    match argument {
        SCHEMA_PLACEHOLDER => Ok(format!("\"${TURN_DIR_ENV}/{SCHEMA_FILE_NAME}\"")),
        LAST_MESSAGE_PLACEHOLDER => Ok(format!("\"${TURN_DIR_ENV}/{LAST_MESSAGE_FILE_NAME}\"")),
        other => single_quote(other),
    }
}

fn single_quote(argument: &str) -> Result<String, AdapterError> {
    if argument.contains('\0') {
        return Err(AdapterError::new("argument contains a NUL byte"));
    }
    let mut quoted = String::with_capacity(argument.len() + 2);
    quoted.push('\'');
    for character in argument.chars() {
        if character == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(character);
        }
    }
    quoted.push('\'');
    Ok(quoted)
}

fn double_quote(argument: &str) -> Result<String, AdapterError> {
    if argument.contains('\0') {
        return Err(AdapterError::new("argument contains a NUL byte"));
    }
    let mut quoted = String::with_capacity(argument.len() + 2);
    quoted.push('"');
    for character in argument.chars() {
        if character == '"' || character == '\\' {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    Ok(quoted)
}

fn argv_pointer_launch(
    program: impl Into<String>,
    mut args: Vec<String>,
    env_names: Vec<&'static str>,
    policy: PermissionPolicy,
) -> TurnLaunch {
    args.push(PROMPT_POINTER.to_string());
    TurnLaunch::new(
        program,
        args,
        PromptDelivery::ArgvPointer,
        env_names,
        policy == PermissionPolicy::Workspace,
    )
}

fn resolve_trailer_result(
    last_message_file: Option<&str>,
    stream_candidates: &[String],
) -> StructuredResult {
    for text in last_message_file
        .into_iter()
        .chain(stream_candidates.iter().map(String::as_str))
    {
        if let Some(result) = parse_trailer_result(text) {
            return result;
        }
    }
    StructuredResult::unknown()
}

fn parse_trailer_result(text: &str) -> Option<StructuredResult> {
    parse_structured_result(last_trailer_block(text)?)
}

fn last_trailer_block(text: &str) -> Option<&str> {
    let open = format!("```{RESULT_TRAILER_TAG}");
    let start = text.rfind(&open)?;
    let after_tag = &text[start + open.len()..];
    let after_tag = after_tag.trim_start_matches([' ', '\t']);
    let body = after_tag
        .strip_prefix("\r\n")
        .or_else(|| after_tag.strip_prefix('\n'))?;
    let end = body.find("```")?;
    Some(body[..end].trim())
}

fn require_session_ref(session_ref: &str) -> Result<&str, AdapterError> {
    if session_ref.is_empty() {
        return Err(AdapterError::new("session reference is unbound"));
    }
    Ok(session_ref)
}

fn validate_params(params: &TurnParams) -> Result<(), AdapterError> {
    params.limits.validate()
}

fn format_usd_cents(cents: u64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

fn bound_summary(input: &str) -> String {
    truncate_bytes(&escape_controls(input), MAX_SUMMARY_BYTES)
}

fn escape_controls(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other if other.is_control() => {
                escaped.push_str(&format!("\\u{{{:04x}}}", u32::from(other)));
            }
            other => escaped.push(other),
        }
    }
    escaped
}

fn truncate_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    input[..end].to_string()
}

fn parse_json_line(line: &str) -> Option<Value> {
    serde_json::from_str(line.trim()).ok()
}

fn resolve_structured_result(
    last_message_file: Option<&str>,
    stream_candidates: &[String],
) -> StructuredResult {
    for text in last_message_file
        .into_iter()
        .chain(stream_candidates.iter().map(String::as_str))
    {
        if let Some(result) = parse_structured_result(text) {
            return result;
        }
    }
    StructuredResult::unknown()
}

fn parse_structured_result(text: &str) -> Option<StructuredResult> {
    let value = extract_json_object(text)?;
    let status = match value.get("status").and_then(Value::as_str) {
        Some("done") => ResultStatus::Done,
        Some("needs_input") => ResultStatus::NeedsInput,
        Some("blocked") => ResultStatus::Blocked,
        _ => ResultStatus::Unknown,
    };
    Some(StructuredResult {
        status,
        summary: value
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        questions: string_array(&value, "questions"),
        files_changed: string_array(&value, "files_changed"),
    })
}

fn extract_json_object(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed)
        && value.is_object()
    {
        return Some(value);
    }
    let start = trimmed.find('{')?;
    let mut deserializer =
        serde_json::Deserializer::from_str(&trimmed[start..]).into_iter::<Value>();
    match deserializer.next()? {
        Ok(value) if value.is_object() => Some(value),
        _ => None,
    }
}

fn string_array(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn json_i32(value: &Value, key: &str) -> Option<i32> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .and_then(|n| i32::try_from(n).ok())
}
