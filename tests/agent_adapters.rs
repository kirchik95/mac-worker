use std::fs;

use mac_worker::agent::{
    AgentEvent, AgentKind, AgentOutcome, PROMPT_POINTER, PermissionPolicy, PromptDelivery,
    RESULT_SCHEMA_JSON, ResultStatus, TurnLaunch, TurnLimits, TurnParams, adapter_for,
    render_shell,
};
use uuid::Uuid;

const DEFAULT_TIMEOUT_MILLIS: u64 = 45 * 60 * 1000;
const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;
const SESSION_PLACEHOLDER: &str = "0d3c…";

fn params(policy: PermissionPolicy) -> TurnParams {
    TurnParams {
        kind: AgentKind::Codex,
        model: None,
        policy,
        limits: TurnLimits::new(DEFAULT_TIMEOUT_MILLIS, None, None).unwrap(),
        session_seed: Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001),
    }
}

// Claude fixtures are hand-derived from the documented stream-json
// envelope (system/init, assistant, result). They were not captured
// from a live Claude Code run.
fn fixture(name: &str) -> String {
    fs::read_to_string(format!("tests/fixtures/agents/{name}"))
        .unwrap_or_else(|error| panic!("fixture {name} must be readable: {error}"))
}

fn opencode_fixture(name: &str) -> String {
    fs::read_to_string(format!("tests/fixtures/opencode/{name}"))
        .unwrap_or_else(|error| panic!("OpenCode fixture {name} must be readable: {error}"))
}

fn fixture_lines(name: &str) -> impl Iterator<Item = String> {
    fixture(name)
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>()
        .into_iter()
}

#[test]
fn codex_first_turn_reads_prompt_from_stdin_and_requests_schema() {
    let launch = adapter_for(AgentKind::Codex)
        .first_turn(&params(PermissionPolicy::Workspace))
        .unwrap();
    assert_eq!(launch.program(), "codex");
    assert!(launch.args().starts_with(&["exec".into(), "--json".into()]));
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "-s" && w[1] == "workspace-write")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "-c" && w[1] == "approval_policy=\"never\"")
    );
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "--approve-for-me")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--output-schema" && w[1] == "{schema}")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "-o" && w[1] == "{last_message}")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| { w[0] == "-c" && w[1] == "sandbox_workspace_write.network_access=true" })
    );
    assert_eq!(launch.args().last().map(String::as_str), Some("-"));
    assert_eq!(launch.prompt_delivery(), PromptDelivery::Stdin);
    assert!(launch.env_names().is_empty());
    assert!(!launch.permission_fallback());
}

#[test]
fn codex_unattended_first_turn_uses_full_bypass() {
    let launch = adapter_for(AgentKind::Codex)
        .first_turn(&params(PermissionPolicy::Unattended))
        .unwrap();
    assert!(
        launch
            .args()
            .contains(&"--dangerously-bypass-approvals-and-sandbox".into())
    );
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "-s" && argument != "--approve-for-me")
    );
    assert_eq!(launch.args().last().map(String::as_str), Some("-"));
}

#[test]
fn codex_resume_uses_config_sandbox_and_keeps_schema() {
    let launch = adapter_for(AgentKind::Codex)
        .resume_turn(&params(PermissionPolicy::Workspace), SESSION_PLACEHOLDER)
        .unwrap();
    assert!(launch.args().starts_with(&[
        "exec".into(),
        "resume".into(),
        SESSION_PLACEHOLDER.into()
    ]));
    for key in [
        "sandbox_mode=\"workspace-write\"",
        "sandbox_workspace_write.network_access=true",
        "approval_policy=\"never\"",
    ] {
        assert!(
            launch
                .args()
                .windows(2)
                .any(|w| w[0] == "-c" && w[1] == key),
            "{key}"
        );
    }
    assert!(
        launch
            .args()
            .iter()
            .all(|a| a != "-C" && a != "-s" && a != "--approve-for-me")
    );
    assert!(launch.args().windows(2).any(|w| w[0] == "--output-schema"));
    assert_eq!(launch.args().last().map(String::as_str), Some("-"));
}

#[test]
fn codex_unattended_resume_uses_bypass_config_equivalents() {
    let launch = adapter_for(AgentKind::Codex)
        .resume_turn(&params(PermissionPolicy::Unattended), SESSION_PLACEHOLDER)
        .unwrap();
    for key in [
        "sandbox_mode=\"danger-full-access\"",
        "approval_policy=\"never\"",
    ] {
        assert!(
            launch
                .args()
                .windows(2)
                .any(|w| w[0] == "-c" && w[1] == key),
            "{key}"
        );
    }
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "-C" && argument != "-s" && argument != "--approve-for-me")
    );
}

#[test]
fn claude_first_turn_binds_generated_session_budget_and_turns() {
    let mut params = params(PermissionPolicy::Unattended);
    params.limits.max_turns = Some(40);
    params.limits.max_budget_usd_cents = Some(1_250);
    let launch = adapter_for(AgentKind::Claude).first_turn(&params).unwrap();
    assert_eq!(launch.program(), "claude");
    assert!(launch.args().starts_with(&["-p".into()]));
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--output-format" && w[1] == "stream-json")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--session-id" && w[1] == params.session_seed.to_string())
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--json-schema" && w[1] == RESULT_SCHEMA_JSON)
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--max-turns" && w[1] == "40")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--max-budget-usd" && w[1] == "12.50")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--permission-mode" && w[1] == "bypassPermissions")
    );
    assert_eq!(
        launch.env_names(),
        &["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]
    );
    assert_eq!(launch.prompt_delivery(), PromptDelivery::Stdin);
    assert!(!launch.permission_fallback());
}

#[test]
fn claude_workspace_policy_falls_back_to_unattended() {
    let launch = adapter_for(AgentKind::Claude)
        .first_turn(&params(PermissionPolicy::Workspace))
        .unwrap();
    assert!(launch.permission_fallback());
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--permission-mode" && w[1] == "bypassPermissions")
    );
}

#[test]
fn claude_resume_uses_resume_and_never_session_id() {
    let launch = adapter_for(AgentKind::Claude)
        .resume_turn(&params(PermissionPolicy::Unattended), SESSION_PLACEHOLDER)
        .unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--resume" && w[1] == SESSION_PLACEHOLDER)
    );
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "--session-id")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--output-format" && w[1] == "stream-json")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--json-schema" && w[1] == RESULT_SCHEMA_JSON)
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--permission-mode" && w[1] == "bypassPermissions")
    );
}

#[test]
fn model_is_passed_through_for_both_agents() {
    let mut params = params(PermissionPolicy::Workspace);
    params.model = Some("gpt-test".into());
    let launch = adapter_for(AgentKind::Codex).first_turn(&params).unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "-m" && w[1] == "gpt-test")
    );

    let resume = adapter_for(AgentKind::Codex)
        .resume_turn(&params, SESSION_PLACEHOLDER)
        .unwrap();
    assert!(
        resume
            .args()
            .windows(2)
            .any(|w| w[0] == "-m" && w[1] == "gpt-test")
    );

    params.model = Some("opus".into());
    let launch = adapter_for(AgentKind::Claude).first_turn(&params).unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "opus")
    );
}

#[test]
fn resume_without_a_session_reference_is_unbound() {
    let error = adapter_for(AgentKind::Codex)
        .resume_turn(&params(PermissionPolicy::Workspace), "")
        .expect_err("an empty session reference must not launch");
    assert!(error.to_string().to_ascii_lowercase().contains("session"));
}

#[test]
fn codex_stream_yields_session_ref_and_normalized_events() {
    let adapter = adapter_for(AgentKind::Codex);
    let events: Vec<AgentEvent> = fixture_lines("codex-success.jsonl")
        .filter_map(|line| adapter.parse_event(&line))
        .collect();
    assert_eq!(
        adapter.session_ref(&events).as_deref(),
        Some(SESSION_PLACEHOLDER)
    );
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::Command {
            exit_code: Some(0),
            ..
        }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::FileChange { paths } if paths == &["src/agent/mod.rs"]
    )));
    assert!(matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));
}

#[test]
fn structured_result_is_extracted_or_unknown_never_an_error() {
    let adapter = adapter_for(AgentKind::Claude);
    assert_eq!(
        adapter
            .extract_result(&fixture("claude-success.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Done
    );
    assert_eq!(
        adapter
            .extract_result(&fixture("claude-malformed.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Unknown
    );
}

#[test]
fn structured_results_cover_needs_input_and_blocked() {
    let codex = adapter_for(AgentKind::Codex);
    let needs_input = codex
        .extract_result(&fixture("codex-needs-input.jsonl"), None)
        .unwrap();
    assert_eq!(needs_input.status(), ResultStatus::NeedsInput);
    assert_eq!(needs_input.questions(), &["Which crate should be renamed?"]);

    let claude = adapter_for(AgentKind::Claude);
    assert_eq!(
        claude
            .extract_result(&fixture("claude-blocked.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Blocked
    );
}

#[test]
fn structured_results_reject_schema_violations_as_unknown() {
    let malformed = [
        r#"{"status":"done","summary":"ok","unexpected":true}"#,
        r#"{"status":"done"}"#,
        r#"{"status":"done","summary":7}"#,
        r#"{"status":"done","summary":"ok","questions":["q",7]}"#,
        r#"{"status":"not_a_status","summary":"ok"}"#,
    ];

    for kind in [AgentKind::Codex, AgentKind::Claude] {
        let adapter = adapter_for(kind);
        for result in malformed {
            assert_eq!(
                adapter.extract_result("", Some(result)).unwrap().status(),
                ResultStatus::Unknown,
                "{kind:?} accepted {result}",
            );
        }
    }
}

#[test]
fn extract_result_prefers_the_last_message_file() {
    let adapter = adapter_for(AgentKind::Codex);
    let result = adapter
        .extract_result(
            &fixture("codex-success.jsonl"),
            Some(
                r#"{"status":"needs_input","summary":"from file","questions":["q"],"files_changed":[]}"#,
            ),
        )
        .unwrap();
    assert_eq!(result.status(), ResultStatus::NeedsInput);
    assert_eq!(result.summary(), "from file");
    assert_eq!(result.questions(), &["q"]);
}

#[test]
fn truncated_final_line_is_ignored() {
    let adapter = adapter_for(AgentKind::Codex);
    let events: Vec<AgentEvent> = fixture_lines("codex-truncated.jsonl")
        .filter_map(|line| adapter.parse_event(&line))
        .collect();
    assert_eq!(
        adapter.session_ref(&events).as_deref(),
        Some(SESSION_PLACEHOLDER)
    );
    assert!(
        adapter
            .parse_event(r#"{"type":"turn.completed","usage":{"input"#)
            .is_none()
    );
    assert!(!matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));
}

#[test]
fn tool_and_command_summaries_are_bounded_and_escape_controls() {
    let adapter = adapter_for(AgentKind::Codex);
    let command = format!("head\n{}", "x".repeat(600));
    let line = format!(
        r#"{{"type":"item.completed","item":{{"id":"item_0","type":"command_execution","command":{command},"exit_code":0}}}}"#,
        command = serde_json::to_string(&command).unwrap()
    );
    let Some(AgentEvent::Command { summary, exit_code }) = adapter.parse_event(&line) else {
        panic!("command_execution must normalize to Command");
    };
    assert_eq!(exit_code, Some(0));
    assert!(summary.len() <= 512);
    assert!(!summary.contains('\n'));
    assert!(summary.contains("\\n"));

    let tool = format!("y\t{}", "z".repeat(600));
    let line = format!(
        r#"{{"type":"item.completed","item":{{"id":"item_1","type":"mcp_tool_call","server":"docs","tool":"search","arguments":{{"q":{query}}}}}}}"#,
        query = serde_json::to_string(&tool).unwrap()
    );
    let Some(AgentEvent::ToolCall { name, summary }) = adapter.parse_event(&line) else {
        panic!("mcp_tool_call must normalize to ToolCall");
    };
    assert_eq!(name, "search");
    assert!(summary.len() <= 512);
    assert!(!summary.contains('\t'));
    assert!(summary.contains("\\t"));
}

#[test]
fn turn_limits_reject_zero_timeout_over_day_and_huge_budget() {
    assert!(TurnLimits::new(0, None, None).is_err());
    assert!(TurnLimits::new(DAY_MILLIS + 1, None, None).is_err());
    assert!(TurnLimits::new(DEFAULT_TIMEOUT_MILLIS, Some(0), None).is_err());
    assert!(TurnLimits::new(DEFAULT_TIMEOUT_MILLIS, None, Some(100_001)).is_err());
    assert!(TurnLimits::new(DEFAULT_TIMEOUT_MILLIS, Some(1), None).is_ok());
    assert!(TurnLimits::new(1, None, Some(100_000)).is_ok());
    assert!(TurnLimits::new(DAY_MILLIS, None, None).is_ok());
}

#[test]
fn result_schema_json_has_exactly_the_declared_keys() {
    let value: serde_json::Value = serde_json::from_str(RESULT_SCHEMA_JSON).unwrap();
    let mut keys: Vec<_> = value["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(keys, ["files_changed", "questions", "status", "summary"]);
}

#[test]
fn result_schema_json_is_strict_structured_output_compatible() {
    // Catches a schema that strict backends reject at the API: Codex failed
    // every live turn with `invalid_json_schema ... Missing 'questions'`
    // because `required` did not list every property.
    let value: serde_json::Value = serde_json::from_str(RESULT_SCHEMA_JSON).unwrap();
    let mut keys: Vec<_> = value["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    keys.sort();
    let mut required: Vec<String> = value["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry.as_str().unwrap().to_owned())
        .collect();
    required.sort();
    assert_eq!(required, keys);
    assert_eq!(
        value["additionalProperties"],
        serde_json::Value::Bool(false)
    );
}

#[test]
fn classify_maps_exit_and_status_without_guessing() {
    let adapter = adapter_for(AgentKind::Codex);
    assert_eq!(
        adapter.classify(Some(0), ResultStatus::Done),
        AgentOutcome::Done
    );
    assert_eq!(
        adapter.classify(Some(0), ResultStatus::NeedsInput),
        AgentOutcome::NeedsInput
    );
    assert_eq!(
        adapter.classify(Some(1), ResultStatus::Done),
        AgentOutcome::Failed { exit_code: 1 }
    );
    assert_eq!(
        adapter.classify(None, ResultStatus::Done),
        AgentOutcome::Signalled
    );
    assert_eq!(
        adapter.classify(Some(0), ResultStatus::Blocked),
        AgentOutcome::Blocked
    );
    assert_eq!(
        adapter.classify(Some(0), ResultStatus::Unknown),
        AgentOutcome::Unknown
    );
}

#[test]
fn render_shell_quotes_arguments_and_renders_file_placeholders_as_env_references() {
    let launch = adapter_for(AgentKind::Codex)
        .first_turn(&params(PermissionPolicy::Workspace))
        .unwrap();
    let shell = render_shell(&launch).unwrap();
    assert!(shell.starts_with("exec 'codex' 'exec' '--json'"));
    assert!(shell.contains("--output-schema' \"$MAC_WORKER_TURN_DIR/result.schema.json\""));
    assert!(shell.contains("'-o' \"$MAC_WORKER_TURN_DIR/last.md\""));
    assert!(!shell.contains("{schema}") && !shell.contains("'/"));
    assert_eq!(render_shell(&launch).unwrap(), shell);
}

#[test]
fn render_shell_rejects_an_argument_containing_nul() {
    let launch = TurnLaunch::new(
        "codex",
        vec!["exec".into(), "bad\0arg".into()],
        PromptDelivery::Stdin,
        Vec::new(),
        false,
    );
    let error = render_shell(&launch).expect_err("NUL must be rejected");
    assert!(error.to_string().to_ascii_lowercase().contains("nul"));
}

const CURSOR_SESSION: &str = "00000000-0000-4000-8000-0000000000c1";
const OPENCODE_SESSION: &str = "ses_PLACEHOLDER";
const OPENCODE_LIVE_SESSION: &str = "ses_f8a1b792bffeJSvEZ94X826hDO";

fn cursor_params(policy: PermissionPolicy) -> TurnParams {
    let mut params = params(policy);
    params.kind = AgentKind::Cursor;
    params
}

fn opencode_params(policy: PermissionPolicy) -> TurnParams {
    let mut params = params(policy);
    params.kind = AgentKind::Opencode;
    params
}

#[test]
fn cursor_first_turn_uses_argv_pointer_trust_and_force() {
    let params = cursor_params(PermissionPolicy::Unattended);
    let launch = adapter_for(AgentKind::Cursor).first_turn(&params).unwrap();
    assert_eq!(launch.program(), "cursor-agent");
    assert!(launch.args().starts_with(&["-p".into()]));
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--output-format" && w[1] == "stream-json")
    );
    assert!(launch.args().contains(&"--trust".into()));
    assert!(launch.args().contains(&"--force".into()));
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "--workspace" && argument != "--yolo")
    );
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--resume" && w[1] == params.session_seed.to_string())
    );
    assert_eq!(
        launch.args().last().map(String::as_str),
        Some(PROMPT_POINTER)
    );
    assert_eq!(launch.prompt_delivery(), PromptDelivery::ArgvPointer);
    assert_eq!(launch.env_names(), &["CURSOR_API_KEY"]);
    assert!(!launch.permission_fallback());
    assert!(
        !launch
            .args()
            .iter()
            .any(|argument| argument.contains("Reply"))
    );
}

#[test]
fn cursor_workspace_policy_falls_back_to_unattended() {
    let launch = adapter_for(AgentKind::Cursor)
        .first_turn(&cursor_params(PermissionPolicy::Workspace))
        .unwrap();
    assert!(launch.permission_fallback());
    assert!(launch.args().contains(&"--force".into()));
}

#[test]
fn cursor_resume_uses_bound_chat_id_and_never_workspace_path() {
    let launch = adapter_for(AgentKind::Cursor)
        .resume_turn(&cursor_params(PermissionPolicy::Unattended), CURSOR_SESSION)
        .unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--resume" && w[1] == CURSOR_SESSION)
    );
    assert!(launch.args().contains(&"--trust".into()));
    assert!(launch.args().contains(&"--force".into()));
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "--workspace" && argument != "--continue")
    );
    assert_eq!(
        launch.args().last().map(String::as_str),
        Some(PROMPT_POINTER)
    );
    assert_eq!(launch.prompt_delivery(), PromptDelivery::ArgvPointer);
}

#[test]
fn cursor_prebind_session_is_create_chat() {
    assert_eq!(
        adapter_for(AgentKind::Cursor).prebind_session(),
        Some(vec!["cursor-agent".into(), "create-chat".into()])
    );
    assert_eq!(adapter_for(AgentKind::Codex).prebind_session(), None);
    assert_eq!(adapter_for(AgentKind::Claude).prebind_session(), None);
    assert_eq!(adapter_for(AgentKind::Opencode).prebind_session(), None);
}

#[test]
fn native_session_delete_commands_match_the_documented_cli_help() {
    assert_eq!(
        adapter_for(AgentKind::Codex).delete_session("session"),
        Some(vec![
            "codex".into(),
            "delete".into(),
            "--force".into(),
            "session".into(),
        ])
    );
    assert_eq!(
        adapter_for(AgentKind::Opencode).delete_session("session"),
        Some(vec![
            "opencode".into(),
            "session".into(),
            "delete".into(),
            "session".into(),
        ])
    );
    assert_eq!(
        adapter_for(AgentKind::Claude).delete_session("session"),
        None
    );
    assert_eq!(
        adapter_for(AgentKind::Cursor).delete_session("session"),
        None
    );
}

#[test]
fn opencode_first_turn_uses_argv_pointer_and_auto() {
    let launch = adapter_for(AgentKind::Opencode)
        .first_turn(&opencode_params(PermissionPolicy::Unattended))
        .unwrap();
    assert_eq!(launch.program(), "opencode");
    assert!(
        launch
            .args()
            .starts_with(&["run".into(), "--format".into(), "json".into()])
    );
    assert!(launch.args().contains(&"--auto".into()));
    assert!(launch.args().iter().all(|argument| argument != "--dir"
        && argument != "--continue"
        && argument != "--session"));
    assert_eq!(
        launch.args().last().map(String::as_str),
        Some(PROMPT_POINTER)
    );
    assert_eq!(launch.prompt_delivery(), PromptDelivery::ArgvPointer);
    assert!(launch.env_names().is_empty());
    assert!(!launch.permission_fallback());
}

#[test]
fn opencode_workspace_policy_falls_back_to_unattended() {
    let launch = adapter_for(AgentKind::Opencode)
        .first_turn(&opencode_params(PermissionPolicy::Workspace))
        .unwrap();
    assert!(launch.permission_fallback());
    assert!(launch.args().contains(&"--auto".into()));
}

#[test]
fn opencode_resume_uses_session_and_keeps_auto() {
    let launch = adapter_for(AgentKind::Opencode)
        .resume_turn(
            &opencode_params(PermissionPolicy::Unattended),
            OPENCODE_SESSION,
        )
        .unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--session" && w[1] == OPENCODE_SESSION)
    );
    assert!(launch.args().contains(&"--auto".into()));
    assert!(
        launch
            .args()
            .iter()
            .all(|argument| argument != "--dir" && argument != "--continue")
    );
    assert_eq!(
        launch.args().last().map(String::as_str),
        Some(PROMPT_POINTER)
    );
}

#[test]
fn model_is_passed_through_for_cursor_and_opencode() {
    let mut params = cursor_params(PermissionPolicy::Unattended);
    params.model = Some("gpt-test".into());
    let launch = adapter_for(AgentKind::Cursor).first_turn(&params).unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "gpt-test")
    );

    let resume = adapter_for(AgentKind::Cursor)
        .resume_turn(&params, CURSOR_SESSION)
        .unwrap();
    assert!(
        resume
            .args()
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "gpt-test")
    );

    params = opencode_params(PermissionPolicy::Unattended);
    params.model = Some("opencode/mimo-v2.5-free".into());
    let launch = adapter_for(AgentKind::Opencode)
        .first_turn(&params)
        .unwrap();
    assert!(
        launch
            .args()
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "opencode/mimo-v2.5-free")
    );
    let resume = adapter_for(AgentKind::Opencode)
        .resume_turn(&params, OPENCODE_SESSION)
        .unwrap();
    assert!(
        resume
            .args()
            .windows(2)
            .any(|w| w[0] == "--model" && w[1] == "opencode/mimo-v2.5-free")
    );
    assert!(
        resume
            .args()
            .windows(2)
            .any(|w| w[0] == "--session" && w[1] == OPENCODE_SESSION)
    );
}

#[test]
fn cursor_and_opencode_resume_without_a_session_reference_is_unbound() {
    for kind in [AgentKind::Cursor, AgentKind::Opencode] {
        let error = adapter_for(kind)
            .resume_turn(&params(PermissionPolicy::Unattended), "")
            .expect_err("an empty session reference must not launch");
        assert!(error.to_string().to_ascii_lowercase().contains("session"));
    }
}

#[test]
fn cursor_stream_yields_session_ref_and_normalized_events() {
    let adapter = adapter_for(AgentKind::Cursor);
    let events: Vec<AgentEvent> = fixture_lines("cursor-success.jsonl")
        .filter_map(|line| adapter.parse_event(&line))
        .collect();
    assert_eq!(
        adapter.session_ref(&events).as_deref(),
        Some(CURSOR_SESSION)
    );
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolCall { name, .. } if name == "read"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::FileChange { paths } if paths == &["src/agent/cursor.rs"]
    )));
    assert!(matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));
}

#[test]
fn opencode_stream_yields_session_ref_and_normalized_events() {
    let adapter = adapter_for(AgentKind::Opencode);
    let events: Vec<AgentEvent> = fixture_lines("opencode-success.jsonl")
        .filter_map(|line| adapter.parse_event(&line))
        .collect();
    assert_eq!(
        adapter.session_ref(&events).as_deref(),
        Some(OPENCODE_SESSION)
    );
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::Command {
            exit_code: Some(0),
            ..
        }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::FileChange { paths } if paths == &["src/agent/opencode.rs"]
    )));
    assert!(matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));
}

#[test]
fn opencode_extracts_pure_json_from_the_last_assistant_text_fixture() {
    let result = adapter_for(AgentKind::Opencode)
        .extract_result(&opencode_fixture("run-format-json.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), ResultStatus::Done);
    assert_eq!(result.summary(), "note created");
    assert_eq!(result.files_changed(), &["note.txt"]);
}

#[test]
fn opencode_extracts_json_fence_from_the_last_assistant_text_fixture() {
    let result = adapter_for(AgentKind::Opencode)
        .extract_result(&opencode_fixture("final-fenced.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), ResultStatus::Done);
    assert_eq!(result.summary(), "fenced result");
    assert_eq!(result.files_changed(), &["fenced.txt"]);
}

#[test]
fn opencode_extracts_the_last_json_object_from_prose_fixture() {
    let result = adapter_for(AgentKind::Opencode)
        .extract_result(&opencode_fixture("final-prose.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), ResultStatus::NeedsInput);
    assert_eq!(result.summary(), "review is needed");
    assert_eq!(result.questions(), &["Which target?"]);
    assert_eq!(result.files_changed(), &["prose.txt"]);
}

#[test]
fn opencode_without_json_remains_unknown() {
    let result = adapter_for(AgentKind::Opencode)
        .extract_result(&opencode_fixture("no-result.jsonl"), None)
        .unwrap();
    assert_eq!(result.status(), ResultStatus::Unknown);
}

#[test]
fn opencode_live_fixture_captures_the_first_event_session_id() {
    let adapter = adapter_for(AgentKind::Opencode);
    let events: Vec<AgentEvent> = opencode_fixture("run-format-json.jsonl")
        .lines()
        .filter_map(|line| adapter.parse_event(line))
        .collect();
    assert!(matches!(
        events.first(),
        Some(AgentEvent::SessionStarted { session_ref }) if session_ref == OPENCODE_LIVE_SESSION
    ));
    assert_eq!(
        adapter.session_ref(&events).as_deref(),
        Some(OPENCODE_LIVE_SESSION)
    );
}

#[test]
fn trailer_result_is_extracted_or_unknown_never_an_error() {
    let cursor = adapter_for(AgentKind::Cursor);
    assert_eq!(
        cursor
            .extract_result(&fixture("cursor-success.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Done
    );
    assert_eq!(
        cursor
            .extract_result(&fixture("cursor-malformed.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Unknown
    );

    let opencode = adapter_for(AgentKind::Opencode);
    assert_eq!(
        opencode
            .extract_result(&fixture("opencode-success.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Done
    );
    assert_eq!(
        opencode
            .extract_result(&fixture("opencode-malformed.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Unknown
    );
}

#[test]
fn trailer_results_cover_needs_input_and_blocked() {
    let cursor = adapter_for(AgentKind::Cursor);
    let needs_input = cursor
        .extract_result(&fixture("cursor-needs-input.jsonl"), None)
        .unwrap();
    assert_eq!(needs_input.status(), ResultStatus::NeedsInput);
    assert_eq!(needs_input.questions(), &["Which crate should be renamed?"]);
    assert_eq!(
        cursor
            .extract_result(&fixture("cursor-blocked.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Blocked
    );

    let opencode = adapter_for(AgentKind::Opencode);
    let needs_input = opencode
        .extract_result(&fixture("opencode-needs-input.jsonl"), None)
        .unwrap();
    assert_eq!(needs_input.status(), ResultStatus::NeedsInput);
    assert_eq!(needs_input.questions(), &["Which crate should be renamed?"]);
    assert_eq!(
        opencode
            .extract_result(&fixture("opencode-blocked.jsonl"), None)
            .unwrap()
            .status(),
        ResultStatus::Blocked
    );
}

#[test]
fn trailer_extract_prefers_the_last_message_file() {
    let adapter = adapter_for(AgentKind::Cursor);
    let result = adapter
        .extract_result(
            &fixture("cursor-success.jsonl"),
            Some("```mac-worker-result\n{\"status\":\"needs_input\",\"summary\":\"from file\",\"questions\":[\"q\"],\"files_changed\":[]}\n```"),
        )
        .unwrap();
    assert_eq!(result.status(), ResultStatus::NeedsInput);
    assert_eq!(result.summary(), "from file");
    assert_eq!(result.questions(), &["q"]);
}

#[test]
fn cursor_and_opencode_truncated_final_line_is_ignored() {
    let cursor = adapter_for(AgentKind::Cursor);
    let events: Vec<AgentEvent> = fixture_lines("cursor-truncated.jsonl")
        .filter_map(|line| cursor.parse_event(&line))
        .collect();
    assert_eq!(cursor.session_ref(&events).as_deref(), Some(CURSOR_SESSION));
    assert!(
        cursor
            .parse_event(r#"{"type":"result","subtype":"success","result":"ok","session_id""#)
            .is_none()
    );
    assert!(!matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));

    let opencode = adapter_for(AgentKind::Opencode);
    let events: Vec<AgentEvent> = fixture_lines("opencode-truncated.jsonl")
        .filter_map(|line| opencode.parse_event(&line))
        .collect();
    assert_eq!(
        opencode.session_ref(&events).as_deref(),
        Some(OPENCODE_SESSION)
    );
    assert!(!matches!(events.last(), Some(AgentEvent::TurnEnd { .. })));
}

#[test]
fn cursor_and_opencode_classify_like_codex() {
    for kind in [AgentKind::Cursor, AgentKind::Opencode] {
        let adapter = adapter_for(kind);
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::Done),
            AgentOutcome::Done
        );
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::NeedsInput),
            AgentOutcome::NeedsInput
        );
        assert_eq!(
            adapter.classify(Some(1), ResultStatus::Done),
            AgentOutcome::Failed { exit_code: 1 }
        );
        assert_eq!(
            adapter.classify(None, ResultStatus::Done),
            AgentOutcome::Signalled
        );
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::Blocked),
            AgentOutcome::Blocked
        );
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::Unknown),
            AgentOutcome::Unknown
        );
    }
}

#[test]
fn render_shell_double_quotes_the_argv_pointer_and_single_quotes_the_rest() {
    let launch = adapter_for(AgentKind::Cursor)
        .first_turn(&cursor_params(PermissionPolicy::Unattended))
        .unwrap();
    let shell = render_shell(&launch).unwrap();
    assert!(shell.starts_with("exec 'cursor-agent' '-p' '--output-format' 'stream-json'"));
    assert!(shell.contains("'--trust' '--force'"));
    assert!(shell.ends_with(&format!("\"{PROMPT_POINTER}\"")));
    assert!(
        shell.contains("\"$MAC_WORKER_TURN_DIR/prompt.md\"")
            || shell.contains("$MAC_WORKER_TURN_DIR/prompt.md")
    );
    assert!(!shell.contains(&format!("'{PROMPT_POINTER}'")));
    assert!(!PROMPT_POINTER.contains("Reply"));

    let launch = adapter_for(AgentKind::Opencode)
        .first_turn(&opencode_params(PermissionPolicy::Unattended))
        .unwrap();
    let shell = render_shell(&launch).unwrap();
    assert!(shell.starts_with("exec 'opencode' 'run' '--format' 'json' '--auto'"));
    assert!(shell.ends_with(&format!("\"{PROMPT_POINTER}\"")));
}
