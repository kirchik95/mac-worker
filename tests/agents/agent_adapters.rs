use std::{fs, os::unix::process::ExitStatusExt, process::ExitStatus};

use mac_worker::test_support::{
    agents::agent::{
        AgentAdapter, AgentEvent, AgentKind, AgentOutcome, AuthProbeResult,
        MAX_DECLARED_ACCEPTANCE, OPENCODE_DIALECT_MISMATCH, OPENCODE_VERSION_UNVERIFIED,
        OpencodeDialect, PROMPT_POINTER, PermissionPolicy, PromptDelivery, Question,
        RESULT_SCHEMA_JSON, ResultStatus, TurnLaunch, TurnLimits, TurnParams, adapter_for,
        adapter_for_host, adapter_for_launch, declared_acceptance_instructions, has_dialects,
        render_shell, verify_opencode_launch,
    },
    host::process::ProcessResult,
};
use uuid::Uuid;

const DEFAULT_TIMEOUT_MILLIS: u64 = 45 * 60 * 1000;
const DAY_MILLIS: u64 = 24 * 60 * 60 * 1000;
const SESSION_PLACEHOLDER: &str = "0d3c…";

fn params(policy: PermissionPolicy) -> TurnParams {
    TurnParams {
        kind: AgentKind::Codex,
        model: None,
        effort: None,
        policy,
        limits: TurnLimits::new(DEFAULT_TIMEOUT_MILLIS, None, None).unwrap(),
        session_seed: Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0001),
        allow_permission_fallback: false,
    }
}

// Claude fixtures are hand-derived from the documented stream-json
// envelope, including verbose system/status, compact_boundary, assistant
// thinking/tool_use and user/tool_result records. Record shapes are documented
// at https://platform.claude.com/docs/en/agent-sdk/typescript#sdkmessage.
// They were not captured from a live Claude Code run.
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
            .windows(3)
            .any(|w| w[0] == "--output-format" && w[1] == "stream-json" && w[2] == "--verbose")
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
fn workspace_policy_falls_back_to_unattended_for_every_sandboxed_agent() {
    for kind in [AgentKind::Claude, AgentKind::Cursor, AgentKind::Opencode] {
        let mut requested = match kind {
            AgentKind::Claude => {
                let mut requested = params(PermissionPolicy::Workspace);
                requested.kind = AgentKind::Claude;
                requested
            }
            AgentKind::Cursor => cursor_params(PermissionPolicy::Workspace),
            _ => opencode_params(PermissionPolicy::Workspace),
        };
        requested.allow_permission_fallback = true;
        let launch = adapter_for(kind).first_turn(&requested).unwrap();
        assert!(launch.permission_fallback(), "{kind:?}");
        let args = launch.args();
        let unattended = match kind {
            AgentKind::Claude => args
                .windows(2)
                .any(|w| w[0] == "--permission-mode" && w[1] == "bypassPermissions"),
            AgentKind::Cursor => args.contains(&"--force".into()),
            _ => args.contains(&"--auto".into()),
        };
        assert!(unattended, "{kind:?}: {args:?}");
    }
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
            .windows(3)
            .any(|w| w[0] == "--output-format" && w[1] == "stream-json" && w[2] == "--verbose")
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
fn every_adapter_stream_yields_its_session_ref_and_normalized_events() {
    for (kind, fixture_name, session, changed_path) in [
        (
            AgentKind::Codex,
            "codex-success.jsonl",
            SESSION_PLACEHOLDER,
            "src/agent/mod.rs",
        ),
        (
            AgentKind::Cursor,
            "cursor-success.jsonl",
            CURSOR_SESSION,
            "src/agent/cursor.rs",
        ),
        (
            AgentKind::Opencode,
            "opencode-success.jsonl",
            OPENCODE_SESSION,
            "src/agent/opencode.rs",
        ),
    ] {
        let adapter = adapter_for(kind);
        let events: Vec<AgentEvent> = fixture_lines(fixture_name)
            .filter_map(|line| adapter.parse_event(&line))
            .collect();
        assert_eq!(
            adapter.session_ref(&events).as_deref(),
            Some(session),
            "{kind:?}"
        );
        let has_activity = match kind {
            AgentKind::Cursor => events
                .iter()
                .any(|event| matches!(event, AgentEvent::ToolCall { name, .. } if name == "read")),
            _ => events.iter().any(|event| {
                matches!(
                    event,
                    AgentEvent::Command {
                        exit_code: Some(0),
                        ..
                    }
                )
            }),
        };
        assert!(has_activity, "{kind:?}");
        assert!(
            events.iter().any(|event| matches!(
                event,
                AgentEvent::FileChange { paths } if paths == &[changed_path]
            )),
            "{kind:?}"
        );
        assert!(
            matches!(events.last(), Some(AgentEvent::TurnEnd { .. })),
            "{kind:?}"
        );
    }
}

#[test]
fn results_are_extracted_or_unknown_never_an_error_for_every_adapter() {
    for (kind, success, malformed) in [
        (
            AgentKind::Claude,
            "claude-success.jsonl",
            "claude-malformed.jsonl",
        ),
        (
            AgentKind::Cursor,
            "cursor-success.jsonl",
            "cursor-malformed.jsonl",
        ),
        (
            AgentKind::Opencode,
            "opencode-success.jsonl",
            "opencode-malformed.jsonl",
        ),
    ] {
        let adapter = adapter_for(kind);
        assert_eq!(
            adapter
                .extract_result(&fixture(success), None)
                .unwrap()
                .status(),
            ResultStatus::Done,
            "{kind:?}"
        );
        assert_eq!(
            adapter
                .extract_result(&fixture(malformed), None)
                .unwrap()
                .status(),
            ResultStatus::Unknown,
            "{kind:?}"
        );
    }
}

#[test]
fn claude_verbose_records_normalize_activity_and_ignore_other_content() {
    let adapter = adapter_for(AgentKind::Claude);
    let events: Vec<_> = fixture_lines("claude-verbose-records.jsonl")
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(&line)
                .expect("verbose fixture records must be valid JSON");
            adapter.parse_event(&line)
        })
        .collect();
    assert_eq!(
        events,
        vec![
            AgentEvent::ToolCall {
                name: "Read".into(),
                summary: r#"{"file_path":"/workspace/src/lib.rs"}"#.into(),
            },
            AgentEvent::AssistantMessage {
                text: "The file inspection is complete.".into(),
            },
        ]
    );
}

#[test]
fn claude_verbose_records_preserve_results_sessions_and_classification() {
    let adapter = adapter_for(AgentKind::Claude);
    let verbose = fixture("claude-verbose-records.jsonl");
    let unknown = concat!(
        r#"{"type":"system","subtype":"future_record","status":"blocked","summary":"not a result"}"#,
        "\n",
        r#"{"type":"assistant","message":{"content":[{"type":"future_block","text":"{\"status\":\"blocked\",\"summary\":\"not a result\"}"}]}}"#,
        "\n",
    );
    for line in unknown.lines() {
        serde_json::from_str::<serde_json::Value>(line)
            .expect("unknown records must be valid JSON");
        assert_eq!(adapter.parse_event(line), None);
    }
    for name in [
        "claude-success.jsonl",
        "claude-blocked.jsonl",
        "claude-malformed.jsonl",
    ] {
        let stream = fixture(name);
        // Cover both terminal result records and the final-assistant fallback.
        let assistant_fallback = stream
            .lines()
            .filter(|line| !matches!(adapter.parse_event(line), Some(AgentEvent::TurnEnd { .. })))
            .collect::<Vec<_>>()
            .join("\n");
        for baseline in [&stream, &assistant_fallback] {
            let noisy = format!("{verbose}{baseline}\n{verbose}{unknown}");
            let baseline_events: Vec<_> = baseline
                .lines()
                .filter_map(|line| adapter.parse_event(line))
                .collect();
            let noisy_events: Vec<_> = noisy
                .lines()
                .filter_map(|line| adapter.parse_event(line))
                .collect();
            assert_eq!(
                adapter.session_ref(&noisy_events),
                adapter.session_ref(&baseline_events),
                "{name}"
            );
            let expected = adapter.extract_result(baseline, None).unwrap();
            let actual = adapter.extract_result(&noisy, None).unwrap();
            assert_eq!(actual, expected, "{name}");
            for exit_code in [Some(0), Some(1), None] {
                assert_eq!(
                    adapter.classify(exit_code, actual.status()),
                    adapter.classify(exit_code, expected.status()),
                    "{name}: {exit_code:?}"
                );
            }
        }
    }
}

#[test]
fn results_cover_needs_input_and_blocked_for_every_adapter() {
    for (kind, needs_input, blocked) in [
        (AgentKind::Codex, Some("codex-needs-input.jsonl"), None),
        (AgentKind::Claude, None, Some("claude-blocked.jsonl")),
        (
            AgentKind::Cursor,
            Some("cursor-needs-input.jsonl"),
            Some("cursor-blocked.jsonl"),
        ),
        (
            AgentKind::Opencode,
            Some("opencode-needs-input.jsonl"),
            Some("opencode-blocked.jsonl"),
        ),
    ] {
        let adapter = adapter_for(kind);
        if let Some(needs_input) = needs_input {
            let result = adapter.extract_result(&fixture(needs_input), None).unwrap();
            assert_eq!(result.status(), ResultStatus::NeedsInput, "{kind:?}");
            assert_eq!(
                result.questions(),
                &[Question::open("Which crate should be renamed?")],
                "{kind:?}"
            );
        }
        if let Some(blocked) = blocked {
            assert_eq!(
                adapter
                    .extract_result(&fixture(blocked), None)
                    .unwrap()
                    .status(),
                ResultStatus::Blocked,
                "{kind:?}"
            );
        }
    }
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
fn extract_result_prefers_the_last_message_file_in_plain_and_fenced_form() {
    for (kind, fixture_name, last_message) in [
        (
            AgentKind::Codex,
            "codex-success.jsonl",
            r#"{"status":"needs_input","summary":"from file","questions":["q"],"files_changed":[]}"#,
        ),
        (
            AgentKind::Cursor,
            "cursor-success.jsonl",
            "```mac-worker-result\n{\"status\":\"needs_input\",\"summary\":\"from file\",\"questions\":[\"q\"],\"files_changed\":[]}\n```",
        ),
    ] {
        let result = adapter_for(kind)
            .extract_result(&fixture(fixture_name), Some(last_message))
            .unwrap();
        assert_eq!(result.status(), ResultStatus::NeedsInput, "{kind:?}");
        assert_eq!(result.summary(), "from file", "{kind:?}");
        assert_eq!(result.questions(), &[Question::open("q")], "{kind:?}");
    }
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
    assert_eq!(
        keys,
        ["checks", "files_changed", "questions", "status", "summary"]
    );
    assert_eq!(required, keys);
    assert_eq!(
        value["additionalProperties"],
        serde_json::Value::Bool(false)
    );
}

#[test]
fn declared_acceptance_instructions_are_a_pure_formatter() {
    assert_eq!(declared_acceptance_instructions(&[]).unwrap(), "");
    let text =
        declared_acceptance_instructions(&["cargo test --lib".into(), "npm test".into()]).unwrap();
    assert!(text.contains("instructed checks"));
    assert!(text.contains("- cargo test --lib"));
    assert!(text.contains("- npm test"));
    assert!(!text.contains("laptop-verified by mac-worker"));
    let too_many: Vec<String> = (0..=MAX_DECLARED_ACCEPTANCE)
        .map(|index| format!("item {index}"))
        .collect();
    assert_eq!(
        declared_acceptance_instructions(&too_many)
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
    assert_eq!(
        declared_acceptance_instructions(&["bad\nline".into()])
            .unwrap_err()
            .public_code(),
        "TASK_CONFIG_INVALID"
    );
}

#[test]
fn question_items_are_strict_structured_output_compatible() {
    let value: serde_json::Value = serde_json::from_str(RESULT_SCHEMA_JSON).unwrap();
    let item = &value["properties"]["questions"]["items"];
    assert_eq!(item["type"], "object");
    let mut keys: Vec<_> = item["properties"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    keys.sort();
    assert_eq!(keys, ["options", "text"]);
    let mut required: Vec<String> = item["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry.as_str().unwrap().to_owned())
        .collect();
    required.sort();
    assert_eq!(required, keys);
    assert_eq!(item["additionalProperties"], serde_json::Value::Bool(false));
    assert_eq!(item["properties"]["options"]["items"]["type"], "string");
}

#[test]
fn codex_passes_reasoning_effort_as_a_config_override_on_both_turns() {
    let mut with_effort = params(PermissionPolicy::Workspace);
    with_effort.model = Some("gpt-5.6-luna".into());
    with_effort.effort = Some("max".into());
    let codex = adapter_for(AgentKind::Codex);

    for launch in [
        codex.first_turn(&with_effort).unwrap(),
        codex.resume_turn(&with_effort, "thread-1").unwrap(),
    ] {
        assert!(
            launch
                .args()
                .windows(2)
                .any(|w| w[0] == "-m" && w[1] == "gpt-5.6-luna")
        );
        assert!(
            launch
                .args()
                .windows(2)
                .any(|w| w[0] == "-c" && w[1] == "model_reasoning_effort=\"max\""),
            "effort override missing from {:?}",
            launch.args()
        );
    }

    let without = codex
        .first_turn(&params(PermissionPolicy::Workspace))
        .unwrap();
    assert!(
        !without
            .args()
            .iter()
            .any(|argument| argument.starts_with("model_reasoning_effort="))
    );
}

#[test]
fn an_unsafe_effort_value_is_rejected_before_launch() {
    let mut params = params(PermissionPolicy::Workspace);
    params.effort = Some("max\" -c sandbox_mode=\"danger-full-access".into());
    assert!(adapter_for(AgentKind::Codex).first_turn(&params).is_err());

    params.effort = Some(String::new());
    assert!(adapter_for(AgentKind::Codex).first_turn(&params).is_err());

    params.effort = Some("x".repeat(33));
    assert!(adapter_for(AgentKind::Codex).first_turn(&params).is_err());
}

#[test]
fn agents_without_a_reasoning_effort_flag_ignore_it() {
    for kind in [AgentKind::Claude, AgentKind::Cursor, AgentKind::Opencode] {
        let mut params = params(PermissionPolicy::Unattended);
        params.kind = kind;
        params.effort = Some("max".into());
        let launch = adapter_for(kind).first_turn(&params).unwrap();
        assert!(
            !launch
                .args()
                .iter()
                .any(|argument| argument.contains("model_reasoning_effort"))
        );
    }
}

#[test]
fn structured_questions_carry_options_and_plain_strings_still_parse() {
    let codex = adapter_for(AgentKind::Codex);
    let structured = codex
        .extract_result(
            "",
            Some(
                r#"{"status":"needs_input","summary":"pick a base","questions":[{"text":"Which base?","options":["main","release"]},{"text":"Anything else?","options":[]}],"files_changed":[]}"#,
            ),
        )
        .unwrap();
    assert_eq!(structured.status(), ResultStatus::NeedsInput);
    assert_eq!(
        structured.questions(),
        &[
            Question::new("Which base?", vec!["main".into(), "release".into()]),
            Question::open("Anything else?"),
        ]
    );

    // A decorated question is refused like any other unknown field on the
    // result, so a malformed object never becomes a silently empty question.
    let decorated = codex
        .extract_result(
            "",
            Some(
                r#"{"status":"needs_input","summary":"pick a base","questions":[{"text":"Which base?","options":[],"required":true}],"files_changed":[]}"#,
            ),
        )
        .unwrap();
    assert_eq!(decorated.status(), ResultStatus::Unknown);

    // Agents that keep emitting bare strings stay supported.
    let legacy = codex
        .extract_result(
            "",
            Some(
                r#"{"status":"needs_input","summary":"pick a base","questions":["Which base?"],"files_changed":[]}"#,
            ),
        )
        .unwrap();
    assert_eq!(legacy.questions(), &[Question::open("Which base?")]);
}

#[test]
fn every_adapter_classifies_exit_and_status_without_guessing() {
    for kind in [
        AgentKind::Codex,
        AgentKind::Claude,
        AgentKind::Cursor,
        AgentKind::Opencode,
    ] {
        let adapter = adapter_for(kind);
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::Done),
            AgentOutcome::Done,
            "{kind:?}"
        );
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::NeedsInput),
            AgentOutcome::NeedsInput,
            "{kind:?}"
        );
        assert_eq!(
            adapter.classify(Some(1), ResultStatus::Done),
            AgentOutcome::Failed { exit_code: 1 },
            "{kind:?}"
        );
        assert_eq!(
            adapter.classify(None, ResultStatus::Done),
            AgentOutcome::Signalled,
            "{kind:?}"
        );
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::Blocked),
            AgentOutcome::Blocked,
            "{kind:?}"
        );
        assert_eq!(
            adapter.classify(Some(0), ResultStatus::Unknown),
            AgentOutcome::Unknown,
            "{kind:?}"
        );
    }
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
fn unsandboxed_agents_reject_workspace_without_an_explicit_fallback() {
    for kind in [AgentKind::Claude, AgentKind::Cursor, AgentKind::Opencode] {
        let mut requested = params(PermissionPolicy::Workspace);
        requested.kind = kind;
        let error = adapter_for(kind)
            .first_turn(&requested)
            .expect_err("workspace without opt-in must not launch");
        let message = error.to_string();
        assert!(
            message.contains("sandbox") && message.contains("permission_fallback"),
            "{kind:?}: {message}"
        );
        let resume = adapter_for(kind)
            .resume_turn(&requested, "session-ref")
            .expect_err("resume must reject workspace without opt-in");
        assert!(
            resume.to_string().contains("permission_fallback"),
            "{kind:?}: {resume}"
        );
    }
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
    assert_eq!(result.questions(), &[Question::open("Which target?")]);
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

// OpenCode has two CLI generations. The v2 fixtures come from a live
// `opencode run --standalone --format json --auto` capture on 2.0.18.

const OPENCODE_V1_VERSION: &str = "1.18.32";
const OPENCODE_V2_VERSION: &str = "2.0.18";
const OPENCODE_V2_SESSION: &str = "ses_f0ea2018effeLWl2cU5BkMIijI";

fn opencode_v1() -> &'static dyn AgentAdapter {
    adapter_for_launch(AgentKind::Opencode, Some(OPENCODE_V1_VERSION))
}

fn opencode_v2() -> &'static dyn AgentAdapter {
    adapter_for_launch(AgentKind::Opencode, Some(OPENCODE_V2_VERSION))
}

fn probe_output(status: i32, stdout: &[u8], stderr: &[u8]) -> ProcessResult {
    ProcessResult {
        status: ExitStatus::from_raw(status << 8),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

#[test]
fn opencode_first_turn_argv_is_exact_in_each_dialect() {
    let mut params = opencode_params(PermissionPolicy::Unattended);
    let v1 = opencode_v1().first_turn(&params).unwrap();
    assert_eq!(v1.program(), "opencode");
    assert_eq!(
        v1.args(),
        ["run", "--format", "json", "--auto", PROMPT_POINTER]
    );
    let v2 = opencode_v2().first_turn(&params).unwrap();
    assert_eq!(v2.program(), "opencode");
    assert_eq!(
        v2.args(),
        [
            "run",
            "--standalone",
            "--format",
            "json",
            "--auto",
            PROMPT_POINTER
        ]
    );

    params.model = Some("opencode/big-pickle#high".into());
    assert_eq!(
        opencode_v1().first_turn(&params).unwrap().args(),
        [
            "run",
            "--format",
            "json",
            "--auto",
            "--model",
            "opencode/big-pickle#high",
            PROMPT_POINTER
        ]
    );
    assert_eq!(
        opencode_v2().first_turn(&params).unwrap().args(),
        [
            "run",
            "--standalone",
            "--format",
            "json",
            "--auto",
            "--model",
            "opencode/big-pickle#high",
            PROMPT_POINTER
        ]
    );
    // The `#variant` of a v2 model reaches the CLI quoted, not as a comment.
    assert_eq!(
        render_shell(&opencode_v2().first_turn(&params).unwrap()).unwrap(),
        format!(
            "exec 'opencode' 'run' '--standalone' '--format' 'json' '--auto' '--model' 'opencode/big-pickle#high' \"{PROMPT_POINTER}\""
        )
    );
    for launch in [v1, v2] {
        assert_eq!(launch.prompt_delivery(), PromptDelivery::ArgvPointer);
        assert!(launch.env_names().is_empty());
        assert!(!launch.permission_fallback());
    }
}

#[test]
fn opencode_resume_argv_is_exact_in_each_dialect() {
    let mut params = opencode_params(PermissionPolicy::Unattended);
    assert_eq!(
        opencode_v1()
            .resume_turn(&params, OPENCODE_V2_SESSION)
            .unwrap()
            .args(),
        [
            "run",
            "--format",
            "json",
            "--auto",
            "--session",
            OPENCODE_V2_SESSION,
            PROMPT_POINTER
        ]
    );
    assert_eq!(
        opencode_v2()
            .resume_turn(&params, OPENCODE_V2_SESSION)
            .unwrap()
            .args(),
        [
            "run",
            "--standalone",
            "--format",
            "json",
            "--auto",
            "--session",
            OPENCODE_V2_SESSION,
            PROMPT_POINTER
        ]
    );

    params.model = Some("opencode-go/deepseek-v4-pro".into());
    assert_eq!(
        opencode_v2()
            .resume_turn(&params, OPENCODE_V2_SESSION)
            .unwrap()
            .args(),
        [
            "run",
            "--standalone",
            "--format",
            "json",
            "--auto",
            "--model",
            "opencode-go/deepseek-v4-pro",
            "--session",
            OPENCODE_V2_SESSION,
            PROMPT_POINTER
        ]
    );
    // The flags v2 removed from `run` were never part of either form.
    for adapter in [opencode_v1(), opencode_v2()] {
        let launch = adapter.resume_turn(&params, OPENCODE_V2_SESSION).unwrap();
        for removed in [
            "--pure",
            "--command",
            "--share",
            "--attach",
            "--dir",
            "--port",
        ] {
            assert!(!launch.args().iter().any(|argument| argument == removed));
        }
        assert!(adapter.resume_turn(&params, "").is_err());
    }
}

#[test]
fn opencode_delete_argv_is_exact_in_each_dialect() {
    assert_eq!(
        opencode_v1().delete_session(OPENCODE_V2_SESSION).unwrap(),
        ["opencode", "session", "delete", OPENCODE_V2_SESSION]
    );
    assert_eq!(
        opencode_v2().delete_session(OPENCODE_V2_SESSION).unwrap(),
        [
            "opencode",
            "session",
            "delete",
            OPENCODE_V2_SESSION,
            "--standalone"
        ]
    );
    for adapter in [opencode_v1(), opencode_v2()] {
        assert_eq!(adapter.delete_session(""), None);
        assert_eq!(adapter.delete_session("--help"), None);
        assert_eq!(adapter.delete_session("session\n1"), None);
        // OpenCode binds its session from the first JSON event: no command
        // to keep standalone here.
        assert_eq!(adapter.prebind_session(), None);
    }
}

#[test]
fn opencode_dialect_is_read_from_the_version_major() {
    use OpencodeDialect::{V1, V2};
    for (version, expected) in [
        (Some("1.18.32"), Some(V1)),
        (Some("0.15.3"), Some(V1)),
        (Some("1"), Some(V1)),
        (Some("2.0.18"), Some(V2)),
        (Some("v2.0.18"), Some(V2)),
        (Some("2.0.0-beta.1"), Some(V2)),
        (Some("3.1.0"), Some(V2)),
        (Some("10.0.0"), Some(V2)),
        (None, None),
        (Some(""), None),
        (Some("latest"), None),
        (Some("local"), None),
        (Some("two.0.18"), None),
        (Some("2x.0.18"), None),
        (Some("2-beta"), None),
        (Some("+2.0.18"), None),
        (Some("-2.0.18"), None),
        (Some(".2.0"), None),
        (Some(" 2.0.18"), None),
        (Some("99999999999999999999999.0"), None),
    ] {
        assert_eq!(OpencodeDialect::observed(version), expected, "{version:?}");
        // A launch keeps today's v1 form unless the facts say v2.
        assert_eq!(
            OpencodeDialect::for_launch(version),
            expected.unwrap_or(V1),
            "{version:?}"
        );
        // A host command takes the v1 form only for a positively known v1.
        assert_eq!(
            OpencodeDialect::for_host_command(version),
            expected.unwrap_or(V2),
            "{version:?}"
        );
    }
}

#[test]
fn launch_adapter_is_chosen_from_the_workers_recorded_version() {
    let opencode = opencode_params(PermissionPolicy::Unattended);
    let standalone = |version: Option<&str>| {
        adapter_for_launch(AgentKind::Opencode, version)
            .first_turn(&opencode)
            .unwrap()
            .args()
            .iter()
            .any(|argument| argument == "--standalone")
    };
    assert!(!standalone(Some("1.18.32")));
    assert!(standalone(Some("2.0.18")));
    // Unknown and garbage versions keep the v1 form, as before.
    assert!(!standalone(None));
    assert!(!standalone(Some("not-a-version")));
    // The default adapter stays v1 for every existing caller.
    assert_eq!(
        adapter_for(AgentKind::Opencode)
            .first_turn(&opencode)
            .unwrap(),
        adapter_for_launch(AgentKind::Opencode, None)
            .first_turn(&opencode)
            .unwrap()
    );

    // A host command is standalone unless the version is a known v1.
    let host_standalone = |version: Option<&str>| {
        adapter_for_host(AgentKind::Opencode, version)
            .delete_session("ses_1")
            .unwrap()
            .iter()
            .any(|argument| argument == "--standalone")
    };
    assert!(!host_standalone(Some("1.18.32")));
    assert!(host_standalone(Some("2.0.18")));
    assert!(host_standalone(None));
    assert!(host_standalone(Some("not-a-version")));

    // Only OpenCode has dialects; a version never changes another agent.
    assert!(has_dialects(AgentKind::Opencode));
    for kind in [AgentKind::Codex, AgentKind::Claude, AgentKind::Cursor] {
        assert!(!has_dialects(kind));
        let mut params = params(PermissionPolicy::Unattended);
        params.kind = kind;
        let default = adapter_for(kind).first_turn(&params).unwrap();
        for version in [None, Some("1.0.0"), Some("2.0.18")] {
            assert_eq!(
                adapter_for_launch(kind, version)
                    .first_turn(&params)
                    .unwrap(),
                default
            );
            assert_eq!(
                adapter_for_host(kind, version).auth_probe().args(),
                adapter_for(kind).auth_probe().args()
            );
        }
    }
}

#[test]
fn launch_guard_refuses_a_generation_the_argv_was_not_built_for() {
    use OpencodeDialect::{V1, V2};
    for (built_for, observed) in [
        (V1, Some("1.18.32")),
        (V1, Some("0.15.3")),
        (V2, Some("2.0.18")),
        // `--standalone` cannot start the service on either generation, so
        // an unobserved version does not block it.
        (V2, None),
        (V2, Some("not-a-version")),
    ] {
        assert!(
            verify_opencode_launch(built_for, observed).is_ok(),
            "{built_for:?} on {observed:?}"
        );
    }
    for (built_for, observed, code) in [
        // Stale v1 facts on a v2 worker: this would start the service.
        (V1, Some("2.0.18"), OPENCODE_DIALECT_MISMATCH),
        (V1, Some("3.0.0"), OPENCODE_DIALECT_MISMATCH),
        (V2, Some("1.18.32"), OPENCODE_DIALECT_MISMATCH),
        // Without the flag an unknown generation may be v2.
        (V1, None, OPENCODE_VERSION_UNVERIFIED),
        (V1, Some("not-a-version"), OPENCODE_VERSION_UNVERIFIED),
    ] {
        let error = verify_opencode_launch(built_for, observed)
            .expect_err("mismatched launch must be refused");
        assert_eq!(error.public_code(), code, "{built_for:?} on {observed:?}");
    }
}

#[test]
fn opencode_v2_events_map_like_v1_in_both_adapters() {
    for adapter in [adapter_for(AgentKind::Opencode), opencode_v2()] {
        let events: Vec<AgentEvent> = opencode_fixture("v2-run-standalone-json.jsonl")
            .lines()
            .filter_map(|line| adapter.parse_event(line))
            .collect();
        assert_eq!(
            events,
            vec![
                AgentEvent::SessionStarted {
                    session_ref: OPENCODE_V2_SESSION.into()
                },
                // The v2 tool part carries `partID` and the call `id`.
                AgentEvent::FileChange {
                    paths: vec!["hello.txt".into()]
                },
                // `step_finish` with `tool-calls` is not a turn end.
                AgentEvent::SessionStarted {
                    session_ref: OPENCODE_V2_SESSION.into()
                },
                AgentEvent::AssistantMessage {
                    text: "done".into()
                },
            ]
        );
        assert_eq!(
            adapter.session_ref(&events).as_deref(),
            Some(OPENCODE_V2_SESSION)
        );
        // The capture ends on the last `text`: v2 printed no closing
        // `step_finish`, so there is no `TurnEnd` to wait for.
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::TurnEnd { .. }))
        );
    }
}

#[test]
fn opencode_v2_tool_parts_with_part_id_and_call_id_are_accepted() {
    let adapter = adapter_for(AgentKind::Opencode);
    // Hand-derived from the captured `write` part: same envelope, other tool.
    let read = r#"{"type":"tool_use","timestamp":1790755867064,"sessionID":"ses_f0ea2018effeLWl2cU5BkMIijI","part":{"partID":"prt_0f15e09ab001WTV2hsA5HmZtAE","sessionID":"ses_f0ea2018effeLWl2cU5BkMIijI","messageID":"msg_0f15dff64001zbFCHq7bPbxZTW","type":"tool","id":"call_function_p1bmuhyrpwxc_2","tool":"read","state":{"status":"completed","input":{"path":"hello.txt"},"output":"hi\n"}}}"#;
    assert_eq!(
        adapter.parse_event(read),
        Some(AgentEvent::ToolCall {
            name: "read".into(),
            summary: r#"{"path":"hello.txt"}"#.into(),
        })
    );
    let bash = r#"{"type":"tool_use","timestamp":1790755867064,"sessionID":"ses_f0ea2018effeLWl2cU5BkMIijI","part":{"partID":"prt_0f15e09ab001WTV2hsA5HmZtAE","sessionID":"ses_f0ea2018effeLWl2cU5BkMIijI","messageID":"msg_0f15dff64001zbFCHq7bPbxZTW","type":"tool","id":"call_function_p1bmuhyrpwxc_3","tool":"bash","state":{"status":"completed","input":{"command":"cat hello.txt"},"output":"hi\n"}}}"#;
    assert_eq!(
        adapter.parse_event(bash),
        Some(AgentEvent::Command {
            summary: "cat hello.txt".into(),
            exit_code: None,
        })
    );
}

#[test]
fn opencode_v2_result_does_not_need_a_closing_step_finish() {
    for adapter in [adapter_for(AgentKind::Opencode), opencode_v2()] {
        // A structured final message in the last `text` event is the result,
        // although no `step_finish` follows it.
        let stream = opencode_fixture("v2-final-json.jsonl");
        assert!(
            stream
                .lines()
                .last()
                .is_some_and(|line| line.starts_with(r#"{"type":"text""#))
        );
        let result = adapter.extract_result(&stream, None).unwrap();
        assert_eq!(result.status(), ResultStatus::Done);
        assert_eq!(result.summary(), "hello.txt created");
        assert_eq!(result.files_changed(), &["hello.txt"]);
        assert!(result.questions().is_empty());
        assert!(result.parse_reason().is_none());

        // The live capture ended with the prose `done`: no result, and the
        // reason says so rather than reporting a cut stream.
        let prose = adapter
            .extract_result(&opencode_fixture("v2-run-standalone-json.jsonl"), None)
            .unwrap();
        assert_eq!(prose.status(), ResultStatus::Unknown);
        assert_eq!(
            serde_json::to_value(prose.parse_reason()).unwrap(),
            "no_result_json"
        );
    }
}

#[test]
fn opencode_v2_auth_probe_is_standalone_and_reads_the_credential_table() {
    let probe = opencode_v2().auth_probe();
    assert_eq!(probe.args(), ["auth", "list", "--standalone"]);
    assert_eq!(
        adapter_for_host(AgentKind::Opencode, Some(OPENCODE_V2_VERSION))
            .auth_probe()
            .args(),
        ["auth", "list", "--standalone"]
    );
    // An unobserved version must not run the form that reaches the service.
    assert_eq!(
        adapter_for_host(AgentKind::Opencode, None)
            .auth_probe()
            .args(),
        ["auth", "list", "--standalone"]
    );
    assert_eq!(
        adapter_for_host(AgentKind::Opencode, Some(OPENCODE_V1_VERSION))
            .auth_probe()
            .args(),
        ["auth", "list"]
    );

    // Captured from `opencode auth list` on 2.0.18.
    let table = b"OpenCode Go     API key                     stored\nGitHub Copilot  OAuth                       stored\nZ.AI            API key                     stored\n";
    let classify = |stdout: &[u8]| probe.classify(&probe_output(0, stdout, b""));
    assert_eq!(classify(table), AuthProbeResult::Authenticated);
    assert_eq!(
        classify(b"Z.AI            API key                     stored\n"),
        AuthProbeResult::Authenticated
    );
    assert_eq!(
        classify(b"\x1b[1mZ.AI\x1b[0m  OAuth  \x1b[32mstored\x1b[0m\n"),
        AuthProbeResult::Authenticated
    );
    // One stored row is enough beside rows in another state.
    assert_eq!(
        classify(b"Anthropic  OAuth  expired\nZ.AI  API key  stored\n"),
        AuthProbeResult::Authenticated
    );

    // No rows: nothing is stored.
    assert_eq!(classify(b""), AuthProbeResult::Unauthenticated);
    assert_eq!(classify(b"\n  \n"), AuthProbeResult::Unauthenticated);

    // Anything else stays unknown.
    for junk in [
        &b"unexpected output\n"[..],
        b"stored\n",
        b"Anthropic  OAuth  expired\n",
        b"Z.AI  API key  stored soon\n",
        b"{\"providers\":[]}\n",
    ] {
        assert_eq!(
            classify(junk),
            AuthProbeResult::Unknown,
            "{}",
            String::from_utf8_lossy(junk)
        );
    }
    // v1 answers the unknown flag with its usage text on stderr.
    assert_eq!(
        probe.classify(&probe_output(
            1,
            b"",
            b"opencode auth list\n\nlist providers and credentials\n\nOptions:\n  -h, --help  show help\n"
        )),
        AuthProbeResult::Unknown
    );
}

#[test]
fn opencode_v1_auth_probe_keeps_its_command_and_does_not_read_the_v2_table() {
    let probe = opencode_v1().auth_probe();
    assert_eq!(probe.args(), ["auth", "list"]);
    assert_eq!(
        adapter_for(AgentKind::Opencode).auth_probe().args(),
        ["auth", "list"]
    );
    assert_eq!(
        probe.classify(&probe_output(0, b"", b"")),
        AuthProbeResult::Unknown
    );
    assert_eq!(
        probe.classify(&probe_output(
            0,
            b"Z.AI            API key                     stored\n",
            b""
        )),
        AuthProbeResult::Unknown
    );
    // Captured from `opencode auth list` on 1.18.32.
    let listing = b"\xe2\x94\x8c  Credentials \x1b[90m~/.local/share/opencode/auth.json\n\xe2\x94\x82\n\xe2\x97\x8f  GitHub Copilot \x1b[90moauth\n\xe2\x94\x82\n\xe2\x97\x8f  OpenCode Go \x1b[90mapi\n\xe2\x94\x82\n\xe2\x97\x8f  Z.AI \x1b[90mapi\n\xe2\x94\x82\n\xe2\x94\x94  3 credentials\n\n";
    assert_eq!(
        probe.classify(&probe_output(0, listing, b"\x1b[0m")),
        AuthProbeResult::Authenticated
    );
}
