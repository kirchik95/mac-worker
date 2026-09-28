use mac_worker::agent::{AgentKind, ResultStatus, adapter_for};

#[test]
fn malformed_result_fixtures_have_typed_safe_reasons() {
    let cases = [
        ("", "empty_output"),
        ("finished, token=secret-never-publish", "no_result_json"),
        (r#"{"status":"done","summary":"unfinished"#, "truncated"),
        (
            r#"{"status":"surprise-secret","summary":"ok"}"#,
            "schema_mismatch:status",
        ),
        (
            r#"{"status":"done","summary":42}"#,
            "schema_mismatch:summary",
        ),
        (
            r#"{"status":"done","summary":"ok","questions":42}"#,
            "schema_mismatch:questions",
        ),
        (
            r#"{"status":"done","summary":"ok","files_changed":[42]}"#,
            "schema_mismatch:files_changed",
        ),
        (
            r#"{"status":"done","summary":"ok","checks":42}"#,
            "schema_mismatch:checks",
        ),
        (
            r#"{"status":"done","summary":"ok","secret-field":"value"}"#,
            "schema_mismatch:unknown_field",
        ),
    ];
    for kind in [
        AgentKind::Codex,
        AgentKind::Claude,
        AgentKind::Cursor,
        AgentKind::Opencode,
    ] {
        for (text, reason) in cases {
            let result = adapter_for(kind).extract_result("", Some(text)).unwrap();
            assert_eq!(result.status(), ResultStatus::Unknown, "{kind:?}: {text}");
            assert_eq!(
                serde_json::to_value(result.parse_reason()).unwrap(),
                reason,
                "{kind:?}"
            );
            assert!(result.summary().is_empty());
        }
        let result = adapter_for(kind)
            .extract_result("", Some(r#"{"status":"done","summary":"ok"}"#))
            .unwrap();
        assert_eq!(result.status(), ResultStatus::Done);
        assert!(result.parse_reason().is_none());
        let result = adapter_for(kind)
            .extract_result("unrecognized envelope", None)
            .unwrap();
        assert_eq!(
            serde_json::to_value(result.parse_reason()).unwrap(),
            "no_result_json"
        );
    }
}

#[test]
fn truncated_stream_envelope_is_distinguished_from_non_json_output() {
    for kind in [
        AgentKind::Codex,
        AgentKind::Claude,
        AgentKind::Cursor,
        AgentKind::Opencode,
    ] {
        let result = adapter_for(kind)
            .extract_result("{\"type\":\"result\",\"result\":\"unfinished", None)
            .unwrap();
        assert_eq!(
            serde_json::to_value(result.parse_reason()).unwrap(),
            "truncated",
            "{kind:?}"
        );
    }
}
