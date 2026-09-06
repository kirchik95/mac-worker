use std::os::unix::process::ExitStatusExt;
use std::{fs, path::Path, process::ExitStatus, sync::Mutex};

use mac_worker::agent_settings::{
    AgentDefaultSettings, AgentSettingsList, AgentSettingsSaveRequest, NativeAgentSettingsStore,
    SETTINGS_AGENT_IDS,
};
use mac_worker::{
    config::WorkerEntry,
    process::{ProcessRequest, ProcessResult, ProcessRunner},
    transfer::{HostOperation, SshJsonTransport},
};
use tempfile::tempdir;

fn store(home: &Path) -> NativeAgentSettingsStore {
    NativeAgentSettingsStore::new(home)
}

#[test]
fn reads_and_updates_codex_without_reformatting_unrelated_toml() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex")).unwrap();
    fs::write(
        home.path().join(".codex/models_cache.json"),
        r#"{"models":[
          {"slug":"gpt-old","supported_reasoning_levels":[{"effort":"high"},{"effort":"max"}]},
          {"slug":"gpt-new","supported_reasoning_levels":[{"effort":"max"}]}
        ]}"#,
    )
    .unwrap();
    let path = home.path().join(".codex/config.toml");
    let original = "# keep this comment\nmodel = \"gpt-old\" # keep inline\nmodel_reasoning_effort = \"high\"\nplan_mode_reasoning_effort = \"medium\"\n[projects]\n\"/tmp/project\" = { trust_level = \"trusted\" }\n";
    fs::write(&path, original).unwrap();

    let settings = store(home.path()).read("codex").unwrap();
    assert_eq!(settings.model.as_deref(), Some("gpt-old"));
    assert_eq!(settings.effort.as_deref(), Some("high"));
    assert_eq!(settings.effort_options, vec!["high", "max"]);
    assert!(settings.revision.is_some());

    let saved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "codex".into(),
            model: Some("gpt-new".into()),
            effort: Some("max".into()),
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert_eq!(saved.model.as_deref(), Some("gpt-new"));
    assert_eq!(saved.effort.as_deref(), Some("max"));
    let updated = fs::read_to_string(path).unwrap();
    assert!(updated.contains("# keep this comment"));
    assert!(updated.contains("plan_mode_reasoning_effort = \"medium\""));
    assert!(updated.contains("[projects]"));
    assert!(updated.contains("model = \"gpt-new\" # keep inline"));
}

#[test]
fn codex_edit_preserves_multiline_instructions_that_resemble_native_keys() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex")).unwrap();
    fs::write(
        home.path().join(".codex/models_cache.json"),
        r#"{"models":[{"slug":"gpt-new","supported_reasoning_levels":[{"effort":"max"}]}]}"#,
    )
    .unwrap();
    let path = home.path().join(".codex/config.toml");
    let instructions = r#"developer_instructions = """
Текст с Unicode: модель должна оставаться literal.
model = "inside-instructions"
[inside_table]
"""
model = "old" # root model
model_reasoning_effort = "high" # root effort
[projects]
"/tmp/project" = { trust_level = "trusted" }
"#;
    fs::write(&path, instructions).unwrap();
    let settings = store(home.path()).read("codex").unwrap();
    store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "codex".into(),
            model: Some("gpt-new".into()),
            effort: Some("max".into()),
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    let updated = fs::read_to_string(path).unwrap();
    assert!(updated.contains("model = \"inside-instructions\""));
    assert!(updated.contains("[inside_table]"));
    assert!(updated.contains("Текст с Unicode: модель должна оставаться literal."));
    assert!(updated.contains("model = \"gpt-new\" # root model"));
    assert!(updated.contains("model_reasoning_effort = \"max\" # root effort"));
    assert!(updated.contains("[projects]"));
}

#[test]
fn codex_effort_choices_follow_the_selected_model_cache_entry() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex")).unwrap();
    fs::write(
        home.path().join(".codex/config.toml"),
        "model = \"gpt-5.6-luna\"\nmodel_reasoning_effort = \"max\"\n",
    )
    .unwrap();
    fs::write(
        home.path().join(".codex/models_cache.json"),
        r#"{"models":[
          {"slug":"gpt-5.6-luna","supported_reasoning_levels":[{"effort":"low"},{"effort":"high"}]},
          {"slug":"gpt-5.6-sol","supported_reasoning_levels":[{"effort":"low"},{"effort":"ultra"}]}
        ]}"#,
    )
    .unwrap();

    let settings = store(home.path()).read("codex").unwrap();
    assert_eq!(settings.effort.as_deref(), Some("max"));
    assert_eq!(settings.effort_options, vec!["low", "high"]);
    let error = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "codex".into(),
            model: None,
            effort: Some("ultra".into()),
            revision: settings.revision.unwrap(),
        })
        .unwrap_err();
    assert_eq!(error.code(), "SETTINGS_INVALID");
    assert_eq!(
        fs::read_to_string(home.path().join(".codex/config.toml")).unwrap(),
        "model = \"gpt-5.6-luna\"\nmodel_reasoning_effort = \"max\"\n"
    );
}

#[test]
fn reads_and_updates_claude_root_fields_only() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".claude")).unwrap();
    let path = home.path().join(".claude/settings.json");
    let original = r#"{
  // keep this JSONC comment
  "model": "claude-old",
  "effortLevel": "high",
  "modelSettings": {"claude-old": {"effortLevel": "low"}},
  "permissions": {"allow": ["Read"]},
}
"#;
    fs::write(&path, original).unwrap();

    let settings = store(home.path()).read("claude").unwrap();
    assert_eq!(settings.model.as_deref(), Some("claude-old"));
    assert_eq!(settings.effort.as_deref(), Some("high"));
    store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "claude".into(),
            model: None,
            effort: Some("max".into()),
            revision: settings.revision.unwrap(),
        })
        .unwrap();

    let updated = fs::read_to_string(path).unwrap();
    assert!(updated.contains("// keep this JSONC comment"));
    assert!(!updated.contains("\"model\": \"claude-old\""));
    assert!(updated.contains("\"effortLevel\": \"max\""));
    assert!(updated.contains("\"modelSettings\": {\"claude-old\": {\"effortLevel\": \"low\"}}"));
}

#[test]
fn reports_all_allowlisted_agents_in_stable_order_and_missing_files_are_readable() {
    let home = tempdir().unwrap();
    let list = store(home.path()).read_all();
    assert_eq!(
        list.agents
            .iter()
            .map(|entry| entry.agent.as_str())
            .collect::<Vec<_>>(),
        SETTINGS_AGENT_IDS
    );
    for entry in list.agents {
        assert!(entry.revision.is_some());
        assert!(entry.model.is_none());
        assert!(entry.effort.is_none());
    }
}

#[test]
fn stale_revision_does_not_overwrite_source() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex")).unwrap();
    let path = home.path().join(".codex/config.toml");
    fs::write(&path, "model = \"before\"\n").unwrap();
    let settings = store(home.path()).read("codex").unwrap();
    fs::write(&path, "model = \"changed\"\n").unwrap();

    let error = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "codex".into(),
            model: Some("after".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap_err();
    assert_eq!(error.code(), "SETTINGS_CONFLICT");
    assert_eq!(fs::read_to_string(path).unwrap(), "model = \"changed\"\n");
}

#[test]
fn updates_cursor_canonical_model_and_preserves_other_parameters() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".cursor")).unwrap();
    let path = home.path().join(".cursor/cli-config.json");
    fs::write(
        &path,
        r#"{
  "model": {"modelId": "old", "displayName": "Old", "aliases": ["old-alias"]},
  "selectedModel": {"modelId": "old", "parameters": [{"id":"effort","value":"high"},{"id":"fast","value":true}]},
  "modelParameters": {
    "old": [{"id":"effort","value":"high"},{"id":"context","value":128000}],
    "new": [{"id":"effort","value":"low"},{"id":"context","value":32000},{"id":"future","value":false}]
  },
  "display": {"theme":"dark"}
}
"#,
    )
    .unwrap();
    let settings = store(home.path()).read("cursor").unwrap();
    assert_eq!(settings.model.as_deref(), Some("old"));
    assert_eq!(settings.effort.as_deref(), Some("high"));
    store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "cursor".into(),
            model: Some("new".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    let updated = fs::read_to_string(path).unwrap();
    let document: serde_json::Value = serde_json::from_str(&updated).unwrap();
    assert_eq!(document["model"]["modelId"], "new");
    assert!(document["model"].get("displayName").is_none());
    assert!(document["model"].get("aliases").is_none());
    assert_eq!(document["selectedModel"]["modelId"], "new");
    assert!(
        document["selectedModel"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .all(|parameter| parameter["id"] != "fast")
    );
    assert!(
        document["selectedModel"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|parameter| parameter["id"] == "context" && parameter["value"] == 32000)
    );
    assert!(
        document["selectedModel"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|parameter| parameter["id"] == "future" && parameter["value"] == false)
    );
    assert!(
        !document["selectedModel"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|parameter| parameter["id"] == "effort")
    );
    assert_eq!(document["modelParameters"]["old"][1]["id"], "context");
    assert_eq!(document["modelParameters"]["new"][0]["id"], "context");
    assert_eq!(document["modelParameters"]["new"][1]["id"], "future");
    assert!(updated.contains("\"id\":\"context\",\"value\":128000"));
    assert!(updated.contains("\"display\": {\"theme\":\"dark\"}"));
}

#[test]
fn cursor_preserves_an_unlisted_current_effort_and_resets_active_selection_as_a_unit() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".cursor")).unwrap();
    let path = home.path().join(".cursor/cli-config.json");
    fs::write(
        &path,
        r#"{
  "model": {"modelId": "old", "displayName": "Old"},
  "selectedModel": {"modelId": "old", "parameters": [{"id":"effort","value":"custom"},{"id":"fast","value":"true"}]},
  "hasChangedDefaultModel": true,
  "modelParameters": {"old": [{"id":"effort","value":"custom"},{"id":"context","value":"1m"}]},
  "modelSelectionHistory": ["old", "older"],
  "display": {"theme":"dark"}
}
"#,
    )
    .unwrap();
    let settings = store(home.path()).read("cursor").unwrap();
    assert_eq!(settings.effort.as_deref(), Some("custom"));
    assert!(settings.effort_options.is_empty());

    let preserved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "cursor".into(),
            model: Some("old".into()),
            effort: Some("custom".into()),
            revision: settings.revision.clone().unwrap(),
        })
        .unwrap();
    assert_eq!(preserved.effort.as_deref(), Some("custom"));
    let revision = preserved.revision.unwrap();
    let reset = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "cursor".into(),
            model: None,
            effort: None,
            revision,
        })
        .unwrap();
    assert_eq!(reset.model, None);
    assert_eq!(reset.effort, None);
    let document: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert!(document.get("model").is_none());
    assert!(document.get("selectedModel").is_none());
    assert!(document.get("hasChangedDefaultModel").is_none());
    assert_eq!(document["modelParameters"]["old"][1]["id"], "context");
    assert_eq!(document["modelSelectionHistory"][0], "old");
    assert_eq!(document["display"]["theme"], "dark");
}

#[test]
fn cursor_reads_effort_from_direct_model_cache_arrays() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".cursor")).unwrap();
    fs::write(
        home.path().join(".cursor/cli-config.json"),
        r#"{
  "model": {"modelId": "cached"},
  "selectedModel": {"modelId": "cached", "parameters": [{"id":"context","value":128000}]},
  "modelParameters": {"cached": [{"id":"effort","value":"verified"},{"id":"context","value":128000}]}
}
"#,
    )
    .unwrap();

    let settings = store(home.path()).read("cursor").unwrap();
    assert_eq!(settings.model.as_deref(), Some("cached"));
    assert_eq!(settings.effort.as_deref(), Some("verified"));
    assert!(settings.effort_options.is_empty());
}

#[test]
fn cursor_rejects_duplicate_or_disagreeing_effort_entries() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".cursor")).unwrap();
    let path = home.path().join(".cursor/cli-config.json");
    fs::write(
        &path,
        r#"{
  "model": {"modelId": "ambiguous"},
  "selectedModel": {"modelId": "ambiguous", "parameters": [{"id":"effort","value":"one"},{"id":"effort","value":"two"}]},
  "modelParameters": {"ambiguous": [{"id":"effort","value":"one"}]}
}
"#,
    )
    .unwrap();
    assert_eq!(
        store(home.path()).read("cursor").unwrap_err().code(),
        "SETTINGS_INVALID_CONFIG"
    );

    fs::write(
        &path,
        r#"{
  "model": {"modelId": "disagree"},
  "selectedModel": {"modelId": "disagree", "parameters": [{"id":"effort","value":"one"}]},
  "modelParameters": {"disagree": [{"id":"effort","value":"two"}]}
}
"#,
    )
    .unwrap();
    assert_eq!(
        store(home.path()).read("cursor").unwrap_err().code(),
        "SETTINGS_INVALID_CONFIG"
    );
}

#[test]
fn cursor_clearing_effort_removes_the_parameter_element() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".cursor")).unwrap();
    let path = home.path().join(".cursor/cli-config.json");
    fs::write(
        &path,
        r#"{
  "model": {"modelId": "same"},
  "selectedModel": {"modelId": "same", "parameters": [{"id":"effort","value":"custom"},{"id":"context","value":128000}]},
  "modelParameters": {"same": [{"id":"effort","value":"custom"},{"id":"context","value":128000}]}
}
"#,
    )
    .unwrap();
    let settings = store(home.path()).read("cursor").unwrap();
    let saved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "cursor".into(),
            model: Some("same".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert_eq!(saved.effort, None);
    let document: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    for parameters in [
        &document["selectedModel"]["parameters"],
        &document["modelParameters"]["same"],
    ] {
        assert!(
            parameters
                .as_array()
                .unwrap()
                .iter()
                .all(|parameter| parameter["id"] != "effort")
        );
        assert!(
            parameters
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["id"] == "context")
        );
    }
}

#[test]
fn updates_opencode_jsonc_and_preserves_comments() {
    let home = tempdir().unwrap();
    let directory = home.path().join(".config/opencode");
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("opencode.jsonc");
    fs::write(&path, "{\n  // keep\n  \"model\": \"old\",\n}\n").unwrap();
    let settings = store(home.path()).read("opencode").unwrap();
    assert_eq!(settings.model.as_deref(), Some("old"));
    store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "opencode".into(),
            model: Some("new".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    let updated = fs::read_to_string(path).unwrap();
    assert!(updated.contains("// keep"));
    assert!(updated.contains("\"model\": \"new\""));
}

#[test]
fn inserts_missing_jsonc_field_before_a_trailing_comma() {
    let home = tempdir().unwrap();
    let directory = home.path().join(".config/opencode");
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("opencode.jsonc");
    fs::write(&path, "{\n  // keep\n  \"theme\": \"dark\",\n}\n").unwrap();
    let settings = store(home.path()).read("opencode").unwrap();
    let saved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "opencode".into(),
            model: Some("new".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert_eq!(saved.model.as_deref(), Some("new"));
    let updated = fs::read_to_string(path).unwrap();
    assert!(updated.contains("\"theme\": \"dark\""));
    assert!(updated.contains("\"model\": \"new\""));
    assert!(
        serde_json::from_str::<serde_json::Value>(
            &updated.replace("// keep\n", "").replace(",\n}", "\n}")
        )
        .is_ok()
    );
}

#[test]
fn opencode_jsonc_override_wins_and_revision_covers_both_documents() {
    let home = tempdir().unwrap();
    let directory = home.path().join(".config/opencode");
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("opencode.json"),
        "{\"model\":\"base\",\"other\":1}\n",
    )
    .unwrap();
    fs::write(
        directory.join("opencode.jsonc"),
        "{\n  // override\n  \"model\": \"overlay\",\n}\n",
    )
    .unwrap();
    let settings = store(home.path()).read("opencode").unwrap();
    assert_eq!(settings.model.as_deref(), Some("overlay"));
    let revision = settings.revision.unwrap();
    fs::write(
        directory.join("opencode.json"),
        "{\"model\":\"changed-base\",\"other\":1}\n",
    )
    .unwrap();
    let error = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "opencode".into(),
            model: Some("next".into()),
            effort: None,
            revision,
        })
        .unwrap_err();
    assert_eq!(error.code(), "SETTINGS_CONFLICT");
    assert!(
        fs::read_to_string(directory.join("opencode.jsonc"))
            .unwrap()
            .contains("overlay")
    );
}

#[test]
fn opencode_edits_lower_source_when_jsonc_only_overlays_unrelated_keys() {
    let home = tempdir().unwrap();
    let directory = home.path().join(".config/opencode");
    fs::create_dir_all(&directory).unwrap();
    let plain = directory.join("opencode.json");
    let jsonc = directory.join("opencode.jsonc");
    fs::write(&plain, "{\"model\":\"base\",\"other\":1}\n").unwrap();
    fs::write(
        &jsonc,
        "{\n  // unrelated upper-layer setting\n  \"theme\": \"dark\",\n}\n",
    )
    .unwrap();

    let settings = store(home.path()).read("opencode").unwrap();
    assert_eq!(settings.model.as_deref(), Some("base"));
    assert!(
        settings
            .message
            .as_deref()
            .is_some_and(|message| message.contains("inherited"))
    );
    store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "opencode".into(),
            model: Some("next".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert!(
        fs::read_to_string(&plain)
            .unwrap()
            .contains("\"model\":\"next\"")
    );
    assert!(
        fs::read_to_string(&jsonc)
            .unwrap()
            .contains("unrelated upper-layer setting")
    );
}

#[test]
fn opencode_clear_overlay_reveals_lower_model_and_revision_tracks_both_files() {
    let home = tempdir().unwrap();
    let directory = home.path().join(".config/opencode");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("opencode.json"), "{\"model\":\"base\"}\n").unwrap();
    fs::write(
        directory.join("opencode.jsonc"),
        "{\n  // upper layer\n  \"model\": \"overlay\",\n}\n",
    )
    .unwrap();

    let settings = store(home.path()).read("opencode").unwrap();
    let saved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "opencode".into(),
            model: None,
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert_eq!(saved.model.as_deref(), Some("base"));
    assert!(
        saved
            .message
            .as_deref()
            .is_some_and(|message| message.contains("inherited"))
    );
    assert!(
        !fs::read_to_string(directory.join("opencode.jsonc"))
            .unwrap()
            .contains("\"model\"")
    );
}

#[test]
fn missing_opencode_null_save_does_not_create_native_directories() {
    let home = tempdir().unwrap();
    let settings = store(home.path()).read("opencode").unwrap();
    let saved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "opencode".into(),
            model: None,
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert_eq!(saved.model, None);
    assert_eq!(saved.effort, None);
    assert!(!home.path().join(".config").exists());
}

#[test]
fn clearing_codex_setting_preserves_its_inline_comment() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex")).unwrap();
    fs::write(
        home.path().join(".codex/models_cache.json"),
        r#"{"models":[{"slug":"old","supported_reasoning_levels":[{"effort":"high"}]}]}"#,
    )
    .unwrap();
    let path = home.path().join(".codex/config.toml");
    fs::write(
        &path,
        "model = \"old\" # retain this note\nmodel_reasoning_effort = \"high\"\n",
    )
    .unwrap();
    let settings = store(home.path()).read("codex").unwrap();
    store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "codex".into(),
            model: None,
            effort: Some("high".into()),
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    let updated = fs::read_to_string(path).unwrap();
    assert!(!updated.contains("model ="));
    assert!(updated.contains("# retain this note"));
    assert!(updated.contains("model_reasoning_effort = \"high\""));
}

#[test]
fn read_only_native_source_is_readable_but_not_reported_writable() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".claude")).unwrap();
    let path = home.path().join(".claude/settings.json");
    fs::write(&path, "{\"model\":\"read-only\"}\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
    }
    let settings = store(home.path()).read("claude").unwrap();
    assert_eq!(settings.model.as_deref(), Some("read-only"));
    assert!(!settings.writable);
    let error = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "claude".into(),
            model: Some("replacement".into()),
            effort: None,
            revision: settings.revision.unwrap(),
        })
        .unwrap_err();
    assert_eq!(error.code(), "SETTINGS_UNAVAILABLE");
}

#[test]
fn invalid_native_value_becomes_entry_level_unavailable() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".claude")).unwrap();
    let path = home.path().join(".claude/settings.json");
    fs::write(&path, "{\"model\":\"line\\nvalue\"}\n").unwrap();
    let entry = store(home.path())
        .read_all()
        .agents
        .into_iter()
        .find(|entry| entry.agent == "claude")
        .unwrap();
    assert!(entry.model.is_none());
    assert!(entry.message.is_some());
    assert!(
        entry
            .message
            .unwrap()
            .contains("native model value is invalid")
    );
}

#[test]
fn rejects_symlink_sources_without_following_them() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex")).unwrap();
    let target = home.path().join("target.toml");
    fs::write(&target, "model = \"secret\"\n").unwrap();
    std::os::unix::fs::symlink(&target, home.path().join(".codex/config.toml")).unwrap();

    let error = store(home.path()).read("codex").unwrap_err();
    assert_eq!(error.code(), "SETTINGS_UNAVAILABLE");
    assert_eq!(fs::read_to_string(target).unwrap(), "model = \"secret\"\n");
}

#[test]
fn rejects_nested_symlink_parent_without_following_an_external_directory() {
    let home = tempdir().unwrap();
    let outside = tempdir().unwrap();
    fs::create_dir_all(outside.path().join(".codex")).unwrap();
    let target = outside.path().join(".codex/config.toml");
    fs::write(&target, "model = \"external\"\n").unwrap();
    std::os::unix::fs::symlink(outside.path().join(".codex"), home.path().join(".codex")).unwrap();

    let error = store(home.path()).read("codex").unwrap_err();
    assert_eq!(error.code(), "SETTINGS_UNAVAILABLE");
    assert_eq!(
        fs::read_to_string(target).unwrap(),
        "model = \"external\"\n"
    );
    assert_eq!(store(home.path()).read_all().agents[0].model, None);
}

#[test]
fn rejects_group_writable_native_directory() {
    let home = tempdir().unwrap();
    let directory = home.path().join(".codex");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("config.toml"), "model = \"unsafe-dir\"\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o775)).unwrap();
    }
    let error = store(home.path()).read("codex").unwrap_err();
    assert_eq!(error.code(), "SETTINGS_UNAVAILABLE");
}

#[test]
fn malformed_source_is_reported_and_never_rewritten() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".claude")).unwrap();
    let path = home.path().join(".claude/settings.json");
    let original = "{\"model\":\n";
    fs::write(&path, original).unwrap();
    let entry = store(home.path())
        .read_all()
        .agents
        .into_iter()
        .find(|entry| entry.agent == "claude")
        .unwrap();
    assert!(entry.message.is_some());
    let error = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "claude".into(),
            model: Some("replacement".into()),
            effort: None,
            revision: entry.revision.unwrap(),
        })
        .unwrap_err();
    assert_eq!(error.code(), "SETTINGS_INVALID_CONFIG");
    assert_eq!(fs::read_to_string(path).unwrap(), original);
}

#[test]
fn missing_file_save_creates_private_native_document() {
    let home = tempdir().unwrap();
    let settings = store(home.path()).read("claude").unwrap();
    let saved = store(home.path())
        .save(&AgentSettingsSaveRequest {
            agent: "claude".into(),
            model: Some("claude-new".into()),
            effort: Some("high".into()),
            revision: settings.revision.unwrap(),
        })
        .unwrap();
    assert_eq!(saved.model.as_deref(), Some("claude-new"));
    assert_eq!(saved.effort.as_deref(), Some("high"));
    assert!(home.path().join(".claude/settings.json").is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(home.path().join(".claude/settings.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}

#[test]
fn nonregular_source_is_refused_without_creating_or_following_it() {
    let home = tempdir().unwrap();
    fs::create_dir_all(home.path().join(".codex/config.toml")).unwrap();
    let error = store(home.path()).read("codex").unwrap_err();
    assert_eq!(error.code(), "SETTINGS_UNAVAILABLE");
    assert!(home.path().join(".codex/config.toml").is_dir());
}

#[test]
fn read_does_not_create_missing_native_directories() {
    let home = tempdir().unwrap();
    let _ = store(home.path()).read("cursor").unwrap();
    assert!(!home.path().join(".cursor").exists());
}

struct RecordingRunner {
    request: Mutex<Option<ProcessRequest>>,
    stdout: Vec<u8>,
}

impl ProcessRunner for RecordingRunner {
    fn run(
        &self,
        request: &ProcessRequest,
    ) -> Result<ProcessResult, mac_worker::error::WorkerError> {
        *self.request.lock().unwrap() = Some(request.clone());
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: self.stdout.clone(),
            stderr: Vec::new(),
        })
    }
}

#[test]
fn typed_settings_transport_uses_fixed_remote_argv_and_json_stdin() {
    let response = AgentSettingsList { agents: vec![] };
    let runner = RecordingRunner {
        request: Mutex::new(None),
        stdout: [serde_json::to_vec(&response).unwrap(), b"\n".to_vec()].concat(),
    };
    let worker = WorkerEntry {
        name: "mini-1".into(),
        ssh: "mini-1".into(),
        slots: 1,
        capabilities: Vec::new(),
        remote_binary: "~/.local/bin/worker".into(),
    };
    let actual = SshJsonTransport::new(&runner)
        .agent_settings_get(&worker)
        .unwrap();
    assert_eq!(actual, response);
    let request = runner.request.lock().unwrap().clone().unwrap();
    assert_eq!(request.program.to_string_lossy(), "/usr/bin/ssh");
    assert_eq!(
        request.args.last().unwrap().to_string_lossy(),
        HostOperation::AgentSettingsGet.command()
    );
    assert_eq!(request.stdin.as_deref(), Some(b"{}".as_slice()));
}

#[test]
fn typed_settings_transport_keeps_user_values_in_json_stdin() {
    let response = AgentDefaultSettings {
        agent: "claude".into(),
        model: Some("model-from-host".into()),
        effort: Some("max".into()),
        effort_options: vec!["max".into()],
        source: "native-claude".into(),
        revision: Some("b".repeat(64)),
        writable: true,
        message: None,
    };
    let runner = RecordingRunner {
        request: Mutex::new(None),
        stdout: [serde_json::to_vec(&response).unwrap(), b"\n".to_vec()].concat(),
    };
    let worker = WorkerEntry {
        name: "mini-1".into(),
        ssh: "mini-1".into(),
        slots: 1,
        capabilities: Vec::new(),
        remote_binary: "~/.local/bin/worker".into(),
    };
    let request = AgentSettingsSaveRequest {
        agent: "claude".into(),
        model: Some("model with spaces".into()),
        effort: Some("max".into()),
        revision: "a".repeat(64),
    };
    let _ = SshJsonTransport::new(&runner)
        .agent_settings_set(&worker, &request)
        .unwrap();
    let process = runner.request.lock().unwrap().clone().unwrap();
    assert!(
        process
            .args
            .iter()
            .all(|argument| !argument.to_string_lossy().contains("model with spaces"))
    );
    let expected_stdin = serde_json::to_vec(&request).unwrap();
    assert_eq!(process.stdin.as_deref(), Some(expected_stdin.as_slice()));
    assert_eq!(
        process.args.last().unwrap().to_string_lossy(),
        HostOperation::AgentSettingsSet.command()
    );
}
