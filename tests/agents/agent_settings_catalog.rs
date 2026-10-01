use std::{fs, path::Path};

use mac_worker::test_support::agents::agent_settings::{ModelOption, NativeAgentSettingsStore};
use serde_json::json;
use tempfile::tempdir;

fn write(home: &Path, relative: &str, contents: &str) {
    let path = home.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn ids(options: &[ModelOption]) -> Vec<&str> {
    options.iter().map(|option| option.id.as_str()).collect()
}

#[test]
fn codex_live_catalog_filters_visibility_orders_priority_and_retains_hidden_current() {
    let home = tempdir().unwrap();
    write(
        home.path(),
        ".codex/config.toml",
        "model = \"hidden-current\"\n",
    );
    write(
        home.path(),
        ".codex/models_cache.json",
        r#"{"models":[{"slug":"stale","visibility":"list"}]}"#,
    );
    let settings = NativeAgentSettingsStore::new(home.path())
        .with_codex_catalog(Some(json!({"models": [
            {"slug":"later", "visibility":"list", "priority":8},
            {"slug":"hidden-other", "visibility":"hide", "priority":0},
            {"slug":"missing-visibility", "priority":0},
            {"slug":"first", "display_name":"First Model", "visibility":"list", "priority":1},
            {"slug":"tied", "visibility":"list", "priority":1},
            {"slug":"first", "visibility":"list", "priority":2},
            {"slug":"bad\nmodel", "visibility":"list", "priority":0},
            {"slug":"hidden-current", "display_name":"Hidden Current", "visibility":"hide", "supported_reasoning_levels":[{"effort":"max"}], "additional_speed_tiers":["fast"]}
        ]})))
        .read("codex").unwrap();
    assert_eq!(
        ids(&settings.model_options),
        ["first", "tied", "later", "hidden-current"]
    );
    assert_eq!(settings.model_options[0].label, "First Model");
    assert_eq!(settings.model_options[3].label, "Hidden Current");
    assert_eq!(settings.effort_options, ["max"]);
    assert!(settings.fast_supported);
    assert_eq!(settings.model_catalog_source.as_deref(), Some("live"));
    assert_eq!(settings.model_catalog_profile, None);
}

#[test]
fn codex_live_catalog_keeps_an_unknown_current_model_without_claiming_capabilities() {
    let home = tempdir().unwrap();
    write(home.path(), ".codex/config.toml", "model = \"retired\"\n");
    let settings = NativeAgentSettingsStore::new(home.path())
        .with_codex_catalog(Some(
            json!({"models":[{"slug":"current", "visibility":"list"}]}),
        ))
        .read("codex")
        .unwrap();
    assert_eq!(ids(&settings.model_options), ["current", "retired"]);
    assert_eq!(settings.model_options[1].label, "retired");
    assert!(settings.effort_options.is_empty());
    assert!(!settings.fast_supported);
    assert!(!settings.model_options[1].capabilities_known);
    assert_eq!(settings.model_catalog_source.as_deref(), Some("live"));
}

#[test]
fn absent_empty_or_invalid_live_codex_catalog_uses_the_unchanged_file_path() {
    let home = tempdir().unwrap();
    write(home.path(), ".codex/config.toml", "model = \"cached\"\n");
    write(
        home.path(),
        ".codex/models_cache.json",
        r#"{"models":[{"slug":"cached","display_name":"Cached","visibility":"list","supported_reasoning_levels":[{"effort":"high"}],"additional_speed_tiers":["fast"]}]}"#,
    );
    let expected = NativeAgentSettingsStore::new(home.path())
        .read("codex")
        .unwrap();
    for live in [
        None,
        Some(json!({"models":[]})),
        Some(json!({"models":"bad"})),
        Some(json!({"models":[{"slug":"hidden","visibility":"hide"}]})),
        Some(json!({"models":[{"slug":"bad\nmodel","visibility":"list"}]})),
    ] {
        let settings = NativeAgentSettingsStore::new(home.path())
            .with_codex_catalog(live)
            .read("codex")
            .unwrap();
        assert_eq!(settings, expected);
        assert_eq!(settings.model_catalog_source.as_deref(), Some("remembered"));
        assert_eq!(ids(&settings.model_options), ["cached"]);
    }
}

#[test]
fn opencode_live_lines_keep_cli_order_and_cached_labels_across_providers() {
    let home = tempdir().unwrap();
    write(
        home.path(),
        ".config/opencode/opencode.json",
        r#"{"model":"retired/current"}"#,
    );
    write(
        home.path(),
        ".cache/opencode/models.json",
        r#"{
        "opencode":{"models":{"zen-free":{"name":"Zen Free","reasoning":true},"paid":{"name":"Unavailable"}}},
        "zai":{"models":{"glm":{"name":"GLM"}}},
        "opencode-go":{"models":{"fast":{"name":"Go Fast"}}}
    }"#,
    );
    let output = concat!(
        "Loading models...\n\u{1b}[32mopencode-go/fast\u{1b}[0m\r\n",
        "zai/glm\nopencode/zen-free\nzai/glm\n",
        "\u{1b}]0;models\u{7}\u{1b}[2KLoading / catalogue\n",
        "\u{1b}]8;;https://example.invalid\u{1b}\\other/model\u{1b}]8;;\u{1b}\\\n",
        "no-provider\n/model\nprovider/\nbad!provider/model\n",
        "provider/white space\nprovider/tab\there\nprovider/unicode\u{2003}space\n",
        " provider/leading\nprovider/trailing \nprøvider/model\nprovider/control\u{7}\n",
        "provider/truncated\u{1b}[\n"
    );
    let settings = NativeAgentSettingsStore::new(home.path())
        .with_opencode_catalog(Some(format!("{output}provider/{}\n", "x".repeat(257))))
        .read("opencode")
        .unwrap();
    assert_eq!(
        ids(&settings.model_options),
        [
            "opencode-go/fast",
            "zai/glm",
            "opencode/zen-free",
            "other/model",
            "retired/current"
        ]
    );
    assert_eq!(
        settings
            .model_options
            .iter()
            .map(|option| option.label.as_str())
            .collect::<Vec<_>>(),
        [
            "Go Fast",
            "GLM",
            "Zen Free",
            "other/model",
            "retired/current"
        ]
    );
    for option in &settings.model_options {
        assert!(option.effort_options.is_empty());
        assert!(!option.fast_supported);
    }
    assert!(settings.effort_options.is_empty());
    assert_eq!(settings.fast, None);
    assert!(!settings.fast_supported);
    assert_eq!(settings.model_catalog_source.as_deref(), Some("live"));
    assert_eq!(settings.model_catalog_profile, None);
}

#[test]
fn opencode_live_catalog_works_without_a_label_cache_and_accepts_provider_punctuation() {
    let home = tempdir().unwrap();
    let settings = NativeAgentSettingsStore::new(home.path())
        .with_opencode_catalog(Some("Provider-1._/family/model\n".into()))
        .read("opencode")
        .unwrap();
    assert_eq!(ids(&settings.model_options), ["Provider-1._/family/model"]);
    assert_eq!(settings.model_options[0].label, "Provider-1._/family/model");
    assert_eq!(settings.model_catalog_source.as_deref(), Some("live"));
}

#[test]
fn absent_empty_or_invalid_live_opencode_catalog_preserves_current_provider_file_fallback() {
    let home = tempdir().unwrap();
    write(
        home.path(),
        ".config/opencode/opencode.json",
        r#"{"model":"zai/glm"}"#,
    );
    write(
        home.path(),
        ".cache/opencode/models.json",
        r#"{"zai":{"models":{"glm":{"name":"GLM"}}},"other":{"models":{"hidden":{"name":"Other Provider"}}}}"#,
    );
    let expected = NativeAgentSettingsStore::new(home.path())
        .read("opencode")
        .unwrap();
    for live in [
        None,
        Some(String::new()),
        Some("Loading models...\nprovider/\nbad!provider/model\n".into()),
    ] {
        let settings = NativeAgentSettingsStore::new(home.path())
            .with_opencode_catalog(live)
            .read("opencode")
            .unwrap();
        assert_eq!(settings, expected);
        assert_eq!(ids(&settings.model_options), ["zai/glm"]);
        assert_eq!(settings.model_catalog_source.as_deref(), Some("remembered"));
    }
}

#[test]
fn live_catalogs_retain_current_model_at_128_options() {
    let home = tempdir().unwrap();
    write(home.path(), ".codex/config.toml", "model = \"retired\"\n");
    write(
        home.path(),
        ".config/opencode/opencode.json",
        r#"{"model":"retired/current"}"#,
    );
    let models = (0..150)
        .map(|index| json!({"slug":format!("model-{index}"),"visibility":"list","priority":index}))
        .collect::<Vec<_>>();
    let lines = (0..150)
        .map(|index| format!("provider/model-{index}\n"))
        .collect::<String>();
    let store = NativeAgentSettingsStore::new(home.path())
        .with_codex_catalog(Some(json!({"models":models})))
        .with_opencode_catalog(Some(lines));
    for (agent, first, last) in [
        ("codex", "model-0", "retired"),
        ("opencode", "provider/model-0", "retired/current"),
    ] {
        let settings = store.read(agent).unwrap();
        assert_eq!(settings.model_options.len(), 128);
        assert_eq!(settings.model_options.first().unwrap().id, first);
        assert_eq!(settings.model_options.last().unwrap().id, last);
    }
}
