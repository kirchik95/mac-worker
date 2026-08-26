mod support;

use std::{fs, os::unix::fs::symlink, time::Duration};

use mac_worker::{
    error::WorkerError,
    project_config::{ProjectSettings, ResourceClass},
    requirements::RequirementDetector,
};
use tempfile::tempdir;

use support::GitRepo;

#[test]
fn absent_project_settings_file_uses_v1_defaults() {
    // This catches treating optional project configuration as mandatory, or
    // changing a default that controls unconfigured projects.
    let repo = GitRepo::init();

    let settings = ProjectSettings::load(repo.root(), &[]).unwrap();

    assert_eq!(settings.requires, Vec::<String>::new());
    assert_eq!(settings.resource_class, ResourceClass::Heavy);
    assert_eq!(settings.timeout, Duration::from_secs(1800));
    assert_eq!(settings.snapshot.include_untracked, Vec::<String>::new());
    assert_eq!(settings.snapshot.include_empty_dirs, Vec::<String>::new());
    assert_eq!(settings.snapshot.allow_sensitive, Vec::<String>::new());
    assert_eq!(settings.artifacts.include, Vec::<String>::new());
    assert_eq!(settings.artifacts.max_total_bytes, None);
}

#[test]
fn project_settings_merge_explicit_and_cli_snapshot_inputs() {
    // This catches losing the file include or putting a CLI include before it.
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        br#"version = 1
requires = ["darwin-arm64", "node"]
resource_class = "heavy"
timeout = "30m"

[snapshot]
include_untracked = ["fixtures/generated/**", "fixtures/generated/**"]
include_empty_dirs = ["fixtures/empty"]
allow_sensitive = ["fixtures/test.env"]

[artifacts]
include = ["coverage/**"]
max_total_bytes = 536870912
"#,
    );

    let settings = ProjectSettings::load(
        repo.root(),
        &[
            "tmp/contract.json".to_owned(),
            "fixtures/generated/**".to_owned(),
        ],
    )
    .unwrap();

    assert_eq!(settings.requires, vec!["darwin-arm64", "node"]);
    assert_eq!(settings.resource_class, ResourceClass::Heavy);
    assert_eq!(settings.timeout, Duration::from_secs(1800));
    assert_eq!(
        settings.snapshot.include_untracked,
        vec!["fixtures/generated/**", "tmp/contract.json"]
    );
    assert_eq!(
        settings.snapshot.include_empty_dirs,
        vec!["fixtures/empty".to_owned()]
    );
    assert_eq!(
        settings.snapshot.allow_sensitive,
        vec!["fixtures/test.env".to_owned()]
    );
    assert_eq!(settings.artifacts.include, vec!["coverage/**"]);
    assert_eq!(settings.artifacts.max_total_bytes, Some(536_870_912));
}

#[test]
fn project_settings_reject_invalid_values_and_unknown_fields_at_each_level() {
    // This catches permissive deserialization and accepting inputs that cannot
    // safely describe a project boundary.
    let cases = [
        ("requires = [\"node\", \"node\"]", "duplicate requirements"),
        ("requires = [\"Node\"]", "uppercase requirement"),
        ("timeout = \"half an hour\"", "invalid duration"),
        ("resource_class = \"small\"", "invalid resource class"),
        ("unexpected = true", "unknown root field"),
        ("[snapshot]\nunexpected = true", "unknown snapshot field"),
        ("[artifacts]\nunexpected = true", "unknown artifacts field"),
        (
            "[snapshot]\ninclude_untracked = [\"**/generated\"]",
            "glob first component",
        ),
        (
            "[snapshot]\ninclude_untracked = [\"fixtures/../generated/**\"]",
            "parent path component",
        ),
        (
            "[snapshot]\ninclude_untracked = [\"/fixtures/generated/**\"]",
            "absolute include path",
        ),
        (
            "[snapshot]\ninclude_empty_dirs = [\"fixtures/*\"]",
            "glob empty directory path",
        ),
        (
            "[snapshot]\nallow_sensitive = [\"fixtures/*.env\"]",
            "glob sensitive allowlist path",
        ),
    ];

    for (contents, name) in cases {
        let repo = GitRepo::init();
        repo.write(".worker.toml", contents.as_bytes());

        let error = ProjectSettings::load(repo.root(), &[]).unwrap_err();

        assert!(matches!(error, WorkerError::Config(_)), "{name}: {error}");
    }
}

#[test]
fn requirement_detector_reports_root_level_regular_file_indicators_once_in_lexical_order() {
    // This catches missing mappings, recursive source scanning, and unstable
    // capability output when several files imply one capability.
    let root = tempdir().unwrap();
    for name in [
        "package.json",
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "Gemfile",
        ".ruby-version",
        "pyproject.toml",
        "requirements.txt",
        ".python-version",
        "go.mod",
        "Package.swift",
        "global.json",
        "solution.sln",
        "app.csproj",
        "Dockerfile",
        "compose.yaml",
        "compose.yml",
        "playwright.config.js",
        "playwright.config.ts",
    ] {
        fs::write(root.path().join(name), b"indicator").unwrap();
    }
    fs::create_dir(root.path().join("nested")).unwrap();
    fs::write(root.path().join("nested/package.json"), b"ignored").unwrap();

    let requirements = RequirementDetector::detect(root.path()).unwrap();

    assert_eq!(
        requirements,
        vec![
            "browser", "docker", "dotnet", "go", "node", "python", "ruby", "swift",
        ]
    );
}

#[test]
fn requirement_detector_ignores_a_symlinked_indicator_file() {
    // This catches following a path controlled outside the project root during
    // requirement detection.
    let root = tempdir().unwrap();
    let target = root.path().join("outside-package.json");
    fs::write(&target, b"indicator").unwrap();
    symlink(&target, root.path().join("package.json")).unwrap();

    let requirements = RequirementDetector::detect(root.path()).unwrap();

    assert_eq!(requirements, Vec::<String>::new());
}
