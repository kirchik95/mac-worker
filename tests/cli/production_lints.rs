use std::process::{Command, Output};

fn compile_fixture(features: &[&str]) -> Output {
    let directory = tempfile::tempdir().unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/test_support_lints.rs");
    let mut command = Command::new("rustc");
    command
        .arg(source)
        .args([
            "--crate-name",
            "test_support_lints",
            "--crate-type",
            "lib",
            "--edition=2024",
            "--emit=metadata",
            "--error-format=json",
            "--deny=warnings",
            "--check-cfg=cfg(feature, values(\"test-support\", \"lint-probe\"))",
            "-Cdebug-assertions=no",
        ])
        .arg("--out-dir")
        .arg(directory.path());
    for feature in features {
        command.arg("--cfg").arg(format!("feature=\"{feature}\""));
    }
    command.output().unwrap()
}

#[test]
fn production_private_helpers_are_linted_without_test_support() {
    for features in [&[][..], &["test-support"][..]] {
        let output = compile_fixture(features);
        assert!(
            output.status.success(),
            "fixture must compile with {features:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    for features in [&["lint-probe"][..], &["test-support", "lint-probe"][..]] {
        let output = compile_fixture(features);
        assert!(!output.status.success(), "unused helper escaped the lint");
        let diagnostics = String::from_utf8(output.stderr).unwrap();
        let errors = diagnostics
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|diagnostic| diagnostic["level"] == "error")
            .filter(|diagnostic| !diagnostic["spans"].as_array().unwrap().is_empty())
            .collect::<Vec<_>>();
        assert_eq!(errors.len(), 1, "{diagnostics}");
        assert_eq!(errors[0]["code"]["code"], "dead_code", "{diagnostics}");
        assert!(
            errors[0]["message"]
                .as_str()
                .unwrap()
                .contains("unused_production_helper"),
            "{diagnostics}"
        );
    }
}
