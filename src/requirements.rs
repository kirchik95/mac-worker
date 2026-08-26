use std::{collections::BTreeSet, fs, path::Path};

use crate::error::WorkerError;

pub struct RequirementDetector;

impl RequirementDetector {
    pub fn detect(root: &Path) -> Result<Vec<String>, WorkerError> {
        let mut requirements = BTreeSet::new();

        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.file_type().is_file() {
                continue;
            }

            if let Some(requirement) = entry
                .file_name()
                .to_str()
                .and_then(requirement_for_indicator)
            {
                requirements.insert(requirement.to_owned());
            }
        }

        Ok(requirements.into_iter().collect())
    }
}

fn requirement_for_indicator(name: &str) -> Option<&'static str> {
    match name {
        "package.json" | "package-lock.json" | "yarn.lock" | "pnpm-lock.yaml" => Some("node"),
        "Gemfile" | ".ruby-version" => Some("ruby"),
        "pyproject.toml" | "requirements.txt" | ".python-version" => Some("python"),
        "go.mod" => Some("go"),
        "Package.swift" => Some("swift"),
        "global.json" => Some("dotnet"),
        "Dockerfile" | "compose.yaml" | "compose.yml" => Some("docker"),
        "playwright.config.js" | "playwright.config.ts" => Some("browser"),
        _ if name.ends_with(".sln") || name.ends_with(".csproj") => Some("dotnet"),
        _ => None,
    }
}
