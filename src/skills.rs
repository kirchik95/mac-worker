use std::path::{Path, PathBuf};

use clap::{ColorChoice, CommandFactory};

use crate::{
    cli::{Cli, SkillsCommand},
    error::WorkerError,
    project_config::{PROJECT_CONFIG, ProjectSettings},
};

/// Compile-time copies of the repository skill kernels.
///
/// `include_str!` is the guarantee that a built binary cannot drift from
/// `.claude/skills/<name>/SKILL.md`. The Grammar section is *not* in those
/// files; it is rendered from the live clap tree so agents do not trust a
/// handwritten flag list.
const POOL_DISPATCH_KERNEL: &str = include_str!("../.claude/skills/pool-dispatch/SKILL.md");
const POOL_TASK_AUTHORING_KERNEL: &str =
    include_str!("../.claude/skills/pool-task-authoring/SKILL.md");

struct BundledSkill {
    name: &'static str,
    kernel: &'static str,
}

const BUNDLED_SKILLS: &[BundledSkill] = &[
    BundledSkill {
        name: "pool-dispatch",
        kernel: POOL_DISPATCH_KERNEL,
    },
    BundledSkill {
        name: "pool-task-authoring",
        kernel: POOL_TASK_AUTHORING_KERNEL,
    },
];

/// Subcommands whose clap help is spliced into the generated Grammar section.
/// The list is the public task loop plus the two observer commands an
/// orchestrator actually runs; `task diff` stays out because the kernel
/// already points at `worker task <cmd> --help` for anything not listed.
const GRAMMAR_PATHS: &[&[&str]] = &[
    &["task", "submit"],
    &["task", "batch"],
    &["task", "list"],
    &["task", "status"],
    &["task", "logs"],
    &["task", "say"],
    &["task", "cancel"],
    &["task", "result"],
    &["task", "fetch"],
    &["task", "close"],
    &["task", "wait"],
    &["task", "reconcile"],
    &["workers"],
    &["dashboard"],
];

pub(crate) fn execute(command: SkillsCommand, cwd: &Path) -> Result<String, WorkerError> {
    match command {
        SkillsCommand::List => Ok(render_list()),
        SkillsCommand::Get { name, grammar_only } => render_get(&name, grammar_only, cwd),
    }
}

fn bundled_skill(name: &str) -> Option<&'static BundledSkill> {
    BUNDLED_SKILLS.iter().find(|skill| skill.name == name)
}

fn render_list() -> String {
    BUNDLED_SKILLS
        .iter()
        .map(|skill| {
            format!(
                "{}: {}",
                skill.name,
                one_line_description(skill.kernel).unwrap_or_else(|| skill.name.to_owned())
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_get(name: &str, grammar_only: bool, cwd: &Path) -> Result<String, WorkerError> {
    let skill = bundled_skill(name).ok_or_else(|| {
        WorkerError::Config(format!("unknown skill {name:?}; see `worker skills list`"))
    })?;
    let grammar = render_grammar(cwd)?;
    if grammar_only {
        Ok(grammar)
    } else {
        Ok(format!("{}{grammar}", skill.kernel))
    }
}

fn render_grammar(cwd: &Path) -> Result<String, WorkerError> {
    let mut markdown = String::from("## Grammar\n\n");
    markdown.push_str(
        "Generated from this binary's clap definitions. Use this section as the only grammar.\n\n",
    );
    markdown.push_str(&render_task_defaults(cwd)?);
    markdown.push('\n');
    for path in GRAMMAR_PATHS {
        markdown.push_str("### worker ");
        markdown.push_str(&path.join(" "));
        markdown.push_str("\n\n```text\n");
        markdown.push_str(render_subcommand_help(path)?.trim_end());
        markdown.push_str("\n```\n\n");
    }
    Ok(markdown.trim_end().to_string())
}

/// Effective `[task]` defaults from the current git project's `.worker.toml`.
///
/// Model and effort are omitted from config more often than `default_agent`,
/// so missing values print as `not configured` rather than inventing a
/// model name. Env profile names and env values are never printed.
fn render_task_defaults(cwd: &Path) -> Result<String, WorkerError> {
    let Some(root) = find_git_root(cwd) else {
        return Ok("Effective [task] defaults (no git project found):\n\
             - default_agent: not configured\n\
             - model: not configured\n\
             - effort: not configured\n"
            .into());
    };
    let settings = ProjectSettings::load(&root, &[])?;
    let config_path = root.join(PROJECT_CONFIG);
    let source = if config_path.is_file() {
        format!("from {}", config_path.display())
    } else {
        format!("built-in; no {PROJECT_CONFIG} at {}", config_path.display())
    };
    Ok(format!(
        "Effective [task] defaults ({source}):\n- default_agent: {}\n- model: {}\n- effort: {}\n",
        settings.task.default_agent,
        optional_config_value(settings.task.model.as_deref()),
        optional_config_value(settings.task.effort.as_deref()),
    ))
}

fn optional_config_value(value: Option<&str>) -> &str {
    match value {
        Some(value) if !value.is_empty() => value,
        _ => "not configured",
    }
}

fn find_git_root(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let git = current.join(".git");
        if git.is_dir() || git.is_file() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn render_subcommand_help(path: &[&str]) -> Result<String, WorkerError> {
    let mut command = Cli::command().color(ColorChoice::Never).term_width(100);
    command.build();
    let mut current = &command;
    for name in path {
        current = current.find_subcommand(name).ok_or_else(|| {
            WorkerError::Protocol(format!("missing clap subcommand {}", path.join(" ")))
        })?;
    }
    let bin_name = format!("worker {}", path.join(" "));
    Ok(current
        .clone()
        .bin_name(bin_name)
        .color(ColorChoice::Never)
        .render_long_help()
        .to_string())
}

fn one_line_description(kernel: &str) -> Option<String> {
    let yaml = yaml_frontmatter(kernel)?;
    let description = yaml_scalar(yaml, "description")?;
    let sentence = description
        .split_once('.')
        .map(|(head, _)| format!("{head}."))
        .unwrap_or_else(|| description.to_owned());
    Some(sentence)
}

fn yaml_frontmatter(kernel: &str) -> Option<&str> {
    let rest = kernel.strip_prefix("---\n")?;
    let (yaml, _) = rest.split_once("\n---")?;
    Some(yaml)
}

fn yaml_scalar<'a>(yaml: &'a str, key: &str) -> Option<&'a str> {
    yaml.lines().find_map(|line| {
        let (found_key, value) = line.split_once(':')?;
        (found_key == key).then(|| unquote_yaml_scalar(value.trim()))
    })
}

fn unquote_yaml_scalar(value: &str) -> &str {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::{
        BUNDLED_SKILLS, POOL_DISPATCH_KERNEL, execute, render_get, render_list,
        render_task_defaults,
    };
    use crate::cli::SkillsCommand;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn list_prints_bundled_guide_names_with_one_line_descriptions() {
        let listed = render_list();
        assert!(listed.contains(
            "pool-dispatch: Dispatch independent coding tasks through the mac-worker pool and collect their results."
        ));
        assert!(listed.contains(
            "pool-task-authoring: Turn an objective into independent, testable prompts for headless agents in the mac-worker pool."
        ));
        assert_eq!(listed.lines().count(), BUNDLED_SKILLS.len());
        assert_eq!(
            BUNDLED_SKILLS
                .iter()
                .map(|skill| skill.name)
                .collect::<Vec<_>>(),
            vec!["pool-dispatch", "pool-task-authoring"]
        );
    }

    #[test]
    fn get_pool_dispatch_starts_with_the_kernel_and_appends_live_grammar() {
        let directory = tempdir().unwrap();
        let output = render_get("pool-dispatch", false, directory.path()).unwrap();
        assert!(
            output.starts_with(POOL_DISPATCH_KERNEL),
            "skills get must start with the embedded kernel"
        );
        let generated = &output[POOL_DISPATCH_KERNEL.len()..];
        assert!(
            generated.contains("## Grammar"),
            "the generated suffix must contain a Grammar heading"
        );
        assert!(
            generated.contains("Usage: worker task submit"),
            "grammar must include the live task submit usage line, got:\n{generated}"
        );
    }

    #[test]
    fn grammar_only_omits_the_kernel_and_still_includes_submit_usage() {
        let directory = tempdir().unwrap();
        let output = render_get("pool-dispatch", true, directory.path()).unwrap();
        assert!(output.starts_with("## Grammar"));
        assert!(!output.starts_with("---"));
        assert!(output.contains("Usage: worker task submit"));
        assert!(output.contains("--message"));
        assert!(output.contains("Usage: worker task wait"));
    }

    #[test]
    fn grammar_prints_configured_task_defaults_and_never_env_profiles() {
        let directory = tempdir().unwrap();
        fs::create_dir(directory.path().join(".git")).unwrap();
        fs::write(
            directory.path().join(".worker.toml"),
            r#"version = 1
[task]
default_agent = "cursor"
model = "test-model"
effort = "high"
env_profile = "secret-profile"
"#,
        )
        .unwrap();

        let defaults = render_task_defaults(directory.path()).unwrap();
        assert!(defaults.contains("default_agent: cursor"));
        assert!(defaults.contains("model: test-model"));
        assert!(defaults.contains("effort: high"));
        assert!(defaults.contains(".worker.toml"));
        assert!(
            !defaults.contains("secret-profile"),
            "env profile names must not appear in the skill grammar"
        );
        assert!(!defaults.contains("env_profile"));
    }

    #[test]
    fn grammar_prints_not_configured_without_a_git_project() {
        let directory = tempdir().unwrap();
        let defaults = render_task_defaults(directory.path()).unwrap();
        assert!(defaults.contains("default_agent: not configured"));
        assert!(defaults.contains("model: not configured"));
        assert!(defaults.contains("effort: not configured"));
    }

    #[test]
    fn execute_list_matches_the_list_renderer() {
        let directory = tempdir().unwrap();
        let output = execute(SkillsCommand::List, directory.path()).unwrap();
        assert_eq!(output, render_list());
    }
}
