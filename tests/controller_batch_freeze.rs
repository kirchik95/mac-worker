#[allow(dead_code)]
mod support;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use mac_worker::{
    config::Config,
    controller::{BatchKind, freeze_laptop_batch},
    dag::DagBase,
    error::WorkerError,
    paths::PathLayout,
    process::SystemProcessRunner,
    project_state::ProjectState,
    task::BaseOid,
};
use support::GitRepo;

const RUNNER: SystemProcessRunner = SystemProcessRunner;

struct Harness {
    _root: tempfile::TempDir,
    repo: GitRepo,
    paths: PathLayout,
    config: Config,
    first_oid: BaseOid,
}

impl Harness {
    fn new() -> Self {
        let root = tempfile::TempDir::new().unwrap();
        let paths = support::task_harness::paths(root.path().canonicalize().unwrap());
        fs::create_dir_all(&paths.cache).unwrap();
        let repo = GitRepo::init();
        repo.write("src/lib.rs", b"fn main() {}\n");
        repo.write(
            ".worker.toml",
            b"[task]\ntimeout = \"30m\"\nmax_followups = 7\n",
        );
        repo.commit_all("base");
        let first_oid = head_oid(&repo);
        Self {
            _root: root,
            repo,
            paths,
            config: empty_laptop_config(),
            first_oid,
        }
    }

    fn extra_commit(&self, path: &str, contents: &[u8], message: &str) -> BaseOid {
        self.repo.write(path, contents);
        self.repo.commit_all(message);
        head_oid(&self.repo)
    }

    fn write_batch(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.repo.root().join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, contents).unwrap();
        path
    }

    fn freeze(
        &self,
        batch: &Path,
        run_name: Option<String>,
        max_parallel: Option<u32>,
    ) -> Result<mac_worker::controller::LaptopFrozenBatch, WorkerError> {
        freeze_laptop_batch(
            &RUNNER,
            &self.config,
            &self.paths,
            self.repo.root(),
            batch,
            run_name,
            max_parallel,
        )
    }
}

fn empty_laptop_config() -> Config {
    Config::parse("version = 1\nworkers = []\n").unwrap()
}

fn head_oid(repo: &GitRepo) -> BaseOid {
    let output = repo.git(&["rev-parse", "HEAD"]);
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn code(error: &WorkerError) -> String {
    error.public_code()
}

#[test]
fn actual_batchfile_rejects_project_key() {
    let harness = Harness::new();
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
id = "one"
project = "other"
prompt = "do one"
"#,
    );
    let error = harness.freeze(&file, None, None).unwrap_err();
    assert_eq!(code(&error), "TASK_CONFIG_INVALID");
    assert!(
        error.to_string().contains("project") || error.to_string().contains("unknown"),
        "{}",
        error
    );
}

#[test]
fn omitted_max_parallel_stays_none_with_empty_laptop_workers() {
    let harness = Harness::new();
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
prompt = "do work"
"#,
    );
    let frozen = harness.freeze(&file, Some("run-a".into()), None).unwrap();
    assert_eq!(frozen.body().kind, BatchKind::Independent);
    assert_eq!(frozen.body().max_parallel, None);
    assert_eq!(frozen.body().name.as_deref(), Some("run-a"));
    assert_eq!(frozen.body().nodes.len(), 1);
    assert_eq!(frozen.sources().len(), 1);
    assert_eq!(frozen.sources()[0].expected_oid(), &harness.first_oid);
    assert_eq!(
        frozen.body().sources[0].request_id,
        frozen.sources()[0].request_id()
    );
    assert!(frozen.sources()[0].git_path().exists());
    assert!(!harness.paths.state.exists());
}

#[test]
fn explicit_zero_max_parallel_is_rejected() {
    let harness = Harness::new();
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
prompt = "do work"
"#,
    );
    let error = harness.freeze(&file, None, Some(0)).unwrap_err();
    assert_eq!(code(&error), "TASK_CONFIG_INVALID");
}

#[test]
fn same_source_dedups_distinct_bases_are_multiple() {
    let harness = Harness::new();
    let second = harness.extra_commit("src/lib.rs", b"fn main() { 2 }\n", "second");
    let file = harness.write_batch(
        "batch.toml",
        &format!(
            r#"version = 1
agent = "codex"

[[tasks]]
id = "old"
base = "{old}"
prompt = "from old"

[[tasks]]
id = "also-old"
base = "{old}"
prompt = "also from old"

[[tasks]]
id = "new"
base = "{new}"
prompt = "from new"
"#,
            old = harness.first_oid,
            new = second
        ),
    );
    let frozen = harness.freeze(&file, None, Some(2)).unwrap();
    assert_eq!(frozen.body().kind, BatchKind::Independent);
    assert_eq!(frozen.body().max_parallel, Some(2));
    assert_eq!(frozen.sources().len(), 2);
    assert_eq!(frozen.body().sources.len(), 2);
    let oids: Vec<_> = frozen
        .sources()
        .iter()
        .map(|source| source.expected_oid().clone())
        .collect();
    assert!(oids.contains(&harness.first_oid));
    assert!(oids.contains(&second));
    assert_eq!(
        frozen.sources()[0].git_path(),
        frozen.sources()[1].git_path()
    );
    assert_ne!(
        frozen.sources()[0].request_id(),
        frozen.sources()[1].request_id()
    );
    let project = ProjectState::load(&RUNNER, harness.repo.root(), &[]).unwrap();
    for source in frozen.sources() {
        assert_eq!(source.project_id(), project.context.project_id);
        assert_eq!(source.worktree_id(), project.context.worktree_id);
        assert_eq!(
            source.pin_ref(),
            format!("refs/mac-worker/requests/{}", source.request_id())
        );
    }
}

#[test]
fn effective_limits_come_from_project_settings() {
    let harness = Harness::new();
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
prompt = "limits"
"#,
    );
    let frozen = harness.freeze(&file, None, None).unwrap();
    let spec = &frozen.body().nodes.values().next().unwrap().frozen;
    assert_eq!(spec.timeout_millis, 30 * 60 * 1000);
    assert_eq!(spec.max_followups, 7);
}

#[test]
fn freeze_is_stable_after_prompt_settings_and_head_mutations() {
    let harness = Harness::new();
    harness.repo.write("tasks/do.md", b"original prompt\n");
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
id = "one"
prompt_file = "tasks/do.md"
"#,
    );
    let frozen = harness.freeze(&file, Some("stable".into()), None).unwrap();
    let saved = serde_json::to_value(frozen.body()).unwrap();
    let saved_oid = frozen.sources()[0].expected_oid().clone();
    let pin = frozen.sources()[0].pin_ref().to_owned();
    let git_path = frozen.sources()[0].git_path().to_path_buf();

    harness.repo.write("tasks/do.md", b"mutated prompt\n");
    harness.repo.write(
        ".worker.toml",
        b"[task]\ntimeout = \"10m\"\nmax_followups = 3\n",
    );
    harness.extra_commit("src/lib.rs", b"fn main() { mutated }\n", "mutate head");

    assert_eq!(serde_json::to_value(frozen.body()).unwrap(), saved);
    assert_eq!(frozen.sources()[0].expected_oid(), &saved_oid);
    assert_ne!(saved_oid, head_oid(&harness.repo));
    let spec = &frozen.body().nodes["one"].frozen;
    assert!(spec.prompt.contains("original prompt"));
    assert!(!spec.prompt.contains("mutated prompt"));
    assert_eq!(spec.timeout_millis, 30 * 60 * 1000);
    let shown = Command::new("/usr/bin/git")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .args(["--git-dir"])
        .arg(&git_path)
        .args(["rev-parse", pin.as_str()])
        .output()
        .unwrap();
    assert!(shown.status.success());
    let pinned: BaseOid = String::from_utf8(shown.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(pinned, saved_oid);
}

#[test]
fn from_node_has_no_source_and_stays_waiting() {
    let harness = Harness::new();
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
id = "parent"
prompt = "parent work"

[[tasks]]
id = "child"
base = "from:parent"
prompt = "child work"
"#,
    );
    let frozen = harness.freeze(&file, None, None).unwrap();
    assert_eq!(frozen.body().kind, BatchKind::Dag);
    assert_eq!(frozen.sources().len(), 1);
    assert_eq!(frozen.body().sources.len(), 1);
    let child = &frozen.body().nodes["child"];
    assert!(matches!(child.base, DagBase::From { ref parent } if parent == "parent"));
    assert_eq!(child.depends_on, vec!["parent".to_string()]);
    assert_eq!(child.state, mac_worker::dag::DagNodeState::Waiting);
    assert!(child.bound_oid.is_none());
    assert!(child.claimed_by.is_none());
    let parent = &frozen.body().nodes["parent"];
    assert!(matches!(parent.base, DagBase::Frozen { .. }));
    assert!(!harness.paths.state.exists());
}

#[test]
fn pinned_worker_is_copied_with_empty_laptop_inventory() {
    let harness = Harness::new();
    let file = harness.write_batch(
        "batch.toml",
        r#"version = 1
agent = "codex"
worker = "mini-1"

[[tasks]]
prompt = "pinned"
"#,
    );
    let frozen = harness.freeze(&file, None, None).unwrap();
    let spec = &frozen.body().nodes.values().next().unwrap().frozen;
    assert_eq!(spec.worker.as_deref(), Some("mini-1"));
}

#[test]
fn empty_and_duplicate_graphs_are_rejected() {
    let harness = Harness::new();
    let empty = harness.write_batch("empty.toml", "version = 1\ntasks = []\n");
    assert_eq!(
        code(&harness.freeze(&empty, None, None).unwrap_err()),
        "TASK_CONFIG_INVALID"
    );
    let dup = harness.write_batch(
        "dup.toml",
        r#"version = 1
agent = "codex"

[[tasks]]
id = "same"
prompt = "a"

[[tasks]]
id = "same"
prompt = "b"
"#,
    );
    assert_eq!(
        code(&harness.freeze(&dup, None, None).unwrap_err()),
        "TASK_CONFIG_INVALID"
    );
}
