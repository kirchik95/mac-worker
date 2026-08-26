# Project Inspection and Immutable Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `worker doctor` and a race-detecting immutable local snapshot pipeline that captures the current filesystem state of any supported Git worktree without copying secrets, Git metadata, or uncovered untracked inputs.

**Architecture:** A project inspector obtains worktree identity and Git metadata through literal `/usr/bin/git` argv. A separate input selector produces a normalized, NUL-safe file set; a descriptor-relative filesystem layer copies that set without following directory symlinks; and a snapshot builder verifies the source a second time before publishing a deterministic manifest. `worker doctor` composes those units, probes the configured workers for required capabilities, reports a typed readiness result, and deletes its temporary snapshot after verification.

**Tech Stack:** Rust 2024, system Git, OpenSSH through the existing transport, `serde`/`serde_json`/`toml`, SHA-256, `globset`, `humantime`, `url`, `libc`, `clap`, `tempfile`, `assert_cmd`, and `proptest`.

**Spec:** `docs/superpowers/specs/2026-08-25-mac-worker-design.md`, especially sections 3, 6, 8, 9, 10, 15, 18, 19, and 20.

## Global Constraints

- The MacBook remains the only source of truth; this phase performs no remote upload and starts no user command.
- The snapshot uses current worktree bytes, including staged and unstaged tracked changes, rather than index blob contents.
- Git filename lists use NUL delimiters; no filename is split on whitespace or newlines.
- Uncovered non-ignored untracked inputs make `doctor` not ready; they are never silently omitted.
- Ignored files are included only through an explicit precise pattern, while forbidden dependency, cache, editor, VCS, and build-output directories remain excluded.
- Sensitive paths fail closed unless an exact path is allowlisted in `.worker.toml`; allowlisting emits a warning and never records file contents in diagnostics.
- `.git` data, repository history, index data, submodules, LFS materialization, and custom Git filters are not copied.
- Symlinks are copied as symlinks and never followed. A symlink in a traversed parent component is rejected.
- A selected-path, type, mode, size, symlink-target, or content change between capture and verification returns `SNAPSHOT_CHANGED` and publishes nothing.
- Completed snapshot trees are read-only. Their canonical manifest and digest are independent of staging location, timestamps, and capture UUID.
- Every child process uses literal argv through `ProcessRunner`; no shell expression is built from project data.
- `XDG_CONFIG_HOME`, `XDG_STATE_HOME`, `XDG_CACHE_HOME`, and `XDG_DATA_HOME` retain their existing semantics.
- Human and JSON output derive from the same typed `DoctorReport`.
- Project/preflight blockers use exit `64`, snapshot integrity failures use exit `70`, unavailable workers use report data, and local I/O failures use exit `74`.

## Delivery Boundary

This plan produces a reusable verified `Snapshot` object and a user-visible `worker doctor`. It does not add `worker run`, rsync transfer, remote acceptance, job supervision, queueing, logs, cancellation, artifacts, or cleanup of remote job data. The next plan consumes the exact `Snapshot`, `SnapshotManifest`, `ProjectContext`, and requirement types defined here for single-worker execution.

## File Map

```text
Cargo.toml                         glob, duration, URL, and property-test dependencies
src/cli.rs                        public doctor syntax
src/error.rs                      stable project and snapshot error codes
src/lib.rs                        doctor dispatch and path/config composition
src/output.rs                     human/JSON doctor rendering and aggregate exit
src/transport.rs                  probe inventory with per-project requirements
src/project.rs                    Git worktree discovery, metadata, and stable identities
src/project_config.rs             strict .worker.toml parsing and snapshot policy
src/requirements.rs               bounded runtime/capability indicator detection
src/inputs.rs                     normalized relative paths and NUL-safe Git input selection
src/rooted_fs.rs                  descriptor-relative, no-follow source/destination operations
src/manifest.rs                   deterministic manifest records and canonical digest
src/snapshot.rs                   capture, second-pass verification, publication, and cleanup
src/doctor.rs                     readiness orchestration and typed report
tests/support/mod.rs              hermetic Git repository fixture
tests/project_inspection.rs       worktree discovery and identity integration tests
tests/project_config.rs           project config and runtime detection tests
tests/input_selection.rs          tracked/untracked/ignored/secret selection tests
tests/rooted_fs.rs                symlink containment and rooted copy tests
tests/snapshot_capture.rs         manifest, race, permissions, and cleanup tests
tests/doctor_command.rs           service, CLI, rendering, and exit-code tests
README.md                         phase-two usage and explicit remaining boundary
```

---

### Task 1: Project Errors, Hermetic Git Fixture, and Worktree Identity

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/error.rs`
- Modify: `src/lib.rs`
- Create: `src/project.rs`
- Create: `tests/support/mod.rs`
- Create: `tests/project_inspection.rs`

**Interfaces:**
- Consumes: `ProcessRunner::run(&ProcessRequest) -> Result<ProcessResult, WorkerError>`.
- Produces: `ProjectInspector::new(&dyn ProcessRunner)`, `ProjectInspector::inspect(&Path) -> Result<ProjectContext, WorkerError>`, `ProjectContext`, and coded `WorkerError::Project` / `WorkerError::Snapshot` variants.

- [ ] **Step 1: Add failing identity and worktree tests**

Create a hermetic repository fixture that invokes `/usr/bin/git` with literal argv, a temporary `HOME`, `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM=1`, and local author configuration. The fixture API is:

```rust
pub struct GitRepo {
    directory: tempfile::TempDir,
}

impl GitRepo {
    pub fn init() -> Self;
    pub fn root(&self) -> &std::path::Path;
    pub fn write(&self, path: &str, bytes: &[u8]);
    pub fn git(&self, args: &[&str]) -> std::process::Output;
    pub fn commit_all(&self, message: &str);
}
```

In `tests/project_inspection.rs`, assert all of the following:

```rust
#[test]
fn inspection_uses_the_worktree_root_and_current_relative_directory() {
    let repo = GitRepo::init();
    repo.write("src/lib.rs", b"pub fn value() -> u8 { 1 }\n");
    repo.commit_all("initial");
    let nested = repo.root().join("src");

    let context = ProjectInspector::new(&SystemProcessRunner)
        .inspect(&nested)
        .unwrap();

    assert_eq!(context.root, repo.root().canonicalize().unwrap());
    assert_eq!(context.relative_cwd, PathBuf::from("src"));
    assert_eq!(context.head.as_deref().map(str::len), Some(40));
    assert!(!context.project_id.is_empty());
    assert!(!context.worktree_id.is_empty());
}

#[test]
fn origin_credentials_never_change_or_appear_in_the_public_identity() {
    let repo = GitRepo::init();
    repo.git(&["remote", "add", "origin", "https://alice:secret@example.com/acme/app.git"]);
    let context = ProjectInspector::new(&SystemProcessRunner).inspect(repo.root()).unwrap();

    assert!(!context.project_id.contains("alice"));
    assert!(!context.project_id.contains("secret"));
    assert_eq!(context.project_id.len(), 64);
}
```

Also cover detached HEAD, an unborn repository, no origin fallback, dirty tracked bytes, a linked worktree sharing the same `project_id` but receiving a different `worktree_id`, a non-repository directory, malformed multi-line origin output, and a path outside the discovered root.

- [ ] **Step 2: Run the focused tests and verify red**

Run: `cargo test --test project_inspection`

Expected: compilation fails because `mac_worker::project::{ProjectContext, ProjectInspector}` and the coded error variants do not exist.

- [ ] **Step 3: Add coded project and snapshot errors**

Extend `WorkerError` without changing the existing setup/worker mappings:

```rust
#[error("project error [{code}]: {message}")]
Project { code: &'static str, message: String },
#[error("snapshot error [{code}]: {message}")]
Snapshot { code: &'static str, message: String },
```

Map `Project` to `ExitKind::Usage` and `Snapshot` to `ExitKind::Infrastructure`. Add unit cases that assert the mapping and the stable rendered code.

- [ ] **Step 4: Implement literal Git inspection and identities**

Add `url = "2"` to dependencies and expose `pub mod project;`. Define:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectContext {
    pub root: PathBuf,
    pub relative_cwd: PathBuf,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub project_id: String,
    pub worktree_id: String,
    pub dirty: bool,
}

pub struct ProjectInspector<'a> {
    runner: &'a dyn ProcessRunner,
}
```

Every Git request uses `/usr/bin/git`, `-C`, the caller-provided path as one `OsString`, fixed arguments, a 5-second deadline, and 8 MiB stdout/stderr bounds. Parse scalar Git output only when it has exactly one optional terminal LF and no embedded CR, LF, or NUL. Use `--path-format=absolute` for `--show-toplevel`, `--git-dir`, and `--git-common-dir`, then physically canonicalize each directory.

Normalize an HTTP/HTTPS/SSH URL with the `url` crate by clearing username/password and query/fragment data, lowercasing the scheme and host, and preserving the repository path. Normalize SCP syntax `[user@]host:path` by removing the user prefix and lowercasing the host. Hash `b"origin\0" + normalized_origin` for `project_id`; if origin is absent, hash `b"common-dir\0" + common_dir.as_os_str().as_bytes()`. Hash `b"worktree\0" + root.as_os_str().as_bytes()` for `worktree_id`. Expose only lowercase hexadecimal digests, never the normalized origin.

Use `git status --porcelain=v2 -z --untracked-files=normal` to set `dirty`, `git rev-parse --verify HEAD` for optional HEAD, and `git symbolic-ref --quiet --short HEAD` for optional branch. Treat Git exit `1`/`128` as optional only for the specifically optional query; every other non-zero result is `NOT_A_WORKTREE` or `GIT_INSPECTION_FAILED`.

- [ ] **Step 5: Verify the focused tests pass**

Run: `cargo test --test project_inspection && cargo test error::tests`

Expected: all worktree, credential-redaction, linked-worktree, and exit-mapping tests pass.

- [ ] **Step 6: Commit the project identity boundary**

```bash
git add Cargo.toml Cargo.lock src/error.rs src/lib.rs src/project.rs tests/support/mod.rs tests/project_inspection.rs
git commit -m "feat: inspect git worktree identity"
```

---

### Task 2: Strict Project Configuration and Requirement Detection

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Create: `src/project_config.rs`
- Create: `src/requirements.rs`
- Create: `tests/project_config.rs`

**Interfaces:**
- Consumes: `ProjectContext::root`.
- Produces: `ProjectSettings::load(&Path, &[String])`, `SnapshotSettings`, `ResourceClass`, `ArtifactSettings`, and `RequirementDetector::detect(&Path) -> Result<Vec<String>, WorkerError>`.

- [ ] **Step 1: Write failing strict-config tests**

Cover an absent file, the complete supported shape, CLI include merging, duplicate requirements, invalid duration, unknown fields at every nesting level, invalid resource class, imprecise include patterns, and non-literal sensitive allowlists.

```rust
#[test]
fn project_settings_merge_explicit_and_cli_snapshot_inputs() {
    let repo = GitRepo::init();
    repo.write(
        ".worker.toml",
        br#"version = 1
requires = ["darwin-arm64", "node"]
resource_class = "heavy"
timeout = "30m"

[snapshot]
include_untracked = ["fixtures/generated/**"]
include_empty_dirs = ["fixtures/empty"]
allow_sensitive = ["fixtures/test.env"]

[artifacts]
include = ["coverage/**"]
max_total_bytes = 536870912
"#,
    );

    let settings = ProjectSettings::load(
        repo.root(),
        &["tmp/contract.json".to_owned()],
    )
    .unwrap();

    assert_eq!(settings.timeout, Duration::from_secs(1800));
    assert_eq!(settings.snapshot.include_untracked.len(), 2);
    assert_eq!(
        settings.snapshot.include_empty_dirs,
        vec!["fixtures/empty".to_owned()]
    );
    assert_eq!(
        settings.snapshot.allow_sensitive,
        vec!["fixtures/test.env".to_owned()]
    );
}
```

- [ ] **Step 2: Write failing bounded requirement-detection tests**

Create table-driven fixtures asserting these exact mappings:

```text
package.json, package-lock.json, yarn.lock, pnpm-lock.yaml -> node
Gemfile, .ruby-version                                  -> ruby
pyproject.toml, requirements.txt, .python-version       -> python
go.mod                                                   -> go
Package.swift                                            -> swift
global.json, *.sln, *.csproj                             -> dotnet
Dockerfile, compose.yaml, compose.yml                    -> docker
playwright.config.js, playwright.config.ts               -> browser
```

Detection reads only root-level metadata names and one root `read_dir`; it does not recursively scan source code. Assert stable lexical output, de-duplication, and rejection of a symlink masquerading as a config file.

- [ ] **Step 3: Run the new tests and verify red**

Run: `cargo test --test project_config`

Expected: compilation fails because `project_config` and `requirements` do not exist.

- [ ] **Step 4: Implement the strict `.worker.toml` schema**

Add `globset = "0.4"` and `humantime = "2"`. Use `#[serde(deny_unknown_fields)]` on every raw TOML struct. Define validated public settings:

```rust
pub struct ProjectSettings {
    pub requires: Vec<String>,
    pub resource_class: ResourceClass,
    pub timeout: Duration,
    pub snapshot: SnapshotSettings,
    pub artifacts: ArtifactSettings,
}

pub struct SnapshotSettings {
    pub include_untracked: Vec<String>,
    pub include_empty_dirs: Vec<String>,
    pub allow_sensitive: Vec<String>,
}

pub enum ResourceClass { Heavy }
```

Defaults are version `1`, no explicit requirements, `heavy`, 30 minutes, empty includes/allowlist, and disabled artifacts. Require lowercase ASCII capability identifiers containing only letters, digits, `_`, and `-`. Validate include patterns as relative UTF-8 paths with a literal first component, no `..`, no leading slash, and no forbidden component. `include_empty_dirs` and sensitive allowlist entries are exact relative paths without glob metacharacters. Merge CLI includes after file includes, preserving first occurrence order.

- [ ] **Step 5: Implement bounded runtime detection**

Use `symlink_metadata` and a single root `read_dir`. Accept only regular indicator files; never follow a symlink. Produce a sorted de-duplicated vector. Merge detected capabilities with explicit `requires` later in `DoctorService`, keeping explicit order first and detected-only values lexical.

- [ ] **Step 6: Run config and requirement tests**

Run: `cargo test --test project_config`

Expected: all schema, duration, pattern, symlink, and requirement cases pass.

- [ ] **Step 7: Commit project settings and detection**

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/project_config.rs src/requirements.rs tests/project_config.rs
git commit -m "feat: load worker project settings"
```

---

### Task 3: NUL-Safe Input Selection and Preflight Blocking

**Files:**
- Modify: `src/lib.rs`
- Create: `src/inputs.rs`
- Create: `tests/input_selection.rs`

**Interfaces:**
- Consumes: `ProjectContext`, `SnapshotSettings`, and `ProcessRunner`.
- Produces: `RelativePath`, `SelectedInput`, `InputOrigin`, `InputSelection`, `SelectionWarning`, `SelectionFailure`, and `InputSelector::select`.

- [ ] **Step 1: Write failing relative-path and NUL parser tests**

Define the expected public behavior:

```rust
#[test]
fn relative_paths_preserve_spaces_unicode_and_newlines_but_reject_escape() {
    for accepted in ["src/a b.rs", "fixtures/привет.txt", "fixtures/line\nbreak.txt"] {
        assert_eq!(RelativePath::parse(accepted.as_bytes()).unwrap().as_str(), accepted);
    }
    for rejected in [b"/absolute".as_slice(), b"../escape", b"a/../../escape", b".git/config", b"bad\0name"] {
        assert!(RelativePath::parse(rejected).is_err());
    }
}
```

Test a NUL stream containing spaces, tabs, newlines, Unicode, and a missing final NUL. The missing terminator and non-UTF-8 path must fail with stable codes instead of truncating or lossily converting a name.

- [ ] **Step 2: Write failing real-Git selection tests**

Using `GitRepo`, cover:

- modified/staged/unstaged tracked files selected from current filesystem bytes;
- tracked deletion recorded separately;
- uncovered non-ignored untracked file returns `UNTRACKED_INPUT` with its exact escaped display path;
- matching `include_untracked` admits a non-ignored file;
- a precise pattern admits an ignored fixture through `git ls-files --others --ignored --exclude-standard -z -- :(glob)<pattern>`;
- an exact `include_empty_dirs` path admits an existing empty directory, while a missing, symlinked, or non-empty declaration is rejected;
- `.git`, `node_modules`, `.pnpm-store`, `.yarn/cache`, `vendor/bundle`, `.venv`, `__pycache__`, `.cache`, `target`, `dist`, `.idea`, and `.vscode` are rejected for untracked/ignored inclusion;
- index mode `160000` returns `UNSUPPORTED_SUBMODULE`;
- `.gitattributes` containing `filter=lfs` returns `UNSUPPORTED_LFS`;
- configured `filter.*` attributes return `UNSUPPORTED_FILTER`;
- a tracked `.env` returns `SENSITIVE_PATH` unless its exact path is allowlisted, in which case selection includes one warning;
- `.env.example` and `.env.sample` are not sensitive by convention;
- selected entries are sorted by raw UTF-8 bytes for deterministic manifests.

- [ ] **Step 3: Run selection tests and verify red**

Run: `cargo test --test input_selection`

Expected: compilation fails because `mac_worker::inputs` does not exist.

- [ ] **Step 4: Implement validated relative paths and fixed policies**

`RelativePath` stores validated UTF-8 bytes in a `String`, rejects empty and `.`/`..` components, absolute paths, NUL, `.git`, and platform separators other than `/`, and exposes `as_str`, `as_path`, and escaped diagnostic display. It must not call `to_string_lossy` on Git filename bytes.

Define the sensitive matcher over exact path components and basenames. It covers `.env`, `.env.*` except `.env.example`/`.env.sample`, `.npmrc`, `.pypirc`, `.netrc`, `id_rsa`, `id_ed25519`, `credentials`, `credentials.json`, and directories `.ssh`, `.aws`, `.config/gcloud`, and `.kube`. Allowlisting compares the complete normalized relative path.

- [ ] **Step 5: Implement Git index, untracked, ignored, and unsupported-feature queries**

Define:

```rust
pub struct InputSelection {
    pub entries: Vec<SelectedInput>,
    pub tracked_deletions: Vec<RelativePath>,
    pub warnings: Vec<SelectionWarning>,
}

pub struct SelectedInput {
    pub path: RelativePath,
    pub origin: InputOrigin,
    pub kind: SelectedInputKind,
}

pub enum InputOrigin { Tracked, IncludedUntracked, IncludedIgnored }

pub enum SelectedInputKind { FilesystemEntry, EmptyDirectory }

pub struct SelectionWarning {
    pub code: &'static str,
    pub message: String,
    pub path: RelativePath,
}

pub struct SelectionFailure {
    pub code: &'static str,
    pub message: String,
    pub paths: Vec<RelativePath>,
    pub total_path_count: usize,
}
```

The selector interface is exact:

```rust
pub struct InputSelector<'a> {
    runner: &'a dyn ProcessRunner,
}

impl<'a> InputSelector<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self;
    pub fn select(
        &self,
        context: &ProjectContext,
        settings: &SnapshotSettings,
    ) -> Result<InputSelection, SelectionFailure>;
}
```

Use `git ls-files --stage -z` to parse index mode and path, `git ls-files --others --exclude-standard -z` for non-ignored untracked files, and only when include patterns exist, one bounded ignored query with fixed `:(glob)` pathspec arguments after `--`. Cap each Git response at 64 MiB and 250,000 entries; crossing either bound returns `INPUT_SET_TOO_LARGE`.

Every include pattern must compile through `globset`. An uncovered non-ignored untracked list is a single `SelectionFailure` containing total count and at most the first 100 sorted paths. Ignored files not matching an explicit pattern remain absent without becoming blockers. Validate every declared empty directory through no-follow metadata and a bounded `read_dir`; it must exist, be empty, and remain inside the worktree. Query `.gitattributes`, index modes, and `git config --get-regexp ^filter\.` without invoking smudge/clean filters.

- [ ] **Step 6: Run the selection suite**

Run: `cargo test --test input_selection`

Expected: all NUL, path, untracked, ignored, sensitive, and unsupported-Git cases pass.

- [ ] **Step 7: Commit the selection contract**

```bash
git add src/lib.rs src/inputs.rs tests/input_selection.rs
git commit -m "feat: select safe snapshot inputs"
```

---

### Task 4: Descriptor-Relative No-Follow Filesystem Layer

**Files:**
- Modify: `src/lib.rs`
- Create: `src/rooted_fs.rs`
- Create: `tests/rooted_fs.rs`

**Interfaces:**
- Consumes: `RelativePath`.
- Produces: `RootedDir::open`, `RootedDir::create`, `RootedDir::inspect`, `RootedDir::copy_regular_to`, `RootedDir::create_empty_directory`, `RootedDir::read_symlink`, `RootedDir::create_symlink`, `RootedDir::make_read_only`, and `RootedDir::remove_owned_tree`.

- [ ] **Step 1: Write failing containment and copy tests**

Cover regular binary bytes, executable-mode normalization, empty files, explicit empty directories, symlink target preservation, a symlink as the final selected entry, a symlink in every parent position, a parent swapped to a symlink after the root is opened, FIFO/socket/device rejection, an existing destination entry, and cleanup with a planted symlink pointing outside.

```rust
#[test]
fn parent_symlink_never_reads_or_writes_its_target() {
    let fixture = RootFixture::new();
    fixture.symlink_source_parent("src", fixture.outside());
    let source = RootedDir::open(fixture.source()).unwrap();
    let destination = RootedDir::create(fixture.destination()).unwrap();

    let error = source
        .copy_regular_to(&RelativePath::parse(b"src/secret.txt").unwrap(), &destination)
        .unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::ELOOP));
    assert!(!fixture.outside().join("copied.txt").exists());
}
```

- [ ] **Step 2: Run the rooted filesystem tests and verify red**

Run: `cargo test --test rooted_fs`

Expected: compilation fails because `RootedDir` does not exist.

- [ ] **Step 3: Implement Unix descriptor-relative traversal**

Open the physical root once with `O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW`. Traverse each parent component using `openat` with the same flags. Create destination directories with `mkdirat(0700)` followed by a no-follow `openat`; treat `EEXIST` as valid only after opening the existing entry as a directory. Convert validated UTF-8 components to `CString` and reject interior NUL before every libc call.

Inspect final entries with `fstatat(..., AT_SYMLINK_NOFOLLOW)`. Regular files open with `openat(O_RDONLY | O_CLOEXEC | O_NOFOLLOW)`. Symlink targets use `readlinkat` and remain byte-exact UTF-8; non-UTF-8 targets return `UNSUPPORTED_PATH_ENCODING`. Reject all types other than regular file and symlink.

- [ ] **Step 4: Implement clone-first regular copying with byte fallback**

On macOS, call `clonefileat(source_parent_fd, source_name, destination_parent_fd, destination_name, 0)`. On `ENOTSUP`, `EXDEV`, or `EINVAL`, prove any destination created by the failed call is a regular file below the destination root, unlink that exact entry, and retry through descriptor-backed `File` handles using `std::io::copy`. Other errors are returned unchanged. On non-macOS test targets, use the byte path directly.

After copy, apply mode `0555` when any source executable bit is set and `0444` otherwise. Destination creation uses `O_CREAT | O_EXCL | O_WRONLY | O_NOFOLLOW`; it never truncates an existing path.

- [ ] **Step 5: Implement safe publication permissions and cleanup**

Walk only directory FDs opened from the owned destination root. Make child directories `0555` after their contents are complete and the tree root `0555` last. `remove_owned_tree` first validates and opens each owned directory with no-follow flags, changes that directory to `0700` through its descriptor, enumerates with `fdopendir/readdir`, unlinks regular files and symlinks with `unlinkat`, recursively opens child directories with no-follow flags, and removes them with `AT_REMOVEDIR`. It must never call `canonicalize` after mutation begins and never follow a symlink.

- [ ] **Step 6: Run the containment suite**

Run: `cargo test --test rooted_fs`

Expected: all copy, clone fallback, mode, symlink, special-file, swap, and cleanup tests pass without changing the outside sentinel.

- [ ] **Step 7: Commit the rooted filesystem layer**

```bash
git add src/lib.rs src/rooted_fs.rs tests/rooted_fs.rs
git commit -m "feat: add rooted snapshot filesystem"
```

---

### Task 5: Deterministic Manifest, Double Verification, and Atomic Snapshot Publication

**Files:**
- Modify: `src/lib.rs`
- Create: `src/manifest.rs`
- Create: `src/snapshot.rs`
- Create: `tests/snapshot_capture.rs`

**Interfaces:**
- Consumes: `ProjectContext`, `InputSelection`, `InputSelector`, `RootedDir`, and a cache root.
- Produces: `SnapshotBuilder::capture`, `Snapshot`, `SnapshotManifest`, `ManifestEntry`, `ManifestEntryKind`, and `SnapshotSummary`.

- [ ] **Step 1: Write failing deterministic-manifest tests**

Assert that two unchanged captures have identical canonical manifest bytes and digest despite different capture UUIDs, and that changing bytes, executable mode, symlink target, declared empty-directory set, deletion set, relative working directory, HEAD, or project/worktree identity changes the digest.

The stable wire shape is:

```rust
#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub version: u32,
    pub project_id: String,
    pub worktree_id: String,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
    pub relative_working_dir: String,
    pub entries: Vec<ManifestEntry>,
    pub tracked_deletions: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    pub path: String,
    pub kind: ManifestEntryKind,
    pub mode: u32,
    pub size: u64,
    pub sha256: String,
    pub symlink_target: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestEntryKind { File, Symlink, Directory }
```

Use compact `serde_json::to_vec`, fixed struct field order, byte-sorted entries, no timestamps, and SHA-256 of the exact canonical bytes.

- [ ] **Step 2: Write failing source-change and publication tests**

Inject a crate-private `SnapshotHook::after_materialization()` and mutate one condition per test: file bytes, mode, type, symlink target, tracked deletion, newly uncovered untracked path, removed path, and included-path set. Every case must return code `SNAPSHOT_CHANGED`, leave no `ready/<capture_id>`, and remove only its own `.partial-<capture_id>` tree.

Also assert:

- binary content and Unicode/newline filenames survive exactly;
- the tree contains no `.git` entry and the manifest is a sidecar, not project input;
- the published tree and regular files are read-only;
- `Snapshot::cleanup()` removes only that snapshot and tolerates read-only modes;
- a pre-existing publication path is rejected, not reused or overwritten;
- a failed cleanup is surfaced as I/O and not reported as successful capture.

- [ ] **Step 3: Run snapshot tests and verify red**

Run: `cargo test --test snapshot_capture`

Expected: compilation fails because manifest and snapshot types do not exist.

- [ ] **Step 4: Implement entry fingerprinting and canonical manifests**

For a regular file, hash bytes read through the no-follow descriptor, use normalized mode `0o755` or `0o644` in the manifest, and record byte size. For a symlink, hash the target bytes with domain prefix `b"symlink\0"`, use mode `0o777`, record target-byte size, and include the target string. For an explicitly declared empty directory, use kind `directory`, mode `0o755`, size `0`, SHA-256 of `b"directory\0"`, and no symlink target. Re-stat a regular file before and after reading and reject an inode, device, type, size, mode, or modification-time change within one read.

The published tree permissions may be `0555`/`0444`; the manifest records source semantics `0755`/`0644`. `SnapshotManifest::canonical_bytes()` and `digest()` are the only digest implementation.

- [ ] **Step 5: Implement capture and second-pass verification**

Create cache directories one component at a time below `PathLayout.cache`, rejecting symlinks. Use:

```text
snapshots/staging/.partial-<capture_id>/tree/
snapshots/staging/.partial-<capture_id>/manifest.json
snapshots/ready/<capture_id>/tree/
snapshots/ready/<capture_id>/manifest.json
```

Capture the first selection, copy each entry, and build first-pass fingerprints. Invoke the hook. Re-run `InputSelector::select`, compare selected paths/origins/deletions/warnings exactly, and re-fingerprint every source entry. Any difference triggers owned cleanup and `WorkerError::Snapshot { code: "SNAPSHOT_CHANGED", ... }`.

Write canonical manifest bytes to a newly created sidecar, `fsync` the file and containing partial directory, make the tree read-only, atomically rename the exact partial directory into `ready/<capture_id>`, and `fsync` the ready parent. Return:

```rust
pub struct Snapshot {
    pub capture_id: uuid::Uuid,
    pub root: PathBuf,
    pub manifest_path: PathBuf,
    pub manifest: SnapshotManifest,
    pub digest: String,
    pub file_count: usize,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotSummary {
    pub digest: String,
    pub file_count: usize,
    pub total_bytes: u64,
    pub tracked_deletion_count: usize,
    pub included_untracked_count: usize,
    pub warning_count: usize,
}
```

The builder interface is exact:

```rust
pub trait SnapshotHook: Send + Sync {
    fn after_materialization(&self, tree: &Path) -> std::io::Result<()>;
}

pub struct SnapshotBuilder<'a> {
    selector: InputSelector<'a>,
    cache_root: &'a Path,
    hook: &'a dyn SnapshotHook,
}

impl<'a> SnapshotBuilder<'a> {
    pub fn new(runner: &'a dyn ProcessRunner, cache_root: &'a Path) -> Self;
    #[doc(hidden)]
    pub fn with_hook(
        runner: &'a dyn ProcessRunner,
        cache_root: &'a Path,
        hook: &'a dyn SnapshotHook,
    ) -> Self;
    pub fn capture(
        &self,
        context: &ProjectContext,
        settings: &SnapshotSettings,
        initial: InputSelection,
    ) -> Result<Snapshot, WorkerError>;
}
```

- [ ] **Step 6: Run snapshot and regression suites**

Run: `cargo test --test snapshot_capture && cargo test --test input_selection && cargo test --test rooted_fs`

Expected: deterministic, mutation, containment, cleanup, and selection cases all pass.

- [ ] **Step 7: Commit immutable local snapshots**

```bash
git add src/lib.rs src/manifest.rs src/snapshot.rs tests/snapshot_capture.rs
git commit -m "feat: build verified immutable snapshots"
```

---

### Task 6: Project-Aware Worker Eligibility and Doctor Service

**Files:**
- Modify: `src/transport.rs`
- Modify: `src/protocol.rs`
- Modify: `src/lib.rs`
- Create: `src/doctor.rs`
- Modify: `tests/workers_command.rs`
- Create: `tests/doctor_command.rs`

**Interfaces:**
- Consumes: `ProjectInspector`, `ProjectSettings`, `RequirementDetector`, `InputSelector`, `SnapshotBuilder`, `WorkersService`, `Config`, and `PathLayout`.
- Produces: `WorkersService::inspect_with_requirements`, `DoctorService::inspect`, `DoctorRequest`, `DoctorReport`, `DoctorProject`, `SnapshotSummary`, `DoctorIssue`, and `IssueSeverity`.

- [ ] **Step 1: Write failing required-capability probe tests**

Add a transport test where inventory declares only `darwin-arm64`, project requirements add `node` and `docker`, and a valid probe reports only `darwin-arm64` and `node`. Assert the worker becomes unavailable with exactly `missing_capabilities == ["docker"]`, while the existing `inspect(&Config)` behavior remains unchanged.

The interface is:

```rust
pub fn inspect_with_requirements(
    &self,
    config: &Config,
    requirements: &[String],
) -> WorkersReport;
```

It de-duplicates inventory and project requirements while preserving inventory order first.

- [ ] **Step 2: Write failing doctor orchestration tests**

Define the report shape and test ready, blocked, and partial-health cases:

```rust
#[derive(Serialize)]
pub struct DoctorReport {
    pub version: u32,
    pub ready: bool,
    pub project: DoctorProject,
    pub requirements: Vec<String>,
    pub snapshot: Option<SnapshotSummary>,
    pub workers: Vec<WorkerHealth>,
    pub issues: Vec<DoctorIssue>,
}

#[derive(Serialize)]
pub struct DoctorProject {
    pub display_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
    pub relative_working_dir: String,
}

#[derive(Serialize)]
pub struct DoctorIssue {
    pub severity: IssueSeverity,
    pub code: String,
    pub message: String,
    pub paths: Vec<String>,
}

#[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum IssueSeverity { Blocker, Warning }
```

Assertions:

- a clean project, successful snapshot, and one eligible ready worker yield `ready: true`;
- uncovered untracked input yields `ready: false`, code `UNTRACKED_INPUT`, no snapshot, and still reports worker health;
- an allowed sensitive path yields `ready: true` plus a warning with no content;
- submodule/LFS/custom filter blockers yield no snapshot;
- zero eligible ready workers yields blocker `NO_ELIGIBLE_WORKER` while preserving each worker diagnostic;
- one offline worker is a warning when another worker is eligible;
- `SNAPSHOT_CHANGED` is a blocker with no published snapshot;
- a successful doctor removes its temporary snapshot after copying only summary values into the report.

- [ ] **Step 3: Run doctor and transport tests and verify red**

Run: `cargo test --test workers_command inspect_with_requirements && cargo test --test doctor_command`

Expected: new interfaces and types are missing.

- [ ] **Step 4: Implement project-aware capability matching**

Refactor `SshTransport::probe_with_failure_kind` to accept a required-capability slice distinct from `WorkerEntry`. Existing `probe` passes inventory capabilities. `WorkersService::inspect_with_requirements` passes the stable union. Do not mutate the loaded `Config` or report requirements as if they were inventory declarations.

- [ ] **Step 5: Implement DoctorService with dependency injection**

Define:

```rust
pub struct DoctorRequest {
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
}

pub struct DoctorService<'a> {
    pub runner: &'a dyn ProcessRunner,
    pub config: &'a Config,
    pub paths: &'a PathLayout,
}

impl DoctorService<'_> {
    pub fn inspect(&self, request: DoctorRequest) -> Result<DoctorReport, WorkerError>;
}
```

The service inspects project/config/features, selects inputs, captures and summarizes a temporary snapshot, probes worker eligibility, and computes issues. It runs independent local inspection and worker probes even when one side has a blocker so the report remains diagnostic. It must not attempt capture after input/feature blockers. After successful capture it obtains `SnapshotSummary`, calls exact owned cleanup, and treats cleanup failure as `WorkerError::Io`.

Set `ready` only when there are no blocker issues, a verified snapshot summary exists, and at least one worker is ready with no missing capabilities. Sort issues by severity, stable code, then first path.

- [ ] **Step 6: Run doctor and worker tests**

Run: `cargo test --test doctor_command && cargo test --test workers_command`

Expected: orchestration and existing worker behavior both pass.

- [ ] **Step 7: Commit doctor domain services**

```bash
git add src/transport.rs src/protocol.rs src/lib.rs src/doctor.rs tests/workers_command.rs tests/doctor_command.rs
git commit -m "feat: diagnose project worker readiness"
```

---

### Task 7: Public `worker doctor`, Typed Output, and Exit Semantics

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/lib.rs`
- Modify: `src/output.rs`
- Modify: `tests/cli_help.rs`
- Modify: `tests/doctor_command.rs`

**Interfaces:**
- Consumes: `DoctorService::inspect(DoctorRequest) -> Result<DoctorReport, WorkerError>`.
- Produces: `worker doctor [--project PATH] [--include PATTERN]`, `CommandOutput::Doctor`, human output, JSON output, and aggregate readiness exit.

- [ ] **Step 1: Write failing CLI syntax and rendering tests**

Assert help exposes `doctor` but still hides `host`. Parse these exact forms:

```text
worker doctor
worker doctor --project /path/to/worktree
worker doctor --include fixtures/generated/** --include tmp/contract.json
worker --json doctor --project /path/to/worktree
```

Reject an empty `--include` and unexpected positional commands with exit `64`.

Build one ready and one blocked `DoctorReport`. Assert JSON is one line with `kind: "doctor"`, exact issue codes, no origin URL, no secret bytes, and no full local root path in the default project summary. Human output must include project/worktree short IDs, branch/HEAD state, dirty state, requirements, snapshot digest/count/bytes, eligible workers, and every warning/blocker.

- [ ] **Step 2: Run CLI/output tests and verify red**

Run: `cargo test --test cli_help && cargo test --test doctor_command doctor_output`

Expected: `doctor` is not a command and `CommandOutput::Doctor` is missing.

- [ ] **Step 3: Add the command and dispatch**

Extend `Command`:

```rust
Doctor {
    #[arg(long)]
    project: Option<PathBuf>,
    #[arg(long = "include", value_parser = non_empty_pattern)]
    includes: Vec<String>,
},
```

Resolve an omitted project from `std::env::current_dir()`. Discover `PathLayout` once, load the global inventory, construct `DoctorService`, and return `CommandOutput::Doctor`. The hidden host probe remains the only command that bypasses inventory loading.

- [ ] **Step 4: Render one typed report in both modes**

Add `Doctor` to the tagged output enum. Human rendering must not reconstruct readiness or query state. Render `ready` as `ready` or `blocked`, show bounded issue paths already present in the report, and never print an origin or environment value.

`aggregate_exit_kind` returns `ExitKind::Usage` when a doctor report is not ready and `None` when ready. JSON/human reports still go to stdout; unexpected `WorkerError` remains stderr-only with its mapped exit.

- [ ] **Step 5: Add executable integration coverage**

Use `run_with_io` with a recording runner and explicit temporary `--project`/`--config` paths to avoid global cwd mutation. Test exit `0` for ready and `64` for a typed blocked report. Assert stdout is complete and stderr empty in both cases, and preserve the existing broken-writer behavior.

- [ ] **Step 6: Run all command tests**

Run: `cargo test --test cli_help && cargo test --test doctor_command && cargo test --test setup_command && cargo test --test workers_command`

Expected: all public command, output, setup, and worker regressions pass.

- [ ] **Step 7: Commit the public doctor command**

```bash
git add src/cli.rs src/lib.rs src/output.rs tests/cli_help.rs tests/doctor_command.rs
git commit -m "feat: add worker doctor command"
```

---

### Task 8: Adversarial Coverage, Documentation, and Phase-Two Acceptance

**Files:**
- Modify: `Cargo.toml`
- Modify: `tests/input_selection.rs`
- Modify: `tests/snapshot_capture.rs`
- Modify: `tests/doctor_command.rs`
- Modify: `README.md`
- Create: `docs/phase-two-validation.md`

**Interfaces:**
- Consumes: all phase-two public interfaces.
- Produces: property/security regression coverage, operator guidance, and recorded local/live acceptance evidence.

- [ ] **Step 1: Add property tests for normalized paths and canonical manifests**

Add `proptest = "1"` to dev dependencies. Generate valid UTF-8 relative components including spaces, tabs, newlines, and non-ASCII characters; assert parse/serialize/parse stability. Generate invalid absolute/traversal/reserved paths; assert rejection. Shuffle identical manifest entries and assert constructor sorting yields the same canonical bytes and digest.

Limit generated cases to 256 per property test and individual path length to 1,024 bytes so the suite remains bounded.

- [ ] **Step 2: Add source-mutation and secret-leak regression matrices**

Run 100 deterministic hook-driven captures that mutate a different selected file after materialization; assert zero successful publications. Add planted secret values to tracked `.env`, `.npmrc`, `.pypirc`, `.ssh/id_ed25519`, and `.aws/credentials`. Assert none of those byte strings appears in human output, JSON output, error text, manifest bytes, or any published tree after the blocked doctor run.

- [ ] **Step 3: Add unusual-file and cross-capture isolation tests**

Cover zero-byte and binary files, 255-byte UTF-8 names where supported, newline names, executable scripts, symlinks to relative and absolute targets, simultaneous captures from two linked worktrees, and cleanup of one snapshot while another remains byte-identical and readable. Skip only a filesystem-specific name-length case when the OS returns `ENAMETOOLONG`; do not skip containment or secret tests.

- [ ] **Step 4: Update README and write the validation checklist**

Document:

```bash
cargo build --release
./target/release/worker doctor --project /path/to/worktree
./target/release/worker --json doctor --project /path/to/worktree | jq .
./target/release/worker doctor --project /path/to/worktree \
  --include 'fixtures/generated/**'
```

State that doctor builds and deletes a local verified snapshot, performs no remote upload, and starts no user command. Explain uncovered untracked and sensitive-path remediation without recommending broad glob includes. Keep `worker run`, queues, remote logs, and artifacts explicitly marked as the next phases.

In `docs/phase-two-validation.md`, record commands and results for one Node-family repository and one non-Node repository, including dirty tracked bytes, one deliberate untracked blocker, explicit inclusion, snapshot digest stability, eligible workers, and cleanup verification. Do not record source contents, origin URLs, credentials, or complete local paths.

- [ ] **Step 5: Run the complete local quality gate**

Run:

```bash
cargo fmt --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo build --release
```

Expected: every command exits `0`; all unit, integration, property, containment, mutation, and regression tests pass.

- [ ] **Step 6: Perform read-only live acceptance against the configured workers**

Run `worker doctor` from two representative local worktrees. Expected:

- project/worktree IDs remain stable across repeated runs;
- unchanged runs produce the same manifest digest;
- dirty tracked bytes change the digest;
- uncovered untracked files block until precisely included or removed;
- at least one compatible worker is reported ready;
- no `incoming/`, `jobs/`, `snapshots/`, or lease state is created remotely by doctor;
- local `snapshots/staging` and `snapshots/ready` contain no doctor-owned capture after command completion.

If any repository uses submodules, LFS, or custom filters, doctor must block it with the corresponding stable code rather than weakening the snapshot contract.

- [ ] **Step 7: Commit phase-two documentation and hardening**

```bash
git add Cargo.toml Cargo.lock tests/input_selection.rs tests/snapshot_capture.rs tests/doctor_command.rs README.md docs/phase-two-validation.md
git commit -m "test: validate immutable project snapshots"
```

## Plan Self-Review Results

- **Spec coverage:** Tasks 1–3 cover worktree identity, current tracked bytes, untracked policy, sensitive paths, unsupported Git features, requirements, and configuration. Tasks 4–5 cover no-follow filesystem access, clone/copy behavior, deterministic manifests, second-pass race detection, read-only publication, and exact cleanup. Tasks 6–7 cover worker compatibility, `doctor`, typed output, and exit semantics. Task 8 covers property, mutation, secret, representative-project, cleanup, and quality gates.
- **Intentional deferrals:** Remote rsync, host-side manifest verification, APFS job workspaces, durable job lifecycle, scheduling, logs, cancellation, artifacts, Docker profiles, and GC remain in plans 3–5 as required by the approved decomposition.
- **Placeholder scan:** Every task names exact files, interfaces, test commands, expected failures, implementation constraints, and commit boundaries; no deferred implementation marker is used.
- **Type consistency:** `ProjectContext`, `ProjectSettings`, `SnapshotSettings`, `RelativePath`, `InputSelection`, `SelectionFailure`, `RootedDir`, `SnapshotManifest`, `Snapshot`, `SnapshotSummary`, `DoctorRequest`, `DoctorProject`, `DoctorIssue`, and `DoctorReport` are introduced once and consumed under the same names and signatures in later tasks.
