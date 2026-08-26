use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs, io,
    path::Path,
    time::Duration,
};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::{
    error::{ProcessError, WorkerError},
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    project::ProjectContext,
    project_config::SnapshotSettings,
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_OUTPUT_LIMIT: usize = 64 * 1024 * 1024;
const GIT_ENTRY_LIMIT: usize = 250_000;
const REPORTED_PATH_LIMIT: usize = 100;
const GIT_DEADLINE: Duration = Duration::from_secs(5);
const GIT_ENVIRONMENT_REMOVALS: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
    "GIT_DISCOVERY_ACROSS_FILESYSTEM",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
    "GIT_ATTR_SOURCE",
    "GIT_CONFIG",
    "GIT_LITERAL_PATHSPECS",
    "GIT_GLOB_PATHSPECS",
    "GIT_NOGLOB_PATHSPECS",
    "GIT_ICASE_PATHSPECS",
];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelativePath(String);

impl RelativePath {
    pub fn parse(bytes: &[u8]) -> Result<Self, SelectionFailure> {
        let value = std::str::from_utf8(bytes).map_err(|_| {
            failure(
                "UNSUPPORTED_PATH_ENCODING",
                "Git returned a non-UTF-8 path",
                Vec::new(),
                0,
            )
        })?;
        if value.is_empty()
            || value.starts_with('/')
            || value.contains('\0')
            || value.contains('\\')
            || value
                .split('/')
                .any(|component| component.is_empty() || matches!(component, "." | ".."))
            || value.split('/').any(|component| component == ".git")
        {
            return Err(failure(
                "INVALID_PATH",
                "selected input path is not a safe relative path",
                Vec::new(),
                0,
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }

    pub fn escaped_display(&self) -> String {
        escape_diagnostic(&self.0)
    }
}

impl std::fmt::Display for RelativePath {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.escaped_display())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputSelection {
    pub entries: Vec<SelectedInput>,
    pub tracked_deletions: Vec<RelativePath>,
    pub warnings: Vec<SelectionWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedInput {
    pub path: RelativePath,
    pub origin: InputOrigin,
    pub kind: SelectedInputKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputOrigin {
    Tracked,
    IncludedUntracked,
    IncludedIgnored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedInputKind {
    FilesystemEntry,
    EmptyDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionWarning {
    pub code: &'static str,
    pub message: String,
    pub path: RelativePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionFailure {
    pub code: &'static str,
    pub message: String,
    pub paths: Vec<RelativePath>,
    pub total_path_count: usize,
}

impl std::fmt::Display for SelectionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "input selection error [{}]: {}",
            self.code, self.message
        )
    }
}

impl std::error::Error for SelectionFailure {}

pub struct InputSelector<'a> {
    runner: &'a dyn ProcessRunner,
}

impl<'a> InputSelector<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self { runner }
    }

    pub fn select(
        &self,
        context: &ProjectContext,
        settings: &SnapshotSettings,
    ) -> Result<InputSelection, SelectionFailure> {
        let (patterns, matcher) = compile_patterns(&settings.include_untracked)?;
        let allow_sensitive = parse_exact_paths(&settings.allow_sensitive)?;
        let empty_directories = parse_exact_paths(&settings.include_empty_dirs)?;

        let index = self.required_git(context, &["ls-files", "--stage", "-z"], None)?;
        let index_entries = parse_index(&index.stdout)?;
        let mut selected = BTreeMap::new();
        let mut tracked_deletions = Vec::new();
        for (mode, path) in index_entries {
            if mode == "160000" {
                return Err(failure(
                    "UNSUPPORTED_SUBMODULE",
                    format!("submodule input {} is not supported", path),
                    vec![path],
                    1,
                ));
            }
            match fs::symlink_metadata(context.root.join(path.as_path())) {
                Ok(_) => {
                    selected.entry(path.clone()).or_insert(SelectedInput {
                        path,
                        origin: InputOrigin::Tracked,
                        kind: SelectedInputKind::FilesystemEntry,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    tracked_deletions.push(path);
                }
                Err(error) => {
                    return Err(failure(
                        "INPUT_INSPECTION_FAILED",
                        format!("could not inspect tracked input {}: {error}", path),
                        vec![path],
                        1,
                    ));
                }
            }
        }

        self.reject_configured_filters(context)?;

        for path in empty_directories {
            reject_forbidden(&path)?;
            validate_empty_directory(&context.root, &path)?;
            selected.entry(path.clone()).or_insert(SelectedInput {
                path,
                origin: InputOrigin::IncludedUntracked,
                kind: SelectedInputKind::EmptyDirectory,
            });
        }

        let untracked = self.required_git(
            context,
            &["ls-files", "--others", "--exclude-standard", "-z"],
            None,
        )?;
        let untracked = parse_path_list(&untracked.stdout)?;
        let mut uncovered = Vec::new();
        for path in untracked {
            if matcher.is_match(path.as_path()) {
                reject_forbidden(&path)?;
                selected.entry(path.clone()).or_insert(SelectedInput {
                    path,
                    origin: InputOrigin::IncludedUntracked,
                    kind: SelectedInputKind::FilesystemEntry,
                });
            } else {
                uncovered.push(path);
            }
        }
        if !uncovered.is_empty() {
            uncovered.sort();
            return Err(path_failure(
                "UNTRACKED_INPUT",
                "untracked inputs are not explicitly included",
                uncovered,
            ));
        }

        if !patterns.is_empty() {
            let mut args = vec![
                OsString::from("ls-files"),
                OsString::from("--others"),
                OsString::from("--ignored"),
                OsString::from("--exclude-standard"),
                OsString::from("-z"),
                OsString::from("--"),
            ];
            args.extend(
                patterns
                    .iter()
                    .map(|pattern| OsString::from(format!(":(glob){pattern}"))),
            );
            let ignored = self.required_git_os(context, args, None)?;
            for path in parse_path_list(&ignored.stdout)? {
                if matcher.is_match(path.as_path()) {
                    reject_forbidden(&path)?;
                    selected.entry(path.clone()).or_insert(SelectedInput {
                        path,
                        origin: InputOrigin::IncludedIgnored,
                        kind: SelectedInputKind::FilesystemEntry,
                    });
                }
            }
        }

        self.reject_attributes(
            context,
            selected
                .values()
                .filter(|entry| entry.kind == SelectedInputKind::FilesystemEntry)
                .map(|entry| &entry.path),
        )?;

        let mut warnings = Vec::new();
        let mut blocked_sensitive = Vec::new();
        for entry in selected.values() {
            if is_sensitive(&entry.path) {
                if allow_sensitive.contains(&entry.path) {
                    warnings.push(SelectionWarning {
                        code: "SENSITIVE_PATH_ALLOWED",
                        message: format!(
                            "sensitive input {} was explicitly allowlisted",
                            entry.path
                        ),
                        path: entry.path.clone(),
                    });
                } else {
                    blocked_sensitive.push(entry.path.clone());
                }
            }
        }
        if !blocked_sensitive.is_empty() {
            return Err(path_failure(
                "SENSITIVE_PATH",
                "sensitive inputs require an exact allowlist entry",
                blocked_sensitive,
            ));
        }

        tracked_deletions.sort();
        tracked_deletions.dedup();
        warnings.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(InputSelection {
            entries: selected.into_values().collect(),
            tracked_deletions,
            warnings,
        })
    }

    fn reject_configured_filters(&self, context: &ProjectContext) -> Result<(), SelectionFailure> {
        let result = self.run_git(
            context,
            ["config", "--get-regexp", r"^filter\."].map(OsString::from),
            None,
        )?;
        if result.status.success() {
            if !result.stdout.is_empty() {
                return Err(failure(
                    "UNSUPPORTED_FILTER",
                    "configured Git filters are not supported for snapshot inputs",
                    Vec::new(),
                    0,
                ));
            }
            return Ok(());
        }
        if result.status.code() == Some(1) {
            return Ok(());
        }
        Err(git_failure("Git filter configuration query failed"))
    }

    fn reject_attributes<'b>(
        &self,
        context: &ProjectContext,
        paths: impl Iterator<Item = &'b RelativePath>,
    ) -> Result<(), SelectionFailure> {
        let expected = paths.collect::<Vec<_>>();
        if expected.len() > GIT_ENTRY_LIMIT {
            return Err(input_set_too_large());
        }
        let mut stdin = Vec::new();
        for path in &expected {
            stdin.extend_from_slice(path.as_str().as_bytes());
            stdin.push(0);
        }
        if stdin.is_empty() {
            return Ok(());
        }
        let output = self.required_git(
            context,
            &["check-attr", "-z", "--stdin", "filter"],
            Some(stdin),
        )?;
        let mut records = nul_records(&output.stdout)?;
        for expected_path in expected {
            let path_bytes = records
                .next()
                .ok_or_else(|| invalid_git_output("Git omitted requested attribute output"))?;
            let attribute = records
                .next()
                .ok_or_else(|| invalid_git_output("Git returned malformed attribute output"))?;
            let value_bytes = records
                .next()
                .ok_or_else(|| invalid_git_output("Git returned malformed attribute output"))?;
            reject_empty_record(path_bytes)?;
            reject_empty_record(attribute)?;
            reject_empty_record(value_bytes)?;
            if path_bytes != expected_path.as_str().as_bytes() {
                return Err(invalid_git_output(
                    "Git returned attributes for an unexpected or reordered path",
                ));
            }
            if attribute != b"filter" {
                return Err(invalid_git_output(
                    "Git returned an unexpected attribute name",
                ));
            }
            let value = std::str::from_utf8(value_bytes)
                .map_err(|_| invalid_git_output("Git returned a non-UTF-8 attribute value"))?;
            if value == "lfs" {
                return Err(failure(
                    "UNSUPPORTED_LFS",
                    format!("Git LFS input {} is not supported", expected_path),
                    vec![expected_path.clone()],
                    1,
                ));
            }
            if !matches!(value, "unspecified" | "unset") {
                return Err(failure(
                    "UNSUPPORTED_FILTER",
                    format!("filtered Git input {} is not supported", expected_path),
                    vec![expected_path.clone()],
                    1,
                ));
            }
        }
        if records.next().is_some() {
            return Err(invalid_git_output(
                "Git returned excess or duplicate attribute output",
            ));
        }
        Ok(())
    }

    fn required_git(
        &self,
        context: &ProjectContext,
        args: &[&str],
        stdin: Option<Vec<u8>>,
    ) -> Result<ProcessResult, SelectionFailure> {
        self.required_git_os(context, args.iter().map(OsString::from).collect(), stdin)
    }

    fn required_git_os(
        &self,
        context: &ProjectContext,
        args: Vec<OsString>,
        stdin: Option<Vec<u8>>,
    ) -> Result<ProcessResult, SelectionFailure> {
        let result = self.run_git(context, args, stdin)?;
        if result.status.success() {
            Ok(result)
        } else {
            Err(git_failure("required Git input query failed"))
        }
    }

    fn run_git(
        &self,
        context: &ProjectContext,
        args: impl IntoIterator<Item = OsString>,
        stdin: Option<Vec<u8>>,
    ) -> Result<ProcessResult, SelectionFailure> {
        let request = ProcessRequest {
            program: OsString::from(GIT_PROGRAM),
            args: std::iter::once(OsString::from("-C"))
                .chain(std::iter::once(context.root.as_os_str().to_os_string()))
                .chain(args)
                .collect(),
            environment: vec![
                (
                    OsString::from("GIT_CONFIG_GLOBAL"),
                    OsString::from("/dev/null"),
                ),
                (OsString::from("GIT_CONFIG_NOSYSTEM"), OsString::from("1")),
                (OsString::from("GIT_ATTR_NOSYSTEM"), OsString::from("1")),
            ],
            environment_remove: GIT_ENVIRONMENT_REMOVALS
                .iter()
                .map(OsString::from)
                .collect(),
            stdin,
            policy: ProcessPolicy {
                stdout_limit: GIT_OUTPUT_LIMIT,
                stderr_limit: GIT_OUTPUT_LIMIT,
                deadline: GIT_DEADLINE,
            },
        };
        let result = self.runner.run(&request).map_err(map_process_failure)?;
        if result.stdout.len() > GIT_OUTPUT_LIMIT || result.stderr.len() > GIT_OUTPUT_LIMIT {
            return Err(input_set_too_large());
        }
        Ok(result)
    }
}

fn compile_patterns(patterns: &[String]) -> Result<(Vec<String>, GlobSet), SelectionFailure> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        validate_pattern(pattern)?;
        let glob = GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .map_err(|error| {
                failure(
                    "INVALID_INCLUDE_PATTERN",
                    format!(
                        "invalid include pattern {}: {error}",
                        escape_diagnostic(pattern)
                    ),
                    Vec::new(),
                    0,
                )
            })?;
        builder.add(glob);
    }
    let matcher = builder.build().map_err(|error| {
        failure(
            "INVALID_INCLUDE_PATTERN",
            format!("could not compile include patterns: {error}"),
            Vec::new(),
            0,
        )
    })?;
    Ok((patterns.to_vec(), matcher))
}

fn validate_pattern(pattern: &str) -> Result<(), SelectionFailure> {
    if pattern.is_empty()
        || pattern.starts_with('/')
        || pattern.contains('\0')
        || pattern.contains('\\')
    {
        return Err(failure(
            "INVALID_INCLUDE_PATTERN",
            format!(
                "include pattern {} is not a safe relative pattern",
                escape_diagnostic(pattern)
            ),
            Vec::new(),
            0,
        ));
    }
    let components = pattern.split('/').collect::<Vec<_>>();
    if components
        .iter()
        .any(|component| component.is_empty() || matches!(*component, "." | ".."))
    {
        return Err(failure(
            "INVALID_INCLUDE_PATTERN",
            format!(
                "include pattern {} contains an invalid component",
                escape_diagnostic(pattern)
            ),
            Vec::new(),
            0,
        ));
    }
    let literal_prefix = components
        .iter()
        .take_while(|component| !contains_glob_metacharacter(component))
        .copied()
        .collect::<Vec<_>>();
    if literal_prefix.is_empty() {
        return Err(failure(
            "INVALID_INCLUDE_PATTERN",
            "include patterns must begin with a literal path component",
            Vec::new(),
            0,
        ));
    }
    if forbidden_components(&literal_prefix) {
        let path = RelativePath::parse(literal_prefix.join("/").as_bytes()).ok();
        let total_path_count = usize::from(path.is_some());
        return Err(failure(
            "FORBIDDEN_PATH",
            format!(
                "include pattern {} enters a forbidden directory",
                escape_diagnostic(pattern)
            ),
            path.into_iter().collect(),
            total_path_count,
        ));
    }
    Ok(())
}

fn parse_exact_paths(values: &[String]) -> Result<BTreeSet<RelativePath>, SelectionFailure> {
    values
        .iter()
        .map(|value| RelativePath::parse(value.as_bytes()))
        .collect()
}

fn parse_index(output: &[u8]) -> Result<Vec<(String, RelativePath)>, SelectionFailure> {
    let records = nul_records(output)?;
    let mut parsed = Vec::new();
    for record in records {
        reject_empty_record(record)?;
        if parsed.len() == GIT_ENTRY_LIMIT {
            return Err(input_set_too_large());
        }
        let separator = record
            .iter()
            .position(|byte| *byte == b'\t')
            .ok_or_else(|| invalid_git_output("Git returned a malformed index record"))?;
        let (header, path_with_separator) = record.split_at(separator);
        let path = &path_with_separator[1..];
        let header = std::str::from_utf8(header)
            .map_err(|_| invalid_git_output("Git returned a non-UTF-8 index header"))?;
        let mut fields = header.split(' ');
        let mode = fields.next().unwrap_or_default();
        let object = fields.next().unwrap_or_default();
        let stage = fields.next().unwrap_or_default();
        if fields.next().is_some()
            || mode.len() != 6
            || !mode.bytes().all(|byte| byte.is_ascii_digit())
            || object.len() < 40
            || !object.bytes().all(|byte| byte.is_ascii_hexdigit())
            || !matches!(stage, "0" | "1" | "2" | "3")
        {
            return Err(invalid_git_output("Git returned a malformed index header"));
        }
        parsed.push((mode.to_owned(), RelativePath::parse(path)?));
    }
    Ok(parsed)
}

fn parse_path_list(output: &[u8]) -> Result<Vec<RelativePath>, SelectionFailure> {
    let records = nul_records(output)?;
    let mut paths = Vec::new();
    for record in records {
        reject_empty_record(record)?;
        if paths.len() == GIT_ENTRY_LIMIT {
            return Err(input_set_too_large());
        }
        paths.push(RelativePath::parse(record)?);
    }
    Ok(paths)
}

fn nul_records(output: &[u8]) -> Result<impl Iterator<Item = &[u8]>, SelectionFailure> {
    if !output.is_empty() && !output.ends_with(&[0]) {
        return Err(invalid_git_output(
            "Git returned a NUL-delimited stream without a final terminator",
        ));
    }
    let records = if output.is_empty() {
        output
    } else {
        &output[..output.len() - 1]
    };
    Ok(records
        .split(is_nul as fn(&u8) -> bool)
        .take(if output.is_empty() { 0 } else { usize::MAX }))
}

fn is_nul(byte: &u8) -> bool {
    *byte == 0
}

fn reject_empty_record(record: &[u8]) -> Result<(), SelectionFailure> {
    if record.is_empty() {
        return Err(invalid_git_output("Git returned an empty path record"));
    }
    Ok(())
}

fn validate_empty_directory(root: &Path, path: &RelativePath) -> Result<(), SelectionFailure> {
    let candidate = root.join(path.as_path());
    let mut traversed = root.to_path_buf();
    for component in path.as_str().split('/') {
        traversed.push(component);
        let metadata = fs::symlink_metadata(&traversed).map_err(|error| {
            failure(
                "INVALID_EMPTY_DIRECTORY",
                format!("declared empty directory {} is unavailable: {error}", path),
                vec![path.clone()],
                1,
            )
        })?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
            return Err(failure(
                "INVALID_EMPTY_DIRECTORY",
                format!(
                    "declared empty directory {} traverses a non-directory or symlink",
                    path
                ),
                vec![path.clone()],
                1,
            ));
        }
    }
    let physical_root = fs::canonicalize(root).map_err(|error| {
        failure(
            "INPUT_INSPECTION_FAILED",
            format!("could not resolve worktree root: {error}"),
            Vec::new(),
            0,
        )
    })?;
    let physical_candidate = fs::canonicalize(&candidate).map_err(|error| {
        failure(
            "INVALID_EMPTY_DIRECTORY",
            format!(
                "could not resolve declared empty directory {}: {error}",
                path
            ),
            vec![path.clone()],
            1,
        )
    })?;
    if !physical_candidate.starts_with(&physical_root) {
        return Err(failure(
            "INVALID_EMPTY_DIRECTORY",
            format!("declared empty directory {} leaves the worktree", path),
            vec![path.clone()],
            1,
        ));
    }
    let mut entries = fs::read_dir(&candidate).map_err(|error| {
        failure(
            "INVALID_EMPTY_DIRECTORY",
            format!("could not read declared empty directory {}: {error}", path),
            vec![path.clone()],
            1,
        )
    })?;
    if let Some(entry) = entries.next() {
        entry.map_err(|error| {
            failure(
                "INVALID_EMPTY_DIRECTORY",
                format!(
                    "could not inspect declared empty directory {}: {error}",
                    path
                ),
                vec![path.clone()],
                1,
            )
        })?;
        return Err(failure(
            "INVALID_EMPTY_DIRECTORY",
            format!("declared empty directory {} is not empty", path),
            vec![path.clone()],
            1,
        ));
    }
    Ok(())
}

fn reject_forbidden(path: &RelativePath) -> Result<(), SelectionFailure> {
    let components = path.as_str().split('/').collect::<Vec<_>>();
    if forbidden_components(&components) {
        return Err(failure(
            "FORBIDDEN_PATH",
            format!("input {} is inside a forbidden directory", path),
            vec![path.clone()],
            1,
        ));
    }
    Ok(())
}

fn forbidden_components(components: &[&str]) -> bool {
    components.iter().any(|component| {
        matches!(
            *component,
            ".git"
                | "node_modules"
                | ".pnpm-store"
                | ".venv"
                | "__pycache__"
                | ".cache"
                | "target"
                | "dist"
                | ".idea"
                | ".vscode"
        )
    }) || components
        .windows(2)
        .any(|pair| matches!(pair, [".yarn", "cache"] | ["vendor", "bundle"]))
}

fn is_sensitive(path: &RelativePath) -> bool {
    let components = path.as_str().split('/').collect::<Vec<_>>();
    let basename = components.last().copied().unwrap_or_default();
    let sensitive_basename = (basename == ".env"
        || (basename.starts_with(".env.") && !matches!(basename, ".env.example" | ".env.sample")))
        || matches!(
            basename,
            ".npmrc"
                | ".pypirc"
                | ".netrc"
                | "id_rsa"
                | "id_ed25519"
                | "credentials"
                | "credentials.json"
        );
    sensitive_basename
        || components
            .iter()
            .any(|component| matches!(*component, ".ssh" | ".aws" | ".kube"))
        || components
            .windows(2)
            .any(|pair| matches!(pair, [".config", "gcloud"]))
}

fn contains_glob_metacharacter(component: &str) -> bool {
    component
        .bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b']' | b'{' | b'}' | b'!'))
}

fn path_failure(
    code: &'static str,
    summary: &str,
    mut paths: Vec<RelativePath>,
) -> SelectionFailure {
    paths.sort();
    paths.dedup();
    let total_path_count = paths.len();
    let diagnostics = paths
        .iter()
        .take(REPORTED_PATH_LIMIT)
        .map(RelativePath::escaped_display)
        .collect::<Vec<_>>()
        .join(", ");
    paths.truncate(REPORTED_PATH_LIMIT);
    let message = if diagnostics.is_empty() {
        summary.to_owned()
    } else {
        format!("{summary}: {diagnostics}")
    };
    failure(code, message, paths, total_path_count)
}

fn escape_diagnostic(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '"' => escaped.push_str("\\\""),
            character if character.is_control() => {
                escaped.extend(character.escape_unicode());
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn map_process_failure(error: WorkerError) -> SelectionFailure {
    match error {
        WorkerError::Process(ProcessError::OutputLimitExceeded { .. }) => input_set_too_large(),
        error => failure(
            "GIT_INPUT_SELECTION_FAILED",
            format!("could not run Git input query: {error}"),
            Vec::new(),
            0,
        ),
    }
}

fn input_set_too_large() -> SelectionFailure {
    failure(
        "INPUT_SET_TOO_LARGE",
        "Git input response exceeded the snapshot selection bounds",
        Vec::new(),
        0,
    )
}

fn invalid_git_output(message: &str) -> SelectionFailure {
    failure("INVALID_GIT_OUTPUT", message, Vec::new(), 0)
}

fn git_failure(message: &str) -> SelectionFailure {
    failure("GIT_INPUT_SELECTION_FAILED", message, Vec::new(), 0)
}

fn failure(
    code: &'static str,
    message: impl Into<String>,
    paths: Vec<RelativePath>,
    total_path_count: usize,
) -> SelectionFailure {
    SelectionFailure {
        code,
        message: message.into(),
        paths,
        total_path_count,
    }
}
