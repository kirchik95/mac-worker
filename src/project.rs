use std::{
    ffi::OsString,
    fs,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::{Digest, Sha256};
use url::Url;

use crate::{
    error::WorkerError,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_OUTPUT_LIMIT: usize = 8 * 1024 * 1024;
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
];

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

impl<'a> ProjectInspector<'a> {
    pub fn new(runner: &'a dyn ProcessRunner) -> Self {
        Self { runner }
    }

    pub fn inspect(&self, cwd: &Path) -> Result<ProjectContext, WorkerError> {
        self.inspect_with_origin(cwd).map(|(context, _)| context)
    }

    pub(crate) fn inspect_with_origin(
        &self,
        cwd: &Path,
    ) -> Result<(ProjectContext, Option<String>), WorkerError> {
        let mut context = self.inspect_without_origin(cwd)?;
        let origin = self.normalized_origin(cwd)?;
        context.project_id = match &origin {
            Some(origin) => hash_identity(b"origin\0", normalize_origin(origin)?.as_bytes()),
            None => hash_identity(b"common-dir\0", context.common_dir.as_os_str().as_bytes()),
        };
        Ok((context, origin))
    }

    /// Inspects the local worktree without consulting the mutable Git origin.
    ///
    /// Durable task records supply the project identity after submission, so
    /// recovery paths can retain their local filesystem context without
    /// recomputing an origin-derived identity.
    pub(crate) fn inspect_with_pinned_project_id(
        &self,
        cwd: &Path,
        project_id: &str,
    ) -> Result<ProjectContext, WorkerError> {
        let mut context = self.inspect_without_origin(cwd)?;
        context.project_id = project_id.to_owned();
        Ok(context)
    }

    fn inspect_without_origin(&self, cwd: &Path) -> Result<ProjectContext, WorkerError> {
        let root = self.required_path(
            cwd,
            &["rev-parse", "--path-format=absolute", "--show-toplevel"],
            true,
        )?;
        let current = fs::canonicalize(cwd).map_err(|error| {
            project_error(
                "GIT_INSPECTION_FAILED",
                format!("could not canonicalize the requested directory: {error}"),
            )
        })?;
        let relative_cwd = current
            .strip_prefix(&root)
            .map_err(|_| {
                project_error(
                    "PATH_OUTSIDE_WORKTREE",
                    "the requested directory is outside the discovered worktree root".into(),
                )
            })?
            .to_path_buf();
        let git_dir = self.required_path(
            cwd,
            &["rev-parse", "--path-format=absolute", "--git-dir"],
            false,
        )?;
        let common_dir = self.required_path(
            cwd,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
            false,
        )?;
        let dirty = !self
            .required_result(
                cwd,
                &["status", "--porcelain=v2", "-z", "--untracked-files=normal"],
                false,
            )?
            .stdout
            .is_empty();
        let head = self.optional_utf8_scalar(cwd, &["rev-parse", "--verify", "HEAD"])?;
        let branch =
            self.optional_utf8_scalar(cwd, &["symbolic-ref", "--quiet", "--short", "HEAD"])?;
        let worktree_id = hash_identity(b"worktree\0", root.as_os_str().as_bytes());

        Ok(ProjectContext {
            root,
            relative_cwd,
            git_dir,
            common_dir,
            head,
            branch,
            project_id: String::new(),
            worktree_id,
            dirty,
        })
    }

    /// Returns the project's origin in the canonical form used by task
    /// records and worker capability requirements.  The raw Git config value
    /// is deliberately never returned to callers.
    pub fn normalized_origin(&self, cwd: &Path) -> Result<Option<String>, WorkerError> {
        self.optional_utf8_scalar(cwd, &["config", "--get", "remote.origin.url"])?
            .map(|origin| normalize_origin(&origin))
            .transpose()
    }

    fn required_path(
        &self,
        cwd: &Path,
        args: &[&str],
        is_worktree_query: bool,
    ) -> Result<PathBuf, WorkerError> {
        let output = self.required_result(cwd, args, is_worktree_query)?;
        let scalar = parse_scalar(&output.stdout)?;
        if scalar.is_empty() {
            return Err(project_error(
                "INVALID_GIT_OUTPUT",
                "Git returned an empty path".into(),
            ));
        }
        fs::canonicalize(PathBuf::from(OsString::from_vec(scalar))).map_err(|error| {
            project_error(
                "GIT_INSPECTION_FAILED",
                format!("could not canonicalize Git path output: {error}"),
            )
        })
    }

    fn optional_utf8_scalar(
        &self,
        cwd: &Path,
        args: &[&str],
    ) -> Result<Option<String>, WorkerError> {
        let output = self.run_git(cwd, args)?;
        if output.status.success() {
            let scalar = parse_scalar(&output.stdout)?;
            let value = std::str::from_utf8(&scalar).map_err(|_| {
                project_error(
                    "INVALID_GIT_OUTPUT",
                    "Git returned non-UTF-8 scalar output".into(),
                )
            })?;
            return Ok(Some(value.to_owned()));
        }
        if matches!(output.status.code(), Some(1 | 128)) {
            return Ok(None);
        }
        Err(git_failure())
    }

    fn required_result(
        &self,
        cwd: &Path,
        args: &[&str],
        is_worktree_query: bool,
    ) -> Result<ProcessResult, WorkerError> {
        let output = self.run_git(cwd, args)?;
        if output.status.success() {
            Ok(output)
        } else if is_worktree_query {
            Err(project_error(
                "NOT_A_WORKTREE",
                "the requested directory is not inside a Git worktree".into(),
            ))
        } else {
            Err(git_failure())
        }
    }

    fn run_git(&self, cwd: &Path, args: &[&str]) -> Result<ProcessResult, WorkerError> {
        let request = ProcessRequest {
            program: OsString::from(GIT_PROGRAM),
            args: [
                vec![OsString::from("-C"), cwd.as_os_str().to_os_string()],
                args.iter().map(OsString::from).collect(),
            ]
            .concat(),
            environment: vec![
                (
                    OsString::from("GIT_CONFIG_GLOBAL"),
                    OsString::from("/dev/null"),
                ),
                (OsString::from("GIT_CONFIG_NOSYSTEM"), OsString::from("1")),
            ],
            environment_remove: GIT_ENVIRONMENT_REMOVALS
                .iter()
                .map(OsString::from)
                .collect(),
            stdin: None,
            policy: ProcessPolicy {
                stdout_limit: GIT_OUTPUT_LIMIT,
                stderr_limit: GIT_OUTPUT_LIMIT,
                deadline: GIT_DEADLINE,
            },
        };
        self.runner.run(&request).map_err(|error| {
            project_error(
                "GIT_INSPECTION_FAILED",
                format!("could not run Git inspection: {error}"),
            )
        })
    }
}

fn parse_scalar(output: &[u8]) -> Result<Vec<u8>, WorkerError> {
    let scalar = output.strip_suffix(b"\n").unwrap_or(output);
    if scalar.contains(&b'\r') || scalar.contains(&b'\n') || scalar.contains(&b'\0') {
        return Err(project_error(
            "INVALID_GIT_OUTPUT",
            "Git returned malformed scalar output".into(),
        ));
    }
    Ok(scalar.to_vec())
}

pub fn normalize_origin(origin: &str) -> Result<String, WorkerError> {
    if let Ok(mut url) = Url::parse(origin)
        && matches!(url.scheme(), "http" | "https" | "ssh")
    {
        let scheme = url.scheme().to_ascii_lowercase();
        url.set_scheme(&scheme).map_err(|_| invalid_origin())?;
        if let Some(host) = url.host_str() {
            url.set_host(Some(&host.to_ascii_lowercase()))
                .map_err(|_| invalid_origin())?;
        }
        url.set_username("").map_err(|_| invalid_origin())?;
        url.set_password(None).map_err(|_| invalid_origin())?;
        url.set_query(None);
        url.set_fragment(None);
        return Ok(url.to_string());
    }

    if let Some((host, path)) = origin.split_once(':')
        && !host.contains('/')
        && !host.is_empty()
        && !path.is_empty()
    {
        let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
        if !host.is_empty() {
            return Ok(format!("{}:{path}", host.to_ascii_lowercase()));
        }
    }

    Err(invalid_origin())
}

pub fn origin_host(origin: &str) -> Result<String, WorkerError> {
    let normalized = normalize_origin(origin)?;
    if let Ok(url) = Url::parse(&normalized) {
        return url
            .host_str()
            .map(str::to_owned)
            .filter(|host| !host.is_empty())
            .ok_or_else(invalid_origin);
    }
    normalized
        .split_once(':')
        .map(|(host, _)| host.to_owned())
        .filter(|host| !host.is_empty())
        .ok_or_else(invalid_origin)
}

fn hash_identity(prefix: &[u8], value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix);
    hasher.update(value);
    format!("{:x}", hasher.finalize())
}

fn invalid_origin() -> WorkerError {
    project_error(
        "INVALID_ORIGIN",
        "Git returned an unsupported origin URL".into(),
    )
}

fn git_failure() -> WorkerError {
    project_error(
        "GIT_INSPECTION_FAILED",
        "a required Git inspection command failed".into(),
    )
}

fn project_error(code: &'static str, message: String) -> WorkerError {
    WorkerError::Project { code, message }
}
