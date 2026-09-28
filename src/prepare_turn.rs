use std::{
    ffi::{OsStr, OsString},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    error::WorkerError,
    process::InheritProcessGroupRunner,
    project_readiness::{
        FrozenSetup, SETUP_STAGE_EXIT_CODE, SetupRequest, persist_setup_stage_result,
        prepare_project_setup,
    },
    turn::EnvProfile,
};

pub const ARG: &str = "__mac_worker_prepare_turn";
const IDENTITY_ARG: &str = "__mac_worker_record_agent";

pub fn requested() -> bool {
    std::env::args_os()
        .nth(1)
        .is_some_and(|argument| argument == ARG || argument == IDENTITY_ARG)
}

pub fn run() -> ExitCode {
    ExitCode::from(run_with_args(std::env::args_os()))
}

/// Libtest helper entry: the recorded child execs this test binary, then this
/// reconstructs the production `[helper, ARG, --, agent...]` argv.
pub fn run_from_env_agent() -> u8 {
    let mut args = vec![
        std::env::args_os()
            .next()
            .unwrap_or_else(|| OsString::from("worker")),
        OsString::from(ARG),
        OsString::from("--"),
    ];
    let encoded = std::env::var("MAC_WORKER_PREPARE_TURN_AGENT").unwrap_or_else(|_| "[]".into());
    args.extend(decode_libtest_agent_argv(&encoded));
    run_with_args(args)
}

/// JSON array of strings. Empty arguments are kept; NUL is not used because
/// POSIX environment values cannot contain it.
fn decode_libtest_agent_argv(encoded: &str) -> Vec<OsString> {
    let parts: Vec<String> = serde_json::from_str(encoded).unwrap_or_default();
    parts.into_iter().map(OsString::from).collect()
}

fn run_with_args<I>(args: I) -> u8
where
    I: IntoIterator<Item = OsString>,
{
    let args: Vec<_> = args.into_iter().collect();
    let result = if args.get(1).is_some_and(|arg| arg == IDENTITY_ARG) {
        agent_argv(args).and_then(|agent| record_then_exec_agent(&agent))
    } else {
        run_setup_then_exec_agent(args)
    };
    let error = match result {
        Ok(()) => {
            // SAFETY: the helper is the recorded child. `_exit` skips
            // destructors so abandoned inherit-group capture threads cannot
            // join after a recipe timeout.
            unsafe { libc::_exit(70) };
        }
        Err(error) => error,
    };
    if let Some(turn_dir) = std::env::var_os("MAC_WORKER_TURN_DIR") {
        let _ = persist_setup_stage_result(Path::new(&turn_dir), &error);
    }
    SETUP_STAGE_EXIT_CODE
}

fn run_setup_then_exec_agent<I>(args: I) -> Result<(), WorkerError>
where
    I: IntoIterator<Item = OsString>,
{
    let agent = agent_argv(args)?;
    run_supervised_turn_setup()?;
    exec_agent(&agent)
}

fn agent_argv<I>(args: I) -> Result<Vec<OsString>, WorkerError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let _argv0 = args.next();
    let marker = args.next();
    if !matches!(marker.as_deref(), Some(marker) if marker == ARG || marker == IDENTITY_ARG) {
        return Err(WorkerError::task(
            "SETUP_FAILED",
            "prepare-turn helper marker is absent",
        ));
    }
    let separator = args.next();
    if separator.as_deref() != Some(OsStr::new("--")) {
        return Err(WorkerError::task(
            "SETUP_FAILED",
            "prepare-turn agent argv is missing",
        ));
    }
    let agent: Vec<OsString> = args.collect();
    if agent.is_empty() {
        return Err(WorkerError::task(
            "SETUP_FAILED",
            "prepare-turn agent argv is empty",
        ));
    }
    Ok(agent)
}

fn exec_agent(argv: &[OsString]) -> Result<(), WorkerError> {
    if argv.len() == 3
        && argv[0] == "/bin/zsh"
        && argv[1] == "-lc"
        && let Some(command) = argv[2]
            .to_str()
            .and_then(|shell| shell.strip_prefix("exec "))
    {
        let helper = std::env::current_exe().map_err(WorkerError::Io)?;
        let helper = helper
            .to_str()
            .ok_or_else(|| WorkerError::task("SETUP_FAILED", "helper path is not UTF-8"))?;
        let shell = format!(
            "exec '{}' {IDENTITY_ARG} -- {command}",
            helper.replace('\'', "'\\''")
        );
        return exec_direct(&[argv[0].clone(), argv[1].clone(), shell.into()]);
    }
    exec_direct(argv)
}

fn exec_direct(argv: &[OsString]) -> Result<(), WorkerError> {
    let program = &argv[0];
    let error = Command::new(program).args(&argv[1..]).exec();
    Err(WorkerError::task(
        "SETUP_FAILED",
        format!("agent executable could not be started: {error}"),
    ))
}

fn record_then_exec_agent(argv: &[OsString]) -> Result<(), WorkerError> {
    use crate::agent::identity::{VERSION_DEADLINE, observe_identity};
    let executable = resolve_executable(&argv[0])?;
    let deadline = std::env::var("MAC_WORKER_LEASE_DEADLINE_MILLIS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|end| Duration::from_millis(end.saturating_sub(unix_now_millis().unwrap_or(end))))
        .unwrap_or(VERSION_DEADLINE)
        .min(VERSION_DEADLINE);
    let identity = observe_identity(&InheritProcessGroupRunner, &executable, deadline);
    // Identity is diagnostic: unavailable versions or storage must not stop
    // real work. On a probe bound, the runner reaps the direct probe child.
    // Exec closes the helper's capture FDs and discards its capture threads;
    // any remaining descendants stay in the recorded group for supervisor
    // cleanup after the agent exits or the turn is cancelled.
    let _ = persist_agent_identity(&identity);
    let mut resolved = argv.to_vec();
    resolved[0] = executable.into_os_string();
    exec_direct(&resolved)
}

fn persist_agent_identity(identity: &crate::agent::AgentIdentity) -> Result<(), WorkerError> {
    let dir = std::env::var_os("MAC_WORKER_TURN_DIR")
        .ok_or_else(|| WorkerError::task("SETUP_FAILED", "turn identity directory is absent"))?;
    let bytes = serde_json::to_vec(identity)
        .map_err(|_| WorkerError::task("SETUP_FAILED", "cannot encode agent identity"))?;
    stage_turn_diagnostic(
        Path::new(&dir),
        crate::agent::identity::STAGED_IDENTITY_FILE,
        &bytes,
    )?;
    Ok(())
}

/// This helper runs in the agent's process group, so a cancel can kill it at
/// any instruction. It must never write into the retained turn directory,
/// where an interrupted private write is cleanup residue. Diagnostics are
/// staged in the turn's `tmp` scope, which terminal cleanup removes whole,
/// and the supervisor adopts complete records after the child is gone.
pub(crate) fn stage_turn_diagnostic(
    turn_dir: &Path,
    name: &str,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let tmp = turn_dir.join("tmp");
    let partial = tmp.join(format!(".{name}.{}", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&partial)?;
    file.write_all(bytes)?;
    drop(file);
    std::fs::rename(&partial, tmp.join(name))
}

fn resolve_executable(program: &OsStr) -> Result<PathBuf, WorkerError> {
    let candidates = if program.as_encoded_bytes().contains(&b'/') {
        vec![PathBuf::from(program)]
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()))
            .map(|directory| directory.join(program))
            .collect()
    };
    for path in candidates {
        if let Ok(metadata) = path.metadata()
            && metadata.is_file()
            && metadata.permissions().mode() & 0o111 != 0
        {
            return std::fs::canonicalize(path).map_err(WorkerError::Io);
        }
    }
    Err(WorkerError::task(
        "AGENT_EXECUTABLE_NOT_FOUND",
        "agent executable was not found in the final login context",
    ))
}

fn run_supervised_turn_setup() -> Result<(), WorkerError> {
    if std::env::var_os("MAC_WORKER_TURN_DIR").is_none() {
        return Ok(());
    }
    let workspace = std::env::current_dir().map_err(WorkerError::Io)?;
    let Some(encoded) = std::env::var_os("MAC_WORKER_FROZEN_SETUP") else {
        return Ok(());
    };
    let frozen: FrozenSetup = serde_json::from_str(&encoded.to_string_lossy())
        .map_err(|_| WorkerError::task("SETUP_FAILED", "frozen setup is invalid"))?;
    frozen.verify_inputs(&workspace)?;
    let recipe = frozen.settings();
    let account_home = PathBuf::from(
        std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .ok_or_else(|| WorkerError::task("SETUP_FAILED", "worker account HOME is absent"))?,
    );
    let task_id = std::env::var("MAC_WORKER_TASK_ID")
        .map_err(|_| WorkerError::task("SETUP_FAILED", "setup task id is absent"))?;
    let project_id = std::env::var("MAC_WORKER_PROJECT_ID")
        .map_err(|_| WorkerError::task("SETUP_FAILED", "setup project id is absent"))?;
    let env_profile_name = std::env::var("MAC_WORKER_ENV_PROFILE")
        .ok()
        .filter(|name| !name.is_empty());
    let profile = match env_profile_name.as_deref() {
        Some(name) => EnvProfile::load_for_home(
            &account_home
                .join(".config")
                .join("mac-worker")
                .join("env")
                .join(format!("{name}.env")),
            &account_home,
        )?,
        None => EnvProfile::empty(),
    };
    let cache_root = account_home.join(".cache").join("mac-worker");
    let started = Instant::now();
    let recipe_deadline = started
        .checked_add(recipe.timeout)
        .ok_or_else(|| WorkerError::task("SETUP_FAILED", "setup deadline overflow"))?;
    let lease_deadline = match std::env::var("MAC_WORKER_LEASE_DEADLINE_MILLIS") {
        Ok(value) => {
            let deadline_ms: u64 = value.parse().map_err(|_| {
                WorkerError::task("SETUP_FAILED", "setup lease deadline is invalid")
            })?;
            let now_ms = unix_now_millis()?;
            started + Duration::from_millis(deadline_ms.saturating_sub(now_ms))
        }
        Err(_) => recipe_deadline,
    };
    let deadline = recipe_deadline.min(lease_deadline);
    prepare_project_setup(
        &InheritProcessGroupRunner,
        SetupRequest {
            account_home: &account_home,
            project_id: &project_id,
            task_id: &task_id,
            env_profile_name: env_profile_name.as_deref(),
            workspace: &workspace,
            cache_root: &cache_root,
            requires: frozen.requires(),
            recipe: &recipe,
            profile_entries: profile.entries(),
            now_millis: unix_now_millis()?,
            deadline,
            should_cancel: &|| false,
        },
    )
    .map(|_| ())
}

fn unix_now_millis() -> Result<u64, WorkerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkerError::task("SETUP_FAILED", "system clock precedes the Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| {
            WorkerError::task(
                "SETUP_FAILED",
                "system clock is outside the supported range",
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn libtest_agent_json_preserves_empty_arguments() {
        let encoded = serde_json::to_string(&["/bin/zsh", "", "kept"]).unwrap();
        assert!(
            !encoded.as_bytes().contains(&0),
            "JSON argv encoding must be NUL-free"
        );
        assert_eq!(
            decode_libtest_agent_argv(&encoded),
            vec![
                OsString::from("/bin/zsh"),
                OsString::from(""),
                OsString::from("kept"),
            ]
        );
    }
}
