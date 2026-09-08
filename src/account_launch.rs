use std::{ffi::OsString, path::Path};

use crate::process::{ProcessPolicy, ProcessRequest};

pub(crate) fn account_environment_scaffold(account_home: &Path) -> Vec<(OsString, OsString)> {
    let user = account_user(account_home);
    vec![
        (
            OsString::from("HOME"),
            account_home.as_os_str().to_os_string(),
        ),
        (
            OsString::from("USER"),
            account_environment_value("USER", user.clone()),
        ),
        (
            OsString::from("LOGNAME"),
            account_environment_value("LOGNAME", user),
        ),
        (
            OsString::from("SHELL"),
            account_environment_value("SHELL", OsString::from("/bin/zsh")),
        ),
    ]
}

pub(crate) fn account_login_shell_request(
    account_home: &Path,
    profile_entries: &[(OsString, OsString)],
    shell_command: &str,
    policy: ProcessPolicy,
) -> ProcessRequest {
    let mut environment = account_environment_scaffold(account_home);
    environment.extend(profile_entries.iter().cloned());
    ProcessRequest {
        program: OsString::from("/bin/zsh"),
        args: vec![
            OsString::from("-lc"),
            OsString::from(shell_command.to_owned()),
        ],
        environment,
        environment_remove: Vec::new(),
        stdin: None,
        policy,
        isolate_parent_environment: true,
    }
}

fn account_user(home: &Path) -> OsString {
    home.file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("worker"))
}

fn account_environment_value(name: &str, fallback: OsString) -> OsString {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
}
