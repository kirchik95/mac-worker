use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use crate::error::WorkerError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathLayout {
    pub config: PathBuf,
    pub state: PathBuf,
    pub cache: PathBuf,
    pub data: PathBuf,
}

impl PathLayout {
    pub fn discover(
        config_override: Option<PathBuf>,
        env: &BTreeMap<OsString, OsString>,
        home: &Path,
    ) -> Result<Self, WorkerError> {
        if home.as_os_str().is_empty() {
            return Err(WorkerError::Config("home directory is unavailable".into()));
        }

        let config_home = env_path(env, "XDG_CONFIG_HOME").unwrap_or_else(|| home.join(".config"));
        let state_home =
            env_path(env, "XDG_STATE_HOME").unwrap_or_else(|| home.join(".local/state"));
        let cache_home = env_path(env, "XDG_CACHE_HOME").unwrap_or_else(|| home.join(".cache"));
        let data_home = env_path(env, "XDG_DATA_HOME").unwrap_or_else(|| home.join(".local/share"));

        Ok(Self {
            config: config_override.unwrap_or_else(|| config_home.join("mac-worker/config.toml")),
            state: state_home.join("mac-worker"),
            cache: cache_home.join("mac-worker"),
            data: data_home.join("mac-worker"),
        })
    }
}

fn env_path(env: &BTreeMap<OsString, OsString>, key: &str) -> Option<PathBuf> {
    env.get(OsStr::new(key))
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}
