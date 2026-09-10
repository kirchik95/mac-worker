use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use crate::error::WorkerError;

const HOST_STATE_DIRECTORY: &str = "host";

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

    /// Returns the fixed worker-owned execution root beneath the installer data container.
    #[must_use]
    pub fn host_state_root(&self) -> PathBuf {
        self.data.join(HOST_STATE_DIRECTORY)
    }

    /// Returns the controller durable-request root. Sibling of `ClientStateStore`
    /// so `validate_root_entries` stays closed.
    #[must_use]
    pub fn controller_state_root(&self) -> PathBuf {
        self.state
            .parent()
            .map(|parent| parent.join("mac-worker-controller"))
            .unwrap_or_else(|| self.data.join("controller"))
    }

    /// Laptop transport cache for controller operation envelopes. Not a task store.
    #[must_use]
    pub fn controller_cache_root(&self) -> PathBuf {
        self.cache.join("controller")
    }

    /// Controller-owned checkouts for frozen logical project/worktree IDs.
    #[must_use]
    pub fn controller_project_root(&self) -> PathBuf {
        self.data.join("controller-projects")
    }
}

fn env_path(env: &BTreeMap<OsString, OsString>, key: &str) -> Option<PathBuf> {
    env.get(OsStr::new(key))
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        os::unix::ffi::OsStringExt,
        path::{Path, PathBuf},
    };

    use super::PathLayout;

    fn relative_xdg_env() -> BTreeMap<OsString, OsString> {
        BTreeMap::from([
            ("XDG_CONFIG_HOME".into(), "relative/config".into()),
            ("XDG_STATE_HOME".into(), "relative/state".into()),
            ("XDG_CACHE_HOME".into(), "relative/cache".into()),
            ("XDG_DATA_HOME".into(), "relative/data".into()),
        ])
    }

    #[test]
    fn relative_xdg_base_directories_fall_back_to_home_defaults() {
        // Regression: relative XDG roots were accepted and resolved against
        // the process working directory instead of being ignored.
        let paths =
            PathLayout::discover(None, &relative_xdg_env(), Path::new("/Users/tester")).unwrap();

        assert_eq!(
            paths.config,
            PathBuf::from("/Users/tester/.config/mac-worker/config.toml")
        );
        assert_eq!(
            paths.state,
            PathBuf::from("/Users/tester/.local/state/mac-worker")
        );
        assert_eq!(
            paths.cache,
            PathBuf::from("/Users/tester/.cache/mac-worker")
        );
        assert_eq!(
            paths.data,
            PathBuf::from("/Users/tester/.local/share/mac-worker")
        );
        assert_eq!(
            paths.controller_state_root(),
            PathBuf::from("/Users/tester/.local/state/mac-worker-controller")
        );
        assert_eq!(
            paths.controller_cache_root(),
            PathBuf::from("/Users/tester/.cache/mac-worker/controller")
        );
    }

    #[test]
    fn explicit_relative_config_still_overrides_relative_xdg_values() {
        // Regression guard: filtering XDG roots must not filter the explicit
        // CLI config path, whose relative form remains intentional.
        let paths = PathLayout::discover(
            Some(PathBuf::from("relative/config.toml")),
            &relative_xdg_env(),
            Path::new("/Users/tester"),
        )
        .unwrap();

        assert_eq!(paths.config, PathBuf::from("relative/config.toml"));
        assert_eq!(
            paths.state,
            PathBuf::from("/Users/tester/.local/state/mac-worker")
        );
    }

    #[test]
    fn absolute_non_utf8_xdg_base_directory_is_preserved() {
        // Regression guard: absolute-path filtering must remain byte-oriented
        // and must not discard a valid non-UTF-8 Unix path.
        let config_home = OsString::from_vec(b"/tmp/config-\xff".to_vec());
        let paths = PathLayout::discover(
            None,
            &BTreeMap::from([("XDG_CONFIG_HOME".into(), config_home.clone())]),
            Path::new("/Users/tester"),
        )
        .unwrap();

        assert_eq!(
            paths.config,
            PathBuf::from(config_home).join("mac-worker/config.toml")
        );
    }

    #[test]
    fn host_state_root_is_a_fixed_child_of_the_default_data_container() {
        let paths =
            PathLayout::discover(None, &BTreeMap::new(), Path::new("/Users/tester")).unwrap();

        assert_eq!(
            paths.host_state_root(),
            PathBuf::from("/Users/tester/.local/share/mac-worker/host")
        );
    }

    #[test]
    fn host_state_root_uses_the_absolute_xdg_data_container() {
        let paths = PathLayout::discover(
            None,
            &BTreeMap::from([("XDG_DATA_HOME".into(), "/srv/worker-data".into())]),
            Path::new("/Users/tester"),
        )
        .unwrap();

        assert_eq!(
            paths.host_state_root(),
            PathBuf::from("/srv/worker-data/mac-worker/host")
        );
    }

    #[test]
    fn host_state_root_joins_non_utf8_xdg_data_without_text_conversion() {
        let data_home = OsString::from_vec(b"/tmp/data-\xff".to_vec());
        let paths = PathLayout::discover(
            None,
            &BTreeMap::from([("XDG_DATA_HOME".into(), data_home.clone())]),
            Path::new("/Users/tester"),
        )
        .unwrap();

        assert_eq!(
            paths.host_state_root(),
            PathBuf::from(data_home).join("mac-worker/host")
        );
    }
}
