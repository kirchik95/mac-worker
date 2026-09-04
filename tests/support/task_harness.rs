use std::path::{Path, PathBuf};

use mac_worker::paths::PathLayout;

/// Returns the isolated client-state roots used by task/runner integration
/// fixtures. Keeping this in one helper makes it harder for a test to
/// accidentally write into the developer's real state directory.
pub fn paths(root: impl AsRef<Path>) -> PathLayout {
    let root = root.as_ref().to_path_buf();
    PathLayout {
        config: root.join("config.toml"),
        state: root.join("state"),
        cache: root.join("cache"),
        data: root.join("data"),
    }
}

/// A stable path for an owner-only runner log in a fixture.
pub fn runner_log(root: impl AsRef<Path>, task: &str, turn: &str) -> PathBuf {
    root.as_ref()
        .join("state")
        .join("runners")
        .join(task)
        .join(format!("{turn}.log"))
}
