use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub struct GitRepo {
    directory: tempfile::TempDir,
}

impl GitRepo {
    pub fn init() -> Self {
        let directory = tempfile::tempdir().expect("create repository directory");
        let repo = Self { directory };
        fs::create_dir(repo.root().join("home")).expect("create isolated home directory");
        assert!(
            repo.git(&["init", "--initial-branch=main"])
                .status
                .success()
        );
        assert!(
            repo.git(&["config", "user.name", "Project Inspector Test"])
                .status
                .success()
        );
        assert!(
            repo.git(&["config", "user.email", "project-inspector@example.test"])
                .status
                .success()
        );
        repo
    }

    pub fn root(&self) -> &Path {
        self.directory.path()
    }

    pub fn write(&self, path: &str, bytes: &[u8]) {
        let path = self.root().join(path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent directories");
        }
        fs::write(path, bytes).expect("write repository fixture file");
    }

    pub fn git(&self, args: &[&str]) -> Output {
        Command::new("/usr/bin/git")
            .current_dir(self.root())
            .env("HOME", self.root().join("home"))
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args(args)
            .output()
            .expect("run isolated git fixture command")
    }

    pub fn commit_all(&self, message: &str) {
        assert!(self.git(&["add", "--all"]).status.success());
        assert!(self.git(&["commit", "-m", message]).status.success());
    }
}

pub fn create_directory(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref().to_path_buf();
    fs::create_dir_all(&path).expect("create fixture directory");
    path
}
