//! Bake `<version>+<git sha>[.dirty]-<debug|release>` into one generated module.
//!
//! The id is written under `OUT_DIR` and included by `src/build_id.rs` only.
//! `cargo:rerun-if-changed` names the git files that change the id, so an
//! ordinary source edit does not rebuild the crate just to refresh the id.
//! In a worktree `.git` is a file; the real git directory is resolved from it.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let profile = if std::env::var("PROFILE").as_deref() == Ok("release") {
        "release"
    } else {
        "debug"
    };
    let version = std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION");
    let git = git_identity(&manifest_dir);
    let id = format!("{version}+{git}-{profile}");

    let dest = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("build_id.rs");
    let contents = format!(
        "pub const BUILD_ID: &str = \"{}\";\n",
        escape_rust_string(&id)
    );
    if fs::read_to_string(&dest).ok().as_deref() != Some(contents.as_str()) {
        fs::write(&dest, contents).expect("write build id module");
    }
}

fn escape_rust_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn git_identity(manifest_dir: &Path) -> String {
    let Some(git_dir) = resolve_git_dir(manifest_dir) else {
        return "unknown".into();
    };
    emit_git_reruns(manifest_dir, &git_dir);
    let Some(sha) = git_stdout(manifest_dir, &["rev-parse", "--short=12", "HEAD"]) else {
        return "unknown".into();
    };
    if sha.len() != 12 || !sha.chars().all(|character| character.is_ascii_hexdigit()) {
        return "unknown".into();
    }
    if git_dirty(manifest_dir) {
        format!("{sha}.dirty")
    } else {
        sha
    }
}

/// `.git` is a directory in a normal checkout and a `gitdir:` file in a worktree.
fn resolve_git_dir(manifest_dir: &Path) -> Option<PathBuf> {
    let git_path = manifest_dir.join(".git");
    if git_path.is_dir() {
        return Some(git_path);
    }
    let text = fs::read_to_string(&git_path).ok()?;
    let rest = text.lines().next()?.trim().strip_prefix("gitdir:")?.trim();
    if rest.is_empty() {
        return None;
    }
    let raw = PathBuf::from(rest);
    let resolved = if raw.is_absolute() {
        raw
    } else {
        git_path.parent()?.join(raw)
    };
    Some(fs::canonicalize(&resolved).unwrap_or(resolved))
}

fn emit_git_reruns(manifest_dir: &Path, git_dir: &Path) {
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join(".git").display()
    );
    println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
    println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    let Ok(head) = fs::read_to_string(git_dir.join("HEAD")) else {
        return;
    };
    let Some(refname) = head.trim().strip_prefix("ref: ") else {
        return;
    };
    let local_ref = git_dir.join(refname);
    if local_ref.is_file() {
        println!("cargo:rerun-if-changed={}", local_ref.display());
    }
    let Some(common) = common_dir(git_dir) else {
        return;
    };
    let shared_ref = common.join(refname);
    if shared_ref.is_file() {
        println!("cargo:rerun-if-changed={}", shared_ref.display());
    }
    let packed = common.join("packed-refs");
    if packed.is_file() {
        println!("cargo:rerun-if-changed={}", packed.display());
    }
}

fn common_dir(git_dir: &Path) -> Option<PathBuf> {
    let pointer = git_dir.join("commondir");
    if !pointer.is_file() {
        return Some(git_dir.to_path_buf());
    }
    let text = fs::read_to_string(pointer).ok()?;
    let raw = PathBuf::from(text.trim());
    if raw.as_os_str().is_empty() {
        return None;
    }
    let resolved = if raw.is_absolute() {
        raw
    } else {
        git_dir.join(raw)
    };
    Some(fs::canonicalize(&resolved).unwrap_or(resolved))
}

fn git_stdout(dir: &Path, args: &[&str]) -> Option<String> {
    let output = git_command(dir, args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_owned())
    }
}

fn git_dirty(dir: &Path) -> bool {
    let Ok(output) = git_command(dir, &["status", "--porcelain"]).output() else {
        return false;
    };
    output.status.success() && output.stdout.iter().any(|byte| !byte.is_ascii_whitespace())
}

fn git_command(dir: &Path, args: &[&str]) -> Command {
    let mut command = Command::new("git");
    command
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0");
    command
}
