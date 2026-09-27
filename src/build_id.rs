//! Build identity baked by `build.rs`.
//!
//! The generated file lives in `OUT_DIR`, so only this module recompiles when
//! the git id changes. Callers see a `&'static str`.

include!(concat!(env!("OUT_DIR"), "/build_id.rs"));

/// True when the id's profile suffix is `debug`.
///
/// The shape is `<version>+<sha>[.dirty]-<debug|release>`. Anything else,
/// including a missing id, is not treated as a debug build.
pub fn is_debug_build_id(build_id: &str) -> bool {
    matches!(build_id.rsplit_once('-'), Some((_, "debug")))
}

#[cfg(test)]
mod tests {
    use super::{BUILD_ID, is_debug_build_id};

    #[test]
    fn build_id_is_version_sha_and_profile() {
        let (version, rest) = BUILD_ID
            .split_once('+')
            .expect("build id must contain a version separator");
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
        let (git, profile) = rest
            .rsplit_once('-')
            .expect("build id must contain a profile suffix");
        assert!(profile == "debug" || profile == "release", "{BUILD_ID}");
        let sha = git.strip_suffix(".dirty").unwrap_or(git);
        assert!(
            sha == "unknown" || (sha.len() == 12 && sha.chars().all(|c| c.is_ascii_hexdigit())),
            "{BUILD_ID}"
        );
    }

    #[test]
    fn debug_suffix_is_the_only_debug_profile() {
        assert!(is_debug_build_id("0.1.0+0123456789ab-debug"));
        assert!(is_debug_build_id("0.1.0+0123456789ab.dirty-debug"));
        assert!(!is_debug_build_id("0.1.0+0123456789ab-release"));
        assert!(!is_debug_build_id("0.1.0+unknown-release"));
        assert!(!is_debug_build_id("not-a-build-id"));
    }
}
