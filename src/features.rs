//! Additive wire feature registry, separate from scheduling capabilities.
//!
//! Bump `PROTOCOL_VERSION` only for breaking changes. An additive command or
//! request field gets a feature string; clients must check for it before use.
//! A missing feature list means an older peer whose features are unknown.

pub const HOST_FEATURES: &[&str] = &[];
pub const CONTROLLER_FEATURES: &[&str] = &["controller.events", "controller.task-logs-wait"];
/// Added only after an existing-only live generation/hello proof.
pub const CONTROLLER_SOCKET: &str = "controller.socket";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registries_are_sorted_unique_and_contain_the_supported_features() {
        assert!(HOST_FEATURES.is_empty());
        assert_eq!(
            CONTROLLER_FEATURES,
            ["controller.events", "controller.task-logs-wait"]
        );
        for features in [HOST_FEATURES, CONTROLLER_FEATURES] {
            assert!(features.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(
                features
                    .iter()
                    .all(|feature| feature.split('.').count() >= 2
                        && feature.split('.').all(|part| !part.is_empty()
                            && part.bytes().all(|b| b.is_ascii_lowercase() || b == b'-')))
            );
        }
    }
}
