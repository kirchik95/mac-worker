//! Additive wire feature registry, separate from scheduling capabilities.
//!
//! Bump `PROTOCOL_VERSION` only for breaking changes. An additive command or
//! request field gets a feature string; clients must check for it before use.
//! A missing feature list means an older peer whose features are unknown.

pub const HOST_FEATURE_SESSION_IMPORT: &str = "task.session-import";
pub const CONTROLLER_FEATURE_SESSION_IMPORT: &str = "controller.session-import";
pub const HOST_FEATURE_INTEGRATION: &str = "task.integration";
pub const CONTROLLER_FEATURE_INTEGRATION: &str = "controller.integration";

pub const HOST_FEATURES: &[&str] = &[HOST_FEATURE_INTEGRATION, HOST_FEATURE_SESSION_IMPORT];
pub const CONTROLLER_FEATURES: &[&str] = &[
    "controller.events",
    CONTROLLER_FEATURE_INTEGRATION,
    CONTROLLER_FEATURE_SESSION_IMPORT,
    "controller.task-logs-wait",
];
/// Added only after an existing-only live generation/hello proof.
pub const CONTROLLER_SOCKET: &str = "controller.socket";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registries_are_sorted_unique_and_contain_the_supported_features() {
        assert!(HOST_FEATURES.contains(&HOST_FEATURE_INTEGRATION));
        assert!(CONTROLLER_FEATURES.contains(&CONTROLLER_FEATURE_INTEGRATION));
        assert_eq!(
            HOST_FEATURES,
            [HOST_FEATURE_INTEGRATION, HOST_FEATURE_SESSION_IMPORT]
        );
        assert_eq!(
            CONTROLLER_FEATURES,
            [
                "controller.events",
                CONTROLLER_FEATURE_INTEGRATION,
                CONTROLLER_FEATURE_SESSION_IMPORT,
                "controller.task-logs-wait"
            ]
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
