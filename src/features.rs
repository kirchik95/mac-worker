//! Additive wire feature registry, separate from scheduling capabilities.
//!
//! Bump `PROTOCOL_VERSION` only for breaking changes. An additive command or
//! request field gets a feature string; clients must check for it before use.
//! A missing feature list means an older peer whose features are unknown.

pub const HOST_FEATURES: &[&str] = &["host.outbox-retry", "host.status-logs"];
pub const CONTROLLER_FEATURES: &[&str] = &["controller.task-logs-wait"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registries_are_sorted_unique_and_contain_the_supported_features() {
        assert_eq!(HOST_FEATURES, ["host.outbox-retry", "host.status-logs"]);
        assert_eq!(CONTROLLER_FEATURES, ["controller.task-logs-wait"]);
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
