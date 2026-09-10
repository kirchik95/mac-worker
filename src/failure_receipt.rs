//! Fixed host-failure receipt vocabulary.
//!
//! Operators need to know which stage failed and which resources remain
//! without reading a payload or a redacted supervisor line. The values are
//! a closed set so the host message stays an opaque string that protocol-6
//! readers already accept, and a laptop that does not know the grammar still
//! sees `HOST_IO: host state operation failed …`.

/// Stages a host I/O failure may name. Never free text.
pub const STAGE_ADMISSION: &str = "admission";
pub const STAGE_PREPARE: &str = "prepare";
pub const STAGE_LAUNCH: &str = "launch";
pub const STAGE_DRAIN: &str = "drain";
pub const STAGE_PUBLISH: &str = "publish";
pub const STAGE_CLEANUP: &str = "cleanup";
pub const STAGE_LEASE_RELEASE: &str = "lease-release";
pub const STAGE_CANCEL: &str = "cancel";
pub const STAGE_FOLLOW: &str = "follow";

pub const STAGES: &[&str] = &[
    STAGE_ADMISSION,
    STAGE_PREPARE,
    STAGE_LAUNCH,
    STAGE_DRAIN,
    STAGE_PUBLISH,
    STAGE_CLEANUP,
    STAGE_LEASE_RELEASE,
    STAGE_CANCEL,
    STAGE_FOLLOW,
];

/// Resources a failed host operation may leave behind. Never free text.
pub const RESIDUAL_LEASE: &str = "lease";
pub const RESIDUAL_CLEANUP_TREE: &str = "cleanup-tree";
pub const RESIDUAL_JOB_DIR: &str = "job-dir";
pub const RESIDUAL_SUPERVISOR_LOCK: &str = "supervisor-lock";
pub const RESIDUAL_TRANSFER_LOCK: &str = "transfer-lock";
pub const RESIDUAL_SESSION: &str = "session";
pub const RESIDUAL_WORKSPACE: &str = "workspace";

pub const RESIDUALS: &[&str] = &[
    RESIDUAL_LEASE,
    RESIDUAL_CLEANUP_TREE,
    RESIDUAL_JOB_DIR,
    RESIDUAL_SUPERVISOR_LOCK,
    RESIDUAL_TRANSFER_LOCK,
    RESIDUAL_SESSION,
    RESIDUAL_WORKSPACE,
];

const HOST_IO_MESSAGE_PREFIX: &str = "host state operation failed";
const HOST_IO_CODE: &str = "HOST_IO";

/// Which stage failed and which vocabulary resources are still present.
///
/// Residuals are unique and stored in [`RESIDUALS`] order so the wire form
/// is stable across callers that observe the same leftover set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureReceipt {
    stage: &'static str,
    residual: Vec<&'static str>,
}

impl FailureReceipt {
    /// Builds a receipt from vocabulary constants. Anything outside the set is
    /// `None` so a caller can never put free text on the wire.
    pub fn new(stage: &str, residual: &[&str]) -> Option<Self> {
        let stage = intern_stage(stage)?;
        let residual = intern_residuals(residual)?;
        Some(Self { stage, residual })
    }

    pub fn stage(&self) -> &'static str {
        self.stage
    }

    pub fn residual(&self) -> &[&'static str] {
        &self.residual
    }

    /// Wire message body. Existing `HostControlError` readers treat this as
    /// an opaque string; protocol 6 does not change.
    pub fn host_message(&self) -> String {
        format!(
            "{HOST_IO_MESSAGE_PREFIX} [stage={} residual={}]",
            self.stage,
            self.residual.join(",")
        )
    }

    /// Operator-facing parenthetical: `(stage=cleanup, residual=lease,cleanup-tree)`.
    pub fn render_parenthetical(&self) -> String {
        format!(
            "(stage={}, residual={})",
            self.stage,
            self.residual.join(",")
        )
    }

    /// `HOST_IO (stage=cleanup, residual=lease,cleanup-tree)`.
    pub fn render_with_code(&self, code: &str) -> String {
        format!("{code} {}", self.render_parenthetical())
    }

    /// Parses the host message body. Unknown vocabulary is no receipt, never
    /// an error, so an old worker's free-form `HOST_IO` text stays opaque.
    pub fn parse_host_message(message: &str) -> Option<Self> {
        let rest = message.strip_prefix(HOST_IO_MESSAGE_PREFIX)?;
        let rest = rest.strip_prefix(" [stage=")?;
        let (stage, rest) = rest.split_once(" residual=")?;
        let residual = rest.strip_suffix(']')?;
        if residual.contains(' ') || residual.contains('[') {
            return None;
        }
        Self::new(stage, &split_residuals(residual)?)
    }

    /// Parses `HOST_IO: host state operation failed [stage=… residual=…]`.
    pub fn parse_protocol_message(message: &str) -> Option<Self> {
        let (code, detail) = message.split_once(": ")?;
        (code == HOST_IO_CODE)
            .then(|| Self::parse_host_message(detail))
            .flatten()
    }
}

fn intern_stage(stage: &str) -> Option<&'static str> {
    STAGES.iter().copied().find(|candidate| *candidate == stage)
}

fn intern_residual(residual: &str) -> Option<&'static str> {
    RESIDUALS
        .iter()
        .copied()
        .find(|candidate| *candidate == residual)
}

fn intern_residuals(residual: &[&str]) -> Option<Vec<&'static str>> {
    let mut interned = Vec::with_capacity(residual.len());
    for item in residual {
        let interned_item = intern_residual(item)?;
        if interned.contains(&interned_item) {
            return None;
        }
        interned.push(interned_item);
    }
    interned.sort_by_key(|item| residual_rank(item));
    Some(interned)
}

fn residual_rank(residual: &str) -> usize {
    RESIDUALS
        .iter()
        .position(|candidate| *candidate == residual)
        .unwrap_or(usize::MAX)
}

fn split_residuals(residual: &str) -> Option<Vec<&str>> {
    if residual.is_empty() {
        return Some(Vec::new());
    }
    Some(residual.split(',').collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_host_message_grammar_round_trips_the_vocabulary() {
        let receipt =
            FailureReceipt::new(STAGE_CLEANUP, &[RESIDUAL_LEASE, RESIDUAL_CLEANUP_TREE]).unwrap();
        assert_eq!(
            receipt.host_message(),
            "host state operation failed [stage=cleanup residual=lease,cleanup-tree]"
        );
        assert_eq!(
            FailureReceipt::parse_host_message(&receipt.host_message()).as_ref(),
            Some(&receipt)
        );
        assert_eq!(
            FailureReceipt::parse_protocol_message(&format!("HOST_IO: {}", receipt.host_message()))
                .as_ref(),
            Some(&receipt)
        );
        assert_eq!(
            receipt.render_with_code("HOST_IO"),
            "HOST_IO (stage=cleanup, residual=lease,cleanup-tree)"
        );
    }

    #[test]
    fn residuals_are_canonicalised_into_vocabulary_order() {
        let receipt =
            FailureReceipt::new(STAGE_FOLLOW, &[RESIDUAL_WORKSPACE, RESIDUAL_LEASE]).unwrap();
        assert_eq!(receipt.residual(), &[RESIDUAL_LEASE, RESIDUAL_WORKSPACE]);
        assert_eq!(
            FailureReceipt::parse_host_message(
                "host state operation failed [stage=follow residual=workspace,lease]"
            )
            .unwrap()
            .residual(),
            &[RESIDUAL_LEASE, RESIDUAL_WORKSPACE]
        );
    }

    #[test]
    fn anything_outside_the_vocabulary_is_no_receipt() {
        let cases = [
            "host state operation failed",
            "host state operation failed [stage=cleanup]",
            "host state operation failed [stage=cleanup residual=lease extra]",
            "host state operation failed [stage=unknown residual=lease]",
            "host state operation failed [stage=cleanup residual=fifo]",
            "host state operation failed [stage=cleanup residual=lease,lease]",
            "host state operation failed [stage=cleanup residual=lease, cleanup-tree]",
            "HOST_IO: host state operation failed [stage=cleanup residual=lease]",
            "other: host state operation failed [stage=cleanup residual=lease]",
        ];
        for message in cases {
            assert_eq!(
                FailureReceipt::parse_host_message(message),
                None,
                "{message}"
            );
        }
        assert_eq!(FailureReceipt::new("CLEANUP", &[RESIDUAL_LEASE]), None);
        assert_eq!(FailureReceipt::new(STAGE_CLEANUP, &["fifo"]), None);
        assert_eq!(
            FailureReceipt::new(STAGE_CLEANUP, &[RESIDUAL_LEASE, RESIDUAL_LEASE]),
            None
        );
        assert_eq!(
            FailureReceipt::parse_protocol_message(
                "HOST_IO: host state operation failed [stage=cleanup residual=fifo]"
            ),
            None
        );
        assert_eq!(
            FailureReceipt::parse_protocol_message("HOST_IO: host state operation failed"),
            None
        );
    }

    #[test]
    fn an_empty_residual_set_is_a_valid_receipt() {
        let receipt = FailureReceipt::new(STAGE_DRAIN, &[]).unwrap();
        assert_eq!(
            receipt.host_message(),
            "host state operation failed [stage=drain residual=]"
        );
        assert_eq!(
            FailureReceipt::parse_host_message(&receipt.host_message()).as_ref(),
            Some(&receipt)
        );
    }
}
