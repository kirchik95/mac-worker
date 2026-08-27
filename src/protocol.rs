pub const PROTOCOL_VERSION: u32 = 2;
pub const SUPERVISION_VERSION: u32 = 2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProbeResponse {
    pub protocol_version: u32,
    pub supervision_version: u32,
    pub hostname: String,
    pub arch: String,
    pub os_version: String,
    pub free_disk_bytes: u64,
    pub total_disk_bytes: u64,
    pub memory_pressure: MemoryPressure,
    pub swap_used_bytes: Option<u64>,
    pub slot_state: crate::lease::SlotState,
    pub active_lease: Option<crate::lease::LeaseSummary>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPressure {
    Normal,
    Warn,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerHealth {
    pub name: String,
    pub ssh: String,
    pub status: HealthStatus,
    pub probe: Option<ProbeResponse>,
    pub missing_capabilities: Vec<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkersReport {
    pub protocol_version: u32,
    pub workers: Vec<WorkerHealth>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DoctorReport {
    pub version: u32,
    pub ready: bool,
    pub project: DoctorProject,
    pub requirements: Vec<String>,
    pub snapshot: Option<crate::snapshot::SnapshotSummary>,
    pub workers: Vec<WorkerHealth>,
    pub issues: Vec<DoctorIssue>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct DoctorProject {
    pub display_name: String,
    pub project_id: String,
    pub worktree_id: String,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
    pub relative_working_dir: String,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub struct DoctorIssue {
    pub severity: IssueSeverity,
    pub code: String,
    pub message: String,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum IssueSeverity {
    Blocker,
    Warning,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetupHostResult {
    pub name: String,
    pub ssh: String,
    pub installed: bool,
    pub protocol_version: Option<u32>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    #[serde(skip)]
    pub failure_kind: Option<SetupFailureKind>,
    #[serde(default)]
    pub warnings: Vec<SetupWarning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupFailureKind {
    Unavailable,
    Infrastructure,
    Io,
}

impl SetupFailureKind {
    pub fn exit_kind(self) -> crate::error::ExitKind {
        match self {
            Self::Unavailable => crate::error::ExitKind::Unavailable,
            Self::Infrastructure => crate::error::ExitKind::Infrastructure,
            Self::Io => crate::error::ExitKind::Io,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SetupWarning {
    pub code: SetupWarningCode,
    pub message: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SetupWarningCode {
    CleanupFailed,
    RollbackFailed,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetupReport {
    pub protocol_version: u32,
    pub workers: Vec<SetupHostResult>,
}

pub fn missing_capabilities(required: &[String], probe: &ProbeResponse) -> Vec<String> {
    required
        .iter()
        .filter(|capability| !probe.capabilities.contains(capability))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse, SUPERVISION_VERSION,
        SetupHostResult, SetupReport, WorkerHealth, WorkersReport, missing_capabilities,
    };

    impl ProbeResponse {
        fn fixture() -> Self {
            Self {
                protocol_version: PROTOCOL_VERSION,
                supervision_version: SUPERVISION_VERSION,
                hostname: "mini-1.local".into(),
                arch: "arm64".into(),
                os_version: "26.2".into(),
                free_disk_bytes: 512 * 1024 * 1024,
                total_disk_bytes: 1024 * 1024 * 1024,
                memory_pressure: MemoryPressure::Normal,
                swap_used_bytes: Some(128 * 1024 * 1024),
                slot_state: crate::lease::SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into(), "git".into()],
            }
        }

        fn fixture_with_capabilities<const N: usize>(capabilities: [&str; N]) -> Self {
            Self {
                capabilities: capabilities.into_iter().map(String::from).collect(),
                ..Self::fixture()
            }
        }
    }

    #[test]
    fn probe_response_has_an_explicit_protocol_version() {
        let response = ProbeResponse::fixture();
        let value = serde_json::to_value(response).unwrap();

        assert_eq!(value["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(value["supervision_version"], SUPERVISION_VERSION);
        assert_eq!(value["arch"], "arm64");
    }

    #[test]
    fn declared_capability_must_also_be_detected() {
        let response = ProbeResponse::fixture_with_capabilities(["darwin-arm64"]);
        let missing = missing_capabilities(&["darwin-arm64".into(), "docker".into()], &response);

        assert_eq!(missing, vec!["docker"]);
    }

    #[test]
    fn worker_health_uses_the_stable_wire_shape() {
        let health = WorkerHealth {
            name: "mini-1".into(),
            ssh: "mac1".into(),
            status: HealthStatus::Ready,
            probe: Some(ProbeResponse::fixture()),
            missing_capabilities: Vec::new(),
            error_code: None,
            error_message: None,
        };

        let value = serde_json::to_value(health).unwrap();

        assert_eq!(value["name"], "mini-1");
        assert_eq!(value["ssh"], "mac1");
        assert_eq!(value["status"], "ready");
        assert_eq!(value["probe"]["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(value["missing_capabilities"], serde_json::json!([]));
        assert!(value["error_code"].is_null());
        assert!(value["error_message"].is_null());
    }

    #[test]
    fn reports_keep_the_protocol_version_at_the_boundary() {
        let workers = WorkersReport {
            protocol_version: PROTOCOL_VERSION,
            workers: vec![WorkerHealth {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                status: HealthStatus::Unavailable,
                probe: None,
                missing_capabilities: vec!["docker".into()],
                error_code: Some("unavailable".into()),
                error_message: Some("probe failed".into()),
            }],
        };
        let setup = SetupReport {
            protocol_version: PROTOCOL_VERSION,
            workers: vec![SetupHostResult {
                name: "mini-1".into(),
                ssh: "mac1".into(),
                installed: true,
                protocol_version: Some(PROTOCOL_VERSION),
                error_code: None,
                error_message: None,
                failure_kind: None,
                warnings: Vec::new(),
            }],
        };

        let workers_json = serde_json::to_value(workers).unwrap();
        let setup_json = serde_json::to_value(setup).unwrap();

        assert_eq!(workers_json["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(workers_json["workers"][0]["status"], "unavailable");
        assert_eq!(setup_json["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(setup_json["workers"][0]["installed"], true);
        assert_eq!(
            setup_json["workers"][0]["protocol_version"],
            PROTOCOL_VERSION
        );
    }
}
