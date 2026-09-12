use crate::agent_facts::{AgentFacts, FACTS_TTL, HerdrFactState, HerdrFacts};

/// Integrated release that transports typed `TaskStatus.reported_checks`.
/// v6 helpers and clients mismatch at preflight; upgrade them together.
pub const PROTOCOL_VERSION: u32 = 7;
pub const SUPERVISION_VERSION: u32 = 3;

/// Spec 5.3 and 12: the `doctor` and `setup` warning for a worker whose
/// operator asked for the reporter while its herdr is not `available`.
pub const HERDR_UNAVAILABLE_CODE: &str = "HERDR_UNAVAILABLE";
pub const HERDR_UNAVAILABLE_MESSAGE: &str =
    "herdr = true but the worker's herdr socket is not reachable; turns run without the reporter";
/// The same warning when the answer is simply not known: the herdr fact is
/// missing or older than its TTL, which a facts refresh settles.
pub const HERDR_FACTS_STALE_MESSAGE: &str = "herdr = true but the worker's herdr facts are stale or missing; run `worker workers --refresh`";
/// Setup installed the helper but could not collect agent facts; the operator
/// can retry collection without reinstalling.
pub const FACTS_REFRESH_FAILED_MESSAGE: &str =
    "agent facts were not refreshed during setup; run `worker workers --refresh`";
/// `doctor` warning when inventory declares `origin:<host>` but the worker
/// account has no HTTPS credential helper for that host. Never a blocker.
pub const ORIGIN_HELPER_MISSING_CODE: &str = "ORIGIN_HELPER_MISSING";
/// `doctor` / `setup` warning when a long-running laptop CLI was started
/// from a binary older than the one now installed. Never a blocker.
pub const LAPTOP_BINARY_OUTDATED_CODE: &str = "LAPTOP_BINARY_OUTDATED";

/// CPU counters on a host probe. Laptop readers ignore unknown keys so an
/// additive host field cannot take the dashboard offline.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CpuCounters {
    pub user_ticks: u64,
    pub system_ticks: u64,
    pub idle_ticks: u64,
    pub nice_ticks: u64,
}

/// Host-produced probe JSON.
///
/// A host may add fields. Laptop readers ignore unknown keys so a
/// still-running older dashboard survives an additive helper. Known fields
/// keep their existing validation after decode. Protocol stays 7.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
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
    #[serde(default)]
    pub available_memory_bytes: Option<u64>,
    #[serde(default)]
    pub cpu_counters: Option<CpuCounters>,
    pub slot_state: crate::lease::SlotState,
    pub active_lease: Option<crate::lease::LeaseSummary>,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub agent_facts: Option<AgentFacts>,
    #[serde(default)]
    pub facts_age_millis: Option<u64>,
    #[serde(default)]
    pub configured_slots: u8,
    #[serde(default)]
    pub busy_slots: u8,
}

impl ProbeResponse {
    /// The herdr fact when it can be trusted for capabilities: facts present,
    /// an age reported within the TTL, and a record that carries the fact.
    /// Otherwise `None`. Display (`worker workers`, the dashboard chip) may
    /// still show a stale fact with its age; scheduling must not.
    pub fn herdr_fact(&self) -> Option<&HerdrFacts> {
        let facts = self.agent_facts.as_ref()?;
        let age = self.facts_age_millis?;
        if age > FACTS_TTL {
            return None;
        }
        facts.herdr.as_ref()
    }

    /// Whether the worker's herdr answered `ping` at the last fresh refresh.
    pub fn herdr_available(&self) -> bool {
        self.herdr_fact()
            .is_some_and(|herdr| herdr.state == HerdrFactState::Available)
    }

    pub fn configured_slot_count(&self) -> u8 {
        if self.configured_slots == 0 {
            1
        } else {
            self.configured_slots
        }
    }

    pub fn busy_slot_count(&self) -> u8 {
        if self.configured_slots == 0 {
            match self.slot_state {
                crate::lease::SlotState::Busy => 1,
                crate::lease::SlotState::Idle => 0,
            }
        } else {
            self.busy_slots
        }
    }

    pub fn has_free_execution_slot(&self) -> bool {
        self.busy_slot_count() < self.configured_slot_count()
    }
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
    /// `herdr = true` but the verification probe's herdr fact is not
    /// `available`; the host is still installed.
    HerdrUnavailable,
    /// First-launch warm-up of the promoted helper failed or timed out;
    /// verification still decided the outcome.
    WarmupFailed,
    /// Post-promotion `host refresh-facts` failed or timed out; the helper
    /// stayed installed and verification still decided the outcome.
    FactsRefreshFailed,
    /// A long-running laptop `dashboard` or `controller run` was started
    /// from a binary older than the one just installed.
    LaptopBinaryOutdated,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetupReport {
    pub protocol_version: u32,
    pub workers: Vec<SetupHostResult>,
    /// Laptop-local warnings (outdated dashboard/controller). Omitted when
    /// empty so older `--json` readers keep their existing shape.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<SetupWarning>,
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
        CpuCounters, HealthStatus, MemoryPressure, PROTOCOL_VERSION, ProbeResponse,
        SUPERVISION_VERSION, SetupHostResult, SetupReport, WorkerHealth, WorkersReport,
        missing_capabilities,
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
                available_memory_bytes: Some(512 * 1024 * 1024),
                cpu_counters: Some(CpuCounters {
                    user_ticks: 1,
                    system_ticks: 2,
                    idle_ticks: 3,
                    nice_ticks: 4,
                }),
                slot_state: crate::lease::SlotState::Idle,
                active_lease: None,
                capabilities: vec!["darwin-arm64".into(), "git".into()],
                agent_facts: None,
                facts_age_millis: None,
                configured_slots: 0,
                busy_slots: 0,
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
        assert_eq!(value["available_memory_bytes"], 512 * 1024 * 1024);
        assert_eq!(value["cpu_counters"]["idle_ticks"], 3);
    }

    #[test]
    fn laptop_probe_response_ignores_unknown_fields_and_keeps_known_validation() {
        let mut value = serde_json::to_value(ProbeResponse::fixture()).unwrap();
        value["origin_https_helpers"] = serde_json::json!({"generic": true});
        value["cpu_counters"]["extra_counter"] = serde_json::json!(9);
        let decoded: ProbeResponse = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.hostname, "mini-1.local");
        assert_eq!(decoded.cpu_counters.as_ref().unwrap().idle_ticks, 3);

        let invalid = serde_json::json!({
            "protocol_version": "not-a-number",
            "supervision_version": 3,
            "hostname": "mini-1.local",
            "arch": "arm64",
            "os_version": "26.2",
            "free_disk_bytes": 1,
            "total_disk_bytes": 2,
            "memory_pressure": "normal",
            "swap_used_bytes": 0,
            "slot_state": "idle",
            "active_lease": null,
            "capabilities": [],
            "unexpected": true,
        });
        assert!(serde_json::from_value::<ProbeResponse>(invalid).is_err());
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
            warnings: Vec::new(),
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
