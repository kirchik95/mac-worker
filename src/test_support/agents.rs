//! Explicit integration-test access for agents contracts.

pub mod agent {
    pub use crate::agent::{
        AUTH_SCAN_TAIL_BYTES, AdapterError, AgentAdapter, AgentEvent, AgentIdentity, AgentKind,
        AgentOutcome, AuthFailureScan, AuthFailureSignature, AuthProbeResult,
        MAX_DECLARED_ACCEPTANCE, OPENCODE_DIALECT_MISMATCH, OPENCODE_VERSION_UNVERIFIED,
        OpencodeDialect, PROMPT_POINTER, PermissionPolicy, PromptDelivery, Question,
        RESULT_SCHEMA_JSON, ReportedCheck, ReportedCheckStatus, ResultParseReason, ResultStatus,
        TurnLaunch, TurnLimits, TurnParams, adapter_for, adapter_for_host, adapter_for_launch,
        declared_acceptance_instructions, has_dialects, parse_prebind_session_ref,
        prebind_login_request, render_prebind_shell, render_shell, verify_opencode_launch,
    };
}
pub mod agent_facts {
    pub use crate::agent_facts::{
        AgentAuth, AgentAutoUpdate, AgentFacts, AgentProbe, EnvProfile,
        FACTS_REFRESH_BUDGET_REASON, FACTS_TTL, FactsClock, HerdrFactState, HerdrFacts,
        PROBE_DEADLINE, ProfileProbe, collect_agent_facts_at,
        collect_agent_facts_at_host_with_timing, collect_agent_facts_at_with_budget,
        collect_agent_facts_at_with_timing, turn_auth_failure_reason,
    };
}
pub mod agent_settings {
    pub use crate::agent_settings::{
        AgentDefaultSettings, AgentSettingsGetRequest, AgentSettingsList, AgentSettingsSaveRequest,
        ModelOption, NativeAgentSettingsStore, SETTINGS_AGENT_IDS,
    };
}
pub mod auth_incidents {
    pub use crate::auth_incidents::{
        AUTH_INCIDENT_REASON, AUTH_INCIDENT_TTL_MILLIS, AUTH_INCIDENTS_FILE,
        AUTH_INCIDENTS_UNREADABLE_REASON, clear_all, enable_after_load_hook_on_this_thread,
        record_incident, record_success, set_after_load_hook, set_before_lock_hook,
        set_before_publish_hook,
    };
}
pub mod doctor {
    pub use crate::doctor::{DoctorRequest, DoctorService};
}
pub mod herdr {
    pub use crate::herdr::{
        AgentState, DEFAULT_SOCKET_RELATIVE, HerdrClient, HerdrError, HerdrSocket,
        NotificationSound, PaneMetadata, SOCKET_ENV_NAME, SOURCE,
    };
}
pub mod herdr_reporter {
    pub use crate::herdr_reporter::{
        CLOSE_BUDGET, DISPLAY_AGENT, FOLLOW_TURN_COMMAND, HerdrClock, HerdrReporter,
        MAX_TABS_PER_PASS, SHELL_SETTLE, START_BUDGET, TurnIdentity, WORKSPACE_LABEL,
        short_task_id, task_label_prefix, terminal_report, title_line, turn_label,
    };
}
pub mod install {
    pub use crate::install::{Installer, prepare_candidate};
}
pub mod keychain {
    pub use crate::keychain::{KeychainUnlockConfig, unlock_keychain};
}
pub mod laptop {
    pub use crate::laptop::{
        BinaryIdentity, EmptyLaptopProcessTable, FixedBinaryIdentitySource,
        FixedLaptopProcessTable, LaptopProcess,
    };
}
pub mod probe {
    pub use crate::probe::ProbeCollector;
}
