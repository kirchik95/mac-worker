use std::sync::Arc;

use crate::{
    agent_settings::{AgentDefaultSettings, AgentSettingsList, AgentSettingsSaveRequest},
    config::Config,
    dashboard::model::ApiError,
    error::WorkerError,
    process::ProcessRunner,
    transfer::SshJsonTransport,
};

pub trait DashboardSettingsSource: Send + Sync + 'static {
    fn worker_exists(&self, _worker_name: &str) -> bool {
        true
    }

    fn read(&self, worker_name: &str) -> Result<AgentSettingsList, ApiError>;
    fn save(
        &self,
        worker_name: &str,
        request: &AgentSettingsSaveRequest,
    ) -> Result<AgentDefaultSettings, ApiError>;
}

pub struct SystemDashboardSettingsSource {
    config: Arc<Config>,
    runner: Arc<dyn ProcessRunner>,
}

impl SystemDashboardSettingsSource {
    pub fn new(config: Arc<Config>, runner: Arc<dyn ProcessRunner>) -> Self {
        Self { config, runner }
    }
}

impl DashboardSettingsSource for SystemDashboardSettingsSource {
    fn worker_exists(&self, worker_name: &str) -> bool {
        self.config.worker(worker_name).is_some()
    }

    fn read(&self, worker_name: &str) -> Result<AgentSettingsList, ApiError> {
        let worker = self.worker(worker_name)?;
        SshJsonTransport::new(self.runner.as_ref())
            .agent_settings_get(worker)
            .map_err(map_worker_error)
    }

    fn save(
        &self,
        worker_name: &str,
        request: &AgentSettingsSaveRequest,
    ) -> Result<AgentDefaultSettings, ApiError> {
        let worker = self.worker(worker_name)?;
        SshJsonTransport::new(self.runner.as_ref())
            .agent_settings_set(worker, request)
            .map_err(map_worker_error)
    }
}

impl SystemDashboardSettingsSource {
    fn worker(&self, worker_name: &str) -> Result<&crate::config::WorkerEntry, ApiError> {
        self.config
            .worker(worker_name)
            .ok_or_else(|| ApiError::new("WORKER_NOT_FOUND", "configured worker was not found"))
    }
}

fn map_worker_error(error: WorkerError) -> ApiError {
    let code = error.public_code();
    let (code, message) = match code.as_str() {
        "SETTINGS_CONFLICT" => (
            "SETTINGS_CONFLICT",
            "native settings changed; refresh and retry",
        ),
        "SETTINGS_INVALID" | "SETTINGS_INVALID_CONFIG" => (
            "SETTINGS_INVALID",
            "native settings request or configuration is invalid",
        ),
        "SETTINGS_UNAVAILABLE"
        | "SSH_UNAVAILABLE"
        | "SSH_TIMEOUT"
        | "SSH_LAUNCH_FAILED"
        | "HOST_REQUEST_FAILED"
        | "INVALID_RESPONSE" => (
            "SETTINGS_UNAVAILABLE",
            "native settings are unavailable on this worker",
        ),
        _ => (
            "SETTINGS_UNAVAILABLE",
            "native settings are unavailable on this worker",
        ),
    };
    ApiError::new(code, message)
}
