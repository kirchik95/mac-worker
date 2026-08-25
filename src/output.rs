#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommandOutput {
    Setup(crate::protocol::SetupReport),
    Workers(crate::protocol::WorkersReport),
    Probe(crate::protocol::ProbeResponse),
}

impl CommandOutput {
    pub fn render_json(&self) -> Result<String, crate::error::WorkerError> {
        serde_json::to_string(self).map_err(|error| {
            crate::error::WorkerError::Protocol(format!(
                "failed to serialize command output: {error}"
            ))
        })
    }

    pub fn render_human(&self) -> String {
        match self {
            Self::Setup(report) => report
                .workers
                .iter()
                .map(|worker| {
                    if worker.installed {
                        match worker.protocol_version {
                            Some(version) => {
                                format!("{}: installed (protocol {version})", worker.name)
                            }
                            None => format!("{}: installed", worker.name),
                        }
                    } else {
                        let code = worker.error_code.as_deref().unwrap_or("UNKNOWN");
                        let message = worker.error_message.as_deref().unwrap_or("setup failed");
                        format!("{}: failed [{code}]: {message}", worker.name)
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Workers(report) => report
                .workers
                .iter()
                .map(|worker| match &worker.probe {
                    Some(probe) if worker.status == crate::protocol::HealthStatus::Ready => {
                        format!(
                            "{}: ready ({}; {}; macOS {}; protocol {})",
                            worker.name,
                            probe.hostname,
                            probe.arch,
                            probe.os_version,
                            probe.protocol_version
                        )
                    }
                    _ => {
                        let code = worker.error_code.as_deref().unwrap_or("UNAVAILABLE");
                        let message = worker
                            .error_message
                            .as_deref()
                            .unwrap_or("worker is unavailable");
                        format!("{}: unavailable [{code}]: {message}", worker.name)
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Probe(probe) => format!(
                "{}: {} (macOS {}; protocol {})",
                probe.hostname, probe.arch, probe.os_version, probe.protocol_version
            ),
        }
    }
}
