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
                    let mut rendered = if worker.installed {
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
                    };
                    for warning in &worker.warnings {
                        rendered.push_str("\n  warning [");
                        rendered.push_str(setup_warning_code(&warning.code));
                        rendered.push_str("]: ");
                        rendered.push_str(&warning.message);
                    }
                    rendered
                })
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Workers(report) => report
                .workers
                .iter()
                .map(render_worker_health)
                .collect::<Vec<_>>()
                .join("\n"),
            Self::Probe(probe) => format!(
                "{}: {} (macOS {}; protocol {})",
                probe.hostname, probe.arch, probe.os_version, probe.protocol_version
            ),
        }
    }

    pub fn write_to(
        &self,
        writer: &mut dyn std::io::Write,
        json: bool,
        raw_probe: bool,
    ) -> Result<(), crate::error::WorkerError> {
        let rendered = if raw_probe {
            let Self::Probe(probe) = self else {
                return Err(crate::error::WorkerError::Protocol(
                    "host probe returned the wrong output type".into(),
                ));
            };
            serde_json::to_string(probe).map_err(|error| {
                crate::error::WorkerError::Protocol(format!(
                    "failed to serialize host probe: {error}"
                ))
            })?
        } else if json {
            self.render_json()?
        } else {
            self.render_human()
        };

        writer.write_all(rendered.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    pub fn aggregate_exit_kind(&self) -> Option<crate::error::ExitKind> {
        match self {
            Self::Setup(report) if report.workers.iter().any(|worker| !worker.installed) => {
                Some(crate::error::ExitKind::Unavailable)
            }
            _ => None,
        }
    }
}

fn render_worker_health(worker: &crate::protocol::WorkerHealth) -> String {
    let mut lines = vec![match &worker.probe {
        Some(probe) if worker.status == crate::protocol::HealthStatus::Ready => format!(
            "{}: ready ({}; {}; macOS {}; protocol {})",
            worker.name, probe.hostname, probe.arch, probe.os_version, probe.protocol_version
        ),
        _ => {
            let code = worker.error_code.as_deref().unwrap_or("UNAVAILABLE");
            let message = worker
                .error_message
                .as_deref()
                .unwrap_or("worker is unavailable");
            format!("{}: unavailable [{code}]: {message}", worker.name)
        }
    }];

    if !worker.missing_capabilities.is_empty() {
        lines.push(format!(
            "  missing capabilities: {}",
            worker.missing_capabilities.join(", ")
        ));
    }
    if let Some(probe) = &worker.probe {
        let capabilities = if probe.capabilities.is_empty() {
            "none".into()
        } else {
            probe.capabilities.join(", ")
        };
        lines.push(format!("  capabilities: {capabilities}"));
        lines.push(format!("  free disk bytes: {}", probe.free_disk_bytes));
        lines.push(format!(
            "  memory pressure: {}",
            memory_pressure_name(&probe.memory_pressure)
        ));
        lines.push(format!(
            "  swap used bytes: {}",
            probe
                .swap_used_bytes
                .map_or_else(|| "unavailable".into(), |bytes| bytes.to_string())
        ));
    }

    lines.join("\n")
}

fn memory_pressure_name(pressure: &crate::protocol::MemoryPressure) -> &'static str {
    match pressure {
        crate::protocol::MemoryPressure::Normal => "normal",
        crate::protocol::MemoryPressure::Warn => "warn",
        crate::protocol::MemoryPressure::Critical => "critical",
        crate::protocol::MemoryPressure::Unknown => "unknown",
    }
}

fn setup_warning_code(code: &crate::protocol::SetupWarningCode) -> &'static str {
    match code {
        crate::protocol::SetupWarningCode::CleanupFailed => "CLEANUP_FAILED",
        crate::protocol::SetupWarningCode::RollbackFailed => "ROLLBACK_FAILED",
    }
}
