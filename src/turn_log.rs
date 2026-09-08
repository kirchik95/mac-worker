//! Renders an agent turn's recorded event log for a human reader.
//!
//! A turn's runner log interleaves the agent's structured stream-json events
//! with plain stderr text and pre-launch diagnostics. This module turns that
//! byte stream into the line-per-event text that `worker task logs` prints on
//! the laptop. The worker's herdr pane (`worker host follow-turn`) renders the
//! same log through the same function, so both surfaces show a turn
//! identically.

use std::io::Write;

use crate::{
    agent::{AgentEvent, AgentKind, adapter_for},
    error::WorkerError,
};

/// Renders `bytes` of a turn's runner log as human-readable lines.
///
/// Every line the agent's adapter recognises is printed as a one-line summary
/// of its event. Lines that are not JSON at all (stderr, launch failures) pass
/// through verbatim so the reason an agent never started stays visible.
/// Structured lines the adapter does not recognise are dropped.
pub fn render_agent_log(
    bytes: &[u8],
    agent: AgentKind,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    let text = String::from_utf8_lossy(bytes);
    let adapter = adapter_for(agent);
    for line in text.lines() {
        if let Some(event) = adapter.parse_event(line) {
            writeln!(stdout, "{}", render_event(event))?;
        } else if serde_json::from_str::<serde_json::Value>(line).is_err() {
            // Runner logs also contain plain stderr and pre-launch failures.
            // Keep unrecognized structured events hidden, but do not discard
            // the diagnostics needed to explain why an agent did not start.
            writeln!(stdout, "{line}")?;
        }
    }
    Ok(())
}

fn render_event(event: AgentEvent) -> String {
    match event {
        AgentEvent::AssistantMessage { text } => text,
        AgentEvent::ToolCall { name, summary } => format!("{name}: {summary}"),
        AgentEvent::FileChange { paths } => paths.join(", "),
        AgentEvent::Command { summary, exit_code } => format!("{summary} ({exit_code:?})"),
        AgentEvent::Usage { .. } => "usage".into(),
        AgentEvent::SessionStarted { session_ref } => format!("session {session_ref}"),
        AgentEvent::TurnEnd { reason } => reason,
    }
}
