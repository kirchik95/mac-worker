//! Renders an agent turn's recorded event log for a human reader.
//!
//! A turn's runner log interleaves the agent's structured stream-json events
//! with plain stderr text and pre-launch diagnostics. This module turns that
//! byte stream into the line-per-event text that `worker task logs` prints on
//! the laptop. The worker's herdr pane (`worker host follow-turn`) renders the
//! same log through the same function, so both surfaces show a turn
//! identically.

use std::io::Write;

use serde_json::Value;

use crate::{
    agent::{AgentEvent, AgentKind, adapter_for},
    error::WorkerError,
};

/// Longest `type` or `subtype` fragment printed in an `event:` summary.
///
/// The renderer also feeds the herdr pane, so a hostile identifier must not
/// be able to spill a prompt, path, or a multi-line payload into the log.
const MAX_EVENT_IDENT_CHARS: usize = 40;

/// Longest inventory worker name printed from a `turn_accepted` line.
const MAX_WORKER_NAME_CHARS: usize = 128;

/// Renders `bytes` of a turn's runner log as human-readable lines.
///
/// Every line the agent's adapter recognises is printed as a one-line summary
/// of its event. Lines that are not JSON at all (stderr, launch failures) pass
/// through verbatim so the reason an agent never started stays visible.
/// Structured lines the adapter does not recognise are folded into a compact
/// `event: <type>[/<subtype>] ×N` summary (`×N` omitted when N is 1) instead
/// of being dropped, except for keys the adapter lists as intentional noise
/// ([`AgentAdapter::quiet_event_keys`](crate::agent::AgentAdapter::quiet_event_keys)).
/// Only those two short identifiers are printed, never any payload.
///
/// Consecutive unrecognised lines with the same key fold into one rendered
/// line. The fold is flushed when the key changes, when a recognised event,
/// a mac-worker framing event, or a plain-text line is rendered, and at the
/// end of `bytes`. Each call is independent: `task logs -f` and `follow-turn`
/// render in chunks, so a run that straddles two polls may print twice. That
/// is acceptable; keeping fold state across polls would hide the latest key
/// until a later event arrived.
pub fn render_agent_log(
    bytes: &[u8],
    agent: AgentKind,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    let text = String::from_utf8_lossy(bytes);
    let adapter = adapter_for(agent);
    let mut fold = None;
    for line in text.lines() {
        if let Some(framing) = mac_worker_framing_line(line) {
            flush_unrecognised(&mut fold, stdout)?;
            if let FramingLine::Print(text) = framing {
                writeln!(stdout, "{text}")?;
            }
            continue;
        }
        if let Some(event) = adapter.parse_event(line) {
            flush_unrecognised(&mut fold, stdout)?;
            writeln!(stdout, "{}", render_event(event))?;
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            flush_unrecognised(&mut fold, stdout)?;
            writeln!(stdout, "{line}")?;
            continue;
        };
        let key = structured_event_key(&value);
        if adapter.quiet_event_keys().contains(&key.as_str()) {
            flush_unrecognised(&mut fold, stdout)?;
            continue;
        }
        if let Some(run) = fold.as_mut()
            && run.key == key
        {
            run.count += 1;
            continue;
        }
        flush_unrecognised(&mut fold, stdout)?;
        fold = Some(UnrecognisedFold { key, count: 1 });
    }
    flush_unrecognised(&mut fold, stdout)
}

/// A rendered mac-worker framing line, or silence for `turn_terminal`.
enum FramingLine {
    Print(String),
    Silent,
}

/// mac-worker's own protocol events, written by `src/turn_runner.rs`
/// `append_event` (do not change the writer). They are not agent events and
/// are recognised here before adapter parsing, independent of the agent kind.
///
/// `turn_accepted` prints `accepted by <worker>` from the inventory `worker`
/// field only, or `accepted` when that field is missing. `turn_terminal` is
/// omitted: the caller already prints the outcome line (`completed` /
/// `turn N failed`). Only the worker name may reach the pane — never task
/// ids, turn ids, paths, prompts, or payloads.
fn mac_worker_framing_line(line: &str) -> Option<FramingLine> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    match value.get("type").and_then(Value::as_str) {
        Some("turn_accepted") => Some(FramingLine::Print(render_turn_accepted(&value))),
        Some("turn_terminal") => Some(FramingLine::Silent),
        _ => None,
    }
}

fn render_turn_accepted(value: &Value) -> String {
    match value
        .get("worker")
        .and_then(Value::as_str)
        .and_then(inventory_worker_name)
    {
        Some(worker) => format!("accepted by {worker}"),
        None => "accepted".into(),
    }
}

/// Accepts only an inventory worker name. Anything else on the field is
/// payload and must not be printed.
fn inventory_worker_name(raw: &str) -> Option<&str> {
    if raw.is_empty() || raw.len() > MAX_WORKER_NAME_CHARS {
        return None;
    }
    raw.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@'))
        .then_some(raw)
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

/// One run of adjacent unrecognised structured lines that share a key.
struct UnrecognisedFold {
    key: String,
    count: usize,
}

fn flush_unrecognised(
    fold: &mut Option<UnrecognisedFold>,
    stdout: &mut dyn Write,
) -> Result<(), WorkerError> {
    let Some(run) = fold.take() else {
        return Ok(());
    };
    writeln!(stdout, "{}", render_unrecognised(&run.key, run.count))?;
    Ok(())
}

fn render_unrecognised(key: &str, count: usize) -> String {
    if count == 1 {
        format!("event: {key}")
    } else {
        format!("event: {key} ×{count}")
    }
}

/// Folding key for a structured line the adapter did not recognise.
///
/// Taken from JSON `type` and `subtype` only. Anything else on the object is
/// payload and must not reach the pane: prompts, paths, and tool arguments
/// live in those other fields.
fn structured_event_key(value: &Value) -> String {
    let Some(kind) = value
        .get("type")
        .and_then(Value::as_str)
        .and_then(sanitize_event_ident)
    else {
        return "unrecognised".into();
    };
    match value
        .get("subtype")
        .and_then(Value::as_str)
        .and_then(sanitize_event_ident)
    {
        Some(subtype) => format!("{kind}/{subtype}"),
        None => kind,
    }
}

fn sanitize_event_ident(raw: &str) -> Option<String> {
    let mut ident = String::new();
    for character in raw.chars() {
        if ident.len() >= MAX_EVENT_IDENT_CHARS {
            break;
        }
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
            ident.push(character);
        }
    }
    if ident.is_empty() { None } else { Some(ident) }
}
