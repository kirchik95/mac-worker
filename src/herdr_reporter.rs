//! Mirrors a pool turn into the herdr running on the worker.
//!
//! Herdr, the terminal workspace manager on every Mac mini, shows agents it
//! can see in a sidebar that the operator's herdr on the MacBook aggregates
//! across machines.  A headless agent turn is invisible to it, so the worker
//! helper reports the turn itself, through herdr's socket, as an external
//! agent source: one workspace named `mac-worker`, one tab per turn whose
//! pane runs `worker host follow-turn` to render the log, and the turn's
//! lifecycle state on that pane.
//!
//! The reporter keeps no state of its own.  The workspace is found by its
//! label and a task's tabs by their label prefix, so a crashed supervisor,
//! a restarted herdr, or a tab the operator closed leave nothing to
//! reconcile.  Every entry point is best effort under a budget: it returns
//! what it managed to do and a short, path-free diagnostic, and it can never
//! change a turn's outcome, exit code, lease, or records.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

use crate::{
    agent::{AgentKind, Question},
    herdr::{AgentState, HerdrClient, HerdrError, HerdrSocket, PaneMetadata},
    task::{HerdrTurnReport, HerdrTurnState, TaskId, TaskOutcome},
};

/// Label of the one workspace mac-worker uses on a worker's herdr.
pub const WORKSPACE_LABEL: &str = "mac-worker";
/// What herdr shows as the agent's origin.
pub const DISPLAY_AGENT: &str = "mac-worker";
/// The helper as installed on every worker, tilde-relative so no path is
/// typed into the pane.
pub const FOLLOW_TURN_COMMAND: &str = "exec ~/.local/bin/worker host follow-turn";

pub const START_BUDGET: Duration = Duration::from_secs(10);
pub const TERMINAL_BUDGET: Duration = Duration::from_secs(5);
pub const CLOSE_BUDGET: Duration = Duration::from_secs(5);
/// How long a fresh pane gets to reach its shell prompt before the log
/// follower is typed into it.
pub const SHELL_SETTLE: Duration = Duration::from_secs(5);
const SHELL_POLL: Duration = Duration::from_millis(200);
/// Longest message handed to herdr; summaries and questions are already
/// redacted and bounded, this keeps the sidebar sane.
const MESSAGE_LIMIT: usize = 1024;

/// Everything the reporter needs to name a turn; identifiers only.
#[derive(Debug, Clone)]
pub struct TurnIdentity {
    pub project_id: String,
    pub worktree_id: String,
    pub job_id: String,
    pub task_id: TaskId,
    pub turn_number: u32,
    pub agent: AgentKind,
    pub title: String,
}

/// What one reporter call achieved, for the turn record and the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reported {
    pub report: HerdrTurnReport,
    /// One short line for `supervisor.log` when something did not work.
    pub diagnostic: Option<String>,
}

impl Reported {
    fn attached(pane_id: String) -> Self {
        Self {
            report: HerdrTurnReport {
                state: HerdrTurnState::Attached,
                pane_id: Some(pane_id),
            },
            diagnostic: None,
        }
    }

    fn unavailable(error: &HerdrError) -> Self {
        Self {
            report: HerdrTurnReport {
                state: HerdrTurnState::Unavailable,
                pane_id: None,
            },
            diagnostic: Some(format!("herdr reporter: unavailable ({})", error.kind())),
        }
    }
}

static START_REPORTS: Mutex<BTreeMap<String, HerdrTurnReport>> = Mutex::new(BTreeMap::new());

/// Keep what `start` achieved for a turn until its terminal report, in this
/// process only.  The supervisor that started a turn is the one that ends it
/// on every ordinary path; cancellation and reconciliation run elsewhere,
/// find nothing here, and open the tab again.  Nothing is written to disk
/// mid-turn, because the task status is being written by the output pump at
/// exactly that time.
pub fn remember_start(job_id: &str, report: HerdrTurnReport) {
    if let Ok(mut reports) = START_REPORTS.lock() {
        reports.insert(job_id.to_owned(), report);
    }
}

/// Take the `start` result for a turn, if this process produced it.
pub fn take_start(job_id: &str) -> Option<HerdrTurnReport> {
    START_REPORTS
        .lock()
        .ok()
        .and_then(|mut reports| reports.remove(job_id))
}

/// The first twelve hex digits of a task id, the form the sidebar shows.
pub fn short_task_id(task_id: TaskId) -> String {
    let full = task_id.to_string();
    full[..12.min(full.len())].to_owned()
}

/// Label prefix shared by every tab of one task.
pub fn task_label_prefix(task_id: TaskId) -> String {
    format!("task {}", short_task_id(task_id))
}

/// Label of the tab that shows one turn.
pub fn turn_label(task_id: TaskId, turn_number: u32) -> String {
    format!("{} · turn {turn_number}", task_label_prefix(task_id))
}

/// The pane title, which is what the sidebar row reads.
pub fn title_line(task_id: TaskId, title: &str) -> String {
    format!("task {} · {title}", short_task_id(task_id))
}

/// Herdr's kind name for an adapter, so the sidebar shows the right icon.
pub fn herdr_agent_kind(agent: AgentKind) -> &'static str {
    match agent {
        AgentKind::Codex => "codex",
        AgentKind::Claude => "claude",
        AgentKind::Cursor => "cursor",
        AgentKind::Opencode => "opencode",
    }
}

/// The herdr state, sidebar outcome token, and message for a finished turn.
pub fn terminal_report(
    outcome: &TaskOutcome,
    summary: Option<&str>,
    questions: &[Question],
) -> (AgentState, &'static str, String) {
    let first_question = questions.first().map(|question| question.text().to_owned());
    let summary = summary.map(str::to_owned);
    match outcome {
        TaskOutcome::Done => (AgentState::Idle, "done", summary.unwrap_or_default()),
        TaskOutcome::NeedsInput => (
            AgentState::Blocked,
            "needs_input",
            first_question.or(summary).unwrap_or_default(),
        ),
        TaskOutcome::Blocked => (
            AgentState::Blocked,
            "blocked",
            summary.or(first_question).unwrap_or_default(),
        ),
        TaskOutcome::Unknown => (
            AgentState::Unknown,
            "unknown",
            "agent reported no structured result".into(),
        ),
        TaskOutcome::Failed { reason } => (AgentState::Unknown, "failed", reason.clone()),
        TaskOutcome::Cancelled => (AgentState::Unknown, "cancelled", "cancelled".into()),
        TaskOutcome::TimedOut => (AgentState::Unknown, "timed_out", "timed out".into()),
        TaskOutcome::Lost => (AgentState::Unknown, "lost", "lost".into()),
    }
}

fn bounded_message(message: &str) -> Option<String> {
    let trimmed: String = message
        .chars()
        .filter(|character| !character.is_control() || *character == ' ')
        .collect();
    let trimmed = trimmed.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut end = MESSAGE_LIMIT.min(trimmed.len());
    while !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    Some(trimmed[..end].to_owned())
}

fn state_labels(turn_number: u32, outcome: Option<&str>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("working".into(), format!("turn {turn_number}"));
    labels.insert("blocked".into(), "needs input".into());
    labels.insert("idle".into(), "done".into());
    labels.insert("unknown".into(), outcome.unwrap_or("unknown").to_owned());
    labels
}

fn tokens(turn: &TurnIdentity, outcome: &str) -> BTreeMap<String, String> {
    let mut tokens = BTreeMap::new();
    tokens.insert("task".into(), short_task_id(turn.task_id));
    tokens.insert("turn".into(), turn.turn_number.to_string());
    tokens.insert("mw_title".into(), title_line(turn.task_id, &turn.title));
    tokens.insert("mw_agent".into(), herdr_agent_kind(turn.agent).to_owned());
    tokens.insert("mw_outcome".into(), outcome.to_owned());
    tokens
}

struct Budget {
    deadline: Instant,
}

impl Budget {
    fn new(total: Duration) -> Self {
        Self {
            deadline: Instant::now() + total,
        }
    }

    fn exhausted(&self) -> bool {
        Instant::now() >= self.deadline
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// The reporter for one worker account's herdr.
#[derive(Debug, Clone)]
pub struct HerdrReporter {
    client: HerdrClient,
}

impl HerdrReporter {
    /// Report to the default herdr session of the account whose home this is.
    pub fn for_home(home: &Path) -> Self {
        Self::with_client(HerdrClient::new(HerdrSocket::default_for_home(home)))
    }

    pub fn with_client(client: HerdrClient) -> Self {
        Self { client }
    }

    /// Open the turn's tab, arm its pane with the log follower, and report
    /// `working`.  Runs after the durable `Running` handoff, never under the
    /// supervisor guard.
    pub fn start(&self, turn: &TurnIdentity) -> Reported {
        let budget = Budget::new(START_BUDGET);
        match self.start_inner(turn, &budget) {
            Ok(pane_id) => Reported::attached(pane_id),
            Err(error) => Reported::unavailable(&error),
        }
    }

    fn start_inner(&self, turn: &TurnIdentity, budget: &Budget) -> Result<String, HerdrError> {
        let workspace_id = self.ensure_workspace()?;
        self.sweep_task(&workspace_id, turn.task_id)?;
        let pane_id = self.open_turn_tab(&workspace_id, turn, budget)?;
        self.report(
            &pane_id,
            turn,
            AgentState::Working,
            "running",
            Some(&turn.title),
        )?;
        Ok(pane_id)
    }

    /// Report the finished turn.  `pane_id` is what `start` recorded; when
    /// it is gone the tab is opened again so a finished state is never lost.
    pub fn terminal(
        &self,
        turn: &TurnIdentity,
        pane_id: Option<&str>,
        outcome: &TaskOutcome,
        summary: Option<&str>,
        questions: &[Question],
    ) -> Reported {
        let budget = Budget::new(TERMINAL_BUDGET);
        match self.terminal_inner(turn, pane_id, outcome, summary, questions, &budget) {
            Ok(pane_id) => Reported::attached(pane_id),
            Err(error) => Reported::unavailable(&error),
        }
    }

    fn terminal_inner(
        &self,
        turn: &TurnIdentity,
        pane_id: Option<&str>,
        outcome: &TaskOutcome,
        summary: Option<&str>,
        questions: &[Question],
        budget: &Budget,
    ) -> Result<String, HerdrError> {
        let pane_id = match pane_id {
            Some(pane_id) if self.client.pane_process_info(pane_id).is_ok() => pane_id.to_owned(),
            _ => {
                let workspace_id = self.ensure_workspace()?;
                self.sweep_task(&workspace_id, turn.task_id)?;
                self.open_turn_tab(&workspace_id, turn, budget)?
            }
        };
        let (state, outcome_kind, message) = terminal_report(outcome, summary, questions);
        self.report(&pane_id, turn, state, outcome_kind, Some(&message))?;
        Ok(pane_id)
    }

    /// Remove every tab of a task; the agent goes with its pane.
    pub fn close(&self, task_id: TaskId) -> Result<(), HerdrError> {
        let _budget = Budget::new(CLOSE_BUDGET);
        let Some(workspace_id) = self.find_workspace()? else {
            return Ok(());
        };
        self.sweep_task(&workspace_id, task_id)
    }

    /// Remove tabs whose task no longer exists or is no longer open; `live`
    /// answers for the twelve-digit task id a label carries.
    pub fn sweep_orphans(&self, live: &dyn Fn(&str) -> bool) -> Result<usize, HerdrError> {
        let _budget = Budget::new(CLOSE_BUDGET);
        let Some(workspace_id) = self.find_workspace()? else {
            return Ok(0);
        };
        let mut closed = 0;
        for tab in self.client.tab_list(&workspace_id)? {
            let Some(short) = tab
                .label
                .as_deref()
                .and_then(|label| label.strip_prefix("task "))
                .and_then(|rest| rest.split(' ').next())
            else {
                continue;
            };
            if short.len() == 12
                && short.bytes().all(|byte| byte.is_ascii_hexdigit())
                && !live(short)
            {
                self.client.tab_close(&tab.tab_id)?;
                closed += 1;
            }
        }
        Ok(closed)
    }

    fn find_workspace(&self) -> Result<Option<String>, HerdrError> {
        Ok(self
            .client
            .workspace_list()?
            .into_iter()
            .find(|workspace| workspace.label.as_deref() == Some(WORKSPACE_LABEL))
            .map(|workspace| workspace.workspace_id))
    }

    fn ensure_workspace(&self) -> Result<String, HerdrError> {
        if let Some(workspace_id) = self.find_workspace()? {
            return Ok(workspace_id);
        }
        Ok(self
            .client
            .workspace_create(WORKSPACE_LABEL, None)?
            .workspace_id)
    }

    fn sweep_task(&self, workspace_id: &str, task_id: TaskId) -> Result<(), HerdrError> {
        let prefix = task_label_prefix(task_id);
        for tab in self.client.tab_list(workspace_id)? {
            if tab
                .label
                .as_deref()
                .is_some_and(|label| label.starts_with(&prefix))
            {
                self.client.tab_close(&tab.tab_id)?;
            }
        }
        Ok(())
    }

    fn open_turn_tab(
        &self,
        workspace_id: &str,
        turn: &TurnIdentity,
        budget: &Budget,
    ) -> Result<String, HerdrError> {
        let created = self.client.tab_create(
            workspace_id,
            &turn_label(turn.task_id, turn.turn_number),
            None,
        )?;
        self.arm_pane(&created.pane_id, turn, budget);
        Ok(created.pane_id)
    }

    /// Wait for the pane's shell to settle, then type the log follower.  A
    /// shell that never settles leaves a plain shell in the pane; the state
    /// reports still go through.
    fn arm_pane(&self, pane_id: &str, turn: &TurnIdentity, budget: &Budget) {
        let settle = Instant::now() + SHELL_SETTLE.min(budget.remaining());
        loop {
            match self.client.pane_process_info(pane_id) {
                Ok(info) if info.is_idle_shell() => break,
                Ok(_) | Err(HerdrError::Server { .. }) => {}
                Err(_) => return,
            }
            if Instant::now() >= settle || budget.exhausted() {
                return;
            }
            thread::sleep(SHELL_POLL);
        }
        let command = format!(
            "{FOLLOW_TURN_COMMAND} {} {} {}",
            turn.project_id, turn.worktree_id, turn.job_id
        );
        let _ = self.client.pane_send_input(pane_id, &command, &["enter"]);
    }

    fn report(
        &self,
        pane_id: &str,
        turn: &TurnIdentity,
        state: AgentState,
        outcome: &str,
        message: Option<&str>,
    ) -> Result<(), HerdrError> {
        let message = message.and_then(bounded_message);
        self.client.pane_report_agent(
            pane_id,
            herdr_agent_kind(turn.agent),
            state,
            message.as_deref(),
        )?;
        let metadata = PaneMetadata {
            agent: Some(herdr_agent_kind(turn.agent).to_owned()),
            title: Some(title_line(turn.task_id, &turn.title)),
            display_agent: Some(DISPLAY_AGENT.to_owned()),
            state_labels: state_labels(turn.turn_number, Some(outcome)),
            tokens: tokens(turn, outcome),
            ttl_ms: None,
        };
        self.client.pane_report_metadata(pane_id, &metadata)
    }
}
