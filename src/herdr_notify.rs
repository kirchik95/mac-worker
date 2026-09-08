//! Tells the operator's herdr on the MacBook that a turn ended.
//!
//! The local turn runner converges every terminal path of a turn in one
//! place; this is what it says there, through the herdr socket the runner
//! was started with (herdr's own `HERDR_SOCKET_PATH` when the command ran
//! inside herdr, else the account's default session).  One notification per
//! terminal turn, bounded by a two-second budget, failures ignored: a
//! missing herdr costs a failed connect and nothing else.

use std::time::Duration;

use crate::{
    agent::Question,
    herdr::{CONNECT_DEADLINE, HerdrClient, HerdrError, HerdrSocket, NotificationSound},
    herdr_reporter::short_task_id,
    task::{TaskId, TaskOutcome},
};

/// The whole notification, connect included, fits in this.
pub const NOTIFY_BUDGET: Duration = Duration::from_secs(2);
const BODY_LIMIT: usize = 512;

/// What herdr is told: a title, an optional body, and a sound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub title: String,
    pub body: Option<String>,
    pub sound: NotificationSound,
}

/// Compose the notification for a finished turn from values that already
/// passed the redaction boundary on the worker.
pub fn notification_for(
    task_id: TaskId,
    title: &str,
    outcome: &TaskOutcome,
    summary: Option<&str>,
    questions: &[Question],
) -> Notification {
    let (kind, sound, detail) = match outcome {
        TaskOutcome::Done => ("done", NotificationSound::Done, summary.map(str::to_owned)),
        TaskOutcome::NeedsInput => (
            "needs_input",
            NotificationSound::Request,
            questions
                .first()
                .map(|question| question.text().to_owned())
                .or_else(|| summary.map(str::to_owned)),
        ),
        TaskOutcome::Blocked => (
            "blocked",
            NotificationSound::Request,
            summary
                .map(str::to_owned)
                .or_else(|| questions.first().map(|question| question.text().to_owned())),
        ),
        TaskOutcome::Unknown => ("unknown", NotificationSound::None, None),
        TaskOutcome::Failed { reason } => ("failed", NotificationSound::None, Some(reason.clone())),
        TaskOutcome::Cancelled => ("cancelled", NotificationSound::None, None),
        TaskOutcome::TimedOut => ("timed_out", NotificationSound::None, None),
        TaskOutcome::Lost => ("lost", NotificationSound::None, None),
    };
    let mut body = title.trim().to_owned();
    if let Some(detail) = detail.map(|detail| detail.trim().to_owned())
        && !detail.is_empty()
    {
        if body.is_empty() {
            body = detail;
        } else {
            body = format!("{body} — {detail}");
        }
    }
    let body: String = body
        .chars()
        .filter(|character| !character.is_control())
        .collect();
    let body = if body.is_empty() {
        None
    } else {
        let mut end = BODY_LIMIT.min(body.len());
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        Some(body[..end].to_owned())
    };
    Notification {
        title: format!("task {}: {kind}", short_task_id(task_id)),
        body,
        sound,
    }
}

/// The laptop-side notifier for one herdr session.
#[derive(Debug, Clone)]
pub struct HerdrNotifier {
    client: HerdrClient,
}

impl HerdrNotifier {
    pub fn new(socket: HerdrSocket) -> Self {
        Self {
            client: HerdrClient::with_deadlines(
                socket,
                CONNECT_DEADLINE,
                NOTIFY_BUDGET.saturating_sub(CONNECT_DEADLINE),
            ),
        }
    }

    pub fn show(&self, notification: &Notification) -> Result<(), HerdrError> {
        self.client.notification_show(
            &notification.title,
            notification.body.as_deref(),
            notification.sound,
        )
    }

    /// One notification for a finished turn; the caller ignores the result.
    pub fn turn_finished(
        &self,
        task_id: TaskId,
        title: &str,
        outcome: &TaskOutcome,
        summary: Option<&str>,
        questions: &[Question],
    ) -> Result<(), HerdrError> {
        self.show(&notification_for(
            task_id, title, outcome, summary, questions,
        ))
    }
}
