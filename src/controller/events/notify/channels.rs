//! Local notification channels. Tests use a fake process runner and a
//! private Unix socket; nothing here posts a real macOS or herdr notification.

use std::{ffi::OsString, fs, os::unix::fs::FileTypeExt, path::Path, sync::Arc, time::Duration};

use crate::{
    config::NotificationsConfig,
    controller::events::contracts::{
        NOTICE_CHANNEL_BUDGET, Notice, NoticeChannel, NoticeSound, NotifyChannel, NotifyOptions,
    },
    error::WorkerError,
    herdr::{HerdrClient, HerdrSocket, NotificationSound},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
};
#[cfg(any(test, feature = "test-support"))]
use crate::{
    controller::events::contracts::{EventSupport, SafeOutcome},
    task::TaskId,
};

pub const OSASCRIPT_HANDLER: &str = "\
on run argv
display notification (item 2 of argv) with title (item 1 of argv)
end run";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChannelOptions {
    pub deadline: Duration,
}

impl Default for ChannelOptions {
    fn default() -> Self {
        Self {
            deadline: NOTICE_CHANNEL_BUDGET,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectedChannel {
    Macos,
    Herdr,
}

pub struct MacosChannel {
    runner: Arc<dyn ProcessRunner>,
    options: ChannelOptions,
}

impl MacosChannel {
    pub fn new(runner: Arc<dyn ProcessRunner>, options: ChannelOptions) -> Self {
        Self { runner, options }
    }
}

impl NoticeChannel for MacosChannel {
    fn deliver(&self, notice: &Notice, deadline: Duration) -> Result<(), WorkerError> {
        let request = ProcessRequest {
            program: OsString::from("/usr/bin/osascript"),
            args: vec![
                OsString::from("-e"),
                OsString::from(OSASCRIPT_HANDLER),
                OsString::from("--"),
                OsString::from(&notice.title),
                OsString::from(&notice.body),
            ],
            environment: Vec::new(),
            environment_remove: Vec::new(),
            stdin: None,
            policy: ProcessPolicy {
                stdout_limit: 4 * 1024,
                stderr_limit: 4 * 1024,
                deadline: deadline.min(self.options.deadline),
            },
            isolate_parent_environment: false,
        };
        let result = self.runner.run(&request)?;
        if result.status.success() {
            Ok(())
        } else {
            Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_NOTIFY_CHANNEL: macos notification failed".into(),
            ))
        }
    }
}

pub struct HerdrChannel {
    socket: HerdrSocket,
    options: ChannelOptions,
}

impl HerdrChannel {
    pub fn new(socket: HerdrSocket, options: ChannelOptions) -> Self {
        Self { socket, options }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn socket(&self) -> &HerdrSocket {
        &self.socket
    }
}

impl NoticeChannel for HerdrChannel {
    fn deliver(&self, notice: &Notice, deadline: Duration) -> Result<(), WorkerError> {
        let budget = deadline.min(self.options.deadline);
        if budget.is_zero() {
            return Err(WorkerError::Unavailable(
                "CONTROLLER_EVENTS_NOTIFY_CHANNEL: herdr timeout".into(),
            ));
        }
        let connect = Duration::from_millis(500).min(budget);
        let response = budget.saturating_sub(connect).max(Duration::from_millis(1));
        let client = HerdrClient::with_deadlines(self.socket.clone(), connect, response);
        client
            .notification_show(&notice.title, Some(&notice.body), herdr_sound(notice.sound))
            .map_err(|error| {
                WorkerError::Unavailable(format!(
                    "CONTROLLER_EVENTS_NOTIFY_CHANNEL: herdr {}",
                    error.kind()
                ))
            })
    }
}

pub fn herdr_sound(sound: NoticeSound) -> NotificationSound {
    match sound {
        NoticeSound::None => NotificationSound::None,
        NoticeSound::Done => NotificationSound::Done,
        NoticeSound::Request => NotificationSound::Request,
    }
}

pub fn select_channels(
    options: &NotifyOptions,
    notifications: &NotificationsConfig,
    herdr_reachable: bool,
) -> (Vec<SelectedChannel>, Option<&'static str>) {
    if options.quiet {
        return (Vec::new(), None);
    }
    match options.channel {
        NotifyChannel::Auto => {
            if notifications.herdr && herdr_reachable {
                (vec![SelectedChannel::Herdr], None)
            } else {
                (vec![SelectedChannel::Macos], None)
            }
        }
        NotifyChannel::Macos => (vec![SelectedChannel::Macos], None),
        NotifyChannel::Herdr => {
            let diagnostic = if herdr_reachable {
                None
            } else {
                Some("herdr notification channel is unavailable")
            };
            (vec![SelectedChannel::Herdr], diagnostic)
        }
        NotifyChannel::Both => (vec![SelectedChannel::Herdr, SelectedChannel::Macos], None),
    }
}

pub fn channels_for(
    options: &NotifyOptions,
    notifications: &NotificationsConfig,
    socket: HerdrSocket,
    reachable: bool,
    runner: Arc<dyn ProcessRunner>,
) -> Vec<Arc<dyn NoticeChannel>> {
    let (selected, _) = select_channels(options, notifications, reachable);
    selected
        .into_iter()
        .map(|channel| -> Arc<dyn NoticeChannel> {
            match channel {
                SelectedChannel::Macos => Arc::new(MacosChannel::new(
                    Arc::clone(&runner),
                    ChannelOptions::default(),
                )),
                SelectedChannel::Herdr => {
                    Arc::new(HerdrChannel::new(socket.clone(), ChannelOptions::default()))
                }
            }
        })
        .collect()
}

pub fn herdr_socket_reachable(socket: &HerdrSocket) -> bool {
    herdr_socket_reachable_with(socket, unix_connect_probe)
}

/// Auto selection uses this probe. A leftover socket pathname is not reachable
/// unless `probe` connects. Callers keep save-before-display and do not retry
/// a delivery that may already have been shown.
fn herdr_socket_reachable_with(socket: &HerdrSocket, probe: impl Fn(&Path) -> bool) -> bool {
    let is_socket = fs::symlink_metadata(socket.path())
        .map(|metadata| metadata.file_type().is_socket())
        .unwrap_or(false);
    is_socket && probe(socket.path())
}

fn unix_connect_probe(path: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(path).is_ok()
}

pub fn laptop_notification_socket<F>(home: &Path, lookup: F) -> HerdrSocket
where
    F: Fn(&str) -> Option<OsString>,
{
    HerdrSocket::from_env_or_home(lookup, home)
}

#[cfg(any(test, feature = "test-support"))]
pub struct UnconfirmedTask {
    pub task_id: TaskId,
    pub outcome: Option<SafeOutcome>,
}

#[cfg(any(test, feature = "test-support"))]
fn outcome_token(outcome: SafeOutcome) -> &'static str {
    match outcome {
        SafeOutcome::Done => "done",
        SafeOutcome::NeedsInput => "needs_input",
        SafeOutcome::Blocked => "blocked",
        SafeOutcome::Unknown => "unknown",
        SafeOutcome::Failed => "failed",
        SafeOutcome::Cancelled => "cancelled",
        SafeOutcome::TimedOut => "timed_out",
        SafeOutcome::Lost => "lost",
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn eligibility_unknown_diagnostic(rows: &[UnconfirmedTask]) -> String {
    let mut text = String::from("eligibility unknown");
    for row in rows {
        text.push('\n');
        text.push_str(&row.task_id.to_string());
        if let Some(outcome) = row.outcome {
            text.push(' ');
            text.push_str(outcome_token(outcome));
        }
    }
    text
}

#[cfg(any(test, feature = "test-support"))]
pub fn notices_for_support(support: EventSupport) -> Vec<Notice> {
    match support {
        EventSupport::Supported | EventSupport::Unsupported => Vec::new(),
    }
}
