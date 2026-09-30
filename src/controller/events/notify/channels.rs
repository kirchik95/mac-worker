//! Local notification channels. Tests use a fake process runner and a
//! private Unix socket; nothing here posts a real macOS or herdr notification.

use std::{ffi::OsString, fs, os::unix::fs::FileTypeExt, path::Path, sync::Arc, time::Duration};

use crate::{
    config::NotificationsConfig,
    controller::events::contracts::{
        EventSupport, NOTICE_CHANNEL_BUDGET, Notice, NoticeChannel, NoticeSound, NotifyChannel,
        NotifyOptions, SafeOutcome,
    },
    error::WorkerError,
    herdr::{HerdrClient, HerdrSocket, NotificationSound},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner},
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
    fs::symlink_metadata(socket.path())
        .map(|metadata| metadata.file_type().is_socket())
        .unwrap_or(false)
}

pub fn laptop_notification_socket<F>(home: &Path, lookup: F) -> HerdrSocket
where
    F: Fn(&str) -> Option<OsString>,
{
    HerdrSocket::from_env_or_home(lookup, home)
}

pub struct UnconfirmedTask {
    pub task_id: TaskId,
    pub outcome: Option<SafeOutcome>,
}

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

pub fn notices_for_support(support: EventSupport) -> Vec<Notice> {
    match support {
        EventSupport::Supported | EventSupport::Unsupported => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{BufRead, BufReader, Write},
        os::unix::{net::UnixListener, process::ExitStatusExt},
        process::ExitStatus,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use serde_json::Value;

    use super::{
        ChannelOptions, HerdrChannel, MacosChannel, OSASCRIPT_HANDLER, SelectedChannel,
        channels_for, eligibility_unknown_diagnostic, herdr_socket_reachable,
        laptop_notification_socket, notices_for_support, select_channels,
    };
    use crate::{
        config::NotificationsConfig,
        controller::events::contracts::{
            EventSupport, Notice, NoticeChannel, NoticeSound, NotifyChannel, NotifyOptions,
            SafeOutcome,
        },
        error::WorkerError,
        herdr::HerdrSocket,
        process::{ProcessRequest, ProcessResult, ProcessRunner},
        task::TaskId,
    };
    use uuid::Uuid;

    struct RecordingRunner {
        requests: Mutex<Vec<ProcessRequest>>,
    }

    impl ProcessRunner for RecordingRunner {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.requests
                .lock()
                .expect("requests")
                .push(request.clone());
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    fn notice(sound: NoticeSound) -> Notice {
        Notice {
            fingerprint: "turn:abc".into(),
            title: "Done".into(),
            body: "body".into(),
            sound,
        }
    }

    #[test]
    fn macos_uses_a_fixed_argv_handler_and_a_two_second_budget() {
        let runner = Arc::new(RecordingRunner {
            requests: Mutex::new(Vec::new()),
        });
        let channel = MacosChannel::new(
            runner.clone(),
            ChannelOptions {
                deadline: Duration::from_secs(2),
            },
        );
        let title = "$(rm -rf /) \" ; do shell script \"echo pwned";
        let body = "item 1 of argv\nsecond";
        channel
            .deliver(
                &Notice {
                    fingerprint: "fp".into(),
                    title: title.into(),
                    body: body.into(),
                    sound: NoticeSound::Done,
                },
                Duration::from_secs(2),
            )
            .expect("deliver");
        let requests = runner.requests.lock().expect("requests");
        let request = &requests[0];
        assert_eq!(request.program, "/usr/bin/osascript");
        assert_eq!(request.policy.deadline, Duration::from_secs(2));
        assert_eq!(request.args[0], "-e");
        assert_eq!(request.args[1], OSASCRIPT_HANDLER);
        assert!(OSASCRIPT_HANDLER.contains("on run argv"));
        assert!(
            OSASCRIPT_HANDLER
                .contains("display notification (item 2 of argv) with title (item 1 of argv)")
        );
        assert!(!request.args[1].to_string_lossy().contains("rm -rf"));
        assert_eq!(request.args[2], "--");
        assert_eq!(
            request.args[3],
            "$(rm -rf /) \" ; do shell script \"echo pwned"
        );
        assert_eq!(request.args[4], "item 1 of argv\nsecond");
        assert!(request.args.iter().all(|arg| arg != "sh" && arg != "-c"));
        assert!(request.stdin.is_none());
    }

    #[test]
    fn herdr_maps_sounds_and_does_not_use_the_controller_socket() {
        let root = tempfile::tempdir().expect("tempdir");
        let socket_path = root.path().join("laptop.sock");
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let listener = UnixListener::bind(&socket_path).expect("bind");
        let recorded_thread = Arc::clone(&recorded);
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut line = String::new();
            reader.read_line(&mut line).expect("read");
            let value: Value = serde_json::from_str(line.trim()).expect("json");
            recorded_thread.lock().expect("record").push(value.clone());
            let reply = serde_json::json!({"id": value["id"], "result": {}});
            let mut stream = stream;
            writeln!(stream, "{reply}").expect("reply");
        });
        let socket = HerdrSocket::at(&socket_path);
        let channel = HerdrChannel::new(socket, ChannelOptions::default());
        channel
            .deliver(&notice(NoticeSound::Request), Duration::from_secs(2))
            .expect("show");
        let requests = recorded.lock().expect("requests");
        assert_eq!(requests[0]["method"], "notification.show");
        assert_eq!(requests[0]["params"]["sound"], "request");
        assert_eq!(requests[0]["params"]["title"], "Done");
        assert_eq!(requests[0]["params"]["body"], "body");

        let laptop = tempfile::tempdir().expect("laptop");
        let controller = tempfile::tempdir().expect("controller");
        let env_socket = laptop.path().join("env.sock");
        let discovered = laptop_notification_socket(controller.path(), |key| {
            (key == "HERDR_SOCKET_PATH").then(|| std::ffi::OsString::from(&env_socket))
        });
        assert_eq!(discovered.path(), env_socket.as_path());
        assert_ne!(
            discovered.path(),
            HerdrSocket::default_for_home(controller.path()).path()
        );
        assert!(!herdr_socket_reachable(&HerdrSocket::default_for_home(
            controller.path()
        )));
    }

    #[test]
    fn auto_both_quiet_and_explicit_herdr_failure() {
        let enabled = NotificationsConfig { herdr: true };
        let disabled = NotificationsConfig { herdr: false };
        let auto = NotifyOptions::default();
        assert_eq!(
            select_channels(&auto, &enabled, true).0,
            vec![SelectedChannel::Herdr]
        );
        assert_eq!(
            select_channels(&auto, &enabled, false).0,
            vec![SelectedChannel::Macos]
        );
        assert_eq!(
            select_channels(&auto, &disabled, true).0,
            vec![SelectedChannel::Macos]
        );
        let both = NotifyOptions {
            channel: NotifyChannel::Both,
            ..NotifyOptions::default()
        };
        assert_eq!(
            select_channels(&both, &enabled, true).0,
            vec![SelectedChannel::Herdr, SelectedChannel::Macos]
        );
        let quiet = NotifyOptions {
            quiet: true,
            channel: NotifyChannel::Both,
            ..NotifyOptions::default()
        };
        assert!(select_channels(&quiet, &enabled, true).0.is_empty());
        let explicit = NotifyOptions {
            channel: NotifyChannel::Herdr,
            ..NotifyOptions::default()
        };
        let (channels, diagnostic) = select_channels(&explicit, &enabled, false);
        assert_eq!(channels, vec![SelectedChannel::Herdr]);
        assert_eq!(
            diagnostic,
            Some("herdr notification channel is unavailable")
        );

        let missing = tempfile::tempdir().expect("missing");
        let path = missing.path().join("controller-secret-socket");
        let error = HerdrChannel::new(HerdrSocket::at(&path), ChannelOptions::default())
            .deliver(&notice(NoticeSound::None), Duration::from_secs(2))
            .expect_err("absent");
        let text = error.to_string();
        assert!(text.contains("herdr absent"));
        assert!(!text.contains("controller-secret-socket"));
        assert!(!fs::metadata(&path).is_ok());
    }

    #[test]
    fn both_attempts_each_channel_once_and_quiet_calls_neither() {
        let runner = Arc::new(RecordingRunner {
            requests: Mutex::new(Vec::new()),
        });
        let root = tempfile::tempdir().expect("tempdir");
        let socket_path = root.path().join("both.sock");
        let hits = Arc::new(Mutex::new(0_u32));
        let listener = UnixListener::bind(&socket_path).expect("bind");
        let hits_thread = Arc::clone(&hits);
        thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut line = String::new();
            reader.read_line(&mut line).expect("read");
            let value: Value = serde_json::from_str(line.trim()).expect("json");
            *hits_thread.lock().expect("hits") += 1;
            assert_eq!(value["params"]["sound"], "done");
            let reply = serde_json::json!({"id": value["id"], "result": {}});
            let mut stream = stream;
            writeln!(stream, "{reply}").expect("reply");
        });
        let options = NotifyOptions {
            channel: NotifyChannel::Both,
            ..NotifyOptions::default()
        };
        let channels = channels_for(
            &options,
            &NotificationsConfig { herdr: true },
            HerdrSocket::at(&socket_path),
            true,
            runner.clone(),
        );
        assert_eq!(channels.len(), 2);
        let notice = Notice {
            fingerprint: "fp".into(),
            title: "Done".into(),
            body: "1 task".into(),
            sound: NoticeSound::Done,
        };
        for channel in &channels {
            channel
                .deliver(&notice, Duration::from_secs(2))
                .expect("attempt");
        }
        assert_eq!(runner.requests.lock().expect("argv").len(), 1);
        assert_eq!(*hits.lock().expect("hits"), 1);

        let quiet = channels_for(
            &NotifyOptions {
                quiet: true,
                channel: NotifyChannel::Both,
                ..NotifyOptions::default()
            },
            &NotificationsConfig { herdr: true },
            HerdrSocket::at(&socket_path),
            true,
            runner.clone(),
        );
        assert!(quiet.is_empty());
    }

    #[test]
    fn unsupported_controller_says_eligibility_unknown_and_raises_no_banners() {
        let task_id = TaskId::new(Uuid::from_u128(77));
        let text = eligibility_unknown_diagnostic(&[super::UnconfirmedTask {
            task_id,
            outcome: Some(SafeOutcome::NeedsInput),
        }]);
        assert!(text.contains("eligibility unknown"));
        assert!(text.contains(&task_id.to_string()));
        assert!(text.contains("needs_input"));
        assert!(!text.contains("task.wait.poll"));
        assert!(notices_for_support(EventSupport::Unsupported).is_empty());
        assert!(notices_for_support(EventSupport::Supported).is_empty());
    }

    #[test]
    fn herdr_sound_map_covers_done_request_and_none() {
        assert_eq!(super::herdr_sound(NoticeSound::Done).as_str(), "done");
        assert_eq!(super::herdr_sound(NoticeSound::Request).as_str(), "request");
        assert_eq!(super::herdr_sound(NoticeSound::None).as_str(), "none");
    }
}
