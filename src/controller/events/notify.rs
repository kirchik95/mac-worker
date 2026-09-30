//! Notifier facade. Policy, private cache and channels are implemented by T7.

mod cache;
mod channels;

pub use cache::{NotifyCache, commit_then_deliver, plan_notifications};
pub use channels::{
    ChannelOptions, HerdrChannel, MacosChannel, OSASCRIPT_HANDLER, SelectedChannel,
    UnconfirmedTask, channels_for, eligibility_unknown_diagnostic, herdr_socket_reachable,
    herdr_socket_reachable_with, herdr_sound, laptop_notification_socket, notices_for_support,
    select_channels,
};

pub use super::contracts::{
    BaselineKind, Notice, NoticeChannel, NoticeSound, NotifyChannel, NotifyOptions, NotifyPlan,
    NotifyState, PendingCandidate, Reconciliation, TaskFacts,
};
