//! Notifier facade. Policy, private cache and channels are implemented by T7.

pub(crate) mod cache;
pub(crate) mod channels;
pub(crate) mod follow;

pub(crate) use cache::{NotifyCache, commit_then_deliver, plan_notifications};
pub(crate) use channels::{
    channels_for, herdr_socket_reachable, laptop_notification_socket, select_channels,
};
