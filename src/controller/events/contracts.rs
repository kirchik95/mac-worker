//! Wire and component contracts. No filesystem or transport side effects.

use crate::error::WorkerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::{fmt, str::FromStr};

/// Canonical decimal u64 on the wire; never a JSON number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

impl Seq {
    pub const ZERO: Self = Self(0);
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn as_u64(self) -> u64 {
        self.0
    }
    pub fn checked_increment(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Seq {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 20
            || !value.bytes().all(|b| b.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return Err("sequence must be canonical decimal u64 text".into());
        }
        value
            .parse()
            .map(Self)
            .map_err(|_| "invalid sequence".into())
    }
}

impl Serialize for Seq {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for Seq {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(de::Error::custom)
    }
}

/// Journal UUID and publication position. Ordering across different UUIDs
/// is deterministic for containers only; it never establishes causality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventCursor {
    #[serde(with = "uuid_wire")]
    pub journal_id: uuid::Uuid,
    pub seq: Seq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadQuery {
    #[serde(default)]
    pub after: Option<EventCursor>,
    #[serde(default = "default_read_limit")]
    pub limit: usize,
    #[serde(default)]
    pub wait_ms: u64,
}

fn default_read_limit() -> usize {
    128
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum EventSelector {
    Read(ReadQuery),
}

impl EventSelector {
    pub fn request_body(&self) -> Result<serde_json::Value, WorkerError> {
        Ok(serde_json::json!({"controller_events": self}))
    }
}

mod uuid_wire {
    use super::*;
    pub fn serialize<S: Serializer>(id: &uuid::Uuid, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&id.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<uuid::Uuid, D::Error> {
        let value = String::deserialize(d)?;
        let id = uuid::Uuid::parse_str(&value).map_err(de::Error::custom)?;
        if id.is_nil() || id.to_string() != value {
            return Err(de::Error::custom("invalid journal UUID"));
        }
        Ok(id)
    }
}
