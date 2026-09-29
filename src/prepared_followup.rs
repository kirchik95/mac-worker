//! Replay-safe prepared follow-up identity for the durable controller.
//!
//! The controller persists one [`PreparedFollowup`] **before** any effect and
//! replays the identical value after a crash. Every publication boundary
//! (prompt write, task CAS, queue enqueue, runner handoff) reuses the same
//! [`TurnId`](crate::task::TurnId), timestamp, message, and derived frozen
//! values, so a retry converges on one new turn instead of allocating a
//! second follow-up. No phantom [`RunId`](crate::task::RunId) is created and
//! no controller feature flag is required: authority is the caller-supplied
//! validated expected [`LocalTaskRecord`].

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    error::WorkerError,
    task::{BaseOid, LocalTaskRecord, QuestionsPolicy, TaskId, TaskOutcome, TaskState, TurnId},
    task_client::{compose_turn_prompt, task_error, validate_prompt},
};

/// One persisted follow-up intent, allocated once by the controller caller.
///
/// `attached` stays a call-time execution mode on
/// [`TaskClient::say_prepared`](crate::task_client::TaskClient::say_prepared);
/// everything needed to reproduce the turn bit-for-bit is frozen here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedFollowup {
    #[serde(default, skip_serializing_if = "crate::task::is_false")]
    auto_continue: bool,
    expected: LocalTaskRecord,
    turn_id: TurnId,
    turn_number: u32,
    created_at_millis: u64,
    message: String,
    composed_prompt: String,
    base_oid: BaseOid,
    agent: String,
    model: Option<String>,
    worker: String,
    max_followups: u32,
}

impl PreparedFollowup {
    /// Freezes one follow-up intent against a trusted expected snapshot.
    ///
    /// Pure: validates the snapshot and derives the stable identity, but
    /// never touches the store. The live revision fence happens in
    /// `say_prepared`, which replays this exact value.
    pub fn prepare(
        expected: &LocalTaskRecord,
        message: String,
        turn_id: TurnId,
        created_at_millis: u64,
    ) -> Result<Self, WorkerError> {
        // A human's preparation freezes the finished task, not the automatic
        // answer it will supersede. Live intent removal belongs to say's CAS.
        let without_automatic = expected.with_auto_continue_intent(None)?;
        let expected = &without_automatic;
        match expected.status().state() {
            TaskState::Active => {
                return Err(task_error("TASK_BUSY", "task has an active turn"));
            }
            TaskState::Closed | TaskState::Abandoned | TaskState::Lost => {
                return Err(task_error("TASK_CLOSED", "task is terminal"));
            }
            TaskState::Queued => {
                return Err(task_error("TASK_BUSY", "task has not reached an open turn"));
            }
            TaskState::Open => {}
        }
        if expected.close_intent().is_some() {
            return Err(task_error("TASK_BUSY", "task close is in progress"));
        }
        let worker = expected
            .status()
            .worker()
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "task has not selected a worker"))?
            .to_owned();
        let followups = expected.status().turns().len().saturating_sub(1) as u32;
        if followups >= expected.meta().limits().max_followups {
            return Err(task_error(
                "FOLLOWUP_LIMIT",
                "task follow-up limit has been reached",
            ));
        }
        let turn_number = u32::try_from(expected.status().turns().len())
            .ok()
            .and_then(|turns| turns.checked_add(1))
            .ok_or_else(|| task_error("TASK_INCONSISTENT", "turn history is too long"))?;
        let base_oid = expected
            .status()
            .head_oid()
            .cloned()
            .unwrap_or_else(|| expected.meta().base_oid().clone());
        let composed_prompt = compose_turn_prompt(
            expected.meta().task_id(),
            turn_number,
            expected.meta().agent(),
            &base_oid,
            None,
            &message,
            true,
            expected.questions_policy(),
        );
        validate_prompt(&composed_prompt)?;
        Ok(Self {
            auto_continue: false,
            expected: expected.clone(),
            turn_id,
            turn_number,
            created_at_millis,
            message,
            composed_prompt,
            base_oid,
            agent: expected.meta().agent().as_str().to_owned(),
            model: expected.meta().model().map(str::to_owned),
            worker,
            max_followups: expected.meta().limits().max_followups,
        })
    }

    /// Rejects a hand-built or stale value whose frozen fields no longer match
    /// what [`prepare`](Self::prepare) derives from its own expected snapshot.
    /// A replayed turn with different input for the same turn id is a
    /// `TASK_REVISION_CONFLICT`, never a silent second meaning.
    pub fn validate_self_consistency(&self) -> Result<(), WorkerError> {
        let rebuilt = if self.auto_continue {
            Self::automatic(&self.expected)?.ok_or_else(|| {
                task_error(
                    "TASK_REVISION_CONFLICT",
                    "task is not eligible for automatic continuation",
                )
            })?
        } else {
            Self::prepare(
                &self.expected,
                self.message.clone(),
                self.turn_id,
                self.created_at_millis,
            )?
        };
        if rebuilt != *self {
            return Err(task_error(
                "TASK_REVISION_CONFLICT",
                "prepared follow-up does not match its expected snapshot",
            ));
        }
        Ok(())
    }

    pub(crate) fn automatic(expected: &LocalTaskRecord) -> Result<Option<Self>, WorkerError> {
        let Some(finished) = expected.status().turns().last() else {
            return Ok(None);
        };
        if expected.questions_policy() != QuestionsPolicy::Decide
            || expected.status().state() != TaskState::Open
            || expected.status().last_outcome() != Some(&TaskOutcome::NeedsInput)
            || finished.outcome() != Some(&TaskOutcome::NeedsInput)
            || finished.terminal().is_none()
            || finished.auto_continue()
            || expected.close_intent().is_some()
            || expected.status().turns().len().saturating_sub(1) as u32
                >= expected.meta().limits().max_followups
        {
            return Ok(None);
        }
        let questions = serde_json::to_string_pretty(expected.status().questions())
            .map_err(|_| task_error("TASK_CONFIG_INVALID", "questions could not be encoded"))?;
        let message = format!(
            "Your questions and options:\n{questions}\n\nNo answer will come. For each question choose the option you recommend (the first option if you listed them by preference), finish the task, and list your choices in the summary."
        );
        let mut prepared = Self::prepare(
            expected,
            message,
            Self::automatic_turn_id(finished.turn_id()),
            finished
                .ended_at_millis()
                .unwrap_or(expected.status().updated_at_millis()),
        )?;
        prepared.auto_continue = true;
        Ok(Some(prepared))
    }

    pub(crate) fn automatic_turn_id(finished: TurnId) -> TurnId {
        let mut digest = Sha256::new();
        digest.update(b"mac-worker/auto-continue/v1\x00");
        digest.update(finished.as_uuid().as_bytes());
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest.finalize()[..16]);
        // RFC 9562 UUIDv8: domain-separated, deterministic SHA-256 identity.
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        TurnId::new(uuid::Uuid::from_bytes(bytes))
    }

    pub fn auto_continue(&self) -> bool {
        self.auto_continue
    }

    pub fn expected(&self) -> &LocalTaskRecord {
        &self.expected
    }

    pub fn task_id(&self) -> TaskId {
        self.expected.meta().task_id()
    }

    pub fn turn_id(&self) -> TurnId {
        self.turn_id
    }

    pub fn turn_number(&self) -> u32 {
        self.turn_number
    }

    pub fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn composed_prompt(&self) -> &str {
        &self.composed_prompt
    }

    /// Minimal durable exact-preparation binding, persisted beside the prompt
    /// and verified before both active and terminal replay — including after
    /// prompt retirement and result-HEAD advance, when neither the prompt
    /// bytes nor the frozen base can still be compared.
    ///
    /// Domain-separated digest of the full stably-serialized preparation,
    /// including the original expected snapshot: the composed prompt alone
    /// does not carry the expected revision, previous-turn history, model,
    /// worker, limits, or the other frozen fields, so hashing it would accept
    /// a different self-consistent preparation under the same turn. Derived
    /// purely from this frozen value, so allowed live sidecar updates never
    /// change an already-persisted binding.
    pub fn binding(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"mac-worker/prepared-followup-binding/v1\x00");
        let bytes =
            serde_json::to_vec(self).expect("prepared follow-up must serialize deterministically");
        digest.update(bytes);
        format!("{:x}", digest.finalize())
    }

    pub fn base_oid(&self) -> &BaseOid {
        &self.base_oid
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn worker(&self) -> &str {
        &self.worker
    }

    pub fn max_followups(&self) -> u32 {
        self.max_followups
    }
}
