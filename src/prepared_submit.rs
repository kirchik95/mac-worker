//! Shared frozen/prepared submit adapter.
//!
//! Ordinary controller submit passes `run_id = None` and never creates a
//! `RunRecord` or requires DAG membership. DAG callers pass `Some(run_id)`.
//! Stable task/turn IDs and `created_at_millis` come from the original
//! frozen request; resume must not recapture HEAD or `.worker.toml`.

use serde::{Deserialize, Serialize};

use crate::{
    agent::{AgentKind, PermissionPolicy, TurnLimits},
    error::WorkerError,
    scheduler::WorkerPreference,
    task::{BaseOid, ClosePolicy, RunId, TaskId, TaskLimits, TurnId},
};

/// Frozen payload for one submit. No MacBook paths. `run_id` is optional:
/// ordinary remote tasks omit it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSubmitBody {
    pub task_id: TaskId,
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    pub created_at_millis: u64,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_url: Option<String>,
    pub publish: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_branch: Option<String>,
    pub close_on: ClosePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    pub wip: bool,
    pub project_id: String,
    pub worktree_id: String,
    pub base_oid: BaseOid,
    pub timeout_millis: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_budget_usd_cents: Option<u64>,
    pub max_followups: u32,
    pub permissions: String,
    pub requires: Vec<String>,
    pub include_untracked: Vec<String>,
    pub include_empty_dirs: Vec<String>,
    pub allow_sensitive: Vec<String>,
    pub cli_includes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// OVERLAPPING/PENDING OpenCode exclusive no_wait. Mechanical field so
    /// the envelope compiles; omitted old bodies default true.
    #[serde(default = "default_true")]
    pub wait_for_capacity: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSubmit {
    pub task_id: TaskId,
    pub turn_id: TurnId,
    pub run_id: Option<RunId>,
    pub created_at_millis: u64,
    pub prompt: String,
    pub title: Option<String>,
    pub agent: AgentKind,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub source: String,
    pub origin_url: Option<String>,
    pub publish: Vec<String>,
    pub publish_branch: Option<String>,
    pub close_policy: ClosePolicy,
    pub env_profile: Option<String>,
    pub preference: WorkerPreference,
    pub wip: bool,
    pub project_id: String,
    pub worktree_id: String,
    pub base_oid: BaseOid,
    pub limits: TaskLimits,
    pub policy: PermissionPolicy,
    pub requires: Vec<String>,
    pub wait_for_capacity: bool,
    pub attached: bool,
    pub branch: Option<String>,
    pub include_untracked: Vec<String>,
    pub include_empty_dirs: Vec<String>,
    pub allow_sensitive: Vec<String>,
}

impl FrozenSubmitBody {
    pub fn prepared(&self) -> Result<PreparedSubmit, WorkerError> {
        let agent = parse_frozen_agent(&self.agent)?;
        let policy = match self.permissions.as_str() {
            "workspace" => PermissionPolicy::Workspace,
            "unattended" => PermissionPolicy::Unattended,
            _ => {
                return Err(WorkerError::task(
                    "TASK_CONFIG_INVALID",
                    "frozen permissions must be workspace or unattended",
                ));
            }
        };
        let limits = TaskLimits::new(
            TurnLimits::new(
                self.timeout_millis,
                self.max_turns,
                self.max_budget_usd_cents,
            )
            .map_err(|error| WorkerError::task("TASK_CONFIG_INVALID", error.to_string()))?,
            self.max_followups,
        )?;
        Ok(PreparedSubmit {
            task_id: self.task_id,
            turn_id: self.turn_id,
            run_id: self.run_id,
            created_at_millis: self.created_at_millis,
            prompt: self.prompt.clone(),
            title: self.title.clone(),
            agent,
            model: self.model.clone(),
            effort: self.effort.clone(),
            source: self.source.clone(),
            origin_url: self.origin_url.clone(),
            publish: self.publish.clone(),
            publish_branch: self.publish_branch.clone(),
            close_policy: self.close_on,
            env_profile: self.env_profile.clone(),
            preference: match &self.worker {
                Some(worker) => WorkerPreference::Pinned {
                    worker: worker.clone(),
                },
                None => WorkerPreference::Automatic,
            },
            wip: self.wip,
            project_id: self.project_id.clone(),
            worktree_id: self.worktree_id.clone(),
            base_oid: self.base_oid.clone(),
            limits,
            policy,
            requires: self.requires.clone(),
            wait_for_capacity: self.wait_for_capacity,
            attached: false,
            branch: self.branch.clone(),
            include_untracked: self.include_untracked.clone(),
            include_empty_dirs: self.include_empty_dirs.clone(),
            allow_sensitive: self.allow_sensitive.clone(),
        })
    }
}

fn parse_frozen_agent(value: &str) -> Result<AgentKind, WorkerError> {
    match value {
        "codex" => Ok(AgentKind::Codex),
        "claude" => Ok(AgentKind::Claude),
        "cursor" => Ok(AgentKind::Cursor),
        "opencode" => Ok(AgentKind::Opencode),
        _ => Err(WorkerError::task("AGENT_UNSUPPORTED", "unknown agent")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{BaseOid, TaskId, TurnId};
    use serde_json::json;

    fn sample_oid() -> BaseOid {
        "dddddddddddddddddddddddddddddddddddddddd".parse().unwrap()
    }

    fn sample_body_json(wait_for_capacity: Option<bool>) -> serde_json::Value {
        let mut body = json!({
            "task_id": "018f0f4a6b5c7d8e9f00112233445566",
            "turn_id": "118f0f4a6b5c7d8e9f00112233445566",
            "created_at_millis": 1,
            "prompt": "p",
            "agent": "codex",
            "source": "local",
            "publish": ["fetch"],
            "close_on": "never",
            "wip": true,
            "project_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "worktree_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "base_oid": sample_oid().as_str(),
            "timeout_millis": 1000,
            "max_followups": 10,
            "permissions": "workspace",
            "requires": [],
            "include_untracked": [],
            "include_empty_dirs": [],
            "allow_sensitive": [],
            "cli_includes": [],
        });
        if let Some(flag) = wait_for_capacity {
            body["wait_for_capacity"] = json!(flag);
        }
        body
    }

    #[test]
    fn omitted_wait_for_capacity_defaults_true() {
        let body: FrozenSubmitBody = serde_json::from_value(sample_body_json(None)).unwrap();
        assert!(body.wait_for_capacity);
        assert!(body.prepared().unwrap().wait_for_capacity);
    }

    #[test]
    fn false_wait_for_capacity_survives_prepared_and_replay() {
        let original = sample_body_json(Some(false));
        let body: FrozenSubmitBody = serde_json::from_value(original.clone()).unwrap();
        assert!(!body.wait_for_capacity);
        let prepared = body.prepared().unwrap();
        assert!(!prepared.wait_for_capacity);
        let restored: FrozenSubmitBody =
            serde_json::from_value(serde_json::to_value(&body).unwrap()).unwrap();
        assert!(!restored.wait_for_capacity);
        assert!(!restored.prepared().unwrap().wait_for_capacity);
        let _task_id: TaskId = body.task_id;
        let _turn_id: TurnId = body.turn_id;
    }
}

