use super::super::{
    contracts::{
        CODEX_ROLLOUT_FILE, PlaceContext, PlacedSession, SessionAgent, SessionFormat,
        SessionPackage, SessionPlace, session_error,
    },
    tokens,
};
use crate::error::WorkerError;
use std::{
    io::ErrorKind,
    path::{Component, Path},
};
pub struct CodexPlace;
impl SessionPlace for CodexPlace {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Codex
    }
    fn place(
        &self,
        package: &SessionPackage,
        cx: &PlaceContext<'_>,
    ) -> Result<PlacedSession, WorkerError> {
        if package.manifest().agent != SessionAgent::Codex
            || package.manifest().format != SessionFormat::CodexRolloutV1
            || package.files().len() != 1
            || package.files()[0].path != CODEX_ROLLOUT_FILE
        {
            return Err(session_error(
                "SESSION_PLACEMENT_FAILED",
                "invalid Codex rollout package",
            ));
        }
        let workspace = cx.workspace.to_str().ok_or_else(|| {
            session_error("SESSION_PLACEMENT_FAILED", "workspace path is not UTF-8")
        })?;
        let bytes = tokens::materialize(&package.files()[0].bytes, workspace, cx.session_id)?;
        let mut lines = bytes.split_inclusive(|&byte| byte == b'\n');
        let first = lines
            .next()
            .ok_or_else(|| session_error("SESSION_PLACEMENT_FAILED", "empty Codex rollout"))?;
        let meta: serde_json::Value = serde_json::from_slice(first).map_err(|_| {
            session_error("SESSION_PLACEMENT_FAILED", "invalid Codex rollout JSONL")
        })?;
        if meta["type"].as_str() != Some("session_meta")
            || meta["payload"]["id"].as_str() != Some(cx.session_id)
            || !meta["payload"]["cwd"]
                .as_str()
                .is_some_and(|cwd| cwd_inside(cx.workspace, Path::new(cwd)))
        {
            return Err(session_error(
                "SESSION_PLACEMENT_FAILED",
                "invalid Codex session metadata",
            ));
        }
        for line in lines {
            serde_json::from_slice::<serde_json::Value>(line).map_err(|_| {
                session_error("SESSION_PLACEMENT_FAILED", "invalid Codex rollout JSONL")
            })?;
        }
        let path = rollout_path(cx.placed_at_millis, cx.session_id);
        cx.store.write_file(&path, &bytes)?;
        Ok(PlacedSession {
            primary_relative: path.clone(),
            files: vec![path],
        })
    }
}

fn cwd_inside(workspace: &Path, cwd: &Path) -> bool {
    if !cwd.is_absolute()
        || !cwd.starts_with(workspace)
        || cwd.components().any(|part| part == Component::ParentDir)
    {
        return false;
    }
    let Ok(workspace) = workspace.canonicalize() else {
        return false;
    };
    // A snapshot can omit empty directories. Resolve the nearest existing
    // ancestor to allow those paths without accepting an escaping symlink.
    let mut ancestor = cwd;
    loop {
        match ancestor.symlink_metadata() {
            Ok(_) => {
                return ancestor
                    .canonicalize()
                    .is_ok_and(|physical| physical.starts_with(&workspace));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let Some(parent) = ancestor.parent() else {
                    return false;
                };
                ancestor = parent;
            }
            Err(_) => return false,
        }
    }
}

fn rollout_path(placed_at_millis: u64, session_id: &str) -> String {
    let seconds = placed_at_millis / 1_000;
    let (year, month, day) = civil_from_days(seconds / 86_400);
    let time = seconds % 86_400;
    let hour = time / 3_600;
    let minute = time / 60 % 60;
    let second = time % 60;
    // UTC avoids host-local timezone dependence on planned-receipt retries.
    // Codex resolves a rollout by its id suffix, not this date's timezone.
    format!(
        "sessions/{year:04}/{month:02}/{day:02}/rollout-{year:04}-{month:02}-{day:02}T{hour:02}-{minute:02}-{second:02}-{session_id}.jsonl"
    )
}

/// Gregorian civil date for nonnegative days since 1970-01-01.
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    // Shift to a March-based calendar, whose leap day ends each year.
    // Every 400-year era has exactly 146097 days.
    let shifted = days + 719_468;
    let era = shifted / 146_097;
    let day_of_era = shifted % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_month + 2) / 5 + 1;
    let month = if march_month < 10 {
        march_month + 3
    } else {
        march_month - 9
    };
    year += u64::from(month <= 2);
    (year, month, day)
}
