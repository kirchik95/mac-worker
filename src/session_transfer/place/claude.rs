use super::super::{claude_dir::claude_project_dir, contracts::*, tokens};
use crate::error::WorkerError;
pub struct ClaudePlace;
impl SessionPlace for ClaudePlace {
    fn agent(&self) -> SessionAgent {
        SessionAgent::Claude
    }
    fn place(
        &self,
        package: &SessionPackage,
        cx: &PlaceContext<'_>,
    ) -> Result<PlacedSession, WorkerError> {
        let sidecar_prefix = format!("{CLAUDE_SIDECAR_DIR}/");
        if package.manifest().agent != SessionAgent::Claude
            || package.manifest().format != SessionFormat::ClaudeJsonlV1
            || !package
                .files()
                .iter()
                .any(|file| file.path == CLAUDE_MAIN_FILE)
            || package.files().iter().any(|file| {
                file.path != CLAUDE_MAIN_FILE && !file.path.starts_with(&sidecar_prefix)
            })
        {
            return Err(session_error(
                "SESSION_PLACEMENT_FAILED",
                "invalid Claude session package",
            ));
        }
        // The caller supplies the physical path; do not resolve it again.
        let workspace = cx.workspace.to_str().ok_or_else(|| {
            session_error("SESSION_PLACEMENT_FAILED", "workspace path is not UTF-8")
        })?;
        let project = format!("projects/{}", claude_project_dir(workspace));
        let primary_relative = format!("{project}/{}.jsonl", cx.session_id);

        // Materialize and validate the entire package before creating native files.
        let mut writes = Vec::with_capacity(package.files().len());
        for file in package.files() {
            let bytes = tokens::materialize(&file.bytes, workspace, cx.session_id)?;
            if file.path.ends_with(".jsonl") {
                for line in bytes.split_inclusive(|&byte| byte == b'\n') {
                    validate_json(line)?;
                }
            } else if file.path.ends_with(".json") {
                validate_json(&bytes)?;
            }
            let relative = if file.path == CLAUDE_MAIN_FILE {
                primary_relative.clone()
            } else {
                let sidecar = file
                    .path
                    .strip_prefix(&sidecar_prefix)
                    .expect("validated sidecar path");
                format!("{project}/{}/{sidecar}", cx.session_id)
            };
            writes.push((relative, bytes));
        }

        // Publish the main transcript only after every sidecar is durable.
        writes.sort_by(|a, b| {
            (a.0 == primary_relative)
                .cmp(&(b.0 == primary_relative))
                .then_with(|| a.0.cmp(&b.0))
        });
        for (relative, bytes) in &writes {
            cx.store.write_file(relative, bytes)?;
        }
        let mut files: Vec<_> = writes.into_iter().map(|(relative, _)| relative).collect();
        files.sort();
        Ok(PlacedSession {
            primary_relative,
            files,
        })
    }
}

fn validate_json(bytes: &[u8]) -> Result<(), WorkerError> {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .map(|_| ())
        .map_err(|_| session_error("SESSION_PLACEMENT_FAILED", "invalid session JSON"))
}
