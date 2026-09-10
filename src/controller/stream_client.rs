//! Laptop-side streamed Git handshake over controller RPC + GitTransport.

use std::path::Path;

use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    config::ControllerConfig,
    controller::{
        ControllerRequest, controller_worker_entry,
        execute::send_controller_read,
        protocol::parse_request,
        stream_rpc::{
            ControllerResultPrepareResult, ControllerSourceFinishResult,
            ControllerSourcePrepareResult,
        },
        transfer::{VerifiedResultMeta, import_controller_result},
    },
    error::WorkerError,
    git_transport::GitTransport,
    job::RequestFingerprint,
    paths::PathLayout,
    process::ProcessRunner,
    project_state::ProjectState,
    protocol::PROTOCOL_VERSION,
    task::{BaseOid, TaskId},
    task_client::FetchReport,
    transfer_repo::TransferRepo,
    transport::SshTransport,
};

pub fn stream_source_receive(
    runner: &dyn ProcessRunner,
    controller: &ControllerConfig,
    operation: &ControllerRequest,
    local_git: &Path,
    project_id: &str,
    worktree_id: &str,
    oid: &BaseOid,
) -> Result<(), WorkerError> {
    let fingerprint = RequestFingerprint::new(operation.payload_sha256().to_owned())?;
    let prepare = transfer_request(
        "controller.transfer.source.prepare",
        json!({
            "request_id": operation.request_id(),
            "fingerprint": fingerprint.as_str(),
            "project_id": project_id,
            "worktree_id": worktree_id,
            "expected_oid": oid.as_str(),
        }),
    )?;
    let identity =
        send_controller_read::<ControllerSourcePrepareResult>(runner, controller, &prepare)?
            .into_result();
    require_source_bind(
        identity.request_id(),
        identity.fingerprint(),
        identity.project_id(),
        identity.worktree_id(),
        identity.expected_oid(),
        operation,
        project_id,
        worktree_id,
        oid,
    )?;
    let worker = controller_worker_entry(controller)?;
    let ssh = SshTransport::new(runner).git_ssh_command(&worker)?;
    GitTransport::new(runner).push_controller_source(
        &ssh,
        &worker.ssh,
        &worker.remote_binary,
        identity.token(),
        operation.request_id(),
        &fingerprint,
        project_id,
        worktree_id,
        oid,
        local_git,
    )?;
    let finish = transfer_request(
        "controller.transfer.source.finish",
        json!({
            "token": identity.token(),
            "request_id": operation.request_id(),
            "fingerprint": fingerprint.as_str(),
            "project_id": project_id,
            "worktree_id": worktree_id,
            "expected_oid": oid.as_str(),
        }),
    )?;
    let receipt =
        send_controller_read::<ControllerSourceFinishResult>(runner, controller, &finish)?
            .into_result();
    require_receipt_bind(
        receipt.request_id(),
        receipt.oid(),
        receipt.request_ref(),
        operation,
        oid,
    )
}

pub fn fetch_via_controller(
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    controller: &ControllerConfig,
    project: &Path,
    task_id: TaskId,
) -> Result<FetchReport, WorkerError> {
    let probed = ProjectState::load(runner, project, &[])?;
    let prepare = transfer_request(
        "controller.transfer.result.prepare",
        json!({ "task_id": task_id.to_string() }),
    )?;
    let identity =
        send_controller_read::<ControllerResultPrepareResult>(runner, controller, &prepare)?
            .into_result();
    require_result_bind(&identity, task_id, &probed.context.project_id)?;
    let fingerprint = RequestFingerprint::new(identity.fingerprint().to_owned())?;
    let imported_oid: BaseOid = identity
        .imported_oid()
        .parse()
        .map_err(|_| WorkerError::Git {
            code: "RESULT_FETCH_FAILED",
            message: "controller result OID is invalid".into(),
        })?;
    if identity.worker().is_empty() {
        return Err(WorkerError::Git {
            code: "RESULT_FETCH_FAILED",
            message: "controller result has no worker identity".into(),
        });
    }
    let meta = VerifiedResultMeta {
        task_id,
        turn_id: identity.turn_id(),
        imported_oid: imported_oid.clone(),
        worker: identity.worker().to_owned(),
    };
    let laptop_transfer = TransferRepo::open_or_create(&paths.cache, &probed.context.common_dir)?;
    let worker = controller_worker_entry(controller)?;
    let ssh = SshTransport::new(runner).git_ssh_command(&worker)?;
    GitTransport::new(runner).fetch_controller_result(
        &ssh,
        &worker.ssh,
        &worker.remote_binary,
        identity.token(),
        identity.request_id(),
        &fingerprint,
        identity.project_id(),
        task_id,
        identity.turn_id(),
        &imported_oid,
        laptop_transfer.path(),
    )?;
    let imported = import_controller_result(
        &laptop_transfer,
        runner,
        &probed.context.common_dir,
        identity.request_id(),
        &meta,
    )?;
    Ok(FetchReport::from_imported(
        task_id,
        imported.head().clone(),
        imported.local_ref().to_owned(),
    ))
}

fn require_source_bind(
    returned_request_id: &str,
    returned_fingerprint: &str,
    returned_project_id: &str,
    returned_worktree_id: &str,
    returned_oid: &str,
    operation: &ControllerRequest,
    project_id: &str,
    worktree_id: &str,
    oid: &BaseOid,
) -> Result<(), WorkerError> {
    if returned_request_id != operation.request_id()
        || returned_fingerprint != operation.payload_sha256()
        || returned_project_id != project_id
        || returned_worktree_id != worktree_id
        || returned_oid != oid.as_str()
    {
        return Err(source_conflict());
    }
    Ok(())
}

fn require_receipt_bind(
    returned_request_id: &str,
    returned_oid: &str,
    returned_ref: &str,
    operation: &ControllerRequest,
    oid: &BaseOid,
) -> Result<(), WorkerError> {
    let expected_ref = TransferRepo::frozen_request_ref(operation.request_id())?;
    if returned_request_id != operation.request_id()
        || returned_oid != oid.as_str()
        || returned_ref != expected_ref
    {
        return Err(source_conflict());
    }
    Ok(())
}

fn require_result_bind(
    identity: &ControllerResultPrepareResult,
    task_id: TaskId,
    project_id: &str,
) -> Result<(), WorkerError> {
    if identity.task_id() != task_id {
        return Err(WorkerError::Protocol(
            "CONTROLLER_REQUEST_CONFLICT: result identity is not the requested task".into(),
        ));
    }
    if identity.project_id() != project_id {
        return Err(WorkerError::task(
            "PROJECT_MISMATCH",
            "current project is not the task's project",
        ));
    }
    Ok(())
}

fn source_conflict() -> WorkerError {
    WorkerError::Protocol(
        "CONTROLLER_REQUEST_CONFLICT: source identity does not match the frozen submit".into(),
    )
}

fn transfer_request(command: &str, body: Value) -> Result<ControllerRequest, WorkerError> {
    let request_id = format!("{:x}", Uuid::new_v4().simple());
    let payload = serde_json::to_vec(&json!({
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "command": command,
        "body": body,
    }))
    .map_err(|_| {
        WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: controller transfer request could not be encoded".into(),
        )
    })?;
    parse_request(&payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{canonical_request_sha256, encode_json_frame};
    use crate::process::{ProcessRequest, ProcessResult, SystemProcessRunner};
    use crate::protocol::PROTOCOL_VERSION as WIRE_VERSION;
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        fs,
        os::unix::process::ExitStatusExt,
        path::{Path, PathBuf},
        process::ExitStatus,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct ProjectMismatchPrepare {
        inner: SystemProcessRunner,
        data_plane: AtomicUsize,
        project_id: String,
        task_id: String,
    }

    impl ProcessRunner for ProjectMismatchPrepare {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            if request.program == OsString::from("/usr/bin/ssh") {
                let payload =
                    crate::controller::decode_frame(request.stdin.as_ref().unwrap()).unwrap();
                let parsed: Value = serde_json::from_slice(payload).unwrap();
                let command = parsed["command"].as_str().unwrap();
                assert_eq!(command, "controller.transfer.result.prepare");
                let request_id = parsed["request_id"].as_str().unwrap();
                let body = parsed["body"].clone();
                let digest = canonical_request_sha256(WIRE_VERSION, command, &body).unwrap();
                let reply = json!({
                    "protocol_version": WIRE_VERSION,
                    "command": command,
                    "request_id": request_id,
                    "payload_sha256": digest,
                    "result": {
                        "token": "018f0f4a6b5c7d8e9f00112233445566",
                        "request_id": "018f0f4a6b5c7d8e9f00112233445577",
                        "fingerprint": "ab".repeat(32),
                        "project_id": self.project_id,
                        "worktree_id": "bb".repeat(32),
                        "task_id": self.task_id,
                        "turn_id": "018f0f4a6b5c7d8e9f00112233445588",
                        "imported_oid": "0123456789abcdef0123456789abcdef01234567",
                        "worker": "mini-1",
                    }
                });
                return Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: encode_json_frame(&reply).unwrap(),
                    stderr: Vec::new(),
                });
            }
            if request.args.iter().any(|arg| {
                let text = arg.to_string_lossy();
                text.contains("controller-upload-pack") || text.contains("controller-receive-pack")
            }) {
                self.data_plane.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.run(request)
        }
    }

    fn committed_project(root: &Path) -> PathBuf {
        let project = root.join("project");
        fs::create_dir_all(&project).unwrap();
        let runner = SystemProcessRunner;
        for args in [
            ["init", "-q"].as_slice(),
            ["config", "user.name", "mac-worker"].as_slice(),
            ["config", "user.email", "mac-worker@localhost"].as_slice(),
        ] {
            git(&runner, &project, args);
        }
        fs::write(project.join("README"), b"project-b\n").unwrap();
        git(&runner, &project, &["add", "README"]);
        git(&runner, &project, &["commit", "-q", "-m", "init"]);
        project
    }

    fn git(runner: &SystemProcessRunner, cwd: &Path, args: &[&str]) {
        let mut command = vec![OsString::from("-C"), cwd.as_os_str().to_os_string()];
        command.extend(args.iter().map(OsString::from));
        let result = runner
            .run(&ProcessRequest {
                program: OsString::from("/usr/bin/git"),
                args: command,
                environment: vec![
                    (
                        OsString::from("GIT_CONFIG_GLOBAL"),
                        OsString::from("/dev/null"),
                    ),
                    (OsString::from("GIT_CONFIG_NOSYSTEM"), OsString::from("1")),
                    (OsString::from("GIT_TERMINAL_PROMPT"), OsString::from("0")),
                ],
                environment_remove: vec![
                    OsString::from("GIT_DIR"),
                    OsString::from("GIT_WORK_TREE"),
                    OsString::from("GIT_INDEX_FILE"),
                ],
                stdin: None,
                policy: crate::process::ProcessPolicy {
                    stdout_limit: 64 * 1024,
                    stderr_limit: 64 * 1024,
                    deadline: std::time::Duration::from_secs(30),
                },
                isolate_parent_environment: false,
            })
            .unwrap();
        assert!(result.status.success());
    }

    #[test]
    fn source_prepare_must_match_original_operation_before_push() {
        let operation = parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": WIRE_VERSION,
                "request_id": "018f0f4a6b5c7d8e9f00112233445566",
                "command": "task.submit",
                "body": { "prompt": "frozen" }
            }))
            .unwrap(),
        )
        .unwrap();
        let oid = "0123456789abcdef0123456789abcdef01234567"
            .parse::<BaseOid>()
            .unwrap();
        let project = "aa".repeat(32);
        let worktree = "bb".repeat(32);
        assert!(
            require_source_bind(
                operation.request_id(),
                "cd".repeat(32).as_str(),
                &project,
                &worktree,
                oid.as_str(),
                &operation,
                &project,
                &worktree,
                &oid,
            )
            .is_err()
        );
        assert!(
            require_source_bind(
                operation.request_id(),
                operation.payload_sha256(),
                &project,
                &worktree,
                oid.as_str(),
                &operation,
                &project,
                &worktree,
                &oid,
            )
            .is_ok()
        );
    }

    #[test]
    fn fetch_project_mismatch_does_not_fetch_or_import() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let project = committed_project(root);
        let live = ProjectState::load(&SystemProcessRunner, &project, &[]).unwrap();
        let other_project = "aa".repeat(32);
        assert_ne!(live.context.project_id, other_project);
        let task_id = TaskId::generate();
        let runner = ProjectMismatchPrepare {
            inner: SystemProcessRunner,
            data_plane: AtomicUsize::new(0),
            project_id: other_project,
            task_id: task_id.to_string(),
        };
        let mut env = BTreeMap::new();
        env.insert(OsString::from("HOME"), root.join("home").into_os_string());
        fs::create_dir_all(root.join("home")).unwrap();
        let paths = PathLayout::discover(None, &env, &root.join("home")).unwrap();
        let controller = crate::config::ControllerConfig {
            enabled: true,
            ssh: "fakecontroller".into(),
            remote_binary: "~/.local/bin/worker".into(),
        };
        let error =
            fetch_via_controller(&runner, &paths, &controller, &project, task_id).unwrap_err();
        assert!(
            format!("{error:?}").contains("PROJECT_MISMATCH"),
            "{error:?}"
        );
        assert_eq!(runner.data_plane.load(Ordering::SeqCst), 0);
        assert!(!project.join(".git/refs/remotes/mac-worker").exists());
        let cache_entries = fs::read_dir(&paths.cache)
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(cache_entries, 0);
    }
}
