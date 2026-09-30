//! Real `task.submit` execution for controller RPC.
//!
//! `checkpoint.submit` stays on the CP1 fake executor. Ordinary submit maps
//! through the thin `TaskClient::submit_prepared` wrapper (`run_id = None`)
//! after durable object registration. Frozen create/enqueue/intent stays on
//! the DAG common transaction. SSH is the laptop transport; this module runs
//! on the controller.

use std::{
    ffi::OsString,
    fs,
    io::{self, Read, Write},
    path::Path,
    time::Duration,
};

use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    client_state::ClientStateStore,
    config::Config,
    controller::{
        batch::{
            BatchExecuteContext, ControllerCheckoutMap, PreparedTaskBatch, execute_task_batch,
            prepare_task_batch,
        },
        leader::now_millis,
        lifecycle::{is_lifecycle_command, serve_lifecycle_command},
        protocol::ControllerRequest,
        read::ControllerTaskStatusResult,
        read::{
            ControllerReadIdentity, invalid_controller_reply, is_read_command, serve_read_command,
        },
        registry::{self, OwnedCheckoutMap, ProjectRegistry},
        store::{
            ActiveResumeConfig, ControllerAck, ControllerCommandHandler, ControllerFault,
            ControllerStore, OperationMeta, rejection_result,
        },
        stream_rpc::{is_transfer_command, serve_transfer_command},
        task_mutations::{
            PreparedTaskMutation, execute_task_mutation, mutation_task_id, prepare_task_mutation,
        },
        transfer::{ControllerTransfer, SourceSubmitBind},
    },
    error::WorkerError,
    job::{HostControlError, RequestFingerprint},
    paths::PathLayout,
    prepared_submit::FrozenSubmitBody,
    process::{ProcessPolicy, ProcessRequest, ProcessResult, ProcessRunner},
    task_client::{TaskClient, validate_close_target},
    transfer_repo::TransferRepo,
    turn_runner::DetachedRunnerExecutor,
};

const GIT_PROGRAM: &str = "/usr/bin/git";
const GIT_OUTPUT_LIMIT: usize = 64 * 1024;
const GIT_DEADLINE: Duration = Duration::from_secs(60);

static DETACHED_EXECUTOR: DetachedRunnerExecutor = DetachedRunnerExecutor;

// Bounded selected recovery at prepare: retire a completed dead runner
// before freezing say/close expected, without starting an automatic answer.
fn recover_selected_task_before_prepare(
    handler: &TaskSubmitHandler<'_>,
    task_id: crate::task::TaskId,
) -> Result<(), WorkerError> {
    let client = TaskClient::new(
        handler.runner,
        handler.config,
        handler.paths,
        handler.client_state,
        &DETACHED_EXECUTOR,
    );
    let _ = client.reconcile_selected_before_mutation(&[task_id])?;
    Ok(())
}

pub struct TaskSubmitHandler<'a> {
    runner: &'a dyn ProcessRunner,
    config: &'a Config,
    paths: &'a PathLayout,
    client_state: &'a ClientStateStore,
}

impl<'a> TaskSubmitHandler<'a> {
    pub fn new(
        runner: &'a dyn ProcessRunner,
        config: &'a Config,
        paths: &'a PathLayout,
        client_state: &'a ClientStateStore,
    ) -> Self {
        Self {
            runner,
            config,
            paths,
            client_state,
        }
    }
}

impl ControllerCommandHandler for TaskSubmitHandler<'_> {
    fn prepare(&self, request: &ControllerRequest) -> Result<OperationMeta, WorkerError> {
        match request.command() {
            "checkpoint.submit" | "task.submit" => {
                crate::controller::default_prepare_operation(request)
            }
            // Recover say/close before freezing expected; cancel stays freeze-only.
            "task.say" | "task.close" => {
                let task_id = mutation_task_id(request)?;
                recover_selected_task_before_prepare(self, task_id)?;
                self.prepare_frozen_mutation(request)
            }
            "task.cancel" => self.prepare_frozen_mutation(request),
            "task.batch" => {
                let transfer = ControllerTransfer::open(&self.paths.controller_state_root())?;
                let prepared = prepare_task_batch(
                    request,
                    &transfer,
                    &self.paths.cache,
                    self.runner,
                    self.config,
                )?;
                let encoded = serde_json::to_value(&prepared).map_err(|_| {
                    WorkerError::Protocol(
                        "CONTROLLER_TRANSPORT: prepared batch could not be encoded".into(),
                    )
                })?;
                Ok(OperationMeta {
                    task_id: prepared.task_id().map(|id| id.to_string()),
                    turn_id: prepared.turn_id().map(|id| id.to_string()),
                    created_at_millis: prepared.created_at_millis(),
                    prepared: encoded,
                })
            }
            other => Err(WorkerError::Protocol(format!(
                "CONTROLLER_TRANSPORT: unsupported controller command {other}"
            ))),
        }
    }

    fn execute(&self, record: &crate::controller::DurableRequest) -> Result<Value, WorkerError> {
        self.execute_typed(record)
    }
}

impl TaskSubmitHandler<'_> {
    // Pure encode of prepare_task_mutation; no recapture.
    fn prepare_frozen_mutation(
        &self,
        request: &ControllerRequest,
    ) -> Result<OperationMeta, WorkerError> {
        let prepared = prepare_task_mutation(request, self.client_state, now_millis()?)?;
        let encoded = serde_json::to_value(&prepared).map_err(|_| {
            WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: prepared mutation could not be encoded".into(),
            )
        })?;
        Ok(OperationMeta {
            task_id: Some(prepared.task_id().to_string()),
            turn_id: prepared.turn_id().map(|id| id.to_string()),
            created_at_millis: prepared.created_at_millis(),
            prepared: encoded,
        })
    }

    pub fn execute_typed(
        &self,
        record: &crate::controller::DurableRequest,
    ) -> Result<Value, WorkerError> {
        match record.command() {
            "checkpoint.submit" => Ok(json!({
                "task_id": record.task_id(),
                "turn_id": record.turn_id(),
            })),
            "task.submit" => {
                self.submit_task(record)?;
                Ok(json!({
                    "task_id": record.task_id(),
                    "turn_id": record.turn_id(),
                }))
            }
            "task.say" | "task.cancel" | "task.close" => self.execute_prepared_mutation(record),
            "task.batch" => self.execute_prepared_batch(record),
            other => Err(WorkerError::Protocol(format!(
                "CONTROLLER_TRANSPORT: unsupported controller command {other}"
            ))),
        }
    }

    fn execute_prepared_mutation(
        &self,
        record: &crate::controller::DurableRequest,
    ) -> Result<Value, WorkerError> {
        let prepared: PreparedTaskMutation = serde_json::from_value(record.prepared().clone())
            .map_err(|_| {
                WorkerError::Protocol("CONTROLLER_TRANSPORT: prepared mutation is invalid".into())
            })?;
        if let PreparedTaskMutation::Close { expected, .. } = &prepared {
            let current = self.client_state.load_task(expected.meta().task_id())?;
            if let Err(error) = validate_close_target(&current, expected) {
                // Preserve the read-only close rejection, including terminal
                // targets. Execution keeps unfinished effects retryable and
                // the store settles definitive typed revision conflicts.
                return Ok(rejection_result(&error));
            }
        }
        let client = TaskClient::new(
            self.runner,
            self.config,
            self.paths,
            self.client_state,
            &DETACHED_EXECUTOR,
        );
        let report = match execute_task_mutation(&client, &prepared) {
            Err(
                error @ WorkerError::Task {
                    code: "TASK_REVISION_CONFLICT",
                    ..
                },
            ) => {
                if let PreparedTaskMutation::Say { prepared } = &prepared {
                    match client.completed_prepared_followup(prepared)? {
                        Some(report) => report,
                        None => return Err(error),
                    }
                } else {
                    return Err(error);
                }
            }
            result => result?,
        };
        serde_json::to_value(ControllerTaskStatusResult::from_report(&report)).map_err(|_| {
            WorkerError::Protocol(
                "CONTROLLER_TRANSPORT: mutation result could not be encoded".into(),
            )
        })
    }

    fn execute_prepared_batch(
        &self,
        record: &crate::controller::DurableRequest,
    ) -> Result<Value, WorkerError> {
        let prepared: PreparedTaskBatch = serde_json::from_value(record.prepared().clone())
            .map_err(|_| {
                WorkerError::Protocol("CONTROLLER_TRANSPORT: prepared batch is invalid".into())
            })?;
        let registry = ProjectRegistry::open(&self.paths.controller_state_root())?;
        bind_prepared_batch_nodes(
            &registry,
            &prepared,
            record.request_id(),
            record.payload_sha256(),
        )?;
        for source in prepared.sources() {
            materialize_controller_checkout(
                self.runner,
                self.paths,
                source.request_id(),
                source.project_id(),
                source.worktree_id(),
                source.expected_oid(),
            )?;
        }
        let identities: Vec<(String, String)> = prepared
            .nodes()
            .values()
            .map(|node| {
                (
                    node.frozen.project_id.clone(),
                    node.frozen.worktree_id.clone(),
                )
            })
            .collect();
        let owned = OwnedCheckoutMap::from_identities(&registry, identities)?;
        let mut checkouts = ControllerCheckoutMap::new();
        for (project_id, worktree_id, path) in owned.iter() {
            checkouts.insert(project_id, worktree_id, path.to_path_buf());
        }
        let client = TaskClient::new(
            self.runner,
            self.config,
            self.paths,
            self.client_state,
            &DETACHED_EXECUTOR,
        );
        let ctx = BatchExecuteContext {
            client: &client,
            store: self.client_state,
            paths: self.paths,
            runner: self.runner,
            checkouts: &checkouts,
        };
        let report = execute_task_batch(&ctx, &prepared)?;
        Ok(json!({
            "run_id": report.run_id().to_string(),
            "task_ids": report.task_ids(),
        }))
    }

    fn submit_task(&self, record: &crate::controller::DurableRequest) -> Result<(), WorkerError> {
        let body: FrozenSubmitBody =
            serde_json::from_value(record.body().clone()).map_err(|_| {
                WorkerError::Protocol(
                    "CONTROLLER_TRANSPORT: task.submit body is not a frozen submit".into(),
                )
            })?;
        let expected_task = body.task_id.to_string();
        let expected_turn = body.turn_id.to_string();
        if record.task_id() != Some(expected_task.as_str())
            || record.turn_id() != Some(expected_turn.as_str())
        {
            return Err(WorkerError::Protocol(
                "CONTROLLER_REQUEST_CONFLICT: frozen task IDs do not match the durable row".into(),
            ));
        }
        if body.run_id.is_some() {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "ordinary controller submit must not carry a run_id",
            ));
        }
        let fingerprint = RequestFingerprint::new(record.payload_sha256().to_owned())?;
        let transfer = ControllerTransfer::open(&self.paths.controller_state_root())?;
        let receipt = transfer.bind_source_for_submit(
            &self.paths.cache,
            self.runner,
            SourceSubmitBind {
                request_id: record.request_id(),
                fingerprint: &fingerprint,
                project_id: &body.project_id,
                worktree_id: &body.worktree_id,
                expected_oid: &body.base_oid,
            },
        )?;
        if receipt.oid().as_str() != body.base_oid.as_str() {
            return Err(WorkerError::Protocol(
                "CONTROLLER_REQUEST_CONFLICT: source receipt does not match the frozen base".into(),
            ));
        }
        let checkout =
            materialize_frozen_checkout(self.runner, self.paths, record.request_id(), &body)?;
        ProjectRegistry::open(&self.paths.controller_state_root())?.bind_task_request(
            body.task_id,
            record.request_id(),
            record.payload_sha256(),
        )?;
        let prepared = body.prepared()?;
        let client = TaskClient::new(
            self.runner,
            self.config,
            self.paths,
            self.client_state,
            &DETACHED_EXECUTOR,
        );
        let mut stdout = io::sink();
        let mut stderr = io::sink();
        client.submit_prepared(&prepared, &checkout, true, true, &mut stdout, &mut stderr)?;
        Ok(())
    }
}

fn bind_prepared_batch_nodes(
    registry: &ProjectRegistry,
    prepared: &PreparedTaskBatch,
    request_id: &str,
    payload_sha256: &str,
) -> Result<(), WorkerError> {
    for node in prepared.nodes().values() {
        registry.bind_task_request(node.task_id, request_id, payload_sha256)?;
    }
    Ok(())
}

fn materialize_frozen_checkout(
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    request_id: &str,
    body: &FrozenSubmitBody,
) -> Result<std::path::PathBuf, WorkerError> {
    materialize_controller_checkout(
        runner,
        paths,
        request_id,
        &body.project_id,
        &body.worktree_id,
        &body.base_oid,
    )
}

fn materialize_controller_checkout(
    runner: &dyn ProcessRunner,
    paths: &PathLayout,
    request_id: &str,
    project_id: &str,
    worktree_id: &str,
    base_oid: &crate::task::BaseOid,
) -> Result<std::path::PathBuf, WorkerError> {
    let checkout =
        registry::checkout_path(&paths.controller_project_root(), project_id, worktree_id)?;
    registry::ensure_checkout_dir(&checkout)?;
    let git_dir = checkout.join(".git");
    if !git_dir.is_dir() {
        git_in(runner, &checkout, &["init", "-q"])?;
    }
    let transfer =
        TransferRepo::open_or_create_controller_cache(&paths.cache, project_id, worktree_id)?;
    let source_ref = TransferRepo::frozen_request_ref(request_id)?;
    let checkout_ref = frozen_checkout_ref(request_id)?;
    let transfer_path = transfer.path().to_str().ok_or_else(|| WorkerError::Git {
        code: "BASE_UNAVAILABLE",
        message: "transfer repository path is not UTF-8".into(),
    })?;
    git_with_dir(
        runner,
        &git_dir,
        None,
        &[
            "-c",
            "core.fsyncObjectFiles=true",
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            transfer_path,
            &format!("{source_ref}:{checkout_ref}"),
        ],
    )?;
    let worktree = checkout.join("requests").join(request_id);
    ensure_request_worktree(runner, &git_dir, &worktree, &checkout_ref, base_oid)?;
    let registry = ProjectRegistry::open(&paths.controller_state_root())?;
    registry.register(project_id, worktree_id, &checkout)?;
    Ok(worktree)
}

fn frozen_checkout_ref(request_id: &str) -> Result<String, WorkerError> {
    let _ = TransferRepo::frozen_request_ref(request_id)?;
    Ok(format!("refs/mac-worker/checkouts/{request_id}"))
}

fn ensure_request_worktree(
    runner: &dyn ProcessRunner,
    git_dir: &Path,
    worktree: &Path,
    checkout_ref: &str,
    oid: &crate::task::BaseOid,
) -> Result<(), WorkerError> {
    fs::create_dir_all(worktree.parent().ok_or_else(|| {
        WorkerError::task(
            "TASK_CONFIG_INVALID",
            "controller request worktree is missing a parent",
        )
    })?)
    .map_err(WorkerError::Io)?;
    if worktree.join(".git").is_file() || worktree.join(".git").is_dir() {
        git_in(
            runner,
            worktree,
            &["checkout", "-f", "--detach", oid.as_str()],
        )?;
        return Ok(());
    }
    if worktree.exists() {
        let empty = match fs::read_dir(worktree) {
            Ok(entries) => entries.count() == 0,
            Err(error) => return Err(WorkerError::Io(error)),
        };
        if empty {
            fs::remove_dir(worktree).map_err(WorkerError::Io)?;
        } else {
            return Err(WorkerError::task(
                "TASK_CONFIG_INVALID",
                "controller request worktree exists and is not a git worktree",
            ));
        }
    }
    let worktree_utf8 = worktree.to_str().ok_or_else(|| WorkerError::Git {
        code: "BASE_UNAVAILABLE",
        message: "controller request worktree path is not UTF-8".into(),
    })?;
    git_with_dir(
        runner,
        git_dir,
        None,
        &["worktree", "add", "--detach", worktree_utf8, checkout_ref],
    )?;
    Ok(())
}

fn git_in(runner: &dyn ProcessRunner, cwd: &Path, args: &[&str]) -> Result<(), WorkerError> {
    let mut command = vec![OsString::from("-C"), cwd.as_os_str().to_os_string()];
    command.extend(args.iter().map(OsString::from));
    run_git(runner, command)
}

fn git_with_dir(
    runner: &dyn ProcessRunner,
    git_dir: &Path,
    work_tree: Option<&Path>,
    args: &[&str],
) -> Result<(), WorkerError> {
    let mut command = vec![
        OsString::from("--git-dir"),
        git_dir.as_os_str().to_os_string(),
    ];
    if let Some(work_tree) = work_tree {
        command.push(OsString::from("--work-tree"));
        command.push(work_tree.as_os_str().to_os_string());
    }
    command.extend(args.iter().map(OsString::from));
    run_git(runner, command)
}

fn run_git(runner: &dyn ProcessRunner, args: Vec<OsString>) -> Result<(), WorkerError> {
    let request = ProcessRequest {
        program: OsString::from(GIT_PROGRAM),
        args,
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
        policy: ProcessPolicy {
            stdout_limit: GIT_OUTPUT_LIMIT,
            stderr_limit: GIT_OUTPUT_LIMIT,
            deadline: GIT_DEADLINE,
        },
        isolate_parent_environment: false,
    };
    let result = runner.run(&request)?;
    if result.status.success() {
        Ok(())
    } else {
        Err(WorkerError::Git {
            code: "BASE_UNAVAILABLE",
            message: "a controller checkout Git command failed".into(),
        })
    }
}

pub fn serve_rpc_with_runtime(
    paths: &PathLayout,
    config: &Config,
    runner: &dyn ProcessRunner,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    fault: ControllerFault,
) -> Result<(), WorkerError> {
    let payload = crate::controller::protocol::read_frame(stdin)?;
    let request = crate::controller::protocol::parse_request(&payload)?;
    let frame = if crate::controller::health_read::is_health_read(&request) {
        crate::controller::health_read::serve_health_read(&request, &paths.controller_state_root())?
    } else if request.command() == "controller.drain" {
        crate::controller::control::serve_drain(&request, &paths.controller_state_root())?
    } else if is_read_command(request.command()) {
        let client_state = ClientStateStore::open(&paths.state)?;
        let client = TaskClient::new(runner, config, paths, &client_state, &DETACHED_EXECUTOR);
        serve_read_command(&request, &client)?
    } else if is_lifecycle_command(request.command()) {
        let client_state = ClientStateStore::open(&paths.state)?;
        let client = TaskClient::new(runner, config, paths, &client_state, &DETACHED_EXECUTOR);
        serve_lifecycle_command(&request, &client)?
    } else if is_transfer_command(request.command()) {
        serve_transfer_command(&request, paths, runner)?
    } else if request.command() == "checkpoint.submit" {
        let store = ControllerStore::open(&paths.controller_state_root())?;
        let ack = store.handle(&request, fault)?;
        crate::controller::protocol::encode_json_frame(&ack)?
    } else {
        let store = ControllerStore::open(&paths.controller_state_root())?;
        let client_state = ClientStateStore::open(&paths.state)?;
        let handler = TaskSubmitHandler::new(runner, config, paths, &client_state);
        let ack = store.handle_with(&request, &handler, fault)?;
        crate::controller::protocol::encode_json_frame(&ack)?
    };
    stdout.write_all(&frame).map_err(WorkerError::Io)?;
    stdout.flush().map_err(WorkerError::Io)?;
    Ok(())
}

pub fn send_controller_request(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    request: &ControllerRequest,
) -> Result<ControllerAck, WorkerError> {
    let ack: ControllerAck =
        decode_controller_json(&exchange_controller_rpc(runner, controller, request)?)?;
    verify_controller_ack(&ack, request)?;
    Ok(ack)
}

enum MutationOutcome {
    Acknowledged(ControllerAck),
    Rejected(WorkerError),
    Ambiguous(WorkerError),
    Resumable(WorkerError),
    UnverifiedAck(WorkerError),
}

/// Classify evidence, not public error strings: a typed remote rejection can
/// have the same code as a lost connection. Runner errors have no send-stage
/// information (including I/O), so absence of a reply is conservatively unknown.
fn classify_mutation_exchange(
    request: &ControllerRequest,
    result: Result<ProcessResult, WorkerError>,
) -> MutationOutcome {
    let result = match result {
        Ok(result) => result,
        Err(error) => return MutationOutcome::Ambiguous(error),
    };
    if result.stdout.is_empty() {
        return MutationOutcome::Ambiguous(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host did not complete the request".into(),
        ));
    }
    let reply = match crate::controller::decode_frame(&result.stdout) {
        Ok(reply) => reply,
        Err(_) => return MutationOutcome::Ambiguous(invalid_controller_reply()),
    };
    if let Ok(error) = serde_json::from_slice::<HostControlError>(reply) {
        if error.error().resumable() {
            return MutationOutcome::Resumable(host_control_to_worker(&error));
        }
        return MutationOutcome::Rejected(host_control_to_worker(&error));
    }
    let ack: ControllerAck = match serde_json::from_slice(reply) {
        Ok(ack) => ack,
        Err(_) => return MutationOutcome::Ambiguous(invalid_controller_reply()),
    };
    match verify_controller_ack(&ack, request) {
        Ok(()) => MutationOutcome::Acknowledged(ack),
        Err(error) => MutationOutcome::UnverifiedAck(error),
    }
}

/// Persist once, retry only the final RPC with byte-identical input, and mark
/// definitive replies. Source streaming is deliberately the caller's job.
pub fn send_controller_mutation(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    cache_root: &Path,
    request: &ControllerRequest,
    stderr: &mut dyn Write,
) -> Result<ControllerAck, WorkerError> {
    send_controller_mutation_with_wait(
        runner,
        controller,
        cache_root,
        request,
        stderr,
        &mut std::thread::sleep,
    )
}

fn send_controller_mutation_with_wait(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    cache_root: &Path,
    request: &ControllerRequest,
    stderr: &mut dyn Write,
    wait: &mut dyn FnMut(Duration),
) -> Result<ControllerAck, WorkerError> {
    use crate::controller::envelope::{
        OperationOutcome, persist_operation_envelope, settle_operation_envelope,
    };

    // Local preparation failures are definitive: no process has been started.
    let ssh = controller_rpc_request(controller, request)?;
    persist_operation_envelope(cache_root, request)?;
    for attempt in 0..=3 {
        match classify_mutation_exchange(request, runner.run(&ssh)) {
            MutationOutcome::Acknowledged(ack) => {
                if let Err(error) =
                    settle_operation_envelope(cache_root, request, OperationOutcome::Acknowledged)
                {
                    let _ = writeln!(
                        stderr,
                        "controller request {}: acknowledged; the local envelope could not be marked settled ({})",
                        request.request_id(),
                        error.public_code(),
                    );
                    let _ = stderr.flush();
                }
                return Ok(ack);
            }
            MutationOutcome::Rejected(error) => {
                // Cache failures must not replace the controller's definitive rejection.
                let _ = settle_operation_envelope(
                    cache_root,
                    request,
                    OperationOutcome::Rejected {
                        code: error.public_code(),
                    },
                );
                return Err(error);
            }
            MutationOutcome::UnverifiedAck(error) => return Err(error),
            MutationOutcome::Ambiguous(error) | MutationOutcome::Resumable(error)
                if attempt < 3 =>
            {
                // Diagnostics must not stop recovery if stderr is closed.
                let _ = writeln!(
                    stderr,
                    "controller request {}: outcome unknown ({}); retrying {}/3",
                    request.request_id(),
                    error.public_code(),
                    attempt + 1
                );
                let _ = stderr.flush();
                let base_millis = [1000, 3000, 9000][attempt];
                let jitter = (uuid::Uuid::new_v4().as_u128() % 501) as u64 + 750;
                wait(Duration::from_millis(base_millis * jitter / 1000));
            }
            MutationOutcome::Resumable(error) => {
                let _ = writeln!(
                    stderr,
                    "controller request {} is published; the controller will finish it; check `worker task list`",
                    request.request_id()
                );
                let _ = stderr.flush();
                return Err(error);
            }
            MutationOutcome::Ambiguous(error) => {
                let _ = writeln!(
                    stderr,
                    "controller request {} outcome is unknown; check `worker task list` or run `worker controller retry {}`",
                    request.request_id(),
                    request.request_id()
                );
                let _ = stderr.flush();
                return Err(error);
            }
        }
    }
    unreachable!("the final attempt returns its outcome")
}

pub fn send_controller_read<T: DeserializeOwned + ControllerReadIdentity>(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    request: &ControllerRequest,
) -> Result<crate::controller::read::ControllerReadReply<T>, WorkerError> {
    let reply: crate::controller::read::ControllerReadReply<T> =
        decode_controller_json(&exchange_controller_rpc(runner, controller, request)?)?;
    reply.verify_envelope(request)?;
    reply.result().verify_payload(request)?;
    Ok(reply)
}

fn verify_controller_ack(
    ack: &ControllerAck,
    request: &ControllerRequest,
) -> Result<(), WorkerError> {
    if ack.protocol_version() != crate::protocol::PROTOCOL_VERSION
        || ack.request_id() != request.request_id()
        || ack.payload_sha256() != request.payload_sha256()
    {
        return Err(invalid_controller_reply());
    }
    if let Some(task_id) = request
        .body()
        .get("task_id")
        .and_then(serde_json::Value::as_str)
        && ack_id(ack.task_id()) != Some(task_id)
    {
        return Err(invalid_controller_reply());
    }
    if let Some(turn_id) = request
        .body()
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        && ack_id(ack.turn_id()) != Some(turn_id)
    {
        return Err(invalid_controller_reply());
    }
    Ok(())
}

fn ack_id(value: Option<&str>) -> Option<&str> {
    value
}

fn exchange_controller_rpc(
    runner: &dyn ProcessRunner,
    controller: &crate::config::ControllerConfig,
    request: &ControllerRequest,
) -> Result<Vec<u8>, WorkerError> {
    let ssh = controller_rpc_request(controller, request)?;
    let result = runner.run(&ssh)?;
    decode_controller_stdout(&result)
}

fn controller_rpc_request(
    controller: &crate::config::ControllerConfig,
    request: &ControllerRequest,
) -> Result<ProcessRequest, WorkerError> {
    let mut ssh = crate::controller::controller_rpc_ssh_request(controller)?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol_version": crate::protocol::PROTOCOL_VERSION,
        "request_id": request.request_id(),
        "command": request.command(),
        "body": request.body(),
    }))
    .map_err(|_| {
        WorkerError::Protocol(
            "CONTROLLER_TRANSPORT: controller request could not be encoded".into(),
        )
    })?;
    ssh.stdin = Some(crate::controller::encode_frame(&payload)?);
    Ok(ssh)
}

fn decode_controller_stdout(result: &ProcessResult) -> Result<Vec<u8>, WorkerError> {
    if result.stdout.is_empty() {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host did not complete the request".into(),
        ));
    }
    let reply = crate::controller::decode_frame(&result.stdout).map_err(|_| {
        WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
        )
    })?;
    if let Ok(error) = serde_json::from_slice::<HostControlError>(reply) {
        return Err(host_control_to_worker(&error));
    }
    if !result.status.success() {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host did not complete the request".into(),
        ));
    }
    Ok(reply.to_vec())
}

fn decode_controller_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WorkerError> {
    serde_json::from_slice(bytes).map_err(|_| {
        WorkerError::Unavailable(
            "CONTROLLER_UNAVAILABLE: controller host returned an invalid ACK".into(),
        )
    })
}

pub(crate) fn host_control_to_worker(error: &HostControlError) -> WorkerError {
    if let Some(decoded) =
        crate::error::error_from_host_category(error.error().code(), error.error().category())
    {
        return decoded;
    }
    let code = error.error().code();
    // Admission rejections must keep their public category across the RPC.
    // Collapsing them into `Protocol` costs the documented capacity exit
    // status 75 and replaces the operator-facing reason with "protocol error".
    if let Some(code) = capacity_code(code) {
        return WorkerError::capacity_public(code, error.error().message().to_owned());
    }
    let message = format!("{code}: {}", error.error().message());
    if code == "CONTROLLER_UNAVAILABLE" {
        WorkerError::Unavailable(message)
    } else {
        WorkerError::Protocol(message)
    }
}

/// Narrow allow-list: only codes the controller raises from admission, so no
/// other error changes category.
fn capacity_code(code: &str) -> Option<&'static str> {
    match code {
        "CAPACITY_BUSY" => Some("CAPACITY_BUSY"),
        "CAPABILITY_MISSING" => Some("CAPABILITY_MISSING"),
        _ => None,
    }
}

/// Leader idle tick: `open()` then once `bootstrap_active_index()`, then
/// every tick `resume_active_bounded`. Bootstrap is idempotent (healthy
/// EEXIST read-back).
pub fn tick_controller_leader(
    store: &ControllerStore,
    handler: &dyn ControllerCommandHandler,
) -> crate::controller::health::ControllerRequestTickReport {
    crate::controller::health::ControllerRequestTickReport {
        bootstrap: store.bootstrap_active_index(),
        resume: store.resume_active_bounded(handler, &ActiveResumeConfig::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Catalogued capacity rejections keep their exit and static public reason
    /// across the controller RPC, without trusting the remote message.
    #[test]
    fn capacity_codes_decode_back_into_capacity_errors() {
        for (code, message) in [
            (
                "CAPACITY_BUSY",
                "no eligible worker currently has an available heavy slot",
            ),
            (
                "CAPABILITY_MISSING",
                "a required capability is not available",
            ),
        ] {
            let wire = HostControlError::new(code, "/Users/alice/PLANTED_HOST_PATH").unwrap();
            let error = host_control_to_worker(&wire);
            assert!(
                matches!(error, WorkerError::Capacity { .. }),
                "{code} decoded as {error:?}"
            );
            assert_eq!(error.public_code(), code);
            assert_eq!(
                error.public_message(),
                message,
                "{code} lost its catalog reason"
            );
            assert_eq!(error.exit_code(), 75, "{code} lost the capacity category");
        }
    }

    /// Known outages use the catalog; uncatalogued errors keep the legacy mapping.
    #[test]
    fn other_host_codes_keep_their_existing_mapping() {
        let unavailable = host_control_to_worker(
            &HostControlError::new("CONTROLLER_UNAVAILABLE", "down").unwrap(),
        );
        assert!(matches!(unavailable, WorkerError::Transport { .. }));
        assert_eq!(unavailable.public_code(), "CONTROLLER_UNAVAILABLE");
        assert_eq!(unavailable.exit_code(), 69);

        let conflict = host_control_to_worker(
            &HostControlError::new("CONTROLLER_REQUEST_CONFLICT", "bound elsewhere").unwrap(),
        );
        assert!(matches!(conflict, WorkerError::Protocol(_)));
        assert_eq!(conflict.public_code(), "CONTROLLER_REQUEST_CONFLICT");
        assert_eq!(conflict.exit_code(), 70);
    }

    /// The same guarantee through the real framed reply path the CLI uses.
    #[test]
    fn framed_capacity_reply_decodes_to_exit_seventy_five() {
        let frame = crate::controller::protocol::encode_json_frame(
            &HostControlError::new(
                "CAPACITY_BUSY",
                "no eligible worker currently has an available heavy slot",
            )
            .unwrap(),
        )
        .unwrap();
        let result = ProcessResult {
            // The controller child exits non-zero alongside the error frame.
            status: <std::process::ExitStatus as std::os::unix::process::ExitStatusExt>::from_raw(
                75 << 8,
            ),
            stdout: frame,
            stderr: Vec::new(),
        };
        let error = decode_controller_stdout(&result).expect_err("a host error frame must fail");
        assert_eq!(error.public_code(), "CAPACITY_BUSY");
        assert_eq!(error.exit_code(), 75);
        assert_eq!(
            error.public_message(),
            "no eligible worker currently has an available heavy slot"
        );
    }

    #[test]
    fn framed_host_error_accepts_unknown_detail_fields() {
        let reply = serde_json::json!({
            "protocol_version": crate::protocol::PROTOCOL_VERSION,
            "error": {
                "code": "TASK_BUSY",
                "message": "/Users/alice/PLANTED_HOST_PATH",
                "future": {"nested": [true]},
            },
        });
        let result = ProcessResult {
            status: <std::process::ExitStatus as std::os::unix::process::ExitStatusExt>::from_raw(
                64 << 8,
            ),
            stdout: crate::controller::protocol::encode_json_frame(&reply).unwrap(),
            stderr: Vec::new(),
        };
        let error = decode_controller_stdout(&result).expect_err("host error frame");
        assert_eq!(error.public_code(), "TASK_BUSY");
        assert_eq!(error.exit_code(), 64);
        assert!(!crate::error::operator_diagnostic(&error).contains("PLANTED"));
    }
    use crate::agent::{AgentKind, PermissionPolicy};
    use crate::dag::{DagBase, DagFrozenSpec, DagNode, DagNodeState};
    use crate::task::{GitIdentity, PublishMode};
    use crate::{
        paths::PathLayout,
        process::SystemProcessRunner,
        project_state::ProjectState,
        task::{
            BaseOid, ClosePolicy, RunId, TaskId, TaskLimits, TaskMeta, TaskMetaInput, TaskSource,
            TurnId,
        },
        transfer_repo::TransferRepo,
    };
    use std::os::unix::fs::PermissionsExt;

    const REQUEST_A: &str = "018f0f4a6b5c7d8e9f00112233445566";
    const REQUEST_B: &str = "018f0f4a6b5c7d8e9f00112233445577";
    const PROJECT_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const WORKTREE_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OTHER_WORKTREE_ID: &str =
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    fn layout(root: &Path) -> PathLayout {
        PathLayout {
            config: root.join("config.toml"),
            state: root.join("state/mac-worker"),
            cache: root.join("cache/mac-worker"),
            data: root.join("data/mac-worker"),
        }
    }

    fn git_stdout(dir: &Path, args: &[&str]) -> String {
        let runner = SystemProcessRunner;
        let mut command = vec![OsString::from("-C"), dir.as_os_str().to_os_string()];
        command.extend(args.iter().map(OsString::from));
        let result = runner
            .run(&crate::process::ProcessRequest {
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
                policy: ProcessPolicy {
                    stdout_limit: GIT_OUTPUT_LIMIT,
                    stderr_limit: GIT_OUTPUT_LIMIT,
                    deadline: GIT_DEADLINE,
                },
                isolate_parent_environment: false,
            })
            .unwrap();
        assert!(
            result.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8(result.stdout).unwrap()
    }

    fn committed_source(root: &Path, name: &str, contents: &[u8]) -> (std::path::PathBuf, BaseOid) {
        let user = root.join(name);
        fs::create_dir_all(&user).unwrap();
        let runner = SystemProcessRunner;
        git_in(&runner, &user, &["init", "-q"]).unwrap();
        git_in(&runner, &user, &["config", "user.name", "mac-worker"]).unwrap();
        git_in(
            &runner,
            &user,
            &["config", "user.email", "mac-worker@localhost"],
        )
        .unwrap();
        fs::write(user.join("README"), contents).unwrap();
        git_in(&runner, &user, &["add", "README"]).unwrap();
        git_in(&runner, &user, &["commit", "-q", "-m", "init"]).unwrap();
        let oid = git_stdout(&user, &["rev-parse", "HEAD"])
            .chars()
            .filter(|ch| *ch != '\n')
            .collect::<String>()
            .parse::<BaseOid>()
            .unwrap();
        (user.join(".git"), oid)
    }

    fn frozen_body(oid: &BaseOid) -> FrozenSubmitBody {
        FrozenSubmitBody {
            questions: None,
            task_id: TaskId::generate(),
            turn_id: TurnId::generate(),
            run_id: None,
            created_at_millis: 1_700_000_000_000,
            prompt: "freeze this snapshot".into(),
            title: None,
            agent: "codex".into(),
            model: None,
            effort: None,
            source: "local".into(),
            origin_url: None,
            publish: vec!["fetch".into()],
            publish_branch: None,
            close_on: ClosePolicy::Never,
            env_profile: None,
            worker: None,
            wip: true,
            project_id: PROJECT_ID.into(),
            worktree_id: WORKTREE_ID.into(),
            base_oid: oid.clone(),
            timeout_millis: 45 * 60 * 1000,
            max_turns: None,
            max_budget_usd_cents: None,
            max_followups: 10,
            permissions: "workspace".into(),
            requires: Vec::new(),
            include_untracked: Vec::new(),
            include_empty_dirs: Vec::new(),
            allow_sensitive: Vec::new(),
            cli_includes: Vec::new(),
            branch: None,
            wait_for_capacity: true,
        }
    }

    fn seed_pinned_source(paths: &PathLayout, git_dir: &Path, request_id: &str, oid: &BaseOid) {
        let runner = SystemProcessRunner;
        let source =
            TransferRepo::open_or_create(&paths.cache.join("seed-source"), git_dir).unwrap();
        let bundle = source
            .bundle_frozen_source(&runner, request_id, oid)
            .unwrap();
        let transfer =
            TransferRepo::open_or_create_controller_cache(&paths.cache, PROJECT_ID, WORKTREE_ID)
                .unwrap();
        transfer
            .import_frozen_source(&runner, request_id, &bundle, oid)
            .unwrap();
    }

    fn head_oid(path: &Path) -> String {
        git_stdout(path, &["rev-parse", "HEAD"])
            .chars()
            .filter(|ch| *ch != '\n')
            .collect()
    }

    fn readme(path: &Path) -> Vec<u8> {
        fs::read(path.join("README")).unwrap()
    }

    fn waiting_node(batch_id: &str, base: DagBase, depends_on: Vec<String>) -> DagNode {
        DagNode {
            batch_id: batch_id.to_owned(),
            task_id: TaskId::generate(),
            turn_id: TurnId::generate(),
            depends_on,
            base,
            frozen: DagFrozenSpec {
                questions: None,
                prompt: format!("do {batch_id}"),
                title: None,
                agent: "codex".into(),
                model: None,
                effort: None,
                source: "local".into(),
                origin_url: None,
                publish: vec!["fetch".into()],
                publish_branch: None,
                close_on: ClosePolicy::Done,
                env_profile: None,
                worker: None,
                wip: false,
                project_path: "/tmp/mac-worker-batch-bind".into(),
                project_id: PROJECT_ID.into(),
                worktree_id: WORKTREE_ID.into(),
                timeout_millis: 45 * 60 * 1000,
                max_turns: None,
                max_budget_usd_cents: None,
                max_followups: 10,
                permissions: "workspace".into(),
                requires: vec!["agent:codex".into()],
                include_untracked: Vec::new(),
                include_empty_dirs: Vec::new(),
                allow_sensitive: Vec::new(),
                cli_includes: Vec::new(),
                branch: None,
            },
            state: DagNodeState::Waiting,
            bound_oid: None,
            bound_turn_id: None,
            pin_ref: None,
            blocked_by: None,
            claimed_by: None,
            claimed_at_millis: None,
        }
    }

    fn owner_only(path: &Path) {
        fs::create_dir_all(path).unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).unwrap();
    }

    #[test]
    fn frozen_checkout_retry_after_materialization_keeps_the_same_worktree() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let paths = layout(root);
        let runner = SystemProcessRunner;
        let (git_dir, oid) = committed_source(root, "source", b"first\n");
        let body = frozen_body(&oid);
        seed_pinned_source(&paths, &git_dir, REQUEST_A, &oid);
        let first = materialize_frozen_checkout(&runner, &paths, REQUEST_A, &body).unwrap();
        assert_eq!(readme(&first), b"first\n");
        let first_head = head_oid(&first);
        let retry = materialize_frozen_checkout(&runner, &paths, REQUEST_A, &body).unwrap();
        assert_eq!(retry, first);
        assert_eq!(head_oid(&retry), first_head);
        assert_eq!(readme(&retry), b"first\n");
        assert!(!first.join(".git").join("HEAD").exists());
        assert!(
            fs::read_to_string(first.join(".git"))
                .unwrap()
                .contains("gitdir:"),
            "request checkout must be a linked worktree, not the shared main worktree"
        );
    }

    #[test]
    fn questions_absent_controller_field_resolves_at_composition_and_replay_keeps_policy() {
        use crate::task::QuestionsPolicy::{Ask, Decide};
        for (project_policy, override_policy, expected) in [
            (None, None, Decide),
            (Some(Ask), None, Ask),
            (Some(Ask), Some(Decide), Decide),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let paths = layout(&temp.path().canonicalize().unwrap());
            let runner = SystemProcessRunner;
            let (git_dir, oid) = committed_source(temp.path(), "source", b"hello\n");
            let mut body = frozen_body(&oid);
            body.questions = override_policy;
            seed_pinned_source(&paths, &git_dir, REQUEST_A, &oid);
            let checkout = materialize_frozen_checkout(&runner, &paths, REQUEST_A, &body).unwrap();
            if let Some(policy) = project_policy {
                fs::write(
                    checkout.join(".worker.toml"),
                    format!("[task]\nquestions = \"{}\"\n", policy.as_str()),
                )
                .unwrap();
            }
            let state = ClientStateStore::open(&paths.state).unwrap();
            let config = Config::parse("version = 1\nworkers = []\n").unwrap();
            let client = TaskClient::new(
                &runner,
                &config,
                &paths,
                &state,
                &crate::turn_runner::InlineRunnerExecutor,
            );
            let prepared = body.prepared().unwrap();
            client
                .submit_prepared(
                    &prepared,
                    &checkout,
                    true,
                    true,
                    &mut io::sink(),
                    &mut io::sink(),
                )
                .unwrap();
            assert_eq!(
                state.load_task(body.task_id).unwrap().questions_policy(),
                expected
            );
            let prompt = state.read_turn_prompt(body.task_id, body.turn_id).unwrap();
            assert_eq!(prompt.contains("No human is available"), expected == Decide);
            fs::write(
                checkout.join(".worker.toml"),
                "[task]\nquestions = \"ask\"\n",
            )
            .unwrap();
            client
                .submit_prepared(
                    &prepared,
                    &checkout,
                    true,
                    true,
                    &mut io::sink(),
                    &mut io::sink(),
                )
                .unwrap();
            assert_eq!(
                state.load_task(body.task_id).unwrap().questions_policy(),
                expected
            );
        }
    }

    #[test]
    fn two_unrelated_requests_do_not_clobber_a_peer_worktree() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let paths = layout(root);
        let runner = SystemProcessRunner;
        let (first_git, first_oid) = committed_source(root, "first", b"alpha\n");
        let (second_git, second_oid) = committed_source(root, "second", b"beta\n");
        assert_ne!(first_oid.as_str(), second_oid.as_str());
        let first_body = frozen_body(&first_oid);
        let second_body = frozen_body(&second_oid);
        seed_pinned_source(&paths, &first_git, REQUEST_A, &first_oid);
        seed_pinned_source(&paths, &second_git, REQUEST_B, &second_oid);
        let first = materialize_frozen_checkout(&runner, &paths, REQUEST_A, &first_body).unwrap();
        let first_head = head_oid(&first);
        let second = materialize_frozen_checkout(&runner, &paths, REQUEST_B, &second_body).unwrap();
        assert_ne!(first, second);
        assert_eq!(head_oid(&first), first_head);
        assert_eq!(readme(&first), b"alpha\n");
        assert_eq!(readme(&second), b"beta\n");
        assert_eq!(head_oid(&second), second_oid.as_str());
        let retry = materialize_frozen_checkout(&runner, &paths, REQUEST_A, &first_body).unwrap();
        assert_eq!(retry, first);
        assert_eq!(readme(&first), b"alpha\n");
        assert_eq!(readme(&second), b"beta\n");
    }

    #[test]
    fn ordinary_load_for_task_keeps_the_hashed_worktree_id() {
        let temp = tempfile::tempdir().unwrap();
        let runner = SystemProcessRunner;
        let (git_dir, _) = committed_source(temp.path(), "source", b"hello\n");
        let project = git_dir.parent().unwrap();
        let live = ProjectState::load(&runner, project, &[]).unwrap();
        let meta = task_meta(
            &live.context.project_id,
            OTHER_WORKTREE_ID,
            &live.context.head.clone().unwrap().parse().unwrap(),
        );
        let loaded = ProjectState::load_for_task(&runner, project, &[], &meta).unwrap();
        assert_eq!(loaded.context.worktree_id, live.context.worktree_id);
        assert_ne!(loaded.context.worktree_id, OTHER_WORKTREE_ID);
    }

    #[test]
    fn registered_checkout_applies_frozen_worktree_id() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let paths = layout(root);
        let runner = SystemProcessRunner;
        let (git_dir, oid) = committed_source(root, "source", b"hello\n");
        let body = frozen_body(&oid);
        seed_pinned_source(&paths, &git_dir, REQUEST_A, &oid);
        let worktree = materialize_frozen_checkout(&runner, &paths, REQUEST_A, &body).unwrap();
        let meta = task_meta(PROJECT_ID, WORKTREE_ID, &oid);
        let hashed = ProjectState::load_for_task(&runner, &worktree, &[], &meta).unwrap();
        assert_ne!(hashed.context.worktree_id, WORKTREE_ID);
        let remapped =
            registry::load_registered_or_local(&runner, &paths, &worktree, &[], &meta).unwrap();
        assert_eq!(remapped.context.worktree_id, WORKTREE_ID);
        assert_eq!(remapped.context.project_id, PROJECT_ID);
        let cache =
            TransferRepo::open_or_create_controller_cache(&paths.cache, PROJECT_ID, WORKTREE_ID)
                .unwrap();
        let expected =
            TransferRepo::controller_transfer_git_path(&paths.cache, PROJECT_ID, WORKTREE_ID)
                .unwrap();
        assert_eq!(cache.path(), expected.as_path());
        let cache_path = cache.path().to_str().unwrap();
        assert!(cache_path.contains("/controller-transfer/"), "{cache_path}");
        assert!(!cache_path.contains("/transfer/"), "{cache_path}");
    }

    #[test]
    fn mutation_retry_lost_acks_recover_through_cli_and_real_rpc_without_second_task() {
        use crate::controller::{envelope::load_operation_envelope, parse_request};
        use clap::Parser;
        use std::{
            collections::BTreeMap,
            sync::{
                Mutex,
                atomic::{AtomicBool, AtomicUsize, Ordering},
            },
        };
        struct NoExecution;
        impl ProcessRunner for NoExecution {
            fn run(&self, _: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                panic!("saved ACK replay must not re-execute Git or a worker command")
            }
        }
        struct LostAckRpc<'a> {
            paths: &'a PathLayout,
            config: &'a Config,
            lose_ack: AtomicBool,
            attempts: AtomicUsize,
            requests: Mutex<Vec<Vec<u8>>>,
            replies: Mutex<Vec<Vec<u8>>>,
        }
        impl ProcessRunner for LostAckRpc<'_> {
            fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                use std::os::unix::process::ExitStatusExt;
                let request = process.stdin.clone().unwrap();
                self.requests.lock().unwrap().push(request.clone());
                let mut reply = Vec::new();
                let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
                let runner: &dyn ProcessRunner = if attempt == 0 {
                    &SystemProcessRunner
                } else {
                    &NoExecution
                };
                // Reopens the store/handler for every RPC, just like a new SSH child.
                serve_rpc_with_runtime(
                    self.paths,
                    self.config,
                    runner,
                    &mut io::Cursor::new(request),
                    &mut reply,
                    ControllerFault::None,
                )?;
                self.replies.lock().unwrap().push(reply.clone());
                Ok(ProcessResult {
                    status: std::process::ExitStatus::from_raw(
                        if self.lose_ack.load(Ordering::SeqCst) {
                            255 << 8
                        } else {
                            0
                        },
                    ),
                    stdout: if self.lose_ack.load(Ordering::SeqCst) {
                        Vec::new()
                    } else {
                        reply
                    },
                    stderr: Vec::new(),
                })
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let paths = layout(&root);
        let config_text = "version = 1\n[controller]\nenabled = true\nssh = 'fakecontroller'\n";
        let config = Config::parse(config_text).unwrap();
        fs::write(&paths.config, config_text).unwrap();
        let (git_dir, oid) = committed_source(&root, "source", b"one task\n");
        let body = frozen_body(&oid);
        let request = parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": crate::protocol::PROTOCOL_VERSION,
                "request_id": REQUEST_A, "command": "task.submit", "body": body
            }))
            .unwrap(),
        )
        .unwrap();
        seed_pinned_source(&paths, &git_dir, REQUEST_A, &oid);
        let transfer = ControllerTransfer::open(&paths.controller_state_root()).unwrap();
        let receive = transfer
            .prepare_source_receive(
                &paths.cache,
                &SystemProcessRunner,
                REQUEST_A,
                &RequestFingerprint::new(request.payload_sha256().to_owned()).unwrap(),
                PROJECT_ID,
                WORKTREE_ID,
                &oid,
            )
            .unwrap();
        transfer
            .finish_source_receive(&paths.cache, &SystemProcessRunner, &receive)
            .unwrap();
        let rpc = LostAckRpc {
            paths: &paths,
            config: &config,
            lose_ack: AtomicBool::new(true),
            attempts: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            replies: Mutex::new(Vec::new()),
        };
        let mut stderr = Vec::new();
        let mut delays = Vec::new();
        let error = send_controller_mutation_with_wait(
            &rpc,
            &config.controller,
            &paths.controller_cache_root(),
            &request,
            &mut stderr,
            &mut |delay| delays.push(delay),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_UNAVAILABLE");
        assert_eq!(rpc.attempts.load(Ordering::SeqCst), 4);
        assert_eq!(delays.len(), 3);
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains(&format!("worker controller retry {REQUEST_A}"))
        );
        let pending = load_operation_envelope(&paths.controller_cache_root(), REQUEST_A)
            .unwrap()
            .unwrap();
        assert!(pending.settled_at_millis().is_none());
        assert!(
            !paths
                .controller_state_root()
                .join("active")
                .join(format!("{REQUEST_A}.json"))
                .exists()
        );
        let state = ClientStateStore::open(&paths.state).unwrap();
        assert_eq!(state.list_tasks().unwrap().len(), 1);
        let saved_task = state.load_task(body.task_id).unwrap();
        let env = BTreeMap::from([
            (
                OsString::from("XDG_STATE_HOME"),
                root.join("state").into_os_string(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                root.join("cache").into_os_string(),
            ),
            (
                OsString::from("XDG_DATA_HOME"),
                root.join("data").into_os_string(),
            ),
        ]);
        let runtime = crate::RuntimeContext::isolated(env, root.join("home"), root.clone());
        let invoke = |args: &[&str]| {
            let cli = crate::cli::Cli::try_parse_from(
                std::iter::once("worker").chain(args.iter().copied()),
            )
            .unwrap();
            let cli = crate::cli::Cli {
                config: Some(paths.config.clone()),
                ..cli
            };
            let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
            let exit = crate::run_with_io_in_context(cli, &rpc, &runtime, &mut stdout, &mut stderr);
            assert_eq!(exit, 0, "{}", String::from_utf8_lossy(&stderr));
            serde_json::from_slice::<Value>(&stdout).unwrap()
        };
        let listed = invoke(&["controller", "pending", "--json"]);
        assert_eq!(listed["pending"][0]["request_id"], REQUEST_A);
        rpc.lose_ack.store(false, Ordering::SeqCst);
        let recovered = invoke(&["controller", "retry", REQUEST_A, "--json"]);
        assert_eq!(recovered["task_id"], body.task_id.to_string());
        assert_eq!(recovered["turn_id"], body.turn_id.to_string());
        assert_eq!(
            invoke(&["controller", "pending", "--json"]),
            json!({"pending": [], "unreadable": []})
        );
        assert_eq!(state.list_tasks().unwrap().len(), 1);
        assert_eq!(state.load_task(body.task_id).unwrap(), saved_task);
        assert_eq!(rpc.attempts.load(Ordering::SeqCst), 5);
        for frames in [&rpc.requests, &rpc.replies] {
            assert!(
                frames
                    .lock()
                    .unwrap()
                    .windows(2)
                    .all(|pair| pair[0] == pair[1])
            );
        }
    }

    #[test]
    fn frozen_submit_digest_excludes_token_and_bundle() {
        let oid = "0123456789abcdef0123456789abcdef01234567"
            .parse::<BaseOid>()
            .unwrap();
        let body = frozen_body(&oid);
        let value = serde_json::to_value(&body).unwrap();
        assert!(value.get("bundle_base64").is_none());
        assert!(value.get("token").is_none());
        let digest = crate::controller::canonical_request_sha256(
            crate::protocol::PROTOCOL_VERSION,
            "task.submit",
            &value,
        )
        .unwrap();
        assert_eq!(digest.len(), 64);
        assert_ne!(digest, oid.as_str());
    }

    #[test]
    fn prepared_batch_binds_waiting_child_nodes_to_the_outer_request() {
        let temp = tempfile::tempdir().unwrap();
        let paths = layout(temp.path());
        owner_only(&paths.controller_state_root());
        let registry = ProjectRegistry::open(&paths.controller_state_root()).unwrap();
        let run_id = RunId::generate();
        let oid = "0123456789abcdef0123456789abcdef01234567"
            .parse::<BaseOid>()
            .unwrap();
        let root = waiting_node(
            "root",
            DagBase::Frozen {
                oid,
                pin_ref: format!("refs/mac-worker/dag/{run_id}/root"),
                wip: false,
            },
            Vec::new(),
        );
        let child = waiting_node(
            "child",
            DagBase::From {
                parent: "root".into(),
            },
            vec!["root".into()],
        );
        let root_id = root.task_id;
        let child_id = child.task_id;
        let prepared: PreparedTaskBatch = serde_json::from_value(json!({
            "kind": "dag",
            "run_id": run_id,
            "requested_max_parallel": null,
            "max_parallel": 1,
            "created_at_millis": 1,
            "nodes": {
                "root": root,
                "child": child,
            },
            "sources": [],
        }))
        .unwrap();
        let fingerprint = "aa".repeat(32);
        bind_prepared_batch_nodes(&registry, &prepared, REQUEST_A, &fingerprint).unwrap();
        bind_prepared_batch_nodes(&registry, &prepared, REQUEST_A, &fingerprint).unwrap();
        for task_id in [root_id, child_id] {
            let bind = registry
                .lookup_task_request(task_id)
                .unwrap()
                .expect("every prepared node must share the outer batch request");
            assert_eq!(bind.request_id, REQUEST_A);
            assert_eq!(bind.fingerprint, fingerprint);
        }
        assert_eq!(prepared.nodes().len(), 2);
    }

    fn task_meta(project_id: &str, worktree_id: &str, oid: &BaseOid) -> TaskMeta {
        TaskMeta::new(TaskMetaInput {
            task_id: TaskId::generate(),
            run_id: None,
            project_id: project_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            agent: AgentKind::Codex,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Local {
                wip: true,
                push_target: None,
            },
            publish: vec![PublishMode::Fetch],
            publish_branch: None,
            base_oid: oid.clone(),
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "fixture prompt".into(),
            created_at_millis: 1,
        })
        .unwrap()
    }
}

#[cfg(test)]
mod read_route_identity_tests {
    use super::*;
    use crate::controller::{ControllerTaskDiffResult, encode_json_frame, parse_request};
    use crate::process::ProcessResult;
    use crate::protocol::PROTOCOL_VERSION as WIRE_VERSION;
    use serde_json::{Value, json};
    use std::os::unix::process::ExitStatusExt;
    use std::process::ExitStatus;

    struct ScriptedReply {
        mutate: fn(&mut Value),
    }

    impl ProcessRunner for ScriptedReply {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
            let payload = crate::controller::decode_frame(request.stdin.as_ref().unwrap()).unwrap();
            let parsed: Value = serde_json::from_slice(payload).unwrap();
            let command = parsed["command"].as_str().unwrap();
            let request_id = parsed["request_id"].as_str().unwrap();
            let body = parsed["body"].clone();
            let digest =
                crate::controller::canonical_request_sha256(WIRE_VERSION, command, &body).unwrap();
            let mut reply = json!({
                "protocol_version": WIRE_VERSION,
                "command": command,
                "request_id": request_id,
                "payload_sha256": digest,
                "result": {
                    "task_id": body["task_id"],
                    "stat": body["stat"],
                    "text": "patch\n"
                }
            });
            (self.mutate)(&mut reply);
            Ok(ProcessResult {
                status: ExitStatus::from_raw(0),
                stdout: encode_json_frame(&reply).unwrap(),
                stderr: Vec::new(),
            })
        }
    }

    fn enabled_controller() -> crate::config::ControllerConfig {
        crate::config::ControllerConfig {
            enabled: true,
            ssh: "user@always-on-host".into(),
            remote_binary: "~/.local/bin/worker".into(),
        }
    }

    fn diff_request() -> ControllerRequest {
        parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": WIRE_VERSION,
                "request_id": "018f0f4a6b5c7d8e9f00112233445566",
                "command": "task.diff",
                "body": {
                    "task_id": "018f0f4a6b5c7d8e9f00112233445566",
                    "stat": false
                }
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn send_diff(
        mutate: fn(&mut Value),
    ) -> Result<crate::controller::read::ControllerReadReply<ControllerTaskDiffResult>, WorkerError>
    {
        send_controller_read(
            &ScriptedReply { mutate },
            &enabled_controller(),
            &diff_request(),
        )
    }

    #[test]
    fn read_route_rejects_mismatched_controller_replies() {
        assert!(
            send_diff(|reply| {
                reply["protocol_version"] = json!(6);
            })
            .is_err()
        );
        assert!(
            send_diff(|reply| {
                reply["request_id"] = json!("018f0f4a6b5c7d8e9f00112233445577");
            })
            .is_err()
        );
        assert!(
            send_diff(|reply| {
                reply["command"] = json!("task.status");
            })
            .is_err()
        );
        assert!(
            send_diff(|reply| {
                reply["result"]["task_id"] = json!("018f0f4a6b5c7d8e9f00112233445577");
            })
            .is_err()
        );
        assert!(
            send_diff(|reply| {
                reply["payload_sha256"] = json!("deadbeef");
            })
            .is_err()
        );
        assert!(send_diff(|_| {}).is_ok());
    }

    #[test]
    fn read_route_rejects_mismatched_ack_identity() {
        struct AckReply {
            mutate: fn(&mut Value),
        }
        impl ProcessRunner for AckReply {
            fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
                let payload =
                    crate::controller::decode_frame(request.stdin.as_ref().unwrap()).unwrap();
                let parsed: Value = serde_json::from_slice(payload).unwrap();
                let command = parsed["command"].as_str().unwrap();
                let request_id = parsed["request_id"].as_str().unwrap().to_owned();
                let body = parsed["body"].clone();
                let digest =
                    crate::controller::canonical_request_sha256(WIRE_VERSION, command, &body)
                        .unwrap();
                let mut reply = json!({
                    "protocol_version": WIRE_VERSION,
                    "status": "ok",
                    "request_id": request_id,
                    "payload_sha256": digest,
                    "task_id": body["task_id"],
                    "turn_id": body["turn_id"],
                    "created_at_millis": 1
                });
                (self.mutate)(&mut reply);
                Ok(ProcessResult {
                    status: ExitStatus::from_raw(0),
                    stdout: encode_json_frame(&reply).unwrap(),
                    stderr: Vec::new(),
                })
            }
        }
        let request = parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": WIRE_VERSION,
                "request_id": "018f0f4a6b5c7d8e9f00112233445566",
                "command": "task.submit",
                "body": {
                    "task_id": "018f0f4a6b5c7d8e9f00112233445566",
                    "turn_id": "018f0f4a6b5c7d8e9f00112233445577"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let controller = enabled_controller();
        assert!(
            send_controller_request(
                &AckReply {
                    mutate: |reply| {
                        reply["protocol_version"] = json!(6);
                    }
                },
                &controller,
                &request,
            )
            .is_err()
        );
        assert!(
            send_controller_request(
                &AckReply {
                    mutate: |reply| {
                        reply["request_id"] = json!("018f0f4a6b5c7d8e9f00112233445577");
                    }
                },
                &controller,
                &request,
            )
            .is_err()
        );
        assert!(
            send_controller_request(
                &AckReply {
                    mutate: |reply| {
                        reply["payload_sha256"] = json!("deadbeef");
                    }
                },
                &controller,
                &request,
            )
            .is_err()
        );
        assert!(
            send_controller_request(
                &AckReply {
                    mutate: |reply| {
                        reply["task_id"] = json!("018f0f4a6b5c7d8e9f00112233445500");
                    }
                },
                &controller,
                &request,
            )
            .is_err()
        );
        assert!(
            send_controller_request(&AckReply { mutate: |_| {} }, &controller, &request,).is_ok()
        );
    }
}

#[cfg(test)]
mod mutation_retry_tests {
    use super::*;
    use crate::{
        controller::{
            encode_json_frame,
            envelope::{OperationOutcome, load_operation_envelope},
            parse_request,
        },
        error::{ExitKind, ProcessError},
        protocol::PROTOCOL_VERSION,
    };
    use std::{
        collections::VecDeque, os::unix::process::ExitStatusExt, process::ExitStatus, sync::Mutex,
    };

    fn request() -> ControllerRequest {
        parse_request(
            &serde_json::to_vec(&json!({
                "protocol_version": PROTOCOL_VERSION,
                "request_id": "018f0f4a6b5c7d8e9f00112233445566",
                "command": "task.cancel",
                "body": {"task_id": "018f0f4a6b5c7d8e9f00112233445577"}
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn ack(request: &ControllerRequest) -> Value {
        json!({"protocol_version": PROTOCOL_VERSION, "status": "acked",
            "request_id": request.request_id(), "payload_sha256": request.payload_sha256(),
            "task_id": request.body()["task_id"], "created_at_millis": 1,
            "result": {"task_id": request.body()["task_id"]}})
    }

    fn output(stdout: Vec<u8>, exit: i32) -> Result<ProcessResult, WorkerError> {
        Ok(ProcessResult {
            status: ExitStatus::from_raw(exit << 8),
            stdout,
            stderr: b"secret remote detail".to_vec(),
        })
    }

    fn missing() -> Result<ProcessResult, WorkerError> {
        output(Vec::new(), 255)
    }

    struct Replies {
        replies: Mutex<VecDeque<Result<ProcessResult, WorkerError>>>,
        frames: Mutex<Vec<Vec<u8>>>,
    }
    impl Replies {
        fn new(replies: Vec<Result<ProcessResult, WorkerError>>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                frames: Mutex::new(Vec::new()),
            }
        }
    }
    impl ProcessRunner for Replies {
        fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            self.frames
                .lock()
                .unwrap()
                .push(request.stdin.clone().unwrap());
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected retry")
        }
    }

    #[test]
    fn classification_table_distinguishes_rejections_and_unverified_acks() {
        let request = request();
        let good = ack(&request);
        let mut wrong = good.clone();
        wrong["request_id"] = json!("018f0f4a6b5c7d8e9f00112233445599");
        let rejection = HostControlError::new("CONTROLLER_UNAVAILABLE", "typed rejection").unwrap();
        let mut resumable = serde_json::to_value(&rejection).unwrap();
        resumable["error"]["resumable"] = json!(true);
        let mut definitive = resumable.clone();
        definitive["error"]["resumable"] = json!(false);
        for (name, result, expected) in [
            ("ack", output(encode_json_frame(&good).unwrap(), 0), "ack"),
            (
                "ack despite exit",
                output(encode_json_frame(&good).unwrap(), 255),
                "ack",
            ),
            (
                "typed unavailable",
                output(encode_json_frame(&rejection).unwrap(), 69),
                "rejected",
            ),
            ("empty", missing(), "ambiguous"),
            (
                "resumable typed error",
                output(encode_json_frame(&resumable).unwrap(), 69),
                "resumable",
            ),
            (
                "explicit definitive error",
                output(encode_json_frame(&definitive).unwrap(), 69),
                "rejected",
            ),
            ("truncated", output(vec![0, 0, 0, 5, b'{'], 0), "ambiguous"),
            (
                "invalid json",
                output(crate::controller::encode_frame(b"no json").unwrap(), 0),
                "ambiguous",
            ),
            (
                "wrong identity",
                output(encode_json_frame(&wrong).unwrap(), 0),
                "unverified",
            ),
            (
                "deadline",
                Err(ProcessError::DeadlineExceeded {
                    deadline: Duration::from_secs(30),
                }
                .into()),
                "ambiguous",
            ),
            (
                "io stage unknown",
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "pipe").into()),
                "ambiguous",
            ),
        ] {
            let actual = match classify_mutation_exchange(&request, result) {
                MutationOutcome::Acknowledged(_) => "ack",
                MutationOutcome::Rejected(_) => "rejected",
                MutationOutcome::Ambiguous(_) => "ambiguous",
                MutationOutcome::Resumable(_) => "resumable",
                MutationOutcome::UnverifiedAck(_) => "unverified",
            };
            assert_eq!(actual, expected, "{name}");
        }
    }

    fn send(
        runner: &Replies,
        cache: &Path,
        stderr: &mut Vec<u8>,
        delays: &mut Vec<Duration>,
    ) -> Result<ControllerAck, WorkerError> {
        let config =
            Config::parse("version = 1\n[controller]\nenabled = true\nssh = 'fakecontroller'\n")
                .unwrap();
        send_controller_mutation_with_wait(
            runner,
            &config.controller,
            cache,
            &request(),
            stderr,
            &mut |delay| delays.push(delay),
        )
    }

    #[test]
    fn ambiguous_then_ack_retries_identical_frames_and_settles() {
        let temp = tempfile::tempdir().unwrap();
        let runner = Replies::new(vec![
            missing(),
            Err(ProcessError::DeadlineExceeded {
                deadline: Duration::from_secs(30),
            }
            .into()),
            output(encode_json_frame(&ack(&request())).unwrap(), 0),
        ]);
        let (mut stderr, mut delays) = (Vec::new(), Vec::new());
        send(
            &runner,
            &temp.path().join("cache"),
            &mut stderr,
            &mut delays,
        )
        .unwrap();
        let frames = runner.frames.lock().unwrap();
        assert_eq!(frames.len(), 3);
        assert!(frames.windows(2).all(|pair| pair[0] == pair[1]));
        let text = String::from_utf8(stderr).unwrap();
        assert!(text.contains(&format!(
            "controller request {}: outcome unknown (CONTROLLER_UNAVAILABLE); retrying 1/3",
            request().request_id()
        )));
        assert!(text.contains("outcome unknown (PROCESS); retrying 2/3"));
        assert!(!text.contains("secret"));
        for (delay, seconds) in delays.iter().zip([1., 3.]) {
            assert!((seconds * 0.75..=seconds * 1.25).contains(&delay.as_secs_f64()));
        }
        let envelope = load_operation_envelope(&temp.path().join("cache"), request().request_id())
            .unwrap()
            .unwrap();
        assert_eq!(envelope.outcome(), Some(&OperationOutcome::Acknowledged));
    }

    #[test]
    fn rejection_is_not_retried_and_preserves_category() {
        let temp = tempfile::tempdir().unwrap();
        let rejection =
            HostControlError::with_category("CAPACITY_BUSY", "redacted", "capacity").unwrap();
        let runner = Replies::new(vec![output(encode_json_frame(&rejection).unwrap(), 75)]);
        let (mut stderr, mut delays) = (Vec::new(), Vec::new());
        let cache = temp.path().join("cache");
        let error = send(&runner, &cache, &mut stderr, &mut delays).unwrap_err();
        assert_eq!(error.exit_kind(), ExitKind::Capacity);
        assert!(delays.is_empty());
        assert!(stderr.is_empty());
        assert_eq!(
            load_operation_envelope(&cache, request().request_id())
                .unwrap()
                .unwrap()
                .outcome(),
            Some(&OperationOutcome::Rejected {
                code: "CAPACITY_BUSY".into()
            })
        );
    }

    struct SettlementFailure {
        replies: Replies,
        cache: std::path::PathBuf,
    }

    impl ProcessRunner for SettlementFailure {
        fn run(&self, process: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
            let reply = self.replies.run(process)?;
            let envelope = self
                .cache
                .join(format!("op-{}.json", request().request_id()));
            assert!(
                envelope.is_file(),
                "the request must be persisted before sending"
            );
            // Replace only this test's private envelope after the reply has
            // been produced. RootedDir must refuse to settle a directory.
            std::fs::remove_file(&envelope).unwrap();
            std::fs::create_dir(&envelope).unwrap();
            Ok(reply)
        }
    }

    #[test]
    fn review_settlement_failure_preserves_verified_ack_and_prints_warning() {
        let temp = tempfile::tempdir().unwrap();
        let config =
            Config::parse("version = 1\n[controller]\nenabled = true\nssh = 'fakecontroller'\n")
                .unwrap();
        let expected = ack(&request());
        let runner = SettlementFailure {
            replies: Replies::new(vec![output(encode_json_frame(&expected).unwrap(), 0)]),
            cache: temp.path().join("cache"),
        };
        let mut stderr = Vec::new();
        let result = send_controller_mutation_with_wait(
            &runner,
            &config.controller,
            &runner.cache,
            &request(),
            &mut stderr,
            &mut |_| panic!("a definitive ACK must not be retried"),
        )
        .expect("settlement failure must not replace a verified ACK");
        assert_eq!(serde_json::to_value(result).unwrap(), expected);
        assert_eq!(runner.replies.frames.lock().unwrap().len(), 1);
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            format!(
                "controller request {}: acknowledged; the local envelope could not be marked settled (IO)\n",
                request().request_id(),
            )
        );
    }

    #[test]
    fn review_settlement_failure_preserves_typed_rejection_and_exit_category() {
        let temp = tempfile::tempdir().unwrap();
        let config =
            Config::parse("version = 1\n[controller]\nenabled = true\nssh = 'fakecontroller'\n")
                .unwrap();
        let rejection =
            HostControlError::with_category("CAPACITY_BUSY", "PRIVATE REMOTE DETAIL", "capacity")
                .unwrap();
        let expected = host_control_to_worker(&rejection);
        let runner = SettlementFailure {
            replies: Replies::new(vec![output(encode_json_frame(&rejection).unwrap(), 75)]),
            cache: temp.path().join("cache"),
        };
        let mut stderr = Vec::new();
        let error = send_controller_mutation_with_wait(
            &runner,
            &config.controller,
            &runner.cache,
            &request(),
            &mut stderr,
            &mut |_| panic!("a typed rejection must not be retried"),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), expected.public_code());
        assert_eq!(error.public_message(), expected.public_message());
        assert_eq!(error.exit_kind(), ExitKind::Capacity);
        assert_eq!(error.exit_code(), 75);
        assert_eq!(runner.replies.frames.lock().unwrap().len(), 1);
        assert!(!String::from_utf8(stderr).unwrap().contains("PRIVATE"));
    }

    #[test]
    fn resumable_errors_retry_the_same_frame_until_ack() {
        let temp = tempfile::tempdir().unwrap();
        let frame = encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "error": {"code": "HOST_IO", "message": "PRIVATE detail", "resumable": true}
        }))
        .unwrap();
        let runner = Replies::new(vec![
            output(frame.clone(), 74),
            output(frame, 74),
            output(encode_json_frame(&ack(&request())).unwrap(), 0),
        ]);
        let cache = temp.path().join("cache");
        let (mut stderr, mut delays) = (Vec::new(), Vec::new());
        let result = send(&runner, &cache, &mut stderr, &mut delays).unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), ack(&request()));
        let frames = runner.frames.lock().unwrap();
        assert_eq!(frames.len(), 3);
        assert!(frames.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(delays.len(), 2);
        let text = String::from_utf8(stderr).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert!(!text.contains("PRIVATE"));
        assert_eq!(
            load_operation_envelope(&cache, request().request_id())
                .unwrap()
                .unwrap()
                .outcome(),
            Some(&OperationOutcome::Acknowledged)
        );
    }

    #[test]
    fn exhausted_resumable_errors_leave_the_envelope_pending_with_a_recovery_hint() {
        let temp = tempfile::tempdir().unwrap();
        let frame = encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "error": {"code": "HOST_IO", "message": "PRIVATE detail", "resumable": true}
        }))
        .unwrap();
        let runner = Replies::new((0..4).map(|_| output(frame.clone(), 74)).collect());
        let cache = temp.path().join("cache");
        let (mut stderr, mut delays) = (Vec::new(), Vec::new());
        let error = send(&runner, &cache, &mut stderr, &mut delays).unwrap_err();
        assert_eq!(error.public_code(), "HOST_IO");
        assert_eq!(error.exit_code(), 70);
        let frames = runner.frames.lock().unwrap();
        assert_eq!(frames.len(), 4);
        assert!(frames.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(delays.len(), 3);
        for (delay, seconds) in delays.iter().zip([1., 3., 9.]) {
            assert!((seconds * 0.75..=seconds * 1.25).contains(&delay.as_secs_f64()));
        }
        let text = String::from_utf8(stderr).unwrap();
        assert_eq!(text.lines().count(), 4);
        assert_eq!(
            text.lines().last().unwrap(),
            format!(
                "controller request {} is published; the controller will finish it; check `worker task list`",
                request().request_id()
            )
        );
        assert!(!text.contains("PRIVATE"));
        assert!(
            load_operation_envelope(&cache, request().request_id())
                .unwrap()
                .unwrap()
                .outcome()
                .is_none()
        );
    }

    #[test]
    fn exhausted_retries_print_recovery_hint_and_remain_pending() {
        let temp = tempfile::tempdir().unwrap();
        let runner = Replies::new((0..4).map(|_| missing()).collect());
        let (mut stderr, mut delays) = (Vec::new(), Vec::new());
        let cache = temp.path().join("cache");
        assert_eq!(
            send(&runner, &cache, &mut stderr, &mut delays)
                .unwrap_err()
                .public_code(),
            "CONTROLLER_UNAVAILABLE"
        );
        assert_eq!(runner.frames.lock().unwrap().len(), 4);
        assert_eq!(delays.len(), 3);
        for (delay, seconds) in delays.iter().zip([1., 3., 9.]) {
            assert!((seconds * 0.75..=seconds * 1.25).contains(&delay.as_secs_f64()));
        }
        let text = String::from_utf8(stderr).unwrap();
        assert_eq!(text.lines().count(), 4);
        assert!(text.contains(&format!("controller request {} outcome is unknown; check `worker task list` or run `worker controller retry {}`", request().request_id(), request().request_id())));
        assert!(
            load_operation_envelope(&cache, request().request_id())
                .unwrap()
                .unwrap()
                .settled_at_millis()
                .is_none()
        );
    }

    #[test]
    fn exhaustion_hint_uses_the_final_reply_and_preserves_the_original_error() {
        let frame = encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "error": {"code": "TASK_BUSY", "message": "PRIVATE detail", "resumable": true}
        }))
        .unwrap();
        for final_resumable in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let cache = temp.path().join("cache");
            let mut replies = (0..3)
                .map(|_| {
                    if final_resumable {
                        missing()
                    } else {
                        output(frame.clone(), 64)
                    }
                })
                .collect::<Vec<_>>();
            replies.push(if final_resumable {
                output(frame.clone(), 64)
            } else {
                missing()
            });
            let runner = Replies::new(replies);
            let (mut stderr, mut delays) = (Vec::new(), Vec::new());
            let error = send(&runner, &cache, &mut stderr, &mut delays).unwrap_err();
            let text = String::from_utf8(stderr).unwrap();
            let expected = if final_resumable {
                assert!(matches!(
                    &error,
                    WorkerError::Task {
                        code: "TASK_BUSY",
                        ..
                    }
                ));
                assert_eq!(error.exit_code(), 64);
                format!(
                    "controller request {} is published; the controller will finish it; check `worker task list`",
                    request().request_id()
                )
            } else {
                assert_eq!(error.public_code(), "CONTROLLER_UNAVAILABLE");
                assert_eq!(error.exit_code(), 69);
                format!(
                    "controller request {} outcome is unknown; check `worker task list` or run `worker controller retry {}`",
                    request().request_id(),
                    request().request_id()
                )
            };
            assert_eq!(text.lines().last().unwrap(), expected);
            assert_eq!(text.lines().count(), 4);
            assert!(!text.contains("PRIVATE"));
            assert_eq!(delays.len(), 3);
            let frames = runner.frames.lock().unwrap();
            assert_eq!(frames.len(), 4);
            assert!(frames.windows(2).all(|pair| pair[0] == pair[1]));
            assert!(
                load_operation_envelope(&cache, request().request_id())
                    .unwrap()
                    .unwrap()
                    .outcome()
                    .is_none()
            );
        }
    }

    #[test]
    fn invalid_ack_identity_is_not_retried_or_settled() {
        let temp = tempfile::tempdir().unwrap();
        let mut wrong = ack(&request());
        wrong["payload_sha256"] = json!("0".repeat(64));
        let runner = Replies::new(vec![output(encode_json_frame(&wrong).unwrap(), 0)]);
        let (mut stderr, mut delays) = (Vec::new(), Vec::new());
        let cache = temp.path().join("cache");
        assert!(send(&runner, &cache, &mut stderr, &mut delays).is_err());
        assert!(delays.is_empty());
        assert!(
            load_operation_envelope(&cache, request().request_id())
                .unwrap()
                .unwrap()
                .settled_at_millis()
                .is_none()
        );
    }

    #[test]
    fn local_configuration_failure_never_sends_or_retries() {
        let temp = tempfile::tempdir().unwrap();
        let runner = Replies::new(Vec::new());
        let config =
            Config::parse("version = 1\n[controller]\nenabled = true\nssh = 'fakecontroller'\n")
                .unwrap();
        let mut controller = config.controller;
        controller.enabled = false;
        let mut stderr = Vec::new();
        let error = send_controller_mutation_with_wait(
            &runner,
            &controller,
            &temp.path().join("cache"),
            &request(),
            &mut stderr,
            &mut |_| panic!("local failures must not wait"),
        )
        .unwrap_err();
        assert_eq!(error.public_code(), "CONTROLLER_UNAVAILABLE");
        assert!(runner.frames.lock().unwrap().is_empty());
        assert!(stderr.is_empty());
    }
}
