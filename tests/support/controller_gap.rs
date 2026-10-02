use std::{
    collections::BTreeMap, ffi::OsString, io::Cursor, os::unix::process::ExitStatusExt,
    process::ExitStatus,
};

use clap::Parser;
use mac_worker::test_support::{
    agents::agent::{AgentKind, PermissionPolicy},
    cli::Cli,
    client_state::ClientStateStore,
    controller::encode_json_frame,
    core::{error::WorkerError, paths::PathLayout, protocol::PROTOCOL_VERSION},
    host::process::{ProcessRequest, ProcessResult, ProcessRunner},
    runtime::{RuntimeContext, run_with_stdio_in_context},
    task::model::{
        ClosePolicy, GitIdentity, LocalTaskRecord, PublishMode, TaskId, TaskLimits, TaskMeta,
        TaskMetaInput, TaskOutcome, TaskSource, TaskState, TaskStatus, TurnId, TurnSummary,
        TurnTerminal,
    },
    transfer::HostOperation,
};
use serde_json::{Value, json};

pub struct IsolatedHost {
    pub paths: PathLayout,
    pub runtime: RuntimeContext,
    _root: tempfile::TempDir,
}

impl IsolatedHost {
    pub fn new(controller: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let canonical = root.path().canonicalize().unwrap();
        let home = canonical.join("home");
        std::fs::create_dir(&home).unwrap();
        let environment = BTreeMap::from([
            (OsString::from("HOME"), home.as_os_str().to_owned()),
            (
                OsString::from("XDG_CONFIG_HOME"),
                canonical.join("config").into(),
            ),
            (
                OsString::from("XDG_STATE_HOME"),
                canonical.join("state").into(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                canonical.join("cache").into(),
            ),
            (
                OsString::from("XDG_DATA_HOME"),
                canonical.join("data").into(),
            ),
        ]);
        let paths = PathLayout::discover(None, &environment, &home).unwrap();
        std::fs::create_dir_all(paths.config.parent().unwrap()).unwrap();
        let config = if controller {
            "version = 1\n[[workers]]\nname = 'mini-1'\nssh = 'fixture-worker'\nslots = 1\n"
        } else {
            "version = 1\n[controller]\nenabled = true\nssh = 'fixture-controller'\n"
        };
        std::fs::write(&paths.config, config).unwrap();
        Self {
            paths,
            runtime: RuntimeContext::isolated(environment, home, canonical),
            _root: root,
        }
    }

    pub fn seed(&self, push: bool, effective_permission: bool) -> LocalTaskRecord {
        let task_id = TaskId::generate();
        let turn_id = TurnId::generate();
        let head = "a".repeat(40).parse().unwrap();
        let mut meta = TaskMeta::new(TaskMetaInput {
            session_import: None,
            task_id,
            run_id: None,
            project_id: "a".repeat(64),
            worktree_id: "b".repeat(64),
            agent: AgentKind::Claude,
            model: None,
            effort: None,
            policy: PermissionPolicy::Workspace,
            source: TaskSource::Origin {
                url: "https://example.test/repo".into(),
            },
            publish: if push {
                vec![PublishMode::Fetch, PublishMode::Push]
            } else {
                vec![PublishMode::Fetch]
            },
            publish_branch: None,
            base_oid: head,
            limits: TaskLimits::default(),
            close_policy: ClosePolicy::Never,
            env_profile: None,
            git_identity: GitIdentity::new("Fixture", "fixture@example.test").unwrap(),
            title: None,
            prompt: "fixture".into(),
            created_at_millis: 1,
        })
        .unwrap();
        if effective_permission {
            meta = meta
                .with_effective_policy(PermissionPolicy::Unattended)
                .unwrap();
        }
        let status = TaskStatus::new(
            TaskState::Closed,
            Some(TaskOutcome::Done),
            Some("mini-1".into()),
            true,
            Some(meta.base_oid().clone()),
            None,
            Vec::new(),
            Vec::new(),
            None,
            vec![TurnSummary::new(
                1,
                turn_id,
                Some(TurnTerminal::Succeeded),
                Some(TaskOutcome::Done),
                Some(true),
                false,
                Some(1),
                Some(2),
            )],
            2,
        )
        .unwrap();
        let record = LocalTaskRecord::new(
            meta,
            status,
            None,
            None,
            None,
            "c".repeat(64),
            None,
            true,
            None,
        )
        .unwrap();
        ClientStateStore::open(&self.paths.state)
            .unwrap()
            .create_task(record.clone())
            .unwrap();
        record
    }

    pub fn cli(&self, runner: &dyn ProcessRunner, args: &[&str]) -> (u8, Vec<u8>, Vec<u8>) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_stdio_in_context(
            Cli::try_parse_from(args).unwrap(),
            runner,
            &self.runtime,
            &mut Cursor::new(Vec::new()),
            &mut stdout,
            &mut stderr,
        );
        (exit, stdout, stderr)
    }

    pub fn rpc(&self, runner: &dyn ProcessRunner, command: &str, body: Value) -> ProcessResult {
        let frame = encode_json_frame(&json!({
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "018f0f4a6b5c7d8e9f00112233445566",
            "command": command,
            "body": body,
        }))
        .unwrap();
        self.serve(runner, frame)
    }

    fn serve(&self, runner: &dyn ProcessRunner, frame: Vec<u8>) -> ProcessResult {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let exit = run_with_stdio_in_context(
            Cli::try_parse_from(["worker", "host", "controller-rpc"]).unwrap(),
            runner,
            &self.runtime,
            &mut Cursor::new(frame),
            &mut stdout,
            &mut stderr,
        );
        ProcessResult {
            status: ExitStatus::from_raw(i32::from(exit) << 8),
            stdout,
            stderr,
        }
    }
}

pub struct ControllerBridge<'a> {
    pub controller: &'a IsolatedHost,
    pub worker: &'a dyn ProcessRunner,
}

impl ProcessRunner for ControllerBridge<'_> {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        assert_eq!(request.program, OsString::from("/usr/bin/ssh"));
        assert_eq!(
            request.args.last().unwrap(),
            HostOperation::ControllerRpc.command()
        );
        assert!(request.args.iter().any(|arg| arg == "fixture-controller"));
        Ok(self
            .controller
            .serve(self.worker, request.stdin.clone().unwrap()))
    }
}

pub struct NoProcesses;

impl ProcessRunner for NoProcesses {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        panic!(
            "unexpected external process: {}",
            request.program.to_string_lossy()
        );
    }
}
