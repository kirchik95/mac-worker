#[path = "../support/controller_process.rs"]
mod controller_process;

use crate::support;
use mac_worker::test_support::{
    agents::agent_facts::{AgentAuth, AgentFacts, AgentProbe},
    host::process::SystemProcessRunner,
    host::store::HostStore,
    session::{
        SessionAgent, claude_fixture, claude_project_dir, codex_fixture, imported_session_id,
    },
    task::{model::TaskId, store::TaskStore},
};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

const SOURCE_ID: &str = "018f0f4a-6b5c-7d8e-9f00-112233445566";

struct Fixture {
    _temp: tempfile::TempDir,
    project: support::GitRepo,
    laptop: PathBuf,
    host: PathBuf,
    ssh: PathBuf,
    config: PathBuf,
}

fn executable(path: &Path, content: &str) {
    fs::write(path, content).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let laptop = root.join("laptop");
        let host = root.join("host");
        fs::create_dir_all(laptop.join(".config/mac-worker")).unwrap();
        fs::create_dir_all(host.join("bin")).unwrap();
        fs::create_dir_all(host.join(".claude")).unwrap();
        fs::create_dir_all(host.join(".codex")).unwrap();
        fs::write(
            host.join(".zprofile"),
            format!(
                "export PATH='{}:/usr/bin:/bin'\n",
                host.join("bin").display()
            ),
        )
        .unwrap();
        let binary = env!("CARGO_BIN_EXE_worker");
        fs::create_dir_all(host.join(".local/bin")).unwrap();
        std::os::unix::fs::symlink(binary, host.join(".local/bin/worker")).unwrap();
        let ssh = root.join("fake-ssh");
        executable(
            &ssh,
            &format!(
                r#"#!/usr/bin/python3
import os, sys, json, subprocess
args = sys.argv[1:]
while args and args[0].startswith('-'):
    flag = args.pop(0)
    if flag in ['-o', '-p', '-S', '-i', '-F']: args.pop(0)
if not args or args.pop(0) != 'fake-session-host': sys.exit(97)
os.environ['HOME'] = {host:?}
for key in ['XDG_CONFIG_HOME','XDG_STATE_HOME','XDG_CACHE_HOME','XDG_DATA_HOME']:
    os.environ.pop(key, None)
os.environ['PATH'] = {bin:?} + ':/usr/bin:/bin'
command = ' '.join(args)
with open(os.path.join(os.environ['HOME'], 'ssh-journal'), 'a') as f: f.write(command + '\n')
if command.endswith(' host probe'):
    result = subprocess.run(['/bin/sh','-c',command], capture_output=True)
    if result.returncode: sys.exit(result.returncode)
    probe = json.loads(result.stdout)
    probe.update(memory_pressure='normal', swap_used_bytes=0, available_memory_bytes=16000000000)
    print(json.dumps(probe, separators=(',',':')))
    sys.exit(0)
if command.endswith(' host task-prepare'):
    data = sys.stdin.buffer.read()
    meta = json.loads(data)['meta']
    if 'session_import' in meta:
        repo = os.path.join(os.environ['HOME'], '.local/share/mac-worker/host/repos', meta['project_id'] + '.git')
        oid = subprocess.check_output(['/usr/bin/git', '--git-dir', repo, 'rev-parse', 'refs/mac-worker/sessions/' + meta['task_id']]).decode().strip()
        assert oid == meta['session_import']['package_oid']
        with open(os.path.join(os.environ['HOME'], 'checked-package'), 'w') as f: f.write(oid)
    result = subprocess.run(['/bin/sh','-c',command], input=data, capture_output=True)
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    sys.exit(result.returncode)
if command.endswith(' host lease-acquire'):
    request = os.path.join(os.environ['HOME'], 'lease-request.json')
    with open(request, 'wb') as f: f.write(sys.stdin.buffer.read())
    os.environ['SESSION_LEASE_FIXTURE'] = '1'
    subprocess.run([{test_binary:?}, '--exact', 'session_import_e2e::host_lease_fixture_helper', '--nocapture'], check=True, stdout=subprocess.DEVNULL)
    with open(os.path.join(os.environ['HOME'], 'lease-response.json'), 'rb') as f: sys.stdout.buffer.write(f.read())
    sys.exit(0)
os.execv('/bin/sh', ['/bin/sh', '-c', command])
"#,
                host = host.to_str().unwrap(),
                bin = host.join("bin").to_str().unwrap(),
                test_binary = std::env::current_exe().unwrap().to_str().unwrap()
            ),
        );
        let config = laptop.join(".config/mac-worker/config.toml");
        fs::write(
            &config,
            "version = 1\n[[workers]]\nname = 'fixture'\nssh = 'fake-session-host'\nslots = 1\n",
        )
        .unwrap();
        let project = support::GitRepo::init();
        project.write("README", b"fixture\n");
        project.commit_all("fixture");
        let fixture = Self {
            _temp: temp,
            project,
            laptop,
            host,
            ssh,
            config,
        };
        fixture.seed_facts();
        fixture
    }

    fn host_root(&self) -> PathBuf {
        self.host.join(".local/share/mac-worker/host")
    }

    fn seed_facts(&self) {
        let store = HostStore::open(&self.host_root()).unwrap();
        let facts = AgentFacts {
            collected_at_millis: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            agents: ["claude", "codex"]
                .into_iter()
                .map(|name| AgentProbe {
                    name: name.into(),
                    autoupdate: None,
                    auth_by_profile: vec![],
                    version: Some(
                        if name == "claude" {
                            "2.1.288"
                        } else {
                            "0.160.0"
                        }
                        .into(),
                    ),
                    auth: AgentAuth::Authenticated,
                })
                .collect(),
            env_profiles: vec![],
            git_identity: true,
            herdr: None,
            origin_https_helpers: Default::default(),
        };
        fs::write(
            store.root().join("facts.json"),
            serde_json::to_vec(&facts).unwrap(),
        )
        .unwrap();
        fs::set_permissions(
            store.root().join("facts.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }

    fn capture_fixture(&self, agent: SessionAgent) -> PathBuf {
        let cwd = self
            .project
            .root()
            .canonicalize()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let (path, bytes) = match agent {
            SessionAgent::Claude => (
                self.laptop
                    .join(".claude/projects")
                    .join(claude_project_dir(&cwd))
                    .join(format!("{SOURCE_ID}.jsonl")),
                claude_fixture(SOURCE_ID, &cwd, "2.1.288", 1),
            ),
            SessionAgent::Codex => (
                self.laptop
                    .join(".codex/sessions/2026/10/03")
                    .join(format!("rollout-2026-10-03T01-02-03-{SOURCE_ID}.jsonl")),
                codex_fixture(SOURCE_ID, &cwd, "0.160.0", 1),
            ),
        };
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        path
    }

    fn install_agent(&self, agent: SessionAgent) {
        // No real provider binaries can be reached, even through login PATH rebuilding.
        for name in ["cursor-agent", "opencode", "herdr"] {
            executable(&self.host.join("bin").join(name), "#!/bin/sh\nexit 127\n");
        }
        let events = match agent {
            SessionAgent::Codex => {
                r#"printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"{\"status\":\"done\",\"summary\":\"fixture done\",\"questions\":[],\"files_changed\":[]}"}}'"#
            }
            SessionAgent::Claude => {
                r#"printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"{\"status\":\"done\",\"summary\":\"fixture done\",\"questions\":[],\"files_changed\":[]}"}'"#
            }
        };
        let script = format!(
            r#"#!/bin/sh
case "$1" in
  --version) printf '{version}\n'; exit 0 ;;
  auth|login) printf '{{"loggedIn":true}}\n'; exit 0 ;;
esac
printf '%s\n' "$@" > "$HOME/argv"
# The runner must resume, and prepare must have already materialized both tokens.
find "$HOME/{store}" -type f -name '*.jsonl' > "$HOME/placed-files"
[ -s "$HOME/placed-files" ] || exit 91
while IFS= read -r file; do
  grep -q '@@MW_' "$file" && exit 92
  grep -q 'Synthetic' "$file" || exit 93
done < "$HOME/placed-files"
{events}
"#,
            version = if agent == SessionAgent::Claude {
                "2.1.288"
            } else {
                "0.160.0"
            },
            store = if agent == SessionAgent::Claude {
                ".claude"
            } else {
                ".codex"
            }
        );
        executable(&self.host.join("bin").join(agent.as_str()), &script);
    }

    fn submit(&self, agent: SessionAgent) -> Output {
        self.worker(&[
            "--json",
            "task",
            "submit",
            "--from-session",
            agent.as_str(),
            "--prompt",
            "continue safely",
            "--close-on",
            "never",
            "--worker",
            "fixture",
            "--no-wait",
            "--wait",
        ])
    }

    fn worker(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_worker"))
            .env_clear()
            .env("HOME", &self.laptop)
            .env("PATH", "/usr/bin:/bin")
            .env("MAC_WORKER_TEST_SSH", &self.ssh)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .current_dir(self.project.root())
            .args(["--config", self.config.to_str().unwrap()])
            .args(args)
            .output()
            .unwrap()
    }
}

fn import_round_trip(agent: SessionAgent) {
    let fixture = Fixture::new();
    let source = fixture.capture_fixture(agent);
    let original = fs::read(&source).unwrap();
    fixture.install_agent(agent);
    let output = fixture.submit(agent);
    if !output.status.success() {
        let observations = fixture.laptop.join(".local/state/mac-worker/observations");
        for entry in fs::read_dir(observations).into_iter().flatten().flatten() {
            if entry.path().is_file() {
                eprintln!(
                    "cached: {}",
                    fs::read_to_string(entry.path()).unwrap_or_default()
                );
            }
        }
    }
    assert!(
        output.status.success(),
        "stdout={} stderr={} ssh={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        fs::read_to_string(fixture.host.join("ssh-journal")).unwrap_or_default()
    );
    let report: serde_json::Value = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .rfind(|value| value.get("session_import").is_some())
        .unwrap();
    let task: TaskId = report["task_id"].as_str().unwrap().parse().unwrap();
    let imported = imported_session_id(&task);
    let argv = fs::read_to_string(fixture.host.join("argv")).unwrap();
    assert!(argv.contains(&imported), "{argv}");
    assert!(
        argv.contains(if agent == SessionAgent::Claude {
            "--resume\n"
        } else {
            "resume\n"
        }),
        "{argv}"
    );
    let placed = fs::read_to_string(fixture.host.join("placed-files")).unwrap();
    assert!(placed.contains(&imported), "{placed}");
    let native = fs::read_to_string(placed.lines().next().unwrap()).unwrap();
    assert!(!native.contains(SOURCE_ID));
    assert!(!native.contains(fixture.project.root().to_str().unwrap()));
    assert!(native.contains(&imported));
    assert!(native.contains("/workspace"));
    assert_eq!(
        fs::read(source).unwrap(),
        original,
        "capture never mutates the laptop transcript"
    );
    assert_eq!(report["session_import"]["agent"], agent.as_str());
    assert_eq!(report["session_import"]["source_id"], SOURCE_ID);
    assert!(report["session_import"]["size"].as_u64().unwrap() > 0);
    assert_eq!(report["status"]["last_outcome"]["kind"], "done");
    assert_eq!(
        fs::read_to_string(fixture.host.join("checked-package")).unwrap(),
        report["session_import"]["package_oid"].as_str().unwrap()
    );
    let store = HostStore::open(&fixture.host_root()).unwrap();
    let project =
        mac_worker::test_support::task::project::ProjectInspector::new(&SystemProcessRunner)
            .inspect(fixture.project.root())
            .unwrap()
            .project_id;
    let binding = TaskStore::new(&store, &SystemProcessRunner)
        .session(&project, task)
        .unwrap();
    assert_eq!(binding.unwrap().session_ref(), imported.as_str());
    let context =
        mac_worker::test_support::task::project::ProjectInspector::new(&SystemProcessRunner)
            .inspect(fixture.project.root())
            .unwrap();
    let transfer = mac_worker::test_support::transfer::repo::TransferRepo::open_or_create(
        &fixture.laptop.join(".cache/mac-worker"),
        &context.common_dir,
    )
    .unwrap();
    for prefix in ["refs/mac-worker/bases/", "refs/mac-worker/sessions/"] {
        assert!(
            !transfer.has_ref(&format!("{prefix}{task}")),
            "completed direct turn leaked {prefix}"
        );
    }
}

#[test]
fn codex_controller_submit_streams_places_and_resumes_native_session() {
    let controller = controller_process::ProcessFixture::new();
    let mut fixture = Fixture::new();
    fixture.laptop = controller.laptop_home.clone();
    let source = fixture.capture_fixture(SessionAgent::Codex);
    let original = fs::read(&source).unwrap();
    fixture.install_agent(SessionAgent::Codex);
    let mut ssh = fs::read_to_string(&fixture.ssh).unwrap();
    ssh = ssh.replace("if not args or args.pop(0) != 'fake-session-host': sys.exit(97)", "if not args: sys.exit(97)\ndestination = args.pop(0)\nif destination not in ['fakeexec', 'fakecontroller']: sys.exit(97)");
    let environment = format!(
        "\nif destination == 'fakecontroller':\n    os.environ['HOME'] = {:?}\n    os.environ.update({{'XDG_CONFIG_HOME': {:?}, 'XDG_STATE_HOME': {:?}, 'XDG_CACHE_HOME': {:?}, 'XDG_DATA_HOME': {:?}}})\n",
        controller.controller_home.to_str().unwrap(),
        controller.controller_xdg_config.to_str().unwrap(),
        controller.controller_xdg_state.to_str().unwrap(),
        controller.controller_xdg_cache.to_str().unwrap(),
        controller.controller_xdg_data.to_str().unwrap()
    );
    ssh = ssh.replace(
        "command = ' '.join(args)",
        &format!("{environment}\ncommand = ' '.join(args)"),
    );
    executable(&controller.fake_ssh, &ssh);
    let mut leader = controller.spawn_controller_run();
    controller.wait_until_leader_ready(&mut leader);
    let (status, stdout, stderr) = controller.run_laptop(
        &[
            "--json",
            "task",
            "submit",
            "--from-session",
            "codex",
            "--prompt",
            "continue safely",
            "--close-on",
            "never",
            "--worker",
            "mini-1",
            "--wait",
        ],
        Some(fixture.project.root()),
    );
    assert!(
        status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    let task: TaskId = report["task_id"].as_str().unwrap().parse().unwrap();
    let imported = imported_session_id(&task);
    assert!(
        fs::read_to_string(fixture.host.join("argv"))
            .unwrap()
            .contains(&imported)
    );
    assert!(
        fs::read_to_string(fixture.host.join("placed-files"))
            .unwrap()
            .contains(&imported)
    );
    assert_eq!(report["status"]["last_outcome"]["kind"], "done");
    assert_eq!(report["session_import"]["agent"], "codex");
    assert_eq!(
        fs::read_to_string(fixture.host.join("checked-package")).unwrap(),
        report["session_import"]["package_oid"].as_str().unwrap()
    );
    assert_eq!(fs::read(source).unwrap(), original);
    let context =
        mac_worker::test_support::task::project::ProjectInspector::new(&SystemProcessRunner)
            .inspect(fixture.project.root())
            .unwrap();
    let git_path =
        mac_worker::test_support::transfer::repo::TransferRepo::controller_transfer_git_path(
            &controller.controller_xdg_cache.join("mac-worker"),
            &context.project_id,
            &context.worktree_id,
        )
        .unwrap();
    assert!(git_path.is_dir());
    for prefix in ["refs/mac-worker/bases/", "refs/mac-worker/sessions/"] {
        let result = Command::new("/usr/bin/git")
            .env_clear()
            .args([
                "--git-dir",
                git_path.to_str().unwrap(),
                "show-ref",
                "--verify",
                &format!("{prefix}{task}"),
            ])
            .output()
            .unwrap();
        assert!(
            !result.status.success(),
            "completed controller turn leaked {prefix}"
        );
    }
    let envelope: serde_json::Value = serde_json::from_slice(
        &fs::read(controller.envelope_paths().into_iter().next().unwrap()).unwrap(),
    )
    .unwrap();
    let request_pin = format!(
        "refs/mac-worker/request-sessions/{}",
        envelope["request_id"].as_str().unwrap()
    );
    let result = Command::new("/usr/bin/git")
        .env_clear()
        .args([
            "--git-dir",
            git_path.to_str().unwrap(),
            "rev-parse",
            "--verify",
            &request_pin,
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "request package pin must remain replayable"
    );
    assert_eq!(
        String::from_utf8(result.stdout).unwrap().trim(),
        report["session_import"]["package_oid"].as_str().unwrap()
    );
    leader.terminate_and_reap();
}

#[test]
fn host_lease_fixture_helper() {
    if std::env::var("SESSION_LEASE_FIXTURE").as_deref() != Ok("1") {
        return;
    }
    use mac_worker::test_support::{
        core::protocol::MemoryPressure,
        host::{
            job::LeaseAcquireRequest,
            lease::{AdmissionFacts, LeaseService},
        },
    };
    let home = PathBuf::from(std::env::var_os("HOME").unwrap());
    let store = HostStore::open(&home.join(".local/share/mac-worker/host")).unwrap();
    let request: LeaseAcquireRequest =
        serde_json::from_slice(&fs::read(home.join("lease-request.json")).unwrap()).unwrap();
    let facts = AdmissionFacts {
        free_disk_bytes: 100 << 30,
        total_disk_bytes: 200 << 30,
        memory_pressure: MemoryPressure::Normal,
        swap_used_bytes: Some(0),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let response = LeaseService::new(&store)
        .acquire(&request, &facts, now)
        .unwrap();
    fs::write(
        home.join("lease-response.json"),
        serde_json::to_vec(&response).unwrap(),
    )
    .unwrap();
}

#[test]
fn claude_direct_submit_places_and_resumes_native_session() {
    import_round_trip(SessionAgent::Claude);
}

#[test]
fn codex_direct_submit_places_and_resumes_native_session() {
    import_round_trip(SessionAgent::Codex);
}
