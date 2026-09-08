# mac-worker v2 design: agent tasks on the worker pool

- Date: 2026-09-03 (revision 2.3, after three rounds of design review and the Task 0 spike on the same day; see section 25)
- Status: draft for review
- Repository: `mac-worker`
- User-facing commands: `worker task ...`, extended `worker workers`, `worker dashboard`
- Supersedes in part: [mac-worker v1 design](2026-08-25-mac-worker-design.md) sections 1, 4, 5, 9, 12, and 15 as listed in section 5
- Depends on: v1 phases 3 and 4 (durable single-worker execution, three-worker FIFO scheduler) with the amendments in section 5.1, and the [local dashboard design](2026-08-26-local-dashboard-design.md)

## 1. Decision

Add a second job kind, the **agent task**, to the existing `worker` binary. An agent task carries a prompt instead of a command. The selected Mac mini materializes a Git worktree for the task, launches a headless coding agent (Codex, Claude Code, Cursor Agent, or OpenCode) inside it, supervises that process exactly like a v1 batch job, and records the result as a Git branch plus the agent's final message and transcript.

The MacBook remains the place where tasks are written, queued, watched, and collected. The agent's model calls, file edits, test runs, and commits all happen on the worker. Nothing on the worker listens for connections; every operation is still an SSH-invoked host-helper call, and every task turn is a process that starts when the task is admitted and exits when the turn ends.

Two local pieces make the immediate-return promise honest without a daemon: a mac-worker-owned **transfer repository** per project, so submission never writes into the user's repository, and a bounded **local turn runner** process per turn, which owns the queue row, admission, transfer, following, outcome recording, and result fetch after the CLI has returned.

The v1 batch job kind (`worker run`) stays unchanged and shares the same leases, queue, supervisor, log, status, and cleanup machinery. This document specifies what the agent task adds, relaxes, or amends.

## 2. Context

### 2.1 What changed since v1

V1 was designed around one premise: the MacBook is the only place where decisions are made, and a worker executes exactly the command the user typed. The operator now wants the opposite division of labour for a specific class of work: hand a list of tasks to the pool from an orchestrating agent on the MacBook, and have a chosen coding agent run each task to completion on a Mac mini, with per-worker limits, a queue, and no interference between tasks.

Earlier analysis compared this with Orca remote runtimes and Cursor self-hosted machines. Both put an agent on the remote machine; Cursor keeps the model loop in its cloud and drives the machine tool-call by tool-call, Orca moves an interactive workspace to the server. This design keeps both the model loop and the hands on the worker, runs agents headless, and keeps the MacBook-side control plane a locked directory plus bounded helper processes rather than a service.

### 2.2 Pool inventory relevant to this design

Sanitized facts observed on 2026-09-03 across the three configured workers:

- Every worker is an Apple M4 Mac mini with 16 GB, never sleeps, and has the v1 host helper installed.
- Every worker has Codex, Claude Code, Cursor Agent, and OpenCode binaries on the login-shell `PATH`. The binary named `agent` on the workers is an unrelated Grok CLI, so adapters must invoke `cursor-agent` by name.
- Codex is authenticated on every worker through a file-based login and works from a non-interactive SSH session today.
- Claude Code is not authenticated on any worker. Cursor Agent fails from SSH because the macOS login keychain is locked in non-GUI sessions. Any keychain-backed login has this problem; environment-provided tokens do not.
- Two workers already hold clones of the two main projects and carry SSH keys for their Git origin; the third holds neither, and may hold no Git identity.
- `tmux` is absent. A Herdr server runs on each worker; this design does not depend on it.

### 2.3 Implementation state

`main` contains v1 phases 1 and 2. Phase 3 (`worker run --worker`, durable supervision, status, reconnectable logs, atomic lease, resolve-or-abandon, durable cleanup) exists on a feature branch whose live acceptance record is still pending. The phase 4 scheduler plan exists with its pure ranking policy implemented. The dashboard projection model, observation cache, snapshot service, and static client exist on feature branches without the HTTP server, CLI command, or data adapters. Those pieces are prerequisites, not part of this design.

## 3. Goals

- Submit one task or a batch of tasks from a shell or from an orchestrating agent, and receive stable task identifiers immediately, with admission, transfer, and following carried on by a bounded local process after the command returns.
- Run each task as a fresh, isolated, headless agent process in its own Git worktree on one worker, with the worker's one heavy slot held for the duration of each turn.
- Enforce limits: one turn per worker at a time, per-worker FIFO admission with an explicit no-wait mode, per-turn timeout, agent-level turn and budget caps where the agent supports them, a cap on follow-up turns per task, and a cap on concurrently active tasks per run.
- Keep every task observable at any moment: reconnectable event log, live diff of the workspace, task and run status, and the dashboard.
- Allow conversation with a task between turns: the agent's final message and questions are surfaced, and a follow-up message resumes the same agent session in the same workspace on the same worker.
- Support two ways of getting code to a worker and two ways of getting results back, chosen per task: base pushed from the MacBook over SSH or fetched by the worker from its origin; result fetched over SSH or pushed by the worker to its origin.
- Never write into the user's repository during submission; the only write into it, ever, is one remote-tracking ref plus its objects when a result is fetched.
- Keep every remote path derived from validated identifiers, every SSH call a fixed host-helper argv, and every worker free of listeners and daemons.
- Keep mac-worker's own diagnostics free of secret values while accepting that agent transcripts are application output.

## 4. Non-goals

- Interactive, PTY-backed agent sessions supervised by mac-worker (see section 22).
- A persistent agent process per worker that waits for tasks, or a persistent local dispatcher.
- Driving a remote agent tool-call by tool-call from the MacBook.
- Creating merge requests or pull requests on the agent's behalf; the agent may do so itself only when the task explicitly instructs it and the worker account is credentialed.
- Automatic installation, upgrade, or login of agent CLIs on workers.
- Any transfer of credentials, tokens, keychain items, or `.env` files by mac-worker.
- Multiple concurrent turns on one worker.
- Cross-task shared workspaces or shared agent sessions.
- Merging, rebasing, or resolving conflicts between task branches; that remains the user's work on the MacBook.
- Sandboxing untrusted prompts or untrusted repositories.
- A non-loopback dashboard, notifications, or multi-user access.

## 5. Relationship to the v1 design

The v1 document remains authoritative for batch jobs. For agent tasks this document supersedes the following v1 statements:

| v1 statement | v2 rule for agent tasks |
|---|---|
| §1, §4: no editing on a worker, no reverse synchronization | The agent edits its own workspace; results return only as Git objects fetched from the worker's mirror, never as an rsync of the workspace |
| §9.2: the remote workspace contains no `.git` directory | A task workspace is a shared clone of a per-project bare mirror on the worker, with its own `.git` inside the checkout |
| §9.1: a sensitive path fails preflight even when tracked; only selected files leave the MacBook | The base commit's full history is transferred to the mirror and retained there for the branch retention period; `SENSITIVE_PATH` runs over the tree of the base commit, not over history |
| §9.2: `SNAPSHOT_CHANGED` re-verification of a staged tree | The base commit is captured through a scratch index and verified by a second capture that must produce the same tree; a difference is `SNAPSHOT_CHANGED` |
| §12: no stdin, clean per-job `HOME`, minimal environment, workspace removed at terminal state | A turn receives the prompt on stdin, runs with the worker account's real `HOME` and login-shell environment plus one named env profile, and leaves the task workspace in place |
| §5, §15: no credentials on workers beyond read-only registry credentials | Agent authentication tokens and the account's own Git origin credentials are accepted on workers by explicit operator choice and are modelled as capabilities |

Everything else in v1 continues to apply: the trust model for trusted code, the one-slot lease as the only admission authority, durable acceptance before acknowledgement, resolve-or-abandon for ambiguous submissions, bounded log chunks, rooted cleanup below the data root, the error and exit-code model, and the no-daemon rule.

### 5.1 Amendments to the phase 3 and phase 4 contracts

Agent tasks require the following changes to already-planned contracts. They are listed here so the implementation plan can gate on them explicitly.

Phase 3 (durable single-worker execution):

- The execution payload gains a version 2 with an optional `turn` section naming the task; the detached supervisor uses it to select the task workspace as the working directory, connect stdin to the prompt file, use the account home and login-shell environment, inject the env profile and Git identity, and skip workspace removal at terminal state. `SUPERVISION_VERSION` is bumped once.
- Turn jobs live in the v1 `jobs/<project_id>/<worktree_id>/<job_id>/` tree and the job index, so status, logs, reconciliation, and retention work unchanged.
- The `HostStore` layout gains `repos/` and `tasks/` with a layout version bump and an explicit migration that runs only through a hidden `migrate-layout` host command, which the `worker setup` script executes under the installation lock before its final probe; every other entry point, including the read-only probe, fails closed on an outdated layout with `HOST_LAYOUT_OUTDATED`, so `worker setup` must be rerun on every worker before it becomes eligible again.
- Hidden host commands `receive-pack`, `upload-pack`, `task-prepare`, `task-turn`, `task-status`, `task-diff`, and `task-close` are added under one protocol version bump.

Phase 4 (three-worker scheduler):

- Queue entries carry a `kind` (`batch` or `task_turn`) and an optional run cap reference.
- `claim_next` changes from head-of-line FIFO to per-worker FIFO: for each ranked idle worker, the oldest waiting row eligible for that worker wins; a row that is ineligible for every idle worker (pinned to a busy worker, capped by its run, or lacking a capability) does not block younger rows eligible for another worker. Between rows eligible for the same worker, the older row always wins. Cancellation, reversion, and dead-dispatcher recovery keep their semantics.
- A `task_turn` row records its local turn runner as owner in both the waiting and the dispatching state (section 8.2). Dead-owner `task_turn` rows are never reaped as abandoned: `worker run` skips them, and the next mutating `worker task` command re-owns them. A `queued` task whose row is missing is re-enqueued at the tail by that command.
- Fleet probes used for admission are served from a shared per-worker observation cache under the local state root with a short TTL and single-flight refresh, so many waiting dispatchers do not multiply SSH probes.

## 6. Trust and isolation model

All tasks are trusted work owned by the user. A task workspace is a filesystem convenience, not a security boundary. An agent turn runs with the full access of the configured worker account: its files, processes, caches, agent configuration, MCP servers, skills, and any credentials that account holds. Agent-native sandboxes (for example Codex's workspace-write sandbox) may be enabled through the permission policy in section 11.4 as defence against accidents, not as isolation from a hostile prompt.

The operator explicitly accepts three new categories of state on workers:

- agent authentication material, provisioned by the operator in an env profile or in the agent's own file-based login;
- the worker account's own Git credentials for origin access, when `source = origin` or `publish = push` is wanted on that worker;
- outbound network access from the worker to the agent's model provider.

mac-worker reads an env profile only to place its variables into the agent's environment and to run the read-only authentication probe; it never copies, forwards, prints, or records values. SSH agent forwarding, privilege elevation, and production or cloud-administrator credentials remain forbidden.

Three exposures are accepted and documented rather than prevented, because they are inherent to the trusted single-account model:

- Hidden identity components travel in the `--receive-pack` and `--upload-pack` program strings and are visible in the worker's process list for the duration of the transfer, exactly as v1's `rsync-receive` components are.
- Agents keep their own session stores outside the mac-worker data root (for example under the account's Codex and Claude directories). Prompts, file contents, and diffs persist there under the agent's own retention; `close --discard` requests agent-native deletion where the agent offers it and otherwise leaves them.
- OpenCode starts a loopback-only HTTP server for the lifetime of its own process. It is bound to the turn, not to mac-worker, and is the one exception to "nothing on the worker listens".

The rule that keeps tasks isolated from one another is structural: a turn is one process group in one worktree, started by the supervisor and terminated with it. No message is ever written into a running agent process; conversation happens only between turns (section 14).

## 7. User experience

### 7.1 Commands

```text
worker task submit [options] (--prompt TEXT | --prompt-file PATH)
worker task batch FILE [--run-name NAME] [--max-parallel N]
worker task list [--run RUN_ID] [--state STATE] [--outcome KIND]
worker task status TASK_ID
worker task logs [-f] TASK_ID [--turn N] [--raw]
worker task diff TASK_ID [--stat]
worker task say TASK_ID (--message TEXT | --message-file PATH) [--wait]
worker task cancel TASK_ID
worker task result TASK_ID
worker task fetch TASK_ID
worker task close TASK_ID [--discard]
worker task wait (TASK_ID... | --run RUN_ID) [--timeout DURATION]
worker task reconcile     # re-own dead runners and re-enqueue orphaned tasks without submitting anything
worker workers [--refresh]  # extended with agent, profile, and origin capabilities; --refresh recollects agent facts
worker dashboard          # extended with tasks and runs
```

`submit` options:

```text
--agent codex|claude|cursor|opencode     required
--model ID                               optional, agent-specific; default from .worker.toml
--effort LEVEL                           optional reasoning effort, passed to agents that accept one
--project PATH                           default: current worktree
--base REF                               default: HEAD
--wip                                    include uncommitted changes as a temporary base commit
--include PATTERN                        untracked inputs for --wip, same policy as v1
--source local|origin                    default from .worker.toml, else local
--publish fetch|push                     repeatable; default from .worker.toml, else fetch
--publish-branch NAME                    origin branch name used only by publish = push
--timeout DURATION                       per turn; default 45m; max 24h
--max-turns N                            agent-internal turn cap where supported
--max-budget-usd AMOUNT                  where supported
--max-followups N                        follow-up turns allowed after the first; default 10
--close-on done|never                    default done
--env-profile NAME                       overrides .worker.toml
--worker NAME                            diagnostic pin, never a raw SSH destination
--no-wait                                CAPACITY_BUSY instead of queueing
--wait                                   stay attached until the first turn ends
```

Without `--wait`, `submit`, `batch`, and `say` return as soon as the task record, the base commit in the transfer repository, and the queue row exist and a local turn runner has taken ownership of the row, or the row is parked behind the per-worker runner cap. With `--wait` the same work happens in the foreground and the command follows the turn's event log to its end. `list`, `status`, `result`, `diff`, and `logs` are read-only; `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and `reconcile` perform runner recovery first (section 8.2).

`--json` on any command emits the same typed records the dashboard consumes; `submit`, `say`, `logs -f`, and `wait` emit versioned NDJSON events, with log chunks base64-encoded as in v1.

Examples:

```text
worker task submit --agent codex --prompt-file tasks/fix-flaky-login.md
worker task submit --agent claude --model opus --wip --prompt "Finish the migration in this branch and make the suite green"
worker task batch tasks/sprint-42.toml --max-parallel 3
worker task say 8f3a… --message "Yes, drop the legacy endpoint too" --wait
worker task fetch 8f3a…
```

### 7.2 Batch file

```toml
version = 1
agent = "codex"
model = "gpt-5.6-luna"
effort = "max"
base = "main"
source = "local"
publish = ["fetch"]
timeout = "45m"

[[tasks]]
title = "Flaky login spec"
prompt_file = "tasks/fix-flaky-login.md"

[[tasks]]
title = "Extract billing client"
prompt = """
Move the billing HTTP client into packages/billing-client …
"""
agent = "claude"
publish = ["fetch", "push"]
publish_branch = "feat/billing-client"
```

Top-level keys are defaults; each task may override them. A batch validates every entry, then creates one run and one task per entry, returns all identifiers, and leaves admission to the tasks' runners. Tasks in a run are independent; the run only groups them for status, waiting, and `max_parallel`.

### 7.3 Project configuration

`.worker.toml` gains a `[task]` table:

```toml
[task]
source = "local"
publish = ["fetch"]
env_profile = "agents"
default_agent = "codex"
model = "gpt-5.6-luna"     # optional; the CLI flag wins
effort = "max"             # optional reasoning effort; the CLI flag wins
timeout = "45m"
max_followups = 10

[task.permissions]
codex = "workspace"        # workspace | unattended
claude = "unattended"
cursor = "unattended"
opencode = "unattended"
```

### 7.4 Orchestrator skill

The repository ships a skill for the orchestrating agent on the MacBook that documents exactly one loop: submit tasks with `--json`, poll or `wait`, read `result`, `fetch` branches, `say` follow-ups when a task reports `needs_input`, and `close` when done. The skill contains no logic; the CLI is the only interface, so the same loop works from Claude Code, Codex, or a shell script.

## 8. Architecture

The agent task reuses v1 components, amends two of them, and adds six:

| Component | Role | Status |
|---|---|---|
| Inventory, SSH transport, `setup`, probes | unchanged; probe adds agent, profile, and Git identity facts | v1 |
| Atomic one-slot lease, durable job records, log chunks, status, resolve-or-abandon, rooted cleanup | unchanged; a turn is a job | v1 phase 3 |
| Detached supervisor | amended per section 5.1: turn payload, stdin, environment, working directory, workspace preservation | v1 phase 3 |
| FIFO queue, automatic selection, `--no-wait`, cancellation, fleet reconciliation | amended per section 5.1: entry kinds, per-worker FIFO, detached dispatch owners | v1 phase 4 |
| **Transfer repository** | a mac-worker-owned bare repository per project on the MacBook, with alternates into the user's object store; every base commit, push, and fetch happens here | new |
| **Local turn runner** | a detached, bounded process per turn that owns the queue row, admission, transfer, submission, following, outcome recording, and result import | new |
| **Repo mirror** | per-project bare repository on each worker, filled by client pushes or origin fetches | new |
| **Task materializer** | creates the base commit, transfers it, and creates or reuses the task worktree on the worker | new |
| **Agent adapters** | build argv, environment, permission flags, session binding, structured result requests; parse event streams | new |
| **Task store and turn orchestration** | durable task records spanning turns, follow-up scheduling with hard pins, close and retention policy, authority order | new |
| **Result publisher** | commits leftover changes, records diff summary, serves fetch, optionally pushes to origin | new |
| Dashboard | extended projection with tasks, runs, turns, runner state, and event timeline | dashboard design + this document |

There is still no always-running service. Local CLI processes and turn runners coordinate through the locked state root on the MacBook. On a worker, the only long-lived processes are a detached supervisor and the agent process it owns, for the duration of one turn.

### 8.1 End-to-end task flow

1. The client inspects the worktree and `.worker.toml`, validates the agent, source, publish, and limit options, and probes the fleet. With `--no-wait` and no eligible idle worker it stops here with `CAPACITY_BUSY`, having created nothing.
2. The client creates or resolves the base commit inside the transfer repository (section 10.2) and records its object ID. For `source = origin` the ref is resolved against the origin's remote-tracking refs, or preflighted with `git ls-remote`, and fails at submit with `BASE_NOT_ON_ORIGIN` when the commit is not on origin.
3. The client creates the local task record and enqueues the first turn as a queue entry of kind `task_turn`, then starts a local turn runner and hands the row to it (section 8.2). Without `--wait` the command returns here.
4. The runner selects an eligible worker under per-worker FIFO: ready, idle, protocol-compatible, with the required agent capability and, when needed, the required origin capability, and with its run below `max_parallel`. It acquires the atomic lease.
5. Under the lease, the runner pushes the base commit from the transfer repository into the worker's mirror over SSH (`source = local`), or the host fetches the recorded object ID from origin into the mirror (`source = origin`). The host verifies the mirror now contains exactly the recorded commit.
6. The host creates or reuses the task worktree on the mirror branch `task/<task_id>`, durably records the turn as `accepted`, starts the detached supervisor, and acknowledges the turn ID. The runner records acceptance locally.
7. The supervisor launches the agent process with the composed prompt on stdin, streams the agent's event log to the append-only turn log, and enforces the timeout.
8. Anyone may follow the event log; disconnecting never affects the turn. The runner polls status with bounded intervals.
9. When the agent process exits, the supervisor commits any uncommitted workspace changes onto the result branch, extracts the structured result and session identifier, records the diff summary and exit classification, releases the lease, and leaves the workspace in place.
10. The task becomes `open` with the last turn's outcome, or `closed` when `--close-on done` applies and the agent reported `done`. Closing removes the workspace and keeps the mirror branch and the task metadata.
11. The runner imports the result branch into the transfer repository over SSH and then into the user's repository as `refs/remotes/mac-worker/<worker>/task/<task_id>`, records the fetched head, and exits. If `publish` includes `push`, the host has already pushed the result branch to origin before the task closed.
12. A follow-up (`say`) creates a new turn pinned to the same worker and a new runner; the host verifies the workspace and branch are intact and resumes the recorded agent session under a fresh lease.

### 8.2 Local turn runner

A runner is started by `submit`, `batch`, and `say` for each new turn, through the same detach discipline the host supervisor uses: its own session and process group, stdio redirected to an owner-only `runner.log` under the local state root, no controlling terminal. Its process identity becomes the queue row's owner, in the waiting state as well as the dispatching state, before the starting command returns. If the handoff is not observed within a bounded wait, the command removes the row, marks the task `abandoned`, releases the base ref, and fails with `RUNNER_HANDOFF_FAILED`, so no task record is left behind without an owner.

At most one runner per configured worker waits for capacity at any time. Younger waiting rows are parked: they have no process, keep their task `queued`, and are given a runner by a runner that finishes or by the next mutating task command. A waiting runner polls with exponential backoff from one second to thirty seconds and reads fleet health from the shared observation cache, refreshing it single-flight only when its entry is older than its TTL. A runner therefore costs the pool no more SSH traffic than one interactive `worker run` would.

The runner's lifetime is the capacity wait plus the turn: it exits after recording the terminal outcome and importing the result, after the turn is `lost`, after a pre-acceptance failure has been resolved or abandoned, or when the local task record says the task was cancelled before acceptance. It holds no lock while waiting for capacity, and no exclusive lock during SSH or Git transport; the transfer repository handle it keeps open for the turn holds that repository's lock shared, which blocks nobody but transfer collection of that repository (section 10.2).

The runner never decides task state on its own: it records what the worker reported. A killed runner leaves the remote turn untouched. Recovery is cooperative and lock-protected, and it runs only in mutating commands: `submit`, `batch`, `say`, `cancel`, `close`, `wait`, and the explicit `worker task reconcile`. Each of these first refreshes the tasks it is about to act on from their recorded workers, re-owns dead-owner rows, re-enqueues `queued` tasks whose row is missing, and starts a replacement runner for any waiting row or active turn that has no live runner, within the per-worker cap. `worker task wait` does this on every poll, so a batch that outlived its submitting shell still completes. `list`, `status`, `result`, `diff`, `logs`, and dashboard task views never mutate task state or start a process; they show a dead runner as `dead` and a stale row as stale. The dashboard's authorized native agent-settings save is outside task lifecycle state. No process is ever started for a task that is `open`, `closed`, `abandoned`, or `lost`.

With `--wait`, the foreground command performs the runner's work itself and additionally follows the event log; it is the row's owner for that turn.

### 8.3 Authority order

Task state has one authority order, mirroring v1 and the dashboard design:

1. the worker's durable task status and turn status for `active`, `open`, `closed`, `lost`, `last_outcome`, session presence, head, and result;
2. the worker's lease for occupancy;
3. the local task record for `queued` and for `abandoned` before acceptance, for runner identity, run membership, and fetched heads;
4. cached local copies of remote state only when a current query fails, always marked stale.

`say`, `close`, `wait`, and run cap counting refresh from the recorded worker before acting on `active` or `open`; `list` and `status` refresh for display only and label rows they could not refresh as stale. A local record can never keep a task `active` after the worker reports its turn terminal.

## 9. Identifiers and paths

The client computes, in addition to v1's `project_id` and `worktree_id`:

- `task_id`: a random 128-bit identifier generated before submission, rendered as 32 lowercase hexadecimal characters like a v1 job ID.
- `turn_id`: a v1 job ID; every turn is a job. The first turn's ID is distinct from the task ID.
- `run_id`: a random identifier for a batch; optional grouping metadata.
- `repo_id`: SHA-256 of the canonical path of the local repository's common Git directory. It keys the transfer repository, because `project_id` is derived from the origin URL and two clones of one origin must not share alternates.
- `base_oid`: the 40-hex object ID of the base commit. Wherever a v1 record requires a 64-hex digest slot for a job, a turn stores the SHA-256 of its canonical turn material there and keeps `base_oid` inside the turn material.
- `session_ref`: the agent-native session identifier, either generated by mac-worker before the first turn or captured from the event stream; stored verbatim and bounded; never guessed.

Worker-side data below the resolved mac-worker data root:

```text
repos/<project_id>.git/                         bare mirror; refs/mac-worker/bases/<task_id>, refs/heads/task/<task_id>
tasks/<project_id>/<task_id>/
  meta.json                                     task identity, source, publish, limits, base, Git identity, title
  status.json                                   task state, turn history, last outcome, result summary, retention marks
  session.json                                  owner-only agent session binding
  workspace/                                    shared clone of the mirror on the result branch; absent after close
jobs/<project_id>/<worktree_id>/<turn_id>/      a v1 job directory: meta, status, stdout (the agent's event stream), stderr, prompt.md, result.schema.json, last.md, tail.log
job-index/, leases/, incoming/, snapshots/      unchanged v1 namespaces
```

The agent's stdout is its raw event stream; the turn's `stdout` log holds it unchanged and the client labels that stream `events`. `stderr` retains the agent's diagnostic output. This keeps the v1 two-stream log contract. The composed prompt is stored once per turn as `prompt.md` in the turn's job directory; it is user content, bounded to 256 KiB, and follows turn retention.

Local state below the v1 state root adds `tasks/<task_id>.json`, `runs/<run_id>.json`, `runners/<task_id>/<turn_id>.log`, and, for a turn awaiting acceptance, the owner-only composed prompt `turns/<task_id>/<turn_id>/prompt.md`, which is removed once the worker holds it. The transfer repository lives below the v1 cache root as `transfer/<repo_id>.git`; the task record stores its alternates target, which is verified to exist before every transfer operation. The user's repository receives exactly one write per fetch: `refs/remotes/mac-worker/<worker>/task/<task_id>` and the objects it needs. mac-worker never creates local branches, checks anything out, or touches the user's index, `HEAD`, hooks, or configuration.

The task title is an explicit optional field (already present on the batch file; `worker task submit --title` follows in a later task). When it is absent the title is derived from the first non-empty prompt line and then passed through the shared redaction boundary, so a secret or machine-local path in that line cannot leak into task metadata, queue rows, JSON output, or the dashboard.

No cleanup or path construction accepts caller-supplied paths. Every component above is a validated identifier.

## 10. Code transfer contract

### 10.1 Two sources, two publications

A task declares one `source` and a set of `publish` modes:

| Axis | Value | Mechanism | Requirement on the worker |
|---|---|---|---|
| `source` | `local` | the runner pushes `base_oid` from the transfer repository into the mirror over SSH | none |
| `source` | `origin` | the host fetches `base_oid` from the normalized origin URL into the mirror | capability `origin:<host>` |
| `publish` | `fetch` | the runner fetches `task/<task_id>` from the mirror into the transfer repository, then into the user's repository | none |
| `publish` | `push` | the host pushes `task/<task_id>` to origin as `--publish-branch` from the mirror | capability `origin:<host>` and a committed base |

Both sources fill the same mirror, so the rest of the lifecycle is identical. The mirror is also the warm cache: later pushes and fetches transfer only missing objects. `fetch` is always performed; `push` is additive.

`push` is rejected at preflight when the base is a temporary `--wip` commit, because that commit would become part of the published branch. The rejection code is `PUBLISH_REQUIRES_COMMITTED_BASE`. `--publish-branch` is validated as a branch name and must be unique within a run; without it, `push` uses `task/<task_id>`.

The mirror branch is always `task/<task_id>`. No option renames it, because two tasks sharing a mirror branch would fail at worktree creation or silently diverge on origin.

### 10.2 Transfer repository and base commits

For each local repository the client keeps a bare transfer repository under its cache root, keyed by `repo_id`, whose `objects/info/alternates` points at that repository's common object directory. Every Git write that submission needs happens there: the user's repository is read, never written, until a result is imported.

For `source = local`, the base is either the resolved `--base` commit or, with `--wip`, a temporary commit. Selection runs read-only inside the user's repository with the user's own configuration, so `info/exclude`, `core.excludesFile`, and repository-local settings apply exactly as they do for v1 snapshots: the v1 input selection lists tracked files with current filesystem bytes, tracked deletions, executable modes, symlinks unfollowed, and untracked files covered by `snapshot.include_untracked` or `--include`. Object creation then happens only in the transfer repository: each selected file is written with `hash-object -w --no-filters` from its worktree bytes, a scratch index is populated with `update-index --cacheinfo` from the selected modes and object IDs, and `write-tree` produces the tree. The selection and hashing are repeated and must produce the identical tree ID; any difference is `SNAPSHOT_CHANGED`. The commit is created with the recorded Git identity and a fixed message, parented on the resolved `HEAD` object ID, and recorded as `refs/mac-worker/bases/<task_id>` in the transfer repository. Neither the user's index nor the user's object store is touched. Refs are never resolved in the transfer repository, which shares objects through alternates but has no branches: `HEAD` and `--base` are resolved read-only in the user's repository and every later command receives the explicit object ID.

For a committed base, `SENSITIVE_PATH` runs over the names in `git ls-tree -r <base_oid>`, and history reachable from the base is transferred to the mirror and retained there (section 5). V1's `UNTRACKED_INPUT` and `SENSITIVE_PATH` preflights apply unchanged to `--wip`. Without `--wip`, a dirty worktree produces a warning that names the count of excluded changes, never their contents.

After a successful push the base ref is removed from the transfer repository; the worker mirror is the durable holder. Transfer repositories are `worker gc` candidates like other caches.

### 10.3 Transport through the host helper

Git transport reuses the fixed-argv discipline: the runner runs `git push --receive-pack='worker host receive-pack <turn_id> <client_id> <lease_token> <request_fingerprint>'` and `git fetch --upload-pack='worker host upload-pack <task_id> <client_id>'` against a remote URL whose path component is only the `project_id`; Git appends that path, quoted, as the program's last argument. The hidden host commands validate every component, resolve the mirror below the data root, and exec `git-receive-pack` or `git-upload-pack` there with the account's global and system Git configuration neutralized. `receive-pack` carries the turn's job ID first and validates the lease and request fingerprint exactly as `rsync-receive` does; `upload-pack` requires the task's metadata and the mirror branch to exist and never creates the mirror.

Both commands run from the transfer repository with `GIT_CONFIG_GLOBAL` and `GIT_CONFIG_NOSYSTEM` neutralizing the user's global configuration, `GIT_SSH_COMMAND` set to the pinned mac-worker ssh argv (the same options as the JSON transport, without a trailing `--`, because Git appends its own options before the host), `--no-verify`, `-c gc.auto=0`, and, for fetch, `--no-write-fetch-head`. User hooks, `core.sshCommand`, and URL rewriting in the user's repository therefore never apply.

The mirror is created by the host on first use with `core.hooksPath` set in its own configuration to its hooks directory, `receive.denyDeletes` enabled, and a pre-receive hook that permits only `refs/mac-worker/bases/*` to be created or updated. The host verifies hook content and mode before every `receive-pack`. Result branches are read-only from the client's side. The hook guards the client push path only; the agent shares the account and could bypass it, which the trust model accepts.

`source = origin` fetches run as `git fetch <normalized-url> <base_oid>` inside the mirror with the worker account's own credentials and a bounded timeout. The host then verifies that the object exists and is a commit; a mismatch or missing object fails the turn with `BASE_UNAVAILABLE` before any workspace is created.

### 10.4 Workspace creation

The host creates `workspace/` as a shared clone of the mirror: `git clone --shared --no-checkout <mirror> <workspace>` followed by `git checkout -b task/<task_id> <base_oid>`. The clone's `.git` directory lives inside the workspace and borrows objects from the mirror through alternates, so agent sandboxes that confine writes to the checkout (Codex's workspace-write sandbox denies writes to a linked worktree's Git directory under the mirror path, as the spike showed) can still stage and commit. Creation is idempotent for retries after a crash: an existing workspace on branch `task/<task_id>` whose `HEAD` equals `base_oid` with no local changes is reused; a partial clone without a valid `HEAD` is removed and recreated; any other inconsistency is `WORKTREE_INCONSISTENT` and no agent starts. The mirror receives the result branch only at publication (section 16). Workspaces of one mirror are independent; a task cannot see another task's uncommitted work.

### 10.5 Result import

The runner fetches `task/<task_id>` from the mirror into the transfer repository, then imports that single ref into the user's repository through a local-path fetch with `--no-write-fetch-head` and `-c gc.auto=0` as `refs/remotes/mac-worker/<worker>/task/<task_id>`. Repeated fetches are idempotent. Merging is left to the user; the CLI prints the exact `git` commands for a branch checkout or cherry-pick as guidance only.

## 11. Agent adapters

### 11.1 Contract

An adapter is a pure description of one agent CLI. It provides:

- the binary name and a bounded, read-only detection and authentication probe;
- argv for a first turn and for a resumed turn, given a workspace, prompt delivery, model, limits, and permission policy;
- the environment variable names it needs from the env profile;
- how the agent session identifier is bound before the first turn or captured from the event stream;
- the event stream format and a normalizer into mac-worker's event vocabulary;
- how the structured final result is requested and extracted;
- exit classification: agent success, agent failure, limit reached, authentication failure.

Adapters never contain worker paths, credentials, or SSH details. They are unit-tested against recorded event fixtures.

### 11.2 Supported agents

| Agent | First turn | Resume | Session binding | Structured result | Limits |
|---|---|---|---|---|---|
| Codex | `codex exec --json -C <workspace> -o <last-message> --output-schema <schema> [-m MODEL] <permission flags> -` with the prompt on stdin | `codex exec resume <session_ref> --json -o <last-message> --output-schema <schema> -c sandbox_mode="workspace-write" -c sandbox_workspace_write.network_access=true -c approval_policy="never" -` with the workspace as the working directory, because `resume` accepts no `-C`, `-s`, or `--approve-for-me`; the spike confirmed this form and that the resumed stream reports the same thread identifier | captured from the thread-started event | `--output-schema` | timeout only |
| Claude Code | `claude -p --output-format stream-json --session-id <uuid> [--model] [--max-turns] [--max-budget-usd] <permission flags>` with the prompt on stdin | `claude -p --resume <uuid>` plus the same output and permission flags | generated by mac-worker before the first turn; `--max-turns` is accepted by the CLI although absent from its help text, and the spike confirms it | `--json-schema` | `--max-turns`, `--max-budget-usd`, timeout |
| Cursor Agent | `cursor-agent -p --output-format stream-json --workspace <workspace> --trust --force [--model] --resume <chat_id> <argv pointer>` | same with the bound chat id | `cursor-agent create-chat` before the first turn returns the id | trailer convention | timeout only |
| OpenCode | `opencode run --format json --dir <workspace> --auto [--model] <argv pointer>` | `opencode run --session <id> …` | captured from the JSON event stream | trailer convention | timeout only |

Prompt delivery: Codex and Claude Code read the prompt from stdin. Cursor Agent and OpenCode accept only a positional prompt, so they receive a short fixed argv pointer that instructs the agent to read the task from the turn's `prompt.md`, whose path is inside the account's own data root; the prompt itself never appears in argv or the process list.

Session binding has no fallback. A resume without a bound `session_ref` fails with `SESSION_UNBOUND`; adapters never use "continue the most recent session" forms, because they are not scoped to a directory for every agent and could resume another task's session on the same worker.

The binding is persisted as early as it is known, never only at the end of a turn: an identifier mac-worker generates is written to `session.json` when the turn is accepted, and an identifier the agent generates is written by the supervisor the moment the session-started event appears in the stream. A first turn that is cancelled, times out, or is lost therefore still leaves a bound session for `say`; reconciliation of a `lost` turn re-extracts the identifier from the recorded stream when the file is missing.

### 11.3 Prompt composition and structured result

A turn's prompt is the user's text followed by a versioned preamble that states: the workspace is a dedicated Git worktree on branch `task/<task_id>` based on `<base_oid>`; commit completed work with descriptive messages; do not push, open merge requests, switch branches, or touch other worktrees unless the task says so; do not attempt to ask interactive questions; finish with a structured result.

The structured result is:

```json
{ "status": "done" | "needs_input" | "blocked",
  "summary": "…",
  "questions": [{ "text": "…", "options": ["…"] }],
  "files_changed": ["…"] }
```

A question's `options` are the answers the agent will accept, so an orchestrator selects one instead of parsing prose; an empty list means the question is open. A question without options is stored and returned as a bare string, so records written before options existed round-trip unchanged and an agent that still emits a string array is still understood.

Codex and Claude Code are asked for it through their schema options. Cursor Agent and OpenCode are asked to end with a fenced `mac-worker-result` JSON block, which the adapter extracts; a missing or malformed block yields `status: "unknown"` and does not fail the turn. The preamble contains no secrets, paths beyond the workspace-relative branch name, or worker identity.

The supervisor keeps draining the agent's stdout for the whole turn, so the agent never blocks on a full pipe. When the recorded event stream reaches its cap (section 13.3) the supervisor stops appending but keeps the final 64 KiB of the stream in memory and records it as the turn's tail, so the structured result of a long turn is never lost. Draining stops when the agent's process group is proven absent, never on waiting for the pipe to close, so a process the agent detached while holding the pipe cannot delay the turn's terminal state.

### 11.4 Permission policy

Turns are unattended, so an agent must never block on a permission prompt. Two policies exist per agent:

- `unattended`: the agent's full bypass mode (Codex `--dangerously-bypass-approvals-and-sandbox`, Claude `--permission-mode bypassPermissions`, Cursor `--force`, OpenCode `--auto`).
- `workspace`: the agent's native workspace-write sandbox with approvals disabled (Codex `-s workspace-write -c approval_policy="never" -c sandbox_workspace_write.network_access=true`; the spike showed that `--approve-for-me` is incompatible with `--sandbox` and that `--ask-for-approval` is not accepted by `codex exec`, so the configuration override is the working form); other agents fall back to `unattended` and the fallback is recorded in the turn's metadata.

The default is `workspace` for Codex and `unattended` for the rest, overridable in `.worker.toml`. A permission prompt that still appears is treated as a hang and ends with the turn timeout. The spike showed that Codex's workspace sandbox denies writes to a linked worktree's Git directory under the mirror path, which is why the task workspace is a shared clone with its own `.git` inside the writable checkout (section 10.4).

## 12. Task and turn lifecycle

### 12.1 Turn states

A turn is a v1 job and uses the v1 lifecycle unchanged:

```text
accepted -> running -> succeeded | failed | cancelled | timed_out | lost
```

`succeeded` means the agent process exited zero and the publisher completed; the agent's reported status is a separate field.

### 12.2 Task states

```text
queued    first turn not yet accepted; cancellable locally without remote effect
active    a turn is accepted or running on a worker
open      no turn is running; workspace, mirror branch, and session retained; accepts say, fetch, close
closed    terminal; result branch retained in the mirror until retention expires; workspace removed; metadata retained
abandoned terminal; no result was produced or the user discarded it
lost      terminal; reconciliation found the workspace or session missing
```

`open` carries `last_outcome`, one of `done`, `needs_input`, `blocked`, `unknown`, `failed`, `cancelled`, `timed_out`, `lost`. A `lost` turn (its supervisor vanished, typically after a worker reboot) leaves the task `open` with `last_outcome = lost` and the workspace in place for inspection; the task itself becomes `lost` only when the workspace or the mirror branch is missing. The task is never marked terminal because an agent failed: the user can `say` to retry with guidance, or `close --discard`.

Transitions:

- `queued -> active` on durable acceptance of the first turn; `queued -> abandoned` on `cancel` before acceptance or on a pre-acceptance failure the runner resolved as abandoned.
- `active -> open` when the turn reaches any terminal state, including `lost`, and the lease is released.
- `active -> closed` when the turn's structured status is `done`, the task has `--close-on done`, and publication succeeded.
- `open -> active` on `say`, bounded by `max_followups`.
- `open -> closed` on `close`, on `--close-on done` after a later turn reports `done`, or by retention.
- `open -> abandoned` on `close --discard`.
- `open | active -> lost` only through reconciliation evidence of a missing workspace or branch, never through a failed probe.

### 12.3 Follow-ups

`say` refreshes the task from its worker, refuses `active` with `TASK_BUSY`, refuses `closed`, `abandoned`, and `lost` with `TASK_CLOSED`, and enforces `max_followups` with `FOLLOWUP_LIMIT`. It then creates a new turn with the user's message as the prompt body and a shorter resume preamble, pinned to the worker holding the session. The pin is hard: if that worker is busy the turn waits behind older entries for that worker and never moves to another worker; under per-worker FIFO it does not delay entries for other workers. Messages are never injected into a running agent.

A cancelled turn leaves the session intact on disk. The next `say` resumes it, losing at most the last in-flight tool call.

### 12.4 Retention

An `open` task with no turn for `task.retention` (default seven days) is closed by `worker gc --apply` with its result branch preserved. Result branches are pruned from the mirror by `gc --apply` after `task.branch_retention` (default thirty days) or immediately with `close --discard`. Task metadata and turn job directories follow v1 job retention. `gc` continues to preview every candidate with a reason and never touches the mirror's other refs.

## 13. Scheduling, capabilities, and limits

### 13.1 Capabilities

The host reports, in addition to v1 facts, the installed agents with version and an authentication check per adapter, the names and permission state of env profiles, and whether the account has a Git identity configured. These agent facts are not collected on the probe's hot path, because each check launches an agent CLI: they are gathered by a separate host operation, `refresh-facts`, invoked by `worker setup`, by `worker workers --refresh`, and by a runner before claiming when the cached facts are older than their TTL (default fifteen minutes). The read-only probe returns the cached facts with their age; facts older than the TTL are reported but count as `unknown` for capability purposes, so the dashboard's poll and every claim stay within the per-host deadline.

Because a keychain-backed login is invisible from SSH, a check reports `authenticated`, `unauthenticated`, or `unknown`; only `authenticated` satisfies a capability. Each check runs once with the account's plain environment and once under each secure env profile, so authentication is a fact about a worker and a profile:

- `agent:<name>` means the agent is authenticated without any profile;
- `agent:<name>@<profile>` means it is authenticated with that profile applied.

A task requires `agent:<name>@<profile>` when it names an env profile and `agent:<name>` otherwise. The operator declares origin access manually in the inventory:

```toml
[[workers]]
name = "mini-1"
ssh = "mac1"
slots = 1
capabilities = ["darwin-arm64", "origin:gitlab.example.com"]
```

A task requires `origin:<host>` when `source = origin` or `push` is requested. Unmet requirements exclude a worker exactly as v1 capabilities do; a pinned worker that lacks them fails with `CAPABILITY_MISSING` rather than being rerouted.

### 13.2 Admission and ordering

The phase 4 queue is extended with an entry `kind` (`batch`, `task_turn`), an optional hard pin, and an optional run reference. Admission is per-worker FIFO as amended in section 5.1: each idle ranked worker takes the oldest waiting row eligible for it; rows that no idle worker can take do not block younger rows for other workers; among rows eligible for one worker the older row always wins. First turns rank like batch jobs: worktree affinity, project affinity, memory, disk, name. Follow-up turns have a hard pin and are eligible only for that worker. A claim is owner-scoped: a runner claims only the row it owns, and only when no older waiting row with a live owner is eligible for the same idle worker. Parked rows have no owner, are never claimed, and are unparked oldest-first, so they are always younger than every owned waiting row.

Batch jobs and task turns share the same lease, so a worker runs at most one of either at a time. Admission thresholds for disk, memory pressure, and swap are unchanged.

`--no-wait` returns `CAPACITY_BUSY` after the fleet probe and before the base commit, the task record, the queue row, or any remote mutation. Because admission happens later in the runner, the flag is persisted with the task: a runner whose first claim finds no eligible worker for such a task removes the row, releases the base, and marks the task `abandoned` with `CAPACITY_BUSY` instead of waiting; `submit --no-wait --wait` keeps the synchronous exit status `75`. A run's `max_parallel` caps how many of its tasks may be `active` simultaneously; the default equals the number of configured workers. The cap is enforced inside the claim, under the local queue lock, from local state only: sibling rows that are `dispatching` plus sibling tasks recorded `active` locally, with acceptance recorded under the same lock. A runner may refresh its siblings from their workers before taking the lock, but that refresh only updates local records and never runs while the lock is held. A claim consumes a cap slot until it is reverted or its turn ends, so two sibling runners cannot both admit the last slot.

### 13.3 Limits

| Limit | Scope | Enforcement |
|---|---|---|
| `timeout` | one turn | supervisor: TERM, ten seconds, KILL, `timed_out` |
| `max_turns`, `max_budget_usd` | one turn | passed to agents that support them; recorded as `unsupported` otherwise |
| `max_followups` | one task | client and host refuse further `say` with `FOLLOWUP_LIMIT` |
| `max_parallel` | one run | runner admission check under the local lock |
| `slots = 1` | one worker | lease |
| log bounds | one turn | v1 64 KiB chunks; the recorded event stream capped at 256 MiB, after which the supervisor keeps draining, records only the final 64 KiB tail, and marks the turn `log_truncated` |

## 14. Interaction with a running or open task

- `logs -f` follows the normalized event stream: assistant messages, tool calls with bounded summaries, file changes, commands with exit codes, usage, and turn end, rendered by the adapter's parser on the client. `--raw` streams the raw stdout bytes instead. Both resume from byte offsets and survive disconnects.
- `diff` runs `git diff` (or `--stat`) against `base_oid` in the workspace through a hidden host command, using a private copy of the workspace index so it never refreshes or locks the agent's index. It is read-only, bounded to 512 KiB per call with an explicit truncation flag, and always includes committed and uncommitted changes since the base.
- `status` shows the task state, last outcome, summary, questions with their options, session presence, branch, diff summary, turn history, and runner state.
- `list --outcome KIND` filters by the recorded `last_outcome` kind and composes with `--state`; `--state open --outcome needs_input` is the orchestrator's query for tasks waiting on an answer.
- `say` is the only way to talk to the agent; it always starts a new turn.
- `cancel` stops the active turn's process group exactly as v1 cancellation does, then leaves the task `open`.

Live typing into a running agent, watching its TUI, or answering its permission prompts is out of scope here and described in section 22.

## 15. Environment, credentials, and secrets

A turn inherits the worker account's real `HOME` and the environment of its login shell (`zsh -lc`, the same resolver the probe uses to locate agent binaries), plus `USER`, `LOGNAME`, and `SHELL` for the account, so agent configuration, skills, MCP servers, tool versions, and file-based logins resolve as they would interactively. The supervisor adds `MAC_WORKER_*` metadata including `MAC_WORKER_TURN_DIR`, a per-turn `TMPDIR`, the Git identity recorded in task metadata as `GIT_AUTHOR_*` and `GIT_COMMITTER_*`, and the variables of one named env profile. The agent's file arguments are written as `"$MAC_WORKER_TURN_DIR/..."` references expanded by the login shell, so the fingerprinted command contains no worker paths and the client never needs to know the worker's data root.

Env profiles are files the operator places on each worker at `~/.config/mac-worker/env/<name>.env` with mode `0600`. They are the intended home for `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`, `CURSOR_API_KEY`, and any provider variables OpenCode needs. The probe reports a profile that is group- or world-readable as a warning, and a turn that references such a profile fails before launch with `ENV_PROFILE_PERMISSIONS`; mac-worker never creates, uploads, or prints a profile. Codex's file-based login needs no profile.

The Git identity used for agent commits and publisher commits is recorded in task metadata at submission from the submitting user's Git configuration (`user.name`, `user.email`), which is not secret, with a fixed `mac-worker` identity as the fallback. It is exported into the turn environment so commits succeed on a worker with no identity configured and never carry the worker's hostname.

Keychain-backed logins are unsupported for headless turns: the probe reports them as `unknown` and the documentation explains the env-profile alternative rather than unlocking the keychain from mac-worker.

mac-worker's own records list environment variable names, never values. Agent transcripts, prompts, and diffs are application content and can contain anything the account can read; the loopback dashboard and `--json` output render them as text and do not sanitize them. Agent session stores outside the data root are described in section 6.

## 16. Results and publication

Every terminal path of a turn runs the publisher before the lease is released: the agent process exiting, the timeout, a prelaunch failure, an ambiguous child identity, host cancellation, and lost-turn reconciliation. The supervisor invokes it from its single status writer whenever a state becomes terminal, so no writer can be missed. Where no workspace exists yet, the publisher only records the outcome. A turn therefore can never leave its task `active`. At the end of every turn the publisher, running on the worker under the still-held lease:

1. checks the workspace with `git status --porcelain`; if anything is uncommitted, creates a commit `mac-worker: uncommitted changes after turn <turn_id>` on the result branch with the recorded Git identity and records `agent_committed = false` for that turn;
2. publishes the branch into the mirror with `git -C <mirror> fetch <workspace> +refs/heads/task/<task_id>:refs/heads/task/<task_id>`, a fetch rather than a push so the client-facing pre-receive hook does not apply and the mirror stays the single fetch source for the client;
3. records the branch head, `git diff --stat` and the changed-file list against `base_oid`, bounded;
4. extracts the structured result and the final assistant message from the recorded stream or tail, and verifies that the session binding recorded during the turn (section 11.2) is present, re-extracting it from the stream if it is not;
5. if `publish` includes `push`, pushes the result branch to origin as `--publish-branch` with the account's credentials and a bounded timeout; a push failure is recorded as `PUBLISH_FAILED` and leaves the task `open` with the branch intact.

`fetch` on the MacBook imports the result as described in section 10.5.

## 17. Cleanup and disk management

Closing a task removes the workspace clone directory below the task directory through the rooted filesystem layer, and keeps the task's metadata, session binding, turn job directories, and mirror branch. `close --discard` additionally deletes the mirror branch and the base ref and requests agent-native session deletion where the agent offers it (Codex has `codex delete <session>`). Worker reconciliation after a reboot marks turns without a live supervisor as `lost`, moves their tasks to `open` with `last_outcome = lost`, recovers a missing session binding from the recorded stream, and lets the user `close` or `say` after reviewing `diff`.

`worker gc` gains task candidates: open tasks past retention, task metadata and turn logs past job retention, result branches past branch retention, mirrors with no tasks and no refs younger than branch retention, and transfer repositories with no base refs. Every candidate is previewed with a size and reason. The mirror is never deleted while any task references it, and `git gc` on a mirror runs only through `worker gc --apply` with a bounded runtime.

Disk admission is unchanged. Because mirrors are shared caches, `gc --apply` prefers pruning branches and logs before mirrors.

## 18. Error model

New stable codes, grouped in the v1 classes plus two new classes:

- `project`: `TASK_CONFIG_INVALID`, `PUBLISH_REQUIRES_COMMITTED_BASE`, `AGENT_UNSUPPORTED`, `BASE_NOT_ON_ORIGIN`.
- `snapshot`: `SNAPSHOT_CHANGED`, `SENSITIVE_PATH`, `UNTRACKED_INPUT` as in v1.
- `capacity`: unchanged, plus `CAPABILITY_MISSING` for pins.
- `git` (new): `BASE_PUSH_FAILED`, `BASE_UNAVAILABLE`, `WORKTREE_CREATE_FAILED`, `WORKTREE_INCONSISTENT`, `RESULT_FETCH_FAILED`, `PUBLISH_FAILED`.
- `infrastructure`: `HOST_LAYOUT_OUTDATED`, reported by the probe as unavailable until `worker setup` migrates the worker.
- `agent` (new): `AGENT_NOT_INSTALLED`, `AGENT_NOT_AUTHENTICATED`, `AGENT_EXITED`, `AGENT_LIMIT_REACHED`, `RESULT_UNPARSEABLE`, `SESSION_UNBOUND`, `ENV_PROFILE_PERMISSIONS`.
- `task`: `TASK_BUSY`, `FOLLOWUP_LIMIT`, `TASK_CLOSED`, `TASK_NOT_FOUND`, and `RUNNER_HANDOFF_FAILED`, which alone maps to the local I/O exit status `74`.

CLI exit codes keep v1 semantics: `64` usage and configuration, `69` pre-acceptance transport, `70` protocol or infrastructure, `74` local I/O, `75` capacity. Commands that end with a turn map the turn's outcome as follows:

| Turn outcome | `submit --wait`, `say --wait` | `wait` (any task in the set) |
|---|---|---|
| agent exited zero, status `done`, `needs_input`, or `unknown` | `0` | `0` if every task ended this way |
| agent exited zero, status `blocked` | `1` | `1` |
| agent exited non-zero with code N | `N`, unchanged, public code `AGENT_EXITED` | `1` |
| signalled, timed out, or cancelled | the status v1 assigns to a signalled, timed-out, or cancelled command | `1` |
| turn `lost`, `PUBLISH_FAILED`, `RESULT_UNPARSEABLE` | `70` | `1` |
| `wait --timeout` elapsed | not applicable | `70`, nothing cancelled |

The durable task record remains the authoritative distinction; JSON output always carries the outcome and code alongside the exit status.

## 19. Observability and dashboard extension

Every turn records the v1 job facts plus: task, run, and turn identifiers; agent, version, model, and permission policy; base object ID and branch; session presence; structured status; token and cost usage when the agent reports them; changed-file count and diff size; publication outcome; whether the log was truncated; and the runner's identity and exit.

The dashboard projection gains, under the existing read-only and loopback rules:

- worker cards show the running turn's agent, task title, and elapsed time;
- a **tasks** view listing runs and tasks with state, last outcome, agent, worker, branch, runner state, and age, with run-level progress;
- the queue shows entry kind, pins, run caps, and the blocking reason under per-worker FIFO;
- task detail replaces raw stdout with the normalized event timeline, keeps the raw log, and adds a result card with summary, questions, files changed, and the exact `fetch` command;
- the log panel polls once per second while a turn is active, as in the dashboard design.

Dashboard task cancellation and other task-lifecycle mutating controls remain deferred, as the dashboard design requires. The native model, effort, and Fast Settings Save operation is already authorized and remains available as a separate settings operation; it changes the selected agent's native defaults without changing task state. `worker task list --json` and the snapshot endpoint share one projection, so the orchestrating agent and the browser see identical state.

## 20. Verification strategy

### 20.1 Automated tests

- Adapter argv, environment, session binding, and result extraction against recorded event fixtures for all four agents, including malformed and truncated streams and the tail-retention path.
- Transfer repository and base commit creation: the user's repository is byte-identical before and after (`HEAD`, index, working tree, refs, reflogs, configuration, hooks); `--wip` selection matches v1 input policy including `info/exclude` and `core.excludesFile`; objects are written only into the transfer repository; two clones of one origin get separate transfer repositories keyed by `repo_id`, and a missing alternates target fails before any transfer; the second-capture check fires `SNAPSHOT_CHANGED`; sensitive-path and untracked preflights fire; base refs are removed after push.
- Host mirror commands: turn-keyed identity validation, hook enforcement with the account's global `core.hooksPath` set, deletions denied, upload-pack serves only the mirror and only for a task with metadata and a branch, no path escapes, ssh argv without a trailing `--`.
- Materialization: base verification, idempotent shared-clone creation on retry, isolation between two tasks on one mirror, `WORKTREE_INCONSISTENT`, failure before any workspace when the base is missing, commits inside the clone under Codex's workspace-write sandbox.
- Turn lifecycle: stdin prompt delivery, login-shell environment with real `HOME`, Git identity export, exit classification, timeout, cancel, lost supervisor, log cap with tail retention, publisher commit of leftovers, idempotent re-publication, workspace preserved at terminal state.
- Task state machine: every transition in section 12, `say` rejection while active, follow-up limits, hard pins, close-on-done, authority order after a killed follower, retention.
- Runner: detach and handoff, handoff failure abandoning the task and releasing the base, ownership recorded in the waiting state, crash before and after acceptance, replacement only by mutating commands, `list` and `status` never mutating or spawning, `wait` completing a batch whose shell exited, parking behind the per-worker cap, backoff, the shared observation cache with single-flight refresh, `worker run` never reaping `task_turn` rows, re-enqueue of a `queued` task without a row, no runner for terminal tasks.
- Scheduler: entry kinds, pins, per-worker FIFO with a busy pinned head, `max_parallel` under the lock with two sibling runners racing for the last slot, capability requirements for agents, profiles, and origins, stale agent facts counting as `unknown`, `--no-wait` before any mutation.
- Session binding persisted at acceptance or at first observation, surviving cancel, timeout, and loss of the first turn; recovery from the recorded stream.
- Privacy: no env values, credentials, SSH configuration, or local paths in task records, queue rows, JSON output, or dashboard projections; env profile permission checks; prompts absent from argv.
- Dashboard: projection extensions, timeline rendering with text-only output, result card, run progress.

### 20.2 Live acceptance

Against the three configured workers, with sanitized records only:

1. Five tasks submitted as one run with the default `max_parallel` from a shell that exits immediately: three become `active` on three distinct workers, two wait with a visible blocking reason, and `worker task wait --run` from a new shell completes all five.
2. A task whose agent reports `needs_input` becomes `open`; `say` resumes the same session on the same worker, and the transcript shows continuity.
3. `cancel` during a turn, then `say`: the session resumes and the task completes.
4. A follow-up pinned to a busy worker waits, never moves, and does not delay a first turn admitted to an idle worker.
5. `--wip` base: the user's repository is byte-identical after submission; the result branch contains the temporary commit; `push` is refused at preflight for it.
6. `source = origin` and `publish = push` route only to workers with the declared origin capability; the pushed branch appears on origin under `--publish-branch`.
7. Disconnect during `logs -f` and during `submit --wait`, plus a killed runner mid-turn: exactly one turn ran, the next command replaces the runner, and status and logs reconnect by the original identifiers.
8. Dashboard shows every state above truthfully through its worker cards, queue, and job views (turns are jobs), and its shutdown changes nothing; the dedicated tasks view is phase 5e and is not required here.
9. Retained metadata contains no planted secret values and no complete local paths.
10. A worker without a Git identity produces correctly attributed commits. The locked-keychain proof for Claude Code and Cursor Agent through env profiles moves to the phase 5d acceptance, because Claude is deferred on the workers by operator decision and Cursor is wired in phase 5d.

### 20.3 Go/no-go thresholds

- 200 turns across the pool produce zero duplicated turns, zero messages delivered into a running agent, and zero follow-ups executed on the wrong worker.
- 100 injected disconnects and 50 killed runners produce zero duplicated turns and zero permanently unknown accepted turns.
- 50 mixed cancel, timeout, and crash turns leave no orphan agent processes, no unowned queue rows, and no unregistered worktrees after cleanup.
- No planted secret appears in mac-worker records, queue rows, JSON output, or dashboard responses.
- Median time from submission to a running agent on an idle worker with a warm mirror is under thirty seconds.
- Retained data on every worker stabilizes within configured limits without global pruning.

## 21. Delivery plan and boundary

Prerequisites, in order: phase 3 lands in `main` with its live acceptance recorded; the phase 4 scheduler lands with queue, cancel, and reconciliation, including the per-worker FIFO and detached-owner amendments in section 5.1; the dashboard branches are consolidated.

Then:

1. **Spike.** A short, disposable shell prototype on one worker with Codex and one project proves headless authentication over SSH, the structured result, base push and result fetch through plain Git with program overrides, Codex commits inside a mirror worktree under its sandbox, `--max-turns` on Claude, and the memory profile of one agent plus a test suite. Its findings are recorded in `docs/agent-task-spike.md`; the prototype is not merged into the binary.
2. **Phase 5a: transfer and materialization.** Transfer repositories, base commits, mirrors, hidden Git transport commands, worktree creation, result import.
3. **Phase 5b: turns and adapters.** Codex and Claude adapters first, turn payload and supervisor amendments, task and turn records, `submit --wait`, `status`, `logs`, `diff`, `close`.
4. **Phase 5c: runners, conversation, and limits.** Detached runners and recovery, `say`, pins, follow-up limits, `cancel`, runs and `batch`, `wait`, the orchestrator skill.
5. **Phase 5d: publication and capabilities.** Profile-keyed agent authentication probes, origin capabilities, `source = origin`, `publish = push`, Cursor and OpenCode adapters, retention through `gc`.
6. **Phase 5e: dashboard extension.**

The v2 execution core is complete when acceptance in section 20.2 passes for Codex on the three configured workers; Claude joins that acceptance when the operator re-enables it on the workers. Cursor and OpenCode may follow without a new design review; the items in section 22 may not.

## 22. Deferred designs

Each of these requires its own review before implementation:

- **Interactive mode.** A turn that runs the agent's TUI inside a Herdr pane on the worker, attachable with Herdr's remote session support, so the user can type to the agent and answer its prompts. It trades the exit-code completion signal for Herdr's heuristic agent states and weakens limits; it must be an explicit `--interactive` flag, never a default. Its observability half, a read-only pane and reported states for a headless turn, is specified separately in the [herdr reporter design](2026-09-08-herdr-reporter-design.md) and does not require this item.
- **Long-lived Claude turns.** Claude Code accepts streaming JSON input, so one process could stay alive across follow-ups and accept messages while working. This changes the "no message into a running agent" rule and is deferred until the turn model is proven.
- **Merge request creation** by mac-worker after `push`.
- **Dashboard task cancel** and other task-lifecycle mutating controls, under the dashboard design's token and same-origin requirements. The native model, effort, and Fast Settings Save operation is already authorized and is outside this deferral.
- **Non-loopback dashboard access**, for example over Tailscale, for watching long runs from another device.
- **Multiple slots per worker** for lighter agent workloads.

## 23. Alternatives considered

### Orca remote runtimes with its orchestration commands

Orca can start a supervised worker on a paired remote runtime, dispatch tasks, and collect `worker_done` reports without new code. It does not schedule or limit workers, its workers are interactive TUIs, and it requires an Orca runtime daemon with pairing on every worker plus the Orca app on the MacBook as the run home. That is the control-plane daemon v1 deliberately avoided, and it cannot enforce the one-slot, FIFO, and no-injection rules this design needs. It remains a reasonable way to try the workflow before building.

### Cursor self-hosted machine pools

Closest in topology, but the model loop runs in Cursor's cloud, every tool call is a round trip, pool workers require a service account on an enterprise plan, and only Cursor's agent can be used.

### Herdr remote sessions alone

Herdr already runs on every worker and can attach to remote sessions. It gives a live view and interactive control but no queue, no limits, no durable task records, and no result publication. It is the basis for the deferred interactive mode, not a replacement for this design. Since herdr 0.9 (2026-09-08) the same server also renders states that an external source reports through its socket, which the [herdr reporter design](2026-09-08-herdr-reporter-design.md) uses to show headless turns without changing this design.

### Blocking `submit` instead of local runners

Requiring the submitting shell to stay attached until acceptance would remove the runner, but an orchestrating agent submitting five tasks to three workers would then be serialized behind capacity, and a follow-up sent from a fresh shell would need the same machinery anyway. Bounded per-turn runners keep the no-daemon rule and match the host supervisor pattern.

### Keeping v1 and running agents on the MacBook

Does not offload the agents, which is the stated goal.

### Hand-written shell scripts over SSH

Adequate for the spike, inadequate for durability: they cannot safely answer whether a disconnected turn ran, cannot prevent duplicate turns, and cannot keep cleanup rooted.

## 24. Consequences

This design accepts agents, their credentials, and outbound model traffic on the workers, and in exchange keeps everything else from v1: a locked directory plus bounded helper processes instead of a control plane, SSH with fixed argv instead of listeners, one lease per worker, durable acceptance, reconnectable logs, and rooted cleanup. The new complexity concentrates in five contracts: the transfer repository as the only place submission writes, the mirror as the single transfer channel, the turn as a job, the runner as a bounded owner, and the structured result as the only signal an agent has to speak to the user. Everything that varies between agents lives in adapters; everything that varies between projects lives in `.worker.toml`; and the orchestrating agent on the MacBook needs nothing but the CLI.

## 25. Review record

Revision 2 incorporates the design review of 2026-09-03. Material changes from revision 1:

- Added the local turn runner (section 8.2) and the authority order (section 8.3); revision 1 promised immediate return without saying who dispatched queued turns after the CLI exited.
- Added the transfer repository (section 10.2) so submission never writes into the user's repository and user hooks or configuration cannot affect transport; base refs are removed after push.
- Replaced head-of-line FIFO with per-worker FIFO and listed every phase 3 and phase 4 amendment (section 5.1).
- Fixed the mirror branch at `task/<task_id>`; `--branch` became `--publish-branch`.
- Turn jobs moved under the v1 `jobs/` tree; `close` keeps task metadata and turn logs and removes only the workspace.
- `diff` uses a private index copy and is bounded to 512 KiB; `git diff` was shown to rewrite the workspace index even with optional locks disabled.
- Added `lost` as a turn outcome that leaves the task `open`, idempotent worktree creation, `WORKTREE_INCONSISTENT`, `SESSION_UNBOUND`, `BASE_NOT_ON_ORIGIN`, `ENV_PROFILE_PERMISSIONS`, `RUNNER_HANDOFF_FAILED`, and a complete exit-code table.
- Profile-keyed agent capabilities, Git identity export, login identity variables, OpenCode `--auto` and its loopback server, argv pointer prompt delivery for Cursor and OpenCode, Codex resume flags, and the note that Claude's `--max-turns` is accepted though undocumented.
- The event-stream cap keeps draining and retains a tail so the structured result survives; the sensitive-path check runs over the base tree and history transfer is stated in section 5.

Revision 2.1 incorporates the re-review of revision 2:

- Runners own their queue rows in the waiting state too; dead-owner `task_turn` rows are re-owned rather than reaped, `worker run` skips them, and orphaned `queued` tasks are re-enqueued. A failed handoff abandons the task instead of leaving a record without an owner.
- Recovery runs only in mutating commands and the new `worker task reconcile`; `list`, `status`, and the dashboard never mutate or spawn.
- Waiting runners are capped at one per configured worker, park the rest, back off, and share one per-worker observation cache, so a deep queue cannot flood the workers with SSH probes.
- The run cap is enforced under the local lock with dispatching and not-yet-refreshed siblings counted, closing the race between two sibling runners.
- Agent, profile, and identity facts move off the probe's hot path into `refresh-facts` with a TTL; stale facts count as `unknown`.
- Session bindings are persisted at acceptance or at first observation, so a cancelled, timed-out, or lost first turn still accepts `say`.
- Selection for `--wip` runs inside the user's repository with its configuration; only object creation happens in the transfer repository, which is keyed by `repo_id` rather than `project_id`.
- Codex resume names its configuration equivalents for the sandbox, network access, and approvals.

Revision 2.2 incorporates the re-review of the implementation plan against the phase 3 code:

- The v1 fingerprint material is not extended; the turn material commits through the digest slot and travels beside the material, so every host path that re-derives the fingerprint from persisted fields keeps working.
- Agent file arguments are login-shell environment references under `MAC_WORKER_TURN_DIR`; the client never learns a worker's data root.
- Every terminal path of a turn, including host cancellation and prelaunch failure, runs the publisher before lease release.
- Turn stdout goes through a supervisor-owned pipe so the log cap and tail are real; batch jobs keep the direct descriptor.
- The host layout migration runs only in `worker setup`; outdated workers report `HOST_LAYOUT_OUTDATED`.
- Refs are resolved only in the user's repository; the transfer repository receives explicit object IDs.
- `--no-wait` is persisted with the task and honoured by the runner's first claim.
- Claims are owner-scoped so a runner can never be handed another task's row, and every turn's composed prompt is persisted locally until the worker holds it, so a replacement runner can still submit a follow-up.
- The layout migration is a hidden host command run by the setup script; the turn acceptance receipt is minted by the acceptance step itself, because the staging nonce it must carry exists only there; the stdout pump stops at the process-group-absence proof rather than pipe EOF; and the terminal hook lives in the supervisor's single status writer so the ambiguous-child path is covered.

Revision 2.3 incorporates the Task 0 spike record (`docs/agent-task-spike.md`):

- The task workspace is a shared clone of the mirror rather than a linked worktree: Codex's workspace-write sandbox denied writes to a linked worktree's Git directory under the mirror path, so no commit was possible. The publisher now brings the result branch into the mirror with a fetch at turn end.
- Codex's `workspace` policy uses `-c approval_policy="never"` because `--approve-for-me` is incompatible with `--sandbox` in the installed CLI; `--ask-for-approval` is not accepted by `codex exec`.
- Confirmed for Codex: the `thread.started` event carries the session identifier, the `-o` file holds the schema-shaped result, resume after `TERM` keeps the session and both edits, `codex delete --force` removes the canonical session, and a failing turn can exit `0` with structured status `blocked`, which is why exit classification consults the structured status first.
- Git's automatic identity fallback produced a commit on a worker without a configured identity, confirming that the launcher must export the recorded identity.
- Claude Code is deferred on the workers by operator decision for now; its adapter is encoded from help text and unverified live. The memory profile remains unproven because the offline Cargo cache on the worker could not run the suite; it is re-measured during phase 5 acceptance.

Revision 2.4 adds reasoning-effort passthrough and structured question options:

- `submit`, batch files, and `.worker.toml` carry `effort` beside `model`; the value is recorded in the task metadata and the turn material, and reaches only the agents that accept one. Codex receives `-c model_reasoning_effort="<value>"` on the first turn and on resume; Claude, Cursor, and OpenCode ignore it exactly as they already ignore `max_turns` and `max_budget_usd`. The value is restricted to ASCII letters, digits, `-`, and `_`, at most 32 bytes, so it cannot smuggle a second configuration override into the launcher argv.
- `effort` is written into the task record and the turn material only when it is set, and is defaulted when absent, so a task created before the field existed keeps its exact canonical bytes and its turn digest and still loads after the upgrade.
- A task's effort is a per-turn override of whatever default the agent holds on the worker: it travels in the launcher argv and never edits the agent's own configuration. A task that sets no effort leaves the worker's native default in force, and the task projection reports no effort for it.
- `.worker.toml` also gains `model`, so a project that always uses one model and effort configures both once. The CLI flag wins over the project default, and a batch task's key wins over its batch defaults.
- A question is now `{ "text", "options" }`. Options are bounded like every other agent-supplied field: at most 8 per question, 256 bytes each, redacted through the same boundary. A question with no options serializes as a bare string, so existing records and agents that still emit string arrays keep working.
- `worker task list` gains `--outcome KIND`, matching the serialized `last_outcome` tag and accepting the dashed spelling, so the orchestrator loop can ask for `needs_input` without reading every row.
- `PROTOCOL_VERSION` moves to 5 because both the turn material and the task status changed shape; every worker needs `worker setup` rerun before it is eligible again.
