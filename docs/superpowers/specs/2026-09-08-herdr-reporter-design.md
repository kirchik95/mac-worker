# Herdr reporter design: pool turns in the herdr sidebar

- Date: 2026-09-08 (revision 1, written after the live spike of the same evening; see section 18)
- Status: approved for implementation on 2026-09-08 (decisions in section 18)
- Repository: `mac-worker`
- User-facing surface: `[[workers]] herdr = true` and `[notifications] herdr` in `config.toml`; new `herdr:` lines in `worker doctor`, `worker setup`, and `worker workers`; a hidden `worker host follow-turn` command that only herdr panes invoke
- Depends on: the [v2 agent task design](2026-09-03-agent-task-pool-design.md) sections 8, 11.3, 12, 15, 18, and 19; herdr 0.9.0 (socket protocol 22) on every worker that opts in and on the MacBook
- Amends: v2 sections 22 and 23, which treated herdr remote sessions as a future basis only

## 1. Decision

Add an opt-in **herdr reporter** to the worker helper. While an agent turn runs on a worker that has `herdr = true`, the helper opens one tab in that worker's own herdr server, runs a read-only log follower in its pane, and reports the turn's state through herdr's socket API as an external agent source. The turn shows up in the herdr sidebar on the MacBook next to the operator's local agents, with the task title as its row, `working` while the agent runs, `blocked` when it needs input, `done` when it finished, and a live rendering of the agent's event stream in the pane. Follow-up turns replace the tab; `task close` removes it.

Add **laptop notifications**: the local turn runner tells the MacBook's own herdr server, when one is running, that a turn ended, with the outcome in the title and herdr's `done` or `request` sound.

Add **herdr facts**: `refresh-facts` records whether a worker has a herdr binary and a live socket, and `doctor`, `setup`, and `workers` print the result, with a warning when a worker is configured for the reporter but cannot reach it.

Nothing about how a turn is scheduled, launched, supervised, published, or recorded changes. The reporter is a bounded, best-effort side channel that can only fail towards silence. It never alters an outcome, an exit code, a lease, or a record, and it is off unless the operator turns it on per worker.

## 2. Context

### 2.1 What herdr 0.9 changed

Herdr 0.9 renders its outer UI on the client and attaches to several servers over SSH, so one TUI on the MacBook now lists the workspaces, tabs, and agents of every connected machine. The three Mac minis run herdr 0.9.0 and are saved as machines on the MacBook. Herdr's own agent CLI still cannot see agents on other machines, and its state detection is screen-based, so a headless `codex exec` driven by mac-worker is invisible to it.

Mac-worker does not need herdr's cross-machine CLI. The helper already runs on the worker, as the same account that owns the worker's herdr server, and can talk to the local socket `~/.config/herdr/herdr.sock` directly. Herdr's client and the operator's herdr-mirror plugin then carry the result to the MacBook.

### 2.2 What the spike verified

On 2026-09-08 a disposable workspace was created on mac1 from the MacBook, driven through herdr's socket API, and removed. Sanitized facts:

- `pane.report_agent` from an external `source` on a plain shell pane is accepted. The reported state outranks herdr's own screen detection and survives output scrolling in that pane. `idle`, `working`, `blocked`, and `unknown` are the accepted states; `idle` shows as `done` until the operator looks at it. `pane.release_agent` removes the agent.
- `pane.report_metadata` carries a title, a display agent, state labels, and arbitrary string tokens. All of them reached the MacBook through the herdr-mirror plugin within about four seconds of each state change, and custom tokens are available to the operator's sidebar row layout by name. `pane.rename` labels do not propagate.
- The sidebar row text is the pane's terminal title. An OSC title written by the process that holds the pane's foreground reached the MacBook row through both the herdr 0.9 machine link and the mirror plugin. A title written by a shell command is reset by the prompt, so the process that follows the log must own the title.
- Herdr 0.9's machine link lists the remote workspace and the reported agent with its state under the machine's entry after a short delay, without any plugin.
- Closing the remote workspace removed the mirror copy on the MacBook within a second.
- `notification.show` on the worker returned `shown: true`; whether it reaches a MacBook client through the machine link is unconfirmed, which is why notifications are sent from the laptop.

These are the only herdr behaviours this design relies on, and every one of them is exercised by the automated tests through a scripted fake socket and by the live acceptance in section 13.2.

### 2.3 Relationship to the deferred interactive mode

The v2 design defers an interactive mode in which the agent's TUI runs inside a herdr pane and the operator types into it. This design is not that. The agent stays headless, stdin stays `prompt.md`, completion stays the exit code plus the structured result, and no message reaches a running agent. The reporter is the observability half of that deferred design, delivered separately because it changes nothing about supervision. Section 15 records what the interactive mode would still need.

## 3. Goals

- Show every active pool turn, on every opted-in worker, in the operator's herdr sidebar with the same lifecycle states herdr uses for local agents, and keep the finished state visible until the operator has seen it.
- Render the turn's log in the pane exactly as `worker task logs -f` renders it, so the pane is a readable transcript rather than raw JSON events.
- Notify the operator in the MacBook's herdr when a turn ends and when a task needs input.
- Make herdr availability a fact the operator can check before relying on it.
- Keep the side channel unable to change a turn's outcome, timing, or records, and unable to leak a prompt, a secret, or a worker path.
- Leave the worker's herdr untouched except for one workspace named `mac-worker` and the tabs inside it.

## 4. Non-goals

- Running the agent inside a herdr pane, attaching to it, or typing into it (section 15).
- Depending on herdr, the herdr-mirror plugin, or a particular sidebar layout. A worker without herdr behaves exactly as today.
- Sending notifications from the worker, or streaming pane content over anything but herdr's own machine link and plugins.
- Reading herdr state back into scheduling or the dashboard. That is a separate, smaller design once these facts exist.
- Any change to herdr's own agent integrations installed on the workers.

## 5. User experience

### 5.1 Configuration

```toml
version = 1

[notifications]
herdr = true            # default true; a no-op when no herdr socket is reachable

[[workers]]
name = "mini-1"
ssh = "mac1"
slots = 1
capabilities = ["darwin-arm64"]
herdr = true            # default false; report turns to this worker's herdr
```

`herdr` on a worker is the only switch for the reporter, and it is per worker because it creates visible tabs on that machine and needs a herdr server there. `[notifications] herdr` is on by default because its only effect is a popup in a herdr client the operator is already looking at, and it degrades to nothing when the socket is absent. Both defaults keep an existing `config.toml` valid without edits; the example configuration documents both keys.

The reporter always targets the worker's default herdr session, whose socket is `~/.config/herdr/herdr.sock` under the worker account's home, the same home the env profiles are read from. The notifier resolves the MacBook socket from `HERDR_SOCKET_PATH` when the submitting process runs inside herdr, and from the same default path otherwise.

### 5.2 What the operator sees

On the worker's herdr, and therefore on the MacBook through the machine link or the mirror plugin:

- one workspace labelled `mac-worker`, created on first use and never removed by mac-worker;
- inside it, one tab per running or finished turn, labelled `task <id12> · turn <n>`, with a single pane;
- the pane's terminal title, and so the sidebar row, reads `task <id12> · <task title>`;
- the agent icon and state follow the turn: `working`, then `blocked`, `done`, or `unknown`, with the outcome's summary or first question as herdr's state message;
- sidebar tokens `task`, `turn`, `mw_title`, `mw_agent`, and `mw_outcome` for operators who lay out their own rows;
- the pane shows the rendered event stream of the turn, then the outcome line, and stays until the next turn of the same task or `task close` replaces or removes it.

On the MacBook, when a turn ends, a herdr notification titled `task <id12>: <outcome>` with the task title and the summary or first question as its body, with the `done` sound for `done`, the `request` sound for `needs_input` and `blocked`, and no sound otherwise.

### 5.3 Doctor, setup, and workers

Each worker block in `worker doctor`, `worker workers`, and the `worker setup` report gains one line:

```text
  herdr: available (0.9.0)
  herdr: not installed
  herdr: installed (0.9.0), no socket
  herdr: installed (0.9.0), no response
  herdr: unknown                       # facts stale or never collected
```

When a worker has `herdr = true` and the fact is anything but `available`, `doctor` adds a warning and `setup` adds a setup warning:

```text
  warning [HERDR_UNAVAILABLE]: herdr = true but the worker's herdr socket is not reachable; turns run without the reporter
```

It is a warning, never a blocker: the pool is complete without herdr.

## 6. Architecture

### 6.1 The herdr socket client

A small module in the `worker` binary speaks herdr's socket protocol: one JSON object per line on a Unix stream socket, `{"id", "method", "params"}` out, `{"id", "result"}` or `{"id", "error": {"code", "message"}}` back. The client is synchronous, opens one connection per request, and uses the methods `ping`, `workspace.list`, `workspace.create`, `tab.list`, `tab.create`, `tab.close`, `pane.process_info`, `pane.send_input`, `pane.report_agent`, `pane.report_metadata`, `pane.release_agent`, and `notification.show`. Every call has a connect deadline of 500 ms and a response deadline of 3 s, and returns a typed error that says whether the socket was absent, refused, timed out, or answered with a herdr error. The socket descriptor is `O_CLOEXEC` and lives only for the call. Request ids are `mac-worker:<unix millis>:<counter>`, and `seq` values for reports are the current time in nanoseconds, the same scheme herdr's own hooks use. Herdr's schema (`herdr api schema --json`, protocol 22) is the reference for the parameter shapes, and a copy of the relevant subset is kept beside the tests.

### 6.2 The worker reporter

The reporter runs inside whichever helper process owns the turn event, always on the worker, always against the worker's own socket:

- **turn start** in the supervisor, immediately after the durable `Running` write and the guard release, so a slow herdr can never hold the job slot;
- **turn terminal** in the single turn terminal hook that every terminal path already funnels through: normal exit, timeout, host cancellation, prelaunch failure, ambiguous child, and lost reconciliation;
- **task close** in the worker's `task-close` handler and in retention close, after the workspace is removed.

It keeps no state of its own. The workspace is found by its `mac-worker` label, and a task's tabs by their `task <id12>` label prefix, so a crashed supervisor, a restarted herdr server, or a tab the operator closed leave nothing to reconcile: the next event re-discovers or re-creates what it needs. Concretely:

1. `ensure_workspace`: `workspace.list`; use the first workspace labelled `mac-worker`, else `workspace.create` with that label, `focus: false`, and the account home as `cwd`.
2. `sweep_task`: `tab.list` for that workspace; `tab.close` every tab whose label starts with `task <id12>`. This runs before a new turn's tab is created and at task close, and it is what removes a previous turn's tab.
3. `open_turn_tab`: `tab.create` with the label, `focus: false`, and the account home as `cwd`; the response names the root pane.
4. `arm_pane`: poll `pane.process_info` until the pane's foreground is exactly the login shell, for at most 5 s; then `pane.send_input` with the text `exec ~/.local/bin/worker host follow-turn <project_id> <worktree_id> <job_id>` and an Enter key. If the shell never settles the pane stays a shell and the states are still reported.
5. `report`: `pane.report_agent` with `source = "mac-worker"`, `agent` set to the herdr kind for the adapter (`codex`, `claude`, `cursor`, `opencode`), the mapped state, and the message; then `pane.report_metadata` with the title, `display_agent = "mac-worker"`, state labels, and the tokens from section 5.2.
6. `release`: `pane.release_agent` with the same source and agent, then `sweep_task`.

At turn start the sequence is 1, 2, 3, 4, 5 under a total budget of 10 s. At terminal it is 1, then find the turn's tab by its exact label, and if it is gone, 3 and 4 again so a finished state is never lost, then 5, under 5 s. At close it is 1, then 6 for the task's tab if present, under 5 s. `host gc --apply` runs `sweep_task` for every tab whose task directory no longer exists on the worker.

The reporter's inputs at turn start are the turn section's task id, turn number, agent kind, project id, and the lease's job and worktree ids, plus the task title read from the task's `meta.json`, which the worker persisted at `task-prepare` and which the terminal hook already reads today. Its inputs at terminal are the finished `TaskOutcome` and the redacted summary and questions that the publisher just wrote into the task status.

### 6.3 The follow-turn pane process

`worker host follow-turn <project_id> <worktree_id> <job_id>` is a hidden, read-only host command meant to be executed inside a herdr pane. It validates the three identifiers exactly as the other host commands do, opens the turn's job directory under the host state root, sets its own terminal title to `task <id12> · <task title>` with an OSC 0 sequence, and then follows `stdout.log` and `stderr.log` through the same offset-based reads that `log-chunk` serves, rendering the event stream with the renderer `worker task logs -f` uses today. When the turn's status becomes terminal it prints one outcome line, identical in content to what `task status` shows, and keeps running until it receives `SIGTERM` or `SIGHUP`, which herdr sends when the tab closes. It never writes to the job directory, never opens the prompt, and refuses to run when the directory is not a turn.

Its argv carries identifiers only, so the command echoed in the pane names no path, and the process that owns the title is the one herdr's title detection sees.

### 6.4 Laptop notifications

The local turn runner already converges every terminal path of a turn in one place: the return of its `execute` step, which covers the normal terminal, both publication failures, and the error path. The notifier runs there, after the durable task record is written and before the runner exits, with `[notifications] herdr` resolved from the same configuration the runner loaded. It sends one `notification.show` per terminal turn, with the fields of section 5.2, under a 2 s budget, and ignores every failure. Inline runners (`submit --wait`, `say --wait`) notify exactly like detached ones, so the behaviour does not depend on how a turn was started.

### 6.5 Facts

`refresh-facts` gains a `herdr` fact beside the agent facts, collected under the same fifteen-minute TTL: whether a `herdr` executable exists on the controlled host paths and its `--version` output, whether the default socket exists, and whether a `ping` request is answered. The probe returns it with the cached agent facts, so `doctor`, `setup`, `workers`, and the dashboard source all see it without a new round trip, and stale facts render as `unknown` exactly like stale agent facts do. The herdr fact is not a capability: it never enters scheduling.

## 7. State mapping

| mac-worker event | herdr state | herdr message | `mw_outcome` |
|---|---|---|---|
| turn `Running` | `working` | task title | `running` |
| outcome `done` | `idle` (shown as done) | summary | `done` |
| outcome `needs_input` | `blocked` | first question text | `needs_input` |
| outcome `blocked` | `blocked` | summary, else first question | `blocked` |
| outcome `unknown` | `unknown` | `agent reported no structured result` | `unknown` |
| outcome `failed` | `unknown` | redacted failure reason | `failed` |
| outcome `cancelled` | `unknown` | `cancelled` | `cancelled` |
| outcome `timed_out` | `unknown` | `timed out` | `timed_out` |
| outcome `lost` | `unknown` | `lost` | `lost` |
| `task close`, retention close, `gc` | released, tab closed | | |

State labels sent with every report: `working = "turn <n>"`, `blocked = "needs input"`, `idle = "done"`, `unknown = "<outcome kind>"`. Messages are the already redacted values from the task status, cut to 1 KiB. Herdr's `unknown` is the honest state for a turn that ended without a usable result: the row stays visible with the reason instead of vanishing.

## 8. Lifecycle and hook points

```text
supervisor: ... Running written, guard dropped
            reporter.start(section, lease, title)          <= 10 s, best effort
            wait_for_child
            terminal classified, publisher runs
            TurnTerminalHook: finish_turn writes status
            reporter.terminal(section, outcome, status)    <= 5 s, best effort
            lease released

host task-cancel / reconcile: same TurnTerminalHook, same reporter.terminal
host task-close, retention close: workspace removed
            reporter.close(task)                           <= 5 s, best effort
host gc --apply: reporter.sweep_orphans()                  <= 5 s, best effort

laptop runner: execute returns terminal or failure
            record written, queue row removed
            notifier.turn_finished(record, outcome)        <= 2 s, best effort
```

A follow-up turn started by `say` is a new turn on the same worker: its start sweeps the previous turn's tab and opens a new one, so the sidebar shows exactly one row per task.

## 9. Failure model and budgets

The reporter and the notifier share one rule: every failure is logged once and swallowed. Nothing they do can return an error into a turn, a close, a gc, or a runner.

- Socket absent or refused: the reporter is disabled for the rest of that event, one line `herdr reporter: unavailable (<kind>)` goes to the turn's `supervisor.log`, and the turn status records `herdr: unavailable`.
- A herdr error on a later call (pane gone, server restarted): the terminal path re-creates the tab and reports again; the start path continues without the pane.
- Deadlines: 500 ms connect, 3 s per response, 10 s total at start, 5 s at terminal, 5 s at close and gc, 2 s for a notification. Budgets are enforced by the reporter, not by herdr.
- The supervisor guard is never held across a reporter call, and reporter calls run in the supervisor's thread only where the supervisor already performs bounded synchronous work: after the Running handoff and inside the terminal hook, where `git` already runs under a deadline.
- The follow-turn process is a child of the herdr pane's shell, never of the agent's process group, never of the supervisor, and holds no descriptor of either. Killing, timing out, or cancelling the agent's exact process group therefore never touches it, and it cannot keep the group from being proven absent.
- The turn directory layout stays closed: the reporter creates no file there, and the follow-turn process only reads.

## 10. Trust, privacy, and what crosses the socket

The worker's herdr socket belongs to the worker account, and the helper already runs as that account with full access; connecting to the socket grants nothing new. What the reporter sends is the closed set: task id, turn number, agent kind, project id, worktree id, job id, the task title, the outcome kind, the redacted summary or first question, the fixed labels, and the follow-turn argv. It never sends the prompt, the composed prompt, any environment value, any profile value, the model, the base object id, or any filesystem path: `cwd` for the workspace and the tab is the account home, the follow-turn argv carries identifiers, and messages come from the redaction boundary that already strips paths and tokens before they reach the laptop. The notifier sends the task id, the outcome kind, the title, and the redacted summary or question from the local record, and nothing else.

What the operator then sees in the pane is the same rendered event stream `worker task logs -f` shows on the laptop, which is application output and may contain whatever the agent printed, exactly as the v2 design already states for logs. Sidebar rows on the MacBook come from the operator's own herdr client and plugins; mac-worker makes no claim about what those render.

## 11. Protocol and record changes

- `TurnSection` gains `herdr_reporter: bool`, serialized only when true, beside `git_identity` and `origin_url`, which are the existing per-worker values the laptop injects into a turn. It is outside the turn material digest, like those two.
- The turn summary in the task status gains an optional `herdr` object: `{ "state": "attached" | "unavailable" | "disabled", "pane_id": "<id>" }`, written by the reporter at start and updated at terminal, so `task status --json` and the dashboard can say whether a turn is visible in herdr. `pane_id` is herdr's opaque pane id and carries no path.
- Agent facts gain `herdr: { "state": "available" | "not_installed" | "no_socket" | "no_response", "version": "<string>" | null }`.
- `Config` gains an optional `[notifications]` table with `herdr: bool` (default true); `WorkerEntry` gains `herdr: bool` (default false). Both structs keep `deny_unknown_fields`.
- One new hidden host command, `follow-turn`.
- `PROTOCOL_VERSION` moves from 5 to 6, because the turn request, the task status, and the probe response change shape and every request struct rejects unknown fields. `SUPERVISION_VERSION` stays at 3: the agent launch contract is unchanged. Every worker needs `worker setup` rerun, as after every protocol change.

## 12. Error model additions

- `HERDR_UNAVAILABLE`: a `doctor` warning and a `setup` warning, never a blocker, for a worker with `herdr = true` whose fact is not `available`.
- Reporter diagnostics use the kinds `absent`, `refused`, `timeout`, and `herdr:<code>` in `supervisor.log` only; they have no public error code because they never surface as a command result.
- `follow-turn` exits `64` for malformed identifiers and `70` when the directory is not a turn; no other host command semantics change.

## 13. Verification strategy

### 13.1 Automated tests

All tests run against a scripted fake herdr server: a Unix listener in a temporary home at the default socket path, answering from a queue of canned responses and recording every request line.

- Client: framing, id echo, error bodies, absent socket classified within the connect deadline, a silent server classified within the response deadline, the descriptor closed after each call and `O_CLOEXEC` set while open.
- Reporter: workspace discovered rather than duplicated; sweep closes exactly the tabs with the task's label prefix and nothing else; tab label and pane armed with the exact `send_input` text and no path in it; every row of section 7 produces the expected `report_agent` and `report_metadata` params; the terminal path re-creates a missing tab; close releases and sweeps; gc sweeps only orphans; every budget is honoured against a stalling server; every failure leaves the outcome, exit code, status bytes other than the `herdr` object, and lease exactly as without the reporter.
- Supervisor and terminal hook: a turn with `herdr_reporter = true` and no socket records `herdr: unavailable`, logs one line, and matches the reference turn byte for byte otherwise; host cancel and lost reconciliation paths invoke the terminal report; the agent's process group membership and the pump's stdout accounting are unchanged.
- follow-turn: renders a recorded Codex, Cursor, and OpenCode stream identically to `task logs`; prints the outcome line at terminal; survives the log cap and tail; exits on `SIGTERM`; refuses a batch job directory and malformed ids; opens nothing for writing.
- Notifier: one `notification.show` per terminal turn from inline and detached runners with the expected title, body, and sound for every outcome; `[notifications] herdr = false` sends none; an absent socket costs less than the budget and sends none; `HERDR_SOCKET_PATH` wins over the default path.
- Config: both new keys parse, both defaults hold, the example configuration parses, unknown keys are still rejected.
- Facts, doctor, setup, workers: each `herdr` fact state renders its line in text and JSON; `HERDR_UNAVAILABLE` appears only for `herdr = true` workers that are not `available`; `setup` carries it as a setup warning and still reports `installed`.
- Privacy: over every recorded request from every test above, no prompt bytes, no environment or profile value, no `/Users/`, `/home/`, or `~` path, and no model string appear; the only paths in any payload are none.

### 13.2 Live acceptance

On mac1 with `herdr = true`, from the MacBook, sanitized records only:

1. Submit a Codex task. Within ten seconds the worker's herdr has a `mac-worker` workspace and a `task <id12> · turn 1` tab whose pane renders the event stream and whose title is the task title; the row appears on the MacBook under the machine and, if the mirror plugin is running, as a mirror workspace, in state `working`.
2. The task ends `done`: the row turns `done` with the summary as its message, the pane shows the outcome line, and the MacBook shows a `done` notification with the task title.
3. A task that ends `needs_input`: the row turns `blocked` with the question as its message and the MacBook plays the `request` sound. `say` starts turn 2: the turn 1 tab is gone and a `turn 2` tab is `working`.
4. `task close` removes the tab and the agent; the mirror copy, if any, disappears.
5. Kill the supervisor mid-turn, then run any command that reconciles: the row reaches `unknown` with message `lost`; the next turn of that task sweeps its tab.
6. Stop herdr on the worker and submit a task: the turn completes identically, `task status --json` shows `herdr: unavailable`, `doctor` warns `HERDR_UNAVAILABLE`, and the record contains no new paths or values.
7. `worker setup mini-1` prints `herdr: available (0.9.0)`; a worker with `herdr = true` and herdr stopped prints the setup warning and still reports `installed`.
8. Retained task status, queue rows, JSON output, and the fake-server recordings from the automated run contain no planted secret and no complete local path.

## 14. Delivery and boundary

One implementation plan, `docs/superpowers/plans/2026-09-08-herdr-reporter.md`, in six tasks: the socket client with its fake server, the configuration and wire changes with the protocol bump, the worker reporter with `follow-turn` and the hooks, the laptop notifier, the facts with their `doctor`, `setup`, and `workers` rendering, and documentation with the live acceptance record. The reporter is complete when section 13.2 passes on mac1 and the full gate (`cargo fmt --check`, `cargo test --locked --all-targets`, `cargo clippy --all-targets -- -D warnings`) is green.

## 15. Deferred

- **Interactive mode.** Still deferred and still needing its own review. With this design in place it would reuse the workspace, the tab, and the client, and replace `follow-turn` with `agent.start` of the agent's TUI plus `agent.prompt --wait` and `agent.wait --until blocked`. What it changes is the completion signal, result extraction, and the no-injection rule, none of which this design touches.
- **Herdr facts in scheduling and the dashboard.** Counting a worker's interactive agents as load, and a herdr chip on the dashboard worker card. The fact exists after this design; the policy does not.
- **Worker-originated notifications** through the machine link, once their propagation is confirmed.
- **A pane per turn kept side by side** instead of tab replacement, if reading a previous turn in herdr proves more useful than `task logs`.

## 16. Alternatives considered

### Rely on the herdr 0.9 machine link alone

It shows workspaces and agents, but only agents herdr can detect, and a headless `codex exec` is not one. Something on the worker has to report the state; that is the reporter.

### Run the turn inside a herdr pane and let herdr's integrations report

That is the deferred interactive mode. It trades the exit code for herdr's heuristic states, gives the pane's shell a chance to interfere with stdin and the process group, and makes herdr a dependency of every turn. The reporter keeps the headless turn and adds a read-only pane beside it.

### Tail the raw log in the pane instead of a helper command

`tail -F stdout.log` shows JSON events, echoes a worker path in the pane, and lets the shell prompt reset the title. A hidden read-only host command renders the stream the way `task logs` does, carries identifiers only, and owns the title.

### Keep reporter state on the worker

A `herdr.json` beside the task metadata would need crash recovery, layout validation, and gc rules. Herdr already holds the state: labels are discoverable, and re-creating a tab is cheaper than reconciling a record.

### Make the herdr fact a capability

Capabilities drive scheduling. Herdr availability must never keep a task off a worker.

### Default the reporter on

It creates visible tabs and depends on a server on the worker; that is an operator decision per machine, so it is opt-in. Notifications default on because their absence costs nothing.

## 17. Consequences

The worker helper gains its first outbound local client, bounded and best-effort, and one hidden read-only command meant to run in a terminal it does not own. The wire grows by one boolean, one optional object, and one fact, which costs a protocol bump and a `worker setup` round. In exchange the operator sees the whole pool in the one place they already watch their agents, hears about finished turns without polling, and learns from `doctor` whether a worker can show them anything. Everything that made the pool boring stays: SSH with fixed argv, no listeners, exact process groups, closed directory layouts, redaction before anything leaves a turn, and a helper that never decides anything an operator did not configure.

## 18. Review record

Revision 1 is written from the live spike of 2026-09-08 on mac1 (section 2.2) and from a reading of the supervisor, terminal hook, task store, turn runner, doctor, installer, facts, and configuration code as of `c8dcbf1`. The four open points were decided with the operator on 2026-09-08 before implementation started:

- `herdr_reporter` lives in `TurnSection`: per turn and per worker, beside the other per-worker values the laptop injects, outside the digest. Close and gc discover tabs by label and need no flag.
- Failed, cancelled, timed-out, and lost turns report `unknown` with the reason as the message. The row stays visible until the next turn or `close`; a vanished row would hide exactly the turns the operator most needs to see.
- `follow-turn` renders both `stdout.log` and `stderr.log`. The first real pool run showed a failed turn whose only explanation was in stderr, and the parallel log-diagnostics work (`f29cf81`) made the same choice for `task logs`.
- The notifier fires for task turns only. `worker run` batch jobs keep their v1 behaviour.

Sequencing note: the pool reliability work of 2026-09-08 changes how a turn records its project context and lands ahead of this design; the `TurnSection` field and the protocol bump in Task 2 of the plan are rebased onto it, so the workers see one `worker setup` round, not two.
