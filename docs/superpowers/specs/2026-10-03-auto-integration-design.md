# Automatic integration into the base branch

Date: 2026-10-03. Status: proposed for orchestrator review; no implementation or live acceptance performed.

Code baseline: `67183a0b9f5926dc806c0ed96976e8ad39437ef0`. Every current-behaviour anchor below refers to that commit. Names, limits, schemas and commands described as proposed are requirements for the next wave.

Plan: [2026-10-03-auto-integration.md](../plans/2026-10-03-auto-integration.md).

The owner wants completed pool work integrated directly into the project's configured branch on origin, normally `main`, without an accept step. The same task session repairs conflicts. Human attention is reserved for exhausted recovery. Integration is opt-in; an unconfigured task retains its current behaviour. Kanban UI, deployment, host-run project checks and other remotes are outside this wave.

## Decision 1 — extend existing task machinery

**Existing mechanisms:** host publication and result refs (`src/turn.rs:1360`); exact-OID origin delivery with worker credentials (`src/git_transport.rs:198`); durable delivery intents (`src/outbox.rs:264`); replayable prepared follow-ups (`src/prepared_followup.rs:1`); bounded leader recovery (`src/task_client.rs:2976`); task projections (`src/task_view.rs:103`); controller events and addressed facts (`src/controller/events/contracts.rs:967`). Reuse these boundaries.

**Integration not found — checked:** the publisher, result finalizer, task close path, origin outbox and project settings (`src/turn.rs:1288`, `src/turn_runner.rs:1632`, `src/task_client.rs:2719`, `src/outbox.rs:792`, `src/project_config.rs:40`). They publish, import or close; none constructs a target merge. Dashboard accept calls close (`src/dashboard/task.rs:460`). The documented non-goal is explicit (`docs/superpowers/plans/2026-09-10-flow.md:8`). This repository is mac-worker, so the Skillaz knowledge-base lookup does not apply.

| Current boundary | Evidence at the baseline | New hook |
| --- | --- | --- |
| Dirty task workspace is committed by the host | `src/turn.rs:1373` | Preserve ordinary turns; auxiliary integration turns use Decision 7 |
| Task branch enters the worker mirror | `src/turn.rs:1384` | Preserve `task/<id>` publication |
| Optional origin task-ref delivery is committed to the outbox | `src/turn.rs:1288` | Keep separate from target integration |
| Checks are redacted, stored agent claims | `src/turn.rs:1326`, `src/agent/mod.rs:496` | Apply the blocking rule in Decision 8 |
| Host immediately closes a Done task | `src/turn.rs:1020` | Freeze effective host close policy to Never for configured tasks |
| Queue-owner runner imports result and records fetched head | `src/turn_runner.rs:1645`, `src/turn_runner.rs:1696` | Stage integration only after successful import |
| Runner retires its row, continues and advances DAGs | `src/turn_runner.rs:1716`, `src/turn_runner.rs:2481` | Wake integration after continuation decision and retirement |
| Automatic continuation is a durable intent | `src/task_client.rs:142`, `src/task_client.rs:3772` | Never integrate while one exists |
| DAG parent gate requires Closed + Done | `src/dag.rs:440`, `src/task_client.rs:5286` | Add a configured-integration gate in Decision 10 |
| Leader calls selected recovery | `src/lib.rs:1475`, `src/task_client.rs:2979` | Also select pending integration sidecars |
| Follow-up resumes the bound worker session | `src/turn_runner.rs:1283` | Use the same path for resolve/verify |
| Open tasks remain in the active index | `src/client_state/active_tasks.rs:27` | Pending integration remains discoverable |
| Retention can close an idle Open task | `src/gc.rs:570`, `src/gc.rs:1033` | Pending/blocked integration protects its workspace |

**Rejected:** a new task scheduler would duplicate durable admission. Reusing the task-ref outbox as the integrator would omit workspace repair, final-turn eligibility and queue-owner fences. Laptop merging would contradict worker placement and stop when the laptop disconnects in controller mode.

## Decision 2 — explicit, frozen project policy

Proposed `.worker.toml`:

```toml
[task]
integrate = "main"
verify_merge = "never" # or "moved-target"
```

Absent `integrate` means disabled. `integrate = false` explicitly disables inherited policy. A branch string enables it. There is no discovery of origin HEAD and no implicit `main`: the documented recommended configuration names `main` explicitly. Validate a short branch through `BranchName`, reject full refs, invalid names and missing project origin. Freeze normalized origin, branch, verify policy, requested close policy and base provenance before any submission effect. Never re-read agent-modified `.worker.toml` during integration.

Submit adds mutually exclusive `--integrate <branch>` / `--no-integrate` and `--verify-merge never|moved-target`. A verify override without an effective integration target is a configuration error. Batch accepts the same `integrate` string-or-false and `verify_merge` at top level, in `[defaults]`, and per task. Existing flat-versus-table exclusion remains (`src/task_client.rs:894`). Target precedence is task/submit override, batch default, project setting, disabled. Verify precedence is the same, with `never` last. A task-level `integrate = false` wins over every default. `batch --preview` prints each effective target and verify policy.

No laptop-wide enabling default in v1. Add `[task] integrate = "main"` to each intended project to meet the owner's preference. A laptop default would silently affect unfamiliar projects and would differ from controller replay unless frozen anyway. A future explicit project allowlist could be added separately.

For a configured task, freeze `ClosePolicy::Never` in the existing TaskMeta/FrozenSubmitBody/DagFrozenSpec and retain the user's requested Done/Never in the integration policy sidecar. This is the smallest close change: the current publisher otherwise removes the workspace before owner-side integration (`src/turn.rs:1022`, `src/task_store.rs:1373`). An older host also understands Never and cannot silently delete this workspace. After integration succeeds the queue owner explicitly closes only if the requested policy was Done. Public projections show the requested policy; an old client can show the conservative effective Never value.

Integration and `publish = ["fetch", "push"]` are independent. Integration also works with fetch-only publication. Add `origin:<host>` and `feature:task.integration` to scheduling requirements even for fetch-only/local-source tasks; feature requirements already have precedent (`src/task_client.rs:6683`). Reject `publish_branch == integrate` on the same origin with `INTEGRATION_PUBLISH_TARGET_COLLISION`: otherwise ordinary outbox delivery could push the task head directly to the target before the merge (`src/turn.rs:1301`). Do not change other task-ref deliveries or wait for their outbox receipt.

**Rejected:** global enablement, guessing a default branch, re-reading worker configuration, or treating task-ref push success as integration. Each loses either explicit authorization, replay identity or the merge requirement.

## Decision 3 — final-turn eligibility and both execution modes

Support controller and direct mode in v1. Both already use the shared runner for prepare/base transport and resume (`src/turn_runner.rs:1358`, `src/turn_runner.rs:1283`). Direct mode needs an integration child and filesystem fences, not a second algorithm.

For configured tasks the proposed sequence is:

1. Host commits ordinary turn changes, publishes the task ref and persists any task-ref outbox intent.
2. Queue-owner runner drains the terminal result, imports its exact head and persists current-turn import proof.
3. Under the completed-turn fence, settle the automatic-continuation decision. If staging or import fails, do not integrate. Retire the ordinary runner/queue row and release its execution slot.
4. If an automatic turn is pending, execute it first. Otherwise an Open, uncancelled Done task with no runner, close intent, submission recovery, pending turn or continuation becomes eligible.
5. Persist the integration intent and wake an integration child. Auxiliary turn completion returns to this intent; it never creates an ordinary integration cycle or auto-continues NeedsInput.
6. Import the successful integration receipt/result ref, settle requested auto-close, then advance eligible DAG edges. DAG scans may run earlier, but their configured gates stay shut.

Current ordering is different: the host closes before import, and `persist_host_auto_close` merely observes that close (`src/turn_runner.rs:1702`, `src/turn_runner.rs:1966`). Decision 2 deliberately changes only configured tasks. An ordinary finalizer recovery path must stage the same intent; a happy-path-only hook is insufficient (`src/turn_runner.rs:2357`). Reconciliation must not interpret an auxiliary Done turn as a new task result to integrate.

| Process | Controller mode | Direct mode |
| --- | --- | --- |
| Freeze policy and source | Laptop; durable controller envelope/stream | Laptop; local submission sidecar |
| Drive ordinary turns and import | Detached controller runner child | Detached laptop runner child |
| Discover/recover integration | Controller leader's bounded tick | Terminal runner wake; laptop `wait`/`reconcile` on recovery |
| Perform one integration drive | Detached controller integration child | Detached laptop integration child |
| Run Git and hold workspace state | Host helper on the task's worker, invoked over existing SSH transport | Same host helper |
| Launch resolve/verify | Queue-owner child admits a prepared pinned follow-up | Same local TaskClient path |
| Manual re-drive | Durable controller mutation records intent; leader/child executes | Local durable intent; child executes |

The controller RPC handler freezes and acknowledges mutations; it does not run a merge or wait for an agent. The leader launches bounded work and remains responsive. In direct mode an already spawned child can finish without an interactive CLI, but a dead child is recovered only when the laptop runs a recovery-driving command. A powered-off direct-mode laptop provides no unattended recovery. This is a visibility/documentation limit, not a silent unsupported mode.

**Rejected:** integration on every terminal turn, inside the supervisor's occupied slot, before current-turn import, or in a synchronous controller RPC. These allow unfinished work, waste scarce slots or stall the controller.

## Decision 4 — base provenance is a hard gate

`--wip` plus integration always fails early with `INTEGRATION_WIP_BASE`, including a snapshot whose tree happens to match a committed tree. The frozen policy carries `base_kind = committed|from-task`; there is no WIP-to-committed reinterpretation. A source-session import with uncommitted work therefore remains non-integratable until submitted from a committed checkout (Decision 14).

At submission, when the laptop can fetch/identify the target and prove that the resolved committed base is not its ancestor, refuse with `INTEGRATION_BASE_NOT_ON_TARGET`. Fetch the named target to an isolated transfer ref and use an ancestry test. The existing origin preflight only tests advertised OID equality and is insufficient for this rule (`src/git_transport.rs:138`). If network access prevents proof, record preflight as unknown; do not equate a stale local tracking ref with authoritative origin. Admission still requires the origin capability. A missing branch positively observed at submission is `INTEGRATION_TARGET_MISSING`.

The worker repeats the gate after fetching origin at every candidate build and before a push. The cycle base is the base of the ordinary source turn, including a prior integrated head for subsequent `say`, not invariably the task's original metadata base. A `from:<id>` child freezes the dependency receipt's imported result as its base. Require `is_ancestor(cycle_base, fetched_target)`, with complete objects; no shallow-history guess and no fetching a private base into origin. Validate the policy origin against the frozen project identity; project IDs derive from normalized origin when present (`src/project.rs:67`, `src/project.rs:264`).

D9 concerns unpublished **input ancestry**; newly produced task commits are the output being integrated. It does not forbid the new merge/task commits themselves. An arbitrary local-only ancestor supplied as the task base remains forbidden even when Git could merge it.

**Rejected:** checking that the base exists somewhere on origin, automatically publishing the base, or trusting laptop proof forever. None proves reachability from the configured target at integration time.

## Decision 5 — one explicit two-parent merge and a guarded non-forcing push

Let `T` be the freshly fetched target, `H` the frozen ordinary task result and `B` the cycle base. First test whether `H` is an ancestor of `T`, after the base gate. If so, return `integrated` with disposition `already_integrated`, observed target `T`, and nullable `merge_oid`; a manually squashed/cherry-picked equivalent tree does not satisfy this ancestry check.

Otherwise compute a merge tree, then build `M` with `commit-tree <tree> -p T -p H`. Always supply both parents, even when `T` is an ancestor of `H`. Verify exactly two distinct parents in that order and a tree matching the accepted candidate. Pin `M` before any external effect. Do not push a symbolic task branch; the sole refspec is `<M>:refs/heads/<configured-branch>`.

Use the existing hermetic Git/worker credential construction (`src/git_transport.rs:206`, `src/git_transport.rs:237`, `src/git_transport.rs:897`). The existing sender uses `--no-verify` and exposes no expected-target argument (`src/git_transport.rs:223`). Add a separate `push_integration` path with a **worker-owned pre-push guard** in a private host hooks directory. It receives the remote advertisement and accepts only one update with local OID `M`, exact target ref, and advertised old OID `T`. All expected values come from the durable candidate, not shell interpolation. Guard mismatch returns a fixed target-moved marker. Do not pass `--no-verify` on this path. This guard is trusted worker code; it never loads repository hooks. Preserve credential helpers exactly as today.

This closes a subtle race: a moved target could be an ancestor of `H`, allowing an ordinary fast-forward push whose merge first parent was an older `T`. A pre-push comparison rejects that advertisement. Movement after advertisement is rejected by origin's expected-old-ref update. T2 must prove both schedules with a bare-origin fixture before the feature is advertised. No `--force`, `--force-with-lease`, `+` refspec or target deletion is allowed. The integration receiver sends only this one branch update; fetching and pinning private refs are local operations.

On target movement discard the candidate for pushing, fetch again, repeat the base/ancestry gates and merge anew. Accept at most **three candidate attempts per cycle**, counted durably before each merge preparation. The third movement ends in `INTEGRATION_TARGET_MOVED_EXHAUSTED`. Unknown push outcome always fetches first; if `M` or `H` is reachable from the target, settle success without another merge. A confirmed policy/auth failure does not masquerade as target movement.

**Rejected:** rebase/squash, fast-forward target updates without a merge, retrying a mutable ref, force-with-lease, and a bare unguarded `push_origin` call. Squash breaks dependency commit identity; the other options violate parent history, replay identity or target discipline.

## Decision 6 — host operations execute only controlled Git

Proposed hidden commands are `worker host task-integration` and `worker host task-integration-turn`. The former accepts typed `arm`, `step`, `read` and `revoke` actions; `step` advances one named durable phase. The latter validates an auxiliary-turn binding and delegates to normal admission/supervision. Both are gated by host `task.integration`, carry protocol 7 and accept only IDs/frozen policy, never an arbitrary command, repository path or refspec. Add `HostOperation` entries and RemoteJobClient adapters; existing host prepare uses this transport (`src/transfer.rs:584`).

`arm` binds the policy to the existing prepared task before its first agent launch, and requires effective close policy Never. The queue owner's submission policy is durable before the task row/queue effect, so a crash cannot create an unconfigured Done task. A failed arm must not launch the first turn. Ordinary `task_prepare` and session import still finish in their existing order first (`src/task_store.rs:805`).

`step` fetches only the configured target into a private ref, checks ancestry, computes/materializes a merge, records resolution/check evidence, builds/pins the commit, pushes it or repairs a receipt. Git steps hold a host task integration fence, but no execution slot. Refuse workspace mutation while any task lease or ordinary turn is active. Resolve/verify uses a normal worker reservation/lease and releases it at normal supervisor retirement. The host never pumps integration independently after losing its queue-owner drive.

Use a separate host-owned scratch repository with minimal validated config and object access to the worker mirror. Host Git processes use its explicit git-dir/index/work-tree, never the workspace's `.git/config`. Disable filesystem monitors, hooks other than the fixed push guard, signing, submodule recursion and externally configured merge/filter/diff programs. Do not invoke setup recipes, check commands, package managers or arbitrary repository scripts. Build trees with raw file/blob/index plumbing (`hash-object --no-filters`, `update-index`, `write-tree`), preserving symlinks and executable bits. Materialize conflict index stages and worktree blobs without invoking project filters. Prepare/restore the actual task repository metadata through rooted, identity-checked operations. The workspace path stays the same for the session. This isolation is needed even though existing Git construction disables global/system config: it still selects a repository with `-C` (`src/git_transport.rs:902`, `src/turn.rs:1430`).

Local Git steps have 60 s deadlines; fetch/push have 60 s deadlines; one host step/RPC transport is capped at 90 s. Credential-helper lookup retains the existing 5 s cap (`src/git_transport.rs:51`). Output is capped at 64 KiB per stream; conflict metadata has its own bounds (Decision 7). These are new integration-only limits; the existing generic Git deadline is 15 minutes (`src/git_transport.rs:31`). Auxiliary admission waits at most 10 minutes, with injected time and durable deadlines, then blocks. An auxiliary turn uses `min(task turn timeout, 10 minutes)` and inherits model token/agent-internal turn/budget limits, environment and permissions.

Use interruptible process execution. Logs contain task/turn/intent IDs, phase, attempt and stable code, plus bounded redacted text. They contain no raw Git stderr, credential-helper output or workspace contents. A lost connection records an uncertain phase; it never authorizes a second push before reading host state and origin. Locks protecting task/queue records are dropped before SSH, Git, launching agents or waiting. The host task integration fence may serialize a bounded push with revoke; unrelated tasks remain responsive.

**Rejected:** `git merge` in a potentially agent-configured repository, ordinary `git add` with filters, executing `.worker.toml` checks outside the agent, moving the session workspace, or holding a slot for Git-only steps.

## Decision 7 — resolve/verify are prepared follow-ups with a publication hook

Persist `PreparedIntegrationTurn { integration_id, epoch, attempt, purpose, followup: PreparedFollowup, workspace_binding, approved_turn_limits }` in a separate sidecar before prompt write, task CAS, queue admission or launch. `purpose` is `resolve` or `verify`. Do not add purpose to strict existing turn/follow-up DTOs. Derive the auxiliary TurnId from a domain-separated UUIDv8 hash of integration ID, epoch, candidate attempt, purpose and ordinal; persist the timestamp and full prepared binding. Existing prepared follow-up identity and binding provide the model (`src/prepared_followup.rs:185`, `src/prepared_followup.rs:242`). Replay must find the same turn, never allocate another.

Pin the same worker and resume its bound session, same agent/model/profile/permissions. Do not recapture an imported session or rerun first-turn prepare. Auxiliary turns appear in existing task turn history and consume **the ordinary `max_followups` allowance**; the integration caps are additional bounds. Existing prepare already counts all follow-ups (`src/prepared_followup.rs:80`). If no allowance remains, block with `INTEGRATION_FOLLOWUP_LIMIT` before admission. Automatic ordinary continuations retain their present budget semantics. Within a cycle allow at most **two resolve turns and three verify turns** (one verify per accepted candidate at most). Retry of the same auxiliary ID does not spend a second turn. Manual re-drive resets cycle caps, never the task's already spent follow-up allowance.

For conflict preparation the host durably saves the clean result head `H`, original task ref/index binding and candidate identifiers; it materializes the merged files and stages 1/2/3 in the existing workspace, sets HEAD to `T` and MERGE_HEAD to `H`. The task's public result head remains `H` until success. The integration-specific host launch validates this planned HEAD/index instead of the ordinary base expectation. Ordinary launches cannot use this exception. No agent commits or stages `.git`; the host does both.

The existing resume path requires HEAD to equal the public result base and a clean workspace (`src/task_store.rs:1619`, `src/task_store.rs:1637`), and JobService requires exact task turn limits (`src/job_service.rs:566`). Add `TaskStore::prepare_integration_resume` and a gated `JobService::submit_integration_turn` adapter. They require the durable purpose/candidate binding, normal task lease, next turn number and same agent/session/model/profile/permissions. They permit only the planned T/index state and the approved reduced timeout; all other turn limits stay equal to TaskMeta. The ordinary `prepare_resume`/`submit_turn` validators remain strict. Persist the purpose before accepted-job execution so supervisor publication and repair can find it without extending ExecutionPayload/TurnSection wire schemas.

**Required publication hook:** on these marked turns, parse/redact/persist the structured result and finish the normal lease, but skip `publish_workspace`, ordinary task-ref outbox creation and host auto-close. Otherwise the current publisher would execute `git commit` before integration accepts the resolution (`src/turn.rs:1242`, `src/turn.rs:1375`). Keep the auxiliary result/checks in TaskStatus and the candidate receipt, then retire the queue row. The queue-owner finalizer uses the purpose sidecar to bypass ordinary result-head import/automatic continuation for that auxiliary turn and wakes the same integration intent. No unmarked turn gets this exception.

After a successful resolve, the host validates that HEAD/MERGE_HEAD match `T/H`, re-reads the working files safely, stages raw resolution blobs/deletions, rejects remaining index stages 1/2/3, and scans changed tracked text for residual conventional or diff3 conflict markers. Check marker-shaped lines in every conflicted path and newly changed text; unchanged literal markers in unrelated files do not cause false failures. Marker removal and no unmerged entries are host Git/file checks, not evidence that project checks passed. Tree changes outside conflicts are recorded and included; they must be checked by the agent. Invalid output, NeedsInput, Blocked, timeout or a failed/error check stops automatic recovery; only a Done resolution with required check evidence proceeds.

For a clean moved-target verify, materialize the candidate tree with HEAD `T`, MERGE_HEAD `H` and a resolved index. Verify is read-only: any tracked/untracked change beyond the baseline workspace inventory blocks with `INTEGRATION_VERIFY_CHANGED_TREE`. The host does not silently include a verifier's edits. A normal user `say` can produce a new source result later.

Proposed exact resolve prompt body (fixed framing is composed with the existing adapter's structured-result instruction):

```text
Resolve this task's integration into the configured origin branch.
Task: {task_uuid}; source turn: {source_turn_uuid}; attempt: {attempt}/3.
The host prepared a merge in this workspace. HEAD is the target; MERGE_HEAD is this task's result.
Conflicted repository-relative paths (JSON strings, redacted):
{one_json_string_per_path}
Inspect Git's unmerged-path list in this workspace if a displayed path was redacted.
Resolve every unmerged path, including deletions. Remove conflict markers.
Do not commit, stage Git metadata, switch branches, rebase, squash or push. The host will stage and commit.
Run this project's applicable checks inside this agent turn. Report each command and its result in checks.
Return status done only when resolution and checks succeed. Report failed/error checks truthfully.
If checks cannot run, return blocked and explain the missing requirement. The host will not run them.
```

Proposed exact verify prompt body:

```text
Verify this task's clean merge onto a moved origin target.
Task: {task_uuid}; source turn: {source_turn_uuid}; attempt: {attempt}/3.
The host materialized the candidate merge in this workspace.
Do not change files, commit, stage Git metadata, switch branches, rebase, squash or push.
Run this project's applicable checks inside this agent turn and report commands/results in checks.
Return status done only when checks succeed. If a check fails or cannot run, return blocked.
The host will not execute project checks or treat your report as independent verification.
```

Prompts are bounded to 16 KiB UTF-8 after redaction; list at most 256 paths, each at most 1,024 bytes, with the whole list also fitting that prompt. Oversized/invalid path metadata blocks with `INTEGRATION_CONFLICT_LIST_TOO_LARGE`; never truncate away a conflict and proceed. Decode Git's NUL-delimited paths without line splitting, validate repository-relative paths, JSON-escape them, apply the host publication redaction boundary, and append fixed framing separately so newlines remain real. Names containing secrets may become placeholders; the agent can read the full index inside its sandbox. No remote URL or absolute path appears in the prompt. If T changes, archive the previous candidate's resolution evidence, restore source H and prepare the new candidate; the next prompt states that the previous candidate was superseded. Never claim its candidate-only edits or checks survived into a different tree.

**Rejected:** a fresh agent/session, unlimited resolver retries, a separate free follow-up budget, agent-created commits, and letting normal publication finish the merge. They lose context, cost bounds or exact parent control.

## Decision 8 — check policy remains agent-reported

Before merging, any `fail` or `error` check on the ordinary source turn blocks with `INTEGRATION_CHECKS_FAILED`, even if its outcome is Done. A source turn's empty list or `not_run` is allowed: D6 does not add a mandatory test policy to every task. Preserve the source turn's checks in the integration intent before an auxiliary turn overwrites latest TaskStatus checks; current TaskStatus carries the latest list (`src/task.rs:1484`, `src/task.rs:1564`).

A resolve or verify turn must report at least one applicable check, all `pass`; empty lists or any `not_run` block with `INTEGRATION_CHECKS_NOT_RUN`. `fail` and `error` block immediately, without spending another automatic resolver trying to fix a test failure. Unknown/malformed check data fails closed. This refines the requirement to run checks during recovery; it does not create a host-run command. The existing statuses are Pass/Fail/NotRun/Error (`src/agent/mod.rs:475`).

`verify_merge = "moved-target"` requests a verify turn only for a clean candidate where `T != B` and the merge tree differs from the source tree `H`. If the source tree already contains the target, its existing check report is the evidence. A conflict always requires resolve checks, regardless of verify policy; it does not then require a redundant verify turn. Re-fetch after every auxiliary turn; a changed target invalidates that candidate's check evidence and consumes another candidate attempt. A new clean combination needs a fresh verify if enabled. Default `never` performs no clean-merge verify turn and reports `verification = source_agent_report_only`.

**Rejected:** host-run checks, promoting a reported pass to independently verified, blocking every untested original task, or reusing checks for a different combined tree.

## Decision 9 — authoritative sidecars and bounded recovery

Keep integration out of strict TaskMeta, TaskStatus, LocalTaskRecord and PreparedFollowup schemas. Persist a task-associated policy and current integration record under the queue-owner state root, `integrations/tasks/<task-id>/record.json`; keep host execution state at `host/tasks/<project-id>/<task-id>/integration/record.json`. These are proposed rooted paths relative to the existing owner/host stores. Neither is a second task/queue registry. The owner record drives lifecycle; host receipts prove local/external effects. Existing durable controller rows keep their schema and carry new prepared bodies in Value (`src/controller/protocol.rs:25`, `src/controller/execute.rs:130`).

Directories are 0700, records/locks 0600, no symlink traversal. Use rooted exact replacement, fsync object/ref pins and containing directories before advancing a phase. Public record <=4 KiB; private current record <=64 KiB; each prepared auxiliary intent <=256 KiB and wire frame <1 MiB (`src/controller/protocol.rs:11`). Keep at most three candidate manifests and five auxiliary intents per cycle. Retain only the current cycle and eight bounded archived receipt summaries; retained current state is never evicted to meet a cap. Refuse over-limit/corrupt state with `INTEGRATION_STATE_INVALID`.

Proposed public record:

```json
{
  "schema_version": 1,
  "integration_id": "<uuid>",
  "epoch": 0,
  "revision": 1,
  "target": "refs/heads/main",
  "state": "pending",
  "source_turn_id": "<uuid>",
  "source_head": "<validated-oid>",
  "merge_oid": null,
  "observed_target_oid": null,
  "disposition": null,
  "attempts": 0,
  "resolve_turns": 0,
  "verify_turns": 0,
  "blocked_code": null,
  "retry_exhausted": false,
  "retry_at_millis": null,
  "verification": "source_agent_report_only",
  "updated_at_millis": 0
}
```

Private fields additionally freeze own origin/target key, base/provenance, policy and requested close, source summary/checks, source/last-effect revision, epoch, actor identity, exact candidate parents/tree/message/identity/time, auxiliary prepared binding, workspace backup identity, push intent/receipt and cancellation tombstone. Counters increase before the associated effect. `attempts` counts candidates, not transport retries; each phase keeps its separate retry counter. The public target is display-redacted; the private validated branch is authoritative. UUID/OID fields are validated structural identifiers; do not run a prose token scrubber over them and corrupt identity.

`integration_id = UUIDv8(SHA256("mac-worker/integration/v1", task_id, ordinary_source_turn_id, H, canonical_target_key))`. Re-drive keeps that ID and increments the durable epoch. Candidate ID is `(integration_id, epoch, attempt)`. Persist an allocated timestamp once, so rebuilding the same candidate yields the same merge OID. A new ordinary result gets a new integration ID. Never bind an auxiliary ID to a different purpose/input after recovery.

| State | Allowed next state | Durable meaning / recovery |
| --- | --- | --- |
| absent | armed | Integration disabled unless policy is durably frozen |
| armed | pending, revoked | Await final ordinary Done/import/continuation retirement; cancellation wins |
| pending | fetching, blocked, revoked | Exact source revision frozen; reserve target/launch one child |
| fetching | resolving, verifying, commit_ready, integrated, retry_wait, blocked | Re-run idempotent fetch/base/ancestry checks; pin target before mutation |
| resolving | fetching, retry_wait, blocked, revoked | Prepared resolve ID is durable; replay admission or observe same turn; completed resolution binds to its candidate |
| verifying | fetching, retry_wait, blocked, revoked | Same for verify; accepted tree/report cannot cross candidate identities |
| commit_ready | pushing, fetching, revoked | Tree/parents/message/time and merge pin are durable; rebuild same OID if necessary |
| pushing | published, fetching, retry_wait, blocked | Push intent precedes network; uncertainty reads origin before any repeat |
| published | integrated, retry_wait | Target success known; repair task HEAD/ref/status and owner import/close |
| retry_wait | recorded resume phase, blocked, revoked | Persist next due time; same ID/epoch, no sleeping under fences |
| integrated | new ordinary cycle only | Target success receipt plus owner result import; close cleanup remains separately retryable |
| blocked | pending by manual re-drive, revoked | No automatic retries; stable code and owner action are visible |
| revoked | new ordinary cycle only | Host has acknowledged stop/restoration or target already committed; never resume old epoch |

`integrated/disposition=already_integrated` is a success, including when no worker-created merge exists. Null merge OID is honest; record observed target as the integration proof. Disabled tasks emit no integration field.

Recovery points are explicit:

| Crash point | Required replay |
| --- | --- |
| Policy written, task creation/admission not complete | Existing submission recovery finishes or rolls back; do not orphan enabled work |
| Result imported, ordinary runner not retired | Finalizer settles continuation and stages one intent after retirement |
| Intent saved, child not spawned / child owner gone | Confirm actor absence with process identity; reclaim same intent |
| Fetch completed, target pin not recorded | Fetch again and bind current target; no commit/push evidence exists |
| Workspace preparing / index or HEAD partly changed | Complete/restore exact backed-up metadata from prepared manifest; never launch into partial state |
| Auxiliary prepared / admitted / accepted / completed | Same TurnId/binding; existing runner journal/lease distinguishes each boundary |
| Merge object written, merge pin/receipt missing | Rebuild from frozen inputs, pin and fsync before pushing |
| Push intent durable, push answer lost | Fetch; reachable M/H means success, otherwise recheck target and retry same intent or next candidate |
| Target updated, workspace/ref/status not repaired | `published` repairs to the exact receipt; never push again |
| Owner receipt imported, auto-close partially finished | Re-drive ordinary close against its existing intent; preserve integrated result |
| Success/blocked state durable, event lost | Addressed reads and periodic notifier repair derive state; journal is only a hint |
| Revoke durable on owner but host acknowledgement lost | Re-send same tombstone; do not declare terminal cancellation until uncertainty is settled |

Host GC treats armed active integration, pending phases, uncertain pushes and blocked integration as retained work even without a lease. Protect source/candidate pins, workspace and receipt; integrate this predicate into both GC inventory and deletion rechecks (`src/gc.rs:549`, `src/gc.rs:1035`). A blocked task retains its session until explicit close/discard. Scratch state is bounded; no indefinite unbounded candidates. Once revoked/closed and no ambiguous effect remains, normal retention may collect integration scratch/pins. Ordinary result retention and session-import package GC stay unchanged.

**Rejected:** adding fields to all strict task records, making the event journal authoritative, rebuilding timestamps on replay, or allowing ordinary idle retention to destroy recovery inputs.

## Decision 10 — target serialization, fairness and DAG gates

At the queue owner allow one Git integration drive in flight for `(normalized-own-origin, refs/heads/branch)`, across runs and checkout paths. Use a short target lock to publish a durable actor reservation; only its child may issue that drive's host steps. Reclaim only on confirmed process absence, never lease age alone. RPC, leader, normal runners and direct reconcile all use this same store. Do not hold StateLock/QueueLock/runner-journal fences during host work. Independent target keys can progress concurrently; cap detached Git drivers at four per owner.

Canonical origin follows the existing normalization, with an explicit `file://` fixture exception (`src/project.rs:264`, `src/git_transport.rs:1055`). Different SSH/HTTPS URL spellings can still name the same remote repository; v1 serializes their canonical configured strings, not unverifiable repository equivalence. Document this limit. Multiple queue owners are outside one reservation domain and rely on the guarded fast-forward push for safety.

Release the target reservation when a resolve/verify turn is queued, during backoff and when blocked. Preserve a durable readiness ticket. Select oldest eligible `(ready_at, run_position, task_id)`, rotate a returning repair behind already-ready tickets once, and apply the three-candidate/two-resolve caps. Never reserve a branch while waiting for a scarce agent slot. A conflicted task cannot monopolize unrelated integrations. Reacquire and fetch after its turn; movement may require a new candidate and another bounded resolver. A child crash does not reset ticket age or budgets.

Within a flat run, integrate by readiness, using run position only as a tie-breaker; do not wait for a slow earlier independent task. For configured DAG parents, require `integrated` and current successful result import before admitting children. A configured parent with requested `--close-on never` can be Ready while Open after integration; it needs no accept. For unconfigured parents keep the existing Closed + Done gate unchanged. Resolve `from:<id>` to the integration receipt's imported head, including M after conflict repair, not a pre-repair stale H. Bind and pin it through the existing DAG mechanism (`src/task_client.rs:5307`, `src/task_client.rs:5323`).

While a parent is pending or integration-blocked, descendants remain queued with `INTEGRATION_DEPENDENCY_BLOCKED` for the latter; this is reversible and does not mark an otherwise successful source turn Failed. Re-drive clears that dependency wait. Explicit abandonment or close-without-integration permanently blocks configured children with `INTEGRATION_DEPENDENCY_NOT_INTEGRATED`. Reject an integrating `from:` child whose parent has integration disabled or a different canonical target: its base cannot be guaranteed on its target. The owner can disable child integration or align targets before submission. Validate this combination at batch freeze; do not invent a cross-target bridge.

**Rejected:** holding the branch lock for the agent's whole turn, mandatory run-order integration, requiring manual close of integrated Open parents, allowing dependent integration before parent success, or automatic bridging between target branches.

## Decision 11 — mutations revoke authority before terminal state

Every drive checks owner revision, epoch and close/cancel markers before preparation, auxiliary admission and push. Host steps also validate the current host tombstone. A host task fence linearizes push initiation against revoke/close. An RPC timeout is not proof a host process stopped.

| Operation | Proposed interaction |
| --- | --- |
| `task close <id>` | Stop/revoke integration, settle uncertain push, restore retained source result if unpushed, then existing close; keep result ref. It never authorizes a merge. |
| `close --discard` | Same revoke barrier first, then existing delivery/discard guards and deletion. Never delete or rewind origin target. |
| `task cancel <id>` | Persist cancellation request, interrupt auxiliary/host child, revoke and settle push before publishing Cancelled. Already integrated returns `INTEGRATION_ALREADY_COMMITTED`; do not report it as cancelled-before-integration. |
| `task say <id>` | During active integration return `TASK_BUSY` (`INTEGRATION_IN_PROGRESS`); explicit `--interrupt` uses the revoke barrier. From blocked, revoke old epoch, retain its candidate diagnostics, restore clean source H, then prepare an ordinary follow-up. Its next Done result creates a new cycle. |
| `task integrate <id>` | Re-drive a blocked/open configured task after cause repair. Same source/target, new durable epoch, reset cycle retry caps; remaining task follow-up limit still applies. Integrated is an idempotent success; closed/discarded/cancelled is refused. No on-the-fly target change. |
| `task publish-retry <id>` | Only ordinary task-ref outbox intents, as today (`src/task_client.rs:2726`). It neither resets integration nor bypasses close/cancel fences. |

Do not first call an unrestricted reconcile that might start integration before a requested close/cancel tombstone is durable; current CLI close reconciles first (`src/task_client.rs:2719`). Wire configured mutations through a freeze/revoke-aware pre-reconcile path. Existing dashboard expected revisions remain necessary (`src/dashboard/task.rs:465`).

A push may have committed before a cancellation request can take effect. Fetch and acknowledge that outcome; never rewrite to undo it. If host/push outcome is unknown, leave the task nonterminal and return `INTEGRATION_STOP_UNCONFIRMED` with the requested mutation still pending. Do not close locally and leave an authorized remote push running. This is the precise D12 guarantee: no new target update after terminal cancel/close/discard is acknowledged. A stopped ordinary task never becomes newly eligible merely because its last reported outcome was Done.

After successful publication, host sets workspace HEAD and task ref to the accepted merge M, restores a clean matching index/tree, and persists TaskStatus head M; owner imports M before exposing final success. For already-integrated, use observed T as the canonical continuation head, with no new merge. Requested Never keeps this workspace/session for the next `say`, whose base is this accepted head. Requested Done performs the ordinary explicit close, retaining the result. Keep the original task head H/source turn in the receipt for provenance. Existing ordinary task-ref outbox intents still reference their original exact OIDs; do not retarget them to M.

**Rejected:** pretending cancellation can undo a remote push, terminalizing on an SSH failure, letting `say` overwrite a live merge workspace, or giving publish-retry two unrelated meanings.

## Decision 12 — commit identity, message and privacy

Current task submission freezes `mac-worker <mac-worker@localhost>` (`src/task_client.rs:62`, `src/task_client.rs:1357`); host turn commit sets both author and committer from TaskMeta (`src/turn.rs:1450`). Use that same frozen identity for the merge, never the origin login or a guessed human identity. Persist author/committer time once with the candidate; disable signing. A protected branch requiring signed commits blocks under the policy-rejection code; do not change identity or signing automatically.

Proposed message:

```text
{task title, redacted, <=120 UTF-8 bytes}

{source turn summary, redacted, <=1024 UTF-8 bytes}

Mac-Worker-Task: {task_uuid}
Mac-Worker-Turn: {ordinary_source_turn_uuid}
Mac-Worker-Integration: {integration_uuid}
```

Entire message <=2 KiB. UUID trailers use validated lowercase hyphenated identifiers. Make title/summary single paragraphs; escape controls and prevent injected blank lines/trailers with the existing fixed-point boundary (`src/redaction.rs:173`). Empty summary uses fixed text `Task completed; see the retained task result.`. Auxiliary resolver prose does not replace the source task title/summary. Apply the launched host redaction context to every new prose field/prompt fragment, not only laptop-home redaction; reported checks already pass that boundary (`src/redaction.rs:155`).

Public errors use catalogued stable codes and static owner guidance. Public origin identity is a digest, never a credential-bearing URL. Redact display branch/path/title/summary fields; keep validated OIDs and UUIDs structural, since the prose redactor intentionally masks long hex strings (`src/redaction.rs:20`). Do not store raw stderr as a blocked reason. Typed counters/codes/IDs carry event payloads; no commit-message prose is copied to the journal.

**Rejected:** worker-account login as author, raw summaries, arbitrary trailers from agent output, unbounded conflict/error text or redacting structural OIDs into invalid identifiers.

## Decision 13 — protocol 7, observable states and explicit unavailable peers

Keep `PROTOCOL_VERSION = 7` (`src/protocol.rs:5`). Add feature strings `task.integration` and `controller.integration`; only the integrated implementation advertises them, sorted as required by `src/features.rs:24`. An enabled submit requires controller feature in controller mode and selected host feature in both modes, using current discovery/observations. Unknown/missing feature, including binary rollback after discovery, returns or blocks `INTEGRATION_UNAVAILABLE`. Never fall back to an ordinary enabled submission or interpret unavailable as success.

Use new gated controller mutation bodies: `task.submit-integrating` wraps `{submit: FrozenSubmitBody, integration: FrozenIntegrationPolicy}`; `task.batch-integrating` wraps `{batch: FrozenBatchBody, integrations: {task_uuid: policy_or_null}}`; `task.integrate` carries task ID, expected integration revision and durable re-drive identity. In an integrating batch the map has exactly one entry per frozen task; null explicitly means disabled. Reject missing/extra IDs instead of inferring policy. Ordinary bodies/commands are unchanged. The wrapper is frozen and hashed as one body; policy sidecars are published before task/admission effects. Source streaming still uses the existing source/package fields extracted from the nested frozen submit. Extend source-finished and paired-pin-release routing to recognize that nesting (Decision 14).

For reads, new clients request the existing safe `task.list` envelope with an exclusive `integration` selector, `{task_ids: [<=16 IDs]}`, returning a new versioned integration-map result. Dispatch it before ordinary list and before durable mutation fallback. This mirrors the existing safe event selector route (`src/controller/execute.rs:784`). An old server's existing list-key rejection is read-only (`src/controller/read.rs:462`). Do not send a new read command that might create a durable unsupported mutation. Gate the selector by `controller.integration`, and still handle new-discovery/old-execution rejection explicitly.

| DTO / surface audited | Compatibility treatment |
| --- | --- |
| TaskMeta custom wire / FrozenSubmitBody | No integration fields; effective Never is an existing value (`src/prepared_submit.rs:20`, `src/task.rs:1103`) |
| TaskStatus / TurnSummary / LocalTaskRecord / PreparedFollowup | No new emitted keys/enum variants; purpose/state lives in sidecars (`src/task.rs:1551`, `src/prepared_followup.rs:27`) |
| Host TaskStatusResponse | Remains byte-compatible; integration read has its own gated DTO (`src/task_store.rs:474`) |
| ControllerReadReply outer envelope | Unchanged strict keys (`src/controller/read.rs:25`) |
| ControllerTaskStatusResult | Add integration annotation only inside existing `events: Vec<Value>`, following questions-policy precedent (`src/controller/read.rs:142`) |
| ControllerTaskResult | No new keys; new client uses companion integration selector (`src/controller/read.rs:379`) |
| TaskListProjection / TaskListRow | Add optional `integration` and `workflow_state`; baseline row decoder is tolerant, not deny-unknown (`src/task_view.rs:159`). Preserve existing ReviewState variants (`src/task_view.rs:23`) |
| DagFrozenSpec / FrozenBatchBody | No integration keys; wrapper map and sidecars carry policy (`src/dag.rs:60`) |
| Controller durable/envelope wrappers | Existing Value body/prepared holds new gated bodies; outer codecs unchanged (`src/controller/envelope.rs:34`) |
| TaskFactsWire | Tolerant optional integration annotation; old decoders ignore it (`src/controller/events/contracts.rs:986`) |
| New integration DTOs/records | Versioned validating codecs, bounded fields; never delivered on an old command expecting a strict old result |
| Local TOML/batch input | New optional keys parse on new binary; old inputs retain defaults. An old binary cannot parse a new feature's TOML keys; fail visibly rather than promising impossible rollback parsing (`src/project_config.rs:149`, `src/task_client.rs:1019`) |

New CLI renderers combine ordinary reports with the integration companion into `task status/result/list --json`. Emit a sibling `integration` object, not a key inside strict `status`. Full output keeps the canonical task record separate from the sidecar. Disabled tasks omit it. When the server feature is absent, new status reads say integration support unknown/unavailable; an explicit integrating operation returns the stable unavailable code. A new reader never infers disabled from a failed extension read.

Proposed text examples:

```text
integration: pending target=main attempts=0
integration: resolving target=main attempts=1 resolve_turns=1
integration: integrated target=main merge=<short-oid> attempts=1
integration: already integrated target=main observed=<short-oid>
integration: blocked target=main code=INTEGRATION_CHECKS_FAILED
```

Keep existing TaskState/ReviewState enums and project a separate `workflow_state`: queued (ordinary admission), running (ordinary/auxiliary agent turn), integrating (pending Git/retry/receipt work), needs_you (integration blocked or existing actionable outcome), done (integration success or existing terminal closed task). Integrated requested-Never is done even though the legacy TaskState is Open. Cancelled/discarded remain terminal with their existing outcome. Pending configured Done tasks project existing `not_reviewable`; blocked ones use existing `ready_for_follow_up` plus integration reason; success has no attention card.

Dashboard snapshot task rows/detail carry the same optional record/workflow value. Update attention filtering: baseline treats every Open task as attention (`ui/src/lib/attention.ts:7`), so it must explicitly exclude automatic pending/integrated work and include integration-blocked work. Display target/state/code and re-drive/close actions. Preserve draft/revision guards; do not add kanban UI. Legacy accept remains explicit close/give-up, not permission for integration.

Controller journal hints are `task.integrating`, `task.integrated` and `task.integration_blocked`; data is task/run/source-turn/integration IDs, epoch/state, stable code and structural merge OID where applicable, within the existing 1,024-byte envelope. Publish only after authoritative state durability and outside fences. Existing unknown kinds are ignored (`src/controller/events/contracts.rs:475`). Integrate this annotation into addressed facts/digest and periodic repair. For N-1 notifier safety, set existing busy/quiescent proof conservatively during pending integration; for blocked integration project SafeOutcome::Blocked/code in **facts**, retaining the actual Done task result. New notifier confirms the matching integration revision before displaying `Task integrated into main` or `Integration blocked: <static reason>`. Integrated uses the existing Done sound; blocked uses Request. Save dedup decision before channel delivery, keyed by integration ID/epoch/state so a push recovery does not alert twice. Events alone never authorize a banner.

**Rejected:** widening all strict DTOs, adding ReviewState enum values that N-1 cannot decode, unsupported read commands through durable fallback, unconfirmed completion banners or an accept card for already integrated work.

## Decision 14 — preserve session-transfer Round 2 contracts

Read the binding Round 2 section of [session transfer](2026-10-03-session-transfer-design.md) (`docs/superpowers/specs/2026-10-03-session-transfer-design.md:13`); its older illustrative anchors do not override current code. Integration has no package format or native-session write path.

- Keep source import gates: snapshot/local source only, explicit agent agreement and existing version requirement. Current gates reject origin-source import and dirty-without-WIP (`src/task_client.rs:6658`); integration additionally rejects WIP. A dirty interactive session must be submitted without integration or after committing its intended base.
- Preserve first prepare order: workspace, active status, import receipt/place/bind, complete receipt, package-ref removal (`src/task_store.rs:805`, `src/task_store.rs:826`, `src/task_store.rs:918`). Arm integration after successful prepare/import, before agent launch. Integration work only starts after its ordinary result finishes and is imported.
- Keep shared base+session transport and controller task package pins (`src/turn_runner.rs:1369`, `src/controller/execute.rs:548`, `src/controller/execute.rs:577`). Never treat the parentless package commit as a merge parent/base or publish session refs to the project origin.
- Auxiliary turns are FollowUp and use the bound ref (`src/turn_runner.rs:1285`); never replay placement or first-turn prebind. A planned-but-incomplete import cannot start integration repair.
- Update `require_session_source_finished` and submission-pin reconciliation to inspect nested integrating wrappers; they currently gate controller retry before mutation send (`src/lib.rs:1140`, `src/lib.rs:5823`). Missing/incomplete stream stays refused; retry never recaptures the live laptop conversation.
- Use the existing idempotent paired base/session pin release for pre-record failure and submission rollback. Policy/wrapper failure cleans its own sidecar only after those releases; integration success does not invent a second package-release path. Host complete import receipt must not rewrite appended native transcripts on recovery.
- Preserve imported-session lifetime for blocked or requested-Never tasks. Explicit discard uses existing native-session deletion after revocation (`src/task_store.rs:1385`). Do not implement reverse session export or change its unresolved upload-pack policy.

**Rejected:** reimporting for a resolver, changing task/package IDs, carrying session packages in target history, envelope-only retry before source finish or making batch/DAG children inherit `--from-session`.

## Decision 15 — stable failure taxonomy and bounded retries

All codes below are proposed integration codes with static redacted catalog messages. Retryable phases permit **initial try plus three transport retries**, due after 2 s, 10 s and 30 s; each stored phase counter survives crashes. No blocking sleep in a tick, RPC or fence. After exhaustion block with the same cause code and `retry_exhausted=true` in private/public diagnostic metadata; candidate and auxiliary caps remain independent. Manual re-drive is explicit and cannot reset task follow-ups or resurrect a terminal task.

| Stable code | Classification | Owner-facing reason/action; attention |
| --- | --- | --- |
| `INTEGRATION_AUTH_FAILED` | Block immediately | Worker origin login cannot push; repair login, integrate again; needs you |
| `INTEGRATION_NETWORK` | Retry, then block | Origin unavailable/timeout; retry after connection repair; needs you after exhaustion |
| `INTEGRATION_POLICY_REJECTED` | Block immediately | Branch protection, remote hook or signing policy rejected push; fix target policy/permission; needs you |
| `INTEGRATION_TARGET_MISSING` | Early refusal or immediate block | Configured branch does not exist; create it outside this workflow or resubmit corrected configuration; needs you for existing task |
| `INTEGRATION_BASE_NOT_ON_TARGET` | Early refusal or immediate block | Task input base is not reachable from target; resubmit from published target ancestry; needs you |
| `INTEGRATION_WIP_BASE` | Early refusal | WIP bases cannot integrate; no task/card created |
| `INTEGRATION_TARGET_MOVED_EXHAUSTED` | Three candidates, then block | Target kept moving; retry when pushes settle; needs you |
| `INTEGRATION_CONFLICT_BUDGET_EXHAUSTED` | Two resolve turns, then block | Resolution did not converge against changing target; provide guidance/new turn; needs you |
| `INTEGRATION_RESOLUTION_INCOMPLETE` | Retry resolver within resolve/candidate cap, then conflict-budget block | Unmerged entries or introduced markers remain; needs you only after cap |
| `INTEGRATION_CHECKS_FAILED` | Block immediately | Source/resolve/verify reported fail/error; fix through a new agent turn; needs you |
| `INTEGRATION_CHECKS_NOT_RUN` | Block immediately on auxiliary turn | Required recovery checks were absent/not_run; fix access/check instructions; needs you |
| `INTEGRATION_RESOLVE_BLOCKED` | Block immediately | Resolver returned blocked/needs_input/failed/lost/timed_out or invalid result; inspect result/logs; needs you |
| `INTEGRATION_FOLLOWUP_LIMIT` | Block before auxiliary admission | Existing task follow-up allowance is spent; resubmit with sufficient allowance; needs you |
| `INTEGRATION_VERIFY_CHANGED_TREE` | Block immediately | Verify changed the candidate; create a normal follow-up to repair it; needs you |
| `INTEGRATION_TURN_QUEUE_TIMEOUT` | Block after 10 min admission wait | Pinned worker slot did not become available; retry when it does; needs you |
| `INTEGRATION_WORKER_OFFLINE` | Retry, then block | Original worker cannot be reached; bring it back; no automatic session migration; needs you after exhaustion |
| `INTEGRATION_WORKSPACE_MISSING` | Block immediately | Original workspace/session recovery input is gone; result ref may remain, but repair cannot proceed; needs you |
| `INTEGRATION_UNAVAILABLE` | Early refusal or immediate block | Controller/worker lacks required feature, or rollback occurred; upgrade/restart, then retry; never silent skip |
| `INTEGRATION_PUBLISH_TARGET_COLLISION` | Early refusal | Task-ref outbox target equals integration target; choose separate task publication; no task/card |
| `INTEGRATION_CONFLICT_LIST_TOO_LARGE` | Block immediately | Conflict paths exceed safe prompt bounds; split work or guide a new turn; needs you |
| `INTEGRATION_STATE_INVALID` | Block immediately | Durable binding/state is corrupt or over bounds; retain evidence, repair installation; needs you |
| `INTEGRATION_DEPENDENCY_BLOCKED` | Reversible queued wait | Parent needs integration recovery; child has no duplicate attention card; parent is needs you |
| `INTEGRATION_DEPENDENCY_NOT_INTEGRATED` | Terminal DAG block | Parent was given up/abandoned without integration; dependent needs resubmission; needs you |
| `INTEGRATION_STOP_UNCONFIRMED` | Pending stop; retry observation | Push/worker stop outcome is unknown; keep task nonterminal, restore connectivity; needs you after bounded observation |
| `INTEGRATION_ALREADY_COMMITTED` | Informational mutation refusal | Integration won before cancel; keep receipt, close if desired; no new error attention |

Parse `git push --porcelain` and the fixed guard marker, never arbitrary substring guesses from unbounded stderr. Auth uses the existing classifier precedent (`src/git_transport.rs:1001`). Treat `[remote rejected]` as policy, target-moved advertisement/rejected non-fast-forward as fetch-and-rebuild, and unclassified transport failure conservatively as network with a bounded retry. A missing advertised ref is target-missing. Preserve a success receipt even if subsequent close/import cleanup fails; report that cleanup separately rather than calling the successful push failed.

**Rejected:** infinite retries, classifying every push failure as a target race, retrying failing project checks without owner input, or reporting raw remote error prose.

## Decision 16 — deterministic local tests, then separately authorized acceptance

Testing follows `docs/testing.md:6` (existing area targets/test-support facade) and `docs/testing.md:158` (channels/hooks/injected time, isolated durable roots, fake agents). This design phase runs no Rust test unless a current-code claim cannot be established by inspection. It contacts no pool, origin account, SSH service, keychain, Herdr or notifier. The tests below are future implementation acceptance.

- Build a local bare `file://` origin fixture entirely under one private temporary root, with real Git trees/refs, two fake worker stores and an owner store. Commit identities/times are fixed. Prove two-parent order for fast-forwardable and divergent clean candidates, exact single-ref push, ancestor idempotency, base not on target, WIP refusal, missing branch, policy hook rejection and unchanged task-ref outbox behavior.
- Coordinate concurrent same-target integrations with barriers at reservation, advertisement, guard and receive update. Prove one owner drive per target, progress on independent targets, an external push between fetch and advertisement, and an external push after advertisement. Include movement to an ancestor of H so the pre-push guard is tested beyond ordinary non-fast-forward rejection. No force/ref deletion or extra pushed refs may occur.
- Conflict fixtures cover add/add, modify/delete, rename, symlink, executable, binary and multiple merge bases. Assert exact index/HEAD/MERGE_HEAD preparation, bounded/NUL-safe names, no custom hook/merge/filter/diff/fsmonitor/signer execution, host raw staging, residual marker detection and deterministic commit object. A sentinel project check/setup script must remain untouched by host Git operations.
- A fake agent emits controlled Done/Blocked/check lists, changes or leaves conflict files and signals its completion. Use a bound fake session to prove same agent/worker resume and no import replacement. Prove slot occupancy only during agent execution, aux purpose publication hook, no early outbox delivery, follow-up accounting, read-only verify and check/tree binding after target movement.
- Inject a crash after every durable step in Decision 9, including sidecar fsync/ref pin/workspace metadata/auxiliary prompt-CAS-enqueue-accept/commit/push/receipt/import/close/event/revoke boundaries. Restart from the same roots with injected actor liveness. Assert one source intent, one auxiliary ID, one target merge or ancestry success, stable budgets, cancellation fencing and GC retention. Unknown push outcomes must fetch before repetition.
- Decode new responses with copied baseline strict codecs: ordinary submit/status/result/host status/full records, DAG/follow-up bodies and envelopes. Test tolerant row/facts additions, unknown event kinds, integration annotation in Value events and omission for disabled tasks. New-discovery/old-execution must reject the safe selector without durable requests, and enabled submission/helper downgrade must return unavailable without ordinary fallback.
- Process wiring covers controller leader/child/RPC and direct terminal child/reconcile, default-disabled unchanged close, continuation pending, configured Never parent gates, from-task bases, interrupted say, retained close, discard, publish-retry separation, offline revoke and imported-session paired-pin cleanup.
- UI/notifier fixtures prove automatic pending/success has no accept/attention card, blocked code/action appears, draft revisions survive updates, typed workflow states map to the five future columns, and event loss repairs/deduplicates from durable facts. Build embedded assets once after all source tracks.

Use focused groups in the existing `host`, `task`, `controller`, `transfer`, `cli` and `dashboard` binaries, with `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4`; a zero-test filter is failure. The implementation integrator later owns formatting, both support-enabled and production Clippy/build graphs, targeted tests and the full landing gate per `docs/testing.md:36` and `docs/testing.md:44`. This docs-only phase must not run that suite.

The cheapest proposed live acceptance origin is **one throwaway private GitHub repository created by the owner**, initialized with `main`, a small known check and harmless fixtures. All workers already have the stated origin capability, but actual write access and policy are unverified here. No new service or real product branch is needed. **Owner decision Q1:** approve/create that repository and authorize a later acceptance track, including cleanup. Never test race/conflict/rejection cases against a production main branch.

Future authorized checklist:

1. Record accepted binary/build IDs, protocol 7 and controller/host features; confirm current session-transfer acceptance is complete first.
2. Configure the throwaway project and push a known committed base. With integration disabled, confirm today's behavior. Enable `main` and submit one clean task; observe one first-parent merge, exact trailers, no accept, and auto-close only after receipt/import.
3. Run two independent tasks on different minis against the same target, plus a controlled outside push. Confirm serialization, rebuild and no lost commits/force updates.
4. Run a dependent pair; child waits for integrated parent and uses its imported accepted head. Block the parent, re-drive it, and confirm queued child resumes. Verify requested Never also unlocks after integration.
5. Create a conflict; confirm same worker/session resume, checks in the agent, exact parent order and bounded resolver outcomes. Verify a clean moved target with opt-in checks, a failing report and default no verify.
6. Exercise worker auth/network/policy rejection using this disposable origin only, missing target and base/WIP refusal. Restore cause and run `task integrate`; compare stable IDs/epochs.
7. Stop a controlled integration child at each selected durable boundary; restart leader/direct reconcile and confirm one merge. Race cancel/close/say with push and verify no target update after terminal stop acknowledgement; record unknown-stop behavior honestly.
8. Submit from clean imported Codex and Claude sessions; check no recapture/replacement, package pins and session append remain correct. Test disabled-integration WIP session separately.
9. Compare JSON/text/dashboard facts, events/notifier, no success attention card, blocked owner action and old-peer unavailable behavior; use authorized disposable-profile notifications only.
10. Record actual commands, OIDs, first-parent/parent proofs and pending checks in validation. Remove the disposable repository only after the owner authorizes cleanup. Unperformed checks remain pending.

**Rejected:** real-pool experiments during this design phase, wall-clock sleep tests, real agent CLIs in automated fixtures, or calling a fake-origin pass live acceptance.

## Open questions for the owner

1. **Q1 — acceptance origin and authorization.** Approve the throwaway private GitHub repository and who creates/deletes it. Actual worker push permissions must be checked in the later authorized track.
2. **Q2 — project rollout.** Confirm which projects should receive `[task] integrate = "main"`. The proposed v1 has no laptop-wide default.
3. **Q3 — check cost and allowance.** Confirm default `verify_merge = "never"` and sufficient `max_followups` for up to two resolves/three verifies. The design deliberately shares the current allowance and blocks when it is spent.

These questions do not reopen D1–D13. The design preserves the origin target, automatic behavior, merge commits, non-forcing push, worker placement and protocol version. Its explicit refinements are effective host Never plus owner close, strict recovery-check evidence, shared bounded follow-ups, a trusted pre-push advertisement guard, configured DAG gates and honest direct-mode recovery limits. Remaining engineering risks are the auxiliary publication hook, safe workspace/index materialization, distributed stop acknowledgement, new wrapper/session-pin audit and branch policies that forbid merges or the existing unsigned identity.
