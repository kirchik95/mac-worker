# Automatic integration into the base branch

Date: 2026-10-03. Status: proposed for orchestrator review; no implementation or live acceptance performed.

Code baseline: `67183a0b9f5926dc806c0ed96976e8ad39437ef0`. Every current-behaviour anchor below refers to that commit. Names, limits, schemas and commands described as proposed are requirements for the next wave.

Plan: [2026-10-03-auto-integration.md](../plans/2026-10-03-auto-integration.md).

The owner wants completed pool work integrated directly into the project's configured branch on origin, normally `main`, without an accept step. The same task session repairs conflicts. Human attention is reserved for exhausted recovery. Integration is opt-in; an unconfigured task retains its current behaviour. Kanban UI, deployment, host-run project checks and other remotes are outside this wave.

## What the owner sees

Configure the project once with `[task] integrate = "main"`, then submit a batch. Tasks run normally and integrate as they finish. Each newly integrated task adds one merge commit to main, with main's previous tip as its first parent and the task result as its second. The notifier says `Task integrated into main`; the task needs no accept action.

If a merge conflicts, the same task's agent resumes on the same worker and session. It sees its own result as HEAD (`ours`) and the updated target as the other side, resolves the files and reports applicable checks. Integration continues automatically within the configured allowance.

When recovery blocks, the card names the target, stable failure code and a short repair action, for example `Integration blocked: worker origin login cannot push`. The owner fixes the cause and runs `worker task integrate <id>`, or closes the task to keep its result and give up integration. Controller drain pauses this automatic work; re-enabling drives resumes the retained intent.

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

Absent `integrate` means disabled. `integrate = false` explicitly disables inherited policy. A branch string enables it. There is no discovery of origin HEAD and no implicit `main`: the recommended configuration names `main` explicitly. Validate through `BranchName` and additionally cap the integration branch at **255 UTF-8 bytes** before configuration freeze; reject full refs, invalid/overlong names and missing origin. This is an integration-only bound; the current branch validator has no byte cap (`src/task.rs:2769`). Public target display is separately capped at **128 UTF-8 bytes after redaction**, including an ellipsis when shortened. Cut only at a character boundary; actions use the private exact target/ID, never that display label. Freeze origin, branch, verify/requested-close policy and base provenance before effects. Never re-read agent-modified `.worker.toml` during integration.

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

`worker controller drain` is also the integration kill switch. Today drain defers new runner handoffs, while requests and running turns continue (`src/controller/drain.rs:1`, `src/controller/drain.rs:94`). Extend that persisted valve to new integration drives, each subsequent host phase and auxiliary admissions. A phase already admitted finishes its bounded step and persists its receipt, then parks. A running auxiliary turn finishes normally; its next integration phase parks. Policy/intent publication and safe reads may continue, but no parked task starts a push or an auxiliary turn.

`controller disable` persists the same stop gate before unloading; current disable confirms removal and retains configuration/state (`src/controller/init.rs:624`, `src/controller/init.rs:636`). Disabled controller tasks stay with that owner, never direct mode. Helper rollback parks the owner intent when its feature disappears; it retains identity/budgets but cannot constrain old host retention GC. Decision 13 documents the seven-day workspace-loss exposure without a layout barrier. With a compatible helper and an Open retained task, re-enable/undrain resumes the saved phase after observing uncertain effects. A host-closed task instead follows settlement only, never another push/turn. Revoke/close/cancel and stop observation remain available while parked.

**Rejected:** integration on every terminal turn, inside the supervisor's occupied slot, before current-turn import, in a synchronous controller RPC, or continuing new integration phases while drained/disabled. These allow unfinished work, waste scarce slots, stall the controller or defeat the owner's kill switch.

## Decision 4 — base provenance is a hard gate

`--wip` plus integration always fails early with `INTEGRATION_WIP_BASE`, including a snapshot whose tree happens to match a committed tree. The frozen policy carries `base_kind = committed|from-task`; there is no WIP-to-committed reinterpretation. A source-session import with uncommitted work therefore remains non-integratable until submitted from a committed checkout (Decision 14).

At submission run only `ls-remote <origin> refs/heads/<branch>`, with the existing preflight's 30 s deadline, 8 MiB stdout and 64 KiB stderr bounds (`src/git_transport.rs:49`, `src/git_transport.rs:1025`). A successfully observed missing branch is `INTEGRATION_TARGET_MISSING`. If the exact advertised OID is already present in the local object database with complete ancestry, test the resolved committed base locally: pass if it is an ancestor, otherwise refuse `INTEGRATION_BASE_NOT_ON_TARGET`. If that OID/history is unavailable locally or the network fails, record preflight as `unknown` and defer proof to the worker. Do not fetch a target or create an isolated transfer ref at submit. The existing origin preflight tests advertised OID equality, not ancestry (`src/git_transport.rs:138`); it cannot substitute for this gate. A stale tracking ref is never authoritative. Admission still requires the origin capability.

The worker repeats the gate after fetching origin at every candidate build and before a push. The cycle base is the base of the ordinary source turn, including a prior integrated head for subsequent `say`, not invariably the task's original metadata base. A `from:<id>` child freezes the dependency receipt's imported result as its base. Require `is_ancestor(cycle_base, fetched_target)`, with complete objects; no shallow-history guess and no fetching a private base into origin. Validate the policy origin against the frozen project identity; project IDs derive from normalized origin when present (`src/project.rs:67`, `src/project.rs:264`).

D9 concerns unpublished **input ancestry**; newly produced task commits are the output being integrated. It does not forbid the new merge/task commits themselves. An arbitrary local-only ancestor supplied as the task base remains forbidden even when Git could merge it.

**Rejected:** checking that the base exists somewhere on origin, fetching target history at submit, automatically publishing the base, or trusting laptop proof forever. Equality is insufficient; fetching adds avoidable submit cost; only the repeated worker gate proves current target reachability.

## Decision 5 — one explicit two-parent merge and an exact-target lease

Let `T` be the freshly fetched target, `H` the frozen ordinary task result and `B` the cycle base. First test whether `H` is an ancestor of `T`, after the base gate. If so, return `integrated` with disposition `already_integrated`, observed target `T`, and nullable `merge_oid`; a manually squashed/cherry-picked equivalent tree does not satisfy this ancestry check.

Otherwise freeze **attribute source H, ours H, theirs T** in the candidate, then run hardened `git --attr-source=<H> merge-tree --write-tree <H> <T>` in the existing bare mirror. Build `M` with `commit-tree <tree> -p T -p H`; merge side order and commit parent order are different contracts. Always supply both parents, even when T is an ancestor of H. Conflicts/opt-in verify use Decision 7's same H/T orientation and attributes. Verify exactly two distinct parents in that order and the accepted tree. Pin M before any external effect. The clean path without an auxiliary turn leaves the workspace untouched until publication succeeds.

Use the existing hermetic Git/worker credential construction (`src/git_transport.rs:206`, `src/git_transport.rs:237`, `src/git_transport.rs:897`). The existing sender uses `--no-verify` and exposes no expected-target argument (`src/git_transport.rs:223`). Add a separate `push_integration` path from the mirror, with validated durable OIDs and one explicit refspec:

```text
git push --porcelain --no-verify --force-with-lease=refs/heads/<branch>:<T> <origin> <M>:refs/heads/<branch>
```

Immediately before invoking it, assert that T is M's first parent, both parent identities match the manifest, and T is an ancestor of M. **Amended D4:** never a non-fast-forward update; the only allowed force-related flag is a lease pinned to the exact expected T. No `--force`, unpinned/implicit lease, `+` refspec, `--mirror`, deletion or second ref. Disable automatic tag/submodule pushes (Decision 6). There is no private hooks directory, custom pre-push script or fixed marker.

The explicit `<ref>:<expect>` lease is a compare-and-swap: a differing advertisement is refused as stale, including movement to an ancestor of H. A movement after advertisement fails the server's old-OID T ref transaction. The server receives OIDs rather than client flags; because M descends from T, a policy forbidding non-fast-forward updates still accepts this update, subject to its other rules. The lease form is documented by [Git push](https://git-scm.com/docs/git-push); these race schedules remain mandatory local fixture evidence before feature advertisement.

After **any failed push**, re-observe the exact target with `ls-remote` or fetch, using the same explicit worker credentials/environment, before classification or another push. First settle a reachable M/H as recovered success. A positively missing branch blocks as target-missing. Otherwise target != T means movement: fetch, repeat the base/ancestry gates and rebuild using the next candidate attempt. If target == T, classify recognized auth as `INTEGRATION_AUTH_FAILED`, porcelain `[remote rejected]` as `INTEGRATION_POLICY_REJECTED`, and other failures as network with bounded retry. A failed re-observation remains uncertain and retries observation; it never guesses policy from the original response. This prevents a server-side CAS failure, which can print `[remote rejected]`, from being mistaken for branch policy.

Accept at most **three candidate attempts per cycle**, counted durably before each merge preparation; movement after the third ends in `INTEGRATION_TARGET_MOVED_EXHAUSTED`. Transport/observation retries have separate persisted counters. Unknown push outcome always observes/fetches first and never creates a second merge while the first is reachable.

**Rejected:** rebase/squash, target updates without a merge, mutable-ref retries, implicit leases/custom push guards and bare `push_origin`. They lose identity/CAS or duplicate Git's lease. Swapping merge inputs to match parent order is also rejected: directional built-in drivers such as union must see H as ours on both paths.

## Decision 6 — host operations execute only controlled Git

Proposed hidden commands are `worker host task-integration` and `worker host task-integration-turn`. The former accepts typed `arm`, `step`, `read` and `revoke` actions; `step` advances one named durable phase. The latter validates an auxiliary-turn binding and delegates to normal admission/supervision. Both are gated by host `task.integration`, carry protocol 7 and accept only IDs/frozen policy, never an arbitrary command, repository path or refspec. Add `HostOperation` entries and RemoteJobClient adapters; existing host prepare uses this transport (`src/transfer.rs:584`).

`arm` binds the policy to the existing prepared task before its first agent launch, and requires effective close policy Never. The queue owner's submission policy is durable before the task row/queue effect, so a crash cannot create an unconfigured Done task. A failed arm must not launch the first turn. Ordinary `task_prepare` and session import still finish in their existing order first (`src/task_store.rs:805`).

`step` fetches only the configured target into a private ref, checks ancestry, computes/materializes a merge, records resolution/check evidence, builds/pins the commit, pushes it or repairs a receipt. Git steps hold a host task integration fence, but no execution slot. Refuse workspace mutation while any task lease or ordinary turn is active. Resolve/verify uses a normal worker reservation/lease and releases it at normal supervisor retirement. The host never pumps integration independently after losing its queue-owner drive.

Use the existing worker bare project mirror and task workspace, with the existing hermetic environment and explicit per-command overrides. No additional scratch repository, private index, raw blob staging or hand-built conflict stages. Host and agent share an OS account; this design does not claim a privilege boundary between them. The adapters already distinguish Codex workspace permissions from unattended agents (`src/agent/mod.rs:145`); today's publication runs `git add --all` and `git commit` in this workspace (`src/turn.rs:1375`, `src/turn.rs:1377`). The host still must not invoke project setup recipes, checks, package managers or scripts.

Every integration Git command includes `-c core.hooksPath=/dev/null -c core.fsmonitor=false -c commit.gpgSign=false -c submodule.recurse=false`. Also set `merge.autoStash=false`, `merge.verifySignatures=false`, `fetch.recurseSubmodules=false`, `push.followTags=false`, `push.recurseSubmodules=no` and `core.attributesFile=/dev/null` through `-c`; use `--no-ext-diff --no-textconv` for host diffs. Command-line overrides win over repository config; global/system config and inherited Git overrides remain stripped by the existing factory (`src/git_transport.rs:914`, `src/git_transport.rs:920`, `src/git_transport.rs:931`). Transport credentials remain the existing explicit exception, never project commands. [Git config](https://git-scm.com/docs/git-config) documents the per-command hooks override.

Read configured driver names through bounded, NUL-safe Git config queries before workspace operations. For each filter name override `clean`, `smudge` and `process` to empty and `required` to false. Replace any configured merge driver command with a fixed worker-chosen Git `merge-file` command, with its recursive driver likewise controlled; no repository command string is executed. Keep Git's built-in attribute/merge behavior and standard porcelain. Host diff commands disable external diff and textconv explicitly. The driver override behavior must be proved by the planted-config sentinel fixtures; an unsupported configuration fails before execution rather than silently running project code. Git 2.50.1's [filter dispatch](https://github.com/git/git/blob/v2.50.1/convert.c) skips empty commands; its [merge dispatch](https://github.com/git/git/blob/v2.50.1/merge-ll.c) needs an explicit trusted replacement for configured driver commands.

The mirror is initialized bare (`src/host_store.rs:3675`), and the current hermetic factory strips inherited `GIT_ATTR_SOURCE` (`src/transfer_repo.rs:56`). Do not assume bare merging loads attributes from a merge input. Explicitly pass `--attr-source=<H>` for mirror merge-tree and workspace merge/staging, retaining H's committed attribute rules throughout candidate preparation. Set `GIT_ATTR_NOSYSTEM=1`, keep `core.attributesFile=/dev/null`, and assert mirror `info/attributes` and workspace `.git/info/attributes` are absent or empty; ambient overrides fail as `INTEGRATION_STATE_INVALID`. This preserves built-in `-merge`/union rules without enabling executable project drivers. [Git 2.50.1's attribute-source option](https://github.com/git/git/blob/v2.50.1/Documentation/git.adoc#L209) and [attribute lookup](https://github.com/git/git/blob/v2.50.1/attr.c#L791) support this explicit source/isolation requirement.

Fetch T into a private mirror ref, run `git --attr-source=<H> merge-tree --write-tree <H> <T>`, build/pin M and push from that mirror. Git 2.50.1 is the supplied deployment baseline; verify it before advertising the feature. [Git merge-tree](https://git-scm.com/docs/git-merge-tree/2.50.0) uses the normal merge engine without changing an index/worktree. Conflict/verify uses hardened workspace `git --attr-source=<H> merge --no-ff --no-commit <T>` from HEAD H. Final success reset uses accepted M's attributes, not a recomputed merge. The session path stays unchanged.

Local Git steps have 60 s deadlines; fetch/push 60 s; host step/RPC 90 s. Credential lookup retains its 5 s cap (`src/git_transport.rs:51`); output is 64 KiB/stream. These integration-only limits differ from the existing 15 min generic Git deadline (`src/git_transport.rs:31`). Auxiliary admission has **10 minutes of active admission time**. Consult the pause gate before timeout/retry accounting; persist remaining admission budget at park and restore its deadline on resume (Decision 9). Drain/restart time is not worker-capacity failure. An already running auxiliary keeps `min(task timeout, 10 minutes)` execution timeout and inherited model/agent budgets/environment/permissions.

Use interruptible process execution. Logs contain task/turn/intent IDs, phase, attempt and stable code, plus bounded redacted text. They contain no raw Git stderr, credential-helper output or workspace contents. A lost connection records an uncertain phase; it never authorizes a second push before reading host state and origin. Locks protecting task/queue records are dropped before SSH, Git, launching agents or waiting. The host task integration fence may serialize a bounded push with revoke; unrelated tasks remain responsive.

**Rejected:** a host-owned scratch repository and raw conflict/index materialization, repository-defined executable Git extensions, executing `.worker.toml` checks outside the agent, moving the session workspace, or holding a slot for Git-only steps. Standard hardened Git handles renames, binary files, modes and merge bases without a second merge implementation.

## Decision 7 — resolve/verify are prepared follow-ups with a publication hook

Persist `PreparedIntegrationTurn { integration_id, epoch, attempt, purpose, followup: PreparedFollowup, workspace_binding, approved_turn_limits }` in a separate sidecar before prompt write, task CAS, queue admission or launch. `purpose` is `resolve` or `verify`. Do not add purpose to strict existing turn/follow-up DTOs. Derive the auxiliary TurnId from a domain-separated UUIDv8 hash of integration ID, epoch, candidate attempt, purpose and ordinal; persist the timestamp and full prepared binding. Existing prepared follow-up identity and binding provide the model (`src/prepared_followup.rs:185`, `src/prepared_followup.rs:242`). Replay must find the same turn, never allocate another.

Pin the same worker and resume its bound session, same agent/model/profile/permissions. Do not recapture an imported session or rerun first-turn prepare. Auxiliary turns appear in existing task turn history and consume **the ordinary `max_followups` allowance**; the integration caps are additional bounds. Existing prepare already counts all follow-ups (`src/prepared_followup.rs:80`). If no allowance remains, block with `INTEGRATION_FOLLOWUP_LIMIT` before admission. Automatic ordinary continuations retain their present budget semantics. Within a cycle allow at most **two resolve turns and three verify turns** (one verify per accepted candidate at most). Retry of the same auxiliary ID does not spend a second turn. Manual re-drive resets cycle caps, never the task's already spent follow-up allowance.

Before conflict or verify preparation, record branch `task/<id>`, HEAD/task ref H, inventory, target T and attribute source/ours H, theirs T. Run hardened `git --attr-source=<H> merge --no-ff --no-commit <T>` from clean H; attributes are H's checked-out committed rules, explicitly selected to remain frozen. HEAD stays H, MERGE_HEAD is T; Git materializes standard index/worktree/conflict stages. For a pinned clean verify candidate, compare `write-tree` with its pinned tree **before launch**. A mismatch or unexpected unmerged index blocks `INTEGRATION_VERIFY_TREE_MISMATCH`; never switch to the recomputed tree or launch a resolver for that mismatch. Public head remains H. Agents never commit or stage metadata.

The existing resume path requires branch `task/<id>`, HEAD equal to the public result base, and a clean workspace (`src/task_store.rs:1619`, `src/task_store.rs:1630`, `src/task_store.rs:1637`), and JobService requires exact task turn limits (`src/job_service.rs:566`). Add `TaskStore::prepare_integration_resume` and a gated `JobService::submit_integration_turn` adapter. They retain the ordinary branch/head/base checks exactly, requiring H, and relax only clean porcelain to a merge in progress with MERGE_HEAD T and candidate-bound index/worktree. They also permit the approved reduced timeout; other limits stay equal to TaskMeta. Require the durable purpose/candidate, normal task lease, next turn number and same agent/session/model/profile/permissions. Ordinary `prepare_resume`/`submit_turn` stays strict. Persist purpose before accepted-job execution so publication/repair can find it without extending ExecutionPayload/TurnSection wire schemas.

**Required publication hook:** on these marked turns, parse/redact/persist the structured result and finish the normal lease, but skip `publish_workspace`, ordinary task-ref outbox creation and host auto-close. Otherwise the current publisher would execute `git commit` before integration accepts the resolution (`src/turn.rs:1242`, `src/turn.rs:1375`). Keep the auxiliary result/checks in TaskStatus and the candidate receipt, then retire the queue row. The queue-owner finalizer uses the purpose sidecar to bypass ordinary result-head import/automatic continuation for that auxiliary turn and wakes the same integration intent. No unmarked turn gets this exception.

After a successful resolve, validate branch `task/<id>`, HEAD H and MERGE_HEAD T. Run hardened `git add -A`, reject remaining index stages 1/2/3, and scan changed tracked text for residual conventional or diff3 conflict markers. Check every conflicted path and newly changed text; unchanged literal markers elsewhere do not cause false failures. Run `write-tree`, then `commit-tree -p T -p H`; parent order is independent of workspace HEAD. Marker removal and no unmerged entries are host Git/file checks, not project-check evidence. Include and record edits outside conflicts; the agent reports their checks under Decision 8. Invalid output, NeedsInput, Blocked, timeout or fail/error stops recovery; only Done with the required evidence proceeds.

For a clean moved-target verify, preparation leaves HEAD H, MERGE_HEAD T and an index proven equal to the pinned tree. Verify is read-only: edits beyond baseline inventory block `INTEGRATION_VERIFY_CHANGED_TREE`. Compare index tree with the same pin again **after the turn** and after hardened H-attribute `add -A`/`write-tree`; a mismatch blocks `INTEGRATION_VERIFY_TREE_MISMATCH`. The host never adopts a recomputed tree silently. A normal `say` can produce a new source result later.

Proposed exact resolve prompt body (fixed framing is composed with the existing adapter's structured-result instruction):

```text
Resolve this task's integration into the configured origin branch.
Task: {task_uuid}; source turn: {source_turn_uuid}; attempt: {attempt}/3.
The host prepared a merge in this workspace. HEAD (ours) is this task's result; MERGE_HEAD is the updated target.
Read-only git status, diff and log commands are fine.
Conflicted repository-relative paths (JSON strings, redacted):
{one_json_string_per_path}
Inspect Git's unmerged-path list in this workspace if a displayed path was redacted.
Resolve every unmerged path, including deletions. Remove conflict markers.
Do not commit, stage Git metadata, switch branches, rebase, squash or push. The host will stage and commit.
The source turn reported checks: {yes_or_no}.
Run applicable project checks inside this turn and report commands/results truthfully.
If the source reported checks, report at least one, all pass; a missing/not_run check blocks integration.
If the source reported none, an empty checks list is allowed. Any fail/error blocks integration.
Return done only when resolution and the applicable check rule succeed; otherwise return blocked.
The host will not run project checks.
```

Proposed exact verify prompt body:

```text
Verify this task's clean merge onto a moved origin target.
Task: {task_uuid}; source turn: {source_turn_uuid}; attempt: {attempt}/3.
The host materialized the candidate merge in this workspace.
HEAD (ours) is this task's result; MERGE_HEAD is the updated target. Read-only git status, diff and log are fine.
Do not change files, commit, stage Git metadata, switch branches, rebase, squash or push.
The source turn reported checks: {yes_or_no}.
Run applicable checks and report commands/results truthfully. Any fail/error blocks integration.
If the source reported checks, report at least one, all pass; missing/not_run means return blocked.
If the source reported none, an empty checks list is allowed. Return done when the applicable rule succeeds.
The host will not execute project checks or treat your report as independent verification.
```

Prompts are bounded to 16 KiB UTF-8 after redaction; list at most 256 paths, each at most 1,024 bytes, with the whole list also fitting that prompt. Oversized/invalid metadata blocks with `INTEGRATION_CONFLICT_LIST_TOO_LARGE`; never truncate away a conflict. Decode Git's NUL-delimited paths, validate relative paths, JSON-escape/redact them and append fixed framing. Redacted names remain inspectable through the index. No remote URL or absolute path appears. If T changes, archive old resolution evidence, run hardened `merge --abort` (or an equivalent hardened restore) to clean H and prepare again. Remove only auxiliary-created untracked files identified by the saved inventory; never run an unrestricted clean. The next prompt states that the old candidate was superseded; its edits/checks are not claimed as retained. Successful publication instead CAS-updates `task/<id>` from H to M and resets clean to M (Decision 11).

**Rejected:** a fresh agent/session, unlimited resolver retries, a separate free follow-up budget, agent-created commits, and letting normal publication finish the merge. They lose context, cost bounds or exact parent control.

## Decision 8 — check policy remains agent-reported

Before merging, any `fail` or `error` check on the ordinary source turn blocks with `INTEGRATION_CHECKS_FAILED`, even if its outcome is Done. A source turn's empty list or `not_run` is allowed: D6 does not add a mandatory test policy to every task. Preserve the source turn's checks in the integration intent before an auxiliary turn overwrites latest TaskStatus checks; current TaskStatus carries the latest list (`src/task.rs:1484`, `src/task.rs:1564`).

If the ordinary source reported at least one check, a resolve/verify turn must report at least one, all `pass`; an empty list or any `not_run` blocks with `INTEGRATION_CHECKS_NOT_RUN`. If the source reported none, the auxiliary may also report none; `not_run` is blocking only under the required-check rule. In both cases any `fail` or `error` blocks immediately, without another automatic resolver trying to fix a test failure. Unknown/malformed check data fails closed. Projects without checks can resolve conflicts automatically. This does not create a host-run command; statuses remain Pass/Fail/NotRun/Error (`src/agent/mod.rs:475`).

`verify_merge = "moved-target"` requests a verify turn only for a clean candidate where `T != B` and the merge tree differs from the source tree `H`. If the source tree already contains the target, its existing check report is the evidence. A conflict always requires a resolve turn under the source-dependent check rule, regardless of verify policy; it does not need a redundant verify turn. Re-fetch after every auxiliary turn; a changed target invalidates that candidate's check evidence and consumes another candidate attempt. A new clean combination needs a fresh verify if enabled. Default `never` performs no clean-merge verify turn and reports `verification = source_agent_report_only`.

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
  "resume_state": null,
  "pause_reason": null,
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

Private fields freeze origin/target key, base/provenance, policy/requested close, source summary/checks/revision, epoch/actor, candidate parents/tree/message/identity/time/**attribute source and H/T side order**, auxiliary binding, clean-H merge manifest, push intent/receipt and tombstone. A park saves resume phase, reason, effective pause timestamp, uncertain effect and remaining admission/backoff budget. Counters precede effects; candidate attempts differ from phase transport retries. Public target is redacted/bounded display; private exact branch is authoritative. UUID/OIDs are structural, never prose-scrubbed.

`integration_id = UUIDv8(SHA256("mac-worker/integration/v1", task_id, ordinary_source_turn_id, H, canonical_target_key))`. Re-drive keeps that ID and increments the durable epoch. Candidate ID is `(integration_id, epoch, attempt)`. Persist an allocated timestamp once, so rebuilding the same candidate yields the same merge OID. A new ordinary result gets a new integration ID. Never bind an auxiliary ID to a different purpose/input after recovery.

| State | Allowed next state | Durable meaning / recovery |
| --- | --- | --- |
| absent | armed | Integration disabled unless policy is durably frozen |
| armed | pending, revoked | Await final ordinary Done/import/continuation retirement; cancellation wins |
| pending | fetching, parked, blocked, revoked | Exact source revision frozen; reserve target/launch one child only when the drive gate is open |
| fetching | resolving, verifying, commit_ready, integrated, retry_wait, parked, blocked, revoked | Re-run idempotent fetch/base/ancestry checks; pin target before mutation |
| resolving | fetching, retry_wait, parked, blocked, revoked | Prepared resolve ID is durable; replay admission or observe same turn; completed resolution binds to its candidate |
| verifying | fetching, retry_wait, parked, blocked, revoked | Same for verify; accepted tree/report cannot cross candidate identities |
| commit_ready | pushing, fetching, parked, revoked | Tree/parents/message/time and merge pin are durable; rebuild same OID if necessary |
| pushing | published, fetching, retry_wait, parked, blocked | Push intent precedes network; a current step persists its answer before parking; uncertainty reads origin before any repeat |
| published | integrated, retry_wait, parked | Target success known; repair task ref/status/workspace and owner import/close |
| retry_wait | recorded resume phase, parked, blocked, revoked | Persist next due time; same ID/epoch, no sleeping under fences |
| parked | saved resume phase, revoked; integrated/blocked by legacy settlement only | Drain/disable/rollback stops new phases/admissions; release actor/target after current phase, retain intent/budgets; resume only with open gates, compatible helper and retained Open task. Legacy Closed follows Decision 13 without new work |
| integrated | new ordinary cycle only | Target success receipt plus owner result import; close cleanup remains separately retryable |
| blocked | pending by manual re-drive, revoked; integrated by retained-receipt settlement only | No automatic business retries; stable code/action visible. Legacy Closed may settle a verified existing effect or replace the blocked code with workspace-missing, never re-drive |
| revoked | new ordinary cycle only | Host has acknowledged stop/restoration or target already committed; never resume old epoch |

`integrated/disposition=already_integrated` is a success, including when no worker-created merge exists. Null merge OID is honest; record observed target as the integration proof. Disabled tasks emit no integration field.

Decision 13's legacy retention-close exception can settle an unsettled record with a frozen eligible source directly to integrated/blocked, without its normal resume phase. This authorizes only origin observation, retained ref/status repair and owner import; host Closed never gains push/turn/workspace authority.

Parking does not reset identity, epoch, attempts, auxiliary queue position or spent follow-ups. **Check pause before spending any timeout/retry budget.** Save `remaining_admission_millis = max(0, deadline - effective_pause_at)` once, bounded by 600,000; on resume use `deadline = resume_now + remaining_admission_millis`. Preserve active time already spent across restarts; never replenish the full allowance. Retain the auxiliary queue entry/ID. Save remaining backoff similarly; a running agent retains its execution deadline. The pause timestamp comes from durable owner gate metadata, so delayed observation cannot count drained time as active. Observe the same completed auxiliary/uncertain push before re-admitting anything. A published receipt remains retained while import/close parks. Helper-unavailable reads show `INTEGRATION_UNAVAILABLE`; legacy host closure follows Decision 13's settlement exception, not automatic resumption.

Recovery points are explicit:

| Crash point | Required replay |
| --- | --- |
| Policy written, task creation/admission not complete | Existing submission recovery finishes or rolls back; do not orphan enabled work |
| Result imported, ordinary runner not retired | Finalizer settles continuation and stages one intent after retirement |
| Intent saved, child not spawned / child owner gone | Confirm actor absence with process identity; reclaim same intent |
| Fetch completed, target pin not recorded | Fetch again and bind current target; no commit/push evidence exists |
| Workspace merge preparing / index partly changed | Verify task branch and H; observe MERGE_HEAD T and standard merge state, or abort/restore clean H and replay preparation; never launch into partial state |
| Auxiliary prepared / admitted / accepted / completed | Same TurnId/binding; existing runner journal/lease distinguishes each boundary |
| Merge object written, merge pin/receipt missing | Rebuild from frozen inputs, pin and fsync before pushing |
| Push intent durable, push answer lost | Fetch; reachable M/H means success, otherwise recheck target and retry same intent or next candidate |
| Target updated, workspace/ref/status not repaired | `published` CAS-updates H→M (or accepts already-M) and repairs status; Open resets clean to M. Legacy Closed repairs only retained ref/status and imports the receipt, without workspace recreation/reset or another push |
| Owner receipt imported, auto-close partially finished | Re-drive ordinary close against its existing intent; preserve integrated result |
| Success/blocked state durable, event lost | Addressed reads and periodic notifier repair derive state; journal is only a hint |
| Revoke durable on owner but host acknowledgement lost | Re-send same tombstone; do not declare terminal cancellation until uncertainty is settled |
| Drain/disable after phase permit / helper rollback | Finish or retain uncertainty for the current phase, save parked resume state, release actor/target; restart cannot launch until gates/features allow; never adopt controller work into direct mode |
| Pre-feature helper GC closed task/deleted workspace | Observe an uncertain push first; reachable M/H settles integrated/import, otherwise block workspace-missing. Keep host terminal and retained result; no push, auxiliary admission or workspace recreation |

The integration-aware helper's GC retains armed/pending/parked/uncertain/blocked work without a lease, protecting pins/workspace/merge state/receipt in inventory and deletion rechecks (`src/gc.rs:549`, `src/gc.rs:1035`). This protection cannot constrain a pre-feature helper; its seven-day retention closure is the explicit Decision 13 exception. Candidate manifests/refs stay bounded. After revoke/close and settlement, ordinary retention may collect them. Ordinary result retention and session-package GC stay unchanged.

**Rejected:** widening strict task records, authoritative event hints, rebuilt timestamps, renewed parked budgets or integration-aware idle GC destroying recovery inputs. Pre-feature retention is the explicit bounded rollback exposure in Decision 13.

## Decision 10 — target serialization, fairness and DAG gates

At the queue owner allow one Git integration drive in flight for `(normalized-own-origin, refs/heads/branch)`, across runs and checkout paths. Use a short target lock to publish a durable actor reservation; only its child may issue that drive's host steps. Reclaim only on confirmed process absence, never lease age alone. RPC, leader, normal runners and direct reconcile all use this same store. Do not hold StateLock/QueueLock/runner-journal fences during host work. Independent target keys can progress concurrently; cap detached Git drivers at four per owner.

Canonical origin follows the existing normalization, with an explicit `file://` fixture exception (`src/project.rs:264`, `src/git_transport.rs:1055`). Different SSH/HTTPS URL spellings can still name the same remote repository; v1 serializes their canonical configured strings, not unverifiable repository equivalence. Document this limit. Multiple queue owners are outside one reservation domain and rely on the exact-T lease and first-parent proof for safety.

Release the target reservation when a resolve/verify turn is queued, during backoff, when blocked and after a current phase parks. Preserve a durable readiness ticket. Select oldest eligible `(ready_at, run_position, task_id)`, rotate a returning repair behind already-ready tickets once, and apply the three-candidate/two-resolve caps. Never reserve a branch while waiting for an agent slot. Reacquire and fetch after its turn; movement may need another bounded candidate/resolver. A crash or park does not reset ticket age or budgets.

Extend the persisted drain valve, not a second stop flag. Acquire a short phase permit under its lock before ownership/handoff or auxiliary admission; drop before SSH/Git/wait (`src/controller/drain.rs:68`, `src/controller/drain.rs:117`). Record integration pause timestamps as separate durable metadata under that same lock, preserving the strict existing drain file. Pause evidence is consulted before timeout/retry accounting, including after restart. Once drain acknowledges no new phase/aux admission succeeds; current work completes then parks. Disable closes the gate; helper rollback closes admission but has the legacy-GC limit in Decision 13. Reads/intent publication/revoke remain available. An enabled compatible owner resumes saved Open work fairly; direct reconcile cannot bypass its valve.

Within a flat run, integrate by readiness, using run position only as a tie-breaker; do not wait for a slow earlier independent task. For configured DAG parents, require `integrated` and current successful result import before admitting children. A configured parent with requested `--close-on never` can be Ready while Open after integration; it needs no accept. For unconfigured parents keep the existing Closed + Done gate unchanged. Resolve `from:<id>` to the integration receipt's imported head, including M after conflict repair, not a pre-repair stale H. Bind and pin it through the existing DAG mechanism (`src/task_client.rs:5307`, `src/task_client.rs:5323`).

While a parent is pending/parked or integration-blocked, descendants remain queued with `INTEGRATION_DEPENDENCY_BLOCKED` for the latter; this is reversible and does not mark an otherwise successful source turn Failed. Resume/re-drive clears that wait. Explicit abandonment or close-without-integration permanently blocks configured children with `INTEGRATION_DEPENDENCY_NOT_INTEGRATED`. Reject an integrating `from:` child whose parent has integration disabled or a different canonical target: its base cannot be guaranteed on its target. The owner can disable child integration or align targets before submission. Validate this at batch freeze; do not invent a cross-target bridge.

**Rejected:** holding the branch lock for the agent turn, mandatory run order, manual close of integrated Open parents, dependent integration before parent success and cross-target bridging. Also reject dropping early disabled/different-target `from:` validation in favor of the worker ancestry gate. That gate would catch unpublished input and could accept a manually published parent/head reachable from both targets, but failure would occur after the child spent a run. V1 deliberately requires aligned enabled policy at batch freeze.

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

After publication, CAS-update `task/<id>` from H to M, reset clean to M and persist TaskStatus head M; replay accepts already-M, never another head. Owner imports M before success. Already-integrated uses observed T, with no new merge. Legacy retention-closed settlement is the Decision 13 exception: repair only the retained ref/status head from the verified receipt, keep Closed and do not recreate/reset a workspace. Supersession or confirmed unpushed revoke aborts/restores clean H; mirror-only work needs no abort. Never retains the Open workspace/session for next `say`; Done explicitly closes. Keep H/source turn and old exact-H outbox intents for provenance.

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

Keep `PROTOCOL_VERSION = 7` (`src/protocol.rs:5`). Add feature strings `task.integration` and `controller.integration`; only the integrated implementation advertises them, sorted as required by `src/features.rs:24`. An enabled submit requires controller feature in controller mode and selected host feature in both modes, using current discovery/observations. Unknown/missing feature refuses submission with `INTEGRATION_UNAVAILABLE`. An already armed intent encountering helper rollback parks with that visible code until compatible support returns (Decisions 3/9); it is retained, never silently skipped. No ordinary enabled fallback or interpretation of unavailable as success.

**Pre-feature helper rollback has a retention limit.** V1 introduces no host layout/version barrier. Old GC can close an idle Open task after seven days without a host status update (`TASK_RETENTION_MILLIS`, `src/gc.rs:25`, `src/gc.rs:570`), even if an integration sidecar says parked/blocked. That non-discard retention close keeps result refs/history/session binding, marks Closed and removes the workspace (`src/task_store.rs:1416`, `src/task_store.rs:1445`, `src/task_store.rs:1458`). Operator docs must say: **rolling the helper back below `task.integration` for more than 7 days can lose the repair workspace of parked or blocked integrations**. The clock uses the last host status update, not the rollback date; ordinary result retention stays unchanged.

On restore, the helper/owner checks host state before any drive. If the legacy helper has closed the task or removed its workspace, settle an uncertain push by observing origin first: reachable M/H proves integration. Repair the retained task ref/status head to verified M or observed T while **keeping Closed**, then owner-import that receipt; never recreate/reset the workspace or reopen the task. Otherwise block `INTEGRATION_WORKSPACE_MISSING`, retaining its result. Failed observation retains uncertainty under the bounded observation policy; no push. Host-closed never authorizes a push, auxiliary, old-cycle resume or DAG unlock merely from Closed + Done. This settles an existing effect, never resurrects integration; manual re-drive still refuses terminal tasks.

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
| TaskFactsWire | Optional typed IntegrationFactsAnnotation <=512 bytes, not the <=4 KiB snapshot; full compound facts <=2 KiB on producer/decoder. Old wire is tolerant (`src/controller/events/contracts.rs:986`) |
| New integration DTOs/records | Versioned validating codecs, bounded fields; never delivered on an old command expecting a strict old result |
| Local TOML/batch input | New optional keys parse on new binary; old inputs retain defaults. An old binary cannot parse a new feature's TOML keys; fail visibly rather than promising impossible rollback parsing (`src/project_config.rs:149`, `src/task_client.rs:1019`) |

New CLI renderers combine ordinary reports with the integration companion into `task status/result/list --json`. Emit a sibling `integration` object, not a key inside strict `status`. Full output keeps the canonical task record separate from the sidecar. Disabled tasks omit it. When the server feature is absent, new status reads say integration support unknown/unavailable; an explicit integrating operation returns the stable unavailable code. A new reader never infers disabled from a failed extension read.

Freeze a separate **IntegrationFactsAnnotation** of at most **512 serialized JSON bytes**, with only integration ID, epoch, revision, state, nullable stable code and nullable structural result OID (merge M or observed T):

```json
{
  "integration_id": "<uuid>",
  "epoch": 0,
  "revision": 1,
  "state": "integrated",
  "code": null,
  "result_oid": "<validated-oid>"
}
```

No target, summary, counters, checks or full snapshot goes into facts. New readers confirm the full <=4 KiB snapshot through the companion `task.list` selector and require matching ID/epoch/revision. Bind the enabled fact digest to the ordinary canonical record plus this canonical annotation, including its revision; disabled facts retain their existing digest/omission. Enforce **2,048 bytes for the whole serialized facts object**, including the annotation, on both producer and decoder. Existing checks are the constant/validator (`src/controller/events/contracts.rs:32`, `src/controller/events/contracts.rs:1051`, `src/controller/events/contracts.rs:1374`) and a separate producer check (`src/controller/events/task_reads.rs:847`). Keep both limits; reserve annotation space, shorten/omit only optional display title if needed, never identities/revision/code/OID. Boundary fixtures cover maximum valid annotation fields within 512 bytes, oversized annotation rejection, the 2,048-byte compound boundary/overflow at producer and decoder, maximum target/display input, and N-1 omission/unknown-field behavior.

Proposed text examples:

```text
integration: pending target=main attempts=0
integration: resolving target=main attempts=1 resolve_turns=1
integration: parked target=main reason=controller_drained resume=commit_ready
integration: integrated target=main merge=<short-oid> attempts=1
integration: already integrated target=main observed=<short-oid>
integration: blocked target=main code=INTEGRATION_CHECKS_FAILED
```

Keep existing TaskState/ReviewState enums and project a separate `workflow_state`: queued (ordinary admission), running (ordinary/auxiliary agent turn), integrating (pending/parked Git/retry/receipt work), needs_you (integration blocked or existing actionable outcome), done (integration success or existing terminal closed task). Integrated requested-Never is done even though legacy TaskState is Open. Cancelled/discarded retain their existing outcome. Pending/parked configured Done projects `not_reviewable`; blocked uses `ready_for_follow_up` plus integration reason; success has no attention card. Parked detail shows reason/resume phase, with no false completed notification.

Dashboard snapshot task rows/detail carry the same optional record/workflow value. Update attention filtering: baseline treats every Open task as attention (`ui/src/lib/attention.ts:7`), so it must explicitly exclude automatic pending/integrated work and include integration-blocked work. Display target/state/code and re-drive/close actions. Preserve draft/revision guards; do not add kanban UI. Legacy accept remains explicit close/give-up, not permission for integration.

Controller hints remain `task.integrating`, `task.integrated`, `task.integration_blocked`, with title-free IDs/epoch/state/code/OID within 1,024 bytes. Publish after state durability/outside fences; unknown kinds are ignored (`src/controller/events/contracts.rs:475`). Addressed facts use only the compact annotation and revision-bound digest; pending/parked stays conservatively busy, blocked projects SafeOutcome::Blocked/code while actual source Done remains. Fetch/confirm the matching full snapshot before an integrated/blocked notice. Dedup in the real notifier cache by integration ID + epoch + state; disabled tasks keep their current task/turn/outcome fingerprint (`src/controller/events/notify/cache.rs:202`, `src/controller/events/notify/cache.rs:226`). Save before delivery as today (`src/controller/events/notify/cache.rs:587`); same-epoch recovery never duplicates, a later blocked epoch may notify again. T5 owns cache policy and shared notifier fixtures. Events alone never authorize a banner.

**Rejected:** widening strict DTOs/ReviewState, unsupported reads through mutation fallback, full snapshots inside facts, raising only one facts limit, identity truncation, unconfirmed banners or success accept cards. A host layout/version barrier for pre-feature rollback is also rejected: it would block the N-1 rollbacks used operationally. V1 accepts the documented seven-day repair-workspace exposure and settles retained results without new pushes.

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

T1 owns each new `src/error.rs` catalog entry, category and static hint, plus only its matching rows in the `docs/usage.md` error table. Existing `hint_for` reads that catalog (`src/error.rs:576`); the usage table is compared exactly by its test (`src/error.rs:1168`). Freeze these contracts before T2–T5; T7 owns usage prose, not deferred catalog repair.

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
| `INTEGRATION_CHECKS_NOT_RUN` | Block on auxiliary turn when source reported checks | Required recovery checks were absent/not_run; fix access/check instructions; projects with no source checks may report none |
| `INTEGRATION_RESOLVE_BLOCKED` | Block immediately | Resolver returned blocked/needs_input/failed/lost/timed_out or invalid result; inspect result/logs; needs you |
| `INTEGRATION_FOLLOWUP_LIMIT` | Block before auxiliary admission | Existing task follow-up allowance is spent; resubmit with sufficient allowance; needs you |
| `INTEGRATION_VERIFY_CHANGED_TREE` | Block immediately | Verify changed the candidate; create a normal follow-up to repair it; needs you |
| `INTEGRATION_VERIFY_TREE_MISMATCH` | Block before/after verify | Prepared index differs from the pinned candidate; retain diagnostics and repair merge/attribute handling; never adopt another tree; needs you |
| `INTEGRATION_TURN_QUEUE_TIMEOUT` | Block after 10 min active admission time | Pinned slot did not become available while admissions were enabled; paused time is excluded; retry when capacity is available; needs you |
| `INTEGRATION_WORKER_OFFLINE` | Retry, then block | Original worker cannot be reached; bring it back; no automatic session migration; needs you after exhaustion |
| `INTEGRATION_WORKSPACE_MISSING` | Block after uncertain-push settlement when applicable | Repair workspace/input is gone, including pre-feature retention close; keep retained result, resubmit as needed; no reopening or new push; needs you |
| `INTEGRATION_UNAVAILABLE` | Early refusal; existing intent parks on helper rollback | Restore compatible helper/owner and clear stop gate to resume retained Open work; legacy Closed only settles receipts/workspace-missing; never silent skip |
| `INTEGRATION_PUBLISH_TARGET_COLLISION` | Early refusal | Task-ref outbox target equals integration target; choose separate task publication; no task/card |
| `INTEGRATION_CONFLICT_LIST_TOO_LARGE` | Block immediately | Conflict paths exceed safe prompt bounds; split work or guide a new turn; needs you |
| `INTEGRATION_STATE_INVALID` | Block immediately | Durable binding/state is corrupt or over bounds; retain evidence, repair installation; needs you |
| `INTEGRATION_DEPENDENCY_BLOCKED` | Reversible queued wait | Parent needs integration recovery; child has no duplicate attention card; parent is needs you |
| `INTEGRATION_DEPENDENCY_NOT_INTEGRATED` | Terminal DAG block | Parent was given up/abandoned without integration; dependent needs resubmission; needs you |
| `INTEGRATION_STOP_UNCONFIRMED` | Pending stop; retry observation | Push/worker stop outcome is unknown; keep task nonterminal, restore connectivity; needs you after bounded observation |
| `INTEGRATION_ALREADY_COMMITTED` | Informational mutation refusal | Integration won before cancel; keep receipt, close if desired; no new error attention |

Parse bounded `git push --porcelain` statuses, with no custom guard marker. After every failed push, re-observe first as Decision 5 requires: reachable M/H settles success; a missing ref is target-missing; a changed target rebuilds. Only an unchanged T permits classification as recognized auth, `[remote rejected]` policy or otherwise bounded network retry. Auth uses the existing bounded classifier precedent (`src/git_transport.rs:1001`). Server CAS rejection cannot become policy solely from its porcelain label. Retain a success receipt even if close/import cleanup fails, and report cleanup separately.

**Rejected:** infinite retries, classifying every push failure as a target race, retrying failing project checks without owner input, or reporting raw remote error prose.

## Decision 16 — deterministic local tests, then separately authorized acceptance

Testing follows `docs/testing.md:6` (existing area targets/test-support facade) and `docs/testing.md:158` (channels/hooks/injected time, isolated durable roots, fake agents). This design phase runs no Rust test unless a current-code claim cannot be established by inspection. It contacts no pool, origin account, SSH service, keychain, Herdr or notifier. The tests below are future implementation acceptance.

- Build a local bare `file://` origin fixture entirely under one private temporary root, with real Git trees/refs, two fake worker stores and an owner store. Commit identities/times are fixed. Prove two-parent order for fast-forwardable and divergent clean candidates, exact single-ref push, ancestor idempotency, base not on target, WIP refusal, missing branch, policy hook rejection and unchanged task-ref outbox behavior.
- Coordinate same-target integrations with barriers at reservation, advertisement and receive update. Prove local serialization and independent-target progress. Move origin before advertisement, including to an ancestor of H, then after advertisement; the explicit lease must reject both. Re-observation distinguishes server `[remote rejected]` CAS failure from unchanged-target policy/auth/network. Assert exact lease argv, first-parent proof and one ref; no non-fast-forward update, unrestricted force, deletion or extra pushed ref.
- Conflict fixtures cover add/add, modify/delete, rename, symlink, executable, binary and multiple merge bases through standard porcelain. Assert task branch/HEAD H, MERGE_HEAD T, standard index stages, abort back to clean H and success CAS/reset to M. Prove the mirror-only clean path leaves workspace untouched until success. Plant hooks, fsmonitor, filter/merge/diff/signer definitions in workspace config/hooks; overrides must prevent every sentinel execution, including add/reset/abort. Check NUL-safe bounds, residual markers and deterministic commit. Host never executes sentinel project check/setup scripts.
- Attribute fixtures put committed `-merge` on a file changed on both sides: mirror merging must conflict. A directional `merge=union` case must produce the identical H/T tree in the bare candidate and workspace preparation. Assert frozen H source/side order, absent/empty info attributes, system/global isolation and unchanged T,H commit parents. Force a pre/post-verifier index mismatch: no launch/push or recomputed-tree adoption; stable tree-mismatch code.
- A fake agent emits controlled Done/Blocked/check lists, changes or leaves conflict files and signals completion. Prove same agent/worker/session, no import replacement, slots only during agent execution, publication bypass, no early outbox, shared follow-ups and read-only verify. Cover zero-source/zero-aux checks succeeding; nonempty-source/empty or not_run aux blocking; any source/aux fail/error blocking; tree/check invalidation on movement.
- Inject a crash after every durable step in Decision 9, including sidecar fsync/ref pin/workspace metadata/auxiliary prompt-CAS-enqueue-accept/commit/push/receipt/import/close/event/revoke boundaries. Restart from the same roots with injected actor liveness. Assert one source intent, one auxiliary ID, one target merge or ancestry success, stable budgets, cancellation fencing and GC retention. Unknown push outcomes must fetch before repetition.
- Decode new responses with copied baseline strict codecs: ordinary submit/status/result/host status/full records, DAG/follow-up bodies and envelopes. Test tolerant row/facts additions, unknown event kinds, integration annotation in Value events and omission for disabled tasks. New-discovery/old-execution must reject the safe selector without durable requests, and enabled submission/helper downgrade must return unavailable without ordinary fallback.
- Process wiring covers controller/direct owners, disabled close, continuation, DAG/Never/from-task, mutation fences and session pins. Drain barriers fence new phases/admissions; current work parks. An injected-clock drain/restart/undrain exceeds 10 minutes while paused, then uses only the saved admission remainder with the same auxiliary ID/queue position/follow-up spend. A real launch-valve test proves this at process handoff. Running-turn timeout stays active. Disable/restore preserves same-phase work; direct reconcile cannot adopt controller tasks.
- Compatibility fixtures roll back below task.integration with parked/blocked work, advance the injected clock past seven idle host-status days and apply baseline non-discard retention close/GC behavior. Restore: observe uncertain origin first; reachable M/H settles integrated/import while host stays Closed, otherwise workspace-missing with retained result refs. Assert no workspace recreation, turn admission or new push even on a clean mirror candidate. No host layout barrier or installation changes.
- Submit gate fixtures return an advertised local OID with ancestor/nonancestor bases, an absent OID, missing branch and failed network. Assert exact-branch `ls-remote` bounds, known pass/refusal versus unknown, and zero target fetch/transfer-ref effects before worker admission.
- UI/notifier fixtures prove automatic pending/success has no accept/attention card, blocked code/action appears, draft revisions survive updates, typed workflow states map to the five future columns, and event loss repairs/deduplicates from durable facts. Build embedded assets once after all source tracks.
- Facts fixtures keep the typed annotation <=512 bytes and complete facts <=2 KiB through both producer and decoder, including maximal display/branch values and overflow. Revision-only changes alter the enabled digest; companion confirmation must match. Two blocked epochs on the same source turn both notify, repeated same-epoch state does not; disabled fingerprint/wire omission stays baseline. T1 catalog/category/hint tests compare its usage-table rows exactly.

T1's handoff report contains an exact interface block for each consuming track T2–T5; their briefs paste those blocks. T6 has three serial checkpoints: (a) entry points/source/pin routing, (b) lifecycle/pause/stop recovery, (c) Git races/N-1 projections/legacy rollback settlement. Each ends with a commit and report section, reviewed by the orchestrator before the next checkpoint or handoff. Features remain unadvertised until (c) is accepted.

Use focused groups in the existing `host`, `task`, `controller`, `transfer`, `cli` and `dashboard` binaries, with `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4`; a zero-test filter is failure. The implementation integrator later owns formatting, both support-enabled and production Clippy/build graphs, targeted tests and the full landing gate per `docs/testing.md:36` and `docs/testing.md:44`. This docs-only phase must not run that suite.

The cheapest proposed live acceptance origin is **one throwaway private GitHub repository created by the owner**, initialized with `main`, a small known check and harmless fixtures. All workers already have the stated origin capability, but actual write access and policy are unverified here. No new service or real product branch is needed. **Owner decision Q1:** approve/create that repository and authorize a later acceptance track, including cleanup. Never test race/conflict/rejection cases against a production main branch.

Future authorized checklist:

1. Record accepted binary/build IDs, protocol 7 and controller/host features; confirm current session-transfer acceptance is complete first.
2. Configure the throwaway project and push a known committed base. With integration disabled, confirm today's behavior. Enable `main` and submit one clean task; observe one first-parent merge, exact trailers, no accept, and auto-close only after receipt/import.
3. Run two independent tasks on different minis against the same target, plus a controlled outside push. Confirm serialization, exact-T lease rebuild, no lost commits and no non-fast-forward updates.
4. Run a dependent pair; child waits for integrated parent and uses its imported accepted head. Block the parent, re-drive it, and confirm queued child resumes. Verify requested Never also unlocks after integration.
5. Create a conflict; confirm same worker/session resume, checks in the agent, exact parent order and bounded resolver outcomes. Verify a clean moved target with opt-in checks, a failing report and default no verify.
6. Exercise worker auth/network/policy rejection using this disposable origin only, missing target and base/WIP refusal. Restore cause and run `task integrate`; compare stable IDs/epochs.
7. Stop a controlled child at durable boundaries and confirm one merge after recovery. Drain/restart/undrain a queued auxiliary and confirm remaining admission time, same ID/position/allowance and no new phase while paused. Disable/re-enable and briefly roll back/restore a helper; document the seven-day legacy-GC exposure, whose expiry schedule is proved locally in T6. Race cancel/close/say with push; no update follows terminal acknowledgement. Record unknown-stop behavior honestly.
8. Submit from clean imported Codex and Claude sessions; check no recapture/replacement, package pins and session append remain correct. Test disabled-integration WIP session separately.
9. Compare JSON/text/dashboard facts, events/notifier, no success attention card, blocked owner action and old-peer unavailable behavior; use authorized disposable-profile notifications only.
10. Record actual commands, OIDs, first-parent/parent proofs and pending checks in validation. Remove the disposable repository only after the owner authorizes cleanup. Unperformed checks remain pending.

**Rejected:** real-pool experiments during this design phase, wall-clock sleep tests, real agent CLIs in automated fixtures, or calling a fake-origin pass live acceptance.

## Open questions for the owner

1. **Q1 — acceptance origin and authorization.** Approve the throwaway private GitHub repository and who creates/deletes it. Actual worker push permissions must be checked in the later authorized track.
2. **Q2 — project rollout.** Confirm which projects should receive `[task] integrate = "main"`. The proposed v1 has no laptop-wide default.

Check cost/allowance is decided: `verify_merge = "never"` by default; auxiliary turns share the task's existing `max_followups`, with the additional two-resolve/three-verify caps. Projects with no source checks may report none during recovery. These are implementation requirements, not an owner question.

The design preserves the explicit origin target, automatic behavior, two-parent merges, fast-forward-only updates, worker placement and protocol 7. D4 permits only an exact-T lease after parent proof. Effective host Never/owner close, bounded follow-ups, early aligned-parent gates and direct recovery limits remain. Risks are the auxiliary publication hook, frozen-attribute merge/abort/reset recovery, pause/stop accounting, compact facts sizing, session-pin audit, legacy helper workspace loss after seven idle days, and branch policies rejecting merges/unsigned identity. V1 accepts that rollback exposure without a layout bump.
