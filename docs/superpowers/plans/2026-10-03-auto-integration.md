# Automatic Integration Implementation Plan

> **For agentic workers:** Use `superpowers:executing-plans` for an assigned implementation track. The orchestrator assigns parallel tracks and reviews their commits. This design-phase assignment authorizes documents only; it does not authorize implementation, internal subagent/reviewer rounds or pool access.

**Goal:** Automatically integrate each configured final Done task into its own origin target with one explicit merge, bounded same-session repair and durable owner-visible outcomes.

**Architecture:** Freeze integration policy beside existing records, keep effective host close policy Never, and let the queue owner drive a detached child. The original worker uses hardened Git in its existing bare mirror/workspace; clean merges without auxiliary turns stay in the mirror until success and pushes use an exact-T lease. Resolve/verify are prepared pinned follow-ups with a publication hook. The controller drain valve parks phases/admissions durably. Commit contracts/fakes first, implement four disjoint tracks, wire serially, then document/build assets and perform separately authorized acceptance.

**Tech stack:** Existing Rust/serde/UUID/sha2/RootedDir/ProcessRunner/SSH/Git; existing controller envelopes, scheduler/lease/supervisor; React/TypeScript/Vitest/Vite. No new service, Git hosting API dependency or agent-session format.

**Spec:** [2026-10-03-auto-integration-design.md](../specs/2026-10-03-auto-integration-design.md), baseline `67183a0b9f5926dc806c0ed96976e8ad39437ef0`. Read its Decisions 1–16 and session-transfer Round 2 before implementation.

## Global constraints

- Protocol 7; feature strings `task.integration` / `controller.integration`; advertise only after T6 acceptance. Missing/rolled-back helper means `INTEGRATION_UNAVAILABLE`, never ordinary fallback.
- Disabled policy preserves current behavior. Explicit own-origin target; no target discovery, no laptop-wide enabling default; verify policy defaults `never`.
- Exact two-parent merge: first parent fetched target T, second parent source H. Assert T is M's first parent/ancestor before exact single-ref `push --porcelain --no-verify --force-with-lease=refs/heads/<branch>:<T> <origin> <M>:refs/heads/<branch>`. Only this explicit lease is allowed; never a non-fast-forward update, unrestricted force, implicit lease, + refspec, mirror, deletion or second ref. Re-observe after every failed push before classifying/rebuilding.
- WIP always refused; cycle base must be target ancestor on each candidate. Task-ref publish target collision is refused. No session package/base ref is sent to origin by integration.
- One Git driver per canonical origin/branch per owner; at most four across targets. Release target reservation for auxiliary admission, backoff, block and park.
- Controller drain pauses new drives, subsequent host phases and auxiliary admissions; current admitted phase/turn finishes then parks. Disable closes that gate before unloading; helper rollback retains parked intent/pins/budgets. Resume the saved phase only with an enabled, undrained compatible owner; no direct-mode adoption. Revoke/stop observation remains available.
- Three candidates, two resolve turns, three verify turns per cycle. Auxiliary turns also consume existing `max_followups`; retries reuse IDs and do not reset that allowance.
- Retryable phase: initial try plus three retries at 2 s, 10 s, 30 s. Local/fetch/push Git deadline 60 s; host transport 90 s; helper lookup 5 s; output 64 KiB/stream. Auxiliary admission 10 min; auxiliary execution min(task timeout, 10 min), with inherited model/agent budgets.
- Host uses existing hermetic Git plus per-command hooks/fsmonitor/signing/submodule and executable-driver overrides. No project setup/check/script or host verification command. No additional scratch repository, private index, raw staging or pre-push script. Clean path uses mirror `merge-tree --write-tree T H`; conflicts/verify use workspace `merge --no-ff --no-commit T`, HEAD H/MERGE_HEAD T. Success CAS-updates task ref H→M and resets clean; supersession aborts/restores H.
- Submit base gate uses exact-branch bounded `ls-remote` only: missing ref refuses, locally available advertised OID allows ancestry proof, absent objects/network failure means unknown. No submit target fetch or transfer ref; worker gate remains authoritative.
- Auxiliary check evidence depends on source: nonempty source checks require nonempty all-pass auxiliary checks; absent/not_run blocks. With no source checks, auxiliary checks may be empty. Any fail/error blocks in either case. Default verify is `never`; auxiliaries share `max_followups`.
- New state/purpose remains in sidecars. Existing strict task/follow-up/DAG/status DTOs and ReviewState variants retain their emitted keys/values. Safe companion reads use an exclusive `task.list` integration selector before durable fallback.
- Public record <=4 KiB; private record <=64 KiB; prepared auxiliary <=256 KiB; RPC <1 MiB; three candidate manifests/five auxiliary intents/current cycle/eight archived receipt summaries maximum.
- Prompt <=16 KiB; <=256 conflict paths, <=1,024 bytes each, all must fit. No truncated-away conflicts. Message title <=120 bytes, summary <=1,024 bytes, whole message <=2 KiB.
- All new prose uses host redaction; structural UUID/OID fields are validated separately. No raw Git/credential stderr in public records or events. Journal hints <=1,024 bytes and never authoritative.
- Cancel/close/discard acknowledge terminal state only after host revocation or known committed outcome. Retain ambiguous/blocked workspace and pins against GC.
- Session import/paired source pins/complete receipt/first-turn gates remain intact. No recapture for auxiliary turns; no batch/DAG import inheritance.
- Tests follow `docs/testing.md:6` and its Writing tests section: existing area modules, test-support facade, isolated roots, fake agents, channels/hooks/injected time. Focused commands use `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4`; zero selected tests is failure.
- No implementation track pushes/merges/deploys to the pool without separate authorization. T7 alone regenerates embedded UI assets. The current docs phase edits only this plan, its spec and `.briefs/integ-spec-report.md`.

## Review focus

- A moved target that is an ancestor of H rejects the exact-T lease at advertisement; movement after advertisement fails the old-OID ref transaction. Re-observation distinguishes CAS failure from policy (T2, T6).
- Resolve keeps ordinary branch/HEAD H checks, relaxing only clean porcelain to the recorded MERGE_HEAD T/index state. It finishes without an ordinary commit/outbox/close; host uses hardened add/write-tree/commit-tree with parents T,H (T2, T3).
- Drain races phase/admission permits; current work parks without another push/turn. Disable and rollback retain the same intent, budgets, pins and uncertain receipt for later resume (T3, T6).
- Cancellation races a lost push reply or an offline host: no acknowledged terminal task leaves a live push authority; committed results are never undone (T2, T3, T6).
- Integrating submit wraps a session package and fails at any source/pin boundary; retry recognizes nesting, never recaptures or leaks paired pins (T4, T6).
- Configured Never task remains Open after success: no attention card, next say bases on accepted head and DAG children proceed; an integration-blocked parent remains recoverable (T3, T5, T6).

---

## File ownership and dependency order

Existing anchors refer to the baseline; new paths are proposed. T1 owns shared roots only for the initial contract commit. Its ownership transfers afterward exactly as below. T2–T5 may run in parallel against T1 fakes. A contract correction is an orchestrator-owned serial commit; consumers stop while it lands. No sibling edits another sibling's files. T6 may lease an earlier file only after all sibling commits have been accepted and ownership explicitly transfers.

| Track | Size | Depends on | Exclusive files |
| --- | --- | --- | --- |
| T1 contracts/fakes | L | Approved implementation handoff | Create `src/integration.rs`, `src/integration/contracts.rs`, `src/integration/testing.rs`, `src/test_support/integration.rs`; seed component facades named below; modify module/export roots `src/lib.rs`, `src/test_support/mod.rs`, `src/controller/mod.rs`, `src/features.rs`, `src/transfer.rs`; add input-only integration fields/codecs in `src/task_client.rs`; register new test modules in `tests/{host,task,controller,transfer,cli,dashboard}/main.rs`; create `ui/src/lib/integration.contract.ts`, `ui/src/lib/integration.fixtures.json` |
| T2 worker Git/host | M | T1 | `src/integration/host.rs`, `src/integration/git.rs`, `src/integration/host_store.rs`, `src/integration/remote.rs`; modify `src/git_transport.rs`, `src/transfer.rs`, `src/turn.rs`, `src/task_store.rs`, `src/job_service.rs`, `src/gc.rs`; tests `tests/host/task_integration_host.rs`, `tests/transfer/task_integration_git.rs` |
| T3 owner lifecycle/DAG | L | T1 | `src/integration/store.rs`, `src/integration/coordinator.rs`, `src/integration/runner.rs`; modify `src/task_client.rs`, `src/turn_runner.rs`, `src/prepared_followup.rs`, `src/dag.rs`; tests `tests/task/task_integration_lifecycle.rs`, `tests/task/task_integration_turns.rs` |
| T4 policy/CLI/controller adapters | M | T1 | `src/integration/config.rs`, `src/controller/integration.rs`; modify `src/project_config.rs`, `src/cli.rs`; tests `tests/cli/task_integration_config.rs`, `tests/controller/controller_integration_contracts.rs` |
| T5 projections/notifier/dashboard | L | T1 | `src/integration/view.rs`; modify `src/task_view.rs`, `src/dashboard/{model,task,source,web}.rs`, `src/controller/events/{contracts,task_reads,notify}.rs`, `src/controller/events/notify/follow.rs`; UI `ui/src/lib/{api,attention,taskPresentation}.ts`, `ui/src/lib/api.test.ts`, new `ui/src/lib/{attention,taskPresentation}.test.ts`, `ui/src/components/{AttentionCards,TaskBadge,TaskTable}.tsx`, new `ui/src/components/{AttentionCards,TaskBadge,TaskTable}.test.tsx`, `ui/src/views/{TaskDetail,TaskDetail.test}.tsx`; tests `tests/dashboard/task_integration_view.rs`, `tests/controller/controller_integration_events.rs` |
| T6 serial process wiring | L | T2–T5 accepted | Modify `src/lib.rs`, `src/transfer.rs`, `src/features.rs`, `src/controller/{execute,read,batch_freeze,task_mutations,drain,control,init,runtime}.rs`; tests `tests/controller/controller_integration_wiring.rs`, `tests/task/task_integration_direct.rs`, `tests/cli/cli_help.rs`, `tests/controller/controller_features.rs`; earlier files only by exclusive accepted-track lease |
| T7 docs/assets/local landing | M | T6 accepted | `README.md`, `docs/{usage,getting-started}.md`, `.claude/skills/pool-dispatch/SKILL.md`, three historical plans listed below; `src/dashboard/static/app/`; create `docs/superpowers/validation/2026-10-03-auto-integration.md` |
| T8 live acceptance | M | T7 + owner Q1/pool authorization | Update only the validation record; accepted code fixes return to a named exclusive owner |

Dependency order: **T1 → (T2 ∥ T3 ∥ T4 ∥ T5) → T6 → T7 → T8**. Exactly eight implementation tracks; four are parallel after the interface gate. No internal agent/reviewer dispatch is part of this design assignment.

T1 creates every listed new Rust component facade and test file. Facades expose contracts only; no fake production completion or advertised feature. Module roots are then frozen until T6. Existing implementation modules remain private; integration tests import through `mac_worker::test_support::integration`. A fake-only assertion does not satisfy real Git/process/RPC acceptance.

## T1 — shared contracts, wire fixtures and test seams

**Size:** L. **Produces:** a compiling, reviewed interface commit before parallel work. **Grounding:** feature grammar `src/features.rs:24`, strict TaskStatus `src/task.rs:1551`, Value annotations `src/controller/read.rs:142`, prepared identity `src/prepared_followup.rs:242`, facade rules `docs/testing.md:12`.

Contracts define `FrozenIntegrationPolicy`, `IntegrationOverride` (Inherit/Disabled/Target), `VerifyPolicy` (Never/MovedTarget), `IntegrationSnapshot`, private `IntegrationRecord`, `IntegrationCandidate` (frozen T/H/tree/M/message/identity/time and clean-H merge manifest), `IntegrationView` (snapshot/workflow/review/attention/requested-close), `IntegrationRevision`, `IntegrationId`, `TargetKey`, `TargetReservation`, `IntegrationStep`, `HostIntegrationRequest/Response`, `PreparedIntegrationTurn`, `IntegrationTaskFacts`, `FrozenIntegratingSubmit/Batch`, and `IntegrationReadResult`. Add `IntegrationBasePreflight` (Pass/Unknown), `IntegrationPhaseKey` (task/intent/epoch/revision/phase), `IntegrationPhasePermit`, `IntegrationDriveAdmission` (Permit/Park) and `IntegrationPauseReason` (ControllerDrained/ControllerDisabled/HelperUnavailable). Record/snapshot carry parked resume state/reason and private remaining waits. Copy bounds/state/code catalog from Decisions 2, 7, 9, 13, 15. Host action is Arm/Step/Read/Revoke; Step is Fetch/Prepare/AcceptTurn/Build/Push/Repair. Responses remain typed Progress/NeedTurn/TargetMoved/Integrated/Blocked/Revoked. Task facts include ordinary record, cycle base, import/continuation/runner/stop proofs and auxiliary purpose.

```rust
pub trait IntegrationHost: Send + Sync {
    fn execute(&self, request: &HostIntegrationRequest)
        -> Result<HostIntegrationResponse, WorkerError>;
}
pub trait IntegrationState: Send + Sync {
    fn load(&self, task: TaskId) -> Result<Option<IntegrationRecord>, WorkerError>;
    fn publish_policy(&self, task: TaskId, policy: &FrozenIntegrationPolicy)
        -> Result<(), WorkerError>; // exact replay or conflict
    fn replace(&self, task: TaskId, expected: IntegrationRevision,
               next: &IntegrationRecord) -> Result<bool, WorkerError>;
    fn reserve(&self, key: &TargetKey, id: IntegrationId, epoch: u32,
               actor: ProcessIdentity) -> Result<Option<TargetReservation>, WorkerError>;
    fn release(&self, reservation: &TargetReservation) -> Result<(), WorkerError>;
    fn due(&self, now_millis: u64, limit: usize)
        -> Result<Vec<TaskId>, WorkerError>;
}
pub trait IntegrationTurns: Send + Sync {
    fn enqueue(&self, prepared: &PreparedIntegrationTurn) -> Result<TurnId, WorkerError>;
}
pub trait IntegrationRuntime: Send + Sync {
    fn now_millis(&self) -> u64;
    fn actor(&self) -> ProcessIdentity;
    fn actor_verdict(&self, actor: ProcessIdentity) -> RunnerLivenessVerdict;
    fn begin_phase(&self, key: &IntegrationPhaseKey)
        -> Result<IntegrationDriveAdmission, WorkerError>;
    fn reach(&self, point: IntegrationHook);
}
pub trait IntegrationObserver: Send + Sync {
    fn facts(&self, task: TaskId) -> Result<IntegrationTaskFacts, WorkerError>;
}
```

`IntegrationCoordinator::new(state, host, turns, runtime, observer)` borrows these five traits. Entry points remain `on_terminal(task: TaskId, source: TurnId) -> Result<(), WorkerError>`, `drive_once(task: TaskId) -> Result<IntegrationSnapshot, WorkerError>`, `redrive(task: TaskId, expected: IntegrationRevision) -> Result<IntegrationSnapshot, WorkerError>` and `revoke(task: TaskId, expected: IntegrationRevision) -> Result<IntegrationSnapshot, WorkerError>`. T1 defines contracts; T3 implements them. `begin_phase` supplies a short permit through durable phase ownership/handoff or auxiliary admission, dropped before SSH/Git/wait. Park saves the next phase without spending budgets. T6 binds this to the existing persisted controller drain valve and feature/owner state; direct tasks have an open local gate. Revoke/stop observation bypasses admission without authorizing a push. `IntegrationHook` covers Decision 9 plus TargetReserved, BeforePhasePermit, AfterPhaseAdmission, BeforeAuxAdmission, AfterPark, BeforeAdvertisement, AfterAdvertisement, BeforePush, BeforeRevokeAck and AfterStateBeforeEvent; production hooks are no-ops.

The shared `IntegrationFixture` has `new()` (isolated root/fixed clock), `enable(task, branch)`, `complete_source(task)`, `drive(task)`, `load(task)`, `host_calls()`, `advance(Duration)`, `crash_at(IntegrationHook)`, `restart()`, `mark_actor_absent()`, `set_host_response(HostIntegrationResponse)`, `set_drive_gate(Option<IntegrationPauseReason>)` and `enqueue_count(TurnId)`. `fixture.task()`/`fixture.source()` expose fixed IDs. T1 implements these APIs. T2 owns `GitIntegrationFixture::new()` and real-origin schedules; the shared fake never claims Git behavior.

- [ ] Write contract assertions for canonical deterministic ID/binding, every state/code, strict old DTO omission and public/private bounds. Seed meaningful contract tests in all registered modules, not empty files.
- [ ] Run the new contract selections in **cli/controller** and verify nonzero tests fail for missing validators, then implement only types/validators/fakes. Keep ordinary production behavior unchanged.
- [ ] Add input-only `integrate`/`verify_merge` fields in TaskSubmitRequest/BatchDefaults/BatchTask and the custom top-level batch Wire. Preserve disabled omission and flat/table exclusion. Freeze these definitions for T4; T3 retains exclusive task_client.rs ownership afterward.
- [ ] Define Rust/TypeScript fixtures for disabled, pending, resolving, parked with each reason/resume state, integrated/already-integrated, blocked, retry and stop-unconfirmed. Distinguish tolerant row/facts metadata from strict wrappers. No existing TaskMeta/TaskStatus/PreparedFollowup/DagFrozenSpec key changes.
- [ ] Add feature constants without placing them in advertised registries; add private module/transport operation roots, test-support exports and all test declarations. Keep actual entry-point wiring for T6.
- [ ] Commit `feat(integration): define contracts and deterministic test seams`. Freeze signature/type/fixture names for T2–T5.

**Acceptance:** contracts compile through the private/facade split; all seeded contract tests select/pass; deterministic IDs and serialization bounds are asserted; no new feature advertised and no enabled production execution path.

## T2 — hardened standard Git, exact-target lease and retention

**Size:** M; standard Git and the lease replace a second repository/index implementation and custom hook. **Consumes:** T1 IntegrationHost contracts/runtime hooks. **Produces:** `HostIntegrationService::new(store: &HostStore, runner: &dyn ProcessRunner, runtime: &dyn IntegrationRuntime)` and `execute(&HostIntegrationRequest) -> Result<HostIntegrationResponse, WorkerError>`; `RemoteIntegrationHost` implements IntegrationHost via existing transport; `IntegrationGit::push_candidate(policy, candidate)` proves first parent T and sends the exact-T lease. Candidate freezes T/H/tree/M/message/time. **Grounding:** publisher `src/turn.rs:1242`, exact push `src/git_transport.rs:206`, helper config `src/git_transport.rs:237`, workspace close `src/task_store.rs:1373`, idle GC `src/gc.rs:570`.

The host track owns `TaskStore::prepare_integration_resume` and `JobService::submit_integration_turn`, taking PreparedIntegrationTurn and existing TaskTurnRequest and returning TaskTurnResponse. Preserve ordinary task branch/HEAD/base H checks; only clean porcelain relaxes to candidate-bound MERGE_HEAD T/index state, plus approved reduced timeout (Decision 7; `src/task_store.rs:1630`, `src/task_store.rs:1637`, `src/job_service.rs:571`).

T2's `src/integration/git.rs` owns the new integration sender; T2 also exclusively owns the narrow `src/git_transport.rs` extension exposing the existing hermetic/credential request factory. Generic push_origin/outbox semantics stay unchanged. Siblings consume IntegrationHost, so they need no access to this internal factory.

- [ ] Create the real private bare-origin fixture, with methods `commit_base()`, `commit_task()`, `advance_target()`, `prepare()`, `push()`, `parents(oid)`, `origin_tip()`, `install_policy_rejection()` and deterministic advertised/update barriers. Fixed identities/times, isolated homes/file URLs and `receive.denyNonFastForwards=true`; prove an exact-lease descendant update is accepted by that policy.
- [ ] Write the two-parent fast-forward case before implementation:

```rust
#[test]
fn fast_forwardable_task_still_gets_one_merge_with_exact_lease() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    let task = f.commit_task();
    let merge = f.prepare();
    f.push();
    assert_eq!(f.parents(&merge), vec![target, task]);
    assert_eq!(f.origin_tip(), merge);
}
```

- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test transfer -E 'test(/^task_integration_git::/)'` red. Fetch target into a private mirror ref; implement base/ancestor gates, mirror `merge-tree --write-tree T H`, `commit-tree -p T -p H`, pin/message identity. A clean candidate needing no auxiliary leaves workspace unchanged until success. Prove wrong target/WIP/missing branch and ancestry idempotency.
- [ ] Add the integration-only sender: assert manifest/parents/ancestry, then exact `push --porcelain --no-verify --force-with-lease=refs/heads/<branch>:<T> <origin> <M>:refs/heads/<branch>` from mirror. Preserve credentials/environment; disable automatic extra tag/submodule refs. No script/private hooks path/fixed marker, no implicit lease, --force, + refspec, --mirror or deletion.
- [ ] Prove movement before advertisement, including to an ancestor of H, rejects despite FF eligibility; barrier an outside push after advertisement and prove server old-OID rejection/rebuild. Re-observe every failure with the same explicit credentials/environment: reachable M/H succeeds, changed T rebuilds, unchanged T permits auth/policy/network classification, missing T blocks. A server CAS `[remote rejected]` must not become policy. Assert exact lease argv/one ref; exercise SSH/HTTPS factories with fake requests only.
- [ ] Prepare conflicts/verify with hardened workspace `merge --no-ff --no-commit T`, keeping task branch/HEAD H and MERGE_HEAD T. Accept with hardened `add -A`, unmerged/marker checks, `write-tree` and `commit-tree -p T -p H`. Supersession/revoke aborts/restores clean H; success CAS-updates H→M then resets/status-repairs M. Use native Git for symlinks/modes/binaries/renames/deletes/multiple bases. No scratch repository/private index/raw staging.
- [ ] Apply per-command hooks/fsmonitor/signing/submodule/autostash/transport-ref overrides plus bounded named filter/merge overrides and no external diff/textconv (Decision 6). Plant executable definitions in workspace config/hooks and reference them in attributes; prove no sentinel executes through merge/add/abort/reset or any other host step. Project check/setup sentinels stay untouched. Cover NUL-safe names/prompt bounds and a clean verify tree matching the mirror candidate.
- [ ] Implement the auxiliary publication hook in turn.rs: purpose-bound turns store structured outcome/checks, skip generic workspace commit/task-ref outbox/auto-close and leave accepted candidate state for host staging. Ordinary turns stay byte/behavior compatible. Require same expected candidate HEAD/index/binding on integration-turn launch.
- [ ] Implement purpose-bound resume/admission in task_store.rs/job_service.rs. Ordinary resume still refuses dirty/changed-HEAD/limit changes. Integration accepts only branch/task HEAD/base H, recorded MERGE_HEAD T/index binding and reduced timeout; changed branch/head still refuses. Lease, sequence and session/agent/model/profile/permissions remain strict; accepted repair finds purpose before publication.
- [ ] Add revoke/push task fence and interruptible deadlines. Inject crashes at each host record/object/ref/index/HEAD/push/repair boundary. Resolve lost reply by fetching M/H ancestry; never repeat an ambiguous push blindly. Repair successful task ref/status/workspace to M or observed T while retaining source H receipt.
- [ ] Extend GC inventory/deletion recheck to retain workspace/merge state/pins/receipts in parked, blocked and unknown-push phases without a lease. Discard checks follow confirmed revoke. Keep session-package GC unchanged.
- [ ] Run focused **host** `test(/^task_integration_host::/)` and **transfer** group green. Commit `feat(integration): add host merges and exact-target lease pushes`.

**Acceptance:** real Git proves parent order, exact lease CAS races/re-observation and crash idempotency; mirror-only clean path and native conflict/verify/abort/reset work; no host project code/sentinel executes; auxiliary publication stays deferred; GC/revoke retains state; every target update is fast-forward.

## T3 — owner sidecars, continuation, auxiliary turns and DAGs

**Size:** L. **Consumes:** five T1 traits, PreparedIntegrationTurn wrapper and code/state catalog. **Produces:** `RootedIntegrationState::open(paths: &PathLayout, runtime: std::sync::Arc<dyn IntegrationRuntime>) -> Result<Self, WorkerError>`, IntegrationState implementation, IntegrationCoordinator methods declared in T1, `IntegrationRunner::run(task: TaskId) -> Result<IntegrationSnapshot, WorkerError>`, `PreparedIntegrationTurn::prepare(record, integration, purpose, attempt, ordinal)` with deterministic identity/binding. **Grounding:** terminal finalizer `src/turn_runner.rs:1632`, retirement `src/turn_runner.rs:2481`, continuation `src/task_client.rs:3772`, follow-up budget `src/prepared_followup.rs:80`, DAG gate/import `src/dag.rs:440`, `src/task_client.rs:5307`.

- [ ] Write final-source eligibility and disabled compatibility tests: no stage before exact import/retirement; pending continuation/close/submission rollback/cancel blocks stage; recovered finalizer converges to one intent; auxiliary Done never creates another source cycle.
- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test task -E 'test(/^task_integration_lifecycle::|^task_integration_turns::/)'` red. Implement rooted policy/record CAS, phase due index, actor reservation/liveness, epoch tombstones and deterministic budgets. No SSH or agent wait under task/queue/journal fences.
- [ ] Implement coordinator transitions against fakes first; seed phase retry counters/tickets before effects. Reclaim only confirmed-dead actors, preserve ticket age, release target for aux/backoff/block/park and cap actors at four. Select a bounded page (32, matching active recovery) without full-list reconcile per ID.
- [ ] Honor `begin_phase` at driver start, every host phase and auxiliary admission. Under its short permit publish phase ownership/handoff/admission, then drop before I/O. Park on drain/disable/helper rollback, preserving resume phase/reason, uncertain receipt, IDs/epoch/budgets/pins and remaining owner wait bounds. Current phase/turn finishes; it cannot chain into another phase. Resume observes a completed auxiliary or uncertain push first. Revoke remains possible. Test all gate points with fakes; T6 wires the real drain lock.
- [ ] Pin the prepared auxiliary follow-up to the task's worker/session. Persist sidecar purpose before the existing prompt/CAS/queue/handoff sequence. Keep PreparedFollowup wire keys unchanged and count auxiliaries against its ordinary allowance. Do not stage automatic continuation for integration NeedsInput; map it to integration block. Add timeout/admission clock tests.
- [ ] Preserve source checks/summary/base before auxiliary status overwrites them. Any source/aux fail/error blocks. Nonempty source checks require nonempty all-pass auxiliary evidence; empty/not_run blocks. With no source checks, allow empty auxiliary checks. Test both source cases and conditional not_run. Keep default verify never, moved-tree opt-in, no verifier edits and new-target evidence invalidation.
- [ ] Implement revoke-aware close/cancel/say ordering before ordinary reconcile can launch enabled work. Stop ambiguity retains nonterminal state; already-committed cannot be labelled cancelled. From blocked say, retain diagnostics, restore H, then ordinary follow-up creates a new cycle. publish-retry stays isolated.
- [ ] Implement configured DAG gate: integrated Open/Never is Ready, pending/parked/blocked parent waits reversibly, given-up parent blocks, from-task uses accepted imported M/T. Unconfigured Closed+Done rules remain unchanged. Add same-target validation seam for T4's batch freeze.
- [ ] Add table-driven crashes at every T1 hook. Restart with explicit actor absence; assert stable IDs/counters/reservations and no push after stop acknowledgement. Test drain before/after phase permit and aux admission, disable/rollback/re-enable, retained published/unknown push receipts, no timeout/budget spend while parked, fair same-phase resume and a parked parent gate. Cover ready tasks during resolve, direct recovery and independent targets.
- [ ] Run focused task groups green; commit `feat(integration): drive durable lifecycle and bounded same-session recovery`.

**Acceptance:** final turn only, no accept, both modes, deterministic retry/cancel barrier, shared follow-ups, source-dependent check rule, reversible DAG state and drain/disable/rollback parking. Fake tests prove ownership; T6 proves real process/valve attachment.

## T4 — freeze policy, CLI grammar and gated controller adapters

**Size:** M. **Consumes:** T1 overrides/policy/wrapper contracts; T3 coordinator via T1 facade/fake. **Produces:** `resolve_integration_policy(project, batch_default, task_override, verify_override, requested_close, origin, base_kind) -> Result<Option<FrozenIntegrationPolicy>, WorkerError>`; `preflight_integration_base(runner: &dyn ProcessRunner, origin: &str, branch: &BranchName, base: Option<&BaseOid>, local_repo: &RootedDir) -> Result<IntegrationBasePreflight, WorkerError>`; `prepare_integrating_submit/batch`; `serve_integration_read`; `prepare_integration_redrive` and `execute_integration_redrive`. Other inputs are T1 contracts or existing ProjectSettings/FrozenSubmitBody/FrozenBatchBody, with T1 factory fixtures. **Grounding:** project settings `src/project_config.rs:149`, batch defaults `src/task_client.rs:894`, frozen submit `src/prepared_submit.rs:20`, prepared Value `src/controller/execute.rs:130`, safe read rejection `src/controller/read.rs:462`, preflight bounds/request `src/git_transport.rs:49`, `src/git_transport.rs:1025`.

- [ ] Write precedence/parser tests for absent/string/false, invalid/full branch refs, verify-without-target, explicit disable, top-level-versus-default-table exclusion and per-task override. Validate own origin/WIP/publish-target collision/from-parent target combinations before effects.
- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test cli -E 'test(/^task_integration_config::/)'` red. Implement optional project settings, pure resolution and CLI flags `--integrate`, `--no-integrate`, `--verify-merge` plus `task integrate <id>` and hidden host/child grammar. Actual routing remains for T6.
- [ ] Freeze requested Done/Never in policy and effective Never in submit/DAG body. Preserve disabled bytes; preview prints effective target/verify. Implement cheap exact-branch `ls-remote` with existing preflight bounds, no target fetch or transfer ref. A missing branch refuses; an advertised OID available locally allows ancestry pass/base refusal; unavailable object/history, unresolved dependency base or failed network freezes Unknown for worker proof. Never substitute equality-only preflight. Fake requests assert zero fetch/ref effects for each case.
- [ ] Implement new gated integrating mutation wrappers without widening old DTOs. Publish policy before ordinary task/admission effects. Request digest includes the wrapper and its complete task-keyed map; missing/extra/mismatched task policy is refused. Re-drive checks the expected revision and terminal-task prohibition, preserves target/source and increments epoch exactly once.
- [ ] Implement the read-only exclusive integration selector for <=16 validated IDs; its new result uses existing ControllerReadReply framing. Reject other selectors/ordinary filters together and return unavailable on feature rollback. Adapter performs no Git/network/agent work.
- [ ] Add copied baseline strict codec tests: ordinary status/result/host status/record/turn/DAG/follow-up and envelope remain readable; new policies travel only in gated bodies or tolerant fields. Test new-discovery/old-execution safe selector rejection has zero durable mutation rows.
- [ ] Add imported-session nested-body contract cases: source OID/package OID are unchanged, source-finished gate can locate the nested import, policy failure preserves/releases paired pins through existing lifecycle and never recaptures. T6 wires actual lib/stream paths; T4 tests adapter inputs with fakes.
- [ ] Run cli and **controller** `test(/^controller_integration_contracts::/)` green; commit `feat(integration): freeze opt-in policy and gated command contracts`.

**Acceptance:** exact override rules, explicit unavailable peers, byte-compatible disabled bodies, immutable enabled wrappers and safe read routing contracts. No laptop-wide default, implicit target, host-run check config or agent-session changes.

## T5 — shared projection, confirmed notifications and dashboard fields

**Size:** L. **Consumes:** T1 IntegrationSnapshot/IntegrationObserver and shared wire fixtures. **Produces:** `project_integration(snapshot: Option<&IntegrationSnapshot>, facts: &IntegrationTaskFacts) -> IntegrationView` containing optional snapshot, workflow state, existing review-state overlay, attention and requested-close policy; integration-aware TaskFacts digest/hints and notifier confirmation; UI types/actions. `IntegrationView` is defined in T1 with these exact fields. **Grounding:** existing Open review state `src/task_view.rs:103`, tolerant rows `src/task_view.rs:159`, baseline attention `ui/src/lib/attention.ts:7`, addressed facts `src/controller/events/contracts.rs:967`, unknown hints `src/controller/events/contracts.rs:475`.

- [ ] Write projection cases for disabled, pending/resolve/verify/retry/receipt, parked reason/resume state, integrated Open/Never, blocked, dependency wait and terminal cancellation. No new ReviewState or mutation of source Done into error. Parked remains integrating with no false completion/blocked card.
- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test dashboard -E 'test(/^task_integration_view::/)'` red. Implement optional row/detail fields and `workflow_state` mapping to queued/running/integrating/needs_you/done. Projections consume snapshots, never reconcile or contact workers themselves.
- [ ] Extend tolerant TaskFacts annotations/digest; use conservative existing busy/quiescent fields during automatic work so an old notifier cannot announce an unconfirmed Done. Integration-blocked facts project Blocked/code; stored task result remains Done. Publish hints only after authoritative state and outside fences.
- [ ] Add kinds `task.integrating`, `task.integrated`, `task.integration_blocked` with bounded title-free data. Confirm snapshot revision before notification; dedup by integration ID/epoch/state, persist before display and repair lost hints. Use existing Done/Request sounds and no success attention card. Do not send notifications during automated tests.
- [ ] Test UI fixture `state=open, integration.state=integrated, workflow_state=done` returns false from needsAttention, while integration-blocked returns true. Keep legacy disabled Open tasks actionable. Render target/state/code, re-drive and close; pending has no accept. Preserve draft/expected revision handling.
- [ ] Add the revision-fenced dashboard re-drive route in web.rs/task.rs and matching api.ts call. It delegates through the T1 coordinator/controller adapter boundary; it never runs host Git inside the HTTP handler. Old-peer unavailable and stale-revision responses preserve the current card/draft.
- [ ] Test a dropped success/blocked hint and notifier restart using fake channel/clock. An unknown integration kind stays readable by baseline event decoder; no journal hint alone confirms state. Track failures and cleanup separately from successful target receipt.
- [ ] Run focused **controller** `test(/^controller_integration_events::/)` and dashboard group green; run UI tests for modified components/helpers plus TypeScript/lint. Do not regenerate embedded assets yet. Commit `feat(integration): project progress and confirmed owner attention`.

**Acceptance:** JSON/text/dashboard projection agrees; configured automatic work produces no review demand; blocked work is actionable; notifier confirms and deduplicates durable state; N-1 enum/unknown-event behavior remains compatible. No kanban implementation.

## T6 — serialize real entry-point wiring and compatibility acceptance

**Size:** L. **Consumes:** accepted T2 host/remote service, T3 rooted coordinator/runner, T4 policy/RPC adapters and T5 projections. **Produces:** real controller/direct routes, advertised features and integrated process evidence. **Grounding:** host prepare/resume `src/turn_runner.rs:1358`, leader selected tick `src/lib.rs:1475`, RPC safe route `src/controller/execute.rs:784`, retry source gate `src/lib.rs:1140`, controller package pin `src/controller/execute.rs:577`.

- [ ] Write process wiring tests with isolated owner/host stores and fake ProcessRunner/agent executables. Cover controller leader/RPC/runner versus direct terminal child/reconcile. Assert neither leader nor RPC runs a merge synchronously, and detached integration reservation recovery uses real re-exec entry points.
- [ ] Run focused **controller** `test(/^controller_integration_wiring::/)` red. Wire policy freeze/publication before task effects, feature/origin admission requirements, ordinary task prepare/import followed by host arm before launch, auxiliary special host command and detached integration child.
- [ ] Route safe integration selector before old list/durable fallback; keep strict status/result outer DTOs unchanged. Put status integration only into Value events, fetch companion for new result renderers and emit local JSON sibling integration. Wire requested-close projection rather than exposing host's effective policy as user intent.
- [ ] Adapt task.submit-integrating/batch-integrating source routing in batch freeze/execute and lib pin/source-finished helpers. Exercise every paired base/package pin failure/retry/release with real nested wrappers. Complete session receipt must preserve an appended fake native transcript; auxiliary resume never places again. Preserve session-import feature/version gates and no batch/DAG imports.
- [ ] Attach selected recovery to controller tick, normal/auxiliary finalizer and direct reconcile/wait. Wire exact-branch cheap submit preflight, proving zero target fetch/ref effects. No admission while continuation exists; import precedes merge; no early close; integrated M/T imports before close/DAG; Never next say uses M/T.
- [ ] Bind `begin_phase`/aux admission to controller drain.lock and ownership publication using short permits, never locks across Git/SSH. Drain acknowledgement fences new driver/phase/turn starts; current admitted phase/turn persists its outcome and parks. Wire controller disable to close the persisted gate before service unload; retain remote owner state with no direct adoption. Wire runtime shutdown/helper downgrade to save parked resume/uncertain effects. Test drain at both sides of each permit, push/resolve/verify/backoff/published phases, disable/leader restart and helper rollback/restore. Clear drain only explicitly; resume same ID/budgets and observe ambiguous pushes first. Reads/revoke remain available.
- [ ] Race cancel/close/discard/interrupted say with BeforePush, lost push response and offline host using channels. Terminal acknowledgement requires revoke/proof; already committed stays success. Preserve publish-retry's old outbox path and ordinary close/discard delivery fences.
- [ ] Add real concurrent owner/worker Git schedules: at most one same-target driver across RPC/leader/child/direct command, four independent-target cap, resolve releases branch and slot, returning resolver cannot starve ready work, budgets survive replacement. Repeat only distinct deterministic schedules, not timing loops.
- [ ] Exercise old/new peer matrix with copied baseline codecs/fakes. Enabling refuses old controller/host; late rollback parks an existing intent visibly as unavailable, preserving state and never launching fallback. Safe reads create no request rows; disabled bytes/close remain baseline; tolerant row/facts accept additions without new enums.
- [ ] Wire integration fact observer into actual dashboard/notifier paths and run UI integration fixtures once after accepted DTO changes. Run focused **task** `test(/^task_integration_direct::/)`, controller wiring, affected CLI help/features and existing session/DAG/close/publish-retry regressions. Select only relevant area groups.
- [ ] Only after all routes use concrete adapters, add feature constants to advertised registries and update sorted-feature expectations. Commit `feat(integration): wire controller and direct automatic integration`.

**Acceptance:** configured Done integrates in both modes; merge/receipt/import precedes close; same-session repair/session gates survive; old helpers refuse or park existing work visibly. Real process/target/drain fences satisfy the spec, with retained same-phase recovery after disable/rollback and no fallback.

## T7 — documentation reversal, one asset build and local landing evidence

**Size:** M. **Consumes:** accepted implementation and test evidence. **Produces:** operator docs, consistent embedded assets and validation record. **Grounding:** current manual-merging text `README.md:9`, `README.md:68`, `docs/getting-started.md:114`, `docs/getting-started.md:133`, `docs/getting-started.md:193`; historical non-goals below.

- [ ] Use the spec's "What the owner sees" narrative: configure once, submit batch, automatic per-task merges, integrated notice, same-session conflicts, blocked repair/re-drive or close. Document precedence/own-origin/default disabled; decided verify never/shared follow-ups; source-dependent auxiliary checks; pending/parked/blocked/success fields; drain/disable/rollback kill switch and explicit resume; task-ref separation/direct recovery; exact-T lease with fast-forward-only updates; branch/signing restrictions and stop uncertainty.
- [ ] Reverse the non-goal throughout README/getting-started/usage/pool-dispatch, including additional introductory/manual-review sentences found at the anchors above. Update historical plans with clearly dated supersession notes and link to the new design; preserve their historical tasks rather than pretending they originally implemented integration:
  - `docs/superpowers/plans/2026-09-10-flow.md:8` and `:42`;
  - `docs/superpowers/plans/2026-09-15-paper-ui-migration.md:18`;
  - `docs/superpowers/plans/2026-09-03-agent-task-execution-core.md:1508`.
- [ ] Update `.claude/skills/pool-dispatch/SKILL.md:164`: the skill uses configured automatic integration and observes/re-drives blocked tasks; it does not perform its own laptop Git merge/push. Manual close remains give-up/retained result, not automatic accept permission. Document session/WIP incompatibility and paired-pin retry behavior.
- [ ] Create validation with build/commit identity, nonzero test counts, crash/old-peer/parent-order/lease CAS/re-observation/park-resume proofs, local versus pending live status and risks. Fake fixtures do not establish pool behavior.
- [ ] Run UI tests/lint/typecheck for the accepted source, then T7 alone builds/commits `src/dashboard/static/app/`; compare a temporary-outDir build of the full asset tree with committed assets. Record the exact production UI build used.
- [ ] Run `cargo fmt --all --check`, all-target Clippy and the production graph gates from `docs/testing.md:36`. After focused groups pass, run the required full landing gate once (`NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 MAC_WORKER_GATE_RAMDISK_MB=0 scripts/test-gate.sh`) in the **future implementation phase only**. This plan does not authorize that whole suite in the current docs assignment.
- [ ] Search changed operator/historical docs for stale unconditional manual-merge/non-goal claims, verify disabled compatibility wording and artifact parity. Commit `docs(integration): document automatic integration and rebuild dashboard assets`.

**Acceptance:** all named non-goals are explicitly superseded, current docs describe actual opt-in behavior and limits, assets match source, local gates are recorded, live checks remain pending until authorized.

## T8 — separately authorized disposable-origin live acceptance

**Size:** M. **Consumes:** T7 accepted release, owner-created throwaway private GitHub repository, named approved hosts and explicit deployment/acceptance authorization. **Produces:** completed or honestly pending validation checklist. This track is not executable during design; it must wait while the other session owns session-transfer pool acceptance.

- [ ] Confirm Q1 repository creation/write access/cleanup owner and Q2 project rollout. Record builds/protocol/features and complete session-transfer acceptance before this track starts. Use only disposable project branches for conflict/policy/crash cases.
- [ ] Follow Decision 16's live checklist: disabled/clean merge/close; concurrent/outside pushes; DAG/Never/blocked re-drive; conflict/verify/check-fail/default-no-verify; auth/network/policy/missing/base/WIP; crash/stop/drain/disable/helper rollback-resume; imported Codex/Claude; JSON/dashboard/event/notifier/old-helper consistency. Include projects with zero checks.
- [ ] Record target/task/merge OIDs, parent order, exact lease, stable IDs/epochs/counters before/after park, reports and pending checks. Confirm no host project command, non-fast-forward update, unrestricted force or extra package/base/tag origin ref.
- [ ] Fix an acceptance failure through its exclusive track owner, rerun only affected local/live cases, then update validation. Do not redesign a binding decision silently.
- [ ] Commit validation `docs(integration): record disposable-origin live acceptance`. Delete the test origin only if separately authorized; otherwise leave its URL and cleanup status with the owner.

**Acceptance:** all required live checks have evidence or explicit pending/blocker status; real product main/pool session-transfer work was never used as an uncontrolled fixture.

## Coverage and handoff

| Spec decisions | Owning tracks |
| --- | --- |
| 1 reuse and baseline code map | T1, T6, T7 |
| 2 configuration/effective close/publish collision | T1, T4, T6 |
| 3 lifecycle/modes/drain/disable/rollback | T3, T6 |
| 4 origin base gate | T2, T4, T6 |
| 5 merge/exact-T lease/re-observation | T2, T6 |
| 6 hardened standard Git/time/slots | T2, T3, T6 |
| 7 auxiliary purpose/prompt/budget/publication | T1, T2, T3, T6 |
| 8 check claims/verification binding | T2, T3 |
| 9 state/parking/crash/GC | T1, T2, T3, T6 |
| 10 serialization/fairness/drain/DAG | T3, T4, T6 |
| 11 mutations/canonical continuation head | T2, T3, T6 |
| 12 identity/message/redaction | T1, T2, T5 |
| 13 interfaces/N-1/projections/events/notifier | T1, T4, T5, T6 |
| 14 session transfer | T2, T4, T6 |
| 15 failure taxonomy | T1–T6; T7 operator catalog |
| 16 tests/disposable acceptance | T1–T8 |

Self-review verifies all D1–D13 bindings against this table, rejects placeholder APIs/zero-test filters, checks that T1 names match consumers and confirms every file has one owner during the parallel wave. The orchestrator reviews this plan and the spec before authorizing code. Current deliverables end at the committed documents and ignored `.briefs/` report; T8 requires a later owner decision and pool authorization.
