# Automatic Integration Implementation Plan

> **For agentic workers:** Use `superpowers:executing-plans` for an assigned implementation track. The orchestrator assigns parallel tracks and reviews their commits. This design-phase assignment authorizes documents only; it does not authorize implementation, internal subagent/reviewer rounds or pool access.

**Goal:** Automatically integrate each configured final Done task into its own origin target with one explicit merge, bounded same-session repair and durable owner-visible outcomes.

**Architecture:** Freeze integration policy beside existing task records, keep effective host close policy Never, and let the queue owner drive a detached integration child. The original worker performs controlled Git; resolve/verify are prepared pinned follow-ups with an integration-specific publication hook. Commit contracts/fakes first, implement four disjoint tracks, wire them serially, then document/build assets and perform separately authorized live acceptance.

**Tech stack:** Existing Rust/serde/UUID/sha2/RootedDir/ProcessRunner/SSH/Git; existing controller envelopes, scheduler/lease/supervisor; React/TypeScript/Vitest/Vite. No new service, Git hosting API dependency or agent-session format.

**Spec:** [2026-10-03-auto-integration-design.md](../specs/2026-10-03-auto-integration-design.md), baseline `67183a0b9f5926dc806c0ed96976e8ad39437ef0`. Read its Decisions 1–16 and session-transfer Round 2 before implementation.

## Global constraints

- Protocol 7; feature strings `task.integration` / `controller.integration`; advertise only after T6 acceptance. Missing/rolled-back helper means `INTEGRATION_UNAVAILABLE`, never ordinary fallback.
- Disabled policy preserves current behavior. Explicit own-origin target; no target discovery, no laptop-wide enabling default; verify policy defaults `never`.
- Exact two-parent merge: first parent fetched/advertised target T, second parent ordinary source H. Use only exact merge-OID single-ref non-forcing push, with the private trusted pre-push expected-T guard.
- WIP always refused; cycle base must be target ancestor on each candidate. Task-ref publish target collision is refused. No session package/base ref is sent to origin by integration.
- One Git driver per canonical origin/branch per owner; at most four drivers across targets. Release target reservation for auxiliary agent admission, backoff and block.
- Three candidates, two resolve turns, three verify turns per cycle. Auxiliary turns also consume existing `max_followups`; retries reuse IDs and do not reset that allowance.
- Retryable phase: initial try plus three retries at 2 s, 10 s, 30 s. Local/fetch/push Git deadline 60 s; host transport 90 s; helper lookup 5 s; output 64 KiB/stream. Auxiliary admission 10 min; auxiliary execution min(task timeout, 10 min), with inherited model/agent budgets.
- Host executes controlled Git and worker code only. No project setup/check/script, repository-defined hook/driver/filter/fsmonitor/signer or host-side verification command.
- New state/purpose remains in sidecars. Existing strict task/follow-up/DAG/status DTOs and ReviewState variants retain their emitted keys/values. Safe companion reads use an exclusive `task.list` integration selector before durable fallback.
- Public record <=4 KiB; private record <=64 KiB; prepared auxiliary <=256 KiB; RPC <1 MiB; three candidate manifests/five auxiliary intents/current cycle/eight archived receipt summaries maximum.
- Prompt <=16 KiB; <=256 conflict paths, <=1,024 bytes each, all must fit. No truncated-away conflicts. Message title <=120 bytes, summary <=1,024 bytes, whole message <=2 KiB.
- All new prose uses host redaction; structural UUID/OID fields are validated separately. No raw Git/credential stderr in public records or events. Journal hints <=1,024 bytes and never authoritative.
- Cancel/close/discard acknowledge terminal state only after host revocation or known committed outcome. Retain ambiguous/blocked workspace and pins against GC.
- Session import/paired source pins/complete receipt/first-turn gates remain intact. No recapture for auxiliary turns; no batch/DAG import inheritance.
- Tests follow `docs/testing.md:6` and its Writing tests section: existing area modules, test-support facade, isolated roots, fake agents, channels/hooks/injected time. Focused commands use `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4`; zero selected tests is failure.
- No implementation track pushes/merges/deploys to the pool without separate authorization. T7 alone regenerates embedded UI assets. The current docs phase edits only this plan, its spec and `.briefs/integ-spec-report.md`.

## Review focus

- A moved target that is an ancestor of H still rejects the stale first parent at advertisement; movement after advertisement fails the origin update (T2, T6).
- Resolve turn executes with unmerged index and read-only Git metadata, then finishes without an ordinary commit/outbox/close; host owns final tree/parent order (T2, T3).
- Cancellation races a lost push reply or an offline host: no acknowledged terminal task leaves a live push authority; committed results are never undone (T2, T3, T6).
- Integrating submit wraps a session package and fails at any source/pin boundary; retry recognizes nesting, never recaptures or leaks paired pins (T4, T6).
- Configured Never task remains Open after success: no attention card, next say bases on accepted head and DAG children proceed; an integration-blocked parent remains recoverable (T3, T5, T6).

---

## File ownership and dependency order

Existing anchors refer to the baseline; new paths are proposed. T1 owns shared roots only for the initial contract commit. Its ownership transfers afterward exactly as below. T2–T5 may run in parallel against T1 fakes. A contract correction is an orchestrator-owned serial commit; consumers stop while it lands. No sibling edits another sibling's files. T6 may lease an earlier file only after all sibling commits have been accepted and ownership explicitly transfers.

| Track | Size | Depends on | Exclusive files |
| --- | --- | --- | --- |
| T1 contracts/fakes | L | Approved implementation handoff | Create `src/integration.rs`, `src/integration/contracts.rs`, `src/integration/testing.rs`, `src/test_support/integration.rs`; seed component facades named below; modify module/export roots `src/lib.rs`, `src/test_support/mod.rs`, `src/controller/mod.rs`, `src/features.rs`, `src/transfer.rs`; add input-only integration fields/codecs in `src/task_client.rs`; register new test modules in `tests/{host,task,controller,transfer,cli,dashboard}/main.rs`; create `ui/src/lib/integration.contract.ts`, `ui/src/lib/integration.fixtures.json` |
| T2 worker Git/host | L | T1 | `src/integration/host.rs`, `src/integration/git.rs`, `src/integration/host_store.rs`, `src/integration/remote.rs`; modify `src/git_transport.rs`, `src/transfer.rs`, `src/turn.rs`, `src/task_store.rs`, `src/job_service.rs`, `src/gc.rs`; tests `tests/host/task_integration_host.rs`, `tests/transfer/task_integration_git.rs` |
| T3 owner lifecycle/DAG | L | T1 | `src/integration/store.rs`, `src/integration/coordinator.rs`, `src/integration/runner.rs`; modify `src/task_client.rs`, `src/turn_runner.rs`, `src/prepared_followup.rs`, `src/dag.rs`; tests `tests/task/task_integration_lifecycle.rs`, `tests/task/task_integration_turns.rs` |
| T4 policy/CLI/controller adapters | M | T1 | `src/integration/config.rs`, `src/controller/integration.rs`; modify `src/project_config.rs`, `src/cli.rs`; tests `tests/cli/task_integration_config.rs`, `tests/controller/controller_integration_contracts.rs` |
| T5 projections/notifier/dashboard | L | T1 | `src/integration/view.rs`; modify `src/task_view.rs`, `src/dashboard/{model,task,source,web}.rs`, `src/controller/events/{contracts,task_reads,notify}.rs`, `src/controller/events/notify/follow.rs`; UI `ui/src/lib/{api,attention,taskPresentation}.ts`, `ui/src/lib/api.test.ts`, new `ui/src/lib/{attention,taskPresentation}.test.ts`, `ui/src/components/{AttentionCards,TaskBadge,TaskTable}.tsx`, new `ui/src/components/{AttentionCards,TaskBadge,TaskTable}.test.tsx`, `ui/src/views/{TaskDetail,TaskDetail.test}.tsx`; tests `tests/dashboard/task_integration_view.rs`, `tests/controller/controller_integration_events.rs` |
| T6 serial process wiring | L | T2–T5 accepted | Modify `src/lib.rs`, `src/transfer.rs`, `src/features.rs`, `src/controller/{execute,read,batch_freeze,task_mutations}.rs`; tests `tests/controller/controller_integration_wiring.rs`, `tests/task/task_integration_direct.rs`, `tests/cli/cli_help.rs`, `tests/controller/controller_features.rs`; earlier files only by exclusive accepted-track lease |
| T7 docs/assets/local landing | M | T6 accepted | `README.md`, `docs/{usage,getting-started}.md`, `.claude/skills/pool-dispatch/SKILL.md`, three historical plans listed below; `src/dashboard/static/app/`; create `docs/superpowers/validation/2026-10-03-auto-integration.md` |
| T8 live acceptance | M | T7 + owner Q1/pool authorization | Update only the validation record; accepted code fixes return to a named exclusive owner |

Dependency order: **T1 → (T2 ∥ T3 ∥ T4 ∥ T5) → T6 → T7 → T8**. Exactly eight implementation tracks; four are parallel after the interface gate. No internal agent/reviewer dispatch is part of this design assignment.

T1 creates every listed new Rust component facade and test file. Facades expose contracts only; no fake production completion or advertised feature. Module roots are then frozen until T6. Existing implementation modules remain private; integration tests import through `mac_worker::test_support::integration`. A fake-only assertion does not satisfy real Git/process/RPC acceptance.

## T1 — shared contracts, wire fixtures and test seams

**Size:** L. **Produces:** a compiling, reviewed interface commit before parallel work. **Grounding:** feature grammar `src/features.rs:24`, strict TaskStatus `src/task.rs:1551`, Value annotations `src/controller/read.rs:142`, prepared identity `src/prepared_followup.rs:242`, facade rules `docs/testing.md:12`.

Contracts define `FrozenIntegrationPolicy`, `IntegrationOverride` (Inherit/Disabled/Target), `VerifyPolicy` (Never/MovedTarget), `IntegrationSnapshot`, private `IntegrationRecord`, `IntegrationCandidate` (frozen T/H/tree/M/message/identity/time), `IntegrationView` (optional snapshot/workflow/review/attention/requested-close), `IntegrationRevision` (monotonic sidecar revision), `IntegrationId` (UUID), `TargetKey` (validated origin/branch), `TargetReservation`, `IntegrationStep`, `HostIntegrationRequest/Response`, `PreparedIntegrationTurn`, `IntegrationTaskFacts`, `FrozenIntegratingSubmit/Batch`, and `IntegrationReadResult`. Fields/bounds/state/code catalog are copied from spec Decisions 2, 7, 9, 13, 15. Host action is a tagged Arm/Step/Read/Revoke enum; Step is Fetch/Prepare/AcceptTurn/Build/Push/Repair. Responses are typed Progress/NeedTurn/TargetMoved/Integrated/Blocked/Revoked, never free-form command text. Task facts contain the expected ordinary record, cycle base, current-import/continuation/runner/close/cancel proof and auxiliary-purpose binding.

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
    fn reach(&self, point: IntegrationHook);
}
pub trait IntegrationObserver: Send + Sync {
    fn facts(&self, task: TaskId) -> Result<IntegrationTaskFacts, WorkerError>;
}
```

`IntegrationCoordinator::new(state, host, turns, runtime, observer)` borrows these five traits. Its public entry points are `on_terminal(task: TaskId, source: TurnId) -> Result<(), WorkerError>`, `drive_once(task: TaskId) -> Result<IntegrationSnapshot, WorkerError>`, `redrive(task: TaskId, expected: IntegrationRevision) -> Result<IntegrationSnapshot, WorkerError>`, and `revoke(task: TaskId, expected: IntegrationRevision) -> Result<IntegrationSnapshot, WorkerError>`. T1 defines their input/output contracts in the facade; T3 supplies implementations. `IntegrationHook` enumerates each crash boundary in spec Decision 9 plus TargetReserved, BeforeAdvertisement, AfterAdvertisement, BeforePush, BeforeRevokeAck and AfterStateBeforeEvent. Runtime hooks are no-ops in production.

The shared `IntegrationFixture` has `new()` (one isolated root/fixed clock), `enable(task, branch)`, `complete_source(task)`, `drive(task)`, `load(task)`, `host_calls()`, `advance(Duration)`, `crash_at(IntegrationHook)`, `restart()`, `mark_actor_absent()`, `set_host_response(HostIntegrationResponse)`, and `enqueue_count(TurnId)`. `fixture.task()`/`fixture.source()` expose fixed validated IDs. These fixture APIs are implemented in T1, not assumed magically in later tests. `GitIntegrationFixture::new()` and real-origin schedules belong to T2's test files, so the shared fake never claims Git behavior.

- [ ] Write contract assertions for canonical deterministic ID/binding, every state/code, strict old DTO omission and public/private bounds. Seed meaningful contract tests in all registered modules, not empty files.
- [ ] Run the new contract selections in **cli/controller** and verify nonzero tests fail for missing validators, then implement only types/validators/fakes. Keep ordinary production behavior unchanged.
- [ ] Add input-only `integrate`/`verify_merge` fields in TaskSubmitRequest/BatchDefaults/BatchTask and the custom top-level batch Wire. Preserve disabled omission and flat/table exclusion. Freeze these definitions for T4; T3 retains exclusive task_client.rs ownership afterward.
- [ ] Define Rust/TypeScript shared fixture examples for disabled, pending, resolving, integrated/already-integrated, blocked, retry and stop-unconfirmed. Make tolerant optional row/facts metadata and strict wrapper/body codecs distinct. No existing TaskMeta/TaskStatus/PreparedFollowup/DagFrozenSpec key changes.
- [ ] Add feature constants without placing them in advertised registries; add private module/transport operation roots, test-support exports and all test declarations. Keep actual entry-point wiring for T6.
- [ ] Commit `feat(integration): define contracts and deterministic test seams`. Freeze signature/type/fixture names for T2–T5.

**Acceptance:** contracts compile through the private/facade split; all seeded contract tests select/pass; deterministic IDs and serialization bounds are asserted; no new feature advertised and no enabled production execution path.

## T2 — controlled host merge, guarded push and retention

**Size:** L. **Consumes:** T1 IntegrationHost request/response and runtime hooks. **Produces:** `HostIntegrationService::new(store: &HostStore, runner: &dyn ProcessRunner, runtime: &dyn IntegrationRuntime)` and `execute(&HostIntegrationRequest) -> Result<HostIntegrationResponse, WorkerError>`; `RemoteIntegrationHost` implements IntegrationHost using the existing transport; `IntegrationGit::push_candidate(policy, candidate)` uses exact-T guard. `candidate` is the validated T1 private candidate manifest, including T/H/tree/M/message/time. **Grounding:** current publisher `src/turn.rs:1242`, exact push `src/git_transport.rs:206`, worker helper config `src/git_transport.rs:237`, workspace close `src/task_store.rs:1373`, idle GC `src/gc.rs:570`.

The host track also owns `TaskStore::prepare_integration_resume` and `JobService::submit_integration_turn`, taking the T1 PreparedIntegrationTurn and existing TaskTurnRequest and returning existing TaskTurnResponse. Their only ordinary-validator exceptions are the durably planned HEAD/index and approved reduced timeout (spec Decision 7; baseline `src/task_store.rs:1630`, `src/job_service.rs:571`).

T2's `src/integration/git.rs` owns the new integration sender; T2 also exclusively owns the narrow `src/git_transport.rs` extension exposing the existing hermetic/credential request factory. Generic push_origin/outbox semantics stay unchanged. Siblings consume IntegrationHost, so they need no access to this internal factory.

- [ ] Create the real private bare-origin fixture, with methods `commit_base()`, `commit_task()`, `advance_target()`, `prepare()`, `push()`, `parents(oid)`, `origin_tip()`, `install_policy_rejection()` and deterministic advertised/update barriers. Fixed identities/times, isolated homes and file URLs only.
- [ ] Write the two-parent fast-forward case before implementation:

```rust
#[test]
fn fast_forwardable_task_still_gets_one_guarded_merge() {
    let mut f = GitIntegrationFixture::new();
    let target = f.commit_base();
    let task = f.commit_task();
    let merge = f.prepare();
    f.push();
    assert_eq!(f.parents(&merge), vec![target, task]);
    assert_eq!(f.origin_tip(), merge);
}
```

- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test transfer -E 'test(/^task_integration_git::/)'` red. Implement fetch/base/ancestor checks, scratch merge/tree/commit pin and message identity. Prove wrong target/WIP/missing branch rejection and already-integrated ancestry, not tree equivalence.
- [ ] Add a worker-owned pre-push script with fixed executable interpreter/framing and expected values passed as environment/argv, never interpolated shell source. It validates exactly one advertised update and old-T/new-M/target ref, emits only a fixed marker on mismatch. New sender sets private hooksPath, omits --no-verify, uses `--porcelain`, disables force and sends one exact OID. Preserve worker helper forwarding and environment stripping.
- [ ] Prove target movement to an ancestor of H before advertisement is refused despite FF eligibility; barrier an outside push after advertisement and prove server rejection/rebuild. Assert pushed ref set and argv contain no force, + refspec, delete, base or package ref. Cover SSH/HTTPS factory with fake ProcessRunner requests; do not contact either transport.
- [ ] Prepare/recover workspace metadata and unmerged index via host-owned scratch config/raw blob plumbing. Preserve symlinks/executable bits and same path. Install sentinel malicious workspace hooks/merge/filter/diff/fsmonitor/signing/check/setup definitions and assert none executes on any host step. Test binary/multiple-base/rename/delete cases and NUL-safe paths/prompt bounds.
- [ ] Implement the auxiliary publication hook in turn.rs: purpose-bound turns store structured outcome/checks, skip generic workspace commit/task-ref outbox/auto-close and leave accepted candidate state for host staging. Ordinary turns stay byte/behavior compatible. Require same expected candidate HEAD/index/binding on integration-turn launch.
- [ ] Implement the purpose-bound resume/admission adapter in task_store.rs/job_service.rs. Prove ordinary resume still refuses dirty/changed-HEAD workspaces and limit changes, while an exact integration manifest can launch against T with a reduced timeout. Lease, turn sequence, session/agent/model/profile/permissions remain strict; accepted repair finds the same purpose before any publisher action.
- [ ] Add revoke/push task fence and interruptible deadlines. Inject crashes at each host record/object/ref/index/HEAD/push/repair boundary. Resolve lost reply by fetching M/H ancestry; never repeat an ambiguous push blindly. Repair successful task ref/status/workspace to M or observed T while retaining source H receipt.
- [ ] Extend both GC inventory and deletion recheck to protect integration workspace/pins/receipts, including blocked/unknown-push state without a lease. Existing discard checks run only after confirmed revoke. Keep session-package GC unchanged.
- [ ] Run focused **host** `test(/^task_integration_host::/)` and **transfer** group green. Commit `feat(integration): add host merge execution and guarded origin push`.

**Acceptance:** real Git fixture proves parent order, expected-tip guard and crash idempotency; no host project code executes; auxiliary turn does not commit prematurely; GC/revoke fences protect necessary state; exact-OID transport remains hermetic and non-forcing.

## T3 — owner sidecars, continuation, auxiliary turns and DAGs

**Size:** L. **Consumes:** five T1 traits, PreparedIntegrationTurn wrapper and code/state catalog. **Produces:** `RootedIntegrationState::open(paths: &PathLayout, runtime: std::sync::Arc<dyn IntegrationRuntime>) -> Result<Self, WorkerError>`, IntegrationState implementation, IntegrationCoordinator methods declared in T1, `IntegrationRunner::run(task: TaskId) -> Result<IntegrationSnapshot, WorkerError>`, `PreparedIntegrationTurn::prepare(record, integration, purpose, attempt, ordinal)` with deterministic identity/binding. **Grounding:** terminal finalizer `src/turn_runner.rs:1632`, retirement `src/turn_runner.rs:2481`, continuation `src/task_client.rs:3772`, follow-up budget `src/prepared_followup.rs:80`, DAG gate/import `src/dag.rs:440`, `src/task_client.rs:5307`.

- [ ] Write final-source eligibility and disabled compatibility tests: no stage before exact import/retirement; pending continuation/close/rollback/cancel blocks stage; recovered finalizer converges to one intent; auxiliary Done never creates another source cycle.
- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test task -E 'test(/^task_integration_lifecycle::|^task_integration_turns::/)'` red. Implement rooted policy/record CAS, phase due index, actor reservation/liveness, epoch tombstones and deterministic budgets. No SSH or agent wait under task/queue/journal fences.
- [ ] Implement coordinator transitions against fake host/turn/observer traits first; seed phase-specific retry counters/tickets before effects. Reclaim only confirmed-dead actors, preserve ticket age, release target for aux/backoff/block and cap cross-target actors at four. Select a bounded page (32, matching active recovery precedent) without full-list reconcile per ID.
- [ ] Pin the prepared auxiliary follow-up to the task's worker/session. Persist sidecar purpose before the existing prompt/CAS/queue/handoff sequence. Keep PreparedFollowup wire keys unchanged and count auxiliaries against its ordinary allowance. Do not stage automatic continuation for integration NeedsInput; map it to integration block. Add timeout/admission clock tests.
- [ ] Copy source checks/summary/base into intent before auxiliary status overwrites latest checks. Require source fail/error block; auxiliary Done/all-pass nonempty evidence; verify clean moved-tree opt-in and no verifier edits. Re-fetch after auxiliary completion and invalidate evidence on new candidate.
- [ ] Implement revoke-aware close/cancel/say ordering before ordinary reconcile can launch enabled work. Stop ambiguity retains nonterminal state; already-committed cannot be labelled cancelled. From blocked say, retain diagnostics, restore H, then ordinary follow-up creates a new cycle. publish-retry stays isolated.
- [ ] Implement configured DAG gate: integrated Open/Never is Ready, pending/blocked parent waits reversibly, given-up parent blocks, from-task uses accepted imported M/T. Unconfigured Closed+Done rules remain unchanged. Add same-target validation seam for T4's batch freeze.
- [ ] Add a table-driven crash test using every T1 IntegrationHook. The fixture crashes, restarts with explicit actor absence, and checks the same integration/aux IDs, counters, reservation limits and no push after stop acknowledgement. Test multiple ready tasks during one resolve, direct owner recovery and independent target progress.
- [ ] Run focused task groups green; commit `feat(integration): drive durable lifecycle and bounded same-session recovery`.

**Acceptance:** final turn only, no accept step, both execution modes supported by the coordinator, deterministic retries/cancel barrier, shared follow-up accounting and correct reversible DAG state. Fake tests prove ownership logic; T6 must still prove real process attachment.

## T4 — freeze policy, CLI grammar and gated controller adapters

**Size:** M. **Consumes:** T1 input-only overrides and frozen policy/wrapper contracts; T3 coordinator via T1 facade/fake. **Produces:** `resolve_integration_policy(project, batch_default, task_override, verify_override, requested_close, origin, base_kind) -> Result<Option<FrozenIntegrationPolicy>, WorkerError>`; `prepare_integrating_submit/batch` (wrap immutable existing bodies and task-keyed policies); `serve_integration_read` (safe task.list selector), `prepare_integration_redrive` and `execute_integration_redrive` (durable mutation adapters). All input types are T1 contracts or existing ProjectSettings/FrozenSubmitBody/FrozenBatchBody; T1 provides factory fixtures for tests. **Grounding:** project strict settings `src/project_config.rs:149`, batch defaults exclusion `src/task_client.rs:894`, frozen submit `src/prepared_submit.rs:20`, controller Value prepared `src/controller/execute.rs:130`, safe read key rejection `src/controller/read.rs:462`.

- [ ] Write precedence/parser tests for absent/string/false, invalid/full branch refs, verify-without-target, explicit disable, top-level-versus-default-table exclusion and per-task override. Validate own origin/WIP/publish-target collision/from-parent target combinations before effects.
- [ ] Run `NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 cargo nextest run --locked --test cli -E 'test(/^task_integration_config::/)'` red. Implement optional project settings, pure resolution and CLI flags `--integrate`, `--no-integrate`, `--verify-merge` plus `task integrate <id>` and hidden host/child grammar. Actual routing remains for T6.
- [ ] Freeze requested Done/Never in policy and effective Never in existing submit/DAG body. Preserve default-disabled bytes. Preview prints effective per-task integration/verify. Freeze explicit preflight unknown separately from known target/base refusal; never call equality-only preflight ancestry proof.
- [ ] Implement new gated integrating mutation wrappers without widening old DTOs. Publish policy before ordinary task/admission effects. Request digest includes the wrapper and its complete task-keyed map; missing/extra/mismatched task policy is refused. Re-drive checks the expected revision and terminal-task prohibition, preserves target/source and increments epoch exactly once.
- [ ] Implement the read-only exclusive integration selector for <=16 validated IDs; its new result uses existing ControllerReadReply framing. Reject other selectors/ordinary filters together and return unavailable on feature rollback. Adapter performs no Git/network/agent work.
- [ ] Add copied baseline strict codec tests: ordinary status/result/host status/record/turn/DAG/follow-up and envelope remain readable; new policies travel only in gated bodies or tolerant fields. Test new-discovery/old-execution safe selector rejection has zero durable mutation rows.
- [ ] Add imported-session nested-body contract cases: source OID/package OID are unchanged, source-finished gate can locate the nested import, policy failure preserves/releases paired pins through existing lifecycle and never recaptures. T6 wires actual lib/stream paths; T4 tests adapter inputs with fakes.
- [ ] Run cli and **controller** `test(/^controller_integration_contracts::/)` green; commit `feat(integration): freeze opt-in policy and gated command contracts`.

**Acceptance:** exact override rules, explicit unavailable peers, byte-compatible disabled bodies, immutable enabled wrappers and safe read routing contracts. No laptop-wide default, implicit target, host-run check config or agent-session changes.

## T5 — shared projection, confirmed notifications and dashboard fields

**Size:** L. **Consumes:** T1 IntegrationSnapshot/IntegrationObserver and shared wire fixtures. **Produces:** `project_integration(snapshot: Option<&IntegrationSnapshot>, facts: &IntegrationTaskFacts) -> IntegrationView` containing optional snapshot, workflow state, existing review-state overlay, attention and requested-close policy; integration-aware TaskFacts digest/hints and notifier confirmation; UI types/actions. `IntegrationView` is defined in T1 with these exact fields. **Grounding:** existing Open review state `src/task_view.rs:103`, tolerant rows `src/task_view.rs:159`, baseline attention `ui/src/lib/attention.ts:7`, addressed facts `src/controller/events/contracts.rs:967`, unknown hints `src/controller/events/contracts.rs:475`.

- [ ] Write projection cases for disabled unchanged, pending/resolve/verify/retry/receipt, integrated Open/Never, blocked, dependency wait and terminal cancellation. Never invent new ReviewState variants or mutate actual source Done outcome into an integration error.
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
- [ ] Attach selected integration recovery to controller tick, normal/auxiliary finalizer and direct reconcile/wait. Prove no admission while continuation exists, imported result proof precedes merge, generic publisher never closes configured task early, integrated M/T imports before requested close or DAG binding, and requested Never next say uses M/T.
- [ ] Race cancel/close/discard/interrupted say with BeforePush, lost push response and offline host using channels. Terminal acknowledgement requires revoke/proof; already committed stays success. Preserve publish-retry's old outbox path and ordinary close/discard delivery fences.
- [ ] Add real concurrent owner/worker Git schedules: at most one same-target driver across RPC/leader/child/direct command, four independent-target cap, resolve releases branch and slot, returning resolver cannot starve ready work, budgets survive replacement. Repeat only distinct deterministic schedules, not timing loops.
- [ ] Exercise old/new peer matrix with copied `67183a0` strict decoders and old execution fakes. Explicit enabling rejects old controller/host, late rollback cannot launch normal fallback, safe extension reads create no request rows, disabled bytes/default close remain baseline, tolerant list/facts consume additions without new enums.
- [ ] Wire integration fact observer into actual dashboard/notifier paths and run UI integration fixtures once after accepted DTO changes. Run focused **task** `test(/^task_integration_direct::/)`, controller wiring, affected CLI help/features and existing session/DAG/close/publish-retry regressions. Select only relevant area groups.
- [ ] Only after all routes use concrete adapters, add feature constants to advertised registries and update sorted-feature expectations. Commit `feat(integration): wire controller and direct automatic integration`.

**Acceptance:** configured final Done integrates automatically in both modes, exact merge/receipt/import precedes auto-close, same-session repair and all current session gates survive, older helpers fail explicitly, and real process/target fences satisfy the spec. No production fake or unsupported enabled fallback.

## T7 — documentation reversal, one asset build and local landing evidence

**Size:** M. **Consumes:** accepted implementation and test evidence. **Produces:** operator docs, consistent embedded assets and validation record. **Grounding:** current manual-merging text `README.md:9`, `README.md:68`, `docs/getting-started.md:114`, `docs/getting-started.md:133`, `docs/getting-started.md:193`; historical non-goals below.

- [ ] Document `.worker.toml`/submit/batch precedence, exact own-origin target, default disabled, clean-verify default/cost, shared follow-up allowance, checks as agent claims, pending/blocked/success/already-integrated fields, re-drive and retained close, task-ref delivery separation and direct-mode recovery limitation. Explain protected-branch/signed-commit restrictions and unsafe-stop uncertainty with stable operator actions.
- [ ] Reverse the non-goal throughout README/getting-started/usage/pool-dispatch, including additional introductory/manual-review sentences found at the anchors above. Update historical plans with clearly dated supersession notes and link to the new design; preserve their historical tasks rather than pretending they originally implemented integration:
  - `docs/superpowers/plans/2026-09-10-flow.md:8` and `:42`;
  - `docs/superpowers/plans/2026-09-15-paper-ui-migration.md:18`;
  - `docs/superpowers/plans/2026-09-03-agent-task-execution-core.md:1508`.
- [ ] Update `.claude/skills/pool-dispatch/SKILL.md:164`: the skill uses configured automatic integration and observes/re-drives blocked tasks; it does not perform its own laptop Git merge/push. Manual close remains give-up/retained result, not automatic accept permission. Document session/WIP incompatibility and paired-pin retry behavior.
- [ ] Create validation with build/commit identity, commands/nonzero test counts, crash-point/old-peer/parent-order/guard proofs, local versus pending live status and risks. No real pool claim comes from fake fixtures.
- [ ] Run UI tests/lint/typecheck for the accepted source, then T7 alone builds/commits `src/dashboard/static/app/`; compare a temporary-outDir build of the full asset tree with committed assets. Record the exact production UI build used.
- [ ] Run `cargo fmt --all --check`, all-target Clippy and the production graph gates from `docs/testing.md:36`. After focused groups pass, run the required full landing gate once (`NEXTEST_TEST_THREADS=4 CARGO_BUILD_JOBS=4 MAC_WORKER_GATE_RAMDISK_MB=0 scripts/test-gate.sh`) in the **future implementation phase only**. This plan does not authorize that whole suite in the current docs assignment.
- [ ] Search changed operator/historical docs for stale unconditional manual-merge/non-goal claims, verify disabled compatibility wording and artifact parity. Commit `docs(integration): document automatic integration and rebuild dashboard assets`.

**Acceptance:** all named non-goals are explicitly superseded, current docs describe actual opt-in behavior and limits, assets match source, local gates are recorded, live checks remain pending until authorized.

## T8 — separately authorized disposable-origin live acceptance

**Size:** M. **Consumes:** T7 accepted release, owner-created throwaway private GitHub repository, named approved hosts and explicit deployment/acceptance authorization. **Produces:** completed or honestly pending validation checklist. This track is not executable during design; it must wait while the other session owns session-transfer pool acceptance.

- [ ] Confirm Q1 repository creation/write access/cleanup owner and Q2 project rollout. Record builds/protocol/features and complete session-transfer acceptance before this track starts. Use only disposable project branches for conflict/policy/crash cases.
- [ ] Follow the spec Decision 16 live checklist in order: disabled/clean merge/auto-close; concurrent tasks and outside push; DAG/Never/blocked re-drive; conflict/verify/check-fail/default-no-verify; auth/network/policy/missing/base/WIP; crash/stop races; clean imported Codex/Claude; JSON/dashboard/event/notifier/old-helper consistency.
- [ ] Record exact target/task/merge OIDs, first-parent history and parent ordering, stable IDs/epochs/counters, actual check reports and all unperformed checks. Confirm no host project command, forced target update or package/base origin ref occurred.
- [ ] Fix an acceptance failure through its exclusive track owner, rerun only affected local/live cases, then update validation. Do not redesign a binding decision silently.
- [ ] Commit validation `docs(integration): record disposable-origin live acceptance`. Delete the test origin only if separately authorized; otherwise leave its URL and cleanup status with the owner.

**Acceptance:** all required live checks have evidence or explicit pending/blocker status; real product main/pool session-transfer work was never used as an uncontrolled fixture.

## Coverage and handoff

| Spec decisions | Owning tracks |
| --- | --- |
| 1 reuse and baseline code map | T1, T6, T7 |
| 2 configuration/effective close/publish collision | T1, T4, T6 |
| 3 lifecycle and both modes | T3, T6 |
| 4 origin base gate | T2, T4, T6 |
| 5 merge/guarded exact push | T2, T6 |
| 6 host isolation/time/slots | T2, T3, T6 |
| 7 auxiliary purpose/prompt/budget/publication | T1, T2, T3, T6 |
| 8 check claims/verification binding | T2, T3 |
| 9 state/crash/GC | T1, T2, T3, T6 |
| 10 serialization/fairness/DAG | T3, T4, T6 |
| 11 mutations/canonical continuation head | T2, T3, T6 |
| 12 identity/message/redaction | T1, T2, T5 |
| 13 interfaces/N-1/projections/events/notifier | T1, T4, T5, T6 |
| 14 session transfer | T2, T4, T6 |
| 15 failure taxonomy | T1–T6; T7 operator catalog |
| 16 tests/disposable acceptance | T1–T8 |

Self-review verifies all D1–D13 bindings against this table, rejects placeholder APIs/zero-test filters, checks that T1 names match consumers and confirms every file has one owner during the parallel wave. The orchestrator reviews this plan and the spec before authorizing code. Current deliverables end at the committed documents and ignored `.briefs/` report; T8 requires a later owner decision and pool authorization.
