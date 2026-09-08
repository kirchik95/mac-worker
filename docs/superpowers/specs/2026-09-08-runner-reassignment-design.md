# Waiting runner reassignment

The client limits detached task runners by the configured worker count. Three runners pinned to one busy Mac can consume that budget while a parked task could run on another Mac. Finishing one turn is currently the only runner-driven way to wake a parked task.

## Decision

A detached runner that cannot claim its waiting turn may transfer its existing process to a committed parked turn that can run now. The queue transition parks the donor and directly reserves the selected worker for the recipient under the existing QueueLock. No additional process is started by this transition. Attached execution continues to follow its requested task.

Increasing the runner count only moves the starvation threshold. Spawning a replacement process during every yield would require another startup-permit protocol and predecessor-exit fencing. Reusing the existing PID keeps the transition within the existing process-identity and queue contracts.

The existing concurrent submit/reconcile count → spawn → adopt sequences still use separate operations. This change adds no process at the reassignment boundary; a strict global startup permit for simultaneous clients remains a separate reliability improvement.

## Contracts

1. Only the exact owner of a Waiting TaskTurn may donate. A dispatching or accepted turn cannot donate. Task IDs, turn IDs, queue IDs, prompts, run membership and pins remain unchanged.
2. A pinned runner first attempts its own worker. After a miss, it observes the remaining fleet only when parked work exists, retaining observations already collected in that iteration. Reassignment reuses SchedulerPolicy for worker readiness/resources/capabilities, QueueEntry eligibility for pins, and run_has_capacity plus queue reservations under QueueLock. An eligible donor keeps its turn unless an older eligible task has priority.
3. Older runnable parked turns participate in per-worker FIFO barriers using the same context eligibility as reassignment. Pending submission/rollback intents, terminal tasks and legacy turns whose context the caller cannot supply are not runnable barriers. A younger incompatible task may still proceed on a different worker.
4. If the recipient inherits legacy context, publish it before the queue transition while retaining the state lock. In one queue publication: donor Waiting → Parked; recipient Parked → Waiting → Dispatching with the same ProcessIdentity and a selected worker. Then update recipient runner metadata and clear donor metadata while retaining the state lock. Count unique live process identities rather than duplicate metadata rows during a partial update.
5. If publication or metadata replacement fails, preserve durable rows. The process exits with an error and normal cooperative reconciliation can recover its dead ownership. A live queue owner with missing runner metadata must not trigger a duplicate replacement process.
6. Store the originating worktree path in owner-only `turns/<task_id>/project.json`, bound to task/project/worktree/repository IDs. Public task and queue JSON retain opaque IDs. Rooted, bounded, canonical reads reject redirected or mismatched context. Unix path bytes are preserved as base64. Existing submission rollback removes this directory; ordinary context retention follows local task/turn retention.
7. Project execution and result import resolve the saved worktree independently of the runner's original cwd. Existing records without context retain the current-directory fallback for their own execution. A legacy recipient may inherit only a saved donor context from the same worktree; a contextless donor may serve recipients with their own saved context. Raw cwd is never made durable by queue reassignment. Legacy rows that cannot be reassigned retain the previous completion/reconciliation startup path.
8. Hidden detached execution opts into reassignment explicitly. The existing attached/library `run` contract still executes only its requested task.

## Files and interfaces

- `src/client_state/task_context.rs`: `write_task_project_path(&self, record: &LocalTaskRecord, project_path: &Path) -> Result<(), WorkerError>` and `task_project_path(&self, record: &LocalTaskRecord) -> Result<Option<PathBuf>, WorkerError>`. The immutable read is usable while QueueLock is held.
- `src/client_state/runner_dispatch.rs`: `claim_parked_for_waiting_runner(&self, task_id: TaskId, turn_id: TurnId, owner: ProcessIdentity, observations: &[CandidateObservation], now_millis: u64) -> Result<Option<(TaskId, QueueClaim)>, WorkerError>` and lock-internal task/FIFO helpers.
- `src/client_state.rs`: register the modules and extend claim_next FIFO checks to runnable parked turns.
- `src/turn_runner.rs`: explicit detached mode, donor/recipient claim result, full-fleet observations for reassignment, saved project context for execution/cleanup.
- `src/task_client.rs`: persist context during rollback-protected submission, resolve per-task paths, count unique process identities, avoid duplicate startup while another live queue owner is handing off.
- `src/lib.rs`: route the hidden runner command to detached mode.

## Acceptance

- Saturated pool: active mini-1 turn plus two mini-1 waiters; parked mini-2 task runs before mini-1 finishes using a donor PID.
- A displaced older task regains priority when its worker becomes available.
- Incomplete submission heads, incompatible workers, occupied reservations and exhausted run caps do not admit the recipient or block an unrelated eligible task.
- Concurrent donors select a recipient once; stale owners and cancellation cannot move another owner's work.
- A failed post-publication update retains recoverable ownership and cannot trigger a duplicate live runner.
- Cross-project and linked-worktree execution/import use the recipient's recorded context.
- Attached execution does not change tasks. Existing transfer-lock and admission-isolation regressions remain green.
