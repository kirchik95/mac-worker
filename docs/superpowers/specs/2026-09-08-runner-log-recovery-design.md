# Runner log recovery and completion

Approved intent: stage 3.2 of the pool reliability roadmap, confirmed by the user on 2026-09-08. Base: `f29cf81`.

## Required behavior

The task runner drains both native streams, including terminal tails exceeding 64 KiB. Restart resumes at committed per-stream offsets without losing or repeating bytes. `task logs -f` follows one selected turn and exits after its native logs and publication are complete, even if the task remains Open or a later turn is active.

Existing mechanisms: `TerminalLogDrain` and `LogCursor` in `src/job.rs`, terminal job status through `RemoteJobClient::status`, and owner-only append/CAS replacement through `RootedDir`. No remote protocol change or new dependency is required.

## Local journal

Use a private, versioned per-turn sidecar next to `runners/<task>/<turn>.log`. It binds task and turn IDs and holds committed stdout/stderr offsets, total mixed-log length, an accepted-event flag, and optional completion with final outcome and whether the turn was drained or conclusively never started. One optional pending append contains at most 64 KiB of decoded bytes and its next committed state. Bound serialized state to 128 KiB. The writer holds a nonblocking exclusive flock and validates the log's rooted binding. Do not expose its writable descriptor.

Transaction: durably publish pending intent; verify the existing log suffix equals the already-written prefix of that intent; append only missing bytes; sync the log and revalidate its binding; atomically publish committed state. Without an intent actual length must equal committed length. Unexpected contents, lengths, identities, permissions or versions fail without altering log bytes. An error after an atomic replacement requires reloading state before any further write. Never truncate or rewrite the visible log. Empty EOF reads need no journal update.

All native bytes, runner diagnostics and lifecycle events use this writer. Accepted-event identity and completion are committed with their event bytes, preventing replay duplicates. Completion is monotonic; identical repeated completion is a no-op.

## Terminal drain and lifecycle

Always run the drain after host acceptance, including immediate terminal submission responses and prompt-free restart. Restore `LogCursor`s from the journal. Bind terminal job status and its final byte lengths, drain both streams through confirmed EOF, then re-fetch and revalidate exactly that terminal job status. Validate the remote job against selected turn, worker, client, project and worktree. A task's Open/Closed state alone never proves the stream drain has finished. Validate a cloned drain before committing bytes and advance its live cursor only after commit. Sleep when no progress is made; consume available backlog without a sleep per chunk.

Finish ordering: complete native drain; fetch/import or establish publication failure; durably store final local status/fetched head while retaining ownership; journal final event and completion together; idempotently release base; clear runner and retire queue/prompt. A restarted completed writer skips remote execution/publication and retries only remaining cleanup.

Reconciliation must preserve/restart incomplete accepted turns even when task state is Open, Closed, Abandoned or Lost, and must release the base before deleting a completed recovery row. Accepted cancellation retains recovery ownership until drain/publication completion. Only proven preacceptance cancellation/failure can record a local never-started completion. `say` must reject a new turn while the previous one is still finalizing. Preserve pins, capacity limits, live/ambiguous owner checks, saved project contexts, and submission rollback fencing.

## Reader and compatibility

Read-only CLI reads checkpoint before bytes, exposes only committed bytes, flushes the final partial line on the selected turn's completion, and uses that completion's outcome so a later remote refresh cannot erase publication failure. Retry safe transient ESTALE from concurrent atomic replacement; do not create, repair or spawn from reads. Preserve exact raw bytes and existing adapter formatting. Historical selected turns finish independently of newer task state.

Legacy logs remain readable with non-follow commands. A nonempty log without a sidecar has ambiguous native offsets: writer recovery returns `LOG_CHECKPOINT_MISSING` and preserves all bytes. Follow without provable completion reports `LOG_COMPLETION_UNKNOWN`. No offsets are inferred from UUID ordering, log length, agent-supplied JSON, task state, fetched head or missing queue rows. Empty logs can initialize a fresh checkpoint; conclusively never-started tasks can finalize locally. Automatic legacy replay/migration is outside this change.

## Validation

Use real private local state/files and real local Git with fixture remote agents. Verify multi-chunk terminal tails, immediate terminal submission, restarts including a committed stdout prefix with stderr still at zero, pending intent before/partial/complete append, malformed state and path substitution, concurrent writer rejection, stale final lengths/identity, cancellation, finalization after Open/Closed, cleanup retry, selected historical follow and unchanged raw output. Keep existing admission, queue/context and submission recovery regressions passing. Run fmt, all-target Clippy, relevant suites and one full Rust suite. No real fleet, provider agent, push or deployment is part of local verification.
