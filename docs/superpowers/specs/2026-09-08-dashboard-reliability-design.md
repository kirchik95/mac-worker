# Dashboard reliability design

Approved intent: stage 4 of the pool reliability roadmap on base
`2d1a6c441303758434fcb73a86d5f6999f9e5c97`. This stage repairs the existing
React dashboard and its delivery checks. It is not a dashboard redesign and
does not add a task, settings, question, or log wire format.

## Decision

Deliver four separately reviewable changes in this order:

1. align the Settings production client with the protected Axum route;
2. drain all currently available blocks of a completed turn log without losing
   live text or split UTF-8;
3. recover task detail after transient errors and load questions for every
   waiting task under a six-request concurrency bound;
4. add pull-request Rust/UI checks, regenerate embedded UI assets once, and
   compare a fresh production build with the complete embedded asset tree.

The first three changes are source-and-test commits. They do not regenerate the
checked-in UI bundle. The fourth change consumes the reviewed source, performs
the single asset regeneration, and adds the parity gate. A final validation
agent runs the complete frozen gate once.

## Existing boundaries and global invariants

- The dashboard remains bound to its existing loopback listener. Preserve exact
  Host matching, no CORS, the current CSP and security headers, text-only remote
  rendering, bounded request bodies, and bounded log chunks.
- Preserve the Settings mutation boundary already implemented in
  `src/dashboard/web.rs`: exact same-origin browser `Origin`,
  `Content-Type: application/json`, `X-Mac-Worker-Settings: 1`, an 8,192-byte
  maximum body, typed validation, configured-worker lookup, and source calls
  only after validation. Browser code must not set or spoof `Origin`; the user
  agent supplies it for a same-origin POST.
- Preserve the existing Settings wire shapes. GET returns
  `AgentSettings { agents: AgentSetting[] }`; POST consumes `SaveSettings` and
  returns one fresh `AgentSetting`. The submitted revision remains the
  optimistic native-source revision. Existing native settings code must keep
  its pre-lock and post-lock source revision checks, atomic write behavior,
  unrelated fields/comments, and peer agent files.
- Preserve the task-detail route and `fetchTaskDetail(taskId, signal)`. Keep the
  two-second detail cadence and task-ID cancellation. Broad poll deduplication,
  direct task lookup, background observation, and other performance changes are
  stage 5.
- Preserve the task log route and `LogChunk` fields: `stream`, `offset`,
  `next_offset`, and Base64 `data`. Chunk limit remains 65,536 decoded bytes.
  Stdout and stderr have independent hook instances, offsets, decoders, and
  text. Live cadence remains 1,000 ms. No EOF flag, final byte count, combined
  stream, SSE, or WebSocket is introduced.
- Questions remain in task detail, not the snapshot. Six limits concurrent
  detail requests, never the number of task IDs that can be read. No question
  answer mutation or new question retry protocol is part of this stage.
- Tests use local fixtures and fake sources only. Do not contact the live fleet,
  execute a real provider task, push, deploy, or modify credentials.
- Implementation is sequential, one Cursor implementation agent at a time,
  with no nested agents and no concurrent Cargo processes. All local Cargo
  commands use `/private/tmp/mac-worker-dashboard-stage4-target`.

## Settings client contract

### Current behavior

`saveAgentSettings` in `ui/src/lib/api.ts` sends JSON and the revision but omits
`X-Mac-Worker-Settings: 1`. `request_has_settings_headers` in
`src/dashboard/web.rs` therefore rejects the shipped request before
`DashboardSettingsSource::save`.

The same helper currently declares the POST response as `AgentSettings`, while
`DashboardSettingsSource::save` and the route return one
`AgentDefaultSettings`/`AgentSetting`. Current component tests hide this by
returning a list wrapper from mocked POSTs. If only the header were repaired,
the selected worker's list state would be replaced by a singular object.

`Settings.Detail` also permits a stale asynchronous save completion to escape
its old selection. Changing worker clears the parent settings and unmounts the
old detail, but unmounting alone does not cancel the already-issued promise.
Its eventual `onSaved` can replace the newly selected worker's settings and
draft. Selecting another agent has the same identity risk.

### Required behavior

The production helper sends:

```ts
headers: {
  'content-type': 'application/json',
  'x-mac-worker-settings': '1',
}
```

It does not send `Origin`. Its request body remains:

```ts
interface SaveSettings {
  agent: string
  model: string | null
  effort: string | null
  fast: boolean | null
  revision: string
}
```

`saveAgentSettings(worker, body)` returns `Promise<AgentSetting>`. The Settings
view replaces only the matching entry in its current `agents` array. Other
agents remain unchanged and the returned fresh revision becomes the next save
revision.

`Detail` owns a save-generation ref. Starting a save captures the current
generation; changing `(worker, setting.agent)` or unmounting invalidates it.
After either the fulfilled or rejected promise, the component updates parent
settings, message, and busy state only if the generation is still current.
Merely adding a React `key` is insufficient because it does not cancel the old
promise callback.

The regression boundary is deliberately paired: production-client tests inspect
the exact Fetch request and consume a singular production-shaped response;
real Axum route tests send the same header/body shape and assert the singular
response plus all existing rejection behavior. There is no claim that one test
executes a browser helper against the Rust fixture process.

## Turn-log lifecycle

### Current behavior

`useTurnLog` treats `live` as part of log identity. A `true → false` change
clears text, offset, and decoder. A completed hook performs one request because
only live hooks install an interval, so a completed stream is truncated to the
first 65,536-byte block. Decoder flush occurs in effect cleanup, where it can
race with the reset for a new selection.

### Required behavior

Log identity is exactly `(taskId, turnId, stream)`. Only identity change resets
text, offset, and error. A `live` change preserves those values. A completed
no-progress read finalizes the decoder once for that stopped reading period; if
the same identity later becomes live, clear finalization and create a fresh
UTF-8 decoder at the preserved offset before reading again.

Use completion-based scheduling rather than `setInterval`, so each hook has at
most one request in flight:

- while live, perform one bounded read and schedule the next attempt 1,000 ms
  after that attempt settles;
- when completed, consume advancing chunks sequentially without a delay;
- an advancing completed read immediately requests the next offset;
- the first successful no-progress response stops the completion drain and
  flushes that identity's `TextDecoder` exactly once for that stopped reading
  period;
- a finalized same-identity reader becoming live preserves text and offset,
  restores a usable decoder, and resumes polling;
- a completed read error preserves bytes and decoder state, records the error,
  and retries after 1,000 ms without requiring remount;
- every successful response clears the prior read error, including an empty
  no-progress response;
- cleanup aborts the active request, clears a pending retry timer, and stops
  dequeuing. Cleanup never flushes a decoder into React state. Identity change
  discards the old decoder; only the same identity's successful completed
  no-progress path may flush it.

No-progress means `next_offset === requested offset`. It must never trigger an
immediate retry loop. Responses continue to be decoded with `{ stream: true }`
until the completed no-progress boundary, preserving a scalar split across
chunks.

### Log-finality contract finding

There is a real proof gap at the dashboard API boundary. `DashboardLogChunk`
exposes no terminal byte target or EOF bit, and `TaskTurnProjection` exposes no
final stdout/stderr lengths. The established runner finality mechanism
(`TerminalLogDrain`/`LogCursor`) requires both a terminal job's immutable final
byte target and an empty read at that exact target. The dashboard hook has only
a separately observed `ended_at_millis` and an empty chunk. Consequently, a
successful no-progress response proves only that no bytes were returned at that
offset by that request; the client cannot itself prove that the offset equals
the terminal job's declared final length.

Normal production ordering is stronger than the exposed dashboard contract:
the supervisor stops and syncs output, publishes terminal job status with final
lengths, then publishes the ended task turn. That makes late growth after a
correct ended turn unexpected. It does not let this client validate the final
target, and malformed or prematurely empty responses remain indistinguishable
from confirmed EOF. Stage 4 therefore improves multi-block behavior and calls
the stopping point “read to the current end,” not “confirmed final EOF.” The
existing panel statement “nothing more will arrive” must be replaced with
honest wording.

This design does not silently add an EOF field. The controller accepts
current-end semantics for stage 4 based on the normal supervisor ordering
above. Implementation reads all advancing available blocks and stops at the
first successful no-progress response; it does not describe that observation
as runner-equivalent proof. A future requirement for proven finality would need
a separate authorized wire design exposing terminal per-stream byte targets
bound to the selected turn, or equivalent server-side proof.

## Detail recovery and waiting questions

### Task detail

`TaskDetail` currently keeps polling after an error, but a later success sets
only `detail`; render still returns the old error. A successful response must
set the new detail and clear error together. If a later request fails and a
last-good detail exists, keep rendering that payload and show the transient
error non-destructively. If no successful payload exists, retain the existing
loading/error states. Task-ID change clears both, aborts the old generation, and
starts the same 2,000 ms cadence.

While an expanded turn has both `started_at_millis == null` and
`ended_at_millis == null`, render a short waiting-for-start state instead of
mounting `TurnLogPanel`. Mount the existing unchanged panel as soon as either
timestamp exists, with the existing `live` derivation. Pre-start failed or ended
turns remain readable when `ended_at_millis` is populated even if
`started_at_millis` is null. Do not change the log hook, `ActiveTurn`, poll
cadence, or log protocol.

### Questions

`useAttentionQuestions` currently slices the waiting IDs to six. Replace that
total cap with at most six asynchronous workers sharing one next-index counter.
Each worker:

1. stops before dequeuing if cleanup cancelled its generation;
2. claims one ID in input order;
3. calls existing `fetchTaskDetail(id, signal)`;
4. records `detail.questions` under that exact ID only on success;
5. checks cancellation/generation again before publishing;
6. loops until every ID has been attempted.

Reset the result map when the ordered ID generation changes. Aborting or
changing generation prevents old completions from publishing and prevents
workers from claiming more IDs. Out-of-order completion cannot change the ID
used as the map key.

An absent map entry remains `undefined` and uses the existing not-loaded
presentation. A successful empty `questions` array is stored as `[]` and may
show the existing “no question” presentation. A failed or unrequested task is
never converted to `[]`. No automatic retry is added for failed question
details in this stage.

## Pull-request checks and embedded assets

The repository currently has only `.github/workflows/release.yml`, triggered by
version tags. Add a pull-request workflow with least-privilege
`contents: read`.

The UI job uses a Node release supported by the lockfile (Node 22 at least
22.12), runs `npm ci`, `npm test`, and `npm run lint`, then creates a fresh
task-owned directory with `mktemp -d` under `$RUNNER_TEMP`. It invokes the
existing production build with that directory as `--outDir` and recursively
compares the entire result with `src/dashboard/static/app`. The comparison
fails for a missing, extra, or byte-changed file. It does not delete a
predictable pre-existing path.

The Rust job runs on the official GitHub-hosted `macos-15` arm64 label
(Sequoia), installs rustfmt and Clippy, and runs:

```sh
cargo fmt --all --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
```

Before introducing `actions/setup-node` or changing an action major, the
implementer verifies the selected version against the official action
repository/Marketplace and records the source in the task report. Existing
`actions/checkout@v4` in the release workflow is the starting precedent, not
permission to guess a current setup action version. Official sources on
2026-09-08 require `actions/checkout@v7` and `actions/setup-node@v7` for the
new PR workflow (Node 20 action runtimes are removed from runners on
2026-09-23). The release workflow remains `actions/checkout@v4` on `macos-14`
until a separately tracked migration.

After Tasks 1–3 pass source review, Task 4 runs the normal UI production build
once into `src/dashboard/static/app`, reviews all generated additions,
deletions, and modifications, then verifies a second fresh temp build is
identical. Rust continues to embed the stable filenames configured by
`ui/vite.config.ts`.

## File ownership

| Task | Production ownership | Test/derived ownership |
| --- | --- | --- |
| Settings | `ui/src/lib/api.ts`, `ui/src/views/Settings.tsx` | `ui/src/lib/api.test.ts`, `ui/src/views/Settings.test.tsx`, `tests/dashboard_settings.rs` |
| Turn logs | `ui/src/hooks/useTurnLog.ts`, wording only in `ui/src/components/TurnLogPanel.tsx` | `ui/src/hooks/useTurnLog.test.ts`, focused panel expectation if needed |
| Detail/questions | `ui/src/views/TaskDetail.tsx`, `ui/src/hooks/useAttentionQuestions.ts`, loading branch in `ui/src/views/Tasks.tsx` | matching view tests and new `ui/src/hooks/useAttentionQuestions.test.ts` |
| CI/assets | `.github/workflows/ci.yml` | one final regeneration of `src/dashboard/static/app/**` |

Tasks run sequentially. If an implementation reveals a necessary file outside
its row, it stops and reports the dependency rather than performing unrelated
cleanup.

## Validation and completion

Task 1 owns `npm ci` and clean baseline UI/focused Settings checks before RED
tests. The already recorded full Rust baseline at this exact base is 1,615
passing tests; do not rerun the full suite per task. Each implementer runs only
its focused RED/GREEN checks and records exact commands, outcomes, and commit
SHA. Reviewers inspect source and tests without regenerating assets or
duplicating test runs.

The final validation agent runs, on one frozen revision: full UI tests and lint,
a fresh temp production build against the checked-in complete asset tree, Rust
formatting, all-target Clippy, all-target tests, and `git diff --check`. It owns
all full gates and uses `/private/tmp/mac-worker-dashboard-stage4-target`.

Completion requires all four task commits, source review before generated
assets, generated-asset review, final parity, no unrelated files, and an
explicit report of the log-finality limitation.

## Completion decisions

Ruling: Validate the Settings boundary with paired production-client contract tests and existing real Axum route tests — the repository already has both and no browser-to-Rust harness — cost: this does not prove real-browser-to-server integration in one end-to-end execution, so that limitation must be reported explicitly.

Ruling: Regenerate and verify embedded UI assets once after the source tasks are reviewed — this keeps source review focused and avoids repeated minified bundle churn — cost: intermediate source commits are not release-ready; no integration occurs before the final asset-parity gate passes.

Ruling: Accept current-end log semantics for stage 4 without a wire change — normal supervisor ordering publishes ended turns after logs and terminal status are durable, while the dashboard exposes no final byte target — cost: a prematurely empty or malformed response cannot be distinguished from final EOF, so this UI is not runner-equivalent proof of log finality.

Ruling: Allow a finalized log reader to resume when the same identity becomes live — TaskDetail also passes live=false for a not-yet-started turn, so finalization cannot be permanent for that identity — cost: finalization is now per stopped reading period and the resume path must preserve text/offset while restoring a usable UTF-8 decoder.

Ruling: Defer a pending turn log panel until TaskDetail observes a start or end timestamp — live=false otherwise conflates queued and completed turns, so a fast queued-to-terminal transition can leave an early-empty reader stopped — cost: pre-start output is not shown until the next detail observation, within the existing polling cadence.

Ruling: Use macos-15 for the new PR Rust job — official runner documentation already deprecates macos-14 with retirement on 2026-11-02 — cost: PR CI and the existing release workflow use different macOS versions until the separately tracked release-workflow migration.

The log-finality gate is resolved. Task 2 may implement the bounded available-log
behavior above without changing the protocol.

## Non-goals

No visual redesign, new authentication token, manual Origin header, new
question protocol, task-answer mutation, polling optimization, direct task
lookup, SSE/WebSocket transport, live fleet smoke, provider execution, release,
push, deployment, or unrelated cleanup belongs to this stage.
