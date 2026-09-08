# Dashboard Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement only the assigned task. The controller dispatches one fresh implementation agent at a time; task agents must not spawn nested agents.

**Goal:** Repair the existing dashboard's Settings contract, completed-log reading, transient recovery, waiting-question fan-out, and pull-request/embedded-asset checks without redesigning the UI or changing existing wire formats.

**Architecture:** Keep the protected Axum routes and typed React API, and repair consumers around their current contracts. Treat task/turn/stream as log identity, use completion-based bounded reads, use a six-worker queue for attention details, and defer the one checked-in UI build until all source tasks pass review. Add separate UI and macOS Rust pull-request jobs, with a fresh-directory comparison proving the complete embedded tree matches a production build.

**Tech Stack:** Rust 2024, Axum 0.8, Tokio, React 19, TypeScript 6, Vite 8, Vitest 5, Testing Library, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-08-dashboard-reliability-design.md`

## Global Constraints

- Base is `2d1a6c441303758434fcb73a86d5f6999f9e5c97`; recheck HEAD/status at every handoff and preserve unrelated work.
- Execute tasks strictly in order: Settings, turn logs, task detail/questions, PR CI plus the single final asset regeneration.
- One Cursor implementation agent works at a time. Do not spawn nested agents and do not run concurrent Cargo commands.
- Use `/private/tmp/mac-worker-dashboard-stage4-target` for every local Cargo command.
- Task 1 owns `npm ci` and clean baseline checks. The exact-base full Rust baseline is already recorded as 1,615 passed; do not rerun a full Rust suite per task.
- Preserve exact Host, browser Origin, `X-Mac-Worker-Settings: 1`, JSON content type, 8,192-byte body limit, typed request validation, configured-worker lookup, native pre/post-lock revision checks, and singular POST response.
- Browser code must not set `Origin`. Do not add an auth token, new Settings shape, log EOF/length field, question protocol, CORS, or external runtime dependency.
- Preserve 65,536-byte log chunks, independent stdout/stderr, 1,000 ms live cadence, task+turn+stream identity, and at most one read in flight per hook.
- Preserve task-detail task-ID cancellation and 2,000 ms cadence. Stage 5 polling/performance work is excluded.
- Six bounds concurrent attention-detail requests, never total IDs. Failed/unrequested tasks must not become successful empty question lists.
- Tests use local fixtures only. Do not contact workers, run provider tasks, inspect credentials, push, deploy, release, reset Git, or edit user files.
- Tasks 1–3 commit source and focused tests without regenerating `src/dashboard/static/app`. Reviewers inspect source first and do not duplicate implementer test runs.
- Task 4 verifies official sources for any newly introduced external action version, regenerates checked-in UI assets exactly once, and compares a second fresh temp build recursively so missing, extra, and changed files fail.
- Every implementation report records changed files, exact RED/GREEN commands and outcomes, commit SHA, limitations, and confirms no live fleet/provider execution.

---

## File map and producer/consumer contracts

| File | Responsibility in this stage |
| --- | --- |
| `ui/src/lib/api.ts` | Emit protected Settings request and type singular save response; keep existing task/log helpers |
| `ui/src/views/Settings.tsx` | Merge a saved agent into its worker list and suppress stale save completions |
| `tests/dashboard_settings.rs` | Exercise the real Axum route's protected request and singular response |
| `ui/src/hooks/useTurnLog.ts` | Preserve log identity state and serialize live/completed reads |
| `ui/src/components/TurnLogPanel.tsx` | Describe current-end semantics honestly |
| `ui/src/views/TaskDetail.tsx` | Keep last-good detail visible and clear errors on success |
| `ui/src/hooks/useAttentionQuestions.ts` | Fetch every waiting task with at most six requests in flight |
| `ui/src/views/Tasks.tsx` | Preserve undefined/not-loaded separately from successful empty questions |
| `.github/workflows/ci.yml` | Run PR UI/parity and macOS Rust checks |
| `src/dashboard/static/app/**` | One final generated production bundle embedded by Rust |

The existing interfaces remain:

```ts
export async function saveAgentSettings(
  worker: string,
  body: SaveSettings,
): Promise<AgentSetting>

export const fetchTaskDetail = (
  taskId: string,
  signal?: AbortSignal,
): Promise<TaskDetail>

export const fetchTurnLog = (
  taskId: string,
  turnId: string,
  stream: LogStream,
  offset: number,
  signal?: AbortSignal,
): Promise<LogChunk>
```

The Rust route continues to consume `AgentSettingsSaveRequest` and
`DashboardSettingsSource::save` continues to return one
`AgentDefaultSettings`. No task changes those Rust interfaces.

### Resolved log-finality decision

The controller accepts stage 4's current-end behavior with no wire change.
Normal supervisor ordering publishes an ended turn after logs and terminal
status are durable, but the dashboard response still has no terminal byte
target/EOF field. Task 2 therefore reads all advancing available blocks and
stops on the first successful no-progress response. It must not invent a field
or call that observation “confirmed final EOF.”

### Task 1: Settings production-client contract and stale-response isolation

**Files:**
- Modify: `ui/src/lib/api.ts:201-217`
- Modify: `ui/src/lib/api.test.ts:34-55`
- Modify: `ui/src/views/Settings.tsx:62-110, 268-297, 408-417`
- Modify: `ui/src/views/Settings.test.tsx`
- Modify: `tests/dashboard_settings.rs:198-405`
- Report: `/private/tmp/mac-worker-dashboard-stage4/task1-report.md`

**Interfaces:**
- Consumes: existing `SaveSettings`, `AgentSetting`, `AgentSettings`,
  `DashboardSettingsSource::save`, and protected Axum POST route.
- Produces: `saveAgentSettings(...): Promise<AgentSetting>`; a Settings merge
  callback that replaces only the matching agent; selection-generation
  invalidation for delayed saves. Task 4 later builds these source changes.

- [ ] **Step 1: Confirm the assigned base and install the locked UI dependencies**

Run:

```sh
git rev-parse HEAD
git status --short --branch
(cd ui && npm ci)
```

Expected: HEAD is the controller-provided Task 1 base descended from
`2d1a6c4`, status contains no unrelated changes, and `npm ci` succeeds from
`ui/package-lock.json`. Do not update the lockfile.

- [ ] **Step 2: Run clean focused baselines before RED tests**

Run:

```sh
(cd ui && npm test)
(cd ui && npm run lint)
cargo test --locked --offline --target-dir /private/tmp/mac-worker-dashboard-stage4-target --test dashboard_settings
```

Expected: UI tests and focused Rust Settings route tests pass. Lint exits zero;
record any existing warnings without broad cleanup. If the Cargo cache is
incomplete, stop and report instead of silently removing `--offline`.

- [ ] **Step 3: Write RED production-client request/response tests**

In `ui/src/lib/api.test.ts`, make the Fetch mock return one production-shaped
agent entry, not `{ agents: [...] }`, and assert both request guards without
expecting browser code to set Origin:

```ts
const saved = await saveAgentSettings('mini-1', request)
const [, init] = fetchMock.mock.calls[0]

expect(init.headers).toMatchObject({
  'content-type': 'application/json',
  'x-mac-worker-settings': '1',
})
expect(init.headers).not.toHaveProperty('origin')
expect(JSON.parse(init.body)).toMatchObject({
  agent: 'codex',
  revision: 'rev-1',
})
expect(saved).toMatchObject({ agent: 'codex', revision: 'rev-2' })
```

Keep the existing 409 `ApiError` behavior test.

- [ ] **Step 4: Write RED component tests for singular merge and delayed save**

Change the Settings POST mock to return one `AgentSetting`. Add one test proving
that after save Codex adopts `rev-2`/new values while OpenCode remains in the
agent table.

Add a deferred-promise regression:

```ts
// Start a mini-1/codex save, then switch the worker before resolving it.
// Resolve mini-1's singular response only after mini-2 settings are visible.
expect(screen.getByText('mini-2 / codex')).toBeInTheDocument()
resolveWorkerOneSave(savedCodexForMiniOne)
await flushPromises()
expect(screen.getByText('mini-2 / codex')).toBeInTheDocument()
expect(miniTwoDraft()).toEqual(beforeDelayedResolution)
```

Use two workers in the snapshot and URL-sensitive GET mocks. Add a second
delayed case for agent identity: start a save for one agent, select another
agent, resolve the old save, and prove the new selected agent and its draft
remain unchanged. Both cases must pass; worker switching exercises unmount,
while agent switching exercises prop identity on a retained `Detail` instance.

- [ ] **Step 5: Strengthen the real Axum response-shape assertion**

In `settings_routes_read_and_protect_save`, parse the accepted POST body and
assert it is a singular object:

```rust
let saved: serde_json::Value = serde_json::from_slice(&accepted.body).unwrap();
assert_eq!(saved["agent"], "codex");
assert!(saved.get("agents").is_none());
```

Retain all existing missing-header, wrong-Origin, wrong-content-type,
oversized-body, unknown-worker, source-call-count, and conflict assertions.
Do not alter the route.

- [ ] **Step 6: Run RED checks**

Run:

```sh
(cd ui && npm test -- src/lib/api.test.ts src/views/Settings.test.tsx)
cargo test --locked --offline --target-dir /private/tmp/mac-worker-dashboard-stage4-target --test dashboard_settings
```

Expected: UI tests fail because the custom header/return type/merge/generation
guard are absent. The Rust response-shape assertion should already pass; it
locks the producer side and is not expected to be RED.

- [ ] **Step 7: Implement the minimal API correction**

Change only the helper's header and return type:

```ts
export async function saveAgentSettings(
  worker: string,
  body: SaveSettings,
): Promise<AgentSetting> {
  const response = await fetch(path, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-mac-worker-settings': '1',
    },
    body: JSON.stringify(body),
  })
  // Preserve existing error parsing.
  return payload as AgentSetting
}
```

Do not add `Origin`, fetch credentials, a token, or a second GET.

- [ ] **Step 8: Merge only the returned agent and invalidate stale saves**

Change `Detail.onSaved` to accept `AgentSetting`. In `Settings`, merge by ID:

```ts
const mergeSaved = (saved: AgentSetting) =>
  setSettings((current) =>
    current == null
      ? current
      : {
          agents: current.agents.map((agent) =>
            agent.agent === saved.agent ? saved : agent,
          ),
        },
  )
```

Inside `Detail`, use a monotonically increasing save-generation ref. Increment
when a save begins; cleanup for `[worker, setting.agent]` invalidates it.
Capture the value before awaiting and guard `onSaved`, `setMessage`, and
`setBusy` after fulfillment/rejection:

```ts
const generation = ++saveGeneration.current
try {
  const next = await saveAgentSettings(worker, body)
  if (generation !== saveGeneration.current) return
  onSaved(next)
  setMessage('Saved.')
} catch (error) {
  if (generation !== saveGeneration.current) return
  setMessage(error instanceof Error ? error.message : String(error))
} finally {
  if (generation === saveGeneration.current) setBusy(false)
}
```

An unmount/identity cleanup increments the ref. Do not rely on a React `key`
alone.

- [ ] **Step 9: Run GREEN focused checks**

Run the Step 6 commands again.

Expected: all focused UI and Rust tests pass. The stale 409 draft test, peer
agent test, delayed worker/agent test, and route guards all pass.

- [ ] **Step 10: Perform source-only review and commit**

Run:

```sh
git diff --check
git diff -- ui/src/lib/api.ts ui/src/lib/api.test.ts ui/src/views/Settings.tsx ui/src/views/Settings.test.tsx tests/dashboard_settings.rs
git status --short
```

Confirm no `src/dashboard/static/app/**` change exists. Write the Task 1 report,
then commit only the five owned repository files:

```sh
git add ui/src/lib/api.ts ui/src/lib/api.test.ts ui/src/views/Settings.tsx ui/src/views/Settings.test.tsx tests/dashboard_settings.rs
git commit -m "fix: align dashboard settings save contract"
```

Record the SHA and actual RED/GREEN results. The controller reviews source and
tests before dispatching Task 2; the reviewer does not regenerate assets or
rerun the focused commands.

### Task 2: Multi-block completed logs and lifecycle-safe UTF-8

**Decision:** Current-end semantics are approved for this task. Preserve the
existing wire shape and report that no-progress is not runner-equivalent proof.

**Files:**
- Modify: `ui/src/hooks/useTurnLog.ts`
- Modify: `ui/src/hooks/useTurnLog.test.ts`
- Modify: `ui/src/components/TurnLogPanel.tsx:48-52, 94-100`
- Test if wording is asserted: `ui/src/views/TaskDetail.test.tsx`
- Report: `/private/tmp/mac-worker-dashboard-stage4/task2-report.md`

**Interfaces:**
- Consumes: unchanged `fetchTurnLog`, `LogChunk`, `LogStream`, 65,536-byte
  request limit, and the `live` boolean derived by `TaskDetail`.
- Produces: unchanged `TurnLog { text, offset, error }`, with identity-preserved
  state, one in-flight read, current-end completion drain, and retry after
  transient completed-read error. Task 4 later builds it.

- [ ] **Step 1: Confirm handoff and run the focused baseline**

Run:

```sh
git rev-parse HEAD
git status --short --branch
(cd ui && npm test -- src/hooks/useTurnLog.test.ts src/views/TaskDetail.test.tsx)
```

Expected: clean Task 1 descendant and focused tests pass.

- [ ] **Step 2: Add RED multi-block and live-to-completed tests**

Use deferred Fetch responses and fake timers. Add a completed stream with at
least `2 * MAX_LOG_CHUNK_BYTES + 4` bytes returned as three advancing chunks
and a fourth empty chunk. Assert exact requested offsets and complete text.

Render live, return `"prefix"`, rerender the same task/turn/stream with
`live=false`, return `"TAIL"` from the previous offset, then empty:

```ts
expect(result.current.text).toBe('prefixTAIL')
expect(requestedOffsets).toEqual([0, prefixBytes, totalBytes])
```

This must fail against the current reset-on-`live` effect.

- [ ] **Step 3: Add RED UTF-8, serialization, and selection-isolation tests**

Cover:

- one UTF-8 scalar split across two completed chunks, followed by no-progress,
  renders once without `\uFFFD`;
- a blocked first request plus timer advancement never starts a second request
  until the first settles;
- changing task, turn, or stream aborts/ignores the old request, resets to
  offset zero, and an old partial decoder flush never appears in the new text;
- stdout and stderr hook instances retain independent offsets/text.

Use bytes rather than JavaScript string length for offsets.

- [ ] **Step 4: Add RED completed-error retry and success-clear tests**

Return one advancing block, then 503, then on the 1,000 ms retry return an empty
successful chunk. Assert:

```ts
expect(result.current.text).toBe('kept bytes')
expect(result.current.error).toContain('503')
// After retry succeeds without advancing:
expect(result.current.text).toBe('kept bytes')
expect(result.current.error).toBeNull()
```

Assert no remount/rerender is needed and no immediate loop occurs while the
error is pending.

- [ ] **Step 5: Run RED tests**

Run:

```sh
(cd ui && npm test -- src/hooks/useTurnLog.test.ts src/views/TaskDetail.test.tsx)
```

Expected: fail on multi-block completion, live-state preservation, completed
retry, no-progress error clearing, and stale decoder isolation.

- [ ] **Step 6: Separate identity reset from read scheduling**

Keep refs for offset, decoder, identity generation, active request, retry timer,
and whether the decoder has been finalized for the current stopped reading
period. One effect keyed only by `[taskId, turnId, stream]` resets state and
creates the decoder. Its cleanup increments generation, aborts work, clears
timers, and discards the decoder without calling `decode()` or publishing text.

A second effect may react to `live`, but it must not reset text or offset. When
a finalized same-identity reader becomes live, it clears finalization and
creates a fresh UTF-8 decoder before resuming from the preserved offset. Use an
async recursive scheduler, not `setInterval`.

- [ ] **Step 7: Implement one read outcome and serialized scheduling**

Implement one internal read attempt with explicit outcomes:

```ts
type ReadOutcome = 'advanced' | 'idle' | 'failed'
```

For a current-generation successful response, clear `error` before branching.
If `next_offset > offset.current`, decode with `{ stream: true }`, append, and
return `advanced`. If equal, return `idle`. Ignore results after abort or
generation change.

Scheduling rules:

```ts
if (live) {
  // Exactly one attempt, then schedule the next after 1000 ms.
} else if (outcome === 'advanced') {
  // Continue immediately and sequentially.
} else if (outcome === 'failed') {
  // Retry after 1000 ms.
} else {
  // Flush this stopped reading period's decoder once and stop.
}
```

Never immediately repeat `idle`; never run two calls concurrently. A failed
completed read retains text/offset/decoder and remains retryable. Never advance
the offset for bytes that were not consumed by a usable decoder.

- [ ] **Step 8: Correct the panel's finality wording**

Replace “the turn is finished, nothing more will arrive” with wording that does
not claim a terminal byte target, for example:

```tsx
'the turn is finished · read to the current end'
```

Keep the live wording, command, byte count, and truncation message unchanged.
Update only the focused expectation that names the old sentence.

- [ ] **Step 9: Run GREEN focused checks**

Run the Step 5 command.

Expected: all lifecycle, offset, retry, UTF-8, cancellation, and panel tests
pass.

- [ ] **Step 10: Review source only, report the finality limitation, and commit**

Run:

```sh
git diff --check
git diff -- ui/src/hooks/useTurnLog.ts ui/src/hooks/useTurnLog.test.ts ui/src/components/TurnLogPanel.tsx ui/src/views/TaskDetail.test.tsx
git status --short
```

Confirm no generated asset changed. The report must say that completed
no-progress is current-end, not runner-equivalent proven EOF. Commit only files
actually changed:

```sh
git add ui/src/hooks/useTurnLog.ts ui/src/hooks/useTurnLog.test.ts ui/src/components/TurnLogPanel.tsx ui/src/views/TaskDetail.test.tsx
git commit -m "fix: drain completed dashboard turn logs"
```

If `TaskDetail.test.tsx` did not need a wording edit, omit it from `git add`.

### Task 3: Task-detail recovery and bounded all-task question loading

**Files:**
- Modify: `ui/src/views/TaskDetail.tsx:103-139`
- Modify: `ui/src/views/TaskDetail.test.tsx`
- Modify: `ui/src/hooks/useAttentionQuestions.ts`
- Create: `ui/src/hooks/useAttentionQuestions.test.ts`
- Modify: `ui/src/views/Tasks.tsx:99-190`
- Modify: `ui/src/views/Tasks.test.tsx`
- Report: `/private/tmp/mac-worker-dashboard-stage4/task3-report.md`

**Interfaces:**
- Consumes: existing `fetchTaskDetail(id, signal)`, `TaskDetail`,
  `(string | Question)[]`, 2,000 ms detail cadence.
- Produces: last-good TaskDetail rendering and
  `Record<string, (string | Question)[] | undefined>` where only successful
  reads install a key; six is maximum in flight, not maximum IDs. Task 4 later
  builds it.

- [ ] **Step 1: Confirm handoff and run focused baselines**

Run:

```sh
git rev-parse HEAD
git status --short --branch
(cd ui && npm test -- src/views/TaskDetail.test.tsx src/views/Tasks.test.tsx)
```

Expected: clean Task 2 descendant and focused tests pass.

- [ ] **Step 2: Add RED TaskDetail failure/recovery tests**

With fake timers, return detail A, then reject one poll, then return detail B.
Assert A remains visible with a transient error after failure. After the next
2,000 ms attempt, assert B is visible and the error is absent.

Add a task-ID rerender test: leave task A pending, rerender task B, resolve A,
and prove A never replaces B. Preserve the existing no-last-good initial error
case.

- [ ] **Step 3: Add a RED hook test for more than six IDs**

Create `useAttentionQuestions.test.ts`. Supply eight IDs, track active Fetch
calls, and hold each in a deferred promise. Assert exactly six start initially;
resolving any one permits the seventh, and resolving another permits the
eighth. At completion:

```ts
expect(maximumActive).toBe(6)
expect(Object.keys(result.current)).toHaveLength(8)
expect(result.current[id8]?.[0]).toMatchObject({ text: 'question 8' })
```

This fails against `.slice(0, MAX_ATTENTION_FETCHES)`.

- [ ] **Step 4: Add RED mapping, empty/not-loaded, and cancellation tests**

Resolve the eight requests out of order and assert every response remains under
its requested ID. Before an ID resolves, assert its value is `undefined`.
Resolve one success with `questions: []` and assert that exact ID becomes `[]`.
Reject another and assert it remains `undefined`, not `[]`.

Rerender with a new ID generation while old deferred requests exist. Assert old
results never publish, aborted workers stop dequeuing old IDs, and the new
generation starts within the same six-request bound.

- [ ] **Step 5: Add the view-level undefined/empty regression**

In `Tasks.test.tsx`, prove an unresolved waiting task shows the existing
“Reading the question…” branch. Then resolve a genuine empty array and prove
only that task shows the no-question copy. A failed detail must not reach the
empty copy.

- [ ] **Step 6: Run RED focused checks**

Run:

```sh
(cd ui && npm test -- src/views/TaskDetail.test.tsx src/hooks/useAttentionQuestions.test.ts src/views/Tasks.test.tsx)
```

Expected: fail because TaskDetail hides last-good data/retains old error, only
six IDs are attempted, failures become empty arrays, and missing entries are
coalesced before the view branch.

- [ ] **Step 7: Publish detail and clear error atomically on success**

In `TaskDetail.poll`:

```ts
const payload = await fetchTaskDetail(taskId, controller.signal)
if (!cancelled) {
  setDetail(payload)
  setError(null)
}
```

On failure, keep `detail` unchanged and set the error. Render a full-page error
only when `detail == null`; when last-good detail exists, render a small
transient error above the unchanged detail. Keep the interval, abort, and
task-ID dependency unchanged.

- [ ] **Step 8: Replace total truncation with a six-worker queue**

Keep the hook's public return type. Reset to `{}` for each ordered `key`
generation. Use a shared integer cursor and start
`Math.min(MAX_ATTENTION_FETCHES, ids.length)` workers:

```ts
const worker = async () => {
  while (!cancelled) {
    const index = nextIndex
    nextIndex += 1
    if (index >= ids.length || cancelled) return
    const id = ids[index]
    try {
      const detail = await fetchTaskDetail(id, controller.signal)
      if (!cancelled) {
        setQuestions((current) => ({
          ...current,
          [id]: detail.questions,
        }))
      }
    } catch {
      // Keep this ID undefined. Do not publish [] for failure.
    }
  }
}
```

Cleanup sets `cancelled`, aborts the shared controller, and therefore prevents
publication and further dequeues. Do not add retries or more endpoints.

- [ ] **Step 9: Preserve undefined in `Tasks`**

Remove the premature fallback:

```ts
const asked = questions[task.task_id]
```

Keep the branches: `undefined` means not loaded; `[]` means a successful detail
contained no questions; nonempty means render each mapped question.

- [ ] **Step 10: Run GREEN focused checks**

Run the Step 6 command.

Expected: all tests pass, all eight IDs are accessible with at most six active,
out-of-order/cancelled results remain isolated, and task detail visibly
recovers without changing cadence.

- [ ] **Step 11: Review source only and commit**

Run:

```sh
git diff --check
git diff -- ui/src/views/TaskDetail.tsx ui/src/views/TaskDetail.test.tsx ui/src/hooks/useAttentionQuestions.ts ui/src/hooks/useAttentionQuestions.test.ts ui/src/views/Tasks.tsx ui/src/views/Tasks.test.tsx
git status --short
```

Confirm no asset changes and no broad polling refactor. Write the report and
commit:

```sh
git add ui/src/views/TaskDetail.tsx ui/src/views/TaskDetail.test.tsx ui/src/hooks/useAttentionQuestions.ts ui/src/hooks/useAttentionQuestions.test.ts ui/src/views/Tasks.tsx ui/src/views/Tasks.test.tsx
git commit -m "fix: recover dashboard task attention data"
```

The controller completes the initial source-only review of Tasks 1–3 before
dispatching Task 4.

### Task 4: Pull-request checks and one embedded-asset regeneration

**Files:**
- Create: `.github/workflows/ci.yml`
- Regenerate once: `src/dashboard/static/app/index.html`
- Regenerate once: `src/dashboard/static/app/favicon.svg`
- Regenerate once: `src/dashboard/static/app/assets/index.js`
- Regenerate once: `src/dashboard/static/app/assets/index.css`
- Regenerate only if emitted differently: `src/dashboard/static/app/assets/*.ttf`
- Report: `/private/tmp/mac-worker-dashboard-stage4/task4-report.md`

**Interfaces:**
- Consumes: reviewed Tasks 1–3 source, `ui/package-lock.json`,
  `ui/package.json` scripts, stable Vite output names, Rust `include_str!` /
  `include_bytes!`, and release workflow macOS precedent.
- Produces: PR-triggered UI/parity and Rust jobs plus the only integrated,
  checked-in production UI tree. The final validation agent consumes this
  frozen commit.

- [ ] **Step 1: Confirm all source tasks are present and the tree is clean**

Run:

```sh
git log -4 --oneline
git status --short --branch
```

Expected: the three reviewed task commits are present and no uncommitted source
or asset change exists.

- [ ] **Step 2: Verify external action versions from official sources**

Check the official `actions/checkout` and `actions/setup-node` GitHub
repositories or Marketplace pages on the implementation date. Reuse
`actions/checkout@v4` only if still officially supported; select a setup-node
major officially supporting Node 22. Record URLs, observed supported majors,
and the chosen versions in the Task 4 report before editing YAML. Do not infer
versions from third-party posts.

- [ ] **Step 3: Add the PR workflow**

Create `.github/workflows/ci.yml` with:

```yaml
name: CI

on:
  pull_request:

permissions:
  contents: read

jobs:
  ui:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
          cache: npm
          cache-dependency-path: ui/package-lock.json
      - run: npm ci
        working-directory: ui
      - run: npm test
        working-directory: ui
      - run: npm run lint
        working-directory: ui
      - name: Verify embedded dashboard assets
        shell: bash
        run: |
          asset_dir="$(mktemp -d "${RUNNER_TEMP}/dashboard-ui.XXXXXX")"
          trap 'rm -rf "$asset_dir"' EXIT
          (cd ui && npm run build -- --outDir "$asset_dir")
          diff -r "$asset_dir" src/dashboard/static/app

  rust:
    runs-on: macos-14
    env:
      CARGO_TARGET_DIR: /private/tmp/mac-worker-dashboard-stage4-target
    steps:
      - uses: actions/checkout@v4
      - run: rustup component add rustfmt clippy
      - run: cargo fmt --all --check
      - run: cargo test --locked --all-targets
      - run: cargo clippy --locked --all-targets -- -D warnings
```

Replace only action major versions contradicted by Step 2's official evidence.
Do not add push, tag, deployment, worker, provider, or release triggers.

- [ ] **Step 4: Run the single checked-in production build**

Run:

```sh
(cd ui && npm run build)
git status --short
git diff --stat -- src/dashboard/static/app
```

Expected: Vite writes only `src/dashboard/static/app/**`. Inspect status for
deleted or newly generated files, including fonts; do not assume the four text
files are the complete diff.

- [ ] **Step 5: Compare a second fresh build with the entire embedded tree**

Use a newly allocated directory, never a predictable pre-existing path:

```sh
asset_dir="$(mktemp -d /private/tmp/mac-worker-dashboard-stage4-ui.XXXXXX)"
trap 'rm -rf "$asset_dir"' EXIT
(cd ui && npm run build -- --outDir "$asset_dir")
diff -r "$asset_dir" src/dashboard/static/app
```

Expected: exit zero. `diff -r` compares all entries and catches missing, extra,
and changed files.

- [ ] **Step 6: Run Task 4's focused delivery checks**

Run:

```sh
cargo test --locked --offline --target-dir /private/tmp/mac-worker-dashboard-stage4-target --test dashboard_web
git diff --check
```

Expected: the embedded Axum serving tests and diff check pass. The production
build/parity checks are already recorded by Steps 4–5. Do not duplicate the
final agent's full UI, lint, or Rust gates, and do not run provider/fleet smoke.

- [ ] **Step 7: Review workflow and derived artifacts, then commit**

Run:

```sh
git diff -- .github/workflows/ci.yml
git diff --stat -- src/dashboard/static/app
git status --short
```

Inspect source commits first, then the generated bundle diff/stat. Confirm all
changed files are the workflow or generated app tree. Write the report, then:

```sh
git add .github/workflows/ci.yml src/dashboard/static/app
git commit -m "ci: verify dashboard source and embedded assets"
```

Record the commit SHA, official action evidence, build/parity result, and exact
generated files. Do not push.

### Final frozen validation and completion review

This is a validation-agent task, not another implementation change.

**Files:**
- Read: all four task diffs and reports
- Write report only: `/private/tmp/mac-worker-dashboard-stage4/final-report.md`

**Interfaces:**
- Consumes: frozen Task 4 commit with source, tests, workflow, and matched
  assets.
- Produces: one complete validation record for controller review. It makes no
  repository commit unless a failed gate is returned to the owning
  implementation task for a focused fix.

- [ ] **Step 1: Freeze and inspect repository state**

Run:

```sh
git rev-parse HEAD
git status --short --branch
git log -5 --oneline
```

Expected: clean tree with the four implementation commits in order.

- [ ] **Step 2: Run the full frozen UI gate**

Run:

```sh
(cd ui && npm ci)
(cd ui && npm test)
(cd ui && npm run lint)
```

Expected: all commands exit zero. Record test-file/test counts and lint
warnings, if any.

- [ ] **Step 3: Run fresh full-tree asset parity**

Run:

```sh
asset_dir="$(mktemp -d /private/tmp/mac-worker-dashboard-stage4-final-ui.XXXXXX)"
trap 'rm -rf "$asset_dir"' EXIT
(cd ui && npm run build -- --outDir "$asset_dir")
diff -r "$asset_dir" src/dashboard/static/app
```

Expected: exit zero with no missing, extra, or changed file.

- [ ] **Step 4: Run the full frozen Rust gate serially as commands**

Run one command at a time with the shared target directory:

```sh
cargo fmt --all --check
cargo clippy --locked --offline --target-dir /private/tmp/mac-worker-dashboard-stage4-target --all-targets -- -D warnings
cargo test --locked --offline --target-dir /private/tmp/mac-worker-dashboard-stage4-target --all-targets
git diff --check
```

Expected: all exit zero. Do not start Clippy and tests concurrently. If a known
timing-sensitive test fails, report the exact failure before any isolated
rerun; do not conceal it with repeated runs.

- [ ] **Step 5: Review completion boundaries**

Confirm:

- Settings tests are honestly paired and do not claim one browser-to-Rust e2e;
- delayed worker/agent save responses cannot replace current settings;
- completed logs drain multiple chunks, preserve live text/split UTF-8, retry
  errors, clear errors on empty success, and make no proven-EOF claim;
- detail last-good recovery and all-ID six-concurrent question loading pass;
- PR CI has only local checks and complete-tree parity;
- checked-in assets equal the frozen source build;
- no unrelated source, dependency, release, credential, fleet, or provider
  changes exist.

Write the final report with HEAD SHA and all outcomes. Do not push or deploy.

## Proposed task/file overlap

| Shared area | Tasks | Coordination rule |
| --- | --- | --- |
| `ui/src/lib/api.ts` helpers/types | 1 consumes Settings; 2/3 consume existing task/log helpers | Task 1 changes only Settings return/header; later tasks must not reshape APIs |
| `ui/src/views/TaskDetail.test.tsx` | 2 may update one wording assertion; 3 adds recovery tests | Sequential commits; Task 3 starts from Task 2 and preserves its log assertion |
| React generated bundle | 1–3 affect output; 4 owns generated files | No source task builds checked-in assets; Task 4 regenerates once |
| UI dependencies | all UI tasks | Task 1 alone runs `npm ci`; later tasks use the unchanged lock/install |
| Cargo target | Tasks 1, 4, final validation | Commands are sequential and always use the same task-owned target |
| Full UI/Rust gates | Task 4 has delivery-focused checks; final validation owns the full frozen gate | Reviewers do not duplicate test runs |

## Self-review results

### Spec coverage

- Settings header, singular response, peer preservation, fresh revision,
  browser-owned Origin, route guards, honest paired tests, and delayed
  worker/agent response isolation are covered by Task 1.
- Log identity, multi-block available-log reads for ended turns, live
  preservation, independent streams, one in-flight request, no-progress stop,
  ended-turn retry, empty
  success error clearing, UTF-8 flush, and stale cleanup isolation are covered
  by Task 2.
- The concrete finality gap and accepted current-end ruling are stated in the
  spec and Task 2; no task invents EOF.
- Last-good detail, success error clearing, task-ID cancellation, 2,000 ms
  cadence, all waiting IDs, six concurrency, ordering, empty/not-loaded,
  cancellation, and stale generation isolation are covered by Task 3.
- Official action verification, PR-only checks, supported Node, macOS Rust,
  fresh directory allocation, full-tree parity, one checked-in build, and
  source-before-bundle review are covered by Task 4.
- Single ownership of the complete frozen UI/Rust/fmt/Clippy/parity gate is
  covered by final validation.

No stage 4 acceptance item lacks an implementation or validation owner.

### Placeholder scan

The plan contains no TBD/TODO steps, generic “add tests” instruction,
unspecified file owner, or speculative scaffold. Conditional action-version
selection is tied to an exact official-source verification step. The only
conditional staging instruction omits `TaskDetail.test.tsx` when Task 2 makes
no change to that file.

### Producer/consumer and type consistency

- POST Settings producer remains singular; TypeScript consumes
  `Promise<AgentSetting>` and merges into `AgentSettings.agents`.
- The save-generation guard is local to `(worker, agent)` and does not alter
  the API or native revision mechanism.
- Turn log public return type and route shape remain unchanged.
- Attention hook returns `undefined` for absent/failed reads and `[]` only for
  successful empty reads; `Tasks` consumes those meanings directly.
- Vite's full output is the exact directory recursively compared and embedded
  by Rust.

## Completion decisions

Ruling: Validate the Settings boundary with paired production-client contract tests and existing real Axum route tests — the repository already has both and no browser-to-Rust harness — cost: this does not prove real-browser-to-server integration in one end-to-end execution, so that limitation must be reported explicitly.

Ruling: Regenerate and verify embedded UI assets once after the source tasks are reviewed — this keeps source review focused and avoids repeated minified bundle churn — cost: intermediate source commits are not release-ready; no integration occurs before the final asset-parity gate passes.

Ruling: Accept current-end log semantics for stage 4 without a wire change — normal supervisor ordering publishes ended turns after logs and terminal status are durable, while the dashboard exposes no final byte target — cost: a prematurely empty or malformed response cannot be distinguished from final EOF, so this UI is not runner-equivalent proof of log finality.

Ruling: Allow a finalized log reader to resume when the same identity becomes live — TaskDetail also passes live=false for a not-yet-started turn, so finalization cannot be permanent for that identity — cost: finalization is now per stopped reading period and the resume path must preserve text/offset while restoring a usable UTF-8 decoder.

The log-finality gate is resolved. This planning commit authorizes no
implementation by itself; the controller dispatches the already specified
tasks separately.
