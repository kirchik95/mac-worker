# Paper UI Migration Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task. The pages share navigation, task semantics and UI primitives; execute their integration sequentially.

**Goal:** Implement the approved I.6 shelf, J.1–J.8 screens and notification drawer in the existing frontend.

**Architecture:** Keep React, Base UI primitives, polling and Rust API contracts. Extract the Paper tokens and approved SVG assets into the existing UI, share task presentation and table primitives, and retain the guarded mutation lifecycle. Add local notification read state without changing task resolution.

**Tech Stack:** React 19, TypeScript 6, Vite 8, Tailwind CSS 4, shadcn/Base UI, Lucide, Vitest, Rust embedded assets.

**Spec:** `DESIGN.md`; approved Paper file `01M1V8VWPMWHMNASQ70NEE0D4A`, app page `5-0`, Design System page `7-0`. Exact source exports and captures are in the original checkout's `.impeccable/mocks/app-redraw/` and `.impeccable/design-system/`.

## Global Constraints

- Personal developer tool; English UI; existing API remains authoritative.
- Inter, Menlo, white/cool canvas, graphite actions, amber questions, green review; DS controls supersede retained clone exceptions.
- Buttons 38 px / radius 6 px; fields 40 px; panels radius 10 px; 40 px desktop insets; focus 2 px with offset 2 px.
- Reply starts another turn; accepting closes and never merges. Preserve optimistic checks, cancellation and polling races.
- Separate task state, agent authentication and observed worker capacity; unknown observations cannot become idle/authenticated.
- No new dashboard submission/cancellation or browser agent sign-in. Existing terminal commands and diagnostic data remain accessible.
- Motion: 3 s working signal, 650 ms completion once, 2 px illustration lift; reduced motion and pause while hidden/offscreen.
- Keep actual SVG marks/illustration geometry. Never substitute synthetic metrics for API values.
- Only new behavior gets new behavioral tests. Reversible visual styling is verified by renders, not implementation-mirroring tests.

### Task 1: Shared system, shelf and navigation

**Files:** `ui/src/index.css`, `ui/src/App.tsx`, `ui/src/components/ui/{button,input,select,switch}.tsx`, `ui/src/components/{Wordmark,WorkerCard,StatusMark}.tsx`, `ui/src/views/Overview.tsx`; new `AgentMark`, `MacIllustration`, `TaskBadge`, `TaskTable`, `AttentionCards`, `Notifications` components and task presentation/read-state helpers. Add Inter asset and update Rust static font serving.

**Interfaces:** Shared components consume `Snapshot`, `TaskRow`, `Worker` from `lib/api.ts`. Navigation callbacks are `onSelectTask(id: string)`, `onShowRun(id: string)`, and `onSetup(worker?: string, agent?: string)`. Preserve exported `parseTaskHash` and `documentTitle`.

- [x] Add failing behavior checks: shelf shows named waiting questions and a review action before worker cards; task actions select their exact ID; stale/offline capacity never appears as free slots.
- [x] Add notification checks: mark-all-read changes unread count without hiding unresolved items in All; a newer task event becomes unread; Escape closes and returns focus; opening/reading never calls task mutation endpoints.
- [x] Extract tokens/assets, implement shared primitives and header, rebuild shelf and worker details, implement the modal side drawer with Base UI focus management.
- [x] Keep active-turn logs, queue diagnostics and capability facts available through contextual details.
- [x] Run targeted App/Overview/notification/task-presentation tests.

### Task 2: Tasks and runs

**Files:** `ui/src/views/{Tasks,Runs}.tsx` and their behavioral tests; shared TaskTable.

**Interfaces:** `Tasks({snapshot, onSelect, initialRun?, onSetup?})`; `Runs({snapshot,onShowRun?})`. State filters map Running to API active; run links set the existing local run filter.

- [x] Add a failing test for View tasks selecting its run and keyboard-accessible explicit row actions.
- [x] Implement compact state filters, search, outcome/run selectors, question shortcut, explicit action column and clear-filter empty state from J.1/J.8.
- [x] Implement the run ledger with actual counts, parallel limits and View tasks; separate run limits from Mac slots.
- [x] Preserve search by complete task ID, outcome filtering and CLI command access.
- [x] Run Tasks/Runs and navigation tests.

### Task 3: Task detail and settings

**Files:** `ui/src/views/{TaskDetail,Settings}.tsx`, `ui/src/components/{TurnLogPanel,CommandList}.tsx`, affected tests.

**Interfaces:** Retain task mutation payload and polling refs; retain native agent settings revision, writable/model/effort/fast constraints. `Settings` may receive contextual `initialWorker` / `initialAgent` and must still work without them.

- [x] Add a failing contextual-setup selection test; existing tests continue to cover mutation/poll races and preserved drafts after failures.
- [x] Render question/review/running variants with the main action beside the title or question, result and agent-reported checks, task facts sidebar, local review commands and turn logs.
- [x] Render settings as agent sidebar + defaults editor + observed worker facts, with contextual CLI sign-in instructions and read-only project defaults.
- [x] Preserve reported checks and complete IDs/commands; do not invent unavailable per-file diff counts or turn summaries.
- [x] Run TaskDetail/Settings/API/log tests.

### Task 4: Integration and verification

**Files:** `ui/src/lib/exampleSnapshot.ts` and any isolated preview fixtures, embedded `src/dashboard/static/app/`, `src/dashboard/web.rs`, implementation handoff.

- [x] Keep example mode synthetic and isolated from live mutation endpoints; exercise shelf, task variants, settings, runs and drawer with deterministic data.
- [x] Run `npm test`, `npm run lint`, `npm run build`; run targeted Rust static asset/server tests and `cargo check` after font serving changes.
- [x] Start the frontend, inspect 1440 px screens and a narrow viewport; check keyboard focus, drawer, filters, reply, save failure, unavailable observations and reduced motion.
- [x] Compare captures against approved Paper composition and DS control corrections; fix material gaps.
- [x] Obtain an independent code review, resolve material findings, report verified scope and the branch/worktree path.

## Baseline

Worktree: `.worktrees/paper-ui`, branch `feat/paper-ui`, base `cae6c128eefb21849b02ecbdf766ded258691818`.
Baseline: 13 test files / 130 tests pass; existing React act warnings in App/Overview.
Existing mechanisms: snapshot polling, task detail/question reads, optimistic reply/accept in `ui/src/lib/api.ts` and `views/TaskDetail.tsx`; native settings revision handling in `views/Settings.tsx`; turn-log streaming in `hooks/useTurnLog.ts`. Reuse them.

## Progress

- [x] Verified stack, approved design evidence, API contract and clean functional baseline.
- [x] Created isolated worktree and reused installed dependencies.


- [x] Implemented all approved screens and preserved API behavior.
- [x] Independent review findings fixed with regression tests; 144 frontend tests and 12 dashboard HTTP tests pass.
- [x] Desktop/mobile browser verification complete; see docs/design/paper-ui-implementation.md.
