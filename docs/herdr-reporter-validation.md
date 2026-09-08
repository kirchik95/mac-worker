# Herdr reporter validation record

Date: 2026-09-09. Branch `feat/herdr-reporter`, protocol 6. Three Mac minis (`mini-1`, `mini-2`, `mini-3`) running herdr 0.9.0 servers; `herdr = true` on `mini-1` only. Tasks were submitted from the laptop inside a herdr pane, so the notifier targeted the operator's live herdr session. Task ids are shortened, paths and prompts are omitted, as the acceptance runbook requires.

## Gate

`cargo fmt --all --check`, `cargo clippy --locked --offline --all-targets -- -D warnings`, and `cargo test --locked --offline --all-targets --no-fail-fast` on the branch: 1688 tests in 73 suites, none failed.

## Setup and facts (spec 13.2 item 7)

`worker setup mini-1 mini-2 mini-3` installed protocol 6 on every worker. `mini-3` failed its first verification probe on the 15 s deadline and installed on the retry; its `refresh-facts` takes about 6.8 s on the old helper against 4.3 s on `mini-1` with the new one, so the herdr checks are not what pushed it over. `worker workers` and `worker doctor` then printed for every worker:

```text
herdr: available (0.9.0)
```

No `HERDR_UNAVAILABLE` warning, as expected with every server up.

## A done turn, start to close (items 1, 2, 4)

Task `ee73c942913f`, Codex, pinned to `mini-1`, told to sleep 60 seconds and finish `done`.

- The task was `active` on `mini-1` six seconds after submission. Twelve seconds later the worker's herdr showed a `mac-worker` workspace, a tab `task ee73c942913f · turn 1`, and an agent `codex` in state `working` with title `task ee73c942913f · herdr check T1`, `display_agent` `mac-worker`, token `mw_outcome = running`.
- The pane ran `worker host follow-turn` with identifiers only in its argv and rendered the turn's events: the session line and the agent's first message. The pane title was the task title.
- After the turn ended the same agent showed `done` (herdr's rendering of `idle` until seen) with `mw_outcome = done`. `worker task status --json` recorded the turn as `herdr: {state: attached, pane_id: w4:p2}`.
- `worker task close` removed the tab and the agent; the `mac-worker` workspace stayed, empty.
- The turn's `supervisor.log` was never created: the reporter left no diagnostic for an attached turn.

## What the second task revealed (not the reporter)

Task `a9b9f216fedd`, submitted to `mini-1` right after the first closed, was accepted by the worker but its detached supervisor failed before it wrote its identity, so the laptop runner ended with `SUPERVISOR_HANDSHAKE_TIMEOUT`, the task stayed `active`, and the worker kept the heavy lease. The reporter runs only after the durable `Running` handoff, which this turn never reached; its first job record shows no herdr report and its `supervisor.log` holds only `error_code=PROTOCOL`. Two pre-existing behaviours made recovery harder and are recorded here for the reliability work:

1. `validate_indexed_turn_prelaunch_job` refuses a job whose `supervisor.log` is not empty. The first failed attempt writes `error_code=PROTOCOL` there, so every later `worker host supervise` of the same job fails prelaunch validation with a new protocol error, and the job can never launch again without clearing the file. Truncating it by hand did not help either: the next attempt exited `70` without writing anything.
2. The supervisor's public diagnostics are content-free by design, so the first protocol error's message is lost; `error_code=PROTOCOL` is all that survives.

`worker task cancel` released the lease and left the task `open` with outcome `cancelled`; `worker task close` then removed it. Separately, a pinned `submit` twenty-five minutes after the last facts refresh was refused with `CAPABILITY_MISSING` until `worker workers --refresh`, the stale-facts behaviour already on the backlog.

Follow-up on 2026-09-09: both traps and the stale-facts refusal are fixed on `main`. `supervisor.log` now carries the redacted message beside the code, the prelaunch checks only bound the log so a retry can launch, the runner's early-exit line names the real error, and a pinned `submit` refreshes stale facts before judging capabilities.

## needs_input, a follow-up, and close (items 3 and 4)

Task `001fa47a42ad`, Codex, pinned to `mini-1`, told to finish `needs_input` with one question.

- Twelve seconds after submission the task was `open` with outcome `needs_input` and the question recorded; the worker's herdr showed tab `task 001fa47a42ad · turn 1` and the agent in state `blocked` with `mw_outcome = needs_input`. The turn record: `herdr: {state: attached, pane_id: w4:p4}`.
- `worker task say … --wait` started turn 2. Afterwards the workspace held `task 001fa47a42ad · turn 2` and no `turn 1` tab: the new turn's start swept the previous one. The agent showed `done` with `mw_outcome = done`, and the record carried both turns as `attached` with their own pane ids (`w4:p4`, `w4:p6`).
- `worker task close` removed the task's tab and agent.

## A cancelled turn (item 5, by way of the orphaned task)

`worker task cancel` on the orphaned task `a9b9f216fedd` went through the host cancel path and the same terminal hook. The reporter, finding no pane from a start it never made, opened `task a9b9f216fedd · turn 1` and reported `unknown` with `mw_outcome = cancelled`, exactly the row the design chose for turns that end without a result. `worker task close` removed it. After both closes the `mac-worker` workspace on `mini-1` held only its default tab and no agents.

## Not exercised

- Item 6 (herdr stopped on a worker) was not run against the operator's live herdr server; the automated tests cover the `unavailable` path with an empty HOME, and the `HERDR_UNAVAILABLE` warning with a fake fact.
- What the MacBook shows is the operator's observation, not something the worker can prove. The operator confirmed both for the done task: the `task ee73c942913f · turn 1` tab appeared under `Mac #1` in the herdr sidebar, and the `task ee73c942913f: done` notification arrived on the laptop.

## Privacy (item 8)

Every request the reporter sent in the automated suites is checked for paths, prompt text, and profile values; the live runs' `task status --json` records carried only `herdr: {state, pane_id}` per turn, and the pane's command line carried the three identifiers.
