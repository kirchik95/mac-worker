# Session Transfer — T0 Live Spike

> **Local sources:** the full spike reports, with exact commands, output excerpts and cleanup ledgers, are working briefs kept outside the repository (`.briefs/st-t0-claude-report.md` and `.briefs/st-t0-codex-report.md`).

Date: 2026-10-03.

Spec: [2026-10-03-session-transfer-design.md](../specs/2026-10-03-session-transfer-design.md), Round 2 items 1–5 and 17.

Executors: pi agents `st-t0-claude` and `st-t0-codex`. The orchestrator created the interactive source sessions in herdr panes.

**Environment:**
- laptop: Claude Code 2.1.288, codex-cli 0.160.0;
- mini-3 (`mac3`): Claude Code 2.1.285, codex-cli 0.159.3.

**Method:** throwaway repositories and sessions only. Destinations were host-shaped workspaces (`~/.local/share/mw-spike-*/host/tasks/<64 hex>/<32 hex>/workspace`), never the pool's host tree. Each transcript was copied, the checkout root was rewritten at path boundaries, the id was replaced everywhere with a fresh UUID, the file was placed in the native store, and the session was resumed with the pool's exact resume argv, with the prompt on stdin. Every created thread, transcript and directory was deleted afterwards, including the orchestrator's interactive sources.

## Claude Code

| # | Experiment | Result |
| --- | --- | --- |
| — | Pool argv as in `src/agent/claude.rs` | **Fails before inference:** `When using --print, --output-format=stream-json requires --verbose`. Pre-existing pool bug, fixed by F1 |
| S1 | Interactive source (2 edit turns + 1 subagent turn), v4 id, argv + `--verbose` | Pass: recalled both words; init `session_id` = placed id; appended in place; no new transcript |
| S1 | Headless source | Pass |
| S2 | Sidecar `<id>/subagents/agent-*.jsonl` + `.meta.json` present vs omitted | Pass both ways for main-conversation recall. The subagent JSONL embeds the session id and paths; `meta.json` holds neither |
| S3 | Project-dir encoding, 162- and 252-character paths | Pass 2/2, including the hash suffix for long paths. The minis have no `~/.claude/projects` (the pool has never completed a Claude turn), so there were no mini samples |
| S6 | Fake `sk-ant-…` token replaced by `[scrubbed]` (4 occurrences) | Pass: the model sees only `[scrubbed]` |
| S8 | Second hop of an already-resumed transcript to a third path with a new id | Pass |
| — | Lookup with the transcript under a different project directory | Pass. Claude finds the session by UUID anywhere under `projects/` and appends in place. Canonical placement is still used |

**Paths occur in many fields:** `cwd`, `attachment.snapshot.*`, `attachment.systemPrompt[]`, `message.content[].input.file_path`, `toolUseResult.*`, `wireToolInputs.<tool id>.*`, `serverClassifierContext.context.git_state.*` and others. A resumed structured answer may restate historical `files_changed`, so the pool diff, not the agent's self-report, is authoritative.

## Codex

| # | Experiment | Result |
| --- | --- | --- |
| S4a | Headless and interactive (`codex-tui`) sources, v4 id, file-only placement at `sessions/YYYY/MM/DD/rollout-<ts>-<id>.jsonl`, pool resume argv and policy flags | Pass: recalled; `thread.started.thread_id` = placed id; appended in place. Codex filled `state_5.sqlite` and `thread_history_1.sqlite` itself |
| S4b | v7 id | Pass |
| S4c | Laptop 0.160.0 → mini 0.159.3, headless and interactive | Pass; the only warning was about the model recorded vs the model resuming |
| S5 | Different `creator_account_id` and `creator_user_id` on mini-3 | Pass. Codex source: no creator check on resume |
| S6 | Scrubbed fake token | Pass |
| S8 | Second hop | Pass |
| — | `codex delete --force <id>` | Removes the rollout and every database row; empty date directories remain |

**Codex source findings** (`rust-v0.160.0`, unchanged in the cited files since `rust-v0.159.2`):
- `exec resume` resolves a thread by live writer, then the state DB, then a filename scan (`thread-store/src/local/thread_rollout_resolver.rs:68-155`, `rollout/src/list.rs:1415-1578`).
- `ThreadId` accepts any UUID version.
- There is no native rollout import command.

**Absolute paths occur in:** `payload.cwd`, `runtime_workspace_roots`, `workspace_roots`, `permission_profile…path`, `thread_settings.*`, `item.aggregated_output`, `item.command[]`, `base_instructions.text` and others.

## Decisions taken from the spike
- **Placement:** file-only for both agents. No database, index or import step.
- **Claude adapter:** gains `--verbose` (F1).
- **Rewriting:** whole-text token rewriting over every package file, including the Claude sidecar.
- **Version gate:** at most one minor behind within the same major (spec Round 2 item 17).
- **Still open, for T8:** large transcripts (S7); a Claude turn on a mini through the pool, once F1 is deployed.
