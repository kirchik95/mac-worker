# Automatic Integration — Local Implementation Record

> **Local sources:** briefs, reports and logs cited below are in `.briefs/`, an excluded working directory. They are not published repository pages. T6 results are reported evidence; T7 checks are recorded separately.

Date: 2026-10-04. Status: local documentation and asset validation draft. T6(a) and T6(b) are accepted. T6(c) is committed and under independent review. Integration features are unadvertised; no deployment or live acceptance is claimed.

Spec: [automatic integration design](../specs/2026-10-03-auto-integration-design.md). The dated D-R8 note supersedes its original disable semantics.

Plan: [automatic integration](../plans/2026-10-03-auto-integration.md), track T7. Operator reference: [Automatic integration](../../usage.md#automatic-integration).

## Identity

- **Feature:** opt-in integration of an eligible task's final imported Done result into a branch on its own canonical origin. A new merge has T,H parents; an already reachable result needs no new merge. Conflicts and verification use the same worker, agent and session. Integration is disabled by default.
- **Code revision:** T6 `a39bc0264c7b7e66da94d0313b2dcae15c5019c7`, branch `feat/ai-t6-wiring`.
- **Documentation branch:** `docs/ai-t7-integration`, starting from part 2a `e9c09c9ff99e72e83125af68712b51afb9657eb0`.
- **Local reconciliation merge:** `3588b408ea001530a881725e67f71005f39321c4`, with T7 first parent and the exact T6 revision second. No conflicts.
- **Feature gate:** `src/features.rs` defines `task.integration` and `controller.integration` but excludes both from its advertised registries at this revision. Local capable-peer fixtures prove the implementation; installed peers without support refuse enabled work with `INTEGRATION_UNAVAILABLE`.
- **Toolchain:** Rust 1.98.1, Cargo 1.98.1, Apple Git 2.50.1; Node v24.13.0, npm 11.6.2, Vite 8.2.2 on darwin-arm64.

**Test binary:** `worker 0.1.0+3588b408ea00.dirty-debug`, recorded from `target/debug/worker --version` after nextest built the merged working tree. The dirty suffix covers the T7 documentation, embedded skill and generated assets awaiting their local commits. Nextest uses the support-enabled test graph; the separate release Clippy check below validates the production graph. No release deployment artifact was produced by T7.

## T6 checkpoints and reviews

| Checkpoint | Commits | Review outcome |
| --- | --- | --- |
| T6(a), source/pin routing and host arming | `96a89cd`, `130f959`, `50ff737`; accepted fix merge `0bd088b`, selector/HTTP fixture addendum `f174b9e` | Accepted by the orchestrator; the addendum gate passed 995/995 Rust and 295/295 UI. |
| T6(b), lifecycle, pause, stop and recovery | `96eca15`, `f601b51`, `9e08d53`, `05afdf1`, `f93de5a`, `ff7fb90`, `36e7bb2` | Original implementation passed its gate, then review required fixes. Original shared disable/shutdown pause claims are superseded. |
| T6(b) fixes | `5997484`, `5177f15`, `f96d856`, `ee93daa`, `435466c`, `1d4e8ad`, `b3ee41b`, `40133de`, `c07ffb6`, `d1b5aac`, final `66d238d` | Accepted checkpoint per the part 2b handoff. Final gate passed 1040/1040 Rust and 295/295 UI. D-R8: disable pauses integration only; restart creates no pause; drain/undrain pause/resume both. |
| Separate R2-b1 source-identity fix | `76ac03b` | Fixed with a proper failing probe and 17/17 focused host tests before continuing (c). It retains stop evidence per ordinary source, including two sources at the same H. |
| T6(c), reads, Git races and legacy settlement | `1c3b036`, `1c3748e`, final `a39bc02`, plus the separate fix above | Committed; independent acceptance pending. Its once-only gate passed 1060/1061 Rust. A post-gate correction passed the affected 120/120 selection; UI passed 296/296 separately. This is not a green full gate on the final integration branch. |

Source: `.briefs/ai-t6-report.md`, all checkpoint sections, including the superseding T6(b) fixes and T6(c).

## Reported T6 verification

All Rust selections below used `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4`. Every recorded selection ran more than zero tests. These historical results were not rerun as a full gate by T7.

| Evidence | Exact command | Result and local log |
| --- | --- | --- |
| T6(a) entry points | `cargo nextest run --locked --test task --test controller -E 'test(/^task_integration_direct::|^controller_integration_wiring::/)'` | 12/12; `ai-t6a-final-entry.log` |
| Nested source stream boundary | `cargo nextest run --locked --test controller -E 'test(/^controller_integration_wiring::verified_nested_source/)'` | 1/1; `ai-t6a-nested-source-boundary.log` |
| T6(a) accepted merge gate | `sh .briefs/ai-t6-gate.sh` | 995/995 Rust, 295/295 UI; `ai-t6b-merge-gate-final.log` |
| T6(b) original gate | `sh .briefs/ai-t6-gate.sh` | 1028/1028 Rust, 295/295 UI; `ai-t6b-final-gate.log` |
| T6(b) final fixes gate | `sh .briefs/ai-t6-gate.sh` | 1040/1040 Rust, 295/295 UI; `t6b-fixes-final-gate.log` |
| Source-identity stop fix | `cargo nextest run --locked --test host -E 'test(/^task_integration_host::.*(revoke|stop|closed_repair|lost_push_reply_settles_after_legacy_close)/)'` | 17/17; `ai-t6c-r2-b1-green.log` |
| Legacy import/wait and real shared-target settlement | `cargo nextest run --locked --lib --test task -E 'test(/wait_does_not_return_the_pre_retention_block|wait_accepts_a_receipt_imported|native_legacy_retention_restore|distinct_rooted_tasks/)'` | 4/4; `ai-t6c-retention-settlement-green.log` |
| T6(c) once-only gate | `sh .briefs/ai-t6-gate.sh` | Exit 100: 1060/1061 Rust, 2871 skipped; `ai-t6c-final-gate.log`. The script stopped before UI. |
| Final affected correction | `cargo nextest run --locked --test host --test transfer --test task -E 'test(/^task_integration_host::|^task_integration_git::|^task_gc::|^task_integration_lifecycle::rooted_git_concurrency::|^task_integration_direct::(native_legacy_retention_restore|native_cancel_with_a_lost_push_reply|native_union_uses|native_dag_uses)/)'` | 120/120; `ai-t6c-post-gate-affected-green.log` |
| Final UI | `npm --prefix ui test -- --maxWorkers=4` | 296/296 in 30 files; `ai-t6c-final-ui.log` |
| Final formatting | `cargo fmt --all --check` | Exit 0; `ai-t6c-final-fmt.log` |
| Final support-enabled Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --all-targets -- -D warnings` | Exit 0; `ai-t6c-final-clippy.log` |
| Final production Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings` | Exit 0; `ai-t6c-final-release-clippy.log` |

The gate's sole Rust failure was `task_integration_host::owner_import_ack_is_retained_after_repair`: its GC fixture used a fixed expiry rather than the last repaired host-status timestamp. Correcting the fixture exposed a no-op Repair timestamp change; the final code preserves an unchanged status on an already repaired receipt. Its new idempotence assertion failed before the fix and passed in the final 120-test selection. The full gate was not repeated.

## Local proof map

| Contract | Named tests and what they prove |
| --- | --- |
| Frozen attributes, merge side order and parents | `task_integration_git::candidate_freezes_h_attributes_and_h_t_merge_orientation`, `union_uses_h_as_ours_in_mirror_and_pinned_verify_workspace`, and `task_integration_direct::native_union_uses_frozen_h_attributes_and_pinned_verification_before_receipt_import` prove H attributes, H,T merge sides, T,H parents and the identical pinned verification tree. `native_binary_h_attributes_resolve_in_the_same_session_and_verifier_index_tampering_blocks` proves same-session binary resolution and tree-mismatch refusal without origin movement. |
| Exact-T lease and fast-forward-only push | `task_integration_git::fast_forwardable_task_still_gets_one_merge_with_exact_lease`, `movement_to_an_ancestor_of_h_rejects_the_old_lease_and_rebuilds`, `server_cas_after_advertisement_is_movement_and_rebuilds`, and `rooted_owner_rebuilds_the_exact_lease_before_and_after_native_advertisement` prove the single-ref explicit lease and rebuilds before/after advertisement. |
| Serialization and driver cap | `task_integration_direct::native_rpc_leader_and_direct_recovery_share_the_detached_git_driver`; `task_integration_lifecycle::rooted_git_concurrency::{rooted_owner_real_git_fences_concurrent_cycle_drivers, distinct_rooted_tasks_share_one_canonical_target_and_preserve_both_git_results, four_real_git_targets_hold_the_shared_cap_and_resolution_releases_it_for_ready_work}` prove one live actor, preserved task ancestry, four independent Git drives and reservation release for queued agent work. |
| Crash replay, lost replies and stop authority | `task_integration_git::{workspace_merge_crash_replays_before_any_auxiliary_admission, resolution_tree_is_frozen_before_commit_and_cannot_rebind_after_crash, clean_candidate_crashes_keep_one_manifest_and_one_merge_oid}`; `task_integration_lifecycle::owner_and_host_boundary_crashes_preserve_ids_counters_and_one_publication`; `task_integration_host::{lost_push_reply_keeps_the_merged_receipt_through_real_owner_fetch_repair_and_import, revoke_journal_and_ack_crashes_never_restore_push_authority, source_retirement_stop_is_durable_before_intent_staging_and_recovery, stale_prephase_revoke_preserves_the_newer_epoch_fence_across_crash_replay, prephase_revoke_rejects_other_ids_and_sources_and_survives_gc_replay}`. |
| Paused budgets and running timeout | `integration::runner::native_launch_tests::{queued_auxiliary_keeps_eight_active_minutes_and_its_position_after_restart, queued_auxiliary_history_pruning_is_exact_below_cap_and_conservative_above_it, native_helper_pause_after_undrain_preserves_only_the_active_remainder}` prove exact retained remainder/ID/queue position/spend below the history cap, helper handoff and conservative expiry after lost history. `task_integration_direct::a_running_auxiliary_times_out_while_the_native_owner_gate_is_drained` proves the running execution budget remains active. The 1,000-fsync nightly stress stays ignored; the fast cap-crossing test covers bounded history in the gate. |
| Compact facts and peer compatibility | `controller_integration_contracts::{typed_compact_annotation_is_retained_while_disabled_facts_keep_their_bytes, facts_decoder_refuses_an_invalid_or_oversized_annotation, rust_loads_the_same_public_views_codes_and_annotation_boundaries_as_typescript, public_target_is_redacted_and_utf8_bounded_with_ellipsis}` cover the 512-byte annotation, 2,048-byte compound facts and 255-byte exact/128-byte display boundaries. `controller_integration_events::{compound_title_budget_never_truncates_annotation_and_decoder_reserves_bound, shared_compound_facts_fixture_checks_decoder_boundary_before_tolerant_normalization}` prove both compound guards; `controller_integration_contracts::copied_baseline_strict_decoders_prove_disabled_dtos_keep_their_bytes` proves the copied baseline strict codecs. |
| Notification truth and dashboard actions | `controller_integration_wiring::native_notifier_repairs_dropped_hints_confirms_revisions_and_deduplicates_across_restart` confirms full companion state after dropped hints and deduplicates per epoch. App test `enables blocked integration actions in Overview after a confirmed preview loads` proves Overview Re-drive and Close availability. Native direct/controller dashboard tests use revision-bound re-drive. `skills::tests` proves generated `task integrate` grammar. |
| Legacy seven-idle-day GC settlement | `task_integration_direct::native_legacy_retention_restore_observes_m_or_h_and_imports_without_resurrecting_closed_work` applies copied baseline retention selection and real non-discard close at deadline−1/deadline+1. Reachable M imports M; otherwise reachable H settles already integrated and imports observed T; neither blocks workspace-missing; failed observation retains uncertainty and bounded Repair/Network backoff. Host tests `closed_repair_settles_a_retained_merge_without_workspace_or_push_authority`, `closed_repair_settles_only_the_source_with_an_already_integrated_receipt`, `closed_repair_blocks_when_neither_retained_merge_nor_source_is_on_origin`, and `closed_repair_failed_observation_retains_uncertainty_and_retries_safely` confirm the same contract. Closed persists, result refs remain, no push/prepare/workspace/auxiliary returns, and terminal integrate refuses. |

These proofs use disposable file-origin repositories, fake agents/transports, isolated stores and recording notification channels. Copied baseline GC and codecs establish those fixture semantics; they do not prove installation or deployment of an older helper.

## T7 checks and assets

Fresh checks ran on the merged T7 working tree with its reconciled embedded skill and rebuilt assets. Rust commands used four build jobs and four test threads, serially. Logs below are local `.briefs/ai-t7-2b-*.log` files.

| Check | Exact command | Result | Log suffix |
| --- | --- | --- | --- |
| UI tests | `npm --prefix ui test -- --maxWorkers=4` | 296/296 tests, 30 files | `ui-tests` |
| UI lint | `npm --prefix ui run lint` | Exit 0; 19 existing warnings, no errors | `ui-lint` |
| UI typecheck | `cd ui && ./node_modules/.bin/tsc -b --noEmit` | Exit 0 | `ui-typecheck` |
| Production assets | `npm --prefix ui run build` | Exit 0; `tsc -b && vite build`, Vite 8.2.2 | `ui-build` |
| Temporary assets | `npm --prefix ui run build -- --outDir .parity-dist` | Exit 0; same production settings | `ui-parity-build` |
| Full asset parity | `diff -r src/dashboard/static/app ui/.parity-dist` | Exit 0; all 6 files byte-identical, 1,733,277 bytes | `ui-parity` |
| Docs site | `npm --prefix docs-site run build` | Exit 0; 98 prepared source files, VitePress 1.6.4 | `docs-build-final` |
| Site links and anchors | `python3 docs-site/check-links.py` | 91 pages, 7,896 internal links/assets; zero broken links | `docs-links-final` |
| Formatting | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo fmt --all --check` | Exit 0 | `fmt` |
| Support-enabled Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --all-targets -- -D warnings` | Exit 0 after final prose and skill wording | `clippy-final` |
| Production Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings` | Exit 0 after final skill wording | `release-clippy-final` |
| Embedded skill | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --lib -E 'test(/^skills::/)'` | 6/6; 1,040 skipped | `skills-lib` |
| CLI skill help | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --test cli -E 'test(/skills/)'` | 1/1; 85 skipped | `skills-cli` |
| Embedded assets and loopback web routes | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --test dashboard -E 'test(/^dashboard_web::/)'` | 11/11; 207 skipped | `dashboard-web` |

`python3 .briefs/scanner_guard.py target/debug/worker` exited 0 before each Rust selection: fresh executions 1.08 s, 0.95 s and 0.96 s; no HUNG. The sandbox blocked CPU sampling for the first two guards; the decisive fresh-execution check passed. The dashboard selection used isolated loopback sockets with approved sandbox escalation; its guard sampled CPU 0%/0%. The installed grammar command `target/debug/worker skills get pool-dispatch --grammar-only` includes `Usage: worker task integrate [OPTIONS] <TASK_ID>`.

Production and parity builds emitted the same large-chunk advisory. The complete six-file SHA-256 manifest is in `.briefs/ai-t7-2b-assets.json`; `.parity-dist` was removed. Docs output was checked and its generated dist removed; prepared content is ignored. All 35 provisional comments are gone, the 70 frozen error-table rows are unchanged, and historical manual-merge constraints retain their dated supersession notes.

## Live status and risks

T8 has not run. It waits for the owner's acceptance-origin and pool authorization. Its checklist is in the [plan's T8 section](../plans/2026-10-03-auto-integration.md#t8--separately-authorized-disposable-origin-live-acceptance). No real pool, SSH host, service, keychain, notification channel, push or deployment was used by T7.

- A rollback below `task.integration` can lose the repair workspace after seven idle host-status days; already-idle tasks can expire sooner than seven days after rollback.
- Lost or capped pause history may conservatively end an auxiliary admission budget early. It cannot enlarge that budget.
- Branch permissions, merge/signing rules, Git version and worker write access still need live checks. Canonical origin strings do not prove SSH/HTTPS repository equivalence; independent owners rely on the lease.
- Unknown push/stop outcomes remain uncertain. A retained Closed task only settles existing effects; it cannot re-drive a new integration.
- Independent T6(c) review may change documented behavior. Whether Closed-task observation runs during drain or disable remains under review; operator docs make no scheduling claim.
- The final integration-branch full gate remains with the owner; the T6 post-gate affected selection and T7 focused checks do not replace it.

PENDING: T6(c) acceptance and independent review outcome for `a39bc02`.

PENDING: Feature advertisement commit after T6(c) acceptance, with its nonzero affected help/features checks.

PENDING: Full gate on `integ/auto-integration`, with final commit, exact command, counts and outcome.

PENDING: Main merge identity and outcome.

PENDING: T8 owner authorization, disposable acceptance origin, approved hosts and live results.
