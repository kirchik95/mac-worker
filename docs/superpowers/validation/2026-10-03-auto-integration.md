# Automatic Integration — Local Implementation Record

> **Local sources:** briefs, reports and logs cited below are in `.briefs/`, an excluded working directory. They are not published repository pages. T6 results are reported evidence; T7 checks are recorded separately.

Date: 2026-10-04. Status: local documentation and asset validation record. The owner accepts T6(a), T6(b), T6(c) and its review fixes. Integration features are advertised in the locally committed release code; no deployment or live acceptance is claimed. The advertisement gate was not green on its single run; its failed selections and the skipped UI command were rerun separately.

Spec: [automatic integration design](../specs/2026-10-03-auto-integration-design.md). The dated D-R8 note supersedes its original disable semantics.

Plan: [automatic integration](../plans/2026-10-03-auto-integration.md), track T7. Operator reference: [Automatic integration](../../usage.md#automatic-integration).

## Identity

- **Feature:** opt-in integration of an eligible task's final imported Done result into a branch on its own canonical origin. A new merge has T,H parents; an already reachable result needs no new merge. Conflicts and verification use the same worker, agent and session. Integration is disabled by default.
- **Code revision:** final T6 snapshot `3134f718ba420c3eedad9a18687834b0d61f2258`, branch `feat/ai-t6-wiring`; this is the feature-advertisement commit and the head captured when Part 2c started.
- **Documentation branch:** `docs/ai-t7-integration`; Part 2c starts from Part 2b `ee76b8e46a40e7d17280109e8f853ecefcbf1761`.
- **Final local reconciliation merge:** `3b1d1c6b0d453d3c81bcb923c22bf29a8541df21`, with `ee76b8e` first parent and the exact `3134f71` second parent. No conflicts. The earlier Part 2b merge `3588b408ea001530a881725e67f71005f39321c4` pinned T6 at `a39bc02`.
- **Feature gate:** `src/features.rs` advertises `task.integration` in host probes and `controller.integration` in controller health/status and ready-service hello. There is no `worker features` subcommand. Installed peers without the required feature refuse enabled work with `INTEGRATION_UNAVAILABLE`, with no ordinary fallback. This records release capability, not deployment to the pool.
- **Toolchain:** Rust 1.98.1, Cargo 1.98.1, Apple Git 2.50.1; Node v24.13.0, npm 11.6.2, Vite 8.2.2 on darwin-arm64.

**Part 2b test binary:** `worker 0.1.0+3588b408ea00.dirty-debug`, recorded from `target/debug/worker --version` after nextest built the merged working tree. The dirty suffix covers the T7 documentation, embedded skill and generated assets awaiting their local commits. Nextest uses the support-enabled test graph; the separate release Clippy check below validates the production graph. No release deployment artifact was produced by T7.

**Part 2c test binary:** `worker 0.1.0+3b1d1c6b0d45.dirty-debug`, recorded after the final skill/CLI/dashboard selections built the captured merge with the reconciled documentation and embedded skill awaiting their local commit.

## T6 checkpoints and reviews

| Checkpoint | Commits | Review outcome |
| --- | --- | --- |
| T6(a), source/pin routing and host arming | `96a89cd`, `130f959`, `50ff737`; accepted fix merge `0bd088b`, selector/HTTP fixture addendum `f174b9e` | Accepted by the orchestrator; the addendum gate passed 995/995 Rust and 295/295 UI. |
| T6(b), lifecycle, pause, stop and recovery | `96eca15`, `f601b51`, `9e08d53`, `05afdf1`, `f93de5a`, `ff7fb90`, `36e7bb2` | Original implementation passed its gate, then review required fixes. Original shared disable/shutdown pause claims are superseded. |
| T6(b) fixes | `5997484`, `5177f15`, `f96d856`, `ee93daa`, `435466c`, `1d4e8ad`, `b3ee41b`, `40133de`, `c07ffb6`, `d1b5aac`, final `66d238d` | Accepted checkpoint per the part 2b handoff. Final gate passed 1040/1040 Rust and 295/295 UI. D-R8: disable pauses integration only; restart creates no pause; drain/undrain pause/resume both. |
| Separate R2-b1 source-identity fix | `76ac03b` | Fixed with a proper failing probe and 17/17 focused host tests before continuing (c). It retains stop evidence per ordinary source, including two sources at the same H. |
| T6(c), reads, Git races and legacy settlement | `1c3b036`, `1c3748e`, final `a39bc02`, plus the separate fix above | Original `a39bc02` review verdict was `accept after fixes`; the owner now accepts the corrected T6(c) behavior. Its original once-only gate returned exit 100 with 1060/1061 Rust passing. A post-gate correction passed the affected 120/120 selection; UI passed 296/296 separately. This is not a green full gate on the final integration branch. |

Source: `.briefs/ai-t6-report.md`, including T6(c) fixes, Round 2, Round 3: shared quota and Advertise. Archived review files were read from `../ai-t6/.briefs/` without editing that worktree; they are local evidence, not published links.


| Final T6 stage | Commits | Review and verification outcome |
| --- | --- | --- |
| R3-b1 follow-up/source fence | `15f357b` | Accepted in the owner handoff and not reopened by later archived reviews. Final affected 284/284; pre-intent Say, supersession and same-source stop evidence retained. |
| Frozen ce7f62f codecs | `ac91b3e`, merge `eda7e9d` | The original review found shape approximations; the rereview confirms actual independent transitive codecs/custom validation. The old-enum guard and 30 controller contract tests pass. |
| Findings 1–5 and native split | `c728aa9`, `bb333af`, `d21c64d`, `0395a88`, `9db7e59`, `f65c3b6` | Author final affected 174/174. Rereview at `f65c3b6` says `accept after fixes`: separate Closed budget/reservation, frozen codecs and seven split schedules pass; advisory recovery, interrupted no-op replay, storage lifetime and event read cost need the next round. |
| Round 2 | `b5e577d`, `6061696`, `024fa0b`, `12c6803`, `10c6b03` | Author affected 163/163 plus native/warm 19/19; independent 191/191. Archived verdict `accept after fixes`: findings 1/2/4 and warm-up proof pass; combined-family quota remains a Medium pre-advertise requirement. Dashboard-only accounting is superseded. |
| Round 3 shared quota | `5a0f213` | Author affected 78/78; independent 78/78 plus 4/4 extra quota probes. Combined count/byte/age policy, paired admission, pending protection, Closed replay and cleanup pass. Archived verdict `accept after fixes` retains one Low deferred-hint release boundary; the Part 2c handoff accepts final operator behavior and notes a possible later hint-scope-only fix. |
| Advertisement | `3134f718ba420c3eedad9a18687834b0d61f2258` | Committed locally after the accepted behavioral fixes. Focused 153/153; once-only broad Rust gate failed, with passing failed-case reruns and UI separately. Exact outcomes follow. |

Archived reviews: `t6c-review-report.md` at `a39bc02` (`accept after fixes`, six findings); `t6c-rereview-report.md` at `f65c3b6` (`accept after fixes`, four remaining gaps); `t6c-rereview-r2-report.md` at `10c6b03` (`accept after fixes`, combined-family quota); and `t6c-rereview-r3-report.md` at `5a0f213` (`accept after fixes`, shared quota fixed, Low hint boundary). The acceptance statement is the owner's Part 2c handoff; these archived conditional verdicts are preserved rather than described as clean reviews. No hint-scope fix is included in the captured merge snapshot.

## Reported T6 verification

All Rust selections below used `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4`. Every recorded selection ran more than zero tests. These historical results were not rerun as a full gate by T7.

| Evidence | Exact command | Result and local log |
| --- | --- | --- |
| T6(a) entry points | `cargo nextest run --locked --test task --test controller -E 'test(/^task_integration_direct::\|^controller_integration_wiring::/)'` | 12/12; `ai-t6a-final-entry.log` |
| Nested source stream boundary | `cargo nextest run --locked --test controller -E 'test(/^controller_integration_wiring::verified_nested_source/)'` | 1/1; `ai-t6a-nested-source-boundary.log` |
| T6(a) accepted merge gate | `sh .briefs/ai-t6-gate.sh` | 995/995 Rust, 295/295 UI; `ai-t6b-merge-gate-final.log` |
| T6(b) original gate | `sh .briefs/ai-t6-gate.sh` | 1028/1028 Rust, 295/295 UI; `ai-t6b-final-gate.log` |
| T6(b) final fixes gate | `sh .briefs/ai-t6-gate.sh` | 1040/1040 Rust, 295/295 UI; `t6b-fixes-final-gate.log` |
| Source-identity stop fix | `cargo nextest run --locked --test host -E 'test(/^task_integration_host::.*(revoke\|stop\|closed_repair\|lost_push_reply_settles_after_legacy_close)/)'` | 17/17; `ai-t6c-r2-b1-green.log` |
| Legacy import/wait and real shared-target settlement | `cargo nextest run --locked --lib --test task -E 'test(/wait_does_not_return_the_pre_retention_block\|wait_accepts_a_receipt_imported\|native_legacy_retention_restore\|distinct_rooted_tasks/)'` | 4/4; `ai-t6c-retention-settlement-green.log` |
| T6(c) once-only gate | `sh .briefs/ai-t6-gate.sh` | Exit 100: 1060/1061 Rust, 2871 skipped; `ai-t6c-final-gate.log`. The script stopped before UI. |
| Final affected correction | `cargo nextest run --locked --test host --test transfer --test task -E 'test(/^task_integration_host::\|^task_integration_git::\|^task_gc::\|^task_integration_lifecycle::rooted_git_concurrency::\|^task_integration_direct::(native_legacy_retention_restore\|native_cancel_with_a_lost_push_reply\|native_union_uses\|native_dag_uses)/)'` | 120/120; `ai-t6c-post-gate-affected-green.log` |
| Final UI | `npm --prefix ui test -- --maxWorkers=4` | 296/296 in 30 files; `ai-t6c-final-ui.log` |
| Final formatting | `cargo fmt --all --check` | Exit 0; `ai-t6c-final-fmt.log` |
| Final support-enabled Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --all-targets -- -D warnings` | Exit 0; `ai-t6c-final-clippy.log` |
| Final production Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings` | Exit 0; `ai-t6c-final-release-clippy.log` |

The gate's sole Rust failure was `task_integration_host::owner_import_ack_is_retained_after_repair`: its GC fixture used a fixed expiry rather than the last repaired host-status timestamp. Correcting the fixture exposed a no-op Repair timestamp change; the final code preserves an unchanged status on an already repaired receipt. Its new idempotence assertion failed before the fix and passed in the final 120-test selection. The full gate was not repeated.

## Reported advertisement gate and corrections

These are T6 report results, not fresh T7 runs. The full advertisement gate was invoked exactly once and was **not green**. It stopped before UI and was not repeated.

| Exact command | Recorded result | Local T6 log |
| --- | --- | --- |
| `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --offline --lib -E 'test(/^features::/)'` | Registry red 1/1 failed, then green 1/1 passed | `ai-t6-advertise-registry-{red,green}.log` |
| `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --offline --no-fail-fast --lib --test cli --test controller --test task --test scheduler --test dashboard --test setup -E 'test(/^features::\|^cli_help::\|^controller_features::\|^controller_health\|^controller_lifecycle_compat::\|^controller_integration_wiring::\|^controller_integration_contracts::\|^probe::tests::collection_normalizes_arch_and_uses_only_the_controlled_path$\|^controller_socket_wiring::t7a::(leader_image_readiness_publication_and_feature_shutdown\|feature_absent_on_unsafe_bind_image_and_record_with_stdio_healthy)$\|^task_client::(session_submission_tests::\|read_cost_tests::)\|^task_integration_direct::(disabled_\|copied_baseline_\|native_read_surfaces_expose_\|direct_missing_helper_\|a_brief_helper_rollback_\|native_rpc_leader_)\|^session_eligibility::\|^dashboard_settings::\|^doctor_command::doctor_output_/)'` | 153/153 across seven binaries, 3,253 skipped; 67.981 s | `ai-t6-advertise-focused-final.log` |
| `CARGO_NET_OFFLINE=true sh .briefs/ai-t6-gate.sh` | **1,103 Rust tests: 1,096 passed (4 slow), 6 failed, 1 timed out; 2,896 skipped; 760.919 s; exit 100. UI skipped by set -e.** | `ai-t6-advertise-gate.log` |
| `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --offline --no-fail-fast --test task -E 'test(/^task_integration_direct::(fresh_operator_reconcile_confirms_dead_integration_driver_and_retained_actor\|native_close_revokes_a_parked_cycle_without_starting_a_git_phase\|native_binary_h_attributes_resolve_in_the_same_session\|native_dag_uses_the_imported_parent_merge_for_its_configured_child\|native_explicit_redrive_advances_one_blocked_epoch_and_recovers_via_reexec\|native_interrupted_say_waits_for_revoke_then_queues_one_ordinary_turn\|native_cancel_with_a_lost_push_reply_imports_the_committed_success_before_close)$/)'` | First seven-case correction: 4/7 passed; 124.565 s | `ai-t6-advertise-gate-correction.log` |
| `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --offline --no-fail-fast --lib --test task -E 'test(/^task_integration_direct::(fresh_operator_reconcile_confirms_dead_integration_driver_and_retained_actor\|native_close_revokes_a_parked_cycle_without_starting_a_git_phase\|native_binary_h_attributes_resolve_in_the_same_session\|native_dag_uses_the_imported_parent_merge_for_its_configured_child\|native_explicit_redrive_advances_one_blocked_epoch_and_recovers_via_reexec\|native_interrupted_say_waits_for_revoke_then_queues_one_ordinary_turn\|native_cancel_with_a_lost_push_reply_imports_the_committed_success_before_close\|disabled_.*\|copied_baseline_.*\|native_read_surfaces_expose_the_same_parked_companion_without_driving_git\|direct_missing_helper_feature_refuses_before_pins_and_admission)$\|^features::\|^probe::tests::(cached_tool_capabilities_skip_version_probes_and_round_trip\|collection_normalizes_arch_and_uses_only_the_controlled_path)$\|^task_client::read_cost_tests::/)'` | After cache seeding: 17 tests, 15 passed (3 slow), 2 failed; 1,779 skipped; 166.388 s | `ai-t6-advertise-gate-correction-final.log` |
| `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --offline --no-fail-fast --test task -E 'test(/^task_integration_direct::(native_binary_h_attributes_resolve_in_the_same_session\|native_dag_uses_the_imported_parent_merge_for_its_configured_child)$/)'` | Final remaining failures: 2/2 passed; 713 skipped; 79.947 s | `ai-t6-advertise-final-two.log` |
| `npm --prefix ui test -- --maxWorkers=4` | Separate skipped UI command: 296/296 tests in 30/30 files; 8.32 s; exit 0 | `ai-t6-advertise-ui.log` |

All seven gate failures were in `task_integration_direct`: `fresh_operator_reconcile_confirms_dead_integration_driver_and_retained_actor`, `native_close_revokes_a_parked_cycle_without_starting_a_git_phase`, `native_binary_h_attributes_resolve_in_the_same_session`, `native_dag_uses_the_imported_parent_merge_for_its_configured_child`, `native_explicit_redrive_advances_one_blocked_epoch_and_recovers_via_reexec`, `native_interrupted_say_waits_for_revoke_then_queues_one_ordinary_turn`, and `native_cancel_with_a_lost_push_reply_imports_the_committed_success_before_close` (180-second nextest timeout).

The fixture correction restores the accepted SSH warm-up for both capable and old peers and seeds the existing private Git-tool cache. No production fix, deadline increase or assertion removal was made. Five original failures passed in the 17-test rerun; attributes and DAG passed in the final two-test rerun. An intervening diagnostic-only attributes run passed 1/1 in 75.086 s (`ai-t6-advertise-probe-diagnostic.log`); temporary diagnostics were removed. Every original failure therefore has a passing rerun, while the single broad gate remains failed. Native process deadlines remain load-sensitive; future broad-run stability is not established.

The final exact formatting/Clippy commands below all exited 0; logs are `ai-t6-advertise-fmt.log`, `ai-t6-advertise-clippy.log` (21.46 s) and `ai-t6-advertise-release-clippy.log` (17.95 s).

```sh
CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo fmt --all --check
CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --offline --all-targets -- -D warnings
CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --offline --release --no-default-features --lib --bin worker -- -D warnings
```

## Local proof map

| Contract | Named tests and what they prove |
| --- | --- |
| Frozen attributes, merge side order and parents | `task_integration_git::candidate_freezes_h_attributes_and_h_t_merge_orientation`, `union_uses_h_as_ours_in_mirror_and_pinned_verify_workspace`, and `task_integration_direct::native_union_uses_frozen_h_attributes_and_pinned_verification_before_receipt_import` prove H attributes, H,T merge sides, T,H parents and the identical pinned verification tree. `native_binary_h_attributes_resolve_in_the_same_session` and `native_verifier_index_tampering_blocks_before_push` separately prove same-session binary resolution and tree-mismatch refusal without origin movement. |
| Exact-T lease and fast-forward-only push | `task_integration_git::fast_forwardable_task_still_gets_one_merge_with_exact_lease`, `movement_to_an_ancestor_of_h_rejects_the_old_lease_and_rebuilds`, `server_cas_after_advertisement_is_movement_and_rebuilds`, and `rooted_owner_rebuilds_the_exact_lease_before_and_after_native_advertisement` prove the single-ref explicit lease and rebuilds before/after advertisement. |
| Serialization and driver cap | `task_integration_direct::native_rpc_leader_and_direct_recovery_share_the_detached_git_driver`; `task_integration_lifecycle::rooted_git_concurrency::{rooted_owner_real_git_fences_concurrent_cycle_drivers, distinct_rooted_tasks_share_one_canonical_target_and_preserve_both_git_results, four_real_git_targets_hold_the_shared_cap_and_resolution_releases_it_for_ready_work}` prove one live actor, preserved task ancestry, four independent Git drives and reservation release for queued agent work. |
| Crash replay, lost replies and stop authority | `task_integration_git::{workspace_merge_crash_replays_before_any_auxiliary_admission, resolution_tree_is_frozen_before_commit_and_cannot_rebind_after_crash, clean_candidate_crashes_keep_one_manifest_and_one_merge_oid}`; `task_integration_lifecycle::owner_and_host_boundary_crashes_preserve_ids_counters_and_one_publication`; `task_integration_host::{lost_push_reply_keeps_the_merged_receipt_through_real_owner_fetch_repair_and_import, revoke_journal_and_ack_crashes_never_restore_push_authority, source_retirement_stop_is_durable_before_intent_staging_and_recovery, stale_prephase_revoke_preserves_the_newer_epoch_fence_across_crash_replay, prephase_revoke_rejects_other_ids_and_sources_and_survives_gc_replay}`. |
| Paused budgets and running timeout | `integration::runner::native_launch_tests::{queued_auxiliary_keeps_eight_active_minutes_and_its_position_after_restart, queued_auxiliary_history_pruning_is_exact_below_cap_and_conservative_above_it, native_helper_pause_after_undrain_preserves_only_the_active_remainder}` prove exact retained remainder/ID/queue position/spend below the history cap, helper handoff and conservative expiry after lost history. `task_integration_direct::a_running_auxiliary_times_out_while_the_native_owner_gate_is_drained` proves the running execution budget remains active. The 1,000-fsync nightly stress stays ignored; the fast cap-crossing test covers bounded history in the gate. |
| Compact facts and peer compatibility | `controller_integration_contracts::{typed_compact_annotation_is_retained_while_disabled_facts_keep_their_bytes, facts_decoder_refuses_an_invalid_or_oversized_annotation, rust_loads_the_same_public_views_codes_and_annotation_boundaries_as_typescript, public_target_is_redacted_and_utf8_bounded_with_ellipsis}` cover the 512-byte annotation, 2,048-byte compound facts and 255-byte exact/128-byte display boundaries. `controller_integration_events::{compound_title_budget_never_truncates_annotation_and_decoder_reserves_bound, shared_compound_facts_fixture_checks_decoder_boundary_before_tolerant_normalization}` prove both compound guards; `controller_integration_contracts::copied_baseline_strict_decoders_prove_disabled_dtos_keep_their_bytes` proves the copied baseline strict codecs. |
| Notification truth and dashboard actions | `controller_integration_wiring::native_notifier_repairs_dropped_hints_confirms_revisions_and_deduplicates_across_restart` confirms full companion state after dropped hints and deduplicates per epoch. App test `enables blocked integration actions in Overview after a confirmed preview loads` proves Overview Re-drive and Close availability. Native direct/controller dashboard tests use revision-bound re-drive. `skills::tests` proves generated `task integrate` grammar. |
| Legacy seven-idle-day GC settlement | `task_integration_direct::{native_legacy_retention_restores_a_retained_merge_without_resurrection, native_legacy_retention_restores_a_reachable_source_without_resurrection, native_legacy_retention_restore_blocks_when_no_retained_result_is_reachable, native_legacy_retention_restore_retries_a_failed_observation_then_imports, native_legacy_retention_restores_a_merge_after_open_repair_exhaustion}` apply baseline retention selection and real non-discard close in five independent schedules. Reachable M imports M; otherwise reachable H settles already integrated and imports observed T; neither blocks workspace-missing; failed observation retains uncertainty and bounded Repair/Network backoff. Host tests `closed_repair_settles_a_retained_merge_without_workspace_or_push_authority`, `closed_repair_settles_only_the_source_with_an_already_integrated_receipt`, `closed_repair_blocks_when_neither_retained_merge_nor_source_is_on_origin`, and `closed_repair_failed_observation_retains_uncertainty_and_retries_safely` confirm the same contract. Closed persists, result refs remain, no push/prepare/workspace/auxiliary returns, and terminal integrate refuses. |

These proofs use disposable file-origin repositories, fake agents/transports, isolated stores and recording notification channels. Copied baseline GC and codecs establish those fixture semantics; they do not prove installation or deployment of an older helper.

| Final contract | Named tests and proof |
| --- | --- |
| Final follow-up fence | `task_integration_host::pre_intent_followup::ordinary_followup_during_first_intent_staging_must_fence_the_old_host_source` and `integration::coordinator::source_fence_tests` prove D-R10 before intent publication and at phase admission. Blocked/revoked follow-ups pass the stop protocol first. `task_interrupt::interrupt_without_an_active_turn_is_a_plain_say` and `controller_say_interrupt::{controller_interrupt_of_an_active_task_cancels_then_says, controller_interrupt_of_an_idle_task_only_says}` establish the interrupt condition: an active turn is stopped before follow-up; without one, the ordinary Say fence still applies. |
| Closed pause exception and independent retry budget | `task_integration_lifecycle::review_fixes::review_closed_restore_does_not_confuse_an_open_repair_retry_with_closed_settlement` and `task_integration_direct::native_legacy_retention_restores_a_merge_after_open_repair_exhaustion` prove Closed observation has a separate budget. `task_integration_lifecycle::review_fixes::review_closed_observation_respects_existing_target_and_four_driver_reservations` covers D-R9 target/cap exclusion during drain/disable, with reservation retained through import. |
| Shared replay quota, lifetime and owner cleanup | `dashboard::task::replay_tests::{review_r2_measure_both_replay_binding_families, review_r2_native_only_replays_obey_the_shared_file_budget, shared_replay_pending_partners_protect_completed_rows_in_both_families, shared_replay_paired_admission_refuses_before_either_publication, dashboard_replay_bindings_are_removed_with_the_owner_record}` cover the shared 32-file/256-KiB budget, completed-only eviction/24-hour pruning, paired admission, pending protection, replay after Closed and cleanup before owner deletion. |

## T7 Part 2b checks and assets

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

## T7 Part 2c checks

The UI delta from `a39bc02` to captured `3134f71` is empty (`git diff --stat a39bc02 feat/ai-t6-wiring -- ui/`). The six Part 2b assets are retained; no rebuild was required. Fresh Part 2c documentation and Rust check results are recorded here after execution, with four Cargo jobs and four nextest threads, serial Cargo processes, and local `.briefs/ai-t7-2c-*.log` evidence.

| Check | Exact command | Result | Log suffix |
| --- | --- | --- | --- |
| Formatting | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo fmt --all --check` | Exit 0 | `fmt` |
| Support-enabled Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --all-targets -- -D warnings` | Exit 0 after final skill wording | `clippy-final` |
| Production Clippy | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo clippy --locked --release --no-default-features --lib --bin worker -- -D warnings` | Exit 0 after final skill wording | `release-clippy-final` |
| Embedded skill | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --lib -E 'test(/^skills::/)'` | 6/6; 1,075 skipped | `skills-lib-final` |
| CLI skill help | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --test cli -E 'test(/skills/)'` | 1/1; 85 skipped | `skills-cli` |
| Embedded assets and loopback web routes | `CARGO_BUILD_JOBS=4 NEXTEST_TEST_THREADS=4 cargo nextest run --locked --test dashboard -E 'test(/^dashboard_web::/)'` | 11/11; 207 skipped | `dashboard-web` |
| Docs site | `npm --prefix docs-site run build` | Exit 0; 98 prepared source files, VitePress 1.6.4 | `docs-build-final` |
| Site links and anchors | `python3 docs-site/check-links.py` | 91 pages, 7,899 internal links/assets; zero broken links | `docs-links-final` |
| Installed skill grammar | `target/debug/worker skills get pool-dispatch --grammar-only` | Exit 0; includes `Usage: worker task integrate [OPTIONS] <TASK_ID>` | `skill-grammar` |

`python3 .briefs/scanner_guard.py target/debug/worker` exited 0 before each final Rust selection: fresh executions 1.17 s, 1.16 s and 0.99 s; no HUNG. The sandbox blocked CPU sampling; the decisive fresh-execution check passed. The dashboard selection used disposable loopback sockets with approved sandbox escalation. The retained six-file SHA-256 manifest matches, totaling 1,733,277 bytes (`ai-t7-2c-assets.log`). All 70 frozen error rows remain byte-identical to Part 2b. Rendered command and proof tables were checked; docs dist was removed after link validation.

## Live status and risks

T8 has not run. It waits for the owner's acceptance-origin and pool authorization. Its checklist is in the [plan's T8 section](../plans/2026-10-03-auto-integration.md#t8--separately-authorized-disposable-origin-live-acceptance). No real pool, SSH host, service, keychain, notification channel, push or deployment was used by T7.

- A rollback below `task.integration` can lose the repair workspace after seven idle host-status days; already-idle tasks can expire sooner than seven days after rollback.
- Lost or capped pause history may conservatively end an auxiliary admission budget early. It cannot enlarge that budget.
- Branch permissions, merge/signing rules, Git version and worker write access still need live checks. Canonical origin strings do not prove SSH/HTTPS repository equivalence; independent owners rely on the lease.
- Unknown push/stop outcomes remain uncertain. A retained Closed task only settles existing effects; it cannot re-drive a new integration.
- D-R9 permits observation-only Closed settlement during drain or disable, with the same target reservation and four-driver limit. The captured `3134f71` snapshot precedes the possible later hint-scope-only fix: archived Round 3 still records that Low publication-boundary requirement; no operator behavior change is claimed for it.
- The final integration-branch full gate remains with the owner; the T6 post-gate affected selection and T7 focused checks do not replace it.

PENDING: Full gate on `integ/auto-integration`, with final commit, exact command, counts and outcome.

PENDING: Main merge identity and outcome.

PENDING: T8 owner authorization, disposable acceptance origin, approved hosts and live results.
