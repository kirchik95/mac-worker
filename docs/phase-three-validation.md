# Phase 3 validation record

Phase 3 implements one explicitly selected worker per `worker run` invocation. It is not an automatic scheduler or queue. This record separates automated local evidence from the completed live single-host acceptance evidence required before relying on the feature operationally.

## Automated local and fake-transport evidence

The committed Task 10 slice reports record these focused commands and results; each is reproducible at the revision shown below. Those revisions are ancestors of this document's branch. They exercise local helpers, isolated filesystem fixtures, and fake or injected transport boundaries as applicable; none is evidence of a configured live worker.

| Command | Revision measured | Recorded result |
| --- | --- | --- |
| `cargo test --locked --test remote_snapshot -- --nocapture` | `4f6125f` | 21 passed, 0 failed |
| `cargo test --locked --test run_command -- --nocapture` | `1dee62e` | 84 passed, 0 failed |
| `cargo test --locked --test job_queries -- --nocapture` | `0d7c341` | 63 passed, 0 failed |
| `cargo test --locked --test supervisor -- --nocapture` | `f7cb2b1` | 44 passed, 0 failed |
| `cargo test --locked --lib transfer::exec_inheritance_tests -- --nocapture` | `f7cb2b1` | 7 passed, 0 failed |
| `cargo test --locked --test run_command --test job_queries --test remote_snapshot --test supervisor -- --nocapture` | `f7cb2b1` | 212 passed (84 run_command + 63 job_queries + 21 remote_snapshot + 44 supervisor), 0 failed, 0 ignored |

The recorded automated coverage includes a deterministic 100-row disconnect/reconciliation matrix, a 100-row remote snapshot mutation matrix, binary log handling, artifact preflight rejection, and exact literal argument-vector handling. The combined Task 10 adversarial gate above is a deliberately scoped controller invocation, not an all-target test or final gate. It does not replace live-host acceptance.

The full final gate completed at the validated software revision below: formatting, all-target tests, strict Clippy, release build, and `git diff --check` each exited successfully. The combined Task 10 adversarial gate remains a deliberately scoped controller invocation, not a substitute for the full gate. The live evidence below covers one explicitly selected worker only; it does not imply fleet behavior.

## Live single-host acceptance

An isolated live run selected only `mini-1`. The record intentionally retains only shortened identifiers, outcome categories, byte counts, durations, and shortened fingerprints; it omits application output, complete paths, clone origin, credentials, raw environment values, source content, and raw logs.

| Required evidence | Sanitized record |
| --- | --- |
| Software commit | `fa89103528c0d6b66a660283a1e69fd068708ffc`; final local gate passed. Setup verified the promoted helper, and a final SHA-256 equality check confirmed the installed helper matched the validated client release binary. |
| Worker protocol version | `2` from setup and ready-worker probes. |
| Selected worker name | `mini-1` only. |
| Sanitized command categories | Setup (including an idempotence control); successful literal-argv `printf`; shell sleep with disconnect/reconnect; `status <job-id>`; non-follow `logs <job-id>`; `logs -f` reconnect; concurrent capacity check; and the operator-selected literal-argv `/bin/sh -c 'exit 7'` case. |
| Successful command outcome | `d73c4d4b…`: terminal `succeeded`, exit `0`, about 5 seconds. |
| Non-zero command outcome | `278984d9…`: terminal `failed`, exit `7`, about 5 seconds. |
| Disconnect and reconnect | `5b27d508…` was accepted once; its local follower was interrupted before terminal status after receiving a 5-byte stdout prefix. `status`, non-follow `logs`, and `logs -f` reconnected by that same ID: terminal `succeeded`/exit `0`, 8 stdout bytes and 0 stderr bytes. The retained bytes matched the original prefix plus an exact 3-byte continuation; no second acceptance occurred. |
| Capacity | While a separate one-slot holder was active, a concurrent submission returned typed `CAPACITY_BUSY` (exit `75`) with only an error event—no acceptance, status, or log event. |
| Cleanup and lease | Holder `026e5efa…` reached terminal `succeeded` with no cleanup error; only afterward did the immediate worker inventory report the slot `idle` with no active lease. |
| Retained metadata and logs | Scoped inspection of retained metadata/log record namespaces found zero planted-marker hits and zero complete-local-path hits. Immutable source snapshot content was intentionally excluded from this record-only inspection. |
| Worker namespace fingerprints | Before: owned data namespace absent; helper namespace `1` entry, fingerprint `1f4adb786d42…`. After: owned data namespace `196` entries, fingerprint `1cef0f92734b…`; helper namespace `1` entry, fingerprint `b502c4f5f57a…`. These expected changes are confined to mac-worker-owned namespaces. |
| Unrelated remote entries | A controlled before/after repeat-setup plus no-output-run check preserved both count and fingerprint for the immediate non-owned sibling sets of the data and helper parents. The operator-preserved archive was not modified. |
| Original isolated clone | Its baseline commit, intentional marker-only status/diff, and clean diff check were unchanged after all remote execution. |

The live record states that the validated helper and client revisions matched, without recording installation locations or connection details. It records only sanitized command categories and outcomes; application output is not safe evidence by default because it may contain application-emitted secrets.

Post-rebase smoke rerun: after rebasing this branch onto `main`, isolated setup, readiness, one literal-argv `printf` job, exact `status` and JSON `logs` retrieval, and idle/no-active-lease confirmation passed on `mini-1`.

## Phase 4 boundary

**Completed live acceptance does not authorize Phase 4.** Automatic scheduling and queueing, cancellation, artifact transfer, caches, Docker profiles, safe garbage collection, and the dashboard remain later phases. In Phase 3, configured artifact collection must continue to reject at preflight rather than execute a job and discard outputs.
