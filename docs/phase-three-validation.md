# Phase 3 validation record

Phase 3 implements one explicitly selected worker per `worker run` invocation. It is not an automatic scheduler or queue. This record separates automated local evidence from the live single-host acceptance evidence still required before relying on the feature operationally.

## Automated local and fake-transport evidence

The committed Task 10 slice reports record these focused commands and results. They exercise local helpers, isolated filesystem fixtures, and fake or injected transport boundaries as applicable; none is evidence of a configured live worker.

| Command | Recorded result |
| --- | --- |
| `cargo test --locked --test remote_snapshot -- --nocapture` | 21 passed, 0 failed |
| `cargo test --locked --test job_queries -- --nocapture` | 63 passed, 0 failed |
| `cargo test --locked --test run_command -- --nocapture` | 84 passed, 0 failed |
| `cargo test --locked --test supervisor -- --nocapture` | 44 passed, 0 failed |
| `cargo test --locked --lib transfer::exec_inheritance_tests -- --nocapture` | 7 passed, 0 failed |
| `cargo fmt --all --check` | passed in the focused slice checks |
| Focused Clippy checks reported by the slices | passed with no warnings |
| `git diff --check` | passed in the focused slice checks |

The recorded automated coverage includes a deterministic 100-row disconnect/reconciliation matrix, a 100-row remote snapshot mutation matrix, binary log handling, artifact preflight rejection, and exact literal argument-vector handling. It does not replace live-host acceptance.

No controller-wide or full-gate result is claimed here: no such final result is recorded in this validation document. Do not infer live acceptance, fleet behavior, or a final release revision from the focused automated results.

## Live single-host acceptance — PENDING

No sanitized live single-host acceptance run is recorded yet. Populate every field below only from a completed isolated live run. Do not include full paths, clone origins, credentials, raw environment values, source contents, full identifiers, or unsanitized logs.

| Required evidence | Sanitized record |
| --- | --- |
| Software commit | **PENDING** |
| Worker protocol version | **PENDING** |
| Selected worker name | **PENDING** |
| Successful command outcome | **PENDING** — shortened job ID, terminal state, exit result, and sanitized duration only |
| Non-zero command outcome | **PENDING** — shortened job ID, terminal state, exit result, and sanitized duration only |
| Disconnect and reconnect | **PENDING** — the follower disconnected, `status` and `logs` reconnected using the same original shortened job ID, and execution occurred once |
| Capacity | **PENDING** — concurrent submission returned `CAPACITY_BUSY` and did not execute |
| Cleanup and lease | **PENDING** — terminal cleanup completed before the one-slot lease became available |
| Worker namespace fingerprints | **PENDING** — before/after fingerprints of mac-worker-owned namespaces only |
| Original isolated clone | **PENDING** — unchanged after remote execution |

The live record must also state whether the validated helper and client revisions matched, without recording installation locations or connection details. Record only sanitized command categories and outcomes; application output is not safe evidence by default because it may contain application-emitted secrets.

## Phase 4 boundary

**PENDING live acceptance does not authorize Phase 4.** Even after this template is completed, automatic scheduling and queueing, cancellation, artifact transfer, caches, Docker profiles, safe garbage collection, and the dashboard remain later phases. In Phase 3, configured artifact collection must continue to reject at preflight rather than execute a job and discard outputs.
