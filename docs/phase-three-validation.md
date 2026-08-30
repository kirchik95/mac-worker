# Phase 3 validation record

Phase 3 implements one explicitly selected worker per `worker run` invocation. It is not an automatic scheduler or queue. This record separates automated local evidence from the live single-host acceptance evidence still required before relying on the feature operationally.

## Automated local and fake-transport evidence

The committed Task 10 slice reports record these focused commands and results at the revisions shown below. Those revisions are ancestors of this document's branch, so each command can be reproduced against its named revision. They exercise local helpers, isolated filesystem fixtures, and fake or injected transport boundaries as applicable; none is evidence of a configured live worker.

| Command | Revision measured | Recorded result |
| --- | --- |
| `cargo test --locked --test remote_snapshot -- --nocapture` | `4f6125f` | 21 passed, 0 failed |
| `cargo test --locked --test run_command -- --nocapture` | `1dee62e` | 84 passed, 0 failed |
| `cargo test --locked --test job_queries -- --nocapture` | `0d7c341` | 63 passed, 0 failed |
| `cargo test --locked --test supervisor -- --nocapture` | `f7cb2b1` | 44 passed, 0 failed |
| `cargo test --locked --lib transfer::exec_inheritance_tests -- --nocapture` | `f7cb2b1` | 7 passed, 0 failed |

The recorded automated coverage includes a deterministic 100-row disconnect/reconciliation matrix, a 100-row remote snapshot mutation matrix, binary log handling, artifact preflight rejection, and exact literal argument-vector handling. These are separate focused measurements, not one combined invocation or final gate. They do not replace live-host acceptance.

The combined all-target test result and final gate are **PENDING**. No controller-wide or final-gate result is claimed here, and no final release revision is recorded. Do not infer live acceptance, fleet behavior, or a final release revision from the focused automated results.

## Live single-host acceptance — PENDING

No sanitized live single-host acceptance run is recorded yet. Populate every field below only from a completed isolated live run. Do not include full paths, clone origins, credentials, raw environment values, source contents, full identifiers, or unsanitized logs.

| Required evidence | Sanitized record |
| --- | --- |
| Software commit | **PENDING** |
| Worker protocol version | **PENDING** |
| Selected worker name | **PENDING** |
| Sanitized command categories | **PENDING** — record setup; the operator-selected shell sleep/reconnect job; `logs -f` reconnect; and the operator-selected literal-argv non-zero `/bin/sh -c 'exit 7'` case |
| Successful command outcome | **PENDING** — record shortened job ID, terminal state, exit result, and sanitized duration only |
| Non-zero command outcome | **PENDING** — record shortened job ID, terminal state, exit result, and sanitized duration only |
| Disconnect and reconnect | **PENDING** — record a follower disconnect, then `status` and `logs` reconnecting by the same original shortened job ID, with one execution |
| Capacity | **PENDING** — record a concurrent submission returning `CAPACITY_BUSY` with no execution |
| Cleanup and lease | **PENDING** — record terminal cleanup completing before the one-slot lease becomes available |
| Retained metadata and logs | **PENDING** — record inspection confirming no local complete paths or planted values |
| Worker namespace fingerprints | **PENDING** — record before/after fingerprints of mac-worker-owned namespaces only |
| Unrelated remote entries | **PENDING** — record that no unrelated remote entries changed |
| Original isolated clone | **PENDING** — record that the original isolated clone remains unchanged after remote execution |

The live record must also state whether the validated helper and client revisions matched, without recording installation locations or connection details. Record only sanitized command categories and outcomes; application output is not safe evidence by default because it may contain application-emitted secrets.

## Phase 4 boundary

**PENDING live acceptance does not authorize Phase 4.** Even after this template is completed, automatic scheduling and queueing, cancellation, artifact transfer, caches, Docker profiles, safe garbage collection, and the dashboard remain later phases. In Phase 3, configured artifact collection must continue to reject at preflight rather than execute a job and discard outputs.
