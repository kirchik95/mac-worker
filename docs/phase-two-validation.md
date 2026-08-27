# Phase-two snapshot validation

Validation was performed on 2026-08-27 with the release binary, the normal three-worker inventory, one temporary clone of a real Node-family repository, and one temporary clone of this Rust repository. The original working trees were never changed. Each clone used an ephemeral home and isolated XDG config, state, cache, and data roots; their values, repository origins, and complete local paths are intentionally omitted.

The local clone operation initially supplied a filesystem-path origin. Doctor rejected it with `INVALID_ORIGIN` before worker probing or cache creation. The clone-only origin metadata was then removed, leaving the original repositories untouched and exercising the documented common-directory identity fallback.

## Commands exercised

The following command shapes were run with `<inventory>`, `<node-clone>`, and `<rust-clone>` standing for the omitted local values:

```bash
cargo build --release
<release-worker> --config <inventory> doctor --project <node-clone>
<release-worker> --config <inventory> --json doctor --project <node-clone>
<release-worker> --config <inventory> doctor --project <rust-clone>
<release-worker> --config <inventory> --json doctor --project <rust-clone>
<release-worker> --config <inventory> --json doctor \
  --project <representative-clone> \
  --include 'task8-acceptance/unique-blocker.txt'
```

Remote verification used only bounded, read-only SSH listings and metadata reads below mac-worker's `incoming`, `jobs`, `snapshots`, and `leases` areas. No setup, upload, deletion, lease acquisition, or user command was invoked.

## Acceptance results

| Check | Node-family clone | Rust clone |
|---|---|---|
| Two unchanged captures | Ready; project ID, worktree ID, and digest stable | Ready; project ID, worktree ID, and digest stable |
| Isolated dirty tracked bytes | Ready; dirty flag set and digest changed | Ready; dirty flag set and digest changed |
| Unique uncovered untracked file | Exit 64; `UNTRACKED_INPUT`; no snapshot summary | Exit 64; `UNTRACKED_INPUT`; no snapshot summary |
| Exact relative include | Ready; one included untracked input and a new digest | Ready; one included untracked input and a new digest |
| Exact blocker removal | Ready; blocker cleared and prior dirty digest restored | Ready; blocker cleared and prior dirty digest restored |
| Tracked-byte restoration | Clean digest and both identities restored | Clean digest and both identities restored |
| Compatible workers | Three eligible workers reported | Three eligible workers reported |
| Local cleanup | Zero capture entries in staging or ready | Zero capture entries in staging or ready |

Read-only remote state fingerprints for all three workers were byte-identical before and after the complete Doctor sequence. Local staging and ready roots contained no Doctor-owned capture; the private cleanup namespace, where present, was empty. Both temporary clone worktrees were clean after restoration, and the validated temporary acceptance root was removed in full.

Focused adversarial evidence also passed: 256 cases for each path property, 256 canonical-manifest shuffle cases, the five-path planted-secret matrix, supported 255-byte UTF-8 filenames, relative and absolute symlinks without dereference, and simultaneous linked-worktree captures with isolated cleanup. The deterministic source-mutation matrix rejected all 1,000 captures with zero ready publications; the full focused snapshot suite reported 28 passing tests, with 4.832 seconds inside the matrix and 6.19 seconds wall time for the command.

## Delivery boundary

Phase two validates and deletes a local immutable snapshot and reports worker eligibility. Remote upload, `worker run`, queueing, remote logs, and artifact transfer remain subsequent phases.
