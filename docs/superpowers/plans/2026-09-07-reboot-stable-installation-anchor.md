# Reboot-stable installation anchor

**Problem.** A worker orphans its own installation when the machine reboots.
Observed on mini-3 on 2026-09-07: the recorded installation parent had
`device 16777229, inode 70086370`, and after a reboot the same directory
reported `device 16777231, inode 70086370`. The inode is unchanged; macOS
assigns APFS volume device numbers at mount time and does not keep them
stable across boots.

The device number reaches durable state in two independent places, so a
reboot breaks the worker twice:

1. `installation_names` hashes the parent's device into the installation
   lock and identity file names. After a reboot the expected names differ
   from the ones on disk, the root is present without its lock, and
   `open_inner` fails closed with `host installation lock is absent`.
   `worker setup` cannot repair it, because it stops on the same invariant.
2. `HostInstallationIdentity` and `HostLayoutIdentity` record a `device` per
   entry and are validated by exact equality, so even with correct names the
   open would fail with `canonical host installation identity changed`.

The evidence that this already recurs: `mac-worker.pre-anchor-2026-09-03`
sits beside the data root on mini-3, from the same repair on 2026-09-03.

**Decision.** The device number is a within-boot cross-entry consistency
value, never a durable identity. It stays in the records for diagnostics and
keeps its build-time check (every entry must live on the parent's device),
but it leaves both the anchor key and the durable equality comparisons.

## Task 1: stable anchor key

Key the installation names on the parent inode and the root component only,
under a new `mac-worker-installation-v2` domain. Every existing installation
keeps its v1 names until Task 2 re-anchors it, so Task 1 must not ship alone.

## Task 2: one-time re-anchor

`open_inner` gains a bounded repair for exactly one shape: the root is
present and the v2 lock and identity are absent. It scans the installation
parent for `.mac-worker-installation-<64 hex>.json`, parses each, and accepts
the single candidate that proves it is the same installation:

- the stored `root_name_sha256` equals the current one;
- the stored parent, root, lock, and layout inodes equal the current ones;
- every stored device is the same value, that is, one uniform device change.

On a unique match it renames the legacy lock and identity to the v2 names and
continues the normal path. Anything else, including two matches or a missing
legacy identity, keeps the existing fail-closed error. A renamed root cannot
satisfy the rule because its inode moves with it, so
`layout_without_the_external_installation_identity_is_not_adopted` and
`active_root_renamed_to_an_absent_canonical_path_cannot_form_a_second_domain`
keep holding.

## Task 3: refresh the anchor instead of weakening comparisons

A stale device is not only compared in the two identity records: the lock is
re-opened through `validate_private_regular_binding`, which compares the whole
`PrivateEntryIdentity`, device included. Weakening those comparisons would
weaken a security check, so the records are refreshed instead.

`open_inner` recognises exactly one further shape, under the installation
lock: every stored inode equals the current one, every stored device is the
same value, and that value differs from the current parent device. That is a
volume remount and nothing else can produce it. In that case the installation
identity and the host layout identity are rewritten with the current values,
and the open proceeds through the unchanged comparisons. Every other
difference keeps its existing fail-closed error.

Task 2 performs the same rewrite after it renames the legacy names, because a
re-anchored installation also carries the old device values.

## Crash safety, and why this is not a small patch

The installation identity is published with
`write_private_atomic_no_replace_with_identity`, which refuses to replace an
existing file and records the identity of the file it creates inside that same
file. A refresh is therefore an unlink followed by a fresh publish while the
installation lock is held, and it has a window in which the identity is
absent. That window is exactly the state `open_inner` reports as
`host installation is incomplete after coordination`, so the repair must not
be able to leave a worker there.

The implementation therefore owes:

- new `HostStoreWritePoint` variants around the unlink and the republish, for
  both the installation identity and the host layout identity;
- fault-injection tests proving that a crash at each new point leaves an
  installation that the next `open` still repairs rather than one that needs a
  human;
- the same treatment for the layout identity inside the root, which records
  devices too.

Until that is done, a rebooted worker is repaired by hand: move the data root
aside and rerun `worker setup`, which is what `mac-worker.pre-anchor-2026-09-03`
and `host.pre-anchor-2026-09-07` on mini-3 record.

## Gate

`cargo test --all-targets`, `cargo fmt --check`, plus new tests: a key that
survives a device change, a re-anchor from a legacy identity, a refusal when
the inodes differ, and a refusal when two legacy candidates match.
