#!/bin/sh
# Run the whole test suite with cargo-nextest, with TMPDIR on a temporary RAM
# disk. The suite is bound by fsync on the SSD, not by CPU: on a RAM disk the
# heaviest binaries run about twice as fast. Formatting and clippy are separate
# steps (see docs/testing.md).
#
#   scripts/test-gate.sh                      # every test
#   scripts/test-gate.sh --profile ci         # CI profile (one retry, JUnit)
#   scripts/test-gate.sh --run-ignored only -E 'test(/_stress$/)'   # nightly stress
#
# MAC_WORKER_GATE_RAMDISK_MB sets the RAM disk size (default 4096); 0 keeps the
# default TMPDIR, for machines without memory to spare such as CI runners.

set -eu

# A gate build is thrown away; incremental data would only fill the disk.
export CARGO_INCREMENTAL=0

size_mb=${MAC_WORKER_GATE_RAMDISK_MB:-4096}
if [ "$size_mb" -eq 0 ]; then
    exec cargo nextest run --locked --all-targets "$@"
fi

dev=$(hdiutil attach -nomount "ram://$((size_mb * 2048))" | tr -d '[:space:]')
cleanup() {
    hdiutil detach "$dev" >/dev/null 2>&1 || hdiutil detach -force "$dev" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

volume="mwgate$$"
diskutil erasevolume APFS "$volume" "$dev" >/dev/null
# A new volume root is group-writable. Host code refuses paths below a
# directory that others can write to, so make the root and TMPDIR private.
chmod 755 "/Volumes/$volume"
TMPDIR="/Volumes/$volume/tmp"
mkdir -m 700 "$TMPDIR"
export TMPDIR

cargo nextest run --locked --all-targets "$@"
