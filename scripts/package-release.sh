#!/bin/sh

set -eu

PROGRAM=package-release
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
TAG=${1:-}
OUTPUT_DIR=${2:-$ROOT_DIR/dist}
TARGET=aarch64-apple-darwin
TMP_ROOT=

die() {
    printf '%s: %s\n' "$PROGRAM" "$1" >&2
    exit 1
}

cleanup() {
    if [ -n "$TMP_ROOT" ] && [ -d "$TMP_ROOT" ]; then
        rm -rf "$TMP_ROOT"
    fi
}

trap cleanup EXIT HUP INT TERM

[ -n "$TAG" ] || die "usage: scripts/package-release.sh vX.Y.Z [output-dir]"

CARGO_VERSION=$(awk '
    $0 == "[package]" { in_package = 1; next }
    in_package && /^\[/ { exit }
    in_package && $1 == "version" {
        gsub(/"/, "", $3)
        print $3
        exit
    }
' "$ROOT_DIR/Cargo.toml")
[ -n "$CARGO_VERSION" ] || die "could not read package version from Cargo.toml"
[ "$TAG" = "v$CARGO_VERSION" ] || \
    die "tag $TAG does not match Cargo.toml version $CARGO_VERSION"

SYSTEM=$(uname -s)
ARCH=$(uname -m)
if [ "$SYSTEM" != Darwin ] || [ "$ARCH" != arm64 ]; then
    die "release packaging requires macOS on Apple Silicon (found $SYSTEM/$ARCH)"
fi

if [ -n "${MAC_WORKER_BINARY_PATH:-}" ]; then
    BINARY=$MAC_WORKER_BINARY_PATH
else
    (cd "$ROOT_DIR" && cargo build --locked --release)
    BINARY=$ROOT_DIR/target/release/worker
fi

[ -f "$BINARY" ] || die "release binary was not found: $BINARY"
[ -x "$BINARY" ] || die "release binary is not executable: $BINARY"
BINARY_TYPE=$(file "$BINARY")
printf '%s\n' "$BINARY_TYPE" | grep -Eq 'Mach-O.*arm64' || \
    die "release binary is not a macOS arm64 executable: $BINARY_TYPE"

ARCHIVE="mac-worker-$TAG-$TARGET.tar.gz"
ARCHIVE_ROOT="mac-worker-$TAG-$TARGET"
FORMULA=mac-worker.rb
TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/mac-worker-package.XXXXXX") || \
    die "could not create a temporary directory"
PACKAGE_ROOT=$TMP_ROOT/$ARCHIVE_ROOT
mkdir -p "$PACKAGE_ROOT"
cp "$BINARY" "$PACKAGE_ROOT/worker"
chmod 755 "$PACKAGE_ROOT/worker"
cp "$ROOT_DIR/LICENSE" "$PACKAGE_ROOT/LICENSE"
cp "$ROOT_DIR/install.sh" "$PACKAGE_ROOT/install.sh"
chmod 755 "$PACKAGE_ROOT/install.sh"

tar -C "$TMP_ROOT" -czf "$TMP_ROOT/$ARCHIVE" "$ARCHIVE_ROOT"
CHECKSUM=$(shasum -a 256 "$TMP_ROOT/$ARCHIVE" | awk '{ print $1 }')
printf '%s  %s\n' "$CHECKSUM" "$ARCHIVE" >"$TMP_ROOT/$ARCHIVE.sha256"

cat >"$TMP_ROOT/$FORMULA" <<EOF
class MacWorker < Formula
  desc "Run coding-agent tasks on a pool of Apple Silicon Macs"
  homepage "https://github.com/kirchik95/mac-worker"
  url "https://github.com/kirchik95/mac-worker/releases/download/$TAG/$ARCHIVE"
  version "$CARGO_VERSION"
  sha256 "$CHECKSUM"
  license "MIT"

  depends_on arch: :arm64
  depends_on :macos

  def install
    bin.install "worker"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/worker --version")
  end
end
EOF

mkdir -p "$OUTPUT_DIR"
mv -f "$TMP_ROOT/$ARCHIVE" "$OUTPUT_DIR/$ARCHIVE"
mv -f "$TMP_ROOT/$ARCHIVE.sha256" "$OUTPUT_DIR/$ARCHIVE.sha256"
mv -f "$TMP_ROOT/$FORMULA" "$OUTPUT_DIR/$FORMULA"

printf 'Created %s\n' "$OUTPUT_DIR/$ARCHIVE"
printf 'Created %s\n' "$OUTPUT_DIR/$ARCHIVE.sha256"
printf 'Created %s\n' "$OUTPUT_DIR/$FORMULA"
