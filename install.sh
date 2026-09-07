#!/bin/sh

set -eu

PROGRAM=mac-worker
DEFAULT_VERSION=v0.1.0
RELEASE_BASE_URL=${MAC_WORKER_RELEASE_BASE_URL:-https://github.com/kirchik95/mac-worker/releases/download}
VERSION=$DEFAULT_VERSION
BIN_DIR=${HOME:?HOME must be set}/.local/bin
TMP_ROOT=
STAGED_BINARY=

usage() {
    cat <<'USAGE'
Install the prebuilt mac-worker CLI for macOS on Apple Silicon.

Usage: install.sh [--version vX.Y.Z] [--bin-dir DIR]

Options:
  --version VERSION  Release tag to install (default: v0.1.0)
  --bin-dir DIR      Installation directory (default: $HOME/.local/bin)
  -h, --help         Show this help
USAGE
}

die() {
    printf '%s: %s\n' "$PROGRAM" "$1" >&2
    exit 1
}

shell_quote() {
    printf "'"
    printf '%s' "$1" | sed "s/'/'\\\\''/g"
    printf "'"
}

cleanup() {
    if [ -n "$STAGED_BINARY" ] && [ -e "$STAGED_BINARY" ]; then
        rm -f "$STAGED_BINARY"
    fi
    if [ -n "$TMP_ROOT" ] && [ -d "$TMP_ROOT" ]; then
        rm -rf "$TMP_ROOT"
    fi
}

trap cleanup EXIT HUP INT TERM

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version)
            [ "$#" -ge 2 ] || die "--version requires a value"
            VERSION=$2
            shift 2
            ;;
        --bin-dir)
            [ "$#" -ge 2 ] || die "--bin-dir requires a value"
            BIN_DIR=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

printf '%s\n' "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$' || \
    die "version must be a release tag such as v0.1.0"
[ -n "$BIN_DIR" ] || die "installation directory must not be empty"

SYSTEM=$(uname -s)
ARCH=$(uname -m)
if [ "$SYSTEM" != Darwin ] || [ "$ARCH" != arm64 ]; then
    die "prebuilt releases require macOS on Apple Silicon (found $SYSTEM/$ARCH)"
fi

command -v curl >/dev/null 2>&1 || die "curl is required to download the release"
command -v shasum >/dev/null 2>&1 || die "shasum is required to verify the release"
command -v tar >/dev/null 2>&1 || die "tar is required to unpack the release"

TARGET=aarch64-apple-darwin
ARCHIVE="mac-worker-$VERSION-$TARGET.tar.gz"
ARCHIVE_ROOT="mac-worker-$VERSION-$TARGET"
RELEASE_URL=${RELEASE_BASE_URL%/}/$VERSION
TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/mac-worker-install.XXXXXX") || \
    die "could not create a temporary directory"
ARCHIVE_PATH=$TMP_ROOT/$ARCHIVE
CHECKSUM_PATH=$ARCHIVE_PATH.sha256

if ! curl --fail --location --silent --show-error \
    "$RELEASE_URL/$ARCHIVE" -o "$ARCHIVE_PATH"; then
    die "download failed for $RELEASE_URL/$ARCHIVE"
fi
if ! curl --fail --location --silent --show-error \
    "$RELEASE_URL/$ARCHIVE.sha256" -o "$CHECKSUM_PATH"; then
    die "download failed for $RELEASE_URL/$ARCHIVE.sha256"
fi

EXPECTED_CHECKSUM=$(awk 'NR == 1 { print $1 }' "$CHECKSUM_PATH")
if ! printf '%s\n' "$EXPECTED_CHECKSUM" | grep -Eq '^[0-9A-Fa-f]{64}$'; then
    die "checksum file is invalid"
fi
ACTUAL_CHECKSUM=$(shasum -a 256 "$ARCHIVE_PATH" | awk '{ print $1 }')
if [ "$ACTUAL_CHECKSUM" != "$EXPECTED_CHECKSUM" ]; then
    die "checksum verification failed for $ARCHIVE"
fi

if ! tar -xzf "$ARCHIVE_PATH" -C "$TMP_ROOT"; then
    die "could not unpack $ARCHIVE"
fi
EXTRACTED_BINARY=$TMP_ROOT/$ARCHIVE_ROOT/worker
[ -f "$EXTRACTED_BINARY" ] || die "release archive does not contain worker"
[ -x "$EXTRACTED_BINARY" ] || die "worker in the release archive is not executable"

mkdir -p "$BIN_DIR" || die "could not create installation directory: $BIN_DIR"
[ ! -d "$BIN_DIR/worker" ] || die "installation target is a directory: $BIN_DIR/worker"
STAGED_BINARY=$(mktemp "$BIN_DIR/.worker.install.XXXXXX") || \
    die "could not create a staging file in $BIN_DIR"
if ! cp "$EXTRACTED_BINARY" "$STAGED_BINARY" || ! chmod 755 "$STAGED_BINARY"; then
    die "could not stage worker in $BIN_DIR"
fi
if ! mv -f "$STAGED_BINARY" "$BIN_DIR/worker"; then
    die "could not install worker in $BIN_DIR"
fi
STAGED_BINARY=

printf 'Installed worker %s at %s/worker\n' "$VERSION" "$BIN_DIR"
case ":${PATH:-}:" in
    *":$BIN_DIR:"*) ;;
    *)
        printf '%s\n' 'Add it to PATH for this shell:'
        QUOTED_BIN_DIR=$(shell_quote "$BIN_DIR")
        printf '  export PATH=%s:"$PATH"\n' "$QUOTED_BIN_DIR"
        ;;
esac
