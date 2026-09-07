#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
INSTALLER="$ROOT_DIR/install.sh"
PACKAGER="$ROOT_DIR/scripts/package-release.sh"
TEST_VERSION=v9.8.7
ARCHIVE="mac-worker-$TEST_VERSION-aarch64-apple-darwin.tar.gz"

fail() {
    printf 'not ok - %s\n' "$1" >&2
    exit 1
}

assert_contains() {
    haystack=$1
    needle=$2
    label=$3
    case "$haystack" in
        *"$needle"*) ;;
        *) fail "$label (missing: $needle)" ;;
    esac
}

assert_file_equals() {
    path=$1
    expected=$2
    label=$3
    [ -f "$path" ] || fail "$label (missing file: $path)"
    actual=$(cat "$path")
    [ "$actual" = "$expected" ] || fail "$label (got: $actual)"
}

run_installer() {
    output_path=$1
    shift
    set +e
    HOME="$TEST_ROOT/home" \
        PATH="$TEST_ROOT/stubs:/usr/bin:/bin" \
        MAC_WORKER_RELEASE_BASE_URL="https://fixtures.invalid/releases/download" \
        FIXTURE_RELEASE_DIR="$TEST_ROOT/releases/$TEST_VERSION" \
        CURL_LOG="$TEST_ROOT/curl.log" \
        UNAME_S="${UNAME_S:-Darwin}" \
        UNAME_M="${UNAME_M:-arm64}" \
        FAIL_DOWNLOAD="${FAIL_DOWNLOAD:-}" \
        "$INSTALLER" "$@" >"$output_path" 2>&1
    status=$?
    set -e
    return "$status"
}

make_stubs() {
    mkdir -p "$TEST_ROOT/stubs"
    cat >"$TEST_ROOT/stubs/uname" <<'STUB'
#!/bin/sh
case "${1:-}" in
    -s) printf '%s\n' "${UNAME_S:-Darwin}" ;;
    -m) printf '%s\n' "${UNAME_M:-arm64}" ;;
    *) exit 2 ;;
esac
STUB
    cat >"$TEST_ROOT/stubs/curl" <<'STUB'
#!/bin/sh
output=
url=
while [ "$#" -gt 0 ]; do
    case "$1" in
        -o|--output)
            [ "$#" -ge 2 ] || exit 2
            output=$2
            shift 2
            ;;
        --fail|--location|--silent|--show-error|-f|-L|-s|-S)
            shift
            ;;
        *)
            url=$1
            shift
            ;;
    esac
done
[ -n "$output" ] && [ -n "$url" ] || exit 2
printf '%s\n' "$url" >>"$CURL_LOG"
name=${url##*/}
case "${FAIL_DOWNLOAD:-}" in
    "$name") exit 22 ;;
esac
cp "$FIXTURE_RELEASE_DIR/$name" "$output"
STUB
    cat >"$TEST_ROOT/stubs/file" <<'STUB'
#!/bin/sh
printf '%s: Mach-O 64-bit executable %s\n' "$1" "${FILE_ARCH:-arm64}"
STUB
    chmod +x "$TEST_ROOT/stubs/uname" "$TEST_ROOT/stubs/curl" "$TEST_ROOT/stubs/file"
}

make_release_fixture() {
    fixture_root="$TEST_ROOT/archive/mac-worker-$TEST_VERSION-aarch64-apple-darwin"
    release_dir="$TEST_ROOT/releases/$TEST_VERSION"
    mkdir -p "$fixture_root" "$release_dir"
    printf '%s\n' 'fixture-worker-v9.8.7' >"$fixture_root/worker"
    chmod +x "$fixture_root/worker"
    printf '%s\n' 'fixture license' >"$fixture_root/LICENSE"
    printf '%s\n' '#!/bin/sh' 'exit 0' >"$fixture_root/install.sh"
    chmod +x "$fixture_root/install.sh"
    tar -C "$TEST_ROOT/archive" -czf "$release_dir/$ARCHIVE" \
        "mac-worker-$TEST_VERSION-aarch64-apple-darwin"
    checksum=$(shasum -a 256 "$release_dir/$ARCHIVE" | awk '{print $1}')
    printf '%s  %s\n' "$checksum" "$ARCHIVE" >"$release_dir/$ARCHIVE.sha256"
}

test_verified_install_into_fresh_directory() {
    destination="$TEST_ROOT/fresh/bin"
    output="$TEST_ROOT/success.out"
    run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination" || \
        fail "verified fixture installation succeeds"
    assert_file_equals "$destination/worker" 'fixture-worker-v9.8.7' \
        "verified binary is installed"
    [ -x "$destination/worker" ] || fail "installed binary is executable"
    output_text=$(cat "$output")
    assert_contains "$output_text" "Installed worker $TEST_VERSION" \
        "success identifies installed version"
    assert_contains "$output_text" "export PATH='$destination':\"\$PATH\"" \
        "success prints PATH guidance"
    printf '%s\n' 'ok - verified installation into a fresh directory'
}

test_bad_checksum_is_rejected() {
    destination="$TEST_ROOT/bad-checksum/bin"
    output="$TEST_ROOT/bad-checksum.out"
    printf '%064d  %s\n' 0 "$ARCHIVE" >"$TEST_ROOT/releases/$TEST_VERSION/$ARCHIVE.sha256"
    if run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination"; then
        fail "bad checksum is rejected"
    fi
    [ ! -e "$destination/worker" ] || fail "bad checksum installs no binary"
    output_text=$(cat "$output")
    assert_contains "$output_text" 'checksum verification failed' \
        "bad checksum reports verification failure"
    make_release_fixture
    printf '%s\n' 'ok - bad checksum is rejected'
}

test_unsupported_platform_downloads_nothing() {
    destination="$TEST_ROOT/unsupported/bin"
    output="$TEST_ROOT/unsupported.out"
    : >"$TEST_ROOT/curl.log"
    export UNAME_S=Linux
    if run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination"; then
        fail "unsupported platform is rejected"
    fi
    unset UNAME_S
    [ ! -s "$TEST_ROOT/curl.log" ] || fail "unsupported platform performs no download"
    [ ! -e "$destination/worker" ] || fail "unsupported platform installs no binary"
    output_text=$(cat "$output")
    assert_contains "$output_text" 'macOS on Apple Silicon' \
        "unsupported platform reports supported target"
    printf '%s\n' 'ok - unsupported platform is rejected before download'
}

test_existing_binary_survives_verification_failure() {
    destination="$TEST_ROOT/existing-checksum/bin"
    output="$TEST_ROOT/existing-checksum.out"
    mkdir -p "$destination"
    printf '%s\n' 'keep-this-worker' >"$destination/worker"
    chmod +x "$destination/worker"
    printf '%064d  %s\n' 0 "$ARCHIVE" >"$TEST_ROOT/releases/$TEST_VERSION/$ARCHIVE.sha256"
    if run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination"; then
        fail "verification failure is returned"
    fi
    assert_file_equals "$destination/worker" 'keep-this-worker' \
        "verification failure preserves the existing binary"
    make_release_fixture
    printf '%s\n' 'ok - verification failure preserves an existing binary'
}

test_existing_binary_survives_download_failure() {
    destination="$TEST_ROOT/existing-download/bin"
    output="$TEST_ROOT/existing-download.out"
    mkdir -p "$destination"
    printf '%s\n' 'keep-this-worker-too' >"$destination/worker"
    chmod +x "$destination/worker"
    export FAIL_DOWNLOAD=$ARCHIVE
    if run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination"; then
        fail "download failure is returned"
    fi
    unset FAIL_DOWNLOAD
    assert_file_equals "$destination/worker" 'keep-this-worker-too' \
        "download failure preserves the existing binary"
    printf '%s\n' 'ok - download failure preserves an existing binary'
}

test_path_guidance_quotes_shell_metacharacters() {
    destination="$TEST_ROOT/bin-\$HOME-\`literal\`-\"quoted\"-'single"
    output="$TEST_ROOT/path-quoting.out"
    run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination" || \
        fail "installation into a metacharacter path succeeds"
    output_text=$(cat "$output")
    expected="export PATH='$TEST_ROOT/bin-\$HOME-\`literal\`-\"quoted\"-'\\''single':\"\$PATH\""
    assert_contains "$output_text" "$expected" \
        "PATH guidance safely single-quotes the installation directory"
    printf '%s\n' 'ok - PATH guidance quotes shell metacharacters'
}

test_worker_directory_is_rejected() {
    destination="$TEST_ROOT/directory-target/bin"
    output="$TEST_ROOT/directory-target.out"
    mkdir -p "$destination/worker"
    printf '%s\n' 'sentinel' >"$destination/worker/keep"
    if run_installer "$output" --version "$TEST_VERSION" --bin-dir "$destination"; then
        fail "existing worker directory is rejected"
    fi
    assert_file_equals "$destination/worker/keep" 'sentinel' \
        "existing worker directory is preserved"
    entries=$(find "$destination/worker" -mindepth 1 -maxdepth 1 -print | wc -l | tr -d ' ')
    [ "$entries" = 1 ] || fail "worker directory receives no staged binary"
    output_text=$(cat "$output")
    assert_contains "$output_text" 'is a directory' \
        "directory target reports the conflict"
    printf '%s\n' 'ok - existing worker directory is rejected'
}

run_packager() {
    output_path=$1
    shift
    set +e
    PATH="$TEST_ROOT/stubs:/usr/bin:/bin" \
        UNAME_S="${UNAME_S:-Darwin}" \
        UNAME_M="${UNAME_M:-arm64}" \
        FILE_ARCH="${FILE_ARCH:-arm64}" \
        MAC_WORKER_BINARY_PATH="$TEST_ROOT/package-worker" \
        "$PACKAGER" "$@" >"$output_path" 2>&1
    status=$?
    set -e
    return "$status"
}

test_packager_rejects_mismatched_version() {
    output_dir="$TEST_ROOT/mismatched-release"
    output="$TEST_ROOT/mismatched-release.out"
    if run_packager "$output" v0.2.0 "$output_dir"; then
        fail "packager rejects tag that differs from Cargo.toml"
    fi
    [ ! -e "$output_dir/mac-worker-v0.2.0-aarch64-apple-darwin.tar.gz" ] || \
        fail "mismatched tag produces no archive"
    output_text=$(cat "$output")
    assert_contains "$output_text" 'does not match Cargo.toml version 0.1.0' \
        "mismatched tag identifies Cargo version"
    printf '%s\n' 'ok - packager rejects mismatched version tags'
}

test_packager_rejects_incompatible_architecture() {
    output_dir="$TEST_ROOT/incompatible-release"
    output="$TEST_ROOT/incompatible-release.out"
    export UNAME_M=x86_64
    if run_packager "$output" v0.1.0 "$output_dir"; then
        fail "packager rejects incompatible host architecture"
    fi
    unset UNAME_M
    [ ! -e "$output_dir/mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz" ] || \
        fail "incompatible host produces no archive"
    output_text=$(cat "$output")
    assert_contains "$output_text" 'requires macOS on Apple Silicon' \
        "incompatible host reports supported build target"
    printf '%s\n' 'ok - packager rejects incompatible architectures'
}

test_packager_creates_release_assets() {
    output_dir="$TEST_ROOT/release-assets"
    output="$TEST_ROOT/release-assets.out"
    run_packager "$output" v0.1.0 "$output_dir" || \
        fail "packager creates release assets"
    archive="$output_dir/mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz"
    checksum="$archive.sha256"
    formula="$output_dir/mac-worker.rb"
    [ -f "$archive" ] || fail "packager creates archive"
    [ -f "$checksum" ] || fail "packager creates checksum"
    [ -f "$formula" ] || fail "packager creates a pinned formula"
    (cd "$output_dir" && shasum -a 256 -c "${checksum##*/}") >/dev/null || \
        fail "packager checksum verifies"
    members=$(tar -tzf "$archive")
    assert_contains "$members" 'mac-worker-v0.1.0-aarch64-apple-darwin/worker' \
        "archive contains worker"
    assert_contains "$members" 'mac-worker-v0.1.0-aarch64-apple-darwin/LICENSE' \
        "archive contains license"
    assert_contains "$members" 'mac-worker-v0.1.0-aarch64-apple-darwin/install.sh' \
        "archive contains installer"
    formula_text=$(cat "$formula")
    assert_contains "$formula_text" \
        'https://github.com/kirchik95/mac-worker/releases/download/v0.1.0/mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz' \
        "formula points at versioned GitHub release asset"
    printf '%s\n' 'ok - packager creates archive, checksum, and formula'
}

TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/mac-worker-install-test.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT HUP INT TERM
mkdir -p "$TEST_ROOT/home"
make_stubs
make_release_fixture
printf '%s\n' 'fixture-package-worker' >"$TEST_ROOT/package-worker"
chmod +x "$TEST_ROOT/package-worker"

test_verified_install_into_fresh_directory
test_bad_checksum_is_rejected
test_unsupported_platform_downloads_nothing
test_existing_binary_survives_verification_failure
test_existing_binary_survives_download_failure
test_path_guidance_quotes_shell_metacharacters
test_worker_directory_is_rejected
test_packager_rejects_mismatched_version
test_packager_rejects_incompatible_architecture
test_packager_creates_release_assets

printf '%s\n' '10 installer and packaging fixture tests passed'
