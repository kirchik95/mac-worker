# Releasing mac-worker

The first binary distribution target is macOS on Apple Silicon
(`aarch64-apple-darwin`). Release archives contain the `worker` executable,
the MIT license, and the checksum-verifying installer. The dashboard does not
need a separate build or runtime because its HTML, CSS, JavaScript, fonts, and
favicon are embedded in the Rust binary at compile time.

The release name for version `0.1.0` is:

```text
mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz
mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz.sha256
mac-worker.rb
```

This change prepares GitHub release and Homebrew assets. Generating these
files locally does not publish them; the external publication steps below
must be carried out separately.

## Prepare and validate a release

Update the package version in `Cargo.toml` and the default version in
`install.sh` in the same release change. Commit that change before creating
the corresponding `vX.Y.Z` tag. The packaging script rejects a tag that does
not exactly match the package version, a non-arm64 macOS host, or a binary
that is not a macOS arm64 executable.

From a clean checkout on an Apple Silicon Mac, run:

```sh
cargo fmt --all --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
scripts/test-install.sh
scripts/package-release.sh v0.1.0 dist
```

For a normal release, the package command runs
`cargo build --locked --release` before assembling the archive. The fixture
suite alone supplies `MAC_WORKER_BINARY_PATH` to exercise packaging with a
controlled executable and no Rust compilation. Inspect and verify the output:

```sh
file target/release/worker
tar -tzf dist/mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz
(cd dist && shasum -a 256 -c mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz.sha256)
ruby -c dist/mac-worker.rb
```

The archive listing must contain exactly one top-level directory with
`worker`, `LICENSE`, and `install.sh`. `file` must identify `worker` as a
Mach-O arm64 executable.

Before any GitHub release exists, exercise the installer against those local
assets by arranging the tag directory that a release server would provide:

```sh
release_fixture=$(mktemp -d "${TMPDIR:-/tmp}/mac-worker-release.XXXXXX")
mkdir -p "$release_fixture/v0.1.0"
cp dist/mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz* "$release_fixture/v0.1.0/"
MAC_WORKER_RELEASE_BASE_URL="file://$release_fixture" \
  sh install.sh --version v0.1.0 --bin-dir "$release_fixture/bin"
"$release_fixture/bin/worker" --version
```

This uses `curl` through the same installer path as GitHub, verifies the
published checksum file, and keeps the installed test binary inside the
temporary fixture. Remove that fixture after inspection.

## Create and publish the GitHub release

Create the signed or annotated release tag only after the release commit is
ready, then push that tag:

```sh
git tag -s v0.1.0 -m "mac-worker v0.1.0"
git push origin v0.1.0
```

Pushing the tag runs `.github/workflows/release.yml` on GitHub's arm64
`macos-14` runner. The workflow repeats formatting, tests, Clippy, and the
installer fixtures; builds the native archive; verifies its checksum; and
creates a **draft** GitHub release with all three generated assets. It does
not publish the release.

Review the workflow log, generated notes, archive contents, checksum, and
formula in the draft. Publish the draft explicitly in GitHub only after that
review. Once published, verify from a temporary directory:

```sh
gh release download v0.1.0 --repo kirchik95/mac-worker --dir /tmp/mac-worker-v0.1.0
(cd /tmp/mac-worker-v0.1.0 && shasum -a 256 -c mac-worker-v0.1.0-aarch64-apple-darwin.tar.gz.sha256)
```

On a clean Apple Silicon Mac, test the public installer without changing an
existing installation by choosing a temporary destination:

```sh
curl -fsSLO https://raw.githubusercontent.com/kirchik95/mac-worker/v0.1.0/install.sh
sh install.sh --version v0.1.0 --bin-dir /tmp/mac-worker-v0.1.0-bin
/tmp/mac-worker-v0.1.0-bin/worker --version
```

The normal installer destination is `$HOME/.local/bin`. It creates that
directory and prints the command needed to add it to `PATH` when necessary.

## Publish through an owned Homebrew tap

After the GitHub release is public, create or choose an owned tap repository,
following Homebrew's tap documentation. For a new
`kirchik95/homebrew-tap`, the local setup starts with:

```sh
brew tap-new kirchik95/tap
```

Copy the generated formula into that tap's `Formula` directory, then audit,
install, and test it before committing the tap:

```sh
tap_root=$(brew --repository kirchik95/tap)
cp dist/mac-worker.rb "$tap_root/Formula/mac-worker.rb"
brew audit --strict --online kirchik95/tap/mac-worker
brew install --formula "$tap_root/Formula/mac-worker.rb"
brew test kirchik95/tap/mac-worker
```

Only after these checks pass, commit and push the formula in the separate tap
repository. At that point the published install command is:

```sh
brew install kirchik95/tap/mac-worker
```

For each later release, generate the matching formula and repeat its audit,
installation test, tap commit, and tap push. Do not advertise a Homebrew
command until that tap repository and formula are actually public.
