# mac-worker

`mac-worker` is a personal remote-execution tool for dispatching heavy local-development commands from a MacBook to a small pool of trusted Mac mini workers.

## Phase-one quick start

First provision each host according to the [macOS worker setup guide](docs/setup-macos-worker.md). In particular, the worker alias must support non-interactive SSH with the existing macOS account selected for worker jobs before running setup. A separate worker-only account is optional hardening, not a prerequisite.

```bash
cargo test --all-targets
cargo build --release
mkdir -p ~/.config/mac-worker
cp config.example.toml ~/.config/mac-worker/config.toml
./target/release/worker setup mini-1
./target/release/worker workers
./target/release/worker --json workers | jq .
```

At this checkpoint, `worker run`, snapshots, queues, logs, and artifacts are not yet implemented.
