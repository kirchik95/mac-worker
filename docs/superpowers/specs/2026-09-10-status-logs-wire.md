# Combined status-logs wire (PERF 5.4)

Date: 2026-09-10. Base: `faf6eff437390dc1f996b498f9fb8aae48c6c83d`.
Status: implements root `protocol-decision.md`. Final integrated `PROTOCOL_VERSION` is 7. PERF does not bump the constant; FLOW owns the dedicated version commit. This track uses the shared constant.

## Goal

Replace per-poll `host status` + two `host log-chunk` calls with one optional `host status-logs` call. Keep typed `reported_checks` on TaskStatus/result (FLOW). Do not scrape display logs to preserve a v6 label.

## Host command

New clap subcommand: `worker host status-logs` (`HostOperation::StatusLogs`). Existing `status` and `log-chunk` stay.

Request (canonical JSON, `deny_unknown_fields`):

- `protocol_version` (must equal `PROTOCOL_VERSION`)
- `job_id`
- `stdout_offset`, `stdout_limit`
- `stderr_offset`, `stderr_limit`

Limits use the same caps as `log-chunk` (`MAX_LOG_CHUNK_BYTES`).

Response:

- `protocol_version`
- `status` (`StatusResponse` — job, not `TaskStatus`)
- `stdout` / `stderr` (`LogChunk`)

`task-status` remains a separate host command. Combined payload does not carry `reported_checks`.

## Mixed-version matrix (root decision)

| Direction | Contract |
|---|---|
| New v7 client → new v7 helper with `status-logs` | Combined JSON success. |
| New v7 client → v7 helper without `status-logs` | Narrow unsupported-command fallback: SSH not 255, **empty stdout**, stderr matches clap unknown-command for `status-logs`. Sequential `status` + two `log-chunk` for the rest of that follow. Capability is cached per worker identity, not globally on the client. |
| New v7 client/helper ↔ v6 peer | Existing `PROTOCOL_MISMATCH` at probe/preflight, before launch/mutate. Intentional upgrade boundary. Document client/helper upgrade together. **Not** a status-logs fallback success. |
| New client → broken matching-version helper | Truncated/malformed JSON, permission, auth, or decoded host JSON error: real failure. No fallback. |

Do not treat every `HOST_REQUEST_FAILED` as missing-command. Do not claim v6 wire compatibility. `serde(default)` is not old-reader compatibility.

## Runner behavior

`follow_remote` tries combined first. On unsupported command (matching protocol only), sequential for the rest of that follow. Preserve byte offsets, terminal two-phase drain/revalidation, cancellation, startup timeout, transport caps. Skip `WAIT_POLL` when a chunk progressed.

## Auth-incident integration (not in this branch)

External draft `71841f8` is withdrawn. Parent is reworking a bounded/streaming signature scan. PERF 5.5 will stream stdout/stderr with a bounded rolling window so a later matcher can scan fixed phrases without allocating 256 MiB. PERF does not implement `AgentAdapter::auth_failure_signatures` and will not cherry-pick that branch.

## SSH multiplexing

Inspect effective `ssh -G` ControlMaster/ControlPersist/ControlPath only. Do not write user SSH config. Do not invent multiplexing if already effective.
