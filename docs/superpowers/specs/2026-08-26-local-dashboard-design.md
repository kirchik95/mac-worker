# mac-worker local dashboard design

- Date: 2026-08-26
- Status: proposed for written review
- Repository: `mac-worker`
- User-facing commands: `worker dashboard`, later `worker watch`
- Depends on: durable job lifecycle and three-worker scheduler

## 1. Decision

Add a read-only operational dashboard after the three-worker scheduler and before the final artifacts, cache, Docker, garbage-collection, and hardening phase.

`worker dashboard` starts an ephemeral HTTP server on the MacBook and opens a browser view of the three Mac minis, the local queue, active jobs, recent jobs, and reconnectable logs. It reuses the existing Rust services and typed records directly. It does not shell out to the public CLI, install a daemon, expose an inbound service on a worker, or create a separate control plane.

The dashboard is an observer. The CLI, local locked state, remote durable job records, and remote leases remain authoritative. A dashboard crash or closed browser cannot start, stop, lose, or change a job.

## 2. Delivery order

The implementation sequence is:

1. project inspection, `.worker.toml`, and immutable local snapshots;
2. single-worker submission, durable supervision, status, and reconnectable logs;
3. three-worker FIFO scheduling, leases, cancellation, and reconciliation;
4. phase 4.5: the read-only local dashboard described here;
5. artifacts, caches, Docker profiles, safe GC, hardening, and the full acceptance matrix.

The dashboard is intentionally after scheduling. Before durable job and queue records exist, a UI could show machine health but could not truthfully answer which job was accepted, running, disconnected, or complete.

## 3. Goals

- Show at a glance whether each Mac mini is idle, busy, unavailable, or stale.
- Show the one-slot utilization of every worker and identify its active job when known.
- Show queue order, project, sanitized command summary, worker assignment, state, and elapsed time.
- Show system CPU utilization, free disk, memory pressure, swap, last successful observation, and capability mismatches.
- Show recent terminal jobs with duration, exit result, and artifact status.
- Reconnect to append-only logs without affecting the job.
- Keep all dashboard traffic and data on the MacBook and local network.
- Continue working partially when one worker is offline or slow.

## 4. Non-goals

- A hosted or remotely accessible website.
- A persistent dashboard daemon or service installed at login.
- A second scheduler, database, or source of truth.
- Multi-user authentication, roles, quotas, or shared queues.
- Prometheus, Grafana, Kubernetes, or a long-term metrics warehouse.
- Editing projects or files on a worker.
- Starting, cancelling, retrying, or deleting jobs in the first dashboard release.
- Exact process-level CPU attribution or energy accounting in the first release.
- Rendering secret environment values or unsanitized command metadata.

Mutating controls such as Cancel and Retry may be designed after the read-only surface proves reliable. They require CSRF protection, an explicit confirmation model, idempotent APIs, and the same authorization boundary as their CLI equivalents.

## 5. User experience

The entry point is:

```text
worker dashboard [--port PORT] [--no-open]
```

The server always binds to `127.0.0.1`. By default the OS selects an available port and the CLI opens the resulting URL in the default browser. `--no-open` prints the URL without opening it. `--port` requests a specific loopback port and fails clearly if it is unavailable. V1 exposes no option to bind a non-loopback address.

The main view contains:

1. **Worker cards** — name, idle/busy/offline/stale state, slot usage, active job, elapsed time, system CPU utilization, memory pressure, swap, free disk, capabilities, and last observation.
2. **Queue** — FIFO position, project display name, sanitized command, age, requirements, and blocking reason.
3. **Active and recent jobs** — state, assigned worker, timestamps, duration, exit result, and artifact status.
4. **Job detail** — immutable job identity, source digest, lifecycle timestamps, infrastructure errors, and reconnectable stdout/stderr.

Busy/idle slot state is the primary load signal. Host metrics explain whether a nominally idle machine is under CPU, memory, swap, or disk pressure. A short in-memory sparkline may show recent observations, but no historical metrics database is introduced.

An optional later `worker watch` command renders the same snapshot in the terminal. It is a second renderer over the dashboard service, not a separate monitoring implementation.

## 6. Architecture

The dashboard adds four local components to the existing Rust codebase:

1. **Dashboard service** — concurrently assembles a typed `DashboardSnapshot` from local queue/job state and bounded remote observations.
2. **Observation cache** — retains the last good observation and a small in-memory sample window for each worker.
3. **Loopback HTTP adapter** — serves versioned JSON and bounded log ranges from the dashboard service.
4. **Embedded web client** — static HTML, CSS, and JavaScript compiled into the `worker` binary.

The dashboard service calls library interfaces directly. It must not spawn `worker workers`, parse human-readable output, duplicate scheduler rules, or infer job state from process names.

No new process runs on a Mac mini. Existing host-helper operations provide authoritative lease, job, log, and health data over the same hardened SSH transport used by the CLI.

## 7. Data model and authority

`DashboardSnapshot` contains:

- generation time and a monotonically increasing local revision;
- one worker summary per configured worker;
- the ordered local queue;
- active jobs;
- bounded recent terminal jobs;
- partial collection errors.

A worker summary contains its configured identity, observed hostname, health, slot state, active job ID when known, capabilities, optional system CPU busy percentage, memory pressure, swap usage, free disk, last-success time, and freshness state.

The data authority order is:

1. remote durable job status for accepted and running jobs;
2. remote lease state for worker occupancy;
3. locked local queue state for jobs not yet accepted;
4. cached observations only when a current query fails.

Cached data is always labelled with its timestamp and never presented as current. The UI distinguishes `offline`, where a current query failed with no usable observation, from `stale`, where the last good observation is shown after a current failure.

Project display names use the local worktree basename plus a short worktree identifier. Full local filesystem paths are omitted from the default list view. Command summaries use the same sanitized metadata as `worker status`; environment values are never returned.

## 8. Collection and refresh

The browser polls a single snapshot endpoint every two seconds. The local service coalesces overlapping refreshes so multiple browser tabs do not multiply SSH traffic. Worker observations run concurrently with per-host deadlines and a global collection deadline.

Each host observation returns cumulative system CPU counters through a typed host-helper response. The dashboard service computes CPU busy percentage from consecutive counter deltas; the first successful sample has no percentage and is rendered as warming up. Counter resets or non-increasing totals discard that interval rather than displaying a fabricated value. The worker never sleeps merely to produce a CPU sample.

The first release uses bounded polling rather than WebSockets or Server-Sent Events. This keeps reconnect, backpressure, shutdown, and test behavior simple. Log detail requests use explicit byte offsets and maximum byte counts, matching the reconnectable log contract. The browser polls an open log view once per second and stops when the view closes or the job reaches a terminal state.

The observation cache holds at most five minutes of two-second samples per worker in memory. It is discarded when `worker dashboard` exits. Recent job history comes from normal mac-worker state and follows its retention policy.

## 9. HTTP surface

The loopback adapter exposes only versioned read operations:

```text
GET /                         embedded application shell
GET /assets/*                 embedded immutable assets
GET /api/v1/snapshot          current assembled DashboardSnapshot
GET /api/v1/jobs/{job_id}     authoritative job detail
GET /api/v1/jobs/{job_id}/logs?stream=stdout&offset=N&limit=N
GET /api/v1/jobs/{job_id}/logs?stream=stderr&offset=N&limit=N
```

Job IDs are parsed as typed identifiers before lookup. Log offsets and limits are bounded; malformed or excessive ranges are rejected. The server never accepts arbitrary paths, SSH destinations, commands, or shell text from HTTP requests.

All API errors use stable codes and JSON bodies. One failed worker observation appears as partial data in a successful snapshot response; it does not fail the entire dashboard.

## 10. Security and privacy

- Bind only to `127.0.0.1`; do not listen on wildcard, LAN, or IPv6 interfaces in the first release.
- Generate a random port by default and print the exact URL.
- Serve no third-party scripts, fonts, analytics, CDNs, or network requests.
- Send a restrictive Content Security Policy, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and `Cache-Control: no-store` for API and log responses.
- Do not enable CORS. Validate the request Host against the actual loopback listener address.
- Render all project names, command summaries, errors, and logs as text rather than HTML.
- Preserve the existing SSH host-key checks, no-agent-forwarding rule, timeouts, and output bounds.
- Return no environment values, credential contents, local SSH configuration, or undisclosed local paths.

Logs belong to the user's trusted jobs and can still contain application-emitted secrets. The loopback-only, process-lifetime server matches the existing single-user trust model but is not a sanitizer for application output.

If mutating endpoints are added later, the server must add a per-process unguessable token, same-origin enforcement, explicit confirmations for destructive actions, and idempotency keys. Read-only V1 does not pre-design those endpoints.

## 11. Failure behavior

- An unreachable worker becomes offline or stale while other worker cards and local queue data continue updating.
- A timeout or malformed host response is shown with its stable transport/protocol error; raw unbounded stderr is not sent to the browser.
- If local state is locked briefly, the service returns the last snapshot with a stale marker rather than blocking the HTTP server indefinitely.
- If no previous snapshot exists, the API returns partial data plus collection errors.
- A browser disconnect cancels only its local response work, never a remote job.
- Closing the dashboard process releases the loopback port and leaves all jobs unchanged.
- A missing or expired retained job produces a typed not-found response rather than reading arbitrary state paths.

## 12. Testing

Automated coverage includes:

- dashboard snapshot assembly, authority ordering, queue order, stale transitions, and bounded history;
- CPU delta calculation, first-sample behavior, counter reset handling, and percentage bounds;
- redaction of paths, commands, errors, and environment metadata;
- partial success when any subset of workers is unavailable;
- refresh coalescing and per-host/global deadlines;
- typed job IDs and log range boundary tests;
- loopback-only binding, Host validation, security headers, no CORS, and asset content types;
- HTTP integration tests using fake local state and an injected remote transport;
- browser tests for worker cards, queue updates, job details, log offset continuation, stale states, and safe text rendering;
- proof that opening, refreshing, and closing the dashboard cannot change job or lease state.

Live acceptance against the three configured Mac minis requires:

1. all three idle workers appear within five seconds;
2. three submitted jobs produce three busy cards with correct job and project identities;
3. a fourth job appears in FIFO queue order with its blocking reason;
4. active logs resume after a browser refresh without duplicated bytes;
5. stopping SSH access to one worker marks only that worker stale/offline;
6. terminal state and exit result match `worker status` for every job;
7. dashboard shutdown leaves all jobs running and queryable from the CLI;
8. no request is accepted through a non-loopback listener.

## 13. Completion boundary

The dashboard phase is complete when the read-only browser view satisfies the live acceptance criteria, all API and browser tests pass, and the CLI remains fully usable with the dashboard stopped.

Cancel, retry, job deletion, public/LAN access, persistent telemetry, notifications, and multi-user access remain outside this design and require explicit follow-up review.
