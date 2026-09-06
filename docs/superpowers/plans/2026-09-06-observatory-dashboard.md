# Observatory dashboard implementation

Implement the approved Paper Overview and Settings artboards in the existing local dashboard. Reference exports: `/private/tmp/mac-worker-paper-overview.jsx` and `/private/tmp/mac-worker-paper-settings.jsx`.

## Global constraints

- Keep loopback-only, read-only dashboard routes and text-only rendering of remote content.
- Preserve snapshot ordering, bounded log requests, independent cursors, and selection race protection.
- Use real observations. Missing model/effort/configuration must remain unavailable. Cached authentication must not be presented as a current connection.
- Preserve task filters, task details/timeline, and legacy job inspection in working navigation views.
- Do not edit the pre-existing untracked `mac-worker-architecture.html`. Do not commit or push.

## Task 1: Safe backend settings projection

Own Rust files and Rust tests only; the coordinator owns static frontend assets and JavaScript tests. No sub-agents.

Extend the existing snapshot and task projection, reusing cached AgentFacts and ProjectSettings. Do not run credential probes or mutate workers.

Contract with frontend (snake_case JSON):
- TaskListRow: add nullable `model`, nullable `effort` (always null until genuinely recorded), nullable `permissions`, nullable `env_profile`. Read model/policy/profile from existing task metadata with existing redaction/bounds. Confirm latest turn model is used when a follow-up overrides model; do not report old model as active. Also DashboardActiveTask gets nullable model and effort, populated from task projection.
- DashboardWorker: add nullable `agent_facts` object with `collected_at_millis`, `freshness` (`current`/`stale`), `agents`. Each agent has `name`, nullable `version`, `auth` (`authenticated`/`unauthenticated`/`unknown`), `auth_by_profile` array of `{profile, auth}`. Explicit allowlist these safe fields; never serialize credentials, raw probe text, Git identity, private paths or arbitrary unknown auth reasons. Derive freshness using the existing AgentFacts TTL and re-evaluate when cached workers are served; frontend additionally treats stale/offline worker as unknown connection.
- DashboardSnapshot: add nullable `project_defaults` object: `default_agent`, `timeout_seconds`, `max_followups`, `source`, `publish` (string array), nullable `env_profile`, `permissions` (agent-to-policy map). Scope is dashboard launch directory; load read-only at launch and omit safely if unavailable. Missing .worker.toml should show actual built-in defaults, never Paper example values. Inject this through a default trait method or similarly backwards-compatible source path, so fixtures don't need real config reads. Document scope in README.

Add meaningful tests for safe projection, per-profile auth, own stale agent facts despite current worker, missing facts, launch defaults, and model changes if supported. Preserve existing serialization redaction properties. Update affected Rust struct fixtures. Run cargo fmt plus focused Rust tests; coordinate full-suite execution with parent. Write report `/private/tmp/mac-worker-backend-report.md` with changed files, interface deviations, commands/results, and concerns. No commits.

## Task 2: Approved frontend layout and interactions

Keep vanilla JS and existing polling engine. Implement responsive light Overview with compact summary, worker cards, queue, run progress and selected task console. Add working Overview, Tasks, Run history and Settings navigation. Build read-only Settings with worker selector, agent table, selected agent details, launch-directory defaults and cross-worker status. Select task from card/list/queue to inspect logs. Use IBM Plex Sans and Mono, local assets under existing CSP. Test navigation, missing settings, profile auth and stale statuses; keep existing injection, polling and cursor tests.

## Task 3: Verification and review

Run node --test tests/dashboard_client.mjs and cargo test --all-targets; cargo fmt --check. Visually verify all views using a controlled snapshot fixture in a browser at desktop and narrow widths, and verify embedded assets through actual Rust server. Review final diff for correctness and privacy; fix findings. Deliver concise Russian outcome and test evidence.

## Completed verification

- Implemented all four views with real snapshot data, local IBM Plex fonts, and responsive layouts.
- Full Rust suite passed; final affected dashboard tests passed after review fixes.
- All 27 frontend tests passed, including profile authentication, stale snapshots, task selection races, terminal log draining, and automatic retry.
- Chromium checks passed at 1440×900 and 390×844; embedded Rust server and font/CSP smoke checks passed.
- Final review approved after fixing clock-skew handling, timeout fallback freshness, and transient task-detail recovery.
