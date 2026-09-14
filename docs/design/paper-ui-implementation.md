# Paper UI implementation

The approved Paper dashboard is implemented on `feat/paper-ui` in `.worktrees/paper-ui`.

## Design sources

- [Application screens](https://app.paper.design/file/01M1V8VWPMWHMNASQ70NEE0D4A/5-0): I.6 shelf, J.1–J.8 and notification drawer.
- [Design System](https://app.paper.design/file/01M1V8VWPMWHMNASQ70NEE0D4A/7-0): foundations, typography, controls, status, patterns and brand.
- Original selected brand, Mac illustration and agent marks are preserved in `paper-sources/`.
- Inter is served locally; its license is `ui/src/assets/fonts/Inter-OFL.txt`.

## Frontend

React 19 + TypeScript 6, Vite 8, Tailwind CSS 4, shadcn components on Base UI, Lucide React and Sonner. No runtime dependency was added.

The shared tokens and layout styles live in `ui/src/index.css`. Components cover the shelf, named questions, review cards, Mac slots, task status/actions and notifications. Mac activity uses a 3-second signal, a 650 ms completion signal and a 2 px hover lift. Hidden/offscreen activity pauses; reduced-motion CSS disables animation and lift.

Tasks have local search, state/outcome/run filters and explicit keyboard-accessible actions. The selected run survives opening a task and returning. Task detail has question, review and running variants. Partial files, checks and commands remain available while an agent waits for a reply.

Settings preserve native revisions, model/effort constraints and unsaved drafts after rejected saves. Contextual setup selects its Mac/agent without resetting a subsequent manual choice during polling. Unavailable defaults render as read-only facts. Permissions and environment profile are explicitly project launch context; authentication and CLI version come from worker observations.

Notification read state is local to this browser and never resolves a task. A new task revision becomes unread. Recent activity comes from current task snapshots; this is not a durable server event history.

## Run and build

From the implementation worktree:

```sh
cd ui
npm run dev -- --host 127.0.0.1 --port 5174
```

- `http://127.0.0.1:5174/?example` — complete design preview.
- `http://127.0.0.1:5174/?example=empty#/tasks` — empty Tasks.
- Without `?example`, Vite proxies the API to `http://127.0.0.1:9173`. Set `DASHBOARD_ORIGIN` to the loopback URL printed by `worker dashboard` if it differs.

Example detail/settings/log responses are local fixtures. Mutations are rejected before any network request.

`npm run build` writes the embedded assets into `src/dashboard/static/app`. Rebuild the Rust binary to embed them. The font allowlist now serves Inter and Plex Mono.

## Verification

- 144 frontend tests pass across 16 files.
- TypeScript and production Vite build pass.
- Oxlint exits successfully: zero errors, 19 warnings, primarily existing React hook/export patterns.
- 12 dashboard HTTP tests pass; `cargo check --offline`, `cargo fmt --check` and `git diff --check` pass.
- Browser inspection at 1440 px and 390 px covered shelf, Tasks, review, questions, live output, Runs, settings/auth, empty Tasks and the notification drawer.
- Checked keyboard task activation, notification Escape/focus return, local read persistence, new unread revisions, and animation stopping offscreen.
- The original reply/accept revision guards, stale-poll suppression and settings draft tests remain green.
- Independent review found four issues; all were fixed with regression tests: contextual Mac reset, hidden partial results, lost run filter, and live missing-agent capability codes.

Vite reports its size advisory for the single ~502 kB minified JS bundle (~162 kB gzip). The embedded server currently serves a fixed JS entry. The interface was exercised with fixtures; no real agent task was replied to or accepted during verification.
