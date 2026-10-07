# mac-worker dashboard UI

React + Tailwind + shadcn/ui front end for the dashboard API, developed against
the Rust loopback server rather than embedded in it.

Before changing how the dashboard looks or reads, read [`PRODUCT.md`](../PRODUCT.md)
and [`DESIGN.md`](../DESIGN.md).

## Running

Start the API in one shell, from the repository root:

```bash
worker dashboard --port 9173 --no-open
```

Then the UI in another:

```bash
cd ui
npm install
npm run dev
```

Vite serves on `http://localhost:5173` and proxies `/api` to the dashboard.
Point it elsewhere with `DASHBOARD_ORIGIN=http://127.0.0.1:<port> npm run dev`.
The proxy keeps the browser same-origin, which matters because the dashboard
sends no CORS headers and validates the request Host against its own listener.

## Tests

```bash
npm test
```

Vitest with jsdom and Testing Library. The suite pins the behaviour the Rust
client used to guarantee: filters stay local and issue no request, a failed poll
keeps the last snapshot instead of blanking the page, a settings draft survives a
rejected revision, and every remote string renders as text rather than markup.

## Generated assets

The compiled dashboard bundle lives in `../src/dashboard/static/app/` and is
committed. `cargo build` embeds it (`include_str!` / `include_bytes!` in
`src/dashboard/web.rs`) and never runs a JavaScript toolchain. Rebuild in one
place only, after the UI source you intend to ship is in the tree, and commit
the whole `app/` tree in that same change, fonts included
(`assets/inter-variable.ttf`, `assets/plex-mono-regular.ttf`). A second rebuild
in another worktree will fight that commit.

```bash
cd ui
npm ci
npm test
npm run lint
npx tsc -b
npm run build
```

`npm run build` is `tsc -b && vite build`. The explicit `npx tsc -b` is the
check the plan runs before that one production build. Vite writes
`../src/dashboard/static/app` with stable names (no content hashes) and
`emptyOutDir`. Do not commit the output of `npm run dev`.

CI (`.github/workflows/ci.yml`, Node 22) installs with `npm ci`, runs
`npm test` and `npm run lint`, then checks parity with a second build that
does not replace the committed tree:

```bash
asset_dir="$(mktemp -d)"
npm run build -- --outDir "$asset_dir"
diff -r "$asset_dir" ../src/dashboard/static/app
```

The diff is the whole tree, not only JS and CSS. `npm run build` passes
`--outDir` through to Vite after `tsc -b`.

## Layout

- `src/lib/api.ts` — types mirroring the Rust snapshot projection in `../src/dashboard/model.rs` (change both together), and fetch helpers
- `src/hooks/useSnapshot.ts` — event-driven snapshot refreshes, a 15-second healthy anti-entropy poll, and two-second fallback polling
- `src/views/` — one file per view
- `src/components/ui/` — shadcn components, owned by this repository
