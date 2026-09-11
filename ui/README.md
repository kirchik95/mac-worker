# mac-worker dashboard UI

React + Tailwind + shadcn/ui front end for the dashboard API, developed against
the Rust loopback server rather than embedded in it.

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

## Layout

- `src/lib/api.ts` — types mirroring the Rust snapshot projection, and fetch helpers
- `src/hooks/useSnapshot.ts` — two-second snapshot polling, matching the Rust client
- `src/views/` — one file per view
- `src/components/ui/` — shadcn components, owned by this repository
