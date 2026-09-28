# mac-worker

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

The primary user is an individual developer with a personal pool of Macs. They delegate coding tasks to agents, follow progress, answer questions, and review the resulting Git branches. The user confirmed this personal-tool focus during Impeccable init on 2026-09-13.

## Product Purpose

Send a coding task to another Mac and get a reviewable Git branch back. mac-worker lets a developer use their own machines for agent work while keeping control of the source, task instructions, and integration of the result.

Successful use means the developer can tell what is running, why work is waiting, what needs their input, and which result is ready to inspect. Accepting a task and merging its code are separate decisions.

## Positioning

mac-worker runs existing coding agents on the developer's own Apple Silicon Macs over SSH. Each task has an isolated Git worktree; results are available as Git refs for local review and integration. Workers use their installed tools and agent accounts. No mac-worker cloud service or database server is required.

## Operating Context

- The developer works from a laptop and delegates to one or more worker Macs. The documented setup requires Git, SSH key access, and an installed, authenticated coding agent on the worker.
- Tasks are submitted through the CLI or the repository's local agent skills. Codex, Cursor, OpenCode, and Claude Code are supported agent choices; availability depends on worker configuration and observed facts.
- The normal workflow is submit, monitor, answer when needed, inspect the result, fetch its Git ref, and decide what to merge.
- The web dashboard runs locally in a browser. The default queue and controller are on the laptop. An optional remote controller keeps coordination on an always-on Mac; the laptop dashboard connects through a managed SSH local-forward.
- The implementation is a Rust CLI and dashboard API with a React/TypeScript frontend using Vite, Tailwind, and shadcn/ui. Frontend development instructions are in `ui/README.md`.

## Capabilities and Constraints

- The dashboard shows workers, slot occupancy, host load, agent facts, queued tasks, active turns, task details and logs, and run history.
- A worker is a Mac. A slot is execution capacity on that worker. A task has its own workspace and can contain multiple agent turns. A run groups tasks and can limit parallel execution independently of free worker slots.
- Free capacity does not imply eligibility for every task. Compatibility, agent facts, worker pins, and run limits affect scheduling. Explain the reported reason rather than inventing one.
- Distinguish a current observation from a stale or unknown one. An installed agent, authenticated agent, available slot, completed turn, and delivered result are separate facts.
- The current dashboard supports replying to a task, accepting a task, and saving supported native agent defaults. Replies and acceptance require the current task-card revision; stale submissions are rejected.
- The current dashboard does not submit or cancel tasks. Agent sign-in takes place on the worker; a browser sign-in flow is not established functionality.
- Dashboard access is through loopback, directly or via the controller tunnel. Do not assume hosted accounts, team roles, or a shared multi-user service.
- The operator supplies project tools, agent credentials, and required permissions. Git review and merge remain under the developer's control.

## Brand Commitments

The established product name is `mac-worker`. The repository contains an existing wordmark component and favicon. The current UI uses English; documentation is available in English and Russian. Further localization requirements are undecided.

## Evidence on Hand

- `README.md`, `README.ru.md`, `docs/getting-started.md`, and `docs/usage.md`: product description, setup, workflows, and current limitations.
- `ui/src/App.tsx`, `ui/src/views/`, and `ui/src/lib/`: implemented navigation, actions, and state interpretation. Prefer current implementation and usage documentation when an older design spec disagrees.
- `ui/README.md`, `ui/package.json`, and `ui/vite.config.ts`: frontend stack and development entry points.
- `docs/images/dashboard.png`, `ui/src/components/Wordmark.tsx`, and `ui/public/favicon.svg`: existing UI and identity assets.
- [Paper: mac-worker — Pool Dashboard](https://app.paper.design/file/01M1V8VWPMWHMNASQ70NEE0D4A/5-0), artboard **I.2 · Isometric shelf — action first**: the current design proposal discussed with the user. Its machine readings, task examples, and authentication states are illustrative. Proposed controls are not evidence of shipped behavior.

## Product Principles

1. Preserve the developer's ownership of machines, agent accounts, source code, and the decision to merge a result.
2. Make waiting work and requests for human input understandable and actionable.
3. Report observed state faithfully, including freshness, uncertainty, and the difference between capacity and eligibility.
4. Name each action by the destination it opens or the effect it performs. Keep navigation distinguishable from operations that change task state.
5. Keep product promises grounded in implemented capabilities and real evidence; mark proposed behavior explicitly.

## Open Decisions

- Product-specific accessibility requirements and supported narrow-screen workflows have not been established with the user.
- Additional team workflows and UI localization beyond the current personal, English-language dashboard are not confirmed requirements.
