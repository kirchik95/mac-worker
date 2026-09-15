# Next UI/UX pass

Priorities from the live dashboard check on 2026-09-16. These are proposals for
the next pass, not implemented changes. Keep the personal-developer workflow:
submit in the CLI, answer and review in the dashboard, merge deliberately.

| Priority | Before | Proposed after | Why |
| --- | --- | --- | --- |
| 1 · Shelf | With no current tasks, the shelf still fills with old abandoned tasks. | Show current work by default; provide a compact link to task history. | Opening the dashboard should immediately reveal what needs a decision today. |
| 2 · Drafts | Replies are component state; Settings drafts survive switching Mac/agent only while the view stays mounted. | Restore unsent replies and settings drafts across navigation and reload, keyed by task or Mac/agent and original revision. Explicit Cancel clears them. | Reading logs or another task should not discard work. Preserve conflict checks when restoring an old draft. |
| 3 · Review | The result lists changed files and terminal commands; inspecting the actual patch requires leaving the dashboard. | Add a readable file diff to the existing review panel, with checks and Accept nearby. Keep terminal commands available. | A developer can assess a small result in one place. Accept must continue to mean closing the task, not merging its branch. |

Start with the shelf and draft persistence. Explore the diff composition in Paper
with a comp before implementing a larger review-layout change. Additional motion
is not a priority: feedback should serve actual loading, saving and state changes.
