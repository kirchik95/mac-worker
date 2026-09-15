# Dashboard motion

Emil design pass on the approved Paper UI. Interaction motion uses existing Base UI
primitives and CSS transitions; no animation dependency is added.

| Interaction | Purpose | Timing and properties |
| --- | --- | --- |
| Notifications | Show the sidebar entering and leaving from the right | Transform + opacity, 240 ms in / 180 ms out, `--ease-drawer` |
| Selects | Connect the menu to its trigger | Scale 0.97 + opacity, 180 ms in / 120 ms out, Base UI transform origin |
| Disclosures | Follow the content being revealed | Measured height + opacity, 200 ms in / 180 ms out; chevron rotates |
| Buttons, switches, copy confirmation | Immediate feedback | 120 ms; press scale 0.97, thumb transform, Copy/Copied crossfade without width shift |
| Filters and agent selection | Identify the active choice | 120 ms background opacity; content changes immediately |
| Working Mac | Indicate observed activity | Existing 3 s LED and warm shadow cycle, paused offscreen and in hidden tabs |
| Mac hover | Subtle pointer feedback | 2 CSS px lift, 120 ms, independent of keyboard interaction durations |
| Worker report | Indicate a newer current observation | One 180 ms half-turn of the two-arrow icon, `--ease-out`; never a continuous spinner |
| Save, reply, accept | Report the actual request state | 120 ms opacity; all labels share one grid cell to preserve button width |
| New question or review | Bring a newly actionable turn to attention | Sonner, 8 px + opacity, 180 ms in / 120 ms out; one grouped toast, 6 s duration |

`useInputModality` sets zero interaction durations for keyboard and assistive
activation, including portal content. Frequent page navigation and log output are
instant. Reduced motion removes displacement and continuous pulses, retaining
gentle opacity/color feedback. Height animation is confined to disclosures; Base UI
measures the content and retains it through exit. Closing content is immediately
inert, and lazy log panels unmount after exit. Reopening retargets the transition.

The existing 650 ms green signal indicates that a working Mac became available;
it is not evidence that a task passed review. Idle or stale workers never pulse.

The worker report icon moves only when `observed_at_millis` advances on a current
worker. Initial mount, clock ticks, duplicate/out-of-order timestamps and stale
reports are silent. Keyboard modality and reduced motion skip the turn. Report
and agent tooltips use a 300 ms initial hover delay, then open immediately when
moving between nearby icons; keyboard focus is immediate and tooltips do not animate.
Agent logos use dark foreground by default and gray only for a current explicit
sign-in requirement. Current task activity stays in the tooltip and task/slot text.

The log viewport is capped at `min(320px, 45dvh)`. Appended output follows the
bottom only while the reader stays there; scrolling up pauses following. Jump to
latest restores following and focuses the viewport without a second scroll.
Log scrolling is always instant. Write follow-up focuses the composer with
`preventScroll`; keyboard and reduced-motion users get instant page scrolling.

Notification announcements are keyed by task, turn count and actionable state,
not polling timestamps. Initial history, offline/stale collection recovery,
individual stale-task recovery, the task already on screen and an open notification
center are silent. Unresolved items remain available in the center. A subsequent
new turn can announce again. No OS notification permission is requested.

Paper alignment now uses one result panel and a full-width follow-up composer,
the shelf table's internal heading and 56 px rows, inline Tasks filters/CLI and a
table shell for no matches. Empty-state content stays inside the visible table
viewport at narrow widths. Questions remain globally reachable through filters.
Run-limit messages require a current task with `RUN_MAX_PARALLEL`; progress counts
alone are not treated as proof that a limit is blocking dispatch.

Settings keeps separate in-memory drafts and original revisions for each Mac and
agent while the Settings view is mounted. Pending controls are disabled. Switching
editors cannot apply a late save to another editor. Cancel explicitly discards that
editor's draft; a rejected revision preserves it. These drafts do not survive a
page reload.

Verification: UI tests cover input modality and listener cleanup; existing workflow
tests cover task actions, settings, notification persistence and Escape focus return.
Diagnostic tests open their disclosure before inspecting its content. Browser checks
cover pointer/keyboard drawer and disclosure behavior, activity state, controls, and
responsive layout at 390 px. With Chrome's reduced-motion emulation enabled, all Mac
LED animation names are `none`; after closing DevTools they return to `mw-working`
for the two busy example Macs and `none` for the idle Mac. Three quick disclosure
toggles finish expanded and show the metrics; the next closes normally.

Additional browser verification: compared shelf and review composition at the
Paper desktop size; checked Tasks no matches and running logs at 390 px. Settings
retains Fast mode edits across agent and Mac changes. A temporary component harness
measured the save button at 138.27 px in idle, pending, success and error states;
the harness was removed. Mac hover retains a 120 ms transition after keyboard use.
The runtime toast uses the intended 180 ms transform/opacity transition and 32 px
action button. No task mutations were sent to the live API.

Motion review of this change:

| Before | After | Why |
| --- | --- | --- |
| Keyboard modality also zeroed later Mac hover | Dedicated hover duration (`ui/src/index.css:784`) | Moving the pointer after typing no longer snaps the illustration |
| Request labels changed button width | Overlapping grid labels (`ui/src/components/ActionFeedback.tsx`) | Pending/success/error feedback preserves the surrounding layout |
| Follow-up always scrolled smoothly | Modality and reduced-motion aware scroll (`ui/src/views/TaskDetail.tsx:378`) | Frequent keyboard actions remain immediate |
| Sonner defaults animate height and use 400 ms transitions | Explicit transform/opacity, 180/120 ms (`ui/src/components/ui/sonner.css:6`) | Short, interruptible feedback consistent with the dashboard |

Verdict: approve for the changed motion. Reduced motion removes displacement and
spinner rotation; hover displacement remains gated by pointer/hover capability.
Existing measured-height disclosure behavior is unchanged.

Validation: 171 UI tests and 12 `dashboard_web` Rust tests pass. The HTTP tests
require loopback binding outside the filesystem sandbox. TypeScript/Vite build
passes; lint exits successfully with 18 existing warnings and no new warnings.
The embedded JS bundle remains over Vite's 500 kB warning threshold (548.33 kB,
175.94 kB gzip). Independent review found a per-task recovery notification case;
it is fixed and covered by two regression cases.

Latest validation (2026-09-16): 226 UI tests pass after the compact agent controls,
model search and local preview proxy updates. Production build and formatting
checks pass. The bundle is 621.15 kB (199.79 kB gzip); the existing size warning and
18 lint warnings remain. Keyboard checks in Chrome confirmed both tooltip types;
the example shelf confirmed 16 px logos and gray only for reported sign-in-needed
agents. Paper's Shelf matches the compact controls.

A disposable project's live Codex task completed question → dashboard reply →
review → Accept. It ran 15 existing tests, reported no changed files, and the local
project stayed clean. The check found and fixed a dev proxy Origin mismatch and
the backend retaining an answered question after a Done outcome. The latter has
a failing-then-passing resume regression; explicit questions on Done remain valid.
