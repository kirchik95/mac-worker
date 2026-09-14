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

`useInputModality` sets zero interaction durations for keyboard and assistive
activation, including portal content. Frequent page navigation and log output are
instant. Reduced motion removes displacement and continuous pulses, retaining
gentle opacity/color feedback. Height animation is confined to disclosures; Base UI
measures the content and retains it through exit. Closing content is immediately
inert, and lazy log panels unmount after exit. Reopening retargets the transition.

The existing 650 ms green signal indicates that a working Mac became available;
it is not evidence that a task passed review. Idle or stale workers never pulse.

Verification: UI tests cover input modality and listener cleanup; existing workflow
tests cover task actions, settings, notification persistence and Escape focus return.
Diagnostic tests open their disclosure before inspecting its content. Browser checks
cover pointer/keyboard drawer and disclosure behavior, activity state, controls, and
responsive layout at 390 px. With Chrome's reduced-motion emulation enabled, all Mac
LED animation names are `none`; after closing DevTools they return to `mw-working`
for the two busy example Macs and `none` for the idle Mac. Three quick disclosure
toggles finish expanded and show the metrics; the next closes normally.

Validation: 146 UI tests and 12 `dashboard_web` Rust tests pass. TypeScript/Vite build
passes; lint exits successfully with the existing 19 warnings. The embedded JS
bundle remains over Vite's 500 kB warning threshold (507.79 kB, 164.37 kB gzip).
