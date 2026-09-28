---
name: mac-worker
description: Implementation target extracted from the approved Paper design system.
colors:
  action: '#181D27'
  action-hover: '#0A0D12'
  canvas: '#FCFCFD'
  surface: '#FFFFFF'
  text: '#101828'
  text-secondary: '#475467'
  text-control: '#344054'
  text-muted: '#667085'
  border: '#EAECF0'
  border-control: '#667085'
  border-secondary: '#D0D5DD'
  neutral-hover: '#F9FAFB'
  neutral-pressed: '#F2F4F7'
  attention: '#B54708'
  attention-bg: '#FFFAEB'
  attention-signal: '#F79009'
  success: '#027A48'
  success-bg: '#ECFDF3'
  error: '#B42318'
  error-bg: '#FEF3F2'
typography:
  page:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 28px
    fontWeight: 600
    lineHeight: 36px
    letterSpacing: -0.02em
  question:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 20px
    fontWeight: 600
    lineHeight: 24px
  section:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 18px
    fontWeight: 600
    lineHeight: 24px
  body:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 14px
    fontWeight: 400
    lineHeight: 20px
  label:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 13px
    fontWeight: 500
    lineHeight: 20px
  metadata:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 12px
    fontWeight: 400
    lineHeight: 20px
  chip:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 12px
    fontWeight: 500
    lineHeight: 18px
  navigation:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 15px
    fontWeight: 400
    lineHeight: 18px
  mono:
    fontFamily: Menlo, ui-monospace, monospace
    fontSize: 14px
    fontWeight: 400
    lineHeight: 20px
  mono-metadata:
    fontFamily: Menlo, ui-monospace, monospace
    fontSize: 12px
    fontWeight: 400
    lineHeight: 20px
  field:
    fontFamily: Inter, system-ui, sans-serif
    fontSize: 13px
    fontWeight: 400
    lineHeight: 20px
rounded:
  control: 6px
  filter: 8px
  panel: 10px
spacing:
  '4': 4px
  '8': 8px
  '12': 12px
  '16': 16px
  '20': 20px
  '24': 24px
  '32': 32px
  '40': 40px
components:
  button-primary:
    backgroundColor: '{colors.action}'
    textColor: '{colors.surface}'
    typography: '{typography.label}'
    rounded: '{rounded.control}'
    padding: 8px 14px
    height: 38px
  button-primary-hover:
    backgroundColor: '{colors.action-hover}'
  button-primary-active:
    backgroundColor: '{colors.action-hover}'
  button-primary-disabled:
    backgroundColor: '{colors.neutral-pressed}'
    textColor: '{colors.text-muted}'
    typography: '{typography.label}'
    rounded: '{rounded.control}'
    padding: 8px 14px
    height: 38px
  button-secondary:
    backgroundColor: '{colors.surface}'
    textColor: '{colors.text-control}'
    typography: '{typography.label}'
    rounded: '{rounded.control}'
    padding: 8px 14px
    height: 38px
  button-secondary-hover:
    backgroundColor: '{colors.neutral-hover}'
  button-secondary-active:
    backgroundColor: '{colors.neutral-pressed}'
  button-secondary-disabled:
    backgroundColor: '{colors.neutral-pressed}'
    textColor: '{colors.text-muted}'
    typography: '{typography.label}'
    rounded: '{rounded.control}'
    padding: 8px 14px
    height: 38px
  button-text:
    backgroundColor: transparent
    textColor: '{colors.text-control}'
    typography: '{typography.label}'
    rounded: '{rounded.control}'
    padding: 8px 14px
    height: 38px
  button-text-active:
    backgroundColor: '{colors.neutral-pressed}'
  button-text-disabled:
    backgroundColor: '{colors.neutral-pressed}'
    textColor: '{colors.text-muted}'
    typography: '{typography.label}'
    rounded: '{rounded.control}'
    padding: 8px 14px
    height: 38px
  input:
    backgroundColor: '{colors.surface}'
    textColor: '{colors.text-control}'
    typography: '{typography.field}'
    rounded: '{rounded.control}'
    padding: 9px 12px
    height: 40px
  input-disabled:
    backgroundColor: '{colors.neutral-pressed}'
    textColor: '{colors.text-muted}'
  navigation:
    backgroundColor: '{colors.surface}'
    textColor: '{colors.text-secondary}'
    typography: '{typography.navigation}'
  navigation-current:
    textColor: '{colors.text}'
  status-attention:
    backgroundColor: '{colors.attention-bg}'
    textColor: '{colors.attention}'
    typography: '{typography.chip}'
    rounded: '{rounded.control}'
    padding: 3px 8px
  status-review:
    backgroundColor: '{colors.success-bg}'
    textColor: '{colors.success}'
    typography: '{typography.chip}'
    rounded: '{rounded.control}'
    padding: 3px 8px
  status-neutral:
    backgroundColor: '{colors.neutral-pressed}'
    textColor: '{colors.text-control}'
    typography: '{typography.chip}'
    rounded: '{rounded.control}'
    padding: 3px 8px
  question-card:
    backgroundColor: '{colors.attention-bg}'
    textColor: '{colors.text}'
    typography: '{typography.body}'
    rounded: '{rounded.panel}'
    padding: '{spacing.16}'
  review-card:
    backgroundColor: '{colors.surface}'
    textColor: '{colors.text}'
    typography: '{typography.body}'
    rounded: '{rounded.panel}'
    padding: '{spacing.16}'
  notification:
    backgroundColor: '{colors.surface}'
    textColor: '{colors.text}'
    typography: '{typography.body}'
    rounded: '{rounded.panel}'
    padding: '{spacing.16}'
---

# Design System: mac-worker

## Overview

**Creative North Star: "Clear state, explicit actions"**

mac-worker is a personal developer tool. Its approved Paper system uses compact Inter typography, near-white canvas, white panels and graphite actions. Amber calls out a question; green marks a result ready for review. Mac illustrations, the B Slots mark and existing agent marks supply the identity.

This is the implementation target extracted from I.6 and J.1–J.8 into the six-section Paper catalog. It records the completed catalog and its documented states; production UI migration and runtime behavior remain separate work. New application screens still follow the user's comp-first workflow.

**Key Characteristics:**

- Readable state and a clear next action.
- Compact spacing, quiet panel edges and visible control boundaries.
- Established Mac imagery and agent marks, with plain English labels.

Authority: [Paper Design System](https://app.paper.design/file/01M1V8VWPMWHMNASQ70NEE0D4A/7-0), [direction](.impeccable/design-system/direction.md), [exact exports and capture inventory](.impeccable/design-system/paper-library.json). The [token stylesheet](.impeccable/design-system/tokens.css) supplies the live CSS aliases; this frontmatter owns resolved documentation primitives. The [sidecar](.impeccable/design.json) adds component previews and properties outside the frontmatter schema. All preview data is synthetic.

## Colors

Graphite and cool neutrals carry the interface; semantic accents communicate a specific state.

- **Primary:** `action` is the main local operation; `action-hover` covers hover and pressed states. Use one primary operation per local decision.
- **Semantic:** `attention` / `attention-bg` mark questions and sign-in needs; `success` / `success-bg` mark review readiness or a confirmed connection; `error` / `error-bg` identify a failed operation and recovery. `attention-signal` is a small unread/activity signal. Keep each label's subject explicit.
- **Neutral:** `canvas` sits behind `surface`; `text` carries headings and content, `text-secondary` explains, `text-control` labels controls, and `text-muted` serves placeholders and unavailable observations. `neutral-hover` and `neutral-pressed` supply quiet interaction states.

**The Control Boundary Rule.** Use the stronger control border for enabled fields, unchecked checkboxes and off-switch tracks. Keep panel dividers quiet and text-only actions unboxed.

The catalog's stronger field/selection contour is a deliberate accessibility specification: `border-control` reaches 4.97:1 against white. It supersedes the source screens' older field contour only for that role. Secondary text-labeled buttons retain `border-secondary`; their readable label identifies the action. Semantic ramp strips in the sidecar are display aids, not extra application tokens.

## Typography

Use the frontmatter's roles directly: page → question/agent → section → body → label → metadata. Menlo is for IDs, paths, commands, measured data and logs. Preserve the full value when copying an ID. The `chip` role is compact state text; `navigation` follows the shared header. The page title has the observed slight negative tracking; other roles use normal tracking.

**The Type Ownership Rule.** Use Inter explicitly for the target UI and Menlo for machine data. Do not inherit the production IBM Plex font variables.

Line icons use the existing SVG paths, rounded joins and a 1.3 px stroke: 16 px for controls and 18 px where navigation needs it. Preserve the B Slots SVG and agent marks from the exports; Claude Code retains its text fallback. An unfamiliar action needs a text label.

## Layout

Use the established spacing scale: 8 px within controls, 16 px between related items, 24 px in general panels and 40 px at desktop page edges. Compact task and notification specimens use 16 px padding. Align fixed lanes for state, agent, Mac, update time and action; let the task title absorb remaining width. Keep shared navigation, freshness and notifications in stable positions.

The library was captured at a 1440 px desktop width. No mobile composition or responsive breakpoints were approved in this extraction. Preview snippets wrap within their host without defining a new application breakpoint system.

## Elevation & Depth

**The Quiet Panels Rule.** Panels stay flat; preserve the dimensional shading and soft oval shadows of the approved Mac illustrations.

Separate surfaces with fill and thin contours. The Mac's material and shadow belong to its approved asset, not a general card box-shadow token. Do not turn the amber working shadow into a permanent panel decoration.

## Shapes

Controls, filters and panels use their corresponding frontmatter radii. Use a 1 px panel separator. Fields and selection controls use the stronger semantic contour. The existing checkbox is 16 px square with 4 px corners; the switch has a 38 × 22 px track and a 16 px round thumb. These are intrinsic selection-control shapes, not extra panel radii. Preserve the B Slots silhouette and original Mac proportions.

## Components

[Controls](.impeccable/design-system/exports/controls.jsx), [status](.impeccable/design-system/exports/status.jsx), [patterns](.impeccable/design-system/exports/patterns.jsx) and [brand/motion](.impeccable/design-system/exports/brand.jsx) are the exact static evidence. The sidecar's nine semantic HTML/CSS specimens apply the normative target; they are not application handlers.

- **Buttons:** primary is graphite, secondary is white with a quiet outline, text is transparent. Use the common 38 px geometry and control radius. Text hover has a 1 px underline with a 3 px offset; pressed text actions gain the neutral pressed fill. Loading keeps geometry and gives a specific label; disabled controls use the legible muted palette and native disabled behavior.
- **Focus and feedback:** keyboard focus is a 2 px graphite outline with a 2 px offset, including text actions. Control color/border feedback takes 150 ms without changing geometry. These are catalog state specifications, not claims about production behavior.
- **Fields and selection:** use persistent labels and 40 px fields. Retain the draft on validation or save failure, associate the specific error with its field, and offer recovery. Unavailable observations state why a value is absent; read-only values remain readable. Checkbox/switch state needs a label as well as its contour or fill.
- **Navigation and filters:** use semantic links for destinations and buttons for operations; emphasize the current route with text weight. Filters retain their selected state and counts. Text-only links do not gain field outlines at rest.
- **Task patterns:** a question shows its task title, the actual question and Answer; a review shows the result/change summary before acceptance. A reply starts a new turn. Closed/accepted does not mean merged; checks remain labeled agent-reported.
- **Worker and notification patterns:** distinguish task state, agent authentication and capacity. Installed is not authenticated; unknown capacity is not idle. Keep observation freshness beside the fact. A notification's read state is independent of task resolution.
- **Motion specification:** a working Mac's LED and warm shadow pulse together over 3 s; completion gives one green signal over 650 ms, then returns to idle. Hover lifts only the Mac illustration by 2 px. Pause loops offscreen and in hidden tabs; reduced motion keeps static state, slot labels and result text. Waiting for a reply does not keep the Mac working.

Retained source exceptions: cloned question/review cards still contain 12 px radii, 16 px titles, 40 px actions with 8 px corners and some `system-ui` declarations. These are evidence of the source clones, not new variants. Foundations/Type/Controls govern new implementation: Inter, panel radius 10 px and button geometry 38 px / 6 px. The sidecar follows those target rules.

## Do's and Don'ts

- Do compose new screens from the approved primitives and confirm the composition before implementation.
- Do pair status color with a readable label and the established line icon.
- Do preserve visible keyboard focus, readable unavailable states and the draft after a recoverable error.
- Do keep observation freshness and agent-reported check provenance visible.
- Don't use worker capacity or an installed agent mark as proof of authentication or task eligibility.
- Don't describe a reply as resuming the same turn, a closed task as merged, or reading a notification as resolving its task.
- Don't add dashboard task creation, cancellation or browser agent sign-in to these patterns.
- Don't promote retained source-clone exceptions or the production Observatory palette into new target defaults.

Not canonized or repaired: the retained clone exceptions above, the existing IBM Plex/Observatory styling in [production CSS](ui/src/index.css), and the stale I.2 design reference in [PRODUCT.md](PRODUCT.md). They are outside this catalog/documentation task; [handoff](.impeccable/design-system/handoff.md) records the remaining integration work and bounded review outcome.
