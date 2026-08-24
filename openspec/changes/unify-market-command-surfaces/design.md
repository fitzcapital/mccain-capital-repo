## Context

Market Pulse and Dashboard already expose authoritative trading, Gamma, chart, account-risk, and
discipline state. Their current templates have accumulated many independently styled cards and
actions. At desktop width the primary chart or decision is pushed down by repeated summary layers,
while strong borders and glows make routine supporting data compete with critical state.

This change is presentation-only. Existing Flask viewmodels, APIs, refresh events, chart libraries,
stored settings, and financial calculations remain authoritative. Work must preserve unrelated
dirty-worktree edits and keep page-specific behavior scoped to the existing templates and static
assets.

## Goals / Non-Goals

**Goals:**

- Establish a shared three-level surface hierarchy across Market Pulse and Dashboard.
- Make decision, reason, and next action readable in one scan path.
- Place the Market Pulse execution chart in the first desktop viewport.
- Reduce simultaneously prominent actions through progressive disclosure.
- Preserve in-place refresh, chart timeframe, drawings, markers, selections, and user workflow state.
- Maintain the Galaxy identity while reducing decorative competition.

**Non-Goals:**

- Change trading permission, scenario ranking, Gamma, candle, account, or discipline logic.
- Add endpoints, data providers, dependencies, persistence, migrations, or financial assumptions.
- Replace the navigation system, chart engine, or page background.
- Redesign every application page in this change.

## Decisions

### 1. Use a three-level shared hierarchy

Level 1 is the primary decision surface; Level 2 contains supporting evidence; Level 3 contains
reference and diagnostic detail. Shared scoped CSS tokens will define surface fill, border,
elevation, spacing, and typography while page-specific selectors retain layout control.

This is preferred to a full component framework or template extraction because the existing Flask
templates and JavaScript bindings are mature and heavily page-specific. The smallest safe change is
shared visual tokens plus targeted semantic wrappers.

### 2. Consolidate Market Pulse above the chart

The current execution-read cards and session/Gamma/strategy summary become one connected command
surface. Data Lock Diagnostics becomes a compact disclosure attached to that surface. Header
utilities retain ticker, spot, session, regime, freshness, and refresh; Sticky Summary and Candle
Opens move into a compact utility group. Existing IDs and data attributes remain stable whenever
possible so canonical refresh and chart listeners continue to work.

The chart remains the existing Lightweight Charts instance. No chart state is recreated during
canonical updates or layout changes.

### 3. Make Dashboard Today decision authoritative

The Dashboard command deck will present one dominant decision, concise cause, and one context-aware
next action. State, mode, alignment, account risk, and market feed become compact evidence rather
than equal-weight command cards. Existing Command, Prepare, Execute, and Review stages remain the
workflow model; secondary controls move into stage-relevant disclosures or drawers.

### 4. Reserve semantic emphasis

Glow and saturated borders are limited to selected, live, blocked, or critical states. Routine
healthy information uses quiet surfaces. Positive/negative colors describe market or state meaning;
they do not decorate unrelated containers. Typography uses short uppercase metadata, sentence-case
explanations, and tabular numerals for prices, scores, and levels.

### 5. Verify behavior at receiving surfaces

Contract tests will protect existing IDs, payload bindings, refresh events, and progressive controls.
Rendered verification will cover 1280, 1440, and 1920 desktop widths plus compact/mobile checks.
Automated checks will measure horizontal overflow, chart initialization, control availability, and
primary-content ordering. Subjective final visual approval remains with the user.

## Risks / Trade-offs

- [Moving markup breaks JavaScript selectors] → Preserve IDs/data attributes and add binding contract
  tests before restructuring.
- [Collapsing detail hides an important blocker] → Keep exception states automatically prominent and
  collapse only routine healthy detail.
- [Chart moves but initializes with the wrong dimensions] → Retain the chart host and trigger the
  existing resize path after disclosure/layout changes.
- [Shared tokens unintentionally affect other pages] → Scope tokens and rules to Market Pulse and
  Dashboard body classes.
- [Reduced glow weakens the Galaxy identity] → Preserve the background, palette, and selected/live
  accents while simplifying ordinary surfaces.
- [Desktop improvements regress compact layouts] → Implement desktop first, then test explicit
  responsive acceptance widths before completion.

## Migration Plan

1. Add shared scoped design tokens without changing layout.
2. Restructure Market Pulse command/header hierarchy while preserving bindings.
3. Verify refresh and chart state, then refine desktop and responsive CSS.
4. Restructure Dashboard Today decision and supporting evidence.
5. Move secondary actions into existing disclosure/drawer patterns.
6. Run focused tests, syntax/lint checks, strict OpenSpec validation, rebuild the local app, and verify
   both receiving pages.

Rollback is a focused revert of template, CSS, JavaScript, and contract-test changes; no data or
schema rollback is required.

## Open Questions

- Final visual spacing, glow intensity, and exact compact-menu composition will be tuned against the
  rendered pages during implementation and left for user visual approval.
