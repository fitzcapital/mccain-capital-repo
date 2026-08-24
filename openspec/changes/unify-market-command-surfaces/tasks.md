## 1. Shared Visual Hierarchy

- [x] 1.1 Add Market Pulse and Dashboard scoped tokens for three surface levels, spacing,
  typography, semantic borders, and restrained glow
- [x] 1.2 Add contract coverage for page-scoped tokens, primary-surface ordering, and preserved
  authoritative IDs and data attributes
- [x] 1.3 Verify the token layer does not alter unrelated application pages

## 2. Market Pulse Command Surface

- [x] 2.1 Simplify the Market Pulse header around ticker, spot, session, regime, freshness, and refresh
- [x] 2.2 Move Sticky Summary and Candle Opens into a compact accessible utility group
- [x] 2.3 Consolidate execution read, active level, next evidence, invalidation, session, Gamma health,
  and strategy state into one connected command surface
- [x] 2.4 Convert Data Lock Diagnostics into an exception-aware disclosure attached to the command
  surface
- [x] 2.5 Move the existing execution chart into the first desktop viewport without replacing its
  host or chart instance
- [x] 2.6 Preserve canonical in-place refresh, Gamma symbol formatting, timeframe, drawings, markers,
  selected levels, and chart resize behavior through the new hierarchy
- [x] 2.7 Reduce repeated Market Pulse lower-page copy while keeping Chart, Gamma Ladder, Scenarios,
  Diagnostics, and Radar responsibilities distinct

## 3. Dashboard Today Decision

- [x] 3.1 Build one dominant Today decision surface containing permission, authoritative blocker or
  reason, and one context-aware next action
- [x] 3.2 Recast state, mode, alignment, account risk, and market-feed state as compact supporting
  evidence rather than competing command cards
- [x] 3.3 Consolidate the Ops Band and session controls so healthy detail stays collapsed and exceptions
  remain immediately visible
- [x] 3.4 Keep Command, Prepare, Execute, and Review as the canonical stage language and expose only
  stage-relevant primary actions
- [x] 3.5 Place broker metrics, history, diagnostics, calendar, health, and extended review tools behind
  existing accessible drawers or disclosures
- [x] 3.6 Position the SPX/VIX market canvas beneath the Today decision while preserving SPX dominance,
  VIX confirmation, comparison tiles, and in-place tape refresh

## 4. Responsive and Interaction Refinement

- [x] 4.1 Tune Market Pulse and Dashboard at 1280, 1440, and 1920 CSS-pixel desktop widths
- [x] 4.2 Implement compact and mobile reflow in decision, reason, action order with no horizontal page
  overflow
- [x] 4.3 Ensure interactive controls retain visible focus, keyboard operation, accurate ARIA state, and
  minimum 44 CSS-pixel targets at compact widths
- [x] 4.4 Verify disclosures, drawers, sticky behavior, refresh actions, and chart resizing without full
  page reloads

## 5. Verification and Delivery

- [x] 5.1 Run focused Market Pulse and Dashboard route, interaction, refresh, chart, and presentation
  contract tests
- [x] 5.2 Run Ruff, JavaScript syntax checks, strict OpenSpec validation, and `git diff --check`
- [x] 5.3 Rebuild the local Podman app and verify `/healthz`
- [x] 5.4 Verify both receiving pages at desktop and compact widths for hierarchy, overflow, chart
  initialization, preserved authoritative values, and console errors
- [x] 5.5 Leave final subjective visual inspection and approval to the user
