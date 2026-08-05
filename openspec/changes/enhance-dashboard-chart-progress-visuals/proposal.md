## Why

> **Outcome (2026-07-15): Rejected in visual review and rolled back.** The shared ring treatment
> produced misleading 100% circles across unrelated metrics, and the CDH/CDL overlay compressed the
> Market Tape charts. This change must not be synced or archived as the production contract.

The Dashboard Market Tape shows useful OHLC movement but does not expose the current-day high and
low where the user is already reading price action. Progress and score visuals also use several
different treatments, making the page feel less cohesive and harder to scan than the modernized
interaction layer.

## What Changes

- Add current-day high (CDH) and current-day low (CDL) reference lines to each supported Market
  Tape chart, with clearly differentiated price tags, accessible labels, and live-refresh updates.
- Derive CDH/CDL from the same symbol-specific current-session intraday rows used by the chart;
  never infer or display a level when the source is missing or not current-session data.
- Keep CDH/CDL visible across selectable tape windows, include the levels in safe chart scaling,
  and resolve close-tag collisions without covering candles or truncating the price.
- Standardize bounded Dashboard progress visuals into a reusable ring treatment for milestone,
  alignment, and balance/goal completion while retaining linear bars for pace, threshold, and
  comparison metrics where position along a track carries meaning.
- Upgrade linear progress bars with clearer tracks, endpoints, threshold markers, value labels,
  state tones, and a consistent visual hierarchy.
- Add restrained reveal and value-transition animation for charts, rings, and fills, with no
  continuous decorative motion and a complete reduced-motion fallback.
- Preserve all current calculations, controls, routes, refresh behavior, responsive layouts, and
  textual values; this is a presentation and market-context enhancement, not a financial-rule
  change.

### Non-goals

- Do not add a third-party charting or animation dependency.
- Do not turn the compact Market Tape into a full technical-analysis chart or add drawing tools.
- Do not replace threshold or pace bars with circles when a linear scale communicates the rule
  more accurately.
- Do not change milestone, consistency, balance, discipline, or alignment calculations.

### Acceptance Criteria

- Valid current-session OHLC data produces CDH and CDL lines and complete price tags for the active
  symbol on initial render and after a live refresh; missing data produces no fabricated tags.
- CDH/CDL labels remain legible without overlapping each other, the last-price marker, or chart
  bounds at desktop, tablet, and mobile widths.
- Milestone, alignment, balance/goal, pace, consistency, and discipline progress remain numerically
  unchanged and expose equivalent text or ARIA values.
- Visual transitions run once when content becomes visible or changes, do not block interaction,
  and are disabled when `prefers-reduced-motion: reduce` is active.
- Focused service, template, JavaScript, responsive browser, and regression tests pass, followed by
  the normal container rebuild and deployed verification.

## Capabilities

### New Capabilities

- `dashboard-market-visuals`: Current-session chart reference levels and a consistent, accessible
  system for Dashboard rings, linear progress, and restrained visual transitions.

### Modified Capabilities

None. The repository currently has no living capability spec for these Dashboard visuals.

## Impact

- Market Tape payload construction in `mccain_capital/services/dashboard_service.py` and reusable
  chart payload helpers in `mccain_capital/services/market_pulse_tape.py`.
- Initial Dashboard rendering in `mccain_capital/templates/dashboard.html` and live chart updates in
  `static/js/dashboard_command_center.js`.
- Dashboard-scoped visual styles, preferably in the existing Dashboard interaction stylesheet or a
  focused companion stylesheet rather than another broad `app.css` override block.
- Existing Dashboard progress markup may gain shared semantic classes and ARIA attributes, but its
  calculations and form behavior remain unchanged.
- No database migration, external dependency, broker mutation, or financial projection change.
