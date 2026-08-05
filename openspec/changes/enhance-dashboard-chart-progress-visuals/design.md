## Context

The Dashboard Market Tape is rendered twice: Python builds the initial timeframe payload and SVG,
then `dashboard_command_center.js` rebuilds the same chart after tape refreshes or timeframe changes.
The payload currently includes windowed OHLC candles, point/percent change, and tone, but no
symbol-level current-session range. The Market Pulse chart already demonstrates the desired CDH/CDL
language, although its chart controller and larger layout are not appropriate to copy wholesale.

Dashboard progress is also split across multiple implementations: milestone and alignment already
have rings, while milestone, balance, discipline, consistency, and pace use separate linear tracks.
The calculations are sound; the opportunity is to give them a shared visual grammar and semantic
contract without moving financial logic into JavaScript.

## Goals / Non-Goals

**Goals:**

- Show source-backed CDH and CDL reference lines and full price tags on every supported Market Tape
  symbol and keep initial/server rendering identical to client refresh rendering.
- Make current-session levels visually useful without obscuring candles, the last-price marker, or
  each other.
- Establish one reusable ring component and one reusable linear-progress component for Dashboard
  metrics, with state tones and accessible numeric values.
- Add restrained one-time reveal and update transitions that remain clear with motion disabled.
- Preserve calculations, live refresh behavior, ticker/timeframe controls, and responsive behavior.

**Non-Goals:**

- No third-party chart, canvas, or animation library.
- No chart drawing tools, zoom/pan, crosshair, or Market Pulse feature duplication.
- No database change or recalculation of milestone, balance, consistency, discipline, or alignment.
- No CDH/CDL value from a prior-session or synthetic close-only fallback.

## Decisions

### 1. Produce a typed current-session level payload beside each timeframe payload

Add a focused helper in `market_pulse_tape.py` that accepts current-session intraday OHLC rows and
returns a small payload containing numeric `high`/`low`, formatted values, source status, and an
as-of timestamp. `timeframe_payloads` will accept this payload and attach the same session levels to
each selectable window. The Dashboard refresh service will derive it only from the original
`get_intraday(symbol)` result, before the chart source falls back to prior-session, cache, or
close-only data.

This separates two concepts that must not be conflated: the selected chart window controls candles
and point change, while CDH/CDL always describe the current session. It also prevents a prior-session
fallback from being mislabeled as current-day data.

Alternative considered: calculate CDH/CDL in the browser from compact candles. Rejected because the
selected window and compaction can omit the true daily extremes and create incorrect financial
context.

### 2. Keep server SVG and browser SVG generation behaviorally identical

Extend both `last_hour_svg` and `buildLastHourChart` with the same level-aware scale and overlay
rules. Valid CDH/CDL values participate in the y-domain with modest padding. Each level renders a
subtle dashed rule plus a right-edge price tag: teal for CDH and rose for CDL. The current-price dot
remains the strongest point marker.

When tags would collide, a small deterministic layout helper will separate their label positions
while retaining short connector lines to their true y-values. Labels will be clamped inside the
viewBox. The SVG will expose a concise accessible chart description outside the decorative SVG,
including CDH/CDL values when present.

Alternative considered: place the values only in the card footer. Rejected because the requested
value comes from seeing price in relation to the levels, not merely reading two numbers.

### 3. Use rings only for bounded completion KPIs

Create a shared Dashboard metric-ring structure for milestone completion, alignment score, and
balance/goal completion. Each ring receives a clamped 0–100 visual percentage through a CSS custom
property while retaining the original unmodified textual value where needed. The ring uses a
conic-gradient track, quiet inner depth, a visible endpoint, and a center label.

Linear progress remains the correct form for target-versus-actual pace, consistency thresholds,
discipline signal, and any comparison where horizontal position communicates a boundary. Those
tracks will share common fill, cap, threshold, tone, and label styles. Redundant decoration may be
removed, but no numeric value or status is removed.

Alternative considered: convert every bar to a circle. Rejected because circles make threshold and
two-series comparisons less precise and would reduce functional clarity.

### 4. Animate state changes, not the page continuously

A small Dashboard-scoped controller will use `IntersectionObserver` to add a ready class once per
visual and will re-trigger only when a live value actually changes. CSS transitions animate ring
arcs, bar fills, chart level opacity, and the final-price point over short, restrained durations.
Controls remain immediately interactive and confirmed text renders before animation.

`prefers-reduced-motion: reduce` disables interpolation and pulsing while preserving endpoints,
colors, labels, and success/stale/error states. No ambient infinite animation will be introduced.

Alternative considered: timer-based animation on every page load. Rejected because it is brittle
with lazy fragments, wastes work off-screen, and does not respect user context.

### 5. Keep the enhancement Dashboard-scoped and dependency-free

Add the new visual rules to a focused Dashboard companion stylesheet and keep controller logic in a
focused Dashboard script or a clearly separated module in the existing controller. Load both only
on the Dashboard through `base.html`. Reuse the existing mutation/rebind pattern for lazy content
and mark bound elements idempotently.

No external dependency is required. This keeps Content Security Policy, offline behavior, load
time, and rollback straightforward.

## Data Flow and Failure Behavior

1. The market-data service returns symbol-specific current-session intraday rows.
2. The tape service validates numeric OHLC rows and derives the maximum high and minimum low.
3. Initial Dashboard rows and `/api/dashboard/tape` receive identical `session_levels` fields.
4. Server and client chart renderers include valid levels in the scale and draw matching overlays.
5. A missing, stale, non-current, or close-only source yields an unavailable status and no CDH/CDL
   rule or price tag; the existing chart continues to render unchanged.
6. Live refresh replaces levels only after a successful payload. A failed refresh preserves the
   last confirmed chart and levels under the existing non-destructive feedback behavior.

Progress values remain server-owned. JavaScript reads numeric data attributes only to animate the
already-rendered endpoint and never recalculates financial meaning.

## Risks / Trade-offs

- [CDH/CDL can compress a short-window candle pattern] → Include valid levels with bounded padding,
  retain a minimum candle-domain floor, and verify 15M through 24H fixtures.
- [Early-session CDH and CDL can be nearly identical] → Use deterministic tag collision separation
  and connector lines rather than hiding a level.
- [Server and client SVGs can drift] → Share fixtures and assert equivalent level classes, values,
  scale bounds, and missing-data behavior in Python and JavaScript-focused tests.
- [Animation can make a dense Dashboard feel busy] → Animate once, keep durations short, avoid
  looping decoration, and disable all interpolation under reduced motion.
- [Broad `app.css` overrides are hard to maintain] → Use a Dashboard-only companion stylesheet and
  stable component classes.
- [A stale intraday feed could present an old range as current] → Require current-session source
  validation and omit levels when recency cannot be established.

## Migration Plan

1. Add service helpers and payload fields without removing existing keys.
2. Add server/client overlays with missing-level compatibility.
3. Migrate progress markup one metric family at a time while retaining text and calculations.
4. Add the Dashboard-scoped stylesheet/controller and reduced-motion rules.
5. Run focused service, template, JavaScript, responsive browser, and regression tests.
6. Rebuild the container, verify `/healthz`, and confirm deployed initial and refreshed charts.

Rollback removes the new optional payload fields, companion assets, and semantic classes; existing
chart and progress values remain compatible because no persisted data or route contract is removed.

## Open Questions

None blocking. CDH/CDL will mean the high and low of the validated current-session intraday source
for each selected symbol, matching the Dashboard feed rather than borrowing levels from another
symbol or a prior-session fallback.
