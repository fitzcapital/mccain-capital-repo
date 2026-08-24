## Why

The Gamma Ladder exposes useful positioning data, but after hours it can label an expired 0DTE
snapshot as current, elevate a distant zero-GEX strike as the Gamma Flip, and repeat the same levels
across several competing summaries. That weakens execution clarity precisely when the page should
be separating next-session planning from live evidence.

## What Changes

- Add an explicit session-aware Gamma Ladder state that distinguishes live, after-hours last-valid,
  stale, degraded, and unavailable snapshots.
- Expose quote time, options-chain time, expiration date, and session context independently; never
  let a fresh after-hours quote make an expired options chain appear live.
- After the regular session, default to the next tradable expiration while keeping the expired
  session available as clearly labeled historical context.
- Replace the duplicated key-level cards, structure summary, and Top Levels panel with one compact
  execution map: immediate decision level, upside magnet/resistance, downside failure level,
  expected range, and next required evidence.
- Validate Gamma Flip locally around spot using a real sign transition, distance and magnitude
  constraints, and confidence; demote distant isolated zero-GEX strikes to extended structure.
- Rank focus levels by proximity, Gamma magnitude, and structural role rather than magnitude alone.
- Show a concise decision-relevant strike set by default, with the full ladder available on demand.
- Bring Gamma Ladder updates under the canonical Market Pulse synchronization boundary while
  preserving an explicit forced-refresh fallback and last-valid failure behavior.
- Preserve exact source values, ticker/window/DTE controls, row inspection, chart selection,
  accessibility, responsive behavior, and the existing no-trade-permission boundary.

### Acceptance Criteria

- During regular hours, the ladder identifies live session state and independently displays quote,
  options-chain, and expiration timestamps.
- After hours, no expired 0DTE basket is labeled Current or Live; the default basket moves to the
  next tradable expiration and prior-session 0DTE remains explicitly historical.
- A Gamma Flip is prominent only when a local positive/negative sign transition satisfies distance
  and magnitude thresholds; isolated zero-GEX rows cannot become the primary flip by themselves.
- The first ladder viewport presents one execution map with decision level, upside level, downside
  failure level, expected range, and next evidence without duplicating those values in other cards.
- The default board contains no more than nine decision-relevant strikes and provides an explicit
  control to reveal the full accepted window without changing source calculations.
- Canonical automatic and forced manual refresh update the execution map, summary timestamps, and
  ladder rows coherently without document reload; failures retain and label the last valid snapshot.
- Existing filters, row selection, chart coordination, keyboard behavior, reduced motion, and
  responsive layouts remain functional.
- Focused and full tests, lint, formatting, JavaScript syntax, strict OpenSpec validation,
  production rebuild, health check, and receiving-surface functional checks pass.

### Non-Goals

- Changing provider APIs, options/GEX formulas, order entry, trade authorization, sizing, or risk
  controls.
- Predicting price direction or treating Gamma levels as confirmation by themselves.
- Replacing the existing DOM-based ladder with Canvas, WebGL, a framework, or a new dependency.
- Removing expired-session research data or the full strike window.
- Broad Market Pulse redesign outside the Gamma Ladder and its canonical refresh integration.

## Capabilities

### New Capabilities

- `gamma-ladder-execution-truth`: Defines session-aware data truth, locally validated Gamma Flip,
  compact execution hierarchy, relevant-strike disclosure, and coherent canonical refresh behavior.

### Modified Capabilities

None. No Gamma Ladder capability currently exists under `openspec/specs/`; this contract replaces
the unsynchronized completed-change delta as the active proposal.

## Impact

- Affected code: Gamma Ladder payload assembly and expiry selection, Gamma calculations and level
  ranking, Market Pulse canonical snapshot/runtime integration, Jinja structure, ladder JavaScript,
  page-scoped CSS, and focused tests.
- Data sources: existing quote feeds, options chains, Gamma snapshot caches, expiration calendars,
  and completed session metadata. No new provider or dependency.
- API contracts: the Gamma Ladder payload gains explicit session, quote/chain timing, expiration
  lifecycle, local-flip confidence, relevance ranking, and canonical-generation metadata while
  retaining current compatibility fields.
- Financial assumptions: displayed GEX remains provider-derived and deterministic. Gamma levels are
  planning context only; execution still requires price and completed-candle confirmation from the
  authoritative Market Pulse verdict.
