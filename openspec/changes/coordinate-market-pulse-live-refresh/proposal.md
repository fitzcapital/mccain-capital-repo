## Why

Market Pulse currently refreshes quotes, bars, levels, and canonical execution context through
independent browser loops. That can briefly pair newer market observations with older execution
guidance, creates unnecessary provider work, and makes Setup Replay's `S` chart marker easy to
mistake for a live strategy signal.

## What Changes

- Coordinate quote, completed-bar, levels, gamma, and canonical execution refreshes through one
  client-side scheduler driven by server-provided cadence and generation metadata.
- Make automatic browser checks read the validated server snapshot first; reserve provider-forced
  refresh for the manual recovery control and bounded server-side refresh work.
- Reevaluate execution when a meaningful dependency changes, while allowing quote-only observation
  updates without implying that the execution generation advanced.
- Add inexpensive conditional generation checks so unchanged snapshots do not repeatedly transfer
  and reconcile the complete payload.
- Preserve single-flight behavior, last-valid state, page visibility recovery, bounded retry, chart
  viewport, drawings, ticker, ladder selection, and disclosure state.
- Replace the ambiguous Setup Replay `S` with a visually distinct replay marker and an explicit
  legend/detail label that cannot be confused with a liquidity sweep or live entry.
- Add measurable timing and failure diagnostics for quote, bars, gamma/levels, canonical generation,
  provider refresh, and client reconciliation.
- Show a compact live countdown to the next scheduled canonical check, switching to the bounded retry
  countdown when freshness is delayed.
- Non-goals: changing SPX strategy rules, scoring, risk controls, data providers, charting library,
  or treating replay setups as executable live signals.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Coordinate observation polling and canonical execution
  promotion, use conditional generation checks, and define event-driven reevaluation and recovery.
- `market-pulse-scenario-ranking`: Clarify Setup Replay chart markers and preserve their historical,
  non-authoritative meaning.

## Impact

- Browser runtime: `mccain_capital/templates/core/market_pulse.html`,
  `static/js/spx_hero_chart.js`, and related Market Pulse JavaScript contracts.
- Server/runtime: Market Pulse context endpoints, cached snapshot metadata, provider refresh
  single-flight behavior, and hero-chart polling metadata.
- APIs: additive generation/version, next-check, component timing, and conditional-response metadata;
  no breaking route removal.
- Tests: focused API, service, JavaScript contract, visibility/focus recovery, replay marker, and
  deployed page verification.
- Acceptance: while the market is open, quote observation is no more than five seconds behind its
  available cached source, completed five-minute bars trigger canonical reevaluation within ten
  seconds of the close grace period, unchanged generations avoid full reconciliation, overlapping
  checks remain single-flight, failures retain and clearly label the last valid execution state, and
  replay markers never present as live entry or sweep confirmation.
