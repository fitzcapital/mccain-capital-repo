## Why

Market Pulse currently can poll successfully while still presenting stale or cross-generation
spot, candle, gamma, and ladder data. Because the page guides live SPX execution, reliability
must be proven continuously and unsafe states must fail closed before further ladder upgrades.

## What Changes

- Introduce one authoritative refresh transaction that coordinates spot, completed candles,
  gamma, ladder levels, scenarios, and setup replay under a shared generation identifier.
- Validate source timestamps, trading-session alignment, symbol scope, and cross-component
  coherence before publishing a generation to the page.
- Add bounded retries with backoff, request timeouts, overlap prevention, and recovery after
  browser suspension, worker changes, or partial provider failure.
- Preserve the last verified generation while marking it stale; never combine fresh values with
  stale decision guidance.
- Make the refresh countdown reflect the actual next attempt and expose refreshing, retrying,
  delayed, stale, and locked states without forcing a full-page reload.
- Gate live execution on freshness and coherence, with component-specific reasons and timestamps.
- Add deterministic telemetry and focused reliability tests for polling cadence, generation
  ordering, multi-worker cache behavior, and partial-failure recovery.
- Keep late-session execution viability and other ladder enhancements outside this reliability
  phase; those will be layered on only after the data contract is stable.

## Capabilities

### New Capabilities

- `market-pulse-refresh-resilience`: Defines reliable polling, atomic generation publication,
  retry/recovery behavior, observability, and fail-closed execution gating.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Requires every visible execution component to come from
  the same verified generation and forbids mixed-generation UI updates.

## Impact

- Affects the Market Pulse context endpoint, freshness and market-data services, shared snapshot
  cache, refresh coordinator, Gamma Ladder binding, execution state, and page status messaging.
- Uses existing SPX market-data and gamma sources; no new provider, database, trading action, or
  financial assumption is introduced.
- Acceptance requires deterministic tests plus deployed verification that repeated polls advance
  coherently, stale data locks execution, transient failures recover automatically, and no poll
  requires a full-page reload.
- Non-goals: changing strategy rules, adding non-SPX support, scoring late setups, redesigning the
  ladder, or weakening existing freshness thresholds.
