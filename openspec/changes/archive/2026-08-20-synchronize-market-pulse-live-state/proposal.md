## Why

During an open-market audit, Market Pulse displayed current provider data but presented conflicting
execution conclusions and different SPX values across execution-facing surfaces. A trader must not
see `ACTIVE NOW` while the canonical read and ordered checklist still require waiting, nor should a
stale ladder value silently coexist with a newer decision value.

## What Changes

- Publish a stable generation identifier and component timing metadata with the canonical Market
  Pulse context.
- Derive all execution-facing status copy from one authoritative permission and confirmation state.
- Prevent actionable language unless the required ordered strategy evidence is confirmed.
- Detect execution-facing value or timestamp divergence and lock guidance while preserving the last
  valid display generation.
- Reconcile the hero, decision card, checklist, chart context, and Gamma Ladder during in-place
  refresh without resetting user-selected chart or disclosure state.
- Bound refresh feedback so an overlapping manual and automatic request cannot leave a permanent
  "refresh running" message, and retry automatically after the active single-flight completes.
- Make Setup Replay resolve to ranked results, an explicit empty state, or a retryable error instead
  of remaining indefinitely in a loading state.
- Present canonical and ladder regime labels with explicit scope when their source calculations
  differ, while keeping the canonical execution regime authoritative.
- Add regression coverage for contradictory decisions, mixed-generation updates, stale components,
  and successful coherent refreshes.
- Non-goals: changing strategy rules, adding symbols beyond SPX, redesigning the page, or changing
  market-data providers.

## Capabilities

### New Capabilities
- `market-pulse-live-state-coherence`: Canonical generation identity, authoritative execution state,
  component coherence checks, and safe in-place refresh behavior.

### Modified Capabilities
- `market-pulse-scenario-ranking`: Require ranked scenario presentation and checklist language to
  obey the same canonical permission and confirmation state.

## Impact

- Affects the Market Pulse context assembly and API, runtime refresh orchestration, Market Pulse
  template/client reconciliation, and focused Python/JavaScript contract tests.
- Uses existing quote, completed-bar, gamma, level, and scenario inputs; no provider, dependency,
  database, or financial-rule changes are introduced.
- Acceptance requires one generation across execution surfaces, no actionable copy before required
  confirmation, an explicit lock on mixed/stale data, preserved last-valid state on failure, and a
  successful in-place refresh that retains user interaction state. Refresh and replay loading states
  must also terminate honestly, and regime labels must not appear contradictory.
