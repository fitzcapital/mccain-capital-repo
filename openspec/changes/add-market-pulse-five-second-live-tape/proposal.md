## Why

The Market Pulse chart needs faster price awareness without weakening the strategy contract that completed five-minute candles provide. Reusing the existing timestamped quote feed for a bounded five-second visual tape gives the user responsive context without adding another polling path or treating incomplete movement as confirmation.

## What Changes

- Add a five-second SPX live-price trace to the Market Pulse hero chart using the existing quote polling response.
- Label the trace as visual-only and keep completed five-minute candles authoritative for setups, confirmations, replay, and invalidation.
- Mark the trace interrupted, paused, closed, or unavailable when timestamps are stale, the page is hidden, the session is closed, or a feed gap occurs.
- Bound retained five-second samples and never interpolate missing ticks.
- Add a compact chart toggle and status label without increasing page-refresh frequency or adding a new network request.
- Non-goals: tick-by-tick order flow, broker execution, one-second strategy signals, synthetic ticks, or changing the existing five-minute strategy logic.

## Capabilities

### New Capabilities
- `market-pulse-five-second-live-tape`: A bounded, non-authoritative five-second SPX price trace with explicit freshness and interruption states.

### Modified Capabilities
- `market-pulse-live-state-coherence`: Clarify that completed five-minute candles remain authoritative while the five-second tape is visual context only.

## Impact

- Affects the Market Pulse template, hero chart JavaScript, chart styles, stream-session configuration, and focused contract tests.
- Reuses the existing `/api/hero/quote` polling path and its provider timestamp; no additional market-data API or dependency is introduced.
- Acceptance requires a visible five-second trace during a fresh live session, explicit non-confirmation labeling, bounded memory, gap-safe rendering, hidden/closed-session pausing, and unchanged five-minute decision authority.
