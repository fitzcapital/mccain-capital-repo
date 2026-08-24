## Why

The Market Pulse hero chart already receives an in-process Tradier stream, but its visual tape still waits for a separate three-second quote poll. Connecting the observation-only chart to the existing stream will show SPX movement faster while retaining the proven polling fallback and completed five-minute strategy authority.

## What Changes

- Feed a compact SPX tape strip below the hero chart from the existing authenticated Market Pulse
  event stream, using an independent time scale.
- Coalesce incoming stream ticks into bounded five-second visual points instead of redrawing for every upstream event.
- Keep the three-second quote lane as an automatic fallback when the stream is unavailable, stale, closed, or reconnecting.
- Expose a clear tape transport state: streaming, polling fallback, interrupted, paused, or closed.
- Keep all seconds-based observations out of the five-minute candle chart's time scale.
- Preserve completed five-minute candles and canonical key levels as the only inputs allowed to confirm a setup.
- Add receiving-surface, lifecycle, reconnect, staleness, and fallback tests.
- Non-goals: no new provider subscription, no browser-side strategy decisions, no sub-five-second strategy candles, and no change to Gamma freshness or setup-ranking rules.

## Capabilities

### New Capabilities

- `market-pulse-streaming-chart`: Stream-first, bounded SPX visual tape with deterministic polling fallback and strict separation from strategy authority.

### Modified Capabilities


## Impact

- Affects the existing `/stream/market` SSE payload, Market Pulse stream client, hero chart JavaScript, chart status copy, and focused Flask/JavaScript tests.
- Reuses the current Tradier-backed market worker and authenticated same-origin stream; no dependency, database, financial assumption, or external service is added.
- Acceptance requires a stream tick to reach the independent tape without waiting for the quote
  poll, automatic fallback after a bounded stale interval, no duplicate redraw storm, no candle-axis
  expansion, and unchanged completed-bar setup behavior.
