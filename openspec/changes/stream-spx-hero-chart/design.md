## Context

Market Pulse already opens `/stream/market`, which emits the in-process `market_worker` snapshot approximately every 250 milliseconds and reconnects after a short bounded session. The hero chart separately polls `/api/hero/quote` every three seconds. A prior attempt placed five-second observations on the candle chart's shared time scale and distorted the five-minute viewport. The stream is still the lowest-latency existing source, but it requires an independent compact tape.

The chart must remain useful when SSE is unavailable and must never turn observation ticks into strategy evidence. Exact SPX setups continue to depend on completed five-minute candles and canonical levels.

## Goals / Non-Goals

**Goals:**

- Deliver valid SPX stream ticks to an independent compact tape without waiting for the quote poll.
- Coalesce redraws to the existing five-second visual cadence and bounded point count.
- Detect stale stream delivery and fall back automatically to the existing quote lane.
- Recover on visibility changes and EventSource reconnects without duplicate listeners or requests.
- Make the active transport and freshness visible without adding noisy UI or changing candle spacing.

**Non-Goals:**

- Do not create another provider connection or endpoint.
- Do not use stream ticks to confirm setups, candle patterns, Gamma state, or key-level acceptance.
- Do not remove the quote, bars, or levels polling lanes.
- Do not persist visual tape ticks.

## Decisions

1. **Reuse the existing browser stream event.** `market_pulse_gamma_context.js` already owns the single authenticated EventSource and dispatches `market-pulse-stream-payload`. The hero chart will subscribe to that event rather than open a second SSE connection. This avoids duplicate server threads and provider pressure.
2. **Use a separate compact chart and time scale.** The tape owns its own small canvas beneath the
   candle chart. Stream payloads may arrive four times per second, but only the newest valid SPX tick
   in each five-second bucket is rendered. Seconds-based timestamps never enter the candle chart.
3. **Use stream-first with polling fallback.** A recently received, provider-timestamped SPX stream tick marks the tape as streaming. When no valid tick arrives within a bounded stale window, the existing three-second quote lane resumes visual-tape feeding and the status changes to polling fallback. Polling continues to update header spot regardless, preserving recovery.
4. **Validate before accepting.** The client will reject missing/non-positive prices, regressions in provider timestamp, closed-market ticks, and stale observations. Gaps remain gaps; the chart will not synthesize movement.
5. **Preserve authority boundaries.** Only `strategy_bars_5m` and canonical level generations can affect setup state. Stream data changes the observation line and displayed spot only.

## Risks / Trade-offs

- [SSE delivery can occupy local Gunicorn threads] → Reuse the existing single EventSource and retain the current aggressive server rotation/reconnect design.
- [High tick rate can cause redraw pressure] → Coalesce by five-second bucket and ignore unchanged timestamps/prices.
- [A shared time scale distorts the five-minute chart] → Render the fast tape in an independent
  compact canvas and keep the main chart seconds visibility disabled.
- [Stream may repeat a cached snapshot] → Evaluate provider timestamps, not browser receipt time, and enter polling fallback after the stale window.
- [Two sources can race] → Prefer fresh stream observations for the tape while allowing the quote lane to take over only after stream staleness.
- [Visual speed could be mistaken for setup confirmation] → Keep the “visual only” label and never route ticks into pattern or scenario services.

## Migration Plan

Deploy the additive independent tape, listener, lifecycle state, and tests with the existing SSE
endpoint unchanged. Verify one EventSource per page, stream-to-tape delivery, bounded redraws,
polling fallback, and an unchanged candle viewport. Rollback removes the tape strip without server
or data migration.

## Open Questions

None. Existing local stream, polling fallback, and five-minute authority boundaries provide the required implementation path.
