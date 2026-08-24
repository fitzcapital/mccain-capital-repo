## Context

Market Pulse already polls `/api/hero/quote` every three seconds during the live session and updates the current chart candle. The strategy, setup replay, and live monitor rely on completed five-minute bars. Adding a second market-data request or promoting incomplete movement into strategy state would increase load and reduce reliability.

## Goals / Non-Goals

**Goals:**

- Show responsive SPX movement in five-second buckets on the existing hero chart.
- Make the display-only nature explicit and preserve completed five-minute candle authority.
- Handle stale timestamps, missing buckets, hidden tabs, closed sessions, and reconnects honestly.
- Keep memory and render work bounded.

**Non-Goals:**

- Tick-by-tick reconstruction, one-second candles, order-flow analysis, or broker execution.
- A new upstream subscription, endpoint, polling timer, database table, or strategy trigger.
- Filling gaps with synthetic prices.

## Decisions

1. **Reuse the existing quote poll.** Each accepted response supplies price and provider timestamp to the five-second accumulator. This adds no requests and preserves the existing single refresh coordinator. A separate browser WebSocket was rejected because it would introduce another lifecycle, authentication, and reconnect surface.
2. **Bucket by provider timestamp.** Samples replace the value in their five-second bucket; missing buckets remain missing. A gap beyond the configured threshold inserts a whitespace break and marks the tape interrupted.
3. **Render a bounded line series.** The chart retains at most the configured number of points, defaults to 72, and updates only when an accepted bucket changes. This caps memory and repaint cost.
4. **Separate observation from authority.** The UI says `5s live tape · visual only`; the tape does not feed scenario scoring, live setup state, replay, confirmation, invalidation, or candle persistence.
5. **Use lifecycle-aware states.** Hidden documents show paused, closed sessions show closed, old provider timestamps show interrupted, and missing data shows unavailable. A fresh sample resumes the tape without backfilling the gap.

## Risks / Trade-offs

- **Sparse quote changes can produce a sparse trace** → Keep gaps honest and label the tape as a sampled live view, not a complete trade record.
- **Provider timestamps may lag receipt time** → Calculate freshness from the provider timestamp and surface interruption instead of masking delay.
- **Mixing granularities can confuse execution** → Use a distinct color, a toggle, and persistent visual-only labeling; leave five-minute candles and all strategy code unchanged.
- **Long-lived tabs can accumulate data** → Enforce a small server-configured maximum point count.

## Migration Plan

Deploy the additive stream configuration, helper, template control, styles, and chart integration together. Rollback removes the line series and helper include; the underlying quote and five-minute chart behavior remains unchanged.

## Open Questions

None. Five seconds is the selected reliability/performance balance for this phase.
