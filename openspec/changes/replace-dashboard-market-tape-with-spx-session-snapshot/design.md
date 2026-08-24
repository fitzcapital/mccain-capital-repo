## Context

The Dashboard currently presents five symbols as an alignment matrix and renders SPX and VIX in
execution-style chart lanes. That visual language overstates the role of non-SPX symbols and
duplicates analysis that belongs on Market Pulse. Existing dashboard tape data and refresh APIs
already provide the prices, changes, intraday ranges, timestamps, and partial-hydration path needed
for a lighter SPX session summary.

## Goals / Non-Goals

**Goals:**

- Make SPX the only primary market subject on the Dashboard.
- Explain SPX location using actual session prices and a range-position visualization.
- Distinguish descriptive cross-market context from strategy confirmation.
- Preserve no-reload refresh behavior and explicit stale/missing fallbacks.
- Keep the surface compact and responsive with one clear route to Market Pulse.

**Non-Goals:**

- Do not add or modify trading signals, permissions, setup scoring, or strategy rules.
- Do not claim that SPY, QQQ, IWM, or VIX validates an SPX trade.
- Do not duplicate Market Pulse levels, Gamma Ladder, scenario ranking, or replay.
- Do not change market-data providers, persistence, or endpoints.

## Decisions

1. **Reuse the existing tape refresh payload.** The Dashboard controller will derive the snapshot
   from the SPX row and render comparison symbols only inside a context strip. This avoids a new
   endpoint and keeps current freshness semantics intact. A new endpoint was rejected because the
   source data and refresh cadence are already shared.
2. **Use session-range location instead of a sentiment rail.** The principal visualization will
   label low, spot, and high with real prices; open and midpoint remain secondary reference marks.
   This answers where SPX is without implying a trading edge.
3. **Use descriptive session-character rules.** Trend and volatility labels are derived from
   available change and range-position inputs and are explicitly labeled as orientation. Missing
   inputs render unavailable states rather than inferred signals.
4. **Keep non-SPX symbols compact and subordinate.** SPY, QQQ, IWM, and VIX show quote and change
   only, under copy stating that they are context rather than validation.
5. **Retain progressive detail.** The primary snapshot is visible; extended per-symbol context is
   compact and the sole execution CTA opens Market Pulse.
6. **Use a compact candle view for session shape.** The center of the snapshot renders the most
   recent one-hour SPX payload as a small 5-minute candlestick chart. It reuses the existing
   Dashboard chart builder and refresh response, stays under roughly 180 pixels tall, and carries
   no levels, setup markers, Gamma overlays, or execution language. Open, range, and range-position
   remain as three supporting statistics beneath the chart.
7. **Cap the wide-desktop reading canvas.** Dashboard content uses a centered, narrower desktop
   frame instead of expanding every card across the available window. The cap applies to the full
   Dashboard workflow, performance hub, snapshot, and calendar while smaller desktop and mobile
   widths continue using the available space. This reduces horizontal eye travel without hiding or
   reorganizing the business metrics.

## Risks / Trade-offs

- [Risk] Intraday high/low may be unavailable or stale. → Show freshness and an explicit
  unavailable range instead of placing a synthetic marker.
- [Risk] Session-character wording could be mistaken for a signal. → Use descriptive labels,
  avoid action verbs, and include the non-validation note adjacent to context.
- [Risk] Existing tests and selectors expect the matrix and dual lanes. → Replace those contracts
  with snapshot-specific selectors while keeping the existing refresh button and endpoint.
- [Risk] Cached assets may make the deployed page appear unchanged. → Rebuild the local container,
  verify asset content, and inspect the receiving Dashboard after deployment.
