## Context

The Market Pulse view-model already provides symbol, direction, tape state, timeframe OHLC SVG,
price, percent move, freshness, and range text. The current template renders a fixed equal-card grid,
which obscures hierarchy and can duplicate a selected symbol.

## Goals / Non-Goals

**Goals:**

- Convert existing tape data into a scan-first Market Radar.
- Keep indices and volatility distinct from individual stock leadership.
- Rank and deduplicate deterministically on the server.
- Preserve lazy refresh and symbol-control hooks.

**Non-Goals:**

- Change quotes, candle construction, tape-state calculations, or data sources.
- Add trading recommendations or persistence.
- Replace the existing refresh endpoint.

## Decisions

1. **Rank in the server view-model.** Add deduplication, instrument group, numeric percent move,
   range position, conviction, and rank fields before rendering. This keeps initial and refreshed
   fragments consistent. CSS ordering was rejected because it cannot reliably deduplicate or expose
   semantic ranks.
2. **Use two semantic groups.** `SPX/SPY/QQQ/IWM/VIX` render as Index Pulse; remaining instruments
   render as ranked watchlist cards. A single mixed grid was rejected because it conflates market
   confirmation with stock opportunity.
3. **Rank by conviction then magnitude.** Strong/weak directional states rank ahead of mixed states;
   magnitude breaks ties, with symbol as the stable final key. Leaders and laggards are projections
   of the same deduplicated watchlist rather than separately fetched data.
4. **Compute range position only from valid values.** Use `(price-low)/(high-low)` clamped to 0–100;
   unavailable or zero-width ranges display unavailable instead of fabricated position.
5. **Preserve DOM refresh hooks.** Existing `data-watch-symbol`, timeframe payload, chart roles, and
   symbol controls stay on cards. New group containers provide stable targets for future updates.

## Risks / Trade-offs

- [Rank changes may move cards during refresh] -> Use deterministic stable sorting and visible ranks.
- [Some symbols lack numeric range inputs] -> Show unavailable range position without blocking cards.
- [Existing JS assumes a fixed card order] -> Audit selectors and retain data attributes rather than
  positional assumptions.
- [Dense mobile layout] -> Use auto-fit desktop grid and a single-column mobile layout.

## Migration Plan

Deploy as view-model, template, CSS, JavaScript compatibility, and test changes. No data migration is
required. Roll back those files if receiving-page verification fails.

## Open Questions

None.
