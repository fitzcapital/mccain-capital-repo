## Why

Market Pulse Core Tape currently gives every symbol equal visual weight, duplicates instruments, and leaves the
user to infer index confirmation, leadership, and laggards from a dense fixed grid. A ranked Market
Radar will surface the market structure and strongest execution candidates first.

## What Changes

- Rename and reorganize Core Tape as Market Radar.
- Separate SPX, SPY, QQQ, IWM, and VIX into a dedicated Index Pulse group.
- Deduplicate instruments before rendering and rank non-index symbols by actionable strength.
- Present leader and laggard summaries above a responsive watchlist grid.
- Redesign cards around symbol/state, price, percent move, larger selected-timeframe chart, relative
  range position, leadership rank, and freshness.
- Make direction and conviction colors consistent: teal strength, coral weakness, neutral blue-gray,
  amber caution, and gray unavailable/stale.
- Move the repeated candle legend into accessible help and remove per-card timeframe repetition.
- Preserve existing tape sources, refresh endpoints, ticker selection, timeframes, and market-state
  calculations.
- Non-goals: changing market-data providers, quote calculations, trading signals, persistence, or
  Market Pulse chart behavior.

## Capabilities

### New Capabilities

- `market-pulse-market-radar`: Defines index separation, symbol deduplication/ranking, actionable tape
  hierarchy, range-position presentation, responsive layout, and refresh compatibility.

### Modified Capabilities

None.

## Impact

- Market Pulse view-model, Jinja template, Market Pulse JavaScript/CSS, and focused tests.
- No API, database, dependency, or financial-assumption changes.
- Acceptance: each symbol appears once; Index Pulse is separate; leaders and laggards are ranked;
  cards show percent move and range position; the grid reflows without fixed empty slots; refresh and
  symbol controls continue working without a full-page reload.
