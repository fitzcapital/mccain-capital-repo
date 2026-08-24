## Why

The Dashboard Market Tape currently gives SPY, QQQ, IWM, and VIX the appearance of being
strategy confirmations even though the trading system is proven only for SPX. The Dashboard
needs a compact orientation surface that explains the current SPX session and routes detailed
execution work to Market Pulse.

## What Changes

- Replace the five-symbol confirmation matrix and dual execution-style chart lanes with one
  compact SPX Session Snapshot.
- Show SPX spot, session change, open, high, low, total range, and position within the session
  range using actual price labels.
- Classify session character as trending up, trending down, balanced, or volatile using descriptive
  market data only; do not generate trade permission or setup confirmation.
- Keep SPY, QQQ, IWM, and VIX in a clearly labeled secondary-context row that cannot be mistaken
  for SPX strategy validation.
- Add concise "what changed" language and retain one route to Market Pulse for execution analysis.
- Preserve partial refresh, freshness, fallback, and responsive behavior.
- Replace the oversized range rail with a compact one-hour SPX mini-chart built from the existing
  Dashboard candle payload, while retaining actual-price range context as concise supporting data.
- **Non-goals:** no new SPX strategy rules, no setup scoring, no historical setup replay, no trade
  signals from non-SPX symbols, and no Market Pulse logic duplicated on the Dashboard.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `dashboard-primary-market-canvas`: Replace the multi-symbol confirmation canvas with an SPX-only
  session-orientation snapshot and explicitly subordinate all non-SPX data to descriptive context.

## Impact

- Dashboard Jinja markup and presentation styles.
- Existing Dashboard mini-chart renderer reused for the SPX snapshot; no new chart dependency.
- Dashboard tape hydration and partial-refresh behavior in the existing JavaScript controller.
- Dashboard contract and runtime tests.
- Existing market-data endpoints remain authoritative; no financial assumptions, persistence,
  dependencies, or Market Pulse execution rules change.
