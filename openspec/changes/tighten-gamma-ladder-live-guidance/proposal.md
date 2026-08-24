## Why

The live Gamma Ladder is data-rich but does not clearly identify the level being tested now, the confirmation path, or the relationship between failure and the next magnet. Its freshness lineage and SPX-only execution scope are also too easy to miss during a live session.

## What Changes

- Add a compact `Now → Confirm → Target → Fail` command rail driven by the accepted ladder snapshot.
- Make the nearest meaningful boundary the default selected level and rank the three highest-priority levels without disturbing strike-order depth visualization.
- Show compact quote age, chain age, and canonical-generation alignment in the ladder header; block execution guidance when lineage is mixed or stale.
- Clarify overlapping level roles with directional language such as “loss of decision level exposes downside magnet.”
- Mark SPY, QQQ, and searched non-SPX symbols as reference-only because execution guidance is proven only for SPX.
- Replace ambiguous hidden-row and crossing copy with exact nearby/total counts, direction, timestamp, and confirmation state.
- Correct the disabled prior-expiration explanation and keep full lineage available in the existing disclosure.
- Preserve the existing ladder API, chart, drawings, scenario ranking, and manual refresh fallback.

### Non-goals

- Do not introduce trade execution, order routing, new market-data providers, or support claims for non-SPX strategies.
- Do not change Gamma calculations, financial assumptions, strike acceptance, or canonical Market Pulse refresh cadence.
- Do not replace the price-ordered depth map with score ordering.

## Capabilities

### New Capabilities

- `gamma-ladder-live-guidance`: Covers immediate-level guidance, priority ranking, lineage visibility, SPX execution scope, and concise ladder language.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Requires Gamma guidance to expose and honor canonical-generation alignment before presenting live execution instructions.

## Impact

- Affects the Market Pulse template, Gamma Ladder presentation JavaScript, ladder styling, and focused Flask/JavaScript tests.
- Uses existing quote timestamps, chain timestamps, Gamma generation identifiers, canonical generation identifiers, accepted ladder rows, and role metadata.
- Acceptance requires an SPX ladder to show actionable guidance only from aligned current data, non-SPX ladders to show reference-only context, priority levels to be deterministic, and focused tests plus deployed local verification to pass.
