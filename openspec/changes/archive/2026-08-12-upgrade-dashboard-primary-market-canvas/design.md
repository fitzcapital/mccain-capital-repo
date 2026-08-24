## Context

The existing dashboard tape already supplies per-symbol quotes, timeframe SVGs, state, range, and freshness through a partial-refresh endpoint. The redesign can therefore be presentation-first and reuse the current lane selectors.

## Goals / Non-Goals

**Goals:**
- Establish SPX as the primary execution-context chart and VIX as confirmation.
- State what is missing before an entry is available.
- Preserve current refresh and symbol-search behavior.

**Non-Goals:**
- New indicators, strategy calculations, endpoints, or persisted preferences.

## Decisions

- Keep the existing two lane DOM contracts but assign `primary` and `confirm` layout roles. This minimizes refresh risk versus replacing chart rendering.
- Derive decision copy from existing state labels, symbol role, market state, and freshness. Copy is descriptive and never creates a new trading signal.
- Render SPY, QQQ, and IWM as compact context chips sourced from the existing tape rows. They are orientation aids, not additional full charts.
- Add four confirmation cells with stable DOM hooks so partial updates can refresh the state without rebuilding the section.

## Risks / Trade-offs

- [Descriptive copy could appear prescriptive] → Label the area “Market read” and keep existing strategy gates authoritative.
- [Existing CSS specificity could flatten the layout] → Append dashboard-scoped selectors after current tape rules.
- [Missing quotes could imply confirmation] → Render explicit unavailable/delayed states and never synthesize positive confirmation.
