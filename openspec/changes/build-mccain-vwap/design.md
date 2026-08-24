## Context

The Market Pulse chart has a canonical VWAP series and refresh boundary, but SPX lacks native traded
volume. The current fallback calculates SPY VWAP and scales every point with one current SPX/SPY
ratio. That can distort the curve and becomes unavailable when its SPY series does not cover the
active SPX chart session. Existing adapters can provide SPX price bars and SPY one-minute OHLCV from
Tradier, Massive, streaming caches, and a bounded Yahoo fallback.

## Goals / Non-Goals

**Goals:**

- Produce an SPX-denominated cumulative VWAP curve from SPX typical price and aligned SPY volume.
- Maintain complete, truthful regular-session coverage with explicit source and alignment metadata.
- Freeze a completed session after close and update live sessions through canonical in-place refresh.
- Match the familiar yellow TradingView-style visual treatment while preserving every chart control.

**Non-Goals:**

- Claiming that SPY volume is native SPX volume or matching a proprietary TradingView value exactly.
- Overnight VWAP, futures substitution, deviation bands in v1, execution permission, or persistence.

## Decisions

### 1. Weight SPX price directly with SPY volume

For each aligned regular-session minute, calculate SPX typical price `(high + low + close) / 3`,
multiply it by SPY volume, and divide cumulative price-volume by cumulative SPY volume. This keeps
the result in SPX price units without applying one ratio to an already aggregated SPY curve.

Alternative: scale SPY VWAP using the latest SPX/SPY ratio. Rejected because a changing intraday
ratio reshapes the relationship and one terminal ratio cannot recover the SPX path.

### 2. Align on normalized minute timestamps

Normalize provider timestamps to America/New_York minute buckets and inner-join SPX and SPY rows.
Reject premarket/postmarket rows, non-positive SPY volume, invalid SPX OHLC, duplicate minutes, and
cross-session pairs. Do not forward-fill volume or fabricate missing minutes.

### 3. Use deterministic provider priority per leg

Use the existing current-session market-data adapters with Tradier first, then Massive aggregates,
then the in-process stream where it supplies the required leg, and the bounded Yahoo fallback only
when enabled. Serialize provider names independently for SPX price and SPY volume. A fallback may
replace a leg only when its accepted coverage is better and belongs to the same session.

### 4. Coverage is session-aware

During the live session, require the latest aligned minute to remain within the configured grace
window and require at least 95% alignment over the overlapping source window. After the regular
close, a series reaching the final expected completed minute becomes `complete` and remains usable;
it is not aged stale by wall-clock time alone. Partial series remain visible as a dashed chart
diagnostic with an explicit `PARTIAL` label, but remain unavailable to scenario confluence.

### 5. One canonical `McCain VWAP` contract

The payload includes method, price source, volume source, session date, aligned/expected minute
counts, coverage ratio/status, latest source time, current value, points, and generation id. Scenario
confluence uses the value only when the coverage state is `current` or `complete`.

### 6. Chart presentation remains non-destructive

Use a `#F2D94E` two-pixel solid line titled `MC VWAP` for accepted coverage and a dashed line titled
`MC VWAP · PARTIAL` when aligned points exist but coverage is incomplete. Include a current-value
badge, distance-from-spot text, and a visibility toggle stored only for the current browser session.
Refresh and toggling update the
existing series without chart recreation, `fitContent`, viewport changes, or loss of drawings,
timeframe, Strat markers, and selected Gamma level.

## Risks / Trade-offs

- [SPY volume is only a proxy for index liquidity] → Label the method on every surface and expose
  both data sources.
- [Provider legs have timestamp drift] → Normalize minute buckets, reject cross-session pairs, and
  expose alignment coverage.
- [Fallback data revises prior minutes] → Apply only complete newer canonical generations and retain
  the last valid generation on failure.
- [95% coverage hides an important missing block] → Also require a current terminal minute and report
  the largest missing gap.
- [Yellow conflicts with another overlay] → Reserve yellow for MC VWAP and keep Gamma/strategy colors
  unchanged.

## Migration Plan

1. Add aligned-series helpers and deterministic fixtures beside the existing VWAP service.
2. Add provider-leg selection and provenance without removing the compatibility fields.
3. Switch canonical Market Pulse VWAP assembly to the new method for SPX only.
4. Update the chart and decision surface, add the toggle, and preserve non-SPX native VWAP behavior.
5. Run focused tests, rebuild the local app, and verify live and completed-session receiving states.
6. Roll back by restoring the current scaled-SPY adapter; no persisted-data migration is required.

## Open Questions

None blocking. Standard-deviation bands remain a separately reviewable follow-up.
