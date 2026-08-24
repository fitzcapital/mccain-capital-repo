## Context

The canonical payload currently contains a valid `SPY VWAP Proxy`, but the inspected live payload
ended at 09:59 ET while the chart contained bars through 14:00 ET. Lightweight Charts correctly
plotted those old points; they simply fell outside the visible time window. The Gamma Ladder already
has execution-map semantics, relevant-row ranking, full-depth disclosure, and chart selection, but
its presentation weights most rows similarly and does not make the spot-centered path immediately
legible.

## Goals / Non-Goals

**Goals:**

- Make VWAP presence truthful by validating source coverage against the current visible session.
- Preserve the canonical in-place refresh boundary and all chart interaction state.
- Turn the ladder into a centered price spine that answers: where is spot, what is above, what is
  below, which boundary matters next, and how strong is positioning?
- Preserve exact signed GEX, distance, role, state, controls, selection, and full-depth research.

**Non-Goals:**

- Changing GEX/VWAP formulas, manufacturing missing bars, adding providers, or authorizing trades.
- Changing Strat numbers, market-state decisions, persisted data, or broker behavior.

## Decisions

### 1. Validate VWAP coverage before declaring it chart-visible

The backend will compare the latest same-session VWAP point with the latest compatible chart bar and
serialize coverage state, latest source time, latest chart time, and age. A source that cannot cover
the active visible window will be `partial` or `unavailable`, not simply `available`.

Alternative: extend the 09:59 value horizontally to 14:00. Rejected because that visually claims a
continuously calculated VWAP without the intervening volume.

### 2. Refresh the proxy from the same session contract

For SPX, the service will request current-session SPY OHLCV through the existing market-data service
and accept it only when its timestamps overlap the selected chart session. Last-valid data may be
shown in the decision surface with its time, but it will not masquerade as a current chart overlay.

Alternative: use the quote's cached short series regardless of coverage. Rejected because its
limited window caused the present off-chart result.

### 3. Let the chart own presentation, not financial inference

The chart controller will plot only canonical accepted points, keep a distinct purple line and
current-value badge, and reconcile it without `fitContent`, series recreation, or timeframe reset.
No client interpolation or proxy calculation is allowed.

### 4. Use spot as the ladder's fixed visual anchor

The default ladder will have three stable zones: resistance above, a highlighted spot rail, and
support/acceleration below. Each strike row remains in price order. Distance controls vertical
proximity text, signed GEX controls a diverging bar from a zero axis, and role/state control badges
and emphasis. The strongest or nearest decision row receives emphasis, not every large value.

Alternative: sort rows by absolute GEX. Rejected because it destroys the physical price path needed
for execution.

### 5. Progressive disclosure stays local

The default view shows the nearest decision-relevant rows plus the spot rail. Full ladder, settings,
selection inspector, and chart coordination remain available. Desktop uses a wider diverging depth
track; mobile collapses secondary prose while preserving strike, distance, role, signed GEX, and
state.

### 6. Use semantic color with redundant cues

Positive GEX uses theme teal, negative GEX uses theme coral, spot uses cyan, and the dominant node
uses restrained gold. Direction, labels, bar direction, border treatment, and icons duplicate the
meaning so color is never the only signal.

## Risks / Trade-offs

- [SPY feed remains incomplete] → Mark VWAP partial/unavailable with source time; do not draw it.
- [Price-spine density becomes noisy] → Keep nine relevant rows by default and move explanations to
  selection/expanded detail.
- [Large GEX outlier flattens other bars] → Use deterministic robust normalization while always
  displaying the exact signed value.
- [Canonical refresh mixes chart and ladder generations] → Validate both before one receiving commit
  and retain the last valid generation on failure.
- [Financial interpretation is overstated] → Keep planning/confirmation language and never map color
  or GEX magnitude to permission.

## Migration Plan

1. Add coverage metadata and tests to the VWAP contract without removing existing fields.
2. Update canonical assembly and chart reconciliation, then verify current-session overlap.
3. Add the price-spine DOM/CSS behind the existing ladder payload and controller boundary.
4. Verify selection, filters, full-depth disclosure, responsive layout, accessibility, and refresh.
5. Rebuild the local app and inspect the receiving page. Rollback restores the prior ladder renderer
   and ignores additive VWAP coverage fields; no data rollback is required.

## Open Questions

None blocking. Initial default disclosure remains nine decision-relevant rows, matching the current
server contract.
