## Context

The current Gamma Ladder is a Jinja shell populated by `static/js/gamma_ladder.js` from
`/api/gamma-ladder`. It already supports accepted-response ordering, symbol/window/DTE controls,
row inspection, chart-selection events, responsive layouts, and restrained motion. The service
builds a ladder from quote and options-chain inputs, but `updated_at` is quote-derived, browser
freshness uses a fixed five-minute wall-clock test, after-hours defaults remain 0DTE, and the global
flip algorithm promotes exact zero-GEX strikes before examining sign transitions. The receiving
surface then repeats levels across seven key cards, a structure summary, Top Levels, and the board.

The existing `tighten-market-pulse-execution` change established one canonical generation,
session-aware component freshness, in-place updates, and a shared forced-refresh gate. This design
extends that boundary to Gamma Ladder truth rather than creating another independent refresh model.

## Goals / Non-Goals

**Goals:**

- Make session, expiration, quote, and chain timing independently truthful.
- Select the appropriate live or next-session basket without hiding prior-session research.
- Replace weak global-zero flip promotion with a local, confidence-qualified sign transition.
- Reduce duplicated summaries to one execution map and a focused strike board.
- Synchronize Gamma with the existing canonical Market Pulse generation and preserve last-valid state.

**Non-Goals:**

- Change provider acquisition, Black-Scholes/GEX formulas, or raw row values.
- Add predictions, trade permission, orders, sizing, or risk systems.
- Replace the DOM controller, animation approach, or application design system.
- Remove full-depth research, existing filters, or chart coordination.

## Decisions

### 1. Separate session truth from timestamp freshness

Add a server-owned `session_context` block with market phase, session date, next session date,
expiration lifecycle, and display mode. Add independent `quote_as_of`, `chain_as_of`, and
`computed_at` fields plus ages/statuses. The aggregate state is the weakest required input under
session-aware thresholds. Browser code renders these values but never calculates whether financial
data is live.

Alternative: keep the five-minute JavaScript stale check and change its label after hours. Rejected
because a fresh quote can still mask an expired chain and client clocks cannot determine exchange
session validity reliably.

### 2. Resolve default expiration at the service boundary

During the supported live options session, preserve 0DTE as the default when available. After
session close, resolve the next listed tradable expiration as the default planning basket. Preserve
the expired date in the available-expiration model with lifecycle `prior_session`; selecting it is
explicit and never changes its truth label.

Alternative: remove 0DTE after close. Rejected because prior-session positioning remains useful for
review and next-session context.

### 3. Promote only a qualified local sign transition as Gamma Flip

Use the existing local-window machinery as the base, but require adjacent net-GEX values with
opposite signs, minimum absolute magnitude relative to the local distribution, a configurable
distance cap, and deterministic interpolation. Exact or near-zero rows are candidates only when
they sit inside a qualifying transition. Return confidence, bracketing strikes/GEX, distance, and
rejection reason. Keep the existing global zero/crossing result as secondary diagnostics only.

Alternative: choose the closest zero or sign crossing without magnitude qualification. Rejected
because noise and distant zero rows receive disproportionate authority.

### 4. Build one server-backed execution-map view model

Create a deterministic Gamma execution-map model from the accepted snapshot: session state,
regime, decision level, upside level, downside failure level, expected range, and next evidence.
Rank candidates with a documented composite of proximity, normalized absolute GEX, and structural
role. The server owns semantic selection; JavaScript owns rendering and interaction only.

The map uses planning language and never sets the Market Pulse execution permission. Positive Gamma
may describe a bounded stabilization map; negative Gamma describes acceleration boundaries.

Alternative: continue deriving the map entirely in JavaScript. Rejected because initial render,
canonical API state, tests, and chart consumers could disagree.

### 5. Use progressive disclosure for depth

Return the complete accepted row set plus stable `relevance_score` and `is_decision_relevant`
metadata. The default board renders at most nine relevant rows in original strike order. `Show full
ladder` reveals the remaining accepted rows locally and reports the count; it does not refetch or
recalculate. Filter changes reset the disclosure and incompatible selected strikes.

Alternative: reduce the API window to nine rows. Rejected because it removes research depth and
makes the disclosure require a second financial request.

### 6. Join the existing canonical refresh transaction

Include compact Gamma Ladder metadata, execution map, relevant rows, and a stable Gamma generation
inside the Market Pulse canonical payload. The current coordinator applies it through the existing
validated in-place boundary. The ladder's visible refresh action delegates to the shared forced
refresh gate rather than starting an independent request. Automatic checks remain cached-only.

Alternative: leave the ladder on its separate controller timer. Rejected because page verdict,
Gamma map, and freshness can display different generations.

### 7. Collapse repeated first-tier summaries

Replace the seven key-level cards plus duplicate structure and Top Levels summaries with one compact
execution map. Keep precise timing/source detail in a collapsed disclosure and exact row values in
the board. Distant flip and low-relevance structure move into Extended structure. The selected-row
inspector remains authoritative for an actively inspected strike.

## Data Flow

1. Existing quote and options providers populate their caches with independent timestamps.
2. Gamma service resolves session/expiration lifecycle and computes unchanged raw GEX rows.
3. Local-flip validation and relevance ranking add deterministic metadata.
4. The execution-map builder selects planning levels from that accepted generation.
5. Canonical Market Pulse assembly fingerprints Gamma truth, map, and relevant rows.
6. The shared coordinator applies a newer generation atomically to the verdict-adjacent map,
   ladder board, diagnostics, and chart-selection context.
7. Full-depth disclosure operates locally over the already accepted row set.

## Risks / Trade-offs

- [Exchange-session boundary or holiday error] → Reuse the repository's market-session calendar,
  test early closes/weekends, and degrade when the next session cannot be resolved.
- [Magnitude threshold hides a useful flip] → Return the rejected candidates and reason in
  diagnostics; keep thresholds configurable and test representative distributions.
- [Composite ranking appears arbitrary] → Publish score inputs, deterministic weights, and role
  reasons in the payload and selected-row detail.
- [Canonical payload becomes too large] → Include compact relevant rows in the canonical boundary
  and retain full rows in the existing ladder payload/cache keyed to the same Gamma generation.
- [Shared forced refresh increases latency] → Preserve concurrency gating, provider timeouts, and
  last-valid atomic commit; update nothing when the full required response is invalid.
- [Removing summaries hides detail] → Preserve exact values in rows, the inspector, and a collapsed
  data-lineage/extended-structure disclosure.
- [Financial overstatement] → Keep planning-only language, explicit timestamps, confidence, and
  the existing completed-candle execution gate.
- [Security/privacy] → No new external destinations, credentials, persistence, or user data.

## Migration Plan

1. Add additive session/timestamp/expiration, local-flip, ranking, map, and generation fields behind
   compatibility tests while retaining current fields.
2. Add domain/service tests for live, after-hours, expired, holiday, missing, and flip boundaries.
3. Switch the ladder renderer to the server-backed execution map and progressive row disclosure.
4. Join the canonical in-place coordinator and delegate forced refresh through its concurrency gate.
5. Remove only the duplicate presentation surfaces after receiving-state parity is proven.
6. Rebuild and verify live/after-hours fixtures, authenticated receiving behavior, responsiveness,
   selection/chart coordination, and console health.

Rollback restores the prior presentation/controller and ignores additive payload fields. No database
or provider migration is required.

## Open Questions

No blocking product questions. Implementation shall derive initial local-window and magnitude
thresholds from existing Gamma service configuration and lock them with fixtures before enabling
the new primary-flip label.
