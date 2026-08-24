## Context

Market Pulse already runs independent workers for quotes, options, and gamma, and exposes a
canonical context API plus an EventSource quote stream. Quote and options workers update more often
than the browser's server-rendered execution state; gamma uses session-aware polling. Manual
refresh currently forces gamma work and reloads the page coherently, but passive pages do not
reliably advance to newer canonical generations.

The failed-sweep domain evaluator is deterministic, but its adapter derives location while sweep,
close-back-inside, 2-2, trigger, acceptance, retest, and runner states normally require explicit
`strategy_evidence`. Existing completed chart bars can prove many of those facts if normalized and
ordered safely. The receiving template also repeats decision language across several cards.

## Goals / Non-Goals

**Goals:**

- Deliver newer cached canonical generations automatically without forcing providers or disrupting
  an unchanged page.
- Make component freshness explicit and authoritative for execution permission.
- Derive ordered strategy evidence from completed candles with timestamped provenance.
- Reduce repeated conclusions to one stable execution hierarchy while preserving useful research.

**Non-Goals:**

- Order entry, authorization, risk or sizing systems, predictive commentary, or new providers.
- Inferring evidence from an unfinished candle or from prose/UI state.
- Replacing the chart, gamma ladder, navigation, or the complete Market Pulse design system.

## Decisions

### 1. Canonical generation fingerprint at the service boundary

Build a serializable canonical metadata block after quote, bar, gamma, options, strategy, and
guardrail assembly. Generate a stable version from normalized execution-critical content and expose
component as-of values plus oldest-required age. Automatic checks call the existing cached context
boundary without `refresh=1`; manual refresh remains the only forced-provider browser path.

Alternative: reload every fixed interval. Rejected because unchanged reloads reset chart and scroll
state, add load, and obscure whether underlying data actually changed.

### 2. One browser synchronization coordinator

Add one coordinator owning the 15-second visible check, `visibilitychange`/focus catch-up, manual
forced refresh, concurrency gate, retry status, and last-applied generation. An unchanged generation
is a no-op. A newer generation is applied through one atomic canonical receiving boundary that
updates all execution-critical nodes and relevant chart data from the validated payload without
navigating or replacing the document. The receiving boundary validates the complete payload first,
then commits its DOM and chart mutations as one generation; failure leaves the previous generation
intact.

Alternative: canonical page navigation with state restoration. Rejected because even a restored
page interrupts chart inspection, drawing state, focus, and execution flow. Independently patching
uncoordinated fields is also rejected because it previously allowed conflicting generations.

### 3. Pure completed-candle evidence extractor

Add a focused Python service beside the existing failed-sweep evaluator. It consumes normalized
five- and fifteen-minute OHLC bars, session identity, active-level identity/value, approach side,
and current time. It emits the evaluator's existing evidence schema with timestamps and provenance.
Only bars whose interval end is at or before current time qualify. Sorting, deduplication,
finite-value validation, chronological prerequisites, and session scoping happen before evaluation.

Strat 2-2 classification compares consecutive completed inside/outside relationships using the
repository's established Strat semantics. Trigger price is derived from the qualifying reversal
candle and can only break on later observations. Acceptance uses separately defined completed-close,
hold, and retest rules and cannot coexist with rejection-ready for the same interaction.

Alternative: implement candle recognition in JavaScript. Rejected because backend-rendered and API
states would diverge and domain tests would be less authoritative.

### 4. Evidence identity prevents carryover

Attach an evidence identity containing ticker, market session date, level key, normalized level
value, approach direction, and timeframe. A material level-value change, direction change, ticker
change, regime-driven active-level change, or session transition invalidates incompatible evidence.
The extractor recomputes from available bars rather than persisting mutable confirmation state.

### 5. One verdict model feeds the execution strip and decision card

Extend the canonical view model with a single verdict object: permission, verdict, active level,
distance, confirmed evidence summary, next evidence, invalidation, management, primary target, and
generation/freshness metadata. The sticky strip shows its compact subset; the decision card shows
details. Other sections may show inputs but cannot compute or label actionability independently.

Secondary tape, news, flow, and extended structure remain intact behind existing folds/research
mode. Contract ideas remain de-emphasized until the verdict reaches a ready terminal state.

The first viewport uses a compact four-part summary: authoritative verdict and lock reason, active
level with distance, next required evidence, and invalidation. Spot, generation hashes, repeated
availability/path labels, and instructional filler do not repeat in that strip. The status row names
the current gamma regime directly. The existing pin preference becomes the single sticky-summary
toggle for the execution strip; its visible label always reports whether stickiness is On or Off.

### 6. Failure behavior is last-valid and explicit

Invalid automatic responses do not replace the rendered generation. The coordinator reports sync
degradation and retries with bounded backoff while retaining normal manual refresh. Missing or stale
required component timestamps lock the verdict. Malformed bars degrade only affected evidence to
Unavailable and cannot crash serialization or page rendering.

### 7. Data-lock diagnostics remain secondary and canonical

Add one collapsed disclosure directly after the authoritative execution strip. Its summary names
the lock state and blocking required components. Expanded rows render spot, bars, gamma, options,
and strategy from `canonical_freshness.components`, including status, age, threshold, component
as-of time, required/optional role, and a concise source/cache label derived from the same payload.
The footer reports the last successful canonical generation time and automatic retry interval.

Diagnostics update inside the existing canonical in-place boundary. They do not fetch providers,
add another refresh button, infer provider health from age alone, or alter the execution lock.

### 8. Canonical strategy overlays use the existing chart instance

Extend the canonical verdict with a structured invalidation level when the backend can resolve it
to a validated level. The chart owns a separate managed collection for Active, Invalidation, and
first valid Target price lines. Initial render and `market-pulse-canonical-update` both reconcile
that collection from the same verdict generation. Missing, non-finite, or non-positive values are
omitted; the browser never parses display prose to manufacture a price.

Target eligibility is decided in the domain evaluator, not the chart. `primary_target` and
expansion targets remain absent for pending, planning, locked, acceptance-without-retest, invalid,
or unavailable states. They become canonical only for `REVERSAL_READY` or
`CONTINUATION_ACTIVE`, and the chart labels the resulting line `REVERSAL TARGET` or
`CONTINUATION TARGET` from the canonical path.

Exact-price collisions are combined into one label so overlapping roles remain readable. A
reconciliation removes only canonical strategy lines and never recreates the chart, calls
`fitContent`, changes the selected timeframe, or touches gamma selection lines, session levels,
drawings, or the visible logical range. A small read-only debug state exposes the applied generation
and normalized overlays for focused functional verification.

## Data Flow

1. Existing workers update quote, bar, options, and gamma caches at their native cadence.
2. The context service assembles one canonical generation and component freshness metadata.
3. Completed candles enter the pure evidence extractor; its output enters the existing failed-sweep
   evaluator and canonical verdict builder.
4. The browser coordinator checks the cached generation while visible.
5. Unchanged generations are ignored; newer valid generations replace all execution-critical
   consumers atomically.
6. The execution strip, decision card, checklist, chart context, and freshness indicators render
   from that same generation.
7. The existing chart reconciles its managed strategy overlays from that same verdict generation.

## Risks / Trade-offs

- [A 15-second check adds local API traffic] → Return cached payloads, avoid provider forcing, use
  no-op generation comparison, pause hidden tabs, and prevent overlap.
- [A partial in-place commit can create conflicting values] → Validate the complete payload first,
  stage one normalized update model, then commit all registered execution consumers together and
  advance the generation id only after success.
- [Candle semantics can over-confirm around boundaries] → Use completed bars, strict chronological
  ordering, explicit session identity, exact tests, and Pending on ambiguity.
- [Acceptance/rejection conflict] → Keep terminal paths exclusive in both extraction and the existing
  evaluator; conflicting evidence yields invalid/unavailable state.
- [Large template/static files increase regression risk] → Add behavior behind focused helpers and
  contract tests; do not perform unrelated component refactors in this change.
- [Removing copy can hide safety context] → Retain the authoritative lock reason, active level,
  next evidence, and invalidation while removing only duplicate or generic helper text.
- [Diagnostics can recreate above-the-fold clutter] → Keep the disclosure collapsed by default,
  use one compact status summary, and place component detail only inside the expanded body.
- [Strategy lines can duplicate existing gamma levels] → Merge canonical strategy roles that
  share an exact price and keep their managed collection isolated from the existing level controls.
- [A mapped destination can look executable before confirmation] → Withhold canonical target
  fields until a terminal reversal or continuation path is confirmed.
- [Financial harm from stale or overstated evidence] → Component-level freshness locks, timestamped
  provenance, deterministic copy, and no order/permission semantics beyond the existing planning gate.
- [Privacy/security] → No new external requests, credentials, personal data, or persistence.

## Migration Plan

1. Add canonical metadata and compatibility tests without enabling browser polling.
2. Add the pure candle extractor and integrate its output behind the existing evidence contract.
3. Switch the verdict/checklist to derived evidence and verify legacy explicit evidence compatibility.
4. Enable the browser coordinator and atomic receiving update.
5. Consolidate execution presentation, run focused/full verification, rebuild, and inspect the
   authenticated desktop and narrow receiving surfaces.
6. Roll back by disabling automatic synchronization and derived evidence, then reverting additive
   payload/template fields; no data migration or stored-state rollback is required.

## Open Questions

None blocking. Exact material level-change tolerance and active-session freshness thresholds should
reuse existing repository constants where available and be locked by tests during implementation.
