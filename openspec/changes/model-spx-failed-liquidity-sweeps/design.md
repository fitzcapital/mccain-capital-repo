## Context

The SPX Playbook is rendered by `mccain_capital/services/core.py` and
`mccain_capital/templates/core/market_pulse.html`, with browser refresh/render behavior in the
Market Pulse JavaScript and existing styling in `static/css/market_pulse.css`. Today the backend
produces broad gamma/location posture and a four-item manual checklist whose items are initialized
inactive. Related planning copy can equate location with a buy-dip or sell-rip posture, but the
actual strategy requires ordered five-minute evidence and a separate acceptance path.

Existing inputs already expose gamma walls/flips, spot, day levels, price series, session/freshness,
and chart state. The implementation must use explicit candle/Strat evidence where present; it must
surface unavailable states rather than reconstructing confirmation from insufficient aggregates.

## Goals / Non-Goals

**Goals:**

- Centralize failed-sweep evaluation in a pure Python domain module with typed input/output models.
- Enforce ordered reversal transitions and mutually exclusive acceptance/continuation outcomes.
- Adapt existing market data into normalized evidence without changing providers.
- Produce one deterministic backend contract for the decision card and six-item checklist.
- Preserve the compact dark Playbook surface and existing chart/ladder controls.

**Non-Goals:**

- Order execution, trade authorization, position sizing, risk calculation, loss limits, or account
  locking.
- Predictive or AI-authored strategy commentary.
- Replacement of the chart, gamma ladder, navigation, quote/gamma providers, or overall page.
- Inferring historical confirmation that the available feed cannot prove.

## Decisions

### 1. Pure domain evaluator with explicit evidence

Add a focused service/domain module under `mccain_capital/services/` containing enums or Literals,
typed dataclasses/TypedDicts, normalization, and a side-effect-free `evaluate_*` function. Its input
will identify context (level key/value, approach side, gamma regime, timeframe) and optional,
timestamped booleans or events for location, sweep, completed close-back-inside, five-minute 2-2,
trigger break, acceptance close/hold/retest, and optional fifteen-minute runner confirmation.

The evaluator will choose the earliest unmet ordered reversal state, but acceptance evidence is a
separate terminal branch. Inconsistent claims produce `SETUP_INVALIDATED` or unavailable evidence,
not precedence-by-accident. Evidence objects are returned so timestamps can survive when supplied.

Alternative considered: extend `_market_pulse_trigger_validation_viewmodel` with more conditionals.
Rejected because it mixes copy/render concerns with domain transitions and is difficult to test
exhaustively.

### 2. Thin adapter from current Market Pulse data

The existing structure snapshot builder will normalize supported level candidates and available
OHLC/Strat evidence into the evaluator input, then attach the result to the canonical Playbook view
model. Missing level or candle evidence is passed as unknown, never derived from prose or UI state.
The adapter will discard non-finite numbers, invalid enum strings, incompatible timestamps, and
malformed collections before evaluation.

Alternative considered: add a new market-data provider. Rejected because the scope requires
preserving providers and because absent confirmation must remain explicitly unavailable.

### 3. Deterministic target selection

Build an ordered, deduplicated set of valid numeric levels from walls/flips, new walls, and day
highs/lows. For bearish outcomes select lower levels; for bullish outcomes select higher levels.
The nearest directional level is the primary target. Negative gamma may expose subsequent levels
as expansion targets; positive gamma exposes only the nearest meaningful target. Continuation uses
the same directional ordering beyond the accepted level. No sample price is introduced.

Alternative considered: hand-written targets per setup type. Rejected because it would diverge as
available levels change and would be harder to verify deterministically.

### 4. Backend contract drives both decision card and checklist

Replace the current four trigger items with six stable keys and four-valued status. The decision
card will consume evaluator labels/fields directly. Browser refresh code may update those same
nodes but will not reimplement strategy transitions. Existing IDs needed by unrelated controls are
preserved or mapped narrowly to avoid breaking chart/ladder behavior.

Alternative considered: evaluate in TypeScript/JavaScript for live updates. Rejected because the
Flask-rendered page and service tests already establish a backend source of truth, and duplicate
frontend logic would risk contradictory outputs.

### 5. Gamma changes management, not setup validity

Regime copy is mapped centrally: positive gamma means absorption/mean reversion and nearest-target
scalp management; negative gamma means expansion after either path gains control, with Target 1
plus optional further levels; Gamma Flip/unconfirmed means reduced confidence pending clear
acceptance or rejection. Fifteen-minute 2-2 evidence is output only as runner support.

### 6. One canonical refresh and decision hierarchy

Replace the unlabeled context-refresh icon and buried full-page refresh link with one compact,
labeled `Refresh data` control beside the updated timestamp. The existing context API remains the
refresh boundary, but success triggers a canonical page-state reload so server-rendered header,
guardrail, decision narrative, chart context, strategy card, and checklist cannot retain different
generations of data. The button presents Refreshing, Updated, and failure/last-valid feedback and
is disabled while a refresh is active. Gamma-ladder-only refresh remains local to the ladder.

The backend/page view applies precedence in this order: guardrail lock or unavailable data;
strategy state/path; gamma interpretation; targets. Legacy score/status fields cannot override a
locked or pending strategy state.

Alternative considered: patch every existing DOM value from the context response. Rejected because
the page has several server-rendered consumers and partial patching already produced conflicting
spot and permission values. Reloading after the canonical refresh is the smaller, safer coherence
boundary.

## Data Flow

1. Existing quote, gamma snapshot, chart series, and day-level data enter the Market Pulse service.
2. A narrow adapter selects/normalizes supported levels and explicit interaction evidence.
3. The pure evaluator returns state, path, direction, evidence/checklist statuses, missing evidence,
   regime interpretation, and target keys/values.
4. The canonical playbook view model serializes that result for Jinja and refresh consumers.
5. Jinja renders the compact card/checklist; JavaScript updates only from the returned contract.

## Failure and Compatibility Behavior

- Missing or stale inputs degrade individual evidence to Pending/Unavailable; no ready state is
  synthesized from spot location alone.
- Malformed values are normalized before serialization, preventing invalid JSON/template output.
- Existing payload keys required by chart, gamma ladder, dashboard, and refresh consumers remain
  intact while new strategy fields are additive; old generic copy can be mapped from the new result
  during migration.
- There is no database migration and no runtime/personal data modification.

## Risks / Trade-offs

- [Current feeds may not expose completed 5m 2-2 or trigger evidence] → Show Unavailable and keep
  the state pending until explicit evidence exists; document this limitation in verification.
- [Ambiguous active level when several levels cluster] → Use stable nearest-distance ordering with
  deterministic key priority and expose the selected level.
- [Conflicting live refresh and server-rendered state] → Keep evaluation server-side and update the
  receiving page from one canonical refreshed generation; do not retain partial legacy values.
- [Existing tests assert legacy buy-dip/sell-rip copy] → Update only assertions whose behavioral
  contract changes and retain unrelated compatibility fields.
- [Financial harm from overstated certainty] → Use evidence-safe statuses, no order/permission
  semantics, and explicit missing-data explanations.
- [Privacy/security] → No new external calls, credentials, user data, or persistence are added.

## Migration Plan

1. Add evaluator and exhaustive unit tests without routing it to the UI.
2. Add the current-data adapter and canonical playbook payload fields with service tests.
3. Switch the decision/checklist rendering and refresh bindings to the canonical result.
4. Run focused tests, type/lint/build checks, then desktop/narrow visual verification.
5. Roll back by reverting the additive module/payload fields and template/static edits; no stored
   data rollback is required.

## Open Questions

None requiring a strategy assumption. During implementation, any confirmation evidence absent from
the current feed will remain Unavailable rather than being inferred.
