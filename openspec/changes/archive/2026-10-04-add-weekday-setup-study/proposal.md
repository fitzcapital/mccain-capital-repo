## Why

Setup Analytics already retains enough SPX setup history to compare performance by session
date, family, and time window, but it does not explain whether a setup behaves differently by
weekday. Traders need a reliable Monday-through-Friday study that identifies the strongest
setup and time combination without presenting a tiny sample's raw 100% result as proven.

## What Changes

- Add an all-history weekday study derived from canonical, deduplicated SPX setup events.
- Summarize each weekday using total occurrences, completed outcomes, target rate, outcome
  coverage, median favorable move, and median adverse move.
- Rank the best setup family and best setup-plus-time combination within each weekday using
  completed outcomes, evidence maturity, adjusted target rate, and excursion quality.
- Add a weekday filter and a compact weekday study view to Setup Analytics, with expandable or
  hover help for definitions rather than permanent explanatory clutter.
- Label insufficient, early, and established samples so one- or two-result groups are not
  presented as dependable leaders.
- Keep open and unavailable outcomes visible in coverage counts but exclude them from target-rate
  denominators and leader scoring.
- Keep gamma context visible when captured, while explicitly withholding weekday-by-gamma
  conclusions when coverage is insufficient.

### Non-goals

- This change will not alter setup detection, entry, target, invalidation, or gamma rules.
- It will not represent setup outcomes as executed trades, option fills, or realized profit.
- It will not backfill missing gamma snapshots or fabricate historical outcomes.
- It will not add external charting dependencies or a separate analytics database.

### Acceptance criteria

- Every canonical setup belongs to exactly one New York weekday and existing date/time/family
  filters continue to constrain the same population.
- Weekday totals reconcile with the filtered analytics total, with duplicate exclusions reported.
- Target rate is calculated as target reached divided by completed outcomes; open and unavailable
  outcomes are excluded from the denominator.
- Each weekday exposes its best eligible setup family and family-plus-time combination, or a clear
  insufficient-evidence state.
- Raw 100% rates with small samples are labeled early evidence and cannot outrank stronger mature
  evidence solely because of the raw percentage.
- The page remains compact, responsive, keyboard accessible, and understandable through concise
  labels plus optional helpers.
- Focused service, API-contract, rendering, and interaction tests cover aggregation, ranking,
  filters, empty states, and reconciliation.

## Capabilities

### New Capabilities

- `market-pulse-weekday-setup-study`: Weekday aggregation, evidence-aware ranking, filtering, and
  compact visualization of retained SPX setup outcomes.

### Modified Capabilities

None.

## Impact

- Extends the existing Setup Analytics service payload and query options.
- Updates the Setup Analytics template and its existing JavaScript/CSS presentation layer.
- Reads only the durable `market_pulse_setup_events` history; no schema migration is expected.
- Uses the existing canonical deduplication, outcome definitions, evidence thresholds, and Wilson
  adjusted-rate ranking conventions.
- Adds focused tests around analytics aggregation and the user-visible weekday study contract.
