## Context

Setup Replay already receives ordered Gamma observations and selects the newest observation whose
timestamp is not later than the setup signal. The frozen replay record currently keeps only
`gamma_as_of` and a boolean availability flag inside `data_availability`; Setup Analytics then
stores that nested evidence JSON but has no typed Gamma columns, API filters, or study surfaces.
Most existing rows therefore cannot answer which Gamma regime existed at signal time.

The implementation must preserve point-in-time provenance, remain additive for the existing SQLite
database, and never replace missing historical context with the current Gamma snapshot.

## Goals / Non-Goals

**Goals:**

- Freeze the selected signal-time Gamma regime and provenance in each replay setup.
- Persist normalized Gamma context in typed analytics columns while retaining evidence JSON.
- Expose Gamma regime as an analytics filter, comparison dimension, and ledger detail.
- Make missing or unprovable history visibly unavailable.
- Preserve setup counts, outcomes, targets, and current option-profit projections.

**Non-Goals:**

- Rebuilding past Gamma from present-day snapshots or candle movement.
- Storing complete option chains, strikes, or dealer books with every setup.
- Changing the Gamma provider, regime classifier, replay trigger logic, or execution permissions.
- Treating Gamma as a substitute for the ordered setup evidence.

## Decisions

### 1. Freeze Gamma in Replay at signal creation

The replay event will include normalized `gamma_regime`, `gamma_as_of`, `gamma_source`, and
`gamma_status`. Values come only from the already-selected observation at or before the signal.
When no eligible observation exists, regime is `unavailable`, timestamp/source remain empty, and
status explains that signal-time Gamma was unavailable.

Alternative considered: resolve Gamma when Analytics imports the event. Rejected because import
may occur later and could accidentally attach future knowledge.

### 2. Add typed nullable/defaulted analytics columns

An additive migration will add Gamma regime, as-of timestamp, source, and status columns to
`market_pulse_setup_events`, with an index on regime for filtering. Existing rows remain
`unavailable` unless their persisted evidence contains a valid pre-signal timestamp and explicit
regime; timestamp-only evidence is not enough to infer a regime.

Alternative considered: query only `evidence_json`. Rejected because filtering and aggregation
would require fragile JSON parsing and would not establish a stable analytics contract.

### 3. Preserve immutable signal context during outcome updates

Insert stores the frozen Gamma fields. Later outcome/MFE/MAE updates SHALL NOT overwrite them with
new snapshots. A same-event update may fill an empty Gamma field only when it carries the same
signal-time observation and passes the timestamp boundary check.

### 4. Add compact Gamma study surfaces

The analytics API will expose Gamma filter values, per-regime comparisons, coverage counts, and
ledger fields. The page will add one compact Gamma filter and concise ledger/detail labels rather
than another large dashboard panel. Unavailable Gamma remains a visible study cohort.

## Data Flow

1. Timestamped Gamma observations enter Setup Replay.
2. Replay selects the latest observation at or before the signal and freezes normalized context.
3. Analytics canonicalization validates that `gamma_as_of <= signal_time`.
4. The repository stores typed Gamma fields plus existing evidence JSON.
5. Analytics queries group/filter records by the stored signal-time regime.
6. The page renders the persisted value and provenance, never a live replacement.

## Risks / Trade-offs

- [Most legacy rows remain unavailable] → Report Gamma coverage separately and do not imply those
  rows belong to any regime.
- [A malformed or future timestamp could contaminate history] → Normalize timestamps and downgrade
  the Gamma context to unavailable when it exceeds signal time.
- [Outcome refresh overwrites frozen context] → Limit update SQL to outcome fields and narrowly
  controlled empty-field enrichment.
- [New filter fragments cache identity] → Include Gamma in normalized filters and existing cache
  keys; keep the indexed value low-cardinality.
- [Schema rollback leaves extra columns] → Application rollback ignores additive columns; no
  destructive down-migration is required.

## Migration Plan

1. Add the migration and migration tests against existing and fresh databases.
2. Add replay freezing and canonical record validation with point-in-time tests.
3. Extend analytics repository/API and UI study surfaces.
4. Run focused tests, JavaScript syntax, strict OpenSpec validation, then deploy through local K8s.
5. Verify new setups store Gamma context while old unproven rows remain unavailable.

## Open Questions

None. The governing rule is strict: only explicit observations available at signal time qualify.
