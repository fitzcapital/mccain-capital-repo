## Why

Setup Replay can select the latest Gamma observation available at or before a signal, but Setup
Analytics currently persists only limited availability metadata and drops the actual regime. This
prevents trustworthy study of how setups performed in positive, negative, transition, or
unavailable Gamma conditions.

## What Changes

- Persist the point-in-time Gamma regime, observation timestamp, source, and availability state
  with each canonical setup event.
- Carry the same frozen Gamma context from Setup Replay into the durable analytics record without
  substituting a later or current snapshot.
- Expose signal-time Gamma in the analytics API, ledger details, filters, and aggregate comparisons.
- Mark historical records without valid contemporaneous Gamma as unavailable; do not fabricate or
  retroactively infer Gamma from current data.
- Preserve existing setup identity, outcome, target, MFE/MAE, and option-projection behavior.
- Add migration, replay, persistence, API, and receiving-page regression coverage.
- Keep the feature read-only and planning/analysis-only; it does not authorize trades or change
  setup eligibility.

### Acceptance Criteria

- A setup generated with an eligible Gamma observation stores the exact regime and observation
  timestamp selected at signal time.
- A Gamma observation after the signal is never attached to that setup.
- Missing historical Gamma remains explicitly unavailable in both storage and UI.
- Analytics can filter and compare setup outcomes by signal-time Gamma regime without changing the
  canonical setup count.
- Existing records and databases migrate safely without rewriting personal or runtime data.
- Focused replay, migration, analytics API, JavaScript, and page-contract checks pass.

### Non-Goals

- Reconstructing or guessing Gamma for legacy records.
- Using today's Gamma as a historical proxy.
- Persisting an entire option chain or dealer-position dataset per setup.
- Changing Gamma computation, scenario ranking, setup triggers, targets, or execution permission.

## Capabilities

### New Capabilities

- `market-pulse-signal-gamma-history`: Defines point-in-time Gamma capture, provenance-safe
  persistence, unavailable-history behavior, and Gamma-aware Setup Analytics study surfaces.

### Modified Capabilities

None.

## Impact

- Affected code: Setup Replay event freezing, Setup Analytics canonicalization/repository queries,
  database migrations, analytics API/view models, page JavaScript/template filters and ledger
  details, and focused tests.
- Data sources: existing timestamped Gamma observations already supplied to Replay; no new provider
  or external dependency is introduced.
- Stored data: additive nullable/defaulted fields on `market_pulse_setup_events`, with current rows
  remaining unavailable unless their existing evidence already proves signal-time Gamma.
- Financial assumptions: Gamma is market context only. Profit estimates remain illustrative and
  continue to exclude historical option fills, IV path, spread, slippage, and realized P&L.
