## Why

Setup Analytics shows frequency and outcome rates, but it does not plainly identify the strongest
setup, strongest time window, or strongest setup-and-time combination across the selected history.
Traders must interpret several charts manually, and tiny perfect samples can look better than
larger, more reliable evidence.

## What Changes

- Add a prominent "What worked best" summary that updates with every study horizon, especially All
  History.
- Rank setup families, 30-minute time windows, and setup-family/time-window combinations using only
  completed outcomes.
- Show target rate, completed sample size, median favorable move, median adverse move, and an
  evidence-strength label beside every winner.
- Require a meaningful minimum sample for a definitive winner and label smaller samples as early
  evidence rather than presenting them as proven.
- Prefer repeatability and favorable-versus-adverse movement over a raw 100% rate from one or two
  occurrences.
- Add direct controls from each winner to filter the page to the underlying setups.
- Explain when no trustworthy winner exists because the selected horizon lacks completed outcomes.
- Count one actionable setup per signal candle, family, direction, and traded level even when legacy
  and current pattern identifiers describe the same event.
- Keep generated screenshots and chart images outside normal page-request behavior; portfolio and
  diagnostic captures remain explicit tools only.

Non-goals:

- Changing setup detection, target/invalidation rules, Replay outcomes, or alerts.
- Treating projected option profit as realized performance or using it to rank setup quality.
- Claiming statistical significance from the current small historical sample.

Acceptance criteria:

- Selecting All History immediately displays the best setup family, best 30-minute window, and best
  setup/time pairing for that exact filtered dataset.
- A perfect small sample cannot outrank a sufficiently larger sample solely because its raw target
  rate is 100%.
- Every leader includes its completed-outcome denominator and clear confidence wording.
- Clicking a leader applies matching filters consistently to KPIs, charts, family cards, and ledger.
- Empty or insufficient selections display a plain-language evidence limitation instead of a
  fabricated winner.

## Capabilities

### New Capabilities

- `market-pulse-setup-history-leaders`: Evidence-aware ranking and drill-down for historical setup
  families, time windows, and combined setup/time cohorts.

### Modified Capabilities

- None.

## Impact

- Setup analytics aggregation and response payloads under `mccain_capital/services/`.
- Setup Analytics header/overview, filters, charts, family cards, and client interactions.
- Focused Python and JavaScript tests for ranking, minimum samples, ties, filtering, and empty data.
- No new provider, dependency, database migration, credential, or financial assumption.
- Historical rows remain intact; deduplication is an analytics read rule, not destructive cleanup.
