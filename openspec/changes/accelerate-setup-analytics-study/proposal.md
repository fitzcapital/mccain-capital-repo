## Why

Setup Analytics defaults to today, but studying another useful horizon still requires opening a
large filter drawer and choosing dates manually. Outcome gaps and the fixed $750 option-cost
assumption are also too vague for a research page: traders need to know whether a setup is still
open, was never evaluated, or lacks historical evidence, and whether a projected premium came from
Tradier or a fallback estimate.

## What Changes

- Default hypothetical trade size to 3 editable contracts and headline total estimated trade profit.
- Identify family occurrences by recorded time, level price, entry, target, pattern, and outcome.

- Add compact study presets for Today, Last 3 Sessions, This Week, Last 20 Sessions, and All
  History, with the active horizon visible without opening advanced filters.
- Keep the existing advanced filters for exact dates, time ranges, setup family, pattern,
  direction, level, grade, outcome, and sorting.
- Replace “No outcomes” with explicit states such as “Not evaluated yet,” “Outcome unavailable,”
  and “No completed outcomes in this selection,” including denominators in charts and cards.
- Use a current Tradier SPX/SPXW near-the-money option-chain quote to anchor premium projections
  when a suitable liquid directional contract can be selected, and show quote source, contract,
  bid/ask or mid, quote time, DTE, strike, spot distance, and delta.
- Fall back to the documented $750 planning assumption only when a current Tradier contract quote
  cannot be selected; never label a fallback estimate as live contract pricing.
- Keep option projections explicitly hypothetical. They are not fills, realized profit, or an
  assertion that a specific contract was tradable at the setup signal time.

Non-goals:

- Reconstructing historical option quotes for prior setup timestamps.
- Selecting or recommending a tradeable option contract for execution.
- Changing setup detection, Replay outcomes, targets, Gamma calculations, or alert behavior.
- Adding another market-data provider or storing raw option-chain responses.

Acceptance criteria:

- One click changes the entire analytics page to each preset and the resulting session/date scope
  is visible and consistent across every chart, KPI, family card, and ledger row.
- “No outcomes” does not appear without a plain-language explanation of the missing evidence.
- A current Tradier premium projection identifies its exact source contract and quote timestamp.
- The illustrative contract is the nearest eligible strike to current SPX spot; premium proximity
  to $750 and delta proximity to 0.40 are secondary tie-breakers, not substitutes for NTM.
- Missing, stale, crossed, or incomplete Tradier quotes produce a clearly labeled fallback estimate.
- Focused Python and JavaScript tests cover session-aware presets, outcome labels, live quote
  selection, stale/unavailable fallback, and financial-source disclosure.

## Capabilities

### New Capabilities

- `market-pulse-setup-study-workspace`: Fast session-aware study horizons, explicit outcome
  evidence states, and source-labeled option-premium projections for Setup Analytics.

### Modified Capabilities

- None.

## Impact

- Setup analytics query normalization, response payloads, page controls, charts, cards, and tests.
- Existing Tradier option-chain adapter logic will be reused or extracted into a narrow read-only
  pricing service with bounded caching and timeout behavior.
- No new dependency, database, provider, credential, or persistent raw market-data payload.
- Financial assumptions become source-aware: Tradier mid/last when current and complete; otherwise
  the existing $750 contract-cost and 0.40 absolute-delta planning fallback.
