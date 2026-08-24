## Why

Forward Pace must answer a direct planning question: if the trader earns a chosen amount per trading day through a chosen date, what will the account balance be, which qualification or buffer milestones will be crossed, and should the trader take a payout now or wait for a safer date and amount?

## What Changes

- Replace generic payout-frequency inputs with plan size, account phase, current balance, average net trading-day profit, expected trading days per week, and planning dates.
- Model the Basic Options evaluation milestone as 10% above starting balance, based on the supplied Vanquish profit-target article.
- Model performance-account buffer, fixed loss limit, theoretical payout capacity, protected payout capacity, proposed payout, post-payout balance, and remaining safety cushion.
- Provide configurable rule fields for buffer balance and fixed loss limit because the supplied public article gives a $50,000 example but not a complete rule table for every plan size.
- Replace the generic lifecycle dashboard with a focused projection result: projected ending balance, milestone status and crossing dates, and one direct payout recommendation.
- Add a balance-over-time chart with only phase-relevant pass, buffer, and recommended-payout markers.
- Estimate dates for evaluation pass, performance buffer, proposed protected payout, and next-account readiness under conservative, expected, and stretch daily-profit scenarios.
- Move tax estimation to a collapsed payout-income section and never subtract tax from evaluation or performance-account balance progress.
- Preserve explicit Update Projection behavior and PDF export.

### Non-goals

- Automating trades, submitting payout requests, or changing broker accounts.
- Claiming that estimated dates or safety cushions guarantee eligibility.
- Inferring Vanquish rules that are not stated in the supplied sources.
- Persisting account values or modifying Dashboard projection behavior.

### Acceptance Criteria

- Evaluation mode identifies the 10% target, remaining profit, projected ending balance, and estimated passing date.
- Performance mode identifies whether the buffer is reached and whether the loss limit is still trailing or fixed.
- Payout capacity is calculated from account balance above the applicable loss limit, with a separate user-selected safety cushion.
- Performance mode recommends a strategic payout amount or explicitly advises waiting, including the next balance and estimated date that unlock the desired payout.
- Tax estimates apply only to withdrawn payout income and do not reduce broker-account milestone progress.
- Every chart and milestone uses the same server-authoritative projection.
- Vanquish-derived assumptions are visibly attributed and configurable where the source is incomplete.

## Capabilities

### New Capabilities

- `forward-pace-account-lifecycle`: Evaluation, performance buffer, loss-limit protection, payout capacity, and next-account milestone planning.

### Modified Capabilities

None.

## Impact

- Forward Pace server model, API response, PDF export, template, JavaScript, CSS, and focused tests.
- User-supplied Vanquish public rules become documented planning inputs; no third-party API or dependency is added.
- No database migration, broker write, runtime-data change, or automated financial action.
