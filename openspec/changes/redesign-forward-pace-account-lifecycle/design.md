## Context

The existing standalone calculator projects generic net cashflow after estimated tax and reserves. The supplied Vanquish documentation instead defines two linked account phases. Evaluation accounts require 10% profit without breaching loss rules. Performance-account payouts reduce account balance; before the profit buffer the loss limit continues to move, while after the buffer it becomes fixed. The public source provides exact $50,000 examples but not a complete buffer/loss-limit table for every plan size.

## Goals / Non-Goals

**Goals:**

- Make account phase and the next lifecycle milestone immediately clear.
- Center the page on the selected daily pace and end date rather than a generic lifecycle overview.
- Keep broker-account growth separate from withdrawn, taxable payout income.
- Quantify protected payout capacity and the result of a proposed payout.
- Visualize milestones and dates across conservative, expected, and stretch pace.
- Make incomplete source rules explicit and editable rather than inferred.

**Non-Goals:**

- Broker API integration, payout submission, or eligibility certification.
- Automated trading or risk decisions.
- Storage of personal account state.
- Invention of plan-size rules absent from the supplied public documentation.

## Decisions

### 1. Use an explicit account phase

The payload will accept `account_phase` as `evaluation` or `performance`. Evaluation mode centers the 10% target. Performance mode centers the configured buffer and loss limit. Keeping separate modes avoids presenting performance payouts before an evaluation is passed.

### 2. Treat trading pace as net broker-account profit

The primary rate input becomes average net profit per trading day. Taxes and personal reserves do not reduce evaluation or performance-account progress because those apply when money is withdrawn, not while it remains broker equity.

### 3. Use documented defaults only where supported

Evaluation target defaults to 10% of plan size for all Basic Options plans, matching the source. The $50,000 performance example defaults to a $52,875 buffer and $50,375 fixed loss limit. Other plan sizes require manual buffer and fixed-limit inputs; the UI will not silently scale the $50,000 example.

### 4. Separate theoretical and protected payout capacity

Theoretical capacity equals current or projected balance minus the applicable loss limit. Protected capacity further subtracts the user's desired safety cushion. A proposed payout is safe only when its post-payout balance remains strictly above the applicable loss limit plus cushion.

### 5. Keep a configurable pre-buffer loss limit

Performance mode accepts the current loss limit because the public article states it moves before the buffer but does not fully define every dynamic case. The tool labels it user-entered/current until the buffer is reached, then uses the configured fixed limit.

### 6. Derive lifecycle dates from weekday pace

Milestone dates use net daily profit and Monday-Friday sessions, excluding weekends but not exchange holidays. Scenario multipliers remain 75%, 100%, and 125%. A next-account-ready date is the first date protected payout capacity reaches the configured next-account funding amount.

### 7. Preserve server authority and explicit updates

The API returns all milestone, safety, schedule, and chart data. The browser applies draft changes only through Update Projection, then redraws native SVG and accessible text from that response.

### 8. Return one strategic action

Evaluation mode recommends continuing until the passing balance and reports its estimated date. Performance mode compares protected capacity with the desired payout. If the desired payout is protected now, it recommends taking it and reports the post-payout balance. Otherwise it recommends waiting and calculates the required balance, additional profit, trading sessions, and estimated readiness date. The interface presents this verdict before supporting calculations.

The desired payout is the governing Performance input. Its required balance is the greater of the performance buffer and the fixed protected floor plus the desired payout. This prevents the pre-buffer trailing limit from making a future payout appear available too early. For example, a $10,000 desired payout with a $50,375 fixed limit and $1,000 cushion requires $61,375, followed by a $51,375 post-payout balance.

### 9. Replace the ladder with a decision path

The primary output is a compact sequence: current balance, projected ending balance, milestone crossings, and payout decision. The chart draws the projected balance path and labels only milestones relevant to the active phase. Detailed loss-limit math remains available as supporting context in Performance mode rather than competing with the verdict.

## Risks / Trade-offs

- **Vanquish rules may change** -> Attribute the rules, label the retrieval context, and keep undocumented performance thresholds editable.
- **A calculated payout may still be ineligible** -> Label output planning-only and state that all separate payout rules must still be met.
- **Pre-buffer loss limits are dynamic** -> Require current loss limit instead of simulating undocumented movement.
- **Weekday dates differ from market dates** -> Disclose that market holidays are not excluded.
- **Large daily pace can imply unrealistic dates** -> Present scenarios as mathematical projections, not promises.

## Migration Plan

No stored data is migrated. Replace the standalone planner model and page assets together while keeping the route and explicit update interaction. Rollback restores the previous calculator without affecting broker or journal data.

## Open Questions

None for implementation. Additional official plan-size buffer tables can be incorporated later without changing the lifecycle model.
