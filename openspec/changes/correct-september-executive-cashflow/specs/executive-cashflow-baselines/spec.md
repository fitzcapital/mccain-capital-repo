## ADDED Requirements

### Requirement: Post-cycle opening balance
The Executive projection SHALL support an opening balance that already includes completed funding-cycle expenses and MUST give those completed expenses zero additional forward impact without suppressing future deposits.

#### Scenario: September post-first-cycle baseline
- **WHEN** September 2026 is projected from its configured $4,500 Current-account opening balance
- **THEN** first-cycle bills and subscriptions are shown as settled and are not deducted from that balance again
- **AND** both remaining September pay events retain their scheduled forward impact

#### Scenario: Unsettled activity remains projected
- **WHEN** a September entry belongs to the second cycle or has no completed-cycle classification
- **THEN** the entry retains its scheduled forward impact

### Requirement: Effective-month recurring obligation
The Executive projection SHALL include Ragan & Ragan as a $150 Current-account bill due on day 28 for September 2026 and each later configured month.

#### Scenario: Recurring bill starts in September
- **WHEN** the Executive model builds September 2026 or a later month
- **THEN** exactly one Ragan & Ragan bill for $150 due on day 28 is present for that month

#### Scenario: Historical months remain unchanged
- **WHEN** the Executive model builds July or August 2026
- **THEN** no Ragan & Ragan bill is present

### Requirement: Explicit financial assumptions
The Executive model SHALL expose the configured opening account allocation and completed-cycle assumption to the receiving dashboard.

#### Scenario: September model payload
- **WHEN** the Executive dashboard loads September 2026
- **THEN** its payload identifies $4,500 as Current-account opening cash and cycle 1 expenses as already settled

### Requirement: Clear post-cycle presentation
The Executive dashboard SHALL identify September's configured $4,500 as a future post-cycle baseline rather than today's live balance.

#### Scenario: September baseline label
- **WHEN** September 2026 is selected
- **THEN** the opening-balance field is labeled `Balance after cycle-one bills`
- **AND** the projection includes the regular September check, the larger September 25 check, and later expenses
