## ADDED Requirements

### Requirement: Dated daily capital-flow path
The system SHALL calculate a daily running balance for BOA and Current from the selected month's opening balances and all effective ledger entries in chronological day order.

#### Scenario: Transactions are applied chronologically
- **WHEN** a withdrawal dated earlier in the month appears after a later deposit in ledger construction order
- **THEN** the system applies the withdrawal on its earlier date and the deposit on its later date

#### Scenario: Month-end balances reconcile
- **WHEN** the daily path is complete
- **THEN** each closing account balance equals its opening balance plus account inflows, minus account outflows, and net transfers

#### Scenario: Same-day entries are deterministic
- **WHEN** multiple effective entries share a due date
- **THEN** the system produces the same end-of-day account balances on every recalculation

### Requirement: Explicit and estimated transaction timing
The system SHALL use a valid explicit due day when supplied and SHALL apply a visible deterministic fallback assumption when timing is missing or invalid.

#### Scenario: Explicit due day is honored
- **WHEN** an effective ledger entry has a valid due day within the selected month
- **THEN** the entry affects the daily path on that day

#### Scenario: Missing due day uses a disclosed fallback
- **WHEN** an effective entry has no valid explicit due day
- **THEN** the system assigns a deterministic fallback day and identifies the timing as estimated

#### Scenario: Day exceeds the selected month
- **WHEN** an entry date exceeds the number of days in the selected month
- **THEN** the system applies the entry on the selected month's last valid day

### Requirement: Independent treasury guardrails
The system SHALL treat the temporary hard floor, permanent floor goal, protected reserve, and month-end target as independent values and SHALL not derive one from another.

#### Scenario: Configured August guardrails are loaded
- **WHEN** the August operating month is selected
- **THEN** the system uses a $4,000 active temporary floor and displays $10,000 as a future floor goal rather than a current breach

#### Scenario: Projected low is labeled as an outcome
- **WHEN** the capital-flow path is calculated
- **THEN** the minimum calculated BOA balance is displayed as the projected low and is not presented as a configured floor

### Requirement: Intramonth liquidity metrics
The system SHALL calculate the projected BOA low, its first occurrence date, immediate cushion, month-path cushion, funding gap, absorbable unexpected expense, and recovery date from the daily path using the active floor for the current phase.

#### Scenario: Temporary breach despite healthy close
- **WHEN** the BOA path falls below the hard floor and later closes above the month-end target
- **THEN** the system reports the earlier projected low and a positive funding gap

#### Scenario: Expense can be absorbed
- **WHEN** the projected low is above the hard floor
- **THEN** absorbable unexpected expense equals the projected low minus the hard floor

#### Scenario: Expense cannot be absorbed
- **WHEN** the projected low is at or below the hard floor
- **THEN** absorbable unexpected expense is zero and never negative

#### Scenario: Recovery date is available
- **WHEN** the path falls below the protected reserve and later returns to or above it
- **THEN** the system reports the first subsequent recovery date

#### Scenario: No recovery occurs
- **WHEN** the path remains below the protected reserve through month end
- **THEN** the system identifies that no recovery occurs within the selected month

### Requirement: Path-based treasury status
The system SHALL assign treasury status using the active reserve phase and lowest projected BOA balance before considering the projected closing balance.

#### Scenario: Hard floor is breached
- **WHEN** the build-phase projected low is below the $4,000 temporary floor
- **THEN** status is `Temporary Floor at Risk` regardless of the projected close

#### Scenario: Protected reserve is breached
- **WHEN** the build-phase projected low is at or above $4,000 and the $10,000 floor has not activated
- **THEN** status is `Building Normally`

#### Scenario: Reserve is protected below target
- **WHEN** a previously secured $10,000 floor is breached by the remaining projected path
- **THEN** status is `Rebuilding Secured Floor`

#### Scenario: Reserve and target are secured
- **WHEN** the remaining path from the first $10,000 balance never falls below $10,000
- **THEN** status is `$10K Floor Secured`

### Requirement: Permanent floor activation
The system SHALL keep $10,000 as a floor goal during accumulation and SHALL activate it as the permanent protected floor only when the remaining projected path supports it.

#### Scenario: Goal is touched but not secured
- **WHEN** the projected balance reaches $10,000 and a later remaining balance falls below $10,000
- **THEN** the system remains in build phase and does not activate the permanent floor

#### Scenario: Goal is secured
- **WHEN** the projected balance reaches $10,000 and every later projected balance remains at or above $10,000
- **THEN** the system enters secured phase, records the activation day, and uses $10,000 as the active floor

#### Scenario: Secured floor later falls below goal
- **WHEN** the permanent floor was previously secured and the current remaining path falls below $10,000
- **THEN** the system enters recovery phase and measures the funding gap against $10,000

#### Scenario: Build-phase absorption
- **WHEN** the permanent floor has not activated
- **THEN** absorbable unexpected expense is measured above the $4,000 temporary floor

#### Scenario: Secured-phase absorption
- **WHEN** the permanent floor has activated
- **THEN** absorbable unexpected expense is measured above the $10,000 permanent floor

### Requirement: Account transfers preserve combined capital
The system SHALL apply transfers to BOA and Current in equal opposite amounts without classifying them as income, expense, or combined-capital growth.

#### Scenario: BOA funds Current
- **WHEN** a BOA-to-Current transfer is effective
- **THEN** BOA decreases and Current increases by the same amount on the transfer day

#### Scenario: Current surplus sweeps to BOA
- **WHEN** a Current-to-BOA sweep is effective
- **THEN** Current decreases and BOA increases by the same amount on the sweep day

### Requirement: Manual state remains authoritative
The system SHALL recalculate from the current manual controls, ledger statuses, and adjustments without silently replacing them.

#### Scenario: Manual opening balance is changed
- **WHEN** the user enters a valid BOA opening balance and recalculates
- **THEN** the daily path and all liquidity metrics use that entered balance

#### Scenario: Paid or skipped entry changes projection
- **WHEN** a ledger entry status changes the entry's effective impact
- **THEN** recalculation uses the saved status in the daily path

#### Scenario: Current balance already includes paid activity
- **WHEN** the user enters a current as-of balance and an entry is marked `Paid` or `Skipped`
- **THEN** the entry remains visible but contributes zero additional impact to the forward projection

#### Scenario: Past-due obligation remains unpaid
- **WHEN** an entry's due day is before the as-of day and its status is `Planned` or `Adjusted`
- **THEN** the entry remains in the forward projection and is applied on the as-of day

#### Scenario: Invalid input is submitted
- **WHEN** a required projection input is invalid or non-finite
- **THEN** the system identifies the invalid input and retains the last successful projection

### Requirement: Continuous paycheck funding cycles
The system SHALL generate regular paycheck cycles every 14 days from July 31, 2026, and SHALL allow a funding window to cross a calendar-month boundary.

#### Scenario: August as-of date uses July 31 cycle
- **WHEN** the projection as-of date falls between July 31 and August 13
- **THEN** the system identifies July 31 as the active settled cycle and August 14 as the next pay event

#### Scenario: Regular paycheck allocation
- **WHEN** a regular future paycheck is projected
- **THEN** the system adds $1,873.78 to Current and $1,873.78 to BOA on its scheduled date

#### Scenario: September estimated exception
- **WHEN** the September 25 pay event is projected and no replacement amount exists
- **THEN** the system uses an estimated $4,700 Current and $4,700 BOA allocation and labels the event estimated

#### Scenario: Active cycle is automatically settled
- **WHEN** the user enters current as-of balances during a funding cycle
- **THEN** that cycle's paycheck and funded bills remain visible but contribute zero forward impact without individual status changes

#### Scenario: Future cycle remains projected
- **WHEN** a paycheck funding cycle begins after the as-of date
- **THEN** its deposits and funded bills remain in the forward capital path

### Requirement: Just-in-time Current funding
The system SHALL transfer money from BOA to Current only on a day when remaining scheduled activity would otherwise make Current negative.

#### Scenario: Current remains non-negative
- **WHEN** Current has enough opening cash and future deposits to cover a day's remaining bills
- **THEN** the system creates no BOA funding transfer for that day

#### Scenario: Current has a daily shortfall
- **WHEN** a day's remaining activity would make Current negative
- **THEN** the system transfers only the amount required to restore Current to zero on that day

#### Scenario: Multiple shortfall days
- **WHEN** Current becomes negative on more than one projected day
- **THEN** the system creates separate dated transfers and reports their sum as required Current funding

### Requirement: Explainable projection history
The system SHALL log a bounded local snapshot when the user explicitly recalculates a projection and SHALL not create history entries from passive rendering.

#### Scenario: Projection is explicitly recalculated
- **WHEN** the user selects Recalculate Projection with valid controls
- **THEN** the system logs the as-of balances, funding-cycle assumptions, projected low and close, required Current funding, active floor, and phase

#### Scenario: Projection history is reviewed
- **WHEN** projection snapshots exist for the selected month
- **THEN** the interface displays recent snapshots with their creation time and material assumptions

### Requirement: Projection assumptions are visible
The system SHALL disclose that the projection is plan-based and SHALL identify the opening balances, guardrails, dated entries, and estimated timing that materially determine the result.

#### Scenario: User reviews the projection basis
- **WHEN** the projection is displayed
- **THEN** the user can distinguish actual manual inputs from configured plan values and estimated timing assumptions

#### Scenario: Current month projection identifies its starting point
- **WHEN** the selected month is the current calendar month
- **THEN** the interface identifies the as-of day and calculates only the remaining forward path from the entered current balances

### Requirement: Desktop Executive decision hierarchy
The system SHALL present one authoritative desktop summary of the selected month's current BOA, projected low, active floor, absorbable cushion, projected close, and next paycheck without repeating the same complete metric set in adjacent page regions.

#### Scenario: BOA-only value is labeled accurately
- **WHEN** a summary displays the user-entered BOA as-of balance
- **THEN** it is labeled `Current BOA` rather than `Current Treasury`

#### Scenario: Executive page opens during an active month
- **WHEN** the current calendar month exists in the operating plan
- **THEN** the Executive workspace selects that month by default

#### Scenario: Phase recommendation is displayed
- **WHEN** the Overview or CEO panel displays an action
- **THEN** it uses the projection's phase-aware recommended action rather than a separate target-only rule

### Requirement: Single planning baseline
The system SHALL provide one 12-month planning baseline and SHALL disclose that future rows repeat configured assumptions until they are updated.

#### Scenario: Annual planning is opened
- **WHEN** the user selects Roadmap
- **THEN** the interface displays the 12-month planning baseline without a duplicate Year View control

#### Scenario: Projected close is zero
- **WHEN** a future month projects a legitimate zero closing balance
- **THEN** the baseline displays zero and does not substitute the month's target

#### Scenario: Permanent goal is not secured
- **WHEN** a month remains in build phase
- **THEN** the $10,000 value is labeled as the floor goal and not as an active or hard floor

#### Scenario: BOA growth includes existing Current cash
- **WHEN** the planning baseline moves cash from Current to BOA
- **THEN** it displays opening BOA, opening Current, net new cash flow, the Current-to-BOA transfer, closing BOA, and combined closing cash separately

#### Scenario: Selected month begins the baseline
- **WHEN** August is the selected operating month
- **THEN** the first baseline row is August and the next eleven configured months follow it

#### Scenario: September estimated paycheck affects the baseline
- **WHEN** the September 25 exception remains estimated
- **THEN** the September row visibly identifies the estimated $9,400 combined paycheck assumption

### Requirement: Funding-cycle commitment map
The system SHALL distinguish BOA strategic cash from Current operating cash and SHALL classify Current carryover by the payday cycle whose obligations it funds.

#### Scenario: Open Roadmap crosses into a new day
- **WHEN** the local calendar date changes while the Executive page remains open or the page becomes visible after the date changed
- **THEN** the system refreshes the as-of date, active funding cycle, settled-cycle state, and current operating month without requiring a full-page refresh

#### Scenario: Funding tables exceed available width
- **WHEN** either Roadmap table is wider than its desktop panel
- **THEN** that table scrolls independently while the section heading, explanatory copy, and other table remain aligned and visible

#### Scenario: Roadmap communicates decision-driving assumptions
- **WHEN** the Roadmap displays required BOA support, an estimated paycheck, or a secured floor
- **THEN** required support is visually emphasized, estimates are labeled as planning placeholders, and the secured status explains that the remaining BOA path stays at or above $10,000

#### Scenario: User explains a BOA support shortfall
- **WHEN** the user hovers over or focuses a non-zero BOA support value
- **THEN** the interface shows Current obligations minus Current paycheck equals required BOA support, identifies the largest obligations in that cycle, and explains that BOA covers only the remaining Current gap

#### Scenario: Shortfall preview omits smaller obligations
- **WHEN** a cycle contains more obligations than the shortfall preview displays
- **THEN** the preview shows an `Other cycle obligations` subtotal and provides an expandable complete list whose amounts reconcile to total Current bills

#### Scenario: One-time Verizon catch-up is complete
- **WHEN** Executive builds current and future funding cycles after the historical catch-up payment
- **THEN** it excludes the Verizon catch-up from recurring obligations and includes one regular Verizon bill of $267 due on day 26

#### Scenario: Direct BOA bills remain visible beside Current support
- **GIVEN** the first monthly pay cycle contains the single $376 monthly Chase BOA payment and may contain Current-account obligations
- **WHEN** the funding-cycle map is rendered
- **THEN** Chase is included in the BOA-bills total and identified by name
- **AND** Chase is not included in the BOA-support amount calculated for Current
- **AND** the second monthly pay cycle does not contain another Chase payment

#### Scenario: Food uses the current operating assumption
- **WHEN** recurring Executive obligations are generated
- **THEN** the system includes one $450 Current-account obligation named `Food`
- **AND** it does not create a separate dates obligation

#### Scenario: August 28 funds early September
- **WHEN** the August 28 paycheck cycle is projected
- **THEN** Current obligations through September 10, including early-September rent, are classified as committed to that cycle rather than August surplus

#### Scenario: Current carryover is reported at month end
- **WHEN** Current has a projected balance at month end
- **THEN** the Roadmap displays Current carryover, next-cycle committed Current, and genuinely free Current separately

#### Scenario: Payday cycle crosses month end
- **WHEN** Current cash from September 25 is required for obligations through October 8
- **THEN** the system retains that cash in Current at September month-end and does not sweep it into BOA merely because the calendar month ended

#### Scenario: Funding-cycle table is reviewed
- **WHEN** the Roadmap is active
- **THEN** it shows each upcoming payday cycle's Current deposit, BOA deposit, Current obligations, BOA obligations, required BOA support, and projected Current remainder

#### Scenario: Active cycle is already settled
- **WHEN** the user-entered as-of balances fall inside the July 31 through August 13 cycle
- **THEN** that cycle is labeled settled and contributes zero remaining-cycle impact

### Requirement: Defensible Executive analytics
The system SHALL display only Executive analytics that are calculated from authoritative projection or operating-plan inputs and SHALL not present static scores or synthetic financial trends as measured facts.

#### Scenario: Treasury workspace is reviewed
- **WHEN** Treasury is active
- **THEN** it emphasizes the daily path, inflow versus obligations, floor-goal progress, and planning baseline

#### Scenario: Unsupported metrics have no authoritative input
- **WHEN** CEO score, cash runway, net-worth trend, or capital allocation cannot be derived from maintained inputs
- **THEN** those metrics are omitted rather than estimated silently

### Requirement: Projection-aware operating widgets
The system SHALL keep budget totals non-overlapping, represent automatically settled cycle items without required bill toggles, and collect sufficient account and timing details for projection-changing quick actions.

#### Scenario: Budget groups are totaled
- **WHEN** grouped monthly responsibilities are displayed
- **THEN** each obligation contributes to only one additive summary group

#### Scenario: Cycle-managed ledger entry is settled automatically
- **WHEN** an entry belongs to the active settled funding cycle
- **THEN** the ledger shows its automatic state without requiring an individual paid status selection

#### Scenario: User adds an expense or adjustment
- **WHEN** a quick action is opened
- **THEN** it collects description, amount, account, effective date, and the direction or expense classification needed to update the projection

#### Scenario: Calendar event explains projection impact
- **WHEN** a cash or obligation event is opened
- **THEN** its detail includes account, funding cycle, settlement state, timing confidence, and projected-path impact

### Requirement: Projection-grounded review
The system SHALL supplement weekly reflection prompts with an automatic comparison of opening balance, current or projected close, projected low, unexpected adjustments, and projection change.

#### Scenario: Weekly review is opened
- **WHEN** projection data exists for the selected month
- **THEN** the review shows the financial comparison before the editable reflection prompts
