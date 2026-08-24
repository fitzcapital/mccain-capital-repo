## ADDED Requirements

### Requirement: Evaluation phase models the documented profit target
The planner SHALL calculate the Basic Options evaluation target as 10% of plan size and SHALL show the remaining profit and estimated weekday date to reach it.

#### Scenario: Fifty-thousand-dollar evaluation
- **WHEN** the plan size is $50,000 and the account is in evaluation
- **THEN** the target profit is $5,000 and the passing balance is $55,000

### Requirement: Performance phase models buffer and loss-limit state
The planner SHALL accept a performance buffer, current loss limit, and fixed loss limit and SHALL identify whether the buffer has been reached and which loss limit applies.

#### Scenario: Buffer not reached
- **WHEN** current balance is below the configured performance buffer
- **THEN** the planner labels the account pre-buffer and uses the current user-entered loss limit for payout-risk calculations

#### Scenario: Buffer reached
- **WHEN** current balance is at or above the configured performance buffer
- **THEN** the planner labels the loss limit fixed and uses the configured fixed loss limit

### Requirement: Payout capacity distinguishes safety levels
The planner MUST show theoretical payout capacity, protected payout capacity after a user-selected cushion, post-payout balance, and remaining distance above the applicable loss limit.

#### Scenario: Protected payout
- **WHEN** a proposed payout leaves the account strictly above the applicable loss limit plus safety cushion
- **THEN** the payout is labeled within the protected plan

#### Scenario: Unsafe payout
- **WHEN** a proposed payout reaches or crosses the protected floor
- **THEN** the planner displays an unsafe warning and the projected cushion after payout

### Requirement: Trading profit and payout income remain distinct
The planner SHALL use average net trading-day profit for account milestone progress and SHALL apply tax estimates only to withdrawn payout income.

#### Scenario: Evaluation progress
- **WHEN** tax inputs are changed during evaluation planning
- **THEN** the evaluation target date and broker-account trajectory remain unchanged

### Requirement: Lifecycle milestones are visualized
The planner SHALL display a projected balance path from the current balance through the selected end date and SHALL label only phase-relevant qualification, buffer, and payout-readiness milestones.

#### Scenario: Performance account chart
- **WHEN** performance mode is selected
- **THEN** the chart identifies the buffer, applicable loss limit, protected payout floor, current balance, and projected balance

### Requirement: Planner produces a strategic payout decision
The planner SHALL return one primary recommendation that states whether to continue, take a protected payout now, or wait for a specified balance and estimated date.

#### Scenario: Desired payout is protected now
- **WHEN** protected payout capacity is at least the desired payout
- **THEN** the planner recommends taking the desired payout and shows the resulting balance and remaining cushion

#### Scenario: Desired payout requires waiting
- **WHEN** protected payout capacity is below the desired payout and daily profit is positive
- **THEN** the planner recommends waiting and shows the balance, additional profit, trading sessions, estimated date, and post-payout balance required to support that payout

#### Scenario: Desired payout is planned before the buffer
- **WHEN** the current performance balance is below the buffer and a desired payout is entered
- **THEN** the required balance is the greater of the buffer and the fixed protected floor plus the desired payout

#### Scenario: Evaluation remains incomplete
- **WHEN** the projected evaluation balance remains below the passing target
- **THEN** the planner recommends continuing and shows the remaining profit and estimated passing date

### Requirement: Scenario dates support account progression
The planner SHALL estimate lifecycle milestone dates at conservative, expected, and stretch daily-profit pace.

#### Scenario: Evaluation scenarios
- **WHEN** evaluation mode has positive daily profit
- **THEN** each scenario shows an estimated evaluation pass date

#### Scenario: Performance scenarios
- **WHEN** performance mode has positive daily profit
- **THEN** each scenario shows estimated buffer and protected payout readiness dates when reachable

### Requirement: Undocumented rule inputs remain explicit
The system MUST NOT infer performance buffer or fixed loss-limit rules for plan sizes not established by configured or documented values.

#### Scenario: Non-default plan size
- **WHEN** a plan size other than the documented $50,000 example is selected
- **THEN** the planner requires or visibly retains editable buffer and loss-limit values and identifies them as planning inputs
