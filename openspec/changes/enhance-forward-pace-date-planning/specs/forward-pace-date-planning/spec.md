## ADDED Requirements

### Requirement: User can select a projection horizon by dates
The standalone Forward Pace planner SHALL support an explicit start date and target date and SHALL derive projection totals from the inclusive weekday sessions within that window.

#### Scenario: Complete and partial weeks
- **WHEN** a valid date window contains complete or partial Monday-Friday weeks
- **THEN** the system groups output by calendar week and prorates each partial week using the weekdays included in the selected window

#### Scenario: Weeks mode compatibility
- **WHEN** a request uses Projection Weeks or omits the new horizon mode
- **THEN** the system preserves weeks-based behavior and derives a corresponding projection end date

#### Scenario: Reversed date window
- **WHEN** the target date precedes the start date
- **THEN** the system rejects the projection with a clear validation error and retains the last valid rendered result

### Requirement: Date assumptions are visible and reconcilable
The planner MUST return and display the calendar days, inclusive weekday sessions, equivalent trading weeks, selected start and target dates, and timing assumption used by the projection.

#### Scenario: Date-window summary
- **WHEN** a date-based projection succeeds
- **THEN** the page and PDF identify the selected dates, calendar duration, weekday-session count, and equivalent five-session weeks

#### Scenario: Market-holiday limitation
- **WHEN** weekday sessions are displayed
- **THEN** the planner states that weekends are excluded and market holidays are not excluded

### Requirement: Optional balance target produces required pace
The planner SHALL calculate the required net daily pace and required net weekly pace for an optional target balance and SHALL compare that requirement with modeled net pace.

#### Scenario: Target above base balance
- **WHEN** the target balance is greater than the base balance and the date window contains weekdays
- **THEN** the planner returns the remaining target amount, required pace per weekday, required pace per five-session week, and an ahead, on-track, or behind status

#### Scenario: Missing or already reached target
- **WHEN** no target is supplied or the target does not exceed the base balance
- **THEN** the planner returns a neutral target state without inventing a required pace

### Requirement: Scenario comparison uses the same date window
The planner SHALL compare conservative, base, and stretch outcomes for the same selected sessions using disclosed net-pace multipliers.

#### Scenario: Three scenario outcomes
- **WHEN** a projection succeeds
- **THEN** the system returns and displays projected profit and ending balance at 75%, 100%, and 125% of the modeled net pace

#### Scenario: Scenario reconciliation
- **WHEN** the base scenario is displayed
- **THEN** its ending balance equals the primary projected balance for the same window

### Requirement: Projection output remains server-authoritative
The page, schedule, target analysis, scenario comparison, and PDF export MUST use the same server-side projection result and financial assumptions.

#### Scenario: Input changes
- **WHEN** the user changes a horizon, cashflow, buffer, tax, or target input
- **THEN** the page marks those inputs as unapplied and retains the currently displayed projection

#### Scenario: Apply projection changes
- **WHEN** the user selects Update Projection with valid inputs
- **THEN** the browser requests one fresh server calculation, updates all dependent sections without a full-page reload, and identifies when the projection was updated

#### Scenario: Invalid response
- **WHEN** the server rejects an input or the request fails
- **THEN** the page keeps the last valid projection visible and displays a concise error near the projection controls

### Requirement: Projection controls have a clear modern hierarchy
The planner SHALL group related inputs by planning purpose and SHALL present Update Projection as the primary action for applying draft changes.

#### Scenario: Planner input groups
- **WHEN** the page renders
- **THEN** Account, Projection Window, and Tax & Reserve controls are visibly grouped and the primary update action is available without searching the result area

#### Scenario: Pending draft
- **WHEN** a previously rendered input changes
- **THEN** the update state indicates that changes have not yet been applied and the primary action is enabled

### Requirement: Planner visualizes goal feasibility
The planner SHALL visualize projected balance against the target, current pace against required pace, and scenario outcomes using values from the server-authoritative projection.

#### Scenario: Target configured
- **WHEN** a projection includes a target balance
- **THEN** the page displays a surplus or shortfall verdict, a balance-versus-target chart, and a current-versus-required weekly pace chart

#### Scenario: No target configured
- **WHEN** a projection has no actionable target balance
- **THEN** the verdict prompts for a target and target-dependent chart elements remain neutral rather than inventing a goal

### Requirement: Scenario target dates are estimated
The system SHALL estimate a weekday-based completion date for each scenario when its net pace is positive and a target balance is configured.

#### Scenario: Reachable scenario target
- **WHEN** a scenario has positive daily net pace and the target exceeds the starting balance
- **THEN** its card shows the estimated weekday on which cumulative scenario growth reaches the target

### Requirement: Detailed calculations use progressive disclosure
The planner SHALL keep tax assumptions and the complete projection schedule accessible while collapsing them by default.

#### Scenario: Initial page view
- **WHEN** the projection first renders
- **THEN** the verdict and charts appear before collapsed assumptions and schedule details
