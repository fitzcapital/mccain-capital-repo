## ADDED Requirements

### Requirement: Compact dashboard account switching
The dashboard SHALL expose a compact account selector and SHALL recalculate account-scoped metrics
for the selected account.

#### Scenario: User switches accounts
- **WHEN** the user selects another active account in the dashboard header
- **THEN** the dashboard reloads with that account ID and displays only its scoped financial data

#### Scenario: Duplicate broker records exist
- **WHEN** legacy active rows share the same normalized broker account ID
- **THEN** the selector displays one canonical choice for that broker account

### Requirement: Isolated financial demo scenario
The system SHALL support a clearly labeled demo account with August 2026 realized net profit of
$10,000 and a consistency ratio of 35% without changing real account records.

#### Scenario: Demo account is selected
- **WHEN** the August demo account is selected for August 2026
- **THEN** net earned is $10,000 and consistency is 35%

#### Scenario: Real account remains isolated
- **WHEN** the user returns to the Protect account
- **THEN** none of the demo trades contribute to Protect metrics

### Requirement: Readable dashboard performance signals
The dashboard SHALL present positive net earnings, daily realized P&L, and consistency status as
distinct, immediately interpretable signals.

#### Scenario: Recent closed trading days exist
- **WHEN** the selected account has closed trading days in the displayed period
- **THEN** each recent P&L item shows its date and realized dollar amount

#### Scenario: Net earned is positive
- **WHEN** monthly net earned is greater than zero
- **THEN** the value uses a controlled neon-mint glow without reducing numeric legibility

#### Scenario: Consistency exceeds the limit
- **WHEN** the largest winner exceeds 30% of positive net profit
- **THEN** the consistency card is visually elevated and states the amount needed to recover

### Requirement: Custom projection window
The dashboard SHALL allow the user to select any valid start and end date as the basis for the
performance scorecard and projection.

#### Scenario: User applies a custom date range
- **WHEN** the user selects a start date and end date and applies the range
- **THEN** Net Earned, daily P&L, quality, consistency, remaining sessions, required pace, and
  projection use trades and weekdays within that selected window

#### Scenario: User switches accounts with a custom range
- **WHEN** a custom projection window is active and the user switches accounts
- **THEN** the selected start and end dates remain active for the new account

#### Scenario: User clears the custom range
- **WHEN** the user clears the projection window
- **THEN** the scorecard returns to the selected calendar month
