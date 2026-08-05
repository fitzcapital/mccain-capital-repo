## ADDED Requirements

### Requirement: Every normal sync attempts equity refresh
The system SHALL attempt to resolve Broker Equity for the selected account during every completed
normal Live Sync, including runs with zero new fills, and SHALL record the equity-refresh outcome
separately from the trade-import outcome.

#### Scenario: Trades import and equity refresh succeed
- **WHEN** a normal sync imports broker fills and obtains valid account-matched broker equity
- **THEN** the system records both import success and equity-refresh success with the accepted value,
  source, and timestamp

#### Scenario: No new fills are returned
- **WHEN** a normal sync completes with no new fills
- **THEN** the system still attempts equity refresh and records its independent outcome

#### Scenario: Trade import succeeds but equity refresh fails
- **WHEN** fills import successfully but no trusted equity source is available
- **THEN** the import remains successful and the equity-refresh outcome records failure or preserved
  fallback with an actionable reason

### Requirement: Automatic equity follows deterministic source priority
The system MUST accept Broker Equity from an account-matched authenticated broker metrics response
before a trustworthy statement ending balance and MUST NOT label ledger-derived balance as broker
equity.

#### Scenario: Broker dashboard equity is available
- **WHEN** the authenticated broker metrics response matches the selected account and contains valid
  equity
- **THEN** the system stores that value with source `broker_dashboard` even when a statement ending
  balance is also available

#### Scenario: Broker dashboard is unavailable but statement balance is trusted
- **WHEN** broker metrics are unavailable and the selected account's statement has a valid ending
  balance from the requested date range
- **THEN** the system stores the statement value with source `statement`

#### Scenario: Statement date range fell back
- **WHEN** the broker did not apply the requested statement date range
- **THEN** the system treats its ending balance as untrusted and does not use it to update Broker
  Equity

#### Scenario: Only ledger balance is available
- **WHEN** broker metrics and a trusted statement ending balance are unavailable but ledger balance
  exists
- **THEN** the system does not copy ledger balance into Broker Equity

### Requirement: Failed and partial refreshes preserve financial values
The system MUST preserve the last valid Broker Equity and all unrelated broker risk metrics when an
automated refresh fails, is unauthenticated, is account-mismatched, or omits individual fields.

#### Scenario: Broker authentication is required
- **WHEN** the broker metrics request redirects to login or signup
- **THEN** the system preserves stored equity and drawdown and records `auth_required` as the refresh
  reason

#### Scenario: Partial response contains equity only
- **WHEN** a valid account-matched metrics response contains equity but omits remaining drawdown and
  max loss
- **THEN** the system updates equity and preserves the stored drawdown and max-loss values

#### Scenario: Partial response omits equity
- **WHEN** a metrics response contains another broker metric but no valid equity
- **THEN** the system does not clear or replace stored Broker Equity

#### Scenario: No prior equity exists
- **WHEN** all automated equity sources fail and the selected account has no stored equity
- **THEN** Broker Equity remains missing and the system records the precise recovery reason

### Requirement: User can update Broker Equity manually from Live Sync
The system SHALL provide a manual Broker Equity editor for the selected account on the Live Sync
review/reference surface and SHALL apply the same validation and peak-preservation rules used by the
Dashboard manual-equity workflow.

#### Scenario: Valid manual equity is saved
- **WHEN** the user submits a finite non-negative amount for the selected account
- **THEN** the system stores the value, advances peak when appropriate, preserves other risk metrics,
  marks the source `manual`, updates the timestamp, and returns to the same Live Sync account context

#### Scenario: Invalid manual equity is rejected
- **WHEN** the user submits an empty, negative, non-numeric, or non-finite amount
- **THEN** the system changes no account metric and displays a validation message on Live Sync

#### Scenario: Account context is invalid
- **WHEN** the manual request references a missing, archived, or unselected account
- **THEN** the system changes no financial value and displays an account-selection error

### Requirement: Broker Equity status explains source and recovery
The Live Sync Broker Equity card MUST display the selected account's stored value when present,
identify its source and timestamp, and distinguish the latest automatic refresh failure from the
stored value's validity.

#### Scenario: Stored manual value survives failed automation
- **WHEN** a manual equity value exists and the latest automated refresh requires broker
  authentication
- **THEN** the card continues displaying the manual value and timestamp while showing a separate
  authentication-required warning and refresh guidance

#### Scenario: Broker value is current
- **WHEN** the latest accepted value came from account-matched broker metrics
- **THEN** the card labels the value `Broker dashboard` and displays the accepted timestamp

#### Scenario: Statement value is current
- **WHEN** the latest accepted value came from a trustworthy statement ending balance
- **THEN** the card labels the value `Statement` and displays the accepted timestamp

#### Scenario: Equity is missing
- **WHEN** the selected account has no stored Broker Equity
- **THEN** the card displays the exact missing reason and a visible manual update action instead of
  only `Ledger fallback active`

### Requirement: Equity actions remain account-scoped and auditable
The system MUST apply automatic and manual equity updates only to the selected account whose local
broker identifier matches the sync context and MUST expose the accepted source in job or audit data.

#### Scenario: Broker account mismatch
- **WHEN** the requested broker identifier does not match the selected local account
- **THEN** the system rejects the equity update without changing either account

#### Scenario: Successful automatic update is recorded
- **WHEN** an automatic source updates Broker Equity
- **THEN** the job summary records the selected account, accepted source, value, and timestamp without
  exposing broker credentials

#### Scenario: Successful manual update is recorded
- **WHEN** the user saves manual Broker Equity
- **THEN** the application records the selected account, source `manual`, value, and timestamp through
  the existing local audit mechanism without storing credentials

### Requirement: Ledger Equity is the primary operational value
The system SHALL calculate Ledger Equity for the selected account as its opening balance plus
recorded net realized trade P&L and SHALL display it as the single primary equity value on Dashboard
and Live Sync.

#### Scenario: Broker Equity is unavailable
- **WHEN** the account has an opening balance and recorded trading transactions but no stored Broker
  Equity
- **THEN** the receiving surface displays Ledger Equity with the opening balance and net-realized-P&L
  components without a competing Broker Equity missing warning

#### Scenario: Broker Equity is available
- **WHEN** both stored Broker Equity and Estimated Ledger Equity are available
- **THEN** the primary receiving surface continues to display Ledger Equity while keeping the broker
  value and its provenance available only to advanced sync diagnostics
