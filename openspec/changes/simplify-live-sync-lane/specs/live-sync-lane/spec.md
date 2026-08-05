## ADDED Requirements

### Requirement: Canonical Live Sync outcome
The system SHALL derive one canonical Live Sync outcome from authoritative job and import facts and
SHALL use that outcome on both the Dashboard and full Live Sync lane.

#### Scenario: Cancelled attempt is represented consistently
- **WHEN** the latest Live Sync attempt was cancelled today
- **THEN** both surfaces show a cancelled outcome and neither surface labels the import completed

#### Scenario: Active job is represented consistently
- **WHEN** a Live Sync job is currently active
- **THEN** both surfaces show the running outcome and identify the same active job

#### Scenario: Unknown mapping fails safely
- **WHEN** the latest persisted status cannot be mapped to a supported outcome
- **THEN** the system shows an unavailable or recovery-required state and does not claim completion

### Requirement: Attempts and successful imports remain distinct
The system SHALL track and present the last attempt separately from the last successful import.
Only a completed normal import or valid no-new-trades import SHALL satisfy daily import completion.

#### Scenario: Diagnostic run finishes
- **WHEN** a diagnostic-only run completes today without importing trades
- **THEN** attempted-today is true and import-completed-today is false

#### Scenario: No new broker fills
- **WHEN** a normal import completes successfully and the broker returns no new fills
- **THEN** the outcome is no-new-trades and import-completed-today is true

#### Scenario: Failed run follows an earlier success
- **WHEN** the latest attempt failed after an earlier successful import
- **THEN** the system shows the failed latest attempt and preserves the earlier successful-import timestamp

### Requirement: Dashboard sync uses safe current defaults
The Dashboard `Sync Today` action SHALL build a new normal-import request for today's Eastern Time
date and SHALL NOT silently inherit a historical date range or diagnostic-only intent from a saved
request.

#### Scenario: Previous request used historical dates
- **WHEN** the saved request contains dates before today and the operator opens Dashboard sync
- **THEN** the preflight and submitted request use today's Eastern Time date

#### Scenario: Previous request was diagnostic-only
- **WHEN** the saved request has diagnostic-only enabled and the operator starts Dashboard sync
- **THEN** the submitted request has diagnostic-only disabled and is labeled as a normal import

#### Scenario: Operator needs a historical import
- **WHEN** the operator needs a date other than today
- **THEN** the Dashboard directs the operator to the full Live Sync lane for an explicit historical request

### Requirement: Sync account identity is validated before execution
The system MUST verify that the selected local ledger's configured broker account identifier matches
the broker account identifier in the request before creating a job or opening a broker session.

#### Scenario: Ledger and broker account match
- **WHEN** the selected ledger has a configured broker identifier that matches the request
- **THEN** account validation passes and the request may proceed

#### Scenario: Ledger and broker account differ
- **WHEN** the selected ledger broker identifier differs from the request broker identifier
- **THEN** the system rejects the request, creates no job, and explains how to correct the account selection

#### Scenario: Broker account is missing
- **WHEN** the selected ledger does not have a configured broker identifier
- **THEN** the system disables sync and links the operator to account configuration

### Requirement: Preflight intent is visible
Before a manual sync can start, the system SHALL display the selected ledger, masked broker account,
date window, sync mode, and whether the run will import data.

#### Scenario: Dashboard preflight is ready
- **WHEN** credentials and account validation are ready for today's normal import
- **THEN** the Dashboard displays all preflight fields and enables `Sync Today`

#### Scenario: Preflight is incomplete
- **WHEN** credentials or account validation are not ready
- **THEN** the action is disabled and the specific blocking reason is displayed

### Requirement: Normal and recovery workflows use progressive disclosure
The desktop Live Sync lane SHALL use exactly three top-level visual regions: a compact canonical
status strip, one primary Sync Today workspace, and one Advanced & Recovery drawer. It SHALL keep
diagnostic, historical, account-editing, credential-editing, artifact, HTML recovery, reliability,
automation configuration, and reset controls inside the drawer or state-triggered recovery UI.

#### Scenario: Lane is ready for normal operation
- **WHEN** the canonical outcome is ready or completed and no recovery action is required
- **THEN** the normal preflight and primary sync action are prominent, the Advanced & Recovery
  drawer remains collapsed, and no peer Operator Deck, Run Feedback, Failure Guide, or Tools status
  cards repeat the canonical status

#### Scenario: Recovery is required
- **WHEN** the canonical outcome is needs-recovery
- **THEN** the Advanced & Recovery drawer opens with relevant guidance and job evidence while the
  primary Sync Today workspace remains visible

#### Scenario: Diagnostic mode is selected
- **WHEN** the operator explicitly selects a diagnostic test in Advanced
- **THEN** the UI states that no import will occur before the test starts

#### Scenario: Page hierarchy remains compact
- **WHEN** the desktop Live Sync lane renders at its normal application width
- **THEN** status is shown once, the primary action is visible without scanning multiple status
  cards, and all secondary operational controls are grouped under one drawer

### Requirement: Automation timing and manual need are explicit
The system SHALL show whether after-market automation is enabled, its next scheduled Eastern Time run,
its latest attempt, and whether a successful import is still needed today.

#### Scenario: Automation has not run today
- **WHEN** after-market automation is enabled and no successful import exists today
- **THEN** the UI shows the next scheduled run and identifies that today's import remains pending

#### Scenario: Automation was cancelled
- **WHEN** today's automated attempt was cancelled
- **THEN** the UI shows the cancellation and continues to identify today's import as pending

#### Scenario: Import already completed today
- **WHEN** a successful import or no-new-trades result exists today
- **THEN** the UI identifies today's import as complete and avoids implying that another manual run is required

### Requirement: Reliability reflects outcome semantics
The system SHALL report completed imports, failures, cancellations, diagnostic-only runs, and unknown
legacy results as distinct reliability categories.

#### Scenario: Window contains only a cancellation
- **WHEN** the reliability window contains one cancelled run and no import attempts
- **THEN** the UI reports one cancellation and does not describe the result as a zero-percent import success rate

#### Scenario: Window contains mixed results
- **WHEN** the reliability window contains successful imports, failures, cancellations, and diagnostic runs
- **THEN** the UI reports the denominator and category counts used for the success calculation

### Requirement: Monitoring remains passive
Monitoring consumers SHALL be allowed to read canonical Live Sync health but MUST NOT start, cancel,
recover, or mutate a sync.

#### Scenario: Monitoring reads sync health
- **WHEN** a monitoring endpoint requests Live Sync status
- **THEN** it receives redacted canonical health data without changing job or broker state

### Requirement: Dashboard sync remains a compact readiness control
The Dashboard SHALL present Live Sync as one compact permission-to-trade readiness item and SHALL
reserve detailed preflight, history, diagnostics, account scope, and recovery controls for the full
Live Sync lane.

#### Scenario: Dashboard import remains pending
- **WHEN** today's normal import has not completed
- **THEN** the checklist shows the canonical outcome, a concise reason, one `Sync Today` action,
  and a link to full details without rendering a second multi-card sync workspace

#### Scenario: Dashboard import is complete
- **WHEN** today's normal import completed or returned no new trades
- **THEN** the readiness item is visibly complete and does not imply another manual run is needed

#### Scenario: Dashboard sync needs recovery
- **WHEN** the canonical state requires diagnostics or recovery
- **THEN** the checklist shows the blocking state and directs the operator to the full Live Sync
  lane rather than exposing recovery controls inside the Dashboard
