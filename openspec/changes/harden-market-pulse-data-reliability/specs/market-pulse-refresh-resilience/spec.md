## ADDED Requirements

### Requirement: Candidate generations are validated before publication
The system SHALL validate the SPX symbol, active market session, source timestamps, required
component freshness, and derived-state coherence before publishing a Market Pulse generation.

#### Scenario: Candidate is fully valid
- **WHEN** spot, completed candles, gamma, and derived guidance pass validation for one session
- **THEN** the system atomically publishes the candidate with one monotonic generation id

#### Scenario: One required component fails validation
- **WHEN** any required component is missing, stale, future-dated, or scoped to another session
- **THEN** the candidate is rejected, the prior verified generation remains primary, and execution
  is locked with the failing component named

### Requirement: Polling is single-flight and self-recovering
The client SHALL run at most one canonical refresh request at a time and SHALL recover from bounded
timeouts, unchanged generations, tab suspension, network restoration, and transient server errors
without requiring a full-page reload.

#### Scenario: Scheduled poll overlaps active request
- **WHEN** the next scheduled poll occurs while a canonical request is active
- **THEN** the coordinator does not start a second request and schedules the next attempt from the
  resolved active flight

#### Scenario: Transient refresh fails
- **WHEN** a request times out or receives a retryable server error
- **THEN** the page retains the last verified generation, clears the busy state, displays the retry
  state, and attempts again using bounded backoff

#### Scenario: Hidden page becomes visible
- **WHEN** the page returns to visibility after its expected poll time
- **THEN** the coordinator immediately revalidates the canonical generation before resuming cadence

### Requirement: Refresh timing is truthful
The page SHALL derive its refresh countdown from the coordinator's actual next-attempt timestamp and
SHALL distinguish a scheduled refresh from retrying, delayed, refreshing, and locked states.

#### Scenario: Countdown reaches zero
- **WHEN** the displayed countdown reaches zero and no request is active
- **THEN** the coordinator starts a refresh or immediately displays the server-directed delay

#### Scenario: Backoff changes the schedule
- **WHEN** a failed request changes the next-attempt time
- **THEN** the visible countdown updates from that new time and does not imply a successful refresh

### Requirement: Worker responses converge on the newest verified generation
Every application worker SHALL compare its local snapshot with the shared verified envelope before
serving Market Pulse context and SHALL never replace a newer verified generation with an older one.

#### Scenario: Request reaches an older worker
- **WHEN** a worker's memory cache is older than the shared verified envelope
- **THEN** the worker adopts and serves the shared newer generation

#### Scenario: Concurrent candidate completes late
- **WHEN** an older candidate finishes after a newer generation has been promoted
- **THEN** the older candidate is discarded and cannot regress the shared envelope

### Requirement: Reliability diagnostics are observable
The system SHALL provide local, structured diagnostics for refresh outcome, duration, generation,
component age, retry reason, and execution blocker without exposing credentials or raw provider
payloads.

#### Scenario: Execution is locked by stale gamma
- **WHEN** gamma exceeds its freshness threshold
- **THEN** diagnostics and the page identify gamma, its age, the last verified time, and the next
  recovery attempt

#### Scenario: Refresh succeeds
- **WHEN** a newer candidate is promoted
- **THEN** diagnostics record one success for that generation and the page advances last-valid time
  exactly once
