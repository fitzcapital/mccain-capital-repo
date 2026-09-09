## ADDED Requirements

### Requirement: Bounded Gamma refresh resources
Gamma background and forced refreshes SHALL remain bounded in thread and task usage. Concurrent
callers MUST coalesce around one process-local refresh and one cross-process coordinated refresh,
and option-chain expiry fetching MUST NOT create an unbounded or per-request worker population.

#### Scenario: Background refresh overlaps forced refresh
- **WHEN** a forced Gamma refresh arrives while the process background refresh is running
- **THEN** the caller receives current last-good state with refresh-in-progress status and no second
  process-local refresh starts

#### Scenario: Repeated refresh soak
- **WHEN** Gamma refresh is invoked repeatedly through the configured soak count
- **THEN** process thread count returns to a bounded baseline and does not grow with invocation count

#### Scenario: Multiple Gunicorn workers refresh
- **WHEN** separate web workers request Gamma refresh at the same time
- **THEN** the shared lock permits one provider computation and other workers consume the shared
  snapshot

### Requirement: Worker resource containment
The application runtime SHALL cap native numerical-library thread counts before application imports
and SHALL gracefully recycle Gunicorn workers after a configurable bounded request count with
jitter.

#### Scenario: Numerical libraries initialize
- **WHEN** the container starts the application
- **THEN** supported BLAS, OpenMP, MKL, and NumExpr libraries are limited to the configured thread cap

#### Scenario: Worker reaches request limit
- **WHEN** a Gunicorn worker reaches its configured request count
- **THEN** Gunicorn gracefully replaces it while another worker continues serving and shared Gamma
  state remains available

### Requirement: Resource pressure diagnostics
Operational health SHALL expose current-process thread count, warning threshold, pressure state, and
the latest Gamma refresh attempt/error without requiring a new Gamma refresh.

#### Scenario: Thread pressure approaches limit
- **WHEN** current-process thread count reaches the configured warning threshold
- **THEN** operational health reports degraded thread pressure before task exhaustion

#### Scenario: Gamma refresh cannot start
- **WHEN** a resource error prevents Gamma refresh work from starting
- **THEN** health reports the failure reason, execution remains locked, and last-good Gamma is
  preserved without being labeled current

### Requirement: Local monitoring events
The local monitoring integration SHALL publish sustained Gamma staleness/failure and worker thread
pressure as Netdata warning or critical alarm transitions. Recovery SHALL produce a resolved event,
and unchanged state MUST NOT create duplicate events on each collection cycle.

#### Scenario: Gamma becomes stale or refresh fails
- **WHEN** Gamma freshness or refresh health breaches its configured sustained threshold
- **THEN** Netdata records a timestamped event identifying Gamma, its current state, threshold, and
  concise failure cause

#### Scenario: Worker pressure approaches exhaustion
- **WHEN** worker thread pressure crosses a configured warning or critical threshold
- **THEN** Netdata records the severity transition before the container task limit is exhausted

#### Scenario: Monitored condition recovers
- **WHEN** Gamma health or worker pressure returns inside its recovery threshold
- **THEN** Netdata records one resolved transition and does not repeat it while state remains healthy
