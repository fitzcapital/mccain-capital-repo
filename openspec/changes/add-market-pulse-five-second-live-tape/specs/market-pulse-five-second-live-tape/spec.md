## ADDED Requirements

### Requirement: Five-second live tape
The Market Pulse hero chart SHALL derive a bounded five-second SPX price trace from accepted timestamped responses on the existing live quote polling path and SHALL NOT create an additional market-data request.

#### Scenario: Fresh live quote sample
- **WHEN** the regular session is live and a quote has a valid price and fresh provider timestamp
- **THEN** the system adds or replaces the corresponding five-second bucket and renders the bounded trace

#### Scenario: Multiple samples in one bucket
- **WHEN** more than one accepted quote belongs to the same five-second bucket
- **THEN** the system replaces that bucket value rather than appending duplicate timestamps

#### Scenario: Bounded long-running page
- **WHEN** accepted samples exceed the configured maximum point count
- **THEN** the system discards the oldest points and retains only the configured bound

### Requirement: Honest tape continuity
The five-second tape SHALL expose paused, closed, unavailable, and interrupted states and SHALL NOT synthesize missing samples.

#### Scenario: Feed gap
- **WHEN** the interval between accepted provider timestamps exceeds the configured gap threshold
- **THEN** the system inserts a visible series break, marks the tape interrupted, and does not interpolate prices

#### Scenario: Hidden page
- **WHEN** the document is hidden
- **THEN** the tape is marked paused and hidden-period samples do not extend the trace

#### Scenario: Closed session
- **WHEN** the canonical market phase is closed
- **THEN** the tape is marked closed and no new five-second point is accepted

#### Scenario: Fresh sample after interruption
- **WHEN** a fresh timestamped quote arrives after an interruption while the page and session are active
- **THEN** the tape resumes from the new sample without backfilling the missing interval

### Requirement: Clear visual hierarchy
The chart SHALL identify the five-second trace as `visual only`, offer a compact visibility toggle, and distinguish it from authoritative candle evidence.

#### Scenario: User inspects live tape
- **WHEN** the five-second trace is visible
- **THEN** the chart shows its cadence, state, and `visual only` label near the chart controls

#### Scenario: User hides live tape
- **WHEN** the user disables the five-second toggle
- **THEN** the trace is hidden while quote polling and authoritative candle processing continue unchanged
