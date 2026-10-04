## ADDED Requirements

### Requirement: Bounded Market Pulse worker tasks
The dedicated Market Pulse worker SHALL keep process task and file-descriptor growth bounded across
repeated refresh cycles and SHALL expose sanitized capacity diagnostics.

#### Scenario: Repeated refresh cycles complete normally
- **WHEN** each enabled worker refresh path runs repeatedly across the configured soak interval
- **THEN** process task and file-descriptor counts return within the documented steady-state bound
  and Market Pulse snapshots continue advancing

#### Scenario: A provider refresh fails
- **WHEN** a provider, browser, stream, or network refresh raises an exception or times out
- **THEN** the worker releases resources owned by that attempt, preserves the last-good snapshot,
  and reports a sanitized component failure

### Requirement: Capacity-aware worker recovery
The dedicated Market Pulse worker SHALL detect sustained critical task usage below its cgroup PID
ceiling and SHALL terminate cleanly so Kubernetes can replace only that worker before process
creation is exhausted.

#### Scenario: Worker task usage remains critically high
- **WHEN** task usage exceeds the configured safety threshold for the configured consecutive samples
- **THEN** the worker records a sanitized capacity failure, stops presenting a healthy heartbeat,
  exits nonzero, and Kubernetes replaces only the worker pod

#### Scenario: Worker task usage briefly spikes
- **WHEN** task usage crosses the warning or critical threshold for fewer than the configured
  consecutive samples and then returns to normal
- **THEN** the worker records the high-water mark and continues without restarting

#### Scenario: Replacement worker starts
- **WHEN** Kubernetes replaces a capacity-failed worker
- **THEN** persistent data and last-good Market Pulse snapshots remain available and chart, gamma,
  target, and setup timestamps resume advancing

### Requirement: Capacity-aware worker health reporting
Worker health and local status tooling SHALL distinguish heartbeat freshness from safe remaining
process capacity.

#### Scenario: Heartbeat is current but task capacity is unsafe
- **WHEN** the heartbeat age is within its normal limit but the latest task sample is critical
- **THEN** worker health reports an unhealthy capacity state rather than healthy

#### Scenario: Worker capacity is healthy
- **WHEN** the heartbeat is current and task usage remains below the warning threshold
- **THEN** worker health reports the current task count, ceiling, and high-water mark as healthy
