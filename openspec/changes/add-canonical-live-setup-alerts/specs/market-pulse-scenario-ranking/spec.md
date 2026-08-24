## ADDED Requirements

### Requirement: Lifecycle-aware primary setup ranking
The system SHALL select the primary live setup by lifecycle actionability before existing scenario
lane and confluence ordering. Terminal or late review-only setups MUST NOT displace a current
confirmed, armed, or watching setup.

#### Scenario: Confirmed setup and closer watch coexist
- **WHEN** a confirmed setup and a numerically closer watching setup coexist
- **THEN** the confirmed setup is primary and the watching setup remains in the collapsed watch list

#### Scenario: Primary setup terminates
- **WHEN** the primary setup becomes invalidated, reaches target, or expires and another non-terminal candidate exists
- **THEN** the next candidate is promoted deterministically without copying action fields from the terminal setup

### Requirement: Late-day setup treatment
The scenario ranking payload SHALL preserve after-cutoff candidates for context and Replay while
marking them non-actionable for live alerting.

#### Scenario: Candidate arms before cutoff and confirms after cutoff
- **WHEN** a setup becomes armed before the cutoff but first confirms after it
- **THEN** ranking identifies the setup as late review-only and alert eligibility remains false

#### Scenario: Pre-cutoff confirmation remains active
- **WHEN** a setup confirms before the cutoff and remains structurally valid afterward
- **THEN** its lifecycle may remain visible after the cutoff without generating a second alert
