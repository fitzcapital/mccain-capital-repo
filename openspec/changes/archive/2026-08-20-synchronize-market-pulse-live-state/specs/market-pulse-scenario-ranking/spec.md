## MODIFIED Requirements

### Requirement: Canonical in-place scenario refresh
Scenario rankings, confluence components, decision language, checklist state, targets, permission,
and authoritative action state SHALL update together from one validated canonical payload without
reloading the page. No independent component refresh may overwrite execution-authoritative fields.

#### Scenario: Successful scenario transition
- **WHEN** a canonical refresh changes a Local Flip breakdown from alternative to active and all
  required confirmation gates pass
- **THEN** all scenario and primary-decision nodes update together while chart viewport, drawings,
  timeframe, and gamma selection remain unchanged

#### Scenario: Refresh failure
- **WHEN** a scenario refresh fails validation or conflicts with canonical permission or evidence
- **THEN** the last valid generation remains visible with failure and timestamp feedback and no
  partial new scenario fields are committed

### Requirement: Coherent grade and confluence score
The Trade Decision letter grade and numeric confluence score SHALL derive from the same primary
scenario score and SHALL display together with visible grade bands. A legacy page-wide grade MUST
NOT be paired with a different scenario score, and neither grade nor score may override the
authoritative execution state.

#### Scenario: Thirty-point scenario
- **WHEN** the primary scenario earns 30 of 100 confluence points
- **THEN** the Trade Decision displays an F-range grade beside `30/100 confluence`, not an A grade

#### Scenario: High confluence remains unconfirmed
- **WHEN** a scenario earns an A-range score but lacks a required hard trigger
- **THEN** every execution surface remains `WAIT` and explains that confirmation—not score—is
  blocking execution

### Requirement: Setup Replay terminates and ranks honestly
Setup Replay SHALL terminate its loading state with best-to-least ranked results, an explicit empty
state, or a retryable error without blocking the canonical live decision.

#### Scenario: Replay results return
- **WHEN** replay analysis returns eligible historical setups
- **THEN** the page displays them in descending score order with deterministic time tie-breaking

#### Scenario: Replay request stalls or fails
- **WHEN** replay analysis exceeds its timeout or returns an error
- **THEN** the loading message is replaced by a concise unavailable state and a retry control
