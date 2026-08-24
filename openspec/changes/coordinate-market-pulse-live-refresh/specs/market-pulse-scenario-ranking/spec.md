## MODIFIED Requirements

### Requirement: Setup Replay terminates and ranks honestly
Setup Replay SHALL terminate its loading state with best-to-least ranked results, an explicit empty
state, or a retryable error without blocking the canonical live decision. Replay setups rendered on
the chart MUST use a distinct historical replay marker and MUST NOT be labeled or styled as a live
entry, liquidity sweep, or canonical confirmation.

#### Scenario: Replay results return
- **WHEN** replay analysis returns eligible historical setups
- **THEN** the panel displays them in descending score order with deterministic time tie-breaking and
  the chart places distinct replay markers at their historical candle times

#### Scenario: Replay marker is inspected
- **WHEN** the user inspects a replay marker or its legend
- **THEN** the page identifies it as `Replay setup` and states that it is historical and not a live
  entry

#### Scenario: Replay request stalls or fails
- **WHEN** replay analysis exceeds its timeout or returns an error
- **THEN** the loading message is replaced by a concise unavailable state and a retry control
