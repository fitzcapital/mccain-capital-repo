## MODIFIED Requirements

### Requirement: Setup Replay terminates and ranks honestly
Setup Replay SHALL terminate its loading state with best-to-least ranked results, an explicit empty
state, or a retryable error without blocking the canonical live decision. The Replay surface SHALL
provide a concise navigation path to the dedicated Setup Analytics page without embedding dense
historical charts in the execution workspace.

#### Scenario: Replay results return
- **WHEN** replay analysis returns eligible historical setups
- **THEN** the page displays them in descending score order with deterministic time tie-breaking

#### Scenario: Replay request stalls or fails
- **WHEN** replay analysis exceeds its timeout or returns an error
- **THEN** the loading message is replaced by a concise unavailable state and a retry control

#### Scenario: Trader opens setup analytics
- **WHEN** the trader selects the analytics link from Setup Replay
- **THEN** the dedicated Setup Analytics page opens without changing Live Setup state or alert behavior
