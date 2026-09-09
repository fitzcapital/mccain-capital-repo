## ADDED Requirements

### Requirement: Specific session synchronization blocker
When required components are not synchronized, the Market Pulse session status SHALL name the
specific blocker rather than displaying generic `Synchronizing` when a blocker is known.

#### Scenario: Gamma alone is stale or unavailable
- **WHEN** canonical execution is locked and Gamma is the only required stale component
- **THEN** the session status displays `Waiting on Gamma`

#### Scenario: Several components are stale
- **WHEN** canonical execution is locked by multiple required components
- **THEN** the session status displays a concise data-synchronization label and the execution card
  lists the components

#### Scenario: Required components align
- **WHEN** the canonical generation contains fresh required components
- **THEN** the session status displays the actual market-session state and no waiting label remains
