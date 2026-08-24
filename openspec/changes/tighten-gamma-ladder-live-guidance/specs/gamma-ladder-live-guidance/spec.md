## ADDED Requirements

### Requirement: Immediate Gamma command sequence
For a coherent current SPX ladder, the system SHALL present a compact `Now`, `Confirm`, `Target`, and `Fail` sequence using accepted level values and SHALL avoid generic execution language when exact levels are available.

#### Scenario: SPX is testing a nearby boundary
- **WHEN** a current coherent SPX snapshot has a relevant level nearest spot
- **THEN** the command sequence identifies that level as `Now` and names exact confirmation, target, and failure levels

#### Scenario: Guidance cannot be trusted
- **WHEN** the ladder is stale, incoherent, or not aligned to the canonical generation
- **THEN** live command wording is replaced by a synchronization or locked state without discarding the last valid map

#### Scenario: Canonical execution is locked
- **WHEN** the canonical Market Pulse generation denies permission or reports a locked action state
- **THEN** exact Gamma commands are suppressed even when the ladder component itself is current and aligned

#### Scenario: Source freshness is explicitly stale
- **WHEN** the Gamma source freshness explicitly reports stale or unavailable during a live session
- **THEN** session state does not override freshness and exact Gamma commands remain suppressed

### Requirement: SPX-only execution scope
The Gamma Ladder SHALL distinguish SPX execution guidance from non-SPX reference data.

#### Scenario: SPX ladder is selected
- **WHEN** SPX data is current and coherent
- **THEN** the ladder may present planning guidance subject to canonical permission

#### Scenario: Another symbol is selected
- **WHEN** SPY, QQQ, or a searched non-SPX symbol is displayed
- **THEN** the ladder labels the data `Reference only` and suppresses SPX execution guidance

#### Scenario: User returns to SPX after reference inspection
- **WHEN** the user switches from an accepted SPX ladder to a reference symbol and back to the same SPX control context
- **THEN** the last aligned SPX payload is restored immediately without replacing it with an unbound standalone snapshot

### Requirement: Ranked priority levels preserve market structure
The system SHALL keep the depth map ordered by strike and SHALL separately rank up to three relevant levels from highest to lowest immediate priority.

#### Scenario: Multiple relevant levels exist
- **WHEN** an accepted snapshot contains at least three relevant strikes
- **THEN** a compact priority strip presents three deterministic levels with strike, role, distance, and reason

#### Scenario: Fewer relevant levels exist
- **WHEN** fewer than three accepted relevant strikes are available
- **THEN** the priority strip shows only available levels without placeholders that imply missing data

### Requirement: Clear ladder counts and transition language
The ladder SHALL distinguish priority rows, nearby hidden rows, and total available strikes, and transient spot-crossing language SHALL include direction and confirmation context when available.

#### Scenario: Focused ladder hides nearby rows
- **WHEN** the focused ladder displays a subset of accepted rows
- **THEN** the header reports priority rows shown, nearby rows hidden, and total strikes available as separate counts

#### Scenario: Spot crosses a level
- **WHEN** a rendered update detects spot crossing an accepted strike
- **THEN** the level identifies crossing direction and treats the event as awaiting confirmation rather than confirmed execution

### Requirement: Useful default inspection
The selected-level inspector SHALL default to the nearest meaningful accepted level while preserving later user selection.

#### Scenario: Initial accepted snapshot renders
- **WHEN** no level has been selected and relevant rows exist
- **THEN** the nearest meaningful row is selected and its context is populated

#### Scenario: Manual selection survives refresh
- **WHEN** the user selected a strike that remains in a newer accepted snapshot
- **THEN** the refreshed inspector continues to show that strike

### Requirement: Truthful expiration controls
Unavailable expiration choices SHALL explain their actual availability constraint.

#### Scenario: Prior snapshot is unavailable
- **WHEN** the Prior DTE control is disabled
- **THEN** its label or tooltip states that no prior snapshot is available and does not claim that only 3DTE data exists
