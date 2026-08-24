## MODIFIED Requirements

### Requirement: Consolidated command hierarchy
The Dashboard SHALL present one compact Market Pulse handoff containing current market-data state,
trade-permission summary, and a single action to open the full execution workspace.

#### Scenario: Dashboard initially renders
- **WHEN** a user opens the Dashboard
- **THEN** execution permission is supporting context rather than the primary page hero
- **AND** detailed trigger, invalidation, and scenario decisions are not repeated from Market Pulse

#### Scenario: Execution or data state is blocked
- **WHEN** permission or required market data is blocked or unavailable
- **THEN** the compact handoff reports the blocker and still links to Market Pulse for detail

#### Scenario: Execution context is healthy
- **WHEN** no execution or data exception requires attention
- **THEN** the handoff remains compact without routine explanatory copy

### Requirement: Unified execution plan
The Dashboard SHALL delegate detailed bias, location, trigger, invalidation, risk, and scenario
planning to Market Pulse while retaining a single execution-navigation action.

#### Scenario: User needs execution detail
- **WHEN** the user activates the Market Pulse handoff
- **THEN** Market Pulse opens as the authoritative execution workspace

#### Scenario: Dashboard displays financial performance
- **WHEN** the Dashboard renders
- **THEN** execution-plan cards do not compete with monthly earnings, goals, risk, and trading quality

### Requirement: Canonical dashboard workflow language
The Dashboard SHALL use the stage labels Performance, Quality, Trends, and Review for its primary
in-page workflow navigation.

#### Scenario: Dashboard renders
- **WHEN** a user opens the Dashboard
- **THEN** its in-page navigation presents `Performance → Quality → Trends → Review`
- **AND** execution stages remain owned by Market Pulse
