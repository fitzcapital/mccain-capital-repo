## ADDED Requirements

### Requirement: Navigation distinguishes primary destinations from tools
The global navigation SHALL expose Dashboard, Market Pulse, Trades, and Journal as primary
destinations and group secondary analysis, planning, and utility pages under a labeled Tools menu.

#### Scenario: Navigation initially renders
- **WHEN** any application page loads
- **THEN** the primary destinations are visible without opening a menu
- **AND** secondary destinations remain reachable through the Tools menu

### Requirement: Existing destinations remain compatible
Navigation regrouping MUST preserve existing destination URLs, active-page indication, and direct
links.

#### Scenario: User selects a regrouped destination
- **WHEN** the user activates a secondary destination
- **THEN** the existing destination URL opens with its active state preserved

### Requirement: Navigation remains accessible
The primary navigation and Tools menu MUST support keyboard operation, visible focus, accurate ARIA
expanded/current state, and compact-width access.

#### Scenario: Keyboard user opens Tools
- **WHEN** focus is on the Tools control and the user activates it
- **THEN** the menu opens, announces its state, and allows focus to reach each destination

#### Scenario: Compact viewport renders
- **WHEN** primary items no longer fit in one row
- **THEN** navigation provides an accessible compact control without horizontal page overflow
