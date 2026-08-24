## ADDED Requirements

### Requirement: Dashboard secondary intelligence spans the reading canvas
The Dashboard SHALL render the SPX Session Snapshot across the same bounded desktop reading canvas
as the performance summary while preserving a compact internal visualization.

#### Scenario: Snapshot opens on a wide desktop
- **WHEN** the performance Dashboard is displayed above the desktop breakpoint
- **THEN** the snapshot stage spans the available Dashboard canvas
- **AND** it does not retain the retired half-width side-rail footprint

### Requirement: User-controlled Dashboard navigation pin
The Dashboard SHALL provide an accessible pin control for its workflow navigation and SHALL retain
the user's chosen pinned state across page loads when browser storage is available.

#### Scenario: User pins the navigation
- **WHEN** the user activates the unpinned control on a desktop viewport
- **THEN** the workflow navigation remains visible below the application header while scrolling
- **AND** the control communicates that the navigation is pinned

#### Scenario: User unpins the navigation
- **WHEN** the user activates the pinned control
- **THEN** the workflow navigation returns to normal document flow
- **AND** the unpinned preference is retained for the next Dashboard visit

#### Scenario: Browser storage is unavailable
- **WHEN** preference storage cannot be read or written
- **THEN** the control continues to toggle for the current page without blocking navigation

### Requirement: Bounded primary-page desktop canvas
The application SHALL center the primary content of Dashboard, Market Pulse, and Executive inside
page-appropriate reading-width bounds on wide desktop viewports.

#### Scenario: Primary page opens on a wide desktop
- **WHEN** Dashboard, Market Pulse, or Executive is displayed above the desktop breakpoint
- **THEN** its primary content is centered with visible side breathing room
- **AND** major cards do not expand into excessively long low-density rows

#### Scenario: Dense Market Pulse opens on a wide desktop
- **WHEN** Market Pulse is displayed above the desktop breakpoint
- **THEN** it receives a modestly wider bound than Dashboard and Executive
- **AND** its chart and Gamma Ladder remain centered with visible side breathing room

#### Scenario: Primary page opens at a smaller width
- **WHEN** the viewport is at or below the desktop breakpoint
- **THEN** the page uses the available width and existing responsive layout
- **AND** no horizontal page overflow is introduced

### Requirement: Primary access to Executive
The application SHALL expose Executive directly in the shared primary navigation alongside the
Dashboard and Market Pulse destinations.

#### Scenario: User needs the application overview
- **WHEN** the shared desktop navigation is visible
- **THEN** Executive is available without opening the secondary Tools menu
- **AND** the current-page state remains visually identifiable
