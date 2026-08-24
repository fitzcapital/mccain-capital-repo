## ADDED Requirements

### Requirement: Shared command-surface hierarchy
Market Pulse and Dashboard SHALL use the same three-level hierarchy: one primary decision surface,
supporting evidence surfaces, and progressively disclosed reference surfaces.

#### Scenario: User opens either page on desktop
- **WHEN** the initial viewport renders
- **THEN** one primary decision is visually dominant
- **AND** its reason and next required action are available in the same scan path
- **AND** supporting cards do not compete with the primary decision through equal glow, border, or
  typographic emphasis

#### Scenario: Primary data is stale or missing
- **WHEN** a required input is stale, failed, or unavailable
- **THEN** the primary surface displays the blocked state and concise cause
- **AND** the page does not substitute an actionable state

### Requirement: Semantic visual emphasis
The pages SHALL reserve strong color, glow, and elevated treatment for live, critical, selected, or
blocked state while ordinary surfaces use quiet borders and backgrounds.

#### Scenario: Routine supporting data renders
- **WHEN** a supporting value is healthy and requires no action
- **THEN** it uses a quiet surface without persistent glow

#### Scenario: State requires immediate attention
- **WHEN** execution is locked, data is unsafe, or a required action is pending
- **THEN** the related primary state receives semantic emphasis distinct from routine information

### Requirement: Progressive action density
Each page SHALL show only stage-relevant primary actions by default and SHALL place secondary
utilities and reference actions behind accessible menus, drawers, or disclosure controls.

#### Scenario: Initial desktop render
- **WHEN** Market Pulse or Dashboard loads
- **THEN** the primary action and refresh or recovery action remain immediately available
- **AND** secondary navigation and reference actions remain discoverable without dominating the page

#### Scenario: Secondary actions are requested
- **WHEN** a user opens the associated menu, drawer, or disclosure using pointer or keyboard input
- **THEN** the existing secondary actions become available without a full-page reload

### Requirement: Responsive command containment
The shared command surfaces MUST remain readable and operable without horizontal page overflow at
supported desktop, compact, and mobile widths.

#### Scenario: Wide desktop renders
- **WHEN** viewport width is 1280, 1440, or 1920 CSS pixels
- **THEN** primary command content uses available width without excessive empty space
- **AND** execution-critical content remains visible before supporting reference sections

#### Scenario: Compact or mobile layout renders
- **WHEN** command fields no longer fit in one row
- **THEN** fields reflow in decision, reason, action order
- **AND** all controls remain keyboard accessible with a minimum 44 CSS-pixel interaction target

### Requirement: Presentation-only modernization
The modernization MUST preserve authoritative calculations, data sources, refresh semantics,
chart state, account values, and user-entered workflow state.

#### Scenario: New hierarchy receives current application state
- **WHEN** existing backend payloads and client-side refresh events are rendered
- **THEN** displayed financial and trading outcomes match the existing authoritative values
- **AND** no new financial assumption or synthetic signal is introduced
