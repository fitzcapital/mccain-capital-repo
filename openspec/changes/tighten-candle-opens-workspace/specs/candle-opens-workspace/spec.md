## ADDED Requirements

### Requirement: Primary navigation access
The application SHALL show Candle Opens immediately after Market Pulse in the full desktop primary
navigation and SHALL avoid a duplicate Candle Opens destination in the desktop Tools menu.

#### Scenario: Desktop navigation order
- **WHEN** a user views the full desktop navigation
- **THEN** Candle Opens appears once immediately after Market Pulse

#### Scenario: Responsive access remains available
- **WHEN** the full desktop navigation is replaced by a responsive menu
- **THEN** Candle Opens remains available from that responsive navigation

### Requirement: Calendar-first timing workspace
The Candle Opens page SHALL make the selected month calendar and selected-day profile the canonical
scan-and-inspect workspace.

#### Scenario: Initial page hierarchy
- **WHEN** the Candle Opens page loads
- **THEN** month controls, compact today context, calendar, and selected-day profile are presented
  before supporting catalyst and reference material

#### Scenario: Selecting a session
- **WHEN** a user selects a calendar session
- **THEN** the selected-day profile displays that session's complete timing details without requiring
  a second duplicate summary section

### Requirement: Compact today context
The page SHALL provide a compact current-session strip containing the date, importance, reset count,
macro count, and a way to select today without repeating the full selected-day profile.

#### Scenario: Today is also selected
- **WHEN** today's calendar session is selected
- **THEN** detailed reset, cycle, macro, tag, and note content appears only in the selected-day profile

#### Scenario: Another day is selected
- **WHEN** a user selects a different day
- **THEN** the compact today strip remains available as orientation and can return selection to today

### Requirement: Unique upcoming catalysts
The page SHALL consolidate upcoming catalyst summaries by session date and SHALL show no more than
three unique dates before expansion.

#### Scenario: Multiple catalyst roles share one date
- **WHEN** one date is the next reset cluster, largest cluster, and a macro overlap
- **THEN** the page shows one date card with multiple reason badges instead of repeated cards

#### Scenario: More than three catalyst dates exist
- **WHEN** more than three unique upcoming catalyst dates are available
- **THEN** the first three are visible and the remainder are reachable through an expansion control

### Requirement: Consolidated timing reference
The page SHALL keep monthly totals and day, week, and month cycle definitions in one collapsed Timing
Reference section while keeping macro-event details reachable separately.

#### Scenario: Reference details are needed
- **WHEN** a user expands Timing Reference
- **THEN** monthly counts and all configured cycle definitions are visible without separate duplicate
  reference panels

#### Scenario: Macro details are needed
- **WHEN** a user expands the macro-event section
- **THEN** all supplied event dates, impact levels, times, and labels remain available

### Requirement: Responsive readability
The Candle Opens workspace SHALL avoid horizontal page overflow and preserve usable calendar and
selected-day detail at supported desktop and responsive widths.

#### Scenario: Compact desktop viewport
- **WHEN** available content width is insufficient for calendar and profile side by side
- **THEN** the selected-day profile stacks below the calendar without clipping controls or text

#### Scenario: Mobile viewport
- **WHEN** the page is viewed at mobile width
- **THEN** primary controls remain operable and supporting content follows the calendar in a readable
  single-column flow
