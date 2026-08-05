## ADDED Requirements

### Requirement: Calendar-first timing workflow
The Candle Opens page SHALL present the selected month calendar and Day Profile before supporting
catalyst cards, cycle reference details, and the expanded macro agenda.

#### Scenario: User opens Candle Opens on desktop
- **WHEN** an authenticated user opens the Candle Opens page
- **THEN** the command header and Today summary are followed by the calendar workspace
- **THEN** catalyst and macro-reference sections appear after the calendar workspace

### Requirement: Compact month command header
The page SHALL provide a compact command header containing the selected month, previous-month
navigation, next-month navigation, and a clearly labeled action that returns to the current month.

#### Scenario: User navigates months
- **WHEN** the user activates previous, next, or This Month
- **THEN** the control SHALL retain its existing Candle Opens destination and query semantics

### Requirement: Consolidated Today intelligence
The page SHALL present today’s importance, reset count and detail, macro count and next-event
context, and timing tags in one decision-oriented summary without changing server-provided values.

#### Scenario: Today has reset and macro activity
- **WHEN** the route supplies reset, macro, importance, and tag values
- **THEN** each value SHALL be visible in the Today summary
- **THEN** the page SHALL not require a duplicate primary cycle panel ahead of the calendar

#### Scenario: Macro data is missing or stale
- **WHEN** no next macro notice is available
- **THEN** the Today summary SHALL display the existing session fallback text
- **THEN** the macro agenda SHALL retain its unavailable or fallback state

### Requirement: Preserved calendar inspection behavior
The redesigned calendar SHALL preserve day cells, selected-day inspection, keyboard activation,
holiday states, reset labels, macro markers, and the Day Profile content.

#### Scenario: User selects a calendar day
- **WHEN** the user activates a day by pointer or keyboard
- **THEN** the Day Profile SHALL update using the existing day metadata
- **THEN** no date, reset, macro, or timing-tag field SHALL be lost

### Requirement: Catalyst and reference hierarchy
The page SHALL retain upcoming focus cards, cluster summaries, cycle definitions, legends, and the
macro agenda while visually subordinating them to the primary calendar workspace.

#### Scenario: Supporting timing information is available
- **WHEN** focus cards, cluster summaries, or cycle definitions are supplied
- **THEN** they SHALL remain reachable after the calendar workspace
- **THEN** macro-event anchors and event links SHALL retain their existing destinations

### Requirement: Responsive and accessible presentation
The Candle Opens workflow SHALL adapt without horizontal overflow and SHALL preserve accessible
names, focus visibility, reduced-motion behavior, and mobile weekly disclosures.

#### Scenario: User opens a narrow viewport
- **WHEN** the viewport is 390 pixels wide
- **THEN** the workflow SHALL use a single-column presentation without horizontal overflow
- **THEN** mobile week disclosures and their day controls SHALL remain operable

#### Scenario: User prefers reduced motion
- **WHEN** `prefers-reduced-motion` is enabled
- **THEN** nonessential Candle Opens transitions SHALL be disabled
