## ADDED Requirements

### Requirement: Gamma-first workflow hierarchy
The Market Pulse page SHALL present market status first, Gamma levels and the execution chart as
the primary cockpit, structure and decision context next, the entry checklist after the decision,
and secondary market context last.

#### Scenario: Desktop primary workflow
- **WHEN** the user opens Market Pulse at a desktop viewport with usable market data
- **THEN** the status bar, Gamma Level Deck, and execution chart appear before the Trade Decision, entry checklist, tape, and news sections
- **AND** the Gamma Level Deck and execution chart are simultaneously visible without horizontal document overflow

#### Scenario: Narrow-screen primary workflow
- **WHEN** the user opens Market Pulse at a narrow viewport
- **THEN** the primary content stacks in the order status, Gamma Level Deck, chart, structure, decision, and checklist
- **AND** secondary context does not displace the primary workflow above the fold

### Requirement: Compact authoritative market status
The page SHALL provide one compact status surface containing the selected ticker, current spot,
session/data state, freshness timestamp, symbol selection, and refresh action.

#### Scenario: Live or delayed data
- **WHEN** a usable live or delayed snapshot is available
- **THEN** the status surface identifies the ticker, spot, state, and freshness with explicit text
- **AND** the same spot value is used by the Level Deck, chart context, and structure map

#### Scenario: Stale or unavailable data
- **WHEN** spot or structure data is stale, degraded, missing, or unavailable
- **THEN** the status surface explicitly identifies that state without presenting a missing value as valid
- **AND** refresh and diagnostic affordances remain available

### Requirement: Gamma Level Deck
The primary cockpit SHALL expose Main Flip, Local Flip, Call Wall, Put Wall, spot, nearest
important level, distance, and gamma classification when those values are available.

#### Scenario: Complete gamma structure
- **WHEN** all principal gamma levels and spot are available
- **THEN** each level is labeled with its value, role, classification, and distance from spot
- **AND** the nearest important level is visually identified without relying on color alone

#### Scenario: Partial gamma structure
- **WHEN** one or more principal gamma levels are unavailable or outside the selected data window
- **THEN** each unavailable level is labeled unavailable or out of range
- **AND** no fabricated numeric value or misleading chart line is shown

### Requirement: Synchronized Gamma and chart interaction
The page SHALL coordinate supported Gamma Level Deck interactions with the corresponding execution
chart overlays while preserving independent chart operation when gamma data is unavailable.

#### Scenario: Select a supported level
- **WHEN** the user selects an available Gamma level in the Level Deck
- **THEN** the matching row and chart overlay become visually identifiable as the same selection
- **AND** the selected level remains identifiable while the user changes supported chart timeframes

#### Scenario: Hover or focus a supported level
- **WHEN** the user hovers or keyboard-focuses an available Gamma level
- **THEN** the matching chart overlay receives a temporary highlight
- **AND** removing hover or focus restores the prior pinned selection state

#### Scenario: Select an unavailable level
- **WHEN** a Gamma level has no valid numeric price
- **THEN** it cannot create or highlight a chart overlay
- **AND** its unavailable state remains explicit

### Requirement: Consolidated structure and Trade Decision
The page SHALL present one structure-location map followed by one authoritative Trade Decision
surface that communicates state, bias, best look, required trigger, invalidation, and execution
status.

#### Scenario: Actionable state
- **WHEN** existing execution rules classify the setup as actionable
- **THEN** the Trade Decision surface displays the actionable state and its trigger and invalidation context
- **AND** no competing panel displays a contradictory decision state

#### Scenario: Wait or no-trade state
- **WHEN** existing execution rules classify the setup as wait, planning only, or no trade
- **THEN** the Trade Decision surface displays that explicit state and the next required confirmation
- **AND** the page does not imply permission to execute through color or layout

### Requirement: Preserved controls and secondary context
The redesign SHALL preserve every current Market Pulse control, destination, modal, and diagnostic
while moving non-primary content into lower-priority or disclosure surfaces.

#### Scenario: Control parity
- **WHEN** final Market Pulse controls are inventoried against the baseline
- **THEN** ticker search, ticker shortcuts, timeframes, chart drawing controls, markers, Gamma and day-level toggles, Gamma symbol/window/DTE controls, refresh actions, replay, diagnostics, Candle Opens, tape, and news remain reachable
- **AND** form actions, query parameters, API endpoints, external destinations, and accessible names remain compatible

#### Scenario: Secondary context disclosure
- **WHEN** the user opens a secondary tape, news, calendar, diagnostics, replay, or advanced Gamma disclosure
- **THEN** its current content and interactions remain usable
- **AND** closing it returns focus according to the existing accessible interaction pattern

### Requirement: Responsive and accessible cockpit behavior
The redesigned workflow SHALL support keyboard operation, visible focus, reduced motion, and
responsive layouts at 390, 768, 1440, and 1920 pixel widths.

#### Scenario: Keyboard navigation
- **WHEN** a keyboard user traverses the status, Level Deck, chart controls, decision, checklist, and disclosures
- **THEN** interactive controls follow the visual workflow order and show visible focus
- **AND** selection, pressed, expanded, loading, and disabled states are semantically exposed

#### Scenario: Responsive verification
- **WHEN** representative captures run at 390, 768, 1440, and 1920 pixel widths
- **THEN** the page has no document-level horizontal overflow, clipped primary controls, detached overlays, or unreadable level values
- **AND** all baseline controls remain present
