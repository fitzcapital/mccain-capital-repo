## ADDED Requirements

### Requirement: Dashboard first viewport communicates the operating decision
The Trading Dashboard SHALL present the current trading state, permission gate, account-risk
context, market context, and primary next action before historical, review, or diagnostic
content at desktop widths.

#### Scenario: Authenticated desktop dashboard loads
- **WHEN** an authenticated user opens the Trading Dashboard at a desktop viewport
- **THEN** the first viewport identifies the current state, whether trading is permitted, the material account-risk context, the relevant market context, and one primary next action

#### Scenario: Trading is locked
- **WHEN** the dashboard's existing business rules produce a locked or no-trade state
- **THEN** the first viewport communicates the lock and its reason without relying on color alone

### Requirement: Secondary dashboard content uses progressive disclosure
The dashboard SHALL keep supporting diagnostics, history, calendar detail, and extended review
content available through explicit, keyboard-operable disclosure controls while keeping the
default decision path concise.

#### Scenario: Default dashboard state
- **WHEN** the dashboard first renders
- **THEN** secondary sections that do not change the immediate trading decision are collapsed or summarized

#### Scenario: User expands a secondary section
- **WHEN** the user activates a disclosure control with pointer, touch, or keyboard
- **THEN** the associated detail becomes available without navigating away or losing the primary decision context

### Requirement: Dashboard values preserve existing authority and provenance
The dashboard MUST preserve all existing financial calculations, account scopes, source timing,
and fallback behavior, and MUST keep manually entered broker metrics unchanged unless an existing
automated refresh succeeds.

#### Scenario: Automated broker refresh fails
- **WHEN** an automated broker refresh returns no valid replacement value
- **THEN** the dashboard continues displaying the existing manual metric and exposes the existing failure or diagnostic state

#### Scenario: Dashboard hierarchy changes
- **WHEN** cards or sections are reordered, summarized, or disclosed
- **THEN** their displayed values retain the same source inputs, account allocation, timestamps, and calculation rules

### Requirement: Dashboard default mobile layout is concise and unobstructed
At a 390-pixel viewport, the dashboard SHALL have no horizontal overflow, SHALL keep its primary
decision sequence before secondary content, and SHALL reserve enough bottom clearance that fixed
navigation does not cover any actionable element.

#### Scenario: Dashboard loads at 390 pixels
- **WHEN** the dashboard is rendered at a 390-pixel viewport
- **THEN** the primary state and next action remain readable, no horizontal overflow occurs, and visible controls are not covered by fixed navigation

#### Scenario: Mobile user opens secondary content
- **WHEN** a mobile user expands a secondary dashboard section
- **THEN** the detail flows within the viewport and remains reachable above the reserved navigation clearance
