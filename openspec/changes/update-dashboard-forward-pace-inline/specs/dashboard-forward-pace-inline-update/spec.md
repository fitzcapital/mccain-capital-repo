## ADDED Requirements

### Requirement: Dashboard Forward Pace saves without full-page navigation
The system SHALL allow an authenticated user with JavaScript enabled to save Dashboard Forward Pace settings and update only the Forward Pace card without navigating or reloading the full Dashboard.

#### Scenario: Save custom projection settings
- **WHEN** the user submits a valid daily pace, pass buffer, start date, or target date from the Dashboard Forward Pace form
- **THEN** the system persists the settings, recalculates the server-authoritative projection, replaces only the Forward Pace card, and announces success

#### Scenario: Reset to live pace
- **WHEN** the user selects `Use Live Pace`
- **THEN** the system clears the same manual settings cleared by the existing reset workflow and redraws the Forward Pace card in live-pace mode without a full-page navigation

#### Scenario: Consecutive inline saves
- **WHEN** the user completes one inline update and then submits the replacement form again
- **THEN** the second update SHALL use the same inline behavior and SHALL NOT fall back to an unintended full-page submission

### Requirement: Server remains authoritative for projection output
The system MUST use the existing Dashboard Forward Pace sources, settings, timing assumptions, account allocation, and calculation behavior when producing the replacement card.

#### Scenario: Successful response reflects persisted values
- **WHEN** an inline save succeeds
- **THEN** the returned card SHALL be rendered from the values persisted by the server and the recalculated Dashboard Forward Pace view model

#### Scenario: Live pace remains the fallback source
- **WHEN** no valid positive manual daily pace is stored
- **THEN** the replacement card SHALL continue to use the existing live trading pace fallback

#### Scenario: Manual settings are not changed by a failed request
- **WHEN** validation, authorization, CSRF protection, persistence, or rendering fails
- **THEN** the system SHALL NOT present unsaved inputs as authoritative and SHALL preserve the last successfully persisted manual settings

### Requirement: Buffer-adjusted availability is distinct from projected balance
The system MUST preserve Projected Balance as the expected account balance and SHALL expose Available Balance After Buffer as a separate server-authoritative value when presenting Forward Pace projections.

#### Scenario: Positive pass buffer
- **WHEN** the projection has a base balance of $75,000, gross projected profit of $14,000, and a pass buffer of $7,500
- **THEN** Projected Balance SHALL be $89,000, Projected Profit After Buffer SHALL be $6,500, and Available Balance After Buffer SHALL be $81,500

#### Scenario: Zero pass buffer
- **WHEN** the configured pass buffer is zero or disabled
- **THEN** Available Balance After Buffer SHALL equal Projected Balance and Projected Profit After Buffer SHALL equal gross projected profit

#### Scenario: Buffer exceeds gross projected profit
- **WHEN** the pass buffer is greater than gross projected profit
- **THEN** Projected Profit After Buffer SHALL be negative, Available Balance After Buffer SHALL equal Projected Balance minus the buffer, and Projected Balance SHALL remain unchanged

#### Scenario: Buffer is deducted exactly once
- **WHEN** the server constructs any target-date or 5D, 10D, or 20D Forward Pace projection
- **THEN** each net or available value SHALL be derived from its gross base by subtracting the pass buffer exactly once

#### Scenario: Buffer terminology is visible
- **WHEN** a positive pass buffer is configured
- **THEN** the Forward Pace card SHALL identify Projected Balance, Reserved Buffer, and Available Balance After Buffer as distinct values and explain that the buffer remains in the account but is excluded from available balance

### Requirement: Inline update failure is safe and accessible
The system SHALL expose pending, success, and error states for the inline save while retaining usable content during failures.

#### Scenario: Request is pending
- **WHEN** an inline save is in progress
- **THEN** the initiating controls SHALL be protected from duplicate submission and an accessible saving state SHALL be available

#### Scenario: Validation or server error
- **WHEN** the server rejects the request or cannot return a valid replacement fragment
- **THEN** the current Forward Pace card and entered values SHALL remain visible and an accessible inline error SHALL explain that the update was not saved

#### Scenario: Network or response parsing error
- **WHEN** the browser cannot complete or parse the inline response
- **THEN** the current card SHALL remain intact and the user SHALL be able to retry or use the normal form workflow

### Requirement: Non-JavaScript fallback remains functional
The Dashboard Forward Pace form MUST retain its standard POST action and redirect behavior when asynchronous enhancement is unavailable.

#### Scenario: JavaScript is unavailable
- **WHEN** the user submits the Forward Pace form without JavaScript interception
- **THEN** the existing `/dashboard/pace` workflow SHALL persist the settings, flash the result, and redirect back to the scoped Dashboard

#### Scenario: Non-JSON request compatibility
- **WHEN** `/dashboard/pace` receives an ordinary form POST without the explicit inline-response signal
- **THEN** the route SHALL return the existing redirect response rather than JSON

### Requirement: Inline replacement preserves Dashboard context
The system SHALL limit the update to the Forward Pace card and preserve the user's surrounding Dashboard context.

#### Scenario: Successful update preserves position and scope
- **WHEN** an inline update succeeds
- **THEN** the Dashboard SHALL retain its current URL scope, selected ticker, pace timeframe, disclosure context where practical, and materially unchanged scroll position

#### Scenario: Unrelated Dashboard regions
- **WHEN** the Forward Pace card is replaced
- **THEN** unrelated Dashboard cards and their client-side state SHALL not be re-rendered or reset
