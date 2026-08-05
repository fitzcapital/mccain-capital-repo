## ADDED Requirements

### Requirement: Market Pulse presents one execution narrative
Market Pulse SHALL connect regime, current spot, decisive gamma levels, trigger, invalidation,
data freshness, and trading permission in a clear first-viewport sequence.

#### Scenario: Healthy authenticated session loads
- **WHEN** Market Pulse has current valid market and gamma data
- **THEN** the first viewport shows the regime, spot, decisive levels, trigger, invalidation, freshness, permission state, and one primary next action

#### Scenario: Trading permission differs from data health
- **WHEN** market data is healthy but the existing trading gate is locked
- **THEN** Market Pulse displays healthy data and locked trading permission as separate, unambiguous states

### Requirement: Market Pulse state language is truthful and distinct
Market Pulse SHALL expose loading, live, stale, unavailable, and locked states with distinct text
and non-color indicators derived from the existing authoritative payload fields.

#### Scenario: Data is stale
- **WHEN** the existing freshness rules classify the displayed data as stale
- **THEN** Market Pulse labels the data stale, shows its relevant timestamp or age, and does not imply that it is live

#### Scenario: A decisive level is unavailable
- **WHEN** an authoritative gamma or level value is absent or invalid
- **THEN** Market Pulse shows an unavailable state rather than a fabricated number or a misleading zero

#### Scenario: Data is loading
- **WHEN** the client is awaiting an existing market or gamma response
- **THEN** Market Pulse identifies the loading state without presenting prior content as newly refreshed

### Requirement: Market Pulse separates execution signals from supporting analysis
Market Pulse SHALL keep the core execution signals immediately visible and SHALL place advanced
gamma detail, diagnostics, supporting levels, and research context behind explicit,
keyboard-operable disclosures.

#### Scenario: Default Market Pulse state
- **WHEN** the page first renders
- **THEN** advanced supporting analysis does not compete visually with regime, decisive levels, trigger, invalidation, freshness, permission, and next action

#### Scenario: User requests advanced context
- **WHEN** the user opens an advanced analysis disclosure
- **THEN** the existing detailed data remains available without changing the authoritative execution state

### Requirement: Market Pulse preserves existing market-data behavior
The change MUST preserve provider selection, payload semantics, refresh timing, gamma
calculations, trading gates, and existing failure fallbacks.

#### Scenario: Visual hierarchy changes
- **WHEN** Market Pulse cards, labels, or disclosures are reorganized
- **THEN** the values and states continue to derive from the same authoritative route payloads and client update paths

#### Scenario: Existing refresh fails
- **WHEN** a market or gamma refresh fails
- **THEN** Market Pulse retains the existing fallback behavior and accurately labels the resulting freshness or availability state

### Requirement: Market Pulse is usable at a 390-pixel viewport
At a 390-pixel viewport, Market Pulse SHALL have no horizontal overflow, SHALL retain the core
execution narrative before advanced context, and SHALL keep actionable content clear of fixed
navigation.

#### Scenario: Market Pulse loads at 390 pixels
- **WHEN** Market Pulse is rendered at a 390-pixel viewport
- **THEN** regime, permission, decisive signals, freshness, and the primary next action remain readable without horizontal overflow or fixed-navigation overlap
