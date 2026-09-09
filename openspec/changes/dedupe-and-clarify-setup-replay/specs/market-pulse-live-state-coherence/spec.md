## ADDED Requirements

### Requirement: Shared canonical setup identity
Live Setup Monitor and Setup Replay SHALL use the same canonical event identity based on symbol,
session, completed pattern, completion timestamp, and direction. Anchor-level multiplicity MUST NOT
produce duplicate live records, replay cards, revisions, or alerts.

#### Scenario: Live evaluation sees two qualifying anchors
- **WHEN** one newly completed pattern qualifies at two compatible nearby structural levels
- **THEN** Live persists one event, emits at most one alert, and exposes the other level as supporting
  confluence

#### Scenario: Evaluation repeats after candidate reorder
- **WHEN** the same completion is evaluated again with a different anchor ordering
- **THEN** canonical identity resolves to the existing event without a duplicate record or alert

### Requirement: Common new-setup admission cutoff
Live Setup Monitor and Setup Replay SHALL use 3:15 PM ET as the inclusive final completion time for
admitting new setup events. Completed patterns after 3:15 PM ET MUST remain chart context only and
MUST NOT appear as setups that Live could have alerted.

#### Scenario: Pattern completes at the cutoff
- **WHEN** an otherwise eligible pattern completes at 3:15 PM ET
- **THEN** Live and Replay may admit the same canonical setup event

#### Scenario: Pattern completes after the cutoff
- **WHEN** an otherwise eligible pattern completes after 3:15 PM ET
- **THEN** neither Live nor Replay admits it as a new setup event

#### Scenario: Admitted setup remains unresolved after cutoff
- **WHEN** a setup was admitted by 3:15 PM ET and remains open afterward
- **THEN** lifecycle monitoring may still resolve its target, invalidation, or expiration without
  admitting a new setup
