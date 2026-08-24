## MODIFIED Requirements

### Requirement: Compact scenario presentation
The Market Pulse page SHALL present one primary scenario, at least one compact alternative when
available, and a collapsed dormant watch list without obscuring the chart or gamma ladder. On
desktop, the alternative, dormant, live monitor, and replay region SHALL use the full execution
cockpit width; live monitor and replay summaries SHALL each span that region while collapsed.

#### Scenario: Multiple candidates render
- **WHEN** active, alternative, and dormant candidates are available
- **THEN** the primary lane exposes its complete five-stage plan, secondary lanes expose concise
  state and trigger summaries, and each lane is visually distinguishable without implying that a
  pending lane is actionable

#### Scenario: Desktop monitoring region renders
- **WHEN** the execution cockpit is displayed at a desktop breakpoint
- **THEN** alternative and dormant watches share the available row and live monitor and replay each
  span the full cockpit width without leaving an unused right column

#### Scenario: Narrow monitoring region renders
- **WHEN** the execution cockpit is displayed below the desktop breakpoint
- **THEN** all scenario summaries stack in one column without horizontal overflow
