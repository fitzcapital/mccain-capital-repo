## ADDED Requirements

### Requirement: Spot-centered ladder structure
The Gamma Ladder SHALL render accepted strikes in price order around one visually dominant spot rail,
with above-spot resistance separated from below-spot support and acceleration structure.

#### Scenario: Default ladder scan
- **WHEN** an accepted Gamma generation contains rows above and below current spot
- **THEN** the default view identifies spot, the nearest upside decision row, and the nearest downside
  decision row without requiring selection or expanded detail

#### Scenario: Spot falls between strikes
- **WHEN** current spot does not equal an accepted strike
- **THEN** the spot rail appears between the adjacent strikes at the correct relative location and is
  not mislabeled as either strike

### Requirement: Deterministic positioning encoding
Each visible strike SHALL expose exact strike, signed net GEX, distance from spot, structural role,
interaction state, and strength while encoding signed magnitude as a diverging bar from a shared
zero axis.

#### Scenario: Positive and negative rows
- **WHEN** visible rows contain both positive and negative net GEX
- **THEN** their bars extend on opposite sides of the zero axis and retain explicit signed values and
  text labels

#### Scenario: Dominant outlier
- **WHEN** one row has much larger absolute GEX than its neighbors
- **THEN** normalization preserves comparative visibility for nearby rows while the exact outlier value
  remains unchanged

### Requirement: Execution emphasis is selective
The ladder SHALL emphasize only spot, the server-selected decision boundary, and the dominant node;
secondary strikes SHALL remain visually subordinate even when visible.

#### Scenario: Nearest decision row
- **WHEN** the execution map identifies a decision level
- **THEN** the matching strike is visibly linked to the decision summary and can be selected or sent
  to the chart

#### Scenario: No qualified decision row
- **WHEN** no accepted row qualifies as a decision boundary
- **THEN** spot remains the anchor and the ladder states that confirmation is unavailable without
  promoting a secondary strike

### Requirement: Progressive responsive inspection
The default ladder SHALL show no more than the accepted decision-relevant row limit and SHALL preserve
local full-depth disclosure, row inspection, filters, and chart coordination on desktop and mobile.

#### Scenario: Full ladder disclosure
- **WHEN** the user activates `Show full ladder`
- **THEN** hidden accepted rows appear in price order without refetching, changing generation, or
  losing the selected strike

#### Scenario: Mobile scan
- **WHEN** the ladder is rendered at a narrow viewport
- **THEN** strike, distance, role, signed GEX, state, and spot relationship remain readable while
  secondary explanatory prose is collapsed

### Requirement: Accessible semantic color
The price spine SHALL use the theme's teal for positive GEX, coral for negative GEX, cyan for spot,
and restrained gold for the dominant node, with text, bar direction, and border or icon cues that do
not rely on color alone.

#### Scenario: Color-independent interpretation
- **WHEN** color differentiation is unavailable or reduced
- **THEN** labels, signed values, bar direction, and role/state markers still communicate each row's
  meaning
