## MODIFIED Requirements

### Requirement: Primary chart hierarchy
The dashboard SHALL render market context beneath the Today decision, with SPX as the dominant
market canvas and VIX as a smaller volatility-confirmation chart.

#### Scenario: Dashboard tape is available
- **WHEN** the dashboard renders its market tape
- **THEN** the Today decision remains the page's primary visual anchor
- **AND** SPX receives primary weight within the supporting market region
- **AND** VIX receives secondary confirmation weight

### Requirement: Responsive layout
The market canvas SHALL preserve decision-before-market and primary-before-secondary hierarchy at
narrower widths.

#### Scenario: Available width is narrow
- **WHEN** the side-by-side canvas no longer fits clearly
- **THEN** the Today decision remains before the market region
- **AND** the VIX confirmation chart stacks after SPX
- **AND** comparison tiles reflow without horizontal page overflow
