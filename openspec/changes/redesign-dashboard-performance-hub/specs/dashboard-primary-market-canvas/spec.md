## MODIFIED Requirements

### Requirement: Primary chart hierarchy
The Dashboard SHALL render SPX and VIX as compact supporting market context beneath trading-business
performance, with SPX retaining greater visual weight than VIX.

#### Scenario: Dashboard tape is available
- **WHEN** the Dashboard renders its market context
- **THEN** SPX receives greater visual weight than VIX
- **AND** neither chart precedes monthly performance, trading quality, or equity trend

### Requirement: Compact comparison context
The Dashboard SHALL expose SPY, QQQ, and IWM as optional compact comparison context without
duplicating full-size charts or dominating the performance review.

#### Scenario: Comparison quotes are present
- **WHEN** the user expands or reaches market context
- **THEN** each symbol shows price direction and percentage context in a compact tile

### Requirement: Responsive layout
The market context SHALL preserve the primary-before-secondary hierarchy at narrower widths without
displacing the Dashboard's financial-performance order.

#### Scenario: Available width is narrow
- **WHEN** the side-by-side market context no longer fits clearly
- **THEN** VIX stacks after SPX and comparison tiles reflow without horizontal page overflow
