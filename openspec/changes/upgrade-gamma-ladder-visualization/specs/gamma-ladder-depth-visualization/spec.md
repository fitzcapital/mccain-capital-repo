## ADDED Requirements

### Requirement: Institutional Gamma Ladder hierarchy
The Gamma Ladder SHALL present a compact status and control header, a key-level summary, the strike
depth board, a selected-level readout, and supporting explanation in that order.

#### Scenario: Complete ladder data
- **WHEN** a valid Gamma Ladder payload is rendered
- **THEN** symbol, spot, regime, expiration, freshness, strongest level, flip, and nearest support or resistance are readable before the strike rows
- **AND** the strike board remains the dominant surface rather than the legend or controls

#### Scenario: Partial ladder data
- **WHEN** one or more summary values are missing or outside the selected window
- **THEN** each missing value is labeled unavailable without a fabricated number
- **AND** the valid rows and controls remain usable

### Requirement: Readable dealer-positioning depth map
Each ladder row SHALL communicate strike, location relative to spot, negative and positive
positioning around a common center axis, net Gamma, strength, and structural role using text or
shape in addition to color.

#### Scenario: Dominant or structural strike
- **WHEN** a strike is spot-nearest, strongest, flip, support, resistance, or acceleration
- **THEN** the row displays the applicable explicit role label and a distinct non-color treatment
- **AND** its numeric values remain aligned with the other rows

#### Scenario: Low-impact strike
- **WHEN** a strike has low relative Gamma strength
- **THEN** it is visually quieter than dominant strikes without making its values unreadable

### Requirement: Event-driven data motion
The ladder SHALL animate only meaningful changes between an accepted previous payload and a newer
payload, while loading indicators MAY loop continuously.

#### Scenario: Refresh changes existing values
- **WHEN** a newer accepted payload changes the width, sign, or value of an existing strike
- **THEN** the affected bar and value transition from their prior presentation to the new state
- **AND** unchanged rows do not replay attention animations

#### Scenario: Strike order changes
- **WHEN** accepted data inserts, removes, or reorders visible strikes
- **THEN** surviving rows move to their new positions without abrupt page-level jumps
- **AND** entering and removed rows use a short one-shot transition

#### Scenario: Continuous refresh stability
- **WHEN** multiple refreshes occur without material value or classification changes
- **THEN** the ladder remains visually still after rendering the accepted payload

### Requirement: Spot, regime, and dominant-node transitions
The ladder SHALL provide restrained one-shot feedback when spot crosses structure, the Gamma regime
changes, or the strongest node moves.

#### Scenario: Spot moves without crossing structure
- **WHEN** spot moves between accepted payloads but does not cross a flip, wall, or displayed strike
- **THEN** the spot marker moves smoothly to its new position without a repeated alert effect

#### Scenario: Spot crosses a structural level
- **WHEN** spot crosses a displayed flip, support, resistance, or acceleration level
- **THEN** the destination row receives one brief pulse and an explicit crossing-state label
- **AND** no trading permission is inferred from the animation

#### Scenario: Regime or strongest node changes
- **WHEN** the accepted regime or strongest strike differs from the prior accepted payload
- **THEN** the new state receives one brief transition and the prior state loses its emphasis
- **AND** the resulting regime and strongest-node labels match the accepted payload

### Requirement: Selected-level inspection and chart coordination
The ladder SHALL keep one authoritative selected strike visible while preserving the existing chart
selection event and row-detail behavior.

#### Scenario: Select a strike
- **WHEN** the user activates an available ladder row with pointer or keyboard
- **THEN** the row, sticky selected-level readout, and corresponding chart line identify the same symbol and strike
- **AND** the row exposes its existing detail values and behavior explanation

#### Scenario: Change filters after selection
- **WHEN** ticker, DTE, expiration, or strike window changes and the selected strike is no longer present
- **THEN** the stale selection is cleared explicitly
- **AND** no chart line or selected-level readout continues to present it as current

### Requirement: Safe refresh, stale, loading, and failure behavior
The redesigned ladder SHALL preserve current loading, empty, stale, degraded, and error semantics
and SHALL NOT animate rejected, invalid, stale, or mismatched-symbol payloads into the board.

#### Scenario: Refresh responses arrive out of order
- **WHEN** an older request completes after a newer request for the active ladder context
- **THEN** the older response is ignored
- **AND** its values and animations do not replace or replay over the newer board

#### Scenario: Refresh fails with prior data
- **WHEN** refresh fails while a prior valid ladder is displayed
- **THEN** the prior values remain visible with an explicit stale or error state
- **AND** refresh, diagnostics, and row inspection remain reachable where currently supported

#### Scenario: No usable data
- **WHEN** no valid ladder payload is available
- **THEN** the board shows an explicit empty or unavailable state without fabricated bars
- **AND** no spot, regime, or dominant-node transition plays

### Requirement: Accessible and responsive motion system
The ladder SHALL support keyboard navigation, visible focus, reduced motion, semantic state labels,
and responsive layouts at 390, 768, 1440, and 1920 pixel widths.

#### Scenario: Reduced-motion preference
- **WHEN** the user requests reduced motion
- **THEN** row movement, value morphs, crossing pulses, sweeps, and detail transitions complete without animated travel
- **AND** every final value and state remains visible

#### Scenario: Narrow viewport
- **WHEN** the viewport is narrow
- **THEN** rows reduce to strike, depth visualization, and net Gamma while secondary details remain available on activation
- **AND** spot and the selected-level readout remain discoverable without document-level horizontal overflow

#### Scenario: Keyboard operation
- **WHEN** the user navigates the ladder using a keyboard
- **THEN** controls and rows follow visual order and expose visible focus, pressed, expanded, loading, disabled, and disclosure states

### Requirement: Existing data and control compatibility
The upgrade SHALL preserve existing Gamma sources, formulas, classifications, payload fields,
selection events, controls, destinations, and fallback behavior.

#### Scenario: Control parity
- **WHEN** final controls are inventoried against the baseline Gamma Ladder
- **THEN** ticker search and shortcuts, strike-window selectors, DTE selectors, refresh, legend explanation, tooltip, and row details remain reachable with compatible names and actions
- **AND** the only new controls are documented visualization or disclosure affordances

#### Scenario: Data identity
- **WHEN** the same Gamma payload is rendered before and after the upgrade
- **THEN** displayed spot, strikes, expiration, regime, net Gamma, call Gamma, put Gamma, strength, and classifications are numerically identical
- **AND** no animation layer recalculates financial values or trading recommendations

### Requirement: User-controlled Playbook header pinning
The Market Pulse Playbook header SHALL remain in normal document flow by default and SHALL provide
an accessible control that lets the user opt into sticky positioning.

#### Scenario: Default page scroll
- **WHEN** no saved pin preference exists
- **THEN** the Playbook header scrolls away with the rest of the page
- **AND** the Gamma Ladder and selected-level inspector retain their own positioning behavior

#### Scenario: Toggle Playbook pinning
- **WHEN** the user activates the Playbook pin control
- **THEN** the header sticky state, control label, and `aria-pressed` value update together
- **AND** the preference is restored on the next Market Pulse visit

### Requirement: Visually segmented Gamma depth board
The depth board SHALL present explicit market-location sections and distinct structural-node
geometry while preserving the accepted strike order and numeric values.

#### Scenario: Rows span both sides of spot
- **WHEN** accepted rows exist above and below the current spot
- **THEN** the board labels the above-spot and below-spot sections and identifies the spot-nearest
  section without relying on color
- **AND** the negative, zero-axis, and positive depth lanes remain aligned across sections

#### Scenario: Structural node comparison
- **WHEN** spot-nearest, flip, and dominant strikes are displayed
- **THEN** each uses a distinguishable marker shape and explicit text label
- **AND** call Gamma, put Gamma, net Gamma, and strike values remain numerically unchanged

### Requirement: Signal emphasis matches structural importance
The ladder SHALL present ordinary minor roles as supporting text and SHALL reserve badge-like
emphasis for scan-critical structural levels without removing row details or interactions.

#### Scenario: Ordinary minor strike
- **WHEN** a rendered strike has minor importance and is not spot-nearest, flip, or dominant
- **THEN** its support, resistance, acceleration, or level role is readable as quiet inline text
- **AND** it does not display stacked button-like importance and role pills

#### Scenario: Scan-critical structural strike
- **WHEN** a strike is a magnet, primary level, Gamma flip, or current spot-nearest level
- **THEN** it displays one compact structural badge with text and a non-color distinction
- **AND** selection exposes the complete role, importance, status, and behavior details

### Requirement: Compact Gamma Ladder command bar
The Gamma Ladder SHALL present a compact control header that keeps symbol switching, search,
current filter context, and refresh visible without displaying every Window and DTE option at once.

#### Scenario: Review current ladder context
- **WHEN** the Gamma Ladder header is visible
- **THEN** the active symbol, spot, regime or freshness, Window, and DTE selections are readable
- **AND** no more than the symbol shortcuts, search, one settings trigger, and refresh appear as
  simultaneous primary controls

#### Scenario: Change Window or DTE
- **WHEN** the user activates the settings trigger
- **THEN** the existing Tight, Standard, Wide, 0DTE, 1DTE, 3DTE, 7DTE, and All choices are available
  in a labeled popover with their pressed and disabled states preserved
- **AND** choosing an option updates the trigger label and refreshes through the existing controller

#### Scenario: Dismiss settings
- **WHEN** the settings popover is open and the user presses Escape, clicks outside, or completes a selection
- **THEN** the popover closes and `aria-expanded` returns to false
- **AND** keyboard focus remains recoverable at the settings trigger

#### Scenario: Narrow command bar
- **WHEN** the viewport is 390 or 768 pixels wide
- **THEN** identity, quick symbols, settings, and refresh reflow without document-level overflow
- **AND** every control retains a visible focus state and readable accessible name
