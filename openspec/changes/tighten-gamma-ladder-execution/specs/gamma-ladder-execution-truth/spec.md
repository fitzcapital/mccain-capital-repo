## ADDED Requirements

### Requirement: Session-aware Gamma data truth
The Gamma Ladder SHALL classify each accepted snapshot as live session, after-hours last-valid,
stale, degraded, or unavailable using market-session state and independent quote, options-chain,
and expiration timestamps. A newer quote timestamp MUST NOT make an older or expired options basket
appear live.

#### Scenario: Regular session data is coherent
- **WHEN** the market is open and quote and options-chain inputs are within their live thresholds
- **THEN** the ladder labels the snapshot Live and displays both input timestamps and expiration date

#### Scenario: Market is closed with a valid prior snapshot
- **WHEN** the market is closed and the last completed-session Gamma snapshot is otherwise valid
- **THEN** the ladder labels it After-hours planning and Last valid session rather than Current or Live

#### Scenario: Fresh quote overlays an expired chain
- **WHEN** a quote updates after hours but the selected 0DTE chain has expired
- **THEN** the chain remains labeled Expired session and cannot inherit the quote's freshness state

#### Scenario: Required timing metadata is missing
- **WHEN** quote time, chain time, expiration, or session identity cannot be validated
- **THEN** the affected field is Unavailable and the ladder cannot claim a live snapshot

### Requirement: Session-aware expiration selection
During the live session the default Gamma Ladder basket SHALL use the configured 0DTE expiration
when available. After that expiration's trading session closes, the default SHALL move to the next
tradable expiration while preserving the expired basket as explicitly labeled prior-session
research.

#### Scenario: Live 0DTE session
- **WHEN** the regular session is open and today's expiration is available
- **THEN** 0DTE remains the default accepted basket

#### Scenario: Session closes
- **WHEN** today's options session has ended
- **THEN** the next tradable expiration becomes the default planning basket and the former 0DTE
  basket is available only as Expired session or Prior session

#### Scenario: Next expiration is unavailable
- **WHEN** no validated next tradable expiration can be loaded after the session closes
- **THEN** the ladder retains the last valid basket with degraded labeling and does not silently
  relabel expired data as the next session

### Requirement: Locally validated Gamma Flip
The primary Gamma Flip SHALL be derived from a positive-to-negative net-GEX sign transition within
a configured spot-local window. Both adjacent sides MUST meet minimum magnitude requirements, and
the result SHALL expose distance, source rows, and confidence. An isolated zero-GEX row MUST NOT by
itself qualify as the primary flip.

#### Scenario: Meaningful local sign transition exists
- **WHEN** adjacent local strikes cross net GEX sign with sufficient magnitude on both sides
- **THEN** the ladder interpolates the flip, reports its inputs and confidence, and may promote it
  into the execution map

#### Scenario: Distant row has zero GEX
- **WHEN** a distant strike has approximately zero GEX without a qualifying adjacent sign transition
- **THEN** it is labeled a distant zero-GEX or low-relevance row and is not the primary Gamma Flip

#### Scenario: No qualified local transition exists
- **WHEN** the local window contains no sign transition satisfying distance and magnitude rules
- **THEN** the primary flip is Unavailable and no replacement value is fabricated

#### Scenario: Multiple local transitions qualify
- **WHEN** more than one local transition satisfies the rules
- **THEN** the closest qualifying transition wins deterministically and the alternatives remain
  available as secondary structure

### Requirement: One compact Gamma execution map
Before the strike board, the ladder SHALL present one compact execution map containing session
state, immediate decision level, upside magnet or resistance, downside failure level, expected
range, and next required price evidence. Those values MUST originate from one accepted Gamma
generation and MUST NOT imply trade permission.

#### Scenario: Positive-Gamma planning map
- **WHEN** valid positive-Gamma structure contains support below and a magnet above spot
- **THEN** the map names the support as the immediate decision level, the magnet as the upside
  level, the next valid support as the downside failure level, and the bounded expected range

#### Scenario: Negative-Gamma structure
- **WHEN** the accepted structure is negative Gamma
- **THEN** the map emphasizes acceleration boundaries and required acceptance or rejection evidence
  instead of presenting a stabilization range

#### Scenario: A map input is unavailable
- **WHEN** a decision, upside, downside, range, or evidence input cannot be validated
- **THEN** that map field says Unavailable and no duplicate summary invents a substitute

#### Scenario: Supporting ladder sections render
- **WHEN** the execution map is visible
- **THEN** key-level cards, structure summary, and Top Levels do not repeat the same primary values
  as competing first-tier surfaces

### Requirement: Decision-relevant strike disclosure
The default strike board SHALL display no more than nine accepted strikes ranked by proximity to
spot, absolute Gamma magnitude, and structural role, while preserving source values and strike
order. The user SHALL be able to reveal the full accepted window explicitly.

#### Scenario: Accepted window has many strikes
- **WHEN** more than nine strikes are available
- **THEN** the default view shows the decision-relevant set and reports how many additional strikes
  are hidden

#### Scenario: Full ladder is requested
- **WHEN** the user activates Show full ladder
- **THEN** every accepted strike in the selected window is shown without recalculating financial values

#### Scenario: Filters change
- **WHEN** symbol, expiration, DTE, or window changes
- **THEN** relevance ranking recomputes from the new accepted payload and stale selections clear

### Requirement: Canonical Gamma synchronization
The Gamma Ladder SHALL participate in the Market Pulse canonical generation and shared refresh
coordinator. Automatic checks SHALL use cached accepted data without forcing providers; forced
manual refresh SHALL remain available, share the concurrency gate, and update all Gamma execution
surfaces coherently without document reload.

#### Scenario: New cached Gamma generation arrives
- **WHEN** the visible page detects a newer accepted Gamma generation
- **THEN** timing labels, execution map, relevant strikes, full ladder data, and chart selection
  context update coherently in place

#### Scenario: Gamma generation is unchanged
- **WHEN** an automatic check returns the rendered Gamma generation
- **THEN** the ladder remains stable without replaying motion or resetting selection and scroll state

#### Scenario: Forced refresh succeeds
- **WHEN** the user activates the authoritative refresh control and providers return valid data
- **THEN** the canonical page and Gamma Ladder advance together through the shared receiving boundary

#### Scenario: Refresh fails
- **WHEN** automatic or forced refresh fails while a last valid Gamma snapshot exists
- **THEN** the prior ladder remains visible with explicit degraded status and no partial generation
  replaces it

### Requirement: Compatibility and execution safety
The tightened ladder SHALL preserve existing ticker, search, window, expiration/DTE, row-inspection,
chart-selection, keyboard, reduced-motion, responsive, stale, empty, and error behavior. It SHALL
NOT add order entry, trade authorization, risk sizing, predictive direction, or provider changes.

#### Scenario: Existing controls are exercised
- **WHEN** a user switches symbols or filters, selects a row, clears selection, or opens the full board
- **THEN** compatible control behavior and chart coordination remain available

#### Scenario: Gamma level changes visually
- **WHEN** a newer accepted payload changes a level, regime, or ranking
- **THEN** motion communicates the data change only and never labels the strategy actionable

#### Scenario: Narrow or reduced-motion environment
- **WHEN** the page renders at 390 or 768 pixels or reduced motion is requested
- **THEN** the execution map and strike board remain usable without document overflow or required animation
