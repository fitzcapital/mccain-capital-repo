## ADDED Requirements

### Requirement: Stream-first SPX visual tape
The Market Pulse page SHALL consume valid SPX ticks from the existing shared market stream and SHALL
render them into bounded five-second visual points on a compact tape with an independent time scale,
without opening a second browser stream connection or adding timestamps to the candle chart.

#### Scenario: Fresh stream tick reaches the chart
- **WHEN** the shared market stream publishes a valid, current SPX tick during the live session
- **THEN** the hero visual tape accepts the tick without waiting for the next quote poll
- **AND** the chart identifies the active transport as streaming

#### Scenario: Tape receives second-level timestamps
- **WHEN** the compact tape renders observations between completed five-minute candles
- **THEN** the main candle chart retains its original visible range and candle spacing
- **AND** seconds remain disabled on the candle chart time axis

#### Scenario: Repeated ticks share a visual bucket
- **WHEN** multiple valid SPX ticks arrive within one five-second bucket
- **THEN** the chart retains only the newest observation for that bucket
- **AND** the visual point count remains bounded

### Requirement: Deterministic polling fallback
The hero chart SHALL retain the existing lightweight quote lane and SHALL use it as the visual-tape source whenever the shared stream is unavailable, stale, interrupted, or reconnecting.

#### Scenario: Stream becomes stale
- **WHEN** no current SPX stream observation arrives within the configured stale window during market hours
- **THEN** the chart changes its transport state to polling fallback
- **AND** valid quote-poll observations continue updating the visual tape

#### Scenario: Stream resumes
- **WHEN** a valid current SPX stream observation arrives after polling fallback
- **THEN** the chart returns to streaming without duplicating points or listeners

#### Scenario: Page visibility changes
- **WHEN** the page becomes hidden and later visible
- **THEN** unnecessary visual updates pause while hidden
- **AND** the chart resumes from the newest valid source without a full-page reload

### Requirement: Observation and strategy authority separation
Stream and quote ticks SHALL remain observation-only, and setup confirmation SHALL continue to use completed five-minute candles and canonical key levels.

#### Scenario: Tick crosses a key level before candle completion
- **WHEN** an SPX stream or quote tick crosses a canonical key level but no supported completed five-minute pattern exists
- **THEN** the visual tape and spot display may update
- **AND** the system MUST NOT confirm or arm a setup from that tick alone

#### Scenario: Stream is unavailable
- **WHEN** the stream cannot provide a valid SPX observation
- **THEN** the failure MUST NOT alter the last canonical setup decision
- **AND** the chart SHALL disclose the fallback or interrupted state

### Requirement: Stream observation validation
The chart SHALL reject invalid, stale, closed-session, or timestamp-regressing stream observations and SHALL preserve visible gaps instead of inventing intermediate prices.

#### Scenario: Invalid stream payload
- **WHEN** a stream payload has no positive SPX price or no usable provider timestamp
- **THEN** the chart ignores that observation
- **AND** polling fallback remains available

#### Scenario: Market is closed
- **WHEN** the refresh contract marks the market session closed
- **THEN** stream-fed visual updates stop
- **AND** the chart retains the last valid session without presenting it as live
