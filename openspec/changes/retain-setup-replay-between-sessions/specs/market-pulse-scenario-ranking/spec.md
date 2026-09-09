## ADDED Requirements

### Requirement: Replay retains the latest available trading session
When no explicit session date is requested, Setup Replay SHALL resolve its session from the latest
available valid completed regular-session candles, in Eastern time. It SHALL retain that session
through weekends, holidays, and premarket until a newer session supplies a completed candle.
Retention MUST NOT relax live-data freshness or mix source context across sessions.

#### Scenario: Sunday retains Friday
- **WHEN** Sunday Aug 30 has no trading session and the source retains Friday Aug 28 candles
- **THEN** Replay evaluates Aug 28 and shows its eligible setups after refresh

#### Scenario: Holiday or premarket has no new completed candles
- **WHEN** the market calendar is closed or the next session has no completed regular-session bar
- **THEN** Replay retains the latest available valid session with its original date

#### Scenario: Next session begins with no setup
- **WHEN** the next session supplies its first completed regular-session candle
- **THEN** Replay switches to that session even if no eligible setup exists and removes prior-session
  results from the default view

#### Scenario: Missing or malformed source
- **WHEN** no compatible persisted source contains valid completed regular-session candles
- **THEN** Replay returns an explicit unavailable state without inventing setups or a completed day

### Requirement: Retained replay identifies date and coverage
Replay SHALL expose and render its resolved session date and latest evaluated candle. Retained or
closed-session results MUST be labeled `Review only` and MUST NOT produce live setup alerts or
modify live execution permissions. Historical chart markers MUST retain their original timestamps.

#### Scenario: Partial Friday snapshot
- **WHEN** Friday's available candles end at 1:15 PM ET and are reviewed on Sunday
- **THEN** the summary identifies Friday and review-only status with coverage through 1:15 PM ET
  rather than implying full-day results

#### Scenario: Review refresh is repeated
- **WHEN** retained replay is loaded or refreshed repeatedly
- **THEN** no notification, trade, journal entry, or live permission change is produced

### Requirement: Explicit replay dates remain authoritative
Replay SHALL honor an explicitly requested valid session date and SHALL scope candles and durable
level observations to that date. It MUST NOT substitute a different date when data is unavailable.

#### Scenario: Requested session is not available
- **WHEN** a valid explicit date has no compatible retained candle source
- **THEN** Replay identifies that date as unavailable rather than returning another day's setups

#### Scenario: Invalid date
- **WHEN** the explicit date is invalid
- **THEN** the endpoint preserves its date-validation error behavior
