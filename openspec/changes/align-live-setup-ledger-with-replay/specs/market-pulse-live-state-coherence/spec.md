## ADDED Requirements

### Requirement: Complete live setup event persistence
The Live Setup Monitor SHALL evaluate each newly completed canonical five-minute candle in chronological order and SHALL durably persist every qualifying triggered setup event, regardless of its current scenario rank. Primary selection SHALL control presentation priority only and MUST NOT determine whether an eligible event is recorded.

#### Scenario: Multiple setups trigger on one candle
- **WHEN** two or more distinct scenarios qualify on the same newly completed candle
- **THEN** each setup SHALL receive exactly one durable event record and the highest-ranked current candidate alone SHALL be selected as Primary

#### Scenario: Secondary setup qualifies
- **WHEN** a qualifying setup ranks below another current candidate
- **THEN** the setup SHALL remain persisted and visible in secondary or recent history rather than being discarded

#### Scenario: Evaluation repeats or candidates reorder
- **WHEN** the same completed candles are evaluated again or candidate rank changes
- **THEN** stable event identity SHALL prevent duplicate setup records, revisions, and alerts

#### Scenario: Worker resumes after missed completed candles
- **WHEN** live evaluation resumes with one or more unprocessed current-session completed candles
- **THEN** those candles SHALL be processed chronologically and eligible past events SHALL be persisted as review-only without emitting a late real-time alert

#### Scenario: Ledger commit fails
- **WHEN** event persistence fails before the completed-candle checkpoint commits
- **THEN** the previous checkpoint SHALL remain authoritative, execution SHALL fail closed, and a retry SHALL safely reprocess the same delta without duplication

#### Scenario: Existing event reaches a terminal state
- **WHEN** a persisted setup reaches target, invalidation, or expiration
- **THEN** subsequent evaluation SHALL preserve its terminal state, acknowledgement, and alert-delivery history monotonically

### Requirement: Frozen event facts and replay-consistent lifecycle
Each durable Live event SHALL preserve the family, pattern, level, direction, and trigger facts from
its stable shared setup event. Live SHALL resolve each persisted event's lifecycle from completed
candles using Replay's target-touch and close-based invalidation semantics. Current Primary ranking
MUST NOT overwrite those frozen facts or downgrade a terminal event.

#### Scenario: Current Primary uses another family
- **WHEN** a persisted `failed_high` event exists and the current Primary is a `breakdown`
- **THEN** the historical event remains `failed_high` while Primary retains its separate family

#### Scenario: Replay invalidates an existing event
- **WHEN** a completed candle closes through the event's invalidation boundary
- **THEN** Live and Replay resolve the same event id as invalidated at that candle time

#### Scenario: Open setup approaches but does not touch target
- **WHEN** price remains valid and reaches less than the full target distance
- **THEN** Live and Replay preserve the event as open without marking target reached

#### Scenario: Reconciliation repeats
- **WHEN** lifecycle reconciliation runs again for an open or terminal event
- **THEN** event facts, acknowledgements, alert delivery, and terminal state remain idempotent
