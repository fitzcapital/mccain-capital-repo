## ADDED Requirements

### Requirement: Point-in-time Gamma evidence for Replay
Setup Replay SHALL use the latest retained Gamma observation known at or before each signal candle.
The regime and dynamic levels MUST come from that observation and MUST retain their source timestamp.
A later Gamma generation MUST NOT change an earlier setup's score, level, or target.

#### Scenario: Later positive regime replaces morning negative regime
- **WHEN** a setup completed while a retained negative-Gamma observation was current and a positive
  observation arrived later
- **THEN** Replay evaluates the setup with the earlier negative regime and its matching levels

#### Scenario: No prior Gamma observation exists
- **WHEN** every available Gamma observation is later than the signal
- **THEN** Replay labels Gamma unavailable, awards no Gamma points, and does not project levels backward

### Requirement: Entry-relative structural Runner
Setup Replay SHALL select a Runner beyond the complete anchor cluster, in the setup direction from
actual entry, and at least five SPX points from actual entry. A candidate failing any condition MUST
be skipped or the Runner marked unavailable.

#### Scenario: Anchor-relative target is behind entry
- **WHEN** a candidate is five points from the anchor but is not favorable from actual entry
- **THEN** Replay rejects it and evaluates the next eligible directional level

#### Scenario: Target is fewer than five points from entry
- **WHEN** the nearest non-cluster level is only four SPX points from entry
- **THEN** Replay does not authorize that level as the Runner

### Requirement: Sourced estimated option-return targets
Replay SHALL calculate illustrative +15%, +20%, and +30% SPX price targets from one valid Tradier NTM
premium and absolute delta reference when available. Otherwise it SHALL use the disclosed $750 and
0.40 fallback. Output MUST identify source, contract, quote timestamp, premium, delta, and fallback
state. Dealer Gamma MUST remain excluded from TP arithmetic.

#### Scenario: Current valid Tradier NTM reference exists
- **WHEN** the current-session Replay receives a timely NTM quote with positive premium, plausible
  delta, and acceptable spread
- **THEN** its TP ladder uses those inputs and identifies the Tradier contract and quote time

#### Scenario: NTM reference is unavailable or invalid
- **WHEN** the quote is missing, stale, crossed, excessively wide, or has invalid delta
- **THEN** Replay uses the $750/0.40 fallback and labels the ladder as an estimate

#### Scenario: Reviewing a prior session
- **WHEN** Replay shows a historical session without a retained same-session option reference
- **THEN** it uses the labeled fallback rather than applying today's contract quote retroactively
