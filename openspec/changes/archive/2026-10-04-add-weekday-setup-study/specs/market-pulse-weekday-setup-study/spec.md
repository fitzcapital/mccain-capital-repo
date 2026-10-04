## ADDED Requirements

### Requirement: Authoritative weekday aggregation
The system SHALL aggregate canonical SPX setup events into ordered Monday-through-Friday cohorts
using the New York trading-session date and the active analytics filters.

#### Scenario: All-history weekday totals reconcile
- **WHEN** a user selects All History without a weekday filter
- **THEN** each canonical setup is counted in exactly one weekday cohort
- **AND** the sum of weekday occurrences equals the filtered total setup count
- **AND** excluded duplicate rows remain reported separately

#### Scenario: Invalid session date is not guessed
- **WHEN** a retained event lacks a valid New York session date
- **THEN** the system excludes it from named weekday cohorts
- **AND** reports an unavailable weekday count for reconciliation

### Requirement: Completed-outcome target rate
The system SHALL calculate weekday target rate as target-reached outcomes divided by completed
outcomes and SHALL exclude open and unavailable outcomes from that denominator.

#### Scenario: Mixed outcome cohort
- **WHEN** a weekday contains target-reached, invalidated, open, and unavailable setups
- **THEN** the displayed fraction and percentage use only target-reached plus invalidated outcomes
- **AND** open and unavailable counts remain visible in coverage details

#### Scenario: No completed outcomes
- **WHEN** a weekday has occurrences but no completed outcomes
- **THEN** the system displays `No completed outcomes`
- **AND** does not display a zero-percent target rate

### Requirement: Evidence-aware weekday leaders
The system SHALL identify the best setup family and best setup-family-plus-time combination within
each weekday using completed outcomes, Wilson-adjusted target rate, evidence maturity, excursion
quality, sample size, and deterministic tie-breaking.

#### Scenario: Small perfect sample
- **WHEN** a weekday cohort has a raw 100% target rate from fewer than five completed outcomes
- **THEN** it is labeled single-example or early evidence according to its completed count
- **AND** raw percentage alone does not determine the winning cohort

#### Scenario: Established leader
- **WHEN** an eligible cohort has at least five completed outcomes
- **THEN** its evidence label states `Established for this sample`
- **AND** its leader summary includes the completed fraction and raw target rate

#### Scenario: No eligible winner
- **WHEN** a weekday has no setup family with a completed outcome
- **THEN** the system names no best setup or combination
- **AND** explains that evidence is not yet available

### Requirement: Weekday filtering and drill-down
The system SHALL let users filter Setup Analytics by weekday and drill from weekday leaders into the
supporting family and time selection while preserving the active date horizon.

#### Scenario: Apply weekday filter
- **WHEN** a user selects Wednesday
- **THEN** headline metrics, leaders, charts, family comparisons, weekday study, and ledger all use
  only Wednesday sessions within the active horizon
- **AND** the active filter is visible and removable

#### Scenario: Study a weekday leader
- **WHEN** a user activates a weekday's best setup-plus-time control
- **THEN** the existing family and time filters select the supporting setups
- **AND** the active date horizon and weekday remain unchanged

### Requirement: Compact and accessible presentation
The system SHALL present weekday results in a compact responsive comparison and SHALL make
definitions and secondary details available without permanently crowding the page.

#### Scenario: Default weekday summary
- **WHEN** the weekday study renders with results
- **THEN** each weekday shows its target rate, completed fraction, evidence label, best setup, and
  best time in a scannable layout
- **AND** secondary metrics are available through keyboard-accessible details or helpers

#### Scenario: Narrow viewport
- **WHEN** the Setup Analytics page is viewed on a narrow screen
- **THEN** weekday summaries stack without clipped titles, overlapping labels, or horizontal page
  overflow

### Requirement: Gamma coverage remains explicit
The system SHALL disclose captured and unavailable gamma context for weekday selections and SHALL
not infer missing historical gamma values.

#### Scenario: Sparse weekday gamma samples
- **WHEN** a weekday has insufficient captured gamma context
- **THEN** the study labels gamma coverage as insufficient
- **AND** does not publish a best weekday-by-gamma conclusion

### Requirement: Backward-compatible analytics response
The system SHALL add weekday analytics without removing or changing the meaning of existing Setup
Analytics response fields.

#### Scenario: Existing consumer omits weekday
- **WHEN** an existing client requests Setup Analytics without a weekday parameter
- **THEN** existing metrics, charts, leaders, comparisons, ledger, and filter options remain
  available with their prior meanings
- **AND** the weekday study is supplied as an additive field
