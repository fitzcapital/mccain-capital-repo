## ADDED Requirements

### Requirement: Analytics identifies historical leaders for the active selection
The system SHALL compute a best setup family, best 30-minute New York time window, and best
setup-family/time-window combination from the exact setup rows selected by the active preset and
advanced filters.

#### Scenario: All History displays three leaders
- **WHEN** the user selects All History and completed outcomes exist
- **THEN** the page displays the best setup, best time, and best setup/time combination for the
  all-history selection
- **AND** every leader displays its target successes, completed-outcome denominator, and raw target
  rate

#### Scenario: Changing the horizon recomputes all leaders
- **WHEN** the user changes from All History to Today, three sessions, this week, or another preset
- **THEN** all leader cards are recomputed from that same active selection
- **AND** the displayed horizon label identifies the scope used

### Requirement: Leader ranking accounts for evidence strength
The system SHALL rank cohorts using completed outcomes and an evidence-adjusted target measure so a
tiny perfect sample does not win solely because its raw rate is 100 percent.

#### Scenario: Larger reliable cohort outranks tiny perfect cohort
- **WHEN** one cohort is 2 of 2 and another sufficiently sampled cohort has a stronger
  evidence-adjusted result
- **THEN** the sufficiently sampled cohort ranks above the tiny perfect cohort
- **AND** both cohorts retain their raw fractions and percentages for inspection

#### Scenario: Ranking ties are deterministic
- **WHEN** two cohorts have the same evidence-adjusted target measure
- **THEN** the system compares established maturity, favorable-versus-adverse movement, completed
  sample size, and a stable label tie-breaker in documented order

#### Scenario: Established non-performer does not mask a stronger early cohort
- **WHEN** an established cohort has no target successes and an early cohort has a higher Wilson
  lower bound
- **THEN** the stronger early cohort is shown as the leader
- **AND** its Early evidence label remains visible

### Requirement: Evidence maturity is explicit
The system SHALL label each leader as Established evidence, Early evidence, Single example, or
Unavailable according to its completed-outcome count.

#### Scenario: Established leader has five completed outcomes
- **WHEN** a winning cohort has at least five completed outcomes
- **THEN** the page labels it "Established for this sample"
- **AND** still displays the exact completed denominator

#### Scenario: Only early evidence exists
- **WHEN** no eligible cohort has five completed outcomes but one or more completed outcomes exist
- **THEN** the page displays the provisional leader with Early evidence or Single example wording
- **AND** does not describe it as proven or statistically significant

### Requirement: Leader cards disclose reward and risk context
The system SHALL display median favorable SPX movement and median adverse SPX movement for every
available leader without using hypothetical option profit as a ranking input.

#### Scenario: Trader compares move quality
- **WHEN** a leader has captured excursion data
- **THEN** its card displays median favorable and median adverse move in SPX points
- **AND** the ranking explanation states that estimated option profit is not part of the ranking

### Requirement: Leaders provide consistent drill-down
The system SHALL allow the user to filter the entire analytics workspace to the supporting setup
family, time window, or combined cohort while preserving the active date horizon.

#### Scenario: Study best combined cohort
- **WHEN** the user activates "Study these setups" on the best setup/time card
- **THEN** the existing family and time filters are applied together
- **AND** KPIs, charts, family cards, and ledger reflect the same narrowed selection

### Requirement: Missing outcome evidence does not fabricate a winner
The system SHALL withhold performance leaders when the active selection contains no completed
outcomes and SHALL explain the unavailable evidence.

#### Scenario: Selection has no completed outcomes
- **WHEN** the selected rows are open or historically unevaluated only
- **THEN** the page states that no best-performing setup can be measured yet
- **AND** it reports awaiting-resolution and historical-unavailable counts separately
- **AND** it does not treat either group as losses

### Requirement: Analytics counts canonical actionable setups
The system SHALL count a stored setup once when multiple records share the same session, signal
candle, setup family, direction, and traded level.

#### Scenario: Pattern aliases describe one trade decision
- **WHEN** 2-1-2 and 2-2 records identify the same candle, family, direction, and level
- **THEN** every analytics metric, chart, leader, family card, and ledger counts one setup
- **AND** the canonical row preserves the strongest available outcome and excursion evidence

#### Scenario: Distinct trade decisions remain distinct
- **WHEN** records differ by signal candle, family, direction, or traded level
- **THEN** Analytics counts them separately

#### Scenario: Historical evidence remains auditable
- **WHEN** Analytics suppresses a duplicate from reporting
- **THEN** the original stored records remain unchanged
- **AND** the response reports how many duplicate rows were excluded

### Requirement: Normal analytics use does not generate image files
The system SHALL render Setup Analytics from HTML, CSS, JavaScript, and stored numeric evidence
without creating screenshots or other image artifacts during normal page requests.

#### Scenario: User opens or filters Setup Analytics
- **WHEN** the page or analytics API is requested
- **THEN** no screenshot or documentation image is written
- **AND** screenshot generation remains limited to explicit capture, diagnostic, or user-upload flows
