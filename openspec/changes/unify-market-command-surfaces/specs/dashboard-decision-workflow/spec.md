## MODIFIED Requirements

### Requirement: Consolidated command hierarchy
The dashboard SHALL present one visually dominant Today decision containing the current trading
permission, concise reason, and one next required action, with operating state, market context, and
account-risk state rendered as supporting evidence.

#### Scenario: Dashboard initially renders
- **WHEN** a user opens the dashboard
- **THEN** the Today decision, reason, and next action are available in one scan-first region
- **AND** supporting command values remain available without presenting a competing primary summary

#### Scenario: A command value is blocked
- **WHEN** alignment, permission, market data, or account risk is blocked or unavailable
- **THEN** the Today decision identifies the authoritative blocker and required next action
- **AND** supporting evidence identifies its individual cause without creating another primary CTA

#### Scenario: A command value is healthy
- **WHEN** a supporting command value requires no attention
- **THEN** it renders as a compact status without routine explanatory copy or persistent glow

### Requirement: Progressive supporting detail
The dashboard SHALL keep the Today decision, next action, and execution-critical market context
visible while placing Foundation, Review, Health, Calendar, broker metrics, and extended explanatory
material behind accessible disclosure controls or drawers.

#### Scenario: Supporting content initially renders
- **WHEN** a user opens the dashboard
- **THEN** supporting sections expose a concise label and useful current status
- **AND** their extended content and secondary actions are collapsed by default

#### Scenario: Supporting content is expanded
- **WHEN** a user activates a supporting disclosure using pointer or keyboard input
- **THEN** its existing detailed content and actions become available without a full-page reload
