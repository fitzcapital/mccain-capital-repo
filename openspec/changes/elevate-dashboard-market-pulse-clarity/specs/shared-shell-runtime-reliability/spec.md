## ADDED Requirements

### Requirement: Shared notification shell initializes without ordering errors
The shared application shell SHALL initialize notification last-read formatting only after all
required formatter functions are available.

#### Scenario: Authenticated primary page loads with stored last-read data
- **WHEN** an authenticated primary page loads and notification last-read state exists
- **THEN** the shell formats the value without a JavaScript initialization exception

#### Scenario: No stored last-read value exists
- **WHEN** an authenticated primary page loads without notification last-read state
- **THEN** the shell displays the existing never-read state and completes notification initialization

### Requirement: Fixed mobile navigation reserves content clearance
Pages using the shared fixed mobile navigation SHALL reserve bottom space at least equal to the
navigation footprint plus safe-area inset so actionable page content is not obscured.

#### Scenario: Actionable control is near the end of a mobile page
- **WHEN** a user scrolls to the final actionable control at a 390-pixel viewport
- **THEN** the control can be fully viewed and activated above the fixed navigation

#### Scenario: Device exposes a bottom safe-area inset
- **WHEN** the browser reports a bottom safe-area inset
- **THEN** the reserved clearance includes that inset without introducing horizontal overflow
