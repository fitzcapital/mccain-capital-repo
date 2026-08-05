## ADDED Requirements

### Requirement: Consistent desktop application chrome

The application SHALL center the shared header and primary navigation within a maximum 1440px
desktop frame regardless of the active page's content-density mode.

#### Scenario: Navigate between standard and wide pages
- **WHEN** a desktop user navigates between a standard page and a wide page
- **THEN** the left and right edges of the shared header and primary navigation remain aligned

#### Scenario: Open a dense workspace
- **WHEN** a desktop user opens a dense workspace wider than 1440px
- **THEN** the shared header remains centered at no more than 1440px while the workspace may extend beyond it

### Requirement: Explicit adaptive content modes

The application SHALL assign each rendered page shell one of three observable content modes:
standard, wide, or dense.

#### Scenario: Render a standard workflow
- **WHEN** a user opens a reading, form, journal, profile, planning, or configuration workflow
- **THEN** its content shell is centered and capped at 1280px on desktop

#### Scenario: Render a wide data workspace
- **WHEN** a user opens Dashboard, Executive, Market Pulse, Candle Opens, or Analytics
- **THEN** its content shell is centered and may use up to 1440px on desktop

#### Scenario: Render a dense workspace
- **WHEN** the application directly renders a page with the dense shell mode on a sufficiently wide desktop viewport
- **THEN** its content shell uses the documented dense cap of up to 1600px

#### Scenario: Preserve the current Budget redirect
- **WHEN** a user requests `/budget`
- **THEN** the application continues redirecting to Executive without changing financial behavior

### Requirement: Readability within wider shells

The application SHALL preserve existing readable line-length and intrinsic card constraints when a
page receives additional horizontal workspace.

#### Scenario: Display prose in a wide workspace
- **WHEN** a wide page contains descriptive prose or guidance text
- **THEN** the text retains its existing character-based maximum width instead of stretching across the full shell

#### Scenario: Display a width-sensitive visualization
- **WHEN** a wide page contains a chart, gamma ladder, calendar, or data table designated to use available space
- **THEN** that visualization may expand to the page shell without causing document-level horizontal overflow

### Requirement: Responsive containment

The application SHALL collapse every shell mode to the available viewport width at tablet and
mobile breakpoints while preserving established page padding and responsive grids.

#### Scenario: View representative pages on tablet
- **WHEN** standard, wide, and dense representative pages render at a tablet viewport
- **THEN** their document width does not exceed the viewport and existing controls remain reachable

#### Scenario: View representative pages on mobile
- **WHEN** standard, wide, and dense representative pages render at a mobile viewport
- **THEN** the shell uses the available width without introducing document-level horizontal scrolling

### Requirement: Functional parity across shell modes

The application MUST preserve existing routes, controls, forms, data hooks, and loading or empty
states when applying adaptive shell widths.

#### Scenario: Use existing page controls
- **WHEN** a user opens any representative standard, wide, or dense page after the shell change
- **THEN** the controls and navigation destinations previously available on that page remain available

#### Scenario: Render missing or delayed data
- **WHEN** a data-heavy page renders an empty, delayed, stale, or loading state
- **THEN** the shell mode and responsive containment remain stable without hiding recovery controls
