## Why

Market Pulse currently mixes data-health warnings with normal optional-feed states and uses ambiguous timestamps and setup labels. This makes a healthy page appear degraded and forces traders to interpret terse or malformed execution guidance.

## What Changes

- Base the top-level trust verdict on required execution feeds; show optional stale or event-driven feeds as advisories.
- Distinguish the last completed five-minute candle from the live forming bar.
- Render Fast Tape event times in New York time.
- Replace terse monitor countdowns and malformed 2-2 labels with readable status text.
- Make the reversal checklist result distinct from its five evidence inputs.
- Clarify no-direction and Gamma confirmation/failure language.
- Non-goals: no changes to setup detection, target calculation, market-data ingestion, or trade execution rules.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Clarify live health, timestamps, monitor state, checklist semantics, and Gamma guidance without changing underlying signals.

## Impact

- Affects Market Pulse operational-health presentation, live page template/JavaScript, and focused contract tests.
- Uses existing Spot, Bars, Gamma, Options, Strategy, and Fast Tape data only; no financial assumptions or new dependencies.
- Acceptance: required-current feeds produce a healthy trust verdict despite optional advisories; visible times identify their data meaning and timezone; setup and Gamma labels read correctly; focused tests pass.
