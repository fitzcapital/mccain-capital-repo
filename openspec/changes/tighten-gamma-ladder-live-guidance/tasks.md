## 1. Presentation Structure

- [x] 1.1 Add compact lineage, execution-scope, command-sequence, and priority-level nodes to the Gamma Ladder template
- [x] 1.2 Add responsive styling that keeps the new guidance compact on desktop and mobile

## 2. Guidance Behavior

- [x] 2.1 Derive deterministic Now, Confirm, Target, and Fail guidance from accepted SPX ladder rows
- [x] 2.2 Rank up to three priority levels while preserving strike order in the depth map
- [x] 2.3 Auto-select the nearest meaningful level once and preserve valid manual selections on refresh
- [x] 2.4 Gate command guidance on SPX scope, source freshness, and canonical-generation alignment
- [x] 2.5 Clarify hidden-row counts, crossing language, overlapping level roles, and unavailable Prior DTE copy

## 3. Verification

- [x] 3.1 Add focused tests for guidance structure, SPX-only scope, alignment gating, ranking, selection, and copy
- [x] 3.2 Run JavaScript syntax checks, focused pytest coverage, and OpenSpec validation
- [x] 3.3 Rebuild the local Podman app, verify `/healthz`, and inspect the deployed Gamma Ladder receiving surface

## 4. Alignment Hardening

- [x] 4.1 Gate Gamma commands on canonical permission and action state, and prevent live session state from overriding explicit stale freshness
- [x] 4.2 Build and align the bootstrap Gamma Ladder through the same canonical-generation helper used by refresh responses
- [x] 4.3 Add regression coverage for locked permission, explicit stale freshness, and initial combined lineage
- [x] 4.4 Re-run focused validation, rebuild the local app, and verify deployed current/reference behavior

## 5. Poll Transition Coherence

- [x] 5.1 Keep Gamma Ladder root lineage and permission attributes synchronized with each accepted canonical poll
- [x] 5.2 Add focused regression coverage and repeat deployed multi-poll verification

## 6. Bootstrap Race Removal

- [x] 6.1 Initialize the ladder controller from the embedded canonical Gamma payload when its controls match
- [x] 6.2 Add regression coverage, rebuild, and confirm a fresh navigation remains aligned before the first coordinated poll
