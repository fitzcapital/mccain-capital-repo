## 1. Dashboard State Contract

- [x] 1.1 Trace the dashboard view context and every rendered import-readiness reference
- [x] 1.2 Normalize import readiness into one presentation state without changing import behavior
- [x] 1.3 Add focused coverage for complete, pending, and unavailable import states

## 2. Decision Hierarchy

- [x] 2.1 Consolidate command, context, and permission summaries into one Command Center
- [x] 2.2 Merge Today's Decision and Daily Brief decision content into one Execution Plan
- [x] 2.3 Remove repeated decision prose and duplicate execution CTA rows
- [x] 2.4 Apply exception-first copy so healthy states remain compact and blockers remain explicit

## 3. Progressive Disclosure

- [x] 3.1 Collapse Foundation, Review, Health, Calendar, and extended supporting content by default
- [x] 3.2 Preserve keyboard access, status summaries, responsive layout, and no-reload expansion
- [x] 3.3 Update dashboard styling for the consolidated hierarchy without changing the visual theme

## 4. Verification

- [x] 4.1 Add or update focused dashboard rendering and interaction contract tests
- [x] 4.2 Run focused pytest, static syntax checks, and OpenSpec strict validation
- [x] 4.3 Rebuild the local Podman app and verify health plus the receiving dashboard surface
