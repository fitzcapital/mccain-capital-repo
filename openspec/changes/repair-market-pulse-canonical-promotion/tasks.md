## 1. Canonical input reconciliation

- [x] 1.1 Trace spot and gamma candidate provenance through the authenticated context refresh
- [x] 1.2 Make newer valid current-session spot observations replace stale values and timestamps atomically
- [x] 1.3 Reconcile the canonical gamma candidate with the current Gamma Ladder source before promotion
- [x] 1.4 Preserve fail-closed behavior for missing, stale, malformed, and cross-session inputs

## 2. Regression coverage

- [x] 2.1 Add focused tests for stale cached spot with a fresh provider observation
- [x] 2.2 Add focused tests for stale cached gamma with a fresh coherent ladder source
- [x] 2.3 Verify incoherent newer observations remain locked with precise blockers

## 3. Runtime verification

- [x] 3.1 Run focused Market Pulse tests and static validation
- [x] 3.2 Rebuild the local Podman application and verify `/healthz`
- [x] 3.3 Verify the authenticated Market Pulse page advances last-valid time and clears false blockers without reload
