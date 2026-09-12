## 1. Point-in-time evidence

- [x] 1.1 Preserve level observation timestamps through normalization and replay output
- [x] 1.2 Persist bounded, deduplicated Gamma observations from accepted canonical generations
- [x] 1.3 Select the latest Gamma observation known at or before each replay signal

## 2. Target calculations

- [x] 2.1 Validate Runner targets directionally and at least five SPX points from entry
- [x] 2.2 Use a valid current-session Tradier NTM reference for TP estimates with an explicit fallback
- [x] 2.3 Keep Gamma as confluence only and expose compact source/timestamp diagnostics

## 3. Verification

- [x] 3.1 Add focused tests for Gamma time travel, level timestamps, Runner distance, and TP sourcing
- [x] 3.2 Run targeted replay and core tests plus formatting checks
- [x] 3.3 Deploy through local Kubernetes and verify health and receiving API behavior
