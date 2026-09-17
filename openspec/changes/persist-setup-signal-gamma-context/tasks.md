## 1. Signal-Time Gamma Contract

- [ ] 1.1 Normalize Gamma regime, observation timestamp, source, and status from only observations
  available at or before the setup signal.
- [ ] 1.2 Freeze the normalized Gamma context into accepted Setup Replay events and keep rejected or
  future observations from becoming historical evidence.
- [ ] 1.3 Add replay tests for eligible, missing, malformed, and future-only Gamma observations.

## 2. Durable Analytics Persistence

- [ ] 2.1 Add an additive migration for typed Gamma fields and a regime index on
  `market_pulse_setup_events`.
- [ ] 2.2 Extend canonical record normalization and inserts to validate and persist signal-time
  Gamma while retaining evidence JSON.
- [ ] 2.3 Preserve frozen Gamma during outcome/MFE/MAE updates and allow only provenance-safe
  enrichment of previously empty fields.
- [ ] 2.4 Add fresh-database, existing-database, canonicalization, and immutable-update tests.

## 3. Gamma-Aware Setup Analytics

- [ ] 3.1 Add normalized Gamma filtering, filter values, coverage counts, and per-regime comparison
  data to the analytics payload and cache identity.
- [ ] 3.2 Add a compact signal-time Gamma filter, coverage explanation, and ledger provenance detail
  to Setup Analytics.
- [ ] 3.3 Ensure unavailable Gamma remains in overall frequency counts but outside named-regime
  performance claims.
- [ ] 3.4 Add API, aggregation, page-contract, and JavaScript tests for captured and unavailable
  Gamma cohorts.

## 4. Verification and Deployment

- [ ] 4.1 Run focused Replay, migration, Setup Analytics, and application contract tests.
- [ ] 4.2 Run Ruff/Black checks for changed Python, JavaScript syntax checks, `git diff --check`, and
  strict OpenSpec validation.
- [ ] 4.3 Deploy through the local Kubernetes path after approval; verify `/healthz`, pod readiness,
  migrated columns, point-in-time Gamma capture, and the receiving Setup Analytics page.
- [ ] 4.4 Record coverage limitations explicitly: legacy setups without proven signal-time Gamma
  remain unavailable and are never backfilled from current snapshots.
