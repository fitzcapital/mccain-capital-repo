## Why

Market Pulse can receive a fresh chart quote and a freshly rebuilt Gamma Ladder while the canonical
execution generation remains locked on older spot and gamma cache records. This split state blocks
live execution guidance indefinitely even though the required provider data is already present.

## What Changes

- Make canonical refresh consume the same current-session spot and gamma observations used by the
  chart and ladder, subject to the existing freshness and coherence checks.
- Prevent an obsolete cached spot record from overriding a newer current-session observation.
- Promote spot, completed bars, and gamma atomically only after their timestamps and session identity
  validate together; otherwise retain the last valid generation and name the precise blocker.
- Add regression coverage for the observed failure: fresh chart/ladder inputs with a stale canonical
  cache must either promote coherently or expose a real provider failure rather than retry forever.
- Verify that the receiving page advances its last-valid time and clears the false spot/gamma lock
  without a full reload.

Non-goals: changing strategy rules, freshness thresholds, Gamma calculations, provider credentials,
or permitting execution from partially coherent data.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Canonical refresh must reconcile newer current-session spot and
  gamma observations instead of remaining pinned to obsolete component caches.

## Impact

The change affects Market Pulse canonical context assembly and cache promotion under
`mccain_capital/services/`, its authenticated context API, focused regression tests, and the existing
receiving-page reconciliation. Tradier quote and options-chain data remain the external sources; no
new dependency or financial assumption is introduced. Acceptance requires a current-session fixture
with stale persisted component caches to produce one coherent newer generation, current component
ages, an advanced last-valid timestamp, and no false `spot, gamma` execution lock.
