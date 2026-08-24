## Why

Market Pulse can identify scenarios and reconstruct earlier potential setups, but it does not yet
provide a durable, unambiguous live callout when a setup progresses toward confirmation. Relying on
the page being open, polling at the right moment, or interpreting Setup Replay after the fact is not
reliable enough for live execution support.

The app needs one canonical live setup monitor that survives refreshes, advances only from validated
completed-candle evidence, and alerts once when a genuinely actionable SPX setup confirms.

## What Changes

- Add a server-owned live setup lifecycle: `WATCHING`, `ARMED`, `CONFIRMED`, `TARGET_REACHED`,
  `INVALIDATED`, and `EXPIRED`.
- Bind every live setup state to the canonical Market Pulse generation, session, ticker, scenario,
  level, and evidence timestamps so a stale or mixed payload cannot produce an alert.
- Add one persistent, highest-priority Live Setup Monitor callout to Market Pulse. Lower-ranked
  candidates remain available in a collapsed watch list rather than competing for attention.
- Notify only on a new transition into `CONFIRMED` when the candidate is at least B quality, all
  required data is fresh, execution permission is safe, and the confirmation occurs before a
  configurable late-day cutoff.
- Persist setup identity and alert-delivery state so polling, reloads, retries, and multiple app
  workers cannot duplicate a setup or notification.
- Show exact location, trigger, confirmation, action, invalidation, target, freshness, and setup age
  in the live callout. If required data becomes stale, preserve the last known setup as paused
  context and remove action authorization.
- Keep Setup Replay historical and read-only. Replay may explain earlier signals and outcomes, but it
  cannot create a live alert or authorize an entry.
- Add in-app alert delivery through the existing notification surface, with optional sound/browser
  notification only after user permission and a visible mute control.

### Non-goals

- No automated order placement, broker execution, position sizing, or trade creation.
- No alerts based on intrabar crosses, incomplete candles, stale gamma, or replay outcomes.
- No strategy expansion beyond the existing proven SPX scenario families and ranking rules.
- No multi-symbol live setup monitoring in this change.
- No SMS, email, or third-party push service in the initial implementation.

## Capabilities

### New Capabilities

- `market-pulse-live-setup-alerting`: Canonical SPX setup lifecycle, durable highest-priority live
  callout, freshness and time gates, idempotent alert delivery, and restart/reload recovery.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Include live setup state and alert eligibility in the same
  atomic canonical generation and freshness contract as execution guidance.
- `market-pulse-scenario-ranking`: Define how the top live setup is selected and how late-day or
  stale candidates remain reviewable without becoming actionable alerts.

## Impact

- Server: canonical Market Pulse context/API, scenario evaluation, setup lifecycle persistence, and
  notification integration.
- Client: a compact Live Setup Monitor, collapsed secondary watch list, alert acknowledgement/mute,
  and atomic in-place refresh handling.
- Data: a small durable setup-event and notification ledger keyed by stable setup identity; no trade
  ledger writes.
- Tests: lifecycle transitions, completed-candle gating, freshness locks, ranking, cutoff behavior,
  deduplication across retries/workers, restart recovery, and client reconciliation.
- Operations: expose last evaluation, state age, next evaluation, and alert-delivery status so polling
  failures are visible rather than silent.

### Acceptance criteria

- A qualifying SPX candidate advances through deterministic lifecycle states from canonical data and
  produces no entry-oriented alert before completed-candle confirmation.
- A new B-or-better confirmed setup produces exactly one in-app alert across repeated polls, manual
  refreshes, page reloads, and concurrent app workers.
- A stale, incoherent, locked, after-cutoff, or replay-only setup produces no actionable alert and
  clearly states why.
- Reloading or reopening Market Pulse restores the current live setup and its acknowledgement state
  without replaying an already-delivered alert.
- The page displays only one primary live callout, ranked by lifecycle state first and confluence
  quality second, while preserving access to lower-ranked watches.
- Setup Replay remains historical, outcome-aware only after the signal, and unable to mutate the
  live monitor, notification ledger, trade ledger, or journal.
