## Context

Market Pulse already produces ranked SPX scenarios, an authoritative execution state, canonical
generation metadata, bounded refresh status, and point-in-time-safe Setup Replay records. The missing
piece is a durable live setup lifecycle. Today the browser can show the current scenario, but it does
not own enough history to distinguish a newly confirmed setup from the same setup returned by another
poll, a reload, or another worker. Setup Replay solves a different problem: it explains historical
signals after their signal times and must not become an alert source.

The primary stakeholder is a single local trader who keeps TradingView open for discretionary chart
reading and wants the app to provide a consistent second callout. Reliability and conservative failure
behavior matter more than alert volume. The Flask app may run multiple workers, browser refreshes can
overlap, and provider data can be partial or stale.

## Goals / Non-Goals

**Goals:**

- Derive one durable SPX live setup lifecycle from canonical completed-candle data.
- Surface one highest-priority live setup with exact evidence and explicit actionability.
- Deliver a confirmed-setup alert once, despite repeated polling, reloads, retries, or multiple workers.
- Preserve the last known setup during temporary data failure while removing action authorization.
- Make last evaluation, next evaluation, state age, freshness, and alert delivery observable.
- Keep historical Setup Replay isolated from live monitoring and financial ledgers.

**Non-Goals:**

- Automated trade execution, broker writes, position sizing, or risk authorization.
- New SPX strategy families, scoring weights, or intrabar confirmation rules.
- Monitoring SPY, QQQ, or other symbols for live setup alerts.
- SMS, email, or remote push infrastructure.
- Reclassifying historical Replay outcomes as real trades or performance.

## Decisions

### 1. The server owns the lifecycle

Add a focused live-setup monitor service under `mccain_capital/services/` that consumes the already
validated canonical Market Pulse generation. It derives `WATCHING`, `ARMED`, `CONFIRMED`,
`TARGET_REACHED`, `INVALIDATED`, or `EXPIRED` from the same scenario evidence used by the execution
guide. The client only renders the returned state.

This avoids browser timing differences and allows state to survive page reloads. A client-only state
machine was rejected because missed polls, sleeping tabs, and reloads would create duplicate or missed
alerts.

### 2. Completed five-minute bars are the transition clock

Lifecycle advancement occurs only when a newer completed five-minute candle or a new canonical lock
condition can change the state. Quote-only updates may update distance and observation text but cannot
confirm, invalidate, or retarget a setup.

This matches existing scenario rules and prevents noisy intrabar alerts. Faster quote-triggered alerts
were rejected because they would weaken the established evidence contract.

### 3. Stable identity separates a setup from an alert delivery

Each candidate receives a stable setup key derived from session date, ticker, scenario family,
direction, normalized level role/value, and the first qualifying evidence bar. Before confirmation,
the monitor also keeps a stable watch identity for that family/level. State events include canonical
generation id and evidence timestamps.

Alert delivery uses a separate idempotency key derived from setup key, transition, and channel. This
lets the same setup advance through lifecycle states while preventing a repeated `CONFIRMED` alert.

Using only generation id was rejected because every poll can create a new generation for one setup.
Using only family and direction was rejected because separate same-day levels would collide.

### 4. Persist a compact operational ledger through the repository layer

Add a repository-managed live-setup state and notification ledger using the app's existing durable
application-data pattern and atomic writes/transactions. Store only setup identity, lifecycle state,
evidence times, canonical generation, acknowledgement, and channel delivery status. Do not write to
the trade ledger, journal, Replay cache, or personal runtime files during migration.

Updates use compare-and-set semantics or a unique idempotency constraint so concurrent workers cannot
deliver the same transition twice. The implementation will first inspect and reuse the repository's
existing durable JSON/SQLite abstraction rather than add a new dependency.

An in-memory cache was rejected because restart and multi-worker behavior would be unreliable.

### 5. Alerting is gated, conservative, and transition-based

A live alert is eligible only when all of the following hold:

- ticker is SPX;
- state transitions into `CONFIRMED` for the first time;
- primary scenario score is at least 76 (B range);
- canonical permission is execution-safe and required quote, completed bars, gamma, and levels are
  coherent and fresh;
- confirmation is based on completed-candle evidence;
- confirmation time is at or before the configured late-day cutoff, default 3:15 PM ET; and
- the alert idempotency key has not already been delivered.

After the cutoff, the setup can remain visible as `LATE / REVIEW ONLY` and later appear in Replay, but
it cannot emit an actionable alert. The cutoff is configuration-backed with a safe default, not a UI
field that can silently change the strategy during a session.

Score-only, quote-only, Replay-derived, stale, and locked alerts are rejected.

### 6. Rank one primary callout by actionability before score

The monitor selects one primary setup using lifecycle priority first (`CONFIRMED`, `ARMED`,
`WATCHING`), then existing lane priority, confluence score, proximity, scenario-family priority, and
stable identity. Terminal states remain accessible in recent history but do not displace a current
live candidate. Secondary candidates render in a collapsed, best-to-least watch list.

This reuses deterministic scenario ranking while preventing several equally prominent cards from
creating conflicting execution cues.

### 7. The UI has one persistent monitor and explicit alert controls

Place a compact Live Setup Monitor below the Alternative and Dormant Watch row and above Setup Replay.
Its collapsed header always shows state, direction, level, grade, age, freshness, and the next
evaluation countdown. Expanded content shows location, trigger, confirmation, action, invalidation,
target, evidence checklist, and alert history.

The existing in-app notification/bell surface receives a structured confirmed-setup event. Optional
sound or browser notification requires explicit browser permission and has a persistent mute control.
Acknowledgement stops repeated presentation but does not alter setup state.

### 8. Refresh reconciliation is atomic and monotonic

The canonical Market Pulse response includes `live_setup_monitor` in the same staged client commit as
execution state and scenario ranking. The client rejects older setup revisions, never moves a terminal
setup backward, and never synthesizes a confirmation locally. A manual refresh joins the existing
single-flight cycle.

If the canonical generation cannot promote, the last setup remains visible as paused/stale with action
language removed. The countdown targets the next server-authoritative evaluation time; it does not
claim a successful data refresh in advance.

### 9. Replay remains a separate, read-only projection

Setup Replay may consume finalized lifecycle facts as historical context only after the session-time
signal exists, but Replay cannot publish to the live monitor or notification channel. No lifecycle or
alert action creates a trade, journal entry, or P&L result.

## Risks / Trade-offs

- **[Provider data remains stale]** → Preserve the last setup, label it paused, suppress alerts, name
  each blocker, and retry through the existing bounded coordinator.
- **[Two workers evaluate the same bar]** → Use stable keys plus atomic compare-and-set/idempotency
  writes before delivery.
- **[A strong setup confirms shortly after the cutoff]** → Keep it visible as review-only and allow
  Replay to assess it; do not weaken the live-execution time gate.
- **[Browser notifications are blocked]** → The in-app monitor and bell remain authoritative; browser
  sound/notification is optional enhancement only.
- **[Persistence write fails]** → Fail closed: show the setup without marking it alert-safe, report
  operational degradation, and do not deliver an alert that cannot be deduplicated.
- **[Ranking changes hide a prior confirmed setup]** → Retain recent terminal/current events in the
  expanded history and promote only according to deterministic actionability ordering.
- **[Additional state increases API payload]** → Return one primary setup, compact secondary summaries,
  and bounded recent history.

## Migration Plan

1. Add lifecycle models, stable identity helpers, repository storage, and unit tests behind a disabled
   feature flag.
2. Add canonical API fields and server evaluation while keeping the UI hidden; verify no financial
   ledger writes and bounded polling behavior.
3. Enable the monitor UI with in-app notifications muted by default and browser notifications off.
4. Run focused lifecycle, concurrency, API, and client tests; rebuild the local Podman app; verify
   `/healthz`, canonical endpoint behavior, and the receiving Market Pulse page.
5. Enable in-app alert delivery after a soak interval confirms one event per transition and stable
   worker counts.

Rollback disables the feature flag and removes the UI integration. Persisted operational records can
remain inert because they are isolated from trades, journals, and financial metrics.

## Open Questions

- Confirm whether the initial late-day cutoff should remain 3:15 PM ET or become a different fixed
  value after live observation.
- Confirm whether sound should default off (recommended) or inherit the existing notification setting.
