## Context

The canonical setup evaluator is called while building a Market Pulse page or context API response.
The browser owns the 15-second request cadence and intentionally suspends it while hidden. This
protects providers from idle polling, but it also makes detection and notification dependent on an
open, visible page. Replay later evaluates the retained completed bars and therefore exposes setups
that the live monitor did not observe in their alert window.

The existing setup builder, live evaluator, durable JSON alert ledger, analytics repository,
canonical snapshot service, exchange calendar, and operational-health model already provide most of
the required behavior. The change should reuse those components and avoid a second interpretation of
the strategy.

## Goals / Non-Goals

**Goals:**

- Evaluate completed SPX five-minute bars throughout each regular exchange session without a browser.
- Keep live, replay, analytics, and alert records on one deterministic setup identity and outcome.
- Deliver at most one eligible alert and retain late-discovered setups without presenting them live.
- Fail closed on stale required data, persistence failure, clock discontinuity, or lost ownership.
- Make monitor coverage and failures visible through sanitized health fields and reliability events.

**Non-Goals:**

- Changing Strat pattern definitions, targets, scores, or gamma interpretation.
- Polling outside regular exchange sessions or executing orders.
- Replacing page-side canonical rendering or its hidden-tab resource controls.
- Migrating or rewriting existing personal runtime data.

## Decisions

### 1. Add one server-owned monitor lifecycle

The application runtime will start one daemon-style monitor per process through the existing runtime
initialization pattern. The monitor derives session permission and cadence from the shared exchange
calendar, sleeps outside the session, and performs bounded checks during the regular session.

Alternative considered: keep browser polling and send a notification after Replay recovery. That
would preserve evidence but still miss the decision window, so it does not meet the live-alert goal.

### 2. Reuse canonical collection and evaluation under single flight

Each monitor check will obtain the same coherent canonical generation used by the context API, then
call the existing setup builder and evaluator. Existing refresh locking prevents duplicate provider
work. The monitor will not introduce a second bar collector or strategy implementation.

Alternative considered: query raw providers directly in the monitor. This is faster to prototype but
would permit live and replay inputs to diverge and would bypass current freshness controls.

### 3. Treat completed candle identity as the progress cursor

The heartbeat persists the newest evaluated completed candle timestamp and setup IDs. Repeated checks
against an unchanged candle remain cheap and idempotent. On recovery, retained bars are evaluated in
order; setups older than the live eligibility window are stored as late review and never back-alerted.

### 4. Keep delivery monotonic and durable

The existing durable ledger remains the source of alert idempotency and terminal setup state. The
monitor and page may both evaluate, but only the first atomic ledger transition can create a delivery
event. Analytics backfill consumes the same ledger after successful evaluation.

### 5. Separate monitor health from page freshness

Operational health gains additive fields for ownership, heartbeat age, last attempt, last success,
last evaluated candle, consecutive failures, clock status, and last sanitized error. A healthy page
cannot mask an unhealthy server monitor. A stale heartbeat during the regular session raises a
degraded event; outside the session it is correctly reported as sleeping.

### 6. Detect clock discontinuity before evaluating alert eligibility

The monitor compares monotonic elapsed time with wall-clock movement. A material discontinuity forces
canonical revalidation and marks the cycle as recovery. It does not deliver alerts until the server
clock and current exchange session reconcile.

## Risks / Trade-offs

- [Multiple application workers start monitors] → Use process ownership metadata plus durable alert
  idempotency; expose worker count and ownership conflict in health.
- [More provider traffic] → Evaluate on the server cadence only during the regular session, reuse
  single-flight caches, and skip unchanged completed candles.
- [Laptop sleep creates a large gap] → Reconcile retained bars in order and classify old setups as
  late review instead of issuing stale alerts.
- [Runtime shutdown interrupts a write] → Preserve atomic ledger writes and update heartbeat only
  after evaluation persistence completes.
- [Current application factory is imported by tests] → Disable automatic monitor startup under test
  configuration and test the lifecycle through an injectable clock and one-cycle runner.

## Migration Plan

1. Add monitor state and a deterministic one-cycle service with no automatic startup.
2. Add runtime lifecycle wiring behind an enabled configuration flag.
3. Add health and reliability-event fields, then focused tests and a bounded soak.
4. Enable the monitor by default locally, rebuild the container, and verify page-closed detection.
5. Roll back by disabling the flag; the additive ledger and health fields remain compatible.

## Open Questions

None. The existing 15-second canonical cadence, regular-session calendar, alert cutoff, and setup
eligibility rules remain authoritative.
