## Context

Market Pulse already promotes immutable canonical generations, separates observation quotes from
execution authority, coordinates browser polling, preserves last-valid data, and persists setup-alert
identity. The remaining risk is operational: short focused tests do not yet prove behavior through
provider faults, browser suspension, process contention, restarts, holiday schedules, or extended
polling. The app is local-only on an Apple Silicon Mac and must remain useful without Redis, cloud
monitoring, broker writes, or remote alert infrastructure.

## Goals / Non-Goals

**Goals:**

- Make every important failure mode deterministic and repeatable in tests.
- Give the user one quiet but truthful view of data-pipeline health and recovery.
- Use one exchange-session calendar for polling, wake times, holidays, and early closes.
- Prove reload equivalence, single-flight recovery, monotonic setup state, and alert idempotency.
- Produce sanitized soak evidence with explicit pass/fail thresholds.

**Non-Goals:**

- Changing SPX strategy families, evidence order, scoring, targets, or risk rules.
- Adding automated trade execution, another backend, Redis, cloud monitoring, or public hosting.
- Supporting live execution for instruments other than SPX.
- Persisting raw provider responses, credentials, orders, account values, or P&L.
- Treating faster observation quotes as execution permission.

## Decisions

### 1. Add injectable reliability boundaries around existing services

Introduce narrow test adapters at provider fetch, canonical-envelope persistence, alert-ledger
persistence, and clock/session boundaries. Production defaults call the existing implementations;
tests inject timeout, malformed, stale, contention, and I/O failures.

This is preferred over adding failure query parameters to production endpoints, which could expose
unsafe controls in the live app. Broad monkeypatch-only tests were rejected because they do not
exercise the same cross-layer recovery path.

### 2. Build the watchdog from existing canonical metadata

Extend the canonical response with a compact `operational_health` object containing status,
generation id, last successful promotion, last attempt, consecutive failures, retry deadline,
blocking components, worker adoption, and persistence state. The client renders this object and does
not independently infer health from timers or text.

The healthy presentation remains one line. Warning and critical states expand with the exact blocker
and recovery action. This avoids another always-open diagnostics panel while still making stale data
immediately explainable.

### 3. Centralize the existing market calendar without a new dependency

Extract the repository's existing US equity holiday rules into one service and add scheduled early
closes, session open/close, current phase, and next-session calculations. Core, UI, gamma, and Market
Pulse use the shared adapter. Tests use an injected Eastern timestamp.

This is smaller and more deterministic for a local-only application than adding a large dataframe
calendar dependency. The adapter will expose a conservative override table for exceptional closures;
unknown calendar failure disables automatic execution polling rather than assuming the market is open.

### 4. Make the resilience harness test-only and layered

Use service-level tests for canonical promotion and persistence, API tests for response and worker
contracts, JavaScript tests for coordinator deadlines, and authenticated browser tests for the final
receiving page. A bounded runner composes representative faults rather than running uncontrolled
infinite chaos.

Every injected fault asserts the same invariants: retain last valid, lock execution, suppress alerts,
name the blocker, keep one flight, recover automatically, and never regress generation or setup state.

### 5. Store only bounded sanitized reliability events

Maintain an in-memory ring plus an optional small atomic local report file under a test/output path,
never the financial database or user data directories. Events contain timing, generation, component,
outcome, and sanitized reason. The page receives only the latest summary; full soak details remain a
developer/test artifact.

### 6. Define hard acceptance thresholds

The deterministic suite and soak runner fail on any mixed-generation commit, duplicate confirmed
alert, unsafe unlocked state, terminal-state regression, unreleased request, or recovery requiring a
page reload. Bounded recovery MUST occur within the server-advertised retry schedule plus test jitter
tolerance. Worker adoption and countdown deadlines MUST agree with the canonical response.

## Risks / Trade-offs

- **[Fault hooks leak into production]** → Keep injection adapters dependency-controlled and reject
  request-driven fault selection outside test configuration.
- **[Calendar rules drift]** → Centralize rules, test known holidays and early closes, permit explicit
  exceptional-closure overrides, and fail closed when calendar resolution fails.
- **[Watchdog becomes visual noise]** → Keep healthy state compact and expand only for warning or
  critical conditions.
- **[Soak tests become slow or flaky]** → Use injected clocks and deterministic delays for CI; reserve
  a separate bounded real-time local soak for deployment verification.
- **[Instrumentation captures sensitive data]** → Allowlist fields and test that credentials, raw
  payloads, orders, account values, and financial records cannot appear.
- **[Multiple workers disagree briefly]** → Continue using the durable canonical envelope and report
  adoption lag without publishing an unverified candidate.

## Migration Plan

1. Extract and test the shared market-session calendar while preserving current public behavior.
2. Add operational-health aggregation and API metadata behind the existing canonical endpoint.
3. Render the compact watchdog and verify healthy, warning, critical, paused, and recovered states.
4. Add injectable fault boundaries and deterministic service/API/JavaScript tests.
5. Add authenticated receiving-page resilience tests and the bounded soak runner.
6. Rebuild the Podman app, verify `/healthz`, run the deterministic soak, and observe a bounded local
   live-session soak before declaring the feature proven.

Rollback hides the watchdog and restores the existing session adapter while leaving additive API
fields harmless. No financial-data migration is required.

## Open Questions

- The first implementation will use the existing fixed 3:15 PM ET actionable-alert cutoff; changing
  that strategy cutoff remains a separate evidence-based decision.
- Real-time soak duration should default to a short verification run and accept a longer explicit
  session mode rather than making ordinary rebuilds wait through market hours.
