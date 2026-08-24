## Why

Market Pulse now has canonical generations, coordinated polling, conservative execution locks, and
durable setup alerts, but its reliability is still proved mainly by focused tests and short manual
checks. Because the page supports live SPX execution decisions, it needs repeatable failure
simulation, visible operational health, exchange-session accuracy, and measurable soak evidence
before the user can depend on it throughout a trading session.

## What Changes

- Add a deterministic Market Pulse resilience harness covering provider timeout, stale or malformed
  components, network loss, tab suspension, overlapping tabs, worker contention, restart recovery,
  and persistence failure.
- Add a compact operational-health watchdog showing the last successful canonical generation,
  latest check, retry deadline, consecutive failures, component blockers, worker agreement, and
  setup-ledger health without exposing credentials or raw provider payloads.
- Replace weekday-only session calculations with an exchange-calendar-aware contract for holidays
  and early closes while retaining server authority over whether automatic polling is enabled.
- Add authenticated end-to-end receiving-page tests proving atomic updates, reload equivalence,
  countdown truth, failure locking, automatic recovery, and duplicate-alert suppression.
- Add a bounded local soak report that records reliability outcomes across many polling and setup
  transitions and clearly reports whether the session met its acceptance thresholds.
- Preserve manual Refresh data as a forced fallback; do not make it the primary freshness method.
- Keep strategy definitions, scenario scoring, broker execution, financial ledgers, and non-SPX
  instruments out of scope.

## Capabilities

### New Capabilities

- `market-pulse-operational-resilience`: Fault simulation, operational-health reporting,
  exchange-session scheduling, soak evidence, and recovery acceptance criteria for Market Pulse.

### Modified Capabilities

- `market-pulse-live-state-coherence`: Extend the canonical page contract with a compact watchdog
  state and require reload-equivalent, automatically recoverable receiving-page behavior.

## Impact

- Affects Market Pulse services, repositories, API response metadata, the refresh coordinator,
  Market Pulse template and styles, and focused Python/JavaScript/browser tests.
- May add a lightweight exchange-calendar dependency or a repository-owned session-calendar adapter;
  the design will select the smallest reliable local-only option.
- Adds local structured reliability records only; it does not store orders, account credentials,
  provider payloads, journal entries, or P&L.
- Acceptance requires zero mixed-generation commits, zero duplicate confirmed-setup alerts, immediate
  fail-closed behavior for required-component faults, automatic recovery without page reload, correct
  holiday/early-close polling state, and a passing deterministic soak run.
