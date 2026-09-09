## 1. Session Resolution

- [x] 1.1 Add a replay-only session resolver for valid completed regular-session candles in Eastern time
- [x] 1.2 Select compatible retained snapshot sources without changing live freshness requirements
- [x] 1.3 Apply the resolved date to replay and durable level reads; preserve explicit-date errors and unavailable states

## 2. Review Presentation

- [x] 2.1 Expose resolved session, review-only status, and evaluated-through time in replay metadata
- [x] 2.2 Render concise date and coverage labels, including partial-session and unavailable states
- [x] 2.3 Keep historical markers session-scoped and review refreshes isolated from live alerts

## 3. Verification

- [x] 3.1 Test Friday-to-Sunday retention, holidays, premarket, early close, and Eastern date boundaries
- [x] 3.2 Test first-new-session candle switching with zero setups, explicit dates, malformed/future rows, and missing sources
- [x] 3.3 Test point-in-time level/session isolation, partial coverage, and no replay-triggered alerts
- [x] 3.4 Run focused pytest, scoped Ruff, relevant JavaScript syntax checks, and git diff --check
- [x] 3.5 Rebuild the local app after implementation approval, check /healthz, and verify dated Friday results on the deployed Sunday replay surface
