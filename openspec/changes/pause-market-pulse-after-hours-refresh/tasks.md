## 1. Refresh Contract

- [x] 1.1 Add explicit automatic-refresh permission and next-session-open timing to the API contract
- [x] 1.2 Suppress automatic provider refresh requests when the regular session is closed
- [x] 1.3 Preserve one-shot manual diagnostic refresh behavior after hours

## 2. Page Scheduling

- [x] 2.1 Route every canonical scheduling path through a session-aware guard
- [x] 2.2 Pause the canonical polling lane while closed and schedule one next-session wake check
- [x] 2.3 Replace the after-hours countdown and live sync copy with a clear paused-session state

## 3. Verification

- [x] 3.1 Add focused API tests for open, closed, and manual after-hours behavior
- [x] 3.2 Add contract/static tests for closed-session client scheduling and resume behavior
- [x] 3.3 Run focused tests, syntax checks, OpenSpec validation, rebuild the local app, and verify health
