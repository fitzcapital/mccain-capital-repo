## 1. Live Tape Contract

- [x] 1.1 Add five-second cadence, gap, point-bound, and authority fields to the stream-session payload
- [x] 1.2 Add focused service contract tests for the live-tape configuration

## 2. Bounded Tape Model

- [x] 2.1 Implement a standalone timestamped five-second accumulator with replacement, bounds, and gap breaks
- [x] 2.2 Add JavaScript unit coverage for fresh, duplicate-bucket, bounded, gap, hidden, closed, and resumed states

## 3. Market Pulse Integration

- [x] 3.1 Add the compact five-second toggle and visual-only status treatment to the hero chart
- [x] 3.2 Feed accepted quote responses into a distinct bounded chart line series without changing candle or strategy state
- [x] 3.3 Add template and integration contract coverage for the non-authoritative hierarchy

## 4. Verification

- [x] 4.1 Run focused Python and JavaScript tests plus syntax and diff checks
- [x] 4.2 Rebuild the local Podman application and verify health and deployed Market Pulse assets
- [x] 4.3 Inspect the receiving page for aligned controls, live state labeling, and browser-console errors
