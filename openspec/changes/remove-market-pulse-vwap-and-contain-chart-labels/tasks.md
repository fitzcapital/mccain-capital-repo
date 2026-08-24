## 1. Remove VWAP Domain and Canonical Data

- [x] 1.1 Remove VWAP calculation, proxy/provider fallback, and canonical payload assembly
- [x] 1.2 Remove VWAP from scenario inputs and renormalize declared confluence weights to 100
- [x] 1.3 Remove obsolete VWAP service helpers and provider-leg selection when no other consumer remains
- [x] 1.4 Update domain, provider, payload, and score tests for the VWAP-free contract

## 2. Remove VWAP Presentation

- [x] 2.1 Remove the VWAP chart series, canonical reconciliation, visibility preference, and toggle
- [x] 2.2 Remove the VWAP scenario card, template bindings, and dedicated styling
- [x] 2.3 Update JavaScript and route contract tests to prohibit VWAP surfaces and preserve chart state

## 3. Contain Chart Annotations

- [x] 3.1 Position the current-session badge below the upper plot boundary
- [x] 3.2 Reserve right-side plot space and constrain execution-level labels inside the chart boundary
- [x] 3.3 Add chart contract tests for top and right annotation containment across refresh and timeframe changes

## 4. Verification

- [x] 4.1 Run focused Market Pulse scenario, chart, snapshot, streaming, and route tests
- [x] 4.2 Run Ruff, JavaScript syntax checks, strict OpenSpec validation, and `git diff --check`
- [x] 4.3 Rebuild the local Podman app and verify `/healthz`
- [x] 4.4 Verify the receiving page has no VWAP data or UI and reports no chart console errors
