## 1. Bound Gamma Refresh Work

- [x] 1.1 Add a process-local nonblocking singleflight gate around Gamma computation
- [x] 1.2 Preserve the cross-process shared lock and return last-good in-progress state to losing callers
- [x] 1.3 Replace per-refresh Tradier expiry thread pools with bounded serial fetching
- [x] 1.4 Preserve refresh timeouts, last-good fallback, and execution-lock behavior

## 2. Contain Runtime Threads

- [x] 2.1 Set native BLAS, OpenMP, MKL, and NumExpr thread caps in the container runtime
- [x] 2.2 Configure graceful Gunicorn max-request recycling with jitter
- [x] 2.3 Keep worker-count, thread-count, and recycling settings configurable with safe defaults

## 3. Diagnose and Explain Pressure

- [x] 3.1 Add low-cost current-process thread count and warning-threshold diagnostics
- [x] 3.2 Expose Gamma attempt, error, in-progress, and resource-pressure state through operational health
- [x] 3.3 Display `Waiting on Gamma` when Gamma is the specific synchronization blocker
- [x] 3.4 Preserve concise multi-component synchronization copy and normal session labels when healthy

## 4. Verification and Recovery

- [x] 4.1 Add concurrent background/forced refresh singleflight tests
- [x] 4.2 Add repeated refresh thread-growth and serial expiry-fetch tests
- [x] 4.3 Add runtime environment, worker recycling, health diagnostic, and session-label contract tests
- [x] 4.4 Run focused pytest, scoped Ruff, and `git diff --check`
- [x] 4.5 Rebuild Podman to clear exhausted workers and verify `/healthz`, bounded thread counts, Gamma refresh, and Market Pulse receiving state

## 5. Publish Local Monitoring Events

- [x] 5.1 Expose stable numeric Gamma freshness/failure and worker-pressure dimensions for Netdata
- [x] 5.2 Add local Netdata warning/critical alarms with sustained thresholds and recovery hysteresis
- [x] 5.3 Include service, current value, threshold, and concise cause in event details
- [x] 5.4 Add event-contract tests covering warning, critical, recovery, and duplicate suppression
- [x] 5.5 Verify the deployed Netdata Events feed records a forced alarm transition and recovery
