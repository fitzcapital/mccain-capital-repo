## 1. Diagnose Resource Ownership

- [x] 1.1 Add sanitized worker task, file-descriptor, baseline, and high-water diagnostics
- [x] 1.2 Build a focused refresh-path soak harness and identify the component retaining threads or pipes
- [x] 1.3 Capture the confirmed root cause and steady-state resource bound in the design notes

## 2. Correct and Bound the Worker

- [x] 2.1 Correct the identified provider, browser, stream, executor, or subprocess cleanup path
- [x] 2.2 Add configurable consecutive-sample task thresholds and clean nonzero worker self-exit
- [x] 2.3 Make heartbeat checks and local status output distinguish freshness from unsafe capacity
- [x] 2.4 Keep last-good Market Pulse snapshots readable during worker replacement

## 3. Verification and Deployment

- [x] 3.1 Add focused unit and soak tests for stable tasks, cleanup after failure, and capacity recovery
- [x] 3.2 Run targeted pytest, Python formatting/lint checks for touched files, and `git diff --check`
- [x] 3.3 Rebuild and deploy the local Kubernetes image without modifying persistent data
- [x] 3.4 Observe multiple refresh intervals and verify worker capacity, `/healthz`, chart candles,
  gamma/target timestamps, and setup replay freshness

## 4. Correct Gamma Renderer Retention

- [x] 4.1 Replace automatic server-side gamma PNG rendering with opt-in export
- [x] 4.2 Add regression coverage proving normal gamma export does not invoke the renderer
- [x] 4.3 Rebuild and deploy without modifying persistent data
- [x] 4.4 Observe multiple gamma refresh intervals with stable tasks and current Market Pulse data
- [x] 4.5 Verify the setup monitor checks the current canonical candle before returning unchanged
