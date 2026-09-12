## 1. Maintenance Command

- [x] 1.1 Add a path-safe PVC usage and aged scratch cleanup command
- [x] 1.2 Add focused tests for retention, missing roots, and protected-path rejection

## 2. Kubernetes Integration

- [x] 2.1 Add a non-overlapping 30-minute CronJob with strict resource limits
- [x] 2.2 Extend status tooling with schedule, recent job, and manual-run guidance
- [x] 2.3 Add manifest contracts for schedule, history, resources, and protected paths

## 3. Verification

- [x] 3.1 Run focused tests, Ruff, Black, OpenSpec validation, and diff checks
- [x] 3.2 Apply the CronJob and verify a successful manual run without modifying protected data
