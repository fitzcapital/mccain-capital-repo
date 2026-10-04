## 1. PID Isolation and Monitoring

- [x] 1.1 Configure a finite per-pod PID ceiling in the kind template
- [x] 1.2 Add node task usage and pressure classification to the laptop monitor

## 2. External Recovery

- [x] 2.1 Add a conservative local Kubernetes recovery guard with failure threshold and cooldown
- [x] 2.2 Add a macOS LaunchAgent installer and uninstaller for the guard
- [x] 2.3 Add focused tests for PID configuration, recovery safety, and monitor output

## 3. Controlled Migration

- [x] 3.1 Validate scripts and OpenSpec artifacts
- [x] 3.2 Back up the database and recreate only the named local kind cluster
- [x] 3.3 Install the guard and verify pod PID limits, workload health, persistent mounts, and site response
