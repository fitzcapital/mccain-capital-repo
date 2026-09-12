## Why

The local Kubernetes deployment needs visible storage health and bounded automatic cleanup without granting a pod access to host Podman or risking application records. This complements the existing host image cleanup with PVC-aware maintenance.

## What Changes

- Add a low-resource Kubernetes CronJob that reports persistent-volume utilization every 30 minutes.
- Delete only aged files inside explicit scratch directories; preserve the database, uploads, books, backups, and Market Pulse evidence.
- Add status and manual-run commands to the local Kubernetes tooling.
- Add manifest and safety-contract tests.
- Non-goals: pruning host Podman images from Kubernetes, deleting user data, or modifying business records.

## Capabilities

### New Capabilities

- `k8s-storage-maintenance`: Bounded PVC monitoring and allowlisted scratch cleanup for the local cluster.

### Modified Capabilities

None.

## Impact

- Affects `k8s/base.yaml`, local Kubernetes status tooling, documentation, and focused tests.
- Uses the existing application image and PVC; no new dependency or external data source.
- Acceptance: scheduled and manual jobs succeed, stay within resource limits, report disk usage, and cannot target protected paths.
