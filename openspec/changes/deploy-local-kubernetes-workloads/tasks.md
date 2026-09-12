## 1. Runtime Role Separation

- [x] 1.1 Add validated standalone, web, and worker runtime-role configuration
- [x] 1.2 Prevent web-role application startup from launching any background loops
- [x] 1.3 Add a dedicated worker entrypoint for sync, backup, dispatch, and setup monitoring
- [x] 1.4 Add worker heartbeat/readiness and single-owner protection
- [x] 1.5 Add focused tests for role isolation, compatibility, and duplicate-worker prevention

## 2. Kubernetes Manifests

- [x] 2.1 Add a one-node kind configuration with Podman provider, host port 5001, and data mount
- [x] 2.2 Add namespace, persistent volume, claim, Service, and immutable configuration manifests
- [x] 2.3 Add one web Deployment and one worker Deployment using the same image revision
- [x] 2.4 Add startup/readiness/liveness probes and explicit CPU/memory requests and limits
- [x] 2.5 Add manifest validation tests for replicas, storage, ports, probes, roles, and resources

## 3. Local Lifecycle Tooling

- [x] 3.1 Add an idempotent installer for kubectl, kind, Freelens, and the local cluster
- [x] 3.2 Add a deployment script that builds once, loads once, applies manifests, and waits for readiness
- [x] 3.3 Add a status command covering pods, worker ownership, localhost health, disk, CPU, and memory
- [x] 3.4 Add safe cleanup and teardown commands that preserve `persistent-data/`
- [x] 3.5 Add a tested rollback command that restores the standalone Podman runtime

## 4. Cutover and Verification

- [x] 4.1 Install missing local tools and size the existing Podman VM to the documented ceiling
- [x] 4.2 Create the kind cluster and verify its kubeconfig context in kubectl and Freelens
- [x] 4.3 Deploy both workloads and verify one shared image revision plus persistent-data visibility
- [x] 4.4 Verify worker ownership, health probes, resource budgets, and representative app routes
- [x] 4.5 Cut over localhost port 5001, stop the standalone container, and verify no duplicate runtime load
- [x] 4.6 Verify image/cache cleanup and record before/after laptop disk, CPU, and memory state
- [x] 4.7 Run focused tests, syntax checks, OpenSpec strict validation, and `git diff --check`

## 5. Operator Guidance

- [x] 5.1 Document Freelens navigation, install/deploy/status/log/restart/cleanup/rollback commands
- [x] 5.2 Document storage ownership, resource limits, single-worker rules, and troubleshooting boundaries
