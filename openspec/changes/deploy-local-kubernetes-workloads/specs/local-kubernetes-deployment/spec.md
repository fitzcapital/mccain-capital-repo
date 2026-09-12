## ADDED Requirements

### Requirement: Reproducible local cluster
The repository SHALL provide an idempotent setup workflow for kubectl, kind with the Podman provider,
Freelens, and one named local cluster.

#### Scenario: Missing tools
- **WHEN** the setup workflow runs on the supported Apple Silicon Mac
- **THEN** it installs missing tools, preserves installed tools, and creates a usable kubeconfig

#### Scenario: Repeated setup
- **WHEN** the named cluster already exists
- **THEN** setup reuses it without deleting persistent application data

### Requirement: Stable localhost access
The Kubernetes web workload SHALL serve the existing application at `http://localhost:5001` through
a Kubernetes Service and kind host-port mapping.

#### Scenario: Healthy deployment
- **WHEN** the web pod becomes ready
- **THEN** `/healthz` and existing application routes respond through localhost port 5001

### Requirement: Preserved persistent data
The deployment SHALL mount the existing repository `persistent-data/` directory at `/data` and SHALL
not copy, delete, initialize over, or replace existing user data during setup, deploy, or teardown.

#### Scenario: Kubernetes cutover
- **WHEN** the workload is deployed
- **THEN** the application observes the existing database, uploads, books, and secret key

#### Scenario: Cluster cleanup
- **WHEN** the operator removes Kubernetes workloads or the local cluster
- **THEN** repository persistent data remains unchanged and available to the Podman rollback launcher

### Requirement: Bounded laptop resources
Both workloads SHALL declare CPU and memory requests and limits, and the cluster workflow SHALL avoid
running duplicate standalone and Kubernetes runtimes after successful cutover.

#### Scenario: Resource inspection
- **WHEN** the workloads are viewed with kubectl or Freelens
- **THEN** web and worker CPU/memory requests and limits are visible and conform to the documented
  local budget

#### Scenario: Successful cutover
- **WHEN** Kubernetes health and data checks pass
- **THEN** the standalone container is stopped so it consumes no duplicate CPU or memory

### Requirement: Bounded image storage
The deployment workflow SHALL build and load one application image for both pods and SHALL remove
obsolete application revisions and dangling host layers only after the new revision is healthy.

#### Scenario: Two pods use one revision
- **WHEN** web and worker pods are running
- **THEN** both reference the same immutable application image identifier

#### Scenario: Deployment cleanup
- **WHEN** a new revision passes readiness checks
- **THEN** stale cluster revisions and dangling host build layers are pruned while current and rollback
  recovery remain available

### Requirement: Freelens observability
The cluster SHALL appear in Freelens through kubeconfig with both workloads, logs, restarts, and
declared resource controls visible.

#### Scenario: Open local cluster
- **WHEN** Freelens loads the generated kubeconfig context
- **THEN** the operator can inspect both pods and their health without additional cluster credentials
