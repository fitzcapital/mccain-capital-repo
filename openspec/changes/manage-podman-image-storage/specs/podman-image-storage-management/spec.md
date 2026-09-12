## ADDED Requirements

### Requirement: Read-only storage status
The storage manager SHALL default to read-only status output and SHALL identify total image usage,
active images, dangling cleanup candidates, and Podman-reported reclaimable storage.

#### Scenario: Run without arguments
- **WHEN** an operator runs the storage manager without arguments
- **THEN** it reports storage and candidate information without invoking a prune or removal command

#### Scenario: Podman is unavailable
- **WHEN** the configured Podman executable cannot connect to its image store
- **THEN** the command exits nonzero with a concise connection error and performs no deletion

### Requirement: Explicit and bounded cleanup
The storage manager SHALL require `--apply` before deleting images and SHALL restrict normal cleanup
to dangling images older than the configured retention period.

#### Scenario: Preview cleanup
- **WHEN** an operator requests cleanup without `--apply`
- **THEN** the manager lists the intended policy and candidates without deleting images

#### Scenario: Apply cleanup
- **WHEN** an operator requests cleanup with `--apply`
- **THEN** the manager invokes a forced, noninteractive Podman image prune limited to dangling images
  older than the configured retention period

#### Scenario: Protected resources
- **WHEN** cleanup is applied
- **THEN** running images, tagged current and rollback images, containers, volumes, and persistent
  application data remain untouched

### Requirement: Deployment rollback protection
The deployment workflow SHALL preserve the previously tagged application image as one rollback image
before replacing the current application tag.

#### Scenario: Existing current image
- **WHEN** deployment begins and the current application image exists
- **THEN** that image is tagged with the configured rollback tag before the new build replaces it

#### Scenario: First deployment
- **WHEN** deployment begins without an existing current image
- **THEN** deployment continues without creating a rollback tag or failing

### Requirement: Post-health automatic maintenance
The deployment workflow SHALL invoke conservative image maintenance only after the replacement
container passes its health check.

#### Scenario: Healthy replacement
- **WHEN** the new container passes `/healthz`
- **THEN** the workflow invokes automatic dangling-image cleanup with the configured retention period

#### Scenario: Failed replacement
- **WHEN** the new container does not pass `/healthz`
- **THEN** automatic cleanup is not invoked and deployment exits nonzero

#### Scenario: Cleanup failure after healthy deployment
- **WHEN** automatic cleanup fails after the new container is healthy
- **THEN** the workflow reports the cleanup failure without stopping the healthy container

### Requirement: Observable cleanup result
Applied cleanup SHALL show the storage summary before and after pruning and SHALL clearly state the
retention policy used.

#### Scenario: Cleanup completes
- **WHEN** an applied cleanup command finishes
- **THEN** the output contains before and after Podman storage summaries plus the retention period

### Requirement: Concurrent operation safety
The storage manager SHALL prevent two applied maintenance operations from running concurrently.

#### Scenario: Lock is already held
- **WHEN** an applied cleanup starts while another applied cleanup owns the maintenance lock
- **THEN** the new cleanup exits without invoking Podman prune and reports that maintenance is busy
