## ADDED Requirements

### Requirement: Local Kubernetes live resource telemetry
The local Kubernetes deployment SHALL install a pinned Metrics Server configuration that exposes
current CPU and memory usage for McCain Capital pods while preserving the configured workload
resource limits when telemetry is unavailable.

#### Scenario: Metrics become available after deployment
- **WHEN** the local Kubernetes deployment completes successfully
- **THEN** the Metrics API becomes available and `kubectl top pods` reports the web and worker pods

#### Scenario: Metrics are still initializing
- **WHEN** Metrics Server is installed but has not produced samples yet
- **THEN** the resource monitor reports that the Metrics API is not ready without marking application
  resource limits as unenforced

#### Scenario: Metrics Server is absent
- **WHEN** the Metrics API service is not installed
- **THEN** the resource monitor identifies the missing component rather than calling it warming up
