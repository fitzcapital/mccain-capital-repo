## ADDED Requirements

### Requirement: Local Kubernetes PID isolation
The local Kubernetes cluster SHALL apply a finite per-pod PID ceiling that prevents one application workload from exhausting process creation for the node, probes, mounts, or sibling workloads.

#### Scenario: One pod grows abnormally
- **WHEN** a local application pod reaches its configured PID ceiling
- **THEN** the failure remains isolated to that pod and the node retains capacity for probes and recovery

### Requirement: External bounded cluster recovery
The local development host SHALL provide an opt-in external guard that confirms repeated health failure, distinguishes pod failure from node process exhaustion, applies the narrowest recovery action, enforces a cooldown, and never deletes persistent data.

#### Scenario: Web endpoint fails but node can create processes
- **WHEN** the endpoint fails three consecutive checks and a node command succeeds
- **THEN** the guard restarts only the web deployment and records the action

#### Scenario: Node cannot create a process
- **WHEN** the endpoint fails three consecutive checks and the exact local kind node cannot execute a lightweight command
- **THEN** the guard restarts only that node container, waits for application health, and records the action

### Requirement: Node PID pressure visibility
The laptop resource monitor SHALL report local Kubernetes node task usage and classify warning or critical pressure before process creation is exhausted.

#### Scenario: Node task usage crosses a threshold
- **WHEN** measured node task usage reaches the configured warning or critical threshold
- **THEN** the monitor highlights the condition and identifies the recovery guard status
