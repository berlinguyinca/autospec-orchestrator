# The execution plane, in diagrams

This plane answers one question: **where and how does work execute?** It never
decides *what* should run (control plane) or *what a result meant* (dispatch
plane).

## Crate responsibilities

```mermaid
flowchart TB
    API["orchestrator-api<br/><i>/api/v1</i>"]
    SCH["orchestrator-scheduler<br/><i>worker matching on<br/>resources only</i>"]
    WRK["orchestrator-worker<br/><i>the run loop</i>"]
    GW["git-worktree<br/><i>mirrors, worktrees,<br/>locks, diffs</i>"]
    HP["harness-pi<br/><i>agent sessions</i>"]
    RT["runtime-traits"]
    RD["runtime-docker"]
    RP["runtime-podman"]
    RA["runtime-apptainer"]
    ST["execution-storage"]
    PS["orchestrator-persistence"]

    API --> SCH --> WRK
    WRK --> GW
    WRK --> HP
    WRK --> RT
    RT --> RD
    RT --> RP
    RT --> RA
    WRK --> ST
    WRK --> PS
```

`runtime-traits` carries the rule that makes the adapters swappable:

> Nothing above this trait may know which runtime is in use.

## One execution, with guaranteed cleanup

```mermaid
sequenceDiagram
    participant D as dispatcher
    participant W as orchestrator-worker
    participant G as git-worktree
    participant R as Runtime adapter
    participant H as harness-pi

    D->>W: TaskPacket
    W->>G: mirror fetch (locked)
    G-->>W: isolated worktree, branch owned
    W->>R: provision(labels, requirements, services)
    R-->>W: EnvironmentHandle
    W->>H: start against worktree + packet
    H-->>W: events, then a diff
    W->>G: capture diff + changed files
    W->>R: destroy(labels)
    Note over R: removes only what matches<br/>labels.selector()
    W-->>D: evidence, or a typed failure
```

## Cleanup is guaranteed by reconcile, not by destroy

A preempted job never reaches `destroy`. On Slurm that is not an edge case —
GPU workers live in the preemptible partition, so eviction is the expected end
of a job, not an anomaly.

```mermaid
flowchart TD
    S[execution starts] --> N{ends normally?}
    N -->|yes| DES["destroy(labels)"] --> C[clean]
    N -->|"preempted, crashed,<br/>walltime, node reboot"| ORPH[resources survive]
    ORPH --> REC["reconcile(live)"]
    REC --> RPT[reported as orphans] --> C

    classDef guar fill:#1a7f3722,stroke:#1a7f37;
    class REC,RPT guar
```

Green is the guarantee. `destroy` is the fast path; `reconcile` is what makes
cleanup true. It follows that `reconcile` must rebuild ownership from the
runtime's own listing — a sidecar state file may be gone while the instances it
described are not.

## Ownership differs by runtime, behaviour does not

```mermaid
flowchart LR
    subgraph D["Docker / Podman"]
        DL["native labels,<br/>daemon-indexed"]
    end
    subgraph A["Apptainer"]
        AL["instance names +<br/>sidecar record"]
        AN["no daemon,<br/>no label store"]
    end
    DL --> SEL["labels.selector()"]
    AL --> SEL
    SEL --> ONLY["destroy removes<br/><b>only</b> what matches"]
```

Limits diverge the same way: Docker and Podman enforce them; unprivileged
Apptainer on HPC cannot, so the allocation does. An adapter must **report**
limits it cannot enforce rather than silently accept them.

## One image definition, every runtime

```mermaid
flowchart LR
    DF["Dockerfile"] --> OCI["OCI image"] --> RD["runtime-docker"]
    OCI --> RP["runtime-podman"]
    DF --> SIF["SIF"] --> RA["runtime-apptainer"]
    RD --> CS["shared conformance suite"]
    RP --> CS
    RA --> CS
    CS --> SK["absent runtime =<br/><b>visible skip</b>,<br/>never a silent pass"]
```

Because the SIF is generated from the same Dockerfile, container content is
identical across runtimes and the suite needs no per-runtime fixture. Slurm has
no Docker, so a suite that *requires* a Docker daemon can never run where this
code executes.

## Where this sits

```mermaid
flowchart LR
    A["autospec<br/><i>what work</i>"] --> B["autospec-dispatcher<br/><i>what next, what it meant</i>"] --> C["<b>autospec-orchestrator</b><br/><i>where and how</i>"] --> D["InferWeave<br/><i>inference</i>"]
```
