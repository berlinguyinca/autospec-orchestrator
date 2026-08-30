# AutoSpec Orchestrator — System Integration, Ownership, Migration, and Supersession Specification

## Status

**Proposed architecture:** Required
**Repository:** `InferWeave/autospec-orchestrator`
**Primary consumers:** AutoSpec, AutoSpec workers, InferWeave Workbench
**Inference backend:** InferWeave
**Initial agent harness:** Pi
**Initial runtime:** Docker
**Future runtimes:** Podman, Apptainer

---

# 1. Executive Summary

`autospec-orchestrator` becomes the authoritative execution layer for AutoSpec.

Its introduction changes an important architectural assumption in AutoSpec.

Previously, AutoSpec could directly:

* create worktrees;
* start coding agents;
* manage agent processes;
* start development services;
* clean up Docker resources;
* manage execution retries;
* track local agent sessions;
* decide where agent processes should run.

Those responsibilities should no longer live inside AutoSpec itself.

Instead:

```text
GitHub
   │
   ▼
AutoSpec
Planning / Issues / DAG / Review / PR policy
   │
   ▼
autospec-orchestrator
Execution scheduling / isolation / workers / environments
   │
   ├───────────────┬─────────────────┐
   ▼               ▼                 ▼
Worker A         Worker B          Worker C
Docker           Docker            Apptainer
   │               │                 │
   ▼               ▼                 ▼
Pi execution     Pi execution      Pi execution
   │               │                 │
   └───────────────┴─────────────────┘
                   │
                   ▼
               InferWeave
        model / GPU / inference routing
```

The architecture should establish three clearly separated planes:

```text
CONTROL PLANE
AutoSpec
"What work should happen?"

EXECUTION PLANE
autospec-orchestrator
"Where and how should the work execute?"

INFERENCE PLANE
InferWeave
"Where and how should model inference execute?"
```

This separation should become a core architectural invariant across the InferWeave/AutoSpec ecosystem.

---

# 2. Why This Repository Exists

AutoSpec is evolving from an issue automation tool into a distributed autonomous software-development system.

That introduces execution concerns that should not be embedded into AutoSpec's planning logic.

An agent performing a coding task may need to:

* modify files;
* create processes;
* compile software;
* start web servers;
* run browsers;
* connect to PostgreSQL;
* connect to Redis;
* create temporary databases;
* run integration tests;
* run Docker-based test infrastructure;
* consume substantial CPU and memory;
* retain a coding-agent conversation;
* survive process or worker restarts.

Running these workloads directly from the AutoSpec process creates several problems:

* process interference;
* port collisions;
* dirty databases;
* orphaned worktrees;
* Docker leaks;
* branch collisions;
* resource exhaustion;
* security exposure;
* poor reproducibility;
* weak recovery;
* inability to distribute execution cleanly.

`autospec-orchestrator` solves these problems by making each execution a managed, isolated resource.

---

# 3. Architectural Layers

The complete system should be considered as five major layers.

```text
┌─────────────────────────────────────────────┐
│                 USERS / UI                  │
│                                             │
│ GitHub       AutoSpec UI       Workbench    │
└──────────────────────┬──────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────┐
│             AUTOSPEC CONTROL PLANE          │
│                                             │
│ Specs                                       │
│ Issue generation                            │
│ DAG/dependencies                            │
│ Planning                                    │
│ Model-role policy                           │
│ Review policy                               │
│ PR lifecycle                                │
│ Kanban/project state                        │
└──────────────────────┬──────────────────────┘
                       │
                 Execution API
                       │
                       ▼
┌─────────────────────────────────────────────┐
│          AUTOSPEC ORCHESTRATOR              │
│             EXECUTION PLANE                 │
│                                             │
│ Workers                                     │
│ Worktrees                                   │
│ Containers                                  │
│ Services                                    │
│ Pi sessions                                 │
│ Resource limits                             │
│ Recovery                                    │
│ Logs/artifacts                              │
│ Cleanup                                     │
└──────────────────────┬──────────────────────┘
                       │
                Model API requests
                       │
                       ▼
┌─────────────────────────────────────────────┐
│                 INFERWEAVE                  │
│              INFERENCE PLANE                │
│                                             │
│ Authentication                              │
│ Model registry                              │
│ GPU/node discovery                          │
│ Dynamic routing                             │
│ Capacity                                    │
│ Priority queues                             │
│ Reservations                                │
│ Inference quotas                            │
│ Token accounting                            │
└─────────────────────────────────────────────┘
```

The fifth conceptual layer is the worker infrastructure itself:

```text
PHYSICAL EXECUTION PLANE

Macs
Linux workstations
Build servers
CPU servers
HPC systems
Cloud VMs
```

Those machines run `autospec-worker`.

They do not make project-level decisions.

---

# 4. Responsibility Rule

Every new feature should first answer:

> Is this deciding what software work should happen, executing that work, or serving inference?

Then ownership follows:

```text
WHAT should happen?
    → AutoSpec

HOW/WHERE should the coding workload execute?
    → autospec-orchestrator

HOW/WHERE should LLM inference execute?
    → InferWeave
```

This rule should be documented across all three repositories.

---

# 5. AutoSpec Responsibilities After This Change

AutoSpec remains the highest-level software-development control system.

It owns:

* requirement intake;
* specification generation;
* issue generation;
* issue decomposition;
* dependency graphs;
* task prioritization;
* planning;
* acceptance criteria;
* model suitability policy;
* role assignment;
* planner selection;
* implementer selection;
* reviewer selection;
* separation-of-duties policy;
* test planning;
* documentation planning;
* UI/UX review requirements;
* GitHub Projects/Kanban state;
* stacked PR policy;
* merge readiness;
* PR creation;
* PR relationships;
* human approval gates;
* release planning.

AutoSpec should produce an execution request.

For example:

```yaml
task:
  repository: InferWeave/inferweave-node
  issue: 417
  role: implementation

git:
  baseRef: main

agent:
  harness: pi

modelPolicy:
  role: coding
  preferred:
    - qwen3.8-27b
  fallbackClass: coding-high

environment:
  profile: repository-default
  requires:
    - postgres
    - redis
```

It should then submit that request to `autospec-orchestrator`.

AutoSpec should not care whether the resulting Pi process executes:

* on the same machine;
* on a Threadripper worker;
* on a Mac;
* on a remote Linux server;
* in Docker;
* in Podman;
* in Apptainer.

---

# 6. Responsibilities Removed From AutoSpec

The following execution concerns should be removed from AutoSpec once orchestrator equivalents are stable.

## 6.1 Direct Pi Process Management

Remove:

```text
AutoSpec
   ↓
spawn("pi ...")
```

Replace with:

```text
AutoSpec
   ↓
CreateExecution(...)
   ↓
autospec-orchestrator
```

AutoSpec should never need to know a Pi PID.

---

## 6.2 Direct OpenCode Process Management

Any new OpenCode-specific execution-management code should not be built into AutoSpec.

Where existing OpenCode lifecycle code exists, deprecate and eventually remove it.

If OpenCode remains supported later, it becomes:

```text
AgentHarness
    ├── PiHarness
    └── OpenCodeHarness
```

inside `autospec-orchestrator`.

However, Pi should remain the initial and primary harness.

---

## 6.3 Worktree Lifecycle Management

AutoSpec should stop owning:

* `git worktree add`;
* worktree paths;
* worktree cleanup;
* stale-worktree scanning;
* worktree locking;
* branch/worktree collision prevention.

These move entirely to `autospec-orchestrator`.

AutoSpec may specify:

```text
repository
base ref
desired branch strategy
```

but not physical worktree paths.

---

## 6.4 Runtime Container Management

AutoSpec should not directly:

* create Docker containers;
* create Docker networks;
* create Docker volumes;
* start PostgreSQL containers;
* start Redis containers;
* tear down development containers.

These are exclusively orchestrator concerns.

---

## 6.5 Local Process Cleanup

Previous AutoSpec cleanup requirements included removing:

* abandoned Docker containers;
* Docker images;
* worktrees;
* processes;
* local branches.

The execution-oriented portions of that cleanup system are superseded.

`autospec-orchestrator` now owns cleanup for every resource it creates.

AutoSpec should retain only **logical lifecycle cleanup**, such as:

* closing superseded issues;
* updating PR state;
* pruning obsolete planning metadata;
* maintaining project board state.

---

# 7. Existing AutoSpec Cleanup Specification

Previous AutoSpec work explicitly addressed recurring leaks involving:

* Git worktrees;
* Docker containers;
* Docker images;
* local Git branches.

That work should be split.

## Retain in AutoSpec

Retain policy such as:

```text
When may an execution's branch be deleted?
When should historical artifacts be retained?
When should a failed task be retried?
When is an issue considered abandoned?
```

## Move to Orchestrator

Move mechanisms such as:

```text
Find stale worktree.
Destroy stale worktree.

Find execution-owned container.
Destroy container.

Find execution network.
Destroy network.

Find execution volume.
Destroy volume.

Terminate leaked agent process.
```

The previous cleanup implementation should therefore not remain as a parallel executor-side subsystem inside AutoSpec.

Where duplicate code exists, delete it after migration.

---

# 8. Git Branch Ownership

Git responsibilities require a careful split.

## AutoSpec owns semantic Git state

AutoSpec determines:

* issue branch naming policy;
* stacked PR relationships;
* base branch;
* whether a PR should exist;
* whether a branch should be pushed;
* whether review is complete;
* when merge is permitted.

## Orchestrator owns physical execution Git state

Orchestrator handles:

* repository mirrors/cache;
* fetch;
* physical worktrees;
* branch checkout;
* worktree locks;
* diff capture;
* execution-local Git operations;
* cleanup.

This gives:

```text
AutoSpec:
"Implement issue #417 from base SHA abc123."

Orchestrator:
"I created a clean isolated worktree for abc123."

Agent:
"I changed these files."

Orchestrator:
"Here is the resulting diff."

AutoSpec:
"Now send it for independent review."
```

---

# 9. PR Management Is NOT Superseded

`autospec-orchestrator` must not become a GitHub PR-management service.

AutoSpec should continue to own:

* creating PRs;
* stacked PR representation;
* linking issues;
* review status;
* merge gates;
* PR labels;
* GitHub Projects integration;
* dependency state.

The orchestrator merely returns the execution result.

For example:

```json
{
  "executionId": "node-417-impl-01",
  "result": "REVIEW_READY",
  "branch": "autospec/417",
  "baseSha": "abc123",
  "diffArtifact": "...",
  "tests": {
    "passed": 143,
    "failed": 0
  }
}
```

AutoSpec decides what happens next.

---

# 10. AutoSpec Model Routing Integration

AutoSpec already has a larger model-policy concept.

That remains important.

AutoSpec decides **which class of model should perform a role**.

Example:

```text
Planner:
higher reasoning model

Implementer:
Qwen 3.8 coding model

Reviewer:
different eligible higher-class model

Documentation:
writing-oriented model

UI/UX verifier:
vision-capable model
```

AutoSpec also preserves separation of duties.

For example:

```text
Qwen implements issue #417

Qwen MUST NOT be the reviewer
for its own implementation.
```

The orchestrator receives the assignment but does not decide project-level separation-of-duties rules.

---

# 11. InferWeave Integration

InferWeave is not superseded.

In fact, the new architecture makes InferWeave's role clearer.

InferWeave owns:

* inference API;
* authentication;
* API keys;
* user identity;
* model registry;
* node registration;
* GPU discovery;
* GPU utilization;
* model capability data;
* dynamic load balancing;
* request routing;
* sticky sessions where required;
* model loading;
* capacity protection;
* priority queues;
* reservations;
* inference usage accounting;
* inference metrics;
* token accounting;
* physical GPU assignment.

The execution worker does not need local GPUs.

Example:

```text
autospec-worker-03

CPU:
32 cores

RAM:
128 GB

GPU:
none

running:
7 isolated Pi environments

            │
            ▼

        InferWeave

            │
     ┌──────┼───────┐
     ▼      ▼       ▼
  3×2080   M4      4090
```

This is a major architectural advantage.

Execution capacity and inference capacity scale independently.

---

# 12. GPU Scheduling Must Not Move Into Orchestrator

Do not add:

```text
selectGpu()
loadModel()
gpuQueue()
modelPlacement()
```

to `autospec-orchestrator`.

That would duplicate InferWeave.

The orchestrator requests:

```yaml
model:
  provider: inferweave
  requestedModel: qwen3.8-27b
```

InferWeave decides:

```text
which node
which GPU
which replica
which quantization
which compatible backend
```

according to its policy.

---

# 13. Resource Scheduling Split

There are two entirely different kinds of scheduling.

## Execution scheduling

Owned by `autospec-orchestrator`.

Resources:

```text
CPU
RAM
disk
OS
architecture
container runtime
browser capability
local toolchains
execution concurrency
```

Example:

```text
Issue #417 needs:

Linux
8 CPUs
16 GB RAM
Docker
Playwright
PostgreSQL

→ send to build-worker-04
```

## Inference scheduling

Owned by InferWeave.

Resources:

```text
VRAM
GPU model
loaded models
KV cache
token throughput
queue depth
model availability
user priority
reservation
```

Example:

```text
Qwen3.8 request

→ InferWeave chooses GPU node 3
```

Never combine these schedulers.

---

# 14. AutoSpec Benchmark System

AutoSpec's model benchmark and advisory system remains separate and should integrate with both systems.

The benchmark system answers questions such as:

```text
How well does Qwen 3.8 perform at coding?

How good is it at:
- architecture
- document analysis
- debugging
- UI coding
- vision
- test generation?

What tokens/s does it produce?
What failure patterns does it exhibit?
```

AutoSpec uses this data to assign roles.

InferWeave can provide runtime performance data.

The orchestrator supplies execution outcome data.

Together:

```text
AutoSpec Benchmark
       ▲
       │
       ├── quality/test outcome
       │     from orchestrator
       │
       └── throughput/model telemetry
             from InferWeave
```

Do not move benchmarking into orchestrator.

---

# 15. AutoSpec Agent Advisory System

The model advisory layer previously discussed for AutoSpec also remains.

Its purpose is:

```text
Task classification
        ↓
Model suitability
        ↓
Role assignment
        ↓
Model selection policy
```

The orchestrator simply receives the result.

Example:

```yaml
role: implementation

modelPolicy:
  class: coding
  preferred:
    - qwen3.8-27b
  alternatives:
    - codex
    - claude
```

The orchestrator may report inability to run a requested harness/environment, but it should not independently rewrite AutoSpec's model policy unless explicitly delegated.

---

# 16. Usage-Aware Model Fallback

Existing AutoSpec plans include checking provider/model capacity and falling back when usage is exhausted.

Preserve that higher-level policy.

However, execution should become asynchronous with respect to model availability.

Example:

```text
AutoSpec
    ↓
preferred implementer = Codex

Provider capacity unavailable
    ↓
policy fallback = Qwen3.8

CreateExecution(model=Qwen3.8)
```

Alternatively, AutoSpec can submit an allowed model policy and InferWeave can choose among locally compatible replicas.

But responsibilities must remain explicit.

---

# 17. InferWeave Workbench Integration

The introduction of `autospec-orchestrator` also changes the planned InferWeave Workbench architecture.

Originally, Workbench was expected to directly launch/manage Pi or OpenCode server processes.

For managed coding workspaces, that functionality should now be superseded.

Instead:

```text
Browser
   │
   ▼
InferWeave Workbench
   │
   ▼
autospec-orchestrator API
   │
   ▼
Execution Environment
   │
   ├── persistent workspace
   ├── Pi
   ├── services
   └── artifacts
   │
   ▼
InferWeave
```

Workbench becomes a client of the execution system.

---

# 18. Workbench Responsibilities After Orchestrator

Workbench should own:

* browser UI;
* user authentication/session;
* chat rendering;
* command history UI;
* workspace selection;
* Pi conversation presentation;
* HTML rendering;
* Markdown rendering;
* YAML/XML rendering;
* images;
* file upload;
* file download;
* artifact browsing;
* session/fork UI;
* task status UI;
* live logs;
* execution controls;
* start/stop/resume buttons.

It should not directly own:

* Pi subprocesses;
* worktrees;
* Docker containers;
* service containers;
* container networks;
* resource quotas;
* execution cleanup.

Those move to orchestrator.

---

# 19. Existing Workbench Pi Server Design Superseded

Any Workbench specification stating:

> Workbench backend launches one Pi process per workspace

should be updated.

New design:

```text
Workbench requests an ExecutionEnvironment.

autospec-orchestrator launches Pi.

Workbench subscribes to execution/Pi events.
```

This avoids implementing the same process-management system twice.

---

# 20. Interactive Workbench Sessions

Not every Workbench session originates from a GitHub issue.

The orchestrator therefore needs generic interactive execution support.

Example:

```yaml
kind: InteractiveWorkspace

repository:
  repo: InferWeave/inferweave-node
  baseRef: main

agent:
  harness: pi

persistence:
  mode: resumable
```

Workbench can then create an environment without an AutoSpec issue.

This allows:

```text
AutoSpec-created execution
or
human-created Workbench execution
```

to use the same substrate.

---

# 21. Shared Execution API

Avoid building separate execution engines for:

```text
AutoSpec
Workbench
CLI
tests
future IDE integration
```

All should use:

```text
autospec-orchestrator API
```

Possible clients:

```text
autospec
inferweave-workbench
autospec-cli
developer scripts
CI systems
future VS Code integration
```

---

# 22. Pi Integration

Pi becomes an agent harness hosted by `autospec-orchestrator`.

The integration should be encapsulated behind:

```text
AgentHarness
```

Initial implementation:

```text
AgentHarness
    └── PiHarness
```

Possible future:

```text
AgentHarness
    ├── PiHarness
    ├── OpenCodeHarness
    ├── CodexHarness
    └── HermesHarness
```

AutoSpec and Workbench should not depend on Pi-specific process semantics.

---

# 23. OpenCode Integration

The ecosystem previously planned considerable OpenCode usage.

Since Pi has become the primary coding interface, avoid maintaining duplicate first-class orchestration paths.

Recommendation:

```text
PRIMARY
Pi

OPTIONAL COMPATIBILITY
OpenCode via harness adapter

DO NOT BUILD
separate OpenCode orchestration subsystem
```

Existing OpenCode-specific AutoSpec process lifecycle should be deprecated.

Existing useful OpenCode functionality should either:

* be reproduced through Pi;
* become generic orchestrator functionality;
* or live behind a future `OpenCodeHarness`.

---

# 24. MCP Integration

MCP remains an agent capability concern.

However, MCP servers that an agent requires should execute within or alongside its isolated environment.

Example:

```text
ExecutionEnvironment
    │
    ├── Pi
    ├── PostgreSQL
    ├── Redis
    └── MCP service
```

The orchestrator manages process/container lifecycle.

AutoSpec determines whether the task is allowed or expected to use that MCP capability.

Workbench displays or configures it.

---

# 25. Tool/Skill Ownership

Separate three concepts.

## Pi skills/prompts

Examples:

```text
implement issue
review diff
perform UI validation
write documentation
```

Owned as reusable agent behavior.

## AutoSpec policy

Examples:

```text
who implements
who reviews
what acceptance criteria apply
```

Owned by AutoSpec.

## Execution capabilities

Examples:

```text
browser installed
PostgreSQL available
Redis available
filesystem writable
```

Owned/provisioned by orchestrator.

Do not mix these layers.

---

# 26. Testing Architecture

The orchestrator becomes the authoritative owner of the physical test environment.

AutoSpec still defines test expectations.

Example:

```text
AutoSpec:
"Integration tests require PostgreSQL and Redis."

Orchestrator:
"Provision fresh PostgreSQL and Redis."

Pi:
"Run tests."

Orchestrator:
"Capture results and artifacts."

Reviewer:
"Run tests again in fresh environment."

AutoSpec:
"Determine whether acceptance criteria are met."
```

---

# 27. Review Isolation

This should supersede any AutoSpec implementation that reviews code inside the same physical execution environment.

Every independent review should be capable of receiving:

```text
fresh worktree
fresh database
fresh Redis
fresh app processes
separate Pi session
```

The review may inspect the implementation branch but should not depend on its runtime state.

---

# 28. UI/UX Review Integration

AutoSpec previously identified UI/UX verification as a dedicated role requiring vision and browser automation.

That architecture remains, but the runtime becomes orchestrator-managed.

Example:

```text
AutoSpec
    ↓
UI review required
    ↓
CreateExecution(
    role=ui-review,
    capabilities=[browser, playwright]
)
    ↓
Orchestrator
    ↓
appropriate worker
    ↓
browser container + application + Pi
    ↓
vision model through InferWeave
```

Artifacts may include:

* screenshots;
* videos;
* Playwright traces;
* accessibility output;
* DOM snapshots.

---

# 29. Documentation Agent Integration

Documentation agents also use the same execution substrate.

They may require:

```text
repository worktree
docs generator
preview server
browser
```

Do not create a special documentation executor in AutoSpec.

Create:

```text
role=documentation
```

and let the orchestrator handle the physical environment.

---

# 30. AutoSpec Kanban Integration

GitHub Project/Kanban generation remains AutoSpec responsibility.

Execution state should feed it.

Example mapping:

```text
QUEUED
→ Ready / Assigned

RUNNING
→ In Progress

REVIEW_READY
→ Review

FAILED
→ Blocked / Retry

COMPLETED
→ Done
```

Do not embed GitHub Project management in orchestrator.

Orchestrator emits state/events.

AutoSpec interprets those events into project workflow.

---

# 31. Event Integration

AutoSpec should subscribe to orchestrator execution events.

Example:

```text
ExecutionCreated
WorkerAssigned
EnvironmentReady
AgentStarted
TestsStarted
TestsFailed
ReviewReady
ExecutionFailed
ExecutionCompleted
```

AutoSpec can then:

* update issue state;
* update project board;
* enqueue reviewer;
* retry task;
* notify human;
* open PR.

---

# 32. Autospec Controller Becomes More Stateless

A major benefit is that AutoSpec no longer has to keep local processes alive.

AutoSpec can be restarted without losing:

* Pi subprocess;
* execution container;
* worktree ownership;
* test services.

The orchestrator retains authoritative execution state.

This reduces coupling significantly.

---

# 33. Worker Model

Workers should be lightweight daemons.

Examples:

```text
autospec-worker on M4

autospec-worker on Threadripper

autospec-worker on Linux CI server

autospec-worker on HPC node
```

Each advertises capabilities.

AutoSpec should not connect directly to workers.

Topology:

```text
AutoSpec
    ↓
Orchestrator Controller
    ↓
Worker
```

---

# 34. Relationship to InferWeave Intermediate Nodes

InferWeave intermediate nodes and AutoSpec workers solve different problems.

Do not merge them.

## InferWeave Intermediate Node

Manages:

```text
GPU nodes
models
inference requests
user priority
token usage
reservations
```

## AutoSpec Worker

Manages:

```text
source workspaces
containers
Pi
build processes
databases
browsers
tests
CPU/RAM/disk
```

A physical machine may run both daemons.

But logically:

```text
inferweave-node

and

autospec-worker
```

must remain independent services.

---

# 35. Why They Must Remain Separate

Consider a Threadripper host:

```text
Threadripper
128 GB RAM
3×2080 Ti
```

It can simultaneously act as:

```text
InferWeave node
→ serves Qwen inference on GPUs

AutoSpec worker
→ runs builds/tests on CPU
```

InferWeave may be serving requests from ten unrelated users while the AutoSpec worker is running three isolated builds.

Combining their schedulers would unnecessarily couple those workloads.

---

# 36. InferWeave Authentication

Agent containers should authenticate to InferWeave using scoped credentials.

Preferred future flow:

```text
AutoSpec user/task identity
        ↓
Orchestrator
        ↓
short-lived execution credential
        ↓
Pi
        ↓
InferWeave
```

This allows InferWeave usage metrics to attribute inference to:

```text
user
repository
issue
execution
agent role
model
node
GPU
```

without exposing long-lived master API keys.

---

# 37. Usage Telemetry Integration

The ecosystem should eventually correlate:

```text
AutoSpec task ID
execution ID
Pi session ID
InferWeave request ID
model
node
GPU
tokens
wall clock
test outcome
review outcome
```

This produces extremely valuable data.

Example future analysis:

```text
Qwen3.8-27B

backend:
3×2080 Ti

task class:
medium TypeScript implementation

median execution:
19 min

median inference:
8.2 min

success before review:
84%

review corrections:
1.4

cost:
$0 local inference
```

This belongs in analytics built from all three systems rather than duplicated tracking.

---

# 38. InferWeave Workbench Session Persistence

Workbench previously required:

> leave the page, log out, come back later, and the work should still be running.

Orchestrator now provides the proper implementation.

Persistence consists of:

```text
Execution record
Git worktree
Pi JSONL session
services
logs
artifacts
```

Workbench stores only UI/user session references.

Example:

```text
Workbench workspace ID
     ↓
Execution ID
     ↓
autospec-orchestrator
```

---

# 39. Workbench Forking

Pi conversation forking and workspace forking should be distinguished.

## Conversation fork

Same source worktree.

Different Pi conversational branch.

Managed through Pi harness.

## Execution fork

Create another isolated worktree/environment from the same Git base.

Managed by orchestrator.

Workbench UI should make the difference clear.

Example:

```text
Fork conversation
vs
Fork workspace
```

---

# 40. AutoSpec Retries

AutoSpec determines **whether** to retry.

Orchestrator determines **how** to create/recover the environment.

Example:

```text
Issue #417
   │
   ├── impl-01 → MODEL_FAILED
   │
   └── impl-02 → fresh or resumed execution
```

The retry policy can specify:

```text
resume same worktree
fresh environment
fresh worktree
different model
different worker
```

AutoSpec selects policy; orchestrator executes it.

---

# 41. Crash Recovery

Crash recovery should supersede ad-hoc AutoSpec restart handling.

AutoSpec should not contain:

```text
if Pi PID disappeared:
   inspect directory
   restart command
```

Instead it receives:

```text
Execution recovered
```

or:

```text
Execution failed: HARNESS_FAILED
```

from orchestrator.

---

# 42. Docker Cleanup Supersession

All execution-created Docker resources must be labeled:

```text
autospec.managed=true
autospec.execution_id=...
```

A central worker reconciliation/GC subsystem supersedes any scripts elsewhere that attempt broad operations like:

```bash
docker container prune
docker image prune
docker volume prune
```

Those broad cleanup commands are dangerous on shared systems.

Orchestrator should only delete resources it owns.

Existing AutoSpec cleanup code using global Docker pruning should be removed once migration is complete.

---

# 43. Branch Cleanup Supersession

Similarly, avoid:

```bash
git branch | grep autospec | xargs git branch -D
```

Cleanup must use recorded execution ownership.

AutoSpec tells orchestrator that the logical work is no longer needed.

Orchestrator removes the exact worktree/branch resources it owns.

---

# 44. Local Model Processes

AutoSpec should not directly start model servers.

If Qwen needs to run locally:

```text
InferWeave node
    ↓
owns Qwen serving process
```

Pi still talks to InferWeave.

This supersedes any AutoSpec code that starts vLLM, MLX, llama.cpp, SGLang, etc. as part of issue execution.

Model serving belongs exclusively to InferWeave.

---

# 45. Hardware Awareness

AutoSpec should reason about **capabilities**, not individual physical machines.

Bad:

```text
run this on Gert's Threadripper
```

Better:

```yaml
requires:
  os: linux
  cpu: 8
  memory: 16GiB
  capabilities:
    - docker
    - playwright
```

Likewise model request:

```yaml
modelClass:
  coding-medium
```

InferWeave can map it to available hardware.

---

# 46. Proposed Repository Relationships

The main repositories become:

```text
autospec
    │
    ├── planning
    ├── issue graph
    ├── model-role policy
    ├── reviews
    ├── PRs
    └── GitHub Projects

autospec-orchestrator
    │
    ├── worker scheduling
    ├── isolated environments
    ├── Pi harness
    ├── services
    ├── worktrees
    └── artifacts

inferweave-node
    │
    ├── model serving
    ├── GPU resources
    └── inference participation

inferweave intermediate/controller
    │
    ├── routing
    ├── priorities
    ├── auth
    ├── reservations
    └── telemetry

inferweave-workbench
    │
    ├── interactive UI
    ├── session UX
    ├── artifact rendering
    └── orchestrator client
```

---

# 47. Dependency Direction

Keep dependencies one-way.

Desired:

```text
AutoSpec ──────────────► autospec-orchestrator
                              │
                              ▼
                           InferWeave

Workbench ─────────────► autospec-orchestrator
                              │
                              ▼
                           InferWeave
```

Avoid:

```text
InferWeave importing AutoSpec

Orchestrator importing AutoSpec business logic

Worker calling GitHub Projects directly

Pi calling AutoSpec database directly
```

Use APIs/events at boundaries.

---

# 48. AutoSpec Data Model Changes

AutoSpec should add an execution reference to work items.

Example:

```yaml
workItem:
  id: issue-417

executions:
  - id: inferweave-node-417-impl-01
    role: implementation

  - id: inferweave-node-417-review-01
    role: review
```

AutoSpec need not duplicate complete runtime state.

Store only enough to reference orchestrator resources and important final results.

---

# 49. Orchestrator Data Is Authoritative for Execution

The following must be authoritative in orchestrator:

```text
worker assignment
runtime state
container state
service state
worktree path
Pi process
Pi session path
resource usage
execution logs
artifact locations
failure classification
```

Do not mirror these into AutoSpec and attempt two-way synchronization.

---

# 50. AutoSpec Data Is Authoritative for Workflow

AutoSpec remains authoritative for:

```text
issue state
task dependencies
acceptance criteria
review policy
PR state
project state
model-role requirements
retry strategy
merge state
```

---

# 51. InferWeave Data Is Authoritative for Inference

InferWeave remains authoritative for:

```text
actual model used
model node
GPU
queue timing
tokens
inference latency
capacity
user/model quotas
```

---

# 52. Source-of-Truth Matrix

| Concern                             | Source of Truth |
| ----------------------------------- | --------------- |
| Requirements                        | AutoSpec        |
| Issue decomposition                 | AutoSpec        |
| DAG                                 | AutoSpec        |
| Planner/implementer/reviewer policy | AutoSpec        |
| PR lifecycle                        | AutoSpec        |
| GitHub Project/Kanban               | AutoSpec        |
| Execution ID                        | Orchestrator    |
| Worker assignment                   | Orchestrator    |
| Worktree                            | Orchestrator    |
| Pi process                          | Orchestrator    |
| Pi session persistence              | Orchestrator    |
| PostgreSQL/Redis environment        | Orchestrator    |
| CPU/RAM/disk quota                  | Orchestrator    |
| Execution cleanup                   | Orchestrator    |
| Model routing                       | InferWeave      |
| GPU selection                       | InferWeave      |
| Model loading                       | InferWeave      |
| Inference queue                     | InferWeave      |
| Token accounting                    | InferWeave      |
| Browser presentation                | Workbench       |
| Interactive UX                      | Workbench       |
| Artifact rendering                  | Workbench       |

This table should be included in architectural documentation across the relevant repositories.

---

# 53. What Is Explicitly Superseded

Once `autospec-orchestrator` reaches production readiness, the following components should be considered superseded.

## AutoSpec

Remove or deprecate:

* direct coding-agent process spawning;
* direct Pi lifecycle management;
* direct OpenCode server lifecycle management;
* issue-level Docker creation;
* Docker service startup;
* local PostgreSQL/Redis lifecycle;
* execution worktree implementation;
* stale worktree runtime cleanup;
* generic Docker prune logic;
* execution PID tracking;
* agent process restart code;
* execution-local port allocation;
* execution-local log storage;
* runtime resource accounting.

## Workbench

Remove or avoid implementing:

* direct Pi process launcher;
* direct OpenCode server launcher;
* Docker environment manager;
* worktree manager;
* service-container manager;
* workspace process supervisor;
* execution cleanup daemon.

## InferWeave

Do not implement:

* coding workspace containers;
* Git worktree management;
* source-repository lifecycle;
* build/test orchestration;
* Pi session management.

These belong to orchestrator.

---

# 54. What Is NOT Superseded

Keep:

## AutoSpec

* AutoSpec specification engine;
* GitHub issue generation;
* AutoSpec issue dependency management;
* model advisory;
* benchmarks;
* separation of duties;
* review orchestration;
* PR strategy;
* stacked PRs;
* testing policy;
* documentation role;
* UI/UX review role;
* GitHub Kanban support.

## InferWeave

* nodes;
* intermediate nodes;
* proxy;
* authentication;
* model routing;
* load balancing;
* GPU capacity;
* reservations;
* priority;
* token tracking;
* user API keys;
* dashboards for inference infrastructure.

## Workbench

* web interface;
* persistent user UX;
* rendering;
* file exchange;
* Pi conversation UI;
* session browsing;
* interactive commands;
* artifact display.

---

# 55. Components That Should Be Moved Rather Than Rewritten

Where existing AutoSpec code already solves execution problems, prefer extraction/migration.

Potential candidates:

```text
worktree handling
cleanup policies
agent process wrappers
execution logging
Docker inspection
retry metadata
task identifiers
test result parsing
```

Do not blindly delete functioning logic.

Refactor reusable mechanisms into `autospec-orchestrator`, then delete the original integration after the new API is proven.

---

# 56. Migration Strategy

Migration should occur incrementally.

## Phase 1 — Introduce Orchestrator

Keep current AutoSpec executor available.

Add:

```text
executionBackend:
  legacy
  orchestrator
```

Use orchestrator experimentally.

---

## Phase 2 — Pi Executions

Move Pi-based coding tasks to orchestrator.

AutoSpec still retains legacy executor for fallback.

Validate:

* worktrees;
* Pi sessions;
* test services;
* cleanup;
* retries.

---

## Phase 3 — Review Executions

Move reviewers to orchestrator.

Require fresh review environments.

---

## Phase 4 — UI/Documentation Roles

Move:

* browser verification;
* documentation;
* integration-test roles.

---

## Phase 5 — Workbench

Change Workbench from direct Pi process management to orchestrator API.

---

## Phase 6 — Remove Legacy Execution

When reliability criteria are met:

delete:

```text
AutoSpec local agent runner
AutoSpec Docker executor
AutoSpec runtime worktree manager
AutoSpec runtime cleanup system
Workbench Pi supervisor
```

Do not maintain two permanent execution systems.

---

# 57. Legacy Removal Gate

Legacy executor code may be removed when the orchestrator demonstrates:

* concurrent issue execution;
* Pi session persistence;
* service isolation;
* clean reviewer environments;
* cancellation;
* resource enforcement;
* crash recovery;
* deterministic cleanup;
* worktree cleanup;
* artifact persistence;
* production-level logging;
* no task loss during worker restart.

---

# 58. Avoid Feature Duplication During Migration

Every related issue should explicitly state its destination architecture.

For example:

```text
Need better Docker cleanup?

Implement in autospec-orchestrator.
Do not modify legacy AutoSpec executor except minimal migration compatibility.
```

Likewise:

```text
Need Pi session recovery?

Implement in autospec-orchestrator.
```

And:

```text
Need worker resource limits?

Implement in autospec-orchestrator.
```

This prevents new technical debt from accumulating while migration occurs.

---

# 59. New AutoSpec Execution Interface

AutoSpec should depend on an abstract execution service.

Conceptually:

```text
ExecutionService

createExecution()
getExecution()
cancelExecution()
retryExecution()
getResult()
getEvents()
getArtifacts()
```

The production implementation becomes:

```text
OrchestratorExecutionService
```

The old implementation can temporarily be:

```text
LegacyLocalExecutionService
```

This enables incremental migration without contaminating AutoSpec business logic with orchestrator details.

---

# 60. AutoSpec State Flow

Future issue lifecycle:

```text
GitHub Issue
    │
    ▼
AutoSpec Planner
    │
    ▼
Task packet
    │
    ▼
Create implementation execution
    │
    ▼
autospec-orchestrator
    │
    ▼
Pi implementation
    │
    ▼
result + diff + test evidence
    │
    ▼
AutoSpec
    │
    ▼
Create review execution
    │
    ▼
autospec-orchestrator
    │
    ▼
Independent reviewer
    │
    ├── approved
    │       ↓
    │    AutoSpec PR lifecycle
    │
    └── changes requested
            ↓
       AutoSpec schedules another
       implementation execution
```

---

# 61. Execution Attempts

Do not conflate:

```text
Issue
Task
Execution
Attempt
Pi session
```

Recommended hierarchy:

```text
Issue #417

Task:
implement reservation cancellation

Execution 1:
implementation attempt

Pi session:
node-417-impl-01

Execution 2:
review

Pi session:
node-417-review-01

Execution 3:
fix review findings

Pi session:
node-417-impl-02
```

This must be reflected consistently across AutoSpec and orchestrator.

---

# 62. Container-per-Execution Principle

Default:

> One execution gets one isolated runtime environment.

Services live in the same isolated execution network but generally separate containers.

Example:

```text
Execution 417

network autospec-417

    pi-agent
    postgres
    redis
    minio
```

Destroying that environment must not affect any other execution.

---

# 63. Multi-Service Development Environments

Repositories should define supported dependencies declaratively.

Example:

```yaml
# .autospec/environment.yaml

runtime:
  image: ghcr.io/inferweave/autospec-java:latest

services:
  postgres:
    image: postgres:17

  redis:
    image: redis:8

  localstack:
    image: localstack/localstack
```

Task:

```yaml
requires:
  - postgres
  - redis
```

Orchestrator provisions only what is needed.

---

# 64. Docker Compose

Existing repositories may already contain `docker-compose.yml`.

Orchestrator may eventually support adapting repository Compose definitions.

However, agents must not automatically receive host Docker access.

Potential model:

```text
Orchestrator
    ↓
parse/validate Compose requirements
    ↓
create services itself
```

or provide an isolated nested runtime when full Compose execution is necessary.

---

# 65. Apptainer

Apptainer should be a later runtime adapter.

It is particularly relevant for:

* UC environments;
* HPC;
* managed compute clusters;
* systems where Docker daemon access is unavailable.

The same execution manifest should work conceptually across runtimes.

Example:

```yaml
runtime:
  type: apptainer
```

AutoSpec should not change behavior because of the runtime.

---

# 66. Worker Registration and InferWeave Registration

Do not reuse one registration protocol.

A machine may register twice:

```text
autospec-worker registration
→ execution capabilities

inferweave-node registration
→ inference capabilities
```

Example:

```yaml
Autospec Worker:
  cpu: 32
  ram: 128GB
  docker: true
  browser: true

InferWeave Node:
  gpu:
    - RTX 2080 Ti
    - RTX 2080 Ti
    - RTX 2080 Ti
  models:
    - Qwen3.8-27B
```

Different services, different concerns.

---

# 67. Autospec-Orchestrator Dashboard

Any orchestrator-specific UI should focus on execution infrastructure.

Examples:

```text
Workers
Executions
Resource consumption
Failure rates
Cleanup health
Queue
Runtime status
```

It should not duplicate InferWeave's GPU dashboard.

Likewise InferWeave dashboard should not attempt to display detailed Git/worktree state.

Workbench or a higher AutoSpec dashboard may combine both datasets where useful.

---

# 68. Unified High-Level Dashboard

AutoSpec could eventually provide a federated view:

```text
Issue #417

Implementation
  Worker: buildbox-02
  CPU: 6/8
  RAM: 9.1/16 GB

Agent
  Pi
  turn 37

Inference
  Qwen3.8-27B
  InferWeave node: gpu03
  GPU: 2080 Ti #2
  28 tok/s

Tests
  143 passed
  0 failed
```

But the data remains authoritative in the underlying systems.

---

# 69. Deployment

Initial single-host development:

```text
Docker Compose

autospec
autospec-orchestrator
autospec-worker
postgres
```

The worker can run on the same machine.

Later:

```text
AutoSpec controller
        │
        ▼
Orchestrator controller
        │
        ├── worker-a
        ├── worker-b
        ├── worker-c
        └── worker-hpc
```

No architectural rewrite should be required.

---

# 70. Failure Domains

The systems should fail independently.

## If AutoSpec crashes

Running executions continue.

## If Workbench crashes

Running executions continue.

## If worker crashes

Orchestrator detects it and recovers/fails affected executions.

## If InferWeave node crashes

InferWeave reroutes or reports model failure.

## If individual Pi crashes

Orchestrator may resume/restart it.

## If one PostgreSQL execution service crashes

Only that execution is affected.

This is a major design objective.

---

# 71. Security Domains

Separate credentials.

```text
AutoSpec
→ GitHub/project-level credentials

Orchestrator Controller
→ worker credentials

Worker
→ runtime privileges

Execution
→ scoped repository + InferWeave credentials

InferWeave
→ model service credentials
```

Do not pass the AutoSpec controller's full GitHub token into every coding container.

---

# 72. Observability Correlation

Use a shared correlation vocabulary.

Recommended IDs:

```text
project_id
issue_id
task_id
execution_id
attempt_id
session_id
worker_id
inferweave_request_id
model_id
```

Logs should be searchable across systems using `execution_id`.

---

# 73. Events Between Systems

Use events rather than deep coupling where possible.

Example:

```text
Orchestrator:
ExecutionCompleted

AutoSpec:
receives event
    ↓
schedules Review

Review completed
    ↓
AutoSpec:
updates GitHub issue/PR
```

Workbench can independently subscribe to the same execution event stream.

---

# 74. No Shared Database

Do not let AutoSpec, orchestrator, Workbench, and InferWeave directly read/write each other's database schemas.

Each service owns its data.

Integration occurs through:

* APIs;
* event streams;
* stable IDs.

This avoids an eventual monolithic database disguised as microservices.

---

# 75. API Versioning

Orchestrator API should be versioned immediately.

Example:

```text
/api/v1/executions
/api/v1/workers
```

Execution manifests:

```text
autospec.dev/v1alpha1
```

This allows early iteration without coupling every repository to internal structures.

---

# 76. Repository Naming

Recommended ecosystem naming:

```text
autospec
autospec-orchestrator
inferweave-node
inferweave-workbench
```

Worker binary can live in `autospec-orchestrator` initially:

```text
autospec-orchestrator
autospec-worker
```

Do not create another repository for workers until there is a concrete reason.

---

# 77. Code That Should NOT Be Shared

Avoid a giant shared package containing every domain model.

Only share API contracts where genuinely useful.

AutoSpec's internal issue classes should not leak into orchestrator.

Orchestrator should use neutral structures:

```text
Execution
TaskReference
RepositoryReference
AgentAssignment
RuntimeRequirement
```

---

# 78. Task Packet Becomes the Contract

The most useful boundary between AutoSpec planning and agent execution is the task packet.

AutoSpec writes:

```text
Goal
Acceptance Criteria
Non-Goals
Relevant Context
Required Tests
Role
```

Orchestrator delivers it to Pi.

Pi executes it.

This prevents Pi from needing unrestricted access to AutoSpec internals.

---

# 79. AutoSpec Instructions to Pi

AutoSpec should not generate giant dynamic system prompts.

Prefer:

```text
repository AGENTS.md
+
role skill
+
task packet
```

Orchestrator assembles/mounts these execution inputs.

This makes tasks reproducible and inspectable.

---

# 80. Repository `AGENTS.md`

Repository-specific agent instructions remain stored with each source repository.

These are not orchestrator policy.

Example:

```text
coding standards
test commands
architecture rules
forbidden operations
```

Orchestrator simply makes them available in the worktree.

---

# 81. Global Security Rules

Hard runtime protections must live in orchestrator, not only `AGENTS.md`.

Example:

```text
PID limit
memory limit
filesystem boundary
network restrictions
credential scoping
container isolation
```

A model instruction is not a security boundary.

On POSIX platforms, descriptor-relative storage mutations authenticate each
directory or file inode before using it and fail closed when an authenticated
object changes. Operation names are collision-resistant, not secret. Because
`mkdirat` returns no descriptor, an uncooperative process running as the worker
uid can observe and replace a newly created directory between `mkdirat` and its
first `statat`/`openat`; that actor is outside the enforceable filesystem
boundary and can also control or trace the worker process. Process mutexes and
descriptor advisory locks serialize cooperative writers only. Deployments must
therefore keep untrusted agents on a different uid and deny them write access to
worker metadata. Execution mountpoints are staged and published with
`RENAME_NOREPLACE`; the authenticated pre-mount descriptor is retained across
the path-based mount, and the mounted filesystem is then reopened relative to
the retained parent descriptor. Exact backend mount/filesystem identity is
checked before and after that capture. The one mount-specific capture path
accepts only an already-private `0700` root or ext4's exact `0755` default,
requires the pre-mount owner and the observed mounted device, and normalizes
`0755` to `0700` with `fchmod` and `fsync` on the retained descriptor before a
strict link/descriptor recheck, Docker proof, layout creation, or Ready
publication. Ordinary directory capture remains fail-closed and never repairs
group- or world-accessible modes.

---

# 82. AutoSpec Cleanup Responsibilities After Migration

Final intended AutoSpec cleanup responsibility:

```text
Logical cleanup only

close stale tasks
close obsolete issues
update dependencies
delete obsolete planning state
request execution deletion
request branch deletion
```

Final orchestrator cleanup responsibility:

```text
Physical cleanup

processes
worktrees
containers
networks
volumes
temporary branches
service state
session state
temporary artifacts
```

This distinction should eliminate the previous repeated cleanup problems.

---

# 83. Infrastructure Ownership Labels

Every resource created by orchestrator must be ownership-labeled.

Examples:

```text
autospec.managed=true
autospec.execution_id=...
autospec.worker_id=...
autospec.repository=...
autospec.issue=...
```

This is mandatory.

It enables safe reconciliation and cleanup without touching unrelated user resources.

---

# 84. Human-Initiated Sessions

WorkBench users can create:

```text
interactive execution
```

AutoSpec can later adopt it if desired.

Example:

```text
Human prototypes solution
        ↓
Workbench execution
        ↓
creates branch/diff
        ↓
Convert to AutoSpec task/PR
```

The common execution substrate enables this without copying work manually.

---

# 85. AutoSpec-Managed Sessions

Conversely, a user should be able to open a running AutoSpec execution in Workbench.

Example:

```text
GitHub issue #417
        ↓
AutoSpec execution
        ↓
"Open in Workbench"
        ↓
browser connects to existing Pi session
```

This is one of the strongest benefits of sharing orchestrator.

---

# 86. Human Takeover

Support a future state:

```text
AUTONOMOUS
        ↓
PAUSED_FOR_HUMAN
        ↓
INTERACTIVE
        ↓
AUTONOMOUS
```

The execution/worktree/Pi session remains the same.

Workbench provides the human interface.

The orchestrator provides runtime continuity.

AutoSpec provides workflow policy.

---

# 87. Review in Workbench

A human reviewer can similarly open:

```text
review execution
```

and inspect:

* diff;
* tests;
* browser artifacts;
* Pi reviewer discussion;
* logs.

No need to reproduce the environment locally.

---

# 88. Benchmark Reproducibility

The execution manifest also makes model benchmarking substantially stronger.

A benchmark run can capture:

```text
same base SHA
same task
same container image
same tests
same services

Model A
vs
Model B
vs
Model C
```

Only the model changes.

This should eventually integrate with AutoSpec's model advisory system.

---

# 89. Competitive Implementations

The architecture naturally supports multiple implementations.

Example:

```text
Issue #417
       │
       ├── Qwen implementation A
       ├── Codex implementation B
       └── Claude implementation C
```

Each uses its own ExecutionEnvironment.

A reviewer can compare outcomes.

This should not be part of the MVP, but no architecture decision should prevent it.

---

# 90. Parallel Subtasks

For a decomposed issue:

```text
Feature
  ├── backend
  ├── frontend
  ├── tests
  └── docs
```

AutoSpec owns the DAG.

Orchestrator can run independent tasks concurrently.

Once dependencies resolve, AutoSpec schedules downstream tasks.

Do not put DAG planning into orchestrator.

---

# 91. Subagents

If Pi or future harnesses use subagents, there are two categories.

## In-process logical subagent

Can remain within one ExecutionEnvironment.

## Independent coding subtask

Should get its own ExecutionEnvironment.

Rule:

> If it can independently mutate source or run conflicting services, it needs execution isolation.

---

# 92. Existing Distributed Agent Ideas

Previous AutoSpec discussions considered distributing subagents across systems.

The orchestrator becomes the mechanism enabling that.

Do not build separate remote-agent distribution infrastructure into AutoSpec.

AutoSpec simply creates multiple executions.

Scheduler distributes them across workers.

---

# 93. Existing Local Cluster Orchestration

Do not confuse AutoSpec worker orchestration with InferWeave's intermediate-node cluster orchestration.

Existing requirements involving:

* GPU allocation;
* PPM/model failures;
* node availability;
* dynamic GPU load;
* user reservations;
* API keys;
* model capacity;

remain InferWeave features.

Autospec-orchestrator only schedules CPU/build/runtime environments.

---

# 94. PPM/Inference Errors

If InferWeave reports a model/backend failure:

```text
Pi
 ↓
InferWeave request fails
 ↓
Pi harness reports model error
 ↓
Orchestrator classifies MODEL_FAILED
 ↓
AutoSpec decides retry/fallback
```

Orchestrator should not attempt to repair GPU inference nodes.

---

# 95. Agent Hung Detection

Orchestrator does own agent execution health.

Examples:

```text
Pi process alive but no events for N minutes
Pi tool subprocess stuck
container consuming CPU indefinitely
```

It should expose:

```text
inactivity
timeout
resource violation
```

AutoSpec then decides whether to retry.

---

# 96. Container Build Strategy

Eventually, repository environment images may be cached/built by workers.

The orchestrator can own:

```text
environment image preparation
image cache
build lifecycle
```

But this should be carefully separated from InferWeave model-container management.

---

# 97. Artifacts and Files

Orchestrator owns artifact storage metadata.

Workbench renders them.

AutoSpec associates them with task/review state.

Example:

```text
Orchestrator:
playwright-report artifact

Workbench:
interactive HTML view

AutoSpec:
records UI test evidence
```

---

# 98. Generated Files

When Pi creates user-facing/generated files:

```text
reports
screenshots
builds
documents
archives
```

the orchestrator captures them as artifacts.

Workbench provides downloads/previews.

AutoSpec may attach/link relevant artifacts to issues/PRs.

---

# 99. Logs

Detailed execution logs should not be copied wholesale into AutoSpec's database.

AutoSpec should retain:

```text
summary
result
important diagnostics
artifact references
```

Orchestrator retains:

```text
full runtime logs
service logs
Pi events
test logs
```

---

# 100. Long-Term Architecture

The intended mature architecture is:

```text
                           ┌───────────────────────┐
                           │        GitHub         │
                           └───────────┬───────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │       AutoSpec        │
                           │                       │
                           │ Requirements          │
                           │ Specs                 │
                           │ Issues                │
                           │ DAG                   │
                           │ Agent policy          │
                           │ Review                │
                           │ PRs                   │
                           └───────────┬───────────┘
                                       │
                              execution requests
                                       │
                                       ▼
                         ┌───────────────────────────┐
                         │ autospec-orchestrator     │
                         │                           │
                         │ Scheduler                 │
                         │ Workers                   │
                         │ Environments              │
                         │ Pi sessions               │
                         │ Services                  │
                         │ Recovery                  │
                         │ Cleanup                   │
                         └─────────────┬─────────────┘
                                       │
                ┌──────────────────────┼──────────────────────┐
                │                      │                      │
                ▼                      ▼                      ▼
        ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
        │ Linux Worker  │      │   Mac Worker  │      │  HPC Worker   │
        │ Docker        │      │ Docker/Native │      │ Apptainer     │
        └───────┬───────┘      └───────┬───────┘      └───────┬───────┘
                │                      │                      │
                └──────────────────────┼──────────────────────┘
                                       │
                                       ▼
                                Pi / Agent Harness
                                       │
                                       ▼
                              ┌─────────────────┐
                              │    InferWeave   │
                              │                 │
                              │ Auth            │
                              │ Routing         │
                              │ Models          │
                              │ GPU scheduling  │
                              │ Quotas          │
                              │ Telemetry       │
                              └───────┬─────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
                GPU Node          GPU Node           M4 Node


Browser
   │
   ▼
┌──────────────────────────┐
│ InferWeave Workbench     │
│                          │
│ Interactive sessions     │
│ Execution status         │
│ Artifacts                │
│ Logs                     │
│ Human takeover           │
└─────────────┬────────────┘
              │
              └────────► autospec-orchestrator
```

---

# 101. Architectural Invariants

The following should be treated as non-negotiable.

## Invariant 1

AutoSpec decides **what work occurs**.

## Invariant 2

`autospec-orchestrator` determines **where and how coding workloads execute**.

## Invariant 3

InferWeave determines **where and how model inference executes**.

## Invariant 4

Workbench is primarily an interaction and presentation layer, not another execution engine.

## Invariant 5

Every autonomous source-mutating task executes in an isolated worktree.

## Invariant 6

Every potentially conflicting execution receives an isolated runtime environment.

## Invariant 7

Implementation and independent review must not depend on the same mutable runtime state.

## Invariant 8

Pi sessions are persistent independently from containers.

## Invariant 9

Execution resources are disposable.

Execution records, agent sessions, and evidence are durable.

## Invariant 10

Runtime cleanup belongs to orchestrator and is ownership-aware.

## Invariant 11

Model-serving lifecycle belongs exclusively to InferWeave.

## Invariant 12

No execution gets unrestricted host Docker access by default.

## Invariant 13

One failing execution must not destabilize another execution or its worker.

## Invariant 14

No permanent duplicate execution engine should remain in AutoSpec or Workbench after migration.

---

# 102. Required Documentation Updates

Once this architecture is accepted, update documentation in:

## `autospec`

Replace references to:

```text
AutoSpec starts agent
AutoSpec creates worktree
AutoSpec manages Docker
```

with:

```text
AutoSpec schedules execution through autospec-orchestrator.
```

## `inferweave-workbench`

Replace:

```text
Workbench launches Pi/OpenCode server.
```

with:

```text
Workbench creates/connects to orchestrated execution environments.
```

## `inferweave`

Clarify:

```text
InferWeave provides inference.
It does not execute coding workspaces.
```

## `autospec-orchestrator`

Document the complete execution-plane responsibility.

---

# 103. Required Issue Audit

Before implementation proceeds significantly, audit existing AutoSpec and Workbench issues for overlap.

Search for issues involving:

```text
agent spawning
Pi lifecycle
OpenCode lifecycle
worktrees
Docker cleanup
Docker orchestration
service startup
PostgreSQL test environment
Redis test environment
worker management
agent distribution
session persistence
process recovery
execution resource limits
execution logs
artifact collection
```

Each overlapping issue should be marked:

```text
KEEP IN AUTOSPEC
MOVE TO ORCHESTRATOR
SUPERSEDED
PARTIALLY SUPERSEDED
```

Do not implement overlapping functionality in two repositories.

---

# 104. Recommended Supersession Labels

Create GitHub labels:

```text
architecture:moved-to-orchestrator
architecture:superseded
architecture:control-plane
architecture:execution-plane
architecture:inference-plane
```

When an existing issue is moved:

```text
Do not simply close it.

Add:
Superseded by InferWeave/autospec-orchestrator#XYZ
```

This preserves historical traceability.

---

# 105. Migration ADR

Create an Architecture Decision Record in AutoSpec:

```text
ADR: Extract Agent Execution into autospec-orchestrator
```

It should explain:

* problem;
* previous architecture;
* new three-plane architecture;
* ownership;
* migration;
* deprecated components;
* consequences.

Equivalent cross-references should exist in Workbench and Orchestrator.

---

# 106. Implementation Priority

The orchestrator should now become a relatively high-priority AutoSpec dependency.

Recommended development order:

```text
1. autospec-orchestrator execution model
2. Docker execution MVP
3. Worktree manager
4. Pi RPC harness
5. PostgreSQL/Redis isolation
6. concurrency
7. cleanup/reconciliation
8. execution API
9. AutoSpec adapter
10. review execution
11. Workbench integration
12. remove legacy AutoSpec executor
13. Apptainer
14. additional harnesses
```

---

# 107. What Not to Implement Before Orchestrator

Avoid substantial new work inside AutoSpec on:

* sophisticated local agent supervisor;
* distributed agent processes;
* container-per-issue logic;
* service-isolation framework;
* Pi restart/resume infrastructure;
* Docker garbage collector;
* remote agent workers.

Those should now be implemented in `autospec-orchestrator`.

Likewise, avoid implementing those independently in Workbench.

---

# 108. Initial Integration Milestone

The first cross-system milestone should demonstrate:

```text
GitHub issue
    ↓
AutoSpec creates task packet
    ↓
AutoSpec calls orchestrator
    ↓
orchestrator assigns worker
    ↓
creates worktree
    ↓
creates Docker environment
    ↓
starts PostgreSQL + Redis
    ↓
starts Pi
    ↓
Pi calls InferWeave/Qwen3.8
    ↓
Pi implements issue
    ↓
tests pass
    ↓
orchestrator returns diff/evidence
    ↓
AutoSpec schedules independent review
    ↓
new isolated environment
    ↓
review completes
    ↓
AutoSpec opens/updates PR
```

At that point, the architectural separation is proven.

---

# 109. Second Integration Milestone

Demonstrate Workbench attachment:

```text
AutoSpec execution running
        ↓
user clicks "Open in Workbench"
        ↓
Workbench displays existing Pi session
        ↓
user interacts with agent
        ↓
same execution continues
        ↓
user leaves browser
        ↓
execution remains alive
        ↓
user returns later
```

No Workbench-managed Pi process should be required.

---

# 110. Third Integration Milestone

Demonstrate distributed workers:

```text
Issue A
→ Linux Docker worker

Issue B
→ Mac worker

Issue C
→ HPC Apptainer worker
```

All use the same orchestrator API and all call InferWeave independently.

---

# 111. Fourth Integration Milestone

Remove obsolete code.

Delete or disable permanently:

```text
AutoSpec legacy local Pi supervisor
AutoSpec execution Docker lifecycle
AutoSpec runtime worktree lifecycle
AutoSpec execution cleanup scripts
Workbench local Pi supervisor
Workbench execution Docker lifecycle
```

A codebase search should verify there is only one authoritative implementation of each execution concern.

---

# 112. Final Ownership Summary

The resulting ecosystem should be understandable in four sentences:

**AutoSpec manages the software-development workflow.**

**AutoSpec Orchestrator manages isolated execution environments for agents and humans.**

**InferWeave manages models, GPUs, inference routing, and inference capacity.**

**InferWeave Workbench provides the persistent interactive user experience over those execution environments.**

Anything that violates those boundaries should require an explicit architecture decision.

---

# 113. Definition of Done

This architecture migration is complete when:

* AutoSpec never directly starts Pi for normal production execution;
* AutoSpec never directly owns execution Docker containers;
* AutoSpec never directly owns execution worktrees;
* Workbench does not maintain a second Pi/container supervisor;
* orchestrator is the source of truth for execution environments;
* InferWeave is the sole source of truth for model/GPU inference placement;
* AutoSpec is the source of truth for task/review/PR workflow;
* Workbench can attach to orchestrated sessions;
* implementation and review environments are isolated;
* execution resources are automatically reconciled and cleaned;
* old execution and cleanup implementations have been removed rather than maintained indefinitely;
* all surviving architecture documentation reflects the new control-plane/execution-plane/inference-plane split.

The most important outcome is not simply adding another repository.

It is eliminating overlapping responsibility.

`autospec-orchestrator` should become the **single execution substrate** used by AutoSpec, Workbench, autonomous agents, reviewers, and eventually interactive development sessions. Everything above it decides what should happen; everything below it provides resources needed to make that execution happen.
