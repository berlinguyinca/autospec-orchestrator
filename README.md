# autospec-orchestrator

The **execution plane** for AutoSpec.

`autospec-orchestrator` owns *where and how coding workloads execute*: workers,
isolated worktrees, containers, service dependencies, agent-harness sessions,
resource limits, recovery, artifacts, and ownership-aware cleanup.

It is one of three planes:

| Plane | System | Question it answers |
| --- | --- | --- |
| Control | [`autospec`](https://github.com/berlinguyinca/autospec) | What work should happen? |
| Execution | **`autospec-orchestrator`** | Where and how should the work execute? |
| Inference | [InferWeave](https://github.com/InferWeave) | Where and how should model inference execute? |

```text
GitHub → AutoSpec → autospec-orchestrator → Worker (Docker/Podman/Apptainer)
                                                → Pi harness → InferWeave
```

## What lives here

* worker scheduling and capability matching (CPU/RAM/disk/OS/runtime/toolchain)
* one isolated runtime environment per execution
* physical Git worktrees, mirrors, locks, and diff capture
* the `AgentHarness` abstraction (Pi first; OpenCode/Codex later)
* PostgreSQL, Redis, and other per-execution service containers
* crash recovery, hung-agent detection, cancellation
* execution logs and artifacts
* deterministic, ownership-labelled cleanup

## What does not live here

* GitHub issues, PRs, project boards, review policy → **AutoSpec**
* model routing, GPU selection, model loading, token accounting → **InferWeave**
* browser UI and interactive presentation → **InferWeave Workbench**

See [`docs/specs/three-plane-execution-architecture.md`](docs/specs/three-plane-execution-architecture.md)
for the full specification and [`docs/adr/0001-three-plane-execution-architecture.md`](docs/adr/0001-three-plane-execution-architecture.md)
for the decision record.

## Layout

```text
crates/
  orchestrator-core/       neutral domain types (Execution, manifest, labels, events)
  orchestrator-api/        versioned HTTP surface (/api/v1)
  orchestrator-scheduler/  worker matching on execution resources only
  orchestrator-worker/     the per-execution run loop
  runtime-traits/          Runtime abstraction
  runtime-docker/          Docker adapter (initial runtime)
  harness-traits/          AgentHarness abstraction
  harness-pi/              Pi harness (primary)
  git-worktree/            mirrors, worktrees, locks, diffs, cleanup
  autospec-orchestrator/   controller binary
  autospec-worker/         worker daemon binary
```

## Build

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
```

## Status

Early scaffold. The domain model, trait boundaries, scheduler, and API shape are
in place; runtime, harness, and worktree implementations are being built out
issue by issue.

## License

Apache-2.0
