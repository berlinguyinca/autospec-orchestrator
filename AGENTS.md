# Agent instructions for autospec-orchestrator

## The one rule

Before adding anything, answer: **is this deciding what work should happen,
executing that work, or serving inference?**

* Deciding what work should happen → belongs in **AutoSpec**, not here.
* Executing coding workloads → belongs **here**.
* Serving model inference → belongs in **InferWeave**, not here.

## Hard boundaries

Never add to this repository:

* GitHub PR, issue, label, or Project/Kanban management
* DAG or dependency planning
* separation-of-duties or model-role policy decisions
* GPU selection, model loading, inference queueing, or token accounting
  (`selectGpu()`, `loadModel()`, `gpuQueue()`, `modelPlacement()` are forbidden)
* model-serving processes (vLLM, MLX, llama.cpp, SGLang)
* imports of AutoSpec business logic — use the neutral types in
  `orchestrator-core` (`Execution`, `TaskReference`, `RepositoryReference`,
  `AgentAssignment`, `RuntimeRequirement`)

## Non-negotiable invariants

1. Every source-mutating task runs in an isolated worktree.
2. Every potentially conflicting execution gets an isolated runtime environment.
3. Implementation and independent review never share mutable runtime state.
4. Harness sessions persist independently of containers.
5. Execution resources are disposable; execution records and evidence are durable.
6. **Every** created resource carries `autospec.managed=true` and
   `autospec.execution_id=…`. Cleanup selects on those labels only.
7. Global `docker * prune` and `git branch | grep … | xargs -D` are forbidden.
   They are dangerous on shared hosts.
8. No execution gets unrestricted host Docker access by default.
9. One failing execution must never destabilise another execution or its worker.
10. Resource limits are enforced by the runtime, not by prompt text. A model
    instruction is not a security boundary.

## Conventions

* Rust 2021, `unsafe_code = "forbid"` workspace-wide.
* Every public API is versioned from day one: `/api/v1`, manifests are
  `autospec.dev/v1alpha1`.
* Correlation IDs use the shared vocabulary: `project_id`, `issue_id`, `task_id`,
  `execution_id`, `attempt_id`, `session_id`, `worker_id`,
  `inferweave_request_id`, `model_id`. Logs must be searchable by `execution_id`.
* Cite the governing spec section in doc comments for non-obvious rules.

## Commands

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```
