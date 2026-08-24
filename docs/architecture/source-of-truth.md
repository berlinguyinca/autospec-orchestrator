# Source-of-truth matrix

Every concern has exactly one authoritative owner. Do not mirror another
system's state and attempt two-way synchronisation.

| Concern | Source of truth |
| --- | --- |
| Requirements | AutoSpec |
| Issue decomposition | AutoSpec |
| DAG | AutoSpec |
| Planner/implementer/reviewer policy | AutoSpec |
| PR lifecycle | AutoSpec |
| GitHub Project/Kanban | AutoSpec |
| Execution ID | Orchestrator |
| Worker assignment | Orchestrator |
| Worktree | Orchestrator |
| Agent process | Orchestrator |
| Agent session persistence | Orchestrator |
| PostgreSQL/Redis environment | Orchestrator |
| CPU/RAM/disk quota | Orchestrator |
| Execution cleanup | Orchestrator |
| Model routing | InferWeave |
| GPU selection | InferWeave |
| Model loading | InferWeave |
| Inference queue | InferWeave |
| Token accounting | InferWeave |
| Browser presentation | Workbench |
| Interactive UX | Workbench |
| Artifact rendering | Workbench |

## Correlation vocabulary

Logs across all four systems must be searchable by these IDs:

`project_id`, `issue_id`, `task_id`, `execution_id`, `attempt_id`, `session_id`,
`worker_id`, `inferweave_request_id`, `model_id`.

## Entity hierarchy

Do not conflate these levels:

```text
Issue #417
└── Task: implement reservation cancellation
    ├── Execution 1 (implementation) → session node-417-impl-01
    ├── Execution 2 (review)         → session node-417-review-01
    └── Execution 3 (fix findings)   → session node-417-impl-02
```
