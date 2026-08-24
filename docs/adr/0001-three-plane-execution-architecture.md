# ADR 0001 — Extract agent execution into autospec-orchestrator

**Status:** Accepted
**Date:** 2026-08-23
**Cross-references:** `autospec` (control plane), `inferweave` (inference plane),
`inferweave-workbench` (presentation)

## Problem

AutoSpec grew from an issue-automation tool into a distributed autonomous
software-development system. Along the way it acquired execution concerns:
spawning coding agents, creating worktrees, starting Docker services, tracking
PIDs, and cleaning up leaked resources.

Running agent workloads inside the AutoSpec process produced recurring failures:
process interference, port collisions, dirty databases, orphaned worktrees,
Docker leaks, branch collisions, resource exhaustion, weak crash recovery, and no
clean path to distributing execution across machines.

InferWeave Workbench was independently heading toward a second, near-identical
process supervisor.

## Previous architecture

AutoSpec directly owned planning *and* execution. Workbench planned to own a
parallel execution path. Both would have needed worktree management, container
lifecycle, service isolation, session persistence, and garbage collection.

## Decision

Split the ecosystem into three planes with one-way dependencies:

```text
AutoSpec ──► autospec-orchestrator ──► InferWeave
Workbench ─►
```

* **AutoSpec (control plane)** decides *what work occurs*: specs, issues, DAG,
  acceptance criteria, model-role policy, separation of duties, review policy,
  PR lifecycle, project boards, retry policy.
* **autospec-orchestrator (execution plane)** decides *where and how coding
  workloads execute*: workers, worktrees, containers, services, harness
  sessions, resource limits, recovery, artifacts, cleanup.
* **InferWeave (inference plane)** decides *where and how model inference
  executes*: auth, model registry, GPU discovery, routing, capacity, quotas,
  token accounting.

Workbench becomes a client of the orchestrator API, not a second execution
engine.

## Ownership

The full source-of-truth matrix lives in
[`docs/architecture/source-of-truth.md`](../architecture/source-of-truth.md).

## Migration

Six phases, incremental, with the legacy AutoSpec executor available behind an
`executionBackend: legacy | orchestrator` switch until the removal gate is met:

1. Introduce orchestrator alongside the legacy executor.
2. Move Pi implementation executions.
3. Move review executions, with fresh environments per review.
4. Move UI, documentation, and integration-test roles.
5. Move Workbench off direct process management.
6. Delete the legacy executor.

The removal gate requires demonstrated concurrent execution, session
persistence, service isolation, clean reviewer environments, cancellation,
resource enforcement, crash recovery, deterministic cleanup, artifact
persistence, production-level logging, and no task loss across worker restart.

## Deprecated components

**AutoSpec:** direct agent spawning, Pi and OpenCode lifecycle, issue-level
Docker creation, local PostgreSQL/Redis lifecycle, execution worktree
implementation, stale-worktree cleanup, global Docker prune, PID tracking, agent
restart code, execution-local port allocation and log storage, runtime resource
accounting.

**Workbench:** Pi and OpenCode launchers, Docker environment manager, worktree
manager, service-container manager, workspace supervisor, cleanup daemon.

**InferWeave:** must not acquire coding workspaces, worktrees, source-repository
lifecycle, build/test orchestration, or harness session management.

## Consequences

*Positive:* execution and inference capacity scale independently; AutoSpec
becomes near-stateless and restartable without losing running work; one
execution substrate serves autonomous tasks, reviewers, and humans; cleanup
becomes ownership-aware and safe on shared hosts; benchmarking becomes
reproducible because only the model varies.

*Negative:* an additional service and API to operate and version; a migration
window in which two execution paths coexist; cross-repository coordination is
required so overlapping issues are not implemented twice.

*Accepted cost:* the migration window. It is bounded by the removal gate — no
permanent duplicate execution engine may remain (invariant 14).
