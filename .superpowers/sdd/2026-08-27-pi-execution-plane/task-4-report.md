# Task 4 Report

## Status

Complete. The Pi 0.84.3 harness now launches a real process with one compact
`TaskPacket` file, exact durable session paths/IDs, explicit tool/skill scopes,
and carried model policy without selecting a model. Host-side ownership,
per-session byte cursors, resume counts, process-group cleanup, native session
resume/fork, normalized sequence-zero events, unknown-event accounting, and
inactivity/wall-clock/CPU-saturation classification are implemented.

## Commits

- `279b7fb` — implementation and tests.
- Report commit — this report.

## Tests

- `cargo test -p harness-pi` — 6 passed; real stub `pi` executable/process,
  including compact arguments, missing binary, TERM→KILL, partial JSONL,
  durable cursor, unknown records, model errors, owner validation, torn tails,
  resume cap, no replay, and same-worktree conversation fork.
- `cargo test -p orchestrator-worker --test health` — 3 passed; half-window
  inactivity warning/reset, inactivity and wall-clock failures, and sustained
  CPU saturation.
- `cargo build --workspace` — passed.
- `cargo test --workspace` — passed, including real Docker and PostgreSQL suites.
- `cargo clippy --workspace --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.

## Concerns

- Live InferWeave inference was not exercised; the installed Pi 0.84.3 CLI/help
  and package source were used as the authoritative process/session interface.
- Runtime/container wiring remains the worker run-loop task; this harness accepts
  the exact executable and mounted host paths supplied for an execution.
