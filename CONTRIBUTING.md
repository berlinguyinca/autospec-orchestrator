# Contributing

Read [`AGENTS.md`](AGENTS.md) first. It states the boundaries this repository
exists to enforce; a change that violates one of them will not be merged
regardless of how well it works.

## Before opening an issue

State the destination architecture explicitly. If the work belongs in AutoSpec
or InferWeave, open it there instead — overlapping functionality must not be
implemented in two repositories.

Label architectural issues with one of:

* `architecture:control-plane`
* `architecture:execution-plane`
* `architecture:inference-plane`
* `architecture:moved-to-orchestrator`
* `architecture:superseded`

When an issue moves, do not simply close the original — cross-reference it so
the history stays traceable.

## Before opening a PR

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

New runtime resources must carry ownership labels and must be removable by
label selector alone.
