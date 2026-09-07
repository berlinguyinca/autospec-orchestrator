# Task 3 report: physical Git isolation and evidence

Status: complete.

## Implemented

- Added `GitWorktreeManager` with fs2-locked bare mirror creation and refresh.
- Kept repository identity canonical as exact `owner/name`, resolved clone locators through GitHub HTTPS by default, and added an explicit clone-base constructor for local repositories.
- Normalized mirror names to injective `{owner}__{repo}.git` paths by rejecting `__` inside canonical components, owner components ending in `_`, and repository-name components starting with `_`, while retaining unambiguous single underscores elsewhere.
- Verified every existing mirror's `origin` exactly matches the configured canonical clone locator before fetch; mismatches fail without mutating refs.
- Created execution-scoped linked worktrees under `worktrees/{execution_id}` with branch locking, resolved `base_sha`, and atomic `.autospec-owner.json` records containing the canonical ownership label map.
- Hardened owner-record creation against committed metadata and temp-path symlinks with pre-existing-path rejection, `create_new`, file sync, atomic rename, and directory sync.
- Persisted the exact created branch in owner records and require the handle, record, and checkout to agree before deletion.
- Appended canonical repository identity to trusted worktree handles and require it to agree with owner records for evidence capture and cleanup.
- Made owner-record failure rollback attempt both exact worktree and exact branch cleanup under locks while preserving all rollback errors.
- Changed the sanctioned synchronous `WorktreeManager::capture_diff` return type to `DiffCapture` and captured deterministic changed-file lists plus patches for tracked and untracked files while excluding manager metadata.
- Require evidence capture's handle repository/branch, owner repository/branch, and live checked-out branch to agree before reading diffs.
- Restricted diff capture and destruction to exact, non-symlinked execution paths with matching managed owner records, execution IDs, base SHAs, repositories, and branches.
- Journaled exact cleanup ownership before mutation so partial worktree/branch cleanup remains safely retryable.
- Removed only the verified linked worktree and its exact recorded branch; no broad branch enumeration/deletion or shell-string commands are used.
- Added owner-record-driven stale worktree discovery that rejects symlink directories and surfaces malformed records as anomalies without deleting resources.

## Verification

- `cargo test -p git-worktree` — 25 real temporary Git repository tests passed, including the separator-boundary collision regression.
- `cargo build --workspace` and `cargo +1.85.0 build --workspace` — passed.
- `cargo test --workspace` and `cargo +1.85.0 test --workspace` — passed, including all eleven real Docker integration tests.
- `cargo clippy --workspace --all-targets -- -D warnings` and the same command under Rust 1.85 — passed.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

## Remaining concerns

None known in Task 3 scope. The prior concurrent Docker-test collision was resolved by the integrated unique-test-ID hardening and the full suite now passes.
