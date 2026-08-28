//! Physical execution Git state (spec section 8).
//!
//! AutoSpec owns semantic Git state — branch naming, PR relationships, merge
//! gates. This crate owns only the physical side: mirrors, fetch, worktrees,
//! locking, diff capture, and ownership-aware cleanup.

mod cleanup;
mod diff;
mod lock;
mod manager;

use orchestrator_core::{ExecutionId, OwnershipLabels};
use thiserror::Error;

pub use manager::GitWorktreeManager;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffCapture {
    pub patch: String,
    pub changed_files: Vec<String>,
}

#[derive(Debug, Error)]
pub enum WorktreeError {
    #[error("mirror update failed: {0}")]
    Mirror(String),
    #[error("worktree creation failed: {0}")]
    Create(String),
    #[error("worktree is locked by {0}")]
    Locked(ExecutionId),
    #[error("cleanup failed: {0}")]
    Cleanup(String),
    #[error("invalid repository reference: {0}")]
    InvalidRepository(String),
    #[error("diff capture failed: {0}")]
    Diff(String),
    #[error("worktree ownership verification failed: {0}")]
    Ownership(String),
}

#[derive(Debug, Clone)]
pub struct Worktree {
    pub execution_id: ExecutionId,
    pub path: String,
    pub branch: String,
    pub base_sha: String,
    pub repository: String,
}

/// Manages the per-worker repository mirror cache and the worktrees carved from
/// it. Never performs `git branch | grep | xargs -D` style cleanup
/// (spec section 43).
pub trait WorktreeManager: Send + Sync {
    /// Ensure a bare mirror of `repo` exists locally and is up to date.
    fn ensure_mirror(&self, repo: &str) -> Result<String, WorktreeError>;

    /// Create an isolated worktree at `base_ref`, checked out on `branch`.
    fn create(
        &self,
        labels: &OwnershipLabels,
        repo: &str,
        base_ref: &str,
        branch: &str,
    ) -> Result<Worktree, WorktreeError>;

    /// Capture the diff produced by an execution as an artifact payload.
    fn capture_diff(&self, worktree: &Worktree) -> Result<DiffCapture, WorktreeError>;

    /// Remove exactly the worktree and branch this execution owns.
    fn destroy(&self, worktree: &Worktree) -> Result<(), WorktreeError>;

    /// Report worktrees whose owning execution is no longer live.
    fn find_stale(&self, live: &[ExecutionId]) -> Result<Vec<Worktree>, WorktreeError>;
}
