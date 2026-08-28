//! Physical execution Git state (spec section 8).
//!
//! AutoSpec owns semantic Git state — branch naming, PR relationships, merge
//! gates. This crate owns only the physical side: mirrors, independent bounded
//! repositories, locking, diff capture, and ownership-aware cleanup.

mod cleanup;
mod command;
mod diff;
mod filesystem;
mod lock;
mod manager;

use execution_storage::AllocationReceipt;
use orchestrator_core::{ExecutionId, OwnershipLabels};
use thiserror::Error;

pub use filesystem::{SystemWorktreeFilesystem, WorktreeFilesystem, WorktreeFilesystemPoint};
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
    #[error("execution storage is full: {0}")]
    StorageFull(String),
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

/// Manages the per-worker mirror cache and independent execution repositories.
/// Never performs `git branch | grep | xargs -D` style cleanup
/// (spec section 43).
pub trait WorktreeManager: Send + Sync {
    /// Ensure a bare mirror of `repo` exists locally and is up to date.
    fn ensure_mirror(&self, repo: &str) -> Result<String, WorktreeError>;

    /// Legacy unbounded creation entry point. Implementations must fail closed;
    /// callers use [`Self::create_in`] with verified execution storage.
    fn create(
        &self,
        labels: &OwnershipLabels,
        repo: &str,
        base_ref: &str,
        branch: &str,
    ) -> Result<Worktree, WorktreeError>;

    /// Create an independent repository inside a pre-allocated execution
    /// filesystem. All mutable Git state remains below `repository_root`.
    fn create_in(
        &self,
        labels: &OwnershipLabels,
        repo: &str,
        base_ref: &str,
        branch: &str,
        storage: &AllocationReceipt,
    ) -> Result<Worktree, WorktreeError>;

    /// Capture the diff produced by an execution as an artifact payload.
    fn capture_diff(&self, worktree: &Worktree) -> Result<DiffCapture, WorktreeError>;

    /// Remove exactly the independent repository this execution owns.
    fn destroy(&self, worktree: &Worktree) -> Result<(), WorktreeError>;

    /// Report repositories whose owning execution is no longer live.
    fn find_stale(&self, live: &[ExecutionId]) -> Result<Vec<Worktree>, WorktreeError>;
}
