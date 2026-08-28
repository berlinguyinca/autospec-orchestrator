use crate::{ExecutionLifecycle, WorkerError};
use execution_storage::AllocationReceipt;
use git_worktree::Worktree;
use harness_traits::SessionRef;
use orchestrator_core::Execution;
use std::sync::Arc;
use std::time::Duration;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct CleanupGuard {
    lifecycle: Arc<dyn ExecutionLifecycle>,
    execution: Execution,
    pub(crate) receipt: Option<AllocationReceipt>,
    pub(crate) worktree: Option<Worktree>,
    pub(crate) session: Option<SessionRef>,
    pub(crate) runtime_created: bool,
}

impl CleanupGuard {
    pub(crate) fn new(lifecycle: Arc<dyn ExecutionLifecycle>, execution: &Execution) -> Self {
        Self {
            lifecycle,
            execution: execution.clone(),
            receipt: None,
            worktree: None,
            session: None,
            runtime_created: false,
        }
    }

    pub(crate) async fn stop_agent(&mut self) -> Result<(), WorkerError> {
        let Some(session) = self.session.as_ref().cloned() else {
            return Ok(());
        };
        tokio::time::timeout(
            CLEANUP_TIMEOUT,
            self.lifecycle.stop(&self.execution, &session),
        )
        .await
        .map_err(|_| WorkerError::Cleanup("timed out stopping Pi".into()))?
        .map_err(WorkerError::from)?;
        self.session = None;
        Ok(())
    }

    pub(crate) async fn cleanup(&mut self) -> Result<(), WorkerError> {
        if self.session.is_some() {
            self.stop_agent().await?;
        }
        if self.runtime_created {
            tokio::time::timeout(
                CLEANUP_TIMEOUT,
                self.lifecycle.destroy_runtime(&self.execution),
            )
            .await
            .map_err(|_| WorkerError::Cleanup("timed out destroying runtime".into()))?
            .map_err(WorkerError::from)?;
            self.runtime_created = false;
        }
        if let Some(worktree) = self.worktree.as_ref().cloned() {
            tokio::time::timeout(CLEANUP_TIMEOUT, self.lifecycle.destroy_worktree(&worktree))
                .await
                .map_err(|_| WorkerError::Cleanup("timed out destroying worktree".into()))?
                .map_err(WorkerError::from)?;
            self.worktree = None;
        }
        if let Some(receipt) = self.receipt.as_ref().cloned() {
            tokio::time::timeout(CLEANUP_TIMEOUT, self.lifecycle.release_storage(&receipt))
                .await
                .map_err(|_| WorkerError::Cleanup("timed out releasing storage".into()))?
                .map_err(WorkerError::from)?;
            self.receipt = None;
        }
        Ok(())
    }
}
