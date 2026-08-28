use crate::{ExecutionLifecycle, WorkerError};
use execution_storage::AllocationReceipt;
use git_worktree::Worktree;
use harness_traits::SessionRef;
use orchestrator_core::{Execution, PersistenceMode};
use std::sync::Arc;

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
        let Some(session) = self.session.take() else {
            return Ok(());
        };
        self.lifecycle
            .stop(&self.execution, &session)
            .await
            .map_err(WorkerError::from)
    }

    pub(crate) async fn cleanup(&mut self) -> Result<(), WorkerError> {
        let mut errors = Vec::new();
        if let Some(session) = self.session.take() {
            if let Err(error) = self.lifecycle.stop(&self.execution, &session).await {
                errors.push(error.to_string());
            }
        }
        if self.runtime_created {
            if let Err(error) = self.lifecycle.destroy_runtime(&self.execution).await {
                errors.push(error.to_string());
            }
            self.runtime_created = false;
        }
        if self.execution.manifest.persistence != PersistenceMode::Resumable {
            if let Some(worktree) = self.worktree.take() {
                if let Err(error) = self.lifecycle.destroy_worktree(&worktree).await {
                    errors.push(error.to_string());
                }
            }
            if let Some(receipt) = self.receipt.take() {
                if let Err(error) = self.lifecycle.release_storage(&receipt).await {
                    errors.push(error.to_string());
                }
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(WorkerError::Cleanup(errors.join("; ")))
        }
    }
}
