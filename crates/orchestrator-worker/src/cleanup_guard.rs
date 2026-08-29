use crate::{ExecutionLifecycle, WorkerError};
use execution_storage::AllocationReceipt;
use git_worktree::Worktree;
use harness_traits::SessionRef;
use orchestrator_core::Execution;
use runtime_traits::EnvironmentHandle;
use std::sync::Arc;
use std::time::Duration;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) struct CleanupGuard {
    lifecycle: Arc<dyn ExecutionLifecycle>,
    execution: Execution,
    pub(crate) receipt: Option<AllocationReceipt>,
    pub(crate) worktree: Option<Worktree>,
    pub(crate) environment: Option<EnvironmentHandle>,
    pub(crate) session: Option<SessionRef>,
    pub(crate) runtime_created: bool,
    agent_stopped: bool,
}

impl CleanupGuard {
    pub(crate) fn new(lifecycle: Arc<dyn ExecutionLifecycle>, execution: &Execution) -> Self {
        Self {
            lifecycle,
            execution: execution.clone(),
            receipt: None,
            worktree: None,
            environment: None,
            session: None,
            runtime_created: false,
            agent_stopped: false,
        }
    }

    pub(crate) async fn stop_agent(&mut self) -> Result<(), WorkerError> {
        if self.agent_stopped {
            return Ok(());
        }
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
        self.agent_stopped = true;
        Ok(())
    }

    pub(crate) async fn stop_runtime_process(&mut self) -> Result<(), WorkerError> {
        if self.session.is_some() && !self.agent_stopped {
            self.stop_agent().await?;
        }
        Ok(())
    }

    pub(crate) async fn destroy_runtime(&mut self) -> Result<(), WorkerError> {
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
        Ok(())
    }

    pub(crate) async fn destroy_worktree(&mut self) -> Result<(), WorkerError> {
        if let Some(worktree) = self.worktree.as_ref().cloned() {
            tokio::time::timeout(CLEANUP_TIMEOUT, self.lifecycle.destroy_worktree(&worktree))
                .await
                .map_err(|_| WorkerError::Cleanup("timed out destroying worktree".into()))?
                .map_err(WorkerError::from)?;
        }
        self.lifecycle
            .recover_interrupted_worktree(&self.execution, self.receipt.as_ref())
            .await?;
        Ok(())
    }

    pub(crate) async fn release_storage(&mut self) -> Result<(), WorkerError> {
        if let Some(receipt) = self.receipt.as_ref().cloned() {
            tokio::time::timeout(CLEANUP_TIMEOUT, self.lifecycle.release_storage(&receipt))
                .await
                .map_err(|_| WorkerError::Cleanup("timed out releasing storage".into()))?
                .map_err(WorkerError::from)?;
        }
        Ok(())
    }
}
