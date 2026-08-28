//! Worker execution lifecycle (spec sections 33, 62, 70).

mod cleanup_guard;
mod health;
mod recovery;
mod run;
mod system;

pub use health::{HealthAssessment, HealthMonitor};
pub use recovery::{RecoveryAuthority, RecoveryCoordinator, RecoveryDisposition};
pub use system::{EvidenceStore, HarnessFactory, RuntimeFactory, SystemExecutionLifecycle};

use async_trait::async_trait;
use execution_storage::AllocationReceipt;
use git_worktree::{DiffCapture, Worktree};
use harness_traits::SessionRef;
use orchestrator_core::{Execution, ExecutionEvent, ExecutionResult, TaskPacket};
use orchestrator_persistence::{ExecutionStore, ReservationStore};
use runtime_traits::EnvironmentHandle;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("execution lifecycle step failed: {0}")]
    Step(String),
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error("execution persistence failed: {0}")]
    Persistence(String),
    #[error("execution panicked: {0}")]
    Panic(String),
    #[error("cleanup failed: {0}")]
    Cleanup(String),
    #[error("execution failed: {execution}; cleanup also failed: {cleanup}")]
    ExecutionAndCleanup { execution: String, cleanup: String },
    #[error("execution is invalid: {0}")]
    Invalid(String),
    #[error("execution was cancelled")]
    Cancelled,
}

#[async_trait]
pub trait ExecutionLifecycle: Send + Sync {
    async fn allocate(&self, execution: &Execution) -> Result<AllocationReceipt, LifecycleError>;
    async fn create_worktree(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Worktree, LifecycleError>;
    async fn provision(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        worktree: &Worktree,
    ) -> Result<EnvironmentHandle, LifecycleError>;
    async fn start(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        worktree: &Worktree,
        packet: &TaskPacket,
    ) -> Result<SessionRef, LifecycleError>;
    async fn resume(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        session: &SessionRef,
    ) -> Result<(), LifecycleError>;
    async fn poll(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<Vec<ExecutionEvent>, LifecycleError>;
    async fn stop(&self, execution: &Execution, session: &SessionRef)
        -> Result<(), LifecycleError>;
    async fn capture(&self, worktree: &Worktree) -> Result<DiffCapture, LifecycleError>;
    async fn persist_evidence(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError>;
    async fn destroy_runtime(&self, execution: &Execution) -> Result<(), LifecycleError>;
    async fn destroy_worktree(&self, worktree: &Worktree) -> Result<(), LifecycleError>;
    async fn release_storage(&self, receipt: &AllocationReceipt) -> Result<(), LifecycleError>;
}

#[derive(Clone)]
pub struct Worker {
    lifecycle: Arc<dyn ExecutionLifecycle>,
    executions: Arc<dyn ExecutionStore>,
    reservations: Arc<dyn ReservationStore>,
}

pub struct ExecutionTask {
    cancelled: Arc<AtomicBool>,
    join: tokio::task::JoinHandle<Result<ExecutionResult, WorkerError>>,
}

impl ExecutionTask {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub async fn join(self) -> Result<ExecutionResult, WorkerError> {
        self.join
            .await
            .map_err(|error| WorkerError::Panic(error.to_string()))?
    }
}

impl Worker {
    pub fn new(
        lifecycle: Arc<dyn ExecutionLifecycle>,
        executions: Arc<dyn ExecutionStore>,
        reservations: Arc<dyn ReservationStore>,
    ) -> Self {
        Self {
            lifecycle,
            executions,
            reservations,
        }
    }

    pub async fn run(&self, execution: &Execution) -> Result<ExecutionResult, WorkerError> {
        run::run(self, execution).await
    }

    pub fn spawn(self: Arc<Self>, execution: Execution) -> ExecutionTask {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        let join = tokio::spawn(async move {
            run::run_with_cancel(&self, &execution, task_cancelled.as_ref()).await
        });
        ExecutionTask { cancelled, join }
    }
}
