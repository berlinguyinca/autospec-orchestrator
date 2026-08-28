//! Worker execution lifecycle (spec sections 33, 62, 70).

mod cleanup_guard;
mod health;
mod recovery;
mod run;
mod system;

pub use health::{HealthAssessment, HealthMonitor};
pub use recovery::{RecoveryAuthority, RecoveryCoordinator, RecoveryDisposition};
pub use system::{
    EvidenceStore, FilesystemEvidenceStore, HarnessFactory, RuntimeFactory,
    SystemExecutionLifecycle, SystemRecoveryConfig, VerifiedDockerRuntimeFactory,
    VerifiedPiHarnessFactory,
};

use async_trait::async_trait;
use execution_storage::AllocationReceipt;
use git_worktree::{DiffCapture, Worktree};
use harness_traits::SessionRef;
use orchestrator_core::{Execution, ExecutionEvent, ExecutionResult, TaskPacket};
use orchestrator_persistence::{
    CleanupAuthority, CleanupAuthorityStore, ExecutionStore, ReservationStore,
};
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
    async fn cpu_percent(&self, execution: &Execution) -> Result<f64, LifecycleError>;
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
    async fn cleanup_authority(
        &self,
        _authority: &CleanupAuthority,
        _execution: &Execution,
    ) -> Result<(), LifecycleError> {
        Err(LifecycleError::Step(
            "execution lifecycle does not support durable authority cleanup".into(),
        ))
    }

    async fn adopt(&self, _execution: &Execution) -> Result<AdoptedExecution, LifecycleError> {
        Err(LifecycleError::Step(
            "execution lifecycle does not support restart adoption".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct AdoptedExecution {
    pub receipt: AllocationReceipt,
    pub worktree: Worktree,
    pub environment: EnvironmentHandle,
    pub session: SessionRef,
}

#[derive(Clone)]
pub struct Worker {
    lifecycle: Arc<dyn ExecutionLifecycle>,
    executions: Arc<dyn ExecutionStore>,
    reservations: Arc<dyn ReservationStore>,
    cleanup_authorities: Arc<dyn CleanupAuthorityStore>,
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

    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }
}

impl Worker {
    pub fn new(
        lifecycle: Arc<dyn ExecutionLifecycle>,
        executions: Arc<dyn ExecutionStore>,
        reservations: Arc<dyn ReservationStore>,
        cleanup_authorities: Arc<dyn CleanupAuthorityStore>,
    ) -> Self {
        Self {
            lifecycle,
            executions,
            reservations,
            cleanup_authorities,
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

    pub fn spawn_adopted(self: Arc<Self>, execution: Execution) -> ExecutionTask {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        let join = tokio::spawn(async move {
            run::run_adopted_with_cancel(&self, &execution, task_cancelled.as_ref()).await
        });
        ExecutionTask { cancelled, join }
    }

    pub async fn recover_cleanup_authority(
        &self,
        authority: &CleanupAuthority,
        execution: &Execution,
    ) -> Result<(), WorkerError> {
        self.lifecycle
            .cleanup_authority(authority, execution)
            .await?;
        self.reservations
            .release_attempt(&authority.execution_id, &authority.attempt_id)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        self.cleanup_authorities
            .resolve(&authority.execution_id)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))
    }
}
