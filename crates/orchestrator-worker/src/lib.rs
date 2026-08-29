//! Worker execution lifecycle (spec sections 33, 62, 70).

mod cleanup_guard;
mod health;
mod recovery;
mod run;
mod system;

pub use health::{HealthAssessment, HealthMonitor};
pub use recovery::{RecoveryAuthority, RecoveryCoordinator, RecoveryDisposition};
pub use system::{
    ContentAddressedEvidenceStore, EvidenceStore, FilesystemEvidenceStore, HarnessFactory,
    RuntimeFactory, SystemExecutionLifecycle, SystemRecoveryConfig, VerifiedDockerRuntimeFactory,
    VerifiedPiHarnessFactory,
};

use async_trait::async_trait;
use execution_storage::AllocationReceipt;
use git_worktree::{DiffCapture, Worktree};
use harness_traits::SessionRef;
use orchestrator_core::{
    Execution, ExecutionEvent, ExecutionId, ExecutionResult, TaskPacket, WorkerId,
};
use orchestrator_persistence::{
    CleanupAuthority, CleanupAuthorityStore, CleanupDisposition, ExecutionStore,
    PendingCancellation, PendingExecutionControl, ReservationStore, StoreError,
};
use runtime_traits::EnvironmentHandle;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::{collections::BTreeSet, sync::Mutex};
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
    async fn pause(
        &self,
        _execution: &Execution,
        _session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        Err(LifecycleError::Step(
            "execution lifecycle does not support pause".into(),
        ))
    }
    async fn fork_conversation(
        &self,
        _execution: &Execution,
        _session: &SessionRef,
        _target: &orchestrator_core::SessionId,
    ) -> Result<SessionRef, LifecycleError> {
        Err(LifecycleError::Step(
            "execution lifecycle does not support conversation fork".into(),
        ))
    }
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
    async fn ack_destroy_worktree(&self, _worktree: &Worktree) -> Result<(), LifecycleError> {
        Ok(())
    }
    async fn recover_interrupted_worktree(
        &self,
        _execution: &Execution,
        _receipt: Option<&AllocationReceipt>,
    ) -> Result<(), LifecycleError> {
        Ok(())
    }
    async fn release_storage(&self, receipt: &AllocationReceipt) -> Result<(), LifecycleError>;
    async fn ack_release_storage(
        &self,
        _receipt: &AllocationReceipt,
    ) -> Result<(), LifecycleError> {
        Ok(())
    }
    async fn cleanup_authority(
        &self,
        _authority: &CleanupAuthority,
        _execution: &Execution,
    ) -> Result<(), LifecycleError> {
        Err(LifecycleError::Step(
            "execution lifecycle does not support durable authority cleanup".into(),
        ))
    }

    async fn cleanup_authority_step(
        &self,
        authority: &CleanupAuthority,
        execution: &Execution,
        disposition: CleanupDisposition,
    ) -> Result<CleanupDisposition, LifecycleError> {
        self.cleanup_authority(authority, execution).await?;
        match disposition {
            CleanupDisposition::CleanupPending => Ok(CleanupDisposition::RuntimeStopped),
            CleanupDisposition::RuntimeStopped => Ok(CleanupDisposition::RuntimeDestroyed),
            CleanupDisposition::RuntimeDestroyed => Ok(CleanupDisposition::GitRecoveredCleaned),
            CleanupDisposition::GitRecoveredCleaned => Ok(CleanupDisposition::StorageReleased),
            other => Err(LifecycleError::Step(format!(
                "cleanup step cannot advance {other}"
            ))),
        }
    }

    async fn ack_cleanup_authority_step(
        &self,
        _authority: &CleanupAuthority,
        _disposition: CleanupDisposition,
    ) -> Result<(), LifecycleError> {
        Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlCheckpoint {
    ApplyingPersisted,
    PauseStopped,
    ResumeLaunched,
    ForkLaunched,
    SideEffectPersisted,
    BeforeCompletion,
}

pub trait ControlCheckpointObserver: Send + Sync {
    fn reached(&self, checkpoint: ControlCheckpoint);
}

struct NoopControlCheckpointObserver;

impl ControlCheckpointObserver for NoopControlCheckpointObserver {
    fn reached(&self, _: ControlCheckpoint) {}
}

#[derive(Clone)]
pub struct Worker {
    lifecycle: Arc<dyn ExecutionLifecycle>,
    executions: Arc<dyn ExecutionStore>,
    reservations: Arc<dyn ReservationStore>,
    cleanup_authorities: Arc<dyn CleanupAuthorityStore>,
    control_checkpoints: Arc<dyn ControlCheckpointObserver>,
}

pub struct ExecutionTask {
    execution_id: ExecutionId,
    cancelled: Arc<AtomicBool>,
    join: tokio::task::JoinHandle<Result<ExecutionResult, WorkerError>>,
    controls: tokio::sync::mpsc::UnboundedSender<PendingExecutionControl>,
    signalled_controls: Mutex<BTreeSet<i64>>,
}

impl ExecutionTask {
    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn signal_control(&self, control: PendingExecutionControl) -> Result<(), WorkerError> {
        let mut signalled = self
            .signalled_controls
            .lock()
            .map_err(|_| WorkerError::Invalid("control signal lock poisoned".into()))?;
        if !signalled.insert(control.request.request_id) {
            return Ok(());
        }
        self.controls
            .send(control)
            .map_err(|_| WorkerError::Invalid("execution control channel closed".into()))
    }

    pub async fn observe_cancellation(
        &self,
        executions: &dyn ExecutionStore,
    ) -> Result<bool, StoreError> {
        let requested = executions
            .cancellation_requested(&self.execution_id)
            .await?;
        if requested {
            self.cancel();
        }
        Ok(requested)
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
            control_checkpoints: Arc::new(NoopControlCheckpointObserver),
        }
    }

    pub fn with_control_checkpoint_observer(
        mut self,
        observer: Arc<dyn ControlCheckpointObserver>,
    ) -> Self {
        self.control_checkpoints = observer;
        self
    }

    pub async fn run(&self, execution: &Execution) -> Result<ExecutionResult, WorkerError> {
        run::run(self, execution).await
    }

    pub fn spawn(self: Arc<Self>, execution: Execution) -> ExecutionTask {
        let execution_id = execution.id.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        let (controls, receiver) = tokio::sync::mpsc::unbounded_channel();
        let join = tokio::spawn(async move {
            run::run_with_cancel_and_controls(&self, &execution, task_cancelled.as_ref(), receiver)
                .await
        });
        ExecutionTask {
            execution_id,
            cancelled,
            join,
            controls,
            signalled_controls: Mutex::new(BTreeSet::new()),
        }
    }

    pub fn spawn_adopted(self: Arc<Self>, execution: Execution) -> ExecutionTask {
        let execution_id = execution.id.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = Arc::clone(&cancelled);
        let (controls, receiver) = tokio::sync::mpsc::unbounded_channel();
        let join = tokio::spawn(async move {
            run::run_adopted_with_cancel_and_controls(
                &self,
                &execution,
                task_cancelled.as_ref(),
                receiver,
            )
            .await
        });
        ExecutionTask {
            execution_id,
            cancelled,
            join,
            controls,
            signalled_controls: Mutex::new(BTreeSet::new()),
        }
    }

    /// Runs the production daemon's durable reconciliation tick. Active tasks
    /// are all signalled before any fallible cleanup recovery is attempted.
    pub async fn reconcile_daemon_tick(
        &self,
        worker_id: &WorkerId,
        tasks: &[ExecutionTask],
    ) -> Result<(), WorkerError> {
        let pending = self
            .executions
            .list_pending_cancellations()
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        for request in &pending {
            if request.worker_id.as_ref() != Some(worker_id) {
                continue;
            }
            if let Some(task) = tasks
                .iter()
                .find(|task| task.execution_id() == &request.execution.id)
            {
                task.cancel();
            }
        }

        let controls = self
            .executions
            .list_pending_controls(worker_id)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        for control in controls {
            if let Some(task) = tasks
                .iter()
                .find(|task| task.execution_id() == &control.execution.id)
            {
                task.signal_control(control)?;
            } else {
                self.executions
                    .begin_control(
                        control.request.request_id,
                        &control.accepted_worker_id,
                        &control.accepted_attempt_id,
                    )
                    .await
                    .map_err(|error| WorkerError::Persistence(error.to_string()))?;
            }
        }

        let mut errors = Vec::new();
        for request in &pending {
            if request.worker_id.as_ref() != Some(worker_id)
                || tasks
                    .iter()
                    .any(|task| task.execution_id() == &request.execution.id)
            {
                continue;
            }
            if let Err(error) = self
                .reconcile_pending_cancellation(worker_id, request)
                .await
            {
                errors.push(format!("execution {}: {error}", request.execution.id));
            }
        }

        match self.cleanup_authorities.list_for_worker(worker_id).await {
            Ok(authorities) => {
                for authority in authorities {
                    if pending
                        .iter()
                        .any(|request| request.execution.id == authority.execution_id)
                    {
                        continue;
                    }
                    let Ok(disposition) = authority.disposition() else {
                        continue;
                    };
                    if !matches!(
                        disposition,
                        CleanupDisposition::CleanupPending
                            | CleanupDisposition::RuntimeStopped
                            | CleanupDisposition::RuntimeDestroyed
                            | CleanupDisposition::GitRecoveredCleaned
                            | CleanupDisposition::StorageReleased
                            | CleanupDisposition::ReservationReleased
                    ) {
                        continue;
                    }
                    match self.executions.get(&authority.execution_id).await {
                        Ok(execution) => {
                            if let Err(error) =
                                self.recover_cleanup_authority(&authority, &execution).await
                            {
                                errors
                                    .push(format!("execution {}: {error}", authority.execution_id));
                            }
                        }
                        Err(error) => errors.push(format!(
                            "execution {} is unavailable: {error}",
                            authority.execution_id
                        )),
                    }
                }
            }
            Err(error) => errors.push(format!("list cleanup authorities: {error}")),
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(WorkerError::Cleanup(errors.join("; ")))
        }
    }

    async fn reconcile_pending_cancellation(
        &self,
        worker_id: &WorkerId,
        request: &PendingCancellation,
    ) -> Result<(), WorkerError> {
        let authority = match self.cleanup_authorities.get(&request.execution.id).await {
            Ok(authority) => authority,
            Err(StoreError::NotFound(_)) => {
                let attempt_id = request.attempt_id.as_ref().ok_or_else(|| {
                    WorkerError::Invalid(format!(
                        "pending cancellation {} lacks attempt authority",
                        request.execution.id
                    ))
                })?;
                self.cleanup_authorities
                    .begin(&request.execution.id, attempt_id, worker_id)
                    .await
                    .map_err(|error| WorkerError::Persistence(error.to_string()))?;
                self.cleanup_authorities
                    .get(&request.execution.id)
                    .await
                    .map_err(|error| WorkerError::Persistence(error.to_string()))?
            }
            Err(error) => return Err(WorkerError::Persistence(error.to_string())),
        };
        self.recover_cleanup_authority(&authority, &request.execution)
            .await
    }

    pub async fn recover_cleanup_authority(
        &self,
        authority: &CleanupAuthority,
        execution: &Execution,
    ) -> Result<(), WorkerError> {
        let mut disposition = authority
            .disposition()
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        let cancellation_pending = self
            .executions
            .cancellation_requested(&authority.execution_id)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        if disposition == CleanupDisposition::Retained && !cancellation_pending {
            return Ok(());
        }
        if disposition == CleanupDisposition::Resolved {
            if cancellation_pending {
                self.executions
                    .complete_cancellation(&authority.execution_id, &authority.attempt_id)
                    .await
                    .map_err(|error| WorkerError::Persistence(error.to_string()))?;
            }
            return Ok(());
        }
        if !matches!(
            disposition,
            CleanupDisposition::RuntimeStopped
                | CleanupDisposition::RuntimeDestroyed
                | CleanupDisposition::GitRecoveredCleaned
                | CleanupDisposition::StorageReleased
                | CleanupDisposition::ReservationReleased
        ) && disposition != CleanupDisposition::CleanupPending
        {
            self.cleanup_authorities
                .fence_for_cleanup(&authority.execution_id, &authority.handles)
                .await
                .map_err(|error| WorkerError::Persistence(error.to_string()))?;
            disposition = CleanupDisposition::CleanupPending;
        }
        while matches!(
            disposition,
            CleanupDisposition::CleanupPending
                | CleanupDisposition::RuntimeStopped
                | CleanupDisposition::RuntimeDestroyed
                | CleanupDisposition::GitRecoveredCleaned
        ) {
            if disposition != CleanupDisposition::GitRecoveredCleaned {
                self.lifecycle
                    .ack_cleanup_authority_step(authority, disposition)
                    .await?;
            }
            let next = self
                .lifecycle
                .cleanup_authority_step(authority, execution, disposition)
                .await?;
            self.cleanup_authorities
                .transition(
                    &authority.execution_id,
                    disposition,
                    next,
                    &authority.handles,
                )
                .await
                .map_err(|error| WorkerError::Persistence(error.to_string()))?;
            disposition = next;
        }
        if disposition == CleanupDisposition::StorageReleased {
            self.reservations
                .finalize_cleanup(&authority.execution_id, &authority.attempt_id)
                .await
                .map_err(|error| WorkerError::Persistence(error.to_string()))?;
            disposition = CleanupDisposition::ReservationReleased;
        }
        if disposition == CleanupDisposition::ReservationReleased {
            self.lifecycle
                .ack_cleanup_authority_step(authority, disposition)
                .await?;
            self.cleanup_authorities
                .resolve(&authority.execution_id)
                .await
                .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        }
        if cancellation_pending {
            self.executions
                .complete_cancellation(&authority.execution_id, &authority.attempt_id)
                .await
                .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        }
        Ok(())
    }
}
