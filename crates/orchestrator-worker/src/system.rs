use crate::{ExecutionLifecycle, LifecycleError};
use async_trait::async_trait;
use execution_storage::{AllocationReceipt, AllocationRequest, ExecutionStorageManager};
use git_worktree::{DiffCapture, Worktree, WorktreeManager};
use harness_traits::{AgentHarness, SessionRef};
use orchestrator_core::{Execution, ExecutionEvent, ExecutionId, TaskPacket};
use runtime_traits::{EnvironmentHandle, Runtime};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[async_trait]
pub trait RuntimeFactory: Send + Sync {
    async fn build(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError>;
}

#[async_trait]
pub trait HarnessFactory: Send + Sync {
    async fn build(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        worktree: &Worktree,
    ) -> Result<Arc<dyn AgentHarness>, LifecycleError>;
}

#[async_trait]
pub trait EvidenceStore: Send + Sync {
    async fn persist(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError>;
}

/// Production composition of the worker lifecycle's independently testable
/// storage, Git, runtime, harness, and evidence boundaries.
pub struct SystemExecutionLifecycle {
    storage: Arc<dyn ExecutionStorageManager>,
    worktrees: Arc<dyn WorktreeManager>,
    runtimes: Arc<dyn RuntimeFactory>,
    harnesses: Arc<dyn HarnessFactory>,
    evidence: Arc<dyn EvidenceStore>,
    active_runtimes: Mutex<BTreeMap<ExecutionId, Arc<dyn Runtime>>>,
    active_harnesses: Mutex<BTreeMap<ExecutionId, Arc<dyn AgentHarness>>>,
}

impl SystemExecutionLifecycle {
    pub fn new(
        storage: Arc<dyn ExecutionStorageManager>,
        worktrees: Arc<dyn WorktreeManager>,
        runtimes: Arc<dyn RuntimeFactory>,
        harnesses: Arc<dyn HarnessFactory>,
        evidence: Arc<dyn EvidenceStore>,
    ) -> Self {
        Self {
            storage,
            worktrees,
            runtimes,
            harnesses,
            evidence,
            active_runtimes: Mutex::new(BTreeMap::new()),
            active_harnesses: Mutex::new(BTreeMap::new()),
        }
    }

    fn runtime(&self, id: &ExecutionId) -> Result<Arc<dyn Runtime>, LifecycleError> {
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .get(id)
            .cloned()
            .ok_or_else(|| LifecycleError::Step(format!("runtime is not active for {id}")))
    }

    fn harness(&self, id: &ExecutionId) -> Result<Arc<dyn AgentHarness>, LifecycleError> {
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .get(id)
            .cloned()
            .ok_or_else(|| LifecycleError::Step(format!("harness is not active for {id}")))
    }
}

#[async_trait]
impl ExecutionLifecycle for SystemExecutionLifecycle {
    async fn allocate(&self, execution: &Execution) -> Result<AllocationReceipt, LifecycleError> {
        let storage = Arc::clone(&self.storage);
        let request = AllocationRequest {
            labels: execution.labels.clone(),
            disk_gib: execution.manifest.runtime.disk_gib,
        };
        tokio::task::spawn_blocking(move || storage.allocate(&request))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn create_worktree(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Worktree, LifecycleError> {
        let worktrees = Arc::clone(&self.worktrees);
        let labels = execution.labels.clone();
        let repository = execution.manifest.repository.clone();
        let receipt = receipt.clone();
        tokio::task::spawn_blocking(move || {
            let base = repository
                .base_sha
                .as_deref()
                .unwrap_or(&repository.base_ref);
            let branch = repository.branch.as_deref().ok_or_else(|| {
                LifecycleError::Step("execution lacks an AutoSpec-provided branch".into())
            })?;
            worktrees
                .create_in(&labels, &repository.repo, base, branch, &receipt)
                .map_err(|error| LifecycleError::Step(error.to_string()))
        })
        .await
        .map_err(|error| LifecycleError::Step(error.to_string()))?
    }

    async fn provision(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        _: &Worktree,
    ) -> Result<EnvironmentHandle, LifecycleError> {
        let runtime = self.runtimes.build(execution, receipt).await?;
        let environment = runtime
            .provision(
                &execution.labels,
                &execution.manifest.runtime,
                &execution.manifest.services,
            )
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .insert(execution.id.clone(), runtime);
        Ok(environment)
    }

    async fn start(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        worktree: &Worktree,
        packet: &TaskPacket,
    ) -> Result<SessionRef, LifecycleError> {
        let harness = self
            .harnesses
            .build(execution, receipt, environment, worktree)
            .await?;
        let session = harness
            .start(packet)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .insert(execution.id.clone(), harness);
        Ok(session)
    }

    async fn resume(
        &self,
        execution: &Execution,
        _: &AllocationReceipt,
        _: &EnvironmentHandle,
        session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        self.harness(&execution.id)?
            .resume(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn poll(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<Vec<ExecutionEvent>, LifecycleError> {
        self.harness(&execution.id)?
            .poll_events(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn stop(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        let harness = self.harness(&execution.id)?;
        harness
            .stop(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .remove(&execution.id);
        Ok(())
    }

    async fn capture(&self, worktree: &Worktree) -> Result<DiffCapture, LifecycleError> {
        let manager = Arc::clone(&self.worktrees);
        let worktree = worktree.clone();
        tokio::task::spawn_blocking(move || manager.capture_diff(&worktree))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn persist_evidence(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError> {
        self.evidence.persist(execution, capture).await
    }

    async fn destroy_runtime(&self, execution: &Execution) -> Result<(), LifecycleError> {
        let runtime = self.runtime(&execution.id)?;
        runtime
            .destroy(&execution.labels)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .remove(&execution.id);
        Ok(())
    }

    async fn destroy_worktree(&self, worktree: &Worktree) -> Result<(), LifecycleError> {
        let manager = Arc::clone(&self.worktrees);
        let worktree = worktree.clone();
        tokio::task::spawn_blocking(move || manager.destroy(&worktree))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn release_storage(&self, receipt: &AllocationReceipt) -> Result<(), LifecycleError> {
        let storage = Arc::clone(&self.storage);
        let receipt = receipt.clone();
        tokio::task::spawn_blocking(move || storage.release(&receipt))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }
}
