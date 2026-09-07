use crate::WorkerError;
use async_trait::async_trait;
use chrono::Utc;
use orchestrator_core::{
    event::ExecutionEventKind, Execution, ExecutionEvent, ExecutionId, ExecutionResult,
    ExecutionState, FailureClass, WorkerId,
};
use orchestrator_persistence::{ExecutionStore, ReservationStore};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDisposition {
    Resumed,
    WorkerLost,
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use orchestrator_core::{
        AgentAssignment, AttemptId, ExecutionManifest, HarnessKind, ModelPolicy, OwnershipLabels,
        PersistenceMode, RepositoryReference, Role, RuntimeRequirement, SessionId,
    };
    use orchestrator_persistence::{Reservation, StoreError};
    use std::{collections::BTreeMap, sync::Mutex};

    struct Authority {
        valid: BTreeMap<ExecutionId, bool>,
        fail: bool,
        cleaned: Mutex<Vec<ExecutionId>>,
    }

    #[async_trait]
    impl RecoveryAuthority for Authority {
        async fn resume_if_valid(&self, execution: &Execution) -> Result<bool, WorkerError> {
            if self.fail {
                Err(WorkerError::Invalid("authority uncertain".into()))
            } else {
                Ok(self.valid.get(&execution.id).copied().unwrap_or(false))
            }
        }

        async fn cleanup_invalid(&self, execution: &Execution) -> Result<(), WorkerError> {
            self.cleaned.lock().unwrap().push(execution.id.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct Store {
        executions: Mutex<Vec<Execution>>,
    }

    #[async_trait]
    impl ExecutionStore for Store {
        async fn insert(&self, execution: &Execution) -> Result<(), StoreError> {
            self.executions.lock().unwrap().push(execution.clone());
            Ok(())
        }
        async fn get(&self, id: &ExecutionId) -> Result<Execution, StoreError> {
            self.executions
                .lock()
                .unwrap()
                .iter()
                .find(|execution| &execution.id == id)
                .cloned()
                .ok_or_else(|| StoreError::NotFound(id.to_string()))
        }
        async fn list_live(&self) -> Result<Vec<Execution>, StoreError> {
            Ok(self.executions.lock().unwrap().clone())
        }
        async fn transition(
            &self,
            _: &ExecutionId,
            _: ExecutionState,
        ) -> Result<Execution, StoreError> {
            unreachable!()
        }
        async fn record_progress(
            &self,
            execution: &Execution,
            _: &ExecutionEvent,
        ) -> Result<u64, StoreError> {
            let mut executions = self.executions.lock().unwrap();
            *executions
                .iter_mut()
                .find(|stored| stored.id == execution.id)
                .unwrap() = execution.clone();
            Ok(1)
        }
    }

    #[derive(Default)]
    struct Reservations(Mutex<Vec<ExecutionId>>);

    #[async_trait]
    impl ReservationStore for Reservations {
        async fn reserve_next(&self, _: &WorkerId) -> Result<Option<Reservation>, StoreError> {
            Ok(None)
        }
        async fn release(&self, id: &ExecutionId) -> Result<(), StoreError> {
            self.0.lock().unwrap().push(id.clone());
            Ok(())
        }
        async fn list_for_worker(&self, _: &WorkerId) -> Result<Vec<Reservation>, StoreError> {
            Ok(Vec::new())
        }
        async fn reconcile(&self, _: &[ExecutionId]) -> Result<Vec<ExecutionId>, StoreError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn resumes_only_exact_authority_and_marks_invalid_attempt_worker_lost() {
        let worker = WorkerId::new("worker-recovery");
        let resumable = execution("resumable", &worker);
        let invalid = execution("invalid", &worker);
        let store = Arc::new(Store {
            executions: Mutex::new(vec![resumable.clone(), invalid.clone()]),
        });
        let reservations = Arc::new(Reservations::default());
        let authority = Arc::new(Authority {
            valid: BTreeMap::from([(resumable.id.clone(), true)]),
            fail: false,
            cleaned: Mutex::new(Vec::new()),
        });
        let coordinator =
            RecoveryCoordinator::new(authority.clone(), store.clone(), reservations.clone());

        let recovered = coordinator.recover_worker(&worker).await.unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(
            store.get(&resumable.id).await.unwrap().state,
            ExecutionState::Running
        );
        let failed = store.get(&invalid.id).await.unwrap();
        assert_eq!(failed.state, ExecutionState::Failed);
        assert_eq!(
            failed.result.unwrap().failure,
            Some(FailureClass::WorkerLost)
        );
        assert_eq!(*authority.cleaned.lock().unwrap(), vec![invalid.id.clone()]);
        assert_eq!(*reservations.0.lock().unwrap(), vec![invalid.id]);
    }

    #[tokio::test]
    async fn uncertain_authority_fails_closed_without_state_or_reservation_changes() {
        let worker = WorkerId::new("worker-uncertain");
        let execution = execution("uncertain", &worker);
        let store = Arc::new(Store {
            executions: Mutex::new(vec![execution.clone()]),
        });
        let reservations = Arc::new(Reservations::default());
        let authority = Arc::new(Authority {
            valid: BTreeMap::new(),
            fail: true,
            cleaned: Mutex::new(Vec::new()),
        });
        let coordinator = RecoveryCoordinator::new(authority, store.clone(), reservations.clone());
        assert!(coordinator.recover_worker(&worker).await.is_err());
        assert_eq!(
            store.get(&execution.id).await.unwrap().state,
            ExecutionState::Running
        );
        assert!(reservations.0.lock().unwrap().is_empty());
    }

    fn execution(id: &str, worker: &WorkerId) -> Execution {
        let id = ExecutionId::new(id);
        Execution {
            id: id.clone(),
            role: Role::Implementation,
            state: ExecutionState::Running,
            manifest: ExecutionManifest {
                api_version: orchestrator_core::MANIFEST_API_VERSION.into(),
                role: Role::Implementation,
                task: None,
                repository: RepositoryReference {
                    repo: "owner/repo".into(),
                    base_ref: "main".into(),
                    base_sha: Some("abc".into()),
                    branch: Some("task".into()),
                },
                agent: AgentAssignment {
                    harness: HarnessKind::Pi,
                    model_policy: ModelPolicy {
                        provider: "inferweave".into(),
                        preferred: Vec::new(),
                        alternatives: Vec::new(),
                        fallback_class: None,
                    },
                },
                runtime: RuntimeRequirement::default(),
                services: Vec::new(),
                persistence: PersistenceMode::Resumable,
                task_packet: None,
            },
            worker_id: Some(worker.clone()),
            attempt_id: Some(AttemptId::new(format!("attempt-{id}"))),
            session_id: Some(SessionId::new(id.to_string())),
            worktree_path: Some("/worktree".into()),
            labels: OwnershipLabels {
                execution_id: id,
                worker_id: worker.clone(),
                repository: "owner/repo".into(),
                issue: None,
            },
            created_at: Utc::now(),
            updated_at: Utc::now(),
            result: None,
        }
    }
}

/// Validates durable storage, Git, runtime labels, Pi holds, and session
/// authority before resuming an existing attempt. `Ok(false)` means the exact
/// authority is absent; errors mean authority is uncertain and fail closed.
#[async_trait]
pub trait RecoveryAuthority: Send + Sync {
    async fn resume_if_valid(&self, execution: &Execution) -> Result<bool, WorkerError>;
    async fn cleanup_invalid(&self, execution: &Execution) -> Result<(), WorkerError>;
}

pub struct RecoveryCoordinator {
    authority: Arc<dyn RecoveryAuthority>,
    executions: Arc<dyn ExecutionStore>,
    reservations: Arc<dyn ReservationStore>,
}

impl RecoveryCoordinator {
    pub fn new(
        authority: Arc<dyn RecoveryAuthority>,
        executions: Arc<dyn ExecutionStore>,
        reservations: Arc<dyn ReservationStore>,
    ) -> Self {
        Self {
            authority,
            executions,
            reservations,
        }
    }

    pub async fn recover_worker(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<(ExecutionId, RecoveryDisposition)>, WorkerError> {
        let live = self
            .executions
            .list_live()
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        let mut recovered = Vec::new();
        for execution in live
            .into_iter()
            .filter(|execution| execution.worker_id.as_ref() == Some(worker_id))
        {
            if self.authority.resume_if_valid(&execution).await? {
                recovered.push((execution.id, RecoveryDisposition::Resumed));
                continue;
            }
            self.authority.cleanup_invalid(&execution).await?;
            self.record_worker_lost(execution.clone()).await?;
            self.reservations
                .release(&execution.id)
                .await
                .map_err(|error| WorkerError::Persistence(error.to_string()))?;
            recovered.push((execution.id, RecoveryDisposition::WorkerLost));
        }
        Ok(recovered)
    }

    async fn record_worker_lost(&self, mut execution: Execution) -> Result<(), WorkerError> {
        execution
            .transition(ExecutionState::Failed)
            .map_err(|error| WorkerError::Invalid(error.to_string()))?;
        execution.result = Some(ExecutionResult {
            execution_id: execution.id.clone(),
            state: ExecutionState::Failed,
            failure: Some(FailureClass::WorkerLost),
            branch: None,
            base_sha: None,
            diff_artifact: None,
            artifacts: Vec::new(),
            tests: None,
        });
        let event = ExecutionEvent {
            execution_id: execution.id.clone(),
            attempt_id: execution.attempt_id.clone(),
            sequence: 0,
            at: Utc::now(),
            state: execution.state,
            kind: ExecutionEventKind::ExecutionFailed {
                failure: FailureClass::WorkerLost,
            },
        };
        self.executions
            .record_progress(&execution, &event)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        Ok(())
    }
}
