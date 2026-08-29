use async_trait::async_trait;
use chrono::Utc;
use execution_storage::{
    AllocationReceipt, BackendIdentity, DockerBindProof, ALLOCATION_API_VERSION,
};
use git_worktree::{DiffCapture, Worktree};
use harness_traits::SessionRef;
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, AttemptId, Execution, ExecutionControlAction,
    ExecutionControlRequest, ExecutionEvent, ExecutionId, ExecutionManifest, ExecutionState,
    HarnessKind, ModelPolicy, OwnershipLabels, PersistenceMode, RepositoryReference, Role,
    RuntimeRequirement, SessionId, TaskPacket, WorkerId,
};
use orchestrator_persistence::{
    CleanupAuthority, CleanupAuthorityStore, CleanupDisposition, ExecutionControlPhase,
    ExecutionStore, PendingCancellation, PendingExecutionControl, Reservation, ReservationStore,
    StoreError,
};
use orchestrator_worker::{AdoptedExecution, ExecutionLifecycle, LifecycleError, Worker};
use runtime_traits::{EnvironmentHandle, VerifiedAgentContainer};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
struct FakeStore {
    execution: Mutex<Option<Execution>>,
    events: Mutex<Vec<ExecutionEvent>>,
    pending_cancellations: Mutex<Vec<PendingCancellation>>,
    pending_controls: Mutex<Vec<PendingExecutionControl>>,
    fail_record: bool,
}

struct StartupExecutionStore {
    executions: Mutex<BTreeMap<ExecutionId, Execution>>,
    cancellation_failure: ExecutionId,
}

#[async_trait]
impl ExecutionStore for StartupExecutionStore {
    async fn insert(&self, _: &Execution) -> Result<(), StoreError> {
        unreachable!("startup regression is read-only")
    }

    async fn get(&self, id: &ExecutionId) -> Result<Execution, StoreError> {
        self.executions
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    async fn list_live(&self) -> Result<Vec<Execution>, StoreError> {
        Ok(self.executions.lock().unwrap().values().cloned().collect())
    }

    async fn cancellation_requested(&self, id: &ExecutionId) -> Result<bool, StoreError> {
        if id == &self.cancellation_failure {
            Err(StoreError::Conflict(
                "injected cancellation lookup failure".into(),
            ))
        } else {
            Ok(false)
        }
    }

    async fn transition(
        &self,
        id: &ExecutionId,
        next: ExecutionState,
    ) -> Result<Execution, StoreError> {
        let mut executions = self.executions.lock().unwrap();
        let execution = executions
            .get_mut(id)
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        execution
            .transition(next)
            .map_err(|_| StoreError::IllegalTransition {
                from: execution.state,
                to: next,
            })?;
        Ok(execution.clone())
    }

    async fn record_progress(
        &self,
        execution: &Execution,
        _: &ExecutionEvent,
    ) -> Result<u64, StoreError> {
        self.executions
            .lock()
            .unwrap()
            .insert(execution.id.clone(), execution.clone());
        Ok(1)
    }
}

struct StartupCleanupAuthorities {
    authorities: Vec<CleanupAuthority>,
    resolved: Mutex<Vec<ExecutionId>>,
}

#[async_trait]
impl CleanupAuthorityStore for StartupCleanupAuthorities {
    async fn begin(&self, _: &ExecutionId, _: &AttemptId, _: &WorkerId) -> Result<(), StoreError> {
        Ok(())
    }

    async fn advance(&self, _: &ExecutionId, _: &str) -> Result<(), StoreError> {
        Ok(())
    }

    async fn resolve(&self, execution_id: &ExecutionId) -> Result<(), StoreError> {
        self.resolved.lock().unwrap().push(execution_id.clone());
        Ok(())
    }

    async fn get(&self, execution_id: &ExecutionId) -> Result<CleanupAuthority, StoreError> {
        self.authorities
            .iter()
            .find(|authority| &authority.execution_id == execution_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(execution_id.to_string()))
    }

    async fn list_for_worker(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<CleanupAuthority>, StoreError> {
        Ok(self
            .authorities
            .iter()
            .filter(|authority| &authority.worker_id == worker_id)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl ExecutionStore for FakeStore {
    async fn insert(&self, execution: &Execution) -> Result<(), StoreError> {
        *self.execution.lock().unwrap() = Some(execution.clone());
        Ok(())
    }
    async fn get(&self, _: &ExecutionId) -> Result<Execution, StoreError> {
        self.execution
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| StoreError::NotFound("execution".into()))
    }
    async fn list_live(&self) -> Result<Vec<Execution>, StoreError> {
        Ok(self.execution.lock().unwrap().clone().into_iter().collect())
    }
    async fn cancellation_requested(&self, id: &ExecutionId) -> Result<bool, StoreError> {
        Ok(self
            .pending_cancellations
            .lock()
            .unwrap()
            .iter()
            .any(|pending| pending.execution.id == *id))
    }
    async fn list_pending_cancellations(&self) -> Result<Vec<PendingCancellation>, StoreError> {
        Ok(self.pending_cancellations.lock().unwrap().clone())
    }
    async fn list_pending_controls(
        &self,
        _: &WorkerId,
    ) -> Result<Vec<PendingExecutionControl>, StoreError> {
        Ok(self.pending_controls.lock().unwrap().clone())
    }
    async fn begin_control(&self, _: i64, _: &WorkerId, _: &AttemptId) -> Result<(), StoreError> {
        Ok(())
    }
    async fn mark_control_side_effect_applied(
        &self,
        _: i64,
        _: Option<&SessionId>,
    ) -> Result<(), StoreError> {
        Ok(())
    }
    async fn complete_control(
        &self,
        request_id: i64,
        execution: &Execution,
        event: &ExecutionEvent,
    ) -> Result<Option<u64>, StoreError> {
        *self.execution.lock().unwrap() = Some(execution.clone());
        self.pending_controls
            .lock()
            .unwrap()
            .retain(|control| control.request.request_id != request_id);
        let mut events = self.events.lock().unwrap();
        let mut event = event.clone();
        event.sequence = events.len() as u64 + 1;
        events.push(event);
        Ok(Some(events.len() as u64))
    }
    async fn transition(
        &self,
        _: &ExecutionId,
        next: ExecutionState,
    ) -> Result<Execution, StoreError> {
        let mut execution = self.execution.lock().unwrap();
        let execution = execution.as_mut().unwrap();
        execution.transition(next).unwrap();
        Ok(execution.clone())
    }
    async fn record_progress(
        &self,
        execution: &Execution,
        event: &ExecutionEvent,
    ) -> Result<u64, StoreError> {
        if self.fail_record {
            return Err(StoreError::Conflict("injected progress failure".into()));
        }
        *self.execution.lock().unwrap() = Some(execution.clone());
        let mut events = self.events.lock().unwrap();
        let mut event = event.clone();
        event.sequence = events.len() as u64 + 1;
        events.push(event);
        Ok(events.len() as u64)
    }
}

#[derive(Default)]
struct FakeReservations {
    released: Mutex<Vec<ExecutionId>>,
    retained: Mutex<Vec<ExecutionId>>,
    fail_retained: AtomicBool,
}

#[derive(Default)]
struct FakeCleanupAuthorities {
    pending: Mutex<Vec<ExecutionId>>,
    checkpoints: Mutex<Vec<(String, serde_json::Value)>>,
    begin_error: bool,
}

#[async_trait]
impl CleanupAuthorityStore for FakeCleanupAuthorities {
    async fn begin(
        &self,
        execution_id: &ExecutionId,
        _: &AttemptId,
        _: &WorkerId,
    ) -> Result<(), StoreError> {
        if self.begin_error {
            return Err(StoreError::Conflict(
                "injected cleanup authority conflict".into(),
            ));
        }
        self.pending.lock().unwrap().push(execution_id.clone());
        Ok(())
    }

    async fn advance(&self, _: &ExecutionId, _: &str) -> Result<(), StoreError> {
        Ok(())
    }

    async fn checkpoint(
        &self,
        _: &ExecutionId,
        phase: &str,
        handles: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.checkpoints
            .lock()
            .unwrap()
            .push((phase.into(), handles.clone()));
        Ok(())
    }

    async fn transition(
        &self,
        _: &ExecutionId,
        _: CleanupDisposition,
        next: CleanupDisposition,
        handles: &serde_json::Value,
    ) -> Result<(), StoreError> {
        self.checkpoints
            .lock()
            .unwrap()
            .push((next.to_string(), handles.clone()));
        Ok(())
    }

    async fn resolve(&self, execution_id: &ExecutionId) -> Result<(), StoreError> {
        self.pending
            .lock()
            .unwrap()
            .retain(|pending| pending != execution_id);
        Ok(())
    }

    async fn get(&self, execution_id: &ExecutionId) -> Result<CleanupAuthority, StoreError> {
        if !self.pending.lock().unwrap().contains(execution_id) {
            return Err(StoreError::NotFound(execution_id.to_string()));
        }
        Ok(CleanupAuthority {
            execution_id: execution_id.clone(),
            attempt_id: AttemptId::new("attempt-1"),
            worker_id: WorkerId::new("worker-1"),
            phase: "RESERVED".into(),
            handles: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    async fn list_for_worker(&self, _: &WorkerId) -> Result<Vec<CleanupAuthority>, StoreError> {
        Ok(Vec::new())
    }
}

fn worker(
    lifecycle: Arc<FakeLifecycle>,
    store: Arc<FakeStore>,
    reservations: Arc<FakeReservations>,
) -> Worker {
    worker_with_cleanup(
        lifecycle,
        store,
        reservations,
        Arc::new(FakeCleanupAuthorities::default()),
    )
}

fn worker_with_cleanup(
    lifecycle: Arc<FakeLifecycle>,
    store: Arc<FakeStore>,
    reservations: Arc<FakeReservations>,
    cleanup: Arc<FakeCleanupAuthorities>,
) -> Worker {
    Worker::new(lifecycle, store, reservations, cleanup)
}

#[async_trait]
impl ReservationStore for FakeReservations {
    async fn reserve_next(&self, _: &WorkerId) -> Result<Option<Reservation>, StoreError> {
        Ok(None)
    }
    async fn release(&self, id: &ExecutionId) -> Result<(), StoreError> {
        self.released.lock().unwrap().push(id.clone());
        Ok(())
    }
    async fn commit_retained_and_release_capacity(
        &self,
        id: &ExecutionId,
        _: &AttemptId,
    ) -> Result<(), StoreError> {
        if self.fail_retained.load(Ordering::SeqCst) {
            return Err(StoreError::Conflict(
                "injected retained reservation failure".into(),
            ));
        }
        self.released.lock().unwrap().push(id.clone());
        self.retained.lock().unwrap().push(id.clone());
        Ok(())
    }
    async fn finalize_cleanup(
        &self,
        id: &ExecutionId,
        _: &AttemptId,
    ) -> Result<orchestrator_persistence::LostWorkerRecovery, StoreError> {
        self.released.lock().unwrap().push(id.clone());
        Ok(orchestrator_persistence::LostWorkerRecovery::Failed(
            id.clone(),
        ))
    }
    async fn list_for_worker(&self, _: &WorkerId) -> Result<Vec<Reservation>, StoreError> {
        Ok(Vec::new())
    }
    async fn reconcile(&self, _: &[ExecutionId]) -> Result<Vec<ExecutionId>, StoreError> {
        Ok(Vec::new())
    }
}

struct FakeLifecycle {
    order: Arc<Mutex<Vec<&'static str>>>,
    fail_at: Option<&'static str>,
    poll_empty: bool,
    cleanup_fail: bool,
    hang_poll: bool,
}

impl FakeLifecycle {
    fn step(&self, name: &'static str) -> Result<(), LifecycleError> {
        self.order.lock().unwrap().push(name);
        if self.fail_at == Some(name) {
            Err(LifecycleError::Step(name.to_owned()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl ExecutionLifecycle for FakeLifecycle {
    async fn allocate(&self, execution: &Execution) -> Result<AllocationReceipt, LifecycleError> {
        self.step("allocate")?;
        Ok(receipt(execution))
    }
    async fn create_worktree(
        &self,
        execution: &Execution,
        _: &AllocationReceipt,
    ) -> Result<Worktree, LifecycleError> {
        self.step("git-create")?;
        Ok(Worktree {
            execution_id: execution.id.clone(),
            path: "/allocation/repository".into(),
            branch: "task".into(),
            base_sha: "abc".into(),
            repository: "owner/repo".into(),
        })
    }
    async fn provision(
        &self,
        execution: &Execution,
        _: &AllocationReceipt,
        _: &Worktree,
    ) -> Result<EnvironmentHandle, LifecycleError> {
        self.step("docker-provision")?;
        Ok(EnvironmentHandle {
            execution_id: execution.id.clone(),
            network: "network".into(),
            agent_container: "container".into(),
            verified_agent_container: VerifiedAgentContainer {
                container_id: "container-id".into(),
                daemon_id: "daemon".into(),
                labels: execution.labels.clone(),
                mounts: Vec::new(),
            },
            service_containers: Vec::new(),
            volumes: Vec::new(),
            credentials_path: None,
        })
    }
    async fn start(
        &self,
        execution: &Execution,
        _: &AllocationReceipt,
        _: &EnvironmentHandle,
        worktree: &Worktree,
        _: &TaskPacket,
    ) -> Result<SessionRef, LifecycleError> {
        self.step("pi-start")?;
        Ok(SessionRef {
            id: SessionId::new(execution.id.to_string()),
            path: "/allocation/session".into(),
            execution_id: execution.id.clone(),
            worktree_path: worktree.path.clone(),
        })
    }
    async fn resume(
        &self,
        _: &Execution,
        _: &AllocationReceipt,
        _: &EnvironmentHandle,
        _: &SessionRef,
    ) -> Result<(), LifecycleError> {
        self.step("pi-resume")
    }
    async fn pause(&self, _: &Execution, _: &SessionRef) -> Result<(), LifecycleError> {
        self.step("pi-pause")
    }
    async fn fork_conversation(
        &self,
        execution: &Execution,
        session: &SessionRef,
        target: &SessionId,
    ) -> Result<SessionRef, LifecycleError> {
        self.step("pi-fork")?;
        Ok(SessionRef {
            id: target.clone(),
            path: format!("{}/fork", session.path),
            execution_id: execution.id.clone(),
            worktree_path: session.worktree_path.clone(),
        })
    }
    async fn poll(
        &self,
        execution: &Execution,
        _: &SessionRef,
    ) -> Result<Vec<ExecutionEvent>, LifecycleError> {
        self.step("pi-poll")?;
        if self.hang_poll {
            std::future::pending::<()>().await;
        }
        if self.poll_empty {
            tokio::task::yield_now().await;
            return Ok(Vec::new());
        }
        Ok(vec![ExecutionEvent {
            execution_id: execution.id.clone(),
            attempt_id: execution.attempt_id.clone(),
            sequence: 0,
            at: Utc::now(),
            state: ExecutionState::ReviewReady,
            kind: ExecutionEventKind::ReviewReady,
        }])
    }
    async fn stop(&self, _: &Execution, _: &SessionRef) -> Result<(), LifecycleError> {
        self.step("pi-stop")
    }
    async fn cpu_percent(&self, _: &Execution) -> Result<f64, LifecycleError> {
        Ok(0.0)
    }
    async fn capture(&self, _: &Worktree) -> Result<DiffCapture, LifecycleError> {
        self.step("git-capture")?;
        Ok(DiffCapture {
            patch: "diff".into(),
            changed_files: vec!["src/lib.rs".into()],
        })
    }
    async fn persist_evidence(
        &self,
        _: &Execution,
        _: &DiffCapture,
    ) -> Result<String, LifecycleError> {
        self.step("persist-evidence")?;
        Ok("sha256:diff".into())
    }
    async fn destroy_runtime(&self, _: &Execution) -> Result<(), LifecycleError> {
        self.step("docker-destroy")?;
        if self.cleanup_fail {
            Err(LifecycleError::Step("docker cleanup".into()))
        } else {
            Ok(())
        }
    }
    async fn destroy_worktree(&self, _: &Worktree) -> Result<(), LifecycleError> {
        self.step("git-destroy")?;
        if self.cleanup_fail {
            Err(LifecycleError::Step("git cleanup".into()))
        } else {
            Ok(())
        }
    }
    async fn release_storage(&self, _: &AllocationReceipt) -> Result<(), LifecycleError> {
        self.step("storage-release")?;
        if self.cleanup_fail {
            Err(LifecycleError::Step("storage cleanup".into()))
        } else {
            Ok(())
        }
    }
    async fn ack_release_storage(&self, _: &AllocationReceipt) -> Result<(), LifecycleError> {
        if self.fail_at == Some("storage-ack") {
            return self.step("storage-ack");
        }
        Ok(())
    }
    async fn ack_cleanup_authority_step(
        &self,
        _: &CleanupAuthority,
        disposition: CleanupDisposition,
    ) -> Result<(), LifecycleError> {
        if disposition == CleanupDisposition::ReservationReleased
            && self.fail_at == Some("storage-ack")
        {
            return self.step("storage-ack");
        }
        Ok(())
    }
    async fn adopt(&self, execution: &Execution) -> Result<AdoptedExecution, LifecycleError> {
        self.step("adopt")?;
        Ok(AdoptedExecution {
            receipt: receipt(execution),
            worktree: Worktree {
                execution_id: execution.id.clone(),
                path: "/allocation/repository".into(),
                branch: "task".into(),
                base_sha: "abc".into(),
                repository: "owner/repo".into(),
            },
            environment: EnvironmentHandle {
                execution_id: execution.id.clone(),
                network: "network".into(),
                agent_container: "container".into(),
                verified_agent_container: VerifiedAgentContainer {
                    container_id: "container-id".into(),
                    daemon_id: "daemon".into(),
                    labels: execution.labels.clone(),
                    mounts: Vec::new(),
                },
                service_containers: Vec::new(),
                volumes: Vec::new(),
                credentials_path: None,
            },
            session: SessionRef {
                id: execution
                    .session_id
                    .clone()
                    .ok_or_else(|| LifecycleError::Step("missing session".into()))?,
                path: "/allocation/session".into(),
                execution_id: execution.id.clone(),
                worktree_path: "/allocation/repository".into(),
            },
        })
    }

    async fn cleanup_authority(
        &self,
        _: &CleanupAuthority,
        _: &Execution,
    ) -> Result<(), LifecycleError> {
        self.step("authority-cleanup")
    }
}

#[tokio::test]
async fn restart_adoption_reuses_attempt_session_and_skips_all_creation_steps() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let mut execution = execution();
    execution.manifest.persistence = PersistenceMode::Resumable;
    execution.state = ExecutionState::Running;
    execution.session_id = Some(SessionId::new("durable-session"));
    execution.worktree_path = Some("/allocation/repository".into());
    store.insert(&execution).await.unwrap();
    let worker = Arc::new(worker(lifecycle, store, reservations.clone()));

    worker.spawn_adopted(execution).join().await.unwrap();

    assert_eq!(
        *order.lock().unwrap(),
        vec![
            "adopt",
            "pi-resume",
            "pi-poll",
            "pi-stop",
            "git-capture",
            "persist-evidence",
        ]
    );
    assert_eq!(
        reservations.released.lock().unwrap().as_slice(),
        &[ExecutionId::new("worker-run-test")]
    );
}

#[tokio::test]
async fn failed_adoption_recovers_durable_authority_instead_of_releasing_live_layers() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: Some("adopt"),
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let cleanup = Arc::new(FakeCleanupAuthorities::default());
    let mut execution = execution();
    execution.manifest.persistence = PersistenceMode::Resumable;
    execution.state = ExecutionState::Running;
    execution.session_id = Some(SessionId::new("durable-session"));
    execution.worktree_path = Some("/allocation/repository".into());
    store.insert(&execution).await.unwrap();
    let worker = Arc::new(worker_with_cleanup(
        lifecycle,
        store,
        reservations.clone(),
        cleanup.clone(),
    ));

    assert!(worker
        .spawn_adopted(execution.clone())
        .join()
        .await
        .is_err());

    assert_eq!(
        order.lock().unwrap().as_slice(),
        &[
            "adopt",
            "authority-cleanup",
            "authority-cleanup",
            "authority-cleanup",
            "authority-cleanup",
        ]
    );
    assert_eq!(
        reservations.released.lock().unwrap().as_slice(),
        std::slice::from_ref(&execution.id)
    );
    assert!(cleanup.pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn successful_run_uses_exact_order_and_persists_result_before_cleanup() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let cleanup = Arc::new(FakeCleanupAuthorities::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = worker_with_cleanup(
        lifecycle,
        store.clone(),
        reservations.clone(),
        cleanup.clone(),
    );

    let result = worker.run(&execution).await.unwrap();

    assert_eq!(result.diff_artifact.as_deref(), Some("sha256:diff"));
    assert_eq!(
        *order.lock().unwrap(),
        vec![
            "allocate",
            "git-create",
            "docker-provision",
            "pi-start",
            "pi-poll",
            "pi-stop",
            "git-capture",
            "persist-evidence",
            "docker-destroy",
            "git-destroy",
            "storage-release"
        ]
    );
    assert!(store
        .events
        .lock()
        .unwrap()
        .iter()
        .all(|event| !matches!(event.kind, ExecutionEventKind::AgentStarted { .. })));
    assert_eq!(
        reservations.released.lock().unwrap().as_slice(),
        std::slice::from_ref(&execution.id)
    );
    let checkpoints = cleanup.checkpoints.lock().unwrap();
    assert_eq!(
        checkpoints
            .iter()
            .map(|(phase, _)| phase.as_str())
            .collect::<Vec<_>>(),
        vec![
            "ACTIVE:STORAGE",
            "ACTIVE:WORKTREE",
            "ACTIVE:RUNTIME",
            "ACTIVE:PI_STARTED",
            "ACTIVE:RUNNING",
            "ACTIVE:POST_PI_BEFORE_EVENT",
            "CLEANUP_PENDING",
            "CLEANUP_PENDING",
            "RUNTIME_STOPPED",
            "RUNTIME_DESTROYED",
            "GIT_RECOVERED_CLEANED",
            "STORAGE_RELEASED",
        ]
    );
    assert!(checkpoints[0].1["receipt"].is_object());
    assert!(checkpoints[1].1["worktree"].is_object());
    assert!(checkpoints[2].1["runtime"].is_null());
    assert!(checkpoints[2].1["runtime_selector"].is_object());
    assert!(checkpoints[3].1["session"].is_object());
}

#[tokio::test]
async fn storage_ack_failure_keeps_finalized_authority_for_fresh_worker_retry() {
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let cleanup = Arc::new(FakeCleanupAuthorities::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let failing = worker_with_cleanup(
        Arc::new(FakeLifecycle {
            order: Arc::new(Mutex::new(Vec::new())),
            fail_at: Some("storage-ack"),
            poll_empty: false,
            cleanup_fail: false,
            hang_poll: false,
        }),
        store.clone(),
        reservations.clone(),
        cleanup.clone(),
    );

    assert!(failing.run(&execution).await.is_err());
    assert_eq!(
        cleanup.pending.lock().unwrap().as_slice(),
        std::slice::from_ref(&execution.id),
        "DB finalization must remain discoverable until external ACK succeeds"
    );

    let authority = CleanupAuthority {
        execution_id: execution.id.clone(),
        attempt_id: execution.attempt_id.clone().unwrap(),
        worker_id: execution.worker_id.clone().unwrap(),
        phase: "RESERVATION_RELEASED".into(),
        handles: serde_json::json!({}),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let replacement = worker_with_cleanup(
        Arc::new(FakeLifecycle {
            order: Arc::new(Mutex::new(Vec::new())),
            fail_at: None,
            poll_empty: false,
            cleanup_fail: false,
            hang_poll: false,
        }),
        store,
        reservations,
        cleanup.clone(),
    );
    replacement
        .recover_cleanup_authority(&authority, &execution)
        .await
        .unwrap();
    assert!(cleanup.pending.lock().unwrap().is_empty());
}

#[tokio::test]
async fn resumable_review_ready_retains_resources_but_releases_capacity() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let cleanup = Arc::new(FakeCleanupAuthorities::default());
    let mut execution = execution();
    execution.manifest.persistence = PersistenceMode::Resumable;
    store.insert(&execution).await.unwrap();
    let worker = worker_with_cleanup(lifecycle, store, reservations.clone(), cleanup.clone());

    worker.run(&execution).await.unwrap();

    assert_eq!(
        reservations.released.lock().unwrap().as_slice(),
        std::slice::from_ref(&execution.id)
    );
    assert_eq!(
        reservations.retained.lock().unwrap().as_slice(),
        std::slice::from_ref(&execution.id)
    );
    assert_eq!(
        *order.lock().unwrap(),
        vec![
            "allocate",
            "git-create",
            "docker-provision",
            "pi-start",
            "pi-poll",
            "pi-stop",
            "git-capture",
            "persist-evidence",
        ]
    );
}

#[tokio::test]
async fn cleanup_authority_conflict_immediately_releases_the_new_reservation() {
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::new(Mutex::new(Vec::new())),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let cleanup = Arc::new(FakeCleanupAuthorities {
        begin_error: true,
        ..Default::default()
    });
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = worker_with_cleanup(lifecycle, store, reservations.clone(), cleanup);

    assert!(worker.run(&execution).await.is_err());

    assert_eq!(
        reservations.released.lock().unwrap().as_slice(),
        &[execution.id]
    );
}

#[tokio::test]
async fn provisioning_failure_runs_reverse_cleanup_and_returns_worker_lost_peer_safe_error() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: Some("docker-provision"),
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = worker(lifecycle, store.clone(), reservations);

    assert!(worker.run(&execution).await.is_err());
    let failed = store.get(&execution.id).await.unwrap();
    assert_eq!(failed.state, ExecutionState::Failed);
    assert_eq!(
        failed.result.unwrap().failure,
        Some(orchestrator_core::FailureClass::EnvironmentFailed)
    );
    assert_eq!(
        *order.lock().unwrap(),
        vec![
            "allocate",
            "git-create",
            "docker-provision",
            "docker-destroy",
            "git-destroy",
            "storage-release"
        ]
    );
}

#[tokio::test]
async fn every_creation_and_result_phase_failure_cleans_only_acquired_lower_layers() {
    let cases: &[(&str, &[&str])] = &[
        ("allocate", &["allocate"]),
        ("git-create", &["allocate", "git-create", "storage-release"]),
        (
            "docker-provision",
            &[
                "allocate",
                "git-create",
                "docker-provision",
                "docker-destroy",
                "git-destroy",
                "storage-release",
            ],
        ),
        (
            "pi-start",
            &[
                "allocate",
                "git-create",
                "docker-provision",
                "pi-start",
                "docker-destroy",
                "git-destroy",
                "storage-release",
            ],
        ),
        (
            "pi-poll",
            &[
                "allocate",
                "git-create",
                "docker-provision",
                "pi-start",
                "pi-poll",
                "pi-stop",
                "docker-destroy",
                "git-destroy",
                "storage-release",
            ],
        ),
        (
            "git-capture",
            &[
                "allocate",
                "git-create",
                "docker-provision",
                "pi-start",
                "pi-poll",
                "pi-stop",
                "git-capture",
                "docker-destroy",
                "git-destroy",
                "storage-release",
            ],
        ),
        (
            "persist-evidence",
            &[
                "allocate",
                "git-create",
                "docker-provision",
                "pi-start",
                "pi-poll",
                "pi-stop",
                "git-capture",
                "persist-evidence",
                "docker-destroy",
                "git-destroy",
                "storage-release",
            ],
        ),
    ];
    for (failure, expected_order) in cases {
        let order = Arc::new(Mutex::new(Vec::new()));
        let lifecycle = Arc::new(FakeLifecycle {
            order: Arc::clone(&order),
            fail_at: Some(failure),
            poll_empty: false,
            cleanup_fail: false,
            hang_poll: false,
        });
        let store = Arc::new(FakeStore::default());
        let reservations = Arc::new(FakeReservations::default());
        let cleanup = Arc::new(FakeCleanupAuthorities::default());
        let execution = execution();
        store.insert(&execution).await.unwrap();
        let worker = worker_with_cleanup(
            lifecycle,
            store.clone(),
            reservations.clone(),
            cleanup.clone(),
        );

        assert!(worker.run(&execution).await.is_err(), "{failure}");
        assert_eq!(
            order.lock().unwrap().as_slice(),
            *expected_order,
            "{failure}"
        );
        assert_eq!(
            reservations.released.lock().unwrap().as_slice(),
            std::slice::from_ref(&execution.id),
            "{failure}"
        );
        assert!(
            cleanup.pending.lock().unwrap().is_empty(),
            "successful external ACK resolves the authority: {failure}"
        );
        assert_eq!(
            store.get(&execution.id).await.unwrap().state,
            ExecutionState::Failed,
            "{failure}"
        );
    }
}

#[tokio::test]
async fn cancelling_one_execution_stops_only_its_task_and_persists_cancelled() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order,
        fail_at: None,
        poll_empty: true,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = Arc::new(worker(lifecycle, store.clone(), reservations));
    let task = worker.spawn(execution.clone());
    tokio::task::yield_now().await;
    task.cancel();
    assert!(task.join().await.is_err());
    let cancelled = store.get(&execution.id).await.unwrap();
    assert_eq!(cancelled.state, ExecutionState::Cancelled);
    assert_eq!(
        cancelled.result.unwrap().failure,
        Some(orchestrator_core::FailureClass::Cancelled)
    );
}

#[tokio::test]
async fn execution_and_reverse_cleanup_failures_are_all_reported() {
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::new(Mutex::new(Vec::new())),
        fail_at: Some("docker-provision"),
        poll_empty: false,
        cleanup_fail: true,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = worker(lifecycle, store, Arc::new(FakeReservations::default()));
    let error = worker.run(&execution).await.unwrap_err().to_string();
    assert!(error.contains("docker-provision"), "{error}");
    assert!(error.contains("docker cleanup"), "{error}");
    assert!(!error.contains("git cleanup"), "{error}");
    assert!(!error.contains("storage cleanup"), "{error}");
}

#[tokio::test]
async fn uncertain_pi_stop_retains_runtime_storage_and_reservation_authority() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: Some("pi-stop"),
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = worker(lifecycle, store, reservations.clone());

    assert!(worker.run(&execution).await.is_err());
    let steps = order.lock().unwrap().clone();
    assert!(!steps.contains(&"docker-destroy"));
    assert!(!steps.contains(&"git-destroy"));
    assert!(!steps.contains(&"storage-release"));
    assert!(reservations.released.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_interrupts_a_hung_poll_and_still_runs_cleanup() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: true,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let execution = execution();
    store.insert(&execution).await.unwrap();
    let worker = Arc::new(worker(lifecycle, store, reservations));
    let task = worker.spawn(execution);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    task.cancel();
    let joined = tokio::time::timeout(std::time::Duration::from_secs(2), task.join()).await;
    assert!(
        joined.is_ok(),
        "cancelled poll task did not stop within bound"
    );
    assert!(order.lock().unwrap().contains(&"pi-stop"));
}

#[tokio::test]
async fn failing_oldest_taskless_cancellation_does_not_starve_active_peer_signal() {
    let mut active = execution();
    active.id = ExecutionId::new("active-peer-cancellation");
    active.labels.execution_id = active.id.clone();
    let mut malformed = active.clone();
    malformed.id = ExecutionId::new("oldest-malformed-cancellation");
    malformed.labels.execution_id = malformed.id.clone();
    malformed.attempt_id = None;
    let worker_id = active.worker_id.clone().unwrap();
    let store = Arc::new(FakeStore::default());
    store.insert(&active).await.unwrap();
    *store.pending_cancellations.lock().unwrap() = vec![
        PendingCancellation {
            execution: malformed,
            worker_id: Some(worker_id.clone()),
            attempt_id: None,
        },
        PendingCancellation {
            execution: active.clone(),
            worker_id: Some(worker_id.clone()),
            attempt_id: active.attempt_id.clone(),
        },
    ];
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::new(Mutex::new(Vec::new())),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: true,
    });
    let worker = Arc::new(worker(
        lifecycle,
        store.clone(),
        Arc::new(FakeReservations::default()),
    ));
    let task = worker.clone().spawn(active);

    assert!(worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .is_err());
    tokio::time::timeout(std::time::Duration::from_secs(2), task.join())
        .await
        .expect("active peer must be signalled before malformed cleanup recovery")
        .unwrap_err();
    assert_eq!(
        store
            .get(&ExecutionId::new("active-peer-cancellation"))
            .await
            .unwrap()
            .state,
        ExecutionState::Cancelled
    );
}

#[tokio::test]
async fn durable_pause_resume_and_conversation_fork_interrupt_poll_without_workspace_fork() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: true,
    });
    let store = Arc::new(FakeStore::default());
    let reservations = Arc::new(FakeReservations::default());
    let execution = execution();
    let worker_id = execution.worker_id.clone().unwrap();
    store.insert(&execution).await.unwrap();
    let worker = Arc::new(worker(lifecycle, store.clone(), reservations));
    let task = worker.clone().spawn(execution.clone());
    for _ in 0..100 {
        if order.lock().unwrap().contains(&"pi-poll") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let mut durable = execution.clone();
    durable.state = ExecutionState::Running;
    durable.worktree_path = Some("/allocation/repository".into());
    durable.session_id = Some(SessionId::new(execution.id.to_string()));
    *store.execution.lock().unwrap() = Some(durable);

    let pending = |request_id, action| {
        let current = store.execution.lock().unwrap().clone().unwrap();
        let source_session_id = current.session_id.clone().unwrap();
        PendingExecutionControl {
            request: ExecutionControlRequest {
                request_id,
                execution_id: execution.id.clone(),
                action,
                requested_at: Utc::now(),
                completed_at: None,
            },
            execution: current.clone(),
            phase: ExecutionControlPhase::Accepted,
            accepted_worker_id: current.worker_id.clone().unwrap(),
            accepted_attempt_id: current.attempt_id.clone().unwrap(),
            source_session_id: source_session_id.clone(),
            worktree_path: current.worktree_path.clone().unwrap(),
            accepted_version: request_id,
            accepted_state: current.state,
            target_session_id: (action == ExecutionControlAction::ForkConversation)
                .then(|| SessionId::new(format!("{source_session_id}-fork-{request_id}"))),
            side_effect_session_id: None,
        }
    };
    store
        .pending_controls
        .lock()
        .unwrap()
        .push(pending(1, ExecutionControlAction::Pause));
    worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    wait_for_state(&store, ExecutionState::PausedForHuman).await;

    store
        .pending_controls
        .lock()
        .unwrap()
        .push(pending(2, ExecutionControlAction::ForkConversation));
    worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    for _ in 0..100 {
        if order.lock().unwrap().contains(&"pi-fork") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let forked = store.get(&execution.id).await.unwrap();
    assert_eq!(
        forked.worktree_path.as_deref(),
        Some("/allocation/repository")
    );
    assert!(forked
        .session_id
        .as_ref()
        .is_some_and(|session| session.as_str().ends_with("-fork-2")));
    assert_eq!(
        order
            .lock()
            .unwrap()
            .iter()
            .filter(|step| **step == "pi-pause")
            .count(),
        2,
        "a fork created while paused must be quiesced before it is persisted"
    );

    store
        .pending_controls
        .lock()
        .unwrap()
        .push(pending(3, ExecutionControlAction::Resume));
    worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    wait_for_state(&store, ExecutionState::Running).await;
    assert!(order.lock().unwrap().contains(&"pi-resume"));
    task.cancel();
    let _ = task.join().await;
}

#[tokio::test]
async fn running_conversation_fork_quiesces_source_and_resumes_exact_target() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: Arc::clone(&order),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: true,
    });
    let store = Arc::new(FakeStore::default());
    let execution = execution();
    let worker_id = execution.worker_id.clone().unwrap();
    store.insert(&execution).await.unwrap();
    let worker = Arc::new(worker(
        lifecycle,
        store.clone(),
        Arc::new(FakeReservations::default()),
    ));
    let task = worker.clone().spawn(execution.clone());
    for _ in 0..100 {
        if order.lock().unwrap().contains(&"pi-poll") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let mut current = execution.clone();
    current.state = ExecutionState::Running;
    current.worktree_path = Some("/allocation/repository".into());
    current.session_id = Some(SessionId::new(execution.id.to_string()));
    let target = SessionId::new(format!("{}-fork-44", current.session_id.as_ref().unwrap()));
    store
        .pending_controls
        .lock()
        .unwrap()
        .push(PendingExecutionControl {
            request: ExecutionControlRequest {
                request_id: 44,
                execution_id: execution.id.clone(),
                action: ExecutionControlAction::ForkConversation,
                requested_at: Utc::now(),
                completed_at: None,
            },
            execution: current.clone(),
            phase: ExecutionControlPhase::Accepted,
            accepted_worker_id: current.worker_id.clone().unwrap(),
            accepted_attempt_id: current.attempt_id.clone().unwrap(),
            source_session_id: current.session_id.clone().unwrap(),
            worktree_path: current.worktree_path.clone().unwrap(),
            accepted_version: 1,
            accepted_state: ExecutionState::Running,
            target_session_id: Some(target.clone()),
            side_effect_session_id: None,
        });
    worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    for _ in 0..100 {
        if store.get(&execution.id).await.unwrap().session_id.as_ref() == Some(&target) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        store.get(&execution.id).await.unwrap().state,
        ExecutionState::Running
    );
    assert_eq!(
        store.get(&execution.id).await.unwrap().session_id,
        Some(target)
    );
    let control_steps = order
        .lock()
        .unwrap()
        .iter()
        .copied()
        .filter(|step| matches!(*step, "pi-pause" | "pi-fork" | "pi-resume"))
        .collect::<Vec<_>>();
    assert_eq!(
        control_steps,
        vec!["pi-pause", "pi-fork", "pi-pause", "pi-resume"]
    );
    task.cancel();
    let _ = task.join().await;
}

async fn wait_for_state(store: &FakeStore, expected: ExecutionState) {
    for _ in 0..100 {
        if store
            .execution
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|execution| execution.state == expected)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("execution did not reach {expected:?}");
}

fn execution() -> Execution {
    let id = ExecutionId::new("worker-run-test");
    Execution {
        id: id.clone(),
        role: Role::Implementation,
        state: ExecutionState::WorkerAssigned,
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
            persistence: PersistenceMode::Ephemeral,
            task_packet: Some(TaskPacket {
                goal: "change".into(),
                acceptance_criteria: vec!["pass".into()],
                non_goals: Vec::new(),
                relevant_context: Vec::new(),
                required_tests: Vec::new(),
                role_skill: None,
            }),
        },
        worker_id: Some(WorkerId::new("worker")),
        attempt_id: Some(AttemptId::new("attempt")),
        session_id: None,
        worktree_path: None,
        labels: OwnershipLabels {
            execution_id: id,
            worker_id: WorkerId::new("worker"),
            repository: "owner/repo".into(),
            issue: None,
        },
        created_at: Utc::now(),
        updated_at: Utc::now(),
        result: None,
    }
}

#[tokio::test]
async fn startup_keeps_a_failed_cancellation_lookup_and_recovers_later_authorities() {
    let worker_id = WorkerId::new("worker-startup-isolation");
    let failed_id = ExecutionId::new("startup-cancellation-store-failure");
    let later_id = ExecutionId::new("startup-later-authority");
    let mut failed = execution();
    failed.id = failed_id.clone();
    failed.labels.execution_id = failed_id.clone();
    failed.labels.worker_id = worker_id.clone();
    failed.worker_id = Some(worker_id.clone());
    failed.attempt_id = Some(AttemptId::new("attempt-failed-lookup"));
    let mut later = execution();
    later.id = later_id.clone();
    later.labels.execution_id = later_id.clone();
    later.labels.worker_id = worker_id.clone();
    later.worker_id = Some(worker_id.clone());
    later.attempt_id = Some(AttemptId::new("attempt-later-authority"));
    let authorities = vec![
        CleanupAuthority {
            execution_id: failed_id.clone(),
            attempt_id: failed.attempt_id.clone().unwrap(),
            worker_id: worker_id.clone(),
            phase: CleanupDisposition::CleanupPending.to_string(),
            handles: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        CleanupAuthority {
            execution_id: later_id.clone(),
            attempt_id: later.attempt_id.clone().unwrap(),
            worker_id: worker_id.clone(),
            phase: CleanupDisposition::CleanupPending.to_string(),
            handles: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    ];
    let store = Arc::new(StartupExecutionStore {
        executions: Mutex::new(
            [(failed_id.clone(), failed), (later_id.clone(), later)]
                .into_iter()
                .collect(),
        ),
        cancellation_failure: failed_id.clone(),
    });
    let cleanup = Arc::new(StartupCleanupAuthorities {
        authorities,
        resolved: Mutex::new(Vec::new()),
    });
    let order = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = Arc::new(FakeLifecycle {
        order: order.clone(),
        fail_at: None,
        poll_empty: false,
        cleanup_fail: false,
        hang_poll: false,
    });
    let worker = Arc::new(Worker::new(
        lifecycle,
        store,
        Arc::new(FakeReservations::default()),
        cleanup.clone(),
    ));

    let tasks = worker.reconcile_startup(&worker_id).await.unwrap();

    assert!(tasks.is_empty());
    assert_eq!(cleanup.resolved.lock().unwrap().as_slice(), &[later_id]);
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &[
            "authority-cleanup",
            "authority-cleanup",
            "authority-cleanup",
            "authority-cleanup"
        ]
    );
    assert!(!cleanup.resolved.lock().unwrap().contains(&failed_id));
}

#[tokio::test]
async fn startup_does_not_spawn_adoption_before_all_fallible_reconciliation_completes() {
    let worker_id = WorkerId::new("worker-startup-two-phase");
    let adopt_id = ExecutionId::new("startup-adopt-before-later-failure");
    let fail_id = ExecutionId::new("startup-later-retained-failure");
    let mut adoptable = execution();
    adoptable.id = adopt_id.clone();
    adoptable.labels.execution_id = adopt_id.clone();
    adoptable.labels.worker_id = worker_id.clone();
    adoptable.worker_id = Some(worker_id.clone());
    adoptable.attempt_id = Some(AttemptId::new("attempt-adopt"));
    adoptable.manifest.persistence = PersistenceMode::Resumable;
    adoptable.state = ExecutionState::Running;
    adoptable.session_id = Some(SessionId::new("session-adopt"));
    adoptable.worktree_path = Some("/allocation/repository".into());
    let mut retained = execution();
    retained.id = fail_id.clone();
    retained.labels.execution_id = fail_id.clone();
    retained.labels.worker_id = worker_id.clone();
    retained.worker_id = Some(worker_id.clone());
    retained.attempt_id = Some(AttemptId::new("attempt-retained"));
    retained.state = ExecutionState::ReviewReady;
    let authorities = vec![
        CleanupAuthority {
            execution_id: adopt_id.clone(),
            attempt_id: adoptable.attempt_id.clone().unwrap(),
            worker_id: worker_id.clone(),
            phase: CleanupDisposition::Active(orchestrator_persistence::CleanupStage::Running)
                .to_string(),
            handles: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
        CleanupAuthority {
            execution_id: fail_id.clone(),
            attempt_id: retained.attempt_id.clone().unwrap(),
            worker_id: worker_id.clone(),
            phase: CleanupDisposition::Retained.to_string(),
            handles: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    ];
    let store = Arc::new(StartupExecutionStore {
        executions: Mutex::new(
            [(adopt_id, adoptable), (fail_id, retained)]
                .into_iter()
                .collect(),
        ),
        cancellation_failure: ExecutionId::new("no-cancellation-failure"),
    });
    let order = Arc::new(Mutex::new(Vec::new()));
    let reservations = Arc::new(FakeReservations {
        fail_retained: AtomicBool::new(true),
        ..Default::default()
    });
    let worker = Arc::new(Worker::new(
        Arc::new(FakeLifecycle {
            order: order.clone(),
            fail_at: None,
            poll_empty: false,
            cleanup_fail: false,
            hang_poll: false,
        }),
        store,
        reservations.clone(),
        Arc::new(StartupCleanupAuthorities {
            authorities,
            resolved: Mutex::new(Vec::new()),
        }),
    ));

    assert!(worker.reconcile_startup(&worker_id).await.is_err());
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(
        order.lock().unwrap().is_empty(),
        "a failed startup pass must not detach an already-planned adoption"
    );

    reservations.fail_retained.store(false, Ordering::SeqCst);
    let tasks = worker.reconcile_startup(&worker_id).await.unwrap();
    assert_eq!(tasks.len(), 1, "retry must own exactly one adoption");
    for task in tasks {
        task.join().await.unwrap();
    }
    assert_eq!(
        order
            .lock()
            .unwrap()
            .iter()
            .filter(|step| **step == "adopt")
            .count(),
        1,
        "retry must not duplicate execution"
    );
}

fn receipt(execution: &Execution) -> AllocationReceipt {
    AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.into(),
        labels: execution.labels.clone(),
        reserved_bytes: 1,
        mount_path: "/allocation".into(),
        backend_kind: "test".into(),
        backend_key: "key".into(),
        pool_identity: "pool".into(),
        backend: BackendIdentity::Apfs {
            container: "c".into(),
            container_uuid: "cu".into(),
            volume: "v".into(),
            volume_name: "vn".into(),
            volume_uuid: "vu".into(),
            ownership_token: "t".into(),
        },
        docker_bind: DockerBindProof {
            daemon_id: "daemon".into(),
            verifier: "probe".into(),
            method_version: "v1".into(),
            source_path: "/allocation".into(),
            filesystem_id: "fs".into(),
        },
    }
}
