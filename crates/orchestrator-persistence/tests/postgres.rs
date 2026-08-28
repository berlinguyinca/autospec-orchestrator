use chrono::{Duration, Utc};
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, Execution, ExecutionEvent, ExecutionId,
    ExecutionManifest, ExecutionState, HarnessKind, ModelPolicy, OwnershipLabels, PersistenceMode,
    RepositoryReference, Role, RuntimeRequirement, WorkerId,
};
use orchestrator_persistence::{
    CleanupAuthorityStore, EventLog, ExecutionStore, LostWorkerRecovery, PgCleanupAuthorityStore,
    PgEventLog, PgExecutionStore, PgReservationStore, PgWorkerStore, ReservationStore, StoreError,
    WorkerStore,
};
use sqlx::postgres::PgPoolOptions;
use std::sync::{Arc, OnceLock};

fn database_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn stores() -> Option<(PgExecutionStore, PgEventLog)> {
    let Ok(url) = std::env::var("AUTOSPEC_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL test: AUTOSPEC_DATABASE_URL is not set");
        return None;
    };
    match PgPoolOptions::new().max_connections(1).connect(&url).await {
        Ok(pool) => pool.close().await,
        Err(error) => {
            eprintln!("skipping PostgreSQL test: database is unavailable: {error}");
            return None;
        }
    }
    let executions = PgExecutionStore::connect(&url)
        .await
        .expect("database migrations and execution schema must succeed");
    let events = PgEventLog::connect(&url)
        .await
        .expect("event log connects after execution store migrated the database");
    Some((executions, events))
}

#[tokio::test]
async fn cleanup_authority_is_durable_until_explicit_resolution() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real PostgreSQL test");
        return;
    };
    let store = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let worker_id = WorkerId::new(format!("worker-cleanup-{}", uuid::Uuid::new_v4().simple()));
    let execution_id = ExecutionId::new(format!(
        "execution-cleanup-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let attempt_id =
        orchestrator_core::AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
    store
        .begin(&execution_id, &attempt_id, &worker_id)
        .await
        .unwrap();
    let handles = serde_json::json!({
        "receipt": {"mount_path": "/allocations/execution"},
        "worktree": "/allocations/execution/repository",
        "container_id": "sha256:container",
        "session_id": "session-1"
    });
    store
        .checkpoint(&execution_id, "PI_STARTED", &handles)
        .await
        .unwrap();
    drop(store);

    let reopened = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let pending = reopened.list_for_worker(&worker_id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].phase, "PI_STARTED");
    assert_eq!(pending[0].handles, handles);
    reopened.resolve(&execution_id).await.unwrap();
    assert!(reopened
        .list_for_worker(&worker_id)
        .await
        .unwrap()
        .is_empty());
}

fn execution(state: ExecutionState) -> Execution {
    let id = ExecutionId::new(format!("test-{}", uuid::Uuid::new_v4().simple()));
    let now = Utc::now();
    Execution {
        id: id.clone(),
        role: Role::Implementation,
        state,
        manifest: ExecutionManifest {
            api_version: orchestrator_core::MANIFEST_API_VERSION.to_owned(),
            role: Role::Implementation,
            task: None,
            repository: RepositoryReference {
                repo: "InferWeave/autospec-orchestrator".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: None,
                branch: None,
            },
            agent: AgentAssignment {
                harness: HarnessKind::Pi,
                model_policy: ModelPolicy {
                    provider: "inferweave".to_owned(),
                    preferred: vec!["test-model".to_owned()],
                    alternatives: Vec::new(),
                    fallback_class: None,
                },
            },
            runtime: RuntimeRequirement::default(),
            services: Vec::new(),
            persistence: PersistenceMode::Ephemeral,
            task_packet: None,
        },
        worker_id: None,
        attempt_id: None,
        session_id: None,
        worktree_path: None,
        labels: OwnershipLabels {
            execution_id: id,
            worker_id: orchestrator_core::WorkerId::new("test-worker"),
            repository: "InferWeave/autospec-orchestrator".to_owned(),
            issue: None,
        },
        created_at: now,
        updated_at: now,
        result: None,
    }
}

fn event(id: &ExecutionId) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: id.clone(),
        attempt_id: None,
        sequence: 0,
        at: Utc::now(),
        state: ExecutionState::Running,
        kind: ExecutionEventKind::EnvironmentReady,
    }
}

#[tokio::test]
async fn execution_round_trip_transition_and_live_filter_are_durable() {
    let _database_test = database_test_lock().lock().await;
    let Some((store, _)) = stores().await else {
        return;
    };
    let queued = execution(ExecutionState::Queued);
    store.insert(&queued).await.expect("insert succeeds");

    let fetched = store.get(&queued.id).await.expect("execution is readable");
    assert_eq!(
        serde_json::to_value(fetched).unwrap(),
        serde_json::to_value(&queued).unwrap()
    );

    let transitioned = store
        .transition(&queued.id, ExecutionState::WorkerAssigned)
        .await
        .expect("legal transition succeeds");
    assert_eq!(transitioned.state, ExecutionState::WorkerAssigned);
    assert_eq!(
        store.get(&queued.id).await.unwrap().state,
        ExecutionState::WorkerAssigned
    );

    let terminal = execution(ExecutionState::Completed);
    store
        .insert(&terminal)
        .await
        .expect("terminal insert succeeds");
    let live = store.list_live().await.expect("live executions list");
    assert!(live.iter().any(|item| item.id == queued.id));
    assert!(!live.iter().any(|item| item.id == terminal.id));
}

#[tokio::test]
async fn illegal_and_concurrent_transitions_are_serialized() {
    let _database_test = database_test_lock().lock().await;
    let Some((store, _)) = stores().await else {
        return;
    };
    let queued = execution(ExecutionState::Queued);
    store.insert(&queued).await.unwrap();
    assert!(matches!(
        store.transition(&queued.id, ExecutionState::Running).await,
        Err(StoreError::IllegalTransition {
            from: ExecutionState::Queued,
            to: ExecutionState::Running
        })
    ));

    let store = Arc::new(store);
    let first = {
        let store = Arc::clone(&store);
        let id = queued.id.clone();
        tokio::spawn(async move { store.transition(&id, ExecutionState::WorkerAssigned).await })
    };
    let second = {
        let store = Arc::clone(&store);
        let id = queued.id.clone();
        tokio::spawn(async move { store.transition(&id, ExecutionState::WorkerAssigned).await })
    };
    let results = [first.await.unwrap(), second.await.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
}

#[tokio::test]
async fn concurrent_events_are_gapless_and_replay_in_order() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, log)) = stores().await else {
        return;
    };
    let id = ExecutionId::new(format!("events-{}", uuid::Uuid::new_v4().simple()));
    let log = Arc::new(log);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let log = Arc::clone(&log);
        let event = event(&id);
        tasks.push(tokio::spawn(
            async move { log.append(&event).await.unwrap() },
        ));
    }
    let mut sequences = Vec::new();
    for task in tasks {
        sequences.push(task.await.unwrap());
    }
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=8).collect::<Vec<_>>());

    let replay = log.since(&id, 3).await.expect("events replay");
    assert_eq!(
        replay.iter().map(|item| item.sequence).collect::<Vec<_>>(),
        vec![4, 5, 6, 7, 8]
    );
}

#[tokio::test]
async fn event_replay_returns_the_complete_tail_without_a_pagination_contract() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, log)) = stores().await else {
        return;
    };
    let id = ExecutionId::new(format!("batch-{}", uuid::Uuid::new_v4().simple()));
    for _ in 0..501 {
        log.append(&event(&id)).await.unwrap();
    }

    assert_eq!(log.since(&id, 0).await.unwrap().len(), 501);
}

fn registered_worker(id: &str, slots: u32) -> orchestrator_core::WorkerRegistration {
    serde_json::from_value(serde_json::json!({
        "id": id,
        "capabilities": {
            "os": "linux",
            "arch": "x86_64",
            "cpu": 16,
            "memoryMib": 32768,
            "diskGib": 500,
            "runtimes": ["docker"],
            "capabilities": ["docker"],
            "maxConcurrentExecutions": slots
        },
        "state": "READY",
        "running_executions": 0,
        "last_heartbeat": Utc::now(),
        "capability_proof": {
            "storage_backend": "test",
            "storage_pool_identity": "pool-a",
            "docker_daemon_id": "daemon-a",
            "docker_verifier": "bind-probe",
            "docker_method_version": "v1"
        }
    }))
    .unwrap()
}

async fn worker_stores() -> Option<(PgExecutionStore, PgWorkerStore, PgReservationStore)> {
    let Ok(url) = std::env::var("AUTOSPEC_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL test: AUTOSPEC_DATABASE_URL is not set");
        return None;
    };
    let executions = PgExecutionStore::connect(&url).await.unwrap();
    let workers = PgWorkerStore::connect(&url).await.unwrap();
    let reservations = PgReservationStore::connect(&url).await.unwrap();
    Some((executions, workers, reservations))
}

#[tokio::test]
async fn worker_registration_requires_storage_and_docker_capability_proof() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, workers, _)) = worker_stores().await else {
        return;
    };
    let valid = registered_worker(&format!("worker-{}", uuid::Uuid::new_v4().simple()), 4);
    workers.register(&valid).await.unwrap();
    workers.register(&valid).await.unwrap();
    assert_eq!(workers.get(&valid.id).await.unwrap().id, valid.id);

    let mut missing_proof = valid.clone();
    missing_proof.id = orchestrator_core::WorkerId::new(format!(
        "worker-missing-proof-{}",
        uuid::Uuid::new_v4().simple()
    ));
    missing_proof.capability_proof = None;
    assert!(matches!(
        workers.register(&missing_proof).await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn concurrent_reservation_assigns_each_execution_once_without_oversubscription() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-slots-{}", uuid::Uuid::new_v4().simple()),
        4,
    );
    workers.register(&worker).await.unwrap();
    for _ in 0..16 {
        executions
            .insert(&execution(ExecutionState::Queued))
            .await
            .unwrap();
    }
    let reservations = Arc::new(reservations);
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let reservations = Arc::clone(&reservations);
        let worker_id = worker.id.clone();
        tasks.push(tokio::spawn(async move {
            reservations.reserve_next(&worker_id).await
        }));
    }
    let mut assigned = Vec::new();
    for task in tasks {
        if let Some(reservation) = task.await.unwrap().unwrap() {
            assigned.push(reservation);
        }
    }
    assert_eq!(assigned.len(), 4);
    let mut ids = assigned
        .iter()
        .map(|reservation| reservation.execution.id.to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);
    assert!(assigned.iter().all(|reservation| {
        reservation.execution.state == ExecutionState::WorkerAssigned
            && reservation.execution.worker_id.as_ref() == Some(&worker.id)
            && reservation.execution.attempt_id.as_ref() == Some(&reservation.attempt_id)
    }));
    assert_eq!(
        reservations
            .list_for_worker(&worker.id)
            .await
            .unwrap()
            .len(),
        4
    );

    for reservation in &assigned {
        reservations
            .release(&reservation.execution.id)
            .await
            .unwrap();
        reservations
            .release(&reservation.execution.id)
            .await
            .unwrap();
    }
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn stale_attempt_cleanup_cannot_release_a_reassigned_reservation() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-fenced-release-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    executions
        .insert(&execution(ExecutionState::Queued))
        .await
        .unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let stale_attempt = reservation.attempt_id;
    let replacement_attempt =
        orchestrator_core::AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE reservations SET attempt_id = $2 WHERE execution_id = $1")
        .bind(reservation.execution.id.as_str())
        .bind(replacement_attempt.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(!reservations
        .release_attempt(&reservation.execution.id, &stale_attempt)
        .await
        .unwrap());
    let remaining = reservations.list_for_worker(&worker.id).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].attempt_id, replacement_attempt);
    assert!(reservations
        .release_attempt(&reservation.execution.id, &replacement_attempt)
        .await
        .unwrap());
}

#[tokio::test]
async fn progress_commit_persists_execution_attempt_and_event_atomically() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-progress-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut running = reservation.execution.clone();
    running.transition(ExecutionState::Provisioning).unwrap();
    running.worktree_path = Some("/verified/worktree".to_owned());
    let progress = ExecutionEvent {
        execution_id: running.id.clone(),
        attempt_id: running.attempt_id.clone(),
        sequence: 0,
        at: Utc::now(),
        state: running.state,
        kind: ExecutionEventKind::EnvironmentReady,
    };
    let sequence = executions
        .record_progress(&running, &progress)
        .await
        .unwrap();
    assert_eq!(sequence, 1);
    let persisted = executions.get(&running.id).await.unwrap();
    assert_eq!(
        persisted.worktree_path.as_deref(),
        Some("/verified/worktree")
    );
    assert_eq!(persisted.attempt_id, running.attempt_id);
    let events = PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap()
        .since(&running.id, 0)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
}

#[tokio::test]
async fn append_and_progress_share_one_gapless_sequence_allocator() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-sequence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut running = reservation.execution;
    running.transition(ExecutionState::Provisioning).unwrap();
    let log = Arc::new(
        PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
            .await
            .unwrap(),
    );
    let executions = Arc::new(executions);
    let barrier = Arc::new(tokio::sync::Barrier::new(64));
    let mut tasks = Vec::new();
    for index in 0..64 {
        let barrier = barrier.clone();
        if index % 2 == 0 {
            let executions = executions.clone();
            let running = running.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                executions
                    .record_progress(
                        &running,
                        &progress_event(&running, ExecutionEventKind::EnvironmentReady),
                    )
                    .await
            }));
        } else {
            let log = log.clone();
            let event = progress_event(&running, ExecutionEventKind::EnvironmentReady);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                log.append(&event).await
            }));
        }
    }
    let mut sequences = Vec::new();
    for task in tasks {
        sequences.push(task.await.unwrap().unwrap());
    }
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=64).collect::<Vec<_>>());
    assert_eq!(log.since(&running.id, 0).await.unwrap().len(), 64);
}

#[tokio::test]
async fn progress_rejects_stale_cancelled_reassigned_and_forged_attempts() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-fence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut provisioning = reservation.execution.clone();
    provisioning
        .transition(ExecutionState::Provisioning)
        .unwrap();
    let progress = progress_event(&provisioning, ExecutionEventKind::EnvironmentReady);
    executions
        .record_progress(&provisioning, &progress)
        .await
        .unwrap();

    let stale = provisioning.clone();
    let mut cancelled = provisioning.clone();
    cancelled.transition(ExecutionState::Cancelled).unwrap();
    let cancelled_event = progress_event(&cancelled, ExecutionEventKind::ExecutionCancelled);
    executions
        .record_progress(&cancelled, &cancelled_event)
        .await
        .unwrap();

    let mut stale_running = stale;
    stale_running.transition(ExecutionState::Running).unwrap();
    let stale_event = progress_event(
        &stale_running,
        ExecutionEventKind::AgentStarted {
            session_id: orchestrator_core::SessionId::new("stale"),
        },
    );
    assert!(matches!(
        executions
            .record_progress(&stale_running, &stale_event)
            .await,
        Err(StoreError::Conflict(_)) | Err(StoreError::IllegalTransition { .. })
    ));

    let mut forged = cancelled.clone();
    forged.worker_id = Some(orchestrator_core::WorkerId::new("foreign-worker"));
    let forged_event = progress_event(&forged, ExecutionEventKind::ExecutionCancelled);
    assert!(matches!(
        executions.record_progress(&forged, &forged_event).await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        executions.get(&cancelled.id).await.unwrap().state,
        ExecutionState::Cancelled
    );
    let events = PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap()
        .since(&cancelled.id, 0)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
}

fn progress_event(execution: &Execution, kind: ExecutionEventKind) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: execution.id.clone(),
        attempt_id: execution.attempt_id.clone(),
        sequence: 0,
        at: Utc::now(),
        state: execution.state,
        kind,
    }
}

#[tokio::test]
async fn stale_worker_becomes_unreachable_and_fresh_proven_heartbeat_restores_ready() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, workers, _)) = worker_stores().await else {
        return;
    };
    let mut worker = registered_worker(
        &format!("worker-heartbeat-{}", uuid::Uuid::new_v4().simple()),
        2,
    );
    worker.last_heartbeat = Utc::now() - Duration::seconds(120);
    workers.register(&worker).await.unwrap();
    let stale = workers
        .mark_stale_before(Utc::now() - Duration::seconds(90))
        .await
        .unwrap();
    assert!(stale.contains(&worker.id));
    assert_eq!(
        workers.get(&worker.id).await.unwrap().state,
        orchestrator_core::WorkerState::Unreachable
    );

    worker.state = orchestrator_core::WorkerState::Ready;
    worker.last_heartbeat = Utc::now();
    assert_eq!(
        workers.heartbeat(&worker).await.unwrap().state,
        orchestrator_core::WorkerState::Ready
    );
}

#[tokio::test]
async fn orphan_reservation_reconcile_is_idempotent() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-orphan-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let assigned = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    assert!(reservations
        .reconcile(&[])
        .await
        .unwrap()
        .contains(&assigned.execution.id));
    assert!(!reservations
        .reconcile(&[])
        .await
        .unwrap()
        .contains(&assigned.execution.id));
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unreachable_worker_atomically_requeues_resumable_and_fails_ephemeral_attempts() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let mut worker =
        registered_worker(&format!("worker-lost-{}", uuid::Uuid::new_v4().simple()), 2);
    let isolation_capability = format!("lost-worker-{}", uuid::Uuid::new_v4().simple());
    worker
        .capabilities
        .capabilities
        .push(isolation_capability.clone());
    worker.capabilities.cpu = 2;
    worker.capabilities.memory_mib = 2048;
    worker.capabilities.disk_gib = 2;
    workers.register(&worker).await.unwrap();
    let mut resumable = execution(ExecutionState::Queued);
    resumable.manifest.persistence = PersistenceMode::Resumable;
    resumable
        .manifest
        .runtime
        .capabilities
        .push(isolation_capability.clone());
    resumable.manifest.runtime.cpu = 1;
    resumable.manifest.runtime.memory_mib = 1024;
    resumable.manifest.runtime.disk_gib = 1;
    let mut ephemeral = execution(ExecutionState::Queued);
    ephemeral
        .manifest
        .runtime
        .capabilities
        .push(isolation_capability);
    ephemeral.manifest.runtime.cpu = 1;
    ephemeral.manifest.runtime.memory_mib = 1024;
    ephemeral.manifest.runtime.disk_gib = 1;
    executions.insert(&resumable).await.unwrap();
    executions.insert(&ephemeral).await.unwrap();
    let resumable = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let ephemeral = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    worker.last_heartbeat = Utc::now() - Duration::seconds(120);
    workers.heartbeat(&worker).await.unwrap();
    workers
        .mark_stale_before(Utc::now() - Duration::seconds(90))
        .await
        .unwrap();

    let recovered = reservations.recover_unreachable(&worker.id).await.unwrap();

    assert!(recovered.iter().any(|item| matches!(
        item,
        LostWorkerRecovery::Requeued(id) if id == &resumable.execution.id
    )));
    assert!(recovered.iter().any(|item| matches!(
        item,
        LostWorkerRecovery::Failed(id) if id == &ephemeral.execution.id
    )));
    let requeued = executions.get(&resumable.execution.id).await.unwrap();
    assert_eq!(requeued.state, ExecutionState::Queued);
    assert!(requeued.worker_id.is_none());
    assert!(requeued.attempt_id.is_none());
    let failed = executions.get(&ephemeral.execution.id).await.unwrap();
    assert_eq!(failed.state, ExecutionState::Failed);
    assert_eq!(
        failed.result.unwrap().failure,
        Some(orchestrator_core::FailureClass::WorkerLost)
    );
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unreachable_reaper_skips_already_fenced_attempt_with_retained_cleanup_reservation() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-fenced-reap-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_attempts SET state = 'FAILED', finished_at = now() WHERE attempt_id = $1",
    )
    .bind(reservation.attempt_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE workers SET state = 'UNREACHABLE' WHERE id = $1")
        .bind(worker.id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(reservations
        .recover_unreachable(&worker.id)
        .await
        .unwrap()
        .is_empty());
    let retained = reservations.list_for_worker(&worker.id).await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].attempt_id, reservation.attempt_id);

    assert!(reservations
        .release_attempt(&reservation.execution.id, &reservation.attempt_id)
        .await
        .unwrap());
}
