use chrono::{Duration, Utc};
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, Execution, ExecutionEvent, ExecutionId,
    ExecutionManifest, ExecutionState, HarnessKind, ModelPolicy, OwnershipLabels, PersistenceMode,
    RepositoryReference, Role, RuntimeRequirement,
};
use orchestrator_persistence::{
    EventLog, ExecutionStore, PgEventLog, PgExecutionStore, PgReservationStore, PgWorkerStore,
    ReservationStore, StoreError, WorkerStore,
};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

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
async fn progress_commit_persists_execution_attempt_and_event_atomically() {
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
async fn stale_worker_becomes_offline_and_fresh_proven_heartbeat_restores_ready() {
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
        orchestrator_core::WorkerState::Offline
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
    assert!(reservations.reconcile(&[]).await.unwrap().is_empty());
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}
