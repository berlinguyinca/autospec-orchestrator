use chrono::Utc;
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, Execution, ExecutionEvent, ExecutionId,
    ExecutionManifest, ExecutionState, HarnessKind, ModelPolicy, OwnershipLabels, PersistenceMode,
    RepositoryReference, Role, RuntimeRequirement,
};
use orchestrator_persistence::{
    EventLog, ExecutionStore, PgEventLog, PgExecutionStore, StoreError,
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
