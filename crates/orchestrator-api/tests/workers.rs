use chrono::{Duration, Utc};
use orchestrator_api::{router, AppState, PerWorkerTokens};
use orchestrator_core::{
    ExecutionId, RuntimeKind, WorkerCapabilities, WorkerCapabilityProof, WorkerId,
    WorkerRegistration, WorkerState,
};
use orchestrator_persistence::test_support::DisposableTestDatabaseLock;
use orchestrator_persistence::{
    CleanupAuthorityStore, CleanupDisposition, CleanupStage, PgArtifactStore,
    PgCleanupAuthorityStore, PgEventLog, PgExecutionStore, PgReservationStore, PgWorkerStore,
    WorkerStore,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test]
async fn authenticated_worker_routes_own_liveness_and_reap_after_ninety_seconds() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let store = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let token = format!("worker-token-{}", uuid::Uuid::new_v4());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let state = app_state(&database_url, store.clone(), reservations, token.clone()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, router(server_state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}/api/v1/workers");
    let mut advertised = registration(&format!("worker-api-{}", uuid::Uuid::new_v4().simple()));
    advertised.last_heartbeat = Utc::now() - Duration::days(30);
    advertised.state = WorkerState::Draining;
    advertised.running_executions = 63;

    assert_eq!(
        client
            .post(&base)
            .json(&advertised)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&base)
            .header("Authorization", "Basic worker-token")
            .json(&advertised)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let created = client
        .post(&base)
        .bearer_auth(&token)
        .json(&advertised)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let created: WorkerRegistration = created.json().await.unwrap();
    assert_eq!(created.state, WorkerState::Ready);
    assert_eq!(created.running_executions, 0);
    assert!(created.last_heartbeat > Utc::now() - Duration::seconds(5));

    let replay = client
        .post(&base)
        .bearer_auth(&token)
        .json(&advertised)
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    assert_eq!(
        store
            .list()
            .await
            .unwrap()
            .into_iter()
            .filter(|worker| worker.id == advertised.id)
            .count(),
        1
    );

    assert_eq!(
        client
            .get(&base)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let listed = client
        .get(&base)
        .bearer_auth("api-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), reqwest::StatusCode::OK);
    let listed: Vec<WorkerRegistration> = listed.json().await.unwrap();
    assert!(listed.iter().any(|worker| worker.id == advertised.id));

    assert_eq!(
        client
            .get(format!("{base}?limit=0"))
            .bearer_auth("api-secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!("{base}?limit=101"))
            .bearer_auth("api-secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let bounded = client
        .get(format!("{base}?limit=1&cursor={}", advertised.id))
        .bearer_auth("api-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(bounded.status(), reqwest::StatusCode::OK);
    assert!(bounded
        .json::<Vec<WorkerRegistration>>()
        .await
        .unwrap()
        .iter()
        .all(|worker| worker.id.as_str() > advertised.id.as_str()));

    let heartbeat = client
        .post(format!("{}/{}/heartbeat", base, advertised.id))
        .bearer_auth(&token)
        .json(&advertised)
        .send()
        .await
        .unwrap();
    assert_eq!(heartbeat.status(), reqwest::StatusCode::OK);
    let heartbeat: WorkerRegistration = heartbeat.json().await.unwrap();
    assert_eq!(heartbeat.state, WorkerState::Ready);
    assert!(heartbeat.last_heartbeat > created.last_heartbeat);

    let mut stale_registration = heartbeat.clone();
    stale_registration.last_heartbeat = Utc::now() - Duration::seconds(91);
    store.heartbeat(&stale_registration).await.unwrap();
    let reaped = state.reap_stale(Utc::now()).await.unwrap();
    assert!(reaped.contains(&advertised.id));
    assert_eq!(
        store.get(&advertised.id).await.unwrap().state,
        WorkerState::Unreachable
    );
}

#[tokio::test]
async fn authenticated_execution_cleanup_requests_durable_reconciliation() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let execution_id = orchestrator_core::ExecutionId::new(format!(
        "cleanup-api-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let attempt_id =
        orchestrator_core::AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
    let worker_id = WorkerId::new(format!("worker-{}", uuid::Uuid::new_v4().simple()));
    cleanup
        .begin(&execution_id, &attempt_id, &worker_id)
        .await
        .unwrap();
    let handles = serde_json::json!({"session": "durable"});
    let transitions = [
        (
            CleanupDisposition::Active(CleanupStage::Reserved),
            CleanupDisposition::Active(CleanupStage::Storage),
        ),
        (
            CleanupDisposition::Active(CleanupStage::Storage),
            CleanupDisposition::Active(CleanupStage::Worktree),
        ),
        (
            CleanupDisposition::Active(CleanupStage::Worktree),
            CleanupDisposition::Active(CleanupStage::Runtime),
        ),
        (
            CleanupDisposition::Active(CleanupStage::Runtime),
            CleanupDisposition::Active(CleanupStage::PiStarted),
        ),
        (
            CleanupDisposition::Active(CleanupStage::PiStarted),
            CleanupDisposition::Active(CleanupStage::Running),
        ),
        (
            CleanupDisposition::Active(CleanupStage::Running),
            CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
        ),
        (
            CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
            CleanupDisposition::RetainRequested,
        ),
        (
            CleanupDisposition::RetainRequested,
            CleanupDisposition::Retained,
        ),
    ];
    for (from, to) in transitions {
        cleanup
            .transition(&execution_id, from, to, &handles)
            .await
            .unwrap();
    }
    let state = app_state(&database_url, workers, reservations, "secret".into())
        .await
        .with_cleanup_authorities(cleanup.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/executions/{execution_id}/cleanup");
    let health_url = format!("http://{address}/api/v1/operator/cleanup-health");

    assert_eq!(
        client.get(&health_url).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let health = client
        .get(&health_url)
        .bearer_auth("api-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    let health = health.json::<serde_json::Value>().await.unwrap();
    assert!(health["unresolved"].as_u64().unwrap() >= 1);
    assert!(health["retained"].as_u64().unwrap() >= 1);
    assert!(health.get("oldest_unresolved_at").is_some());

    assert_eq!(
        client.post(&url).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    assert_eq!(
        cleanup
            .get(&execution_id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::CleanupPending
    );

    let active_execution =
        ExecutionId::new(format!("cleanup-active-{}", uuid::Uuid::new_v4().simple()));
    cleanup
        .begin(
            &active_execution,
            &orchestrator_core::AttemptId::new("active-attempt"),
            &WorkerId::new("active-worker"),
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .post(format!(
                "http://{address}/api/v1/executions/{active_execution}/cleanup"
            ))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    assert_eq!(
        cleanup
            .get(&active_execution)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::Active(CleanupStage::Reserved)
    );
}

async fn retained_execution(cleanup: &PgCleanupAuthorityStore, label: &str) -> ExecutionId {
    let execution_id = ExecutionId::new(format!("{label}-{}", uuid::Uuid::new_v4().simple()));
    cleanup
        .begin(
            &execution_id,
            &orchestrator_core::AttemptId::new(format!(
                "attempt-{}",
                uuid::Uuid::new_v4().simple()
            )),
            &WorkerId::new(format!("worker-{}", uuid::Uuid::new_v4().simple())),
        )
        .await
        .unwrap();
    let handles = serde_json::json!({"session": "durable"});
    let path = [
        CleanupDisposition::Active(CleanupStage::Reserved),
        CleanupDisposition::Active(CleanupStage::Storage),
        CleanupDisposition::Active(CleanupStage::Worktree),
        CleanupDisposition::Active(CleanupStage::Runtime),
        CleanupDisposition::Active(CleanupStage::PiStarted),
        CleanupDisposition::Active(CleanupStage::Running),
        CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
        CleanupDisposition::RetainRequested,
        CleanupDisposition::Retained,
    ];
    for pair in path.windows(2) {
        cleanup
            .transition(&execution_id, pair[0], pair[1], &handles)
            .await
            .unwrap();
    }
    execution_id
}

#[tokio::test]
async fn post_merge_cleanup_accepts_the_api_token_and_is_idempotent() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let retained = retained_execution(&cleanup, "merge-retained").await;
    let state = app_state(&database_url, workers, reservations, "worker-secret".into())
        .await
        .with_cleanup_authorities(cleanup.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/executions/{retained}/cleanup");

    // No token, or a token that is neither credential: 401, nothing moves.
    assert_eq!(
        client.post(&url).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("not-a-token")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        cleanup.get(&retained).await.unwrap().disposition().unwrap(),
        CleanupDisposition::Retained
    );

    // The API token moves Retained to CleanupPending.
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("api-secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    let first = cleanup.get(&retained).await.unwrap();
    assert_eq!(
        first.disposition().unwrap(),
        CleanupDisposition::CleanupPending
    );

    // Replays (either credential) are 202 and write nothing.
    for token in ["api-secret", "worker-secret"] {
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(token)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::ACCEPTED
        );
        let again = cleanup.get(&retained).await.unwrap();
        assert_eq!(again.phase, first.phase);
        assert_eq!(again.updated_at, first.updated_at, "replay wrote a row");
    }

    // A later phase is also a no-op 202 and is not dragged back.
    cleanup
        .transition(
            &retained,
            CleanupDisposition::CleanupPending,
            CleanupDisposition::RuntimeStopped,
            &first.handles,
        )
        .await
        .unwrap();
    let advanced = cleanup.get(&retained).await.unwrap();
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("api-secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    let after = cleanup.get(&retained).await.unwrap();
    assert_eq!(after.phase, advanced.phase);
    assert_eq!(after.updated_at, advanced.updated_at);

    // Concurrent identical requests on a fresh retained execution all succeed.
    let racing = retained_execution(&cleanup, "merge-racing").await;
    let racing_url = format!("http://{address}/api/v1/executions/{racing}/cleanup");
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let client = client.clone();
        let racing_url = racing_url.clone();
        tasks.push(tokio::spawn(async move {
            client
                .post(racing_url)
                .bearer_auth("api-secret")
                .send()
                .await
                .unwrap()
                .status()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), reqwest::StatusCode::ACCEPTED);
    }
}

#[tokio::test]
async fn post_merge_cleanup_refuses_active_and_unknown_executions_untouched() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let state = app_state(&database_url, workers, reservations, "worker-secret".into())
        .await
        .with_cleanup_authorities(cleanup.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let post = |id: &ExecutionId, token: &'static str| {
        let request = client
            .post(format!("http://{address}/api/v1/executions/{id}/cleanup"))
            .bearer_auth(token);
        async move { request.send().await.unwrap() }
    };

    let active = ExecutionId::new(format!("merge-active-{}", uuid::Uuid::new_v4().simple()));
    cleanup
        .begin(
            &active,
            &orchestrator_core::AttemptId::new("active-attempt"),
            &WorkerId::new("active-worker"),
        )
        .await
        .unwrap();
    let before = cleanup.get(&active).await.unwrap();
    for token in ["api-secret", "worker-secret"] {
        let response = post(&active, token).await;
        assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        let body = response.text().await.unwrap();
        assert!(body.contains("CLEANUP_EXECUTION_ACTIVE"), "{body}");
    }
    let after = cleanup.get(&active).await.unwrap();
    assert_eq!(after.phase, before.phase);
    assert_eq!(after.updated_at, before.updated_at);

    // An execution that only asked to be retained is not yet cleanable.
    let requested = ExecutionId::new(format!("merge-req-{}", uuid::Uuid::new_v4().simple()));
    cleanup
        .begin(
            &requested,
            &orchestrator_core::AttemptId::new("req-attempt"),
            &WorkerId::new("req-worker"),
        )
        .await
        .unwrap();
    let handles = serde_json::json!({});
    let steps = [
        CleanupDisposition::Active(CleanupStage::Reserved),
        CleanupDisposition::Active(CleanupStage::Storage),
        CleanupDisposition::Active(CleanupStage::Worktree),
        CleanupDisposition::Active(CleanupStage::Runtime),
        CleanupDisposition::Active(CleanupStage::PiStarted),
        CleanupDisposition::Active(CleanupStage::Running),
        CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
        CleanupDisposition::RetainRequested,
    ];
    for pair in steps.windows(2) {
        cleanup
            .transition(&requested, pair[0], pair[1], &handles)
            .await
            .unwrap();
    }
    assert_eq!(
        post(&requested, "api-secret").await.status(),
        reqwest::StatusCode::CONFLICT
    );
    assert_eq!(
        cleanup
            .get(&requested)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::RetainRequested
    );

    let unknown = ExecutionId::new("merge-never-existed");
    assert_eq!(
        post(&unknown, "api-secret").await.status(),
        reqwest::StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn worker_routes_reject_invalid_and_oversized_bodies() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let store = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let state = app_state(&database_url, store, reservations, "secret".into()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}/api/v1/workers");
    let invalid = client
        .post(&base)
        .bearer_auth("secret")
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    let oversized = client
        .post(&base)
        .bearer_auth("secret")
        .header("content-type", "application/json")
        .body(vec![b'x'; 1_048_577])
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn worker_api_registers_every_preparation_failure_as_safe_offline_health() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let store = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let token = format!("worker-token-{}", uuid::Uuid::new_v4());
    let state = app_state(&database_url, store.clone(), reservations, token.clone()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{address}/api/v1/workers");

    for (kind, code) in [
        ("configuration", "worker-configuration-invalid"),
        ("persistence", "worker-preparation-unavailable"),
        ("recovery", "worker-preparation-unavailable"),
        ("storage", "storage-capability-unavailable"),
        ("docker", "docker-capability-unavailable"),
    ] {
        let mut worker = registration(&format!(
            "worker-api-{kind}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        worker.state = WorkerState::Offline;
        worker.capability_proof = None;
        worker.capabilities.runtimes.clear();
        worker.capabilities.capabilities.clear();
        worker.capabilities.health_errors = vec![code.to_owned()];
        let response = client
            .post(&url)
            .bearer_auth(&token)
            .json(&worker)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CREATED, "{kind}");
        let registered: WorkerRegistration = response.json().await.unwrap();
        assert_eq!(registered.state, WorkerState::Offline);
        assert_eq!(registered.capabilities.health_errors, [code]);
        assert_eq!(
            store.get(&registered.id).await.unwrap().state,
            WorkerState::Offline
        );
    }
}

#[tokio::test]
async fn disk_pressure_health_errors_take_a_worker_offline_and_clear_on_the_next_heartbeat() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let _database_lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let store = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let token = format!("worker-token-{}", uuid::Uuid::new_v4());
    let state = app_state(&database_url, store.clone(), reservations, token.clone()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    let client = reqwest::Client::new();
    let base = format!("http://{address}/api/v1/workers");
    let mut worker = registration(&format!("worker-api-{}", uuid::Uuid::new_v4().simple()));
    for code in ["disk-pressure-bytes", "disk-pressure-inodes"] {
        worker.capabilities.health_errors = vec![code.to_owned()];
        let response = client
            .post(format!("{base}/{}/heartbeat", worker.id))
            .bearer_auth(&token)
            .json(&worker)
            .send()
            .await
            .unwrap();
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let created = client
                .post(&base)
                .bearer_auth(&token)
                .json(&worker)
                .send()
                .await
                .unwrap();
            assert_eq!(created.status(), reqwest::StatusCode::CREATED, "{code}");
            let created: WorkerRegistration = created.json().await.unwrap();
            assert_eq!(created.state, WorkerState::Offline, "{code}");
        } else {
            assert_eq!(response.status(), reqwest::StatusCode::OK, "{code}");
            let beat: WorkerRegistration = response.json().await.unwrap();
            assert_eq!(beat.state, WorkerState::Offline, "{code}");
        }
        assert_eq!(
            store.get(&worker.id).await.unwrap().state,
            WorkerState::Offline,
            "{code}"
        );
    }
    worker.capabilities.health_errors.clear();
    let response = client
        .post(format!("{base}/{}/heartbeat", worker.id))
        .bearer_auth(&token)
        .json(&worker)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let beat: WorkerRegistration = response.json().await.unwrap();
    assert_eq!(beat.state, WorkerState::Ready);
    assert_eq!(
        store.get(&worker.id).await.unwrap().state,
        WorkerState::Ready
    );
}

fn registration(id: &str) -> WorkerRegistration {
    WorkerRegistration {
        id: WorkerId::new(id),
        capabilities: WorkerCapabilities {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            cpu: 4,
            memory_mib: 4096,
            disk_gib: 40,
            runtimes: vec![RuntimeKind::Docker],
            capabilities: vec!["docker".into()],
            toolchains: Default::default(),
            max_concurrent_executions: 2,
            health_errors: Vec::new(),
        },
        state: WorkerState::Ready,
        running_executions: 0,
        last_heartbeat: Utc::now(),
        capability_proof: Some(WorkerCapabilityProof {
            storage_backend: "apfs".into(),
            storage_pool_identity: "pool".into(),
            docker_daemon_id: "daemon".into(),
            docker_verifier: "immutable@sha256:abc".into(),
            docker_method_version: "v1".into(),
        }),
    }
}

async fn app_state(
    database_url: &str,
    workers: Arc<PgWorkerStore>,
    reservations: Arc<PgReservationStore>,
    worker_token: String,
) -> AppState {
    AppState::new(
        Arc::new(PgExecutionStore::connect(database_url).await.unwrap()),
        Arc::new(PgEventLog::connect(database_url).await.unwrap()),
        workers,
        reservations,
        Arc::new(
            PgArtifactStore::connect(database_url, std::env::temp_dir())
                .await
                .unwrap(),
        ),
        "api-secret".into(),
        worker_token,
    )
}

fn digest_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct IdentityFixture {
    base: String,
    client: reqwest::Client,
    w1: String,
    w2: String,
    t1: String,
    t2: String,
    shared: String,
    cleanup: Arc<PgCleanupAuthorityStore>,
}

/// A real router whose worker auth is per-worker tokens only.
async fn identity_fixture(
    per_worker: bool,
) -> Option<(IdentityFixture, DisposableTestDatabaseLock)> {
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").ok()?;
    let lock = DisposableTestDatabaseLock::acquire(&database_url)
        .await
        .unwrap();
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let (w1, w2) = (format!("ident-w1-{nonce}"), format!("ident-w2-{nonce}"));
    let (t1, t2) = (format!("tok1-{nonce}"), format!("tok2-{nonce}"));
    let shared = format!("shared-{nonce}");
    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let mut state = app_state(&database_url, workers, reservations, shared.clone())
        .await
        .with_cleanup_authorities(cleanup.clone());
    if per_worker {
        let file = format!(
            "# test identities\n{w1} {}\n{w2} {}\n",
            digest_hex(&t1),
            digest_hex(&t2)
        );
        state = state.with_worker_tokens(PerWorkerTokens::parse(&file).unwrap());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    Some((
        IdentityFixture {
            base: format!("http://{address}/api/v1"),
            client: reqwest::Client::new(),
            w1,
            w2,
            t1,
            t2,
            shared,
            cleanup,
        },
        lock,
    ))
}

async fn retained_execution_owned_by(
    cleanup: &PgCleanupAuthorityStore,
    owner: &str,
) -> ExecutionId {
    let execution_id = ExecutionId::new(format!("ident-exec-{}", uuid::Uuid::new_v4().simple()));
    cleanup
        .begin(
            &execution_id,
            &orchestrator_core::AttemptId::new(format!(
                "attempt-{}",
                uuid::Uuid::new_v4().simple()
            )),
            &WorkerId::new(owner),
        )
        .await
        .unwrap();
    let handles = serde_json::json!({"session": "durable"});
    let path = [
        CleanupDisposition::Active(CleanupStage::Reserved),
        CleanupDisposition::Active(CleanupStage::Storage),
        CleanupDisposition::Active(CleanupStage::Worktree),
        CleanupDisposition::Active(CleanupStage::Runtime),
        CleanupDisposition::Active(CleanupStage::PiStarted),
        CleanupDisposition::Active(CleanupStage::Running),
        CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
        CleanupDisposition::RetainRequested,
        CleanupDisposition::Retained,
    ];
    for pair in path.windows(2) {
        cleanup
            .transition(&execution_id, pair[0], pair[1], &handles)
            .await
            .unwrap();
    }
    execution_id
}

#[tokio::test]
async fn identity_a_worker_token_cannot_register_or_heartbeat_for_another_worker() {
    let Some((f, _lock)) = identity_fixture(true).await else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let register = |token: &str, id: &str| {
        f.client
            .post(format!("{}/workers", f.base))
            .bearer_auth(token)
            .json(&registration(id))
            .send()
    };
    let heartbeat = |token: &str, path_id: &str, body_id: &str| {
        f.client
            .post(format!("{}/workers/{path_id}/heartbeat", f.base))
            .bearer_auth(token)
            .json(&registration(body_id))
            .send()
    };

    // w1's token cannot register w2, and nothing is written for w2.
    let denied = register(&f.t1, &f.w2).await.unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["code"], "WORKER_IDENTITY_MISMATCH", "{body}");
    assert!(!body.to_string().contains(&f.t1));
    let listed: serde_json::Value = f
        .client
        .get(format!("{}/workers", f.base))
        .bearer_auth("api-secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !listed.to_string().contains(&f.w2),
        "w2 was never registered"
    );

    // Each token registers and heartbeats its own worker.
    assert_eq!(
        register(&f.t1, &f.w1).await.unwrap().status(),
        reqwest::StatusCode::CREATED
    );
    assert_eq!(
        register(&f.t2, &f.w2).await.unwrap().status(),
        reqwest::StatusCode::CREATED
    );
    assert_eq!(
        heartbeat(&f.t1, &f.w1, &f.w1).await.unwrap().status(),
        reqwest::StatusCode::OK
    );

    // A worker token on another worker's heartbeat route is refused, however the ids line up.
    for (path_id, body_id, expected) in [
        (&f.w2, &f.w2, reqwest::StatusCode::FORBIDDEN),
        (&f.w1, &f.w2, reqwest::StatusCode::FORBIDDEN),
        (&f.w2, &f.w1, reqwest::StatusCode::BAD_REQUEST),
    ] {
        let status = heartbeat(&f.t1, path_id, body_id).await.unwrap().status();
        assert_eq!(status, expected, "path {path_id} body {body_id}");
    }
    assert_eq!(
        heartbeat(&f.t1, &f.w2, &f.w2).await.unwrap().status(),
        reqwest::StatusCode::FORBIDDEN
    );

    // The shared token no longer works at all once per-worker tokens are loaded.
    assert_eq!(
        register(&f.shared, &f.w1).await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        register("not-a-token", &f.w1).await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn identity_a_worker_token_cannot_clean_up_another_workers_execution() {
    let Some((f, _lock)) = identity_fixture(true).await else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let owned_by_w2 = retained_execution_owned_by(&f.cleanup, &f.w2).await;
    let url = |id: &ExecutionId| format!("{}/executions/{id}/cleanup", f.base);

    let denied = f
        .client
        .post(url(&owned_by_w2))
        .bearer_auth(&f.t1)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["code"], "WORKER_IDENTITY_MISMATCH", "{body}");
    assert_eq!(
        f.cleanup
            .get(&owned_by_w2)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::Retained,
        "a refused request leaves the authority untouched"
    );

    // The owner's own token, and the API token, still request cleanup.
    let owned_by_w1 = retained_execution_owned_by(&f.cleanup, &f.w1).await;
    assert_eq!(
        f.client
            .post(url(&owned_by_w1))
            .bearer_auth(&f.t1)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    assert_eq!(
        f.client
            .post(url(&owned_by_w2))
            .bearer_auth("api-secret")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    assert_eq!(
        f.cleanup
            .get(&owned_by_w2)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::CleanupPending
    );
}

#[tokio::test]
async fn identity_the_legacy_shared_token_still_speaks_for_every_worker() {
    let Some((f, _lock)) = identity_fixture(false).await else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    for id in [&f.w2, &f.w1] {
        let response = f
            .client
            .post(format!("{}/workers", f.base))
            .bearer_auth(&f.shared)
            .json(&registration(id))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    }
    let owned_by_w2 = retained_execution_owned_by(&f.cleanup, &f.w2).await;
    assert_eq!(
        f.client
            .post(format!("{}/executions/{owned_by_w2}/cleanup", f.base))
            .bearer_auth(&f.shared)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    // Per-worker tokens are not in play: they do not authenticate.
    assert_eq!(
        f.client
            .post(format!("{}/workers", f.base))
            .bearer_auth(&f.t1)
            .json(&registration(&f.w1))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}
