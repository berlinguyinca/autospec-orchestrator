use chrono::{Duration, Utc};
use orchestrator_api::{router_with_workers, WorkerApiState};
use orchestrator_core::{
    RuntimeKind, WorkerCapabilities, WorkerCapabilityProof, WorkerId, WorkerRegistration,
    WorkerState,
};
use orchestrator_persistence::{PgReservationStore, PgWorkerStore, WorkerStore};
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::test]
async fn authenticated_worker_routes_own_liveness_and_reap_after_ninety_seconds() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let store = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let token = format!("worker-token-{}", uuid::Uuid::new_v4());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let state = WorkerApiState::new(store.clone(), reservations, token.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, router_with_workers(server_state))
            .await
            .unwrap();
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

    let listed = client.get(&base).bearer_auth(&token).send().await.unwrap();
    assert_eq!(listed.status(), reqwest::StatusCode::OK);
    let listed: Vec<WorkerRegistration> = listed.json().await.unwrap();
    assert!(listed.iter().any(|worker| worker.id == advertised.id));

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
    assert_eq!(reaped, vec![advertised.id.clone()]);
    assert_eq!(
        store.get(&advertised.id).await.unwrap().state,
        WorkerState::Unreachable
    );
}

#[tokio::test]
async fn worker_routes_reject_invalid_and_oversized_bodies() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real worker API test");
        return;
    };
    let store = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let state = WorkerApiState::new(store, reservations, "secret".into());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router_with_workers(state))
            .await
            .unwrap();
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
            max_concurrent_executions: 2,
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
