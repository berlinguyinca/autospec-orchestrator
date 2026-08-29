use chrono::Utc;
use orchestrator_api::{router, AppState};
use orchestrator_core::{
    event::ExecutionEventKind, ExecutionEvent, ExecutionId, ExecutionManifest, ExecutionState,
};
use orchestrator_persistence::{
    ArtifactStore, EventLog, PgArtifactStore, PgEventLog, PgExecutionStore, PgReservationStore,
    PgWorkerStore,
};
use std::sync::Arc;
use tokio::net::TcpListener;

struct TestApi {
    base: String,
    token: String,
    events: Arc<PgEventLog>,
    artifacts: Arc<PgArtifactStore>,
    _root: tempfile::TempDir,
}

async fn test_api() -> Option<TestApi> {
    let Ok(database_url) = std::env::var("AUTOSPEC_DATABASE_URL") else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real execution API test");
        return None;
    };
    let root = tempfile::tempdir().unwrap();
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let events = Arc::new(PgEventLog::connect(&database_url).await.unwrap());
    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let artifacts = Arc::new(
        PgArtifactStore::connect(&database_url, root.path())
            .await
            .unwrap(),
    );
    let token = format!("api-token-{}", uuid::Uuid::new_v4());
    let state = AppState::new(
        executions,
        events.clone(),
        workers,
        reservations,
        artifacts.clone(),
        token.clone(),
        "worker-secret".into(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    Some(TestApi {
        base: format!("http://{address}/api/v1"),
        token,
        events,
        artifacts,
        _root: root,
    })
}

fn manifest() -> ExecutionManifest {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "autospec.dev/v1alpha1",
        "role": "implementation",
        "task": {"project_id": "inferweave", "issue_id": "417"},
        "repository": {"repo": "InferWeave/inferweave-node", "baseRef": "main"},
        "agent": {"harness": "pi", "modelPolicy": {"provider": "inferweave"}},
        "runtime": {"type": "docker", "cpu": 2, "memoryMib": 4096, "diskGib": 20},
        "persistence": "resumable"
    }))
    .unwrap()
}

async fn cancel_execution(client: &reqwest::Client, api: &TestApi, execution_id: &ExecutionId) {
    let response = client
        .post(format!("{}/executions/{execution_id}/cancel", api.base))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn create_read_auth_idempotency_validation_and_body_limit_contract() {
    let Some(api) = test_api().await else { return };
    let client = reqwest::Client::new();
    let url = format!("{}/executions", api.base);
    let unauthorized = client.post(&url).json(&manifest()).send().await.unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        unauthorized.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({
            "error": {
                "code": "UNAUTHORIZED",
                "message": "missing or invalid API bearer token"
            }
        })
    );
    let key = format!("create-{}", uuid::Uuid::new_v4());
    let created = client
        .post(&url)
        .bearer_auth(&api.token)
        .header("Idempotency-Key", &key)
        .json(&manifest())
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let created: orchestrator_core::Execution = created.json().await.unwrap();
    assert_eq!(created.state, ExecutionState::Queued);
    assert!(created.id.as_str().starts_with("inferweave-node-417-impl-"));

    let replay = client
        .post(&url)
        .bearer_auth(&api.token)
        .header("Idempotency-Key", &key)
        .json(&manifest())
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    let replay: orchestrator_core::Execution = replay.json().await.unwrap();
    assert_eq!(replay.id, created.id);

    let mut changed = manifest();
    changed.repository.base_ref = "release".into();
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&api.token)
            .header("Idempotency-Key", &key)
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );

    let read = client
        .get(format!("{url}/{}", created.id))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(read.status(), reqwest::StatusCode::OK);
    assert_eq!(
        read.json::<orchestrator_core::Execution>()
            .await
            .unwrap()
            .id,
        created.id
    );

    let mut invalid = manifest();
    invalid.runtime.cpu = 0;
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&api.token)
            .header(
                "Idempotency-Key",
                format!("invalid-{}", uuid::Uuid::new_v4())
            )
            .json(&invalid)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&api.token)
            .header("content-type", "application/json")
            .body(vec![b'x'; 1_048_577])
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );
    cancel_execution(&client, &api, &created.id).await;
}

#[tokio::test]
async fn cancel_and_retry_enforce_lifecycle_without_choosing_retry_policy() {
    let Some(api) = test_api().await else { return };
    let client = reqwest::Client::new();
    let create = client
        .post(format!("{}/executions", api.base))
        .bearer_auth(&api.token)
        .header(
            "Idempotency-Key",
            format!("cancel-{}", uuid::Uuid::new_v4()),
        )
        .json(&manifest())
        .send()
        .await
        .unwrap()
        .json::<orchestrator_core::Execution>()
        .await
        .unwrap();
    let cancel_url = format!("{}/executions/{}/cancel", api.base, create.id);
    let cancelled = client
        .post(&cancel_url)
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(cancelled.status(), reqwest::StatusCode::OK);
    assert_eq!(
        cancelled
            .json::<orchestrator_core::Execution>()
            .await
            .unwrap()
            .state,
        ExecutionState::Cancelled
    );
    let events = api.events.since(&create.id, 0).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(events[1].state, ExecutionState::Cancelled);
    assert_eq!(
        client
            .post(&cancel_url)
            .bearer_auth(&api.token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let retry = client
        .post(format!("{}/executions/{}/retry", api.base, create.id))
        .bearer_auth(&api.token)
        .header("Idempotency-Key", format!("retry-{}", uuid::Uuid::new_v4()))
        .send()
        .await
        .unwrap();
    assert_eq!(retry.status(), reqwest::StatusCode::CREATED);
    let retry: orchestrator_core::Execution = retry.json().await.unwrap();
    assert_ne!(retry.id, create.id);
    assert_eq!(retry.state, ExecutionState::Queued);
    assert_eq!(
        retry.manifest.repository.repo,
        create.manifest.repository.repo
    );
    assert_eq!(
        client
            .post(format!("{}/executions/{}/retry", api.base, retry.id))
            .bearer_auth(&api.token)
            .header(
                "Idempotency-Key",
                format!("illegal-{}", uuid::Uuid::new_v4())
            )
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    cancel_execution(&client, &api, &retry.id).await;
}

#[tokio::test]
async fn sse_resumes_after_cursor_and_artifact_listing_never_returns_blob_content() {
    let Some(api) = test_api().await else { return };
    let client = reqwest::Client::new();
    let created = client
        .post(format!("{}/executions", api.base))
        .bearer_auth(&api.token)
        .header(
            "Idempotency-Key",
            format!("events-{}", uuid::Uuid::new_v4()),
        )
        .json(&manifest())
        .send()
        .await
        .unwrap()
        .json::<orchestrator_core::Execution>()
        .await
        .unwrap();
    let sequence = api
        .events
        .append(&ExecutionEvent {
            execution_id: created.id.clone(),
            attempt_id: None,
            sequence: 0,
            at: Utc::now(),
            state: ExecutionState::Cancelled,
            kind: ExecutionEventKind::ExecutionCancelled,
        })
        .await
        .unwrap();
    assert_eq!(sequence, 2);
    let mut response = client
        .get(format!("{}/executions/{}/events", api.base, created.id))
        .bearer_auth(&api.token)
        .header("Last-Event-ID", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let first = response.chunk().await.unwrap().unwrap();
    let text = String::from_utf8_lossy(&first);
    assert!(text.contains("id: 2"));
    assert!(!text.contains("id: 1"));
    let mut query_response = client
        .get(format!(
            "{}/executions/{}/events?cursor=1",
            api.base, created.id
        ))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    let query_first = query_response.chunk().await.unwrap().unwrap();
    let query_text = String::from_utf8_lossy(&query_first);
    assert!(query_text.contains("id: 2"));
    assert!(!query_text.contains("id: 1"));

    let mut live_response = client
        .get(format!("{}/executions/{}/events", api.base, created.id))
        .bearer_auth(&api.token)
        .header("Last-Event-ID", "2")
        .send()
        .await
        .unwrap();
    let live_sequence = api
        .events
        .append(&ExecutionEvent {
            execution_id: created.id.clone(),
            attempt_id: None,
            sequence: 0,
            at: Utc::now(),
            state: ExecutionState::Cancelled,
            kind: ExecutionEventKind::ExecutionCancelled,
        })
        .await
        .unwrap();
    assert_eq!(live_sequence, 3);
    let live_chunk = tokio::time::timeout(std::time::Duration::from_secs(3), live_response.chunk())
        .await
        .expect("durably appended worker events must reach an existing SSE stream")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&live_chunk).contains("id: 3"));

    let secret = b"large binary artifact that must never enter a Pi task packet";
    api.artifacts
        .store(
            &created.id,
            "evidence.bin",
            "application/octet-stream",
            secret,
        )
        .await
        .unwrap();
    let listed = client
        .get(format!("{}/executions/{}/artifacts", api.base, created.id))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), reqwest::StatusCode::OK);
    let body = listed.text().await.unwrap();
    assert!(body.contains("evidence.bin"));
    assert!(!body.contains(std::str::from_utf8(secret).unwrap()));
    let unknown = ExecutionId::new(format!("unknown-{}", uuid::Uuid::new_v4().simple()));
    assert_eq!(
        client
            .get(format!("{}/executions/{unknown}/artifacts", api.base))
            .bearer_auth(&api.token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    cancel_execution(&client, &api, &created.id).await;
}
