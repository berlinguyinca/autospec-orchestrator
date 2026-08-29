use chrono::Utc;
use orchestrator_api::{router, AppState};
use orchestrator_core::{
    event::ExecutionEventKind, ExecutionEvent, ExecutionId, ExecutionManifest, ExecutionState,
};
use orchestrator_persistence::{
    ArtifactStore, EventLog, ExecutionStore, PgArtifactStore, PgEventLog, PgExecutionStore,
    PgReservationStore, PgWorkerStore,
};
use std::sync::Arc;
use tokio::net::TcpListener;

struct TestApi {
    base: String,
    token: String,
    events: Arc<PgEventLog>,
    executions: Arc<PgExecutionStore>,
    artifacts: Arc<PgArtifactStore>,
    event_tx: tokio::sync::broadcast::Sender<ExecutionEvent>,
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
        executions.clone(),
        events.clone(),
        workers,
        reservations,
        artifacts.clone(),
        token.clone(),
        "worker-secret".into(),
    );
    let event_tx = state.event_tx.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    Some(TestApi {
        base: format!("http://{address}/api/v1"),
        token,
        events,
        executions,
        artifacts,
        event_tx,
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
        "runtime": {"type": "docker", "cpu": 2, "memoryMib": 4096, "diskGib": 20,
                    "capabilities": ["api-contract-only"]},
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
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
}

#[tokio::test]
async fn operator_execution_and_queue_routes_are_authenticated_bounded_and_metadata_only() {
    let Some(api) = test_api().await else { return };
    let client = reqwest::Client::new();
    let created = client
        .post(format!("{}/executions", api.base))
        .bearer_auth(&api.token)
        .header(
            "Idempotency-Key",
            format!("operator-{}", uuid::Uuid::new_v4()),
        )
        .json(&manifest())
        .send()
        .await
        .unwrap()
        .json::<orchestrator_core::Execution>()
        .await
        .unwrap();
    let executions = format!("{}/operator/executions?limit=1", api.base);
    assert_eq!(
        client.get(&executions).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let listed = client
        .get(&executions)
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), reqwest::StatusCode::OK);
    let listed = listed.json::<serde_json::Value>().await.unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert!(listed[0].get("manifest").is_none());
    assert!(listed[0].get("task_packet").is_none());
    assert!(!listed.to_string().contains("api-contract-only"));
    assert_eq!(
        client
            .get(format!("{}/operator/executions?limit=101", api.base))
            .bearer_auth(&api.token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let queue = client
        .get(format!("{}/operator/queue", api.base))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(queue.status(), reqwest::StatusCode::OK);
    let queue = queue.json::<serde_json::Value>().await.unwrap();
    assert!(queue["queued"].as_u64().unwrap() >= 1);
    assert!(queue["total"].as_u64().unwrap() >= 1);
    cancel_execution(&client, &api, &created.id).await;
}

#[tokio::test]
async fn interactive_routes_are_authenticated_idempotent_bounded_and_metadata_only() {
    let Some(api) = test_api().await else { return };
    let client = reqwest::Client::new();
    let mut execution: orchestrator_core::Execution = serde_json::from_value(serde_json::json!({
        "id": format!("interactive-{}", uuid::Uuid::new_v4().simple()),
        "role": "interactive",
        "state": "RUNNING",
        "manifest": manifest(),
        "worker_id": "worker-interactive",
        "attempt_id": "attempt-interactive",
        "session_id": "session-interactive",
        "worktree_path": "/bounded/execution/repository",
        "labels": {
            "execution_id": "placeholder",
            "worker_id": "worker-interactive",
            "repository": "owner/repository"
        },
        "created_at": Utc::now(),
        "updated_at": Utc::now()
    }))
    .unwrap();
    execution.labels.execution_id = execution.id.clone();
    execution.manifest.role = orchestrator_core::Role::Interactive;
    api.executions.insert(&execution).await.unwrap();
    let base = format!("{}/executions/{}", api.base, execution.id);

    assert_eq!(
        client
            .post(format!("{base}/pause"))
            .header("Idempotency-Key", "pause-1")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let pause = client
        .post(format!("{base}/pause"))
        .bearer_auth(&api.token)
        .header("Idempotency-Key", "pause-1")
        .send()
        .await
        .unwrap();
    assert_eq!(pause.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        pause.json::<serde_json::Value>().await.unwrap()["action"],
        "pause"
    );
    let replay = client
        .post(format!("{base}/pause"))
        .bearer_auth(&api.token)
        .header("Idempotency-Key", "pause-1")
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), reqwest::StatusCode::OK);

    let attach = client
        .get(format!("{base}/attach"))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(attach.status(), reqwest::StatusCode::OK);
    let attach = attach.json::<serde_json::Value>().await.unwrap();
    assert_eq!(attach["session_id"], "session-interactive");
    assert_eq!(
        attach["workspace_ref"],
        format!("execution:{}:workspace", execution.id)
    );
    assert!(attach.get("worktree_path").is_none());
    assert!(attach.get("event_cursor").is_some());
    assert!(attach.get("events").is_none());
    assert!(attach.get("artifacts").is_none());
    assert!(attach.get("prompt").is_none());

    let fork = client
        .post(format!("{base}/attach"))
        .bearer_auth(&api.token)
        .header("Idempotency-Key", "fork-1")
        .json(&serde_json::json!({"mode": "fork-conversation"}))
        .send()
        .await
        .unwrap();
    assert_eq!(fork.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        fork.json::<serde_json::Value>().await.unwrap()["action"],
        "fork-conversation"
    );

    let resume = client
        .post(format!("{base}/resume"))
        .bearer_auth(&api.token)
        .header("Idempotency-Key", "resume-1")
        .send()
        .await
        .unwrap();
    assert_eq!(resume.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(
        resume.json::<serde_json::Value>().await.unwrap()["action"],
        "resume"
    );
    assert_eq!(
        client
            .post(format!("{base}/resume"))
            .bearer_auth(&api.token)
            .header("Idempotency-Key", "resume-conflict")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );

    assert_eq!(
        client
            .post(format!("{base}/attach"))
            .bearer_auth(&api.token)
            .header("Idempotency-Key", "too-large")
            .header("content-type", "application/json")
            .body(vec![b'x'; 1_048_577])
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );

    cancel_execution(&client, &api, &execution.id).await;
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
    assert_eq!(
        client
            .post(&url)
            .header("Authorization", "Basic api-token")
            .json(&manifest())
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
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
async fn concurrent_create_and_retry_requests_preserve_idempotency_and_id_allocation() {
    let Some(api) = test_api().await else { return };
    let client = reqwest::Client::new();
    let url = format!("{}/executions", api.base);
    let shared_key = format!("concurrent-create-{}", uuid::Uuid::new_v4());
    let create = |key: String| {
        let client = client.clone();
        let url = url.clone();
        let token = api.token.clone();
        async move {
            client
                .post(url)
                .bearer_auth(token)
                .header("Idempotency-Key", key)
                .json(&manifest())
                .send()
                .await
                .unwrap()
        }
    };
    let (left, right) = tokio::join!(create(shared_key.clone()), create(shared_key));
    assert_eq!(
        [left.status(), right.status()]
            .into_iter()
            .filter(|status| *status == reqwest::StatusCode::CREATED)
            .count(),
        1
    );
    let left: orchestrator_core::Execution = left.json().await.unwrap();
    let right: orchestrator_core::Execution = right.json().await.unwrap();
    assert_eq!(left.id, right.id);
    let conflicting_key = format!("concurrent-conflict-{}", uuid::Uuid::new_v4());
    let mut changed = manifest();
    changed.repository.base_ref = "release".into();
    let create_manifest = |request: ExecutionManifest| {
        let client = client.clone();
        let url = url.clone();
        let token = api.token.clone();
        let key = conflicting_key.clone();
        async move {
            client
                .post(url)
                .bearer_auth(token)
                .header("Idempotency-Key", key)
                .json(&request)
                .send()
                .await
                .unwrap()
        }
    };
    let (original, changed) = tokio::join!(create_manifest(manifest()), create_manifest(changed));
    let statuses = [original.status(), changed.status()];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::CREATED)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == reqwest::StatusCode::CONFLICT)
            .count(),
        1
    );

    let (first_collision, second_collision) = tokio::join!(
        create(format!("collision-a-{}", uuid::Uuid::new_v4())),
        create(format!("collision-b-{}", uuid::Uuid::new_v4()))
    );
    assert_eq!(first_collision.status(), reqwest::StatusCode::CREATED);
    assert_eq!(second_collision.status(), reqwest::StatusCode::CREATED);
    let first_collision: orchestrator_core::Execution = first_collision.json().await.unwrap();
    let second_collision: orchestrator_core::Execution = second_collision.json().await.unwrap();
    assert_ne!(first_collision.id, second_collision.id);

    let mut source = left.clone();
    source.id = ExecutionId::new(format!("retry-source-{}", uuid::Uuid::new_v4().simple()));
    source.labels.execution_id = source.id.clone();
    source.state = ExecutionState::Cancelled;
    api.executions.insert(&source).await.unwrap();
    let retry_url = format!("{}/executions/{}/retry", api.base, source.id);
    let retry_key = format!("concurrent-retry-{}", uuid::Uuid::new_v4());
    let retry = || {
        let client = client.clone();
        let retry_url = retry_url.clone();
        let token = api.token.clone();
        let retry_key = retry_key.clone();
        async move {
            client
                .post(retry_url)
                .bearer_auth(token)
                .header("Idempotency-Key", retry_key)
                .send()
                .await
                .unwrap()
        }
    };
    let (left_retry, right_retry) = tokio::join!(retry(), retry());
    assert_eq!(
        [left_retry.status(), right_retry.status()]
            .into_iter()
            .filter(|status| *status == reqwest::StatusCode::CREATED)
            .count(),
        1
    );
    let left_retry: orchestrator_core::Execution = left_retry.json().await.unwrap();
    let right_retry: orchestrator_core::Execution = right_retry.json().await.unwrap();
    assert_eq!(left_retry.id, right_retry.id);
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
    assert_eq!(cancelled.status(), reqwest::StatusCode::ACCEPTED);
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
    assert!(!api
        .executions
        .cancellation_requested(&create.id)
        .await
        .unwrap());
    assert_eq!(
        client
            .post(&cancel_url)
            .bearer_auth(&api.token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    assert_eq!(
        client
            .post(format!("{}/executions/{}/retry", api.base, create.id))
            .bearer_auth(&api.token)
            .header("Idempotency-Key", format!("early-{}", uuid::Uuid::new_v4()))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CREATED
    );
    let mut cancelled_source = create.clone();
    cancelled_source.id = ExecutionId::new(format!(
        "cancelled-source-{}",
        uuid::Uuid::new_v4().simple()
    ));
    cancelled_source.labels.execution_id = cancelled_source.id.clone();
    cancelled_source.state = ExecutionState::Cancelled;
    api.executions.insert(&cancelled_source).await.unwrap();
    let retry = client
        .post(format!(
            "{}/executions/{}/retry",
            api.base, cancelled_source.id
        ))
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

    let malformed = client
        .get(format!(
            "{}/executions/{}/events?cursor=not-a-number",
            api.base, created.id
        ))
        .bearer_auth(&api.token)
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        malformed.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({
            "error": {
                "code": "VALIDATION",
                "message": "cursor must be an unsigned integer"
            }
        })
    );

    let mut live_response = client
        .get(format!("{}/executions/{}/events", api.base, created.id))
        .bearer_auth(&api.token)
        .header("Last-Event-ID", "2")
        .send()
        .await
        .unwrap();
    let second_sequence = api
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
    assert_eq!(second_sequence, 3);
    let third_sequence = api
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
    assert_eq!(third_sequence, 4);
    let wake_sender = api.event_tx.clone();
    let wake_flood = tokio::spawn(async move {
        for sequence in 1..=200 {
            let _ = wake_sender.send(ExecutionEvent {
                execution_id: ExecutionId::new("shared-wake-from-another-execution"),
                attempt_id: None,
                sequence,
                at: Utc::now(),
                state: ExecutionState::Cancelled,
                kind: ExecutionEventKind::ExecutionCancelled,
            });
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    });
    let live_chunk = tokio::time::timeout(std::time::Duration::from_secs(3), live_response.chunk())
        .await
        .expect("durably appended worker events must reach an existing SSE stream")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&live_chunk).contains("id: 3"));
    let next_chunk = tokio::time::timeout(std::time::Duration::from_secs(3), live_response.chunk())
        .await
        .expect("durable sequence four must follow sequence three")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&next_chunk).contains("id: 4"));
    wake_flood.abort();

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
