use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use orchestrator_core::{
    event::ExecutionEventKind, Execution, ExecutionEvent, ExecutionId, ExecutionManifest,
    ExecutionState, OwnershipLabels, Role, WorkerId,
};
use orchestrator_persistence::StoreError;

use crate::{auth::authorize_api, error::ApiError, state::AppState};

const BODY_LIMIT: usize = 1_048_576;
const MAX_ID_ATTEMPTS: u32 = 10_000;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/executions", post(create))
        .route("/executions/{id}", get(read))
        .route("/executions/{id}/cancel", post(cancel))
        .route("/executions/{id}/retry", post(retry))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}

async fn create(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<ExecutionManifest>, JsonRejection>,
) -> Result<(StatusCode, Json<Execution>), ApiError> {
    authorize_api(&state, &headers)?;
    let Json(manifest) = decode_payload(payload)?;
    let key = idempotency_key(&headers)?;
    manifest
        .validate()
        .map_err(|error| ApiError::validation(error.to_string()))?;
    create_execution(&state, manifest, key, "create").await
}

async fn read(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Execution>, ApiError> {
    authorize_api(&state, &headers)?;
    state
        .executions
        .get(&ExecutionId::new(id))
        .await
        .map(Json)
        .map_err(ApiError::store)
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Execution>, ApiError> {
    authorize_api(&state, &headers)?;
    let execution_id = ExecutionId::new(id);
    let current = state
        .executions
        .get(&execution_id)
        .await
        .map_err(ApiError::store)?;
    let mut event = ExecutionEvent {
        execution_id: execution_id.clone(),
        attempt_id: current.attempt_id,
        sequence: 0,
        at: Utc::now(),
        state: ExecutionState::Cancelled,
        kind: ExecutionEventKind::ExecutionCancelled,
    };
    let (execution, sequence) = state
        .executions
        .transition_with_event(&execution_id, ExecutionState::Cancelled, &event)
        .await
        .map_err(ApiError::store)?;
    event.sequence = sequence;
    let _ = state.event_tx.send(event);
    Ok(Json(execution))
}

async fn retry(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<Execution>), ApiError> {
    authorize_api(&state, &headers)?;
    let key = idempotency_key(&headers)?;
    let source = state
        .executions
        .get(&ExecutionId::new(id))
        .await
        .map_err(ApiError::store)?;
    if !matches!(
        source.state,
        ExecutionState::Failed | ExecutionState::Cancelled
    ) {
        return Err(ApiError::conflict(
            "only failed or cancelled executions can be retried",
        ));
    }
    let scope = format!("retry:{}", source.id);
    create_execution(&state, source.manifest, key, &scope).await
}

async fn create_execution(
    state: &AppState,
    manifest: ExecutionManifest,
    idempotency_key: &str,
    request_scope: &str,
) -> Result<(StatusCode, Json<Execution>), ApiError> {
    for ordinal in 1..=MAX_ID_ATTEMPTS {
        let id = allocate_id(&manifest, ordinal)?;
        let now = Utc::now();
        let execution = Execution {
            id: id.clone(),
            role: manifest.role,
            state: ExecutionState::Queued,
            manifest: manifest.clone(),
            worker_id: None,
            attempt_id: None,
            session_id: None,
            worktree_path: None,
            labels: OwnershipLabels {
                execution_id: id.clone(),
                worker_id: WorkerId::new("unassigned"),
                repository: manifest.repository.repo.clone(),
                issue: manifest.task.as_ref().map(|task| task.issue_id.clone()),
            },
            created_at: now,
            updated_at: now,
            result: None,
        };
        let event = ExecutionEvent {
            execution_id: id,
            attempt_id: None,
            sequence: 0,
            at: now,
            state: ExecutionState::Queued,
            kind: ExecutionEventKind::ExecutionCreated,
        };
        match state
            .executions
            .create_idempotent(&execution, &event, idempotency_key, request_scope)
            .await
        {
            Ok(outcome) => {
                if let Some(sequence) = outcome.event_sequence {
                    let mut persisted = event;
                    persisted.sequence = sequence;
                    let _ = state.event_tx.send(persisted);
                }
                return Ok((
                    if outcome.created {
                        StatusCode::CREATED
                    } else {
                        StatusCode::OK
                    },
                    Json(outcome.execution),
                ));
            }
            Err(StoreError::DuplicateExecutionId(_)) => continue,
            Err(error) => return Err(ApiError::store(error)),
        }
    }
    Err(ApiError::conflict(
        "execution identifier space is exhausted for this task and role",
    ))
}

fn allocate_id(manifest: &ExecutionManifest, ordinal: u32) -> Result<ExecutionId, ApiError> {
    let repository = manifest
        .repository
        .repo
        .rsplit('/')
        .next()
        .map(slug)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::validation("repository cannot produce an execution id"))?;
    let issue = manifest
        .task
        .as_ref()
        .map(|task| slug(&task.issue_id))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "adhoc".to_owned());
    let role = match manifest.role {
        Role::Implementation => "impl",
        Role::Review => "review",
        Role::Documentation => "doc",
        Role::UiReview => "ui",
        Role::IntegrationTest => "itest",
        Role::Interactive => "inter",
    };
    let suffix = format!("-{issue}-{role}-{ordinal:02}");
    let available = 63usize.saturating_sub(suffix.len());
    let repository = repository.chars().take(available).collect::<String>();
    let repository = repository.trim_end_matches('-');
    if repository.is_empty() {
        return Err(ApiError::validation("execution id prefix is too long"));
    }
    Ok(ExecutionId::new(format!("{repository}{suffix}")))
}

fn slug(value: &str) -> String {
    let mut result = String::new();
    let mut hyphen = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            result.push(character);
            hyphen = false;
        } else if !result.is_empty() && !hyphen {
            result.push('-');
            hyphen = true;
        }
    }
    result.trim_end_matches('-').to_owned()
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 255)
        .ok_or_else(|| ApiError::validation("Idempotency-Key must be 1-255 bytes"))
}

fn decode_payload(
    payload: Result<Json<ExecutionManifest>, JsonRejection>,
) -> Result<Json<ExecutionManifest>, ApiError> {
    payload.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "PAYLOAD_TOO_LARGE",
                rejection.body_text(),
            )
        } else {
            ApiError::validation(rejection.body_text())
        }
    })
}
