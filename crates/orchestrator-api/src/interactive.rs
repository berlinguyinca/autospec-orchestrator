use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use orchestrator_core::{
    AttachmentMode, AttachmentRequest, ExecutionAttachment, ExecutionControlAction,
    ExecutionControlRequest, ExecutionId,
};

use crate::{auth::authorize_api, error::ApiError, state::AppState};

const BODY_LIMIT: usize = 1_048_576;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/executions/{id}/pause", post(pause))
        .route("/executions/{id}/resume", post(resume))
        .route("/executions/{id}/attach", get(attach).post(fork))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
}

async fn pause(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<ExecutionControlRequest>), ApiError> {
    request_control(state, id, headers, ExecutionControlAction::Pause).await
}

async fn resume(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<ExecutionControlRequest>), ApiError> {
    request_control(state, id, headers, ExecutionControlAction::Resume).await
}

async fn request_control(
    state: AppState,
    id: String,
    headers: HeaderMap,
    action: ExecutionControlAction,
) -> Result<(StatusCode, Json<ExecutionControlRequest>), ApiError> {
    authorize_api(&state, &headers)?;
    let key = idempotency_key(&headers)?;
    let outcome = state
        .executions
        .request_control(&ExecutionId::new(&id), action, key)
        .await
        .map_err(|error| ApiError::store_for(error, &id))?;
    Ok((
        if outcome.created {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        },
        Json(outcome.request),
    ))
}

async fn attach(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ExecutionAttachment>, ApiError> {
    authorize_api(&state, &headers)?;
    let id = ExecutionId::new(id);
    let execution = state
        .executions
        .get(&id)
        .await
        .map_err(|error| ApiError::store_for(error, id.as_str()))?;
    let session_id = execution
        .session_id
        .ok_or_else(|| ApiError::conflict("execution has no attachable Pi session"))?;
    let worktree_path = execution
        .worktree_path
        .ok_or_else(|| ApiError::conflict("execution has no attachable workspace"))?;
    let event_cursor = state
        .events
        .latest_sequence(&id)
        .await
        .map_err(ApiError::store)?;
    Ok(Json(ExecutionAttachment {
        execution_id: id,
        state: execution.state,
        session_id,
        worktree_path,
        event_cursor,
    }))
}

async fn fork(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<AttachmentRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<ExecutionControlRequest>), ApiError> {
    authorize_api(&state, &headers)?;
    let Json(request) = payload.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "PAYLOAD_TOO_LARGE",
                rejection.body_text(),
            )
        } else {
            ApiError::validation(rejection.body_text())
        }
    })?;
    if request.mode != AttachmentMode::ForkConversation {
        return Err(ApiError::validation("unsupported attachment mode"));
    }
    request_control(state, id, headers, ExecutionControlAction::ForkConversation).await
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, ApiError> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 255)
        .ok_or_else(|| ApiError::validation("Idempotency-Key must be 1-255 bytes"))
}
