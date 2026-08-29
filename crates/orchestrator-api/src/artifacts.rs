use axum::{
    extract::{Path, State},
    http::HeaderMap,
    routing::get,
    Json, Router,
};
use orchestrator_core::ExecutionId;
use orchestrator_persistence::Artifact;

use crate::{auth::authorize_api, error::ApiError, state::AppState};

pub fn routes() -> Router<AppState> {
    Router::new().route("/executions/{id}/artifacts", get(list))
}

async fn list(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Vec<Artifact>>, ApiError> {
    authorize_api(&state, &headers)?;
    state
        .artifacts
        .list(&ExecutionId::new(&id))
        .await
        .map(Json)
        .map_err(|error| ApiError::store_for(error, &id))
}
