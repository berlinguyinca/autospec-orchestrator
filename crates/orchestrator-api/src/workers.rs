use axum::{
    body::Body,
    extract::{rejection::JsonRejection, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use orchestrator_core::{WorkerAdvertisement, WorkerId, WorkerRegistration, WorkerState};
use orchestrator_persistence::{ReservationStore, StoreError, WorkerStore};
use serde_json::json;
use std::sync::Arc;

use crate::auth::{authorize_bearer, ApiTokenValidator, StaticApiTokenValidator};

const WORKER_BODY_LIMIT: usize = 1_048_576;
const HEARTBEAT_DEADLINE_SECONDS: i64 = 90;

#[derive(Clone)]
pub struct WorkerApiState {
    workers: Arc<dyn WorkerStore>,
    reservations: Arc<dyn ReservationStore>,
    token_validator: Arc<dyn ApiTokenValidator>,
}

impl WorkerApiState {
    pub fn new(
        workers: Arc<dyn WorkerStore>,
        reservations: Arc<dyn ReservationStore>,
        token: String,
    ) -> Self {
        Self {
            workers,
            reservations,
            token_validator: Arc::new(StaticApiTokenValidator::new(token)),
        }
    }

    pub fn with_token_validator(
        workers: Arc<dyn WorkerStore>,
        reservations: Arc<dyn ReservationStore>,
        token_validator: Arc<dyn ApiTokenValidator>,
    ) -> Self {
        Self {
            workers,
            reservations,
            token_validator,
        }
    }

    pub async fn reap_stale(&self, now: DateTime<Utc>) -> Result<Vec<WorkerId>, StoreError> {
        let stale = self
            .workers
            .mark_stale_before(now - Duration::seconds(HEARTBEAT_DEADLINE_SECONDS))
            .await?;
        for worker_id in &stale {
            self.reservations.recover_unreachable(worker_id).await?;
        }
        Ok(stale)
    }

    pub async fn recover_stranded(&self) -> Result<Vec<WorkerId>, StoreError> {
        let unreachable = self
            .workers
            .list()
            .await?
            .into_iter()
            .filter(|worker| worker.state == WorkerState::Unreachable)
            .map(|worker| worker.id)
            .collect::<Vec<_>>();
        for worker_id in &unreachable {
            self.reservations.recover_unreachable(worker_id).await?;
        }
        Ok(unreachable)
    }
}

pub fn routes() -> Router<WorkerApiState> {
    Router::new()
        .route("/workers", get(list).post(register))
        .route("/workers/{id}/heartbeat", post(heartbeat))
        .layer(DefaultBodyLimit::max(WORKER_BODY_LIMIT))
}

async fn register(
    State(state): State<WorkerApiState>,
    headers: HeaderMap,
    payload: Result<Json<WorkerAdvertisement>, JsonRejection>,
) -> Result<(StatusCode, Json<WorkerRegistration>), ApiError> {
    authorize(&state, &headers)?;
    let Json(advertisement) = decode_payload(payload)?;
    let mut worker = WorkerRegistration {
        id: advertisement.id,
        capabilities: advertisement.capabilities,
        state: WorkerState::Offline,
        running_executions: 0,
        last_heartbeat: Utc::now(),
        capability_proof: advertisement.capability_proof,
    };
    let exists = match state.workers.get(&worker.id).await {
        Ok(existing) => {
            worker.running_executions = existing.running_executions;
            true
        }
        Err(StoreError::NotFound(_)) => {
            worker.running_executions = 0;
            false
        }
        Err(error) => return Err(ApiError::store(error)),
    };
    own_liveness(&mut worker);
    state
        .workers
        .register(&worker)
        .await
        .map_err(ApiError::store)?;
    Ok((
        if exists {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(worker),
    ))
}

async fn heartbeat(
    State(state): State<WorkerApiState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<WorkerAdvertisement>, JsonRejection>,
) -> Result<Json<WorkerRegistration>, ApiError> {
    authorize(&state, &headers)?;
    let Json(advertisement) = decode_payload(payload)?;
    if advertisement.id.as_str() != id {
        return Err(ApiError::bad_request(
            "worker id does not match heartbeat path",
        ));
    }
    let existing = state
        .workers
        .get(&advertisement.id)
        .await
        .map_err(ApiError::store)?;
    let mut worker = WorkerRegistration {
        id: advertisement.id,
        capabilities: advertisement.capabilities,
        state: existing.state,
        running_executions: existing.running_executions,
        last_heartbeat: Utc::now(),
        capability_proof: advertisement.capability_proof,
    };
    own_liveness(&mut worker);
    state
        .workers
        .heartbeat(&worker)
        .await
        .map(Json)
        .map_err(ApiError::store)
}

async fn list(
    State(state): State<WorkerApiState>,
    headers: HeaderMap,
) -> Result<Json<Vec<WorkerRegistration>>, ApiError> {
    authorize(&state, &headers)?;
    state
        .workers
        .list()
        .await
        .map(Json)
        .map_err(ApiError::store)
}

fn own_liveness(worker: &mut WorkerRegistration) {
    worker.last_heartbeat = Utc::now();
    worker.state = if worker
        .capability_proof
        .as_ref()
        .is_some_and(orchestrator_core::WorkerCapabilityProof::is_complete)
    {
        WorkerState::Ready
    } else {
        WorkerState::Offline
    };
}

fn decode_payload(
    payload: Result<Json<WorkerAdvertisement>, JsonRejection>,
) -> Result<Json<WorkerAdvertisement>, ApiError> {
    payload.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "PAYLOAD_TOO_LARGE",
                rejection.body_text(),
            )
        } else {
            ApiError::bad_request(rejection.body_text())
        }
    })
}

fn authorize(state: &WorkerApiState, headers: &HeaderMap) -> Result<(), ApiError> {
    if authorize_bearer(headers, state.token_validator.as_ref()) {
        Ok(())
    } else {
        Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing or invalid worker bearer token",
        ))
    }
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "VALIDATION", message)
    }

    fn store(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(message) => Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message),
            StoreError::Conflict(message) => Self::bad_request(message),
            other => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                other.to_string(),
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        (
            self.status,
            Json(json!({"error": {"code": self.code, "message": self.message}})),
        )
            .into_response()
    }
}
