use axum::{
    extract::{rejection::JsonRejection, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use orchestrator_core::{
    ExecutionId, WorkerAdvertisement, WorkerId, WorkerRegistration, WorkerState,
};
use orchestrator_persistence::{CleanupDisposition, StoreError};

use crate::{auth::authorize_worker, error::ApiError, state::AppState};

const WORKER_BODY_LIMIT: usize = 1_048_576;
const HEARTBEAT_DEADLINE_SECONDS: i64 = 90;

impl AppState {
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

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/workers", get(list).post(register))
        .route("/workers/{id}/heartbeat", post(heartbeat))
        .route("/executions/{id}/cleanup", post(request_cleanup))
        .layer(DefaultBodyLimit::max(WORKER_BODY_LIMIT))
}

async fn request_cleanup(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    authorize_worker(&state, &headers)?;
    let cleanup = state.cleanup_authorities.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "CLEANUP_UNAVAILABLE",
            "durable cleanup reconciliation is unavailable",
        )
    })?;
    let execution_id = ExecutionId::new(id);
    let authority = cleanup.get(&execution_id).await.map_err(ApiError::store)?;
    if authority.disposition().map_err(ApiError::store)? != CleanupDisposition::Retained {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "CLEANUP_NOT_RETAINED",
            "only a retained execution can be explicitly cleaned",
        ));
    }
    cleanup
        .transition(
            &execution_id,
            CleanupDisposition::Retained,
            CleanupDisposition::CleanupPending,
            &authority.handles,
        )
        .await
        .map_err(ApiError::store)?;
    Ok(StatusCode::ACCEPTED)
}

async fn register(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<WorkerAdvertisement>, JsonRejection>,
) -> Result<(StatusCode, Json<WorkerRegistration>), ApiError> {
    authorize_worker(&state, &headers)?;
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
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<WorkerAdvertisement>, JsonRejection>,
) -> Result<Json<WorkerRegistration>, ApiError> {
    authorize_worker(&state, &headers)?;
    let Json(advertisement) = decode_payload(payload)?;
    if advertisement.id.as_str() != id {
        return Err(ApiError::validation(
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
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<WorkerRegistration>>, ApiError> {
    authorize_worker(&state, &headers)?;
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
            ApiError::validation(rejection.body_text())
        }
    })
}
