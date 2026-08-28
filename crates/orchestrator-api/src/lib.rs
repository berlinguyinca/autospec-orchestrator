//! Versioned HTTP surface of the execution plane (spec sections 21, 75).
//!
//! Every client — AutoSpec, Workbench, the CLI, CI — uses this one API. There is
//! no second execution engine anywhere in the ecosystem (spec invariant 14).

use anyhow::Result;
mod auth;
mod workers;

use axum::{routing::get, Router};
use orchestrator_core::API_VERSION;
use orchestrator_persistence::{PgReservationStore, PgWorkerStore};
use std::{env, sync::Arc, time::Duration};
use tokio::net::TcpListener;

pub use auth::{ApiTokenValidator, StaticApiTokenValidator};
pub use workers::WorkerApiState;

pub fn api_root() -> String {
    format!("/api/{API_VERSION}")
}

/// Router for the orchestrator controller.
///
/// Planned routes under `/api/v1`:
/// `POST   /executions`            create an execution
/// `GET    /executions/{id}`       read authoritative execution state
/// `POST   /executions/{id}/cancel`
/// `POST   /executions/{id}/retry`
/// `GET    /executions/{id}/events` event stream (SSE)
/// `GET    /executions/{id}/artifacts`
/// `GET    /workers`               registered workers and capacity
/// `POST   /workers`               worker registration and heartbeat
pub fn router(state: WorkerApiState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .nest(&api_root(), workers::routes().with_state(state))
}

/// Bind and serve the controller API.
pub async fn serve(addr: &str) -> Result<()> {
    let database_url = env::var("AUTOSPEC_DATABASE_URL")?;
    let token = env::var("AUTOSPEC_WORKER_TOKEN")?;
    let state = WorkerApiState::new(
        Arc::new(PgWorkerStore::connect(&database_url).await?),
        Arc::new(PgReservationStore::connect(&database_url).await?),
        token,
    );
    let reaper = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(error) = reaper.reap_stale(chrono::Utc::now()).await {
                tracing::error!(%error, "worker heartbeat reaper failed");
            }
            if let Err(error) = reaper.recover_stranded().await {
                tracing::error!(%error, "stranded worker recovery failed");
            }
        }
    });
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, api = %api_root(), "orchestrator controller listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_is_versioned_from_the_start() {
        assert_eq!(api_root(), "/api/v1");
    }
}
