//! Versioned HTTP surface of the execution plane (spec sections 21, 75).
//!
//! Every client — AutoSpec, Workbench, the CLI, CI — uses this one API. There is
//! no second execution engine anywhere in the ecosystem (spec invariant 14).

use anyhow::Result;
mod artifacts;
mod auth;
mod error;
mod events;
mod executions;
mod interactive;
mod operations;
mod state;
mod workers;

use axum::{routing::get, Router};
use orchestrator_core::API_VERSION;
use orchestrator_persistence::{
    PgArtifactStore, PgCleanupAuthorityStore, PgEventLog, PgExecutionStore, PgReservationStore,
    PgWorkerStore,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::net::TcpListener;

pub use auth::{ApiTokenValidator, StaticApiTokenValidator};
pub use state::AppState;

pub fn api_root() -> String {
    format!("/api/{API_VERSION}")
}

pub struct ServerConfig {
    pub addr: String,
    pub database_url: String,
    pub state_root: PathBuf,
    pub api_token: String,
    pub worker_token: String,
}

impl ServerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.api_token.is_empty() {
            anyhow::bail!("API bearer token must not be empty");
        }
        if self.worker_token.is_empty() {
            anyhow::bail!("worker bearer token must not be empty");
        }
        Ok(())
    }
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
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .nest(
            &api_root(),
            Router::new()
                .merge(executions::routes())
                .merge(interactive::routes())
                .merge(events::routes())
                .merge(artifacts::routes())
                .merge(operations::routes())
                .merge(workers::routes()),
        )
        .with_state(state)
}

/// Bind and serve the controller API.
pub async fn serve(config: ServerConfig) -> Result<()> {
    config.validate()?;
    let state = AppState::new(
        Arc::new(PgExecutionStore::connect(&config.database_url).await?),
        Arc::new(PgEventLog::connect(&config.database_url).await?),
        Arc::new(PgWorkerStore::connect(&config.database_url).await?),
        Arc::new(PgReservationStore::connect(&config.database_url).await?),
        Arc::new(PgArtifactStore::connect(&config.database_url, config.state_root).await?),
        config.api_token,
        config.worker_token,
    )
    .with_cleanup_authorities(Arc::new(
        PgCleanupAuthorityStore::connect(&config.database_url).await?,
    ));
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
    let listener = TcpListener::bind(&config.addr).await?;
    tracing::info!(addr = %config.addr, api = %api_root(), "orchestrator controller listening");
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

    #[test]
    fn server_config_rejects_empty_api_and_worker_tokens() {
        let config = |api_token: &str, worker_token: &str| ServerConfig {
            addr: "127.0.0.1:0".into(),
            database_url: "postgres://unused".into(),
            state_root: "/tmp/unused".into(),
            api_token: api_token.into(),
            worker_token: worker_token.into(),
        };
        assert!(config("", "worker").validate().is_err());
        assert!(config("api", "").validate().is_err());
        assert!(config("api", "worker").validate().is_ok());
    }
}
