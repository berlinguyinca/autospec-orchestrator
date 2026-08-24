//! Versioned HTTP surface of the execution plane (spec sections 21, 75).
//!
//! Every client — AutoSpec, Workbench, the CLI, CI — uses this one API. There is
//! no second execution engine anywhere in the ecosystem (spec invariant 14).

use anyhow::Result;
use axum::{routing::get, Router};
use orchestrator_core::API_VERSION;
use tokio::net::TcpListener;

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
pub fn router() -> Router {
    Router::new().route("/healthz", get(|| async { "ok" }))
}

/// Bind and serve the controller API.
pub async fn serve(addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, api = %api_root(), "orchestrator controller listening");
    axum::serve(listener, router()).await?;
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
