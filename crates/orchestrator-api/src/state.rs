use crate::{
    auth::{PerWorkerTokens, SharedWorkerToken, WorkerTokenValidator},
    ApiTokenValidator, StaticApiTokenValidator,
};
use orchestrator_core::ExecutionEvent;
use orchestrator_persistence::{
    ArtifactStore, CleanupAuthorityStore, EventLog, ExecutionStore, ReservationStore, WorkerStore,
};
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct AppState {
    pub executions: Arc<dyn ExecutionStore>,
    pub events: Arc<dyn EventLog>,
    pub workers: Arc<dyn WorkerStore>,
    pub event_tx: broadcast::Sender<ExecutionEvent>,
    pub(crate) reservations: Arc<dyn ReservationStore>,
    pub(crate) artifacts: Arc<dyn ArtifactStore>,
    pub(crate) api_token_validator: Arc<dyn ApiTokenValidator>,
    pub(crate) worker_token_validator: Arc<dyn WorkerTokenValidator>,
    pub(crate) cleanup_authorities: Option<Arc<dyn CleanupAuthorityStore>>,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        executions: Arc<dyn ExecutionStore>,
        events: Arc<dyn EventLog>,
        workers: Arc<dyn WorkerStore>,
        reservations: Arc<dyn ReservationStore>,
        artifacts: Arc<dyn ArtifactStore>,
        api_token: String,
        worker_token: String,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            executions,
            events,
            workers,
            event_tx,
            reservations,
            artifacts,
            api_token_validator: Arc::new(StaticApiTokenValidator::new(api_token)),
            worker_token_validator: Arc::new(SharedWorkerToken::new(worker_token)),
            cleanup_authorities: None,
        }
    }

    /// Replaces the shared worker token with per-worker identities.
    ///
    /// The shared token stops working: it is not kept as a fallback, so a
    /// leaked per-worker token speaks for one worker only.
    #[must_use]
    pub fn with_worker_tokens(mut self, tokens: PerWorkerTokens) -> Self {
        self.worker_token_validator = Arc::new(tokens);
        self
    }

    pub fn with_cleanup_authorities(
        mut self,
        cleanup_authorities: Arc<dyn CleanupAuthorityStore>,
    ) -> Self {
        self.cleanup_authorities = Some(cleanup_authorities);
        self
    }
}
