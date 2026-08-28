use orchestrator_core::{ExecutionState, WorkerId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("record not found: {0}")]
    NotFound(String),
    #[error("illegal state transition: {from:?} -> {to:?}")]
    IllegalTransition {
        from: ExecutionState,
        to: ExecutionState,
    },
    #[error("storage conflict: {0}")]
    Conflict(String),
    #[error("event sequence conflict")]
    SequenceConflict,
    #[error("worker capacity exhausted: {0}")]
    CapacityExhausted(WorkerId),
    #[error("invalid artifact name: {0}")]
    InvalidArtifactName(String),
}
