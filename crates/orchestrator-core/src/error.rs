//! Execution failure classification (spec sections 41, 94, 95).

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// How an execution failed. AutoSpec uses this to decide retry policy; the
/// orchestrator never decides retry policy itself (spec section 40).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureClass {
    /// The requested environment could not be provisioned.
    EnvironmentFailed,
    /// The agent harness (e.g. Pi) crashed or exited abnormally.
    HarnessFailed,
    /// InferWeave reported a model or backend failure.
    ModelFailed,
    /// The execution exceeded its wall-clock budget.
    Timeout,
    /// No harness events were observed within the inactivity window.
    Inactivity,
    /// The execution violated a CPU, memory, disk, or PID limit.
    ResourceViolation,
    /// The assigned worker became unreachable.
    WorkerLost,
    /// The execution was cancelled by a client.
    Cancelled,
    /// The task itself failed (for example, tests did not pass).
    TaskFailed,
    /// An unclassified internal error.
    Internal,
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid execution manifest: {0}")]
    InvalidManifest(String),

    #[error("unknown execution: {0}")]
    UnknownExecution(String),

    #[error("illegal state transition: {from:?} -> {to:?}")]
    IllegalTransition {
        from: crate::ExecutionState,
        to: crate::ExecutionState,
    },
}
