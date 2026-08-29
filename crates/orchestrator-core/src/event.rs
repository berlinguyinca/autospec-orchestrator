//! Execution events published to AutoSpec and Workbench (spec sections 31, 73).

use crate::{error::FailureClass, execution::ExecutionState, ids::*};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum ExecutionEventKind {
    ExecutionCreated,
    WorkerAssigned {
        worker_id: WorkerId,
    },
    EnvironmentReady,
    AgentStarted {
        session_id: SessionId,
    },
    TestsStarted,
    TestsFailed,
    ReviewReady,
    ExecutionRequeued {
        failure: FailureClass,
    },
    ExecutionFailed {
        failure: FailureClass,
    },
    ExecutionCompleted,
    ExecutionCancelled,
    /// Emitted when the orchestrator detects a hung agent (spec section 95).
    AgentInactive {
        seconds: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub execution_id: ExecutionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
    pub sequence: u64,
    pub at: DateTime<Utc>,
    pub state: ExecutionState,
    #[serde(flatten)]
    pub kind: ExecutionEventKind,
}
