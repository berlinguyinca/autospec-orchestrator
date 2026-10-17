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
    ExecutionPaused,
    ExecutionResumed,
    ConversationForked {
        session_id: SessionId,
    },
    /// A periodic watchdog sample for a running execution (spec section 95).
    /// Optional fields are absent when the probe could not measure them; an
    /// unknown value is omitted, never a zero or empty placeholder.
    WatchdogSample {
        verdict: WatchdogVerdict,
        #[serde(skip_serializing_if = "Option::is_none")]
        current_command: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_progress_at: Option<DateTime<Utc>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cpu_percent: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        rss_bytes: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        io_bytes: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        disk_bytes: Option<u64>,
    },
}

/// Watchdog verdict for a running execution (spec section 95).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogVerdict {
    Healthy,
    QuietHealthy,
    Stalled,
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
