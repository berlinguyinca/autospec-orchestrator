//! The execution record: authoritative state owned by the orchestrator
//! (spec sections 49, 61).

use crate::{error::FailureClass, ids::*, manifest::ExecutionManifest, CoreError, OwnershipLabels};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The role AutoSpec assigned to this execution. The orchestrator carries the
/// role but never chooses it (spec sections 10, 15).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    Implementation,
    Review,
    Documentation,
    UiReview,
    IntegrationTest,
    /// A human-driven Workbench session with no AutoSpec issue behind it
    /// (spec section 20).
    Interactive,
}

/// Lifecycle state of an execution. Maps onto AutoSpec's Kanban columns, but the
/// mapping itself lives in AutoSpec (spec section 30).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionState {
    Queued,
    WorkerAssigned,
    Provisioning,
    Running,
    PausedForHuman,
    ReviewReady,
    Completed,
    Failed,
    Cancelled,
}

impl ExecutionState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ExecutionState::Completed | ExecutionState::Failed | ExecutionState::Cancelled
        )
    }

    pub fn can_transition_to(self, next: ExecutionState) -> bool {
        use ExecutionState::*;
        match (self, next) {
            (a, b) if a == b => false,
            (_, Cancelled) | (_, Failed) => !self.is_terminal(),
            (Queued, WorkerAssigned) => true,
            (WorkerAssigned, Provisioning) => true,
            (Provisioning, Running) => true,
            (Running, PausedForHuman) | (PausedForHuman, Running) => true,
            (Running, ReviewReady) | (Running, Completed) => true,
            (ReviewReady, Completed) => true,
            _ => false,
        }
    }
}

/// Outcome the orchestrator hands back to AutoSpec. It contains evidence, never
/// a decision about what happens next (spec section 9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub execution_id: ExecutionId,
    pub state: ExecutionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diff_artifact: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tests: Option<TestSummary>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct TestSummary {
    pub passed: u32,
    pub failed: u32,
    #[serde(default)]
    pub skipped: u32,
}

/// Durable interactive action requested through the controller and executed by
/// the owning worker. This is execution control, not scheduling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionControlAction {
    Pause,
    Resume,
    ForkConversation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionControlRequest {
    pub request_id: i64,
    pub execution_id: ExecutionId,
    pub action: ExecutionControlAction,
    pub requested_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

/// Cursor-only attachment metadata. Conversation history and artifacts remain
/// available through their incremental/event and artifact APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAttachment {
    pub execution_id: ExecutionId,
    pub state: ExecutionState,
    pub session_id: SessionId,
    pub workspace_ref: String,
    pub event_cursor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AttachmentMode {
    ForkConversation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentRequest {
    pub mode: AttachmentMode,
}

/// The authoritative execution record (spec section 49).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: ExecutionId,
    pub role: Role,
    pub state: ExecutionState,
    pub manifest: ExecutionManifest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<WorkerId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    pub labels: OwnershipLabels,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ExecutionResult>,
}

impl Execution {
    pub fn transition(&mut self, next: ExecutionState) -> Result<(), CoreError> {
        if !self.state.can_transition_to(next) {
            return Err(CoreError::IllegalTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_transitions_are_allowed() {
        use ExecutionState::*;
        assert!(Queued.can_transition_to(WorkerAssigned));
        assert!(WorkerAssigned.can_transition_to(Provisioning));
        assert!(Provisioning.can_transition_to(Running));
        assert!(Running.can_transition_to(ReviewReady));
        assert!(ReviewReady.can_transition_to(Completed));
    }

    #[test]
    fn terminal_states_do_not_transition() {
        use ExecutionState::*;
        assert!(Completed.is_terminal());
        assert!(!Completed.can_transition_to(Running));
        assert!(!Cancelled.can_transition_to(Failed));
    }

    #[test]
    fn skipping_provisioning_is_rejected() {
        use ExecutionState::*;
        assert!(!Queued.can_transition_to(Running));
    }
}
