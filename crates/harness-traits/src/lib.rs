//! The agent-harness abstraction (spec sections 22, 23).
//!
//! Pi is the initial and primary harness. AutoSpec and Workbench must not depend
//! on Pi-specific process semantics.

use async_trait::async_trait;
use orchestrator_core::{ExecutionEvent, ExecutionId, SessionId, TaskPacket};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("harness not installed: {0}")]
    NotInstalled(String),
    #[error("harness start failed: {0}")]
    Start(String),
    #[error("harness crashed: {0}")]
    Crashed(String),
    #[error("model backend reported failure: {0}")]
    ModelFailed(String),
    #[error("session not resumable: {0}")]
    NotResumable(String),
    #[error("invalid harness session: {0}")]
    InvalidSession(String),
    #[error("harness I/O failed: {0}")]
    Io(String),
}

/// Where a harness session's durable state lives. Sessions persist independently
/// of containers (spec invariant 8).
#[derive(Debug, Clone)]
pub struct SessionRef {
    pub id: SessionId,
    pub path: String,
    pub execution_id: ExecutionId,
    pub worktree_path: String,
}

#[async_trait]
pub trait AgentHarness: Send + Sync {
    fn name(&self) -> &'static str;

    /// Start a fresh session for the given task packet.
    async fn start(&self, packet: &TaskPacket) -> Result<SessionRef, HarnessError>;

    /// Resume a previously persisted session after a crash or restart
    /// (spec section 41).
    async fn resume(&self, session: &SessionRef) -> Result<(), HarnessError>;

    /// Fork the conversation without forking the workspace (spec section 39).
    async fn fork_conversation(&self, session: &SessionRef) -> Result<SessionRef, HarnessError>;

    /// Drain harness events for republishing as execution events.
    async fn poll_events(&self, session: &SessionRef) -> Result<Vec<ExecutionEvent>, HarnessError>;

    async fn stop(&self, session: &SessionRef) -> Result<(), HarnessError>;
}
