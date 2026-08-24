//! Pi agent harness (spec section 22).

use async_trait::async_trait;
use harness_traits::{AgentHarness, HarnessError, SessionRef};
use orchestrator_core::{ExecutionEvent, TaskPacket};

#[derive(Debug, Default, Clone)]
pub struct PiHarness;

impl PiHarness {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AgentHarness for PiHarness {
    fn name(&self) -> &'static str {
        "pi"
    }

    async fn start(&self, _packet: &TaskPacket) -> Result<SessionRef, HarnessError> {
        todo!("start Pi against the mounted worktree and task packet")
    }

    async fn resume(&self, _session: &SessionRef) -> Result<(), HarnessError> {
        todo!("resume from the persisted Pi JSONL session")
    }

    async fn fork_conversation(&self, _session: &SessionRef) -> Result<SessionRef, HarnessError> {
        todo!("branch the Pi conversation without a new worktree")
    }

    async fn poll_events(
        &self,
        _session: &SessionRef,
    ) -> Result<Vec<ExecutionEvent>, HarnessError> {
        todo!("translate Pi events into execution events")
    }

    async fn stop(&self, _session: &SessionRef) -> Result<(), HarnessError> {
        todo!("stop Pi, leaving the session durable")
    }
}
