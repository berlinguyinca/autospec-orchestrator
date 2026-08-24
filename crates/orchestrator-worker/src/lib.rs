//! The worker daemon's execution loop (spec sections 33, 62, 70).
//!
//! A worker provisions isolated environments, runs an agent harness inside them,
//! reports events upstream, and cleans up exactly what it created. It makes no
//! project-level decisions.

use anyhow::Result;
use git_worktree::WorktreeManager;
use harness_traits::AgentHarness;
use orchestrator_core::{Execution, ExecutionResult};
use runtime_traits::Runtime;
use std::sync::Arc;

pub struct Worker {
    pub runtime: Arc<dyn Runtime>,
    pub harness: Arc<dyn AgentHarness>,
    pub worktrees: Arc<dyn WorktreeManager>,
}

impl Worker {
    pub fn new(
        runtime: Arc<dyn Runtime>,
        harness: Arc<dyn AgentHarness>,
        worktrees: Arc<dyn WorktreeManager>,
    ) -> Self {
        Self {
            runtime,
            harness,
            worktrees,
        }
    }

    /// Run one execution to a terminal state. A failure here must never
    /// destabilise another execution on the same worker (spec invariant 13).
    pub async fn run(&self, _execution: &Execution) -> Result<ExecutionResult> {
        todo!("worktree -> environment -> harness -> evidence -> cleanup")
    }
}
