//! The task packet: the contract between AutoSpec planning and agent execution
//! (spec sections 78, 79).
//!
//! The orchestrator delivers this to the harness. It never interprets the
//! contents.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPacket {
    pub goal: String,
    #[serde(default, rename = "acceptanceCriteria")]
    pub acceptance_criteria: Vec<String>,
    #[serde(default, rename = "nonGoals")]
    pub non_goals: Vec<String>,
    #[serde(default, rename = "relevantContext")]
    pub relevant_context: Vec<String>,
    #[serde(default, rename = "requiredTests")]
    pub required_tests: Vec<String>,
    /// Name of the role skill the harness should load alongside the repository
    /// `AGENTS.md` (spec section 79).
    #[serde(skip_serializing_if = "Option::is_none", rename = "roleSkill")]
    pub role_skill: Option<String>,
}
