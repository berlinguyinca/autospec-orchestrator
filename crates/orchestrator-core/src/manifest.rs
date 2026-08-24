//! The execution manifest: what AutoSpec (or Workbench) asks the execution plane
//! to do (spec sections 5, 20, 45, 63, 75).

use crate::{task_packet::TaskPacket, Role};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionManifest {
    /// Always `autospec.dev/v1alpha1` for this revision.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskReference>,
    pub repository: RepositoryReference,
    pub agent: AgentAssignment,
    #[serde(default)]
    pub runtime: RuntimeRequirement,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ServiceRequirement>,
    #[serde(default)]
    pub persistence: PersistenceMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_packet: Option<TaskPacket>,
}

/// A reference back into AutoSpec's workflow domain. The orchestrator treats
/// these as opaque correlation keys (spec section 77).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReference {
    pub project_id: String,
    pub issue_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryReference {
    /// `owner/name`.
    pub repo: String,
    #[serde(rename = "baseRef")]
    pub base_ref: String,
    /// Resolved base commit, filled in by the orchestrator when it fetches.
    #[serde(rename = "baseSha", skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    /// Branch AutoSpec wants the work on. AutoSpec owns branch *naming*; the
    /// orchestrator owns the physical worktree (spec section 8).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HarnessKind {
    Pi,
    OpenCode,
    Codex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAssignment {
    pub harness: HarnessKind,
    #[serde(rename = "modelPolicy")]
    pub model_policy: ModelPolicy,
}

/// AutoSpec's model decision, passed through verbatim. The orchestrator does not
/// select GPUs, load models, or rewrite this policy (spec sections 12, 15).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPolicy {
    /// Always `inferweave` today.
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preferred: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alternatives: Vec<String>,
    #[serde(rename = "fallbackClass", skip_serializing_if = "Option::is_none")]
    pub fallback_class: Option<String>,
}

fn default_provider() -> String {
    "inferweave".to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeKind {
    #[default]
    Docker,
    Podman,
    Apptainer,
}

/// Capability-shaped placement requirements. Never a named physical machine
/// (spec section 45).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRequirement {
    #[serde(rename = "type", default)]
    pub kind: RuntimeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    #[serde(default = "default_cpu")]
    pub cpu: u32,
    #[serde(rename = "memoryMib", default = "default_memory")]
    pub memory_mib: u64,
    #[serde(rename = "diskGib", default = "default_disk")]
    pub disk_gib: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

fn default_cpu() -> u32 {
    2
}
fn default_memory() -> u64 {
    4096
}
fn default_disk() -> u64 {
    20
}

impl Default for RuntimeRequirement {
    fn default() -> Self {
        Self {
            kind: RuntimeKind::default(),
            image: None,
            os: None,
            cpu: default_cpu(),
            memory_mib: default_memory(),
            disk_gib: default_disk(),
            capabilities: Vec::new(),
        }
    }
}

/// A supporting service the execution needs, provisioned into the execution's
/// own isolated network (spec sections 62, 63).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRequirement {
    pub name: String,
    pub image: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceMode {
    /// Environment is torn down when the execution reaches a terminal state.
    #[default]
    Ephemeral,
    /// Session and worktree survive so a human can attach later
    /// (spec sections 38, 86).
    Resumable,
}
