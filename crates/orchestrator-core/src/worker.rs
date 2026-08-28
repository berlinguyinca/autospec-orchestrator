//! Worker registration and capability advertisement (spec sections 33, 66).
//!
//! This is deliberately distinct from InferWeave node registration. A single
//! host may run both daemons, but they register independently.

use crate::{ids::WorkerId, manifest::RuntimeKind};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapabilities {
    pub os: String,
    pub arch: String,
    pub cpu: u32,
    #[serde(rename = "memoryMib")]
    pub memory_mib: u64,
    #[serde(rename = "diskGib")]
    pub disk_gib: u64,
    #[serde(default)]
    pub runtimes: Vec<RuntimeKind>,
    /// Free-form execution capabilities, e.g. `docker`, `playwright`, `browser`.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(rename = "maxConcurrentExecutions", default = "default_concurrency")]
    pub max_concurrent_executions: u32,
}

fn default_concurrency() -> u32 {
    2
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerState {
    Ready,
    Draining,
    Unreachable,
    Offline,
}

/// Storage and Docker-bind proof required before a worker may advertise Ready.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapabilityProof {
    pub storage_backend: String,
    pub storage_pool_identity: String,
    pub docker_daemon_id: String,
    pub docker_verifier: String,
    pub docker_method_version: String,
}

impl WorkerCapabilityProof {
    pub fn is_complete(&self) -> bool {
        [
            &self.storage_backend,
            &self.storage_pool_identity,
            &self.docker_daemon_id,
            &self.docker_verifier,
            &self.docker_method_version,
        ]
        .iter()
        .all(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub id: WorkerId,
    pub capabilities: WorkerCapabilities,
    pub state: WorkerState,
    pub running_executions: u32,
    pub last_heartbeat: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_proof: Option<WorkerCapabilityProof>,
}

/// Worker-owned capability advertisement. Liveness, state, and running counts
/// are intentionally absent because the controller owns those fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerAdvertisement {
    pub id: WorkerId,
    pub capabilities: WorkerCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_proof: Option<WorkerCapabilityProof>,
}

impl WorkerRegistration {
    pub fn has_capacity(&self) -> bool {
        self.state == WorkerState::Ready
            && self
                .capability_proof
                .as_ref()
                .is_some_and(WorkerCapabilityProof::is_complete)
            && self.running_executions < self.capabilities.max_concurrent_executions
    }
}
