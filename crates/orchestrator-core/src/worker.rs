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
    /// Sanitized, operator-actionable reasons this worker cannot advertise
    /// execution capacity. Values must never contain credentials or raw paths.
    #[serde(
        rename = "healthErrors",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub health_errors: Vec<String>,
}

fn default_concurrency() -> u32 {
    2
}

impl WorkerCapabilities {
    pub fn health_errors_are_sanitized(&self) -> bool {
        self.health_errors.len() <= 2
            && self.health_errors.iter().all(|error| {
                matches!(
                    error.as_str(),
                    "storage-capability-unavailable"
                        | "docker-capability-unavailable"
                        | "worker-configuration-invalid"
                        | "worker-preparation-unavailable"
                )
            })
    }
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
            && self.capabilities.health_errors.is_empty()
            && self
                .capability_proof
                .as_ref()
                .is_some_and(WorkerCapabilityProof::is_complete)
            && self.running_executions < self.capabilities.max_concurrent_executions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitized_health_failures_prevent_ready_capacity_even_with_a_complete_proof() {
        let worker: WorkerRegistration = serde_json::from_value(serde_json::json!({
            "id": "worker-health",
            "capabilities": {
                "os": "linux",
                "arch": "x86_64",
                "cpu": 4,
                "memoryMib": 4096,
                "diskGib": 40,
                "runtimes": ["docker"],
                "capabilities": ["docker"],
                "maxConcurrentExecutions": 2,
                "healthErrors": ["storage-capability-unavailable"]
            },
            "state": "READY",
            "running_executions": 0,
            "last_heartbeat": Utc::now(),
            "capability_proof": {
                "storage_backend": "apfs",
                "storage_pool_identity": "pool",
                "docker_daemon_id": "daemon",
                "docker_verifier": "verifier",
                "docker_method_version": "v2"
            }
        }))
        .unwrap();
        assert!(worker.capabilities.health_errors_are_sanitized());
        assert!(!worker.has_capacity());
        let mut malicious = worker.capabilities.clone();
        malicious.health_errors = vec!["/secret/path?token=credential".to_owned()];
        assert!(!malicious.health_errors_are_sanitized());
    }

    #[test]
    fn every_typed_worker_preparation_failure_code_is_sanitized() {
        let capabilities = WorkerCapabilities {
            os: "linux".into(),
            arch: "x86_64".into(),
            cpu: 4,
            memory_mib: 4096,
            disk_gib: 40,
            runtimes: Vec::new(),
            capabilities: Vec::new(),
            max_concurrent_executions: 2,
            health_errors: Vec::new(),
        };
        for code in [
            "worker-configuration-invalid",
            "worker-preparation-unavailable",
            "storage-capability-unavailable",
            "docker-capability-unavailable",
        ] {
            let mut advertised = capabilities.clone();
            advertised.health_errors = vec![code.to_owned()];
            assert!(
                advertised.health_errors_are_sanitized(),
                "typed preparation failure code {code} must be accepted"
            );
        }
    }
}
