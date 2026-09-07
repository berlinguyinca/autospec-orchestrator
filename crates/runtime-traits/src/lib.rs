//! The runtime abstraction: Docker today, Podman and Apptainer later
//! (spec sections 62, 64, 65).
//!
//! Nothing above this trait may know which runtime is in use. AutoSpec behaviour
//! must not change because of the runtime (spec section 65).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestrator_core::{
    Execution, ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement,
};
use std::path::PathBuf;
use thiserror::Error;

pub const RUNTIME_CONFORMANCE_VERSION: &str = "autospec.dev/runtime-conformance/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeAvailability {
    Available,
    DetectedUnsupported,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConformanceReport {
    pub contract: &'static str,
    pub runtime: &'static str,
    pub availability: RuntimeAvailability,
}

/// Adapter-owned eligibility metadata kept outside the frozen runtime trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConformanceMetadata {
    pub runtime: &'static str,
    pub lifecycle_conformant: bool,
}

impl RuntimeConformanceMetadata {
    pub const fn eligible(runtime: &'static str) -> Self {
        Self {
            runtime,
            lifecycle_conformant: true,
        }
    }

    pub const fn detected_unsupported(runtime: &'static str) -> Self {
        Self {
            runtime,
            lifecycle_conformant: false,
        }
    }
}

/// Runs the frozen host-independent portion of the runtime contract. Lifecycle
/// conformance remains adapter-owned because provisioning requires a real,
/// runtime-specific image and an execution-storage allocation.
pub async fn inspect_runtime(
    runtime: &dyn Runtime,
    metadata: RuntimeConformanceMetadata,
) -> RuntimeConformanceReport {
    let metadata_matches = metadata.runtime == runtime.name();
    RuntimeConformanceReport {
        contract: RUNTIME_CONFORMANCE_VERSION,
        runtime: runtime.name(),
        availability: match (
            runtime.available().await,
            metadata_matches && metadata.lifecycle_conformant,
        ) {
            (true, true) => RuntimeAvailability::Available,
            (true, false) => RuntimeAvailability::DetectedUnsupported,
            (false, _) => RuntimeAvailability::Unavailable,
        },
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime unavailable: {0}")]
    Unavailable(String),
    #[error("provisioning failed: {0}")]
    Provisioning(String),
    #[error("resource limit rejected: {0}")]
    ResourceLimit(String),
    #[error("cleanup failed: {0}")]
    Cleanup(String),
}

/// One exact daemon-verified bind attached to the agent container.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct VerifiedBindMount {
    pub source: PathBuf,
    pub target: String,
    pub writable: bool,
}

/// Immutable runtime authority required to execute an agent workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedAgentContainer {
    pub container_id: String,
    pub daemon_id: String,
    pub labels: OwnershipLabels,
    pub mounts: Vec<VerifiedBindMount>,
}

/// Handle to a provisioned, isolated execution environment.
#[derive(Debug, Clone)]
pub struct EnvironmentHandle {
    pub execution_id: ExecutionId,
    pub network: String,
    pub agent_container: String,
    pub verified_agent_container: VerifiedAgentContainer,
    pub service_containers: Vec<String>,
    pub volumes: Vec<String>,
    pub credentials_path: Option<PathBuf>,
}

/// Exact execution-scoped credential material made available to one workload.
/// The broker owns its lifetime; callers persist neither its content nor path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCredentials {
    pub path: PathBuf,
    pub expires_at: DateTime<Utc>,
}

/// Issuance boundary for short-lived execution credentials (spec section 36).
/// InferWeave policy and validation remain outside the orchestrator.
/// Repeated minting for an adopted execution must return the same unexpired
/// path and authority. A caller must reject an expired or rotated mounted
/// credential and recreate the runtime; replacing a bind-mounted inode is not
/// a valid adoption strategy.
#[async_trait]
pub trait CredentialBroker: Send + Sync {
    async fn mint(&self, execution: &Execution) -> Result<ExecutionCredentials, RuntimeError>;
    async fn revoke(&self, id: &ExecutionId) -> Result<(), RuntimeError>;
}

/// A runtime provisions one isolated environment per execution and can destroy
/// exactly the resources it owns — never more (spec sections 42, 83).
#[async_trait]
pub trait Runtime: Send + Sync {
    fn name(&self) -> &'static str;

    /// True when this runtime is usable on the current host.
    async fn available(&self) -> bool;

    async fn provision(
        &self,
        labels: &OwnershipLabels,
        runtime: &RuntimeRequirement,
        services: &[ServiceRequirement],
    ) -> Result<EnvironmentHandle, RuntimeError>;

    /// Destroy every resource carrying this execution's ownership labels.
    async fn destroy(&self, labels: &OwnershipLabels) -> Result<(), RuntimeError>;

    /// Find orphaned resources labelled `autospec.managed=true` whose execution
    /// is no longer live, so they can be reclaimed (spec section 42).
    async fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError>;
}

#[cfg(test)]
mod conformance_tests {
    use super::*;

    struct AvailableRuntime;

    #[async_trait]
    impl Runtime for AvailableRuntime {
        fn name(&self) -> &'static str {
            "fixture"
        }
        async fn available(&self) -> bool {
            true
        }
        async fn provision(
            &self,
            _: &OwnershipLabels,
            _: &RuntimeRequirement,
            _: &[ServiceRequirement],
        ) -> Result<EnvironmentHandle, RuntimeError> {
            Err(RuntimeError::Provisioning("not exercised".into()))
        }
        async fn destroy(&self, _: &OwnershipLabels) -> Result<(), RuntimeError> {
            Ok(())
        }
        async fn reconcile(&self, _: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn frozen_conformance_report_names_version_and_availability() {
        let report = inspect_runtime(
            &AvailableRuntime,
            RuntimeConformanceMetadata::eligible("fixture"),
        )
        .await;
        assert_eq!(report.contract, RUNTIME_CONFORMANCE_VERSION);
        assert_eq!(report.runtime, "fixture");
        assert_eq!(report.availability, RuntimeAvailability::Available);
    }

    #[tokio::test]
    async fn adapter_metadata_not_the_runtime_trait_controls_eligibility() {
        let report = inspect_runtime(
            &AvailableRuntime,
            RuntimeConformanceMetadata::detected_unsupported("fixture"),
        )
        .await;
        assert_eq!(
            report.availability,
            RuntimeAvailability::DetectedUnsupported
        );
    }
}
