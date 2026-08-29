//! Podman runtime detection boundary (spec sections 62 and 65).
//!
//! Podman is intentionally not advertised for execution until its lifecycle
//! can prove the same storage, ownership-label, and cleanup guarantees as the
//! Docker adapter. Availability remains observable through conformance checks.

use async_trait::async_trait;
use orchestrator_core::{ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, Runtime, RuntimeError};
use std::{path::PathBuf, process::Command};

#[derive(Debug, Clone)]
pub struct PodmanRuntime {
    binary: PathBuf,
}

impl PodmanRuntime {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
        }
    }
}

impl Default for PodmanRuntime {
    fn default() -> Self {
        Self::new("podman")
    }
}

#[async_trait]
impl Runtime for PodmanRuntime {
    fn name(&self) -> &'static str {
        "podman"
    }

    async fn available(&self) -> bool {
        Command::new(&self.binary)
            .args(["info", "--format", "json"])
            .output()
            .is_ok_and(|output| output.status.success())
    }

    async fn provision(
        &self,
        _: &OwnershipLabels,
        _: &RuntimeRequirement,
        _: &[ServiceRequirement],
    ) -> Result<EnvironmentHandle, RuntimeError> {
        Err(RuntimeError::Unavailable("Podman lifecycle is not enabled until storage and targeted-cleanup conformance is implemented".to_owned()))
    }

    async fn destroy(&self, _: &OwnershipLabels) -> Result<(), RuntimeError> {
        Err(RuntimeError::Cleanup(
            "Podman cleanup is unavailable because Podman provisioning is disabled".to_owned(),
        ))
    }

    async fn reconcile(&self, _: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError> {
        Err(RuntimeError::Cleanup(
            "Podman reconciliation is unavailable because Podman provisioning is disabled"
                .to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_traits::{inspect_runtime, RuntimeAvailability};
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn availability_requires_a_successful_real_command() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("podman");
        std::fs::write(&binary, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let report = inspect_runtime(&PodmanRuntime::new(&binary)).await;
        assert_eq!(report.runtime, "podman");
        assert_eq!(
            report.availability,
            RuntimeAvailability::DetectedUnsupported
        );
        assert!(
            !PodmanRuntime::new(directory.path().join("missing"))
                .available()
                .await
        );
    }
}
