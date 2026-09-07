//! Apptainer runtime detection boundary (spec section 65).
//!
//! Apptainer is a later HPC adapter. It is not advertised for execution until
//! it can prove execution storage, resource limits, and exact cleanup without
//! weakening the shared runtime contract.

use async_trait::async_trait;
use orchestrator_core::{ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, Runtime, RuntimeConformanceMetadata, RuntimeError};
use std::{path::PathBuf, process::Command};

pub const CONFORMANCE_METADATA: RuntimeConformanceMetadata =
    RuntimeConformanceMetadata::detected_unsupported("apptainer");

#[derive(Debug, Clone)]
pub struct ApptainerRuntime {
    binary: PathBuf,
}

impl ApptainerRuntime {
    pub fn new(binary: impl Into<PathBuf>) -> Self {
        Self {
            binary: binary.into(),
        }
    }
}

impl Default for ApptainerRuntime {
    fn default() -> Self {
        Self::new("apptainer")
    }
}

#[async_trait]
impl Runtime for ApptainerRuntime {
    fn name(&self) -> &'static str {
        "apptainer"
    }

    async fn available(&self) -> bool {
        Command::new(&self.binary)
            .arg("version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    async fn provision(
        &self,
        _: &OwnershipLabels,
        _: &RuntimeRequirement,
        _: &[ServiceRequirement],
    ) -> Result<EnvironmentHandle, RuntimeError> {
        Err(RuntimeError::Unavailable("Apptainer lifecycle is not enabled until storage, limits, and targeted-cleanup conformance is implemented".to_owned()))
    }

    async fn destroy(&self, _: &OwnershipLabels) -> Result<(), RuntimeError> {
        Err(RuntimeError::Cleanup(
            "Apptainer cleanup is unavailable because Apptainer provisioning is disabled"
                .to_owned(),
        ))
    }

    async fn reconcile(&self, _: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError> {
        Err(RuntimeError::Cleanup(
            "Apptainer reconciliation is unavailable because Apptainer provisioning is disabled"
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
        let binary = directory.path().join("apptainer");
        std::fs::write(&binary, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        let report = inspect_runtime(&ApptainerRuntime::new(&binary), CONFORMANCE_METADATA).await;
        assert_eq!(report.runtime, "apptainer");
        assert_eq!(
            report.availability,
            RuntimeAvailability::DetectedUnsupported
        );
        assert!(
            !ApptainerRuntime::new(directory.path().join("missing"))
                .available()
                .await
        );
    }
}
