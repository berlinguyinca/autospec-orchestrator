//! Docker runtime adapter (spec section 62, initial runtime).
//!
//! Every resource it creates carries the mandatory ownership labels, and cleanup
//! selects on those labels only. Global prune operations are forbidden
//! (spec section 42).

mod cleanup;
mod limits;
mod provision;
mod services;

use async_trait::async_trait;
use bollard::{image::CreateImageOptions, Docker};
use futures_util::StreamExt;
use orchestrator_core::{ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, Runtime, RuntimeError};
use std::env;

pub use limits::{host_limits, HostConfigLimits, DEFAULT_PIDS_LIMIT};

const DEFAULT_MIN_API_VERSION: &str = "1.41";

#[derive(Debug, Clone)]
pub struct DockerRuntime {
    pub(crate) client: Docker,
    min_api_version: String,
}

impl DockerRuntime {
    pub fn connect(socket: Option<&str>) -> Result<Self, RuntimeError> {
        let configured_socket = socket
            .map(ToOwned::to_owned)
            .or_else(|| env::var("AUTOSPEC_DOCKER_SOCKET").ok());
        let client = match configured_socket {
            Some(socket) => Docker::connect_with_socket(&socket, 120, bollard::API_DEFAULT_VERSION),
            None => Docker::connect_with_local_defaults(),
        }
        .map_err(|error| RuntimeError::Unavailable(error.to_string()))?;
        Ok(Self {
            client,
            min_api_version: DEFAULT_MIN_API_VERSION.to_owned(),
        })
    }

    pub fn new() -> Result<Self, RuntimeError> {
        Self::connect(None)
    }

    /// Network name for an execution's isolated environment.
    pub fn network_name(execution_id: &ExecutionId) -> String {
        format!("autospec-{execution_id}")
    }

    pub fn agent_container_name(execution_id: &ExecutionId) -> String {
        format!("autospec-{execution_id}-agent")
    }

    pub fn service_container_name(execution_id: &ExecutionId, service: &str) -> String {
        format!("autospec-{execution_id}-{service}")
    }

    async fn require_compatible_daemon(&self) -> Result<(), RuntimeError> {
        let version = self
            .client
            .version()
            .await
            .map_err(|error| RuntimeError::Unavailable(error.to_string()))?;
        let api_version = version.api_version.ok_or_else(|| {
            RuntimeError::Unavailable("Docker daemon did not report an API version".to_owned())
        })?;
        if api_version_at_least(&api_version, &self.min_api_version) {
            Ok(())
        } else {
            Err(RuntimeError::Unavailable(format!(
                "Docker API {api_version} is below required {}",
                self.min_api_version
            )))
        }
    }

    async fn ensure_image(&self, image: &str) -> Result<(), RuntimeError> {
        if self.client.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        let mut pull = self.client.create_image(
            Some(CreateImageOptions {
                from_image: image,
                ..Default::default()
            }),
            None,
            None,
        );
        while let Some(progress) = pull.next().await {
            progress.map_err(|error| {
                RuntimeError::Provisioning(format!("pull image {image}: {error}"))
            })?;
        }
        self.client.inspect_image(image).await.map_err(|error| {
            RuntimeError::Provisioning(format!("image {image} unavailable after pull: {error}"))
        })?;
        Ok(())
    }
}

fn api_version_at_least(actual: &str, required: &str) -> bool {
    fn parts(version: &str) -> Option<(u32, u32)> {
        let (major, minor) = version.split_once('.')?;
        Some((major.parse().ok()?, minor.parse().ok()?))
    }
    matches!((parts(actual), parts(required)), (Some(actual), Some(required)) if actual >= required)
}

#[async_trait]
impl Runtime for DockerRuntime {
    fn name(&self) -> &'static str {
        "docker"
    }

    async fn available(&self) -> bool {
        self.require_compatible_daemon().await.is_ok()
    }

    async fn provision(
        &self,
        labels: &OwnershipLabels,
        runtime: &RuntimeRequirement,
        services: &[ServiceRequirement],
    ) -> Result<EnvironmentHandle, RuntimeError> {
        provision::provision(self, labels, runtime, services).await
    }

    async fn destroy(&self, labels: &OwnershipLabels) -> Result<(), RuntimeError> {
        cleanup::destroy(self, labels).await
    }

    async fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError> {
        cleanup::reconcile(self, live).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_name_is_execution_scoped() {
        assert_eq!(
            DockerRuntime::network_name(&ExecutionId::new("node-417-impl-01")),
            "autospec-node-417-impl-01"
        );
    }

    #[test]
    fn api_versions_are_compared_numerically() {
        assert!(api_version_at_least("1.41", "1.41"));
        assert!(api_version_at_least("1.47", "1.41"));
        assert!(!api_version_at_least("1.40", "1.41"));
        assert!(!api_version_at_least("invalid", "1.41"));
    }
}
