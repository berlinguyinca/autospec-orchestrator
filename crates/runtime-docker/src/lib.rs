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
use bollard::{image::CreateImageOptions, models::ImageInspect, Docker};
use execution_storage::{AllocationReceipt, ReadyAllocationVerifier};
use futures_util::StreamExt;
use orchestrator_core::{ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, Runtime, RuntimeError};
use std::{env, path::PathBuf, sync::Arc};

pub use limits::{host_limits, HostConfigLimits, DEFAULT_PIDS_LIMIT};

const DEFAULT_MIN_API_VERSION: &str = "1.41";

#[derive(Debug, Clone)]
pub struct DockerRuntime {
    pub(crate) client: Docker,
    min_api_version: String,
    pub(crate) state_root: PathBuf,
    pub(crate) storage_verifier: Option<Arc<dyn ReadyAllocationVerifier>>,
    pub(crate) allocation: Option<AllocationReceipt>,
}

impl DockerRuntime {
    /// Connects using `AUTOSPEC_STATE_ROOT`, defaulting to `/var/lib/autospec`.
    pub fn connect(socket: Option<&str>) -> Result<Self, RuntimeError> {
        let state_root = env::var_os("AUTOSPEC_STATE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/var/lib/autospec"));
        Self::connect_with_state_root(socket, state_root)
    }

    /// Connects for availability/reconciliation without a storage allocation.
    ///
    /// Provisioning through this legacy constructor fails closed because it has
    /// no live Ready execution-storage capability.
    pub fn connect_with_state_root(
        socket: Option<&str>,
        state_root: impl Into<PathBuf>,
    ) -> Result<Self, RuntimeError> {
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
            state_root: state_root.into(),
            storage_verifier: None,
            allocation: None,
        })
    }

    /// Connects a runtime to one exact durable Ready execution allocation.
    ///
    /// The verifier is invoked immediately before provisioning; its receipt
    /// supplies the canonical state root and Docker bind-proof identity.
    pub fn connect_with_execution_storage(
        socket: Option<&str>,
        verifier: Arc<dyn ReadyAllocationVerifier>,
        allocation: AllocationReceipt,
    ) -> Result<Self, RuntimeError> {
        let mut runtime = Self::connect(socket)?;
        runtime.state_root = allocation
            .mount_path
            .parent()
            .and_then(|executions| executions.parent())
            .ok_or_else(|| {
                RuntimeError::ResourceLimit("allocation mount path lacks state root".to_owned())
            })?
            .to_path_buf();
        runtime.storage_verifier = Some(verifier);
        runtime.allocation = Some(allocation);
        Ok(runtime)
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

    pub fn volume_name(execution_id: &ExecutionId, purpose: &str) -> String {
        format!("autospec-{execution_id}-{purpose}")
    }

    pub fn client_api_version(&self) -> (usize, usize) {
        let version = self.client.client_version();
        (version.major_version, version.minor_version)
    }

    async fn require_compatible_daemon(&self) -> Result<(), RuntimeError> {
        let version = self
            .client
            .version()
            .await
            .map_err(|error| RuntimeError::Unavailable(error.to_string()))?;
        let client_version = format!(
            "{}.{}",
            bollard::API_DEFAULT_VERSION.major_version,
            bollard::API_DEFAULT_VERSION.minor_version
        );
        validate_daemon_api(
            version.api_version.as_deref(),
            version.min_api_version.as_deref(),
            &client_version,
            &self.min_api_version,
        )?;
        self.client
            .clone()
            .negotiate_version()
            .await
            .map_err(|error| RuntimeError::Unavailable(format!("negotiate Docker API: {error}")))?;
        Ok(())
    }

    async fn ensure_image(&self, image: &str) -> Result<ImageInspect, RuntimeError> {
        match self.client.inspect_image(image).await {
            Ok(inspect) => return Ok(inspect),
            Err(error) if is_image_not_found(&error) => {}
            Err(error) => {
                return Err(RuntimeError::Provisioning(format!(
                    "inspect image {image}: {error}"
                )))
            }
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
        })
    }
}

fn api_version_at_least(actual: &str, required: &str) -> bool {
    fn parts(version: &str) -> Option<(u32, u32)> {
        let (major, minor) = version.split_once('.')?;
        Some((major.parse().ok()?, minor.parse().ok()?))
    }
    matches!((parts(actual), parts(required)), (Some(actual), Some(required)) if actual >= required)
}

fn is_image_not_found(error: &bollard::errors::Error) -> bool {
    matches!(
        error,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

fn validate_daemon_api(
    daemon_maximum: Option<&str>,
    daemon_minimum: Option<&str>,
    client_version: &str,
    required_version: &str,
) -> Result<(), RuntimeError> {
    let daemon_maximum = daemon_maximum.ok_or_else(|| {
        RuntimeError::Unavailable("Docker daemon did not report an API version".to_owned())
    })?;
    if !api_version_at_least(daemon_maximum, required_version) {
        return Err(RuntimeError::Unavailable(format!(
            "Docker API {daemon_maximum} is below required {required_version}"
        )));
    }
    if daemon_minimum.is_some_and(|minimum| !api_version_at_least(client_version, minimum)) {
        return Err(RuntimeError::Unavailable(format!(
            "Docker daemon requires API {}, but this client supports {client_version}",
            daemon_minimum.expect("checked as some")
        )));
    }
    Ok(())
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

    #[test]
    fn only_a_404_image_inspection_error_means_pull_is_needed() {
        let missing = bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            message: "not found".to_owned(),
        };
        let forbidden = bollard::errors::Error::DockerResponseServerError {
            status_code: 403,
            message: "forbidden".to_owned(),
        };
        let transport = bollard::errors::Error::RequestTimeoutError;

        assert!(is_image_not_found(&missing));
        assert!(!is_image_not_found(&forbidden));
        assert!(!is_image_not_found(&transport));
    }

    #[test]
    fn daemon_absence_and_api_incompatibility_are_distinct_failures() {
        let absent = DockerRuntime::connect(Some("/definitely/missing/autospec-docker-test.sock"))
            .expect_err("missing socket is daemon absence")
            .to_string();
        let incompatible = validate_daemon_api(Some("1.40"), None, "1.47", "1.41")
            .expect_err("old daemon is incompatible")
            .to_string();

        assert!(absent.to_ascii_lowercase().contains("socket"));
        assert!(incompatible.contains("below required"));
        assert_ne!(absent, incompatible);
    }

    #[test]
    fn daemon_minimum_must_not_exceed_the_bollard_client_version() {
        let error = validate_daemon_api(Some("1.53"), Some("1.48"), "1.47", "1.41")
            .expect_err("daemon minimum is newer than Bollard")
            .to_string();

        assert!(error.contains("requires API 1.48"));
        assert!(error.contains("supports 1.47"));
    }
}
