//! Docker runtime adapter (spec section 62, initial runtime).
//!
//! Every resource it creates carries the mandatory ownership labels, and cleanup
//! selects on those labels only. Global prune operations are forbidden
//! (spec section 42).

mod cleanup;
mod credentials;
mod limits;
mod provision;
mod services;

use async_trait::async_trait;
use bollard::Docker;
use execution_storage::{AllocationReceipt, ExecutionLayout, ReadyAllocationVerifier};
use orchestrator_core::{ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{
    EnvironmentHandle, ExecutionCredentials, Runtime, RuntimeConformanceMetadata, RuntimeError,
};
use std::{env, path::PathBuf, sync::Arc};

pub use cleanup::{gc_build_cache, select_evictions, CacheEntry, GcReport};
pub use credentials::LocalCredentialBroker;
pub use limits::{
    drop_all_capabilities, host_limits, ContainerHardening, HostConfigLimits, DEFAULT_PIDS_LIMIT,
    NO_NEW_PRIVILEGES,
};

const DEFAULT_MIN_API_VERSION: &str = "1.41";
pub const CONFORMANCE_METADATA: RuntimeConformanceMetadata =
    RuntimeConformanceMetadata::eligible("docker");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedVerifierImage {
    pub(crate) image_id: String,
    pub(crate) stat_command: String,
}

impl TrustedVerifierImage {
    pub fn new(
        image_id: impl Into<String>,
        stat_command: impl Into<String>,
    ) -> Result<Self, RuntimeError> {
        let image_id = image_id.into();
        let digest = image_id.strip_prefix("sha256:").unwrap_or_default();
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(RuntimeError::ResourceLimit(
                "trusted verifier image must be an immutable sha256 image ID".to_owned(),
            ));
        }
        let stat_command = stat_command.into();
        if !stat_command.starts_with('/') || stat_command.contains('\0') {
            return Err(RuntimeError::ResourceLimit(
                "trusted verifier command must be an absolute container path".to_owned(),
            ));
        }
        Ok(Self {
            image_id,
            stat_command,
        })
    }

    pub fn proof_method(&self) -> String {
        format!(
            "autospec.dev/docker-bind-stat/v2;image={};command={}",
            self.image_id, self.stat_command
        )
    }
}

/// The agent container's default user: root, until the worker opts in.
pub const DEFAULT_AGENT_USER: &str = "0:0";

/// Accepts only a numeric `uid:gid` (each part a decimal `u32`).
pub fn validate_agent_user(raw: &str) -> Result<(), RuntimeError> {
    let numeric = |part: &str| {
        !part.is_empty()
            && part.len() <= 10
            && part.bytes().all(|byte| byte.is_ascii_digit())
            && part.parse::<u32>().is_ok()
    };
    match raw.split_once(':') {
        Some((uid, gid)) if numeric(uid) && numeric(gid) => Ok(()),
        _ => Err(RuntimeError::ResourceLimit(format!(
            "agent user {raw:?} must be a numeric uid:gid such as 1000:1000"
        ))),
    }
}

/// The alias the inference relay answers to on the execution network, so the
/// agent reaches it as `http://inferweave:<port>` (spec section 5 R-ORCH).
pub const INFERENCE_RELAY_ALIAS: &str = "inferweave";

/// A per-execution relay that forwards to exactly one InferWeave `host:port`.
///
/// The operator supplies `image`; the orchestrator hands it only the listen
/// port and the upstream as its two arguments (`<listen_port> <upstream>`), so
/// the relay cannot be told to forward anywhere else. Build one through
/// [`DockerRuntime::with_inference_relay`], which validates `upstream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceRelay {
    pub image: String,
    pub upstream: String,
    pub listen_port: u16,
}

/// Accepts only `host:port` with a bare host name or address and a non-zero
/// port. A scheme, path, credential, whitespace or a missing port is refused.
fn validate_relay_upstream(upstream: &str) -> Result<(), RuntimeError> {
    let invalid = || {
        RuntimeError::ResourceLimit(format!(
            "inference upstream {upstream:?} must be host:port, for example gateway.local:8080"
        ))
    };
    let (host, port) = upstream.rsplit_once(':').ok_or_else(invalid)?;
    let bracketed = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'));
    let host_ok = match bracketed {
        Some(inner) => {
            !inner.is_empty() && inner.bytes().all(|b| b.is_ascii_hexdigit() || b == b':')
        }
        None => {
            !host.is_empty()
                && host.len() <= 253
                && host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'_')
        }
    };
    let port_ok = !port.is_empty()
        && port.len() <= 5
        && port.bytes().all(|b| b.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|port| port != 0);
    if host_ok && port_ok {
        Ok(())
    } else {
        Err(invalid())
    }
}

#[derive(Debug, Clone)]
pub struct DockerRuntime {
    pub(crate) client: Docker,
    min_api_version: String,
    pub(crate) state_root: PathBuf,
    pub(crate) storage_verifier: Option<Arc<dyn ReadyAllocationVerifier>>,
    pub(crate) allocation: Option<AllocationReceipt>,
    pub(crate) trusted_verifier: Option<TrustedVerifierImage>,
    pub(crate) credentials: Option<ExecutionCredentials>,
    cleanup_labels: Option<OwnershipLabels>,
    pub(crate) build_cache: Option<BuildCacheConfig>,
    pub(crate) hardening: ContainerHardening,
    pub(crate) agent_user: String,
    pub(crate) inference_relay: Option<InferenceRelay>,
}

/// Where the opt-in shared sccache lives and how large it may grow
/// (spec section 5 R-BUILD defaults: 40 GiB, LRU).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildCacheConfig {
    pub(crate) root: PathBuf,
    pub(crate) max_gib: u64,
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
            Some(socket) if socket.starts_with("tcp://") || socket.starts_with("http://") => {
                Docker::connect_with_http(&socket, 120, bollard::API_DEFAULT_VERSION)
            }
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
            trusted_verifier: None,
            credentials: None,
            cleanup_labels: None,
            build_cache: None,
            hardening: ContainerHardening::Enforced,
            agent_user: DEFAULT_AGENT_USER.to_owned(),
            inference_relay: None,
        })
    }

    /// Runs the agent container as `uid_gid` (numeric `uid:gid`, for example
    /// `1000:1000`) instead of root.
    ///
    /// The default stays `0:0`, so switching the agent off root is an explicit,
    /// reversible step taken by the worker (spec section 5 R-ORCH). Names are
    /// refused: a name would resolve through the image's `/etc/passwd`.
    pub fn with_agent_user(mut self, uid_gid: &str) -> Result<Self, RuntimeError> {
        validate_agent_user(uid_gid)?;
        self.agent_user = uid_gid.to_owned();
        Ok(self)
    }

    /// Configures the per-execution inference relay.
    ///
    /// Refuses an empty image, a zero listen port and an upstream that is not
    /// a plain `host:port`. The relay forwards only to that upstream.
    pub fn with_inference_relay(mut self, relay: InferenceRelay) -> Result<Self, RuntimeError> {
        if relay.image.trim().is_empty() || relay.listen_port == 0 {
            return Err(RuntimeError::ResourceLimit(
                "inference relay needs an image and a non-zero listen port".to_owned(),
            ));
        }
        validate_relay_upstream(&relay.upstream)?;
        self.inference_relay = Some(relay);
        Ok(self)
    }

    /// The configured inference relay, if any.
    pub fn inference_relay(&self) -> Option<&InferenceRelay> {
        self.inference_relay.as_ref()
    }

    /// The `uid:gid` the agent container runs as.
    pub fn agent_user(&self) -> &str {
        &self.agent_user
    }

    /// Selects the container privilege limits (default `Enforced`: every
    /// container `no-new-privileges`, agent capabilities all dropped).
    #[must_use]
    pub fn with_hardening(mut self, hardening: ContainerHardening) -> Self {
        self.hardening = hardening;
        self
    }

    /// The container privilege limits this runtime applies.
    pub fn hardening(&self) -> ContainerHardening {
        self.hardening
    }

    /// Enables the opt-in shared sccache under `root`, bounded to `max_gib`.
    ///
    /// Off by default. Only an agent whose requirement lists the
    /// `rust-build-cache` capability mounts it; a requirement that lists the
    /// capability on a runtime without this configuration fails provisioning.
    /// Who may request the capability is the worker's decision, not this
    /// adapter's (AGENTS.md invariant 3).
    #[must_use]
    pub fn with_build_cache(mut self, root: PathBuf, max_gib: u64) -> Self {
        self.build_cache = Some(BuildCacheConfig { root, max_gib });
        self
    }

    /// The configured shared build cache `(root, max_gib)`, if any.
    pub fn build_cache(&self) -> Option<(&std::path::Path, u64)> {
        self.build_cache
            .as_ref()
            .map(|cache| (cache.root.as_path(), cache.max_gib))
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

    /// Connects provisioning to an exact Ready allocation and immutable verifier image.
    pub fn connect_with_verified_execution_storage(
        socket: Option<&str>,
        verifier: Arc<dyn ReadyAllocationVerifier>,
        allocation: AllocationReceipt,
        trusted_verifier: TrustedVerifierImage,
    ) -> Result<Self, RuntimeError> {
        if allocation.docker_bind.method_version != trusted_verifier.proof_method() {
            return Err(RuntimeError::ResourceLimit(
                "trusted verifier image does not match the allocation Docker proof method"
                    .to_owned(),
            ));
        }
        let mut runtime = Self::connect_with_execution_storage(socket, verifier, allocation)?;
        runtime.trusted_verifier = Some(trusted_verifier);
        Ok(runtime)
    }

    /// Connects only the exact authority needed to destroy one persisted
    /// execution runtime after a worker restart (spec sections 36 and 48).
    ///
    /// Cleanup deliberately does not accept the current provisioning verifier
    /// image or command: a verifier rollout must not strand resources created
    /// under an older durable receipt. The receipt still binds cleanup to the
    /// exact execution labels, state-root path, and Docker daemon identity.
    pub fn connect_for_cleanup(
        socket: Option<&str>,
        allocation: AllocationReceipt,
        labels: &OwnershipLabels,
    ) -> Result<Self, RuntimeError> {
        let state_root = allocation
            .mount_path
            .parent()
            .and_then(|executions| executions.parent())
            .ok_or_else(|| {
                RuntimeError::Cleanup("allocation mount path lacks state root".to_owned())
            })?
            .to_path_buf();
        let layout = ExecutionLayout::new(&state_root, &labels.execution_id)
            .map_err(|error| RuntimeError::Cleanup(error.to_string()))?;
        allocation
            .validate(labels, &layout)
            .map_err(|error| RuntimeError::Cleanup(error.to_string()))?;
        let metadata = std::fs::symlink_metadata(&allocation.mount_path).map_err(|error| {
            RuntimeError::Cleanup(format!(
                "inspect cleanup allocation {}: {error}",
                allocation.mount_path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(RuntimeError::Cleanup(
                "cleanup allocation path is not an owned directory".to_owned(),
            ));
        }
        let canonical = allocation.mount_path.canonicalize().map_err(|error| {
            RuntimeError::Cleanup(format!(
                "canonicalize cleanup allocation {}: {error}",
                allocation.mount_path.display()
            ))
        })?;
        if canonical != allocation.mount_path {
            return Err(RuntimeError::Cleanup(
                "cleanup allocation path is not canonical".to_owned(),
            ));
        }
        let mut runtime = Self::connect_with_state_root(socket, state_root)?;
        runtime.allocation = Some(allocation);
        runtime.cleanup_labels = Some(labels.clone());
        Ok(runtime)
    }

    pub fn new() -> Result<Self, RuntimeError> {
        Self::connect(None)
    }

    /// Binds one already-minted execution credential into only the agent.
    pub fn with_credentials(mut self, credentials: ExecutionCredentials) -> Self {
        self.credentials = Some(credentials);
        self
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

    /// Name of an execution's inference relay container.
    pub fn relay_container_name(execution_id: &ExecutionId) -> String {
        format!("autospec-{execution_id}-inferweave-relay")
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

    async fn require_cleanup_authority(
        &self,
        labels: &OwnershipLabels,
    ) -> Result<(), RuntimeError> {
        if let Some(expected) = &self.cleanup_labels {
            if expected != labels {
                return Err(RuntimeError::Cleanup(
                    "runtime cleanup labels differ from the persisted receipt".to_owned(),
                ));
            }
            let receipt = self.allocation.as_ref().ok_or_else(|| {
                RuntimeError::Cleanup("runtime cleanup lacks its persisted receipt".to_owned())
            })?;
            if &receipt.labels != labels {
                return Err(RuntimeError::Cleanup(
                    "runtime cleanup receipt labels differ from selector authority".to_owned(),
                ));
            }
            let actual = self
                .client
                .info()
                .await
                .map_err(|error| {
                    RuntimeError::Unavailable(format!(
                        "inspect Docker daemon identity for cleanup: {error}"
                    ))
                })?
                .id
                .ok_or_else(|| {
                    RuntimeError::Cleanup(
                        "Docker daemon did not report an identity for cleanup".to_owned(),
                    )
                })?;
            if actual != receipt.docker_bind.daemon_id {
                return Err(RuntimeError::Cleanup(format!(
                    "Docker daemon identity {actual} does not match the persisted cleanup receipt"
                )));
            }
        }
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
    fn inference_relay_upstream_must_be_host_and_port() {
        let relay = |upstream: &str| InferenceRelay {
            image: "relay:1".to_owned(),
            upstream: upstream.to_owned(),
            listen_port: 8080,
        };
        let runtime = || DockerRuntime::connect_with_state_root(None, "/tmp").expect("client");
        for good in [
            "gateway.local:8080",
            "10.0.0.5:443",
            "[::1]:8080",
            "a_b-c:1",
        ] {
            let configured = runtime()
                .with_inference_relay(relay(good))
                .unwrap_or_else(|error| panic!("{good}: {error}"));
            assert_eq!(
                configured.inference_relay().map(|r| r.upstream.as_str()),
                Some(good)
            );
        }
        for bad in [
            "gateway.local",
            "gateway.local:",
            ":8080",
            "gateway.local:0",
            "gateway.local:65536",
            "gateway.local:80a",
            "http://gateway.local:8080",
            "user@gateway.local:8080",
            "gateway.local:8080/path",
            "gate way:8080",
            "",
        ] {
            assert!(
                runtime().with_inference_relay(relay(bad)).is_err(),
                "{bad:?}"
            );
        }
        let mut no_port = relay("gateway.local:8080");
        no_port.listen_port = 0;
        assert!(runtime().with_inference_relay(no_port).is_err());
        let mut no_image = relay("gateway.local:8080");
        no_image.image = " ".to_owned();
        assert!(runtime().with_inference_relay(no_image).is_err());
        assert!(
            runtime().inference_relay().is_none(),
            "off unless configured"
        );
    }

    #[test]
    fn relay_container_name_is_execution_scoped() {
        assert_eq!(
            DockerRuntime::relay_container_name(&ExecutionId::new("node-417-impl-01")),
            "autospec-node-417-impl-01-inferweave-relay"
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
    fn constrained_http_proxy_endpoints_are_constructed_without_socket_path_checks() {
        assert!(DockerRuntime::connect_with_state_root(
            Some("tcp://docker-api:2375"),
            "/var/lib/autospec"
        )
        .is_ok());
        assert!(DockerRuntime::connect_with_state_root(
            Some("http://docker-api:2375"),
            "/var/lib/autospec"
        )
        .is_ok());
    }

    #[test]
    fn daemon_minimum_must_not_exceed_the_bollard_client_version() {
        let error = validate_daemon_api(Some("1.53"), Some("1.48"), "1.47", "1.41")
            .expect_err("daemon minimum is newer than Bollard")
            .to_string();

        assert!(error.contains("requires API 1.48"));
        assert!(error.contains("supports 1.47"));
    }

    #[test]
    fn trusted_mount_verifier_requires_an_immutable_image_and_absolute_command() {
        assert!(TrustedVerifierImage::new("alpine:3.20", "/bin/stat").is_err());
        let image_id = format!("sha256:{}", "a".repeat(64));
        assert!(TrustedVerifierImage::new(&image_id, "stat").is_err());

        let trusted =
            TrustedVerifierImage::new(&image_id, "/bin/stat").expect("immutable trusted verifier");
        assert_eq!(
            trusted.proof_method(),
            format!("autospec.dev/docker-bind-stat/v2;image={image_id};command=/bin/stat")
        );
    }
}
