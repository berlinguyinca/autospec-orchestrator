use crate::{AdoptedExecution, ExecutionLifecycle, LifecycleError, ResourceSample};
use async_trait::async_trait;
use execution_storage::{
    AllocationPhase, AllocationReceipt, AllocationRequest, ExecutionLayout,
    ExecutionLifecycleHoldStore, ExecutionStorageManager, JournalStore, ReadyAllocationVerifier,
};
use git_worktree::{DiffCapture, Worktree, WorktreeManager};
use harness_pi::{PiHarness, PiHarnessConfig};
use harness_traits::{AgentHarness, SessionRef};
use orchestrator_core::{
    Execution, ExecutionEvent, ExecutionId, OwnershipLabels, Role, RuntimeRequirement, SessionId,
    TaskPacket,
};
use orchestrator_persistence::{ArtifactStore, CleanupAuthority, CleanupDisposition};
use runtime_docker::{ContainerHardening, DockerRuntime, TrustedVerifierImage};
use runtime_traits::build_cache::BUILD_CACHE_CAPABILITY;
use runtime_traits::{
    CredentialBroker, EnvironmentHandle, Runtime, VerifiedAgentContainer, VerifiedBindMount,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};

#[async_trait]
pub trait RuntimeFactory: Send + Sync {
    async fn build(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError>;
    async fn build_for_adoption(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        let _ = environment;
        self.build(execution, receipt).await
    }
    /// Constructs exact runtime cleanup authority without minting workload
    /// credentials, which may be expired when durable cleanup resumes (spec
    /// sections 36 and 48).
    async fn build_for_cleanup(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError>;
    async fn cpu_percent(&self, execution: &Execution) -> Result<f64, LifecycleError>;
    /// Default wraps `cpu_percent`; the Docker factory measures the rest.
    async fn resource_sample(
        &self,
        execution: &Execution,
    ) -> Result<ResourceSample, LifecycleError> {
        Ok(ResourceSample {
            cpu_percent: self.cpu_percent(execution).await?,
            ..ResourceSample::default()
        })
    }
    async fn revoke_credentials(&self, execution_id: &ExecutionId) -> Result<(), LifecycleError>;
}

#[async_trait]
pub trait HarnessFactory: Send + Sync {
    async fn build(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        worktree: &Worktree,
    ) -> Result<Arc<dyn AgentHarness>, LifecycleError>;
}

#[async_trait]
pub trait EvidenceStore: Send + Sync {
    async fn persist(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError>;
}

#[derive(Clone)]
pub struct VerifiedDockerRuntimeFactory {
    socket: Option<String>,
    docker_binary: PathBuf,
    verifier: Arc<dyn ReadyAllocationVerifier>,
    trusted_verifier: TrustedVerifierImage,
    credentials: Arc<dyn CredentialBroker>,
    build_cache: Option<(PathBuf, u64)>,
    hardening: ContainerHardening,
}

impl VerifiedDockerRuntimeFactory {
    pub fn new(
        socket: Option<String>,
        docker_binary: PathBuf,
        verifier: Arc<dyn ReadyAllocationVerifier>,
        trusted_verifier: TrustedVerifierImage,
        credentials: Arc<dyn CredentialBroker>,
    ) -> Self {
        Self {
            socket,
            docker_binary,
            verifier,
            trusted_verifier,
            credentials,
            build_cache: None,
            hardening: ContainerHardening::Enforced,
        }
    }

    /// Selects the container privilege limits handed to every runtime this
    /// factory builds. The default `Enforced` sets `no-new-privileges` on every
    /// container and drops the agent's capabilities; `Relaxed` is the operator
    /// opt-out for images that need setuid helpers or capabilities.
    #[must_use]
    pub fn with_container_hardening(mut self, hardening: ContainerHardening) -> Self {
        self.hardening = hardening;
        self
    }

    /// The container privilege limits this factory hands to runtimes.
    pub fn container_hardening(&self) -> ContainerHardening {
        self.hardening
    }

    /// Lets Implementation executions that request `rust-build-cache` mount the
    /// shared sccache under `root` (bounded to `max_gib`). Off by default.
    #[must_use]
    pub fn with_build_cache(mut self, root: PathBuf, max_gib: u64) -> Self {
        self.build_cache = Some((root, max_gib));
        self
    }

    /// The shared build cache `(root, max_gib)` this factory hands to runtimes.
    pub fn build_cache(&self) -> Option<(&Path, u64)> {
        self.build_cache
            .as_ref()
            .map(|(root, max_gib)| (root.as_path(), *max_gib))
    }

    pub(crate) fn configure(&self, runtime: DockerRuntime) -> DockerRuntime {
        apply_build_cache(runtime, self.build_cache.as_ref()).with_hardening(self.hardening)
    }
}

fn apply_build_cache(runtime: DockerRuntime, cache: Option<&(PathBuf, u64)>) -> DockerRuntime {
    match cache {
        Some((root, max_gib)) => runtime.with_build_cache(root.clone(), *max_gib),
        None => runtime,
    }
}

/// The shared build cache is mutable state; independent review must never
/// share mutable state with implementation (AGENTS.md invariant 3), so only an
/// `Implementation` execution may ask for it. Checked before any container
/// exists.
pub fn check_build_cache_role(
    role: Role,
    requirement: &RuntimeRequirement,
) -> Result<(), LifecycleError> {
    let wants_cache = requirement
        .capabilities
        .iter()
        .any(|capability| capability == BUILD_CACHE_CAPABILITY);
    if wants_cache && role != Role::Implementation {
        return Err(LifecycleError::Step(format!(
            "capability {BUILD_CACHE_CAPABILITY} is limited to implementation executions, not {role:?}"
        )));
    }
    Ok(())
}

#[async_trait]
impl RuntimeFactory for VerifiedDockerRuntimeFactory {
    async fn build(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        // Before credentials exist and before any container: a role that may not
        // share the build cache never gets a runtime that requests it.
        check_build_cache_role(execution.role, &execution.manifest.runtime)?;
        let credentials = self
            .credentials
            .mint(execution)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        if credentials.expires_at <= chrono::Utc::now()
            || !credentials
                .path
                .starts_with(receipt.mount_path.join("credentials"))
        {
            let revocation = self.credentials.revoke(&execution.id).await;
            return Err(LifecycleError::Step(match revocation {
                Ok(()) => "credential broker returned invalid execution authority".into(),
                Err(error) => format!(
                    "credential broker returned invalid execution authority; credential revocation failed: {error}"
                ),
            }));
        }
        match DockerRuntime::connect_with_verified_execution_storage(
            self.socket.as_deref(),
            Arc::clone(&self.verifier),
            receipt.clone(),
            self.trusted_verifier.clone(),
        )
        .map(|runtime| {
            Arc::new(self.configure(runtime).with_credentials(credentials)) as Arc<dyn Runtime>
        }) {
            Ok(runtime) => Ok(runtime),
            Err(error) => {
                let revocation = self.credentials.revoke(&execution.id).await;
                Err(LifecycleError::Step(match revocation {
                    Ok(()) => error.to_string(),
                    Err(revoke) => format!("{error}; credential revocation failed: {revoke}"),
                }))
            }
        }
    }

    async fn build_for_adoption(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        let runtime = self.build(execution, receipt).await?;
        let credentials = self
            .credentials
            .mint(execution)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        if credentials.expires_at <= chrono::Utc::now()
            || environment.credentials_path.as_ref() != Some(&credentials.path)
        {
            return Err(LifecycleError::Step(
                "adopted runtime credential differs from broker authority; runtime recreation is required"
                    .into(),
            ));
        }
        Ok(runtime)
    }

    async fn build_for_cleanup(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        DockerRuntime::connect_for_cleanup(
            self.socket.as_deref(),
            receipt.clone(),
            &execution.labels,
        )
        .map(|runtime| Arc::new(runtime) as Arc<dyn Runtime>)
        .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn cpu_percent(&self, execution: &Execution) -> Result<f64, LifecycleError> {
        let docker = self.docker_binary.clone();
        let socket = self.socket.clone();
        let container = DockerRuntime::agent_container_name(&execution.id);
        tokio::task::spawn_blocking(move || {
            let mut command = docker_command(&docker, socket.as_deref());
            let output = command
                .args([
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.CPUPerc}}",
                    &container,
                ])
                .output()
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
            if !output.status.success() {
                return Err(LifecycleError::Step(format!(
                    "Docker CPU probe failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )));
            }
            String::from_utf8(output.stdout)
                .map_err(|error| LifecycleError::Step(error.to_string()))?
                .trim()
                .trim_end_matches('%')
                .parse::<f64>()
                .map_err(|error| LifecycleError::Step(error.to_string()))
        })
        .await
        .map_err(|error| LifecycleError::Step(error.to_string()))?
    }

    async fn resource_sample(
        &self,
        execution: &Execution,
    ) -> Result<ResourceSample, LifecycleError> {
        let docker = self.docker_binary.clone();
        let socket = self.socket.clone();
        let container = DockerRuntime::agent_container_name(&execution.id);
        tokio::task::spawn_blocking(move || {
            probe_resource_sample(&docker, socket.as_deref(), &container)
        })
        .await
        .map_err(|error| LifecycleError::Step(error.to_string()))?
    }

    async fn revoke_credentials(&self, execution_id: &ExecutionId) -> Result<(), LifecycleError> {
        self.credentials
            .revoke(execution_id)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedPiHarnessFactory {
    verifier: Arc<dyn ReadyAllocationVerifier>,
    docker_socket: Option<String>,
    docker_binary: PathBuf,
    pi_executable: String,
    allow_untested_pi_version: bool,
    tools: Vec<String>,
    skills: Vec<PathBuf>,
}

impl VerifiedPiHarnessFactory {
    pub fn new(
        verifier: Arc<dyn ReadyAllocationVerifier>,
        docker_socket: Option<String>,
        docker_binary: PathBuf,
        pi_executable: String,
        tools: Vec<String>,
        skills: Vec<PathBuf>,
    ) -> Self {
        Self {
            verifier,
            docker_socket,
            docker_binary,
            pi_executable,
            allow_untested_pi_version: false,
            tools,
            skills,
        }
    }

    /// Permits Pi releases outside the tested matrix (logged as a warning).
    pub fn with_allow_untested_pi_version(mut self, allow: bool) -> Self {
        self.allow_untested_pi_version = allow;
        self
    }
}

#[async_trait]
impl HarnessFactory for VerifiedPiHarnessFactory {
    async fn build(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        _: &Worktree,
    ) -> Result<Arc<dyn AgentHarness>, LifecycleError> {
        let mut config = PiHarnessConfig::for_ready_allocation(
            Arc::clone(&self.verifier),
            receipt.clone(),
            environment.verified_agent_container.clone(),
        )
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
        config.docker_binary = self.docker_binary.clone();
        config.docker_host = self.docker_socket.clone();
        config.pi_executable = self.pi_executable.clone();
        config.allow_untested_pi_version = self.allow_untested_pi_version;
        config.tools = self.tools.clone();
        config.skills = self.skills.clone();
        let credential_path = environment
            .credentials_path
            .as_ref()
            .ok_or_else(|| LifecycleError::Step("Pi harness lacks credential authority".into()))?;
        let credential = std::fs::read(credential_path)
            .map_err(|error| LifecycleError::Step(format!("read Pi credential: {error}")))?;
        let token = credential
            .split(|byte| *byte == b'\n')
            .next()
            .filter(|token| token.len() >= 32)
            .ok_or_else(|| LifecycleError::Step("Pi credential is malformed".into()))?;
        config.credential_redactions = vec![token.to_vec()];
        config.model_policy = Some(execution.manifest.agent.model_policy.clone());
        Ok(Arc::new(PiHarness::new(config)))
    }
}

#[derive(Debug, Clone)]
pub struct FilesystemEvidenceStore {
    root: PathBuf,
}

#[derive(Clone)]
pub struct ContentAddressedEvidenceStore {
    artifacts: Arc<dyn ArtifactStore>,
}

impl ContentAddressedEvidenceStore {
    pub fn new(artifacts: Arc<dyn ArtifactStore>) -> Self {
        Self { artifacts }
    }
}

#[async_trait]
impl EvidenceStore for ContentAddressedEvidenceStore {
    async fn persist(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError> {
        let attempt_id = execution
            .attempt_id
            .as_ref()
            .ok_or_else(|| LifecycleError::Step("evidence lacks attempt id".into()))?;
        let name = format!("diff-{attempt_id}.patch");
        self.artifacts
            .store(
                &execution.id,
                &name,
                "text/x-diff; charset=utf-8",
                capture.patch.as_bytes(),
            )
            .await
            .map(|artifact| artifact.sha256)
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }
}

impl FilesystemEvidenceStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl EvidenceStore for FilesystemEvidenceStore {
    async fn persist(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError> {
        let root = self.root.clone();
        let execution_id = execution.id.to_string();
        let attempt_id = execution
            .attempt_id
            .as_ref()
            .map(ToString::to_string)
            .ok_or_else(|| LifecycleError::Step("evidence lacks attempt id".into()))?;
        let bytes = capture.patch.as_bytes().to_vec();
        tokio::task::spawn_blocking(move || {
            persist_evidence_file(&root, &execution_id, &attempt_id, &bytes)
        })
        .await
        .map_err(|error| LifecycleError::Step(error.to_string()))?
    }
}

fn persist_evidence_file(
    root: &Path,
    execution_id: &str,
    attempt_id: &str,
    bytes: &[u8],
) -> Result<String, LifecycleError> {
    validate_evidence_component(execution_id, "execution id")?;
    validate_evidence_component(attempt_id, "attempt id")?;
    let root = root
        .canonicalize()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    let directory = root.join("evidence").join(execution_id);
    std::fs::create_dir_all(&directory).map_err(|error| LifecycleError::Step(error.to_string()))?;
    let canonical_directory = directory
        .canonicalize()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !canonical_directory.starts_with(&root) || canonical_directory != directory {
        return Err(LifecycleError::Step(
            "evidence directory escaped its trusted root".into(),
        ));
    }
    let path = directory.join(format!("{attempt_id}.json"));
    if std::fs::symlink_metadata(&path).is_ok() {
        return Err(LifecycleError::Step(
            "evidence artifact already exists".into(),
        ));
    }
    let temporary = directory.join(format!(".{attempt_id}.tmp"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    std::fs::rename(&temporary, &path).map_err(|error| LifecycleError::Step(error.to_string()))?;
    File::open(&directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    Ok(path.to_string_lossy().into_owned())
}

fn validate_evidence_component(value: &str, purpose: &str) -> Result<(), LifecycleError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.as_bytes().contains(&0)
    {
        return Err(LifecycleError::Step(format!(
            "{purpose} is not a safe path component"
        )));
    }
    Ok(())
}

/// Production composition of the worker lifecycle's independently testable
/// storage, Git, runtime, harness, and evidence boundaries.
struct KnownSecret {
    token: Vec<u8>,
    expires_at: chrono::DateTime<chrono::Utc>,
}

pub struct SystemExecutionLifecycle {
    state_root: PathBuf,
    verifier: Arc<dyn ReadyAllocationVerifier>,
    docker_binary: PathBuf,
    docker_socket: Option<String>,
    storage: Arc<dyn ExecutionStorageManager>,
    worktrees: Arc<dyn WorktreeManager>,
    runtimes: Arc<dyn RuntimeFactory>,
    harnesses: Arc<dyn HarnessFactory>,
    evidence: Arc<dyn EvidenceStore>,
    active_runtimes: Mutex<BTreeMap<ExecutionId, Arc<dyn Runtime>>>,
    active_harnesses: Mutex<BTreeMap<ExecutionId, Arc<dyn AgentHarness>>>,
    known_secrets: Mutex<BTreeMap<ExecutionId, KnownSecret>>,
}

#[derive(Debug, Clone)]
pub struct SystemRecoveryConfig {
    pub state_root: PathBuf,
    pub verifier: Arc<dyn ReadyAllocationVerifier>,
    pub docker_binary: PathBuf,
    pub docker_socket: Option<String>,
}

impl SystemExecutionLifecycle {
    pub fn new(
        recovery: SystemRecoveryConfig,
        storage: Arc<dyn ExecutionStorageManager>,
        worktrees: Arc<dyn WorktreeManager>,
        runtimes: Arc<dyn RuntimeFactory>,
        harnesses: Arc<dyn HarnessFactory>,
        evidence: Arc<dyn EvidenceStore>,
    ) -> Self {
        Self {
            state_root: recovery.state_root,
            verifier: recovery.verifier,
            docker_binary: recovery.docker_binary,
            docker_socket: recovery.docker_socket,
            storage,
            worktrees,
            runtimes,
            harnesses,
            evidence,
            active_runtimes: Mutex::new(BTreeMap::new()),
            active_harnesses: Mutex::new(BTreeMap::new()),
            known_secrets: Mutex::new(BTreeMap::new()),
        }
    }

    fn runtime(&self, id: &ExecutionId) -> Result<Arc<dyn Runtime>, LifecycleError> {
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .get(id)
            .cloned()
            .ok_or_else(|| LifecycleError::Step(format!("runtime is not active for {id}")))
    }

    fn harness(&self, id: &ExecutionId) -> Result<Arc<dyn AgentHarness>, LifecycleError> {
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .get(id)
            .cloned()
            .ok_or_else(|| LifecycleError::Step(format!("harness is not active for {id}")))
    }

    fn remember_and_validate_credentials(
        &self,
        execution: &Execution,
        environment: &EnvironmentHandle,
    ) -> Result<(), LifecycleError> {
        let path = environment.credentials_path.as_ref().ok_or_else(|| {
            LifecycleError::Step("execution runtime lacks credential authority".into())
        })?;
        let bytes = std::fs::read(path)
            .map_err(|error| LifecycleError::Step(format!("read credential authority: {error}")))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| LifecycleError::Step("credential authority is malformed".into()))?;
        let mut lines = text.lines();
        let token = lines
            .next()
            .filter(|token| token.len() >= 32)
            .ok_or_else(|| LifecycleError::Step("credential authority is malformed".into()))?;
        let expires_at = lines
            .next()
            .and_then(|expiry| expiry.parse::<chrono::DateTime<chrono::Utc>>().ok())
            .ok_or_else(|| LifecycleError::Step("credential authority is malformed".into()))?;
        if expires_at <= chrono::Utc::now() {
            return Err(LifecycleError::Step(
                "execution credential expired before use".into(),
            ));
        }
        self.known_secrets
            .lock()
            .map_err(|_| LifecycleError::Step("secret registry lock poisoned".into()))?
            .insert(
                execution.id.clone(),
                KnownSecret {
                    token: token.as_bytes().to_vec(),
                    expires_at,
                },
            );
        Ok(())
    }

    fn ensure_credentials_live(&self, id: &ExecutionId) -> Result<(), LifecycleError> {
        let secrets = self
            .known_secrets
            .lock()
            .map_err(|_| LifecycleError::Step("secret registry lock poisoned".into()))?;
        let secret = secrets
            .get(id)
            .ok_or_else(|| LifecycleError::Step("credential authority is unavailable".into()))?;
        if secret.expires_at <= chrono::Utc::now() {
            return Err(LifecycleError::Step(
                "execution credential expired before use".into(),
            ));
        }
        Ok(())
    }

    fn reject_secret(&self, id: &ExecutionId, bytes: &[u8]) -> Result<(), LifecycleError> {
        let secrets = self
            .known_secrets
            .lock()
            .map_err(|_| LifecycleError::Step("secret registry lock poisoned".into()))?;
        if secrets
            .get(id)
            .is_some_and(|secret| contains_bytes(bytes, &secret.token))
        {
            return Err(LifecycleError::Step(
                "credential exfiltration was rejected".into(),
            ));
        }
        Ok(())
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[derive(Deserialize)]
struct DurableWorktreeOwner {
    labels: BTreeMap<String, String>,
    base_sha: String,
    branch: String,
}

#[derive(Deserialize)]
struct DurableCleanupHandles {
    receipt: Option<AllocationReceipt>,
    worktree: Option<DurableWorktreeHandle>,
    runtime: Option<DurableRuntimeHandle>,
    #[serde(default)]
    runtime_selector: Option<OwnershipLabels>,
    session: Option<DurableSessionHandle>,
}

#[derive(Deserialize)]
struct DurableWorktreeHandle {
    execution_id: ExecutionId,
    path: String,
    branch: String,
    base_sha: String,
    repository: String,
}

#[derive(Deserialize)]
struct DurableRuntimeHandle {
    execution_id: ExecutionId,
    network: String,
    agent_container: String,
    container_id: String,
    daemon_id: String,
    labels: OwnershipLabels,
    mounts: Vec<DurableMountHandle>,
    service_containers: Vec<String>,
    volumes: Vec<String>,
    credentials_path: Option<PathBuf>,
}

#[derive(Deserialize)]
struct DurableMountHandle {
    source: PathBuf,
    target: String,
    writable: bool,
}

#[derive(Deserialize)]
struct DurableSessionHandle {
    id: SessionId,
    path: String,
    execution_id: ExecutionId,
    worktree_path: String,
}

fn durable_worktree(handle: DurableWorktreeHandle) -> Worktree {
    Worktree {
        execution_id: handle.execution_id,
        path: handle.path,
        branch: handle.branch,
        base_sha: handle.base_sha,
        repository: handle.repository,
    }
}

fn durable_environment(handle: DurableRuntimeHandle) -> EnvironmentHandle {
    EnvironmentHandle {
        execution_id: handle.execution_id,
        network: handle.network,
        agent_container: handle.agent_container,
        verified_agent_container: VerifiedAgentContainer {
            container_id: handle.container_id,
            daemon_id: handle.daemon_id,
            labels: handle.labels,
            mounts: handle
                .mounts
                .into_iter()
                .map(|mount| VerifiedBindMount {
                    source: mount.source,
                    target: mount.target,
                    writable: mount.writable,
                })
                .collect(),
        },
        service_containers: handle.service_containers,
        volumes: handle.volumes,
        credentials_path: handle.credentials_path,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerInspect {
    id: String,
    state: DockerContainerState,
    config: DockerContainerConfig,
    mounts: Vec<DockerContainerMount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerState {
    running: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerConfig {
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DockerContainerMount {
    #[serde(rename = "Type")]
    mount_type: String,
    source: PathBuf,
    destination: String,
    #[serde(rename = "RW")]
    writable: bool,
}

fn docker_container_capability(
    docker: &Path,
    socket: Option<&str>,
    receipt: &AllocationReceipt,
    layout: &ExecutionLayout,
    container_id: &str,
    execution: &Execution,
) -> Result<runtime_traits::VerifiedAgentContainer, LifecycleError> {
    let daemon = docker_command(docker, socket)
        .args(["info", "--format={{.ID}}"])
        .output()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !daemon.status.success()
        || String::from_utf8_lossy(&daemon.stdout).trim() != receipt.docker_bind.daemon_id
    {
        return Err(LifecycleError::Step(
            "Docker daemon identity changed during restart adoption".into(),
        ));
    }
    let output = docker_command(docker, socket)
        .args(["inspect", "--type", "container", container_id])
        .output()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !output.status.success() {
        return Err(LifecycleError::Step(
            "durable Pi container is unavailable during restart adoption".into(),
        ));
    }
    let mut inspections: Vec<DockerContainerInspect> = serde_json::from_slice(&output.stdout)
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if inspections.len() != 1 {
        return Err(LifecycleError::Step(
            "Docker returned an ambiguous container inspection".into(),
        ));
    }
    let inspection = inspections.pop().expect("length checked");
    if inspection.id != container_id
        || !inspection.state.running
        || inspection.config.labels != receipt.labels.to_map()
    {
        return Err(LifecycleError::Step(
            "durable Pi container authority changed during restart adoption".into(),
        ));
    }
    let mut mounts = Vec::with_capacity(inspection.mounts.len());
    for mount in inspection.mounts {
        if mount.mount_type != "bind" {
            return Err(LifecycleError::Step(
                "adopted agent container has a non-bind mount".into(),
            ));
        }
        mounts.push(runtime_traits::VerifiedBindMount {
            source: mount
                .source
                .canonicalize()
                .map_err(|error| LifecycleError::Step(error.to_string()))?,
            target: mount.destination,
            writable: mount.writable,
        });
    }
    mounts.sort();
    let expected_repository = layout
        .repository
        .canonicalize()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    let expected_conversation = layout
        .conversation
        .canonicalize()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !mounts.iter().any(|mount| {
        mount.source == expected_repository && mount.target == "/workspace" && mount.writable
    }) || !mounts.iter().any(|mount| {
        mount.source == expected_conversation && mount.target == "/session" && mount.writable
    }) || mounts.iter().any(|mount| mount.source == layout.session)
    {
        return Err(LifecycleError::Step(
            "adopted agent container mounts do not match durable execution layout".into(),
        ));
    }
    let mut expected_containers =
        BTreeSet::from([DockerRuntime::agent_container_name(&execution.id)]);
    expected_containers.extend(
        execution
            .manifest
            .services
            .iter()
            .map(|service| DockerRuntime::service_container_name(&execution.id, &service.name)),
    );
    let all_containers = docker_resource_names(
        docker,
        socket,
        &["ps", "-a"],
        &receipt.labels.execution_id,
        "{{.Names}}",
    )?;
    let running_containers = docker_resource_names(
        docker,
        socket,
        &["ps"],
        &receipt.labels.execution_id,
        "{{.Names}}",
    )?;
    if all_containers != expected_containers || running_containers != expected_containers {
        return Err(LifecycleError::Step(
            "durable runtime container set is incomplete, stopped, or contains extras".into(),
        ));
    }
    for container in &expected_containers {
        let labels = docker_resource_labels(
            docker,
            socket,
            &[
                "inspect",
                "--type",
                "container",
                "--format={{json .Config.Labels}}",
            ],
            container,
        )?;
        if labels != receipt.labels.to_map() {
            return Err(LifecycleError::Step(format!(
                "durable runtime container {container} has foreign or incomplete labels"
            )));
        }
    }
    let expected_network = DockerRuntime::network_name(&receipt.labels.execution_id);
    let networks = docker_resource_names(
        docker,
        socket,
        &["network", "ls"],
        &receipt.labels.execution_id,
        "{{.Name}}",
    )?;
    if networks != BTreeSet::from([expected_network.clone()]) {
        return Err(LifecycleError::Step(
            "durable runtime network set differs from the manifest".into(),
        ));
    }
    if docker_resource_labels(
        docker,
        socket,
        &["network", "inspect", "--format={{json .Labels}}"],
        &expected_network,
    )? != receipt.labels.to_map()
    {
        return Err(LifecycleError::Step(
            "durable runtime network has foreign or incomplete labels".into(),
        ));
    }
    let volumes = docker_resource_names(
        docker,
        socket,
        &["volume", "ls"],
        &receipt.labels.execution_id,
        "{{.Name}}",
    )?;
    if !volumes.is_empty() {
        return Err(LifecycleError::Step(
            "durable runtime contains unexpected Docker volumes".into(),
        ));
    }
    Ok(runtime_traits::VerifiedAgentContainer {
        container_id: container_id.into(),
        daemon_id: receipt.docker_bind.daemon_id.clone(),
        labels: receipt.labels.clone(),
        mounts,
    })
}

fn docker_resource_labels(
    docker: &Path,
    socket: Option<&str>,
    prefix: &[&str],
    resource: &str,
) -> Result<BTreeMap<String, String>, LifecycleError> {
    let output = docker_command(docker, socket)
        .args(prefix)
        .arg(resource)
        .output()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !output.status.success() {
        return Err(LifecycleError::Step(format!(
            "Docker resource label inspection failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    serde_json::from_slice(String::from_utf8_lossy(&output.stdout).trim().as_bytes())
        .map_err(|error| LifecycleError::Step(format!("malformed Docker resource labels: {error}")))
}

fn docker_resource_names(
    docker: &Path,
    socket: Option<&str>,
    prefix: &[&str],
    execution_id: &ExecutionId,
    format: &str,
) -> Result<BTreeSet<String>, LifecycleError> {
    let mut command = docker_command(docker, socket);
    command.args(prefix).args([
        "--filter",
        "label=autospec.managed=true",
        "--filter",
        &format!("label=autospec.execution_id={execution_id}"),
        "--format",
        format,
    ]);
    let output = command
        .output()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !output.status.success() {
        return Err(LifecycleError::Step(format!(
            "Docker resource inventory failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect())
}

fn docker_container_id(
    docker: &Path,
    socket: Option<&str>,
    container: &str,
) -> Result<String, LifecycleError> {
    let output = docker_command(docker, socket)
        .args([
            "inspect",
            "--type",
            "container",
            "--format={{.Id}}",
            container,
        ])
        .output()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !output.status.success() || id.is_empty() {
        return Err(LifecycleError::Step(
            "durable agent container identity is unavailable".into(),
        ));
    }
    Ok(id)
}

const CURRENT_COMMAND_MAX_CHARS: usize = 200;

/// One blocking probe of the agent container: `docker stats` for CPU, RSS and
/// block I/O (a failure is an error, never a zero counter) and `docker top`
/// for the current command (best effort: `None` when it cannot be read).
fn probe_resource_sample(
    docker: &Path,
    socket: Option<&str>,
    container: &str,
) -> Result<ResourceSample, LifecycleError> {
    let output = docker_command(docker, socket)
        .args(["stats", "--no-stream", "--format", "{{json .}}", container])
        .output()
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    if !output.status.success() {
        return Err(LifecycleError::Step(format!(
            "Docker stats probe failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let line = String::from_utf8(output.stdout)
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
    let mut sample = parse_stats(line.trim())?;
    sample.current_command = docker_command(docker, socket)
        .args(["top", container, "-eo", "pid,args"])
        .output()
        .ok()
        .filter(|top| top.status.success())
        .and_then(|top| parse_top_command(&String::from_utf8_lossy(&top.stdout)));
    Ok(sample)
}

#[derive(Deserialize)]
struct DockerStatsLine {
    #[serde(rename = "CPUPerc")]
    cpu_perc: Option<String>,
    #[serde(rename = "MemUsage")]
    mem_usage: Option<String>,
    #[serde(rename = "BlockIO")]
    block_io: Option<String>,
}

/// Parses one `docker stats --no-stream --format '{{json .}}'` line. Any
/// missing or malformed CPU or block-I/O field is an error: a probe failure
/// must never look like a flat counter.
fn parse_stats(json: &str) -> Result<ResourceSample, LifecycleError> {
    let invalid = |what: String| LifecycleError::Step(format!("Docker stats: {what}"));
    let line: DockerStatsLine =
        serde_json::from_str(json).map_err(|error| invalid(format!("invalid JSON: {error}")))?;
    let cpu = line
        .cpu_perc
        .ok_or_else(|| invalid("missing CPUPerc".into()))?;
    let cpu_percent = cpu
        .trim()
        .trim_end_matches('%')
        .parse::<f64>()
        .map_err(|error| invalid(format!("CPUPerc {cpu:?}: {error}")))?;
    let block_io = line
        .block_io
        .ok_or_else(|| invalid("missing BlockIO".into()))?;
    let io_bytes = block_io
        .split('/')
        .map(|part| parse_size(part).ok_or_else(|| invalid(format!("BlockIO {block_io:?}"))))
        .sum::<Result<u64, _>>()?;
    if block_io.split('/').count() != 2 {
        return Err(invalid(format!("BlockIO {block_io:?}")));
    }
    let rss_bytes = match line.mem_usage {
        None => None,
        Some(usage) => {
            let used = usage.split('/').next().unwrap_or_default();
            Some(parse_size(used).ok_or_else(|| invalid(format!("MemUsage {usage:?}")))?)
        }
    };
    Ok(ResourceSample {
        cpu_percent,
        io_bytes: Some(io_bytes),
        rss_bytes,
        disk_bytes: None,
        current_command: None,
    })
}

/// Docker prints sizes as `12.3MB` (decimal, block I/O) or `10.5MiB`
/// (binary, memory).
fn parse_size(text: &str) -> Option<u64> {
    let text = text.trim();
    let split = text.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
    let (number, unit) = text.split_at(split);
    let multiplier: f64 = match unit {
        "B" => 1.0,
        "kB" | "KB" => 1e3,
        "MB" => 1e6,
        "GB" => 1e9,
        "TB" => 1e12,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        "TiB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let value = number.parse::<f64>().ok()?;
    (value.is_finite() && value >= 0.0).then(|| (value * multiplier).round() as u64)
}

/// The newest process (highest pid) of `docker top ... -eo pid,args`,
/// truncated to 200 characters.
fn parse_top_command(output: &str) -> Option<String> {
    output
        .lines()
        .skip(1)
        .filter_map(|line| {
            let line = line.trim_start();
            let (pid, args) = line.split_once(char::is_whitespace)?;
            Some((pid.parse::<u64>().ok()?, args.trim()))
        })
        .filter(|(_, args)| !args.is_empty())
        .max_by_key(|(pid, _)| *pid)
        .map(|(_, args)| args.chars().take(CURRENT_COMMAND_MAX_CHARS).collect())
}

/// Used bytes of the volume mounted at `root` (spec section 5 R-WATCH (a)).
///
/// Each execution gets a dedicated volume, so a `root` on its own device is
/// that execution's usage. When `root` shares a device with `state_root` the
/// filesystem figure would be host-wide, so it is withheld (`None`); so is any
/// error, with a debug log. A number is never invented.
fn execution_disk_bytes(root: &Path, state_root: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let root_device = std::fs::metadata(root).map(|m| m.dev());
    let state_device = std::fs::metadata(state_root).map(|m| m.dev());
    let (Ok(root_device), Ok(state_device)) = (root_device, state_device) else {
        tracing::debug!(root = %root.display(), "execution disk probe: cannot stat");
        return None;
    };
    if root_device == state_device {
        return None;
    }
    match rustix::fs::statvfs(root) {
        Ok(stat) => stat
            .f_blocks
            .saturating_sub(stat.f_bfree)
            .checked_mul(stat.f_frsize),
        Err(error) => {
            tracing::debug!(root = %root.display(), %error, "execution disk probe: statvfs failed");
            None
        }
    }
}

/// Drops a sampled command line that `is_secret` flags; the numeric counters
/// carry no secret and stay.
fn scrub_current_command(sample: &mut ResourceSample, is_secret: impl Fn(&[u8]) -> bool) {
    if sample
        .current_command
        .as_ref()
        .is_some_and(|command| is_secret(command.as_bytes()))
    {
        sample.current_command = None;
    }
}

fn docker_command(binary: &Path, socket: Option<&str>) -> Command {
    let mut command = Command::new(binary);
    if let Some(socket) = socket {
        command.env("DOCKER_HOST", socket);
    }
    command
}

#[async_trait]
impl ExecutionLifecycle for SystemExecutionLifecycle {
    async fn allocate(&self, execution: &Execution) -> Result<AllocationReceipt, LifecycleError> {
        let storage = Arc::clone(&self.storage);
        let request = AllocationRequest {
            labels: execution.labels.clone(),
            disk_gib: execution.manifest.runtime.disk_gib,
        };
        tokio::task::spawn_blocking(move || storage.allocate(&request))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn create_worktree(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Worktree, LifecycleError> {
        let worktrees = Arc::clone(&self.worktrees);
        let labels = execution.labels.clone();
        let repository = execution.manifest.repository.clone();
        let receipt = receipt.clone();
        tokio::task::spawn_blocking(move || {
            let base = repository
                .base_sha
                .as_deref()
                .unwrap_or(&repository.base_ref);
            let branch = repository.branch.as_deref().ok_or_else(|| {
                LifecycleError::Step("execution lacks an AutoSpec-provided branch".into())
            })?;
            worktrees
                .create_in(&labels, &repository.repo, base, branch, &receipt)
                .map_err(|error| LifecycleError::Step(error.to_string()))
        })
        .await
        .map_err(|error| LifecycleError::Step(error.to_string()))?
    }

    async fn provision(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        _: &Worktree,
    ) -> Result<EnvironmentHandle, LifecycleError> {
        let runtime = self.runtimes.build(execution, receipt).await?;
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .insert(execution.id.clone(), Arc::clone(&runtime));
        let environment = match runtime
            .provision(
                &execution.labels,
                &execution.manifest.runtime,
                &execution.manifest.services,
            )
            .await
        {
            Ok(environment) => environment,
            Err(error) => {
                let revocation = self.runtimes.revoke_credentials(&execution.id).await;
                return Err(LifecycleError::Step(match revocation {
                    Ok(()) => error.to_string(),
                    Err(revoke) => format!("{error}; credential revocation failed: {revoke}"),
                }));
            }
        };
        Ok(environment)
    }

    async fn start(
        &self,
        execution: &Execution,
        receipt: &AllocationReceipt,
        environment: &EnvironmentHandle,
        worktree: &Worktree,
        packet: &TaskPacket,
    ) -> Result<SessionRef, LifecycleError> {
        self.remember_and_validate_credentials(execution, environment)?;
        self.reject_secret(
            &execution.id,
            &serde_json::to_vec(packet).map_err(|error| LifecycleError::Step(error.to_string()))?,
        )?;
        let packet_directory = Path::new(&worktree.path).join(".autospec");
        match std::fs::create_dir(&packet_directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = std::fs::symlink_metadata(&packet_directory)
                    .map_err(|error| LifecycleError::Step(error.to_string()))?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(LifecycleError::Step(
                        "TaskPacket staging path is not a real directory".into(),
                    ));
                }
            }
            Err(error) => return Err(LifecycleError::Step(error.to_string())),
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&packet_directory, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
        }
        self.verifier
            .verify_ready(receipt)
            .and_then(|verified| verified.verify_directory(&packet_directory))
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        let harness = self
            .harnesses
            .build(execution, receipt, environment, worktree)
            .await?;
        let session = harness
            .start(packet)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .insert(execution.id.clone(), harness);
        Ok(session)
    }

    async fn resume(
        &self,
        execution: &Execution,
        _: &AllocationReceipt,
        environment: &EnvironmentHandle,
        session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        self.remember_and_validate_credentials(execution, environment)?;
        self.harness(&execution.id)?
            .resume_interactive(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn pause(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        self.harness(&execution.id)?
            .stop(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn fork_conversation(
        &self,
        execution: &Execution,
        session: &SessionRef,
        target: &SessionId,
    ) -> Result<SessionRef, LifecycleError> {
        self.ensure_credentials_live(&execution.id)?;
        self.harness(&execution.id)?
            .fork_conversation_as(session, target)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn poll(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<Vec<ExecutionEvent>, LifecycleError> {
        self.ensure_credentials_live(&execution.id)?;
        let events = self
            .harness(&execution.id)?
            .poll_events(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.reject_secret(
            &execution.id,
            &serde_json::to_vec(&events)
                .map_err(|error| LifecycleError::Step(error.to_string()))?,
        )?;
        Ok(events)
    }

    async fn cpu_percent(&self, execution: &Execution) -> Result<f64, LifecycleError> {
        self.runtimes.cpu_percent(execution).await
    }

    /// The sampled command line is observed output of the agent: one that
    /// contains the execution's credential is dropped, not reported (the
    /// numeric counters carry no secret and stay).
    async fn resource_sample(
        &self,
        execution: &Execution,
    ) -> Result<ResourceSample, LifecycleError> {
        let mut sample = self.runtimes.resource_sample(execution).await?;
        scrub_current_command(&mut sample, |bytes| {
            self.reject_secret(&execution.id, bytes).is_err()
        });
        sample.disk_bytes = ExecutionLayout::new(&self.state_root, &execution.id)
            .ok()
            .and_then(|layout| execution_disk_bytes(&layout.root, &self.state_root));
        Ok(sample)
    }

    async fn stop(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        let harness = self.harness(&execution.id)?;
        harness
            .stop(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .remove(&execution.id);
        Ok(())
    }

    async fn capture(&self, worktree: &Worktree) -> Result<DiffCapture, LifecycleError> {
        let manager = Arc::clone(&self.worktrees);
        let worktree = worktree.clone();
        let execution_id = worktree.execution_id.clone();
        let worktree_path = worktree.path.clone();
        let capture = tokio::task::spawn_blocking(move || manager.capture_diff(&worktree))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.reject_secret(&execution_id, capture.patch.as_bytes())?;
        let root = Path::new(&worktree_path)
            .canonicalize()
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        for changed in &capture.changed_files {
            let candidate = root.join(changed);
            let metadata = match std::fs::symlink_metadata(&candidate) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(LifecycleError::Step(error.to_string())),
            };
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(LifecycleError::Step(
                    "changed evidence path is not a regular file".into(),
                ));
            }
            let canonical = candidate
                .canonicalize()
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
            if !canonical.starts_with(&root) {
                return Err(LifecycleError::Step(
                    "changed evidence path escaped its worktree".into(),
                ));
            }
            self.reject_secret(
                &execution_id,
                &std::fs::read(canonical)
                    .map_err(|error| LifecycleError::Step(error.to_string()))?,
            )?;
        }
        Ok(capture)
    }

    async fn persist_evidence(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError> {
        self.reject_secret(&execution.id, capture.patch.as_bytes())?;
        self.evidence.persist(execution, capture).await
    }

    async fn destroy_runtime(&self, execution: &Execution) -> Result<(), LifecycleError> {
        let runtime = self.runtime(&execution.id)?;
        let destroy = runtime.destroy(&execution.labels).await;
        let revoke = self.runtimes.revoke_credentials(&execution.id).await;
        if destroy.is_ok() && revoke.is_ok() {
            self.active_runtimes
                .lock()
                .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
                .remove(&execution.id);
            self.known_secrets
                .lock()
                .map_err(|_| LifecycleError::Step("secret registry lock poisoned".into()))?
                .remove(&execution.id);
            return Ok(());
        }
        Err(LifecycleError::Step(format!(
            "runtime destroy: {}; credential revoke: {}",
            destroy
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "ok".to_owned()),
            revoke
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "ok".to_owned())
        )))
    }

    async fn destroy_worktree(&self, worktree: &Worktree) -> Result<(), LifecycleError> {
        let manager = Arc::clone(&self.worktrees);
        let worktree = worktree.clone();
        tokio::task::spawn_blocking(move || manager.destroy(&worktree))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn ack_destroy_worktree(&self, worktree: &Worktree) -> Result<(), LifecycleError> {
        let manager = Arc::clone(&self.worktrees);
        let worktree = worktree.clone();
        tokio::task::spawn_blocking(move || manager.ack_destroy(&worktree))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn recover_interrupted_worktree(
        &self,
        execution: &Execution,
        _receipt: Option<&AllocationReceipt>,
    ) -> Result<(), LifecycleError> {
        let layout = ExecutionLayout::new(&self.state_root, &execution.id)
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        let manager = Arc::clone(&self.worktrees);
        let labels = execution.labels.clone();
        tokio::task::spawn_blocking(move || {
            manager.recover_interrupted_create(&labels, &layout.repository)
        })
        .await
        .map_err(|error| LifecycleError::Step(error.to_string()))?
        .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn release_storage(&self, receipt: &AllocationReceipt) -> Result<(), LifecycleError> {
        let storage = Arc::clone(&self.storage);
        let receipt = receipt.clone();
        tokio::task::spawn_blocking(move || storage.release(&receipt))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn ack_release_storage(&self, receipt: &AllocationReceipt) -> Result<(), LifecycleError> {
        let storage = Arc::clone(&self.storage);
        let receipt = receipt.clone();
        tokio::task::spawn_blocking(move || storage.ack_release(&receipt))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn cleanup_authority(
        &self,
        authority: &CleanupAuthority,
        execution: &Execution,
    ) -> Result<(), LifecycleError> {
        let handles: DurableCleanupHandles = serde_json::from_value(authority.handles.clone())
            .map_err(|error| LifecycleError::Step(format!("decode cleanup authority: {error}")))?;
        let layout = ExecutionLayout::new(&self.state_root, &authority.execution_id)
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        let mut receipt = handles.receipt;
        if receipt.is_none() && layout.root.exists() {
            let storage = Arc::clone(&self.storage);
            let request = AllocationRequest {
                labels: execution.labels.clone(),
                disk_gib: execution.manifest.runtime.disk_gib,
            };
            receipt = Some(
                tokio::task::spawn_blocking(move || storage.allocate(&request))
                    .await
                    .map_err(|error| LifecycleError::Step(error.to_string()))?
                    .map_err(|error| LifecycleError::Step(error.to_string()))?,
            );
        }
        if let Some(receipt) = &receipt {
            if receipt.labels.execution_id != authority.execution_id {
                return Err(LifecycleError::Step(
                    "cleanup receipt belongs to another execution".into(),
                ));
            }
            self.verifier
                .verify_ready(receipt)
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
        }
        let mut worktree = handles.worktree.map(|handle| Worktree {
            execution_id: handle.execution_id,
            path: handle.path,
            branch: handle.branch,
            base_sha: handle.base_sha,
            repository: handle.repository,
        });
        if worktree
            .as_ref()
            .is_some_and(|worktree| worktree.execution_id != authority.execution_id)
        {
            return Err(LifecycleError::Step(
                "cleanup worktree belongs to another execution".into(),
            ));
        }
        if worktree.is_none() && layout.repository.join(".autospec-owner.json").is_file() {
            let owner: DurableWorktreeOwner = serde_json::from_slice(
                &std::fs::read(layout.repository.join(".autospec-owner.json"))
                    .map_err(|error| LifecycleError::Step(error.to_string()))?,
            )
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
            if owner.labels != execution.labels.to_map() {
                return Err(LifecycleError::Step(
                    "recovered worktree owner differs from cleanup authority".into(),
                ));
            }
            worktree = Some(Worktree {
                execution_id: authority.execution_id.clone(),
                path: layout.repository.to_string_lossy().into_owned(),
                branch: owner.branch,
                base_sha: owner.base_sha,
                repository: execution.manifest.repository.repo.clone(),
            });
        }
        let environment = handles.runtime.map(|handle| EnvironmentHandle {
            execution_id: handle.execution_id,
            network: handle.network,
            agent_container: handle.agent_container,
            verified_agent_container: VerifiedAgentContainer {
                container_id: handle.container_id,
                daemon_id: handle.daemon_id,
                labels: handle.labels,
                mounts: handle
                    .mounts
                    .into_iter()
                    .map(|mount| VerifiedBindMount {
                        source: mount.source,
                        target: mount.target,
                        writable: mount.writable,
                    })
                    .collect(),
            },
            service_containers: handle.service_containers,
            volumes: handle.volumes,
            credentials_path: handle.credentials_path,
        });
        if environment.as_ref().is_some_and(|environment| {
            environment.execution_id != authority.execution_id
                || environment.verified_agent_container.labels.execution_id
                    != authority.execution_id
        }) {
            return Err(LifecycleError::Step(
                "cleanup runtime belongs to another execution".into(),
            ));
        }
        let mut session = handles.session.map(|handle| SessionRef {
            id: handle.id,
            path: handle.path,
            execution_id: handle.execution_id,
            worktree_path: handle.worktree_path,
        });
        if session
            .as_ref()
            .is_some_and(|session| session.execution_id != authority.execution_id)
        {
            return Err(LifecycleError::Step(
                "cleanup session belongs to another execution".into(),
            ));
        }

        if session.is_none() && environment.is_some() {
            let holds = ExecutionLifecycleHoldStore::new(&self.state_root)
                .and_then(|store| store.list(&authority.execution_id))
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
            let matching = holds
                .into_iter()
                .filter(|hold| hold.labels == execution.labels)
                .collect::<Vec<_>>();
            if matching.len() > 1 {
                return Err(LifecycleError::Step(
                    "cleanup authority has multiple durable Pi holds".into(),
                ));
            }
            if let Some(hold) = matching.first() {
                session = Some(SessionRef {
                    id: SessionId::new(&hold.session_id),
                    path: layout.session.to_string_lossy().into_owned(),
                    execution_id: authority.execution_id.clone(),
                    worktree_path: layout.repository.to_string_lossy().into_owned(),
                });
            }
        }

        let runtime_present = if let Some(receipt) = &receipt {
            !docker_resource_names(
                &self.docker_binary,
                self.docker_socket.as_deref(),
                &["ps", "-a"],
                &receipt.labels.execution_id,
                "{{.Names}}",
            )?
            .is_empty()
                || !docker_resource_names(
                    &self.docker_binary,
                    self.docker_socket.as_deref(),
                    &["network", "ls"],
                    &receipt.labels.execution_id,
                    "{{.Name}}",
                )?
                .is_empty()
                || !docker_resource_names(
                    &self.docker_binary,
                    self.docker_socket.as_deref(),
                    &["volume", "ls"],
                    &receipt.labels.execution_id,
                    "{{.Name}}",
                )?
                .is_empty()
        } else {
            false
        };
        if let Some(environment) = &environment {
            let receipt = receipt.as_ref().ok_or_else(|| {
                LifecycleError::Step("runtime cleanup lacks storage receipt".into())
            })?;
            if let (Some(worktree), Some(_session)) = (&worktree, &session) {
                let harness = self
                    .harnesses
                    .build(execution, receipt, environment, worktree)
                    .await?;
                harness
                    .recover_abandoned()
                    .await
                    .map_err(|error| LifecycleError::Step(error.to_string()))?;
            }
        }
        if runtime_present {
            let receipt = receipt.as_ref().ok_or_else(|| {
                LifecycleError::Step("runtime cleanup lacks storage receipt".into())
            })?;
            self.runtimes
                .build_for_cleanup(execution, receipt)
                .await?
                .destroy(&receipt.labels)
                .await
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
        }
        if let Some(worktree) = worktree {
            self.destroy_worktree(&worktree).await?;
        }
        self.recover_interrupted_worktree(execution, receipt.as_ref())
            .await?;
        if let Some(receipt) = receipt {
            self.release_storage(&receipt).await?;
        }
        Ok(())
    }

    async fn cleanup_authority_step(
        &self,
        authority: &CleanupAuthority,
        execution: &Execution,
        disposition: CleanupDisposition,
    ) -> Result<CleanupDisposition, LifecycleError> {
        let handles: DurableCleanupHandles = serde_json::from_value(authority.handles.clone())
            .map_err(|error| LifecycleError::Step(format!("decode cleanup authority: {error}")))?;
        let layout = ExecutionLayout::new(&self.state_root, &authority.execution_id)
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        match disposition {
            CleanupDisposition::CleanupPending => {
                if handles
                    .runtime_selector
                    .as_ref()
                    .is_some_and(|labels| labels != &execution.labels)
                {
                    return Err(LifecycleError::Step(
                        "runtime cleanup selector differs from execution authority".into(),
                    ));
                }
                if let (Some(receipt), Some(runtime), Some(worktree)) =
                    (handles.receipt, handles.runtime, handles.worktree)
                {
                    if receipt.labels.execution_id != authority.execution_id
                        || runtime.execution_id != authority.execution_id
                        || worktree.execution_id != authority.execution_id
                    {
                        return Err(LifecycleError::Step(
                            "Pi cleanup handles belong to another execution".into(),
                        ));
                    }
                    let environment = durable_environment(runtime);
                    let worktree = durable_worktree(worktree);
                    let harness = self
                        .harnesses
                        .build(execution, &receipt, &environment, &worktree)
                        .await?;
                    harness
                        .recover_abandoned()
                        .await
                        .map_err(|error| LifecycleError::Step(error.to_string()))?;
                }
                Ok(CleanupDisposition::RuntimeStopped)
            }
            CleanupDisposition::RuntimeStopped => {
                if let Some(receipt) = handles.receipt {
                    if receipt.labels.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "runtime cleanup receipt belongs to another execution".into(),
                        ));
                    }
                    let runtime_present = !docker_resource_names(
                        &self.docker_binary,
                        self.docker_socket.as_deref(),
                        &["ps", "-a"],
                        &authority.execution_id,
                        "{{.Names}}",
                    )?
                    .is_empty()
                        || !docker_resource_names(
                            &self.docker_binary,
                            self.docker_socket.as_deref(),
                            &["network", "ls"],
                            &authority.execution_id,
                            "{{.Name}}",
                        )?
                        .is_empty()
                        || !docker_resource_names(
                            &self.docker_binary,
                            self.docker_socket.as_deref(),
                            &["volume", "ls"],
                            &authority.execution_id,
                            "{{.Name}}",
                        )?
                        .is_empty();
                    if runtime_present {
                        self.runtimes
                            .build_for_cleanup(execution, &receipt)
                            .await?
                            .destroy(&receipt.labels)
                            .await
                            .map_err(|error| LifecycleError::Step(error.to_string()))?;
                    }
                }
                self.runtimes
                    .revoke_credentials(&authority.execution_id)
                    .await?;
                Ok(CleanupDisposition::RuntimeDestroyed)
            }
            CleanupDisposition::RuntimeDestroyed => {
                if let Some(worktree) = handles.worktree {
                    let worktree = durable_worktree(worktree);
                    if worktree.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "worktree cleanup handle belongs to another execution".into(),
                        ));
                    }
                    self.destroy_worktree(&worktree).await?;
                }
                let manager = Arc::clone(&self.worktrees);
                let labels = execution.labels.clone();
                tokio::task::spawn_blocking(move || {
                    manager.recover_interrupted_create(&labels, &layout.repository)
                })
                .await
                .map_err(|error| LifecycleError::Step(error.to_string()))?
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
                Ok(CleanupDisposition::GitRecoveredCleaned)
            }
            CleanupDisposition::GitRecoveredCleaned => {
                if let Some(receipt) = handles.receipt {
                    if receipt.labels.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "storage cleanup receipt belongs to another execution".into(),
                        ));
                    }
                    self.release_storage(&receipt).await?;
                }
                Ok(CleanupDisposition::StorageReleased)
            }
            other => Err(LifecycleError::Step(format!(
                "cleanup step cannot advance {other}"
            ))),
        }
    }

    async fn ack_cleanup_authority_step(
        &self,
        authority: &CleanupAuthority,
        disposition: CleanupDisposition,
    ) -> Result<(), LifecycleError> {
        let handles: DurableCleanupHandles = serde_json::from_value(authority.handles.clone())
            .map_err(|error| LifecycleError::Step(format!("decode cleanup authority: {error}")))?;
        match disposition {
            CleanupDisposition::GitRecoveredCleaned => {
                if let Some(worktree) = handles.worktree {
                    let worktree = durable_worktree(worktree);
                    if worktree.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "worktree cleanup acknowledgment belongs to another execution".into(),
                        ));
                    }
                    let manager = Arc::clone(&self.worktrees);
                    tokio::task::spawn_blocking(move || manager.ack_destroy(&worktree))
                        .await
                        .map_err(|error| LifecycleError::Step(error.to_string()))?
                        .map_err(|error| LifecycleError::Step(error.to_string()))?;
                }
            }
            CleanupDisposition::StorageReleased => {
                if let Some(receipt) = handles.receipt {
                    if receipt.labels.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "storage release acknowledgment belongs to another execution".into(),
                        ));
                    }
                    let storage = Arc::clone(&self.storage);
                    tokio::task::spawn_blocking(move || storage.ack_release(&receipt))
                        .await
                        .map_err(|error| LifecycleError::Step(error.to_string()))?
                        .map_err(|error| LifecycleError::Step(error.to_string()))?;
                }
            }
            CleanupDisposition::ReservationReleased => {
                if let Some(worktree) = handles.worktree {
                    let worktree = durable_worktree(worktree);
                    if worktree.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "worktree cleanup acknowledgment belongs to another execution".into(),
                        ));
                    }
                    let manager = Arc::clone(&self.worktrees);
                    tokio::task::spawn_blocking(move || manager.ack_destroy(&worktree))
                        .await
                        .map_err(|error| LifecycleError::Step(error.to_string()))?
                        .map_err(|error| LifecycleError::Step(error.to_string()))?;
                }
                if let Some(receipt) = handles.receipt {
                    if receipt.labels.execution_id != authority.execution_id {
                        return Err(LifecycleError::Step(
                            "storage release acknowledgment belongs to another execution".into(),
                        ));
                    }
                    let storage = Arc::clone(&self.storage);
                    tokio::task::spawn_blocking(move || storage.ack_release(&receipt))
                        .await
                        .map_err(|error| LifecycleError::Step(error.to_string()))?
                        .map_err(|error| LifecycleError::Step(error.to_string()))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn adopt(&self, execution: &Execution) -> Result<AdoptedExecution, LifecycleError> {
        let session_id = execution
            .session_id
            .clone()
            .ok_or_else(|| LifecycleError::Step("running execution lacks session id".into()))?;
        let layout = ExecutionLayout::new(&self.state_root, &execution.id)
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        if execution.worktree_path.as_deref() != Some(layout.repository.to_string_lossy().as_ref())
        {
            return Err(LifecycleError::Step(
                "persisted worktree path differs from allocation layout".into(),
            ));
        }
        let journal = JournalStore::new(&self.state_root)
            .and_then(|journals| journals.read(&layout))
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        let receipt = journal.receipt.ok_or_else(|| {
            LifecycleError::Step("storage journal lacks a durable allocation receipt".into())
        })?;
        if journal.phase != AllocationPhase::Ready
            || receipt.labels != execution.labels
            || receipt.mount_path != layout.root
        {
            return Err(LifecycleError::Step(
                "storage allocation is not the exact durable Ready authority".into(),
            ));
        }
        let ready = self
            .verifier
            .verify_ready(&receipt)
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        ready
            .verify()
            .map_err(|error| LifecycleError::Step(error.to_string()))?;

        let owner: DurableWorktreeOwner = serde_json::from_slice(
            &std::fs::read(layout.repository.join(".autospec-owner.json"))
                .map_err(|error| LifecycleError::Step(error.to_string()))?,
        )
        .map_err(|error| LifecycleError::Step(error.to_string()))?;
        let branch = execution
            .manifest
            .repository
            .branch
            .as_deref()
            .ok_or_else(|| LifecycleError::Step("execution lacks durable branch".into()))?;
        if owner.labels != execution.labels.to_map() || owner.branch != branch {
            return Err(LifecycleError::Step(
                "Git owner record differs from persisted execution authority".into(),
            ));
        }
        let worktree = Worktree {
            execution_id: execution.id.clone(),
            path: layout.repository.to_string_lossy().into_owned(),
            branch: owner.branch,
            base_sha: owner.base_sha,
            repository: execution.manifest.repository.repo.clone(),
        };

        let holds = ExecutionLifecycleHoldStore::new(&self.state_root)
            .and_then(|holds| holds.list(&execution.id))
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        let agent_container = DockerRuntime::agent_container_name(&execution.id);
        let container_id = docker_container_id(
            &self.docker_binary,
            self.docker_socket.as_deref(),
            &agent_container,
        )?;
        if holds
            .iter()
            .any(|hold| hold.labels != execution.labels || hold.container_id != container_id)
        {
            return Err(LifecycleError::Step(
                "restart adoption found a foreign Pi lifecycle hold".into(),
            ));
        }
        let container = docker_container_capability(
            &self.docker_binary,
            self.docker_socket.as_deref(),
            &receipt,
            &layout,
            &container_id,
            execution,
        )?;
        let credentials_path = container
            .mounts
            .iter()
            .find(|mount| mount.target == "/autospec-credential")
            .map(|mount| mount.source.clone());
        let environment = EnvironmentHandle {
            execution_id: execution.id.clone(),
            network: DockerRuntime::network_name(&execution.id),
            agent_container,
            verified_agent_container: container,
            service_containers: execution
                .manifest
                .services
                .iter()
                .map(|service| DockerRuntime::service_container_name(&execution.id, &service.name))
                .collect(),
            volumes: Vec::new(),
            credentials_path,
        };
        let runtime = self
            .runtimes
            .build_for_adoption(execution, &receipt, &environment)
            .await?;
        let harness = self
            .harnesses
            .build(execution, &receipt, &environment, &worktree)
            .await?;
        self.remember_and_validate_credentials(execution, &environment)?;
        let session = SessionRef {
            id: session_id,
            path: layout.session.to_string_lossy().into_owned(),
            execution_id: execution.id.clone(),
            worktree_path: layout.repository.to_string_lossy().into_owned(),
        };
        if !holds.is_empty() {
            harness
                .recover_abandoned()
                .await
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
        }
        let remaining_holds = ExecutionLifecycleHoldStore::new(&self.state_root)
            .and_then(|holds| holds.list(&execution.id))
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        if !remaining_holds.is_empty() {
            return Err(LifecycleError::Step(
                "restart adoption did not quiesce abandoned Pi authority".into(),
            ));
        }
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .insert(execution.id.clone(), runtime);
        self.active_harnesses
            .lock()
            .map_err(|_| LifecycleError::Step("harness registry lock poisoned".into()))?
            .insert(execution.id.clone(), harness);
        Ok(AdoptedExecution {
            receipt,
            worktree,
            environment,
            session,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        check_build_cache_role, execution_disk_bytes, parse_size, parse_stats, parse_top_command,
        persist_evidence_file, probe_resource_sample, scrub_current_command,
    };
    use orchestrator_core::{Role, RuntimeRequirement};
    use runtime_traits::build_cache::BUILD_CACHE_CAPABILITY;

    #[test]
    fn evidence_rejects_path_escape_and_existing_artifacts() {
        let root = std::env::temp_dir().join(format!(
            "autospec-worker-evidence-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();

        assert!(persist_evidence_file(&root, "../escape", "attempt", b"bad").is_err());
        let artifact = persist_evidence_file(&root, "execution", "attempt", b"first").unwrap();
        assert_eq!(std::fs::read(&artifact).unwrap(), b"first");
        assert!(persist_evidence_file(&root, "execution", "attempt", b"replace").is_err());
        assert_eq!(std::fs::read(&artifact).unwrap(), b"first");

        std::fs::remove_dir_all(root).unwrap();
    }

    fn requirement(capabilities: &[&str]) -> RuntimeRequirement {
        RuntimeRequirement {
            capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
            ..RuntimeRequirement::default()
        }
    }

    #[test]
    fn only_implementation_may_request_the_shared_build_cache() {
        let with_cache = requirement(&["docker", BUILD_CACHE_CAPABILITY]);
        assert!(check_build_cache_role(Role::Implementation, &with_cache).is_ok());
        for role in [
            Role::Review,
            Role::Documentation,
            Role::UiReview,
            Role::IntegrationTest,
            Role::Interactive,
        ] {
            assert!(
                check_build_cache_role(role, &with_cache).is_err(),
                "{role:?} must not get the cache"
            );
            assert!(check_build_cache_role(role, &requirement(&["docker"])).is_ok());
        }
    }

    #[derive(Debug)]
    struct StubVerifier;

    impl execution_storage::ReadyAllocationVerifier for StubVerifier {
        fn verify_ready(
            &self,
            _: &execution_storage::AllocationReceipt,
        ) -> Result<
            Box<dyn execution_storage::VerifiedExecutionStorage>,
            execution_storage::StorageError,
        > {
            Err(execution_storage::StorageError::Unavailable("stub".into()))
        }
    }

    fn factory() -> super::VerifiedDockerRuntimeFactory {
        super::VerifiedDockerRuntimeFactory::new(
            None,
            std::path::PathBuf::from("docker"),
            std::sync::Arc::new(StubVerifier),
            runtime_docker::TrustedVerifierImage::new(
                format!("sha256:{}", "a".repeat(64)),
                "/usr/bin/stat",
            )
            .unwrap(),
            std::sync::Arc::new(
                runtime_docker::LocalCredentialBroker::new(
                    std::env::temp_dir(),
                    chrono::Duration::minutes(15),
                )
                .unwrap(),
            ),
        )
    }

    #[test]
    fn the_factory_hardening_setting_reaches_every_runtime_it_configures() {
        use runtime_docker::ContainerHardening::{Enforced, Relaxed};
        let runtime =
            || runtime_docker::DockerRuntime::connect_with_state_root(None, "/tmp").unwrap();
        assert_eq!(factory().container_hardening(), Enforced);
        assert_eq!(factory().configure(runtime()).hardening(), Enforced);
        let relaxed = factory().with_container_hardening(Relaxed);
        assert_eq!(relaxed.container_hardening(), Relaxed);
        assert_eq!(relaxed.configure(runtime()).hardening(), Relaxed);
    }

    #[test]
    fn the_runtime_factory_build_cache_setting_reaches_the_docker_runtime() {
        let root = std::path::PathBuf::from("/var/cache/autospec-build");
        let plain = runtime_docker::DockerRuntime::connect_with_state_root(None, "/tmp").unwrap();
        assert!(super::apply_build_cache(plain, None)
            .build_cache()
            .is_none());
        let plain = runtime_docker::DockerRuntime::connect_with_state_root(None, "/tmp").unwrap();
        let configured = super::apply_build_cache(plain, Some(&(root.clone(), 40)));
        assert_eq!(configured.build_cache(), Some((root.as_path(), 40)));
    }

    #[test]
    fn a_command_containing_the_credential_is_dropped_but_counters_stay() {
        let mut sample = crate::ResourceSample {
            cpu_percent: 3.0,
            io_bytes: Some(9),
            current_command: Some("curl -H token=SECRET-TOKEN-VALUE".into()),
            ..Default::default()
        };
        scrub_current_command(&mut sample, |bytes| {
            super::contains_bytes(bytes, b"SECRET-TOKEN-VALUE")
        });
        assert_eq!(sample.current_command, None);
        assert_eq!(sample.io_bytes, Some(9));
        let mut clean = crate::ResourceSample {
            current_command: Some("cargo test".into()),
            ..Default::default()
        };
        scrub_current_command(&mut clean, |bytes| {
            super::contains_bytes(bytes, b"SECRET-TOKEN-VALUE")
        });
        assert_eq!(clean.current_command.as_deref(), Some("cargo test"));
    }

    fn scratch(base: &std::path::Path, name: &str) -> std::path::PathBuf {
        let dir = base.join(format!("autospec-disk-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn execution_disk_bytes_is_none_when_the_root_shares_the_state_root_device() {
        let dir = scratch(&std::env::temp_dir(), "same");
        assert_eq!(execution_disk_bytes(&dir, &dir), None);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn execution_disk_bytes_on_a_missing_root_is_none_without_panicking() {
        let state = scratch(&std::env::temp_dir(), "state");
        let missing = state.join("no-such-execution-root");
        assert_eq!(execution_disk_bytes(&missing, &state), None);
        assert_eq!(execution_disk_bytes(&state, &missing), None);
        std::fs::remove_dir_all(state).unwrap();
    }

    /// A tmpfs under /dev/shm is its own device, like a per-execution volume.
    #[test]
    fn execution_disk_bytes_reports_used_bytes_of_a_distinct_volume() {
        use std::os::unix::fs::MetadataExt;
        let shm = std::path::Path::new("/dev/shm");
        let state = scratch(&std::env::temp_dir(), "state2");
        if !shm.is_dir()
            || std::fs::metadata(shm).unwrap().dev() == std::fs::metadata(&state).unwrap().dev()
        {
            std::fs::remove_dir_all(state).unwrap();
            return;
        }
        let root = scratch(shm, "volume");
        let before = execution_disk_bytes(&root, &state).expect("distinct device yields a value");
        std::fs::write(root.join("blob"), vec![7u8; 8 * 1024 * 1024]).unwrap();
        let after = execution_disk_bytes(&root, &state).unwrap();
        assert!(
            after >= before + 8 * 1024 * 1024,
            "used bytes must grow by the file size: {before} -> {after}"
        );
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(state).unwrap();
    }

    const STATS_LINE: &str = r#"{"BlockIO":"1.5MB / 500kB","CPUPerc":"12.50%","Container":"c","ID":"x","MemPerc":"0.5%","MemUsage":"10.5MiB / 1.9GiB","Name":"c","NetIO":"0B / 0B","PIDs":"3"}"#;

    #[test]
    fn parse_stats_sums_block_io_read_and_write() {
        assert_eq!(parse_stats(STATS_LINE).unwrap().io_bytes, Some(2_000_000));
    }

    #[test]
    fn parse_stats_reads_cpu_percent_and_the_first_memory_term() {
        let sample = parse_stats(STATS_LINE).unwrap();
        assert_eq!(sample.cpu_percent, 12.5);
        assert_eq!(sample.rss_bytes, Some(11_010_048));
        assert_eq!(sample.current_command, None);
    }

    #[test]
    fn parse_stats_without_block_io_is_an_error_not_a_zero_counter() {
        let line = r#"{"CPUPerc":"1.00%","MemUsage":"1MiB / 2GiB"}"#;
        assert!(parse_stats(line).is_err());
    }

    #[test]
    fn parse_stats_rejects_malformed_fields() {
        for line in [
            "not json",
            r#"{"CPUPerc":"abc","BlockIO":"0B / 0B"}"#,
            r#"{"CPUPerc":"1%","BlockIO":"12 / 0B"}"#,
            r#"{"CPUPerc":"1%","BlockIO":"1MB"}"#,
            r#"{"CPUPerc":"1%","BlockIO":"1QB / 0B"}"#,
            r#"{"BlockIO":"0B / 0B"}"#,
            r#"{"CPUPerc":"1%","BlockIO":"0B / 0B","MemUsage":"x / y"}"#,
        ] {
            assert!(parse_stats(line).is_err(), "{line}");
        }
    }

    #[test]
    fn parse_size_knows_decimal_and_binary_units() {
        assert_eq!(parse_size("0B"), Some(0));
        assert_eq!(parse_size("500kB"), Some(500_000));
        assert_eq!(parse_size("12.3MB"), Some(12_300_000));
        assert_eq!(parse_size("2GiB"), Some(2_147_483_648));
        assert_eq!(parse_size("5"), None);
    }

    #[test]
    fn current_command_is_the_newest_process_truncated_to_200_chars() {
        let long = "x".repeat(300);
        let top = format!("PID   COMMAND\n10    sh -c loop\n  42    {long}\n7     init\n");
        let command = parse_top_command(&top).unwrap();
        assert_eq!(command.chars().count(), 200);
        assert!(command.starts_with("xxx"));
        assert_eq!(parse_top_command("PID COMMAND\n"), None);
    }

    fn docker_available() -> bool {
        std::process::Command::new("docker")
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .is_ok_and(|output| output.status.success())
    }

    struct TestContainer(String);

    impl Drop for TestContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.0])
                .output();
        }
    }

    /// Real-daemon check of the actual probe. Returns early without Docker
    /// (GitHub Actions has no daemon); the operator runs it on a Docker host.
    #[test]
    fn live_probe_sees_block_io_advance_and_the_current_command() {
        if !docker_available() {
            return;
        }
        let name = format!("autospec-resource-sample-test-{}", std::process::id());
        let started = std::process::Command::new("docker")
            .args([
                "run", "-d", "--name", &name, "--label", "autospec.test=resource-sample",
                "alpine:latest", "sh", "-c",
                "while true; do dd if=/dev/zero of=/tmp/f bs=1M count=64 oflag=direct conv=fsync 2>/dev/null; sleep 1; done",
            ])
            .output()
            .unwrap();
        if !started.status.success() {
            // Image not present and no network: nothing to probe.
            return;
        }
        let _guard = TestContainer(name.clone());
        let docker = std::path::Path::new("docker");
        let first = probe_resource_sample(docker, None, &name).unwrap();
        assert!(first.io_bytes.is_some(), "stats must report block I/O");
        // The cgroup block-I/O counter does not move for filesystem writes on
        // the ZFS or fuse-overlayfs storage drivers or under rootless Docker
        // (observed), so the advance is only asserted on a rootful block-device
        // driver.
        if docker_block_io_is_accounted() {
            let mut second = first.clone();
            for _ in 0..20 {
                std::thread::sleep(std::time::Duration::from_secs(1));
                second = probe_resource_sample(docker, None, &name).unwrap();
                if second.io_bytes > first.io_bytes {
                    break;
                }
            }
            assert!(
                second.io_bytes > first.io_bytes,
                "block I/O must advance: {first:?} -> {second:?}"
            );
        }
        let last = probe_resource_sample(docker, None, &name).unwrap();
        assert!(last.rss_bytes.is_some_and(|rss| rss > 0));
        assert!(last.cpu_percent >= 0.0);
        assert!(last
            .current_command
            .is_some_and(|command| !command.is_empty()));
    }

    fn docker_block_io_is_accounted() -> bool {
        let info = std::process::Command::new("docker")
            .args(["info", "--format", "{{.Driver}} {{.SecurityOptions}}"])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
            .unwrap_or_default();
        info.starts_with("overlay2") && !info.contains("rootless")
    }

    #[test]
    fn probing_a_missing_container_is_an_error_not_a_flat_counter() {
        if !docker_available() {
            return;
        }
        let result = probe_resource_sample(
            std::path::Path::new("docker"),
            None,
            "autospec-no-such-container-for-resource-sample",
        );
        assert!(result.is_err());
    }
}
