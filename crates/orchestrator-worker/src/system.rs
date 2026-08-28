use crate::{AdoptedExecution, ExecutionLifecycle, LifecycleError};
use async_trait::async_trait;
use execution_storage::{
    AllocationPhase, AllocationReceipt, AllocationRequest, ExecutionLayout,
    ExecutionLifecycleHoldStore, ExecutionStorageManager, JournalStore, ReadyAllocationVerifier,
};
use git_worktree::{DiffCapture, Worktree, WorktreeManager};
use harness_pi::{PiHarness, PiHarnessConfig};
use harness_traits::{AgentHarness, SessionRef};
use orchestrator_core::{
    Execution, ExecutionEvent, ExecutionId, OwnershipLabels, SessionId, TaskPacket,
};
use orchestrator_persistence::CleanupAuthority;
use runtime_docker::{DockerRuntime, TrustedVerifierImage};
use runtime_traits::{EnvironmentHandle, Runtime, VerifiedAgentContainer, VerifiedBindMount};
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
    async fn cpu_percent(&self, execution: &Execution) -> Result<f64, LifecycleError>;
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

#[derive(Debug, Clone)]
pub struct VerifiedDockerRuntimeFactory {
    socket: Option<String>,
    docker_binary: PathBuf,
    verifier: Arc<dyn ReadyAllocationVerifier>,
    trusted_verifier: TrustedVerifierImage,
}

impl VerifiedDockerRuntimeFactory {
    pub fn new(
        socket: Option<String>,
        docker_binary: PathBuf,
        verifier: Arc<dyn ReadyAllocationVerifier>,
        trusted_verifier: TrustedVerifierImage,
    ) -> Self {
        Self {
            socket,
            docker_binary,
            verifier,
            trusted_verifier,
        }
    }
}

#[async_trait]
impl RuntimeFactory for VerifiedDockerRuntimeFactory {
    async fn build(
        &self,
        _: &Execution,
        receipt: &AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        DockerRuntime::connect_with_verified_execution_storage(
            self.socket.as_deref(),
            Arc::clone(&self.verifier),
            receipt.clone(),
            self.trusted_verifier.clone(),
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
}

#[derive(Debug, Clone)]
pub struct VerifiedPiHarnessFactory {
    verifier: Arc<dyn ReadyAllocationVerifier>,
    docker_socket: Option<String>,
    docker_binary: PathBuf,
    pi_executable: String,
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
            tools,
            skills,
        }
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
        config.tools = self.tools.clone();
        config.skills = self.skills.clone();
        config.model_policy = Some(execution.manifest.agent.model_policy.clone());
        Ok(Arc::new(PiHarness::new(config)))
    }
}

#[derive(Debug, Clone)]
pub struct FilesystemEvidenceStore {
    root: PathBuf,
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
        let environment = runtime
            .provision(
                &execution.labels,
                &execution.manifest.runtime,
                &execution.manifest.services,
            )
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .insert(execution.id.clone(), runtime);
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
        _: &EnvironmentHandle,
        session: &SessionRef,
    ) -> Result<(), LifecycleError> {
        self.harness(&execution.id)?
            .resume(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn poll(
        &self,
        execution: &Execution,
        session: &SessionRef,
    ) -> Result<Vec<ExecutionEvent>, LifecycleError> {
        self.harness(&execution.id)?
            .poll_events(session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn cpu_percent(&self, execution: &Execution) -> Result<f64, LifecycleError> {
        self.runtimes.cpu_percent(execution).await
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
        tokio::task::spawn_blocking(move || manager.capture_diff(&worktree))
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?
            .map_err(|error| LifecycleError::Step(error.to_string()))
    }

    async fn persist_evidence(
        &self,
        execution: &Execution,
        capture: &DiffCapture,
    ) -> Result<String, LifecycleError> {
        self.evidence.persist(execution, capture).await
    }

    async fn destroy_runtime(&self, execution: &Execution) -> Result<(), LifecycleError> {
        let runtime = self.runtime(&execution.id)?;
        runtime
            .destroy(&execution.labels)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
        self.active_runtimes
            .lock()
            .map_err(|_| LifecycleError::Step("runtime registry lock poisoned".into()))?
            .remove(&execution.id);
        Ok(())
    }

    async fn destroy_worktree(&self, worktree: &Worktree) -> Result<(), LifecycleError> {
        let manager = Arc::clone(&self.worktrees);
        let worktree = worktree.clone();
        tokio::task::spawn_blocking(move || manager.destroy(&worktree))
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
                .build(execution, receipt)
                .await?
                .destroy(&receipt.labels)
                .await
                .map_err(|error| LifecycleError::Step(error.to_string()))?;
        }
        if let Some(worktree) = worktree {
            self.destroy_worktree(&worktree).await?;
        }
        if let Some(receipt) = receipt {
            self.release_storage(&receipt).await?;
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
        let matching: Vec<_> = holds
            .into_iter()
            .filter(|hold| {
                hold.labels == execution.labels && hold.session_id == session_id.as_str()
            })
            .collect();
        if matching.len() != 1 {
            return Err(LifecycleError::Step(format!(
                "restart adoption requires exactly one matching Pi lifecycle hold, found {}",
                matching.len()
            )));
        }
        let container_id = matching[0].container_id.clone();
        let container = docker_container_capability(
            &self.docker_binary,
            self.docker_socket.as_deref(),
            &receipt,
            &layout,
            &container_id,
            execution,
        )?;
        let environment = EnvironmentHandle {
            execution_id: execution.id.clone(),
            network: DockerRuntime::network_name(&execution.id),
            agent_container: DockerRuntime::agent_container_name(&execution.id),
            verified_agent_container: container,
            service_containers: execution
                .manifest
                .services
                .iter()
                .map(|service| DockerRuntime::service_container_name(&execution.id, &service.name))
                .collect(),
            volumes: Vec::new(),
            credentials_path: None,
        };
        let runtime = self.runtimes.build(execution, &receipt).await?;
        let harness = self
            .harnesses
            .build(execution, &receipt, &environment, &worktree)
            .await?;
        let session = SessionRef {
            id: session_id,
            path: layout.session.to_string_lossy().into_owned(),
            execution_id: execution.id.clone(),
            worktree_path: layout.repository.to_string_lossy().into_owned(),
        };
        harness
            .resume(&session)
            .await
            .map_err(|error| LifecycleError::Step(error.to_string()))?;
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
    use super::persist_evidence_file;

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
}
