//! Pi 0.84.3 agent harness (spec sections 22, 38, 39, 41, 79, 95).

mod events;
mod resume;
mod session;

use async_trait::async_trait;
use execution_storage::{
    AllocationReceipt, ExecutionLayout, ExecutionLifecycleHoldStore, ReadyAllocationVerifier,
    ReadyLease, VerifiedExecutionStorage,
};
use harness_traits::{AgentHarness, HarnessError, SessionRef};
use orchestrator_core::{ExecutionEvent, ModelPolicy, OwnershipLabels, TaskPacket};
use runtime_traits::VerifiedAgentContainer;
use std::{
    collections::HashMap,
    path::PathBuf,
    process::Child,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct PiHarnessConfig {
    pub state_root: PathBuf,
    pub worktree: PathBuf,
    pub docker_binary: PathBuf,
    pub agent_container: String,
    pub pi_executable: String,
    pub labels: OwnershipLabels,
    /// AutoSpec's model policy, carried into Pi without choosing an alternative.
    pub model_policy: Option<ModelPolicy>,
    /// Exact Pi tool allowlist. Empty means Pi receives `--no-tools`.
    pub tools: Vec<String>,
    /// Additional explicit skill paths. Package discovery remains disabled.
    pub skills: Vec<PathBuf>,
    pub stop_timeout: Duration,
    storage: Option<VerifiedAllocationConfig>,
}

#[derive(Debug, Clone)]
struct VerifiedAllocationConfig {
    verifier: Arc<dyn ReadyAllocationVerifier>,
    receipt: AllocationReceipt,
    layout: ExecutionLayout,
    container: VerifiedAgentContainer,
}

impl PiHarnessConfig {
    /// Creates an unverified legacy configuration for non-execution uses.
    ///
    /// `PiHarness::start` deliberately rejects this configuration because the
    /// paths are not backed by a live Ready allocation capability.
    pub fn for_execution(
        state_root: PathBuf,
        worktree: PathBuf,
        agent_container: String,
        labels: OwnershipLabels,
    ) -> Self {
        Self {
            state_root,
            worktree,
            docker_binary: PathBuf::from("docker"),
            agent_container,
            pi_executable: "pi".to_owned(),
            labels,
            model_policy: None,
            tools: vec![
                "read".to_owned(),
                "bash".to_owned(),
                "edit".to_owned(),
                "write".to_owned(),
            ],
            skills: Vec::new(),
            stop_timeout: Duration::from_secs(10),
            storage: None,
        }
    }

    /// Binds Pi to one exact Ready execution allocation.
    pub fn for_ready_allocation(
        verifier: Arc<dyn ReadyAllocationVerifier>,
        receipt: AllocationReceipt,
        container: VerifiedAgentContainer,
    ) -> Result<Self, HarnessError> {
        let state_root = receipt
            .mount_path
            .parent()
            .and_then(|executions| executions.parent())
            .ok_or_else(|| {
                HarnessError::Start(
                    "execution allocation mount path lacks a deterministic state root".to_owned(),
                )
            })?
            .to_path_buf();
        let layout = ExecutionLayout::new(&state_root, &receipt.labels.execution_id)
            .map_err(storage_error)?;
        receipt
            .validate(&receipt.labels, &layout)
            .map_err(storage_error)?;
        let labels = receipt.labels.clone();
        let worktree = layout.repository.clone();
        let agent_container = container.container_id.clone();
        Ok(Self {
            state_root,
            worktree,
            docker_binary: PathBuf::from("docker"),
            agent_container,
            pi_executable: "pi".to_owned(),
            labels,
            model_policy: None,
            tools: vec![
                "read".to_owned(),
                "bash".to_owned(),
                "edit".to_owned(),
                "write".to_owned(),
            ],
            skills: Vec::new(),
            stop_timeout: Duration::from_secs(10),
            storage: Some(VerifiedAllocationConfig {
                verifier,
                receipt,
                layout,
                container,
            }),
        })
    }
}

#[derive(Debug)]
struct ProcessRegistry {
    children: Mutex<HashMap<String, Arc<ManagedProcess>>>,
    docker_binary: PathBuf,
    agent_container: String,
    stop_timeout: Duration,
}

#[derive(Debug)]
struct ManagedProcess {
    child: Mutex<Child>,
    pgid: u32,
    supervisor_token: String,
    lifecycle_holds: ExecutionLifecycleHoldStore,
    hold_id: String,
    hold_execution_id: orchestrator_core::ExecutionId,
    _storage_lease: Box<dyn ReadyLease>,
}

pub(crate) struct ReadyPiStorage {
    pub(crate) layout: ExecutionLayout,
    pub(crate) lease: Box<dyn ReadyLease>,
}

impl Drop for ProcessRegistry {
    fn drop(&mut self) {
        let Ok(children) = self.children.get_mut() else {
            return;
        };
        for process in children.values() {
            if !session::terminate_on_drop(
                &self.docker_binary,
                &self.agent_container,
                process,
                self.stop_timeout,
            ) {
                session::quarantine_registered(
                    self.docker_binary.clone(),
                    self.agent_container.clone(),
                    Arc::clone(process),
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PiHarness {
    config: PiHarnessConfig,
    processes: Arc<ProcessRegistry>,
    unknown_events: Arc<AtomicU64>,
}

impl PiHarness {
    pub fn new(config: PiHarnessConfig) -> Self {
        let docker_binary = config.docker_binary.clone();
        let agent_container = config.agent_container.clone();
        let stop_timeout = config.stop_timeout;
        Self {
            config,
            processes: Arc::new(ProcessRegistry {
                children: Mutex::new(HashMap::new()),
                docker_binary,
                agent_container,
                stop_timeout,
            }),
            unknown_events: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn config(&self) -> &PiHarnessConfig {
        &self.config
    }

    pub fn unknown_event_count(&self) -> u64 {
        self.unknown_events.load(Ordering::Relaxed)
    }

    pub(crate) fn acquire_ready_storage(&self) -> Result<ReadyPiStorage, HarnessError> {
        let storage = self.config.storage.as_ref().ok_or_else(|| {
            HarnessError::Start(
                "Pi execution requires an exact Ready execution-storage allocation".to_owned(),
            )
        })?;
        let expected =
            ExecutionLayout::new(&self.config.state_root, &self.config.labels.execution_id)
                .map_err(storage_error)?;
        storage
            .receipt
            .validate(&self.config.labels, &expected)
            .map_err(storage_error)?;
        if storage.layout != expected
            || storage.receipt.labels != self.config.labels
            || self.config.worktree != expected.repository
            || self.config.agent_container != storage.container.container_id
        {
            return Err(HarnessError::Start(
                "Pi configuration does not exactly match its execution allocation".to_owned(),
            ));
        }
        validate_container_capability(&storage.container, &storage.receipt, &expected)?;

        let verified = storage
            .verifier
            .verify_ready(&storage.receipt)
            .map_err(storage_error)?;
        verify_storage_paths(verified.as_ref(), &expected)?;
        let lease = storage
            .verifier
            .acquire_ready_lease(&storage.receipt)
            .map_err(storage_error)?;
        verify_storage_paths(lease.verified(), &expected)?;
        Ok(ReadyPiStorage {
            layout: expected,
            lease,
        })
    }

    pub(crate) fn container_capability(&self) -> Result<&VerifiedAgentContainer, HarnessError> {
        self.config
            .storage
            .as_ref()
            .map(|storage| &storage.container)
            .ok_or_else(|| {
                HarnessError::Start(
                    "Pi execution requires an exact runtime-issued agent container capability"
                        .to_owned(),
                )
            })
    }

    pub(crate) fn validate_session(&self, session: &SessionRef) -> Result<(), HarnessError> {
        let storage = self.config.storage.as_ref().ok_or_else(|| {
            HarnessError::InvalidSession(
                "Pi session has no verified execution allocation".to_owned(),
            )
        })?;
        if session.execution_id != self.config.labels.execution_id
            || session.path != storage.layout.session.display().to_string()
            || session.worktree_path != storage.layout.repository.display().to_string()
        {
            return Err(HarnessError::InvalidSession(
                "Pi session paths do not match the verified execution layout".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_container_capability(
    container: &VerifiedAgentContainer,
    receipt: &AllocationReceipt,
    layout: &ExecutionLayout,
) -> Result<(), HarnessError> {
    if container.container_id.is_empty()
        || container.daemon_id != receipt.docker_bind.daemon_id
        || container.labels != receipt.labels
    {
        return Err(HarnessError::Start(
            "agent container capability does not match allocation identity".to_owned(),
        ));
    }
    let root = std::fs::canonicalize(&layout.root).map_err(|error| {
        HarnessError::Start(format!(
            "canonicalize execution root for container proof: {error}"
        ))
    })?;
    let repository = std::fs::canonicalize(&layout.repository).map_err(|error| {
        HarnessError::Start(format!(
            "canonicalize worktree for container proof: {error}"
        ))
    })?;
    let session_root = std::fs::canonicalize(&layout.session).map_err(|error| {
        HarnessError::Start(format!(
            "canonicalize private session root for container proof: {error}"
        ))
    })?;
    let conversation = std::fs::canonicalize(&layout.conversation).map_err(|error| {
        HarnessError::Start(format!(
            "canonicalize conversation for container proof: {error}"
        ))
    })?;
    let workspace = container.mounts.iter().filter(|mount| {
        mount.target == session::CONTAINER_WORKTREE && mount.source == repository && mount.writable
    });
    let session = container.mounts.iter().filter(|mount| {
        mount.target == session::CONTAINER_SESSION && mount.source == conversation && mount.writable
    });
    if workspace.count() != 1 || session.count() != 1 {
        return Err(HarnessError::Start(
            "agent container capability lacks exact writable execution binds".to_owned(),
        ));
    }
    let mut targets = std::collections::BTreeSet::new();
    for mount in &container.mounts {
        if !mount.source.starts_with(&root)
            || !targets.insert(&mount.target)
            || (mount.source.starts_with(&session_root) && mount.source != conversation)
            || mount.target == "/var/run/docker.sock"
            || mount.source == std::path::Path::new("/var/run/docker.sock")
        {
            return Err(HarnessError::Start(
                "agent container capability exposes an unverified host path".to_owned(),
            ));
        }
    }
    Ok(())
}

fn verify_storage_paths(
    verified: &dyn VerifiedExecutionStorage,
    layout: &ExecutionLayout,
) -> Result<(), HarnessError> {
    verified.verify().map_err(storage_error)?;
    if verified.repository_path() != layout.repository {
        return Err(HarnessError::Start(
            "verified repository path differs from the execution layout".to_owned(),
        ));
    }
    for path in [&layout.repository, &layout.session, &layout.conversation] {
        verified.verify_directory(path).map_err(storage_error)?;
    }
    Ok(())
}

fn storage_error(error: execution_storage::StorageError) -> HarnessError {
    HarnessError::Start(format!("execution storage verification failed: {error}"))
}

#[async_trait]
impl AgentHarness for PiHarness {
    fn name(&self) -> &'static str {
        "pi"
    }

    async fn start(&self, packet: &TaskPacket) -> Result<SessionRef, HarnessError> {
        session::start(self, packet)
    }

    async fn resume(&self, session: &SessionRef) -> Result<(), HarnessError> {
        resume::resume(self, session)
    }

    async fn fork_conversation(&self, session: &SessionRef) -> Result<SessionRef, HarnessError> {
        resume::fork_conversation(self, session)
    }

    async fn poll_events(&self, session: &SessionRef) -> Result<Vec<ExecutionEvent>, HarnessError> {
        events::poll_events(self, session)
    }

    async fn stop(&self, session: &SessionRef) -> Result<(), HarnessError> {
        session::stop(self, session).await
    }
}
