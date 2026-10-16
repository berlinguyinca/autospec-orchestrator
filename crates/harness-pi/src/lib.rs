//! Pi 0.84.3 agent harness (spec sections 22, 38, 39, 41, 79, 95).

mod events;
mod resume;
mod session;
mod version;

use async_trait::async_trait;
use execution_storage::{
    AllocationReceipt, ExecutionLayout, ExecutionLifecycleHoldStore, ReadyAllocationVerifier,
    ReadyLease, VerifiedExecutionStorage,
};
use harness_traits::{AgentHarness, HarnessError, SessionRef};
use orchestrator_core::{ExecutionEvent, ModelPolicy, OwnershipLabels, TaskPacket};
use runtime_traits::{build_cache, VerifiedAgentContainer, VerifiedBindMount};
use std::{
    collections::HashMap,
    path::PathBuf,
    process::{Child, Command},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
pub use version::{check_pi_version, parse_pi_version, TESTED_PI_VERSIONS};

#[derive(Debug, Clone)]
pub struct PiHarnessConfig {
    pub state_root: PathBuf,
    pub worktree: PathBuf,
    pub docker_binary: PathBuf,
    /// Optional Docker CLI endpoint propagated to every inspect, exec, stats,
    /// signal, and recovery command without mutating process-global state.
    pub docker_host: Option<String>,
    pub agent_container: String,
    pub pi_executable: String,
    /// Permits a Pi release outside [`TESTED_PI_VERSIONS`] (logged as a warning).
    /// Off by default; an unparseable `pi --version` is refused regardless.
    pub allow_untested_pi_version: bool,
    pub labels: OwnershipLabels,
    /// AutoSpec's model policy, carried into Pi without choosing an alternative.
    pub model_policy: Option<ModelPolicy>,
    /// Exact Pi tool allowlist. Empty means Pi receives `--no-tools`.
    pub tools: Vec<String>,
    /// Additional explicit skill paths. Package discovery remains disabled.
    pub skills: Vec<PathBuf>,
    /// Exact in-memory values removed before Pi output reaches durable JSONL.
    pub credential_redactions: Vec<Vec<u8>>,
    pub stop_timeout: Duration,
    /// `uid:gid` the harness's process-group probes and signals run as inside
    /// the agent container. It must be the user the container's processes run
    /// as: a process may signal its own uid without `CAP_KILL`, which a
    /// `cap_drop ALL` container no longer has. Default `0:0` matches the
    /// default agent container user. Validated by [`validate_agent_user`].
    pub agent_user: String,
    storage: Option<VerifiedAllocationConfig>,
}

/// Accepts only a numeric `uid:gid` (for example `1000:1000`).
///
/// User names are refused: a name resolves through the container's
/// `/etc/passwd`, which the agent can influence, and an empty or malformed
/// value would make `docker exec --user` fall back to the image default.
pub fn validate_agent_user(raw: &str) -> Result<(), HarnessError> {
    let numeric = |part: &str| {
        !part.is_empty()
            && part.len() <= 10
            && part.bytes().all(|byte| byte.is_ascii_digit())
            && part.parse::<u32>().is_ok()
    };
    match raw.split_once(':') {
        Some((uid, gid)) if numeric(uid) && numeric(gid) => Ok(()),
        _ => Err(HarnessError::Start(format!(
            "agent user {raw:?} must be a numeric uid:gid such as 1000:1000"
        ))),
    }
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
            docker_host: None,
            agent_container,
            pi_executable: "pi".to_owned(),
            allow_untested_pi_version: false,
            labels,
            model_policy: None,
            tools: vec![
                "read".to_owned(),
                "bash".to_owned(),
                "edit".to_owned(),
                "write".to_owned(),
            ],
            skills: Vec::new(),
            credential_redactions: Vec::new(),
            stop_timeout: Duration::from_secs(10),
            agent_user: "0:0".to_owned(),
            storage: None,
        }
    }

    /// Sets the agent user after validating it (see [`validate_agent_user`]).
    pub fn with_agent_user(mut self, user: &str) -> Result<Self, HarnessError> {
        validate_agent_user(user)?;
        self.agent_user = user.to_owned();
        Ok(self)
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
            docker_host: None,
            agent_container,
            pi_executable: "pi".to_owned(),
            allow_untested_pi_version: false,
            labels,
            model_policy: None,
            tools: vec![
                "read".to_owned(),
                "bash".to_owned(),
                "edit".to_owned(),
                "write".to_owned(),
            ],
            skills: Vec::new(),
            credential_redactions: Vec::new(),
            stop_timeout: Duration::from_secs(10),
            agent_user: "0:0".to_owned(),
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
    docker: DockerCli,
    agent_container: String,
    stop_timeout: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct DockerCli {
    binary: PathBuf,
    host: Option<String>,
    agent_user: String,
}

impl DockerCli {
    pub(crate) fn new(binary: PathBuf, host: Option<String>, agent_user: String) -> Self {
        Self {
            binary,
            host,
            agent_user,
        }
    }

    /// The `uid:gid` every probe and signal runs as inside the agent container.
    pub(crate) fn agent_user(&self) -> &str {
        &self.agent_user
    }

    pub(crate) fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        if let Some(host) = &self.host {
            command.env("DOCKER_HOST", host);
        }
        command
    }
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
                &self.docker,
                &self.agent_container,
                process,
                self.stop_timeout,
            ) {
                session::quarantine_registered(
                    self.docker.clone(),
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
        let docker = DockerCli::new(
            config.docker_binary.clone(),
            config.docker_host.clone(),
            config.agent_user.clone(),
        );
        let agent_container = config.agent_container.clone();
        let stop_timeout = config.stop_timeout;
        Self {
            config,
            processes: Arc::new(ProcessRegistry {
                children: Mutex::new(HashMap::new()),
                docker,
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
        validate_agent_user(&self.config.agent_user)?;
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
    validate_mount_sources(&container.mounts, &root, &session_root, &conversation)
}

/// Every bind must sit inside the execution root, with two exceptions that are
/// checked, not trusted: the harness never sees private session state, and the
/// opt-in shared build cache (one writable `/sccache` bind) is accepted only
/// when its source is a real directory holding a managed owner file.
fn validate_mount_sources(
    mounts: &[VerifiedBindMount],
    root: &std::path::Path,
    session_root: &std::path::Path,
    conversation: &std::path::Path,
) -> Result<(), HarnessError> {
    let mut targets = std::collections::BTreeSet::new();
    for mount in mounts {
        let shared_build_cache = mount.target == build_cache::CONTAINER_BUILD_CACHE
            && mount.writable
            && build_cache::is_managed_build_cache_dir(&mount.source);
        if (!mount.source.starts_with(root) && !shared_build_cache)
            || !targets.insert(&mount.target)
            || (mount.source.starts_with(session_root) && mount.source != conversation)
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

    async fn resume_interactive(&self, session: &SessionRef) -> Result<(), HarnessError> {
        resume::resume_interactive(self, session)
    }

    async fn recover_abandoned(&self) -> Result<(), HarnessError> {
        let _storage = session::prepare_launch(self)?;
        Ok(())
    }

    async fn fork_conversation(&self, session: &SessionRef) -> Result<SessionRef, HarnessError> {
        resume::fork_conversation(self, session)
    }

    async fn fork_conversation_as(
        &self,
        session: &SessionRef,
        target: &orchestrator_core::SessionId,
    ) -> Result<SessionRef, HarnessError> {
        resume::fork_conversation_as(self, session, target)
    }

    async fn poll_events(&self, session: &SessionRef) -> Result<Vec<ExecutionEvent>, HarnessError> {
        events::poll_events(self, session)
    }

    async fn stop(&self, session: &SessionRef) -> Result<(), HarnessError> {
        session::stop(self, session).await
    }
}

#[cfg(test)]
mod build_cache_mount_tests {
    use super::*;
    use std::path::PathBuf;

    struct Fixture {
        _dir: tempfile::TempDir,
        root: PathBuf,
        session: PathBuf,
        conversation: PathBuf,
        cache: PathBuf,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(dir.path()).unwrap();
        let root = base.join("exec");
        let session = root.join("session");
        let conversation = session.join("conversation");
        let cache = base.join("cache/sccache/k");
        std::fs::create_dir_all(&conversation).unwrap();
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::write(
            cache.join(build_cache::OWNER_FILE),
            br#"{"managed":true,"key":"k"}"#,
        )
        .unwrap();
        Fixture {
            _dir: dir,
            root,
            session,
            conversation,
            cache,
        }
    }

    fn bind(source: &std::path::Path, target: &str, writable: bool) -> VerifiedBindMount {
        VerifiedBindMount {
            source: source.to_path_buf(),
            target: target.to_owned(),
            writable,
        }
    }

    fn check(f: &Fixture, mounts: &[VerifiedBindMount]) -> Result<(), HarnessError> {
        validate_mount_sources(mounts, &f.root, &f.session, &f.conversation)
    }

    #[test]
    fn a_managed_shared_cache_bind_outside_the_execution_root_is_accepted() {
        let f = fixture();
        let session = bind(&f.conversation, "/session", true);
        assert!(check(&f, &[session.clone(), bind(&f.cache, "/sccache", true)]).is_ok());
    }

    #[test]
    fn an_unmarked_read_only_or_misplaced_outside_bind_is_refused() {
        let f = fixture();
        let session = bind(&f.conversation, "/session", true);
        std::fs::remove_file(f.cache.join(build_cache::OWNER_FILE)).unwrap();
        assert!(check(&f, &[session.clone(), bind(&f.cache, "/sccache", true)]).is_err());
        std::fs::write(
            f.cache.join(build_cache::OWNER_FILE),
            br#"{"managed":true}"#,
        )
        .unwrap();
        assert!(check(&f, &[session.clone(), bind(&f.cache, "/sccache", false)]).is_err());
        // A managed directory mounted anywhere but /sccache gets no exemption.
        assert!(check(&f, &[session, bind(&f.cache, "/elsewhere", true)]).is_err());
    }
}

#[cfg(test)]
mod agent_user_tests {
    use super::*;
    use orchestrator_core::{ExecutionId, WorkerId};

    #[test]
    fn numeric_uid_gid_is_accepted() {
        for ok in ["1000:1000", "0:0", "65534:65534", "4294967295:0"] {
            assert!(validate_agent_user(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn names_and_malformed_values_are_refused() {
        for bad in [
            "root",
            "",
            "1000",
            "1000:",
            ":1000",
            "1000:1000:1",
            "root:root",
            "1000:wheel",
            " 1000:1000",
            "1000:1000 ",
            "-1:0",
            "+1:0",
            "4294967296:0",
            "00000000000:0",
            "1,0:1",
        ] {
            assert!(validate_agent_user(bad).is_err(), "{bad:?}");
        }
    }

    fn config() -> PiHarnessConfig {
        PiHarnessConfig::for_execution(
            "/nonexistent/state".into(),
            "/nonexistent/worktree".into(),
            "agent".to_owned(),
            OwnershipLabels {
                execution_id: ExecutionId::new("exec-agent-user"),
                worker_id: WorkerId::new("worker"),
                repository: "repo".to_owned(),
                issue: None,
            },
        )
    }

    #[test]
    fn default_is_root_so_behaviour_is_unchanged_until_the_runtime_switches() {
        assert_eq!(config().agent_user, "0:0");
        assert_eq!(
            PiHarness::new(config()).processes.docker.agent_user(),
            "0:0"
        );
    }

    #[test]
    fn builder_validates_and_the_value_reaches_the_docker_context() {
        assert!(config().with_agent_user("root").is_err());
        let harness = PiHarness::new(config().with_agent_user("1000:1000").unwrap());
        assert_eq!(harness.config().agent_user, "1000:1000");
        assert_eq!(harness.processes.docker.agent_user(), "1000:1000");
    }

    #[test]
    fn an_unvalidated_field_is_still_refused_before_any_docker_work() {
        let mut bad = config();
        bad.agent_user = "root".to_owned();
        let error = PiHarness::new(bad).acquire_ready_storage().err().unwrap();
        assert!(error.to_string().contains("uid:gid"), "{error}");
    }
}
