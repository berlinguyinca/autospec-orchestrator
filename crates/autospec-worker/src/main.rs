//! `autospec-worker` — authenticated worker registration and heartbeat daemon.

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use disk_budget::{DiskPolicy, DiskVerdict, Sampler};
use execution_storage::{
    ApfsBackend, DockerBindCapability, DockerBindProof, DockerBindVerifier, ExecutionStorage,
    ExecutionStorageManager, LvmBackend, ProcessCommandRunner, StorageBackend, StorageError,
};
use git_worktree::GitWorktreeManager;
use orchestrator_core::{
    ExecutionId, OwnershipLabels, RuntimeKind, WorkerAdvertisement, WorkerCapabilities,
    WorkerCapabilityProof, WorkerId, API_VERSION,
};
use orchestrator_persistence::{
    ExecutionStore, PgArtifactStore, PgCleanupAuthorityStore, PgExecutionStore, PgReservationStore,
    ReservationStore,
};
use orchestrator_worker::{
    ContentAddressedEvidenceStore, ExecutionTask, SystemExecutionLifecycle, SystemRecoveryConfig,
    VerifiedDockerRuntimeFactory, VerifiedPiHarnessFactory, Worker,
};
use runtime_docker::LocalCredentialBroker;
use runtime_docker::TrustedVerifierImage;
use runtime_traits::build_cache::BUILD_CACHE_CAPABILITY;
use runtime_traits::CredentialBroker;
use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

mod disk_budget;

struct Secret(String);

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StorageKind {
    Apfs,
    Lvm,
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlDisposition {
    Registered,
    ReRegister,
    Retry,
}

fn control_disposition(registered: bool, status: reqwest::StatusCode) -> ControlDisposition {
    if status.is_success() {
        ControlDisposition::Registered
    } else if registered && status == reqwest::StatusCode::NOT_FOUND {
        ControlDisposition::ReRegister
    } else {
        ControlDisposition::Retry
    }
}

#[derive(Parser)]
#[command(name = "autospec-worker", version, about)]
struct Cli {
    #[arg(
        long,
        env = "AUTOSPEC_ORCHESTRATOR_URL",
        default_value = "http://127.0.0.1:8420"
    )]
    controller: String,
    #[arg(long, env = "AUTOSPEC_WORKER_ID")]
    worker_id: String,
    #[arg(long, env = "AUTOSPEC_WORKER_TOKEN")]
    worker_token: String,
    #[arg(long, env = "AUTOSPEC_WORKER_CONCURRENCY", default_value_t = 2)]
    concurrency: u32,
    #[arg(long, default_value_t = 1024)]
    memory_mib: u64,
    #[arg(long, default_value_t = 20)]
    disk_gib: u64,
    #[arg(long, value_enum)]
    storage_kind: StorageKind,
    /// APFS probe path or LVM volume-group name; its identity is probed, not advertised verbatim.
    #[arg(long)]
    storage_pool: String,
    /// Immutable verifier image ID (`sha256:<64 hex>`).
    #[arg(long)]
    docker_verifier_image: String,
    #[arg(long, default_value = "/usr/bin/stat")]
    docker_verifier_command: String,
    #[arg(long, default_value = "docker")]
    docker_binary: String,
    #[arg(long, env = "AUTOSPEC_DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "AUTOSPEC_STATE_ROOT", default_value = "/var/lib/autospec")]
    state_root: PathBuf,
    /// State-root usage (bytes or inodes) at which a warning is logged.
    #[arg(long, default_value_t = 80)]
    disk_warn_percent: u8,
    /// State-root usage (bytes or inodes) at which new executions stop being admitted.
    #[arg(long, default_value_t = 90)]
    disk_stop_percent: u8,
    /// Free state-root space below which new executions stop being admitted (0 disables).
    #[arg(long, default_value_t = 0)]
    disk_reserve_gib: u64,
    /// Toolchain this host serves, as `name=version` (repeatable), e.g.
    /// `--toolchain rust=1.86.0`. Advertised verbatim once the worker is ready;
    /// the scheduler and the claim path match versions exactly (spec §5 R-PAR).
    #[arg(long = "toolchain", value_name = "NAME=VERSION", value_parser = parse_toolchain)]
    toolchains: Vec<(String, String)>,
    /// Directory holding the shared sccache (`<root>/sccache/<key>`). When set the
    /// worker advertises `rust-build-cache`; only Implementation executions that
    /// request it get the mount. Unset: no cache, no advertisement.
    #[arg(long, env = "AUTOSPEC_BUILD_CACHE_ROOT")]
    build_cache_root: Option<PathBuf>,
    /// Upper bound, in GiB, passed to sccache as `SCCACHE_CACHE_SIZE`.
    #[arg(long, env = "AUTOSPEC_BUILD_CACHE_MAX_GIB", default_value_t = 40)]
    build_cache_max_gib: u64,
    /// Opt-out of container privilege hardening. By default every container runs
    /// with `no-new-privileges` and the agent drops all capabilities except the
    /// two file-access ones; this restores the previous defaults for images that
    /// need setuid helpers or capabilities. Logged as a warning at startup.
    #[arg(
        long,
        env = "AUTOSPEC_RELAX_CONTAINER_HARDENING",
        default_value_t = false
    )]
    relax_container_hardening: bool,
    /// `uid:gid` every agent container and every Pi probe/signal runs as. One
    /// value feeds the runtime and the harness. Must be numeric. Default
    /// `1000:1000`: a non-root agent can use its owner-only worktree only where
    /// the daemon maps this uid to the worker's own (for example a rootful
    /// daemon and a worker running as uid 1000); otherwise provisioning is
    /// refused with an explanation. Root needs `--allow-root-agent`.
    #[arg(long, env = "AUTOSPEC_AGENT_USER", default_value = "1000:1000")]
    agent_user: String,
    /// Permits `--agent-user 0:0` (root agents). Off by default; logged as a
    /// warning when used.
    #[arg(long, env = "AUTOSPEC_ALLOW_ROOT_AGENT", default_value_t = false)]
    allow_root_agent: bool,
    /// InferWeave `host:port` the per-execution inference relay forwards to.
    /// Needs `--inference-relay-image`. With a relay each execution runs on an
    /// internal network and reaches only this upstream.
    #[arg(long, env = "AUTOSPEC_INFERENCE_UPSTREAM")]
    inference_upstream: Option<String>,
    /// Operator-supplied relay image: its entrypoint receives
    /// `<listen_port> <upstream>`. Needs `--inference-upstream`.
    #[arg(long, env = "AUTOSPEC_INFERENCE_RELAY_IMAGE")]
    inference_relay_image: Option<String>,
    /// Port the relay listens on; agents reach it as `http://inferweave:<port>`.
    #[arg(long, env = "AUTOSPEC_INFERENCE_RELAY_PORT", default_value_t = 8080)]
    inference_relay_port: u16,
    /// Starts WITHOUT an inference relay: executions keep an ordinary network
    /// with unrestricted egress. Without this flag the worker refuses to start
    /// unless both `--inference-upstream` and `--inference-relay-image` are set.
    /// Logged as a warning at startup.
    #[arg(long, env = "AUTOSPEC_ALLOW_AGENT_EGRESS", default_value_t = false)]
    allow_agent_egress: bool,
    #[arg(long, default_value = "https://github.com")]
    clone_base: String,
    #[arg(long, env = "AUTOSPEC_DOCKER_SOCKET")]
    docker_socket: Option<String>,
    /// Host-loopback port published by the constrained Docker API proxy.
    #[arg(long, env = "AUTOSPEC_DOCKER_PROXY_PORT", default_value_t = 2375)]
    docker_proxy_port: u16,
    /// Allows the worker to use only the deployment's constrained Docker API proxy.
    #[arg(long, env = "AUTOSPEC_WORKER_HOST_DOCKER", default_value_t = false)]
    host_docker: bool,
    #[arg(long, default_value = "pi")]
    pi_executable: String,
    /// Runs Pi releases outside the tested version matrix (logged as a warning).
    #[arg(
        long,
        env = "AUTOSPEC_ALLOW_UNTESTED_PI_VERSION",
        default_value_t = false
    )]
    allow_untested_pi_version: bool,
    /// Explicitly permits the local development-only credential issuer.
    #[arg(
        long,
        env = "AUTOSPEC_ALLOW_LOCAL_DEVELOPMENT_CREDENTIALS",
        default_value_t = false
    )]
    allow_local_development_credentials: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    validate_cli(&cli)?;
    if cli.relax_container_hardening {
        tracing::warn!(
            "container hardening is relaxed: no-new-privileges is not set and the agent keeps its default capabilities"
        );
    }
    if cli.allow_agent_egress {
        tracing::warn!(
            "agent egress is allowed: executions run on an ordinary network with unrestricted outbound access"
        );
    }
    let prepared = prepare_execution_plane(&cli).await;
    let (worker, execution_plane) = advertisement_from_preparation(&cli, prepared)?;
    let token = Secret(cli.worker_token.clone());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    run_control_loop(
        &cli,
        worker,
        execution_plane,
        token,
        client,
        Arc::new(disk_budget::sample),
        cache_collector(&cli),
    )
    .await
}

/// Startup checks that need no host access, so a bad flag fails before any
/// storage, Docker or database work begins.
fn validate_cli(cli: &Cli) -> Result<()> {
    disk_policy(cli)?;
    toolchain_map(&cli.toolchains)?;
    validate_build_cache(cli)?;
    resolve_agent_user(&cli.agent_user, cli.allow_root_agent)?;
    resolve_relay(cli)?;
    Ok(())
}

/// The inference relay every execution uses, or `None` when the operator
/// explicitly allowed agent egress (spec section 5 R-ORCH: internal by default).
///
/// - both relay flags: `Some(relay)`;
/// - neither, with `--allow-agent-egress`: `None`;
/// - anything else is an error naming the flags, so an upgrade that sets
///   neither is seen immediately rather than silently losing isolation. Relay
///   flags together with `--allow-agent-egress` contradict each other and are
///   refused too.
fn resolve_relay(cli: &Cli) -> Result<Option<runtime_docker::InferenceRelay>> {
    let upstream = cli.inference_upstream.as_deref().filter(|v| !v.is_empty());
    let image = cli
        .inference_relay_image
        .as_deref()
        .filter(|v| !v.is_empty());
    match (upstream, image, cli.allow_agent_egress) {
        (Some(upstream), Some(image), false) => {
            let relay = runtime_docker::InferenceRelay {
                image: image.to_owned(),
                upstream: upstream.to_owned(),
                listen_port: cli.inference_relay_port,
            };
            runtime_docker::validate_inference_relay(&relay)?;
            Ok(Some(relay))
        }
        (None, None, true) => Ok(None),
        (Some(_), Some(_), true) => anyhow::bail!(
            "--allow-agent-egress contradicts --inference-upstream/--inference-relay-image; \
             pass the relay flags or --allow-agent-egress, not both"
        ),
        (Some(_), None, _) | (None, Some(_), _) => {
            anyhow::bail!("--inference-upstream and --inference-relay-image must be given together")
        }
        (None, None, false) => anyhow::bail!(
            "no inference relay configured: executions would have unrestricted network egress. \
             Pass --inference-upstream <host:port> and --inference-relay-image <image> \
             (AUTOSPEC_INFERENCE_UPSTREAM, AUTOSPEC_INFERENCE_RELAY_IMAGE) to run agents on an \
             internal network, or --allow-agent-egress (AUTOSPEC_ALLOW_AGENT_EGRESS) to keep \
             unrestricted egress"
        ),
    }
}

/// The one agent `uid:gid` handed to both the runtime and the harness.
///
/// Numeric `uid:gid` only. A root user (uid 0, any gid) is refused unless the
/// operator also passed `--allow-root-agent`, and then logged: running agents
/// as root must be a visible, deliberate choice.
fn resolve_agent_user(user: &str, allow_root: bool) -> Result<String> {
    runtime_docker::validate_agent_user(user)?;
    let uid = user
        .split(':')
        .next()
        .and_then(|uid| uid.parse::<u32>().ok())
        .unwrap_or(0);
    if uid == 0 {
        anyhow::ensure!(
            allow_root,
            "--agent-user {user} runs agents as root; pass --allow-root-agent to confirm"
        );
        tracing::warn!(
            agent_user = user,
            "agents run as root by explicit operator choice"
        );
    }
    Ok(user.to_owned())
}

/// Parses one `--toolchain name=version` value; both halves must be non-empty.
fn parse_toolchain(raw: &str) -> Result<(String, String), String> {
    let (name, version) = raw
        .split_once('=')
        .ok_or_else(|| format!("toolchain `{raw}` must be NAME=VERSION"))?;
    let (name, version) = (name.trim(), version.trim());
    if name.is_empty() || version.is_empty() {
        return Err(format!(
            "toolchain `{raw}` must have a non-empty name and version"
        ));
    }
    Ok((name.to_owned(), version.to_owned()))
}

/// The advertised toolchain map; a name declared twice is ambiguous and refused
/// even when the versions agree.
fn toolchain_map(list: &[(String, String)]) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    for (name, version) in list {
        if map.insert(name.clone(), version.clone()).is_some() {
            anyhow::bail!("toolchain `{name}` is declared more than once");
        }
    }
    Ok(map)
}

/// The cache root must be absolute, must not sit inside the per-execution
/// state (which cleanup removes), and needs a positive size bound.
fn validate_build_cache(cli: &Cli) -> Result<()> {
    let Some(root) = &cli.build_cache_root else {
        return Ok(());
    };
    anyhow::ensure!(
        root.is_absolute(),
        "--build-cache-root must be an absolute path"
    );
    anyhow::ensure!(
        !root.starts_with(cli.state_root.join("executions")),
        "--build-cache-root must not be inside the per-execution state directory"
    );
    anyhow::ensure!(
        cli.build_cache_max_gib > 0,
        "--build-cache-max-gib must be greater than zero"
    );
    Ok(())
}

/// The Docker runtime factory, carrying the shared build cache only when the
/// operator configured one (the same condition that advertises the capability).
fn runtime_factory(
    cli: &Cli,
    agent_user: &str,
    verifier: Arc<dyn execution_storage::ReadyAllocationVerifier>,
    trusted_verifier: TrustedVerifierImage,
    credentials: Arc<dyn CredentialBroker>,
) -> Result<VerifiedDockerRuntimeFactory> {
    let factory = VerifiedDockerRuntimeFactory::new(
        cli.docker_socket.clone(),
        PathBuf::from(&cli.docker_binary),
        verifier,
        trusted_verifier,
        credentials,
    )
    .with_agent_user(agent_user)?;
    let factory = factory
        .with_container_hardening(container_hardening(cli))
        .with_inference_relay(resolve_relay(cli)?);
    Ok(match &cli.build_cache_root {
        Some(root) => factory.with_build_cache(root.clone(), cli.build_cache_max_gib),
        None => factory,
    })
}

/// The Pi harness factory; takes the same resolved `agent_user` as
/// [`runtime_factory`] so the container user and the probe user cannot differ.
fn harness_factory(
    cli: &Cli,
    agent_user: &str,
    verifier: Arc<dyn execution_storage::ReadyAllocationVerifier>,
) -> Result<VerifiedPiHarnessFactory> {
    Ok(VerifiedPiHarnessFactory::new(
        verifier,
        cli.docker_socket.clone(),
        PathBuf::from(&cli.docker_binary),
        cli.pi_executable.clone(),
        vec!["read".into(), "bash".into(), "edit".into(), "write".into()],
        Vec::new(),
    )
    .with_allow_untested_pi_version(cli.allow_untested_pi_version)
    .with_agent_user(agent_user)?)
}

fn container_hardening(cli: &Cli) -> runtime_docker::ContainerHardening {
    if cli.relax_container_hardening {
        runtime_docker::ContainerHardening::Relaxed
    } else {
        runtime_docker::ContainerHardening::Enforced
    }
}

fn disk_policy(cli: &Cli) -> Result<DiskPolicy> {
    DiskPolicy::new(
        cli.disk_warn_percent,
        cli.disk_stop_percent,
        cli.disk_reserve_gib,
    )
}

/// The advertisement actually sent on a heartbeat: the base advertisement plus
/// a freshly sampled disk-pressure health error (spec §5 R-BUILD). Sampling on
/// every call lets admission resume when space returns.
#[cfg(test)]
fn heartbeat_advertisement(
    base: &WorkerAdvertisement,
    state_root: &Path,
    policy: &DiskPolicy,
    sampler: &Sampler,
) -> WorkerAdvertisement {
    heartbeat_assessed(base, state_root, policy, sampler).0
}

/// [`heartbeat_advertisement`] plus the verdict it was built from, so the
/// caller can react to `Warn` (which is not advertised) with one sample per beat.
fn heartbeat_assessed(
    base: &WorkerAdvertisement,
    state_root: &Path,
    policy: &DiskPolicy,
    sampler: &Sampler,
) -> (WorkerAdvertisement, DiskVerdict) {
    let mut advertised = base.clone();
    let verdict = policy.assess(state_root, sampler);
    if let DiskVerdict::Stop(reason) = verdict {
        if !advertised
            .capabilities
            .health_errors
            .iter()
            .any(|error| error == reason)
        {
            advertised
                .capabilities
                .health_errors
                .push(reason.to_owned());
        }
    }
    (advertised, verdict)
}

/// Starts one background build-cache GC pass at a time. A pass is spawned, not
/// awaited, so a large cache never delays a heartbeat; a pass still running when
/// the next pressured heartbeat arrives is not duplicated.
struct CacheCollector {
    start: Arc<dyn Fn() -> tokio::task::JoinHandle<()> + Send + Sync>,
    running: Option<tokio::task::JoinHandle<()>>,
}

impl CacheCollector {
    fn new(start: Arc<dyn Fn() -> tokio::task::JoinHandle<()> + Send + Sync>) -> Self {
        Self {
            start,
            running: None,
        }
    }

    /// Runs a pass when the disk is under `Warn` or `Stop` pressure.
    fn on_verdict(&mut self, verdict: DiskVerdict) {
        if verdict == DiskVerdict::Ok {
            return;
        }
        if self
            .running
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return;
        }
        self.running = Some((self.start)());
    }
}

/// The real collector: present only when the operator configured a cache root.
fn cache_collector(cli: &Cli) -> Option<CacheCollector> {
    let root = cli.build_cache_root.clone()?;
    let max_gib = cli.build_cache_max_gib;
    let socket = cli.docker_socket.clone();
    let state_root = cli.state_root.clone();
    Some(CacheCollector::new(Arc::new(move || {
        let (root, socket, state_root) = (root.clone(), socket.clone(), state_root.clone());
        tokio::spawn(async move {
            let runtime = match runtime_docker::DockerRuntime::connect_with_state_root(
                socket.as_deref(),
                state_root,
            ) {
                Ok(runtime) => runtime.with_build_cache(root, max_gib),
                Err(error) => {
                    tracing::warn!(%error, "build cache gc could not reach Docker");
                    return;
                }
            };
            match runtime_docker::gc_build_cache(&runtime).await {
                Ok(report) => tracing::info!(
                    evicted = report.evicted.len(),
                    freed_bytes = report.freed_bytes,
                    kept_live = report.kept_live.len(),
                    foreign = report.foreign.len(),
                    pruned_refs = report.pruned_refs,
                    "build cache gc finished"
                ),
                Err(error) => tracing::warn!(%error, "build cache gc failed"),
            }
        })
    })))
}

struct ExecutionPlane {
    worker: Arc<Worker>,
    executions: Arc<PgExecutionStore>,
    reservations: Arc<PgReservationStore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparationFailureKind {
    Configuration,
    Persistence,
    Recovery,
    Storage,
    Docker,
}

struct PreparationFailure {
    kind: PreparationFailureKind,
    diagnostic: &'static str,
}

impl PreparationFailure {
    fn configuration(_: impl fmt::Display) -> Self {
        Self {
            kind: PreparationFailureKind::Configuration,
            diagnostic: "worker configuration is invalid or unavailable",
        }
    }

    fn persistence(_: impl fmt::Display) -> Self {
        Self {
            kind: PreparationFailureKind::Persistence,
            diagnostic: "persistence connection failed",
        }
    }

    fn storage(_: impl fmt::Display) -> Self {
        Self {
            kind: PreparationFailureKind::Storage,
            diagnostic: "execution storage capability probe failed",
        }
    }

    fn recovery(_: impl fmt::Display) -> Self {
        Self {
            kind: PreparationFailureKind::Recovery,
            diagnostic: "durable startup recovery failed",
        }
    }

    fn docker(_: impl fmt::Display) -> Self {
        Self {
            kind: PreparationFailureKind::Docker,
            diagnostic: "constrained Docker capability probe failed",
        }
    }

    fn health_code(&self) -> &'static str {
        match self.kind {
            PreparationFailureKind::Storage => "storage-capability-unavailable",
            PreparationFailureKind::Docker => "docker-capability-unavailable",
            PreparationFailureKind::Configuration => "worker-configuration-invalid",
            PreparationFailureKind::Persistence | PreparationFailureKind::Recovery => {
                "worker-preparation-unavailable"
            }
        }
    }

    fn diagnostic(&self) -> &'static str {
        self.diagnostic
    }
}

impl fmt::Debug for PreparationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparationFailure")
            .field("kind", &self.health_code())
            .field("diagnostic", &self.diagnostic)
            .finish()
    }
}

impl fmt::Display for PreparationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.diagnostic)
    }
}

impl std::error::Error for PreparationFailure {}

type PreparationResult<T> = std::result::Result<T, PreparationFailure>;

struct PreparationRetry {
    backoff: Duration,
}

impl Default for PreparationRetry {
    fn default() -> Self {
        Self {
            backoff: Duration::from_secs(1),
        }
    }
}

impl PreparationRetry {
    fn record_failure(&mut self) -> Duration {
        let delay = self.backoff;
        self.backoff = (self.backoff * 2).min(Duration::from_secs(30));
        delay
    }

    fn record_success(&mut self) {
        self.backoff = Duration::from_secs(1);
    }
}

fn advertisement_from_preparation(
    cli: &Cli,
    prepared: PreparationResult<(WorkerCapabilityProof, ExecutionPlane)>,
) -> Result<(WorkerAdvertisement, Option<ExecutionPlane>)> {
    match prepared {
        Ok((proof, plane)) => Ok((advertisement(cli, Some(proof), Vec::new())?, Some(plane))),
        Err(error) => {
            let failure = error.health_code();
            tracing::error!(worker_id = %cli.worker_id, failure, diagnostic = error.diagnostic(), "worker execution capability unavailable; advertising no runtime capacity");
            Ok((advertisement(cli, None, vec![failure.to_owned()])?, None))
        }
    }
}

async fn prepare_execution_plane(
    cli: &Cli,
) -> PreparationResult<(WorkerCapabilityProof, ExecutionPlane)> {
    validate_storage_runtime_pairing(cli.storage_kind, std::env::consts::OS)
        .map_err(PreparationFailure::configuration)?;
    validate_host_docker_policy(cli).map_err(PreparationFailure::configuration)?;
    ensure_local_development_credentials_allowed(cli).map_err(PreparationFailure::configuration)?;
    let storage = build_storage(cli)?;
    let proof = probe_capabilities(cli, storage.as_ref())?;
    let executions = Arc::new(
        PgExecutionStore::connect(&cli.database_url)
            .await
            .map_err(PreparationFailure::persistence)?,
    );
    let reservations = Arc::new(
        PgReservationStore::connect(&cli.database_url)
            .await
            .map_err(PreparationFailure::persistence)?,
    );
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&cli.database_url)
            .await
            .map_err(PreparationFailure::persistence)?,
    );
    let artifacts = Arc::new(
        PgArtifactStore::connect(&cli.database_url, &cli.state_root)
            .await
            .map_err(PreparationFailure::persistence)?,
    );
    let verifier: Arc<dyn execution_storage::ReadyAllocationVerifier> = storage.clone();
    let worktrees = Arc::new(GitWorktreeManager::with_clone_base_and_verifier(
        &cli.state_root,
        &cli.clone_base,
        verifier.clone(),
    ));
    let trusted_verifier =
        TrustedVerifierImage::new(&cli.docker_verifier_image, &cli.docker_verifier_command)
            .map_err(PreparationFailure::configuration)?;
    let credential_broker: Arc<dyn CredentialBroker> = Arc::new(
        LocalCredentialBroker::new(&cli.state_root, chrono::Duration::minutes(15))
            .map_err(PreparationFailure::configuration)?,
    );
    let agent_user = resolve_agent_user(&cli.agent_user, cli.allow_root_agent)
        .map_err(PreparationFailure::configuration)?;
    let runtimes = Arc::new(
        runtime_factory(
            cli,
            &agent_user,
            verifier.clone(),
            trusted_verifier,
            credential_broker,
        )
        .map_err(PreparationFailure::configuration)?,
    );
    let harnesses = Arc::new(
        harness_factory(cli, &agent_user, verifier.clone())
            .map_err(PreparationFailure::configuration)?,
    );
    let lifecycle = Arc::new(SystemExecutionLifecycle::new(
        SystemRecoveryConfig {
            state_root: cli.state_root.clone(),
            verifier: verifier.clone(),
            docker_binary: PathBuf::from(&cli.docker_binary),
            docker_socket: cli.docker_socket.clone(),
        },
        storage,
        worktrees,
        runtimes,
        harnesses,
        Arc::new(ContentAddressedEvidenceStore::new(artifacts)),
    ));
    lifecycle
        .ensure_consistent_agent_user()
        .map_err(PreparationFailure::configuration)?;
    let execution_worker = Arc::new(Worker::new(
        lifecycle,
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    Ok((
        proof,
        ExecutionPlane {
            worker: execution_worker,
            executions,
            reservations,
        },
    ))
}

fn validate_storage_runtime_pairing(storage_kind: StorageKind, os: &str) -> Result<()> {
    if os == "linux" && matches!(storage_kind, StorageKind::Lvm) {
        Ok(())
    } else {
        anyhow::bail!(
            "Docker execution workers require Linux with thick LVM; APFS is storage-lifecycle-only pending a privilege-separated macOS worker"
        )
    }
}

async fn run_control_loop(
    cli: &Cli,
    mut worker: WorkerAdvertisement,
    mut execution_plane: Option<ExecutionPlane>,
    token: Secret,
    client: reqwest::Client,
    sampler: Sampler,
    mut collector: Option<CacheCollector>,
) -> Result<()> {
    let policy = disk_policy(cli)?;
    tracing::info!(
        worker_id = %worker.id,
        controller = %cli.controller,
        ready = worker.capability_proof.is_some(),
        "autospec-worker starting"
    );
    let workers_url = format!(
        "{}/api/{API_VERSION}/workers",
        cli.controller.trim_end_matches('/')
    );
    let heartbeat_url = format!("{}/{}/heartbeat", workers_url, worker.id);
    let mut registered = false;
    let mut backoff = Duration::from_secs(1);
    let mut heartbeat_due = tokio::time::Instant::now();
    let mut preparation_retry = PreparationRetry::default();
    let mut preparation_due = tokio::time::Instant::now();
    let mut tasks: Vec<ExecutionTask> = Vec::new();
    match execution_plane.as_ref() {
        Some(plane) => match plane.worker.reconcile_startup(&worker.id).await {
            Ok(recovered) => tasks = recovered,
            Err(error) => {
                let failure = PreparationFailure::recovery(error);
                worker = advertisement(cli, None, vec![failure.health_code().to_owned()])?;
                execution_plane = None;
                preparation_due += preparation_retry.record_failure();
                tracing::warn!(
                    worker_id = %worker.id,
                    failure = failure.health_code(),
                    diagnostic = failure.diagnostic(),
                    "startup recovery is unavailable; advertising no runtime capacity"
                );
            }
        },
        None => preparation_due += preparation_retry.record_failure(),
    }
    loop {
        if execution_plane.is_none() && tokio::time::Instant::now() >= preparation_due {
            match prepare_execution_plane(cli).await {
                Ok((proof, plane)) => match plane.worker.reconcile_startup(&worker.id).await {
                    Ok(recovered) => {
                        tasks = recovered;
                        worker = advertisement(cli, Some(proof), Vec::new())?;
                        execution_plane = Some(plane);
                        preparation_retry.record_success();
                        registered = false;
                        heartbeat_due = tokio::time::Instant::now();
                        tracing::info!(worker_id = %worker.id, "worker execution capability recovered; re-registering ready");
                    }
                    Err(error) => {
                        let failure = PreparationFailure::recovery(error);
                        let delay = preparation_retry.record_failure();
                        preparation_due = tokio::time::Instant::now() + delay;
                        worker = advertisement(cli, None, vec![failure.health_code().to_owned()])?;
                        tracing::warn!(
                            worker_id = %worker.id,
                            failure = failure.health_code(),
                            diagnostic = failure.diagnostic(),
                            retry_seconds = delay.as_secs(),
                            "startup recovery remains unavailable"
                        );
                    }
                },
                Err(error) => {
                    let delay = preparation_retry.record_failure();
                    preparation_due = tokio::time::Instant::now() + delay;
                    worker = advertisement(cli, None, vec![error.health_code().to_owned()])?;
                    tracing::warn!(
                        worker_id = %worker.id,
                        failure = error.health_code(),
                        diagnostic = error.diagnostic(),
                        retry_seconds = delay.as_secs(),
                        "worker execution capability remains unavailable"
                    );
                }
            }
        }
        let mut index = tasks.len();
        while index > 0 {
            index -= 1;
            if tasks[index].is_finished() {
                let task = tasks.swap_remove(index);
                if let Err(error) = task.join().await {
                    tracing::error!(worker_id = %worker.id, %error, "execution task failed");
                }
            }
        }
        if let Some(plane) = execution_plane.as_ref() {
            while registered && tasks.len() < usize::try_from(cli.concurrency)? {
                match plane.reservations.reserve_next(&worker.id).await {
                    Ok(Some(reservation)) => {
                        tasks.push(plane.worker.clone().spawn(reservation.execution));
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(
                            worker_id = %worker.id,
                            %error,
                            retry_seconds = backoff.as_secs(),
                            "reservation poll failed; active execution tasks remain supervised"
                        );
                        heartbeat_due = tokio::time::Instant::now() + backoff;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        break;
                    }
                }
            }
        }
        if let Some(plane) = execution_plane.as_ref() {
            if let Err(error) = plane.worker.reconcile_daemon_tick(&worker.id, &tasks).await {
                tracing::warn!(
                    worker_id = %worker.id,
                    %error,
                    "durable cancellation reconciliation remains pending"
                );
            }
        }
        if tokio::time::Instant::now() >= heartbeat_due {
            let (advertised, verdict) =
                heartbeat_assessed(&worker, &cli.state_root, &policy, &sampler);
            if let Some(collector) = collector.as_mut() {
                collector.on_verdict(verdict);
            }
            let request = if registered {
                client.post(&heartbeat_url)
            } else {
                client.post(&workers_url)
            };
            match request.bearer_auth(&token.0).json(&advertised).send().await {
                Ok(response)
                    if control_disposition(registered, response.status())
                        == ControlDisposition::Registered =>
                {
                    registered = true;
                    backoff = Duration::from_secs(1);
                    heartbeat_due = tokio::time::Instant::now() + Duration::from_secs(30);
                }
                Ok(response)
                    if control_disposition(registered, response.status())
                        == ControlDisposition::ReRegister =>
                {
                    registered = false;
                    backoff = Duration::from_secs(1);
                    heartbeat_due = tokio::time::Instant::now();
                    tracing::warn!(
                        worker_id = %worker.id,
                        "controller forgot worker registration; re-registering"
                    );
                }
                Ok(response) => {
                    tracing::warn!(
                        worker_id = %worker.id,
                        status = %response.status(),
                        retry_seconds = backoff.as_secs(),
                        "worker control-plane request failed"
                    );
                    heartbeat_due = tokio::time::Instant::now() + backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
                Err(error) => {
                    tracing::warn!(worker_id = %worker.id, %error, retry_seconds = backoff.as_secs(), "worker control-plane request failed");
                    heartbeat_due = tokio::time::Instant::now() + backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for shutdown")?;
                for task in &tasks {
                    let Some(plane) = execution_plane.as_ref() else {
                        break;
                    };
                    if let Err(error) = plane.executions.request_cancellation(task.execution_id()).await {
                        tracing::error!(execution_id = %task.execution_id(), %error, "failed to persist shutdown cancellation request");
                    }
                    task.cancel();
                }
                for task in tasks {
                    if let Err(error) = task.join().await {
                        tracing::error!(worker_id = %worker.id, %error, "execution cleanup during drain failed");
                    }
                }
                break;
            }
        }
    }
    Ok(())
}

fn validate_host_docker_policy(cli: &Cli) -> Result<()> {
    anyhow::ensure!(
        cli.host_docker,
        "Docker execution requires AUTOSPEC_WORKER_HOST_DOCKER=true"
    );
    anyhow::ensure!(
        cli.docker_proxy_port != 0,
        "Docker proxy port must be between 1 and 65535"
    );
    let host_endpoint = format!("tcp://127.0.0.1:{}", cli.docker_proxy_port);
    let supported_endpoint = cli.docker_socket.as_deref() == Some(host_endpoint.as_str())
        || (cli.docker_proxy_port == 2375
            && cli.docker_socket.as_deref() == Some("tcp://docker-api:2375"));
    anyhow::ensure!(
        supported_endpoint,
        "Docker execution requires the constrained deployment proxy"
    );
    Ok(())
}

fn ensure_local_development_credentials_allowed(cli: &Cli) -> Result<()> {
    anyhow::ensure!(
        cli.allow_local_development_credentials,
        "no production credential issuer is configured; local development credentials require --allow-local-development-credentials"
    );
    Ok(())
}

fn advertisement(
    cli: &Cli,
    proof: Option<WorkerCapabilityProof>,
    health_errors: Vec<String>,
) -> Result<WorkerAdvertisement> {
    if cli.concurrency == 0 || cli.concurrency > 64 {
        anyhow::bail!("worker concurrency must be between 1 and 64");
    }
    let ready = proof
        .as_ref()
        .is_some_and(WorkerCapabilityProof::is_complete);
    let toolchains = toolchain_map(&cli.toolchains)?;
    Ok(WorkerAdvertisement {
        id: WorkerId::new(&cli.worker_id),
        capabilities: WorkerCapabilities {
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            cpu: std::thread::available_parallelism()
                .map(|count| u32::try_from(count.get()).unwrap_or(u32::MAX))
                .unwrap_or(1),
            memory_mib: cli.memory_mib,
            disk_gib: cli.disk_gib,
            runtimes: if ready {
                vec![RuntimeKind::Docker]
            } else {
                Vec::new()
            },
            capabilities: if ready {
                let mut capabilities = vec!["docker".to_owned()];
                if cli.build_cache_root.is_some() {
                    capabilities.push(BUILD_CACHE_CAPABILITY.to_owned());
                }
                capabilities
            } else {
                Vec::new()
            },
            toolchains: if ready { toolchains } else { BTreeMap::new() },
            max_concurrent_executions: cli.concurrency,
            health_errors,
        },
        capability_proof: proof,
    })
}

fn probe_capabilities(
    cli: &Cli,
    storage: &dyn ExecutionStorageManager,
) -> PreparationResult<WorkerCapabilityProof> {
    let capability = storage.probe(cli.disk_gib).map_err(|error| match error {
        StorageError::DockerCapability(_) => PreparationFailure::docker(error),
        _ => PreparationFailure::storage(error),
    })?;
    Ok(WorkerCapabilityProof {
        storage_backend: capability.backend.backend,
        storage_pool_identity: capability.backend.pool_identity,
        docker_daemon_id: capability.docker_bind.daemon_id,
        docker_verifier: capability.docker_bind.verifier,
        docker_method_version: capability.docker_bind.method_version,
    })
}

fn build_storage(cli: &Cli) -> PreparationResult<Arc<ExecutionStorage>> {
    let runner = Arc::new(ProcessCommandRunner);
    let backend: Box<dyn StorageBackend> = match cli.storage_kind {
        StorageKind::Apfs => Box::new(
            ApfsBackend::new(&cli.storage_pool, runner).map_err(PreparationFailure::storage)?,
        ),
        StorageKind::Lvm => Box::new(
            LvmBackend::new(&cli.storage_pool, runner).map_err(PreparationFailure::storage)?,
        ),
    };
    let verifier =
        TrustedVerifierImage::new(&cli.docker_verifier_image, &cli.docker_verifier_command)
            .map_err(PreparationFailure::configuration)?;
    let output = docker_command(&cli.docker_binary, cli.docker_socket.as_deref())
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .with_context(|| format!("execute {} info", cli.docker_binary))
        .map_err(PreparationFailure::docker)?;
    if !output.status.success() {
        return Err(PreparationFailure::docker(
            "Docker daemon identity probe failed",
        ));
    }
    let daemon_id = String::from_utf8(output.stdout)
        .map_err(PreparationFailure::docker)?
        .trim()
        .to_owned();
    if daemon_id.is_empty() {
        return Err(PreparationFailure::docker(
            "Docker daemon identity probe returned empty ID",
        ));
    }
    Ok(Arc::new(
        ExecutionStorage::new(
            &cli.state_root,
            backend,
            Box::new(DockerCapabilityVerifier {
                docker: PathBuf::from(&cli.docker_binary),
                docker_host: cli.docker_socket.clone(),
                daemon_id,
                verifier_image: cli.docker_verifier_image.clone(),
                verifier_command: cli.docker_verifier_command.clone(),
                method_version: verifier.proof_method(),
                worker_id: WorkerId::new(&cli.worker_id),
            }),
        )
        .map_err(PreparationFailure::storage)?,
    ))
}

#[derive(Debug)]
struct DockerCapabilityVerifier {
    docker: PathBuf,
    docker_host: Option<String>,
    daemon_id: String,
    verifier_image: String,
    verifier_command: String,
    method_version: String,
    worker_id: WorkerId,
}

impl DockerBindVerifier for DockerCapabilityVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        &self.daemon_id
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        let output = docker_command(&self.docker, self.docker_host.as_deref())
            .args([
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                &self.verifier_image,
            ])
            .output()
            .map_err(|error| StorageError::Command(error.to_string()))?;
        if !output.status.success()
            || String::from_utf8_lossy(&output.stdout).trim() != self.verifier_image
        {
            return Err(StorageError::Unavailable(
                "immutable Docker verifier image is unavailable or changed".into(),
            ));
        }
        Ok(DockerBindCapability {
            daemon_id: self.daemon_id.clone(),
            verifier: self.verifier_image.clone(),
            method_version: self.method_version.clone(),
        })
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        let canonical = source
            .canonicalize()
            .map_err(|error| StorageError::Unavailable(error.to_string()))?;
        let execution_id = canonical
            .file_name()
            .and_then(|name| name.to_str())
            .map(ExecutionId::new)
            .ok_or_else(|| {
                StorageError::IdentityMismatch("allocation lacks execution id".into())
            })?;
        let labels = OwnershipLabels {
            execution_id,
            worker_id: self.worker_id.clone(),
            repository: "storage-proof".into(),
            issue: None,
        };
        let mount = format!("type=bind,src={},dst=/proof,readonly", canonical.display());
        let mut command = docker_command(&self.docker, self.docker_host.as_deref());
        command.args(["run", "--rm", "--network", "none", "--read-only"]);
        for (key, value) in labels.to_map() {
            command.args(["--label", &format!("{key}={value}")]);
        }
        let output = command
            .args([
                "--mount",
                &mount,
                &self.verifier_image,
                &self.verifier_command,
                "-c",
                "%d:%i",
                "/proof",
            ])
            .output()
            .map_err(|error| StorageError::Command(error.to_string()))?;
        if !output.status.success() {
            return Err(StorageError::Command(
                String::from_utf8_lossy(&output.stderr).trim().into(),
            ));
        }
        let filesystem_id = String::from_utf8(output.stdout)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
            .trim()
            .to_owned();
        if filesystem_id.is_empty() || !filesystem_id.contains(':') {
            return Err(StorageError::IdentityMismatch(
                "Docker verifier returned malformed filesystem identity".into(),
            ));
        }
        Ok(DockerBindProof {
            daemon_id: self.daemon_id.clone(),
            verifier: self.verifier_image.clone(),
            method_version: self.method_version.clone(),
            source_path: canonical,
            filesystem_id,
        })
    }
}

fn docker_command(binary: impl AsRef<Path>, host: Option<&str>) -> Command {
    let mut command = Command::new(binary.as_ref());
    if let Some(host) = host {
        command.env("DOCKER_HOST", host);
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli() -> Cli {
        Cli {
            controller: "http://127.0.0.1:8420".into(),
            worker_id: "worker-1".into(),
            worker_token: "secret".into(),
            concurrency: 2,
            memory_mib: 4096,
            disk_gib: 100,
            storage_kind: StorageKind::Apfs,
            storage_pool: "/tmp".into(),
            docker_verifier_image: format!("sha256:{}", "a".repeat(64)),
            docker_verifier_command: "/usr/bin/stat".into(),
            docker_binary: "docker".into(),
            database_url: "postgres://test".into(),
            state_root: "/tmp".into(),
            toolchains: Vec::new(),
            build_cache_root: None,
            build_cache_max_gib: 40,
            relax_container_hardening: false,
            agent_user: "1000:1000".into(),
            allow_root_agent: false,
            inference_upstream: None,
            inference_relay_image: None,
            inference_relay_port: 8080,
            allow_agent_egress: true,
            clone_base: "https://github.com".into(),
            docker_socket: None,
            docker_proxy_port: 2375,
            host_docker: false,
            pi_executable: "pi".into(),
            allow_untested_pi_version: false,
            allow_local_development_credentials: false,
            disk_warn_percent: 80,
            disk_stop_percent: 90,
            disk_reserve_gib: 0,
        }
    }

    /// Parses the worker flags exactly as given (plus the required ones).
    fn parse_raw(extra: &[&str]) -> Cli {
        let mut args = vec![
            "autospec-worker",
            "--worker-id",
            "w",
            "--worker-token",
            "t",
            "--storage-kind",
            "apfs",
            "--storage-pool",
            "/tmp",
            "--docker-verifier-image",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--database-url",
            "postgres://test",
        ];
        args.extend_from_slice(extra);
        Cli::try_parse_from(args).expect("worker CLI parses")
    }

    /// `parse_raw` plus `--allow-agent-egress` unless the test sets a relay
    /// flag itself, so tests of unrelated flags pass the startup relay rule.
    fn parse(extra: &[&str]) -> Cli {
        let own = extra.iter().any(|arg| {
            matches!(
                *arg,
                "--inference-upstream" | "--inference-relay-image" | "--allow-agent-egress"
            )
        });
        let mut all = extra.to_vec();
        if !own {
            all.push("--allow-agent-egress");
        }
        parse_raw(&all)
    }

    #[test]
    fn untested_pi_versions_are_refused_unless_the_flag_is_given() {
        assert!(!parse(&[]).allow_untested_pi_version);
        assert!(parse(&["--allow-untested-pi-version"]).allow_untested_pi_version);
    }

    #[test]
    fn production_startup_fails_closed_without_explicit_local_credential_opt_in() {
        let mut cli = cli();
        assert!(ensure_local_development_credentials_allowed(&cli).is_err());
        cli.allow_local_development_credentials = true;
        ensure_local_development_credentials_allowed(&cli).unwrap();
    }

    #[test]
    fn missing_storage_proof_forces_offline_and_token_debug_is_redacted() {
        let worker = advertisement(
            &cli(),
            None,
            vec!["storage-capability-unavailable".to_owned()],
        )
        .unwrap();
        assert!(worker.capability_proof.is_none());
        assert_eq!(
            worker.capabilities.health_errors,
            vec!["storage-capability-unavailable"]
        );
        let wire = serde_json::to_value(worker).unwrap();
        assert!(wire.get("state").is_none());
        assert!(wire.get("last_heartbeat").is_none());
        assert!(wire.get("running_executions").is_none());
        assert_eq!(format!("{:?}", Secret("do-not-log".into())), "[REDACTED]");
    }

    #[test]
    fn complete_storage_and_docker_proof_allows_ready() {
        let proof = WorkerCapabilityProof {
            storage_backend: "apfs".into(),
            storage_pool_identity: "pool".into(),
            docker_daemon_id: "daemon".into(),
            docker_verifier: format!("sha256:{}", "a".repeat(64)),
            docker_method_version: "v2".into(),
        };
        let worker = advertisement(&cli(), Some(proof), Vec::new()).unwrap();
        assert_eq!(worker.capabilities.runtimes, vec![RuntimeKind::Docker]);
        assert!(worker.capabilities.health_errors.is_empty());
    }

    #[test]
    fn heartbeat_not_found_forces_registration_without_backoff() {
        assert_eq!(
            control_disposition(true, reqwest::StatusCode::NOT_FOUND),
            ControlDisposition::ReRegister
        );
        assert_eq!(
            control_disposition(false, reqwest::StatusCode::NOT_FOUND),
            ControlDisposition::Retry
        );
        assert_eq!(
            control_disposition(true, reqwest::StatusCode::NO_CONTENT),
            ControlDisposition::Registered
        );
    }

    #[test]
    fn host_docker_requires_explicit_opt_in_and_the_constrained_proxy() {
        let mut cli = cli();
        cli.docker_socket = Some("tcp://docker-api:2375".into());
        assert!(validate_host_docker_policy(&cli).is_err());
        cli.host_docker = true;
        validate_host_docker_policy(&cli).unwrap();
        cli.docker_socket = Some("tcp://127.0.0.1:2375".into());
        validate_host_docker_policy(&cli).unwrap();
        cli.docker_proxy_port = 42375;
        assert!(validate_host_docker_policy(&cli).is_err());
        cli.docker_socket = Some("tcp://127.0.0.1:42375".into());
        validate_host_docker_policy(&cli).unwrap();
        cli.docker_socket = Some("unix:///var/run/docker.sock".into());
        assert!(validate_host_docker_policy(&cli).is_err());
        cli.docker_socket = Some("tcp://192.0.2.10:42375".into());
        assert!(validate_host_docker_policy(&cli).is_err());
        cli.docker_proxy_port = 0;
        cli.docker_socket = Some("tcp://127.0.0.1:0".into());
        assert!(validate_host_docker_policy(&cli).is_err());
    }

    #[test]
    fn storage_runtime_pairing_supports_only_linux_lvm_for_docker_workers() {
        assert!(validate_storage_runtime_pairing(StorageKind::Lvm, "linux").is_ok());
        assert!(validate_storage_runtime_pairing(StorageKind::Apfs, "macos").is_err());
        assert!(validate_storage_runtime_pairing(StorageKind::Apfs, "linux").is_err());
        assert!(validate_storage_runtime_pairing(StorageKind::Lvm, "macos").is_err());
    }

    #[test]
    fn preparation_failures_are_typed_and_diagnostics_never_echo_sources() {
        let secret = "postgres://user:password@database/private";
        for (failure, kind, code) in [
            (
                PreparationFailure::configuration(secret),
                PreparationFailureKind::Configuration,
                "worker-configuration-invalid",
            ),
            (
                PreparationFailure::persistence(secret),
                PreparationFailureKind::Persistence,
                "worker-preparation-unavailable",
            ),
            (
                PreparationFailure::recovery(secret),
                PreparationFailureKind::Recovery,
                "worker-preparation-unavailable",
            ),
            (
                PreparationFailure::storage(secret),
                PreparationFailureKind::Storage,
                "storage-capability-unavailable",
            ),
            (
                PreparationFailure::docker(secret),
                PreparationFailureKind::Docker,
                "docker-capability-unavailable",
            ),
        ] {
            assert_eq!(failure.kind, kind);
            assert_eq!(failure.health_code(), code);
            assert!(!format!("{failure:?}").contains(secret));
            let worker = advertisement(&cli(), None, vec![code.to_owned()]).unwrap();
            assert!(worker.capability_proof.is_none());
            assert!(worker.capabilities.runtimes.is_empty());
            assert!(worker.capabilities.health_errors_are_sanitized());
        }
    }

    #[test]
    fn preparation_retry_backoff_is_bounded_and_resets_after_recovery() {
        let mut retry = PreparationRetry::default();
        assert_eq!(retry.record_failure(), Duration::from_secs(1));
        assert_eq!(retry.record_failure(), Duration::from_secs(2));
        for _ in 0..10 {
            retry.record_failure();
        }
        assert_eq!(retry.record_failure(), Duration::from_secs(30));
        retry.record_success();
        assert_eq!(retry.record_failure(), Duration::from_secs(1));
    }

    #[test]
    fn docker_construction_failure_still_produces_sanitized_offline_registration() {
        let mut cli = cli();
        cli.allow_local_development_credentials = true;
        cli.host_docker = true;
        cli.docker_socket = Some("tcp://docker-api:2375".into());
        cli.docker_binary = "/definitely/missing/autospec-docker".into();
        let failure = build_storage(&cli).expect_err("missing Docker binary");
        assert_eq!(failure.kind, PreparationFailureKind::Docker);
        let worker =
            advertisement(&cli, None, vec!["docker-capability-unavailable".to_owned()]).unwrap();
        assert!(worker.capability_proof.is_none());
        assert!(worker.capabilities.runtimes.is_empty());
        assert_eq!(
            worker.capabilities.health_errors,
            vec!["docker-capability-unavailable"]
        );
    }
    fn sampler_with_used_percent(
        used: &'static [u64],
        calls: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Sampler {
        Arc::new(move |_| {
            let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let pct = used[call.min(used.len() - 1)];
            Ok(disk_budget::DiskSample {
                total_bytes: 1000,
                free_bytes: 1000 - pct * 10,
                total_inodes: 1000,
                free_inodes: 1000,
            })
        })
    }

    #[test]
    fn disk_flags_default_and_parse_through_cli_into_policy() {
        let defaults = parse(&[]);
        assert_eq!(
            (
                defaults.disk_warn_percent,
                defaults.disk_stop_percent,
                defaults.disk_reserve_gib
            ),
            (80, 90, 0)
        );
        assert_eq!(
            disk_policy(&defaults).unwrap(),
            DiskPolicy::new(80, 90, 0).unwrap()
        );
        let cli = parse(&[
            "--disk-warn-percent",
            "50",
            "--disk-stop-percent",
            "60",
            "--disk-reserve-gib",
            "3",
        ]);
        let policy = disk_policy(&cli).unwrap();
        assert_eq!(policy.warn_percent, 50);
        assert_eq!(policy.stop_percent, 60);
        assert_eq!(policy.reserve_bytes, 3 * 1024 * 1024 * 1024);
        assert!(disk_policy(&parse(&["--disk-warn-percent", "95"])).is_err());
    }

    #[test]
    fn configured_stop_percent_and_reserve_reach_the_advertisement() {
        let base = advertisement(&cli(), None, Vec::new()).unwrap();
        let root = Path::new("/state");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sampler = sampler_with_used_percent(&[55], calls);
        let strict = disk_policy(&parse(&[
            "--disk-warn-percent",
            "40",
            "--disk-stop-percent",
            "50",
        ]))
        .unwrap();
        let lenient = disk_policy(&parse(&[])).unwrap();
        assert_eq!(
            heartbeat_advertisement(&base, root, &strict, &sampler)
                .capabilities
                .health_errors,
            vec!["disk-pressure-bytes"]
        );
        assert!(heartbeat_advertisement(&base, root, &lenient, &sampler)
            .capabilities
            .health_errors
            .is_empty());
        // 1000 total bytes is far below a 1 GiB reserve, whatever the percent.
        let reserve = disk_policy(&parse(&["--disk-reserve-gib", "1"])).unwrap();
        assert_eq!(
            heartbeat_advertisement(&base, root, &reserve, &sampler)
                .capabilities
                .health_errors,
            vec!["disk-pressure-bytes"]
        );
    }

    #[test]
    fn heartbeat_resamples_each_call_and_keeps_preparation_errors_sanitized() {
        let base =
            advertisement(&cli(), None, vec!["storage-capability-unavailable".into()]).unwrap();
        let policy = disk_policy(&parse(&[])).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sampler = sampler_with_used_percent(&[95, 10], calls.clone());
        let root = Path::new("/state");
        let full = heartbeat_advertisement(&base, root, &policy, &sampler);
        assert_eq!(
            full.capabilities.health_errors,
            vec!["storage-capability-unavailable", "disk-pressure-bytes"]
        );
        assert!(full.capabilities.health_errors_are_sanitized());
        let freed = heartbeat_advertisement(&base, root, &policy, &sampler);
        assert_eq!(
            freed.capabilities.health_errors,
            vec!["storage-capability-unavailable"]
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        // The base advertisement is never mutated by pressure.
        assert_eq!(base.capabilities.health_errors.len(), 1);
    }

    /// Serves `500` to every request and forwards each request body.
    async fn fake_controller(
        listener: tokio::net::TcpListener,
        bodies: tokio::sync::mpsc::UnboundedSender<String>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            let body = loop {
                let read = socket.read(&mut chunk).await.unwrap_or(0);
                if read == 0 {
                    break String::new();
                }
                buffer.extend_from_slice(&chunk[..read]);
                let text = String::from_utf8_lossy(&buffer).into_owned();
                if let Some((head, body)) = text.split_once("\r\n\r\n") {
                    let length = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if body.len() >= length {
                        break body.to_owned();
                    }
                }
            };
            let _ = bodies.send(body);
            let _ = socket
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await;
        }
    }

    #[tokio::test]
    async fn control_loop_heartbeats_advertise_disk_pressure_then_resume() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut bodies) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(fake_controller(listener, sender));
        let state_root = tempfile::tempdir().unwrap();
        let mut config = cli();
        config.controller = format!("http://{address}");
        config.state_root = state_root.path().to_owned();
        let worker = advertisement(&config, None, Vec::new()).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // First heartbeat sees a full disk, later ones see space again.
        let sampler = sampler_with_used_percent(&[95, 10], calls);
        let client = reqwest::Client::new();
        let control = tokio::spawn(async move {
            run_control_loop(
                &config,
                worker,
                None,
                Secret("t".into()),
                client,
                sampler,
                None,
            )
            .await
        });
        let mut errors = Vec::new();
        for _ in 0..2 {
            let body = tokio::time::timeout(Duration::from_secs(30), bodies.recv())
                .await
                .expect("heartbeat arrives")
                .expect("server alive");
            let wire: serde_json::Value = serde_json::from_str(&body).unwrap();
            errors.push(
                wire["capabilities"]["healthErrors"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        control.abort();
        server.abort();
        assert!(errors[0].iter().any(|error| error == "disk-pressure-bytes"));
        assert!(!errors[1].iter().any(|error| error == "disk-pressure-bytes"));
    }

    fn ready_proof() -> WorkerCapabilityProof {
        WorkerCapabilityProof {
            storage_backend: "apfs".into(),
            storage_pool_identity: "pool".into(),
            docker_daemon_id: "daemon".into(),
            docker_verifier: format!("sha256:{}", "a".repeat(64)),
            docker_method_version: "v2".into(),
        }
    }

    #[test]
    fn parse_toolchain_accepts_name_equals_version_and_rejects_the_rest() {
        assert_eq!(
            parse_toolchain("rust=1.86.0").unwrap(),
            ("rust".to_owned(), "1.86.0".to_owned())
        );
        for bad in ["rust=", "=1.86.0", "rust", "=", " =  "] {
            assert!(parse_toolchain(bad).is_err(), "accepted `{bad}`");
        }
    }

    #[test]
    fn toolchain_map_rejects_a_repeated_name_even_with_the_same_version() {
        let pair = |n: &str, v: &str| (n.to_owned(), v.to_owned());
        assert!(toolchain_map(&[pair("rust", "1.86.0"), pair("rust", "1.85.0")]).is_err());
        assert!(toolchain_map(&[pair("rust", "1.86.0"), pair("rust", "1.86.0")]).is_err());
        let map = toolchain_map(&[pair("rust", "1.86.0"), pair("node", "22")]).unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn toolchain_flags_flow_from_the_command_line_into_a_ready_advertisement() {
        let cli = parse(&["--toolchain", "rust=1.86.0", "--toolchain", "node=22.1.0"]);
        assert_eq!(cli.toolchains.len(), 2);
        let worker = advertisement(&cli, Some(ready_proof()), Vec::new()).unwrap();
        let wire = serde_json::to_value(&worker).unwrap();
        assert_eq!(
            wire["capabilities"]["toolchains"],
            serde_json::json!({"node": "22.1.0", "rust": "1.86.0"})
        );
        // The heartbeat sends a clone of the base advertisement, so the
        // toolchains survive the per-beat disk-pressure refresh.
        let policy = disk_policy(&cli).unwrap();
        let sampler =
            sampler_with_used_percent(&[10], Arc::new(std::sync::atomic::AtomicUsize::new(0)));
        let beat = heartbeat_advertisement(&worker, Path::new("/tmp"), &policy, &sampler);
        assert_eq!(beat.capabilities.toolchains["rust"], "1.86.0");
    }

    #[test]
    fn an_unready_worker_advertises_no_toolchains() {
        let cli = parse(&["--toolchain", "rust=1.86.0"]);
        let worker =
            advertisement(&cli, None, vec!["docker-capability-unavailable".into()]).unwrap();
        assert!(worker.capabilities.toolchains.is_empty());
        let wire = serde_json::to_value(&worker).unwrap();
        assert!(wire["capabilities"].get("toolchains").is_none());
    }

    #[test]
    fn startup_validation_rejects_malformed_and_duplicate_toolchain_flags() {
        assert!(Cli::try_parse_from([
            "autospec-worker",
            "--worker-id",
            "w",
            "--worker-token",
            "t",
            "--storage-kind",
            "apfs",
            "--storage-pool",
            "/tmp",
            "--docker-verifier-image",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--database-url",
            "postgres://test",
            "--toolchain",
            "rust=",
        ])
        .is_err());
        let dup = parse(&["--toolchain", "rust=1.86.0", "--toolchain", "rust=1.85.0"]);
        assert!(validate_cli(&dup).is_err());
        assert!(validate_cli(&parse(&["--toolchain", "rust=1.86.0"])).is_ok());
    }

    fn count(worker: &WorkerAdvertisement, capability: &str) -> usize {
        worker
            .capabilities
            .capabilities
            .iter()
            .filter(|c| c.as_str() == capability)
            .count()
    }

    #[test]
    fn without_a_cache_root_the_worker_advertises_docker_and_no_build_cache() {
        let cli = parse(&[]);
        assert!(cli.build_cache_root.is_none());
        assert_eq!(cli.build_cache_max_gib, 40);
        let worker = advertisement(&cli, Some(ready_proof()), Vec::new()).unwrap();
        assert_eq!(count(&worker, "docker"), 1);
        assert_eq!(count(&worker, "rust-build-cache"), 0);
    }

    #[test]
    fn a_cache_root_advertises_rust_build_cache_only_when_ready() {
        let cli = parse(&[
            "--build-cache-root",
            "/var/cache/autospec",
            "--build-cache-max-gib",
            "12",
        ]);
        assert_eq!(cli.build_cache_max_gib, 12);
        let ready = advertisement(&cli, Some(ready_proof()), Vec::new()).unwrap();
        assert_eq!(count(&ready, "docker"), 1);
        assert_eq!(count(&ready, "rust-build-cache"), 1);
        let offline =
            advertisement(&cli, None, vec!["docker-capability-unavailable".into()]).unwrap();
        assert!(offline.capabilities.capabilities.is_empty());
    }

    #[test]
    fn the_cache_flags_reach_the_runtime_factory() {
        #[derive(Debug)]
        struct NoStorage;
        impl execution_storage::ReadyAllocationVerifier for NoStorage {
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
        let build = |cli: &Cli| {
            runtime_factory(
                cli,
                &cli.agent_user,
                Arc::new(NoStorage),
                TrustedVerifierImage::new(format!("sha256:{}", "a".repeat(64)), "/usr/bin/stat")
                    .unwrap(),
                Arc::new(
                    LocalCredentialBroker::new(std::env::temp_dir(), chrono::Duration::minutes(15))
                        .unwrap(),
                ),
            )
            .unwrap()
        };
        assert!(build(&parse(&[])).build_cache().is_none());
        let cli = parse(&[
            "--build-cache-root",
            "/var/cache/autospec",
            "--build-cache-max-gib",
            "12",
        ]);
        assert_eq!(
            build(&cli).build_cache(),
            Some((Path::new("/var/cache/autospec"), 12))
        );
    }

    #[test]
    fn container_hardening_is_enforced_unless_the_operator_opts_out() {
        use runtime_docker::ContainerHardening::{Enforced, Relaxed};
        #[derive(Debug)]
        struct NoStorage;
        impl execution_storage::ReadyAllocationVerifier for NoStorage {
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
        let build = |cli: &Cli| {
            runtime_factory(
                cli,
                &cli.agent_user,
                Arc::new(NoStorage),
                TrustedVerifierImage::new(format!("sha256:{}", "a".repeat(64)), "/usr/bin/stat")
                    .unwrap(),
                Arc::new(
                    LocalCredentialBroker::new(std::env::temp_dir(), chrono::Duration::minutes(15))
                        .unwrap(),
                ),
            )
            .unwrap()
        };
        assert!(!parse(&[]).relax_container_hardening);
        assert_eq!(build(&parse(&[])).container_hardening(), Enforced);
        let cli = parse(&["--relax-container-hardening"]);
        assert!(cli.relax_container_hardening);
        assert_eq!(build(&cli).container_hardening(), Relaxed);
    }

    #[test]
    fn resolve_agent_user_refuses_root_unless_explicitly_allowed() {
        assert!(resolve_agent_user("0:0", false).is_err());
        assert_eq!(resolve_agent_user("0:0", true).unwrap(), "0:0");
        assert_eq!(resolve_agent_user("1000:1000", false).unwrap(), "1000:1000");
        assert_eq!(resolve_agent_user("1000:1000", true).unwrap(), "1000:1000");
        // Root by uid alone is still root, whatever the gid.
        assert!(resolve_agent_user("0:1000", false).is_err());
        assert!(resolve_agent_user("00:5", false).is_err());
        for bad in [
            "root",
            "",
            "1000",
            "1000:",
            "ubuntu:ubuntu",
            "-1:-1",
            " 1:1",
        ] {
            assert!(resolve_agent_user(bad, true).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_agent_user_flags_default_to_non_root_and_gate_root() {
        let cli = parse(&[]);
        assert_eq!(
            (cli.agent_user.as_str(), cli.allow_root_agent),
            ("1000:1000", false)
        );
        assert!(validate_cli(&cli).is_ok());
        let root = parse(&["--agent-user", "0:0"]);
        assert!(
            validate_cli(&root).is_err(),
            "root without the extra flag fails at startup"
        );
        let allowed = parse(&["--agent-user", "0:0", "--allow-root-agent"]);
        assert!(validate_cli(&allowed).is_ok());
        assert!(validate_cli(&parse(&["--agent-user", "ubuntu"])).is_err());
    }

    #[test]
    fn one_agent_user_reaches_both_the_runtime_and_the_harness_factory() {
        use orchestrator_worker::{HarnessFactory, RuntimeFactory};
        #[derive(Debug)]
        struct NoStorage;
        impl execution_storage::ReadyAllocationVerifier for NoStorage {
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
        let cli = parse(&["--agent-user", "1234:5678"]);
        let user = resolve_agent_user(&cli.agent_user, cli.allow_root_agent).unwrap();
        let runtime = runtime_factory(
            &cli,
            &user,
            Arc::new(NoStorage),
            TrustedVerifierImage::new(format!("sha256:{}", "a".repeat(64)), "/usr/bin/stat")
                .unwrap(),
            Arc::new(
                LocalCredentialBroker::new(std::env::temp_dir(), chrono::Duration::minutes(15))
                    .unwrap(),
            ),
        )
        .unwrap();
        let harness = harness_factory(&cli, &user, Arc::new(NoStorage)).unwrap();
        assert_eq!(runtime.agent_user(), Some("1234:5678"));
        assert_eq!(harness.agent_user(), Some("1234:5678"));
        // A user that does not validate cannot reach either factory.
        assert!(harness_factory(&cli, "root", Arc::new(NoStorage)).is_err());
    }

    #[test]
    fn resolve_relay_refuses_without_a_relay_unless_egress_is_allowed() {
        // Neither relay flag nor the opt-out: refuse, naming all three flags.
        let bare = parse_raw(&[]);
        assert!(!bare.allow_agent_egress);
        let error = validate_cli(&bare).unwrap_err().to_string();
        for flag in [
            "--inference-upstream",
            "--inference-relay-image",
            "--allow-agent-egress",
        ] {
            assert!(error.contains(flag), "{error}");
        }
        assert!(resolve_relay(&bare)
            .unwrap_err()
            .to_string()
            .contains("--inference-upstream"));
        // The explicit opt-out starts, with no relay.
        let egress = parse_raw(&["--allow-agent-egress"]);
        assert!(validate_cli(&egress).is_ok());
        assert_eq!(resolve_relay(&egress).unwrap(), None);
    }

    #[test]
    fn resolve_relay_makes_a_relay_from_both_flags_with_the_default_or_a_given_port() {
        let both = parse_raw(&[
            "--inference-upstream",
            "gateway.local:8080",
            "--inference-relay-image",
            "relay:1",
        ]);
        assert!(validate_cli(&both).is_ok());
        assert_eq!(
            resolve_relay(&both).unwrap(),
            Some(runtime_docker::InferenceRelay {
                image: "relay:1".into(),
                upstream: "gateway.local:8080".into(),
                listen_port: 8080,
            })
        );
        let port = parse_raw(&[
            "--inference-upstream",
            "gateway.local:8080",
            "--inference-relay-image",
            "relay:1",
            "--inference-relay-port",
            "9000",
        ]);
        assert_eq!(resolve_relay(&port).unwrap().unwrap().listen_port, 9000);
    }

    #[test]
    fn resolve_relay_refuses_half_contradictory_or_malformed_relays_at_startup() {
        for extra in [
            vec!["--inference-upstream", "gateway.local:8080"],
            vec!["--inference-relay-image", "relay:1"],
            vec![
                "--inference-upstream",
                "gateway.local:8080",
                "--inference-relay-image",
                "relay:1",
                "--allow-agent-egress",
            ],
            vec![
                "--inference-upstream",
                "gateway.local",
                "--inference-relay-image",
                "relay:1",
            ],
            vec![
                "--inference-upstream",
                "http://gateway.local:8080",
                "--inference-relay-image",
                "relay:1",
            ],
        ] {
            let cli = parse_raw(&extra);
            assert!(resolve_relay(&cli).is_err(), "{extra:?}");
            assert!(validate_cli(&cli).is_err(), "{extra:?}");
        }
    }

    #[test]
    fn the_relay_flags_reach_the_runtime_factory() {
        #[derive(Debug)]
        struct NoStorage;
        impl execution_storage::ReadyAllocationVerifier for NoStorage {
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
        let build = |cli: &Cli| {
            runtime_factory(
                cli,
                &cli.agent_user,
                Arc::new(NoStorage),
                TrustedVerifierImage::new(format!("sha256:{}", "a".repeat(64)), "/usr/bin/stat")
                    .unwrap(),
                Arc::new(
                    LocalCredentialBroker::new(std::env::temp_dir(), chrono::Duration::minutes(15))
                        .unwrap(),
                ),
            )
        };
        assert!(build(&parse_raw(&["--allow-agent-egress"]))
            .unwrap()
            .inference_relay()
            .is_none());
        let cli = parse_raw(&[
            "--inference-upstream",
            "gateway.local:8080",
            "--inference-relay-image",
            "relay:1",
            "--inference-relay-port",
            "9001",
        ]);
        let factory = build(&cli).unwrap();
        let relay = factory
            .inference_relay()
            .expect("relay reaches the factory");
        assert_eq!(
            (
                relay.image.as_str(),
                relay.upstream.as_str(),
                relay.listen_port
            ),
            ("relay:1", "gateway.local:8080", 9001)
        );
        // A configuration that would run without a relay cannot build a factory.
        assert!(build(&parse_raw(&[])).is_err());
    }

    #[test]
    fn the_relay_flags_have_environment_twins() {
        // clap reads these from the environment when the flag is absent.
        let cmd = <Cli as clap::CommandFactory>::command();
        for (long, env) in [
            ("inference-upstream", "AUTOSPEC_INFERENCE_UPSTREAM"),
            ("inference-relay-image", "AUTOSPEC_INFERENCE_RELAY_IMAGE"),
            ("inference-relay-port", "AUTOSPEC_INFERENCE_RELAY_PORT"),
            ("allow-agent-egress", "AUTOSPEC_ALLOW_AGENT_EGRESS"),
        ] {
            let arg = cmd
                .get_arguments()
                .find(|arg| arg.get_long() == Some(long))
                .unwrap_or_else(|| panic!("--{long} exists"));
            assert_eq!(arg.get_env().and_then(|e| e.to_str()), Some(env), "{long}");
        }
    }

    #[test]
    fn startup_rejects_an_unsafe_cache_root_or_size() {
        assert!(validate_cli(&parse(&["--build-cache-root", "relative/cache"])).is_err());
        assert!(validate_cli(&parse(&[
            "--state-root",
            "/var/lib/autospec",
            "--build-cache-root",
            "/var/lib/autospec/executions/x/cache"
        ]))
        .is_err());
        assert!(validate_cli(&parse(&[
            "--build-cache-root",
            "/var/cache/autospec",
            "--build-cache-max-gib",
            "0"
        ]))
        .is_err());
        assert!(validate_cli(&parse(&["--build-cache-root", "/var/cache/autospec"])).is_ok());
        assert!(validate_cli(&parse(&[])).is_ok());
    }

    fn counting_collector(
        started: Arc<std::sync::atomic::AtomicUsize>,
        hold: Option<Arc<tokio::sync::Notify>>,
    ) -> CacheCollector {
        CacheCollector::new(Arc::new(move || {
            started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let hold = hold.clone();
            tokio::spawn(async move {
                if let Some(hold) = hold {
                    hold.notified().await;
                }
            })
        }))
    }

    #[tokio::test]
    async fn the_collector_runs_on_warn_and_stop_but_not_ok_and_never_overlaps() {
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hold = Arc::new(tokio::sync::Notify::new());
        let mut collector = counting_collector(started.clone(), Some(hold.clone()));
        let runs = || started.load(std::sync::atomic::Ordering::SeqCst);
        collector.on_verdict(DiskVerdict::Ok);
        assert_eq!(runs(), 0);
        collector.on_verdict(DiskVerdict::Warn);
        assert_eq!(runs(), 1);
        collector.on_verdict(DiskVerdict::Stop("disk-pressure-bytes"));
        assert_eq!(runs(), 1, "a pass is still running");
        hold.notify_one();
        for _ in 0..100 {
            if collector.running.as_ref().is_some_and(|t| t.is_finished()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        collector.on_verdict(DiskVerdict::Stop("disk-pressure-bytes"));
        assert_eq!(runs(), 2);
    }

    #[test]
    fn a_real_collector_exists_only_with_a_configured_cache_root() {
        let none = parse(&[]);
        assert!(cache_collector(&none).is_none());
        let some = parse(&["--build-cache-root", "/var/cache/autospec"]);
        // Constructing it must not spawn anything (it only captures the config).
        assert!(cache_collector(&some).is_some());
    }

    async fn heartbeats_with_pressure(used: &'static [u64], beats: usize) -> usize {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, mut bodies) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(fake_controller(listener, sender));
        let state_root = tempfile::tempdir().unwrap();
        let mut config = cli();
        config.controller = format!("http://{address}");
        config.state_root = state_root.path().to_owned();
        let worker = advertisement(&config, None, Vec::new()).unwrap();
        let sampler = sampler_with_used_percent(used, Arc::new(Default::default()));
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let collector = counting_collector(started.clone(), None);
        let client = reqwest::Client::new();
        let control = tokio::spawn(async move {
            run_control_loop(
                &config,
                worker,
                None,
                Secret("t".into()),
                client,
                sampler,
                Some(collector),
            )
            .await
        });
        for _ in 0..beats {
            tokio::time::timeout(Duration::from_secs(30), bodies.recv())
                .await
                .expect("heartbeat arrives")
                .expect("server alive");
        }
        control.abort();
        server.abort();
        started.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn the_control_loop_triggers_cache_gc_on_warn_and_stop_heartbeats() {
        // 85 % is Warn (not advertised as an error) and must still collect.
        assert!(heartbeats_with_pressure(&[85], 1).await >= 1);
        // 95 % is Stop.
        assert!(heartbeats_with_pressure(&[95], 1).await >= 1);
    }

    #[tokio::test]
    async fn the_control_loop_does_not_collect_without_pressure() {
        assert_eq!(heartbeats_with_pressure(&[10], 1).await, 0);
    }
}
