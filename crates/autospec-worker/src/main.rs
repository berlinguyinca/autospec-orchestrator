//! `autospec-worker` — authenticated worker registration and heartbeat daemon.

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
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
use runtime_traits::CredentialBroker;
use std::{
    fmt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

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
    let prepared = prepare_execution_plane(&cli).await;
    let (worker, execution_plane) = advertisement_from_preparation(&cli, prepared)?;
    let token = Secret(cli.worker_token.clone());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    run_control_loop(&cli, worker, execution_plane, token, client).await
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
    let runtimes = Arc::new(VerifiedDockerRuntimeFactory::new(
        cli.docker_socket.clone(),
        PathBuf::from(&cli.docker_binary),
        verifier.clone(),
        trusted_verifier,
        credential_broker,
    ));
    let harnesses = Arc::new(VerifiedPiHarnessFactory::new(
        verifier.clone(),
        cli.docker_socket.clone(),
        PathBuf::from(&cli.docker_binary),
        cli.pi_executable.clone(),
        vec!["read".into(), "bash".into(), "edit".into(), "write".into()],
        Vec::new(),
    ));
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

async fn run_control_loop(
    cli: &Cli,
    mut worker: WorkerAdvertisement,
    mut execution_plane: Option<ExecutionPlane>,
    token: Secret,
    client: reqwest::Client,
) -> Result<()> {
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
            let request = if registered {
                client.post(&heartbeat_url)
            } else {
                client.post(&workers_url)
            };
            match request.bearer_auth(&token.0).json(&worker).send().await {
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
                vec!["docker".to_owned()]
            } else {
                Vec::new()
            },
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
            clone_base: "https://github.com".into(),
            docker_socket: None,
            docker_proxy_port: 2375,
            host_docker: false,
            pi_executable: "pi".into(),
            allow_local_development_credentials: false,
        }
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
    fn preparation_failures_are_typed_and_diagnostics_never_echo_sources() {
        let secret = "postgres://user:password@database/private";
        let failure = PreparationFailure::persistence(secret);
        assert_eq!(failure.kind, PreparationFailureKind::Persistence);
        assert_eq!(failure.health_code(), "worker-preparation-unavailable");
        assert_eq!(failure.diagnostic(), "persistence connection failed");
        assert!(!format!("{failure}").contains(secret));

        let storage = PreparationFailure::storage(secret);
        assert_eq!(storage.health_code(), "storage-capability-unavailable");
        assert!(!format!("{storage:?}").contains(secret));

        let docker = PreparationFailure::docker(secret);
        assert_eq!(docker.health_code(), "docker-capability-unavailable");
        assert!(!format!("{docker:?}").contains(secret));
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

    #[tokio::test]
    async fn docker_construction_failure_still_produces_sanitized_offline_registration() {
        let mut cli = cli();
        cli.allow_local_development_credentials = true;
        cli.host_docker = true;
        cli.docker_socket = Some("tcp://docker-api:2375".into());
        cli.docker_binary = "/definitely/missing/autospec-docker".into();
        let prepared = prepare_execution_plane(&cli).await;
        assert!(prepared.is_err());
        let (worker, plane) = advertisement_from_preparation(&cli, prepared).unwrap();
        assert!(plane.is_none());
        assert!(worker.capability_proof.is_none());
        assert!(worker.capabilities.runtimes.is_empty());
        assert_eq!(
            worker.capabilities.health_errors,
            vec!["docker-capability-unavailable"]
        );
    }
}
