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
    CleanupAuthorityStore, ExecutionStore, PgCleanupAuthorityStore, PgExecutionStore,
    PgReservationStore, ReservationStore,
};
use orchestrator_worker::{
    ExecutionTask, FilesystemEvidenceStore, SystemExecutionLifecycle, SystemRecoveryConfig,
    VerifiedDockerRuntimeFactory, VerifiedPiHarnessFactory, Worker,
};
use runtime_docker::TrustedVerifierImage;
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
    #[arg(long, default_value = "pi")]
    pi_executable: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let storage = build_storage(&cli)?;
    let proof = match probe_capabilities(&cli, storage.as_ref()) {
        Ok(proof) => Some(proof),
        Err(error) => {
            tracing::error!(worker_id = %cli.worker_id, %error, "worker capability proof unavailable; advertising no runtime capacity");
            None
        }
    };
    let worker = advertisement(&cli, proof)?;
    let executions = Arc::new(PgExecutionStore::connect(&cli.database_url).await?);
    let reservations = Arc::new(PgReservationStore::connect(&cli.database_url).await?);
    let cleanup = Arc::new(PgCleanupAuthorityStore::connect(&cli.database_url).await?);
    let verifier: Arc<dyn execution_storage::ReadyAllocationVerifier> = storage.clone();
    let worktrees = Arc::new(GitWorktreeManager::with_clone_base_and_verifier(
        &cli.state_root,
        &cli.clone_base,
        verifier.clone(),
    ));
    let trusted_verifier =
        TrustedVerifierImage::new(&cli.docker_verifier_image, &cli.docker_verifier_command)?;
    let runtimes = Arc::new(VerifiedDockerRuntimeFactory::new(
        cli.docker_socket.clone(),
        PathBuf::from(&cli.docker_binary),
        verifier.clone(),
        trusted_verifier,
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
        Arc::new(FilesystemEvidenceStore::new(&cli.state_root)),
    ));
    let execution_worker = Arc::new(Worker::new(
        lifecycle,
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let token = Secret(cli.worker_token);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
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
    let mut tasks: Vec<ExecutionTask> = Vec::new();
    for authority in cleanup.list_for_worker(&worker.id).await? {
        let execution = match executions.get(&authority.execution_id).await {
            Ok(execution) => execution,
            Err(error) => {
                tracing::error!(
                    worker_id = %worker.id,
                    execution_id = %authority.execution_id,
                    attempt_id = %authority.attempt_id,
                    %error,
                    "cleanup authority has no reconstructable execution; retaining it for recovery"
                );
                continue;
            }
        };
        let same_attempt = execution.worker_id.as_ref() == Some(&worker.id)
            && execution.attempt_id.as_ref() == Some(&authority.attempt_id);
        if same_attempt
            && execution.state == orchestrator_core::ExecutionState::Running
            && matches!(authority.phase.as_str(), "PI_STARTED" | "RUNNING")
        {
            tracing::info!(
                worker_id = %worker.id,
                execution_id = %execution.id,
                attempt_id = %authority.attempt_id,
                session_id = ?execution.session_id,
                "adopting durable Pi execution after worker restart"
            );
            tasks.push(execution_worker.clone().spawn_adopted(execution));
            continue;
        }
        if same_attempt
            && execution.state == orchestrator_core::ExecutionState::ReviewReady
            && authority.phase == "REVIEW_READY"
        {
            tracing::info!(
                worker_id = %worker.id,
                execution_id = %execution.id,
                attempt_id = %authority.attempt_id,
                "retaining resumable ReviewReady execution for later attach or explicit cleanup"
            );
            continue;
        }
        match execution_worker
            .recover_cleanup_authority(&authority, &execution)
            .await
        {
            Ok(()) => tracing::info!(
                worker_id = %worker.id,
                execution_id = %authority.execution_id,
                attempt_id = %authority.attempt_id,
                phase = %authority.phase,
                "recovered abandoned execution authority"
            ),
            Err(error) => tracing::error!(
                worker_id = %worker.id,
                execution_id = %authority.execution_id,
                attempt_id = %authority.attempt_id,
                phase = %authority.phase,
                %error,
                "abandoned authority recovery remains unresolved"
            ),
        }
    }
    loop {
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
        while registered && tasks.len() < usize::try_from(cli.concurrency)? {
            match reservations.reserve_next(&worker.id).await {
                Ok(Some(reservation)) => {
                    tasks.push(execution_worker.clone().spawn(reservation.execution));
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

fn advertisement(cli: &Cli, proof: Option<WorkerCapabilityProof>) -> Result<WorkerAdvertisement> {
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
        },
        capability_proof: proof,
    })
}

fn probe_capabilities(
    cli: &Cli,
    storage: &dyn ExecutionStorageManager,
) -> Result<WorkerCapabilityProof> {
    let capability = storage.probe(cli.disk_gib)?;
    Ok(WorkerCapabilityProof {
        storage_backend: capability.backend.backend,
        storage_pool_identity: capability.backend.pool_identity,
        docker_daemon_id: capability.docker_bind.daemon_id,
        docker_verifier: capability.docker_bind.verifier,
        docker_method_version: capability.docker_bind.method_version,
    })
}

fn build_storage(cli: &Cli) -> Result<Arc<ExecutionStorage>> {
    let runner = Arc::new(ProcessCommandRunner);
    let backend: Box<dyn StorageBackend> = match cli.storage_kind {
        StorageKind::Apfs => Box::new(ApfsBackend::new(&cli.storage_pool, runner)?),
        StorageKind::Lvm => Box::new(LvmBackend::new(&cli.storage_pool, runner)?),
    };
    let verifier =
        TrustedVerifierImage::new(&cli.docker_verifier_image, &cli.docker_verifier_command)?;
    let output = docker_command(&cli.docker_binary, cli.docker_socket.as_deref())
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .with_context(|| format!("execute {} info", cli.docker_binary))?;
    if !output.status.success() {
        anyhow::bail!(
            "Docker daemon identity probe failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let daemon_id = String::from_utf8(output.stdout)?.trim().to_owned();
    if daemon_id.is_empty() {
        anyhow::bail!("Docker daemon identity probe returned empty ID");
    }
    Ok(Arc::new(ExecutionStorage::new(
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
    )?))
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
            pi_executable: "pi".into(),
        }
    }

    #[test]
    fn missing_storage_proof_forces_offline_and_token_debug_is_redacted() {
        let worker = advertisement(&cli(), None).unwrap();
        assert!(worker.capability_proof.is_none());
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
        let worker = advertisement(&cli(), Some(proof)).unwrap();
        assert_eq!(worker.capabilities.runtimes, vec![RuntimeKind::Docker]);
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
}
