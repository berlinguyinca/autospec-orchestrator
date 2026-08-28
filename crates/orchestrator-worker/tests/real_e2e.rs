use chrono::Utc;
use execution_storage::{
    BackendCapability, BackendIdentity, BackendState, DockerBindCapability, DockerBindProof,
    DockerBindVerifier, ExecutionLayout, ExecutionStorage, StorageBackend, StorageError,
};
use git_worktree::GitWorktreeManager;
use orchestrator_core::{
    AgentAssignment, Execution, ExecutionId, ExecutionManifest, ExecutionState, HarnessKind,
    ModelPolicy, OwnershipLabels, PersistenceMode, RepositoryReference, Role, RuntimeKind,
    RuntimeRequirement, TaskPacket, WorkerCapabilities, WorkerCapabilityProof, WorkerId,
    WorkerRegistration, WorkerState,
};
use orchestrator_persistence::{
    CleanupAuthorityStore, ExecutionStore, PgCleanupAuthorityStore, PgExecutionStore,
    PgReservationStore, PgWorkerStore, ReservationStore, WorkerStore,
};
use orchestrator_worker::{
    FilesystemEvidenceStore, SystemExecutionLifecycle, SystemRecoveryConfig,
    VerifiedDockerRuntimeFactory, VerifiedPiHarnessFactory, Worker,
};
use runtime_docker::TrustedVerifierImage;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug, Default)]
struct FixedFilesystemBackend;

impl StorageBackend for FixedFilesystemBackend {
    fn key(&self, labels: &OwnershipLabels) -> String {
        format!("fixed:{}", labels.execution_id)
    }

    fn probe(&self, _: u64) -> Result<BackendCapability, StorageError> {
        Ok(BackendCapability {
            backend: "fixed-test-filesystem".into(),
            pool_identity: "fixed-test-pool".into(),
            reservable_bytes: u64::MAX,
        })
    }

    fn discover(
        &self,
        layout: &ExecutionLayout,
        token: &str,
        _: u64,
    ) -> Result<Option<BackendIdentity>, StorageError> {
        if layout.root.exists() {
            Ok(Some(fixed_identity(layout, token)?))
        } else {
            Ok(None)
        }
    }

    fn create(
        &self,
        layout: &ExecutionLayout,
        _: &OwnershipLabels,
        token: &str,
        _: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let mount = format!(
            "type=bind,src={},dst=/proof,readonly",
            layout.root.display()
        );
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
                "--mount",
                &mount,
                "alpine:3.20",
                "/bin/stat",
                "-c",
                "%d:%i",
                "/proof",
            ])
            .output()
            .map_err(|error| StorageError::Command(error.to_string()))?;
        if !output.status.success() {
            return Err(StorageError::Command(
                String::from_utf8_lossy(&output.stderr).into(),
            ));
        }
        let filesystem_id = String::from_utf8(output.stdout)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
            .trim()
            .to_owned();
        fixed_identity_with_filesystem(layout, token, filesystem_id)
    }

    fn prepare(
        &self,
        _: &ExecutionLayout,
        identity: &BackendIdentity,
        _: u64,
    ) -> Result<BackendIdentity, StorageError> {
        Ok(identity.clone())
    }

    fn mount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        _: u64,
    ) -> Result<(), StorageError> {
        let _ = fs::remove_file(fixed_marker(layout, "unmounted"));
        let _ = fs::remove_file(fixed_marker(layout, "removed"));
        let _ = identity;
        Ok(())
    }

    fn state(
        &self,
        layout: &ExecutionLayout,
        _: &BackendIdentity,
        _: u64,
    ) -> Result<BackendState, StorageError> {
        if fixed_marker(layout, "removed").exists() || !layout.root.exists() {
            Ok(BackendState::Absent)
        } else if fixed_marker(layout, "unmounted").exists() {
            Ok(BackendState::Unmounted)
        } else {
            Ok(BackendState::Mounted)
        }
    }

    fn unmount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        fs::remove_dir_all(&layout.root)
            .and_then(|()| fs::create_dir(&layout.root))
            .map_err(|error| StorageError::Command(error.to_string()))?;
        #[cfg(unix)]
        fs::set_permissions(&layout.root, fs::Permissions::from_mode(0o700))
            .map_err(|error| StorageError::Command(error.to_string()))?;
        fs::write(
            fixed_marker(layout, "unmounted"),
            identity.ownership_token(),
        )
        .map_err(|error| StorageError::Command(error.to_string()))?;
        Ok(())
    }

    fn remove(&self, identity: &BackendIdentity) -> Result<(), StorageError> {
        let BackendIdentity::Apfs { volume_name, .. } = identity else {
            return Err(StorageError::IdentityMismatch(
                "fixed backend received a non-APFS test identity".into(),
            ));
        };
        let root = Path::new(volume_name);
        fs::write(
            root.with_extension("fixed-removed"),
            identity.ownership_token(),
        )
        .map_err(|error| StorageError::Command(error.to_string()))?;
        Ok(())
    }
}

fn fixed_marker(layout: &ExecutionLayout, phase: &str) -> PathBuf {
    layout.root.with_extension(format!("fixed-{phase}"))
}

fn fixed_identity(layout: &ExecutionLayout, token: &str) -> Result<BackendIdentity, StorageError> {
    let mount = format!(
        "type=bind,src={},dst=/proof,readonly",
        layout.root.display()
    );
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--mount",
            &mount,
            "alpine:3.20",
            "/bin/stat",
            "-c",
            "%d:%i",
            "/proof",
        ])
        .output()
        .map_err(|error| StorageError::Command(error.to_string()))?;
    if !output.status.success() {
        return Err(StorageError::Command(
            String::from_utf8_lossy(&output.stderr).into(),
        ));
    }
    let filesystem_id = String::from_utf8(output.stdout)
        .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
        .trim()
        .to_owned();
    fixed_identity_with_filesystem(layout, token, filesystem_id)
}

fn fixed_identity_with_filesystem(
    layout: &ExecutionLayout,
    token: &str,
    filesystem_id: String,
) -> Result<BackendIdentity, StorageError> {
    Ok(BackendIdentity::Apfs {
        container: "fixed-container".into(),
        container_uuid: "fixed-container-uuid".into(),
        volume: format!("fixed-{token}"),
        volume_name: layout.root.to_string_lossy().into_owned(),
        volume_uuid: filesystem_id,
        ownership_token: token.into(),
    })
}

#[derive(Debug)]
struct TestDockerBindVerifier {
    daemon_id: String,
    image_id: String,
    method: String,
}

impl DockerBindVerifier for TestDockerBindVerifier {
    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: self.daemon_id.clone(),
            verifier: self.image_id.clone(),
            method_version: self.method.clone(),
        })
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        let source = source
            .canonicalize()
            .map_err(|error| StorageError::Unavailable(error.to_string()))?;
        let mount = format!("type=bind,src={},dst=/proof,readonly", source.display());
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
                "--mount",
                &mount,
                &self.image_id,
                "/bin/stat",
                "-c",
                "%d:%i",
                "/proof",
            ])
            .output()
            .map_err(|error| StorageError::Command(error.to_string()))?;
        if !output.status.success() {
            return Err(StorageError::Command(
                String::from_utf8_lossy(&output.stderr).into(),
            ));
        }
        let filesystem_id = String::from_utf8(output.stdout)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
            .trim()
            .to_owned();
        Ok(DockerBindProof {
            daemon_id: self.daemon_id.clone(),
            verifier: self.image_id.clone(),
            method_version: self.method.clone(),
            source_path: source,
            filesystem_id,
        })
    }
}

#[tokio::test]
async fn crashed_worker_is_adopted_across_postgres_git_docker_pi_evidence_and_cleanup() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task5 E2E: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task5 E2E: Docker or alpine:3.20 is unavailable");
        return;
    };
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task5-e2e-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote_root = root.join("remotes");
    create_stub_repository(&remote_root);

    let trusted = TrustedVerifierImage::new(&image_id, "/bin/stat").unwrap();
    let method = trusted.proof_method();

    let worker_id = WorkerId::new(format!("task5-e2e-worker-{suffix}"));
    let execution_id = ExecutionId::new(format!("task5-e2e-{suffix}"));
    let e2e_capability = format!("task5-e2e-capability-{suffix}");
    let labels = OwnershipLabels {
        execution_id: execution_id.clone(),
        worker_id: worker_id.clone(),
        repository: "owner/repo".into(),
        issue: Some("task5-e2e".into()),
    };
    let worker_registration = WorkerRegistration {
        id: worker_id.clone(),
        capabilities: WorkerCapabilities {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            cpu: 2,
            memory_mib: 2048,
            disk_gib: 1,
            runtimes: vec![RuntimeKind::Docker],
            capabilities: vec!["docker".into(), e2e_capability.clone()],
            max_concurrent_executions: 1,
        },
        state: WorkerState::Ready,
        running_executions: 0,
        last_heartbeat: Utc::now(),
        capability_proof: Some(WorkerCapabilityProof {
            storage_backend: "fixed-test-filesystem".into(),
            storage_pool_identity: "fixed-test-pool".into(),
            docker_daemon_id: daemon_id.clone(),
            docker_verifier: image_id.clone(),
            docker_method_version: method,
        }),
    };
    let workers = PgWorkerStore::connect(&database_url).await.unwrap();
    workers.register(&worker_registration).await.unwrap();
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    executions
        .insert(&queued_execution(
            execution_id.clone(),
            labels,
            image_id.clone(),
            e2e_capability,
        ))
        .await
        .unwrap();
    let crashed_child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_process_helper", "--nocapture"])
        .env("AUTOSPEC_TASK5_CRASH_HELPER", "1")
        .env("AUTOSPEC_DATABASE_URL", &database_url)
        .env("AUTOSPEC_TASK5_ROOT", &root)
        .env("AUTOSPEC_TASK5_REMOTE", &remote_root)
        .env("AUTOSPEC_TASK5_DAEMON", &daemon_id)
        .env("AUTOSPEC_TASK5_IMAGE", &image_id)
        .env("AUTOSPEC_TASK5_WORKER", worker_id.as_str())
        .status()
        .unwrap();
    assert_eq!(crashed_child.code(), Some(77));
    let crashed = wait_for_running_session(executions.as_ref(), &execution_id).await;
    let original_attempt = crashed.attempt_id.clone();
    let original_session = crashed.session_id.clone();
    assert_eq!(
        execution_storage::ExecutionLifecycleHoldStore::new(&root)
            .unwrap()
            .list(&execution_id)
            .unwrap()
            .len(),
        1
    );
    let adopted = executions.get(&execution_id).await.unwrap();
    let replacement = Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let result = replacement
        .clone()
        .spawn_adopted(adopted)
        .join()
        .await
        .unwrap();

    assert_eq!(result.state, ExecutionState::ReviewReady);
    assert!(result
        .diff_artifact
        .as_ref()
        .is_some_and(|path| Path::new(path).is_file()));
    assert!(fs::read_to_string(result.diff_artifact.as_ref().unwrap())
        .unwrap()
        .contains("+worker-e2e-2"));
    assert_eq!(
        executions.get(&execution_id).await.unwrap().state,
        ExecutionState::ReviewReady
    );
    assert_eq!(
        executions.get(&execution_id).await.unwrap().attempt_id,
        original_attempt
    );
    assert_eq!(
        executions.get(&execution_id).await.unwrap().session_id,
        original_session
    );
    let released_layout = ExecutionLayout::new(&root, &execution_id).unwrap();
    assert!(released_layout.root.exists());
    assert!(execution_storage::JournalStore::new(&root)
        .unwrap()
        .read(&released_layout)
        .is_ok());
    let retained = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=autospec.execution_id={execution_id}"),
        ])
        .output()
        .unwrap();
    assert!(!String::from_utf8_lossy(&retained.stdout).trim().is_empty());
    assert_eq!(
        reservations
            .list_for_worker(&worker_id)
            .await
            .unwrap()
            .len(),
        1
    );
    let authorities = cleanup.list_for_worker(&worker_id).await.unwrap();
    assert_eq!(authorities.len(), 1);
    assert_eq!(authorities[0].phase, "REVIEW_READY");
    assert!(authorities[0].handles.get("receipt").is_some());
    assert!(authorities[0].handles.get("runtime").is_some());
    assert!(authorities[0].handles.get("session").is_some());

    let retained_execution = executions.get(&execution_id).await.unwrap();
    replacement
        .recover_cleanup_authority(&authorities[0], &retained_execution)
        .await
        .unwrap();
    assert!(!released_layout.root.exists());
    assert!(execution_storage::JournalStore::new(&root)
        .unwrap()
        .read(&released_layout)
        .is_err());
    let leaked = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=autospec.execution_id={execution_id}"),
        ])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&leaked.stdout).trim().is_empty());
    assert!(reservations
        .list_for_worker(&worker_id)
        .await
        .unwrap()
        .is_empty());
    assert!(cleanup
        .list_for_worker(&worker_id)
        .await
        .unwrap()
        .is_empty());
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    sqlx::query("DELETE FROM cleanup_authorities WHERE execution_id = $1")
        .bind(execution_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM execution_events WHERE execution_id = $1")
        .bind(execution_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM executions WHERE id = $1")
        .bind(execution_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM workers WHERE id = $1")
        .bind(worker_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn crash_process_helper() {
    if std::env::var_os("AUTOSPEC_TASK5_CRASH_HELPER").is_none() {
        return;
    }
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let root = PathBuf::from(std::env::var_os("AUTOSPEC_TASK5_ROOT").unwrap());
    let remote = PathBuf::from(std::env::var_os("AUTOSPEC_TASK5_REMOTE").unwrap());
    let daemon = std::env::var("AUTOSPEC_TASK5_DAEMON").unwrap();
    let image = std::env::var("AUTOSPEC_TASK5_IMAGE").unwrap();
    let worker_id = WorkerId::new(std::env::var("AUTOSPEC_TASK5_WORKER").unwrap());
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let assigned = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .expect("child should reserve queued execution")
        .execution;
    let execution_id = assigned.id.clone();
    let worker = Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote, &daemon, &image),
        executions.clone(),
        reservations,
        cleanup,
    ));
    let running_worker = worker.clone();
    tokio::spawn(async move { running_worker.run(&assigned).await });
    wait_for_running_session(executions.as_ref(), &execution_id).await;
    std::process::exit(77);
}

async fn wait_for_running_session(store: &PgExecutionStore, id: &ExecutionId) -> Execution {
    for _ in 0..300 {
        let execution = store.get(id).await.unwrap();
        if execution.state == ExecutionState::Running && execution.session_id.is_some() {
            return execution;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("execution did not reach durable Running session");
}

fn build_system_lifecycle(
    root: &Path,
    remote_root: &Path,
    daemon_id: &str,
    image_id: &str,
) -> Arc<SystemExecutionLifecycle> {
    let trusted = TrustedVerifierImage::new(image_id, "/bin/stat").unwrap();
    let method = trusted.proof_method();
    let storage = Arc::new(
        ExecutionStorage::new(
            root,
            Box::new(FixedFilesystemBackend),
            Box::new(TestDockerBindVerifier {
                daemon_id: daemon_id.into(),
                image_id: image_id.into(),
                method,
            }),
        )
        .unwrap(),
    );
    let verifier: Arc<dyn execution_storage::ReadyAllocationVerifier> = storage.clone();
    Arc::new(SystemExecutionLifecycle::new(
        SystemRecoveryConfig {
            state_root: root.into(),
            verifier: verifier.clone(),
            docker_binary: PathBuf::from("docker"),
            docker_socket: None,
        },
        storage,
        Arc::new(GitWorktreeManager::with_clone_base_and_verifier(
            root,
            format!("file://{}", remote_root.display()),
            verifier.clone(),
        )),
        Arc::new(VerifiedDockerRuntimeFactory::new(
            None,
            PathBuf::from("docker"),
            verifier.clone(),
            trusted,
        )),
        Arc::new(VerifiedPiHarnessFactory::new(
            verifier,
            None,
            PathBuf::from("docker"),
            "/workspace/pi".into(),
            Vec::new(),
            Vec::new(),
        )),
        Arc::new(FilesystemEvidenceStore::new(root)),
    ))
}

fn queued_execution(
    id: ExecutionId,
    labels: OwnershipLabels,
    image: String,
    capability: String,
) -> Execution {
    Execution {
        id,
        role: Role::Implementation,
        state: ExecutionState::Queued,
        manifest: ExecutionManifest {
            api_version: orchestrator_core::MANIFEST_API_VERSION.into(),
            role: Role::Implementation,
            task: None,
            repository: RepositoryReference {
                repo: "owner/repo".into(),
                base_ref: "main".into(),
                base_sha: None,
                branch: Some("task5-e2e".into()),
            },
            agent: AgentAssignment {
                harness: HarnessKind::Pi,
                model_policy: ModelPolicy {
                    provider: "inferweave".into(),
                    preferred: Vec::new(),
                    alternatives: Vec::new(),
                    fallback_class: None,
                },
            },
            runtime: RuntimeRequirement {
                kind: RuntimeKind::Docker,
                image: Some(image),
                os: None,
                cpu: 1,
                memory_mib: 256,
                disk_gib: 1,
                capabilities: vec![capability],
            },
            services: Vec::new(),
            persistence: PersistenceMode::Resumable,
            task_packet: Some(TaskPacket {
                goal: "produce a real worker diff".into(),
                acceptance_criteria: vec!["result.txt exists".into()],
                non_goals: Vec::new(),
                relevant_context: Vec::new(),
                required_tests: Vec::new(),
                role_skill: None,
            }),
        },
        worker_id: None,
        attempt_id: None,
        session_id: None,
        worktree_path: None,
        labels,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        result: None,
    }
}

fn docker_capability() -> Option<(String, String)> {
    let daemon = Command::new("docker")
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .ok()?;
    let image = Command::new("docker")
        .args(["image", "inspect", "alpine:3.20", "--format", "{{.Id}}"])
        .output()
        .ok()?;
    if !daemon.status.success() || !image.status.success() {
        return None;
    }
    Some((
        String::from_utf8(daemon.stdout).ok()?.trim().into(),
        String::from_utf8(image.stdout).ok()?.trim().into(),
    ))
}

fn initialize_state_root(root: &Path) {
    fs::create_dir_all(root.join("execution-storage")).unwrap();
    fs::create_dir(root.join("executions")).unwrap();
    #[cfg(unix)]
    for path in [
        root,
        &root.join("execution-storage"),
        &root.join("executions"),
    ] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

fn create_stub_repository(remote_root: &Path) {
    let remote = remote_root.join("owner/repo.git");
    fs::create_dir_all(remote.parent().unwrap()).unwrap();
    run(Command::new("git").args(["init", "--bare", remote.to_str().unwrap()]));
    let seed = remote_root.join("seed");
    run(Command::new("git").args(["init", seed.to_str().unwrap()]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["config", "user.name", "Task5 E2E"]));
    run(Command::new("git").current_dir(&seed).args([
        "config",
        "user.email",
        "task5@example.invalid",
    ]));
    let pi = seed.join("pi");
    fs::write(
        &pi,
        "#!/bin/sh\ncount=1\nif test -f /session/invocations; then count=$(( $(cat /session/invocations) + 1 )); fi\nprintf '%s\\n' \"$count\" > /session/invocations\nprintf '%s\\n' '{\"type\":\"session\",\"id\":\"e2e\"}' '{\"type\":\"agent_start\"}' '{\"type\":\"turn_start\"}'\nif test \"$count\" -eq 1; then while :; do sleep 1; done; fi\nprintf 'worker-e2e-%s\\n' \"$count\" > /workspace/result.txt\nprintf '%s\\n' '{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"done\"}],\"stopReason\":\"stop\"}}' '{\"type\":\"agent_settled\"}'\n",
    )
    .unwrap();
    #[cfg(unix)]
    fs::set_permissions(&pi, fs::Permissions::from_mode(0o755)).unwrap();
    run(Command::new("git").current_dir(&seed).args(["add", "pi"]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["commit", "-m", "seed"]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["branch", "-M", "main"]));
    run(Command::new("git").current_dir(&seed).args([
        "remote",
        "add",
        "origin",
        remote.to_str().unwrap(),
    ]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["push", "-u", "origin", "main"]));
}

fn run(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
