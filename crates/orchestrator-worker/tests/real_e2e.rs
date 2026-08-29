use chrono::Utc;
use execution_storage::{
    BackendCapability, BackendIdentity, BackendState, DockerBindCapability, DockerBindProof,
    DockerBindVerifier, ExecutionLayout, ExecutionStorage, StorageBackend, StorageError,
};
use git_worktree::{GitWorktreeManager, Worktree};
use harness_traits::SessionRef;
use orchestrator_api::{router, AppState};
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, Execution, ExecutionEvent, ExecutionId,
    ExecutionManifest, ExecutionResult, ExecutionState, HarnessKind, ModelPolicy, OwnershipLabels,
    PersistenceMode, RepositoryReference, Role, RuntimeKind, RuntimeRequirement, TaskPacket,
    WorkerAdvertisement, WorkerCapabilities, WorkerCapabilityProof, WorkerId, WorkerRegistration,
    WorkerState,
};
use orchestrator_persistence::{
    CleanupAuthorityStore, CleanupDisposition, CleanupStage, EventLog, ExecutionStore,
    PgArtifactStore, PgCleanupAuthorityStore, PgEventLog, PgExecutionStore, PgReservationStore,
    PgWorkerStore, ReservationStore, WorkerStore,
};
use orchestrator_worker::{
    ExecutionLifecycle, FilesystemEvidenceStore, SystemExecutionLifecycle, SystemRecoveryConfig,
    VerifiedDockerRuntimeFactory, VerifiedPiHarnessFactory, Worker,
};
use runtime_docker::TrustedVerifierImage;
use runtime_traits::EnvironmentHandle;
use sqlx::{Connection, PgConnection};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tokio::net::TcpListener;

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
    let _serial = acquire_real_test_lock(&database_url).await;
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
        0
    );
    let authorities = cleanup.list_for_worker(&worker_id).await.unwrap();
    assert_eq!(authorities.len(), 1);
    assert_eq!(authorities[0].phase, "RETAINED");
    assert!(authorities[0].handles.get("receipt").is_some());
    assert!(authorities[0].handles.get("runtime").is_some());
    assert!(authorities[0].handles.get("session").is_some());
    let event_log = PgEventLog::connect(&database_url).await.unwrap();
    assert_eq!(
        event_log
            .since(&execution_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ReviewReady))
            .count(),
        1,
        "restart adoption must publish ReviewReady exactly once"
    );

    let retained_execution = executions.get(&execution_id).await.unwrap();
    cleanup
        .transition(
            &execution_id,
            orchestrator_persistence::CleanupDisposition::Retained,
            orchestrator_persistence::CleanupDisposition::CleanupPending,
            &authorities[0].handles,
        )
        .await
        .unwrap();
    let requested = cleanup.get(&execution_id).await.unwrap();
    replacement
        .recover_cleanup_authority(&requested, &retained_execution)
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
    assert_eq!(
        event_log
            .since(&execution_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ReviewReady))
            .count(),
        1,
        "explicit retained cleanup must not republish ReviewReady"
    );
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
    sqlx::query(
        "DELETE FROM workers w
         WHERE id = $1
           AND NOT EXISTS (SELECT 1 FROM executions WHERE worker_id = w.id)
           AND NOT EXISTS (SELECT 1 FROM execution_attempts WHERE worker_id = w.id)",
    )
    .bind(worker_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn real_failure_stage_matrix_reconciles_without_resource_leaks() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task5 matrix: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task5 matrix: Docker or alpine:3.20 is unavailable");
        return;
    };
    for (stage_index, stage) in [
        "post_reservation",
        "post_storage",
        "mid_git_create",
        "post_runtime",
        "ephemeral_post_runtime",
        "git_deleted_pre_disposition",
        "storage_deleted_pre_disposition",
        "post_pi_pre_event",
        "review_ready_pre_retention",
        "post_retention",
        "post_finalize_pre_ack",
    ]
    .into_iter()
    .enumerate()
    {
        let suffix = format!(
            "matrix-{stage_index}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        );
        let root = std::env::temp_dir().join(format!("autospec-task5-{suffix}"));
        initialize_state_root(&root);
        let root = root.canonicalize().unwrap();
        let remote_root = root.join("remotes");
        create_stub_repository(&remote_root);
        let worker_id = WorkerId::new(format!("worker-{suffix}"));
        let execution_id = ExecutionId::new(format!("execution-{suffix}"));
        let capability = format!("capability-{suffix}");
        let labels = OwnershipLabels {
            execution_id: execution_id.clone(),
            worker_id: worker_id.clone(),
            repository: "owner/repo".into(),
            issue: Some("task5-matrix".into()),
        };
        let workers = PgWorkerStore::connect(&database_url).await.unwrap();
        workers
            .register(&matrix_worker_registration(
                worker_id.clone(),
                capability.clone(),
                &daemon_id,
                &image_id,
            ))
            .await
            .unwrap();
        let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
        let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
        let cleanup = Arc::new(
            PgCleanupAuthorityStore::connect(&database_url)
                .await
                .unwrap(),
        );
        let mut queued =
            queued_execution(execution_id.clone(), labels, image_id.clone(), capability);
        if stage == "ephemeral_post_runtime" {
            queued.manifest.persistence = PersistenceMode::Ephemeral;
        }
        executions.insert(&queued).await.unwrap();
        let crashed = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "failure_stage_process_helper", "--nocapture"])
            .env("AUTOSPEC_TASK5_MATRIX_HELPER", "1")
            .env("AUTOSPEC_TASK5_MATRIX_STAGE", stage)
            .env("AUTOSPEC_DATABASE_URL", &database_url)
            .env("AUTOSPEC_TASK5_ROOT", &root)
            .env("AUTOSPEC_TASK5_REMOTE", &remote_root)
            .env("AUTOSPEC_TASK5_DAEMON", &daemon_id)
            .env("AUTOSPEC_TASK5_IMAGE", &image_id)
            .env("AUTOSPEC_TASK5_WORKER", worker_id.as_str())
            .status()
            .unwrap();
        assert_eq!(crashed.code(), Some(77), "stage {stage}");
        if stage == "post_finalize_pre_ack" {
            assert_eq!(
                cleanup
                    .get(&execution_id)
                    .await
                    .unwrap()
                    .disposition()
                    .unwrap(),
                CleanupDisposition::ReservationReleased,
                "DB finalization must remain discoverable before external ACK"
            );
        }

        if stage == "post_runtime" {
            let token = format!("reaper-token-{suffix}");
            let api_state = AppState::new(
                Arc::new(PgExecutionStore::connect(&database_url).await.unwrap()),
                Arc::new(PgEventLog::connect(&database_url).await.unwrap()),
                Arc::new(PgWorkerStore::connect(&database_url).await.unwrap()),
                reservations.clone(),
                Arc::new(
                    PgArtifactStore::connect(&database_url, &root)
                        .await
                        .unwrap(),
                ),
                "api-secret".into(),
                token.clone(),
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server_state = api_state.clone();
            let server = tokio::spawn(async move {
                axum::serve(listener, router(server_state)).await.unwrap();
            });
            let registration = matrix_worker_registration(
                worker_id.clone(),
                format!("capability-{suffix}"),
                &daemon_id,
                &image_id,
            );
            let advertised = WorkerAdvertisement {
                id: registration.id,
                capabilities: registration.capabilities,
                capability_proof: registration.capability_proof,
            };
            let response = reqwest::Client::new()
                .post(format!("http://{address}/api/v1/workers"))
                .bearer_auth(&token)
                .json(&advertised)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_success());
            let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
            sqlx::query(
                "UPDATE workers SET last_heartbeat = now() - interval '91 seconds' WHERE id = $1",
            )
            .bind(worker_id.as_str())
            .execute(&pool)
            .await
            .unwrap();
            assert!(api_state
                .reap_stale(Utc::now())
                .await
                .unwrap()
                .contains(&worker_id));
            let restarted = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "reaper_restart_process_helper", "--nocapture"])
                .env("AUTOSPEC_TASK5_REAPER_RESTART_HELPER", "1")
                .env("AUTOSPEC_DATABASE_URL", &database_url)
                .env("AUTOSPEC_TASK5_ROOT", &root)
                .env("AUTOSPEC_TASK5_REMOTE", &remote_root)
                .env("AUTOSPEC_TASK5_DAEMON", &daemon_id)
                .env("AUTOSPEC_TASK5_IMAGE", &image_id)
                .env("AUTOSPEC_TASK5_EXECUTION", execution_id.as_str())
                .status()
                .unwrap();
            assert!(
                restarted.success(),
                "fresh worker process must reconcile fenced cleanup"
            );
            server.abort();
            assert_matrix_case_clean(&root, &execution_id, &worker_id, &reservations, &cleanup)
                .await;
            delete_matrix_records(&database_url, &execution_id, &worker_id).await;
            if root.exists() {
                fs::remove_dir_all(&root).unwrap();
            }
            continue;
        }

        let mut authority = cleanup.get(&execution_id).await.unwrap();
        let execution = executions.get(&execution_id).await.unwrap();
        let replacement = Worker::new(
            build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
            executions.clone(),
            reservations.clone(),
            cleanup.clone(),
        );
        match authority.disposition().unwrap() {
            CleanupDisposition::RetainRequested => {
                reservations
                    .commit_retained_and_release_capacity(&execution_id, &authority.attempt_id)
                    .await
                    .unwrap();
                authority = cleanup.get(&execution_id).await.unwrap();
            }
            CleanupDisposition::Retained => {}
            disposition => {
                if matches!(disposition, CleanupDisposition::Active(_)) {
                    reservations
                        .fence_lost_attempt(&execution_id, &authority.attempt_id)
                        .await
                        .unwrap();
                }
                replacement
                    .recover_cleanup_authority(&authority, &execution)
                    .await
                    .unwrap();
            }
        }
        if authority.disposition().unwrap() == CleanupDisposition::Retained {
            assert!(ExecutionLayout::new(&root, &execution_id)
                .unwrap()
                .root
                .exists());
            assert!(reservations
                .list_for_worker(&worker_id)
                .await
                .unwrap()
                .is_empty());
            cleanup
                .transition(
                    &execution_id,
                    CleanupDisposition::Retained,
                    CleanupDisposition::CleanupPending,
                    &authority.handles,
                )
                .await
                .unwrap();
            authority = cleanup.get(&execution_id).await.unwrap();
            replacement
                .recover_cleanup_authority(&authority, &execution)
                .await
                .unwrap();
        }
        assert_matrix_case_clean(&root, &execution_id, &worker_id, &reservations, &cleanup).await;
        let events = PgEventLog::connect(&database_url).await.unwrap();
        let persisted_events = events.since(&execution_id, 0).await.unwrap();
        if matches!(
            stage,
            "review_ready_pre_retention" | "post_retention" | "post_finalize_pre_ack"
        ) {
            assert_eq!(
                persisted_events
                    .iter()
                    .filter(|event| matches!(event.kind, ExecutionEventKind::ReviewReady))
                    .count(),
                1,
                "stage {stage} must preserve the single published ReviewReady"
            );
        }
        if stage == "ephemeral_post_runtime" {
            assert_eq!(
                persisted_events
                    .iter()
                    .filter(|event| matches!(
                        event.kind,
                        ExecutionEventKind::ExecutionFailed { .. }
                    ))
                    .count(),
                1,
                "recovery must publish exactly one terminal failure"
            );
        }
        delete_matrix_records(&database_url, &execution_id, &worker_id).await;
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
}

#[tokio::test]
async fn reaper_restart_process_helper() {
    if std::env::var_os("AUTOSPEC_TASK5_REAPER_RESTART_HELPER").is_none() {
        return;
    }
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let root = PathBuf::from(std::env::var_os("AUTOSPEC_TASK5_ROOT").unwrap());
    let remote = PathBuf::from(std::env::var_os("AUTOSPEC_TASK5_REMOTE").unwrap());
    let daemon = std::env::var("AUTOSPEC_TASK5_DAEMON").unwrap();
    let image = std::env::var("AUTOSPEC_TASK5_IMAGE").unwrap();
    let execution_id = ExecutionId::new(std::env::var("AUTOSPEC_TASK5_EXECUTION").unwrap());
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let authority = cleanup.get(&execution_id).await.unwrap();
    let execution = executions.get(&execution_id).await.unwrap();
    let disposition = authority.disposition().unwrap();
    let has_live_reservation = reservations
        .list_for_worker(&authority.worker_id)
        .await
        .unwrap()
        .iter()
        .any(|reservation| {
            reservation.execution.id == execution_id
                && reservation.attempt_id == authority.attempt_id
        });
    if has_live_reservation
        && matches!(
            disposition,
            CleanupDisposition::Active(_) | CleanupDisposition::CleanupPending
        )
    {
        reservations
            .fence_lost_attempt(&execution_id, &authority.attempt_id)
            .await
            .expect("live startup authority is fenced exactly once");
    }
    Worker::new(
        build_system_lifecycle(&root, &remote, &daemon, &image),
        executions,
        reservations,
        cleanup,
    )
    .recover_cleanup_authority(&authority, &execution)
    .await
    .expect("fresh worker cleans and atomically finalizes exact authority");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_cleanup_uncertainty_does_not_destabilize_a_concurrent_peer() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task5 peer isolation: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task5 peer isolation: Docker or alpine:3.20 is unavailable");
        return;
    };
    let suffix = format!(
        "peer-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task5-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote_root = root.join("remotes");
    create_completing_stub_repository(&remote_root);
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let peer_id = ExecutionId::new(format!("peer-{suffix}"));
    let failed_id = ExecutionId::new(format!("failed-{suffix}"));
    let peer_capability = format!("peer-capability-{suffix}");
    let failed_capability = format!("failed-capability-{suffix}");
    let mut registration = matrix_worker_registration(
        worker_id.clone(),
        peer_capability.clone(),
        &daemon_id,
        &image_id,
    );
    registration.capabilities.max_concurrent_executions = 2;
    registration
        .capabilities
        .capabilities
        .push(failed_capability.clone());
    let workers = PgWorkerStore::connect(&database_url).await.unwrap();
    workers.register(&registration).await.unwrap();
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let mut peer_execution = queued_execution(
        peer_id.clone(),
        OwnershipLabels {
            execution_id: peer_id.clone(),
            worker_id: worker_id.clone(),
            repository: "owner/repo".into(),
            issue: Some("peer".into()),
        },
        image_id.clone(),
        peer_capability,
    );
    peer_execution.manifest.repository.branch = Some(format!("peer-{suffix}"));
    executions.insert(&peer_execution).await.unwrap();
    let peer_reservation = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(peer_reservation.execution.id, peer_id);
    let mut failed_execution = queued_execution(
        failed_id.clone(),
        OwnershipLabels {
            execution_id: failed_id.clone(),
            worker_id: worker_id.clone(),
            repository: "owner/repo".into(),
            issue: Some("failed-peer".into()),
        },
        image_id.clone(),
        failed_capability,
    );
    failed_execution.manifest.repository.branch = Some(format!("failed-{suffix}"));
    executions.insert(&failed_execution).await.unwrap();
    let lifecycle = build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id);
    let worker = Arc::new(Worker::new(
        lifecycle,
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let peer_task = worker.clone().spawn(peer_reservation.execution);
    let crashed = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "failure_stage_process_helper", "--nocapture"])
        .env("AUTOSPEC_TASK5_MATRIX_HELPER", "1")
        .env("AUTOSPEC_TASK5_MATRIX_STAGE", "post_runtime")
        .env("AUTOSPEC_DATABASE_URL", &database_url)
        .env("AUTOSPEC_TASK5_ROOT", &root)
        .env("AUTOSPEC_TASK5_REMOTE", &remote_root)
        .env("AUTOSPEC_TASK5_DAEMON", &daemon_id)
        .env("AUTOSPEC_TASK5_IMAGE", &image_id)
        .env("AUTOSPEC_TASK5_WORKER", worker_id.as_str())
        .status()
        .unwrap();
    assert_eq!(crashed.code(), Some(77));
    let peer_result = peer_task.join().await.unwrap();
    assert_eq!(peer_result.state, ExecutionState::ReviewReady);
    assert!(peer_result
        .diff_artifact
        .as_ref()
        .is_some_and(|path| Path::new(path).is_file()));

    let failed_authority = cleanup.get(&failed_id).await.unwrap();
    let failed_execution = executions.get(&failed_id).await.unwrap();
    reservations
        .fence_lost_attempt(&failed_id, &failed_authority.attempt_id)
        .await
        .unwrap();
    worker
        .recover_cleanup_authority(&failed_authority, &failed_execution)
        .await
        .unwrap();
    assert_matrix_case_clean(&root, &failed_id, &worker_id, &reservations, &cleanup).await;

    let peer_layout = ExecutionLayout::new(&root, &peer_id).unwrap();
    assert!(peer_layout.root.exists());
    assert!(peer_result
        .diff_artifact
        .as_ref()
        .is_some_and(|path| Path::new(path).is_file()));
    let peer_authority = cleanup.get(&peer_id).await.unwrap();
    assert_eq!(
        peer_authority.disposition().unwrap(),
        CleanupDisposition::Retained
    );
    cleanup
        .transition(
            &peer_id,
            CleanupDisposition::Retained,
            CleanupDisposition::CleanupPending,
            &peer_authority.handles,
        )
        .await
        .unwrap();
    let requested = cleanup.get(&peer_id).await.unwrap();
    let peer_execution = executions.get(&peer_id).await.unwrap();
    worker
        .recover_cleanup_authority(&requested, &peer_execution)
        .await
        .unwrap();
    assert_matrix_case_clean(&root, &peer_id, &worker_id, &reservations, &cleanup).await;
    delete_matrix_records(&database_url, &failed_id, &worker_id).await;
    delete_matrix_records(&database_url, &peer_id, &worker_id).await;
    if root.exists() {
        fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn failure_stage_process_helper() {
    if std::env::var_os("AUTOSPEC_TASK5_MATRIX_HELPER").is_none() {
        return;
    }
    let stage = std::env::var("AUTOSPEC_TASK5_MATRIX_STAGE").unwrap();
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let root = PathBuf::from(std::env::var_os("AUTOSPEC_TASK5_ROOT").unwrap());
    let remote = PathBuf::from(std::env::var_os("AUTOSPEC_TASK5_REMOTE").unwrap());
    let daemon = std::env::var("AUTOSPEC_TASK5_DAEMON").unwrap();
    let image = std::env::var("AUTOSPEC_TASK5_IMAGE").unwrap();
    let worker_id = WorkerId::new(std::env::var("AUTOSPEC_TASK5_WORKER").unwrap());
    let executions = PgExecutionStore::connect(&database_url).await.unwrap();
    let reservations = PgReservationStore::connect(&database_url).await.unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let reservation = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .expect("matrix helper reserves exact queued execution");
    let mut execution = reservation.execution;
    cleanup
        .begin(&execution.id, &reservation.attempt_id, &worker_id)
        .await
        .unwrap();
    if stage == "post_reservation" {
        std::process::exit(77);
    }
    let lifecycle = build_system_lifecycle(&root, &remote, &daemon, &image);
    let receipt = lifecycle.allocate(&execution).await.unwrap();
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Active(CleanupStage::Reserved),
            CleanupDisposition::Active(CleanupStage::Storage),
            &matrix_handles(Some(&receipt), None, None, None),
        )
        .await
        .unwrap();
    if stage == "post_storage" {
        std::process::exit(77);
    }
    if stage == "mid_git_create" {
        write_interrupted_git_create(&root, &remote, &execution);
        std::process::exit(77);
    }
    let worktree = lifecycle
        .create_worktree(&execution, &receipt)
        .await
        .unwrap();
    execution.worktree_path = Some(worktree.path.clone());
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Active(CleanupStage::Storage),
            CleanupDisposition::Active(CleanupStage::Worktree),
            &matrix_handles(Some(&receipt), Some(&worktree), None, None),
        )
        .await
        .unwrap();
    execution.transition(ExecutionState::Provisioning).unwrap();
    let environment = lifecycle
        .provision(&execution, &receipt, &worktree)
        .await
        .unwrap();
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Active(CleanupStage::Worktree),
            CleanupDisposition::Active(CleanupStage::Runtime),
            &matrix_handles(Some(&receipt), Some(&worktree), Some(&environment), None),
        )
        .await
        .unwrap();
    if matches!(stage.as_str(), "post_runtime" | "ephemeral_post_runtime") {
        std::process::exit(77);
    }
    if matches!(
        stage.as_str(),
        "git_deleted_pre_disposition" | "storage_deleted_pre_disposition"
    ) {
        reservations
            .fence_lost_attempt(&execution.id, &reservation.attempt_id)
            .await
            .unwrap();
        let authority = cleanup.get(&execution.id).await.unwrap();
        let runtime_stopped = lifecycle
            .cleanup_authority_step(&authority, &execution, CleanupDisposition::CleanupPending)
            .await
            .unwrap();
        cleanup
            .transition(
                &execution.id,
                CleanupDisposition::CleanupPending,
                runtime_stopped,
                &authority.handles,
            )
            .await
            .unwrap();
        let runtime_destroyed = lifecycle
            .cleanup_authority_step(&authority, &execution, runtime_stopped)
            .await
            .unwrap();
        cleanup
            .transition(
                &execution.id,
                runtime_stopped,
                runtime_destroyed,
                &authority.handles,
            )
            .await
            .unwrap();
        let git_cleaned = lifecycle
            .cleanup_authority_step(&authority, &execution, runtime_destroyed)
            .await
            .unwrap();
        if stage == "git_deleted_pre_disposition" {
            std::process::exit(77);
        }
        cleanup
            .transition(
                &execution.id,
                runtime_destroyed,
                git_cleaned,
                &authority.handles,
            )
            .await
            .unwrap();
        lifecycle
            .cleanup_authority_step(&authority, &execution, git_cleaned)
            .await
            .unwrap();
        std::process::exit(77);
    }
    executions
        .record_progress(
            &execution,
            &matrix_event(&execution, ExecutionEventKind::EnvironmentReady),
        )
        .await
        .unwrap();
    let packet = execution.manifest.task_packet.clone().unwrap();
    let session = lifecycle
        .start(&execution, &receipt, &environment, &worktree, &packet)
        .await
        .unwrap();
    execution.session_id = Some(session.id.clone());
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Active(CleanupStage::Runtime),
            CleanupDisposition::Active(CleanupStage::PiStarted),
            &matrix_handles(
                Some(&receipt),
                Some(&worktree),
                Some(&environment),
                Some(&session),
            ),
        )
        .await
        .unwrap();
    execution.transition(ExecutionState::Running).unwrap();
    executions
        .record_progress(
            &execution,
            &matrix_event(&execution, ExecutionEventKind::TestsStarted),
        )
        .await
        .unwrap();
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Active(CleanupStage::PiStarted),
            CleanupDisposition::Active(CleanupStage::Running),
            &matrix_handles(
                Some(&receipt),
                Some(&worktree),
                Some(&environment),
                Some(&session),
            ),
        )
        .await
        .unwrap();
    lifecycle.stop(&execution, &session).await.unwrap();
    let capture = lifecycle.capture(&worktree).await.unwrap();
    let artifact = lifecycle
        .persist_evidence(&execution, &capture)
        .await
        .unwrap();
    execution.transition(ExecutionState::ReviewReady).unwrap();
    execution.result = Some(ExecutionResult {
        execution_id: execution.id.clone(),
        state: ExecutionState::ReviewReady,
        failure: None,
        branch: Some(worktree.branch.clone()),
        base_sha: Some(worktree.base_sha.clone()),
        diff_artifact: Some(artifact),
        artifacts: Vec::new(),
        tests: None,
    });
    let handles = matrix_handles(
        Some(&receipt),
        Some(&worktree),
        Some(&environment),
        Some(&session),
    );
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Active(CleanupStage::Running),
            CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
            &handles,
        )
        .await
        .unwrap();
    if stage == "post_pi_pre_event" {
        std::process::exit(77);
    }
    executions
        .record_progress_and_request_retention(
            &execution,
            &matrix_event(&execution, ExecutionEventKind::ReviewReady),
        )
        .await
        .unwrap();
    if stage == "review_ready_pre_retention" {
        std::process::exit(77);
    }
    reservations
        .commit_retained_and_release_capacity(&execution.id, &reservation.attempt_id)
        .await
        .unwrap();
    if stage == "post_retention" {
        std::process::exit(77);
    }
    let authority = cleanup.get(&execution.id).await.unwrap();
    cleanup
        .transition(
            &execution.id,
            CleanupDisposition::Retained,
            CleanupDisposition::CleanupPending,
            &authority.handles,
        )
        .await
        .unwrap();
    let mut disposition = CleanupDisposition::CleanupPending;
    while matches!(
        disposition,
        CleanupDisposition::CleanupPending
            | CleanupDisposition::RuntimeStopped
            | CleanupDisposition::RuntimeDestroyed
            | CleanupDisposition::GitRecoveredCleaned
    ) {
        let next = lifecycle
            .cleanup_authority_step(&authority, &execution, disposition)
            .await
            .unwrap();
        cleanup
            .transition(&execution.id, disposition, next, &authority.handles)
            .await
            .unwrap();
        disposition = next;
    }
    reservations
        .finalize_cleanup(&execution.id, &reservation.attempt_id)
        .await
        .unwrap();
    std::process::exit(77);
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
    let task = tokio::spawn(async move { running_worker.run(&assigned).await });
    for _ in 0..300 {
        let execution = executions.get(&execution_id).await.unwrap();
        if execution.state == ExecutionState::Running && execution.session_id.is_some() {
            std::process::exit(77);
        }
        if task.is_finished() {
            panic!("worker ended before durable Running: {:?}", task.await);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("execution did not reach durable Running session");
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

fn matrix_worker_registration(
    id: WorkerId,
    capability: String,
    daemon_id: &str,
    image_id: &str,
) -> WorkerRegistration {
    let trusted = TrustedVerifierImage::new(image_id, "/bin/stat").unwrap();
    WorkerRegistration {
        id,
        capabilities: WorkerCapabilities {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            cpu: 2,
            memory_mib: 2048,
            disk_gib: 1,
            runtimes: vec![RuntimeKind::Docker],
            capabilities: vec!["docker".into(), capability],
            max_concurrent_executions: 1,
        },
        state: WorkerState::Ready,
        running_executions: 0,
        last_heartbeat: Utc::now(),
        capability_proof: Some(WorkerCapabilityProof {
            storage_backend: "fixed-test-filesystem".into(),
            storage_pool_identity: "fixed-test-pool".into(),
            docker_daemon_id: daemon_id.into(),
            docker_verifier: image_id.into(),
            docker_method_version: trusted.proof_method(),
        }),
    }
}

fn matrix_handles(
    receipt: Option<&execution_storage::AllocationReceipt>,
    worktree: Option<&Worktree>,
    runtime: Option<&EnvironmentHandle>,
    session: Option<&SessionRef>,
) -> serde_json::Value {
    serde_json::json!({
        "receipt": receipt,
        "worktree": worktree.map(|worktree| serde_json::json!({
            "execution_id": worktree.execution_id,
            "path": worktree.path,
            "branch": worktree.branch,
            "base_sha": worktree.base_sha,
            "repository": worktree.repository,
        })),
        "runtime": runtime.map(|runtime| serde_json::json!({
            "execution_id": runtime.execution_id,
            "network": runtime.network,
            "agent_container": runtime.agent_container,
            "container_id": runtime.verified_agent_container.container_id,
            "daemon_id": runtime.verified_agent_container.daemon_id,
            "labels": runtime.verified_agent_container.labels,
            "mounts": runtime.verified_agent_container.mounts.iter().map(|mount| serde_json::json!({
                "source": mount.source,
                "target": mount.target,
                "writable": mount.writable,
            })).collect::<Vec<_>>(),
            "service_containers": runtime.service_containers,
            "volumes": runtime.volumes,
            "credentials_path": runtime.credentials_path,
        })),
        "session": session.map(|session| serde_json::json!({
            "id": session.id,
            "path": session.path,
            "execution_id": session.execution_id,
            "worktree_path": session.worktree_path,
        })),
    })
}

fn matrix_event(execution: &Execution, kind: ExecutionEventKind) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: execution.id.clone(),
        attempt_id: execution.attempt_id.clone(),
        sequence: 0,
        at: Utc::now(),
        state: execution.state,
        kind,
    }
}

fn write_interrupted_git_create(root: &Path, remote: &Path, execution: &Execution) {
    let layout = ExecutionLayout::new(root, &execution.id).unwrap();
    run(Command::new("git").args([
        "clone",
        &format!("file://{}/owner/repo.git", remote.display()),
        layout.repository.to_str().unwrap(),
    ]));
    let metadata = root.join("worktrees");
    fs::create_dir_all(&metadata).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&metadata, fs::Permissions::from_mode(0o700)).unwrap();
    let base_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                layout.repository.to_str().unwrap(),
                "rev-parse",
                "HEAD",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let intent = serde_json::json!({
        "labels": execution.labels.to_map(),
        "repository": execution.manifest.repository.repo,
        "base_ref": execution.manifest.repository.base_ref,
        "base_sha": base_sha,
        "branch": execution.manifest.repository.branch.as_deref().unwrap(),
        "repository_path": layout.repository,
        "phase": "cloning",
    });
    let intent_path = metadata.join(format!(".create-{}.json", execution.id));
    fs::write(&intent_path, serde_json::to_vec_pretty(&intent).unwrap()).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&intent_path, fs::Permissions::from_mode(0o600)).unwrap();
}

async fn assert_matrix_case_clean(
    root: &Path,
    execution_id: &ExecutionId,
    worker_id: &WorkerId,
    reservations: &PgReservationStore,
    cleanup: &PgCleanupAuthorityStore,
) {
    assert!(!ExecutionLayout::new(root, execution_id)
        .unwrap()
        .root
        .exists());
    assert!(!reservations
        .list_for_worker(worker_id)
        .await
        .unwrap()
        .iter()
        .any(|reservation| &reservation.execution.id == execution_id));
    assert!(!cleanup
        .list_for_worker(worker_id)
        .await
        .unwrap()
        .iter()
        .any(|authority| &authority.execution_id == execution_id));
    let docker = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=autospec.execution_id={execution_id}"),
        ])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&docker.stdout).trim().is_empty());
}

async fn delete_matrix_records(
    database_url: &str,
    execution_id: &ExecutionId,
    worker_id: &WorkerId,
) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(database_url)
        .await
        .unwrap();
    sqlx::query("DELETE FROM cleanup_authorities WHERE execution_id = $1")
        .bind(execution_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM executions WHERE id = $1")
        .bind(execution_id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "DELETE FROM workers w
         WHERE id = $1
           AND NOT EXISTS (SELECT 1 FROM executions WHERE worker_id = w.id)
           AND NOT EXISTS (SELECT 1 FROM execution_attempts WHERE worker_id = w.id)",
    )
    .bind(worker_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
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

async fn acquire_real_test_lock(database_url: &str) -> PgConnection {
    let mut connection = PgConnection::connect(database_url).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(870_051_003)")
        .execute(&mut connection)
        .await
        .unwrap();
    connection
}

fn create_stub_repository(remote_root: &Path) {
    create_stub_repository_with_behavior(remote_root, true);
}

fn create_completing_stub_repository(remote_root: &Path) {
    create_stub_repository_with_behavior(remote_root, false);
}

fn create_stub_repository_with_behavior(remote_root: &Path, block_first_run: bool) {
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
    let first_run_gate = if block_first_run {
        "if test \"$count\" -eq 1; then while :; do sleep 1; done; fi\n"
    } else {
        ""
    };
    fs::write(
        &pi,
        format!(
            "#!/bin/sh\ncount=1\nif test -f /session/invocations; then count=$(( $(cat /session/invocations) + 1 )); fi\nprintf '%s\\n' \"$count\" > /session/invocations\nprintf '%s\\n' '{{\"type\":\"session\",\"id\":\"e2e\"}}' '{{\"type\":\"agent_start\"}}' '{{\"type\":\"turn_start\"}}'\n{first_run_gate}printf 'worker-e2e-%s\\n' \"$count\" > /workspace/result.txt\nprintf '%s\\n' '{{\"type\":\"message_end\",\"message\":{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"done\"}}],\"stopReason\":\"stop\"}}}}' '{{\"type\":\"agent_settled\"}}'\n"
        ),
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
