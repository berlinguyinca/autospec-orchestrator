use chrono::Utc;
use execution_storage::{
    BackendCapability, BackendIdentity, BackendState, DockerBindCapability, DockerBindProof,
    DockerBindVerifier, ExecutionLayout, ExecutionLifecycleHoldStore, ExecutionStorage,
    StorageBackend, StorageError,
};
use git_worktree::{GitWorktreeManager, Worktree};
use harness_traits::SessionRef;
use orchestrator_api::{router, AppState};
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, AttemptId, Execution, ExecutionControlAction,
    ExecutionEvent, ExecutionId, ExecutionManifest, ExecutionResult, ExecutionState, HarnessKind,
    ModelPolicy, OwnershipLabels, PersistenceMode, RepositoryReference, Role, RuntimeKind,
    RuntimeRequirement, ServiceRequirement, TaskPacket, WorkerAdvertisement, WorkerCapabilities,
    WorkerCapabilityProof, WorkerId, WorkerRegistration, WorkerState,
};
use orchestrator_persistence::{
    ArtifactStore, CleanupAuthority, CleanupAuthorityStore, CleanupDisposition, CleanupStage,
    EventLog, ExecutionStore, PgArtifactStore, PgCleanupAuthorityStore, PgEventLog,
    PgExecutionStore, PgReservationStore, PgWorkerStore, ReservationStore, WorkerStore,
};
use orchestrator_worker::{
    ContentAddressedEvidenceStore, ControlCheckpoint, ControlCheckpointObserver,
    ExecutionLifecycle, FilesystemEvidenceStore, LifecycleError, RuntimeFactory,
    SystemExecutionLifecycle, SystemRecoveryConfig, VerifiedDockerRuntimeFactory,
    VerifiedPiHarnessFactory, Worker,
};
use runtime_docker::{DockerRuntime, LocalCredentialBroker, TrustedVerifierImage};
use runtime_traits::{CredentialBroker, EnvironmentHandle, Runtime, RuntimeError};
use sqlx::{Connection, PgConnection};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::net::TcpListener;

const REAL_RUNTIME_RUNNING_WAIT_STEPS: usize = 1_200;

struct ExitAtNthControlCheckpoint {
    checkpoint: ControlCheckpoint,
    skip: usize,
    seen: AtomicUsize,
}

impl ControlCheckpointObserver for ExitAtNthControlCheckpoint {
    fn reached(&self, checkpoint: ControlCheckpoint) {
        if checkpoint == self.checkpoint && self.seen.fetch_add(1, Ordering::SeqCst) == self.skip {
            std::process::exit(77);
        }
    }
}

struct AssertRecoveryBoundary {
    root: PathBuf,
    execution_id: ExecutionId,
    expected_session: String,
    live: bool,
    seen: AtomicBool,
}

struct PartialProvisionRuntime {
    fail_destroy_once: AtomicBool,
    expected_authority: (String, ExecutionId),
}

#[async_trait::async_trait]
impl Runtime for PartialProvisionRuntime {
    fn name(&self) -> &'static str {
        "partial-provision-test"
    }

    async fn available(&self) -> bool {
        true
    }

    async fn provision(
        &self,
        labels: &OwnershipLabels,
        _: &RuntimeRequirement,
        _: &[ServiceRequirement],
    ) -> Result<EnvironmentHandle, RuntimeError> {
        let cleanup = PgCleanupAuthorityStore::connect(&self.expected_authority.0)
            .await
            .map_err(|error| RuntimeError::Provisioning(error.to_string()))?;
        let authority = cleanup
            .get(&self.expected_authority.1)
            .await
            .map_err(|error| RuntimeError::Provisioning(error.to_string()))?;
        assert_eq!(
            authority.disposition().unwrap(),
            CleanupDisposition::Active(CleanupStage::Runtime),
            "label-scoped cleanup authority must precede the first Docker side effect"
        );
        let output = Command::new("docker")
            .args([
                "network",
                "create",
                "--label",
                "autospec.managed=true",
                "--label",
                &format!("autospec.execution_id={}", labels.execution_id),
                &DockerRuntime::network_name(&labels.execution_id),
            ])
            .output()
            .map_err(|error| RuntimeError::Provisioning(error.to_string()))?;
        assert!(output.status.success());
        Err(RuntimeError::Provisioning(
            "injected failure after partial Docker creation".into(),
        ))
    }

    async fn destroy(&self, labels: &OwnershipLabels) -> Result<(), RuntimeError> {
        if self.fail_destroy_once.swap(false, Ordering::SeqCst) {
            return Err(RuntimeError::Cleanup(
                "injected first rollback failure".into(),
            ));
        }
        let output = Command::new("docker")
            .args([
                "network",
                "rm",
                &DockerRuntime::network_name(&labels.execution_id),
            ])
            .output()
            .map_err(|error| RuntimeError::Cleanup(error.to_string()))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(RuntimeError::Cleanup(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ))
        }
    }

    async fn reconcile(&self, _: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError> {
        Ok(Vec::new())
    }
}

struct PartialProvisionFactory(Arc<PartialProvisionRuntime>);

#[async_trait::async_trait]
impl RuntimeFactory for PartialProvisionFactory {
    async fn build(
        &self,
        _: &Execution,
        _: &execution_storage::AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        Ok(self.0.clone())
    }

    async fn build_for_cleanup(
        &self,
        _: &Execution,
        _: &execution_storage::AllocationReceipt,
    ) -> Result<Arc<dyn Runtime>, LifecycleError> {
        Ok(self.0.clone())
    }

    async fn cpu_percent(&self, _: &Execution) -> Result<f64, LifecycleError> {
        Ok(0.0)
    }

    async fn revoke_credentials(&self, _: &ExecutionId) -> Result<(), LifecycleError> {
        Ok(())
    }
}

impl ControlCheckpointObserver for AssertRecoveryBoundary {
    fn reached(&self, checkpoint: ControlCheckpoint) {
        let expected_checkpoint = if self.live {
            ControlCheckpoint::RunningRestored
        } else {
            ControlCheckpoint::SideEffectPersisted
        };
        if checkpoint != expected_checkpoint || self.seen.swap(true, Ordering::SeqCst) {
            return;
        }
        let holds = ExecutionLifecycleHoldStore::new(&self.root)
            .unwrap()
            .list(&self.execution_id)
            .unwrap();
        if self.live {
            assert_eq!(holds.len(), 1, "exactly one restored session must be live");
            assert_eq!(holds[0].session_id, self.expected_session);
        } else {
            assert!(holds.is_empty(), "paused control must remain quiescent");
        }
    }
}

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task9_manifest_runs_through_real_worker_and_exact_cleanup_with_durable_evidence() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP Task9 execution-plane audit: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("SKIP Task9 execution-plane audit: Docker or alpine:3.20 is unavailable");
        return;
    };
    let redis = Command::new("docker")
        .args(["image", "inspect", "redis:7-alpine"])
        .output()
        .unwrap();
    if !redis.status.success() {
        eprintln!("SKIP Task9 execution-plane audit: redis:7-alpine is unavailable");
        return;
    }

    let suffix = format!(
        "task9-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let root = std::env::temp_dir().join(format!("autospec-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote_root = root.join("remotes");
    create_task9_fixture_repository(&remote_root);
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let capability = format!("capability-{suffix}");

    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
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
    let events = Arc::new(PgEventLog::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let artifacts = Arc::new(
        PgArtifactStore::connect(&database_url, &root)
            .await
            .unwrap(),
    );
    let api_token = format!("api-{suffix}");
    let state = AppState::new(
        executions.clone(),
        events.clone(),
        workers,
        reservations.clone(),
        artifacts.clone(),
        api_token.clone(),
        "worker-secret".into(),
    )
    .with_cleanup_authorities(cleanup.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });

    let goal = format!("task9-goal-{suffix}");
    let manifest: ExecutionManifest = serde_json::from_value(serde_json::json!({
        "apiVersion": orchestrator_core::MANIFEST_API_VERSION,
        "role": "implementation",
        "task": {"project_id": "autospec", "issue_id": suffix},
        "repository": {"repo": "owner/repo", "baseRef": "main", "branch": "task9-audit"},
        "agent": {"harness": "pi", "modelPolicy": {"provider": "inferweave", "fallbackClass": "coding-high"}},
        "runtime": {"type": "docker", "image": image_id, "cpu": 1, "memoryMib": 256, "diskGib": 1, "capabilities": [capability]},
        "services": [{"name": "cache", "image": "redis:7-alpine"}],
        "persistence": "resumable",
        "task_packet": {
            "goal": goal,
            "acceptance_criteria": ["result.txt exists"],
            "non_goals": [],
            "relevant_context": [],
            "required_tests": []
        }
    }))
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("http://{address}/api/v1/executions"))
        .bearer_auth(&api_token)
        .header("Idempotency-Key", format!("create-{suffix}"))
        .json(&manifest)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let created: Execution = response.json().await.unwrap();
    assert_eq!(
        serde_json::to_value(&created.manifest).unwrap(),
        serde_json::to_value(&manifest).unwrap()
    );
    assert_eq!(
        executions.get(&created.id).await.unwrap().state,
        ExecutionState::Queued
    );
    assert_eq!(events.since(&created.id, 0).await.unwrap().len(), 1);

    let assigned = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .expect("manifest must match the real worker")
        .execution;
    let lifecycle = build_task9_lifecycle(
        &root,
        &remote_root,
        &daemon_id,
        &image_id,
        artifacts.clone(),
    );
    let worker = Arc::new(Worker::new(
        lifecycle,
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let task = worker.clone().spawn(assigned);
    let running = wait_for_running_session(&executions, &created.id).await;
    let layout = ExecutionLayout::new(&root, &created.id).unwrap();
    assert!(layout.repository.join(".autospec-owner.json").is_file());
    assert!(layout.session.join("owner.json").is_file());
    assert!(docker_resource_exists(
        "container",
        &DockerRuntime::agent_container_name(&created.id)
    ));
    assert!(docker_resource_exists(
        "container",
        &DockerRuntime::service_container_name(&created.id, "cache")
    ));
    assert!(docker_resource_exists(
        "network",
        &DockerRuntime::network_name(&created.id)
    ));
    assert_eq!(running.manifest.task_packet.as_ref().unwrap().goal, goal);
    let owned_resources = capture_owned_docker_resources(&running.labels);
    assert_eq!(
        owned_resources
            .iter()
            .map(|resource| (resource.kind.as_str(), resource.name.as_str()))
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            (
                "container",
                DockerRuntime::agent_container_name(&created.id).as_str(),
            ),
            (
                "container",
                DockerRuntime::service_container_name(&created.id, "cache").as_str(),
            ),
            ("network", DockerRuntime::network_name(&created.id).as_str()),
        ])
    );
    assert!(owned_resources
        .iter()
        .all(|resource| resource.labels == running.labels.to_map()));

    let result = task.join().await.unwrap();
    assert_eq!(result.state, ExecutionState::ReviewReady);
    let artifact_id = result.diff_artifact.expect("content-addressed diff");
    let listed = artifacts.list(&created.id).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].sha256, artifact_id);
    assert!(root
        .join(format!("artifacts/{}/{}", &artifact_id[..2], artifact_id))
        .is_file());
    let artifact_path = root.join(format!("artifacts/{}/{}", &artifact_id[..2], artifact_id));
    let artifact_bytes = fs::read(&artifact_path).unwrap();
    let artifact_text = String::from_utf8(artifact_bytes).unwrap();
    let goal_derived_result = format!("task9-result:{goal}");
    assert_eq!(
        artifact_text.matches(&goal_derived_result).count(),
        1,
        "the single Pi task-packet argument must drive the durable artifact"
    );
    assert_eq!(
        fs::read_to_string(layout.conversation.join("invocations")).unwrap(),
        "1\n"
    );
    let durable_events = events.since(&created.id, 0).await.unwrap();
    assert_eq!(durable_events.len(), 4);
    assert!(matches!(
        durable_events[0].kind,
        ExecutionEventKind::ExecutionCreated
    ));
    assert!(matches!(
        durable_events[1].kind,
        ExecutionEventKind::EnvironmentReady
    ));
    assert!(matches!(
        durable_events[2].kind,
        ExecutionEventKind::AgentStarted { .. }
    ));
    assert!(matches!(
        durable_events[3].kind,
        ExecutionEventKind::ReviewReady
    ));
    assert!(durable_events
        .windows(2)
        .all(|pair| pair[1].sequence == pair[0].sequence + 1));

    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    for table in [
        "artifact_blobs",
        "execution_control_requests",
        "execution_cancellation_requests",
        "reservations",
        "workers",
        "execution_attempts",
        "execution_events",
        "cleanup_authorities",
        "artifacts",
    ] {
        let query = format!(
            "SELECT EXISTS(SELECT 1 FROM {table} record WHERE to_jsonb(record)::text LIKE '%' || $1 || '%')"
        );
        let duplicated: bool = sqlx::query_scalar(&query)
            .bind(&goal)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!duplicated, "task packet duplicated into {table}");
    }
    let compatibility_copy: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_requests \
         WHERE execution_id = $1 AND manifest::text LIKE '%' || $2 || '%')",
    )
    .bind(created.id.as_str())
    .bind(&goal)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        compatibility_copy,
        "mixed-version rollout requires the temporary prior-controller replay copy"
    );

    executions.request_cancellation(&created.id).await.unwrap();
    worker.reconcile_daemon_tick(&worker_id, &[]).await.unwrap();
    assert_eq!(
        executions.get(&created.id).await.unwrap().state,
        ExecutionState::Cancelled
    );
    assert!(!layout.root.exists());
    assert_owned_docker_resources_absent(&owned_resources, &running.labels);
    assert_matrix_case_clean(&root, &created.id, &worker_id, &reservations, &cleanup).await;
    assert_eq!(artifacts.list(&created.id).await.unwrap().len(), 1);
    let reopened_events = PgEventLog::connect(&database_url)
        .await
        .unwrap()
        .since(&created.id, 0)
        .await
        .unwrap();
    assert_eq!(reopened_events.len(), durable_events.len() + 1);
    assert!(reopened_events
        .windows(2)
        .all(|pair| pair[1].sequence == pair[0].sequence + 1));
    assert_eq!(
        reopened_events
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        1
    );
    let reopened_artifact = fs::read(&artifact_path).unwrap();
    assert_eq!(
        sha256_file(&artifact_path),
        artifact_id,
        "durable artifact bytes must still verify after resource cleanup"
    );
    assert_eq!(String::from_utf8(reopened_artifact).unwrap(), artifact_text);

    server.abort();
    delete_matrix_records(&database_url, &created.id, &worker_id).await;
    if root.exists() {
        fs::remove_dir_all(&root).unwrap();
    }
}

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
    command: String,
    method: String,
}

impl DockerBindVerifier for TestDockerBindVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        &self.daemon_id
    }

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
                &self.command,
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
async fn paused_worker_is_adopted_across_a_separate_process_with_zero_live_pi_holds() {
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
            health_errors: Vec::new(),
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
        .env("AUTOSPEC_TASK7_PAUSE_BEFORE_CRASH", "1")
        .status()
        .unwrap();
    assert_eq!(crashed_child.code(), Some(77));
    let crashed = wait_for_exact_state(
        executions.as_ref(),
        &execution_id,
        ExecutionState::PausedForHuman,
    )
    .await;
    let original_attempt = crashed.attempt_id.clone();
    let original_session = crashed.session_id.clone();
    assert_eq!(
        execution_storage::ExecutionLifecycleHoldStore::new(&root)
            .unwrap()
            .list(&execution_id)
            .unwrap()
            .len(),
        0
    );
    let adopted = executions.get(&execution_id).await.unwrap();
    let replacement = Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let task = replacement.clone().spawn_adopted(adopted);
    executions
        .request_control(
            &execution_id,
            ExecutionControlAction::Resume,
            "resume-after-paused-restart",
        )
        .await
        .unwrap();
    replacement
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    let result = task.join().await.unwrap();

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

    executions
        .request_cancellation(&execution_id)
        .await
        .unwrap();
    replacement
        .reconcile_daemon_tick(&worker_id, &[])
        .await
        .unwrap();
    assert_eq!(
        executions.get(&execution_id).await.unwrap().state,
        ExecutionState::Cancelled
    );
    assert!(!executions
        .cancellation_requested(&execution_id)
        .await
        .unwrap());
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
    assert_eq!(
        event_log
            .since(&execution_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        1,
        "retained cancellation must publish one terminal event after cleanup"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_side_effect_crash_cuts_reconcile_in_a_fresh_process_exactly_once() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task7 control crash cuts: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task7 control crash cuts: Docker is unavailable");
        return;
    };
    for stage in [
        "pause_side_effect",
        "resume_side_effect",
        "paused_fork_side_effect",
        "running_fork_side_effect",
    ] {
        let stage_tag = match stage {
            "pause_side_effect" => "pse",
            "resume_side_effect" => "rse",
            "paused_fork_side_effect" => "pfse",
            "running_fork_side_effect" => "rfse",
            _ => unreachable!(),
        };
        let suffix = format!(
            "cc-{stage_tag}-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        );
        let root = std::env::temp_dir().join(format!("autospec-task7-{suffix}"));
        initialize_state_root(&root);
        let root = root.canonicalize().unwrap();
        let remote = root.join("remotes");
        create_stub_repository(&remote);
        let worker_id = WorkerId::new(format!("worker-{suffix}"));
        let execution_id = ExecutionId::new(format!("execution-{suffix}"));
        let capability = format!("capability-{suffix}");
        PgWorkerStore::connect(&database_url)
            .await
            .unwrap()
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
        let mut queued = queued_execution(
            execution_id.clone(),
            OwnershipLabels {
                execution_id: execution_id.clone(),
                worker_id: worker_id.clone(),
                repository: "owner/repo".into(),
                issue: Some(stage.into()),
            },
            image_id.clone(),
            capability,
        );
        queued.role = Role::Interactive;
        queued.manifest.role = Role::Interactive;
        queued.manifest.persistence = PersistenceMode::Resumable;
        queued.manifest.repository.branch = Some(format!("branch-{suffix}"));
        executions.insert(&queued).await.unwrap();
        let crashed = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_process_helper", "--nocapture"])
            .env("AUTOSPEC_TASK5_CRASH_HELPER", "1")
            .env("AUTOSPEC_TASK7_CONTROL_CRASH", stage)
            .env("AUTOSPEC_DATABASE_URL", &database_url)
            .env("AUTOSPEC_TASK5_ROOT", &root)
            .env("AUTOSPEC_TASK5_REMOTE", &remote)
            .env("AUTOSPEC_TASK5_DAEMON", &daemon_id)
            .env("AUTOSPEC_TASK5_IMAGE", &image_id)
            .env("AUTOSPEC_TASK5_WORKER", worker_id.as_str())
            .status()
            .unwrap();
        assert_eq!(crashed.code(), Some(77), "stage {stage}");
        let pending = executions.list_pending_controls(&worker_id).await.unwrap();
        assert_eq!(pending.len(), 1, "stage {stage}");
        assert_eq!(
            pending[0].phase,
            orchestrator_persistence::ExecutionControlPhase::SideEffectApplied,
            "crash phase: {stage}"
        );
        let pre_recovery_events = PgEventLog::connect(&database_url)
            .await
            .unwrap()
            .since(&execution_id, 0)
            .await
            .unwrap();
        assert!(pre_recovery_events
            .iter()
            .all(|event| !matches!(event.kind, ExecutionEventKind::ReviewReady)));
        let pre_recovery_cursor = pre_recovery_events
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_process_helper", "--nocapture"])
            .env("AUTOSPEC_TASK5_CRASH_HELPER", "1")
            .env("AUTOSPEC_TASK7_CONTROL_RECOVERY", stage)
            .env("AUTOSPEC_TASK7_EXECUTION", execution_id.as_str())
            .env(
                "AUTOSPEC_TASK7_EXPECTED_SESSION",
                pending[0]
                    .side_effect_session_id
                    .as_ref()
                    .expect("persisted side effect has a session")
                    .as_str(),
            )
            .env("AUTOSPEC_DATABASE_URL", &database_url)
            .env("AUTOSPEC_TASK5_ROOT", &root)
            .env("AUTOSPEC_TASK5_REMOTE", &remote)
            .env("AUTOSPEC_TASK5_DAEMON", &daemon_id)
            .env("AUTOSPEC_TASK5_IMAGE", &image_id)
            .env("AUTOSPEC_TASK5_WORKER", worker_id.as_str())
            .status()
            .unwrap();
        assert!(
            recovered.success(),
            "fresh recovery process failed: {stage}"
        );
        assert_eq!(
            executions.get(&execution_id).await.unwrap().state,
            ExecutionState::ReviewReady,
            "stage {stage}"
        );
        let events = PgEventLog::connect(&database_url)
            .await
            .unwrap()
            .since(&execution_id, 0)
            .await
            .unwrap();
        let matching = events
            .iter()
            .filter(|event| {
                matches!(
                    (&event.kind, stage),
                    (ExecutionEventKind::ExecutionPaused, "pause_side_effect")
                        | (ExecutionEventKind::ExecutionResumed, "resume_side_effect")
                        | (
                            ExecutionEventKind::ConversationForked { .. },
                            "paused_fork_side_effect" | "running_fork_side_effect"
                        )
                )
            })
            .count();
        assert_eq!(matching, 1, "control event must be exactly once: {stage}");
        let review_ready = events
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ReviewReady))
            .collect::<Vec<_>>();
        assert_eq!(
            review_ready.len(),
            1,
            "recovery publishes one terminal event"
        );
        assert!(
            review_ready[0].sequence > pre_recovery_cursor,
            "ReviewReady must be produced by the replacement process"
        );

        executions
            .request_cancellation(&execution_id)
            .await
            .unwrap();
        let replacement = Worker::new(
            build_system_lifecycle(&root, &remote, &daemon_id, &image_id),
            executions.clone(),
            reservations.clone(),
            cleanup.clone(),
        );
        replacement
            .reconcile_daemon_tick(&worker_id, &[])
            .await
            .unwrap();
        assert_matrix_case_clean(&root, &execution_id, &worker_id, &reservations, &cleanup).await;
        delete_matrix_records(&database_url, &execution_id, &worker_id).await;
        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }
    }
}

#[tokio::test]
async fn expired_bound_credential_does_not_block_exact_runtime_cleanup() {
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping expired credential cleanup: Docker is unavailable");
        return;
    };
    let suffix = format!(
        "expired-cleanup-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task7-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote = root.join("remotes");
    create_stub_repository(&remote);
    let execution_id = ExecutionId::new(format!("execution-{suffix}"));
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let labels = OwnershipLabels {
        execution_id: execution_id.clone(),
        worker_id: worker_id.clone(),
        repository: "owner/repo".into(),
        issue: Some("expired-cleanup".into()),
    };
    let execution = queued_execution(
        execution_id.clone(),
        labels.clone(),
        image_id.clone(),
        format!("capability-{suffix}"),
    );
    let lifecycle = build_system_lifecycle(&root, &remote, &daemon_id, &image_id);
    let receipt = lifecycle.allocate(&execution).await.unwrap();
    let credential = receipt.mount_path.join("credentials/inferweave.credential");
    fs::write(&credential, "expired\n2020-01-01T00:00:00Z\n").unwrap();
    #[cfg(unix)]
    fs::set_permissions(&credential, fs::Permissions::from_mode(0o600)).unwrap();
    let network = DockerRuntime::network_name(&execution_id);
    let network_status = Command::new("docker")
        .args([
            "network",
            "create",
            "--label",
            "autospec.managed=true",
            "--label",
            &format!("autospec.execution_id={execution_id}"),
            "--label",
            &format!("autospec.worker_id={worker_id}"),
            "--label",
            "autospec.repository=owner/repo",
            "--label",
            "autospec.issue=expired-cleanup",
            &network,
        ])
        .status()
        .unwrap();
    assert!(network_status.success());
    let authority = CleanupAuthority {
        execution_id: execution_id.clone(),
        attempt_id: AttemptId::new(format!("attempt-{suffix}")),
        worker_id,
        phase: CleanupDisposition::RuntimeStopped.to_string(),
        handles: matrix_handles(Some(&receipt), None, None, None),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let result = lifecycle
        .cleanup_authority_step(&authority, &execution, CleanupDisposition::RuntimeStopped)
        .await;
    if result.is_err() {
        let runtime = DockerRuntime::connect_with_state_root(None, &root).unwrap();
        let _ = runtime.destroy(&labels).await;
        let broker = LocalCredentialBroker::new(&root, chrono::Duration::minutes(15)).unwrap();
        let _ = broker.revoke(&execution_id).await;
    }
    assert!(!credential.exists());
    assert!(!docker_resource_exists("network", &network));

    let storage_released = lifecycle
        .cleanup_authority_step(
            &authority,
            &execution,
            CleanupDisposition::GitRecoveredCleaned,
        )
        .await
        .unwrap();
    assert_eq!(storage_released, CleanupDisposition::StorageReleased);
    lifecycle
        .ack_cleanup_authority_step(&authority, storage_released)
        .await
        .unwrap();
    if root.exists() {
        fs::remove_dir_all(root).unwrap();
    }
    assert_eq!(result.unwrap(), CleanupDisposition::RuntimeDestroyed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_docker_provision_and_rollback_failure_recovers_by_exact_selector_after_restart() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real partial provision rollback: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real partial provision rollback: Docker is unavailable");
        return;
    };
    let suffix = format!(
        "partial-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task7-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote = root.join("remotes");
    create_stub_repository(&remote);
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let execution_id = ExecutionId::new(format!("execution-{suffix}"));
    let restarted_image_id = format!("sha256:{}", "b".repeat(64));
    let restarted_verifier_command = "/usr/bin/stat";
    let peer_network = format!("autospec-peer-{suffix}");
    let peer = Command::new("docker")
        .args([
            "network",
            "create",
            "--label",
            "autospec.managed=true",
            "--label",
            "autospec.execution_id=peer-task7",
            &peer_network,
        ])
        .output()
        .unwrap();
    assert!(peer.status.success());
    let capability = format!("cap-{suffix}");
    PgWorkerStore::connect(&database_url)
        .await
        .unwrap()
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
    let mut queued = queued_execution(
        execution_id.clone(),
        OwnershipLabels {
            execution_id: execution_id.clone(),
            worker_id: worker_id.clone(),
            repository: "owner/repo".into(),
            issue: Some("partial-provision".into()),
        },
        image_id.clone(),
        capability,
    );
    queued.manifest.repository.branch = Some(format!("branch-{suffix}"));
    executions.insert(&queued).await.unwrap();
    let provision = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "partial_provision_process_helper", "--nocapture"])
        .env("AUTOSPEC_TASK7_PARTIAL_HELPER", "provision")
        .env("AUTOSPEC_DATABASE_URL", &database_url)
        .env("AUTOSPEC_TASK7_PARTIAL_ROOT", &root)
        .env("AUTOSPEC_TASK7_PARTIAL_REMOTE", &remote)
        .env("AUTOSPEC_TASK7_PARTIAL_DAEMON", &daemon_id)
        .env("AUTOSPEC_TASK7_PARTIAL_IMAGE", &image_id)
        .env("AUTOSPEC_TASK7_PARTIAL_WORKER", worker_id.as_str())
        .env("AUTOSPEC_TASK7_PARTIAL_EXECUTION", execution_id.as_str())
        .status()
        .unwrap();
    assert!(
        provision.success(),
        "original provision process failed its assertions"
    );
    assert!(docker_resource_exists(
        "network",
        &DockerRuntime::network_name(&execution_id)
    ));
    assert!(docker_resource_exists("network", &peer_network));
    assert_eq!(
        cleanup
            .get(&execution_id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::RuntimeStopped
    );
    let authority = cleanup.get(&execution_id).await.unwrap();
    let receipt: execution_storage::AllocationReceipt =
        serde_json::from_value(authority.handles["receipt"].clone()).unwrap();
    let credential = receipt.mount_path.join("credentials/inferweave.credential");
    fs::write(&credential, "expired\n2020-01-01T00:00:00Z\n").unwrap();
    #[cfg(unix)]
    fs::set_permissions(&credential, fs::Permissions::from_mode(0o600)).unwrap();
    let restarted_verifier =
        TrustedVerifierImage::new(&restarted_image_id, restarted_verifier_command).unwrap();
    let verifier: Arc<dyn execution_storage::ReadyAllocationVerifier> = Arc::new(
        ExecutionStorage::new(
            &root,
            Box::new(FixedFilesystemBackend),
            Box::new(TestDockerBindVerifier {
                daemon_id: daemon_id.clone(),
                image_id: image_id.clone(),
                command: "/bin/stat".into(),
                method: TrustedVerifierImage::new(&image_id, "/bin/stat")
                    .unwrap()
                    .proof_method(),
            }),
        )
        .unwrap(),
    );
    assert!(
        DockerRuntime::connect_with_verified_execution_storage(
            None,
            verifier,
            receipt,
            restarted_verifier,
        )
        .is_err(),
        "provisioning must still reject a receipt made with the old verifier proof"
    );

    let recovery = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "partial_provision_process_helper", "--nocapture"])
        .env("AUTOSPEC_TASK7_PARTIAL_HELPER", "recover")
        .env("AUTOSPEC_DATABASE_URL", &database_url)
        .env("AUTOSPEC_TASK7_PARTIAL_ROOT", &root)
        .env("AUTOSPEC_TASK7_PARTIAL_REMOTE", &remote)
        .env("AUTOSPEC_TASK7_PARTIAL_DAEMON", &daemon_id)
        .env("AUTOSPEC_TASK7_PARTIAL_IMAGE", &image_id)
        .env("AUTOSPEC_TASK7_PARTIAL_RESTART_IMAGE", &restarted_image_id)
        .env(
            "AUTOSPEC_TASK7_PARTIAL_RESTART_COMMAND",
            restarted_verifier_command,
        )
        .env("AUTOSPEC_TASK7_PARTIAL_WORKER", worker_id.as_str())
        .env("AUTOSPEC_TASK7_PARTIAL_EXECUTION", execution_id.as_str())
        .status()
        .unwrap();
    assert!(recovery.success(), "fresh startup recovery process failed");
    assert!(!docker_resource_exists(
        "network",
        &DockerRuntime::network_name(&execution_id)
    ));
    assert!(!credential.exists());
    assert!(docker_resource_exists("network", &peer_network));
    assert!(
        !root
            .join(format!("worktrees/.cleanup-{execution_id}.json"))
            .exists(),
        "Git cleanup tombstone must be acknowledged"
    );
    assert!(
        !root
            .join(format!("execution-storage/releases/{execution_id}.json"))
            .exists(),
        "storage release tombstone must be acknowledged"
    );
    assert_matrix_case_clean(&root, &execution_id, &worker_id, &reservations, &cleanup).await;
    run(Command::new("docker").args(["network", "rm", &peer_network]));
    delete_matrix_records(&database_url, &execution_id, &worker_id).await;
    if root.exists() {
        fs::remove_dir_all(&root).unwrap();
    }
}

#[tokio::test]
async fn partial_provision_process_helper() {
    let Some(mode) = std::env::var_os("AUTOSPEC_TASK7_PARTIAL_HELPER") else {
        return;
    };
    let mode = mode.to_string_lossy();
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let root = PathBuf::from(std::env::var_os("AUTOSPEC_TASK7_PARTIAL_ROOT").unwrap());
    let remote = PathBuf::from(std::env::var_os("AUTOSPEC_TASK7_PARTIAL_REMOTE").unwrap());
    let daemon = std::env::var("AUTOSPEC_TASK7_PARTIAL_DAEMON").unwrap();
    let image = std::env::var("AUTOSPEC_TASK7_PARTIAL_IMAGE").unwrap();
    let restarted_image = std::env::var("AUTOSPEC_TASK7_PARTIAL_RESTART_IMAGE").ok();
    let restarted_command = std::env::var("AUTOSPEC_TASK7_PARTIAL_RESTART_COMMAND").ok();
    let worker_id = WorkerId::new(std::env::var("AUTOSPEC_TASK7_PARTIAL_WORKER").unwrap());
    let execution_id = ExecutionId::new(std::env::var("AUTOSPEC_TASK7_PARTIAL_EXECUTION").unwrap());
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );

    match mode.as_ref() {
        "provision" => {
            let assigned = reservations
                .reserve_next(&worker_id)
                .await
                .unwrap()
                .expect("original child should reserve the queued execution")
                .execution;
            assert_eq!(assigned.id, execution_id);
            let partial = Arc::new(PartialProvisionRuntime {
                fail_destroy_once: AtomicBool::new(true),
                expected_authority: (database_url.clone(), execution_id.clone()),
            });
            let worker = Worker::new(
                build_system_lifecycle_with_runtime(
                    &root,
                    &remote,
                    &daemon,
                    &image,
                    Some(Arc::new(PartialProvisionFactory(partial))),
                ),
                executions,
                reservations,
                cleanup.clone(),
            );
            assert!(worker.run(&assigned).await.is_err());
            assert!(docker_resource_exists(
                "network",
                &DockerRuntime::network_name(&execution_id)
            ));
            assert_eq!(
                cleanup
                    .get(&execution_id)
                    .await
                    .unwrap()
                    .disposition()
                    .unwrap(),
                CleanupDisposition::RuntimeStopped
            );
        }
        "recover" => {
            let restarted_image = restarted_image.expect("restart verifier image");
            let restarted_command = restarted_command.expect("restart verifier command");
            let worker = Arc::new(Worker::new(
                build_system_lifecycle_with_verifier_config(
                    &root,
                    &remote,
                    &daemon,
                    &restarted_image,
                    &restarted_command,
                ),
                executions,
                reservations,
                cleanup.clone(),
            ));
            let tasks = worker
                .reconcile_startup(&worker_id)
                .await
                .expect("fresh process must use production startup reconciliation");
            assert!(tasks.is_empty());
            assert_eq!(
                cleanup
                    .get(&execution_id)
                    .await
                    .unwrap()
                    .disposition()
                    .unwrap(),
                CleanupDisposition::Resolved
            );
        }
        other => panic!("unknown partial provision helper mode: {other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_cancellation_stops_hung_pi_before_one_terminal_event_and_releases_everything() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task6 cancellation E2E: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task6 cancellation E2E: Docker or alpine:3.20 is unavailable");
        return;
    };
    let suffix = format!(
        "cancel-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task6-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote_root = root.join("remotes");
    create_stub_repository(&remote_root);
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let execution_id = ExecutionId::new(format!("execution-{suffix}"));
    let capability = format!("capability-{suffix}");
    let workers = Arc::new(PgWorkerStore::connect(&database_url).await.unwrap());
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
    let events = Arc::new(PgEventLog::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let labels = OwnershipLabels {
        execution_id: execution_id.clone(),
        worker_id: worker_id.clone(),
        repository: "owner/repo".into(),
        issue: Some("task6-cancel".into()),
    };
    let mut queued = queued_execution(execution_id.clone(), labels, image_id.clone(), capability);
    queued.manifest.persistence = PersistenceMode::Ephemeral;
    queued.manifest.repository.branch = Some(format!("cancel-{suffix}"));
    executions.insert(&queued).await.unwrap();
    let assigned = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .unwrap()
        .execution;
    let worker = Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let task = worker.clone().spawn(assigned);
    for _ in 0..REAL_RUNTIME_RUNNING_WAIT_STEPS {
        if cleanup.get(&execution_id).await.is_ok_and(|authority| {
            authority.disposition().ok() == Some(CleanupDisposition::Active(CleanupStage::Running))
        }) {
            break;
        }
        assert!(
            !task.is_finished(),
            "worker ended before hung Pi was running"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        cleanup
            .get(&execution_id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::Active(CleanupStage::Running)
    );

    let api_token = format!("api-{suffix}");
    let state = AppState::new(
        executions.clone(),
        events.clone(),
        workers,
        reservations.clone(),
        Arc::new(
            PgArtifactStore::connect(&database_url, &root)
                .await
                .unwrap(),
        ),
        api_token.clone(),
        "worker-secret".into(),
    )
    .with_cleanup_authorities(cleanup.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    let response = reqwest::Client::new()
        .post(format!(
            "http://{address}/api/v1/executions/{execution_id}/cancel"
        ))
        .bearer_auth(&api_token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    assert!(!executions
        .get(&execution_id)
        .await
        .unwrap()
        .state
        .is_terminal());
    assert_eq!(
        events
            .since(&execution_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        0
    );
    worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(15), task.join())
        .await
        .expect("controller cancellation must bound hung Pi termination");
    assert!(outcome.is_err());
    assert_eq!(
        executions.get(&execution_id).await.unwrap().state,
        ExecutionState::Cancelled
    );
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
    assert!(!ExecutionLayout::new(&root, &execution_id)
        .unwrap()
        .root
        .exists());
    let remaining = Command::new("docker")
        .args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=autospec.execution_id={execution_id}"),
        ])
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&remaining.stdout).trim().is_empty());
    assert_eq!(
        events
            .since(&execution_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        1
    );

    let before_task_id = ExecutionId::new(format!("execution-before-task-{suffix}"));
    let mut before_task = queued.clone();
    before_task.id = before_task_id.clone();
    before_task.labels.execution_id = before_task_id.clone();
    before_task.state = ExecutionState::Queued;
    before_task.worker_id = None;
    before_task.attempt_id = None;
    before_task.session_id = None;
    before_task.worktree_path = None;
    before_task.result = None;
    before_task.created_at = Utc::now();
    before_task.updated_at = before_task.created_at;
    executions.insert(&before_task).await.unwrap();
    reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .expect("pre-task cancellation execution must reserve");
    executions
        .request_cancellation(&before_task_id)
        .await
        .unwrap();
    worker.reconcile_daemon_tick(&worker_id, &[]).await.unwrap();
    assert_eq!(
        executions.get(&before_task_id).await.unwrap().state,
        ExecutionState::Cancelled
    );
    assert!(!executions
        .cancellation_requested(&before_task_id)
        .await
        .unwrap());
    assert_eq!(
        events
            .since(&before_task_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        1
    );

    let restart_id = ExecutionId::new(format!("execution-post-resolve-{suffix}"));
    let mut restart_execution = queued.clone();
    restart_execution.id = restart_id.clone();
    restart_execution.labels.execution_id = restart_id.clone();
    restart_execution.state = ExecutionState::Queued;
    restart_execution.worker_id = None;
    restart_execution.attempt_id = None;
    restart_execution.session_id = None;
    restart_execution.worktree_path = None;
    restart_execution.result = None;
    restart_execution.created_at = Utc::now();
    restart_execution.updated_at = restart_execution.created_at;
    executions.insert(&restart_execution).await.unwrap();
    let restart_reservation = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .expect("post-resolve cancellation execution must reserve");
    cleanup
        .begin(&restart_id, &restart_reservation.attempt_id, &worker_id)
        .await
        .unwrap();
    executions.request_cancellation(&restart_id).await.unwrap();
    cleanup
        .fence_for_cleanup(&restart_id, &serde_json::json!({}))
        .await
        .unwrap();
    let mut disposition = CleanupDisposition::CleanupPending;
    for next in [
        CleanupDisposition::RuntimeStopped,
        CleanupDisposition::RuntimeDestroyed,
        CleanupDisposition::GitRecoveredCleaned,
        CleanupDisposition::StorageReleased,
    ] {
        cleanup
            .transition(&restart_id, disposition, next, &serde_json::json!({}))
            .await
            .unwrap();
        disposition = next;
    }
    reservations
        .finalize_cleanup(&restart_id, &restart_reservation.attempt_id)
        .await
        .unwrap();
    cleanup.resolve(&restart_id).await.unwrap();
    assert!(executions
        .get(&restart_id)
        .await
        .unwrap()
        .worker_id
        .is_none());
    let restarted_worker = Worker::new(
        build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
        Arc::new(PgExecutionStore::connect(&database_url).await.unwrap()),
        Arc::new(PgReservationStore::connect(&database_url).await.unwrap()),
        Arc::new(
            PgCleanupAuthorityStore::connect(&database_url)
                .await
                .unwrap(),
        ),
    );
    restarted_worker
        .reconcile_daemon_tick(&worker_id, &[])
        .await
        .unwrap();
    let reopened_events = PgEventLog::connect(&database_url).await.unwrap();
    assert_eq!(
        reopened_events
            .since(&restart_id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        1
    );
    assert!(!executions
        .cancellation_requested(&restart_id)
        .await
        .unwrap());
    server.abort();
    delete_matrix_records(&database_url, &execution_id, &worker_id).await;
    delete_matrix_records(&database_url, &before_task_id, &worker_id).await;
    delete_matrix_records(&database_url, &restart_id, &worker_id).await;
    if root.exists() {
        fs::remove_dir_all(root).unwrap();
    }
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
    let worker_id = cleanup.get(&execution_id).await.unwrap().worker_id;
    Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote, &daemon, &image),
        executions,
        reservations,
        cleanup,
    ))
    .reconcile_startup(&worker_id)
    .await
    .expect("production startup reconciliation cleans exact authority");
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
    peer_execution.manifest.services.push(ServiceRequirement {
        name: "cache".into(),
        image: "redis:7-alpine".into(),
        env: BTreeMap::new(),
    });
    peer_execution.role = Role::Review;
    peer_execution.manifest.role = Role::Review;
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
    failed_execution.manifest.services.push(ServiceRequirement {
        name: "cache".into(),
        image: "redis:7-alpine".into(),
        env: BTreeMap::new(),
    });
    executions.insert(&failed_execution).await.unwrap();
    let lifecycle = build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id);
    let worker = Arc::new(Worker::new(
        lifecycle,
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let peer_task = worker.clone().spawn(peer_reservation.execution);
    let peer_layout = ExecutionLayout::new(&root, &peer_id).unwrap();
    for _ in 0..300 {
        if peer_layout
            .credentials
            .join("inferweave.credential")
            .is_file()
            && docker_resource_exists("container", &DockerRuntime::agent_container_name(&peer_id))
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    if peer_task.is_finished() {
        panic!(
            "peer ended before isolation probe: {:?}",
            peer_task.join().await
        );
    }
    let crashed = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "failure_stage_process_helper", "--nocapture"])
        .env("AUTOSPEC_TASK5_MATRIX_HELPER", "1")
        .env("AUTOSPEC_TASK5_MATRIX_STAGE", "running")
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

    assert_execution_boundaries_are_disjoint(&root, &failed_id, &peer_id);
    for id in [&failed_id, &peer_id] {
        let layout = ExecutionLayout::new(&root, id).unwrap();
        let credential =
            fs::read_to_string(layout.credentials.join("inferweave.credential")).unwrap();
        let token = credential.lines().next().unwrap();
        assert_secret_absent_from_durable_records(&database_url, token).await;
        assert!(
            !fs::read_to_string(peer_result.diff_artifact.as_ref().unwrap())
                .unwrap()
                .contains(token)
        );
    }

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

    assert!(peer_layout.root.exists());
    assert!(peer_layout
        .credentials
        .join("inferweave.credential")
        .is_file());
    assert!(peer_layout.session.exists());
    assert!(docker_resource_exists(
        "container",
        &DockerRuntime::agent_container_name(&peer_id)
    ));
    assert!(docker_resource_exists(
        "network",
        &DockerRuntime::network_name(&peer_id)
    ));
    assert_service_mounts_and_network_bounded(
        &peer_id,
        &peer_layout.root,
        &ExecutionLayout::new(&root, &failed_id).unwrap().root,
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_forks_a_running_conversation_by_quiescing_and_resuming_the_target() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task7 interactive controls: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task7 interactive controls: Docker is unavailable");
        return;
    };
    let suffix = format!(
        "interactive-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task7-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote_root = root.join("remotes");
    create_stub_repository(&remote_root);
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let execution_id = ExecutionId::new(format!("control-{suffix}"));
    let capability = format!("control-capability-{suffix}");
    let registration =
        matrix_worker_registration(worker_id.clone(), capability.clone(), &daemon_id, &image_id);
    let workers = PgWorkerStore::connect(&database_url).await.unwrap();
    workers.register(&registration).await.unwrap();
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let events = PgEventLog::connect(&database_url).await.unwrap();
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let mut queued = queued_execution(
        execution_id.clone(),
        OwnershipLabels {
            execution_id: execution_id.clone(),
            worker_id: worker_id.clone(),
            repository: "owner/repo".into(),
            issue: Some("interactive".into()),
        },
        image_id.clone(),
        capability,
    );
    queued.role = Role::Interactive;
    queued.manifest.role = Role::Interactive;
    queued.manifest.persistence = PersistenceMode::Resumable;
    queued.manifest.repository.branch = Some(format!("interactive-{suffix}"));
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .unwrap();
    let worker = Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let task = worker.clone().spawn(reservation.execution);
    let running = wait_for_running_session(&executions, &execution_id).await;
    let original_session = running.session_id.clone().unwrap();
    let original_worktree = running.worktree_path.clone();

    executions
        .request_control(
            &execution_id,
            ExecutionControlAction::ForkConversation,
            "fork-real",
        )
        .await
        .unwrap();
    worker
        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
        .await
        .unwrap();
    let forked = wait_for_new_session(&executions, &execution_id, &original_session).await;
    assert_eq!(forked.state, ExecutionState::Running);
    assert_eq!(forked.worktree_path, original_worktree);
    let result = task.join().await.unwrap();
    assert_eq!(result.state, ExecutionState::ReviewReady);
    let interactive_events = events.since(&execution_id, 0).await.unwrap();
    assert_eq!(
        interactive_events
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionPaused))
            .count(),
        0
    );
    assert_eq!(
        interactive_events
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionResumed))
            .count(),
        0
    );
    assert_eq!(
        interactive_events
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ConversationForked { .. }))
            .count(),
        1
    );
    let layout = ExecutionLayout::new(&root, &execution_id).unwrap();
    assert_eq!(
        fs::read_to_string(layout.conversation.join("invocations"))
            .unwrap()
            .trim(),
        "3",
        "start, fork, and resume each invoke Pi once without replay loops"
    );

    let retained = cleanup.get(&execution_id).await.unwrap();
    cleanup
        .transition(
            &execution_id,
            CleanupDisposition::Retained,
            CleanupDisposition::CleanupPending,
            &retained.handles,
        )
        .await
        .unwrap();
    worker
        .recover_cleanup_authority(
            &cleanup.get(&execution_id).await.unwrap(),
            &executions.get(&execution_id).await.unwrap(),
        )
        .await
        .unwrap();
    assert_matrix_case_clean(&root, &execution_id, &worker_id, &reservations, &cleanup).await;
    delete_matrix_records(&database_url, &execution_id, &worker_id).await;
    if root.exists() {
        fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adversarial_agent_credential_echo_and_copy_are_not_durably_exfiltrated() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("skipping real Task7 credential exfiltration: AUTOSPEC_DATABASE_URL is unset");
        return;
    };
    let _serial = acquire_real_test_lock(&database_url).await;
    let Some((daemon_id, image_id)) = docker_capability() else {
        eprintln!("skipping real Task7 credential exfiltration: Docker is unavailable");
        return;
    };
    let suffix = format!(
        "secret-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let root = std::env::temp_dir().join(format!("autospec-task7-{suffix}"));
    initialize_state_root(&root);
    let root = root.canonicalize().unwrap();
    let remote_root = root.join("remotes");
    create_secret_exfiltrating_stub_repository(&remote_root);
    let worker_id = WorkerId::new(format!("worker-{suffix}"));
    let execution_id = ExecutionId::new(format!("execution-{suffix}"));
    let capability = format!("secret-capability-{suffix}");
    let registration =
        matrix_worker_registration(worker_id.clone(), capability.clone(), &daemon_id, &image_id);
    PgWorkerStore::connect(&database_url)
        .await
        .unwrap()
        .register(&registration)
        .await
        .unwrap();
    let executions = Arc::new(PgExecutionStore::connect(&database_url).await.unwrap());
    let reservations = Arc::new(PgReservationStore::connect(&database_url).await.unwrap());
    let cleanup = Arc::new(
        PgCleanupAuthorityStore::connect(&database_url)
            .await
            .unwrap(),
    );
    let mut queued = queued_execution(
        execution_id.clone(),
        OwnershipLabels {
            execution_id: execution_id.clone(),
            worker_id: worker_id.clone(),
            repository: "owner/repo".into(),
            issue: Some("secret".into()),
        },
        image_id.clone(),
        capability,
    );
    queued.manifest.persistence = PersistenceMode::Ephemeral;
    queued.manifest.repository.branch = Some(format!("secret-{suffix}"));
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .unwrap();
    let worker = Arc::new(Worker::new(
        build_system_lifecycle(&root, &remote_root, &daemon_id, &image_id),
        executions.clone(),
        reservations.clone(),
        cleanup.clone(),
    ));
    let task = worker.spawn(reservation.execution);
    wait_for_running_session(&executions, &execution_id).await;
    let layout = ExecutionLayout::new(&root, &execution_id).unwrap();
    for _ in 0..300 {
        if layout.repository.join("leaked.txt").is_file() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let credential = fs::read_to_string(layout.credentials.join("inferweave.credential")).unwrap();
    let token = credential.lines().next().unwrap().to_owned();
    assert_eq!(
        fs::read_to_string(layout.repository.join("leaked.txt")).unwrap(),
        token
    );
    let packet = fs::read(layout.repository.join(".autospec/task-packet.json")).unwrap();
    assert!(!packet
        .windows(token.len())
        .any(|window| window == token.as_bytes()));
    for log in [
        layout
            .session
            .join(format!("pi.events-{execution_id}.jsonl")),
        layout.session.join(format!("pi.stderr-{execution_id}.log")),
    ] {
        assert!(
            log.is_file(),
            "expected materialized Pi log: {}",
            log.display()
        );
        let bytes = fs::read(log).unwrap();
        assert!(!bytes
            .windows(token.len())
            .any(|window| window == token.as_bytes()));
    }
    let error = tokio::time::timeout(std::time::Duration::from_secs(30), task.join())
        .await
        .expect("recognized terminal event must finish the execution")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("credential") || error.contains("secret"),
        "exfiltration must fail closed: {error}"
    );
    assert!(!error.contains(&token));
    assert_secret_absent_from_durable_records(&database_url, &token).await;
    assert!(!layout.root.exists(), "failed exfiltration must be cleaned");
    assert_secret_absent_from_tree(&root, &token);
    assert_matrix_case_clean(&root, &execution_id, &worker_id, &reservations, &cleanup).await;
    delete_matrix_records(&database_url, &execution_id, &worker_id).await;
    if root.exists() {
        fs::remove_dir_all(root).unwrap();
    }
}

async fn wait_for_exact_state(
    store: &PgExecutionStore,
    id: &ExecutionId,
    expected: ExecutionState,
) -> Execution {
    for _ in 0..300 {
        let execution = store.get(id).await.unwrap();
        if execution.state == expected {
            return execution;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("execution {id} did not reach {expected:?}");
}

async fn wait_for_new_session(
    store: &PgExecutionStore,
    id: &ExecutionId,
    previous: &orchestrator_core::SessionId,
) -> Execution {
    for _ in 0..300 {
        let execution = store.get(id).await.unwrap();
        if execution
            .session_id
            .as_ref()
            .is_some_and(|id| id != previous)
        {
            return execution;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("execution {id} did not persist a forked Pi session");
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
    if stage == "running" {
        std::process::exit(77);
    }
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
    if let Ok(stage) = std::env::var("AUTOSPEC_TASK7_CONTROL_RECOVERY") {
        let execution_id = ExecutionId::new(std::env::var("AUTOSPEC_TASK7_EXECUTION").unwrap());
        let expected_session = std::env::var("AUTOSPEC_TASK7_EXPECTED_SESSION").unwrap();
        let live = matches!(
            stage.as_str(),
            "resume_side_effect" | "running_fork_side_effect"
        );
        let observer = Arc::new(AssertRecoveryBoundary {
            root: root.clone(),
            execution_id: execution_id.clone(),
            expected_session,
            live,
            seen: AtomicBool::new(false),
        });
        let worker = Arc::new(
            Worker::new(
                build_system_lifecycle(&root, &remote, &daemon, &image),
                executions.clone(),
                reservations.clone(),
                cleanup.clone(),
            )
            .with_control_checkpoint_observer(observer.clone()),
        );
        let mut tasks = worker
            .reconcile_startup(&worker_id)
            .await
            .expect("production startup reconciliation adopts the exact control authority");
        assert_eq!(tasks.len(), 1);
        worker
            .reconcile_daemon_tick(&worker_id, &tasks)
            .await
            .unwrap();
        if matches!(
            stage.as_str(),
            "pause_side_effect" | "paused_fork_side_effect"
        ) {
            wait_for_exact_state(
                executions.as_ref(),
                &execution_id,
                ExecutionState::PausedForHuman,
            )
            .await;
            assert!(execution_storage::ExecutionLifecycleHoldStore::new(&root)
                .unwrap()
                .list(&execution_id)
                .unwrap()
                .is_empty());
            executions
                .request_control(
                    &execution_id,
                    ExecutionControlAction::Resume,
                    &format!("finish-{stage}"),
                )
                .await
                .unwrap();
            worker
                .reconcile_daemon_tick(&worker_id, &tasks)
                .await
                .unwrap();
        }
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tasks.pop().unwrap().join(),
        )
        .await
        .expect("fresh process must reconcile the control")
        .unwrap();
        assert_eq!(result.state, ExecutionState::ReviewReady);
        assert!(
            observer.seen.load(Ordering::SeqCst),
            "fresh recovery must cross the asserted Pi liveness boundary"
        );
        return;
    }
    let assigned = reservations
        .reserve_next(&worker_id)
        .await
        .unwrap()
        .expect("child should reserve queued execution")
        .execution;
    let execution_id = assigned.id.clone();
    let mut worker = Worker::new(
        build_system_lifecycle(&root, &remote, &daemon, &image),
        executions.clone(),
        reservations,
        cleanup,
    );
    if let Ok(stage) = std::env::var("AUTOSPEC_TASK7_CONTROL_CRASH") {
        let checkpoint = match stage.as_str() {
            "pause_side_effect"
            | "resume_side_effect"
            | "paused_fork_side_effect"
            | "running_fork_side_effect" => ControlCheckpoint::SideEffectPersisted,
            other => panic!("unknown control crash stage {other}"),
        };
        let skip = usize::from(matches!(
            stage.as_str(),
            "resume_side_effect" | "paused_fork_side_effect"
        ));
        worker = worker.with_control_checkpoint_observer(Arc::new(ExitAtNthControlCheckpoint {
            checkpoint,
            skip,
            seen: AtomicUsize::new(0),
        }));
    }
    let worker = Arc::new(worker);
    let task = worker.clone().spawn(assigned);
    for _ in 0..REAL_RUNTIME_RUNNING_WAIT_STEPS {
        let execution = executions.get(&execution_id).await.unwrap();
        if execution.state == ExecutionState::Running && execution.session_id.is_some() {
            if let Ok(stage) = std::env::var("AUTOSPEC_TASK7_CONTROL_CRASH") {
                if matches!(
                    stage.as_str(),
                    "resume_side_effect" | "paused_fork_side_effect"
                ) {
                    executions
                        .request_control(
                            &execution_id,
                            ExecutionControlAction::Pause,
                            "prepare-resume-crash",
                        )
                        .await
                        .unwrap();
                    worker
                        .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
                        .await
                        .unwrap();
                    wait_for_exact_state(
                        executions.as_ref(),
                        &execution_id,
                        ExecutionState::PausedForHuman,
                    )
                    .await;
                }
                let action = match stage.as_str() {
                    "pause_side_effect" => ExecutionControlAction::Pause,
                    "resume_side_effect" => ExecutionControlAction::Resume,
                    "paused_fork_side_effect" | "running_fork_side_effect" => {
                        ExecutionControlAction::ForkConversation
                    }
                    _ => unreachable!(),
                };
                executions
                    .request_control(&execution_id, action, &format!("crash-{stage}"))
                    .await
                    .unwrap();
                worker
                    .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
                    .await
                    .unwrap();
                for _ in 0..300 {
                    if task.is_finished() {
                        panic!("worker ended before control crash hook at {stage}");
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                panic!("control crash hook did not exit at {stage}");
            }
            if std::env::var_os("AUTOSPEC_TASK7_PAUSE_BEFORE_CRASH").is_some() {
                executions
                    .request_control(
                        &execution_id,
                        ExecutionControlAction::Pause,
                        "pause-before-process-crash",
                    )
                    .await
                    .unwrap();
                worker
                    .reconcile_daemon_tick(&worker_id, std::slice::from_ref(&task))
                    .await
                    .unwrap();
                let paused = wait_for_exact_state(
                    executions.as_ref(),
                    &execution_id,
                    ExecutionState::PausedForHuman,
                )
                .await;
                assert_eq!(paused.session_id, execution.session_id);
                assert!(execution_storage::ExecutionLifecycleHoldStore::new(&root)
                    .unwrap()
                    .list(&execution_id)
                    .unwrap()
                    .is_empty());
            }
            std::process::exit(77);
        }
        if task.is_finished() {
            panic!("worker ended before durable Running");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("execution did not reach durable Running session");
}

async fn wait_for_running_session(store: &PgExecutionStore, id: &ExecutionId) -> Execution {
    for _ in 0..REAL_RUNTIME_RUNNING_WAIT_STEPS {
        let execution = store.get(id).await.unwrap();
        if execution.state == ExecutionState::Running && execution.session_id.is_some() {
            return execution;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("execution did not reach durable Running session");
}

fn docker_resource_exists(kind: &str, name: &str) -> bool {
    let mut command = Command::new("docker");
    match kind {
        "container" => {
            command.args(["inspect", "--type", "container", name]);
        }
        "network" => {
            command.args(["network", "inspect", name]);
        }
        "volume" => {
            command.args(["volume", "inspect", name]);
        }
        _ => panic!("unsupported Docker resource kind {kind}"),
    }
    command.output().is_ok_and(|output| output.status.success())
}

#[derive(Debug)]
struct OwnedDockerResource {
    kind: String,
    id: String,
    name: String,
    labels: BTreeMap<String, String>,
}

fn capture_owned_docker_resources(labels: &OwnershipLabels) -> Vec<OwnedDockerResource> {
    let mut resources = Vec::new();
    for (kind, list_arguments, inspect_arguments) in [
        (
            "container",
            vec!["ps", "-aq"],
            vec!["inspect", "--type", "container"],
        ),
        (
            "network",
            vec!["network", "ls", "-q"],
            vec!["network", "inspect"],
        ),
        (
            "volume",
            vec!["volume", "ls", "-q"],
            vec!["volume", "inspect"],
        ),
    ] {
        let mut list = Command::new("docker");
        list.args(&list_arguments);
        for selector in labels.selector() {
            list.args(["--filter", &format!("label={selector}")]);
        }
        let output = list.output().unwrap();
        assert!(output.status.success(), "list owned Docker {kind}s");
        for id in String::from_utf8(output.stdout).unwrap().lines() {
            let output = Command::new("docker")
                .args(&inspect_arguments)
                .arg(id)
                .output()
                .unwrap();
            assert!(output.status.success(), "inspect owned Docker {kind} {id}");
            let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            let value = &value[0];
            let resource_labels = if kind == "container" {
                &value["Config"]["Labels"]
            } else {
                &value["Labels"]
            };
            resources.push(OwnedDockerResource {
                kind: kind.into(),
                id: value["Id"].as_str().unwrap_or(id).to_owned(),
                name: value["Name"]
                    .as_str()
                    .unwrap_or(id)
                    .trim_start_matches('/')
                    .to_owned(),
                labels: serde_json::from_value(resource_labels.clone()).unwrap(),
            });
        }
    }
    resources.sort_by(|left, right| {
        (&left.kind, &left.name, &left.id).cmp(&(&right.kind, &right.name, &right.id))
    });
    resources
}

fn assert_owned_docker_resources_absent(
    resources: &[OwnedDockerResource],
    labels: &OwnershipLabels,
) {
    for resource in resources {
        assert!(!docker_resource_exists(&resource.kind, &resource.id));
        assert!(!docker_resource_exists(&resource.kind, &resource.name));
    }
    assert!(capture_owned_docker_resources(labels).is_empty());
}

fn sha256_file(path: &Path) -> String {
    for (program, arguments) in [("shasum", vec!["-a", "256"]), ("sha256sum", vec![])] {
        let output = Command::new(program).args(arguments).arg(path).output();
        if let Ok(output) = output {
            if output.status.success() {
                return String::from_utf8(output.stdout)
                    .unwrap()
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .to_owned();
            }
        }
    }
    panic!("no SHA-256 verifier is available");
}

async fn assert_secret_absent_from_durable_records(database_url: &str, secret: &str) {
    let pool = sqlx::PgPool::connect(database_url).await.unwrap();
    for table in [
        "artifact_blobs",
        "execution_requests",
        "execution_control_requests",
        "execution_cancellation_requests",
        "reservations",
        "workers",
        "executions",
        "execution_attempts",
        "execution_events",
        "cleanup_authorities",
        "artifacts",
    ] {
        let query = format!(
            "SELECT EXISTS(SELECT 1 FROM {table} record WHERE to_jsonb(record)::text LIKE '%' || $1 || '%')"
        );
        let present: bool = sqlx::query_scalar(&query)
            .bind(secret)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!present, "credential material persisted in {table}");
    }
}

fn assert_secret_absent_from_tree(root: &Path, secret: &str) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let metadata = fs::symlink_metadata(&path).unwrap();
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            pending.extend(
                fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
            continue;
        }
        if metadata.is_file() {
            let bytes = fs::read(&path).unwrap();
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "credential material persisted in {}",
                path.display()
            );
        }
    }
}

fn assert_execution_boundaries_are_disjoint(
    root: &Path,
    implementation_id: &ExecutionId,
    review_id: &ExecutionId,
) {
    let implementation = ExecutionLayout::new(root, implementation_id).unwrap();
    let review = ExecutionLayout::new(root, review_id).unwrap();
    assert_ne!(implementation.root, review.root);
    assert_ne!(
        implementation.repository.canonicalize().unwrap(),
        review.repository.canonicalize().unwrap()
    );
    assert_ne!(
        git_path(&implementation.repository, &["--git-common-dir"]),
        git_path(&review.repository, &["--git-common-dir"])
    );
    assert_ne!(
        git_path(&implementation.repository, &["--git-path", "objects"]),
        git_path(&review.repository, &["--git-path", "objects"])
    );
    assert_ne!(
        implementation.session.canonicalize().unwrap(),
        review.session.canonicalize().unwrap()
    );
    assert_ne!(
        implementation.conversation.canonicalize().unwrap(),
        review.conversation.canonicalize().unwrap()
    );
    let implementation_credential = implementation.credentials.join("inferweave.credential");
    let review_credential = review.credentials.join("inferweave.credential");
    assert_ne!(
        fs::read(&implementation_credential).unwrap(),
        fs::read(&review_credential).unwrap()
    );
    assert_ne!(
        DockerRuntime::network_name(implementation_id),
        DockerRuntime::network_name(review_id)
    );
    assert_ne!(
        DockerRuntime::agent_container_name(implementation_id),
        DockerRuntime::agent_container_name(review_id)
    );
    assert_agent_mounts_bounded(implementation_id, &implementation.root, &review.root);
    assert_agent_mounts_bounded(review_id, &review.root, &implementation.root);
    assert_service_mounts_and_network_bounded(
        implementation_id,
        &implementation.root,
        &review.root,
    );
    assert_service_mounts_and_network_bounded(review_id, &review.root, &implementation.root);
    let implementation_service = docker_container_id_by_name(
        &DockerRuntime::service_container_name(implementation_id, "cache"),
    );
    let review_service =
        docker_container_id_by_name(&DockerRuntime::service_container_name(review_id, "cache"));
    assert_ne!(implementation_service, review_service);
    for id in [implementation_id, review_id] {
        let volumes = Command::new("docker")
            .args([
                "volume",
                "ls",
                "--filter",
                "label=autospec.managed=true",
                "--filter",
                &format!("label=autospec.execution_id={id}"),
                "--format",
                "{{.Name}}",
            ])
            .output()
            .unwrap();
        assert!(volumes.status.success());
        assert!(String::from_utf8(volumes.stdout).unwrap().trim().is_empty());
    }
}

fn docker_container_id_by_name(name: &str) -> String {
    let output = Command::new("docker")
        .args(["inspect", "--type", "container", "--format={{.Id}}", name])
        .output()
        .unwrap();
    assert!(output.status.success(), "missing container {name}");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn assert_service_mounts_and_network_bounded(id: &ExecutionId, own_root: &Path, peer_root: &Path) {
    let name = DockerRuntime::service_container_name(id, "cache");
    let output = Command::new("docker")
        .args(["inspect", "--type", "container", &name])
        .output()
        .unwrap();
    assert!(output.status.success(), "missing service container {name}");
    let inspect: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let inspect = &inspect[0];
    let networks = inspect["NetworkSettings"]["Networks"].as_object().unwrap();
    assert_eq!(networks.len(), 1);
    assert!(networks.contains_key(&DockerRuntime::network_name(id)));
    for mount in inspect["Mounts"].as_array().unwrap() {
        assert_eq!(mount["Type"], "bind");
        assert_ne!(mount["Destination"], "/autospec-credential");
        let source = PathBuf::from(mount["Source"].as_str().unwrap())
            .canonicalize()
            .unwrap();
        assert!(
            source.starts_with(own_root),
            "foreign service bind: {source:?}"
        );
        assert!(
            !source.starts_with(peer_root),
            "peer service bind: {source:?}"
        );
    }
}

fn git_path(repository: &Path, arguments: &[&str]) -> PathBuf {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .arg("rev-parse")
        .args(arguments)
        .output()
        .unwrap();
    assert!(output.status.success());
    let path = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());
    let path = if path.is_absolute() {
        path
    } else {
        repository.join(path)
    };
    path.canonicalize().unwrap()
}

fn assert_agent_mounts_bounded(id: &ExecutionId, own_root: &Path, peer_root: &Path) {
    let output = Command::new("docker")
        .args([
            "inspect",
            "--type",
            "container",
            "--format={{json .Mounts}}",
            &DockerRuntime::agent_container_name(id),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let mounts: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let mounts = mounts.as_array().unwrap();
    assert!(!mounts.is_empty());
    for mount in mounts {
        assert_eq!(mount["Type"], "bind");
        let source = PathBuf::from(mount["Source"].as_str().unwrap())
            .canonicalize()
            .unwrap();
        assert!(source.starts_with(own_root), "foreign bind: {source:?}");
        assert!(!source.starts_with(peer_root), "peer bind: {source:?}");
        if mount["Destination"] == "/autospec-credential" {
            assert_eq!(mount["RW"], false);
        }
    }
}

fn build_system_lifecycle(
    root: &Path,
    remote_root: &Path,
    daemon_id: &str,
    image_id: &str,
) -> Arc<SystemExecutionLifecycle> {
    build_system_lifecycle_with_runtime(root, remote_root, daemon_id, image_id, None)
}

fn build_task9_lifecycle(
    root: &Path,
    remote_root: &Path,
    daemon_id: &str,
    image_id: &str,
    artifacts: Arc<PgArtifactStore>,
) -> Arc<SystemExecutionLifecycle> {
    let trusted = TrustedVerifierImage::new(image_id, "/bin/stat").unwrap();
    let storage = Arc::new(
        ExecutionStorage::new(
            root,
            Box::new(FixedFilesystemBackend),
            Box::new(TestDockerBindVerifier {
                daemon_id: daemon_id.into(),
                image_id: image_id.into(),
                command: "/bin/stat".into(),
                method: trusted.proof_method(),
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
            Arc::new(LocalCredentialBroker::new(root, chrono::Duration::minutes(15)).unwrap())
                as Arc<dyn CredentialBroker>,
        )),
        Arc::new(VerifiedPiHarnessFactory::new(
            verifier,
            None,
            PathBuf::from("docker"),
            "/workspace/pi".into(),
            Vec::new(),
            Vec::new(),
        )),
        Arc::new(ContentAddressedEvidenceStore::new(artifacts)),
    ))
}

#[derive(Clone, Copy)]
struct TestVerifierConfig<'a> {
    image_id: &'a str,
    command: &'a str,
}

fn build_system_lifecycle_with_verifier_config(
    root: &Path,
    remote_root: &Path,
    daemon_id: &str,
    verifier_image_id: &str,
    verifier_command: &str,
) -> Arc<SystemExecutionLifecycle> {
    let verifier = TestVerifierConfig {
        image_id: verifier_image_id,
        command: verifier_command,
    };
    build_system_lifecycle_with_configs(root, remote_root, daemon_id, verifier, verifier, None)
}

fn build_system_lifecycle_with_runtime(
    root: &Path,
    remote_root: &Path,
    daemon_id: &str,
    image_id: &str,
    runtime_override: Option<Arc<dyn RuntimeFactory>>,
) -> Arc<SystemExecutionLifecycle> {
    let verifier = TestVerifierConfig {
        image_id,
        command: "/bin/stat",
    };
    build_system_lifecycle_with_configs(
        root,
        remote_root,
        daemon_id,
        verifier,
        verifier,
        runtime_override,
    )
}

fn build_system_lifecycle_with_configs(
    root: &Path,
    remote_root: &Path,
    daemon_id: &str,
    storage_verifier: TestVerifierConfig<'_>,
    runtime_verifier: TestVerifierConfig<'_>,
    runtime_override: Option<Arc<dyn RuntimeFactory>>,
) -> Arc<SystemExecutionLifecycle> {
    let storage_trusted =
        TrustedVerifierImage::new(storage_verifier.image_id, storage_verifier.command).unwrap();
    let trusted =
        TrustedVerifierImage::new(runtime_verifier.image_id, runtime_verifier.command).unwrap();
    let method = storage_trusted.proof_method();
    let storage = Arc::new(
        ExecutionStorage::new(
            root,
            Box::new(FixedFilesystemBackend),
            Box::new(TestDockerBindVerifier {
                daemon_id: daemon_id.into(),
                image_id: storage_verifier.image_id.into(),
                command: storage_verifier.command.into(),
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
        runtime_override.unwrap_or_else(|| {
            Arc::new(VerifiedDockerRuntimeFactory::new(
                None,
                PathBuf::from("docker"),
                verifier.clone(),
                trusted,
                Arc::new(LocalCredentialBroker::new(root, chrono::Duration::minutes(15)).unwrap())
                    as Arc<dyn CredentialBroker>,
            ))
        }),
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
            health_errors: Vec::new(),
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
    sqlx::query("DELETE FROM execution_requests WHERE execution_id = $1")
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

fn create_task9_fixture_repository(remote_root: &Path) {
    let remote = remote_root.join("owner/repo.git");
    fs::create_dir_all(remote.parent().unwrap()).unwrap();
    run(Command::new("git").args(["init", "--bare", remote.to_str().unwrap()]));
    let seed = remote_root.join("seed");
    run(Command::new("git").args(["init", seed.to_str().unwrap()]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["config", "user.name", "Task9 Audit"]));
    run(Command::new("git").current_dir(&seed).args([
        "config",
        "user.email",
        "task9@example.invalid",
    ]));
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/task9-pi-complete.sh");
    fs::copy(&fixture, seed.join("pi"))
        .unwrap_or_else(|error| panic!("copy Task9 Pi fixture {}: {error}", fixture.display()));
    #[cfg(unix)]
    fs::set_permissions(seed.join("pi"), fs::Permissions::from_mode(0o755)).unwrap();
    run(Command::new("git").current_dir(&seed).args(["add", "pi"]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["commit", "-m", "seed Task9 Pi fixture"]));
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

fn create_secret_exfiltrating_stub_repository(remote_root: &Path) {
    let remote = remote_root.join("owner/repo.git");
    fs::create_dir_all(remote.parent().unwrap()).unwrap();
    run(Command::new("git").args(["init", "--bare", remote.to_str().unwrap()]));
    let seed = remote_root.join("seed");
    run(Command::new("git").args(["init", seed.to_str().unwrap()]));
    run(Command::new("git")
        .current_dir(&seed)
        .args(["config", "user.name", "Task7 E2E"]));
    run(Command::new("git").current_dir(&seed).args([
        "config",
        "user.email",
        "task7@example.invalid",
    ]));
    let pi = seed.join("pi");
    fs::write(
        &pi,
        "#!/bin/sh\nsession_id=\nsource_session=\nwhile test \"$#\" -gt 0; do\n  case \"$1\" in\n    --session-id) session_id=$2; shift 2 ;;\n    --session|--fork) source_session=$2; shift 2 ;;\n    *) shift ;;\n  esac\ndone\nif test -z \"$session_id\" && test -n \"$source_session\"; then\n  session_id=$(sed -n '1s/.*\"id\":\"\\([^\"]*\\)\".*/\\1/p' \"$source_session\")\nfi\nif test -n \"$session_id\" && test ! -s \"/session/session_${session_id}.jsonl\"; then\n  printf '{\"type\":\"session\",\"version\":3,\"id\":\"%s\",\"cwd\":\"/workspace\"}\\n' \"$session_id\" > \"/session/session_${session_id}.jsonl\"\nfi\ncount=1\nif test -f /session/invocations; then count=$(( $(cat /session/invocations) + 1 )); fi\nprintf '%s\\n' \"$count\" > /session/invocations\nprintf '%s\\n' '{\"type\":\"session\",\"id\":\"e2e\"}' '{\"type\":\"agent_start\"}' '{\"type\":\"turn_start\"}'\nsecret=$(sed -n '1p' \"$INFERWEAVE_CREDENTIAL_FILE\")\nprintf '%s' \"$secret\" > /workspace/leaked.txt\nprintf '%s\\n' \"$secret\" >&2\nprintf 'worker-e2e-%s\\n' \"$count\" > /workspace/result.txt\nprintf '%s\\n' '{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"done\"}],\"stopReason\":\"stop\"}}' '{\"type\":\"agent_settled\"}'\n",
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
            "#!/bin/sh\nsession_id=\nsource_session=\nwhile test \"$#\" -gt 0; do\n  case \"$1\" in\n    --session-id) session_id=$2; shift 2 ;;\n    --session|--fork) source_session=$2; shift 2 ;;\n    *) shift ;;\n  esac\ndone\nif test -z \"$session_id\" && test -n \"$source_session\"; then\n  session_id=$(sed -n '1s/.*\"id\":\"\\([^\"]*\\)\".*/\\1/p' \"$source_session\")\nfi\nif test -n \"$session_id\" && test ! -s \"/session/session_${{session_id}}.jsonl\"; then\n  printf '{{\"type\":\"session\",\"version\":3,\"id\":\"%s\",\"cwd\":\"/workspace\"}}\\n' \"$session_id\" > \"/session/session_${{session_id}}.jsonl\"\nfi\ncount=1\nif test -f /session/invocations; then count=$(( $(cat /session/invocations) + 1 )); fi\nprintf '%s\\n' \"$count\" > /session/invocations\nprintf '%s\\n' '{{\"type\":\"session\",\"id\":\"e2e\"}}' '{{\"type\":\"agent_start\"}}' '{{\"type\":\"turn_start\"}}'\n{first_run_gate}printf 'worker-e2e-%s\\n' \"$count\" > /workspace/result.txt\nprintf '%s\\n' '{{\"type\":\"message_end\",\"message\":{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"done\"}}],\"stopReason\":\"stop\"}}}}' '{{\"type\":\"agent_settled\"}}'\n"
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
