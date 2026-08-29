use bollard::{
    container::{Config, CreateContainerOptions, LogOutput},
    exec::{CreateExecOptions, StartExecOptions, StartExecResults},
    image::{CommitContainerOptions, CreateImageOptions, RemoveImageOptions},
    models::{HostConfig, Mount, MountTypeEnum},
    network::CreateNetworkOptions,
    volume::CreateVolumeOptions,
    Docker,
};
#[cfg(target_os = "macos")]
use execution_storage::ApfsBackend;
#[cfg(target_os = "linux")]
use execution_storage::LvmBackend;
use execution_storage::{
    AllocationReceipt, AllocationRequest, BackendIdentity, CommandRunner, DockerBindCapability,
    DockerBindProof, DockerBindVerifier, ExecutionStorage, ExecutionStorageManager,
    ProcessCommandRunner, ReadyAllocationVerifier, ReadyLease, StorageBackend, StorageError,
    VerifiedExecutionStorage, ALLOCATION_API_VERSION,
};
use futures_util::StreamExt;
use orchestrator_core::{
    labels, ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement, WorkerId,
};
use runtime_docker::{host_limits, DockerRuntime, TrustedVerifierImage, DEFAULT_PIDS_LIMIT};
use runtime_traits::Runtime;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
fn secure_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("set owner-only mode");
}

static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestStateRoot {
    path: PathBuf,
}

impl TestStateRoot {
    fn new(labels: &OwnershipLabels) -> Self {
        let path = env::temp_dir().join(format!(
            "autospec-runtime-docker-state-{}",
            labels.execution_id
        ));
        fs::create_dir(&path).expect("create unique test state root");
        let state = Self { path };
        state.add_execution(labels);
        state
    }

    fn add_execution(&self, labels: &OwnershipLabels) {
        let root = self
            .path
            .join("executions")
            .join(labels.execution_id.as_str());
        fs::create_dir_all(root.join("repository")).expect("create execution repository");
        fs::create_dir_all(root.join("runtime")).expect("create execution runtime");
        let session = root.join("session");
        fs::create_dir_all(&session).expect("create host-private session root");
        fs::create_dir(session.join("conversation")).expect("create conversation");
        for (name, contents) in [
            ("owner.json", "host owner\n"),
            (".cursor", "host cursor\n"),
            ("resume-count", "7\n"),
            (
                &format!("pi.events-{}.jsonl", labels.execution_id),
                "host live events\n",
            ),
        ] {
            fs::write(session.join(name), contents).expect("write host-private session metadata");
        }
    }

    fn worktree(&self, labels: &OwnershipLabels) -> PathBuf {
        self.path
            .join("executions")
            .join(labels.execution_id.as_str())
            .join("repository")
    }

    fn session(&self, labels: &OwnershipLabels) -> PathBuf {
        self.path
            .join("executions")
            .join(labels.execution_id.as_str())
            .join("session")
    }

    fn receipt(&self, labels: &OwnershipLabels, disk_gib: u64) -> AllocationReceipt {
        let root = self
            .path
            .join("executions")
            .join(labels.execution_id.as_str());
        AllocationReceipt {
            api_version: ALLOCATION_API_VERSION.to_owned(),
            labels: labels.clone(),
            reserved_bytes: disk_gib * 1024 * 1024 * 1024,
            mount_path: root.clone(),
            backend_kind: "test".to_owned(),
            backend_key: format!("test:{}", labels.execution_id),
            pool_identity: "test-pool".to_owned(),
            backend: BackendIdentity::Apfs {
                container: "test-container".to_owned(),
                container_uuid: "test-pool".to_owned(),
                volume: "test-volume".to_owned(),
                volume_name: format!("autospec-{}", labels.execution_id),
                volume_uuid: "test-filesystem".to_owned(),
                ownership_token: "test-token".to_owned(),
            },
            docker_bind: DockerBindProof {
                daemon_id: "test-daemon".to_owned(),
                verifier: "test-ready-verifier".to_owned(),
                method_version: "v1".to_owned(),
                source_path: root,
                filesystem_id: "test-device".to_owned(),
            },
        }
    }
}

#[derive(Debug)]
struct TestReadyVerifier;

#[derive(Debug)]
struct RejectReadyAfterContainerCreate {
    docker: PathBuf,
    container: String,
}

#[derive(Debug)]
struct ObserveProvisionLease {
    docker: PathBuf,
    agent_container: String,
    active: Arc<AtomicBool>,
    observed_after_create: Arc<AtomicBool>,
}

#[derive(Debug)]
struct ObservedReadyLease {
    verified: TestVerifiedStorage,
    active: Arc<AtomicBool>,
}

impl Drop for ObservedReadyLease {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
    }
}

impl ReadyLease for ObservedReadyLease {
    fn verified(&self) -> &dyn VerifiedExecutionStorage {
        &self.verified
    }
}

#[derive(Debug)]
struct TestVerifiedStorage {
    root: PathBuf,
    repository: PathBuf,
}

#[derive(Debug)]
struct TestReadyLease(TestVerifiedStorage);

impl ReadyLease for TestReadyLease {
    fn verified(&self) -> &dyn VerifiedExecutionStorage {
        &self.0
    }
}

impl VerifiedExecutionStorage for TestVerifiedStorage {
    fn repository_path(&self) -> &Path {
        &self.repository
    }
    fn verify(&self) -> Result<(), StorageError> {
        let root = fs::canonicalize(&self.root)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
        let repository = fs::canonicalize(&self.repository)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
        if repository.parent() != Some(root.as_path()) {
            return Err(StorageError::IdentityMismatch(
                "repository escaped execution root".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_directory(&self, path: &Path) -> Result<(), StorageError> {
        self.verify()?;
        let root = fs::canonicalize(&self.root)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
        let directory = fs::canonicalize(path)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
        if !directory.starts_with(&root) || directory == root {
            return Err(StorageError::IdentityMismatch(
                "bind directory escaped execution root".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ReadyAllocationVerifier for TestReadyVerifier {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        let repository = receipt.mount_path.join("repository");
        let verified = TestVerifiedStorage {
            root: receipt.mount_path.clone(),
            repository,
        };
        verified.verify()?;
        Ok(Box::new(verified))
    }

    fn acquire_ready_lease(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn ReadyLease>, StorageError> {
        let repository = receipt.mount_path.join("repository");
        let verified = TestVerifiedStorage {
            root: receipt.mount_path.clone(),
            repository,
        };
        verified.verify()?;
        Ok(Box::new(TestReadyLease(verified)))
    }
}

impl ReadyAllocationVerifier for RejectReadyAfterContainerCreate {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        if Command::new(&self.docker)
            .args(["container", "inspect", &self.container])
            .output()
            .map_err(|error| StorageError::Command(error.to_string()))?
            .status
            .success()
        {
            return Err(StorageError::IdentityMismatch(
                "allocation transitioned away from Ready after container creation".to_owned(),
            ));
        }
        TestReadyVerifier.verify_ready(receipt)
    }

    fn acquire_ready_lease(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn ReadyLease>, StorageError> {
        TestReadyVerifier.acquire_ready_lease(receipt)
    }
}

impl ReadyAllocationVerifier for ObserveProvisionLease {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        let exists = Command::new(&self.docker)
            .args(["container", "inspect", &self.agent_container])
            .output()
            .map_err(|error| StorageError::Command(error.to_string()))?
            .status
            .success();
        if exists {
            self.observed_after_create.store(true, Ordering::SeqCst);
            if !self.active.load(Ordering::SeqCst) {
                return Err(StorageError::IdentityMismatch(
                    "Ready lease was dropped before workload start".to_owned(),
                ));
            }
        }
        TestReadyVerifier.verify_ready(receipt)
    }

    fn acquire_ready_lease(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn ReadyLease>, StorageError> {
        self.active.store(true, Ordering::SeqCst);
        Ok(Box::new(ObservedReadyLease {
            verified: TestVerifiedStorage {
                root: receipt.mount_path.clone(),
                repository: receipt.mount_path.join("repository"),
            },
            active: Arc::clone(&self.active),
        }))
    }
}

#[derive(Debug)]
struct DockerCliBindVerifier {
    docker: PathBuf,
    labels: OwnershipLabels,
    daemon_id: String,
    verifier_image_id: String,
}

impl DockerBindVerifier for DockerCliBindVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        &self.daemon_id
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: self.daemon_id.clone(),
            verifier: "docker-cli-stat".to_owned(),
            method_version: format!(
                "autospec.dev/docker-bind-stat/v2;image={};command=/bin/stat",
                self.verifier_image_id
            ),
        })
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        let canonical = source.canonicalize().map_err(|error| {
            StorageError::Unavailable(format!("canonicalize Docker bind source: {error}"))
        })?;
        let mount = format!("type=bind,src={},dst=/proof,readonly", canonical.display());
        let name = format!("autospec-{}-storage-proof", self.labels.execution_id);
        let mut command = Command::new(&self.docker);
        command.args([
            "run",
            "--rm",
            "--name",
            &name,
            "--network",
            "none",
            "--read-only",
        ]);
        for (key, value) in self.labels.to_map() {
            command.args(["--label", &format!("{key}={value}")]);
        }
        let output = command
            .args([
                "--mount",
                &mount,
                &self.verifier_image_id,
                "/bin/stat",
                "-c",
                "%d:%i",
                "/proof",
            ])
            .output()
            .map_err(|error| StorageError::Command(format!("run Docker bind stat: {error}")))?;
        if !output.status.success() {
            return Err(StorageError::Command(format!(
                "Docker bind stat failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let filesystem_id = String::from_utf8(output.stdout)
            .map_err(|error| {
                StorageError::IdentityMismatch(format!("Docker stat is not UTF-8: {error}"))
            })?
            .trim()
            .to_owned();
        if !filesystem_id.contains(':') {
            return Err(StorageError::IdentityMismatch(
                "Docker stat lacks device/inode identity".to_owned(),
            ));
        }
        Ok(DockerBindProof {
            daemon_id: self.daemon_id.clone(),
            verifier: "docker-cli-stat".to_owned(),
            method_version: format!(
                "autospec.dev/docker-bind-stat/v2;image={};command=/bin/stat",
                self.verifier_image_id
            ),
            source_path: canonical,
            filesystem_id,
        })
    }
}

fn docker_cli() -> Option<PathBuf> {
    env::var_os("AUTOSPEC_DOCKER_BIN")
        .map(PathBuf::from)
        .or_else(|| {
            [
                "/usr/local/bin/docker",
                "/opt/homebrew/bin/docker",
                "/usr/bin/docker",
            ]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.is_file())
        })
}

fn configured_storage_backend(
    configured: String,
    runner: Arc<dyn CommandRunner>,
) -> Box<dyn StorageBackend> {
    #[cfg(target_os = "macos")]
    return Box::new(ApfsBackend::new(configured, runner).expect("configured APFS backend"));
    #[cfg(target_os = "linux")]
    return Box::new(LvmBackend::new(configured, runner).expect("configured LVM backend"));
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (configured, runner);
        panic!("configured execution storage is unsupported on this platform")
    }
}

impl Drop for TestStateRoot {
    fn drop(&mut self) {
        let is_owned_test_path = self.path.parent() == Some(env::temp_dir().as_path())
            && self
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("autospec-runtime-docker-state-"));
        if is_owned_test_path {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

struct StorageReceiptGuard {
    manager: Arc<ExecutionStorage>,
    receipt: Option<AllocationReceipt>,
}

impl StorageReceiptGuard {
    fn new(manager: Arc<ExecutionStorage>, receipt: AllocationReceipt) -> Self {
        Self {
            manager,
            receipt: Some(receipt),
        }
    }

    fn receipt(&self) -> &AllocationReceipt {
        self.receipt.as_ref().expect("live storage receipt")
    }

    fn release(&mut self) {
        let receipt = self.receipt.as_ref().expect("live storage receipt");
        self.manager
            .release(receipt)
            .expect("release configured execution storage");
        self.receipt = None;
    }
}

impl Drop for StorageReceiptGuard {
    fn drop(&mut self) {
        if let Some(receipt) = self.receipt.take() {
            let _ = self.manager.release(&receipt);
        }
    }
}

struct DockerImageGuard {
    docker: Docker,
    names: Vec<String>,
    cleaned: bool,
}

impl DockerImageGuard {
    fn new(docker: &Docker) -> Self {
        Self {
            docker: docker.clone(),
            names: Vec::new(),
            cleaned: false,
        }
    }

    fn track(&mut self, name: String) {
        self.names.push(name);
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        cleanup_test_images(&self.docker, &self.names).await?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for DockerImageGuard {
    fn drop(&mut self) {
        if self.cleaned || self.names.is_empty() {
            return;
        }
        let docker = self.docker.clone();
        let names = self.names.clone();
        let cleanup_thread = std::thread::Builder::new()
            .name("runtime-docker-image-cleanup".to_owned())
            .spawn(move || {
                let tokio_runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("create image-cleanup Tokio runtime: {error}"))?;
                tokio_runtime.block_on(cleanup_test_images(&docker, &names))
            });
        let cleanup = match cleanup_thread {
            Ok(thread) => thread.join().map_err(thread_panic_message),
            Err(error) => {
                write_cleanup_diagnostic(&format!("create image-cleanup thread: {error}"));
                return;
            }
        };
        match cleanup {
            Ok(Ok(())) => {}
            Ok(Err(error)) | Err(error) => write_cleanup_diagnostic(&error),
        }
    }
}

async fn cleanup_test_images(docker: &Docker, names: &[String]) -> Result<(), String> {
    let mut errors = Vec::new();
    for name in names {
        if let Err(error) = docker
            .remove_image(
                name,
                Some(RemoveImageOptions {
                    force: true,
                    noprune: false,
                }),
                None,
            )
            .await
        {
            errors.push(format!("remove test image {name}: {error}"));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn volume_names(docker: &Docker) -> BTreeSet<String> {
    docker
        .list_volumes::<String>(None)
        .await
        .expect("list Docker volumes")
        .volumes
        .unwrap_or_default()
        .into_iter()
        .map(|volume| volume.name)
        .collect()
}

fn runtime_requirement() -> RuntimeRequirement {
    RuntimeRequirement {
        image: Some("alpine:3.20".to_owned()),
        cpu: 2,
        memory_mib: 384,
        disk_gib: 3,
        ..RuntimeRequirement::default()
    }
}

fn labels_for(execution_id: ExecutionId) -> OwnershipLabels {
    OwnershipLabels {
        execution_id,
        worker_id: WorkerId::new("docker-test-worker"),
        repository: "InferWeave/autospec-orchestrator".to_owned(),
        issue: Some("task-2".to_owned()),
    }
}

fn unique_execution_id() -> ExecutionId {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after Unix epoch")
        .as_nanos();
    let sequence = EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    ExecutionId::new(format!(
        "runtime-docker-{}-{nonce}-{sequence}",
        std::process::id()
    ))
}

fn control_label_map(labels: &OwnershipLabels) -> HashMap<String, String> {
    control_labels_for(labels).to_map().into_iter().collect()
}

fn control_labels_for(labels: &OwnershipLabels) -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new(format!("{}-control", labels.execution_id)),
        worker_id: labels.worker_id.clone(),
        repository: labels.repository.clone(),
        issue: labels.issue.clone(),
    }
}

struct DockerTestScope {
    runtime: DockerRuntime,
    labels: OwnershipLabels,
    cleaned: bool,
}

impl DockerTestScope {
    fn new(runtime: &DockerRuntime, labels: &OwnershipLabels) -> Self {
        Self {
            runtime: runtime.clone(),
            labels: labels.clone(),
            cleaned: false,
        }
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        cleanup_test_resources(&self.runtime, &self.labels).await?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for DockerTestScope {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        let runtime = self.runtime.clone();
        let labels = self.labels.clone();
        let cleanup_thread = std::thread::Builder::new()
            .name("runtime-docker-test-cleanup".to_owned())
            .spawn(move || {
                let tokio_runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("create cleanup Tokio runtime: {error}"))?;
                tokio_runtime.block_on(cleanup_test_resources(&runtime, &labels))
            });
        let cleanup = match cleanup_thread {
            Ok(thread) => thread.join().map_err(thread_panic_message),
            Err(error) => {
                write_cleanup_diagnostic(&format!("create cleanup thread: {error}"));
                return;
            }
        };
        match cleanup {
            Ok(Ok(())) => {}
            Ok(Err(error)) => write_cleanup_diagnostic(&error),
            Err(error) => write_cleanup_diagnostic(&error),
        }
    }
}

fn thread_panic_message(payload: Box<dyn Any + Send>) -> String {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    format!("cleanup thread panicked: {message}")
}

fn write_cleanup_diagnostic(message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(b"Docker test cleanup failed: ");
    let _ = stderr.write_all(message.as_bytes());
    let _ = stderr.write_all(b"\n");
}

async fn cleanup_test_resources(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
) -> Result<(), String> {
    let control_labels = control_labels_for(labels);
    let mut errors = Vec::new();
    for (purpose, scoped_labels) in [("control", &control_labels), ("execution", labels)] {
        if let Err(error) = runtime.destroy(scoped_labels).await {
            errors.push(format!(
                "{purpose} execution_id={}: {error}",
                scoped_labels.execution_id
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[test]
fn execution_ids_are_scoped_to_the_test_process() {
    let execution_id = unique_execution_id();

    assert!(execution_id
        .as_str()
        .contains(&format!("-{}-", std::process::id())));
}

#[test]
fn control_resources_use_only_distinct_contract_ownership_labels() {
    let execution_labels = labels_for(unique_execution_id());
    let support_labels = control_label_map(&execution_labels);
    let expected_keys = BTreeSet::from([
        labels::MANAGED.to_owned(),
        labels::EXECUTION_ID.to_owned(),
        labels::WORKER_ID.to_owned(),
        labels::REPOSITORY.to_owned(),
        labels::ISSUE.to_owned(),
    ]);

    assert_eq!(
        support_labels.keys().cloned().collect::<BTreeSet<_>>(),
        expected_keys
    );
    assert_ne!(
        support_labels.get(labels::EXECUTION_ID),
        Some(&execution_labels.execution_id.to_string())
    );
}

#[test]
fn unwind_cleanup_uses_only_fallible_thread_and_diagnostic_operations() {
    let source = include_str!("docker_runtime.rs");
    let scope_implementation = source
        .split_once("struct DockerTestScope")
        .expect("scope has a Drop implementation")
        .1
        .split_once("#[test]\nfn execution_ids_are_scoped_to_the_test_process")
        .expect("scope helpers precede tests")
        .0;
    let drop_implementation = scope_implementation
        .split_once("impl Drop for DockerTestScope")
        .expect("scope has a Drop implementation")
        .1
        .split_once("fn thread_panic_message")
        .expect("diagnostic helpers follow Drop")
        .0;

    assert!(!drop_implementation.contains("std::thread::spawn("));
    assert!(!scope_implementation.contains(concat!("eprint", "ln!")));
    assert!(!drop_implementation.contains(".expect("));
    assert!(!drop_implementation.contains(".unwrap("));
    assert!(!drop_implementation.contains("panic!("));
    assert!(drop_implementation.contains("std::thread::Builder::new()"));
    assert!(drop_implementation.contains("write_cleanup_diagnostic"));
    assert!(scope_implementation.contains("stderr.write_all"));
    for diagnostic in [
        "create cleanup thread",
        "create cleanup Tokio runtime",
        "cleanup thread panicked",
    ] {
        assert!(scope_implementation.contains(diagnostic));
    }
}

fn raw_client() -> Result<Docker, bollard::errors::Error> {
    match env::var("AUTOSPEC_DOCKER_SOCKET") {
        Ok(socket) => Docker::connect_with_socket(&socket, 120, bollard::API_DEFAULT_VERSION),
        Err(_) => Docker::connect_with_local_defaults(),
    }
}

async fn exec_output(docker: &Docker, container: &str, command: &str) -> (i64, String, String) {
    let exec = docker
        .create_exec(
            container,
            CreateExecOptions {
                cmd: Some(vec!["sh".to_owned(), "-c".to_owned(), command.to_owned()]),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("create quota probe exec");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    match docker
        .start_exec(&exec.id, None)
        .await
        .expect("start quota probe exec")
    {
        StartExecResults::Attached { mut output, .. } => {
            while let Some(item) = output.next().await {
                match item.expect("read quota probe output") {
                    LogOutput::StdOut { message } | LogOutput::Console { message } => {
                        stdout.extend_from_slice(&message)
                    }
                    LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                    _ => {}
                }
            }
        }
        StartExecResults::Detached => panic!("quota probe unexpectedly detached"),
    }
    let exit = docker
        .inspect_exec(&exec.id)
        .await
        .expect("inspect quota probe exec")
        .exit_code
        .expect("quota probe has exit code");
    (
        exit,
        String::from_utf8(stdout).expect("quota stdout UTF-8"),
        String::from_utf8(stderr).expect("quota stderr UTF-8"),
    )
}

async fn runtime_or_skip(test_name: &str) -> Option<DockerRuntime> {
    let runtime = match DockerRuntime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            println!("SKIP {test_name}: Docker daemon unavailable: {error}");
            return None;
        }
    };
    let daemon =
        raw_client().expect("runtime connection and raw test connection use the same socket");
    if let Err(error) = daemon.version().await {
        println!("SKIP {test_name}: Docker daemon unavailable: {error}");
        return None;
    }
    assert!(
        runtime.available().await,
        "Docker daemon is present but below the runtime's required API version"
    );
    Some(runtime)
}

async fn runtime_at_state_root_or_skip(
    test_name: &str,
    state_root: &Path,
    labels: &OwnershipLabels,
    disk_gib: u64,
) -> Option<DockerRuntime> {
    let state = TestStateRoot {
        path: state_root.to_path_buf(),
    };
    let daemon =
        raw_client().expect("runtime connection and raw test connection use the same socket");
    if let Err(error) = daemon.version().await {
        println!("SKIP {test_name}: Docker daemon unavailable: {error}");
        return None;
    }
    let daemon_id = daemon
        .info()
        .await
        .expect("read probed daemon identity")
        .id
        .expect("Docker daemon reports an identity");
    ensure_alpine_image(&daemon).await;
    let verifier_image_id = daemon
        .inspect_image("alpine:3.20")
        .await
        .expect("inspect trusted verifier image")
        .id
        .expect("trusted verifier image has immutable ID");
    let trusted_verifier = TrustedVerifierImage::new(&verifier_image_id, "/bin/stat")
        .expect("trusted verifier configuration");
    let mut receipt = state.receipt(labels, disk_gib);
    receipt.docker_bind = DockerCliBindVerifier {
        docker: docker_cli().expect("real Docker tests require Docker CLI bind verification"),
        labels: labels.clone(),
        daemon_id,
        verifier_image_id,
    }
    .verify(&receipt.mount_path)
    .expect("derive immutable execution filesystem identity from the Docker daemon");
    assert_eq!(
        receipt.docker_bind.method_version,
        trusted_verifier.proof_method()
    );
    std::mem::forget(state);
    let runtime = match DockerRuntime::connect_with_verified_execution_storage(
        None,
        Arc::new(TestReadyVerifier),
        receipt,
        trusted_verifier,
    ) {
        Ok(runtime) => runtime,
        Err(error) => {
            println!("SKIP {test_name}: Docker daemon unavailable: {error}");
            return None;
        }
    };
    assert!(
        runtime.available().await,
        "Docker daemon is present but below the runtime's required API version"
    );
    Some(runtime)
}

async fn execution_runtime_or_skip(
    test_name: &str,
    labels: &OwnershipLabels,
) -> Option<(TestStateRoot, DockerRuntime)> {
    let state = TestStateRoot::new(labels);
    let runtime = runtime_at_state_root_or_skip(
        test_name,
        &state.path,
        labels,
        runtime_requirement().disk_gib,
    )
    .await?;
    Some((state, runtime))
}

async fn execution_runtime_with_disk_or_skip(
    test_name: &str,
    labels: &OwnershipLabels,
    disk_gib: u64,
) -> Option<(TestStateRoot, DockerRuntime)> {
    let state = TestStateRoot::new(labels);
    let runtime = runtime_at_state_root_or_skip(test_name, &state.path, labels, disk_gib).await?;
    Some((state, runtime))
}

async fn ensure_alpine_image(docker: &Docker) {
    match docker.inspect_image("alpine:3.20").await {
        Ok(_) => return,
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => {}
        Err(error) => panic!("inspect Alpine test image: {error}"),
    }
    let mut pull = docker.create_image(
        Some(CreateImageOptions {
            from_image: "alpine:3.20",
            ..Default::default()
        }),
        None,
        None,
    );
    while let Some(progress) = pull.next().await {
        progress.expect("pull Alpine test image");
    }
}

async fn commit_volume_image(
    docker: &Docker,
    source_container: &str,
    labels: &OwnershipLabels,
    target: &str,
    index: usize,
) -> String {
    let repository = format!("autospec-reserved-volume-{}", labels.execution_id);
    let tag = format!("case-{index}");
    let image = format!("{repository}:{tag}");
    docker
        .commit_container(
            CommitContainerOptions {
                container: source_container,
                repo: repository.as_str(),
                tag: tag.as_str(),
                pause: false,
                ..Default::default()
            },
            Config::<String> {
                labels: Some(labels.to_map().into_iter().collect()),
                volumes: Some(HashMap::from([(target.to_owned(), HashMap::new())])),
                ..Default::default()
            },
        )
        .await
        .expect("commit real image with reserved VOLUME metadata");
    let inspect = docker
        .inspect_image(&image)
        .await
        .expect("inspect committed VOLUME image");
    assert!(inspect
        .config
        .and_then(|config| config.volumes)
        .is_some_and(|volumes| volumes.contains_key(target)));
    image
}

#[test]
fn host_limits_enforce_compute_limits_and_disable_the_writable_layer() {
    let limits = host_limits(&runtime_requirement());

    assert_eq!(limits.nano_cpus, Some(2_000_000_000));
    assert_eq!(limits.memory, Some(384 * 1024 * 1024));
    assert_eq!(limits.memory_swap, Some(384 * 1024 * 1024));
    assert_eq!(limits.pids_limit, Some(DEFAULT_PIDS_LIMIT));
    assert_eq!(limits.storage_opt, None);
    assert_eq!(limits.readonly_rootfs, Some(true));
    assert_eq!(
        limits
            .log_config
            .as_ref()
            .and_then(|config| config.typ.as_deref()),
        Some("none")
    );
    assert_eq!(limits.publish_all_ports, Some(false));
    assert_eq!(limits.privileged, Some(false));
}

#[tokio::test]
async fn daemon_probe_reports_a_compatible_real_daemon() {
    let Some(runtime) = runtime_or_skip("daemon_probe_reports_a_compatible_real_daemon").await
    else {
        return;
    };

    assert_eq!(runtime.name(), "docker");
    let daemon_version = raw_client()
        .expect("connect to probed daemon")
        .version()
        .await
        .expect("read daemon version")
        .api_version
        .expect("daemon reports API version");
    let mut expected = daemon_version
        .split_once('.')
        .map(|(major, minor)| {
            (
                major.parse::<usize>().expect("numeric daemon major"),
                minor.parse::<usize>().expect("numeric daemon minor"),
            )
        })
        .expect("daemon API is major.minor");
    expected = expected.min((
        bollard::API_DEFAULT_VERSION.major_version,
        bollard::API_DEFAULT_VERSION.minor_version,
    ));
    assert_eq!(runtime.client_api_version(), expected);
}

#[test]
fn cleanup_constructor_requires_exact_persisted_labels_and_directory() {
    let labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&labels);
    let receipt = state.receipt(&labels, runtime_requirement().disk_gib);
    let mut foreign_labels = labels.clone();
    foreign_labels.worker_id = WorkerId::new("foreign-worker");
    let error = DockerRuntime::connect_for_cleanup(None, receipt.clone(), &foreign_labels)
        .expect_err("cleanup labels must exactly match the persisted receipt")
        .to_string();
    assert!(error.contains("ownership labels"));

    fs::remove_dir_all(&receipt.mount_path).unwrap();
    fs::write(&receipt.mount_path, "not a directory").unwrap();
    let error = DockerRuntime::connect_for_cleanup(None, receipt, &labels)
        .expect_err("cleanup authority must name an execution directory")
        .to_string();
    assert!(error.contains("not an owned directory"));
}

#[tokio::test]
async fn cleanup_constructor_rejects_foreign_daemon_before_exact_resource_removal() {
    let labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&labels);
    let Some(legacy) =
        runtime_or_skip("cleanup_constructor_rejects_foreign_daemon_before_exact_resource_removal")
            .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let network = DockerRuntime::network_name(&labels.execution_id);
    docker
        .create_network(CreateNetworkOptions {
            name: network.clone(),
            check_duplicate: true,
            driver: "bridge".to_owned(),
            internal: false,
            attachable: false,
            ingress: false,
            ipam: Default::default(),
            enable_ipv6: false,
            options: HashMap::new(),
            labels: labels.to_map().into_iter().collect(),
        })
        .await
        .expect("create exact cleanup target");
    let mut scope = DockerTestScope::new(&legacy, &labels);
    let mut receipt = state.receipt(&labels, runtime_requirement().disk_gib);
    receipt.mount_path = receipt.mount_path.canonicalize().unwrap();
    receipt.docker_bind.source_path = receipt.mount_path.clone();
    receipt.docker_bind.daemon_id = "definitely-not-this-daemon".to_owned();
    let cleanup = DockerRuntime::connect_for_cleanup(None, receipt, &labels)
        .expect("construct persisted cleanup authority");

    let error = cleanup
        .destroy(&labels)
        .await
        .expect_err("foreign daemon receipt must fail closed")
        .to_string();
    assert!(error.contains("does not match the persisted cleanup receipt"));
    docker
        .inspect_network::<String>(&network, None)
        .await
        .expect("daemon mismatch preserves the target");

    scope.cleanup().await.expect("remove exact test target");
}

#[tokio::test]
async fn legacy_runtime_and_foreign_daemon_receipts_fail_before_docker_resources() {
    let labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&labels);
    let Some(legacy) =
        runtime_or_skip("legacy_runtime_and_foreign_daemon_receipts_fail_before_docker_resources")
            .await
    else {
        return;
    };
    let error = legacy
        .provision(&labels, &runtime_requirement(), &[])
        .await
        .expect_err("legacy provisioning must fail closed");
    assert!(
        matches!(error, runtime_traits::RuntimeError::ResourceLimit(message)
        if message.contains("Ready execution-storage verifier"))
    );

    let mut receipt = state.receipt(&labels, runtime_requirement().disk_gib);
    receipt.docker_bind.daemon_id = "definitely-not-this-daemon".to_owned();
    let foreign =
        DockerRuntime::connect_with_execution_storage(None, Arc::new(TestReadyVerifier), receipt)
            .expect("connect runtime with foreign receipt");
    let mut scope = DockerTestScope::new(&foreign, &labels);
    let error = foreign
        .provision(&labels, &runtime_requirement(), &[])
        .await
        .expect_err("foreign Docker proof must fail closed");
    assert!(
        matches!(error, runtime_traits::RuntimeError::ResourceLimit(message)
        if message.contains("Docker daemon identity"))
    );

    let docker = raw_client().expect("connect to probed daemon");
    assert!(docker
        .inspect_network::<String>(&DockerRuntime::network_name(&labels.execution_id), None)
        .await
        .is_err());
    scope
        .cleanup()
        .await
        .expect("cleanup foreign-proof test scope");
}

#[tokio::test]
async fn real_image_volume_collisions_fail_before_resources_and_preserve_another_execution() {
    let execution_labels = labels_for(unique_execution_id());
    let healthy_labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&execution_labels);
    state.add_execution(&healthy_labels);
    let Some(runtime) = runtime_at_state_root_or_skip(
        "real_image_volume_collisions_fail_before_resources_and_preserve_another_execution",
        &state.path,
        &execution_labels,
        runtime_requirement().disk_gib,
    )
    .await
    else {
        return;
    };
    let healthy_runtime = runtime_at_state_root_or_skip(
        "healthy execution for collision isolation",
        &state.path,
        &healthy_labels,
        runtime_requirement().disk_gib,
    )
    .await
    .expect("same Docker daemon remains available");
    let docker = raw_client().expect("connect to probed daemon");
    ensure_alpine_image(&docker).await;
    let mut execution_scope = DockerTestScope::new(&runtime, &execution_labels);
    let mut healthy_scope = DockerTestScope::new(&healthy_runtime, &healthy_labels);
    let control_labels = control_labels_for(&execution_labels);
    let source_container = format!("autospec-{}-image-source", control_labels.execution_id);
    docker
        .create_container(
            Some(CreateContainerOptions {
                name: source_container.clone(),
                platform: None,
            }),
            Config::<String> {
                image: Some("alpine:3.20".to_owned()),
                labels: Some(control_labels.to_map().into_iter().collect()),
                ..Default::default()
            },
        )
        .await
        .expect("create labelled image source container");

    let mut image_guard = DockerImageGuard::new(&docker);
    let mut collision_images = Vec::new();
    for (index, target) in ["/workspace", "/session/history", "/"]
        .into_iter()
        .enumerate()
    {
        let image =
            commit_volume_image(&docker, &source_container, &execution_labels, target, index).await;
        image_guard.track(image.clone());
        collision_images.push((target, image));
    }
    runtime
        .destroy(&control_labels)
        .await
        .expect("remove labelled image source container");

    let healthy = healthy_runtime
        .provision(&healthy_labels, &runtime_requirement(), &[])
        .await
        .expect("provision independent healthy execution");
    for (target, image) in collision_images {
        let requirement = RuntimeRequirement {
            image: Some(image),
            ..runtime_requirement()
        };
        let error = runtime
            .provision(&execution_labels, &requirement, &[])
            .await
            .expect_err("reserved image VOLUME must fail closed");
        assert!(
            matches!(error, runtime_traits::RuntimeError::ResourceLimit(ref message)
                if message.contains(target) && message.contains("reserved agent mount")),
            "unexpected collision error: {error}"
        );
        assert!(docker
            .inspect_network::<String>(
                &DockerRuntime::network_name(&execution_labels.execution_id),
                None,
            )
            .await
            .is_err());
        let containers = docker
            .list_containers(Some(bollard::container::ListContainersOptions {
                all: true,
                filters: HashMap::from([("label".to_owned(), execution_labels.selector())]),
                ..Default::default()
            }))
            .await
            .expect("list rejected execution containers");
        assert!(containers.is_empty());
        let healthy_inspect = docker
            .inspect_container(&healthy.agent_container, None)
            .await
            .expect("independent execution survives rejected provisioning");
        assert_eq!(
            healthy_inspect.state.and_then(|state| state.running),
            Some(true)
        );
    }

    execution_scope
        .cleanup()
        .await
        .expect("cleanup rejected execution selector");
    healthy_scope
        .cleanup()
        .await
        .expect("cleanup healthy execution");
    image_guard
        .cleanup()
        .await
        .expect("cleanup committed images");
}

#[tokio::test]
async fn ready_transition_blocks_malicious_workload_entrypoint_before_marker_write() {
    let Some(docker_bin) = docker_cli() else {
        println!("SKIP malicious pre-start gate: Docker CLI is absent");
        return;
    };
    let labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&labels);
    let docker = raw_client().expect("connect Docker");
    if let Err(error) = docker.version().await {
        println!("SKIP malicious pre-start gate: Docker daemon unavailable: {error}");
        return;
    }
    ensure_alpine_image(&docker).await;
    let daemon_id = docker
        .info()
        .await
        .expect("daemon info")
        .id
        .expect("daemon ID");
    let verifier_image_id = docker
        .inspect_image("alpine:3.20")
        .await
        .expect("trusted image")
        .id
        .expect("trusted image ID");
    let trusted =
        TrustedVerifierImage::new(&verifier_image_id, "/bin/stat").expect("trusted verifier");
    let mut receipt = state.receipt(&labels, runtime_requirement().disk_gib);
    receipt.docker_bind = DockerCliBindVerifier {
        docker: docker_bin.clone(),
        labels: labels.clone(),
        daemon_id,
        verifier_image_id,
    }
    .verify(&receipt.mount_path)
    .expect("derive daemon-side execution filesystem identity");
    let agent_name = DockerRuntime::agent_container_name(&labels.execution_id);
    let runtime = DockerRuntime::connect_with_verified_execution_storage(
        None,
        Arc::new(RejectReadyAfterContainerCreate {
            docker: docker_bin,
            container: agent_name,
        }),
        receipt,
        trusted,
    )
    .expect("storage-backed runtime");
    let mut scope = DockerTestScope::new(&runtime, &labels);

    let source = format!("autospec-{}-malicious-source", labels.execution_id);
    docker
        .create_container(
            Some(CreateContainerOptions {
                name: source.clone(),
                platform: None,
            }),
            Config::<String> {
                image: Some("alpine:3.20".to_owned()),
                labels: Some(control_label_map(&labels)),
                ..Default::default()
            },
        )
        .await
        .expect("create malicious image source");
    let repository = format!("autospec-malicious-entrypoint-{}", labels.execution_id);
    let image = format!("{repository}:test");
    docker
        .commit_container(
            CommitContainerOptions {
                container: source.as_str(),
                repo: repository.as_str(),
                tag: "test",
                pause: false,
                ..Default::default()
            },
            Config::<String> {
                labels: Some(labels.to_map().into_iter().collect()),
                entrypoint: Some(vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    "printf started > /workspace/workload-entrypoint-started; exec /bin/sleep infinity"
                        .to_owned(),
                ]),
                ..Default::default()
            },
        )
        .await
        .expect("commit malicious workload image");
    docker
        .remove_container(
            &source,
            Some(bollard::container::RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await
        .expect("remove malicious image source");
    let mut image_guard = DockerImageGuard::new(&docker);
    image_guard.track(image.clone());

    let requirement = RuntimeRequirement {
        image: Some(image),
        ..runtime_requirement()
    };
    let error = runtime
        .provision(&labels, &requirement, &[])
        .await
        .expect_err("Ready transition must stop workload startup");
    assert!(error.to_string().contains("transitioned away from Ready"));
    assert!(
        !state
            .worktree(&labels)
            .join("workload-entrypoint-started")
            .exists(),
        "malicious workload entrypoint ran before the trusted gate"
    );

    scope
        .cleanup()
        .await
        .expect("cleanup malicious gate resources");
    image_guard
        .cleanup()
        .await
        .expect("remove malicious workload image");
}

#[tokio::test]
async fn ready_lease_remains_held_through_the_final_workload_start_gate() {
    let Some(docker_bin) = docker_cli() else {
        println!("SKIP Ready lease start gate: Docker CLI is absent");
        return;
    };
    let labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&labels);
    let docker = raw_client().expect("connect Docker");
    if let Err(error) = docker.version().await {
        println!("SKIP Ready lease start gate: Docker daemon unavailable: {error}");
        return;
    }
    ensure_alpine_image(&docker).await;
    let daemon_id = docker
        .info()
        .await
        .expect("daemon info")
        .id
        .expect("daemon ID");
    let verifier_image_id = docker
        .inspect_image("alpine:3.20")
        .await
        .expect("trusted image")
        .id
        .expect("trusted image ID");
    let trusted =
        TrustedVerifierImage::new(&verifier_image_id, "/bin/stat").expect("trusted verifier");
    let mut receipt = state.receipt(&labels, runtime_requirement().disk_gib);
    receipt.docker_bind = DockerCliBindVerifier {
        docker: docker_bin.clone(),
        labels: labels.clone(),
        daemon_id,
        verifier_image_id,
    }
    .verify(&receipt.mount_path)
    .expect("derive daemon-side execution filesystem identity");
    let active = Arc::new(AtomicBool::new(false));
    let observed_after_create = Arc::new(AtomicBool::new(false));
    let runtime = DockerRuntime::connect_with_verified_execution_storage(
        None,
        Arc::new(ObserveProvisionLease {
            docker: docker_bin,
            agent_container: DockerRuntime::agent_container_name(&labels.execution_id),
            active: Arc::clone(&active),
            observed_after_create: Arc::clone(&observed_after_create),
        }),
        receipt,
        trusted,
    )
    .expect("storage-backed runtime");
    let mut scope = DockerTestScope::new(&runtime, &labels);

    runtime
        .provision(&labels, &runtime_requirement(), &[])
        .await
        .expect("provision while retaining Ready lease");
    assert!(observed_after_create.load(Ordering::SeqCst));
    assert!(
        !active.load(Ordering::SeqCst),
        "provisioning must drop its Ready lease after the final start succeeds"
    );

    scope.cleanup().await.expect("cleanup lease-gated runtime");
}

#[tokio::test]
async fn verifier_image_volume_is_rejected_without_creating_anonymous_volumes() {
    let labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&labels);
    let docker = raw_client().expect("connect Docker");
    if let Err(error) = docker.version().await {
        println!("SKIP verifier image VOLUME rejection: Docker daemon unavailable: {error}");
        return;
    }
    ensure_alpine_image(&docker).await;
    let daemon_id = docker
        .info()
        .await
        .expect("daemon info")
        .id
        .expect("daemon ID");
    let source = format!("autospec-{}-verifier-volume-source", labels.execution_id);
    docker
        .create_container(
            Some(CreateContainerOptions {
                name: source.clone(),
                platform: None,
            }),
            Config::<String> {
                image: Some("alpine:3.20".to_owned()),
                labels: Some(control_label_map(&labels)),
                ..Default::default()
            },
        )
        .await
        .expect("create verifier image source");
    let image = commit_volume_image(&docker, &source, &labels, "/proof-cache", 0).await;
    docker
        .remove_container(
            &source,
            Some(bollard::container::RemoveContainerOptions {
                force: true,
                v: false,
                ..Default::default()
            }),
        )
        .await
        .expect("remove verifier image source");
    let verifier_image_id = docker
        .inspect_image(&image)
        .await
        .expect("inspect verifier VOLUME image")
        .id
        .expect("verifier image ID");
    let trusted =
        TrustedVerifierImage::new(&verifier_image_id, "/bin/stat").expect("trusted verifier");
    let mut receipt = state.receipt(&labels, runtime_requirement().disk_gib);
    receipt.docker_bind.daemon_id = daemon_id;
    receipt.docker_bind.method_version = trusted.proof_method();
    let runtime = DockerRuntime::connect_with_verified_execution_storage(
        None,
        Arc::new(TestReadyVerifier),
        receipt,
        trusted,
    )
    .expect("storage-backed runtime");
    let mut scope = DockerTestScope::new(&runtime, &labels);
    let mut image_guard = DockerImageGuard::new(&docker);
    image_guard.track(image);
    let volumes_before = volume_names(&docker).await;

    let error = runtime
        .provision(&labels, &runtime_requirement(), &[])
        .await
        .expect_err("trusted verifier VOLUME must fail before Docker resources");
    assert!(error.to_string().contains("declares writable volumes"));
    let volumes_after = volume_names(&docker).await;
    let new_anonymous = volumes_after
        .difference(&volumes_before)
        .filter(|name| name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .collect::<Vec<_>>();
    assert!(
        new_anonymous.is_empty(),
        "verifier rejection leaked anonymous volumes: {new_anonymous:?}"
    );
    assert!(docker
        .list_containers(Some(bollard::container::ListContainersOptions {
            all: true,
            filters: HashMap::from([("label".to_owned(), labels.selector())]),
            ..Default::default()
        }))
        .await
        .expect("list rejected verifier execution")
        .is_empty());

    scope.cleanup().await.expect("cleanup rejected execution");
    image_guard.cleanup().await.expect("cleanup verifier image");
}

#[tokio::test]
async fn agent_mounts_only_writable_worktree_and_durable_conversation() {
    let execution_labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&execution_labels);
    let Some(runtime) = runtime_at_state_root_or_skip(
        "agent_mounts_only_writable_worktree_and_durable_conversation",
        &state.path,
        &execution_labels,
        runtime_requirement().disk_gib,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let handle = runtime
        .provision(&execution_labels, &runtime_requirement(), &[])
        .await
        .expect("provision mounted agent");
    let inspect = docker
        .inspect_container(&handle.agent_container, None)
        .await
        .expect("inspect mounted agent");
    let capability = &handle.verified_agent_container;
    assert_eq!(capability.container_id, inspect.id.clone().unwrap());
    assert_eq!(capability.labels, execution_labels);
    assert_eq!(
        capability.daemon_id,
        docker.info().await.unwrap().id.unwrap()
    );
    assert!(capability.mounts.iter().any(|mount| {
        mount.target == "/workspace"
            && mount.source == fs::canonicalize(state.worktree(&execution_labels)).unwrap()
            && mount.writable
    }));
    assert!(capability.mounts.iter().any(|mount| {
        mount.target == "/session"
            && mount.source
                == fs::canonicalize(state.session(&execution_labels).join("conversation")).unwrap()
            && mount.writable
    }));
    let mounts = inspect
        .host_config
        .expect("agent host config")
        .mounts
        .expect("agent bind mounts");
    let actual_mounts = mounts
        .iter()
        .map(|mount| {
            (
                mount.target.clone().expect("mount target"),
                mount.source.clone().expect("mount source"),
                mount.typ,
            )
        })
        .collect::<BTreeSet<_>>();
    assert!(mounts.iter().all(|mount| mount.read_only != Some(true)));
    assert!(actual_mounts.contains(&(
        "/workspace".to_owned(),
        fs::canonicalize(state.worktree(&execution_labels))
            .expect("canonical worktree")
            .display()
            .to_string(),
        Some(MountTypeEnum::BIND),
    )));
    assert!(actual_mounts.contains(&(
        "/session".to_owned(),
        fs::canonicalize(state.session(&execution_labels).join("conversation"))
            .expect("canonical conversation")
            .display()
            .to_string(),
        Some(MountTypeEnum::BIND),
    )));
    let runtime_root = fs::canonicalize(
        state
            .path
            .join("executions")
            .join(execution_labels.execution_id.as_str())
            .join("runtime"),
    )
    .expect("runtime root");
    assert!(actual_mounts
        .iter()
        .filter(|(target, _, _)| target != "/workspace" && target != "/session")
        .all(|(_, source, typ)| {
            *typ == Some(MountTypeEnum::BIND) && Path::new(source).starts_with(&runtime_root)
        }));

    let private_live_events = format!("pi.events-{}.jsonl", execution_labels.execution_id);
    let probe = docker
        .create_exec(
            &handle.agent_container,
            CreateExecOptions {
                cmd: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    format!(
                        "printf workspace > /workspace/container-write && \
                         printf conversation > /session/container-write && \
                         test ! -e /session/owner.json && \
                         test ! -e /session/.cursor && \
                         test ! -e /session/resume-count && \
                         test ! -e /session/{private_live_events} && \
                         test ! -e /var/run/docker.sock"
                    ),
                ]),
                attach_stdout: Some(false),
                attach_stderr: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("create mount isolation probe");
    docker
        .start_exec(
            &probe.id,
            Some(StartExecOptions {
                detach: true,
                ..Default::default()
            }),
        )
        .await
        .expect("start mount isolation probe");
    let mut exit_code = None;
    for _ in 0..200 {
        let inspect = docker
            .inspect_exec(&probe.id)
            .await
            .expect("inspect mount isolation probe");
        if inspect.running == Some(false) {
            exit_code = inspect.exit_code;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(exit_code, Some(0));
    assert_eq!(
        fs::read_to_string(state.worktree(&execution_labels).join("container-write"))
            .expect("read worktree write"),
        "workspace"
    );
    let conversation_write = state
        .session(&execution_labels)
        .join("conversation/container-write");
    assert_eq!(
        fs::read_to_string(&conversation_write).expect("read conversation write"),
        "conversation"
    );
    for (name, contents) in [
        ("owner.json", "host owner\n"),
        (".cursor", "host cursor\n"),
        ("resume-count", "7\n"),
        (private_live_events.as_str(), "host live events\n"),
    ] {
        assert_eq!(
            fs::read_to_string(state.session(&execution_labels).join(name))
                .expect("read private metadata"),
            contents
        );
    }

    scope.cleanup().await.expect("cleanup mounted agent");
    assert_eq!(
        fs::read_to_string(conversation_write).expect("conversation survives container cleanup"),
        "conversation"
    );
}

#[tokio::test]
async fn panicking_test_scope_removes_only_its_execution_resources() {
    let Some(runtime) =
        runtime_or_skip("panicking_test_scope_removes_only_its_execution_resources").await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let scope = DockerTestScope::new(&runtime, &execution_labels);
    let network = DockerRuntime::network_name(&execution_labels.execution_id);
    docker
        .create_network(CreateNetworkOptions {
            name: network.clone(),
            driver: "bridge".to_owned(),
            labels: execution_labels.to_map().into_iter().collect(),
            ..Default::default()
        })
        .await
        .expect("create owned network");

    let panic = tokio::spawn(async move {
        let _scope = scope;
        panic!("exercise failure cleanup");
    })
    .await;
    assert!(panic.is_err());
    let cleaned_during_unwind = docker
        .inspect_network::<String>(&network, None)
        .await
        .is_err();
    let _ = runtime.destroy(&execution_labels).await;

    assert!(
        cleaned_during_unwind,
        "a test assertion panic must trigger execution-scoped cleanup"
    );
}

#[tokio::test]
async fn failed_normal_cleanup_keeps_scope_armed() {
    let Some(runtime) = runtime_or_skip("failed_normal_cleanup_keeps_scope_armed").await else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let control_labels = control_labels_for(&execution_labels);
    let blocker_labels = [
        labels_for(ExecutionId::new(format!(
            "{}-blocker-one",
            execution_labels.execution_id
        ))),
        labels_for(ExecutionId::new(format!(
            "{}-blocker-two",
            execution_labels.execution_id
        ))),
    ];
    let volumes = [
        DockerRuntime::volume_name(&execution_labels.execution_id, "blocked"),
        DockerRuntime::volume_name(&control_labels.execution_id, "blocked"),
    ];
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let mut blocker_scopes = blocker_labels
        .iter()
        .map(|labels| DockerTestScope::new(&runtime, labels))
        .collect::<Vec<_>>();

    for ((volume, volume_labels), blocker_labels) in volumes
        .iter()
        .zip([&execution_labels, &control_labels])
        .zip(&blocker_labels)
    {
        docker
            .create_volume(CreateVolumeOptions {
                name: volume.clone(),
                driver: "local".to_owned(),
                labels: volume_labels.to_map().into_iter().collect(),
                ..Default::default()
            })
            .await
            .expect("create selector-owned blocked volume");
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name: format!("{volume}-holder"),
                    platform: None,
                }),
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    labels: Some(blocker_labels.to_map().into_iter().collect()),
                    host_config: Some(HostConfig {
                        mounts: Some(vec![Mount {
                            target: Some("/held".to_owned()),
                            source: Some(volume.clone()),
                            typ: Some(MountTypeEnum::VOLUME),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("create validly-owned blocking container");
    }

    let cleanup_error = scope
        .cleanup()
        .await
        .expect_err("both blocked selectors must report cleanup failures");
    let incorrectly_marked_clean = scope.cleaned;
    for blocker_scope in &mut blocker_scopes {
        blocker_scope
            .cleanup()
            .await
            .expect("cleanup blocker resources by their selector");
    }
    scope
        .cleanup()
        .await
        .expect("cleanup succeeds after blockers are removed");

    assert!(cleanup_error.contains(&volumes[0]));
    assert!(cleanup_error.contains(&volumes[1]));
    assert!(
        !incorrectly_marked_clean,
        "cleanup failures must leave the unwind guard armed"
    );
}

#[tokio::test]
async fn frozen_conformance_requires_docker_and_proves_lifecycle_limits_storage_and_targeted_cleanup(
) {
    let execution_labels = labels_for(unique_execution_id());
    let (state, runtime) = execution_runtime_or_skip(
        "frozen_conformance_requires_docker_and_proves_lifecycle_limits_storage_and_targeted_cleanup",
        &execution_labels,
    )
    .await
    .expect("Docker and its immutable verifier image are required for frozen conformance");
    let docker = raw_client().expect("the already-probed Docker daemon remains connectable");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::from([("SERVICE_MODE".to_owned(), "test".to_owned())]),
    };
    let unrelated_network = format!("unrelated-{}", execution_labels.execution_id);
    let unrelated_container = format!("unrelated-{}", execution_labels.execution_id);
    let unrelated_volume = format!("unrelated-{}", execution_labels.execution_id);
    docker
        .create_network(CreateNetworkOptions {
            name: unrelated_network.clone(),
            driver: "bridge".to_owned(),
            labels: control_label_map(&execution_labels),
            ..Default::default()
        })
        .await
        .expect("create unrelated control network");

    let result = async {
        let handle = runtime
            .provision(&execution_labels, &runtime_requirement(), &[service])
            .await?;
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name: unrelated_container.clone(),
                    platform: None,
                }),
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    labels: Some(control_label_map(&execution_labels)),
                    ..Default::default()
                },
            )
            .await
            .expect("create unrelated control container");
        docker
            .create_volume(CreateVolumeOptions {
                name: unrelated_volume.clone(),
                driver: "local".to_owned(),
                labels: control_label_map(&execution_labels),
                ..Default::default()
            })
            .await
            .expect("create unrelated control volume");
        assert_eq!(
            handle.network,
            format!("autospec-{}", execution_labels.execution_id)
        );
        assert_eq!(
            handle.agent_container,
            format!("autospec-{}-agent", execution_labels.execution_id)
        );
        assert_eq!(
            handle.service_containers,
            vec![format!("autospec-{}-cache", execution_labels.execution_id)]
        );
        assert!(handle.volumes.is_empty());
        assert!(handle.credentials_path.is_none());

        let network = docker
            .inspect_network::<String>(&handle.network, None)
            .await
            .expect("inspect provisioned network");
        let actual_network_labels = network.labels.expect("network has labels");
        for (key, value) in execution_labels.to_map() {
            assert_eq!(actual_network_labels.get(&key), Some(&value));
        }

        for container_name in
            std::iter::once(&handle.agent_container).chain(handle.service_containers.iter())
        {
            let container = docker
                .inspect_container(container_name, None)
                .await
                .expect("inspect provisioned container");
            let config = container.config.expect("container has config");
            let actual_labels = config.labels.expect("container has labels");
            for (key, value) in execution_labels.to_map() {
                assert_eq!(actual_labels.get(&key), Some(&value));
            }
            let host = container.host_config.expect("container has host config");
            assert_eq!(host.nano_cpus, Some(2_000_000_000));
            assert_eq!(host.memory, Some(384 * 1024 * 1024));
            assert_eq!(host.memory_swap, Some(384 * 1024 * 1024));
            assert_eq!(host.pids_limit, Some(DEFAULT_PIDS_LIMIT));
            assert_eq!(host.storage_opt, None);
            assert_eq!(host.readonly_rootfs, Some(true));
            assert_eq!(
                host.log_config
                    .as_ref()
                    .and_then(|config| config.typ.as_deref()),
                Some("none")
            );
            assert!(host
                .mounts
                .as_ref()
                .is_some_and(|mounts| mounts.iter().all(|mount| {
                    mount.typ == Some(MountTypeEnum::BIND) && mount.tmpfs_options.is_none()
                })));
            assert!(host.port_bindings.as_ref().is_none_or(HashMap::is_empty));
            assert_eq!(host.publish_all_ports, Some(false));
            assert_eq!(host.network_mode.as_deref(), Some(handle.network.as_str()));
            assert_eq!(
                container.state.and_then(|state| state.running),
                Some(true),
                "provisioned containers must be running"
            );
        }

        let service_container = docker
            .inspect_container(&handle.service_containers[0], None)
            .await
            .expect("inspect service network alias");
        let aliases = service_container
            .network_settings
            .and_then(|settings| settings.networks)
            .and_then(|networks| networks.get(&handle.network).cloned())
            .and_then(|endpoint| endpoint.aliases)
            .unwrap_or_default();
        assert!(aliases.iter().any(|alias| alias == "cache"));
        let service_mounts = service_container.mounts.unwrap_or_default();
        let data_mount = service_mounts
            .iter()
            .find(|mount| mount.destination.as_deref() == Some("/data"))
            .expect("Redis image-declared /data volume is overridden");
        assert_eq!(
            data_mount.typ,
            Some(bollard::models::MountPointTypeEnum::BIND)
        );
        let runtime_root = fs::canonicalize(
            state
                .path
                .join("executions")
                .join(execution_labels.execution_id.as_str())
                .join("runtime"),
        )
        .expect("canonical execution runtime");
        assert!(data_mount
            .source
            .as_ref()
            .is_some_and(|source| { Path::new(source).starts_with(&runtime_root) }));
        assert!(data_mount.name.is_none());
        assert!(service_mounts
            .iter()
            .all(|mount| mount.typ != Some(bollard::models::MountPointTypeEnum::VOLUME)));

        assert!(!runtime
            .reconcile(std::slice::from_ref(&execution_labels.execution_id))
            .await?
            .contains(&execution_labels.execution_id));
        let orphans = runtime.reconcile(&[]).await?;
        assert!(orphans.contains(&execution_labels.execution_id));
        docker
            .inspect_network::<String>(&handle.network, None)
            .await
            .expect("reconciliation reports but does not delete the network");
        docker
            .inspect_container(&handle.agent_container, None)
            .await
            .expect("reconciliation reports but does not delete containers");

        runtime.destroy(&execution_labels).await?;
        assert!(docker
            .inspect_network::<String>(&handle.network, None)
            .await
            .is_err());
        for container_name in
            std::iter::once(&handle.agent_container).chain(handle.service_containers.iter())
        {
            assert!(docker
                .inspect_container(container_name, None)
                .await
                .is_err());
        }
        docker
            .inspect_network::<String>(&unrelated_network, None)
            .await
            .expect("selector cleanup preserves unrelated Docker resources");
        docker
            .inspect_container(&unrelated_container, None)
            .await
            .expect("selector cleanup preserves unrelated containers");
        docker
            .inspect_volume(&unrelated_volume)
            .await
            .expect("selector cleanup preserves unrelated volumes");

        Ok::<(), runtime_traits::RuntimeError>(())
    }
    .await;

    scope.cleanup().await.expect("cleanup lifecycle resources");
    result.expect("real Docker lifecycle succeeds");

    for scoped_labels in [
        execution_labels.clone(),
        control_labels_for(&execution_labels),
    ] {
        let filters = HashMap::from([("label".to_owned(), scoped_labels.selector())]);
        assert!(
            docker
                .list_containers(Some(bollard::container::ListContainersOptions {
                    all: true,
                    filters: filters.clone(),
                    ..Default::default()
                }))
                .await
                .expect("list exact conformance containers")
                .is_empty(),
            "conformance containers must not leak"
        );
        assert!(
            docker
                .list_networks(Some(bollard::network::ListNetworksOptions {
                    filters: filters.clone(),
                }))
                .await
                .expect("list exact conformance networks")
                .is_empty(),
            "conformance networks must not leak"
        );
        assert!(
            docker
                .list_volumes(Some(bollard::volume::ListVolumesOptions { filters }))
                .await
                .expect("list exact conformance volumes")
                .volumes
                .unwrap_or_default()
                .is_empty(),
            "conformance volumes must not leak"
        );
    }
}

#[tokio::test]
async fn image_volumes_are_bind_backed_and_container_roots_are_read_only() {
    let execution_labels = labels_for(unique_execution_id());
    let Some((state, runtime)) = execution_runtime_with_disk_or_skip(
        "image_volumes_are_bind_backed_and_container_roots_are_read_only",
        &execution_labels,
        1,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let requirement = RuntimeRequirement {
        image: Some("alpine:3.20".to_owned()),
        cpu: 1,
        memory_mib: 2048,
        disk_gib: 1,
        ..RuntimeRequirement::default()
    };
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::new(),
    };
    let handle = runtime
        .provision(&execution_labels, &requirement, &[service])
        .await
        .expect("provision storage-backed service");
    let agent_inspect = docker
        .inspect_container(&handle.agent_container, None)
        .await
        .expect("inspect bounded agent");
    let service_inspect = docker
        .inspect_container(&handle.service_containers[0], None)
        .await
        .expect("inspect bounded service");
    let hosts = [
        agent_inspect.host_config.expect("agent host config"),
        service_inspect.host_config.expect("service host config"),
    ];
    for host in &hosts {
        assert_eq!(host.readonly_rootfs, Some(true));
        assert_eq!(
            host.log_config.as_ref().and_then(|log| log.typ.as_deref()),
            Some("none")
        );
        assert!(host.storage_opt.is_none());
        assert!(host.tmpfs.is_none());
        assert!(host
            .mounts
            .as_ref()
            .expect("writable bind mounts")
            .iter()
            .all(|mount| mount.typ == Some(MountTypeEnum::BIND)));
    }

    let host_mount = hosts[1]
        .mounts
        .clone()
        .unwrap_or_default()
        .into_iter()
        .find(|mount| mount.target.as_deref() == Some("/data"))
        .expect("storage-backed /data mount exists");
    assert_eq!(host_mount.typ, Some(MountTypeEnum::BIND));
    let runtime_root = fs::canonicalize(
        state
            .path
            .join("executions")
            .join(execution_labels.execution_id.as_str())
            .join("runtime"),
    )
    .expect("canonical runtime root");
    let data_source = fs::canonicalize(host_mount.source.expect("/data bind source"))
        .expect("canonical /data bind source");
    assert!(data_source.starts_with(&runtime_root));

    for (container, command) in [
        (
            handle.agent_container.as_str(),
            "printf home > /home/autospec/durable; printf tmp > /tmp/durable; \
             printf vartmp > /var/tmp/durable; printf run > /run/durable",
        ),
        (
            handle.service_containers[0].as_str(),
            "printf home > /home/autospec/durable; printf tmp > /tmp/durable; \
             printf vartmp > /var/tmp/durable; printf run > /run/durable; \
             printf data > /data/durable",
        ),
    ] {
        let (exit, _, error) = exec_output(&docker, container, command).await;
        assert_eq!(exit, 0, "allowed bind write failed: {error}");
        let (exit, _, _) = exec_output(&docker, container, "touch /etc/root-write-must-fail").await;
        assert_ne!(exit, 0, "container root unexpectedly remained writable");
    }

    let durable_files = hosts
        .iter()
        .flat_map(|host| host.mounts.as_ref().into_iter().flatten())
        .filter_map(|mount| {
            let target = mount.target.as_deref()?;
            let expected = match target {
                "/home/autospec" => "home",
                "/tmp" => "tmp",
                "/var/tmp" => "vartmp",
                "/run" => "run",
                "/data" => "data",
                _ => return None,
            };
            Some((
                PathBuf::from(mount.source.as_deref().expect("bind source")).join("durable"),
                expected,
            ))
        })
        .collect::<Vec<_>>();
    for (path, expected) in &durable_files {
        assert_eq!(
            fs::read_to_string(path).expect("read durable bind write"),
            *expected
        );
    }

    let exec = docker
        .create_exec(
            &handle.service_containers[0],
            CreateExecOptions {
                cmd: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "touch /etc/root-write-must-fail".to_owned(),
                ]),
                attach_stdout: Some(false),
                attach_stderr: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("create read-only root probe");
    docker
        .start_exec(
            &exec.id,
            Some(StartExecOptions {
                detach: true,
                ..Default::default()
            }),
        )
        .await
        .expect("start read-only root probe");
    let mut exit_code = None;
    for _ in 0..300 {
        let inspect = docker
            .inspect_exec(&exec.id)
            .await
            .expect("inspect read-only root probe");
        if inspect.running == Some(false) {
            exit_code = inspect.exit_code;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    scope
        .cleanup()
        .await
        .expect("cleanup storage-backed resources");
    for (path, expected) in durable_files {
        assert_eq!(
            fs::read_to_string(path).expect("bind write survives container cleanup"),
            expected
        );
    }
    assert!(exit_code.is_some_and(|code| code != 0));
    assert!(docker
        .inspect_container(&handle.service_containers[0], None)
        .await
        .is_err());
}

#[tokio::test]
async fn configured_storage_enforces_one_aggregate_quota_and_preserves_another_execution() {
    #[cfg(target_os = "macos")]
    let configured = env::var("AUTOSPEC_APFS_PROBE_PATH").ok();
    #[cfg(target_os = "linux")]
    let configured = env::var("AUTOSPEC_LVM_VOLUME_GROUP").ok();
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let configured: Option<String> = None;
    let Some(configured) = configured else {
        println!("SKIP configured Docker aggregate quota: operator storage pool is absent");
        return;
    };
    let Some(docker_bin) = docker_cli() else {
        println!("SKIP configured Docker aggregate quota: Docker CLI is absent");
        return;
    };
    let daemon = Command::new(&docker_bin)
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .expect("run Docker info");
    if !daemon.status.success() {
        println!("SKIP configured Docker aggregate quota: Docker daemon is absent");
        return;
    }
    let daemon_id = String::from_utf8(daemon.stdout)
        .expect("Docker daemon id UTF-8")
        .trim()
        .to_owned();
    let docker = raw_client().expect("connect to configured Docker daemon");
    ensure_alpine_image(&docker).await;
    let verifier_image_id = docker
        .inspect_image("alpine:3.20")
        .await
        .expect("inspect trusted verifier image")
        .id
        .expect("trusted verifier image ID");

    let state = tempfile::tempdir().expect("configured storage state root");
    secure_mode(state.path());
    for directory in ["execution-storage", "executions"] {
        let path = state.path().join(directory);
        fs::create_dir(&path).expect("create storage control directory");
        secure_mode(&path);
    }
    let first_labels = labels_for(unique_execution_id());
    let second_labels = labels_for(unique_execution_id());
    let make_manager = |labels: &OwnershipLabels| {
        Arc::new(
            ExecutionStorage::new(
                state.path(),
                configured_storage_backend(configured.clone(), Arc::new(ProcessCommandRunner)),
                Box::new(DockerCliBindVerifier {
                    docker: docker_bin.clone(),
                    labels: labels.clone(),
                    daemon_id: daemon_id.clone(),
                    verifier_image_id: verifier_image_id.clone(),
                }),
            )
            .expect("configured execution storage manager"),
        )
    };
    let first_manager = make_manager(&first_labels);
    let second_manager = make_manager(&second_labels);
    let mut first_storage = StorageReceiptGuard::new(
        first_manager.clone(),
        first_manager
            .allocate(&AllocationRequest {
                labels: first_labels.clone(),
                disk_gib: 1,
            })
            .expect("allocate first hard-bounded execution"),
    );
    let mut second_storage = StorageReceiptGuard::new(
        second_manager.clone(),
        second_manager
            .allocate(&AllocationRequest {
                labels: second_labels.clone(),
                disk_gib: 1,
            })
            .expect("allocate isolated control execution"),
    );
    let first_runtime = DockerRuntime::connect_with_verified_execution_storage(
        None,
        first_manager,
        first_storage.receipt().clone(),
        TrustedVerifierImage::new(&verifier_image_id, "/bin/stat").expect("trusted verifier"),
    )
    .expect("connect first storage-backed runtime");
    let second_runtime = DockerRuntime::connect_with_verified_execution_storage(
        None,
        second_manager,
        second_storage.receipt().clone(),
        TrustedVerifierImage::new(&verifier_image_id, "/bin/stat").expect("trusted verifier"),
    )
    .expect("connect second storage-backed runtime");
    let mut first_scope = DockerTestScope::new(&first_runtime, &first_labels);
    let mut second_scope = DockerTestScope::new(&second_runtime, &second_labels);
    let requirement = RuntimeRequirement {
        image: Some("alpine:3.20".to_owned()),
        cpu: 1,
        memory_mib: 2048,
        disk_gib: 1,
        ..RuntimeRequirement::default()
    };
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::new(),
    };
    let control = second_runtime
        .provision(&second_labels, &requirement, &[])
        .await
        .expect("provision isolated control execution");
    let bounded = first_runtime
        .provision(&first_labels, &requirement, &[service])
        .await
        .expect("provision aggregate-bounded agent and service");

    let mut successful = 0_u64;
    let mut quota_failure = None;
    for index in 0..20 {
        let (container, path) = if index % 2 == 0 {
            (&bounded.agent_container, format!("/workspace/fill-{index}"))
        } else {
            (
                &bounded.service_containers[0],
                format!("/data/fill-{index}"),
            )
        };
        let (exit, _stdout, stderr) = exec_output(
            &docker,
            container,
            &format!("dd if=/dev/zero of={path} bs=1M count=64 conv=fsync"),
        )
        .await;
        if exit == 0 {
            successful += 64 * 1024 * 1024;
        } else {
            quota_failure = Some(stderr);
            break;
        }
    }
    assert!(
        successful >= 256 * 1024 * 1024,
        "quota failed before substantial aggregate writes: {successful}"
    );
    assert!(
        quota_failure
            .as_deref()
            .is_some_and(|error| error.contains("No space left on device")),
        "aggregate fill did not fail with ENOSPC: {quota_failure:?}"
    );
    let (exit, _, error) = exec_output(
        &docker,
        &control.agent_container,
        "printf isolated > /workspace/after-peer-enospc",
    )
    .await;
    assert_eq!(exit, 0, "control execution write failed: {error}");

    first_scope
        .cleanup()
        .await
        .expect("cleanup bounded Docker resources");
    second_scope
        .cleanup()
        .await
        .expect("cleanup control Docker resources");
    first_storage.release();
    second_storage.release();
}

#[tokio::test]
async fn cleanup_aggregates_volume_failures_and_still_removes_the_network() {
    let Some(runtime) =
        runtime_or_skip("cleanup_aggregates_volume_failures_and_still_removes_the_network").await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let network = DockerRuntime::network_name(&execution_labels.execution_id);
    docker
        .create_network(CreateNetworkOptions {
            name: network.clone(),
            driver: "bridge".to_owned(),
            labels: execution_labels.to_map().into_iter().collect(),
            ..Default::default()
        })
        .await
        .expect("create owned network");

    let volumes = [
        DockerRuntime::volume_name(&execution_labels.execution_id, "held-one"),
        DockerRuntime::volume_name(&execution_labels.execution_id, "held-two"),
    ];
    let holders = [
        format!("{network}-holder-one"),
        format!("{network}-holder-two"),
    ];
    for (volume, holder) in volumes.iter().zip(&holders) {
        docker
            .create_volume(CreateVolumeOptions {
                name: volume.clone(),
                driver: "local".to_owned(),
                labels: execution_labels.to_map().into_iter().collect(),
                ..Default::default()
            })
            .await
            .expect("create held owned volume");
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name: holder.clone(),
                    platform: None,
                }),
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    cmd: Some(vec!["sleep".to_owned(), "infinity".to_owned()]),
                    labels: Some(control_label_map(&execution_labels)),
                    host_config: Some(HostConfig {
                        mounts: Some(vec![Mount {
                            target: Some("/held".to_owned()),
                            source: Some(volume.clone()),
                            typ: Some(MountTypeEnum::VOLUME),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("create unrelated holder container");
    }

    let error = runtime
        .destroy(&execution_labels)
        .await
        .expect_err("in-use volumes make scoped cleanup report failure")
        .to_string();
    assert!(error.contains(&volumes[0]));
    assert!(error.contains(&volumes[1]));
    assert!(docker
        .inspect_network::<String>(&network, None)
        .await
        .is_err());

    scope
        .cleanup()
        .await
        .expect("cleanup aggregate-failure resources");
}

#[tokio::test]
async fn provisioning_failure_reports_rollback_failure_and_leaks_no_anonymous_volume() {
    let execution_labels = labels_for(unique_execution_id());
    let Some((_state, runtime)) = execution_runtime_or_skip(
        "provisioning_failure_reports_rollback_failure_and_leaks_no_anonymous_volume",
        &execution_labels,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let owned_volume = DockerRuntime::volume_name(&execution_labels.execution_id, "cache-data");
    let holder = format!("autospec-{}-holder", execution_labels.execution_id);
    let conflicting_agent = DockerRuntime::agent_container_name(&execution_labels.execution_id);
    docker
        .create_volume(CreateVolumeOptions {
            name: owned_volume.clone(),
            driver: "local".to_owned(),
            labels: execution_labels.to_map().into_iter().collect(),
            ..Default::default()
        })
        .await
        .expect("create held execution volume");
    for (name, mounts) in [
        (
            holder.clone(),
            Some(vec![Mount {
                target: Some("/held".to_owned()),
                source: Some(owned_volume.clone()),
                typ: Some(MountTypeEnum::VOLUME),
                ..Default::default()
            }]),
        ),
        (conflicting_agent.clone(), None),
    ] {
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name,
                    platform: None,
                }),
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    cmd: Some(vec!["sleep".to_owned(), "infinity".to_owned()]),
                    labels: Some(control_label_map(&execution_labels)),
                    host_config: Some(HostConfig {
                        mounts,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("create failure-injection container");
    }
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::new(),
    };
    let volumes_before_failure = volume_names(&docker).await;

    let error = runtime
        .provision(&execution_labels, &runtime_requirement(), &[service])
        .await
        .expect_err("agent name conflict triggers provisioning rollback")
        .to_string();
    let volumes_after_failure = volume_names(&docker).await;
    assert!(error.contains(execution_labels.execution_id.as_str()));
    assert!(error.contains("create agent container"));
    assert!(error.contains("rollback"));
    assert!(error.contains(&owned_volume));
    let new_anonymous_volumes = volumes_after_failure
        .difference(&volumes_before_failure)
        .filter(|name| {
            name.len() == 64 && name.chars().all(|character| character.is_ascii_hexdigit())
        })
        .collect::<Vec<_>>();
    assert!(
        new_anonymous_volumes.is_empty(),
        "failed provisioning must not leave anonymous image volumes: {new_anonymous_volumes:?}"
    );
    assert!(docker
        .inspect_network::<String>(
            &DockerRuntime::network_name(&execution_labels.execution_id),
            None,
        )
        .await
        .is_err());

    let managed_mounts = docker
        .list_containers(Some(bollard::container::ListContainersOptions {
            all: true,
            filters: HashMap::from([("label".to_owned(), execution_labels.selector())]),
            ..Default::default()
        }))
        .await
        .expect("list execution-labelled containers");
    assert!(
        managed_mounts.is_empty(),
        "rollback removes service containers"
    );

    scope
        .cleanup()
        .await
        .expect("cleanup rollback-failure resources");
}
