use execution_storage::{
    disk_gib_to_bytes, AllocationPhase, AllocationReceipt, AllocationRequest, BackendCapability,
    BackendIdentity, BackendState, DockerBindCapability, DockerBindProof, DockerBindVerifier,
    ExecutionLayout, ExecutionLifecycleHold, ExecutionLifecycleHoldStore, ExecutionStorage,
    ExecutionStorageManager, JournalStore, PhaseJournal, ReadyAllocationVerifier, ReadyLease,
    ReleasePhase, SecureMetadataDirectory, StorageBackend, StorageError, ALLOCATION_API_VERSION,
};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use std::{
    fs,
    path::Path,
    process::Command,
    sync::{Arc, Mutex},
};

#[cfg(unix)]
fn mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("set secure mode");
}

fn storage_directories(root: &Path) {
    fs::create_dir(root.join("execution-storage")).expect("journal directory");
    fs::create_dir(root.join("executions")).expect("execution directory");
    #[cfg(unix)]
    {
        mode(root, 0o700);
        mode(&root.join("execution-storage"), 0o700);
        mode(&root.join("executions"), 0o700);
    }
}

#[test]
fn secure_metadata_directory_never_deletes_an_unauthenticated_temporary_file() {
    let root = tempfile::tempdir().expect("temporary metadata root");
    #[cfg(unix)]
    mode(root.path(), 0o700);
    let metadata = SecureMetadataDirectory::new(root.path()).expect("secure metadata directory");

    fs::write(root.path().join("intent.json.tmp"), b"{\"partial\":")
        .expect("simulate interrupted temporary write");
    #[cfg(unix)]
    mode(&root.path().join("intent.json.tmp"), 0o600);

    assert_eq!(
        metadata.read("intent.json").expect("reconcile intent"),
        None
    );
    assert_eq!(
        fs::read(root.path().join("intent.json.tmp")).expect("attacker temporary is preserved"),
        b"{\"partial\":"
    );
}

#[test]
fn journal_store_ignores_but_preserves_an_unauthenticated_orphan_temporary_file() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let state_root = root.path().canonicalize().expect("canonical state root");
    let temporary = state_root.join("execution-storage/.node-417.json.123.7.tmp");
    fs::write(&temporary, b"{\"phase\":").expect("write interrupted journal temporary");
    #[cfg(unix)]
    mode(&temporary, 0o600);
    let store = JournalStore::new(&state_root).expect("journal store");

    assert!(store
        .list(&state_root)
        .expect("reconcile journals")
        .is_empty());
    assert_eq!(
        fs::read(&temporary).expect("unverified temporary is preserved"),
        b"{\"phase\":"
    );
}

#[test]
fn secure_metadata_directory_owns_exact_temporary_subdirectories() {
    let root = tempfile::tempdir().expect("temporary metadata root");
    #[cfg(unix)]
    mode(root.path(), 0o700);
    let metadata = SecureMetadataDirectory::new(root.path()).expect("secure metadata directory");

    let capture = metadata
        .create_subdirectory("capture-node-417")
        .expect("create pinned capture directory");
    capture
        .create("index", b"trusted index")
        .expect("copy index");
    assert_eq!(
        capture.read("index").expect("read copied index"),
        Some(b"trusted index".to_vec())
    );
    capture.remove("index").expect("remove copied index");
    metadata
        .remove_subdirectory("capture-node-417", &capture)
        .expect("remove exact pinned capture directory");

    assert!(!root.path().join("capture-node-417").exists());
}

fn labels() -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new("node-417-impl-01"),
        worker_id: WorkerId::new("buildbox-02"),
        repository: "InferWeave/autospec-orchestrator".to_owned(),
        issue: Some("417".to_owned()),
    }
}

fn apfs_identity(volume: &str, volume_uuid: &str, token: &str) -> BackendIdentity {
    BackendIdentity::Apfs {
        container: "disk3".to_owned(),
        container_uuid: "POOL-UUID".to_owned(),
        volume: volume.to_owned(),
        volume_name: format!("autospec-{token}"),
        volume_uuid: volume_uuid.to_owned(),
        ownership_token: token.to_owned(),
    }
}

fn receipt() -> AllocationReceipt {
    let root = std::env::temp_dir().join("autospec-storage-contract");
    AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.to_owned(),
        labels: labels(),
        reserved_bytes: disk_gib_to_bytes(1).expect("bytes"),
        mount_path: root.clone(),
        backend_kind: "apfs".to_owned(),
        backend_key: "apfs:node-417-impl-01".to_owned(),
        pool_identity: "POOL-UUID".to_owned(),
        backend: apfs_identity("disk9s1", "FS-7", "token-7"),
        docker_bind: DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            verifier: "docker-bind-probe".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
            source_path: root,
            filesystem_id: "FS-7".to_owned(),
        },
    }
}

#[test]
fn layout_is_deterministic_and_never_accepts_path_components() {
    let root = tempfile::tempdir().expect("temporary state root");
    let layout = ExecutionLayout::new(root.path(), &ExecutionId::new("node-417-impl-01"))
        .expect("valid execution layout");

    assert_eq!(layout.root, root.path().join("executions/node-417-impl-01"));
    assert_eq!(layout.repository, layout.root.join("repository"));
    assert_eq!(layout.session, layout.root.join("session"));
    assert_eq!(layout.conversation, layout.session.join("conversation"));
    assert_eq!(layout.credentials, layout.root.join("credentials"));
    assert_eq!(layout.runtime, layout.root.join("runtime"));
    assert_eq!(
        layout.journal,
        root.path().join("execution-storage/node-417-impl-01.json")
    );

    for invalid in ["", "../escape", "Upper", "under_score"] {
        assert!(ExecutionLayout::new(root.path(), &ExecutionId::new(invalid)).is_err());
    }
}

#[test]
fn gib_conversion_is_checked_and_exact() {
    assert_eq!(disk_gib_to_bytes(1).expect("one GiB"), 1_073_741_824);
    assert_eq!(disk_gib_to_bytes(3).expect("three GiB"), 3_221_225_472);
    assert!(disk_gib_to_bytes(0).is_err());
    assert!(disk_gib_to_bytes(u64::MAX).is_err());
}

#[test]
fn docker_bind_contract_versions_the_verifier_and_proof_method() {
    let capability = DockerBindCapability {
        daemon_id: "daemon-7".to_owned(),
        verifier: "docker-bind-probe".to_owned(),
        method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
    };
    let proof = DockerBindProof {
        daemon_id: capability.daemon_id.clone(),
        verifier: capability.verifier.clone(),
        method_version: capability.method_version.clone(),
        source_path: Path::new("/canonical/execution").to_path_buf(),
        filesystem_id: "FS-7".to_owned(),
    };

    assert_eq!(proof.verifier, capability.verifier);
    assert_eq!(proof.method_version, capability.method_version);
}

#[derive(Debug)]
struct DockerCliBindVerifier {
    docker: std::path::PathBuf,
    daemon_id: String,
}

impl DockerBindVerifier for DockerCliBindVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        &self.daemon_id
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: self.daemon_id.clone(),
            verifier: "docker-cli-stat".to_owned(),
            method_version: "autospec.dev/docker-bind-stat/v1;command=/bin/stat".to_owned(),
        })
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        let canonical = source.canonicalize().map_err(|error| {
            StorageError::Unavailable(format!("canonicalize Docker bind source: {error}"))
        })?;
        let mount = format!("type=bind,src={},dst=/proof,readonly", canonical.display());
        let mut command = Command::new(&self.docker);
        command.args(["run", "--rm", "--network", "none", "--read-only"]);
        for (key, value) in labels().to_map() {
            command.args(["--label", &format!("{key}={value}")]);
        }
        let output = command
            .args([
                "--mount",
                &mount,
                "alpine:3.20",
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
        if filesystem_id.is_empty() || !filesystem_id.contains(':') {
            return Err(StorageError::IdentityMismatch(
                "Docker stat lacks device/inode identity".to_owned(),
            ));
        }
        Ok(DockerBindProof {
            daemon_id: self.daemon_id.clone(),
            verifier: "docker-cli-stat".to_owned(),
            method_version: "autospec.dev/docker-bind-stat/v1;command=/bin/stat".to_owned(),
            source_path: canonical,
            filesystem_id,
        })
    }
}

#[test]
fn real_docker_bind_verifier_contract_is_versioned_when_available() {
    let docker = std::env::var_os("AUTOSPEC_DOCKER_BIN")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            [
                "/usr/local/bin/docker",
                "/opt/homebrew/bin/docker",
                "/usr/bin/docker",
            ]
            .into_iter()
            .map(std::path::PathBuf::from)
            .find(|path| path.is_file())
        });
    let Some(docker) = docker else {
        println!("SKIP real Docker bind verifier contract: Docker CLI is absent");
        return;
    };
    let daemon = Command::new(&docker)
        .args(["info", "--format", "{{.ID}}"])
        .output()
        .expect("run Docker info");
    if !daemon.status.success() {
        println!("SKIP real Docker bind verifier contract: Docker daemon is absent");
        return;
    }
    let image = Command::new(&docker)
        .args(["image", "inspect", "alpine:3.20"])
        .output()
        .expect("inspect verifier image");
    if !image.status.success() {
        println!("SKIP real Docker bind verifier contract: alpine:3.20 is absent");
        return;
    }
    let source = tempfile::tempdir().expect("bind source");
    #[cfg(unix)]
    mode(source.path(), 0o700);
    let canonical = source.path().canonicalize().expect("canonical bind source");
    let verifier = DockerCliBindVerifier {
        docker,
        daemon_id: String::from_utf8(daemon.stdout)
            .expect("daemon UTF-8")
            .trim()
            .to_owned(),
    };
    let capability = verifier.probe().expect("probe verifier");
    let proof = verifier
        .verify(&canonical)
        .expect("daemon-side bind identity");
    assert_eq!(proof.verifier, capability.verifier);
    assert_eq!(proof.method_version, capability.method_version);
    assert_eq!(proof.source_path, canonical);
    assert!(proof.filesystem_id.contains(':'));
}

#[test]
fn releasing_journal_models_each_idempotent_cleanup_subphase() {
    for release_phase in [
        ReleasePhase::Mounted,
        ReleasePhase::Unmounted,
        ReleasePhase::ObjectAbsent,
    ] {
        let journal = PhaseJournal::releasing(receipt(), release_phase);
        assert_eq!(journal.phase, AllocationPhase::Releasing);
        assert_eq!(journal.release_phase, Some(release_phase));
    }
}

#[cfg(unix)]
#[test]
fn manager_rejects_group_or_world_accessible_state_roots() {
    let root = tempfile::tempdir().expect("state");
    storage_directories(root.path());
    mode(root.path(), 0o755);
    let error = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::new(Mutex::new(Vec::new())),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect_err("unsafe state root must fail closed");
    assert!(error.to_string().contains("mode"));
}

#[cfg(unix)]
#[test]
fn manager_rejects_replaced_execution_directory_inode() {
    let (_root, manager, _calls) = manager_fixture();
    let executions = manager.state_root().join("executions");
    let displaced = manager.state_root().join("executions-displaced");
    fs::rename(&executions, &displaced).expect("displace guarded directory");
    fs::create_dir(&executions).expect("replacement directory");
    mode(&executions, 0o700);

    let error = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 1,
        })
        .expect_err("replaced inode must fail closed");
    assert!(error.to_string().contains("inode"));
    assert!(displaced.is_dir());
}

#[test]
fn journal_creation_is_exclusive_and_never_overwrites_existing_state() {
    let root = tempfile::tempdir().expect("state");
    storage_directories(root.path());
    let canonical = root.path().canonicalize().expect("canonical root");
    let layout = ExecutionLayout::new(&canonical, &labels().execution_id).expect("layout");
    let store = JournalStore::new(&canonical).expect("store");
    let first = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(1).expect("bytes"),
        layout.root.clone(),
        "fake".to_owned(),
        format!("fake:{}", labels().execution_id),
        "pool-7".to_owned(),
        "token-first".to_owned(),
    );
    let second = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(1).expect("bytes"),
        layout.root.clone(),
        "fake".to_owned(),
        "fake:key".to_owned(),
        "pool-7".to_owned(),
        "token-second".to_owned(),
    );
    store.create(&layout, &first).expect("first create");
    assert!(store.create(&layout, &second).is_err());
    assert_eq!(store.read(&layout).expect("retained first"), first);
}

#[test]
fn receipt_wire_format_is_versioned_and_preserves_exact_identity() {
    let root = tempfile::tempdir().expect("temporary state root");
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    let receipt = AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.to_owned(),
        labels: labels(),
        reserved_bytes: disk_gib_to_bytes(3).expect("bytes"),
        mount_path: layout.root.clone(),
        backend_kind: "apfs".to_owned(),
        backend_key: "apfs:node-417-impl-01".to_owned(),
        pool_identity: "POOL-UUID".to_owned(),
        backend: apfs_identity("disk9s1", "A1B2-C3D4", "token-a"),
        docker_bind: DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            verifier: "fake-docker-bind-inspector".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
            source_path: layout.root.clone(),
            filesystem_id: "A1B2-C3D4".to_owned(),
        },
    };

    let encoded = serde_json::to_vec(&receipt).expect("serialize receipt");
    let decoded: AllocationReceipt = serde_json::from_slice(&encoded).expect("deserialize receipt");
    assert_eq!(decoded, receipt);
    assert!(String::from_utf8(encoded)
        .expect("JSON is UTF-8")
        .contains(ALLOCATION_API_VERSION));
}

#[test]
fn journal_transitions_are_fsynced_and_symlinks_are_rejected() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let canonical = root.path().canonicalize().expect("canonical root");
    let layout = ExecutionLayout::new(&canonical, &labels().execution_id).expect("layout");
    let store = JournalStore::new(&canonical).expect("journal store");
    let allocating = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(3).expect("bytes"),
        layout.root.clone(),
        "apfs".to_owned(),
        "apfs:disk3:autospec-node-417-impl-01".to_owned(),
        "POOL-UUID".to_owned(),
        "token-a".to_owned(),
    );
    store
        .create(&layout, &allocating)
        .expect("create allocating");
    assert_eq!(store.read(&layout).expect("read allocating"), allocating);

    let receipt = AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.to_owned(),
        labels: labels(),
        reserved_bytes: disk_gib_to_bytes(3).expect("bytes"),
        mount_path: layout.root.clone(),
        backend_kind: "apfs".to_owned(),
        backend_key: "apfs:disk3:autospec-node-417-impl-01".to_owned(),
        pool_identity: "POOL-UUID".to_owned(),
        backend: apfs_identity("disk9s1", "A1B2-C3D4", "token-a"),
        docker_bind: DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            verifier: "fake-docker-bind-inspector".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
            source_path: layout.root.clone(),
            filesystem_id: "A1B2-C3D4".to_owned(),
        },
    };
    let ready = PhaseJournal::ready(receipt.clone());
    store.write(&layout, &ready).expect("write ready");
    assert_eq!(store.read(&layout).expect("read ready"), ready);
    let releasing = PhaseJournal::releasing(receipt, ReleasePhase::Mounted);
    store.write(&layout, &releasing).expect("write releasing");
    assert_eq!(releasing.phase, AllocationPhase::Releasing);

    store.remove(&layout).expect("remove exact journal");
    assert!(!layout.journal.exists());

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(root.path(), &layout.journal).expect("journal symlink");
        assert!(store.read(&layout).is_err());
        fs::remove_file(&layout.journal).expect("remove test symlink");
    }
}

#[test]
fn allocating_journal_can_persist_backend_identity_before_ready() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let canonical = root.path().canonicalize().expect("canonical root");
    let layout = ExecutionLayout::new(&canonical, &labels().execution_id).expect("layout");
    let store = JournalStore::new(&canonical).expect("journal store");
    let identity = apfs_identity("disk9s1", "EXEC-UUID", "token-a");
    let allocating = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(3).expect("bytes"),
        layout.root.clone(),
        "apfs".to_owned(),
        "apfs:disk3:autospec-node-417-impl-01".to_owned(),
        "POOL-UUID".to_owned(),
        "token-a".to_owned(),
    )
    .with_backend_identity(identity.clone());

    store
        .create(&layout, &allocating)
        .expect("persist allocated identity");
    assert_eq!(
        store
            .read(&layout)
            .expect("read allocating identity")
            .backend,
        Some(identity)
    );
}

#[derive(Debug)]
struct FakeBackend {
    calls: Arc<Mutex<Vec<String>>>,
    removes_mountpoint: bool,
    state: Arc<Mutex<BackendState>>,
    pool_identity: &'static str,
    mutate_prepared: bool,
}

impl StorageBackend for FakeBackend {
    fn key(&self, labels: &OwnershipLabels) -> String {
        format!("fake:{}", labels.execution_id)
    }

    fn probe(&self, required_bytes: u64) -> Result<BackendCapability, StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("probe:{required_bytes}"));
        Ok(BackendCapability {
            backend: "fake".to_owned(),
            pool_identity: self.pool_identity.to_owned(),
            reservable_bytes: required_bytes,
        })
    }

    fn discover(
        &self,
        _layout: &ExecutionLayout,
        _ownership_token: &str,
        _reserved_bytes: u64,
    ) -> Result<Option<BackendIdentity>, StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push("discover".to_owned());
        Ok(None)
    }

    fn create(
        &self,
        layout: &ExecutionLayout,
        labels: &OwnershipLabels,
        ownership_token: &str,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push("create".to_owned());
        Ok(apfs_identity(
            &format!("volume-{}", labels.execution_id),
            &format!("fs-{}-{reserved_bytes}", layout.root.display()),
            ownership_token,
        ))
    }

    fn prepare(
        &self,
        _layout: &ExecutionLayout,
        identity: &BackendIdentity,
        _reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("prepare:{}", identity.filesystem_id()));
        *self.state.lock().expect("fake state") = BackendState::Unmounted;
        let mut prepared = identity.clone();
        if self.mutate_prepared {
            if let BackendIdentity::Apfs { volume_uuid, .. } = &mut prepared {
                *volume_uuid = "mutated-filesystem".to_owned();
            }
        }
        Ok(prepared)
    }

    fn mount(
        &self,
        _layout: &ExecutionLayout,
        identity: &BackendIdentity,
        _reserved_bytes: u64,
    ) -> Result<(), StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("mount:{}", identity.filesystem_id()));
        *self.state.lock().expect("fake state") = BackendState::Mounted;
        Ok(())
    }

    fn state(
        &self,
        _layout: &ExecutionLayout,
        _identity: &BackendIdentity,
        _reserved_bytes: u64,
    ) -> Result<BackendState, StorageError> {
        Ok(*self.state.lock().expect("fake state"))
    }

    fn unmount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("unmount:{}", identity.filesystem_id()));
        if fs::symlink_metadata(&layout.root)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            *self.state.lock().expect("fake state") = BackendState::Unmounted;
            return Ok(());
        }
        for entry in fs::read_dir(&layout.root).expect("read fake mounted filesystem") {
            let path = entry.expect("fake filesystem entry").path();
            if path.is_dir() {
                fs::remove_dir_all(path).expect("remove fake filesystem directory");
            } else {
                fs::remove_file(path).expect("remove fake filesystem file");
            }
        }
        if self.removes_mountpoint {
            fs::remove_dir(&layout.root).expect("remove fake mountpoint");
        }
        *self.state.lock().expect("fake state") = BackendState::Unmounted;
        Ok(())
    }

    fn remove(&self, identity: &BackendIdentity) -> Result<(), StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("remove:{}", identity.filesystem_id()));
        *self.state.lock().expect("fake state") = BackendState::Absent;
        Ok(())
    }
}

#[derive(Debug)]
struct FakeDockerVerifier;

impl DockerBindVerifier for FakeDockerVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        "daemon-7"
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: "daemon-7".to_owned(),
            verifier: "fake-docker-bind-inspector".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
        })
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        Ok(DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            verifier: "fake-docker-bind-inspector".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
            source_path: source.to_path_buf(),
            filesystem_id: format!("daemon-stat:{}", source.display()),
        })
    }
}

#[derive(Debug)]
struct RotatedCleanupDockerVerifier {
    daemon_id: &'static str,
}

impl DockerBindVerifier for RotatedCleanupDockerVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        self.daemon_id
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: self.daemon_id.to_owned(),
            verifier: "replacement-verifier-image".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v2".to_owned(),
        })
    }

    fn verify(&self, _source: &Path) -> Result<DockerBindProof, StorageError> {
        Err(StorageError::Unavailable(
            "cleanup must not execute the replacement verifier".to_owned(),
        ))
    }
}

#[derive(Debug)]
struct FailingDockerVerifier;

impl DockerBindVerifier for FailingDockerVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        "daemon-7"
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: "daemon-7".to_owned(),
            verifier: "failing-fake-docker-bind-inspector".to_owned(),
            method_version: "autospec.dev/docker-bind-proof/v1".to_owned(),
        })
    }

    fn verify(&self, _source: &Path) -> Result<DockerBindProof, StorageError> {
        Err(StorageError::Unavailable(
            "Docker daemon cannot prove the bind source".to_owned(),
        ))
    }
}

#[derive(Debug)]
struct ChangingContractVerifier;

impl DockerBindVerifier for ChangingContractVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        "daemon-7"
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: "daemon-7".to_owned(),
            verifier: "bind-inspector".to_owned(),
            method_version: "v1".to_owned(),
        })
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        Ok(DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            verifier: "bind-inspector".to_owned(),
            method_version: "v2".to_owned(),
            source_path: source.to_path_buf(),
            filesystem_id: format!("daemon-stat:{}", source.display()),
        })
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct SymlinkSwappingDockerVerifier;

#[cfg(unix)]
impl DockerBindVerifier for SymlinkSwappingDockerVerifier {
    fn cleanup_daemon_id(&self) -> &str {
        "daemon-7"
    }

    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        FailingDockerVerifier.probe()
    }

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError> {
        fs::remove_dir_all(source).expect("remove fake mounted filesystem");
        std::os::unix::fs::symlink(source.with_extension("foreign"), source)
            .expect("replace mountpoint with dangling symlink");
        Err(StorageError::Unavailable(
            "Docker daemon cannot prove the bind source".to_owned(),
        ))
    }
}

fn manager_fixture() -> (tempfile::TempDir, ExecutionStorage, Arc<Mutex<Vec<String>>>) {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("storage manager");
    (root, manager, calls)
}

#[test]
fn release_accepts_backend_that_removes_its_mountpoint() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::new(Mutex::new(Vec::new())),
            removes_mountpoint: true,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("storage manager");
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");

    manager
        .release(&receipt)
        .expect("backend-owned mountpoint removal is successful release");
    assert!(!receipt.mount_path.exists());
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    assert!(!layout.journal.exists());
}

#[test]
fn manager_trait_is_object_safe_and_lifecycle_is_journaled() {
    let (_root, manager, calls) = manager_fixture();
    let object: &dyn ExecutionStorageManager = &manager;
    let request = AllocationRequest {
        labels: labels(),
        disk_gib: 3,
    };
    let capability = object.probe(request.disk_gib).expect("probe storage");
    assert_eq!(
        capability.backend.reservable_bytes,
        disk_gib_to_bytes(3).unwrap()
    );
    assert_eq!(capability.docker_bind.daemon_id, "daemon-7");

    let receipt = object.allocate(&request).expect("allocate storage");
    let layout = ExecutionLayout::new(manager.state_root(), &request.labels.execution_id)
        .expect("execution layout");
    assert!(layout.repository.is_dir());
    assert!(layout.conversation.is_dir());
    assert!(layout.credentials.is_dir());
    assert!(layout.runtime.is_dir());
    assert_eq!(
        JournalStore::new(manager.state_root())
            .expect("journal store")
            .read(&layout)
            .expect("ready journal")
            .phase,
        AllocationPhase::Ready
    );

    object.release(&receipt).expect("release exact allocation");
    assert!(!layout.root.exists());
    assert!(!layout.journal.exists());
    assert!(
        object.allocate(&request).is_err(),
        "release awaits durable ack"
    );
    object
        .release(&receipt)
        .expect("physical absence is authenticated by the external tombstone");
    object
        .ack_release(&receipt)
        .expect("controller disposition acknowledges physical release");
    let replacement = object.allocate(&request).expect("ack permits exact reuse");
    object.release(&replacement).expect("release replacement");
    object.ack_release(&replacement).expect("ack replacement");
    let calls = calls.lock().expect("fake calls");
    assert_eq!(calls[0], format!("probe:{}", disk_gib_to_bytes(3).unwrap()));
    assert!(calls.iter().any(|call| call == "create"));
    assert!(calls.iter().any(|call| call.starts_with("prepare:")));
    assert!(calls.iter().any(|call| call.starts_with("mount:")));
    assert!(calls.iter().any(|call| call.starts_with("remove:")));
}

#[test]
fn cleanup_releases_exact_durable_ready_allocation_after_verifier_rotation() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(BackendState::Absent));
    let original = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state: Arc::clone(&state),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("original manager");
    let receipt = original
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("original allocation");
    drop(original);

    let replacement = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state,
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(RotatedCleanupDockerVerifier {
            daemon_id: "daemon-7",
        }),
    )
    .expect("replacement manager");

    assert!(
        replacement.verify_ready(&receipt).is_err(),
        "rotated proof must remain invalid for live provisioning and adoption"
    );
    replacement
        .release(&receipt)
        .expect("durable cleanup must survive verifier image and method rotation");
    replacement
        .ack_release(&receipt)
        .expect("durable cleanup tombstone must remain acknowledgeable");
    let layout = ExecutionLayout::new(replacement.state_root(), &labels().execution_id)
        .expect("execution layout");
    assert!(!layout.root.exists());
    assert!(!layout.journal.exists());
}

#[test]
fn cleanup_rejects_rotated_configuration_for_a_different_daemon() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let state = Arc::new(Mutex::new(BackendState::Absent));
    let original = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state: Arc::clone(&state),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("original manager");
    let receipt = original
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("original allocation");
    drop(original);
    let replacement = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state,
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(RotatedCleanupDockerVerifier {
            daemon_id: "foreign-daemon",
        }),
    )
    .expect("replacement manager");

    assert!(matches!(
        replacement.release(&receipt),
        Err(StorageError::IdentityMismatch(_))
    ));
    assert!(receipt.mount_path.is_dir());
    assert!(!calls
        .lock()
        .expect("fake calls")
        .iter()
        .any(|call| call.starts_with("remove:")));
}

#[test]
fn cleanup_rejects_each_durable_receipt_identity_mismatch() {
    let (_root, manager, calls) = manager_fixture();
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");
    let mut mismatches = Vec::new();

    let mut labels_mismatch = receipt.clone();
    labels_mismatch.labels.worker_id = WorkerId::new("foreign-worker");
    mismatches.push(("labels", labels_mismatch));

    let mut source_mismatch = receipt.clone();
    source_mismatch.docker_bind.source_path = receipt.mount_path.join("foreign");
    mismatches.push(("source", source_mismatch));

    let mut filesystem_mismatch = receipt.clone();
    filesystem_mismatch.docker_bind.filesystem_id = "foreign-filesystem".to_owned();
    mismatches.push(("filesystem", filesystem_mismatch));

    let mut device_mismatch = receipt.clone();
    let BackendIdentity::Apfs { volume, .. } = &mut device_mismatch.backend else {
        panic!("fixture must use APFS identity");
    };
    *volume = "foreign-device".to_owned();
    mismatches.push(("device", device_mismatch));

    let mut allocation_mismatch = receipt.clone();
    allocation_mismatch.backend_key = "apfs:foreign-allocation".to_owned();
    mismatches.push(("allocation", allocation_mismatch));

    for (identity, mismatch) in mismatches {
        assert!(
            matches!(
                manager.release(&mismatch),
                Err(StorageError::IdentityMismatch(_))
            ),
            "cleanup accepted mismatched {identity} identity"
        );
    }
    assert!(receipt.mount_path.is_dir());
    assert!(!calls
        .lock()
        .expect("fake calls")
        .iter()
        .any(|call| call.starts_with("remove:")));
}

#[cfg(unix)]
#[test]
fn cleanup_rejects_symlink_and_irregular_allocation_paths_without_mutation() {
    let (_root, manager, calls) = manager_fixture();
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");
    let displaced = receipt.mount_path.with_extension("displaced");
    fs::rename(&receipt.mount_path, &displaced).expect("displace allocation");
    let marker = displaced.join("foreign-marker");
    fs::write(&marker, "preserve").expect("foreign marker");
    std::os::unix::fs::symlink(&displaced, &receipt.mount_path)
        .expect("replace allocation with symlink");

    assert!(matches!(
        manager.release(&receipt),
        Err(StorageError::IdentityMismatch(_))
    ));
    assert_eq!(
        fs::read_to_string(&marker).expect("marker survives"),
        "preserve"
    );
    assert!(!calls
        .lock()
        .expect("fake calls")
        .iter()
        .any(|call| call.starts_with("remove:")));

    fs::remove_file(&receipt.mount_path).expect("remove test symlink");
    fs::rename(&displaced, &receipt.mount_path).expect("restore allocation");
    fs::remove_file(receipt.mount_path.join("foreign-marker")).expect("remove test marker");
    for entry in fs::read_dir(&receipt.mount_path).expect("allocation entries") {
        let path = entry.expect("allocation entry").path();
        if path.is_dir() {
            fs::remove_dir_all(path).expect("remove allocation directory");
        } else {
            fs::remove_file(path).expect("remove allocation file");
        }
    }
    fs::remove_dir(&receipt.mount_path).expect("remove allocation directory");
    fs::write(&receipt.mount_path, "foreign-file").expect("irregular allocation path");
    assert!(matches!(
        manager.release(&receipt),
        Err(StorageError::IdentityMismatch(_))
    ));
    assert_eq!(
        fs::read_to_string(&receipt.mount_path).expect("irregular path survives"),
        "foreign-file"
    );
}

#[test]
fn live_ready_verification_requires_exact_ready_journal_and_mounted_identity() {
    let (_root, manager, _calls) = manager_fixture();
    let request = AllocationRequest {
        labels: labels(),
        disk_gib: 3,
    };
    let receipt = manager.allocate(&request).expect("allocate storage");

    manager
        .verify_ready(&receipt)
        .expect("verify exact durable Ready allocation");
    let layout = ExecutionLayout::new(manager.state_root(), &request.labels.execution_id)
        .expect("execution layout");
    JournalStore::new(manager.state_root())
        .expect("journal store")
        .write(
            &layout,
            &PhaseJournal::releasing(receipt.clone(), ReleasePhase::Mounted),
        )
        .expect("transition journal away from Ready");

    assert!(matches!(
        manager.verify_ready(&receipt),
        Err(StorageError::IdentityMismatch(_))
    ));
}

#[test]
fn ready_lease_prevents_concurrent_release_until_the_consumer_drops_it() {
    let (_root, manager, _calls) = manager_fixture();
    let manager = Arc::new(manager);
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");
    let lease: Box<dyn ReadyLease> = manager
        .acquire_ready_lease(&receipt)
        .expect("acquire exact Ready lease");
    lease
        .verified()
        .verify()
        .expect("leased allocation remains verified");

    let releasing_manager = Arc::clone(&manager);
    let releasing_receipt = receipt.clone();
    let release = std::thread::spawn(move || releasing_manager.release(&releasing_receipt))
        .join()
        .expect("release thread");
    assert!(matches!(release, Err(StorageError::Unavailable(_))));

    drop(lease);
    manager
        .release(&receipt)
        .expect("release succeeds after the Ready lease is dropped");
}

#[test]
fn durable_execution_hold_blocks_release_across_manager_instances() {
    let (_root, manager, _calls) = manager_fixture();
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");
    let holds = ExecutionLifecycleHoldStore::new(manager.state_root()).expect("hold store");
    holds
        .create(&ExecutionLifecycleHold {
            labels: receipt.labels.clone(),
            hold_id: "pi-session".to_owned(),
            container_id: "container-id".to_owned(),
            session_id: "session-id".to_owned(),
            supervisor_token: "token".to_owned(),
            pgid: Some(42),
        })
        .expect("durable hold");
    assert!(matches!(
        manager.release(&receipt),
        Err(StorageError::Unavailable(_))
    ));
    drop(holds);
    ExecutionLifecycleHoldStore::new(manager.state_root())
        .expect("reopen hold store")
        .remove(&receipt.labels.execution_id, "pi-session")
        .expect("remove hold");
    manager
        .release(&receipt)
        .expect("release after durable hold recovery");
}

#[test]
fn live_capability_pins_each_runtime_bind_directory_identity() {
    let (_root, manager, _calls) = manager_fixture();
    let request = AllocationRequest {
        labels: labels(),
        disk_gib: 3,
    };
    let receipt = manager.allocate(&request).expect("allocate storage");
    let verified = manager.verify_ready(&receipt).expect("live capability");
    let layout = ExecutionLayout::new(manager.state_root(), &request.labels.execution_id)
        .expect("execution layout");

    verified
        .verify_directory(&layout.conversation)
        .expect("pin conversation directory");
    let displaced = layout.session.join("conversation-displaced");
    fs::rename(&layout.conversation, &displaced).expect("displace pinned conversation");
    fs::create_dir(&layout.conversation).expect("replace conversation directory");

    assert!(matches!(
        verified.verify_directory(&layout.conversation),
        Err(StorageError::IdentityMismatch(_))
    ));
}

#[test]
fn release_refuses_label_or_backend_identity_mismatch_without_cleanup() {
    let (root, manager, calls) = manager_fixture();
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");
    let release_count = || {
        calls
            .lock()
            .expect("fake calls")
            .iter()
            .filter(|call| call.starts_with("remove:"))
            .count()
    };

    let mut wrong_labels = receipt.clone();
    wrong_labels.labels.execution_id = ExecutionId::new("foreign-execution");
    assert!(manager.release(&wrong_labels).is_err());
    assert!(
        !manager
            .state_root()
            .join("execution-storage/foreign-execution.lease")
            .exists(),
        "invalid release must not create lease metadata"
    );
    assert!(!root
        .path()
        .join("execution-storage/releases/foreign-execution.json")
        .exists());

    let mut wrong_labels = receipt.clone();
    wrong_labels.labels.worker_id = WorkerId::new("foreign-worker");
    assert!(manager.release(&wrong_labels).is_err());
    assert_eq!(release_count(), 0);
    assert!(!root
        .path()
        .join(format!(
            "execution-storage/releases/{}.json",
            wrong_labels.labels.execution_id
        ))
        .exists());

    let mut wrong_backend = receipt;
    wrong_backend.backend = apfs_identity("foreign-volume", "foreign-fs", "foreign-token");
    wrong_backend.docker_bind.filesystem_id = "foreign-fs".to_owned();
    assert!(manager.release(&wrong_backend).is_err());
    assert_eq!(release_count(), 0);
    assert!(!root
        .path()
        .join(format!(
            "execution-storage/releases/{}.json",
            wrong_backend.labels.execution_id
        ))
        .exists());
}

#[test]
fn release_ack_rejects_dangling_layout_symlink_and_preserves_tombstone() {
    let (root, manager, _calls) = manager_fixture();
    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate storage");
    let layout = ExecutionLayout::new(manager.state_root(), &receipt.labels.execution_id)
        .expect("execution layout");
    manager.release(&receipt).expect("physical release");
    std::os::unix::fs::symlink(root.path().join("missing-target"), &layout.root)
        .expect("replace deleted allocation with dangling symlink");
    let tombstone = root.path().join(format!(
        "execution-storage/releases/{}.json",
        receipt.labels.execution_id
    ));

    assert!(matches!(
        manager.ack_release(&receipt),
        Err(StorageError::IdentityMismatch(_))
    ));
    assert!(tombstone.is_file(), "failed ack must preserve tombstone");
}

#[test]
fn reconcile_reports_exact_orphan_journals_and_deletes_nothing() {
    let (_root, manager, _calls) = manager_fixture();
    let orphan = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("allocate orphan");
    let live_labels = OwnershipLabels {
        execution_id: ExecutionId::new("node-418-review-01"),
        ..labels()
    };
    let live = manager
        .allocate(&AllocationRequest {
            labels: live_labels.clone(),
            disk_gib: 3,
        })
        .expect("allocate live execution");

    let reported = manager
        .reconcile(std::slice::from_ref(&live_labels.execution_id))
        .expect("reconcile journals");
    assert_eq!(reported.len(), 1);
    assert_eq!(reported[0].labels, orphan.labels);
    assert!(orphan.mount_path.exists());
    assert!(live.mount_path.exists());
}

#[test]
fn failed_post_mount_proof_rolls_back_backend_mountpoint_and_journal() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FailingDockerVerifier),
    )
    .expect("storage manager");
    let error = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect_err("missing Docker bind proof fails closed");
    assert!(error.to_string().contains("Docker daemon"));
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    assert!(!layout.root.exists());
    assert!(!layout.journal.exists());
    assert!(calls
        .lock()
        .expect("fake calls")
        .iter()
        .any(|call| call.starts_with("remove:")));
}

#[test]
fn verifier_method_change_fails_allocation_and_rolls_back() {
    let root = tempfile::tempdir().expect("state");
    storage_directories(root.path());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(ChangingContractVerifier),
    )
    .expect("manager");
    let error = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 1,
        })
        .expect_err("method drift must fail closed");
    assert!(error.to_string().contains("verifier contract"));
    assert!(calls
        .lock()
        .expect("calls")
        .iter()
        .any(|call| call.starts_with("remove:")));
}

#[test]
fn existing_allocating_journal_is_recovered_before_a_new_exclusive_allocation() {
    let (root, manager, calls) = manager_fixture();
    let layout =
        ExecutionLayout::new(manager.state_root(), &labels().execution_id).expect("layout");
    fs::create_dir(&layout.root).expect("stale mountpoint");
    #[cfg(unix)]
    mode(&layout.root, 0o700);
    let stale = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(3).expect("bytes"),
        layout.root.clone(),
        "fake".to_owned(),
        format!("fake:{}", labels().execution_id),
        "pool-7".to_owned(),
        "stale-token".to_owned(),
    );
    JournalStore::new(manager.state_root())
        .expect("store")
        .create(&layout, &stale)
        .expect("stale journal");

    let receipt = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect("recover then allocate");
    assert_ne!(receipt.backend.ownership_token(), "stale-token");
    let calls = calls.lock().expect("calls");
    assert_eq!(calls[0], "probe:0");
    assert_eq!(calls[1], "discover");
    assert_eq!(calls[2], format!("probe:{}", disk_gib_to_bytes(3).unwrap()));
    assert_eq!(
        JournalStore::new(manager.state_root())
            .expect("store")
            .read(&layout)
            .expect("ready")
            .phase,
        AllocationPhase::Ready
    );
    drop(root);
}

#[test]
fn recovery_rejects_backend_pool_drift_before_discovery_or_cleanup() {
    let root = tempfile::tempdir().expect("state");
    storage_directories(root.path());
    let canonical = root.path().canonicalize().expect("canonical state");
    let layout = ExecutionLayout::new(&canonical, &labels().execution_id).expect("layout");
    fs::create_dir(&layout.root).expect("stale mountpoint");
    #[cfg(unix)]
    mode(&layout.root, 0o700);
    let stale = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(1).expect("bytes"),
        layout.root.clone(),
        "fake".to_owned(),
        format!("fake:{}", labels().execution_id),
        "old-pool".to_owned(),
        "stale-token".to_owned(),
    );
    JournalStore::new(&canonical)
        .expect("store")
        .create(&layout, &stale)
        .expect("journal");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("manager");
    let error = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 1,
        })
        .expect_err("pool drift must retain allocation");
    assert!(error.to_string().contains("pool identity changed"));
    assert_eq!(*calls.lock().expect("calls"), vec!["probe:0"]);
    assert_eq!(
        JournalStore::new(&canonical)
            .expect("store")
            .read(&layout)
            .expect("retained"),
        stale
    );
}

#[test]
fn prepare_must_preserve_exact_apfs_identity() {
    let root = tempfile::tempdir().expect("state");
    storage_directories(root.path());
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::new(Mutex::new(Vec::new())),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: true,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("manager");
    let error = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 1,
        })
        .expect_err("prepare identity mutation must roll back");
    assert!(error
        .to_string()
        .contains("prepared backend identity changed"));
}

#[test]
fn release_resumes_from_unmounted_and_already_absent_subphases() {
    for recovered_state in [BackendState::Unmounted, BackendState::Absent] {
        let root = tempfile::tempdir().expect("state");
        storage_directories(root.path());
        let state = Arc::new(Mutex::new(BackendState::Absent));
        let manager = ExecutionStorage::new(
            root.path(),
            Box::new(FakeBackend {
                calls: Arc::new(Mutex::new(Vec::new())),
                removes_mountpoint: false,
                state: Arc::clone(&state),
                pool_identity: "pool-7",
                mutate_prepared: false,
            }),
            Box::new(FakeDockerVerifier),
        )
        .expect("manager");
        let receipt = manager
            .allocate(&AllocationRequest {
                labels: labels(),
                disk_gib: 1,
            })
            .expect("allocate");
        let layout =
            ExecutionLayout::new(manager.state_root(), &labels().execution_id).expect("layout");
        for entry in fs::read_dir(&layout.root).expect("root entries") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                fs::remove_dir_all(path).expect("clear mounted content");
            } else {
                fs::remove_file(path).expect("clear file");
            }
        }
        *state.lock().expect("state") = recovered_state;
        let phase = if recovered_state == BackendState::Unmounted {
            ReleasePhase::Unmounted
        } else {
            ReleasePhase::ObjectAbsent
        };
        JournalStore::new(manager.state_root())
            .expect("store")
            .write(&layout, &PhaseJournal::releasing(receipt.clone(), phase))
            .expect("releasing phase");
        manager.release(&receipt).expect("idempotent recovery");
        assert!(!layout.journal.exists());
        assert!(!layout.root.exists());
    }
}

#[cfg(unix)]
#[test]
fn allocation_rollback_rejects_a_mountpoint_replaced_by_a_symlink() {
    let root = tempfile::tempdir().expect("temporary state root");
    storage_directories(root.path());
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::new(Mutex::new(Vec::new())),
            removes_mountpoint: false,
            state: Arc::new(Mutex::new(BackendState::Absent)),
            pool_identity: "pool-7",
            mutate_prepared: false,
        }),
        Box::new(SymlinkSwappingDockerVerifier),
    )
    .expect("storage manager");

    let error = manager
        .allocate(&AllocationRequest {
            labels: labels(),
            disk_gib: 3,
        })
        .expect_err("symlink swap must fail closed");
    assert!(error.to_string().contains("not a real directory"));
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    assert!(fs::symlink_metadata(&layout.root)
        .expect("foreign symlink remains untouched")
        .file_type()
        .is_symlink());
    assert!(layout.journal.is_file());
}

#[test]
fn reconcile_rejects_unexpected_entries_without_deleting_them() {
    let (root, manager, _calls) = manager_fixture();
    let foreign = root.path().join("execution-storage/foreign.tmp");
    fs::write(&foreign, "do not delete").expect("foreign metadata");

    assert!(manager.reconcile(&[]).is_err());
    assert_eq!(
        fs::read_to_string(foreign).expect("foreign entry survives"),
        "do not delete"
    );
}
