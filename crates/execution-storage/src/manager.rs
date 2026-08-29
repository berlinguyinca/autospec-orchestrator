use crate::journal::{require_real_directory, PinnedDirectory};
use crate::{
    disk_gib_to_bytes, AllocationPhase, AllocationReceipt, BackendIdentity, BackendState,
    DockerBindProof, ExecutionLayout, JournalStore, PhaseJournal, ReleasePhase,
    SecureMetadataDirectory, StorageError, ALLOCATION_API_VERSION,
};
use fs2::FileExt;
use orchestrator_core::{ExecutionId, OwnershipLabels};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    fs::{self, File},
    path::Path,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

static TOKEN_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationRequest {
    pub labels: OwnershipLabels,
    pub disk_gib: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendCapability {
    pub backend: String,
    pub pool_identity: String,
    pub reservable_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerBindCapability {
    pub daemon_id: String,
    pub verifier: String,
    pub method_version: String,
}

impl DockerBindCapability {
    fn validate(&self) -> Result<(), StorageError> {
        if self.daemon_id.is_empty() || self.verifier.is_empty() || self.method_version.is_empty() {
            Err(StorageError::Unavailable(
                "Docker bind capability lacks daemon or verifier identity".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCapability {
    pub backend: BackendCapability,
    pub docker_bind: DockerBindCapability,
}

pub trait StorageBackend: Debug + Send + Sync {
    fn key(&self, labels: &OwnershipLabels) -> String;
    fn probe(&self, required_bytes: u64) -> Result<BackendCapability, StorageError>;
    fn discover(
        &self,
        layout: &ExecutionLayout,
        ownership_token: &str,
        reserved_bytes: u64,
    ) -> Result<Option<BackendIdentity>, StorageError>;
    fn create(
        &self,
        layout: &ExecutionLayout,
        labels: &OwnershipLabels,
        ownership_token: &str,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError>;
    fn prepare(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError>;
    fn mount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
    ) -> Result<(), StorageError>;
    fn state(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
    ) -> Result<crate::BackendState, StorageError>;
    fn unmount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError>;
    fn remove(&self, identity: &BackendIdentity) -> Result<(), StorageError>;
}

pub trait DockerBindVerifier: Debug + Send + Sync {
    fn probe(&self) -> Result<DockerBindCapability, StorageError>;

    fn verify(&self, source: &Path) -> Result<DockerBindProof, StorageError>;
}

/// Revalidates a durable ready allocation immediately before a consumer writes
/// into its mounted filesystem.
pub trait VerifiedExecutionStorage: Debug + Send + Sync {
    fn repository_path(&self) -> &Path;
    fn verify(&self) -> Result<(), StorageError>;
    fn verify_directory(&self, _path: &Path) -> Result<(), StorageError> {
        Err(StorageError::Unavailable(
            "verified storage capability does not expose pinned bind directories".to_owned(),
        ))
    }
    fn refresh_after_exact_recovery(&self) -> Result<(), StorageError> {
        self.verify()
    }
}

pub trait ReadyAllocationVerifier: Debug + Send + Sync {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError>;

    /// Acquires a shared lifecycle lease after revalidating the exact Ready allocation.
    ///
    /// The consumer must retain this lease across its complete mutation lifecycle;
    /// release obtains the corresponding exclusive lease (spec sections 13 and 81).
    fn acquire_ready_lease(
        &self,
        _receipt: &AllocationReceipt,
    ) -> Result<Box<dyn ReadyLease>, StorageError> {
        Err(StorageError::Unavailable(
            "Ready verifier does not provide a lifecycle lease".to_owned(),
        ))
    }
}

/// Object-safe, sendable guard preventing Ready storage from entering release.
pub trait ReadyLease: Debug + Send + Sync {
    /// Returns the path/device/inode capability bound to this lease.
    fn verified(&self) -> &dyn VerifiedExecutionStorage;
}

#[derive(Debug)]
struct LiveVerifiedExecutionStorage {
    root: PinnedDirectory,
    repository_path: PathBuf,
    repository: Mutex<Option<PinnedDirectory>>,
    directories: Mutex<BTreeMap<PathBuf, PinnedDirectory>>,
}

#[derive(Debug)]
struct LiveReadyLease {
    _lock: File,
    verified: LiveVerifiedExecutionStorage,
}

impl ReadyLease for LiveReadyLease {
    fn verified(&self) -> &dyn VerifiedExecutionStorage {
        &self.verified
    }
}

impl VerifiedExecutionStorage for LiveVerifiedExecutionStorage {
    fn repository_path(&self) -> &Path {
        &self.repository_path
    }

    fn verify(&self) -> Result<(), StorageError> {
        self.root.verify("execution mountpoint")?;
        if self.repository_path.parent() != Some(self.root.path.as_path()) {
            return Err(StorageError::IdentityMismatch(
                "execution repository is not a direct child of the mounted allocation".to_owned(),
            ));
        }
        let mut repository = self.repository.lock().map_err(|_| {
            StorageError::IdentityMismatch("execution repository pin is poisoned".to_owned())
        })?;
        let metadata = match fs::symlink_metadata(&self.repository_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && repository.is_none() => {
                return Ok(())
            }
            Err(error) => return Err(StorageError::IdentityMismatch(error.to_string())),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::IdentityMismatch(
                "execution repository is not a real directory".to_owned(),
            ));
        }
        match repository.as_ref() {
            Some(repository) => repository.verify("execution repository")?,
            None => {
                *repository = Some(PinnedDirectory::capture(
                    &self.repository_path,
                    "execution repository",
                )?);
            }
        }
        #[cfg(unix)]
        if fs::symlink_metadata(&self.root.path)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
            .dev()
            != fs::symlink_metadata(&self.repository_path)
                .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
                .dev()
        {
            return Err(StorageError::IdentityMismatch(
                "execution repository is not on the mounted allocation filesystem".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_directory(&self, path: &Path) -> Result<(), StorageError> {
        if path == self.repository_path {
            return self.verify();
        }
        self.root.verify("execution mountpoint")?;
        if !path.starts_with(&self.root.path) || path == self.root.path {
            return Err(StorageError::IdentityMismatch(
                "bind directory is outside the execution mountpoint".to_owned(),
            ));
        }
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::IdentityMismatch(
                "bind path is not a real directory".to_owned(),
            ));
        }
        #[cfg(unix)]
        if metadata.dev()
            != fs::symlink_metadata(&self.root.path)
                .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?
                .dev()
        {
            return Err(StorageError::IdentityMismatch(
                "bind directory is outside the execution filesystem".to_owned(),
            ));
        }
        let mut directories = self.directories.lock().map_err(|_| {
            StorageError::IdentityMismatch("bind directory pins are poisoned".to_owned())
        })?;
        match directories.get(path) {
            Some(directory) => directory.verify("execution bind directory"),
            None => {
                directories.insert(
                    path.to_path_buf(),
                    PinnedDirectory::capture(path, "execution bind directory")?,
                );
                Ok(())
            }
        }
    }

    fn refresh_after_exact_recovery(&self) -> Result<(), StorageError> {
        self.root.verify("execution mountpoint")?;
        if self.repository_path.parent() != Some(self.root.path.as_path()) {
            return Err(StorageError::IdentityMismatch(
                "execution repository is not a direct child of the mounted allocation".to_owned(),
            ));
        }
        let replacement = match fs::symlink_metadata(&self.repository_path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Some(
                PinnedDirectory::capture(&self.repository_path, "execution repository")?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Ok(_) => {
                return Err(StorageError::IdentityMismatch(
                    "execution repository is not a real directory".to_owned(),
                ))
            }
            Err(error) => return Err(StorageError::IdentityMismatch(error.to_string())),
        };
        *self.repository.lock().map_err(|_| {
            StorageError::IdentityMismatch("execution repository pin is poisoned".to_owned())
        })? = replacement;
        self.verify()
    }
}

pub trait ExecutionStorageManager: Send + Sync {
    fn probe(&self, disk_gib: u64) -> Result<StorageCapability, StorageError>;
    fn allocate(&self, request: &AllocationRequest) -> Result<AllocationReceipt, StorageError>;
    fn release(&self, receipt: &AllocationReceipt) -> Result<(), StorageError>;
    fn ack_release(&self, _receipt: &AllocationReceipt) -> Result<(), StorageError> {
        Ok(())
    }
    fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<PhaseJournal>, StorageError>;
}

#[derive(Debug)]
pub struct ExecutionStorage {
    state_root: PathBuf,
    journals: JournalStore,
    backend: Box<dyn StorageBackend>,
    docker: Box<dyn DockerBindVerifier>,
    executions_directory: PinnedDirectory,
    lifecycle_holds: crate::ExecutionLifecycleHoldStore,
    release_tombstones: SecureMetadataDirectory,
}

impl ExecutionStorage {
    pub fn new(
        state_root: impl AsRef<Path>,
        backend: Box<dyn StorageBackend>,
        docker: Box<dyn DockerBindVerifier>,
    ) -> Result<Self, StorageError> {
        let state_root = state_root.as_ref().canonicalize().map_err(|error| {
            StorageError::Unavailable(format!(
                "canonicalize state root {}: {error}",
                state_root.as_ref().display()
            ))
        })?;
        let state_identity = PinnedDirectory::capture(&state_root, "state root")?;
        let executions_directory =
            PinnedDirectory::capture(&state_root.join("executions"), "executions directory")?;
        let journals = JournalStore::new(&state_root)?;
        let lifecycle_holds = crate::ExecutionLifecycleHoldStore::new(&state_root)?;
        let metadata = SecureMetadataDirectory::new(state_root.join("execution-storage"))?;
        let release_path = metadata.path().join("releases");
        let release_tombstones = if release_path.exists() {
            SecureMetadataDirectory::new(&release_path)?
        } else {
            metadata.create_subdirectory("releases")?
        };
        state_identity.verify("state root")?;
        Ok(Self {
            state_root,
            journals,
            backend,
            docker,
            executions_directory,
            lifecycle_holds,
            release_tombstones,
        })
    }

    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    fn verify_ready_allocation(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<LiveVerifiedExecutionStorage, StorageError> {
        let layout = ExecutionLayout::new(&self.state_root, &receipt.labels.execution_id)?;
        receipt.validate(&receipt.labels, &layout)?;
        let journal = self.journals.read(&layout)?;
        if journal.phase != AllocationPhase::Ready || journal.receipt.as_ref() != Some(receipt) {
            return Err(StorageError::IdentityMismatch(
                "allocation receipt does not exactly match a durable Ready journal".to_owned(),
            ));
        }
        self.validate_backend_config(&journal)?;
        if self
            .backend
            .state(&layout, &receipt.backend, receipt.reserved_bytes)?
            != BackendState::Mounted
        {
            return Err(StorageError::IdentityMismatch(
                "ready backend is not mounted".to_owned(),
            ));
        }
        let current_bind = self.docker.verify(&layout.root)?;
        if current_bind != receipt.docker_bind {
            return Err(StorageError::IdentityMismatch(
                "Docker bind proof changed since allocation".to_owned(),
            ));
        }
        self.executions_directory.verify("executions directory")?;
        require_real_directory(&layout.root, "execution mountpoint")?;
        let verified = LiveVerifiedExecutionStorage {
            root: PinnedDirectory::capture(&layout.root, "execution mountpoint")?,
            repository_path: layout.repository.clone(),
            repository: Mutex::new(None),
            directories: Mutex::new(BTreeMap::new()),
        };
        verified.verify()?;
        Ok(verified)
    }

    fn create_mountpoint(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.executions_directory.verify("executions directory")?;
        if layout.root.parent() != Some(self.state_root.join("executions").as_path()) {
            return Err(StorageError::IdentityMismatch(
                "execution mountpoint is outside the deterministic root".to_owned(),
            ));
        }
        let name = mountpoint_name(layout)?;
        match self.executions_directory.child_metadata(name)? {
            Some(_) => Err(StorageError::IdentityMismatch(format!(
                "execution mountpoint already exists: {}",
                layout.root.display()
            ))),
            None => self.executions_directory.create_directory(name),
        }
    }

    fn remove_released_mountpoint(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.executions_directory.verify("executions directory")?;
        let name = mountpoint_name(layout)?;
        match self.executions_directory.child_metadata(name)? {
            Some(metadata) if metadata.file_type().is_dir() => {
                self.executions_directory.remove_directory(name)
            }
            Some(_) => Err(StorageError::IdentityMismatch(format!(
                "released mountpoint is not a real directory: {}",
                layout.root.display()
            ))),
            None => Ok(()),
        }
    }

    fn create_layout_directories(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.executions_directory.verify("executions directory")?;
        for directory in [
            &layout.repository,
            &layout.session,
            &layout.conversation,
            &layout.credentials,
            &layout.runtime,
        ] {
            create_secure_directory(directory).map_err(|error| {
                StorageError::Journal(format!("create directory {}: {error}", directory.display()))
            })?;
            require_real_directory(directory, "execution directory")?;
        }
        Ok(())
    }

    fn cleanup_identity(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
        receipt: Option<&AllocationReceipt>,
    ) -> Result<(), StorageError> {
        let mut state = self.backend.state(layout, identity, reserved_bytes)?;
        if state == BackendState::Mounted {
            self.backend.unmount(layout, identity)?;
            state = self.backend.state(layout, identity, reserved_bytes)?;
            if state != BackendState::Unmounted {
                return Err(StorageError::IdentityMismatch(
                    "backend remained mounted after exact unmount".to_owned(),
                ));
            }
            if let Some(receipt) = receipt {
                self.journals.write(
                    layout,
                    &PhaseJournal::releasing(receipt.clone(), ReleasePhase::Unmounted),
                )?;
            }
        }
        if state == BackendState::Unmounted {
            self.backend.remove(identity)?;
            state = self.backend.state(layout, identity, reserved_bytes)?;
            if state != BackendState::Absent {
                return Err(StorageError::IdentityMismatch(
                    "backend object remained after exact removal".to_owned(),
                ));
            }
            if let Some(receipt) = receipt {
                self.journals.write(
                    layout,
                    &PhaseJournal::releasing(receipt.clone(), ReleasePhase::ObjectAbsent),
                )?;
            }
        }
        if state != BackendState::Absent {
            return Err(StorageError::IdentityMismatch(
                "backend cleanup reached an unknown state".to_owned(),
            ));
        }
        self.remove_released_mountpoint(layout)?;
        self.journals.remove(layout)
    }

    fn recover_existing(
        &self,
        layout: &ExecutionLayout,
        request: &AllocationRequest,
    ) -> Result<(), StorageError> {
        let journal = self.journals.read(layout)?;
        if journal.labels != request.labels
            || journal.reserved_bytes != disk_gib_to_bytes(request.disk_gib)?
        {
            return Err(StorageError::IdentityMismatch(
                "existing journal belongs to a different allocation request".to_owned(),
            ));
        }
        self.validate_backend_config(&journal)?;
        match journal.phase {
            AllocationPhase::Ready => Err(StorageError::IdentityMismatch(
                "execution storage is already allocated".to_owned(),
            )),
            AllocationPhase::Releasing => {
                let receipt = journal.receipt.ok_or_else(|| {
                    StorageError::IdentityMismatch("releasing journal lacks receipt".to_owned())
                })?;
                self.cleanup_identity(
                    layout,
                    &receipt.backend,
                    receipt.reserved_bytes,
                    Some(&receipt),
                )
            }
            AllocationPhase::Allocating => {
                let lacked_identity = journal.backend.is_none();
                let identity = match journal.backend.clone() {
                    Some(identity) => Some(identity),
                    None => self.backend.discover(
                        layout,
                        &journal.ownership_token,
                        journal.reserved_bytes,
                    )?,
                };
                if let Some(identity) = identity {
                    if identity.ownership_token() != journal.ownership_token {
                        return Err(StorageError::IdentityMismatch(
                            "discovered backend ownership token changed".to_owned(),
                        ));
                    }
                    if lacked_identity {
                        self.journals.write(
                            layout,
                            &journal.clone().with_backend_identity(identity.clone()),
                        )?;
                    }
                    self.cleanup_identity(layout, &identity, journal.reserved_bytes, None)
                } else {
                    self.remove_released_mountpoint(layout)?;
                    self.journals.remove(layout)
                }
            }
        }
    }

    fn validate_backend_config(&self, journal: &PhaseJournal) -> Result<(), StorageError> {
        let configured = self.backend.probe(0)?;
        if journal.backend_kind == configured.backend
            && journal.backend_key == self.backend.key(&journal.labels)
            && journal.pool_identity == configured.pool_identity
        {
            Ok(())
        } else {
            Err(StorageError::IdentityMismatch(
                "existing journal backend configuration or pool identity changed".to_owned(),
            ))
        }
    }

    fn rollback_allocation(
        &self,
        layout: &ExecutionLayout,
        journal: &PhaseJournal,
        identity: Option<&BackendIdentity>,
        cause: StorageError,
    ) -> StorageError {
        let cleanup = (|| {
            let identity = match identity {
                Some(identity) => Some(identity.clone()),
                None => self.backend.discover(
                    layout,
                    &journal.ownership_token,
                    journal.reserved_bytes,
                )?,
            };
            if let Some(identity) = identity {
                if identity.ownership_token() != journal.ownership_token {
                    return Err(StorageError::IdentityMismatch(
                        "rollback discovery returned a foreign ownership token".to_owned(),
                    ));
                }
                let durable = journal.clone().with_backend_identity(identity.clone());
                self.journals.write(layout, &durable)?;
                self.cleanup_identity(layout, &identity, journal.reserved_bytes, None)
            } else {
                self.remove_released_mountpoint(layout)?;
                self.journals.remove(layout)
            }
        })();
        match cleanup {
            Ok(()) => cause,
            Err(cleanup) => StorageError::Cleanup(format!(
                "allocation failed: {cause}; rollback failed: {cleanup}"
            )),
        }
    }
}

impl ReadyAllocationVerifier for ExecutionStorage {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        Ok(Box::new(self.verify_ready_allocation(receipt)?))
    }

    fn acquire_ready_lease(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn ReadyLease>, StorageError> {
        let layout = ExecutionLayout::new(&self.state_root, &receipt.labels.execution_id)?;
        let lock = self.journals.open_ready_lease(&layout)?;
        FileExt::try_lock_shared(&lock).map_err(|error| {
            StorageError::Unavailable(format!("execution Ready allocation is changing: {error}"))
        })?;
        let verified = self.verify_ready_allocation(receipt)?;
        Ok(Box::new(LiveReadyLease {
            _lock: lock,
            verified,
        }))
    }
}

fn create_secure_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    fs::create_dir(path)
}

fn mountpoint_name(layout: &ExecutionLayout) -> Result<&str, StorageError> {
    layout
        .root
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StorageError::InvalidRequest(format!(
                "execution mountpoint name is not UTF-8: {}",
                layout.root.display()
            ))
        })
}

fn ownership_token() -> Result<String, StorageError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StorageError::Unavailable(format!("system clock before epoch: {error}")))?
        .as_nanos();
    let counter = TOKEN_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(format!(
        "{:016x}{:08x}",
        nanos as u64 ^ counter,
        std::process::id()
    ))
}

fn valid_prepared_transition(created: &BackendIdentity, prepared: &BackendIdentity) -> bool {
    match (created, prepared) {
        (BackendIdentity::Apfs { .. }, BackendIdentity::Apfs { .. }) => created == prepared,
        (
            BackendIdentity::Lvm {
                volume_group,
                volume_group_uuid,
                logical_volume,
                logical_volume_uuid,
                filesystem_uuid,
                ownership_token,
            },
            BackendIdentity::Lvm {
                volume_group: prepared_group,
                volume_group_uuid: prepared_group_uuid,
                logical_volume: prepared_volume,
                logical_volume_uuid: prepared_volume_uuid,
                filesystem_uuid: prepared_filesystem_uuid,
                ownership_token: prepared_token,
            },
        ) => {
            filesystem_uuid.is_empty()
                && !prepared_filesystem_uuid.is_empty()
                && volume_group == prepared_group
                && volume_group_uuid == prepared_group_uuid
                && logical_volume == prepared_volume
                && logical_volume_uuid == prepared_volume_uuid
                && ownership_token == prepared_token
        }
        _ => false,
    }
}

impl ExecutionStorageManager for ExecutionStorage {
    fn probe(&self, disk_gib: u64) -> Result<StorageCapability, StorageError> {
        let backend = self.backend.probe(disk_gib_to_bytes(disk_gib)?)?;
        let docker_bind = self.docker.probe()?;
        docker_bind.validate()?;
        Ok(StorageCapability {
            backend,
            docker_bind,
        })
    }

    fn allocate(&self, request: &AllocationRequest) -> Result<AllocationReceipt, StorageError> {
        let reserved_bytes = disk_gib_to_bytes(request.disk_gib)?;
        let layout = ExecutionLayout::new(&self.state_root, &request.labels.execution_id)?;
        if self
            .release_tombstones
            .read(&release_tombstone_name(&request.labels.execution_id)?)?
            .is_some()
        {
            return Err(StorageError::Unavailable(
                "prior physical release is awaiting durable acknowledgment".to_owned(),
            ));
        }
        if self.journals.exists(&layout)? {
            self.recover_existing(&layout, request)?;
        }
        let capability = self.probe(request.disk_gib)?;
        if capability.backend.reservable_bytes < reserved_bytes {
            return Err(StorageError::Unavailable(format!(
                "backend can reserve {} bytes, need {reserved_bytes}",
                capability.backend.reservable_bytes
            )));
        }
        self.create_mountpoint(&layout)?;
        let ownership_token = ownership_token()?;
        let allocating = PhaseJournal::allocating(
            request.labels.clone(),
            reserved_bytes,
            layout.root.clone(),
            capability.backend.backend.clone(),
            self.backend.key(&request.labels),
            capability.backend.pool_identity.clone(),
            ownership_token.clone(),
        );
        if let Err(error) = self.journals.create(&layout, &allocating) {
            let _ = self.remove_released_mountpoint(&layout);
            return Err(error);
        }
        let created_identity =
            match self
                .backend
                .create(&layout, &request.labels, &ownership_token, reserved_bytes)
            {
                Ok(identity) => identity,
                Err(error) => {
                    return Err(self.rollback_allocation(&layout, &allocating, None, error))
                }
            };
        if created_identity.ownership_token() != ownership_token {
            return Err(self.rollback_allocation(
                &layout,
                &allocating,
                Some(&created_identity),
                StorageError::IdentityMismatch(
                    "created backend ownership token differs from the journal".to_owned(),
                ),
            ));
        }
        if let Err(error) = self.journals.write(
            &layout,
            &allocating
                .clone()
                .with_backend_identity(created_identity.clone()),
        ) {
            return Err(self.rollback_allocation(
                &layout,
                &allocating,
                Some(&created_identity),
                error,
            ));
        }
        let prepared_identity =
            match self
                .backend
                .prepare(&layout, &created_identity, reserved_bytes)
            {
                Ok(identity) => identity,
                Err(error) => {
                    return Err(self.rollback_allocation(
                        &layout,
                        &allocating,
                        Some(&created_identity),
                        error,
                    ))
                }
            };
        if !valid_prepared_transition(&created_identity, &prepared_identity) {
            return Err(self.rollback_allocation(
                &layout,
                &allocating,
                Some(&prepared_identity),
                StorageError::IdentityMismatch(
                    "prepared backend identity changed outside the filesystem UUID transition"
                        .to_owned(),
                ),
            ));
        }
        let prepared_journal = allocating
            .clone()
            .with_backend_identity(prepared_identity.clone());
        if let Err(error) = self.journals.write(&layout, &prepared_journal) {
            return Err(self.rollback_allocation(
                &layout,
                &allocating,
                Some(&prepared_identity),
                error,
            ));
        }
        if let Err(error) = self
            .backend
            .mount(&layout, &prepared_identity, reserved_bytes)
        {
            return Err(self.rollback_allocation(
                &layout,
                &prepared_journal,
                Some(&prepared_identity),
                error,
            ));
        }
        let result = (|| {
            if self
                .backend
                .state(&layout, &prepared_identity, reserved_bytes)?
                != BackendState::Mounted
            {
                return Err(StorageError::IdentityMismatch(
                    "prepared backend is not mounted".to_owned(),
                ));
            }
            let docker_bind = self.docker.verify(&layout.root)?;
            if docker_bind.daemon_id != capability.docker_bind.daemon_id
                || docker_bind.verifier != capability.docker_bind.verifier
                || docker_bind.method_version != capability.docker_bind.method_version
            {
                return Err(StorageError::IdentityMismatch(
                    "Docker bind verifier contract changed after capability probe".to_owned(),
                ));
            }
            self.create_layout_directories(&layout)?;
            let receipt = AllocationReceipt {
                api_version: ALLOCATION_API_VERSION.to_owned(),
                labels: request.labels.clone(),
                reserved_bytes,
                mount_path: layout.root.clone(),
                backend_kind: capability.backend.backend.clone(),
                backend_key: self.backend.key(&request.labels),
                pool_identity: capability.backend.pool_identity.clone(),
                backend: prepared_identity.clone(),
                docker_bind,
            };
            receipt.validate(&request.labels, &layout)?;
            self.journals.ensure_ready_lease(&layout)?;
            self.journals
                .write(&layout, &PhaseJournal::ready(receipt.clone()))?;
            Ok(receipt)
        })();
        result.map_err(|error| {
            self.rollback_allocation(&layout, &prepared_journal, Some(&prepared_identity), error)
        })
    }

    fn release(&self, receipt: &AllocationReceipt) -> Result<(), StorageError> {
        let layout = ExecutionLayout::new(&self.state_root, &receipt.labels.execution_id)?;
        let tombstone_name = release_tombstone_name(&receipt.labels.execution_id)?;
        let receipt_bytes = serde_json::to_vec(receipt)
            .map_err(|error| StorageError::Journal(error.to_string()))?;
        match self.release_tombstones.read(&tombstone_name)? {
            Some(existing) if existing != receipt_bytes => {
                return Err(StorageError::IdentityMismatch(
                    "release tombstone belongs to another allocation receipt".to_owned(),
                ));
            }
            Some(_)
                if !layout.root.exists()
                    && self
                        .backend
                        .state(&layout, &receipt.backend, receipt.reserved_bytes)?
                        == BackendState::Absent =>
            {
                return Ok(())
            }
            Some(_) => {}
            None => self
                .release_tombstones
                .create(&tombstone_name, &receipt_bytes)?,
        }
        if !self
            .lifecycle_holds
            .list(&receipt.labels.execution_id)?
            .is_empty()
        {
            return Err(StorageError::Unavailable(
                "execution has an unresolved durable lifecycle hold".to_owned(),
            ));
        }
        let release_lock = self.journals.open_ready_lease(&layout)?;
        FileExt::try_lock_exclusive(&release_lock).map_err(|error| {
            StorageError::Unavailable(format!(
                "execution has an active Ready consumer lease: {error}"
            ))
        })?;
        receipt.validate(&receipt.labels, &layout)?;
        let journal = self.journals.read(&layout)?;
        if journal.receipt.as_ref() != Some(receipt)
            || !matches!(
                journal.phase,
                crate::AllocationPhase::Ready | crate::AllocationPhase::Releasing
            )
        {
            return Err(StorageError::IdentityMismatch(
                "release receipt does not exactly match the durable journal".to_owned(),
            ));
        }
        self.validate_backend_config(&journal)?;
        if journal.phase == AllocationPhase::Ready {
            if self
                .backend
                .state(&layout, &receipt.backend, receipt.reserved_bytes)?
                != BackendState::Mounted
            {
                return Err(StorageError::IdentityMismatch(
                    "ready backend is not mounted".to_owned(),
                ));
            }
            let current_bind = self.docker.verify(&layout.root)?;
            if current_bind != receipt.docker_bind {
                return Err(StorageError::IdentityMismatch(
                    "Docker bind proof changed since allocation".to_owned(),
                ));
            }
            self.journals.write(
                &layout,
                &PhaseJournal::releasing(receipt.clone(), ReleasePhase::Mounted),
            )?;
        }
        self.cleanup_identity(
            &layout,
            &receipt.backend,
            receipt.reserved_bytes,
            Some(receipt),
        )
    }

    fn ack_release(&self, receipt: &AllocationReceipt) -> Result<(), StorageError> {
        let name = release_tombstone_name(&receipt.labels.execution_id)?;
        let expected = serde_json::to_vec(receipt)
            .map_err(|error| StorageError::Journal(error.to_string()))?;
        let actual = self.release_tombstones.read(&name)?;
        let layout = ExecutionLayout::new(&self.state_root, &receipt.labels.execution_id)?;
        if actual.is_none()
            && self
                .backend
                .state(&layout, &receipt.backend, receipt.reserved_bytes)?
                == BackendState::Absent
            && !layout.root.exists()
        {
            return Ok(());
        }
        let actual = actual.ok_or_else(|| {
            StorageError::IdentityMismatch("release tombstone is absent".to_owned())
        })?;
        if actual != expected {
            return Err(StorageError::IdentityMismatch(
                "release tombstone belongs to another allocation receipt".to_owned(),
            ));
        }
        if self
            .backend
            .state(&layout, &receipt.backend, receipt.reserved_bytes)?
            != BackendState::Absent
        {
            return Err(StorageError::IdentityMismatch(
                "cannot acknowledge a release while its backend is present".to_owned(),
            ));
        }
        self.release_tombstones.remove(&name)
    }

    fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<PhaseJournal>, StorageError> {
        let live = live.iter().cloned().collect::<BTreeSet<_>>();
        Ok(self
            .journals
            .list(&self.state_root)?
            .into_iter()
            .filter(|journal| !live.contains(&journal.labels.execution_id))
            .collect())
    }
}

fn release_tombstone_name(execution_id: &ExecutionId) -> Result<String, StorageError> {
    let name = format!("{}.json", execution_id.as_str());
    if execution_id.as_str().is_empty()
        || execution_id.as_str().contains('/')
        || execution_id.as_str().contains("..")
    {
        return Err(StorageError::IdentityMismatch(
            "release tombstone execution id is unsafe".to_owned(),
        ));
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::valid_prepared_transition;
    use crate::BackendIdentity;

    fn lvm(filesystem_uuid: &str) -> BackendIdentity {
        BackendIdentity::Lvm {
            volume_group: "vg".to_owned(),
            volume_group_uuid: "vg-uuid".to_owned(),
            logical_volume: "lv".to_owned(),
            logical_volume_uuid: "lv-uuid".to_owned(),
            filesystem_uuid: filesystem_uuid.to_owned(),
            ownership_token: "token".to_owned(),
        }
    }

    #[test]
    fn lvm_prepare_may_only_fill_the_filesystem_uuid() {
        let created = lvm("");
        assert!(valid_prepared_transition(&created, &lvm("fs-uuid")));
        let mut changed = lvm("fs-uuid");
        if let BackendIdentity::Lvm {
            logical_volume_uuid,
            ..
        } = &mut changed
        {
            *logical_volume_uuid = "foreign".to_owned();
        }
        assert!(!valid_prepared_transition(&created, &changed));
        assert!(!valid_prepared_transition(
            &lvm("already-formatted"),
            &lvm("fs-uuid")
        ));
    }
}
