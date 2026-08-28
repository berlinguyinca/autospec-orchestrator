use crate::journal::{require_real_directory, DirectoryIdentity};
use crate::{
    disk_gib_to_bytes, AllocationPhase, AllocationReceipt, BackendIdentity, BackendState,
    DockerBindProof, ExecutionLayout, JournalStore, PhaseJournal, ReleasePhase, StorageError,
    ALLOCATION_API_VERSION,
};
use orchestrator_core::{ExecutionId, OwnershipLabels};
use std::{
    collections::BTreeSet,
    fmt::Debug,
    fs,
    path::Path,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

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

    fn verify(&self, source: &Path, filesystem_id: &str) -> Result<DockerBindProof, StorageError>;
}

pub trait ExecutionStorageManager: Send + Sync {
    fn probe(&self, disk_gib: u64) -> Result<StorageCapability, StorageError>;
    fn allocate(&self, request: &AllocationRequest) -> Result<AllocationReceipt, StorageError>;
    fn release(&self, receipt: &AllocationReceipt) -> Result<(), StorageError>;
    fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<PhaseJournal>, StorageError>;
}

#[derive(Debug)]
pub struct ExecutionStorage {
    state_root: PathBuf,
    journals: JournalStore,
    backend: Box<dyn StorageBackend>,
    docker: Box<dyn DockerBindVerifier>,
    executions_directory: DirectoryIdentity,
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
        let state_identity = DirectoryIdentity::capture(&state_root, "state root")?;
        let executions_directory =
            DirectoryIdentity::capture(&state_root.join("executions"), "executions directory")?;
        let journals = JournalStore::new(&state_root)?;
        state_identity.verify("state root")?;
        Ok(Self {
            state_root,
            journals,
            backend,
            docker,
            executions_directory,
        })
    }

    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    fn create_mountpoint(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.executions_directory.verify("executions directory")?;
        if layout.root.parent() != Some(self.state_root.join("executions").as_path()) {
            return Err(StorageError::IdentityMismatch(
                "execution mountpoint is outside the deterministic root".to_owned(),
            ));
        }
        match fs::symlink_metadata(&layout.root) {
            Ok(_) => Err(StorageError::IdentityMismatch(format!(
                "execution mountpoint already exists: {}",
                layout.root.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                create_secure_directory(&layout.root).map_err(|error| {
                    StorageError::Journal(format!(
                        "create mountpoint {}: {error}",
                        layout.root.display()
                    ))
                })
            }
            Err(error) => Err(StorageError::Journal(format!(
                "inspect mountpoint {}: {error}",
                layout.root.display()
            ))),
        }
    }

    fn remove_released_mountpoint(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.executions_directory.verify("executions directory")?;
        match fs::symlink_metadata(&layout.root) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                fs::remove_dir(&layout.root).map_err(|error| {
                    StorageError::Cleanup(format!(
                        "remove mountpoint {}: {error}",
                        layout.root.display()
                    ))
                })
            }
            Ok(_) => Err(StorageError::IdentityMismatch(format!(
                "released mountpoint is not a real directory: {}",
                layout.root.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::Cleanup(format!(
                "inspect released mountpoint {}: {error}",
                layout.root.display()
            ))),
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
        match fs::symlink_metadata(&layout.journal) {
            Ok(_) => self.recover_existing(&layout, request)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::Journal(format!(
                    "inspect journal {}: {error}",
                    layout.journal.display()
                )))
            }
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
            self.backend.key(&request.labels),
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
        if prepared_identity.ownership_token() != ownership_token {
            return Err(self.rollback_allocation(
                &layout,
                &allocating,
                Some(&prepared_identity),
                StorageError::IdentityMismatch(
                    "prepared backend ownership token differs from the journal".to_owned(),
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
            let docker_bind = self
                .docker
                .verify(&layout.root, prepared_identity.filesystem_id())?;
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
                backend: prepared_identity.clone(),
                docker_bind,
            };
            receipt.validate(&request.labels, &layout)?;
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
            let current_bind = self
                .docker
                .verify(&layout.root, receipt.backend.filesystem_id())?;
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
