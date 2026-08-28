use crate::journal::require_real_directory;
use crate::{
    disk_gib_to_bytes, AllocationReceipt, BackendIdentity, DockerBindProof, ExecutionLayout,
    JournalStore, PhaseJournal, StorageError, ALLOCATION_API_VERSION,
};
use orchestrator_core::{ExecutionId, OwnershipLabels};
use std::{collections::BTreeSet, fmt::Debug, fs, path::Path, path::PathBuf};

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
}

impl DockerBindCapability {
    fn validate(&self) -> Result<(), StorageError> {
        if self.daemon_id.is_empty() || self.verifier.is_empty() {
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
    fn allocate(
        &self,
        layout: &ExecutionLayout,
        labels: &OwnershipLabels,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError>;
    fn verify(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
    ) -> Result<(), StorageError>;
    fn release(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError>;
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
}

impl ExecutionStorage {
    pub fn new(
        state_root: impl AsRef<Path>,
        backend: Box<dyn StorageBackend>,
        docker: Box<dyn DockerBindVerifier>,
    ) -> Result<Self, StorageError> {
        let state_root = state_root.as_ref().to_path_buf();
        require_real_directory(&state_root, "state root")?;
        require_real_directory(&state_root.join("executions"), "executions directory")?;
        let journals = JournalStore::new(&state_root)?;
        Ok(Self {
            state_root,
            journals,
            backend,
            docker,
        })
    }

    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    fn create_mountpoint(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
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
                fs::create_dir(&layout.root).map_err(|error| {
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
        for directory in [
            &layout.repository,
            &layout.session,
            &layout.conversation,
            &layout.credentials,
            &layout.runtime,
        ] {
            fs::create_dir(directory).map_err(|error| {
                StorageError::Journal(format!("create directory {}: {error}", directory.display()))
            })?;
            require_real_directory(directory, "execution directory")?;
        }
        Ok(())
    }

    fn rollback_allocation(
        &self,
        layout: &ExecutionLayout,
        identity: Option<&BackendIdentity>,
        cause: StorageError,
    ) -> StorageError {
        let mut failures = Vec::new();
        if let Some(identity) = identity {
            if let Err(error) = self.backend.release(layout, identity) {
                failures.push(error.to_string());
            }
        }
        if let Err(error) = self.remove_released_mountpoint(layout) {
            failures.push(error.to_string());
        }
        if identity.is_some() && failures.is_empty() {
            if let Err(error) = self.journals.remove(layout) {
                failures.push(error.to_string());
            }
        }
        if failures.is_empty() {
            cause
        } else {
            StorageError::Cleanup(format!(
                "allocation failed: {cause}; rollback failed: {}",
                failures.join("; ")
            ))
        }
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
        let capability = self.probe(request.disk_gib)?;
        if capability.backend.reservable_bytes < reserved_bytes {
            return Err(StorageError::Unavailable(format!(
                "backend can reserve {} bytes, need {reserved_bytes}",
                capability.backend.reservable_bytes
            )));
        }
        let layout = ExecutionLayout::new(&self.state_root, &request.labels.execution_id)?;
        self.create_mountpoint(&layout)?;
        let allocating = PhaseJournal::allocating(
            request.labels.clone(),
            reserved_bytes,
            layout.root.clone(),
            self.backend.key(&request.labels),
        );
        if let Err(error) = self.journals.write(&layout, &allocating) {
            return Err(self.rollback_allocation(&layout, None, error));
        }
        let identity = match self
            .backend
            .allocate(&layout, &request.labels, reserved_bytes)
        {
            Ok(identity) => identity,
            Err(error) => return Err(self.rollback_allocation(&layout, None, error)),
        };
        if let Err(error) = self.journals.write(
            &layout,
            &allocating.clone().with_backend_identity(identity.clone()),
        ) {
            return Err(self.rollback_allocation(&layout, Some(&identity), error));
        }
        let result = (|| {
            self.backend.verify(&layout, &identity, reserved_bytes)?;
            let docker_bind = self.docker.verify(&layout.root, identity.filesystem_id())?;
            if docker_bind.daemon_id != capability.docker_bind.daemon_id {
                return Err(StorageError::IdentityMismatch(
                    "Docker daemon identity changed after capability probe".to_owned(),
                ));
            }
            self.create_layout_directories(&layout)?;
            let receipt = AllocationReceipt {
                api_version: ALLOCATION_API_VERSION.to_owned(),
                labels: request.labels.clone(),
                reserved_bytes,
                mount_path: layout.root.clone(),
                backend: identity.clone(),
                docker_bind,
            };
            receipt.validate(&request.labels, &layout)?;
            self.journals
                .write(&layout, &PhaseJournal::ready(receipt.clone()))?;
            Ok(receipt)
        })();
        result.map_err(|error| self.rollback_allocation(&layout, Some(&identity), error))
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
        self.journals
            .write(&layout, &PhaseJournal::releasing(receipt.clone()))?;
        self.backend
            .verify(&layout, &receipt.backend, receipt.reserved_bytes)?;
        let current_bind = self
            .docker
            .verify(&layout.root, receipt.backend.filesystem_id())?;
        if current_bind != receipt.docker_bind {
            return Err(StorageError::IdentityMismatch(
                "Docker bind proof changed since allocation".to_owned(),
            ));
        }
        self.backend.release(&layout, &receipt.backend)?;
        self.remove_released_mountpoint(&layout)?;
        self.journals.remove(&layout)
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
