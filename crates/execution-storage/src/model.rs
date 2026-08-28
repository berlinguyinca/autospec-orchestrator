use orchestrator_core::{ExecutionId, OwnershipLabels};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const ALLOCATION_API_VERSION: &str = "autospec.dev/execution-storage/v1alpha1";
const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("invalid storage request: {0}")]
    InvalidRequest(String),
    #[error("storage capability unavailable: {0}")]
    Unavailable(String),
    #[error("storage identity mismatch: {0}")]
    IdentityMismatch(String),
    #[error("storage journal error: {0}")]
    Journal(String),
    #[error("storage command failed: {0}")]
    Command(String),
    #[error("storage cleanup failed: {0}")]
    Cleanup(String),
}

pub fn disk_gib_to_bytes(disk_gib: u64) -> Result<u64, StorageError> {
    if disk_gib == 0 {
        return Err(StorageError::InvalidRequest(
            "disk_gib must be greater than zero".to_owned(),
        ));
    }
    disk_gib
        .checked_mul(GIB)
        .ok_or_else(|| StorageError::InvalidRequest("disk_gib overflows bytes".to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionLayout {
    pub state_root: PathBuf,
    pub root: PathBuf,
    pub repository: PathBuf,
    pub session: PathBuf,
    pub conversation: PathBuf,
    pub credentials: PathBuf,
    pub runtime: PathBuf,
    pub journal: PathBuf,
}

impl ExecutionLayout {
    pub fn new(
        state_root: impl AsRef<Path>,
        execution_id: &ExecutionId,
    ) -> Result<Self, StorageError> {
        validate_execution_id(execution_id.as_str())?;
        let state_root = state_root.as_ref().to_path_buf();
        let root = state_root.join("executions").join(execution_id.as_str());
        let session = root.join("session");
        Ok(Self {
            state_root: state_root.clone(),
            repository: root.join("repository"),
            conversation: session.join("conversation"),
            credentials: root.join("credentials"),
            runtime: root.join("runtime"),
            session,
            journal: state_root
                .join("execution-storage")
                .join(format!("{execution_id}.json")),
            root,
        })
    }
}

fn validate_execution_id(execution_id: &str) -> Result<(), StorageError> {
    let bytes = execution_id.as_bytes();
    let valid = (1..=63).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidRequest(format!(
            "execution_id is not a safe path component: {execution_id}"
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackendIdentity {
    Apfs {
        container: String,
        volume: String,
        volume_uuid: String,
    },
    Lvm {
        volume_group: String,
        volume_group_uuid: String,
        logical_volume: String,
        logical_volume_uuid: String,
        filesystem_uuid: String,
    },
}

impl BackendIdentity {
    pub fn filesystem_id(&self) -> &str {
        match self {
            Self::Apfs { volume_uuid, .. } => volume_uuid,
            Self::Lvm {
                filesystem_uuid, ..
            } => filesystem_uuid,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        let fields: &[(&str, &str)] = match self {
            Self::Apfs {
                container,
                volume,
                volume_uuid,
            } => &[
                ("APFS container", container),
                ("APFS volume", volume),
                ("APFS volume UUID", volume_uuid),
            ],
            Self::Lvm {
                volume_group,
                volume_group_uuid,
                logical_volume,
                logical_volume_uuid,
                filesystem_uuid,
            } => &[
                ("LVM volume group", volume_group),
                ("LVM volume group UUID", volume_group_uuid),
                ("LVM logical volume", logical_volume),
                ("LVM logical volume UUID", logical_volume_uuid),
                ("filesystem UUID", filesystem_uuid),
            ],
        };
        if let Some((name, _)) = fields.iter().find(|(_, value)| value.is_empty()) {
            Err(StorageError::IdentityMismatch(format!("{name} is empty")))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerBindProof {
    pub daemon_id: String,
    pub source_path: PathBuf,
    pub filesystem_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocationReceipt {
    pub api_version: String,
    pub labels: OwnershipLabels,
    pub reserved_bytes: u64,
    pub mount_path: PathBuf,
    pub backend: BackendIdentity,
    pub docker_bind: DockerBindProof,
}

impl AllocationReceipt {
    pub fn validate(
        &self,
        expected_labels: &OwnershipLabels,
        expected_layout: &ExecutionLayout,
    ) -> Result<(), StorageError> {
        if self.api_version != ALLOCATION_API_VERSION {
            return Err(StorageError::IdentityMismatch(format!(
                "allocation API version {} is not {ALLOCATION_API_VERSION}",
                self.api_version
            )));
        }
        if &self.labels != expected_labels {
            return Err(StorageError::IdentityMismatch(
                "ownership labels differ from the requested execution".to_owned(),
            ));
        }
        if self.mount_path != expected_layout.root {
            return Err(StorageError::IdentityMismatch(format!(
                "receipt mount {} is not {}",
                self.mount_path.display(),
                expected_layout.root.display()
            )));
        }
        if self.reserved_bytes == 0 {
            return Err(StorageError::IdentityMismatch(
                "reserved byte count is zero".to_owned(),
            ));
        }
        self.backend.validate()?;
        if self.docker_bind.daemon_id.is_empty()
            || self.docker_bind.source_path != self.mount_path
            || self.docker_bind.filesystem_id != self.backend.filesystem_id()
        {
            return Err(StorageError::IdentityMismatch(
                "Docker bind proof does not match the allocation".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationPhase {
    Allocating,
    Ready,
    Releasing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseJournal {
    pub api_version: String,
    pub phase: AllocationPhase,
    pub labels: OwnershipLabels,
    pub reserved_bytes: u64,
    pub mount_path: PathBuf,
    pub backend_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<BackendIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt: Option<AllocationReceipt>,
}

impl PhaseJournal {
    pub fn allocating(
        labels: OwnershipLabels,
        reserved_bytes: u64,
        mount_path: PathBuf,
        backend_key: String,
    ) -> Self {
        Self {
            api_version: ALLOCATION_API_VERSION.to_owned(),
            phase: AllocationPhase::Allocating,
            labels,
            reserved_bytes,
            mount_path,
            backend_key,
            backend: None,
            receipt: None,
        }
    }

    pub fn with_backend_identity(mut self, backend: BackendIdentity) -> Self {
        self.backend = Some(backend);
        self
    }

    pub fn ready(receipt: AllocationReceipt) -> Self {
        Self::from_receipt(AllocationPhase::Ready, receipt)
    }

    pub fn releasing(receipt: AllocationReceipt) -> Self {
        Self::from_receipt(AllocationPhase::Releasing, receipt)
    }

    fn from_receipt(phase: AllocationPhase, receipt: AllocationReceipt) -> Self {
        Self {
            api_version: ALLOCATION_API_VERSION.to_owned(),
            phase,
            labels: receipt.labels.clone(),
            reserved_bytes: receipt.reserved_bytes,
            mount_path: receipt.mount_path.clone(),
            backend_key: receipt.backend.filesystem_id().to_owned(),
            backend: Some(receipt.backend.clone()),
            receipt: Some(receipt),
        }
    }

    pub(crate) fn validate(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        if self.api_version != ALLOCATION_API_VERSION
            || self.labels.execution_id.as_str()
                != layout
                    .journal
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
            || self.mount_path != layout.root
            || self.reserved_bytes == 0
            || self.backend_key.is_empty()
        {
            return Err(StorageError::IdentityMismatch(
                "journal identity does not match its deterministic path".to_owned(),
            ));
        }
        if let Some(backend) = &self.backend {
            backend.validate()?;
        }
        match (&self.phase, &self.backend, &self.receipt) {
            (AllocationPhase::Allocating, _, None) => Ok(()),
            (AllocationPhase::Ready | AllocationPhase::Releasing, Some(backend), Some(receipt))
                if backend == &receipt.backend =>
            {
                receipt.validate(&self.labels, layout)
            }
            _ => Err(StorageError::IdentityMismatch(
                "journal phase and receipt presence disagree".to_owned(),
            )),
        }
    }
}
