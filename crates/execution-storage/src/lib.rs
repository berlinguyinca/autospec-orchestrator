//! Hard-bounded, worker-owned execution storage (Task 4.5).

mod apfs;
mod command;
mod journal;
mod lvm;
mod manager;
mod model;

pub use apfs::ApfsBackend;
pub use command::{CommandOutput, CommandRunner, CommandSpec, ProcessCommandRunner};
pub use journal::{JournalStore, SecureMetadataDirectory};
pub use lvm::LvmBackend;
pub use manager::{
    AllocationRequest, BackendCapability, DockerBindCapability, DockerBindVerifier,
    ExecutionStorage, ExecutionStorageManager, ReadyAllocationVerifier, ReadyLease, StorageBackend,
    StorageCapability, VerifiedExecutionStorage,
};
pub use model::{
    disk_gib_to_bytes, AllocationPhase, AllocationReceipt, BackendIdentity, BackendState,
    DockerBindProof, ExecutionLayout, PhaseJournal, ReleasePhase, StorageError,
    ALLOCATION_API_VERSION,
};
