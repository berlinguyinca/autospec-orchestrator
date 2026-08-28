use execution_storage::{
    disk_gib_to_bytes, AllocationPhase, AllocationReceipt, AllocationRequest, BackendCapability,
    BackendIdentity, DockerBindCapability, DockerBindProof, DockerBindVerifier, ExecutionLayout,
    ExecutionStorage, ExecutionStorageManager, JournalStore, PhaseJournal, StorageBackend,
    StorageError, ALLOCATION_API_VERSION,
};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

fn labels() -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new("node-417-impl-01"),
        worker_id: WorkerId::new("buildbox-02"),
        repository: "InferWeave/autospec-orchestrator".to_owned(),
        issue: Some("417".to_owned()),
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
fn receipt_wire_format_is_versioned_and_preserves_exact_identity() {
    let root = tempfile::tempdir().expect("temporary state root");
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    let receipt = AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.to_owned(),
        labels: labels(),
        reserved_bytes: disk_gib_to_bytes(3).expect("bytes"),
        mount_path: layout.root.clone(),
        backend: BackendIdentity::Apfs {
            container: "disk3".to_owned(),
            volume: "disk9s1".to_owned(),
            volume_uuid: "A1B2-C3D4".to_owned(),
        },
        docker_bind: DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
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
    fs::create_dir(root.path().join("execution-storage")).expect("journal directory");
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    let store = JournalStore::new(root.path()).expect("journal store");
    let allocating = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(3).expect("bytes"),
        layout.root.clone(),
        "apfs:disk3:autospec-node-417-impl-01".to_owned(),
    );
    store.write(&layout, &allocating).expect("write allocating");
    assert_eq!(store.read(&layout).expect("read allocating"), allocating);

    let receipt = AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.to_owned(),
        labels: labels(),
        reserved_bytes: disk_gib_to_bytes(3).expect("bytes"),
        mount_path: layout.root.clone(),
        backend: BackendIdentity::Apfs {
            container: "disk3".to_owned(),
            volume: "disk9s1".to_owned(),
            volume_uuid: "A1B2-C3D4".to_owned(),
        },
        docker_bind: DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            source_path: layout.root.clone(),
            filesystem_id: "A1B2-C3D4".to_owned(),
        },
    };
    let ready = PhaseJournal::ready(receipt.clone());
    store.write(&layout, &ready).expect("write ready");
    assert_eq!(store.read(&layout).expect("read ready"), ready);
    let releasing = PhaseJournal::releasing(receipt);
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
    fs::create_dir(root.path().join("execution-storage")).expect("journal directory");
    let layout = ExecutionLayout::new(root.path(), &labels().execution_id).expect("layout");
    let store = JournalStore::new(root.path()).expect("journal store");
    let identity = BackendIdentity::Apfs {
        container: "disk3".to_owned(),
        volume: "disk9s1".to_owned(),
        volume_uuid: "EXEC-UUID".to_owned(),
    };
    let allocating = PhaseJournal::allocating(
        labels(),
        disk_gib_to_bytes(3).expect("bytes"),
        layout.root.clone(),
        "apfs:disk3:autospec-node-417-impl-01".to_owned(),
    )
    .with_backend_identity(identity.clone());

    store
        .write(&layout, &allocating)
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
            pool_identity: "pool-7".to_owned(),
            reservable_bytes: required_bytes,
        })
    }

    fn allocate(
        &self,
        layout: &ExecutionLayout,
        labels: &OwnershipLabels,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push("allocate".to_owned());
        Ok(BackendIdentity::Apfs {
            container: "disk3".to_owned(),
            volume: format!("volume-{}", labels.execution_id),
            volume_uuid: format!("fs-{}-{reserved_bytes}", layout.root.display()),
        })
    }

    fn verify(
        &self,
        _layout: &ExecutionLayout,
        identity: &BackendIdentity,
        _reserved_bytes: u64,
    ) -> Result<(), StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("verify:{}", identity.filesystem_id()));
        Ok(())
    }

    fn release(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        self.calls
            .lock()
            .expect("fake calls")
            .push(format!("release:{}", identity.filesystem_id()));
        if fs::symlink_metadata(&layout.root)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
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
        Ok(())
    }
}

#[derive(Debug)]
struct FakeDockerVerifier;

impl DockerBindVerifier for FakeDockerVerifier {
    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: "daemon-7".to_owned(),
            verifier: "fake-docker-bind-inspector".to_owned(),
        })
    }

    fn verify(&self, source: &Path, filesystem_id: &str) -> Result<DockerBindProof, StorageError> {
        Ok(DockerBindProof {
            daemon_id: "daemon-7".to_owned(),
            source_path: source.to_path_buf(),
            filesystem_id: filesystem_id.to_owned(),
        })
    }
}

#[derive(Debug)]
struct FailingDockerVerifier;

impl DockerBindVerifier for FailingDockerVerifier {
    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        Ok(DockerBindCapability {
            daemon_id: "daemon-7".to_owned(),
            verifier: "failing-fake-docker-bind-inspector".to_owned(),
        })
    }

    fn verify(
        &self,
        _source: &Path,
        _filesystem_id: &str,
    ) -> Result<DockerBindProof, StorageError> {
        Err(StorageError::Unavailable(
            "Docker daemon cannot prove the bind source".to_owned(),
        ))
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct SymlinkSwappingDockerVerifier;

#[cfg(unix)]
impl DockerBindVerifier for SymlinkSwappingDockerVerifier {
    fn probe(&self) -> Result<DockerBindCapability, StorageError> {
        FailingDockerVerifier.probe()
    }

    fn verify(&self, source: &Path, _filesystem_id: &str) -> Result<DockerBindProof, StorageError> {
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
    fs::create_dir(root.path().join("execution-storage")).expect("journal directory");
    fs::create_dir(root.path().join("executions")).expect("execution directory");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
        }),
        Box::new(FakeDockerVerifier),
    )
    .expect("storage manager");
    (root, manager, calls)
}

#[test]
fn release_accepts_backend_that_removes_its_mountpoint() {
    let root = tempfile::tempdir().expect("temporary state root");
    fs::create_dir(root.path().join("execution-storage")).expect("journal directory");
    fs::create_dir(root.path().join("executions")).expect("execution directory");
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::new(Mutex::new(Vec::new())),
            removes_mountpoint: true,
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
    let calls = calls.lock().expect("fake calls");
    assert_eq!(calls[0], format!("probe:{}", disk_gib_to_bytes(3).unwrap()));
    assert!(calls.iter().any(|call| call == "allocate"));
    assert!(calls.iter().any(|call| call.starts_with("release:")));
}

#[test]
fn release_refuses_label_or_backend_identity_mismatch_without_cleanup() {
    let (_root, manager, calls) = manager_fixture();
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
            .filter(|call| call.starts_with("release:"))
            .count()
    };

    let mut wrong_labels = receipt.clone();
    wrong_labels.labels.worker_id = WorkerId::new("foreign-worker");
    assert!(manager.release(&wrong_labels).is_err());
    assert_eq!(release_count(), 0);

    let mut wrong_backend = receipt;
    wrong_backend.backend = BackendIdentity::Apfs {
        container: "disk3".to_owned(),
        volume: "foreign-volume".to_owned(),
        volume_uuid: "foreign-fs".to_owned(),
    };
    wrong_backend.docker_bind.filesystem_id = "foreign-fs".to_owned();
    assert!(manager.release(&wrong_backend).is_err());
    assert_eq!(release_count(), 0);
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
    fs::create_dir(root.path().join("execution-storage")).expect("journal directory");
    fs::create_dir(root.path().join("executions")).expect("execution directory");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::clone(&calls),
            removes_mountpoint: false,
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
        .any(|call| call.starts_with("release:")));
}

#[cfg(unix)]
#[test]
fn allocation_rollback_rejects_a_mountpoint_replaced_by_a_symlink() {
    let root = tempfile::tempdir().expect("temporary state root");
    fs::create_dir(root.path().join("execution-storage")).expect("journal directory");
    fs::create_dir(root.path().join("executions")).expect("execution directory");
    let manager = ExecutionStorage::new(
        root.path(),
        Box::new(FakeBackend {
            calls: Arc::new(Mutex::new(Vec::new())),
            removes_mountpoint: false,
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
