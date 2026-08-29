use crate::command::{git_command, repository_git_command};
use crate::filesystem::{SystemWorktreeFilesystem, WorktreeFilesystem, WorktreeFilesystemPoint};
use crate::lock::FileLock;
use crate::{DiffCapture, Worktree, WorktreeError, WorktreeManager};
use execution_storage::{
    AllocationReceipt, ExecutionLayout, ReadyAllocationVerifier, SecureMetadataDirectory,
    StorageError, VerifiedExecutionStorage,
};
use orchestrator_core::labels::{EXECUTION_ID, REPOSITORY};
use orchestrator_core::{ExecutionId, OwnershipLabels};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

#[derive(Debug, Clone)]
pub struct GitWorktreeManager {
    pub(crate) cache_root: PathBuf,
    clone_base: String,
    pub(crate) filesystem: Arc<dyn WorktreeFilesystem>,
    storage_verifier: Arc<dyn ReadyAllocationVerifier>,
}

#[derive(Debug)]
struct RejectUnverifiedStorage;

impl ReadyAllocationVerifier for RejectUnverifiedStorage {
    fn verify_ready(
        &self,
        _receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        Err(StorageError::IdentityMismatch(
            "no live execution-storage verifier is configured".to_owned(),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OwnerRecord {
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) base_sha: String,
    pub(crate) branch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CreatePhase {
    Cloning,
    Cloned,
    CheckedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateIntent {
    labels: BTreeMap<String, String>,
    repository: String,
    base_ref: String,
    base_sha: String,
    branch: String,
    repository_path: String,
    phase: CreatePhase,
}

struct MirrorRoot {
    path: PathBuf,
    handle: File,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    owner: u32,
}

pub(crate) struct LockedMirror {
    root: MirrorRoot,
    path: PathBuf,
    handle: File,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    owner: u32,
    _lock: FileLock,
}

impl MirrorRoot {
    fn verify(&self) -> Result<(), WorktreeError> {
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        let opened = self
            .handle
            .metadata()
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        if !metadata.file_type().is_dir() {
            return Err(WorktreeError::Mirror(format!(
                "mirror root is not a real directory: {}",
                self.path.display()
            )));
        }
        #[cfg(unix)]
        if metadata.dev() != self.device
            || metadata.ino() != self.inode
            || metadata.uid() != self.owner
            || metadata.mode() & 0o077 != 0
            || opened.dev() != self.device
            || opened.ino() != self.inode
        {
            return Err(WorktreeError::Mirror(
                "mirror root identity, owner, or mode changed".to_owned(),
            ));
        }
        Ok(())
    }
}

impl GitWorktreeManager {
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self::with_clone_base(cache_root, "https://github.com")
    }

    pub fn with_clone_base(cache_root: impl Into<PathBuf>, clone_base: impl Into<String>) -> Self {
        Self::with_clone_base_and_filesystem(
            cache_root,
            clone_base,
            Arc::new(SystemWorktreeFilesystem),
        )
    }

    pub fn with_clone_base_and_verifier(
        cache_root: impl Into<PathBuf>,
        clone_base: impl Into<String>,
        storage_verifier: Arc<dyn ReadyAllocationVerifier>,
    ) -> Self {
        Self::with_clone_base_filesystem_and_verifier(
            cache_root,
            clone_base,
            Arc::new(SystemWorktreeFilesystem),
            storage_verifier,
        )
    }

    pub fn with_clone_base_and_filesystem(
        cache_root: impl Into<PathBuf>,
        clone_base: impl Into<String>,
        filesystem: Arc<dyn WorktreeFilesystem>,
    ) -> Self {
        Self::with_clone_base_filesystem_and_verifier(
            cache_root,
            clone_base,
            filesystem,
            Arc::new(RejectUnverifiedStorage),
        )
    }

    pub fn with_clone_base_filesystem_and_verifier(
        cache_root: impl Into<PathBuf>,
        clone_base: impl Into<String>,
        filesystem: Arc<dyn WorktreeFilesystem>,
        storage_verifier: Arc<dyn ReadyAllocationVerifier>,
    ) -> Self {
        Self {
            cache_root: cache_root.into(),
            clone_base: clone_base.into().trim_end_matches('/').to_owned(),
            filesystem,
            storage_verifier,
        }
    }

    pub(crate) fn mirrors_root(&self) -> PathBuf {
        self.cache_root.join("mirrors")
    }

    pub(crate) fn worktrees_root(&self) -> PathBuf {
        self.cache_root.join("worktrees")
    }

    pub(crate) fn executions_root(&self) -> PathBuf {
        self.cache_root.join("executions")
    }

    pub(crate) fn execution_repository_path(&self, execution_id: &ExecutionId) -> PathBuf {
        self.executions_root()
            .join(execution_id.as_str())
            .join("repository")
    }

    fn clone_locator(&self, repo: &str) -> Result<String, WorktreeError> {
        let (owner, name) = canonical_repository(repo)?;
        Ok(format!("{}/{owner}/{name}.git", self.clone_base))
    }

    pub(crate) fn lock_mirror_for_capture(
        &self,
        repo: &str,
    ) -> Result<LockedMirror, WorktreeError> {
        let path = PathBuf::from(self.ensure_mirror(repo)?);
        let lock_path = path.with_extension("git.lock");
        let lock = FileLock::acquire_with(&lock_path, WorktreeError::Diff)?;
        let root = secure_mirror_root(&self.cache_root)?;
        let clone_locator = self.clone_locator(repo)?;
        verify_mirror_repository(&root, &path, &clone_locator)?;
        verify_mirror_storage_preflight(&root, &path)?;
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        let handle = File::open(&path).map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        let opened = handle
            .metadata()
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        #[cfg(unix)]
        if metadata.dev() != opened.dev()
            || metadata.ino() != opened.ino()
            || metadata.uid() != opened.uid()
        {
            return Err(WorktreeError::Mirror(
                "mirror identity changed while pinning".to_owned(),
            ));
        }
        Ok(LockedMirror {
            root,
            path,
            handle,
            #[cfg(unix)]
            device: opened.dev(),
            #[cfg(unix)]
            inode: opened.ino(),
            #[cfg(unix)]
            owner: opened.uid(),
            _lock: lock,
        })
    }
}

impl LockedMirror {
    pub(crate) fn objects_path(&self) -> PathBuf {
        self.path.join("objects")
    }

    pub(crate) fn verify(&self) -> Result<(), WorktreeError> {
        self.root.verify()?;
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        let opened = self
            .handle
            .metadata()
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        if !metadata.file_type().is_dir() {
            return Err(WorktreeError::Mirror(
                "locked mirror is no longer a real directory".to_owned(),
            ));
        }
        #[cfg(unix)]
        if metadata.dev() != self.device
            || metadata.ino() != self.inode
            || metadata.uid() != self.owner
            || opened.dev() != self.device
            || opened.ino() != self.inode
        {
            return Err(WorktreeError::Mirror(
                "locked mirror identity changed".to_owned(),
            ));
        }
        verify_mirror_storage_preflight(&self.root, &self.path)
    }
}

impl WorktreeManager for GitWorktreeManager {
    fn ensure_mirror(&self, repo: &str) -> Result<String, WorktreeError> {
        let mirror_root = secure_mirror_root(&self.cache_root)?;
        let clone_locator = self.clone_locator(repo)?;
        let mirror = mirror_root
            .path
            .join(format!("{}.git", normalized_repository_name(repo)?));
        let lock_path = mirror.with_extension("git.lock");
        let _lock = FileLock::acquire(&lock_path)?;
        mirror_root.verify()?;

        match fs::symlink_metadata(&mirror) {
            Ok(_) => {
                verify_mirror_repository(&mirror_root, &mirror, &clone_locator)?;
                run_git(
                    [
                        OsStr::new("--git-dir"),
                        mirror.as_os_str(),
                        OsStr::new("remote"),
                        OsStr::new("update"),
                        OsStr::new("--prune"),
                    ],
                    WorktreeError::Mirror,
                )?;
                verify_mirror_repository(&mirror_root, &mirror, &clone_locator)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                run_git(
                    [
                        OsStr::new("clone"),
                        OsStr::new("--mirror"),
                        OsStr::new(&clone_locator),
                        mirror.as_os_str(),
                    ],
                    WorktreeError::Mirror,
                )?;
                verify_mirror_repository(&mirror_root, &mirror, &clone_locator)?;
            }
            Err(error) => return Err(WorktreeError::Mirror(error.to_string())),
        }

        Ok(mirror.to_string_lossy().into_owned())
    }

    fn create(
        &self,
        _labels: &OwnershipLabels,
        _repo: &str,
        _base_ref: &str,
        _branch: &str,
    ) -> Result<Worktree, WorktreeError> {
        Err(WorktreeError::Create(
            "unbounded worktree creation is disabled; use create_in with verified execution storage"
                .to_owned(),
        ))
    }

    fn create_in(
        &self,
        labels: &OwnershipLabels,
        repo: &str,
        base_ref: &str,
        branch: &str,
        storage: &AllocationReceipt,
    ) -> Result<Worktree, WorktreeError> {
        validate_execution_id(&labels.execution_id)?;
        if labels.repository != repo {
            return Err(WorktreeError::Ownership(format!(
                "repository label {} does not match {repo}",
                labels.repository
            )));
        }
        validate_branch(branch)?;
        let layout = ExecutionLayout::new(&self.cache_root, &labels.execution_id)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        storage
            .validate(labels, &layout)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        if storage.docker_bind.filesystem_id != storage.backend.filesystem_id() {
            return Err(WorktreeError::Ownership(
                "storage backend and bind-proof filesystem identities differ".to_owned(),
            ));
        }
        let repository_root = &layout.repository;
        verify_repository_selector(self, &layout, repository_root)?;
        let verified_storage = self
            .storage_verifier
            .verify_ready(storage)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        verify_live_storage(verified_storage.as_ref(), repository_root)?;

        let intent_path = create_intent_path(self, &labels.execution_id);
        let recovered_intent = read_create_intent(self, &intent_path)?;
        let recovered_base_sha = if let Some(existing) = recovered_intent.as_ref() {
            verify_create_intent(existing, labels, repo, base_ref, branch, repository_root)?;
            rollback_independent_repository(self.filesystem.as_ref(), repository_root)
                .map_err(WorktreeError::Create)?;
            verified_storage
                .refresh_after_exact_recovery()
                .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
            Some(existing.base_sha.clone())
        } else {
            None
        };

        let mirror = PathBuf::from(self.ensure_mirror(repo)?);
        let mirror_lock = mirror.with_extension("git.lock");
        let _mirror_lock = FileLock::acquire_with(&mirror_lock, WorktreeError::Create)?;
        let mirror_root = secure_mirror_root(&self.cache_root)
            .map_err(|error| WorktreeError::Create(error.to_string()))?;
        mirror_root
            .verify()
            .map_err(|error| WorktreeError::Create(error.to_string()))?;
        let clone_locator = self.clone_locator(repo)?;
        verify_mirror_repository(&mirror_root, &mirror, &clone_locator)
            .map_err(|error| WorktreeError::Create(error.to_string()))?;
        let branch_lock = self.mirrors_root().join(format!(
            "{}.branch-{}.lock",
            normalized_repository_name(repo)?,
            hex_component(branch)
        ));
        let _branch_lock = FileLock::acquire(&branch_lock)?;
        if let Some(owner) = self.branch_owner(repo, branch)? {
            return Err(WorktreeError::Locked(owner));
        }
        let base_sha = match recovered_base_sha {
            Some(base_sha) => {
                run_git(
                    [
                        OsStr::new("--git-dir"),
                        mirror.as_os_str(),
                        OsStr::new("cat-file"),
                        OsStr::new("-e"),
                        OsStr::new(&format!("{base_sha}^{{commit}}")),
                    ],
                    WorktreeError::Create,
                )?;
                base_sha
            }
            None => git_stdout(
                [
                    OsStr::new("--git-dir"),
                    mirror.as_os_str(),
                    OsStr::new("rev-parse"),
                    OsStr::new("--verify"),
                    OsStr::new(base_ref),
                ],
                WorktreeError::Create,
            )?,
        };
        if fs::read_dir(repository_root)
            .map_err(|error| WorktreeError::Create(error.to_string()))?
            .next()
            .is_some()
        {
            return Err(WorktreeError::Create(format!(
                "repository root is not empty: {}",
                repository_root.display()
            )));
        }
        let mut intent = CreateIntent {
            labels: labels.to_map(),
            repository: repo.to_owned(),
            base_ref: base_ref.to_owned(),
            base_sha: base_sha.clone(),
            branch: branch.to_owned(),
            repository_path: repository_root.to_string_lossy().into_owned(),
            phase: CreatePhase::Cloning,
        };
        write_create_intent(self, &intent_path, &intent, recovered_intent.is_some())?;
        let setup_result = (|| {
            mirror_root
                .verify()
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            verify_mirror_repository(&mirror_root, &mirror, &clone_locator)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            let current_storage = self
                .storage_verifier
                .verify_ready(storage)
                .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
            verify_live_storage(current_storage.as_ref(), repository_root)?;
            self.filesystem
                .clone_repository(&mirror, repository_root)
                .map_err(create_io_error)?;
            self.filesystem
                .checkpoint(
                    WorktreeFilesystemPoint::CloneObjectPack,
                    &repository_root.join(".git/objects/pack"),
                )
                .map_err(create_io_error)?;
            verify_repository_storage(repository_root)?;
            verified_storage
                .verify()
                .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
            intent.phase = CreatePhase::Cloned;
            write_create_intent(self, &intent_path, &intent, true)?;
            run_repository_git(
                repository_root,
                [
                    OsStr::new("remote"),
                    OsStr::new("set-url"),
                    OsStr::new("origin"),
                    OsStr::new(&clone_locator),
                ],
                WorktreeError::Create,
            )?;
            run_repository_git(
                repository_root,
                [
                    OsStr::new("checkout"),
                    OsStr::new("-b"),
                    OsStr::new(branch),
                    OsStr::new(&base_sha),
                ],
                WorktreeError::Create,
            )?;
            self.filesystem
                .checkpoint(
                    WorktreeFilesystemPoint::CheckoutIndex,
                    &repository_root.join(".git/index"),
                )
                .map_err(create_io_error)?;
            intent.phase = CreatePhase::CheckedOut;
            write_create_intent(self, &intent_path, &intent, true)?;
            verify_repository_storage(repository_root)?;
            verified_storage
                .verify()
                .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
            write_owner_record(
                self.filesystem.as_ref(),
                repository_root,
                &OwnerRecord {
                    labels: labels.to_map(),
                    base_sha: base_sha.clone(),
                    branch: branch.to_owned(),
                },
            )
        })();
        if let Err(error) = setup_result {
            let rollback =
                rollback_independent_repository(self.filesystem.as_ref(), repository_root);
            return Err(match rollback {
                Ok(()) => match remove_create_intent(self, &intent_path) {
                    Ok(()) => error,
                    Err(removal) => WorktreeError::Create(format!(
                        "{error}; remove create intent failed: {removal}"
                    )),
                },
                Err(rollback) => {
                    WorktreeError::Create(format!("{error}; rollback failed: {rollback}"))
                }
            });
        }
        remove_create_intent(self, &intent_path)?;

        Ok(Worktree {
            execution_id: labels.execution_id.clone(),
            path: repository_root.to_string_lossy().into_owned(),
            branch: branch.to_owned(),
            base_sha,
            repository: repo.to_owned(),
        })
    }

    fn capture_diff(&self, worktree: &Worktree) -> Result<DiffCapture, WorktreeError> {
        crate::diff::capture(self, worktree)
    }

    fn destroy(&self, worktree: &Worktree) -> Result<(), WorktreeError> {
        crate::cleanup::destroy(self, worktree)
    }

    fn recover_interrupted_create(
        &self,
        labels: &OwnershipLabels,
        repository_root: &Path,
    ) -> Result<(), WorktreeError> {
        validate_execution_id(&labels.execution_id)?;
        let expected = self.execution_repository_path(&labels.execution_id);
        if repository_root != expected {
            return Err(WorktreeError::Ownership(format!(
                "interrupted create path {} is not exact expected path {}",
                repository_root.display(),
                expected.display()
            )));
        }
        let intent_path = create_intent_path(self, &labels.execution_id);
        let Some(intent) = read_create_intent(self, &intent_path)? else {
            return Ok(());
        };
        verify_repository_selector(
            self,
            &ExecutionLayout::new(&self.cache_root, &labels.execution_id)
                .map_err(|error| WorktreeError::Ownership(error.to_string()))?,
            repository_root,
        )?;
        if intent.labels != labels.to_map()
            || Path::new(&intent.repository_path) != repository_root
            || intent.repository != labels.repository
        {
            return Err(WorktreeError::Ownership(
                "create intent does not match exact execution labels and root".to_owned(),
            ));
        }
        rollback_independent_repository(self.filesystem.as_ref(), repository_root)
            .map_err(WorktreeError::Cleanup)?;
        remove_create_intent(self, &intent_path)
            .map_err(|error| WorktreeError::Cleanup(error.to_string()))
    }

    fn find_stale(&self, live: &[ExecutionId]) -> Result<Vec<Worktree>, WorktreeError> {
        crate::cleanup::find_stale(self, live)
    }
}

impl GitWorktreeManager {
    fn branch_owner(&self, repo: &str, branch: &str) -> Result<Option<ExecutionId>, WorktreeError> {
        let entries = match fs::read_dir(self.executions_root()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(WorktreeError::Create(error.to_string())),
        };
        for entry in entries {
            let execution_root = entry
                .map_err(|error| WorktreeError::Create(error.to_string()))?
                .path();
            let metadata = fs::symlink_metadata(&execution_root)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            if !metadata.file_type().is_dir() {
                return Err(WorktreeError::Ownership(format!(
                    "execution root is not a real directory: {}",
                    execution_root.display()
                )));
            }
            let path = execution_root.join("repository");
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(WorktreeError::Create(error.to_string())),
            };
            if !metadata.file_type().is_dir() {
                return Err(WorktreeError::Ownership(format!(
                    "repository entry is not a real directory: {}",
                    path.display()
                )));
            }
            let Ok(record) = read_owner_record(&path) else {
                continue;
            };
            if record.labels.get(REPOSITORY).map(String::as_str) != Some(repo) {
                continue;
            }
            if record.branch != branch {
                continue;
            }
            verify_git_storage_preflight(&path)?;
            let current = repository_git_stdout(
                &path,
                [OsStr::new("branch"), OsStr::new("--show-current")],
                WorktreeError::Create,
            )?;
            if current != record.branch {
                return Err(WorktreeError::Ownership(format!(
                    "recorded branch {} does not match checkout {current} at {}",
                    record.branch,
                    path.display()
                )));
            }
            if let Some(execution_id) = record.labels.get(EXECUTION_ID) {
                return Ok(Some(ExecutionId::new(execution_id)));
            }
        }
        Ok(None)
    }
}

fn secure_mirror_root(cache_root: &Path) -> Result<MirrorRoot, WorktreeError> {
    let cache_metadata = fs::symlink_metadata(cache_root)
        .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    if !cache_metadata.file_type().is_dir() {
        return Err(WorktreeError::Mirror(format!(
            "cache root is not a real directory: {}",
            cache_root.display()
        )));
    }
    let root = cache_root.join("mirrors");
    match fs::symlink_metadata(&root) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(WorktreeError::Mirror(format!(
                "mirror root is not a real directory: {}",
                root.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&root).map_err(|error| WorktreeError::Mirror(error.to_string()))?;
            #[cfg(unix)]
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        }
        Err(error) => return Err(WorktreeError::Mirror(error.to_string())),
    }
    let path = root
        .canonicalize()
        .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    let metadata =
        fs::symlink_metadata(&path).map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    let handle = File::open(&path).map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    let opened = handle
        .metadata()
        .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    #[cfg(unix)]
    if metadata.uid() != cache_metadata.uid() || metadata.mode() & 0o077 != 0 {
        return Err(WorktreeError::Mirror(
            "mirror root owner or mode does not match secure cache root".to_owned(),
        ));
    }
    #[cfg(unix)]
    let root = MirrorRoot {
        path,
        handle,
        device: opened.dev(),
        inode: opened.ino(),
        owner: opened.uid(),
    };
    #[cfg(not(unix))]
    let root = MirrorRoot { path, handle };
    root.verify()?;
    Ok(root)
}

fn verify_mirror_repository(
    root: &MirrorRoot,
    mirror: &Path,
    clone_locator: &str,
) -> Result<(), WorktreeError> {
    root.verify()?;
    let metadata =
        fs::symlink_metadata(mirror).map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    if !metadata.file_type().is_dir() {
        return Err(WorktreeError::Mirror(format!(
            "mirror is not a real directory: {}",
            mirror.display()
        )));
    }
    let canonical = mirror
        .canonicalize()
        .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    if canonical.parent() != Some(root.path.as_path()) {
        return Err(WorktreeError::Mirror(format!(
            "mirror escaped pinned root: {}",
            canonical.display()
        )));
    }
    #[cfg(unix)]
    if metadata.uid() != root.owner {
        return Err(WorktreeError::Mirror(
            "mirror owner differs from mirror root owner".to_owned(),
        ));
    }
    let bare = git_stdout(
        [
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("rev-parse"),
            OsStr::new("--is-bare-repository"),
        ],
        WorktreeError::Mirror,
    )?;
    if bare != "true" {
        return Err(WorktreeError::Mirror(format!(
            "mirror is not bare: {}",
            mirror.display()
        )));
    }
    let actual_origin = git_stdout(
        [
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("remote"),
            OsStr::new("get-url"),
            OsStr::new("origin"),
        ],
        WorktreeError::Mirror,
    )?;
    if actual_origin != clone_locator {
        return Err(WorktreeError::Mirror(format!(
            "mirror origin mismatch: expected {clone_locator}, found {actual_origin}"
        )));
    }
    Ok(())
}

fn verify_mirror_storage_preflight(root: &MirrorRoot, mirror: &Path) -> Result<(), WorktreeError> {
    root.verify()?;
    let canonical_mirror = mirror
        .canonicalize()
        .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    if canonical_mirror.parent() != Some(root.path.as_path()) {
        return Err(WorktreeError::Mirror(format!(
            "mirror escaped pinned root: {}",
            canonical_mirror.display()
        )));
    }
    let objects = mirror.join("objects");
    let metadata =
        fs::symlink_metadata(&objects).map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(WorktreeError::Mirror(format!(
            "mirror object database is not a real directory: {}",
            objects.display()
        )));
    }
    let canonical_objects = objects
        .canonicalize()
        .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
    if canonical_objects.parent() != Some(canonical_mirror.as_path()) {
        return Err(WorktreeError::Mirror(format!(
            "mirror object database escaped its repository: {}",
            canonical_objects.display()
        )));
    }
    for name in ["alternates", "http-alternates"] {
        let alternate = objects.join("info").join(name);
        match fs::symlink_metadata(&alternate) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(WorktreeError::Mirror(format!(
                    "mirror alternate object database is forbidden: {}",
                    alternate.display()
                )))
            }
            Err(error) => return Err(WorktreeError::Mirror(error.to_string())),
        }
    }
    Ok(())
}

pub(crate) fn normalized_repository_name(repo: &str) -> Result<String, WorktreeError> {
    let (owner, name) = canonical_repository(repo)?;
    Ok(format!("{owner}__{name}"))
}

fn canonical_repository(repo: &str) -> Result<(&str, &str), WorktreeError> {
    let mut components = repo.split('/');
    match (components.next(), components.next(), components.next()) {
        (Some(owner), Some(name), None)
            if is_safe_component(owner)
                && !owner.ends_with('_')
                && is_safe_component(name)
                && !name.starts_with('_') =>
        {
            Ok((owner, name))
        }
        _ => Err(WorktreeError::InvalidRepository(repo.to_owned())),
    }
}

fn validate_execution_id(execution_id: &ExecutionId) -> Result<(), WorktreeError> {
    let value = execution_id.as_str();
    if !value.is_empty()
        && value.len() <= 63
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || (index > 0 && byte == b'-')
        })
    {
        Ok(())
    } else {
        Err(WorktreeError::Create(format!(
            "invalid execution_id: {value}"
        )))
    }
}

fn validate_branch(branch: &str) -> Result<(), WorktreeError> {
    run_git(
        [
            OsStr::new("check-ref-format"),
            OsStr::new("--branch"),
            OsStr::new(branch),
        ],
        WorktreeError::Create,
    )?;
    Ok(())
}

pub(crate) fn hex_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn write_owner_record(
    filesystem: &dyn WorktreeFilesystem,
    path: &Path,
    record: &OwnerRecord,
) -> Result<(), WorktreeError> {
    let bytes = serde_json::to_vec_pretty(record)
        .map_err(|error| WorktreeError::Create(error.to_string()))?;
    filesystem
        .write_owner_record(path, &bytes)
        .map_err(create_io_error)
}

fn create_io_error(error: std::io::Error) -> WorktreeError {
    if error.raw_os_error() == Some(28) {
        WorktreeError::StorageFull(error.to_string())
    } else {
        WorktreeError::Create(error.to_string())
    }
}

fn create_intent_path(manager: &GitWorktreeManager, execution_id: &ExecutionId) -> PathBuf {
    manager
        .worktrees_root()
        .join(format!(".create-{execution_id}.json"))
}

fn read_create_intent(
    manager: &GitWorktreeManager,
    path: &Path,
) -> Result<Option<CreateIntent>, WorktreeError> {
    let bytes = metadata_directory(manager, WorktreeError::Create)?
        .read(metadata_name(path)?)
        .map_err(|error| WorktreeError::Create(error.to_string()))?;
    bytes
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(|error| WorktreeError::Create(error.to_string()))
        })
        .transpose()
}

fn write_create_intent(
    manager: &GitWorktreeManager,
    path: &Path,
    intent: &CreateIntent,
    replace: bool,
) -> Result<(), WorktreeError> {
    let bytes = serde_json::to_vec_pretty(intent)
        .map_err(|error| WorktreeError::Create(error.to_string()))?;
    let directory = metadata_directory(manager, WorktreeError::Create)?;
    if replace {
        directory
            .replace(metadata_name(path)?, &bytes)
            .map_err(|error| WorktreeError::Create(error.to_string()))
    } else {
        directory
            .create(metadata_name(path)?, &bytes)
            .map_err(|error| WorktreeError::Create(error.to_string()))
    }
}

fn remove_create_intent(manager: &GitWorktreeManager, path: &Path) -> Result<(), WorktreeError> {
    metadata_directory(manager, WorktreeError::Create)?
        .remove(metadata_name(path)?)
        .map_err(|error| WorktreeError::Create(error.to_string()))
}

pub(crate) fn metadata_directory(
    manager: &GitWorktreeManager,
    error: fn(String) -> WorktreeError,
) -> Result<SecureMetadataDirectory, WorktreeError> {
    let path = manager.worktrees_root();
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder
                    .create(&path)
                    .map_err(|cause| error(cause.to_string()))?;
            }
            #[cfg(not(unix))]
            fs::create_dir(&path).map_err(|cause| error(cause.to_string()))?;
        }
        Err(cause) => return Err(error(cause.to_string())),
    }
    SecureMetadataDirectory::new(&path).map_err(|cause| error(cause.to_string()))
}

pub(crate) fn metadata_name(path: &Path) -> Result<&str, WorktreeError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            WorktreeError::Ownership(format!("metadata name is invalid: {}", path.display()))
        })
}

fn verify_create_intent(
    intent: &CreateIntent,
    labels: &OwnershipLabels,
    repo: &str,
    base_ref: &str,
    branch: &str,
    path: &Path,
) -> Result<(), WorktreeError> {
    if intent.labels == labels.to_map()
        && intent.repository == repo
        && intent.base_ref == base_ref
        && intent.branch == branch
        && Path::new(&intent.repository_path) == path
    {
        Ok(())
    } else {
        Err(WorktreeError::Ownership(
            "create intent does not match requested execution".to_owned(),
        ))
    }
}

fn rollback_independent_repository(
    filesystem: &dyn WorktreeFilesystem,
    path: &Path,
) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => filesystem
            .create_repository_root(path)
            .map_err(|error| error.to_string()),
        Ok(metadata) if metadata.file_type().is_dir() => {
            if fs::read_dir(path)
                .map_err(|error| error.to_string())?
                .next()
                .is_none()
            {
                Ok(())
            } else {
                filesystem
                    .remove_repository(path)
                    .map_err(|error| error.to_string())?;
                filesystem
                    .create_repository_root(path)
                    .map_err(|error| error.to_string())
            }
        }
        Ok(_) => Err(format!(
            "repository rollback target is not a real directory: {}",
            path.display()
        )),
        Err(error) => Err(error.to_string()),
    }
}

fn verify_repository_selector(
    manager: &GitWorktreeManager,
    layout: &ExecutionLayout,
    repository_root: &Path,
) -> Result<(), WorktreeError> {
    if layout.root
        != manager.executions_root().join(
            layout
                .root
                .file_name()
                .ok_or_else(|| WorktreeError::Ownership("execution root has no name".to_owned()))?,
        )
        || repository_root != layout.root.join("repository")
    {
        return Err(WorktreeError::Ownership(
            "execution storage layout is not deterministic".to_owned(),
        ));
    }
    let root = fs::symlink_metadata(&layout.root)
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    if !root.file_type().is_dir() {
        return Err(WorktreeError::Ownership(format!(
            "execution root is not a real directory: {}",
            layout.root.display()
        )));
    }
    Ok(())
}

fn verify_live_storage(
    storage: &dyn VerifiedExecutionStorage,
    repository_root: &Path,
) -> Result<(), WorktreeError> {
    if storage.repository_path() != repository_root {
        return Err(WorktreeError::Ownership(format!(
            "verified repository {} is not {}",
            storage.repository_path().display(),
            repository_root.display()
        )));
    }
    storage
        .verify()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))
}

pub(crate) fn verify_repository_storage(path: &Path) -> Result<(), WorktreeError> {
    verify_git_storage_preflight(path)?;
    let canonical_root = path
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    let top_level = repository_git_stdout(
        path,
        [OsStr::new("rev-parse"), OsStr::new("--show-toplevel")],
        WorktreeError::Ownership,
    )?;
    let canonical_top_level = PathBuf::from(top_level)
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    if canonical_top_level != canonical_root {
        return Err(WorktreeError::Ownership(format!(
            "Git work tree {} is not repository root {}",
            canonical_top_level.display(),
            canonical_root.display()
        )));
    }
    for arguments in [
        &["rev-parse", "--git-dir"][..],
        &["rev-parse", "--git-common-dir"][..],
        &["rev-parse", "--git-path", "objects"][..],
        &["rev-parse", "--git-path", "refs"][..],
        &["rev-parse", "--git-path", "index"][..],
    ] {
        verify_git_storage_preflight(path)?;
        let reported = repository_git_stdout(
            path,
            arguments.iter().map(OsStr::new),
            WorktreeError::Ownership,
        )?;
        let reported = PathBuf::from(reported);
        let resolved = if reported.is_absolute() {
            reported
        } else {
            path.join(reported)
        };
        let canonical = match resolved.canonicalize() {
            Ok(canonical) => canonical,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = resolved.parent().ok_or_else(|| {
                    WorktreeError::Ownership(format!(
                        "Git path has no parent: {}",
                        resolved.display()
                    ))
                })?;
                parent
                    .canonicalize()
                    .map_err(|error| WorktreeError::Ownership(error.to_string()))?
                    .join(resolved.file_name().ok_or_else(|| {
                        WorktreeError::Ownership(format!(
                            "Git path has no file name: {}",
                            resolved.display()
                        ))
                    })?)
            }
            Err(error) => return Err(WorktreeError::Ownership(error.to_string())),
        };
        if !canonical.starts_with(&canonical_root) {
            return Err(WorktreeError::Ownership(format!(
                "Git path escapes repository root: {}",
                canonical.display()
            )));
        }
    }
    verify_git_storage_preflight(path)?;
    let alternates = PathBuf::from(repository_git_stdout(
        path,
        [
            OsStr::new("rev-parse"),
            OsStr::new("--git-path"),
            OsStr::new("objects/info/alternates"),
        ],
        WorktreeError::Ownership,
    )?);
    let alternates = if alternates.is_absolute() {
        alternates
    } else {
        path.join(alternates)
    };
    match fs::symlink_metadata(&alternates) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(WorktreeError::Ownership(format!(
            "alternate object database is forbidden: {}",
            alternates.display()
        ))),
        Err(error) => Err(WorktreeError::Ownership(error.to_string())),
    }
}

pub(crate) fn verify_git_storage_preflight(path: &Path) -> Result<(), WorktreeError> {
    let commondir = path.join(".git/commondir");
    match fs::symlink_metadata(&commondir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(WorktreeError::Ownership(format!(
                "linked Git common directory is forbidden: {}",
                commondir.display()
            )))
        }
        Err(error) => return Err(WorktreeError::Ownership(error.to_string())),
    }
    for (purpose, candidate, expected_directory) in [
        ("repository", path.to_path_buf(), true),
        ("Git directory", path.join(".git"), true),
        ("object database", path.join(".git/objects"), true),
        ("refs directory", path.join(".git/refs"), true),
    ] {
        let metadata = fs::symlink_metadata(&candidate)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        if metadata.file_type().is_symlink()
            || (expected_directory && !metadata.file_type().is_dir())
        {
            return Err(WorktreeError::Ownership(format!(
                "{purpose} is not a real directory: {}",
                candidate.display()
            )));
        }
    }
    let index = path.join(".git/index");
    match fs::symlink_metadata(&index) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(WorktreeError::Ownership(format!(
                "Git index is not a real file: {}",
                index.display()
            )))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(WorktreeError::Ownership(error.to_string())),
    }
    let alternates = path.join(".git/objects/info/alternates");
    match fs::symlink_metadata(&alternates) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(WorktreeError::Ownership(format!(
            "alternate object database is forbidden: {}",
            alternates.display()
        ))),
        Err(error) => Err(WorktreeError::Ownership(error.to_string())),
    }
}

pub(crate) fn read_owner_record(path: &Path) -> Result<OwnerRecord, WorktreeError> {
    let bytes = fs::read(path.join(".autospec-owner.json"))
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|error| WorktreeError::Cleanup(error.to_string()))
}

fn is_safe_component(component: &str) -> bool {
    !component.is_empty()
        && component != "."
        && component != ".."
        && !component.contains("__")
        && component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub(crate) fn run_git<I, S>(
    args: I,
    error: fn(String) -> WorktreeError,
) -> Result<Output, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = git_command().args(args).output().map_err(|cause| {
        if cause.raw_os_error() == Some(28) {
            WorktreeError::StorageFull(cause.to_string())
        } else {
            error(cause.to_string())
        }
    })?;
    if output.status.success() {
        Ok(output)
    } else if output.status.code() == Some(28)
        || String::from_utf8_lossy(&output.stderr).contains("No space left on device")
    {
        Err(WorktreeError::StorageFull(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    } else {
        Err(error(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    }
}

pub(crate) fn run_repository_git<I, S>(
    path: &Path,
    args: I,
    error: fn(String) -> WorktreeError,
) -> Result<Output, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    verify_git_storage_preflight(path)?;
    let output = repository_git_command(path)
        .args(args)
        .output()
        .map_err(|cause| {
            if cause.raw_os_error() == Some(28) {
                WorktreeError::StorageFull(cause.to_string())
            } else {
                error(cause.to_string())
            }
        })?;
    if output.status.success() {
        Ok(output)
    } else if output.status.code() == Some(28)
        || String::from_utf8_lossy(&output.stderr).contains("No space left on device")
    {
        Err(WorktreeError::StorageFull(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    } else {
        Err(error(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    }
}

pub(crate) fn repository_git_stdout<I, S>(
    path: &Path,
    args: I,
    error: fn(String) -> WorktreeError,
) -> Result<String, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = run_repository_git(path, args, error)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub(crate) fn git_stdout<I, S>(
    args: I,
    error: fn(String) -> WorktreeError,
) -> Result<String, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = run_git(args, error)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
