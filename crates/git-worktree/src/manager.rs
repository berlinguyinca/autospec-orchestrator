use crate::command::git_command;
use crate::filesystem::{SystemWorktreeFilesystem, WorktreeFilesystem, WorktreeFilesystemPoint};
use crate::lock::FileLock;
use crate::{DiffCapture, Worktree, WorktreeError, WorktreeManager};
use execution_storage::{AllocationReceipt, ExecutionLayout};
use orchestrator_core::labels::{EXECUTION_ID, REPOSITORY};
use orchestrator_core::{ExecutionId, OwnershipLabels};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
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

struct BoundedStorage {
    root: PathBuf,
    repository: PathBuf,
    root_handle: File,
    repository_handle: File,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    root_inode: u64,
    #[cfg(unix)]
    repository_inode: u64,
}

impl BoundedStorage {
    fn verify(&self) -> Result<(), WorktreeError> {
        let root = fs::symlink_metadata(&self.root)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        let repository = fs::symlink_metadata(&self.repository)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        if !root.file_type().is_dir() || !repository.file_type().is_dir() {
            return Err(WorktreeError::Ownership(
                "execution storage ancestry is no longer real directories".to_owned(),
            ));
        }
        let opened_root = self
            .root_handle
            .metadata()
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        let opened_repository = self
            .repository_handle
            .metadata()
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        #[cfg(unix)]
        if root.dev() != self.device
            || repository.dev() != self.device
            || root.ino() != self.root_inode
            || repository.ino() != self.repository_inode
            || opened_root.dev() != self.device
            || opened_repository.dev() != self.device
            || opened_root.ino() != self.root_inode
            || opened_repository.ino() != self.repository_inode
        {
            return Err(WorktreeError::Ownership(
                "execution storage filesystem or inode identity changed".to_owned(),
            ));
        }
        Ok(())
    }
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

    pub fn with_clone_base_and_filesystem(
        cache_root: impl Into<PathBuf>,
        clone_base: impl Into<String>,
        filesystem: Arc<dyn WorktreeFilesystem>,
    ) -> Self {
        Self {
            cache_root: cache_root.into(),
            clone_base: clone_base.into().trim_end_matches('/').to_owned(),
            filesystem,
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
        verify_bounded_repository_root(self, &layout, repository_root)?;

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
        let base_sha = git_stdout(
            [
                OsStr::new("--git-dir"),
                mirror.as_os_str(),
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new(base_ref),
            ],
            WorktreeError::Create,
        )?;
        let intent_path = create_intent_path(self, &labels.execution_id);
        if let Some(existing) = read_create_intent(&intent_path)? {
            verify_create_intent(&existing, labels, repo, base_ref, branch, repository_root)?;
            rollback_independent_repository(self.filesystem.as_ref(), repository_root)
                .map_err(WorktreeError::Create)?;
            remove_create_intent(self, &intent_path)?;
        }
        let bounded_storage = verify_bounded_repository_root(self, &layout, repository_root)?;
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
        write_create_intent(self, &intent_path, &intent, false)?;
        let setup_result = (|| {
            bounded_storage.verify()?;
            mirror_root
                .verify()
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            verify_mirror_repository(&mirror_root, &mirror, &clone_locator)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            self.filesystem
                .clone_repository(&mirror, repository_root)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            self.filesystem
                .checkpoint(
                    WorktreeFilesystemPoint::CloneObjectPack,
                    &repository_root.join(".git/objects/pack"),
                )
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            verify_repository_storage(repository_root)?;
            bounded_storage.verify()?;
            intent.phase = CreatePhase::Cloned;
            write_create_intent(self, &intent_path, &intent, true)?;
            run_git(
                [
                    OsStr::new("-C"),
                    repository_root.as_os_str(),
                    OsStr::new("remote"),
                    OsStr::new("set-url"),
                    OsStr::new("origin"),
                    OsStr::new(&clone_locator),
                ],
                WorktreeError::Create,
            )?;
            run_git(
                [
                    OsStr::new("-C"),
                    repository_root.as_os_str(),
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
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            intent.phase = CreatePhase::CheckedOut;
            write_create_intent(self, &intent_path, &intent, true)?;
            verify_repository_storage(repository_root)?;
            bounded_storage.verify()?;
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
            let current = git_stdout(
                [
                    OsStr::new("-C"),
                    path.as_os_str(),
                    OsStr::new("branch"),
                    OsStr::new("--show-current"),
                ],
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
        .map_err(|error| WorktreeError::Create(error.to_string()))
}

pub(crate) fn atomic_write_new(
    directory: &Path,
    final_path: &Path,
    temporary_path: &Path,
    bytes: &[u8],
    error: fn(String) -> WorktreeError,
) -> Result<(), WorktreeError> {
    reject_existing_path(final_path, error)?;
    reject_existing_path(temporary_path, error)?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary_path)
            .map_err(|cause| error(cause.to_string()))?;
        file.write_all(bytes)
            .map_err(|cause| error(cause.to_string()))?;
        file.sync_all().map_err(|cause| error(cause.to_string()))?;
        fs::rename(temporary_path, final_path).map_err(|cause| error(cause.to_string()))?;
        File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|cause| error(cause.to_string()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary_path);
    }
    result
}

fn create_intent_path(manager: &GitWorktreeManager, execution_id: &ExecutionId) -> PathBuf {
    manager
        .worktrees_root()
        .join(format!(".create-{execution_id}.json"))
}

fn read_create_intent(path: &Path) -> Result<Option<CreateIntent>, WorktreeError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WorktreeError::Create(error.to_string())),
    };
    if !metadata.file_type().is_file() {
        return Err(WorktreeError::Ownership(format!(
            "create intent is not a regular file: {}",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|error| WorktreeError::Create(error.to_string()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| WorktreeError::Create(error.to_string()))
}

fn write_create_intent(
    manager: &GitWorktreeManager,
    path: &Path,
    intent: &CreateIntent,
    replace: bool,
) -> Result<(), WorktreeError> {
    fs::create_dir_all(manager.worktrees_root())
        .map_err(|error| WorktreeError::Create(error.to_string()))?;
    let temporary = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(intent)
        .map_err(|error| WorktreeError::Create(error.to_string()))?;
    if replace {
        reject_existing_path(&temporary, WorktreeError::Create)?;
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            file.write_all(&bytes)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            file.sync_all()
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            fs::rename(&temporary, path)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
            File::open(manager.worktrees_root())
                .and_then(|directory| directory.sync_all())
                .map_err(|error| WorktreeError::Create(error.to_string()))
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    } else {
        atomic_write_new(
            &manager.worktrees_root(),
            path,
            &temporary,
            &bytes,
            WorktreeError::Create,
        )
    }
}

fn remove_create_intent(manager: &GitWorktreeManager, path: &Path) -> Result<(), WorktreeError> {
    fs::remove_file(path).map_err(|error| WorktreeError::Create(error.to_string()))?;
    File::open(manager.worktrees_root())
        .and_then(|directory| directory.sync_all())
        .map_err(|error| WorktreeError::Create(error.to_string()))
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

fn reject_existing_path(
    path: &Path,
    error: fn(String) -> WorktreeError,
) -> Result<(), WorktreeError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(error(format!(
            "ownership metadata path already exists: {}",
            path.display()
        ))),
        Err(cause) => Err(error(cause.to_string())),
    }
}

fn rollback_independent_repository(
    filesystem: &dyn WorktreeFilesystem,
    path: &Path,
) -> Result<(), String> {
    filesystem
        .remove_repository(path)
        .map_err(|error| error.to_string())?;
    filesystem
        .create_repository_root(path)
        .map_err(|error| error.to_string())
}

fn verify_bounded_repository_root(
    manager: &GitWorktreeManager,
    layout: &ExecutionLayout,
    repository_root: &Path,
) -> Result<BoundedStorage, WorktreeError> {
    for (description, path) in [
        ("executions root", manager.executions_root()),
        (
            "execution root",
            manager
                .executions_root()
                .join(layout.root.file_name().ok_or_else(|| {
                    WorktreeError::Ownership("execution root has no name".to_owned())
                })?),
        ),
        ("repository root", repository_root.to_path_buf()),
    ] {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| WorktreeError::Create(error.to_string()))?;
        if !metadata.file_type().is_dir() {
            return Err(WorktreeError::Ownership(format!(
                "{description} is not a real directory: {}",
                path.display()
            )));
        }
    }
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
    let root = layout
        .root
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    let repository = repository_root
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    if repository.parent() != Some(root.as_path()) {
        return Err(WorktreeError::Ownership(
            "repository is not a direct child of the execution mount".to_owned(),
        ));
    }
    let root_handle =
        File::open(&root).map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    let repository_handle =
        File::open(&repository).map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    let root_metadata = root_handle
        .metadata()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    let repository_metadata = repository_handle
        .metadata()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    #[cfg(unix)]
    if root_metadata.dev() != repository_metadata.dev() {
        return Err(WorktreeError::Ownership(
            "repository is not on the execution mount filesystem".to_owned(),
        ));
    }
    #[cfg(unix)]
    let storage = BoundedStorage {
        root,
        repository,
        root_handle,
        repository_handle,
        device: root_metadata.dev(),
        root_inode: root_metadata.ino(),
        repository_inode: repository_metadata.ino(),
    };
    #[cfg(not(unix))]
    let storage = BoundedStorage {
        root,
        repository,
        root_handle,
        repository_handle,
    };
    storage.verify()?;
    Ok(storage)
}

pub(crate) fn verify_repository_storage(path: &Path) -> Result<(), WorktreeError> {
    let canonical_root = path
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    for arguments in [
        &["rev-parse", "--git-dir"][..],
        &["rev-parse", "--git-common-dir"][..],
        &["rev-parse", "--git-path", "objects"][..],
        &["rev-parse", "--git-path", "refs"][..],
        &["rev-parse", "--git-path", "index"][..],
    ] {
        let reported = git_stdout(
            std::iter::once(OsStr::new("-C"))
                .chain(std::iter::once(path.as_os_str()))
                .chain(arguments.iter().map(OsStr::new)),
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
    let alternates = PathBuf::from(git_stdout(
        [
            OsStr::new("-C"),
            path.as_os_str(),
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
    let output = git_command()
        .args(args)
        .output()
        .map_err(|cause| error(cause.to_string()))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(error(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    }
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
