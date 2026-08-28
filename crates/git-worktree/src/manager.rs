use crate::filesystem::{SystemWorktreeFilesystem, WorktreeFilesystem};
use crate::lock::FileLock;
use crate::{DiffCapture, Worktree, WorktreeError, WorktreeManager};
use orchestrator_core::labels::{EXECUTION_ID, REPOSITORY};
use orchestrator_core::{ExecutionId, OwnershipLabels};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

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
        fs::create_dir_all(self.mirrors_root())
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        let clone_locator = self.clone_locator(repo)?;
        let mirror = self
            .mirrors_root()
            .join(format!("{}.git", normalized_repository_name(repo)?));
        let lock_path = mirror.with_extension("git.lock");
        let _lock = FileLock::acquire(&lock_path)?;

        if mirror.exists() {
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
        } else {
            run_git(
                [
                    OsStr::new("clone"),
                    OsStr::new("--mirror"),
                    OsStr::new(&clone_locator),
                    mirror.as_os_str(),
                ],
                WorktreeError::Mirror,
            )?;
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
        repository_root: &Path,
    ) -> Result<Worktree, WorktreeError> {
        validate_execution_id(&labels.execution_id)?;
        if labels.repository != repo {
            return Err(WorktreeError::Ownership(format!(
                "repository label {} does not match {repo}",
                labels.repository
            )));
        }
        validate_branch(branch)?;
        let expected = self.execution_repository_path(&labels.execution_id);
        if repository_root != expected {
            return Err(WorktreeError::Ownership(format!(
                "repository root {} does not match bounded path {}",
                repository_root.display(),
                expected.display()
            )));
        }
        verify_bounded_repository_root(self, &labels.execution_id, repository_root)?;
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

        let mirror = PathBuf::from(self.ensure_mirror(repo)?);
        let mirror_lock = mirror.with_extension("git.lock");
        let _mirror_lock = FileLock::acquire_with(&mirror_lock, WorktreeError::Create)?;
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
        let clone_locator = self.clone_locator(repo)?;
        let setup_result = (|| {
            self.filesystem
                .clone_repository(&mirror, repository_root)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
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
                Ok(()) => error,
                Err(rollback) => {
                    WorktreeError::Create(format!("{error}; rollback failed: {rollback}"))
                }
            });
        }

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
    execution_id: &ExecutionId,
    repository_root: &Path,
) -> Result<(), WorktreeError> {
    for (description, path) in [
        ("executions root", manager.executions_root()),
        (
            "execution root",
            manager.executions_root().join(execution_id.as_str()),
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
    Ok(())
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
    let output = Command::new("git")
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
