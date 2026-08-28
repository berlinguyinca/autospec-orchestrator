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

#[derive(Debug, Clone)]
pub struct GitWorktreeManager {
    pub(crate) cache_root: PathBuf,
    clone_base: String,
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
        Self {
            cache_root: cache_root.into(),
            clone_base: clone_base.into().trim_end_matches('/').to_owned(),
        }
    }

    pub(crate) fn mirrors_root(&self) -> PathBuf {
        self.cache_root.join("mirrors")
    }

    pub(crate) fn worktrees_root(&self) -> PathBuf {
        self.cache_root.join("worktrees")
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
        let mirror = self
            .mirrors_root()
            .join(format!("{}.git", normalized_repository_name(repo)?));
        let lock_path = mirror.with_extension("git.lock");
        let _lock = FileLock::acquire(&lock_path)?;

        if mirror.exists() {
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
            let clone_locator = self.clone_locator(repo)?;
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
        labels: &OwnershipLabels,
        repo: &str,
        base_ref: &str,
        branch: &str,
    ) -> Result<Worktree, WorktreeError> {
        validate_execution_id(&labels.execution_id)?;
        if labels.repository != repo {
            return Err(WorktreeError::Ownership(format!(
                "repository label {} does not match {repo}",
                labels.repository
            )));
        }
        validate_branch(branch)?;
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
        let path = self.worktrees_root().join(labels.execution_id.as_str());
        if path.exists() {
            return Err(WorktreeError::Locked(labels.execution_id.clone()));
        }
        fs::create_dir_all(self.worktrees_root())
            .map_err(|error| WorktreeError::Create(error.to_string()))?;

        run_git(
            [
                OsStr::new("--git-dir"),
                mirror.as_os_str(),
                OsStr::new("worktree"),
                OsStr::new("add"),
                OsStr::new("-b"),
                OsStr::new(branch),
                path.as_os_str(),
                OsStr::new(&base_sha),
            ],
            WorktreeError::Create,
        )?;

        let record = OwnerRecord {
            labels: labels.to_map(),
            base_sha: base_sha.clone(),
            branch: branch.to_owned(),
        };
        if let Err(error) = write_owner_record(&path, &record) {
            let rollback_errors = rollback_created_worktree(&mirror, &path, branch);
            let mut message = error.to_string();
            if !rollback_errors.is_empty() {
                message.push_str("; rollback failed: ");
                message.push_str(&rollback_errors.join("; "));
            }
            return Err(WorktreeError::Create(message));
        }

        Ok(Worktree {
            execution_id: labels.execution_id.clone(),
            path: path.to_string_lossy().into_owned(),
            branch: branch.to_owned(),
            base_sha,
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
        let entries = match fs::read_dir(self.worktrees_root()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(WorktreeError::Create(error.to_string())),
        };
        for entry in entries {
            let path = entry
                .map_err(|error| WorktreeError::Create(error.to_string()))?
                .path();
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
        (Some(owner), Some(name), None) if is_safe_component(owner) && is_safe_component(name) => {
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

fn write_owner_record(path: &Path, record: &OwnerRecord) -> Result<(), WorktreeError> {
    let final_path = path.join(".autospec-owner.json");
    let temporary_path = path.join(".autospec-owner.json.tmp");
    let bytes = serde_json::to_vec_pretty(record)
        .map_err(|error| WorktreeError::Create(error.to_string()))?;
    atomic_write_new(
        path,
        &final_path,
        &temporary_path,
        &bytes,
        WorktreeError::Create,
    )
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

fn rollback_created_worktree(mirror: &Path, path: &Path, branch: &str) -> Vec<String> {
    let mut errors = Vec::new();
    if let Err(error) = run_git(
        [
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("worktree"),
            OsStr::new("remove"),
            OsStr::new("--force"),
            path.as_os_str(),
        ],
        WorktreeError::Cleanup,
    ) {
        errors.push(format!("remove worktree: {error}"));
    }
    if let Err(error) = run_git(
        [
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("branch"),
            OsStr::new("-D"),
            OsStr::new(branch),
        ],
        WorktreeError::Cleanup,
    ) {
        errors.push(format!("delete branch: {error}"));
    }
    errors
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
