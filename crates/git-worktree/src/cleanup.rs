use crate::lock::FileLock;
use crate::manager::{
    git_stdout, hex_component, normalized_repository_name, read_owner_record, run_git,
    GitWorktreeManager, OwnerRecord,
};
use crate::{Worktree, WorktreeError};
use orchestrator_core::labels::{EXECUTION_ID, MANAGED, REPOSITORY};
use orchestrator_core::ExecutionId;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn destroy(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<(), WorktreeError> {
    let path = verified_path(manager, worktree)?;
    let record = verified_owner(&path, worktree)?;
    let repository = required_label(&record, REPOSITORY)?;
    let current_branch = current_branch(&path)?;
    if current_branch != worktree.branch {
        return Err(WorktreeError::Ownership(format!(
            "branch mismatch at {}",
            path.display()
        )));
    }

    let normalized = normalized_repository_name(repository)?;
    let mirror = manager.mirrors_root().join(format!("{normalized}.git"));
    if !mirror.is_dir() {
        return Err(WorktreeError::Ownership(format!(
            "missing owned mirror {}",
            mirror.display()
        )));
    }
    let mirror_lock = mirror.with_extension("git.lock");
    let _mirror_lock = FileLock::acquire_with(&mirror_lock, WorktreeError::Cleanup)?;
    let lock_path = manager.mirrors_root().join(format!(
        "{normalized}.branch-{}.lock",
        hex_component(&worktree.branch)
    ));
    let _lock = FileLock::acquire_with(&lock_path, WorktreeError::Cleanup)?;

    run_git(
        [
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("worktree"),
            OsStr::new("remove"),
            OsStr::new("--force"),
            path.as_os_str(),
        ],
        WorktreeError::Cleanup,
    )?;
    run_git(
        [
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("branch"),
            OsStr::new("-D"),
            OsStr::new(&worktree.branch),
        ],
        WorktreeError::Cleanup,
    )?;
    Ok(())
}

pub(crate) fn find_stale(
    manager: &GitWorktreeManager,
    live: &[ExecutionId],
) -> Result<Vec<Worktree>, WorktreeError> {
    let live: BTreeSet<&ExecutionId> = live.iter().collect();
    let entries = match fs::read_dir(manager.worktrees_root()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(WorktreeError::Cleanup(error.to_string())),
    };
    let mut stale = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| WorktreeError::Cleanup(error.to_string()))?
            .path();
        if !path.is_dir() {
            continue;
        }
        let record = match read_owner_record(&path) {
            Ok(record) => record,
            Err(_) => continue,
        };
        if record.labels.get(MANAGED).map(String::as_str) != Some("true") {
            continue;
        }
        let execution_id = ExecutionId::new(required_label(&record, EXECUTION_ID)?);
        if live.contains(&execution_id) {
            continue;
        }
        let expected = manager.worktrees_root().join(execution_id.as_str());
        if path != expected {
            continue;
        }
        stale.push(Worktree {
            execution_id,
            path: path.to_string_lossy().into_owned(),
            branch: current_branch(&path)?,
            base_sha: record.base_sha,
        });
    }
    stale.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
    Ok(stale)
}

pub(crate) fn verified_path(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<PathBuf, WorktreeError> {
    let expected = manager
        .worktrees_root()
        .join(worktree.execution_id.as_str());
    let is_real_directory = fs::symlink_metadata(&expected)
        .map(|metadata| metadata.file_type().is_dir())
        .unwrap_or(false);
    if Path::new(&worktree.path) == expected && is_real_directory {
        Ok(expected)
    } else {
        Err(WorktreeError::Ownership(worktree.path.clone()))
    }
}

fn verified_owner(path: &Path, worktree: &Worktree) -> Result<OwnerRecord, WorktreeError> {
    let record = read_owner_record(path)?;
    if record.labels.get(MANAGED).map(String::as_str) == Some("true")
        && record.labels.get(EXECUTION_ID).map(String::as_str)
            == Some(worktree.execution_id.as_str())
        && record.base_sha == worktree.base_sha
    {
        Ok(record)
    } else {
        Err(WorktreeError::Ownership(worktree.path.clone()))
    }
}

fn required_label<'a>(record: &'a OwnerRecord, key: &str) -> Result<&'a str, WorktreeError> {
    record
        .labels
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| WorktreeError::Ownership(format!("owner record missing {key}")))
}

fn current_branch(path: &Path) -> Result<String, WorktreeError> {
    git_stdout(
        [
            OsStr::new("-C"),
            path.as_os_str(),
            OsStr::new("branch"),
            OsStr::new("--show-current"),
        ],
        WorktreeError::Cleanup,
    )
}
