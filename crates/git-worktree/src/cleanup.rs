use crate::lock::FileLock;
use crate::manager::{
    hex_component, metadata_directory, metadata_name, normalized_repository_name,
    read_owner_record, repository_git_stdout, verify_git_storage_preflight,
    verify_repository_storage, GitWorktreeManager, OwnerRecord,
};
use crate::{Worktree, WorktreeError};
use orchestrator_core::labels::{EXECUTION_ID, MANAGED, REPOSITORY};
use orchestrator_core::ExecutionId;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CleanupJournal {
    owner: OwnerRecord,
    worktree_path: String,
}

pub(crate) fn destroy(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<(), WorktreeError> {
    let journal_path = cleanup_journal_path(manager, &worktree.execution_id);
    let existing_journal = read_cleanup_journal(manager, &journal_path)?;
    let (journal, needs_write) = match existing_journal {
        Some(journal) => {
            verify_journal(manager, worktree, &journal)?;
            (journal, false)
        }
        None => {
            let path = verified_path(manager, worktree)?;
            verify_repository_storage(&path)?;
            let owner = verified_owner(&path, worktree)?;
            let current = current_branch(&path)?;
            if current != owner.branch {
                return Err(WorktreeError::Ownership(format!(
                    "recorded branch {} does not match checkout {current} at {}",
                    owner.branch,
                    path.display()
                )));
            }
            (
                CleanupJournal {
                    owner,
                    worktree_path: worktree.path.clone(),
                },
                true,
            )
        }
    };
    let repository = required_label(&journal.owner, REPOSITORY)?;
    let normalized = normalized_repository_name(repository)?;
    let lock_path = manager.mirrors_root().join(format!(
        "{normalized}.branch-{}.lock",
        hex_component(&worktree.branch)
    ));
    let _lock = FileLock::acquire_with(&lock_path, WorktreeError::Cleanup)?;

    if needs_write {
        write_cleanup_journal(manager, &journal_path, &journal)?;
    }

    let path = Path::new(&journal.worktree_path);
    remove_repository_if_present(manager, path, &journal.owner.branch)?;
    metadata_directory(manager, WorktreeError::Cleanup)?
        .remove(metadata_name(&journal_path)?)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))
}

fn cleanup_journal_path(manager: &GitWorktreeManager, execution_id: &ExecutionId) -> PathBuf {
    manager
        .worktrees_root()
        .join(format!(".cleanup-{execution_id}.json"))
}

fn read_cleanup_journal(
    manager: &GitWorktreeManager,
    path: &Path,
) -> Result<Option<CleanupJournal>, WorktreeError> {
    let bytes = metadata_directory(manager, WorktreeError::Cleanup)?
        .read(metadata_name(path)?)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
    bytes
        .map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| WorktreeError::Cleanup(error.to_string()))
        })
        .transpose()
}

fn write_cleanup_journal(
    manager: &GitWorktreeManager,
    final_path: &Path,
    journal: &CleanupJournal,
) -> Result<(), WorktreeError> {
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
    metadata_directory(manager, WorktreeError::Cleanup)?
        .create(metadata_name(final_path)?, &bytes)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))
}

fn verify_journal(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
    journal: &CleanupJournal,
) -> Result<(), WorktreeError> {
    let expected = manager.execution_repository_path(&worktree.execution_id);
    if journal.worktree_path == worktree.path
        && Path::new(&journal.worktree_path) == expected
        && journal.owner.labels.get(MANAGED).map(String::as_str) == Some("true")
        && journal.owner.labels.get(EXECUTION_ID).map(String::as_str)
            == Some(worktree.execution_id.as_str())
        && journal.owner.base_sha == worktree.base_sha
        && journal.owner.branch == worktree.branch
        && journal.owner.labels.get(REPOSITORY).map(String::as_str)
            == Some(worktree.repository.as_str())
    {
        Ok(())
    } else {
        Err(WorktreeError::Ownership(format!(
            "cleanup journal does not match {}",
            worktree.execution_id
        )))
    }
}

fn remove_repository_if_present(
    manager: &GitWorktreeManager,
    path: &Path,
    branch: &str,
) -> Result<(), WorktreeError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(WorktreeError::Cleanup(error.to_string())),
    };
    if !metadata.file_type().is_dir() {
        return Err(WorktreeError::Ownership(format!(
            "repository path is not a real directory: {}",
            path.display()
        )));
    }
    verify_repository_storage(path)?;
    let current = current_branch(path)?;
    if current != branch {
        return Err(WorktreeError::Ownership(format!(
            "recorded branch {branch} does not match checkout {current} at {}",
            path.display()
        )));
    }
    manager
        .filesystem
        .remove_repository(path)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))
}

pub(crate) fn find_stale(
    manager: &GitWorktreeManager,
    live: &[ExecutionId],
) -> Result<Vec<Worktree>, WorktreeError> {
    let live: BTreeSet<&ExecutionId> = live.iter().collect();
    let entries = match fs::read_dir(manager.executions_root()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(WorktreeError::Cleanup(error.to_string())),
    };
    let mut stale = Vec::new();
    for entry in entries {
        let execution_root = entry
            .map_err(|error| WorktreeError::Cleanup(error.to_string()))?
            .path();
        let metadata = fs::symlink_metadata(&execution_root)
            .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
        if metadata.file_type().is_symlink() {
            return Err(WorktreeError::Ownership(format!(
                "symlinked execution entry: {}",
                execution_root.display()
            )));
        }
        if !metadata.file_type().is_dir() {
            continue;
        }
        let path = execution_root.join("repository");
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(WorktreeError::Cleanup(error.to_string())),
        };
        if !metadata.file_type().is_dir() {
            return Err(WorktreeError::Ownership(format!(
                "repository entry is not a real directory: {}",
                path.display()
            )));
        }
        let record = read_owner_record(&path)?;
        if record.labels.get(MANAGED).map(String::as_str) != Some("true") {
            continue;
        }
        let execution_id = ExecutionId::new(required_label(&record, EXECUTION_ID)?);
        let expected_execution_root = manager.executions_root().join(execution_id.as_str());
        if execution_root != expected_execution_root {
            return Err(WorktreeError::Ownership(format!(
                "owner record path mismatch: {}",
                path.display()
            )));
        }
        if live.contains(&execution_id) {
            continue;
        }
        verify_repository_storage(&path)?;
        let branch = current_branch(&path)?;
        if branch != record.branch {
            return Err(WorktreeError::Ownership(format!(
                "recorded branch {} does not match checkout {branch} at {}",
                record.branch,
                path.display()
            )));
        }
        let repository = required_label(&record, REPOSITORY)?.to_owned();
        normalized_repository_name(&repository)?;
        stale.push(Worktree {
            execution_id,
            path: path.to_string_lossy().into_owned(),
            branch: record.branch,
            base_sha: record.base_sha,
            repository,
        });
    }
    stale.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
    Ok(stale)
}

pub(crate) fn verified_path(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<PathBuf, WorktreeError> {
    let expected = manager.execution_repository_path(&worktree.execution_id);
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
        && record.branch == worktree.branch
        && record.labels.get(REPOSITORY).map(String::as_str) == Some(worktree.repository.as_str())
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
    verify_git_storage_preflight(path)?;
    repository_git_stdout(
        path,
        [OsStr::new("branch"), OsStr::new("--show-current")],
        WorktreeError::Cleanup,
    )
}
