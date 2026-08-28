use crate::lock::FileLock;
use crate::manager::{
    atomic_write_new, git_stdout, hex_component, normalized_repository_name, read_owner_record,
    run_git, GitWorktreeManager, OwnerRecord,
};
use crate::{Worktree, WorktreeError};
use orchestrator_core::labels::{EXECUTION_ID, MANAGED, REPOSITORY};
use orchestrator_core::ExecutionId;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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
    let existing_journal = read_cleanup_journal(&journal_path)?;
    let (journal, needs_write) = match existing_journal {
        Some(journal) => {
            verify_journal(manager, worktree, &journal)?;
            (journal, false)
        }
        None => {
            let path = verified_path(manager, worktree)?;
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

    if needs_write {
        write_cleanup_journal(manager, &journal_path, &journal)?;
    }

    let path = Path::new(&journal.worktree_path);
    let mut errors = Vec::new();
    if let Err(error) = remove_worktree_if_present(&mirror, path, &journal.owner.branch) {
        errors.push(format!("remove worktree: {error}"));
    }
    match branch_exists(&mirror, &journal.owner.branch) {
        Ok(true) => {
            if let Err(error) = run_git(
                [
                    OsStr::new("--git-dir"),
                    mirror.as_os_str(),
                    OsStr::new("branch"),
                    OsStr::new("-D"),
                    OsStr::new(&journal.owner.branch),
                ],
                WorktreeError::Cleanup,
            ) {
                errors.push(format!("delete branch: {error}"));
            }
        }
        Ok(false) => {}
        Err(error) => errors.push(format!("inspect branch: {error}")),
    }
    if errors.is_empty() {
        fs::remove_file(&journal_path).map_err(|error| WorktreeError::Cleanup(error.to_string()))
    } else {
        Err(WorktreeError::Cleanup(errors.join("; ")))
    }
}

fn cleanup_journal_path(manager: &GitWorktreeManager, execution_id: &ExecutionId) -> PathBuf {
    manager
        .worktrees_root()
        .join(format!(".cleanup-{execution_id}.json"))
}

fn read_cleanup_journal(path: &Path) -> Result<Option<CleanupJournal>, WorktreeError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WorktreeError::Cleanup(error.to_string())),
    };
    if !metadata.file_type().is_file() {
        return Err(WorktreeError::Ownership(format!(
            "cleanup journal is not a regular file: {}",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))
}

fn write_cleanup_journal(
    manager: &GitWorktreeManager,
    final_path: &Path,
    journal: &CleanupJournal,
) -> Result<(), WorktreeError> {
    let temporary_path = final_path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
    atomic_write_new(
        &manager.worktrees_root(),
        final_path,
        &temporary_path,
        &bytes,
        WorktreeError::Cleanup,
    )
}

fn verify_journal(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
    journal: &CleanupJournal,
) -> Result<(), WorktreeError> {
    let expected = manager
        .worktrees_root()
        .join(worktree.execution_id.as_str());
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

fn remove_worktree_if_present(
    mirror: &Path,
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
            "worktree path is not a real directory: {}",
            path.display()
        )));
    }
    let current = current_branch(path)?;
    if current != branch {
        return Err(WorktreeError::Ownership(format!(
            "recorded branch {branch} does not match checkout {current} at {}",
            path.display()
        )));
    }
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
    Ok(())
}

fn branch_exists(mirror: &Path, branch: &str) -> Result<bool, WorktreeError> {
    let reference = format!("refs/heads/{branch}");
    let output = Command::new("git")
        .args([
            OsStr::new("--git-dir"),
            mirror.as_os_str(),
            OsStr::new("show-ref"),
            OsStr::new("--verify"),
            OsStr::new("--quiet"),
            OsStr::new(&reference),
        ])
        .output()
        .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(WorktreeError::Cleanup(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )),
    }
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
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| WorktreeError::Cleanup(error.to_string()))?;
        if metadata.file_type().is_symlink() {
            return Err(WorktreeError::Ownership(format!(
                "symlinked worktree entry: {}",
                path.display()
            )));
        }
        if !metadata.file_type().is_dir() {
            continue;
        }
        let record = read_owner_record(&path)?;
        if record.labels.get(MANAGED).map(String::as_str) != Some("true") {
            continue;
        }
        let execution_id = ExecutionId::new(required_label(&record, EXECUTION_ID)?);
        let expected = manager.worktrees_root().join(execution_id.as_str());
        if path != expected {
            return Err(WorktreeError::Ownership(format!(
                "owner record path mismatch: {}",
                path.display()
            )));
        }
        if live.contains(&execution_id) {
            continue;
        }
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
