use crate::cleanup::verified_path;
use crate::manager::read_owner_record;
use crate::manager::GitWorktreeManager;
use crate::{DiffCapture, Worktree, WorktreeError};
use orchestrator_core::labels::{EXECUTION_ID, MANAGED};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::process::{Command, Output};

pub(crate) fn capture(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<DiffCapture, WorktreeError> {
    let owned_path = verified_path(manager, worktree)?;
    let path = Path::new(&owned_path);
    let owner = read_owner_record(path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    if owner.labels.get(MANAGED).map(String::as_str) != Some("true")
        || owner.labels.get(EXECUTION_ID).map(String::as_str)
            != Some(worktree.execution_id.as_str())
        || owner.base_sha != worktree.base_sha
    {
        return Err(WorktreeError::Ownership(worktree.path.clone()));
    }

    let mut patch = git_success(
        path,
        [
            OsStr::new("diff"),
            OsStr::new("--binary"),
            OsStr::new(&worktree.base_sha),
            OsStr::new("--"),
        ],
    )?
    .stdout;
    let tracked = git_success(
        path,
        [
            OsStr::new("diff"),
            OsStr::new("--name-only"),
            OsStr::new("-z"),
            OsStr::new(&worktree.base_sha),
            OsStr::new("--"),
        ],
    )?
    .stdout;
    let untracked = git_success(
        path,
        [
            OsStr::new("ls-files"),
            OsStr::new("--others"),
            OsStr::new("--exclude-standard"),
            OsStr::new("-z"),
        ],
    )?
    .stdout;

    let mut changed_files = parse_paths(&tracked)?;
    for file in parse_paths(&untracked)? {
        let output = Command::new("git")
            .current_dir(path)
            .args([
                OsString::from("diff"),
                OsString::from("--no-index"),
                OsString::from("--binary"),
                OsString::from("--"),
                OsString::from("/dev/null"),
                OsString::from(&file),
            ])
            .output()
            .map_err(|error| WorktreeError::Diff(error.to_string()))?;
        if output.status.code() != Some(1) {
            return Err(WorktreeError::Diff(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        patch.extend_from_slice(&output.stdout);
        changed_files.insert(file);
    }

    Ok(DiffCapture {
        patch: String::from_utf8(patch).map_err(|error| WorktreeError::Diff(error.to_string()))?,
        changed_files: changed_files.into_iter().collect(),
    })
}

fn git_success<I, S>(path: &Path, args: I) -> Result<Output, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new("git")
        .current_dir(path)
        .args(args)
        .output()
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(WorktreeError::Diff(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    }
}

fn parse_paths(bytes: &[u8]) -> Result<BTreeSet<String>, WorktreeError> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .filter(|path| *path != b".autospec-owner.json")
        .map(|path| {
            String::from_utf8(path.to_vec()).map_err(|error| WorktreeError::Diff(error.to_string()))
        })
        .collect()
}
