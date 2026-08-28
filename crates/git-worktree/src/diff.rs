use crate::cleanup::verified_path;
use crate::command::{git_command, repository_git_command};
use crate::manager::{
    read_owner_record, verify_git_storage_preflight, verify_repository_storage, GitWorktreeManager,
};
use crate::{DiffCapture, Worktree, WorktreeError};
use orchestrator_core::labels::{EXECUTION_ID, MANAGED, REPOSITORY};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::Path;
use std::process::Output;

#[derive(Debug, PartialEq, Eq)]
struct CaptureConfiguration {
    config: Vec<u8>,
    attributes: BTreeMap<std::path::PathBuf, Vec<u8>>,
}

pub(crate) fn capture(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<DiffCapture, WorktreeError> {
    let owned_path = verified_path(manager, worktree)?;
    let path = Path::new(&owned_path);
    let configuration = capture_configuration(path)?;
    verify_repository_storage(path)?;
    verify_capture_configuration(path, &configuration)?;
    let owner = read_owner_record(path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    if owner.labels.get(MANAGED).map(String::as_str) != Some("true")
        || owner.labels.get(EXECUTION_ID).map(String::as_str)
            != Some(worktree.execution_id.as_str())
        || owner.base_sha != worktree.base_sha
        || owner.branch != worktree.branch
        || owner.labels.get(REPOSITORY).map(String::as_str) != Some(worktree.repository.as_str())
    {
        return Err(WorktreeError::Ownership(worktree.path.clone()));
    }
    verify_git_storage_preflight(path)?;
    let current_branch = String::from_utf8(
        git_success(
            path,
            &configuration,
            [OsStr::new("branch"), OsStr::new("--show-current")],
        )?
        .stdout,
    )
    .map_err(|error| WorktreeError::Diff(error.to_string()))?
    .trim()
    .to_owned();
    if current_branch != owner.branch {
        return Err(WorktreeError::Ownership(format!(
            "recorded branch {} does not match checkout {current_branch} at {}",
            owner.branch,
            path.display()
        )));
    }

    let mut patch = git_success(
        path,
        &configuration,
        [
            OsStr::new("diff"),
            OsStr::new("--no-ext-diff"),
            OsStr::new("--no-textconv"),
            OsStr::new("--binary"),
            OsStr::new(&worktree.base_sha),
            OsStr::new("--"),
        ],
    )?
    .stdout;
    let tracked = git_success(
        path,
        &configuration,
        [
            OsStr::new("diff"),
            OsStr::new("--no-ext-diff"),
            OsStr::new("--no-textconv"),
            OsStr::new("--name-only"),
            OsStr::new("-z"),
            OsStr::new(&worktree.base_sha),
            OsStr::new("--"),
        ],
    )?
    .stdout;
    let untracked = git_success(
        path,
        &configuration,
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
        verify_git_storage_preflight(path)?;
        verify_capture_configuration(path, &configuration)?;
        let output = safe_repository_git(path)
            .args([
                OsString::from("diff"),
                OsString::from("--no-index"),
                OsString::from("--no-ext-diff"),
                OsString::from("--no-textconv"),
                OsString::from("--binary"),
                OsString::from("--"),
                OsString::from("/dev/null"),
                OsString::from(&file),
            ])
            .output()
            .map_err(|error| WorktreeError::Diff(error.to_string()))?;
        verify_capture_configuration(path, &configuration)?;
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

fn git_success<I, S>(
    path: &Path,
    configuration: &CaptureConfiguration,
    args: I,
) -> Result<Output, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    verify_git_storage_preflight(path)?;
    verify_capture_configuration(path, configuration)?;
    let output = safe_repository_git(path)
        .args(args)
        .output()
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    verify_capture_configuration(path, configuration)?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(WorktreeError::Diff(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ))
    }
}

fn capture_configuration(path: &Path) -> Result<CaptureConfiguration, WorktreeError> {
    verify_git_storage_preflight(path)?;
    let config = path.join(".git/config");
    let config_metadata =
        fs::symlink_metadata(&config).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    if config_metadata.file_type().is_symlink() || !config_metadata.is_file() {
        return Err(WorktreeError::Diff(format!(
            "repository config is not a real file: {}",
            config.display()
        )));
    }
    let config_bytes = fs::read(&config).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    let output = git_command()
        .args(["config", "--file"])
        .arg(&config)
        .args(["--no-includes", "--null", "--name-only", "--list"])
        .output()
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    if !output.status.success() {
        return Err(WorktreeError::Diff(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    if config_bytes != fs::read(&config).map_err(|error| WorktreeError::Diff(error.to_string()))? {
        return Err(WorktreeError::Ownership(
            "repository config changed during evidence validation".to_owned(),
        ));
    }
    for key in output.stdout.split(|byte| *byte == 0) {
        if key.is_empty() {
            continue;
        }
        let key = String::from_utf8(key.to_vec())
            .map_err(|error| WorktreeError::Diff(error.to_string()))?
            .to_ascii_lowercase();
        if unsafe_capture_config_key(&key) {
            return Err(WorktreeError::Diff(format!(
                "repository config key is unsafe for host evidence capture: {key}"
            )));
        }
    }
    let mut attributes = BTreeMap::new();
    validate_attribute_tree(path, path, &mut attributes)?;
    let info_attributes = path.join(".git/info/attributes");
    match fs::symlink_metadata(&info_attributes) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(metadata) if metadata.file_type().is_file() => {
            let bytes = validate_attribute_file(&info_attributes)?;
            attributes.insert(info_attributes, bytes);
        }
        Ok(_) => {
            return Err(WorktreeError::Diff(format!(
                "Git attributes path is not a real file: {}",
                info_attributes.display()
            )))
        }
        Err(error) => return Err(WorktreeError::Diff(error.to_string())),
    }
    Ok(CaptureConfiguration {
        config: config_bytes,
        attributes,
    })
}

fn verify_capture_configuration(
    path: &Path,
    expected: &CaptureConfiguration,
) -> Result<(), WorktreeError> {
    let current = capture_configuration(path)?;
    if &current == expected {
        Ok(())
    } else {
        Err(WorktreeError::Ownership(
            "repository config or attributes changed during evidence capture".to_owned(),
        ))
    }
}

fn unsafe_capture_config_key(key: &str) -> bool {
    key.starts_with("filter.")
        || key.starts_with("include.")
        || key.starts_with("includeif.")
        || matches!(
            key,
            "core.attributesfile"
                | "core.fsmonitor"
                | "core.hookspath"
                | "core.worktree"
                | "diff.external"
        )
        || (key.starts_with("diff.") && (key.ends_with(".command") || key.ends_with(".textconv")))
}

fn validate_attribute_tree(
    root: &Path,
    directory: &Path,
    attributes: &mut BTreeMap<std::path::PathBuf, Vec<u8>>,
) -> Result<(), WorktreeError> {
    for entry in fs::read_dir(directory).map_err(|error| WorktreeError::Diff(error.to_string()))? {
        let entry = entry.map_err(|error| WorktreeError::Diff(error.to_string()))?;
        let path = entry.path();
        if path == root.join(".git") {
            continue;
        }
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
        if metadata.file_type().is_symlink() {
            if path.file_name() == Some(OsStr::new(".gitattributes")) {
                return Err(WorktreeError::Diff(format!(
                    "Git attributes path is a symlink: {}",
                    path.display()
                )));
            }
            continue;
        }
        if metadata.is_dir() {
            validate_attribute_tree(root, &path, attributes)?;
        } else if path.file_name() == Some(OsStr::new(".gitattributes")) {
            let bytes = validate_attribute_file(&path)?;
            attributes.insert(path, bytes);
        }
    }
    Ok(())
}

fn validate_attribute_file(path: &Path) -> Result<Vec<u8>, WorktreeError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(WorktreeError::Diff(format!(
            "Git attributes path is not a real file: {}",
            path.display()
        )));
    }
    let bytes = fs::read(path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    let contents =
        std::str::from_utf8(&bytes).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    for line in contents.lines() {
        let line = line.trim_start();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        for attribute in line.split_ascii_whitespace().skip(1) {
            let attribute = attribute.trim_start_matches(['-', '!']);
            let name = attribute.split('=').next().unwrap_or(attribute);
            if name.eq_ignore_ascii_case("filter") || name.eq_ignore_ascii_case("diff") {
                return Err(WorktreeError::Diff(format!(
                    "executable Git attribute is unsafe for host evidence capture: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(bytes)
}

fn safe_repository_git(path: &Path) -> std::process::Command {
    let mut command = repository_git_command(path);
    command.current_dir(path).args([
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "diff.external=",
        "-c",
        "include.path=/dev/null",
    ]);
    command
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
