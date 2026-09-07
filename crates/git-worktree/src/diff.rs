use crate::cleanup::verified_path;
use crate::command::git_command;
use crate::manager::{
    metadata_directory, read_owner_record, verify_git_storage_preflight, GitWorktreeManager,
    LockedMirror,
};
use crate::{DiffCapture, Worktree, WorktreeError};
use execution_storage::SecureMetadataDirectory;
use orchestrator_core::labels::{EXECUTION_ID, MANAGED, REPOSITORY};
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

static CAPTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TrustedCaptureContext {
    parent: SecureMetadataDirectory,
    directory: SecureMetadataDirectory,
    name: String,
    git_dir: PathBuf,
    work_tree: PathBuf,
    mirror: LockedMirror,
    object_database: VerifiedObjectDirectory,
    temp_objects: Option<SecureMetadataDirectory>,
    temp_refs: Option<SecureMetadataDirectory>,
    has_config: bool,
}

struct VerifiedObjectDirectory {
    path: PathBuf,
    handle: File,
    metadata: fs::Metadata,
}

pub(crate) fn capture(
    manager: &GitWorktreeManager,
    worktree: &Worktree,
) -> Result<DiffCapture, WorktreeError> {
    let owned_path = verified_path(manager, worktree)?;
    let path = Path::new(&owned_path);
    verify_git_storage_preflight(path)?;
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
    verify_current_branch(path, &owner.branch)?;
    let context = TrustedCaptureContext::create(manager, path, worktree)?;
    let result = capture_with_context(&context, worktree);
    let cleanup = context.cleanup();
    match (result, cleanup) {
        (Ok(capture), Ok(())) => Ok(capture),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(cleanup)) => Err(WorktreeError::Diff(format!(
            "remove trusted capture context failed: {cleanup}"
        ))),
        (Err(error), Err(cleanup)) => Err(WorktreeError::Diff(format!(
            "{error}; remove trusted capture context failed: {cleanup}"
        ))),
    }
}

fn capture_with_context(
    context: &TrustedCaptureContext,
    worktree: &Worktree,
) -> Result<DiffCapture, WorktreeError> {
    let top_level = String::from_utf8(
        git_success(
            context,
            [OsStr::new("rev-parse"), OsStr::new("--show-toplevel")],
        )?
        .stdout,
    )
    .map_err(|error| WorktreeError::Diff(error.to_string()))?
    .trim()
    .to_owned();
    let canonical_top_level = PathBuf::from(top_level)
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    let canonical_work_tree = context
        .work_tree
        .canonicalize()
        .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    if canonical_top_level != canonical_work_tree {
        return Err(WorktreeError::Ownership(format!(
            "trusted capture work tree {} is not {}",
            canonical_top_level.display(),
            canonical_work_tree.display()
        )));
    }

    let mut patch = git_success(
        context,
        [
            OsStr::new("diff"),
            OsStr::new("--no-ext-diff"),
            OsStr::new("--no-textconv"),
            OsStr::new("--ignore-submodules=dirty"),
            OsStr::new("--binary"),
            OsStr::new(&worktree.base_sha),
            OsStr::new("--"),
        ],
    )?
    .stdout;
    let tracked = git_success(
        context,
        [
            OsStr::new("diff"),
            OsStr::new("--no-ext-diff"),
            OsStr::new("--no-textconv"),
            OsStr::new("--ignore-submodules=dirty"),
            OsStr::new("--name-only"),
            OsStr::new("-z"),
            OsStr::new(&worktree.base_sha),
            OsStr::new("--"),
        ],
    )?
    .stdout;
    let untracked = git_success(
        context,
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
        let output = context
            .command()?
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
        context.verify()?;
        if output.status.code() != Some(1) {
            return Err(WorktreeError::Diff(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        patch.extend_from_slice(&output.stdout);
        changed_files.insert(file);
    }
    context.verify()?;

    Ok(DiffCapture {
        patch: String::from_utf8(patch).map_err(|error| WorktreeError::Diff(error.to_string()))?,
        changed_files: changed_files.into_iter().collect(),
    })
}

impl TrustedCaptureContext {
    fn create(
        manager: &GitWorktreeManager,
        work_tree: &Path,
        worktree: &Worktree,
    ) -> Result<Self, WorktreeError> {
        verify_git_storage_preflight(work_tree)?;
        let mirror = manager.lock_mirror_for_capture(&worktree.repository)?;
        mirror.verify()?;
        let object_database = VerifiedObjectDirectory::capture(&mirror.objects_path())?;
        let config = match worktree.base_sha.len() {
            40 => None,
            64 => Some(
                b"[core]\n\trepositoryformatversion = 1\n[extensions]\n\tobjectformat = sha256\n"
                    .as_slice(),
            ),
            length => {
                return Err(WorktreeError::Diff(format!(
                    "unsupported Git object identifier length: {length}"
                )))
            }
        };
        let parent = metadata_directory(manager, WorktreeError::Diff)?;
        let name = format!(
            ".capture-{}-{}-{}",
            worktree.execution_id.as_str(),
            std::process::id(),
            CAPTURE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let directory = parent
            .create_subdirectory(&name)
            .map_err(|error| WorktreeError::Diff(error.to_string()))?;
        let mut context = Self {
            git_dir: directory.path().to_path_buf(),
            work_tree: work_tree.to_path_buf(),
            parent,
            directory,
            name,
            mirror,
            object_database,
            temp_objects: None,
            temp_refs: None,
            has_config: config.is_some(),
        };
        let setup = (|| {
            context.temp_objects = Some(
                context
                    .directory
                    .create_subdirectory("objects")
                    .map_err(|error| WorktreeError::Diff(error.to_string()))?,
            );
            context.temp_refs = Some(
                context
                    .directory
                    .create_subdirectory("refs")
                    .map_err(|error| WorktreeError::Diff(error.to_string()))?,
            );
            context
                .directory
                .create("HEAD", format!("{}\n", worktree.base_sha).as_bytes())
                .map_err(|error| WorktreeError::Diff(error.to_string()))?;
            if let Some(config) = config {
                context
                    .directory
                    .create("config", config)
                    .map_err(|error| WorktreeError::Diff(error.to_string()))?;
            }
            context.verify()?;
            git_success(
                &context,
                [OsStr::new("read-tree"), OsStr::new(&worktree.base_sha)],
            )?;
            context
                .directory
                .restrict_file_to_owner("index")
                .map_err(|error| WorktreeError::Diff(error.to_string()))?;
            context.verify()
        })();
        match setup {
            Ok(()) => Ok(context),
            Err(error) => match context.cleanup() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(WorktreeError::Diff(format!(
                    "{error}; rollback trusted capture context failed: {cleanup}"
                ))),
            },
        }
    }

    fn command(&self) -> Result<Command, WorktreeError> {
        self.verify()?;
        let mut command = git_command();
        command
            .arg("--git-dir")
            .arg(&self.git_dir)
            .arg("--work-tree")
            .arg(&self.work_tree)
            .args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.fsmonitor=false",
                "-c",
                "diff.external=",
            ])
            .env("GIT_OBJECT_DIRECTORY", &self.object_database.path)
            .current_dir(&self.work_tree);
        Ok(command)
    }

    fn verify(&self) -> Result<(), WorktreeError> {
        self.mirror.verify()?;
        self.object_database.verify()?;
        if self.directory.path() != self.git_dir {
            return Err(WorktreeError::Ownership(
                "trusted capture Git directory identity changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn cleanup(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        for name in ["index.lock", "index"] {
            if let Err(error) = self.directory.remove_tool_file(name) {
                errors.push(format!("remove {name}: {error}"));
            }
        }
        for name in ["config", "HEAD"] {
            if name == "config" && !self.has_config {
                continue;
            }
            if let Err(error) = self.directory.remove(name) {
                errors.push(format!("remove {name}: {error}"));
            }
        }
        for (name, child) in [
            ("objects", self.temp_objects.as_ref()),
            ("refs", self.temp_refs.as_ref()),
        ] {
            if let Some(child) = child {
                if let Err(error) = self.directory.remove_subdirectory(name, child) {
                    errors.push(format!("remove {name}: {error}"));
                }
            }
        }
        if let Err(error) = self.parent.remove_subdirectory(&self.name, &self.directory) {
            errors.push(format!("remove directory: {error}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl VerifiedObjectDirectory {
    fn capture(supplied: &Path) -> Result<Self, WorktreeError> {
        let metadata = fs::symlink_metadata(supplied)
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(WorktreeError::Mirror(format!(
                "object database is not a real directory: {}",
                supplied.display()
            )));
        }
        let path = supplied
            .canonicalize()
            .map_err(|error| WorktreeError::Mirror(error.to_string()))?;
        let handle = File::open(&path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
        let opened = handle
            .metadata()
            .map_err(|error| WorktreeError::Diff(error.to_string()))?;
        verify_same_file(&metadata, &opened, "object database")?;
        Ok(Self {
            path,
            handle,
            metadata: opened,
        })
    }

    fn verify(&self) -> Result<(), WorktreeError> {
        let current = fs::symlink_metadata(&self.path)
            .map_err(|error| WorktreeError::Ownership(error.to_string()))?;
        if current.file_type().is_symlink() || !current.is_dir() {
            return Err(WorktreeError::Ownership(
                "object database is no longer a real directory".to_owned(),
            ));
        }
        let opened = self
            .handle
            .metadata()
            .map_err(|error| WorktreeError::Diff(error.to_string()))?;
        verify_same_file(&self.metadata, &current, "object database")?;
        verify_same_file(&self.metadata, &opened, "object database")
    }
}

fn verify_current_branch(path: &Path, branch: &str) -> Result<(), WorktreeError> {
    let head = read_stable_file(&path.join(".git/HEAD"), "Git HEAD")?;
    let head =
        std::str::from_utf8(&head).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    let expected = format!("ref: refs/heads/{branch}");
    if head.trim() == expected {
        Ok(())
    } else {
        Err(WorktreeError::Ownership(format!(
            "recorded branch {branch} does not match checkout {} at {}",
            head.trim(),
            path.display()
        )))
    }
}

fn read_stable_file(path: &Path, purpose: &str) -> Result<Vec<u8>, WorktreeError> {
    let supplied =
        fs::symlink_metadata(path).map_err(|error| WorktreeError::Ownership(error.to_string()))?;
    if supplied.file_type().is_symlink() || !supplied.is_file() {
        return Err(WorktreeError::Ownership(format!(
            "{purpose} is not a real file: {}",
            path.display()
        )));
    }
    let mut file = File::open(path).map_err(|error| WorktreeError::Diff(error.to_string()))?;
    let opened = file
        .metadata()
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    verify_same_file(&supplied, &opened, purpose)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    let after = file
        .metadata()
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    verify_same_file(&opened, &after, purpose)?;
    if opened.len() != after.len() {
        return Err(WorktreeError::Ownership(format!(
            "{purpose} changed while copying"
        )));
    }
    Ok(bytes)
}

fn verify_same_file(
    expected: &fs::Metadata,
    actual: &fs::Metadata,
    purpose: &str,
) -> Result<(), WorktreeError> {
    #[cfg(unix)]
    if expected.dev() != actual.dev()
        || expected.ino() != actual.ino()
        || expected.uid() != actual.uid()
    {
        return Err(WorktreeError::Ownership(format!(
            "{purpose} identity changed"
        )));
    }
    #[cfg(not(unix))]
    let _ = (expected, actual, purpose);
    Ok(())
}

fn git_success<I, S>(context: &TrustedCaptureContext, args: I) -> Result<Output, WorktreeError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = context
        .command()?
        .args(args)
        .output()
        .map_err(|error| WorktreeError::Diff(error.to_string()))?;
    context.verify()?;
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
