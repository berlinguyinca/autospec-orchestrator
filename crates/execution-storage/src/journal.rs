use crate::{ExecutionLayout, PhaseJournal, StorageError};
use orchestrator_core::ExecutionId;
use orchestrator_core::OwnershipLabels;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(test)]
type RaceHook = Option<(String, Box<dyn FnOnce()>)>;

#[cfg(test)]
thread_local! {
    static RACE_HOOK: std::cell::RefCell<RaceHook> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn install_race_hook(point: &str, hook: impl FnOnce() + 'static) {
    RACE_HOOK.with(|slot| *slot.borrow_mut() = Some((point.to_owned(), Box::new(hook))));
}

#[cfg(test)]
fn race_hook(point: &str) {
    RACE_HOOK.with(|slot| {
        let matches = slot
            .borrow()
            .as_ref()
            .is_some_and(|(expected, _)| expected == point);
        if matches {
            let (_, hook) = slot.borrow_mut().take().expect("race hook disappeared");
            hook();
        }
    });
}

#[cfg(not(test))]
fn race_hook(_: &str) {}

static OWNER_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
use rustix::fs::{AtFlags, FileType, Mode, OFlags, RenameFlags};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[derive(Debug, Clone)]
pub struct JournalStore {
    directory: PathBuf,
    state_root: PinnedDirectory,
    journal_directory: PinnedDirectory,
}

/// Owner-only, descriptor-pinned directory for small durable metadata records.
#[derive(Debug, Clone)]
pub struct SecureMetadataDirectory {
    directory: PinnedDirectory,
}

impl SecureMetadataDirectory {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self {
            directory: PinnedDirectory::capture(path.as_ref(), "metadata directory")?,
        })
    }

    #[cfg(unix)]
    fn capture_subdirectory(&self, name: &str) -> Result<Self, StorageError> {
        Ok(Self {
            directory: self
                .directory
                .capture_child(name, "metadata subdirectory")?,
        })
    }

    pub fn create(&self, name: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let _lock = self.directory.lock()?;
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        if self.directory.child_metadata(name)?.is_some() {
            return Err(StorageError::Journal(format!(
                "metadata record already exists: {name}"
            )));
        }
        let temporary = temporary_name(&format!("metadata-{name}"))?;
        let file = self.directory.create_file(&temporary)?;
        let temporary_identity = file
            .metadata()
            .map_err(|error| journal_error("inspect", &self.directory.path, error))?;
        if let Err(error) =
            write_bytes_and_sync(file, bytes, &self.directory.child_path(&temporary)?)
        {
            let _ = self
                .directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
        race_hook("metadata-create-before-commit");
        if let Err(error) = self.directory.rename_noreplace(&temporary, name) {
            let _ = self
                .directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
        self.directory.sync()
    }

    pub fn replace(&self, name: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let _lock = self.directory.lock()?;
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        let current = self.directory.open_file(name)?;
        let expected = current
            .metadata()
            .map_err(|error| journal_error("inspect", &self.directory.path, error))?;
        let temporary = temporary_name(&format!("metadata-{name}"))?;
        let file = self.directory.create_file(&temporary)?;
        let temporary_identity = file
            .metadata()
            .map_err(|error| journal_error("inspect", &self.directory.path, error))?;
        if let Err(error) =
            write_bytes_and_sync(file, bytes, &self.directory.child_path(&temporary)?)
        {
            let _ = self
                .directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
        if let Err(error) = self.directory.exchange_verified(
            &temporary,
            name,
            &expected,
            "metadata-replace-before-rename",
        ) {
            let _ = self
                .directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
        self.directory.remove_file_verified(&temporary, &expected)?;
        self.directory.sync()
    }

    pub fn read(&self, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        if self.directory.child_metadata(name)?.is_none() {
            return Ok(None);
        }
        let mut file = self.directory.open_file(name)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| journal_error("read", &self.directory.path, error))?;
        self.directory.verify("metadata directory")?;
        Ok(Some(bytes))
    }

    pub fn remove(&self, name: &str) -> Result<(), StorageError> {
        let _lock = self.directory.lock()?;
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        let Some(_) = self.directory.child_metadata(name)? else {
            return Ok(());
        };
        let file = self.directory.open_file(name)?;
        let expected = file
            .metadata()
            .map_err(|error| journal_error("inspect", &self.directory.path, error))?;
        let tombstone = temporary_name(&format!("remove-metadata-{name}"))?;
        self.directory.rename_verified(
            name,
            &tombstone,
            &expected,
            "metadata-remove-before-rename",
        )?;
        self.directory.remove_file_verified(&tombstone, &expected)?;
        self.directory.sync()
    }

    /// Returns the canonical path of this descriptor-pinned metadata directory.
    pub fn path(&self) -> &Path {
        &self.directory.path
    }

    pub fn names(&self) -> Result<Vec<String>, StorageError> {
        self.directory.verify("metadata directory")?;
        let names = self.directory.child_names()?;
        let mut records = Vec::new();
        for name in names {
            if is_operation_temporary_name(&name) || is_owner_probe_name(&name) {
                self.directory.open_file(&name)?;
                continue;
            }
            validate_metadata_name(&name)?;
            records.push(name);
        }
        self.directory.verify("metadata directory")?;
        Ok(records)
    }

    /// Restricts an existing real direct-child file to owner-only access and fsyncs it.
    #[cfg(unix)]
    pub fn restrict_file_to_owner(&self, name: &str) -> Result<(), StorageError> {
        let _lock = self.directory.lock()?;
        self.restrict_file_to_owner_locked(name).map(|_| ())
    }

    #[cfg(unix)]
    fn restrict_file_to_owner_locked(&self, name: &str) -> Result<fs::Metadata, StorageError> {
        use std::os::unix::fs::PermissionsExt;

        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        let path = self.directory.child_path(name)?;
        let expected = self.directory.child_metadata(name)?.ok_or_else(|| {
            StorageError::IdentityMismatch("metadata file disappeared".to_owned())
        })?;
        if expected.file_type().is_symlink() || !expected.is_file() {
            return Err(StorageError::IdentityMismatch(format!(
                "metadata child is not a real file: {}",
                path.display()
            )));
        }
        let file = self.directory.open_file_read_write(name)?;
        verify_opened_identity(&expected, &file, &path)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| journal_error("restrict", &path, error))?;
        file.sync_all()
            .map_err(|error| journal_error("fsync", &path, error))?;
        self.directory.verify_child(name, &expected)?;
        self.directory.sync()?;
        Ok(expected)
    }

    /// Restricts and removes an optional real direct-child file created by a trusted host tool.
    #[cfg(unix)]
    pub fn remove_tool_file(&self, name: &str) -> Result<(), StorageError> {
        let _lock = self.directory.lock()?;
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        if self.directory.child_metadata(name)?.is_none() {
            return Ok(());
        }
        let expected = self.restrict_file_to_owner_locked(name)?;
        let tombstone = temporary_name(&format!("remove-tool-{name}"))?;
        self.directory.rename_verified(
            name,
            &tombstone,
            &expected,
            "metadata-tool-remove-before-rename",
        )?;
        self.directory.remove_file_verified(&tombstone, &expected)?;
        self.directory.sync()
    }

    /// Creates and pins one owner-only direct child beneath this directory.
    #[cfg(unix)]
    pub fn create_subdirectory(&self, name: &str) -> Result<Self, StorageError> {
        let _lock = self.directory.lock()?;
        self.create_subdirectory_with(
            name,
            |directory, child| {
                directory.child_metadata(child)?.ok_or_else(|| {
                    StorageError::IdentityMismatch("metadata subdirectory disappeared".to_owned())
                })
            },
            PinnedDirectory::sync,
            |directory, child| {
                Ok(Self {
                    directory: directory.capture_child(child, "metadata subdirectory")?,
                })
            },
        )
    }

    #[cfg(unix)]
    fn create_subdirectory_with<Metadata, Sync, Pin>(
        &self,
        name: &str,
        inspect_child: Metadata,
        sync_parent: Sync,
        pin_child: Pin,
    ) -> Result<Self, StorageError>
    where
        Metadata: FnOnce(&PinnedDirectory, &str) -> Result<fs::Metadata, StorageError>,
        Sync: FnOnce(&PinnedDirectory) -> Result<(), StorageError>,
        Pin: FnOnce(&PinnedDirectory, &str) -> Result<Self, StorageError>,
    {
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        if self.directory.child_metadata(name)?.is_some() {
            return Err(StorageError::Journal(format!(
                "metadata subdirectory already exists: {name}"
            )));
        }
        let expected = self.directory.create_directory_unsynced(name)?;
        let result = (|| {
            let observed = inspect_child(&self.directory, name)?;
            verify_directory_identity(&expected, &observed, "metadata subdirectory")?;
            sync_parent(&self.directory)?;
            let child = pin_child(&self.directory, name)?;
            let opened = child
                .directory
                .handle
                .metadata()
                .map_err(|error| journal_error("inspect", &child.directory.path, error))?;
            if expected.dev() != opened.dev()
                || expected.ino() != opened.ino()
                || expected.uid() != opened.uid()
            {
                return Err(StorageError::IdentityMismatch(
                    "metadata subdirectory changed while pinning".to_owned(),
                ));
            }
            self.directory.verify("metadata directory")?;
            Ok(child)
        })();
        match result {
            Ok(child) => Ok(child),
            Err(error) => match self.rollback_created_subdirectory(name, &expected) {
                Ok(()) => Err(error),
                Err(rollback) => Err(StorageError::Journal(format!(
                    "{error}; rollback metadata subdirectory failed: {rollback}"
                ))),
            },
        }
    }

    #[cfg(unix)]
    fn rollback_created_subdirectory(
        &self,
        name: &str,
        expected: &fs::Metadata,
    ) -> Result<(), StorageError> {
        let current = self.directory.child_metadata(name)?.ok_or_else(|| {
            StorageError::IdentityMismatch("metadata subdirectory disappeared".to_owned())
        })?;
        if current.file_type().is_symlink()
            || !current.is_dir()
            || current.dev() != expected.dev()
            || current.ino() != expected.ino()
            || current.uid() != expected.uid()
        {
            return Err(StorageError::IdentityMismatch(
                "created metadata subdirectory identity changed".to_owned(),
            ));
        }
        self.directory.remove_directory(name)
    }

    /// Removes an empty direct child only when its retained pinned identity matches.
    #[cfg(unix)]
    pub fn remove_subdirectory(&self, name: &str, child: &Self) -> Result<(), StorageError> {
        let _lock = self.directory.lock()?;
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        child.directory.verify("metadata subdirectory")?;
        if child.directory.path != self.directory.path.join(name) {
            return Err(StorageError::IdentityMismatch(
                "metadata subdirectory selector does not match pinned child".to_owned(),
            ));
        }
        let expected = child
            .directory
            .handle
            .metadata()
            .map_err(|error| journal_error("inspect", &child.directory.path, error))?;
        let current = self.directory.child_metadata(name)?.ok_or_else(|| {
            StorageError::IdentityMismatch("metadata subdirectory disappeared".to_owned())
        })?;
        if current.dev() != expected.dev()
            || current.ino() != expected.ino()
            || current.uid() != expected.uid()
        {
            return Err(StorageError::IdentityMismatch(
                "metadata subdirectory identity changed".to_owned(),
            ));
        }
        let tombstone = temporary_name(&format!("remove-directory-{name}"))?;
        self.directory.rename_verified(
            name,
            &tombstone,
            &expected,
            "metadata-subdirectory-before-remove",
        )?;
        self.directory
            .remove_directory_verified(&tombstone, &expected)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionLifecycleHold {
    pub labels: OwnershipLabels,
    pub hold_id: String,
    pub container_id: String,
    pub session_id: String,
    pub supervisor_token: String,
    pub pgid: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct ExecutionLifecycleHoldStore {
    directory: SecureMetadataDirectory,
}

impl ExecutionLifecycleHoldStore {
    pub fn new(state_root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let state_root = PinnedDirectory::capture(state_root.as_ref(), "state root")?;
        let parent = SecureMetadataDirectory {
            directory: state_root.capture_child("execution-storage", "journal directory")?,
        };
        let directory = if parent.directory.child_metadata("holds")?.is_some() {
            parent.capture_subdirectory("holds")?
        } else {
            parent.create_subdirectory("holds")?
        };
        Ok(Self { directory })
    }

    pub fn create(&self, hold: &ExecutionLifecycleHold) -> Result<(), StorageError> {
        validate_hold(hold)?;
        self.directory.create(
            &hold_name(&hold.labels.execution_id, &hold.hold_id)?,
            &serde_json::to_vec(hold).map_err(|error| StorageError::Journal(error.to_string()))?,
        )
    }

    pub fn replace(&self, hold: &ExecutionLifecycleHold) -> Result<(), StorageError> {
        validate_hold(hold)?;
        self.directory.replace(
            &hold_name(&hold.labels.execution_id, &hold.hold_id)?,
            &serde_json::to_vec(hold).map_err(|error| StorageError::Journal(error.to_string()))?,
        )
    }

    pub fn remove(&self, execution_id: &ExecutionId, hold_id: &str) -> Result<(), StorageError> {
        self.directory.remove(&hold_name(execution_id, hold_id)?)
    }

    pub fn list(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<ExecutionLifecycleHold>, StorageError> {
        let prefix = format!("{}--", execution_id.as_str());
        let mut holds = Vec::new();
        for name in self
            .directory
            .names()?
            .into_iter()
            .filter(|name| name.starts_with(&prefix))
        {
            let bytes = self
                .directory
                .read(&name)?
                .ok_or_else(|| StorageError::Journal("lifecycle hold disappeared".to_owned()))?;
            let hold: ExecutionLifecycleHold = serde_json::from_slice(&bytes)
                .map_err(|error| StorageError::Journal(error.to_string()))?;
            validate_hold(&hold)?;
            if &hold.labels.execution_id != execution_id {
                return Err(StorageError::IdentityMismatch(
                    "lifecycle hold execution changed".to_owned(),
                ));
            }
            holds.push(hold);
        }
        Ok(holds)
    }
}

fn hold_name(execution_id: &ExecutionId, hold_id: &str) -> Result<String, StorageError> {
    let name = format!("{}--{hold_id}.json", execution_id.as_str());
    validate_metadata_name(&name)?;
    Ok(name)
}

fn validate_hold(hold: &ExecutionLifecycleHold) -> Result<(), StorageError> {
    if hold.hold_id.is_empty()
        || hold.container_id.is_empty()
        || hold.session_id.is_empty()
        || hold.supervisor_token.is_empty()
    {
        return Err(StorageError::IdentityMismatch(
            "lifecycle hold identity is incomplete".to_owned(),
        ));
    }
    hold_name(&hold.labels.execution_id, &hold.hold_id).map(|_| ())
}

impl JournalStore {
    pub fn new(state_root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let state_root = PinnedDirectory::capture(state_root.as_ref(), "state root")?;
        let directory = state_root.path.join("execution-storage");
        let journal_directory =
            state_root.capture_child("execution-storage", "journal directory")?;
        Ok(Self {
            directory,
            state_root,
            journal_directory,
        })
    }

    pub fn create(
        &self,
        layout: &ExecutionLayout,
        journal: &PhaseJournal,
    ) -> Result<(), StorageError> {
        let _lock = self.journal_directory.lock()?;
        self.verify_directories()?;
        self.validate_path(layout)?;
        journal.validate(layout)?;
        let name = journal_name(layout)?;
        let file = self.journal_directory.create_file(&name)?;
        write_and_sync(file, journal, &layout.journal)?;
        self.journal_directory.sync()?;
        self.verify_directories()
    }

    pub(crate) fn exists(&self, layout: &ExecutionLayout) -> Result<bool, StorageError> {
        self.verify_directories()?;
        self.validate_path(layout)?;
        Ok(self
            .journal_directory
            .child_metadata(&journal_name(layout)?)?
            .is_some())
    }

    pub fn write(
        &self,
        layout: &ExecutionLayout,
        journal: &PhaseJournal,
    ) -> Result<(), StorageError> {
        let _lock = self.journal_directory.lock()?;
        self.verify_directories()?;
        self.validate_path(layout)?;
        journal.validate(layout)?;
        let name = journal_name(layout)?;
        let current = self.journal_directory.open_file(&name)?;
        let expected = current
            .metadata()
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        let temporary = temporary_name(&name)?;
        let file = self.journal_directory.create_file(&temporary)?;
        let temporary_identity = file
            .metadata()
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        if let Err(error) = write_and_sync(
            file,
            journal,
            &self.journal_directory.child_path(&temporary)?,
        ) {
            let _ = self
                .journal_directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
        if let Err(error) = self.journal_directory.exchange_verified(
            &temporary,
            &name,
            &expected,
            "journal-write-before-rename",
        ) {
            let _ = self
                .journal_directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
        self.journal_directory
            .remove_file_verified(&temporary, &expected)?;
        self.journal_directory.sync()?;
        self.verify_directories()
    }

    pub fn read(&self, layout: &ExecutionLayout) -> Result<PhaseJournal, StorageError> {
        self.verify_directories()?;
        self.validate_path(layout)?;
        let file = self.journal_directory.open_file(&journal_name(layout)?)?;
        let journal: PhaseJournal =
            serde_json::from_reader(BufReader::new(file)).map_err(|error| {
                StorageError::Journal(format!("parse {}: {error}", layout.journal.display()))
            })?;
        journal.validate(layout)?;
        Ok(journal)
    }

    pub(crate) fn ensure_ready_lease(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        let _lock = self.journal_directory.lock()?;
        self.verify_directories()?;
        self.validate_path(layout)?;
        let name = lease_name(layout)?;
        match self.journal_directory.child_metadata(&name)? {
            Some(_) => {
                self.journal_directory.open_file(&name)?;
                Ok(())
            }
            None => match self.journal_directory.create_file(&name) {
                Ok(file) => {
                    file.sync_all().map_err(|error| {
                        journal_error("fsync Ready lease", &layout.journal, error)
                    })?;
                    self.journal_directory.sync()?;
                    Ok(())
                }
                Err(_) => {
                    self.journal_directory.open_file(&name)?;
                    Ok(())
                }
            },
        }
    }

    pub(crate) fn open_ready_lease(&self, layout: &ExecutionLayout) -> Result<File, StorageError> {
        self.verify_directories()?;
        self.validate_path(layout)?;
        self.journal_directory.open_file(&lease_name(layout)?)
    }

    pub fn remove(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        let _lock = self.journal_directory.lock()?;
        self.verify_directories()?;
        self.validate_path(layout)?;
        let name = journal_name(layout)?;
        let file = self.journal_directory.open_file(&name)?;
        let expected = file
            .metadata()
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        let tombstone = temporary_name(&format!("remove-{name}"))?;
        self.journal_directory.rename_verified(
            &name,
            &tombstone,
            &expected,
            "journal-remove-before-rename",
        )?;
        self.journal_directory
            .remove_file_verified(&tombstone, &expected)?;
        self.journal_directory.sync()?;
        self.verify_directories()
    }

    pub fn list(&self, state_root: &Path) -> Result<Vec<PhaseJournal>, StorageError> {
        self.verify_directories()?;
        if self.directory != state_root.join("execution-storage") {
            return Err(StorageError::IdentityMismatch(
                "journal store and state root disagree".to_owned(),
            ));
        }
        let mut journals = Vec::new();
        for name in self.journal_directory.child_names()? {
            let entry_path = self.directory.join(&name);
            let metadata = self
                .journal_directory
                .child_metadata(&name)?
                .ok_or_else(|| {
                    StorageError::Journal(format!("journal entry disappeared: {name}"))
                })?;
            if matches!(name.as_str(), "holds" | "releases") && metadata.is_dir() {
                self.journal_directory
                    .capture_child(&name, "journal metadata subdirectory")?;
                continue;
            }
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(StorageError::Journal(format!(
                    "unexpected journal entry type: {}",
                    entry_path.display()
                )));
            }
            if name.ends_with(".lease") {
                self.journal_directory.open_file(&name)?;
                continue;
            }
            if is_owner_probe_name(&name) {
                self.journal_directory.open_file(&name)?;
                continue;
            }
            if is_journal_temporary_name(&name) {
                self.journal_directory.open_file(&name)?;
                continue;
            }
            let execution_id = name.strip_suffix(".json").ok_or_else(|| {
                StorageError::Journal(format!("unexpected journal filename: {name}"))
            })?;
            let layout = ExecutionLayout::new(state_root, &ExecutionId::new(execution_id))?;
            journals.push(self.read(&layout)?);
        }
        journals.sort_by(|left, right| left.labels.execution_id.cmp(&right.labels.execution_id));
        Ok(journals)
    }

    fn validate_path(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        if layout.journal.parent() != Some(self.directory.as_path()) {
            return Err(StorageError::IdentityMismatch(format!(
                "journal {} is outside {}",
                layout.journal.display(),
                self.directory.display()
            )));
        }
        Ok(())
    }

    fn verify_directories(&self) -> Result<(), StorageError> {
        self.state_root.verify("state root")?;
        self.journal_directory.verify("journal directory")
    }
}

fn lease_name(layout: &ExecutionLayout) -> Result<String, StorageError> {
    Ok(format!(
        "{}.lease",
        journal_name(layout)?.trim_end_matches(".json")
    ))
}

#[derive(Debug, Clone)]
/// Internal boundary for descriptor-pinned, no-follow directory operations.
///
/// All child access is resolved by the kernel relative to the retained
/// descriptor. The canonical path is used only for identity verification and
/// diagnostics, never as mutation authority.
pub(crate) struct PinnedDirectory {
    pub(crate) path: PathBuf,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    uid: u32,
    #[cfg(unix)]
    handle: Arc<File>,
}

#[cfg(unix)]
struct DirectoryLock<'a> {
    handle: &'a File,
}

#[cfg(unix)]
impl Drop for DirectoryLock<'_> {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(self.handle);
    }
}

impl PinnedDirectory {
    #[cfg(unix)]
    fn lock(&self) -> Result<DirectoryLock<'_>, StorageError> {
        fs2::FileExt::lock_exclusive(&*self.handle)
            .map_err(|error| journal_error("lock directory", &self.path, error))?;
        Ok(DirectoryLock {
            handle: &self.handle,
        })
    }

    pub(crate) fn capture(path: &Path, purpose: &str) -> Result<Self, StorageError> {
        let supplied_path = path.to_path_buf();
        let supplied = fs::symlink_metadata(&supplied_path)
            .map_err(|error| journal_error("inspect", path, error))?;
        race_hook("capture-after-lstat");
        if supplied.file_type().is_symlink() || !supplied.is_dir() {
            return Err(StorageError::Journal(format!(
                "{purpose} is not a real directory: {}",
                path.display()
            )));
        }
        #[cfg(unix)]
        let handle = open_directory(&supplied_path)?;
        #[cfg(unix)]
        let opened = handle
            .metadata()
            .map_err(|error| journal_error("inspect", &supplied_path, error))?;
        #[cfg(unix)]
        if opened.dev() != supplied.dev()
            || opened.ino() != supplied.ino()
            || opened.uid() != supplied.uid()
            || opened.mode() != supplied.mode()
        {
            return Err(StorageError::IdentityMismatch(format!(
                "{purpose} changed while opening"
            )));
        }
        let path = supplied_path.canonicalize().map_err(|error| {
            StorageError::Journal(format!(
                "canonicalize {purpose} {}: {error}",
                supplied_path.display()
            ))
        })?;
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| journal_error("inspect", &path, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::Journal(format!(
                "{purpose} is not a real directory: {}",
                path.display()
            )));
        }
        #[cfg(unix)]
        {
            if metadata.mode() & 0o077 != 0 {
                return Err(StorageError::Unavailable(format!(
                    "{purpose} mode {:o} permits group or world access",
                    metadata.mode() & 0o777
                )));
            }
            if opened.dev() != metadata.dev()
                || opened.ino() != metadata.ino()
                || opened.uid() != metadata.uid()
                || opened.mode() != metadata.mode()
            {
                return Err(StorageError::IdentityMismatch(format!(
                    "{purpose} changed while canonicalizing diagnostics"
                )));
            }
            verify_current_owner(&handle, &path, metadata.uid(), purpose)?;
            Ok(Self {
                path,
                device: metadata.dev(),
                inode: metadata.ino(),
                uid: metadata.uid(),
                handle: Arc::new(handle),
            })
        }
        #[cfg(not(unix))]
        Ok(Self { path })
    }

    pub(crate) fn verify(&self, purpose: &str) -> Result<(), StorageError> {
        let metadata = fs::symlink_metadata(&self.path)
            .map_err(|error| journal_error("inspect", &self.path, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::IdentityMismatch(format!(
                "{purpose} is no longer a real directory"
            )));
        }
        let opened = open_directory(&self.path)?
            .metadata()
            .map_err(|error| journal_error("inspect", &self.path, error))?;
        #[cfg(unix)]
        if metadata.dev() != self.device
            || metadata.ino() != self.inode
            || metadata.uid() != self.uid
            || opened.dev() != self.device
            || opened.ino() != self.inode
            || opened.mode() & 0o077 != 0
        {
            return Err(StorageError::IdentityMismatch(format!(
                "{purpose} inode, owner, or mode changed"
            )));
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn capture_child(&self, name: &str, purpose: &str) -> Result<Self, StorageError> {
        validate_descriptor_name(name)?;
        let diagnostic = self.path.join(name);
        let stat = rustix::fs::statat(&*self.handle, name, AtFlags::SYMLINK_NOFOLLOW)
            .map_err(|error| rustix_error("inspect", &diagnostic, error))?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
            return Err(StorageError::Journal(format!(
                "{purpose} is not a real directory: {}",
                diagnostic.display()
            )));
        }
        if stat.st_mode as u32 & 0o077 != 0 {
            return Err(StorageError::Unavailable(format!(
                "{purpose} mode {:o} permits group or world access",
                stat.st_mode as u32 & 0o777
            )));
        }
        race_hook("capture-child-after-lstat");
        let fd = rustix::fs::openat(
            &*self.handle,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| rustix_error("open directory", &diagnostic, error))?;
        let handle = File::from(fd);
        let opened = handle
            .metadata()
            .map_err(|error| journal_error("inspect", &diagnostic, error))?;
        if opened.dev() != stat.st_dev as u64
            || opened.ino() != stat.st_ino as u64
            || opened.uid() != stat.st_uid
            || opened.mode() != stat.st_mode as u32
        {
            return Err(StorageError::IdentityMismatch(format!(
                "{purpose} changed while opening"
            )));
        }
        let path = diagnostic.canonicalize().map_err(|error| {
            StorageError::Journal(format!(
                "canonicalize {purpose} {}: {error}",
                diagnostic.display()
            ))
        })?;
        let current =
            fs::symlink_metadata(&path).map_err(|error| journal_error("inspect", &path, error))?;
        if current.dev() != opened.dev()
            || current.ino() != opened.ino()
            || current.uid() != opened.uid()
            || current.mode() != opened.mode()
        {
            return Err(StorageError::IdentityMismatch(format!(
                "{purpose} changed while canonicalizing diagnostics"
            )));
        }
        verify_current_owner(&handle, &path, opened.uid(), purpose)?;
        Ok(Self {
            path,
            device: opened.dev(),
            inode: opened.ino(),
            uid: opened.uid(),
            handle: Arc::new(handle),
        })
    }

    #[cfg(unix)]
    pub(crate) fn child_path(&self, name: &str) -> Result<PathBuf, StorageError> {
        validate_descriptor_name(name)?;
        Ok(self.path.join(name))
    }

    #[cfg(unix)]
    fn create_file(&self, name: &str) -> Result<File, StorageError> {
        let path = self.child_path(name)?;
        let fd = rustix::fs::openat(
            &*self.handle,
            name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .map_err(|error| rustix_error("create", &path, error))?;
        Ok(File::from(fd))
    }

    #[cfg(unix)]
    fn open_file(&self, name: &str) -> Result<File, StorageError> {
        let path = self.child_path(name)?;
        let file = self.open_file_with(name, OFlags::RDONLY)?;
        let metadata = file
            .metadata()
            .map_err(|error| journal_error("inspect", &path, error))?;
        if !metadata.is_file() || metadata.mode() & 0o077 != 0 {
            return Err(StorageError::Journal(format!(
                "journal is not a private real file: {}",
                path.display()
            )));
        }
        Ok(file)
    }

    #[cfg(unix)]
    fn open_file_read_write(&self, name: &str) -> Result<File, StorageError> {
        let path = self.child_path(name)?;
        let file = self.open_file_with(name, OFlags::RDWR)?;
        let metadata = file
            .metadata()
            .map_err(|error| journal_error("inspect", &path, error))?;
        if !metadata.is_file() {
            return Err(StorageError::Journal(format!(
                "metadata child is not a real file: {}",
                path.display()
            )));
        }
        Ok(file)
    }

    #[cfg(unix)]
    fn open_file_with(&self, name: &str, access: OFlags) -> Result<File, StorageError> {
        let path = self.child_path(name)?;
        let fd = rustix::fs::openat(
            &*self.handle,
            name,
            access | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| rustix_error("open", &path, error))?;
        Ok(File::from(fd))
    }

    #[cfg(unix)]
    fn verify_child(&self, name: &str, expected: &fs::Metadata) -> Result<(), StorageError> {
        let file = self.open_file(name)?;
        verify_opened_file(expected, &file, &self.child_path(name)?)
    }

    #[cfg(unix)]
    fn verify_child_identity(
        &self,
        name: &str,
        expected: &fs::Metadata,
    ) -> Result<(), StorageError> {
        let actual = self.child_metadata(name)?.ok_or_else(|| {
            StorageError::IdentityMismatch(format!(
                "descriptor-relative child disappeared: {}",
                self.path.join(name).display()
            ))
        })?;
        verify_same_identity(expected, &actual, "descriptor-relative child")
    }

    #[cfg(unix)]
    #[cfg(test)]
    fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        self.child_path(from)?;
        let to_path = self.child_path(to)?;
        rustix::fs::renameat(&*self.handle, from, &*self.handle, to)
            .map_err(|error| rustix_error("rename", &to_path, error))
    }

    #[cfg(unix)]
    fn rename_noreplace(&self, from: &str, to: &str) -> Result<(), StorageError> {
        self.child_path(from)?;
        let to_path = self.child_path(to)?;
        rustix::fs::renameat_with(
            &*self.handle,
            from,
            &*self.handle,
            to,
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| rustix_error("rename without replacement", &to_path, error))
    }

    #[cfg(unix)]
    fn exchange_verified(
        &self,
        replacement: &str,
        target: &str,
        expected_target: &fs::Metadata,
        hook: &str,
    ) -> Result<(), StorageError> {
        self.child_path(replacement)?;
        self.child_path(target)?;
        self.verify_child_identity(target, expected_target)?;
        race_hook(hook);
        rustix::fs::renameat_with(
            &*self.handle,
            replacement,
            &*self.handle,
            target,
            RenameFlags::EXCHANGE,
        )
        .map_err(|error| rustix_error("atomically exchange", &self.path.join(target), error))?;
        if let Err(identity) = self.verify_child_identity(replacement, expected_target) {
            let rollback = rustix::fs::renameat_with(
                &*self.handle,
                replacement,
                &*self.handle,
                target,
                RenameFlags::EXCHANGE,
            );
            return Err(match rollback {
                Ok(()) => identity,
                Err(error) => StorageError::Journal(format!(
                    "{identity}; rollback atomic exchange failed: {error}"
                )),
            });
        }
        Ok(())
    }

    #[cfg(unix)]
    fn rename_verified(
        &self,
        source: &str,
        tombstone: &str,
        expected: &fs::Metadata,
        hook: &str,
    ) -> Result<(), StorageError> {
        self.verify_child_identity(source, expected)?;
        race_hook(hook);
        self.rename_noreplace(source, tombstone)?;
        if let Err(identity) = self.verify_child_identity(tombstone, expected) {
            let rollback = self.rename_noreplace(tombstone, source);
            return Err(match rollback {
                Ok(()) => identity,
                Err(error) => StorageError::Journal(format!(
                    "{identity}; rollback verified rename failed: {error}"
                )),
            });
        }
        Ok(())
    }

    #[cfg(unix)]
    fn remove_file(&self, name: &str) -> Result<(), StorageError> {
        let path = self.child_path(name)?;
        rustix::fs::unlinkat(&*self.handle, name, AtFlags::empty())
            .map_err(|error| rustix_error("remove", &path, error))
    }

    #[cfg(unix)]
    fn remove_file_verified(
        &self,
        name: &str,
        expected: &fs::Metadata,
    ) -> Result<(), StorageError> {
        self.verify_child_identity(name, expected)?;
        self.remove_file(name)
    }

    #[cfg(unix)]
    fn sync(&self) -> Result<(), StorageError> {
        rustix::fs::fsync(&*self.handle)
            .map_err(|error| rustix_error("fsync directory", &self.path, error))
    }

    #[cfg(unix)]
    pub(crate) fn create_directory(&self, name: &str) -> Result<(), StorageError> {
        let _ = self.create_directory_unsynced(name)?;
        self.sync()
    }

    #[cfg(unix)]
    fn create_directory_unsynced(&self, name: &str) -> Result<fs::Metadata, StorageError> {
        let path = self.child_path(name)?;
        rustix::fs::mkdirat(&*self.handle, name, Mode::from_bits_truncate(0o700))
            .map_err(|error| rustix_error("create directory", &path, error))?;
        match self.child_metadata(name) {
            Ok(Some(metadata)) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                Ok(metadata)
            }
            Ok(_) => Err(StorageError::IdentityMismatch(
                "new metadata child is not a real directory; preserve unverified replacement"
                    .to_owned(),
            )),
            Err(error) => Err(error),
        }
    }

    #[cfg(unix)]
    pub(crate) fn remove_directory(&self, name: &str) -> Result<(), StorageError> {
        let path = self.child_path(name)?;
        let metadata = self.child_metadata(name)?.ok_or_else(|| {
            StorageError::Journal(format!("inspect {}: no such directory", path.display()))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::IdentityMismatch(format!(
                "descriptor-relative mountpoint is not a real directory: {}",
                path.display()
            )));
        }
        let tombstone = temporary_name(&format!("remove-directory-{name}"))?;
        self.rename_verified(
            name,
            &tombstone,
            &metadata,
            "directory-remove-before-rename",
        )?;
        self.unlink_directory_verified(&tombstone, &metadata)?;
        self.sync()
    }

    #[cfg(unix)]
    fn remove_directory_verified(
        &self,
        name: &str,
        expected: &fs::Metadata,
    ) -> Result<(), StorageError> {
        self.verify_child_identity(name, expected)?;
        let tombstone = temporary_name(&format!("remove-directory-{name}"))?;
        self.rename_verified(name, &tombstone, expected, "directory-remove-before-rename")?;
        self.unlink_directory_verified(&tombstone, expected)?;
        self.sync()
    }

    #[cfg(unix)]
    fn unlink_directory_verified(
        &self,
        name: &str,
        expected: &fs::Metadata,
    ) -> Result<(), StorageError> {
        let path = self.child_path(name)?;
        self.verify_child_identity(name, expected)?;
        rustix::fs::unlinkat(&*self.handle, name, AtFlags::REMOVEDIR)
            .map_err(|error| rustix_error("remove directory", &path, error))
    }

    #[cfg(unix)]
    pub(crate) fn child_metadata(&self, name: &str) -> Result<Option<fs::Metadata>, StorageError> {
        let path = self.child_path(name)?;
        let stat = match rustix::fs::statat(&*self.handle, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(rustix_error("inspect", &path, error)),
        };
        let flags = match FileType::from_raw_mode(stat.st_mode) {
            FileType::RegularFile => OFlags::RDONLY,
            FileType::Directory => OFlags::RDONLY | OFlags::DIRECTORY,
            FileType::Symlink => {
                return Err(StorageError::IdentityMismatch(format!(
                    "descriptor-relative child is not a real directory or file (symlink): {}",
                    path.display()
                )))
            }
            _ => {
                return Err(StorageError::IdentityMismatch(format!(
                    "descriptor-relative child has an unsupported type: {}",
                    path.display()
                )))
            }
        };
        race_hook("child-metadata-after-lstat");
        let fd = rustix::fs::openat(
            &*self.handle,
            name,
            flags | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| rustix_error("open for inspection", &path, error))?;
        let file = File::from(fd);
        let metadata = file
            .metadata()
            .map_err(|error| journal_error("inspect opened child", &path, error))?;
        if metadata.dev() != stat.st_dev as u64
            || metadata.ino() != stat.st_ino as u64
            || metadata.uid() != stat.st_uid
        {
            return Err(StorageError::IdentityMismatch(format!(
                "descriptor-relative child changed while opening: {}",
                path.display()
            )));
        }
        Ok(Some(metadata))
    }

    #[cfg(unix)]
    fn child_names(&self) -> Result<Vec<String>, StorageError> {
        let directory = rustix::fs::Dir::read_from(&*self.handle)
            .map_err(|error| rustix_error("list", &self.path, error))?;
        let mut names = Vec::new();
        for entry in directory {
            let entry = entry.map_err(|error| rustix_error("list", &self.path, error))?;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            let name = std::str::from_utf8(bytes).map_err(|_| {
                StorageError::IdentityMismatch(
                    "descriptor-relative child name is not UTF-8".to_owned(),
                )
            })?;
            validate_descriptor_name(name)?;
            names.push(name.to_owned());
        }
        names.sort();
        Ok(names)
    }
}

#[cfg(unix)]
fn verify_current_owner(
    handle: &File,
    directory: &Path,
    expected_uid: u32,
    purpose: &str,
) -> Result<(), StorageError> {
    let name = format!(
        ".autospec-owner-{}-{}",
        std::process::id(),
        OWNER_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let probe = directory.join(&name);
    let fd = rustix::fs::openat(
        handle,
        name.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(|error| rustix_error("create ownership probe", &probe, error))?;
    let file = File::from(fd);
    let mut probe_guard = OwnershipProbeGuard {
        directory: handle,
        name: name.as_str(),
        linked: true,
    };
    race_hook("ownership-probe-after-create");
    rustix::fs::unlinkat(handle, name.as_str(), AtFlags::empty())
        .map_err(|error| rustix_error("remove ownership probe", &probe, error))?;
    probe_guard.linked = false;
    race_hook("ownership-probe-after-unlink");
    let actual_uid = file
        .metadata()
        .map_err(|error| journal_error("inspect ownership probe", &probe, error))?
        .uid();
    if actual_uid == expected_uid {
        Ok(())
    } else {
        Err(StorageError::Unavailable(format!(
            "{purpose} is owned by uid {expected_uid}, current uid is {actual_uid}"
        )))
    }
}

#[cfg(unix)]
struct OwnershipProbeGuard<'a> {
    directory: &'a File,
    name: &'a str,
    linked: bool,
}

#[cfg(unix)]
impl Drop for OwnershipProbeGuard<'_> {
    fn drop(&mut self) {
        if self.linked {
            let _ = rustix::fs::unlinkat(self.directory, self.name, AtFlags::empty());
        }
    }
}

#[cfg(unix)]
fn open_directory(path: &Path) -> Result<File, StorageError> {
    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| rustix_error("open directory", path, error))?;
    Ok(File::from(fd))
}

#[cfg(unix)]
fn validate_descriptor_name(name: &str) -> Result<(), StorageError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        Err(StorageError::InvalidRequest(
            "descriptor-relative name is unsafe".to_owned(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn rustix_error(action: &str, path: &Path, error: rustix::io::Errno) -> StorageError {
    journal_error(action, path, error.into())
}

fn write_and_sync(file: File, journal: &PhaseJournal, path: &Path) -> Result<(), StorageError> {
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, journal)
        .map_err(|error| StorageError::Journal(format!("serialize {}: {error}", path.display())))?;
    writer
        .write_all(b"\n")
        .map_err(|error| journal_error("write", path, error))?;
    writer
        .flush()
        .map_err(|error| journal_error("flush", path, error))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|error| journal_error("fsync", path, error))
}

fn write_bytes_and_sync(mut file: File, bytes: &[u8], path: &Path) -> Result<(), StorageError> {
    file.write_all(bytes)
        .map_err(|error| journal_error("write", path, error))?;
    file.sync_all()
        .map_err(|error| journal_error("fsync", path, error))
}

fn validate_metadata_name(name: &str) -> Result<(), StorageError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        Err(StorageError::InvalidRequest(
            "metadata record name is unsafe".to_owned(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn verify_same_identity(
    expected: &fs::Metadata,
    actual: &fs::Metadata,
    purpose: &str,
) -> Result<(), StorageError> {
    if expected.dev() != actual.dev()
        || expected.ino() != actual.ino()
        || expected.uid() != actual.uid()
        || expected.file_type() != actual.file_type()
    {
        return Err(StorageError::IdentityMismatch(format!(
            "{purpose} identity changed"
        )));
    }
    Ok(())
}

pub(crate) fn require_real_directory(path: &Path, purpose: &str) -> Result<(), StorageError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        StorageError::Journal(format!("inspect {purpose} {}: {error}", path.display()))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StorageError::Journal(format!(
            "{purpose} is not a real directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn verify_opened_file(
    expected: &fs::Metadata,
    file: &File,
    path: &Path,
) -> Result<(), StorageError> {
    let opened = file
        .metadata()
        .map_err(|error| journal_error("inspect opened file", path, error))?;
    #[cfg(unix)]
    if expected.dev() != opened.dev()
        || expected.ino() != opened.ino()
        || expected.uid() != opened.uid()
        || opened.mode() & 0o077 != 0
    {
        return Err(StorageError::IdentityMismatch(format!(
            "journal inode, owner, or mode changed while opening {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_opened_identity(
    expected: &fs::Metadata,
    file: &File,
    path: &Path,
) -> Result<(), StorageError> {
    let opened = file
        .metadata()
        .map_err(|error| journal_error("inspect opened file", path, error))?;
    if expected.dev() != opened.dev()
        || expected.ino() != opened.ino()
        || expected.uid() != opened.uid()
        || !opened.is_file()
    {
        return Err(StorageError::IdentityMismatch(format!(
            "metadata file identity changed while opening {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_directory_identity(
    expected: &fs::Metadata,
    actual: &fs::Metadata,
    purpose: &str,
) -> Result<(), StorageError> {
    if actual.file_type().is_symlink()
        || !actual.is_dir()
        || expected.dev() != actual.dev()
        || expected.ino() != actual.ino()
        || expected.uid() != actual.uid()
    {
        return Err(StorageError::IdentityMismatch(format!(
            "{purpose} identity changed"
        )));
    }
    Ok(())
}

fn journal_name(layout: &ExecutionLayout) -> Result<String, StorageError> {
    layout
        .journal
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            StorageError::Journal(format!(
                "journal name is not UTF-8: {}",
                layout.journal.display()
            ))
        })
}

fn temporary_name(name: &str) -> Result<String, StorageError> {
    Ok(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        OWNER_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

fn is_journal_temporary_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix('.') else {
        return false;
    };
    let mut components = rest.rsplitn(3, '.');
    components.next() == Some("tmp")
        && components
            .next()
            .is_some_and(|counter| counter.bytes().all(|byte| byte.is_ascii_digit()))
        && components.next().is_some_and(|prefix| {
            let Some((journal, process)) = prefix.rsplit_once('.') else {
                return false;
            };
            journal.ends_with(".json")
                && process.bytes().all(|byte| byte.is_ascii_digit())
                && !journal.is_empty()
        })
}

fn is_operation_temporary_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix('.') else {
        return false;
    };
    let mut components = rest.rsplitn(3, '.');
    components.next() == Some("tmp")
        && components
            .next()
            .is_some_and(|counter| counter.bytes().all(|byte| byte.is_ascii_digit()))
        && components.next().is_some_and(|prefix| {
            prefix.rsplit_once('.').is_some_and(|(operation, process)| {
                !operation.is_empty() && process.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
}

fn is_owner_probe_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(".autospec-owner-") else {
        return false;
    };
    rest.split_once('-').is_some_and(|(process, counter)| {
        !process.is_empty()
            && !counter.is_empty()
            && process.bytes().all(|byte| byte.is_ascii_digit())
            && counter.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn journal_error(action: &str, path: &Path, error: std::io::Error) -> StorageError {
    StorageError::Journal(format!("{action} {}: {error}", path.display()))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use orchestrator_core::WorkerId;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    fn metadata_directory() -> (tempfile::TempDir, SecureMetadataDirectory) {
        let root = tempfile::tempdir().expect("temporary metadata root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure metadata root");
        let metadata = SecureMetadataDirectory::new(root.path()).expect("pin metadata root");
        (root, metadata)
    }

    fn journal_fixture() -> (
        tempfile::TempDir,
        JournalStore,
        ExecutionLayout,
        PhaseJournal,
    ) {
        let root = tempfile::tempdir().expect("temporary state root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure state root");
        let journal_directory = root.path().join("execution-storage");
        fs::create_dir(&journal_directory).expect("create journal directory");
        fs::set_permissions(&journal_directory, fs::Permissions::from_mode(0o700))
            .expect("secure journal directory");
        let canonical = root.path().canonicalize().expect("canonical state root");
        let labels = OwnershipLabels {
            execution_id: ExecutionId::new("dirfd-race"),
            worker_id: WorkerId::new("worker-a"),
            repository: "owner/repository".to_owned(),
            issue: Some("29".to_owned()),
        };
        let layout = ExecutionLayout::new(&canonical, &labels.execution_id).expect("layout");
        let journal = PhaseJournal::allocating(
            labels,
            1,
            layout.root.clone(),
            "test".to_owned(),
            "test:key".to_owned(),
            "pool".to_owned(),
            "token".to_owned(),
        );
        let store = JournalStore::new(&canonical).expect("journal store");
        (root, store, layout, journal)
    }

    fn swapped_directory() -> (tempfile::TempDir, PinnedDirectory, PathBuf, PathBuf) {
        let root = tempfile::tempdir().expect("temporary parent");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure temporary parent");
        let selected = root.path().join("selected");
        fs::create_dir(&selected).expect("create selected directory");
        fs::set_permissions(&selected, fs::Permissions::from_mode(0o700))
            .expect("secure selected directory");
        let pinned = PinnedDirectory::capture(&selected, "race-test directory")
            .expect("pin selected directory");
        let captured = root.path().join("captured");
        fs::rename(&selected, &captured).expect("displace captured directory");
        fs::create_dir(&selected).expect("install attacker directory at selected path");
        fs::set_permissions(&selected, fs::Permissions::from_mode(0o700))
            .expect("secure attacker directory");
        (root, pinned, captured, selected)
    }

    #[test]
    fn public_capture_rejects_a_supplied_directory_swapped_after_initial_inspection() {
        let root = tempfile::tempdir().expect("temporary parent");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure temporary parent");
        let selected = root.path().join("selected");
        let captured = root.path().join("captured");
        let attacker = root.path().join("attacker");
        fs::create_dir(&selected).expect("create selected directory");
        fs::create_dir(&attacker).expect("create attacker directory");
        fs::set_permissions(&selected, fs::Permissions::from_mode(0o700))
            .expect("secure selected directory");
        fs::set_permissions(&attacker, fs::Permissions::from_mode(0o700))
            .expect("secure attacker directory");
        fs::write(attacker.join("sentinel"), b"attacker sentinel")
            .expect("write attacker sentinel");
        let selected_for_hook = selected.clone();
        let captured_for_hook = captured.clone();
        let attacker_for_hook = attacker.clone();
        install_race_hook("capture-after-lstat", move || {
            fs::rename(&selected_for_hook, &captured_for_hook)
                .expect("displace inspected directory");
            fs::rename(&attacker_for_hook, &selected_for_hook).expect("install attacker directory");
        });

        let error = SecureMetadataDirectory::new(&selected)
            .expect_err("reject directory replaced during capture");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected.join("sentinel")).expect("attacker sentinel remains"),
            b"attacker sentinel"
        );
    }

    #[test]
    fn journal_store_captures_nested_directory_from_the_retained_parent() {
        let root = tempfile::tempdir().expect("temporary state root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure state root");
        let selected = root.path().join("execution-storage");
        let captured = root.path().join("captured-storage");
        let attacker = root.path().join("attacker-storage");
        fs::create_dir(&selected).expect("create selected journal directory");
        fs::create_dir(&attacker).expect("create attacker journal directory");
        fs::set_permissions(&selected, fs::Permissions::from_mode(0o700))
            .expect("secure selected journal directory");
        fs::set_permissions(&attacker, fs::Permissions::from_mode(0o700))
            .expect("secure attacker journal directory");
        fs::write(attacker.join("sentinel"), b"attacker sentinel")
            .expect("write attacker sentinel");
        let selected_for_hook = selected.clone();
        let captured_for_hook = captured.clone();
        let attacker_for_hook = attacker.clone();
        install_race_hook("capture-child-after-lstat", move || {
            fs::rename(&selected_for_hook, &captured_for_hook)
                .expect("displace selected journal directory");
            fs::rename(&attacker_for_hook, &selected_for_hook)
                .expect("install attacker journal directory");
        });

        let error = JournalStore::new(root.path())
            .expect_err("nested capture must reject a child swapped after parent capture");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected.join("sentinel")).expect("attacker sentinel remains"),
            b"attacker sentinel"
        );
    }

    #[test]
    fn ownership_probe_cleanup_survives_a_failure_after_creation() {
        let root = tempfile::tempdir().expect("temporary metadata root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure metadata root");
        install_race_hook("ownership-probe-after-create", || {
            panic!("injected crash cut after probe creation")
        });

        let failure = std::panic::catch_unwind(|| SecureMetadataDirectory::new(root.path()));

        assert!(failure.is_err());
        assert!(
            fs::read_dir(root.path())
                .expect("list metadata root")
                .next()
                .is_none(),
            "ownership probe must not remain linked"
        );
        SecureMetadataDirectory::new(root.path()).expect("recovery capture is not blocked");
    }

    #[test]
    fn ownership_probe_is_unlinked_before_later_capture_work() {
        let root = tempfile::tempdir().expect("temporary metadata root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure metadata root");
        install_race_hook("ownership-probe-after-unlink", || {
            panic!("injected crash cut after probe unlink")
        });

        let failure = std::panic::catch_unwind(|| SecureMetadataDirectory::new(root.path()));

        assert!(failure.is_err());
        assert!(
            fs::read_dir(root.path())
                .expect("list metadata root")
                .next()
                .is_none(),
            "unlinked ownership probe must not reappear"
        );
        SecureMetadataDirectory::new(root.path()).expect("recovery capture is not blocked");
    }

    fn replace_child_with_attacker(
        selected: PathBuf,
        displaced: PathBuf,
        bytes: &'static [u8],
    ) -> impl FnOnce() {
        move || {
            fs::rename(&selected, &displaced).expect("displace verified child");
            fs::write(&selected, bytes).expect("install attacker child");
            fs::set_permissions(&selected, fs::Permissions::from_mode(0o600))
                .expect("secure attacker child");
        }
    }

    #[test]
    fn public_metadata_replace_refuses_a_child_swapped_after_verification() {
        let (root, metadata) = metadata_directory();
        metadata
            .create("record", b"trusted")
            .expect("create record");
        let selected = root.path().join("record");
        let displaced = root.path().join("trusted-away");
        install_race_hook(
            "metadata-replace-before-rename",
            replace_child_with_attacker(selected.clone(), displaced.clone(), b"attacker sentinel"),
        );

        let error = metadata
            .replace("record", b"new trusted")
            .expect_err("refuse replacement after verified inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert_eq!(fs::read(displaced).expect("trusted remains"), b"trusted");
    }

    #[test]
    fn public_metadata_create_never_overwrites_a_late_attacker_child() {
        let (root, metadata) = metadata_directory();
        let selected = root.path().join("record");
        let selected_for_hook = selected.clone();
        install_race_hook("metadata-create-before-commit", move || {
            fs::write(&selected_for_hook, b"attacker sentinel").expect("install attacker child");
            fs::set_permissions(&selected_for_hook, fs::Permissions::from_mode(0o600))
                .expect("secure attacker child");
        });

        let error = metadata
            .create("record", b"trusted")
            .expect_err("exclusive commit rejects late child");

        assert!(matches!(error, StorageError::Journal(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
    }

    #[test]
    fn public_metadata_remove_refuses_a_child_swapped_after_verification() {
        let (root, metadata) = metadata_directory();
        metadata
            .create("record", b"trusted")
            .expect("create record");
        let selected = root.path().join("record");
        let displaced = root.path().join("trusted-away");
        install_race_hook(
            "metadata-remove-before-rename",
            replace_child_with_attacker(selected.clone(), displaced.clone(), b"attacker sentinel"),
        );

        let error = metadata
            .remove("record")
            .expect_err("refuse removal after verified inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert_eq!(fs::read(displaced).expect("trusted remains"), b"trusted");
    }

    #[test]
    fn public_metadata_subdirectory_remove_refuses_a_swapped_child() {
        let (root, metadata) = metadata_directory();
        let child = metadata
            .create_subdirectory("session")
            .expect("create pinned child");
        let selected = root.path().join("session");
        let displaced = root.path().join("trusted-session-away");
        let selected_for_hook = selected.clone();
        let displaced_for_hook = displaced.clone();
        install_race_hook("metadata-subdirectory-before-remove", move || {
            fs::rename(&selected_for_hook, &displaced_for_hook)
                .expect("displace verified directory");
            fs::create_dir(&selected_for_hook).expect("install attacker directory");
            fs::set_permissions(&selected_for_hook, fs::Permissions::from_mode(0o700))
                .expect("secure attacker directory");
            fs::write(selected_for_hook.join("sentinel"), b"attacker sentinel")
                .expect("write attacker sentinel");
        });

        let error = metadata
            .remove_subdirectory("session", &child)
            .expect_err("refuse removal after pinned directory is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected.join("sentinel")).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(displaced.is_dir());
    }

    #[test]
    fn public_subdirectory_creation_refuses_a_new_child_swapped_during_capture() {
        let (root, metadata) = metadata_directory();
        let selected = root.path().join("session");
        let displaced = root.path().join("created-away");
        let selected_for_hook = selected.clone();
        let displaced_for_hook = displaced.clone();
        install_race_hook("child-metadata-after-lstat", move || {
            fs::rename(&selected_for_hook, &displaced_for_hook)
                .expect("displace newly created directory");
            fs::create_dir(&selected_for_hook).expect("install attacker directory");
            fs::set_permissions(&selected_for_hook, fs::Permissions::from_mode(0o700))
                .expect("secure attacker directory");
            fs::write(selected_for_hook.join("sentinel"), b"attacker sentinel")
                .expect("write attacker sentinel");
        });

        let error = metadata
            .create_subdirectory("session")
            .expect_err("reject new directory swapped while capturing its identity");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected.join("sentinel")).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(displaced.is_dir());
    }

    #[test]
    fn public_journal_write_refuses_a_child_swapped_after_verification() {
        let (_root, store, layout, journal) = journal_fixture();
        store.create(&layout, &journal).expect("create journal");
        let selected = layout.journal.clone();
        let displaced = layout.journal.with_extension("trusted-away");
        install_race_hook(
            "journal-write-before-rename",
            replace_child_with_attacker(selected.clone(), displaced.clone(), b"attacker sentinel"),
        );

        let error = store
            .write(&layout, &journal)
            .expect_err("refuse journal write after verified inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(fs::read(displaced)
            .expect("trusted remains")
            .starts_with(b"{"));
    }

    #[test]
    fn public_journal_remove_refuses_a_child_swapped_after_verification() {
        let (_root, store, layout, journal) = journal_fixture();
        store.create(&layout, &journal).expect("create journal");
        let selected = layout.journal.clone();
        let displaced = layout.journal.with_extension("trusted-away");
        install_race_hook(
            "journal-remove-before-rename",
            replace_child_with_attacker(selected.clone(), displaced.clone(), b"attacker sentinel"),
        );

        let error = store
            .remove(&layout)
            .expect_err("refuse journal removal after verified inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(fs::read(displaced)
            .expect("trusted remains")
            .starts_with(b"{"));
    }

    #[test]
    fn descriptor_relative_file_access_stays_in_captured_directory_after_path_swap() {
        let (_root, pinned, captured, attacker) = swapped_directory();
        fs::write(captured.join("existing"), b"trusted").expect("write trusted child");
        fs::set_permissions(captured.join("existing"), fs::Permissions::from_mode(0o600))
            .expect("secure trusted child");
        fs::write(attacker.join("existing"), b"attacker sentinel")
            .expect("write attacker sentinel");
        fs::set_permissions(attacker.join("existing"), fs::Permissions::from_mode(0o600))
            .expect("secure attacker sentinel");

        let mut opened = pinned.open_file("existing").expect("open captured child");
        let mut bytes = Vec::new();
        opened.read_to_end(&mut bytes).expect("read captured child");
        assert_eq!(bytes, b"trusted");
        let expected = fs::symlink_metadata(captured.join("existing")).expect("trusted metadata");
        let observed = pinned
            .child_metadata("existing")
            .expect("descriptor-relative metadata")
            .expect("captured child exists");
        assert_eq!(
            (observed.dev(), observed.ino()),
            (expected.dev(), expected.ino())
        );

        let mut created = pinned
            .create_file("created")
            .expect("create captured child");
        created
            .write_all(b"created safely")
            .expect("write captured child");
        created.sync_all().expect("sync captured child");
        assert_eq!(
            fs::read(captured.join("created")).expect("read created captured child"),
            b"created safely"
        );
        assert!(!attacker.join("created").exists());
        assert_eq!(
            fs::read(attacker.join("existing")).expect("read attacker sentinel"),
            b"attacker sentinel"
        );
    }

    #[test]
    fn descriptor_relative_mutations_leave_replacement_path_untouched() {
        let (_root, pinned, captured, attacker) = swapped_directory();
        fs::write(captured.join("record.tmp"), b"trusted").expect("write trusted temporary");
        fs::set_permissions(
            captured.join("record.tmp"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("secure trusted temporary");
        fs::write(attacker.join("record.tmp"), b"attacker sentinel")
            .expect("write attacker temporary");
        fs::set_permissions(
            attacker.join("record.tmp"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("secure attacker temporary");

        pinned
            .rename("record.tmp", "record")
            .expect("rename captured child");
        assert_eq!(
            fs::read(captured.join("record")).expect("read renamed captured child"),
            b"trusted"
        );
        assert_eq!(
            fs::read(attacker.join("record.tmp")).expect("read attacker sentinel"),
            b"attacker sentinel"
        );
        assert!(!attacker.join("record").exists());

        pinned.remove_file("record").expect("remove captured child");
        assert!(!captured.join("record").exists());
        assert_eq!(
            fs::read(attacker.join("record.tmp")).expect("read attacker sentinel after removal"),
            b"attacker sentinel"
        );

        pinned
            .create_directory("session")
            .expect("create captured directory");
        assert!(captured.join("session").is_dir());
        assert!(!attacker.join("session").exists());
        pinned
            .remove_directory("session")
            .expect("remove captured directory");
        assert!(!captured.join("session").exists());
        assert!(!attacker.join("session").exists());
    }

    #[test]
    fn descriptor_relative_listing_reads_only_the_captured_directory() {
        let (_root, pinned, captured, attacker) = swapped_directory();
        fs::write(captured.join("trusted"), b"trusted").expect("write trusted child");
        fs::write(attacker.join("attacker"), b"attacker").expect("write attacker child");

        assert_eq!(
            pinned.child_names().expect("list captured directory"),
            vec!["trusted".to_owned()]
        );
        assert_eq!(
            fs::read(attacker.join("attacker")).expect("read attacker child"),
            b"attacker"
        );
    }

    #[test]
    fn subdirectory_creation_removes_exact_child_when_parent_fsync_fails() {
        let (root, metadata) = metadata_directory();

        let error = metadata
            .create_subdirectory_with(
                "capture-fsync-failure",
                |directory, name| {
                    directory.child_metadata(name)?.ok_or_else(|| {
                        StorageError::IdentityMismatch(
                            "metadata subdirectory disappeared".to_owned(),
                        )
                    })
                },
                |_| {
                    Err(StorageError::Journal(
                        "injected parent fsync failure".to_owned(),
                    ))
                },
                |directory, name| {
                    Ok(SecureMetadataDirectory {
                        directory: directory.capture_child(name, "metadata subdirectory")?,
                    })
                },
            )
            .expect_err("surface injected parent fsync failure");

        assert!(matches!(error, StorageError::Journal(message) if message.contains("injected")));
        assert!(!root.path().join("capture-fsync-failure").exists());
    }

    #[test]
    fn subdirectory_creation_removes_exact_child_when_pinning_fails() {
        let (root, metadata) = metadata_directory();

        let error = metadata
            .create_subdirectory_with(
                "capture-pin-failure",
                |directory, name| {
                    directory.child_metadata(name)?.ok_or_else(|| {
                        StorageError::IdentityMismatch(
                            "metadata subdirectory disappeared".to_owned(),
                        )
                    })
                },
                PinnedDirectory::sync,
                |_, _| {
                    Err::<SecureMetadataDirectory, _>(StorageError::IdentityMismatch(
                        "injected child pin failure".to_owned(),
                    ))
                },
            )
            .expect_err("surface injected child pin failure");

        assert!(
            matches!(error, StorageError::IdentityMismatch(message) if message.contains("injected"))
        );
        assert!(!root.path().join("capture-pin-failure").exists());
    }

    #[test]
    fn subdirectory_creation_metadata_failure_removes_only_the_created_child() {
        let (root, metadata) = metadata_directory();
        let foreign = root.path().join("foreign");
        fs::create_dir(&foreign).expect("create foreign directory");
        fs::write(foreign.join("sentinel"), b"still safe").expect("write foreign sentinel");

        let error = metadata
            .create_subdirectory_with(
                "capture-metadata-failure",
                |_, _| {
                    Err(StorageError::Journal(
                        "injected child metadata failure".to_owned(),
                    ))
                },
                PinnedDirectory::sync,
                |directory, name| {
                    Ok(SecureMetadataDirectory {
                        directory: directory.capture_child(name, "metadata subdirectory")?,
                    })
                },
            )
            .expect_err("surface injected metadata failure");

        assert!(matches!(error, StorageError::Journal(message) if message.contains("injected")));
        assert!(!root.path().join("capture-metadata-failure").exists());
        assert_eq!(
            fs::read(foreign.join("sentinel")).expect("read foreign sentinel"),
            b"still safe"
        );
    }
}
