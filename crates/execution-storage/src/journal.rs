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
        Arc, Mutex, MutexGuard,
    },
};

#[cfg(test)]
type RaceHook = Option<(String, Box<dyn FnOnce()>)>;

#[cfg(test)]
thread_local! {
    static RACE_HOOK: std::cell::RefCell<RaceHook> =
        const { std::cell::RefCell::new(None) };
    static TEMPORARY_NAME_FIXTURES: std::cell::RefCell<std::collections::VecDeque<String>> =
        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
    static PROBE_NAME_FIXTURES: std::cell::RefCell<std::collections::VecDeque<String>> =
        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
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

#[cfg(test)]
fn install_temporary_name_fixtures(names: impl IntoIterator<Item = &'static str>) {
    TEMPORARY_NAME_FIXTURES.with(|slot| {
        *slot.borrow_mut() = names.into_iter().map(str::to_owned).collect();
    });
}

#[cfg(test)]
fn install_probe_name_fixtures(names: impl IntoIterator<Item = &'static str>) {
    PROBE_NAME_FIXTURES.with(|slot| {
        *slot.borrow_mut() = names.into_iter().map(str::to_owned).collect();
    });
}

#[cfg(not(test))]
fn race_hook(_: &str) {}

static OWNER_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);
const MAX_TEMPORARY_NAME_ATTEMPTS: usize = 32;
const MAX_IGNORED_RESIDUE_ENTRIES: usize = 1024;

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

#[cfg(unix)]
#[derive(Clone, Copy)]
struct DescriptorIdentity {
    device: u64,
    inode: u64,
    uid: u32,
    mode: u32,
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
        let (temporary, file) = self
            .directory
            .create_temporary_file(&format!("metadata-{name}"))?;
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
        if let Err(error) = self.directory.commit_noreplace_verified(
            &temporary,
            name,
            &temporary_identity,
            "metadata-create-before-commit",
            None,
        ) {
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
        let (temporary, file) = self
            .directory
            .create_temporary_file(&format!("metadata-{name}"))?;
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
            &temporary_identity,
            &expected,
            "metadata-replace-before-rename",
            "metadata-replace-before-final-authentication",
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
        let tombstone = self.directory.rename_verified(
            name,
            &format!("remove-metadata-{name}"),
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
        let mut ignored_residue = 0usize;
        for name in names {
            if is_operation_temporary_name(&name) || is_owner_probe_name(&name) {
                self.directory.child_metadata(&name)?.ok_or_else(|| {
                    StorageError::IdentityMismatch(
                        "temporary residue disappeared during inspection".to_owned(),
                    )
                })?;
                ignored_residue += 1;
                if ignored_residue > MAX_IGNORED_RESIDUE_ENTRIES {
                    return Err(residue_limit_error(&self.directory.path));
                }
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
        let tombstone = self.directory.rename_verified(
            name,
            &format!("remove-tool-{name}"),
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
            PinnedDirectory::sync,
            |directory, child, handle, expected| {
                Ok(Self {
                    directory: directory.pin_authenticated_child(
                        child,
                        handle,
                        expected,
                        "metadata subdirectory",
                    )?,
                })
            },
        )
    }

    #[cfg(unix)]
    fn create_subdirectory_with<Sync, Pin>(
        &self,
        name: &str,
        sync_parent: Sync,
        pin_child: Pin,
    ) -> Result<Self, StorageError>
    where
        Sync: FnOnce(&PinnedDirectory) -> Result<(), StorageError>,
        Pin: FnOnce(&PinnedDirectory, &str, File, &fs::Metadata) -> Result<Self, StorageError>,
    {
        validate_metadata_name(name)?;
        self.directory.verify("metadata directory")?;
        if self.directory.child_metadata(name)?.is_some() {
            return Err(StorageError::Journal(format!(
                "metadata subdirectory already exists: {name}"
            )));
        }
        let operation = format!("metadata-subdirectory-{name}");
        let (staged, handle, expected) = self
            .directory
            .create_temporary_directory(&operation, "metadata-subdirectory-after-staged-mkdir")?;
        let result = (|| {
            sync_parent(&self.directory)?;
            self.directory.commit_directory_noreplace_verified(
                &staged,
                name,
                &expected,
                "metadata-subdirectory-before-commit",
                "metadata-subdirectory-before-final-authentication",
            )?;
            self.directory.sync()?;
            let child = pin_child(&self.directory, name, handle, &expected)?;
            self.directory.verify("metadata directory")?;
            Ok(child)
        })();
        match result {
            Ok(child) => Ok(child),
            Err(error) => match self.rollback_created_subdirectory(&staged, name, &expected) {
                Ok(()) => Err(error),
                Err(rollback) => {
                    let message =
                        format!("{error}; rollback metadata subdirectory failed: {rollback}");
                    if matches!(error, StorageError::IdentityMismatch(_))
                        || matches!(rollback, StorageError::IdentityMismatch(_))
                    {
                        Err(StorageError::IdentityMismatch(message))
                    } else {
                        Err(StorageError::Journal(message))
                    }
                }
            },
        }
    }

    #[cfg(unix)]
    fn rollback_created_subdirectory(
        &self,
        staged: &str,
        final_name: &str,
        expected: &fs::Metadata,
    ) -> Result<(), StorageError> {
        if self.directory.child_metadata(staged)?.is_some() {
            return self.directory.remove_directory_verified(staged, expected);
        }
        if self.directory.child_metadata(final_name)?.is_some() {
            return self
                .directory
                .remove_directory_verified(final_name, expected);
        }
        Err(StorageError::IdentityMismatch(
            "created metadata subdirectory disappeared; preserve unverified state".to_owned(),
        ))
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
        let tombstone = self.directory.rename_verified(
            name,
            &format!("remove-directory-{name}"),
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
        if self.journal_directory.child_metadata(&name)?.is_some() {
            return Err(StorageError::Journal(format!(
                "journal already exists: {}",
                layout.journal.display()
            )));
        }
        let (temporary, file) = self.journal_directory.create_temporary_file(&name)?;
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
        if let Err(error) = self.journal_directory.commit_noreplace_verified(
            &temporary,
            &name,
            &temporary_identity,
            "journal-create-before-commit",
            Some("journal-create-before-final-authentication"),
        ) {
            let _ = self
                .journal_directory
                .remove_file_verified(&temporary, &temporary_identity);
            return Err(error);
        }
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
        let (temporary, file) = self.journal_directory.create_temporary_file(&name)?;
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
            &temporary_identity,
            &expected,
            "journal-write-before-rename",
            "journal-write-before-final-authentication",
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
        let tombstone = self.journal_directory.rename_verified(
            &name,
            &format!("remove-{name}"),
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
        let mut ignored_residue = 0usize;
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
                ignored_residue += 1;
                if ignored_residue > MAX_IGNORED_RESIDUE_ENTRIES {
                    return Err(residue_limit_error(&self.journal_directory.path));
                }
                continue;
            }
            if is_journal_temporary_name(&name) {
                self.journal_directory.open_file(&name)?;
                ignored_residue += 1;
                if ignored_residue > MAX_IGNORED_RESIDUE_ENTRIES {
                    return Err(residue_limit_error(&self.journal_directory.path));
                }
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
    #[cfg(unix)]
    process_lock: Arc<Mutex<()>>,
}

#[cfg(unix)]
struct DirectoryLock<'a> {
    handle: &'a File,
    _process_lock: MutexGuard<'a, ()>,
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
        let process_lock = self.process_lock.lock().map_err(|_| {
            StorageError::Journal(format!(
                "process-local directory lock poisoned: {}",
                self.path.display()
            ))
        })?;
        fs2::FileExt::lock_exclusive(&*self.handle)
            .map_err(|error| journal_error("lock directory", &self.path, error))?;
        Ok(DirectoryLock {
            handle: &self.handle,
            _process_lock: process_lock,
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
                process_lock: Arc::new(Mutex::new(())),
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
            process_lock: Arc::new(Mutex::new(())),
        })
    }

    #[cfg(unix)]
    fn pin_authenticated_child(
        &self,
        name: &str,
        handle: File,
        expected: &fs::Metadata,
        purpose: &str,
    ) -> Result<Self, StorageError> {
        validate_descriptor_name(name)?;
        let path = self.path.join(name);
        let opened = handle
            .metadata()
            .map_err(|error| journal_error("inspect authenticated directory", &path, error))?;
        verify_directory_identity(expected, &opened, purpose)?;
        if opened.mode() & 0o077 != 0 {
            return Err(StorageError::Unavailable(format!(
                "{purpose} mode {:o} permits group or world access",
                opened.mode() & 0o777
            )));
        }
        self.verify_child_identity(name, expected)?;
        verify_current_owner(&handle, &path, opened.uid(), purpose)?;
        Ok(Self {
            path,
            device: opened.dev(),
            inode: opened.ino(),
            uid: opened.uid(),
            handle: Arc::new(handle),
            process_lock: Arc::new(Mutex::new(())),
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
    fn create_temporary_file(&self, operation: &str) -> Result<(String, File), StorageError> {
        for _ in 0..MAX_TEMPORARY_NAME_ATTEMPTS {
            let name = temporary_name(operation)?;
            let path = self.child_path(&name)?;
            match rustix::fs::openat(
                &*self.handle,
                name.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_bits_truncate(0o600),
            ) {
                Ok(fd) => return Ok((name, File::from(fd))),
                Err(rustix::io::Errno::EXIST) => continue,
                Err(error) => return Err(rustix_error("create temporary", &path, error)),
            }
        }
        Err(StorageError::Journal(format!(
            "temporary namespace collision limit reached in {}",
            self.path.display()
        )))
    }

    #[cfg(unix)]
    fn create_temporary_directory(
        &self,
        operation: &str,
        after_mkdir_hook: &str,
    ) -> Result<(String, File, fs::Metadata), StorageError> {
        for _ in 0..MAX_TEMPORARY_NAME_ATTEMPTS {
            let name = temporary_name(operation)?;
            let path = self.child_path(&name)?;
            match rustix::fs::mkdirat(
                &*self.handle,
                name.as_str(),
                Mode::from_bits_truncate(0o700),
            ) {
                Ok(()) => {}
                Err(rustix::io::Errno::EXIST) => continue,
                Err(error) => return Err(rustix_error("create temporary directory", &path, error)),
            }
            let stat = rustix::fs::statat(&*self.handle, name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|error| {
                    rustix_error("inspect created temporary directory", &path, error)
                })?;
            let expected = DescriptorIdentity {
                device: stat.st_dev as u64,
                inode: stat.st_ino as u64,
                uid: stat.st_uid,
                mode: stat.st_mode as u32,
            };
            if FileType::from_raw_mode(stat.st_mode) != FileType::Directory
                || expected.mode & 0o077 != 0
                || expected.uid != self.uid
            {
                return Err(StorageError::IdentityMismatch(
                    "new staged metadata directory has an invalid type, owner, or mode; preserve unverified state"
                        .to_owned(),
                ));
            }
            race_hook(after_mkdir_hook);
            let fd = rustix::fs::openat(
                &*self.handle,
                name.as_str(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| rustix_error("open created temporary directory", &path, error))?;
            let handle = File::from(fd);
            let opened = handle.metadata().map_err(|error| {
                journal_error("inspect created temporary directory", &path, error)
            })?;
            if expected.device != opened.dev()
                || expected.inode != opened.ino()
                || expected.uid != opened.uid()
                || expected.mode != opened.mode()
                || !opened.is_dir()
            {
                return Err(StorageError::IdentityMismatch(
                    "staged metadata directory changed before authentication; preserve all staged objects"
                        .to_owned(),
                ));
            }
            self.verify_child_identity(&name, &opened)?;
            return Ok((name, handle, opened));
        }
        Err(StorageError::Journal(format!(
            "temporary directory namespace collision limit reached in {}",
            self.path.display()
        )))
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
    fn commit_noreplace_verified(
        &self,
        source: &str,
        target: &str,
        expected_source: &fs::Metadata,
        before_commit_hook: &str,
        before_final_authentication_hook: Option<&str>,
    ) -> Result<(), StorageError> {
        self.child_path(source)?;
        self.child_path(target)?;
        race_hook(before_commit_hook);
        self.verify_child_identity(source, expected_source)?;
        if self.child_metadata(target)?.is_some() {
            return Err(StorageError::IdentityMismatch(format!(
                "commit target appeared before no-replace rename: {}",
                self.path.join(target).display()
            )));
        }
        self.rename_noreplace(source, target)?;
        if let Some(hook) = before_final_authentication_hook {
            race_hook(hook);
        }
        self.verify_child_identity(target, expected_source)
    }

    #[cfg(unix)]
    fn commit_directory_noreplace_verified(
        &self,
        source: &str,
        target: &str,
        expected_source: &fs::Metadata,
        before_commit_hook: &str,
        before_final_authentication_hook: &str,
    ) -> Result<(), StorageError> {
        self.child_path(source)?;
        self.child_path(target)?;
        race_hook(before_commit_hook);
        self.verify_child_identity(source, expected_source)?;
        if self.child_metadata(target)?.is_some() {
            return Err(StorageError::IdentityMismatch(format!(
                "directory commit target appeared before no-replace rename: {}",
                self.path.join(target).display()
            )));
        }
        self.rename_noreplace(source, target)?;
        race_hook(before_final_authentication_hook);
        let source_result = match self.child_metadata(source)? {
            None => Ok(()),
            Some(_) => Err(StorageError::IdentityMismatch(format!(
                "staged directory source reappeared after commit: {}",
                self.path.join(source).display()
            ))),
        };
        let final_result = self.verify_child_identity(target, expected_source);
        match (source_result, final_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(source_error), Err(final_error)) => Err(StorageError::Journal(format!(
                "{source_error}; final directory also failed authentication: {final_error}"
            ))),
        }
    }

    #[cfg(unix)]
    fn exchange_verified(
        &self,
        replacement: &str,
        target: &str,
        expected_replacement: &fs::Metadata,
        expected_target: &fs::Metadata,
        before_exchange_hook: &str,
        before_final_authentication_hook: &str,
    ) -> Result<(), StorageError> {
        self.child_path(replacement)?;
        self.child_path(target)?;
        race_hook(before_exchange_hook);
        self.verify_child_identity(replacement, expected_replacement)?;
        self.verify_child_identity(target, expected_target)?;
        rustix::fs::renameat_with(
            &*self.handle,
            replacement,
            &*self.handle,
            target,
            RenameFlags::EXCHANGE,
        )
        .map_err(|error| rustix_error("atomically exchange", &self.path.join(target), error))?;
        race_hook(before_final_authentication_hook);
        let final_target = self.verify_child_identity(target, expected_replacement);
        let displaced_target = self.verify_child_identity(replacement, expected_target);
        match (final_target, displaced_target) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(target_error), Err(replacement_error)) => Err(StorageError::Journal(format!(
                "{target_error}; displaced exchange side also failed authentication: {replacement_error}"
            ))),
        }
    }

    #[cfg(unix)]
    fn rename_verified(
        &self,
        source: &str,
        tombstone_operation: &str,
        expected: &fs::Metadata,
        hook: &str,
    ) -> Result<String, StorageError> {
        race_hook(hook);
        self.verify_child_identity(source, expected)?;
        for _ in 0..MAX_TEMPORARY_NAME_ATTEMPTS {
            let tombstone = temporary_name(tombstone_operation)?;
            let path = self.child_path(&tombstone)?;
            match rustix::fs::renameat_with(
                &*self.handle,
                source,
                &*self.handle,
                tombstone.as_str(),
                RenameFlags::NOREPLACE,
            ) {
                Ok(()) => {
                    self.verify_child_identity(&tombstone, expected)?;
                    return Ok(tombstone);
                }
                Err(rustix::io::Errno::EXIST) => continue,
                Err(error) => return Err(rustix_error("rename without replacement", &path, error)),
            }
        }
        Err(StorageError::Journal(format!(
            "temporary namespace collision limit reached in {}",
            self.path.display()
        )))
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
        let fd = rustix::fs::openat(
            &*self.handle,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| rustix_error("open created directory", &path, error))?;
        let created = File::from(fd);
        race_hook("directory-create-before-stat");
        let expected = created
            .metadata()
            .map_err(|error| journal_error("inspect created directory", &path, error))?;
        let observed = self.child_metadata(name)?.ok_or_else(|| {
            StorageError::IdentityMismatch(
                "new metadata child disappeared; preserve unverified state".to_owned(),
            )
        })?;
        verify_directory_identity(&expected, &observed, "new metadata child")?;
        if expected.mode() & 0o077 != 0 {
            return Err(StorageError::IdentityMismatch(
                "new metadata child is not owner-only; preserve unverified state".to_owned(),
            ));
        }
        Ok(expected)
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
        let tombstone = self.rename_verified(
            name,
            &format!("remove-directory-{name}"),
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
        let tombstone = self.rename_verified(
            name,
            &format!("remove-directory-{name}"),
            expected,
            "directory-remove-before-rename",
        )?;
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
    let (name, fd) = (0..MAX_TEMPORARY_NAME_ATTEMPTS)
        .find_map(|_| {
            let name = match next_ownership_probe_name() {
                Ok(name) => name,
                Err(error) => return Some(Err(error)),
            };
            let probe = directory.join(&name);
            match rustix::fs::openat(
                handle,
                name.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_bits_truncate(0o600),
            ) {
                Ok(fd) => Some(Ok((name, fd))),
                Err(rustix::io::Errno::EXIST) => None,
                Err(error) => Some(Err(rustix_error("create ownership probe", &probe, error))),
            }
        })
        .transpose()?
        .ok_or_else(|| {
            StorageError::Journal(format!(
                "ownership probe namespace collision limit reached in {}",
                directory.display()
            ))
        })?;
    let probe = directory.join(&name);
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
    #[cfg(test)]
    if let Some(name) = TEMPORARY_NAME_FIXTURES.with(|slot| slot.borrow_mut().pop_front()) {
        validate_descriptor_name(&name)?;
        return Ok(name);
    }
    let epoch_nanos = epoch_nanos()?;
    Ok(format!(
        ".{name}.{}.{epoch_nanos:x}.{}.tmp",
        std::process::id(),
        OWNER_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

fn epoch_nanos() -> Result<u128, StorageError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| StorageError::Journal(format!("system clock before epoch: {error}")))
        .map(|duration| duration.as_nanos())
}

fn ownership_probe_name() -> Result<String, StorageError> {
    Ok(format!(
        ".autospec-owner-{}-{:x}-{}",
        std::process::id(),
        epoch_nanos()?,
        OWNER_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

fn next_ownership_probe_name() -> Result<String, StorageError> {
    #[cfg(test)]
    if let Some(name) = PROBE_NAME_FIXTURES.with(|slot| slot.borrow_mut().pop_front()) {
        validate_descriptor_name(&name)?;
        return Ok(name);
    }
    ownership_probe_name()
}

fn is_journal_temporary_name(name: &str) -> bool {
    temporary_operation(name)
        .or_else(|| legacy_temporary_operation(name))
        .is_some_and(|operation| operation.ends_with(".json"))
}

fn is_operation_temporary_name(name: &str) -> bool {
    temporary_operation(name).is_some() || legacy_temporary_operation(name).is_some()
}

fn legacy_temporary_operation(name: &str) -> Option<&str> {
    let rest = name.strip_prefix('.')?;
    let without_suffix = rest.strip_suffix(".tmp")?;
    let (prefix, counter) = without_suffix.rsplit_once('.')?;
    let (operation, process) = prefix.rsplit_once('.')?;
    (!operation.is_empty()
        && process.bytes().all(|byte| byte.is_ascii_digit())
        && !counter.is_empty()
        && counter.bytes().all(|byte| byte.is_ascii_digit()))
    .then_some(operation)
}

fn temporary_operation(name: &str) -> Option<&str> {
    let rest = name.strip_prefix('.')?;
    let without_suffix = rest.strip_suffix(".tmp")?;
    let (prefix, counter) = without_suffix.rsplit_once('.')?;
    let (prefix, epoch) = prefix.rsplit_once('.')?;
    let (operation, process) = prefix.rsplit_once('.')?;
    (!operation.is_empty()
        && process.bytes().all(|byte| byte.is_ascii_digit())
        && !epoch.is_empty()
        && epoch.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !counter.is_empty()
        && counter.bytes().all(|byte| byte.is_ascii_digit()))
    .then_some(operation)
}

fn is_owner_probe_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(".autospec-owner-") else {
        return false;
    };
    let mut components = rest.split('-');
    let process = components.next();
    let epoch = components.next();
    let counter = components.next();
    let current = components.next().is_none()
        && process.is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        })
        && epoch.is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        && counter.is_some_and(|value| {
            !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
        });
    current
        || rest.split_once('-').is_some_and(|(process, counter)| {
            !process.is_empty()
                && !counter.is_empty()
                && process.bytes().all(|byte| byte.is_ascii_digit())
                && counter.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn residue_limit_error(directory: &Path) -> StorageError {
    StorageError::Journal(format!(
        "unauthenticated temporary residue limit exceeded in {}; preserve entries for operator inspection",
        directory.display()
    ))
}

fn journal_error(action: &str, path: &Path, error: std::io::Error) -> StorageError {
    StorageError::Journal(format!("{action} {}: {error}", path.display()))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use orchestrator_core::WorkerId;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::sync::mpsc;
    use std::time::Duration;

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

    fn assert_second_create_waits_for_first(
        first: Arc<SecureMetadataDirectory>,
        second: Arc<SecureMetadataDirectory>,
    ) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let first_thread = std::thread::spawn(move || {
            install_race_hook("metadata-create-before-commit", move || {
                entered_tx.send(()).expect("announce held mutation");
                release_rx.recv().expect("release held mutation");
            });
            first.create("first", b"first").expect("first create");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first mutation reached lock-protected commit");
        let second_thread = std::thread::spawn(move || {
            let result = second.create("second", b"second");
            completed_tx.send(result).expect("report second mutation");
        });

        assert!(
            completed_rx
                .recv_timeout(Duration::from_millis(150))
                .is_err(),
            "second public mutation entered while first still held the directory lock"
        );
        release_tx.send(()).expect("release first mutation");
        first_thread.join().expect("first mutation thread");
        completed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second mutation completes after unlock")
            .expect("second create");
        second_thread.join().expect("second mutation thread");
    }

    #[test]
    fn concurrent_calls_on_the_same_metadata_instance_are_excluded() {
        let (_root, metadata) = metadata_directory();
        let shared = Arc::new(metadata);
        assert_second_create_waits_for_first(Arc::clone(&shared), shared);
    }

    #[test]
    fn concurrent_calls_on_cloned_metadata_instances_are_excluded() {
        let (_root, metadata) = metadata_directory();
        let cloned = metadata.clone();
        assert_second_create_waits_for_first(Arc::new(metadata), Arc::new(cloned));
    }

    #[test]
    fn concurrent_calls_on_independently_captured_metadata_instances_are_excluded() {
        let (root, first) = metadata_directory();
        let second = SecureMetadataDirectory::new(root.path()).expect("capture same root again");
        assert_second_create_waits_for_first(Arc::new(first), Arc::new(second));
    }

    #[test]
    fn killed_subprocess_stale_probe_does_not_block_later_capture() {
        const CHILD_ENV: &str = "AUTOSPEC_STALE_PROBE_CHILD";
        if let Ok(root) = std::env::var(CHILD_ENV) {
            let root = PathBuf::from(root);
            let hook_root = root.clone();
            install_race_hook("ownership-probe-after-create", move || {
                let probe = fs::read_dir(&hook_root)
                    .expect("list production ownership probes")
                    .map(|entry| entry.expect("read ownership probe entry").path())
                    .find(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(is_owner_probe_name)
                    })
                    .expect("production ownership probe is linked");
                fs::write(&probe, b"stale production probe sentinel")
                    .expect("write through production probe path");
                let readiness_temporary = hook_root.join("child-ready.tmp");
                fs::write(&readiness_temporary, probe.as_os_str().as_encoded_bytes())
                    .expect("write stale probe readiness");
                fs::rename(readiness_temporary, hook_root.join("child-ready"))
                    .expect("publish stale probe readiness atomically");
                loop {
                    std::thread::park();
                }
            });
            let _ = SecureMetadataDirectory::new(&root);
            return;
        }

        let root = tempfile::tempdir().expect("temporary metadata root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure metadata root");
        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "journal::tests::killed_subprocess_stale_probe_does_not_block_later_capture",
                "--nocapture",
            ])
            .env(CHILD_ENV, root.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn probe subprocess");
        let ready = root.path().join("child-ready");
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "probe subprocess reached crash cut");
        child.kill().expect("kill probe subprocess");
        assert!(!child.wait().expect("reap probe subprocess").success());
        let stale_probe = PathBuf::from(
            String::from_utf8(fs::read(&ready).expect("read stale probe path"))
                .expect("UTF-8 stale probe path"),
        );
        fs::remove_file(&ready).expect("remove parent-owned readiness marker");

        let metadata = SecureMetadataDirectory::new(root.path())
            .expect("stale probe never blocks later capture");
        assert!(metadata
            .names()
            .expect("ignore unauthenticated probe-shaped residue")
            .is_empty());
        assert_eq!(
            fs::read(&stale_probe).expect("stale probe is preserved"),
            b"stale production probe sentinel"
        );
        metadata
            .create("record", b"trusted")
            .expect("captured directory remains usable");
    }

    #[test]
    fn excessive_unauthenticated_temporary_residue_fails_closed() {
        let (root, metadata) = metadata_directory();
        for index in 0..=1024 {
            let path = root
                .path()
                .join(format!(".metadata-record.1.{index:x}.0.tmp"));
            fs::write(&path, b"untrusted residue").expect("create residue");
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                .expect("secure residue-shaped file");
        }

        let error = metadata
            .names()
            .expect_err("unbounded unauthenticated residue must fail closed");

        assert!(
            matches!(error, StorageError::Journal(message) if message.contains("residue limit"))
        );
        assert_eq!(
            fs::read(root.path().join(".metadata-record.1.0.0.tmp"))
                .expect("residue remains untouched"),
            b"untrusted residue"
        );
    }

    #[test]
    fn public_metadata_create_retries_a_temporary_name_collision_without_deleting_it() {
        let (root, metadata) = metadata_directory();
        let collision = root.path().join(".metadata-record.1.1.1.tmp");
        fs::write(&collision, b"attacker collision sentinel").expect("create collision");
        fs::set_permissions(&collision, fs::Permissions::from_mode(0o600))
            .expect("secure collision");
        install_temporary_name_fixtures([
            ".metadata-record.1.1.1.tmp",
            ".metadata-record.1.1.2.tmp",
        ]);

        metadata
            .create("record", b"trusted")
            .expect("retry colliding temporary name");

        assert!(TEMPORARY_NAME_FIXTURES.with(|slot| slot.borrow().is_empty()));
        assert_eq!(fs::read(root.path().join("record")).unwrap(), b"trusted");
        assert_eq!(
            fs::read(collision).expect("collision remains untouched"),
            b"attacker collision sentinel"
        );
    }

    #[test]
    fn public_capture_retries_a_probe_name_collision_without_deleting_it() {
        let root = tempfile::tempdir().expect("temporary metadata root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("secure metadata root");
        let collision = root.path().join(".autospec-owner-1-1-1");
        fs::write(&collision, b"attacker collision sentinel").expect("create collision");
        fs::set_permissions(&collision, fs::Permissions::from_mode(0o600))
            .expect("secure collision");
        install_probe_name_fixtures([".autospec-owner-1-1-1", ".autospec-owner-1-1-2"]);

        SecureMetadataDirectory::new(root.path()).expect("retry colliding probe name");

        assert!(PROBE_NAME_FIXTURES.with(|slot| slot.borrow().is_empty()));
        assert_eq!(
            fs::read(collision).expect("probe collision remains untouched"),
            b"attacker collision sentinel"
        );
        assert!(!root.path().join(".autospec-owner-1-1-2").exists());
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

    fn replace_operation_temporary_with_attacker(
        directory: PathBuf,
        operation_prefix: &'static str,
        displaced: PathBuf,
        bytes: &'static [u8],
    ) -> impl FnOnce() {
        move || {
            let temporary = fs::read_dir(&directory)
                .expect("list operation directory")
                .map(|entry| entry.expect("read operation entry").path())
                .find(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(operation_prefix))
                })
                .expect("trusted operation temporary exists");
            fs::rename(&temporary, &displaced).expect("displace trusted operation temporary");
            fs::write(&temporary, bytes).expect("install attacker operation temporary");
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
                .expect("secure attacker operation temporary");
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

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
    }

    #[test]
    fn public_metadata_create_refuses_a_swapped_temporary_source() {
        let (root, metadata) = metadata_directory();
        let displaced = root.path().join("trusted-create-away");
        install_race_hook(
            "metadata-create-before-commit",
            replace_operation_temporary_with_attacker(
                root.path().to_path_buf(),
                ".metadata-record.",
                displaced.clone(),
                b"attacker temporary sentinel",
            ),
        );

        let error = metadata
            .create("record", b"trusted new")
            .expect_err("refuse create after its temporary inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert!(!root.path().join("record").exists());
        let attacker = fs::read_dir(root.path())
            .expect("list metadata directory")
            .map(|entry| entry.expect("read entry").path())
            .find(|path| fs::read(path).is_ok_and(|bytes| bytes == b"attacker temporary sentinel"))
            .expect("attacker temporary remains untouched");
        assert_eq!(
            fs::read(attacker).expect("read attacker"),
            b"attacker temporary sentinel"
        );
        assert_eq!(
            fs::read(displaced).expect("trusted source remains"),
            b"trusted new"
        );
    }

    #[test]
    fn public_metadata_replace_refuses_a_swapped_temporary_and_preserves_old_record() {
        let (root, metadata) = metadata_directory();
        metadata
            .create("record", b"trusted old")
            .expect("create record");
        let displaced = root.path().join("trusted-new-away");
        install_race_hook(
            "metadata-replace-before-rename",
            replace_operation_temporary_with_attacker(
                root.path().to_path_buf(),
                ".metadata-record.",
                displaced.clone(),
                b"attacker temporary sentinel",
            ),
        );

        let error = metadata
            .replace("record", b"trusted new")
            .expect_err("refuse replace after its temporary inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(root.path().join("record")).expect("trusted old remains canonical"),
            b"trusted old"
        );
        assert_eq!(
            fs::read(displaced).expect("trusted new remains"),
            b"trusted new"
        );
        let attacker = fs::read_dir(root.path())
            .expect("list metadata directory")
            .map(|entry| entry.expect("read entry").path())
            .find(|path| fs::read(path).is_ok_and(|bytes| bytes == b"attacker temporary sentinel"))
            .expect("attacker temporary remains untouched");
        assert_eq!(
            fs::read(attacker).expect("read attacker"),
            b"attacker temporary sentinel"
        );
    }

    #[test]
    fn public_metadata_replace_authenticates_both_sides_after_exchange() {
        let (root, metadata) = metadata_directory();
        metadata
            .create("record", b"trusted old")
            .expect("create record");
        let selected = root.path().join("record");
        let trusted_new = root.path().join("trusted-new-away");
        let selected_for_hook = selected.clone();
        let trusted_new_for_hook = trusted_new.clone();
        install_race_hook("metadata-replace-before-final-authentication", move || {
            fs::rename(&selected_for_hook, &trusted_new_for_hook)
                .expect("displace committed trusted new record");
            fs::write(&selected_for_hook, b"attacker final sentinel")
                .expect("install attacker final record");
            fs::set_permissions(&selected_for_hook, fs::Permissions::from_mode(0o600))
                .expect("secure attacker final record");
        });

        let error = metadata
            .replace("record", b"trusted new")
            .expect_err("refuse exchanged target swapped before final authentication");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker final sentinel"
        );
        assert_eq!(
            fs::read(trusted_new).expect("trusted new remains"),
            b"trusted new"
        );
        let trusted_old = fs::read_dir(root.path())
            .expect("list metadata directory")
            .map(|entry| entry.expect("read entry").path())
            .find(|path| fs::read(path).is_ok_and(|bytes| bytes == b"trusted old"))
            .expect("trusted old exchange side remains preserved");
        assert_eq!(fs::read(trusted_old).unwrap(), b"trusted old");
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
    fn public_subdirectory_creation_refuses_a_staged_child_swapped_before_open() {
        let (root, metadata) = metadata_directory();
        let staged_name = ".metadata-subdirectory-session.1.1.1.tmp";
        let staged = root.path().join(staged_name);
        let displaced = root.path().join("trusted-staged-away");
        let staged_for_hook = staged.clone();
        let displaced_for_hook = displaced.clone();
        install_temporary_name_fixtures([staged_name]);
        install_race_hook("metadata-subdirectory-after-staged-mkdir", move || {
            let trusted = fs::symlink_metadata(&staged_for_hook)
                .expect("inspect trusted staged directory before displacement");
            fs::rename(&staged_for_hook, &displaced_for_hook)
                .expect("displace trusted staged directory");
            let preserved = fs::symlink_metadata(&displaced_for_hook)
                .expect("inspect preserved trusted staged directory");
            assert_eq!(
                (preserved.dev(), preserved.ino()),
                (trusted.dev(), trusted.ino())
            );
            fs::create_dir(&staged_for_hook).expect("install attacker staged directory");
            fs::set_permissions(&staged_for_hook, fs::Permissions::from_mode(0o700))
                .expect("secure attacker directory");
            fs::write(staged_for_hook.join("sentinel"), b"attacker sentinel")
                .expect("write attacker sentinel");
        });

        let error = metadata
            .create_subdirectory("session")
            .expect_err("reject staged directory swapped before it is opened");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(staged.join("sentinel")).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(displaced.is_dir(), "trusted staged directory remains exact");
        assert!(!root.path().join("session").exists());
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
    fn public_journal_write_refuses_a_swapped_temporary_and_preserves_old_journal() {
        let (_root, store, layout, journal) = journal_fixture();
        store.create(&layout, &journal).expect("create journal");
        let old = fs::read(&layout.journal).expect("read old journal");
        let displaced = layout.journal.with_extension("trusted-new-away");
        install_race_hook(
            "journal-write-before-rename",
            replace_operation_temporary_with_attacker(
                layout.journal.parent().unwrap().to_path_buf(),
                ".dirfd-race.json.",
                displaced.clone(),
                b"attacker temporary sentinel",
            ),
        );

        let error = store
            .write(&layout, &journal)
            .expect_err("refuse journal write after its temporary inode is swapped");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(fs::read(&layout.journal).expect("old journal remains"), old);
        assert!(fs::read(displaced)
            .expect("trusted new remains")
            .starts_with(b"{"));
    }

    #[test]
    fn public_journal_create_rejects_a_final_path_swapped_after_write() {
        let (_root, store, layout, journal) = journal_fixture();
        let selected = layout.journal.clone();
        let displaced = layout.journal.with_extension("trusted-away");
        install_race_hook(
            "journal-create-before-final-authentication",
            replace_child_with_attacker(selected.clone(), displaced.clone(), b"attacker sentinel"),
        );

        let error = store
            .create(&layout, &journal)
            .expect_err("reject final journal inode swapped after creation");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(selected).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(fs::read(displaced)
            .expect("trusted journal remains")
            .starts_with(b"{"));
    }

    #[test]
    fn public_subdirectory_creation_rejects_a_staged_swap_before_commit() {
        let (root, metadata) = metadata_directory();
        let staged_name = ".metadata-subdirectory-session.1.1.1.tmp";
        let staged = root.path().join(staged_name);
        let displaced = root.path().join("trusted-staged-away");
        let staged_for_hook = staged.clone();
        let displaced_for_hook = displaced.clone();
        install_temporary_name_fixtures([staged_name]);
        install_race_hook("metadata-subdirectory-before-commit", move || {
            let trusted = fs::symlink_metadata(&staged_for_hook)
                .expect("inspect authenticated staged directory before displacement");
            fs::rename(&staged_for_hook, &displaced_for_hook)
                .expect("displace authenticated staged directory");
            let preserved = fs::symlink_metadata(&displaced_for_hook)
                .expect("inspect preserved authenticated staged directory");
            assert_eq!(
                (preserved.dev(), preserved.ino()),
                (trusted.dev(), trusted.ino())
            );
            fs::create_dir(&staged_for_hook).expect("install attacker staged directory");
            fs::set_permissions(&staged_for_hook, fs::Permissions::from_mode(0o700))
                .expect("secure attacker directory");
            fs::write(staged_for_hook.join("sentinel"), b"attacker sentinel")
                .expect("write attacker sentinel");
        });

        let error = metadata
            .create_subdirectory("session")
            .expect_err("reject authenticated staged directory swapped before commit");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(staged.join("sentinel")).expect("attacker remains"),
            b"attacker sentinel"
        );
        assert!(displaced.is_dir(), "trusted staged directory remains exact");
        assert!(!root.path().join("session").exists());
    }

    #[test]
    fn public_subdirectory_creation_authenticates_source_and_final_after_commit() {
        let (root, metadata) = metadata_directory();
        let staged_name = ".metadata-subdirectory-session.1.1.1.tmp";
        let final_path = root.path().join("session");
        let trusted_final = root.path().join("trusted-final-away");
        let final_for_hook = final_path.clone();
        let trusted_for_hook = trusted_final.clone();
        install_temporary_name_fixtures([staged_name]);
        install_race_hook(
            "metadata-subdirectory-before-final-authentication",
            move || {
                let trusted = fs::symlink_metadata(&final_for_hook)
                    .expect("inspect committed trusted directory before displacement");
                fs::rename(&final_for_hook, &trusted_for_hook)
                    .expect("displace committed trusted directory");
                let preserved = fs::symlink_metadata(&trusted_for_hook)
                    .expect("inspect preserved committed trusted directory");
                assert_eq!(
                    (preserved.dev(), preserved.ino()),
                    (trusted.dev(), trusted.ino())
                );
                fs::create_dir(&final_for_hook).expect("install attacker final directory");
                fs::set_permissions(&final_for_hook, fs::Permissions::from_mode(0o700))
                    .expect("secure attacker final directory");
                fs::write(final_for_hook.join("sentinel"), b"attacker final sentinel")
                    .expect("write attacker final sentinel");
            },
        );

        let error = metadata
            .create_subdirectory("session")
            .expect_err("reject final directory swapped after no-replace commit");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(final_path.join("sentinel")).expect("attacker final remains"),
            b"attacker final sentinel"
        );
        assert!(
            trusted_final.is_dir(),
            "trusted committed directory remains exact"
        );
        assert!(!root.path().join(staged_name).exists());
    }

    #[test]
    fn public_subdirectory_creation_rejects_a_reappearing_source_after_commit() {
        let (root, metadata) = metadata_directory();
        let staged_name = ".metadata-subdirectory-session.1.1.1.tmp";
        let staged = root.path().join(staged_name);
        let staged_for_hook = staged.clone();
        install_temporary_name_fixtures([staged_name]);
        install_race_hook(
            "metadata-subdirectory-before-final-authentication",
            move || {
                fs::create_dir(&staged_for_hook).expect("install attacker staged source");
                fs::set_permissions(&staged_for_hook, fs::Permissions::from_mode(0o700))
                    .expect("secure attacker staged source");
                fs::write(
                    staged_for_hook.join("sentinel"),
                    b"attacker source sentinel",
                )
                .expect("write attacker source sentinel");
            },
        );

        let error = metadata
            .create_subdirectory("session")
            .expect_err("reject a source name that reappears after commit");

        assert!(matches!(error, StorageError::IdentityMismatch(_)));
        assert_eq!(
            fs::read(staged.join("sentinel")).expect("attacker source remains"),
            b"attacker source sentinel"
        );
        assert!(root.path().join("session").is_dir());
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
                |_| {
                    Err(StorageError::Journal(
                        "injected parent fsync failure".to_owned(),
                    ))
                },
                |directory, name, handle, expected| {
                    Ok(SecureMetadataDirectory {
                        directory: directory.pin_authenticated_child(
                            name,
                            handle,
                            expected,
                            "metadata subdirectory",
                        )?,
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
                PinnedDirectory::sync,
                |_, _, _, _| {
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
    fn subdirectory_creation_retries_an_exclusive_staged_name_collision() {
        let (root, metadata) = metadata_directory();
        let collision_name = ".metadata-subdirectory-session.1.1.1.tmp";
        let collision = root.path().join(collision_name);
        fs::create_dir(&collision).expect("create attacker collision directory");
        fs::write(collision.join("sentinel"), b"attacker collision sentinel")
            .expect("write attacker collision sentinel");
        install_temporary_name_fixtures([
            collision_name,
            ".metadata-subdirectory-session.1.1.2.tmp",
        ]);

        metadata
            .create_subdirectory("session")
            .expect("retry exclusive staged name collision");

        assert!(root.path().join("session").is_dir());
        assert_eq!(
            fs::read(collision.join("sentinel")).expect("attacker collision remains"),
            b"attacker collision sentinel"
        );
    }
}
