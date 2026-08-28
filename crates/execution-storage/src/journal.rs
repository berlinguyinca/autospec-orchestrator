use crate::{ExecutionLayout, PhaseJournal, StorageError};
use orchestrator_core::ExecutionId;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

static OWNER_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

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

    pub fn create(&self, name: &str, bytes: &[u8]) -> Result<(), StorageError> {
        self.reconcile(name)?;
        if self.directory.child_metadata(name)?.is_some() {
            return Err(StorageError::Journal(format!(
                "metadata record already exists: {name}"
            )));
        }
        let temporary = metadata_temporary_name(name)?;
        let file = self.directory.create_file(&temporary)?;
        write_bytes_and_sync(file, bytes, &self.directory.child_path(&temporary)?)?;
        self.directory.rename(&temporary, name)?;
        self.directory.sync()
    }

    pub fn replace(&self, name: &str, bytes: &[u8]) -> Result<(), StorageError> {
        self.reconcile(name)?;
        let current = self.directory.open_file(name)?;
        let expected = current
            .metadata()
            .map_err(|error| journal_error("inspect", &self.directory.path, error))?;
        let temporary = metadata_temporary_name(name)?;
        let file = self.directory.create_file(&temporary)?;
        write_bytes_and_sync(file, bytes, &self.directory.child_path(&temporary)?)?;
        self.directory.verify_child(name, &expected)?;
        self.directory.rename(&temporary, name)?;
        self.directory.sync()
    }

    pub fn read(&self, name: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.reconcile(name)?;
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
        self.reconcile(name)?;
        let Some(_) = self.directory.child_metadata(name)? else {
            return Ok(());
        };
        let file = self.directory.open_file(name)?;
        let expected = file
            .metadata()
            .map_err(|error| journal_error("inspect", &self.directory.path, error))?;
        let tombstone = metadata_removal_name(name)?;
        self.directory.rename(name, &tombstone)?;
        self.directory.verify_child(&tombstone, &expected)?;
        self.directory.remove_file(&tombstone)?;
        self.directory.sync()
    }

    fn reconcile(&self, name: &str) -> Result<(), StorageError> {
        self.directory.verify("metadata directory")?;
        let temporary = metadata_temporary_name(name)?;
        let tombstone = metadata_removal_name(name)?;
        if self.directory.child_metadata(&tombstone)?.is_some() {
            self.directory.open_file(&tombstone)?;
            self.directory.remove_file(&tombstone)?;
            self.directory.sync()?;
        }
        if self.directory.child_metadata(&temporary)?.is_some() {
            self.directory.open_file(&temporary)?;
            if self.directory.child_metadata(name)?.is_some() {
                self.directory.open_file(name)?;
                self.directory.remove_file(&temporary)?;
            } else {
                self.directory.rename(&temporary, name)?;
            }
            self.directory.sync()?;
        }
        self.directory.verify("metadata directory")
    }
}

impl JournalStore {
    pub fn new(state_root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let state_root = PinnedDirectory::capture(state_root.as_ref(), "state root")?;
        let directory = state_root.path.join("execution-storage");
        let journal_directory = PinnedDirectory::capture(&directory, "journal directory")?;
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
        write_and_sync(
            file,
            journal,
            &self.journal_directory.child_path(&temporary)?,
        )?;
        self.journal_directory.verify_child(&name, &expected)?;
        self.journal_directory.rename(&temporary, &name)?;
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

    pub fn remove(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.verify_directories()?;
        self.validate_path(layout)?;
        let name = journal_name(layout)?;
        let file = self.journal_directory.open_file(&name)?;
        let expected = file
            .metadata()
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        let tombstone = temporary_name(&format!("remove-{name}"))?;
        self.journal_directory.rename(&name, &tombstone)?;
        self.journal_directory.verify_child(&tombstone, &expected)?;
        self.journal_directory.remove_file(&tombstone)?;
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
        let entries = fs::read_dir(&self.directory)
            .map_err(|error| journal_error("read directory", &self.directory, error))?;
        for entry in entries {
            let entry =
                entry.map_err(|error| journal_error("read entry", &self.directory, error))?;
            let file_type = entry
                .file_type()
                .map_err(|error| journal_error("inspect entry", &entry.path(), error))?;
            if file_type.is_symlink() || !file_type.is_file() {
                return Err(StorageError::Journal(format!(
                    "unexpected journal entry type: {}",
                    entry.path().display()
                )));
            }
            let name = entry.file_name().into_string().map_err(|name| {
                StorageError::Journal(format!(
                    "journal filename is not UTF-8: {}",
                    Path::new(&name).display()
                ))
            })?;
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

#[derive(Debug, Clone)]
/// Internal boundary for descriptor-pinned, no-follow directory operations.
///
/// Linux resolves children through `/proc/self/fd`; macOS currently retains the
/// same inode/mode checks but needs a safe `openat`/`renameat`/`unlinkat` wrapper
/// before it can avoid path re-resolution entirely.
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

impl PinnedDirectory {
    pub(crate) fn capture(path: &Path, purpose: &str) -> Result<Self, StorageError> {
        let supplied =
            fs::symlink_metadata(path).map_err(|error| journal_error("inspect", path, error))?;
        if supplied.file_type().is_symlink() || !supplied.is_dir() {
            return Err(StorageError::Journal(format!(
                "{purpose} is not a real directory: {}",
                path.display()
            )));
        }
        let path = path.canonicalize().map_err(|error| {
            StorageError::Journal(format!(
                "canonicalize {purpose} {}: {error}",
                path.display()
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
            verify_current_owner(&path, metadata.uid(), purpose)?;
            let handle = File::open(&path).map_err(|error| journal_error("open", &path, error))?;
            let opened = handle
                .metadata()
                .map_err(|error| journal_error("inspect", &path, error))?;
            if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
                return Err(StorageError::IdentityMismatch(format!(
                    "{purpose} changed while opening"
                )));
            }
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
        let opened = File::open(&self.path)
            .and_then(|file| file.metadata())
            .map_err(|error| journal_error("open", &self.path, error))?;
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
    pub(crate) fn child_path(&self, name: &str) -> Result<PathBuf, StorageError> {
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            return Err(StorageError::InvalidRequest(
                "descriptor-relative name is unsafe".to_owned(),
            ));
        }
        #[cfg(target_os = "linux")]
        let parent = Path::new("/proc/self/fd").join(self.handle.as_raw_fd().to_string());
        #[cfg(not(target_os = "linux"))]
        let parent = self.path.clone();
        Ok(parent.join(name))
    }

    #[cfg(unix)]
    fn create_file(&self, name: &str) -> Result<File, StorageError> {
        let path = self.child_path(name)?;
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| journal_error("create", &path, error))
    }

    #[cfg(unix)]
    fn open_file(&self, name: &str) -> Result<File, StorageError> {
        let path = self.child_path(name)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| journal_error("open", &path, error))?;
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
    fn verify_child(&self, name: &str, expected: &fs::Metadata) -> Result<(), StorageError> {
        let file = self.open_file(name)?;
        verify_opened_file(expected, &file, &self.child_path(name)?)
    }

    #[cfg(unix)]
    fn rename(&self, from: &str, to: &str) -> Result<(), StorageError> {
        let from_path = self.child_path(from)?;
        let to_path = self.child_path(to)?;
        fs::rename(&from_path, &to_path).map_err(|error| journal_error("rename", &to_path, error))
    }

    #[cfg(unix)]
    fn remove_file(&self, name: &str) -> Result<(), StorageError> {
        let path = self.child_path(name)?;
        fs::remove_file(&path).map_err(|error| journal_error("remove", &path, error))
    }

    #[cfg(unix)]
    fn sync(&self) -> Result<(), StorageError> {
        self.handle
            .sync_all()
            .map_err(|error| journal_error("fsync directory", &self.path, error))
    }

    #[cfg(unix)]
    pub(crate) fn create_directory(&self, name: &str) -> Result<(), StorageError> {
        use std::os::unix::fs::DirBuilderExt;
        let path = self.child_path(name)?;
        let mut builder = fs::DirBuilder::new();
        builder
            .mode(0o700)
            .create(&path)
            .map_err(|error| journal_error("create directory", &path, error))?;
        self.sync()
    }

    #[cfg(unix)]
    pub(crate) fn remove_directory(&self, name: &str) -> Result<(), StorageError> {
        let path = self.child_path(name)?;
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| journal_error("inspect", &path, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::IdentityMismatch(format!(
                "descriptor-relative mountpoint is not a real directory: {}",
                path.display()
            )));
        }
        fs::remove_dir(&path).map_err(|error| journal_error("remove directory", &path, error))?;
        self.sync()
    }

    #[cfg(unix)]
    pub(crate) fn child_metadata(&self, name: &str) -> Result<Option<fs::Metadata>, StorageError> {
        let path = self.child_path(name)?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(journal_error("inspect", &path, error)),
        }
    }
}

#[cfg(unix)]
fn verify_current_owner(
    directory: &Path,
    expected_uid: u32,
    purpose: &str,
) -> Result<(), StorageError> {
    let probe = directory.join(format!(
        ".autospec-owner-{}-{}",
        std::process::id(),
        OWNER_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&probe)
        .map_err(|error| journal_error("create ownership probe", &probe, error))?;
    let actual_uid = file
        .metadata()
        .map_err(|error| journal_error("inspect ownership probe", &probe, error))?
        .uid();
    drop(file);
    fs::remove_file(&probe)
        .map_err(|error| journal_error("remove ownership probe", &probe, error))?;
    if actual_uid == expected_uid {
        Ok(())
    } else {
        Err(StorageError::Unavailable(format!(
            "{purpose} is owned by uid {expected_uid}, current uid is {actual_uid}"
        )))
    }
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

fn metadata_temporary_name(name: &str) -> Result<String, StorageError> {
    validate_metadata_name(name)?;
    Ok(format!("{name}.tmp"))
}

fn metadata_removal_name(name: &str) -> Result<String, StorageError> {
    validate_metadata_name(name)?;
    Ok(format!("remove-{name}.tmp"))
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

fn journal_error(action: &str, path: &Path, error: std::io::Error) -> StorageError {
    StorageError::Journal(format!("{action} {}: {error}", path.display()))
}
