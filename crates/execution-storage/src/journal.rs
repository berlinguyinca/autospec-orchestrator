use crate::{ExecutionLayout, PhaseJournal, StorageError};
use orchestrator_core::ExecutionId;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static OWNER_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[derive(Debug, Clone)]
pub struct JournalStore {
    directory: PathBuf,
    state_root: DirectoryIdentity,
    journal_directory: DirectoryIdentity,
}

impl JournalStore {
    pub fn new(state_root: impl AsRef<Path>) -> Result<Self, StorageError> {
        let state_root = DirectoryIdentity::capture(state_root.as_ref(), "state root")?;
        let directory = state_root.path.join("execution-storage");
        let journal_directory = DirectoryIdentity::capture(&directory, "journal directory")?;
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
        reject_any_existing_path(&layout.journal, "journal")?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .secure_file_mode()
            .open(&layout.journal)
            .map_err(|error| journal_error("create", &layout.journal, error))?;
        write_and_sync(file, journal, &layout.journal)?;
        sync_directory(&self.directory)?;
        self.verify_directories()
    }

    pub fn write(
        &self,
        layout: &ExecutionLayout,
        journal: &PhaseJournal,
    ) -> Result<(), StorageError> {
        self.verify_directories()?;
        self.validate_path(layout)?;
        journal.validate(layout)?;
        let metadata = fs::symlink_metadata(&layout.journal)
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StorageError::Journal(format!(
                "journal is not a real file: {}",
                layout.journal.display()
            )));
        }
        let temporary = temporary_path(&layout.journal)?;
        reject_any_existing_path(&temporary, "journal temporary file")?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .secure_file_mode()
            .open(&temporary)
            .map_err(|error| journal_error("create", &temporary, error))?;
        write_and_sync(file, journal, &temporary)?;
        fs::rename(&temporary, &layout.journal)
            .map_err(|error| journal_error("rename", &layout.journal, error))?;
        sync_directory(&self.directory)?;
        self.verify_directories()
    }

    pub fn read(&self, layout: &ExecutionLayout) -> Result<PhaseJournal, StorageError> {
        self.verify_directories()?;
        self.validate_path(layout)?;
        let metadata = fs::symlink_metadata(&layout.journal)
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StorageError::Journal(format!(
                "journal is not a real file: {}",
                layout.journal.display()
            )));
        }
        let file = File::open(&layout.journal)
            .map_err(|error| journal_error("open", &layout.journal, error))?;
        verify_opened_file(&metadata, &file, &layout.journal)?;
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
        let metadata = fs::symlink_metadata(&layout.journal)
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StorageError::Journal(format!(
                "refuse to remove non-file journal {}",
                layout.journal.display()
            )));
        }
        let file = File::open(&layout.journal)
            .map_err(|error| journal_error("open", &layout.journal, error))?;
        verify_opened_file(&metadata, &file, &layout.journal)?;
        drop(file);
        fs::remove_file(&layout.journal)
            .map_err(|error| journal_error("remove", &layout.journal, error))?;
        sync_directory(&self.directory)?;
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
pub(crate) struct DirectoryIdentity {
    pub(crate) path: PathBuf,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    uid: u32,
}

impl DirectoryIdentity {
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
            Ok(Self {
                path,
                device: metadata.dev(),
                inode: metadata.ino(),
                uid: metadata.uid(),
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

trait SecureOpenOptions {
    fn secure_file_mode(&mut self) -> &mut Self;
}
impl SecureOpenOptions for OpenOptions {
    fn secure_file_mode(&mut self) -> &mut Self {
        #[cfg(unix)]
        {
            self.mode(0o600)
        }
        #[cfg(not(unix))]
        {
            self
        }
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

fn reject_any_existing_path(path: &Path, purpose: &str) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(StorageError::Journal(format!(
            "{purpose} already exists: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(journal_error("inspect", path, error)),
    }
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

fn temporary_path(journal: &Path) -> Result<PathBuf, StorageError> {
    let name = journal
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            StorageError::Journal(format!("journal name is not UTF-8: {}", journal.display()))
        })?;
    Ok(journal.with_file_name(format!(".{name}.tmp")))
}

fn sync_directory(directory: &Path) -> Result<(), StorageError> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| journal_error("fsync directory", directory, error))
}

fn journal_error(action: &str, path: &Path, error: std::io::Error) -> StorageError {
    StorageError::Journal(format!("{action} {}: {error}", path.display()))
}
