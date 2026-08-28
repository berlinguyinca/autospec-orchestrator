use crate::{ExecutionLayout, PhaseJournal, StorageError};
use orchestrator_core::ExecutionId;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct JournalStore {
    directory: PathBuf,
}

impl JournalStore {
    pub fn new(state_root: impl AsRef<Path>) -> Result<Self, StorageError> {
        require_real_directory(state_root.as_ref(), "state root")?;
        let directory = state_root.as_ref().join("execution-storage");
        require_real_directory(&directory, "journal directory")?;
        Ok(Self { directory })
    }

    pub fn write(
        &self,
        layout: &ExecutionLayout,
        journal: &PhaseJournal,
    ) -> Result<(), StorageError> {
        self.validate_path(layout)?;
        journal.validate(layout)?;
        reject_symlink_if_present(&layout.journal, "journal")?;
        let temporary = temporary_path(&layout.journal)?;
        reject_any_existing_path(&temporary, "journal temporary file")?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| journal_error("create", &temporary, error))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, journal).map_err(|error| {
            StorageError::Journal(format!("serialize {}: {error}", layout.journal.display()))
        })?;
        writer
            .write_all(b"\n")
            .map_err(|error| journal_error("write", &temporary, error))?;
        writer
            .flush()
            .map_err(|error| journal_error("flush", &temporary, error))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| journal_error("fsync", &temporary, error))?;
        fs::rename(&temporary, &layout.journal)
            .map_err(|error| journal_error("rename", &layout.journal, error))?;
        sync_directory(&self.directory)
    }

    pub fn read(&self, layout: &ExecutionLayout) -> Result<PhaseJournal, StorageError> {
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
        let journal: PhaseJournal =
            serde_json::from_reader(BufReader::new(file)).map_err(|error| {
                StorageError::Journal(format!("parse {}: {error}", layout.journal.display()))
            })?;
        journal.validate(layout)?;
        Ok(journal)
    }

    pub fn remove(&self, layout: &ExecutionLayout) -> Result<(), StorageError> {
        self.validate_path(layout)?;
        let metadata = fs::symlink_metadata(&layout.journal)
            .map_err(|error| journal_error("inspect", &layout.journal, error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StorageError::Journal(format!(
                "refuse to remove non-file journal {}",
                layout.journal.display()
            )));
        }
        fs::remove_file(&layout.journal)
            .map_err(|error| journal_error("remove", &layout.journal, error))?;
        sync_directory(&self.directory)
    }

    pub fn list(&self, state_root: &Path) -> Result<Vec<PhaseJournal>, StorageError> {
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

fn reject_symlink_if_present(path: &Path, purpose: &str) -> Result<(), StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(StorageError::Journal(format!(
            "{purpose} is a symlink: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(journal_error("inspect", path, error)),
    }
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
