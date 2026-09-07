use crate::WorktreeError;
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub(crate) struct FileLock {
    file: File,
}

impl FileLock {
    pub(crate) fn acquire(path: &Path) -> Result<Self, WorktreeError> {
        Self::acquire_with(path, WorktreeError::Mirror)
    }

    pub(crate) fn acquire_with(
        path: &Path,
        error: fn(String) -> WorktreeError,
    ) -> Result<Self, WorktreeError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|cause| error(cause.to_string()))?;
        file.lock_exclusive()
            .map_err(|cause| error(cause.to_string()))?;
        Ok(Self { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}
