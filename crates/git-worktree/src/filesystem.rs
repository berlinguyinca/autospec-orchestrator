use crate::manager::atomic_write_new;
use crate::WorktreeError;
use std::fmt::Debug;
use std::fs;
use std::io;
use std::path::Path;
use std::process::Command;

/// Filesystem boundary for repository materialization and exact cleanup.
///
/// The boundary permits deterministic ENOSPC injection without weakening the
/// production rule that Git commands are always passed as argument vectors.
pub trait WorktreeFilesystem: Debug + Send + Sync {
    fn clone_repository(&self, mirror: &Path, destination: &Path) -> io::Result<()>;
    fn write_owner_record(&self, directory: &Path, bytes: &[u8]) -> io::Result<()>;
    fn remove_repository(&self, path: &Path) -> io::Result<()>;
    fn create_repository_root(&self, path: &Path) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub struct SystemWorktreeFilesystem;

impl WorktreeFilesystem for SystemWorktreeFilesystem {
    fn clone_repository(&self, mirror: &Path, destination: &Path) -> io::Result<()> {
        let output = Command::new("git")
            .args(["clone", "--no-local", "--no-hardlinks", "--no-checkout"])
            .arg(mirror)
            .arg(destination)
            .output()?;
        if output.status.success() {
            Ok(())
        } else {
            Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ))
        }
    }

    fn write_owner_record(&self, directory: &Path, bytes: &[u8]) -> io::Result<()> {
        let final_path = directory.join(".autospec-owner.json");
        let temporary_path = directory.join(".autospec-owner.json.tmp");
        atomic_write_new(
            directory,
            &final_path,
            &temporary_path,
            bytes,
            WorktreeError::Create,
        )
        .map_err(|error| io::Error::other(error.to_string()))
    }

    fn remove_repository(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn create_repository_root(&self, path: &Path) -> io::Result<()> {
        fs::create_dir(path)
    }
}
