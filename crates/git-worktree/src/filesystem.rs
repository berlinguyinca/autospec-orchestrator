use crate::command::git_command;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeFilesystemPoint {
    CloneObjectPack,
    CheckoutIndex,
    OwnerTemporaryWritten,
    OwnerTemporarySynced,
    OwnerRenamed,
}

/// Filesystem boundary for repository materialization and exact cleanup.
///
/// The boundary permits deterministic ENOSPC injection without weakening the
/// production rule that Git commands are always passed as argument vectors.
pub trait WorktreeFilesystem: Debug + Send + Sync {
    fn clone_repository(&self, mirror: &Path, destination: &Path) -> io::Result<()>;
    fn write_owner_record(&self, directory: &Path, bytes: &[u8]) -> io::Result<()> {
        write_owner_record(self, directory, bytes)
    }
    fn remove_repository(&self, path: &Path) -> io::Result<()>;
    fn create_repository_root(&self, path: &Path) -> io::Result<()>;
    fn checkpoint(&self, _point: WorktreeFilesystemPoint, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SystemWorktreeFilesystem;

impl WorktreeFilesystem for SystemWorktreeFilesystem {
    fn clone_repository(&self, mirror: &Path, destination: &Path) -> io::Result<()> {
        let output = git_command()
            .args(["clone", "--no-local", "--no-hardlinks", "--no-checkout"])
            .arg(mirror)
            .arg(destination)
            .output()?;
        if output.status.success() {
            Ok(())
        } else if output.status.code() == Some(28)
            || String::from_utf8_lossy(&output.stderr).contains("No space left on device")
        {
            Err(io::Error::from_raw_os_error(28))
        } else {
            Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ))
        }
    }

    fn remove_repository(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn create_repository_root(&self, path: &Path) -> io::Result<()> {
        fs::create_dir(path)
    }
}

fn write_owner_record<F: WorktreeFilesystem + ?Sized>(
    filesystem: &F,
    directory: &Path,
    bytes: &[u8],
) -> io::Result<()> {
    let final_path = directory.join(".autospec-owner.json");
    let temporary_path = directory.join(".autospec-owner.json.tmp");
    reject_existing(&final_path)?;
    reject_existing(&temporary_path)?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)?;
        file.write_all(bytes)?;
        filesystem.checkpoint(
            WorktreeFilesystemPoint::OwnerTemporaryWritten,
            &temporary_path,
        )?;
        file.sync_all()?;
        filesystem.checkpoint(
            WorktreeFilesystemPoint::OwnerTemporarySynced,
            &temporary_path,
        )?;
        fs::rename(&temporary_path, &final_path)?;
        filesystem.checkpoint(WorktreeFilesystemPoint::OwnerRenamed, &final_path)?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn reject_existing(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("ownership metadata path already exists: {}", path.display()),
        )),
        Err(error) => Err(error),
    }
}
