use crate::command::{args, run_checked};
use crate::{
    BackendCapability, BackendIdentity, CommandRunner, CommandSpec, ExecutionLayout,
    StorageBackend, StorageError,
};
use orchestrator_core::OwnershipLabels;
use std::{path::Path, sync::Arc};

const DISKUTIL: &str = "/usr/sbin/diskutil";
const ID: &str = "/usr/bin/id";

#[derive(Debug)]
pub struct ApfsBackend {
    probe_path: std::path::PathBuf,
    runner: Arc<dyn CommandRunner>,
}

impl ApfsBackend {
    pub fn new(
        probe_path: impl AsRef<Path>,
        runner: Arc<dyn CommandRunner>,
    ) -> Result<Self, StorageError> {
        let metadata = std::fs::symlink_metadata(probe_path.as_ref()).map_err(|error| {
            StorageError::Unavailable(format!(
                "inspect APFS probe path {}: {error}",
                probe_path.as_ref().display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::Unavailable(format!(
                "APFS probe path is not a real directory: {}",
                probe_path.as_ref().display()
            )));
        }
        Ok(Self {
            probe_path: probe_path.as_ref().to_path_buf(),
            runner,
        })
    }

    fn require_root(&self) -> Result<(), StorageError> {
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(ID, args(&["-u"])),
            "inspect APFS allocation privilege",
        )?;
        if text(&output.stdout, "id output")? == "0" {
            Ok(())
        } else {
            Err(StorageError::Unavailable(
                "APFS quota mountpoint allocation requires root".to_owned(),
            ))
        }
    }

    fn root_info(&self) -> Result<ApfsInfo, StorageError> {
        let path = path_text(&self.probe_path)?;
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["info", path])),
            "inspect APFS pool",
        )?;
        parse_info(&output.stdout)
    }

    fn inspect_allocation(
        &self,
        layout: &ExecutionLayout,
        expected: Option<&BackendIdentity>,
        expected_bytes: Option<u64>,
    ) -> Result<BackendIdentity, StorageError> {
        let mount = path_text(&layout.root)?;
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["info", mount])),
            "inspect APFS execution volume",
        )?;
        let info = parse_info(&output.stdout)?;
        if info.personality != "APFS" || info.read_only != "No" || info.mount_point != mount {
            return Err(StorageError::IdentityMismatch(
                "APFS mount proof is missing or read-only".to_owned(),
            ));
        }
        let identity = BackendIdentity::Apfs {
            container: info.container.clone(),
            volume: info.device.clone(),
            volume_uuid: info.uuid.clone(),
        };
        if expected.is_some_and(|expected| expected != &identity) {
            return Err(StorageError::IdentityMismatch(
                "APFS volume identity changed".to_owned(),
            ));
        }
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["apfs", "list", &info.container])),
            "inspect APFS quota and reserve",
        )?;
        let (reserve, quota) = parse_volume_bounds(&output.stdout, &info.device)?;
        if reserve == 0 || quota == 0 || reserve != quota {
            return Err(StorageError::IdentityMismatch(
                "APFS quota and reserve are absent or unequal".to_owned(),
            ));
        }
        if expected_bytes.is_some_and(|bytes| reserve != bytes) {
            return Err(StorageError::IdentityMismatch(format!(
                "APFS reservation is {reserve} bytes, expected {}",
                expected_bytes.unwrap_or_default()
            )));
        }
        Ok(identity)
    }
}

impl StorageBackend for ApfsBackend {
    fn key(&self, labels: &OwnershipLabels) -> String {
        format!("apfs:autospec-{}", labels.execution_id)
    }

    fn probe(&self, required_bytes: u64) -> Result<BackendCapability, StorageError> {
        self.require_root()?;
        let info = self.root_info()?;
        if info.personality != "APFS" || info.read_only != "No" {
            return Err(StorageError::Unavailable(
                "state root is not on writable APFS".to_owned(),
            ));
        }
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["apfs", "list", &info.container])),
            "inspect APFS free capacity",
        )?;
        let free = parse_global_bytes(&output.stdout, "Capacity Not Allocated")?;
        if free < required_bytes {
            return Err(StorageError::Unavailable(format!(
                "APFS container has {free} reservable bytes, need {required_bytes}"
            )));
        }
        Ok(BackendCapability {
            backend: "apfs".to_owned(),
            pool_identity: info.container,
            reservable_bytes: free,
        })
    }

    fn allocate(
        &self,
        layout: &ExecutionLayout,
        labels: &OwnershipLabels,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let root = self.root_info()?;
        let mount = path_text(&layout.root)?;
        let name = format!("autospec-{}", labels.execution_id);
        let bound = format!("{reserved_bytes}b");
        let created = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                DISKUTIL,
                args(&[
                    "apfs",
                    "addVolume",
                    &root.container,
                    "APFS",
                    &name,
                    "-quota",
                    &bound,
                    "-reserve",
                    &bound,
                    "-mountpoint",
                    mount,
                ]),
            ),
            "create reserved APFS execution volume",
        )?;
        let created_volume = text(&created.stdout, "diskutil addVolume output")?
            .split_whitespace()
            .rev()
            .find(|field| {
                field.starts_with("disk") && field.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
            .ok_or_else(|| {
                StorageError::IdentityMismatch(
                    "diskutil did not report the created APFS volume identity".to_owned(),
                )
            })?;
        match self.inspect_allocation(layout, None, Some(reserved_bytes)) {
            Ok(identity) => Ok(identity),
            Err(cause) => {
                let cleanup = run_checked(
                    self.runner.as_ref(),
                    CommandSpec::new(DISKUTIL, args(&["apfs", "deleteVolume", created_volume])),
                    "rollback exact APFS execution volume",
                );
                match cleanup {
                    Ok(_) => Err(cause),
                    Err(cleanup) => Err(StorageError::Cleanup(format!(
                        "APFS allocation failed: {cause}; rollback failed: {cleanup}"
                    ))),
                }
            }
        }
    }

    fn verify(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
    ) -> Result<(), StorageError> {
        self.inspect_allocation(layout, Some(identity), Some(reserved_bytes))?;
        Ok(())
    }

    fn release(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        self.inspect_allocation(layout, Some(identity), None)?;
        let volume = match identity {
            BackendIdentity::Apfs { volume, .. } => volume,
            _ => {
                return Err(StorageError::IdentityMismatch(
                    "APFS backend received a non-APFS identity".to_owned(),
                ))
            }
        };
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["apfs", "deleteVolume", volume])),
            "delete exact APFS execution volume",
        )?;
        Ok(())
    }
}

#[derive(Debug)]
struct ApfsInfo {
    device: String,
    container: String,
    personality: String,
    uuid: String,
    mount_point: String,
    read_only: String,
}

fn parse_info(output: &[u8]) -> Result<ApfsInfo, StorageError> {
    Ok(ApfsInfo {
        device: field(output, "Device Identifier")?,
        container: field(output, "APFS Container")?,
        personality: field(output, "File System Personality")?,
        uuid: field(output, "Volume UUID")?,
        mount_point: field(output, "Mount Point")?,
        read_only: field(output, "Volume Read-Only")?,
    })
}

fn parse_volume_bounds(output: &[u8], volume: &str) -> Result<(u64, u64), StorageError> {
    let output = text(output, "diskutil apfs list")?;
    let marker = format!("Volume {volume}");
    let section = output.split(&marker).nth(1).ok_or_else(|| {
        StorageError::IdentityMismatch(format!("APFS list does not contain volume {volume}"))
    })?;
    let section = section.split("+-> Volume ").next().unwrap_or(section);
    Ok((
        parse_bytes_field(section, "Capacity Reserve")?,
        parse_bytes_field(section, "Capacity Quota")?,
    ))
}

fn parse_global_bytes(output: &[u8], name: &str) -> Result<u64, StorageError> {
    parse_bytes_field(text(output, "diskutil apfs list")?, name)
}

fn parse_bytes_field(output: &str, name: &str) -> Result<u64, StorageError> {
    let value = output
        .lines()
        .find_map(|line| {
            let line = line.trim();
            line.strip_prefix(name)
                .and_then(|value| value.trim_start().strip_prefix(':'))
                .map(str::trim)
        })
        .ok_or_else(|| StorageError::Unavailable(format!("missing APFS field {name}")))?;
    value
        .split_whitespace()
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| StorageError::Unavailable(format!("invalid APFS byte field {name}")))
}

fn field(output: &[u8], name: &str) -> Result<String, StorageError> {
    let output = text(output, "diskutil info")?;
    output
        .lines()
        .find_map(|line| {
            let line = line.trim();
            line.strip_prefix(name)
                .and_then(|value| value.trim_start().strip_prefix(':'))
                .map(str::trim)
        })
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| StorageError::Unavailable(format!("missing diskutil field {name}")))
}

fn text<'a>(output: &'a [u8], purpose: &str) -> Result<&'a str, StorageError> {
    std::str::from_utf8(output)
        .map(str::trim)
        .map_err(|error| StorageError::Unavailable(format!("{purpose} is not UTF-8: {error}")))
}

fn path_text(path: &Path) -> Result<&str, StorageError> {
    path.to_str().ok_or_else(|| {
        StorageError::InvalidRequest(format!("path is not UTF-8: {}", path.display()))
    })
}
