use crate::command::{args, run_checked};
use crate::{
    BackendCapability, BackendIdentity, BackendState, CommandRunner, CommandSpec, ExecutionLayout,
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
        let probe_path = probe_path.as_ref().canonicalize().map_err(|error| {
            StorageError::Unavailable(format!(
                "canonicalize APFS probe path {}: {error}",
                probe_path.as_ref().display()
            ))
        })?;
        let metadata = std::fs::symlink_metadata(&probe_path).map_err(|error| {
            StorageError::Unavailable(format!(
                "inspect APFS probe path {}: {error}",
                probe_path.display()
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::Unavailable(format!(
                "APFS probe path is not a real directory: {}",
                probe_path.display()
            )));
        }
        Ok(Self { probe_path, runner })
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
    fn info(&self, target: &str) -> Result<ApfsInfo, StorageError> {
        parse_info(
            &run_checked(
                self.runner.as_ref(),
                CommandSpec::new(DISKUTIL, args(&["info", target])),
                "inspect APFS identity",
            )?
            .stdout,
        )
    }
    fn root_info(&self) -> Result<ApfsInfo, StorageError> {
        self.info(path_text(&self.probe_path)?)
    }
    fn list(&self, container: &str) -> Result<Vec<u8>, StorageError> {
        Ok(run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["apfs", "list", container])),
            "inspect APFS container",
        )?
        .stdout)
    }

    fn verify_object(
        &self,
        identity: &BackendIdentity,
        expected_bytes: u64,
    ) -> Result<ApfsInfo, StorageError> {
        let (container, container_uuid, volume, name, volume_uuid, token) =
            apfs_identity(identity)?;
        let info = self.info(volume)?;
        if info.container != container
            || info.device != volume
            || info.name != name
            || info.uuid != volume_uuid
            || name != format!("autospec-{token}")
            || info.personality != "APFS"
            || info.read_only != "No"
        {
            return Err(StorageError::IdentityMismatch(
                "APFS token, pool, object, or filesystem identity changed".to_owned(),
            ));
        }
        let listing = self.list(container)?;
        if parse_container_uuid(&listing, container)? != container_uuid {
            return Err(StorageError::IdentityMismatch(
                "APFS container UUID changed".to_owned(),
            ));
        }
        let (reserve, quota) = parse_volume_bounds(&listing, volume)?;
        if reserve != expected_bytes || quota != expected_bytes {
            return Err(StorageError::IdentityMismatch(format!(
                "APFS quota/reserve differ from {expected_bytes} bytes"
            )));
        }
        Ok(info)
    }
    fn identity_from_device(
        &self,
        device: &str,
        token: &str,
        bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let info = self.info(device)?;
        let listing = self.list(&info.container)?;
        let container_uuid = parse_container_uuid(&listing, &info.container)?;
        let identity = BackendIdentity::Apfs {
            container: info.container,
            container_uuid,
            volume: info.device,
            volume_name: info.name,
            volume_uuid: info.uuid,
            ownership_token: token.to_owned(),
        };
        self.verify_object(&identity, bytes)?;
        Ok(identity)
    }
}

impl StorageBackend for ApfsBackend {
    fn key(&self, labels: &OwnershipLabels) -> String {
        format!("apfs:{}", labels.execution_id)
    }
    fn probe(&self, required_bytes: u64) -> Result<BackendCapability, StorageError> {
        self.require_root()?;
        let info = self.root_info()?;
        if info.personality != "APFS" || info.read_only != "No" {
            return Err(StorageError::Unavailable(
                "state root is not on a writable identified APFS container".to_owned(),
            ));
        }
        let listing = self.list(&info.container)?;
        let container_uuid = parse_container_uuid(&listing, &info.container)?;
        let free = parse_global_bytes(&listing, "Capacity Not Allocated")?;
        if free < required_bytes {
            return Err(StorageError::Unavailable(format!(
                "APFS container has {free} reservable bytes, need {required_bytes}"
            )));
        }
        Ok(BackendCapability {
            backend: "apfs".to_owned(),
            pool_identity: container_uuid,
            reservable_bytes: free,
        })
    }
    fn discover(
        &self,
        _layout: &ExecutionLayout,
        token: &str,
        bytes: u64,
    ) -> Result<Option<BackendIdentity>, StorageError> {
        let root = self.root_info()?;
        let listing = self.list(&root.container)?;
        let Some(device) = find_volume_device_by_name(
            text(&listing, "diskutil apfs list")?,
            &format!("autospec-{token}"),
        )?
        else {
            return Ok(None);
        };
        self.identity_from_device(&device, token, bytes).map(Some)
    }
    fn create(
        &self,
        _layout: &ExecutionLayout,
        _labels: &OwnershipLabels,
        token: &str,
        bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let root = self.root_info()?;
        let name = format!("autospec-{token}");
        let bound = format!("{bytes}b");
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
                    "-nomount",
                ]),
            ),
            "create unmounted reserved APFS execution volume",
        )?;
        let device = text(&created.stdout, "diskutil addVolume output")?
            .split_whitespace()
            .rev()
            .find(|field| {
                field.starts_with("disk") && field.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
            .ok_or_else(|| {
                StorageError::IdentityMismatch(
                    "diskutil did not report the created APFS device".to_owned(),
                )
            })?;
        self.identity_from_device(device, token, bytes)
    }
    fn prepare(
        &self,
        _layout: &ExecutionLayout,
        identity: &BackendIdentity,
        bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        if !is_not_mounted(&self.verify_object(identity, bytes)?.mount_point) {
            return Err(StorageError::IdentityMismatch(
                "new APFS volume was mounted before prepare".to_owned(),
            ));
        }
        Ok(identity.clone())
    }
    fn mount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        bytes: u64,
    ) -> Result<(), StorageError> {
        let (_, _, volume, _, _, _) = apfs_identity(identity)?;
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                DISKUTIL,
                args(&["mount", "-mountPoint", path_text(&layout.root)?, volume]),
            ),
            "mount exact APFS execution volume",
        )?;
        if self.state(layout, identity, bytes)? != BackendState::Mounted {
            return Err(StorageError::IdentityMismatch(
                "APFS mount proof failed".to_owned(),
            ));
        }
        Ok(())
    }
    fn state(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        bytes: u64,
    ) -> Result<BackendState, StorageError> {
        let (_, _, volume, _, _, _) = apfs_identity(identity)?;
        let output = self
            .runner
            .run(&CommandSpec::new(DISKUTIL, args(&["info", volume])))?;
        if output.code != 0 {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("Could not find") || stderr.contains("not found") {
                return Ok(BackendState::Absent);
            }
            return Err(StorageError::Command(format!(
                "inspect APFS identity exited {}: {}",
                output.code,
                stderr.trim()
            )));
        }
        let info = parse_info(&output.stdout)?;
        let expected = self.verify_object(identity, bytes)?;
        if info != expected {
            return Err(StorageError::IdentityMismatch(
                "APFS identity changed between proof reads".to_owned(),
            ));
        }
        if info.mount_point == path_text(&layout.root)? {
            Ok(BackendState::Mounted)
        } else if is_not_mounted(&info.mount_point) {
            Ok(BackendState::Unmounted)
        } else {
            Err(StorageError::IdentityMismatch(
                "APFS volume is mounted at a foreign path".to_owned(),
            ))
        }
    }
    fn unmount(
        &self,
        _layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        let expected = apfs_identity(identity)?;
        let info = self.info(expected.2)?;
        if info.container != expected.0
            || info.device != expected.2
            || info.name != expected.3
            || info.uuid != expected.4
        {
            return Err(StorageError::IdentityMismatch(
                "refuse to unmount APFS object with changed token or UUID".to_owned(),
            ));
        }
        if parse_container_uuid(&self.list(expected.0)?, expected.0)? != expected.1 {
            return Err(StorageError::IdentityMismatch(
                "refuse to unmount APFS object from changed container UUID".to_owned(),
            ));
        }
        let volume = expected.2;
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["unmount", volume])),
            "unmount exact APFS execution volume",
        )?;
        Ok(())
    }
    fn remove(&self, identity: &BackendIdentity) -> Result<(), StorageError> {
        let expected = apfs_identity(identity)?;
        let info = self.info(expected.2)?;
        if !is_not_mounted(&info.mount_point)
            || info.container != expected.0
            || info.device != expected.2
            || info.name != expected.3
            || info.uuid != expected.4
        {
            return Err(StorageError::IdentityMismatch(
                "refuse to remove APFS object with changed identity".to_owned(),
            ));
        }
        if parse_container_uuid(&self.list(expected.0)?, expected.0)? != expected.1 {
            return Err(StorageError::IdentityMismatch(
                "refuse to remove APFS object from changed container UUID".to_owned(),
            ));
        }
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(DISKUTIL, args(&["apfs", "deleteVolume", expected.2])),
            "delete exact APFS execution volume",
        )?;
        Ok(())
    }
}

fn apfs_identity(
    identity: &BackendIdentity,
) -> Result<(&str, &str, &str, &str, &str, &str), StorageError> {
    match identity {
        BackendIdentity::Apfs {
            container,
            container_uuid,
            volume,
            volume_name,
            volume_uuid,
            ownership_token,
        } => Ok((
            container,
            container_uuid,
            volume,
            volume_name,
            volume_uuid,
            ownership_token,
        )),
        _ => Err(StorageError::IdentityMismatch(
            "APFS backend received a non-APFS identity".to_owned(),
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ApfsInfo {
    device: String,
    container: String,
    personality: String,
    name: String,
    uuid: String,
    mount_point: String,
    read_only: String,
}
fn parse_info(output: &[u8]) -> Result<ApfsInfo, StorageError> {
    Ok(ApfsInfo {
        device: field(output, "Device Identifier")?,
        container: field(output, "APFS Container")?,
        personality: field(output, "File System Personality")?,
        name: field(output, "Volume Name")?,
        uuid: field(output, "Volume UUID")?,
        mount_point: field(output, "Mount Point")?,
        read_only: field(output, "Volume Read-Only")?,
    })
}
fn find_volume_device_by_name(output: &str, name: &str) -> Result<Option<String>, StorageError> {
    let mut device = None;
    for line in output.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("+-> Volume ") {
            device = value.split_whitespace().next().map(ToOwned::to_owned);
        }
        if line
            .strip_prefix("Name:")
            .map(str::trim)
            .and_then(|value| value.split(" (Case-").next())
            == Some(name)
        {
            return device
                .ok_or_else(|| {
                    StorageError::IdentityMismatch(
                        "APFS token matched without device identity".to_owned(),
                    )
                })
                .map(Some);
        }
    }
    Ok(None)
}

fn parse_container_uuid(output: &[u8], container: &str) -> Result<String, StorageError> {
    let prefix = format!("+-- Container {container} ");
    text(output, "diskutil apfs list")?
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.split_whitespace().next())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            StorageError::IdentityMismatch(format!(
                "APFS list lacks UUID for container {container}"
            ))
        })
}

fn is_not_mounted(value: &str) -> bool {
    value.eq_ignore_ascii_case("not mounted")
}
fn parse_volume_bounds(output: &[u8], volume: &str) -> Result<(u64, u64), StorageError> {
    let output = text(output, "diskutil apfs list")?;
    let section = output
        .split(&format!("Volume {volume}"))
        .nth(1)
        .ok_or_else(|| {
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
    output
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix(name)
                .and_then(|value| value.trim_start().strip_prefix(':'))
                .map(str::trim)
        })
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            StorageError::Unavailable(format!("missing or invalid APFS byte field {name}"))
        })
}
fn field(output: &[u8], name: &str) -> Result<String, StorageError> {
    text(output, "diskutil info")?
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix(name)
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
