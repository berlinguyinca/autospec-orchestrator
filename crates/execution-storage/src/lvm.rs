use crate::command::{args, run_checked};
use crate::{
    BackendCapability, BackendIdentity, CommandRunner, CommandSpec, ExecutionLayout,
    StorageBackend, StorageError,
};
use orchestrator_core::OwnershipLabels;
use std::sync::{Arc, Mutex};

const ID: &str = "/usr/bin/id";
const LVM: &str = "/usr/sbin/lvm";
const MKFS: &str = "/usr/sbin/mkfs.ext4";
const MOUNT: &str = "/usr/bin/mount";
const UMOUNT: &str = "/usr/bin/umount";
const FINDMNT: &str = "/usr/bin/findmnt";
const BLKID: &str = "/usr/sbin/blkid";

#[derive(Debug, Clone)]
struct LvmPool {
    uuid: String,
    free_bytes: u64,
    extent_bytes: u64,
}

#[derive(Debug)]
pub struct LvmBackend {
    volume_group: String,
    runner: Arc<dyn CommandRunner>,
    pool: Mutex<Option<LvmPool>>,
}

impl LvmBackend {
    pub fn new(
        volume_group: impl Into<String>,
        runner: Arc<dyn CommandRunner>,
    ) -> Result<Self, StorageError> {
        let volume_group = volume_group.into();
        if volume_group.is_empty()
            || !volume_group
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"+_.-".contains(&byte))
        {
            return Err(StorageError::InvalidRequest(format!(
                "invalid LVM volume group: {volume_group}"
            )));
        }
        Ok(Self {
            volume_group,
            runner,
            pool: Mutex::new(None),
        })
    }

    fn require_root_and_tools(&self) -> Result<(), StorageError> {
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(ID, args(&["-u"])),
            "inspect LVM allocation privilege",
        )?;
        if output.stdout != b"0\n" {
            return Err(StorageError::Unavailable(
                "thick-LVM allocation requires root".to_owned(),
            ));
        }
        for (program, version_argument) in [
            (LVM, "version"),
            (MKFS, "-V"),
            (MOUNT, "--version"),
            (UMOUNT, "--version"),
            (FINDMNT, "--version"),
            (BLKID, "--version"),
        ] {
            run_checked(
                self.runner.as_ref(),
                CommandSpec::new(program, args(&[version_argument])),
                "probe required storage tool",
            )?;
        }
        Ok(())
    }

    fn inspect_pool(&self) -> Result<LvmPool, StorageError> {
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                LVM,
                args(&[
                    "vgs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,vg_free,vg_extent_size",
                    &self.volume_group,
                ]),
            ),
            "inspect thick-LVM pool",
        )?;
        let row = utf8_trim(&output.stdout, "vgs output")?;
        let fields = row.split(':').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 3 || fields[0].is_empty() {
            return Err(StorageError::Unavailable(
                "vgs did not return exact pool identity".to_owned(),
            ));
        }
        Ok(LvmPool {
            uuid: fields[0].to_owned(),
            free_bytes: parse_u64(fields[1], "VG free bytes")?,
            extent_bytes: parse_u64(fields[2], "VG extent bytes")?,
        })
    }

    fn pool(&self) -> Result<LvmPool, StorageError> {
        self.pool
            .lock()
            .map_err(|_| StorageError::Unavailable("LVM pool cache is poisoned".to_owned()))?
            .clone()
            .ok_or_else(|| StorageError::Unavailable("LVM backend was not probed".to_owned()))
    }

    fn lv_name(labels: &OwnershipLabels) -> String {
        format!("autospec-{}", labels.execution_id)
    }

    fn inspect_lv(&self, vg_lv: &str) -> Result<LvInfo, StorageError> {
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                LVM,
                args(&[
                    "lvs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,lv_uuid,lv_size,lv_path",
                    vg_lv,
                ]),
            ),
            "inspect thick logical volume",
        )?;
        let fields = utf8_trim(&output.stdout, "lvs output")?
            .split(':')
            .map(str::trim)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if fields.len() != 4 || fields.iter().any(String::is_empty) {
            return Err(StorageError::IdentityMismatch(
                "lvs did not return exact logical-volume identity".to_owned(),
            ));
        }
        Ok(LvInfo {
            vg_uuid: fields[0].clone(),
            lv_uuid: fields[1].clone(),
            size_bytes: parse_u64(&fields[2], "LV size bytes")?,
            path: fields[3].clone(),
        })
    }

    fn filesystem_uuid(&self, device: &str) -> Result<String, StorageError> {
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                BLKID,
                args(&["--output", "value", "--match-tag", "UUID", device]),
            ),
            "inspect filesystem UUID",
        )?;
        let uuid = utf8_trim(&output.stdout, "blkid output")?;
        if uuid.is_empty() {
            Err(StorageError::IdentityMismatch(
                "filesystem UUID is empty".to_owned(),
            ))
        } else {
            Ok(uuid.to_owned())
        }
    }

    fn verify_internal(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        expected_bytes: Option<u64>,
    ) -> Result<(), StorageError> {
        let (volume_group, volume_group_uuid, logical_volume, logical_volume_uuid, filesystem_uuid) =
            match identity {
                BackendIdentity::Lvm {
                    volume_group,
                    volume_group_uuid,
                    logical_volume,
                    logical_volume_uuid,
                    filesystem_uuid,
                } => (
                    volume_group,
                    volume_group_uuid,
                    logical_volume,
                    logical_volume_uuid,
                    filesystem_uuid,
                ),
                _ => {
                    return Err(StorageError::IdentityMismatch(
                        "LVM backend received a non-LVM identity".to_owned(),
                    ))
                }
            };
        if volume_group != &self.volume_group {
            return Err(StorageError::IdentityMismatch(
                "LVM volume group changed".to_owned(),
            ));
        }
        let vg_lv = format!("{volume_group}/{logical_volume}");
        let info = self.inspect_lv(&vg_lv)?;
        if &info.vg_uuid != volume_group_uuid
            || &info.lv_uuid != logical_volume_uuid
            || expected_bytes.is_some_and(|bytes| info.size_bytes != bytes)
        {
            return Err(StorageError::IdentityMismatch(
                "thick logical-volume identity or size changed".to_owned(),
            ));
        }
        let mount = layout.root.to_str().ok_or_else(|| {
            StorageError::InvalidRequest(format!(
                "mount path is not UTF-8: {}",
                layout.root.display()
            ))
        })?;
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                FINDMNT,
                args(&[
                    "--noheadings",
                    "--output",
                    "UUID,FSTYPE,TARGET",
                    "--target",
                    mount,
                ]),
            ),
            "verify thick-LVM mount",
        )?;
        let expected_mount = format!("{filesystem_uuid} ext4 {mount}");
        if utf8_trim(&output.stdout, "findmnt output")? != expected_mount {
            return Err(StorageError::IdentityMismatch(
                "findmnt source, filesystem, or target changed".to_owned(),
            ));
        }
        if &self.filesystem_uuid(&info.path)? != filesystem_uuid {
            return Err(StorageError::IdentityMismatch(
                "filesystem UUID changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn rollback_created_lv(
        &self,
        layout: &ExecutionLayout,
        vg_lv: &str,
        mounted: bool,
        cause: StorageError,
    ) -> StorageError {
        let mut failures = Vec::new();
        if mounted {
            match layout.root.to_str() {
                Some(mount) => {
                    if let Err(error) = run_checked(
                        self.runner.as_ref(),
                        CommandSpec::new(UMOUNT, args(&["--", mount])),
                        "rollback execution filesystem mount",
                    ) {
                        failures.push(error.to_string());
                    }
                }
                None => failures.push(format!(
                    "rollback mount path is not UTF-8: {}",
                    layout.root.display()
                )),
            }
        }
        if let Err(error) = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(LVM, args(&["lvremove", "--yes", vg_lv])),
            "rollback exact thick logical volume",
        ) {
            failures.push(error.to_string());
        }
        if failures.is_empty() {
            cause
        } else {
            StorageError::Cleanup(format!(
                "thick-LVM allocation failed: {cause}; rollback failed: {}",
                failures.join("; ")
            ))
        }
    }
}

impl StorageBackend for LvmBackend {
    fn key(&self, labels: &OwnershipLabels) -> String {
        format!("lvm:{}/{}", self.volume_group, Self::lv_name(labels))
    }

    fn probe(&self, required_bytes: u64) -> Result<BackendCapability, StorageError> {
        self.require_root_and_tools()?;
        let pool = self.inspect_pool()?;
        if pool.extent_bytes == 0 || pool.free_bytes < required_bytes {
            return Err(StorageError::Unavailable(format!(
                "LVM pool has {} free bytes with {}-byte extents, need {required_bytes}",
                pool.free_bytes, pool.extent_bytes
            )));
        }
        *self
            .pool
            .lock()
            .map_err(|_| StorageError::Unavailable("LVM pool cache is poisoned".to_owned()))? =
            Some(pool.clone());
        Ok(BackendCapability {
            backend: "thick_lvm".to_owned(),
            pool_identity: pool.uuid,
            reservable_bytes: pool.free_bytes,
        })
    }

    fn allocate(
        &self,
        layout: &ExecutionLayout,
        labels: &OwnershipLabels,
        reserved_bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let pool = self.pool()?;
        let extents = reserved_bytes
            .checked_add(pool.extent_bytes - 1)
            .ok_or_else(|| StorageError::InvalidRequest("LVM extent count overflows".to_owned()))?
            / pool.extent_bytes;
        let allocated_bytes = extents.checked_mul(pool.extent_bytes).ok_or_else(|| {
            StorageError::InvalidRequest("LVM allocation size overflows".to_owned())
        })?;
        if allocated_bytes != reserved_bytes {
            return Err(StorageError::Unavailable(format!(
                "requested {reserved_bytes} bytes is not an exact multiple of the {}-byte LVM extent",
                pool.extent_bytes
            )));
        }
        let logical_volume = Self::lv_name(labels);
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                LVM,
                args(&[
                    "lvcreate",
                    "--yes",
                    "--type",
                    "linear",
                    "--extents",
                    &extents.to_string(),
                    "--name",
                    &logical_volume,
                    &self.volume_group,
                ]),
            ),
            "create thick logical volume",
        )?;
        let vg_lv = format!("{}/{}", self.volume_group, logical_volume);
        let mut mounted = false;
        let result = (|| {
            let info = self.inspect_lv(&vg_lv)?;
            if info.vg_uuid != pool.uuid || info.size_bytes != reserved_bytes {
                return Err(StorageError::IdentityMismatch(
                    "created logical volume does not match the probed pool or reservation"
                        .to_owned(),
                ));
            }
            run_checked(
                self.runner.as_ref(),
                CommandSpec::new(MKFS, args(&["-F", &info.path])),
                "format execution filesystem",
            )?;
            let filesystem_uuid = self.filesystem_uuid(&info.path)?;
            let mount = layout.root.to_str().ok_or_else(|| {
                StorageError::InvalidRequest(format!(
                    "mount path is not UTF-8: {}",
                    layout.root.display()
                ))
            })?;
            run_checked(
                self.runner.as_ref(),
                CommandSpec::new(
                    MOUNT,
                    args(&[
                        "--types",
                        "ext4",
                        "--options",
                        "nodev,nosuid",
                        &info.path,
                        mount,
                    ]),
                ),
                "mount execution filesystem",
            )?;
            mounted = true;
            let identity = BackendIdentity::Lvm {
                volume_group: self.volume_group.clone(),
                volume_group_uuid: info.vg_uuid,
                logical_volume,
                logical_volume_uuid: info.lv_uuid,
                filesystem_uuid,
            };
            self.verify_internal(layout, &identity, Some(reserved_bytes))?;
            Ok(identity)
        })();
        result.map_err(|cause| self.rollback_created_lv(layout, &vg_lv, mounted, cause))
    }

    fn verify(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        reserved_bytes: u64,
    ) -> Result<(), StorageError> {
        self.verify_internal(layout, identity, Some(reserved_bytes))
    }

    fn release(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        self.verify_internal(layout, identity, None)?;
        let (volume_group, logical_volume) = match identity {
            BackendIdentity::Lvm {
                volume_group,
                logical_volume,
                ..
            } => (volume_group, logical_volume),
            _ => {
                return Err(StorageError::IdentityMismatch(
                    "LVM backend received a non-LVM identity".to_owned(),
                ))
            }
        };
        let mount = layout.root.to_str().ok_or_else(|| {
            StorageError::InvalidRequest(format!(
                "mount path is not UTF-8: {}",
                layout.root.display()
            ))
        })?;
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(UMOUNT, args(&["--", mount])),
            "unmount exact execution filesystem",
        )?;
        let vg_lv = format!("{volume_group}/{logical_volume}");
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(LVM, args(&["lvremove", "--yes", &vg_lv])),
            "remove exact thick logical volume",
        )?;
        Ok(())
    }
}

#[derive(Debug)]
struct LvInfo {
    vg_uuid: String,
    lv_uuid: String,
    size_bytes: u64,
    path: String,
}

fn utf8_trim<'a>(bytes: &'a [u8], purpose: &str) -> Result<&'a str, StorageError> {
    std::str::from_utf8(bytes)
        .map(str::trim)
        .map_err(|error| StorageError::Unavailable(format!("{purpose} is not UTF-8: {error}")))
}

fn parse_u64(value: &str, purpose: &str) -> Result<u64, StorageError> {
    value
        .parse()
        .map_err(|error| StorageError::Unavailable(format!("invalid {purpose}: {error}")))
}
