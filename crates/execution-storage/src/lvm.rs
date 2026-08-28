use crate::command::{args, run_checked};
use crate::{
    BackendCapability, BackendIdentity, BackendState, CommandRunner, CommandSpec, ExecutionLayout,
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
        validate_lvm_name(&volume_group)?;
        Ok(Self {
            volume_group,
            runner,
            pool: Mutex::new(None),
        })
    }
    fn require_root_and_tools(&self) -> Result<(), StorageError> {
        if run_checked(
            self.runner.as_ref(),
            CommandSpec::new(ID, args(&["-u"])),
            "inspect LVM privilege",
        )?
        .stdout
            != b"0\n"
        {
            return Err(StorageError::Unavailable(
                "thick-LVM allocation requires root".to_owned(),
            ));
        }
        for (program, version) in [
            (LVM, "version"),
            (MKFS, "-V"),
            (MOUNT, "--version"),
            (UMOUNT, "--version"),
            (FINDMNT, "--version"),
            (BLKID, "--version"),
        ] {
            run_checked(
                self.runner.as_ref(),
                CommandSpec::new(program, args(&[version])),
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
                    "--",
                    &self.volume_group,
                ]),
            ),
            "inspect thick-LVM pool",
        )?;
        let fields = utf8_trim(&output.stdout, "vgs output")?
            .split(':')
            .map(str::trim)
            .collect::<Vec<_>>();
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
            .map_err(|_| StorageError::Unavailable("LVM pool cache poisoned".to_owned()))?
            .clone()
            .ok_or_else(|| StorageError::Unavailable("LVM backend was not probed".to_owned()))
    }
    fn tag(token: &str) -> String {
        format!("autospec.{token}")
    }
    fn lv_name(token: &str) -> String {
        format!("autospec-{token}")
    }
    fn inspect_lv_result(&self, vg_lv: &str) -> Result<Option<LvInfo>, StorageError> {
        let command = CommandSpec::new(
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
                "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                "--",
                vg_lv,
            ]),
        );
        let output = self.runner.run(&command)?;
        if output.code != 0 {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("Failed to find") || stderr.contains("not found") {
                return Ok(None);
            }
            return Err(StorageError::Command(format!(
                "inspect logical volume exited {}: {}",
                output.code,
                stderr.trim()
            )));
        }
        parse_lv(&output.stdout).map(Some)
    }
    fn inspect_exact(
        &self,
        identity: &BackendIdentity,
        expected_bytes: u64,
    ) -> Result<Option<LvInfo>, StorageError> {
        let (vg, vg_uuid, lv, lv_uuid, _, token) = lvm_identity(identity)?;
        if vg != self.volume_group {
            return Err(StorageError::IdentityMismatch(
                "LVM volume group changed".to_owned(),
            ));
        }
        let Some(info) = self.inspect_lv_result(&format!("{vg}/{lv}"))? else {
            return Ok(None);
        };
        if info.vg_uuid != vg_uuid
            || info.lv_uuid != lv_uuid
            || info.size_bytes != expected_bytes
            || info.name != lv
            || !info
                .tags
                .split(',')
                .any(|tag| tag.trim() == Self::tag(token))
        {
            return Err(StorageError::IdentityMismatch(
                "LVM token, pool UUID, object UUID, or size changed".to_owned(),
            ));
        }
        Ok(Some(info))
    }
    fn filesystem_uuid(&self, device: &str) -> Result<String, StorageError> {
        let output = run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                BLKID,
                args(&["--output", "value", "--match-tag", "UUID", "--", device]),
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
    fn mount_state(
        &self,
        layout: &ExecutionLayout,
        filesystem_uuid: &str,
    ) -> Result<BackendState, StorageError> {
        if filesystem_uuid.is_empty() {
            return Ok(BackendState::Unmounted);
        }
        let mount = path_text(&layout.root)?;
        let output = self.runner.run(&CommandSpec::new(
            FINDMNT,
            args(&[
                "--noheadings",
                "--output",
                "UUID,FSTYPE,TARGET",
                "--target",
                mount,
            ]),
        ))?;
        if output.code != 0 {
            return Ok(BackendState::Unmounted);
        }
        if utf8_trim(&output.stdout, "findmnt output")? == format!("{filesystem_uuid} ext4 {mount}")
        {
            Ok(BackendState::Mounted)
        } else {
            Err(StorageError::IdentityMismatch(
                "findmnt source, filesystem, or target changed".to_owned(),
            ))
        }
    }
}

impl StorageBackend for LvmBackend {
    fn key(&self, labels: &OwnershipLabels) -> String {
        format!("lvm:{}:{}", self.volume_group, labels.execution_id)
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
            .map_err(|_| StorageError::Unavailable("LVM pool cache poisoned".to_owned()))? =
            Some(pool.clone());
        Ok(BackendCapability {
            backend: "thick_lvm".to_owned(),
            pool_identity: pool.uuid,
            reservable_bytes: pool.free_bytes,
        })
    }
    fn discover(
        &self,
        _layout: &ExecutionLayout,
        token: &str,
        bytes: u64,
    ) -> Result<Option<BackendIdentity>, StorageError> {
        let tag = Self::tag(token);
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--select",
                    &format!("lv_tags={tag}"),
                    "--",
                    &self.volume_group,
                ]),
            ),
            "discover owned logical volume",
        )?;
        if utf8_trim(&output.stdout, "lvs discovery output")?.is_empty() {
            return Ok(None);
        }
        let info = parse_lv(&output.stdout)?;
        if info.size_bytes != bytes
            || !info
                .tags
                .split(',')
                .any(|candidate| candidate.trim() == tag)
        {
            return Err(StorageError::IdentityMismatch(
                "discovered LVM object does not match token and size".to_owned(),
            ));
        }
        Ok(Some(BackendIdentity::Lvm {
            volume_group: self.volume_group.clone(),
            volume_group_uuid: info.vg_uuid,
            logical_volume: info.name,
            logical_volume_uuid: info.lv_uuid,
            filesystem_uuid: String::new(),
            ownership_token: token.to_owned(),
        }))
    }
    fn create(
        &self,
        _layout: &ExecutionLayout,
        _labels: &OwnershipLabels,
        token: &str,
        bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let pool = self.pool()?;
        let extents = bytes
            .checked_add(pool.extent_bytes - 1)
            .ok_or_else(|| StorageError::InvalidRequest("LVM extent count overflows".to_owned()))?
            / pool.extent_bytes;
        if extents.checked_mul(pool.extent_bytes) != Some(bytes) {
            return Err(StorageError::Unavailable(format!(
                "requested {bytes} bytes is not an exact multiple of the {}-byte LVM extent",
                pool.extent_bytes
            )));
        }
        let lv = Self::lv_name(token);
        let tag = Self::tag(token);
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
                    &lv,
                    "--addtag",
                    &tag,
                    "--",
                    &self.volume_group,
                ]),
            ),
            "create tagged thick logical volume",
        )?;
        let info = self
            .inspect_lv_result(&format!("{}/{}", self.volume_group, lv))?
            .ok_or_else(|| {
                StorageError::IdentityMismatch("created logical volume is absent".to_owned())
            })?;
        if info.vg_uuid != pool.uuid
            || info.size_bytes != bytes
            || !info
                .tags
                .split(',')
                .any(|candidate| candidate.trim() == tag)
        {
            return Err(StorageError::IdentityMismatch(
                "created logical volume identity does not match pool, token, or reservation"
                    .to_owned(),
            ));
        }
        Ok(BackendIdentity::Lvm {
            volume_group: self.volume_group.clone(),
            volume_group_uuid: info.vg_uuid,
            logical_volume: info.name,
            logical_volume_uuid: info.lv_uuid,
            filesystem_uuid: String::new(),
            ownership_token: token.to_owned(),
        })
    }
    fn prepare(
        &self,
        _layout: &ExecutionLayout,
        identity: &BackendIdentity,
        bytes: u64,
    ) -> Result<BackendIdentity, StorageError> {
        let info = self.inspect_exact(identity, bytes)?.ok_or_else(|| {
            StorageError::IdentityMismatch("logical volume disappeared before prepare".to_owned())
        })?;
        let (_, _, _, _, filesystem_uuid, _) = lvm_identity(identity)?;
        if !filesystem_uuid.is_empty() {
            return Err(StorageError::IdentityMismatch(
                "logical volume was already formatted before prepare".to_owned(),
            ));
        }
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(MKFS, args(&["-F", "--", &info.path])),
            "format exact execution filesystem",
        )?;
        let uuid = self.filesystem_uuid(&info.path)?;
        let mut prepared = identity.clone();
        if let BackendIdentity::Lvm {
            filesystem_uuid, ..
        } = &mut prepared
        {
            *filesystem_uuid = uuid;
        }
        Ok(prepared)
    }
    fn mount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
        bytes: u64,
    ) -> Result<(), StorageError> {
        let info = self.inspect_exact(identity, bytes)?.ok_or_else(|| {
            StorageError::IdentityMismatch("logical volume disappeared before mount".to_owned())
        })?;
        if self.mount_state(layout, lvm_identity(identity)?.4)? != BackendState::Unmounted {
            return Err(StorageError::IdentityMismatch(
                "logical volume was mounted before mount phase".to_owned(),
            ));
        }
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                MOUNT,
                args(&[
                    "--types",
                    "ext4",
                    "--options",
                    "nodev,nosuid",
                    "--",
                    &info.path,
                    path_text(&layout.root)?,
                ]),
            ),
            "mount exact execution filesystem",
        )?;
        if self.state(layout, identity, bytes)? != BackendState::Mounted {
            return Err(StorageError::IdentityMismatch(
                "LVM mount proof failed".to_owned(),
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
        let Some(info) = self.inspect_exact(identity, bytes)? else {
            return Ok(BackendState::Absent);
        };
        let (_, _, _, _, filesystem_uuid, _) = lvm_identity(identity)?;
        if !filesystem_uuid.is_empty() && self.filesystem_uuid(&info.path)? != filesystem_uuid {
            return Err(StorageError::IdentityMismatch(
                "filesystem UUID changed".to_owned(),
            ));
        }
        self.mount_state(layout, filesystem_uuid)
    }
    fn unmount(
        &self,
        layout: &ExecutionLayout,
        identity: &BackendIdentity,
    ) -> Result<(), StorageError> {
        let expected = lvm_identity(identity)?;
        let filesystem_uuid = expected.4;
        if filesystem_uuid.is_empty() {
            return Ok(());
        }
        let info = self
            .inspect_lv_result(&format!("{}/{}", expected.0, expected.2))?
            .ok_or_else(|| {
                StorageError::IdentityMismatch(
                    "logical volume disappeared before unmount".to_owned(),
                )
            })?;
        if info.vg_uuid != expected.1
            || info.lv_uuid != expected.3
            || !info
                .tags
                .split(',')
                .any(|tag| tag.trim() == Self::tag(expected.5))
        {
            return Err(StorageError::IdentityMismatch(
                "refuse to unmount LVM object with changed token or UUID".to_owned(),
            ));
        }
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(UMOUNT, args(&["--", path_text(&layout.root)?])),
            "unmount exact execution filesystem",
        )?;
        Ok(())
    }
    fn remove(&self, identity: &BackendIdentity) -> Result<(), StorageError> {
        let (vg, _, lv, _, _, _) = lvm_identity(identity)?;
        let info = self
            .inspect_lv_result(&format!("{vg}/{lv}"))?
            .ok_or_else(|| {
                StorageError::IdentityMismatch(
                    "logical volume already absent before removal proof".to_owned(),
                )
            })?;
        let expected = lvm_identity(identity)?;
        if info.vg_uuid != expected.1
            || info.lv_uuid != expected.3
            || !info
                .tags
                .split(',')
                .any(|tag| tag.trim() == Self::tag(expected.5))
        {
            return Err(StorageError::IdentityMismatch(
                "refuse to remove LVM object with changed token or UUID".to_owned(),
            ));
        }
        run_checked(
            self.runner.as_ref(),
            CommandSpec::new(
                LVM,
                args(&["lvremove", "--yes", "--", &format!("{vg}/{lv}")]),
            ),
            "remove exact thick logical volume",
        )?;
        Ok(())
    }
}

fn validate_lvm_name(name: &str) -> Result<(), StorageError> {
    let reserved = [".", "..", "snapshot", "pvmove"];
    let reserved_prefixes = [
        "mirror", "mimage", "mlog", "rimage", "rmeta", "tdata", "tmeta", "vdata", "vdo",
    ];
    let valid = !name.is_empty()
        && !name.starts_with('-')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"+_.-".contains(&byte))
        && !reserved.contains(&name)
        && !reserved_prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix));
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidRequest(format!(
            "invalid or reserved LVM volume group: {name}"
        )))
    }
}
fn lvm_identity(
    identity: &BackendIdentity,
) -> Result<(&str, &str, &str, &str, &str, &str), StorageError> {
    match identity {
        BackendIdentity::Lvm {
            volume_group,
            volume_group_uuid,
            logical_volume,
            logical_volume_uuid,
            filesystem_uuid,
            ownership_token,
        } => Ok((
            volume_group,
            volume_group_uuid,
            logical_volume,
            logical_volume_uuid,
            filesystem_uuid,
            ownership_token,
        )),
        _ => Err(StorageError::IdentityMismatch(
            "LVM backend received a non-LVM identity".to_owned(),
        )),
    }
}
#[derive(Debug)]
struct LvInfo {
    vg_uuid: String,
    lv_uuid: String,
    size_bytes: u64,
    path: String,
    tags: String,
    name: String,
}
fn parse_lv(output: &[u8]) -> Result<LvInfo, StorageError> {
    let fields = utf8_trim(output, "lvs output")?
        .split(':')
        .map(str::trim)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if fields.len() != 6 || fields.iter().any(String::is_empty) {
        return Err(StorageError::IdentityMismatch(
            "lvs did not return exact logical-volume identity".to_owned(),
        ));
    }
    Ok(LvInfo {
        vg_uuid: fields[0].clone(),
        lv_uuid: fields[1].clone(),
        size_bytes: parse_u64(&fields[2], "LV size bytes")?,
        path: fields[3].clone(),
        tags: fields[4].clone(),
        name: fields[5].clone(),
    })
}
fn path_text(path: &std::path::Path) -> Result<&str, StorageError> {
    path.to_str().ok_or_else(|| {
        StorageError::InvalidRequest(format!("path is not UTF-8: {}", path.display()))
    })
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
