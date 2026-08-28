use execution_storage::{
    disk_gib_to_bytes, ApfsBackend, CommandOutput, CommandRunner, CommandSpec, ExecutionLayout,
    LvmBackend, ProcessCommandRunner, StorageBackend, StorageError,
};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use std::{
    collections::VecDeque,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

fn labels() -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new("node-417-impl-01"),
        worker_id: WorkerId::new("buildbox-02"),
        repository: "InferWeave/autospec-orchestrator".to_owned(),
        issue: Some("417".to_owned()),
    }
}

fn command(program: &str, args: &[&str]) -> CommandSpec {
    CommandSpec::new(program, args.iter().map(OsString::from))
}

fn success(stdout: impl Into<Vec<u8>>) -> CommandOutput {
    CommandOutput {
        code: 0,
        stdout: stdout.into(),
        stderr: Vec::new(),
    }
}

fn failure(code: i32, stderr: &str) -> CommandOutput {
    CommandOutput {
        code,
        stdout: Vec::new(),
        stderr: stderr.as_bytes().to_vec(),
    }
}

#[derive(Debug)]
struct FakeRunner {
    expected: Mutex<VecDeque<(CommandSpec, Result<CommandOutput, StorageError>)>>,
}

impl FakeRunner {
    fn new(expected: Vec<(CommandSpec, Result<CommandOutput, StorageError>)>) -> Arc<Self> {
        Arc::new(Self {
            expected: Mutex::new(expected.into()),
        })
    }

    fn assert_drained(&self) {
        assert!(
            self.expected.lock().expect("fake command queue").is_empty(),
            "not every separately-argued command was invoked"
        );
    }
}

impl CommandRunner for FakeRunner {
    fn run(&self, actual: &CommandSpec) -> Result<CommandOutput, StorageError> {
        let (expected, output) = self
            .expected
            .lock()
            .expect("fake command queue")
            .pop_front()
            .expect("unexpected command");
        assert_eq!(*actual, expected);
        output
    }
}

fn apfs_info(path: &Path, volume: &str, uuid: &str) -> Vec<u8> {
    format!(
        "   Device Identifier:        {volume}\n\
         APFS Container:              disk3\n\
         File System Personality:     APFS\n\
         Volume UUID:                 {uuid}\n\
         Mount Point:                 {}\n\
         Volume Read-Only:            No\n",
        path.display()
    )
    .into_bytes()
}

fn apfs_list(volume: &str, reserve: u64, quota: u64, free: u64) -> Vec<u8> {
    format!(
        "APFS Container (1 found)\n\
         +-- Container disk3\n\
             Capacity Not Allocated: {free} B\n\
             +-> Volume {volume}\n\
                 Capacity Reserve: {reserve} B\n\
                 Capacity Quota: {quota} B\n"
    )
    .into_bytes()
}

#[test]
fn apfs_uses_quota_and_reserve_and_verifies_exact_mount_identity() {
    let state = tempfile::tempdir().expect("state root");
    let mount = state.path().join("executions/node-417-impl-01");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let bytes = disk_gib_to_bytes(3).expect("bytes");
    let diskutil = "/usr/sbin/diskutil";
    let id = "/usr/bin/id";
    let volume_name = "autospec-node-417-impl-01";
    let runner = FakeRunner::new(vec![
        (command(id, &["-u"]), Ok(success("0\n"))),
        (
            command(diskutil, &["info", state.path().to_str().unwrap()]),
            Ok(success(apfs_info(state.path(), "disk3s5", "ROOT-UUID"))),
        ),
        (
            command(diskutil, &["apfs", "list", "disk3"]),
            Ok(success(apfs_list("disk3s5", 0, 0, bytes * 2))),
        ),
        (
            command(diskutil, &["info", state.path().to_str().unwrap()]),
            Ok(success(apfs_info(state.path(), "disk3s5", "ROOT-UUID"))),
        ),
        (
            command(
                diskutil,
                &[
                    "apfs",
                    "addVolume",
                    "disk3",
                    "APFS",
                    volume_name,
                    "-quota",
                    &format!("{bytes}b"),
                    "-reserve",
                    &format!("{bytes}b"),
                    "-mountpoint",
                    mount.to_str().unwrap(),
                ],
            ),
            Ok(success("Created new APFS Volume disk9s1\n")),
        ),
        (
            command(diskutil, &["info", mount.to_str().unwrap()]),
            Ok(success(apfs_info(&mount, "disk9s1", "EXEC-UUID"))),
        ),
        (
            command(diskutil, &["apfs", "list", "disk3"]),
            Ok(success(apfs_list("disk9s1", bytes, bytes, bytes))),
        ),
        (
            command(diskutil, &["info", mount.to_str().unwrap()]),
            Ok(success(apfs_info(&mount, "disk9s1", "EXEC-UUID"))),
        ),
        (
            command(diskutil, &["apfs", "list", "disk3"]),
            Ok(success(apfs_list("disk9s1", bytes, bytes, bytes))),
        ),
        (
            command(diskutil, &["info", mount.to_str().unwrap()]),
            Ok(success(apfs_info(&mount, "disk9s1", "EXEC-UUID"))),
        ),
        (
            command(diskutil, &["apfs", "list", "disk3"]),
            Ok(success(apfs_list("disk9s1", bytes, bytes, bytes))),
        ),
        (
            command(diskutil, &["apfs", "deleteVolume", "disk9s1"]),
            Ok(success("Deleted APFS Volume\n")),
        ),
    ]);
    let backend = ApfsBackend::new(state.path(), runner.clone()).expect("APFS backend");
    let capability = backend.probe(bytes).expect("APFS capability");
    assert_eq!(capability.pool_identity, "disk3");
    assert!(capability.reservable_bytes >= bytes);
    let layout = ExecutionLayout::new(state.path(), &labels().execution_id).expect("layout");
    let identity = backend
        .allocate(&layout, &labels(), bytes)
        .expect("APFS allocation");
    backend
        .verify(&layout, &identity, bytes)
        .expect("APFS verification");
    backend
        .release(&layout, &identity)
        .expect("exact APFS release");
    runner.assert_drained();
}

#[test]
fn apfs_probe_fails_closed_without_root_privilege() {
    let root = tempfile::tempdir().expect("state root");
    let runner = FakeRunner::new(vec![(
        command("/usr/bin/id", &["-u"]),
        Ok(success("501\n")),
    )]);
    let backend = ApfsBackend::new(root.path(), runner.clone()).expect("APFS backend");
    assert!(matches!(
        backend.probe(disk_gib_to_bytes(1).unwrap()),
        Err(StorageError::Unavailable(message)) if message.contains("root")
    ));
    runner.assert_drained();
}

#[test]
fn apfs_allocation_removes_exact_created_volume_when_reservation_proof_fails() {
    let state = tempfile::tempdir().expect("state root");
    let mount = state.path().join("executions/node-417-impl-01");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let bytes = disk_gib_to_bytes(1).expect("bytes");
    let runner = FakeRunner::new(vec![
        (
            command(
                "/usr/sbin/diskutil",
                &["info", state.path().to_str().unwrap()],
            ),
            Ok(success(apfs_info(state.path(), "disk3s5", "ROOT-UUID"))),
        ),
        (
            command(
                "/usr/sbin/diskutil",
                &[
                    "apfs",
                    "addVolume",
                    "disk3",
                    "APFS",
                    "autospec-node-417-impl-01",
                    "-quota",
                    &format!("{bytes}b"),
                    "-reserve",
                    &format!("{bytes}b"),
                    "-mountpoint",
                    mount.to_str().unwrap(),
                ],
            ),
            Ok(success("Created new APFS Volume disk9s1\n")),
        ),
        (
            command("/usr/sbin/diskutil", &["info", mount.to_str().unwrap()]),
            Ok(success(apfs_info(&mount, "disk9s1", "EXEC-UUID"))),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(apfs_list("disk9s1", 0, 0, bytes))),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "deleteVolume", "disk9s1"]),
            Ok(success("Deleted APFS Volume\n")),
        ),
    ]);
    let backend = ApfsBackend::new(state.path(), runner.clone()).expect("APFS backend");
    let layout = ExecutionLayout::new(state.path(), &labels().execution_id).expect("layout");

    assert!(matches!(
        backend.allocate(&layout, &labels(), bytes),
        Err(StorageError::IdentityMismatch(message)) if message.contains("quota and reserve")
    ));
    runner.assert_drained();
}

fn lvm_row(vg_uuid: &str, free: u64, extent: u64) -> Vec<u8> {
    format!("{vg_uuid}:{free}:{extent}\n").into_bytes()
}

fn lv_row(vg_uuid: &str, lv_uuid: &str, size: u64, path: &str) -> Vec<u8> {
    format!("{vg_uuid}:{lv_uuid}:{size}:{path}\n").into_bytes()
}

#[test]
fn thick_lvm_reserves_extents_formats_mounts_and_verifies_every_identity() {
    let state = tempfile::tempdir().expect("state root");
    let mount = state.path().join("executions/node-417-impl-01");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let bytes = disk_gib_to_bytes(3).expect("bytes");
    let extent = 4 * 1024 * 1024;
    let extents = bytes / extent;
    let lv = "autospec-node-417-impl-01";
    let device = format!("/dev/vg-autospec/{lv}");
    let vg_lv = format!("vg-autospec/{lv}");
    let mut expected = vec![(command("/usr/bin/id", &["-u"]), Ok(success("0\n")))];
    for (program, version_arg) in [
        ("/usr/sbin/lvm", "version"),
        ("/usr/sbin/mkfs.ext4", "-V"),
        ("/usr/bin/mount", "--version"),
        ("/usr/bin/umount", "--version"),
        ("/usr/bin/findmnt", "--version"),
        ("/usr/sbin/blkid", "--version"),
    ] {
        expected.push((command(program, &[version_arg]), Ok(success("available\n"))));
    }
    expected.extend([
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "vgs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,vg_free,vg_extent_size",
                    "vg-autospec",
                ],
            ),
            Ok(success(lvm_row("VG-UUID", bytes * 2, extent))),
        ),
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "lvcreate",
                    "--yes",
                    "--type",
                    "linear",
                    "--extents",
                    &extents.to_string(),
                    "--name",
                    lv,
                    "vg-autospec",
                ],
            ),
            Ok(success("created\n")),
        ),
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "lvs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,lv_uuid,lv_size,lv_path",
                    &vg_lv,
                ],
            ),
            Ok(success(lv_row("VG-UUID", "LV-UUID", bytes, &device))),
        ),
        (
            command("/usr/sbin/mkfs.ext4", &["-F", &device]),
            Ok(success("formatted\n")),
        ),
        (
            command(
                "/usr/sbin/blkid",
                &["--output", "value", "--match-tag", "UUID", &device],
            ),
            Ok(success("FS-UUID\n")),
        ),
        (
            command(
                "/usr/bin/mount",
                &[
                    "--types",
                    "ext4",
                    "--options",
                    "nodev,nosuid",
                    &device,
                    mount.to_str().unwrap(),
                ],
            ),
            Ok(success(Vec::new())),
        ),
    ]);
    for _ in 0..3 {
        expected.extend([
            (
                command(
                    "/usr/sbin/lvm",
                    &[
                        "lvs",
                        "--noheadings",
                        "--units",
                        "b",
                        "--nosuffix",
                        "--separator",
                        ":",
                        "--options",
                        "vg_uuid,lv_uuid,lv_size,lv_path",
                        &vg_lv,
                    ],
                ),
                Ok(success(lv_row("VG-UUID", "LV-UUID", bytes, &device))),
            ),
            (
                command(
                    "/usr/bin/findmnt",
                    &[
                        "--noheadings",
                        "--output",
                        "UUID,FSTYPE,TARGET",
                        "--target",
                        mount.to_str().unwrap(),
                    ],
                ),
                Ok(success(format!("FS-UUID ext4 {}\n", mount.display()))),
            ),
            (
                command(
                    "/usr/sbin/blkid",
                    &["--output", "value", "--match-tag", "UUID", &device],
                ),
                Ok(success("FS-UUID\n")),
            ),
        ]);
    }
    expected.extend([
        (
            command("/usr/bin/umount", &["--", mount.to_str().unwrap()]),
            Ok(success(Vec::new())),
        ),
        (
            command("/usr/sbin/lvm", &["lvremove", "--yes", &vg_lv]),
            Ok(success("removed\n")),
        ),
    ]);
    let runner = FakeRunner::new(expected);
    let backend = LvmBackend::new("vg-autospec", runner.clone()).expect("LVM backend");
    assert!(
        backend
            .probe(bytes)
            .expect("LVM capability")
            .reservable_bytes
            >= bytes
    );
    let layout = ExecutionLayout::new(state.path(), &labels().execution_id).expect("layout");
    let identity = backend
        .allocate(&layout, &labels(), bytes)
        .expect("thick LVM allocation");
    backend
        .verify(&layout, &identity, bytes)
        .expect("LVM verification");
    backend
        .release(&layout, &identity)
        .expect("exact LVM release");
    runner.assert_drained();
}

#[test]
fn thick_lvm_probe_fails_closed_when_a_required_tool_is_missing() {
    let runner = FakeRunner::new(vec![
        (command("/usr/bin/id", &["-u"]), Ok(success("0\n"))),
        (
            command("/usr/sbin/lvm", &["version"]),
            Ok(failure(127, "lvm missing")),
        ),
    ]);
    let backend = LvmBackend::new("vg-autospec", runner.clone()).expect("LVM backend");

    assert!(matches!(
        backend.probe(disk_gib_to_bytes(1).unwrap()),
        Err(StorageError::Command(message)) if message.contains("lvm missing")
    ));
    runner.assert_drained();
}

#[test]
fn thick_lvm_allocation_unmounts_and_removes_exact_lv_when_mount_proof_fails() {
    let state = tempfile::tempdir().expect("state root");
    let mount = state.path().join("executions/node-417-impl-01");
    std::fs::create_dir_all(&mount).expect("mountpoint");
    let bytes = disk_gib_to_bytes(1).expect("bytes");
    let extent = 4 * 1024 * 1024;
    let extents = bytes / extent;
    let lv = "autospec-node-417-impl-01";
    let device = format!("/dev/vg-autospec/{lv}");
    let vg_lv = format!("vg-autospec/{lv}");
    let mut expected = vec![(command("/usr/bin/id", &["-u"]), Ok(success("0\n")))];
    for (program, version_arg) in [
        ("/usr/sbin/lvm", "version"),
        ("/usr/sbin/mkfs.ext4", "-V"),
        ("/usr/bin/mount", "--version"),
        ("/usr/bin/umount", "--version"),
        ("/usr/bin/findmnt", "--version"),
        ("/usr/sbin/blkid", "--version"),
    ] {
        expected.push((command(program, &[version_arg]), Ok(success("available\n"))));
    }
    expected.extend([
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "vgs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,vg_free,vg_extent_size",
                    "vg-autospec",
                ],
            ),
            Ok(success(lvm_row("VG-UUID", bytes * 2, extent))),
        ),
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "lvcreate",
                    "--yes",
                    "--type",
                    "linear",
                    "--extents",
                    &extents.to_string(),
                    "--name",
                    lv,
                    "vg-autospec",
                ],
            ),
            Ok(success("created\n")),
        ),
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "lvs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,lv_uuid,lv_size,lv_path",
                    &vg_lv,
                ],
            ),
            Ok(success(lv_row("VG-UUID", "LV-UUID", bytes, &device))),
        ),
        (
            command("/usr/sbin/mkfs.ext4", &["-F", &device]),
            Ok(success("formatted\n")),
        ),
        (
            command(
                "/usr/sbin/blkid",
                &["--output", "value", "--match-tag", "UUID", &device],
            ),
            Ok(success("FS-UUID\n")),
        ),
        (
            command(
                "/usr/bin/mount",
                &[
                    "--types",
                    "ext4",
                    "--options",
                    "nodev,nosuid",
                    &device,
                    mount.to_str().unwrap(),
                ],
            ),
            Ok(success(Vec::new())),
        ),
        (
            command(
                "/usr/sbin/lvm",
                &[
                    "lvs",
                    "--noheadings",
                    "--units",
                    "b",
                    "--nosuffix",
                    "--separator",
                    ":",
                    "--options",
                    "vg_uuid,lv_uuid,lv_size,lv_path",
                    &vg_lv,
                ],
            ),
            Ok(success(lv_row("VG-UUID", "LV-UUID", bytes, &device))),
        ),
        (
            command(
                "/usr/bin/findmnt",
                &[
                    "--noheadings",
                    "--output",
                    "UUID,FSTYPE,TARGET",
                    "--target",
                    mount.to_str().unwrap(),
                ],
            ),
            Ok(success(format!("FOREIGN-FS ext4 {}\n", mount.display()))),
        ),
        (
            command("/usr/bin/umount", &["--", mount.to_str().unwrap()]),
            Ok(success(Vec::new())),
        ),
        (
            command("/usr/sbin/lvm", &["lvremove", "--yes", &vg_lv]),
            Ok(success("removed\n")),
        ),
    ]);
    let runner = FakeRunner::new(expected);
    let backend = LvmBackend::new("vg-autospec", runner.clone()).expect("LVM backend");
    backend.probe(bytes).expect("LVM capability");
    let layout = ExecutionLayout::new(state.path(), &labels().execution_id).expect("layout");

    assert!(matches!(
        backend.allocate(&layout, &labels(), bytes),
        Err(StorageError::IdentityMismatch(message)) if message.contains("findmnt")
    ));
    runner.assert_drained();
}

#[test]
fn real_platform_probe_is_explicitly_skipped_without_an_operator_pool() {
    #[cfg(target_os = "macos")]
    {
        let root = tempfile::tempdir().expect("real APFS probe directory");
        let runner: Arc<dyn CommandRunner> = Arc::new(ProcessCommandRunner);
        let backend = ApfsBackend::new(root.path(), runner).expect("real APFS backend");
        match backend.probe(disk_gib_to_bytes(1).expect("probe bytes")) {
            Ok(capability) => println!(
                "SKIP real APFS allocation: pool {} is probeable but no destructive test pool is configured",
                capability.pool_identity
            ),
            Err(error) => println!("SKIP real APFS allocation: {error}"),
        }
    }
    #[cfg(target_os = "linux")]
    {
        match std::env::var("AUTOSPEC_LVM_VOLUME_GROUP") {
            Ok(volume_group) => {
                let runner: Arc<dyn CommandRunner> = Arc::new(ProcessCommandRunner);
                LvmBackend::new(volume_group, runner)
                    .expect("configured real LVM backend")
                    .probe(disk_gib_to_bytes(1).expect("probe bytes"))
                    .expect("configured real thick-LVM pool must be usable");
                println!("SKIP real thick-LVM allocation: probe passed but destructive tests are opt-in only");
            }
            Err(_) => println!(
                "SKIP real thick-LVM allocation: AUTOSPEC_LVM_VOLUME_GROUP is not configured"
            ),
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    println!("SKIP real storage allocation: platform has no supported backend");
}

#[test]
fn command_spec_keeps_program_and_arguments_separate() {
    let spec = CommandSpec::new(
        PathBuf::from("/usr/sbin/diskutil"),
        [OsString::from("apfs"), OsString::from("list")],
    );
    assert_eq!(spec.program, PathBuf::from("/usr/sbin/diskutil"));
    assert_eq!(spec.arguments, vec!["apfs", "list"]);
}
