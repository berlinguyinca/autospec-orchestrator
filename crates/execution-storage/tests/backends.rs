use execution_storage::{
    ApfsBackend, BackendState, CommandOutput, CommandRunner, CommandSpec, ExecutionLayout,
    LvmBackend, ProcessCommandRunner, StorageBackend, StorageError,
};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use std::{
    collections::VecDeque,
    ffi::OsString,
    fs,
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};

const TOKEN: &str = "0123456789abcdef01234567";

fn labels() -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new("node-417-impl-01"),
        worker_id: WorkerId::new("buildbox-02"),
        repository: "InferWeave/autospec-orchestrator".to_owned(),
        issue: Some("417".to_owned()),
    }
}
fn command(program: &str, arguments: &[&str]) -> CommandSpec {
    CommandSpec::new(program, arguments.iter().map(OsString::from))
}
fn success(stdout: impl Into<Vec<u8>>) -> CommandOutput {
    CommandOutput {
        code: 0,
        stdout: stdout.into(),
        stderr: Vec::new(),
    }
}
fn failure(stderr: &str) -> CommandOutput {
    CommandOutput {
        code: 5,
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
            self.expected.lock().expect("commands").is_empty(),
            "not every separately-argued command ran"
        );
    }
}
impl CommandRunner for FakeRunner {
    fn run(&self, actual: &CommandSpec) -> Result<CommandOutput, StorageError> {
        let (expected, output) = self
            .expected
            .lock()
            .expect("commands")
            .pop_front()
            .expect("unexpected command");
        assert_eq!(*actual, expected);
        output
    }
}

fn apfs_info(target: &Path, device: &str, name: &str, uuid: &str, mount: &str) -> Vec<u8> {
    format!("Device Identifier: {device}\nAPFS Container: disk3\nAPFS Container UUID: POOL-UUID\nFile System Personality: APFS\nVolume Name: {name}\nVolume UUID: {uuid}\nMount Point: {mount}\nVolume Read-Only: No\nProbe: {}\n", target.display()).into_bytes()
}
fn apfs_list(device: &str, name: &str, bytes: u64, free: u64) -> Vec<u8> {
    format!("+-- Container disk3 POOL-UUID\nCapacity Not Allocated: {free} B\n+-> Volume {device} VOL-UUID\nName: {name} (Case-insensitive)\nCapacity Reserve: {bytes} B\nCapacity Quota: {bytes} B\n").into_bytes()
}

#[test]
fn apfs_create_identity_is_proved_before_mount_and_cleanup_rechecks_uuid_and_token() {
    let state = tempfile::tempdir().expect("state");
    let canonical_state = state.path().canonicalize().expect("canonical state");
    let mount = canonical_state.join("executions/node-417-impl-01");
    fs::create_dir_all(&mount).expect("mount");
    let bytes = 16 * 1024 * 1024;
    let name = format!("autospec-{TOKEN}");
    let info_unmounted = apfs_info(state.path(), "disk9s1", &name, "VOL-UUID", "Not mounted");
    let info_mounted = apfs_info(
        state.path(),
        "disk9s1",
        &name,
        "VOL-UUID",
        mount.to_str().unwrap(),
    );
    let list = apfs_list("disk9s1", &name, bytes, bytes * 4);
    let root_info = apfs_info(
        &canonical_state,
        "disk3s5",
        "Data",
        "ROOT-UUID",
        canonical_state.to_str().unwrap(),
    );
    let mut expected = vec![
        (command("/usr/bin/id", &["-u"]), Ok(success("0\n"))),
        (
            command(
                "/usr/sbin/diskutil",
                &["info", canonical_state.to_str().unwrap()],
            ),
            Ok(success(root_info.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command(
                "/usr/sbin/diskutil",
                &["info", canonical_state.to_str().unwrap()],
            ),
            Ok(success(root_info)),
        ),
        (
            command(
                "/usr/sbin/diskutil",
                &[
                    "apfs",
                    "addVolume",
                    "disk3",
                    "APFS",
                    &name,
                    "-quota",
                    &format!("{bytes}b"),
                    "-reserve",
                    &format!("{bytes}b"),
                    "-nomount",
                ],
            ),
            Ok(success("Created disk9s1\n")),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_unmounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_unmounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_unmounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command(
                "/usr/sbin/diskutil",
                &["mount", "-mountPoint", mount.to_str().unwrap(), "disk9s1"],
            ),
            Ok(success(Vec::new())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_mounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_mounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_mounted)),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["unmount", "disk9s1"]),
            Ok(success(Vec::new())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_unmounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_unmounted.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list.clone())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(success(info_unmounted)),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "list", "disk3"]),
            Ok(success(list)),
        ),
        (
            command("/usr/sbin/diskutil", &["apfs", "deleteVolume", "disk9s1"]),
            Ok(success(Vec::new())),
        ),
        (
            command("/usr/sbin/diskutil", &["info", "disk9s1"]),
            Ok(failure("Could not find disk")),
        ),
    ];
    let runner = FakeRunner::new(std::mem::take(&mut expected));
    let backend = ApfsBackend::new(state.path(), runner.clone()).expect("backend");
    backend.probe(bytes).expect("probe");
    let layout = ExecutionLayout::new(&canonical_state, &labels().execution_id).expect("layout");
    let created = backend
        .create(&layout, &labels(), TOKEN, bytes)
        .expect("create");
    let prepared = backend.prepare(&layout, &created, bytes).expect("prepare");
    backend.mount(&layout, &prepared, bytes).expect("mount");
    backend.unmount(&layout, &prepared).expect("unmount");
    assert_eq!(
        backend.state(&layout, &prepared, bytes).expect("state"),
        BackendState::Unmounted
    );
    backend.remove(&prepared).expect("remove");
    assert_eq!(
        backend.state(&layout, &prepared, bytes).expect("absent"),
        BackendState::Absent
    );
    runner.assert_drained();
}

fn lvm_row(vg_uuid: &str, free: u64, extent: u64) -> Vec<u8> {
    format!("{vg_uuid}:{free}:{extent}\n").into_bytes()
}
fn lv_row(bytes: u64, device: &str) -> Vec<u8> {
    format!("VG-UUID:LV-UUID:{bytes}:{device}:autospec.{TOKEN}:autospec-{TOKEN}\n").into_bytes()
}

#[test]
fn lvm_create_is_tagged_and_identified_before_format_mount_and_exact_removal() {
    let state = tempfile::tempdir().expect("state");
    let mount = state.path().join("executions/node-417-impl-01");
    fs::create_dir_all(&mount).expect("mount");
    let bytes = 16 * 1024 * 1024;
    let extent = 4 * 1024 * 1024;
    let device = format!("/dev/vg-autospec/autospec-{TOKEN}");
    let target = format!("vg-autospec/autospec-{TOKEN}");
    let mut expected = vec![(command("/usr/bin/id", &["-u"]), Ok(success("0\n")))];
    for (program, version) in [
        ("/usr/sbin/lvm", "version"),
        ("/usr/sbin/mkfs.ext4", "-V"),
        ("/usr/bin/mount", "--version"),
        ("/usr/bin/umount", "--version"),
        ("/usr/bin/findmnt", "--version"),
        ("/usr/sbin/blkid", "--version"),
    ] {
        expected.push((command(program, &[version]), Ok(success("ok\n"))));
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
                    "--",
                    "vg-autospec",
                ],
            ),
            Ok(success(lvm_row("VG-UUID", bytes * 4, extent))),
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
                    "4",
                    "--name",
                    &format!("autospec-{TOKEN}"),
                    "--addtag",
                    &format!("autospec.{TOKEN}"),
                    "--",
                    "vg-autospec",
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
        ),
        (
            command("/usr/sbin/mkfs.ext4", &["-F", "--", &device]),
            Ok(success(Vec::new())),
        ),
        (
            command(
                "/usr/sbin/blkid",
                &["--output", "value", "--match-tag", "UUID", "--", &device],
            ),
            Ok(success("FS-UUID\n")),
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
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
            Ok(failure("not mounted")),
        ),
        (
            command(
                "/usr/bin/mount",
                &[
                    "--types",
                    "ext4",
                    "--options",
                    "nodev,nosuid",
                    "--",
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
        ),
        (
            command(
                "/usr/sbin/blkid",
                &["--output", "value", "--match-tag", "UUID", "--", &device],
            ),
            Ok(success("FS-UUID\n")),
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
        ),
        (
            command("/usr/bin/umount", &["--", mount.to_str().unwrap()]),
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
        ),
        (
            command(
                "/usr/sbin/blkid",
                &["--output", "value", "--match-tag", "UUID", "--", &device],
            ),
            Ok(success("FS-UUID\n")),
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
            Ok(failure("not mounted")),
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(success(lv_row(bytes, &device))),
        ),
        (
            command("/usr/sbin/lvm", &["lvremove", "--yes", "--", &target]),
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
                    "vg_uuid,lv_uuid,lv_size,lv_path,lv_tags,lv_name",
                    "--",
                    &target,
                ],
            ),
            Ok(failure("Failed to find logical volume")),
        ),
    ]);
    let runner = FakeRunner::new(expected);
    let backend = LvmBackend::new("vg-autospec", runner.clone()).expect("backend");
    backend.probe(bytes).expect("probe");
    let layout = ExecutionLayout::new(state.path(), &labels().execution_id).expect("layout");
    let created = backend
        .create(&layout, &labels(), TOKEN, bytes)
        .expect("create");
    assert!(created.filesystem_id().is_empty());
    let prepared = backend.prepare(&layout, &created, bytes).expect("prepare");
    backend.mount(&layout, &prepared, bytes).expect("mount");
    backend.unmount(&layout, &prepared).expect("unmount");
    assert_eq!(
        backend.state(&layout, &prepared, bytes).expect("unmounted"),
        BackendState::Unmounted
    );
    backend.remove(&prepared).expect("remove");
    assert_eq!(
        backend.state(&layout, &prepared, bytes).expect("absent"),
        BackendState::Absent
    );
    runner.assert_drained();
}

#[test]
fn lvm_volume_group_validation_rejects_option_injection_and_reserved_names() {
    let runner: Arc<dyn CommandRunner> = Arc::new(ProcessCommandRunner);
    for invalid in [
        "-vg",
        ".",
        "..",
        "snapshot",
        "pvmove",
        "mirrorpool",
        "bad/name",
        "",
    ] {
        assert!(
            LvmBackend::new(invalid, runner.clone()).is_err(),
            "accepted {invalid}"
        );
    }
    LvmBackend::new("vg.safe-01", runner).expect("full safe grammar");
}

#[test]
fn missing_required_lvm_tool_fails_closed() {
    let runner = FakeRunner::new(vec![
        (command("/usr/bin/id", &["-u"]), Ok(success("0\n"))),
        (
            command("/usr/sbin/lvm", &["version"]),
            Ok(failure("missing")),
        ),
    ]);
    let backend = LvmBackend::new("vg-autospec", runner.clone()).expect("backend");
    assert!(backend.probe(16 * 1024 * 1024).is_err());
    runner.assert_drained();
}

#[test]
fn command_spec_keeps_program_and_arguments_separate() {
    let spec = command("/usr/sbin/lvm", &["lvs", "--", "vg.safe"]);
    assert!(spec.program.is_absolute());
    assert_eq!(spec.arguments, vec!["lvs", "--", "vg.safe"]);
}

#[test]
fn configured_real_pool_runs_full_quota_lifecycle_or_explicitly_skips() {
    #[cfg(target_os = "macos")]
    let configured = std::env::var("AUTOSPEC_APFS_PROBE_PATH")
        .ok()
        .map(|path| ("apfs", path));
    #[cfg(target_os = "linux")]
    let configured = std::env::var("AUTOSPEC_LVM_VOLUME_GROUP")
        .ok()
        .map(|name| ("lvm", name));
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let configured: Option<(&str, String)> = None;
    let Some((kind, configured)) = configured else {
        println!("SKIP real storage quota lifecycle: operator pool configuration is absent");
        return;
    };
    let bytes = std::env::var("AUTOSPEC_STORAGE_TEST_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16 * 1024 * 1024);
    let state = tempfile::tempdir().expect("state");
    let mount = state.path().join("executions/real-storage-proof");
    fs::create_dir_all(&mount).expect("mount");
    let layout = ExecutionLayout::new(state.path(), &ExecutionId::new("real-storage-proof"))
        .expect("layout");
    let runner: Arc<dyn CommandRunner> = Arc::new(ProcessCommandRunner);
    let backend: Box<dyn StorageBackend> = if kind == "apfs" {
        Box::new(ApfsBackend::new(configured, runner).expect("configured APFS backend"))
    } else {
        Box::new(LvmBackend::new(configured, runner).expect("configured LVM backend"))
    };
    backend.probe(bytes).expect("configured pool probe");
    let created = backend
        .create(&layout, &labels(), TOKEN, bytes)
        .expect("physical create and identity");
    let prepared = match backend.prepare(&layout, &created, bytes) {
        Ok(prepared) => prepared,
        Err(error) => {
            backend.remove(&created).expect("rollback created object");
            panic!("prepare configured storage: {error}");
        }
    };
    if let Err(error) = backend.mount(&layout, &prepared, bytes) {
        if matches!(
            backend.state(&layout, &prepared, bytes),
            Ok(BackendState::Mounted)
        ) {
            backend.unmount(&layout, &prepared).expect("rollback mount");
        }
        backend.remove(&prepared).expect("rollback prepared object");
        panic!("mount configured storage: {error}");
    }
    let exercise = (|| -> std::io::Result<bool> {
        fs::create_dir_all(&layout.repository)?;
        let mut first = fs::File::create(layout.repository.join("aggregate-a"))?;
        fs::create_dir_all(&layout.conversation)?;
        let mut second = fs::File::create(layout.conversation.join("aggregate-b"))?;
        let block = vec![0x5a; 1024 * 1024];
        for index in 0..=(bytes / block.len() as u64 + 2) {
            let result = if index % 2 == 0 {
                first.write_all(&block)
            } else {
                second.write_all(&block)
            };
            if result.is_err() {
                return Ok(true);
            }
        }
        Ok(false)
    })();
    backend.unmount(&layout, &prepared).expect("unmount");
    backend.remove(&prepared).expect("release");
    assert!(
        exercise.expect("exercise aggregate quota"),
        "writes beyond the configured hard bound unexpectedly succeeded"
    );
}
