use execution_storage::{
    disk_gib_to_bytes, AllocationReceipt, BackendIdentity, DockerBindProof, ExecutionLayout,
    ReadyAllocationVerifier, ReadyLease, StorageError, VerifiedExecutionStorage,
    ALLOCATION_API_VERSION,
};
use harness_pi::{PiHarness, PiHarnessConfig};
use harness_traits::{AgentHarness, HarnessError};
use orchestrator_core::{
    event::ExecutionEventKind, ExecutionId, FailureClass, ModelPolicy, OwnershipLabels, TaskPacket,
    WorkerId,
};
use runtime_traits::{VerifiedAgentContainer, VerifiedBindMount};
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const IMAGE: &str = "debian:bookworm-slim";

fn packet() -> TaskPacket {
    TaskPacket {
        goal: "Change only the requested behavior".to_owned(),
        acceptance_criteria: vec!["tests pass".to_owned()],
        non_goals: vec!["repo-wide prompt synthesis".to_owned()],
        relevant_context: vec!["src/lib.rs".to_owned()],
        required_tests: vec!["cargo test".to_owned()],
        role_skill: Some("role.md".to_owned()),
    }
}

struct DockerPi {
    root: TempDir,
    container: String,
    execution_id: String,
    receipt: AllocationReceipt,
    verifier: Arc<TestReadyVerifier>,
    capability: VerifiedAgentContainer,
    removed: bool,
}

#[derive(Debug, Default)]
struct LeaseState {
    active: Mutex<usize>,
    released: Condvar,
}

#[derive(Debug)]
struct TestReadyVerifier {
    expected: AllocationReceipt,
    layout: ExecutionLayout,
    reject: AtomicBool,
    lease_state: Arc<LeaseState>,
    verified_directories: Arc<Mutex<Vec<PathBuf>>>,
}

#[derive(Debug)]
struct TestVerifiedStorage {
    layout: ExecutionLayout,
    verified_directories: Arc<Mutex<Vec<PathBuf>>>,
}

impl VerifiedExecutionStorage for TestVerifiedStorage {
    fn repository_path(&self) -> &Path {
        &self.layout.repository
    }

    fn verify(&self) -> Result<(), StorageError> {
        for path in [&self.layout.root, &self.layout.repository] {
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(StorageError::IdentityMismatch(format!(
                    "verified path is not a real directory: {}",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    fn verify_directory(&self, path: &Path) -> Result<(), StorageError> {
        if !path.starts_with(&self.layout.root) {
            return Err(StorageError::IdentityMismatch(format!(
                "directory escaped allocation: {}",
                path.display()
            )));
        }
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| StorageError::IdentityMismatch(error.to_string()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StorageError::IdentityMismatch(format!(
                "directory is not real: {}",
                path.display()
            )));
        }
        self.verified_directories
            .lock()
            .unwrap()
            .push(path.to_path_buf());
        Ok(())
    }
}

#[derive(Debug)]
struct TestReadyLease {
    verified: TestVerifiedStorage,
    state: Arc<LeaseState>,
}

impl Drop for TestReadyLease {
    fn drop(&mut self) {
        let mut active = self.state.active.lock().unwrap();
        *active -= 1;
        self.state.released.notify_all();
    }
}

impl ReadyLease for TestReadyLease {
    fn verified(&self) -> &dyn VerifiedExecutionStorage {
        &self.verified
    }
}

impl ReadyAllocationVerifier for TestReadyVerifier {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        self.validate(receipt)?;
        Ok(Box::new(self.verified()))
    }

    fn acquire_ready_lease(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn ReadyLease>, StorageError> {
        self.validate(receipt)?;
        *self.lease_state.active.lock().unwrap() += 1;
        Ok(Box::new(TestReadyLease {
            verified: self.verified(),
            state: Arc::clone(&self.lease_state),
        }))
    }
}

impl TestReadyVerifier {
    fn validate(&self, receipt: &AllocationReceipt) -> Result<(), StorageError> {
        if self.reject.load(Ordering::SeqCst) || receipt != &self.expected {
            Err(StorageError::IdentityMismatch(
                "receipt is stale or forged".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn verified(&self) -> TestVerifiedStorage {
        TestVerifiedStorage {
            layout: self.layout.clone(),
            verified_directories: Arc::clone(&self.verified_directories),
        }
    }

    fn active_leases(&self) -> usize {
        *self.lease_state.active.lock().unwrap()
    }

    fn wait_for_release(&self) {
        let mut active = self.lease_state.active.lock().unwrap();
        while *active != 0 {
            active = self.lease_state.released.wait(active).unwrap();
        }
    }
}

impl DockerPi {
    fn create() -> Option<Self> {
        if !Command::new("docker")
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .is_ok_and(|output| output.status.success())
        {
            eprintln!("SKIP: Docker daemon is unavailable for harness-pi integration test");
            return None;
        }
        if !Command::new("docker")
            .args(["image", "inspect", IMAGE])
            .output()
            .is_ok_and(|output| output.status.success())
        {
            eprintln!("SKIP: required local test image {IMAGE} is unavailable");
            return None;
        }
        let root = TempDir::new().expect("temporary Docker Pi fixture");
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let execution_id = format!("pi-test-{}-{sequence}", std::process::id());
        let state_root = root.path().join("state");
        let execution = ExecutionId::new(&execution_id);
        let layout = ExecutionLayout::new(&state_root, &execution).unwrap();
        let worktree = layout.repository.clone();
        let conversation = layout.conversation.clone();
        fs::create_dir_all(worktree.join(".autospec")).unwrap();
        fs::create_dir_all(&conversation).unwrap();
        fs::create_dir_all(&layout.credentials).unwrap();
        fs::create_dir_all(&layout.runtime).unwrap();
        fs::write(worktree.join("AGENTS.md"), "Stay scoped.\n").unwrap();
        fs::write(worktree.join("role.md"), "Implement.\n").unwrap();
        fs::write(
            worktree.join("pi-json-events.jsonl"),
            include_str!("fixtures/pi-0.84.3-json-mode.jsonl"),
        )
        .unwrap();
        let stub = layout.runtime.join("pi");
        fs::write(&stub, STUB_PI).unwrap();
        fs::set_permissions(&stub, fs::Permissions::from_mode(0o755)).unwrap();
        let setsid = layout.runtime.join("setsid");
        fs::write(&setsid, STUB_SETSID).unwrap();
        fs::set_permissions(&setsid, fs::Permissions::from_mode(0o755)).unwrap();

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let container = format!("autospec-pi-test-{}-{nonce}-{sequence}", std::process::id());
        let labels = labels(&execution_id);
        let created = Command::new("docker")
            .args(["create", "--name", &container])
            .args(
                labels
                    .to_map()
                    .iter()
                    .flat_map(|(key, value)| ["--label".to_owned(), format!("{key}={value}")]),
            )
            .args([
                "--mount",
                &format!("type=bind,src={},dst=/workspace", worktree.display()),
            ])
            .args([
                "--mount",
                &format!("type=bind,src={},dst=/session", conversation.display()),
            ])
            .args([
                "--mount",
                &format!(
                    "type=bind,src={},dst=/usr/local/bin/pi,readonly",
                    stub.display()
                ),
            ])
            .args([
                "--mount",
                &format!(
                    "type=bind,src={},dst=/usr/local/bin/setsid,readonly",
                    setsid.display()
                ),
            ])
            .args([IMAGE, "sleep", "infinity"])
            .output()
            .expect("create stub Pi container");
        assert!(created.status.success(), "create stub Pi container");
        let container_id = String::from_utf8(created.stdout).unwrap().trim().to_owned();
        assert!(Command::new("docker")
            .args(["start", &container])
            .status()
            .unwrap()
            .success());
        let daemon_id = String::from_utf8(
            Command::new("docker")
                .args(["info", "--format", "{{.ID}}"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        let receipt = AllocationReceipt {
            api_version: ALLOCATION_API_VERSION.to_owned(),
            labels: labels.clone(),
            reserved_bytes: disk_gib_to_bytes(1).unwrap(),
            mount_path: layout.root.clone(),
            backend_kind: "test".to_owned(),
            backend_key: "test-backend".to_owned(),
            pool_identity: "test-pool".to_owned(),
            backend: BackendIdentity::Apfs {
                container: "test-container".to_owned(),
                container_uuid: "test-container-uuid".to_owned(),
                volume: "test-volume".to_owned(),
                volume_name: "test-volume-name".to_owned(),
                volume_uuid: "test-filesystem".to_owned(),
                ownership_token: "test-owner-token".to_owned(),
            },
            docker_bind: DockerBindProof {
                daemon_id: daemon_id.clone(),
                verifier: "test-verifier".to_owned(),
                method_version: "test-method-v1".to_owned(),
                source_path: layout.root.clone(),
                filesystem_id: "test-filesystem".to_owned(),
            },
        };
        let verifier = Arc::new(TestReadyVerifier {
            expected: receipt.clone(),
            layout,
            reject: AtomicBool::new(false),
            lease_state: Arc::new(LeaseState::default()),
            verified_directories: Arc::new(Mutex::new(Vec::new())),
        });
        let mut mounts = vec![
            VerifiedBindMount {
                source: fs::canonicalize(&worktree).unwrap(),
                target: "/workspace".to_owned(),
                writable: true,
            },
            VerifiedBindMount {
                source: fs::canonicalize(&conversation).unwrap(),
                target: "/session".to_owned(),
                writable: true,
            },
            VerifiedBindMount {
                source: fs::canonicalize(&stub).unwrap(),
                target: "/usr/local/bin/pi".to_owned(),
                writable: false,
            },
            VerifiedBindMount {
                source: fs::canonicalize(&setsid).unwrap(),
                target: "/usr/local/bin/setsid".to_owned(),
                writable: false,
            },
        ];
        mounts.sort();
        let capability = VerifiedAgentContainer {
            container_id,
            daemon_id,
            labels: labels.clone(),
            mounts,
        };
        Some(Self {
            root,
            container,
            execution_id,
            receipt,
            verifier,
            capability,
            removed: false,
        })
    }

    fn harness(&self) -> PiHarness {
        let mut config = PiHarnessConfig::for_ready_allocation(
            self.verifier.clone(),
            self.receipt.clone(),
            self.capability.clone(),
        )
        .unwrap();
        config.docker_binary = PathBuf::from("docker");
        config.pi_executable = "/usr/local/bin/pi".to_owned();
        config.model_policy = Some(ModelPolicy {
            provider: "inferweave".to_owned(),
            preferred: vec!["qwen/code".to_owned()],
            alternatives: vec!["qwen/fallback".to_owned()],
            fallback_class: Some("coding".to_owned()),
        });
        config.tools = vec!["read".to_owned(), "bash".to_owned(), "edit".to_owned()];
        config.skills = vec![];
        config.stop_timeout = Duration::from_millis(250);
        PiHarness::new(config)
    }

    fn session_dir(&self) -> PathBuf {
        self.receipt.mount_path.join("session")
    }

    fn conversation_dir(&self) -> PathBuf {
        self.session_dir().join("conversation")
    }

    fn worktree_dir(&self) -> PathBuf {
        self.receipt.mount_path.join("repository")
    }

    fn set_event_phases(&self, first: &str, second: &str) {
        fs::write(self.worktree_dir().join("pi-json-events.jsonl"), first).unwrap();
        let second_path = self.worktree_dir().join("pi-json-events-2.jsonl");
        if second.is_empty() {
            let _ = fs::remove_file(second_path);
        } else {
            fs::write(second_path, second).unwrap();
        }
    }

    fn delayed_docker_wrapper(&self) -> PathBuf {
        let docker = Command::new("sh")
            .args(["-c", "command -v docker"])
            .output()
            .unwrap();
        assert!(docker.status.success());
        let docker = String::from_utf8(docker.stdout).unwrap();
        let wrapper = self.root.path().join("delayed-docker");
        fs::write(
            &wrapper,
            format!(
                r#"#!/bin/sh
real_docker={docker:?}
case "$1 $2 $3 $4" in
  "exec --interactive --env AUTOSPEC_SUPERVISOR_TOKEN="*)
    sleep 0.4
    exec "$real_docker" "$@"
    ;;
  *) exec "$real_docker" "$@" ;;
esac
"#,
                docker = docker.trim()
            ),
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        wrapper
    }

    fn ack_dropping_docker_proxy(&self) -> PathBuf {
        let docker = Command::new("sh")
            .args(["-c", "command -v docker"])
            .output()
            .unwrap();
        assert!(docker.status.success());
        let docker = String::from_utf8(docker.stdout).unwrap();
        let wrapper = self.root.path().join("ack-dropping-docker");
        fs::write(
            &wrapper,
            format!(
                r#"#!/bin/sh
real_docker={docker:?}
case "$1 $2 $3 $4" in
  "exec --interactive --env AUTOSPEC_SUPERVISOR_TOKEN="*)
    (IFS= read -r dropped; sleep 5) >/dev/null 2>&1 &
    "$real_docker" "$@" </dev/null
    status=$?
    wait
    exit "$status"
    ;;
  *) exec "$real_docker" "$@" ;;
esac
"#,
                docker = docker.trim()
            ),
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        wrapper
    }

    fn controllable_cleanup_docker_proxy(&self, blocked: &Path) -> PathBuf {
        let docker = Command::new("sh")
            .args(["-c", "command -v docker"])
            .output()
            .unwrap();
        assert!(docker.status.success());
        let docker = String::from_utf8(docker.stdout).unwrap();
        let wrapper = self.root.path().join("cleanup-controlled-docker");
        fs::write(
            &wrapper,
            format!(
                r#"#!/bin/sh
real_docker={docker:?}
blocked={blocked:?}
if [ "$1 $2" = "exec --user" ] && [ -f "$blocked" ]; then
  exit 86
fi
exec "$real_docker" "$@"
"#,
                docker = docker.trim(),
                blocked = blocked.display().to_string()
            ),
        )
        .unwrap();
        fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        wrapper
    }

    fn remove_container(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .status();
        self.removed = true;
    }
}

impl Drop for DockerPi {
    fn drop(&mut self) {
        if !self.removed {
            let _ = Command::new("docker")
                .args(["rm", "-f", &self.container])
                .status();
        }
    }
}

fn labels(execution_id: &str) -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new(execution_id),
        worker_id: WorkerId::new("docker-test-worker"),
        repository: "owner/repo".to_owned(),
        issue: Some("15".to_owned()),
    }
}

async fn wait_for_content(path: &Path, needle: &str) -> String {
    for _ in 0..500 {
        if let Ok(contents) = fs::read_to_string(path) {
            if contents.contains(needle) {
                return contents;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {needle:?} in {}", path.display());
}

async fn wait_for_length_greater_than(path: &Path, previous: u64) {
    for _ in 0..200 {
        if fs::metadata(path).is_ok_and(|metadata| metadata.len() > previous) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "timed out waiting for {} to grow past {previous} bytes",
        path.display()
    );
}

fn pi_is_alive(fixture: &DockerPi) -> bool {
    Command::new("docker")
        .args(["top", &fixture.container, "-eo", "pid,pgid,args"])
        .output()
        .is_ok_and(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("/usr/local/bin/pi")
        })
}

async fn wait_for_pi_pgid(fixture: &DockerPi) -> u32 {
    let path = fixture.conversation_dir().join("test-pgid");
    for _ in 0..500 {
        if let Ok(contents) = fs::read_to_string(&path) {
            if let Ok(pgid) = contents.trim().parse() {
                return pgid;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for Pi PGID in {}", path.display());
}

fn group_states(fixture: &DockerPi, pgid: u32) -> Vec<String> {
    let output = Command::new("docker")
        .args([
            "exec",
            "--user",
            "root",
            &fixture.container,
            "/bin/sh",
            "-c",
            r#"target=$1
for stat_file in /proc/[0-9]*/stat; do
  stat=$(cat "$stat_file" 2>/dev/null) || continue
  rest=${stat##*) }
  set -- $rest
  [ "$3" = "$target" ] && printf '%s\n' "$1"
done
exit 0"#,
            "test-group-states",
            &pgid.to_string(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn assert_zombie_only_group(fixture: &DockerPi, pgid: u32) {
    let states = group_states(fixture, pgid);
    assert!(!states.is_empty(), "expected unreaped zombie group {pgid}");
    assert!(
        states.iter().all(|state| state == "Z"),
        "group {pgid} still has runnable members: {states:?}"
    );
}

#[tokio::test]
async fn stale_or_forged_receipt_is_rejected_before_harness_writes() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let mut forged = fixture.receipt.clone();
    forged.backend_key = "forged-backend".to_owned();
    let config = PiHarnessConfig::for_ready_allocation(
        fixture.verifier.clone(),
        forged,
        fixture.capability.clone(),
    )
    .unwrap();
    let result = PiHarness::new(config).start(&packet()).await;
    assert!(matches!(result, Err(HarnessError::Start(_))));
    assert!(!fixture
        .receipt
        .mount_path
        .join("repository/.autospec/task-packet.json")
        .exists());
    assert!(!fixture.session_dir().join("owner.json").exists());
    assert!(!fixture.session_dir().join(".cursor").exists());
    assert!(!fixture
        .receipt
        .mount_path
        .join("repository/pi-body-started")
        .exists());
}

#[tokio::test]
async fn foreign_container_id_is_rejected_before_harness_writes_or_pi_execution() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let foreign_name = format!("{}-foreign", fixture.container);
    let created = Command::new("docker")
        .args(["create", "--name", &foreign_name])
        .args([
            "--mount",
            &format!(
                "type=bind,src={},dst=/workspace",
                fixture.worktree_dir().display()
            ),
        ])
        .args([
            "--mount",
            &format!(
                "type=bind,src={},dst=/session",
                fixture.conversation_dir().display()
            ),
        ])
        .args([
            "--mount",
            &format!(
                "type=bind,src={},dst=/usr/local/bin/pi,readonly",
                fixture.receipt.mount_path.join("runtime/pi").display()
            ),
        ])
        .args([
            "--mount",
            &format!(
                "type=bind,src={},dst=/usr/local/bin/setsid,readonly",
                fixture.receipt.mount_path.join("runtime/setsid").display()
            ),
        ])
        .args([IMAGE, "sleep", "infinity"])
        .output()
        .unwrap();
    assert!(created.status.success());
    let foreign_id = String::from_utf8(created.stdout).unwrap().trim().to_owned();
    assert!(Command::new("docker")
        .args(["start", &foreign_id])
        .status()
        .unwrap()
        .success());
    let mut capability = fixture.capability.clone();
    capability.container_id = foreign_id.clone();
    let config = PiHarnessConfig::for_ready_allocation(
        fixture.verifier.clone(),
        fixture.receipt.clone(),
        capability,
    )
    .unwrap();
    let result = PiHarness::new(config).start(&packet()).await;
    let _ = Command::new("docker")
        .args(["rm", "-f", &foreign_id])
        .status();
    let error = result.expect_err("foreign container must fail closed");
    assert!(
        matches!(&error, HarnessError::Start(message) if message.contains("live container ownership")),
        "unexpected foreign-container result: {error:?}"
    );
    assert!(!fixture
        .worktree_dir()
        .join(".autospec/task-packet.json")
        .exists());
    assert!(!fixture.session_dir().join("owner.json").exists());
    assert!(!fixture.session_dir().join(".cursor").exists());
    assert!(!fixture.session_dir().join("resume-count").exists());
    assert!(!fixture.worktree_dir().join("pi-body-started").exists());
}

#[tokio::test]
async fn legacy_arbitrary_paths_fail_closed_before_filesystem_mutation() {
    let root = TempDir::new().unwrap();
    let state_root = root.path().join("arbitrary-state");
    let worktree = root.path().join("arbitrary-worktree");
    let config = PiHarnessConfig::for_execution(
        state_root.clone(),
        worktree.clone(),
        "not-started".to_owned(),
        labels("legacy-path-test"),
    );
    let result = PiHarness::new(config).start(&packet()).await;
    assert!(matches!(result, Err(HarnessError::Start(_))));
    assert!(!state_root.exists());
    assert!(!worktree.exists());
}

#[tokio::test]
async fn ready_lease_blocks_release_until_the_live_pi_session_stops() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    assert_eq!(fixture.verifier.active_leases(), 1);

    let verifier = Arc::clone(&fixture.verifier);
    let (released_tx, released_rx) = std::sync::mpsc::channel();
    let release = std::thread::spawn(move || {
        verifier.wait_for_release();
        released_tx.send(()).unwrap();
    });
    assert!(released_rx
        .recv_timeout(Duration::from_millis(200))
        .is_err());
    harness.stop(&session).await.unwrap();
    released_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    release.join().unwrap();
    assert_eq!(fixture.verifier.active_leases(), 0);
}

#[tokio::test]
async fn failed_drop_quarantines_cleanup_authority_and_lease_until_confirmed_reap() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("hung-descendant"), "").unwrap();
    let blocked = fixture.root.path().join("block-cleanup");
    let mut config = fixture.harness().config().clone();
    config.docker_binary = fixture.controllable_cleanup_docker_proxy(&blocked);
    let harness = PiHarness::new(config);
    harness.start(&packet()).await.unwrap();
    wait_for_content(&fixture.worktree_dir().join("pi-body-started"), "started").await;
    let _ = wait_for_pi_pgid(&fixture).await;
    fs::write(&blocked, "").unwrap();
    assert_eq!(fixture.verifier.active_leases(), 1);
    assert!(!Command::new(harness.config().docker_binary.clone())
        .args([
            "exec",
            "--user",
            "root",
            &fixture.capability.container_id,
            "true"
        ])
        .status()
        .unwrap()
        .success());
    drop(harness);
    assert_eq!(fixture.verifier.active_leases(), 1);
    assert!(fixture
        .session_dir()
        .read_dir()
        .unwrap()
        .flatten()
        .any(|entry| entry
            .file_name()
            .to_string_lossy()
            .starts_with("quarantine-")));
    fs::remove_file(blocked).unwrap();
    let verifier = Arc::clone(&fixture.verifier);
    let (released_tx, released_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        verifier.wait_for_release();
        let _ = released_tx.send(());
    });
    released_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(!pi_is_alive(&fixture));
}

#[tokio::test]
async fn failed_start_quarantines_cleanup_authority_and_lease_until_confirmed_reap() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("handshake-invalid"), "").unwrap();
    fs::write(fixture.worktree_dir().join("hung-descendant"), "").unwrap();
    let blocked = fixture.root.path().join("block-startup-cleanup");
    fs::write(&blocked, "").unwrap();
    let mut config = fixture.harness().config().clone();
    config.docker_binary = fixture.controllable_cleanup_docker_proxy(&blocked);
    let harness = PiHarness::new(config);
    assert!(matches!(
        harness.start(&packet()).await,
        Err(HarnessError::Start(_))
    ));
    assert_eq!(fixture.verifier.active_leases(), 1);
    fs::remove_file(blocked).unwrap();
    let verifier = Arc::clone(&fixture.verifier);
    let (released_tx, released_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        verifier.wait_for_release();
        let _ = released_tx.send(());
    });
    released_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(!pi_is_alive(&fixture));
}

#[tokio::test]
async fn harness_state_and_task_packet_stay_inside_the_verified_allocation() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let layout = ExecutionLayout::new(
        fixture
            .receipt
            .mount_path
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
        &fixture.receipt.labels.execution_id,
    )
    .unwrap();
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    wait_for_content(
        &fixture
            .session_dir()
            .join(format!("pi.events-{}.jsonl", fixture.execution_id)),
        "agent_settled",
    )
    .await;
    assert_eq!(Path::new(&session.path), layout.session);
    assert_eq!(Path::new(&session.worktree_path), layout.repository);
    for path in [
        layout.repository.join(".autospec/task-packet.json"),
        layout.session.join("owner.json"),
        layout.session.join(".cursor"),
        layout.session.join("resume-count"),
        layout
            .session
            .join(format!("pi.events-{}.jsonl", fixture.execution_id)),
        layout
            .session
            .join(format!("pi.stderr-{}.log", fixture.execution_id)),
        layout
            .conversation
            .join(format!("session_{}.jsonl", fixture.execution_id)),
    ] {
        assert!(path.is_file(), "missing harness file {}", path.display());
        assert!(path.starts_with(&layout.root));
    }
    assert!(!fixture.root.path().join("state/sessions").exists());
    {
        let verified = fixture.verifier.verified_directories.lock().unwrap();
        assert!(verified.contains(&layout.repository));
        assert!(verified.contains(&layout.session));
        assert!(verified.contains(&layout.conversation));
    }
    harness.stop(&session).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_conversation_is_rejected_before_harness_writes() {
    use std::os::unix::fs::symlink;

    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let displaced = fixture.receipt.mount_path.join("conversation-displaced");
    fs::rename(fixture.conversation_dir(), &displaced).unwrap();
    symlink(&displaced, fixture.conversation_dir()).unwrap();
    let result = fixture.harness().start(&packet()).await;
    assert!(matches!(result, Err(HarnessError::Start(_))));
    assert!(!fixture.session_dir().join("owner.json").exists());
    assert!(!fixture
        .receipt
        .mount_path
        .join("repository/.autospec/task-packet.json")
        .exists());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_live_event_file_cannot_escape_private_session() {
    use std::os::unix::fs::symlink;

    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let outside = fixture.root.path().join("outside-events");
    fs::write(&outside, "sentinel\n").unwrap();
    let events = fixture
        .session_dir()
        .join(format!("pi.events-{}.jsonl", fixture.execution_id));
    symlink(&outside, &events).unwrap();
    let result = fixture.harness().start(&packet()).await;
    assert!(matches!(result, Err(HarnessError::Start(_))));
    assert_eq!(fs::read_to_string(outside).unwrap(), "sentinel\n");
    assert!(!fixture.worktree_dir().join("pi-body-started").exists());
}

#[tokio::test]
async fn docker_exec_uses_only_container_paths_and_carries_no_model_selection_flags() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let args = wait_for_content(
        &fixture.conversation_dir().join("arguments"),
        &format!("--session-id {}", fixture.execution_id),
    )
    .await;
    assert!(args.contains("--session-dir /session"));
    assert!(args.contains("--skill /workspace/role.md"));
    assert!(args.contains("@/workspace/.autospec/task-packet.json"));
    assert!(!args.contains(fixture.root.path().to_string_lossy().as_ref()));
    assert!(!args.contains("--provider"));
    assert!(!args.contains("--model"));
    assert!(!args.contains("--models"));
    let owner: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.session_dir().join("owner.json")).unwrap())
            .unwrap();
    assert_eq!(owner["model_policy"]["provider"], "inferweave");
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn live_event_stdout_is_incremental_and_distinct_from_durable_conversation_jsonl() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let events_path = fixture
        .session_dir()
        .join(format!("pi.events-{}.jsonl", fixture.execution_id));
    wait_for_content(&events_path, "future_pi_record").await;
    let first = harness.poll_events(&session).await.unwrap();
    assert_eq!(first.len(), 2);
    assert!(matches!(
        first[0].kind,
        ExecutionEventKind::AgentStarted { .. }
    ));
    assert!(matches!(first[1].kind, ExecutionEventKind::ReviewReady));
    assert_eq!(harness.unknown_event_count(), 2);
    let live_jsonl = fs::read_to_string(&events_path).unwrap();
    assert!(live_jsonl.starts_with("{\"type\":\"session\""));
    assert!(!live_jsonl.contains("autospec_control"));
    assert!(harness.poll_events(&session).await.unwrap().is_empty());
    write!(
        fs::OpenOptions::new()
            .append(true)
            .open(&events_path)
            .unwrap(),
        "{{\"type\":\"model_error\"}}"
    )
    .unwrap();
    assert!(harness.poll_events(&session).await.unwrap().is_empty());
    writeln!(fs::OpenOptions::new()
        .append(true)
        .open(&events_path)
        .unwrap())
    .unwrap();
    let final_events = harness.poll_events(&session).await.unwrap();
    assert!(matches!(
        final_events[0].kind,
        ExecutionEventKind::ExecutionFailed {
            failure: FailureClass::ModelFailed
        }
    ));
    let durable = fs::read_to_string(
        fixture
            .conversation_dir()
            .join(format!("session_{}.jsonl", fixture.execution_id)),
    )
    .unwrap();
    assert!(durable.contains("\"type\":\"session\""));
    assert!(!durable.contains("turn_start"));
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn cursor_rejects_any_nonempty_path_other_than_the_exact_live_event_file() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    harness.stop(&session).await.unwrap();
    let external = fixture.root.path().join("external-events.jsonl");
    fs::write(&external, "{\"type\":\"agent_start\"}\n").unwrap();
    fs::write(
        fixture.session_dir().join(".cursor"),
        serde_json::to_vec(&serde_json::json!({
            "sessions": {
                fixture.execution_id.clone(): {
                    "path": external,
                    "offset": 0
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        harness.poll_events(&session).await,
        Err(HarnessError::InvalidSession(_))
    ));
}

#[tokio::test]
async fn retrying_backend_error_emits_only_start_then_review_ready_across_restart() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fixture.set_event_phases(
        include_str!("fixtures/retry-success-1.jsonl"),
        include_str!("fixtures/retry-success-2.jsonl"),
    );
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let events_path = fixture
        .session_dir()
        .join(format!("pi.events-{}.jsonl", fixture.execution_id));
    wait_for_content(&events_path, "auto_retry_start").await;
    let first = harness.poll_events(&session).await.unwrap();
    assert_eq!(first.len(), 1);
    assert!(matches!(
        first[0].kind,
        ExecutionEventKind::AgentStarted { .. }
    ));

    let restarted = PiHarness::new(harness.config().clone());
    fs::write(fixture.worktree_dir().join("continue-events"), "").unwrap();
    wait_for_content(&events_path, "recovered").await;
    let terminal = restarted.poll_events(&session).await.unwrap();
    assert_eq!(terminal.len(), 1);
    assert!(matches!(terminal[0].kind, ExecutionEventKind::ReviewReady));
    assert_eq!(restarted.unknown_event_count(), 0);
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn exhausted_retry_emits_one_model_failure_and_never_review_ready() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fixture.set_event_phases(
        include_str!("fixtures/retry-failure-1.jsonl"),
        include_str!("fixtures/retry-failure-2.jsonl"),
    );
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let events_path = fixture
        .session_dir()
        .join(format!("pi.events-{}.jsonl", fixture.execution_id));
    wait_for_content(&events_path, "auto_retry_start").await;
    let first = harness.poll_events(&session).await.unwrap();
    assert_eq!(first.len(), 1);
    assert!(matches!(
        first[0].kind,
        ExecutionEventKind::AgentStarted { .. }
    ));

    let restarted = PiHarness::new(harness.config().clone());
    fs::write(fixture.worktree_dir().join("continue-events"), "").unwrap();
    wait_for_content(&events_path, "finalError").await;
    let terminal = restarted.poll_events(&session).await.unwrap();
    assert_eq!(terminal.len(), 1);
    assert!(matches!(
        terminal[0].kind,
        ExecutionEventKind::ExecutionFailed {
            failure: FailureClass::ModelFailed
        }
    ));
    assert!(!terminal
        .iter()
        .any(|event| matches!(event.kind, ExecutionEventKind::ReviewReady)));
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn pi_writable_conversation_cannot_forge_host_private_harness_state() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("forge-metadata"), "").unwrap();
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let events_path = fixture
        .session_dir()
        .join(format!("pi.events-{}.jsonl", fixture.execution_id));
    wait_for_content(&events_path, "future_pi_record").await;
    wait_for_content(&fixture.conversation_dir().join("owner.json"), "forged").await;
    wait_for_content(&fixture.conversation_dir().join("resume-count"), "999").await;
    let events = harness.poll_events(&session).await.unwrap();
    assert_eq!(events.len(), 2);
    assert!(!events
        .iter()
        .any(|event| matches!(event.kind, ExecutionEventKind::ExecutionFailed { .. })));
    let owner: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.session_dir().join("owner.json")).unwrap())
            .unwrap();
    assert_eq!(owner["execution_id"], fixture.execution_id);
    assert_eq!(
        fs::read_to_string(fixture.session_dir().join("resume-count")).unwrap(),
        "0\n"
    );
    let cursor: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.session_dir().join(".cursor")).unwrap()).unwrap();
    assert!(cursor["sessions"][&fixture.execution_id]["offset"]
        .as_u64()
        .is_some_and(|offset| offset > 0));
    assert_eq!(
        fs::read_to_string(fixture.conversation_dir().join("resume-count")).unwrap(),
        "999\n"
    );
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn invalid_supervisor_header_reaps_started_pi_and_descendant() {
    startup_handshake_failure_reaps("handshake-invalid").await;
}

#[tokio::test]
async fn supervisor_header_reader_failure_reaps_started_pi_and_descendant() {
    startup_handshake_failure_reaps("handshake-read-error").await;
}

#[tokio::test]
async fn supervisor_header_timeout_reaps_started_pi_and_descendant() {
    startup_handshake_failure_reaps("handshake-timeout").await;
}

#[tokio::test]
async fn delayed_reader_failure_cannot_release_a_pi_body() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("hung-descendant"), "").unwrap();
    fs::write(fixture.worktree_dir().join("handshake-read-error"), "").unwrap();
    let mut config = fixture.harness().config().clone();
    config.docker_binary = fixture.delayed_docker_wrapper();
    let harness = PiHarness::new(config);
    let started = Instant::now();
    let result = harness.start(&packet()).await;
    assert!(matches!(result.unwrap_err(), HarnessError::Start(_)));
    assert!(started.elapsed() >= Duration::from_millis(400));
    assert!(started.elapsed() < Duration::from_secs(20));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !fixture.worktree_dir().join("pi-body-started").exists(),
        "Pi body executed after host startup failure"
    );
    assert!(
        !pi_is_alive(&fixture),
        "delayed Pi survived cleanup: {}",
        String::from_utf8_lossy(
            &Command::new("docker")
                .args(["top", &fixture.container, "-eo", "pid,pgid,stat,args"])
                .output()
                .unwrap()
                .stdout
        )
    );
}

#[tokio::test]
async fn docker_proxy_that_drops_ack_cannot_release_a_pi_body() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let mut config = fixture.harness().config().clone();
    config.docker_binary = fixture.ack_dropping_docker_proxy();
    let harness = PiHarness::new(config);
    let started = Instant::now();
    let result = harness.start(&packet()).await;
    assert!(matches!(result, Err(HarnessError::Start(_))));
    assert!(started.elapsed() < Duration::from_secs(20));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !fixture.worktree_dir().join("pi-body-started").exists(),
        "Pi body executed without a READY confirmation"
    );
    assert!(!pi_is_alive(&fixture));
}

#[tokio::test]
async fn malformed_ready_confirmation_reaps_supervisor_without_running_pi() {
    startup_ready_failure_reaps("handshake-ready-malformed").await;
}

#[tokio::test]
async fn replayed_wrong_token_ready_reaps_supervisor_without_running_pi() {
    startup_ready_failure_reaps("handshake-ready-wrong-token").await;
}

async fn startup_ready_failure_reaps(failure: &str) {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join(failure), "").unwrap();
    let harness = fixture.harness();
    let started = Instant::now();
    let result = harness.start(&packet()).await;
    let pgid = wait_for_pi_pgid(&fixture).await;
    assert!(matches!(result, Err(HarnessError::Start(_))));
    assert!(started.elapsed() < Duration::from_secs(20));
    assert!(
        !fixture.worktree_dir().join("pi-body-started").exists(),
        "Pi body executed after invalid READY confirmation"
    );
    assert!(!pi_is_alive(&fixture));
    assert_zombie_only_group(&fixture, pgid);
}

async fn startup_handshake_failure_reaps(failure: &str) {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join(failure), "").unwrap();
    fs::write(fixture.worktree_dir().join("hung-descendant"), "").unwrap();
    let harness = fixture.harness();
    let started = Instant::now();
    let result = harness.start(&packet()).await;
    let pgid = wait_for_pi_pgid(&fixture).await;
    assert!(matches!(result.unwrap_err(), HarnessError::Start(_)));
    assert!(started.elapsed() < Duration::from_secs(20));
    assert!(!pi_is_alive(&fixture));
    assert_zombie_only_group(&fixture, pgid);
}

#[tokio::test]
async fn durable_session_survives_agent_container_removal() {
    let Some(mut fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    wait_for_content(
        &fixture
            .conversation_dir()
            .join(format!("session_{}.jsonl", fixture.execution_id)),
        "\"type\":\"session\"",
    )
    .await;
    harness.stop(&session).await.unwrap();
    fixture.remove_container();
    assert!(fixture
        .conversation_dir()
        .join(format!("session_{}.jsonl", fixture.execution_id))
        .is_file());
    assert!(fixture.session_dir().join("owner.json").is_file());
}

#[tokio::test]
async fn empty_session_resumes_via_fresh_packet_without_resetting_event_cursor() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let events_path = fixture
        .session_dir()
        .join(format!("pi.events-{}.jsonl", fixture.execution_id));
    wait_for_content(&events_path, "future_pi_record").await;
    assert_eq!(harness.poll_events(&session).await.unwrap().len(), 2);
    harness.stop(&session).await.unwrap();
    fs::write(
        fixture
            .conversation_dir()
            .join(format!("session_{}.jsonl", fixture.execution_id)),
        "",
    )
    .unwrap();
    let delivered_length = fs::metadata(&events_path).unwrap().len();
    harness.resume(&session).await.unwrap();
    let args = wait_for_content(
        &fixture.conversation_dir().join("arguments"),
        "@/workspace/.autospec/task-packet.json",
    )
    .await;
    assert!(args.contains(&format!("--session-id {}", fixture.execution_id)));
    assert!(!args.contains("Continue from the persisted session"));
    wait_for_length_greater_than(&events_path, delivered_length).await;
    assert_eq!(harness.poll_events(&session).await.unwrap().len(), 2);
    assert!(harness.poll_events(&session).await.unwrap().is_empty());
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn resume_rejects_malformed_complete_record_but_truncates_only_a_torn_tail() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let durable = fixture
        .conversation_dir()
        .join(format!("session_{}.jsonl", fixture.execution_id));
    wait_for_content(&durable, "\"type\":\"session\"").await;
    harness.stop(&session).await.unwrap();
    fs::write(&durable, "{not-json}\n").unwrap();
    assert!(matches!(
        harness.resume(&session).await.unwrap_err(),
        HarnessError::NotResumable(_)
    ));
    fs::write(
        &durable,
        format!(
            "{{\"type\":\"session\",\"id\":\"{}\"}}\n{{\"type\":\"message\"",
            fixture.execution_id
        ),
    )
    .unwrap();
    harness.resume(&session).await.unwrap();
    assert_eq!(
        fs::read_to_string(&durable).unwrap(),
        format!(
            "{{\"type\":\"session\",\"id\":\"{}\"}}\n",
            fixture.execution_id
        )
    );
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn stop_and_duplicate_terminate_the_in_container_process_group() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("hung-descendant"), "").unwrap();
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let first_pgid = wait_for_pi_pgid(&fixture).await;
    wait_for_content(&fixture.conversation_dir().join("arguments"), "--mode json").await;
    assert!(pi_is_alive(&fixture));
    assert!(matches!(
        harness.start(&packet()).await.unwrap_err(),
        HarnessError::Start(_)
    ));
    assert!(pi_is_alive(&fixture));
    fs::write(fixture.conversation_dir().join("ignore-term"), "").unwrap();
    let started = Instant::now();
    harness.stop(&session).await.unwrap();
    assert!(started.elapsed() <= Duration::from_secs(5));
    assert!(!pi_is_alive(&fixture));
    assert_zombie_only_group(&fixture, first_pgid);
}

#[tokio::test]
async fn pi_cannot_replace_supervisor_control_state() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("forge-control"), "").unwrap();
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    wait_for_content(
        &fixture.conversation_dir().join("pi-process-forged"),
        "999999",
    )
    .await;
    harness.stop(&session).await.unwrap();
    assert!(!pi_is_alive(&fixture));
    assert!(!fixture.session_dir().join(".pi-launch").exists());
    assert_eq!(
        fs::read_to_string(fixture.conversation_dir().join(".pi-control")).unwrap(),
        "#!/bin/sh\nexit 0\n"
    );
    assert_eq!(
        fs::read_to_string(
            fixture
                .conversation_dir()
                .join(format!("pi-process-{}", fixture.execution_id))
        )
        .unwrap(),
        "{\"pgid\":999999}\n"
    );
}

#[tokio::test]
async fn drop_is_bounded_when_a_descendant_ignores_term_and_pi_forges_control_files() {
    let Some(mut fixture) = DockerPi::create() else {
        return;
    };
    fs::write(fixture.worktree_dir().join("forge-control"), "").unwrap();
    fs::write(fixture.worktree_dir().join("hung-descendant"), "").unwrap();
    let harness = fixture.harness();
    harness.start(&packet()).await.unwrap();
    let pgid = wait_for_pi_pgid(&fixture).await;
    wait_for_content(
        &fixture.conversation_dir().join("pi-process-forged"),
        "999999",
    )
    .await;

    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let drop_thread = std::thread::spawn(move || {
        drop(harness);
        let _ = finished_tx.send(());
    });
    let bounded = finished_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    if !bounded {
        fixture.remove_container();
    }
    drop_thread.join().unwrap();
    assert!(
        bounded,
        "ProcessRegistry::drop exceeded its five-second bound"
    );
    assert!(!pi_is_alive(&fixture));
    assert_zombie_only_group(&fixture, pgid);
}

#[tokio::test]
async fn missing_docker_binary_is_not_installed() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let mut config = fixture.harness().config().clone();
    config.docker_binary = fixture.root.path().join("missing-docker");
    assert!(matches!(
        PiHarness::new(config).start(&packet()).await.unwrap_err(),
        HarnessError::NotInstalled(_)
    ));
}

#[tokio::test]
async fn missing_pi_inside_the_agent_container_is_not_installed() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let mut config = fixture.harness().config().clone();
    config.pi_executable = "/usr/local/bin/missing-pi".to_owned();
    assert!(matches!(
        PiHarness::new(config).start(&packet()).await.unwrap_err(),
        HarnessError::NotInstalled(_)
    ));
}

const STUB_PI: &str = r#"#!/bin/sh
session_dir=
session_id=
source_session=
all_args=$*
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir) session_dir=$2; shift 2 ;;
    --session-id) session_id=$2; shift 2 ;;
    --session|--fork) source_session=$2; shift 2 ;;
    *) shift ;;
  esac
done
printf 'started\n' > /workspace/pi-body-started
stat=$(cat /proc/$$/stat)
rest=${stat##*) }
set -- $rest
printf '%s\n' "$3" > "$session_dir/test-pgid"
printf '%s\n' "$all_args" > "$session_dir/arguments"
if [ -z "$session_id" ] && [ -n "$source_session" ]; then
  session_id=$(sed -n '1s/.*"id":"\([^"]*\)".*/\1/p' "$source_session")
fi
session_file="$session_dir/session_${session_id}.jsonl"
if [ ! -s "$session_file" ]; then
  printf '{"type":"session","version":3,"id":"%s","timestamp":"2026-08-28T00:00:00Z","cwd":"/workspace"}\n' "$session_id" > "$session_file"
fi
cat /workspace/pi-json-events.jsonl
if [ -f /workspace/pi-json-events-2.jsonl ]; then
  while [ ! -f /workspace/continue-events ]; do sleep 0.01; done
  cat /workspace/pi-json-events-2.jsonl
fi
if [ -f /workspace/forge-metadata ]; then
  printf '{"execution_id":"forged"}\n' > "$session_dir/owner.json"
  printf '{"sessions":{"forged":{"path":"/session/forged.jsonl","offset":0}}}\n' > "$session_dir/.cursor"
  printf '999\n' > "$session_dir/resume-count"
  printf '{"type":"model_error"}\n' > "$session_dir/pi.events-${session_id}.jsonl"
fi
if [ -f /workspace/forge-control ]; then
  printf '#!/bin/sh\nexit 0\n' > "$session_dir/.pi-control"
  chmod 755 "$session_dir/.pi-control"
  printf '{"pgid":999999}\n' > "$session_dir/pi-process-${session_id}"
  printf '999999\n' > "$session_dir/pi-process-forged"
fi
if [ -f /workspace/hung-descendant ]; then
  (trap '' TERM; while :; do sleep 0.05; done) &
fi
trap 'if [ -f "$session_dir/ignore-term" ]; then :; else exit 0; fi' TERM
while :; do sleep 0.05; done
"#;

const STUB_SETSID: &str = r#"#!/bin/sh
if [ -f /workspace/handshake-ready-malformed ] || [ -f /workspace/handshake-ready-wrong-token ]; then
  token=$5
  mode=malformed
  [ -f /workspace/handshake-ready-wrong-token ] && mode=wrong-token
  shift 5
  exec /usr/bin/setsid /bin/sh -c '
    token=$1; mode=$2
    stat=$(cat /proc/$$/stat); rest=${stat##*) }; set -- $rest; printf "%s\n" "$3" > /session/test-pgid
    printf "{\"type\":\"autospec_control\",\"token\":\"%s\",\"pgid\":%s}\n" "$token" "$3"
    IFS= read -r ack || exit 125
    [ "$ack" = "ACK $token" ] || exit 125
    if [ "$mode" = malformed ]; then
      printf "not-ready\n"
    else
      printf "{\"type\":\"autospec_ready\",\"token\":\"replayed-token\"}\n"
    fi
    IFS= read -r ignored || exit 125
    exit 125
  ' injected "$token" "$mode" "$@"
fi
if [ -f /workspace/handshake-invalid ]; then
  shift 5
  exec /usr/bin/setsid /bin/sh -c '
    stat=$(cat /proc/$$/stat); rest=${stat##*) }; set -- $rest; printf "%s\n" "$3" > /session/test-pgid
    (trap "" TERM; while :; do sleep 0.05; done) &
    printf "not-json\n"
    IFS= read -r ignored || exit 125
    exit 125
  ' injected "$@"
fi
if [ -f /workspace/handshake-read-error ]; then
  shift 5
  exec /usr/bin/setsid /bin/sh -c '
    stat=$(cat /proc/$$/stat); rest=${stat##*) }; set -- $rest; printf "%s\n" "$3" > /session/test-pgid
    (trap "" TERM; while :; do sleep 0.05; done) &
    printf "\377\n"
    IFS= read -r ignored || exit 125
    exit 125
  ' injected "$@"
fi
if [ -f /workspace/handshake-timeout ]; then
  shift 5
  exec /usr/bin/setsid /bin/sh -c '
    stat=$(cat /proc/$$/stat); rest=${stat##*) }; set -- $rest; printf "%s\n" "$3" > /session/test-pgid
    (trap "" TERM; while :; do sleep 0.05; done) &
    IFS= read -r ignored || exit 125
    exit 125
  ' injected "$@"
fi
exec /usr/bin/setsid "$@"
"#;
