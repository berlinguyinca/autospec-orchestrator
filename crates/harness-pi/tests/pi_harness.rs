use harness_pi::{PiHarness, PiHarnessConfig};
use harness_traits::{AgentHarness, HarnessError};
use orchestrator_core::{
    event::ExecutionEventKind, ExecutionId, FailureClass, ModelPolicy, OwnershipLabels, TaskPacket,
    WorkerId,
};
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
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
    removed: bool,
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
        let worktree = root.path().join("worktree");
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let execution_id = format!("pi-test-{}-{sequence}", std::process::id());
        let session = root.path().join("state/sessions").join(&execution_id);
        fs::create_dir_all(worktree.join(".autospec")).unwrap();
        fs::create_dir_all(&session).unwrap();
        fs::write(worktree.join("AGENTS.md"), "Stay scoped.\n").unwrap();
        fs::write(worktree.join("role.md"), "Implement.\n").unwrap();
        let stub = root.path().join("pi");
        fs::write(&stub, STUB_PI).unwrap();
        fs::set_permissions(&stub, fs::Permissions::from_mode(0o755)).unwrap();

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let container = format!("autospec-pi-test-{}-{nonce}-{sequence}", std::process::id());
        let labels = labels(&execution_id);
        let status = Command::new("docker")
            .args(["create", "--init", "--name", &container])
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
                &format!("type=bind,src={},dst=/session", session.display()),
            ])
            .args([
                "--mount",
                &format!(
                    "type=bind,src={},dst=/usr/local/bin/pi,readonly",
                    stub.display()
                ),
            ])
            .args([IMAGE, "sleep", "infinity"])
            .status()
            .expect("create stub Pi container");
        assert!(status.success(), "create stub Pi container");
        assert!(Command::new("docker")
            .args(["start", &container])
            .status()
            .unwrap()
            .success());
        Some(Self {
            root,
            container,
            execution_id,
            removed: false,
        })
    }

    fn harness(&self) -> PiHarness {
        PiHarness::new(PiHarnessConfig {
            state_root: self.root.path().join("state"),
            worktree: self.root.path().join("worktree"),
            docker_binary: PathBuf::from("docker"),
            agent_container: self.container.clone(),
            pi_executable: "/usr/local/bin/pi".to_owned(),
            labels: labels(&self.execution_id),
            model_policy: Some(ModelPolicy {
                provider: "inferweave".to_owned(),
                preferred: vec!["qwen/code".to_owned()],
                alternatives: vec!["qwen/fallback".to_owned()],
                fallback_class: Some("coding".to_owned()),
            }),
            tools: vec!["read".to_owned(), "bash".to_owned(), "edit".to_owned()],
            skills: vec![],
            stop_timeout: Duration::from_millis(250),
        })
    }

    fn session_dir(&self) -> PathBuf {
        self.root
            .path()
            .join("state/sessions")
            .join(&self.execution_id)
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
    for _ in 0..200 {
        if let Ok(contents) = fs::read_to_string(path) {
            if contents.contains(needle) {
                return contents;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {needle:?} in {}", path.display());
}

fn group_is_alive(fixture: &DockerPi) -> bool {
    Command::new("docker")
        .args([
            "exec",
            &fixture.container,
            "/session/.pi-control",
            "CHECK",
            &format!("/session/pi-process-{}", fixture.execution_id),
        ])
        .status()
        .is_ok_and(|status| status.success())
}

#[tokio::test]
async fn docker_exec_uses_only_container_paths_and_carries_no_model_selection_flags() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    let args = wait_for_content(
        &fixture.session_dir().join("arguments"),
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
    assert!(matches!(first[1].kind, ExecutionEventKind::TestsStarted));
    assert_eq!(harness.unknown_event_count(), 1);
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
            .session_dir()
            .join(format!("session_{}.jsonl", fixture.execution_id)),
    )
    .unwrap();
    assert!(durable.contains("\"type\":\"session\""));
    assert!(!durable.contains("turn_start"));
    harness.stop(&session).await.unwrap();
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
            .session_dir()
            .join(format!("session_{}.jsonl", fixture.execution_id)),
        "\"type\":\"session\"",
    )
    .await;
    harness.stop(&session).await.unwrap();
    fixture.remove_container();
    assert!(fixture
        .session_dir()
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
    wait_for_content(&events_path, "turn_start").await;
    assert_eq!(harness.poll_events(&session).await.unwrap().len(), 2);
    harness.stop(&session).await.unwrap();
    fs::write(
        fixture
            .session_dir()
            .join(format!("session_{}.jsonl", fixture.execution_id)),
        "",
    )
    .unwrap();
    harness.resume(&session).await.unwrap();
    let args = wait_for_content(
        &fixture.session_dir().join("arguments"),
        "@/workspace/.autospec/task-packet.json",
    )
    .await;
    assert!(args.contains(&format!("--session-id {}", fixture.execution_id)));
    assert!(!args.contains("Continue from the persisted session"));
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
        .session_dir()
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
async fn stop_duplicate_and_drop_terminate_the_in_container_process_group() {
    let Some(fixture) = DockerPi::create() else {
        return;
    };
    let harness = fixture.harness();
    let session = harness.start(&packet()).await.unwrap();
    wait_for_content(
        &fixture
            .session_dir()
            .join(format!("pi-process-{}", fixture.execution_id)),
        "pgid",
    )
    .await;
    assert!(group_is_alive(&fixture));
    assert!(matches!(
        harness.start(&packet()).await.unwrap_err(),
        HarnessError::Start(_)
    ));
    assert!(group_is_alive(&fixture));
    fs::write(fixture.session_dir().join("ignore-term"), "").unwrap();
    let started = Instant::now();
    harness.stop(&session).await.unwrap();
    assert!(started.elapsed() <= Duration::from_secs(2));
    assert!(!group_is_alive(&fixture));

    fs::remove_file(fixture.session_dir().join("ignore-term")).unwrap();
    harness.resume(&session).await.unwrap();
    wait_for_content(
        &fixture
            .session_dir()
            .join(format!("pi-process-{}", fixture.execution_id)),
        "pgid",
    )
    .await;
    assert!(group_is_alive(&fixture));
    drop(harness);
    assert!(!group_is_alive(&fixture));
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
printf '%s\n' "$all_args" > "$session_dir/arguments"
if [ -z "$session_id" ] && [ -n "$source_session" ]; then
  session_id=$(sed -n '1s/.*"id":"\([^"]*\)".*/\1/p' "$source_session")
fi
session_file="$session_dir/session_${session_id}.jsonl"
if [ ! -s "$session_file" ]; then
  printf '{"type":"session","version":3,"id":"%s","timestamp":"2026-08-28T00:00:00Z","cwd":"/workspace"}\n' "$session_id" > "$session_file"
fi
printf '{"type":"turn_start"}\n'
printf '{"type":"test_run"}\n'
printf '{"type":"future_pi_record","prompt":"never-log-this"}\n'
trap 'if [ -f "$session_dir/ignore-term" ]; then :; else exit 0; fi' TERM
while :; do sleep 0.05; done
"#;
