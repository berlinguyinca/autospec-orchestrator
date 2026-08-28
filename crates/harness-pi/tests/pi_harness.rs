use harness_pi::{PiHarness, PiHarnessConfig};
use harness_traits::{AgentHarness, HarnessError};
use orchestrator_core::{
    event::ExecutionEventKind, ExecutionId, FailureClass, ModelPolicy, OwnershipLabels, TaskPacket,
    WorkerId,
};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};
use tempfile::TempDir;

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

fn stub_pi(root: &Path) -> std::path::PathBuf {
    let path = root.join("pi");
    fs::write(
        &path,
        r#"#!/bin/sh
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
mkdir -p "$session_dir"
printf '%s\n' "$all_args" > "$session_dir/arguments"
if [ -z "$session_id" ] && [ -n "$source_session" ]; then
  session_id=$(sed -n '1s/.*"id":"\([^"]*\)".*/\1/p' "$source_session")
fi
session_file="$session_dir/stub_${session_id}.jsonl"
if [ ! -f "$session_file" ]; then
  printf '{"type":"session","version":3,"id":"%s","timestamp":"2026-08-28T00:00:00Z","cwd":"%s"}\n' "$session_id" "$PWD" > "$session_file"
fi
trap 'if [ -f "$session_dir/ignore-term" ]; then :; else exit 0; fi' TERM
while :; do sleep 0.05; done
"#,
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    path
}

fn configured(root: &TempDir) -> PiHarness {
    let worktree = root.path().join("worktree");
    fs::create_dir(&worktree).unwrap();
    fs::write(worktree.join("AGENTS.md"), "Stay scoped.\n").unwrap();
    fs::write(worktree.join("role.md"), "Implement.\n").unwrap();
    PiHarness::new(PiHarnessConfig {
        state_root: root.path().join("state"),
        worktree,
        executable: stub_pi(root.path()),
        labels: OwnershipLabels {
            execution_id: ExecutionId::new("repo-15-impl-01"),
            worker_id: WorkerId::new("worker-01"),
            repository: "owner/repo".to_owned(),
            issue: Some("15".to_owned()),
        },
        model_policy: Some(ModelPolicy {
            provider: "inferweave".to_owned(),
            preferred: vec!["qwen/code".to_owned()],
            alternatives: vec![],
            fallback_class: None,
        }),
        tools: vec!["read".to_owned(), "bash".to_owned(), "edit".to_owned()],
        skills: vec![],
        stop_timeout: Duration::from_millis(150),
    })
}

async fn wait_for_content(path: &Path, needle: &str) -> String {
    for _ in 0..100 {
        if let Ok(contents) = fs::read_to_string(path) {
            if contents.contains(needle) {
                return contents;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {needle:?} in {}", path.display());
}

#[tokio::test]
async fn start_serializes_packet_once_and_uses_exact_pi_0843_flags() {
    let root = TempDir::new().unwrap();
    let harness = configured(&root);
    let session = harness.start(&packet()).await.unwrap();

    assert_eq!(session.id.as_str(), "repo-15-impl-01");
    assert_eq!(
        session.path,
        root.path()
            .join("state/sessions/repo-15-impl-01")
            .display()
            .to_string()
    );
    let persisted: serde_json::Value = serde_json::from_slice(
        &fs::read(root.path().join("worktree/.autospec/task-packet.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(persisted, serde_json::to_value(packet()).unwrap());
    let arguments = Path::new(&session.path).join("arguments");
    let args = wait_for_content(&arguments, "--session-id repo-15-impl-01").await;
    assert!(args.contains("--session-dir "));
    assert!(args.contains("--session-id repo-15-impl-01"));
    assert!(args.contains("--tools read,bash,edit"));
    assert!(args.contains("--provider inferweave"));
    assert!(args.contains("--models qwen/code"));
    assert!(!args.contains("--model "));
    assert!(args.contains("--skill "));
    assert!(args.contains("@"));
    assert!(!args.contains("--task-packet"));
    assert!(Path::new(&session.path).join("owner.json").is_file());
    assert!(Path::new(&session.path).join(".cursor").is_file());
    assert!(Path::new(&session.path).join("resume-count").is_file());

    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn missing_executable_is_not_installed() {
    let root = TempDir::new().unwrap();
    let mut config = configured(&root).config().clone();
    config.executable = root.path().join("missing-pi");
    let error = PiHarness::new(config).start(&packet()).await.unwrap_err();
    assert!(matches!(error, HarnessError::NotInstalled(_)));
}

#[tokio::test]
async fn polling_is_incremental_partial_line_safe_and_counts_unknown_records() {
    let root = TempDir::new().unwrap();
    let harness = configured(&root);
    let session = harness.start(&packet()).await.unwrap();
    let session_file = Path::new(&session.path).join("stub_repo-15-impl-01.jsonl");
    fs::write(
        &session_file,
        concat!(
            "{\"type\":\"turn_start\"}\n",
            "{\"type\":\"test_run\"}\n",
            "{\"type\":\"future_pi_record\",\"prompt\":\"secret\"}\n",
            "{\"type\":\"model_error\"}"
        ),
    )
    .unwrap();

    let first = harness.poll_events(&session).await.unwrap();
    assert_eq!(first.len(), 2);
    assert!(first.iter().all(|event| event.sequence == 0));
    assert!(matches!(
        first[0].kind,
        ExecutionEventKind::AgentStarted { .. }
    ));
    assert!(matches!(first[1].kind, ExecutionEventKind::TestsStarted));
    assert_eq!(harness.unknown_event_count(), 1);
    assert!(harness.poll_events(&session).await.unwrap().is_empty());

    use std::io::Write;
    writeln!(fs::OpenOptions::new()
        .append(true)
        .open(&session_file)
        .unwrap())
    .unwrap();
    let final_events = harness.poll_events(&session).await.unwrap();
    assert_eq!(final_events.len(), 1);
    assert!(matches!(
        final_events[0].kind,
        ExecutionEventKind::ExecutionFailed {
            failure: FailureClass::ModelFailed
        }
    ));
    harness.stop(&session).await.unwrap();
}

#[tokio::test]
async fn resume_preserves_cursor_and_fork_reuses_worktree() {
    let root = TempDir::new().unwrap();
    let harness = configured(&root);
    let session = harness.start(&packet()).await.unwrap();
    let session_file = Path::new(&session.path).join("stub_repo-15-impl-01.jsonl");
    fs::write(&session_file, "{\"type\":\"turn_start\"}\n").unwrap();
    assert_eq!(harness.poll_events(&session).await.unwrap().len(), 1);
    harness.stop(&session).await.unwrap();

    harness.resume(&session).await.unwrap();
    assert!(harness.poll_events(&session).await.unwrap().is_empty());
    assert_eq!(
        fs::read_to_string(Path::new(&session.path).join("resume-count")).unwrap(),
        "1\n"
    );
    let arguments = Path::new(&session.path).join("arguments");
    let args = wait_for_content(&arguments, "Continue from the persisted session").await;
    assert!(args.contains("--session "));
    assert!(args.contains("Continue from the persisted session without repeating completed work."));
    assert!(!args.contains("@"));

    let fork = harness.fork_conversation(&session).await.unwrap();
    assert_ne!(fork.id, session.id);
    assert_eq!(fork.worktree_path, session.worktree_path);
    assert_eq!(fork.path, session.path);
    harness.stop(&session).await.unwrap();
    harness.stop(&fork).await.unwrap();
}

#[tokio::test]
async fn fourth_resume_is_rejected_and_stop_kills_term_ignoring_process() {
    let root = TempDir::new().unwrap();
    let harness = configured(&root);
    let session = harness.start(&packet()).await.unwrap();
    fs::write(Path::new(&session.path).join("ignore-term"), "").unwrap();
    harness.stop(&session).await.unwrap();
    for expected in 1..=3 {
        harness.resume(&session).await.unwrap();
        assert_eq!(
            fs::read_to_string(Path::new(&session.path).join("resume-count")).unwrap(),
            format!("{expected}\n")
        );
        harness.stop(&session).await.unwrap();
    }
    let error = harness.resume(&session).await.unwrap_err();
    assert!(matches!(error, HarnessError::NotResumable(_)));
}

#[tokio::test]
async fn resume_rejects_foreign_owner_and_truncates_a_torn_tail() {
    let root = TempDir::new().unwrap();
    let harness = configured(&root);
    let session = harness.start(&packet()).await.unwrap();
    let session_file = Path::new(&session.path).join("stub_repo-15-impl-01.jsonl");
    fs::write(
        &session_file,
        "{\"type\":\"turn_start\"}\n{\"type\":\"message\"",
    )
    .unwrap();
    harness.stop(&session).await.unwrap();
    harness.resume(&session).await.unwrap();
    assert_eq!(
        fs::read_to_string(&session_file).unwrap(),
        "{\"type\":\"turn_start\"}\n"
    );
    harness.stop(&session).await.unwrap();

    let owner_path = Path::new(&session.path).join("owner.json");
    let mut owner: serde_json::Value =
        serde_json::from_slice(&fs::read(&owner_path).unwrap()).unwrap();
    owner["execution_id"] = serde_json::Value::String("foreign-execution".to_owned());
    fs::write(&owner_path, serde_json::to_vec(&owner).unwrap()).unwrap();
    let error = harness.resume(&session).await.unwrap_err();
    assert!(matches!(error, HarnessError::NotResumable(_)));
}
