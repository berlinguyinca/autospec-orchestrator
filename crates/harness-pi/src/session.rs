use crate::PiHarness;
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::{ModelPolicy, SessionId, TaskPacket};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(crate) const OWNER_FILE: &str = "owner.json";
pub(crate) const CURSOR_FILE: &str = ".cursor";
pub(crate) const RESUME_COUNT_FILE: &str = "resume-count";
pub(crate) const CONTAINER_WORKTREE: &str = "/workspace";
pub(crate) const CONTAINER_SESSION: &str = "/session";
const LAUNCHER_FILE: &str = ".pi-launch";
const CONTROL_FILE: &str = ".pi-control";

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SessionOwner {
    pub(crate) execution_id: String,
    pub(crate) session_id: String,
    pub(crate) worktree_path: String,
    pub(crate) labels: std::collections::BTreeMap<String, String>,
    pub(crate) model_policy: Option<ModelPolicy>,
}

pub(crate) fn start(harness: &PiHarness, packet: &TaskPacket) -> Result<SessionRef, HarnessError> {
    ensure_docker_and_pi(harness)?;
    let execution_id = harness.config.labels.execution_id.clone();
    let session_id = SessionId::new(execution_id.to_string());
    let session_dir = harness
        .config
        .state_root
        .join("sessions")
        .join(execution_id.as_str());
    fs::create_dir_all(&session_dir).map_err(io_error)?;
    let packet_path = harness.config.worktree.join(".autospec/task-packet.json");
    write_packet_once(&packet_path, packet)?;
    write_json_once(
        &session_dir.join(OWNER_FILE),
        &SessionOwner {
            execution_id: execution_id.to_string(),
            session_id: session_id.to_string(),
            worktree_path: harness.config.worktree.display().to_string(),
            labels: harness.config.labels.to_map(),
            model_policy: harness.config.model_policy.clone(),
        },
    )?;
    write_once(&session_dir.join(CURSOR_FILE), b"{}\n")?;
    write_once(&session_dir.join(RESUME_COUNT_FILE), b"0\n")?;
    materialize_process_scripts(&session_dir)?;

    let session = SessionRef {
        id: session_id,
        path: session_dir.display().to_string(),
        execution_id,
        worktree_path: harness.config.worktree.display().to_string(),
    };
    let mut args = base_args(harness)?;
    args.extend(["--session-id".into(), session.id.to_string()]);
    args.push("@/workspace/.autospec/task-packet.json".into());
    spawn(harness, &session, args)?;
    Ok(session)
}

pub(crate) fn base_args(harness: &PiHarness) -> Result<Vec<String>, HarnessError> {
    let mut args = vec![
        "--mode".into(),
        "json".into(),
        "--session-dir".into(),
        CONTAINER_SESSION.into(),
        "--no-extensions".into(),
        "--no-prompt-templates".into(),
        "--no-themes".into(),
        "--approve".into(),
    ];
    if harness.config.tools.is_empty() {
        args.push("--no-tools".into());
    } else {
        args.extend(["--tools".into(), harness.config.tools.join(",")]);
    }
    let mut skills = harness.config.skills.clone();
    if let Some(role_skill) = packet_role_skill(&harness.config.worktree) {
        skills.push(resolve_role_skill(&harness.config.worktree, &role_skill)?);
    }
    args.push("--no-skills".into());
    for skill in skills {
        args.extend([
            "--skill".into(),
            container_worktree_path(&harness.config.worktree, &skill)?,
        ]);
    }
    Ok(args)
}

fn packet_role_skill(worktree: &Path) -> Option<String> {
    let bytes = fs::read(worktree.join(".autospec/task-packet.json")).ok()?;
    serde_json::from_slice::<TaskPacket>(&bytes)
        .ok()?
        .role_skill
}

fn resolve_role_skill(worktree: &Path, role_skill: &str) -> Result<PathBuf, HarnessError> {
    let worktree = fs::canonicalize(worktree).map_err(io_error)?;
    let skill = fs::canonicalize(worktree.join(role_skill)).map_err(io_error)?;
    if !skill.starts_with(&worktree) || !skill.is_file() {
        return Err(HarnessError::Start(format!(
            "role skill is outside the worktree: {role_skill}"
        )));
    }
    Ok(skill)
}

fn container_worktree_path(worktree: &Path, host_path: &Path) -> Result<String, HarnessError> {
    let worktree = fs::canonicalize(worktree).map_err(io_error)?;
    let host_path = fs::canonicalize(host_path).map_err(io_error)?;
    let relative = host_path.strip_prefix(&worktree).map_err(|_| {
        HarnessError::Start(format!(
            "skill path is outside the mounted worktree: {}",
            host_path.display()
        ))
    })?;
    Ok(Path::new(CONTAINER_WORKTREE)
        .join(relative)
        .display()
        .to_string())
}

pub(crate) fn spawn(
    harness: &PiHarness,
    session: &SessionRef,
    pi_args: Vec<String>,
) -> Result<(), HarnessError> {
    ensure_docker_and_pi(harness)?;
    let mut children = harness
        .processes
        .children
        .lock()
        .map_err(|_| HarnessError::Start("process registry lock poisoned".to_owned()))?;
    if children.contains_key(session.id.as_str()) {
        return Err(HarnessError::Start(format!(
            "Pi session {} is already running",
            session.id
        )));
    }
    let session_dir = Path::new(&session.path);
    let events = File::options()
        .create(true)
        .append(true)
        .open(live_events_path(session))
        .map_err(io_error)?;
    let stderr = File::options()
        .create(true)
        .append(true)
        .open(session_dir.join(format!("pi.stderr-{}.log", session.id)))
        .map_err(io_error)?;
    let record = container_process_record(session.id.as_str());
    let host_record = host_process_record(session);
    if host_record.exists() {
        fs::remove_file(&host_record).map_err(io_error)?;
    }
    let mut command = Command::new(&harness.config.docker_binary);
    command
        .args(["exec", "--workdir", CONTAINER_WORKTREE])
        .arg(&harness.config.agent_container)
        .args([format!("{CONTAINER_SESSION}/{LAUNCHER_FILE}"), record])
        .arg(&harness.config.pi_executable)
        .args(pi_args)
        .stdin(Stdio::null())
        .stdout(events)
        .stderr(stderr);
    let child = command.spawn().map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            HarnessError::NotInstalled(harness.config.docker_binary.display().to_string())
        } else {
            HarnessError::Start(error.to_string())
        }
    })?;
    children.insert(session.id.to_string(), Arc::new(Mutex::new(child)));
    drop(children);
    wait_for_process_record(harness, session)
}

pub(crate) async fn stop(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    let child = harness
        .processes
        .children
        .lock()
        .map_err(|_| HarnessError::Crashed("process registry lock poisoned".to_owned()))?
        .get(session.id.as_str())
        .cloned();
    let Some(child) = child else {
        return Ok(());
    };
    if process_reaped(harness, session, &child)? {
        remove_registered_child(harness, session, &child)?;
        return Ok(());
    }
    signal_container_group(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        session.id.as_str(),
        "TERM",
    )?;
    let deadline = Instant::now() + harness.config.stop_timeout;
    while Instant::now() < deadline {
        if process_reaped(harness, session, &child)? {
            remove_registered_child(harness, session, &child)?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    signal_container_group(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        session.id.as_str(),
        "KILL",
    )?;
    let kill_deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < kill_deadline {
        if process_reaped(harness, session, &child)? {
            remove_registered_child(harness, session, &child)?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(HarnessError::Crashed(format!(
        "Pi process group for session {} was not reaped",
        session.id
    )))
}

fn process_reaped(
    harness: &PiHarness,
    session: &SessionRef,
    child: &Arc<Mutex<std::process::Child>>,
) -> Result<bool, HarnessError> {
    let child_exited = child
        .lock()
        .map_err(|_| HarnessError::Crashed("Pi child lock poisoned".to_owned()))?
        .try_wait()
        .map_err(io_error)?
        .is_some();
    Ok(child_exited && !container_group_alive(harness, session.id.as_str())?)
}

fn remove_registered_child(
    harness: &PiHarness,
    session: &SessionRef,
    expected: &Arc<Mutex<std::process::Child>>,
) -> Result<(), HarnessError> {
    let mut children = harness
        .processes
        .children
        .lock()
        .map_err(|_| HarnessError::Crashed("process registry lock poisoned".to_owned()))?;
    if children
        .get(session.id.as_str())
        .is_some_and(|registered| Arc::ptr_eq(registered, expected))
    {
        children.remove(session.id.as_str());
        let record = host_process_record(session);
        if record.exists() {
            fs::remove_file(record).map_err(io_error)?;
        }
    }
    Ok(())
}

fn wait_for_process_record(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    let record = host_process_record(session);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if record.is_file() {
            return Ok(());
        }
        let child = harness
            .processes
            .children
            .lock()
            .map_err(|_| HarnessError::Start("process registry lock poisoned".to_owned()))?
            .get(session.id.as_str())
            .cloned();
        if let Some(child) = child {
            if let Some(status) = child
                .lock()
                .map_err(|_| HarnessError::Start("Pi child lock poisoned".to_owned()))?
                .try_wait()
                .map_err(io_error)?
            {
                return Err(HarnessError::Start(format!(
                    "docker exec exited before Pi recorded its process group: {status}"
                )));
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err(HarnessError::Start(
        "timed out waiting for in-container Pi process record".to_owned(),
    ))
}

fn ensure_docker_and_pi(harness: &PiHarness) -> Result<(), HarnessError> {
    let inspect = Command::new(&harness.config.docker_binary)
        .args([
            "inspect",
            "--type",
            "container",
            &harness.config.agent_container,
        ])
        .output()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                HarnessError::NotInstalled(harness.config.docker_binary.display().to_string())
            } else {
                HarnessError::Start(error.to_string())
            }
        })?;
    if !inspect.status.success() {
        return Err(HarnessError::Start(format!(
            "agent container is unavailable: {}",
            harness.config.agent_container
        )));
    }
    let output = Command::new(&harness.config.docker_binary)
        .args(["exec", &harness.config.agent_container, "test", "-x"])
        .arg(&harness.config.pi_executable)
        .output()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                HarnessError::NotInstalled(harness.config.docker_binary.display().to_string())
            } else {
                HarnessError::Start(error.to_string())
            }
        })?;
    if output.status.success() {
        Ok(())
    } else {
        Err(HarnessError::NotInstalled(format!(
            "{} in container {}",
            harness.config.pi_executable, harness.config.agent_container
        )))
    }
}

pub(crate) fn signal_container_group(
    docker_binary: &Path,
    container: &str,
    session_id: &str,
    signal: &str,
) -> Result<(), HarnessError> {
    let status = Command::new(docker_binary)
        .args([
            "exec",
            container,
            &format!("{CONTAINER_SESSION}/{CONTROL_FILE}"),
            signal,
            &container_process_record(session_id),
        ])
        .status()
        .map_err(io_error)?;
    if status.success() || signal == "CHECK" {
        Ok(())
    } else if !container_recorded_group_alive(docker_binary, container, session_id)? {
        // The group exited between the caller's check and signal delivery.
        Ok(())
    } else {
        Err(HarnessError::Crashed(format!(
            "failed to signal Pi process group {signal} for {session_id}"
        )))
    }
}

fn container_recorded_group_alive(
    docker_binary: &Path,
    container: &str,
    session_id: &str,
) -> Result<bool, HarnessError> {
    let status = Command::new(docker_binary)
        .args([
            "exec",
            container,
            &format!("{CONTAINER_SESSION}/{CONTROL_FILE}"),
            "CHECK",
            &container_process_record(session_id),
        ])
        .status()
        .map_err(io_error)?;
    Ok(status.success())
}

fn container_group_alive(harness: &PiHarness, session_id: &str) -> Result<bool, HarnessError> {
    container_recorded_group_alive(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        session_id,
    )
}

fn live_events_path(session: &SessionRef) -> PathBuf {
    Path::new(&session.path).join(format!("pi.events-{}.jsonl", session.id))
}

pub(crate) fn events_path(session: &SessionRef) -> PathBuf {
    live_events_path(session)
}

fn host_process_record(session: &SessionRef) -> PathBuf {
    Path::new(&session.path).join(format!("pi-process-{}", session.id))
}

fn container_process_record(session_id: &str) -> String {
    format!("{CONTAINER_SESSION}/pi-process-{session_id}")
}

fn materialize_process_scripts(session_dir: &Path) -> Result<(), HarnessError> {
    write_once(&session_dir.join(LAUNCHER_FILE), LAUNCHER.as_bytes())?;
    write_once(&session_dir.join(CONTROL_FILE), CONTROL.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            session_dir.join(LAUNCHER_FILE),
            fs::Permissions::from_mode(0o755),
        )
        .map_err(io_error)?;
        fs::set_permissions(
            session_dir.join(CONTROL_FILE),
            fs::Permissions::from_mode(0o755),
        )
        .map_err(io_error)?;
    }
    Ok(())
}

fn write_packet_once(path: &Path, packet: &TaskPacket) -> Result<(), HarnessError> {
    let bytes = serde_json::to_vec(packet).map_err(|error| HarnessError::Io(error.to_string()))?;
    if path.exists() {
        return if fs::read(path).map_err(io_error)? == bytes {
            Ok(())
        } else {
            Err(HarnessError::InvalidSession(format!(
                "task packet differs: {}",
                path.display()
            )))
        };
    }
    atomic_write(path, &bytes)
}

fn write_json_once(path: &Path, value: &impl Serialize) -> Result<(), HarnessError> {
    let bytes = serde_json::to_vec(value).map_err(|error| HarnessError::Io(error.to_string()))?;
    if path.exists() {
        return if fs::read(path).map_err(io_error)? == bytes {
            Ok(())
        } else {
            Err(HarnessError::InvalidSession(format!(
                "owner record differs: {}",
                path.display()
            )))
        };
    }
    atomic_write(path, &bytes)
}

pub(crate) fn write_once(path: &Path, bytes: &[u8]) -> Result<(), HarnessError> {
    if path.exists() {
        return Ok(());
    }
    atomic_write(path, bytes)
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), HarnessError> {
    let parent = path
        .parent()
        .ok_or_else(|| HarnessError::Io("path has no parent".to_owned()))?;
    fs::create_dir_all(parent).map_err(io_error)?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = File::create(&temporary).map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(&temporary, path).map_err(io_error)
}

pub(crate) fn io_error(error: io::Error) -> HarnessError {
    HarnessError::Io(error.to_string())
}

const LAUNCHER: &str = r#"#!/bin/sh
record=$1
shift
setsid "$@" &
pid=$!
printf '{"pid":%s,"pgid":%s}\n' "$pid" "$pid" > "${record}.tmp"
mv "${record}.tmp" "$record"
wait "$pid"
"#;

const CONTROL: &str = r#"#!/bin/sh
action=$1
record=$2
[ -f "$record" ] || exit 1
pgid=$(sed -n 's/.*"pgid":\([0-9][0-9]*\).*/\1/p' "$record")
[ -n "$pgid" ] || exit 1
case "$action" in
  CHECK) kill -0 "-$pgid" 2>/dev/null ;;
  TERM) kill -TERM "-$pgid" ;;
  KILL) kill -KILL "-$pgid" ;;
  *) exit 2 ;;
esac
"#;
