use crate::PiHarness;
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::{ModelPolicy, SessionId, TaskPacket};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    fs::{self, File},
    io::{self, Write},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub(crate) const OWNER_FILE: &str = "owner.json";
pub(crate) const CURSOR_FILE: &str = ".cursor";
pub(crate) const RESUME_COUNT_FILE: &str = "resume-count";

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SessionOwner {
    pub(crate) execution_id: String,
    pub(crate) session_id: String,
    pub(crate) worktree_path: String,
    pub(crate) labels: std::collections::BTreeMap<String, String>,
    pub(crate) model_policy: Option<ModelPolicy>,
}

pub(crate) fn start(harness: &PiHarness, packet: &TaskPacket) -> Result<SessionRef, HarnessError> {
    ensure_executable(&harness.config.executable)?;
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

    let session = SessionRef {
        id: session_id,
        path: session_dir.display().to_string(),
        execution_id,
        worktree_path: harness.config.worktree.display().to_string(),
    };
    let mut args = base_args(harness, &session_dir)?;
    args.extend(["--session-id".into(), session.id.to_string()]);
    args.push(format!("@{}", packet_path.display()));
    spawn(harness, &session, args)?;
    Ok(session)
}

pub(crate) fn base_args(
    harness: &PiHarness,
    session_dir: &Path,
) -> Result<Vec<String>, HarnessError> {
    let mut args = vec![
        "--mode".into(),
        "json".into(),
        "--session-dir".into(),
        session_dir.display().to_string(),
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
    // Disable ambient discovery even when explicit skills are loaded.
    args.push("--no-skills".into());
    for skill in skills {
        args.extend(["--skill".into(), skill.display().to_string()]);
    }
    if let Some(policy) = harness.config.model_policy.as_ref() {
        args.extend(["--provider".into(), policy.provider.clone()]);
        let models = policy
            .preferred
            .iter()
            .chain(&policy.alternatives)
            .cloned()
            .collect::<Vec<_>>();
        if !models.is_empty() {
            args.extend(["--models".into(), models.join(",")]);
        }
    }
    Ok(args)
}

fn packet_role_skill(worktree: &Path) -> Option<String> {
    let bytes = fs::read(worktree.join(".autospec/task-packet.json")).ok()?;
    serde_json::from_slice::<TaskPacket>(&bytes)
        .ok()?
        .role_skill
}

fn resolve_role_skill(
    worktree: &Path,
    role_skill: &str,
) -> Result<std::path::PathBuf, HarnessError> {
    let worktree = fs::canonicalize(worktree).map_err(io_error)?;
    let skill = fs::canonicalize(worktree.join(role_skill)).map_err(io_error)?;
    if !skill.starts_with(&worktree) || !skill.is_file() {
        return Err(HarnessError::Start(format!(
            "role skill is outside the worktree: {role_skill}"
        )));
    }
    Ok(skill)
}

pub(crate) fn spawn(
    harness: &PiHarness,
    session: &SessionRef,
    args: Vec<String>,
) -> Result<(), HarnessError> {
    let session_dir = Path::new(&session.path);
    let stdout = File::options()
        .create(true)
        .append(true)
        .open(session_dir.join("pi.stdout.jsonl"))
        .map_err(io_error)?;
    let stderr = File::options()
        .create(true)
        .append(true)
        .open(session_dir.join("pi.stderr.log"))
        .map_err(io_error)?;
    let mut command = Command::new(&harness.config.executable);
    command
        .args(args)
        .current_dir(&harness.config.worktree)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    #[cfg(unix)]
    command.process_group(0);
    let child = command.spawn().map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            HarnessError::NotInstalled(harness.config.executable.display().to_string())
        } else {
            HarnessError::Start(error.to_string())
        }
    })?;
    let mut processes = harness
        .processes
        .0
        .lock()
        .map_err(|_| HarnessError::Start("process registry lock poisoned".to_owned()))?;
    if processes.contains_key(session.id.as_str()) {
        let mut child = child;
        let _ = child.kill();
        let _ = child.wait();
        return Err(HarnessError::Start(format!(
            "Pi session {} is already running",
            session.id
        )));
    }
    processes.insert(session.id.to_string(), child);
    Ok(())
}

pub(crate) async fn stop(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    let child = harness
        .processes
        .0
        .lock()
        .map_err(|_| HarnessError::Crashed("process registry lock poisoned".to_owned()))?
        .remove(session.id.as_str());
    let Some(mut child) = child else {
        return Ok(());
    };
    if child.try_wait().map_err(io_error)?.is_some() {
        return Ok(());
    }
    send_term(child.id())?;
    let deadline = Instant::now() + harness.config.stop_timeout;
    while Instant::now() < deadline {
        if child.try_wait().map_err(io_error)?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    send_signal(child.id(), "-KILL")?;
    child.wait().map_err(io_error)?;
    Ok(())
}

fn send_term(pid: u32) -> Result<(), HarnessError> {
    send_signal(pid, "-TERM")
}

fn send_signal(pid: u32, signal: &str) -> Result<(), HarnessError> {
    #[cfg(unix)]
    let target = format!("-{pid}");
    #[cfg(not(unix))]
    let target = pid.to_string();
    let status = Command::new("kill")
        .args([signal, "--", &target])
        .status()
        .map_err(io_error)?;
    status
        .success()
        .then_some(())
        .ok_or_else(|| HarnessError::Crashed(format!("failed to terminate Pi process {pid}")))
}

fn ensure_executable(path: &Path) -> Result<(), HarnessError> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(()),
        _ => Err(HarnessError::NotInstalled(path.display().to_string())),
    }
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

pub(crate) fn kill_process_group(pid: u32) -> Result<(), HarnessError> {
    send_signal(pid, "-KILL")
}
