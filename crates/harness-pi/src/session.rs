use crate::{ManagedProcess, PiHarness};
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::{ModelPolicy, SessionId, TaskPacket};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

pub(crate) const OWNER_FILE: &str = "owner.json";
pub(crate) const CURSOR_FILE: &str = ".cursor";
pub(crate) const RESUME_COUNT_FILE: &str = "resume-count";
pub(crate) const CONTAINER_WORKTREE: &str = "/workspace";
pub(crate) const CONTAINER_SESSION: &str = "/session";
pub(crate) const CONVERSATION_DIR: &str = "conversation";
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISOR_HEADER_TIMEOUT: Duration = Duration::from_secs(2);
const KILL_REAP_TIMEOUT: Duration = Duration::from_secs(1);
const SUPERVISOR: &str = r#"token=$1
shift
printf '{"type":"autospec_control","token":"%s","pgid":%s}\n' "$token" "$$"
IFS= read -r ack || exit 125
[ "$ack" = "ACK $token" ] || exit 125
exec "$@""#;
const GROUP_HAS_RUNNABLE: &str = r#"target=$1
for stat_file in /proc/[0-9]*/stat; do
  stat=$(cat "$stat_file" 2>/dev/null) || continue
  rest=${stat##*) }
  set -- $rest
  state=$1
  pgid=$3
  [ "$pgid" = "$target" ] && [ "$state" != Z ] && exit 0
done
exit 1"#;
const TOKEN_PGIDS: &str = r#"token=$1
for environment in /proc/[0-9]*/environ; do
  [ -r "$environment" ] || continue
  if tr '\000' '\n' < "$environment" 2>/dev/null | grep -Fqx "AUTOSPEC_SUPERVISOR_TOKEN=$token"; then
    pid=${environment#/proc/}
    pid=${pid%/environ}
    stat=$(cat "/proc/$pid/stat" 2>/dev/null) || continue
    rest=${stat##*) }
    set -- $rest
    state=$1
    pgid=$3
    [ "$state" = Z ] || printf '%s\n' "$pgid"
  fi
done"#;
static SUPERVISOR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    fs::create_dir_all(session_dir.join(CONVERSATION_DIR)).map_err(io_error)?;
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
    let supervisor_token = format!(
        "{}-{}",
        std::process::id(),
        SUPERVISOR_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut command = Command::new(&harness.config.docker_binary);
    command
        .args(["exec", "--interactive", "--env"])
        .arg(format!("AUTOSPEC_SUPERVISOR_TOKEN={supervisor_token}"))
        .args(["--workdir", CONTAINER_WORKTREE])
        .arg(&harness.config.agent_container)
        .args([
            "setsid",
            "/bin/sh",
            "-c",
            SUPERVISOR,
            "autospec-pi-supervisor",
        ])
        .arg(&supervisor_token)
        .arg(&harness.config.pi_executable)
        .args(pi_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(stderr);
    let mut child = command.spawn().map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            HarnessError::NotInstalled(harness.config.docker_binary.display().to_string())
        } else {
            HarnessError::Start(error.to_string())
        }
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| HarnessError::Start("docker exec stdout was not piped".to_owned()))?;
    let mut supervisor_input = child
        .stdin
        .take()
        .ok_or_else(|| HarnessError::Start("docker exec stdin was not piped".to_owned()))?;
    let (control_tx, control_rx) = mpsc::sync_channel(1);
    let pump_docker = harness.config.docker_binary.clone();
    let pump_container = harness.config.agent_container.clone();
    let expected_supervisor_token = supervisor_token.clone();
    let event_thread = thread::Builder::new().name(format!("pi-events-{}", session.id));
    if let Err(error) = event_thread.spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut header = String::new();
        let control = reader
            .read_line(&mut header)
            .map_err(|error| error.to_string())
            .and_then(|length| {
                if length == 0 {
                    Err("supervisor exited before its control record".to_owned())
                } else {
                    parse_supervisor_header(&header, &expected_supervisor_token)
                }
            });
        let valid = control.is_ok();
        let pgid = control.as_ref().ok().copied();
        if control_tx.send(control).is_ok() && valid {
            let mut events = events;
            if let Err(error) = io::copy(&mut reader, &mut events) {
                if let Some(pgid) = pgid {
                    let cleanup =
                        cleanup_container_authority(&pump_docker, &pump_container, Some(pgid), "");
                    tracing::error!(
                        error = %error,
                        cleanup_error = cleanup.err().map(|failure| failure.to_string()),
                        "Pi event reader failed; terminated its process group"
                    );
                }
            }
            let _ = events.flush();
        }
    }) {
        drop(supervisor_input);
        cleanup_failed_start(harness, &supervisor_token, None, &mut child)?;
        return Err(HarnessError::Start(error.to_string()));
    }
    let pgid = match control_rx.recv_timeout(SUPERVISOR_HEADER_TIMEOUT) {
        Ok(Ok(pgid)) => pgid,
        Ok(Err(error)) => {
            drop(supervisor_input);
            cleanup_failed_start(harness, &supervisor_token, None, &mut child)?;
            return Err(HarnessError::Start(error));
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            drop(supervisor_input);
            cleanup_failed_start(harness, &supervisor_token, None, &mut child)?;
            return Err(HarnessError::Start(
                "timed out waiting for trusted Pi supervisor control record".to_owned(),
            ));
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            drop(supervisor_input);
            cleanup_failed_start(harness, &supervisor_token, None, &mut child)?;
            return Err(HarnessError::Start(
                "Pi supervisor control channel disconnected".to_owned(),
            ));
        }
    };
    if let Err(error) =
        writeln!(supervisor_input, "ACK {supervisor_token}").and_then(|()| supervisor_input.flush())
    {
        drop(supervisor_input);
        cleanup_failed_start(harness, &supervisor_token, Some(pgid), &mut child)?;
        return Err(HarnessError::Start(format!(
            "failed to acknowledge Pi supervisor: {error}"
        )));
    }
    drop(supervisor_input);
    children.insert(
        session.id.to_string(),
        Arc::new(ManagedProcess {
            child: std::sync::Mutex::new(child),
            pgid,
        }),
    );
    Ok(())
}

pub(crate) async fn stop(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    let process = harness
        .processes
        .children
        .lock()
        .map_err(|_| HarnessError::Crashed("process registry lock poisoned".to_owned()))?
        .get(session.id.as_str())
        .cloned();
    let Some(process) = process else {
        return Ok(());
    };
    if process_reaped(harness, &process)? {
        remove_registered_child(harness, session, &process)?;
        return Ok(());
    }
    signal_container_group(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        process.pgid,
        "TERM",
    )?;
    let deadline = Instant::now() + harness.config.stop_timeout;
    while Instant::now() < deadline {
        if process_reaped(harness, &process)? {
            remove_registered_child(harness, session, &process)?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    signal_container_group(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        process.pgid,
        "KILL",
    )?;
    let kill_deadline = Instant::now() + KILL_REAP_TIMEOUT;
    while Instant::now() < kill_deadline {
        if process_reaped(harness, &process)? {
            remove_registered_child(harness, session, &process)?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(HarnessError::Crashed(format!(
        "Pi process group for session {} was not reaped",
        session.id
    )))
}

fn process_reaped(harness: &PiHarness, process: &ManagedProcess) -> Result<bool, HarnessError> {
    let child_exited = process
        .child
        .lock()
        .map_err(|_| HarnessError::Crashed("Pi child lock poisoned".to_owned()))?
        .try_wait()
        .map_err(io_error)?
        .is_some();
    Ok(child_exited && !container_group_alive(harness, process.pgid)?)
}

fn remove_registered_child(
    harness: &PiHarness,
    session: &SessionRef,
    expected: &Arc<ManagedProcess>,
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
    }
    Ok(())
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
    pgid: u32,
    signal: &str,
) -> Result<(), HarnessError> {
    let status = run_control_command(docker_binary, container, pgid, signal)?;
    if status.success() {
        Ok(())
    } else if !container_group_alive_with(docker_binary, container, pgid)? {
        // The group exited between the caller's check and signal delivery.
        Ok(())
    } else {
        Err(HarnessError::Crashed(format!(
            "failed to signal Pi process group {pgid} with {signal}"
        )))
    }
}

fn container_group_alive_with(
    docker_binary: &Path,
    container: &str,
    pgid: u32,
) -> Result<bool, HarnessError> {
    let mut command = Command::new(docker_binary);
    command
        .args([
            "exec",
            "--user",
            "root",
            container,
            "/bin/sh",
            "-c",
            GROUP_HAS_RUNNABLE,
            "autospec-pi-group-liveness",
        ])
        .arg(pgid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    Ok(run_command_bounded(&mut command, CONTROL_TIMEOUT)?.success())
}

fn run_control_command(
    docker_binary: &Path,
    container: &str,
    pgid: u32,
    signal: &str,
) -> Result<ExitStatus, HarnessError> {
    let mut command = Command::new(docker_binary);
    command
        .args([
            "exec",
            "--user",
            "root",
            container,
            "/bin/sh",
            "-c",
            r#"kill "$1" "-$2""#,
            "autospec-pi-kill",
        ])
        .arg(format!("-{signal}"))
        .arg(pgid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    run_command_bounded(&mut command, CONTROL_TIMEOUT)
}

fn cleanup_failed_start(
    harness: &PiHarness,
    token: &str,
    pgid: Option<u32>,
    child: &mut std::process::Child,
) -> Result<(), HarnessError> {
    let client = terminate_host_child(child, CONTROL_TIMEOUT);
    let cleanup = cleanup_container_authority(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        pgid,
        token,
    );
    match (client, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (client, cleanup) => Err(HarnessError::Start(format!(
            "failed startup cleanup: docker client={}; Pi authority={}",
            cleanup_result(client),
            cleanup_result(cleanup)
        ))),
    }
}

fn cleanup_result(result: Result<(), HarnessError>) -> String {
    match result {
        Ok(()) => "reaped".to_owned(),
        Err(error) => error.to_string(),
    }
}

fn cleanup_container_authority(
    docker_binary: &Path,
    container: &str,
    pgid: Option<u32>,
    token: &str,
) -> Result<(), HarnessError> {
    if let Some(pgid) = pgid {
        return cleanup_process_groups(docker_binary, container, &[pgid]);
    }
    let pgids = derive_token_pgids(docker_binary, container, token)?;
    if pgids.is_empty() {
        Ok(())
    } else {
        cleanup_process_groups(docker_binary, container, &pgids)
    }
}

fn cleanup_process_groups(
    docker_binary: &Path,
    container: &str,
    pgids: &[u32],
) -> Result<(), HarnessError> {
    for pgid in pgids {
        signal_container_group(docker_binary, container, *pgid, "TERM")?;
    }
    if wait_for_process_groups_reap(docker_binary, container, pgids, Duration::from_millis(250))? {
        return Ok(());
    }
    for pgid in pgids {
        signal_container_group(docker_binary, container, *pgid, "KILL")?;
    }
    if wait_for_process_groups_reap(docker_binary, container, pgids, KILL_REAP_TIMEOUT)? {
        Ok(())
    } else {
        Err(HarnessError::Crashed(
            "startup-failed Pi process groups were not reaped".to_owned(),
        ))
    }
}

fn wait_for_process_groups_reap(
    docker_binary: &Path,
    container: &str,
    pgids: &[u32],
    timeout: Duration,
) -> Result<bool, HarnessError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut any_alive = false;
        for pgid in pgids {
            any_alive |= container_group_alive_with(docker_binary, container, *pgid)?;
        }
        if !any_alive {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(false)
}

fn derive_token_pgids(
    docker_binary: &Path,
    container: &str,
    token: &str,
) -> Result<Vec<u32>, HarnessError> {
    let mut command = Command::new(docker_binary);
    command
        .args([
            "exec",
            "--user",
            "root",
            container,
            "/bin/sh",
            "-c",
            TOKEN_PGIDS,
            "autospec-pi-token-control",
            token,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let (status, stdout) = run_command_capture_bounded(&mut command, CONTROL_TIMEOUT)?;
    if !status.success() {
        return Err(HarnessError::Crashed(
            "failed to derive startup process groups from trusted token".to_owned(),
        ));
    }
    let mut pgids = stdout
        .lines()
        .filter_map(|line| line.parse::<u32>().ok())
        .filter(|pgid| *pgid > 1)
        .collect::<Vec<_>>();
    pgids.sort_unstable();
    pgids.dedup();
    Ok(pgids)
}

fn container_group_alive(harness: &PiHarness, pgid: u32) -> Result<bool, HarnessError> {
    container_group_alive_with(
        &harness.config.docker_binary,
        &harness.config.agent_container,
        pgid,
    )
}

pub(crate) fn terminate_on_drop(
    docker_binary: &Path,
    container: &str,
    process: &ManagedProcess,
    stop_timeout: Duration,
) {
    let _ = signal_container_group(docker_binary, container, process.pgid, "TERM");
    if wait_for_reap_bounded(docker_binary, container, process, stop_timeout) {
        return;
    }
    let _ = signal_container_group(docker_binary, container, process.pgid, "KILL");
    if wait_for_reap_bounded(docker_binary, container, process, KILL_REAP_TIMEOUT) {
        return;
    }
    if let Ok(mut child) = process.child.try_lock() {
        let _ = child.kill();
        let _ = child.try_wait();
    }
}

fn wait_for_reap_bounded(
    docker_binary: &Path,
    container: &str,
    process: &ManagedProcess,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let child_exited = process
            .child
            .try_lock()
            .ok()
            .and_then(|mut child| child.try_wait().ok().flatten())
            .is_some();
        let group_alive =
            container_group_alive_with(docker_binary, container, process.pgid).unwrap_or(true);
        if child_exited && !group_alive {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

fn run_command_bounded(
    command: &mut Command,
    timeout: Duration,
) -> Result<ExitStatus, HarnessError> {
    let mut child = command.spawn().map_err(io_error)?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().map_err(io_error)? {
            return Ok(status);
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.try_wait();
    Err(HarnessError::Crashed(
        "timed out running Docker process-group control command".to_owned(),
    ))
}

fn run_command_capture_bounded(
    command: &mut Command,
    timeout: Duration,
) -> Result<(ExitStatus, String), HarnessError> {
    let mut child = command.spawn().map_err(io_error)?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().map_err(io_error)? {
            let mut stdout = String::new();
            if let Some(mut pipe) = child.stdout.take() {
                pipe.read_to_string(&mut stdout).map_err(io_error)?;
            }
            return Ok((status, stdout));
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.try_wait();
    Err(HarnessError::Crashed(
        "timed out deriving Docker process-group control state".to_owned(),
    ))
}

fn terminate_host_child(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Result<(), HarnessError> {
    let _ = child.kill();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Err(HarnessError::Crashed(
        "Docker exec client was not reaped after startup failure".to_owned(),
    ))
}

#[derive(Deserialize)]
struct SupervisorControl {
    #[serde(rename = "type")]
    record_type: String,
    token: String,
    pgid: u32,
}

fn parse_supervisor_header(header: &str, expected_token: &str) -> Result<u32, String> {
    let control: SupervisorControl = serde_json::from_str(header)
        .map_err(|error| format!("invalid supervisor header: {error}"))?;
    if control.record_type != "autospec_control"
        || control.token != expected_token
        || control.pgid == 0
    {
        return Err("invalid supervisor control record".to_owned());
    }
    Ok(control.pgid)
}

fn live_events_path(session: &SessionRef) -> PathBuf {
    Path::new(&session.path).join(format!("pi.events-{}.jsonl", session.id))
}

pub(crate) fn events_path(session: &SessionRef) -> PathBuf {
    live_events_path(session)
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
