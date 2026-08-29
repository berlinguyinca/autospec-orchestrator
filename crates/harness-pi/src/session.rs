use crate::{verify_storage_paths, DockerCli, ManagedProcess, PiHarness, ReadyPiStorage};
use execution_storage::{ExecutionLifecycleHold, ExecutionLifecycleHoldStore, ReadyLease};
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::{ModelPolicy, SessionId, TaskPacket};
use runtime_traits::VerifiedBindMount;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{mpsc, Arc, Mutex, OnceLock},
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
const SUPERVISOR_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const KILL_REAP_TIMEOUT: Duration = Duration::from_secs(1);
const GROUP_PROBE: &str = r#"target=$1
token=$2
printf 'AUTOSPEC_GROUP_PROBE %s\n' "$token"
for stat_file in /proc/[0-9]*/stat; do
  stat=$(cat "$stat_file" 2>/dev/null) || continue
  rest=${stat##*) }
  set -- $rest
  state=$1
  pgid=$3
  [ "$pgid" = "$target" ] && printf 'MEMBER %s\n' "$state"
done
exit 0"#;
const SUPERVISOR: &str = r#"token=$1
shift
printf '{"type":"autospec_control","token":"%s","pgid":%s}\n' "$token" "$$"
IFS= read -r ack || exit 125
[ "$ack" = "ACK $token" ] || exit 125
printf '{"type":"autospec_ready","token":"%s"}\n' "$token"
exec "$@""#;
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

enum QuarantinedProcess {
    Registered {
        docker: DockerCli,
        container: String,
        process: Arc<ManagedProcess>,
    },
    Startup {
        docker: DockerCli,
        container: String,
        child: Mutex<std::process::Child>,
        pgid: Option<u32>,
        token: String,
        lifecycle_holds: ExecutionLifecycleHoldStore,
        hold_execution_id: orchestrator_core::ExecutionId,
        hold_id: String,
        _storage_lease: Box<dyn ReadyLease>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SessionOwner {
    pub(crate) execution_id: String,
    pub(crate) session_id: String,
    pub(crate) worktree_path: String,
    pub(crate) labels: std::collections::BTreeMap<String, String>,
    pub(crate) model_policy: Option<ModelPolicy>,
}

pub(crate) fn start(harness: &PiHarness, packet: &TaskPacket) -> Result<SessionRef, HarnessError> {
    let storage = prepare_launch(harness)?;
    ensure_docker_and_pi(harness)?;
    let execution_id = harness.config.labels.execution_id.clone();
    let session_id = SessionId::new(execution_id.to_string());
    let session_dir = storage.layout.session.clone();
    let packet_directory = storage.layout.repository.join(".autospec");
    storage
        .lease
        .verified()
        .verify_directory(&packet_directory)
        .map_err(crate::storage_error)?;
    let packet_path = packet_directory.join("task-packet.json");
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
    spawn(harness, &session, args, storage)?;
    Ok(session)
}

pub(crate) fn prepare_launch(harness: &PiHarness) -> Result<ReadyPiStorage, HarnessError> {
    let storage = harness.acquire_ready_storage()?;
    verify_live_agent_container(harness, &storage.layout)?;
    recover_lifecycle_holds(harness)?;
    let unresolved = lifecycle_hold_store(harness)?
        .list(&harness.config.labels.execution_id)
        .map_err(crate::storage_error)?;
    if !unresolved.is_empty() {
        return Err(HarnessError::Start(format!(
            "{} unresolved Pi lifecycle hold(s) remain for execution {}",
            unresolved.len(),
            harness.config.labels.execution_id
        )));
    }
    Ok(storage)
}

fn lifecycle_hold_store(harness: &PiHarness) -> Result<ExecutionLifecycleHoldStore, HarnessError> {
    ExecutionLifecycleHoldStore::new(&harness.config.state_root).map_err(crate::storage_error)
}

fn recover_lifecycle_holds(harness: &PiHarness) -> Result<(), HarnessError> {
    let store = lifecycle_hold_store(harness)?;
    for hold in store
        .list(&harness.config.labels.execution_id)
        .map_err(crate::storage_error)?
    {
        if harness
            .processes
            .children
            .lock()
            .map_err(|_| HarnessError::Start("process registry lock poisoned".to_owned()))?
            .contains_key(&hold.session_id)
        {
            continue;
        }
        if hold.labels != harness.config.labels
            || hold.container_id != harness.config.agent_container
        {
            return Err(HarnessError::Start(
                "durable Pi lifecycle hold does not match the live container capability".to_owned(),
            ));
        }
        cleanup_container_authority(
            &harness.processes.docker,
            &hold.container_id,
            hold.pgid,
            &hold.supervisor_token,
        )?;
        store
            .remove(&hold.labels.execution_id, &hold.hold_id)
            .map_err(crate::storage_error)?;
    }
    Ok(())
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
    storage: ReadyPiStorage,
) -> Result<(), HarnessError> {
    harness.validate_session(session)?;
    verify_storage_paths(storage.lease.verified(), &storage.layout)?;
    verify_live_agent_container(harness, &storage.layout)?;
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
    let events = open_private_append(&live_events_path(session))?;
    let mut stderr =
        open_private_append(&session_dir.join(format!("pi.stderr-{}.log", session.id)))?;
    let supervisor_token = supervisor_token()?;
    let lifecycle_holds = ExecutionLifecycleHoldStore::new(&harness.config.state_root)
        .map_err(crate::storage_error)?;
    let hold_id = format!("pi-{}", session.id);
    let mut lifecycle_hold = ExecutionLifecycleHold {
        labels: harness.config.labels.clone(),
        hold_id: hold_id.clone(),
        container_id: harness.config.agent_container.clone(),
        session_id: session.id.to_string(),
        supervisor_token: supervisor_token.clone(),
        pgid: None,
    };
    lifecycle_holds
        .create(&lifecycle_hold)
        .map_err(crate::storage_error)?;
    let mut command = harness.processes.docker.command();
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
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| {
        let _ = lifecycle_holds.remove(&harness.config.labels.execution_id, &hold_id);
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
    let stderr_pipe = child
        .stderr
        .take()
        .ok_or_else(|| HarnessError::Start("docker exec stderr was not piped".to_owned()))?;
    let mut supervisor_input = child
        .stdin
        .take()
        .ok_or_else(|| HarnessError::Start("docker exec stdin was not piped".to_owned()))?;
    let (control_tx, control_rx) = mpsc::sync_channel(1);
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let pump_docker = harness.processes.docker.clone();
    let pump_container = harness.config.agent_container.clone();
    let expected_supervisor_token = supervisor_token.clone();
    let credential_redactions = harness.config.credential_redactions.clone();
    let stderr_redactions = credential_redactions.clone();
    let stderr_thread = thread::Builder::new()
        .name(format!("pi-stderr-{}", session.id))
        .spawn(move || {
            let mut reader = BufReader::new(stderr_pipe);
            let mut record = Vec::new();
            loop {
                record.clear();
                match reader.read_until(b'\n', &mut record) {
                    Ok(0) => break,
                    Ok(_) => {
                        for secret in &stderr_redactions {
                            redact_bytes(&mut record, secret);
                        }
                        if stderr.write_all(&record).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = stderr.flush();
        });
    if let Err(error) = stderr_thread {
        drop(supervisor_input);
        cleanup_failed_start(
            harness,
            &supervisor_token,
            None,
            child,
            storage.lease,
            lifecycle_holds,
            hold_id,
        );
        return Err(HarnessError::Start(error.to_string()));
    }
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
            let mut ready = String::new();
            let confirmation = reader
                .read_line(&mut ready)
                .map_err(|error| error.to_string())
                .and_then(|length| {
                    if length == 0 {
                        Err("supervisor exited before its READY confirmation".to_owned())
                    } else {
                        parse_supervisor_ready(&ready, &expected_supervisor_token)
                    }
                });
            let confirmed = confirmation.is_ok();
            if ready_tx.send(confirmation).is_err() || !confirmed {
                return;
            }
            let mut events = events;
            let copy_result = (|| -> io::Result<()> {
                let mut record = Vec::new();
                loop {
                    record.clear();
                    if reader.read_until(b'\n', &mut record)? == 0 {
                        return Ok(());
                    }
                    for secret in &credential_redactions {
                        redact_bytes(&mut record, secret);
                    }
                    events.write_all(&record)?;
                }
            })();
            if let Err(error) = copy_result {
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
        cleanup_failed_start(
            harness,
            &supervisor_token,
            None,
            child,
            storage.lease,
            lifecycle_holds,
            hold_id,
        );
        return Err(HarnessError::Start(error.to_string()));
    }
    let pgid = match control_rx.recv_timeout(SUPERVISOR_HEADER_TIMEOUT) {
        Ok(Ok(pgid)) => pgid,
        Ok(Err(error)) => {
            drop(supervisor_input);
            cleanup_failed_start(
                harness,
                &supervisor_token,
                None,
                child,
                storage.lease,
                lifecycle_holds,
                hold_id,
            );
            return Err(HarnessError::Start(error));
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            drop(supervisor_input);
            cleanup_failed_start(
                harness,
                &supervisor_token,
                None,
                child,
                storage.lease,
                lifecycle_holds,
                hold_id,
            );
            return Err(HarnessError::Start(
                "timed out waiting for trusted Pi supervisor control record".to_owned(),
            ));
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            drop(supervisor_input);
            cleanup_failed_start(
                harness,
                &supervisor_token,
                None,
                child,
                storage.lease,
                lifecycle_holds,
                hold_id,
            );
            return Err(HarnessError::Start(
                "Pi supervisor control channel disconnected".to_owned(),
            ));
        }
    };
    if let Err(error) =
        writeln!(supervisor_input, "ACK {supervisor_token}").and_then(|()| supervisor_input.flush())
    {
        drop(supervisor_input);
        cleanup_failed_start(
            harness,
            &supervisor_token,
            Some(pgid),
            child,
            storage.lease,
            lifecycle_holds,
            hold_id,
        );
        return Err(HarnessError::Start(format!(
            "failed to acknowledge Pi supervisor: {error}"
        )));
    }
    drop(supervisor_input);
    match ready_rx.recv_timeout(SUPERVISOR_HEADER_TIMEOUT) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            cleanup_failed_start(
                harness,
                &supervisor_token,
                Some(pgid),
                child,
                storage.lease,
                lifecycle_holds,
                hold_id,
            );
            return Err(HarnessError::Start(error));
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            cleanup_failed_start(
                harness,
                &supervisor_token,
                Some(pgid),
                child,
                storage.lease,
                lifecycle_holds,
                hold_id,
            );
            return Err(HarnessError::Start(
                "timed out waiting for trusted Pi supervisor READY confirmation".to_owned(),
            ));
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            cleanup_failed_start(
                harness,
                &supervisor_token,
                Some(pgid),
                child,
                storage.lease,
                lifecycle_holds,
                hold_id,
            );
            return Err(HarnessError::Start(
                "Pi supervisor READY channel disconnected".to_owned(),
            ));
        }
    }
    lifecycle_hold.pgid = Some(pgid);
    if let Err(error) = lifecycle_holds.replace(&lifecycle_hold) {
        cleanup_failed_start(
            harness,
            &supervisor_token,
            Some(pgid),
            child,
            storage.lease,
            lifecycle_holds,
            hold_id,
        );
        return Err(crate::storage_error(error));
    }
    children.insert(
        session.id.to_string(),
        Arc::new(ManagedProcess {
            child: std::sync::Mutex::new(child),
            pgid,
            supervisor_token,
            lifecycle_holds,
            hold_id,
            hold_execution_id: harness.config.labels.execution_id.clone(),
            _storage_lease: storage.lease,
        }),
    );
    Ok(())
}

fn redact_bytes(bytes: &mut Vec<u8>, secret: &[u8]) {
    const REDACTION: &[u8] = b"[REDACTED_CREDENTIAL]";
    if secret.is_empty() {
        return;
    }
    while let Some(index) = bytes
        .windows(secret.len())
        .position(|window| window == secret)
    {
        bytes.splice(index..index + secret.len(), REDACTION.iter().copied());
    }
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
        &harness.processes.docker,
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
        &harness.processes.docker,
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
    expected
        .lifecycle_holds
        .remove(&expected.hold_execution_id, &expected.hold_id)
        .map_err(crate::storage_error)?;
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
    let inspect = harness
        .processes
        .docker
        .command()
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
    let output = harness
        .processes
        .docker
        .command()
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

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LiveContainerInspect {
    id: String,
    state: LiveContainerState,
    config: LiveContainerConfig,
    mounts: Vec<LiveContainerMount>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LiveContainerState {
    running: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LiveContainerConfig {
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct LiveContainerMount {
    #[serde(rename = "Type")]
    mount_type: String,
    source: PathBuf,
    destination: String,
    #[serde(rename = "RW")]
    writable: bool,
}

pub(crate) fn verify_live_agent_container(
    harness: &PiHarness,
    layout: &execution_storage::ExecutionLayout,
) -> Result<(), HarnessError> {
    let capability = harness.container_capability()?;
    let daemon = harness
        .processes
        .docker
        .command()
        .args(["info", "--format={{.ID}}"])
        .output()
        .map_err(docker_start_error)?;
    if !daemon.status.success()
        || String::from_utf8_lossy(&daemon.stdout).trim() != capability.daemon_id
    {
        return Err(HarnessError::Start(
            "live Docker daemon differs from the runtime-issued container capability".to_owned(),
        ));
    }
    let output = harness
        .processes
        .docker
        .command()
        .args(["inspect", "--type", "container"])
        .arg(&capability.container_id)
        .output()
        .map_err(docker_start_error)?;
    if !output.status.success() {
        return Err(HarnessError::Start(
            "runtime-issued agent container is unavailable".to_owned(),
        ));
    }
    let mut inspected: Vec<LiveContainerInspect> = serde_json::from_slice(&output.stdout)
        .map_err(|error| HarnessError::Start(format!("decode live agent container: {error}")))?;
    if inspected.len() != 1 {
        return Err(HarnessError::Start(
            "Docker inspect did not return exactly one agent container".to_owned(),
        ));
    }
    let inspect = inspected.pop().expect("length checked");
    if inspect.id != capability.container_id || !inspect.state.running {
        return Err(HarnessError::Start(
            "runtime-issued agent container identity is stale or not running".to_owned(),
        ));
    }
    if inspect.config.labels != capability.labels.to_map() {
        return Err(HarnessError::Start(
            "live container ownership labels differ from its runtime capability".to_owned(),
        ));
    }
    let mut mounts = Vec::with_capacity(inspect.mounts.len());
    for mount in inspect.mounts {
        if mount.mount_type != "bind" {
            return Err(HarnessError::Start(
                "live agent container has a non-bind mount".to_owned(),
            ));
        }
        let source = fs::canonicalize(&mount.source).map_err(|error| {
            HarnessError::Start(format!("canonicalize live agent bind mount: {error}"))
        })?;
        mounts.push(VerifiedBindMount {
            source,
            target: mount.destination,
            writable: mount.writable,
        });
    }
    mounts.sort();
    if mounts != capability.mounts {
        return Err(HarnessError::Start(
            "live agent container mounts differ from its runtime capability".to_owned(),
        ));
    }
    let private_session = fs::canonicalize(&layout.session).map_err(io_error)?;
    let conversation = fs::canonicalize(&layout.conversation).map_err(io_error)?;
    if mounts.iter().any(|mount| {
        (mount.source.starts_with(&private_session) && mount.source != conversation)
            || mount.target == "/var/run/docker.sock"
            || mount.source == Path::new("/var/run/docker.sock")
    }) {
        return Err(HarnessError::Start(
            "live agent container exposes private harness state or Docker authority".to_owned(),
        ));
    }
    Ok(())
}

fn docker_start_error(error: io::Error) -> HarnessError {
    if error.kind() == io::ErrorKind::NotFound {
        HarnessError::NotInstalled("docker".to_owned())
    } else {
        HarnessError::Start(error.to_string())
    }
}

pub(crate) fn signal_container_group(
    docker: &DockerCli,
    container: &str,
    pgid: u32,
    signal: &str,
) -> Result<(), HarnessError> {
    let status = run_control_command(docker, container, pgid, signal)?;
    if status.success() {
        Ok(())
    } else if !container_group_alive_with(docker, container, pgid)? {
        // The group exited between the caller's check and signal delivery.
        Ok(())
    } else {
        Err(HarnessError::Crashed(format!(
            "failed to signal Pi process group {pgid} with {signal}"
        )))
    }
}

fn container_group_alive_with(
    docker: &DockerCli,
    container: &str,
    pgid: u32,
) -> Result<bool, HarnessError> {
    let token = supervisor_token()?;
    let mut command = docker.command();
    command
        .args([
            "exec",
            "--user",
            "root",
            container,
            "/bin/sh",
            "-c",
            GROUP_PROBE,
            "autospec-pi-group-probe",
        ])
        .arg(pgid.to_string())
        .arg(&token)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let (status, output) = run_command_capture_bounded(&mut command, CONTROL_TIMEOUT)?;
    if !status.success() {
        return Err(HarnessError::Crashed(
            "trusted Docker process-group probe failed without a result".to_owned(),
        ));
    }
    let mut lines = output.lines();
    let header = lines.next().ok_or_else(|| {
        HarnessError::Crashed("trusted process-group probe omitted its token".to_owned())
    })?;
    if header != format!("AUTOSPEC_GROUP_PROBE {token}") {
        return Err(HarnessError::Crashed(
            "trusted process-group probe returned the wrong token".to_owned(),
        ));
    }
    for line in lines {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() != 2 || columns[0] != "MEMBER" || columns[1].is_empty() {
            return Err(HarnessError::Crashed(
                "trusted process-group probe returned a malformed row".to_owned(),
            ));
        }
        if !columns[1].starts_with('Z') {
            return Ok(true);
        }
    }
    Ok(false)
}

fn run_control_command(
    docker: &DockerCli,
    container: &str,
    pgid: u32,
    signal: &str,
) -> Result<ExitStatus, HarnessError> {
    let mut command = docker.command();
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
    mut child: std::process::Child,
    storage_lease: Box<dyn ReadyLease>,
    lifecycle_holds: ExecutionLifecycleHoldStore,
    hold_id: String,
) {
    let client = terminate_host_child(&mut child, CONTROL_TIMEOUT);
    let cleanup = cleanup_container_authority(
        &harness.processes.docker,
        &harness.config.agent_container,
        pgid,
        token,
    );
    if client.is_ok()
        && cleanup.is_ok()
        && lifecycle_holds
            .remove(&harness.config.labels.execution_id, &hold_id)
            .is_ok()
    {
        return;
    }
    tracing::error!(
        docker_client = %cleanup_result(client),
        pi_authority = %cleanup_result(cleanup),
        "startup cleanup was uncertain; quarantining its storage lease and process authority"
    );
    quarantine(QuarantinedProcess::Startup {
        docker: harness.processes.docker.clone(),
        container: harness.config.agent_container.clone(),
        child: Mutex::new(child),
        pgid,
        token: token.to_owned(),
        lifecycle_holds,
        hold_execution_id: harness.config.labels.execution_id.clone(),
        hold_id,
        _storage_lease: storage_lease,
    });
}

fn cleanup_result(result: Result<(), HarnessError>) -> String {
    result.map_or_else(|error| error.to_string(), |()| "reaped".to_owned())
}

fn quarantine(process: QuarantinedProcess) {
    static SENDER: OnceLock<Option<mpsc::Sender<QuarantinedProcess>>> = OnceLock::new();
    let sender = SENDER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        match thread::Builder::new()
            .name("pi-quarantine-reaper".to_owned())
            .spawn(move || quarantine_reaper(receiver))
        {
            Ok(_) => Some(sender),
            Err(error) => {
                tracing::error!(%error, "failed to spawn Pi quarantine reaper; authority will remain quarantined for journal recovery");
                None
            }
        }
    });
    let Some(sender) = sender else {
        Box::leak(Box::new(process));
        return;
    };
    if let Err(error) = sender.send(process) {
        tracing::error!(
            "Pi quarantine reaper stopped; authority will remain quarantined for journal recovery"
        );
        Box::leak(Box::new(error.0));
    }
}

fn quarantine_reaper(receiver: mpsc::Receiver<QuarantinedProcess>) {
    let mut pending = Vec::new();
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(process) => pending.push(process),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) if pending.is_empty() => return,
            Err(mpsc::RecvTimeoutError::Disconnected) => {}
        }
        pending.retain(|process| !quarantine_reaped(process));
    }
}

fn quarantine_reaped(process: &QuarantinedProcess) -> bool {
    let (docker, container, child, pgid, token, lifecycle_holds, hold_execution_id, hold_id) =
        match process {
            QuarantinedProcess::Registered {
                docker,
                container,
                process,
            } => (
                docker,
                container,
                &process.child,
                Some(process.pgid),
                process.supervisor_token.as_str(),
                &process.lifecycle_holds,
                &process.hold_execution_id,
                process.hold_id.as_str(),
            ),
            QuarantinedProcess::Startup {
                docker,
                container,
                child,
                pgid,
                token,
                lifecycle_holds,
                hold_execution_id,
                hold_id,
                ..
            } => (
                docker,
                container,
                child,
                *pgid,
                token.as_str(),
                lifecycle_holds,
                hold_execution_id,
                hold_id.as_str(),
            ),
        };
    let client_reaped = child.try_lock().ok().is_some_and(|mut child| {
        if child.try_wait().ok().flatten().is_some() {
            true
        } else {
            let _ = child.kill();
            child.try_wait().ok().flatten().is_some()
        }
    });
    let pgids = match pgid {
        Some(pgid) => vec![pgid],
        None => match derive_token_pgids(docker, container, token) {
            Ok(pgids) => pgids,
            Err(_) => return false,
        },
    };
    let mut groups_reaped = true;
    for pgid in pgids {
        let _ = run_control_command(docker, container, pgid, "KILL");
        match container_group_alive_with(docker, container, pgid) {
            Ok(false) => {}
            Ok(true) | Err(_) => groups_reaped = false,
        }
    }
    if client_reaped && groups_reaped {
        lifecycle_holds.remove(hold_execution_id, hold_id).is_ok()
    } else {
        false
    }
}

pub(crate) fn quarantine_registered(
    docker: DockerCli,
    container: String,
    process: Arc<ManagedProcess>,
) {
    quarantine(QuarantinedProcess::Registered {
        docker,
        container,
        process,
    });
}

fn cleanup_container_authority(
    docker: &DockerCli,
    container: &str,
    pgid: Option<u32>,
    token: &str,
) -> Result<(), HarnessError> {
    if let Some(pgid) = pgid {
        return cleanup_process_groups(docker, container, &[pgid]);
    }
    let pgids = derive_token_pgids(docker, container, token)?;
    if pgids.is_empty() {
        Ok(())
    } else {
        cleanup_process_groups(docker, container, &pgids)
    }
}

fn cleanup_process_groups(
    docker: &DockerCli,
    container: &str,
    pgids: &[u32],
) -> Result<(), HarnessError> {
    for pgid in pgids {
        signal_container_group(docker, container, *pgid, "TERM")?;
    }
    if wait_for_process_groups_reap(docker, container, pgids, Duration::from_millis(250))? {
        return Ok(());
    }
    for pgid in pgids {
        signal_container_group(docker, container, *pgid, "KILL")?;
    }
    if wait_for_process_groups_reap(docker, container, pgids, KILL_REAP_TIMEOUT)? {
        Ok(())
    } else {
        Err(HarnessError::Crashed(
            "startup-failed Pi process groups were not reaped".to_owned(),
        ))
    }
}

fn wait_for_process_groups_reap(
    docker: &DockerCli,
    container: &str,
    pgids: &[u32],
    timeout: Duration,
) -> Result<bool, HarnessError> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut any_alive = false;
        for pgid in pgids {
            any_alive |= container_group_alive_with(docker, container, *pgid)?;
        }
        if !any_alive {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(20));
    }
    Ok(false)
}

fn derive_token_pgids(
    docker: &DockerCli,
    container: &str,
    token: &str,
) -> Result<Vec<u32>, HarnessError> {
    let mut command = docker.command();
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
        &harness.processes.docker,
        &harness.config.agent_container,
        pgid,
    )
}

pub(crate) fn terminate_on_drop(
    docker: &DockerCli,
    container: &str,
    process: &ManagedProcess,
    stop_timeout: Duration,
) -> bool {
    let _ = signal_container_group(docker, container, process.pgid, "TERM");
    if wait_for_reap_bounded(docker, container, process, stop_timeout) {
        return true;
    }
    let _ = signal_container_group(docker, container, process.pgid, "KILL");
    if wait_for_reap_bounded(docker, container, process, KILL_REAP_TIMEOUT) {
        return true;
    }
    if let Ok(mut child) = process.child.try_lock() {
        let _ = child.kill();
        let _ = child.try_wait();
    }
    wait_for_reap_bounded(docker, container, process, Duration::from_millis(50))
}

fn wait_for_reap_bounded(
    docker: &DockerCli,
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
            container_group_alive_with(docker, container, process.pgid).unwrap_or(true);
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
#[serde(deny_unknown_fields)]
struct SupervisorControl {
    #[serde(rename = "type")]
    record_type: String,
    token: String,
    pgid: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SupervisorReady {
    #[serde(rename = "type")]
    record_type: String,
    token: String,
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

fn parse_supervisor_ready(ready: &str, expected_token: &str) -> Result<(), String> {
    let ready: SupervisorReady = serde_json::from_str(ready)
        .map_err(|error| format!("invalid supervisor READY confirmation: {error}"))?;
    if ready.record_type != "autospec_ready" || ready.token != expected_token {
        return Err("invalid supervisor READY confirmation".to_owned());
    }
    Ok(())
}

fn supervisor_token() -> Result<String, HarnessError> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|error| {
            HarnessError::Start(format!("failed to create supervisor token: {error}"))
        })?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        token.push(HEX[usize::from(byte >> 4)] as char);
        token.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    Ok(token)
}

fn live_events_path(session: &SessionRef) -> PathBuf {
    Path::new(&session.path).join(format!("pi.events-{}.jsonl", session.id))
}

pub(crate) fn events_path(session: &SessionRef) -> PathBuf {
    live_events_path(session)
}

fn write_packet_once(path: &Path, packet: &TaskPacket) -> Result<(), HarnessError> {
    let bytes = serde_json::to_vec(packet).map_err(|error| HarnessError::Io(error.to_string()))?;
    if let Some(existing) = read_real_file_optional(path)? {
        return if existing == bytes {
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
    if let Some(existing) = read_real_file_optional(path)? {
        return if existing == bytes {
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
    if read_real_file_optional(path)?.is_some() {
        return Ok(());
    }
    atomic_write(path, bytes)
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), HarnessError> {
    let parent = path
        .parent()
        .ok_or_else(|| HarnessError::Io("path has no parent".to_owned()))?;
    let metadata = fs::symlink_metadata(parent).map_err(io_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(HarnessError::Io(format!(
            "atomic write parent is not a real directory: {}",
            parent.display()
        )));
    }
    let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_error)?;
    restrict_private_file(&file)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(&temporary, path).map_err(io_error)
}

pub(crate) fn read_real_file(path: &Path) -> Result<Vec<u8>, HarnessError> {
    let mut file = open_real_file_read(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

fn read_real_file_optional(path: &Path) -> Result<Option<Vec<u8>>, HarnessError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(HarnessError::Start(format!(
            "harness path is not a real file: {}",
            path.display()
        )));
    }
    read_real_file(path).map(Some)
}

pub(crate) fn open_real_file_read(path: &Path) -> Result<File, HarnessError> {
    let expected = fs::symlink_metadata(path).map_err(io_error)?;
    if expected.file_type().is_symlink() || !expected.is_file() {
        return Err(HarnessError::Start(format!(
            "harness path is not a real file: {}",
            path.display()
        )));
    }
    let file = File::open(path).map_err(io_error)?;
    let current = fs::symlink_metadata(path).map_err(io_error)?;
    let opened = file.metadata().map_err(io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if current.file_type().is_symlink()
            || !current.is_file()
            || current.dev() != expected.dev()
            || current.ino() != expected.ino()
            || opened.dev() != expected.dev()
            || opened.ino() != expected.ino()
        {
            return Err(HarnessError::Start(format!(
                "harness file identity changed: {}",
                path.display()
            )));
        }
    }
    Ok(file)
}

fn open_private_append(path: &Path) -> Result<File, HarnessError> {
    let expected = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(HarnessError::Start(format!(
                "harness append path is not a real file: {}",
                path.display()
            )))
        }
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(io_error(error)),
    };
    let mut options = OpenOptions::new();
    options.write(true).append(true);
    if expected.is_some() {
        options.create(false);
    } else {
        options.create_new(true);
    }
    let file = options.open(path).map_err(|error| {
        HarnessError::Start(format!(
            "open private harness file {}: {error}",
            path.display()
        ))
    })?;
    restrict_private_file(&file)?;
    if let Some(expected) = expected {
        let current = fs::symlink_metadata(path).map_err(io_error)?;
        let opened = file.metadata().map_err(io_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if current.file_type().is_symlink()
                || !current.is_file()
                || current.dev() != expected.dev()
                || current.ino() != expected.ino()
                || opened.dev() != expected.dev()
                || opened.ino() != expected.ino()
            {
                return Err(HarnessError::Start(format!(
                    "private harness file identity changed: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(file)
}

fn restrict_private_file(file: &File) -> Result<(), HarnessError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(io_error)?;
    }
    Ok(())
}

pub(crate) fn io_error(error: io::Error) -> HarnessError {
    HarnessError::Io(error.to_string())
}

#[cfg(test)]
mod secret_tests {
    use super::redact_bytes;

    #[test]
    fn credential_is_removed_from_every_occurrence_before_durable_output() {
        let secret = b"0123456789abcdef0123456789abcdef";
        let mut record = [
            b"before ".as_slice(),
            secret,
            b" middle ",
            secret,
            b" after\n",
        ]
        .concat();
        redact_bytes(&mut record, secret);
        assert!(!record.windows(secret.len()).any(|window| window == secret));
        assert_eq!(
            String::from_utf8(record).unwrap(),
            "before [REDACTED_CREDENTIAL] middle [REDACTED_CREDENTIAL] after\n"
        );
    }
}
