use crate::{
    events::find_session_file,
    session::{
        atomic_write, base_args, io_error, spawn, CONTAINER_SESSION, CONVERSATION_DIR, OWNER_FILE,
        RESUME_COUNT_FILE,
    },
    PiHarness,
};
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::SessionId;
use std::{fs, path::Path};

const MAX_RESUMES: u64 = 3;
const RESUME_PROMPT: &str = "Continue from the persisted session without repeating completed work.";

enum DurableSession {
    Empty,
    Ready(std::path::PathBuf),
}

pub(crate) fn resume(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    validate_owner(harness, session)?;
    let session_dir = Path::new(&session.path);
    let conversation_dir = session_dir.join(CONVERSATION_DIR);
    let count_path = session_dir.join(RESUME_COUNT_FILE);
    let current = fs::read_to_string(&count_path)
        .map_err(io_error)?
        .trim()
        .parse::<u64>()
        .map_err(|error| HarnessError::NotResumable(error.to_string()))?;
    if current >= MAX_RESUMES {
        return Err(HarnessError::NotResumable(format!(
            "resume limit {MAX_RESUMES} exceeded"
        )));
    }
    let source = match find_session_file(&conversation_dir, session.id.as_str())? {
        Some(path) => validate_and_repair(&path)?,
        None => DurableSession::Empty,
    };
    let next = current + 1;
    atomic_write(&count_path, format!("{next}\n").as_bytes())?;
    let mut args = base_args(harness)?;
    match source {
        DurableSession::Ready(path) => {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    HarnessError::NotResumable("durable session filename is invalid".to_owned())
                })?;
            args.extend(["--session".into(), format!("{CONTAINER_SESSION}/{name}")]);
            args.push(RESUME_PROMPT.into());
        }
        DurableSession::Empty => {
            // Fresh-start semantics reload the one materialized packet without
            // touching the already-delivered live-event cursor.
            let packet_path = Path::new(&session.worktree_path).join(".autospec/task-packet.json");
            let bytes = fs::read(&packet_path).map_err(io_error)?;
            serde_json::from_slice::<orchestrator_core::TaskPacket>(&bytes)
                .map_err(|error| HarnessError::NotResumable(error.to_string()))?;
            args.extend(["--session-id".into(), session.id.to_string()]);
            args.push("@/workspace/.autospec/task-packet.json".into());
        }
    }
    spawn(harness, session, args)
}

pub(crate) fn fork_conversation(
    harness: &PiHarness,
    session: &SessionRef,
) -> Result<SessionRef, HarnessError> {
    validate_owner(harness, session)?;
    let session_dir = Path::new(&session.path);
    let conversation_dir = session_dir.join(CONVERSATION_DIR);
    let source = find_session_file(&conversation_dir, session.id.as_str())?
        .ok_or_else(|| HarnessError::NotResumable("source session JSONL is missing".to_owned()))?;
    if !matches!(validate_and_repair(&source)?, DurableSession::Ready(_)) {
        return Err(HarnessError::NotResumable(
            "cannot fork an empty Pi conversation".to_owned(),
        ));
    }
    let fork_id = SessionId::new(format!(
        "{}-fork-{}",
        session.execution_id,
        uuid::Uuid::new_v4().simple()
    ));
    let fork = SessionRef {
        id: fork_id,
        path: session.path.clone(),
        execution_id: session.execution_id.clone(),
        worktree_path: session.worktree_path.clone(),
    };
    let name = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            HarnessError::NotResumable("durable session filename is invalid".to_owned())
        })?;
    let mut args = base_args(harness)?;
    args.extend([
        "--fork".into(),
        format!("{CONTAINER_SESSION}/{name}"),
        "--session-id".into(),
        fork.id.to_string(),
    ]);
    spawn(harness, &fork, args)?;
    Ok(fork)
}

fn validate_owner(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    let bytes = fs::read(Path::new(&session.path).join(OWNER_FILE)).map_err(io_error)?;
    let owner: crate::session::SessionOwner = serde_json::from_slice(&bytes)
        .map_err(|error| HarnessError::NotResumable(error.to_string()))?;
    if owner.execution_id != session.execution_id.as_str()
        || owner.worktree_path != session.worktree_path
        || owner.execution_id != harness.config.labels.execution_id.as_str()
        || owner.labels != harness.config.labels.to_map()
        || owner.worktree_path != harness.config.worktree.display().to_string()
    {
        return Err(HarnessError::NotResumable(
            "session owner does not match execution".to_owned(),
        ));
    }
    Ok(())
}

fn validate_and_repair(path: &Path) -> Result<DurableSession, HarnessError> {
    let bytes = fs::read(path).map_err(io_error)?;
    if bytes.is_empty() {
        return Ok(DurableSession::Empty);
    }
    let complete_len = if bytes.last() == Some(&b'\n') {
        bytes.len()
    } else {
        bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1)
    };
    for line in bytes[..complete_len]
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        serde_json::from_slice::<serde_json::Value>(line).map_err(|error| {
            HarnessError::NotResumable(format!("malformed durable Pi JSONL: {error}"))
        })?;
    }
    if complete_len != bytes.len() {
        fs::write(path, &bytes[..complete_len]).map_err(io_error)?;
    }
    if complete_len == 0 {
        Ok(DurableSession::Empty)
    } else {
        Ok(DurableSession::Ready(path.to_path_buf()))
    }
}
