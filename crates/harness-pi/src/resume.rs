use crate::{
    events::find_session_file,
    session::{
        atomic_write, base_args, prepare_launch, read_real_file, spawn, CONTAINER_SESSION,
        CONVERSATION_DIR, OWNER_FILE, RESUME_COUNT_FILE,
    },
    PiHarness,
};
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::SessionId;
use std::path::Path;

const MAX_RESUMES: u64 = 3;
const RESUME_PROMPT: &str = "Continue from the persisted session without repeating completed work.";

enum DurableSession {
    Empty,
    Ready(std::path::PathBuf),
}

pub(crate) fn resume(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    resume_with_budget(harness, session, true)
}

pub(crate) fn resume_interactive(
    harness: &PiHarness,
    session: &SessionRef,
) -> Result<(), HarnessError> {
    resume_with_budget(harness, session, false)
}

fn resume_with_budget(
    harness: &PiHarness,
    session: &SessionRef,
    consume_automatic_budget: bool,
) -> Result<(), HarnessError> {
    let storage = prepare_launch(harness)?;
    harness.validate_session(session)?;
    validate_owner(harness, session)?;
    let session_dir = Path::new(&session.path);
    let conversation_dir = session_dir.join(CONVERSATION_DIR);
    let count_path = session_dir.join(RESUME_COUNT_FILE);
    let current = String::from_utf8(read_real_file(&count_path)?)
        .map_err(|error| HarnessError::NotResumable(error.to_string()))?
        .trim()
        .parse::<u64>()
        .map_err(|error| HarnessError::NotResumable(error.to_string()))?;
    if consume_automatic_budget && current >= MAX_RESUMES {
        return Err(HarnessError::NotResumable(format!(
            "resume limit {MAX_RESUMES} exceeded"
        )));
    }
    let source = match find_session_file(&conversation_dir, session.id.as_str())? {
        Some(path) => validate_and_repair(&path)?,
        None if consume_automatic_budget => DurableSession::Empty,
        None => {
            return Err(HarnessError::NotResumable(
                "interactive resume requires an existing native Pi conversation".to_owned(),
            ))
        }
    };
    if !consume_automatic_budget && matches!(source, DurableSession::Empty) {
        return Err(HarnessError::NotResumable(
            "interactive resume requires a non-empty native Pi conversation".to_owned(),
        ));
    }
    if consume_automatic_budget {
        let next = current + 1;
        atomic_write(&count_path, format!("{next}\n").as_bytes())?;
    }
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
            let bytes = read_real_file(&packet_path)?;
            serde_json::from_slice::<orchestrator_core::TaskPacket>(&bytes)
                .map_err(|error| HarnessError::NotResumable(error.to_string()))?;
            args.extend(["--session-id".into(), session.id.to_string()]);
            args.push("@/workspace/.autospec/task-packet.json".into());
        }
    }
    spawn(harness, session, args, storage)
}

pub(crate) fn fork_conversation(
    harness: &PiHarness,
    session: &SessionRef,
) -> Result<SessionRef, HarnessError> {
    let fork_id = SessionId::new(format!(
        "{}-fork-{}",
        session.execution_id,
        uuid::Uuid::new_v4().simple()
    ));
    fork_conversation_as(harness, session, &fork_id)
}

pub(crate) fn fork_conversation_as(
    harness: &PiHarness,
    session: &SessionRef,
    fork_id: &SessionId,
) -> Result<SessionRef, HarnessError> {
    let storage = prepare_launch(harness)?;
    harness.validate_session(session)?;
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
    let fork = SessionRef {
        id: fork_id.clone(),
        path: session.path.clone(),
        execution_id: session.execution_id.clone(),
        worktree_path: session.worktree_path.clone(),
    };
    if let Some(existing) = find_session_file(&conversation_dir, fork.id.as_str())? {
        if matches!(validate_and_repair(&existing)?, DurableSession::Ready(_)) {
            return Ok(fork);
        }
        return Err(HarnessError::NotResumable(
            "fenced fork target exists without a durable conversation".to_owned(),
        ));
    }
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
    spawn(harness, &fork, args, storage)?;
    Ok(fork)
}

fn validate_owner(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    let bytes = read_real_file(&Path::new(&session.path).join(OWNER_FILE))?;
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
    let bytes = read_real_file(path)?;
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
        atomic_write(path, &bytes[..complete_len])?;
    }
    if complete_len == 0 {
        Ok(DurableSession::Empty)
    } else {
        Ok(DurableSession::Ready(path.to_path_buf()))
    }
}
