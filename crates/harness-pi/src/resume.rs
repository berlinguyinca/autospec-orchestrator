use crate::{
    events::find_session_file,
    session::{atomic_write, base_args, io_error, spawn, OWNER_FILE, RESUME_COUNT_FILE},
    PiHarness,
};
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::SessionId;
use std::{fs, path::Path};

const MAX_RESUMES: u64 = 3;
const RESUME_PROMPT: &str = "Continue from the persisted session without repeating completed work.";

pub(crate) fn resume(harness: &PiHarness, session: &SessionRef) -> Result<(), HarnessError> {
    validate_owner(session)?;
    let session_dir = Path::new(&session.path);
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
    let source = find_session_file(session_dir, session.id.as_str())?;
    if let Some(path) = source.as_ref() {
        truncate_torn_tail(path)?;
    }
    let next = current + 1;
    atomic_write(&count_path, format!("{next}\n").as_bytes())?;
    let mut args = base_args(harness, session_dir)?;
    match source {
        Some(path) => args.extend(["--session".into(), path.display().to_string()]),
        None => args.extend(["--session-id".into(), session.id.to_string()]),
    }
    args.push(RESUME_PROMPT.into());
    spawn(harness, session, args)
}

pub(crate) fn fork_conversation(
    harness: &PiHarness,
    session: &SessionRef,
) -> Result<SessionRef, HarnessError> {
    validate_owner(session)?;
    let session_dir = Path::new(&session.path);
    let source = find_session_file(session_dir, session.id.as_str())?
        .ok_or_else(|| HarnessError::NotResumable("source session JSONL is missing".to_owned()))?;
    truncate_torn_tail(&source)?;
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
    let mut args = base_args(harness, session_dir)?;
    args.extend([
        "--fork".into(),
        source.display().to_string(),
        "--session-id".into(),
        fork.id.to_string(),
    ]);
    spawn(harness, &fork, args)?;
    Ok(fork)
}

fn validate_owner(session: &SessionRef) -> Result<(), HarnessError> {
    let bytes = fs::read(Path::new(&session.path).join(OWNER_FILE)).map_err(io_error)?;
    let owner: crate::session::SessionOwner = serde_json::from_slice(&bytes)
        .map_err(|error| HarnessError::NotResumable(error.to_string()))?;
    if owner.execution_id != session.execution_id.as_str()
        || owner.worktree_path != session.worktree_path
    {
        return Err(HarnessError::NotResumable(
            "session owner does not match execution".to_owned(),
        ));
    }
    Ok(())
}

fn truncate_torn_tail(path: &Path) -> Result<(), HarnessError> {
    let bytes = fs::read(path).map_err(io_error)?;
    if bytes.is_empty() || bytes.last() == Some(&b'\n') {
        return Ok(());
    }
    let complete_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    fs::write(path, &bytes[..complete_len]).map_err(io_error)
}
