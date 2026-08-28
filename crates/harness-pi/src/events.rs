use crate::{
    session::{atomic_write, io_error, CURSOR_FILE},
    PiHarness,
};
use harness_traits::{HarnessError, SessionRef};
use orchestrator_core::{
    event::ExecutionEventKind, ExecutionEvent, ExecutionState, FailureClass, SessionId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

#[derive(Debug, Default, Serialize, Deserialize)]
struct CursorFile {
    #[serde(default)]
    sessions: BTreeMap<String, JsonlCursor>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct JsonlCursor {
    path: PathBuf,
    offset: u64,
}

pub(crate) fn poll_events(
    harness: &PiHarness,
    session: &SessionRef,
) -> Result<Vec<ExecutionEvent>, HarnessError> {
    let session_dir = Path::new(&session.path);
    let cursor_path = session_dir.join(CURSOR_FILE);
    let mut cursors = read_cursors(&cursor_path)?;
    let cursor = cursors.sessions.entry(session.id.to_string()).or_default();
    if cursor.path.as_os_str().is_empty() {
        let Some(path) = find_session_file(session_dir, session.id.as_str())? else {
            return Ok(Vec::new());
        };
        cursor.path = path;
    }
    let mut file = File::open(&cursor.path).map_err(io_error)?;
    let length = file.metadata().map_err(io_error)?.len();
    if cursor.offset > length {
        return Err(HarnessError::InvalidSession(format!(
            "cursor {} exceeds JSONL length {length}",
            cursor.offset
        )));
    }
    file.seek(SeekFrom::Start(cursor.offset))
        .map_err(io_error)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(io_error)?;
    let Some(last_newline) = bytes.iter().rposition(|byte| *byte == b'\n') else {
        return Ok(Vec::new());
    };
    let complete = &bytes[..=last_newline];
    let mut events = Vec::new();
    let mut unknown = 0_u64;
    for line in complete
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: Value = serde_json::from_slice(line).map_err(|error| {
            HarnessError::InvalidSession(format!("invalid complete Pi JSONL record: {error}"))
        })?;
        match normalize(session, &record) {
            Some(event) => events.push(event),
            None => {
                unknown += 1;
                tracing::debug!(
                    execution_id = %session.execution_id,
                    pi_record_type = record.get("type").and_then(|value| value.as_str()).unwrap_or("missing"),
                    "ignored unrecognized Pi session record"
                );
            }
        }
    }
    cursor.offset += complete.len() as u64;
    let serialized =
        serde_json::to_vec(&cursors).map_err(|error| HarnessError::Io(error.to_string()))?;
    atomic_write(&cursor_path, &serialized)?;
    harness.unknown_events.fetch_add(unknown, Ordering::Relaxed);
    Ok(events)
}

fn normalize(session: &SessionRef, record: &Value) -> Option<ExecutionEvent> {
    let record_type = record.get("type")?.as_str()?;
    let (state, kind) = match record_type {
        "session" | "turn_start" | "agent_start" => (
            ExecutionState::Running,
            ExecutionEventKind::AgentStarted {
                session_id: SessionId::new(session.id.to_string()),
            },
        ),
        "test_run" => (ExecutionState::Running, ExecutionEventKind::TestsStarted),
        "test_failed" => (ExecutionState::Running, ExecutionEventKind::TestsFailed),
        "done" | "agent_settled" => (ExecutionState::ReviewReady, ExecutionEventKind::ReviewReady),
        "model_error" => (
            ExecutionState::Failed,
            ExecutionEventKind::ExecutionFailed {
                failure: FailureClass::ModelFailed,
            },
        ),
        "message" if is_model_error(record) => (
            ExecutionState::Failed,
            ExecutionEventKind::ExecutionFailed {
                failure: FailureClass::ModelFailed,
            },
        ),
        "tool_execution_start" if is_test_tool(record) => {
            (ExecutionState::Running, ExecutionEventKind::TestsStarted)
        }
        _ => return None,
    };
    Some(ExecutionEvent {
        execution_id: session.execution_id.clone(),
        attempt_id: None,
        sequence: 0,
        at: chrono::Utc::now(),
        state,
        kind,
    })
}

fn is_model_error(record: &Value) -> bool {
    let message = record.get("message").unwrap_or(record);
    message.get("role").and_then(Value::as_str) == Some("assistant")
        && (message.get("stopReason").and_then(Value::as_str) == Some("error")
            || message.get("errorMessage").is_some())
}

fn is_test_tool(record: &Value) -> bool {
    record.get("toolName").and_then(Value::as_str) == Some("bash")
        && record
            .get("args")
            .and_then(|args| args.get("command"))
            .and_then(Value::as_str)
            .is_some_and(|command| command.contains("test"))
}

fn read_cursors(path: &Path) -> Result<CursorFile, HarnessError> {
    if !path.exists() {
        return Ok(CursorFile::default());
    }
    let bytes = fs::read(path).map_err(io_error)?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(CursorFile::default());
    }
    // The initial `{}` deliberately deserializes to an empty cursor map.
    serde_json::from_slice(&bytes).map_err(|error| HarnessError::InvalidSession(error.to_string()))
}

pub(crate) fn find_session_file(
    session_dir: &Path,
    session_id: &str,
) -> Result<Option<PathBuf>, HarnessError> {
    let mut candidates = fs::read_dir(session_dir)
        .map_err(io_error)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .filter(|path| path.file_name().and_then(|name| name.to_str()) != Some("pi.stdout.jsonl"))
        .collect::<Vec<_>>();
    candidates.sort();
    for path in candidates {
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(session_id))
        {
            return Ok(Some(path));
        }
        let mut first_line = String::new();
        if File::open(&path)
            .map(BufReader::new)
            .and_then(|mut reader| reader.read_line(&mut first_line))
            .is_ok()
            && serde_json::from_str::<Value>(&first_line)
                .ok()
                .and_then(|value| value.get("id").and_then(Value::as_str).map(str::to_owned))
                .as_deref()
                == Some(session_id)
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}
