use crate::{
    session::{
        atomic_write, events_path, io_error, open_real_file_read, read_real_file, CURSOR_FILE,
    },
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
    fs,
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
    #[serde(default)]
    reducer: ReducerState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReducerState {
    pending_model_error: bool,
    retrying: bool,
    terminal: TerminalState,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TerminalState {
    #[default]
    Active,
    ReviewReady,
    ModelFailed,
}

pub(crate) fn poll_events(
    harness: &PiHarness,
    session: &SessionRef,
) -> Result<Vec<ExecutionEvent>, HarnessError> {
    harness.validate_session(session)?;
    let _storage = harness.acquire_ready_storage()?;
    let session_dir = Path::new(&session.path);
    let cursor_path = session_dir.join(CURSOR_FILE);
    let mut cursors = read_cursors(&cursor_path)?;
    let cursor = cursors.sessions.entry(session.id.to_string()).or_default();
    let expected_events = normalized_absolute(&events_path(session))?;
    if cursor.path.as_os_str().is_empty() {
        cursor.path = expected_events.clone();
    } else if cursor.path != expected_events {
        return Err(HarnessError::InvalidSession(
            "cursor path does not exactly match the session live-event file".to_owned(),
        ));
    }
    if !cursor.path.exists() {
        return Ok(Vec::new());
    }
    let mut file = open_real_file_read(&cursor.path)?;
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
        match normalize(session, &record, &mut cursor.reducer) {
            Normalized::Event(event) => events.push(event),
            Normalized::KnownIgnored => {}
            Normalized::Unknown => {
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

fn normalized_absolute(path: &Path) -> Result<PathBuf, HarnessError> {
    if !path.is_absolute() {
        return Err(HarnessError::InvalidSession(format!(
            "harness path is not absolute: {}",
            path.display()
        )));
    }
    let parent = path.parent().ok_or_else(|| {
        HarnessError::InvalidSession(format!("harness path has no parent: {}", path.display()))
    })?;
    let name = path.file_name().ok_or_else(|| {
        HarnessError::InvalidSession(format!("harness path has no filename: {}", path.display()))
    })?;
    Ok(fs::canonicalize(parent).map_err(io_error)?.join(name))
}

enum Normalized {
    Event(ExecutionEvent),
    KnownIgnored,
    Unknown,
}

fn normalize(session: &SessionRef, record: &Value, reducer: &mut ReducerState) -> Normalized {
    let Some(record_type) = record.get("type").and_then(Value::as_str) else {
        return Normalized::Unknown;
    };
    let (state, kind) = match record_type {
        "agent_start" => {
            *reducer = ReducerState::default();
            (
                ExecutionState::Running,
                ExecutionEventKind::AgentStarted {
                    session_id: SessionId::new(session.id.to_string()),
                },
            )
        }
        "test_run" => (ExecutionState::Running, ExecutionEventKind::TestsStarted),
        "test_failed" => (ExecutionState::Running, ExecutionEventKind::TestsFailed),
        "done" => return settle(session, reducer),
        "agent_settled" => return settle(session, reducer),
        "model_error" => return model_failed(session, reducer),
        "message_end" => {
            if is_assistant_message(record) {
                reducer.pending_model_error = is_model_error(record);
            }
            return Normalized::KnownIgnored;
        }
        "agent_end" => {
            let will_retry = record
                .get("willRetry")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            reducer.retrying = will_retry;
            if !will_retry && reducer.pending_model_error {
                return model_failed(session, reducer);
            }
            return Normalized::KnownIgnored;
        }
        "auto_retry_start" => {
            reducer.retrying = true;
            return Normalized::KnownIgnored;
        }
        "auto_retry_end" => {
            reducer.retrying = false;
            let succeeded = record
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            reducer.pending_model_error = !succeeded;
            return Normalized::KnownIgnored;
        }
        "tool_execution_start" if is_test_tool(record) => {
            (ExecutionState::Running, ExecutionEventKind::TestsStarted)
        }
        "session"
        | "turn_start"
        | "turn_end"
        | "message_start"
        | "message_update"
        | "tool_execution_start"
        | "tool_execution_update"
        | "tool_execution_end"
        | "queue_update"
        | "compaction_start"
        | "compaction_end"
        | "entry_appended"
        | "session_info_changed"
        | "thinking_level_changed"
        | "summarization_retry_scheduled"
        | "summarization_retry_attempt_start"
        | "summarization_retry_finished"
        | "bash_execution_update" => return Normalized::KnownIgnored,
        _ => return Normalized::Unknown,
    };
    Normalized::Event(ExecutionEvent {
        execution_id: session.execution_id.clone(),
        attempt_id: None,
        sequence: 0,
        at: chrono::Utc::now(),
        state,
        kind,
    })
}

fn settle(session: &SessionRef, reducer: &mut ReducerState) -> Normalized {
    if reducer.terminal == TerminalState::ModelFailed {
        return Normalized::KnownIgnored;
    }
    if reducer.pending_model_error || reducer.retrying {
        return model_failed(session, reducer);
    }
    if reducer.terminal == TerminalState::ReviewReady {
        return Normalized::KnownIgnored;
    }
    reducer.terminal = TerminalState::ReviewReady;
    Normalized::Event(event(
        session,
        ExecutionState::ReviewReady,
        ExecutionEventKind::ReviewReady,
    ))
}

fn model_failed(session: &SessionRef, reducer: &mut ReducerState) -> Normalized {
    if reducer.terminal == TerminalState::ModelFailed {
        return Normalized::KnownIgnored;
    }
    reducer.pending_model_error = false;
    reducer.retrying = false;
    reducer.terminal = TerminalState::ModelFailed;
    Normalized::Event(event(
        session,
        ExecutionState::Failed,
        ExecutionEventKind::ExecutionFailed {
            failure: FailureClass::ModelFailed,
        },
    ))
}

fn event(session: &SessionRef, state: ExecutionState, kind: ExecutionEventKind) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: session.execution_id.clone(),
        attempt_id: None,
        sequence: 0,
        at: chrono::Utc::now(),
        state,
        kind,
    }
}

fn is_model_error(record: &Value) -> bool {
    let message = record.get("message").unwrap_or(record);
    message.get("role").and_then(Value::as_str) == Some("assistant")
        && (message.get("stopReason").and_then(Value::as_str) == Some("error")
            || message.get("errorMessage").is_some())
}

fn is_assistant_message(record: &Value) -> bool {
    record
        .get("message")
        .unwrap_or(record)
        .get("role")
        .and_then(Value::as_str)
        == Some("assistant")
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
    let bytes = match fs::symlink_metadata(path) {
        Ok(_) => read_real_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CursorFile::default())
        }
        Err(error) => return Err(io_error(error)),
    };
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
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
        .filter(|path| {
            !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("pi.events-") || name.starts_with("pi.stderr-")
                })
        })
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
        if open_real_file_read(&path)
            .map(BufReader::new)
            .and_then(|mut reader| reader.read_line(&mut first_line).map_err(io_error))
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
