use crate::{
    session::{
        atomic_write, events_path, io_error, open_real_file_read, read_real_file,
        session_process_exited, CURSOR_FILE,
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
    /// Assistant `message_end` records that are not model errors. A settle
    /// record with none of these is not evidence that the agent ran (spec s3:
    /// exit status and settle records are not proof of work). Cursors written
    /// before this field existed deserialize to 0.
    #[serde(default)]
    assistant_turns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TerminalState {
    #[default]
    Active,
    ReviewReady,
    ModelFailed,
    /// Pi settled without a single assistant turn.
    HarnessFailed,
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
    // Sample liveness BEFORE reading: a process seen exited here has written
    // everything it ever will, so the read below cannot miss a final record
    // that lands between the two steps.
    let exited = session_process_exited(harness, session)?;
    let mut bytes = Vec::new();
    if cursor.path.exists() {
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
        file.read_to_end(&mut bytes).map_err(io_error)?;
    }
    let complete: &[u8] = match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(last_newline) => &bytes[..=last_newline],
        None => &[],
    };
    let process_gone = exited && cursor.reducer.terminal == TerminalState::Active;
    if complete.is_empty() && !process_gone {
        return Ok(Vec::new());
    }
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
    if process_gone && cursor.reducer.terminal == TerminalState::Active {
        // Pi is gone and never wrote a terminal record: free the slot now
        // instead of waiting for the inactivity timer.
        if let Normalized::Event(event) = harness_failed(session, &mut cursor.reducer) {
            events.push(event);
        }
    }
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
                if !reducer.pending_model_error {
                    reducer.assistant_turns += 1;
                }
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
    if matches!(
        reducer.terminal,
        TerminalState::ModelFailed | TerminalState::HarnessFailed
    ) {
        return Normalized::KnownIgnored;
    }
    if reducer.pending_model_error || reducer.retrying {
        return model_failed(session, reducer);
    }
    if reducer.assistant_turns == 0 {
        return harness_failed(session, reducer);
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

/// Pi reached a terminal record without ever producing an assistant turn.
/// Until AutoSpec T04 defines `AGENT-NEVER-STARTED` this maps to `HarnessFailed`.
fn harness_failed(session: &SessionRef, reducer: &mut ReducerState) -> Normalized {
    reducer.terminal = TerminalState::HarnessFailed;
    Normalized::Event(event(
        session,
        ExecutionState::Failed,
        ExecutionEventKind::ExecutionFailed {
            failure: FailureClass::HarnessFailed,
        },
    ))
}

fn model_failed(session: &SessionRef, reducer: &mut ReducerState) -> Normalized {
    if matches!(
        reducer.terminal,
        TerminalState::ModelFailed | TerminalState::HarnessFailed
    ) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::ExecutionId;
    use serde_json::json;

    fn session() -> SessionRef {
        SessionRef {
            id: SessionId::new("s"),
            path: "/session".into(),
            execution_id: ExecutionId::new("e"),
            worktree_path: "/workspace".into(),
        }
    }

    /// Feeds records through `normalize` and returns the produced events.
    fn feed(reducer: &mut ReducerState, records: &[Value]) -> Vec<ExecutionEventKind> {
        let session = session();
        records
            .iter()
            .filter_map(|record| match normalize(&session, record, reducer) {
                Normalized::Event(event) => Some(event.kind),
                _ => None,
            })
            .collect()
    }

    fn assistant_ok() -> Value {
        json!({"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"stop"}})
    }

    fn assistant_error() -> Value {
        json!({"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"x"}})
    }

    fn failed_with(kinds: &[ExecutionEventKind], class: FailureClass) -> bool {
        kinds.iter().any(
            |kind| matches!(kind, ExecutionEventKind::ExecutionFailed { failure } if *failure == class),
        )
    }

    fn review_ready_count(kinds: &[ExecutionEventKind]) -> usize {
        kinds
            .iter()
            .filter(|kind| matches!(kind, ExecutionEventKind::ReviewReady))
            .count()
    }

    #[test]
    fn settle_without_an_assistant_turn_is_harness_failed_not_review_ready() {
        for terminal in ["agent_settled", "done"] {
            let mut reducer = ReducerState::default();
            let kinds = feed(
                &mut reducer,
                &[json!({"type":"agent_start"}), json!({"type": terminal})],
            );
            assert!(
                failed_with(&kinds, FailureClass::HarnessFailed),
                "{terminal}"
            );
            assert_eq!(review_ready_count(&kinds), 0, "{terminal}");
        }
    }

    #[test]
    fn one_assistant_turn_before_settle_yields_exactly_one_review_ready() {
        let mut reducer = ReducerState::default();
        let kinds = feed(
            &mut reducer,
            &[
                json!({"type":"agent_start"}),
                assistant_ok(),
                json!({"type":"agent_settled"}),
                json!({"type":"done"}),
            ],
        );
        assert_eq!(review_ready_count(&kinds), 1);
        assert!(!failed_with(&kinds, FailureClass::HarnessFailed));
    }

    #[test]
    fn an_error_only_assistant_turn_does_not_count() {
        let mut reducer = ReducerState::default();
        feed(
            &mut reducer,
            &[json!({"type":"agent_start"}), assistant_error()],
        );
        assert_eq!(reducer.assistant_turns, 0);
    }

    #[test]
    fn failure_is_terminal_so_a_repeated_settle_stays_ignored() {
        let mut reducer = ReducerState::default();
        let kinds = feed(
            &mut reducer,
            &[
                json!({"type":"agent_start"}),
                json!({"type":"agent_settled"}),
                json!({"type":"agent_settled"}),
                json!({"type":"done"}),
            ],
        );
        assert_eq!(kinds.len(), 2); // AgentStarted + one ExecutionFailed
        assert_eq!(reducer.terminal, TerminalState::HarnessFailed);
    }

    #[test]
    fn agent_start_resets_the_turn_counter() {
        let mut reducer = ReducerState::default();
        feed(
            &mut reducer,
            &[json!({"type":"agent_start"}), assistant_ok()],
        );
        assert_eq!(reducer.assistant_turns, 1);
        feed(&mut reducer, &[json!({"type":"agent_start"})]);
        assert_eq!(reducer.assistant_turns, 0);
    }

    #[test]
    fn a_cursor_without_assistant_turns_deserializes_to_zero() {
        let cursor: JsonlCursor = serde_json::from_str(
            r#"{"path":"/session/pi.events.jsonl","offset":7,"reducer":{"pending_model_error":false,"retrying":false,"terminal":"active"}}"#,
        )
        .unwrap();
        assert_eq!(cursor.reducer.assistant_turns, 0);
        let legacy: JsonlCursor = serde_json::from_str(r#"{"path":"/p","offset":0}"#).unwrap();
        assert_eq!(legacy.reducer.assistant_turns, 0);
    }
}
