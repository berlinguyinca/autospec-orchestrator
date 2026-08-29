use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Router,
};
use futures_util::stream::{self, Stream};
use orchestrator_core::{ExecutionEvent, ExecutionId};
use serde::Deserialize;
use std::{collections::VecDeque, convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::broadcast;

use crate::{auth::authorize_api, error::ApiError, state::AppState};

#[derive(Debug, Default, Deserialize)]
struct CursorQuery {
    cursor: Option<u64>,
}

struct StreamState {
    execution_id: ExecutionId,
    cursor: u64,
    pending: VecDeque<ExecutionEvent>,
    receiver: broadcast::Receiver<ExecutionEvent>,
    events: Arc<dyn orchestrator_persistence::EventLog>,
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/executions/{id}/events", get(events))
}

async fn events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<CursorQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authorize_api(&state, &headers)?;
    let execution_id = ExecutionId::new(id);
    state
        .executions
        .get(&execution_id)
        .await
        .map_err(ApiError::store)?;
    let header_cursor = headers
        .get("last-event-id")
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| ApiError::validation("Last-Event-ID must be an unsigned integer"))
        })
        .transpose()?;
    let cursor = header_cursor.or(query.cursor).unwrap_or(0);
    let receiver = state.event_tx.subscribe();
    let pending = state
        .events
        .since(&execution_id, cursor)
        .await
        .map_err(ApiError::store)?
        .into();
    let stream_state = StreamState {
        execution_id,
        cursor,
        pending,
        receiver,
        events: state.events.clone(),
    };
    let stream = stream::unfold(stream_state, next_event);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn next_event(mut state: StreamState) -> Option<(Result<Event, Infallible>, StreamState)> {
    loop {
        if let Some(event) = state.pending.pop_front() {
            if event.sequence <= state.cursor {
                continue;
            }
            state.cursor = event.sequence;
            return Some((Ok(to_sse(&event)), state));
        }
        match tokio::time::timeout(Duration::from_millis(500), state.receiver.recv()).await {
            Err(_) => {
                if let Ok(events) = state.events.since(&state.execution_id, state.cursor).await {
                    state.pending.extend(events);
                }
            }
            Ok(Ok(event))
                if event.execution_id == state.execution_id && event.sequence > state.cursor =>
            {
                state.cursor = event.sequence;
                return Some((Ok(to_sse(&event)), state));
            }
            Ok(Ok(_)) => {}
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                if let Ok(events) = state.events.since(&state.execution_id, state.cursor).await {
                    state.pending.extend(events);
                }
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => return None,
        }
    }
}

fn to_sse(event: &ExecutionEvent) -> Event {
    Event::default()
        .id(event.sequence.to_string())
        .event("execution")
        .json_data(event)
        .expect("ExecutionEvent serialization is infallible")
}
