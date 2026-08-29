use axum::{
    extract::{rejection::QueryRejection, Path, Query, State},
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
    needs_fetch: bool,
}

const EVENT_BATCH_SIZE: usize = 128;

pub fn routes() -> Router<AppState> {
    Router::new().route("/executions/{id}/events", get(events))
}

async fn events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    query: Result<Query<CursorQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authorize_api(&state, &headers)?;
    let Query(query) =
        query.map_err(|_| ApiError::validation("cursor must be an unsigned integer"))?;
    let execution_id = ExecutionId::new(id);
    state
        .executions
        .get(&execution_id)
        .await
        .map_err(|error| ApiError::store_for(error, execution_id.as_str()))?;
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
    if i64::try_from(cursor).is_err() {
        return Err(ApiError::validation(
            "event cursor exceeds the supported range",
        ));
    }
    let receiver = state.event_tx.subscribe();
    let pending = state
        .events
        .since_batch(&execution_id, cursor, EVENT_BATCH_SIZE)
        .await
        .map_err(|error| ApiError::store_for(error, execution_id.as_str()))?;
    let needs_fetch = pending.len() == EVENT_BATCH_SIZE;
    let stream_state = StreamState {
        execution_id,
        cursor,
        pending: pending.into(),
        receiver,
        events: state.events.clone(),
        needs_fetch,
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
        if state.needs_fetch {
            match state
                .events
                .since_batch(&state.execution_id, state.cursor, EVENT_BATCH_SIZE)
                .await
            {
                Ok(events) => {
                    state.needs_fetch = events.len() == EVENT_BATCH_SIZE;
                    state.pending.extend(events);
                    continue;
                }
                Err(error) => {
                    tracing::error!(
                        execution_id = %state.execution_id,
                        cursor = state.cursor,
                        %error,
                        "durable SSE replay failed; terminating stream for client reconnect"
                    );
                    return None;
                }
            }
        }
        match tokio::time::timeout(Duration::from_millis(500), state.receiver.recv()).await {
            Err(_) => state.needs_fetch = true,
            Ok(Ok(_)) => state.needs_fetch = true,
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => state.needs_fetch = true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_persistence::{EventLog, StoreError};

    struct FailingEvents;

    impl EventLog for FailingEvents {
        fn append<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _: &'life1 ExecutionEvent,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<u64, StoreError>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { unreachable!() })
        }

        fn since<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _: &'life1 ExecutionId,
            _: u64,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Vec<ExecutionEvent>, StoreError>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Err(StoreError::SequenceConflict) })
        }
    }

    #[tokio::test]
    async fn durable_store_failure_terminates_stream_for_reconnect() {
        let (sender, receiver) = broadcast::channel(1);
        let state = StreamState {
            execution_id: ExecutionId::new("failed-stream"),
            cursor: 41,
            pending: VecDeque::new(),
            receiver,
            events: Arc::new(FailingEvents),
            needs_fetch: true,
        };
        assert!(next_event(state).await.is_none());
        drop(sender);
    }
}
