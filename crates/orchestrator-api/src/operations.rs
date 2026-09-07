use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use orchestrator_core::{AttemptId, Execution, ExecutionId, ExecutionState, Role, WorkerId};
use serde::{Deserialize, Serialize};

use crate::{auth::authorize_api, error::ApiError, state::AppState};

#[derive(Debug, Deserialize)]
struct OperationsQuery {
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct OperationalExecution {
    id: ExecutionId,
    role: Role,
    state: ExecutionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_id: Option<WorkerId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<AttemptId>,
    updated_at: DateTime<Utc>,
}

impl From<Execution> for OperationalExecution {
    fn from(execution: Execution) -> Self {
        Self {
            id: execution.id,
            role: execution.role,
            state: execution.state,
            worker_id: execution.worker_id,
            attempt_id: execution.attempt_id,
            updated_at: execution.updated_at,
        }
    }
}

#[derive(Debug, Default, Serialize)]
struct QueueSummary {
    queued: u64,
    worker_assigned: u64,
    provisioning: u64,
    running: u64,
    paused_for_human: u64,
    review_ready: u64,
    total: u64,
}

#[derive(Debug, Serialize)]
struct CleanupHealth {
    unresolved: u64,
    cleanup_pending: u64,
    retained: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    oldest_unresolved_at: Option<DateTime<Utc>>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/operator/executions", get(executions))
        .route("/operator/queue", get(queue))
        .route("/operator/cleanup-health", get(cleanup_health))
}

async fn executions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<OperationsQuery>,
) -> Result<Json<Vec<OperationalExecution>>, ApiError> {
    authorize_api(&state, &headers)?;
    let limit = validated_limit(query.limit)?;
    state
        .executions
        .list_operational(limit)
        .await
        .map(|executions| {
            Json(
                executions
                    .into_iter()
                    .map(OperationalExecution::from)
                    .collect(),
            )
        })
        .map_err(ApiError::store)
}

async fn queue(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<QueueSummary>, ApiError> {
    authorize_api(&state, &headers)?;
    state
        .executions
        .live_state_counts()
        .await
        .map(summarize_counts)
        .map(Json)
        .map_err(ApiError::store)
}

async fn cleanup_health(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<CleanupHealth>, ApiError> {
    authorize_api(&state, &headers)?;
    let cleanup = state.cleanup_authorities.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "CLEANUP_UNAVAILABLE",
            "durable cleanup reconciliation is unavailable",
        )
    })?;
    cleanup
        .health_snapshot()
        .await
        .map(|snapshot| {
            Json(CleanupHealth {
                unresolved: snapshot.unresolved,
                cleanup_pending: snapshot.cleanup_pending,
                retained: snapshot.retained,
                oldest_unresolved_at: snapshot.oldest_unresolved_at,
            })
        })
        .map_err(ApiError::store)
}

fn validated_limit(limit: Option<u32>) -> Result<u32, ApiError> {
    let limit = limit.unwrap_or(50);
    if (1..=100).contains(&limit) {
        Ok(limit)
    } else {
        Err(ApiError::validation("limit must be between 1 and 100"))
    }
}

#[cfg(test)]
fn summarize_queue(executions: impl IntoIterator<Item = Execution>) -> QueueSummary {
    summarize_counts(executions.into_iter().fold(
        Vec::<(ExecutionState, u64)>::new(),
        |mut counts, item| {
            if let Some((_, count)) = counts.iter_mut().find(|(state, _)| *state == item.state) {
                *count += 1;
            } else {
                counts.push((item.state, 1));
            }
            counts
        },
    ))
}

fn summarize_counts(counts: Vec<(ExecutionState, u64)>) -> QueueSummary {
    let mut summary = QueueSummary::default();
    for (state, count) in counts {
        match state {
            ExecutionState::Queued => summary.queued += count,
            ExecutionState::WorkerAssigned => summary.worker_assigned += count,
            ExecutionState::Provisioning => summary.provisioning += count,
            ExecutionState::Running => summary.running += count,
            ExecutionState::PausedForHuman => summary.paused_for_human += count,
            ExecutionState::ReviewReady => summary.review_ready += count,
            ExecutionState::Completed | ExecutionState::Failed | ExecutionState::Cancelled => {}
        }
        if !state.is_terminal() {
            summary.total += count;
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orchestrator_core::{Execution, ExecutionState};

    fn execution(state: ExecutionState) -> Execution {
        serde_json::from_value(serde_json::json!({
            "id": "operator-projection-1",
            "role": "implementation",
            "state": state,
            "manifest": {
                "apiVersion": "autospec.dev/v1alpha1",
                "role": "implementation",
                "repository": {"repo": "owner/repository", "baseRef": "main"},
                "agent": {
                    "harness": "pi",
                    "modelPolicy": {"provider": "inferweave"}
                },
                "runtime": {"type": "docker"},
                "task_packet": {"goal": "must-not-leak"}
            },
            "worker_id": "worker-1",
            "labels": {
                "execution_id": "operator-projection-1",
                "worker_id": "worker-1",
                "repository": "owner/repository"
            },
            "created_at": Utc::now(),
            "updated_at": Utc::now()
        }))
        .unwrap()
    }

    #[test]
    fn execution_projection_is_bounded_metadata_without_manifest_or_prompt() {
        let value = serde_json::to_value(OperationalExecution::from(execution(
            ExecutionState::Running,
        )))
        .unwrap();
        assert_eq!(value["id"], "operator-projection-1");
        assert_eq!(value["state"], "RUNNING");
        assert!(value.get("manifest").is_none());
        assert!(value.get("task_packet").is_none());
        assert!(!value.to_string().contains("must-not-leak"));
    }

    #[test]
    fn operator_limit_rejects_zero_and_values_above_one_hundred() {
        assert_eq!(validated_limit(None).ok(), Some(50));
        assert_eq!(validated_limit(Some(1)).ok(), Some(1));
        assert_eq!(validated_limit(Some(100)).ok(), Some(100));
        assert!(validated_limit(Some(0)).is_err());
        assert!(validated_limit(Some(101)).is_err());
    }

    #[test]
    fn queue_projection_counts_execution_states_without_policy() {
        let summary = summarize_queue([
            execution(ExecutionState::Queued),
            execution(ExecutionState::Queued),
            execution(ExecutionState::Running),
        ]);
        assert_eq!(summary.queued, 2);
        assert_eq!(summary.running, 1);
        assert_eq!(summary.total, 3);
    }
}
