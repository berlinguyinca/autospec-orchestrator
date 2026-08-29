//! Durable PostgreSQL storage for execution-plane records (spec sections 32, 49, 61, 74).

mod artifacts;
mod cleanup;
mod error;
mod event_log;
mod reservations;
mod workers;

pub use artifacts::{Artifact, ArtifactStore, PgArtifactStore};
pub use cleanup::{
    CleanupAuthority, CleanupAuthorityStore, CleanupDisposition, CleanupHealthSnapshot,
    CleanupStage, PgCleanupAuthorityStore,
};
pub use error::StoreError;
pub use event_log::{EventLog, PgEventLog};
pub use reservations::{LostWorkerRecovery, PgReservationStore, Reservation, ReservationStore};
pub use workers::{PgWorkerStore, WorkerStore};

use async_trait::async_trait;
use event_log::append_in_transaction;
use orchestrator_core::{
    event::ExecutionEventKind, AttemptId, Execution, ExecutionAttachment, ExecutionControlAction,
    ExecutionControlRequest, ExecutionEvent, ExecutionId, ExecutionResult, ExecutionState,
    FailureClass, OwnershipLabels, Role, SessionId, WorkerId,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

/// Durable execution record access shared by the controller and recovery paths.
#[async_trait]
pub trait ExecutionStore: Send + Sync {
    async fn insert(&self, execution: &Execution) -> Result<(), StoreError>;
    async fn get(&self, id: &ExecutionId) -> Result<Execution, StoreError>;
    async fn list_live(&self) -> Result<Vec<Execution>, StoreError>;
    /// Bounded metadata source for operator surfaces. Implementations should
    /// apply `limit` in the backing store rather than materializing all rows.
    async fn list_operational(&self, limit: u32) -> Result<Vec<Execution>, StoreError> {
        let mut executions = self.list_live().await?;
        executions.truncate(limit as usize);
        Ok(executions)
    }
    /// Exact live-state counts used by queue health. This reports execution
    /// state only; retry and prioritization policy remain outside this crate.
    async fn live_state_counts(&self) -> Result<Vec<(ExecutionState, u64)>, StoreError> {
        let mut counts = std::collections::BTreeMap::<String, (ExecutionState, u64)>::new();
        for execution in self.list_live().await? {
            let key = enum_text(&execution.state)?;
            counts
                .entry(key)
                .and_modify(|(_, count)| *count += 1)
                .or_insert((execution.state, 1));
        }
        Ok(counts.into_values().collect())
    }
    async fn request_cancellation(&self, id: &ExecutionId) -> Result<Execution, StoreError> {
        Err(StoreError::Conflict(format!(
            "durable cancellation requests are unavailable for {id}"
        )))
    }
    async fn cancellation_requested(&self, _id: &ExecutionId) -> Result<bool, StoreError> {
        Ok(false)
    }
    async fn list_pending_cancellations(&self) -> Result<Vec<PendingCancellation>, StoreError> {
        Ok(Vec::new())
    }
    async fn complete_cancellation(
        &self,
        id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<(Execution, u64), StoreError> {
        let mut execution = self.get(id).await?;
        execution
            .transition(ExecutionState::Cancelled)
            .map_err(|_| StoreError::IllegalTransition {
                from: execution.state,
                to: ExecutionState::Cancelled,
            })?;
        execution.result = Some(cancelled_result(id));
        let event = ExecutionEvent {
            execution_id: id.clone(),
            attempt_id: Some(attempt_id.clone()),
            sequence: 0,
            at: chrono::Utc::now(),
            state: ExecutionState::Cancelled,
            kind: ExecutionEventKind::ExecutionCancelled,
        };
        let sequence = self.record_progress(&execution, &event).await?;
        Ok((execution, sequence))
    }
    async fn transition(
        &self,
        id: &ExecutionId,
        next: ExecutionState,
    ) -> Result<Execution, StoreError>;
    async fn transition_with_event(
        &self,
        _id: &ExecutionId,
        _next: ExecutionState,
        _event: &orchestrator_core::ExecutionEvent,
    ) -> Result<(Execution, u64), StoreError> {
        Err(StoreError::Conflict(
            "atomic transition events are unavailable for this store".to_owned(),
        ))
    }
    async fn record_progress(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<u64, StoreError>;
    async fn record_progress_and_request_retention(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<u64, StoreError> {
        self.record_progress(execution, event).await
    }
    async fn create_idempotent(
        &self,
        _execution: &Execution,
        _event: &orchestrator_core::ExecutionEvent,
        _idempotency_key: &str,
        _request_scope: &str,
    ) -> Result<IdempotentExecution, StoreError> {
        Err(StoreError::Conflict(
            "idempotent creation is unavailable for this store".to_owned(),
        ))
    }
    async fn request_control(
        &self,
        id: &ExecutionId,
        action: ExecutionControlAction,
        idempotency_key: &str,
    ) -> Result<IdempotentExecutionControl, StoreError> {
        Err(StoreError::Conflict(format!(
            "durable interactive controls are unavailable for {id}: {action:?} {idempotency_key}"
        )))
    }
    async fn list_pending_controls(
        &self,
        _worker_id: &WorkerId,
    ) -> Result<Vec<PendingExecutionControl>, StoreError> {
        Ok(Vec::new())
    }
    async fn begin_control(
        &self,
        _request_id: i64,
        _worker_id: &WorkerId,
        _attempt_id: &AttemptId,
    ) -> Result<(), StoreError> {
        Err(StoreError::Conflict(
            "durable interactive control phases are unavailable".to_owned(),
        ))
    }
    async fn mark_control_side_effect_applied(
        &self,
        _request_id: i64,
        _session_id: Option<&SessionId>,
    ) -> Result<(), StoreError> {
        Err(StoreError::Conflict(
            "durable interactive control phases are unavailable".to_owned(),
        ))
    }
    async fn complete_control(
        &self,
        _request_id: i64,
        _execution: &Execution,
        _event: &ExecutionEvent,
    ) -> Result<Option<u64>, StoreError> {
        Err(StoreError::Conflict(
            "durable interactive controls are unavailable".to_owned(),
        ))
    }
    async fn attachment_snapshot(
        &self,
        id: &ExecutionId,
    ) -> Result<ExecutionAttachment, StoreError> {
        Err(StoreError::Conflict(format!(
            "atomic attachment snapshots are unavailable for {id}"
        )))
    }
}

#[derive(Debug, Clone)]
pub struct IdempotentExecution {
    pub execution: Execution,
    pub created: bool,
    pub event_sequence: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PendingCancellation {
    pub execution: Execution,
    pub worker_id: Option<WorkerId>,
    pub attempt_id: Option<AttemptId>,
}

#[derive(Debug, Clone)]
pub struct IdempotentExecutionControl {
    pub request: ExecutionControlRequest,
    pub created: bool,
}

#[derive(Debug, Clone)]
pub struct PendingExecutionControl {
    pub request: ExecutionControlRequest,
    pub execution: Execution,
    pub phase: ExecutionControlPhase,
    pub accepted_worker_id: WorkerId,
    pub accepted_attempt_id: AttemptId,
    pub source_session_id: SessionId,
    pub worktree_path: String,
    pub accepted_version: i64,
    pub accepted_state: ExecutionState,
    pub target_session_id: Option<SessionId>,
    pub side_effect_session_id: Option<SessionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionControlPhase {
    Accepted,
    Applying,
    SideEffectApplied,
}

#[derive(Debug, Clone)]
pub struct PgExecutionStore {
    pool: PgPool,
}

impl PgExecutionStore {
    /// Connects to the controller database and applies this crate's migrations.
    pub async fn connect(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        run_migrations(&pool).await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ExecutionStore for PgExecutionStore {
    async fn insert(&self, execution: &Execution) -> Result<(), StoreError> {
        let result = execution.result.as_ref().map(to_json).transpose()?;
        sqlx::query(
            "INSERT INTO executions \
             (id, role, state, manifest, worker_id, attempt_id, session_id, worktree_path, \
              labels, result, created_at, updated_at, version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1)",
        )
        .bind(execution.id.as_str())
        .bind(enum_text(&execution.role)?)
        .bind(enum_text(&execution.state)?)
        .bind(to_json(&execution.manifest)?)
        .bind(execution.worker_id.as_ref().map(WorkerId::as_str))
        .bind(execution.attempt_id.as_ref().map(AttemptId::as_str))
        .bind(execution.session_id.as_ref().map(SessionId::as_str))
        .bind(execution.worktree_path.as_deref())
        .bind(to_json(&execution.labels)?)
        .bind(result)
        .bind(execution.created_at)
        .bind(execution.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_conflict)?;
        Ok(())
    }

    async fn get(&self, id: &ExecutionId) -> Result<Execution, StoreError> {
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1")
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        decode_execution(&row)
    }

    async fn list_live(&self) -> Result<Vec<Execution>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM executions \
             WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') \
             ORDER BY created_at, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_execution).collect()
    }

    async fn list_operational(&self, limit: u32) -> Result<Vec<Execution>, StoreError> {
        let rows = sqlx::query(
            "SELECT * FROM executions \
             WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') \
             ORDER BY created_at, id LIMIT $1",
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_execution).collect()
    }

    async fn live_state_counts(&self) -> Result<Vec<(ExecutionState, u64)>, StoreError> {
        sqlx::query(
            "SELECT state, COUNT(*) AS count FROM executions \
             WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') GROUP BY state ORDER BY state",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            let state = enum_from_text::<ExecutionState>(row.try_get("state")?)?;
            let count = u64::try_from(row.try_get::<i64, _>("count")?)
                .map_err(|_| StoreError::Conflict("negative execution count".to_owned()))?;
            Ok((state, count))
        })
        .collect()
    }

    async fn request_cancellation(&self, id: &ExecutionId) -> Result<Execution, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let mut execution = decode_execution(&row)?;
        let existing_request = sqlx::query(
            "SELECT completed_at FROM execution_cancellation_requests WHERE execution_id = $1",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if execution.state == ExecutionState::Cancelled && existing_request.is_some() {
            transaction.commit().await?;
            return Ok(execution);
        }
        if execution.state.is_terminal() {
            return Err(StoreError::IllegalTransition {
                from: execution.state,
                to: ExecutionState::Cancelled,
            });
        }
        sqlx::query(
            "INSERT INTO execution_cancellation_requests (execution_id) VALUES ($1) \
             ON CONFLICT (execution_id) DO NOTHING",
        )
        .bind(id.as_str())
        .execute(&mut *transaction)
        .await?;
        stale_controls_for_execution(
            &mut transaction,
            id,
            "execution cancellation took precedence",
        )
        .await?;
        if execution.state == ExecutionState::Queued
            && execution.worker_id.is_none()
            && execution.attempt_id.is_none()
        {
            execution
                .transition(ExecutionState::Cancelled)
                .map_err(|_| StoreError::IllegalTransition {
                    from: ExecutionState::Queued,
                    to: ExecutionState::Cancelled,
                })?;
            execution.result = Some(cancelled_result(id));
            sqlx::query(
                "UPDATE executions SET state = 'CANCELLED', result = $2, updated_at = $3, \
                 version = version + 1 WHERE id = $1",
            )
            .bind(id.as_str())
            .bind(to_json(
                execution.result.as_ref().expect("cancelled result exists"),
            )?)
            .bind(execution.updated_at)
            .execute(&mut *transaction)
            .await?;
            let event = ExecutionEvent {
                execution_id: id.clone(),
                attempt_id: None,
                sequence: 0,
                at: execution.updated_at,
                state: ExecutionState::Cancelled,
                kind: ExecutionEventKind::ExecutionCancelled,
            };
            append_in_transaction(&mut transaction, &event).await?;
            sqlx::query(
                "UPDATE execution_cancellation_requests SET completed_at = $2 \
                 WHERE execution_id = $1 AND completed_at IS NULL",
            )
            .bind(id.as_str())
            .bind(execution.updated_at)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(execution)
    }

    async fn cancellation_requested(&self, id: &ExecutionId) -> Result<bool, StoreError> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution_cancellation_requests \
             WHERE execution_id = $1 AND completed_at IS NULL)",
        )
        .bind(id.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    async fn list_pending_cancellations(&self) -> Result<Vec<PendingCancellation>, StoreError> {
        let rows = sqlx::query(
            "SELECT e.*, COALESCE(c.worker_id, e.worker_id) AS cancellation_worker_id, \
             COALESCE(c.attempt_id, e.attempt_id) AS cancellation_attempt_id \
             FROM executions e JOIN execution_cancellation_requests r ON r.execution_id = e.id \
             LEFT JOIN cleanup_authorities c ON c.execution_id = e.id \
             WHERE r.completed_at IS NULL \
             ORDER BY r.requested_at, e.id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(PendingCancellation {
                    execution: decode_execution(row)?,
                    worker_id: row
                        .try_get::<Option<String>, _>("cancellation_worker_id")?
                        .map(WorkerId::new),
                    attempt_id: row
                        .try_get::<Option<String>, _>("cancellation_attempt_id")?
                        .map(AttemptId::new),
                })
            })
            .collect()
    }

    async fn complete_cancellation(
        &self,
        id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<(Execution, u64), StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let request = sqlx::query(
            "SELECT requested_at FROM execution_cancellation_requests \
             WHERE execution_id = $1 AND completed_at IS NULL FOR UPDATE",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if request.is_none() {
            return Err(StoreError::Conflict(format!(
                "execution {id} has no pending cancellation request"
            )));
        }
        let mut execution = decode_execution(&row)?;
        let from = execution.state;
        execution
            .transition(ExecutionState::Cancelled)
            .map_err(|_| StoreError::IllegalTransition {
                from,
                to: ExecutionState::Cancelled,
            })?;
        let reservation_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM reservations WHERE execution_id = $1)",
        )
        .bind(id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        let attempt_finished = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM execution_attempts \
             WHERE execution_id = $1 AND attempt_id = $2 AND finished_at IS NOT NULL)",
        )
        .bind(id.as_str())
        .bind(attempt_id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        let cleanup_resolved = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM cleanup_authorities \
             WHERE execution_id = $1 AND attempt_id = $2 AND phase = 'RESOLVED')",
        )
        .bind(id.as_str())
        .bind(attempt_id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        if reservation_exists || !attempt_finished || !cleanup_resolved {
            return Err(StoreError::Conflict(format!(
                "execution {id} cancellation cannot complete before cleanup and reservation release"
            )));
        }
        execution.result = Some(cancelled_result(id));
        sqlx::query(
            "UPDATE executions SET state = 'CANCELLED', result = $2, updated_at = $3, \
             version = version + 1 WHERE id = $1",
        )
        .bind(id.as_str())
        .bind(to_json(
            execution.result.as_ref().expect("cancelled result exists"),
        )?)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE execution_attempts SET state = 'CANCELLED', result = $3, updated_at = $4 \
             WHERE execution_id = $1 AND attempt_id = $2",
        )
        .bind(id.as_str())
        .bind(attempt_id.as_str())
        .bind(to_json(
            execution.result.as_ref().expect("cancelled result exists"),
        )?)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        let event = ExecutionEvent {
            execution_id: id.clone(),
            attempt_id: Some(attempt_id.clone()),
            sequence: 0,
            at: execution.updated_at,
            state: ExecutionState::Cancelled,
            kind: ExecutionEventKind::ExecutionCancelled,
        };
        let sequence = append_in_transaction(&mut transaction, &event).await?;
        sqlx::query(
            "UPDATE execution_cancellation_requests SET completed_at = $2 \
             WHERE execution_id = $1 AND completed_at IS NULL",
        )
        .bind(id.as_str())
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok((execution, sequence))
    }

    async fn transition(
        &self,
        id: &ExecutionId,
        next: ExecutionState,
    ) -> Result<Execution, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let mut execution = decode_execution(&row)?;
        if cancellation_pending(&mut transaction, id).await? {
            return Err(StoreError::Conflict(format!(
                "execution {id} has a pending cancellation request"
            )));
        }
        let from = execution.state;
        if !from.can_transition_to(next) {
            return Err(StoreError::IllegalTransition { from, to: next });
        }
        execution
            .transition(next)
            .map_err(|_| StoreError::IllegalTransition { from, to: next })?;
        sqlx::query(
            "UPDATE executions \
             SET state = $2, updated_at = $3, version = version + 1 \
             WHERE id = $1",
        )
        .bind(id.as_str())
        .bind(enum_text(&execution.state)?)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(execution)
    }

    async fn transition_with_event(
        &self,
        id: &ExecutionId,
        next: ExecutionState,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<(Execution, u64), StoreError> {
        if id != &event.execution_id || next != event.state || event.sequence != 0 {
            return Err(StoreError::Conflict(
                "execution transition and event identity do not match".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let mut execution = decode_execution(&row)?;
        if cancellation_pending(&mut transaction, id).await? {
            return Err(StoreError::Conflict(format!(
                "execution {id} has a pending cancellation request"
            )));
        }
        if execution.attempt_id != event.attempt_id {
            return Err(StoreError::Conflict(
                "execution transition and event attempt do not match".to_owned(),
            ));
        }
        let from = execution.state;
        if !from.can_transition_to(next) {
            return Err(StoreError::IllegalTransition { from, to: next });
        }
        execution
            .transition(next)
            .map_err(|_| StoreError::IllegalTransition { from, to: next })?;
        sqlx::query(
            "UPDATE executions SET state = $2, updated_at = $3, version = version + 1 \
             WHERE id = $1",
        )
        .bind(id.as_str())
        .bind(enum_text(&execution.state)?)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        let sequence = append_in_transaction(&mut transaction, event).await?;
        transaction.commit().await?;
        Ok((execution, sequence))
    }

    async fn record_progress(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<u64, StoreError> {
        record_progress(&self.pool, execution, event, false).await
    }

    async fn record_progress_and_request_retention(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<u64, StoreError> {
        record_progress(&self.pool, execution, event, true).await
    }

    async fn create_idempotent(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
        idempotency_key: &str,
        request_scope: &str,
    ) -> Result<IdempotentExecution, StoreError> {
        if idempotency_key.is_empty() || idempotency_key.len() > 255 {
            return Err(StoreError::Conflict(
                "idempotency key must be 1-255 bytes".to_owned(),
            ));
        }
        if execution.id != event.execution_id
            || execution.state != event.state
            || event.sequence != 0
        {
            return Err(StoreError::Conflict(
                "created execution and event identity do not match".to_owned(),
            ));
        }
        let manifest = to_json(&execution.manifest)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 1))")
            .bind(idempotency_key)
            .fetch_one(&mut *transaction)
            .await?;
        if let Some(row) = sqlx::query(
            "SELECT request_scope, manifest, execution_id FROM execution_requests \
             WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let stored_scope: String = row.try_get("request_scope")?;
            let stored_manifest: Value = row.try_get("manifest")?;
            if stored_scope != request_scope || stored_manifest != manifest {
                return Err(StoreError::IdempotencyConflict(
                    "idempotency key was already used for a different request".to_owned(),
                ));
            }
            let execution_id = ExecutionId::new(row.try_get::<String, _>("execution_id")?);
            let row = sqlx::query("SELECT * FROM executions WHERE id = $1")
                .bind(execution_id.as_str())
                .fetch_one(&mut *transaction)
                .await?;
            let replay = decode_execution(&row)?;
            transaction.commit().await?;
            return Ok(IdempotentExecution {
                execution: replay,
                created: false,
                event_sequence: None,
            });
        }

        let result = execution.result.as_ref().map(to_json).transpose()?;
        sqlx::query(
            "INSERT INTO executions \
             (id, role, state, manifest, worker_id, attempt_id, session_id, worktree_path, \
              labels, result, created_at, updated_at, version) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1)",
        )
        .bind(execution.id.as_str())
        .bind(enum_text(&execution.role)?)
        .bind(enum_text(&execution.state)?)
        .bind(&manifest)
        .bind(execution.worker_id.as_ref().map(WorkerId::as_str))
        .bind(execution.attempt_id.as_ref().map(AttemptId::as_str))
        .bind(execution.session_id.as_ref().map(SessionId::as_str))
        .bind(execution.worktree_path.as_deref())
        .bind(to_json(&execution.labels)?)
        .bind(result)
        .bind(execution.created_at)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_conflict)?;
        sqlx::query(
            "INSERT INTO execution_requests \
             (idempotency_key, request_scope, manifest, execution_id, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(idempotency_key)
        .bind(request_scope)
        .bind(&manifest)
        .bind(execution.id.as_str())
        .bind(execution.created_at)
        .execute(&mut *transaction)
        .await?;
        let sequence = append_in_transaction(&mut transaction, event).await?;
        transaction.commit().await?;
        let mut persisted = execution.clone();
        persisted.updated_at = execution.updated_at;
        Ok(IdempotentExecution {
            execution: persisted,
            created: true,
            event_sequence: Some(sequence),
        })
    }

    async fn request_control(
        &self,
        id: &ExecutionId,
        action: ExecutionControlAction,
        idempotency_key: &str,
    ) -> Result<IdempotentExecutionControl, StoreError> {
        if idempotency_key.is_empty() || idempotency_key.len() > 255 {
            return Err(StoreError::Conflict(
                "idempotency key must be 1-255 bytes".to_owned(),
            ));
        }
        let action_text = enum_text(&action)?;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let execution = decode_execution(&row)?;
        if let Some(row) = sqlx::query(
            "SELECT request_id, action, requested_at, completed_at \
             FROM execution_control_requests \
             WHERE execution_id = $1 AND idempotency_key = $2 FOR UPDATE",
        )
        .bind(id.as_str())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            let stored_action: String = row.try_get("action")?;
            if stored_action != action_text {
                return Err(StoreError::IdempotencyConflict(
                    "idempotency key was already used for another execution control".to_owned(),
                ));
            }
            let request = decode_control_request(id, &row)?;
            transaction.commit().await?;
            return Ok(IdempotentExecutionControl {
                request,
                created: false,
            });
        }
        let cancellation_pending = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM execution_cancellation_requests \
             WHERE execution_id = $1 AND completed_at IS NULL)",
        )
        .bind(id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        if cancellation_pending {
            return Err(StoreError::Conflict(format!(
                "execution {id} has a pending cancellation"
            )));
        }
        let pending_actions = sqlx::query(
            "SELECT action, target_session_id FROM execution_control_requests \
             WHERE execution_id = $1 AND completed_at IS NULL \
             ORDER BY requested_at, request_id",
        )
        .bind(id.as_str())
        .fetch_all(&mut *transaction)
        .await?;
        let mut projected_state = execution.state;
        let mut projected_session = execution.session_id.clone().ok_or_else(|| {
            StoreError::Conflict(format!(
                "execution {id} lacks durable interactive authority"
            ))
        })?;
        let current_version: i64 = row.try_get("version")?;
        let mut projected_version = current_version;
        for pending in pending_actions {
            let pending_action = decode_control_action(pending.try_get("action")?)?;
            match pending_action {
                ExecutionControlAction::Pause if projected_state == ExecutionState::Running => {
                    projected_state = ExecutionState::PausedForHuman;
                }
                ExecutionControlAction::Resume
                    if projected_state == ExecutionState::PausedForHuman =>
                {
                    projected_state = ExecutionState::Running;
                }
                ExecutionControlAction::ForkConversation
                    if matches!(
                        projected_state,
                        ExecutionState::Running | ExecutionState::PausedForHuman
                    ) =>
                {
                    projected_session = SessionId::new(
                        pending
                            .try_get::<Option<String>, _>("target_session_id")?
                            .ok_or_else(|| {
                                StoreError::Conflict(
                                    "pending fork lacks a target session".to_owned(),
                                )
                            })?,
                    );
                }
                pending => {
                    return Err(StoreError::Conflict(format!(
                        "pending {pending:?} is invalid for projected state {projected_state:?}"
                    )))
                }
            }
            projected_version += 1;
        }
        let allowed = match action {
            ExecutionControlAction::Pause => projected_state == ExecutionState::Running,
            ExecutionControlAction::Resume => projected_state == ExecutionState::PausedForHuman,
            ExecutionControlAction::ForkConversation => matches!(
                projected_state,
                ExecutionState::Running | ExecutionState::PausedForHuman
            ),
        };
        if !allowed {
            return Err(StoreError::Conflict(format!(
                "execution {id} cannot accept {action:?} while {:?}",
                projected_state
            )));
        }
        let worker_id = execution.worker_id.as_ref().ok_or_else(|| {
            StoreError::Conflict(format!(
                "execution {id} lacks durable interactive authority"
            ))
        })?;
        let attempt_id = execution.attempt_id.as_ref().ok_or_else(|| {
            StoreError::Conflict(format!(
                "execution {id} lacks durable interactive authority"
            ))
        })?;
        let worktree_path = execution.worktree_path.as_deref().ok_or_else(|| {
            StoreError::Conflict(format!(
                "execution {id} lacks durable interactive authority"
            ))
        })?;
        let request_id: i64 = sqlx::query_scalar(
            "SELECT nextval(pg_get_serial_sequence('execution_control_requests', 'request_id'))",
        )
        .fetch_one(&mut *transaction)
        .await?;
        let target_session = (action == ExecutionControlAction::ForkConversation)
            .then(|| SessionId::new(format!("{}-fork-{request_id}", projected_session.as_str())));
        let inserted = sqlx::query(
            "INSERT INTO execution_control_requests \
             (request_id, execution_id, action, idempotency_key, accepted_worker_id, \
              accepted_attempt_id, source_session_id, worktree_path, \
              accepted_execution_version, accepted_state, target_session_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             RETURNING request_id, action, requested_at, completed_at",
        )
        .bind(request_id)
        .bind(id.as_str())
        .bind(action_text)
        .bind(idempotency_key)
        .bind(worker_id.as_str())
        .bind(attempt_id.as_str())
        .bind(projected_session.as_str())
        .bind(worktree_path)
        .bind(projected_version)
        .bind(enum_text(&projected_state)?)
        .bind(target_session.as_ref().map(SessionId::as_str))
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_conflict)?;
        let request = decode_control_request(id, &inserted)?;
        transaction.commit().await?;
        Ok(IdempotentExecutionControl {
            request,
            created: true,
        })
    }

    async fn list_pending_controls(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<PendingExecutionControl>, StoreError> {
        let rows = sqlx::query(
            "SELECT c.request_id AS control_request_id, c.action AS control_action, \
                    c.requested_at AS control_requested_at, \
                    c.completed_at AS control_completed_at, c.phase AS control_phase, \
                    c.accepted_worker_id AS control_worker_id, \
                    c.accepted_attempt_id AS control_attempt_id, \
                    c.source_session_id AS control_source_session_id, \
                    c.worktree_path AS control_worktree_path, \
                    c.accepted_execution_version AS control_execution_version, \
                    c.accepted_state AS control_accepted_state, \
                    c.target_session_id AS control_target_session_id, \
                    c.side_effect_session_id AS control_side_effect_session_id, e.* \
             FROM execution_control_requests c \
             JOIN executions e ON e.id = c.execution_id \
             WHERE c.phase NOT IN ('COMPLETED', 'STALE') \
               AND (c.accepted_worker_id = $1 OR e.worker_id = $1) \
             ORDER BY c.requested_at, c.request_id",
        )
        .bind(worker_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let execution = decode_execution(row)?;
                let action = decode_control_action(row.try_get("control_action")?)?;
                Ok(PendingExecutionControl {
                    request: ExecutionControlRequest {
                        request_id: row.try_get("control_request_id")?,
                        execution_id: execution.id.clone(),
                        action,
                        requested_at: row.try_get("control_requested_at")?,
                        completed_at: row.try_get("control_completed_at")?,
                    },
                    execution,
                    phase: decode_control_phase(row.try_get("control_phase")?)?,
                    accepted_worker_id: WorkerId::new(
                        row.try_get::<String, _>("control_worker_id")?,
                    ),
                    accepted_attempt_id: AttemptId::new(
                        row.try_get::<String, _>("control_attempt_id")?,
                    ),
                    source_session_id: SessionId::new(
                        row.try_get::<String, _>("control_source_session_id")?,
                    ),
                    worktree_path: row.try_get("control_worktree_path")?,
                    accepted_version: row.try_get("control_execution_version")?,
                    accepted_state: decode_execution_state(row.try_get("control_accepted_state")?)?,
                    target_session_id: row
                        .try_get::<Option<String>, _>("control_target_session_id")?
                        .map(SessionId::new),
                    side_effect_session_id: row
                        .try_get::<Option<String>, _>("control_side_effect_session_id")?
                        .map(SessionId::new),
                })
            })
            .collect()
    }

    async fn begin_control(
        &self,
        request_id: i64,
        worker_id: &WorkerId,
        attempt_id: &AttemptId,
    ) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        let execution_id: String = sqlx::query_scalar(
            "SELECT execution_id FROM execution_control_requests WHERE request_id = $1",
        )
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("control request {request_id}")))?;
        let execution_row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(&execution_id)
            .fetch_one(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT phase, accepted_worker_id, accepted_attempt_id, source_session_id, \
                    worktree_path, accepted_execution_version, accepted_state \
             FROM execution_control_requests WHERE request_id = $1 FOR UPDATE",
        )
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("control request {request_id}")))?;
        let phase: String = row.try_get("phase")?;
        if !matches!(
            phase.as_str(),
            "ACCEPTED" | "APPLYING" | "SIDE_EFFECT_APPLIED"
        ) {
            return Err(StoreError::Conflict(format!(
                "control request {request_id} is terminal: {phase}"
            )));
        }
        let accepted_worker: String = row.try_get("accepted_worker_id")?;
        let accepted_attempt: String = row.try_get("accepted_attempt_id")?;
        let current = decode_execution(&execution_row)?;
        let fenced = accepted_worker == worker_id.as_str()
            && accepted_attempt == attempt_id.as_str()
            && current.worker_id.as_ref().map(WorkerId::as_str) == Some(accepted_worker.as_str())
            && current.attempt_id.as_ref().map(AttemptId::as_str)
                == Some(accepted_attempt.as_str())
            && current.session_id.as_ref().map(SessionId::as_str)
                == Some(row.try_get::<String, _>("source_session_id")?.as_str())
            && current.worktree_path.as_deref()
                == Some(row.try_get::<String, _>("worktree_path")?.as_str())
            && execution_row.try_get::<i64, _>("version")?
                == row.try_get::<i64, _>("accepted_execution_version")?
            && enum_text(&current.state)? == row.try_get::<String, _>("accepted_state")?;
        let cancellation =
            cancellation_pending(&mut transaction, &ExecutionId::new(execution_id.clone())).await?;
        if !fenced || cancellation {
            sqlx::query(
                "UPDATE execution_control_requests SET phase = 'STALE', \
                 stale_reason = $2, completed_at = now() \
                 WHERE request_id = $1",
            )
            .bind(request_id)
            .bind(if cancellation {
                "execution cancellation took precedence"
            } else {
                "accepted execution authority changed"
            })
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Err(StoreError::Conflict(
                "accepted interactive control authority is stale".to_owned(),
            ));
        }
        if phase == "ACCEPTED" {
            sqlx::query(
                "UPDATE execution_control_requests SET phase = 'APPLYING' WHERE request_id = $1",
            )
            .bind(request_id)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn mark_control_side_effect_applied(
        &self,
        request_id: i64,
        session_id: Option<&SessionId>,
    ) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        let execution_id: String = sqlx::query_scalar(
            "SELECT execution_id FROM execution_control_requests WHERE request_id = $1",
        )
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("control request {request_id}")))?;
        sqlx::query("SELECT id FROM executions WHERE id = $1 FOR UPDATE")
            .bind(&execution_id)
            .fetch_one(&mut *transaction)
            .await?;
        let row = sqlx::query(
            "SELECT action, phase, source_session_id, target_session_id, \
                    side_effect_session_id FROM execution_control_requests \
             WHERE request_id = $1 FOR UPDATE",
        )
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("control request {request_id}")))?;
        if cancellation_pending(&mut transaction, &ExecutionId::new(execution_id)).await? {
            sqlx::query(
                "UPDATE execution_control_requests SET phase = 'STALE', \
                 stale_reason = 'execution cancellation took precedence', completed_at = now() \
                 WHERE request_id = $1 AND phase NOT IN ('COMPLETED', 'STALE')",
            )
            .bind(request_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Err(StoreError::Conflict(
                "execution cancellation took precedence".to_owned(),
            ));
        }
        let phase: String = row.try_get("phase")?;
        let action = decode_control_action(row.try_get("action")?)?;
        let expected = match action {
            ExecutionControlAction::ForkConversation => row
                .try_get::<Option<String>, _>("target_session_id")?
                .ok_or_else(|| StoreError::Conflict("fork target session is missing".to_owned()))?,
            _ => row.try_get("source_session_id")?,
        };
        let supplied = session_id
            .map(SessionId::as_str)
            .unwrap_or(expected.as_str());
        if supplied != expected {
            return Err(StoreError::Conflict(
                "control side effect belongs to a different session".to_owned(),
            ));
        }
        if phase == "SIDE_EFFECT_APPLIED"
            && row
                .try_get::<Option<String>, _>("side_effect_session_id")?
                .as_deref()
                == Some(expected.as_str())
        {
            transaction.commit().await?;
            return Ok(());
        }
        if phase != "APPLYING" {
            return Err(StoreError::Conflict(format!(
                "control request {request_id} cannot record a side effect from {phase}"
            )));
        }
        sqlx::query(
            "UPDATE execution_control_requests SET phase = 'SIDE_EFFECT_APPLIED', \
             side_effect_session_id = $2 WHERE request_id = $1",
        )
        .bind(request_id)
        .bind(expected)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn complete_control(
        &self,
        request_id: i64,
        execution: &Execution,
        event: &ExecutionEvent,
    ) -> Result<Option<u64>, StoreError> {
        if event.execution_id != execution.id
            || event.state != execution.state
            || event.attempt_id != execution.attempt_id
            || event.sequence != 0
        {
            return Err(StoreError::Conflict(
                "interactive control progress and event identity do not match".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(execution.id.as_str())
            .fetch_one(&mut *transaction)
            .await?;
        let control = sqlx::query(
            "SELECT execution_id, action, phase, completed_at, accepted_worker_id, \
                    accepted_attempt_id, source_session_id, worktree_path, \
                    accepted_execution_version, accepted_state, target_session_id, \
                    side_effect_session_id \
             FROM execution_control_requests \
             WHERE request_id = $1 FOR UPDATE",
        )
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(format!("control request {request_id}")))?;
        if control.try_get::<String, _>("execution_id")? != execution.id.as_str() {
            return Err(StoreError::Conflict(
                "interactive control belongs to another execution".to_owned(),
            ));
        }
        let phase: String = control.try_get("phase")?;
        if phase == "COMPLETED" {
            transaction.commit().await?;
            return Ok(None);
        }
        if cancellation_pending(&mut transaction, &execution.id).await? {
            sqlx::query(
                "UPDATE execution_control_requests SET phase = 'STALE', \
                 stale_reason = 'execution cancellation took precedence', completed_at = now() \
                 WHERE request_id = $1",
            )
            .bind(request_id)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Err(StoreError::Conflict(
                "execution cancellation took precedence".to_owned(),
            ));
        }
        if phase != "SIDE_EFFECT_APPLIED" {
            return Err(StoreError::Conflict(format!(
                "control request {request_id} cannot complete from {phase}"
            )));
        }
        let action = decode_control_action(control.try_get("action")?)?;
        let accepted_state = decode_execution_state(control.try_get("accepted_state")?)?;
        let source_session = SessionId::new(control.try_get::<String, _>("source_session_id")?);
        let expected_session = match action {
            ExecutionControlAction::ForkConversation => SessionId::new(
                control
                    .try_get::<Option<String>, _>("target_session_id")?
                    .ok_or_else(|| {
                        StoreError::Conflict("fork target session is missing".to_owned())
                    })?,
            ),
            _ => source_session.clone(),
        };
        if control
            .try_get::<Option<String>, _>("side_effect_session_id")?
            .as_deref()
            != Some(expected_session.as_str())
        {
            return Err(StoreError::Conflict(
                "control side effect session is not the accepted target".to_owned(),
            ));
        }
        let expected_state = match action {
            ExecutionControlAction::Pause => ExecutionState::PausedForHuman,
            ExecutionControlAction::Resume => ExecutionState::Running,
            ExecutionControlAction::ForkConversation => accepted_state,
        };
        let event_matches_action = matches!(
            (&action, &event.kind),
            (
                ExecutionControlAction::Pause,
                ExecutionEventKind::ExecutionPaused
            ) | (
                ExecutionControlAction::Resume,
                ExecutionEventKind::ExecutionResumed
            )
        ) || matches!(
            (&action, &event.kind),
            (
                ExecutionControlAction::ForkConversation,
                ExecutionEventKind::ConversationForked { session_id }
            ) if session_id == &expected_session
        );
        if execution.state != expected_state
            || execution.session_id.as_ref() != Some(&expected_session)
            || execution.worktree_path.as_deref()
                != Some(control.try_get::<String, _>("worktree_path")?.as_str())
            || !event_matches_action
        {
            return Err(StoreError::Conflict(format!(
                "completed {action:?} does not match its accepted state/session/worktree/event"
            )));
        }
        let persisted = decode_execution(&row)?;
        let persisted_version: i64 = row.try_get("version")?;
        if persisted.worker_id.as_ref().map(WorkerId::as_str)
            != Some(control.try_get::<String, _>("accepted_worker_id")?.as_str())
            || persisted.attempt_id.as_ref().map(AttemptId::as_str)
                != Some(
                    control
                        .try_get::<String, _>("accepted_attempt_id")?
                        .as_str(),
                )
            || persisted.session_id.as_ref() != Some(&source_session)
            || persisted.worktree_path != execution.worktree_path
            || persisted.state != accepted_state
            || persisted_version != control.try_get::<i64, _>("accepted_execution_version")?
        {
            return Err(StoreError::Conflict(
                "interactive control attempt authority changed".to_owned(),
            ));
        }
        if action != ExecutionControlAction::ForkConversation
            && !persisted.state.can_transition_to(execution.state)
        {
            return Err(StoreError::IllegalTransition {
                from: persisted.state,
                to: execution.state,
            });
        }
        sqlx::query(
            "UPDATE executions SET state = $2, session_id = $3, updated_at = $4, \
             version = version + 1 WHERE id = $1",
        )
        .bind(execution.id.as_str())
        .bind(enum_text(&execution.state)?)
        .bind(execution.session_id.as_ref().map(SessionId::as_str))
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE execution_attempts SET state = $2, session_id = $3, updated_at = $4 \
             WHERE execution_id = $1 AND attempt_id = $5 AND finished_at IS NULL",
        )
        .bind(execution.id.as_str())
        .bind(enum_text(&execution.state)?)
        .bind(execution.session_id.as_ref().map(SessionId::as_str))
        .bind(execution.updated_at)
        .bind(execution.attempt_id.as_ref().map(AttemptId::as_str))
        .execute(&mut *transaction)
        .await?;
        let sequence = append_in_transaction(&mut transaction, event).await?;
        sqlx::query(
            "UPDATE execution_control_requests SET phase = 'COMPLETED', completed_at = $2 \
             WHERE request_id = $1",
        )
        .bind(request_id)
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(sequence))
    }

    async fn attachment_snapshot(
        &self,
        id: &ExecutionId,
    ) -> Result<ExecutionAttachment, StoreError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT id FROM executions WHERE id = $1 FOR SHARE")
            .bind(id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let cleanup_phase = sqlx::query_scalar::<_, String>(
            "SELECT phase FROM cleanup_authorities WHERE execution_id = $1 FOR SHARE",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        let row = sqlx::query(
            "SELECT e.*, \
                    COALESCE((SELECT MAX(sequence) FROM execution_events \
                              WHERE execution_id = e.id), 0) AS attachment_cursor \
             FROM executions e WHERE e.id = $1",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        let execution = decode_execution(&row)?;
        let active = matches!(
            execution.state,
            ExecutionState::Running | ExecutionState::PausedForHuman
        ) && execution.worker_id.is_some()
            && execution.attempt_id.is_some();
        let retained = execution.state == ExecutionState::ReviewReady
            && cleanup_phase.as_deref() == Some("RETAINED");
        if !active && !retained {
            return Err(StoreError::Conflict(format!(
                "execution {id} has no live or retained attachment authority"
            )));
        }
        let session_id = execution.session_id.ok_or_else(|| {
            StoreError::Conflict(format!("execution {id} lacks an attachable session"))
        })?;
        if execution.worktree_path.is_none() {
            return Err(StoreError::Conflict(format!(
                "execution {id} lacks an attachable workspace"
            )));
        }
        let cursor = u64::try_from(row.try_get::<i64, _>("attachment_cursor")?)
            .map_err(|_| StoreError::Conflict("negative attachment cursor".to_owned()))?;
        transaction.commit().await?;
        Ok(ExecutionAttachment {
            execution_id: id.clone(),
            state: execution.state,
            session_id,
            workspace_ref: format!("execution:{id}:workspace"),
            event_cursor: cursor,
        })
    }
}

fn decode_control_request(
    execution_id: &ExecutionId,
    row: &sqlx::postgres::PgRow,
) -> Result<ExecutionControlRequest, StoreError> {
    Ok(ExecutionControlRequest {
        request_id: row.try_get("request_id")?,
        execution_id: execution_id.clone(),
        action: decode_control_action(row.try_get("action")?)?,
        requested_at: row.try_get("requested_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn decode_control_action(value: String) -> Result<ExecutionControlAction, StoreError> {
    serde_json::from_value(Value::String(value))
        .map_err(|error| StoreError::Conflict(format!("invalid execution control action: {error}")))
}

fn decode_control_phase(value: String) -> Result<ExecutionControlPhase, StoreError> {
    match value.as_str() {
        "ACCEPTED" => Ok(ExecutionControlPhase::Accepted),
        "APPLYING" => Ok(ExecutionControlPhase::Applying),
        "SIDE_EFFECT_APPLIED" => Ok(ExecutionControlPhase::SideEffectApplied),
        _ => Err(StoreError::Conflict(format!(
            "invalid pending execution control phase: {value}"
        ))),
    }
}

fn decode_execution_state(value: String) -> Result<ExecutionState, StoreError> {
    serde_json::from_value(Value::String(value))
        .map_err(|error| StoreError::Conflict(format!("invalid execution state: {error}")))
}

fn cancelled_result(id: &ExecutionId) -> ExecutionResult {
    ExecutionResult {
        execution_id: id.clone(),
        state: ExecutionState::Cancelled,
        failure: Some(FailureClass::Cancelled),
        branch: None,
        base_sha: None,
        diff_artifact: None,
        artifacts: Vec::new(),
        tests: None,
    }
}

async fn record_progress(
    pool: &PgPool,
    execution: &Execution,
    event: &orchestrator_core::ExecutionEvent,
    request_retention: bool,
) -> Result<u64, StoreError> {
    if execution.id != event.execution_id
        || execution.state != event.state
        || execution.attempt_id != event.attempt_id
    {
        return Err(StoreError::Conflict(
            "execution progress and event identity do not match".to_owned(),
        ));
    }
    let mut transaction = pool.begin().await?;
    let locked = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
        .bind(execution.id.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(execution.id.to_string()))?;
    let persisted = decode_execution(&locked)?;
    if cancellation_pending(&mut transaction, &execution.id).await? {
        return Err(StoreError::Conflict(format!(
            "execution {} has a pending cancellation request",
            execution.id
        )));
    }
    let version = locked.try_get::<i64, _>("version")?;
    if persisted.worker_id != execution.worker_id || persisted.attempt_id != execution.attempt_id {
        return Err(StoreError::Conflict(
            "execution worker or attempt authority changed".to_owned(),
        ));
    }
    if persisted.state != execution.state && !persisted.state.can_transition_to(execution.state) {
        return Err(StoreError::IllegalTransition {
            from: persisted.state,
            to: execution.state,
        });
    }
    if execution.state.is_terminal() {
        lock_controls_for_execution(&mut transaction, &execution.id).await?;
    }
    let (attempt_id, worker_id) = execution
        .attempt_id
        .as_ref()
        .zip(execution.worker_id.as_ref())
        .ok_or_else(|| {
            StoreError::Conflict("execution progress lacks active attempt authority".to_owned())
        })?;
    let active_attempts = sqlx::query_scalar::<_, String>(
        "SELECT attempt_id FROM execution_attempts \
             WHERE attempt_id = $1 AND execution_id = $2 AND worker_id = $3 \
             AND finished_at IS NULL FOR UPDATE",
    )
    .bind(attempt_id.as_str())
    .bind(execution.id.as_str())
    .bind(worker_id.as_str())
    .fetch_all(&mut *transaction)
    .await?;
    if active_attempts.len() != 1 {
        return Err(StoreError::Conflict(format!(
            "expected exactly one active attempt, found {}",
            active_attempts.len()
        )));
    }
    let result = execution.result.as_ref().map(to_json).transpose()?;
    let updated = sqlx::query(
        "UPDATE executions SET state = $2, worker_id = $3, attempt_id = $4, session_id = $5, \
             worktree_path = $6, result = $7, updated_at = $8, version = version + 1 \
             WHERE id = $1 AND version = $9",
    )
    .bind(execution.id.as_str())
    .bind(enum_text(&execution.state)?)
    .bind(execution.worker_id.as_ref().map(WorkerId::as_str))
    .bind(execution.attempt_id.as_ref().map(AttemptId::as_str))
    .bind(execution.session_id.as_ref().map(SessionId::as_str))
    .bind(execution.worktree_path.as_deref())
    .bind(result.clone())
    .bind(execution.updated_at)
    .bind(version)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(StoreError::Conflict(
            "execution version changed while recording progress".to_owned(),
        ));
    }
    if let Some(attempt_id) = &execution.attempt_id {
        let updated = sqlx::query(
            "UPDATE execution_attempts SET state = $2, worktree_path = $3, session_id = $4, \
                 result = $5, updated_at = $6, finished_at = CASE WHEN $7 THEN $6 ELSE NULL END \
                 WHERE attempt_id = $1",
        )
        .bind(attempt_id.as_str())
        .bind(enum_text(&execution.state)?)
        .bind(execution.worktree_path.as_deref())
        .bind(execution.session_id.as_ref().map(SessionId::as_str))
        .bind(result)
        .bind(execution.updated_at)
        .bind(execution.state.is_terminal())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Conflict(
                "active execution attempt disappeared while recording progress".to_owned(),
            ));
        }
    }
    if request_retention {
        let updated = sqlx::query(
            "UPDATE cleanup_authorities SET phase = 'RETAIN_REQUESTED', updated_at = now() \
                 WHERE execution_id = $1 AND attempt_id = $2 AND worker_id = $3 \
                 AND phase IN ('ACTIVE:POST_PI_BEFORE_EVENT', 'RETAIN_REQUESTED')",
        )
        .bind(execution.id.as_str())
        .bind(attempt_id.as_str())
        .bind(worker_id.as_str())
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(StoreError::Conflict(
                "cleanup authority cannot atomically request retention".to_owned(),
            ));
        }
    }
    if execution.state.is_terminal() {
        stale_controls_for_execution(
            &mut transaction,
            &execution.id,
            "execution entered a terminal state",
        )
        .await?;
    }
    let sequence = append_in_transaction(&mut transaction, event).await?;
    transaction.commit().await?;
    Ok(sequence)
}

async fn cancellation_pending(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &ExecutionId,
) -> Result<bool, StoreError> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM execution_cancellation_requests \
         WHERE execution_id = $1 AND completed_at IS NULL)",
    )
    .bind(id.as_str())
    .fetch_one(&mut **transaction)
    .await
    .map_err(StoreError::from)
}

async fn stale_controls_for_execution(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &ExecutionId,
    reason: &str,
) -> Result<(), StoreError> {
    lock_controls_for_execution(transaction, id).await?;
    sqlx::query(
        "UPDATE execution_control_requests SET phase = 'STALE', stale_reason = $2, \
         completed_at = now() WHERE execution_id = $1 \
         AND phase IN ('ACCEPTED', 'APPLYING', 'SIDE_EFFECT_APPLIED')",
    )
    .bind(id.as_str())
    .bind(reason)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn lock_controls_for_execution(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &ExecutionId,
) -> Result<(), StoreError> {
    sqlx::query_scalar::<_, i64>(
        "SELECT request_id FROM execution_control_requests \
         WHERE execution_id = $1 AND phase IN ('ACCEPTED', 'APPLYING', 'SIDE_EFFECT_APPLIED') \
         ORDER BY request_id FOR UPDATE",
    )
    .bind(id.as_str())
    .fetch_all(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn run_migrations(pool: &PgPool) -> Result<(), StoreError> {
    sqlx::migrate!()
        .run(pool)
        .await
        .map_err(|error| StoreError::Conflict(format!("failed to run migrations: {error}")))
}

pub(crate) fn enum_text<T: Serialize>(value: &T) -> Result<String, StoreError> {
    match to_json(value)? {
        Value::String(value) => Ok(value),
        _ => Err(StoreError::Conflict(
            "enum did not serialize as text".to_owned(),
        )),
    }
}

pub(crate) fn to_json<T: Serialize>(value: &T) -> Result<Value, StoreError> {
    serde_json::to_value(value)
        .map_err(|error| StoreError::Conflict(format!("failed to serialize record: {error}")))
}

pub(crate) fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, StoreError> {
    serde_json::from_value(value)
        .map_err(|error| StoreError::Conflict(format!("failed to deserialize record: {error}")))
}

pub(crate) fn enum_from_text<T: DeserializeOwned>(value: String) -> Result<T, StoreError> {
    from_json(Value::String(value))
}

pub(crate) fn decode_execution(row: &sqlx::postgres::PgRow) -> Result<Execution, StoreError> {
    Ok(Execution {
        id: ExecutionId::new(row.try_get::<String, _>("id")?),
        role: enum_from_text::<Role>(row.try_get("role")?)?,
        state: enum_from_text::<ExecutionState>(row.try_get("state")?)?,
        manifest: from_json(row.try_get("manifest")?)?,
        worker_id: row
            .try_get::<Option<String>, _>("worker_id")?
            .map(WorkerId::new),
        attempt_id: row
            .try_get::<Option<String>, _>("attempt_id")?
            .map(AttemptId::new),
        session_id: row
            .try_get::<Option<String>, _>("session_id")?
            .map(SessionId::new),
        worktree_path: row.try_get("worktree_path")?,
        labels: from_json::<OwnershipLabels>(row.try_get("labels")?)?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        result: row
            .try_get::<Option<Value>, _>("result")?
            .map(from_json::<ExecutionResult>)
            .transpose()?,
    })
}

fn map_conflict(error: sqlx::Error) -> StoreError {
    if let Some(database_error) = error.as_database_error() {
        if database_error.code().as_deref() == Some("23505") {
            return if database_error.constraint() == Some("executions_pkey") {
                StoreError::DuplicateExecutionId(error.to_string())
            } else {
                StoreError::Conflict(error.to_string())
            };
        }
    }
    StoreError::Db(error)
}

#[cfg(test)]
mod contract_tests {
    use super::{ArtifactStore, EventLog, ExecutionStore, ReservationStore, WorkerStore};

    #[allow(dead_code)]
    fn traits_are_object_safe(
        _: &dyn ExecutionStore,
        _: &dyn EventLog,
        _: &dyn WorkerStore,
        _: &dyn ReservationStore,
        _: &dyn ArtifactStore,
    ) {
    }
}
