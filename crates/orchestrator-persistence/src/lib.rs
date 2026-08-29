//! Durable PostgreSQL storage for execution-plane records (spec sections 32, 49, 61, 74).

mod artifacts;
mod cleanup;
mod error;
mod event_log;
mod reservations;
mod workers;

pub use artifacts::{Artifact, ArtifactStore, PgArtifactStore};
pub use cleanup::{
    CleanupAuthority, CleanupAuthorityStore, CleanupDisposition, CleanupStage,
    PgCleanupAuthorityStore,
};
pub use error::StoreError;
pub use event_log::{EventLog, PgEventLog};
pub use reservations::{LostWorkerRecovery, PgReservationStore, Reservation, ReservationStore};
pub use workers::{PgWorkerStore, WorkerStore};

use async_trait::async_trait;
use event_log::append_in_transaction;
use orchestrator_core::{
    event::ExecutionEventKind, AttemptId, Execution, ExecutionEvent, ExecutionId, ExecutionResult,
    ExecutionState, FailureClass, OwnershipLabels, Role, SessionId, WorkerId,
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
