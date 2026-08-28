//! Durable PostgreSQL storage for execution-plane records (spec sections 32, 49, 61, 74).

mod error;
mod event_log;
mod reservations;
mod workers;

pub use error::StoreError;
pub use event_log::{EventLog, PgEventLog};
pub use reservations::{PgReservationStore, Reservation, ReservationStore};
pub use workers::{PgWorkerStore, WorkerStore};

use async_trait::async_trait;
use orchestrator_core::{
    AttemptId, Execution, ExecutionId, ExecutionResult, ExecutionState, OwnershipLabels, Role,
    SessionId, WorkerId,
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
    async fn transition(
        &self,
        id: &ExecutionId,
        next: ExecutionState,
    ) -> Result<Execution, StoreError>;
    async fn record_progress(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<u64, StoreError>;
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

    async fn record_progress(
        &self,
        execution: &Execution,
        event: &orchestrator_core::ExecutionEvent,
    ) -> Result<u64, StoreError> {
        if execution.id != event.execution_id
            || execution.state != event.state
            || execution.attempt_id != event.attempt_id
        {
            return Err(StoreError::Conflict(
                "execution progress and event identity do not match".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(execution.id.as_str())
            .fetch_one(&mut *transaction)
            .await?;
        let sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM execution_events WHERE execution_id = $1",
        )
        .bind(execution.id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        let sequence = u64::try_from(sequence)
            .map_err(|_| StoreError::Conflict("negative event sequence".to_owned()))?;
        let result = execution.result.as_ref().map(to_json).transpose()?;
        sqlx::query(
            "UPDATE executions SET state = $2, worker_id = $3, attempt_id = $4, session_id = $5, \
             worktree_path = $6, result = $7, updated_at = $8, version = version + 1 WHERE id = $1",
        )
        .bind(execution.id.as_str())
        .bind(enum_text(&execution.state)?)
        .bind(execution.worker_id.as_ref().map(WorkerId::as_str))
        .bind(execution.attempt_id.as_ref().map(AttemptId::as_str))
        .bind(execution.session_id.as_ref().map(SessionId::as_str))
        .bind(execution.worktree_path.as_deref())
        .bind(result.clone())
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        if let Some(attempt_id) = &execution.attempt_id {
            sqlx::query(
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
        }
        let mut persisted = event.clone();
        persisted.sequence = sequence;
        sqlx::query(
            "INSERT INTO execution_events (execution_id, sequence, at, state, payload) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(execution.id.as_str())
        .bind(i64::try_from(sequence).map_err(|_| {
            StoreError::Conflict("event sequence exceeds PostgreSQL BIGINT".to_owned())
        })?)
        .bind(event.at)
        .bind(enum_text(&event.state)?)
        .bind(to_json(&persisted)?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(sequence)
    }
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
    if error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "23505")
    {
        StoreError::Conflict(error.to_string())
    } else {
        StoreError::Db(error)
    }
}

#[cfg(test)]
mod contract_tests {
    use super::{EventLog, ExecutionStore, ReservationStore, WorkerStore};

    #[allow(dead_code)]
    fn traits_are_object_safe(
        _: &dyn ExecutionStore,
        _: &dyn EventLog,
        _: &dyn WorkerStore,
        _: &dyn ReservationStore,
    ) {
    }
}
