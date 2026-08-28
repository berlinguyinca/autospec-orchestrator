//! Durable PostgreSQL storage for execution-plane records (spec sections 32, 49, 61, 74).

mod error;
mod event_log;

pub use error::StoreError;
pub use event_log::{EventLog, PgEventLog};

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

fn to_json<T: Serialize>(value: &T) -> Result<Value, StoreError> {
    serde_json::to_value(value)
        .map_err(|error| StoreError::Conflict(format!("failed to serialize record: {error}")))
}

fn from_json<T: DeserializeOwned>(value: Value) -> Result<T, StoreError> {
    serde_json::from_value(value)
        .map_err(|error| StoreError::Conflict(format!("failed to deserialize record: {error}")))
}

fn enum_from_text<T: DeserializeOwned>(value: String) -> Result<T, StoreError> {
    from_json(Value::String(value))
}

fn decode_execution(row: &sqlx::postgres::PgRow) -> Result<Execution, StoreError> {
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
    use super::{EventLog, ExecutionStore};

    #[allow(dead_code)]
    fn traits_are_object_safe(_: &dyn ExecutionStore, _: &dyn EventLog) {}
}
