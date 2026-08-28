use crate::{enum_text, run_migrations, StoreError};
use async_trait::async_trait;
use orchestrator_core::{ExecutionEvent, ExecutionId};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

/// Append-only execution history with a gapless sequence per execution.
#[async_trait]
pub trait EventLog: Send + Sync {
    async fn append(&self, event: &ExecutionEvent) -> Result<u64, StoreError>;
    async fn since(&self, id: &ExecutionId, after: u64) -> Result<Vec<ExecutionEvent>, StoreError>;
}

#[derive(Debug, Clone)]
pub struct PgEventLog {
    pool: PgPool,
}

impl PgEventLog {
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

    async fn append_once(&self, event: &ExecutionEvent) -> Result<u64, StoreError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(event.execution_id.as_str())
            .fetch_one(&mut *transaction)
            .await?;
        let sequence: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), 0) + 1 \
             FROM execution_events WHERE execution_id = $1",
        )
        .bind(event.execution_id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        let sequence = u64::try_from(sequence)
            .map_err(|_| StoreError::Conflict("negative event sequence".to_owned()))?;
        let mut persisted = event.clone();
        persisted.sequence = sequence;
        let payload = serde_json::to_value(&persisted).map_err(|error| {
            StoreError::Conflict(format!("failed to serialize execution event: {error}"))
        })?;
        let insert = sqlx::query(
            "INSERT INTO execution_events (execution_id, sequence, at, state, payload) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(event.execution_id.as_str())
        .bind(i64::try_from(sequence).map_err(|_| {
            StoreError::Conflict("event sequence exceeds PostgreSQL BIGINT".to_owned())
        })?)
        .bind(event.at)
        .bind(enum_text(&event.state)?)
        .bind(payload)
        .execute(&mut *transaction)
        .await;
        match insert {
            Ok(_) => {
                transaction.commit().await?;
                Ok(sequence)
            }
            Err(error) if is_unique_violation(&error) => Err(StoreError::SequenceConflict),
            Err(error) => Err(StoreError::Db(error)),
        }
    }
}

#[async_trait]
impl EventLog for PgEventLog {
    async fn append(&self, event: &ExecutionEvent) -> Result<u64, StoreError> {
        match self.append_once(event).await {
            Err(StoreError::SequenceConflict) => self.append_once(event).await,
            result => result,
        }
    }

    async fn since(&self, id: &ExecutionId, after: u64) -> Result<Vec<ExecutionEvent>, StoreError> {
        let after = i64::try_from(after).map_err(|_| {
            StoreError::Conflict("event cursor exceeds PostgreSQL BIGINT".to_owned())
        })?;
        let rows = sqlx::query(
            "SELECT sequence, payload FROM execution_events \
             WHERE execution_id = $1 AND sequence > $2 \
             ORDER BY sequence ASC LIMIT 500",
        )
        .bind(id.as_str())
        .bind(after)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let sequence = u64::try_from(row.try_get::<i64, _>("sequence")?)
                    .map_err(|_| StoreError::Conflict("negative event sequence".to_owned()))?;
                let mut event: ExecutionEvent = serde_json::from_value(row.try_get("payload")?)
                    .map_err(|error| {
                        StoreError::Conflict(format!(
                            "failed to deserialize execution event: {error}"
                        ))
                    })?;
                event.sequence = sequence;
                Ok(event)
            })
            .collect()
    }
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "23505")
}
