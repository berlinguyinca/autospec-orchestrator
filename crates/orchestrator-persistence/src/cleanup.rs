use crate::{run_migrations, StoreError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestrator_core::{AttemptId, ExecutionId, WorkerId};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupAuthority {
    pub execution_id: ExecutionId,
    pub attempt_id: AttemptId,
    pub worker_id: WorkerId,
    pub phase: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait CleanupAuthorityStore: Send + Sync {
    async fn begin(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
        worker_id: &WorkerId,
    ) -> Result<(), StoreError>;
    async fn advance(&self, execution_id: &ExecutionId, phase: &str) -> Result<(), StoreError>;
    async fn resolve(&self, execution_id: &ExecutionId) -> Result<(), StoreError>;
    async fn list_for_worker(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<CleanupAuthority>, StoreError>;
}

#[derive(Debug, Clone)]
pub struct PgCleanupAuthorityStore {
    pool: PgPool,
}

impl PgCleanupAuthorityStore {
    pub async fn connect(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
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
impl CleanupAuthorityStore for PgCleanupAuthorityStore {
    async fn begin(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
        worker_id: &WorkerId,
    ) -> Result<(), StoreError> {
        let inserted = sqlx::query(
            "INSERT INTO cleanup_authorities (execution_id, attempt_id, worker_id, phase) \
             VALUES ($1, $2, $3, 'RESERVED') ON CONFLICT (execution_id) DO UPDATE \
             SET updated_at = now() WHERE cleanup_authorities.attempt_id = EXCLUDED.attempt_id \
             AND cleanup_authorities.worker_id = EXCLUDED.worker_id",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .bind(worker_id.as_str())
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            Ok(())
        } else {
            Err(StoreError::Conflict(format!(
                "cleanup authority for {execution_id} belongs to another attempt"
            )))
        }
    }

    async fn advance(&self, execution_id: &ExecutionId, phase: &str) -> Result<(), StoreError> {
        let updated = sqlx::query(
            "UPDATE cleanup_authorities SET phase = $2, updated_at = now() WHERE execution_id = $1",
        )
        .bind(execution_id.as_str())
        .bind(phase)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 1 {
            Ok(())
        } else {
            Err(StoreError::NotFound(execution_id.to_string()))
        }
    }

    async fn resolve(&self, execution_id: &ExecutionId) -> Result<(), StoreError> {
        sqlx::query("DELETE FROM cleanup_authorities WHERE execution_id = $1")
            .bind(execution_id.as_str())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_for_worker(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<CleanupAuthority>, StoreError> {
        sqlx::query(
            "SELECT execution_id, attempt_id, worker_id, phase, created_at, updated_at \
             FROM cleanup_authorities WHERE worker_id = $1 ORDER BY updated_at, execution_id",
        )
        .bind(worker_id.as_str())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok(CleanupAuthority {
                execution_id: ExecutionId::new(row.try_get::<String, _>("execution_id")?),
                attempt_id: AttemptId::new(row.try_get::<String, _>("attempt_id")?),
                worker_id: WorkerId::new(row.try_get::<String, _>("worker_id")?),
                phase: row.try_get("phase")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .collect()
    }
}
