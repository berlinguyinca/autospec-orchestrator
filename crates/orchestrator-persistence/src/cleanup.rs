use crate::{run_migrations, StoreError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestrator_core::{AttemptId, ExecutionId, WorkerId};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupStage {
    Reserved,
    Storage,
    Worktree,
    Runtime,
    PiStarted,
    Running,
    PostPiBeforeEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupDisposition {
    Active(CleanupStage),
    RetainRequested,
    Retained,
    CleanupPending,
    RuntimeStopped,
    RuntimeDestroyed,
    GitRecoveredCleaned,
    StorageReleased,
    ReservationReleased,
    Resolved,
}

impl CleanupDisposition {
    fn can_transition_to(self, next: Self) -> bool {
        use CleanupDisposition as D;
        use CleanupStage as S;
        matches!(
            (self, next),
            (D::Active(S::Reserved), D::Active(S::Storage))
                | (D::Active(S::Storage), D::Active(S::Worktree))
                | (D::Active(S::Worktree), D::Active(S::Runtime))
                | (D::Active(S::Runtime), D::Active(S::PiStarted))
                | (D::Active(S::PiStarted), D::Active(S::Running))
                | (D::Active(S::Running), D::Active(S::PostPiBeforeEvent))
                | (D::Active(S::PostPiBeforeEvent), D::RetainRequested)
                | (D::RetainRequested, D::Retained)
                | (D::Retained, D::CleanupPending)
                | (D::CleanupPending, D::RuntimeStopped)
                | (D::RuntimeStopped, D::RuntimeDestroyed)
                | (D::RuntimeDestroyed, D::GitRecoveredCleaned)
                | (D::GitRecoveredCleaned, D::StorageReleased)
                | (D::StorageReleased, D::ReservationReleased)
                | (D::ReservationReleased, D::Resolved)
                | (D::Active(_), D::CleanupPending)
                | (D::RetainRequested, D::CleanupPending)
        )
    }
}

impl fmt::Display for CleanupDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Active(CleanupStage::Reserved) => "ACTIVE:RESERVED",
            Self::Active(CleanupStage::Storage) => "ACTIVE:STORAGE",
            Self::Active(CleanupStage::Worktree) => "ACTIVE:WORKTREE",
            Self::Active(CleanupStage::Runtime) => "ACTIVE:RUNTIME",
            Self::Active(CleanupStage::PiStarted) => "ACTIVE:PI_STARTED",
            Self::Active(CleanupStage::Running) => "ACTIVE:RUNNING",
            Self::Active(CleanupStage::PostPiBeforeEvent) => "ACTIVE:POST_PI_BEFORE_EVENT",
            Self::RetainRequested => "RETAIN_REQUESTED",
            Self::Retained => "RETAINED",
            Self::CleanupPending => "CLEANUP_PENDING",
            Self::RuntimeStopped => "RUNTIME_STOPPED",
            Self::RuntimeDestroyed => "RUNTIME_DESTROYED",
            Self::GitRecoveredCleaned => "GIT_RECOVERED_CLEANED",
            Self::StorageReleased => "STORAGE_RELEASED",
            Self::ReservationReleased => "RESERVATION_RELEASED",
            Self::Resolved => "RESOLVED",
        };
        formatter.write_str(text)
    }
}

impl FromStr for CleanupDisposition {
    type Err = StoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        use CleanupDisposition as D;
        use CleanupStage as S;
        match value {
            "ACTIVE:RESERVED" | "RESERVED" => Ok(D::Active(S::Reserved)),
            "ACTIVE:STORAGE" | "STORAGE_ALLOCATED" => Ok(D::Active(S::Storage)),
            "ACTIVE:WORKTREE" | "WORKTREE_CREATED" => Ok(D::Active(S::Worktree)),
            "ACTIVE:RUNTIME" | "RUNTIME_CREATED" => Ok(D::Active(S::Runtime)),
            "ACTIVE:PI_STARTED" | "PI_STARTED" => Ok(D::Active(S::PiStarted)),
            "ACTIVE:RUNNING" | "RUNNING" => Ok(D::Active(S::Running)),
            "ACTIVE:POST_PI_BEFORE_EVENT" => Ok(D::Active(S::PostPiBeforeEvent)),
            "RETAIN_REQUESTED" | "REVIEW_READY" => Ok(D::RetainRequested),
            "RETAINED" => Ok(D::Retained),
            "CLEANUP_PENDING" => Ok(D::CleanupPending),
            "RUNTIME_STOPPED" => Ok(D::RuntimeStopped),
            "RUNTIME_DESTROYED" => Ok(D::RuntimeDestroyed),
            "GIT_RECOVERED_CLEANED" => Ok(D::GitRecoveredCleaned),
            "STORAGE_RELEASED" => Ok(D::StorageReleased),
            "RESERVATION_RELEASED" => Ok(D::ReservationReleased),
            "RESOLVED" => Ok(D::Resolved),
            other => Err(StoreError::Conflict(format!(
                "unknown cleanup disposition {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupAuthority {
    pub execution_id: ExecutionId,
    pub attempt_id: AttemptId,
    pub worker_id: WorkerId,
    pub phase: String,
    pub handles: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CleanupAuthority {
    pub fn disposition(&self) -> Result<CleanupDisposition, StoreError> {
        self.phase.parse()
    }
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
    async fn checkpoint(
        &self,
        execution_id: &ExecutionId,
        phase: &str,
        handles: &Value,
    ) -> Result<(), StoreError> {
        let _ = handles;
        self.advance(execution_id, phase).await
    }
    async fn transition(
        &self,
        execution_id: &ExecutionId,
        expected: CleanupDisposition,
        next: CleanupDisposition,
        handles: &Value,
    ) -> Result<(), StoreError> {
        if expected == next || expected.can_transition_to(next) {
            self.checkpoint(execution_id, &next.to_string(), handles)
                .await
        } else {
            Err(StoreError::Conflict(format!(
                "illegal cleanup transition {expected} -> {next}"
            )))
        }
    }
    async fn resolve(&self, execution_id: &ExecutionId) -> Result<(), StoreError>;
    async fn get(&self, execution_id: &ExecutionId) -> Result<CleanupAuthority, StoreError> {
        Err(StoreError::NotFound(execution_id.to_string()))
    }
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
             VALUES ($1, $2, $3, 'ACTIVE:RESERVED') ON CONFLICT (execution_id) DO UPDATE \
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
        let authority = self.get(execution_id).await?;
        self.transition(
            execution_id,
            authority.disposition()?,
            phase.parse()?,
            &authority.handles,
        )
        .await
    }

    async fn checkpoint(
        &self,
        execution_id: &ExecutionId,
        phase: &str,
        handles: &Value,
    ) -> Result<(), StoreError> {
        let authority = self.get(execution_id).await?;
        self.transition(
            execution_id,
            authority.disposition()?,
            phase.parse()?,
            handles,
        )
        .await
    }

    async fn transition(
        &self,
        execution_id: &ExecutionId,
        expected: CleanupDisposition,
        next: CleanupDisposition,
        handles: &Value,
    ) -> Result<(), StoreError> {
        if expected != next && !expected.can_transition_to(next) {
            return Err(StoreError::Conflict(format!(
                "illegal cleanup transition {expected} -> {next}"
            )));
        }
        let updated = sqlx::query(
            "UPDATE cleanup_authorities SET phase = $3, handles = $4, updated_at = now() \
             WHERE execution_id = $1 AND phase IN ($2, $3)",
        )
        .bind(execution_id.as_str())
        .bind(expected.to_string())
        .bind(next.to_string())
        .bind(handles)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 1 {
            Ok(())
        } else {
            let actual = self.get(execution_id).await?.disposition()?;
            Err(StoreError::Conflict(format!(
                "cleanup authority is {actual}, expected {expected}"
            )))
        }
    }

    async fn resolve(&self, execution_id: &ExecutionId) -> Result<(), StoreError> {
        let resolved = sqlx::query(
            "UPDATE cleanup_authorities SET phase = 'RESOLVED', updated_at = now() \
             WHERE execution_id = $1 AND phase IN ('RESERVATION_RELEASED', 'RESOLVED')",
        )
        .bind(execution_id.as_str())
        .execute(&self.pool)
        .await?;
        if resolved.rows_affected() == 1 {
            Ok(())
        } else {
            Err(StoreError::Conflict(format!(
                "cleanup authority {execution_id} is not reservation-released"
            )))
        }
    }

    async fn get(&self, execution_id: &ExecutionId) -> Result<CleanupAuthority, StoreError> {
        let row = sqlx::query(
            "SELECT execution_id, attempt_id, worker_id, phase, handles, created_at, updated_at \
             FROM cleanup_authorities WHERE execution_id = $1",
        )
        .bind(execution_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StoreError::NotFound(execution_id.to_string()))?;
        Ok(CleanupAuthority {
            execution_id: ExecutionId::new(row.try_get::<String, _>("execution_id")?),
            attempt_id: AttemptId::new(row.try_get::<String, _>("attempt_id")?),
            worker_id: WorkerId::new(row.try_get::<String, _>("worker_id")?),
            phase: row.try_get("phase")?,
            handles: row.try_get("handles")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    async fn list_for_worker(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<CleanupAuthority>, StoreError> {
        sqlx::query(
            "SELECT execution_id, attempt_id, worker_id, phase, handles, created_at, updated_at \
             FROM cleanup_authorities WHERE worker_id = $1 AND phase <> 'RESOLVED' \
             ORDER BY updated_at, execution_id",
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
                handles: row.try_get("handles")?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .collect()
    }
}
