use crate::{enum_from_text, enum_text, from_json, run_migrations, to_json, StoreError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestrator_core::{WorkerId, WorkerRegistration, WorkerState};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

#[async_trait]
pub trait WorkerStore: Send + Sync {
    async fn register(&self, worker: &WorkerRegistration) -> Result<(), StoreError>;
    async fn heartbeat(
        &self,
        worker: &WorkerRegistration,
    ) -> Result<WorkerRegistration, StoreError>;
    async fn get(&self, id: &WorkerId) -> Result<WorkerRegistration, StoreError>;
    async fn list(&self) -> Result<Vec<WorkerRegistration>, StoreError>;
    async fn mark_stale_before(&self, deadline: DateTime<Utc>)
        -> Result<Vec<WorkerId>, StoreError>;
}

#[derive(Debug, Clone)]
pub struct PgWorkerStore {
    pub(crate) pool: PgPool,
}

impl PgWorkerStore {
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

fn validate(worker: &WorkerRegistration) -> Result<(), StoreError> {
    let capabilities = &worker.capabilities;
    if capabilities.cpu == 0
        || capabilities.memory_mib < 1024
        || capabilities.disk_gib == 0
        || capabilities.max_concurrent_executions == 0
        || capabilities.max_concurrent_executions > 64
    {
        return Err(StoreError::Conflict(
            "worker capabilities are outside accepted bounds".to_owned(),
        ));
    }
    if capabilities.capabilities.iter().any(|capability| {
        let capability = capability.to_ascii_lowercase();
        capability.contains("gpu") || capability.contains("vram") || capability.contains("model")
    }) {
        return Err(StoreError::Conflict(
            "inference capabilities do not belong in worker registration".to_owned(),
        ));
    }
    let proof_is_complete = worker
        .capability_proof
        .as_ref()
        .is_some_and(orchestrator_core::WorkerCapabilityProof::is_complete);
    if worker.state == WorkerState::Ready && !proof_is_complete {
        return Err(StoreError::Conflict(
            "Ready worker lacks complete storage and Docker capability proof".to_owned(),
        ));
    }
    Ok(())
}

#[async_trait]
impl WorkerStore for PgWorkerStore {
    async fn register(&self, worker: &WorkerRegistration) -> Result<(), StoreError> {
        validate(worker)?;
        sqlx::query(
            "INSERT INTO workers \
             (id, capabilities, capability_proof, state, running_executions, last_heartbeat, updated_at) \
             VALUES ($1, $2, $3, $4, 0, $5, now()) \
             ON CONFLICT (id) DO UPDATE SET \
               capabilities = EXCLUDED.capabilities, \
               capability_proof = EXCLUDED.capability_proof, \
               state = EXCLUDED.state, \
               last_heartbeat = EXCLUDED.last_heartbeat, \
               updated_at = now()",
        )
        .bind(worker.id.as_str())
        .bind(to_json(&worker.capabilities)?)
        .bind(to_json(&worker.capability_proof)?)
        .bind(enum_text(&worker.state)?)
        .bind(worker.last_heartbeat)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        worker: &WorkerRegistration,
    ) -> Result<WorkerRegistration, StoreError> {
        validate(worker)?;
        let row = sqlx::query(
            "UPDATE workers SET capabilities = $2, capability_proof = $3, state = $4, \
             last_heartbeat = $5, updated_at = now() WHERE id = $1 RETURNING *",
        )
        .bind(worker.id.as_str())
        .bind(to_json(&worker.capabilities)?)
        .bind(to_json(&worker.capability_proof)?)
        .bind(enum_text(&worker.state)?)
        .bind(worker.last_heartbeat)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StoreError::NotFound(worker.id.to_string()))?;
        decode_worker(&row)
    }

    async fn get(&self, id: &WorkerId) -> Result<WorkerRegistration, StoreError> {
        let row = sqlx::query("SELECT * FROM workers WHERE id = $1")
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        decode_worker(&row)
    }

    async fn list(&self) -> Result<Vec<WorkerRegistration>, StoreError> {
        sqlx::query("SELECT * FROM workers ORDER BY id")
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(decode_worker)
            .collect()
    }

    async fn mark_stale_before(
        &self,
        deadline: DateTime<Utc>,
    ) -> Result<Vec<WorkerId>, StoreError> {
        let rows = sqlx::query(
            "UPDATE workers SET state = 'OFFLINE', updated_at = now() \
             WHERE state IN ('READY', 'DRAINING') AND last_heartbeat < $1 RETURNING id",
        )
        .bind(deadline)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                row.try_get::<String, _>("id")
                    .map(WorkerId::new)
                    .map_err(StoreError::from)
            })
            .collect()
    }
}

pub(crate) fn decode_worker(row: &sqlx::postgres::PgRow) -> Result<WorkerRegistration, StoreError> {
    Ok(WorkerRegistration {
        id: WorkerId::new(row.try_get::<String, _>("id")?),
        capabilities: from_json(row.try_get("capabilities")?)?,
        state: enum_from_text::<WorkerState>(row.try_get("state")?)?,
        running_executions: u32::try_from(row.try_get::<i32, _>("running_executions")?)
            .map_err(|_| StoreError::Conflict("negative running execution count".to_owned()))?,
        last_heartbeat: row.try_get("last_heartbeat")?,
        capability_proof: from_json(row.try_get("capability_proof")?)?,
    })
}
