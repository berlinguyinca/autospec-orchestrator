use crate::{decode_execution, run_migrations, workers::decode_worker, StoreError};
use async_trait::async_trait;
use orchestrator_core::{AttemptId, Execution, ExecutionId, ExecutionState, WorkerId};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct Reservation {
    pub execution: Execution,
    pub worker_id: WorkerId,
    pub attempt_id: AttemptId,
    pub cpu: u32,
    pub memory_mib: u64,
}

#[async_trait]
pub trait ReservationStore: Send + Sync {
    async fn reserve_next(&self, worker_id: &WorkerId) -> Result<Option<Reservation>, StoreError>;
    async fn release(&self, execution_id: &ExecutionId) -> Result<(), StoreError>;
    async fn list_for_worker(&self, worker_id: &WorkerId) -> Result<Vec<Reservation>, StoreError>;
    async fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<ExecutionId>, StoreError>;
}

#[derive(Debug, Clone)]
pub struct PgReservationStore {
    pool: PgPool,
}

impl PgReservationStore {
    pub async fn connect(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(32)
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
impl ReservationStore for PgReservationStore {
    async fn reserve_next(&self, worker_id: &WorkerId) -> Result<Option<Reservation>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let worker_row = sqlx::query("SELECT * FROM workers WHERE id = $1 FOR UPDATE")
            .bind(worker_id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(worker_id.to_string()))?;
        let worker = decode_worker(&worker_row)?;
        let usage = sqlx::query(
            "SELECT COUNT(*) AS slots, COALESCE(SUM(cpu), 0)::BIGINT AS cpu, \
             COALESCE(SUM(memory_mib), 0)::BIGINT AS memory_mib \
             FROM reservations WHERE worker_id = $1",
        )
        .bind(worker_id.as_str())
        .fetch_one(&mut *transaction)
        .await?;
        let slots = u32::try_from(usage.try_get::<i64, _>("slots")?)
            .map_err(|_| StoreError::Conflict("invalid reservation count".to_owned()))?;
        let used_cpu = u32::try_from(usage.try_get::<i64, _>("cpu")?)
            .map_err(|_| StoreError::Conflict("invalid reserved cpu".to_owned()))?;
        let used_memory = u64::try_from(usage.try_get::<i64, _>("memory_mib")?)
            .map_err(|_| StoreError::Conflict("invalid reserved memory".to_owned()))?;
        if worker.state != orchestrator_core::WorkerState::Ready
            || !worker
                .capability_proof
                .as_ref()
                .is_some_and(orchestrator_core::WorkerCapabilityProof::is_complete)
            || slots >= worker.capabilities.max_concurrent_executions
        {
            transaction.commit().await?;
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT * FROM executions WHERE state = 'QUEUED' \
             ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 64",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let mut selected = None;
        for row in rows {
            let execution = decode_execution(&row)?;
            let requirement = &execution.manifest.runtime;
            let fits = requirement
                .os
                .as_ref()
                .is_none_or(|os| os.eq_ignore_ascii_case(&worker.capabilities.os))
                && worker.capabilities.runtimes.contains(&requirement.kind)
                && requirement.cpu <= worker.capabilities.cpu.saturating_sub(used_cpu)
                && requirement.memory_mib
                    <= worker.capabilities.memory_mib.saturating_sub(used_memory)
                && requirement.disk_gib <= worker.capabilities.disk_gib
                && requirement
                    .capabilities
                    .iter()
                    .all(|capability| worker.capabilities.capabilities.contains(capability));
            if fits {
                selected = Some(execution);
                break;
            }
        }
        let Some(mut execution) = selected else {
            transaction.commit().await?;
            return Ok(None);
        };
        let attempt_id = AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
        sqlx::query(
            "INSERT INTO reservations (execution_id, worker_id, attempt_id, cpu, memory_mib) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(execution.id.as_str())
        .bind(worker_id.as_str())
        .bind(attempt_id.as_str())
        .bind(i32::try_from(execution.manifest.runtime.cpu).map_err(|_| {
            StoreError::Conflict("execution cpu exceeds PostgreSQL INTEGER".to_owned())
        })?)
        .bind(
            i64::try_from(execution.manifest.runtime.memory_mib).map_err(|_| {
                StoreError::Conflict("execution memory exceeds PostgreSQL BIGINT".to_owned())
            })?,
        )
        .execute(&mut *transaction)
        .await?;
        execution
            .transition(ExecutionState::WorkerAssigned)
            .map_err(|_| StoreError::IllegalTransition {
                from: ExecutionState::Queued,
                to: ExecutionState::WorkerAssigned,
            })?;
        execution.worker_id = Some(worker_id.clone());
        execution.attempt_id = Some(attempt_id.clone());
        sqlx::query(
            "UPDATE executions SET state = 'WORKER_ASSIGNED', worker_id = $2, attempt_id = $3, \
             updated_at = $4, version = version + 1 WHERE id = $1",
        )
        .bind(execution.id.as_str())
        .bind(worker_id.as_str())
        .bind(attempt_id.as_str())
        .bind(execution.updated_at)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO execution_attempts (attempt_id, execution_id, worker_id, state) \
             VALUES ($1, $2, $3, 'WORKER_ASSIGNED')",
        )
        .bind(attempt_id.as_str())
        .bind(execution.id.as_str())
        .bind(worker_id.as_str())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE workers SET running_executions = running_executions + 1, updated_at = now() \
             WHERE id = $1",
        )
        .bind(worker_id.as_str())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(Some(Reservation {
            cpu: execution.manifest.runtime.cpu,
            memory_mib: execution.manifest.runtime.memory_mib,
            execution,
            worker_id: worker_id.clone(),
            attempt_id,
        }))
    }

    async fn release(&self, execution_id: &ExecutionId) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        let worker = sqlx::query_scalar::<_, String>(
            "DELETE FROM reservations WHERE execution_id = $1 RETURNING worker_id",
        )
        .bind(execution_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(worker) = worker {
            sqlx::query(
                "UPDATE workers SET running_executions = GREATEST(running_executions - 1, 0), \
                 updated_at = now() WHERE id = $1",
            )
            .bind(worker)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn list_for_worker(&self, worker_id: &WorkerId) -> Result<Vec<Reservation>, StoreError> {
        let rows = sqlx::query(
            "SELECT e.*, r.worker_id AS reservation_worker_id, r.attempt_id AS reservation_attempt_id, \
             r.cpu AS reservation_cpu, r.memory_mib AS reservation_memory_mib \
             FROM reservations r JOIN executions e ON e.id = r.execution_id \
             WHERE r.worker_id = $1 ORDER BY r.created_at, r.execution_id",
        )
        .bind(worker_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(Reservation {
                    execution: decode_execution(row)?,
                    worker_id: WorkerId::new(row.try_get::<String, _>("reservation_worker_id")?),
                    attempt_id: AttemptId::new(row.try_get::<String, _>("reservation_attempt_id")?),
                    cpu: u32::try_from(row.try_get::<i32, _>("reservation_cpu")?)
                        .map_err(|_| StoreError::Conflict("invalid reserved cpu".to_owned()))?,
                    memory_mib: u64::try_from(row.try_get::<i64, _>("reservation_memory_mib")?)
                        .map_err(|_| StoreError::Conflict("invalid reserved memory".to_owned()))?,
                })
            })
            .collect()
    }

    async fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<ExecutionId>, StoreError> {
        let live = live.iter().cloned().collect::<BTreeSet<_>>();
        let rows = sqlx::query("SELECT execution_id FROM reservations")
            .fetch_all(&self.pool)
            .await?;
        let mut released = Vec::new();
        for row in rows {
            let id = ExecutionId::new(row.try_get::<String, _>("execution_id")?);
            if !live.contains(&id) {
                self.release(&id).await?;
                released.push(id);
            }
        }
        Ok(released)
    }
}
