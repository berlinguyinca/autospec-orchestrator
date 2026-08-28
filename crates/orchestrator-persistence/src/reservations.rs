use crate::{
    decode_execution, enum_text, run_migrations, to_json, workers::decode_worker, StoreError,
};
use async_trait::async_trait;
use chrono::Utc;
use orchestrator_core::{
    event::ExecutionEventKind, AttemptId, Execution, ExecutionEvent, ExecutionId, ExecutionResult,
    ExecutionState, FailureClass, PersistenceMode, WorkerId,
};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LostWorkerRecovery {
    Requeued(ExecutionId),
    Failed(ExecutionId),
}

#[async_trait]
pub trait ReservationStore: Send + Sync {
    async fn reserve_next(&self, worker_id: &WorkerId) -> Result<Option<Reservation>, StoreError>;
    async fn release(&self, execution_id: &ExecutionId) -> Result<(), StoreError>;
    async fn list_for_worker(&self, worker_id: &WorkerId) -> Result<Vec<Reservation>, StoreError>;
    async fn reconcile(&self, live: &[ExecutionId]) -> Result<Vec<ExecutionId>, StoreError>;
    async fn recover_unreachable(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<LostWorkerRecovery>, StoreError> {
        Err(StoreError::Conflict(format!(
            "lost-worker recovery is unavailable for {worker_id}"
        )))
    }
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

    async fn recover_unreachable(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Vec<LostWorkerRecovery>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let state =
            sqlx::query_scalar::<_, String>("SELECT state FROM workers WHERE id = $1 FOR UPDATE")
                .bind(worker_id.as_str())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or_else(|| StoreError::NotFound(worker_id.to_string()))?;
        if state != "UNREACHABLE" {
            return Err(StoreError::Conflict(format!(
                "worker {worker_id} is {state}, not UNREACHABLE"
            )));
        }
        let rows = sqlx::query(
            "SELECT e.* FROM reservations r JOIN executions e ON e.id = r.execution_id \
             WHERE r.worker_id = $1 ORDER BY r.created_at, r.execution_id FOR UPDATE OF e, r",
        )
        .bind(worker_id.as_str())
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = Vec::with_capacity(rows.len());
        for row in rows {
            let mut execution = decode_execution(&row)?;
            let attempt_id = execution.attempt_id.clone().ok_or_else(|| {
                StoreError::Conflict("reserved execution lacks attempt id".to_owned())
            })?;
            let resumable = execution.manifest.persistence == PersistenceMode::Resumable;
            let attempt_result = ExecutionResult {
                execution_id: execution.id.clone(),
                state: ExecutionState::Failed,
                failure: Some(FailureClass::WorkerLost),
                branch: None,
                base_sha: None,
                diff_artifact: None,
                artifacts: Vec::new(),
                tests: None,
            };
            let attempt_updated = sqlx::query(
                "UPDATE execution_attempts SET state = 'FAILED', result = $4, updated_at = now(), \
                 finished_at = now() WHERE attempt_id = $1 AND execution_id = $2 AND worker_id = $3 \
                 AND finished_at IS NULL",
            )
            .bind(attempt_id.as_str())
            .bind(execution.id.as_str())
            .bind(worker_id.as_str())
            .bind(to_json(&attempt_result)?)
            .execute(&mut *transaction)
            .await?;
            if attempt_updated.rows_affected() != 1 {
                return Err(StoreError::Conflict(format!(
                    "attempt {} was already fenced",
                    attempt_id
                )));
            }
            let (next, result) = if resumable {
                (ExecutionState::Queued, None)
            } else {
                execution.result = Some(attempt_result.clone());
                (
                    ExecutionState::Failed,
                    execution.result.as_ref().map(to_json).transpose()?,
                )
            };
            let updated = sqlx::query(
                "UPDATE executions SET state = $2, worker_id = NULL, attempt_id = NULL, result = $3, \
                 updated_at = now(), version = version + 1 WHERE id = $1 AND worker_id = $4 \
                 AND attempt_id = $5",
            )
            .bind(execution.id.as_str())
            .bind(enum_text(&next)?)
            .bind(result)
            .bind(worker_id.as_str())
            .bind(attempt_id.as_str())
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(StoreError::Conflict(format!(
                    "execution {} authority changed during recovery",
                    execution.id
                )));
            }
            let sequence = sqlx::query_scalar::<_, i64>(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM execution_events WHERE execution_id = $1",
            )
            .bind(execution.id.as_str())
            .fetch_one(&mut *transaction)
            .await?;
            let event = ExecutionEvent {
                execution_id: execution.id.clone(),
                attempt_id: Some(attempt_id),
                sequence: u64::try_from(sequence).map_err(|_| {
                    StoreError::Conflict("negative recovery event sequence".to_owned())
                })?,
                at: Utc::now(),
                state: next,
                kind: ExecutionEventKind::ExecutionFailed {
                    failure: FailureClass::WorkerLost,
                },
            };
            sqlx::query(
                "INSERT INTO execution_events (execution_id, sequence, at, state, payload) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(execution.id.as_str())
            .bind(sequence)
            .bind(event.at)
            .bind(enum_text(&next)?)
            .bind(to_json(&event)?)
            .execute(&mut *transaction)
            .await?;
            let deleted =
                sqlx::query("DELETE FROM reservations WHERE execution_id = $1 AND worker_id = $2")
                    .bind(execution.id.as_str())
                    .bind(worker_id.as_str())
                    .execute(&mut *transaction)
                    .await?;
            if deleted.rows_affected() != 1 {
                return Err(StoreError::Conflict(
                    "reservation disappeared during recovery".to_owned(),
                ));
            }
            recovered.push(if resumable {
                LostWorkerRecovery::Requeued(execution.id)
            } else {
                LostWorkerRecovery::Failed(execution.id)
            });
        }
        sqlx::query("UPDATE workers SET running_executions = 0, updated_at = now() WHERE id = $1")
            .bind(worker_id.as_str())
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(recovered)
    }
}
