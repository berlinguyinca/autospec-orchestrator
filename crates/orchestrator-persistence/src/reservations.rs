use crate::{
    decode_execution, enum_text, event_log::append_in_transaction, run_migrations, to_json,
    workers::decode_worker, StoreError,
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
    CleanupPending(ExecutionId),
    Requeued(ExecutionId),
    ReviewReady(ExecutionId),
    Failed(ExecutionId),
}

#[async_trait]
pub trait ReservationStore: Send + Sync {
    async fn reserve_next(&self, worker_id: &WorkerId) -> Result<Option<Reservation>, StoreError>;
    async fn release(&self, execution_id: &ExecutionId) -> Result<(), StoreError>;
    async fn release_attempt(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<bool, StoreError> {
        let _ = attempt_id;
        self.release(execution_id).await?;
        Ok(true)
    }
    async fn commit_retained_and_release_capacity(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<(), StoreError> {
        Err(StoreError::Conflict(format!(
            "atomic retained capacity release is unavailable for {execution_id}/{attempt_id}"
        )))
    }
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
    async fn fence_lost_attempt(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<LostWorkerRecovery, StoreError> {
        Err(StoreError::Conflict(format!(
            "startup fencing is unavailable for {execution_id}/{attempt_id}"
        )))
    }
    async fn finalize_cleanup(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<LostWorkerRecovery, StoreError> {
        Err(StoreError::Conflict(format!(
            "atomic cleanup finalization is unavailable for {execution_id}/{attempt_id}"
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
             AND NOT EXISTS (SELECT 1 FROM cleanup_authorities c \
                 WHERE c.execution_id = executions.id AND c.phase <> 'RESOLVED') \
             ORDER BY created_at, id FOR UPDATE SKIP LOCKED",
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

    async fn release_attempt(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<bool, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let worker = sqlx::query_scalar::<_, String>(
            "DELETE FROM reservations WHERE execution_id = $1 AND attempt_id = $2 \
             RETURNING worker_id",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(worker) = &worker {
            sqlx::query(
                "UPDATE workers SET running_executions = GREATEST(running_executions - 1, 0), \
                 updated_at = now() WHERE id = $1",
            )
            .bind(worker)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(worker.is_some())
    }

    async fn commit_retained_and_release_capacity(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<(), StoreError> {
        let mut transaction = self.pool.begin().await?;
        let execution = sqlx::query("SELECT * FROM executions WHERE id = $1 FOR UPDATE")
            .bind(execution_id.as_str())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(execution_id.to_string()))?;
        let execution = decode_execution(&execution)?;
        if execution.state != ExecutionState::ReviewReady
            || execution.manifest.persistence != PersistenceMode::Resumable
            || execution.attempt_id.as_ref() != Some(attempt_id)
        {
            return Err(StoreError::Conflict(format!(
                "execution {execution_id} is not resumable ReviewReady authority for {attempt_id}"
            )));
        }
        let authority = sqlx::query(
            "SELECT phase, worker_id FROM cleanup_authorities \
             WHERE execution_id = $1 AND attempt_id = $2 FOR UPDATE",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::NotFound(execution_id.to_string()))?;
        let phase = authority.try_get::<String, _>("phase")?;
        if phase != "RETAIN_REQUESTED" && phase != "RETAINED" {
            return Err(StoreError::Conflict(format!(
                "cleanup authority is {phase}, expected RETAIN_REQUESTED or RETAINED"
            )));
        }
        let worker_id = authority.try_get::<String, _>("worker_id")?;
        sqlx::query(
            "SELECT attempt_id FROM execution_attempts \
             WHERE attempt_id = $1 AND execution_id = $2 AND worker_id = $3 FOR UPDATE",
        )
        .bind(attempt_id.as_str())
        .bind(execution_id.as_str())
        .bind(&worker_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::Conflict(format!("attempt {attempt_id} authority is absent")))?;
        sqlx::query("SELECT id FROM workers WHERE id = $1 FOR UPDATE")
            .bind(&worker_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| StoreError::NotFound(worker_id.clone()))?;
        let reservation_worker = sqlx::query_scalar::<_, String>(
            "DELETE FROM reservations WHERE execution_id = $1 AND attempt_id = $2 \
             RETURNING worker_id",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if phase == "RETAIN_REQUESTED" && reservation_worker.is_none() {
            return Err(StoreError::Conflict(
                "retention request lost its reservation before atomic commit".to_owned(),
            ));
        }
        if let Some(reservation_worker) = reservation_worker {
            if reservation_worker != worker_id {
                return Err(StoreError::Conflict(
                    "retention reservation belongs to another worker".to_owned(),
                ));
            }
            sqlx::query(
                "UPDATE workers SET running_executions = GREATEST(running_executions - 1, 0), \
                 updated_at = now() WHERE id = $1",
            )
            .bind(&worker_id)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE cleanup_authorities SET phase = 'RETAINED', updated_at = now() \
             WHERE execution_id = $1 AND attempt_id = $2 AND phase IN ('RETAIN_REQUESTED', 'RETAINED')",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .execute(&mut *transaction)
        .await?;
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
            "SELECT e.*, r.attempt_id AS reservation_attempt_id FROM reservations r \
             JOIN executions e ON e.id = r.execution_id \
             WHERE r.worker_id = $1 ORDER BY r.created_at, r.execution_id FOR UPDATE OF e, r",
        )
        .bind(worker_id.as_str())
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = Vec::with_capacity(rows.len());
        for row in rows {
            let execution = decode_execution(&row)?;
            let attempt_id = AttemptId::new(row.try_get::<String, _>("reservation_attempt_id")?);
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
                // A prior recovery may have fenced the attempt while durable
                // cleanup authority intentionally retained its reservation.
                // Leave that reservation untouched for exact resource recovery
                // and continue reaping unrelated executions.
                continue;
            }
            sqlx::query(
                "INSERT INTO cleanup_authorities (execution_id, attempt_id, worker_id, phase) \
                 VALUES ($1, $2, $3, 'CLEANUP_PENDING') ON CONFLICT (execution_id) DO UPDATE \
                 SET phase = 'CLEANUP_PENDING', updated_at = now() \
                 WHERE cleanup_authorities.attempt_id = EXCLUDED.attempt_id \
                 AND cleanup_authorities.worker_id = EXCLUDED.worker_id \
                 AND cleanup_authorities.phase NOT IN ('RUNTIME_STOPPED', 'RUNTIME_DESTROYED', \
                     'GIT_RECOVERED_CLEANED', 'STORAGE_RELEASED', 'RESERVATION_RELEASED', 'RESOLVED')",
            )
            .bind(execution.id.as_str())
            .bind(attempt_id.as_str())
            .bind(worker_id.as_str())
            .execute(&mut *transaction)
            .await?;
            recovered.push(LostWorkerRecovery::CleanupPending(execution.id));
        }
        transaction.commit().await?;
        Ok(recovered)
    }

    async fn fence_lost_attempt(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<LostWorkerRecovery, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT e.* FROM executions e JOIN reservations r ON r.execution_id = e.id \
             WHERE e.id = $1 AND e.attempt_id = $2 AND r.attempt_id = $2 FOR UPDATE OF e, r",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| {
            StoreError::Conflict(format!(
                "startup authority changed for {execution_id}/{attempt_id}"
            ))
        })?;
        let execution = decode_execution(&row)?;
        let worker_id = execution.worker_id.clone().ok_or_else(|| {
            StoreError::Conflict("startup-fenced execution lacks worker authority".to_owned())
        })?;
        let result = ExecutionResult {
            execution_id: execution.id.clone(),
            state: ExecutionState::Failed,
            failure: Some(FailureClass::WorkerLost),
            branch: None,
            base_sha: None,
            diff_artifact: None,
            artifacts: Vec::new(),
            tests: None,
        };
        let fenced = sqlx::query(
            "UPDATE execution_attempts SET state = 'FAILED', result = $4, updated_at = now(), \
             finished_at = now() WHERE attempt_id = $1 AND execution_id = $2 AND worker_id = $3 \
             AND finished_at IS NULL",
        )
        .bind(attempt_id.as_str())
        .bind(execution_id.as_str())
        .bind(worker_id.as_str())
        .bind(to_json(&result)?)
        .execute(&mut *transaction)
        .await?;
        if fenced.rows_affected() != 1 {
            let phase = sqlx::query_scalar::<_, String>(
                "SELECT c.phase FROM cleanup_authorities c \
                 JOIN execution_attempts a ON a.attempt_id = c.attempt_id \
                 WHERE c.execution_id = $1 AND c.attempt_id = $2 AND c.worker_id = $3 \
                 AND a.finished_at IS NOT NULL FOR UPDATE OF c, a",
            )
            .bind(execution_id.as_str())
            .bind(attempt_id.as_str())
            .bind(worker_id.as_str())
            .fetch_optional(&mut *transaction)
            .await?;
            if !phase.as_deref().is_some_and(|phase| {
                matches!(
                    phase,
                    "CLEANUP_PENDING"
                        | "RUNTIME_STOPPED"
                        | "RUNTIME_DESTROYED"
                        | "GIT_RECOVERED_CLEANED"
                        | "STORAGE_RELEASED"
                        | "RESERVATION_RELEASED"
                        | "RESOLVED"
                )
            }) {
                return Err(StoreError::Conflict(format!(
                    "attempt {attempt_id} was already fenced without exact cleanup authority"
                )));
            }
            transaction.commit().await?;
            return Ok(LostWorkerRecovery::CleanupPending(execution_id.clone()));
        }
        sqlx::query(
            "INSERT INTO cleanup_authorities (execution_id, attempt_id, worker_id, phase) \
             VALUES ($1, $2, $3, 'CLEANUP_PENDING') ON CONFLICT (execution_id) DO UPDATE \
             SET phase = 'CLEANUP_PENDING', updated_at = now() \
             WHERE cleanup_authorities.attempt_id = EXCLUDED.attempt_id \
             AND cleanup_authorities.worker_id = EXCLUDED.worker_id \
             AND cleanup_authorities.phase NOT IN ('RUNTIME_STOPPED', 'RUNTIME_DESTROYED', \
                 'GIT_RECOVERED_CLEANED', 'STORAGE_RELEASED', 'RESERVATION_RELEASED', 'RESOLVED')",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .bind(worker_id.as_str())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(LostWorkerRecovery::CleanupPending(execution_id.clone()))
    }

    async fn finalize_cleanup(
        &self,
        execution_id: &ExecutionId,
        attempt_id: &AttemptId,
    ) -> Result<LostWorkerRecovery, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT e.*, c.phase AS cleanup_phase, c.worker_id AS cleanup_worker_id \
             FROM executions e JOIN cleanup_authorities c ON c.execution_id = e.id \
             AND c.attempt_id = $2 WHERE e.id = $1 \
             AND c.phase IN ('STORAGE_RELEASED', 'RESERVATION_RELEASED') FOR UPDATE OF e, c",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| {
            StoreError::Conflict(format!(
                "lost cleanup is not ready to finalize for {execution_id}/{attempt_id}"
            ))
        })?;
        let execution = decode_execution(&row)?;
        let phase = row.try_get::<String, _>("cleanup_phase")?;
        let authority_worker = row.try_get::<String, _>("cleanup_worker_id")?;
        let outcome = cleanup_outcome(&execution);
        let attempt = sqlx::query(
            "SELECT worker_id, finished_at FROM execution_attempts \
             WHERE attempt_id = $1 AND execution_id = $2 FOR UPDATE",
        )
        .bind(attempt_id.as_str())
        .bind(execution_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| StoreError::Conflict(format!("attempt {attempt_id} authority is absent")))?;
        let attempt_worker = attempt.try_get::<String, _>("worker_id")?;
        if attempt_worker != authority_worker {
            return Err(StoreError::Conflict(
                "cleanup authority and attempt worker differ".to_owned(),
            ));
        }
        if phase == "RESERVATION_RELEASED" {
            transaction.commit().await?;
            return Ok(outcome);
        }
        let worker_id = sqlx::query_scalar::<_, String>(
            "SELECT worker_id FROM reservations WHERE execution_id = $1 AND attempt_id = $2 FOR UPDATE",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        if worker_id.is_none()
            && attempt
                .try_get::<Option<chrono::DateTime<Utc>>, _>("finished_at")?
                .is_none()
            && execution.state != ExecutionState::ReviewReady
            && !execution.state.is_terminal()
        {
            return Err(StoreError::Conflict(
                "cleanup has neither an exact reservation nor fenced attempt evidence".to_owned(),
            ));
        }
        if let Some(worker_id) = &worker_id {
            if worker_id != &authority_worker {
                return Err(StoreError::Conflict(
                    "cleanup reservation belongs to another worker".to_owned(),
                ));
            }
            sqlx::query("SELECT id FROM workers WHERE id = $1 FOR UPDATE")
                .bind(worker_id)
                .fetch_one(&mut *transaction)
                .await?;
        }
        let lost = !execution.state.is_terminal();
        let review_ready = execution.state == ExecutionState::ReviewReady;
        let resumable =
            lost && !review_ready && execution.manifest.persistence == PersistenceMode::Resumable;
        let result = ExecutionResult {
            execution_id: execution_id.clone(),
            state: ExecutionState::Failed,
            failure: Some(FailureClass::WorkerLost),
            branch: None,
            base_sha: None,
            diff_artifact: None,
            artifacts: Vec::new(),
            tests: None,
        };
        let next = if review_ready {
            ExecutionState::ReviewReady
        } else if resumable {
            ExecutionState::Queued
        } else if lost {
            ExecutionState::Failed
        } else {
            execution.state
        };
        let persisted_result = if review_ready {
            execution.result.as_ref().map(to_json).transpose()?
        } else if resumable {
            None
        } else if lost {
            Some(to_json(&result)?)
        } else {
            execution.result.as_ref().map(to_json).transpose()?
        };
        sqlx::query(
            "UPDATE executions SET state = $2, worker_id = NULL, attempt_id = NULL, \
             session_id = NULL, worktree_path = NULL, result = $3, \
             updated_at = now(), version = version + 1 WHERE id = $1 AND attempt_id = $4",
        )
        .bind(execution_id.as_str())
        .bind(enum_text(&next)?)
        .bind(persisted_result)
        .bind(attempt_id.as_str())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE execution_attempts SET state = $3, result = COALESCE($4, result), \
             updated_at = now(), finished_at = COALESCE(finished_at, now()) \
             WHERE attempt_id = $1 AND execution_id = $2",
        )
        .bind(attempt_id.as_str())
        .bind(execution_id.as_str())
        .bind(enum_text(&if resumable {
            ExecutionState::Failed
        } else {
            next
        })?)
        .bind(if resumable {
            Some(to_json(&result)?)
        } else {
            None
        })
        .execute(&mut *transaction)
        .await?;
        let deleted =
            sqlx::query("DELETE FROM reservations WHERE execution_id = $1 AND attempt_id = $2")
                .bind(execution_id.as_str())
                .bind(attempt_id.as_str())
                .execute(&mut *transaction)
                .await?;
        if deleted.rows_affected() == 1 {
            sqlx::query(
                "UPDATE workers SET running_executions = GREATEST(running_executions - 1, 0), updated_at = now() WHERE id = $1",
            )
            .bind(&authority_worker)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            "UPDATE cleanup_authorities SET phase = 'RESERVATION_RELEASED', updated_at = now() \
             WHERE execution_id = $1 AND attempt_id = $2 AND phase = 'STORAGE_RELEASED'",
        )
        .bind(execution_id.as_str())
        .bind(attempt_id.as_str())
        .execute(&mut *transaction)
        .await?;
        if next != execution.state {
            let kind = match next {
                ExecutionState::Queued => ExecutionEventKind::ExecutionRequeued {
                    failure: FailureClass::WorkerLost,
                },
                ExecutionState::ReviewReady => ExecutionEventKind::ReviewReady,
                ExecutionState::Failed => ExecutionEventKind::ExecutionFailed {
                    failure: execution
                        .result
                        .as_ref()
                        .and_then(|result| result.failure)
                        .unwrap_or(FailureClass::WorkerLost),
                },
                ExecutionState::Cancelled => ExecutionEventKind::ExecutionCancelled,
                ExecutionState::Completed => ExecutionEventKind::ExecutionCompleted,
                other => {
                    return Err(StoreError::Conflict(format!(
                        "cleanup finalization cannot publish state {other:?}"
                    )))
                }
            };
            append_in_transaction(
                &mut transaction,
                &ExecutionEvent {
                    execution_id: execution_id.clone(),
                    attempt_id: Some(attempt_id.clone()),
                    sequence: 0,
                    at: Utc::now(),
                    state: next,
                    kind,
                },
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(outcome)
    }
}

fn cleanup_outcome(execution: &Execution) -> LostWorkerRecovery {
    if execution.state == ExecutionState::ReviewReady {
        LostWorkerRecovery::ReviewReady(execution.id.clone())
    } else if !execution.state.is_terminal()
        && execution.manifest.persistence == PersistenceMode::Resumable
    {
        LostWorkerRecovery::Requeued(execution.id.clone())
    } else {
        LostWorkerRecovery::Failed(execution.id.clone())
    }
}
