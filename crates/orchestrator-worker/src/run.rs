use crate::{
    cleanup_guard::CleanupGuard, HealthAssessment, HealthMonitor, LifecycleError, Worker,
    WorkerError,
};
use chrono::Utc;
use futures_util::FutureExt;
use orchestrator_core::{
    event::ExecutionEventKind, Execution, ExecutionEvent, ExecutionResult, ExecutionState,
    FailureClass, PersistenceMode,
};
use orchestrator_persistence::{CleanupDisposition, CleanupStage};
use std::{
    any::Any,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

pub(crate) async fn run(
    worker: &Worker,
    execution: &Execution,
) -> Result<ExecutionResult, WorkerError> {
    run_with_cancel(worker, execution, &AtomicBool::new(false)).await
}

pub(crate) async fn run_with_cancel(
    worker: &Worker,
    execution: &Execution,
    cancelled: &AtomicBool,
) -> Result<ExecutionResult, WorkerError> {
    let attempt_id = execution
        .attempt_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("execution lacks attempt authority".into()))?;
    let worker_id = execution
        .worker_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("execution lacks worker authority".into()))?;
    if let Err(error) = worker
        .cleanup_authorities
        .begin(&execution.id, attempt_id, worker_id)
        .await
    {
        let authority = WorkerError::Persistence(error.to_string());
        return match worker.reservations.release(&execution.id).await {
            Ok(()) => Err(authority),
            Err(release) => Err(WorkerError::ExecutionAndCleanup {
                execution: authority.to_string(),
                cleanup: format!("release conflicting reservation: {release}"),
            }),
        };
    }
    let mut guard = CleanupGuard::new(worker.lifecycle.clone(), execution);
    let mut tracked = execution.clone();
    let attempted = AssertUnwindSafe(run_inner(worker, &mut tracked, &mut guard, cancelled))
        .catch_unwind()
        .await;
    let mut outcome = match attempted {
        Ok(outcome) => outcome,
        Err(panic) => Err(WorkerError::Panic(panic_message(panic))),
    };
    let mut cleanup_errors = Vec::new();
    if outcome.is_err() && !tracked.state.is_terminal() {
        let failure = if !guard.runtime_created {
            FailureClass::EnvironmentFailed
        } else if guard.session.is_some() {
            FailureClass::HarnessFailed
        } else {
            FailureClass::Internal
        };
        if let Err(error) = persist_failure(worker, &mut tracked, failure).await {
            cleanup_errors.push(error.to_string());
        }
    }
    if retain_for_resume(&tracked, &outcome) {
        worker
            .reservations
            .commit_retained_and_release_capacity(&execution.id, attempt_id)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        return outcome;
    }
    let cleanup_complete = match cleanup_phases(worker, &tracked, &mut guard).await {
        Ok(()) => true,
        Err(error) => {
            cleanup_errors.push(error.to_string());
            false
        }
    };
    if cleanup_complete {
        let reservation_released = match worker
            .reservations
            .release_attempt(&execution.id, attempt_id)
            .await
        {
            Ok(true) => true,
            Ok(false) => {
                cleanup_errors.push("reservation belongs to another attempt".into());
                false
            }
            Err(error) => {
                cleanup_errors.push(error.to_string());
                false
            }
        };
        if reservation_released {
            if let Err(error) = transition_authority(
                worker,
                &tracked,
                CleanupDisposition::StorageReleased,
                CleanupDisposition::ReservationReleased,
                &guard,
            )
            .await
            {
                cleanup_errors.push(error.to_string());
            }
            if let Err(error) = worker.cleanup_authorities.resolve(&execution.id).await {
                cleanup_errors.push(error.to_string());
            }
        } else {
            cleanup_errors.push("cleanup authority retained for reservation recovery".into());
        }
    }
    if !cleanup_errors.is_empty() {
        let cleanup = cleanup_errors.join("; ");
        outcome = match outcome {
            Ok(_) => Err(WorkerError::Cleanup(cleanup)),
            Err(error) => Err(WorkerError::ExecutionAndCleanup {
                execution: error.to_string(),
                cleanup,
            }),
        };
    }
    outcome
}

pub(crate) async fn run_adopted_with_cancel(
    worker: &Worker,
    execution: &Execution,
    cancelled: &AtomicBool,
) -> Result<ExecutionResult, WorkerError> {
    let attempt_id = execution
        .attempt_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("execution lacks attempt authority".into()))?;
    let worker_id = execution
        .worker_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("execution lacks worker authority".into()))?;
    worker
        .cleanup_authorities
        .begin(&execution.id, attempt_id, worker_id)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    let mut guard = CleanupGuard::new(worker.lifecycle.clone(), execution);
    let mut tracked = execution.clone();
    let attempted = AssertUnwindSafe(adopt_inner(worker, &mut tracked, &mut guard, cancelled))
        .catch_unwind()
        .await;
    let mut outcome = match attempted {
        Ok(outcome) => outcome,
        Err(panic) => Err(WorkerError::Panic(panic_message(panic))),
    };
    let mut cleanup_errors = Vec::new();
    if outcome.is_err() && !tracked.state.is_terminal() {
        if let Err(error) = persist_failure(worker, &mut tracked, FailureClass::WorkerLost).await {
            cleanup_errors.push(error.to_string());
        }
    }
    if outcome.is_err() && guard.receipt.is_none() {
        let durable_recovery = match worker.cleanup_authorities.get(&execution.id).await {
            Ok(authority) => worker
                .recover_cleanup_authority(&authority, &tracked)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = durable_recovery {
            cleanup_errors.push(format!(
                "durable adoption cleanup remains unresolved: {error}"
            ));
        }
        if cleanup_errors.is_empty() {
            return outcome;
        }
        return Err(WorkerError::ExecutionAndCleanup {
            execution: outcome
                .expect_err("adoption recovery branch requires execution failure")
                .to_string(),
            cleanup: cleanup_errors.join("; "),
        });
    }
    if retain_for_resume(&tracked, &outcome) {
        worker
            .reservations
            .commit_retained_and_release_capacity(&execution.id, attempt_id)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        return outcome;
    }
    let cleanup_complete = match cleanup_phases(worker, &tracked, &mut guard).await {
        Ok(()) => true,
        Err(error) => {
            cleanup_errors.push(error.to_string());
            false
        }
    };
    if cleanup_complete {
        if let Err(error) = worker
            .reservations
            .release_attempt(&execution.id, attempt_id)
            .await
        {
            cleanup_errors.push(error.to_string());
        } else if let Err(error) = transition_authority(
            worker,
            &tracked,
            CleanupDisposition::StorageReleased,
            CleanupDisposition::ReservationReleased,
            &guard,
        )
        .await
        {
            cleanup_errors.push(error.to_string());
        } else if let Err(error) = worker.cleanup_authorities.resolve(&execution.id).await {
            cleanup_errors.push(error.to_string());
        }
    }
    if !cleanup_errors.is_empty() {
        let cleanup = cleanup_errors.join("; ");
        outcome = match outcome {
            Ok(_) => Err(WorkerError::Cleanup(cleanup)),
            Err(error) => Err(WorkerError::ExecutionAndCleanup {
                execution: error.to_string(),
                cleanup,
            }),
        };
    }
    outcome
}

async fn cleanup_phases(
    worker: &Worker,
    execution: &Execution,
    guard: &mut CleanupGuard,
) -> Result<(), WorkerError> {
    let authority = worker
        .cleanup_authorities
        .get(&execution.id)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    let disposition = authority
        .disposition()
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    if disposition != CleanupDisposition::CleanupPending {
        worker
            .cleanup_authorities
            .fence_for_cleanup(&execution.id, &cleanup_handles(guard))
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    }
    guard.stop_runtime_process().await?;
    transition_authority(
        worker,
        execution,
        CleanupDisposition::CleanupPending,
        CleanupDisposition::RuntimeStopped,
        guard,
    )
    .await?;
    guard.destroy_runtime().await?;
    transition_authority(
        worker,
        execution,
        CleanupDisposition::RuntimeStopped,
        CleanupDisposition::RuntimeDestroyed,
        guard,
    )
    .await?;
    let worktree_tombstone = guard.worktree.clone();
    guard.destroy_worktree().await?;
    transition_authority(
        worker,
        execution,
        CleanupDisposition::RuntimeDestroyed,
        CleanupDisposition::GitRecoveredCleaned,
        guard,
    )
    .await?;
    if let Some(worktree) = &worktree_tombstone {
        worker.lifecycle.ack_destroy_worktree(worktree).await?;
    }
    let release_tombstone = guard.receipt.clone();
    guard.release_storage().await?;
    transition_authority(
        worker,
        execution,
        CleanupDisposition::GitRecoveredCleaned,
        CleanupDisposition::StorageReleased,
        guard,
    )
    .await?;
    if let Some(receipt) = &release_tombstone {
        worker.lifecycle.ack_release_storage(receipt).await?;
    }
    Ok(())
}

async fn transition_authority(
    worker: &Worker,
    execution: &Execution,
    expected: CleanupDisposition,
    next: CleanupDisposition,
    guard: &CleanupGuard,
) -> Result<(), WorkerError> {
    worker
        .cleanup_authorities
        .transition(&execution.id, expected, next, &cleanup_handles(guard))
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))
}

fn retain_for_resume(
    execution: &Execution,
    outcome: &Result<ExecutionResult, WorkerError>,
) -> bool {
    outcome.is_ok()
        && execution.manifest.persistence == PersistenceMode::Resumable
        && !execution.state.is_terminal()
}

async fn adopt_inner(
    worker: &Worker,
    execution: &mut Execution,
    guard: &mut CleanupGuard,
    cancelled: &AtomicBool,
) -> Result<ExecutionResult, WorkerError> {
    if execution.state == ExecutionState::Provisioning {
        execution
            .transition(ExecutionState::Running)
            .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    } else if execution.state != ExecutionState::Running {
        return Err(WorkerError::Invalid(format!(
            "execution {} is {:?}, not RUNNING",
            execution.id, execution.state
        )));
    }
    let adopted = worker.lifecycle.adopt(execution).await?;
    guard.receipt = Some(adopted.receipt);
    guard.worktree = Some(adopted.worktree.clone());
    guard.environment = Some(adopted.environment);
    guard.runtime_created = true;
    guard.session = Some(adopted.session.clone());
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Running),
        CleanupDisposition::Active(CleanupStage::Running),
    )
    .await?;
    drive_running(
        worker,
        execution,
        guard,
        cancelled,
        &adopted.worktree,
        &adopted.session,
    )
    .await
}

async fn run_inner(
    worker: &Worker,
    execution: &mut Execution,
    guard: &mut CleanupGuard,
    cancelled: &AtomicBool,
) -> Result<ExecutionResult, WorkerError> {
    if execution.state != ExecutionState::WorkerAssigned {
        return Err(WorkerError::Invalid(format!(
            "execution {} is {:?}, not WORKER_ASSIGNED",
            execution.id, execution.state
        )));
    }
    if cancelled.load(Ordering::SeqCst) {
        persist_cancelled(worker, execution).await?;
        return Err(WorkerError::Cancelled);
    }
    let receipt = worker.lifecycle.allocate(execution).await?;
    guard.receipt = Some(receipt.clone());
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Reserved),
        CleanupDisposition::Active(CleanupStage::Storage),
    )
    .await?;
    let worktree = worker
        .lifecycle
        .create_worktree(execution, &receipt)
        .await?;
    execution.worktree_path = Some(worktree.path.clone());
    guard.worktree = Some(worktree.clone());
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Storage),
        CleanupDisposition::Active(CleanupStage::Worktree),
    )
    .await?;
    execution
        .transition(ExecutionState::Provisioning)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    let environment = worker
        .lifecycle
        .provision(execution, &receipt, &worktree)
        .await?;
    guard.environment = Some(environment.clone());
    guard.runtime_created = true;
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Worktree),
        CleanupDisposition::Active(CleanupStage::Runtime),
    )
    .await?;
    record(worker, execution, ExecutionEventKind::EnvironmentReady).await?;
    let packet = execution.manifest.task_packet.as_ref().ok_or_else(|| {
        WorkerError::Invalid("execution manifest lacks compact TaskPacket".to_owned())
    })?;
    let session = worker
        .lifecycle
        .start(execution, &receipt, &environment, &worktree, packet)
        .await?;
    execution.session_id = Some(session.id.clone());
    guard.session = Some(session.clone());
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Runtime),
        CleanupDisposition::Active(CleanupStage::PiStarted),
    )
    .await?;
    execution
        .transition(ExecutionState::Running)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::PiStarted),
        CleanupDisposition::Active(CleanupStage::Running),
    )
    .await?;
    drive_running(worker, execution, guard, cancelled, &worktree, &session).await
}

async fn drive_running(
    worker: &Worker,
    execution: &mut Execution,
    guard: &mut CleanupGuard,
    cancelled: &AtomicBool,
    worktree: &git_worktree::Worktree,
    session: &harness_traits::SessionRef,
) -> Result<ExecutionResult, WorkerError> {
    let mut health = HealthMonitor::default_at(Instant::now());
    'events: loop {
        if cancelled.load(Ordering::SeqCst) {
            persist_cancelled(worker, execution).await?;
            return Err(WorkerError::Cancelled);
        }
        let cancellation = async {
            loop {
                if cancelled.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        };
        let events = tokio::select! {
            events = worker.lifecycle.poll(execution, session) => events?,
            () = cancellation => {
                persist_cancelled(worker, execution).await?;
                return Err(WorkerError::Cancelled);
            }
        };
        if events.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            health.record_event(Instant::now());
        }
        let cpu_percent = worker.lifecycle.cpu_percent(execution).await?;
        match health.assess(Instant::now(), cpu_percent) {
            HealthAssessment::Healthy => {}
            HealthAssessment::InactiveWarning { seconds } => {
                record(
                    worker,
                    execution,
                    ExecutionEventKind::AgentInactive { seconds },
                )
                .await?;
            }
            HealthAssessment::Failed(failure) => return fail(worker, execution, failure).await,
        }
        if events.is_empty() {
            continue;
        }
        for mut event in events {
            match &event.kind {
                ExecutionEventKind::ReviewReady => break 'events,
                ExecutionEventKind::ExecutionFailed { failure } => {
                    return fail(worker, execution, *failure).await;
                }
                _ => {
                    event.state = execution.state;
                    event.attempt_id = execution.attempt_id.clone();
                    worker
                        .executions
                        .record_progress(execution, &event)
                        .await
                        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
                }
            }
        }
    }
    guard.stop_agent().await?;
    let capture = worker.lifecycle.capture(worktree).await?;
    let artifact = worker
        .lifecycle
        .persist_evidence(execution, &capture)
        .await?;
    execution
        .transition(ExecutionState::ReviewReady)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    let result = ExecutionResult {
        execution_id: execution.id.clone(),
        state: execution.state,
        failure: None,
        branch: Some(worktree.branch.clone()),
        base_sha: Some(worktree.base_sha.clone()),
        diff_artifact: Some(artifact),
        artifacts: Vec::new(),
        tests: None,
    };
    execution.result = Some(result.clone());
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Running),
        CleanupDisposition::Active(CleanupStage::PostPiBeforeEvent),
    )
    .await?;
    let event = ExecutionEvent {
        execution_id: execution.id.clone(),
        attempt_id: execution.attempt_id.clone(),
        sequence: 0,
        at: Utc::now(),
        state: execution.state,
        kind: ExecutionEventKind::ReviewReady,
    };
    if execution.manifest.persistence == PersistenceMode::Resumable {
        worker
            .executions
            .record_progress_and_request_retention(execution, &event)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    } else {
        worker
            .executions
            .record_progress(execution, &event)
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
        worker
            .cleanup_authorities
            .fence_for_cleanup(&execution.id, &cleanup_handles(guard))
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    }
    Ok(result)
}

async fn transition_cleanup(
    worker: &Worker,
    execution: &Execution,
    guard: &CleanupGuard,
    expected: CleanupDisposition,
    next: CleanupDisposition,
) -> Result<(), WorkerError> {
    let handles = cleanup_handles(guard);
    worker
        .cleanup_authorities
        .transition(&execution.id, expected, next, &handles)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))
}

fn cleanup_handles(guard: &CleanupGuard) -> serde_json::Value {
    let receipt = guard
        .receipt
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .unwrap_or(None);
    let worktree = guard.worktree.as_ref().map(|worktree| {
        serde_json::json!({
            "execution_id": worktree.execution_id,
            "path": worktree.path,
            "branch": worktree.branch,
            "base_sha": worktree.base_sha,
            "repository": worktree.repository,
        })
    });
    let runtime = guard.environment.as_ref().map(|environment| {
        serde_json::json!({
            "execution_id": environment.execution_id,
            "network": environment.network,
            "agent_container": environment.agent_container,
            "container_id": environment.verified_agent_container.container_id,
            "daemon_id": environment.verified_agent_container.daemon_id,
            "labels": environment.verified_agent_container.labels,
            "mounts": environment.verified_agent_container.mounts.iter().map(|mount| serde_json::json!({
                "source": mount.source,
                "target": mount.target,
                "writable": mount.writable,
            })).collect::<Vec<_>>(),
            "service_containers": environment.service_containers,
            "volumes": environment.volumes,
            "credentials_path": environment.credentials_path,
        })
    });
    let session = guard.session.as_ref().map(|session| {
        serde_json::json!({
            "id": session.id,
            "path": session.path,
            "execution_id": session.execution_id,
            "worktree_path": session.worktree_path,
        })
    });
    serde_json::json!({
        "receipt": receipt,
        "worktree": worktree,
        "runtime": runtime,
        "session": session,
    })
}

async fn persist_cancelled(worker: &Worker, execution: &mut Execution) -> Result<(), WorkerError> {
    execution
        .transition(ExecutionState::Cancelled)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    execution.result = Some(ExecutionResult {
        execution_id: execution.id.clone(),
        state: ExecutionState::Cancelled,
        failure: Some(FailureClass::Cancelled),
        branch: None,
        base_sha: None,
        diff_artifact: None,
        artifacts: Vec::new(),
        tests: None,
    });
    record(worker, execution, ExecutionEventKind::ExecutionCancelled).await
}

async fn fail(
    worker: &Worker,
    execution: &mut Execution,
    failure: FailureClass,
) -> Result<ExecutionResult, WorkerError> {
    persist_failure(worker, execution, failure).await?;
    Err(WorkerError::Lifecycle(LifecycleError::Step(format!(
        "execution failed: {failure:?}"
    ))))
}

async fn persist_failure(
    worker: &Worker,
    execution: &mut Execution,
    failure: FailureClass,
) -> Result<(), WorkerError> {
    execution
        .transition(ExecutionState::Failed)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    execution.result = Some(ExecutionResult {
        execution_id: execution.id.clone(),
        state: ExecutionState::Failed,
        failure: Some(failure),
        branch: None,
        base_sha: None,
        diff_artifact: None,
        artifacts: Vec::new(),
        tests: None,
    });
    record(
        worker,
        execution,
        ExecutionEventKind::ExecutionFailed { failure },
    )
    .await?;
    Ok(())
}

async fn record(
    worker: &Worker,
    execution: &Execution,
    kind: ExecutionEventKind,
) -> Result<(), WorkerError> {
    worker
        .executions
        .record_progress(
            execution,
            &ExecutionEvent {
                execution_id: execution.id.clone(),
                attempt_id: execution.attempt_id.clone(),
                sequence: 0,
                at: Utc::now(),
                state: execution.state,
                kind,
            },
        )
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    Ok(())
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic".to_owned()
    }
}
