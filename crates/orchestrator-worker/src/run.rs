use crate::{
    cleanup_guard::CleanupGuard, ControlCheckpoint, HealthAssessment, HealthMonitor,
    LifecycleError, Worker, WorkerError,
};
use chrono::Utc;
use futures_util::FutureExt;
use orchestrator_core::{
    event::ExecutionEventKind, Execution, ExecutionControlAction, ExecutionEvent, ExecutionResult,
    ExecutionState, FailureClass, PersistenceMode,
};
use orchestrator_persistence::{CleanupDisposition, CleanupStage, PendingExecutionControl};
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
    let (_controls, receiver) = tokio::sync::mpsc::unbounded_channel();
    run_with_cancel_and_controls(worker, execution, &AtomicBool::new(false), receiver).await
}

pub(crate) async fn run_with_cancel_and_controls(
    worker: &Worker,
    execution: &Execution,
    cancelled: &AtomicBool,
    controls: tokio::sync::mpsc::UnboundedReceiver<PendingExecutionControl>,
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
    let attempted = AssertUnwindSafe(run_inner(
        worker,
        &mut tracked,
        &mut guard,
        cancelled,
        controls,
    ))
    .catch_unwind()
    .await;
    let mut outcome = match attempted {
        Ok(outcome) => outcome,
        Err(panic) => Err(WorkerError::Panic(panic_message(panic))),
    };
    let cancellation_lookup = worker
        .executions
        .cancellation_requested(&execution.id)
        .await;
    let cancellation =
        matches!(&outcome, Err(WorkerError::Cancelled)) || matches!(cancellation_lookup, Ok(true));
    let mut cleanup_errors = Vec::new();
    if let Err(error) = cancellation_lookup {
        cleanup_errors.push(format!("read durable cancellation request: {error}"));
    }
    if outcome.is_err() && !tracked.state.is_terminal() && !cancellation {
        let failure = if guard.environment.is_none() {
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
        let finalized = match worker
            .reservations
            .finalize_cleanup(&execution.id, attempt_id)
            .await
        {
            Ok(_) => true,
            Err(error) => {
                cleanup_errors.push(error.to_string());
                false
            }
        };
        if finalized {
            let authority = worker.cleanup_authorities.get(&execution.id).await;
            match authority {
                Ok(authority) => {
                    if let Err(error) = worker
                        .lifecycle
                        .ack_cleanup_authority_step(
                            &authority,
                            CleanupDisposition::ReservationReleased,
                        )
                        .await
                    {
                        cleanup_errors.push(error.to_string());
                    }
                }
                Err(error) => cleanup_errors.push(error.to_string()),
            }
            if cleanup_errors.is_empty() {
                if let Err(error) = worker.cleanup_authorities.resolve(&execution.id).await {
                    cleanup_errors.push(error.to_string());
                }
            }
        }
    }
    if cleanup_errors.is_empty() && cancellation {
        if let Err(error) = worker
            .executions
            .complete_cancellation(&execution.id, attempt_id)
            .await
        {
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

pub(crate) async fn run_adopted_with_cancel_and_controls(
    worker: &Worker,
    execution: &Execution,
    cancelled: &AtomicBool,
    controls: tokio::sync::mpsc::UnboundedReceiver<PendingExecutionControl>,
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
    let attempted = AssertUnwindSafe(adopt_inner(
        worker,
        &mut tracked,
        &mut guard,
        cancelled,
        controls,
    ))
    .catch_unwind()
    .await;
    let mut outcome = match attempted {
        Ok(outcome) => outcome,
        Err(panic) => Err(WorkerError::Panic(panic_message(panic))),
    };
    let cancellation_lookup = worker
        .executions
        .cancellation_requested(&execution.id)
        .await;
    let cancellation =
        matches!(&outcome, Err(WorkerError::Cancelled)) || matches!(cancellation_lookup, Ok(true));
    let mut cleanup_errors = Vec::new();
    if let Err(error) = cancellation_lookup {
        cleanup_errors.push(format!("read durable cancellation request: {error}"));
    }
    if outcome.is_err() && !tracked.state.is_terminal() && !cancellation {
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
            .finalize_cleanup(&execution.id, attempt_id)
            .await
        {
            cleanup_errors.push(error.to_string());
        } else {
            let authority = worker.cleanup_authorities.get(&execution.id).await;
            match authority {
                Ok(authority) => {
                    if let Err(error) = worker
                        .lifecycle
                        .ack_cleanup_authority_step(
                            &authority,
                            CleanupDisposition::ReservationReleased,
                        )
                        .await
                    {
                        cleanup_errors.push(error.to_string());
                    }
                }
                Err(error) => cleanup_errors.push(error.to_string()),
            }
            if cleanup_errors.is_empty() {
                if let Err(error) = worker.cleanup_authorities.resolve(&execution.id).await {
                    cleanup_errors.push(error.to_string());
                }
            }
        }
    }
    if cleanup_errors.is_empty() && cancellation {
        if let Err(error) = worker
            .executions
            .complete_cancellation(&execution.id, attempt_id)
            .await
        {
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
            .fence_for_cleanup(&execution.id, &cleanup_handles(execution, guard))
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
    guard.destroy_worktree().await?;
    transition_authority(
        worker,
        execution,
        CleanupDisposition::RuntimeDestroyed,
        CleanupDisposition::GitRecoveredCleaned,
        guard,
    )
    .await?;
    guard.release_storage().await?;
    transition_authority(
        worker,
        execution,
        CleanupDisposition::GitRecoveredCleaned,
        CleanupDisposition::StorageReleased,
        guard,
    )
    .await?;
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
        .transition(
            &execution.id,
            expected,
            next,
            &cleanup_handles(execution, guard),
        )
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
    controls: tokio::sync::mpsc::UnboundedReceiver<PendingExecutionControl>,
) -> Result<ExecutionResult, WorkerError> {
    if execution.state == ExecutionState::Provisioning {
        execution
            .transition(ExecutionState::Running)
            .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    } else if !matches!(
        execution.state,
        ExecutionState::Running | ExecutionState::PausedForHuman
    ) {
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
    let worker_id = execution
        .worker_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("adopted execution lacks worker authority".into()))?;
    let has_pending_control = worker
        .executions
        .list_pending_controls(worker_id)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?
        .iter()
        .any(|control| control.request.execution_id == execution.id);
    if execution.state == ExecutionState::Running && !has_pending_control {
        let receipt = guard
            .receipt
            .as_ref()
            .ok_or_else(|| WorkerError::Invalid("adopted execution lacks receipt".into()))?;
        let environment = guard
            .environment
            .as_ref()
            .ok_or_else(|| WorkerError::Invalid("adopted execution lacks runtime".into()))?;
        worker
            .lifecycle
            .resume(execution, receipt, environment, &adopted.session)
            .await?;
    }
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
        adopted.session,
        controls,
    )
    .await
}

async fn run_inner(
    worker: &Worker,
    execution: &mut Execution,
    guard: &mut CleanupGuard,
    cancelled: &AtomicBool,
    controls: tokio::sync::mpsc::UnboundedReceiver<PendingExecutionControl>,
) -> Result<ExecutionResult, WorkerError> {
    if execution.state != ExecutionState::WorkerAssigned {
        return Err(WorkerError::Invalid(format!(
            "execution {} is {:?}, not WORKER_ASSIGNED",
            execution.id, execution.state
        )));
    }
    if cancelled.load(Ordering::SeqCst) {
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
    // Label-scoped cleanup authority must be durable before a runtime can
    // create even its first network/container/volume.
    guard.runtime_created = true;
    transition_cleanup(
        worker,
        execution,
        guard,
        CleanupDisposition::Active(CleanupStage::Worktree),
        CleanupDisposition::Active(CleanupStage::Runtime),
    )
    .await?;
    let environment = worker
        .lifecycle
        .provision(execution, &receipt, &worktree)
        .await?;
    guard.environment = Some(environment.clone());
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
    drive_running(
        worker, execution, guard, cancelled, &worktree, session, controls,
    )
    .await
}

async fn drive_running(
    worker: &Worker,
    execution: &mut Execution,
    guard: &mut CleanupGuard,
    cancelled: &AtomicBool,
    worktree: &git_worktree::Worktree,
    mut session: harness_traits::SessionRef,
    mut controls: tokio::sync::mpsc::UnboundedReceiver<PendingExecutionControl>,
) -> Result<ExecutionResult, WorkerError> {
    let started_at = Instant::now();
    let mut health = HealthMonitor::default_at(started_at);
    if execution.state == ExecutionState::PausedForHuman {
        // An adopted human pause begins paused at the monitor's epoch; no wall
        // or inactivity time may accrue between process start and adoption.
        health.pause(started_at);
    }
    'events: loop {
        if cancelled.load(Ordering::SeqCst) {
            return Err(WorkerError::Cancelled);
        }
        if execution.state == ExecutionState::PausedForHuman {
            tokio::select! {
                control = controls.recv() => {
                    let control = control.ok_or_else(|| WorkerError::Invalid(
                        "interactive control channel closed while paused".into()
                    ))?;
                    handle_control(worker, execution, guard, &mut health, &mut session, control).await?;
                    continue;
                }
                () = cancellation_signal(cancelled) => return Err(WorkerError::Cancelled),
            }
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
            events = worker.lifecycle.poll(execution, &session) => events?,
            control = controls.recv() => {
                let control = control.ok_or_else(|| WorkerError::Invalid(
                    "interactive control channel closed while running".into()
                ))?;
                handle_control(worker, execution, guard, &mut health, &mut session, control).await?;
                continue;
            }
            () = cancellation => {
                return Err(WorkerError::Cancelled);
            }
        };
        if events.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            health.record_event(Instant::now());
        }
        let cpu_percent = tokio::select! {
            cpu_percent = worker.lifecycle.cpu_percent(execution) => cpu_percent?,
            control = controls.recv() => {
                let control = control.ok_or_else(|| WorkerError::Invalid(
                    "interactive control channel closed during health probe".into()
                ))?;
                handle_control(worker, execution, guard, &mut health, &mut session, control).await?;
                continue;
            }
            () = cancellation_signal(cancelled) => return Err(WorkerError::Cancelled),
        };
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
    guard.session = Some(session.clone());
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
            .fence_for_cleanup(&execution.id, &cleanup_handles(execution, guard))
            .await
            .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    }
    Ok(result)
}

async fn cancellation_signal(cancelled: &AtomicBool) {
    while !cancelled.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn handle_control(
    worker: &Worker,
    execution: &mut Execution,
    guard: &mut CleanupGuard,
    health: &mut HealthMonitor,
    session: &mut harness_traits::SessionRef,
    control: PendingExecutionControl,
) -> Result<(), WorkerError> {
    if control.request.execution_id != execution.id {
        return Err(WorkerError::Invalid(
            "interactive control belongs to another execution".into(),
        ));
    }
    let attempt_id = execution
        .attempt_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("interactive control lacks attempt".into()))?;
    let worker_id = execution
        .worker_id
        .as_ref()
        .ok_or_else(|| WorkerError::Invalid("interactive control lacks worker".into()))?;
    worker
        .executions
        .begin_control(control.request.request_id, worker_id, attempt_id)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    worker
        .control_checkpoints
        .reached(ControlCheckpoint::ApplyingPersisted);
    let already_applied =
        control.phase == orchestrator_persistence::ExecutionControlPhase::SideEffectApplied;
    let kind = match control.request.action {
        ExecutionControlAction::Pause => {
            if execution.state != ExecutionState::Running {
                return Err(WorkerError::Invalid("pause requires RUNNING".into()));
            }
            if !already_applied {
                worker.lifecycle.pause(execution, session).await?;
                worker
                    .control_checkpoints
                    .reached(ControlCheckpoint::PauseStopped);
            }
            health.pause(Instant::now());
            execution
                .transition(ExecutionState::PausedForHuman)
                .map_err(|error| WorkerError::Invalid(error.to_string()))?;
            ExecutionEventKind::ExecutionPaused
        }
        ExecutionControlAction::Resume => {
            if execution.state != ExecutionState::PausedForHuman {
                return Err(WorkerError::Invalid(
                    "resume requires PAUSED_FOR_HUMAN".into(),
                ));
            }
            let receipt = guard
                .receipt
                .as_ref()
                .ok_or_else(|| WorkerError::Invalid("resume lacks storage authority".into()))?;
            let environment = guard
                .environment
                .as_ref()
                .ok_or_else(|| WorkerError::Invalid("resume lacks runtime authority".into()))?;
            if !already_applied {
                worker
                    .lifecycle
                    .resume(execution, receipt, environment, session)
                    .await?;
                worker
                    .control_checkpoints
                    .reached(ControlCheckpoint::ResumeLaunched);
            } else {
                // Adoption deliberately quiesces abandoned holds. Reapplying a
                // persisted side effect must restore the exact native session,
                // never replay the task packet.
                worker
                    .lifecycle
                    .resume(execution, receipt, environment, session)
                    .await?;
            }
            worker
                .control_checkpoints
                .reached(ControlCheckpoint::RunningRestored);
            health.resume(Instant::now());
            execution
                .transition(ExecutionState::Running)
                .map_err(|error| WorkerError::Invalid(error.to_string()))?;
            ExecutionEventKind::ExecutionResumed
        }
        ExecutionControlAction::ForkConversation => {
            let original_worktree = session.worktree_path.clone();
            let target = control.target_session_id.as_ref().ok_or_else(|| {
                WorkerError::Invalid("conversation fork lacks fenced target".into())
            })?;
            let was_running = control.accepted_state == ExecutionState::Running;
            let forked = if already_applied {
                harness_traits::SessionRef {
                    id: target.clone(),
                    path: session.path.clone(),
                    execution_id: execution.id.clone(),
                    worktree_path: original_worktree.clone(),
                }
            } else {
                if was_running {
                    worker.lifecycle.pause(execution, session).await?;
                    health.pause(Instant::now());
                }
                let forked = worker
                    .lifecycle
                    .fork_conversation(execution, session, target)
                    .await?;
                worker
                    .control_checkpoints
                    .reached(ControlCheckpoint::ForkLaunched);
                worker.lifecycle.pause(execution, &forked).await?;
                if was_running {
                    let receipt = guard.receipt.as_ref().ok_or_else(|| {
                        WorkerError::Invalid("fork resume lacks storage authority".into())
                    })?;
                    let environment = guard.environment.as_ref().ok_or_else(|| {
                        WorkerError::Invalid("fork resume lacks runtime authority".into())
                    })?;
                    worker
                        .lifecycle
                        .resume(execution, receipt, environment, &forked)
                        .await?;
                    health.resume(Instant::now());
                }
                forked
            };
            if already_applied && was_running {
                let receipt = guard.receipt.as_ref().ok_or_else(|| {
                    WorkerError::Invalid("fork resume lacks storage authority".into())
                })?;
                let environment = guard.environment.as_ref().ok_or_else(|| {
                    WorkerError::Invalid("fork resume lacks runtime authority".into())
                })?;
                worker
                    .lifecycle
                    .resume(execution, receipt, environment, &forked)
                    .await?;
                worker
                    .control_checkpoints
                    .reached(ControlCheckpoint::RunningRestored);
                health.resume(Instant::now());
            }
            if forked.execution_id != execution.id
                || forked.worktree_path != original_worktree
                || forked.id == session.id
            {
                return Err(WorkerError::Invalid(
                    "conversation fork changed execution/workspace authority".into(),
                ));
            }
            *session = forked;
            guard.session = Some(session.clone());
            execution.session_id = Some(session.id.clone());
            execution.updated_at = Utc::now();
            ExecutionEventKind::ConversationForked {
                session_id: session.id.clone(),
            }
        }
    };
    worker
        .executions
        .mark_control_side_effect_applied(control.request.request_id, execution.session_id.as_ref())
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    worker
        .control_checkpoints
        .reached(ControlCheckpoint::SideEffectPersisted);
    let event = ExecutionEvent {
        execution_id: execution.id.clone(),
        attempt_id: execution.attempt_id.clone(),
        sequence: 0,
        at: execution.updated_at,
        state: execution.state,
        kind,
    };
    worker
        .control_checkpoints
        .reached(ControlCheckpoint::BeforeCompletion);
    worker
        .executions
        .complete_control(control.request.request_id, execution, &event)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))?;
    Ok(())
}

async fn transition_cleanup(
    worker: &Worker,
    execution: &Execution,
    guard: &CleanupGuard,
    expected: CleanupDisposition,
    next: CleanupDisposition,
) -> Result<(), WorkerError> {
    let handles = cleanup_handles(execution, guard);
    worker
        .cleanup_authorities
        .transition(&execution.id, expected, next, &handles)
        .await
        .map_err(|error| WorkerError::Persistence(error.to_string()))
}

fn cleanup_handles(execution: &Execution, guard: &CleanupGuard) -> serde_json::Value {
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
        "runtime_selector": guard.runtime_created.then(|| execution.labels.clone()),
        "session": session,
    })
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
