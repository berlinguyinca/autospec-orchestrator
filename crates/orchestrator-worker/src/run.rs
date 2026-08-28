use crate::{cleanup_guard::CleanupGuard, LifecycleError, Worker, WorkerError};
use chrono::Utc;
use futures_util::FutureExt;
use orchestrator_core::{
    event::ExecutionEventKind, Execution, ExecutionEvent, ExecutionResult, ExecutionState,
    FailureClass,
};
use std::{
    any::Any,
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicBool, Ordering},
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
    let mut guard = CleanupGuard::new(worker.lifecycle.clone(), execution);
    let mut tracked = execution.clone();
    let attempted = AssertUnwindSafe(run_inner(worker, &mut tracked, &mut guard, cancelled))
        .catch_unwind()
        .await;
    let mut outcome = match attempted {
        Ok(outcome) => outcome,
        Err(panic) => Err(WorkerError::Panic(panic_message(panic))),
    };
    if outcome.is_err() && !tracked.state.is_terminal() {
        let failure = if !guard.runtime_created {
            FailureClass::EnvironmentFailed
        } else if guard.session.is_some() {
            FailureClass::HarnessFailed
        } else {
            FailureClass::Internal
        };
        persist_failure(worker, &mut tracked, failure).await?;
    }
    let mut cleanup_errors = Vec::new();
    if let Err(error) = guard.cleanup().await {
        cleanup_errors.push(error.to_string());
    }
    if let Err(error) = worker.reservations.release(&execution.id).await {
        cleanup_errors.push(error.to_string());
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
    let worktree = worker
        .lifecycle
        .create_worktree(execution, &receipt)
        .await?;
    execution.worktree_path = Some(worktree.path.clone());
    guard.worktree = Some(worktree.clone());
    execution
        .transition(ExecutionState::Provisioning)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    let environment = worker
        .lifecycle
        .provision(execution, &receipt, &worktree)
        .await?;
    guard.runtime_created = true;
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
    execution
        .transition(ExecutionState::Running)
        .map_err(|error| WorkerError::Invalid(error.to_string()))?;
    record(
        worker,
        execution,
        ExecutionEventKind::AgentStarted {
            session_id: session.id.clone(),
        },
    )
    .await?;
    let mut empty_polls = 0_u32;
    'events: loop {
        if cancelled.load(Ordering::SeqCst) {
            persist_cancelled(worker, execution).await?;
            return Err(WorkerError::Cancelled);
        }
        let events = worker.lifecycle.poll(execution, &session).await?;
        if events.is_empty() {
            empty_polls += 1;
            if empty_polls >= 1_000 {
                return fail(worker, execution, FailureClass::Inactivity).await;
            }
            tokio::task::yield_now().await;
            continue;
        }
        empty_polls = 0;
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
    let capture = worker.lifecycle.capture(&worktree).await?;
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
    record(worker, execution, ExecutionEventKind::ReviewReady).await?;
    Ok(result)
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
