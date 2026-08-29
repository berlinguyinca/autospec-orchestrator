use chrono::{Duration, Utc};
use orchestrator_core::{
    event::ExecutionEventKind, AgentAssignment, Execution, ExecutionControlAction, ExecutionEvent,
    ExecutionId, ExecutionManifest, ExecutionResult, ExecutionState, HarnessKind, ModelPolicy,
    OwnershipLabels, PersistenceMode, RepositoryReference, Role, RuntimeRequirement, SessionId,
    WorkerId,
};

#[tokio::test]
async fn interactive_intents_are_ordered_restart_visible_and_complete_exactly_once() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let events = PgEventLog::connect(&database_url).await.unwrap();
    let worker = registered_worker(
        &format!("worker-controls-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let mut queued = execution(ExecutionState::Queued);
    queued.manifest.persistence = PersistenceMode::Resumable;
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut running = reservation.execution;
    running.transition(ExecutionState::Provisioning).unwrap();
    running.worktree_path = Some("/bounded/execution/repository".into());
    executions
        .record_progress(
            &running,
            &progress_event(&running, ExecutionEventKind::EnvironmentReady),
        )
        .await
        .unwrap();
    running.transition(ExecutionState::Running).unwrap();
    running.session_id = Some(SessionId::new("session-original"));
    executions
        .record_progress(
            &running,
            &progress_event(
                &running,
                ExecutionEventKind::AgentStarted {
                    session_id: SessionId::new("session-original"),
                },
            ),
        )
        .await
        .unwrap();

    let pause = executions
        .request_control(&running.id, ExecutionControlAction::Pause, "pause-once")
        .await
        .unwrap();
    assert!(pause.created);
    assert!(
        !executions
            .request_control(&running.id, ExecutionControlAction::Pause, "pause-once")
            .await
            .unwrap()
            .created
    );
    assert_eq!(
        executions
            .list_pending_controls(&worker.id)
            .await
            .unwrap()
            .iter()
            .filter(|control| control.execution.id == running.id)
            .count(),
        1
    );

    let mut paused = running.clone();
    paused.transition(ExecutionState::PausedForHuman).unwrap();
    let paused_event = progress_event(&paused, ExecutionEventKind::ExecutionPaused);
    executions
        .begin_control(
            pause.request.request_id,
            &worker.id,
            running.attempt_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    executions
        .mark_control_side_effect_applied(pause.request.request_id, None)
        .await
        .unwrap();
    assert!(executions
        .complete_control(pause.request.request_id, &paused, &paused_event)
        .await
        .unwrap()
        .is_some());
    assert!(executions
        .complete_control(pause.request.request_id, &paused, &paused_event)
        .await
        .unwrap()
        .is_none());

    let fork = executions
        .request_control(
            &running.id,
            ExecutionControlAction::ForkConversation,
            "fork-once",
        )
        .await
        .unwrap();
    let mut forked = paused.clone();
    let fork_target = executions
        .list_pending_controls(&worker.id)
        .await
        .unwrap()
        .into_iter()
        .find(|control| control.request.request_id == fork.request.request_id)
        .unwrap()
        .target_session_id
        .unwrap();
    executions
        .begin_control(
            fork.request.request_id,
            &worker.id,
            running.attempt_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    executions
        .mark_control_side_effect_applied(fork.request.request_id, Some(&fork_target))
        .await
        .unwrap();
    forked.session_id = Some(fork_target.clone());
    forked.updated_at = Utc::now();
    executions
        .complete_control(
            fork.request.request_id,
            &forked,
            &progress_event(
                &forked,
                ExecutionEventKind::ConversationForked {
                    session_id: fork_target.clone(),
                },
            ),
        )
        .await
        .unwrap();
    let persisted_fork = executions.get(&running.id).await.unwrap();
    assert_eq!(persisted_fork.worktree_path, running.worktree_path);
    assert_eq!(
        persisted_fork.session_id.as_ref().map(SessionId::as_str),
        Some(fork_target.as_str())
    );

    let resume = executions
        .request_control(&running.id, ExecutionControlAction::Resume, "resume-once")
        .await
        .unwrap();
    let mut resumed = forked;
    resumed.transition(ExecutionState::Running).unwrap();
    executions
        .begin_control(
            resume.request.request_id,
            &worker.id,
            running.attempt_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    executions
        .mark_control_side_effect_applied(resume.request.request_id, None)
        .await
        .unwrap();
    executions
        .complete_control(
            resume.request.request_id,
            &resumed,
            &progress_event(&resumed, ExecutionEventKind::ExecutionResumed),
        )
        .await
        .unwrap();
    assert_eq!(
        events
            .since(&running.id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionPaused))
            .count(),
        1
    );
    assert!(executions
        .list_pending_controls(&worker.id)
        .await
        .unwrap()
        .iter()
        .all(|control| control.execution.id != running.id));

    let cancelled_control = executions
        .request_control(&running.id, ExecutionControlAction::Pause, "cancel-wins")
        .await
        .unwrap();
    executions.request_cancellation(&running.id).await.unwrap();
    assert!(executions
        .request_control(
            &running.id,
            ExecutionControlAction::ForkConversation,
            "after-cancel"
        )
        .await
        .is_err());
    assert!(executions
        .list_pending_controls(&worker.id)
        .await
        .unwrap()
        .iter()
        .all(|control| control.request.request_id != cancelled_control.request.request_id));
}

#[tokio::test]
async fn interactive_control_is_fenced_phased_and_completion_matrix_is_exact() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-control-fence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let mut queued = execution(ExecutionState::Queued);
    queued.manifest.persistence = PersistenceMode::Resumable;
    executions.insert(&queued).await.unwrap();
    let mut running = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap()
        .execution;
    running.transition(ExecutionState::Provisioning).unwrap();
    running.worktree_path = Some("/bounded/fenced/repository".into());
    executions
        .record_progress(
            &running,
            &progress_event(&running, ExecutionEventKind::EnvironmentReady),
        )
        .await
        .unwrap();
    running.transition(ExecutionState::Running).unwrap();
    running.session_id = Some(SessionId::new("session-fenced"));
    executions
        .record_progress(
            &running,
            &progress_event(
                &running,
                ExecutionEventKind::AgentStarted {
                    session_id: SessionId::new("session-fenced"),
                },
            ),
        )
        .await
        .unwrap();

    let pause = executions
        .request_control(&running.id, ExecutionControlAction::Pause, "fenced-pause")
        .await
        .unwrap();
    let pending = executions
        .list_pending_controls(&worker.id)
        .await
        .unwrap()
        .into_iter()
        .find(|control| control.request.request_id == pause.request.request_id)
        .unwrap();
    assert_eq!(pending.phase, ExecutionControlPhase::Accepted);
    assert_eq!(pending.accepted_worker_id, worker.id);
    assert_eq!(
        pending.accepted_attempt_id,
        running.attempt_id.clone().unwrap()
    );
    assert_eq!(pending.source_session_id, SessionId::new("session-fenced"));
    assert_eq!(pending.worktree_path, "/bounded/fenced/repository");
    assert_eq!(pending.accepted_state, ExecutionState::Running);

    let mut paused = running.clone();
    paused.transition(ExecutionState::PausedForHuman).unwrap();
    let event = progress_event(&paused, ExecutionEventKind::ExecutionPaused);
    assert!(executions
        .complete_control(pause.request.request_id, &paused, &event)
        .await
        .is_err());
    executions
        .begin_control(
            pause.request.request_id,
            &worker.id,
            running.attempt_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    executions
        .mark_control_side_effect_applied(pause.request.request_id, None)
        .await
        .unwrap();
    let wrong_event = progress_event(&paused, ExecutionEventKind::ExecutionResumed);
    assert!(executions
        .complete_control(pause.request.request_id, &paused, &wrong_event)
        .await
        .is_err());
    assert!(executions
        .complete_control(pause.request.request_id, &paused, &event)
        .await
        .unwrap()
        .is_some());
    assert!(executions
        .complete_control(pause.request.request_id, &paused, &event)
        .await
        .unwrap()
        .is_none());

    let resume = executions
        .request_control(&paused.id, ExecutionControlAction::Resume, "stale-version")
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();
    executions
        .begin_control(
            resume.request.request_id,
            &worker.id,
            paused.attempt_id.as_ref().unwrap(),
        )
        .await
        .unwrap();
    executions
        .mark_control_side_effect_applied(resume.request.request_id, None)
        .await
        .unwrap();
    sqlx::query("UPDATE executions SET version = version + 1 WHERE id = $1")
        .bind(paused.id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        executions
            .begin_control(
                resume.request.request_id,
                &worker.id,
                paused.attempt_id.as_ref().unwrap(),
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    let stale: (String, Option<String>) = sqlx::query_as(
        "SELECT phase, stale_reason FROM execution_control_requests WHERE request_id = $1",
    )
    .bind(resume.request.request_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale.0, "STALE");
    assert!(stale.1.is_some());
    assert!(executions
        .list_pending_controls(&worker.id)
        .await
        .unwrap()
        .into_iter()
        .all(|control| control.request.request_id != resume.request.request_id));
}

#[tokio::test]
async fn attachment_snapshot_is_atomic_opaque_and_rejects_non_attachable_authority() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-attach-snapshot-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    assert!(executions.attachment_snapshot(&queued.id).await.is_err());
    let mut running = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap()
        .execution;
    running.transition(ExecutionState::Provisioning).unwrap();
    running.worktree_path = Some("/secret/host/path".into());
    executions
        .record_progress(
            &running,
            &progress_event(&running, ExecutionEventKind::EnvironmentReady),
        )
        .await
        .unwrap();
    running.transition(ExecutionState::Running).unwrap();
    running.session_id = Some(SessionId::new("attach-session"));
    executions
        .record_progress(
            &running,
            &progress_event(
                &running,
                ExecutionEventKind::AgentStarted {
                    session_id: SessionId::new("attach-session"),
                },
            ),
        )
        .await
        .unwrap();
    let snapshot = executions.attachment_snapshot(&running.id).await.unwrap();
    assert_eq!(snapshot.session_id, SessionId::new("attach-session"));
    assert_eq!(
        snapshot.workspace_ref,
        format!("execution:{}:workspace", running.id)
    );
    assert!(!serde_json::to_string(&snapshot)
        .unwrap()
        .contains("/secret/host/path"));
    assert!(snapshot.event_cursor > 0);

    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    cleanup
        .begin(
            &running.id,
            running.attempt_id.as_ref().unwrap(),
            &worker.id,
        )
        .await
        .unwrap();
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
    sqlx::query("UPDATE executions SET state = 'REVIEW_READY' WHERE id = $1")
        .bind(running.id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE cleanup_authorities SET phase = 'RETAINED' WHERE execution_id = $1")
        .bind(running.id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    let mut cleanup_transition = pool.begin().await.unwrap();
    sqlx::query("SELECT execution_id FROM cleanup_authorities WHERE execution_id = $1 FOR UPDATE")
        .bind(running.id.as_str())
        .fetch_one(&mut *cleanup_transition)
        .await
        .unwrap();
    let attach_store = executions.clone();
    let attach_id = running.id.clone();
    let attach = tokio::spawn(async move { attach_store.attachment_snapshot(&attach_id).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !attach.is_finished(),
        "attach returned without locking retained cleanup authority"
    );
    sqlx::query("UPDATE cleanup_authorities SET phase = 'CLEANUP_PENDING' WHERE execution_id = $1")
        .bind(running.id.as_str())
        .execute(&mut *cleanup_transition)
        .await
        .unwrap();
    cleanup_transition.commit().await.unwrap();
    assert!(attach.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_finalization_and_attachment_share_execution_first_lock_order() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let worker = registered_worker(
        &format!("worker-finalize-attach-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut running = reservation.execution.clone();
    running.transition(ExecutionState::Provisioning).unwrap();
    running.worktree_path = Some("/bounded/execution/repository".into());
    executions
        .record_progress(
            &running,
            &progress_event(&running, ExecutionEventKind::EnvironmentReady),
        )
        .await
        .unwrap();
    running.transition(ExecutionState::Running).unwrap();
    running.session_id = Some(SessionId::new("finalize-attach-session"));
    executions
        .record_progress(
            &running,
            &progress_event(
                &running,
                ExecutionEventKind::AgentStarted {
                    session_id: SessionId::new("finalize-attach-session"),
                },
            ),
        )
        .await
        .unwrap();
    cleanup
        .begin(&running.id, &reservation.attempt_id, &worker.id)
        .await
        .unwrap();
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
    sqlx::query(
        "UPDATE cleanup_authorities SET phase = 'STORAGE_RELEASED' WHERE execution_id = $1",
    )
    .bind(running.id.as_str())
    .execute(&pool)
    .await
    .unwrap();

    let mut cleanup_barrier = pool.begin().await.unwrap();
    sqlx::query("SELECT execution_id FROM cleanup_authorities WHERE execution_id = $1 FOR UPDATE")
        .bind(running.id.as_str())
        .fetch_one(&mut *cleanup_barrier)
        .await
        .unwrap();
    let finalize_store = reservations.clone();
    let finalize_id = running.id.clone();
    let finalize_attempt = reservation.attempt_id.clone();
    let finalize = tokio::spawn(async move {
        finalize_store
            .finalize_cleanup(&finalize_id, &finalize_attempt)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !finalize.is_finished(),
        "finalizer must wait at the cleanup-row barrier"
    );
    let blocked_finalizer = sqlx::query_scalar::<_, String>(
        "SELECT query FROM pg_stat_activity \
         WHERE wait_event_type = 'Lock' AND query LIKE '%cleanup_authorities%' \
         AND query LIKE '%FOR UPDATE%' AND datname = current_database() \
         AND pid <> pg_backend_pid() \
         ORDER BY query_start DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !blocked_finalizer.contains(" JOIN "),
        "cleanup row must be locked by its own query after the execution row: {blocked_finalizer}"
    );

    let attach_store = executions.clone();
    let attach_id = running.id.clone();
    let attach = tokio::spawn(async move { attach_store.attachment_snapshot(&attach_id).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !attach.is_finished(),
        "attachment must wait behind execution-first finalization"
    );
    cleanup_barrier.commit().await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), finalize)
        .await
        .expect("cleanup finalization and attachment deadlocked")
        .unwrap()
        .expect("cleanup finalization returned a PostgreSQL lock error");
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(10), attach)
            .await
            .expect("attachment remained blocked after finalization")
            .unwrap()
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_uses_execution_first_lock_order_at_every_control_phase() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();

    for phase in ["accepted", "applying", "side_effect_applied"] {
        let worker = registered_worker(
            &format!(
                "worker-lock-order-{phase}-{}",
                uuid::Uuid::new_v4().simple()
            ),
            1,
        );
        workers.register(&worker).await.unwrap();
        let mut queued = execution(ExecutionState::Queued);
        queued.manifest.persistence = PersistenceMode::Resumable;
        executions.insert(&queued).await.unwrap();
        let mut running = reservations
            .reserve_next(&worker.id)
            .await
            .unwrap()
            .unwrap()
            .execution;
        running.transition(ExecutionState::Provisioning).unwrap();
        running.worktree_path = Some(format!("/bounded/lock-order/{phase}"));
        executions
            .record_progress(
                &running,
                &progress_event(&running, ExecutionEventKind::EnvironmentReady),
            )
            .await
            .unwrap();
        running.transition(ExecutionState::Running).unwrap();
        running.session_id = Some(SessionId::new(format!("session-{phase}")));
        executions
            .record_progress(
                &running,
                &progress_event(
                    &running,
                    ExecutionEventKind::AgentStarted {
                        session_id: running.session_id.clone().unwrap(),
                    },
                ),
            )
            .await
            .unwrap();
        let control = executions
            .request_control(
                &running.id,
                ExecutionControlAction::Pause,
                &format!("cancel-race-{phase}"),
            )
            .await
            .unwrap();
        if phase != "accepted" {
            executions
                .begin_control(
                    control.request.request_id,
                    &worker.id,
                    running.attempt_id.as_ref().unwrap(),
                )
                .await
                .unwrap();
        }
        if phase == "side_effect_applied" {
            executions
                .mark_control_side_effect_applied(control.request.request_id, None)
                .await
                .unwrap();
        }

        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM executions WHERE id = $1 FOR UPDATE")
            .bind(running.id.as_str())
            .fetch_one(&mut *blocker)
            .await
            .unwrap();
        let cancel_store = executions.clone();
        let cancel_id = running.id.clone();
        let cancellation =
            tokio::spawn(async move { cancel_store.request_cancellation(&cancel_id).await });
        for _ in 0..100 {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock' \
                 AND query LIKE 'SELECT%executions%FOR UPDATE%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if blocked >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let control_store = executions.clone();
        let control_worker = worker.id.clone();
        let control_attempt = running.attempt_id.clone().unwrap();
        let request_id = control.request.request_id;
        let mut paused = running.clone();
        paused.transition(ExecutionState::PausedForHuman).unwrap();
        let control_operation = tokio::spawn(async move {
            match phase {
                "accepted" => {
                    control_store
                        .begin_control(request_id, &control_worker, &control_attempt)
                        .await
                }
                "applying" => {
                    control_store
                        .mark_control_side_effect_applied(request_id, None)
                        .await
                }
                "side_effect_applied" => control_store
                    .complete_control(
                        request_id,
                        &paused,
                        &progress_event(&paused, ExecutionEventKind::ExecutionPaused),
                    )
                    .await
                    .map(|_| ()),
                _ => unreachable!(),
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            !control_operation.is_finished(),
            "{phase} operation bypassed the execution-first lock"
        );
        blocker.commit().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), cancellation)
            .await
            .expect("cancellation deadlocked")
            .unwrap()
            .expect("cancellation must win the queued lock");
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), control_operation)
                .await
                .expect("control operation deadlocked")
                .unwrap()
                .is_err()
        );
        let persisted_phase: String = sqlx::query_scalar(
            "SELECT phase FROM execution_control_requests WHERE request_id = $1",
        )
        .bind(request_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(persisted_phase, "STALE");
    }
}
use orchestrator_persistence::{
    ArtifactStore, CleanupAuthorityStore, CleanupDisposition, CleanupStage, EventLog,
    ExecutionControlPhase, ExecutionStore, LostWorkerRecovery, PgArtifactStore,
    PgCleanupAuthorityStore, PgEventLog, PgExecutionStore, PgReservationStore, PgWorkerStore,
    ReservationStore, StoreError, WorkerStore,
};
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, Row};
use std::{
    borrow::Cow,
    sync::{Arc, OnceLock},
};

#[tokio::test]
async fn queued_cancellation_is_immediate_atomic_and_idempotent() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, events)) = stores().await else {
        return;
    };
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();

    let requested = executions.request_cancellation(&queued.id).await.unwrap();
    assert_eq!(requested.state, ExecutionState::Cancelled);
    assert!(!executions.cancellation_requested(&queued.id).await.unwrap());
    let cancelled = events.since(&queued.id, 0).await.unwrap();
    assert_eq!(cancelled.len(), 1);
    assert!(matches!(
        cancelled[0].kind,
        ExecutionEventKind::ExecutionCancelled
    ));
    assert!(cancelled[0].attempt_id.is_none());

    let replay = executions.request_cancellation(&queued.id).await.unwrap();
    assert_eq!(replay.id, queued.id);
    assert_eq!(replay.state, ExecutionState::Cancelled);
    assert_eq!(events.since(&queued.id, 0).await.unwrap().len(), 1);
}

#[tokio::test]
async fn scheduler_excludes_a_queued_execution_with_pending_cancellation() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let mut worker = registered_worker(
        &format!("worker-cancel-exclusion-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    let capability = format!("cancel-exclusion-{}", uuid::Uuid::new_v4().simple());
    worker.capabilities.capabilities.push(capability.clone());
    workers.register(&worker).await.unwrap();
    let mut cancelled = execution(ExecutionState::Queued);
    cancelled
        .manifest
        .runtime
        .capabilities
        .push(capability.clone());
    cancelled.created_at = Utc::now() - Duration::days(30_001);
    let mut available = execution(ExecutionState::Queued);
    available.manifest.runtime.capabilities.push(capability);
    available.created_at = Utc::now() - Duration::days(30_000);
    executions.insert(&cancelled).await.unwrap();
    executions.insert(&available).await.unwrap();
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution_cancellation_requests (execution_id) VALUES ($1)")
        .bind(cancelled.id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    let reserved = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reserved.execution.id, available.id);
}

#[tokio::test]
async fn accepted_cancellation_fences_final_progress_and_retention() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-cancel-fence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let mut queued = execution(ExecutionState::Queued);
    queued.manifest.persistence = PersistenceMode::Resumable;
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    executions
        .request_cancellation(&reservation.execution.id)
        .await
        .unwrap();
    let mut review_ready = reservation.execution;
    review_ready
        .transition(ExecutionState::Provisioning)
        .unwrap();
    review_ready.transition(ExecutionState::Running).unwrap();
    review_ready
        .transition(ExecutionState::ReviewReady)
        .unwrap();

    assert!(matches!(
        executions
            .record_progress_and_request_retention(
                &review_ready,
                &progress_event(&review_ready, ExecutionEventKind::ReviewReady),
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        reservations
            .commit_retained_and_release_capacity(
                &review_ready.id,
                review_ready.attempt_id.as_ref().unwrap(),
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(executions
        .cancellation_requested(&review_ready.id)
        .await
        .unwrap());
}

#[tokio::test]
async fn resolved_cleanup_cancellation_is_listable_and_restart_idempotent() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let events = PgEventLog::connect(&database_url).await.unwrap();
    let worker = registered_worker(
        &format!("worker-resolved-cancel-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    cleanup
        .begin(
            &reservation.execution.id,
            &reservation.attempt_id,
            &worker.id,
        )
        .await
        .unwrap();
    executions
        .request_cancellation(&reservation.execution.id)
        .await
        .unwrap();
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE execution_attempts SET finished_at = now(), updated_at = now() \
         WHERE execution_id = $1 AND attempt_id = $2",
    )
    .bind(reservation.execution.id.as_str())
    .bind(reservation.attempt_id.as_str())
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::query("DELETE FROM reservations WHERE execution_id = $1 AND attempt_id = $2")
        .bind(reservation.execution.id.as_str())
        .bind(reservation.attempt_id.as_str())
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE cleanup_authorities SET phase = 'RESOLVED', updated_at = now() \
         WHERE execution_id = $1 AND attempt_id = $2",
    )
    .bind(reservation.execution.id.as_str())
    .bind(reservation.attempt_id.as_str())
    .execute(&mut *transaction)
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    assert!(executions
        .list_pending_cancellations()
        .await
        .unwrap()
        .iter()
        .any(|pending| pending.execution.id == reservation.execution.id));
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT execution_id FROM execution_cancellation_requests \
         WHERE execution_id = $1 FOR UPDATE",
    )
    .bind(reservation.execution.id.as_str())
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    let completing = {
        let executions = executions.clone();
        let id = reservation.execution.id.clone();
        let attempt_id = reservation.attempt_id.clone();
        tokio::spawn(async move { executions.complete_cancellation(&id, &attempt_id).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let replaying = {
        let executions = executions.clone();
        let id = reservation.execution.id.clone();
        tokio::spawn(async move { executions.request_cancellation(&id).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    blocker.commit().await.unwrap();
    let (completed, replay) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(completing, replaying)
    })
    .await
    .expect("cancellation completion and replay must not deadlock");
    completed.unwrap().unwrap();
    let replay = replay.unwrap().unwrap();
    assert_eq!(replay.state, ExecutionState::Cancelled);
    assert!(!executions
        .cancellation_requested(&reservation.execution.id)
        .await
        .unwrap());
    assert_eq!(
        events
            .since(&reservation.execution.id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|event| matches!(event.kind, ExecutionEventKind::ExecutionCancelled))
            .count(),
        1
    );
}

#[tokio::test]
async fn current_migrator_fills_vacant_task6_versions_without_losing_pre_task6_rows() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        return;
    };
    let schema = format!("task6_upgrade_{}", uuid::Uuid::new_v4().simple());
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    let current = sqlx::migrate!();
    let pre_task6 = sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            current
                .migrations
                .iter()
                .filter(|migration| matches!(migration.version, 1 | 2 | 3 | 4 | 6 | 7 | 8 | 9))
                .cloned()
                .collect(),
        ),
        ignore_missing: false,
        locking: false,
        no_tx: false,
    };
    pre_task6.run(&mut connection).await.unwrap();
    sqlx::query(
        "INSERT INTO executions \
         (id, role, state, manifest, labels, created_at, updated_at) \
         VALUES ('preserved', 'implementation', 'QUEUED', '{}'::jsonb, '{}'::jsonb, now(), now())",
    )
    .execute(&mut connection)
    .await
    .unwrap();

    current.run(&mut connection).await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version IN (5, 10, 11)"
        )
        .fetch_one(&mut connection)
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT id FROM executions WHERE id = 'preserved'")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        "preserved"
    );
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('artifact_blobs') IS NOT NULL \
         AND to_regclass('execution_requests') IS NOT NULL"
    )
    .fetch_one(&mut connection)
    .await
    .unwrap());
    sqlx::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&mut connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn migration_0011_to_0012_preserves_execution_authority_and_installs_control_fences() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        return;
    };
    let schema = format!("task7_upgrade_{}", uuid::Uuid::new_v4().simple());
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}"
    ))
    .execute(&mut connection)
    .await
    .unwrap();
    let current = sqlx::migrate!();
    let through_0011 = sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            current
                .migrations
                .iter()
                .filter(|migration| migration.version <= 11)
                .cloned()
                .collect(),
        ),
        ignore_missing: false,
        locking: false,
        no_tx: false,
    };
    through_0011.run(&mut connection).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO workers (id, capabilities, capability_proof, state, last_heartbeat) \
         VALUES ('upgrade-worker', '{}'::jsonb, '{}'::jsonb, 'READY', now()); \
         INSERT INTO executions \
         (id, role, state, manifest, worker_id, attempt_id, session_id, worktree_path, labels, created_at, updated_at, version) \
         VALUES ('upgrade-execution', 'implementation', 'RUNNING', '{}'::jsonb, \
                 'upgrade-worker', 'upgrade-attempt', 'upgrade-session', '/bounded/repository', \
                 '{}'::jsonb, now(), now(), 7); \
         INSERT INTO execution_attempts \
         (attempt_id, execution_id, worker_id, state, worktree_path, session_id) \
         VALUES ('upgrade-attempt', 'upgrade-execution', 'upgrade-worker', 'RUNNING', \
                 '/bounded/repository', 'upgrade-session')",
    )
    .execute(&mut connection)
    .await
    .unwrap();

    current.run(&mut connection).await.unwrap();

    let preserved: (String, i64, String, String) = sqlx::query_as(
        "SELECT state, version, session_id, worktree_path FROM executions WHERE id = 'upgrade-execution'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        preserved,
        (
            "RUNNING".into(),
            7,
            "upgrade-session".into(),
            "/bounded/repository".into()
        )
    );
    sqlx::query(
        "INSERT INTO execution_control_requests \
         (execution_id, action, idempotency_key, accepted_worker_id, accepted_attempt_id, \
          source_session_id, worktree_path, accepted_execution_version, accepted_state) \
         VALUES ('upgrade-execution', 'pause', 'upgrade-control', 'upgrade-worker', \
                 'upgrade-attempt', 'upgrade-session', '/bounded/repository', 7, 'RUNNING')",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    assert!(sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('execution_control_requests_pending_order_idx') IS NOT NULL"
    )
    .fetch_one(&mut connection)
    .await
    .unwrap());
    assert!(sqlx::query(
        "INSERT INTO execution_control_requests \
         (execution_id, action, idempotency_key, accepted_worker_id, accepted_attempt_id, \
          source_session_id, worktree_path, accepted_execution_version, accepted_state) \
         VALUES ('upgrade-execution', 'fork-conversation', 'invalid-fork', 'upgrade-worker', \
                 'upgrade-attempt', 'upgrade-session', '/bounded/repository', 7, 'RUNNING')"
    )
    .execute(&mut connection)
    .await
    .is_err());
    sqlx::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&mut connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn event_batches_are_bounded_and_strictly_ordered() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, events)) = stores().await else {
        return;
    };
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    for _ in 0..140 {
        events
            .append(&ExecutionEvent {
                execution_id: queued.id.clone(),
                attempt_id: None,
                sequence: 0,
                at: Utc::now(),
                state: ExecutionState::Queued,
                kind: ExecutionEventKind::ExecutionCreated,
            })
            .await
            .unwrap();
    }
    let first = events.since_batch(&queued.id, 0, 32).await.unwrap();
    assert_eq!(first.len(), 32);
    assert_eq!(
        first.iter().map(|event| event.sequence).collect::<Vec<_>>(),
        (1..=32).collect::<Vec<_>>()
    );
    let second = events.since_batch(&queued.id, 32, 32).await.unwrap();
    assert_eq!(
        second
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        (33..=64).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn artifacts_are_content_addressed_deduplicated_and_execution_scoped() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real PostgreSQL test");
        return;
    };
    let state_root = tempfile::tempdir().unwrap();
    let executions = PgExecutionStore::connect(&database_url).await.unwrap();
    let artifacts = PgArtifactStore::connect(&database_url, state_root.path())
        .await
        .unwrap();
    let first = execution(ExecutionState::Queued);
    let second = execution(ExecutionState::Queued);
    executions.insert(&first).await.unwrap();
    executions.insert(&second).await.unwrap();

    let first_record = artifacts
        .store(
            &first.id,
            "test-results.json",
            "application/json",
            br#"{"passed":12}"#,
        )
        .await
        .unwrap();
    let replay = artifacts
        .store(
            &first.id,
            "test-results.json",
            "application/json",
            br#"{"passed":12}"#,
        )
        .await
        .unwrap();
    let second_record = artifacts
        .store(
            &second.id,
            "review-results.json",
            "application/json",
            br#"{"passed":12}"#,
        )
        .await
        .unwrap();

    assert_eq!(first_record.sha256, replay.sha256);
    assert_eq!(first_record.sha256, second_record.sha256);
    assert_eq!(first_record.size_bytes, 13);
    assert_eq!(artifacts.list(&first.id).await.unwrap(), vec![first_record]);
    assert_eq!(
        artifacts.list(&second.id).await.unwrap(),
        vec![second_record]
    );
    let blob_count = std::fs::read_dir(
        state_root
            .path()
            .join("artifacts")
            .join(&replay.sha256[..2]),
    )
    .unwrap()
    .filter(|entry| {
        entry
            .as_ref()
            .is_ok_and(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
    })
    .count();
    assert_eq!(blob_count, 1, "identical bytes share one durable blob");
    assert!(matches!(
        artifacts
            .store(&first.id, "../escape", "text/plain", b"bad")
            .await,
        Err(StoreError::InvalidArtifactName(_))
    ));
    assert!(matches!(
        artifacts
            .store(
                &first.id,
                "test-results.json",
                "application/json",
                b"different",
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    assert!(matches!(
        artifacts
            .store(
                &ExecutionId::new("unknown-execution"),
                "evidence.txt",
                "text/plain",
                b"foreign",
            )
            .await,
        Err(StoreError::NotFound(_))
    ));

    let blob_path = state_root
        .path()
        .join("artifacts")
        .join(&replay.sha256[..2])
        .join(&replay.sha256);
    std::fs::remove_file(&blob_path).unwrap();
    std::fs::write(&blob_path, b"corrupted").unwrap();
    assert!(matches!(
        artifacts
            .store(
                &first.id,
                "test-results.json",
                "application/json",
                br#"{"passed":12}"#,
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
}

fn database_test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn stores() -> Option<(PgExecutionStore, PgEventLog)> {
    let Ok(url) = std::env::var("AUTOSPEC_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL test: AUTOSPEC_DATABASE_URL is not set");
        return None;
    };
    match PgPoolOptions::new().max_connections(1).connect(&url).await {
        Ok(pool) => pool.close().await,
        Err(error) => {
            eprintln!("skipping PostgreSQL test: database is unavailable: {error}");
            return None;
        }
    }
    let executions = PgExecutionStore::connect(&url)
        .await
        .expect("database migrations and execution schema must succeed");
    let events = PgEventLog::connect(&url)
        .await
        .expect("event log connects after execution store migrated the database");
    Some((executions, events))
}

#[tokio::test]
async fn cleanup_authority_is_durable_until_explicit_resolution() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real PostgreSQL test");
        return;
    };
    let store = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let worker_id = WorkerId::new(format!("worker-cleanup-{}", uuid::Uuid::new_v4().simple()));
    let execution_id = ExecutionId::new(format!(
        "execution-cleanup-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let attempt_id =
        orchestrator_core::AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
    store
        .begin(&execution_id, &attempt_id, &worker_id)
        .await
        .unwrap();
    let handles = serde_json::json!({
        "receipt": {"mount_path": "/allocations/execution"},
        "worktree": "/allocations/execution/repository",
        "container_id": "sha256:container",
        "session_id": "session-1"
    });
    for (from, to) in [
        (CleanupStage::Reserved, CleanupStage::Storage),
        (CleanupStage::Storage, CleanupStage::Worktree),
        (CleanupStage::Worktree, CleanupStage::Runtime),
        (CleanupStage::Runtime, CleanupStage::PiStarted),
    ] {
        store
            .transition(
                &execution_id,
                CleanupDisposition::Active(from),
                CleanupDisposition::Active(to),
                &handles,
            )
            .await
            .unwrap();
    }
    drop(store);

    let reopened = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let pending = reopened.list_for_worker(&worker_id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].phase, "ACTIVE:PI_STARTED");
    assert_eq!(pending[0].handles, handles);
    assert!(reopened
        .transition(
            &execution_id,
            CleanupDisposition::Active(CleanupStage::PiStarted),
            CleanupDisposition::CleanupPending,
            &handles,
        )
        .await
        .is_err());
}

#[tokio::test]
async fn cleanup_disposition_transitions_are_exact_and_idempotent() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real PostgreSQL test");
        return;
    };
    let store = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let worker_id = WorkerId::new(format!("worker-state-{}", uuid::Uuid::new_v4().simple()));
    let execution_id =
        ExecutionId::new(format!("execution-state-{}", uuid::Uuid::new_v4().simple()));
    let attempt_id =
        orchestrator_core::AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
    store
        .begin(&execution_id, &attempt_id, &worker_id)
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&execution_id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::Active(CleanupStage::Reserved)
    );

    store
        .transition(
            &execution_id,
            CleanupDisposition::Active(CleanupStage::Reserved),
            CleanupDisposition::Active(CleanupStage::Storage),
            &serde_json::json!({"receipt": "durable"}),
        )
        .await
        .unwrap();
    store
        .transition(
            &execution_id,
            CleanupDisposition::Active(CleanupStage::Reserved),
            CleanupDisposition::Active(CleanupStage::Storage),
            &serde_json::json!({"receipt": "durable"}),
        )
        .await
        .expect("replayed exact transition is idempotent");
    assert!(matches!(
        store
            .transition(
                &execution_id,
                CleanupDisposition::Active(CleanupStage::Reserved),
                CleanupDisposition::RuntimeDestroyed,
                &serde_json::json!({}),
            )
            .await,
        Err(StoreError::Conflict(_))
    ));
    let authority = store.get(&execution_id).await.unwrap();
    assert_eq!(
        authority.disposition().unwrap(),
        CleanupDisposition::Active(CleanupStage::Storage)
    );
    assert_eq!(authority.handles, serde_json::json!({"receipt": "durable"}));
    assert!(store
        .transition(
            &execution_id,
            CleanupDisposition::Active(CleanupStage::Storage),
            CleanupDisposition::CleanupPending,
            &authority.handles,
        )
        .await
        .is_err());
}

fn execution(state: ExecutionState) -> Execution {
    let id = ExecutionId::new(format!("test-{}", uuid::Uuid::new_v4().simple()));
    let now = Utc::now();
    Execution {
        id: id.clone(),
        role: Role::Implementation,
        state,
        manifest: ExecutionManifest {
            api_version: orchestrator_core::MANIFEST_API_VERSION.to_owned(),
            role: Role::Implementation,
            task: None,
            repository: RepositoryReference {
                repo: "InferWeave/autospec-orchestrator".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: None,
                branch: None,
            },
            agent: AgentAssignment {
                harness: HarnessKind::Pi,
                model_policy: ModelPolicy {
                    provider: "inferweave".to_owned(),
                    preferred: vec!["test-model".to_owned()],
                    alternatives: Vec::new(),
                    fallback_class: None,
                },
            },
            runtime: RuntimeRequirement::default(),
            services: Vec::new(),
            persistence: PersistenceMode::Ephemeral,
            task_packet: None,
        },
        worker_id: None,
        attempt_id: None,
        session_id: None,
        worktree_path: None,
        labels: OwnershipLabels {
            execution_id: id,
            worker_id: orchestrator_core::WorkerId::new("test-worker"),
            repository: "InferWeave/autospec-orchestrator".to_owned(),
            issue: None,
        },
        created_at: now,
        updated_at: now,
        result: None,
    }
}

fn event(id: &ExecutionId) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: id.clone(),
        attempt_id: None,
        sequence: 0,
        at: Utc::now(),
        state: ExecutionState::Running,
        kind: ExecutionEventKind::EnvironmentReady,
    }
}

#[tokio::test]
async fn execution_round_trip_transition_and_live_filter_are_durable() {
    let _database_test = database_test_lock().lock().await;
    let Some((store, _)) = stores().await else {
        return;
    };
    let queued = execution(ExecutionState::Queued);
    store.insert(&queued).await.expect("insert succeeds");

    let fetched = store.get(&queued.id).await.expect("execution is readable");
    assert_eq!(
        serde_json::to_value(fetched).unwrap(),
        serde_json::to_value(&queued).unwrap()
    );

    let transitioned = store
        .transition(&queued.id, ExecutionState::WorkerAssigned)
        .await
        .expect("legal transition succeeds");
    assert_eq!(transitioned.state, ExecutionState::WorkerAssigned);
    assert_eq!(
        store.get(&queued.id).await.unwrap().state,
        ExecutionState::WorkerAssigned
    );

    let terminal = execution(ExecutionState::Completed);
    store
        .insert(&terminal)
        .await
        .expect("terminal insert succeeds");
    let live = store.list_live().await.expect("live executions list");
    assert!(live.iter().any(|item| item.id == queued.id));
    assert!(!live.iter().any(|item| item.id == terminal.id));
}

#[tokio::test]
async fn illegal_and_concurrent_transitions_are_serialized() {
    let _database_test = database_test_lock().lock().await;
    let Some((store, _)) = stores().await else {
        return;
    };
    let queued = execution(ExecutionState::Queued);
    store.insert(&queued).await.unwrap();
    assert!(matches!(
        store.transition(&queued.id, ExecutionState::Running).await,
        Err(StoreError::IllegalTransition {
            from: ExecutionState::Queued,
            to: ExecutionState::Running
        })
    ));

    let store = Arc::new(store);
    let first = {
        let store = Arc::clone(&store);
        let id = queued.id.clone();
        tokio::spawn(async move { store.transition(&id, ExecutionState::WorkerAssigned).await })
    };
    let second = {
        let store = Arc::clone(&store);
        let id = queued.id.clone();
        tokio::spawn(async move { store.transition(&id, ExecutionState::WorkerAssigned).await })
    };
    let results = [first.await.unwrap(), second.await.unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
}

#[tokio::test]
async fn concurrent_events_are_gapless_and_replay_in_order() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, log)) = stores().await else {
        return;
    };
    let id = ExecutionId::new(format!("events-{}", uuid::Uuid::new_v4().simple()));
    let log = Arc::new(log);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let log = Arc::clone(&log);
        let event = event(&id);
        tasks.push(tokio::spawn(
            async move { log.append(&event).await.unwrap() },
        ));
    }
    let mut sequences = Vec::new();
    for task in tasks {
        sequences.push(task.await.unwrap());
    }
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=8).collect::<Vec<_>>());

    let replay = log.since(&id, 3).await.expect("events replay");
    assert_eq!(
        replay.iter().map(|item| item.sequence).collect::<Vec<_>>(),
        vec![4, 5, 6, 7, 8]
    );
}

#[tokio::test]
async fn event_replay_returns_the_complete_tail_without_a_pagination_contract() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, log)) = stores().await else {
        return;
    };
    let id = ExecutionId::new(format!("batch-{}", uuid::Uuid::new_v4().simple()));
    for _ in 0..501 {
        log.append(&event(&id)).await.unwrap();
    }

    assert_eq!(log.since(&id, 0).await.unwrap().len(), 501);
}

fn registered_worker(id: &str, slots: u32) -> orchestrator_core::WorkerRegistration {
    serde_json::from_value(serde_json::json!({
        "id": id,
        "capabilities": {
            "os": "linux",
            "arch": "x86_64",
            "cpu": 16,
            "memoryMib": 32768,
            "diskGib": 500,
            "runtimes": ["docker"],
            "capabilities": ["docker"],
            "maxConcurrentExecutions": slots
        },
        "state": "READY",
        "running_executions": 0,
        "last_heartbeat": Utc::now(),
        "capability_proof": {
            "storage_backend": "test",
            "storage_pool_identity": "pool-a",
            "docker_daemon_id": "daemon-a",
            "docker_verifier": "bind-probe",
            "docker_method_version": "v1"
        }
    }))
    .unwrap()
}

async fn worker_stores() -> Option<(PgExecutionStore, PgWorkerStore, PgReservationStore)> {
    let Ok(url) = std::env::var("AUTOSPEC_DATABASE_URL") else {
        eprintln!("skipping PostgreSQL test: AUTOSPEC_DATABASE_URL is not set");
        return None;
    };
    let executions = PgExecutionStore::connect(&url).await.unwrap();
    let workers = PgWorkerStore::connect(&url).await.unwrap();
    let reservations = PgReservationStore::connect(&url).await.unwrap();
    Some((executions, workers, reservations))
}

#[tokio::test]
async fn worker_registration_requires_storage_and_docker_capability_proof() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, workers, _)) = worker_stores().await else {
        return;
    };
    let valid = registered_worker(&format!("worker-{}", uuid::Uuid::new_v4().simple()), 4);
    workers.register(&valid).await.unwrap();
    workers.register(&valid).await.unwrap();
    assert_eq!(workers.get(&valid.id).await.unwrap().id, valid.id);

    let mut missing_proof = valid.clone();
    missing_proof.id = orchestrator_core::WorkerId::new(format!(
        "worker-missing-proof-{}",
        uuid::Uuid::new_v4().simple()
    ));
    missing_proof.capability_proof = None;
    assert!(matches!(
        workers.register(&missing_proof).await,
        Err(StoreError::Conflict(_))
    ));
}

#[tokio::test]
async fn concurrent_reservation_assigns_each_execution_once_without_oversubscription() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-slots-{}", uuid::Uuid::new_v4().simple()),
        4,
    );
    workers.register(&worker).await.unwrap();
    for _ in 0..16 {
        executions
            .insert(&execution(ExecutionState::Queued))
            .await
            .unwrap();
    }
    let reservations = Arc::new(reservations);
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let reservations = Arc::clone(&reservations);
        let worker_id = worker.id.clone();
        tasks.push(tokio::spawn(async move {
            reservations.reserve_next(&worker_id).await
        }));
    }
    let mut assigned = Vec::new();
    for task in tasks {
        if let Some(reservation) = task.await.unwrap().unwrap() {
            assigned.push(reservation);
        }
    }
    assert_eq!(assigned.len(), 4);
    let mut ids = assigned
        .iter()
        .map(|reservation| reservation.execution.id.to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);
    assert!(assigned.iter().all(|reservation| {
        reservation.execution.state == ExecutionState::WorkerAssigned
            && reservation.execution.worker_id.as_ref() == Some(&worker.id)
            && reservation.execution.attempt_id.as_ref() == Some(&reservation.attempt_id)
            && reservation.execution.labels.worker_id == worker.id
    }));
    assert_eq!(
        reservations
            .list_for_worker(&worker.id)
            .await
            .unwrap()
            .len(),
        4
    );

    for reservation in &assigned {
        reservations
            .release(&reservation.execution.id)
            .await
            .unwrap();
        reservations
            .release(&reservation.execution.id)
            .await
            .unwrap();
    }
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn stale_attempt_cleanup_cannot_release_a_reassigned_reservation() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-fenced-release-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    executions
        .insert(&execution(ExecutionState::Queued))
        .await
        .unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let stale_attempt = reservation.attempt_id;
    let replacement_attempt =
        orchestrator_core::AttemptId::new(format!("attempt-{}", uuid::Uuid::new_v4().simple()));
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE reservations SET attempt_id = $2 WHERE execution_id = $1")
        .bind(reservation.execution.id.as_str())
        .bind(replacement_attempt.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(!reservations
        .release_attempt(&reservation.execution.id, &stale_attempt)
        .await
        .unwrap());
    let remaining = reservations.list_for_worker(&worker.id).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].attempt_id, replacement_attempt);
    assert!(reservations
        .release_attempt(&reservation.execution.id, &replacement_attempt)
        .await
        .unwrap());
}

#[tokio::test]
async fn startup_fence_blocks_reassignment_until_physical_cleanup_finalizes() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let mut worker = registered_worker(
        &format!("worker-startup-fence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    let capability = format!("startup-fence-{}", uuid::Uuid::new_v4().simple());
    worker.capabilities.capabilities.push(capability.clone());
    workers.register(&worker).await.unwrap();
    let mut queued = execution(ExecutionState::Queued);
    queued.manifest.persistence = PersistenceMode::Resumable;
    queued.manifest.runtime.capabilities.push(capability);
    queued.created_at = Utc::now() - Duration::days(3_650);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let events = PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();

    assert!(matches!(
        reservations
            .fence_lost_attempt(&reservation.execution.id, &reservation.attempt_id)
            .await
            .unwrap(),
        LostWorkerRecovery::CleanupPending(id) if id == reservation.execution.id
    ));
    assert!(matches!(
        reservations
            .fence_lost_attempt(&reservation.execution.id, &reservation.attempt_id)
            .await
            .unwrap(),
        LostWorkerRecovery::CleanupPending(id) if id == reservation.execution.id
    ));
    assert!(
        events
            .since(&reservation.execution.id, 0)
            .await
            .unwrap()
            .is_empty(),
        "fencing must not publish a terminal event while the execution row remains assigned"
    );
    let classified = executions.get(&reservation.execution.id).await.unwrap();
    assert_eq!(classified.state, ExecutionState::WorkerAssigned);
    assert_eq!(classified.worker_id.as_ref(), Some(&worker.id));
    assert_eq!(
        classified.attempt_id.as_ref(),
        Some(&reservation.attempt_id)
    );
    assert_eq!(
        reservations
            .list_for_worker(&worker.id)
            .await
            .unwrap()
            .len(),
        1,
        "old capacity remains fenced until exact physical cleanup completes"
    );
    let cleanup =
        PgCleanupAuthorityStore::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
            .await
            .unwrap();
    let handles = serde_json::json!({});
    let mut disposition = CleanupDisposition::CleanupPending;
    for next in [
        CleanupDisposition::RuntimeStopped,
        CleanupDisposition::RuntimeDestroyed,
        CleanupDisposition::GitRecoveredCleaned,
        CleanupDisposition::StorageReleased,
    ] {
        cleanup
            .transition(&reservation.execution.id, disposition, next, &handles)
            .await
            .unwrap();
        disposition = next;
    }
    assert!(matches!(
        reservations
            .finalize_cleanup(&reservation.execution.id, &reservation.attempt_id)
            .await
            .unwrap(),
        LostWorkerRecovery::Requeued(id) if id == reservation.execution.id
    ));
    assert_eq!(
        executions
            .get(&reservation.execution.id)
            .await
            .unwrap()
            .state,
        ExecutionState::Queued
    );
    assert_eq!(
        cleanup
            .get(&reservation.execution.id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::ReservationReleased
    );
    let final_events = events.since(&reservation.execution.id, 0).await.unwrap();
    assert_eq!(final_events.len(), 1);
    assert_eq!(final_events[0].state, ExecutionState::Queued);
    assert!(matches!(
        final_events[0].kind,
        ExecutionEventKind::ExecutionRequeued {
            failure: orchestrator_core::FailureClass::WorkerLost
        }
    ));
    assert_eq!(cleanup.list_for_worker(&worker.id).await.unwrap().len(), 1);
    cleanup.resolve(&reservation.execution.id).await.unwrap();
    assert!(cleanup
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unresolved_cleanup_authority_excludes_a_queued_execution_from_scheduling() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let mut worker = registered_worker(
        &format!("worker-cleanup-exclusion-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    let capability = format!("cleanup-exclusion-{}", uuid::Uuid::new_v4().simple());
    worker.capabilities.capabilities.push(capability.clone());
    workers.register(&worker).await.unwrap();
    let mut blocked = execution(ExecutionState::Queued);
    blocked
        .manifest
        .runtime
        .capabilities
        .push(capability.clone());
    blocked.created_at = Utc::now() - Duration::days(20_000);
    let mut available = execution(ExecutionState::Queued);
    available.manifest.runtime.capabilities.push(capability);
    available.created_at = Utc::now() - Duration::days(19_999);
    executions.insert(&blocked).await.unwrap();
    executions.insert(&available).await.unwrap();
    cleanup
        .begin(
            &blocked.id,
            &orchestrator_core::AttemptId::new("abandoned-attempt"),
            &worker.id,
        )
        .await
        .unwrap();

    let reserved = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reserved.execution.id, available.id);
}

#[tokio::test]
async fn progress_commit_persists_execution_attempt_and_event_atomically() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-progress-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut running = reservation.execution.clone();
    running.transition(ExecutionState::Provisioning).unwrap();
    running.worktree_path = Some("/verified/worktree".to_owned());
    let progress = ExecutionEvent {
        execution_id: running.id.clone(),
        attempt_id: running.attempt_id.clone(),
        sequence: 0,
        at: Utc::now(),
        state: running.state,
        kind: ExecutionEventKind::EnvironmentReady,
    };
    let sequence = executions
        .record_progress(&running, &progress)
        .await
        .unwrap();
    assert_eq!(sequence, 1);
    let persisted = executions.get(&running.id).await.unwrap();
    assert_eq!(
        persisted.worktree_path.as_deref(),
        Some("/verified/worktree")
    );
    assert_eq!(persisted.attempt_id, running.attempt_id);
    let events = PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap()
        .since(&running.id, 0)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
}

#[tokio::test]
async fn review_ready_progress_and_retention_request_commit_atomically() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let worker = registered_worker(
        &format!("worker-retain-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let mut queued = execution(ExecutionState::Queued);
    queued.manifest.persistence = PersistenceMode::Resumable;
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    cleanup
        .begin(
            &reservation.execution.id,
            &reservation.attempt_id,
            &worker.id,
        )
        .await
        .unwrap();
    let handles = serde_json::json!({"session": "session-1"});
    let transitions = [
        (CleanupStage::Reserved, CleanupStage::Storage),
        (CleanupStage::Storage, CleanupStage::Worktree),
        (CleanupStage::Worktree, CleanupStage::Runtime),
        (CleanupStage::Runtime, CleanupStage::PiStarted),
        (CleanupStage::PiStarted, CleanupStage::Running),
        (CleanupStage::Running, CleanupStage::PostPiBeforeEvent),
    ];
    for (from, to) in transitions {
        cleanup
            .transition(
                &reservation.execution.id,
                CleanupDisposition::Active(from),
                CleanupDisposition::Active(to),
                &handles,
            )
            .await
            .unwrap();
    }
    let mut running = reservation.execution;
    running.transition(ExecutionState::Provisioning).unwrap();
    executions
        .record_progress(
            &running,
            &progress_event(&running, ExecutionEventKind::EnvironmentReady),
        )
        .await
        .unwrap();
    running.transition(ExecutionState::Running).unwrap();
    executions
        .record_progress(
            &running,
            &progress_event(
                &running,
                ExecutionEventKind::AgentStarted {
                    session_id: orchestrator_core::SessionId::new("session-1"),
                },
            ),
        )
        .await
        .unwrap();
    running.transition(ExecutionState::ReviewReady).unwrap();
    executions
        .record_progress_and_request_retention(
            &running,
            &progress_event(&running, ExecutionEventKind::ReviewReady),
        )
        .await
        .unwrap();

    assert_eq!(
        cleanup
            .get(&running.id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::RetainRequested
    );
    assert_eq!(
        executions.get(&running.id).await.unwrap().state,
        ExecutionState::ReviewReady
    );
}

#[tokio::test]
async fn retained_disposition_and_capacity_release_are_one_transaction() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let worker = registered_worker(
        &format!("worker-retain-commit-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let mut queued = execution(ExecutionState::Queued);
    queued.manifest.persistence = PersistenceMode::Resumable;
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    cleanup
        .begin(
            &reservation.execution.id,
            &reservation.attempt_id,
            &worker.id,
        )
        .await
        .unwrap();
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
    sqlx::query(
        "UPDATE executions SET state = 'REVIEW_READY', manifest = jsonb_set(manifest, '{persistence}', '\"resumable\"') WHERE id = $1",
    )
    .bind(reservation.execution.id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE cleanup_authorities SET phase = 'RETAIN_REQUESTED' WHERE execution_id = $1",
    )
    .bind(reservation.execution.id.as_str())
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE OR REPLACE FUNCTION autospec_test_fail_retained() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'injected retained commit failure'; END $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER autospec_test_fail_retained BEFORE UPDATE ON cleanup_authorities \
         FOR EACH ROW WHEN (NEW.phase = 'RETAINED') EXECUTE FUNCTION autospec_test_fail_retained()",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(reservations
        .commit_retained_and_release_capacity(&reservation.execution.id, &reservation.attempt_id,)
        .await
        .is_err());
    assert_eq!(
        cleanup
            .get(&reservation.execution.id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::RetainRequested
    );
    assert_eq!(
        reservations
            .list_for_worker(&worker.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(workers.get(&worker.id).await.unwrap().running_executions, 1);
    sqlx::query("DROP TRIGGER autospec_test_fail_retained ON cleanup_authorities")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION autospec_test_fail_retained()")
        .execute(&pool)
        .await
        .unwrap();

    reservations
        .commit_retained_and_release_capacity(&reservation.execution.id, &reservation.attempt_id)
        .await
        .unwrap();

    assert_eq!(
        cleanup
            .get(&reservation.execution.id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::Retained
    );
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(workers.get(&worker.id).await.unwrap().running_executions, 0);
    reservations
        .commit_retained_and_release_capacity(&reservation.execution.id, &reservation.attempt_id)
        .await
        .expect("startup replay is idempotent");
}

#[tokio::test]
async fn ephemeral_review_ready_cleanup_preserves_result_and_finalizes_atomically() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let database_url = std::env::var("AUTOSPEC_DATABASE_URL").unwrap();
    let cleanup = PgCleanupAuthorityStore::connect(&database_url)
        .await
        .unwrap();
    let events = PgEventLog::connect(&database_url).await.unwrap();
    let worker = registered_worker(
        &format!("worker-ephemeral-review-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    cleanup
        .begin(
            &reservation.execution.id,
            &reservation.attempt_id,
            &worker.id,
        )
        .await
        .unwrap();
    let result = ExecutionResult {
        execution_id: reservation.execution.id.clone(),
        state: ExecutionState::ReviewReady,
        failure: None,
        branch: Some("autospec/review-ready".to_owned()),
        base_sha: Some("abc123".to_owned()),
        diff_artifact: Some("evidence/diff.patch".to_owned()),
        artifacts: Vec::new(),
        tests: None,
    };
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
    sqlx::query("UPDATE executions SET state = 'REVIEW_READY', result = $2 WHERE id = $1")
        .bind(reservation.execution.id.as_str())
        .bind(serde_json::to_value(&result).unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE cleanup_authorities SET phase = 'STORAGE_RELEASED' WHERE execution_id = $1",
    )
    .bind(reservation.execution.id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    events
        .append(&ExecutionEvent {
            execution_id: reservation.execution.id.clone(),
            attempt_id: Some(reservation.attempt_id.clone()),
            sequence: 0,
            at: Utc::now(),
            state: ExecutionState::ReviewReady,
            kind: ExecutionEventKind::ReviewReady,
        })
        .await
        .unwrap();

    sqlx::query("DROP TRIGGER IF EXISTS autospec_test_fail_finalize ON cleanup_authorities")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION IF EXISTS autospec_test_fail_finalize()")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE OR REPLACE FUNCTION autospec_test_fail_finalize() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION 'injected cleanup finalize failure'; END $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER autospec_test_fail_finalize BEFORE UPDATE ON cleanup_authorities \
         FOR EACH ROW WHEN (NEW.phase = 'RESERVATION_RELEASED') EXECUTE FUNCTION autospec_test_fail_finalize()",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(reservations
        .finalize_cleanup(&reservation.execution.id, &reservation.attempt_id)
        .await
        .is_err());
    assert_eq!(
        reservations
            .list_for_worker(&worker.id)
            .await
            .unwrap()
            .len(),
        1,
        "failed transaction must retain capacity"
    );
    assert_eq!(
        cleanup
            .get(&reservation.execution.id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::StorageReleased
    );
    assert_eq!(
        events
            .since(&reservation.execution.id, 0)
            .await
            .unwrap()
            .len(),
        1
    );
    sqlx::query("DROP TRIGGER autospec_test_fail_finalize ON cleanup_authorities")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION autospec_test_fail_finalize()")
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        reservations
            .finalize_cleanup(&reservation.execution.id, &reservation.attempt_id)
            .await
            .unwrap(),
        LostWorkerRecovery::ReviewReady(id) if id == reservation.execution.id
    ));
    let persisted = executions.get(&reservation.execution.id).await.unwrap();
    assert_eq!(persisted.state, ExecutionState::ReviewReady);
    assert_eq!(
        persisted.result.unwrap().diff_artifact,
        result.diff_artifact
    );
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        cleanup
            .get(&reservation.execution.id)
            .await
            .unwrap()
            .disposition()
            .unwrap(),
        CleanupDisposition::ReservationReleased
    );
    let final_events = events.since(&reservation.execution.id, 0).await.unwrap();
    assert_eq!(final_events.len(), 1);
    assert_eq!(final_events[0].state, ExecutionState::ReviewReady);
    assert!(matches!(
        final_events[0].kind,
        ExecutionEventKind::ReviewReady
    ));
    sqlx::query(
        "UPDATE cleanup_authorities SET worker_id = 'forged-worker' WHERE execution_id = $1",
    )
    .bind(reservation.execution.id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        reservations
            .finalize_cleanup(&reservation.execution.id, &reservation.attempt_id)
            .await
            .is_err(),
        "reservation-released replay must authenticate the exact attempt worker"
    );
    sqlx::query("UPDATE cleanup_authorities SET worker_id = $2 WHERE execution_id = $1")
        .bind(reservation.execution.id.as_str())
        .bind(worker.id.as_str())
        .execute(&pool)
        .await
        .unwrap();
    reservations
        .finalize_cleanup(&reservation.execution.id, &reservation.attempt_id)
        .await
        .unwrap();
    assert_eq!(
        events
            .since(&reservation.execution.id, 0)
            .await
            .unwrap()
            .len(),
        1,
        "idempotent finalization must not duplicate the terminal event"
    );
    cleanup.resolve(&reservation.execution.id).await.unwrap();
}

#[tokio::test]
async fn cleanup_disposition_migration_never_retains_legacy_ephemeral_or_ambiguous_rows() {
    let _database_test = database_test_lock().lock().await;
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        return;
    };
    let pool = PgPoolOptions::new().connect(&database_url).await.unwrap();
    let schema = format!("migration_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema}; SET search_path TO {schema}; \
         CREATE TABLE executions (id TEXT PRIMARY KEY, manifest JSONB NOT NULL); \
         CREATE TABLE cleanup_authorities (execution_id TEXT PRIMARY KEY, phase TEXT NOT NULL); \
         INSERT INTO executions VALUES \
           ('resumable', '{{\"persistence\":\"resumable\"}}'), \
           ('ephemeral', '{{\"persistence\":\"ephemeral\"}}'), \
           ('ambiguous', '{{}}'); \
         INSERT INTO cleanup_authorities VALUES \
           ('resumable', 'REVIEW_READY'), ('ephemeral', 'REVIEW_READY'), ('ambiguous', 'REVIEW_READY');"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!("SET search_path TO {schema}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0009_cleanup_dispositions.sql"))
        .execute(&pool)
        .await
        .unwrap();
    let rows =
        sqlx::query("SELECT execution_id, phase FROM cleanup_authorities ORDER BY execution_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    let phases = rows
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("execution_id"),
                row.get::<String, _>("phase"),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(phases["resumable"], "RETAIN_REQUESTED");
    assert_eq!(phases["ephemeral"], "CLEANUP_PENDING");
    assert_eq!(phases["ambiguous"], "CLEANUP_PENDING");
    sqlx::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn append_and_progress_share_one_gapless_sequence_allocator() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-sequence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut running = reservation.execution;
    running.transition(ExecutionState::Provisioning).unwrap();
    let log = Arc::new(
        PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
            .await
            .unwrap(),
    );
    let executions = Arc::new(executions);
    let barrier = Arc::new(tokio::sync::Barrier::new(64));
    let mut tasks = Vec::new();
    for index in 0..64 {
        let barrier = barrier.clone();
        if index % 2 == 0 {
            let executions = executions.clone();
            let running = running.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                executions
                    .record_progress(
                        &running,
                        &progress_event(&running, ExecutionEventKind::EnvironmentReady),
                    )
                    .await
            }));
        } else {
            let log = log.clone();
            let event = progress_event(&running, ExecutionEventKind::EnvironmentReady);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                log.append(&event).await
            }));
        }
    }
    let mut sequences = Vec::new();
    for task in tasks {
        sequences.push(task.await.unwrap().unwrap());
    }
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=64).collect::<Vec<_>>());
    assert_eq!(log.since(&running.id, 0).await.unwrap().len(), 64);
}

#[tokio::test]
async fn progress_rejects_stale_cancelled_reassigned_and_forged_attempts() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-fence-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let mut provisioning = reservation.execution.clone();
    provisioning
        .transition(ExecutionState::Provisioning)
        .unwrap();
    let progress = progress_event(&provisioning, ExecutionEventKind::EnvironmentReady);
    executions
        .record_progress(&provisioning, &progress)
        .await
        .unwrap();

    let stale = provisioning.clone();
    let mut cancelled = provisioning.clone();
    cancelled.transition(ExecutionState::Cancelled).unwrap();
    let cancelled_event = progress_event(&cancelled, ExecutionEventKind::ExecutionCancelled);
    executions
        .record_progress(&cancelled, &cancelled_event)
        .await
        .unwrap();

    let mut stale_running = stale;
    stale_running.transition(ExecutionState::Running).unwrap();
    let stale_event = progress_event(
        &stale_running,
        ExecutionEventKind::AgentStarted {
            session_id: orchestrator_core::SessionId::new("stale"),
        },
    );
    assert!(matches!(
        executions
            .record_progress(&stale_running, &stale_event)
            .await,
        Err(StoreError::Conflict(_)) | Err(StoreError::IllegalTransition { .. })
    ));

    let mut forged = cancelled.clone();
    forged.worker_id = Some(orchestrator_core::WorkerId::new("foreign-worker"));
    let forged_event = progress_event(&forged, ExecutionEventKind::ExecutionCancelled);
    assert!(matches!(
        executions.record_progress(&forged, &forged_event).await,
        Err(StoreError::Conflict(_))
    ));
    assert_eq!(
        executions.get(&cancelled.id).await.unwrap().state,
        ExecutionState::Cancelled
    );
    let events = PgEventLog::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap()
        .since(&cancelled.id, 0)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
}

fn progress_event(execution: &Execution, kind: ExecutionEventKind) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: execution.id.clone(),
        attempt_id: execution.attempt_id.clone(),
        sequence: 0,
        at: Utc::now(),
        state: execution.state,
        kind,
    }
}

#[tokio::test]
async fn stale_worker_becomes_unreachable_and_fresh_proven_heartbeat_restores_ready() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, workers, _)) = worker_stores().await else {
        return;
    };
    let mut worker = registered_worker(
        &format!("worker-heartbeat-{}", uuid::Uuid::new_v4().simple()),
        2,
    );
    worker.last_heartbeat = Utc::now() - Duration::seconds(120);
    workers.register(&worker).await.unwrap();
    let stale = workers
        .mark_stale_before(Utc::now() - Duration::seconds(90))
        .await
        .unwrap();
    assert!(stale.contains(&worker.id));
    assert_eq!(
        workers.get(&worker.id).await.unwrap().state,
        orchestrator_core::WorkerState::Unreachable
    );

    worker.state = orchestrator_core::WorkerState::Ready;
    worker.last_heartbeat = Utc::now();
    assert_eq!(
        workers.heartbeat(&worker).await.unwrap().state,
        orchestrator_core::WorkerState::Ready
    );
}

#[tokio::test]
async fn worker_page_is_strictly_bounded_and_resumes_after_cursor() {
    let _database_test = database_test_lock().lock().await;
    let Some((_, workers, _)) = worker_stores().await else {
        return;
    };
    let prefix = format!("worker-page-{}", uuid::Uuid::new_v4().simple());
    for suffix in ["-a", "-b", "-c"] {
        workers
            .register(&registered_worker(&format!("{prefix}{suffix}"), 2))
            .await
            .unwrap();
    }
    let first = workers
        .list_page(2, Some(&format!("{prefix}-0")))
        .await
        .unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].id.as_str(), format!("{prefix}-a"));
    assert_eq!(first[1].id.as_str(), format!("{prefix}-b"));
    let second = workers
        .list_page(2, Some(first[1].id.as_str()))
        .await
        .unwrap();
    assert_eq!(second[0].id.as_str(), format!("{prefix}-c"));
}

#[tokio::test]
async fn orphan_reservation_reconcile_is_idempotent() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-orphan-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let assigned = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    assert!(reservations
        .reconcile(&[])
        .await
        .unwrap()
        .contains(&assigned.execution.id));
    assert!(!reservations
        .reconcile(&[])
        .await
        .unwrap()
        .contains(&assigned.execution.id));
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unreachable_worker_fences_capacity_until_each_cleanup_finalizes() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let mut worker =
        registered_worker(&format!("worker-lost-{}", uuid::Uuid::new_v4().simple()), 2);
    let isolation_capability = format!("lost-worker-{}", uuid::Uuid::new_v4().simple());
    worker
        .capabilities
        .capabilities
        .push(isolation_capability.clone());
    worker.capabilities.cpu = 2;
    worker.capabilities.memory_mib = 2048;
    worker.capabilities.disk_gib = 2;
    workers.register(&worker).await.unwrap();
    let mut resumable = execution(ExecutionState::Queued);
    resumable.manifest.persistence = PersistenceMode::Resumable;
    resumable
        .manifest
        .runtime
        .capabilities
        .push(isolation_capability.clone());
    resumable.manifest.runtime.cpu = 1;
    resumable.manifest.runtime.memory_mib = 1024;
    resumable.manifest.runtime.disk_gib = 1;
    let mut ephemeral = execution(ExecutionState::Queued);
    ephemeral
        .manifest
        .runtime
        .capabilities
        .push(isolation_capability);
    ephemeral.manifest.runtime.cpu = 1;
    ephemeral.manifest.runtime.memory_mib = 1024;
    ephemeral.manifest.runtime.disk_gib = 1;
    executions.insert(&resumable).await.unwrap();
    executions.insert(&ephemeral).await.unwrap();
    let resumable = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let ephemeral = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    worker.last_heartbeat = Utc::now() - Duration::seconds(120);
    workers.heartbeat(&worker).await.unwrap();
    workers
        .mark_stale_before(Utc::now() - Duration::seconds(90))
        .await
        .unwrap();

    let recovered = reservations.recover_unreachable(&worker.id).await.unwrap();

    assert!(recovered.iter().any(|item| matches!(
        item,
        LostWorkerRecovery::CleanupPending(id) if id == &resumable.execution.id
    )));
    assert!(recovered.iter().any(|item| matches!(
        item,
        LostWorkerRecovery::CleanupPending(id) if id == &ephemeral.execution.id
    )));
    assert_eq!(
        reservations
            .list_for_worker(&worker.id)
            .await
            .unwrap()
            .len(),
        2
    );
    let cleanup =
        PgCleanupAuthorityStore::connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
            .await
            .unwrap();
    for reserved in [&resumable, &ephemeral] {
        let mut disposition = CleanupDisposition::CleanupPending;
        for next in [
            CleanupDisposition::RuntimeStopped,
            CleanupDisposition::RuntimeDestroyed,
            CleanupDisposition::GitRecoveredCleaned,
            CleanupDisposition::StorageReleased,
        ] {
            cleanup
                .transition(
                    &reserved.execution.id,
                    disposition,
                    next,
                    &serde_json::json!({}),
                )
                .await
                .unwrap();
            disposition = next;
        }
        reservations
            .finalize_cleanup(&reserved.execution.id, &reserved.attempt_id)
            .await
            .unwrap();
    }
    assert_eq!(
        executions.get(&resumable.execution.id).await.unwrap().state,
        ExecutionState::Queued
    );
    let failed = executions.get(&ephemeral.execution.id).await.unwrap();
    assert_eq!(failed.state, ExecutionState::Failed);
    assert_eq!(
        failed.result.unwrap().failure,
        Some(orchestrator_core::FailureClass::WorkerLost)
    );
    assert!(reservations
        .list_for_worker(&worker.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn unreachable_reaper_skips_already_fenced_attempt_with_retained_cleanup_reservation() {
    let _database_test = database_test_lock().lock().await;
    let Some((executions, workers, reservations)) = worker_stores().await else {
        return;
    };
    let worker = registered_worker(
        &format!("worker-fenced-reap-{}", uuid::Uuid::new_v4().simple()),
        1,
    );
    workers.register(&worker).await.unwrap();
    let queued = execution(ExecutionState::Queued);
    executions.insert(&queued).await.unwrap();
    let reservation = reservations
        .reserve_next(&worker.id)
        .await
        .unwrap()
        .unwrap();
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("AUTOSPEC_DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE execution_attempts SET state = 'FAILED', finished_at = now() WHERE attempt_id = $1",
    )
    .bind(reservation.attempt_id.as_str())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE workers SET state = 'UNREACHABLE' WHERE id = $1")
        .bind(worker.id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(reservations
        .recover_unreachable(&worker.id)
        .await
        .unwrap()
        .is_empty());
    let retained = reservations.list_for_worker(&worker.id).await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].attempt_id, reservation.attempt_id);

    assert!(reservations
        .release_attempt(&reservation.execution.id, &reservation.attempt_id)
        .await
        .unwrap());
}
