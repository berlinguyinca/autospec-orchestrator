//! Control-plane resilience contracts consumed by the execution plane.
//!
//! The autospec control plane (berlinguyinca/autospec#4672) owns the Resilient
//! Agent Runtime contracts: versioned JSON Schemas (`autospec/schemas/*.json`)
//! and the stable event names in
//! `autospec/docs/contracts/autospec-resilience-events-v1.md`. This module is the
//! execution plane's *consumer* of those contracts: it reuses the exact
//! versioned wire shapes and the exact event names, so the execution lifecycle
//! is observable through the same durable contract and can be mirrored into
//! `autospec-db.resilience_events` (berlinguyinca/autospec-db#3).
//!
//! It defines **no competing vocabulary**: the event names below are byte-for-byte
//! the control-plane contract names, and the wire types carry the same `schema`
//! constants and field names as the published JSON Schemas.

use serde::{Deserialize, Serialize};

use crate::event::ExecutionEventKind;

/// `schema` constant carried by every work receipt (autospec.work-receipt.v1).
pub const WORK_RECEIPT_SCHEMA: &str = "autospec.work-receipt.v1";
/// `schema` constant carried by every context checkpoint
/// (autospec.context-checkpoint.v1).
pub const CONTEXT_CHECKPOINT_SCHEMA: &str = "autospec.context-checkpoint.v1";

/// The stable resilient-runtime lifecycle event names, reusing the control-plane
/// contract exactly (`autospec-resilience-events-v1.md`). Additive-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResilienceLifecycleEvent {
    /// Context Guardian
    #[serde(rename = "context.threshold_reached")]
    ContextThresholdReached,
    #[serde(rename = "checkpoint.requested")]
    CheckpointRequested,
    #[serde(rename = "checkpoint.persisted")]
    CheckpointPersisted,
    #[serde(rename = "checkpoint.acknowledged")]
    CheckpointAcknowledged,
    #[serde(rename = "execution.resumed")]
    ExecutionResumed,
    /// Durable work protocol
    #[serde(rename = "work.assigned")]
    WorkAssigned,
    #[serde(rename = "work.delivered")]
    WorkDelivered,
    #[serde(rename = "claim.acquired")]
    ClaimAcquired,
    #[serde(rename = "claim.renewed")]
    ClaimRenewed,
    #[serde(rename = "claim.expired")]
    ClaimExpired,
    #[serde(rename = "attempt.started")]
    AttemptStarted,
    #[serde(rename = "attempt.completed")]
    AttemptCompleted,
    #[serde(rename = "validation.completed")]
    ValidationCompleted,
    #[serde(rename = "review.completed")]
    ReviewCompleted,
    /// Attention streams
    #[serde(rename = "attention.started")]
    AttentionStarted,
    #[serde(rename = "attention.progressed")]
    AttentionProgressed,
    #[serde(rename = "attention.completed")]
    AttentionCompleted,
    /// Memory map
    #[serde(rename = "memory.map_generated")]
    MemoryMapGenerated,
    #[serde(rename = "memory.retrieved")]
    MemoryRetrieved,
    /// Verified learning
    #[serde(rename = "lesson.candidate_created")]
    LessonCandidateCreated,
    #[serde(rename = "lesson.validated")]
    LessonValidated,
    #[serde(rename = "lesson.promoted")]
    LessonPromoted,
    #[serde(rename = "lesson.rejected")]
    LessonRejected,
}

impl ResilienceLifecycleEvent {
    /// The exact wire name (`kind`) per the control-plane events contract.
    pub fn kind(&self) -> &'static str {
        match self {
            ResilienceLifecycleEvent::ContextThresholdReached => "context.threshold_reached",
            ResilienceLifecycleEvent::CheckpointRequested => "checkpoint.requested",
            ResilienceLifecycleEvent::CheckpointPersisted => "checkpoint.persisted",
            ResilienceLifecycleEvent::CheckpointAcknowledged => "checkpoint.acknowledged",
            ResilienceLifecycleEvent::ExecutionResumed => "execution.resumed",
            ResilienceLifecycleEvent::WorkAssigned => "work.assigned",
            ResilienceLifecycleEvent::WorkDelivered => "work.delivered",
            ResilienceLifecycleEvent::ClaimAcquired => "claim.acquired",
            ResilienceLifecycleEvent::ClaimRenewed => "claim.renewed",
            ResilienceLifecycleEvent::ClaimExpired => "claim.expired",
            ResilienceLifecycleEvent::AttemptStarted => "attempt.started",
            ResilienceLifecycleEvent::AttemptCompleted => "attempt.completed",
            ResilienceLifecycleEvent::ValidationCompleted => "validation.completed",
            ResilienceLifecycleEvent::ReviewCompleted => "review.completed",
            ResilienceLifecycleEvent::AttentionStarted => "attention.started",
            ResilienceLifecycleEvent::AttentionProgressed => "attention.progressed",
            ResilienceLifecycleEvent::AttentionCompleted => "attention.completed",
            ResilienceLifecycleEvent::MemoryMapGenerated => "memory.map_generated",
            ResilienceLifecycleEvent::MemoryRetrieved => "memory.retrieved",
            ResilienceLifecycleEvent::LessonCandidateCreated => "lesson.candidate_created",
            ResilienceLifecycleEvent::LessonValidated => "lesson.validated",
            ResilienceLifecycleEvent::LessonPromoted => "lesson.promoted",
            ResilienceLifecycleEvent::LessonRejected => "lesson.rejected",
        }
    }
}

/// Stage of a work receipt (schema enum `sent|delivered|claimed|handled`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkReceiptStage {
    Sent,
    Delivered,
    Claimed,
    Handled,
}

/// A durable acknowledgement receipt (`autospec.work-receipt.v1`). Field names
/// and `schema` constant match `autospec/schemas/autospec-work-receipt.schema.json`
/// exactly, so a receipt serialized here validates against the control-plane schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkReceiptV1 {
    pub schema: String,
    pub receipt_id: String,
    pub idempotency_key: String,
    pub work_id: String,
    pub attempt_id: String,
    pub consumer: String,
    pub stage: WorkReceiptStage,
    pub at: u64,
}

impl WorkReceiptV1 {
    /// Build a receipt for an attempt consumed by a named consumer.
    /// `at` is a monotonic non-negative timestamp (ms since epoch is typical).
    pub fn new(
        work_id: impl Into<String>,
        attempt_id: impl Into<String>,
        consumer: impl Into<String>,
        stage: WorkReceiptStage,
        at: u64,
    ) -> Self {
        let attempt_id = attempt_id.into();
        Self {
            schema: WORK_RECEIPT_SCHEMA.to_owned(),
            receipt_id: format!("receipt-{at}-{attempt_id}"),
            idempotency_key: format!("idem-{attempt_id}-{at}"),
            work_id: work_id.into(),
            attempt_id,
            consumer: consumer.into(),
            stage,
            at,
        }
    }
}

/// A structured continuation checkpoint (`autospec.context-checkpoint.v1`).
/// Required fields and `schema` constant match the control-plane schema; the
/// optional arrays are the common continuation vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextCheckpointV1 {
    pub schema: String,
    pub checkpoint_id: String,
    pub created_at: String,
    pub execution_id: String,
    pub attempt_id: String,
    pub session_id: String,
    pub work_id: String,
    pub objective: String,
    pub acceptance_criteria: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pull_request: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_progress: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_actions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blockers: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unresolved_questions: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextWindowV1>,
}

/// Context-window accounting within a checkpoint (schema `context` object).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextWindowV1 {
    pub window_tokens: u64,
    pub estimated_used_tokens: u64,
    pub source: ContextSource,
}

/// How the used-token estimate was obtained (schema `context.source` enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ContextSource {
    Exact,
    Estimated,
    ConservativeEstimate,
    Unknown,
}

/// How one execution-plane event maps onto the control-plane resilience
/// lifecycle. `None` where no resilience milestone corresponds (additive-only:
/// never reinterprets an event).
pub fn resilience_lifecycle(kind: &ExecutionEventKind) -> Option<ResilienceLifecycleEvent> {
    use ExecutionEventKind::*;
    use ResilienceLifecycleEvent as R;
    match kind {
        ExecutionCreated => Some(R::WorkAssigned),
        WorkerAssigned { .. } => Some(R::WorkDelivered),
        EnvironmentReady => Some(R::ClaimAcquired),
        AgentStarted { .. } => Some(R::AttemptStarted),
        ExecutionCompleted => Some(R::AttemptCompleted),
        ExecutionRequeued { .. } => Some(R::AttemptCompleted),
        ExecutionFailed { .. } => Some(R::AttemptCompleted),
        ExecutionCancelled => Some(R::AttemptCompleted),
        ReviewReady => Some(R::ReviewCompleted),
        AgentInactive { .. } => Some(R::ClaimExpired),
        ExecutionResumed => Some(R::ExecutionResumed),
        // No resilience milestone: these do not reinterpret the lifecycle.
        ExecutionPaused => None,
        TestsStarted => None,
        TestsFailed => None,
        ConversationForked { .. } => None,
        WatchdogSample { .. } => None,
    }
}

/// The work-receipt stage for an execution-plane event, when it advances a
/// receipt (schema enum `sent|delivered|claimed|handled`).
pub fn work_receipt_stage(kind: &ExecutionEventKind) -> Option<WorkReceiptStage> {
    use ExecutionEventKind::*;
    use WorkReceiptStage::*;
    match kind {
        ExecutionCreated => Some(Sent),
        WorkerAssigned { .. } => Some(Delivered),
        EnvironmentReady => Some(Claimed),
        AgentStarted { .. } => Some(Claimed),
        ExecutionCompleted
        | ExecutionRequeued { .. }
        | ExecutionFailed { .. }
        | ExecutionCancelled => Some(Handled),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::event::ExecutionEventKind;
    use crate::WorkerId;

    fn ev(kind: ExecutionEventKind) -> ExecutionEventKind {
        kind
    }

    #[test]
    fn lifecycle_mapping_covers_the_work_protocol_milestones() {
        // Every milestone in the work protocol has a stable lifecycle mapping.
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::ExecutionCreated)),
            Some(ResilienceLifecycleEvent::WorkAssigned)
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::WorkerAssigned {
                worker_id: WorkerId::new("w1"),
            })),
            Some(ResilienceLifecycleEvent::WorkDelivered)
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::EnvironmentReady)),
            Some(ResilienceLifecycleEvent::ClaimAcquired)
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::AgentStarted {
                session_id: crate::SessionId::new("s1"),
            })),
            Some(ResilienceLifecycleEvent::AttemptStarted)
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::ExecutionCompleted)),
            Some(ResilienceLifecycleEvent::AttemptCompleted)
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::ReviewReady)),
            Some(ResilienceLifecycleEvent::ReviewCompleted)
        );
    }

    #[test]
    fn lifecycle_mapping_reports_expired_claim_and_resume() {
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::AgentInactive { seconds: 300 })),
            Some(ResilienceLifecycleEvent::ClaimExpired)
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::ExecutionResumed)),
            Some(ResilienceLifecycleEvent::ExecutionResumed)
        );
    }

    #[test]
    fn non_milestone_events_do_not_reinterpret_the_lifecycle() {
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::ExecutionPaused)),
            None
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::TestsStarted)),
            None
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::TestsFailed)),
            None
        );
        assert_eq!(
            resilience_lifecycle(&ev(ExecutionEventKind::ConversationForked {
                session_id: crate::SessionId::new("s1"),
            })),
            None
        );
    }

    #[test]
    fn lifecycle_event_kinds_match_the_control_plane_contract() {
        // The wire names must be byte-for-byte the contract names.
        assert_eq!(
            ResilienceLifecycleEvent::WorkAssigned.kind(),
            "work.assigned"
        );
        assert_eq!(
            ResilienceLifecycleEvent::AttemptCompleted.kind(),
            "attempt.completed"
        );
        assert_eq!(
            ResilienceLifecycleEvent::ContextThresholdReached.kind(),
            "context.threshold_reached"
        );
        assert_eq!(
            ResilienceLifecycleEvent::LessonRejected.kind(),
            "lesson.rejected"
        );
        assert_eq!(
            ResilienceLifecycleEvent::CheckpointPersisted.kind(),
            "checkpoint.persisted"
        );
    }

    #[test]
    fn lifecycle_event_serializes_to_the_contract_kind() {
        let json = serde_json::to_string(&ResilienceLifecycleEvent::LessonPromoted).unwrap();
        assert_eq!(json, "\"lesson.promoted\"");
    }

    #[test]
    fn work_receipt_serializes_with_schema_and_stage_field_names() {
        let receipt = WorkReceiptV1::new(
            "work-1",
            "attempt-1",
            "orchestrator",
            WorkReceiptStage::Claimed,
            1_700_000_000_000,
        );
        let json = serde_json::to_value(&receipt).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["schema"], "autospec.work-receipt.v1");
        assert_eq!(obj["stage"], "claimed");
        assert_eq!(obj["work_id"], "work-1");
        assert_eq!(obj["attempt_id"], "attempt-1");
        assert_eq!(obj["consumer"], "orchestrator");
        assert!(obj["receipt_id"].as_str().unwrap().starts_with("receipt-"));
        assert!(obj["idempotency_key"]
            .as_str()
            .unwrap()
            .starts_with("idem-"));
    }

    #[test]
    fn work_receipt_stage_advances_through_the_lifecycle() {
        assert_eq!(
            work_receipt_stage(&ExecutionEventKind::ExecutionCreated),
            Some(WorkReceiptStage::Sent)
        );
        assert_eq!(
            work_receipt_stage(&ExecutionEventKind::WorkerAssigned {
                worker_id: WorkerId::new("w1"),
            }),
            Some(WorkReceiptStage::Delivered)
        );
        assert_eq!(
            work_receipt_stage(&ExecutionEventKind::EnvironmentReady),
            Some(WorkReceiptStage::Claimed)
        );
        assert_eq!(
            work_receipt_stage(&ExecutionEventKind::ExecutionCompleted),
            Some(WorkReceiptStage::Handled)
        );
        assert_eq!(
            work_receipt_stage(&ExecutionEventKind::ExecutionPaused),
            None
        );
    }

    #[test]
    fn context_checkpoint_serializes_with_required_and_optional_fields() {
        let checkpoint = ContextCheckpointV1 {
            schema: CONTEXT_CHECKPOINT_SCHEMA.to_owned(),
            checkpoint_id: "checkpoint-1".to_owned(),
            created_at: "2026-09-15T00:00:00Z".to_owned(),
            execution_id: "exec-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            session_id: "session-1".to_owned(),
            work_id: "work-1".to_owned(),
            objective: "implement the resilience contracts".to_owned(),
            acceptance_criteria: vec!["tests pass".to_owned()],
            repository: Some("berlinguyinca/autospec".to_owned()),
            worktree: None,
            branch: Some("feat/x".to_owned()),
            issue: None,
            pull_request: None,
            completed: Some(vec!["draft".to_owned()]),
            in_progress: Some(vec!["tests".to_owned()]),
            next_actions: None,
            changed_files: None,
            blockers: None,
            unresolved_questions: None,
            context: Some(ContextWindowV1 {
                window_tokens: 120_000,
                estimated_used_tokens: 64_000,
                source: ContextSource::Estimated,
            }),
        };
        let json = serde_json::to_value(&checkpoint).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["schema"], "autospec.context-checkpoint.v1");
        assert_eq!(obj["objective"], "implement the resilience contracts");
        assert_eq!(obj["context"]["source"], "estimated");
        // Optional fields not set are omitted (schema `additionalProperties: false`).
        assert!(!obj.contains_key("worktree"));
        assert!(!obj.contains_key("next_actions"));
    }

    #[test]
    fn failure_kinds_map_to_attempt_completed() {
        use crate::error::FailureClass;
        for kind in [
            ExecutionEventKind::ExecutionFailed {
                failure: FailureClass::Internal,
            },
            ExecutionEventKind::ExecutionRequeued {
                failure: FailureClass::Internal,
            },
        ] {
            assert_eq!(
                resilience_lifecycle(&kind),
                Some(ResilienceLifecycleEvent::AttemptCompleted)
            );
        }
    }
}
