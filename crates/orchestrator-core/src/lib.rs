//! Neutral domain types for the AutoSpec execution plane.
//!
//! This crate deliberately contains no AutoSpec business logic and no InferWeave
//! inference logic. It defines only the vocabulary shared across the execution
//! plane: executions, task references, runtime requirements, agent assignments,
//! ownership labels, and events.
//!
//! See `docs/specs/three-plane-execution-architecture.md` sections 77 and 101.

pub mod environment;
pub mod error;
pub mod event;
pub mod execution;
pub mod ids;
pub mod labels;
pub mod manifest;
pub mod task_packet;
pub mod telemetry;
pub mod worker;

pub use environment::EnvironmentFile;
pub use error::{CoreError, FailureClass};
pub use event::ExecutionEvent;
pub use execution::{Execution, ExecutionResult, ExecutionState, Role};
pub use ids::{AttemptId, ExecutionId, SessionId, WorkerId};
pub use labels::OwnershipLabels;
pub use manifest::{
    AgentAssignment, ExecutionManifest, HarnessKind, ModelPolicy, PersistenceMode,
    RepositoryReference, RuntimeKind, RuntimeRequirement, ServiceRequirement, TaskReference,
};
pub use task_packet::TaskPacket;
pub use telemetry::{execution_span, init_tracing};
pub use worker::{WorkerCapabilities, WorkerCapabilityProof, WorkerRegistration, WorkerState};

/// API version prefix for the orchestrator HTTP surface (spec section 75).
pub const API_VERSION: &str = "v1";

/// Manifest API group/version (spec section 75).
pub const MANIFEST_API_VERSION: &str = "autospec.dev/v1alpha1";
