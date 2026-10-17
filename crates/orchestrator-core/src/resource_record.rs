//! Projection of owned execution resources into shared-protocol
//! `ResourceRecord` objects (T08 protocol subset).
//!
//! The orchestrator is the source of truth for which resources an execution
//! created (worktree, container, build dir, cache). This module projects the
//! cleanup authority's handles into the protocol's `ResourceRecord` shape so
//! external consumers can observe owned resources through one shared
//! vocabulary.
//!
//! Field names copy the T08 `ResourceRecord` schema
//! (`autospec.protocol.resource-record.v1`). Fields the cleanup authority does
//! not hold (mission id, sequence, lease id, idempotency key) are omitted from
//! the serialized form, never invented.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ExecutionId;

/// Protocol identifier for the resource-record schema (T08 golden fixture).
pub const RESOURCE_RECORD_SCHEMA: &str = "autospec.protocol.resource-record.v1";

/// Protocol version of the resource-record schema (T08 golden fixture).
pub const RESOURCE_RECORD_PROTOCOL_VERSION: u32 = 1;

/// The kind of an owned resource, matching the T08 `ResourceRecord` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Worktree,
    Process,
    BuildDir,
    Cache,
    Container,
    TempFile,
}

/// Lifecycle state of a resource, matching the T08 `ResourceRecord` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceState {
    Active,
    Releasable,
    Released,
    RetainedForDiagnostics,
}

/// Owning principal of a resource. `mission_id` is present only when the
/// cleanup authority holds it; `project` populates `execution_id` only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceOwner {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    pub execution_id: String,
}

/// A projected owned resource, matching the T08 `ResourceRecord` field names.
///
/// Fields the orchestrator does not hold are `Option` and omitted from the
/// serialized form (`never invented`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub schema: String,
    pub protocol_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required_extensions: Option<Vec<String>>,
    pub resource_id: String,
    pub kind: ResourceKind,
    pub owner: ResourceOwner,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub state: ResourceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inodes: Option<u64>,
}

/// Project the cleanup authority's handles into `ResourceRecord` objects.
///
/// Only handle shapes the orchestrator recognizes are projected; unknown
/// shapes are skipped, never invented:
/// - `worktree`  -> kind `worktree` (carries the worktree `path`)
/// - `runtime`   -> kind `container`
/// - `build_dir` -> kind `build_dir`
/// - `cache`     -> kind `cache`
pub fn project(
    execution: &ExecutionId,
    created_at: &DateTime<Utc>,
    phase: &str,
    handles: &Value,
) -> Vec<ResourceRecord> {
    let state = state_from_phase(phase);
    let owner = ResourceOwner {
        mission_id: None,
        execution_id: execution.as_str().to_owned(),
    };
    let mut records = Vec::new();

    if let Some(worktree) = handles.get("worktree").filter(|value| !value.is_null()) {
        records.push(record(
            &owner,
            execution,
            created_at,
            state,
            ResourceKind::Worktree,
            worktree
                .get("path")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ));
    }
    if handles
        .get("runtime")
        .filter(|value| !value.is_null())
        .is_some()
    {
        records.push(record(
            &owner,
            execution,
            created_at,
            state,
            ResourceKind::Container,
            None,
        ));
    }
    if handles
        .get("build_dir")
        .filter(|value| !value.is_null())
        .is_some()
    {
        records.push(record(
            &owner,
            execution,
            created_at,
            state,
            ResourceKind::BuildDir,
            None,
        ));
    }
    if handles
        .get("cache")
        .filter(|value| !value.is_null())
        .is_some()
    {
        records.push(record(
            &owner,
            execution,
            created_at,
            state,
            ResourceKind::Cache,
            None,
        ));
    }

    records
}

fn record(
    owner: &ResourceOwner,
    execution: &ExecutionId,
    created_at: &DateTime<Utc>,
    state: ResourceState,
    kind: ResourceKind,
    path: Option<String>,
) -> ResourceRecord {
    ResourceRecord {
        schema: RESOURCE_RECORD_SCHEMA.to_owned(),
        protocol_version: RESOURCE_RECORD_PROTOCOL_VERSION,
        idempotency_key: None,
        mission_id: None,
        sequence: None,
        required_extensions: None,
        resource_id: format!(
            "{}:{}",
            execution.as_str(),
            format!("{:?}", kind).to_lowercase()
        ),
        kind,
        owner: owner.clone(),
        created_at: created_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        lease_id: None,
        state,
        path,
        bytes: None,
        inodes: None,
    }
}

/// Map the cleanup authority's phase onto the T08 resource-state vocabulary.
fn state_from_phase(phase: &str) -> ResourceState {
    if phase.starts_with("ACTIVE:") {
        return ResourceState::Active;
    }
    match phase {
        "RESOLVED" => ResourceState::Released,
        "RETAIN_REQUESTED" | "RETAINED" => ResourceState::RetainedForDiagnostics,
        "CLEANUP_PENDING"
        | "RUNTIME_STOPPED"
        | "RUNTIME_DESTROYED"
        | "GIT_RECOVERED_CLEANED"
        | "STORAGE_RELEASED"
        | "RESERVATION_RELEASED" => ResourceState::Releasable,
        // Unrecognized phases map to active: the resource is still owned and
        // not known to be releasable or released.
        _ => ResourceState::Active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn execution() -> ExecutionId {
        ExecutionId::new("e-1")
    }

    fn at() -> DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-10-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn projects_worktree_and_runtime_handles() {
        let handles = json!({
            "worktree": { "execution_id": "e-1", "path": "/tmp/wt-e-1", "branch": "main" },
            "runtime": { "execution_id": "e-1", "container_id": "abc123" },
        });
        let records = project(&execution(), &at(), "ACTIVE:RUNNING", &handles);
        let kinds: Vec<ResourceKind> = records.iter().map(|record| record.kind).collect();
        assert_eq!(kinds, vec![ResourceKind::Worktree, ResourceKind::Container]);
        for record in &records {
            // schema tag + version match the T08 ResourceRecord golden fixture
            assert_eq!(record.schema, "autospec.protocol.resource-record.v1");
            assert_eq!(record.protocol_version, 1);
            assert_eq!(record.owner.execution_id, "e-1");
            assert_eq!(record.state, ResourceState::Active);
        }
        // the worktree record carries its path
        assert_eq!(records[0].path, Some("/tmp/wt-e-1".to_owned()));
        // fields the cleanup authority does not hold are never invented
        assert!(records.iter().all(|record| {
            record.mission_id.is_none()
                && record.lease_id.is_none()
                && record.idempotency_key.is_none()
                && record.sequence.is_none()
                && record.owner.mission_id.is_none()
        }));
    }

    #[test]
    fn skips_unknown_handle_shapes() {
        let handles = json!({
            "receipt": { "id": "r-1" },
            "session": { "id": "s-1" },
            "mystery": { "foo": "bar" },
        });
        let records = project(&execution(), &at(), "RESOLVED", &handles);
        assert!(
            records.is_empty(),
            "unrecognized handles must be skipped, not invented"
        );
    }

    #[test]
    fn null_handles_are_skipped() {
        let handles = json!({ "worktree": null, "runtime": null });
        let records = project(&execution(), &at(), "ACTIVE:RUNNING", &handles);
        assert!(records.is_empty());
    }

    #[test]
    fn maps_resolved_phase_to_released() {
        let handles = json!({ "worktree": { "path": "/tmp/wt" } });
        let records = project(&execution(), &at(), "RESOLVED", &handles);
        assert_eq!(records[0].state, ResourceState::Released);
    }

    #[test]
    fn maps_retain_phase_to_retained_for_diagnostics() {
        let handles = json!({ "runtime": { "container_id": "abc" } });
        let records = project(&execution(), &at(), "RETAINED", &handles);
        assert_eq!(records[0].state, ResourceState::RetainedForDiagnostics);
    }

    #[test]
    fn serialized_record_carries_schema_tag_and_omits_unknown_fields() {
        let handles = json!({ "worktree": { "path": "/tmp/wt" } });
        let record = project(&execution(), &at(), "ACTIVE:RUNNING", &handles)[0].clone();
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["schema"], "autospec.protocol.resource-record.v1");
        assert_eq!(value["protocol_version"], 1);
        assert_eq!(value["kind"], "worktree");
        assert_eq!(value["owner"]["execution_id"], "e-1");
        assert_eq!(value["created_at"], "2026-10-15T12:00:00Z");
        // omitted, not invented
        assert!(value.get("mission_id").is_none());
        assert!(value.get("lease_id").is_none());
        assert!(value.get("idempotency_key").is_none());
        assert!(value.get("sequence").is_none());
        assert!(value["owner"].get("mission_id").is_none());
    }
}
