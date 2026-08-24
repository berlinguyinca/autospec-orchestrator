//! Mandatory ownership labels applied to every runtime resource the orchestrator
//! creates (spec sections 42, 83).
//!
//! Reconciliation and GC must select on these labels only. Broad operations such
//! as `docker container prune` are forbidden.

use crate::ids::{ExecutionId, WorkerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MANAGED: &str = "autospec.managed";
pub const EXECUTION_ID: &str = "autospec.execution_id";
pub const WORKER_ID: &str = "autospec.worker_id";
pub const REPOSITORY: &str = "autospec.repository";
pub const ISSUE: &str = "autospec.issue";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipLabels {
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub repository: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issue: Option<String>,
}

impl OwnershipLabels {
    /// Render as the label map attached to containers, networks, and volumes.
    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        map.insert(MANAGED.to_owned(), "true".to_owned());
        map.insert(EXECUTION_ID.to_owned(), self.execution_id.to_string());
        map.insert(WORKER_ID.to_owned(), self.worker_id.to_string());
        map.insert(REPOSITORY.to_owned(), self.repository.clone());
        if let Some(issue) = &self.issue {
            map.insert(ISSUE.to_owned(), issue.clone());
        }
        map
    }

    /// Docker-style filter selecting exactly the resources this execution owns.
    pub fn selector(&self) -> Vec<String> {
        vec![
            format!("{MANAGED}=true"),
            format!("{EXECUTION_ID}={}", self.execution_id),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> OwnershipLabels {
        OwnershipLabels {
            execution_id: ExecutionId::new("node-417-impl-01"),
            worker_id: WorkerId::new("buildbox-02"),
            repository: "InferWeave/inferweave-node".to_owned(),
            issue: Some("417".to_owned()),
        }
    }

    #[test]
    fn every_resource_is_marked_managed() {
        assert_eq!(
            labels().to_map().get(MANAGED).map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn selector_is_scoped_to_one_execution() {
        let selector = labels().selector();
        assert!(selector.contains(&"autospec.managed=true".to_owned()));
        assert!(selector
            .iter()
            .any(|s| s == "autospec.execution_id=node-417-impl-01"));
    }
}
