//! Docker runtime adapter (spec section 62, initial runtime).
//!
//! Every resource it creates carries the mandatory ownership labels, and cleanup
//! selects on those labels only. Global prune operations are forbidden
//! (spec section 42).

use async_trait::async_trait;
use orchestrator_core::{ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, Runtime, RuntimeError};

#[derive(Debug, Default, Clone)]
pub struct DockerRuntime;

impl DockerRuntime {
    pub fn new() -> Self {
        Self
    }

    /// Network name for an execution's isolated environment.
    pub fn network_name(execution_id: &ExecutionId) -> String {
        format!("autospec-{execution_id}")
    }
}

#[async_trait]
impl Runtime for DockerRuntime {
    fn name(&self) -> &'static str {
        "docker"
    }

    async fn available(&self) -> bool {
        todo!("probe the Docker daemon")
    }

    async fn provision(
        &self,
        _labels: &OwnershipLabels,
        _runtime: &RuntimeRequirement,
        _services: &[ServiceRequirement],
    ) -> Result<EnvironmentHandle, RuntimeError> {
        todo!("create labelled network, service containers, and agent container")
    }

    async fn destroy(&self, _labels: &OwnershipLabels) -> Result<(), RuntimeError> {
        todo!("remove only resources matching labels.selector()")
    }

    async fn reconcile(&self, _live: &[ExecutionId]) -> Result<Vec<ExecutionId>, RuntimeError> {
        todo!("list autospec.managed=true resources and report orphans")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_name_is_execution_scoped() {
        assert_eq!(
            DockerRuntime::network_name(&ExecutionId::new("node-417-impl-01")),
            "autospec-node-417-impl-01"
        );
    }
}
