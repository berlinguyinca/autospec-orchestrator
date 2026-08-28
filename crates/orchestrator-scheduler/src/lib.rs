//! Execution scheduling: matching capability-shaped requirements to workers
//! (spec section 13).
//!
//! This scheduler deals in CPU, RAM, disk, OS, runtimes, and toolchains only.
//! It never reasons about VRAM, GPUs, loaded models, or token throughput —
//! that is InferWeave's scheduler (spec section 12).

use orchestrator_core::{RuntimeRequirement, WorkerRegistration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("no worker satisfies the requirement: {0}")]
    NoEligibleWorker(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReservedResources {
    pub slots: u32,
    pub cpu: u32,
    pub memory_mib: u64,
}

/// Whether a worker can host an execution with these requirements.
pub fn worker_fits(worker: &WorkerRegistration, req: &RuntimeRequirement) -> bool {
    worker_fits_with_reservations(
        worker,
        req,
        ReservedResources {
            slots: worker.running_executions,
            ..ReservedResources::default()
        },
    )
}

pub fn worker_fits_with_reservations(
    worker: &WorkerRegistration,
    req: &RuntimeRequirement,
    reserved: ReservedResources,
) -> bool {
    if worker.state != orchestrator_core::WorkerState::Ready
        || !worker
            .capability_proof
            .as_ref()
            .is_some_and(orchestrator_core::WorkerCapabilityProof::is_complete)
        || reserved.slots >= worker.capabilities.max_concurrent_executions
    {
        return false;
    }
    if let Some(os) = &req.os {
        if !os.eq_ignore_ascii_case(&worker.capabilities.os) {
            return false;
        }
    }
    if worker.capabilities.cpu.saturating_sub(reserved.cpu) < req.cpu
        || worker
            .capabilities
            .memory_mib
            .saturating_sub(reserved.memory_mib)
            < req.memory_mib
        || worker.capabilities.disk_gib < req.disk_gib
    {
        return false;
    }
    if !worker.capabilities.runtimes.contains(&req.kind) {
        return false;
    }
    req.capabilities
        .iter()
        .all(|c| worker.capabilities.capabilities.contains(c))
}

/// Pick the least-loaded eligible worker.
pub fn select<'a>(
    workers: &'a [WorkerRegistration],
    req: &RuntimeRequirement,
) -> Result<&'a WorkerRegistration, ScheduleError> {
    workers
        .iter()
        .filter(|w| worker_fits(w, req))
        .min_by_key(|w| w.running_executions)
        .ok_or_else(|| ScheduleError::NoEligibleWorker(format!("{req:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orchestrator_core::{
        RuntimeKind, WorkerCapabilities, WorkerCapabilityProof, WorkerId, WorkerState,
    };

    fn worker(id: &str, cpu: u32, caps: &[&str], running: u32) -> WorkerRegistration {
        WorkerRegistration {
            id: WorkerId::new(id),
            capabilities: WorkerCapabilities {
                os: "linux".into(),
                arch: "x86_64".into(),
                cpu,
                memory_mib: 32768,
                disk_gib: 500,
                runtimes: vec![RuntimeKind::Docker],
                capabilities: caps.iter().map(|s| (*s).to_owned()).collect(),
                max_concurrent_executions: 4,
            },
            state: WorkerState::Ready,
            running_executions: running,
            last_heartbeat: Utc::now(),
            capability_proof: Some(WorkerCapabilityProof {
                storage_backend: "test".into(),
                storage_pool_identity: "pool".into(),
                docker_daemon_id: "daemon".into(),
                docker_verifier: "probe".into(),
                docker_method_version: "v1".into(),
            }),
        }
    }

    fn req(cpu: u32, caps: &[&str]) -> RuntimeRequirement {
        RuntimeRequirement {
            os: Some("linux".into()),
            cpu,
            capabilities: caps.iter().map(|s| (*s).to_owned()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn missing_capability_disqualifies_a_worker() {
        let w = worker("a", 32, &["docker"], 0);
        assert!(!worker_fits(&w, &req(8, &["docker", "playwright"])));
    }

    #[test]
    fn least_loaded_eligible_worker_wins() {
        let workers = vec![
            worker("busy", 32, &["docker"], 3),
            worker("idle", 32, &["docker"], 0),
        ];
        assert_eq!(
            select(&workers, &req(8, &["docker"])).unwrap().id.as_str(),
            "idle"
        );
    }

    #[test]
    fn a_full_worker_is_not_eligible() {
        let mut w = worker("full", 32, &["docker"], 4);
        w.running_executions = 4;
        assert!(!worker_fits(&w, &req(2, &[])));
    }

    #[test]
    fn aggregate_reservations_reduce_cpu_and_memory_capacity() {
        let w = worker("reserved", 32, &["docker"], 1);
        let reserved = ReservedResources {
            slots: 1,
            cpu: 28,
            memory_mib: 30_000,
        };
        assert!(!worker_fits_with_reservations(
            &w,
            &req(8, &["docker"]),
            reserved
        ));
    }
}
