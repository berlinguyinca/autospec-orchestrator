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
    /// Disk (GiB) already promised to other executions on the worker.
    pub disk_gib: u64,
}

/// Canonical form of a CPU architecture name. `amd64` is `x86_64` and `arm64`
/// is `aarch64`; matching is case-insensitive (like `os`). Unknown names pass
/// through lowercased and compare exactly.
fn canonical_arch(arch: &str) -> String {
    let lower = arch.trim().to_ascii_lowercase();
    match lower.as_str() {
        "amd64" | "x86_64" => "x86_64".to_owned(),
        "arm64" | "aarch64" => "aarch64".to_owned(),
        _ => lower,
    }
}

/// Whether a required architecture is satisfied by the one a worker advertises
/// (spec section 5 R-PAR).
pub fn arch_matches(want: &str, have: &str) -> bool {
    canonical_arch(want) == canonical_arch(have)
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
    if let Some(arch) = &req.arch {
        if !arch_matches(arch, &worker.capabilities.arch) {
            return false;
        }
    }
    // Toolchain versions are exact strings; a toolchain the worker does not
    // advertise never matches.
    if !req
        .toolchains
        .iter()
        .all(|(name, version)| worker.capabilities.toolchains.get(name) == Some(version))
    {
        return false;
    }
    if worker.capabilities.cpu.saturating_sub(reserved.cpu) < req.cpu
        || worker
            .capabilities
            .memory_mib
            .saturating_sub(reserved.memory_mib)
            < req.memory_mib
        || worker
            .capabilities
            .disk_gib
            .saturating_sub(reserved.disk_gib)
            < req.disk_gib
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
                toolchains: [("rust".to_owned(), "1.85.1".to_owned())].into(),
                max_concurrent_executions: 4,
                health_errors: Vec::new(),
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
            ..ReservedResources::default()
        };
        assert!(!worker_fits_with_reservations(
            &w,
            &req(8, &["docker"]),
            reserved
        ));
    }

    fn fits_req(r: &RuntimeRequirement) -> bool {
        worker_fits(&worker("w", 32, &["docker"], 0), r)
    }

    fn with_arch(arch: &str) -> RuntimeRequirement {
        RuntimeRequirement {
            arch: Some(arch.into()),
            ..req(1, &[])
        }
    }

    fn with_toolchain(name: &str, version: &str) -> RuntimeRequirement {
        RuntimeRequirement {
            toolchains: [(name.to_owned(), version.to_owned())].into(),
            ..req(1, &[])
        }
    }

    #[test]
    fn amd64_requirement_fits_an_x86_64_worker() {
        assert!(fits_req(&with_arch("amd64")));
        assert!(fits_req(&with_arch("x86_64")));
    }

    #[test]
    fn aarch64_requirement_does_not_fit_an_x86_64_worker() {
        assert!(!fits_req(&with_arch("aarch64")));
        assert!(!fits_req(&with_arch("arm64")));
    }

    #[test]
    fn arch_aliases_match_in_both_directions_and_ignore_case() {
        for (a, b) in [
            ("x86_64", "amd64"),
            ("amd64", "x86_64"),
            ("aarch64", "arm64"),
            ("arm64", "aarch64"),
            ("AMD64", "x86_64"),
            ("ARM64", "aarch64"),
        ] {
            assert!(arch_matches(a, b), "{a} should match {b}");
        }
        assert!(!arch_matches("amd64", "arm64"));
        assert!(!arch_matches("riscv64", "x86_64"));
        assert!(arch_matches("riscv64", "RISCV64"));
    }

    #[test]
    fn arm64_requirement_fits_an_aarch64_worker() {
        let mut w = worker("arm", 32, &["docker"], 0);
        w.capabilities.arch = "aarch64".into();
        assert!(worker_fits(&w, &with_arch("arm64")));
        assert!(!worker_fits(&w, &with_arch("amd64")));
    }

    #[test]
    fn toolchain_version_must_match_exactly() {
        assert!(fits_req(&with_toolchain("rust", "1.85.1")));
        assert!(!fits_req(&with_toolchain("rust", "1.86.0")));
        assert!(!fits_req(&with_toolchain("rust", "1.85")));
    }

    #[test]
    fn unadvertised_toolchain_does_not_fit() {
        assert!(!fits_req(&with_toolchain("node", "20.11.0")));
        let mut w = worker("old", 32, &["docker"], 0);
        w.capabilities.toolchains.clear();
        assert!(!worker_fits(&w, &with_toolchain("rust", "1.85.1")));
    }

    #[test]
    fn requirement_without_new_fields_fits_a_worker_without_toolchains() {
        let mut w = worker("old", 32, &["docker"], 0);
        w.capabilities.toolchains.clear();
        assert!(worker_fits(&w, &req(1, &[])));
    }

    #[test]
    fn reserved_disk_reduces_available_disk() {
        let mut w = worker("disk", 32, &["docker"], 0);
        w.capabilities.disk_gib = 100;
        let want = RuntimeRequirement {
            disk_gib: 20,
            ..req(1, &[])
        };
        let reserved = |disk_gib| ReservedResources {
            disk_gib,
            ..ReservedResources::default()
        };
        assert!(!worker_fits_with_reservations(&w, &want, reserved(90)));
        assert!(worker_fits_with_reservations(&w, &want, reserved(80)));
        assert!(!worker_fits_with_reservations(&w, &want, reserved(200)));
    }

    #[test]
    fn manifest_runtime_without_new_fields_still_parses() {
        let parsed: RuntimeRequirement =
            serde_json::from_str(r#"{"type":"docker","cpu":4,"memoryMib":1024,"diskGib":10}"#)
                .unwrap();
        assert!(parsed.arch.is_none());
        assert!(parsed.toolchains.is_empty());
        let round: RuntimeRequirement =
            serde_json::from_str(&serde_json::to_string(&parsed).unwrap()).unwrap();
        assert!(round.arch.is_none() && round.toolchains.is_empty());
    }

    #[test]
    fn new_requirement_fields_round_trip() {
        let parsed: RuntimeRequirement =
            serde_json::from_str(r#"{"arch":"arm64","toolchains":{"rust":"1.86.0"}}"#).unwrap();
        assert_eq!(parsed.arch.as_deref(), Some("arm64"));
        assert_eq!(parsed.toolchains["rust"], "1.86.0");
    }

    #[test]
    fn older_worker_capabilities_json_without_toolchains_still_parses() {
        let parsed: WorkerCapabilities = serde_json::from_str(
            r#"{"os":"linux","arch":"x86_64","cpu":4,"memoryMib":4096,"diskGib":40}"#,
        )
        .unwrap();
        assert!(parsed.toolchains.is_empty());
    }
}
