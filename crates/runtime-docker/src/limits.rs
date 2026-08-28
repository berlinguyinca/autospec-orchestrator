use bollard::models::{HostConfig, HostConfigLogConfig};
use orchestrator_core::RuntimeRequirement;

/// A conservative process ceiling applied to every execution container.
pub const DEFAULT_PIDS_LIMIT: i64 = 512;

/// Docker normalizes a zero shared-memory request to its 64 MiB default.
/// Keep that memory-only pseudo-filesystem explicit and bounded instead.
pub const DEFAULT_SHM_SIZE: i64 = 64 * 1024 * 1024;

pub type HostConfigLimits = HostConfig;

/// Translate manifest resource requirements into daemon-enforced limits.
///
/// CPU, memory, PID, and disk quotas are runtime protections, not prompt
/// guidance (spec sections 13 and 81). The writable container layer is disabled;
/// execution-storage provides the verified, aggregate-bounded bind filesystem.
pub fn host_limits(requirement: &RuntimeRequirement) -> HostConfigLimits {
    let memory = requirement
        .memory_mib
        .saturating_mul(1024)
        .saturating_mul(1024)
        .min(i64::MAX as u64) as i64;
    let nano_cpus = u64::from(requirement.cpu)
        .saturating_mul(1_000_000_000)
        .min(i64::MAX as u64) as i64;

    HostConfig {
        nano_cpus: Some(nano_cpus),
        memory: Some(memory),
        memory_swap: Some(memory),
        pids_limit: Some(DEFAULT_PIDS_LIMIT),
        ipc_mode: Some("none".to_owned()),
        shm_size: Some(DEFAULT_SHM_SIZE),
        privileged: Some(false),
        publish_all_ports: Some(false),
        port_bindings: None,
        readonly_rootfs: Some(true),
        log_config: Some(HostConfigLogConfig {
            typ: Some("none".to_owned()),
            config: None,
        }),
        storage_opt: None,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_writable_namespace_is_memory_only_and_ipc_is_disabled() {
        let limits = host_limits(&RuntimeRequirement::default());

        assert_eq!(limits.ipc_mode.as_deref(), Some("none"));
        assert_eq!(limits.shm_size, Some(DEFAULT_SHM_SIZE));
        assert!(limits.tmpfs.is_none());
    }
}
