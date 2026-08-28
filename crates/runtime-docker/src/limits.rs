use bollard::models::{HostConfig, HostConfigLogConfig};
use orchestrator_core::RuntimeRequirement;

/// A conservative process ceiling applied to every execution container.
pub const DEFAULT_PIDS_LIMIT: i64 = 512;

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
