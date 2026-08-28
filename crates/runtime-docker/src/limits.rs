use bollard::models::HostConfig;
use orchestrator_core::RuntimeRequirement;
use std::collections::HashMap;

/// A conservative process ceiling applied to every execution container.
pub const DEFAULT_PIDS_LIMIT: i64 = 512;

pub type HostConfigLimits = HostConfig;

/// Translate manifest resource requirements into daemon-enforced limits.
///
/// CPU, memory, PID, and writable-layer disk quotas are runtime protections,
/// not prompt guidance (spec sections 13 and 81). Image-declared data paths are
/// replaced by bounded tmpfs mounts so Docker never creates anonymous volumes.
/// Every agent/service writable layer and tmpfs allocation shares one manifest
/// disk budget; a daemon that cannot enforce either bound rejects provisioning.
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
        storage_opt: Some(HashMap::from([(
            "size".to_owned(),
            format!("{}G", requirement.disk_gib),
        )])),
        ..Default::default()
    }
}

pub(crate) fn set_writable_layer_limit(limits: &mut HostConfigLimits, bytes: u64) {
    limits.storage_opt = Some(HashMap::from([("size".to_owned(), bytes.to_string())]));
}
