use bollard::models::{HostConfig, HostConfigLogConfig};
use orchestrator_core::RuntimeRequirement;

/// A conservative process ceiling applied to every execution container.
pub const DEFAULT_PIDS_LIMIT: i64 = 512;

/// Docker normalizes a zero shared-memory request to its 64 MiB default.
/// Keep that memory-only pseudo-filesystem explicit and bounded instead.
pub const DEFAULT_SHM_SIZE: i64 = 64 * 1024 * 1024;

pub type HostConfigLimits = HostConfig;

/// Security option applied to every execution container: a process can never
/// gain privileges through setuid or file capabilities (AGENTS.md invariant 10,
/// spec section 5 R-ORCH).
pub const NO_NEW_PRIVILEGES: &str = "no-new-privileges:true";

/// Whether workload containers run with the runtime-enforced privilege limits.
///
/// `Enforced` is the default. `Relaxed` restores the pre-hardening behaviour
/// for deployments whose images need setuid helpers or capabilities (the agent
/// keeps its default capabilities and `no-new-privileges` is not set on agent
/// or service containers). It is an explicit operator opt-out; the trusted
/// mount verifier is always hardened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContainerHardening {
    #[default]
    Enforced,
    Relaxed,
}

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
        security_opt: Some(vec![NO_NEW_PRIVILEGES.to_owned()]),
        ..Default::default()
    }
}

/// Capabilities the root agent keeps after `cap_drop: ALL`.
///
/// The agent edits a repository the host worker created: files owned by the
/// orchestrator uid with ordinary modes, in nested directories. On a rootful
/// daemon a capability-less container root is "other" to those files, so it
/// could not write `src/lib.rs`, create files below `src/`, or remove a
/// top-level file from the sticky workspace root (observed with
/// `Permission denied`). `CAP_DAC_OVERRIDE` and `CAP_FOWNER` restore exactly
/// that file access and nothing else. Remove this when the agent runs as the
/// uid that owns its worktree (the non-root agent chain).
pub const ROOT_AGENT_RETAINED_CAPABILITIES: [&str; 2] = ["DAC_OVERRIDE", "FOWNER"];

/// Drops every Linux capability. Applied to the agent and the trusted
/// verifier; service containers keep the default set so entrypoints that
/// `chown` or switch user (for example `postgres`, `redis`) still start.
///
/// A root process keeps signalling processes of its own uid without
/// `CAP_KILL`, so harness probes keep working.
pub fn drop_all_capabilities(limits: &mut HostConfigLimits) {
    limits.cap_drop = Some(vec!["ALL".to_owned()]);
}

/// Applies `hardening` to an agent container's limits.
pub(crate) fn harden_agent(limits: &mut HostConfigLimits, hardening: ContainerHardening) {
    match hardening {
        ContainerHardening::Enforced => {
            drop_all_capabilities(limits);
            limits.cap_add = Some(
                ROOT_AGENT_RETAINED_CAPABILITIES
                    .iter()
                    .map(|capability| (*capability).to_owned())
                    .collect(),
            );
        }
        ContainerHardening::Relaxed => limits.security_opt = None,
    }
}

/// Applies `hardening` to a service container's limits (default capabilities).
pub(crate) fn harden_service(limits: &mut HostConfigLimits, hardening: ContainerHardening) {
    if hardening == ContainerHardening::Relaxed {
        limits.security_opt = None;
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

    #[test]
    fn every_container_gets_no_new_privileges() {
        let limits = host_limits(&RuntimeRequirement::default());
        assert_eq!(
            limits.security_opt,
            Some(vec!["no-new-privileges:true".to_owned()])
        );
        assert_eq!(limits.cap_drop, None, "host_limits alone drops nothing");
    }

    #[test]
    fn drop_all_capabilities_sets_cap_drop_all() {
        let mut limits = host_limits(&RuntimeRequirement::default());
        drop_all_capabilities(&mut limits);
        assert_eq!(limits.cap_drop, Some(vec!["ALL".to_owned()]));
        assert_eq!(
            limits.security_opt,
            Some(vec!["no-new-privileges:true".to_owned()])
        );
    }

    #[test]
    fn enforced_hardening_drops_agent_capabilities_but_not_service_capabilities() {
        let mut agent = host_limits(&RuntimeRequirement::default());
        harden_agent(&mut agent, ContainerHardening::Enforced);
        assert_eq!(agent.cap_drop, Some(vec!["ALL".to_owned()]));
        assert_eq!(
            agent.cap_add,
            Some(vec!["DAC_OVERRIDE".to_owned(), "FOWNER".to_owned()])
        );
        let mut service = host_limits(&RuntimeRequirement::default());
        harden_service(&mut service, ContainerHardening::Enforced);
        assert_eq!(service.cap_drop, None);
        assert!(service.security_opt.is_some());
    }

    #[test]
    fn relaxed_hardening_restores_the_previous_container_options() {
        let mut agent = host_limits(&RuntimeRequirement::default());
        harden_agent(&mut agent, ContainerHardening::Relaxed);
        assert_eq!(
            (agent.cap_drop, agent.cap_add, agent.security_opt),
            (None, None, None)
        );
        let mut service = host_limits(&RuntimeRequirement::default());
        harden_service(&mut service, ContainerHardening::Relaxed);
        assert_eq!((service.cap_drop, service.security_opt), (None, None));
    }
}
