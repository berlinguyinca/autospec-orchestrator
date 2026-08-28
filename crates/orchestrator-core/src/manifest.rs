//! The execution manifest: what AutoSpec (or Workbench) asks the execution plane
//! to do (spec sections 5, 20, 45, 63, 75).

use crate::{task_packet::TaskPacket, CoreError, Role, MANIFEST_API_VERSION};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::OnceLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionManifest {
    /// Always `autospec.dev/v1alpha1` for this revision.
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub role: Role,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskReference>,
    pub repository: RepositoryReference,
    pub agent: AgentAssignment,
    #[serde(default)]
    pub runtime: RuntimeRequirement,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ServiceRequirement>,
    #[serde(default)]
    pub persistence: PersistenceMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_packet: Option<TaskPacket>,
}

impl ExecutionManifest {
    /// Validates only execution-plane constraints before provisioning resources.
    /// Task intent and model policy remain opaque (spec sections 5, 45, 75, 77).
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.api_version != MANIFEST_API_VERSION {
            return invalid(format!(
                "apiVersion must be {MANIFEST_API_VERSION}, got {}",
                self.api_version
            ));
        }
        validate_repository(&self.repository.repo)?;
        validate_runtime(&self.runtime)?;
        for service in &self.services {
            validate_service_name(&service.name)?;
            validate_image(&service.image, "service image")?;
        }
        Ok(())
    }
}

pub(crate) fn validate_runtime(runtime: &RuntimeRequirement) -> Result<(), CoreError> {
    if runtime.cpu == 0 {
        return invalid("runtime cpu must be greater than zero");
    }
    if runtime.memory_mib < 256 {
        return invalid("runtime memoryMib must be at least 256");
    }
    if runtime.disk_gib == 0 {
        return invalid("runtime diskGib must be greater than zero");
    }
    if let Some(image) = &runtime.image {
        validate_image(image, "runtime image")?;
    }
    for capability in &runtime.capabilities {
        if !capability_pattern().is_match(capability) {
            return invalid(format!("invalid runtime capability: {capability}"));
        }
    }
    Ok(())
}

pub(crate) fn validate_service_name(name: &str) -> Result<(), CoreError> {
    if capability_pattern().is_match(name) {
        Ok(())
    } else {
        invalid(format!("invalid service name: {name}"))
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T, CoreError> {
    Err(CoreError::InvalidManifest(message.into()))
}

fn validate_repository(repository: &str) -> Result<(), CoreError> {
    let mut parts = repository.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(owner), Some(name), None) if !owner.is_empty() && !name.is_empty() => Ok(()),
        _ => invalid("repository.repo must be exactly owner/name"),
    }
}

pub(crate) fn validate_image(image: &str, field: &str) -> Result<(), CoreError> {
    let tagged = image
        .rsplit_once('/')
        .map_or(image, |(_, final_component)| final_component)
        .split_once(':')
        .is_some_and(|(name, tag)| !name.is_empty() && !tag.is_empty());
    let digested = image
        .split_once('@')
        .is_some_and(|(name, digest)| !name.is_empty() && !digest.is_empty());
    if tagged || digested {
        Ok(())
    } else {
        invalid(format!("{field} must include a tag or digest: {image}"))
    }
}

fn capability_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"^[a-z0-9][a-z0-9-]*$").expect("static regex is valid"))
}

/// A reference back into AutoSpec's workflow domain. The orchestrator treats
/// these as opaque correlation keys (spec section 77).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReference {
    pub project_id: String,
    pub issue_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryReference {
    /// `owner/name`.
    pub repo: String,
    #[serde(rename = "baseRef")]
    pub base_ref: String,
    /// Resolved base commit, filled in by the orchestrator when it fetches.
    #[serde(rename = "baseSha", skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    /// Branch AutoSpec wants the work on. AutoSpec owns branch *naming*; the
    /// orchestrator owns the physical worktree (spec section 8).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HarnessKind {
    Pi,
    OpenCode,
    Codex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAssignment {
    pub harness: HarnessKind,
    #[serde(rename = "modelPolicy")]
    pub model_policy: ModelPolicy,
}

/// AutoSpec's model decision, passed through verbatim. The orchestrator does not
/// select GPUs, load models, or rewrite this policy (spec sections 12, 15).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelPolicy {
    /// Always `inferweave` today.
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preferred: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alternatives: Vec<String>,
    #[serde(rename = "fallbackClass", skip_serializing_if = "Option::is_none")]
    pub fallback_class: Option<String>,
}

fn default_provider() -> String {
    "inferweave".to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeKind {
    #[default]
    Docker,
    Podman,
    Apptainer,
}

/// Capability-shaped placement requirements. Never a named physical machine
/// (spec section 45).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRequirement {
    #[serde(rename = "type", default)]
    pub kind: RuntimeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<String>,
    #[serde(default = "default_cpu")]
    pub cpu: u32,
    #[serde(rename = "memoryMib", default = "default_memory")]
    pub memory_mib: u64,
    #[serde(rename = "diskGib", default = "default_disk")]
    pub disk_gib: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

fn default_cpu() -> u32 {
    2
}
fn default_memory() -> u64 {
    4096
}
fn default_disk() -> u64 {
    20
}

impl Default for RuntimeRequirement {
    fn default() -> Self {
        Self {
            kind: RuntimeKind::default(),
            image: None,
            os: None,
            cpu: default_cpu(),
            memory_mib: default_memory(),
            disk_gib: default_disk(),
            capabilities: Vec::new(),
        }
    }
}

/// A supporting service the execution needs, provisioned into the execution's
/// own isolated network (spec sections 62, 63).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRequirement {
    pub name: String,
    pub image: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistenceMode {
    /// Environment is torn down when the execution reaches a terminal state.
    #[default]
    Ephemeral,
    /// Session and worktree survive so a human can attach later
    /// (spec sections 38, 86).
    Resumable,
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    fn example_manifest() -> ExecutionManifest {
        serde_yaml::from_str(include_str!("../../../examples/execution-manifest.yaml"))
            .expect("example manifest parses")
    }

    #[test]
    fn canonical_example_is_valid() {
        example_manifest().validate().expect("manifest is valid");
    }

    #[test]
    fn wrong_api_version_is_rejected() {
        let mut manifest = example_manifest();
        manifest.api_version = "autospec.dev/v2".to_owned();

        assert!(matches!(
            manifest.validate(),
            Err(crate::CoreError::InvalidManifest(message)) if message.contains("apiVersion")
        ));
    }

    #[test]
    fn invalid_resources_repository_services_and_capabilities_are_rejected() {
        let mut manifest = example_manifest();
        manifest.runtime.cpu = 0;
        assert!(manifest.validate().is_err());

        let mut manifest = example_manifest();
        manifest.runtime.memory_mib = 255;
        assert!(manifest.validate().is_err());

        let mut manifest = example_manifest();
        manifest.runtime.disk_gib = 0;
        assert!(manifest.validate().is_err());

        let mut manifest = example_manifest();
        manifest.repository.repo = "owner/repo/extra".to_owned();
        assert!(manifest.validate().is_err());

        let mut manifest = example_manifest();
        manifest.services[0].image = "postgres".to_owned();
        assert!(manifest.validate().is_err());

        let mut manifest = example_manifest();
        manifest
            .runtime
            .capabilities
            .push("worker.example".to_owned());
        assert!(manifest.validate().is_err());

        let mut manifest = example_manifest();
        manifest.runtime.capabilities.push("Docker".to_owned());
        assert!(manifest.validate().is_err());
    }
}
