//! Repository-declared execution environment resolution (spec sections 45, 63).

use crate::{
    manifest::{validate_image, validate_runtime, validate_service_name},
    CoreError, RuntimeRequirement, ServiceRequirement,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Parsed `.autospec/environment.yaml` from a source repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentFile {
    #[serde(default)]
    pub runtime: RuntimeRequirement,
    #[serde(default)]
    pub services: BTreeMap<String, EnvironmentService>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentService {
    pub image: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl EnvironmentFile {
    /// Validates every declared runtime and service before any name is resolved.
    pub fn validate(&self) -> Result<(), CoreError> {
        validate_runtime(&self.runtime)?;
        for (name, service) in &self.services {
            validate_service_name(name)?;
            validate_image(&service.image, "environment service image")?;
        }
        Ok(())
    }

    /// Resolves requested names without inferring additional task dependencies.
    pub fn resolve(&self, requires: &[String]) -> Result<Vec<ServiceRequirement>, CoreError> {
        self.validate()?;
        requires
            .iter()
            .map(|name| {
                let declared = self.services.get(name).ok_or_else(|| {
                    CoreError::InvalidManifest(format!(
                        "requested service is not declared by the repository: {name}"
                    ))
                })?;
                Ok(ServiceRequirement {
                    name: name.clone(),
                    image: declared.image.clone(),
                    env: declared.env.clone(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::EnvironmentFile;

    #[test]
    fn resolves_requested_services_from_the_repository_environment() {
        let environment: EnvironmentFile =
            serde_yaml::from_str(include_str!("../../../examples/environment.yaml"))
                .expect("environment parses");

        environment.validate().expect("environment is valid");

        let services = environment
            .resolve(&["redis".to_owned(), "postgres".to_owned()])
            .expect("declared services resolve");

        assert_eq!(services.len(), 2);
        assert_eq!(services[0].name, "redis");
        assert_eq!(services[0].image, "redis:8");
        assert_eq!(services[1].name, "postgres");
        assert_eq!(services[1].image, "postgres:17");
        assert_eq!(
            services[1].env.get("POSTGRES_PASSWORD").map(String::as_str),
            Some("autospec")
        );
    }

    #[test]
    fn rejects_a_service_the_repository_did_not_declare() {
        let environment: EnvironmentFile =
            serde_yaml::from_str(include_str!("../../../examples/environment.yaml"))
                .expect("environment parses");

        assert!(environment.resolve(&["mysql".to_owned()]).is_err());
    }

    #[test]
    fn rejects_invalid_runtime_resources_and_capabilities() {
        let mut environment: EnvironmentFile =
            serde_yaml::from_str(include_str!("../../../examples/environment.yaml"))
                .expect("environment parses");
        environment.runtime.cpu = 0;
        assert!(environment.validate().is_err());

        let mut environment: EnvironmentFile =
            serde_yaml::from_str(include_str!("../../../examples/environment.yaml"))
                .expect("environment parses");
        environment
            .runtime
            .capabilities
            .push("Build.Host".to_owned());
        assert!(environment.validate().is_err());
    }

    #[test]
    fn resolve_rejects_an_unrequested_malformed_service_declaration() {
        let environment: EnvironmentFile = serde_yaml::from_str(
            r#"
runtime:
  image: ghcr.io/inferweave/autospec-rust:latest
services:
  postgres:
    image: postgres:17
  unused:
    image: malformed
"#,
        )
        .expect("environment parses");

        assert!(environment.resolve(&["postgres".to_owned()]).is_err());
    }
}
