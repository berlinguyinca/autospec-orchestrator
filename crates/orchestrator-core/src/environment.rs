//! Repository-declared execution environment resolution (spec sections 45, 63).

use crate::{manifest::validate_image, CoreError, RuntimeRequirement, ServiceRequirement};
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
    /// Resolves requested names without inferring additional task dependencies.
    pub fn resolve(&self, requires: &[String]) -> Result<Vec<ServiceRequirement>, CoreError> {
        requires
            .iter()
            .map(|name| {
                let declared = self.services.get(name).ok_or_else(|| {
                    CoreError::InvalidManifest(format!(
                        "requested service is not declared by the repository: {name}"
                    ))
                })?;
                validate_image(&declared.image, "environment service image")?;
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
}
