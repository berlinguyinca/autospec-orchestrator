use crate::{limits::host_limits, DockerRuntime};
use bollard::{
    container::{Config, CreateContainerOptions, NetworkingConfig},
    models::EndpointSettings,
};
use orchestrator_core::{OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::RuntimeError;
use std::collections::HashMap;

pub(crate) async fn create_services(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    services: &[ServiceRequirement],
    network: &str,
) -> Result<Vec<String>, RuntimeError> {
    let mut containers = Vec::with_capacity(services.len());
    for service in services {
        runtime.ensure_image(&service.image).await?;
        let name = DockerRuntime::service_container_name(&labels.execution_id, &service.name);
        let mut limits = host_limits(requirement);
        limits.network_mode = Some(network.to_owned());
        let config = Config {
            image: Some(service.image.clone()),
            env: (!service.env.is_empty()).then(|| {
                service
                    .env
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect()
            }),
            labels: Some(labels.to_map().into_iter().collect()),
            host_config: Some(limits),
            networking_config: Some(networking_config(network, &service.name)),
            ..Default::default()
        };
        runtime
            .client
            .create_container(
                Some(CreateContainerOptions {
                    name: name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .map_err(|error| {
                RuntimeError::Provisioning(format!("create service container {name}: {error}"))
            })?;
        runtime
            .client
            .start_container::<String>(&name, None)
            .await
            .map_err(|error| {
                RuntimeError::Provisioning(format!("start service container {name}: {error}"))
            })?;
        containers.push(name);
    }
    Ok(containers)
}

pub(crate) fn networking_config(network: &str, alias: &str) -> NetworkingConfig<String> {
    NetworkingConfig {
        endpoints_config: HashMap::from([(
            network.to_owned(),
            EndpointSettings {
                aliases: Some(vec![alias.to_owned()]),
                ..Default::default()
            },
        )]),
    }
}
