use crate::{
    limits::host_limits,
    provision::{container_create_error, verify_container_mount_sources, ReadyStorageGuard},
    DockerRuntime,
};
use bollard::{
    container::{Config, CreateContainerOptions, NetworkingConfig},
    models::{EndpointSettings, Mount},
};
use orchestrator_core::{OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::RuntimeError;
use std::{collections::HashMap, path::Path};

pub(crate) async fn create_services(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    services: &[ServiceRequirement],
    network: &str,
    service_mounts: &[Vec<Mount>],
    storage: (&Path, &ReadyStorageGuard<'_>, &[Mount]),
) -> Result<Vec<String>, RuntimeError> {
    let (execution_root, storage, all_mounts) = storage;
    let mut containers = Vec::with_capacity(services.len());
    for (service, mounts) in services.iter().zip(service_mounts) {
        let name = DockerRuntime::service_container_name(&labels.execution_id, &service.name);
        let mut limits = host_limits(requirement);
        limits.network_mode = Some(network.to_owned());
        limits.mounts = Some(mounts.clone());
        let mut environment = service
            .env
            .iter()
            .filter(|(key, _)| *key != "HOME" && *key != "TMPDIR")
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        environment.push("HOME=/home/autospec".to_owned());
        environment.push("TMPDIR=/tmp".to_owned());
        let config = Config {
            image: Some(service.image.clone()),
            env: Some(environment),
            labels: Some(labels.to_map().into_iter().collect()),
            host_config: Some(limits),
            networking_config: Some(networking_config(network, &service.name)),
            ..Default::default()
        };
        storage.verify(all_mounts).await?;
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
                container_create_error(format!("create service container {name}"), error, true)
            })?;
        storage.verify(all_mounts).await?;
        verify_container_mount_sources(runtime, &name, execution_root, mounts).await?;
        containers.push(name);
    }
    Ok(containers)
}

pub(crate) async fn start_services(
    runtime: &DockerRuntime,
    containers: &[String],
    storage: &ReadyStorageGuard<'_>,
    all_mounts: &[Mount],
) -> Result<(), RuntimeError> {
    for name in containers {
        storage.verify(all_mounts).await?;
        runtime
            .client
            .start_container::<String>(name, None)
            .await
            .map_err(|error| {
                RuntimeError::Provisioning(format!("start service container {name}: {error}"))
            })?;
    }
    Ok(())
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
