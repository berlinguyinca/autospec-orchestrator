use crate::{limits::host_limits, services, DockerRuntime};
use bollard::{
    container::{Config, CreateContainerOptions},
    network::CreateNetworkOptions,
};
use orchestrator_core::{OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, RuntimeError};

pub(crate) async fn provision(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    service_requirements: &[ServiceRequirement],
) -> Result<EnvironmentHandle, RuntimeError> {
    runtime.require_compatible_daemon().await?;
    let result = provision_inner(runtime, labels, requirement, service_requirements).await;
    if result.is_err() {
        let _ = crate::cleanup::destroy(runtime, labels).await;
    }
    result
}

async fn provision_inner(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    service_requirements: &[ServiceRequirement],
) -> Result<EnvironmentHandle, RuntimeError> {
    let network = DockerRuntime::network_name(&labels.execution_id);
    runtime
        .client
        .create_network(CreateNetworkOptions {
            name: network.clone(),
            check_duplicate: true,
            driver: "bridge".to_owned(),
            internal: false,
            labels: labels.to_map().into_iter().collect(),
            ..Default::default()
        })
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!("create network {network}: {error}"))
        })?;

    let service_containers =
        services::create_services(runtime, labels, requirement, service_requirements, &network)
            .await?;

    let image = requirement.image.as_deref().ok_or_else(|| {
        RuntimeError::Provisioning("runtime image is required for Docker provisioning".to_owned())
    })?;
    runtime.ensure_image(image).await?;
    let agent_container = DockerRuntime::agent_container_name(&labels.execution_id);
    let mut limits = host_limits(requirement);
    limits.network_mode = Some(network.clone());
    runtime
        .client
        .create_container(
            Some(CreateContainerOptions {
                name: agent_container.clone(),
                platform: None,
            }),
            Config {
                image: Some(image.to_owned()),
                cmd: Some(vec!["sleep".to_owned(), "infinity".to_owned()]),
                labels: Some(labels.to_map().into_iter().collect()),
                host_config: Some(limits),
                networking_config: Some(services::networking_config(&network, "agent")),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!("create agent container {agent_container}: {error}"))
        })?;
    runtime
        .client
        .start_container::<String>(&agent_container, None)
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!("start agent container {agent_container}: {error}"))
        })?;

    Ok(EnvironmentHandle {
        execution_id: labels.execution_id.clone(),
        network,
        agent_container,
        service_containers,
        volumes: Vec::new(),
        credentials_path: None,
    })
}
