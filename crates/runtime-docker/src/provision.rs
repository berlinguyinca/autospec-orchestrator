use crate::{limits::host_limits, services, DockerRuntime};
use bollard::{
    container::{Config, CreateContainerOptions},
    models::{ImageInspect, Mount, MountTypeEnum},
    network::CreateNetworkOptions,
    volume::CreateVolumeOptions,
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
    match provision_inner(runtime, labels, requirement, service_requirements).await {
        Ok(handle) => Ok(handle),
        Err(provision_error) => match crate::cleanup::destroy(runtime, labels).await {
            Ok(()) => Err(provision_error),
            Err(rollback_error) => Err(RuntimeError::Provisioning(format!(
                "execution_id={}: {provision_error}; rollback failed: {rollback_error}",
                labels.execution_id
            ))),
        },
    }
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

    let provisioned_services =
        services::create_services(runtime, labels, requirement, service_requirements, &network)
            .await?;

    let image = requirement.image.as_deref().ok_or_else(|| {
        RuntimeError::Provisioning("runtime image is required for Docker provisioning".to_owned())
    })?;
    let image_inspect = runtime.ensure_image(image).await?;
    let agent_container = DockerRuntime::agent_container_name(&labels.execution_id);
    let mut limits = host_limits(requirement);
    limits.network_mode = Some(network.clone());
    let agent_volumes = create_image_volumes(runtime, labels, &image_inspect, "agent").await?;
    limits.mounts = (!agent_volumes.mounts.is_empty()).then_some(agent_volumes.mounts);
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
        service_containers: provisioned_services.containers,
        volumes: provisioned_services
            .volumes
            .into_iter()
            .chain(agent_volumes.names)
            .collect(),
        credentials_path: None,
    })
}

pub(crate) async fn create_image_volumes(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    image: &ImageInspect,
    purpose_prefix: &str,
) -> Result<ImageVolumes, RuntimeError> {
    let mut targets = image
        .config
        .as_ref()
        .and_then(|config| config.volumes.as_ref())
        .map(|volumes| volumes.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    targets.sort();

    let expected_labels = labels.to_map();
    let mut names = Vec::with_capacity(targets.len());
    let mut mounts = Vec::with_capacity(targets.len());
    for (index, target) in targets.into_iter().enumerate() {
        let target_purpose = volume_purpose(&target);
        let suffix = if index == 0 {
            String::new()
        } else {
            format!("-{}", index + 1)
        };
        let purpose = format!("{purpose_prefix}-{target_purpose}{suffix}");
        let name = DockerRuntime::volume_name(&labels.execution_id, &purpose);
        let volume = runtime
            .client
            .create_volume(CreateVolumeOptions {
                name: name.clone(),
                driver: "local".to_owned(),
                driver_opts: Default::default(),
                labels: expected_labels.clone().into_iter().collect(),
            })
            .await
            .map_err(|error| {
                RuntimeError::Provisioning(format!("create volume {name}: {error}"))
            })?;
        if !expected_labels
            .iter()
            .all(|(key, value)| volume.labels.get(key) == Some(value))
        {
            return Err(RuntimeError::Provisioning(format!(
                "volume {name} exists without this execution's ownership labels"
            )));
        }
        mounts.push(Mount {
            target: Some(target),
            source: Some(name.clone()),
            typ: Some(MountTypeEnum::VOLUME),
            ..Default::default()
        });
        names.push(name);
    }
    Ok(ImageVolumes { names, mounts })
}

pub(crate) struct ImageVolumes {
    pub(crate) names: Vec<String>,
    pub(crate) mounts: Vec<Mount>,
}

fn volume_purpose(target: &str) -> String {
    let normalized = target
        .trim_matches('/')
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('-');
    if normalized.is_empty() {
        "root".to_owned()
    } else {
        normalized.to_owned()
    }
}
