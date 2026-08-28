use crate::{
    limits::{host_limits, set_writable_layer_limit},
    services, DockerRuntime,
};
use bollard::{
    container::{Config, CreateContainerOptions},
    models::{ImageInspect, Mount, MountTmpfsOptions, MountTypeEnum},
    network::CreateNetworkOptions,
};
use orchestrator_core::{OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, RuntimeError};
use std::{fs, io, path::Path};

const CONTAINER_WORKTREE: &str = "/workspace";
const CONTAINER_SESSION: &str = "/session";

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
    let image = requirement.image.as_deref().ok_or_else(|| {
        RuntimeError::Provisioning("runtime image is required for Docker provisioning".to_owned())
    })?;
    let image_inspect = runtime.ensure_image(image).await?;
    let mut service_images = Vec::with_capacity(service_requirements.len());
    for service in service_requirements {
        service_images.push(runtime.ensure_image(&service.image).await?);
    }
    let image_inspects = std::iter::once(&image_inspect)
        .chain(service_images.iter())
        .collect::<Vec<_>>();
    let disk_slot_bytes = execution_disk_slot_bytes(requirement, &image_inspects)?;
    let execution_mounts = execution_bind_mounts(runtime, labels)?;

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

    let service_containers = services::create_services(
        runtime,
        labels,
        requirement,
        service_requirements,
        &service_images,
        &network,
        disk_slot_bytes,
    )
    .await?;

    let agent_container = DockerRuntime::agent_container_name(&labels.execution_id);
    let mut limits = host_limits(requirement);
    limits.network_mode = Some(network.clone());
    let agent_mounts = bounded_image_mounts(&image_inspect, disk_slot_bytes);
    set_writable_layer_limit(&mut limits, agent_mounts.writable_layer_bytes);
    let has_bounded_mounts = !agent_mounts.mounts.is_empty();
    let mut mounts = agent_mounts.mounts;
    mounts.extend(execution_mounts);
    limits.mounts = Some(mounts);
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
            container_create_error(
                format!("create agent container {agent_container}"),
                error,
                has_bounded_mounts,
            )
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

fn execution_bind_mounts(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
) -> Result<Vec<Mount>, RuntimeError> {
    validate_execution_id(labels.execution_id.as_str())?;
    ensure_real_directory(&runtime.state_root, false, "state root")?;
    let worktrees_root = runtime.state_root.join("worktrees");
    ensure_real_directory(&worktrees_root, false, "worktrees root")?;
    let worktree = worktrees_root.join(labels.execution_id.as_str());
    ensure_real_directory(&worktree, false, "execution worktree")?;

    let sessions_root = runtime.state_root.join("sessions");
    ensure_real_directory(&sessions_root, false, "sessions root")?;
    let session_root = sessions_root.join(labels.execution_id.as_str());
    ensure_real_directory(&session_root, false, "execution session root")?;
    let conversation = session_root.join("conversation");
    ensure_real_directory(&conversation, true, "conversation directory")?;

    Ok(vec![
        bind_mount(&worktree, CONTAINER_WORKTREE)?,
        bind_mount(&conversation, CONTAINER_SESSION)?,
    ])
}

fn validate_execution_id(execution_id: &str) -> Result<(), RuntimeError> {
    let bytes = execution_id.as_bytes();
    let valid = (1..=63).contains(&bytes.len())
        && bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if valid {
        Ok(())
    } else {
        Err(RuntimeError::Provisioning(format!(
            "invalid execution_id for runtime paths: {execution_id}"
        )))
    }
}

fn ensure_real_directory(path: &Path, create: bool, purpose: &str) -> Result<(), RuntimeError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(RuntimeError::Provisioning(format!(
                "{purpose} is not a real directory: {}",
                path.display()
            )))
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            match fs::create_dir(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    ensure_real_directory(path, false, purpose)
                }
                Err(error) => Err(RuntimeError::Provisioning(format!(
                    "create {purpose} {}: {error}",
                    path.display()
                ))),
            }
        }
        Err(error) => Err(RuntimeError::Provisioning(format!(
            "inspect {purpose} {}: {error}",
            path.display()
        ))),
    }
}

fn bind_mount(source: &Path, target: &str) -> Result<Mount, RuntimeError> {
    let source = fs::canonicalize(source).map_err(|error| {
        RuntimeError::Provisioning(format!(
            "canonicalize bind source {}: {error}",
            source.display()
        ))
    })?;
    let source = source.into_os_string().into_string().map_err(|source| {
        RuntimeError::Provisioning(format!(
            "bind source is not UTF-8: {}",
            Path::new(&source).display()
        ))
    })?;
    Ok(Mount {
        target: Some(target.to_owned()),
        source: Some(source),
        typ: Some(MountTypeEnum::BIND),
        read_only: Some(false),
        ..Default::default()
    })
}

pub(crate) fn bounded_image_mounts(
    image: &ImageInspect,
    disk_slot_bytes: u64,
) -> BoundedImageMounts {
    let targets = image_volume_targets(image);
    let mounts = targets
        .into_iter()
        .map(|target| Mount {
            target: Some(target),
            source: None,
            typ: Some(MountTypeEnum::TMPFS),
            tmpfs_options: Some(MountTmpfsOptions {
                size_bytes: Some(disk_slot_bytes as i64),
                mode: Some(0o1777),
                ..Default::default()
            }),
            ..Default::default()
        })
        .collect();
    BoundedImageMounts {
        writable_layer_bytes: disk_slot_bytes,
        mounts,
    }
}

fn image_volume_targets(image: &ImageInspect) -> Vec<String> {
    let mut targets = image
        .config
        .as_ref()
        .and_then(|config| config.volumes.as_ref())
        .map(|volumes| volumes.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    targets.sort();
    targets
}

fn execution_disk_slot_bytes(
    requirement: &RuntimeRequirement,
    images: &[&ImageInspect],
) -> Result<u64, RuntimeError> {
    let total_bytes = requirement
        .disk_gib
        .checked_mul(1024 * 1024 * 1024)
        .ok_or_else(|| RuntimeError::ResourceLimit("disk budget overflows bytes".to_owned()))?;
    let slot_count = images
        .iter()
        .try_fold(images.len(), |count, image| {
            count.checked_add(image_volume_targets(image).len())
        })
        .ok_or_else(|| RuntimeError::ResourceLimit("too many image volume paths".to_owned()))?;
    let divisor = u64::try_from(slot_count)
        .map_err(|_| RuntimeError::ResourceLimit("too many image volume paths".to_owned()))?;
    let slot_bytes = total_bytes / divisor;
    if slot_bytes == 0 || slot_bytes > i64::MAX as u64 {
        return Err(RuntimeError::ResourceLimit(format!(
            "disk budget {}GiB cannot bound {slot_count} execution storage slots",
            requirement.disk_gib
        )));
    }
    Ok(slot_bytes)
}

pub(crate) struct BoundedImageMounts {
    pub(crate) writable_layer_bytes: u64,
    pub(crate) mounts: Vec<Mount>,
}

pub(crate) fn container_create_error(
    context: String,
    error: bollard::errors::Error,
    has_bounded_mounts: bool,
) -> RuntimeError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    if (has_bounded_mounts && (lower.contains("tmpfs") || lower.contains("mount")))
        || lower.contains("quota")
        || lower.contains("storage opt")
    {
        RuntimeError::ResourceLimit(format!("{context}: {message}"))
    } else {
        RuntimeError::Provisioning(format!("{context}: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::ImageConfig;
    use std::collections::HashMap;

    #[test]
    fn all_container_layers_and_tmpfs_mounts_share_one_execution_disk_budget() {
        let agent_image = ImageInspect {
            config: Some(ImageConfig {
                volumes: Some(HashMap::from([
                    ("/cache".to_owned(), HashMap::new()),
                    ("/data".to_owned(), HashMap::new()),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        };
        let service_image = ImageInspect::default();
        let requirement = RuntimeRequirement {
            disk_gib: 1,
            ..RuntimeRequirement::default()
        };

        let slot_bytes = execution_disk_slot_bytes(&requirement, &[&agent_image, &service_image])
            .expect("allocate disk budget");
        let allocation = bounded_image_mounts(&agent_image, slot_bytes);
        let mount_bytes = allocation
            .mounts
            .iter()
            .map(|mount| {
                mount
                    .tmpfs_options
                    .as_ref()
                    .and_then(|options| options.size_bytes)
                    .expect("every image path is bounded") as u64
            })
            .sum::<u64>();

        assert_eq!(allocation.mounts.len(), 2);
        assert_eq!(
            allocation.writable_layer_bytes + mount_bytes + slot_bytes,
            (1024 * 1024 * 1024 / 4) * 4
        );
        assert!(allocation.writable_layer_bytes + mount_bytes + slot_bytes <= 1024 * 1024 * 1024);
    }

    #[test]
    fn daemon_rejection_of_a_bounded_mount_is_a_resource_limit_error() {
        let error = container_create_error(
            "create service container".to_owned(),
            bollard::errors::Error::DockerResponseServerError {
                status_code: 400,
                message: "invalid mount config for type tmpfs".to_owned(),
            },
            true,
        );

        assert!(matches!(error, RuntimeError::ResourceLimit(message) if message.contains("tmpfs")));
    }

    #[test]
    fn runtime_paths_accept_only_safe_execution_id_components() {
        assert!(validate_execution_id("node-417-impl-01").is_ok());
        for invalid in [
            "",
            "../escape",
            "Uppercase",
            "contains_underscore",
            "a234567890123456789012345678901234567890123456789012345678901234",
        ] {
            assert!(
                validate_execution_id(invalid).is_err(),
                "accepted unsafe execution id {invalid}"
            );
        }
    }
}
