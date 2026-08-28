use crate::{limits::host_limits, services, DockerRuntime};
use bollard::{
    container::{Config, CreateContainerOptions, LogOutput},
    exec::{CreateExecOptions, StartExecResults},
    models::{ImageInspect, Mount, MountTypeEnum},
    network::CreateNetworkOptions,
};
use execution_storage::{disk_gib_to_bytes, ExecutionLayout, VerifiedExecutionStorage};
use futures_util::StreamExt;
use orchestrator_core::{OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, RuntimeError};
use std::{collections::BTreeSet, fs, io, path::Path};

const CONTAINER_WORKTREE: &str = "/workspace";
const CONTAINER_SESSION: &str = "/session";

pub(crate) async fn provision(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    service_requirements: &[ServiceRequirement],
) -> Result<EnvironmentHandle, RuntimeError> {
    let (verified, layout) = verified_execution_layout(runtime, labels, requirement)?;
    runtime.require_compatible_daemon().await?;
    validate_storage_daemon(runtime).await?;
    match provision_inner(
        runtime,
        labels,
        requirement,
        service_requirements,
        verified.as_ref(),
        &layout,
    )
    .await
    {
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

async fn validate_storage_daemon(runtime: &DockerRuntime) -> Result<(), RuntimeError> {
    let expected = &runtime
        .allocation
        .as_ref()
        .ok_or_else(|| {
            RuntimeError::ResourceLimit(
                "Docker provisioning requires an exact Ready allocation receipt".to_owned(),
            )
        })?
        .docker_bind
        .daemon_id;
    let actual = runtime
        .client
        .info()
        .await
        .map_err(|error| {
            RuntimeError::Unavailable(format!("inspect Docker daemon identity: {error}"))
        })?
        .id
        .ok_or_else(|| {
            RuntimeError::ResourceLimit("Docker daemon did not report an identity".to_owned())
        })?;
    if &actual != expected {
        return Err(RuntimeError::ResourceLimit(format!(
            "Docker daemon identity {actual} does not match the Ready storage proof"
        )));
    }
    Ok(())
}

async fn provision_inner(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    service_requirements: &[ServiceRequirement],
    verified: &dyn VerifiedExecutionStorage,
    layout: &ExecutionLayout,
) -> Result<EnvironmentHandle, RuntimeError> {
    verified.verify().map_err(storage_error)?;
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
    validate_reserved_image_volumes(&image_inspects)?;
    let execution_mounts = execution_bind_mounts(layout)?;
    let agent_mounts = writable_container_mounts(&layout.runtime, "agent", &image_inspect)?;
    let service_mounts = service_requirements
        .iter()
        .zip(&service_images)
        .map(|(service, image)| {
            writable_container_mounts(&layout.runtime, &format!("service-{}", service.name), image)
        })
        .collect::<Result<Vec<_>, _>>()?;
    verified.verify().map_err(storage_error)?;

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
        &network,
        &service_mounts,
        (&layout.root, verified),
    )
    .await?;

    let agent_container = DockerRuntime::agent_container_name(&labels.execution_id);
    let mut limits = host_limits(requirement);
    limits.network_mode = Some(network.clone());
    let mut mounts = agent_mounts;
    mounts.extend(execution_mounts);
    limits.mounts = Some(mounts);
    verified.verify().map_err(storage_error)?;
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
                env: Some(vec![
                    "HOME=/home/autospec".to_owned(),
                    "TMPDIR=/tmp".to_owned(),
                ]),
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
                true,
            )
        })?;
    verified.verify().map_err(storage_error)?;
    verify_container_mount_sources(runtime, &agent_container, &layout.root).await?;
    runtime
        .client
        .start_container::<String>(&agent_container, None)
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!("start agent container {agent_container}: {error}"))
        })?;
    verified.verify().map_err(storage_error)?;
    verify_container_mount_devices(runtime, &agent_container).await?;

    Ok(EnvironmentHandle {
        execution_id: labels.execution_id.clone(),
        network,
        agent_container,
        service_containers,
        volumes: Vec::new(),
        credentials_path: None,
    })
}

fn execution_bind_mounts(layout: &ExecutionLayout) -> Result<Vec<Mount>, RuntimeError> {
    Ok(vec![
        bind_mount(&layout.repository, CONTAINER_WORKTREE)?,
        bind_mount(&layout.conversation, CONTAINER_SESSION)?,
    ])
}

fn verified_execution_layout(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
) -> Result<(Box<dyn VerifiedExecutionStorage>, ExecutionLayout), RuntimeError> {
    let verifier = runtime.storage_verifier.as_ref().ok_or_else(|| {
        RuntimeError::ResourceLimit(
            "Docker provisioning requires a live Ready execution-storage verifier".to_owned(),
        )
    })?;
    let receipt = runtime.allocation.as_ref().ok_or_else(|| {
        RuntimeError::ResourceLimit(
            "Docker provisioning requires an exact Ready allocation receipt".to_owned(),
        )
    })?;
    if &receipt.labels != labels
        || receipt.reserved_bytes
            != disk_gib_to_bytes(requirement.disk_gib).map_err(storage_error)?
    {
        return Err(RuntimeError::ResourceLimit(
            "runtime request does not exactly match the Ready allocation".to_owned(),
        ));
    }
    let verified = verifier.verify_ready(receipt).map_err(storage_error)?;
    verified.verify().map_err(storage_error)?;
    let layout =
        ExecutionLayout::new(&runtime.state_root, &labels.execution_id).map_err(storage_error)?;
    if layout.root != receipt.mount_path || verified.repository_path() != layout.repository {
        return Err(RuntimeError::ResourceLimit(
            "verified storage paths do not match the exact execution layout".to_owned(),
        ));
    }
    for (path, purpose) in [
        (&layout.root, "execution root"),
        (&layout.repository, "execution repository"),
        (&layout.conversation, "execution conversation"),
        (&layout.runtime, "execution runtime"),
    ] {
        ensure_real_directory(path, false, purpose)?;
    }
    Ok((verified, layout))
}

fn storage_error(error: execution_storage::StorageError) -> RuntimeError {
    RuntimeError::ResourceLimit(format!("execution storage verification failed: {error}"))
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

pub(crate) async fn verify_container_mount_sources(
    runtime: &DockerRuntime,
    container: &str,
    execution_root: &Path,
) -> Result<(), RuntimeError> {
    let root = fs::canonicalize(execution_root).map_err(|error| {
        RuntimeError::ResourceLimit(format!("canonicalize execution root: {error}"))
    })?;
    #[cfg(unix)]
    let root_device = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(&root)
            .map_err(|error| {
                RuntimeError::ResourceLimit(format!("inspect execution root: {error}"))
            })?
            .dev()
    };
    let inspect = runtime
        .client
        .inspect_container(container, None)
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!("inspect container {container}: {error}"))
        })?;
    let host = inspect.host_config.ok_or_else(|| {
        RuntimeError::ResourceLimit(format!("container {container} lacks host configuration"))
    })?;
    if host.readonly_rootfs != Some(true)
        || host
            .log_config
            .as_ref()
            .and_then(|config| config.typ.as_deref())
            != Some("none")
        || host
            .storage_opt
            .as_ref()
            .is_some_and(|options| !options.is_empty())
        || host.tmpfs.as_ref().is_some_and(|mounts| !mounts.is_empty())
    {
        return Err(RuntimeError::ResourceLimit(format!(
            "container {container} does not use readonly rootfs, log none, and bind-only storage"
        )));
    }
    let mounts = host.mounts.ok_or_else(|| {
        RuntimeError::ResourceLimit(format!("container {container} has no writable bind mounts"))
    })?;
    for mount in mounts {
        if mount.typ != Some(MountTypeEnum::BIND) || mount.read_only == Some(true) {
            return Err(RuntimeError::ResourceLimit(format!(
                "container {container} has a non-bind or read-only writable-path mount"
            )));
        }
        let target = mount.target.unwrap_or_default();
        if target == "/var/run/docker.sock" || target == "/run/docker.sock" {
            return Err(RuntimeError::ResourceLimit(
                "Docker socket mount is forbidden".to_owned(),
            ));
        }
        let source = mount.source.ok_or_else(|| {
            RuntimeError::ResourceLimit(format!("container {container} bind lacks source"))
        })?;
        let canonical = fs::canonicalize(&source).map_err(|error| {
            RuntimeError::ResourceLimit(format!(
                "canonicalize daemon bind source {source}: {error}"
            ))
        })?;
        if !canonical.starts_with(&root) || canonical == root {
            return Err(RuntimeError::ResourceLimit(format!(
                "daemon bind source {} escapes execution root {}",
                canonical.display(),
                root.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if fs::metadata(&canonical)
                .map_err(|error| {
                    RuntimeError::ResourceLimit(format!("inspect bind source: {error}"))
                })?
                .dev()
                != root_device
            {
                return Err(RuntimeError::ResourceLimit(format!(
                    "daemon bind source {} is on a different execution filesystem",
                    canonical.display()
                )));
            }
        }
    }
    Ok(())
}

pub(crate) async fn verify_container_mount_devices(
    runtime: &DockerRuntime,
    container: &str,
) -> Result<(), RuntimeError> {
    let inspect = runtime
        .client
        .inspect_container(container, None)
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!("inspect running container {container}: {error}"))
        })?;
    let targets = inspect
        .mounts
        .unwrap_or_default()
        .into_iter()
        .filter(|mount| mount.rw == Some(true))
        .filter_map(|mount| mount.destination)
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(RuntimeError::ResourceLimit(format!(
            "container {container} has no daemon-inspected RW mounts"
        )));
    }
    let mut command = vec!["stat".to_owned(), "-c".to_owned(), "%d".to_owned()];
    command.extend(targets.iter().cloned());
    let exec = runtime
        .client
        .create_exec(
            container,
            CreateExecOptions {
                cmd: Some(command),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| {
            RuntimeError::ResourceLimit(format!("create daemon-side mount proof: {error}"))
        })?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    match runtime
        .client
        .start_exec(&exec.id, None)
        .await
        .map_err(|error| {
            RuntimeError::ResourceLimit(format!("start daemon-side mount proof: {error}"))
        })? {
        StartExecResults::Attached { mut output, .. } => {
            while let Some(item) = output.next().await {
                match item.map_err(|error| {
                    RuntimeError::ResourceLimit(format!("read daemon-side mount proof: {error}"))
                })? {
                    LogOutput::StdOut { message } | LogOutput::Console { message } => {
                        stdout.extend_from_slice(&message)
                    }
                    LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                    _ => {}
                }
            }
        }
        StartExecResults::Detached => {
            return Err(RuntimeError::ResourceLimit(
                "daemon-side mount proof detached".to_owned(),
            ))
        }
    }
    let result = runtime
        .client
        .inspect_exec(&exec.id)
        .await
        .map_err(|error| {
            RuntimeError::ResourceLimit(format!("inspect daemon-side mount proof: {error}"))
        })?;
    if result.exit_code != Some(0) {
        return Err(RuntimeError::ResourceLimit(format!(
            "daemon-side mount proof failed: {}",
            String::from_utf8_lossy(&stderr).trim()
        )));
    }
    let output = String::from_utf8(stdout).map_err(|error| {
        RuntimeError::ResourceLimit(format!("daemon-side device proof is not UTF-8: {error}"))
    })?;
    let devices = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<BTreeSet<_>>();
    if devices.len() != 1 {
        return Err(RuntimeError::ResourceLimit(format!(
            "RW mounts do not share one daemon-observed device: {devices:?}"
        )));
    }
    Ok(())
}

pub(crate) fn writable_container_mounts(
    runtime_root: &Path,
    scope: &str,
    image: &ImageInspect,
) -> Result<Vec<Mount>, RuntimeError> {
    validate_safe_component(scope, "container storage scope")?;
    ensure_real_directory(runtime_root, false, "execution runtime root")?;
    let scope_root = runtime_root.join(scope);
    ensure_real_directory(&scope_root, true, "container storage root")?;
    let fixed = [
        ("home", "/home/autospec"),
        ("tmp", "/tmp"),
        ("var-tmp", "/var/tmp"),
        ("run", "/run"),
    ];
    let mut mounts = Vec::new();
    let mut targets = BTreeSet::new();
    for (purpose, target) in fixed {
        let source = scope_root.join(purpose);
        ensure_real_directory(&source, true, "container writable directory")?;
        make_container_writable(&source)?;
        mounts.push(bind_mount(&source, target)?);
        targets.insert(target.to_owned());
    }
    let image_root = scope_root.join("image-volumes");
    ensure_real_directory(&image_root, true, "image volume root")?;
    for target in image_volume_targets(image) {
        if !targets.insert(target.clone()) {
            continue;
        }
        let source = image_root.join(hex_component(target.as_bytes()));
        ensure_real_directory(&source, true, "image volume directory")?;
        make_container_writable(&source)?;
        mounts.push(bind_mount(&source, &target)?);
    }
    Ok(mounts)
}

#[cfg(unix)]
fn make_container_writable(path: &Path) -> Result<(), RuntimeError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o1777)).map_err(|error| {
        RuntimeError::Provisioning(format!(
            "make container bind directory writable {}: {error}",
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn make_container_writable(_path: &Path) -> Result<(), RuntimeError> {
    Err(RuntimeError::ResourceLimit(
        "storage-backed writable container binds require Unix permissions".to_owned(),
    ))
}

fn validate_safe_component(value: &str, purpose: &str) -> Result<(), RuntimeError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(RuntimeError::Provisioning(format!(
            "invalid {purpose}: {value}"
        )))
    }
}

fn hex_component(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
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

fn validate_reserved_image_volumes(images: &[&ImageInspect]) -> Result<(), RuntimeError> {
    for target in images.iter().flat_map(|image| image_volume_targets(image)) {
        let mut components = Vec::new();
        if !target.starts_with('/') {
            return Err(RuntimeError::ResourceLimit(format!(
                "image volume target is not an absolute container path: {target}"
            )));
        }
        for component in target.split('/') {
            match component {
                "" | "." => {}
                ".." => {
                    if components.pop().is_none() {
                        return Err(RuntimeError::ResourceLimit(format!(
                            "image volume target escapes the container root: {target}"
                        )));
                    }
                }
                component => components.push(component),
            }
        }
        let shadows_reserved_mount =
            components.is_empty() || matches!(components.first(), Some(&"workspace" | &"session"));
        if shadows_reserved_mount {
            return Err(RuntimeError::ResourceLimit(format!(
                "image volume target collides with reserved agent mount: {target}"
            )));
        }
    }
    Ok(())
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
    fn writable_image_paths_are_bind_mapped_beneath_execution_runtime() {
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
        let state = tempfile::tempdir().expect("state");
        let runtime_root = state.path().join("runtime");
        std::fs::create_dir(&runtime_root).expect("runtime root");
        let runtime_root = std::fs::canonicalize(runtime_root).expect("canonical runtime root");
        let mounts = writable_container_mounts(&runtime_root, "agent", &agent_image)
            .expect("build writable binds");

        assert!(mounts
            .iter()
            .all(|mount| mount.typ == Some(MountTypeEnum::BIND)));
        assert!(mounts.iter().all(|mount| mount.tmpfs_options.is_none()));
        assert!(mounts.iter().all(|mount| {
            Path::new(mount.source.as_deref().expect("source")).starts_with(&runtime_root)
        }));
        let targets = mounts
            .iter()
            .filter_map(|mount| mount.target.as_deref())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(targets.contains("/home/autospec"));
        assert!(targets.contains("/tmp"));
        assert!(targets.contains("/var/tmp"));
        assert!(targets.contains("/run"));
        assert!(targets.contains("/cache"));
        assert!(targets.contains("/data"));
    }

    #[test]
    fn daemon_rejection_of_an_execution_bind_is_a_resource_limit_error() {
        let error = container_create_error(
            "create service container".to_owned(),
            bollard::errors::Error::DockerResponseServerError {
                status_code: 400,
                message: "invalid mount config for type bind".to_owned(),
            },
            true,
        );

        assert!(matches!(error, RuntimeError::ResourceLimit(message) if message.contains("bind")));
    }

    #[test]
    fn reserved_agent_mounts_reject_equal_descendant_and_shadowing_image_volumes() {
        for target in [
            "/workspace",
            "/workspace/cache",
            "/session",
            "/session/history",
            "/",
            "/workspace/../session",
        ] {
            let image = ImageInspect {
                config: Some(ImageConfig {
                    volumes: Some(HashMap::from([(target.to_owned(), HashMap::new())])),
                    ..Default::default()
                }),
                ..Default::default()
            };

            assert!(
                validate_reserved_image_volumes(&[&image]).is_err(),
                "accepted image volume that collides with an agent bind: {target}"
            );
        }
    }

    #[test]
    fn image_volumes_outside_reserved_agent_mounts_remain_supported() {
        let image = ImageInspect {
            config: Some(ImageConfig {
                volumes: Some(HashMap::from([
                    ("/cache".to_owned(), HashMap::new()),
                    ("/workspace-cache".to_owned(), HashMap::new()),
                    ("/sessions".to_owned(), HashMap::new()),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(validate_reserved_image_volumes(&[&image]).is_ok());
    }
}
