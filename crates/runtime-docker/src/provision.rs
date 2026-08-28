use crate::{limits::host_limits, services, DockerRuntime};
use bollard::{
    container::{
        AttachContainerOptions, Config, CreateContainerOptions, LogOutput, RemoveContainerOptions,
    },
    image::CreateImageOptions,
    models::{ImageInspect, Mount, MountPoint, MountPointTypeEnum, MountTypeEnum},
    network::CreateNetworkOptions,
};
use execution_storage::{disk_gib_to_bytes, ExecutionLayout, VerifiedExecutionStorage};
use futures_util::StreamExt;
use orchestrator_core::{OwnershipLabels, RuntimeRequirement, ServiceRequirement};
use runtime_traits::{EnvironmentHandle, RuntimeError};
use std::{collections::BTreeSet, fs, io, path::Path};

const CONTAINER_WORKTREE: &str = "/workspace";
const CONTAINER_SESSION: &str = "/session";

pub(crate) struct ReadyStorageGuard<'a> {
    runtime: &'a DockerRuntime,
    labels: &'a OwnershipLabels,
    requirement: &'a RuntimeRequirement,
    pinned: &'a dyn VerifiedExecutionStorage,
}

impl ReadyStorageGuard<'_> {
    pub(crate) async fn verify(&self, mounts: &[Mount]) -> Result<(), RuntimeError> {
        self.pinned.verify().map_err(storage_error)?;
        verify_mount_directories(self.pinned, mounts)?;
        let receipt = self.runtime.allocation.as_ref().ok_or_else(|| {
            RuntimeError::ResourceLimit(
                "Docker provisioning requires an exact Ready allocation receipt".to_owned(),
            )
        })?;
        if &receipt.labels != self.labels
            || receipt.reserved_bytes
                != disk_gib_to_bytes(self.requirement.disk_gib).map_err(storage_error)?
        {
            return Err(RuntimeError::ResourceLimit(
                "runtime request does not exactly match the Ready allocation".to_owned(),
            ));
        }
        let fresh = self
            .runtime
            .storage_verifier
            .as_ref()
            .ok_or_else(|| {
                RuntimeError::ResourceLimit(
                    "Docker provisioning requires a live Ready execution-storage verifier"
                        .to_owned(),
                )
            })?
            .verify_ready(receipt)
            .map_err(storage_error)?;
        fresh.verify().map_err(storage_error)?;
        verify_mount_directories(fresh.as_ref(), mounts)?;
        validate_storage_daemon(self.runtime).await?;
        validate_trusted_verifier(self.runtime).await
    }
}

pub(crate) async fn provision(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    service_requirements: &[ServiceRequirement],
) -> Result<EnvironmentHandle, RuntimeError> {
    let (verified, layout) = verified_execution_layout(runtime, labels, requirement)?;
    runtime.require_compatible_daemon().await?;
    let storage = ReadyStorageGuard {
        runtime,
        labels,
        requirement,
        pinned: verified.as_ref(),
    };
    storage.verify(&[]).await?;
    match provision_inner(
        runtime,
        labels,
        requirement,
        service_requirements,
        &storage,
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

async fn validate_trusted_verifier(runtime: &DockerRuntime) -> Result<(), RuntimeError> {
    let trusted = runtime.trusted_verifier.as_ref().ok_or_else(|| {
        RuntimeError::ResourceLimit(
            "Docker provisioning requires an immutable trusted verifier image".to_owned(),
        )
    })?;
    let receipt = runtime.allocation.as_ref().ok_or_else(|| {
        RuntimeError::ResourceLimit(
            "Docker provisioning requires an exact Ready allocation receipt".to_owned(),
        )
    })?;
    if receipt.docker_bind.method_version != trusted.proof_method() {
        return Err(RuntimeError::ResourceLimit(
            "trusted verifier image does not match the allocation Docker proof method".to_owned(),
        ));
    }
    let inspect = runtime
        .client
        .inspect_image(&trusted.image_id)
        .await
        .map_err(|error| {
            RuntimeError::ResourceLimit(format!("inspect trusted verifier image: {error}"))
        })?;
    if inspect.id.as_deref() != Some(trusted.image_id.as_str()) {
        return Err(RuntimeError::ResourceLimit(
            "trusted verifier image identity drifted".to_owned(),
        ));
    }
    Ok(())
}

fn verify_mount_directories(
    verified: &dyn VerifiedExecutionStorage,
    mounts: &[Mount],
) -> Result<(), RuntimeError> {
    let sources = mounts
        .iter()
        .filter_map(|mount| mount.source.as_deref())
        .collect::<BTreeSet<_>>();
    for source in sources {
        verified
            .verify_directory(Path::new(source))
            .map_err(storage_error)?;
    }
    Ok(())
}

async fn ensure_image(
    runtime: &DockerRuntime,
    storage: &ReadyStorageGuard<'_>,
    image: &str,
) -> Result<ImageInspect, RuntimeError> {
    match runtime.client.inspect_image(image).await {
        Ok(inspect) => return Ok(inspect),
        Err(error) if crate::is_image_not_found(&error) => {}
        Err(error) => {
            return Err(RuntimeError::Provisioning(format!(
                "inspect image {image}: {error}"
            )))
        }
    }
    storage.verify(&[]).await?;
    let mut pull = runtime.client.create_image(
        Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        }),
        None,
        None,
    );
    while let Some(progress) = pull.next().await {
        progress
            .map_err(|error| RuntimeError::Provisioning(format!("pull image {image}: {error}")))?;
    }
    runtime.client.inspect_image(image).await.map_err(|error| {
        RuntimeError::Provisioning(format!("image {image} unavailable after pull: {error}"))
    })
}

async fn provision_inner(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    service_requirements: &[ServiceRequirement],
    storage: &ReadyStorageGuard<'_>,
    layout: &ExecutionLayout,
) -> Result<EnvironmentHandle, RuntimeError> {
    let image = requirement.image.as_deref().ok_or_else(|| {
        RuntimeError::Provisioning("runtime image is required for Docker provisioning".to_owned())
    })?;
    let image_inspect = ensure_image(runtime, storage, image).await?;
    let mut service_images = Vec::with_capacity(service_requirements.len());
    for service in service_requirements {
        service_images.push(ensure_image(runtime, storage, &service.image).await?);
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
    let all_mounts = agent_mounts
        .iter()
        .chain(execution_mounts.iter())
        .chain(service_mounts.iter().flatten())
        .cloned()
        .collect::<Vec<_>>();

    let network = DockerRuntime::network_name(&labels.execution_id);
    storage.verify(&all_mounts).await?;
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
        (&layout.root, storage, &all_mounts),
    )
    .await?;

    let agent_container = DockerRuntime::agent_container_name(&labels.execution_id);
    let mut limits = host_limits(requirement);
    limits.network_mode = Some(network.clone());
    let mut mounts = agent_mounts;
    mounts.extend(execution_mounts);
    limits.mounts = Some(mounts.clone());
    storage.verify(&all_mounts).await?;
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
    storage.verify(&all_mounts).await?;
    verify_container_mount_sources(runtime, &agent_container, &layout.root, &mounts).await?;

    verify_with_trusted_container(
        runtime,
        labels,
        requirement,
        storage,
        &layout.root,
        &all_mounts,
    )
    .await?;
    services::start_services(runtime, &service_containers, storage, &all_mounts).await?;
    storage.verify(&all_mounts).await?;
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

fn validate_actual_mounts(requested: &[Mount], actual: &[MountPoint]) -> Result<(), RuntimeError> {
    let requested = requested
        .iter()
        .map(|mount| {
            let source = mount.source.as_deref().ok_or_else(|| {
                RuntimeError::ResourceLimit("requested bind lacks source".to_owned())
            })?;
            let source = fs::canonicalize(source).map_err(|error| {
                RuntimeError::ResourceLimit(format!("canonicalize requested bind: {error}"))
            })?;
            let target = mount.target.clone().ok_or_else(|| {
                RuntimeError::ResourceLimit("requested bind lacks target".to_owned())
            })?;
            if mount.typ != Some(MountTypeEnum::BIND) {
                return Err(RuntimeError::ResourceLimit(
                    "requested mount is not a bind".to_owned(),
                ));
            }
            Ok((source, target, mount.read_only == Some(true)))
        })
        .collect::<Result<BTreeSet<_>, RuntimeError>>()?;
    let actual = actual
        .iter()
        .map(|mount| {
            if mount.typ != Some(MountPointTypeEnum::BIND)
                || mount.name.is_some()
                || mount.rw.is_none()
            {
                return Err(RuntimeError::ResourceLimit(
                    "daemon reported an unexpected or anonymous mount".to_owned(),
                ));
            }
            let source = mount.source.as_deref().ok_or_else(|| {
                RuntimeError::ResourceLimit("actual bind lacks source".to_owned())
            })?;
            let source = fs::canonicalize(source).map_err(|error| {
                RuntimeError::ResourceLimit(format!("canonicalize actual bind: {error}"))
            })?;
            let target = mount.destination.clone().ok_or_else(|| {
                RuntimeError::ResourceLimit("actual bind lacks target".to_owned())
            })?;
            Ok((source, target, mount.rw == Some(false)))
        })
        .collect::<Result<BTreeSet<_>, RuntimeError>>()?;
    if actual != requested {
        return Err(RuntimeError::ResourceLimit(
            "daemon mounts do not exactly match requested binds".to_owned(),
        ));
    }
    Ok(())
}

async fn verify_with_trusted_container(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    storage: &ReadyStorageGuard<'_>,
    execution_root: &Path,
    workload_mounts: &[Mount],
) -> Result<(), RuntimeError> {
    let trusted = runtime.trusted_verifier.as_ref().ok_or_else(|| {
        RuntimeError::ResourceLimit(
            "Docker provisioning requires an immutable trusted verifier image".to_owned(),
        )
    })?;
    let canonical_root = fs::canonicalize(execution_root).map_err(|error| {
        RuntimeError::ResourceLimit(format!("canonicalize execution root: {error}"))
    })?;
    let mut sources = vec![canonical_root.clone()];
    for source in workload_mounts
        .iter()
        .filter_map(|mount| mount.source.as_deref())
        .map(Path::new)
        .map(fs::canonicalize)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|error| RuntimeError::ResourceLimit(format!("canonicalize proof bind: {error}")))?
    {
        if source != canonical_root {
            sources.push(source);
        }
    }
    let proof_mounts = sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let mut mount = bind_mount(source, &format!("/autospec-proof/{index}"))?;
            mount.read_only = Some(true);
            Ok(mount)
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    let proof_targets = (0..sources.len())
        .map(|index| format!("/autospec-proof/{index}"))
        .collect::<Vec<_>>();
    let name = format!("autospec-{}-mount-verifier", labels.execution_id);
    let mut limits = host_limits(requirement);
    limits.network_mode = Some("none".to_owned());
    limits.mounts = Some(proof_mounts.clone());
    let mut command = vec!["-c".to_owned(), "%d:%i".to_owned()];
    command.extend(proof_targets);

    storage.verify(workload_mounts).await?;
    runtime
        .client
        .create_container(
            Some(CreateContainerOptions {
                name: name.clone(),
                platform: None,
            }),
            Config {
                image: Some(trusted.image_id.clone()),
                entrypoint: Some(vec![trusted.stat_command.clone()]),
                cmd: Some(command),
                labels: Some(labels.to_map().into_iter().collect()),
                host_config: Some(limits),
                network_disabled: Some(true),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| {
            RuntimeError::ResourceLimit(format!("create trusted mount verifier: {error}"))
        })?;

    let verification = async {
        verify_container_mount_sources(runtime, &name, execution_root, &proof_mounts).await?;
        let mut attached = runtime
            .client
            .attach_container(
                &name,
                Some(AttachContainerOptions::<String> {
                    stdout: Some(true),
                    stderr: Some(true),
                    stream: Some(true),
                    logs: Some(false),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|error| {
                RuntimeError::ResourceLimit(format!("attach trusted mount verifier: {error}"))
            })?;
        storage.verify(workload_mounts).await?;
        runtime
            .client
            .start_container::<String>(&name, None)
            .await
            .map_err(|error| {
                RuntimeError::ResourceLimit(format!("start trusted mount verifier: {error}"))
            })?;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        while let Some(output) = attached.output.next().await {
            match output.map_err(|error| {
                RuntimeError::ResourceLimit(format!("read trusted mount verifier: {error}"))
            })? {
                LogOutput::StdOut { message } | LogOutput::Console { message } => {
                    stdout.extend_from_slice(&message)
                }
                LogOutput::StdErr { message } => stderr.extend_from_slice(&message),
                _ => {}
            }
        }
        let inspect = runtime
            .client
            .inspect_container(&name, None)
            .await
            .map_err(|error| {
                RuntimeError::ResourceLimit(format!("inspect trusted mount verifier: {error}"))
            })?;
        if inspect.state.and_then(|state| state.exit_code) != Some(0) {
            return Err(RuntimeError::ResourceLimit(format!(
                "trusted mount verifier failed: {}",
                String::from_utf8_lossy(&stderr).trim()
            )));
        }
        let identities = String::from_utf8(stdout)
            .map_err(|error| {
                RuntimeError::ResourceLimit(format!(
                    "trusted verifier output is not UTF-8: {error}"
                ))
            })?
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if identities.len() != sources.len() {
            return Err(RuntimeError::ResourceLimit(
                "trusted verifier did not return one identity per bind source".to_owned(),
            ));
        }
        let expected = &runtime
            .allocation
            .as_ref()
            .ok_or_else(|| {
                RuntimeError::ResourceLimit(
                    "trusted verifier requires a Ready allocation receipt".to_owned(),
                )
            })?
            .docker_bind
            .filesystem_id;
        if identities.first() != Some(expected) {
            return Err(RuntimeError::ResourceLimit(
                "trusted verifier execution-root identity differs from the Ready proof".to_owned(),
            ));
        }
        let expected_device = expected.split(':').next().unwrap_or_default();
        if identities
            .iter()
            .any(|identity| identity.split(':').next().unwrap_or_default() != expected_device)
        {
            return Err(RuntimeError::ResourceLimit(
                "trusted verifier found a bind source on another filesystem".to_owned(),
            ));
        }
        Ok(())
    }
    .await;

    let ready_before_removal = storage.verify(workload_mounts).await;
    let removal = runtime
        .client
        .remove_container(
            &name,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await
        .map_err(|error| {
            RuntimeError::ResourceLimit(format!("remove trusted mount verifier: {error}"))
        });
    match (verification, ready_before_removal, removal) {
        (Ok(()), Ok(()), Ok(())) => Ok(()),
        (result, ready, removal) => Err(RuntimeError::ResourceLimit(format!(
            "trusted mount verification failed: {}; readiness before cleanup: {}; cleanup: {}",
            result
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "ok".to_owned()),
            ready
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "ok".to_owned()),
            removal
                .err()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "ok".to_owned()),
        ))),
    }
}

pub(crate) async fn verify_container_mount_sources(
    runtime: &DockerRuntime,
    container: &str,
    execution_root: &Path,
    requested: &[Mount],
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
    validate_actual_mounts(requested, inspect.mounts.as_deref().unwrap_or_default())?;
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
        || host.ipc_mode.as_deref() != Some("none")
        || host.shm_size != Some(crate::limits::DEFAULT_SHM_SIZE)
    {
        return Err(RuntimeError::ResourceLimit(format!(
            "container {container} does not use readonly rootfs, log none, and bind-only storage"
        )));
    }
    let mounts = host.mounts.ok_or_else(|| {
        RuntimeError::ResourceLimit(format!("container {container} has no writable bind mounts"))
    })?;
    for mount in mounts {
        if mount.typ != Some(MountTypeEnum::BIND) {
            return Err(RuntimeError::ResourceLimit(format!(
                "container {container} has a non-bind mount"
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
        if !canonical.starts_with(&root) {
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
    use bollard::models::{ImageConfig, MountPoint, MountPointTypeEnum};
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

    #[test]
    fn actual_mounts_must_exactly_equal_requested_binds() {
        let state = tempfile::tempdir().expect("state");
        let source = state.path().canonicalize().expect("canonical source");
        let requested = vec![bind_mount(&source, "/workspace").expect("requested bind")];
        let exact = vec![MountPoint {
            typ: Some(MountPointTypeEnum::BIND),
            source: Some(source.display().to_string()),
            destination: Some("/workspace".to_owned()),
            rw: Some(true),
            ..Default::default()
        }];
        assert!(validate_actual_mounts(&requested, &exact).is_ok());

        let mut anonymous = exact.clone();
        anonymous.push(MountPoint {
            typ: Some(MountPointTypeEnum::VOLUME),
            name: Some("anonymous".to_owned()),
            destination: Some("/image-volume".to_owned()),
            rw: Some(true),
            ..Default::default()
        });
        assert!(validate_actual_mounts(&requested, &anonymous).is_err());

        let mut wrong_source = exact;
        wrong_source[0].source = Some(state.path().join("other").display().to_string());
        assert!(validate_actual_mounts(&requested, &wrong_source).is_err());
    }
}
