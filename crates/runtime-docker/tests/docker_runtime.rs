use bollard::{
    container::{Config, CreateContainerOptions, RemoveContainerOptions},
    exec::{CreateExecOptions, StartExecOptions},
    models::{HostConfig, Mount, MountTypeEnum},
    network::CreateNetworkOptions,
    volume::{CreateVolumeOptions, RemoveVolumeOptions},
    Docker,
};
use orchestrator_core::{
    labels, ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement, WorkerId,
};
use runtime_docker::{host_limits, DockerRuntime, DEFAULT_PIDS_LIMIT};
use runtime_traits::Runtime;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    time::{SystemTime, UNIX_EPOCH},
};

async fn volume_names(docker: &Docker) -> BTreeSet<String> {
    docker
        .list_volumes::<String>(None)
        .await
        .expect("list Docker volumes")
        .volumes
        .unwrap_or_default()
        .into_iter()
        .map(|volume| volume.name)
        .collect()
}

fn runtime_requirement() -> RuntimeRequirement {
    RuntimeRequirement {
        image: Some("alpine:3.20".to_owned()),
        cpu: 2,
        memory_mib: 384,
        disk_gib: 3,
        ..RuntimeRequirement::default()
    }
}

fn labels_for(execution_id: ExecutionId) -> OwnershipLabels {
    OwnershipLabels {
        execution_id,
        worker_id: WorkerId::new("docker-test-worker"),
        repository: "InferWeave/autospec-orchestrator".to_owned(),
        issue: Some("task-2".to_owned()),
    }
}

fn unique_execution_id() -> ExecutionId {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after Unix epoch")
        .as_nanos();
    ExecutionId::new(format!("runtime-docker-{nonce}"))
}

fn raw_client() -> Result<Docker, bollard::errors::Error> {
    match env::var("AUTOSPEC_DOCKER_SOCKET") {
        Ok(socket) => Docker::connect_with_socket(&socket, 120, bollard::API_DEFAULT_VERSION),
        Err(_) => Docker::connect_with_local_defaults(),
    }
}

async fn runtime_or_skip(test_name: &str) -> Option<DockerRuntime> {
    let runtime = match DockerRuntime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            println!("SKIP {test_name}: Docker daemon unavailable: {error}");
            return None;
        }
    };
    let daemon =
        raw_client().expect("runtime connection and raw test connection use the same socket");
    if let Err(error) = daemon.version().await {
        println!("SKIP {test_name}: Docker daemon unavailable: {error}");
        return None;
    }
    assert!(
        runtime.available().await,
        "Docker daemon is present but below the runtime's required API version"
    );
    Some(runtime)
}

#[test]
fn host_limits_enforce_cpu_memory_pid_and_disk_quotas() {
    let limits = host_limits(&runtime_requirement());

    assert_eq!(limits.nano_cpus, Some(2_000_000_000));
    assert_eq!(limits.memory, Some(384 * 1024 * 1024));
    assert_eq!(limits.memory_swap, Some(384 * 1024 * 1024));
    assert_eq!(limits.pids_limit, Some(DEFAULT_PIDS_LIMIT));
    assert_eq!(
        limits
            .storage_opt
            .as_ref()
            .and_then(|opts| opts.get("size")),
        Some(&"3G".to_owned())
    );
    assert_eq!(limits.publish_all_ports, Some(false));
    assert_eq!(limits.privileged, Some(false));
}

#[tokio::test]
async fn daemon_probe_reports_a_compatible_real_daemon() {
    let Some(runtime) = runtime_or_skip("daemon_probe_reports_a_compatible_real_daemon").await
    else {
        return;
    };

    assert_eq!(runtime.name(), "docker");
    let daemon_version = raw_client()
        .expect("connect to probed daemon")
        .version()
        .await
        .expect("read daemon version")
        .api_version
        .expect("daemon reports API version");
    let mut expected = daemon_version
        .split_once('.')
        .map(|(major, minor)| {
            (
                major.parse::<usize>().expect("numeric daemon major"),
                minor.parse::<usize>().expect("numeric daemon minor"),
            )
        })
        .expect("daemon API is major.minor");
    expected = expected.min((
        bollard::API_DEFAULT_VERSION.major_version,
        bollard::API_DEFAULT_VERSION.minor_version,
    ));
    assert_eq!(runtime.client_api_version(), expected);
}

#[tokio::test]
async fn provision_reconcile_and_destroy_preserve_execution_isolation() {
    let Some(runtime) =
        runtime_or_skip("provision_reconcile_and_destroy_preserve_execution_isolation").await
    else {
        return;
    };
    let docker = raw_client().expect("the already-probed Docker daemon remains connectable");
    let execution_labels = labels_for(unique_execution_id());
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::from([("SERVICE_MODE".to_owned(), "test".to_owned())]),
    };
    let unrelated_network = format!("unrelated-{}", execution_labels.execution_id);
    let unrelated_container = format!("unrelated-{}", execution_labels.execution_id);
    let unrelated_volume = format!("unrelated-{}", execution_labels.execution_id);
    docker
        .create_network(CreateNetworkOptions {
            name: unrelated_network.clone(),
            driver: "bridge".to_owned(),
            ..Default::default()
        })
        .await
        .expect("create unrelated control network");

    let result = async {
        let volumes_before_provision = volume_names(&docker).await;
        let handle = runtime
            .provision(&execution_labels, &runtime_requirement(), &[service])
            .await?;
        assert_eq!(volume_names(&docker).await, volumes_before_provision);
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name: unrelated_container.clone(),
                    platform: None,
                }),
                Config {
                    image: Some("alpine:3.20"),
                    ..Default::default()
                },
            )
            .await
            .expect("create unrelated control container");
        docker
            .create_volume(CreateVolumeOptions {
                name: unrelated_volume.clone(),
                driver: "local".to_owned(),
                ..Default::default()
            })
            .await
            .expect("create unrelated control volume");
        assert_eq!(
            handle.network,
            format!("autospec-{}", execution_labels.execution_id)
        );
        assert_eq!(
            handle.agent_container,
            format!("autospec-{}-agent", execution_labels.execution_id)
        );
        assert_eq!(
            handle.service_containers,
            vec![format!("autospec-{}-cache", execution_labels.execution_id)]
        );
        assert!(handle.volumes.is_empty());
        assert!(handle.credentials_path.is_none());

        let network = docker
            .inspect_network::<String>(&handle.network, None)
            .await
            .expect("inspect provisioned network");
        let actual_network_labels = network.labels.expect("network has labels");
        for (key, value) in execution_labels.to_map() {
            assert_eq!(actual_network_labels.get(&key), Some(&value));
        }

        for container_name in
            std::iter::once(&handle.agent_container).chain(handle.service_containers.iter())
        {
            let container = docker
                .inspect_container(container_name, None)
                .await
                .expect("inspect provisioned container");
            let config = container.config.expect("container has config");
            let actual_labels = config.labels.expect("container has labels");
            for (key, value) in execution_labels.to_map() {
                assert_eq!(actual_labels.get(&key), Some(&value));
            }
            let host = container.host_config.expect("container has host config");
            assert_eq!(host.nano_cpus, Some(2_000_000_000));
            assert_eq!(host.memory, Some(384 * 1024 * 1024));
            assert_eq!(host.memory_swap, Some(384 * 1024 * 1024));
            assert_eq!(host.pids_limit, Some(DEFAULT_PIDS_LIMIT));
            let writable_layer_bytes = host
                .storage_opt
                .as_ref()
                .and_then(|opts| opts.get("size"))
                .expect("writable layer has a byte quota")
                .parse::<u64>()
                .expect("writable layer quota is numeric bytes");
            let tmpfs_bytes = host
                .mounts
                .as_ref()
                .into_iter()
                .flatten()
                .filter_map(|mount| mount.tmpfs_options.as_ref())
                .filter_map(|options| options.size_bytes)
                .map(|bytes| bytes as u64)
                .sum::<u64>();
            assert!(writable_layer_bytes > 0);
            assert!(writable_layer_bytes + tmpfs_bytes <= 3 * 1024 * 1024 * 1024);
            assert!(host.port_bindings.as_ref().is_none_or(HashMap::is_empty));
            assert_eq!(host.publish_all_ports, Some(false));
            assert_eq!(host.network_mode.as_deref(), Some(handle.network.as_str()));
            assert_eq!(
                container.state.and_then(|state| state.running),
                Some(true),
                "provisioned containers must be running"
            );
        }

        let service_container = docker
            .inspect_container(&handle.service_containers[0], None)
            .await
            .expect("inspect service network alias");
        let aliases = service_container
            .network_settings
            .and_then(|settings| settings.networks)
            .and_then(|networks| networks.get(&handle.network).cloned())
            .and_then(|endpoint| endpoint.aliases)
            .unwrap_or_default();
        assert!(aliases.iter().any(|alias| alias == "cache"));
        let service_mounts = service_container.mounts.unwrap_or_default();
        let data_mount = service_mounts
            .iter()
            .find(|mount| mount.destination.as_deref() == Some("/data"))
            .expect("Redis image-declared /data volume is overridden");
        assert_eq!(
            data_mount.typ,
            Some(bollard::models::MountPointTypeEnum::TMPFS)
        );
        assert!(data_mount.name.is_none());
        assert!(service_mounts
            .iter()
            .all(|mount| mount.typ != Some(bollard::models::MountPointTypeEnum::VOLUME)));

        assert!(!runtime
            .reconcile(std::slice::from_ref(&execution_labels.execution_id))
            .await?
            .contains(&execution_labels.execution_id));
        let orphans = runtime.reconcile(&[]).await?;
        assert!(orphans.contains(&execution_labels.execution_id));
        docker
            .inspect_network::<String>(&handle.network, None)
            .await
            .expect("reconciliation reports but does not delete the network");
        docker
            .inspect_container(&handle.agent_container, None)
            .await
            .expect("reconciliation reports but does not delete containers");

        runtime.destroy(&execution_labels).await?;
        assert!(docker
            .inspect_network::<String>(&handle.network, None)
            .await
            .is_err());
        for container_name in
            std::iter::once(&handle.agent_container).chain(handle.service_containers.iter())
        {
            assert!(docker
                .inspect_container(container_name, None)
                .await
                .is_err());
        }
        docker
            .inspect_network::<String>(&unrelated_network, None)
            .await
            .expect("selector cleanup preserves unrelated Docker resources");
        docker
            .inspect_container(&unrelated_container, None)
            .await
            .expect("selector cleanup preserves unrelated containers");
        docker
            .inspect_volume(&unrelated_volume)
            .await
            .expect("selector cleanup preserves unrelated volumes");

        Ok::<(), runtime_traits::RuntimeError>(())
    }
    .await;

    let _ = runtime.destroy(&execution_labels).await;
    let _ = docker
        .remove_container(
            &unrelated_container,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await;
    let _ = docker
        .remove_volume(&unrelated_volume, Some(RemoveVolumeOptions { force: true }))
        .await;
    let _ = docker.remove_network(&unrelated_network).await;
    result.expect("real Docker lifecycle succeeds");

    let managed_filter = HashMap::from([(
        "label".to_owned(),
        vec![format!("{}=true", labels::MANAGED)],
    )]);
    let leaked = docker
        .list_networks(Some(bollard::network::ListNetworksOptions {
            filters: managed_filter,
        }))
        .await
        .expect("list managed networks")
        .into_iter()
        .filter_map(|network| network.labels)
        .any(|resource_labels| {
            resource_labels.get(labels::EXECUTION_ID)
                == Some(&execution_labels.execution_id.to_string())
        });
    assert!(!leaked, "test execution network must not leak");
}

#[tokio::test]
async fn image_tmpfs_rejects_writes_past_the_execution_disk_budget() {
    let Some(runtime) =
        runtime_or_skip("image_tmpfs_rejects_writes_past_the_execution_disk_budget").await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let requirement = RuntimeRequirement {
        image: Some("alpine:3.20".to_owned()),
        cpu: 1,
        memory_mib: 2048,
        disk_gib: 1,
        ..RuntimeRequirement::default()
    };
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::new(),
    };
    let volumes_before = volume_names(&docker).await;

    let handle = runtime
        .provision(&execution_labels, &requirement, &[service])
        .await
        .expect("provision bounded tmpfs service");
    assert_eq!(volume_names(&docker).await, volumes_before);
    let agent_inspect = docker
        .inspect_container(&handle.agent_container, None)
        .await
        .expect("inspect bounded agent");
    let service_inspect = docker
        .inspect_container(&handle.service_containers[0], None)
        .await
        .expect("inspect bounded service");
    let hosts = [
        agent_inspect.host_config.expect("agent host config"),
        service_inspect.host_config.expect("service host config"),
    ];
    let total_configured_bytes = hosts
        .iter()
        .map(|host| {
            let root = host
                .storage_opt
                .as_ref()
                .and_then(|options| options.get("size"))
                .expect("root quota")
                .parse::<u64>()
                .expect("numeric root quota");
            let mounts = host
                .mounts
                .as_ref()
                .into_iter()
                .flatten()
                .filter_map(|mount| mount.tmpfs_options.as_ref())
                .filter_map(|options| options.size_bytes)
                .map(|bytes| bytes as u64)
                .sum::<u64>();
            root + mounts
        })
        .sum::<u64>();
    assert!(total_configured_bytes <= 1024 * 1024 * 1024);

    let host_mount = hosts[1]
        .mounts
        .clone()
        .unwrap_or_default()
        .into_iter()
        .find(|mount| mount.target.as_deref() == Some("/data"))
        .expect("bounded /data mount exists");
    assert_eq!(host_mount.typ, Some(MountTypeEnum::TMPFS));
    let mount_bytes = host_mount
        .tmpfs_options
        .and_then(|options| options.size_bytes)
        .expect("tmpfs byte limit");
    let overflow_mib = mount_bytes as u64 / (1024 * 1024) + 1;

    let exec = docker
        .create_exec(
            &handle.service_containers[0],
            CreateExecOptions {
                cmd: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    format!("dd if=/dev/zero of=/data/overflow bs=1M count={overflow_mib}"),
                ]),
                attach_stdout: Some(false),
                attach_stderr: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("create disk-boundary probe");
    docker
        .start_exec(
            &exec.id,
            Some(StartExecOptions {
                detach: true,
                ..Default::default()
            }),
        )
        .await
        .expect("start disk-boundary probe");
    let mut exit_code = None;
    for _ in 0..300 {
        let inspect = docker
            .inspect_exec(&exec.id)
            .await
            .expect("inspect disk-boundary probe");
        if inspect.running == Some(false) {
            exit_code = inspect.exit_code;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let _ = runtime.destroy(&execution_labels).await;
    assert!(exit_code.is_some_and(|code| code != 0));
    assert!(docker
        .inspect_container(&handle.service_containers[0], None)
        .await
        .is_err());
}

#[tokio::test]
async fn cleanup_aggregates_volume_failures_and_still_removes_the_network() {
    let Some(runtime) =
        runtime_or_skip("cleanup_aggregates_volume_failures_and_still_removes_the_network").await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let network = DockerRuntime::network_name(&execution_labels.execution_id);
    docker
        .create_network(CreateNetworkOptions {
            name: network.clone(),
            driver: "bridge".to_owned(),
            labels: execution_labels.to_map().into_iter().collect(),
            ..Default::default()
        })
        .await
        .expect("create owned network");

    let volumes = [
        DockerRuntime::volume_name(&execution_labels.execution_id, "held-one"),
        DockerRuntime::volume_name(&execution_labels.execution_id, "held-two"),
    ];
    let holders = [
        format!("{network}-holder-one"),
        format!("{network}-holder-two"),
    ];
    for (volume, holder) in volumes.iter().zip(&holders) {
        docker
            .create_volume(CreateVolumeOptions {
                name: volume.clone(),
                driver: "local".to_owned(),
                labels: execution_labels.to_map().into_iter().collect(),
                ..Default::default()
            })
            .await
            .expect("create held owned volume");
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name: holder.clone(),
                    platform: None,
                }),
                Config {
                    image: Some("alpine:3.20"),
                    cmd: Some(vec!["sleep", "infinity"]),
                    host_config: Some(HostConfig {
                        mounts: Some(vec![Mount {
                            target: Some("/held".to_owned()),
                            source: Some(volume.clone()),
                            typ: Some(MountTypeEnum::VOLUME),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("create unrelated holder container");
    }

    let error = runtime
        .destroy(&execution_labels)
        .await
        .expect_err("in-use volumes make scoped cleanup report failure")
        .to_string();
    assert!(error.contains(&volumes[0]));
    assert!(error.contains(&volumes[1]));
    assert!(docker
        .inspect_network::<String>(&network, None)
        .await
        .is_err());

    for holder in &holders {
        let _ = docker
            .remove_container(
                holder,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
    }
    for volume in &volumes {
        let _ = docker
            .remove_volume(volume, Some(RemoveVolumeOptions { force: true }))
            .await;
    }
}

#[tokio::test]
async fn provisioning_failure_reports_rollback_failure_and_leaks_no_anonymous_volume() {
    let Some(runtime) = runtime_or_skip(
        "provisioning_failure_reports_rollback_failure_and_leaks_no_anonymous_volume",
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let owned_volume = DockerRuntime::volume_name(&execution_labels.execution_id, "cache-data");
    let holder = format!("autospec-{}-holder", execution_labels.execution_id);
    let conflicting_agent = DockerRuntime::agent_container_name(&execution_labels.execution_id);
    docker
        .create_volume(CreateVolumeOptions {
            name: owned_volume.clone(),
            driver: "local".to_owned(),
            labels: execution_labels.to_map().into_iter().collect(),
            ..Default::default()
        })
        .await
        .expect("create held execution volume");
    for (name, mounts) in [
        (
            holder.clone(),
            Some(vec![Mount {
                target: Some("/held".to_owned()),
                source: Some(owned_volume.clone()),
                typ: Some(MountTypeEnum::VOLUME),
                ..Default::default()
            }]),
        ),
        (conflicting_agent.clone(), None),
    ] {
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name,
                    platform: None,
                }),
                Config {
                    image: Some("alpine:3.20"),
                    cmd: Some(vec!["sleep", "infinity"]),
                    host_config: Some(HostConfig {
                        mounts,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("create failure-injection container");
    }
    let service = ServiceRequirement {
        name: "cache".to_owned(),
        image: "redis:7-alpine".to_owned(),
        env: BTreeMap::new(),
    };
    let volumes_before_failure = volume_names(&docker).await;

    let error = runtime
        .provision(&execution_labels, &runtime_requirement(), &[service])
        .await
        .expect_err("agent name conflict triggers provisioning rollback")
        .to_string();
    let volumes_after_failure = volume_names(&docker).await;
    assert!(error.contains(execution_labels.execution_id.as_str()));
    assert!(error.contains("create agent container"));
    assert!(error.contains("rollback"));
    assert!(error.contains(&owned_volume));
    assert_eq!(
        volumes_after_failure, volumes_before_failure,
        "failed provisioning must not leave an anonymous image volume"
    );
    assert!(docker
        .inspect_network::<String>(
            &DockerRuntime::network_name(&execution_labels.execution_id),
            None,
        )
        .await
        .is_err());

    let managed_mounts = docker
        .list_containers(Some(bollard::container::ListContainersOptions {
            all: true,
            filters: HashMap::from([("label".to_owned(), execution_labels.selector())]),
            ..Default::default()
        }))
        .await
        .expect("list execution-labelled containers");
    assert!(
        managed_mounts.is_empty(),
        "rollback removes service containers"
    );

    for container in [&holder, &conflicting_agent] {
        let _ = docker
            .remove_container(
                container,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
    }
    let _ = docker
        .remove_volume(&owned_volume, Some(RemoveVolumeOptions { force: true }))
        .await;
}
