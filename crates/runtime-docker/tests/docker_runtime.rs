use bollard::{
    container::{Config, CreateContainerOptions, RemoveContainerOptions},
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
    collections::{BTreeMap, HashMap},
    env,
    time::{SystemTime, UNIX_EPOCH},
};

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
        let handle = runtime
            .provision(&execution_labels, &runtime_requirement(), &[service])
            .await?;
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
            assert_eq!(
                host.storage_opt.as_ref().and_then(|opts| opts.get("size")),
                Some(&"3G".to_owned())
            );
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
