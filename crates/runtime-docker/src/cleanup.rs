use crate::DockerRuntime;
use bollard::{
    container::{ListContainersOptions, RemoveContainerOptions},
    network::ListNetworksOptions,
    volume::{ListVolumesOptions, RemoveVolumeOptions},
};
use orchestrator_core::{labels, ExecutionId, OwnershipLabels};
use runtime_traits::RuntimeError;
use std::collections::{BTreeSet, HashMap};

pub(crate) async fn destroy(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
) -> Result<(), RuntimeError> {
    runtime.require_compatible_daemon().await?;
    let filters = label_filters(labels.selector());

    let containers = runtime
        .client
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: filters.clone(),
            ..Default::default()
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list owned containers: {error}")))?;
    for container in containers {
        if let Some(id) = container.id {
            runtime
                .client
                .remove_container(
                    &id,
                    Some(RemoveContainerOptions {
                        force: true,
                        v: false,
                        link: false,
                    }),
                )
                .await
                .map_err(|error| {
                    RuntimeError::Cleanup(format!("remove owned container {id}: {error}"))
                })?;
        }
    }

    let volumes = runtime
        .client
        .list_volumes(Some(ListVolumesOptions {
            filters: filters.clone(),
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list owned volumes: {error}")))?;
    for volume in volumes.volumes.unwrap_or_default() {
        let name = volume.name;
        runtime
            .client
            .remove_volume(&name, Some(RemoveVolumeOptions { force: true }))
            .await
            .map_err(|error| {
                RuntimeError::Cleanup(format!("remove owned volume {name}: {error}"))
            })?;
    }

    let networks = runtime
        .client
        .list_networks(Some(ListNetworksOptions { filters }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list owned networks: {error}")))?;
    for network in networks {
        if let Some(id) = network.id {
            runtime.client.remove_network(&id).await.map_err(|error| {
                RuntimeError::Cleanup(format!("remove owned network {id}: {error}"))
            })?;
        }
    }
    Ok(())
}

pub(crate) async fn reconcile(
    runtime: &DockerRuntime,
    live: &[ExecutionId],
) -> Result<Vec<ExecutionId>, RuntimeError> {
    runtime.require_compatible_daemon().await?;
    let filters = label_filters(vec![format!("{}=true", labels::MANAGED)]);
    let live = live.iter().collect::<BTreeSet<_>>();
    let mut orphans = BTreeSet::new();

    let containers = runtime
        .client
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: filters.clone(),
            ..Default::default()
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed containers: {error}")))?;
    for resource_labels in containers.into_iter().filter_map(|item| item.labels) {
        collect_orphan(&resource_labels, &live, &mut orphans);
    }

    let networks = runtime
        .client
        .list_networks(Some(ListNetworksOptions {
            filters: filters.clone(),
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed networks: {error}")))?;
    for resource_labels in networks.into_iter().filter_map(|item| item.labels) {
        collect_orphan(&resource_labels, &live, &mut orphans);
    }

    let volumes = runtime
        .client
        .list_volumes(Some(ListVolumesOptions { filters }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed volumes: {error}")))?;
    for resource_labels in volumes
        .volumes
        .unwrap_or_default()
        .into_iter()
        .map(|item| item.labels)
    {
        collect_orphan(&resource_labels, &live, &mut orphans);
    }

    Ok(orphans.into_iter().collect())
}

fn label_filters(selector: Vec<String>) -> HashMap<String, Vec<String>> {
    HashMap::from([("label".to_owned(), selector)])
}

fn collect_orphan(
    resource_labels: &HashMap<String, String>,
    live: &BTreeSet<&ExecutionId>,
    orphans: &mut BTreeSet<ExecutionId>,
) {
    if let Some(value) = resource_labels.get(labels::EXECUTION_ID) {
        let execution_id = ExecutionId::new(value.clone());
        if !live.contains(&execution_id) {
            orphans.insert(execution_id);
        }
    }
}
