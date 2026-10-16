use crate::{
    limits::{drop_all_capabilities, harden_service, host_limits},
    provision::{container_create_error, verify_container_mount_sources, ReadyStorageGuard},
    DockerRuntime, InferenceRelay, INFERENCE_RELAY_ALIAS,
};
use bollard::{
    container::{Config, CreateContainerOptions, NetworkingConfig},
    models::{EndpointSettings, Mount},
    network::ConnectNetworkOptions,
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
        harden_service(&mut limits, runtime.hardening);
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
        verify_container_mount_sources(runtime, &name, execution_root, mounts, None).await?;
        containers.push(name);
    }
    Ok(containers)
}

/// The docker network every container can reach the outside through.
const DEFAULT_BRIDGE: &str = "bridge";

/// The relay container's definition (spec section 5 R-ORCH).
///
/// The image receives exactly two arguments, `<listen_port> <upstream>`, so it
/// can forward only to the configured InferWeave `host:port`. It carries the
/// execution's ownership labels (AGENTS.md invariant 6), so `destroy` and
/// `reconcile` remove it with the rest of the execution, has every capability
/// dropped and the same CPU, memory and PID limits as the other containers.
#[allow(dead_code)] // wired into provisioning by the next issue (#75)
pub(crate) fn relay_config(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    network: &str,
    relay: &InferenceRelay,
) -> Config<String> {
    let mut limits = host_limits(requirement);
    harden_service(&mut limits, runtime.hardening);
    drop_all_capabilities(&mut limits);
    limits.network_mode = Some(network.to_owned());
    Config {
        image: Some(relay.image.clone()),
        cmd: Some(vec![relay.listen_port.to_string(), relay.upstream.clone()]),
        env: Some(vec!["HOME=/tmp".to_owned(), "TMPDIR=/tmp".to_owned()]),
        labels: Some(labels.to_map().into_iter().collect()),
        host_config: Some(limits),
        networking_config: Some(networking_config(network, INFERENCE_RELAY_ALIAS)),
        ..Default::default()
    }
}

/// Creates (does not start) the execution's inference relay: on the execution
/// network under the `inferweave` alias, and on the default bridge so it can
/// reach the upstream when the execution network is internal.
///
/// The relay image must already be present locally.
#[allow(dead_code)] // wired into provisioning by the next issue (#75)
pub(crate) async fn create_relay(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
    requirement: &RuntimeRequirement,
    network: &str,
    relay: &InferenceRelay,
) -> Result<String, RuntimeError> {
    let name = DockerRuntime::relay_container_name(&labels.execution_id);
    let config = relay_config(runtime, labels, requirement, network, relay);
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
            container_create_error(format!("create relay container {name}"), error, false)
        })?;
    runtime
        .client
        .connect_network(
            DEFAULT_BRIDGE,
            ConnectNetworkOptions {
                container: name.clone(),
                endpoint_config: EndpointSettings::default(),
            },
        )
        .await
        .map_err(|error| {
            RuntimeError::Provisioning(format!(
                "attach relay {name} to the default bridge: {error}"
            ))
        })?;
    Ok(name)
}

#[allow(dead_code)] // wired into provisioning by the next issue (#75)
pub(crate) async fn start_relay(runtime: &DockerRuntime, name: &str) -> Result<(), RuntimeError> {
    runtime
        .client
        .start_container::<String>(name, None)
        .await
        .map_err(|error| RuntimeError::Provisioning(format!("start relay {name}: {error}")))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cleanup;
    use bollard::{
        container::{LogOutput, RemoveContainerOptions},
        exec::{CreateExecOptions, StartExecResults},
        image::RemoveImageOptions,
        Docker,
    };
    use futures_util::StreamExt;
    use orchestrator_core::{ExecutionId, WorkerId};
    use std::{collections::HashMap, process::Command};

    fn labels(id: &str) -> OwnershipLabels {
        OwnershipLabels {
            execution_id: ExecutionId::new(id),
            worker_id: WorkerId::new("relay-test-worker"),
            repository: "InferWeave/autospec-orchestrator".to_owned(),
            issue: Some("task-74".to_owned()),
        }
    }

    fn relay(image: &str, upstream: &str) -> InferenceRelay {
        InferenceRelay {
            image: image.to_owned(),
            upstream: upstream.to_owned(),
            listen_port: 8080,
        }
    }

    fn offline_runtime() -> DockerRuntime {
        DockerRuntime::connect_with_state_root(Some("tcp://docker-api:2375"), "/tmp")
            .expect("lazy client")
    }

    #[test]
    fn relay_gets_only_the_listen_port_and_upstream_all_labels_and_no_capabilities() {
        let labels = labels("relay-config-unit");
        let config = relay_config(
            &offline_runtime(),
            &labels,
            &RuntimeRequirement::default(),
            "autospec-relay-config-unit",
            &relay("relay:1", "gateway.local:8080"),
        );
        assert_eq!(config.image.as_deref(), Some("relay:1"));
        assert_eq!(
            config.cmd,
            Some(vec!["8080".to_owned(), "gateway.local:8080".to_owned()])
        );
        let expected: HashMap<String, String> = labels.to_map().into_iter().collect();
        assert_eq!(config.labels, Some(expected));
        let host = config.host_config.expect("host config");
        assert_eq!(host.cap_drop, Some(vec!["ALL".to_owned()]));
        assert_eq!(
            host.network_mode.as_deref(),
            Some("autospec-relay-config-unit")
        );
        assert_eq!(host.privileged, Some(false));
        assert!(host.mounts.is_none(), "the relay mounts nothing");
        let endpoints = config
            .networking_config
            .expect("networking")
            .endpoints_config;
        assert_eq!(
            endpoints["autospec-relay-config-unit"].aliases,
            Some(vec!["inferweave".to_owned()])
        );
    }

    async fn exec(docker: &Docker, container: &str, command: &str) -> (i64, String) {
        let exec = docker
            .create_exec(
                container,
                CreateExecOptions {
                    cmd: Some(vec!["sh".to_owned(), "-c".to_owned(), command.to_owned()]),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await;
        let Ok(exec) = exec else {
            return (-1, String::new());
        };
        let mut out = String::new();
        if let Ok(StartExecResults::Attached { mut output, .. }) =
            docker.start_exec(&exec.id, None).await
        {
            while let Some(Ok(item)) = output.next().await {
                if let LogOutput::StdOut { message } | LogOutput::StdErr { message } = item {
                    out.push_str(&String::from_utf8_lossy(&message));
                }
            }
        }
        let code = docker.inspect_exec(&exec.id).await.expect("inspect exec");
        (code.exit_code.unwrap_or(-1), out)
    }

    /// Builds a throwaway relay image from the local Alpine image: busybox
    /// `nc` listening on `$1` and forwarding each connection to `$2`, plus a
    /// tiny upstream that reads the request before answering (answering first
    /// resets the connection and loses the response when a hop is added).
    fn build_test_relay_image(tag: &str) -> bool {
        let dir = tempfile::tempdir().expect("build context");
        for (name, body) in [
            (
                "relay.sh",
                "#!/bin/sh\nhost=${2%:*}\nport=${2##*:}\nwhile true; do nc -l -p \"$1\" -e nc \"$host\" \"$port\"; done\n",
            ),
            (
                "upstream.sh",
                "#!/bin/sh\nwhile true; do nc -l -p 8000 -e /respond.sh; done\n",
            ),
            (
                "respond.sh",
                "#!/bin/sh\nwhile read -r line; do case \"$line\" in \"$(printf '\\r')\"|\"\") break;; esac; done\nprintf 'HTTP/1.0 200 OK\\r\\n\\r\\nrelayed-ok\\n'\n",
            ),
        ] {
            std::fs::write(dir.path().join(name), body).expect("write script");
        }
        std::fs::write(
            dir.path().join("Dockerfile"),
            "FROM alpine:3.20\nCOPY relay.sh upstream.sh respond.sh /\nRUN chmod 755 /relay.sh /upstream.sh /respond.sh\nENTRYPOINT [\"/relay.sh\"]\n",
        )
        .expect("write Dockerfile");
        Command::new("docker")
            .args(["build", "-q", "-t", tag])
            .arg(dir.path())
            .output()
            .is_ok_and(|output| output.status.success())
    }

    #[tokio::test]
    async fn a_created_relay_is_labelled_unprivileged_forwards_and_is_removed_by_destroy() {
        let Ok(docker) = Docker::connect_with_local_defaults() else {
            return;
        };
        if docker.version().await.is_err() {
            println!("SKIP: Docker daemon unavailable");
            return;
        }
        let runtime = DockerRuntime::new().expect("runtime");
        let id = format!("relay-live-{}", std::process::id());
        let labels = labels(&id);
        let tag = format!("autospec-relay-test:{}", std::process::id());
        if !build_test_relay_image(&tag) {
            println!("SKIP: cannot build the throwaway relay image (alpine:3.20 not local)");
            return;
        }
        let network = DockerRuntime::network_name(&labels.execution_id);
        let upstream_name = format!("autospec-{id}-upstream");
        let probe_name = format!("autospec-{id}-probe");
        let result = async {
            // An upstream on the default bridge serving a fixed page.
            let mut upstream_labels = labels.to_map().into_iter().collect::<HashMap<_, _>>();
            upstream_labels.insert("autospec.test".to_owned(), "relay".to_owned());
            let create = |name: &str, image: &str, cmd: Vec<&str>, network: &str, labels| {
                let config = Config {
                    image: Some(image.to_owned()),
                    entrypoint: Some(vec![]),
                    cmd: Some(cmd.into_iter().map(str::to_owned).collect()),
                    labels: Some(labels),
                    host_config: Some(bollard::models::HostConfig {
                        network_mode: Some(network.to_owned()),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let docker = docker.clone();
                let name = name.to_owned();
                async move {
                    docker
                        .create_container(
                            Some(CreateContainerOptions {
                                name,
                                platform: None,
                            }),
                            config,
                        )
                        .await
                        .map(|_| ())
                }
            };
            create(
                &upstream_name,
                &tag,
                vec!["/upstream.sh"],
                "bridge",
                upstream_labels.clone(),
            )
            .await
            .map_err(|e| e.to_string())?;
            docker
                .start_container::<String>(&upstream_name, None)
                .await
                .map_err(|e| e.to_string())?;
            let upstream_ip = docker
                .inspect_container(&upstream_name, None)
                .await
                .map_err(|e| e.to_string())?
                .network_settings
                .and_then(|n| n.networks)
                .and_then(|n| n.get("bridge").and_then(|e| e.ip_address.clone()))
                .ok_or("upstream has no bridge address")?;

            docker
                .create_network(bollard::network::CreateNetworkOptions {
                    name: network.clone(),
                    driver: "bridge".to_owned(),
                    labels: labels.to_map().into_iter().collect::<HashMap<_, _>>(),
                    ..Default::default()
                })
                .await
                .map_err(|e| e.to_string())?;
            let name = create_relay(
                &runtime,
                &labels,
                &RuntimeRequirement::default(),
                &network,
                &relay(&tag, &format!("{upstream_ip}:8000")),
            )
            .await
            .map_err(|e| e.to_string())?;
            assert_eq!(
                name,
                DockerRuntime::relay_container_name(&labels.execution_id)
            );
            start_relay(&runtime, &name)
                .await
                .map_err(|e| e.to_string())?;

            let inspect = docker
                .inspect_container(&name, None)
                .await
                .map_err(|e| e.to_string())?;
            let container_labels = inspect.config.and_then(|c| c.labels).unwrap_or_default();
            assert_eq!(
                container_labels.get("autospec.managed").map(String::as_str),
                Some("true")
            );
            assert_eq!(
                container_labels
                    .get("autospec.execution_id")
                    .map(String::as_str),
                Some(id.as_str())
            );
            assert_eq!(
                inspect.host_config.and_then(|h| h.cap_drop),
                Some(vec!["ALL".to_owned()])
            );
            let networks = inspect
                .network_settings
                .and_then(|n| n.networks)
                .unwrap_or_default();
            assert!(networks.contains_key("bridge") && networks.contains_key(&network));

            // A probe on the execution network reaches the upstream only through the relay.
            create(
                &probe_name,
                "alpine:3.20",
                vec!["sleep", "600"],
                &network,
                labels.to_map().into_iter().collect(),
            )
            .await
            .map_err(|e| e.to_string())?;
            docker
                .start_container::<String>(&probe_name, None)
                .await
                .map_err(|e| e.to_string())?;
            let mut body = String::new();
            for _ in 0..90 {
                let (code, out) = exec(
                    &docker,
                    &probe_name,
                    "wget -T 3 -qO- http://inferweave:8080/ 2>&1",
                )
                .await;
                // busybox wget may exit non-zero after the body when the peer
                // closes with a reset, so the body decides, not the exit code.
                let _ = code;
                if out.contains("relayed-ok") {
                    body = out;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            if !body.contains("relayed-ok") {
                for container in [&name, &upstream_name] {
                    let logs = Command::new("docker").args(["logs", container]).output();
                    eprintln!(
                        "--- logs {container}: {:?}",
                        logs.map(|o| (
                            String::from_utf8_lossy(&o.stdout).into_owned(),
                            String::from_utf8_lossy(&o.stderr).into_owned()
                        ))
                    );
                }
                let (code, out) = exec(
                    &docker,
                    &probe_name,
                    "wget -T 3 -O- http://inferweave:8080/ 2>&1; getent hosts inferweave",
                )
                .await;
                eprintln!("--- last probe: {code} {out}");
            }
            assert!(
                body.contains("relayed-ok"),
                "the relay forwards to its upstream, got {body:?}"
            );
            Ok::<_, String>(name)
        }
        .await;

        let destroyed = cleanup::destroy(&runtime, &labels).await;
        let _ = docker
            .remove_container(
                &upstream_name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
        let _ = docker
            .remove_image(
                &tag,
                Some(RemoveImageOptions {
                    force: true,
                    ..Default::default()
                }),
                None,
            )
            .await;
        let name = result.expect("relay lifecycle");
        destroyed.expect("destroy removes the relay by its ownership labels");
        assert!(
            docker.inspect_container(&name, None).await.is_err(),
            "the relay no longer exists after destroy"
        );
    }
}
