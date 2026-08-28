use bollard::{
    container::{Config, CreateContainerOptions},
    exec::{CreateExecOptions, StartExecOptions},
    models::{HostConfig, Mount, MountTypeEnum},
    network::CreateNetworkOptions,
    volume::CreateVolumeOptions,
    Docker,
};
use orchestrator_core::{
    labels, ExecutionId, OwnershipLabels, RuntimeRequirement, ServiceRequirement, WorkerId,
};
use runtime_docker::{host_limits, DockerRuntime, DEFAULT_PIDS_LIMIT};
use runtime_traits::Runtime;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestStateRoot {
    path: PathBuf,
}

impl TestStateRoot {
    fn new(labels: &OwnershipLabels) -> Self {
        let path = env::temp_dir().join(format!(
            "autospec-runtime-docker-state-{}",
            labels.execution_id
        ));
        fs::create_dir(&path).expect("create unique test state root");
        fs::create_dir_all(path.join("worktrees").join(labels.execution_id.as_str()))
            .expect("create execution worktree");
        let session = path.join("sessions").join(labels.execution_id.as_str());
        fs::create_dir_all(&session).expect("create host-private session root");
        for (name, contents) in [
            ("owner.json", "host owner\n"),
            (".cursor", "host cursor\n"),
            ("resume-count", "7\n"),
            (
                &format!("pi.events-{}.jsonl", labels.execution_id),
                "host live events\n",
            ),
        ] {
            fs::write(session.join(name), contents).expect("write host-private session metadata");
        }
        Self { path }
    }

    fn worktree(&self, labels: &OwnershipLabels) -> PathBuf {
        self.path
            .join("worktrees")
            .join(labels.execution_id.as_str())
    }

    fn session(&self, labels: &OwnershipLabels) -> PathBuf {
        self.path
            .join("sessions")
            .join(labels.execution_id.as_str())
    }
}

impl Drop for TestStateRoot {
    fn drop(&mut self) {
        let is_owned_test_path = self.path.parent() == Some(env::temp_dir().as_path())
            && self
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("autospec-runtime-docker-state-"));
        if is_owned_test_path {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

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
    let sequence = EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    ExecutionId::new(format!(
        "runtime-docker-{}-{nonce}-{sequence}",
        std::process::id()
    ))
}

fn control_label_map(labels: &OwnershipLabels) -> HashMap<String, String> {
    control_labels_for(labels).to_map().into_iter().collect()
}

fn control_labels_for(labels: &OwnershipLabels) -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new(format!("{}-control", labels.execution_id)),
        worker_id: labels.worker_id.clone(),
        repository: labels.repository.clone(),
        issue: labels.issue.clone(),
    }
}

struct DockerTestScope {
    runtime: DockerRuntime,
    labels: OwnershipLabels,
    cleaned: bool,
}

impl DockerTestScope {
    fn new(runtime: &DockerRuntime, labels: &OwnershipLabels) -> Self {
        Self {
            runtime: runtime.clone(),
            labels: labels.clone(),
            cleaned: false,
        }
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        cleanup_test_resources(&self.runtime, &self.labels).await?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for DockerTestScope {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        let runtime = self.runtime.clone();
        let labels = self.labels.clone();
        let cleanup_thread = std::thread::Builder::new()
            .name("runtime-docker-test-cleanup".to_owned())
            .spawn(move || {
                let tokio_runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("create cleanup Tokio runtime: {error}"))?;
                tokio_runtime.block_on(cleanup_test_resources(&runtime, &labels))
            });
        let cleanup = match cleanup_thread {
            Ok(thread) => thread.join().map_err(thread_panic_message),
            Err(error) => {
                write_cleanup_diagnostic(&format!("create cleanup thread: {error}"));
                return;
            }
        };
        match cleanup {
            Ok(Ok(())) => {}
            Ok(Err(error)) => write_cleanup_diagnostic(&error),
            Err(error) => write_cleanup_diagnostic(&error),
        }
    }
}

fn thread_panic_message(payload: Box<dyn Any + Send>) -> String {
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload");
    format!("cleanup thread panicked: {message}")
}

fn write_cleanup_diagnostic(message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(b"Docker test cleanup failed: ");
    let _ = stderr.write_all(message.as_bytes());
    let _ = stderr.write_all(b"\n");
}

async fn cleanup_test_resources(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
) -> Result<(), String> {
    let control_labels = control_labels_for(labels);
    let mut errors = Vec::new();
    for (purpose, scoped_labels) in [("control", &control_labels), ("execution", labels)] {
        if let Err(error) = runtime.destroy(scoped_labels).await {
            errors.push(format!(
                "{purpose} execution_id={}: {error}",
                scoped_labels.execution_id
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[test]
fn execution_ids_are_scoped_to_the_test_process() {
    let execution_id = unique_execution_id();

    assert!(execution_id
        .as_str()
        .contains(&format!("-{}-", std::process::id())));
}

#[test]
fn control_resources_use_only_distinct_contract_ownership_labels() {
    let execution_labels = labels_for(unique_execution_id());
    let support_labels = control_label_map(&execution_labels);
    let expected_keys = BTreeSet::from([
        labels::MANAGED.to_owned(),
        labels::EXECUTION_ID.to_owned(),
        labels::WORKER_ID.to_owned(),
        labels::REPOSITORY.to_owned(),
        labels::ISSUE.to_owned(),
    ]);

    assert_eq!(
        support_labels.keys().cloned().collect::<BTreeSet<_>>(),
        expected_keys
    );
    assert_ne!(
        support_labels.get(labels::EXECUTION_ID),
        Some(&execution_labels.execution_id.to_string())
    );
}

#[test]
fn unwind_cleanup_uses_only_fallible_thread_and_diagnostic_operations() {
    let source = include_str!("docker_runtime.rs");
    let scope_implementation = source
        .split_once("struct DockerTestScope")
        .expect("scope has a Drop implementation")
        .1
        .split_once("#[test]\nfn execution_ids_are_scoped_to_the_test_process")
        .expect("scope helpers precede tests")
        .0;
    let drop_implementation = scope_implementation
        .split_once("impl Drop for DockerTestScope")
        .expect("scope has a Drop implementation")
        .1
        .split_once("fn thread_panic_message")
        .expect("diagnostic helpers follow Drop")
        .0;

    assert!(!drop_implementation.contains("std::thread::spawn("));
    assert!(!scope_implementation.contains(concat!("eprint", "ln!")));
    assert!(!drop_implementation.contains(".expect("));
    assert!(!drop_implementation.contains(".unwrap("));
    assert!(!drop_implementation.contains("panic!("));
    assert!(drop_implementation.contains("std::thread::Builder::new()"));
    assert!(drop_implementation.contains("write_cleanup_diagnostic"));
    assert!(scope_implementation.contains("stderr.write_all"));
    for diagnostic in [
        "create cleanup thread",
        "create cleanup Tokio runtime",
        "cleanup thread panicked",
    ] {
        assert!(scope_implementation.contains(diagnostic));
    }
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

async fn runtime_at_state_root_or_skip(
    test_name: &str,
    state_root: &Path,
) -> Option<DockerRuntime> {
    let runtime = match DockerRuntime::connect_with_state_root(None, state_root) {
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

async fn execution_runtime_or_skip(
    test_name: &str,
    labels: &OwnershipLabels,
) -> Option<(TestStateRoot, DockerRuntime)> {
    let state = TestStateRoot::new(labels);
    let runtime = runtime_at_state_root_or_skip(test_name, &state.path).await?;
    Some((state, runtime))
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
async fn agent_mounts_only_writable_worktree_and_durable_conversation() {
    let execution_labels = labels_for(unique_execution_id());
    let state = TestStateRoot::new(&execution_labels);
    let Some(runtime) = runtime_at_state_root_or_skip(
        "agent_mounts_only_writable_worktree_and_durable_conversation",
        &state.path,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let handle = runtime
        .provision(&execution_labels, &runtime_requirement(), &[])
        .await
        .expect("provision mounted agent");
    let inspect = docker
        .inspect_container(&handle.agent_container, None)
        .await
        .expect("inspect mounted agent");
    let mounts = inspect
        .host_config
        .expect("agent host config")
        .mounts
        .expect("agent bind mounts");
    let actual_mounts = mounts
        .iter()
        .map(|mount| {
            (
                mount.target.clone().expect("mount target"),
                mount.source.clone().expect("mount source"),
                mount.typ,
            )
        })
        .collect::<BTreeSet<_>>();
    assert!(mounts.iter().all(|mount| mount.read_only != Some(true)));
    let expected_mounts = BTreeSet::from([
        (
            "/workspace".to_owned(),
            fs::canonicalize(state.worktree(&execution_labels))
                .expect("canonical worktree")
                .display()
                .to_string(),
            Some(MountTypeEnum::BIND),
        ),
        (
            "/session".to_owned(),
            fs::canonicalize(state.session(&execution_labels).join("conversation"))
                .expect("canonical conversation")
                .display()
                .to_string(),
            Some(MountTypeEnum::BIND),
        ),
    ]);
    assert_eq!(actual_mounts, expected_mounts);

    let private_live_events = format!("pi.events-{}.jsonl", execution_labels.execution_id);
    let probe = docker
        .create_exec(
            &handle.agent_container,
            CreateExecOptions {
                cmd: Some(vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    format!(
                        "printf workspace > /workspace/container-write && \
                         printf conversation > /session/container-write && \
                         test ! -e /session/owner.json && \
                         test ! -e /session/.cursor && \
                         test ! -e /session/resume-count && \
                         test ! -e /session/{private_live_events} && \
                         test ! -e /var/run/docker.sock"
                    ),
                ]),
                attach_stdout: Some(false),
                attach_stderr: Some(false),
                ..Default::default()
            },
        )
        .await
        .expect("create mount isolation probe");
    docker
        .start_exec(
            &probe.id,
            Some(StartExecOptions {
                detach: true,
                ..Default::default()
            }),
        )
        .await
        .expect("start mount isolation probe");
    let mut exit_code = None;
    for _ in 0..200 {
        let inspect = docker
            .inspect_exec(&probe.id)
            .await
            .expect("inspect mount isolation probe");
        if inspect.running == Some(false) {
            exit_code = inspect.exit_code;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(exit_code, Some(0));
    assert_eq!(
        fs::read_to_string(state.worktree(&execution_labels).join("container-write"))
            .expect("read worktree write"),
        "workspace"
    );
    let conversation_write = state
        .session(&execution_labels)
        .join("conversation/container-write");
    assert_eq!(
        fs::read_to_string(&conversation_write).expect("read conversation write"),
        "conversation"
    );
    for (name, contents) in [
        ("owner.json", "host owner\n"),
        (".cursor", "host cursor\n"),
        ("resume-count", "7\n"),
        (private_live_events.as_str(), "host live events\n"),
    ] {
        assert_eq!(
            fs::read_to_string(state.session(&execution_labels).join(name))
                .expect("read private metadata"),
            contents
        );
    }

    scope.cleanup().await.expect("cleanup mounted agent");
    assert_eq!(
        fs::read_to_string(conversation_write).expect("conversation survives container cleanup"),
        "conversation"
    );
}

#[tokio::test]
async fn panicking_test_scope_removes_only_its_execution_resources() {
    let Some(runtime) =
        runtime_or_skip("panicking_test_scope_removes_only_its_execution_resources").await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let scope = DockerTestScope::new(&runtime, &execution_labels);
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

    let panic = tokio::spawn(async move {
        let _scope = scope;
        panic!("exercise failure cleanup");
    })
    .await;
    assert!(panic.is_err());
    let cleaned_during_unwind = docker
        .inspect_network::<String>(&network, None)
        .await
        .is_err();
    let _ = runtime.destroy(&execution_labels).await;

    assert!(
        cleaned_during_unwind,
        "a test assertion panic must trigger execution-scoped cleanup"
    );
}

#[tokio::test]
async fn failed_normal_cleanup_keeps_scope_armed() {
    let Some(runtime) = runtime_or_skip("failed_normal_cleanup_keeps_scope_armed").await else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let execution_labels = labels_for(unique_execution_id());
    let control_labels = control_labels_for(&execution_labels);
    let blocker_labels = [
        labels_for(ExecutionId::new(format!(
            "{}-blocker-one",
            execution_labels.execution_id
        ))),
        labels_for(ExecutionId::new(format!(
            "{}-blocker-two",
            execution_labels.execution_id
        ))),
    ];
    let volumes = [
        DockerRuntime::volume_name(&execution_labels.execution_id, "blocked"),
        DockerRuntime::volume_name(&control_labels.execution_id, "blocked"),
    ];
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
    let mut blocker_scopes = blocker_labels
        .iter()
        .map(|labels| DockerTestScope::new(&runtime, labels))
        .collect::<Vec<_>>();

    for ((volume, volume_labels), blocker_labels) in volumes
        .iter()
        .zip([&execution_labels, &control_labels])
        .zip(&blocker_labels)
    {
        docker
            .create_volume(CreateVolumeOptions {
                name: volume.clone(),
                driver: "local".to_owned(),
                labels: volume_labels.to_map().into_iter().collect(),
                ..Default::default()
            })
            .await
            .expect("create selector-owned blocked volume");
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name: format!("{volume}-holder"),
                    platform: None,
                }),
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    labels: Some(blocker_labels.to_map().into_iter().collect()),
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
            .expect("create validly-owned blocking container");
    }

    let cleanup_error = scope
        .cleanup()
        .await
        .expect_err("both blocked selectors must report cleanup failures");
    let incorrectly_marked_clean = scope.cleaned;
    for blocker_scope in &mut blocker_scopes {
        blocker_scope
            .cleanup()
            .await
            .expect("cleanup blocker resources by their selector");
    }
    scope
        .cleanup()
        .await
        .expect("cleanup succeeds after blockers are removed");

    assert!(cleanup_error.contains(&volumes[0]));
    assert!(cleanup_error.contains(&volumes[1]));
    assert!(
        !incorrectly_marked_clean,
        "cleanup failures must leave the unwind guard armed"
    );
}

#[tokio::test]
async fn provision_reconcile_and_destroy_preserve_execution_isolation() {
    let execution_labels = labels_for(unique_execution_id());
    let Some((_state, runtime)) = execution_runtime_or_skip(
        "provision_reconcile_and_destroy_preserve_execution_isolation",
        &execution_labels,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("the already-probed Docker daemon remains connectable");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
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
            labels: control_label_map(&execution_labels),
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
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    labels: Some(control_label_map(&execution_labels)),
                    ..Default::default()
                },
            )
            .await
            .expect("create unrelated control container");
        docker
            .create_volume(CreateVolumeOptions {
                name: unrelated_volume.clone(),
                driver: "local".to_owned(),
                labels: control_label_map(&execution_labels),
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

    scope.cleanup().await.expect("cleanup lifecycle resources");
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
    let execution_labels = labels_for(unique_execution_id());
    let Some((_state, runtime)) = execution_runtime_or_skip(
        "image_tmpfs_rejects_writes_past_the_execution_disk_budget",
        &execution_labels,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
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
    let handle = runtime
        .provision(&execution_labels, &requirement, &[service])
        .await
        .expect("provision bounded tmpfs service");
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

    scope
        .cleanup()
        .await
        .expect("cleanup disk-boundary resources");
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
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
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
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    cmd: Some(vec!["sleep".to_owned(), "infinity".to_owned()]),
                    labels: Some(control_label_map(&execution_labels)),
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

    scope
        .cleanup()
        .await
        .expect("cleanup aggregate-failure resources");
}

#[tokio::test]
async fn provisioning_failure_reports_rollback_failure_and_leaks_no_anonymous_volume() {
    let execution_labels = labels_for(unique_execution_id());
    let Some((_state, runtime)) = execution_runtime_or_skip(
        "provisioning_failure_reports_rollback_failure_and_leaks_no_anonymous_volume",
        &execution_labels,
    )
    .await
    else {
        return;
    };
    let docker = raw_client().expect("connect to probed daemon");
    let mut scope = DockerTestScope::new(&runtime, &execution_labels);
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
                Config::<String> {
                    image: Some("alpine:3.20".to_owned()),
                    cmd: Some(vec!["sleep".to_owned(), "infinity".to_owned()]),
                    labels: Some(control_label_map(&execution_labels)),
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
    let new_anonymous_volumes = volumes_after_failure
        .difference(&volumes_before_failure)
        .filter(|name| {
            name.len() == 64 && name.chars().all(|character| character.is_ascii_hexdigit())
        })
        .collect::<Vec<_>>();
    assert!(
        new_anonymous_volumes.is_empty(),
        "failed provisioning must not leave anonymous image volumes: {new_anonymous_volumes:?}"
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

    scope
        .cleanup()
        .await
        .expect("cleanup rollback-failure resources");
}
