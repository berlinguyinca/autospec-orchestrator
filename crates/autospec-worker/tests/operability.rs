use std::{
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::Duration,
};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn compose_command(compose: &Path, project: &str, proxy_port: u16) -> Command {
    let mut command = Command::new("docker");
    command
        .args(["compose", "-p", project, "-f"])
        .arg(compose)
        .args(["--profile", "host-docker-worker"])
        .env("AUTOSPEC_API_TOKEN", "test-api-token")
        .env("AUTOSPEC_WORKER_TOKEN", "test-worker-token")
        .env("AUTOSPEC_POSTGRES_PASSWORD", "test-postgres-password")
        .env("AUTOSPEC_WORKER_ID", "test-worker")
        .env("AUTOSPEC_DEPLOYMENT_ID", project)
        .env("AUTOSPEC_WORKER_HOST_DOCKER", "true")
        .env("AUTOSPEC_STORAGE_KIND", "lvm")
        .env("AUTOSPEC_STORAGE_POOL", "autospec-vg")
        .env(
            "AUTOSPEC_DOCKER_VERIFIER_IMAGE",
            format!("sha256:{}", "a".repeat(64)),
        )
        .env("AUTOSPEC_DOCKER_PROXY_PORT", proxy_port.to_string());
    command
}

struct ComposeScope {
    compose: PathBuf,
    project: String,
    proxy_port: u16,
}

impl Drop for ComposeScope {
    fn drop(&mut self) {
        let _ = compose_command(&self.compose, &self.project, self.proxy_port)
            .args(["down", "--volumes", "--remove-orphans"])
            .output();
    }
}

#[test]
fn storage_pool_provisioning_is_dry_run_first_and_requires_exact_confirmation() {
    let script = root().join("deploy/provision-storage-pool.sh");
    let planned = Command::new(&script)
        .args(["plan", "lvm", "autospec-vg", "/var/lib/autospec"])
        .output()
        .expect("execute storage provisioning plan");
    assert!(planned.status.success());
    let plan = String::from_utf8(planned.stdout).unwrap();
    assert!(plan.contains("AUTOSPEC_LVM_VOLUME_GROUP=autospec-vg"));
    assert!(plan.contains("/var/lib/autospec"));

    let unconfirmed = Command::new(&script)
        .args(["apply", "lvm", "autospec-vg", "/var/lib/autospec"])
        .output()
        .expect("execute rejected storage provisioning apply");
    assert!(!unconfirmed.status.success());
    assert!(String::from_utf8_lossy(&unconfirmed.stderr).contains("AUTOSPEC_STORAGE_CONFIRM"));
}

#[test]
fn compose_renders_without_exposing_the_host_docker_socket_to_the_worker() {
    let compose = root().join("deploy/docker-compose.yml");
    let output = Command::new("docker")
        .args(["compose", "-f"])
        .arg(&compose)
        .args([
            "--profile",
            "host-docker-worker",
            "config",
            "--format",
            "json",
        ])
        .env("AUTOSPEC_API_TOKEN", "test-api-token")
        .env("AUTOSPEC_WORKER_TOKEN", "test-worker-token")
        .env("AUTOSPEC_POSTGRES_PASSWORD", "test-postgres-password")
        .env("AUTOSPEC_WORKER_ID", "test-worker")
        .env("AUTOSPEC_DEPLOYMENT_ID", "deployment-test")
        .env("AUTOSPEC_WORKER_HOST_DOCKER", "true")
        .env("AUTOSPEC_STORAGE_KIND", "lvm")
        .env("AUTOSPEC_STORAGE_POOL", "autospec-vg")
        .env(
            "AUTOSPEC_DOCKER_VERIFIER_IMAGE",
            format!("sha256:{}", "a".repeat(64)),
        )
        .output()
        .expect("execute docker compose config");
    assert!(
        output.status.success(),
        "docker compose config failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let worker = &config["services"]["worker"];
    let mounts = worker["volumes"].as_array().cloned().unwrap_or_default();
    assert!(mounts.iter().all(|mount| {
        !mount.to_string().contains("/var/run/docker.sock")
            && !mount.to_string().contains("/run/podman/podman.sock")
    }));
    assert_eq!(
        worker["environment"]["DOCKER_HOST"],
        "tcp://docker-api:2375"
    );
    assert_eq!(
        config["services"]["docker-api"]["ports"][0]["host_ip"],
        "127.0.0.1"
    );
    assert_eq!(
        config["services"]["docker-api"]["ports"][0]["published"],
        "2375"
    );
    assert_eq!(config["services"]["docker-api"]["ports"][0]["target"], 2375);
    for service in config["services"].as_object().unwrap().values() {
        assert_eq!(service["labels"]["autospec.managed"], "true");
        assert_eq!(
            service["labels"]["autospec.execution_id"],
            "deployment-test"
        );
    }
    for resources in ["networks", "volumes"] {
        for resource in config[resources].as_object().unwrap().values() {
            assert_eq!(resource["labels"]["autospec.managed"], "true");
            assert_eq!(
                resource["labels"]["autospec.execution_id"],
                "deployment-test"
            );
        }
    }
}

#[test]
fn compose_proxy_is_reachable_only_through_its_host_loopback_publication() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve loopback test port");
    let proxy_port = listener.local_addr().unwrap().port();
    drop(listener);
    let compose = root().join("deploy/docker-compose.yml");
    let project = format!("autospec-proxy-test-{}-{proxy_port}", std::process::id());
    let scope = ComposeScope {
        compose: compose.clone(),
        project: project.clone(),
        proxy_port,
    };
    let started = compose_command(&compose, &project, proxy_port)
        .args(["up", "-d", "--no-build", "docker-api"])
        .output()
        .expect("start constrained Docker proxy");
    assert!(
        started.status.success(),
        "start constrained Docker proxy: {}",
        String::from_utf8_lossy(&started.stderr)
    );

    let endpoint = format!("tcp://127.0.0.1:{proxy_port}");
    let mut connected = false;
    for _ in 0..20 {
        connected = Command::new("docker")
            .args(["-H", &endpoint, "info", "--format", "{{.ID}}"])
            .output()
            .is_ok_and(|output| output.status.success() && !output.stdout.is_empty());
        if connected {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        connected,
        "host worker cannot reach the loopback Docker proxy"
    );

    let inspected = Command::new("docker")
        .args([
            "inspect",
            &format!("{project}-docker-api-1"),
            "--format",
            "{{json .HostConfig.PortBindings}}",
        ])
        .output()
        .expect("inspect constrained Docker proxy");
    assert!(inspected.status.success());
    let bindings = String::from_utf8(inspected.stdout).unwrap();
    assert!(bindings.contains("127.0.0.1"));
    assert!(!bindings.contains("0.0.0.0"));
    drop(scope);
}
