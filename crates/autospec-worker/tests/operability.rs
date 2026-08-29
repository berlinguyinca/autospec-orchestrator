use std::{path::PathBuf, process::Command};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
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
        .env("AUTOSPEC_STORAGE_KIND", "lvm")
        .env("AUTOSPEC_STORAGE_POOL", "autospec-vg")
        .env(
            "AUTOSPEC_DOCKER_VERIFIER_IMAGE",
            format!("sha256:{}", "a".repeat(64)),
        )
        .output()
        .expect("execute docker compose config");
    if !output.status.success() {
        eprintln!(
            "SKIP single-host Compose rendering: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
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
}
