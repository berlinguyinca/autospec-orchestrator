//! `autospec-worker` — authenticated worker registration and heartbeat daemon.

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use orchestrator_core::{
    RuntimeKind, WorkerCapabilities, WorkerCapabilityProof, WorkerId, WorkerRegistration,
    WorkerState, API_VERSION,
};
use std::{fmt, time::Duration};

struct Secret(String);

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Parser)]
#[command(name = "autospec-worker", version, about)]
struct Cli {
    #[arg(
        long,
        env = "AUTOSPEC_ORCHESTRATOR_URL",
        default_value = "http://127.0.0.1:8420"
    )]
    controller: String,
    #[arg(long, env = "AUTOSPEC_WORKER_ID")]
    worker_id: String,
    #[arg(long, env = "AUTOSPEC_WORKER_TOKEN")]
    worker_token: String,
    #[arg(long, env = "AUTOSPEC_WORKER_CONCURRENCY", default_value_t = 2)]
    concurrency: u32,
    #[arg(long, default_value_t = 1024)]
    memory_mib: u64,
    #[arg(long, default_value_t = 20)]
    disk_gib: u64,
    #[arg(long)]
    storage_backend: Option<String>,
    #[arg(long)]
    storage_pool_identity: Option<String>,
    #[arg(long)]
    docker_daemon_id: Option<String>,
    #[arg(long)]
    docker_verifier: Option<String>,
    #[arg(long)]
    docker_method_version: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let mut worker = registration(&cli)?;
    let token = Secret(cli.worker_token);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    tracing::info!(
        worker_id = %worker.id,
        controller = %cli.controller,
        state = ?worker.state,
        "autospec-worker starting"
    );
    let workers_url = format!(
        "{}/api/{API_VERSION}/workers",
        cli.controller.trim_end_matches('/')
    );
    let heartbeat_url = format!("{}/{}/heartbeat", workers_url, worker.id);
    let mut registered = false;
    let mut backoff = Duration::from_secs(1);
    loop {
        worker.last_heartbeat = Utc::now();
        let request = if registered {
            client.post(&heartbeat_url)
        } else {
            client.post(&workers_url)
        };
        let sent = request
            .bearer_auth(&token.0)
            .json(&worker)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);
        match sent {
            Ok(_) => {
                registered = true;
                backoff = Duration::from_secs(1);
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(30)) => {}
                    signal = tokio::signal::ctrl_c() => {
                        signal.context("failed to listen for shutdown")?;
                        break;
                    }
                }
            }
            Err(error) => {
                tracing::warn!(worker_id = %worker.id, %error, retry_seconds = backoff.as_secs(), "worker control-plane request failed");
                tokio::select! {
                    () = tokio::time::sleep(backoff) => {}
                    signal = tokio::signal::ctrl_c() => {
                        signal.context("failed to listen for shutdown")?;
                        break;
                    }
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
    Ok(())
}

fn registration(cli: &Cli) -> Result<WorkerRegistration> {
    if cli.concurrency == 0 || cli.concurrency > 64 {
        anyhow::bail!("worker concurrency must be between 1 and 64");
    }
    let proof = match (
        &cli.storage_backend,
        &cli.storage_pool_identity,
        &cli.docker_daemon_id,
        &cli.docker_verifier,
        &cli.docker_method_version,
    ) {
        (Some(backend), Some(pool), Some(daemon), Some(verifier), Some(method)) => {
            Some(WorkerCapabilityProof {
                storage_backend: backend.clone(),
                storage_pool_identity: pool.clone(),
                docker_daemon_id: daemon.clone(),
                docker_verifier: verifier.clone(),
                docker_method_version: method.clone(),
            })
        }
        _ => None,
    };
    let ready = proof
        .as_ref()
        .is_some_and(WorkerCapabilityProof::is_complete);
    Ok(WorkerRegistration {
        id: WorkerId::new(&cli.worker_id),
        capabilities: WorkerCapabilities {
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            cpu: std::thread::available_parallelism()
                .map(|count| u32::try_from(count.get()).unwrap_or(u32::MAX))
                .unwrap_or(1),
            memory_mib: cli.memory_mib,
            disk_gib: cli.disk_gib,
            runtimes: if ready {
                vec![RuntimeKind::Docker]
            } else {
                Vec::new()
            },
            capabilities: if ready {
                vec!["docker".to_owned()]
            } else {
                Vec::new()
            },
            max_concurrent_executions: cli.concurrency,
        },
        state: if ready {
            WorkerState::Ready
        } else {
            WorkerState::Offline
        },
        running_executions: 0,
        last_heartbeat: Utc::now(),
        capability_proof: proof,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli() -> Cli {
        Cli {
            controller: "http://127.0.0.1:8420".into(),
            worker_id: "worker-1".into(),
            worker_token: "secret".into(),
            concurrency: 2,
            memory_mib: 4096,
            disk_gib: 100,
            storage_backend: None,
            storage_pool_identity: None,
            docker_daemon_id: None,
            docker_verifier: None,
            docker_method_version: None,
        }
    }

    #[test]
    fn missing_storage_proof_forces_offline_and_token_debug_is_redacted() {
        let worker = registration(&cli()).unwrap();
        assert_eq!(worker.state, WorkerState::Offline);
        assert!(!worker.has_capacity());
        assert_eq!(format!("{:?}", Secret("do-not-log".into())), "[REDACTED]");
    }

    #[test]
    fn complete_storage_and_docker_proof_allows_ready() {
        let mut cli = cli();
        cli.storage_backend = Some("apfs".into());
        cli.storage_pool_identity = Some("pool".into());
        cli.docker_daemon_id = Some("daemon".into());
        cli.docker_verifier = Some("probe".into());
        cli.docker_method_version = Some("v1".into());
        assert_eq!(registration(&cli).unwrap().state, WorkerState::Ready);
    }
}
