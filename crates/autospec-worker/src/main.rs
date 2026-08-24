//! `autospec-worker` — the per-host execution daemon.

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "autospec-worker", version, about)]
struct Cli {
    /// Orchestrator controller to register with.
    #[arg(
        long,
        env = "AUTOSPEC_ORCHESTRATOR_URL",
        default_value = "http://127.0.0.1:8420"
    )]
    controller: String,

    /// Worker identity advertised during registration.
    #[arg(long, env = "AUTOSPEC_WORKER_ID")]
    worker_id: Option<String>,

    /// Maximum executions to run concurrently on this host.
    #[arg(long, env = "AUTOSPEC_WORKER_CONCURRENCY", default_value_t = 2)]
    concurrency: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    tracing::info!(
        controller = %cli.controller,
        concurrency = cli.concurrency,
        "autospec-worker starting"
    );
    tracing::warn!("worker registration loop is not implemented yet");
    Ok(())
}
