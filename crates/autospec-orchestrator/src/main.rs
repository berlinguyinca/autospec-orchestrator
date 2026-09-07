//! `autospec-orchestrator` — the execution-plane controller.

use anyhow::Result;
use clap::Parser;
use orchestrator_api::{serve, ServerConfig};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "autospec-orchestrator", version, about)]
struct Cli {
    /// Address to bind the HTTP API on.
    #[arg(
        long,
        env = "AUTOSPEC_ORCHESTRATOR_ADDR",
        default_value = "127.0.0.1:8420"
    )]
    addr: String,

    /// Controller PostgreSQL data source name.
    #[arg(long, env = "AUTOSPEC_DATABASE_URL")]
    database_url: String,

    /// Bearer token accepted from API clients.
    #[arg(long, env = "AUTOSPEC_API_TOKEN")]
    api_token: String,

    /// Bearer token accepted from execution workers.
    #[arg(long, env = "AUTOSPEC_WORKER_TOKEN")]
    worker_token: String,

    /// Durable state root shared by worktrees, Pi sessions, and artifacts.
    #[arg(long, env = "AUTOSPEC_STATE_ROOT", default_value = "/var/lib/autospec")]
    state_root: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    serve(ServerConfig {
        addr: cli.addr,
        database_url: cli.database_url,
        state_root: cli.state_root,
        api_token: cli.api_token,
        worker_token: cli.worker_token,
    })
    .await
}
