//! `autospec-orchestrator` — the execution-plane controller.

use anyhow::Result;
use clap::Parser;
use orchestrator_api::serve;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    serve(&cli.addr).await
}
