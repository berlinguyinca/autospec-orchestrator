#[path = "../cli.rs"]
mod cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run_from_env().await
}
