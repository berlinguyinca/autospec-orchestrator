use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use orchestrator_core::API_VERSION;
use reqwest::{Response, Url};
use std::{fmt, io::Write, time::Duration};

const MAX_RESPONSE_BYTES: usize = 1_048_576;

struct Secret(String);

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "autospecctl",
    version,
    about = "Authenticated execution-plane operator CLI"
)]
struct OperatorCli {
    #[arg(
        long,
        env = "AUTOSPEC_ORCHESTRATOR_URL",
        default_value = "http://127.0.0.1:8420"
    )]
    controller: String,
    #[arg(long, env = "AUTOSPEC_API_TOKEN")]
    api_token: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List registered worker capacity and sanitized health failures.
    Workers,
    /// List bounded live execution metadata (never manifests or task packets).
    Executions {
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,
    },
    /// Show exact live execution-state counts.
    Queue,
    /// Show durable unresolved cleanup counts and age.
    CleanupHealth,
}

pub async fn run_from_env() -> Result<()> {
    run(OperatorCli::parse()).await
}

async fn run(cli: OperatorCli) -> Result<()> {
    let url = endpoint(&cli.controller, &cli.command)?;
    let token = Secret(cli.api_token);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let response = client.get(url).bearer_auth(&token.0).send().await?;
    let status = response.status();
    let body = read_bounded(response).await?;
    if !status.is_success() {
        anyhow::bail!(
            "controller returned {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let value: serde_json::Value =
        serde_json::from_slice(&body).context("controller returned invalid JSON")?;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, &value)?;
    writeln!(output)?;
    Ok(())
}

fn endpoint(controller: &str, command: &Command) -> Result<String> {
    let base = Url::parse(controller).context("invalid controller URL")?;
    anyhow::ensure!(
        matches!(base.scheme(), "http" | "https"),
        "controller URL must use http or https"
    );
    anyhow::ensure!(
        base.username().is_empty() && base.password().is_none(),
        "controller URL must not contain credentials"
    );
    anyhow::ensure!(
        base.path().is_empty() || base.path() == "/",
        "controller URL must not contain a path"
    );
    anyhow::ensure!(
        base.query().is_none() && base.fragment().is_none(),
        "controller URL must not contain a query or fragment"
    );
    let root = controller.trim_end_matches('/');
    let suffix = match command {
        Command::Workers => "workers".to_owned(),
        Command::Executions { limit } => {
            anyhow::ensure!((1..=100).contains(limit), "limit must be between 1 and 100");
            format!("operator/executions?limit={limit}")
        }
        Command::Queue => "operator/queue".to_owned(),
        Command::CleanupHealth => "operator/cleanup-health".to_owned(),
    };
    Ok(format!("{root}/api/{API_VERSION}/{suffix}"))
}

async fn read_bounded(mut response: Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        append_bounded(&mut body, &chunk)?;
    }
    Ok(body)
}

fn append_bounded(body: &mut Vec<u8>, chunk: &[u8]) -> Result<()> {
    anyhow::ensure!(
        body.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES,
        "controller response exceeds one mebibyte"
    );
    body.extend_from_slice(chunk);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_map_only_to_bounded_authenticated_operator_endpoints() {
        let base = "http://127.0.0.1:8420";
        assert_eq!(
            endpoint(base, &Command::Workers).unwrap(),
            "http://127.0.0.1:8420/api/v1/workers"
        );
        assert_eq!(
            endpoint(base, &Command::Executions { limit: 25 }).unwrap(),
            "http://127.0.0.1:8420/api/v1/operator/executions?limit=25"
        );
        assert_eq!(
            endpoint(base, &Command::Queue).unwrap(),
            "http://127.0.0.1:8420/api/v1/operator/queue"
        );
        assert_eq!(
            endpoint(base, &Command::CleanupHealth).unwrap(),
            "http://127.0.0.1:8420/api/v1/operator/cleanup-health"
        );
        assert!(endpoint("file:///tmp/controller", &Command::Workers).is_err());
        assert!(endpoint("http://user:secret@localhost", &Command::Workers).is_err());
        assert!(endpoint("http://localhost?token=secret", &Command::Workers).is_err());
        assert!(endpoint("http://localhost#fragment", &Command::Workers).is_err());
    }

    #[test]
    fn response_accumulator_rejects_more_than_one_mebibyte() {
        let mut body = Vec::new();
        append_bounded(&mut body, &[0; 1_048_576]).unwrap();
        assert!(append_bounded(&mut body, &[1]).is_err());
    }

    #[test]
    fn bearer_token_debug_is_redacted() {
        assert_eq!(
            format!("{:?}", Secret("operator-secret".into())),
            "[REDACTED]"
        );
    }
}
