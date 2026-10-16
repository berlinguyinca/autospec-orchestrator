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

    /// Legacy shared bearer token accepted from every execution worker. Used
    /// only together with `--allow-shared-worker-token`.
    #[arg(long, env = "AUTOSPEC_WORKER_TOKEN", hide_env_values = true)]
    worker_token: Option<String>,

    /// File of per-worker token digests, one `<worker_id> <sha256-hex-of-token>`
    /// per line (`#` comments). Each worker's token is accepted for that
    /// worker only. Must not be readable by group or other (chmod 600).
    #[arg(long, env = "AUTOSPEC_WORKER_TOKENS_FILE")]
    worker_tokens_file: Option<PathBuf>,

    /// Authenticate every worker with the one shared `--worker-token` instead
    /// of per-worker tokens. Without `--worker-tokens-file` the controller
    /// refuses to start unless this is given.
    #[arg(long, env = "AUTOSPEC_ALLOW_SHARED_WORKER_TOKEN")]
    allow_shared_worker_token: bool,

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

    serve(server_config(Cli::parse())).await
}

fn server_config(cli: Cli) -> ServerConfig {
    ServerConfig {
        addr: cli.addr,
        database_url: cli.database_url,
        state_root: cli.state_root,
        api_token: cli.api_token,
        worker_token: cli.worker_token,
        worker_tokens_file: cli.worker_tokens_file,
        allow_shared_worker_token: cli.allow_shared_worker_token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> ServerConfig {
        let mut args = vec![
            "autospec-orchestrator",
            "--database-url",
            "postgres://unused",
            "--api-token",
            "api",
        ];
        args.extend_from_slice(extra);
        server_config(Cli::try_parse_from(args).expect("valid arguments"))
    }

    #[test]
    fn worker_credential_flags_reach_the_server_config() {
        let config = parse(&["--worker-tokens-file", "/etc/autospec/worker-tokens"]);
        assert_eq!(
            config.worker_tokens_file.as_deref(),
            Some(std::path::Path::new("/etc/autospec/worker-tokens"))
        );
        assert!(!config.allow_shared_worker_token);
        assert_eq!(config.worker_token, None);

        let config = parse(&["--worker-token", "shared", "--allow-shared-worker-token"]);
        assert!(config.allow_shared_worker_token);
        assert_eq!(config.worker_token.as_deref(), Some("shared"));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn no_worker_flags_parse_but_fail_validation_naming_the_fix() {
        let config = parse(&[]);
        assert!(!config.allow_shared_worker_token && config.worker_tokens_file.is_none());
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("--worker-tokens-file"), "{error}");
        assert!(error.contains("--allow-shared-worker-token"), "{error}");
    }

    #[test]
    fn the_shared_token_alone_no_longer_validates() {
        let error = parse(&["--worker-token", "shared"])
            .validate()
            .unwrap_err()
            .to_string();
        assert!(error.contains("--allow-shared-worker-token"), "{error}");
    }
}
