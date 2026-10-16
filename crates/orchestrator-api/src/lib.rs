//! Versioned HTTP surface of the execution plane (spec sections 21, 75).
//!
//! Every client — AutoSpec, Workbench, the CLI, CI — uses this one API. There is
//! no second execution engine anywhere in the ecosystem (spec invariant 14).

use anyhow::Result;
mod artifacts;
mod auth;
mod error;
mod events;
mod executions;
mod interactive;
mod operations;
mod state;
mod workers;

use axum::{routing::get, Router};
use orchestrator_core::API_VERSION;
use orchestrator_persistence::{
    PgArtifactStore, PgCleanupAuthorityStore, PgEventLog, PgExecutionStore, PgReservationStore,
    PgWorkerStore,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::net::TcpListener;

pub use auth::{
    ApiTokenValidator, PerWorkerTokens, SharedWorkerToken, StaticApiTokenValidator, WorkerIdentity,
    WorkerTokenValidator,
};
pub use state::AppState;

pub fn api_root() -> String {
    format!("/api/{API_VERSION}")
}

pub struct ServerConfig {
    pub addr: String,
    pub database_url: String,
    pub state_root: PathBuf,
    pub api_token: String,
    /// The legacy shared worker bearer token. Used only with
    /// `allow_shared_worker_token`; never together with `worker_tokens_file`.
    pub worker_token: Option<String>,
    /// File of per-worker token digests (`<worker_id> <sha256-hex>` per line).
    pub worker_tokens_file: Option<PathBuf>,
    /// Explicit operator choice to authenticate every worker with one shared
    /// token (spec section 5 R-ORCH replaces it with per-worker credentials).
    pub allow_shared_worker_token: bool,
}

/// How workers authenticate to the controller, decided once at startup.
pub enum WorkerAuth {
    /// Per-worker tokens: each speaks for the worker it was issued to.
    PerWorker(PerWorkerTokens),
    /// The legacy shared token (explicit opt-in only).
    Shared(String),
}

impl ServerConfig {
    /// Checks the configuration without touching the network or database.
    ///
    /// Exactly one worker credential source must be active: a tokens file, or
    /// the shared token together with `allow_shared_worker_token`. Neither, or
    /// both, is refused so an upgrade never leaves workers authenticating in a
    /// way the operator did not choose.
    pub fn validate(&self) -> Result<()> {
        if self.api_token.is_empty() {
            anyhow::bail!("API bearer token must not be empty");
        }
        let shared = self
            .worker_token
            .as_deref()
            .filter(|token| !token.is_empty());
        if self.worker_token.is_some() && shared.is_none() {
            anyhow::bail!("worker bearer token must not be empty");
        }
        match (&self.worker_tokens_file, self.allow_shared_worker_token) {
            (Some(_), true) => anyhow::bail!(
                "--worker-tokens-file and --allow-shared-worker-token are mutually exclusive; \
                 choose per-worker tokens or the shared token, not both"
            ),
            (Some(_), false) if shared.is_some() => anyhow::bail!(
                "--worker-tokens-file and --worker-token are mutually exclusive; \
                 remove the shared worker token"
            ),
            (Some(path), false) => check_tokens_file(path)?,
            (None, true) if shared.is_none() => anyhow::bail!(
                "--allow-shared-worker-token needs the shared token in --worker-token \
                 (AUTOSPEC_WORKER_TOKEN)"
            ),
            (None, true) => {}
            (None, false) => anyhow::bail!(
                "no worker credential source: set --worker-tokens-file \
                 (AUTOSPEC_WORKER_TOKENS_FILE) for per-worker tokens, or pass \
                 --allow-shared-worker-token with --worker-token to keep one shared \
                 token for every worker"
            ),
        }
        Ok(())
    }

    /// Resolves the worker credential source. Reads the tokens file once.
    pub fn worker_auth(&self) -> Result<WorkerAuth> {
        self.validate()?;
        match (&self.worker_tokens_file, &self.worker_token) {
            (Some(path), _) => Ok(WorkerAuth::PerWorker(load_worker_tokens(path)?)),
            (None, Some(token)) => Ok(WorkerAuth::Shared(token.clone())),
            (None, None) => anyhow::bail!("no worker credential source"),
        }
    }
}

/// Opens the digest file, refuses a group- or world-accessible file on Unix,
/// and parses it. The mode is read from the opened handle, so the file cannot
/// be swapped between the check and the read. Errors name the path and the
/// failing line only, never a digest.
fn load_worker_tokens(path: &std::path::Path) -> Result<PerWorkerTokens> {
    use std::io::Read;
    let file = std::fs::File::open(path)
        .map_err(|error| anyhow::anyhow!("open worker tokens file {}: {error}", path.display()))?;
    check_tokens_file_handle(&file, path)?;
    let mut text = String::new();
    file.take(1024 * 1024)
        .read_to_string(&mut text)
        .map_err(|error| anyhow::anyhow!("read worker tokens file {}: {error}", path.display()))?;
    PerWorkerTokens::parse(&text).map_err(|error| anyhow::anyhow!("{} ({})", error, path.display()))
}

fn check_tokens_file(path: &std::path::Path) -> Result<()> {
    let file = std::fs::File::open(path)
        .map_err(|error| anyhow::anyhow!("open worker tokens file {}: {error}", path.display()))?;
    check_tokens_file_handle(&file, path)
}

fn check_tokens_file_handle(file: &std::fs::File, path: &std::path::Path) -> Result<()> {
    let metadata = file
        .metadata()
        .map_err(|error| anyhow::anyhow!("stat worker tokens file {}: {error}", path.display()))?;
    if !metadata.is_file() {
        anyhow::bail!(
            "worker tokens file {} is not a regular file",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            anyhow::bail!(
                "worker tokens file {} is accessible to group or other (mode {mode:04o}); \
                 run `chmod 600` on it",
                path.display()
            );
        }
    }
    Ok(())
}

/// Router for the orchestrator controller.
///
/// Planned routes under `/api/v1`:
/// `POST   /executions`            create an execution
/// `GET    /executions/{id}`       read authoritative execution state
/// `POST   /executions/{id}/cancel`
/// `POST   /executions/{id}/retry`
/// `GET    /executions/{id}/events` event stream (SSE)
/// `GET    /executions/{id}/artifacts`
/// `GET    /workers`               registered workers and capacity
/// `POST   /workers`               worker registration and heartbeat
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .nest(
            &api_root(),
            Router::new()
                .merge(executions::routes())
                .merge(interactive::routes())
                .merge(events::routes())
                .merge(artifacts::routes())
                .merge(operations::routes())
                .merge(workers::routes()),
        )
        .with_state(state)
}

/// Bind and serve the controller API.
pub async fn serve(config: ServerConfig) -> Result<()> {
    // Resolve the worker credential source first: a bad or missing source must
    // stop the process before it opens a database connection or a socket.
    let worker_auth = config.worker_auth()?;
    let (initial_worker_token, per_worker) = match worker_auth {
        WorkerAuth::Shared(token) => (token, None),
        WorkerAuth::PerWorker(tokens) => (String::new(), Some(tokens)),
    };
    let mut state = AppState::new(
        Arc::new(PgExecutionStore::connect(&config.database_url).await?),
        Arc::new(PgEventLog::connect(&config.database_url).await?),
        Arc::new(PgWorkerStore::connect(&config.database_url).await?),
        Arc::new(PgReservationStore::connect(&config.database_url).await?),
        Arc::new(PgArtifactStore::connect(&config.database_url, config.state_root).await?),
        config.api_token,
        initial_worker_token,
    )
    .with_cleanup_authorities(Arc::new(
        PgCleanupAuthorityStore::connect(&config.database_url).await?,
    ));
    if let Some(tokens) = per_worker {
        // Replaces the shared validator outright: no shared-token fallback.
        state = state.with_worker_tokens(tokens);
    }
    let reaper = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(error) = reaper.reap_stale(chrono::Utc::now()).await {
                tracing::error!(%error, "worker heartbeat reaper failed");
            }
            if let Err(error) = reaper.recover_stranded().await {
                tracing::error!(%error, "stranded worker recovery failed");
            }
        }
    });
    let listener = TcpListener::bind(&config.addr).await?;
    tracing::info!(addr = %config.addr, api = %api_root(), "orchestrator controller listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_is_versioned_from_the_start() {
        assert_eq!(api_root(), "/api/v1");
    }

    use std::os::unix::fs::PermissionsExt;

    fn config(
        api_token: &str,
        worker_token: Option<&str>,
        file: Option<&std::path::Path>,
        allow_shared: bool,
    ) -> ServerConfig {
        ServerConfig {
            addr: "127.0.0.1:0".into(),
            database_url: "postgres://unused".into(),
            state_root: "/tmp/unused".into(),
            api_token: api_token.into(),
            worker_token: worker_token.map(str::to_owned),
            worker_tokens_file: file.map(std::path::Path::to_path_buf),
            allow_shared_worker_token: allow_shared,
        }
    }

    fn tokens_file(mode: u32) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("worker-tokens");
        let digest = "ab".repeat(32);
        std::fs::write(&path, format!("w1 {digest}\n")).expect("write tokens file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        (dir, path)
    }

    #[test]
    fn server_config_rejects_empty_api_and_worker_tokens() {
        assert!(config("", Some("worker"), None, true).validate().is_err());
        assert!(config("api", Some(""), None, true).validate().is_err());
        assert!(config("api", Some("worker"), None, true).validate().is_ok());
    }

    #[test]
    fn server_config_requires_a_worker_credential_source_and_names_the_flags() {
        let error = config("api", None, None, false)
            .validate()
            .unwrap_err()
            .to_string();
        for flag in ["--worker-tokens-file", "--allow-shared-worker-token"] {
            assert!(error.contains(flag), "{error}");
        }
        // The shared token alone is no longer enough: it needs the explicit flag.
        let error = config("api", Some("worker"), None, false)
            .validate()
            .unwrap_err()
            .to_string();
        assert!(error.contains("--allow-shared-worker-token"), "{error}");
        // The flag without a token has nothing to authenticate with.
        assert!(config("api", None, None, true).validate().is_err());
    }

    #[test]
    fn server_config_accepts_a_private_tokens_file_and_refuses_group_or_other_access() {
        let (_dir, private) = tokens_file(0o600);
        assert!(config("api", None, Some(&private), false)
            .validate()
            .is_ok());
        for mode in [0o644, 0o640, 0o604, 0o660, 0o666, 0o700 | 0o010] {
            let (_dir, path) = tokens_file(mode);
            let error = config("api", None, Some(&path), false)
                .validate()
                .expect_err("group/other-accessible file");
            assert!(error.to_string().contains("chmod 600"), "{mode:o}: {error}");
        }
        assert!(config(
            "api",
            None,
            Some(std::path::Path::new("/nonexistent/tokens")),
            false
        )
        .validate()
        .is_err());
        let dir = tempfile::tempdir().unwrap();
        assert!(config("api", None, Some(dir.path()), false)
            .validate()
            .is_err());
    }

    #[test]
    fn server_config_refuses_both_sources() {
        let (_dir, path) = tokens_file(0o600);
        assert!(config("api", Some("worker"), Some(&path), false)
            .validate()
            .is_err());
        assert!(config("api", Some("worker"), Some(&path), true)
            .validate()
            .is_err());
        assert!(config("api", None, Some(&path), true).validate().is_err());
    }

    #[test]
    fn worker_auth_loads_the_digest_file_or_the_explicit_shared_token() {
        let (_dir, path) = tokens_file(0o600);
        let per_worker = config("api", None, Some(&path), false)
            .worker_auth()
            .unwrap();
        let WorkerAuth::PerWorker(tokens) = per_worker else {
            panic!("a tokens file selects per-worker auth");
        };
        assert_eq!(format!("{tokens:?}"), "PerWorkerTokens(1 workers)");
        let shared = config("api", Some("worker"), None, true)
            .worker_auth()
            .unwrap();
        assert!(matches!(shared, WorkerAuth::Shared(token) if token == "worker"));
        // A malformed file fails startup without echoing its contents.
        let (_dir, bad) = tokens_file(0o600);
        std::fs::write(&bad, "w1 not-a-digest\n").unwrap();
        let error = match config("api", None, Some(&bad), false).worker_auth() {
            Ok(_) => panic!("malformed file must fail"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("line 1") && !error.contains("not-a-digest"),
            "{error}"
        );
    }
}
