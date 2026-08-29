use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use orchestrator_core::{Execution, ExecutionId};
use runtime_traits::{CredentialBroker, ExecutionCredentials, RuntimeError};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const CREDENTIAL_FILE: &str = "inferweave.credential";

/// Local execution-scoped credential issuer used until an InferWeave issuance
/// endpoint exists. It creates opaque, short-lived material only; it does not
/// choose a model, serve inference, or define InferWeave authorization policy.
#[derive(Debug, Clone)]
pub struct LocalCredentialBroker {
    state_root: PathBuf,
    ttl: Duration,
}

impl LocalCredentialBroker {
    pub fn new(state_root: impl AsRef<Path>, ttl: Duration) -> Result<Self, RuntimeError> {
        if ttl <= Duration::zero() {
            return Err(RuntimeError::Provisioning(
                "credential TTL must be positive".to_owned(),
            ));
        }
        let state_root = state_root.as_ref().canonicalize().map_err(|error| {
            RuntimeError::Provisioning(format!("canonicalize credential state root: {error}"))
        })?;
        Ok(Self { state_root, ttl })
    }

    fn credential_path(&self, id: &ExecutionId) -> Result<PathBuf, RuntimeError> {
        validate_execution_id(id)?;
        Ok(self
            .state_root
            .join("executions")
            .join(id.as_str())
            .join("credentials")
            .join(CREDENTIAL_FILE))
    }

    fn verified_parent(&self, id: &ExecutionId) -> Result<PathBuf, RuntimeError> {
        let path = self.credential_path(id)?;
        let expected = path
            .parent()
            .expect("credential file always has a parent")
            .to_path_buf();
        let parent = expected.canonicalize().map_err(|error| {
            RuntimeError::Provisioning(format!(
                "canonicalize execution credential directory: {error}"
            ))
        })?;
        let execution_root = self
            .state_root
            .join("executions")
            .join(id.as_str())
            .canonicalize()
            .map_err(|error| {
                RuntimeError::Provisioning(format!("canonicalize execution root: {error}"))
            })?;
        if parent != expected || !parent.starts_with(&execution_root) || execution_root == parent {
            return Err(RuntimeError::ResourceLimit(
                "credential path escaped its exact execution root".to_owned(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).map_err(|error| {
                RuntimeError::Provisioning(format!("secure credential directory: {error}"))
            })?;
        }
        Ok(parent)
    }

    fn read_live(&self, path: &Path) -> Result<Option<DateTime<Utc>>, RuntimeError> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(RuntimeError::Provisioning(format!(
                    "inspect execution credential: {error}"
                )))
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(RuntimeError::ResourceLimit(
                "execution credential is not a regular file".to_owned(),
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(RuntimeError::ResourceLimit(
                    "execution credential permissions are not private".to_owned(),
                ));
            }
        }
        let mut text = String::new();
        File::open(path)
            .and_then(|mut file| file.read_to_string(&mut text))
            .map_err(|error| {
                RuntimeError::Provisioning(format!("read execution credential: {error}"))
            })?;
        let expires_at = text
            .lines()
            .nth(1)
            .ok_or_else(|| RuntimeError::ResourceLimit("credential expiry is missing".to_owned()))?
            .parse::<DateTime<Utc>>()
            .map_err(|_| RuntimeError::ResourceLimit("credential expiry is invalid".to_owned()))?;
        Ok((expires_at > Utc::now()).then_some(expires_at))
    }

    fn create(&self, path: &Path, expires_at: DateTime<Utc>) -> Result<(), RuntimeError> {
        let mut entropy = [0u8; 32];
        File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut entropy))
            .map_err(|error| {
                RuntimeError::Unavailable(format!("secure operating-system entropy: {error}"))
            })?;
        let token = entropy
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(|error| {
            RuntimeError::Provisioning(format!("create execution credential: {error}"))
        })?;
        write!(file, "{token}\n{}\n", expires_at.to_rfc3339())
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                RuntimeError::Provisioning(format!("persist execution credential: {error}"))
            })?;
        File::open(path.parent().expect("credential has a parent"))
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                RuntimeError::Provisioning(format!("sync credential directory: {error}"))
            })?;
        Ok(())
    }
}

#[async_trait]
impl CredentialBroker for LocalCredentialBroker {
    async fn mint(&self, execution: &Execution) -> Result<ExecutionCredentials, RuntimeError> {
        let parent = self.verified_parent(&execution.id)?;
        let path = parent.join(CREDENTIAL_FILE);
        if let Some(expires_at) = self.read_live(&path)? {
            return Ok(ExecutionCredentials { path, expires_at });
        }
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(RuntimeError::Provisioning(format!(
                    "remove expired credential: {error}"
                )))
            }
        }
        let expires_at = Utc::now() + self.ttl;
        self.create(&path, expires_at)?;
        Ok(ExecutionCredentials { path, expires_at })
    }

    async fn revoke(&self, id: &ExecutionId) -> Result<(), RuntimeError> {
        let path = self.credential_path(id)?;
        if self.read_live(&path)?.is_none() && !path.exists() {
            return Ok(());
        }
        let parent = match self.verified_parent(id) {
            Ok(parent) => parent,
            Err(_error) if !path.exists() => return Ok(()),
            Err(error) => return Err(error),
        };
        match fs::remove_file(&path) {
            Ok(()) => File::open(&parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    RuntimeError::Cleanup(format!("sync credential revocation: {error}"))
                }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RuntimeError::Cleanup(format!(
                "revoke execution credential: {error}"
            ))),
        }
    }
}

fn validate_execution_id(id: &ExecutionId) -> Result<(), RuntimeError> {
    let value = id.as_str();
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !value.as_bytes()[0].is_ascii_alphanumeric()
    {
        return Err(RuntimeError::ResourceLimit(
            "credential execution id is not a safe path component".to_owned(),
        ));
    }
    Ok(())
}
