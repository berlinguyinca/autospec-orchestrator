use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use orchestrator_core::{Execution, ExecutionId};
use runtime_traits::{CredentialBroker, ExecutionCredentials, RuntimeError};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

const CREDENTIAL_FILE: &str = "inferweave.credential";
static CREDENTIAL_LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

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

    fn random_hex(&self) -> Result<String, RuntimeError> {
        let mut entropy = [0u8; 32];
        File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut entropy))
            .map_err(|error| {
                RuntimeError::Unavailable(format!("secure operating-system entropy: {error}"))
            })?;
        let mut token = String::with_capacity(entropy.len() * 2);
        for byte in entropy {
            write!(token, "{byte:02x}").expect("writing to a String cannot fail");
        }
        Ok(token)
    }

    fn create_candidate(
        &self,
        parent: &Path,
        expires_at: DateTime<Utc>,
    ) -> Result<PathBuf, RuntimeError> {
        let token = self.random_hex()?;
        let suffix = self.random_hex()?;
        let path = parent.join(format!(".{CREDENTIAL_FILE}.{suffix}.tmp"));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&path).map_err(|error| {
            RuntimeError::Provisioning(format!("create execution credential: {error}"))
        })?;
        write!(file, "{token}\n{}\n", expires_at.to_rfc3339())
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                RuntimeError::Provisioning(format!("persist execution credential: {error}"))
            })?;
        Ok(path)
    }

    fn sync_directory(parent: &Path, operation: &str) -> Result<(), RuntimeError> {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| RuntimeError::Provisioning(format!("{operation}: {error}")))
    }

    fn execution_lock(&self, id: &ExecutionId) -> Result<Arc<Mutex<()>>, RuntimeError> {
        let path = self.credential_path(id)?;
        let mut locks = CREDENTIAL_LOCKS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| RuntimeError::Unavailable("credential lock registry poisoned".into()))?;
        Ok(locks
            .entry(path)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
    }
}

#[async_trait]
impl CredentialBroker for LocalCredentialBroker {
    async fn mint(&self, execution: &Execution) -> Result<ExecutionCredentials, RuntimeError> {
        let lock = self.execution_lock(&execution.id)?;
        let _guard = lock
            .lock()
            .map_err(|_| RuntimeError::Unavailable("execution credential lock poisoned".into()))?;
        let parent = self.verified_parent(&execution.id)?;
        let path = parent.join(CREDENTIAL_FILE);
        if let Some(expires_at) = self.read_live(&path)? {
            return Ok(ExecutionCredentials { path, expires_at });
        }
        if fs::symlink_metadata(&path).is_ok() {
            return Err(RuntimeError::ResourceLimit(
                "execution credential is expired and remains bound until cleanup".to_owned(),
            ));
        }
        let expires_at = Utc::now() + self.ttl;
        let candidate = self.create_candidate(&parent, expires_at)?;
        match fs::hard_link(&candidate, &path) {
            Ok(()) => {
                fs::remove_file(&candidate).map_err(|error| {
                    RuntimeError::Provisioning(format!(
                        "remove linked credential candidate: {error}"
                    ))
                })?;
                Self::sync_directory(&parent, "sync credential directory")?;
                Ok(ExecutionCredentials { path, expires_at })
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                fs::remove_file(&candidate).map_err(|remove_error| {
                    RuntimeError::Provisioning(format!(
                        "remove losing credential candidate: {remove_error}"
                    ))
                })?;
                let expires_at = self.read_live(&path)?.ok_or_else(|| {
                    RuntimeError::ResourceLimit(
                        "concurrent credential winner is expired".to_owned(),
                    )
                })?;
                Ok(ExecutionCredentials { path, expires_at })
            }
            Err(error) => Err(RuntimeError::Provisioning(format!(
                "publish execution credential: {error}"
            ))),
        }
    }

    async fn revoke(&self, id: &ExecutionId) -> Result<(), RuntimeError> {
        let lock = self.execution_lock(id)?;
        let _guard = lock
            .lock()
            .map_err(|_| RuntimeError::Unavailable("execution credential lock poisoned".into()))?;
        let path = self.credential_path(id)?;
        let parent = match self.verified_parent(id) {
            Ok(parent) => parent,
            Err(_error)
                if matches!(
                    fs::symlink_metadata(&path),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound
                ) =>
            {
                return Ok(())
            }
            Err(error) => return Err(error),
        };
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(RuntimeError::Cleanup(format!(
                    "inspect execution credential for revocation: {error}"
                )))
            }
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(RuntimeError::ResourceLimit(
                "execution credential revocation authority is not a regular file".to_owned(),
            ));
        }
        match fs::remove_file(&path) {
            Ok(()) => Self::sync_directory(&parent, "sync credential revocation")
                .map_err(|error| RuntimeError::Cleanup(error.to_string())),
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
