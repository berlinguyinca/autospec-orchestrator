use crate::{run_migrations, StoreError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestrator_core::ExecutionId;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

/// Durable artifact metadata. Payload bytes stay in content-addressed storage
/// and are never serialized into execution manifests or Pi task packets
/// (spec sections 97-99).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub execution_id: ExecutionId,
    pub name: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub media_type: String,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn store(
        &self,
        execution_id: &ExecutionId,
        name: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<Artifact, StoreError>;
    async fn list(&self, execution_id: &ExecutionId) -> Result<Vec<Artifact>, StoreError>;
}

#[derive(Debug, Clone)]
pub struct PgArtifactStore {
    pool: PgPool,
    state_root: PathBuf,
}

impl PgArtifactStore {
    pub async fn connect(
        database_url: &str,
        state_root: impl Into<PathBuf>,
    ) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        run_migrations(&pool).await?;
        Ok(Self {
            pool,
            state_root: state_root.into(),
        })
    }

    pub fn from_pool(pool: PgPool, state_root: impl Into<PathBuf>) -> Self {
        Self {
            pool,
            state_root: state_root.into(),
        }
    }
}

#[async_trait]
impl ArtifactStore for PgArtifactStore {
    async fn store(
        &self,
        execution_id: &ExecutionId,
        name: &str,
        media_type: &str,
        bytes: &[u8],
    ) -> Result<Artifact, StoreError> {
        validate_name(name)?;
        if media_type.trim().is_empty() || media_type.len() > 255 {
            return Err(StoreError::InvalidArtifactName(
                "artifact media type must be 1-255 bytes".to_owned(),
            ));
        }
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM executions WHERE id = $1)")
                .bind(execution_id.as_str())
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            return Err(StoreError::NotFound(execution_id.to_string()));
        }

        let digest = sha256_hex(bytes);
        let relative_path = format!("artifacts/{}/{}", &digest[..2], digest);
        let destination = self.state_root.join(&relative_path);
        let payload = bytes.to_vec();
        tokio::task::spawn_blocking(move || persist_blob(&destination, &payload))
            .await
            .map_err(|error| StoreError::Conflict(format!("artifact writer failed: {error}")))??;

        let created_at = Utc::now();
        let size_bytes = u64::try_from(bytes.len())
            .map_err(|_| StoreError::Conflict("artifact length exceeds u64".to_owned()))?;
        let size_i64 = i64::try_from(size_bytes)
            .map_err(|_| StoreError::Conflict("artifact length exceeds BIGINT".to_owned()))?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO artifact_blobs (sha256, size_bytes, relative_path, created_at) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (sha256) DO NOTHING",
        )
        .bind(&digest)
        .bind(size_i64)
        .bind(&relative_path)
        .bind(created_at)
        .execute(&mut *transaction)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO artifacts (execution_id, name, sha256, media_type, created_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (execution_id, name) DO NOTHING",
        )
        .bind(execution_id.as_str())
        .bind(name)
        .bind(&digest)
        .bind(media_type)
        .bind(created_at)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 0 {
            let existing = sqlx::query(
                "SELECT sha256, media_type FROM artifacts WHERE execution_id = $1 AND name = $2",
            )
            .bind(execution_id.as_str())
            .bind(name)
            .fetch_one(&mut *transaction)
            .await?;
            if existing.try_get::<String, _>("sha256")? != digest
                || existing.try_get::<String, _>("media_type")? != media_type
            {
                return Err(StoreError::Conflict(format!(
                    "artifact name {name} is already associated with different content"
                )));
            }
        }
        transaction.commit().await?;
        self.list(execution_id)
            .await?
            .into_iter()
            .find(|artifact| artifact.name == name)
            .ok_or_else(|| StoreError::NotFound(name.to_owned()))
    }

    async fn list(&self, execution_id: &ExecutionId) -> Result<Vec<Artifact>, StoreError> {
        let exists =
            sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM executions WHERE id = $1)")
                .bind(execution_id.as_str())
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            return Err(StoreError::NotFound(execution_id.to_string()));
        }
        let rows = sqlx::query(
            "SELECT a.execution_id, a.name, a.sha256, b.size_bytes, a.media_type, a.created_at \
             FROM artifacts a JOIN artifact_blobs b USING (sha256) \
             WHERE a.execution_id = $1 ORDER BY a.created_at, a.name",
        )
        .bind(execution_id.as_str())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(decode_artifact).collect()
    }
}

fn decode_artifact(row: sqlx::postgres::PgRow) -> Result<Artifact, StoreError> {
    let size = row.try_get::<i64, _>("size_bytes")?;
    Ok(Artifact {
        execution_id: ExecutionId::new(row.try_get::<String, _>("execution_id")?),
        name: row.try_get("name")?,
        sha256: row.try_get("sha256")?,
        size_bytes: u64::try_from(size)
            .map_err(|_| StoreError::Conflict("negative artifact size".to_owned()))?,
        media_type: row.try_get("media_type")?,
        created_at: row.try_get("created_at")?,
    })
}

fn validate_name(name: &str) -> Result<(), StoreError> {
    if name.is_empty()
        || name.len() > 255
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.chars().any(char::is_control)
    {
        Err(StoreError::InvalidArtifactName(name.to_owned()))
    } else {
        Ok(())
    }
}

fn persist_blob(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| StoreError::Conflict("artifact path lacks parent".to_owned()))?;
    std::fs::create_dir_all(parent).map_err(io_error)?;
    reject_symlink_directory(parent)?;
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(StoreError::Conflict(
                "artifact blob path is not a regular file".to_owned(),
            ));
        }
        let existing = std::fs::read(path).map_err(io_error)?;
        if existing == bytes {
            return Ok(());
        }
        return Err(StoreError::Conflict(
            "artifact blob content does not match its SHA-256 path".to_owned(),
        ));
    }
    let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4().simple()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(io_error)?;
    if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        let _ = std::fs::remove_file(&temporary);
        return Err(io_error(error));
    }
    if path.exists() {
        std::fs::remove_file(&temporary).map_err(io_error)?;
    } else {
        std::fs::rename(&temporary, path).map_err(io_error)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(io_error)?;
    }
    Ok(())
}

fn reject_symlink_directory(path: &Path) -> Result<(), StoreError> {
    let metadata = std::fs::symlink_metadata(path).map_err(io_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StoreError::Conflict(
            "artifact directory is not a regular directory".to_owned(),
        ));
    }
    if let Some(artifacts_root) = path.parent() {
        let metadata = std::fs::symlink_metadata(artifacts_root).map_err(io_error)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(StoreError::Conflict(
                "artifact root is not a regular directory".to_owned(),
            ));
        }
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> StoreError {
    StoreError::Conflict(format!("artifact filesystem error: {error}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    const INITIAL: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut message = bytes.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());
    let mut hash = INITIAL;
    for chunk in message.chunks_exact(64) {
        let mut schedule = [0u32; 64];
        for (index, word) in chunk.chunks_exact(4).enumerate() {
            schedule[index] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for index in 16..64 {
            let s0 = schedule[index - 15].rotate_right(7)
                ^ schedule[index - 15].rotate_right(18)
                ^ (schedule[index - 15] >> 3);
            let s1 = schedule[index - 2].rotate_right(17)
                ^ schedule[index - 2].rotate_right(19)
                ^ (schedule[index - 2] >> 10);
            schedule[index] = schedule[index - 16]
                .wrapping_add(s0)
                .wrapping_add(schedule[index - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = hash;
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choice = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(schedule[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        for (target, value) in hash.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *target = target.wrapping_add(value);
        }
    }
    let mut output = String::with_capacity(64);
    for word in hash {
        std::fmt::Write::write_fmt(&mut output, format_args!("{word:08x}"))
            .expect("writing to a String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_standard_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
