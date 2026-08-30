//! Shared PostgreSQL isolation for workspace integration tests.

use sqlx::{Connection, PgConnection};

const TEST_DATABASE_LOCK: i64 = 870_051_003;

/// A cross-process lock held for the complete scope of a database-mutating test.
///
/// The database-name check is intentionally performed before taking the lock so
/// integration tests cannot mutate an operational database by mistake.
#[doc(hidden)]
pub struct DisposableTestDatabaseLock {
    _connection: PgConnection,
}

impl DisposableTestDatabaseLock {
    pub async fn acquire(database_url: &str) -> Result<Self, sqlx::Error> {
        let mut connection = PgConnection::connect(database_url).await?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&mut connection)
            .await?;
        require_disposable_test_database(&database_name).map_err(sqlx::Error::Protocol)?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(TEST_DATABASE_LOCK)
            .execute(&mut connection)
            .await?;
        Ok(Self {
            _connection: connection,
        })
    }
}

#[doc(hidden)]
pub fn require_disposable_test_database(database_name: &str) -> Result<(), String> {
    let suffix = database_name
        .strip_prefix("autospec_test_")
        .or_else(|| database_name.strip_prefix("autospec_orchestrator_test_"))
        .ok_or_else(|| {
            format!(
                "refusing PostgreSQL test mutations outside a disposable autospec test database: {database_name}"
            )
        })?;
    if suffix.len() < 16
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(format!(
            "disposable autospec test database needs a unique lowercase suffix of at least 16 characters: {database_name}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::require_disposable_test_database;

    #[test]
    fn rejects_operational_database_names() {
        assert!(require_disposable_test_database("postgres").is_err());
        assert!(require_disposable_test_database("autospec_test_short").is_err());
        assert!(require_disposable_test_database("autospec_test_0123456789abcdef").is_ok());
    }
}
