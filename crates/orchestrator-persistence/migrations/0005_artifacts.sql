-- Task-plan migration 0005 landed after worker lifecycle migrations 0006-0009.
-- The number remains fixed by the shared contract; sqlx orders it correctly.
CREATE TABLE IF NOT EXISTS artifact_blobs (
    sha256 TEXT PRIMARY KEY CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    relative_path TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    sha256 TEXT NOT NULL REFERENCES artifact_blobs(sha256),
    media_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (execution_id, name)
);

CREATE INDEX IF NOT EXISTS artifacts_execution_idx
    ON artifacts (execution_id, created_at, name);
