CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    manifest JSONB NOT NULL,
    worker_id TEXT,
    attempt_id TEXT,
    session_id TEXT,
    worktree_path TEXT,
    labels JSONB NOT NULL,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0)
);

CREATE INDEX IF NOT EXISTS executions_live_idx
    ON executions (created_at, id)
    WHERE state NOT IN ('COMPLETED', 'FAILED', 'CANCELLED');
