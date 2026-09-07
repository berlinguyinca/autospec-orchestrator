CREATE TABLE IF NOT EXISTS execution_requests (
    idempotency_key TEXT PRIMARY KEY,
    request_scope TEXT NOT NULL,
    manifest JSONB NOT NULL,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    created_at TIMESTAMPTZ NOT NULL
);
