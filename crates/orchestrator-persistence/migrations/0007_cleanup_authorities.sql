CREATE TABLE cleanup_authorities (
    execution_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX cleanup_authorities_worker_idx
    ON cleanup_authorities (worker_id, updated_at, execution_id);
