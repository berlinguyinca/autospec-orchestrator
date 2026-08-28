CREATE TABLE reservations (
    execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    attempt_id TEXT NOT NULL UNIQUE,
    cpu INTEGER NOT NULL CHECK (cpu > 0),
    memory_mib BIGINT NOT NULL CHECK (memory_mib > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX reservations_worker_idx ON reservations (worker_id, created_at);
