CREATE TABLE IF NOT EXISTS execution_events (
    execution_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (execution_id, sequence)
);
