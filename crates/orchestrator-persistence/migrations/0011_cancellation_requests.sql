CREATE TABLE execution_cancellation_requests (
    execution_id TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX execution_cancellation_requests_pending_idx
    ON execution_cancellation_requests (requested_at, execution_id)
    WHERE completed_at IS NULL;
