CREATE TABLE execution_control_requests (
    request_id BIGSERIAL PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (execution_id, action, idempotency_key)
);

CREATE INDEX execution_control_requests_pending_order_idx
    ON execution_control_requests (requested_at, request_id)
    WHERE completed_at IS NULL;
