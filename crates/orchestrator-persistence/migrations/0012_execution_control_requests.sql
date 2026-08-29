CREATE TABLE execution_control_requests (
    request_id BIGSERIAL PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    accepted_worker_id TEXT NOT NULL,
    accepted_attempt_id TEXT NOT NULL,
    source_session_id TEXT NOT NULL,
    worktree_path TEXT NOT NULL,
    accepted_execution_version BIGINT NOT NULL CHECK (accepted_execution_version > 0),
    accepted_state TEXT NOT NULL,
    target_session_id TEXT,
    side_effect_session_id TEXT,
    phase TEXT NOT NULL DEFAULT 'ACCEPTED'
        CHECK (phase IN ('ACCEPTED', 'APPLYING', 'SIDE_EFFECT_APPLIED', 'COMPLETED', 'STALE')),
    stale_reason TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    UNIQUE (execution_id, idempotency_key),
    CHECK ((phase = 'STALE') = (stale_reason IS NOT NULL)),
    CHECK ((phase IN ('COMPLETED', 'STALE')) = (completed_at IS NOT NULL)),
    CHECK ((action = 'fork-conversation') = (target_session_id IS NOT NULL))
);

CREATE INDEX execution_control_requests_pending_order_idx
    ON execution_control_requests (accepted_worker_id, requested_at, request_id)
    WHERE phase NOT IN ('COMPLETED', 'STALE');
